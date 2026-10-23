"""Materialize a sealed checkout snapshot, whichever transport delivered it.

Split out of ``pool.py`` unchanged.  The pull queue was the first transport to
need this, so it was written where it was first used; it is not *about* the
queue.  A SLURM batch job has to perform exactly the same materialization --
initialize a repository under a box-local root, fetch the sealed commit out of
the CAS-carried bundle, check it out detached, hand the subdirectory to the
worker, and remove the tree afterwards without ever turning completed work into
a retry -- and a second copy of that sequence is how two transports end up
executing two different trees for one action key.

So the code lives here and both transports import it.  The functions keep the
names and bodies they had in ``pool.py``; only the module they live in changed.
The one addition is ``local_checkout_root``: the root is a deployment fact, and
each transport keeps its own spelling of it (``pool.LOCAL_CHECKOUT_ROOT`` stays
the pull queue's), so the caller passes the root it means rather than this
module deciding for it.

Stdlib only, like everything a worker node has to import.
"""
from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
import math
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import tempfile
import time
import uuid

from . import core as pb
from . import owned_tree, immutable_files
from .immutable_files import MaterializationError, MaterializationContractError

# The snapshot bundle travels through the shared CAS, but execution trees stay
# on each worker's local disk. Same spelling on every box, different storage.
#
# The variable name and the default are named constants because a second reader
# has to agree with them: ``fleet/slurm/epilog.sh`` bounds its ``rm -rf`` by
# this root, and a shell script cannot import this module. It spells the same
# variable and the same default, and
# ``test_the_epilog_and_the_materializer_name_the_same_checkout_root`` is what
# notices when the two drift.
LOCAL_CHECKOUT_ROOT_ENV = "PRISMABUILD_LOCAL_CHECKOUT_ROOT"
DEFAULT_LOCAL_CHECKOUT_ROOT = "/home/rob/tmp/prismabuild-checkouts"
LOCAL_CHECKOUT_ROOT = Path(
    os.environ.get(LOCAL_CHECKOUT_ROOT_ENV, DEFAULT_LOCAL_CHECKOUT_ROOT)
)
#: The only names ``_execution_checkout`` gives its private trees:
#: ``<action key 12>.<mkdtemp suffix>``.  Each carries a sibling owner lock
#: (``owned_tree``), never one inside the tree, which actions hash.
CHECKOUT_TREE = re.compile(r"[0-9a-f]{12}\.[a-z0-9_]{8}")


class MaterializationDeadline(MaterializationError):
    """A step ran out of the time its caller gave the whole checkout (#1429).

    Raised only when the caller passed a deadline.  The step did not finish,
    so nothing the tree holds is trusted; the caller refuses the launch.
    """


def _now() -> float:
    return time.time()


#: What one Git call may take when no caller deadline is shorter.
_GIT_TIMEOUT_S = 120.0


def _seconds_before(deadline_unix: float, where: str) -> float:
    """The seconds left before ``deadline_unix``, or the deadline refusal."""

    remaining = float(deadline_unix) - _now()
    if remaining <= 0:
        raise MaterializationDeadline(f"{where}: the deadline has passed")
    return remaining


def _run_materializer_git(
    argv: Sequence[str],
    *,
    where: str,
    environment: Mapping[str, str] | None = None,
    deadline_unix: float | None = None,
) -> str:
    timeout = _GIT_TIMEOUT_S
    if deadline_unix is not None:
        # One call may not outlive the caller's deadline either (#1429).
        timeout = min(timeout, _seconds_before(deadline_unix, where))
    try:
        # ``argv`` is ``git <args>``; ``git -C . <args>`` is the same command
        # run through the one shared runner (#1318).
        if not argv or argv[0] != "git":
            raise MaterializationError(f"{where} failed: not a git command")
        completed = pb._git_run(
            ".",
            *argv[1:],
            timeout=timeout,
            env=None if environment is None else dict(environment),
        )
    except subprocess.TimeoutExpired as exc:
        if timeout < _GIT_TIMEOUT_S:
            raise MaterializationDeadline(
                f"{where}: the deadline passed during the call") from exc
        raise MaterializationError(f"{where} failed: {exc}") from exc
    except OSError as exc:
        raise MaterializationError(f"{where} failed: {exc}") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise MaterializationError(f"{where} failed: {detail or completed.returncode}")
    return completed.stdout

def _remove_checkout_tree(temporary: Path, *, expect=None) -> None:
    """Remove one private tree on its own filesystem, read-only parts too."""

    parent = os.open(temporary.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        owned_tree.remove_tree(parent, temporary.name, expect=expect)
    finally:
        os.close(parent)

def _remove_tree_before(path: Path, deadline_unix: float) -> None:
    """Remove through the owned-tree authority while checking the fence."""
    parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        owned_tree.remove_tree(
            parent, path.name,
            check=lambda: _seconds_before(deadline_unix, "checkout removal"))
    finally:
        os.close(parent)


def _cleanup_execution_checkout(
    base: Path, temporary: Path, item: Mapping[str, object],
    *, deadline_unix: float | None = None, expect=None,
) -> dict[str, object]:
    """Remove one private tree, recording a leak without changing task status.

    With ``deadline_unix`` the removal stops there (#1429); the rest is a leak
    like any other failed removal.
    """

    error = ""
    try:
        if deadline_unix is None:
            _remove_checkout_tree(temporary, **({} if expect is None else {"expect": expect}))
        else:
            _remove_tree_before(temporary, deadline_unix)
    except Exception as exc:  # cleanup must not turn completed work into retry
        error = f"{type(exc).__name__}: {exc}"
    try:
        remains = temporary.exists() or temporary.is_symlink()
    except OSError as exc:
        remains = True
        error = error or f"cannot verify removal: {exc}"
    if remains and not error:
        error = "materialized checkout still exists after recursive cleanup"
    verdict = {"complete": not error, "ended_unix": _now(), "error": error}
    if not error:
        return verdict

    action_key = str(item.get("action_key") or "unknown-action")
    record = {
        "schema": "prismaquant.prismabuild.checkout_cleanup_failure.v1",
        "action_key": action_key,
        "path": str(temporary),
        "error": error,
        "recorded_unix": _now(),
        "host": socket.gethostname(),
    }
    record_path = (
        base / "cleanup-failures" /
        f"{action_key}.{temporary.name}.json"
    )
    record_error = ""
    try:
        immutable_files.write_json_atomic(record_path, record)
    except Exception as exc:  # stderr remains the observable fallback
        record_error = f"; could not publish {record_path}: {exc}"
    print(
        "prismabuild: checkout cleanup failed after action "
        f"{action_key}: {temporary}: {error}{record_error}",
        file=sys.stderr,
        flush=True,
    )

    return verdict


def _new_owned_checkout(base: Path, prefix: str) -> tuple[Path, int]:
    """Reap dead siblings, then make and lock this action's private tree.

    A worker killed mid-action never reaches its cleanup, so each new
    checkout reaps the trees whose owner lock is free and whose retention
    window has passed (``owned_tree``), a bounded few per call.  Reaping is
    housekeeping: it never fails the action.
    """

    root = os.open(base, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        try:
            owned_tree.reap_dead_trees(root, CHECKOUT_TREE, beside=True)
        except Exception as exc:                                 # noqa: BLE001
            print(f"prismabuild: dead checkout reaping failed in {base}: {exc}",
                  file=sys.stderr, flush=True)
        temporary = Path(tempfile.mkdtemp(prefix=prefix, dir=str(base)))
        try:
            return temporary, owned_tree.take_owner_lock(
                root, temporary.name + owned_tree.LOCK_SUFFIX)
        except BaseException:
            os.rmdir(temporary)
            raise
    finally:
        os.close(root)


def _release_checkout(base: Path, temporary: Path, lock: int) -> None:
    """Drop the tree's lock: with the tree, or starting its retention clock.

    Like the removal before it, this never fails completed work.
    """

    try:
        root = os.open(base, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    except OSError as exc:
        os.close(lock)
        print(f"prismabuild: checkout owner lock kept in {base}: {exc}",
              file=sys.stderr, flush=True)
        return
    try:
        owned_tree.drop_owner_lock(root, temporary.name, lock)
    except OSError as exc:
        print(f"prismabuild: checkout owner lock kept in {base}: {exc}",
              file=sys.stderr, flush=True)
    finally:
        os.close(root)


@contextmanager
def _require_contained_materialized_links(
    repository: Path, *, deadline_unix: float | None = None,
) -> None:
    """Refuse a materialized checkout whose symlinks reach outside it.

    The seal-time gate reasons about the tree it is about to bundle, and this
    one reasons about the tree that actually landed.  Both legs are needed: a
    bundle sealed by an older ``pbrun`` carries whatever that version accepted,
    including two links that each normalize inside the tree but compose into an
    escape, and the record and the digest of such a bundle look exactly like
    any other.  Resolving each link against the real filesystem after checkout
    settles the question with the resolver the action itself will use.

    A link that dangles inside the tree stays acceptable: the snapshot seals a
    link text, not a target.  Only a resolution that leaves the tree, or one
    the filesystem refuses to resolve, is refused.
    """

    root = repository.resolve()
    pending = [root]
    while pending:
        if deadline_unix is not None:
            _seconds_before(deadline_unix, "materialized link check")
        directory = pending.pop()
        try:
            entries = list(os.scandir(directory))
        except OSError as exc:
            raise MaterializationContractError(
                f"cannot inspect materialized checkout path: {directory}"
            ) from exc
        for entry in entries:
            candidate = Path(entry.path)
            if entry.is_symlink():
                try:
                    resolved = candidate.resolve()
                except (OSError, RuntimeError) as exc:
                    # A link the filesystem cannot follow, a loop among them
                    # included, is a contract refusal and not a crash.  The
                    # resolver reports a loop as ``OSError`` on some Python
                    # versions and as ``RuntimeError`` on others, so both are
                    # caught here.
                    raise MaterializationContractError(
                        "materialized checkout symlink cannot be resolved: "
                        f"{candidate.relative_to(root)}"
                    ) from exc
                if not resolved.is_relative_to(root):
                    raise MaterializationContractError(
                        "materialized checkout symlink points outside the "
                        f"sealed repository: {candidate.relative_to(root)}"
                    )
                continue
            if entry.is_dir(follow_symlinks=False) and candidate != root / ".git":
                pending.append(candidate)


@contextmanager
def _execution_checkout(
    item: Mapping[str, object],
    *,
    local_checkout_root: str | Path | None = None,
    on_temporary: Callable[[Path], None] | None = None,
    deadline_unix: float | None = None,
    cleanup_deadline_unix: float | None = None,
    cleanup: dict[str, object] | None = None,
) -> Iterator[Path]:
    """Yield the live path or a private checkout of the sealed snapshot.

    Args:
        item: The three fields this reads, in the pool's own shape.
        local_checkout_root: The box-local root private trees are made under.
        on_temporary: Called once with the per-action directory, straight after
            ``mkdtemp`` creates it and before any Git runs.  A caller that has
            to record the tree for something outside this process needs the
            name before the fetch, not after it: a SLURM job killed at its time
            limit during a large fetch leaves the tree behind, and the Epilog
            removes only what the job wrote down.  Reported rather than
            pre-created so ``mkdtemp`` and the cleanup below stay owned by this
            one function, and so the signature both transports share is
            unchanged for the caller that does not need it.
        deadline_unix: With it, every Git call and the link check end before
            this instant or raise :class:`MaterializationDeadline` (#1429).
        cleanup_deadline_unix: With it, the final removal stops there and
            records the rest as a leak.  Without either, the calls are the
            ones every other transport has always made.
    """

    raw_snapshot = item.get("checkout_snapshot")
    if raw_snapshot is None:
        raw_root = item.get("checkout_root")
        if not raw_root:
            raise MaterializationContractError(
                "pool item has neither checkout_root nor checkout_snapshot"
            )
        yield Path(str(raw_root))
        return

    snapshot = pb.validate_pbrun_checkout_snapshot(raw_snapshot)
    cas = pb.PrismaBuildCAS(str(item["cas_root"]))
    bundle = cas.input_path(snapshot["input"])
    base = Path(
        LOCAL_CHECKOUT_ROOT if local_checkout_root is None
        else local_checkout_root
    )
    if not base.is_absolute() or base == Path("/") or ".." in base.parts:
        raise MaterializationContractError(
            "PRISMABUILD_LOCAL_CHECKOUT_ROOT must be an absolute non-root path"
        )
    base.mkdir(parents=True, exist_ok=True)
    temporary, owner_lock = _new_owned_checkout(
        base, f"{str(item['action_key'])[:12]}.")
    if cleanup is not None:
        identity = temporary.stat(follow_symlinks=False)
        cleanup.update(path=str(temporary), device=identity.st_dev, inode=identity.st_ino,
                       complete=False, ended_unix=None)
    if on_temporary is not None:
        on_temporary(temporary)
    repository = temporary / "checkout"
    bounded = {} if deadline_unix is None else {"deadline_unix": deadline_unix}
    try:
        _run_materializer_git(
            ["git", "init", "-q", str(repository)],
            where="initialize materialized checkout",
            **bounded,
        )
        heads = _run_materializer_git(
            ["git", "-C", str(repository), "bundle", "list-heads", str(bundle)],
            where="read checkout snapshot bundle",
            **bounded,
        )
        commit = str(snapshot["commit"])
        advertised = {
            fields[1]: fields[0]
            for line in heads.splitlines()
            if len(fields := line.split(maxsplit=1)) == 2
        }
        sealed = [name for name, oid in advertised.items() if oid == commit]
        if not sealed:
            raise MaterializationContractError(
                "checkout snapshot bundle does not advertise its sealed commit"
            )
        refspecs = [sealed[0]]
        # A v2 record names branches the action will spell -- ``master...HEAD``
        # in a diff-derived gate.  The record and the bundle travel separately,
        # so the record alone cannot be the authority for where a branch
        # points: creating ``refs/heads/master`` at an id nothing in the bundle
        # reaches would make every later comparison a silent lie rather than a
        # refusal.  Both must say the same thing before either is used.
        for name, sealed_id in sorted(dict(snapshot.get("refs") or {}).items()):
            qualified = f"refs/heads/{name}"
            if advertised.get(qualified) != sealed_id:
                raise MaterializationContractError(
                    f"checkout snapshot bundle contradicts sealed ref {name!r}"
                )
            refspecs.append(f"{qualified}:{qualified}")
        # ``git init`` leaves HEAD a symref to the unborn default branch, and
        # ``git fetch`` refuses to update the branch HEAD points at -- which is
        # exactly ``master`` on the checkout whose gate this exists for.  Point
        # HEAD at the snapshot's own reserved name, which no record may claim,
        # before fetching anything.  Only a record that names branches needs
        # this: the sealed ref is fetched as a bare refspec into FETCH_HEAD and
        # updates no local branch, so a v1 record runs the same Git commands it
        # always did.
        if len(refspecs) > 1:
            _run_materializer_git(
                [
                    "git", "-C", str(repository), "symbolic-ref", "HEAD",
                    f"refs/heads/"
                    f"{pb.PBRUN_CHECKOUT_SNAPSHOT_REF_NAME}.materializing",
                ],
                where="detach materialized HEAD from a fetched branch",
                **bounded,
            )
        _run_materializer_git(
            [
                "git", "-C", str(repository), "fetch", "-q", "--no-tags",
                str(bundle), *refspecs,
            ],
            where="fetch checkout snapshot bundle",
            **bounded,
        )
        _run_materializer_git(
            [
                "git",
                "-c", "core.autocrlf=false",
                "-c", "core.attributesFile=/dev/null",
                "-C", str(repository), "checkout", "-q", "--detach", commit,
            ],
            where="check out sealed commit",
            environment={**os.environ, "GIT_ATTR_NOSYSTEM": "1"},
            **bounded,
        )
        _require_contained_materialized_links(repository, **bounded)
        subdirectory = repository / str(snapshot["subdirectory"])
        if not subdirectory.is_dir():
            raise MaterializationContractError(
                "checkout snapshot subdirectory is absent after materialization"
            )
        yield subdirectory
    finally:
        # The path was created by mkdtemp below a validated local-only root;
        # never widen this cleanup to the root itself. A root-owned container
        # dropping can leave residue inside this bounded root, but cleanup
        # failure must not change an already-published action into a failure.
        try:
            verdict = _cleanup_execution_checkout(
                base, temporary, item,
                **({} if cleanup_deadline_unix is None
                   else {"deadline_unix": cleanup_deadline_unix}))
            if cleanup is not None:
                cleanup.update(verdict)
        finally:
            _release_checkout(base, temporary, owner_lock)



def settle_checkout_cleanup(
    record: Mapping[str, object], *, local_checkout_root: Path,
) -> dict[str, object]:
    """Retry only the registered tree of an exact fenced snapshot attempt.

    A timer never returns resources. Missing proof, a foreign attempt, or a
    replacement inode retains them. A later successful retry records its own
    completion time, so settlement after the fence remains UNKNOWN.
    """
    from . import lifetime_fence

    proof = record.get("checkout_cleanup")
    if not isinstance(proof, Mapping) or proof.get("claim") != lifetime_fence.attempt_identity(record):
        return {"complete": False, "error": "checkout cleanup proof missing or belongs to another attempt"}
    if proof.get("complete") is True:
        ended = proof.get("ended_unix")
        if type(ended) not in (int, float) or not math.isfinite(ended):
            return {**proof, "complete": False, "error": "checkout cleanup has no completion time"}
        return dict(proof)
    try:
        path = Path(str(proof.get("path") or ""))
        if (path.parent != local_checkout_root or not CHECKOUT_TREE.fullmatch(path.name)
                or not path.name.startswith(str(record["action_key"])[:12] + ".")
                or type(proof.get("device")) is not int or type(proof.get("inode")) is not int):
            raise ValueError("checkout cleanup path or inode is not registered")
        try:
            path.lstat()
        except FileNotFoundError:
            return {**proof, "complete": False, "error": "registered checkout vanished without removal proof"}
        verdict = _cleanup_execution_checkout(
            local_checkout_root, path, record,
            expect=(proof["device"], proof["inode"]))
        return {**proof, **verdict}
    except (OSError, ValueError) as exc:
        return {**proof, "complete": False, "error": str(exc)}

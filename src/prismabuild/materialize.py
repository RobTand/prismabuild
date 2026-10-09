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
import errno
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid

from . import core as pb

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


class MaterializationError(pb.PrismaBuildError):
    """A checkout could not be materialized from its sealed snapshot."""


class MaterializationContractError(MaterializationError, ValueError):
    """A record handed to the materializer does not satisfy its schema."""


class MaterializationDeadline(MaterializationError):
    """A step ran out of the time its caller gave the whole checkout (#1429).

    Raised only when the caller passed a deadline.  The step did not finish,
    so nothing the tree holds is trusted; the caller refuses the launch.
    """


def _now() -> float:
    return time.time()


#: What one Git call may take when no caller deadline is shorter.
_GIT_TIMEOUT_S = 120.0
#: How many directory entries a bounded removal deletes between clock reads.
_REMOVAL_CLOCK_STRIDE = 256
#: Opens a directory for descriptor-relative work: never through a link in
#: the last component, and never as anything but a directory.
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


def _seconds_before(deadline_unix: float, where: str) -> float:
    """The seconds left before ``deadline_unix``, or the deadline refusal."""

    remaining = float(deadline_unix) - _now()
    if remaining <= 0:
        raise MaterializationDeadline(f"{where}: the deadline has passed")
    return remaining


def _write_json_atomic(path: Path, payload: Mapping[str, object], *,
                       make_parent: bool = True,
                       trailing_newline: bool = False,
                       text: str = "canonical",
                       tmp: str = "pid_uuid",
                       fsync: bool = True) -> None:
    """Publish a record by rename, so no reader ever sees a partial file.

    The one owner for rename-atomic JSON records (#1330): the pool queue
    writers already call this, and the hand-rolled remainder migrate to
    it with exactly the parameters that preserve their history -- never
    silently upgraded or downgraded.

    ``make_parent=False`` skips the ``mkdir``: a caller that rewrites one
    record every cycle creates its directory only when a write finds it
    missing (``FileNotFoundError``), not on every call (#960).

    ``trailing_newline=True`` terminates the record with one LF, the lane
    history: the retired ``slurm_lane._write_latest`` wrote
    ``_canonical_file_bytes``.  Lane records are read by another box's
    ``json.load``, which accepts both shapes, but their bytes are pinned,
    so the flag preserves them bit for bit.  ``False`` keeps this module's
    historical bare bytes, which feed content-addressed paths and must not
    move.  ``text" selects the bytes: ``"canonical"`` (the default,
    ``_canonical_bytes``), ``"canonical_lf"`` (same plus one LF, the
    spelling ``trailing_newline=True`` keeps for its callers), or
    ``"sorted_lf"`` (``core._sorted_lf_bytes`` -- ``json.dumps`` with
    ``sort_keys`` plus one LF, the exact bytes ``pool``'s holder
    records and reader-scope proofs wrote).  ``core``'s status
    sidecars, ``progress`` and ``resource_scope`` keep their own
    spellings: all three run standalone (runpy, as-a-program, and
    single-file-in-generation respectively) and cannot import this
    owner at any level -- proven by the reachability suites, which
    failed on the migration and pass on the revert.

    ``tmp`` names the temp file: ``"pid_uuid"`` (the default -- a lane
    directory lives on the shared mount, where two boxes submitting one
    action key write into it and a pid alone names one file on both) or
    ``"pid"`` (the local writers' history).  ``pid_uuid`` opens
    ``O_EXCL`` (a collision there is a bug); ``pid`` opens ``O_TRUNC``
    -- the migrated writers used ``write_text``/``open("w")``, and a
    stale pid temp left by a SIGKILLed writer must truncate, not wedge
    every later write with ``FileExistsError`` (PIDs get reused, and
    the status sidecars swallow ``OSError`` while progress records feed
    stall detection: #1331 review).

    ``fsync=False`` skips the file fsync (the local status writers'
    history -- a best-effort sidecar must not pay spindle latency).
    The rename sits inside the ``try`` so a failed rename still cleans
    its temp instead of littering a directory an operator reads.

    Deliberately NOT parametrized further: the upgrade client's
    (plain temp, post-write chmod, dir fsync) and the resource broker's
    (token temp, ``O_NOFOLLOW``, dir fsync) writers stay hand-rolled --
    both are standalone host tools that must not import this package,
    and their durability contracts are not this owner's.
    """

    if text not in ("canonical", "canonical_lf", "sorted_lf"):
        raise MaterializationContractError(
            f"atomic record text must be canonical, canonical_lf or "
            f"sorted_lf, not {text!r}")
    if tmp not in ("pid_uuid", "pid"):
        raise MaterializationContractError(
            f"atomic record tmp must be pid_uuid or pid, not {tmp!r}")
    if make_parent:
        path.parent.mkdir(parents=True, exist_ok=True)
    if tmp == "pid_uuid":
        tmp_name = f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    else:
        tmp_name = f".{path.name}.{os.getpid()}.tmp"
    tmp_path = path.parent / tmp_name
    if text == "sorted_lf":
        data = pb._sorted_lf_bytes(dict(payload))
    elif text == "canonical_lf" or trailing_newline:
        data = pb._canonical_file_bytes(dict(payload))
    else:
        data = pb._canonical_bytes(dict(payload))
    flags = os.O_WRONLY | os.O_CREAT
    flags |= os.O_EXCL if tmp == "pid_uuid" else os.O_TRUNC
    descriptor = os.open(tmp_path, flags, 0o644)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            if fsync:
                os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


def write_bytes_atomic(path: Path, data: bytes, *,
                       make_parent: bool = True) -> None:
    """Publish exact bytes by rename, beside the JSON owner (#1330).

    ``pool._write_bytes_atomic``'s contract verbatim: pid+UUID temp,
    ``0o644`` exclusive create, file fsync, rename; the same sync policy
    as JSON records.  One spelling so the two can never drift.
    """

    if make_parent:
        path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    os.replace(temporary, path)

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

def _remove_tree_before(path: Path, deadline_unix: float) -> None:
    """``shutil.rmtree`` that stops at ``deadline_unix`` and says so (#1429).

    Descriptor-relative and never through a link, as ``shutil.rmtree`` is.  A
    directory is opened with ``O_NOFOLLOW``, compared with the entry that was
    listed, and every name below it is deleted relative to that descriptor.
    A path name is never resolved twice, so a descendant that swaps a listed
    directory for a link, or for another directory, steers no deletion out of
    the tree: the open refuses and the removal raises.  ``path`` itself is
    trusted, as it is for ``shutil.rmtree``.  The clock is read once per
    directory and every ``_REMOVAL_CLOCK_STRIDE`` entries.  What remains at
    the deadline stays where it is: the caller records the leak, as it does
    for any other removal failure.
    """

    removed = 0

    def empty(directory: int) -> None:
        nonlocal removed
        _seconds_before(deadline_unix, "checkout removal")
        with os.scandir(directory) as listing:
            entries = list(listing)
        for entry in entries:
            if entry.is_dir(follow_symlinks=False):
                listed = entry.stat(follow_symlinks=False)
                child = os.open(entry.name, _DIRECTORY_FLAGS, dir_fd=directory)
                try:
                    if not os.path.samestat(listed, os.fstat(child)):
                        raise OSError(
                            errno.ELOOP, "directory replaced during removal", entry.name)
                    empty(child)
                finally:
                    os.close(child)
                os.rmdir(entry.name, dir_fd=directory)
            else:
                os.unlink(entry.name, dir_fd=directory)
            removed += 1
            if removed % _REMOVAL_CLOCK_STRIDE == 0:
                _seconds_before(deadline_unix, "checkout removal")

    top = os.open(path, _DIRECTORY_FLAGS)
    try:
        empty(top)
    finally:
        os.close(top)
    os.rmdir(path)


def _cleanup_execution_checkout(
    base: Path, temporary: Path, item: Mapping[str, object],
    *, deadline_unix: float | None = None,
) -> None:
    """Remove one private tree, recording a leak without changing task status.

    With ``deadline_unix`` the removal stops there (#1429); the rest is a leak
    like any other failed removal.
    """

    error = ""
    try:
        if deadline_unix is None:
            shutil.rmtree(temporary)
        else:
            _remove_tree_before(temporary, deadline_unix)
    except Exception as exc:  # cleanup must not turn completed work into retry
        error = str(exc)
    try:
        remains = temporary.exists() or temporary.is_symlink()
    except OSError as exc:
        remains = True
        error = error or f"cannot verify removal: {exc}"
    if remains and not error:
        error = "materialized checkout still exists after recursive cleanup"
    if not error:
        return

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
        _write_json_atomic(record_path, record)
    except Exception as exc:  # stderr remains the observable fallback
        record_error = f"; could not publish {record_path}: {exc}"
    print(
        "prismabuild: checkout cleanup failed after action "
        f"{action_key}: {temporary}: {error}{record_error}",
        file=sys.stderr,
        flush=True,
    )


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
    temporary = Path(
        tempfile.mkdtemp(
            prefix=f"{str(item['action_key'])[:12]}.", dir=str(base)
        )
    )
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
        _cleanup_execution_checkout(
            base, temporary, item,
            **({} if cleanup_deadline_unix is None
               else {"deadline_unix": cleanup_deadline_unix}))



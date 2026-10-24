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
from contextlib import ExitStack, contextmanager
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
import weakref
from typing import NamedTuple

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

def _remove_tree_before(path: Path, deadline_unix: float, *, expect=None) -> None:
    """Remove through the owned-tree authority while checking the fence."""
    parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        owned_tree.remove_tree(
            parent, path.name, expect=expect,
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
            _remove_tree_before(temporary, deadline_unix,
                                **({} if expect is None else {"expect": expect}))
    except Exception as exc:  # cleanup must not turn completed work into retry
        error = f"{type(exc).__name__}: {exc}"
    if expect is None:
        try:
            remains = temporary.exists() or temporary.is_symlink()
        except OSError as exc:
            remains = True
            error = error or f"cannot verify removal: {exc}"
        if remains and not error:
            error = "materialized checkout still exists after recursive cleanup"
    # With a registration, owned_tree's held descriptor already proved the
    # exact inode unlinked. A new name at that path cannot erase that proof.
    verdict = {"complete": not error, "ended_unix": _now(), "error": error,
               "unlinked": not error and expect is not None}
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


def _new_owned_checkout(base: Path, prefix: str, *, name=None, parent_identity=None) -> tuple[Path, int]:
    """Reap dead siblings, then make and lock this action's private tree.

    A worker killed mid-action never reaches its cleanup, so each new
    checkout reaps the trees whose owner lock is free and whose retention
    window has passed (``owned_tree``), a bounded few per call.  Reaping is
    housekeeping: it never fails the action.
    """

    root = os.open(base, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        if parent_identity is not None and (os.fstat(root).st_dev, os.fstat(root).st_ino) != parent_identity:
            raise MaterializationContractError("checkout intent parent changed")
        try:
            owned_tree.reap_dead_trees(root, CHECKOUT_TREE, beside=True)
        except Exception as exc:                                 # noqa: BLE001
            print(f"prismabuild: dead checkout reaping failed in {base}: {exc}",
                  file=sys.stderr, flush=True)
        if name is None:
            temporary = Path(tempfile.mkdtemp(prefix=prefix, dir=str(base)))
        else:
            os.mkdir(name, dir_fd=root)
            temporary = base / name
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
    checkout_intent: Mapping[str, object] | None = None,
    creation_guard=None,
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
        creation_guard: Holds intent publication, mkdir and inode registration
            in one custody transition. Released before Git materialization.
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
    with ExitStack() as creating:
        if creation_guard is not None:
            creating.enter_context(creation_guard())
        temporary, owner_lock = _new_owned_checkout(
            base, f"{str(item['action_key'])[:12]}.",
            **({} if checkout_intent is None else {
                "name": Path(checkout_intent["path"]).name,
                "parent_identity": (checkout_intent["parent_device"], checkout_intent["parent_inode"])}))
        expected = None
        if cleanup is not None:
            identity = temporary.stat(follow_symlinks=False)
            expected = (identity.st_dev, identity.st_ino)
            cleanup.update(path=str(temporary), device=identity.st_dev, inode=identity.st_ino,
                           complete=False, ended_unix=None)
        repository = temporary / "checkout"
        bounded = {} if deadline_unix is None else {"deadline_unix": deadline_unix}
        try:
            if on_temporary is not None:
                on_temporary(temporary)
            creating.close()
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
                    **({} if expected is None else {"expect": expected}),
                    **({} if cleanup_deadline_unix is None
                       else {"deadline_unix": cleanup_deadline_unix}))
                if cleanup is not None:
                    cleanup.update(verdict)
            finally:
                _release_checkout(base, temporary, owner_lock)



# Issuance and immutable bindings stay in this module, never on a certificate.
_ISSUED_RELEASE_CERTIFICATES = weakref.WeakKeyDictionary()


class _ReleaseBinding(NamedTuple):
    root: Path
    base: Path
    holder: str
    manifest: bytes
    owner_path: Path
    claim: bytes
    registrations: bytes
    token_names: frozenset | None
    custody_key: str
    attempt: object


class ReleaseCertificate:
    """Opaque process-local capability issued only by the cleanup certifier."""

    __slots__ = ("__weakref__",)

    def __new__(cls, *args, **kwargs):
        raise pb.PrismaBuildError("release certificate can only be minted by the certifier")

    def __setattr__(self, name, value):
        from .record_store import PoolContractError
        raise PoolContractError("release certificate is immutable")

    def __reduce_ex__(self, protocol):
        raise TypeError("release certificate cannot be copied or serialized")

    @staticmethod
    def _issued(certificate):
        from .record_store import PoolContractError
        if (type(certificate) is not ReleaseCertificate
                or (binding := _ISSUED_RELEASE_CERTIFICATES.get(certificate)) is None):
            raise PoolContractError("release certificate required for this reservation")
        return binding

    def permits(self, token_name):
        binding = self._issued(self)
        return binding.token_names is None or token_name in binding.token_names

    @staticmethod
    @contextmanager
    def returning(certificate, ledger, holder):
        from . import pool, record_store
        binding = ReleaseCertificate._issued(certificate)
        # Never wait in the reverse order under a tier or host mutation guard.
        queue = pool.PoolQueue(ledger.release_records.root)
        with queue.action_transition_lock(binding.custody_key, blocking=False) as acquired:
            if not acquired:
                raise record_store.PoolContractError("release certificate custody is busy")
            ReleaseCertificate.require(certificate, ledger, holder)
            yield

    @staticmethod
    def require(certificate, ledger, holder):
        from . import lifetime_fence, record_store
        invalid = record_store.PoolContractError
        binding = ReleaseCertificate._issued(certificate)
        if (binding.root != ledger.release_records.root or not binding.owner_path.is_relative_to(binding.root)
                or binding.base != ledger.base or binding.holder != holder):
            raise invalid("release certificate required for this reservation")
        manifest = record_store.read_document(ledger.release_owner_path(holder))
        owner = record_store.read_document(binding.owner_path)
        if (pb._canonical_bytes(manifest) != binding.manifest or owner is None
                or pb._canonical_bytes(lifetime_fence.attempt_identity(owner)) != binding.claim
                or owner.get("attempts") != binding.attempt
                or pb._canonical_bytes(owner.get("checkout_registrations")) != binding.registrations):
            raise invalid("release certificate is stale or belongs to another claim")
        key = binding.custody_key
        lease_path = ledger.release_records.lease_path(key)
        claim_path = ledger.release_records.item_path(record_store.CLAIMED, key)
        own_unstarted_rollback = (
            binding.token_names is not None and binding.owner_path == claim_path
            and owner.get("checkout_registrations") == []
            and owner.get("resource_scope") is None and owner.get("resource_scope_intent") is None)
        for path in (lease_path, claim_path):
            # A private admission receipt owns exactly its new token names.
            # Returning those cannot return a predecessor's same-key custody.
            if path == lease_path and own_unstarted_rollback:
                continue
            live = record_store.read_document(path)
            if (live is not None and live.get("claimed_by") is not None
                    and (pb._canonical_bytes(lifetime_fence.attempt_identity(live)) != binding.claim
                         or live.get("attempts") != binding.attempt)):
                raise invalid("release certificate conflicts with live claim custody")
            if (path == lease_path and live is not None and "checkout_registrations" in live
                    and pb._canonical_bytes(live["checkout_registrations"]) != binding.registrations):
                raise invalid("release certificate does not name the lease's current registrations")
        return binding


# Original descriptors stay process-private until their unlink is journalled.
_CHECKOUT_WITNESSES = {}


class CheckoutCertifier:
    """Cleanup authority bound to its queue at construction, never per call."""

    def __init__(self, queue_root, *, ledger=None, recover=True):
        self._root = Path(queue_root)
        self._ledger = ledger
        self._recover = recover
        if ledger is not None and self._root != ledger.release_records.root:
            raise pb.PrismaBuildError("certificate authority and ledger queue differ")

    def _hold_registered(self, action_key, *, attempt=None, claim_nonce=None):
        from . import claim_identity, record_store
        owner = record_store.read_document(record_store.RecordStore(self._root).lease_path(action_key))
        if (owner is None or owner.get("attempts") != attempt
                or claim_identity.claim_nonce(owner) != claim_nonce):
            raise pb.PrismaBuildError("checkout witness owner changed")
        for entry in owner["checkout_registrations"]:
            registration = {k: v for k, v in entry.items() if k not in ("cleanup", "completion_claim")}
            key = (self._root, registration["execution_id"])
            if "inode" not in registration or key in _CHECKOUT_WITNESSES:
                continue
            receipt = record_store.read_document(self._root / "checkout-unlinks" / (key[1] + ".json"))
            if receipt is not None and receipt.get("registration") == registration:
                continue
            try:
                fd = os.open(registration["path"], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            except FileNotFoundError:
                continue  # Missing older debt cannot supply an unlink witness.
            info = os.fstat(fd)
            if (info.st_dev, info.st_ino) != (registration["device"], registration["inode"]):
                os.close(fd)
                raise pb.PrismaBuildError("checkout witness registration differs")
            _CHECKOUT_WITNESSES[key] = (fd, registration, attempt, claim_nonce)

    def _journal_unlinked(self, action_key, *, attempt=None, claim_nonce=None):
        from . import record_store
        records = record_store.RecordStore(self._root)
        for key, (fd, registration, registered_attempt, registered_nonce) in list(_CHECKOUT_WITNESSES.items()):
            if (key[0] != self._root or registration["claim"]["action_key"] != action_key
                    or (registered_attempt, registered_nonce) != (attempt, claim_nonce)):
                continue
            try:
                info = os.fstat(fd)
                if (info.st_dev, info.st_ino, info.st_nlink) == (registration["device"], registration["inode"], 0):
                    records.replace_record_at(self._root / "checkout-unlinks" / (key[1] + ".json"), {
                        "registration": registration, "ended_unix": _now()})
            finally:
                del _CHECKOUT_WITNESSES[key]
                os.close(fd)

    def certify_checkout_cleanup(self, action_key, *, attempt=None, claim_nonce=None, nonce=None):
        """Derive evidence from this authority's durable custody and filesystem."""
        from . import claim_identity, lifetime_fence, record_store, pool, storage_tiers

        try:
            key = action_key.removeprefix(storage_tiers.RAM_HOST_MEMORY_PREFIX)
            queue = pool.PoolQueue(self._root)
            release_ledger = self._ledger
            recover = self._recover
            reservation_nonce = nonce
            holder = action_key
            local_checkout_root = pool.LOCAL_CHECKOUT_ROOT
            def matches(candidate):
                return (candidate is not None and candidate.get("action_key") in {action_key, key}
                        and (attempt is None or candidate.get("attempts") == attempt)
                        and (claim_nonce is None or claim_identity.claim_nonce(candidate) == claim_nonce))
            record = None
            with queue.action_transition_lock(key, blocking=release_ledger is None) as acquired:
                if not acquired:
                    return {"complete": False, "used": True, "error": "checkout custody is busy"}
                # Custody paths are derived here, never supplied by a caller.
                # Recovery may certify an exact journal beside a successor; a
                # private rollback may return only its own receipt's token names.
                path = queue.records.lease_path(key)
                owner = record_store.read_document(path)
                claim_path = queue.records.item_path(record_store.CLAIMED, key)
                live_claim = record_store.read_document(claim_path)
                if not matches(owner):
                    path, owner = claim_path, live_claim if matches(live_claim) else None
                # Late attempts are selected by identity, never by a supplied path.
                try:
                    names = queue.records.claimed_names()
                    journals = tuple(name for name in names.names if name.endswith(".late-finish"))
                except FileNotFoundError:
                    journals = ()
                for name in journals:
                    if name.startswith(key + "."):
                        candidate_path = queue.records.dir(record_store.CLAIMED) / name
                        candidate = record_store.read_document(candidate_path)
                        if matches(candidate) and (owner is None or "heartbeat_recovery" in candidate):
                            path, owner = candidate_path, candidate
                if owner is None:
                    try:
                        names = queue.records.claimed_names().finishes
                    except FileNotFoundError:
                        names = ()
                    matches_finish = []
                    for name in names:
                        if name.startswith(key + "."):
                            candidate_path = queue.records.dir(record_store.CLAIMED) / name
                            candidate = record_store.read_document(candidate_path)
                            if matches(candidate):
                                matches_finish.append((candidate_path, candidate))
                    if len(matches_finish) > 1:
                        return {"complete": False, "used": True, "error": "ambiguous finishing checkout custody"}
                    if matches_finish:
                        path, owner = matches_finish[0]
                # Adopted custody is discovered from manifests on every route,
                # including terminal reclaim. The caller supplies no inventory.
                if owner is None:
                    from .legacy_custody import LegacyCustodyMigration, legacy_checkout_owner_path
                    for _, _, ledger in LegacyCustodyMigration(queue)._ledgers():
                        candidate = record_store.read_document(ledger.legacy_owner_path(action_key))
                        binding = record_store.read_document(ledger.release_owner_path(action_key))
                        if (matches(candidate) and binding is not None
                                and binding.get("legacy_adoption") == candidate.get("inventory_id")
                                and binding.get("legacy_owner_nonce") == candidate.get("manifest_nonce")):
                            path = legacy_checkout_owner_path(queue.root, key)
                            owner = record_store.read_document(path)
                            if not matches(owner) or owner.get("inventory_id") != candidate["inventory_id"]:
                                return {"complete": False, "used": True, "error": "legacy checkout authority missing"}
                            break
                manifest = None
                token_names = None
                if release_ledger is not None:
                    manifest_path = release_ledger.release_owner_path(holder)
                    manifest = record_store.read_document(manifest_path)
                    if manifest is None:
                        return {"complete": False, "used": True, "error": "reservation owner missing"}
                    if reservation_nonce is not None:
                        receipt = record_store.read_document(
                            release_ledger.acquisition_receipt_path(holder, reservation_nonce))
                        if (receipt is None or receipt.get("nonce") != reservation_nonce
                                or receipt.get("action_key") != holder
                                or receipt.get("reservation_nonce") != manifest.get("nonce")
                                or not isinstance(receipt.get("token_names"), list)):
                            return {"complete": False, "used": True, "error": "reservation nonce differs"}
                        token_names = receipt["token_names"]
                    if owner is None:
                        try:
                            names = queue.records.claimed_names().finishes
                        except FileNotFoundError:
                            names = ()  # Standalone ledger, with no queue namespace.
                        marks = [queue.records.dir(record_store.CLAIMED) / name
                                 for name in names if name.startswith(key + ".")]
                        if len(marks) > 1:
                            return {"complete": False, "used": True, "error": "ambiguous finishing checkout custody"}
                        if marks:
                            path, owner = marks[0], record_store.read_document(marks[0])
                    if owner is None and manifest.get("legacy_adoption") is not None:
                        from .legacy_custody import legacy_checkout_owner_path
                        legacy_path = release_ledger.legacy_owner_path(holder)
                        legacy = record_store.read_document(legacy_path)
                        if (legacy is None or legacy.get("inventory_id") != manifest["legacy_adoption"]
                                or legacy.get("manifest_nonce") != manifest.get("legacy_owner_nonce", manifest.get("nonce"))):
                            return {"complete": False, "used": True, "error": "legacy custody journal differs"}
                        path = legacy_checkout_owner_path(queue.root, key)
                        owner = record_store.read_document(path)
                        if owner is None or owner.get("inventory_id") != legacy["inventory_id"]:
                            return {"complete": False, "used": True, "error": "legacy checkout authority missing"}
                    if owner is None:
                        # The reservation's durable empty registration is authority
                        # only before it was bound to an executable claim.
                        if manifest.get("claim") is not None:
                            for state in (record_store.DONE, record_store.FAILED, record_store.WITHDRAWN):
                                candidate_path = queue.records.item_path(state, key)
                                candidate = record_store.read_document(candidate_path)
                                if candidate is not None and {k: v for k, v in lifetime_fence.attempt_identity(candidate).items() if k not in ("nonce", "scope_id")} == manifest["claim"]:
                                    path, owner = candidate_path, candidate
                                    break
                        else:
                            path, owner = manifest_path, dict(manifest)
                    if (reservation_nonce is not None and manifest.get("claim") is None
                            and owner is not None and owner.get("claimed_by") is None
                            and "checkout_registrations" not in owner):
                        # This exact private take precedes the claim rewrite. Its
                        # durable empty list requires no mutation of the moved row.
                        path, owner = manifest_path, dict(manifest)
                    if (manifest.get("claim") is not None and owner is not None
                            and manifest.get("checkout_snapshot") != owner.get("checkout_snapshot")):
                        return {"complete": False, "used": True, "error": "reservation checkout kind differs"}
                    if owner is not None and "checkout_registrations" not in owner:
                        if owner.get("checkout_snapshot") is not None:
                            # A private rollback can precede the claim rewrite.
                            if reservation_nonce is None or manifest.get("claim") is not None:
                                return {"complete": False, "used": True, "error": "checkout custody missing"}
                            path, owner = manifest_path, dict(manifest)
                        else:
                            owner["checkout_registrations"] = []
                            if recover:
                                queue.records.replace_record_at(path, owner)
                    if manifest.get("claim") is not None and (owner is None or
                            {k: v for k, v in lifetime_fence.attempt_identity(owner).items() if k not in ("nonce", "scope_id")} != manifest["claim"]):
                        return {"complete": False, "used": True, "error": "reservation claim differs"}
                    if reservation_nonce is not None and manifest.get("claim") is None:
                        record = owner
                if not matches(owner):
                    return {"complete": False, "used": True, "error": "durable checkout owner unavailable"}
                record = owner
                if "checkout_registrations" not in owner and owner.get("checkout_snapshot") is None:
                    owner["checkout_registrations"] = []
                    if recover:
                        queue.records.replace_record_at(path, owner)
                registrations = owner.get("checkout_registrations")
                if not isinstance(registrations, list):
                    return {"complete": False, "used": True, "error": "durable checkout registrations missing"}
                claim = lifetime_fence.attempt_identity(owner)
                base_claim = {k: claim[k] for k in claim if k not in ("nonce", "scope_id")}
                updated = []
                failures = []
                ended = []
                seen = set()
                last = {}
                for entry in registrations:
                    if not isinstance(entry, dict):
                        return {"complete": False, "used": True, "error": "invalid checkout registration"}
                    registration = {k: v for k, v in entry.items()
                                    if k not in ("cleanup", "completion_claim")}
                    execution_claim = entry.get("completion_claim", claim)
                    if (not isinstance(execution_claim, Mapping)
                            or set(execution_claim) != set(claim)
                            or {k: v for k, v in execution_claim.items()
                                if k not in ("nonce", "scope_id")} != base_claim):
                        return {"complete": False, "used": True,
                                "error": "checkout completion incarnation differs"}
                    directory = Path(str(registration.get("path") or ""))
                    registered_root = local_checkout_root
                    if (owner.get("inventory_id") is not None and any(
                            row.get("kind") == "checkout" and Path(row.get("path", "")) == directory.parent
                            for row in (owner.get("legacy_proof") or {}).get("scan", {}).get("roots", []))):
                        registered_root = directory.parent
                    execution = registration.get("execution_id")
                    if (not isinstance(execution, str) or not re.fullmatch(r"[0-9a-f]{32}", execution)
                            or execution in seen or registration.get("claim") != base_claim
                            or directory.parent != registered_root
                            or not CHECKOUT_TREE.fullmatch(directory.name)
                            or not directory.name.startswith(key[:12] + ".")):
                        return {"complete": False, "used": True, "error": "checkout registration identity differs"}
                    seen.add(execution)
                    unbound = "device" not in registration and "inode" not in registration
                    if unbound:
                        try:
                            parent = os.open(directory.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
                            try:
                                identity = os.fstat(parent)
                                if (identity.st_dev, identity.st_ino) != (registration.get("parent_device"), registration.get("parent_inode")):
                                    raise ValueError("checkout intent parent identity differs")
                                try:
                                    os.stat(directory.name, dir_fd=parent, follow_symlinks=False)
                                except FileNotFoundError:
                                    saved = entry.get("cleanup") or {}
                                    if (saved.get("complete") is True and saved.get("created") is False
                                            and saved.get("registration") == registration
                                            and saved.get("claim") == execution_claim):
                                        valid = dict(saved)
                                    elif recover:
                                        valid = {**registration, "complete": True, "created": False,
                                                 "ended_unix": _now(), "claim": dict(execution_claim),
                                                 "registration": registration}
                                    else:
                                        return {"complete": False, "used": True, "error": "never-created proof not persisted"}
                                else:
                                    raise ValueError("unbound checkout path exists; custody retained")
                            finally:
                                os.close(parent)
                        except (OSError, ValueError) as exc:
                            return {"complete": False, "used": True, "error": str(exc)}
                        updated.append({**registration, "completion_claim": dict(execution_claim), "cleanup": valid})
                        ended.append(valid["ended_unix"])
                        last = valid
                        continue
                    if type(registration.get("device")) is not int or type(registration.get("inode")) is not int:
                        return {"complete": False, "used": True, "error": "checkout inode registration incomplete"}
                    # Lease/outcome proof dictionaries are audit data, never authority.
                    # Only an unlink journal written from our held original fd counts.
                    receipt_path = self._root / "checkout-unlinks" / (execution + ".json")
                    receipt = record_store.read_document(receipt_path)
                    valid = None
                    if receipt is not None and receipt.get("registration") == registration:
                        parent = os.open(directory.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
                        try:
                            try:
                                named = os.stat(directory.name, dir_fd=parent, follow_symlinks=False)
                            except FileNotFoundError:
                                named = None
                            if named is not None and (named.st_dev, named.st_ino) == (registration["device"], registration["inode"]):
                                raise ValueError("registered checkout still exists beside unlink journal")
                        finally:
                            os.close(parent)
                        valid = {**registration, "complete": True, "unlinked": True,
                                 "claim": dict(execution_claim), "registration": registration,
                                 "ended_unix": receipt["ended_unix"]}
                    if valid is None and recover:
                        held = None
                        try:
                            held = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
                            info = os.fstat(held)
                            if (info.st_dev, info.st_ino) != (registration["device"], registration["inode"]):
                                raise ValueError("registered checkout was replaced")
                            verdict = _cleanup_execution_checkout(
                                registered_root, directory, owner,
                                expect=(registration["device"], registration["inode"]))
                            if verdict.get("complete") is True:
                                if os.fstat(held).st_nlink != 0:
                                    raise ValueError("registered inode remains linked")
                                queue.records.replace_record_at(receipt_path, {
                                    "registration": registration, "ended_unix": verdict["ended_unix"]})
                            valid = {**registration, **verdict, "claim": dict(execution_claim),
                                     "registration": registration}
                        except (OSError, ValueError) as exc:
                            valid = {"complete": False, "error": f"registered checkout has no unlink proof: {exc}"}
                        finally:
                            if held is not None:
                                os.close(held)
                    if valid is None:
                        valid = {"complete": False, "error": "registered checkout unlink proof missing"}
                    if valid.get("complete") is not True:
                        failures.append(str(valid.get("error") or "checkout cleanup unproved"))
                    else:
                        ended.append(valid["ended_unix"])
                    updated.append({**registration, "completion_claim": dict(execution_claim),
                                    "cleanup": valid})
                    last = valid
                result = {**last, "checkout_registrations": updated, "complete": not failures,
                          "ended_unix": max(ended, default=_now()), "error": "; ".join(failures)}
                owner["checkout_registrations"] = updated
                if registrations:
                    owner["checkout_cleanup"] = result
                # A final return is a read-only certification. Replacing its
                # finishing tombstone would invalidate the finisher's identity.
                if recover:
                    queue.records.replace_record_at(path, owner)
                if isinstance(record, dict):
                    record["checkout_registrations"] = updated
                    if registrations:
                        record["checkout_cleanup"] = result
                # A live lease owns the list. Keep its claim mirror current without
                # ever writing a successor or a separate late-finish journal.
                if recover and path == queue.records.lease_path(key):
                    mirror = queue.records.item_path(record_store.CLAIMED, key)
                    live = record_store.read_document(mirror)
                    if live is not None and claim_identity.same_claim(live, owner):
                        live["checkout_registrations"] = updated
                        if registrations:
                            live["checkout_cleanup"] = result
                        queue.records.replace_record_at(mirror, live)
                if release_ledger is not None and result["complete"]:
                    certificate = object.__new__(ReleaseCertificate)
                    observed = record_store.read_document(path)
                    _ISSUED_RELEASE_CERTIFICATES[certificate] = _ReleaseBinding(
                        self._root, release_ledger.base, holder, pb._canonical_bytes(manifest), path,
                        pb._canonical_bytes(lifetime_fence.attempt_identity(owner)),
                        pb._canonical_bytes(observed.get("checkout_registrations")),
                        None if token_names is None else frozenset(token_names), key,
                        owner.get("attempts"))
                    return {**result, "certificate": certificate}
                return result
        except (OSError, ValueError, TypeError, KeyError, record_store.PoolContractError) as exc:
            return {"complete": False, "used": True,
                    "error": f"checkout certification failed: {type(exc).__name__}: {exc}"}

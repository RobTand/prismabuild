"""Produced-batch records, histories, reservations and path authority."""
from __future__ import annotations
from collections.abc import Collection, Mapping, Sequence
import contextlib
import errno
import json
import os
from pathlib import Path
import socket
import stat
import tempfile
import time
import uuid
from prismabuild import claim_identity, core, digest_primitives, immutable_files, kept_reads
from prismabuild.record_store import Absent, KeyStanding, Moving, Present, QueueRow, RecordStore, Unknown

from . import output_contract as oc

def require_live_owner(queue, checked_instance: Mapping[str, object]
                        ) -> dict[str, object] | None:
    """Refuse new writes from a stale or absent owner, or None to proceed.

    The live CLAIMED row for the owner key must name the instance's exact
    broker attempt (nonce + scope); a live row for another attempt means
    this instance is superseded (a retry owns the key now), and no live
    row means the owner is not running. Either way `require_prewrite`
    (whose SUCCESS authorizes the first payload write) and `commit_batch`
    (which consumes durable quota and moves tier tokens) must not run. A
    corrupt or unreadable live row is unknown state that retains rather
    than authorizing. The duplicate-commit replay path runs before this
    check (it mutates nothing); cleanup paths (abort, retire, release,
    reclaim) never call this: a stale reservation lands in its own
    superseded instance directory where it can neither consume quota
    nor move tokens, while freeing headroom must work after the owner
    is gone.
    """

    owner = str(checked_instance["owner_action_key"])
    attempt = checked_instance["owner_attempt"]
    assert isinstance(attempt, dict)
    try:
        observed = RecordStore(queue.root).claimed(owner)
        if isinstance(observed, Unknown):
            assert observed.error is not None
            raise observed.error
        live = None if isinstance(observed, Absent) else observed.value
    except Exception as exc:
        return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    if live is None:
        return {"ok": False, "refusal": "owner-not-running"}
    if not isinstance(live, Mapping):
        return {"ok": False, "refusal": "unknown-retain: claim-shape"}
    control = live.get("resource_scope")
    live_nonce = live_scope = ""
    if isinstance(control, Mapping):
        candidate = control.get("nonce")
        if isinstance(candidate, str) and candidate:
            live_nonce = candidate
        for field in ("scope_id", "scope_unit", "unit"):
            unit = control.get(field)
            if isinstance(unit, str) and unit:
                live_scope = unit
                break
    if (live_nonce == attempt["nonce"] and live_scope == attempt["scope_id"]):
        return None
    return {"ok": False, "refusal": "stale-superseded-owner"}


def scopes_root(queue_root: str | Path) -> Path:
    return Path(queue_root) / "residency" / oc.OUTPUT_SCOPES_SUBDIR


def scope_dir(queue_root: str | Path, instance: Mapping[str, object]) -> Path:
    checked = oc.validate_instance(instance)
    attempt = checked["owner_attempt"]
    assert isinstance(attempt, dict)
    return (scopes_root(queue_root)
            / str(checked["owner_action_key"])
            / f"{checked['template_id']}.{attempt['nonce']}")


def declare_instance(queue_root: str | Path, instance: Mapping[str, object]) -> Path:
    """File one bound instance immutably; a conflicting body refuses."""

    from prismabuild import record_store

    checked = oc.validate_instance(instance)
    attempt = checked["owner_attempt"]
    assert isinstance(attempt, dict)
    # Indexed BEFORE the instance exists (#1053): an attempt that can
    # prewrite is always one another action's prewrite gate can find.
    _index_attempt(queue_root, str(checked["owner_action_key"]),
                   str(checked["template_id"]), str(attempt["nonce"]))
    directory = scope_dir(queue_root, checked)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "instance.json"
    raw = core._compact_ascii_lf_bytes(checked)[:-1].decode("ascii").encode() + b"\n"
    try:
        immutable_files.publish_exact(
            path,
            raw,
            where="produced-output instance",
            conflict_error=record_store.PoolContractError,
        )
    except record_store.PoolContractError as exc:
        raise oc.ProducedOutputError(
            f"a different instance is already filed for this owner/template/nonce: {exc}"
        ) from None
    return path


_TEMPLATE_BODIES: dict[tuple[str, int], dict[str, object] | None] = {}


_PREFIX_REALPATHS: dict[str, str] = {}


def _attempts_root(queue_root: str | Path) -> Path:
    return (Path(queue_root) / "residency" / oc.OUTPUT_TEMPLATES_SUBDIR
            / oc.OUTPUT_TEMPLATE_ATTEMPTS_SUBDIR)


def _index_attempt(queue_root: str | Path, owner_action_key: str,
                   template_id: str, nonce: str) -> None:
    """File one attempt's pointer, immutably and idempotently.

    Raises `ProducedOutputError` when it cannot be filed, or when a
    different body is already filed under the name.
    """

    from prismabuild import record_store

    owner = oc.output_key(owner_action_key, where="attempt owner_action_key")
    template = oc.output_name(template_id, where="attempt template_id")
    attempt = oc.output_nonce(nonce, where="attempt nonce")
    directory = _attempts_root(queue_root) / template
    raw = (core._compact_ascii_lf_bytes({"owner_action_key": owner, "nonce": attempt,
                       "template_id": template})[:-1].decode("ascii") + "\n").encode()
    try:
        directory.mkdir(parents=True, exist_ok=True)
        immutable_files.publish_exact(directory / f"{owner}.{attempt}", raw,
                                    where="produced-output attempt index",
                                    conflict_error=record_store.PoolContractError)
    except (OSError, record_store.PoolContractError) as exc:
        raise oc.ProducedOutputError(
            f"unknown-retain: attempt index: {exc}") from None


def _mark_attempt_index_complete(queue_root: str | Path) -> None:
    from prismabuild import record_store

    root = _attempts_root(queue_root)
    try:
        root.mkdir(parents=True, exist_ok=True)
        immutable_files.publish_exact(root / oc.ATTEMPT_INDEX_COMPLETE, b"1\n",
                                    where="produced-output attempt index",
                                    conflict_error=record_store.PoolContractError)
    except (OSError, record_store.PoolContractError) as exc:
        raise oc.ProducedOutputError(
            f"unknown-retain: attempt index: {exc}") from None


def _filed_templates(queue_root: str | Path) -> dict[str, dict[str, object]]:
    """Every filed template's id and output prefix, by id.

    One listing of the templates directory; a file not seen before is read
    once. Only ``template_id`` and ``output_prefix`` are read, not the whole
    schema: a template filed by an older or a newer PB still names the
    prefix its attempts write under, and that is all an owner lookup needs.
    A file with no absolute ``output_prefix`` -- not JSON, not an object,
    or without one -- is not a template `declare_template` filed, and no
    attempt can have been bound to it (`bind_instance` validates the
    template), so it names no paths and is skipped. A file that cannot be
    read raises `ProducedOutputError`: whether it covers a path is unknown,
    and an answer that leaves it out could miss an owner.
    """

    directory = Path(queue_root) / "residency" / oc.OUTPUT_TEMPLATES_SUBDIR
    try:
        with os.scandir(directory) as iterator:
            listed = sorted((entry.name, entry.inode()) for entry in iterator
                            if entry.name.endswith(".json")
                            and not entry.name.startswith("."))
    except FileNotFoundError:
        return {}
    except OSError as exc:
        raise oc.ProducedOutputError(f"unknown-retain: templates: {exc}") from None
    found: dict[str, dict[str, object]] = {}
    for name, inode in listed:
        key = (str(directory / name), inode)
        if key not in _TEMPLATE_BODIES:
            try:
                raw = (directory / name).read_text()
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise oc.ProducedOutputError(
                    f"unknown-retain: template {name}: {exc}") from None
            try:
                body = json.loads(raw)
            except ValueError:
                body = None
            prefix = body.get("output_prefix") if isinstance(body, Mapping) else None
            if not isinstance(prefix, str) or not os.path.isabs(prefix):
                _TEMPLATE_BODIES[key] = None
            else:
                named = body.get("template_id")
                _TEMPLATE_BODIES[key] = {
                    "template_id": (named if isinstance(named, str) and named
                                    else name[:-len(".json")]),
                    "output_prefix": prefix}
        body = _TEMPLATE_BODIES[key]
        if body is not None:
            found[str(body["template_id"])] = body
    return found


def _resolved_prefix(prefix: str) -> str:
    resolved = _PREFIX_REALPATHS.get(prefix)
    if resolved is None:
        resolved = os.path.realpath(prefix)
        _PREFIX_REALPATHS[prefix] = resolved
    return resolved


def _prefixes_overlap(first: str, second: str) -> bool:
    """Whether one output prefix contains the other, symlinks resolved."""

    a, b = _resolved_prefix(first), _resolved_prefix(second)
    common = os.path.commonpath([a, b])
    return common == a or common == b


def overlapping_templates(queue_root: str | Path,
                              template: Mapping[str, object]) -> list[str]:
    """This template's id and every filed template whose prefix overlaps it."""

    own = str(template["output_prefix"])
    ids = {str(template["template_id"])}
    for template_id, body in _filed_templates(queue_root).items():
        if _prefixes_overlap(own, str(body["output_prefix"])):
            ids.add(template_id)
    return sorted(ids)


def _output_prefix_lock_order(own_prefix: str,
                              templates: Mapping[str, Mapping[str, object]]
                              ) -> list[str]:
    """The prefixes whose locks guard ``own_prefix``'s paths, in order (#1063).

    ``own_prefix`` and the prefix of each of ``templates`` that overlaps it
    (`_prefixes_overlap`), once per lock -- a stage ownership lock is named
    by the absolute path (`pool.PoolQueue.stage_ownership_lock`) -- sorted
    by that path.
    """

    held = {str(Path(own_prefix).absolute()): own_prefix}
    for body in templates.values():
        other = str(body["output_prefix"])
        if _prefixes_overlap(own_prefix, other):
            held.setdefault(str(Path(other).absolute()), other)
    return [held[name] for name in sorted(held)]


def path_locks(
        queue, instance: Mapping[str, object], *,
        templates: Mapping[str, Mapping[str, object]] | None = None,
        reads: "LedgerReads | None" = None):
    """Every ownership lock that guards this instance's origin paths (#1063).

    A path is named by each template whose output prefix contains it, and
    any two such prefixes nest. Every step that files a claim on an origin
    path (`require_prewrite`, `commit_batch`, `commit_origin_batch`) or
    deletes one (`_retire_consumed_batch`) holds, across its check and its
    act, the lock of this instance's prefix and the lock of every filed
    template's prefix that overlaps it. Two such steps on one path always
    share a lock: a template is filed before any attempt of it can bind
    (`PoolQueue.publish`) and is never removed, so of two steps the one that
    lists the templates later finds the other's template and takes its
    lock, and the other takes its own. Each used to take its own lock only,
    so a retirement could delete a file that a successor under a nested
    prefix had committed between the retirement's owner read and its
    delete.

    The locks are taken in one order, sorted by absolute path, and a holder
    asks for no other lock of the family while it holds them. Every other
    holder of an ownership lock holds one and asks for no other
    (`pool.PoolQueue.stage_ownership_lock`), so no two holders can each
    hold a lock the other waits for.

    ``templates`` is a listing the caller took after this instance's
    template was filed (the retirement tick's, `LedgerReads.templates`); by
    default the templates are listed now. The listing is taken here, before
    any lock: a failed one raises `ProducedOutputError`, since which locks
    guard the paths is then unknown. With ``reads``, the order is the
    tick's memoized one for this listing (`LedgerReads.lock_order`, #992),
    which a newly filed template invalidates. Returns the context manager
    that holds the locks.
    """

    listed = _filed_templates(queue.root) if templates is None else templates
    own = str(instance["output_prefix"])
    order = (reads.lock_order(own, listed) if reads is not None
             else _output_prefix_lock_order(own, listed))
    return _hold_prefixes(queue, order)


@contextlib.contextmanager
def _hold_prefixes(queue, prefixes: Sequence[str]):
    with contextlib.ExitStack() as stack:
        for prefix in prefixes:
            stack.enter_context(queue.stage_ownership_lock(prefix))
        yield


def template_attempts(queue_root: str | Path, template_ids: Collection[str]
                       ) -> list[tuple[str, str, str]]:
    """``(owner, template_id, nonce)`` for every attempt of these templates.

    From the attempt index once a tick has marked it complete: one listing
    per template. Before that, from the scopes themselves: one listing of
    the owners and one per owner. A listing that fails raises
    `ProducedOutputError`, never an empty answer.
    """

    wanted = set(template_ids)
    found: set[tuple[str, str, str]] = set()
    root = _attempts_root(queue_root)
    if (root / oc.ATTEMPT_INDEX_COMPLETE).exists():
        for template_id in sorted(wanted):
            try:
                names = os.listdir(root / template_id)
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise oc.ProducedOutputError(
                    f"unknown-retain: attempt index: {exc}") from None
            for name in names:
                owner, dot, nonce = name.partition(".")
                if (dot and len(owner) == 64 and len(nonce) == oc.NONCE_HEX_LENGTH
                        and not name.startswith(".")):
                    found.add((owner, template_id, nonce))
        return sorted(found)
    scopes_root = Path(queue_root) / "residency" / oc.OUTPUT_SCOPES_SUBDIR
    try:
        owners = scope_owners(scopes_root)
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise oc.ProducedOutputError(f"unknown-retain: scopes: {exc}") from None
    for owner in owners:
        try:
            names = os.listdir(scopes_root / owner)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise oc.ProducedOutputError(
                f"unknown-retain: scopes {owner[:12]}: {exc}") from None
        for name in names:
            template_id, dot, nonce = name.rpartition(".")
            if dot and template_id in wanted and len(nonce) == oc.NONCE_HEX_LENGTH:
                found.add((owner, template_id, nonce))
    return sorted(found)


def attempt_scope(queue_root: str | Path, owner: str, template_id: str,
                   nonce: str) -> Path:
    return (Path(queue_root) / "residency" / oc.OUTPUT_SCOPES_SUBDIR / owner
            / f"{template_id}.{nonce}")


def _committed_paths(queue_root, entry, batch_id, *, scope=None,
                     instance=None, template=None):
    """Captured committed paths, including the immutable fallback for legacy rows."""
    indexed = entry.get("paths")
    if isinstance(indexed, list):
        return [str(item) for item in indexed]
    if instance is None:
        try:
            instance = oc.validate_instance(json.loads(
                (scope / "instance.json").read_text()))
            template = oc.validate_template(json.loads(
                (Path(queue_root) / "residency" / oc.OUTPUT_TEMPLATES_SUBDIR
                 / f"{instance['template_id']}.json").read_text()))
        except (OSError, ValueError) as exc:
            raise oc.ProducedOutputError(
                f"unknown-retain: {scope.name}: {exc}") from None
    _filed, sealed = BatchLedger.open(
        queue_root, instance, template, entry, batch_id)
    return [str(desc["path"]) for desc in sealed]


def _capture_attempt_paths(queue_root: str | Path, scope: Path,
                         batches: Mapping[str, object]
                         ) -> tuple[dict[str, str], dict[str, str]]:
    """``(committed, prewritten)``: the origin paths one attempt names.

    Each maps a path to a batch id. ``committed`` holds every path of a
    committed batch whose origin is not reclaimed, staged or origin-only,
    retired or not: the commit recorded that file as the batch's.
    ``prewritten`` holds every path of a prewrite record still filed, which
    is permission to write it. ``batches`` is the attempt's commitments.
    Raises `ProducedOutputError` on a record that cannot be read.
    """

    committed: dict[str, str] = {}
    for batch_id, entry in sorted(batches.items()):
        if not isinstance(entry, Mapping):
            raise oc.ProducedOutputError(
                f"unknown-retain: {scope.name}: bad committed batch {batch_id!r}")
        if entry.get("origin_reclaimed"):
            continue
        paths = _committed_paths(queue_root, entry, batch_id, scope=scope)
        for path in paths:
            committed.setdefault(path, batch_id)
    prewritten: dict[str, str] = {}
    directory = scope / "prewrites"
    try:
        names = sorted(name for name in os.listdir(directory)
                       if name.endswith(".prewrite.json"))
    except FileNotFoundError:
        names = []
    except OSError as exc:
        raise oc.ProducedOutputError(
            f"unknown-retain: {scope.name}: {exc}") from None
    for name in names:
        batch_id = name[:-len(".prewrite.json")]
        record = read_prewrite(directory / name)
        if record is None:
            continue
        reserved = record.get("paths")
        if not isinstance(reserved, list):
            raise oc.ProducedOutputError(
                f"unknown-retain: {scope.name}: bad prewrite record {batch_id!r}")
        for path in reserved:
            prewritten.setdefault(str(path), batch_id)
    return committed, prewritten


def _claimed_nonce(queue, owner: str) -> str | None:
    """The nonce of the attempt holding ``owner``'s claim.

    ``""`` when nothing holds it, ``None`` when the claim cannot be read or
    names no attempt: unknown, which the gate treats as live.
    """

    try:
        observed = RecordStore(queue.root).claimed(owner)
        if isinstance(observed, Unknown):
            assert observed.error is not None
            raise observed.error
        live = None if isinstance(observed, Absent) else observed.value
    except Exception:
        return None
    if live is None:
        return ""
    control = live.get("resource_scope") if isinstance(live, Mapping) else None
    nonce = control.get("nonce") if isinstance(control, Mapping) else None
    return nonce if isinstance(nonce, str) and nonce else None


def foreign_write_owner(queue, checked_instance: Mapping[str, object],
                             checked_template: Mapping[str, object],
                             planned_paths: Sequence[str]
                             ) -> dict[str, object] | None:
    """Another action's live attempt that owns one of these paths (#1053).

    The cross-action half of `own_write_owner`. The attempts it reads are
    those of every template whose output prefix overlaps this one's
    (`template_attempts`), less this owner key's: only one attempt of a key
    holds its claim, and this one does. Each other key's claim is read once.
    An attempt that does not hold its key's claim cannot prewrite or commit
    (`require_live_owner`), so it owns nothing a new write could disturb,
    and its records are not read at all: a dead attempt's commitments cost
    the gate nothing. A live attempt, or one whose claim cannot be read,
    owns every path its committed batches name (unless reclaimed) and every
    path a prewrite record of it names (`_capture_attempt_paths`).

    Returns the refusal's details for the first owned path in sorted order,
    or ``None``. Raises `ProducedOutputError` when an attempt that may be
    live cannot be read.
    """

    wanted = set(planned_paths)
    own = str(checked_instance["owner_action_key"])
    attempts = template_attempts(
        queue.root, overlapping_templates(queue.root, checked_template))
    claims: dict[str, str | None] = {}
    for owner, template_id, nonce in attempts:
        if owner == own:
            continue
        if owner not in claims:
            claims[owner] = _claimed_nonce(queue, owner)
        claimed = claims[owner]
        if claimed == "" or (claimed is not None and claimed != nonce):
            continue
        scope = attempt_scope(queue.root, owner, template_id, nonce)
        batches = BatchLedger.at_scope(scope).capture()["batches"]
        assert isinstance(batches, Mapping)
        committed, prewritten = _capture_attempt_paths(queue.root, scope, batches)
        for path in sorted(wanted):
            for kind, owned in (("batch", committed), ("prewrite", prewritten)):
                if path in owned:
                    return {"path": path, "owner_action_key": owner,
                            "owner_nonce": nonce, "owner_kind": kind,
                            "owner_batch_id": owned[path],
                            "owner_state": "live" if claimed else "unknown"}
    return None


def _file_stat(path: Path | str) -> os.stat_result | None:
    """One ``stat``; absent is ``None``, any other failure raises."""

    try:
        return os.stat(path)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise oc.ProducedOutputError(f"unknown-retain: {exc}") from None


def _file_version(path: Path | str) -> tuple[int, ...] | None:
    """The C11 file version, or ``None`` when absent.

    Any other failure to ``stat`` raises `ProducedOutputError`.
    """

    info = _file_stat(path)
    return None if info is None else kept_reads.metadata_version(info)


class LedgerReads:
    """What one origin-retirement tick has read, so it reads each thing once.

    Commitments are kept by the file's trusted version (`file_version`
    over a fence read before the stat, #1045) and handed back while a fresh
    ``stat`` returns the same version: every writer replaces the file by
    rename (`BatchLedger.file`), so a changed file has another version,
    and a version no fence vouches for is never kept.  The ``stat`` comes
    before the read, so a file replaced between the two is read again next
    time, never remembered under the new version with the old bytes.  The
    mapping handed back is a copy: a retirement's read-modify-write mutates
    its own container, never what the next batch reads (#992).  What an
    attempt owns (`_capture_attempt_paths`) is kept the same way, under the
    versions of its commitments and of each prewrite record it has.  An
    owner key's standing (`RecordStore.standing`) and its funding census are
    read at most once per tick.

    ``forget(scope)`` drops a scope's commitments immediately before a write
    is attempted, so a decision that could not be filed is never handed to a
    later batch.  ``lock_order`` keeps one output-prefix lock order per
    template listing the tick has seen: the retirement asks for the same
    instance once per due batch, and a template filed between two asks
    changes the listing and so the key.
    """

    def __init__(self, queue, *, lease_timeout_s: float, trust=None) -> None:
        self.queue = queue
        self.lease_timeout_s = lease_timeout_s
        self.trust = kept_reads.filesystem_trust if trust is None else trust
        self._commitments: dict[str, tuple[tuple[int, ...], dict[str, object]]] = {}
        self._owned: dict[str, tuple[object, dict[str, str], dict[str, str]]] = {}
        self._generations: dict[str, KeyStanding] = {}
        self._census: dict[str, tuple[list[dict], bool]] = {}
        self._templates: dict[str, dict[str, object]] | None = None
        self._lock_orders: dict[tuple[str, tuple[tuple[str, str], ...]],
                                list[str]] = {}
        #: Whole-document reads of a scope's commitments, and the reads
        #: served from the version already parsed this tick (#992).
        self.commitments_read = 0
        self.commitments_reused = 0
        #: Output-prefix lock orders derived (`lock_order`'s misses).
        self.lock_orders = 0

    def batches(self, scope: Path) -> dict[str, object]:
        path = BatchLedger.at_scope(scope).path
        fence = (self.trust.file_fence()
                 if kept_reads.file_versions_enabled() else None)
        info = _file_stat(path)
        version = (None if info is None else
                   self.trust.file_version(info, fence, path=path))
        kept = self._commitments.get(str(path))
        if (fence is not None and version is not None
                and kept is not None and kept[0] == version):
            self.commitments_reused += 1
            return dict(kept[1])
        try:
            batches = BatchLedger(path).capture()["batches"]
        except oc.ProducedOutputError:
            self._commitments.pop(str(path), None)
            raise
        assert isinstance(batches, dict)
        self.commitments_read += 1
        if info is None:
            self._commitments.pop(str(path), None)
            return dict(batches)
        # Kept only at a version no later write can reproduce (#1045): the
        # fence was read before the stat, so a change after it moved ctime
        # to or past the fence, and a refused version is read again.
        keepable = self.trust.file_version(info, fence, path=path)
        if keepable is None:
            self._commitments.pop(str(path), None)
        else:
            self._commitments[str(path)] = (keepable, batches)
        return dict(batches)

    def entries(self, scope: Path):
        """The tick's captured batch answers, with the same version fence."""
        return BatchCapture(BatchLedger.at_scope(scope),
                            {"batches": self.batches(scope)})

    def forget(self, scope: Path) -> None:
        """Drop this scope's commitments before its record is written.

        Called immediately before every commitments write a retirement makes
        (#992): if the write fails, the bytes on disk are the ones before
        it, and the parse that was not filed must not answer a later batch.
        """

        self._commitments.pop(str(BatchLedger.at_scope(scope).path), None)

    def lock_order(self, own_prefix: str,
                   templates: Mapping[str, Mapping[str, object]]
                   ) -> list[str]:
        """The prefixes guarding ``own_prefix``, for this template listing.

        `_output_prefix_lock_order` scans every template with a `commonpath`
        call, and the tick's retirement asks for the same instance prefix
        once per due batch (#992).  The answer is kept for the tick under the
        listing's own content -- a template filed between two asks changes
        the key -- so a stale derived order is never reused.  The caller
        passes the listing it will read path owners under.
        """

        fingerprint = tuple(sorted(
            (str(template_id), str(body["output_prefix"]))
            for template_id, body in templates.items()))
        key = (str(own_prefix), fingerprint)
        found = self._lock_orders.get(key)
        if found is None:
            found = _output_prefix_lock_order(str(own_prefix), templates)
            self._lock_orders[key] = found
            self.lock_orders += 1
        return found

    def owned(self, scope: Path
              ) -> tuple[object, dict[str, str], dict[str, str]]:
        """``(fingerprint, committed, prewritten)`` for one attempt.

        `_capture_attempt_paths`, re-derived only when the fingerprint -- the
        versions of the commitments and of each prewrite record, taken
        before either is read -- has changed.
        """

        directory = scope / "prewrites"
        try:
            names = sorted(name for name in os.listdir(directory)
                           if name.endswith(".prewrite.json"))
        except FileNotFoundError:
            names = []
        except OSError as exc:
            raise oc.ProducedOutputError(
                f"unknown-retain: {scope.name}: {exc}") from None
        fingerprint = (_file_version(scope / "commitments.json"),
                       tuple((name, _file_version(directory / name))
                             for name in names))
        kept = self._owned.get(str(scope))
        if kept is not None and kept[0] == fingerprint:
            return kept
        committed, prewritten = _capture_attempt_paths(
            self.queue.root, scope, self.batches(scope))
        self._owned[str(scope)] = (fingerprint, committed, prewritten)
        return fingerprint, committed, prewritten

    def generation(self, owner: str) -> KeyStanding:
        if owner not in self._generations:
            self._generations[owner] = RecordStore(self.queue.root).standing(owner)
        return self._generations[owner]

    def census(self, owner: str) -> tuple[list[dict], bool]:
        if owner not in self._census:
            try:
                self._census[owner] = self.queue.output_census_for_owner(owner)
            except Exception:
                self._census[owner] = ([], True)
        return self._census[owner]

    def templates(self) -> dict[str, dict[str, object]]:
        if self._templates is None:
            self._templates = _filed_templates(self.queue.root)
        return self._templates

    def path_owners(self, instance: Mapping[str, object],
                    template: Mapping[str, object], *,
                    relist: bool = False) -> "RetirementOwners":
        """Every other attempt's claim on paths under this template's prefix.

        The attempts are those of every template whose output prefix
        overlaps this one's (`template_attempts`), less this instance's
        own. Read now -- the index, and each attempt's commitments and
        prewrite records through this tick's versions -- so a caller under
        the output-prefix locks sees every commit and prewrite filed under
        them before they were taken.

        ``relist`` lists the templates again first, and the tick keeps the
        new listing. A caller that deletes on this answer passes it, under
        its locks (#1063): a template filed after the tick first listed them
        can have an attempt that committed one of these paths before the
        locks were granted, and the tick's first listing does not name it.
        """

        if relist:
            self._templates = None
        own_template = str(template["template_id"])
        ids = {own_template}
        for template_id, body in self.templates().items():
            if _prefixes_overlap(str(template["output_prefix"]),
                                 str(body["output_prefix"])):
                ids.add(template_id)
        attempt = instance["owner_attempt"]
        assert isinstance(attempt, dict)
        mine = (str(instance["owner_action_key"]), own_template,
                str(attempt["nonce"]))
        owners = RetirementOwners()
        for owner, template_id, nonce in template_attempts(
                self.queue.root, sorted(ids)):
            if (owner, template_id, nonce) == mine:
                continue
            scope = attempt_scope(self.queue.root, owner, template_id, nonce)
            version, committed, prewritten = self.owned(scope)
            if not committed and not prewritten:
                continue
            state = (attempt_state(self.generation(owner), nonce)
                     if prewritten else "")
            unknown = None
            if state == "unknown":
                # Why, for the report a hold by it files (#1065). Whether it
                # is orphaned turns with the clock, so it is part of the
                # fingerprint: a kept decision is taken again when it turns.
                unknown = {**_unknown_attempt_detail(
                    self.generation(owner), nonce,
                    last_write_ns=_newest_write_ns(version),
                    now=time.time(), lease_timeout_s=self.lease_timeout_s), "template_id": template_id}
            owners.add(owner, nonce, foreign=owner != mine[0],
                       committed=committed, prewritten=prewritten,
                       state=state, unknown=unknown)
            owners.fingerprint.append((
                owner, template_id, nonce, version, state,
                None if unknown is None
                else (unknown["why"], unknown["orphaned"])))
        return owners


class RetirementOwners:
    """Who else names an origin path, per attempt, asked one path at a time.

    A path is *committed* by another attempt when its commit names it (the
    commit recorded that file), or when a prewrite of an attempt that
    succeeded names it. It is *pending* when a prewrite of an attempt that
    can still commit names it: live, or in a state that cannot be read. A
    dead attempt's prewrite names nothing: it can never commit.
    """

    def __init__(self) -> None:
        self._attempts: list[tuple[str, str, bool, dict[str, str],
                                   dict[str, str], str]] = []
        #: Why each attempt whose state is ``unknown`` reads so (#1065), by
        #: ``(owner, nonce)``: ``{"why", "orphaned", "last_write_unix",
        #: "template_id"}`` (`_unknown_attempt_detail`).
        self._unknown: dict[tuple[str, str], dict[str, object]] = {}
        #: What the answers were read from: each attempt that names a path,
        #: its records' versions and its state. Equal fingerprints give
        #: equal answers.
        self.fingerprint: list[tuple[object, ...]] = []

    def add(self, owner: str, nonce: str, *, foreign: bool,
            committed: dict[str, str], prewritten: dict[str, str],
            state: str, unknown: Mapping[str, object] | None = None) -> None:
        self._attempts.append((owner, nonce, foreign, committed, prewritten,
                               state))
        if unknown is not None:
            self._unknown[(owner, nonce)] = dict(unknown)

    @staticmethod
    def _note(owner: str, nonce: str, batch_id: str,
              foreign: bool) -> dict[str, str]:
        # The #949 shape for a retry of the same key; another key is named.
        note = {"nonce": nonce, "batch_id": batch_id}
        if foreign:
            note["owner_action_key"] = owner
        return note

    def committed(self, path: str) -> dict[str, str] | None:
        for owner, nonce, foreign, committed, prewritten, state in self._attempts:
            batch_id = committed.get(path)
            if batch_id is None and state == "succeeded":
                batch_id = prewritten.get(path)
            if batch_id is not None:
                return self._note(owner, nonce, batch_id, foreign)
        return None

    def pending(self, path: str) -> dict[str, str] | None:
        for owner, nonce, foreign, _committed, prewritten, state in self._attempts:
            if state in oc.ENDED_ATTEMPT_STATES:
                continue
            batch_id = prewritten.get(path)
            if batch_id is not None:
                return self._note(owner, nonce, batch_id, foreign)
        return None

    def unknown_holders(self, path: str) -> list[dict[str, object]]:
        """Each attempt holding ``path`` whose state could not be read (#1065).

        `pending` holds a path for an attempt that is ``live`` or
        ``unknown``; a live one is a writer at work and holds quietly. An
        unknown one -- no queue row, a row that is queued or being moved, a
        row that cannot be read -- may never commit or end, so a hold it
        causes is reported: who holds it (``owner_action_key``, ``nonce``,
        ``batch_id``, ``foreign``), ``why`` its state is unknown and whether
        it is ``orphaned`` (`_unknown_attempt_detail`).
        """

        found: list[dict[str, object]] = []
        for owner, nonce, foreign, _committed, prewritten, state in self._attempts:
            if state != "unknown":
                continue
            batch_id = prewritten.get(path)
            if batch_id is None:
                continue
            detail = self._unknown.get((owner, nonce), {})
            found.append({"path": path, "owner_action_key": owner,
                          "nonce": nonce, "batch_id": batch_id,
                          "foreign": foreign, "state": state, **detail})
        return found


def _unknown_attempt_detail(generation: KeyStanding,
                            nonce: str, *, last_write_ns: int | None,
                            now: float, lease_timeout_s: float) -> dict[str, object]:
    """Why `attempt_state` reads ``unknown`` for this attempt (#1065).

    ``why`` is one of ``no-queue-row`` (no record of the key in any queue
    directory), ``queued`` (a ``ready`` row: the key waits for a claim),
    ``moving`` (a claim being moved), ``claim-names-no-nonce``,
    ``done-names-no-nonce``, ``done-status-<status>``,
    ``cache-hit-by-another-attempt`` or ``queue-row-unreadable``.

    ``orphaned`` is true for ``no-queue-row`` once the attempt's newest
    record (its commitments or a prewrite record, ``last_write_ns``) is
    older than the lease timeout (`pool.LEASE_TIMEOUT_S`). A running attempt
    holds a claimed row that its heartbeat renews within that timeout, so
    an attempt with no row that wrote nothing for as long cannot be one.
    The hold it causes stays in place; only the report says so.
    """

    from prismabuild import record_store

    if isinstance(generation, Absent):
        why = "no-queue-row"
    elif isinstance(generation, Unknown):
        why = "queue-row-unreadable"
    elif isinstance(generation.value, Moving):
        why = "moving"
    else:
        state, record = generation.value.state, generation.value.record
        if state == record_store.READY:
            why = "queued"
        elif state == record_store.CLAIMED:
            why = "claim-names-no-nonce"
        elif state == record_store.DONE:
            status = str(record.get("status"))
            if status not in ("executed", "cache_hit"):
                why = f"done-status-{status}"
            elif not record_nonce(record):
                why = "done-names-no-nonce"
            else:
                why = "cache-hit-by-another-attempt"
        else:
            why = "queue-row-unreadable"
    last_write = (None if last_write_ns is None
                  else round(last_write_ns / 1e9, 3))
    orphaned = (why == "no-queue-row" and last_write is not None
                and now - last_write > lease_timeout_s)
    return {"why": why, "orphaned": orphaned, "last_write_unix": last_write}


def _newest_write_ns(version: object) -> int | None:
    """The newest mtime in a `LedgerReads.owned` fingerprint, or None."""

    newest: int | None = None

    def visit(item: object) -> None:
        nonlocal newest
        if isinstance(item, kept_reads.FileVersion):
            newest = (item.mtime_ns if newest is None
                      else max(newest, item.mtime_ns))
            return
        if isinstance(item, tuple):
            for part in item:
                visit(part)

    visit(version)
    return newest


def unknown_holder_report(holders: Sequence[Mapping[str, object]],
                           queue_root: str | Path) -> dict[str, object]:
    """An `ORIGIN_HELD_BY_UNKNOWN_EVENT`'s body, without its subject (#1065).

    One entry per holding attempt and path, each with the attempt's
    ``prewrite_record`` (the record whose removal frees an orphaned hold),
    and ``reason``: ``held-by-orphaned-attempt`` when any holder is
    orphaned, else ``held-by-unknown-attempt``, and the ``remedy`` when one
    is. ``last_write_unix`` moves only when the holder writes a record, so
    the event, and the signature it is reported once per change by, stays
    the same while nothing changes.
    """

    listed: list[dict[str, object]] = []
    for holder in holders:
        item = dict(holder)
        template_id = str(item.pop("template_id", "") or "")
        if template_id:
            item["prewrite_record"] = str(
                attempt_scope(queue_root, str(item["owner_action_key"]),
                               template_id, str(item["nonce"]))
                / "prewrites" / f"{item['batch_id']}.prewrite.json")
        listed.append(item)
    listed.sort(key=lambda item: (str(item["path"]),
                                  str(item["owner_action_key"]),
                                  str(item["nonce"])))
    orphaned = any(item.get("orphaned") for item in listed)
    event: dict[str, object] = {
        "reason": ("held-by-orphaned-attempt" if orphaned
                   else "held-by-unknown-attempt"),
        "holders": listed}
    if orphaned:
        event["remedy"] = oc.ORPHANED_HOLDER_REMEDY
    return event


def unknown_path_holders(owners: RetirementOwners, paths: Sequence[object]
                          ) -> list[dict[str, object]]:
    """The retirement's hold test over ``paths``, for the listing (#1065).

    Empty when no path another attempt can still commit holds is held by an
    attempt whose state is unknown; a path already committed by another
    attempt holds nothing, as in the retirement.
    """

    found: list[dict[str, object]] = []
    for path in sorted(str(item) for item in paths):
        if owners.committed(path) is None and owners.pending(path) is not None:
            found.extend(owners.unknown_holders(path))
    return found


def prewrites_dir(queue_root: str | Path,
                   instance: Mapping[str, object]) -> Path:
    return scope_dir(queue_root, instance) / "prewrites"


def _funding_dir(queue_root: str | Path,
                 instance: Mapping[str, object]) -> Path:
    return scope_dir(queue_root, instance) / "funding"


def read_prewrite(path: Path) -> dict[str, object] | None:
    """One outstanding prewrite record, None when absent.

    Raises ProducedOutputError on corrupt/unreadable (unknown state, never
    an empty reservation). Class bytes are validated exact here so no
    caller can mint quota from a malformed record.
    """

    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise oc.ProducedOutputError(
            f"prewrite record unreadable: {exc}") from None
    if not isinstance(raw, Mapping):
        raise oc.ProducedOutputError("prewrite record is corrupt")
    record = dict(raw)
    record["class_bytes"] = oc.output_class_bytes(
        record.get("class_bytes"), where="prewrite class_bytes")
    paths = record.get("paths", [])
    if not isinstance(paths, list) or any(
            not isinstance(p, str) or not p for p in paths):
        raise oc.ProducedOutputError("prewrite paths are corrupt")
    record["paths"] = list(paths)
    return record


def _outstanding_sums(queue_root: str | Path, instance: Mapping[str, object],
                      exclude_batch_id: str) -> dict[str, int]:
    """Committed + outstanding class bytes, excluding one batch's own record.

    Outstanding prewrites are real reservations: writers may already hold
    HDD bytes against them. A corrupt prewrite file fails the whole
    accounting closed (unknown, never zero).
    """

    checked = oc.validate_instance(instance)
    try:
        commitments = BatchLedger.for_instance(queue_root, checked).capture()
    except oc.ProducedOutputError as exc:
        raise oc.ProducedOutputError(f"unknown-retain: {exc}") from None
    sums = _class_sums(commitments["batches"])
    directory = prewrites_dir(queue_root, checked)
    try:
        with os.scandir(directory) as iterator:
            names = sorted(entry.name for entry in iterator
                           if entry.name.endswith(".prewrite.json"))
    except FileNotFoundError:
        return sums
    except OSError as exc:
        raise oc.ProducedOutputError(f"unknown-retain: {exc}") from None
    for name in names:
        if name == f"{exclude_batch_id}.prewrite.json":
            continue
        record = read_prewrite(directory / name)
        if record is None:
            continue
        class_bytes = record.get("class_bytes")
        if not isinstance(class_bytes, Mapping):
            raise oc.ProducedOutputError("unknown-retain: bad prewrite record")
        for cls in sums:
            sums[cls] += int(class_bytes.get(cls, 0) or 0)
    return sums


def publish_prewrite(queue, instance: Mapping[str, object], *,
                             batch_id: str, tier: str,
                             class_bytes: Mapping[str, int],
                             paths: Sequence[str]) -> Path:
    """File one outstanding prewrite reservation immutably.

    The single construction of a prewrite record, so the gate above it
    and any caller that must file one cannot drift on its shape.
    Raises whatever `_publish_immutable` raises for a conflicting body;
    `require_prewrite` owns the idempotent-replay interpretation.
    """

    from prismabuild import record_store

    directory = prewrites_dir(queue.root, instance)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{batch_id}.prewrite.json"
    record = {"batch_id": batch_id, "tier": tier,
              "class_bytes": dict(class_bytes), "paths": list(paths),
              "owner_action_key": str(instance["owner_action_key"]),
              "owner_attempt": dict(instance["owner_attempt"])}
    raw = core._compact_ascii_lf_bytes(record)[:-1].decode("ascii").encode() + b"\n"
    immutable_files.publish_exact(
        path,
        raw,
        where="produced-output prewrite",
        conflict_error=record_store.PoolContractError,
    )
    return path


def own_write_owner(queue_root: str | Path,
                     checked_instance: Mapping[str, object],
                     checked_template: Mapping[str, object],
                     planned_paths: Sequence[str],
                     exclude_batch_id: str) -> tuple[str, str, str] | None:
    """The live writer that already owns one of these origin paths.

    Staged material identity is keyed by the ORIGIN PATH, so a path
    belongs to at most one live writer: a second writer replacing those
    bytes would invalidate the owner's published material and anything
    fenced on it, which the mover refuses outright. That refusal lands
    only after the bytes are written and the batch committed, so
    ownership is answered HERE, at the write-authorization gate.

    Two kinds of owner, both live:

    * a committed batch whose ACTIVE materialization is not retired -- its
      staged copy stands (the first one, or a restaged successor) and its
      entries name the paths;
    * an outstanding prewrite for another batch id -- it already holds
      permission to write those paths, whether or not it has committed.

    Ownership ends at retirement, which evicts the staged copy; the
    durable charge outlives it but does not reserve the name, so a
    retired batch's paths regenerate. Unknown state fails closed: an
    unreadable batch record or prewrite raises rather than reading as
    unowned, because absent metadata never proves an absent owner.

    Returns ``(path, owner_kind, owner_batch_id)`` for the first owned
    path in sorted order, or None when every path is free.
    """

    wanted = set(planned_paths)
    if not wanted:
        return None
    commitments = BatchLedger.for_instance(queue_root, checked_instance).capture()
    batches = commitments["batches"]
    assert isinstance(batches, Mapping)
    owners: dict[str, tuple[str, str]] = {}
    for owner_id in sorted(batches):
        entry = batches[owner_id]
        if owner_id == exclude_batch_id:
            continue
        if not isinstance(entry, Mapping):
            raise oc.ProducedOutputError(
                f"unknown-retain: bad committed batch {owner_id!r}")
        # `retired` on the entry describes only the FIRST materialization. A
        # batch that has been restaged has a LIVE stage copy under a successor
        # mover reading these very origin files, so authorizing a second
        # writer over them here would corrupt a copy in flight. Ownership
        # follows the active materialization.
        #
        # An origin-only batch (#912) has no copy, and its origin files are
        # what a consumer declared and will stage.  It owns its paths until
        # `reclaim_origin` proves them gone; regenerating them earlier would
        # change bytes under a declared read.
        if entry.get("origin_only") is True:
            if entry.get("origin_reclaimed"):
                continue
        elif BatchFacets(entry).stage_retired:
            continue
        owned_paths = _committed_paths(
            queue_root, entry, owner_id,
            instance=checked_instance, template=checked_template)
        for owned_path in owned_paths:
            owners.setdefault(owned_path, ("batch", owner_id))
    directory = prewrites_dir(queue_root, checked_instance)
    try:
        with os.scandir(directory) as iterator:
            names = sorted(entry.name for entry in iterator
                           if entry.name.endswith(".prewrite.json"))
    except FileNotFoundError:
        names = []
    except OSError as exc:
        raise oc.ProducedOutputError(f"unknown-retain: {exc}") from None
    for name in names:
        owner_id = name[:-len(".prewrite.json")]
        if owner_id == exclude_batch_id:
            continue
        record = read_prewrite(directory / name)
        if record is None:
            continue
        reserved = record.get("paths")
        if not isinstance(reserved, list):
            raise oc.ProducedOutputError("unknown-retain: bad prewrite record")
        for reserved_path in reserved:
            owners.setdefault(str(reserved_path), ("prewrite", owner_id))
    for candidate in sorted(wanted):
        owner = owners.get(candidate)
        if owner is not None:
            return (candidate, owner[0], owner[1])
    return None


def planned_omitted_absent(prewrite: Mapping[str, object],
                            sealed: list[dict[str, object]]) -> bool:
    """Every planned path the descriptors omit is proven absent.

    The conservative prewrite admitted a planned superset; the paths the
    commit's descriptors do NOT cover must be gone by commit time (a
    written temporary left behind would otherwise lose its charge with the
    prewrite consumed). Absence is lstat-only: no payload read, no hash.
    Present or unreadable retains (False); proven absent passes.
    """

    actual = {str(d["path"]) for d in sealed}
    for planned in prewrite.get("paths", []):
        text = str(planned)
        if text in actual:
            continue
        try:
            os.lstat(text)
        except FileNotFoundError:
            continue
        return False
    return True


def actual_within_ceiling(prewrite: Mapping[str, object],
                           sealed: list[dict[str, object]]) -> bool:
    """Actual per-class bytes at or under the admitted prewrite ceiling.

    The prewrite admits per-class UPPER BOUNDS for the batch's temporary
    lifetime (the conservative shape a producer serializes before sizes
    are known); the actual descriptors must land at or under each bound.
    Exact equality remains the special case of an exact ceiling.
    """

    planned = prewrite.get("class_bytes")
    if not isinstance(planned, Mapping):
        return False
    actual = {"payload": 0, "checkpoint": 0, "temp": 0}
    for desc in sealed:
        actual[str(desc["artifact_class"])] += int(desc["bytes"])
    for cls, value in actual.items():
        try:
            if value > int(planned.get(cls, -1)):
                return False
        except (TypeError, ValueError):
            return False
    return True


def _class_sums(batches: Mapping[str, object]) -> dict[str, int]:
    """Committed class bytes still charged against durable-origin quota.

    Stage retirement (`retired`, set after a complete SSD/RAM egress) frees
    the tier working window, never the durable HDD payload/checkpoint/temp
    bytes: those stay charged until `reclaim_origin` proves every origin
    path absent. Only batches with `origin_reclaimed` set stop counting.
    Malformed records fail closed; records predating the flag (no key)
    count as unreclaimed.
    """

    sums = {"payload": 0, "checkpoint": 0, "temp": 0}
    for batch_id, record in batches.items():
        if not isinstance(record, Mapping):
            raise oc.ProducedOutputError(
                f"unknown-retain: bad committed batch {batch_id!r}")
        if record.get("origin_reclaimed"):
            continue
        for cls in sums:
            sums[cls] += oc.output_class_bytes(
                record.get("class_bytes"),
                where=f"committed batch {batch_id!r}")[cls]
    return sums


def portable_origin_identity(info) -> dict[str, int]:
    """The existing origin-identity tuple for one stat result.

    `reader_lease.portable_identity` is the fleet's one spelling of server-side
    file identity (`ino/size/mtime_ns/ctime_ns`, deliberately without st_dev,
    which is per-client on NFS). This lane reads it; it never defines a second
    identity and never edits that module.
    """

    from prismabuild import reader_lease as lease_mod

    return lease_mod.portable_identity(info)


def origin_identity_at_commit(sealed: list[dict[str, object]]
                               ) -> tuple[dict[str, dict[str, int]] | None,
                                          dict[str, object] | None]:
    """Capture each sealed origin's identity, one lstat per path.

    Both commit paths call it under the output-prefix lock (#1064), so the
    move aside and put-back of a retirement holding that lock, which move
    the ctime, are either behind it or have not started. ONE stat answers
    both questions the commit asks -- is this file the length the
    descriptor claims, and what exactly is this file -- so the size that is
    checked and the identity that is recorded can never be two different
    moments. `os.lstat` (not `stat`) matches the check this path has always
    made and is strictly stronger for the proof: replacing a regular file
    with a symlink to identical bytes changes the recorded identity and
    refuses.
    Returns `(identity_map, None)` or `(None, refusal)`.
    """

    identity: dict[str, dict[str, int]] = {}
    for desc in sealed:
        path = str(desc["path"])
        try:
            info = os.lstat(path)
        except OSError as exc:
            return (None, {"ok": False,
                           "refusal": f"descriptor-unstatable: {exc}"})
        if int(info.st_size) != int(desc["bytes"]):
            return (None, {"ok": False, "refusal": "descriptor-size-mismatch",
                           "path": path})
        identity[path] = portable_origin_identity(info)
    return (identity, None)


def read_landed_origins(sealed: list[dict[str, object]],
                         landed: Mapping[str, Mapping[str, object]],
                         verified: dict[str, dict[str, object]]
                         ) -> dict[str, object] | None:
    """Read each landed origin whose timestamps alone moved, with no lock held.

    ``landed`` is the identity each origin's writer recorded when it landed
    (`commit_origin_batch`). An origin that is still that file, or the file
    ``verified`` already read, costs one lstat. One with the landed inode and
    size and other timestamps (an NFS delegation recall, #1111) is read and
    hashed (`_verify_origin_content`), and its re-pin is kept in ``verified``
    by path for `landed_identity` to check under the lock. Everything else
    is left to that check, which takes the identity the batch records: an
    origin that cannot be stat'ed here may be at a retirement's private name
    for a moment, and is back by the time the lock is granted (#1064).
    Returns the refusal of a read whose digest is not the descriptor's, or
    ``None``.
    """

    from prismabuild import reader_lease as lease_mod

    for desc in sealed:
        path = str(desc["path"])
        try:
            live = portable_origin_identity(os.lstat(path))
        except OSError:
            continue
        got = verified.get(path)
        if (lease_mod.file_id_matches(landed[path], live)
                or (got is not None
                    and lease_mod.file_id_matches(got["to"], live))
                or not lease_mod.timestamp_only_mismatch(landed[path], live)):
            continue
        repin, why = _verify_origin_content(
            path, landed[path], live, desc["sha256"], where="commit")
        if repin is None:
            return {"ok": False, "refusal": "origin-is-not-the-landed-copy",
                    "detail": f"{path}: {why}"}
        verified[path] = repin
    return None


def landed_identity(sealed: list[dict[str, object]],
                     landed: Mapping[str, Mapping[str, object]] | None,
                     verified: Mapping[str, Mapping[str, object]],
                     origin_identity: dict[str, dict[str, int]]
                     ) -> tuple[list[dict[str, object]], list[str],
                                dict[str, object] | None]:
    """Is each origin, stat'ed under the lock, the copy that landed?

    ``origin_identity`` is the identity `origin_identity_at_commit` took
    under the output-prefix lock. Each origin must be the landed file, or the
    file a read outside the lock hashed (``verified``, from
    `read_landed_origins`); that re-pin is returned for the entry, and the
    identity committed is the hashed one. An origin whose inode or size is
    not the landed one refuses. One whose timestamps alone moved and that no
    read has hashed at that identity is returned in ``moved``: the caller lets
    the lock go and reads it. Returns ``(landed_repins, moved, refusal)``.
    """

    from prismabuild import reader_lease as lease_mod

    repins: list[dict[str, object]] = []
    moved: list[str] = []
    if landed is None:
        return (repins, moved, None)
    for desc in sealed:
        path = str(desc["path"])
        live = origin_identity[path]
        if lease_mod.file_id_matches(landed[path], live):
            continue
        got = verified.get(path)
        if got is not None and lease_mod.file_id_matches(got["to"], live):
            origin_identity[path] = dict(got["to"])
            repins.append(dict(got))
            continue
        if not lease_mod.timestamp_only_mismatch(landed[path], live):
            return (repins, moved,
                    {"ok": False, "refusal": "origin-is-not-the-landed-copy"})
        moved.append(path)
    return (repins, moved, None)


_ORIGIN_REPIN_FIELDS = frozenset({"path", "from", "to", "reason", "sha256",
                                  "bytes", "rehash_s", "reads", "host", "unix",
                                  "where"})


def _identity_key(identity: Mapping[str, object]) -> tuple[object, ...]:
    return tuple(identity.get(field)
                 for field in ("ino", "size", "mtime_ns", "ctime_ns"))


def _verify_origin_content(path: str, published: Mapping[str, object],
                           observed: Mapping[str, object], sha256: object, *,
                           where: str
                           ) -> tuple[dict[str, object] | None, str | None]:
    """Settle one origin whose timestamps alone moved, by its content (#1111).

    ``published`` is the identity a check compares against, and ``observed``
    the one it found: the same inode and size, other timestamps
    (`reader_lease.timestamp_only_mismatch`).  That is what an NFS
    delegation recall does to a file its writer recorded: the server applies
    the writer's delegated timestamps, and the bytes stay the same.  The file
    is read through its parent without following a link and hashed
    (`reader_lease.content_identity`), with no lock held; the caller files
    the result under its own lock after a re-stat.

    Returns ``(repin, None)`` when the digest is the committed ``sha256`` and
    the inode and size are the published ones: ``repin`` names the path, the
    identity it replaces, the hashed one, the reason, the seconds the read
    took and where it was made.  Otherwise ``(None, why)``.  A batch with no
    digest (the DEV null convention) is never settled this way.
    """

    from prismabuild import reader_lease as lease_mod

    if not isinstance(sha256, str) or not sha256:
        return (None, "only the timestamps moved, and the batch carries no "
                      "sha256 to verify its content against")
    memo = (path, _identity_key(observed), sha256)
    if memo in _CONTENT_REFUSED:
        return (None, _CONTENT_REFUSED[memo])
    directory, name = os.path.split(path)
    try:
        parent = os.open(directory or ".",
                         os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    except OSError as exc:
        return (None, f"only the timestamps moved, and the content could not "
                      f"be verified: {exc}")
    try:
        read = lease_mod.content_identity(name, dir_fd=parent)
    except (OSError, lease_mod.ReaderLeaseError) as exc:
        return (None, f"only the timestamps moved, and the content could not "
                      f"be verified: {exc}")
    finally:
        os.close(parent)
    now = read["identity"]
    if (now["ino"], now["size"]) != (published.get("ino"), published.get("size")):
        return (None, "the file changed while its content was verified")
    if read["sha256"] != sha256:
        why = (f"only the timestamps moved, but the content sha256 "
               f"{read['sha256']} is not the committed {sha256}")
        _CONTENT_REFUSED[memo] = why
        _CONTENT_REFUSED[(path, _identity_key(now), sha256)] = why
        return (None, why)
    return ({"path": path, "from": dict(published), "to": dict(now),
             "reason": oc.ORIGIN_REPIN_REASON, "sha256": sha256,
             "bytes": read["bytes"], "rehash_s": read["seconds"],
             "reads": read["reads"], "host": socket.gethostname(),
             "unix": round(time.time(), 3), "where": where}, None)


def effective_origin_identity(filed: Mapping[str, object],
                               sealed: list[dict[str, object]],
                               entry: Mapping[str, object] | None
                               ) -> dict[str, dict[str, object]] | None:
    """The identity each origin is checked against: the commit's, re-pinned (#1111).

    The immutable batch record carries the identity its commit recorded.
    The mutable commitments entry may carry ``origin_repins``, each filed
    after a content check (`_verify_origin_content`); applied in order, each
    must start from the identity the ones before it left, change only the
    timestamps, and carry the batch's sha256 for its path.  A list that does
    not chain is corrupt and raises ``unknown-retain``, never reads as the
    committed identity.  Returns ``None`` when the record has no identity.
    """

    from prismabuild import reader_lease as lease_mod

    recorded = filed.get("origin_identity")
    if not isinstance(recorded, Mapping):
        return None
    current = {str(path): dict(identity) if isinstance(identity, Mapping)
               else identity for path, identity in recorded.items()}
    repins = (entry or {}).get("origin_repins")
    if repins is None:
        return current
    corrupt = oc.ProducedOutputError(
        "unknown-retain: origin re-pins do not chain from the committed identity")
    if not isinstance(repins, list) or not repins:
        raise corrupt
    digests = {str(desc["path"]): desc.get("sha256") for desc in sealed}
    for repin in repins:
        if (not isinstance(repin, Mapping) or set(repin) != _ORIGIN_REPIN_FIELDS
                or repin["reason"] != oc.ORIGIN_REPIN_REASON):
            raise corrupt
        path = repin["path"]
        if (not isinstance(path, str) or path not in current
                or not isinstance(digests.get(path), str)
                or repin["sha256"] != digests[path]
                or repin["from"] != current[path]):
            raise corrupt
        try:
            after = lease_mod._check_identity(repin["to"], where="re-pinned to")
        except lease_mod.ReaderLeaseError:
            raise corrupt from None
        if not lease_mod.timestamp_only_mismatch(repin["from"], after):
            raise corrupt
        current[path] = dict(after)
    return current


def check_origin_identity(filed: Mapping[str, object],
                           sealed: list[dict[str, object]],
                           entry: Mapping[str, object] | None = None, *,
                           verify: bool = False, where: str = ""
                           ) -> dict[str, object]:
    """Do the sealed origins still carry the identity their commit recorded?

    This is the whole safety of restaging a DEV null-digest batch. The
    descriptor carries no payload digest, so the only thing that can say the
    bytes about to be copied a second time are the bytes the batch was
    committed over is the file identity captured when it was committed --
    `(ino, size, mtime_ns, ctime_ns)` through the same
    `reader_lease.file_id_matches` every strict reader and every publication
    proof uses. Size alone is NOT that proof: a rewritten file of identical
    length passes an lstat size check and is a different artifact.

    The identity compared is the commit's with the entry's re-pins applied
    (`effective_origin_identity`). A mismatch in the timestamps alone, on an
    origin whose descriptor carries its sha256, is what an NFS delegation
    recall leaves (#1111). With ``verify`` the origin is read and hashed
    here, with no lock held (`_verify_origin_content`), and the answer
    carries the re-pins for the caller to file under its lock; without it
    the answer is ``restage-origin-unverified`` and names the paths. Nothing
    else is ever read: a match costs one lstat, and a batch with no digest
    stays strict.

    A batch committed before the proof existed has no `origin_identity` and
    refuses `restage-origin-proof-missing` -- the current bytes are never
    retroactively blessed as the committed ones. An unstatable path is unknown
    and refuses. Returns ``{"ok": True, "repins": [...]}`` or
    ``{"ok": False, "refusal", "detail"?, "paths"?}``.
    """

    from prismabuild import reader_lease as lease_mod

    try:
        recorded = effective_origin_identity(filed, sealed, entry)
    except oc.ProducedOutputError as exc:
        return {"ok": False, "refusal": str(exc)}
    if not recorded or len(recorded) != len(sealed):
        return {"ok": False, "refusal": "restage-origin-proof-missing"}
    repins: list[dict[str, object]] = []
    unverified: list[str] = []
    for desc in sealed:
        path = str(desc["path"])
        published = recorded.get(path)
        if not isinstance(published, Mapping):
            return {"ok": False, "refusal": "restage-origin-proof-missing"}
        # A proof that disagrees with the manifest it is filed beside is not a
        # proof of anything.
        if published.get("size") != int(desc["bytes"]):
            return {"ok": False, "refusal": "restage-origin-proof-missing"}
        try:
            live = portable_origin_identity(os.lstat(path))
        except OSError as exc:
            return {"ok": False, "refusal": f"restage-origin-unstatable: {exc}"}
        if lease_mod.file_id_matches(published, live):
            continue
        if (desc.get("sha256") is None
                or not lease_mod.timestamp_only_mismatch(published, live)):
            return {"ok": False, "refusal": "restage-origin-changed"}
        if not verify:
            unverified.append(path)
            continue
        repin, why = _verify_origin_content(path, published, live,
                                            desc["sha256"], where=where)
        if repin is None:
            return {"ok": False, "refusal": "restage-origin-changed",
                    "detail": f"{path}: {why}"}
        repins.append(repin)
    if unverified:
        return {"ok": False, "refusal": "restage-origin-unverified",
                "paths": unverified}
    return {"ok": True, "repins": repins}


def recheck_origin_identity(filed: Mapping[str, object],
                             sealed: list[dict[str, object]],
                             entry: Mapping[str, object] | None = None
                             ) -> tuple[bool, str | None]:
    """`check_origin_identity` without a read: ``(True, None)`` or ``(False, refusal)``."""

    verdict = check_origin_identity(filed, sealed, entry)
    return (bool(verdict["ok"]), verdict.get("refusal"))


def apply_origin_repins(entry: Mapping[str, object],
                         filed: Mapping[str, object],
                         sealed: list[dict[str, object]],
                         repins: Sequence[Mapping[str, object]]
                         ) -> tuple[dict[str, object] | None, dict[str, str] | None]:
    """``entry`` with verified re-pins appended, checked under the caller's lock.

    Each origin is stat'ed again: it must still be the identity its content
    check hashed, or it changed after the read and refuses. An origin whose
    entry already carries that identity (another caller filed the same
    re-pin) adds nothing. Returns ``(entry, None)`` -- the same mapping when
    nothing is added -- or ``(None, refusal)``.
    """

    from prismabuild import reader_lease as lease_mod

    try:
        current = effective_origin_identity(filed, sealed, entry)
    except oc.ProducedOutputError as exc:
        return (None, {"refusal": str(exc)})
    if current is None:
        return (None, {"refusal": "restage-origin-proof-missing"})
    added: list[dict[str, object]] = []
    for repin in repins:
        path = str(repin["path"])
        try:
            live = portable_origin_identity(os.lstat(path))
        except OSError as exc:
            return (None, {"refusal": f"restage-origin-unstatable: {exc}"})
        if not lease_mod.file_id_matches(repin["to"], live):
            return (None, {"refusal": "restage-origin-changed",
                           "detail": f"{path}: the file changed after its "
                                     f"content was verified"})
        if lease_mod.file_id_matches(current.get(path), live):
            continue
        if current.get(path) != repin["from"]:
            return (None, {"refusal": "restage-origin-changed",
                           "detail": f"{path}: another identity was filed "
                                     f"after its content was verified"})
        added.append(dict(repin))
        current[path] = dict(repin["to"])
    if not added:
        return (dict(entry), None)
    return ({**entry, "origin_repins": [*(entry.get("origin_repins") or []),
                                        *added]}, None)


class _PrewriteContainment:
    def __init__(self, prefix: str) -> None:
        self.prefix = prefix
        self.real_prefix = os.path.realpath(prefix)
        self.suffix_modes: dict[str, int | None] = {}
        try:
            self.prefix_is_directory = stat.S_ISDIR(os.stat(self.real_prefix).st_mode)
        except OSError:
            self.prefix_is_directory = False

    def _lexically_contained(self, path: str) -> bool:
        if (not self.prefix_is_directory
                or self.prefix.startswith("//") or path.startswith("//")
                or self.prefix != os.path.normpath(self.prefix)
                or path == self.prefix
                or os.path.commonpath([self.prefix, path]) != self.prefix):
            return False
        suffix = os.path.relpath(path, self.prefix).split(os.sep)
        mapped = os.path.join(self.real_prefix, *suffix)
        if os.path.commonpath([self.real_prefix, mapped]) != self.real_prefix:
            return False
        current = self.real_prefix
        for index, component in enumerate(suffix):
            current = os.path.join(current, component)
            if current not in self.suffix_modes:
                try:
                    mode = os.lstat(current).st_mode
                except OSError as exc:
                    if exc.errno != errno.ENOENT:
                        return False
                    mode = None
                self.suffix_modes[current] = mode
            mode = self.suffix_modes[current]
            if mode is None:
                return True
            if stat.S_ISLNK(mode):
                return False
            if index < len(suffix) - 1 and not stat.S_ISDIR(mode):
                return False
        return True

    def check(self, path: str) -> None:
        if self._lexically_contained(path):
            return
        real_path = os.path.realpath(path)
        try:
            common = os.path.commonpath([self.real_prefix, real_path])
        except ValueError as exc:
            raise oc.ProducedOutputError(
                f"prewrite paths[] is not under the scope prefix: {exc}")
        if common != self.real_prefix:
            raise oc.ProducedOutputError("prewrite paths[] escapes the scope prefix")


def _prewrite_paths(prefix: str, paths: list[str]) -> list[str]:
    containment: _PrewriteContainment | None = None
    planned_paths: list[str] = []
    for path in paths:
        candidate = oc.absolute_output_path(path, where="prewrite paths[]")
        if containment is None:
            containment = _PrewriteContainment(prefix)
        containment.check(candidate)
        planned_paths.append(candidate)
    return planned_paths


def _origin_record(checked_template: Mapping[str, object],
                   checked_instance: Mapping[str, object], *, batch_id: str,
                   batch_ns: str, manifest_digest: str, tier: str,
                   class_bytes: Mapping[str, int],
                   sealed: list[dict[str, object]],
                   origin_identity: Mapping[str, object],
                   lifetime: str = oc.ORIGIN_LIFETIME_RETAIN) -> dict[str, object]:
    """The immutable record of one origin-only batch, minus its commit time.

    A ``consumed`` lifetime is written into the record; ``retain`` is not, so
    a retained batch's record is the one #912 filed.
    """

    record = {
        "schema": oc.BATCH_SCHEMA_V1,
        "batch_id": batch_id,
        "batch_namespace": batch_ns,
        "manifest_schema": oc.BATCH_MANIFEST_SCHEMA_V1,
        "manifest_digest": manifest_digest,
        # The tier whose pool-side fill paced the export; nothing is staged.
        "tier": tier,
        "mover_key": None,
        "origin_only": True,
        "class_bytes": dict(class_bytes),
        "total_bytes": sum(int(v) for v in class_bytes.values()),
        "entry_count": len(sealed),
        "entries": sealed,
        "template_id": str(checked_template["template_id"]),
        "template_sha256": oc.template_sha256(checked_template),
        "owner_action_key": str(checked_instance["owner_action_key"]),
        "owner_attempt": dict(checked_instance["owner_attempt"]),
        "origin_identity": dict(origin_identity),
        "object_set_id": oc.manifest_object_set_id({
            f"{d['bytes']}:{d['path']}": {"bytes": int(d["bytes"]),
                                         "sha256": d["sha256"]}
            for d in sealed}),
    }
    if lifetime != oc.ORIGIN_LIFETIME_RETAIN:
        record["lifetime"] = lifetime
    return record


def _public_batch_record(queue_root: str | Path,
                         checked_instance: Mapping[str, object],
                         checked_template: Mapping[str, object],
                         entry: object, batch_id: str) -> dict[str, object]:
    if not isinstance(entry, Mapping):
        raise oc.ProducedOutputError(
            f"unknown-retain: commitments entry for {batch_id!r} is not a record")
    filed, sealed = BatchLedger.open(
        queue_root, checked_instance, checked_template, entry, batch_id)
    return {
        "batch_id": batch_id,
        "state": BatchFacets(entry).state,
        "lifetime": BatchFacets(entry).lifetime,
        "entries": [{"path": str(desc["path"]), "bytes": int(desc["bytes"]),
                     "sha256": desc["sha256"],
                     "artifact_class": str(desc["artifact_class"])}
                    for desc in sealed],
        "total_bytes": int(filed["total_bytes"]),
        "manifest_digest": str(filed["manifest_digest"]),
        "commitment": json.loads(json.dumps(entry)),
        "record": filed,
    }


def verify_origins_unlocked(queue_root, instance: Mapping[str, object],
                             template: Mapping[str, object], batch_id: str, *,
                             where: str) -> dict[str, object]:
    """`check_origin_identity` with reads, on a lockless read of the batch (#1111)."""

    try:
        checked_template, checked_instance = oc.bound_output_contract(
            template, instance)
        entry = BatchLedger.for_instance(queue_root, checked_instance).capture()["batches"].get(batch_id)
        if not isinstance(entry, Mapping):
            return {"ok": False, "refusal": "unknown-batch"}
        filed, sealed = BatchLedger.open(
            queue_root, checked_instance, checked_template, entry, batch_id)
    except oc.ProducedOutputError as exc:
        return {"ok": False, "refusal": str(exc)}
    return check_origin_identity(filed, sealed, entry, verify=True,
                                  where=where)


def origin_scope(queue_root: str | Path, checked_ref: Mapping[str, str]
                      ) -> tuple[dict[str, object], dict[str, object]]:
    """The filed instance and template a checked ref names, bound together.

    Read from PB's own records only: the instance at
    ``<owner>/<template>.<nonce>`` and the template it is bound to. Raises
    `ProducedOutputError` naming what is missing or does not match.
    """

    scope_dir = (Path(queue_root) / "residency" / oc.OUTPUT_SCOPES_SUBDIR
                 / checked_ref["owner_action_key"]
                 / f"{checked_ref['template_id']}.{checked_ref['owner_nonce']}")
    try:
        instance = oc.validate_instance(json.loads(
            (scope_dir / "instance.json").read_text()))
        template = oc.validate_template(json.loads(
            (Path(queue_root) / "residency" / oc.OUTPUT_TEMPLATES_SUBDIR
             / f"{checked_ref['template_id']}.json").read_text()))
    except FileNotFoundError:
        raise oc.ProducedOutputError(
            "origin-batch-unknown: no instance or template is filed for "
            f"{checked_ref['owner_action_key'][:12]}/"
            f"{checked_ref['template_id']}") from None
    except (OSError, ValueError) as exc:
        raise oc.ProducedOutputError(
            f"unknown-retain: origin batch scope unreadable: {exc}") from None
    attempt = instance["owner_attempt"]
    assert isinstance(attempt, dict)
    if (instance["owner_action_key"] != checked_ref["owner_action_key"]
            or attempt["nonce"] != checked_ref["owner_nonce"]
            or instance["template_id"] != checked_ref["template_id"]
            or instance["template_sha256"] != oc.template_sha256(template)):
        raise oc.ProducedOutputError(
            "unknown-retain: origin batch scope is bound to another template "
            "or attempt")
    return instance, template


def resolve_origin_batch(queue_root: str | Path, ref: Mapping[str, object]
                       ) -> tuple[dict[str, object], list[dict[str, object]]]:
    """One batch resolved with no lock held, and the re-pins it needs filed."""

    checked_ref = oc.checked_origin_ref(ref)
    instance, template = origin_scope(queue_root, checked_ref)
    if not template.get("write_only"):
        raise oc.ProducedOutputError(
            "origin-batch-not-write-only: its template stages its batches")
    commitments = BatchLedger.for_instance(queue_root, instance).capture()
    entry = commitments["batches"].get(checked_ref["batch_id"])
    if not isinstance(entry, Mapping):
        raise oc.ProducedOutputError(
            f"origin-batch-uncommitted: {checked_ref['batch_id']}")
    if (entry.get("origin_only") is not True
            or entry.get("manifest_digest") != checked_ref["manifest_digest"]):
        raise oc.ProducedOutputError(
            "origin-batch-mismatch: the committed batch is not origin-only "
            "over this manifest digest")
    if entry.get("origin_reclaimed"):
        raise oc.ProducedOutputError(
            f"origin-batch-reclaimed: {checked_ref['batch_id']}")
    if entry.get("retiring"):
        # `origin_retirement_tick` decided to delete it (#914); its files may
        # already be going, and no new consumer may count on them.
        raise oc.ProducedOutputError(
            f"origin-batch-retiring: {checked_ref['batch_id']}")
    filed, sealed = BatchLedger.open(
        queue_root, instance, template, entry, checked_ref["batch_id"])
    if filed.get("origin_only") is not True:
        raise oc.ProducedOutputError(
            "unknown-retain: origin batch record is not origin-only")
    verdict = check_origin_identity(filed, sealed, entry, verify=True,
                                     where="declare")
    if not verdict["ok"]:
        detail = f": {verdict['detail']}" if verdict.get("detail") else ""
        raise oc.ProducedOutputError(
            f"origin-batch-changed: {verdict['refusal']} "
            f"({checked_ref['batch_id']}){detail}")
    return ({"ref": checked_ref, "instance": instance, "template": template,
             "record": filed, "entries": sealed}, list(verdict["repins"]))


def _consumers_dir(queue_root: str | Path, instance: Mapping[str, object],
                   batch_id: str) -> Path:
    return scope_dir(queue_root, instance) / "consumers" / batch_id


def _released_consumers_dir(queue_root: str | Path,
                            instance: Mapping[str, object],
                            batch_id: str) -> Path:
    return scope_dir(queue_root, instance) / "released-consumers" / batch_id


def read_consumer_release(queue_root: str | Path, instance: Mapping[str, object],
                      batch_id: str, key: str,
                      checked_ref: Mapping[str, str]) -> dict[str, object] | None:
    """The operator's release of one declaration, or ``None``; raise if bad."""

    path = _released_consumers_dir(queue_root, instance, batch_id) / f"{key}.json"
    try:
        body = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise oc.ProducedOutputError(
            f"unknown-retain: consumer release {key[:12]} unreadable: {exc}"
        ) from None
    if (not isinstance(body, Mapping)
            or body.get("schema") != oc.ORIGIN_CONSUMER_RELEASE_SCHEMA_V1
            or body.get("consumer_action_key") != key
            or body.get("ref") != dict(checked_ref)):
        raise oc.ProducedOutputError(
            f"unknown-retain: consumer release {key[:12]} does not name this batch")
    return dict(body)


def declared_consumers(queue_root: str | Path, instance: Mapping[str, object],
                        batch_id: str,
                        checked_ref: Mapping[str, str]) -> list[str]:
    """The consumer action keys filed for one batch, sorted.

    A missing directory is no declaration. An unreadable directory or
    declaration, or one that names another batch or another key than its
    file name, raises: an unknown declaration may be the one live reader.
    """

    directory = _consumers_dir(queue_root, instance, batch_id)
    try:
        names = sorted(os.listdir(directory))
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise oc.ProducedOutputError(
            f"unknown-retain: consumer declarations unreadable: {exc}") from None
    keys: list[str] = []
    for name in names:
        if name.startswith("."):
            # A publication's temporary file; the declaration it becomes is
            # read on the next cycle.
            continue
        if not name.endswith(".json"):
            raise oc.ProducedOutputError(
                f"unknown-retain: unexpected consumer declaration {name!r}")
        try:
            body = json.loads((directory / name).read_text())
        except (OSError, ValueError) as exc:
            raise oc.ProducedOutputError(
                f"unknown-retain: consumer declaration {name} unreadable: "
                f"{exc}") from None
        if (not isinstance(body, Mapping)
                or body.get("schema") != oc.ORIGIN_CONSUMER_SCHEMA_V1
                or body.get("consumer_action_key") != name[:-len(".json")]
                or body.get("ref") != dict(checked_ref)):
            raise oc.ProducedOutputError(
                f"unknown-retain: consumer declaration {name} does not name "
                "this batch")
        keys.append(oc.output_key(body["consumer_action_key"],
                           where="consumer declaration key"))
    return keys


def record_nonce(record: Mapping[str, object] | None) -> str:
    control = (record or {}).get("resource_scope")
    if isinstance(control, Mapping):
        nonce = control.get("nonce")
        if isinstance(nonce, str) and nonce:
            return nonce
    return ""


def consumer_state(queue, key: str) -> str:
    """How a declared consumer stands, for retirement.

    ``succeeded`` (its latest generation is ``done`` as executed or a cache
    hit), ``live`` (queued, claimed or moving), ``failed``, ``withdrawn``,
    ``unpublished`` (declared but no row filed) or ``unknown``.
    """

    from prismabuild import record_store

    standing = RecordStore(queue.root).standing(key)
    if isinstance(standing, Absent):
        return "unpublished"
    if isinstance(standing, Unknown):
        return "unknown"
    if isinstance(standing.value, Moving):
        return "live"
    state, record = standing.value.state, standing.value.record
    if state in (record_store.CLAIMED, record_store.READY):
        return "live"
    if state == record_store.DONE:
        if record.get("status") in ("executed", "cache_hit"):
            return "succeeded"
        return "unknown"
    if state == record_store.FAILED:
        return "failed"
    if state == record_store.WITHDRAWN:
        return "withdrawn"
    return "unknown"


def producer_attempt_state(queue, instance: Mapping[str, object]) -> str:
    """Whether the attempt that filed this instance is dead, for the sweep.

    ``dead`` when nothing of the owner's is running and its latest
    generation ended without this attempt succeeding: the owner's claim names
    another attempt (a retry superseded this one), or its latest terminal is
    ``failed`` or ``withdrawn``, or another attempt executed it.
    ``succeeded`` when it is ``done`` by this attempt; ``live`` while this
    attempt holds the claim. Everything else -- queued, moving, absent, a
    cache hit by another attempt, a record without the attempt's nonce,
    anything unreadable -- is ``unknown``, and the sweep keeps the batch.
    """

    attempt = instance["owner_attempt"]
    assert isinstance(attempt, dict)
    return attempt_state(
        RecordStore(queue.root).standing(str(instance["owner_action_key"])),
        str(attempt["nonce"]))


def attempt_state(generation: KeyStanding,
                   nonce: str) -> str:
    """`producer_attempt_state` for one attempt, over a generation already read.

    Every attempt of one owner key answers from the same read of the key
    (`RecordStore.standing`), so a caller that asks about several attempts of it
    reads the key once.
    """

    from prismabuild import record_store

    if not isinstance(generation, Present) or isinstance(generation.value, Moving):
        return "unknown"
    state, record = generation.value.state, generation.value.record
    if state == record_store.CLAIMED:
        live = record_nonce(record)
        if not live:
            return "unknown"
        return "live" if live == nonce else "dead"
    if state in (record_store.FAILED, record_store.WITHDRAWN):
        return "dead"
    if state == record_store.DONE:
        finished = record_nonce(record)
        if record.get("status") not in ("executed", "cache_hit") or not finished:
            return "unknown"
        if finished == nonce:
            return "succeeded"
        # A cache hit by another attempt ran nothing: it found the receipt
        # this attempt may have published before it died, and then these
        # batches are the producer's only output (#913).
        return "dead" if record.get("status") == "executed" else "unknown"
    return "unknown"


def _unretired_staged_batches(batches: Mapping[str, object]) -> list[str]:
    """Batch ids with a stage copy not yet retired, in order.

    Every committed batch that is not origin-only and whose ACTIVE
    materialization is unretired (`_batch_stage_retired`). A malformed
    materialization list is listed too: the retirement step reports it.
    """

    found: list[str] = []
    for batch_id, entry in sorted(batches.items()):
        if not isinstance(entry, Mapping) or entry.get("origin_only") is True:
            continue
        try:
            if BatchFacets(entry).stage_retired:
                continue
        except oc.ProducedOutputError:
            pass
        found.append(batch_id)
    return found


def scope_owners(scopes_root: Path) -> list[str]:
    """Owner directories under the produced-output scopes root, sorted."""

    return sorted(p.name for p in scopes_root.iterdir() if p.is_dir())


def owner_instance_paths(owner_dir: Path) -> list[Path]:
    """Every filed instance record of one owner, in name order.

    Bound instances live at ``<owner>/<template>.<nonce>/instance.json``; a
    flat ``<owner>/<name>.json`` other than ``commitments.json`` is the older
    spelling and still counts. Raises ``OSError`` when the owner directory
    cannot be listed.
    """

    candidates: list[Path] = []
    for child in sorted(owner_dir.iterdir()):
        if child.is_dir():
            candidate = child / "instance.json"
            if candidate.is_file():
                candidates.append(candidate)
        elif child.suffix == ".json" and child.name != "commitments.json":
            candidates.append(child)
    return candidates


def funding_verdict(queue, mover_key: str,
                            tier: str) -> tuple[str, str | None]:
    """What the FILED funding record says about one mover's fence.

    ``("spent", None)`` a parsed `consumed` record -- the pool's own
    statement that the fence is gone, and `output_funded_cover` covers only
    a record in `transferring`, so nothing can re-cover it;
    ``("fundable", None)`` `reserved`/`transferring`, still coverable;
    ``("none", None)`` a PROVEN absent file -- no prepaid intent was ever
    filed, so the ordinary claim path applies; ``("unknown", reason)``
    anything unreadable, unparsable, or disagreeing with itself.

    Read through `PoolQueue.output_funding_file_state`, never
    `read_output_funding`, because this is a census path and that method
    cannot tell absent from corrupt -- its own docstring forbids it here.
    One question, asked identically wherever a batch's claimability is
    judged, so the READY and FAILED branches cannot drift apart.
    """

    if not mover_key or not tier:
        return ("unknown", "no mover or tier on the batch entry")
    try:
        record, file_state = queue.output_funding_file_state(mover_key, tier)
    except Exception as exc:
        return ("unknown", repr(exc))
    if file_state == "corrupt":
        return ("unknown", "funding record unreadable")
    if file_state == "absent":
        return ("none", None)
    state = str(record.get("state")) if isinstance(record, Mapping) else ""
    if state in ("reserved", "transferring"):
        return ("fundable", None)
    if state == "consumed":
        return ("spent", None)
    # `released` beside a batch the commitments still call live is evidence
    # disagreeing with itself, and so is a record with no readable state.
    return ("unknown", f"funding state {state!r} on a live batch")


def mover_receipt(queue, mover_key: str) -> Mapping[str, object] | None:
    """One mover's filed receipt, or None when there is none to read."""

    if not mover_key:
        return None
    try:
        receipt = queue.move_record(mover_key)
    except Exception:
        return None
    if not isinstance(receipt, Mapping):
        return None
    return receipt


def receipt_complete(receipt: Mapping[str, object] | None) -> bool | None:
    """Did one already-read receipt say the batch landed whole? (3-valued)

    True/False from a filed, unrefused receipt; None when there is none to
    read or it cannot be read. Fragments cannot answer this: `stage_move`
    publishes one per entry as the bytes land and files its receipt once at
    the end, so a half-staged batch composes exactly like a whole one and a
    killed mover leaves fragments with no receipt at all. Every census in
    this lane asks the same question the same way, and a reader that needs
    the refusal too derives both from ONE observation of the record.
    """

    if receipt is None:
        return None
    if receipt.get("refusal"):
        # A filed refusal is evidence, and it is not "complete".
        return False
    return receipt.get("complete") is True


def receipt_refusal(receipt: Mapping[str, object] | None) -> str | None:
    """The refusal one already-read receipt filed, or None when it filed none.

    The mover's own typed verdict beside completeness: `residency_moved_nothing`
    and `residency_overran_reservation` are the mover's existing refusals, and
    `origin_unreachable` is the one that says the origin directories are not on
    the tier host (#804). An absent or unreadable receipt answers None, never a
    refusal: absence is silence, and a reader must not turn it into a named
    failure.
    """

    if receipt is None:
        return None
    refusal = receipt.get("refusal")
    return str(refusal) if refusal else None


def mover_receipt_complete(queue, mover_key: str) -> bool | None:
    """Read one mover's receipt and answer the completeness question."""

    return receipt_complete(mover_receipt(queue, mover_key))


def _delete_funding(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except OSError as exc:
        raise oc.ProducedOutputError(
            f"funding intent unreleasable: {exc}") from None


def _write_funding(path: Path, record: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(
        dir=str(path.parent), prefix=".funding.")
    try:
        with os.fdopen(handle, "w") as stream:
            json.dump(dict(record), stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def _read_funding(path: Path) -> dict[str, object] | None:
    """One batch's durable funding intent, None when no attempt was made.

    Raises ProducedOutputError on corrupt/unreadable (unknown state, never
    an unfunded batch).
    """

    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise oc.ProducedOutputError(
            f"funding intent unreadable: {exc}") from None
    if not isinstance(raw, Mapping):
        raise oc.ProducedOutputError("funding intent is corrupt")
    return dict(raw)


def output_fragment_root(residency_root: str | Path) -> Path:
    """The output material namespace root, beside input fragments."""

    return Path(residency_root) / oc.OUTPUT_FRAGMENTS_SUBDIR


_CONTENT_REFUSED: dict[tuple[str, tuple[object, ...], str], str] = {}


class BatchFacets:
    """Independent batch facts derived from one captured commitment."""

    def __init__(self, entry: Mapping[str, object]):
        self._entry = entry

    @property
    def kind(self) -> str:
        return "origin" if self._entry.get("origin_only") is True else "staged"

    @property
    def origin_reclaimed(self) -> bool:
        return bool(self._entry.get("origin_reclaimed"))

    @property
    def copies(self) -> list[dict[str, object]]:
        """The entry's restage materializations, checked whole, oldest first.

        A malformed `materializations` list is unknown state and RAISES, never
        reads as empty: retirement, the censuses and the successor gate all turn
        on it, and an unreadable list that answered "none" would orphan a live
        stage copy. An absent or empty list is a batch that was never restaged.

        THE ONE validator. Per-row shape is not enough, because the dangerous
        histories are the internally inconsistent ones rather than the malformed
        ones: `[gen1 live, gen2 retired]` passes every row check, yet
        `_active_materialization` would read the last row, answer "retired", and
        authorize a reclaim over a live earlier mover. So the legal sequential
        history is enforced HERE, once, for every caller -- `_active_*`,
        `_live_*`, `_batch_stage_retired`, the censuses and the retain paths all
        read through this function and inherit it:

        * generations are 1..N in order (the per-row check above);
        * every generation before the last is retired -- only the LATEST may be
          live, so no earlier stage copy can be forgotten;
        * the batch's own first copy is retired whenever any successor exists;
        * no mover key repeats, including the batch's own first mover key, so a
          spent terminal key can never be read as a live successor.

        None of these are transitions the lane can reach: each one is corruption,
        and corruption fails RETAIN.
        """

        entry = self._entry
        raw = entry.get("materializations")
        if raw is None:
            return []
        if not isinstance(raw, list):
            raise oc.ProducedOutputError("unknown-retain: materializations")
        out: list[dict[str, object]] = []
        for index, item in enumerate(raw):
            if (not isinstance(item, Mapping)
                    or len(str(item.get("mover_key") or "")) != 64
                    or any(c not in oc.OUTPUT_KEY_HEX for c in str(item.get("mover_key") or ""))
                    or not isinstance(item.get("tier"), str)
                    or not item.get("tier")
                    or type(item.get("generation")) is not int
                    or int(item["generation"]) != index + 1
                    or type(item.get("retired")) is not bool
                    or str(item.get("state") or "") not in ("intent", "funded")):
                raise oc.ProducedOutputError("unknown-retain: materializations")
            out.append(dict(item))
        if out:
            if not bool(entry.get("retired")):
                raise oc.ProducedOutputError("unknown-retain: materializations")
            seen = {str(entry.get("mover_key") or "")}
            for index, item in enumerate(out):
                key = str(item["mover_key"])
                if key in seen:
                    raise oc.ProducedOutputError("unknown-retain: materializations")
                seen.add(key)
                if index < len(out) - 1 and not item["retired"]:
                    raise oc.ProducedOutputError("unknown-retain: materializations")
        return out


    @property
    def pending_copy(self) -> dict[str, object] | None:
        """The single live (unretired) restage materialization, or None.

        The lane is a sequential writer (one producer action per instance), and
        the bounded-window path stages one window at a time, so at most ONE
        materialization may be live or pending per logical batch. More than one is
        corrupt state and raises rather than guessing which one owns the material.
        """

        entry = self._entry
        live = [item for item in BatchFacets(entry).copies
                if not item.get("retired")]
        if len(live) > 1:
            raise oc.ProducedOutputError("unknown-retain: materializations")
        return live[0] if live else None


    @property
    def active_copy(self) -> dict[str, object]:
        """Which mover owns this batch's stage copy RIGHT NOW.

        The one question every retire, census and row-preparation path must ask
        instead of reading `entry["retired"]`, which only ever describes the FIRST
        materialization. After a restage the first one is retired and a successor
        owns the material; a path that kept reading the entry flag would call a
        live stage copy retired and orphan it.

        Returns the latest materialization: the last filed restage row when there
        is one, else the batch's own first publication as generation 0. Raises on
        a malformed list (unknown retains).
        """

        entry = self._entry
        if entry.get("origin_only") is True:
            # An origin-only batch (#912) never had a stage copy, so every reader
            # that asks "is a copy live?" hears no.  Path ownership is the one
            # question that must not follow this answer; `own_write_owner` asks
            # it of the origin instead.
            return {"mover_key": "", "tier": str(entry.get("tier") or ""),
                    "generation": 0, "retired": True, "state": "origin-only",
                    "staged_paths": [], "source": "origin"}
        mats = BatchFacets(entry).copies
        if mats:
            latest = dict(mats[-1])
            latest["source"] = "materialization"
            return latest
        return {"mover_key": str(entry.get("mover_key") or ""),
                "tier": str(entry.get("tier") or ""),
                "generation": 0,
                "retired": bool(entry.get("retired")),
                "state": "funded",
                "staged_paths": list(entry.get("staged_paths") or []),
                "source": "batch"}


    @property
    def movers(self) -> list[str]:
        """Every mover key that has ever owned this batch's material, in order."""

        entry = self._entry
        keys = [str(entry.get("mover_key") or "")]
        keys += [str(item.get("mover_key")) for item in BatchFacets(entry).copies]
        return [key for key in keys if len(key) == 64]


    @property
    def stage_retired(self) -> bool:
        """Is NO stage copy of this batch live or pending right now?

        The replacement for every `entry.get("retired")` test outside the commit
        path. A restaged batch's own flag says only that its FIRST copy is gone;
        what callers actually need is whether the LATEST materialization is
        retired, which is what this answers. Raises on a malformed materialization
        list, so unknown retains instead of reading as retired.
        """

        entry = self._entry
        return bool(BatchFacets(entry).active_copy.get("retired"))


    @property
    def lifetime(self) -> str:
        """A commitments entry's origin lifetime; an entry without one retains."""

        entry = self._entry
        return str(entry.get("lifetime") or oc.ORIGIN_LIFETIME_RETAIN)


    @property
    def state(self) -> str:
        entry = self._entry
        if entry.get("origin_reclaimed"):
            return oc.BATCH_STATE_RECLAIMED
        if entry.get("retiring"):
            return oc.BATCH_STATE_RETIRING
        return oc.BATCH_STATE_COMMITTED


    @property
    def manifest(self) -> object:
        return self._entry.get("manifest_digest")

    @property
    def required_manifest(self) -> object:
        return self._entry["manifest_digest"]

    @property
    def namespace(self) -> object:
        return self._entry.get("batch_namespace")

    @property
    def first_mover(self) -> object:
        return self._entry.get("mover_key")

    @property
    def tier(self) -> object:
        return self._entry.get("tier")

    @property
    def class_bytes(self) -> object:
        return self._entry.get("class_bytes")

    @property
    def paths(self) -> object:
        return self._entry.get("paths")

    @property
    def staged_paths(self) -> object:
        return self._entry.get("staged_paths")

    @property
    def is_retiring(self) -> bool:
        return bool(self._entry.get("retiring"))

    @property
    def delete_intent(self) -> OriginDeleteIntent | None:
        retiring = self._entry.get("retiring")
        if isinstance(retiring, Mapping):
            return OriginDeleteIntent(retiring)
        if retiring:
            raise oc.ProducedOutputError(
                "unknown-retain: retiring record is corrupt")
        return None

    def origin_charge(self, batch_id: str) -> int:
        return sum(oc.output_class_bytes(
            self._entry.get("class_bytes"),
            where=f"committed batch {batch_id!r}").values())

    @property
    def has_retirement_report(self) -> bool:
        return "retirement_report" in self._entry

    @property
    def active(self) -> StageCopy:
        return StageCopy(self.active_copy)

    @property
    def history(self) -> list[StageCopy]:
        return [StageCopy(item) for item in self.copies]

    def open(self, queue_root, instance, template, batch_id):
        return BatchLedger.open(queue_root, instance, template, self._entry,
                                batch_id)

    def effective_origins(self, filed, sealed):
        return effective_origin_identity(filed, sealed, self._entry)


class OriginDeleteIntent:
    """The selected durable delete decision, distinct from its truthiness guard."""

    def __init__(self, decision: Mapping[str, object]):
        self._decision = decision

    @property
    def reason(self) -> str:
        return str(self._decision.get("reason") or "")

    @property
    def consumers(self) -> list[object]:
        return list(self._decision.get("consumers") or [])


class StageCopy:
    """One named copy answer; construction performs no validation or reads."""

    def __init__(self, row) -> None:
        self._row = row

    @property
    def mover_key(self) -> object:
        return self._row.get("mover_key")

    @property
    def tier(self) -> object:
        return self._row.get("tier")

    @property
    def generation(self) -> object:
        return self._row.get("generation")

    @property
    def retired(self) -> object:
        return self._row.get("retired")

    @property
    def state(self) -> object:
        return self._row.get("state")

    @property
    def source(self) -> object:
        return self._row.get("source")

    @property
    def staged_paths(self) -> object:
        return self._row.get("staged_paths")

    @property
    def host(self) -> object:
        return self._row.get("host")

    @property
    def fill_mb_s_pool_side(self) -> object:
        return self._row.get("fill_mb_s_pool_side")


class PrewriteFacts:
    """A validated reservation; separate export and commit questions stay named."""

    def __init__(self, record) -> None:
        self._record = record

    @property
    def owner(self) -> object:
        return self._record.get("owner_action_key")

    @property
    def attempt(self) -> object:
        return self._record.get("owner_attempt")

    @property
    def attempt_binding(self) -> dict[str, object]:
        # Missing and explicitly null attempts retain their original meanings.
        return dict(self._record.get("owner_attempt", {}))

    @property
    def tier(self) -> object:
        return self._record.get("tier")

    @property
    def paths(self) -> list[str]:
        return self._record["paths"]

    @property
    def class_bytes(self) -> dict[str, int]:
        return self._record["class_bytes"]

    def matches_commit(self, instance, tier, sealed) -> bool:
        prewrite = self._record
        return not (prewrite.get("tier") != tier
                or not actual_within_ceiling(prewrite, sealed)
                or not set(str(d["path"]) for d in sealed)
                <= set(prewrite.get("paths", []))
                or prewrite.get("owner_action_key")
                != instance["owner_action_key"]
                or dict(prewrite.get("owner_attempt", {})) != dict(
                    instance["owner_attempt"]))

    def omitted_absent(self, sealed) -> bool:
        return planned_omitted_absent(self._record, sealed)


class BatchLedger:
    """The explicit record boundary for one scope; construction performs no I/O.

    Captures are independent mutable documents. Callers retain their existing
    ownership locks and file at the same points as the domain transitions.
    """

    def __init__(self, path: Path):
        self.path = path

    @classmethod
    def for_instance(cls, queue_root, instance):
        return cls.at_scope(scope_dir(queue_root, instance))

    @classmethod
    def at_scope(cls, scope: Path) -> BatchLedger:
        """Address an already selected scope without reading its bindings."""
        return cls(scope / "commitments.json")

    @property
    def instance_path(self) -> Path:
        return self.path.parent / "instance.json"

    def prewrite_path(self, batch_id) -> Path:
        return self.path.parent / "prewrites" / f"{batch_id}.prewrite.json"

    def prewrite(self, batch_id) -> PrewriteFacts | None:
        record = read_prewrite(self.prewrite_path(batch_id))
        return None if record is None else PrewriteFacts(record)

    def entries(self) -> BatchCapture:
        """Capture one generation of entries; no entry history is read eagerly."""
        return BatchCapture(self, self.capture())

    @staticmethod
    def precommit(
            queue, instance: Mapping[str, object],
            template: Mapping[str, object], batch_id: str,
            descriptors: list[Mapping[str, object]],
            tier: str, mover_key: str) -> dict[str, object]:
        """Read-only validated precommit facts for one batch (base-compatible).

        New shared seam owned by the pool funding lane (read-only; no writer
        mutation, no edits to commit/prewrite/retire/release).  Uses only
        base-available helpers (no R3 loader): validates from LIVE/filed
        precommit records, never from caller-shaped hashes alone:

        * bound template/instance contract (exact template_sha binding);
        * live owner CLAIMED row still names the instance's exact nonce/scope
          (via require_live_owner; stale/superseded/absent raises);
        * durable prewrite exists for batch_id and matches descriptors exactly
          (same tier/class_bytes/paths/owner/attempt checks as commit_batch);
        * every descriptor validates via validate_descriptor against the bound
          template/instance, with recomputed manifest digest.

        Does NOT require the filed batch (commit comes AFTER funding in the
        future writer order: prewrite -> publish mover -> fund -> commit).
        Returns the exact facts the pool output intent binds: owner/template/
        batch/manifest/total/range(0..total)/tier/mover/namespace/class_bytes.
        Raises ProducedOutputError on any mismatch.
        """

        checked_template, checked_instance = oc.bound_output_contract(
            template, instance)
        oc.output_name(batch_id, where="batch_id")
        oc.output_key(mover_key, where="precommit mover_key")
        if tier not in checked_template["permitted_tiers"]:
            raise oc.ProducedOutputError("tier-not-permitted")
        if not isinstance(descriptors, list) or not descriptors:
            raise oc.ProducedOutputError("precommit descriptors required")
        gated = require_live_owner(queue, checked_instance)
        if gated is not None:
            raise oc.ProducedOutputError(str(gated.get("refusal", "owner-not-running")))
        try:
            prewrite = read_prewrite(
                prewrites_dir(queue.root, checked_instance)
                / f"{batch_id}.prewrite.json")
        except oc.ProducedOutputError as exc:
            raise oc.ProducedOutputError(f"unknown-retain: {exc}") from None
        sealed = [oc.validate_descriptor(dict(d), checked_template,
                                      checked_instance)
                  for d in descriptors]
        class_bytes: dict[str, int] = {"payload": 0, "checkpoint": 0, "temp": 0}
        for desc in sealed:
            class_bytes[str(desc["artifact_class"])] += int(desc["bytes"])
        manifest = oc.output_manifest_sha256(sealed)
        total = sum(class_bytes.values())
        if prewrite is None:
            # RESTAGE authority (`ensure_batch_materialized`): the prewrite was
            # consumed by the commit that made this batch durable, so a
            # re-materialization has none to present and the only conformant
            # authority is the COMMITTED record in its restageable state -- first
            # copy stage-retired, origin charge intact, origins still carrying the
            # identity that commit recorded, and PB's own filed materialization
            # row naming exactly this mover key. Every OTHER absent-prewrite case
            # -- an unretired committed batch (its own mover still owns the
            # material), a reclaimed-origin batch, a caller-invented mover key --
            # keeps today's `prewrite-reservation-missing` refusal unchanged.
            if BatchLedger.restage_authority(
                    queue, checked_instance, checked_template, batch_id,
                    manifest, mover_key) is None:
                raise oc.ProducedOutputError("prewrite-reservation-missing")
        else:
            # Ceiling reconciliation (conservative prewrite -> actual): same tier,
            # owner and attempt; actual paths are a SUBSET of the planned superset
            # (unwritten planned paths stay absent, which abort already requires);
            # actual per-class bytes land at or under the admitted ceiling.
            if (prewrite.get("tier") != tier
                    or not actual_within_ceiling(prewrite, sealed)
                    or not set(str(d["path"]) for d in sealed)
                    <= set(prewrite.get("paths", []))
                    or prewrite.get("owner_action_key")
                    != checked_instance["owner_action_key"]
                    or dict(prewrite.get("owner_attempt", {})) != dict(
                        checked_instance["owner_attempt"])):
                raise oc.ProducedOutputError("prewrite-mismatch")
            if not planned_omitted_absent(prewrite, sealed):
                # A planned-but-omitted path still EXISTS (or is unstatable): the
                # prewrite's charge for it must not vanish with the commit.
                raise oc.ProducedOutputError("planned-path-present-retain")
        if total <= 0:
            raise oc.ProducedOutputError("unknown-retain: precommit-total")
        attempt = checked_instance["owner_attempt"]
        assert isinstance(attempt, dict)
        namespace = oc.batch_namespace(checked_instance, batch_id, manifest)
        return {
            "owner_action_key": str(checked_instance["owner_action_key"]),
            "owner_nonce": str(attempt["nonce"]),
            "owner_scope_id": str(attempt["scope_id"]),
            "template_id": str(checked_template["template_id"]),
            "template_sha256": oc.template_sha256(checked_template),
            "batch_id": batch_id,
            "manifest_digest": manifest,
            "total_bytes": total,
            "range_start_bytes": 0,
            "range_end_bytes": total,
            "tier": tier,
            "mover_key": mover_key,
            "batch_namespace": namespace,
            "class_bytes": dict(class_bytes),
        }


    def capture(self) -> dict[str, object]:
        path = self.path
        try:
            raw = json.loads(path.read_text())
        except FileNotFoundError:
            return {"batches": {}, "admission": None}
        except (OSError, ValueError) as exc:
            # Corrupt/unreadable commitments are unknown state, never an empty
            # scope: every caller retains charge and names the record.
            raise oc.ProducedOutputError(
                f"commitments record corrupt or unreadable: {exc}") from None
        if not isinstance(raw, Mapping) or not isinstance(raw.get("batches"), Mapping):
            raise oc.ProducedOutputError("commitments record is corrupt")
        admission = raw.get("admission")
        if admission is not None and not isinstance(admission, Mapping):
            raise oc.ProducedOutputError("commitments admission record is corrupt")
        return {"batches": dict(raw["batches"]), "admission": admission}


    def file(self, record: Mapping[str, object]) -> None:
        path = self.path
        path.parent.mkdir(parents=True, exist_ok=True)
        admission = record.get("admission")
        if admission is None and "admission" not in record:
            # Read-modify-write callers pass batches only; never drop a bound
            # admission credit record on a batch update.  A caller that holds
            # the record it read under the same lock passes ``admission`` itself,
            # which spares a second parse of the whole document (#1072).
            try:
                previous = BatchLedger(path).capture()
                admission = previous.get("admission")
            except oc.ProducedOutputError:
                admission = None
        # One encode and one write.  ``json.dump`` runs the pure-Python encoder
        # (the C encoder serves only ``dumps``, CPython's ``_one_shot`` path) and
        # hands the stream one small chunk at a time; on R13's 9.2 MB document
        # that was a large part of every retirement (#1072).  The text is the
        # same either way.
        text = digest_primitives.sorted_json({"batches": dict(record["batches"]),
                           "admission": admission}) + "\n"
        handle, temporary = tempfile.mkstemp(
            dir=str(path.parent), prefix=".commitments.")
        try:
            with os.fdopen(handle, "w") as stream:
                stream.write(text)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        except BaseException:
            Path(temporary).unlink(missing_ok=True)
            raise


    @staticmethod
    def authorizes_mover(queue_root, instance, checked_template, *, batch_id,
                         manifest, tier_id, mover_key, total) -> bool:
        """The captured batch's immutable and selected-history authority.

        Pool supplies its already checked funding tuple and bindings. This owner
        answers the batch question at the existing conditional read point.
        """
        # Commitments entry (existing helper; small file per instance).
        try:
            commitments = BatchLedger.for_instance(queue_root, instance).capture()
        except oc.ProducedOutputError:
            return False
        batches = commitments.get("batches")
        if not isinstance(batches, dict):
            return False
        entry = batches.get(batch_id)
        if not isinstance(entry, dict):
            return False
        if (str(entry.get("manifest_digest")) != manifest
                or str(entry.get("tier")) != tier_id):
            return False
        # WHICH mover this committed batch authorizes. The batch's own first
        # publication always does. A RE-materialization of the same batch
        # (`produced_output.ensure_batch_materialized`) stages the identical
        # manifest over the identical origins under a successor mover whose
        # key PB sealed and filed on the batch's materialization list, so the
        # committed batch authorizes that key too -- and ONLY while it is the
        # single live row on that list, with the batch's own tier. A
        # malformed list is unknown and authorizes nothing (no credit), never
        # a downgrade to the prewrite path.
        if str(entry.get("mover_key")) != mover_key:
            try:
                live = BatchFacets(entry).pending_copy
            except oc.ProducedOutputError:
                return False
            except (OSError, ValueError):
                return False
            if (live is None
                    or str(live.get("mover_key")) != mover_key
                    or str(live.get("tier")) != tier_id):
                return False
        # Strict loader (R4): bounded, exact binding, re-validated entries,
        # recomputed manifest. Missing/empty entries refuse inside (never
        # vacuously True); corrupt/mismatched commitments never fall through
        # to prewrite (no downgrade).
        try:
            filed, _ = BatchLedger.open(
                queue_root, instance, checked_template, entry, batch_id)
        except oc.ProducedOutputError:
            return False
        except (OSError, ValueError):
            return False
        if (str(filed.get("manifest_digest")) != manifest
                or str(filed.get("tier")) != tier_id
                or int(filed.get("total_bytes", -1)) != total):
            return False
        # The immutable record names the FIRST mover; a successor is
        # authorized only through the filed materialization list checked
        # above, and the record's own mover must still agree with the entry
        # (a changed mover in commitments authorizes nothing).
        if str(filed.get("mover_key")) != str(entry.get("mover_key")):
            return False
        try:
            # The first mover also needs a known whole history. A malformed
            # successor list cannot confer authority on any generation.
            if mover_key not in BatchFacets(entry).movers:
                return False
        except oc.ProducedOutputError:
            return False
        return True


    @staticmethod
    def restage_authority(queue, checked_instance, checked_template,
                                     batch_id: str, manifest: str,
                                     mover_key: str) -> dict[str, object] | None:
        """The restageable committed batch as a funding authority, or None.

        Read-only. The prewrite was consumed by the commit that made this batch
        durable, so a re-materialization has no prewrite to present and must found
        its funding on the immutable record itself -- and ONLY in the exact state
        where restaging is the designed transition:

        * the mutable commitments entry agrees with the immutable record on
          mover/tier/namespace and the recomputed manifest digest equals the seal
          (so caller descriptor drift refuses);
        * the FIRST materialization is stage-retired -- its egress completed, so
          no pin can be holding it -- and the origins were never reclaimed;
        * exactly ONE live materialization is filed and it names THIS mover key.
          The caller therefore cannot pick a successor id: only the key PB itself
          sealed and filed under the ownership lock is fundable.
        * the sealed origins still carry the identity the first commit recorded,
          or the one its entry re-pinned after a content check (#1111).

        A damaged record or commitments file raises `unknown-retain` (fail
        closed), and a changed or unprovable origin raises its own typed reason so
        funding refuses by name; an absent entry, an unretired batch, a
        reclaimed-origin batch, a digest disagreement or a mover key with no filed
        materialization returns None and the caller keeps its ordinary refusal.
        """

        try:
            commitments = BatchLedger.for_instance(queue.root, checked_instance).capture()
        except oc.ProducedOutputError as exc:
            raise oc.ProducedOutputError(f"unknown-retain: {exc}") from None
        batches = commitments["batches"]
        assert isinstance(batches, Mapping)
        entry = batches.get(batch_id)
        if not isinstance(entry, Mapping):
            return None
        try:
            filed, sealed = BatchLedger.open(
                queue.root, checked_instance, checked_template, entry, batch_id)
        except oc.ProducedOutputError as exc:
            raise oc.ProducedOutputError(str(exc)) from None
        try:
            namespace = oc.batch_namespace(checked_instance, batch_id, manifest)
        except oc.ProducedOutputError as exc:
            raise oc.ProducedOutputError(str(exc)) from None
        if (not entry.get("retired")
                or entry.get("origin_reclaimed")
                or str(entry.get("manifest_digest") or "") != manifest
                or str(filed.get("manifest_digest") or "") != manifest
                or str(entry.get("batch_namespace") or "") != namespace
                or str(filed.get("batch_namespace") or "") != namespace):
            return None
        live = BatchFacets(entry).pending_copy
        if live is None or str(live.get("mover_key")) != str(mover_key):
            return None
        # Against the entry's re-pins, and never a read: the restage filed any
        # re-pin under the lock before this key was sealed (#1111).
        ok, refusal = recheck_origin_identity(filed, sealed, entry)
        if not ok:
            raise oc.ProducedOutputError(str(refusal))
        return dict(entry)


    @staticmethod
    def append_materialization(
            queue, checked_instance, checked_template, batch_id: str, *,
            mover_key: str, tier: str, generation: int, host: str,
            fill: int | None = None) -> bool:
        """File one restage INTENT under the caller's ownership lock.

        This is the durable resumption point, and it is filed BEFORE any funding
        or movement side effect reaches the pool: a crash at any later prefix
        finds this row, re-drives the SAME sealed mover key, and allocates neither
        a fresh generation nor a second credit.

        Re-validates provenance exactly as the primary commit path does (the
        immutable record loads; the entry still agrees on mover/tier/namespace;
        the first materialization is stage-retired with its origin charge intact),
        re-derives the generation from the record on disk, and appends
        `{mover_key, tier, generation, retired: False, state: "intent", host}`,
        plus `fill_mb_s_pool_side` when the sealed mover reserved pool fill
        (#747): a resume republishes the row from this record without re-sealing,
        and a row's resources must equal its sealed demand.
        Idempotent: a materialization already filed with this mover key returns
        True without appending. Any provenance failure raises.
        """

        path = BatchLedger.for_instance(queue.root, checked_instance).path
        try:
            commitments = BatchLedger(path).capture()
        except oc.ProducedOutputError as exc:
            raise oc.ProducedOutputError(f"unknown-retain: {exc}") from None
        batches = commitments["batches"]
        assert isinstance(batches, dict)
        entry = batches.get(batch_id)
        if not isinstance(entry, Mapping):
            raise oc.ProducedOutputError("unknown batch_id for this instance")
        try:
            filed, _sealed = BatchLedger.open(
                queue.root, checked_instance, checked_template, entry, batch_id)
        except oc.ProducedOutputError as exc:
            raise oc.ProducedOutputError(str(exc)) from None
        if (not entry.get("retired")
                or entry.get("origin_reclaimed")
                or str(entry.get("mover_key") or "")
                != str(filed.get("mover_key") or "")
                or str(entry.get("tier") or "") != str(filed.get("tier") or "")
                or str(filed.get("tier") or "") != tier):
            raise oc.ProducedOutputError("unknown-retain: restage-target-mismatch")
        existing = BatchFacets(entry).copies
        for item in existing:
            if str(item.get("mover_key")) == str(mover_key):
                return True
        if BatchFacets(entry).pending_copy is not None:
            raise oc.ProducedOutputError("materialization-live-retain")
        if len(existing) + 1 != generation:
            # Another restage (or a crash-retry) advanced the list between this
            # caller's read and this append; the sealed key it published belongs
            # to the generation it read. Refuse rather than filing a
            # generation-mismatched row: every step is idempotent, so the retry
            # re-derives the current generation and heals by re-calling.
            raise oc.ProducedOutputError("restage-generation-changed")
        row: dict[str, object] = {
            "mover_key": str(mover_key), "tier": tier,
            "generation": int(generation), "retired": False,
            "state": "intent", "host": str(host)}
        if fill is not None:
            row["fill_mb_s_pool_side"] = int(fill)
        updated = dict(entry)
        updated["materializations"] = existing + [row]
        batches[batch_id] = updated
        try:
            BatchLedger(path).file({"batches": batches})
        except oc.ProducedOutputError as exc:
            raise oc.ProducedOutputError(f"unknown-retain: {exc}") from None
        return False


    @staticmethod
    def fund_materialization(
            queue, checked_instance, batch_id: str, *, mover_key: str) -> None:
        """Advance one filed materialization intent to `funded` under the lock.

        Bookkeeping only -- the authority is the pool's own funding record, which
        this caller has just read as `transferring` with the tokens held under the
        mover. Idempotent, and it never invents a row: a mover key with no filed
        materialization raises rather than filing one late.
        """

        path = BatchLedger.for_instance(queue.root, checked_instance).path
        try:
            commitments = BatchLedger(path).capture()
        except oc.ProducedOutputError as exc:
            raise oc.ProducedOutputError(f"unknown-retain: {exc}") from None
        batches = commitments["batches"]
        assert isinstance(batches, dict)
        entry = batches.get(batch_id)
        if not isinstance(entry, Mapping):
            raise oc.ProducedOutputError("unknown batch_id for this instance")
        items = BatchFacets(entry).copies
        changed = False
        for index, item in enumerate(items):
            if str(item.get("mover_key")) == str(mover_key):
                if item.get("retired"):
                    raise oc.ProducedOutputError(
                        "unknown-retain: materialization-retired")
                if str(item.get("state")) != "funded":
                    item = dict(item)
                    item["state"] = "funded"
                    items[index] = item
                    changed = True
                break
        else:
            raise oc.ProducedOutputError("unknown-retain: materializations")
        if not changed:
            return
        updated = dict(entry)
        updated["materializations"] = items
        batches[batch_id] = updated
        try:
            BatchLedger(path).file({"batches": batches})
        except oc.ProducedOutputError as exc:
            raise oc.ProducedOutputError(f"unknown-retain: {exc}") from None




    @staticmethod
    def admit(queue, instance: Mapping[str, object],
                       template: Mapping[str, object]) -> dict[str, object]:
        """Record the instance's bound admission metadata (idempotent).

        Copies the template's per-tier minimum/window demands into commitments
        as the scope side of the funding contract. This record is BINDING
        metadata only: it authorizes no bytes and funds nothing. Physical
        funding happens per batch at `commit_batch` (exact ledger acquire +
        whole transfer); general window admission is `admit_funded_window`
        (pending on the liveness primitive). Returns {"ok": True, ...}.
        """

        try:
            checked_template, checked_instance = oc.bound_output_contract(
                template, instance)
        except oc.ProducedOutputError:
            return {"ok": False, "refusal": "template-mismatch"}
        with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
            path = BatchLedger.for_instance(queue.root, checked_instance).path
            try:
                commitments = BatchLedger(path).capture()
            except oc.ProducedOutputError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            demands = checked_template["working_demands"]
            assert isinstance(demands, dict)
            admission = {
                "minimum_gib": {tier: int(demands[tier]["minimum_gib"])
                                for tier in checked_template["permitted_tiers"]},
                "window_gib": {tier: int(demands[tier]["window_gib"])
                               for tier in checked_template["permitted_tiers"]},
                "bound_unix": time.time(),
            }
            BatchLedger(path).file({"batches": commitments["batches"],
                                      "admission": admission})
        return {"ok": True, "admission": admission}


    @staticmethod
    def reserve(queue, instance: Mapping[str, object],
                         template: Mapping[str, object], *, batch_id: str,
                         tier: str, class_bytes: Mapping[str, int],
                         paths: list[str]) -> dict[str, object]:
        """File a prewrite budget claim BEFORE any HDD byte is written.

        The production writer path must call this (not an optional helper):
        uncharged temp/checkpoint writes refuse here. `paths` names the PLANNED
        durable-origin files of this batch (absolute, normalized, under the
        template prefix, distinct) -- a conservative superset is allowed, and
        the commit's descriptors must be a subset of it. `class_bytes` are
        per-class CEILINGS for the batch's temporary lifetime: the headroom
        charges them while the prewrite lives, and the commit's actual bytes
        must land at or under each bound (exact sizes remain the special case
        of an exact ceiling). Abort requires every PLANNED path absent.
        Uncommitted does NOT mean unwritten: files without a batch never enter
        staged accounting.
        Checks the bound admission record (binding metadata, never funding),
        durable headroom for the planned class bytes, and PATH OWNERSHIP --
        one origin path has at most one live writer, so a path a committed
        unretired batch or another outstanding prewrite already owns refuses
        `prewrite-path-owned-by-live-batch` here rather than at the mover,
        after the bytes are written (`own_write_owner`). Ownership ends at
        retirement, which evicts the staged copy. Across actions (#1053), a
        path another action's LIVE attempt has committed (unless reclaimed) or
        prewritten refuses `prewrite-path-owned-by-live-action`, naming that
        owner (`foreign_write_owner`); a dead or finished action's paths
        are free to regenerate, which is how a relaunch resumes. Then files an immutable
        prewrite record the later commit must present. Physical funding happens
        only at `commit_batch` (exact ledger acquire) and in the liveness
        window primitive (pending). Zero-byte classes are valid (explicit
        zeros, never missing keys).
        """

        try:
            checked_template, checked_instance = oc.bound_output_contract(
                template, instance)
        except oc.ProducedOutputError:
            return {"ok": False, "refusal": "template-mismatch"}
        oc.output_name(batch_id, where="batch_id")
        if tier not in checked_template["permitted_tiers"]:
            return {"ok": False, "refusal": "tier-not-permitted"}
        if not isinstance(class_bytes, Mapping) or set(class_bytes) != {
                "payload", "checkpoint", "temp"}:
            return {"ok": False, "refusal": "prewrite-classes-must-name-all-three"}
        planned = {}
        for cls in ("payload", "checkpoint", "temp"):
            planned[cls] = oc.nonnegative_output_integer(class_bytes.get(cls),
                                       where=f"prewrite class_bytes.{cls}")
        if not isinstance(paths, list) or not paths:
            return {"ok": False, "refusal": "prewrite-paths-required"}
        planned_paths = _prewrite_paths(
            str(checked_template["output_prefix"]), paths)
        if len(set(planned_paths)) != len(planned_paths):
            return {"ok": False, "refusal": "prewrite-paths-must-be-distinct"}
        planned_paths.sort()
        try:
            # This prefix's lock and every overlapping template's (#1063).
            locks = path_locks(queue, checked_instance)
        except oc.ProducedOutputError as exc:
            return {"ok": False, "refusal": str(exc)}
        with locks:
            # Write authorization lives here: a SUCCESS return is the producer's
            # permission to start writing HDD bytes, so a stale, superseded, or
            # absent owner refuses BEFORE the first payload byte -- not later at
            # commit. The idempotent duplicate replay below re-checks the same
            # gate: a read-only duplicate grants no permission for new writes.
            gated = require_live_owner(queue, checked_instance)
            if gated is not None:
                return gated
            try:
                sums = _outstanding_sums(queue.root, checked_instance, batch_id)
            except oc.ProducedOutputError as exc:
                return {"ok": False, "refusal": str(exc)}
            if not isinstance(BatchLedger.for_instance(queue.root, checked_instance).capture().get(
                    "admission"), Mapping):
                # The bound admission record proves the instance was admitted;
                # without it nothing is prewritable. (Zero-minimum tiers still
                # need the bound record, never ledger presence.)
                return {"ok": False, "refusal": "prewrite-not-admitted"}
            maxima = oc.checked_instance_maxima(checked_template)
            for cls in ("payload", "checkpoint", "temp"):
                cap = maxima[f"{cls}_max_bytes"]
                if sums[cls] + planned[cls] > cap:
                    return {"ok": False, "refusal": f"prewrite-exceeds-{cls}-maxima",
                            "class": cls}
            # One origin path, one live writer. Staged material identity is
            # keyed by the path, so authorizing a second writer here would
            # authorize bytes the mover must later refuse -- after they are
            # written and committed. This gate is where that is answered,
            # before the first payload byte, which is the contract this
            # function already states for a stale or absent owner.
            try:
                owned = own_write_owner(queue.root, checked_instance,
                                         checked_template, planned_paths,
                                         batch_id)
            except oc.ProducedOutputError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            if owned is not None:
                owned_path, owner_kind, owner_batch_id = owned
                return {"ok": False,
                        "refusal": "prewrite-path-owned-by-live-batch",
                        "path": owned_path, "owner_kind": owner_kind,
                        "owner_batch_id": owner_batch_id}
            # The same rule across actions (#1053): a path another action's
            # live attempt has committed or prewritten is that attempt's. Under
            # the locks of every template whose prefix overlaps this one's
            # (#1063), which each of them takes to prewrite or commit, so none
            # can claim a path between this check and the record filed below.
            try:
                foreign = foreign_write_owner(
                    queue, checked_instance, checked_template, planned_paths)
            except oc.ProducedOutputError as exc:
                reason = str(exc)
                if not reason.startswith("unknown-retain"):
                    reason = f"unknown-retain: {reason}"
                return {"ok": False, "refusal": reason}
            if foreign is not None:
                return {"ok": False,
                        "refusal": "prewrite-path-owned-by-live-action", **foreign}
            path = prewrites_dir(queue.root, checked_instance) / \
                f"{batch_id}.prewrite.json"
            try:
                publish_prewrite(
                    queue, checked_instance, batch_id=batch_id, tier=tier,
                    class_bytes=planned, paths=planned_paths)
            except Exception as exc:
                # Same body republishes idempotently; a different body for a
                # live batch id refuses (the first reservation stands).
                try:
                    existing = read_prewrite(path)
                except oc.ProducedOutputError as inner:
                    return {"ok": False, "refusal": f"prewrite-unreadable: {inner}"}
                if (existing is not None and existing.get("tier") == tier
                        and dict(existing.get("class_bytes", {})) == planned
                        and list(existing.get("paths", [])) == planned_paths):
                    return {"ok": True, "batch_id": batch_id,
                            "class_bytes": planned, "paths": planned_paths,
                            "duplicate": True}
                return {"ok": False, "refusal": f"prewrite-conflict: {exc}"}
        return {"ok": True, "batch_id": batch_id, "class_bytes": planned,
                "paths": planned_paths}


    @staticmethod
    def abort(queue, instance: Mapping[str, object],
                       template: Mapping[str, object], *, batch_id: str) -> dict[str, object]:
        """Abort an outstanding prewrite after proving nothing durable remains.

        Allowed only while the batch is uncommitted. Uncommitted does NOT mean
        unwritten, so every planned path must be absent (safe disposal is the
        producer's job: this lane never deletes durable-origin files): a
        present file refuses `abort-files-present-retain`, an unstatable one
        retains unknown. Frees the outstanding accounting headroom; no ledger
        tokens move because prewrites hold none (physical funding happens at
        commit). Returns {"ok": True, "aborted": ...}.
        """

        try:
            checked_template, checked_instance = oc.bound_output_contract(
                template, instance)
        except oc.ProducedOutputError:
            return {"ok": False, "refusal": "template-mismatch"}
        oc.output_name(batch_id, where="batch_id")
        with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
            try:
                commitments = BatchLedger.for_instance(queue.root, checked_instance).capture()
            except oc.ProducedOutputError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            batches = commitments["batches"]
            assert isinstance(batches, dict)
            if batch_id in batches:
                return {"ok": False, "refusal": "batch-committed"}
            try:
                prewrite = read_prewrite(
                    prewrites_dir(queue.root, checked_instance)
                    / f"{batch_id}.prewrite.json")
            except oc.ProducedOutputError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            if prewrite is None:
                return {"ok": True, "batch_id": batch_id, "aborted": False}
            # Prepaid lane (R7 liveness): while a pool funding intent names this
            # batch, the prewrite is its precommit recovery authority (drive
            # accepts precommit-OR-commit only while the owner is live). Deleting
            # it here would strand the intent -- and any credits it already
            # transferred -- with no completion path. Retire the intent first
            # (release_output_funding proves mover nonexecution); abort refuses
            # while one exists. An unreadable census retains the same way.
            try:
                intents, census_unknown = queue.output_census_for_owner(
                    str(checked_instance["owner_action_key"]))
            except Exception as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            if census_unknown:
                return {"ok": False, "refusal": "unknown-retain: funding-census"}
            _attempt = checked_instance["owner_attempt"]
            assert isinstance(_attempt, dict)
            for record in intents:
                if (str(record.get("batch_id")) == batch_id
                        and str(record.get("tier_id"))
                        == str(prewrite.get("tier"))
                        and str(record.get("template_sha256"))
                        == oc.template_sha256(checked_template)
                        and str(record.get("owner_nonce"))
                        == str(_attempt["nonce"])
                        and str(record.get("owner_scope_id"))
                        == str(_attempt["scope_id"])):
                    return {"ok": False,
                            "refusal": "prepaid-intent-exists-retain",
                            "mover_action_key": str(
                                record.get("mover_action_key")),
                            "intent_state": str(record.get("state"))}
            for planned in prewrite.get("paths", []):
                try:
                    os.lstat(str(planned))
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    return {"ok": False, "refusal": f"unknown-retain: {exc}"}
                return {"ok": False, "refusal": "abort-files-present-retain",
                        "path": str(planned)}
            path = prewrites_dir(queue.root, checked_instance) / f"{batch_id}.prewrite.json"
            try:
                path.unlink()
            except FileNotFoundError:
                return {"ok": True, "batch_id": batch_id, "aborted": False}
            except OSError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        return {"ok": True, "batch_id": batch_id, "aborted": True}


    @staticmethod
    def commit_origin(queue, instance: Mapping[str, object],
                            template: Mapping[str, object],
                            descriptors: list[Mapping[str, object]], *,
                            batch_id: str,
                            landed: Mapping[str, Mapping[str, object]] | None = None,
                            lifetime: str = oc.ORIGIN_LIFETIME_RETAIN
                            ) -> dict[str, object]:
        """Commit one write-only batch at its origin, with no stage copy (#912).

        The write-only sibling of `commit_batch`. A write-only template's batches
        are never read again by the action that wrote them, so nothing is staged,
        no mover is sealed and no tier token moves: the batch is its origin files,
        which the producer wrote -- through the paced spool export or directly --
        under a prewrite this commit consumes. A later action declares the batch
        in its data manifest (`origin_batch_manifest`) and stages it like any
        other input.

        Checks are `commit_batch`'s, minus the window and the funding: the live
        owner under the output-prefix locks (this prefix's and every overlapping
        template's, `path_locks`, #1063), the prewrite present and
        matched (its tier, owner and attempt, the paths a subset of the planned
        ones, each class within its ceiling), every planned path the batch omits
        absent, the durable maxima, and one lstat per origin for size and
        identity, taken under those locks (#1064). The identity is recorded in
        the batch, and `origin_batch_manifest` rechecks it before any consumer is
        told the bytes are there.

        Two checks are this commit's own:

        - Every descriptor carries its sha256. No mover copies an origin-only
          batch at commit, so the digest is the only thing that binds the bytes a
          consumer's mover later copies to the bytes committed here; that mover
          verifies it on copy.
        - ``landed`` maps each origin path to the identity its writer recorded
          when the file landed; the spool passes its export receipt's
          (`ProducedSpool.commit_origin_group`). Each origin must still be that
          file. An export retried before its receipt can replace a landed copy,
          so an identity taken from before the receipt is not the committed one.
          An origin whose timestamps alone moved since the receipt (an NFS
          delegation recall, #1111) is read and hashed before the lock is taken;
          when its digest is the descriptor's, the hashed identity is the one
          committed, and the entry and the answer carry ``landed_repins``. The
          lstat under the lock must find the identity the read hashed; one whose
          timestamps moved again meanwhile (a retirement's link-back that the
          lock ordered, #1064) is read once more with the lock let go, and a
          second move refuses rather than commit an identity the file no longer
          has.

        A read-back template's batch commits here too (#1034), but only with
        ``landed``: an owner that reads a group back from its own local spool
        (PQ #1118) publishes no stage copy, so `commit_batch` never runs for it,
        and without this commit its prewrite stayed outstanding at its ceiling
        for the owner's whole life. The spool commits it once the export is
        acknowledged; a caller with no export receipt still refuses
        ``template-reads-back``. Such a batch is no handoff: `load_origin_batch`
        and `declare_origin_consumer` refuse it, and a ``consumed`` one is
        retired once its owner attempt has ended, success included.

        The commitments entry carries ``origin_only: true`` and no mover. Its
        paths stay owned until `reclaim_origin` proves them absent, because a
        consumer may have declared them; its durable class bytes stay charged
        until then too, exactly as for a staged batch.

        ``lifetime`` (#914) says who ends the batch. ``retain``, the default,
        leaves that to the producer and the operator: PB never deletes it, and
        nothing about it differs from #912. ``consumed`` hands it to
        `origin_retirement_tick`, which deletes the origin once every consumer
        that declared the batch has succeeded, or once its producer attempt is
        dead if no consumer declared it. The lifetime is part of the record and
        the entry, so a replay that names another lifetime refuses.

        Idempotent: a replay with the same manifest answers the duplicate, and a
        crash between filing the record and filing the entry resumes from the
        filed record rather than refusing it. Returns ``{"ok": True, ..., "ref"}``
        or a typed refusal.
        """

        from prismabuild import record_store

        try:
            checked_template, checked_instance = oc.bound_output_contract(
                template, instance)
        except oc.ProducedOutputError:
            return {"ok": False, "refusal": "template-mismatch"}
        oc.output_name(batch_id, where="batch_id")
        if lifetime not in oc.ORIGIN_LIFETIMES:
            raise oc.ProducedOutputError(
                f"origin batch lifetime must be one of {sorted(oc.ORIGIN_LIFETIMES)}, "
                f"not {lifetime!r}")
        if not checked_template.get("write_only") and landed is None:
            # A read-back template's batch commits here only when its owner reads
            # it back from its own local spool (#1034): the spool's export
            # receipt is what passes ``landed``.  Otherwise its batches are staged
            # for their owner to read again: `commit_batch`, through a mover and a
            # funded window.
            return {"ok": False, "refusal": "template-reads-back"}
        if not isinstance(descriptors, list) or not descriptors:
            return {"ok": False, "refusal": "batch-has-no-entries"}
        sealed = [oc.validate_descriptor(d, checked_template, checked_instance)
                  for d in descriptors]
        if any(desc["sha256"] is None for desc in sealed):
            return {"ok": False, "refusal": "origin-batch-needs-sha256"}
        if landed is not None and (
                not isinstance(landed, Mapping)
                or set(landed) != {str(desc["path"]) for desc in sealed}):
            return {"ok": False, "refusal": "origin-is-not-the-landed-copy"}
        class_bytes = {"payload": 0, "checkpoint": 0, "temp": 0}
        for desc in sealed:
            class_bytes[str(desc["artifact_class"])] += int(desc["bytes"])
        manifest_digest = oc.output_manifest_sha256(sealed)
        batch_ns = oc.batch_namespace(checked_instance, batch_id, manifest_digest)
        prewrite_path = (prewrites_dir(queue.root, checked_instance)
                         / f"{batch_id}.prewrite.json")
        ref = oc.origin_batch_ref(checked_instance, batch_id=batch_id,
                               manifest_digest=manifest_digest)
        # Each landed origin whose timestamps alone moved, read and hashed with
        # no lock held (#1111), by path; the lock's lstat must find the identity
        # the read hashed. Two rounds: one more if it moved again in between.
        commitments_path = BatchLedger.for_instance(queue.root, checked_instance).path
        verified: dict[str, dict[str, object]] = {}
        for _round in (1, 2):
            if landed is not None:
                refusal = read_landed_origins(sealed, landed, verified)
                if refusal is not None:
                    return refusal
            try:
                # This prefix's lock and every overlapping template's (#1063).
                locks = path_locks(queue, checked_instance)
            except oc.ProducedOutputError as exc:
                return {"ok": False, "refusal": str(exc)}
            with locks:
                # The identity is taken under the locks (#1064): a retirement's
                # delete, which holds one of them whatever its template's prefix
                # (#1063), can move an origin aside and link it back
                # (`_unlink_if_committed`), which moves its ctime, so an identity
                # taken before the locks could be stale before it was filed, and
                # an lstat taken while the file was aside would refuse.
                origin_identity, identity_refusal = origin_identity_at_commit(sealed)
                if identity_refusal is not None:
                    return identity_refusal
                assert origin_identity is not None
                landed_repins, moved, refusal = landed_identity(
                    sealed, landed, verified, origin_identity)
                if refusal is not None:
                    return refusal
                if moved:
                    # Read again with the lock let go.
                    continue
                try:
                    commitments = BatchLedger(commitments_path).capture()
                except oc.ProducedOutputError as exc:
                    return {"ok": False, "refusal": f"unknown-retain: {exc}"}
                batches = commitments["batches"]
                assert isinstance(batches, dict)
                if batch_id in batches:
                    existing = batches[batch_id]
                    if (isinstance(existing, Mapping)
                            and existing.get("origin_only") is True
                            and existing.get("manifest_digest") == manifest_digest):
                        if BatchFacets(existing).lifetime != lifetime:
                            return {"ok": False, "refusal": "batch-lifetime-mismatch"}
                        # The commit consumed the prewrite; a crash after the entry
                        # and before the unlink leaves it, and the replay finishes it.
                        prewrite_path.unlink(missing_ok=True)
                        return {"ok": True, "batch_id": batch_id, "duplicate": True,
                                "batch_namespace": batch_ns,
                                "manifest_digest": manifest_digest,
                                "origin_only": True, "ref": ref}
                    return {"ok": False, "refusal": "batch-id-in-use"}
                gated = require_live_owner(queue, checked_instance)
                if gated is not None:
                    return gated
                try:
                    prewrite = read_prewrite(prewrite_path)
                except oc.ProducedOutputError as exc:
                    return {"ok": False, "refusal": f"prewrite-unreadable: {exc}"}
                if prewrite is None:
                    return {"ok": False, "refusal": "prewrite-reservation-missing"}
                tier = str(prewrite.get("tier") or "")
                if (tier not in checked_template["permitted_tiers"]
                        or not actual_within_ceiling(prewrite, sealed)
                        or not set(str(d["path"]) for d in sealed)
                        <= set(prewrite.get("paths", []))
                        or prewrite.get("owner_action_key")
                        != checked_instance["owner_action_key"]
                        or dict(prewrite.get("owner_attempt", {})) != dict(
                            checked_instance["owner_attempt"])):
                    return {"ok": False, "refusal": "prewrite-mismatch"}
                if not planned_omitted_absent(prewrite, sealed):
                    return {"ok": False, "refusal": "planned-path-present-retain"}
                try:
                    sums = _class_sums(batches)
                except oc.ProducedOutputError as exc:
                    return {"ok": False, "refusal": f"unknown-retain: {exc}"}
                maxima = oc.checked_instance_maxima(checked_template)
                for cls in sums:
                    if sums[cls] + class_bytes[cls] > maxima[f"{cls}_max_bytes"]:
                        return {"ok": False, "refusal": f"commit-exceeds-{cls}-maxima"}
                body = _origin_record(
                    checked_template, checked_instance, batch_id=batch_id,
                    batch_ns=batch_ns, manifest_digest=manifest_digest, tier=tier,
                    class_bytes=class_bytes, sealed=sealed,
                    origin_identity=origin_identity, lifetime=lifetime)
                batch_dir = (Path(queue.root) / "residency" / oc.OUTPUT_BATCHES_SUBDIR
                             / oc.instance_namespace(checked_instance))
                batch_dir.mkdir(parents=True, exist_ok=True)
                batch_path = batch_dir / f"{batch_id}.json"
                record = dict(body, unix=time.time())
                try:
                    immutable_files.publish_exact(
                        batch_path,
                        core._compact_ascii_lf_bytes(record)[:-1].decode("ascii").encode() + b"\n",
                        where="produced-output batch", conflict_error=record_store.PoolContractError)
                except record_store.PoolContractError as exc:
                    # A crash after this record and before the entry below: the same
                    # batch, over the same identities, resumes from what is filed.
                    # Anything else is a different batch under this id.
                    try:
                        filed = json.loads(batch_path.read_text())
                    except (OSError, ValueError):
                        filed = None
                    if (not isinstance(filed, Mapping)
                            or {k: v for k, v in filed.items() if k != "unix"}
                            != json.loads(json.dumps(body))):
                        return {"ok": False, "refusal": f"batch-conflict: {exc}"}
                # `retired` stays false for the life of the entry: nothing was staged
                # to retire, `_active_materialization` answers "no copy" from
                # `origin_only`, and `_committed_restage_authority` reads this flag,
                # so false is also what keeps the batch unfundable as a restage.
                entry = {
                    "manifest_digest": manifest_digest,
                    "batch_namespace": batch_ns,
                    "tier": tier,
                    "mover_key": None,
                    "origin_only": True,
                    "class_bytes": class_bytes,
                    "paths": sorted(str(d["path"]) for d in sealed),
                    "retired": False,
                    "origin_reclaimed": False,
                }
                if lifetime != oc.ORIGIN_LIFETIME_RETAIN:
                    entry["lifetime"] = lifetime
                if landed_repins:
                    # What the commit read to accept the landed copy; the record's
                    # identity is already the hashed one, so nothing chains from it.
                    entry["landed_repins"] = landed_repins
                batches[batch_id] = entry
                BatchLedger(commitments_path).file({"batches": batches})
                prewrite_path.unlink(missing_ok=True)
            break
        else:
            # Its timestamps moved after each read. The identity the lock found
            # was never read, and committing it unread would bless bytes nobody
            # checked (#1111).
            return {"ok": False, "refusal": "origin-is-not-the-landed-copy",
                    "detail": f"{', '.join(moved)}: its timestamps moved again "
                              f"after its content was verified"}
        result = {"ok": True, "batch_id": batch_id, "batch_namespace": batch_ns,
                  "manifest_digest": manifest_digest, "class_bytes": class_bytes,
                  "tier": tier, "entries": sealed, "origin_only": True, "ref": ref}
        if lifetime != oc.ORIGIN_LIFETIME_RETAIN:
            result["lifetime"] = lifetime
        if landed_repins:
            result["landed_repins"] = landed_repins
        return result


    @staticmethod
    def records(queue, instance: Mapping[str, object],
                      template: Mapping[str, object]) -> list[dict[str, object]]:
        """Every batch this instance committed, as :func:`batch_record` reads it.

        In ``batch_id`` order, reclaimed and retiring batches included: the
        caller filters on ``state``.  An instance that committed nothing answers
        ``[]``; an unreadable commitments document, or any batch whose record
        does not read and validate, raises `ProducedOutputError` -- the census
        rule: unknown is never read as empty, so no batch is silently dropped
        from the list.
        """

        checked_template, checked_instance = oc.bound_output_contract(
            template, instance)
        commitments = BatchLedger.for_instance(queue.root, checked_instance).capture()
        batches = commitments["batches"]
        assert isinstance(batches, Mapping)
        return [_public_batch_record(queue.root, checked_instance,
                                     checked_template, batches[batch_id],
                                     str(batch_id))
                for batch_id in sorted(batches)]


    @staticmethod
    def record(queue, instance: Mapping[str, object],
                     template: Mapping[str, object], *, batch_id: str
                     ) -> dict[str, object]:
        """Read-only: one committed batch's record, validated (#955).

        The public reader for a produced-output batch, so a caller outside PB
        never imports `_load_batch_record` or `_read_commitments`.  Returns:

        - ``entries``: every committed descriptor, ``path``, ``bytes``,
          ``sha256`` and ``artifact_class``, in the record's order;
        - ``lifetime``: ``retain`` or ``consumed`` (``ORIGIN_LIFETIME_*``);
        - ``commitment``: the batch's entry in the instance's commitments
          document, as filed (class bytes, manifest digest, materializations,
          a ``retiring`` decision, ``origin_reclaimed``);
        - ``state``: ``committed``, ``retiring`` (the retirement tick decided to
          delete its origin files) or ``reclaimed`` (PB stopped charging it);
        - ``record``: the filed batch record itself (``origin_identity``
          included), plus ``total_bytes`` and ``manifest_digest`` off it.

        Mutates nothing and takes no lock.  The record is validated exactly as
        retirement and reclaim validate it: bound to this instance, attempt and
        template, every descriptor re-checked, the manifest digest recomputed.
        Raises `ProducedOutputError` for a template the instance is not bound
        to (``template-mismatch``), a batch the instance never committed
        (``unknown-batch``), and a commitments document or batch record that is
        missing, unreadable or does not validate (``unknown-retain: ...``):
        unknown is never read as empty.
        """

        checked_template, checked_instance = oc.bound_output_contract(
            template, instance)
        oc.output_name(batch_id, where="batch_id")
        commitments = BatchLedger.for_instance(queue.root, checked_instance).capture()
        batches = commitments["batches"]
        assert isinstance(batches, Mapping)
        if batch_id not in batches:
            raise oc.ProducedOutputError(f"unknown-batch: {batch_id}")
        return _public_batch_record(queue.root, checked_instance,
                                    checked_template, batches[batch_id], batch_id)


    @staticmethod
    def materialization(queue, instance: Mapping[str, object],
                              template: Mapping[str, object], *, batch_id: str
                              ) -> dict[str, object]:
        """Read-only: which materialization of this batch is current, and where.

        The question a bounded-window reader asks before deciding whether it needs
        `ensure_batch_materialized`: is a stage copy of this batch live now, under
        which mover, at which generation, and has its mover's receipt said the
        bytes landed whole. Mutates nothing, takes no lock, and fails closed --
        an unreadable record or a malformed materialization list answers
        `unknown-retain`, never "nothing is staged".

        One receipt read answers both mover questions, so `mover_receipt_complete`
        and `mover_refusal` can never describe two different observations of a
        record the mover rewrites. A typed `origin_unreachable` -- the mover's
        proof that a produced-output prefix is not on the tier host -- is
        returned through the existing failure contract as `ok: False` with
        `refusal` beside the retained mover/materialization identity, which is
        the gate every caller already raises on; the owner therefore stops on
        the first failed attempt instead of reading the same symptom a mover
        defect produces. Only the ACTIVE materialization's own receipt can
        answer this: a retired predecessor's refusal is history, not this
        state. An absent or unreadable receipt leaves both answers None --
        silence is never a named failure and never readiness.
        """

        try:
            checked_template, checked_instance = oc.bound_output_contract(
                template, instance)
        except oc.ProducedOutputError:
            return {"ok": False, "refusal": "template-mismatch"}
        oc.output_name(batch_id, where="batch_id")
        try:
            commitments = BatchLedger.for_instance(queue.root, checked_instance).capture()
        except oc.ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        batches = commitments["batches"]
        assert isinstance(batches, dict)
        entry = batches.get(batch_id)
        if not isinstance(entry, Mapping):
            return {"ok": False, "refusal": "unknown-batch"}
        try:
            filed, _sealed = BatchLedger.open(
                queue.root, checked_instance, checked_template, entry, batch_id)
            active = BatchFacets(entry).active_copy
        except oc.ProducedOutputError as exc:
            return {"ok": False, "refusal": str(exc)}
        mover = str(active.get("mover_key") or "")
        receipt = mover_receipt(queue, mover)
        complete = receipt_complete(receipt)
        refusal = receipt_refusal(receipt)
        state = {
            "ok": True,
            "batch_id": batch_id,
            "tier": str(filed.get("tier") or ""),
            "manifest_digest": str(filed.get("manifest_digest") or ""),
            "batch_namespace": str(filed.get("batch_namespace") or ""),
            "total_bytes": int(filed.get("total_bytes") or 0),
            "mover_key": mover,
            "generation": int(active.get("generation") or 0),
            "source": str(active.get("source") or ""),
            "funding_state": str(active.get("state") or ""),
            "stage_retired": bool(active.get("retired")),
            "origin_reclaimed": bool(entry.get("origin_reclaimed")),
            "mover_receipt_complete": complete,
            "mover_refusal": refusal,
        }
        match RecordStore(queue.root).mover(mover) if mover else Absent():
            case Present(QueueRow(mover_state, _row)):
                pass
            case Absent():
                mover_state = "absent"
            case Unknown():
                mover_state = "unknown"
        state["mover_queue_state"] = mover_state
        if refusal == oc.ORIGIN_UNREACHABLE_REFUSAL:
            state["ok"] = False
            state["refusal"] = refusal
        return state




    @staticmethod
    def open(queue_root: str | Path,
                           checked_instance: Mapping[str, object],
                           checked_template: Mapping[str, object],
                           commitments_entry: Mapping[str, object],
                           batch_id: str) -> tuple[dict[str, object], list[dict[str, object]]]:
        """Load and fully validate one immutable batch record.

        The single loader for retire/reclaim (and any future reader): bounded
        intake, exact schema/id/instance binding, strict integer shapes (bools
        are not counts), a non-empty entry list whose per-class sizes and
        total match the record, and -- strongest -- every entry re-validated
        as a descriptor against the bound template and instance (mapping
        shape enforced by the descriptor validator itself, never `dict()`
        coercion) with the canonical manifest digest recomputed over them
        matching the seal. A missing, unreadable, or damaged record
        (entries removed, replaced, unbound, or non-mapping) raises
        `ProducedOutputError`: unknown or tampered state retains quota and
        refuses retirement, never filters away to a vacuous proof.
        """

        attempt = checked_instance["owner_attempt"]
        assert isinstance(attempt, dict)
        batch_file = (Path(queue_root) / "residency" / oc.OUTPUT_BATCHES_SUBDIR
                      / oc.instance_namespace(checked_instance)
                      / f"{batch_id}.json")
        try:
            with open(batch_file, "rb") as handle:
                raw = handle.read(4 * 1024 * 1024 + 1)
        except FileNotFoundError:
            raise oc.ProducedOutputError(
                "unknown-retain: batch-record-missing") from None
        except OSError as exc:
            raise oc.ProducedOutputError(f"unknown-retain: {exc}") from None
        if len(raw) > 4 * 1024 * 1024:
            raise oc.ProducedOutputError("unknown-retain: batch-record-oversize")
        try:
            filed = json.loads(raw.decode())
        except (ValueError, UnicodeDecodeError) as exc:
            raise oc.ProducedOutputError(
                f"unknown-retain: batch-record-unreadable: {exc}") from None
        if not isinstance(filed, Mapping):
            raise oc.ProducedOutputError("unknown-retain: batch-record-missing")
        if filed.get("schema") != oc.BATCH_SCHEMA_V1:
            raise oc.ProducedOutputError("unknown-retain: batch-record-schema")
        filed_attempt = filed.get("owner_attempt")
        if (not isinstance(filed_attempt, Mapping)
                or set(filed_attempt) != {"nonce", "scope_id"}):
            raise oc.ProducedOutputError("unknown-retain: batch-record-attempt")
        if (str(filed.get("batch_id") or "") != batch_id
                or str(filed.get("template_id") or "")
                != str(checked_template["template_id"])
                or str(filed.get("template_sha256") or "")
                != str(checked_instance["template_sha256"])
                or str(filed.get("owner_action_key") or "")
                != str(checked_instance["owner_action_key"])
                or str(filed_attempt.get("nonce") or "") != str(attempt["nonce"])
                or str(filed_attempt.get("scope_id") or "")
                != str(attempt["scope_id"])
                or str(filed.get("manifest_digest") or "")
                != str(commitments_entry.get("manifest_digest") or "")):
            raise oc.ProducedOutputError(
                "unknown-retain: batch-record-binding-mismatch")
        entries = filed.get("entries")
        if not isinstance(entries, list) or not entries:
            raise oc.ProducedOutputError("unknown-retain: batch-record-no-entries")
        sealed = [oc.validate_descriptor(entry, checked_template,
                                      checked_instance)
                  for entry in entries]
        if oc.strict_output_integer(filed.get("entry_count"),
                       where="batch record entry_count") != len(sealed):
            raise oc.ProducedOutputError("unknown-retain: batch-record-count")
        classes = {"payload": 0, "checkpoint": 0, "temp": 0}
        for desc in sealed:
            classes[str(desc["artifact_class"])] += int(desc["bytes"])
        stored_classes = commitments_entry.get("class_bytes")
        if not isinstance(stored_classes, Mapping):
            raise oc.ProducedOutputError("unknown-retain: batch-record-classes")
        for cls in classes:
            if int(classes[cls]) != oc.strict_output_integer(
                    stored_classes.get(cls),
                    where=f"committed batch {batch_id!r}.{cls}"):
                raise oc.ProducedOutputError("unknown-retain: batch-record-classes")
        if sum(classes.values()) != oc.strict_output_integer(
                filed.get("total_bytes"), where="batch record total_bytes"):
            raise oc.ProducedOutputError("unknown-retain: batch-record-total")
        if oc.output_manifest_sha256(sealed) != str(filed.get("manifest_digest")):
            raise oc.ProducedOutputError("unknown-retain: batch-record-manifest")
        return dict(filed), sealed








    @staticmethod
    def reclaim(queue, instance: Mapping[str, object],
                       template: Mapping[str, object], *,
                       batch_id: str) -> dict[str, object]:
        """Free durable-origin quota after proving every origin path absent.

        Stage retirement frees the tier window only. Each committed batch keeps
        charging its payload/checkpoint/temp classes until this call stats
        every sealed entry path and finds all of them absent (producer-side
        disposal: this call never unlinks an origin file). A present file
        refuses `origin-present-retain` keeping the charge; an unstatable
        path retains unknown. Exactly-once: already-reclaimed returns
        `{"ok": True, "reclaimed": False}`.

        The one place PB itself deletes origin files is the retirement of a
        ``consumed`` origin-only batch (`origin_retirement_tick`, #914), which
        sets the same ``origin_reclaimed`` flag when it is done.
        """

        try:
            checked_template, checked_instance = oc.bound_output_contract(
                template, instance)
        except oc.ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        oc.output_name(batch_id, where="batch_id")
        with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
            try:
                commitments = BatchLedger.for_instance(queue.root, checked_instance).capture()
            except oc.ProducedOutputError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            batches = commitments["batches"]
            assert isinstance(batches, dict)
            entry = batches.get(batch_id)
            if not isinstance(entry, Mapping):
                return {"ok": False, "refusal": "unknown-batch"}
            if entry.get("origin_reclaimed"):
                return {"ok": True, "batch_id": batch_id, "reclaimed": False}
            # Origin paths come from the single batch-record loader (exact
            # sealed descriptors, binding/entries/total re-validated), never
            # from caller arguments: a damaged record retains instead of
            # proving a vacuous absence.
            try:
                _, sealed = BatchLedger.open(
                    queue.root, checked_instance, checked_template,
                    entry, batch_id)
            except oc.ProducedOutputError as exc:
                return {"ok": False, "refusal": str(exc)}
            for desc_path in sorted(str(desc["path"]) for desc in sealed):
                if not desc_path:
                    return {"ok": False, "refusal": "unknown-retain: bad-entry"}
                try:
                    os.lstat(desc_path)
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    return {"ok": False, "refusal": f"unknown-retain: {exc}"}
                return {"ok": False, "refusal": "origin-present-retain",
                        "path": desc_path}
            entry = dict(entry)
            entry["origin_reclaimed"] = True
            batches[batch_id] = entry
            try:
                BatchLedger.for_instance(queue.root, checked_instance).file({"batches": batches})
            except oc.ProducedOutputError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        return {"ok": True, "batch_id": batch_id, "reclaimed": True}


    @staticmethod
    def publish_stage(queue, checked_template, checked_instance, sealed, *,
                      batch_id, batch_ns, manifest_digest, tier, mover,
                      class_bytes, batch_total, origin_identity, batches,
                      funding_path):
        """Publish the immutable stage batch, file its charge, then consume prewrite."""
        from . import record_store

        batch_record = {
            "schema": oc.BATCH_SCHEMA_V1,
            "batch_id": batch_id,
            "batch_namespace": batch_ns,
            "manifest_schema": oc.BATCH_MANIFEST_SCHEMA_V1,
            "manifest_digest": manifest_digest,
            "tier": tier,
            "mover_key": mover,
            "class_bytes": class_bytes,
            "total_bytes": batch_total,
            "entry_count": len(sealed),
            "entries": sealed,
            "template_id": str(checked_template["template_id"]),
            "template_sha256": oc.template_sha256(checked_template),
            "owner_action_key": str(checked_instance["owner_action_key"]),
            "owner_attempt": dict(checked_instance["owner_attempt"]),
            # The immutable producer record of what the origins WERE at the
            # moment this batch became durable. Restage rechecks exactly this
            # before every re-materialization and refuses a changed file; a
            # batch filed before this field existed carries no proof and can
            # never be restaged (its current bytes are never retroactively
            # blessed).
            "origin_identity": origin_identity,
            "object_set_id": oc.manifest_object_set_id({
                f"{d['bytes']}:{d['path']}": {"bytes": int(d["bytes"]),
                                             "sha256": (d["sha256"]
                                                        if d["sha256"]
                                                        is not None else
                                                        None)}
                for d in sealed}),
            "unix": time.time(),
        }
        batch_dir = (Path(queue.root) / "residency" / oc.OUTPUT_BATCHES_SUBDIR
                     / oc.instance_namespace(checked_instance))
        batch_dir.mkdir(parents=True, exist_ok=True)
        batch_path = batch_dir / f"{batch_id}.json"
        try:
            immutable_files.publish_exact(
                batch_path,
                core._compact_ascii_lf_bytes(batch_record)[:-1].decode("ascii").encode() + b"\n",
                where="produced-output batch", conflict_error=record_store.PoolContractError)
        except record_store.PoolContractError as exc:
            return {"ok": False, "refusal": f"batch-conflict: {exc}"}
        batches[batch_id] = {
            "manifest_digest": manifest_digest,
            "batch_namespace": batch_ns,
            "tier": tier,
            "mover_key": mover,
            "class_bytes": class_bytes,
            # The origin paths this batch owns while it is live. Indexed
            # here so `own_write_owner` can answer ownership from the
            # commitments record alone: re-deriving it from the immutable
            # batch record would make one damaged record freeze every
            # later prewrite for the instance, including paths that batch
            # never owned.
            "paths": sorted(str(d["path"]) for d in sealed),
            "retired": False,
            "origin_reclaimed": False,
        }
        BatchLedger.for_instance(queue.root, checked_instance).file({"batches": batches})
        # Legacy intent + prewrite consumed only here, after the durable
        # batch publication: a crash anywhere above resumes from the
        # intent, never by re-acquiring the same budget. The prepaid pool
        # record is deliberately NOT consumed at commit: it stays
        # `transferring` until the mover's claim marks it `consumed`, and
        # release refuses it while the filed commit stands.
        _delete_funding(funding_path)
        (prewrites_dir(queue.root, checked_instance)
         / f"{batch_id}.prewrite.json").unlink(missing_ok=True)

    @staticmethod
    def repin_scope(queue, key, instance, items):
        """Revalidate captured origin re-pins and file one instance atomically."""
        path = Path(key)
        with queue.stage_ownership_lock(str(instance["output_prefix"])):
            commitments = BatchLedger(path).capture()
            batches = commitments["batches"]
            assert isinstance(batches, dict)
            changed = False
            for item, repins in items:
                batch_id = str(item["ref"]["batch_id"])
                entry = batches.get(batch_id)
                if (not isinstance(entry, Mapping)
                        or entry.get("origin_only") is not True
                        or entry.get("manifest_digest")
                        != item["ref"]["manifest_digest"]
                        or entry.get("origin_reclaimed")):
                    continue
                updated, refusal = apply_origin_repins(
                    entry, item["record"], item["entries"], repins)
                if updated is None:
                    detail = (f": {refusal['detail']}"
                              if refusal and refusal.get("detail") else "")
                    raise oc.ProducedOutputError(
                        f"origin-batch-changed: "
                        f"{(refusal or {}).get('refusal')} ({batch_id}){detail}")
                if updated != entry:
                    batches[batch_id] = updated
                    changed = True
            if changed:
                BatchLedger(path).file(commitments)


    @staticmethod
    def due_mover_rows(queue, instance: Mapping[str, object],
                       template: Mapping[str, object], *,
                       residency_schema: str) -> list[dict[str, object]]:
        """Frozen mover-row skeletons for batches needing (re)publication.

        NEW method owned by this lane (pure preparation, no queue mutation).
        For each committed unretired batch: composing fragments under their own
        namespace need no row; a FAILED mover whose fence is still fundable
        needs a retry row; an absent mover with no staged fragments needs its
        first row. A batch whose filed funding record reads `consumed` gets no
        row at all -- see `funding_verdict`.

        Composing is deliberately the test HERE, unlike in the censuses, and it
        does not mean the batch is complete: a partially staged batch composes
        too, and it is skipped because it has no publishable row, not because
        it needs nothing. Its funding is spent and its mover key is fixed by
        the batch, so any row this could emit would be unfundable; the
        disposition that batch actually needs is the terminal route
        `recover_batches` names (retire -> reclaim -> re-plan). Emitting a
        retry row for it would be inventing work the pool cannot admit.

        Each row
        carries the mover-variant residency block (`pool.validate_residency`
        accepts it: batch manifest digest + 0..total range on the batch tier
        with demand at/above the range floor) and qualified tier demand, so the
        submitter lane seals it (pbrun movement-graph machinery: real declared
        inputs/argv/manifest) through the EXISTING `queue.publish` + claim
        channel unchanged. Publication itself (behind the funding gate) stays
        with the tier loop. Deterministic order: batch_id ascending.
        """

        from prismabuild import record_store
        from prismabuild import residency_map as map_mod
        from prismabuild import storage_tiers as tiers_mod

        checked_template = oc.validate_template(template)
        checked_instance = oc.validate_instance(instance)
        try:
            commitments = BatchLedger.for_instance(queue.root, checked_instance).capture()
        except oc.ProducedOutputError:
            return []
        batches = commitments["batches"]
        assert isinstance(batches, dict)
        out_base = output_fragment_root(queue.root / record_store.RESIDENCY)
        rows: list[dict[str, object]] = []
        for batch_id in sorted(batches):
            entry = batches[batch_id]
            if not isinstance(entry, Mapping):
                continue
            # The row a batch needs is its ACTIVE materialization's row. After a
            # restage the entry's own mover is spent and terminal; publishing for
            # it would be work no claim can admit, while the live successor --
            # the one that actually needs a row -- would be skipped.
            try:
                active = BatchFacets(entry).active_copy
            except oc.ProducedOutputError:
                continue
            if active.get("retired"):
                continue
            ns = str(entry.get("batch_namespace") or "")
            mover = str(active.get("mover_key") or "")
            tier = str(active.get("tier") or "")
            manifest = str(entry.get("manifest_digest") or "")
            generation = int(active.get("generation") or 0)
            if not ns or not mover or not tier or not manifest:
                continue
            try:
                fragments = map_mod.read_fragments(out_base, ns)
                staged = bool(fragments) and bool(map_mod.compose(fragments))
            except Exception:
                staged = False
            if staged:
                continue
            mover_row = RecordStore(queue.root).mover(mover)
            if (isinstance(mover_row, Present)
                    and mover_row.value.state in ("claimed", "ready")):
                continue
            # A batch whose fence is spent can never fund another claim on this
            # mover key, so a row emitted for it would be work the pool cannot
            # admit -- the retry-pointing-at-unclaimable-work defect in row
            # form. An unreadable fence is not proof either way, and inventing
            # work on unknown is the wrong direction for a function that
            # creates it. Both defer to the terminal route `recover_batches`
            # names; neither is a claim that the batch is finished.
            verdict, _reason = funding_verdict(queue, mover, tier)
            if verdict in ("spent", "unknown"):
                continue
            class_bytes = entry.get("class_bytes")
            total = sum(int(class_bytes.get(c, 0)) for c in
                        ("payload", "checkpoint", "temp")) \
                if isinstance(class_bytes, Mapping) else 0
            kind = tiers_mod.capacity_kind_of(tier)
            try:
                gib = tiers_mod.stage_tokens_for_bytes(total) if total > 0 else 0
            except ValueError:
                gib = 0
            demand = {f"{kind}{tiers_mod.TIER_DEMAND_SEPARATOR}{tier}": gib,
                      "cpu": 1, "mem_gb": 1} if gib > 0 else {"cpu": 1, "mem_gb": 1}
            rows.append({
                "batch_id": batch_id,
                "action_key": mover,
                "tier": tier,
                "manifest_digest": manifest,
                "batch_namespace": ns,
                "resources": demand,
                "residency": {
                    "schema": residency_schema,
                    "tier_id": tier,
                    "manifest_sha256": manifest,
                    "manifest_bytes": total if total > 0 else 1,
                    "range_start_bytes": 0,
                    "range_end_bytes": total if total > 0 else 1,
                },
                "generation": generation,
                "reason": ("retry-failed-mover" if isinstance(mover_row, Present)
                           and mover_row.value.state == "failed"
                           else "needs-publish"),
            })
        return rows









class BatchCapture:
    """One captured scope generation and the transitions that may update it.

    Capturing validates the document shape only. History and immutable-record
    reads remain conditional on the caller's question. Caller-held prefix locks
    and file points stay explicit; there is no implicit refresh or transaction.
    """

    def __init__(self, ledger, record) -> None:
        self.ledger = ledger
        self._record = record
        self._batches = record["batches"]

    def __len__(self) -> int:
        return len(self._batches)

    def __contains__(self, batch_id) -> bool:
        return batch_id in self._batches

    def __iter__(self):
        return iter(self._batches)

    def __getitem__(self, batch_id) -> BatchFacets | None:
        entry = self._batches[batch_id]
        return BatchFacets(entry) if isinstance(entry, Mapping) else None

    def lookup(self, batch_id) -> BatchFacets | None:
        entry = self._batches.get(batch_id)
        return BatchFacets(entry) if isinstance(entry, Mapping) else None

    def items(self):
        return ((key, self[key]) for key in self._batches)

    def values(self):
        return (self[key] for key in self._batches)

    def outstanding_prewrite_ids(self) -> list[str]:
        """Batch ids with a prewrite in this ledger's scope and no committed batch.

        A commit consumes its prewrite, so a record whose batch id is not in the
        commitments is outstanding. Raises ``OSError`` when the directory cannot
        be listed.
        """
        scope = self.ledger.path.parent
        batches = self._batches

        try:
            with os.scandir(scope / "prewrites") as iterator:
                names = sorted(entry.name for entry in iterator
                               if entry.name.endswith(".prewrite.json"))
        except FileNotFoundError:
            return []
        ids = [name[:-len(".prewrite.json")] for name in names]
        return [batch_id for batch_id in ids if batch_id not in batches]


    @property
    def charges(self) -> dict[str, int]:
        return _class_sums(self._batches)

    @property
    def unretired_stage_count(self) -> int:
        """The replay benchmark's count, retaining its original row policy."""
        return sum(1 for entry in self._batches.values()
                   if isinstance(entry, dict) and not entry.get("origin_only")
                   and not BatchFacets(entry).stage_retired)

    @property
    def unretired_staged(self) -> list[str]:
        return _unretired_staged_batches(self._batches)

    def file(self, *, admission=False, reads=None) -> None:
        if reads is not None:
            # Invalidate even when the subsequent canonical write fails (#992).
            reads.forget(self.ledger.path.parent)
        self.ledger.file(self._record if admission else {"batches": self._batches})

    def retirement(self, batch_id, *, reads) -> RetirementEntry:
        entry = self._batches[batch_id]
        return RetirementEntry(self, batch_id, dict(entry), reads)

    def publish_stage(self, queue, template, instance, sealed, **publication):
        # Keep the canonical C12 orchestration seam and its argument semantics.
        return BatchLedger.publish_stage(queue, template, instance, sealed,
                                         batches=self._batches, **publication)


    def retire_materialization(self,
            batch_id: str, *, mover_key: str,
            receipt: Mapping[str, object], canonical_ns: str,
            staged_paths: list[str]) -> bool:
        """Mark one materialization's stage retirement in this capture.

        The caller captured the commitments under its ownership lock and files
        this capture under the same lock. This operation marks without reading
        or filing (#1072). Returns whether it changed anything.

        Proof-checked exactly like the primary retirement: the egress receipt must
        be complete, error-free, and name THIS materialization's mover with the
        batch's canonical namespace. The namespace is digest-derived and shared by
        every materialization of one batch, so the reader contract is unchanged.
        The durable charge is NOT touched -- retiring a materialization returns
        window credit only, and the origin charge stays constant across forward
        and reverse staging.
        """
        batches = self._batches

        if not isinstance(receipt, Mapping) or receipt.get("complete") is not True:
            raise oc.ProducedOutputError("retire needs a complete egress receipt")
        if receipt.get("errors"):
            raise oc.ProducedOutputError("retire needs an error-free egress receipt")
        if (str(receipt.get("action_key") or "") != str(mover_key)
                or str(receipt.get("consumer_action_key") or "") != canonical_ns):
            raise oc.ProducedOutputError("retire receipt names another materialization")
        entry = batches.get(batch_id)
        if not isinstance(entry, Mapping):
            raise oc.ProducedOutputError("unknown batch_id for this instance")
        items = BatchFacets(entry).copies
        for index, item in enumerate(items):
            if str(item.get("mover_key")) == str(mover_key):
                if item.get("retired"):
                    return False
                item = dict(item)
                item["retired"] = True
                item["staged_paths"] = sorted(set(staged_paths))
                items[index] = item
                break
        else:
            raise oc.ProducedOutputError("unknown-retain: materializations")
        updated = dict(entry)
        updated["materializations"] = items
        batches[batch_id] = updated
        return True


    def retire_first_copy(self, queue, checked_instance: Mapping[str, object],
                                   checked_template: Mapping[str, object],
                                   batch_id: str,
                                   receipt: Mapping[str, object],
                                   staged_paths: list[str]) -> bool:
        """Mark stage retirement in this capture under the ownership lock.

        The caller captured the commitments under that lock and files this
        capture under the same lock. This operation marks without reading or
        filing (#1072). Returns whether it changed anything.

        Proof-checked, never a bare flag: the receipt must be a complete
        egress for this exact batch (mover + namespace match, no errors),
        and the batch must still be unretired. Records the staged paths the
        egress vouched so later census attribution can name them.
        """
        batches = self._batches

        entry = batches.get(batch_id)
        if not isinstance(entry, Mapping):
            raise oc.ProducedOutputError("unknown batch_id for this instance")
        if entry.get("retired"):
            return False
        # Owner/attempt/manifest proof comes from the filed immutable batch
        # record through the single loader: exact schema/id/binding plus
        # re-validated entries whose canonical digest matches the seal.
        # Neither a caller dict nor a bare commitments flag can retire
        # another attempt's batch or a damaged record.
        try:
            BatchLedger.open(queue.root, checked_instance, checked_template,
                                entry, batch_id)
        except oc.ProducedOutputError as exc:
            raise oc.ProducedOutputError(str(exc)) from None
        if not isinstance(receipt, Mapping) or receipt.get("complete") is not True:
            raise oc.ProducedOutputError("retire needs a complete egress receipt")
        if receipt.get("errors"):
            raise oc.ProducedOutputError("retire needs an error-free egress receipt")
        if (str(receipt.get("action_key") or "") != str(entry.get("mover_key") or "")
                or str(receipt.get("consumer_action_key") or "")
                != str(entry.get("batch_namespace") or "")):
            raise oc.ProducedOutputError("retire receipt names another batch")
        entry = dict(entry)
        entry["retired"] = True
        entry["staged_paths"] = sorted(set(staged_paths))
        batches[batch_id] = entry
        return True


    def record_staged_paths(self, queue, checked_instance: Mapping[str, object],
                             batch_id: str,
                             source: str,
                             mover_key: str, staged_paths: list[str]) -> None:
        """File the staged paths BEFORE a tier-host egress can drop the fragment.

        Caller holds the output-prefix ownership lock. The in-process egress reads
        the fragment and deletes in one call, so the paths it vouched are in hand
        when the retirement is filed. A tier-host egress ends between two calls,
        and the call that files the retirement finds the fragment already gone:
        without this, a retired batch would record NO staged paths, and a live pin
        over one of them could never be attributed to it again. `retired` is not
        touched -- the same field, written early, on a copy that is still live.
        """
        batches = self._batches
        entry = self._batches[batch_id]

        paths = sorted(set(staged_paths))
        updated = dict(entry)
        if source == "materialization":
            items = BatchFacets(entry).copies
            for index, item in enumerate(items):
                if str(item.get("mover_key")) == str(mover_key):
                    item = dict(item)
                    item["staged_paths"] = paths
                    items[index] = item
                    break
            else:
                raise oc.ProducedOutputError("unknown-retain: materializations")
            updated["materializations"] = items
        else:
            updated["staged_paths"] = paths
        batches[batch_id] = updated
        BatchLedger.for_instance(queue.root, checked_instance).file({"batches": batches})


    def file_retirement(self, queue, checked_instance: Mapping[str, object],
                                checked_template: Mapping[str, object],
                                batch_id: str,
                                selection: Mapping[str, object],
                                receipt: Mapping[str, object]
                                ) -> dict[str, object]:
        """Phase three of a retirement: revalidate and mark, under the ownership lock.

        This capture holds the commitments the caller read again under the
        lock it took back after egress. The caller files it once for every
        batch it marks.  The EXACT selection is revalidated before anything is
        marked: the record still loads, the batch still resolves to the same
        manifest, and the active materialization is still the generation whose
        copy this receipt just deleted.  A selection that moved underneath the
        egress is unknown state and retains.  The answer carries ``marked``:
        whether this capture changed.
        """
        batches = self._batches

        entry = batches.get(batch_id)
        if not isinstance(entry, Mapping):
            return {"ok": False, "refusal": "unknown-retain: unknown-batch"}
        try:
            filed, _sealed = BatchLedger.open(
                queue.root, checked_instance, checked_template, entry,
                batch_id)
            active = BatchFacets(entry).active_copy
        except oc.ProducedOutputError as exc:
            return {"ok": False, "refusal": str(exc)}
        target_mover = str(selection["target_mover"])
        generation = int(selection["generation"])  # type: ignore[arg-type]
        if (str(filed.get("manifest_digest") or "") != selection["manifest"]
                or str(active.get("mover_key") or "") != target_mover
                or str(active.get("source") or "") != selection["source"]
                or int(active.get("generation") or 0) != generation):
            return {"ok": False,
                    "refusal": "unknown-retain: materialization-changed"}
        if active.get("retired"):
            return {"ok": True, "batch_id": batch_id, "duplicate": True,
                    "receipt": receipt, "mover_key": target_mover,
                    "generation": generation, "marked": False}
        staged_paths = list(selection["staged_paths"])  # type: ignore[arg-type]
        try:
            if selection["source"] == "materialization":
                marked = self.retire_materialization(
                    batch_id, mover_key=target_mover, receipt=receipt,
                    canonical_ns=str(selection["canonical_ns"]),
                    staged_paths=staged_paths)
            else:
                marked = self.retire_first_copy(
                    queue, checked_instance, checked_template, batch_id,
                    receipt, staged_paths)
        except oc.ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        return {"ok": True, "batch_id": batch_id, "receipt": receipt,
                "mover_key": target_mover, "generation": generation,
                "staged_paths": sorted(set(staged_paths)), "marked": marked}


    def origin_gate(self, batch_id: str,
                            entry: BatchFacets,
                            filed: Mapping[str, object],
                            sealed: list[dict[str, object]],
                            verified: Sequence[Mapping[str, object]] | None
                            ) -> dict[str, object]:
        """The origin check a caller holding the prefix lock makes (#1111).

        Without a read. When only re-pins are missing and ``verified`` carries
        them (hashed by the caller before it took the lock), they are checked
        against a fresh stat, appended to the entry and written, and the check
        is made again. Returns `check_origin_identity`'s answer.
        """
        batches = self._batches
        commitments = self._record
        commitments_path = self.ledger.path
        entry = entry._entry

        verdict = check_origin_identity(filed, sealed, entry)
        if (verdict["ok"] or verdict.get("refusal") != "restage-origin-unverified"
                or not verified):
            return verdict
        updated, refusal = apply_origin_repins(entry, filed, sealed, verified)
        if updated is None:
            return {"ok": False, **(refusal or {})}
        if updated != entry:
            batches = commitments["batches"]
            assert isinstance(batches, dict)
            batches[batch_id] = updated
            BatchLedger(commitments_path).file(commitments)
        return check_origin_identity(filed, sealed, updated)



class RetirementEntry(BatchFacets):
    """A selected private entry copy, filed at the existing retirement points."""

    def __init__(self, capture, batch_id, entry, reads) -> None:
        super().__init__(entry)
        self._capture = capture
        self._batch_id = batch_id
        self._reads = reads

    def file(self) -> None:
        self._capture._batches[self._batch_id] = self._entry
        self._capture.file(reads=self._reads)


    def begin_origin_retirement(self, *, reason, consumers):
        """Record the selected delete intent before the first unlink."""
        entry = self._entry
        entry["retiring"] = {"reason": reason, "consumers": consumers}
        entry.pop("retirement_report", None)


    def complete_origin_retirement(self):
        """End the durable charge after the selected paths have settled."""
        entry = self._entry
        entry["origin_reclaimed"] = True
        entry.pop("retirement_report", None)


    def retirement_report(self, event):
        """Remember one changed report without rewriting an unchanged decision."""
        entry = self._entry
        signature = digest_primitives.raw_sha256(
            digest_primitives.sorted_json(event).encode())
        if entry.get("retirement_report") == signature:
            return False
        entry["retirement_report"] = signature
        return True


    def quiet_retirement(self):
        entry = self._entry
        if "retirement_report" in entry:
            entry.pop("retirement_report")
            return True
        return False


    def retirement_repins(self, repinned):
        entry = self._entry
        if repinned:
            entry["origin_repins"] = [
                *(entry.get("origin_repins") or []), *repinned]
            repinned.clear()


def prewrite_inputs(record_path: Path, record: PrewriteFacts | None,
                     kept_dirs: Sequence[str] | None = None,
                     seen: dict[str, object] | None = None
                     ) -> tuple[object, ...] | None:
    """``(record version, directories, their versions)``, or None if unreadable.

    The record's version is taken before the caller reads it and the
    directories' before any path is ``lstat``ed, so a change that lands
    between is seen as a change next cycle, never remembered as none.
    ``seen`` keeps each directory's version for one caller's pass: an
    instance's prewrites usually name one directory.
    """

    def version_of(directory: str) -> object:
        if seen is None:
            return _file_version(directory)
        if directory not in seen:
            seen[directory] = _file_version(directory)
        return seen[directory]

    try:
        version = _file_version(record_path)
        if kept_dirs is None:
            paths = record.paths if record is not None else []
            dirs = tuple(sorted({os.path.dirname(str(path)) for path in paths}))
        else:
            dirs = tuple(kept_dirs)
        return (version, dirs, tuple(version_of(item) for item in dirs))
    except oc.ProducedOutputError:
        return None


def file_consumer_declaration(queue_root, instance, batch_id, consumer, checked_ref):
    from . import record_store

    directory = _consumers_dir(queue_root, instance, batch_id)
    directory.mkdir(parents=True, exist_ok=True)
    body = {"schema": oc.ORIGIN_CONSUMER_SCHEMA_V1,
            "consumer_action_key": consumer, "ref": dict(checked_ref)}
    try:
        immutable_files.publish_exact(
            directory / f"{consumer}.json",
            core._compact_ascii_lf_bytes(body)[:-1].decode("ascii").encode() + b"\n",
            where="produced-output origin consumer", conflict_error=record_store.PoolContractError)
    except record_store.PoolContractError as exc:
        raise oc.ProducedOutputError(f"origin-consumer-conflict: {exc}") from None


class ConsumerReleaseFile:
    """One prepared release body; its queue index is filed before publication."""

    def __init__(self, directory, consumer, body) -> None:
        self._directory = directory
        self._consumer = consumer
        self._body = body

    @classmethod
    def prepare(cls, queue_root, instance, batch_id, consumer, checked_ref, *,
                state, by, reason, sealed) -> ConsumerReleaseFile:
        directory = _released_consumers_dir(queue_root, instance, batch_id)
        directory.mkdir(parents=True, exist_ok=True)
        body = {"schema": oc.ORIGIN_CONSUMER_RELEASE_SCHEMA_V1,
                "consumer_action_key": consumer, "ref": dict(checked_ref),
                "state": state, "by": str(by), "reason": str(reason),
                "released_unix": time.time(), **sealed}
        return cls(directory, consumer, body)

    def file(self) -> None:
        from . import record_store

        directory, consumer, body = self._directory, self._consumer, self._body
        try:
            immutable_files.publish_exact(
                directory / f"{consumer}.json",
                core._compact_ascii_lf_bytes(body)[:-1].decode("ascii").encode() + b"\n",
                where="produced-output origin consumer release", conflict_error=record_store.PoolContractError)
        except record_store.PoolContractError as exc:
            raise oc.ProducedOutputError(
                f"origin-consumer-release-conflict: {exc}") from None


def retirement_origin_proof(path, published, observed, sha256, *, where):
    """A verified re-pin, or the same retained content/refusal observation."""
    repin, why = _verify_origin_content(path, published, observed, sha256,
                                       where=where)
    if repin is not None:
        return repin
    changed = (path, _identity_key(observed), sha256) in _CONTENT_REFUSED
    return {"refused": why, "reason": (
        "origin-changed" if changed else "origin-unverified")}


_INDEXED_ATTEMPTS: set[tuple[str, str, str]] = set()
_INDEX_MARKED: set[str] = set()


def backfill_attempt(queue_root: str | Path, owner: str, scope_name: str) -> bool:
    """Index one scope the tick listed, if it is not yet (#1053).

    A scope filed before the index existed, or by a producer running code
    that predates it, has no pointer; the tick files it, idempotently.
    Returns False when it could not be filed.
    """

    key = (str(queue_root), owner, scope_name)
    if key in _INDEXED_ATTEMPTS:
        return True
    template_id, dot, nonce = scope_name.rpartition(".")
    if (not dot or not template_id or len(owner) != 64
            or len(nonce) != oc.NONCE_HEX_LENGTH or any(c not in oc.OUTPUT_KEY_HEX for c in nonce)):
        # Not an attempt's scope (an older flat spelling): nothing to index.
        _INDEXED_ATTEMPTS.add(key)
        return True
    pointer = _attempts_root(queue_root) / template_id / f"{owner}.{nonce}"
    try:
        if not os.path.lexists(pointer):
            _index_attempt(queue_root, owner, template_id, nonce)
    except oc.ProducedOutputError:
        return False
    _INDEXED_ATTEMPTS.add(key)
    return True



def finish_attempt_index(queue_root, *, complete):
    if complete and str(queue_root) not in _INDEX_MARKED:
        try:
            if not os.path.lexists(_attempts_root(queue_root)
                                   / oc.ATTEMPT_INDEX_COMPLETE):
                _mark_attempt_index_complete(queue_root)
            _INDEX_MARKED.add(str(queue_root))
        except oc.ProducedOutputError:
            pass


def spool_claims(queue, instance, template, wanted, *, lease_timeout_s):
    """Foreign export claims; byte identity and ended-export authority stay in spool."""
    own = (str(instance["owner_action_key"]), str(instance["template_id"]),
           str(instance["owner_attempt"]["nonce"]))
    reads = LedgerReads(queue, lease_timeout_s=lease_timeout_s)
    claims = {}
    attempts = template_attempts(
        queue.root, overlapping_templates(queue.root, template))
    for owner, template_id, nonce in attempts:
        if (owner, template_id, nonce) == own:
            continue
        _version, committed, prewritten = reads.owned(
            attempt_scope(queue.root, owner, template_id, nonce))
        named = [(kind, path, batch_id)
                 for kind, owned in (("batch", committed), ("prewrite", prewritten))
                 for path, batch_id in sorted(owned.items()) if path in wanted]
        if not named:
            continue
        state = attempt_state(reads.generation(owner), nonce)
        for kind, path, batch_id in named:
            claims.setdefault(path, []).append({
                "owner_action_key": owner, "owner_nonce": nonce,
                "template_id": template_id, "owner_kind": kind,
                "owner_batch_id": batch_id, "owner_state": state})
    return claims

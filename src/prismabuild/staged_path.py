"""Staged-copy publication, ownership census and containment acts."""
from __future__ import annotations
import array
import bisect
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from contextlib import ExitStack, contextmanager, nullcontext, suppress
import contextvars
import errno
import hashlib
import json
import os
from pathlib import Path
import socket
import stat as statmod
import struct
import sys
import threading
import time
from prismabuild import core as pb, kept_reads, manifest_layout, movement_actions, pool
from prismabuild import reader_lease, residency_map, residency_plan, storage_tiers, tier_holdings
from prismabuild import output_vocabulary
from prismabuild import progress as pb_progress
from . import data_manifest as manifest_codec
from prismabuild.stage_marks import STAGE_MOVER_XATTR, STAGE_SOURCE_XATTR, source_identity


def _date_vouch(root, fragment, generation, entries, *, material_epoch):
    return reader_lease.write_material(
        root, consumer_action_key=str(fragment["consumer_action_key"]),
        mover_action_key=str(fragment["mover_action_key"]),
        tier_id=str(fragment["tier_id"]), stage_root=str(fragment["stage_root"]),
        manifest_sha256=str(fragment["manifest_sha256"]),
        generation=generation, entries=entries, epoch=material_epoch)


def publish_vouch(root: str | Path, fragment: Mapping[str, object], *,
                  generation: str | None = None,
                  material_entries: Mapping[str, object] | None = None) -> Path:
    """Date new coverage before publishing the fragment that vouches for it."""
    if material_entries:
        _date_vouch(root, fragment, generation, material_entries,
                    material_epoch=fragment.get("epoch"))
    return residency_map.write_fragment(root, fragment)


def reissue_vouch(root: str | Path, source: Mapping[str, object], *,
                  consumer_action_key: str, mover_action_key: str,
                  generation: str, material_entries: Mapping[str, object]) -> Path:
    """Date a shared copy before reissuing its fragment to another namespace."""
    _date_vouch(root, {**source, "consumer_action_key": consumer_action_key,
                      "mover_action_key": mover_action_key}, generation, material_entries,
                material_epoch=source.get("epoch"))
    return residency_map.write_fragment(root, residency_map.reissue(
        source, consumer_action_key=consumer_action_key, mover_action_key=mover_action_key))


@dataclass(frozen=True)
class VouchShrink:
    fragment_committed: bool
    complete: bool
    error: str = ""


def shrink_vouch(root: str | Path, fragment: Mapping[str, object], *,
                 generation: str, material_entries: Mapping[str, object],
                 material_epoch: str | None) -> VouchShrink:
    """Withdraw mentions before trimming their dates, retaining partial results."""
    try:
        residency_map.write_fragment(root, fragment)
    except (OSError, ValueError, pb.PrismaBuildError) as exc:
        return VouchShrink(False, False, f"fragment rewrite: {exc}")
    try:
        _date_vouch(root, fragment, generation, material_entries,
                    material_epoch=material_epoch)
    except (OSError, ValueError, pb.PrismaBuildError) as exc:
        return VouchShrink(True, False, f"material rewrite: {exc}")
    return VouchShrink(True, True)

#: The errnos that mean "this file has no such attribute", as opposed to "this
#: question cannot be answered here".  Linux reports ``ENODATA``; the name
#: ``ENOATTR`` is an alias for it where it exists at all.
_XATTR_ABSENT = frozenset(
    value for value in (getattr(errno, "ENODATA", None),
                        getattr(errno, "ENOATTR", None))
    if value is not None)

#: How ``stage_move`` names the file it is copying into before it verifies the
#: digest and ``os.replace``s it into place: ``.<final name>.partial`` beside
#: the destination, or ``.<final name>.<owner>.partial`` once the mover keys
#: its temporary by its own action key (#620).  One definition, read here,
#: because a temporary left by a killed or withdrawn copy is nobody's and
#: nothing else ever removes it.
PARTIAL_PREFIX = "."
PARTIAL_SUFFIX = ".partial"

#: The event a reconciled eviction publishes, so an operator can tell bytes a
#: mover's own fragment named from bytes nothing named at all.
UNATTRIBUTED_EVENT = "stage-unattributed-evicted"

#: What :func:`reconcile` can find on a file that no attribution names, as its
#: receipt sizes it (#1088).  The first three may be deleted and are reported
#: in ``deleted_by_kind``; the last three are what a pass can leave, reported
#: in ``unowned_left_by_kind``:
#:
#: * ``partial`` -- a mover's own ``.<name>.partial`` temporary;
#: * ``unmarked`` -- a file carrying neither stage mark;
#: * ``mover_residue`` -- a copy a mover renamed into place, carrying
#:   ``stage_marks.STAGE_MOVER_XATTR``, that no fragment names.  It is left
#:   while its mover is still wanted or queued, or while a live promotion
#:   reads it;
#: * ``source_mark_only`` -- a file carrying the prewarm loop's
#:   ``user.pbstage.source`` and no mover mark: a prewarm object, or a mover's
#:   copy published before #1088, which no attribute tells apart.  Always
#:   left;
#: * ``mark_unanswerable`` -- a file whose marks the filesystem cannot report,
#:   or whose mover mark names no action key.  Always left.
RECONCILE_DELETED_KINDS = ("partial", "unmarked", "mover_residue")
RECONCILE_LEFT_KINDS = ("mover_residue", "source_mark_only",
                        "mark_unanswerable")

#: The event a bounded orphan recovery publishes.  Deliberately not
#: ``UNATTRIBUTED_EVENT``: that one reports what routine reconciliation found
#: unowned by walking the stage, and this one reports a named historical range
#: an operator asked about by identity.  Telling them apart is the audit.
ORPHAN_RECOVERY_EVENT = "stage-orphan-recovered"

#: The file a stage root carries to say which queue it belongs to (#628), and
#: the event a sweep publishes when a root does not belong to it.
STAGE_ROOT_MARKER = ".prismabuild-stage.json"
STAGE_ROOT_MARKER_SCHEMA_V1 = "prismabuild.stage-root.v1"
STAGE_ROOT_REFUSED_EVENT = "stage-root-refused"

#: The event a bounded prune of positively stale mentions publishes (#853).
#: Unlike a dead-owner eviction this keeps the whole old holder: no charge
#: moves until the last fragment goes through the ordinary whole-owner egress,
#: which settles it exactly once.  The receipt says so explicitly.
STALE_MENTION_EVENT = "stage-stale-mention-pruned"

#: The process-local, skip-only checkpoint cache (#853, #1056) is bounded by
#: what is on disk, not by literals: at most one checkpoint per owner the
#: dead-owner sweep discovered on its latest pass
#: (:func:`_retain_skip_checkpoints`), and each checkpoint's fences are the
#: parent directories of its own fragment's paths plus the co-owner
#: fragments that name them -- documents the census already holds parsed.
#: A full cache refuses a newcomer, which then takes the uncached scan
#: (slower, never weaker), and never evicts an owner the next pass visits.
#: A checkpoint may only skip the cleanup scan -- never a mutable-state
#: check, a deletion, or an adoption.


def queue_identity(queue: pool.PoolQueue) -> str:
    """The string a marker names a queue by: its root's real path.

    The tier loop, the worker loops and the egress CLI on the storage box all
    spell the queue as ``/mnt/shared/prismabuild-fleet/pb-queue``, and the real
    path is what they agree on even where one of them is handed a symlink.
    """

    return os.path.realpath(str(queue.root))


def read_stage_root_marker(stage_root: str | Path) -> dict[str, object] | None:
    """The marker under ``stage_root``, or ``None`` when there is none.

    Raises ``OSError`` for anything but absence and ``ValueError`` for a file
    that is not the object this module writes; both are "cannot answer", which
    every caller reads as "not mine".
    """

    path = Path(stage_root) / STAGE_ROOT_MARKER
    try:
        with open(path) as stream:
            marker = json.load(stream)
    except FileNotFoundError:
        return None
    if (not isinstance(marker, dict)
            or marker.get("schema") != STAGE_ROOT_MARKER_SCHEMA_V1):
        raise ValueError(f"{path}: not a stage-root marker")
    return marker


def stage_root_refusal(queue: pool.PoolQueue, stage_root: str | Path) -> str | None:
    """Why this queue may not delete under ``stage_root``; ``None`` when it may.

    Owned means a marker is there and names this queue.  Missing, unreadable,
    malformed and another queue's marker all refuse: the failure this guards
    is deleting what is not one's own, so every answer short of "yes, mine" is
    "no".  Nothing here is a policy about *when* a root is claimable --- an
    unregistered root refuses too, and only :func:`register_stage_root` turns
    it into an owned one.
    """

    try:
        marker = read_stage_root_marker(stage_root)
    except OSError as exc:
        return f"stage_root_marker_unreadable: {exc}"
    except ValueError as exc:
        return f"stage_root_marker_invalid: {exc}"
    if marker is None:
        return "stage_root_unregistered"
    owner = str(marker.get("queue_root") or "")
    if owner != queue_identity(queue):
        return f"stage_root_belongs_to_another_queue: {owner}"
    return None


def register_stage_root(queue: pool.PoolQueue, *, tier_id: str,
                        stage_root: str | Path) -> str:
    """Mark ``stage_root`` as this queue's, or say why it could not be.

    Returns ``"registered"`` --- written now, or already naming this queue ---
    or the refusal.  A marker naming another queue is never overwritten: the
    box's loop claiming a root a test's queue registered, or the reverse, is
    the two-owners state this exists to refuse, and the operator removes the
    marker by hand when the queue really has moved.  A root this queue cannot
    write (the Sparks mount the stage read-only) is reported, not raised: the
    tier is still announced, and the sweep refuses it on the same fact.
    Written by temporary and ``os.replace`` so a reader never sees half of it.
    """

    refusal = stage_root_refusal(queue, stage_root)
    if refusal is None:
        return "registered"
    if refusal != "stage_root_unregistered":
        return refusal
    path = Path(stage_root) / STAGE_ROOT_MARKER
    marker = {
        "schema": STAGE_ROOT_MARKER_SCHEMA_V1,
        "queue_root": queue_identity(queue),
        "queue_root_given": str(queue.root),
        "tier_id": str(tier_id),
        "host": socket.gethostname(),
        "unix": time.time(),
    }
    temporary = path.with_name(f".{STAGE_ROOT_MARKER}.{os.getpid()}.tmp")
    try:
        with open(temporary, "w") as stream:
            json.dump(marker, stream, indent=2, sort_keys=True)
            stream.write("\n")
        os.replace(temporary, path)
    except OSError as exc:
        try:
            temporary.unlink()
        except OSError:
            pass
        return f"stage_root_marker_unwritable: {exc}"
    return "registered"


def _refused_receipt(*, tier_id: str, stage_root: str | Path, refusal: str,
                     mover_action_key: str = "", consumer_action_key: str = "",
                     reason: str = "orphan-sweep") -> dict[str, object]:
    """The receipt a refused deletion leaves: nothing deleted, nothing released."""

    return {
        "schema": pool.POOL_EGRESS_SCHEMA_V1,
        "event": STAGE_ROOT_REFUSED_EVENT,
        "action_key": mover_action_key,
        "consumer_action_key": consumer_action_key,
        "tier_id": tier_id,
        "stage_root": str(stage_root),
        "reason": reason,
        "entries_deleted": 0,
        "entries_already_gone": 0,
        "bytes_deleted": 0,
        "tokens_released": 0,
        "skipped": refusal,
        # Not complete: whatever this key's bytes are, they are still where
        # they were, and the tokens that stand for them stay held.
        "complete": False,
        "errors": [refusal],
        "host": socket.gethostname(),
        "unix": time.time(),
    }


def prune_empty(directory: Path, stop: Path) -> None:
    """Remove the directories a deleted range leaves behind, never past the stage."""

    stop = stop.resolve()
    while directory != stop:
        try:
            if directory.resolve() == stop or stop not in directory.resolve().parents:
                return
            directory.rmdir()
        except OSError:
            return
        directory = directory.parent


#: How many co-owners one egress receipt names before it counts the rest.
SHARED_WITH_LIMIT = 5

#: Residency subdirectories that hold records, never fragments.  Pins and
#: retiring marks (``leases/``) and publish-time sidecars (``material/``)
#: live beside the fragments in dedicated subdirectories, and the
#: produced-output template, scope and batch records live under their own
#: exported subdirectories.  Every fragment enumerator skips them: a record
#: parsed as a fragment would taint -- or, before #798, crash -- every egress
#: and sweep on a healthy tier.  The produced *fragments* are a real fragment
#: namespace one level deeper (``OUTPUT_FRAGMENTS_SUBDIR``, reached through
#: :func:`produced_output.output_fragment_root`) and are traversed by
#: :func:`_fragment_census`, never skipped.
RESERVED_RESIDENCY_SUBDIRS = frozenset({
    reader_lease.LEASES_SUBDIR,
    reader_lease.MATERIAL_SUBDIR,
    output_vocabulary.OUTPUT_TEMPLATES_SUBDIR,
    output_vocabulary.OUTPUT_SCOPES_SUBDIR,
    output_vocabulary.OUTPUT_BATCHES_SUBDIR,
})


def _namespace_shaped(name: str) -> bool:
    """A 64-character lowercase-hex directory name: an action key or namespace."""

    return (len(name) == 64
            and all(character in "0123456789abcdef" for character in name))


def locked_parse_record(memo: "kept_reads.CensusMemo | None",
                         before: tuple[int, int] | None) -> dict[str, object]:
    """How many census documents a hold parsed and reused (#988).

    ``locked_parses`` counts every fragment, pin and material sidecar the
    pass under the lock parsed; ``locked_reuses`` the fragments and sidecars
    it reused because their file version had not changed since the census
    before the lock.  ``None`` when the caller kept no memo.
    """

    if memo is None or before is None:
        return {"locked_parses": None, "locked_reuses": None}
    parsed, reused = memo.counts()
    return {"locked_parses": parsed - before[0],
            "locked_reuses": reused - before[1]}


def _read_own_material(root: Path, consumer_action_key: str,
                       mover_action_key: str, memo: "kept_reads.CensusMemo | None"):
    """``reader_lease.read_material``, reused while its file version holds.

    The same three answers: the sidecar, ``None`` when it is absent, or the
    error.  With a ``memo`` the file is still opened, and its ``fstat``
    version decides whether the earlier parse is reused (#988).
    """

    if memo is None:
        return reader_lease.read_material(root, consumer_action_key,
                                          mover_action_key)
    path = reader_lease.material_path(root, consumer_action_key,
                                      mover_action_key)
    key = str(path)
    fence = memo.trust.file_fence()
    try:
        with open(path) as stream:
            info = os.fstat(stream.fileno())
            version = kept_reads.metadata_version(info)
            kept = memo.trust.file_version(info, fence, descriptor=stream.fileno())
            hit = memo.materials.get(key)
            if kept is not None and hit is not None and hit[0] == version:
                memo.reuses += 1
                return hit[1]
            memo.parses += 1
            material = reader_lease.validate_material(json.load(stream))
            # Validate the original fstat while its exact descriptor is open.
            kept = memo.trust.file_version(info, fence, descriptor=stream.fileno())
    except FileNotFoundError:
        memo.materials.pop(key, None)
        return None
    except (OSError, ValueError) as exc:
        memo.materials.pop(key, None)
        return exc
    # Kept only at a version no later file can reproduce (#1045).
    if kept is None:
        memo.materials.pop(key, None)
    else:
        memo.materials[key] = (kept, material)
    return material


def _fragment_stage_paths(document: Mapping[str, object]) -> frozenset[str]:
    """Every ``stage_path`` one validated fragment names, as written."""

    out: set[str] = set()
    for entry in dict(document["entries"]).values():
        if isinstance(entry, Mapping):
            path = entry.get("stage_path")
            if isinstance(path, str):
                out.add(path)
    return frozenset(out)


def _read_fragment(path: Path, memo: kept_reads.CensusMemo | None = None,
                   ) -> dict[str, object] | str:
    """One validated fragment, or the reason it is not one.  Never a skip.

    Skipping is right for a consumer composing its own map
    (``residency_map.read_fragments``) and wrong for every caller that
    deletes: a document that cannot be read is unknown ownership, and
    unknown ownership reported as "no owner" is exactly how staged bytes
    are lost.

    With a ``memo``, a document whose version is unchanged since the memo
    parsed it is returned without a second parse (see :class:`kept_reads.CensusMemo`).
    """

    key = str(path)
    trust = memo.trust if memo is not None else kept_reads.filesystem_trust
    fence = trust.file_fence() if memo is not None else None
    try:
        with open(path) as stream:
            info = None
            kept = None
            if memo is not None:
                info = os.fstat(stream.fileno())
                version = kept_reads.metadata_version(info)
                kept = trust.file_version(info, fence, descriptor=stream.fileno())
                hit = memo.fragments.get(key)
                if kept is not None and hit is not None and hit[0] == version:
                    memo.reuses += 1
                    return hit[1]
                memo.parses += 1
            document = residency_map.validate_fragment(json.load(stream))
            if info is not None:
                kept = trust.file_version(info, fence, descriptor=stream.fileno())
    except (OSError, ValueError) as exc:
        if memo is not None:
            memo.forget(key)
        return str(exc)
    if memo is not None:
        memo.forget(key)
        # Kept only at a version no later file can reproduce (#1045).
        if kept is None:
            return document
        memo.fragments[key] = (kept, document)
        memo.paths[id(document)] = _fragment_stage_paths(document)
        memo.files[id(document)] = key
    return document


def _census_fragment_directory(directory: Path, namespace: str, *,
                               direct: bool,
                               fragments: list[tuple[str, str, dict[str, object], bool]],
                               tainted: list[str],
                               memo: kept_reads.CensusMemo | None = None) -> None:
    """Every valid fragment filed directly under one namespace directory.

    ``direct`` is true only for a namespace that is an immediate child of the
    root the census was asked about; the callers use it to scope
    self-exclusion to that root's own namespace domain.  A symlinked entry is
    taint without being followed: a link can leave the store or point back
    into it, and neither is a fragment.  The same rule covers a ``.json``
    entry that is not a regular file -- a directory, fifo, socket or device
    that has displaced a fragment file.  It hides the only document that
    vouches for the mover's staged bytes, so it is unknown ownership, never a
    skip: a caller that deletes would otherwise read the loss as "no owner".
    """

    index = memo if isinstance(memo, kept_reads.CensusIndex) else None
    with (index.namespace(directory) if index is not None else nullcontext()) as capture:
        if capture is not None and capture.hit:
            for mover, document in capture.value[0]:
                fragments.append((namespace, mover, document, direct))
            return
        found: list[tuple[str, dict[str, object]]] = []
        read: set[str] = set()
        taint_before = len(tainted)
        try:
            entries = sorted(os.scandir(directory), key=lambda entry: entry.name)
        except OSError as exc:
            tainted.append(f"{namespace}: {exc}")
            if index is not None:
                index.remember_namespace(directory, capture, [], set(), True)
            return
        for entry in entries:
            if not entry.name.endswith(".json"):
                continue
            try:
                if entry.is_symlink():
                    tainted.append(
                        f"{namespace}/{entry.name}: symlink is not a fragment")
                    continue
                is_file = entry.is_file()
            except OSError as exc:
                tainted.append(f"{namespace}/{entry.name}: {exc}")
                continue
            if not is_file:
                tainted.append(
                    f"{namespace}/{entry.name}: not a regular file")
                continue
            read.add(entry.path)
            document = _read_fragment(Path(entry.path), memo)
            if isinstance(document, str):
                tainted.append(f"{namespace}/{entry.name}: {document}")
                continue
            if str(document["consumer_action_key"]) != namespace:
                # The directory is what attributes this document; a fragment
                # filed under another namespace is malformed ownership.
                tainted.append(
                    f"{namespace}/{entry.name}: fragment is filed under another namespace")
                continue
            mover = str(document["mover_action_key"])
            if entry.name != f"{mover}.json":
                # The file name is the mover's identity in the directory, and the
                # egress excludes its own document by the mover it carries.
                # A name that disagrees could hide another owner's copy (or
                # impersonate one): unknown ownership, never self.
                tainted.append(
                    f"{namespace}/{entry.name}: fragment names another mover")
                continue
            found.append((mover, document))
            fragments.append((namespace, mover, document, direct))
        if index is not None:
            index.remember_namespace(directory, capture, found, read,
                                     len(tainted) > taint_before)

class _LevelChild:
    """The two ``os.DirEntry`` fields :func:`_census_level` reads, by name."""

    __slots__ = ("name", "path")

    def __init__(self, directory: Path, name: str) -> None:
        self.name = name
        self.path = os.path.join(directory, name)


def _census_level(directory: Path, *, direct: bool, allow_nested: bool,
                  fragments: list[tuple[str, str, dict[str, object], bool]],
                  tainted: list[str],
                  skip: frozenset[str] = frozenset(),
                  memo: kept_reads.CensusMemo | None = None) -> None:
    """Classify one directory level of the fragment store.

    One rule for both layouts: reserved bookkeeping is skipped, a
    ``produced-output-fragments`` child is the nested produced namespace, a
    64-character directory is a fragment namespace, and anything else is
    taint.  The traversal is bounded to the two known layouts: the produced
    container is descended **once**, only from the base store
    (``allow_nested``), and a container name inside the produced store, or a
    symlink anywhere, is unknown ownership -- taint, never a walk.  ``skip``
    names children this call must not revisit -- the produced store itself
    when its own parent level is walked for co-owner domains.

    A non-directory where the level expects a namespace is the other half of
    the same rule: a 64-character name, or the ``produced-output-fragments``
    container, standing as a regular file, fifo or socket hides every
    fragment filed below it, so it is taint -- unknown ownership, never a
    skip.  A composed ``<consumer>.map.json`` beside its namespace directory
    (``residency_map.map_path``) is not such a name -- it is nine
    characters longer than a namespace and is the document a reader reads --
    so it keeps the old behaviour: a stray file that names no layout taints
    nothing.
    """

    index = memo if isinstance(memo, kept_reads.CensusIndex) else None
    with (index.levels.acquire(directory) if index is not None else nullcontext()) as capture:
        children = capture.value if capture is not None and capture.hit else None
        if children is not None:
            index.kept += 1
        else:
            try:
                entries = sorted(os.scandir(directory),
                                 key=lambda entry: entry.name)
            except OSError as exc:
                tainted.append(f"{directory}: {exc}")
                return
            children = []
            for entry in entries:
                try:
                    if entry.is_symlink():
                        kind = "link"
                    elif entry.is_dir():
                        kind = "dir"
                    else:
                        kind = "other"
                except OSError as exc:
                    kind = f"error: {exc}"
                children.append((entry.name, kind))
            if index is not None:
                index.remember_level(directory, capture, children)
    for name, kind in children:
        # One classification of each child, whether this pass listed the
        # level or reused a listing whose stamp still holds (#992).
        entry = _LevelChild(directory, name)
        if entry.name in skip:
            continue
        if kind == "link":
            tainted.append(
                f"{entry.name}: symlink is not a fragment namespace")
            continue
        if kind.startswith("error"):
            tainted.append(f"{entry.name}: {kind[len('error: '):]}")
            continue
        is_directory = kind == "dir"
        if not is_directory:
            if entry.name == output_vocabulary.OUTPUT_FRAGMENTS_SUBDIR:
                tainted.append(
                    f"{entry.name}: produced namespace is not a directory")
                continue
            if _namespace_shaped(entry.name):
                tainted.append(
                    f"{entry.name}: namespace is not a directory")
            continue
        namespace = entry.name
        if namespace in RESERVED_RESIDENCY_SUBDIRS:
            # Reader pins/retiring marks (``leases/``) and material sidecars
            # (``material/``) live inside the produced store too, because
            # reader_lease resolves them from the supplied residency root.
            continue
        if namespace == output_vocabulary.OUTPUT_FRAGMENTS_SUBDIR:
            if not allow_nested:
                tainted.append(
                    f"{namespace}: nested produced namespace is not a "
                    f"fragment namespace")
                continue
            _census_level(Path(entry.path), direct=False, allow_nested=False,
                          fragments=fragments, tainted=tainted, memo=memo)
            continue
        if not _namespace_shaped(namespace):
            tainted.append(f"{namespace}: unknown residency directory")
            continue
        _census_fragment_directory(Path(entry.path), namespace, direct=direct,
                                   fragments=fragments, tainted=tainted,
                                   memo=memo)


def fragment_census(root: Path, memo: kept_reads.CensusMemo | None = None,
                     ) -> tuple[list[tuple[str, str, dict[str, object], bool]],
                                list[str]]:
    """Every fragment under one residency root, and what cannot be read.

    Two layouts share the store.  Legacy flat fragments sit at
    ``<root>/<consumer action key>/<mover>.json``.  Produced-output batches
    file theirs at ``<root>/produced-output-fragments/<batch namespace>/
    <mover>.json``; that namespace is a material namespace, not a queue
    action.

    A walk of the store descends into the produced subdirectory exactly
    once; a walk of the produced store itself -- the root an egress or
    retirement is given for a produced mover -- also walks the flat
    namespaces beside it, because a legacy co-owner may vouch for the same
    physical staged bytes.  Nothing else is a layout: a second container
    name, or a symlink, is taint.  Self-exclusion is the caller's business
    and is scoped to the root walked: only fragments found directly under it
    carry ``direct=True``.

    Returns ``(fragments, tainted)``: each fragment as ``(namespace, mover,
    document, direct)``, each taint a bounded one-line reason.  A directory
    that is neither reserved bookkeeping nor namespace-shaped, a ``.json``
    file that cannot be read or does not validate, a ``.json`` entry that is
    not a regular file, and a namespace-shaped or produced-container name
    that is not a directory, are taint -- never a silent skip, never a crash,
    never followed.  A caller that deletes treats taint as unknown ownership
    and retains.
    """

    fragments: list[tuple[str, str, dict[str, object], bool]] = []
    tainted: list[str] = []
    if root.name == output_vocabulary.OUTPUT_FRAGMENTS_SUBDIR:
        _census_level(root, direct=True, allow_nested=False,
                      fragments=fragments, tainted=tainted, memo=memo)
        _census_level(root.parent, direct=False, allow_nested=False,
                      fragments=fragments, tainted=tainted,
                      skip=frozenset({root.name}), memo=memo)
    else:
        _census_level(root, direct=True, allow_nested=True,
                      fragments=fragments, tainted=tainted, memo=memo)
    return fragments, tainted


def fragment_owners(root: Path, wanted: set[str], *,
                     except_consumer: str = "",
                     except_mover: str = "",
                     memo: kept_reads.CensusMemo | None = None,
                     named_by: list[dict[str, object]] | None = None,
                     same_mover: list[str] | None = None,
                     ) -> tuple[dict[str, set[tuple[str, str]]], list[str]]:
    """Which of ``wanted`` paths are still vouched for, and by whom, in one walk.

    A single scan of every fragment namespace -- never per entry --
    intersecting validated ``stage_path`` strings against ``wanted`` before
    storing.  No metadata walk per foreign entry: the fragment validator
    already guarantees absolute, normalized paths, so string intersection is
    exact and only matches are stored.  The egress keeps its own resolve-based
    containment fence before any unlink.  A fragment that cannot be read or
    validated taints the scan: its paths are unknowable, so nothing may be
    treated as unowned on this pass.  Fail closed, the way an unreadable own
    fragment keeps its tokens.

    The exclusion is scoped to the root being walked: only a fragment filed
    directly under ``root`` -- legacy flat, or the produced store when that
    is the root -- can be this caller's own.  A fragment carrying the same
    key in a nested produced namespace is a *different* owner's copy and
    keeps its protection; "same key" is never automatically self.

    ``named_by``, when given, collects every document that names at least
    one wanted path: the co-owner fragments a verdict relies on (#1056).

    ``same_mover``, when given, is a shared mover's whole-range eviction
    (#1026): every fragment filed directly under the root for
    ``except_mover`` -- its share namespace's and each consumer's copy the
    tier loop fans out -- is the range's own vouch, never a co-owner, and
    each one's namespace is collected here so the eviction can drop it.  A
    shared mover's key is filed under many namespaces by design, which is
    what makes "same key" self here and nowhere else.
    """

    owners: dict[str, set[tuple[str, str]]] = {}
    fragments, tainted = fragment_census(root, memo)
    for namespace, mover, fragment, direct in fragments:
        if direct and namespace == except_consumer and mover == except_mover:
            continue
        if same_mover is not None and direct and mover == except_mover:
            same_mover.append(namespace)
            continue
        named = memo.paths_of(fragment) if memo is not None else None
        if named is not None:
            # The same exact string intersection as below, over the path
            # set the memo built when it parsed this version (#988).
            shared = named & wanted
            for path in shared:
                owners.setdefault(path, set()).add((namespace, mover))
            if shared and named_by is not None:
                named_by.append(fragment)
            continue
        names_one = False
        for entry in dict(fragment["entries"]).values():
            if not isinstance(entry, Mapping):
                continue
            path = entry.get("stage_path")
            # Validated absolute and normalized, so this comparison is
            # exact with no metadata touch.
            if isinstance(path, str) and path in wanted:
                owners.setdefault(path, set()).add((namespace, mover))
                names_one = True
        if names_one and named_by is not None:
            named_by.append(fragment)
    return owners, tainted


_manifest_layout_cache: dict[tuple[str, str], tuple[str, list[dict[str, object]]]] = {}


def manifest_layout_for(cas_root: str, digest: str) -> tuple[str, list[dict[str, object]]] | None:
    """One manifest's mount prefix and entries by content digest, or ``None``.

    Manifests are immutable under their digest.  Only successful loads are
    cached (bounded); a transient miss is re-read next pass rather than
    remembered indefinitely, so a short CAS outage cannot pin every later
    egress into skipping.
    """

    key = (cas_root, digest)
    hit = _manifest_layout_cache.get(key)
    if hit is not None:
        return hit
    try:
        blob = pb.PrismaBuildCAS(Path(cas_root)).blob_path(digest)
        manifest = manifest_codec.load_data_manifest(blob)
    except (OSError, ValueError, pb.PrismaBuildError):
        return None
    if not isinstance(manifest, Mapping):
        return None
    entries = manifest_layout.read_entries(manifest)
    if not isinstance(entries, list):
        return None
    prefix = manifest.get("mount_prefix")
    if not isinstance(prefix, str) or not prefix.startswith("/"):
        return None
    layout = (prefix, [dict(entry) for entry in entries])
    if len(_manifest_layout_cache) >= 4:
        _manifest_layout_cache.clear()
    _manifest_layout_cache[key] = layout
    return layout


def _produced_hold_verified(item: Mapping, request: Mapping) -> bool:
    """Whether a tier-demand claim is a verified producer reservation.

    Shape alone is not verification: the mutable queue projection could
    name any template id and digest. The sealed request carries the
    authoritative declaration (`params.produced_output_template`,
    validated here through the existing core seam against the request's
    own inputs), and it must equal the item's projected ref exactly --
    same template id and digest. A substituted claim ref, or a request
    whose declaration disagrees or is absent, is not a verified hold
    and stays on the taint path. Callers additionally require the
    sealed command to carry no movement range flags (mover/non-mover
    classification unchanged); no separate receipt protocol exists.
    """

    ref = item.get("produced_output")
    if not isinstance(ref, Mapping):
        return False
    if ref.get("schema") != output_vocabulary.PRODUCED_OUTPUT_REF_SCHEMA_V1:
        return False
    params = request.get("params")
    if not isinstance(params, Mapping):
        return False
    declaration = params.get(pb.PRODUCED_OUTPUT_TEMPLATE_PARAM)
    if not isinstance(declaration, Mapping):
        return False
    inputs = request.get("inputs")
    if not isinstance(inputs, list):
        return False
    try:
        checked = pb.validate_produced_output_declaration(declaration, inputs)
    except (pb.ActionContractError, ValueError, TypeError):
        return False
    return (checked.get("template_id") == ref.get("template_id")
            and checked.get("template_sha256") == ref.get("template_sha256"))


def claimed_paths(queue: pool.PoolQueue, tier_id: str,
                   cas_root: str | Path | None = None,
                   *, exclude: set[str] | frozenset[str] | None = None,
                   memo: kept_reads.CensusMemo | None = None,
                   ) -> tuple[set[str], list[str]]:
    """Staged paths a claimed copy may be writing, by sealed range.

    A copy in flight has no fragment yet, so fragments alone cannot attribute
    it -- but its claim already exists, and the claim's sealed request names
    its manifest and its read-order range.  Resolving those through the same
    ``stage_relative`` computation both movers use attributes exactly the
    files the copy can rename into place.  Non-movement claims are skipped,
    never tainting: a claim with no demand on the tier, or one that demands
    only a rate there (the paced produced export's pool fill, #1060), is not
    a copy.  A claim that demands occupancy on the tier is a copy, and one
    whose sealed command carries no range taints unless it is a verified
    producer reservation.  Anything unreadable taints the pass, the same
    fail-closed rule as fragments.

    The CAS root comes from each sealed claim record's own ``cas_root`` where
    present (an explicit override wins for tests); the queue-sibling default
    applies only when no record names one.

    ``exclude`` names claim keys that never count as another publisher --
    the staged-path publication gate passes its own mover key, so a mover
    never defers to itself.

    The egress needs one more distinction -- the mover being evicted's own
    claim against a foreign one -- and reads
    :func:`_claimed_paths_attributed`; this wrapper is the two-value view the
    publication gate and the focused tests already write against.
    """

    paths, tainted, _own = claimed_paths_attributed(
        queue, tier_id, cas_root, exclude=exclude, memo=memo)
    return paths, tainted


def claimed_paths_attributed(queue: pool.PoolQueue, tier_id: str,
                              cas_root: str | Path | None = None, *,
                              exclude: set[str] | frozenset[str] | None = None,
                              own_key: str = "",
                              memo: kept_reads.CensusMemo | None = None,
                              ) -> tuple[set[str], list[str], set[str]]:
    """:func:`_claimed_paths`, attributing one key's paths separately.

    Everything above holds.  ``own_key`` is the key an egress is retiring: its
    claim's derived paths come back as the third element instead of joining
    the competing set, because the evicted mover's own claim is never a
    distinct co-owner (#793).  A live own claim is still a *possible writer*,
    though: it defers the retire rather than sharing it, and the claim stops
    being visible at all once the worker's terminal transition retires it.
    There is deliberately no receipt-based shortcut here.  A move receipt
    carries no immutable attempt identity -- no nonce or scope -- so a
    complete-looking one cannot be told from a previous attempt's while the
    same key is claimed again, and wall-clock stamps are no substitute
    (2026-09-21 root QA).  ``exclude`` keeps its unconditional meaning and is
    checked first.

    With a ``memo``, a range mover's derived stage paths are remembered by
    claim key and CAS root: they come from its sealed request and its data
    manifest, both immutable under their digests.  The claim listing and
    each claim record are still read on every call (#988).
    """

    paths: set[str] = set()
    own_paths: set[str] = set()
    tainted: list[str] = []
    try:
        keys = sorted(path.name[:-len(".json")] if path.name.endswith(".json")
                      else path.name
                      for path in pb.immutable_files.scan_optional(
                          queue.records.dir(pool.record_store.CLAIMED),
                      ))
    except OSError as exc:
        return paths, [f"claimed: {exc}"], own_paths
    records: list[tuple[str, dict[str, object]]] = []
    for key in keys:
        try:
            item = pool.record_store.value_or_raise(
                queue.records.item(pool.record_store.CLAIMED, key),
            )
        except (OSError, pool.PoolContractError) as exc:
            tainted.append(f"{key[:12]}: {exc}")
            continue
        if item is None:
            continue    # finished between the scan and the read: its
                        # fragment, if it published one, still vouches for it
        if not isinstance(item, dict):
            tainted.append(f"{key[:12]}: unreadable claim record")
            continue
        records.append((key, item))
    default_cas = (str(cas_root) if cas_root is not None
                   else str(queue.root.parent / "cas"))
    for key, item in records:
        if exclude and key in exclude:
            continue    # this publisher's own claim: never another publisher
        own_claim = bool(own_key) and key == own_key
        # No first-record inheritance: each claim resolves its own CAS root --
        # the explicit override wins, else the record's own root, else the
        # queue-sibling default.  A rootless claim among rooted claims reads
        # the default, never another record's root.
        if cas_root is not None:
            own_cas = str(cas_root)
        else:
            root = item.get("cas_root")
            own_cas = (str(root) if isinstance(root, str) and root
                       else default_cas)
        resources = item.get("resources")
        if not isinstance(resources, Mapping):
            # Absent or malformed: a claim without a demand shape cannot
            # establish non-mover.  Every published row seals ``resources``,
            # and no queue transition writes a claim without one, so there is
            # no safe behavior but taint.  (The old code threw mid-scan on
            # ``None`` and skipped the unknown silently.)
            tainted.append(f"{key[:12]}: malformed resources")
            continue
        on_tier = {str(kind).split("@", 1)[0] for kind in resources
                   if "@" in str(kind) and str(kind).split("@", 1)[1] == tier_id}
        if not on_tier:
            continue    # not a movement node on this tier; a consumer is
                        # not a copy
        if on_tier <= tier_holdings.RATE_KINDS:
            # A rate reservation names no bytes (#636, #1060): the paced
            # produced export reserves this tier's pool-side fill and
            # writes under its template's output prefix, never onto the
            # stage.  The pool's publish gate draws the same line -- only
            # occupancy kinds need a range or a working window -- and
            # every copy onto a tier carries the tier's capacity kind
            # (``storage_tiers.residency_demand``, the produced-output
            # mover).  An occupancy or unknown kind still reads as a copy
            # below, so a mover that seals no range still taints.
            continue
        memo_key = (key, own_cas, tier_id)
        remembered = memo.claims.get(memo_key) if memo is not None else None
        if remembered is not None:
            (own_paths if own_claim else paths).update(remembered)
            continue
        try:
            request = pool.record_store.read_document(
                Path(own_cas) / "requests" / key[:2] / f"{key}.json")
        except (OSError, pool.PoolContractError) as exc:
            tainted.append(f"{key[:12]}: {exc}")
            continue
        if not isinstance(request, Mapping):
            tainted.append(f"{key[:12]}: unreadable sealed request")
            continue
        params = request.get("params")
        command = params.get("command") if isinstance(params, Mapping) else None
        if not isinstance(command, list):
            # Identified as a mover by its tier demand, but seals no argv:
            # corrupt, not a consumer -- consumers never reach this branch.
            tainted.append(f"{key[:12]}: mover seals no command")
            continue
        if "--range-start-bytes" not in command:
            # A verified produced-output producer reservation, not a copy:
            # the item's projected ref must equal the sealed request's
            # own validated declaration (template id + digest bound to
            # its inputs). A substituted ref, or a request whose
            # declaration disagrees or is absent, stays unknown and
            # taints below. Unknown rows are never skipped, and an
            # active copy's protection is untouched.
            if _produced_hold_verified(item, request):
                continue
            tainted.append(f"{key[:12]}: mover seals no range")
            continue
        try:
            start = command[command.index("--range-start-bytes") + 1]
            end = command[command.index("--range-end-bytes") + 1]
            # Sealed argv bounds are digit strings by schema: no bool, float,
            # or whitespace-tolerant coercion may accept a malformed bound.
            for bound in (start, end):
                if (isinstance(bound, bool) or not isinstance(bound, str)
                        or not bound.isdigit()):
                    raise ValueError(
                        "range bounds must be nonnegative integer strings")
            start, end = int(start), int(end)
            if end < start:
                raise ValueError("range end precedes start")
        except (ValueError, IndexError, TypeError):
            # An identified mover whose exact range cannot be determined must
            # not silently read as unowned.
            tainted.append(f"{key[:12]}: mover seals an invalid range")
            continue
        digest = None
        # The manifest rides on the sealed request's top-level inputs (verified
        # against a live fixture), never under params.
        inputs = request.get("inputs")
        if isinstance(inputs, list):
            for entry in inputs:
                if (isinstance(entry, Mapping)
                        and entry.get("id") == manifest_codec.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID):
                    digest = entry.get("sha256")
        if not isinstance(digest, str) or not digest:
            tainted.append(f"{key[:12]}: sealed request names no data manifest")
            continue
        layout = manifest_layout_for(own_cas, digest)
        if layout is None:
            tainted.append(f"{key[:12]}: manifest {digest[:12]} unreadable")
            continue
        mount_prefix, entries = layout
        named_once = paths_named_once(entries)
        try:
            window = manifest_layout.entries_between(entries, start, end)
        except (ValueError, TypeError) as exc:
            # A window that cannot be cut is an undeterminable range: taint,
            # never unowned.
            tainted.append(f"{key[:12]}: range not cuttable: {exc}")
            continue
        derived: set[str] = set()
        for entry in window:
            path, offset = str(entry["path"]), int(entry["offset"])
            try:
                # Both spellings: a claimed mover may be a frozen plan row
                # of a generation from before range-only naming, still
                # writing the bare name.  A retention census over-retains.
                relatives = {
                    stage_relative(path, offset, int(entry["bytes"]),
                                   mount_prefix=mount_prefix),
                    pre_range_stage_relative(
                        path, offset, int(entry["bytes"]),
                        mount_prefix=mount_prefix,
                        named_once=path in named_once)}
            except ValueError:
                continue
            # Compared against fragment ``stage_path`` values, which join the
            # stage root with this same relative name.
            derived.update(relatives)
        (own_paths if own_claim else paths).update(derived)
        if memo is not None:
            memo.claims[memo_key] = frozenset(derived)
    return paths, tainted, own_paths


def withdraw_vouch(queue: pool.PoolQueue | None, root: Path, namespace: str,
                mover_action_key: str) -> None:
    """Remove one namespace's fragment, material and retiring mark for a mover.

    The documents only, never a file: what they vouch for is someone else's
    to delete.  Fragment first, because it is what composes a map and names
    a path to the dead-owner sweep; a material left by an interruption dates
    nothing a fragment names. A retiring mark goes after the material.
    ``queue=None`` discards an overrun mover's unpublished pair, preserving
    its strict material-error behavior and touching no retiring mark.
    """

    residency_map.fragment_path(root, namespace,
                                mover_action_key).unlink(missing_ok=True)
    try:
        reader_lease.material_path(root, namespace,
                                   mover_action_key).unlink(missing_ok=True)
    except OSError:
        if queue is None:
            raise
    if queue is not None:
        reader_lease.clear_retiring(
            reader_lease.leases_root(queue, root),
            consumer_action_key=namespace, mover_action_key=mover_action_key)


def shared_interest(
    queue: pool.PoolQueue,
    mover_action_key: str,
    *,
    exclude: frozenset[str] | set[str] = frozenset(),
    store=None,
) -> dict[str, list[str]]:
    """The live consumers that will still read a shared range (#1026).

    A consumer is interested while it is ready or claimed, its frozen plan
    names the mover, and its accepted progress has not passed the leg's
    phase.  Derived from the queue, the plan and the lease every time rather
    than filed by the consumer: a filed marker would outlive a consumer
    that died without dropping it and need a reaper of its own, and a
    consumer requeued from its first phase would have to file it again.
    Read this way, a restart regains its interest by construction.

    ``unknown`` names the consumers whose plan or progress did not read.
    The caller counts them as interested: deleting a range on a question
    nobody could answer is the direction that cannot be taken back.
    """
    store = queue.records if store is None else store

    interested: list[str] = []
    unknown: list[str] = []
    for state in (pool.record_store.READY, pool.record_store.CLAIMED):
        for path, item in store.state_records(state):
            if not isinstance(item, dict):
                continue
            # The row's block, or its filed plan's (#1332): a planner-filed
            # consumer is as interested in a shared range as a sealed one.
            residency, _source = residency_plan.consumer_residency(queue, item)
            if residency is None:
                continue
            key = (path.name[:-len(".json")] if path.name.endswith(".json")
                   else path.name)
            if key in exclude:
                continue
            refused: list[Exception] = []
            plan = residency_plan.read(queue, key, on_unreadable=refused.append)
            if plan is None:
                if refused:
                    unknown.append(key)
                continue
            leg = residency_plan.find_mover_leg(plan, mover_action_key)
            if leg is None or leg.get("mover_role") != "mover_row":
                continue
            accepted = None
            if state == pool.record_store.CLAIMED:
                claimed_unix = item.get("claimed_unix")
                if (isinstance(claimed_unix, bool)
                        or not isinstance(claimed_unix, (int, float))):
                    unknown.append(key)
                    continue
                observation = queue.records.accepted_progress_phase(key, float(claimed_unix))
                if observation is not None:
                    accepted = residency_plan.accepted_plan_phase(
                        queue, item, str(observation["phase"]))
            ahead = {str(phase["name"]) for phase in
                     residency_plan.remaining(plan, accepted)}
            if str(leg["phase"]) in ahead:
                interested.append(key)
    return {"interested": sorted(set(interested)),
            "unknown": sorted(set(unknown))}


def _range_live_or_landed(queue: pool.PoolQueue, mover_action_key: str) -> bool:
    """Whether a mover is queued, running, or filed a complete receipt."""

    if (pool.record_store.value_or_raise(
        queue.records.item_presence(pool.record_store.READY, mover_action_key),
    )
            or pool.record_store.value_or_raise(
                queue.records.item_presence(pool.record_store.CLAIMED, mover_action_key),
            )):
        return True
    receipt = queue.move_record(mover_action_key)
    return isinstance(receipt, Mapping) and receipt.get("complete") is True


class EgressReporter:
    """Report this egress's released bytes to the worker's stall check (#1021).

    An egress sealed with a progress policy (``movement_actions.
    egress_progress_policy``) is ended by the worker's ``no_progress`` rung
    when it reports nothing for its grace.  This is the other half.  The
    egress enters its phases in order -- ``snapshot`` at launch, ``drain``
    when the unlinks start, ``release`` when they are done -- and each entry
    is committed at once, because a new phase is advancement.  Inside the
    drain every entry whose unlink returned, or found the file already gone,
    adds its bytes, and a thread of its own commits the count every
    ``interval_s`` when it grew.  An entry counts once its unlink has
    returned, which is the durable unit the contract asks for, and the
    unlink loop does no extra work for it beyond one addition under a lock.

    The ``unit`` names the mover whose range is being released, so the
    ending record of a wedged egress names the bytes released and the range
    they belong to.  Without a progress channel -- an egress sealed with no
    policy, or a direct run -- nothing starts and nothing is written.  A
    report that fails is counted and never fails the egress: the worker then
    reads it as quiet, which is the honest verdict.
    """

    def __init__(self, *, action_key: str, mover_action_key: str,
                 interval_s: float) -> None:
        self.action_key = str(action_key)
        self.interval_s = max(0.001, float(interval_s))
        self.unit = (f"bytes released of mover {str(mover_action_key)[:12]}'s "
                     f"staged range")
        self.channel = pb_progress.channel() is not None
        self.phase = movement_actions.EGRESS_SNAPSHOT_PHASE
        self.units = 0
        self.reported_units = 0
        self.reported_phase: str | None = None
        self.reports = 0
        self.unwritten = 0
        self.last_reported_unix: float | None = None
        self.refusal: str | None = None
        #: Orders the commits.  The count has its own lock, so an unlink
        #: never waits on a report being written.
        self._lock = threading.Lock()
        self._count_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def report(self) -> None:
        """Commit the count now if it grew or the phase moved on."""

        if not self.channel or self.refusal is not None:
            return
        with self._lock:
            with self._count_lock:
                units, phase = self.units, self.phase
            if units <= self.reported_units and phase == self.reported_phase:
                return
            if (units == 0 and self.reported_phase is None
                    and phase == movement_actions.EGRESS_SNAPSHOT_PHASE):
                # The first phase is granted at launch; a zero count earns
                # nothing and would only be rejected as a replay.
                return
            try:
                written = pb_progress.commit(units, phase, unit=self.unit)
            except ValueError as exc:
                # A phase this launch did not declare: the policy and this
                # tool disagree, and every later report would be refused too.
                self.refusal = str(exc)
                return
            if not written:
                self.unwritten += 1
                return
            self.reports += 1
            self.reported_units = max(self.reported_units, units)
            self.reported_phase = phase
            self.last_reported_unix = time.time()

    def enter(self, phase: str) -> None:
        """Enter ``phase`` and commit it now."""

        with self._count_lock:
            self.phase = phase
        self.report()

    def released(self, nbytes: int) -> None:
        """One entry's unlink returned; its bytes count from here on."""

        with self._count_lock:
            self.units += int(nbytes)

    def start(self) -> None:
        if not self.channel or self._thread is not None:
            return

        def loop() -> None:
            while not self._stop.wait(self.interval_s):
                self.report()

        self._thread = threading.Thread(target=loop, daemon=True,
                                        name="stage-release-progress")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        self.report()

    def record(self) -> dict[str, object]:
        """What the receipt says about the reports this egress made."""

        return {"channel": self.channel, "interval_s": self.interval_s,
                "reports": self.reports, "units_reported": self.reported_units,
                "units_released": self.units,
                "phase": self.reported_phase, "unit": self.unit,
                "last_reported_unix": self.last_reported_unix,
                "unwritten": self.unwritten, "refusal": self.refusal}


#: The egress this process runs as an action, when it runs as one: ``main``
#: sets it around its one eviction, so the unlink loop can report through it
#: and the ownership hold can name it.  ``None`` for a sweep, a reconcile or
#: a tier cycle calling the same code in process.
egress_run: contextvars.ContextVar["_EgressReporter | None"] = (
    contextvars.ContextVar("stage_release_egress_run", default=None))


@contextmanager
def ownership(queue: pool.PoolQueue, stage: str | Path, *, role: str):
    """Hold ``stage``'s ownership lock, named as its holder (#1021).

    Every hold this tool takes files the holder's record once the lock is
    granted and removes it before letting go
    (``PoolQueue.write_stage_ownership_holder``), so a worker whose mover
    waits at its start gate can say which egress it waited on, and the
    worker running this egress can tell the egress's own hold from a wait.
    The record names this process's action when it runs as one -- the
    egress ``main`` runs, or any action whose launcher set
    ``core.ACTION_KEY_ENV`` -- and ``role`` names the pass.

    Yields the ``time.perf_counter()`` of the grant, so a caller's
    ``lock_held_s`` counts the record's own write and removal as part of
    the hold they are.

    An action under a progress contract that cannot file its record lets
    the lock go at once and raises: its own worker could not tell its hold
    from a wait and would credit it for as long as the hold lasted.
    Anywhere else a record that will not write is a gap in a diagnostic,
    and the hold goes ahead without it.
    """

    run = egress_run.get()
    action_key = (run.action_key if run is not None
                  else os.environ.get(pb.ACTION_KEY_ENV) or None)
    # The one holder-record hold, shared with ``stage_move``'s (#1110).
    with queue.recorded_stage_ownership(
            stage, role=role, action_key=action_key,
            require_record=pb_progress.channel() is not None) as granted:
        yield granted


def _evict_shared_locked(queue: pool.PoolQueue, mover_action_key: str, *,
                         namespace: str, consumer_action_key: str,
                         stage_root: str, residency_root: str | Path | None,
                         reason: str, whole: bool,
                         range_wide: bool, store=None) -> dict[str, object]:
    """:func:`evict` of a shared range, with the mover's transition lock held.

    An egress drops one consumer's interest (#1026).  While another live
    consumer will still read the range, it removes only the egressing
    consumer's own copy of the vouch -- the fragment and material the tier
    loop fanned out to it -- and the file and the tokens stay, both under
    the one mover every sharer names.  The last interest to go evicts the
    range: the ordinary eviction, run as the share namespace, with every
    fanned copy of this mover read as the range's own vouch rather than as a
    co-owner (a co-owner keeps its file and *decharges* the tokens, which
    for one holder would free room the bytes still occupy).

    ``range_wide`` is an eviction for every consumer at once: the claim
    order's and the refill horizon's relief, whose candidates were already
    checked against every sharer's horizon.  An eviction named for the share
    namespace itself -- the orphan sweep, reading the mover's receipt -- is
    range-wide by construction.  Neither drops an interest; both take the
    whole range or, with ``whole``, decline it whole.

    So is an eviction of a range that did not land: a mover neither queued,
    running nor complete.  Its partials are nobody's resident range, and the
    failed-mover reclaim (#627) publishes this egress to free them for the
    recopy.  Dropping one interest would leave them for as long as any
    sharer's plan names the range, which is exactly as long as the recopy
    needs the room.
    """
    store = queue.records if store is None else store

    root = Path(residency_root if residency_root is not None
                else queue.root / pool.record_store.RESIDENCY)
    if (consumer_action_key != namespace and not range_wide
            and _range_live_or_landed(queue, mover_action_key)):
        try:
            others = shared_interest(queue, mover_action_key,
                                     exclude={consumer_action_key}, store=store)
        except (OSError, pool.PoolContractError, ValueError) as exc:
            others = {"interested": [],
                      "unknown": [f"interest census unreadable: {exc!r}"]}
        remaining = sorted(set(others["interested"]) | set(others["unknown"]))
        if remaining:
            refusal = stage_root_refusal(queue, stage_root)
            if refusal is not None:
                return _refused_receipt(
                    tier_id="", stage_root=stage_root, refusal=refusal,
                    mover_action_key=mover_action_key,
                    consumer_action_key=consumer_action_key, reason=reason)
            withdraw_vouch(queue, root, consumer_action_key, mover_action_key)
            return {
                "schema": pool.POOL_EGRESS_SCHEMA_V1,
                "action_key": mover_action_key,
                "consumer_action_key": consumer_action_key,
                "stage_root": str(stage_root),
                "reason": reason,
                "share_namespace": namespace,
                "interest_dropped": True,
                "interest_remaining": remaining[:SHARED_WITH_LIMIT],
                "interest_remaining_count": len(remaining),
                "entries_deleted": 0, "entries_already_gone": 0,
                "entries_shared": 0, "shared_with": [],
                "entries_deferred": 0, "live_pins": [],
                "deferred_handoffs": [], "deferred_own": [],
                "auto_reclaimed": [], "auto_retained": {},
                "retiring": False, "bytes_deleted": 0, "bytes_shared": 0,
                "tokens_released": 0, "tokens_decharged": 0,
                "complete": True, "errors": [],
                "host": socket.gethostname(), "unix": time.time(),
                **({"declined": []} if whole else {}),
            }
    receipt = _evict_locked(queue, mover_action_key,
                            consumer_action_key=namespace,
                            stage_root=stage_root,
                            residency_root=residency_root, reason=reason,
                            whole=whole, shared=True)
    receipt["share_namespace"] = namespace
    receipt["egress_consumer_action_key"] = consumer_action_key
    return receipt


def evict(queue: pool.PoolQueue, mover_action_key: str, *,
          consumer_action_key: str, stage_root: str,
          residency_root: str | Path | None = None,
          reason: str = "egress", whole: bool = False,
          range_wide: bool = False, store=None) -> dict[str, object]:
    """Delete one mover's staged files and settle its tier tokens.

    A shared range's mover (#1026, :func:`residency_plan.read_shared_mover`)
    is evicted per interest instead: :func:`_evict_shared_locked`.  Its
    index is read under the mover's transition lock, so the answer cannot
    change between the read and the act; one that does not read keeps the
    files, the fragment and the tokens, as an unreadable fragment does.
    ``range_wide`` only matters for such a range.

    ``whole`` makes the eviction all or nothing (#903).  An egress of a passed
    phase may delete part of a range and defer the rest behind a reader,
    because nobody will read the part it deleted.  A range a live consumer
    has not reached yet is different: it will be read, so deleting part of
    it while its tokens and fragment stay would leave it looking staged with
    holes in it, and nothing would copy it again.  With ``whole`` every entry
    is judged under the ownership lock before anything is unlinked, and if
    any entry would defer -- a reader's pin, a promotion's handoff, the
    mover's own live copy -- or cannot be judged, the eviction declines:
    nothing is unlinked, no retiring mark is filed, the tokens and the
    fragment stay, and the receipt names the reason in ``declined``.

    Tokens for deleted bytes return; tokens for bytes staying under a
    co-owner are decharged (#733).

    Idempotent in both halves: a file already gone is counted as gone rather
    than raised on, and ``ResourceLedger.release`` is documented safe to call
    twice.  A second egress of the same range is therefore a no-op receipt, not
    a failure --- which matters, because the tier loop may publish one while a
    sweep is doing the same work.

    Containment reclamation happens between the two locks (#780): after the
    transition lock, which makes the fragment this reads stable, and before
    the ownership lock, because reclamation takes the ownership lock of every
    root its pins name and a second root requested under the first is a
    cycle.  The egress can wait on another root there, but it waits holding
    only this mover's own transition lock, which is strictly less than the
    root it used to hold while waiting.

    Held under the mover's transition lock since #598, because a second party
    can now decide the same range's ownership: an adoption hands these tokens
    to a successor's mover and re-issues the fragment under it.  Read-delete-
    release and transfer-then-drop-the-fragment are each safe alone, and
    interleaved either way one of them acts on half the other's decision --- so
    they exclude each other rather than being ordered.  After the lock this
    sees one of two settled states: the fragment is here and the tokens are
    this key's, or the fragment is gone and so are the tokens, which is the
    no-op above.
    """
    store = queue.records if store is None else store

    with queue.mover_transition_lock(str(mover_action_key)):
        try:
            namespace = residency_plan.share_namespace_of(
                queue, str(mover_action_key))
        except residency_plan.ResidencyPlanError as exc:
            receipt = _refused_receipt(
                tier_id="", stage_root=stage_root,
                refusal=f"shared_range_unreadable: {exc}",
                mover_action_key=mover_action_key,
                consumer_action_key=consumer_action_key, reason=reason)
            receipt["event"] = "shared-range-unreadable"
            return receipt
        if namespace is not None:
            return _evict_shared_locked(
                queue, mover_action_key, namespace=namespace,
                consumer_action_key=consumer_action_key,
                stage_root=stage_root, residency_root=residency_root,
                reason=reason, whole=whole, range_wide=range_wide, store=store)
        return _evict_locked(queue, mover_action_key,
                             consumer_action_key=consumer_action_key,
                             stage_root=stage_root,
                             residency_root=residency_root, reason=reason,
                             whole=whole)


def _evict_locked(queue: pool.PoolQueue, mover_action_key: str, *,
                  consumer_action_key: str, stage_root: str,
                  residency_root: str | Path | None = None,
                  reason: str = "egress", whole: bool = False,
                  shared: bool = False) -> dict[str, object]:
    """:func:`evict`'s body, with the mover's transition lock already held.

    One staged file can have two owners: forward and reverse passes stage the
    same source extent through different movers onto one content-addressed
    name, and two read phases of one v2 plan do the same inside one consumer.
    Deleting on one owner's egress while another owner's fragment still
    vouches for the file leaves a hole behind a live map, so a path another
    live fragment -- or a claimed copy with no fragment yet -- still names is
    kept, and this mover's tokens for those bytes are decharged rather than
    freed, while its own fragment is still dropped (#733).  The last owner
    to leave deletes the file.  The whole check-and-act runs under
    the stage root's ownership lock (taken here, inside the transition lock --
    adoption takes them in the same order), so two concurrent egresses order
    instead of both concluding "unshared", and the tier mint lock is taken
    last, as a leaf, around the settle alone.  Exactly one stage root is held
    at a time: anything that would take a second one -- containment
    reclamation is the only such thing here -- runs before this lock, never
    inside it (#780).
    """

    refusal = stage_root_refusal(queue, stage_root)
    if refusal is not None:
        # Not this queue's stage (#628).  The fragment is not even read: a
        # fragment is authority over which files are this mover's, never over
        # whose stage they sit on.
        return _refused_receipt(tier_id="", stage_root=stage_root,
                                refusal=refusal,
                                mover_action_key=mover_action_key,
                                consumer_action_key=consumer_action_key,
                                reason=reason)
    root = Path(residency_root if residency_root is not None
                else queue.root / pool.record_store.RESIDENCY)
    fragment_path = residency_map.fragment_path(
        root, consumer_action_key, mover_action_key)
    entries: dict[str, object] = {}
    tier_id: str | None = None
    errors: list[str] = []
    try:
        with open(fragment_path) as stream:
            checked = residency_map.validate_fragment(json.load(stream))
            entries = dict(checked["entries"])
            tier_value = checked.get("tier_id")
            tier_id = str(tier_value) if isinstance(tier_value, str) else None
    except FileNotFoundError:
        # No fragment at all.  Either the mover never published one -- in which
        # case it staged nothing -- or an earlier egress already removed it.
        # Both mean nothing of this mover's is on the stage, so the tokens come
        # back; holding them would cost the tier its capacity for good.
        entries = {}
    except (OSError, ValueError) as exc:
        # A fragment that exists and cannot be read is the opposite case: its
        # bytes may well still be there and this egress cannot name them.
        # Releasing on that would let the ledger admit a mover onto capacity
        # that is occupied, so the tokens stay and the next sweep retries.
        errors.append(f"{fragment_path.name}: {exc}")
    stage = Path(stage_root)
    auto_reclaimed: list[str] = []
    auto_retained: dict[str, str] = {}
    memo = kept_reads.CensusMemo()
    fences: dict[str, tuple] | None = None
    census_s = 0.0
    if entries and tier_id is not None:
        # Containment reclamation runs here, holding no stage ownership lock
        # (#780).  ``auto_reclaim`` walks every pin owner and ``release_refs``
        # takes the root each pin names -- root B for a pin filed on another
        # stage -- so called from inside this root's exclusion it is an A->B
        # request, while a second egress holding B walking the same lease tree
        # asks for A.  ``posix_lock.held`` nests on the same path only, so
        # same-root reentrancy does not prevent that cycle.
        #
        # It needs no exclusion of ours to be correct: each release takes its
        # own pin's root lock around its own check-and-act, and the delete
        # decision below is still one atomic unit, because the census it acts
        # on is taken under the lock -- after this, never before it.
        #
        # This census only decides whether to reclaim at all: the same
        # question the in-lock pass asked, asked without the lock.  It is a
        # hint and is allowed to be stale in both directions.  A ref whose
        # containment evidence lands between it and the lock is not freed on
        # this pass; the entry defers and the next sweep retries, which is the
        # direction this node already fails in.
        census_started = time.perf_counter()
        hint, hint_tainted = reader_lease.live_for(
            queue, _wanted_stage_paths(entries), residency_root=root,
            memo=memo.pins)
        if hint or hint_tainted:
            reclaimed = reader_lease.auto_reclaim(queue, residency_root=root)
            auto_reclaimed.extend(reclaimed["released"])
            for ref_id, why in reclaimed["retained"].items():
                auto_retained.setdefault(ref_id, why)
        # The rest of the census, and each entry's containment fence, are
        # hints too (#988).  They run here, without the lock, so the pass
        # under it re-reads only what changed (see :class:`kept_reads.CensusMemo`)
        # and resolves no path.  Nothing decided here is acted on: every
        # verdict is judged again under the lock against the census taken
        # there.
        ownership_census(queue, mover_action_key,
                          consumer_action_key=consumer_action_key,
                          stage=stage, tier_id=tier_id, root=root,
                          entries=entries, memo=memo, shared=shared)
        _read_own_material(root, consumer_action_key, mover_action_key, memo)
        fences = _entry_fences(stage, entries)
        census_s = time.perf_counter() - census_started
    parents: set[Path] = set()
    asked = time.perf_counter()
    with ownership(queue, stage, role=reason) as granted:
        receipt = _evict_owned(queue, mover_action_key,
                               consumer_action_key=consumer_action_key,
                               stage=stage, tier_id=tier_id, root=root,
                               fragment_path=fragment_path, entries=entries,
                               errors=errors, reason=reason,
                               auto_reclaimed=auto_reclaimed,
                               auto_retained=auto_retained, whole=whole,
                               memo=memo, fences=fences, prune_after=parents,
                               shared_range=shared)
    released = time.perf_counter()
    # Empty directories go after the lock.  A publisher's rename lands in a
    # directory that already holds its temporary, so ``rmdir`` cannot take
    # it from under the rename; its ``mkdir`` runs outside the lock, before
    # its temporary exists, so this prune can still remove that directory in
    # the gap.  ``stage_move._copy_one`` retries the ``mkdir`` once, on
    # ``FileNotFoundError``, when that race costs it its temporary's creation
    # (#1008 item 2) -- the same single retry ``residency_map._write_atomic``
    # already takes for its own directory.
    pruned_started = time.perf_counter()
    for parent in sorted(parents, key=lambda one: len(one.parts), reverse=True):
        prune_empty(parent, stage)
    receipt.update(hold_record(
        lock_wait_s=granted - asked, lock_held_s=released - granted,
        census_s=census_s, prune_s=time.perf_counter() - pruned_started))
    return receipt


#: The lock-scope fields an egress receipt carries since #988, in the order
#: an event copies them: the queue for the lock, the hold, the entries the
#: hold judged, the census before the lock, the census under it, the
#: unlinks under it and the prune after it, and the census documents the
#: hold parsed and reused.
LOCK_SCOPE_FIELDS = ("lock_wait_s", "lock_held_s", "entries_judged",
                     "census_s", "census_validate_s", "unlink_s", "prune_s",
                     "locked_parses", "locked_reuses")


def lock_scope(receipt: Mapping[str, object]) -> dict[str, object]:
    """The :data:`LOCK_SCOPE_FIELDS` one egress receipt carries, for an event."""

    return {field: receipt[field] for field in LOCK_SCOPE_FIELDS
            if field in receipt}


def hold_record(*, lock_wait_s: float, lock_held_s: float,
                 census_s: float, prune_s: float = 0.0) -> dict[str, object]:
    """The lock-scope fields an egress receipt carries (#988).

    ``lock_held_s`` is the one stage ownership hold this call took, from the
    grant to the release; ``lock_wait_s`` is the time it queued for it.
    ``census_s`` is the census taken before the lock, as a hint, and
    ``prune_s`` the empty-directory prune after it.  The census under the
    lock (``census_validate_s``), the unlinks (``unlink_s``) and
    ``entries_judged`` come from the pass under the lock itself.
    """

    return {"lock_wait_s": round(lock_wait_s, 6),
            "lock_held_s": round(lock_held_s, 6),
            "census_s": round(census_s, 6),
            "prune_s": round(prune_s, 6)}


def _entry_fences(stage: Path, entries: Mapping[str, object],
                  ) -> dict[str, tuple]:
    """Each entry's containment fence, computed once, outside the lock (#988).

    ``("ok", path, norm, resolved, relative)``: the entry's path, its
    normalized spelling, its resolved spelling and its name relative to the
    resolved stage root.  ``("error", message)`` for a path that resolves
    outside the stage or cannot be resolved.

    The fence is the same ``resolve`` the judge made under the lock before
    #988, taken earlier.  The stage tree's directories are made by movers
    (``mkdir``) and removed by egresses (``rmdir``); nothing in PrismaBuild
    makes a symlink in it.  So no PrismaBuild actor can change what a path
    resolves to between this call and the lock, and nothing outside
    PrismaBuild takes the lock, so holding it never made the fence atomic
    with the unlink against one.  The stage root itself is resolved once
    here instead of twice per entry.
    """

    fences: dict[str, tuple] = {}
    try:
        stage_resolved = stage.resolve()
    except OSError as exc:
        return {str(key): ("error", f"{key}: {exc}") for key in entries}
    for key, entry in entries.items():
        if not isinstance(entry, Mapping):
            continue
        path = Path(str(entry["stage_path"]))
        try:
            resolved = path.resolve()
        except OSError as exc:
            fences[str(key)] = ("error", f"{key}: {exc}")
            continue
        if stage_resolved not in resolved.parents:
            # A fragment naming a path outside the stage is not a thing to
            # act on: the writer validated it, so this is corruption or
            # someone else's file.
            fences[str(key)] = ("error", f"{key}: outside {stage}")
            continue
        fences[str(key)] = ("ok", path, os.path.normpath(str(path)),
                            os.path.normpath(str(resolved)),
                            str(resolved.relative_to(stage_resolved)))
    return fences


def ownership_census(queue: pool.PoolQueue, mover_action_key: str, *,
                      consumer_action_key: str, stage: Path, tier_id: str,
                      root: Path, entries: Mapping[str, object],
                      memo: kept_reads.CensusMemo | None = None,
                      shared: bool = False) -> dict[str, object]:
    """The four ownership censuses one egress decides on, in snapshot order.

    Claimed movers first, then fragment directories, then pins, then
    promotion sources: the order :func:`_evict_owned` argues from.  Called
    twice per egress since #988 -- once before the lock as a hint that fills
    ``memo``, and once under it, where the result is acted on.
    ``co_owner_documents`` are the fragments that name any of ``entries``'
    paths, for a skip checkpoint to fence on (#1056).
    """

    wanted = _wanted_stage_paths(dict(entries))
    claimed, claimed_taint, own_claimed = claimed_paths_attributed(
        queue, tier_id, own_key=mover_action_key, memo=memo)
    named_by: list[dict[str, object]] = []
    same_mover: list[str] | None = [] if shared else None
    owners, fragment_taint = fragment_owners(
        root, wanted,
        except_consumer=consumer_action_key,
        except_mover=mover_action_key, memo=memo, named_by=named_by,
        same_mover=same_mover)
    pins, pin_taint = reader_lease.live_for(
        queue, wanted, residency_root=root,
        memo=memo.pins if memo is not None else None)
    source_paths, source_taint = claimed_source_paths(queue, stage, memo=memo)
    return {"claimed": claimed, "own_claimed": own_claimed, "owners": owners,
            "co_owner_documents": named_by,
            "same_mover_namespaces": sorted(set(same_mover or ())),
            "pins": pins, "source_paths": source_paths,
            "tainted": fragment_taint + claimed_taint + pin_taint + source_taint}


@dataclass(frozen=True)
class RetainedPaths:
    paths: frozenset[str] = frozenset()
    refusal: str = ""
    errors: tuple[str, ...] = ()
    movers_in_flight: tuple[str, ...] = ()


def source_mark_census(queue: pool.PoolQueue, *, tier_id: str, stage: Path,
                       cas_root: str, residency_root,
                       memo: kept_reads.CensusMemo, hint: bool = False) -> RetainedPaths:
    """One source-mark retention verdict, preserving conditional census reads."""
    if not hint:
        try:
            moving = movers_in_flight(queue, tier_id=tier_id)
        except (OSError, pool.PoolContractError, ValueError) as exc:
            return RetainedPaths(refusal="queue_census_unreadable", errors=(str(exc),))
        if moving:
            return RetainedPaths(refusal="movers_in_flight", errors=("movers_in_flight",),
                                 movers_in_flight=tuple(sorted(moving)))
    attributed, taint = attributed_census(
        queue, wanted=None, residency_root=residency_root, memo=memo)
    if taint and not hint:
        return RetainedPaths(refusal="attribution_unreadable", errors=tuple(
            f"ownership uncertain: {item}" for item in taint[:ATTRIBUTION_TAINT_LIMIT]))
    pins, taint = reader_lease.live_for(
        queue, None, residency_root=residency_root, memo=memo.pins)
    if taint and not hint:
        return RetainedPaths(refusal="pin_census_unreadable", errors=tuple(
            f"ownership uncertain: {item}" for item in taint))
    attributed |= set(pins)
    claims, taint = claimed_paths(queue, tier_id, cas_root, memo=memo)
    if taint and not hint:
        return RetainedPaths(refusal="claim_census_unreadable", errors=tuple(
            f"ownership uncertain: {item}" for item in taint))
    if not hint:
        for relative in claims:
            attributed.add(os.path.normpath(os.path.join(str(stage), str(relative))))
    sources, taint = claimed_source_paths(queue, stage, cas_root, memo=memo)
    if taint and not hint:
        return RetainedPaths(refusal="promotion_census_unreadable", errors=tuple(
            f"ownership uncertain: {item}" for item in taint))
    if not hint:
        attributed |= {os.path.normpath(str(one)) for one in sources}
    return RetainedPaths(frozenset(attributed))


@contextmanager
def source_mark_authority(queue: pool.PoolQueue, *, tier_id: str, stage: Path,
                          cas_root: str, residency_root, memo: kept_reads.CensusMemo):
    """Re-census under the existing ownership lock and keep it through the act."""
    with ownership(queue, stage, role="reclaim-source-marks") as granted:
        parse_counts = memo.counts()
        validate_started = time.perf_counter()
        verdict = source_mark_census(
            queue, tier_id=tier_id, stage=stage, cas_root=cas_root,
            residency_root=residency_root, memo=memo)
        yield granted, verdict, parse_counts, validate_started


def claimed_source_paths(queue: pool.PoolQueue, stage: Path,
                          cas_root: str | Path | None = None,
                          memo: kept_reads.CensusMemo | None = None,
                          ) -> tuple[set[str], list[str]]:
    """Stage paths a live RAM promotion may be reading, by sealed source leg.

    The promotion copies stage -> ram, so the stage egress must treat the
    promotion's *source* window the way it treats a stage mover's
    destination: a pending copy handoff that blocks eviction until its ram
    fragment lands.  Only ram-tier mover claims are read (a stage mover's
    destinations are `_claimed_paths`' job); the sealed `--source-stage-root`
    decides which stage this attribution joins, resolved before comparing.
    Anything unreadable taints the pass, the same fail-closed rule as
    fragments and destination claims.  With a ``memo``, a promotion's derived
    source paths are remembered by claim key, as
    :func:`_claimed_paths_attributed` remembers a mover's (#988).
    """

    try:
        stage_real = os.path.realpath(stage)
    except OSError as exc:
        return set(), [f"promotion-source: {exc}"]
    paths: set[str] = set()
    tainted: list[str] = []
    try:
        keys = sorted(path.name[:-len(".json")] if path.name.endswith(".json")
                      else path.name
                      for path in pb.immutable_files.scan_optional(
                          queue.records.dir(pool.record_store.CLAIMED),
                      ))
    except OSError as exc:
        return paths, [f"claimed: {exc}"]
    records: list[tuple[str, dict[str, object]]] = []
    for key in keys:
        try:
            item = pool.record_store.value_or_raise(
                queue.records.item(pool.record_store.CLAIMED, key),
            )
        except (OSError, pool.PoolContractError) as exc:
            tainted.append(f"{key[:12]}: {exc}")
            continue
        if item is None:
            continue
        if not isinstance(item, dict):
            tainted.append(f"{key[:12]}: unreadable claim record")
            continue
        records.append((key, item))
    default_cas = (str(cas_root) if cas_root is not None
                   else str(queue.root.parent / "cas"))
    for key, item in records:
        if cas_root is not None:
            own_cas = str(cas_root)
        else:
            root = item.get("cas_root")
            own_cas = (str(root) if isinstance(root, str) and root
                       else default_cas)
        resources = item.get("resources")
        if not isinstance(resources, Mapping):
            tainted.append(f"{key[:12]}: malformed resources")
            continue
        kinds = {str(kind).split("@", 1)[1] for kind in resources
                 if "@" in str(kind)}
        if not any(kind.startswith(storage_tiers.RAM_TIER_PREFIX)
                   for kind in kinds):
            continue    # not a promotion; stage destinations are elsewhere
        memo_key = (key, own_cas, stage_real)
        remembered = memo.sources.get(memo_key) if memo is not None else None
        if remembered is not None:
            paths.update(remembered)
            continue
        try:
            request = pool.record_store.read_document(
                Path(own_cas) / "requests" / key[:2] / f"{key}.json")
        except (OSError, pool.PoolContractError) as exc:
            tainted.append(f"{key[:12]}: {exc}")
            continue
        if not isinstance(request, Mapping):
            tainted.append(f"{key[:12]}: unreadable sealed request")
            continue
        params = request.get("params")
        command = params.get("command") if isinstance(params, Mapping) else None
        if not isinstance(command, list):
            tainted.append(f"{key[:12]}: promotion seals no command")
            continue
        try:
            source_root = command[command.index("--source-stage-root") + 1]
            start = command[command.index("--range-start-bytes") + 1]
            end = command[command.index("--range-end-bytes") + 1]
            if (not isinstance(source_root, str) or not source_root):
                raise ValueError("source stage root must be a path")
            for bound in (start, end):
                if (isinstance(bound, bool) or not isinstance(bound, str)
                        or not bound.isdigit()):
                    raise ValueError(
                        "range bounds must be nonnegative integer strings")
            start, end = int(start), int(end)
            if end < start:
                raise ValueError("range end precedes start")
        except (ValueError, IndexError, TypeError):
            tainted.append(f"{key[:12]}: promotion seals an invalid source range")
            continue
        try:
            if os.path.realpath(source_root) != stage_real:
                if memo is not None:
                    memo.sources[memo_key] = frozenset()
                continue    # another stage's source leg, not this egress
        except OSError as exc:
            tainted.append(f"{key[:12]}: {exc}")
            continue
        digest = None
        inputs = request.get("inputs")
        if isinstance(inputs, list):
            for entry in inputs:
                if (isinstance(entry, Mapping)
                        and entry.get("id") == manifest_codec.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID):
                    digest = entry.get("sha256")
        if not isinstance(digest, str) or not digest:
            tainted.append(f"{key[:12]}: sealed request names no data manifest")
            continue
        layout = manifest_layout_for(own_cas, digest)
        if layout is None:
            tainted.append(f"{key[:12]}: manifest {digest[:12]} unreadable")
            continue
        mount_prefix, entries = layout
        named_once = paths_named_once(entries)
        try:
            window = manifest_layout.entries_between(entries, start, end)
        except (ValueError, TypeError) as exc:
            tainted.append(f"{key[:12]}: range not cuttable: {exc}")
            continue
        derived: set[str] = set()
        for entry in window:
            path, offset = str(entry["path"]), int(entry["offset"])
            try:
                # Both spellings, as in :func:`_claimed_paths_attributed`: a
                # promotion sealed before range-only naming reads the bare
                # name its stage leg wrote.
                relatives = {
                    stage_relative(path, offset, int(entry["bytes"]),
                                   mount_prefix=mount_prefix),
                    pre_range_stage_relative(
                        path, offset, int(entry["bytes"]),
                        mount_prefix=mount_prefix,
                        named_once=path in named_once)}
            except ValueError:
                continue
            for relative in relatives:
                derived.add(os.path.normpath(os.path.join(stage_real, relative)))
        paths.update(derived)
        if memo is not None:
            memo.sources[memo_key] = frozenset(derived)
    return paths, tainted


def _wanted_stage_paths(entries: dict[str, object]) -> set[str]:
    """The staged paths one fragment's entries name, normalized.

    The same set the pin census is asked about inside the ownership lock and
    outside it, so the hint that decides whether to reclaim and the census
    that decides what to delete are asking about the same bytes.
    """

    return {os.path.normpath(str(entry.get("stage_path", "")))
            for entry in entries.values() if isinstance(entry, Mapping)}


def _evict_owned(queue: pool.PoolQueue, mover_action_key: str, *,
                 consumer_action_key: str, stage: Path, tier_id: str | None,
                 root: Path, fragment_path: Path,
                 entries: dict[str, object], errors: list[str],
                 reason: str, auto_reclaimed: list[str],
                 auto_retained: dict[str, str],
                 whole: bool = False,
                 memo: kept_reads.CensusMemo | None = None,
                 fences: Mapping[str, tuple] | None = None,
                 prune_after: set[Path] | None = None,
                 shared_range: bool = False) -> dict[str, object]:
    """Unlink what is exclusively this mover's, under the ownership lock.

    ``shared_range`` is a shared range's whole eviction (#1026), called with
    the range's share namespace as ``consumer_action_key``.  It is not
    ``shared``: that name is this function's count of entries a co-owner
    keeps, and a flag under it read as that count, zero.  Every fragment of
    this mover under another namespace is the range's own vouch -- the copy
    the tier loop fans out to each interested consumer -- so none of them is
    a co-owner that keeps a file and decharges the tokens, and each is
    dropped with the share namespace's own once the range is gone.  A
    deferral files the retiring mark under each of them too: a reader
    acquires its cover through its own consumer's copy, and the mark closes
    a generation per ``(consumer, mover)``.

    ``auto_reclaimed``/``auto_retained`` are what containment reclamation did
    before this lock was taken (#780); nothing here reclaims, because
    reclamation takes other roots' locks and this one is already held.

    The mover being retired's own live claim is never a distinct co-owner
    (#793).  It is not settled on its receipt either: a move receipt carries
    no immutable attempt identity, so a complete-looking one cannot be told
    from a previous attempt's while the same key is claimed again (2026-09-21
    root QA).  A live own claim therefore defers the retire -- file, fragment,
    material and charge stay -- and the claim stops being visible at all once
    the worker's terminal transition retires it; the next sweep then deletes
    and releases exactly once.  Foreign claims keep the shared skip unchanged.

    What runs here is the act, not the census (#988).  ``memo`` carries the
    censuses :func:`_evict_locked` took before the lock, so the census taken
    here re-lists every directory and re-opens every document but re-parses
    only what changed.  ``fences`` carries each entry's containment fence,
    resolved before the lock.  ``prune_after`` collects the directories the
    unlinks may have emptied, for the caller to prune after the lock.  A
    caller that passes none of them (``prune_stale_mentions``) gets the
    same verdicts, computed here.
    """

    deleted = missing = shared = deferred = 0
    validate_started = time.perf_counter()
    parse_counts = memo.counts() if memo is not None else None
    bytes_deleted = bytes_shared = bytes_gone = 0
    shared_with: list[str] = []
    live_pins: list[str] = []
    deferred_handoffs: list[str] = []
    own_deferred = False
    own_claimed: set[str] = set()
    retiring_written = False
    same_namespaces: list[str] = []
    if entries and tier_id is not None:
        # Snapshot order is the argument: claimed movers first, then fragment
        # dirs, then pins.  A claim exists before its copy starts (the start
        # gate), a fragment exists before its claim is gone (publication
        # precedes the terminal marking), and a pin is filed under the
        # ownership lock before its first read -- so one view in this order
        # covers a publisher, a reader, or a promotion in either transition.
        # Pins join the same snapshot (same lock) rather than a second
        # unlocked check: delete is blocked by ANY current ref, and the
        # check-and-act is one atomic unit with the unlink below.
        census = ownership_census(
            queue, mover_action_key, consumer_action_key=consumer_action_key,
            stage=stage, tier_id=tier_id, root=root, entries=entries,
            memo=memo, shared=shared_range)
        same_namespaces = list(census.get("same_mover_namespaces") or ())
        claimed = census["claimed"]
        own_claimed = census["own_claimed"]
        owners = census["owners"]
        pins = census["pins"]
        source_paths = census["source_paths"]
        tainted = census["tainted"]
        if tainted:
            # Ownership is uncertain: behave like the unreadable-fragment
            # case -- nothing is unlinked, no tokens come back, the receipt
            # says so and the next sweep retries.
            errors.extend(f"ownership uncertain: {item}" for item in tainted)
            owners, claimed = {}, set()
            pins, source_paths = {}, set()
            own_claimed = set()
            blind = True
        else:
            blind = False
        own_generation: str | None = None
        if not blind:
            # The pins above are the post-reclamation census: refs whose
            # attempts are provably contained (terminal broker telemetry plus
            # broker-persisted proof) were retired before this lock was taken,
            # so ordinary completion, crash/withdrawal cleanup and old-attempt
            # drains free their pins without an operator.  Whatever this
            # snapshot still shows is a genuinely live reader, and only that
            # defers.
            #
            # The retiring mark this deferral may file binds the material
            # generation, never the path: without a sidecar the generation
            # is unknowable, so a pinned legacy range taints instead of
            # filing a mark that could wedge the path's future generations.
            material = _read_own_material(
                root, consumer_action_key, mover_action_key, memo)
            if isinstance(material, dict):
                own_generation = str(material.get("generation") or "")
            elif material is not None:
                errors.append(
                    f"ownership uncertain: material unreadable for "
                    f"{mover_action_key[:12]}")
                owners, claimed = {}, set()
                pins, source_paths = {}, set()
                blind = True
    else:
        owners, claimed = {}, set()
        pins, source_paths = {}, set()
        own_generation = None
        blind = False
        if shared_range:
            # No vouch of the range's own to judge, but the fanned copies
            # may still stand, and they go with the range (#1026).
            _owners, taint = fragment_owners(
                root, set(), except_consumer=consumer_action_key,
                except_mover=mover_action_key, memo=memo,
                same_mover=same_namespaces)
            errors.extend(f"ownership uncertain: {item}" for item in taint)
    census_validate_s = time.perf_counter() - validate_started
    locked_parses = locked_parse_record(memo, parse_counts)
    if fences is None and entries and not blind:
        # No fences from before the lock: resolve them here, as before #988.
        fences = _entry_fences(stage, entries)

    def judge(key: str, entry: Mapping[str, object]) -> tuple[str, object]:
        """One entry's verdict under this lock, with nothing done yet.

        ``("error", message)``, ``("handoff", pins)``, ``("own", None)``,
        ``("shared", (co_owners, in_flight))``, ``("pinned", pins)`` or
        ``("unlink", path)``.  Judged separately from acting so an
        all-or-nothing eviction (``whole``, #903) can see every entry's
        verdict before the first unlink; the ordinary pass acts on the same
        verdicts in the same order, under the same lock.
        """

        # The sharing check compares validated strings (exact, no metadata);
        # the resolve is the containment fence before any unlink, taken
        # once per entry by :func:`_entry_fences` (#988).
        assert fences is not None
        fence = fences.get(str(key))
        if fence is None:
            return ("error", f"{key}: no containment fence")
        if fence[0] == "error":
            return ("error", fence[1])
        _ok, path, norm, resolved_norm, relative = fence
        pinned = pins.get(norm, [])
        if resolved_norm in source_paths:
            # A live promotion is reading this source leg into RAM: a pending
            # copy handoff, deferred exactly like a live pin.  The file, this
            # mover's fragment, its material sidecar and its full occupancy
            # charge all stay until the handoff ends -- the promotion's ram
            # fragment names another tier and path and can never prove this
            # SSD incarnation, so dropping the same-path proof here is what
            # left an unprovable surviving file behind.
            #
            # This check precedes the co-owner/in-flight skip.  Another
            # consumer's same-path fragment proves the bytes for a general
            # stage publisher, but a promotion resolves its source cover in
            # its own consumer/manifest namespace (ram_promote's coverage
            # loop and ``reader_lease.acquire``), so a co-owner cannot stand
            # in for this owner's pending proof acquisition.  The handoff
            # wins while it lives; the ordinary shared decharge or last-owner
            # deletion settles on the retry after the claim is gone.
            #
            # No retiring mark is filed while any handoff in this mover's
            # document is deferred.  A mark closes one material generation to
            # new acquires, and ``ram_promote`` takes its proof-only cover
            # through ``reader_lease.acquire`` *after* its claim row exists,
            # so a mark filed here would refuse the very handoff this defers
            # for.  The mark is per mover, not per entry, so a pinned entry
            # on the same mover waits for the handoff to end before its own
            # mark is filed; deleting stays safe meanwhile because every pass
            # re-reads claims and pins under this same ownership lock.
            return ("handoff", pinned)
        if relative in own_claimed:
            # This mover's own copy is still live: its claim is not a
            # distinct co-owner, so this is a deferral, not a shared skip.
            # The file, this mover's fragment, its material and its full
            # charge stay, and the next sweep retries once the worker's
            # terminal transition has retired the claim.  Sharing here would
            # decharge this mover's own duplicate and drop its only vouch
            # while the bytes stayed behind nothing (#793).
            return ("own", None)
        co_owners = sorted(owners.get(norm, set()))
        in_flight = relative in claimed
        if co_owners or in_flight:
            # Another live fragment vouches for these bytes, or a claimed
            # copy is about to land them: keep the file, drop only this
            # mover's own vouching below.  The last owner to leave deletes.
            return ("shared", (co_owners, in_flight))
        if pinned:
            # A live reader holds these bytes (open FD, prefetch, mmap, or a
            # promotion source pin): defer, mark retiring for this material
            # generation, keep the file, the fragment and the charge.  The
            # next sweep deletes after the last release.
            if not own_generation:
                return ("error", f"{key}: pinned but material unqualifiable")
            return ("pinned", pinned)
        return ("unlink", path)

    verdicts: dict[str, tuple[str, object]] = (
        {} if blind else {key: judge(key, entry)
                          for key, entry in entries.items()})
    entries_judged = len(verdicts)
    declined: list[str] = []
    if whole:
        # All or nothing (#903): a range a live consumer will still read is
        # either given back whole or kept whole.  Any entry that would defer
        # or cannot be judged declines the eviction before anything moves --
        # no unlink, no retiring mark, the tokens and the fragment held --
        # and the pins that caused it are named so the caller can say why.
        declined = sorted({verdict for verdict, _detail in verdicts.values()
                           if verdict in ("error", "handoff", "own", "pinned")})
        if blind or errors:
            declined = sorted(set(declined) | {"ownership-uncertain"})
        if declined:
            for verdict, detail in verdicts.values():
                if verdict in ("handoff", "pinned"):
                    live_pins.extend(detail)              # type: ignore[arg-type]
            verdicts = {}
    # The egress this process runs as an action, if any (#1021): the drain
    # is its second phase, and each entry whose unlink returned counts.
    run = egress_run.get()
    if run is not None:
        run.enter(movement_actions.EGRESS_DRAIN_PHASE)
    unlink_started = time.perf_counter()
    for key, (verdict, detail) in verdicts.items():
        entry = entries[key]
        if verdict == "error":
            errors.append(str(detail))
            continue
        if verdict == "handoff":
            deferred += 1
            deferred_handoffs.append("promotion-handoff")
            live_pins.extend(detail)                      # type: ignore[arg-type]
            continue
        if verdict == "own":
            deferred += 1
            own_deferred = True
            continue
        if verdict == "shared":
            co_owners, in_flight = detail                 # type: ignore[misc]
            shared += 1
            shared_with.extend(
                f"{consumer[:12]}/{mover[:12]}" for consumer, mover in co_owners)
            if in_flight:
                shared_with.append("in-flight-copy")
            bytes_shared += int(entry["bytes"])
            continue
        if verdict == "pinned":
            deferred += 1
            live_pins.extend(detail)                      # type: ignore[arg-type]
            continue
        path = detail                                     # type: ignore[assignment]
        try:
            os.unlink(path)
        except FileNotFoundError:
            missing += 1
            bytes_gone += int(entry["bytes"])
            if run is not None:
                run.released(int(entry["bytes"]))
            continue
        except OSError as exc:
            errors.append(f"{key}: {exc}")
            continue
        deleted += 1
        bytes_deleted += int(entry["bytes"])
        if run is not None:
            run.released(int(entry["bytes"]))
        if prune_after is not None:
            prune_after.add(path.parent)
        else:
            prune_empty(path.parent, stage)
    unlink_s = time.perf_counter() - unlink_started
    if run is not None:
        run.enter(movement_actions.EGRESS_RELEASE_PHASE)

    released = decharged = 0
    if (deferred and not deferred_handoffs and not errors and own_generation
            and not own_deferred):
        # A handoff-deferred pass files nothing: closing this generation
        # would refuse the promotion's own cover acquire and strand the
        # handoff holding these bytes.  The next pass files the ordinary mark
        # once no handoff remains and only readers do; a mark already on disk
        # is preserved, never cleared by a deferral.  An own-copy deferral
        # files nothing either, for the same shape of reason: the live copy
        # is republishing this material and a mark would close the generation
        # it is producing.
        reader_lease.write_retiring(
            reader_lease.leases_root(queue, root),
            consumer_action_key=consumer_action_key,
            mover_action_key=mover_action_key, generation=own_generation)
        for namespace in same_namespaces:
            reader_lease.write_retiring(
                reader_lease.leases_root(queue, root),
                consumer_action_key=namespace,
                mover_action_key=mover_action_key, generation=own_generation)
        retiring_written = True
    # Retiring retains the charge until the actual delete with the last live
    # ref already absent; release-before-reclaim stays forbidden.
    # A shared skip is not an error, but its tokens no longer come back as
    # free: the bytes live on under another accounted owner (same lock, so
    # the last owner cannot disappear between the check and the act), so
    # the duplicate ownership is decharged (#733) while this mover's own
    # vouching is dropped.  Ledger release is idempotent: no double-free.
    released = decharged = 0
    if errors or deferred or declined:
        pass
    elif tier_id is None:
        # No fragment at all: nothing is known shared, so every token comes
        # back exactly as before -- holding them would cost the tier its
        # capacity for good.
        released = tier_holdings.egress_all(queue, mover_action_key)
    else:
        # Freed share: whole GiB actually leaving the stage now (deleted
        # here).  Floor, not ceil: a 1-token holder with 0.5 GiB deleted
        # and 0.5 GiB shared must not free a whole token for half a GiB of
        # new room.  Already-gone files count here too, and that is still
        # single-count: bytes gone before discovery are already inside the
        # minted writable (and were inside landed while a complete holder
        # stood for them), bytes gone after it are not, and never-landed
        # bytes moved nothing at all -- while every retry caps both shares
        # at the still-held remainder, so no pass frees twice.
        # Decharged share: enough to cover every staying byte (ceil), so
        # the duplicate can never leak into free through a fraction.
        # Anything left over (slack between demand and ceil, unknown
        # kinds, rates which are not byte-backed) returns, as before.
        # The mint lock is the leaf here (transition -> ownership ->
        # mint; nothing takes it in the other order), so a stale-snapshot
        # mint cannot slip between the decharge and the fragment drop.
        occupancy = storage_tiers.capacity_kind_of(tier_id)
        held = queue.tier(tier_id).holdings(mover_action_key)
        destroy: dict[str, int] = {}
        free: dict[str, int] = {}
        for kind, count in held.items():
            if kind in tier_holdings.RATE_KINDS or kind != occupancy:
                free[kind] = count
                continue
            freed = min(count, _tokens_for_newly_free_bytes(
                bytes_deleted + bytes_gone))
            shared_part = min(count - freed, _tokens_for_egressed_bytes(
                bytes_shared))
            if shared_part:
                destroy[kind] = shared_part
            free[kind] = count - shared_part
        with queue.tier(tier_id).mutation_lock():
            outcome = queue.tier(tier_id).settle_egress(mover_action_key, destroy=destroy, free=free)
        decharged = sum(outcome["destroyed"].values())
        released = sum(outcome["released"].values())
        shortfall = outcome["shortfall"]
        if shortfall:
            # A decharge that failed partway keeps its tokens and fails
            # loudly: freeing the duplicate it was meant to destroy would
            # reopen the phantom, and dropping the fragment would lose the
            # retry.  The next sweep converges.
            assert isinstance(shortfall, dict)
            errors.append("decharge-incomplete: %s" % (sorted(
                "%s=%d" % item for item in shortfall.items()),))
        # Movers hold on one tier; sweep any other tier the old path freed
        # -- but never this one, where a destroy shortfall above is still
        # retained and must stay held.
        for other_tier in tier_holdings.ids(queue.records):
            if other_tier == tier_id:
                continue
            try:
                released += queue.tier(other_tier).egress_best_effort(mover_action_key)
            except (OSError, pool.PoolContractError):
                continue
    if not errors and not deferred and not declined:
        # A shared range's fanned copies go first and its own vouch last
        # (#1026), so an interrupted pass always leaves the share
        # namespace's fragment for the next one to find the others by.
        for namespace in same_namespaces:
            withdraw_vouch(queue, root, namespace, mover_action_key)
        withdraw_vouch(queue, root, consumer_action_key, mover_action_key)
    return {
        "schema": pool.POOL_EGRESS_SCHEMA_V1,
        "action_key": mover_action_key,
        "consumer_action_key": consumer_action_key,
        "stage_root": str(stage),
        "reason": reason,
        "entries_deleted": deleted,
        "entries_already_gone": missing,
        "entries_shared": shared,
        "shared_with": sorted(set(shared_with))[:SHARED_WITH_LIMIT],
        "entries_deferred": deferred,
        "live_pins": sorted(set(live_pins)),
        "deferred_handoffs": sorted(set(deferred_handoffs)),
        # An own-copy deferral names itself, so the retry loop can tell a
        # claim that was still writing from a reader pin or a promotion.
        "deferred_own": ["own-copy-in-flight"] if own_deferred else [],
        "auto_reclaimed": sorted(set(auto_reclaimed)),
        "auto_retained": dict(sorted(auto_retained.items())),
        "retiring": retiring_written,
        "bytes_deleted": bytes_deleted,
        "bytes_shared": bytes_shared,
        "tokens_released": released,
        "tokens_decharged": decharged,
        # Errors -- or a deferral -- mean the stage still holds bytes, so
        # the tokens stay held: releasing them would let the ledger admit a
        # mover onto capacity that is not there.  The receipt says so and
        # the next sweep retries.  A shared skip is not an error, but its
        # tokens no longer come back as free either: the bytes live on
        # under another owner, so the duplicate ownership is decharged
        # (#733) and only this mover's own vouching is dropped.  A deferred
        # handoff keeps this mover's own vouching too: the promotion's ram
        # fragment cannot prove the SSD incarnation it read (#768).
        "complete": not errors and not deferred and not declined,
        "errors": errors,
        "host": socket.gethostname(),
        "unix": time.time(),
        # Only an all-or-nothing eviction declines (#903), and only its
        # receipt names the key: the egress receipt keeps its shape.
        **({"declined": declined} if whole else {}),
        # A shared range's eviction names the copies it took with it.
        **({"shared_vouches": sorted(same_namespaces)} if shared else {}),
        # What the pass under the lock cost (#988): the entries it judged,
        # the census it re-took there, the documents that census parsed
        # and reused, and its unlinks.  The caller adds the hold itself and
        # the census it took before the lock.
        "entries_judged": entries_judged,
        "census_validate_s": round(census_validate_s, 6),
        **locked_parses,
        "unlink_s": round(unlink_s, 6),
    }


_skip_checkpoints: dict[tuple, dict[str, object]] = {}
_skip_checkpoints_lock = threading.Lock()
_skip_checkpoint_usage = {"fences": 0, "bytes": 0}
#: The owners the dead-owner sweep discovered on its latest pass, as
#: checkpoint keys, and how many checkpoints that allows
#: (:func:`_retain_skip_checkpoints`).  Empty until a sweep has run: only an
#: owner a sweep discovered can be skipped by one.
_skip_checkpoint_scope: dict[str, object] = {"keys": frozenset(), "capacity": 0}


def reset_skip_checkpoints() -> None:
    """Forget every skip checkpoint (tests, and callers that want a reset)."""

    with _skip_checkpoints_lock:
        _skip_checkpoints.clear()
        _skip_checkpoint_usage["fences"] = 0
        _skip_checkpoint_usage["bytes"] = 0
        _skip_checkpoint_scope["keys"] = frozenset()
        _skip_checkpoint_scope["capacity"] = 0


def skip_checkpoint_usage() -> dict[str, int]:
    """How many checkpoints, fences and fence path bytes the cache holds."""

    with _skip_checkpoints_lock:
        return {"checkpoints": len(_skip_checkpoints),
                "capacity": int(_skip_checkpoint_scope["capacity"]),
                **_skip_checkpoint_usage}


def _skip_checkpoint_capacity(discovered: frozenset) -> int:
    """How many checkpoints a sweep that discovered ``discovered`` may keep.

    One per discovered owner (#1056).  Not a literal: the owners are the
    fragments on disk the sweep's census found, and each checkpoint's fences
    are bounded by its own fragment's entries and the co-owner fragments
    that name them.
    """

    return len(discovered)


def _retain_skip_checkpoints(discovered) -> None:
    """Keep only the checkpoints of the owners a sweep just discovered.

    The dead-owner sweep calls this once its discovery completes, before it
    consults any checkpoint.  An owner it no longer discovers -- evicted,
    adopted, its fragment gone -- loses its checkpoint here, so what the
    cache holds follows what is on disk, and the capacity is what this
    discovery allows.
    """

    keys = frozenset(discovered)
    with _skip_checkpoints_lock:
        for key in [key for key in _skip_checkpoints if key not in keys]:
            _forget_checkpoint_locked(key)
        _skip_checkpoint_scope["keys"] = keys
        _skip_checkpoint_scope["capacity"] = _skip_checkpoint_capacity(keys)


def _path_version(path: Path | str) -> kept_reads.FileVersion | None:
    """Change evidence for one regular metadata file, or ``None``."""

    try:
        info = os.lstat(path)
    except OSError:
        return None
    if not statmod.S_ISREG(info.st_mode):
        return None
    return kept_reads.metadata_version(info)


def _fenced_path_version(
    path: Path | str,
    fence: int | None,
    *,
    trust,
) -> tuple[kept_reads.FileVersion | None,
                                    kept_reads.FileVersion | None]:
    """``(version, trusted)`` for one regular metadata file, from one ``lstat``.

    ``version`` is :func:`_path_version`'s change evidence, which the prune
    compares before and after its scan.  ``trusted`` is the same version
    when a skip checkpoint may stand on it, else ``None``: the #1045 rule
    (:func:`trust.file_version`) with ``fence`` read before this
    ``lstat``.  A file whose ctime is not strictly before the fence changed
    in the fence's clock tick, and a rename cycle later in that tick can be
    given the freed inode number and reproduce its file-version fields (#1070).
    """

    try:
        info = os.lstat(path)
    except OSError:
        return None, None
    if not statmod.S_ISREG(info.st_mode):
        return None, None
    return kept_reads.metadata_version(info), trust.file_version(
        info, fence, path=path, follow_symlinks=False)


def _skip_checkpoint_key(queue: pool.PoolQueue, root: Path, stage: Path,
                         tier_id: str, consumer_action_key: str,
                         mover_action_key: str) -> tuple:
    """The cache key: both roots and the owner identity, never just the key.

    Two queues, two residency roots or two stage roots can name the same
    action key; a checkpoint is a statement about one root's files only, so
    the roots travel in the key.
    """

    return (str(queue.root), str(root), str(stage), tier_id,
            consumer_action_key, mover_action_key)


def _forget_checkpoint_locked(key: tuple) -> None:
    record = _skip_checkpoints.pop(key, None)
    if record is not None:
        _skip_checkpoint_usage["fences"] -= int(record["fence_count"])
        _skip_checkpoint_usage["bytes"] -= int(record["bytes"])


def _co_owner_fences(memo: kept_reads.CensusMemo,
                     documents: list[dict[str, object]],
                     ) -> dict[str, tuple] | None:
    """The file version of every co-owner fragment a verdict relied on.

    Each is the version the census under the lock read the document at
    (:meth:`kept_reads.CensusMemo.version_of`), never a fresh sample, so a co-owner
    removed or rewritten after that read is seen by the next pass.  ``None``
    when any document's version is unknown: then nothing may be cached.
    """

    fences: dict[str, tuple] = {}
    for document in documents:
        seen = memo.version_of(document)
        if seen is None:
            return None
        fences[seen[0]] = seen[1]
    return fences


class _CheckpointVerdict:
    """What :func:`_install_skip_checkpoint` did: installed, or why not (#1069).

    Truthy exactly when the checkpoint was installed, so it reads as the
    boolean it always was; ``refused`` names the reason when it was not:

    * ``document-version-unknown`` -- this owner's fragment or material
      version, or a co-owner fragment's, could not be read, or the #1070
      fence refused this owner's (changed in the tick it was read in, or on
      a filesystem the trusted rule does not list; documents are tested
      first, so an owner on such a filesystem reports this reason);
    * ``no-directory-stamp`` -- the owner names no path, so there is no
      directory to fence;
    * ``directory-stamp-untrusted`` -- a parent directory's stamp was refused
      by the trusted rule (#1062): changed in the tick it was read in, or on
      a filesystem the rule does not list;
    * ``outside-sweep-scope`` -- the latest sweep did not discover this owner;
    * ``cache-full`` -- the cache holds as many checkpoints as the sweep
      allows, and a newcomer is refused rather than evicting one.
    """

    __slots__ = ("refused",)

    def __init__(self, refused: str = "") -> None:
        self.refused = refused

    def __bool__(self) -> bool:
        return not self.refused

    def __repr__(self) -> str:
        return f"_CheckpointVerdict(refused={self.refused!r})"


#: Every reason a skip checkpoint can be refused, in the order they are tested.
SKIP_CHECKPOINT_REFUSALS = (
    "document-version-unknown", "no-directory-stamp",
    "directory-stamp-untrusted", "outside-sweep-scope", "cache-full")


def _install_skip_checkpoint(key: tuple, fragment_version, material_version,
                             stamps: dict[str, kept_reads.DirectoryEvidence | None],
                             documents: dict[str, tuple] | None = None,
                             ) -> _CheckpointVerdict:
    """Install the EXACT verified versions of one owner with nothing to act on.

    The caller passes the trusted stamps it sampled before its scan
    (:func:`kept_reads.filesystem_trust.directory_before`, #1062) and proved equal to
    a sample after it, and the co-owner fragment versions its census read
    (``documents``, which ``None`` refuses); nothing is sampled again here, so
    a rename that lands between the scan and this call can never be blessed
    as clean -- the next pass reads the recorded (older) stamp, sees the
    difference and re-scans.  Every stamp must be present: a directory the
    trusted rule refused, because it changed in the tick its stamp was taken
    in, is one a later rename might not move, so nothing is installed on it.
    The fragment and material versions are the trusted ones
    (:func:`_fenced_path_version`, #1070), ``None`` for a document changed in
    the tick of its read, and ``None`` installs nothing either.
    Only an owner the latest sweep discovered is cached, one checkpoint each
    (:func:`_retain_skip_checkpoints`); when the cache is full the newcomer
    is refused and nothing is evicted.  A cache entry only ever skips a
    cleanup scan: it holds no deletion or adoption authority.

    Returns a :class:`_CheckpointVerdict`: truthy when installed, else it
    names the refusal, which the stale-mention receipt carries (#1069).
    """

    if fragment_version is None or material_version is None:
        return _CheckpointVerdict("document-version-unknown")
    if documents is None or any(version is None
                                for version in documents.values()):
        return _CheckpointVerdict("document-version-unknown")
    if not stamps:
        return _CheckpointVerdict("no-directory-stamp")
    if any(version is None for version in stamps.values()):
        return _CheckpointVerdict("directory-stamp-untrusted")
    fences = len(stamps) + len(documents)
    chars = sum(len(name) for name in stamps) + sum(len(name) for name in documents)
    with _skip_checkpoints_lock:
        _forget_checkpoint_locked(key)
        if key not in _skip_checkpoint_scope["keys"]:  # type: ignore[operator]
            return _CheckpointVerdict("outside-sweep-scope")
        if (len(_skip_checkpoints)
                >= int(_skip_checkpoint_scope["capacity"])):  # type: ignore[arg-type]
            return _CheckpointVerdict("cache-full")
        _skip_checkpoints[key] = {
            "fragment": fragment_version, "material": material_version,
            "dirs": dict(stamps), "documents": dict(documents),
            "fence_count": fences, "bytes": chars,
        }
        _skip_checkpoint_usage["fences"] += fences
        _skip_checkpoint_usage["bytes"] += chars
    return _CheckpointVerdict()


def _skip_checkpoint_hit(key: tuple, fragment_path: Path,
                         material_path: Path, *, trust) -> bool:
    """Whether an unchanged owner with nothing to act on may skip its scan.

    Every recorded stamp is re-read -- this owner's fragment and material,
    each parent directory of its paths, and each co-owner fragment its
    verdict relied on; any difference forgets the checkpoint and runs the
    uncached check, so a rename, a replacement, a removal or a metadata
    rewrite is seen before any skip. File and directory versions must also
    still be trusted: bare equality cannot preserve a checkpoint after its
    filesystem, namespace or mount-watch evidence is lost. A hit only ever
    skips the per-entry scan: the owner's terminal, live, lease and plan state were checked
    before this is consulted, and no deletion, adoption or mutable-state
    check is skipped.
    """

    with _skip_checkpoints_lock:
        record = _skip_checkpoints.get(key)
    if record is None:
        return False
    fence = trust.file_fence()  # before any current file observation (#1045)
    fresh = (_fenced_path_version(fragment_path, fence, trust=trust)[1] == record["fragment"]
             and _fenced_path_version(material_path, fence, trust=trust)[1] == record["material"])
    if fresh:
        for name, version in dict(record["documents"]).items():
            if _fenced_path_version(name, fence, trust=trust)[1] != version:
                fresh = False
                break
    if fresh:
        for evidence in dict(record["dirs"]).values():
            if not trust.holds(evidence):
                fresh = False
                break
    if not fresh:
        with _skip_checkpoints_lock:
            _forget_checkpoint_locked(key)
    return fresh


def _read_material_nofollow(root: Path, consumer_action_key: str,
                            mover_action_key: str):
    """The sidecar read with the cleanup authority's regular/no-follow rules.

    ``reader_lease.read_material`` follows a symlink standing where the
    sidecar belongs; a deletion authority may not.  Returns the validated
    document, ``None`` for absence, or a reason string for anything that is
    not a readable, valid regular file.
    """

    path = reader_lease.material_path(
        root, consumer_action_key, mover_action_key)
    try:
        raw = pb.immutable_files.read_stable_file(path, where="prune material")
    except FileNotFoundError:
        return None
    except (OSError, ValueError, pb.PrismaBuildError) as exc:
        return f"material unreadable: {exc}"
    try:
        return reader_lease.validate_material(json.loads(raw))
    except (ValueError, reader_lease.ReaderLeaseError, pb.PrismaBuildError) as exc:
        return f"material invalid: {exc}"


def _containment_state(stage: Path, target: Path) -> str:
    """``"ok"``, ``"absent"`` or ``"unknown"`` for one path under a stage.

    Every component is ``lstat``ed from the stage root down, so a symlinked
    intermediate directory, a file standing where a directory belongs, or a
    path that leaves the stage is unknown ownership, never a pathname to act
    on.  Only the final component may be missing, and that is ``"absent"``:
    the caller may prune the mention but never unlink anything.

    A staged range's final name is two components, ``<rel>.pbrange`` and
    ``<offset>-<size>``, and the unlink that retires the range prunes the
    then-empty ``<rel>.pbrange`` directory with it.  That directory missing
    is therefore the range missing, ``"absent"``, not an unknown
    intermediate: every component above it was still ``lstat``ed.
    """

    try:
        # The configured root is resolved once (a stage root may itself be a
        # symlink); every component *below* it is lstat'ed, so a symlinked
        # intermediate directory is never followed.
        nominal = Path(os.path.abspath(str(stage)))
        base = Path(os.path.realpath(str(stage)))
        target_abs = Path(os.path.abspath(str(target)))
    except OSError:
        return "unknown"
    try:
        relative = target_abs.relative_to(nominal)
    except ValueError:
        return "unknown"
    if not relative.parts:
        return "unknown"
    current = base
    for index, part in enumerate(relative.parts):
        current = current / part
        last = index == len(relative.parts) - 1
        range_directory = (index == len(relative.parts) - 2
                           and part.endswith(RANGE_SUFFIX))
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            return "absent" if last or range_directory else "unknown"
        except OSError:
            return "unknown"
        if last:
            return "ok"
        if not statmod.S_ISDIR(info.st_mode):
            return "unknown"
    return "unknown"


def prune_stale_mentions(
    queue: pool.PoolQueue,
    mover_action_key: str,
    *,
    consumer_action_key: str,
    stage: Path,
    tier_id: str,
    root: Path,
    observed: Mapping[str, object],
    trust=None,
) -> dict[str, object]:
    """Prune positively stale paths from one terminal owner's documents (#853).

    A FAILED consumer's executed DONE mover can leave a material sidecar
    dating an incarnation the live destination no longer carries.  The shared
    publisher is right to refuse both adoption and replacement of that name,
    so the owner's stale vouch has to go before a successor can publish the
    path -- but a *mixed* document also holds coherent entries whose proof,
    bytes and charge are reusable cache, and whole-owner ``evict`` would
    destroy them.

    This transaction selects, classifies and acts inside one stage ownership
    hold (the lock a publisher's decide-and-rename holds), under the mover
    transition lock the caller already holds.  The rules, in order:

    * the stage root must positively belong to this queue
      (:func:`stage_root_refusal`), and the fragment must be the snapshot the
      terminal checks authorized and bind exactly to this tier and stage;
    * the material is read with the cleanup authority's regular/no-follow
      rules, and must bind the same consumer, mover, tier, stage root,
      manifest and epoch as the fragment;
    * **every** fragment entry must bind exactly to its own material key --
      key present, matching stage path, matching bytes, and matching digest
      where the fragment declares one -- before any path state is classified,
      so unknown metadata can never authorize a deletion;
    * a live claim, live reader pin, promotion handoff or same-key claim
      overlapping the candidate retains the whole owner: the bounded #877
      mover ends before the later ordinary sweep recovers the still-owned
      paths, and no mention is dropped merely because a foreign claim promises
      a future proof;
    * a path component outside the stage, or a symlinked intermediate
      directory, file or anything else standing where a directory belongs, is
      unknown ownership and retains the whole owner.  Only a missing leaf is
      ``absent``;
    * positive staleness for an existing regular file is inode difference,
      exactly as :meth:`staged_path.StagedPublisher._proof_candidate` reads it
      (#755).  Same-inode size/time change is divergence, not permission.  A
      co-owner fragment protects its physical file, so that entry and its date
      stay untouched;
    * the fragment and material file versions are sampled before their reads
      and again after the scan; a change during classification retains the
      whole owner and installs no checkpoint;
    * a fully stale, unprotected owner goes through :func:`_evict_owned` --
      the ordinary whole-owner egress -- inside this same transaction (no
      containment reclamation: that runs before the ownership lock by
      design), so its holder is settled exactly once;
    * a mixed owner is partially pruned: each positively stale destination is
      unlinked after a fresh identity comparison, then the fragment is
      rewritten to its survivors and the material to the same survivors'
      mentions, under the same generation and epoch.  A crash between the two
      document writes is safe: the fragment is authoritative and first, so a
      surviving material mention that no fragment entry names is a date
      without an owner, never ownership.

    The old holder is deliberately left with its entire charge.  That is a
    conservative reservation, including any slack for the deleted entries:
    when the final old fragment disappears, the ordinary whole-owner egress
    releases it exactly once, and ordinary pressure eviction may retire the
    smaller fragment and return the remainder in the meantime.  No partial
    capacity reclamation is claimed.

    The receipt reports committed metadata prunes (only after the fragment
    write succeeds), already-absent entries, entries actually unlinked and
    their bytes, whether the fragment/material pair completed, and that the
    whole charge was retained.

    An owner with nothing to act on -- no stale or absent path and no
    superset material this pass would trim (only an owner no co-owner
    protects is trimmed), whether coherent, protected by co-owners, or
    retained for any reason above -- changes nothing and installs a skip
    checkpoint (#1056).  It fences this owner's fragment and material, every
    parent directory of its paths, and every co-owner fragment the census
    under the lock read, each at the version verified here.  The next pass
    skips only while all of them are unchanged, and a skip only ever
    retains, so a removed or rewritten co-owner re-runs this transaction
    before anything it protected can prune or evict.  An owner with a stale
    or absent path is never cached, whatever retains it.
    """
    trust = kept_reads.filesystem_trust if trust is None else trust

    fragment_path = residency_map.fragment_path(
        root, consumer_action_key, mover_action_key)
    material_path = reader_lease.material_path(
        root, consumer_action_key, mover_action_key)
    errors: list[str] = []
    retained_reason = ""

    def receipt(*, pruned: int = 0, absent: int = 0, retained: int = 0,
                unlinked: int = 0, unlinked_bytes: int = 0,
                pair_complete: bool = True, partial: bool = False,
                complete: bool = False, cacheable=False,
                charge_retained: bool = True) -> dict[str, object]:
        # ``cacheable`` may be a checkpoint verdict: an idle owner whose
        # checkpoint was refused says why (#1069); every other receipt,
        # cached or not idle, carries an empty reason.
        refused = getattr(cacheable, "refused", "")
        return {
            "schema": pool.POOL_EGRESS_SCHEMA_V1, "event": STALE_MENTION_EVENT,
            "action_key": mover_action_key,
            "consumer_action_key": consumer_action_key,
            "tier_id": tier_id, "stage_root": str(stage),
            "reason": "stale-mention-prune",
            "entries_pruned": pruned, "entries_already_absent": absent,
            "entries_retained": retained, "entries_unlinked": unlinked,
            "bytes_unlinked": unlinked_bytes,
            "document_pair_complete": pair_complete,
            "charge_retained": charge_retained, "partial": partial,
            "cacheable": bool(cacheable), "cache_refused": refused,
            "retained_reason": retained_reason,
            "complete": complete, "errors": errors,
            "host": socket.gethostname(), "unix": time.time(),
        }

    refusal = stage_root_refusal(queue, str(stage))
    if refusal is not None:
        retained_reason = "stage-root-refused"
        errors.append(f"stage root refused: {refusal}")
        return receipt(retained=len(observed.get("entries") or {}))

    # Versions before the reads and again after the scan: metadata that
    # changed while this transaction classified it is not acted on, and the
    # versions a checkpoint installs are exactly the ones verified here.
    # A checkpoint installs only the trusted ones (#1070): a document changed
    # in the tick of the clock read before its ``lstat`` is one a same-tick
    # rename given the freed inode could reproduce, so it is scanned again
    # next pass, as a refused directory stamp is (#1062).
    document_fence = trust.file_fence()
    fragment_version_before, fragment_trusted = _fenced_path_version(
        fragment_path, document_fence, trust=trust)
    material_version_before, material_trusted = _fenced_path_version(
        material_path, document_fence, trust=trust)

    # The censuses and the containment fences go first, as hints, without
    # the lock (#988): the pass under it re-reads only what changed, and the
    # observed entries are the ones it acts on, because it retains unless
    # the fragment under the lock equals ``observed``.
    memo = kept_reads.CensusMemo(trust=trust)
    census_started = time.perf_counter()
    observed_entries = observed.get("entries")
    hint_fences: dict[str, tuple] | None = None
    if isinstance(observed_entries, Mapping) and observed_entries:
        ownership_census(queue, mover_action_key,
                          consumer_action_key=consumer_action_key,
                          stage=stage, tier_id=tier_id, root=root,
                          entries=observed_entries, memo=memo)
        _read_own_material(root, consumer_action_key, mover_action_key, memo)
        hint_fences = _entry_fences(stage, observed_entries)
    census_s = time.perf_counter() - census_started
    emptied: set[Path] = set()

    def _held_prune_stale() -> dict[str, object]:
        """The transaction itself, under the stage ownership lock."""

        nonlocal retained_reason
        try:
            current = residency_map.validate_fragment(
                json.loads(
                    pb.immutable_files.read_stable_file(
                        fragment_path,
                        where="stale-mention fragment",
                    ),
                ),
            )
        except (OSError, ValueError, pb.PrismaBuildError) as exc:
            errors.append(f"ownership uncertain: {exc}")
            return receipt(retained=len(observed.get("entries") or {}))
        entries = dict(current["entries"])
        total = len(entries)
        if current != observed:
            retained_reason = "fragment-changed"
            return receipt(retained=total)
        if str(current.get("tier_id")) != tier_id:
            retained_reason = "ownership-uncertain"
            errors.append("ownership uncertain: fragment tier does not match")
            return receipt(retained=total)
        if str(current.get("stage_root")) != str(stage):
            retained_reason = "ownership-uncertain"
            errors.append("ownership uncertain: fragment stage does not match")
            return receipt(retained=total)
        material = _read_material_nofollow(
            root, consumer_action_key, mover_action_key)
        if material is None:
            retained_reason = "material-absent"
            return receipt(retained=total)
        if isinstance(material, str):
            retained_reason = "material-unreadable"
            errors.append(f"ownership uncertain: {material}")
            return receipt(retained=total)
        if (str(material.get("consumer_action_key")) != consumer_action_key
                or str(material.get("mover_action_key")) != mover_action_key
                or str(material.get("tier_id")) != str(current.get("tier_id"))
                or str(material.get("stage_root")) != str(current.get("stage_root"))
                or str(material.get("manifest_sha256"))
                != str(current.get("manifest_sha256"))):
            retained_reason = "material-does-not-bind"
            return receipt(retained=total)
        # The strict reader's exact sidecar convention, read the way
        # ``reader_lease.acquire`` and ``covers_for_keys`` read it: a
        # non-RAM tier's fragment and sidecar carry no epoch at all
        # (absent or empty), and a RAM tier's carry the same non-empty
        # one.  Agreement is not validity -- two documents that agree on
        # a staged epoch are two documents the reader refuses
        # ("ownership-uncertain: staged epoch set"), and a deletion
        # authority may never read metadata the reader will not read as
        # ownership.  Anything else retains the whole candidate.
        fragment_epoch = current.get("epoch")
        material_epoch = material.get("epoch")
        if tier_id.startswith(storage_tiers.RAM_TIER_PREFIX):
            if not (isinstance(fragment_epoch, str) and fragment_epoch):
                retained_reason = "epoch-invalid"
                return receipt(retained=total)
            if material_epoch != fragment_epoch:
                retained_reason = "material-epoch-mismatch"
                return receipt(retained=total)
        elif (fragment_epoch not in (None, "")
                or material_epoch not in (None, "")):
            retained_reason = "epoch-invalid"
            return receipt(retained=total)
        # Every fragment entry binds exactly to its own material key before
        # any path state is classified: a by-path or first-mention match would
        # let unknown metadata authorize a deletion.  Extra material keys are
        # the crash superset and are ignored; every surviving key must bind.
        material_entries = dict(material.get("entries") or {})
        bound: dict[str, Mapping] = {}
        for key, entry in entries.items():
            mention = material_entries.get(key)
            if not isinstance(mention, Mapping):
                retained_reason = "material-key-missing"
                return receipt(retained=total)
            if (os.path.normpath(str(mention["stage_path"]))
                    != os.path.normpath(str(entry["stage_path"]))
                    or int(mention["bytes"]) != int(entry["bytes"])):
                retained_reason = "material-key-mismatch"
                return receipt(retained=total)
            declared = entry.get("sha256")
            if (isinstance(declared, str) and declared
                    and str(mention["sha256"]) != declared):
                retained_reason = "material-digest-mismatch"
                return receipt(retained=total)
            if not isinstance(mention.get("file_id"), Mapping):
                retained_reason = "material-key-undated"
                return receipt(retained=total)
            bound[key] = mention
        census = ownership_census(
            queue, mover_action_key, consumer_action_key=consumer_action_key,
            stage=stage, tier_id=tier_id, root=root, entries=entries,
            memo=memo)
        claimed = census["claimed"]
        own_claimed = census["own_claimed"]
        owners = census["owners"]
        pins = census["pins"]
        source_paths = census["source_paths"]
        tainted = census["tainted"]
        checkpoint_key = _skip_checkpoint_key(
            queue, root, stage, tier_id, consumer_action_key, mover_action_key)
        # The whole owner is retained, whatever its paths say, for any of
        # these, in the order the transaction has always tested them.  Since
        # #1056 they no longer end the classification: the path state below
        # decides whether a skip checkpoint may stand for this owner.  The
        # checkpoint certifies only that nothing is stale, absent or to trim,
        # and a skip only ever retains, so a retention reason ending changes
        # nothing it stands for -- while a stale path under any reason is
        # never cached, because the retention only postpones its prune.
        held_reason = ""
        held_errors: list[str] = []
        if tainted:
            held_reason = "ownership-uncertain"
            held_errors = [f"ownership uncertain: {item}" for item in tainted]
        elif own_claimed:
            held_reason = "same-key-claimed"
        elif pins:
            held_reason = "live-pin"

        def uncached(reason: str, *why: str) -> dict[str, object]:
            """Retain the whole owner uncached; the first reason found wins."""

            nonlocal retained_reason
            if held_reason:
                retained_reason = held_reason
                errors.extend(held_errors)
            else:
                retained_reason = reason
                errors.extend(why)
            return receipt(retained=total)

        stage_abs = Path(os.path.abspath(str(stage)))
        overlap_claim = overlap_handoff = False
        contained: dict[str, str] = {}
        for key, entry in entries.items():
            path = Path(str(entry["stage_path"]))
            state = _containment_state(stage, path)
            contained[key] = state
            if state == "unknown":
                return uncached(
                    "ownership-uncertain",
                    f"ownership uncertain: {entry['stage_path']} is not a "
                    f"contained stage path")
            try:
                relative = str(Path(os.path.abspath(str(path))).relative_to(
                    stage_abs))
            except (OSError, ValueError):
                relative = None
            if relative is not None and relative in claimed:
                overlap_claim = True
            try:
                resolved = os.path.normpath(str(path.resolve()))
            except OSError:
                resolved = ""
            if resolved in source_paths:
                overlap_handoff = True
        if not held_reason and overlap_claim:
            held_reason = "live-claim"
        elif not held_reason and overlap_handoff:
            held_reason = "promotion-handoff"
        # The verified directory stamps, sampled around the classification
        # scan; nothing is sampled a third time for installation.  The
        # sample before the scan is a TRUSTED stamp (#1062): two changes in
        # one coarse clock tick share a stamp, so a stamp sampled between
        # them would be one the second change never moves, and a checkpoint
        # standing on it would skip a rename made in that tick.
        # ``trust.directory_before`` refuses a directory changed
        # in the current tick (and one on a filesystem whose times are not
        # this kernel's clock); a refused stamp still takes part in the
        # before/after comparison, at its bare version, but the checkpoint
        # refuses to install on it, so the owner is scanned again next pass.
        parents = {Path(str(entry["stage_path"])).parent
                   for entry in entries.values()}
        directory_observations = trust.directory_observations(parents)
        dirs_trusted = directory_observations.trusted
        prune: list[str] = []
        absent: list[str] = []
        retained_paths = 0
        expected_ino: dict[str, int] = {}
        for key, entry in entries.items():
            if held_reason and (prune or absent):
                # Retained whatever the rest says, and never cacheable once
                # a path is stale or absent: the scan has nothing left to
                # decide, so it stops here rather than hold the lock for it.
                return uncached(held_reason)
            if contained[key] == "absent":
                absent.append(key)
                continue
            path = Path(str(entry["stage_path"]))
            norm = os.path.normpath(str(path))
            # The path's own state is classified before the co-owner branch:
            # a co-owner's fragment protects a physical file, but it can never
            # make a nonregular, unstatable or unknown path clean, and it must
            # not let another stale path of the same candidate delete.
            try:
                info = os.lstat(path)
            except FileNotFoundError:
                absent.append(key)
                continue
            except OSError as exc:
                return uncached("ownership-uncertain",
                                f"ownership uncertain: {key}: {exc}")
            if not statmod.S_ISREG(info.st_mode):
                return uncached(
                    "ownership-uncertain",
                    f"ownership uncertain: {key} is not a regular file")
            if owners.get(norm):
                # A co-owner's fragment protects the physical file: keep this
                # entry and its date exactly as they are.
                retained_paths += 1
                continue
            if int(bound[key]["file_id"].get("ino", -1)) != int(info.st_ino):
                # Positive staleness: the name carries an incarnation this
                # record does not date (#755), so the vouch can never prove
                # or protect it.
                prune.append(key)
                expected_ino[key] = int(info.st_ino)
                continue
            live = reader_lease.stat_identity(str(path))
            if live is None or not reader_lease.file_id_matches(
                    bound[key]["file_id"], live):
                # Same inode, changed in place: divergence, not permission.
                return uncached(
                    "ownership-uncertain",
                    f"ownership uncertain: {key} changed in place")
        # The after sample needs no clock: it is only compared with the
        # before sample.  Equal means no change moved a stamp during the
        # scan, so each trusted stamp is still the directory's version, and
        # that trusted stamp (never this sample) is what a checkpoint records.
        if not directory_observations.unchanged():
            return uncached("ownership-uncertain",
                            "ownership uncertain: a parent directory changed")
        fragment_version_after = _path_version(fragment_path)
        material_version_after = _path_version(material_path)
        if (fragment_version_after is None or material_version_after is None
                or fragment_version_after != fragment_version_before
                or material_version_after != material_version_before):
            return uncached("documents-changed")
        # What a skip checkpoint fences beyond this owner's own documents and
        # directories: every co-owner fragment that names one of its paths,
        # at the version the census under this lock read it (#1056).  One
        # removed or rewritten re-runs this census on the next pass, where
        # the file it protected may now prune or evict.
        co_owner_fences = _co_owner_fences(memo, census["co_owner_documents"])
        exact_material = set(material_entries) == set(entries)
        if held_reason:
            retained_reason = held_reason
            errors.extend(held_errors)
            # Idle means a pass without the reason would change nothing:
            # nothing stale or absent, and no superset material that pass
            # would trim (it trims only an owner no co-owner protects).
            idle = (not prune and not absent
                    and (retained_paths > 0 or exact_material))
            cacheable = idle and _install_skip_checkpoint(
                checkpoint_key, fragment_trusted,
                material_trusted, dirs_trusted, co_owner_fences)
            return receipt(retained=total, cacheable=cacheable)
        if not prune and not absent:
            if retained_paths:
                # Protected by co-owners and otherwise coherent: nothing
                # changes until a document it read does (#1056).
                retained_reason = "co-owner"
                cacheable = _install_skip_checkpoint(
                    checkpoint_key, fragment_trusted,
                    material_trusted, dirs_trusted, co_owner_fences)
                return receipt(retained=total, cacheable=cacheable)
            # A crash between the fragment and material writes leaves the
            # material a superset.  The strict reader ignores a date no
            # vouch cites (#1087), but the pair is complete only when the
            # material dates exactly the fragment's validated keys: trim it
            # before caching this otherwise-coherent owner, under the same
            # generation.
            material_final_version = material_trusted
            if not exact_material:
                try:
                    _date_vouch(
                        root, current, str(material["generation"]),
                        {key: material_entries[key] for key in entries},
                        material_epoch=material.get("epoch"))
                except (OSError, ValueError, pb.PrismaBuildError) as exc:
                    errors.append(f"material trim: {exc}")
                    return receipt(retained=total)
                # The trim's own write is in the current tick, so this is
                # trusted only once a tick has passed since it (#1070).
                material_final_version = _fenced_path_version(
                    material_path, trust.file_fence(), trust=trust)[1]
            cacheable = _install_skip_checkpoint(
                checkpoint_key, fragment_trusted,
                material_final_version, dirs_trusted, co_owner_fences)
            return receipt(retained=total, cacheable=cacheable)
        if not retained_paths and len(prune) + len(absent) == total:
            # Fully stale and unprotected: the ordinary whole-owner egress is
            # exact and settles the holder once.  Called here, inside the same
            # ownership transaction; containment reclamation deliberately does
            # not run under this lock.
            return _evict_owned(
                queue, mover_action_key, consumer_action_key=consumer_action_key,
                stage=stage, tier_id=tier_id, root=root,
                fragment_path=fragment_path, entries=entries, errors=errors,
                reason="stale-mention-prune", auto_reclaimed=[],
                auto_retained={}, memo=memo, fences=hint_fences,
                prune_after=emptied)
        # Partial prune: unlink the positively stale destinations, then write
        # the fragment to its survivors and the material to the same
        # survivors' mentions.  No ledger call: the whole old charge stays.
        surviving = {key: entry for key, entry in entries.items()
                     if key not in expected_ino and key not in absent}
        unlinked: list[str] = []
        unlinked_bytes = 0

        def partial_receipt(*, fragment_committed: bool,
                            pair_complete: bool) -> dict[str, object]:
            # Only the committed fragment write makes a metadata prune real;
            # the physical deletions are reported as they actually happened,
            # even when the pair never completed.
            return receipt(
                pruned=(len(prune) + len(absent)) if fragment_committed else 0,
                absent=len(absent) if fragment_committed else 0,
                retained=len(surviving) if fragment_committed else total,
                unlinked=len(unlinked), unlinked_bytes=unlinked_bytes,
                pair_complete=pair_complete, partial=True)

        for key in prune:
            path = Path(str(entries[key]["stage_path"]))
            try:
                info = os.lstat(path)
            except FileNotFoundError:
                continue
            except OSError as exc:
                errors.append(f"{key}: {exc}")
                return partial_receipt(fragment_committed=False,
                                       pair_complete=False)
            if (not statmod.S_ISREG(info.st_mode)
                    or int(info.st_ino) != expected_ino[key]):
                # Fresh identity comparison at the act itself: what the scan
                # classified is what is unlinked, or nothing is.
                errors.append(f"{key}: destination changed under the lock")
                return partial_receipt(fragment_committed=False,
                                       pair_complete=False)
            try:
                os.unlink(path)
            except FileNotFoundError:
                continue
            except OSError as exc:
                errors.append(f"{key}: {exc}")
                return partial_receipt(fragment_committed=False,
                                       pair_complete=False)
            unlinked.append(key)
            unlinked_bytes += int(info.st_size)
            emptied.add(path.parent)
        # Survivor material is filtered by the fragment's exact validated key
        # set, never by matching path: an extra material key naming the same
        # path must not survive the rewrite, and safe extra material never
        # grants ownership or deletion authority.
        survivor_mentions = {key: material_entries[key] for key in surviving}
        document = {key: current[key] for key in current if key != "entries"}
        document["entries"] = surviving
        rewritten = shrink_vouch(
            root, document, generation=str(material["generation"]),
            material_entries=survivor_mentions, material_epoch=material.get("epoch"))
        if rewritten.error:
            errors.append(rewritten.error)
        return partial_receipt(fragment_committed=rewritten.fragment_committed,
                               pair_complete=rewritten.complete)

    # One hold for the whole transaction, as before #988: its per-entry
    # ``lstat`` classification is the act's own identity evidence, and a
    # partial prune rewrites the fragment and material inside it.  What
    # left the hold is the census parse and the fence (above) and the
    # empty-directory prune (below).
    asked = time.perf_counter()
    with ownership(queue, stage, role="prune-stale-mentions") as granted:
        result = _held_prune_stale()
    released = time.perf_counter()
    pruned_started = time.perf_counter()
    for parent in sorted(emptied, key=lambda one: len(one.parts), reverse=True):
        prune_empty(parent, stage)
    result.update(hold_record(
        lock_wait_s=granted - asked, lock_held_s=released - granted,
        census_s=census_s, prune_s=time.perf_counter() - pruned_started))
    return result


def _tokens_for_newly_free_bytes(stage_bytes: int) -> int:
    """Token count actually leaving the stage settles, in whole GiB, floored.

    The conservative sibling of :func:`_tokens_for_egressed_bytes`: only
    whole GiB of genuinely new room return as free.  A 1-token holder with
    0.5 GiB deleted and 0.5 GiB shared frees nothing here -- the ceil
    would hand a whole token back for half a GiB of new room -- while the
    staying half is covered by the decharge share instead.  Slack the
    floor leaves behind is restored by the next fresh mint counting the
    actually-grown writable, never by freeing what is still occupied.
    """

    if stage_bytes <= 0:
        return 0
    return int(stage_bytes) // storage_tiers.GIB


def _tokens_for_egressed_bytes(stage_bytes: int) -> int:
    """Token count a bucket of egressed bytes settles, in whole GiB.

    The same ceil the demand was sealed with
    (:func:`storage_tiers.stage_tokens_for_bytes`): a sealed demand is
    always at least this floor, so capping the freed share at this number
    can only decharge more, never free more -- the safe direction.  Zero
    bytes settle zero tokens.
    """

    if stage_bytes <= 0:
        return 0
    return storage_tiers.stage_tokens_for_bytes(int(stage_bytes))


def _owner_record(queue: pool.PoolQueue, state: str, key: str) -> dict:
    """Read a regular, identity-bound queue outcome; filenames are not proof."""
    record = queue.records.retirement_candidate(state, key)
    if (not isinstance(record, dict)
            or record.get("schema") != pool.POOL_OUTCOME_SCHEMA_V1
            or record.get("action_key") != key):
        raise pool.PoolContractError("dead-owner outcome identity mismatch")
    queue.records.attempt_generation(record)  # validates a finite generation and key
    return record


def _require_exact_withdrawal(queue: pool.PoolQueue, key: str) -> dict:
    """The withdrawn record of key, with its decision unchanged.

    The generation has exactly one immutable decision and withdrawal_covers
    must answer that decision. Worker additions are accepted only as the
    verified immutable withdrawn attempt's projection; every decision field
    remains exact and arbitrary extra fields still refuse.
    """

    marker = _owner_record(queue, pool.record_store.WITHDRAWN, key)
    decisions = queue.records.withdrawal_decisions(key, generation=marker["published_unix"])
    if (marker.get("status") != "withdrawn" or len(decisions) != 1
            or queue.withdrawal_covers(marker, action_key=key) != decisions[0][1]):
        raise pool.PoolContractError("withdrawal lacks its exact immutable decision")
    decision = decisions[0][1]
    if marker == decision:
        return marker
    retained = marker.get("withdrawn_attempt")
    if (not isinstance(retained, Mapping)
            or any(retained.get(field) != decision.get(field) for field in
                   ("action_key", "published_unix", "max_attempts", "retry_safe"))):
        raise pool.PoolContractError("withdrawal lacks its exact immutable decision")
    ending = queue.attempt_ending(retained)
    if ending["status"] != pool.record_store.WITHDRAWN or ending["disposition"] != pool.record_store.WITHDRAWN:
        raise pool.PoolContractError("withdrawal lacks its exact immutable decision")
    expected = {**decision, "withdrawn_attempt": retained}
    for field in ("resource_scope", "resource_scope_intent", "resource_scope_cleanup", "container_cleanup"):
        if field not in decision and field in ending["detail"]:
            expected[field] = ending["detail"][field]
    for field in ("finished_unix", "finished_host"):
        if field not in decision:
            expected[field] = ending[field]
    if marker != expected:
        raise pool.PoolContractError("withdrawal lacks its exact immutable decision")
    return marker


def unended_owner(queue: pool.PoolQueue, consumer: str) -> str:
    """Why a fragment-named consumer has not provably ended, or ``""``.

    The fragment names who staged the bytes; only an ending says nobody will
    read them.  The consumer must be neither ready nor claimed, and exactly
    one outcome record must say how it ended: ``done`` (its inputs are
    spent), ``failed``, or ``withdrawn`` with its one immutable decision.  No
    outcome at all is not an ending -- #798's legacy consumer is that shape,
    and a queue whose records are not all visible yet looks the same -- and
    two outcomes are a question, never an answer.
    """

    try:
        live, why = residency_plan.live_state(queue, consumer)
        if why:
            return f"its consumer's queue state is uncertain: {why}"
        if live:
            return f"its consumer {consumer[:12]} is still queued ({live})"
        ended = [state for state in (pool.record_store.DONE, pool.record_store.FAILED, pool.record_store.WITHDRAWN)
                 if not (not pool.record_store.value_or_raise(
                     queue.records.retirement_presence_at(queue.records.item_path(state, consumer)),
                 ))]
        if not ended:
            return f"its consumer {consumer[:12]} has no outcome record"
        if len(ended) > 1:
            return (f"its consumer {consumer[:12]} has {len(ended)} outcome "
                    f"records: {', '.join(ended)}")
        if ended[0] == pool.record_store.WITHDRAWN:
            _require_exact_withdrawal(queue, consumer)
        else:
            _owner_record(queue, ended[0], consumer)
    except (OSError, ValueError, pb.PrismaBuildError) as exc:
        return f"its consumer's outcome is unreadable: {exc}"
    return ""


def _is_mover_partial(name: str) -> bool:
    return name.startswith(PARTIAL_PREFIX) and name.endswith(PARTIAL_SUFFIX)


def _marked_by_the_prewarm_stage(path: Path | str) -> bool | None:
    """Whether this file is a prewarm stage object, or ``None`` if unanswerable.

    The prewarm loop stages into the same pool, and its objects carry
    ``user.pbstage.source`` set on the handle before the rename, so a completed
    one is always marked.  ``None`` is the case that matters: a filesystem
    without user extended attributes still stages, and there "no mark" would
    read as "not the prewarm loop's" for every object it owns.  So the answer is
    unknown rather than false, and the caller refuses to delete on it.
    """

    try:
        os.getxattr(path, STAGE_SOURCE_XATTR)
    except OSError as exc:
        # The attribute is absent: an answer, and it is "no".
        if exc.errno in _XATTR_ABSENT:
            return False
        # Anything else -- the filesystem does not carry user attributes, the
        # call is not permitted -- is not an answer.
        return None
    return True


def _stage_mark(path: Path | str) -> tuple[str, str]:
    """Which kind of unattributed file this is, by its marks (#1088).

    Returns ``(kind, mover)``, the kind one of :data:`RECONCILE_DELETED_KINDS`
    or :data:`RECONCILE_LEFT_KINDS` other than ``partial``, and ``mover`` the
    writer a ``mover_residue`` mark names.  The mover's mark is read first:
    a mover's copy carries both marks and only this one says whose it is.
    Its absence is an answer and hands the question to the prewarm loop's
    mark, decided as it always was (:func:`_marked_by_the_prewarm_stage`);
    any other failure, or a value that is not an action key, is no answer.
    """

    try:
        raw = os.getxattr(path, STAGE_MOVER_XATTR)
    except OSError as exc:
        if exc.errno not in _XATTR_ABSENT:
            return "mark_unanswerable", ""
    else:
        mover = raw.decode("utf-8", "replace")
        if not is_action_key(mover):
            return "mark_unanswerable", ""
        return "mover_residue", mover
    marked = _marked_by_the_prewarm_stage(path)
    if marked is None:
        return "mark_unanswerable", ""
    return ("source_mark_only" if marked else "unmarked"), ""


#: How many census taints one receipt names before it counts the rest.
ATTRIBUTION_TAINT_LIMIT = 8


def attributed_census(queue: pool.PoolQueue, *, wanted: set[str] | None,
                       residency_root: str | Path | None = None,
                       memo: kept_reads.CensusMemo | None = None,
                       ) -> tuple[set[str], list[str]]:
    """The strict attribution census behind :func:`attributed_stage_paths`.

    Same selection, with the taint channel a deletion pass needs: a caller
    that deletes must retain when ownership is unknown, and a fragment the
    reader tolerance skipped is unknown ownership, not an unowned file.
    """

    of_wanted, _of_any, tainted = _named_census(
        queue, wanted=wanted, residency_root=residency_root, memo=memo)
    return of_wanted, tainted


def _named_census(queue: pool.PoolQueue, *, wanted: set[str] | None,
                  residency_root: str | Path | None = None,
                  memo: kept_reads.CensusMemo | None = None,
                  ) -> tuple[set[str], set[str], list[str]]:
    """:func:`_attributed_census`, and every path *any* fragment names.

    One census, two answers, for :func:`reconcile` (#1088): the paths the
    ``wanted`` movers' fragments name (all of them when ``wanted`` is
    ``None``), and the paths every fragment names, wanted or not.  A mover's
    copy that some fragment names has an owner the key-driven passes retire
    together with that fragment and its material, so only a copy outside the
    second set is residue.  Same taint channel as the first.
    """

    root = Path(residency_root if residency_root is not None
                else queue.root / pool.record_store.RESIDENCY)
    fragments, tainted = fragment_census(root, memo)
    of_wanted: set[str] = set()
    of_any: set[str] = set()
    for _namespace, mover, fragment, _direct in fragments:
        named = (memo.normalized_paths_of(fragment) if memo is not None
                 else None)
        if named is None:
            named = frozenset(os.path.normpath(str(entry["stage_path"]))
                              for entry in dict(fragment["entries"]).values())
        of_any.update(named)
        if wanted is None or mover in wanted:
            of_wanted.update(named)
    return of_wanted, of_any, tainted


def attributed_stage_paths(queue: pool.PoolQueue, *, wanted: set[str] | None,
                           residency_root: str | Path | None = None) -> set[str]:
    """Every stage path a mover the fleet still wants has vouched for.

    ``wanted=None`` means *every* fragment counts, wanted or not.  The
    reconciliation cannot use that -- a withdrawn mover's fragment is exactly
    what it exists to clear -- but a bounded repair can, and prefers to: there
    the question is not "whose bytes are these" but "is there any reason at
    all to keep them", and one more retained file is a pass.

    Read off the fragments, because a fragment is the only document that says
    "this file is that mover's": the plan names ranges and the ledger names
    keys, and neither can be compared against a directory entry.  Only the
    fragments of ``wanted`` movers count.  A withdrawn mover's fragment is still
    on disk -- nothing deletes it, since no egress ran -- and treating it as
    attribution is exactly how its bytes became invisible.

    Both fragment layouts count: legacy flat consumers and the nested
    produced-output namespaces (see :func:`_fragment_census`).  This is the
    paths-only view; a caller that deletes reads :func:`_attributed_census`
    and retains on its taint instead.
    """

    out, _taint = attributed_census(queue, wanted=wanted,
                                     residency_root=residency_root)
    return out


def reconcile(queue: pool.PoolQueue, *, tier_id: str, stage_root: str,
              wanted: set[str], residency_root: str | Path | None = None,
              index: kept_reads.CensusIndex | None = None, store=None) -> dict[str, object]:
    """Evict what is on the stage that nothing on the fleet accounts for.

    The half of #608 the held-key sweep structurally cannot do: it walks the
    tier ledger's held keys, so bytes no key holds are invisible to it forever.
    A killed or withdrawn mid-copy mover leaves exactly that -- its
    ``.partial`` temporaries, and the copies it verified and renamed into
    place after the last fragment it filed -- and its tokens come back when
    its worker stops it, so the ledger reads its full supply free over
    occupied stage.  A stage mover files its fragment every
    ``stage_move.FRAGMENT_PUBLISH_S``; a RAM promotion files its only one at
    the end of its range, so a kill leaves every copy it renamed (#1088).

    Four rules decide, and each one is a fact rather than a policy:

    * **Nothing is deleted while a mover could be writing.**  A mover ready or
      claimed on this tier means a copy is in flight or about to be, and its
      destination is not in any fragment until it has been verified.  So the
      whole reconciliation is skipped for that tier, named in the receipt.  A
      per-file test cannot replace this: a ready mover can be claimed between
      the walk and the unlink.
    * **A mover's own temporary is always its own.**  ``.<name>.partial`` is
      ``stage_move``'s naming and nothing else writes it, so one that no live
      copy is producing is the residue of a killed or withdrawn one.
    * **A mover's copy that no fragment names is residue once its writer has
      ended (#1088).**  Both movers name themselves on every copy, in
      ``stage_marks.STAGE_MOVER_XATTR``, before its rename.  A copy that *no*
      fragment names -- wanted or not: a copy some fragment names is retired
      with that fragment and its material by the egress, the dead-owner
      sweep or the uncharged-owner pass -- and no live pin names is deleted
      once its writer has ended: the writer is not in ``wanted``, where a
      live consumer's plan may retry it under the same key and adopt the
      copy by content (#1081), and no entry under its key is ready or
      claimed (``residency_plan.live_state``, the ending proof the
      publication gate's owner arbitration reads, so a lease that outlived
      its record still keeps the copy).  A live RAM promotion reading the
      copy as a source leg keeps it too.  A queue state or promotion census
      that cannot be read keeps it, and says so in ``errors``.
    * **Anything else must be shown to be unowned.**  The prewarm loop stages
      into this same pool and marks its objects with ``user.pbstage.source``, so
      an unmarked file that no wanted mover's fragment names is unowned.  A
      file carrying that mark and no mover mark is left: it is a prewarm
      object or a mover's copy from before #1088, and nothing on the file
      tells the two apart (``recover_orphaned_range`` is the bounded repair
      for the second).  Where the filesystem cannot answer the xattr question
      at all, "unmarked" means nothing, and the file is left alone.

    Every file the walk finds unattributed is sized by kind
    (:data:`RECONCILE_DELETED_KINDS`, :data:`RECONCILE_LEFT_KINDS`):
    ``deleted_by_kind`` and ``unowned_left_by_kind`` carry entries and bytes,
    ``unowned_left`` and ``unowned_left_bytes`` total what was left, and
    ``mover_residue_left_reasons`` counts why each left residue copy stayed.

    **Unknown ownership never deletes.**  The attribution census is strict
    (see :func:`_fragment_census`): a fragment that cannot be read or
    validated, or a residency directory that is neither reserved bookkeeping
    nor a namespace shape, returns an incomplete receipt with bounded reasons
    and no unlinks, and the tier cycle continues.  The reader tolerance that
    skips a bad fragment is for a consumer composing its map, never for a
    pass that deletes.

    Ownership is checked the way ``evict`` checks it: the resolved path must be
    under the resolved stage root.
    """
    store = queue.records if store is None else store

    stage = Path(stage_root)
    receipt: dict[str, object] = {
        "schema": pool.POOL_EGRESS_SCHEMA_V1,
        "event": UNATTRIBUTED_EVENT,
        "action_key": "",
        "consumer_action_key": "",
        "tier_id": tier_id,
        "stage_root": str(stage),
        "reason": "unattributed-reconcile",
        "entries_deleted": 0,
        "entries_already_gone": 0,
        "bytes_deleted": 0,
        "tokens_released": 0,
        "partials_deleted": 0,
        "deleted_by_kind": _sized_kinds(RECONCILE_DELETED_KINDS),
        "unowned_left": 0,
        "unowned_left_bytes": 0,
        "unowned_left_by_kind": _sized_kinds(RECONCILE_LEFT_KINDS),
        "mover_residue_left_reasons": {},
        "left_since_walk": 0,
        "complete": True,
        "errors": [],
        "host": socket.gethostname(),
        "unix": time.time(),
    }
    refusal = stage_root_refusal(queue, stage)
    if refusal is not None:
        # Whose stage this is comes before what is on it (#628).
        receipt["event"] = STAGE_ROOT_REFUSED_EVENT
        receipt["skipped"] = refusal
        receipt["complete"] = False
        receipt["errors"] = [refusal]
        return receipt
    # The walk and the censuses go first, without the lock (#988).  The walk
    # names candidates; nothing it decides is acted on.  Under the lock,
    # every candidate is judged again against a fresh mover check, a fresh
    # attribution census and a fresh pin census, and unlinked only while its
    # ``lstat`` identity still equals the one the walk saw: a file replaced,
    # rewritten or re-marked (``setxattr`` moves ctime) since is left for the
    # next pass.  A file that appears after the walk is not a candidate on
    # this pass at all.  So the rules below are decided under the lock, as
    # before; only the listing and the per-file classification left it.
    early = movers_in_flight(queue, tier_id=tier_id, store=store)
    if early:
        # A pre-check only: the answer that counts is taken under the lock.
        # Skipping on it costs nothing, since the pass would skip anyway.
        receipt["skipped"] = "movers_in_flight"
        receipt["movers_in_flight"] = sorted(early)
        return receipt
    try:
        stage_resolved = stage.resolve(strict=True)
    except OSError as exc:
        receipt["skipped"] = f"stage_root_unreadable: {exc}"
        receipt["complete"] = False
        return receipt
    # The tier loop's index when it passes one (#992): then neither census
    # below re-reads a namespace or document unchanged since the last cycle,
    # and the one under the lock compares each directory's stamp again
    # rather than listing it (:class:`kept_reads.CensusIndex`).  Reader pins are read
    # fresh by every call either way.
    memo: kept_reads.CensusMemo = index if index is not None else kept_reads.CensusMemo()
    if index is not None:
        index.fresh_pins()
    census_started = time.perf_counter()
    hinted, hinted_named, _hint_taint = _named_census(
        queue, wanted=wanted, residency_root=residency_root, memo=memo)
    hint_pins, _hint_pin_taint = reader_lease.live_for(
        queue, None, residency_root=residency_root, memo=memo.pins)
    hinted |= set(hint_pins)
    hinted_named |= set(hint_pins)
    walk_memo = index if isinstance(index, kept_reads.CensusIndex) else None
    if walk_memo is None:
        # The old call shape, so a caller's stand-in walk that knows nothing
        # of the memo still serves a reconcile with no census to keep.
        candidates, walk_errors = _unattributed_candidates(
            stage, stage_resolved, hinted, hinted_named)
    else:
        candidates, walk_errors = _unattributed_candidates(
            stage, stage_resolved, hinted, hinted_named, memo=walk_memo)
    if any(one[2] == "mover_residue" for one in candidates):
        # Mover residue's promotion census once as a hint (#1088): it fills
        # the memo with each promotion's derived source legs, so the one
        # under the lock reads only the claim listing and its records.
        claimed_source_paths(queue, stage, memo=memo)
    census_s = time.perf_counter() - census_started
    emptied: set[Path] = set()

    def _held_reconcile() -> None:
        """The decision and the act, under the stage ownership lock."""

        validate_started = time.perf_counter()
        parse_counts = memo.counts()
        in_flight = movers_in_flight(queue, tier_id=tier_id, store=store)
        if in_flight:
            receipt["skipped"] = "movers_in_flight"
            receipt["movers_in_flight"] = sorted(in_flight)
            return
        attributed, named, attribution_taint = _named_census(
            queue, wanted=wanted, residency_root=residency_root, memo=memo)
        if attribution_taint:
            # A fragment that cannot be read, or a directory that cannot be
            # classified, is unknown ownership -- never an unowned file.  The
            # pass deletes nothing, says why with bounded reasons, and the
            # tier cycle continues; the next sweep retries once the state is
            # readable again.
            receipt["skipped"] = "attribution_unreadable"
            receipt["complete"] = False
            receipt["errors"] = [
                f"ownership uncertain: {item}"
                for item in attribution_taint[:ATTRIBUTION_TAINT_LIMIT]]
            if len(attribution_taint) > ATTRIBUTION_TAINT_LIMIT:
                receipt["errors"].append(
                    f"ownership uncertain: {len(attribution_taint)} entry(ies) "
                    f"unreadable")
            return
        pin_owners, pin_taint = reader_lease.live_for(
            queue, None, residency_root=residency_root, memo=memo.pins)
        if pin_taint:
            receipt["complete"] = False
            receipt["errors"] = [
                f"ownership uncertain: {item}" for item in pin_taint]
            return
        # A live pin is attribution: unattributed bytes nobody accounts for
        # go, pinned bytes never do.
        attributed |= set(pin_owners)
        named |= set(pin_owners)
        errors: list[str] = list(walk_errors)
        # Mover residue's own keepers (#1088), read only when there is some:
        # its writers' queue states, then the live promotions' source legs.
        # Stats and listings only, and no transition lock: none may be taken
        # under this one (transition, then ownership), and a writer queued
        # again after this read cannot rename before the lock is released,
        # nor adopt a copy without re-checking it under the lock.
        residue = [one for one in candidates if one[2] == "mover_residue"]
        writers = (_unended_writers(queue, {one[4] for one in residue},
                                    wanted) if residue else {})
        errors.extend(why for _state, why in sorted(set(writers.values()))
                      if why)
        reading: set[str] | None = set()
        if any(one[4] not in writers for one in residue):
            sources, source_taint = claimed_source_paths(queue, stage,
                                                          memo=memo)
            if source_taint:
                reading = None
                errors.extend(
                    f"promotion census uncertain: {item}"
                    for item in source_taint[:ATTRIBUTION_TAINT_LIMIT])
            else:
                reading = sources
        receipt["census_validate_s"] = round(
            time.perf_counter() - validate_started, 6)
        receipt.update(locked_parse_record(memo, parse_counts))
        receipt["entries_judged"] = len(candidates)
        deleted = bytes_deleted = partials = left = 0
        deleted_by = receipt["deleted_by_kind"]
        residue_why: dict[str, int] = {}
        for path, identity, kind, size, mover in candidates:
            normalized = os.path.normpath(str(path))
            if normalized in (named if kind == "mover_residue"
                              else attributed):
                left += 1   # an owner or a pin named it after the walk
                continue
            if kind == "mover_residue":
                why = writers[mover][0] if mover in writers else ""
                if not why and reading is None:
                    why = "promotion_census_unreadable"
                elif not why and _under_resolved(
                        path, stage, stage_resolved) in reading:
                    why = "promotion_source"
                if why:
                    residue_why[why] = residue_why.get(why, 0) + 1
                    left_by_kind["mover_residue"]["entries"] += 1
                    left_by_kind["mover_residue"]["bytes"] += size
                    continue
            try:
                info = os.lstat(path)
            except FileNotFoundError:
                continue
            except OSError as exc:
                errors.append(f"{path.name}: {exc}")
                continue
            if (not statmod.S_ISREG(info.st_mode)
                    or kept_reads.metadata_version(info) != identity):
                left += 1   # changed since the walk: the next pass decides
                continue
            try:
                os.unlink(path)
            except FileNotFoundError:
                continue
            except OSError as exc:
                errors.append(f"{path.name}: {exc}")
                continue
            deleted += 1
            bytes_deleted += int(info.st_size)
            partials += 1 if kind == "partial" else 0
            deleted_by[kind]["entries"] += 1
            deleted_by[kind]["bytes"] += int(info.st_size)
            emptied.add(path.parent)
        receipt["entries_deleted"] = deleted
        receipt["bytes_deleted"] = bytes_deleted
        receipt["partials_deleted"] = partials
        receipt["unowned_left_by_kind"] = left_by_kind
        receipt["unowned_left"] = sum(
            int(one["entries"]) for one in left_by_kind.values())
        receipt["unowned_left_bytes"] = sum(
            int(one["bytes"]) for one in left_by_kind.values())
        receipt["mover_residue_left_reasons"] = dict(
            sorted(residue_why.items()))
        # Candidates the walk found that the locked re-check kept: a
        # fragment or pin named them, or the file changed, after the walk.
        receipt["left_since_walk"] = left
        receipt["errors"] = errors
        receipt["complete"] = not errors

    # What the walk left outright -- a source mark alone, or marks nobody
    # can read -- is sized here; residue the lock keeps joins it there.
    left_by_kind = _sized_kinds(RECONCILE_LEFT_KINDS)
    for _path, identity, kind, size, _mover in candidates:
        if identity is None:
            left_by_kind[kind]["entries"] += 1
            left_by_kind[kind]["bytes"] += size
    candidates = [one for one in candidates if one[1] is not None]
    asked = time.perf_counter()
    with ownership(queue, stage, role="reconcile") as granted:
        _held_reconcile()
    released = time.perf_counter()
    pruned_started = time.perf_counter()
    for parent in sorted(emptied, key=lambda one: len(one.parts), reverse=True):
        prune_empty(parent, stage)
    receipt.update(hold_record(
        lock_wait_s=granted - asked, lock_held_s=released - granted,
        census_s=census_s, prune_s=time.perf_counter() - pruned_started))
    return receipt


def _sized_kinds(kinds: tuple[str, ...]) -> dict[str, dict[str, int]]:
    """A zero entries-and-bytes count per kind, for a receipt to fill."""

    return {kind: {"entries": 0, "bytes": 0} for kind in kinds}


def _unended_writers(queue: pool.PoolQueue, movers: set[str],
                     wanted: set[str]) -> dict[str, tuple[str, str]]:
    """Each residue writer not provably ended, as ``(reason, error)`` (#1088).

    A writer in ``wanted`` -- a live consumer's leads or plan, a live item,
    a held key -- may be retried under its own key, and the retry adopts its
    copies by content (#1081): ``mover_wanted``.  A writer with any entry
    under its key in ``ready/`` or ``claimed/`` -- its record, or a lease that
    outlived it -- may still be writing: ``mover_ready`` or
    ``mover_claimed``.  That is ``residency_plan.live_state``, the proof the
    publication gate reads before it replaces an ended owner's name.  A state
    that cannot be read is no proof: ``mover_state_unreadable``, with the
    error for the receipt.  A writer absent from the answer has ended.
    """

    out: dict[str, tuple[str, str]] = {}
    for mover in sorted(movers):
        if mover in wanted:
            out[mover] = ("mover_wanted", "")
            continue
        live, error = residency_plan.live_state(queue, mover)
        if error:
            out[mover] = ("mover_state_unreadable",
                          f"residue writer {mover[:12]}: {error}")
        elif live is not None:
            out[mover] = (f"mover_{live}", "")
    return out


def _under_resolved(path: Path, stage: Path, stage_resolved: Path) -> str:
    """``path`` spelled under the resolved stage root, as sources are named.

    The walk names a file under ``stage`` as given; the promotion census
    (:func:`_claimed_source_paths`) names a source leg under the stage's
    ``realpath``.
    """

    return os.path.normpath(os.path.join(
        str(stage_resolved), os.path.relpath(str(path), str(stage))))


def _unattributed_candidates(stage: Path, stage_resolved: Path,
                             attributed: set[str],
                             named: set[str] | None = None, *,
                             memo: "kept_reads.CensusIndex | None" = None,
                             ) -> tuple[list[tuple[Path, tuple | None, str,
                                                   int, str]],
                                        list[str]]:
    """The files :func:`reconcile` may delete, found without the lock (#988).

    The walk and every per-file rule of the reconciliation: the root's own
    markers are skipped, a symlink or non-regular file is skipped, an
    attributed path is skipped, a mover's
    copy that any fragment names is skipped (#1088), a file the prewarm
    stage alone marked -- or whose marks cannot be read -- is left, and a
    file that would be deleted but resolves outside the stage is skipped.

    ``attributed`` is what the wanted movers' fragments and the live pins
    name.  ``named`` is what every fragment and live pin names, wanted or
    not, and judges a mover's copy only (:func:`_named_census`); ``None``
    reads as ``attributed``.

    Containment is checked only for a file about to carry an identity
    (#1073).  It guards a deletion and nothing else, and resolving every
    file of the live stage (54,400, 38,422 of them marked and only counted)
    took 48% of the tier loop.  It still runs after the ``lstat`` that
    captures the identity: a directory swapped for a symlink before that
    ``lstat`` is caught here, and one swapped after it changes the identity
    the locked re-check compares, which never re-checks containment.

    ``memo`` is the tier loop's census (:class:`kept_reads.CensusIndex`).  With one, a
    directory whose trusted stamp still holds is not listed again, and a
    file whose version still holds is not probed again: its classification
    from the last pass is reused.  Every entry is still ``lstat``-ed, and
    attribution and pins are re-read by the caller, so a reused listing
    decides nothing by itself.  ``mark_unanswerable`` and a directory that
    cannot be read are never reused -- the next pass probes them again --
    and where no stamp or version can be kept (a network mount, the tick a
    directory changed in) the walk runs as it always did.  Without a memo
    nothing is kept at all.

    Returns ``(candidates, errors)``: each candidate as ``(path, identity,
    kind, size, mover)`` where ``identity`` is the ``lstat`` version the
    caller must see again under the lock, or ``None`` for a file left
    (counted and sized, never deleted); ``kind`` is one of
    :data:`RECONCILE_DELETED_KINDS` or :data:`RECONCILE_LEFT_KINDS`;
    ``size`` is that ``lstat``'s ``st_size``; and ``mover`` is the writer a
    ``mover_residue`` mark names, otherwise ``""``.
    """

    trust = memo.trust if memo is not None else kept_reads.filesystem_trust
    if named is None:
        named = attributed
    candidates: list[tuple[Path, tuple | None, str, int, str]] = []
    errors: list[str] = []
    # ``stage_resolved in resolved.parents``, as a string test: the resolved
    # path lies strictly below the resolved stage.
    inside = os.path.join(str(stage_resolved), "")
    root = os.path.normpath(os.fspath(stage))
    #: A name attribution covered on the last pass.  It is remembered so a
    #: reused listing still sees it when the fragment that covered it ends,
    #: and it carries no mark probe while it stays attributed.
    attributed_mark = "attributed"
    #: A name whose ``lstat`` failed.  It is not classified and not kept; the
    #: directory's listing is refused instead, so the name is tried again
    #: even though the failure did not move the directory's version.
    unreadable = object()

    def classify(name: str, path: str) -> tuple[str, str]:
        if _is_mover_partial(name):
            return "partial", ""
        return _stage_mark(path)

    def judge(name: str, path: str, info: os.stat_result,
              kind: str, mover: str) -> None:
        """The per-file rules, exactly as before (#1088, #1073)."""

        normalized = os.path.normpath(path)
        if kind == "mover_residue" and normalized in named:
            # Some fragment names this copy, wanted or not: its owner is
            # retired with that fragment and its material by the key-driven
            # passes, never from under them.
            return
        if kind in ("source_mark_only", "mark_unanswerable"):
            candidates.append((Path(path), None, kind, int(info.st_size), ""))
            return
        try:
            if not os.path.realpath(path).startswith(inside):
                return
        except OSError as exc:
            errors.append(f"{name}: {exc}")
            return
        candidates.append((Path(path), kept_reads.metadata_version(info), kind,
                           int(info.st_size), mover))

    def skipped_name(directory: str, name: str) -> bool:
        return (directory == root
                and name in (STAGE_ROOT_MARKER, storage_tiers.RAM_EPOCH_MARKER))

    def read_entry(directory: str, name: str, previous: dict | None,
                   fence: int | None,
                   ) -> "tuple[os.stat_result, str, str, tuple | None] | None | object":
        """One name's lstat and classification, reused when its version holds.

        ``unreadable`` for a name that could not be stat-ed: the caller must
        refuse the directory's listing, not drop the name from it.
        """

        path = os.path.join(directory, name)
        try:
            info = os.lstat(path)
            if not statmod.S_ISREG(info.st_mode):
                return None
        except OSError as exc:
            errors.append(f"{name}: {exc}")
            return unreadable
        version = trust.file_version(info, fence, path=path, follow_symlinks=False)
        if os.path.normpath(path) in attributed:
            # Attribution decides first and reads no mark; the name is kept
            # so a later pass that no longer attributes it can judge it.
            return info, attributed_mark, "", version
        old = previous.get(name) if previous is not None else None
        if (old is not None and old[0] is not None and old[0] == version
                and old[1] not in ("mark_unanswerable",
                                   attributed_mark)):
            kind, mover = old[1], old[2]
            if memo is not None:
                memo.walk_reused += 1
        else:
            kind, mover = classify(name, path)
            if memo is not None:
                memo.walk_probed += 1
        judge(name, path, info, kind, mover)
        return info, kind, mover, version

    def fresh(directory: str, previous: dict | None,
              ) -> tuple[list[str], dict[str, tuple], bool] | None:
        """List and classify one stage directory; the caller keeps the memo.

        ``None`` for a directory that cannot be read: ``os.walk`` skipped it
        silently, and unknown ownership is never cached or recursed into.
        The third answer says a name could not be stat-ed, so the directory
        must be listed again next pass.
        """

        children: list[str] = []
        entries: dict[str, tuple] = {}
        retry = False
        fence = trust.file_fence()
        try:
            with os.scandir(directory) as scanned:
                listed = list(scanned)
        except OSError:
            return None
        names: list[str] = []
        for entry in listed:
            try:
                is_dir = entry.is_dir()
            except OSError:
                is_dir = False
            if is_dir:
                if not entry.is_symlink():
                    children.append(entry.path)
            else:
                names.append(entry.name)
        for name in sorted(names):
            if skipped_name(directory, name):
                continue
            read = read_entry(directory, name, previous, fence)
            if read is None:
                continue
            if read is unreadable:
                retry = True
                continue
            _info, kind, mover, version = read
            entries[name] = (version, kind, mover)
        return children, entries, retry

    def visit(directory: str) -> None:
        with (memo.stage_walk.acquire(directory) if memo is not None else nullcontext()) as capture:
            visit_capture(directory, capture)

    def visit_capture(directory, capture):
        if capture is not None and capture.hit:
            memo.walk_kept += 1
            children, entries = capture.value
            refreshed: dict[str, tuple] = {}
            retry = False
            fence = trust.file_fence()
            for name, (version, kind, mover) in entries.items():
                if skipped_name(directory, name):
                    continue
                path = os.path.join(directory, name)
                try:
                    info = os.lstat(path)
                    if not statmod.S_ISREG(info.st_mode):
                        continue
                except OSError as exc:
                    errors.append(f"{name}: {exc}")
                    retry = True
                    continue
                current_version = trust.file_version(
                    info, fence, path=path, follow_symlinks=False)
                normalized = os.path.normpath(path)
                if kind == attributed_mark:
                    if normalized in attributed:
                        refreshed[name] = (current_version, kind, mover)
                        continue
                    # Attribution ended: judge the file on this pass.
                    kind, mover = classify(name, path)
                    if memo is not None:
                        memo.walk_probed += 1
                    version = current_version
                    refreshed[name] = (version, kind, mover)
                    judge(name, path, info, kind, mover)
                    continue
                if normalized in attributed:
                    refreshed[name] = (None, attributed_mark, "")
                    continue
                if (version is not None
                        and current_version == version
                        and kind != "mark_unanswerable"):
                    if memo is not None:
                        memo.walk_reused += 1
                else:
                    kind, mover = classify(name, path)
                    if memo is not None:
                        memo.walk_probed += 1
                    version = current_version
                refreshed[name] = (version, kind, mover)
                judge(name, path, info, kind, mover)
            payload = (children, refreshed)
            capture.commit(payload, complete=not retry, candidates=payload)
        else:
            previous_kept = capture.previous if capture is not None else kept_reads.NoCapture
            previous = previous_kept[1] if previous_kept is not kept_reads.NoCapture else None
            previous_children = previous_kept[0] if previous_kept is not kept_reads.NoCapture else ()
            scanned = fresh(directory, previous)
            if scanned is None:
                if memo is not None:
                    memo.forget_stage(directory)
                return
            children, entries, retry = scanned
            if memo is not None:
                memo.walk_listed += 1
                for gone in set(previous_children) - set(children):
                    memo.forget_stage(gone)
                payload = (tuple(children), entries)
                capture.commit(payload, complete=not retry, candidates=payload)
        for child in children:
            visit(child)

    visit(root)
    return candidates, errors

def _scope_for_range(cas_root: str, manifest_sha256: str,
                     start: int, end: int,
                     ) -> tuple[set[str], list[dict[str, object]], str] | None:
    """The staged names one historical range covers, by the usual mapping.

    The same derivation ``_claimed_paths`` runs for a claim in flight, driven
    from a pinned manifest digest and range instead of a sealed claim: the
    manifest is immutable under its digest, so the names it yields are a fact
    about that request and not about anything currently on the tier.  Returns
    ``None`` when the manifest or the window cannot be read, which the caller
    turns into a refusal -- an undeterminable scope never reads as empty.
    """

    layout = manifest_layout_for(cas_root, manifest_sha256)
    if layout is None:
        return None
    mount_prefix, entries = layout
    try:
        window = manifest_layout.entries_between(entries, start, end)
    except (ValueError, TypeError):
        return None
    names: set[str] = set()
    for entry in window:
        path, offset = str(entry["path"]), int(entry["offset"])
        try:
            names.add(stage_relative(
                path, offset, int(entry["bytes"]),
                mount_prefix=mount_prefix))
        except ValueError:
            continue
    return names, list(window), mount_prefix


def bounded_int(value: object) -> int | None:
    """A nonnegative integer that is not a bool, or ``None``."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _move_receipt(queue: pool.PoolQueue, action_key: str, *,
                  schemas: tuple[str, ...] = (pool.POOL_MOVE_SCHEMA_V1,),
                  ) -> tuple[dict[str, object] | None, str]:
    """One filed move receipt, or why it cannot be evidence.

    ``schemas`` names the receipts that count.  An egress's receipt is
    filed as ``pool.POOL_EGRESS_SCHEMA_V1`` since #1158 and as
    ``pool.POOL_MOVE_SCHEMA_V1`` before it, so the egress evidence reads
    either.

    The receipt is the positive terminal evidence this repair turns on.  An
    absent one is not "finished": it is no answer, and no answer refuses.
    """

    try:
        record = pool.record_store.read_document(queue.records.move_path(action_key))
    except (OSError, pool.PoolContractError) as exc:
        return None, f"{action_key[:12]}: receipt unreadable: {exc}"
    if not isinstance(record, Mapping):
        return None, f"{action_key[:12]}: no filed move receipt"
    if str(record.get("action_key") or "") != action_key:
        return None, f"{action_key[:12]}: receipt is filed under another " \
                     f"action's key"
    if record.get("schema") not in schemas:
        return None, f"{action_key[:12]}: receipt is not a pool move record"
    if record.get("complete") is not True:
        return None, f"{action_key[:12]}: receipt does not report complete"
    return dict(record), ""


def _sealed_command_flags(
        sealed: Mapping[str, object], *,
        required: tuple[str, ...],
        forbidden: tuple[str, ...]) -> tuple[dict[str, str] | None, str]:
    """Read named flags out of a sealed request's argv, as list elements.

    The command is read positionally and never re-parsed as a shell string.
    Each named flag must appear exactly once: these requests are built by a
    tool, so a duplicate is a real signal about how this one was built, not a
    quirk to tolerate.  Only the named values are returned, so no caller can
    print the whole argv.
    """

    params = sealed.get("params")
    if not isinstance(params, Mapping):
        return None, "carries no params"
    command = params.get("command")
    if not isinstance(command, list) or not command:
        return None, "carries no command list"
    if not all(isinstance(one, str) for one in command):
        return None, "command is not a list of strings"
    out: dict[str, str] = {}
    for flag in tuple(required) + tuple(forbidden):
        seen = [i for i, one in enumerate(command) if one == flag]
        if len(seen) > 1:
            return None, f"names {flag} {len(seen)} times"
        if flag in forbidden:
            if seen:
                return None, f"names {flag}, which carries no authority here"
            continue
        if not seen:
            return None, f"names no {flag}"
        after = seen[0] + 1
        if after >= len(command):
            return None, f"{flag} names no value"
        out[flag] = command[after]
    return out, ""


def validated_sealed_request(
        cas_root: str, action_key: str,
        ) -> tuple[dict[str, object] | None, str]:
    """A historical CAS request proven to be the action asked for.

    ``prewarm_loop.sealed_request`` is a bare JSON load: any bytes at that
    path answer for the sealed request, including a hand-edited body wearing
    the key it is read under.  This repair treats the request's flags as the
    authority for a destructive scope, so it applies the same sequence the
    pool applies to a claimed action -- stable read, ``validate_action``
    over the whole v2 contract, and the recorded key equal to the key asked
    for -- and no bytes are re-hashed beyond that: the action key is the
    digest work, and no payload is touched.
    """

    path = (Path(cas_root) / "requests" / action_key[:2]
            / f"{action_key}.json")
    try:
        raw = pb.immutable_files.read_stable_file(path, where="orphan recovery action request")
        action = pb.validate_action(
            pb.digest_primitives.decode_strict_json(raw, where="orphan recovery action request"),
        )
    except FileNotFoundError:
        return None, f"{action_key[:12]}: not sealed in the CAS"
    except (OSError, ValueError, pb.PrismaBuildError) as exc:
        return None, f"{action_key[:12]}: not a validly sealed action: {exc}"
    if str(action.get("action_key") or "") != action_key:
        return None, f"{action_key[:12]}: sealed for another action"
    return action, ""


def recover_orphaned_range(
        queue: pool.PoolQueue, *, stage_root: str,
        head_action_key: str, egress_action_key: str,
        cas_root: str | Path | None = None,
        residency_root: str | Path | None = None,
        apply: bool = False, store=None) -> dict[str, object]:
    """Retire the cache copies of one retired head that nothing can prove.

    Routine ``reconcile`` structurally cannot reach these when they were
    published before #1088.  It decides ownership from the
    ``user.pbstage.source`` mark, and ``stage_move`` set that same mark, and
    no other, on every file it published, so such a stage copy reads as a
    prewarm object and lands in ``unowned_left`` for the life of the fleet.
    (A copy published since also names its writer, and ``reconcile`` takes it
    back once no fragment names it and that writer has ended.)  A head whose
    fragment and material are gone therefore leaves bytes no document proves
    and no sweep may touch, and every later head pays the publisher grace
    once per entry for them.

    Scope is bound to history, not to the caller.  The only identities taken
    are the head's own filed move receipt and the egress receipt that retired
    it; the tier, the stage root, the consumer, the manifest and the exact
    range are **read off those receipts**, so there is no caller-supplied
    digest or window to get wrong.  Both must be filed, well-formed and
    ``complete``: absence is not terminal evidence, it is no evidence.  Each
    historical request is then read as an action and validated under its own
    key (``validate_action`` plus the key comparison) before anything is
    derived from it -- a bare JSON load would let any bytes at that path
    answer for the sealed request -- and the surviving flag checks bind the
    two requests to each other and to the receipts: the egress must name the
    head as its mover, both must name the same consumer and stage root, the
    head alone carries tier and range, and a filed receipt may not widen the
    window its request authorized.

    Permission is that identity-bound scope **plus** the positive absence of
    every other ownership, established fresh under the lock the egress holds.
    The old source mark is never permission -- only a necessary condition, so
    an unmarked or unanswerable file is retained.  Every fragment retains,
    wanted or not -- a fragment that cannot be read or validated refuses the
    whole pass instead of reading as absent, and a withdrawn mover's fragment
    retains like any other; live pins, claims in flight and promotion source
    handoffs retain; and any census that cannot be read **refuses the pass**
    rather than reading as absence.

    Originals are proven before anything is destroyed: every entry the window
    names must still exist at its source path as a **regular file** that
    holds at least the ``offset + bytes`` extent the manifest gives it, and
    that path must resolve outside the stage root, so a staged copy is never
    the last surviving input and a directory or a shrunken source is not an
    input at all.  Ambiguity retains, and any refused or unreadable entry
    fails the whole pass -- over-retaining is a pass and over-removing is a
    failure.

    ``apply`` is false by default: the pass reports what it *would* retire and
    changes nothing.
    """
    store = queue.records if store is None else store

    stage = Path(stage_root)
    receipt: dict[str, object] = {
        "schema": pool.POOL_EGRESS_SCHEMA_V1,
        "event": ORPHAN_RECOVERY_EVENT,
        "action_key": "",
        "head_action_key": head_action_key,
        "egress_action_key": egress_action_key,
        "consumer_action_key": "",
        "tier_id": "",
        "stage_root": str(stage),
        "reason": "orphan-recovery",
        "applied": bool(apply),
        "scope_entries": 0,
        "entries_eligible": 0,
        "entries_retired": 0,
        "entries_retained": 0,
        "entries_already_gone": 0,
        "entries_refused": 0,
        "bytes_eligible": 0,
        "bytes_retired": 0,
        "retained_reasons": {},
        "originals_checked": 0,
        "originals_present": 0,
        "complete": True,
        "errors": [],
        "host": socket.gethostname(),
        "unix": time.time(),
    }

    def refuse(why: str) -> dict[str, object]:
        receipt["skipped"] = why
        receipt["complete"] = False
        errs = list(receipt["errors"])
        errs.append(why)
        receipt["errors"] = errs
        return receipt

    # Whose stage this is comes before anything else on it (#628): a root
    # that does not belong to this queue is refused before its receipts are
    # even read.
    refusal = stage_root_refusal(queue, stage)
    if refusal is not None:
        receipt["event"] = STAGE_ROOT_REFUSED_EVENT
        return refuse(refusal)

    head, why = _move_receipt(queue, head_action_key)
    if head is None:
        return refuse(f"head evidence refused: {why}")
    egress, why = _move_receipt(queue, egress_action_key,
                                schemas=pool.POOL_MOVEMENT_RECEIPT_SCHEMAS)
    if egress is None:
        return refuse(f"egress evidence refused: {why}")
    if egress.get("reason") != "egress":
        return refuse(
            f"{egress_action_key[:12]} is not an egress receipt")

    consumer = str(head.get("consumer_action_key") or "")
    tier_id = str(head.get("tier_id") or "")
    manifest_sha256 = str(head.get("manifest_sha256") or "")
    start = bounded_int(head.get("range_start_bytes"))
    end = bounded_int(head.get("range_end_bytes"))
    if not consumer or not tier_id or not manifest_sha256:
        return refuse("the head receipt names no consumer, tier or manifest")
    if start is None or end is None or end <= start:
        return refuse("the head receipt names no usable byte range")
    if str(egress.get("consumer_action_key") or "") != consumer:
        return refuse("the egress retired a different consumer's window")
    for label, record in (("head", head), ("egress", egress)):
        named = str(record.get("stage_root") or "")
        if named and os.path.normpath(named) != os.path.normpath(str(stage)):
            return refuse(
                f"the {label} receipt names stage root {named}, not {stage}")
    own_cas_early = (str(cas_root) if cas_root is not None
                     else str(queue.root.parent / "cas"))
    flags: dict[str, dict[str, str]] = {}
    for label, key, want, deny in (
            ("head", head_action_key,
             ("--consumer-action-key", "--tier-id", "--stage-root",
              "--range-start-bytes", "--range-end-bytes"),
             ("--mover-action-key",)),
            ("egress", egress_action_key,
             ("--mover-action-key", "--consumer-action-key", "--stage-root"),
             ("--tier-id", "--range-start-bytes", "--range-end-bytes"))):
        sealed, why = validated_sealed_request(own_cas_early, key)
        if sealed is None:
            return refuse(f"the {label} request {why}; its scope cannot "
                          f"be bound")
        params = sealed.get("params")
        declared = (params.get("data_manifest")
                    if isinstance(params, Mapping) else None)
        named = (declared.get("input") if isinstance(declared, Mapping)
                 else None)
        digest = (named.get("sha256") if isinstance(named, Mapping) else None)
        if digest != manifest_sha256:
            return refuse(
                f"the sealed {label} request names manifest "
                f"{str(digest)[:12]}, not the {manifest_sha256[:12]} its "
                f"receipt recorded")
        # Manifest equality is far too weak on its own.  Both phases of one
        # campaign share a digest, so it would admit the whole manifest
        # against authority for one phase's window.  The flags are what
        # actually scope this.
        got, why = _sealed_command_flags(sealed, required=want, forbidden=deny)
        if got is None:
            return refuse(f"the sealed {label} request {why}")
        flags[label] = got

    if flags["egress"]["--mover-action-key"] != head_action_key:
        return refuse(
            f"the sealed egress request retires mover "
            f"{flags['egress']['--mover-action-key'][:12]}, not the head "
            f"{head_action_key[:12]} this recovery is scoped to")
    for label in ("head", "egress"):
        if flags[label]["--consumer-action-key"] != consumer:
            return refuse(
                f"the sealed {label} request names consumer "
                f"{flags[label]['--consumer-action-key'][:12]}, not the "
                f"{consumer[:12]} its receipt recorded")
        named = flags[label]["--stage-root"]
        if os.path.normpath(named) != os.path.normpath(str(stage)):
            return refuse(
                f"the sealed {label} request names stage root {named}, "
                f"not {stage}")
    if flags["head"]["--tier-id"] != tier_id:
        return refuse(
            f"the sealed head request names tier {flags['head']['--tier-id']},"
            f" not the {tier_id} its receipt recorded")
    try:
        sealed_start = int(flags["head"]["--range-start-bytes"])
        sealed_end = int(flags["head"]["--range-end-bytes"])
    except ValueError:
        return refuse("the sealed head request names an unreadable range")
    if sealed_start < 0 or sealed_end <= sealed_start:
        return refuse("the sealed head request names no usable byte range")
    if (sealed_start, sealed_end) != (start, end):
        return refuse(
            f"the head receipt claims {start}..{end} but its sealed request "
            f"authorizes only {sealed_start}..{sealed_end}")
    # Equal by the check above; take them from the sealed request anyway, so
    # the authorized window is the one a filed receipt cannot widen.
    start, end = sealed_start, sealed_end
    receipt["scope_authority"] = "sealed_head_request"
    receipt["consumer_action_key"] = consumer
    receipt["tier_id"] = tier_id
    receipt["manifest_sha256"] = manifest_sha256
    receipt["range_start_bytes"] = start
    receipt["range_end_bytes"] = end

    own_cas = (str(cas_root) if cas_root is not None
               else str(queue.root.parent / "cas"))

    def _scope_outside_the_lock() -> dict[str, object]:
        """The range's scope and its originals, which no lock guards (#988).

        The manifest window, the staged names it derives and the originals
        it proves recapturable are read from immutable CAS records and from
        source files outside the stage.  None of it is state the stage
        ownership lock orders, and the originals are stats on the shared
        mount, one or more NFS round trips per entry, so they no longer run
        under it.  A refusal found here is returned by the pass under the
        lock, after its own head, consumer and mover checks, so the order
        in which refusals are reported is unchanged.
        """

        fields: dict[str, object] = {}

        def scoped(why: str) -> dict[str, object]:
            return {"refusal": why, "fields": fields}

        try:
            stage_resolved = stage.resolve(strict=True)
        except OSError as exc:
            return scoped(f"stage_root_unreadable: {exc}")

        layout = manifest_layout_for(own_cas, manifest_sha256)
        if layout is None:
            return scoped(
                f"manifest {manifest_sha256[:12]} unreadable; scope "
                f"undeterminable")
        mount_prefix, entries = layout
        try:
            window = manifest_layout.entries_between(entries, start, end)
        except (ValueError, TypeError) as exc:
            return scoped(f"range not cuttable: {exc}")
        if not window:
            return scoped("the recorded range covers no manifest entry")
        # ``entries_between`` includes a straddling entry whole, so the
        # window is a cover of the range and not a partition of it: it may
        # exceed the span, and must never fall short of it.  The manifest is
        # the authority for what the range contains -- a manifest-wide
        # ``entry_count`` is a different number and is not interchangeable
        # with the entries one range covers.
        covered = sum(int(one.get("bytes", 0)) for one in window)
        if covered < end - start:
            return scoped(
                f"the window covers {covered} bytes, short of the "
                f"{end - start} the head recorded; scope is not the "
                f"recorded range")
        staged = bounded_int(head.get("entries_staged"))
        if staged is not None and staged != len(window):
            return scoped(
                f"the window holds {len(window)} entries, not the {staged} "
                f"the head recorded staging; scope is not that window")
        names: dict[str, dict[str, object]] = {}
        #: The bare name a head from before range-only naming may have
        #: written instead, per derived name.  Never deleted here: a bare
        #: name was shared by every read of that path from offset zero, so it
        #: is not this head's identity.  Found, it is retained and named in
        #: the receipt rather than counted as already gone.
        pre_range: dict[str, str] = {}
        named_once = paths_named_once(entries)
        for entry in window:
            source, offset = str(entry["path"]), int(entry["offset"])
            try:
                relative = stage_relative(
                    source, offset, int(entry["bytes"]),
                    mount_prefix=mount_prefix)
                legacy = pre_range_stage_relative(
                    source, offset, int(entry["bytes"]),
                    mount_prefix=mount_prefix,
                    named_once=source in named_once)
            except ValueError as exc:
                # A name that cannot be derived shrinks the scope silently if
                # it is skipped, and a partial scope is a different question
                # from the one the receipt asked.
                return scoped(f"{source}: staged name underivable: {exc}")
            names[relative] = entry
            if legacy != relative:
                pre_range[relative] = legacy
        fields["scope_entries"] = len(names)

        # Originals first: nothing is destroyed before the inputs they stand
        # for are proven to survive it.  ``exists`` is not that proof -- a
        # directory exists, and a source shrunken below the extent its window
        # covers exists too -- so each original must be a regular file that
        # still holds the whole extent ``offset + bytes`` the manifest names,
        # resolved outside the stage root.  Size, never a digest: proving
        # recapturability is a stat, not a model-sized hash.
        checked = present = 0
        for entry in window:
            source = str(entry.get("path") or "")
            if not source:
                return scoped("a window entry names no source path")
            offset = bounded_int(entry.get("offset"))
            span = bounded_int(entry.get("bytes"))
            if offset is None or span is None:
                return scoped(
                    f"{source}: window entry names an unusable extent")
            checked += 1
            try:
                original = Path(source)
                if not original.exists():
                    return scoped(
                        f"original missing for {source}; the staged copy may "
                        f"be the last surviving input")
                if stage_resolved in original.resolve().parents:
                    return scoped(
                        f"original {source} resolves inside the stage root; "
                        f"it is not a separate input")
                if not original.is_file():
                    return scoped(
                        f"original {source} is not a regular source file")
                size = original.stat().st_size
            except OSError as exc:
                return scoped(f"original {source} unreadable: {exc}")
            if size < offset + span:
                return scoped(
                    f"original {source} holds {size} bytes, short of the "
                    f"{offset + span} its window covers; the staged copy is "
                    f"not proven recapturable")
            present += 1
        fields["originals_checked"] = checked
        fields["originals_present"] = present

        return {"refusal": "", "fields": fields,
                "stage_resolved": stage_resolved,
                "mount_prefix": mount_prefix, "names": names,
                "pre_range": pre_range}

    scope = _scope_outside_the_lock()
    memo = kept_reads.CensusMemo()
    census_started = time.perf_counter()
    if not scope["refusal"]:
        # The censuses once, as hints that fill the memo; the pass under
        # the lock takes them again and acts only on that.
        hint_paths = {os.path.normpath(str(stage / relative))
                      for relative in scope["names"]}
        fragment_owners(Path(residency_root if residency_root is not None
                              else queue.root / pool.record_store.RESIDENCY),
                         hint_paths, memo=memo)
        reader_lease.live_for(queue, None, residency_root=residency_root,
                              memo=memo.pins)
        claimed_paths(queue, tier_id, own_cas, memo=memo)
        claimed_source_paths(queue, stage, own_cas, memo=memo)
    census_s = time.perf_counter() - census_started
    emptied: set[Path] = set()

    def _held_recover() -> dict[str, object]:
        """The ownership decision and the act, under the stage lock."""

        for key, label in ((head_action_key, "head"), (consumer, "consumer")):
            for state in (pool.record_store.READY, pool.record_store.CLAIMED):
                try:
                    if pool.record_store.value_or_raise(queue.records.item_presence(state, key)):
                        return refuse(
                            f"{label} {key[:12]} is still {state}; recovery "
                            f"acts only on a finished request")
                except OSError as exc:
                    return refuse(f"{label} {key[:12]} unreadable: {exc}")
        in_flight = movers_in_flight(queue, tier_id=tier_id, store=store)
        if in_flight:
            receipt["movers_in_flight"] = sorted(in_flight)
            return refuse("movers_in_flight")
        receipt.update(scope["fields"])
        if scope["refusal"]:
            return refuse(str(scope["refusal"]))
        stage_resolved = scope["stage_resolved"]
        mount_prefix = scope["mount_prefix"]
        names = scope["names"]
        pre_range = scope["pre_range"]
        # Every fragment retains, wanted or not: the question here is not
        # whose bytes these are but whether anything at all still names them.
        # ``_fragment_owners`` -- not ``attributed_stage_paths`` -- because
        # the repair's half of the question is answered against the exact
        # staged-path set this scope derived, and because the map-composition
        # reader deliberately skips a fragment that cannot be read or
        # validated so a consumer still finds its other movers' copies.
        # Readiness must not reuse that tolerance: skipped is how unknown
        # collapses into unowned, and unowned is what deletes.  A taint
        # refuses the whole pass, the same fail-closed rule the egress
        # applies to its own fragment.  No mover is excepted, so a withdrawn
        # mover's fragment retains too -- one more kept file is a pass.
        fragment_root = Path(residency_root if residency_root is not None
                             else queue.root / pool.record_store.RESIDENCY)
        scope_paths = {os.path.normpath(str(stage / relative))
                       for relative in names}
        parse_counts = memo.counts()
        fragment_mentions, fragment_taint = fragment_owners(
            fragment_root, scope_paths, memo=memo)
        if fragment_taint:
            return refuse(f"fragment census unreadable: "
                          f"{'; '.join(fragment_taint[:3])}")
        attributed = set(fragment_mentions)
        pin_owners, pin_taint = reader_lease.live_for(
            queue, None, residency_root=residency_root, memo=memo.pins)
        if pin_taint:
            return refuse(f"pin census unreadable: {'; '.join(pin_taint[:3])}")
        attributed |= set(pin_owners)
        claimed, claim_taint = claimed_paths(queue, tier_id, own_cas,
                                              memo=memo)
        if claim_taint:
            return refuse(
                f"claim census unreadable: {'; '.join(claim_taint[:3])}")
        attributed |= {os.path.normpath(str(stage / one)) for one in claimed}
        handoffs, handoff_taint = claimed_source_paths(
            queue, stage, own_cas, memo=memo)
        if handoff_taint:
            return refuse(
                f"promotion handoff census unreadable: "
                f"{'; '.join(handoff_taint[:3])}")
        attributed |= {os.path.normpath(str(one)) for one in handoffs}
        receipt.update(locked_parse_record(memo, parse_counts))

        retained: dict[str, int] = {}

        def retain(why: str) -> None:
            retained[why] = retained.get(why, 0) + 1

        eligible: list[tuple[Path, int]] = []
        already_gone = 0
        for relative in sorted(names):
            path = stage / relative
            try:
                if not path.exists():
                    legacy = pre_range.get(relative)
                    if legacy is not None and os.path.lexists(stage / legacy):
                        retain("pre_range_name_not_this_heads_identity")
                    else:
                        already_gone += 1
                    continue
                if path.is_symlink() or not path.is_file():
                    retain("not_a_regular_file")
                    continue
                if stage_resolved not in path.resolve().parents:
                    retain("outside_the_owned_stage_root")
                    continue
                size = path.stat().st_size
            except OSError as exc:
                # An entry nothing can read is not an entry proven unowned.
                return refuse(f"{relative}: {exc}")
            if os.path.normpath(str(path)) in attributed:
                retain("attributed_pinned_claimed_or_handed_off")
                continue
            if _marked_by_the_prewarm_stage(path) is not True:
                retain("not_marked_by_the_stage")
                continue
            eligible.append((path, int(size)))

        receipt["entries_eligible"] = len(eligible)
        receipt["bytes_eligible"] = sum(size for _path, size in eligible)
        receipt["entries_retained"] = sum(retained.values())
        receipt["entries_already_gone"] = already_gone
        receipt["retained_reasons"] = dict(sorted(retained.items()))
        receipt["mount_prefix"] = mount_prefix
        if not apply:
            return receipt

        retired = bytes_retired = 0
        errors: list[str] = []
        for path, size in eligible:
            try:
                os.unlink(path)
            except FileNotFoundError:
                already_gone += 1
                continue
            except OSError as exc:
                errors.append(f"{path.name}: {exc}")
                continue
            retired += 1
            bytes_retired += size
            emptied.add(path.parent)
        receipt["entries_retired"] = retired
        receipt["bytes_retired"] = bytes_retired
        receipt["entries_already_gone"] = already_gone
        receipt["entries_refused"] = len(errors)
        receipt["errors"] = errors
        receipt["complete"] = not errors
        return receipt

    # The per-entry classification and the unlinks stay in one hold.  This
    # is an operator's repair, run by hand through the CLI and never by a
    # tier cycle, and each entry's ``exists``/``resolve``/mark reads are the
    # evidence its unlink stands on; what left the hold is the scope, the
    # originals and the census parse (above) and the directory prune.
    asked = time.perf_counter()
    with ownership(queue, stage,
                          role="recover-orphaned-range") as granted:
        result = _held_recover()
    released = time.perf_counter()
    pruned_started = time.perf_counter()
    for parent in sorted(emptied, key=lambda one: len(one.parts), reverse=True):
        prune_empty(parent, stage)
    result.update(hold_record(
        lock_wait_s=granted - asked, lock_held_s=released - granted,
        census_s=census_s, prune_s=time.perf_counter() - pruned_started))
    return result


def movers_in_flight(queue: pool.PoolQueue, *, tier_id: str, store=None) -> set[str]:
    """Mover keys ready or claimed on this tier, i.e. copies that may be writing.

    A mover row is the one whose residency block names a *range*; a consumer's
    names leads.  Ready as well as claimed, because a ready mover can be
    claimed between a directory walk and an unlink.
    """
    store = queue.records if store is None else store

    out: set[str] = set()
    for state in (pool.record_store.READY, pool.record_store.CLAIMED):
        for path, item in store.state_records(state):
            residency = item.get("residency") if isinstance(item, dict) else None
            if not isinstance(residency, dict):
                continue
            if "range_start_bytes" not in residency:
                continue
            if str(residency.get("tier_id") or "") != tier_id:
                continue
            key = path.name[:-len(".json")] if path.name.endswith(".json") else path.name
            out.add(key)
    return out


#: Every staged object lives under this suffix, named by the exact range it
#: holds, so the name of one range can never be the name of another.
RANGE_SUFFIX = ".pbrange"


def stage_relative(path: str, offset: int, size: int, *, mount_prefix: str,
                   namespace: str | None = None,
                   named_once: bool = False) -> str:
    """Where one manifest entry's bytes live under the stage root.

    **Staged input.**  Every entry is named by the exact range it holds:
    ``<rel>.pbrange/<offset>-<size>``.  Two entries share a staged name
    exactly when they name the same bytes of the same source, which is
    exactly when sharing one staged object is correct, and the staged
    object's length is always the range's length, so the consumer reads it
    from position 0.  ``named_once`` is ignored for staged input.

    **Produced output** (``namespace`` given) keeps the name its producer
    declared: ``produced-output/<namespace>/<rel>`` for a path its manifest
    names once from offset zero (``named_once``), and a range name otherwise.
    The namespace is the producing action's own digest, so no other
    publisher's read can derive a name inside it and the declared name
    cannot collide across consumers.

    Staged input deliberately has no bare-name branch.  Its name is a pure
    function of the sealed manifest entry (``path``, ``offset``, ``bytes``),
    and a manifest entry carries no file size, so "this entry covers the
    whole file" is not derivable from what the name may depend on.  The
    former rule -- a path the manifest names exactly once, from offset zero,
    keeps its bare name -- took "named once from zero" for "whole": a routing
    capture that read each safetensors shard's 45 KB header and a consumer
    that read the whole 5.37 GB shard derived one name for two different
    objects, and whichever published second refused after the grace, for as
    long as the first publication stood (GLM Stage A R11, 2026-09-22: 74
    shard names across 40 read phases).  :func:`pre_range_stage_relative`
    keeps the former spelling for the retention censuses only.

    The staged-input derivation is injective: the last component is
    ``<offset>-<size>``, digits only, so the relative path and the range are
    both recoverable from the name, and a declared path that itself ends in
    ``.pbrange/<a>-<b>`` gets a suffix of its own rather than landing on
    another entry's range.
    """

    relative = path[len(mount_prefix.rstrip("/")) + 1:]
    if not relative or relative.startswith("/"):
        raise ValueError(f"{path!r} is not inside {mount_prefix!r}")
    if namespace is not None:
        if (len(namespace) != 64
                or any(c not in "0123456789abcdef" for c in namespace)):
            raise ValueError("produced stage namespace must be a 64-character digest")
        relative = f"produced-output/{namespace}/{relative}"
        if named_once and offset == 0:
            return relative
    return f"{relative}{RANGE_SUFFIX}/{offset}-{size}"


def paths_named_once(entries: list[dict[str, object]]) -> set[str]:
    """Paths the manifest names exactly once.

    Decides a produced output's declared name (:func:`stage_relative`) and
    the pre-range spelling of a staged input
    (:func:`pre_range_stage_relative`); it never says a read covers a file.
    """

    counts: dict[str, int] = {}
    for entry in entries:
        path = str(entry["path"])
        counts[path] = counts.get(path, 0) + 1
    return {path for path, count in counts.items() if count == 1}


def pre_range_stage_relative(path: str, offset: int, size: int, *,
                             mount_prefix: str, named_once: bool,
                             namespace: str | None = None) -> str:
    """The name a mover of a generation before range-only naming derived.

    Only the retention censuses read this.  A plan row keeps the tool path of
    the generation that sealed it, so a mover published after a runtime
    publication can still be an older mover writing the bare name for a path
    its manifest names once from offset zero.  A census that decides what
    another publisher may be writing therefore counts both spellings:
    over-retaining is a pass and over-removing is a failure.  Nothing may
    *publish* or *delete* by this name.
    """

    ranged = stage_relative(path, offset, size, mount_prefix=mount_prefix,
                            namespace=namespace, named_once=named_once)
    if namespace is None and named_once and offset == 0:
        return ranged[:ranged.rindex(RANGE_SUFFIX + "/")]
    return ranged


#: Grace for a late fragment or an in-flight publisher before an unproven
#: staged name settles to refuse-or-heal.  Every wait sleeps outside the
#: ownership lock in short polls; nothing about the timeout grants
#: permission -- expiry re-verifies, and only re-verified positive absence
#: heals.  Covers the incremental fragment rate limit (FRAGMENT_PUBLISH_S)
#: with wide margin; a publisher slower than this still converges, through
#: the stall policy's retry, never through replacement.  A name under
#: positive absence whose bytes hash to the trusted digest is adopted at
#: once and never waits for it (#1081, :meth:`StagedPublisher._content_proof`).
_PUBLISH_GRACE_S = 30.0
_PUBLISH_POLL_S = 0.25

#: The four ways :meth:`StagedPublisher._decide` can make a publication wait
#: for another publisher, spelled exactly as :meth:`StagedPublisher.publish`
#: totals them onto the receipt's ``publish_waits`` (#994): a fragment vouches
#: for the name without a usable date, a live mover claim covers it, a
#: sibling copy is in flight, or nothing names it yet and it has not been
#: re-verified absent.  Before #994 a mover could sit in these waits, refuse
#: after the 30 s grace, and leave a receipt with no per-entry count, no
#: seconds and no verdict -- only the free-text refusal, capped at 20 entries
#: and easily buried by later, unrelated errors (#853).
PUBLISH_WAIT_OWNED = "owned"
PUBLISH_WAIT_LIVE_PUBLISHER = "live_publisher"
PUBLISH_WAIT_IN_FLIGHT = "in_flight"
PUBLISH_WAIT_UNATTRIBUTED = "unattributed"

#: Residency subdirectories that never hold consumer fragments.
_NON_FRAGMENT_DIRS = frozenset({"leases", "material"})

#: Byte ceiling for one publisher's lookup reuse (#761, #778), and the
#: conservative sizes it is measured in.  A count of anything -- references,
#: records, paths -- bounds only the thing it counts, so this is bytes, every
#: retained row is priced, and each price is charged ONCE at insert from the
#: actual object: the real packed path bytes, the real mention records, the
#: real per-record overhead.  Nothing is free, including a malformed or empty
#: record, because a row that costs nothing is a row that can be added
#: without limit.
#:
#: What is retained is a packed projection of exactly what the decision
#: reads -- normalized paths in one bytes blob with 8-byte end offsets and
#: 8-byte hash keys, and one fixed 72-byte record per validated ordered
#: mention -- rather than the parsed Python object graph of #761, whose
#: interned strings, set and dict slots, digest strings and identity dicts
#: can refuse retention once a document's object graph is oversized; every
#: later adoption of a destination that document names then re-reads and
#: re-decodes the whole document, which is the amplification #778
#: reproduced at fixture scale (the live forest's exact size and multiplier
#: were not measured).
#:
#: Each figure below is an over-estimate of the retained bytes on a 64-bit
#: CPython build, so :meth:`_index_bytes` never reports under the truth.
#: ``_STR_CHAR_BYTES`` is the UCS-4 width, which is what a non-Latin-1 path
#: actually costs and four times what an ASCII one does: erring high is the
#: point, and it means the few strings still retained whole -- a record's
#: file key and its mover key -- are charged what they are rather than a flat
#: guess.  A packed table's own storage is measured with ``sys.getsizeof``,
#: which reports the array's allocated capacity rather than only its used
#: slots, so growth headroom is charged too; that is a real allocation and
#: it scales with the path count, so it cannot be papered over with a larger
#: constant.
#:
#: Path tables that carry the same paths are shared: one actual table is
#: interned per packed identity ``(blob, ends)``, fragments and sidecars that
#: name the same destinations reference that one table, and it is charged
#: once.  ``_reclaim`` rebuilds the interned set from the live records and
#: recomputes that charge from the real packed bytes, the one accounting that
#: cannot drift.
#:
#: This prices the RETAINED index only.  The transient cost of decoding one
#: 22 MB fragment is a peak that is freed before the next lookup; it belongs
#: to the process's peak RSS, not here, and the two are reported separately.
#:
#: Past the ceiling a record is not retained: that file answers uncached,
#: which costs what it cost before this existed and is never a different
#: answer.  Because every charge is exact and travels with its entry, a
#: refusal is never permanent -- it lifts as soon as the index has room.
_INDEX_BUDGET_BYTES = 192 << 20
_STR_HEADER_BYTES = 64
_STR_CHAR_BYTES = 4
_RECORD_OVERHEAD_BYTES = 512
#: One packed mention record: ``<Q32s4Q`` -- size, raw sha256, identity.
_PACKED_MENTION_BYTES = 72
#: What one retained table owns beyond its own components: the two-tuple it
#: is keyed by and its slot in the interned dictionary.  Charged once per
#: table and only in the interned charge, never again in a record's.
_INTERN_TABLE_BYTES = 224
#: The unsigned width the 64-bit path hash is normalized to before it is
#: stored and bisected; the exact bytes comparison below decides membership.
_HASH_MASK = (1 << 64) - 1
_MENTION_STRUCT = struct.Struct("<Q32s4Q")


def _string_bytes(value: object) -> int:
    """Conservative retained size of one string: header plus UCS-4 width."""

    return _STR_HEADER_BYTES + _STR_CHAR_BYTES * len(str(value))


def _read_metadata(path: Path) -> tuple[kept_reads.FileVersion, bytes]:
    """One metadata file's bytes, with the version those bytes were read at.

    ``fstat`` of the open descriptor rather than a second ``stat`` of the
    name: the version returned names the exact bytes returned, so a
    concurrent replace between a directory scan and this read can never file
    fresh bytes under a stale version, or a stale parse under a fresh one.
    """

    with open(path, "rb") as stream:
        version = kept_reads.metadata_version(os.fstat(stream.fileno()))
        return version, stream.read()


def _observe(path: Path) -> "kept_reads.FileVersion | OSError":
    """One metadata file's version, or the ``OSError`` its ``stat`` raised.

    The per-lookup stat of the reuse index (#761), taken before the
    publisher's lookup lock rather than under it (#981): once the proof left
    the stage ownership lock, sixteen copy workers proving at once would
    otherwise queue one ``stat`` at a time behind that small lock.  The
    version compare, and any read and parse a changed version needs, still
    run under it, so one file is still parsed once per version.
    """

    try:
        return kept_reads.metadata_version(os.stat(path))
    except OSError as exc:
        return exc


def _origin_id_of(path_str: str) -> str | None:
    """``st_size:st_mtime_ns:st_ino`` of a copy source, or ``None``.

    The same three numbers :mod:`prismabuild.stage_marks` defines for
    ``STAGE_SOURCE_XATTR`` at copy time: the existing origin
    change-detection evidence, read here rather than reinvented.
    """

    try:
        sample = os.stat(path_str)
    except OSError:
        return None
    if not statmod.S_ISREG(sample.st_mode):
        return None
    return source_identity(sample)


def _packed_order(names: "list[str] | set[str]") -> list[str]:
    """One deterministic order for a path set, shared by every packer.

    Ascending unsigned 64-bit ``hash`` of the name, then the encoded bytes as
    the tie-break, so two documents that name the same paths build the same
    packed identity and share one table.  The order is process-local (the
    hash is salted) and never leaves the publisher.
    """

    return sorted(names, key=lambda name: (
        hash(name) & _HASH_MASK, name.encode("utf-8", "surrogatepass")))


class _PackedPaths:
    """Exact membership over a packed, shareable set of normalized paths.

    The paths are UTF-8 encoded (``surrogatepass``, so a lone surrogate from
    JSON cannot compare equal to a real path -- the encoding stays injective
    over Python strings) and packed into one bytes blob, with an 8-byte end
    offset and an unsigned 64-bit ``hash`` key per path in the same order.
    Lookup is a C-level ``bisect`` on the key array, followed by an exact
    byte comparison of the blob slice for every candidate with an equal key:
    a hash collision costs one comparison and never a different answer.

    ``retained_bytes`` prices the blob, both arrays and fixed headers.  The
    ``(blob, ends)`` pair is the table's exact identity -- ends fixes every
    boundary, so equal pairs are equal path sequences -- and the one key the
    publisher's intern table shares it under.
    """

    __slots__ = ("blob", "ends", "keys")

    def __init__(self, names: "list[str] | set[str]") -> None:
        ordered = _packed_order(list(names))
        encoded = [name.encode("utf-8", "surrogatepass") for name in ordered]
        self.blob = b"".join(encoded)
        ends = bytearray()
        total = 0
        for item in encoded:
            total += len(item)
            ends += total.to_bytes(8, "little")
        self.ends = bytes(ends)
        self.keys = array.array("Q", (hash(name) & _HASH_MASK
                                      for name in ordered))

    @property
    def retained_bytes(self) -> int:
        """Actual retained size of this table, allocated storage included.

        ``sys.getsizeof`` of the key array reports its allocated capacity,
        not only the slots in use, and the blob, offsets and instance headers
        are measured the same way, so this is never under the bytes the table
        holds.  The intern tuple and its dictionary slot are added once per
        table, in the interned charge only -- never again in a record's.
        """

        if not self.keys:
            return 0
        return (sys.getsizeof(self) + sys.getsizeof(self.blob)
                + sys.getsizeof(self.ends) + sys.getsizeof(self.keys)
                + _INTERN_TABLE_BYTES)

    def intern_key(self) -> tuple[bytes, bytes]:
        """The exact identity one shared table is interned under."""

        return (self.blob, self.ends)

    def index(self, path: str) -> int:
        """The path's position in the packed order, or -1 when absent."""

        keys = self.keys
        count = len(keys)
        if not count:
            return -1
        needle_hash = hash(path) & _HASH_MASK
        position = bisect.bisect_left(keys, needle_hash)
        if position == count or keys[position] != needle_hash:
            return -1
        needle = path.encode("utf-8", "surrogatepass")
        blob, ends = self.blob, self.ends
        while position < count and keys[position] == needle_hash:
            start = (int.from_bytes(ends[position * 8 - 8:position * 8],
                                    "little") if position else 0)
            end = int.from_bytes(ends[position * 8:position * 8 + 8], "little")
            if blob[start:end] == needle:
                return position
            position += 1
        return -1

    def __contains__(self, path: object) -> bool:
        return self.index(str(path)) >= 0


#: One shared empty table for records that name no path; never interned and
#: never charged a table cost (the per-record overhead already prices the
#: row, and the one object is process-wide).
_EMPTY_PATHS = _PackedPaths([])

#: A census slot whose fragment record the reuse index would not keep (#981):
#: its packed table is too big for the budget, so the census holds no copy of
#: it, and each decision that reaches the slot looks the file up again, as
#: every decision did before the census was shared.
_LOOK_AGAIN = object()


def _pack_mention(mention: tuple) -> bytes:
    """One validated mention as its fixed binary record.

    Raises for anything the fixed width cannot carry exactly --
    ``validate_material`` accepts arbitrary-size non-negative integers -- so
    the caller falls back to the uncached answer rather than truncate.
    """

    size, digest, identity = mention
    if not isinstance(digest, str) or not isinstance(identity, Mapping):
        raise ValueError("mention is not a validated (bytes, sha256, file_id)")
    return _MENTION_STRUCT.pack(int(size), bytes.fromhex(digest),
                                int(identity["ino"]), int(identity["size"]),
                                int(identity["mtime_ns"]),
                                int(identity["ctime_ns"]))


def _decode_mentions(packed: bytes) -> tuple:
    """The ``(size, sha256, file_id)`` triples one path's records decode to."""

    out = []
    for offset in range(0, len(packed), _PACKED_MENTION_BYTES):
        size, digest, ino, size_bytes, mtime_ns, ctime_ns = (
            _MENTION_STRUCT.unpack_from(packed, offset))
        out.append((size, digest.hex(),
                    {"ino": ino, "size": size_bytes,
                     "mtime_ns": mtime_ns, "ctime_ns": ctime_ns}))
    return tuple(out)


class _PackedMentions:
    """One sidecar's ordered per-path mentions, packed and shareable.

    ``paths`` is a :class:`_PackedPaths` table (shared with every other
    document naming the same paths); ``values`` is one bytes blob of fixed
    72-byte mention records and ``ends`` the 8-byte end offsets into it, both
    parallel to the table's packed order.  ``get`` reconstructs exactly the
    triples the object representation returned, in the same order, because
    the digest search and the identity search legitimately read different
    mentions of one path.
    """

    __slots__ = ("paths", "values", "ends")

    def __init__(self, paths: _PackedPaths, values: bytes,
                 ends: bytes) -> None:
        self.paths = paths
        self.values = values
        self.ends = ends

    @property
    def values_bytes(self) -> int:
        """Actual retained size of this record's values and its offsets."""

        return (sys.getsizeof(self) + sys.getsizeof(self.values)
                + sys.getsizeof(self.ends))

    @classmethod
    def build(cls, mentions: Mapping[str, tuple]) -> "_PackedMentions | None":
        """The packed projection of one validated sidecar, or ``None``.

        ``None`` means some mention cannot be carried exactly by the fixed
        record.  The caller then decides from the freshly parsed object,
        uncached: the same verdict, never a truncated one.
        """

        try:
            packed = {path: b"".join(_pack_mention(one) for one in carried)
                      for path, carried in mentions.items()}
        except (struct.error, OverflowError, ValueError, TypeError, KeyError):
            return None
        ordered = _packed_order(list(packed))
        values = b"".join(packed[path] for path in ordered)
        ends = bytearray()
        total = 0
        for path in ordered:
            total += len(packed[path])
            ends += total.to_bytes(8, "little")
        return cls(_PackedPaths(ordered), values, bytes(ends))

    def shared_with(self, paths: _PackedPaths) -> "_PackedMentions":
        """The same values on the one actual interned table."""

        return _PackedMentions(paths, self.values, self.ends)

    def get(self, path: str, default: object = ()) -> object:
        """The ordered triples this sidecar carries for ``path``."""

        index = self.paths.index(path)
        if index < 0:
            return default
        start = (int.from_bytes(self.ends[index * 8 - 8:index * 8], "little")
                 if index else 0)
        end = int.from_bytes(self.ends[index * 8:index * 8 + 8], "little")
        return _decode_mentions(self.values[start:end])


class PublicationRefused(OSError):
    """The publication gate refused one entry; the range stops issuing work.

    The refusal is made per staged name, and it says nothing about the other
    names: they may be perfectly publishable.  It does, however, make the
    range incomplete -- this mover cannot publish what it declared -- so
    every further copy and publication grace is spent on a run that must be
    retried anyway, and the first obstruction is buried under later entry
    errors by the receipt's error cap.  A copier thread that receives this
    signal therefore stops handing out queued entries for the range, bounding
    the wasted work and surfacing the obstruction; the entries already
    dispatched to the active group still finish or refuse, and everything
    they committed keeps its fragment, sidecar and byte count (#853).

    An ordinary source read, digest or filesystem failure is a plain
    ``OSError`` and keeps the existing per-entry handling: one bad source is
    not evidence about the rest of the range.

    A subclass of ``OSError`` so every caller that already catches
    ``OSError`` keeps catching it -- the type breaks no existing behavior.
    The copier deliberately changes its range handling for it, which is the
    point of the type.

    ``refusal`` is set only on a refusal no rerun can clear (#966): a live
    owner holds different bytes under the name.  ``conflict`` then names
    the path and both owners, and the mover's receipt carries both, so it
    exits nonzero and retires its own window instead of being republished
    into the same refusal.  Every other refusal leaves them ``None`` and
    stays retryable.

    ``wait_verdict`` (#994) is set only when this refusal is what a
    :data:`PUBLISH_WAIT_OWNED`-family wait settled to after the grace: one of
    ``owned``, ``live_publisher`` or ``in_flight``.  It is ``None`` for a
    refusal that never waited -- an unstatable or non-regular destination, an
    unreadable census, a live pin, a proven divergence -- and for
    ``unattributed``, which the grace heals by replacement rather than
    refusing.  The copier reads it to name the verdict on the per-entry error
    it records (:meth:`_Copier.run`), so the obstruction the grace timed out
    on is not left to the free-text message alone.
    """

    def __init__(self, *args: object, refusal: str | None = None,
                 conflict: Mapping[str, object] | None = None,
                 wait_verdict: str | None = None) -> None:
        super().__init__(*args)
        self.refusal = refusal
        self.conflict = dict(conflict) if conflict is not None else None
        self.wait_verdict = wait_verdict


#: The terminal refusal a mover files when a live owner holds different bytes
#: under one of its staged names (#966).
STAGED_DESTINATION_CONFLICT = "staged_destination_conflict"

#: The terminal refusal a mover files when a divergent name's owner's ending
#: stayed unprovable across :data:`UNPROVEN_ENDING_RUNS` runs (#1004 item 2).
STAGED_DESTINATION_UNPROVEN = "staged_destination_unproven"

#: How many consecutive runs of one mover may refuse on the same unprovable
#: ending before the refusal is terminal (#1004 item 2).  An unprovable
#: ending is one no amount of waiting settles by itself: a consumer with no
#: outcome record (#798's legacy shape), two outcomes, an outcome or a
#: fragment that does not parse, a key that is not an action key.  The only
#: benign reason to see one is a record the shared mount has not shown this
#: host yet (``staged_path.unended_owner``).  The span below is what
#: excludes that lag; the count makes the terminal verdict rest on three
#: separate claims of the mover, each with its own reads, rather than on one
#: unlucky pass (a choice, recorded in #1004).  A queued, claimed or leased
#: key, an I/O error and a busy lock settle by themselves and are never
#: counted, however long they last.
UNPROVEN_ENDING_RUNS = 3

#: The least time the counted runs must span before the refusal is terminal:
#: twice the queue mount's ``acdirmin`` (30 s, ``pool.record_store._read_json_fresh``), so
#: a record that was only not yet visible to the first refusal is visible to
#: the last.  Runs republished faster than this keep counting and wait.
UNPROVEN_ENDING_MIN_SPAN_S = 60.0


def _unproven_evidence(unproven: Sequence[Mapping[str, object]],
                       ) -> list[list[str]] | None:
    """The fingerprint a run's unproven refusals are counted by, or ``None``.

    One ``[consumer, mover, why]`` per owner whose ending was not proven, or
    ``["", "", why]`` for a divergence whose owners all ended but whose
    census could not read a fragment.  The staged path is left out: a copy
    stops dispatch at its first refusal, and which name a multi-reader copy
    meets first is not evidence about the owner.  ``None`` when the run
    refused on nothing unproven, or when any of its causes settles by itself
    (:data:`UNPROVEN_ENDING_RUNS`): such a run is never counted.
    """

    if not unproven:
        return None
    evidence: set[tuple[str, str, str]] = set()
    for record in unproven:
        if record.get("settles"):
            return None
        owners = [row for row in record.get("owners") or ()
                  if isinstance(row, Mapping)]
        if not owners:
            evidence.add(("", "", str(record.get("why") or "")))
        for row in owners:
            evidence.add((str(row.get("consumer_action_key") or ""),
                          str(row.get("mover_action_key") or ""),
                          str(row.get("why") or record.get("why") or "")))
    return [list(item) for item in sorted(evidence)]


def unproven_streak(prior: object, unproven: Sequence[Mapping[str, object]],
                    *, now: float) -> dict[str, object] | None:
    """This run's ``unproven`` receipt block, counted against its prior run.

    ``prior`` is the receipt this mover's key filed last (``move_record``,
    read before this run files its own).  A run whose unprovable evidence
    equals the prior run's continues its count; anything else starts again,
    and a run with a cause that settles by itself counts zero.  ``terminal``
    is true once the count reaches :data:`UNPROVEN_ENDING_RUNS` across at
    least :data:`UNPROVEN_ENDING_MIN_SPAN_S`.  ``None`` when this run refused
    on no unproven ending at all.
    """

    if not unproven:
        return None
    evidence = _unproven_evidence(unproven)
    runs, first = 0, float(now)
    if evidence is not None:
        runs = 1
        before = (prior.get("unproven") if isinstance(prior, Mapping)
                  else None)
        if (isinstance(before, Mapping) and before.get("evidence") == evidence
                and isinstance(before.get("runs"), int)
                and not isinstance(before.get("runs"), bool)
                and int(before["runs"]) > 0
                and isinstance(before.get("first_unix"), (int, float))):
            runs = int(before["runs"]) + 1
            first = float(before["first_unix"])
    owners: dict[tuple[str, str], dict[str, object]] = {}
    for record in unproven:
        for row in record.get("owners") or ():
            if isinstance(row, Mapping):
                owners.setdefault((str(row.get("consumer_action_key")),
                                   str(row.get("mover_action_key"))),
                                  dict(row))
    head = unproven[0]
    return {
        "evidence": evidence,
        "settles": evidence is None,
        "runs": runs,
        "bound": UNPROVEN_ENDING_RUNS,
        "first_unix": first,
        "terminal": (runs >= UNPROVEN_ENDING_RUNS
                     and now - first >= UNPROVEN_ENDING_MIN_SPAN_S),
        "stage_path": head.get("stage_path"),
        "stage_paths": sorted({str(record.get("stage_path"))
                               for record in unproven})[:20],
        "declared_sha256": head.get("declared_sha256"),
        "why": head.get("why"),
        "owners": [owners[key] for key in sorted(owners)],
    }


class _Owners:
    """Who a proof search found naming one staged path it found divergent.

    Filled by :meth:`StagedPublisher._proof_search` when one is passed in:
    every ``(consumer, mover)`` whose fragment names the path and whose
    record is not a date for a superseded incarnation, except the
    publisher's own pair.  ``complete`` is false when a fragment or a child
    directory could not be read, since an unreadable record may name the
    path too.
    """

    __slots__ = ("pairs", "complete")

    def __init__(self) -> None:
        self.pairs: set[tuple[str, str]] = set()
        self.complete = True


def _divergent_detail(norm: str) -> str:
    return (f"staged destination holds different bytes than manifest "
            f"digest for {norm}; refusing to invalidate its owner")


def is_action_key(value: object) -> bool:
    return (isinstance(value, str) and len(value) == 64
            and all(character in "0123456789abcdef" for character in value))


def _combined_owner_state(rows: list[dict[str, object]],
                          complete: bool) -> tuple[str, str]:
    """One divergence's state from its judged owners (#966).

    A live owner decides it even beside an unreadable fragment: that owner
    holds different bytes under the name whatever else might.  Otherwise an
    unreadable fragment, or any owner not proven ended, leaves it
    uncertain, and only a complete census of ended owners is ``ended``.
    """

    if any(row["state"] == "live" for row in rows):
        return "live", ""
    if not complete:
        return "uncertain", "a fragment that may name it could not be read"
    for row in rows:
        if row["state"] != "ended":
            return "uncertain", str(row.get("why")
                                    or "an owner's ending is unproven")
    return "ended", ""


class PhaseClock:
    """Thread-seconds a mover spends in each phase, for its receipt (#981).

    A receipt that reports only totals cannot say where a multi-minute
    window went: chunk 0 of ``234d662cdc4f`` ran 368.7 s, landed its bytes in
    the first 90, and nothing on it split the other 270.  Each phase here is
    summed across the copy workers -- thread-seconds, not wall -- with its
    call count and its longest single call, so a phase that is slow on
    average and a phase that is slow once both show.  The copy's own phases
    (``pace_wait``, ``copy_read``, ``copy_write``, ``hash``, ``fsync``) are
    added once per entry; the publisher's (``adopt_proof``,
    ``publish_decide``, ``ownership_lock_wait``, ``ownership_lock_held``,
    ``publish_poll_sleep``, ``content_proof`` (#1081), and, for a divergent
    name, ``owner_judgement`` and ``owner_locks_held``, #966) once per call.
    ``outcomes`` counts how each entry ended, one outcome per entry:
    ``adopted`` before any copy on a record's proof, ``adopted_by_content``
    before any copy on its own bytes (#1081), ``renamed`` by its own rename,
    ``adopted_at_publication`` after copying on a record's proof,
    ``adopted_by_content_at_publication`` after copying on its own bytes, or
    -- a name every owner of which had ended -- replaced at publication.

    ``publish_waits`` (#994) totals, by :data:`PUBLISH_WAIT_OWNED`-family
    kind, how many entries waited under it and the real elapsed seconds spent
    waiting -- :meth:`StagedPublisher.publish` credits an entry once, when
    its own wait ends, not once per poll, so ``entries`` counts distinct
    entries and ``seconds`` is the total time this mover spent parked in
    that kind of wait.  Reported for the receipt's own ``publish_waits``
    field, a sibling of ``phase_timings`` rather than a part of it, the way
    ``start_gate_wait_s`` sits beside it rather than inside it.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._phases: dict[str, list[float]] = {}
        self._outcomes: dict[str, int] = {}
        self._waits: dict[str, list[float]] = {}

    def add(self, phase: str, seconds: float, calls: int = 1) -> None:
        with self._lock:
            row = self._phases.setdefault(phase, [0.0, 0, 0.0])
            row[0] += seconds
            row[1] += calls
            if seconds > row[2]:
                row[2] = seconds

    @contextmanager
    def timing(self, phase: str):
        started = time.perf_counter()
        try:
            yield
        finally:
            self.add(phase, time.perf_counter() - started)

    def outcome(self, name: str) -> None:
        with self._lock:
            self._outcomes[name] = self._outcomes.get(name, 0) + 1

    def wait(self, kind: str, seconds: float) -> None:
        """One entry's finished wait: ``kind`` is a ``PUBLISH_WAIT_*`` value."""

        with self._lock:
            row = self._waits.setdefault(kind, [0.0, 0])
            row[0] += max(0.0, seconds)
            row[1] += 1

    def report(self) -> dict[str, object]:
        with self._lock:
            return {
                "thread_seconds": {
                    phase: {"seconds": round(row[0], 6), "calls": int(row[1]),
                            "max_s": round(row[2], 6)}
                    for phase, row in sorted(self._phases.items())},
                "outcomes": dict(sorted(self._outcomes.items())),
            }

    def publish_waits_report(self) -> dict[str, object]:
        with self._lock:
            return {
                kind: {"entries": int(row[1]), "seconds": round(row[0], 6)}
                for kind, row in sorted(self._waits.items())
            }


class StagedPublisher:
    """Linearize staged-path publication under the stage ownership lock.

    Overlapping consumers stage one content-addressed name for the same
    bytes; an unconditional rename per copy mints a fresh inode/mtime
    and invalidates every other consumer's published material identity
    (and any live cover proof or pin fenced on it). The payload copy
    into the private owner-keyed temp stays outside every lock; this
    gate runs immediately before destination publication and decides
    metadata-only.  What it decides *acts* under the existing
    stage-ownership exclusion (the same lock egress holds across
    snapshot-to-release); what it only *reads* does not hold it (#981).
    The proof search and the pin, claim and in-flight censuses are
    reads -- they run first, without the lock -- and a verdict that acts
    is then re-established under the lock before it acts: an adoption
    commits only while the destination still carries every field of the
    ``file_id`` its proof dated, and a replacement or a refusal is decided
    again, in full, under the lock, exactly as before.  One lock for the
    whole stage root held across every proof made every mover on the
    host prove one entry at a time (about 100 ms each on dl380g10,
    2026-09-23).  The lock never excluded the fragment *writers* --
    ``write_fragment`` runs outside it -- so a search under it was never
    a snapshot of the forest either; what it excludes is the egress's
    scan-to-unlink and a racing first publication's rename, and both are
    what the commit re-checks.  The verdicts:

    * absent destination: this copy publishes (concurrent absentees
      serialize here, so exactly one first publication wins; a loser
      that already passed finds a present path below and converges);
    * present with an adoption proof -- some consumer's fragment entry
      naming this path with equal bytes, a material sidecar digest that
      matches (the declared digest; the copier's own computed digest
      for digest-less manifests, which pin no content; or the origin
      fast path below), and a live stat equal to the sidecar's file
      identity: adopt the published incarnation, discard the temp.
      Unchanged bytes keep every live pin valid, so no pin census is
      needed on this path;
    * present and provably different -- a record that dates the current
      incarnation names a digest that matches neither the declared nor
      the computed digest, or a same-inode record whose size also
      disagrees (no legitimate publication writes in place): never a blind
      overwrite.  A same-inode record whose size still agrees but whose
      ``mtime_ns``/``ctime_ns`` drifted is not decided from stat alone --
      an allocator handing a freed inode straight back to the very next
      create in the directory reads exactly like an in-place write, so
      whether it is genuine divergence is settled by the file's own bytes
      against the record's own digest first (:meth:`_content_proof`,
      #1078), the same tie-break #1096 uses for an NFS delegation recall;
      a confirmed match is not itself proof of an unbroken lineage, so it
      is treated like a record dating a superseded incarnation below,
      never adopted on this record alone.  On a stage mover the owners
      decide an actual divergence (#966,
      :meth:`_arbitration`): a live owner is a conflict for an owner to
      resolve, refused terminally and named; an owner whose ending cannot
      be proven is refused retryably, as before, until the same unprovable
      evidence has refused :data:`UNPROVEN_ENDING_RUNS` runs of the mover
      (#1004 item 2); and a name every owner of
      which has provably ended is invalidated and restaged by this copy,
      after the same pin, claim and in-flight censuses a heal passes.  A
      ram promotion arbitrates the same way, by its promoting consumer
      (#1004 item 1); a publisher built with no consumer refuses at once.
      A record whose sidecar dates a superseded incarnation -- a different
      inode, the name replaced by a real publication -- is not this: it
      is deferred like an undated vouch while another record may still
      prove the current one (#755);
    * present but unproven, live-pinned, or unreadable: refuse at once
      for pins and unreadable proof state; otherwise wait out the grace
      for a late fragment or an in-flight publisher, then refuse --
      deferring to the action stall policy's retry, which adopts once
      the proof lands.  No timeout ever permits replacement.

    The single exception is re-verified positive absence: no fragment
    names the path, no pin refs it, no live mover claim (own excluded)
    covers it, no sibling copy is in flight, and every census read
    clean -- re-checked after the grace, not merely elapsed.  Only then
    does the copy heal the name as orphan/crash residue.  A live or
    unknown publisher always means wait/refuse, never replace.

    Under that same positive absence the bytes already there are proven by
    their content before any grace (#1081, :meth:`_content_proof`): hashed
    and compared with the declared digest, or with the copy's own digest
    for a digest-less manifest.  A match is adopted with no grace and no
    rename -- before any copy when the manifest declares a digest -- so a
    promotion restarted after a kill (its records are filed only at the end
    of its range) takes back the copies its last attempt left instead of
    copying each again, waiting out the grace for a record that never
    comes, and replacing it.  A mismatch changes nothing: the name waits
    out the grace and heals as before.  Every census that defers or refuses
    runs first, so a live pin, a live claim, a copy in flight and any record
    of the name keep exactly the verdicts above.

    Digest-less (dev/null) manifests converge like the rest: the origin
    fast path adopts without copying where the published file still
    carries the source identity this copy's source stats today
    (``user.pbstage.source``, the prewarm loop's existing evidence);
    otherwise the necessary private copy's own computed digest is
    compared against the stored material proof at final publication --
    the existing file is never rehashed.  No material schema change:
    origin provenance rides the existing xattr where the filesystem
    carries it, and the digest comparison where it does not.

    Never replaces an occupied path except into proven absence (first
    publication) or re-verified orphan residue.  Returns ``(written,
    digest, identity)`` like a copy; raises ``_PublicationRefused`` on a
    gate refusal and plain ``OSError`` on stop.  The worker files both as an
    entry error exactly like a digest mismatch; a refusal additionally stops
    the copier from issuing further entries for this range (#853).
    """

    def __init__(self, *, queue, stage_root, residency_root,
                 mover_action_key: str,
                 manifest_sha256: str, tier_id: str, cas_root,
                 consumer_action_key: str = "", trust=None) -> None:
        self.queue = queue
        self.trust = kept_reads.filesystem_trust if trust is None else trust
        self.stage_root = Path(stage_root)
        self.residency_root = Path(residency_root)
        self.mover = mover_action_key
        self.consumer = str(consumer_action_key)
        self.manifest_sha256 = manifest_sha256
        self.tier_id = tier_id
        self.cas_root = cas_root
        #: The generation this run's material sidecar carries.  A resumed
        #: run starts from its prior own generation (stable reuse, the
        #: ``reader_lease.adopted_generation`` rule: same bytes without
        #: replacement keep their generation) and mints a fresh one only
        #: when it first replaces actual bytes -- new material, new date.
        #: A first-ever run mints once and never re-mints mid-run, exactly
        #: as the single-per-run generation always did.
        self._material_generation: str | None = None
        self._generation_is_resumed = False
        self._generation_bumped = False
        # Invocation-local reuse of parsed publication metadata (#761, #778).
        # ``_proof_search`` runs once per destination -- tens of thousands
        # per mover -- and again on every publish poll, and before this it
        # re-opened and re-parsed every consumer fragment on each run (118
        # files, 334.6 MB, on the live root when this was measured) plus
        # every relevant sidecar.  Each file is now parsed once per version
        # and retained as a packed projection of exactly what a decision
        # reads -- never the parsed object graph, whose bytes can refuse
        # retention once a document's graph is oversized, after which every
        # later lookup re-reads and re-decodes the whole document (the
        # amplification #778 reproduced at fixture scale).  The directory
        # scans and the per-file stat that see additions, changes and
        # removals still run before every decision, so nothing decides on
        # evidence that moved.  Process-local, nothing persisted, dropped
        # with the publisher.  Its own lock because the stage ownership lock
        # is a POSIX file lock, which does not exclude two threads of one
        # process from each other.
        self._lookup_lock = threading.Lock()
        # Each entry is ``(version, record, charge)``: the charge is measured
        # from the real packed object at insert and travels with it, so
        # dropping an entry cannot forget to discount it and no row is ever
        # free.
        self._fragments: dict[str, tuple[tuple, object, int]] = {}
        self._materials: dict[tuple[str, str], tuple[tuple, object, int]] = {}
        #: One actual packed path table per exact identity, shared by every
        #: fragment and sidecar that names those paths and charged once.
        self._interned: dict[tuple[bytes, bytes], _PackedPaths] = {}
        self._interned_cost = 0
        self._fragment_cost = 0
        self._material_cost = 0
        #: Names whose bytes did not hash to the trusted digest, by the
        #: identity and digest they were hashed under (#1081,
        #: :meth:`_content_proof`); guarded by ``_lookup_lock``.
        self._content_misses: dict[str, tuple] = {}
        #: Where this publisher's time goes, for the mover's receipt.
        self.clock = PhaseClock()
        # The single-flight forest census (#981), see :meth:`_shared_census`.
        # ``_census`` is the latest finished census and ``_census_done`` its
        # number; ``_census_started`` counts censuses begun.  Only one runs
        # at a time, so the numbers finish in order.
        self._census_cond = threading.Condition(threading.Lock())
        self._census_started = 0
        self._census_done = 0
        self._census_running = False
        self._census: tuple | None = None
        #: What the last census listed, each directory under the stamp taken
        #: before its listing (#1004 item 4, the #992 index): the residency
        #: root's children, and each child's fragment names and paths.  A
        #: census compares each directory with its stamp and lists again only
        #: the ones that moved (:meth:`_forest_census`); records are looked
        #: up afresh every census.  Touched only by the one running census.
        self._forest_roots = kept_reads.DirectoryCaptures(self.trust)
        self._forest_children = kept_reads.DirectoryCaptures(self.trust)
        #: Whether a divergent name is settled by its owners' states (#966).
        #: A stage mover and a ram promotion (#1004 item 1) name their
        #: consumer and do; a publisher built with none keeps the retryable
        #: refusal.
        self._arbitrates = bool(self.consumer)
        #: One divergence is arbitrated at a time per publisher: the owners'
        #: transition locks are taken without blocking, and a sibling copy
        #: thread holding them must read as a wait, not as another process's
        #: contention.
        self._arbitration_lock = threading.Lock()
        #: Owners this run proved ended, by ``(consumer, mover)``, each with
        #: its judged row.  Read and written under ``_arbitration_lock``.
        self._ended_owners: dict[tuple[str, str], dict[str, object]] = {}
        #: The names this run replaced because every owner had ended, each
        #: with the owners it proved, for the receipt.  Appended under
        #: ``_arbitration_lock``.
        self.invalidated: list[dict[str, object]] = []
        #: The divergent names this run refused because an owner's ending
        #: was not proven, each with those owners and whether the cause
        #: settles by itself (#1004 item 2); the mover counts them against
        #: its prior run (:func:`unproven_streak`).  Appended under
        #: ``_arbitration_lock``.
        self.unproven: list[dict[str, object]] = []
        # The claim census memo for this publisher's whole run (#1089).
        # ``staged_path.claimed_paths`` derives a claimed range mover's
        # staged paths from its sealed request and its data manifest, both
        # immutable under their digests -- so once derived for a claim key
        # the answer cannot go stale, and a memo may keep it for as long as
        # this publisher runs, not just for one call.  What is *not*
        # memoized, by the memo's own contract, is the claim listing and
        # each claim record: ``_claimed_paths`` reads both fresh on every
        # call, so a claim that appears, ends or changes is still seen at
        # once -- the memo only skips re-deriving paths for a claim it has
        # already seen unchanged. The census and publisher share this owner.
        # Every copy worker thread shares one publisher and can reach
        # ``_live_claim_cover`` concurrently.  No lock is taken around the
        # census: ``_claimed_paths`` touches this memo only through single
        # ``memo.claims`` dict reads and writes of immutable frozensets, so
        # two threads racing on one claim at worst derive it twice, and a
        # lock held across the census's directory scans and record reads
        # would serialize every worker's check behind them.

        self._claim_memo = kept_reads.CensusMemo(trust=self.trust)
        #: The range decision's partial-temporaries hint (#1028): each
        #: range directory is listed once under
        #: :func:`kept_reads.filesystem_trust.directory_before` and a name revalidates the
        #: hint with one directory version, re-listing only what moved.
        #: The claim census and the pin census are main's, per name.
        #: Guarded by ``_range_lock`` because every copy worker thread
        #: shares one publisher.
        self._range_lock = threading.Lock()
        #: Each range directory's ``(stamp, partial names)``, listed once
        #: per :func:`kept_reads.filesystem_trust.directory_before`; only a fenced listing is
        #: remembered.
        self._partial_names = kept_reads.DirectoryCaptures(self.trust)

    @contextmanager
    def _ownership(self):
        """The stage ownership lock, with its wait and its hold timed."""

        asked = time.perf_counter()
        with self.queue.stage_ownership_lock(str(self.stage_root)):
            granted = time.perf_counter()
            self.clock.add("ownership_lock_wait", granted - asked)
            try:
                yield
            finally:
                self.clock.add("ownership_lock_held",
                               time.perf_counter() - granted)

    def begin_material(self, resumed: str | None = None) -> str:
        """Start this run's material generation, optionally resuming one.

        ``resumed`` is this mover's own prior generation, carried over when
        the run preserves qualified prior coverage: unchanged bytes keep
        their date, so a live pin's ``covers`` still match.  ``None`` is a
        first publication, and mints.
        """

        if resumed is not None:
            self._material_generation = resumed
            self._generation_is_resumed = True
        else:
            self._material_generation = reader_lease.mint_generation()
        return self._material_generation

    def material_generation(self) -> str:
        """The generation the sidecar should be written under right now."""

        if self._material_generation is None:
            self.begin_material()
        return self._material_generation

    def _note_replacement(self) -> None:
        """Record that this run replaced actual bytes at a destination.

        The one event that mints a new generation on a resumed run: the
        sidecar is a single document with one generation, and once any
        entry carries bytes this run wrote, the carried-over date no
        longer describes the whole document.  Minted once -- the document
        then keeps that generation for the rest of the run, the same
        one-per-run stability a first publication always had.
        """

        if self._generation_is_resumed and not self._generation_bumped:
            self._material_generation = reader_lease.mint_generation()
            self._generation_bumped = True

    def try_adopt(self, entry: dict[str, object], destination: Path,
                  source_id: str | None = None,
                  ) -> tuple[int, str, dict[str, int]] | None:
        """Adopt an already-published incarnation without copying, if proven.

        The same proof the publish gate requires, minus the copy-compare
        path (no computed digest exists yet).  Returns ``(want, digest,
        file_id)`` or ``None`` to proceed with the copy, and counts the
        adoption's outcome on the receipt's clock.  Raises
        ``_PublicationRefused`` for provably divergent bytes, failing fast
        before any copy.

        A name no record names is proven by its bytes when the manifest
        declares a digest and every census that could defer it reads clean
        (#1081, :meth:`_adopt_by_content`).  That is the one path here that
        reads a payload, and it reads the destination, never the source.
        """

        adopted, outcome = self._try_adopt(entry, destination, source_id)
        if adopted is not None:
            self.clock.outcome(outcome)
        return adopted

    def _try_adopt(self, entry: dict[str, object], destination: Path,
                   source_id: str | None = None,
                   ) -> tuple[tuple[int, str, dict[str, int]] | None, str]:
        """:meth:`try_adopt`'s decision and the outcome it counts.

        The proof runs without the stage ownership lock (#981); only the
        commit takes it.  Under the lock, a proof adopts only while the
        destination still carries every field of the ``file_id`` the proof
        dated -- the same identity check every strict reader makes -- so the
        bytes adopted are the bytes the proof's sidecar digest describes.
        Anything else that would act -- a divergence, or a proof whose
        incarnation moved after the search -- is searched again under the
        lock and decided exactly as before.  An unknown, owned or clean
        answer acts on nothing: the copy proceeds, and its publication is
        gated under the lock.

        A divergence on a stage mover is arbitrated by its owners' states
        (#966, :meth:`_arbitration`) before anything is copied: a live owner
        raises the terminal conflict and an unproven ending the retryable
        refusal, as before, so neither costs a copy.  When every owner has
        ended the copy proceeds, and its publication replaces the name by
        rename; the owners' endings are remembered for the run, so the
        publication does not judge them again.  Nothing is unlinked here: a
        rename over the old file keeps its inode allocated until the new one
        takes the name, so the new bytes can never reuse the inode number the
        old owner's material dates, which would read as an in-place write.
        """

        want = int(entry["bytes"])
        declared = entry.get("sha256")
        norm = os.path.normpath(str(destination))
        try:
            present = os.lstat(destination)
        except OSError:
            return None, ""
        if not statmod.S_ISREG(present.st_mode):
            return None, ""
        owners = _Owners() if self._arbitrates else None
        with self.clock.timing("adopt_proof"):
            proof, standing, detail = self._proof_search(
                norm, want, declared, source_id=source_id, owners=owners)
        if proof is None and standing == "clean":
            return (self._adopt_by_content(destination, norm, want, declared),
                    "adopted_by_content")
        if proof is None and standing != "divergent":
            return None, ""
        if standing == "divergent" and owners is not None:
            return self._adopt_divergent(destination, norm, want, declared,
                                         source_id, owners, detail), "adopted"
        with self._ownership():
            if proof is not None and reader_lease.file_id_matches(
                    proof[1], reader_lease.stat_identity(norm)):
                return (want, proof[0], proof[1]), "adopted"
            owners = _Owners() if self._arbitrates else None
            proof, standing, detail = self._proof_search(
                norm, want, declared, source_id=source_id, owners=owners)
            if standing == "unknown":
                return None, ""
            if standing == "divergent":
                if owners is None:
                    raise PublicationRefused(detail)
            elif proof is None:
                return None, ""
            else:
                return (want, proof[0], proof[1]), "adopted"
        # A divergence that appeared after the lockless search: arbitrated
        # once the stage lock is released, since the owners' transition
        # locks come before it.
        return self._adopt_divergent(destination, norm, want, declared,
                                     source_id, owners, detail), "adopted"

    def _adopt_by_content(self, destination: Path, norm: str, want: int,
                          declared: object,
                          ) -> tuple[int, str, dict[str, int]] | None:
        """Adopt a name no record names, on its own bytes, before any copy.

        #1081.  A promotion files its records only at the end of its range,
        so one killed before then -- every PB publish restarts the tier role
        -- leaves correct copies that nothing names.  Here they are proven
        the way the copy itself would have been: hashed and compared with
        the declared digest.  The same censuses :meth:`_decide` runs before
        a heal run first and in the same order, and anything they find --
        a live pin, a live claim, a copy in flight, or a census that cannot
        be read -- returns ``None``: the copy proceeds and its publication
        waits or refuses exactly as it always did.  A digest-less manifest
        has nothing to compare yet and copies; its publication compares the
        copy's digest instead.

        Everything here runs without the stage ownership lock: the hash is
        of one file and grows with its size, so it may not run under a lock
        every mover on the host takes (review checklist, item 1).  The
        commit takes the lock only to confirm that the name still carries
        every field of the identity the hash was taken under -- the same
        check a record's proof commits with.  Adoption writes nothing, so
        nothing else needs re-deciding there.
        """

        if not isinstance(declared, str) or not declared:
            return None
        pins = self._live_pins(norm)
        if pins is None or pins:
            return None
        cover, _detail = self._live_claim_cover(norm)
        if cover is None or cover:
            return None
        partials = self._inflight_partials(destination)
        if partials is None or partials:
            return None
        file_id = self._content_proof(norm, want, declared)
        if file_id is None:
            return None
        with self._ownership():
            if reader_lease.file_id_matches(
                    file_id, reader_lease.stat_identity(norm)):
                return want, declared, file_id
        return None

    def _content_proof(self, norm: str, want: int, digest: object,
                       ) -> dict[str, int] | None:
        """The identity of ``norm`` if its bytes hash to ``digest`` (#1081).

        The file is opened without following a symlink, must be a regular
        file of exactly ``want`` bytes on the inode first stat'ed, and must
        carry the same identity -- inode, size, mtime and ctime -- after the
        read as before it, so the digest describes the incarnation returned
        and not a file that changed underneath the read.  ``None`` for any
        other answer, including an unreadable file: this only ever adopts,
        so a failure costs a copy and never a verdict.

        A mismatch is remembered for this publisher by ``(identity,
        digest)``, so a name that waits out the grace is hashed once, not
        once per poll; any change to the file changes its identity and is
        hashed again.  The memo holds one row per mismatching name, at most
        one per entry of the range, and dies with the process.  Seconds and
        calls are recorded as the ``content_proof`` phase.
        """

        if not isinstance(digest, str) or not digest:
            return None
        before = reader_lease.stat_identity(norm)
        if before is None or int(before["size"]) != want:
            return None
        fence = (tuple(sorted(before.items())), digest)
        with self._lookup_lock:
            if self._content_misses.get(norm) == fence:
                return None
        hasher = hashlib.sha256()
        seen = 0
        with self.clock.timing("content_proof"):
            try:
                fd = os.open(norm, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            except OSError:
                return None
            try:
                info = os.fstat(fd)
                if (not statmod.S_ISREG(info.st_mode)
                        or info.st_ino != int(before["ino"])):
                    return None
                buffer = bytearray(reader_lease.CONTENT_BLOCK_BYTES)
                view = memoryview(buffer)
                while seen < want:
                    got = os.readv(fd, [view[:min(len(buffer), want - seen)]])
                    if not got:
                        break
                    hasher.update(view[:got])
                    seen += got
            except OSError:
                return None
            finally:
                os.close(fd)
        after = reader_lease.stat_identity(norm)
        if after is None or not reader_lease.file_id_matches(before, after):
            return None
        if seen != want or hasher.hexdigest() != digest:
            with self._lookup_lock:
                self._content_misses[norm] = fence
            return None
        with self._lookup_lock:
            self._content_misses.pop(norm, None)
        return after

    def _adopt_divergent(self, destination: Path, norm: str, want: int,
                         declared: object, source_id: str | None,
                         owners: _Owners, detail: str | None,
                         ) -> tuple[int, str, dict[str, int]] | None:
        """:meth:`try_adopt`'s answer for a name its search found divergent.

        Acts on nothing: a live owner or an unproven ending refuses before
        any copy, and otherwise the copy proceeds to a publication that
        decides the name under the stage lock.
        """

        def redo(fresh: _Owners) -> tuple:
            return self._decide(destination, want, declared, None, source_id,
                                heal=False, owners=fresh, content=False)

        with self._arbitration(owners, redo) as (state, rows, verdict, why):
            if state == "changed":
                if verdict[0] == "adopt":
                    return want, verdict[1], verdict[2]
                return None
            if state == "live":
                raise self._conflict(norm, declared, detail, rows)
            if state == "uncertain":
                self._unproven_seen(norm, declared, rows, why)
                raise PublicationRefused(f"{detail}; {why}")
            # ``ended``: the publication replaces it.  ``busy`` or ``moved``:
            # nothing was decided, and the publication arbitrates again.
            return None

    def publish(self, entry: dict[str, object], destination: Path,
                temp_path: Path, computed: str,
                stop: threading.Event | None = None,
                source_id: str | None = None,
                ) -> tuple[int, str, dict[str, int] | None]:
        """Decide one entry's publication; copy already verified in temp.

        Each poll decides first without the stage ownership lock (#981):
        the proof search and the pin, claim and in-flight censuses only
        read, and a ``wait`` acts on nothing, so a waiting poll never takes
        the lock at all.  A verdict that acts takes it and is re-established
        there before it acts: an adoption commits only while the destination
        still carries every field of the ``file_id`` its proof dated, and
        anything else -- a replacement, a refusal, or an adoption whose
        incarnation moved -- is decided again, in full, under the lock,
        exactly as before.  A fresh destination's rename therefore holds the
        lock for one ``lstat`` and the rename, not for anybody's proof.

        On a stage mover a divergent verdict is arbitrated by its owners'
        states (#966, :meth:`_arbitration`): a live owner is the terminal
        conflict, an unproven ending the retryable refusal, and a name every
        owner of which has ended is replaced by this copy.  A divergence
        first seen under the stage lock is decided on the next poll, since
        the owners' transition locks come before that lock.

        A ``"wait"`` verdict's third element names which of the four
        :data:`PUBLISH_WAIT_OWNED`-family kinds it is (#994).  This entry's
        time under each kind -- real elapsed seconds between the poll that
        first saw it and the poll that saw something else -- is totalled
        onto ``self.clock`` (:meth:`_PhaseClock.wait`) exactly once, when
        this call returns or raises, for the receipt's ``publish_waits`` to
        report and for the eventual refusal (:meth:`_act`) to name.  A wait
        that changes kind mid-poll is flushed under its old kind and
        restarted under its new one, so one entry can credit two kinds
        rather than mislabel the second as the first.
        """

        want = int(entry["bytes"])
        declared = entry.get("sha256")
        norm = os.path.normpath(str(destination))
        deadline = time.monotonic() + _PUBLISH_GRACE_S
        wait_kind: str | None = None
        wait_started: float | None = None
        try:
            while True:
                now = time.monotonic()
                heal = now >= deadline
                owners = _Owners() if self._arbitrates else None
                with self.clock.timing("publish_decide"):
                    verdict = self._decide(destination, want, declared, computed,
                                           source_id, heal=heal, owners=owners)
                if verdict[0] == "wait":
                    kind = verdict[2]
                    if wait_started is None:
                        wait_started = now
                    elif kind != wait_kind:
                        self.clock.wait(wait_kind, now - wait_started)
                        wait_started = now
                    wait_kind = kind
                if verdict[0] == "divergent":
                    done = self._publish_divergent(
                        destination, norm, temp_path, want, declared, computed,
                        source_id, heal, owners, verdict[1])
                    if done is not None:
                        return done
                elif verdict[0] != "wait":
                    with self._ownership():
                        if not (verdict[0] == "adopt"
                                and reader_lease.file_id_matches(
                                    verdict[2], reader_lease.stat_identity(norm))):
                            verdict = self._decide(
                                destination, want, declared, computed, source_id,
                                heal=heal,
                                owners=_Owners() if self._arbitrates else None,
                                content=False)
                        done = self._act(verdict, destination, temp_path, want,
                                         computed)
                        if done is not None:
                            return done
                if stop is not None and stop.is_set():
                    temp_path.unlink(missing_ok=True)
                    raise OSError(f"stopping before {destination} publishes")
                with self.clock.timing("publish_poll_sleep"):
                    time.sleep(_PUBLISH_POLL_S)
        finally:
            if wait_started is not None:
                self.clock.wait(wait_kind, time.monotonic() - wait_started)

    def _act(self, verdict: tuple, destination: Path, temp_path: Path,
             want: int, computed: str,
             ) -> tuple[int, str, dict[str, int] | None] | None:
        """Carry out a publication verdict under the stage lock.

        ``None`` for a verdict that acts on nothing -- a wait, or a
        divergence first seen under the lock -- so the caller polls again.
        """

        if verdict[0] == "replace":
            os.replace(temp_path, destination)
            self._note_replacement()
            self.clock.outcome("renamed")
            return want, computed, self._identity(destination)
        if verdict[0] == "adopt":
            temp_path.unlink(missing_ok=True)
            self.clock.outcome(verdict[3] if len(verdict) > 3
                               else "adopted_at_publication")
            return want, verdict[1], verdict[2]
        if verdict[0] == "refuse":
            temp_path.unlink(missing_ok=True)
            raise PublicationRefused(
                verdict[1],
                wait_verdict=verdict[2] if len(verdict) > 2 else None)
        return None

    def _publish_divergent(self, destination: Path, norm: str,
                           temp_path: Path, want: int, declared: object,
                           computed: str, source_id: str | None, heal: bool,
                           owners: _Owners, detail: str,
                           ) -> tuple[int, str, dict[str, int] | None] | None:
        """:meth:`publish`'s answer for a divergent verdict; ``None`` polls."""

        def redo(fresh: _Owners) -> tuple:
            return self._decide(destination, want, declared, computed,
                                source_id, heal=heal, owners=fresh,
                                content=False)

        with self._arbitration(owners, redo) as (state, rows, verdict, why):
            if state == "changed":
                return self._act(verdict, destination, temp_path, want,
                                 computed)
            if state in ("busy", "moved"):
                if heal:
                    temp_path.unlink(missing_ok=True)
                    raise PublicationRefused(
                        f"{detail}; {why} after the grace, deferring to retry")
                return None
            if state == "live":
                temp_path.unlink(missing_ok=True)
                raise self._conflict(norm, declared, detail, rows)
            if state == "uncertain":
                temp_path.unlink(missing_ok=True)
                self._unproven_seen(norm, declared, rows, why)
                raise PublicationRefused(f"{detail}; {why}")
            blocked = self._invalidation_blocked(norm, destination)
            if blocked:
                temp_path.unlink(missing_ok=True)
                raise PublicationRefused(
                    f"{detail}; every owner has ended, but {blocked}")
            os.replace(temp_path, destination)
            self._note_replacement()
            self._invalidated(norm, rows)
            self.clock.outcome("replaced_ended_owner")
            return want, computed, self._identity(destination)

    @contextmanager
    def _arbitration(self, owners: _Owners, redo):
        """Judge a divergent name's owners, then decide it again (#966).

        Before this, a name whose recorded owner held different bytes was
        refused forever: the refusal was retryable, the retry met the same
        owner, and a mover whose owner had long ended reran without end
        while holding fill.  The owners' states now decide it.

        Every owner that names it is held by its transition locks --
        consumers, then movers, each set sorted, taken without blocking so a
        long egress or claim never stalls a copy; contention is ``busy`` --
        and each owner not already proven ended in this run is judged under
        them.  Then, under the stage ownership lock, the name is decided
        again by ``redo`` with a fresh collector.  The lock order is the one
        every holder keeps: transition before ownership.

        An owner proven ended is remembered for the rest of the run, so a
        range whose names one dead owner holds is judged once, not once per
        name.  An ending is terminal for the queue unless the same key is
        resubmitted, which the dead-consumer pass and an operator both do,
        and a resubmitted mover may adopt the old bytes under a name this
        run has not reached yet.  So a remembered ending is re-checked under
        the locks at every act, by :meth:`_requeued`'s four stats rather
        than a full judgment; a return drops it and the name is judged
        again.  What a full judgment reads is listed in
        :meth:`_owner_state`.

        Yields ``(state, rows, verdict, why)``:

        * ``busy``: an owner's transition lock is held elsewhere; nothing
          was judged;
        * ``changed``: under the stage lock the name no longer diverges, and
          ``verdict`` is the fresh decision, for the caller to act on while
          the lock is held;
        * ``moved``: it still diverges, but an owner that was not held and
          judged names it now, or an owner remembered as ended is queued
          again;
        * ``live``: an owner's consumer is still queued or claimed -- a real
          conflict, refused terminally and never destroyed;
        * ``uncertain``: an ending could not be proven, or a fragment could
          not be read; the refusal stays retryable, as it always was, and
          the mover bounds its reruns (#1004 item 2,
          :func:`bound_unproven_endings`);
        * ``ended``: every owner that names it now has provably ended.

        ``rows`` are the owners that name it now, each with its state.
        """

        with self._arbitration_lock:
            pending = sorted(pair for pair in owners.pairs
                             if pair not in self._ended_owners)
            judged: dict[tuple[str, str], dict[str, object]] = {}
            with ExitStack() as held:
                consumers = sorted({c for c, _ in owners.pairs
                                    if is_action_key(c)})
                movers = sorted({m for _, m in owners.pairs
                                 if is_action_key(m)})
                for key in consumers:
                    if not held.enter_context(self.queue.action_transition_lock(
                            key, blocking=False)):
                        yield ("busy", [], None,
                               f"owner consumer {key[:12]}'s transition lock "
                               f"is held")
                        return
                for key in movers:
                    if not held.enter_context(self.queue.mover_transition_lock(
                            key, blocking=False)):
                        yield ("busy", [], None,
                               f"owner mover {key[:12]}'s transition lock "
                               f"is held")
                        return
                taken = time.perf_counter()
                held.callback(lambda: self.clock.add(
                    "owner_locks_held", time.perf_counter() - taken))
                if pending:
                    with self.clock.timing("owner_judgement"):
                        rows = self._judge_owners(set(pending))
                    for row in rows:
                        pair = (str(row["consumer_action_key"]),
                                str(row["mover_action_key"]))
                        judged[pair] = row
                        if row["state"] == "ended":
                            self._ended_owners[pair] = row
                with self._ownership():
                    fresh = _Owners()
                    with self.clock.timing("publish_decide"):
                        verdict = redo(fresh)
                    if verdict[0] != "divergent":
                        yield "changed", [], verdict, ""
                        return
                    if not fresh.pairs <= owners.pairs:
                        yield ("moved", [], verdict,
                               "an owner that was not judged names it now")
                        return
                    for pair in sorted(fresh.pairs - judged.keys()):
                        requeued = self._requeued(pair)
                        if requeued:
                            del self._ended_owners[pair]
                            yield "moved", [], verdict, requeued
                            return
                    known = {**self._ended_owners, **judged}
                    rows = [known[pair] for pair in sorted(fresh.pairs)]
                    state, why = _combined_owner_state(rows, fresh.complete)
                    yield state, rows, verdict, why

    def _requeued(self, pair: tuple[str, str]) -> str:
        """Why an owner remembered as ended may have come back, or ``""``.

        A remembered ending is re-checked at every act by the records a
        resubmission writes: a queue record for its consumer or its mover
        in ``ready/`` or ``claimed/``.  Four stats and no listing; the
        caller holds both owners' transition locks, so no record appears
        while it acts.  An unreadable stat counts as a return: the owner is
        judged again in full.

        A sibling's consumer is this copy's own, queued or claimed by
        construction while its movers run, so only the sibling's mover is
        re-checked, exactly as :meth:`_owner_state` judges it.  Checking the
        consumer too read every remembered sibling ending as a return and
        judged it again for every name, one publication poll each (#1004
        item 5).
        """

        consumer, mover = pair
        for key in ((mover,) if consumer == self.consumer else pair):
            for state in (pool.record_store.READY, pool.record_store.CLAIMED):
                try:
                    self.queue.records.item_metadata(state, key)
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    return (f"owner {key[:12]}'s {state} record is "
                            f"unreadable: {exc}")
                return f"owner {key[:12]} is {state} again"
        return ""

    def _judge_owners(self, pairs: set[tuple[str, str]],
                      ) -> list[dict[str, object]]:
        """Each owner's state, in sorted order; the caller holds its locks."""

        rows = []
        for consumer, mover in sorted(pairs):
            state, why = self._owner_state(consumer, mover, unended_owner)
            row: dict[str, object] = {"consumer_action_key": consumer,
                                      "mover_action_key": mover,
                                      "state": state}
            if why:
                row["why"] = why
            if state == "uncertain":
                # Whether the uncertainty ends by itself (#1004 item 2).
                row["settles"] = self._ending_settles(consumer, mover)
            rows.append(row)
        return rows

    def _ending_settles(self, consumer: str, mover: str) -> bool:
        """Whether an unproven ending is one that settles by itself.

        It settles when either key is queued, claimed or leased, or when its
        queue state could not be read: the claim ends, the lease is reaped
        within ``LEASE_TIMEOUT_S``, the mount answers.  It does not when both
        keys are out of the queue and the proof still fails -- no outcome
        record, two of them, one that does not parse, a key that is not an
        action key: nothing will change that but an operator.  Read only for
        an owner already judged uncertain, so an ended owner pays nothing;
        the caller holds both keys' transition locks.
        """

        if not (is_action_key(consumer) and is_action_key(mover)):
            return False
        for key in ((mover,) if consumer == self.consumer
                    else (consumer, mover)):
            live, error = residency_plan.live_state(self.queue, key)
            if error or live is not None:
                return True
        return False

    def _owner_state(self, consumer: str, mover: str,
                     unended) -> tuple[str, str]:
        """``live``, ``ended`` or ``uncertain``, with why when uncertain.

        ``ended`` needs both facts below, each a queue record rather than a
        clock.  The consumer is neither queued nor claimed and has exactly
        one outcome (``staged_path.unended_owner``, the proof the
        dead-owner sweep uses).  The fragment's mover is neither queued nor
        claimed (``residency_plan.live_state``).  Both reads count any entry
        under ``claimed/`` -- the record, its lease, its status sidecar --
        as claimed, so a lease outliving its record reads as not ended.

        ``live`` is a consumer whose queue record itself is in ``claimed/``
        or ``ready/``.  Everything else -- no outcome, a lease without a
        record, a queued or leased mover, a record that cannot be read, a
        key that is not an action key -- is ``uncertain``.

        A sibling -- another mover of this copy's own consumer, as when a
        forward and a reverse pass, or two phases of one plan, stage one
        extent onto one name -- is judged by its mover alone.  Its consumer
        is this copy's own and is live by construction, so it is no
        conflict; the consumer's readers hold pins, which the act checks.

        The consumer's plan is deliberately not a fact here: a DONE
        consumer keeps its frozen plan so a retry republishes the same
        children (``tier_loop``'s dead-consumer pass), and the plan's other
        movers do not name this path.  A live claim that covers it is
        caught by :meth:`_invalidation_blocked`.

        Reads, for an ended owner: two stats and, on a miss, one listing
        each of ``ready/`` and ``claimed/``, once for the consumer and once
        for the mover, plus the consumer's outcome records.  A live owner
        costs one or two more stats of its consumer's record.
        """

        if not (is_action_key(consumer) and is_action_key(mover)):
            return "uncertain", "its fragment does not name two action keys"
        if consumer != self.consumer:
            why = unended(self.queue, consumer)
            if why:
                for state in (pool.record_store.CLAIMED, pool.record_store.READY):
                    try:
                        self.queue.records.item_metadata(state, consumer)
                    except FileNotFoundError:
                        continue
                    except OSError as exc:
                        return "uncertain", f"{why}; {exc}"
                    return "live", ""
                return "uncertain", why
        live, error = residency_plan.live_state(self.queue, mover)
        if error:
            return ("uncertain",
                    f"its mover's queue state is uncertain: {error}")
        if live is not None:
            return "uncertain", f"its mover {mover[:12]} is still {live}"
        return "ended", ""

    def _invalidation_blocked(self, norm: str, destination: Path) -> str:
        """Why an ended owner's name may not be invalidated now, or ``""``.

        The same censuses a replacement of unattributed residue passes: no
        live pin, no other live mover claim covering the name, no sibling
        copy in flight, each read clean.  Any of them refuses retryably at
        once instead of waiting out a grace: the owners are proven ended,
        so what remains is a reader or a sibling copy, and the retry
        re-judges both.
        """

        pins = self._live_pins(norm)
        if pins is None:
            return "the pin census is unreadable"
        if pins:
            return f"it is live-pinned by {pins}"
        cover, cover_detail = self._live_claim_cover(norm)
        if cover is None:
            return f"the claim census is unreadable: {cover_detail}"
        if cover:
            return "another live mover claim covers it"
        partials = self._inflight_partials(destination)
        if partials is None:
            return "the copy census is unreadable"
        if partials:
            return f"a copy is in flight ({partials})"
        return ""

    def _invalidated(self, norm: str, rows: list[dict[str, object]]) -> None:
        """Record one replaced name; the caller holds the arbitration lock."""

        self.invalidated.append({"stage_path": norm, "owners": list(rows)})

    def _unproven_seen(self, norm: str, declared: object,
                       rows: list[dict[str, object]], why: str) -> None:
        """Record one name refused on an unproven ending (#1004 item 2).

        The owners are those not proven ended; none when every owner ended
        but a fragment could not be read, which never settles by itself
        either.  The caller holds the arbitration lock.
        """

        held = [dict(row) for row in rows if row.get("state") != "ended"]
        self.unproven.append({
            "stage_path": norm,
            "declared_sha256": (declared if isinstance(declared, str)
                                and declared else None),
            "why": why,
            "owners": held,
            "settles": any(bool(row.get("settles")) for row in held),
        })

    def _conflict(self, norm: str, declared: object, detail: str | None,
                  rows: list[dict[str, object]]) -> PublicationRefused:
        """The terminal refusal for a name a live owner holds, naming both."""

        live = [row for row in rows if row["state"] == "live"]
        held_by = ", ".join(
            f"consumer {row['consumer_action_key']} mover "
            f"{row['mover_action_key']}" for row in live)
        return PublicationRefused(
            f"{detail}; it is held by live owner {held_by}, and this copy "
            f"is consumer {self.consumer} mover {self.mover}: a conflict "
            f"for an owner to resolve, never retried",
            refusal=STAGED_DESTINATION_CONFLICT,
            conflict={
                "stage_path": norm,
                "consumer_action_key": self.consumer,
                "mover_action_key": self.mover,
                "manifest_sha256": self.manifest_sha256,
                "declared_sha256": (declared if isinstance(declared, str)
                                    and declared else None),
                "owners": list(rows),
            })

    @staticmethod
    def _identity(path: Path) -> dict[str, int] | None:
        try:
            info = os.stat(path)
            return {"ino": info.st_ino, "size": info.st_size,
                    "mtime_ns": info.st_mtime_ns,
                    "ctime_ns": int(getattr(info, "st_ctime_ns", 0))}
        except OSError:
            return None

    def _decide(self, destination: Path, want: int, declared: object,
                computed: str | None, source_id: str | None, heal: bool,
                owners: _Owners | None = None, content: bool = True) -> tuple:
        """One publication verdict for ``destination``, deciding nothing twice.

        ``owners``, when given, collects who names a divergent destination,
        and the verdict for one is then ``("divergent", detail)`` rather than
        a refusal, for :meth:`_arbitration` to settle (#966).  Without it a
        divergence refuses, as it always did.

        ``content`` lets positive absence be proven by the destination's own
        bytes (#1081, :meth:`_content_proof`).  Every caller that holds a
        lock passes ``False``: the hash reads a whole file, and a verdict
        decided under a lock only confirms one decided before it.  Without
        it positive absence waits, then heals, as it always did.

        A ``"wait"`` verdict, and the ``"refuse"`` a wait settles to after
        the grace, each carry a third element naming which of the four
        :data:`PUBLISH_WAIT_OWNED`-family kinds it is (#994), for
        :meth:`publish` to total onto the receipt's ``publish_waits`` and
        for the eventual refusal to name.  Every other verdict --
        ``"replace"``, ``"adopt"``, ``"divergent"``, and a ``"refuse"`` that
        never waited (an unstatable or non-regular destination, an
        unreadable census, a live pin) -- stays the plain two-element tuple
        it always was.
        """

        norm = os.path.normpath(str(destination))
        try:
            present = os.lstat(destination)
        except FileNotFoundError:
            return ("replace",)
        except OSError as exc:
            return ("refuse",
                    f"staged destination unstatable, not replacing "
                    f"{destination}: {exc}")
        if not statmod.S_ISREG(present.st_mode):
            return ("refuse",
                    f"staged destination is not a regular file, not "
                    f"replacing: {destination}")
        proof, standing, detail = self._proof_search(
            norm, want, declared, computed=computed, source_id=source_id,
            owners=owners)
        if proof is not None:
            return ("adopt", proof[0], proof[1])
        if standing == "unknown":
            return ("refuse",
                    f"staged publication proof unreadable for "
                    f"{destination}: {detail}; not replacing")
        if standing == "divergent":
            return ("divergent" if owners is not None else "refuse", detail)
        pins = self._live_pins(norm)
        if pins is None:
            return ("refuse",
                    f"staged pin census unreadable for {destination}; "
                    f"not replacing")
        if pins:
            return ("refuse",
                    f"shared staged name is live-pinned by "
                    f"{pins}, not replacing: {destination}")
        if standing == "owned":
            # A fragment vouches but no usable date exists: a vouch the
            # same-key resume kept undated because its bytes no longer
            # qualify, a sidecar dating an incarnation this name no longer
            # carries, or a vouch filed before #1087, when a crash between
            # the fragment and its sidecar could leave one.  Movers now
            # write the sidecar first, so an interrupted publication leaves
            # a date without a vouch, which reads as absence below.
            # Defer to the stall policy's retry; never replace what
            # another publication names -- including after the grace.
            if heal:
                temp_note = ("still unproven after the grace, "
                             "deferring to retry")
                return ("refuse",
                        f"shared staged name is published elsewhere, "
                        f"{temp_note}: {destination}", PUBLISH_WAIT_OWNED)
            return ("wait",
                    f"shared staged name is published elsewhere, "
                    f"deferring: {destination}", PUBLISH_WAIT_OWNED)
        cover, cover_detail = self._live_claim_cover(norm)
        if cover is None:
            return ("refuse",
                    f"staged claim census unreadable for {destination}: "
                    f"{cover_detail}; not replacing")
        if cover:
            # A live mover claim covers this name: its fragment may still
            # land.  Defer; the retry adopts once it does.  After the
            # grace the answer is still refuse, never replace.
            if heal:
                return ("refuse",
                        f"shared staged name still has a live publisher "
                        f"after the grace, deferring to retry: "
                        f"{destination}", PUBLISH_WAIT_LIVE_PUBLISHER)
            return ("wait",
                    f"shared staged name has a live publisher, "
                    f"deferring: {destination}", PUBLISH_WAIT_LIVE_PUBLISHER)
        partials = self._inflight_partials(destination)
        if partials is None:
            return ("refuse",
                    f"staged copy census unreadable for {destination}; "
                    f"not replacing")
        if partials:
            if heal:
                return ("refuse",
                        f"shared staged name still has a copy in flight "
                        f"({partials}) after the grace, deferring to "
                        f"retry: {destination}", PUBLISH_WAIT_IN_FLIGHT)
            return ("wait",
                    f"shared staged name has a copy in flight "
                    f"({partials}), deferring: {destination}",
                    PUBLISH_WAIT_IN_FLIGHT)
        # Positive absence, re-verified: no fragment, no pin, no live
        # claim, no copy in flight, every census clean.  Bytes that hash to
        # the trusted digest -- the declared one, or the copy's own for a
        # digest-less manifest -- are adopted now, grace or no grace
        # (#1081): nothing is replaced, so nothing a late record could date
        # is lost.  Otherwise this still waits for a late fragment before
        # the grace expires; only a grace that expires with the absence
        # intact heals the name as orphan/crash residue -- elapsed time
        # alone never permits it.
        if content:
            trusted = (declared if isinstance(declared, str) and declared
                       else computed)
            file_id = self._content_proof(norm, want, trusted)
            if file_id is not None:
                return ("adopt", trusted, file_id,
                        "adopted_by_content_at_publication")
        if heal:
            return ("replace",)
        return ("wait",
                f"shared staged name unattributed, deferring: {destination}",
                PUBLISH_WAIT_UNATTRIBUTED)

    def _range_partial_census(self, directory: Path) -> tuple[str, ...] | None:
        """One range directory's partial temporaries, as a fenced hint (#1028).

        Listed once per :func:`kept_reads.filesystem_trust.directory_before` of the directory
        and remembered -- the stamp taken before the scan, kept only while
        a fresh directory version still equals it, the same fence the
        forest census's listings are kept under (#1004, #992) --
        :meth:`_inflight_partials` revalidates with one directory version
        per name and the directory is listed again only when it moved.  A
        version the directory's own clock tick refuses, and a scan the
        directory moved, are returned once and never remembered: the next
        name lists again, so a partial created in the same tick or during
        a scan cannot hide behind a remembered listing.  On a filesystem
        whose directory times are not this kernel's, no stamp is ever
        trusted and every name lists, as before.  ``None`` for a directory
        that could not be read, never remembered: every name fails closed
        on its own, as the per-name listing did.  The reads run without
        :attr:`_range_lock`; only a fenced listing is stored under it.
        """

        with self._partial_names.acquire(directory) as capture:
            if capture.hit:
                return capture.value
            try:
                names = tuple(sorted(
                    entry.name for entry in os.scandir(directory)
                    if entry.name.endswith(".partial")))
            except OSError:
                return None
            return capture.commit(names)

    def _live_pins(self, norm: str) -> list[str] | None:
        """Pin ids live on one staged path, or None when unknowable."""

        try:
            owners, tainted = reader_lease.live_for(
                self.queue, {norm}, residency_root=self.residency_root)
        except Exception:
            return None
        if tainted:
            return None
        return sorted(owners.get(norm, []))[:5]

    def _live_claim_cover(self, norm: str) -> tuple[bool | None, str]:
        """Whether another live mover claim covers one staged name.

        The staged-path containment census attributes
        in-flight copies from their sealed claims, which exist before any
        fragment does.  This mover's own claim is excluded, so a mover
        never defers to itself.  ``None`` means unknowable (fail closed);
        the claim paths are stage-root-relative there, joined here.

        Runs once per clean entry at content adoption, once per publish
        poll while a name waits, and once per divergence invalidation
        (#1089) -- so this passes ``self._claim_memo``, held for this
        publisher's whole run, instead of deriving every claimed mover's
        staged paths fresh on each check.  The memo does not weaken the
        gate: it only remembers a claim's *derived* paths, which its own
        sealed request and manifest fix forever; the claim listing and
        each claim record are still read fresh inside ``_claimed_paths``
        on every call, so a claim that appears, ends or changes is still
        seen at once.  Unlocked across the census (see ``__init__``).
        """

        try:
            paths, tainted = claimed_paths(
                self.queue, str(self.tier_id), self.cas_root,
                exclude={str(self.mover)}, memo=self._claim_memo)
        except Exception as exc:
            return None, str(exc)
        if tainted:
            return None, "; ".join(tainted[:3])
        root = str(self.stage_root)
        for relative in paths:
            if os.path.normpath(os.path.join(root, str(relative))) == norm:
                return True, "live mover claim"
        return False, ""

    def _inflight_partials(self, destination: Path) -> list[str] | None:
        """Sibling copy temporaries for one destination, sans this mover's.

        The owner-keyed ``.<name>.<owner>.partial`` convention (plus the
        legacy shared ``.<name>.partial``) is this copier's own namespace:
        a sibling means another copy is in flight -- or crashed, in which
        case the sweep reaps it and a later retry proceeds.  ``None``
        means the directory could not be read (fail closed).

        The range directory is listed once per
        :func:`kept_reads.filesystem_trust.directory_before` and remembered
        (#1028, :meth:`_range_partial_census`); each name revalidates the
        listing with one directory version
        (:func:`directory_current`) and the directory is listed
        again only when it moved.  Each name's check filters the
        remembered listing by the same prefix rule and re-stats only the
        names that pass it; a candidate that cannot be stat'ed -- one
        removed since the listing among them -- fails the name closed
        exactly as the per-name listing's stat did.
        A partial filed after the listing moves the directory, so the next
        name of the same range sees it; the publisher's own temporary and
        rename move it every entry too, so a range of replacements lists
        it once an entry, as the per-name listing did -- the hoist's win
        is every decision that runs while the directory holds still, and
        on a filesystem whose directory times are not this kernel's the
        fence never holds and every name lists, as before.  A copy in
        flight is still attributed first by its sealed claim
        (:meth:`_live_claim_cover`).
        """

        own = f".{destination.name}.{str(self.mover)[:16]}.partial"
        # The name decides membership; the stat only confirms what a matching
        # name already is.  Asking them in that order spends one string
        # compare on a sibling that cannot be a partial, instead of a stat on
        # every dirent of the remembered listing.
        prefix = f".{destination.name}."
        remembered = self._range_partial_census(destination.parent)
        if remembered is None:
            return None
        parent = os.path.normpath(str(destination.parent))
        out = []
        for name in remembered:
            if name == own or not name.startswith(prefix):
                continue
            try:
                info = os.stat(os.path.join(parent, name),
                               follow_symlinks=False)
            except OSError:
                return None
            if statmod.S_ISREG(info.st_mode):
                out.append(name)
        # ``remembered`` is sorted, so ``out`` already is.
        return out[:5]

    def _index_bytes(self) -> int:
        """What the reuse index currently costs, in retained bytes.

        Every retained structure is in here -- the shared interned table,
        the fragment records and the sidecar records -- each charged once
        from the object it holds.  This is the retained cost, not the peak:
        a 22 MB fragment's decode buffer is freed before the next lookup and
        belongs to the process's peak RSS instead.
        """

        return self._interned_cost + self._fragment_cost + self._material_cost

    def _forget_fragment(self, key: str) -> None:
        """Drop one fragment entry and discount exactly what it was charged."""

        previous = self._fragments.pop(key, None)
        if previous is not None:
            self._fragment_cost -= previous[2]

    def _forget_material(self, key: tuple[str, str]) -> None:
        """Drop one sidecar entry and discount exactly what it was charged."""

        previous = self._materials.pop(key, None)
        if previous is not None:
            self._material_cost -= previous[2]

    def _retain_table(self, table: _PackedPaths,
                      base: int) -> _PackedPaths | None:
        """The one shared table this record references, or ``None``.

        ``base`` is the record's own cost (file key, mover, per-record
        overhead, and for a sidecar its mention values) and is the whole
        charge the caller stores with the record.  A table's own bytes live
        in the interned charge alone: a table already interned is shared and
        costs nothing more, and a new one is charged and inserted only after
        the ceiling admits ``base`` plus its storage -- so a refusal leaves
        the index exactly as it was, never half-inserted, never a duplicate
        blob beside an equal interned one, and never a table paid for twice.

        ``_room_for`` may reclaim unreferenced tables while deciding.  A
        reclaim can only drop tables no retained record names, so the lookup
        is repeated after the check and the table re-interned and charged if
        the check removed the one it was about to share.
        """

        key = table.intern_key()
        cached = self._interned.get(key)
        if cached is None:
            if not self._room_for(base + table.retained_bytes):
                return None
            self._interned[key] = table
            self._interned_cost += table.retained_bytes
            return table
        if not self._room_for(base):
            return None
        if self._interned.get(key) is cached:
            return cached
        if not self._room_for(base + cached.retained_bytes):
            return None
        self._interned[key] = cached
        self._interned_cost += cached.retained_bytes
        return cached

    def _reclaim(self) -> None:
        """Give back what the index holds for metadata that is no longer there.

        Two kinds of residue, both of which would otherwise sit in the index
        for the life of the publisher.  A file that *disappears* from the
        forest is never visited again -- the per-lookup drop paths only run
        on a file the directory scan still lists -- so its entry is dropped
        here, by the one authority that can say it is gone: a stat.  And a
        dropped or replaced record leaves its packed path table interned but
        unreferenced, so the interned set is rebuilt from the tables the
        retained records still reference, and its charge recomputed from
        their real packed bytes rather than adjusted -- the one accounting
        that cannot drift.  One actual table stays per referenced identity,
        however many records share it.
        """

        for key in [k for k in self._fragments if not os.path.exists(k)]:
            self._forget_fragment(key)
        for key in [k for k in self._materials
                    if not reader_lease.material_path(
                        self.residency_root, k[0], k[1]).exists()]:
            self._forget_material(key)
        live: dict[tuple[bytes, bytes], _PackedPaths] = {}
        for _version, record, _charge in self._fragments.values():
            if isinstance(record, tuple) and isinstance(record[1],
                                                        _PackedPaths):
                if record[1].keys:
                    live[record[1].intern_key()] = record[1]
        for _version, record, _charge in self._materials.values():
            if isinstance(record, _PackedMentions) and record.paths.keys:
                live[record.paths.intern_key()] = record.paths
        self._interned = live
        self._interned_cost = sum(table.retained_bytes
                                  for table in live.values())

    def _room_for(self, cost: int) -> bool:
        """Whether the index can hold ``cost`` more bytes, reclaiming first.

        ``cost`` prices the record from the real packed object and charges a
        new shared table only when it is genuinely new, so a yes here is
        never optimistic.  A refusal only ever follows a reclaim, so it means
        the index is genuinely that full -- never that its accounting
        drifted.
        """

        if self._index_bytes() + cost <= _INDEX_BUDGET_BYTES:
            return True
        self._reclaim()
        return self._index_bytes() + cost <= _INDEX_BUDGET_BYTES

    def _keep_fragment(self, key: str, version: tuple, record: object,
                       charge: int) -> None:
        """Retain one fragment entry, replacing and re-pricing any previous."""

        self._forget_fragment(key)
        self._fragments[key] = (version, record, charge)
        self._fragment_cost += charge

    def _remember_verdict(self, key: str, version: tuple,
                          record: object) -> None:
        """Retain a corrupt-or-foreign file's verdict, if there is room.

        A row that names no path still costs its key, its version tuple and
        its dict slot.  Pricing it at zero is how an index with a budget
        grows without limit: a directory of empty or malformed files would
        be admitted without end.  So it is charged and checked like any
        other, and refused like any other when the ceiling is reached.
        """

        self._forget_fragment(key)
        charge = _RECORD_OVERHEAD_BYTES + _string_bytes(key)
        if not self._room_for(charge):
            return
        self._fragments[key] = (version, record, charge)
        self._fragment_cost += charge

    def _fragment_record(self, path: Path, observed: object = None) -> object:
        """One fragment file's reusable record, parsed once per version.

        ``None`` for a file that is gone or is not a residency fragment,
        ``"tainted"`` for one that cannot be read or parsed, otherwise
        ``(mover_action_key, packed set of the normalized staged paths its
        entries name)`` -- everything :meth:`_proof_candidate` asks of a
        fragment and nothing else, so a 22 MB fragment is retained as a
        packed projection rather than as its parsed document or its object
        graph.

        A ``ValueError`` verdict caches against the version that produced
        it: corrupt content cannot heal without the file changing.  An
        ``OSError`` never caches -- a permission or transport failure is
        transient, and the pre-fix code re-attempted it on every lookup, so
        unreadable stays freshly unreadable rather than becoming a held
        verdict.  The stat happens on every lookup, so an added, changed or
        removed file is seen before any decision uses it; ``observed`` is
        that stat when the caller took it before the lookup lock
        (:func:`_observe`).
        """

        key = str(path)
        current = _observe(path) if observed is None else observed
        if isinstance(current, FileNotFoundError):
            self._forget_fragment(key)
            return None
        if isinstance(current, OSError):
            self._forget_fragment(key)
            return "tainted"
        cached = self._fragments.get(key)
        if cached is not None and cached[0] == current:
            return cached[1]
        try:
            version, raw = _read_metadata(path)
        except FileNotFoundError:
            self._forget_fragment(key)
            return None
        except OSError:
            self._forget_fragment(key)
            return "tainted"
        try:
            fragment = json.loads(raw)
        except ValueError:
            self._remember_verdict(key, version, "tainted")
            return "tainted"
        if (not isinstance(fragment, dict)
                or fragment.get("schema")
                != residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1):
            self._remember_verdict(key, version, None)
            return None
        names: set[str] = set()
        for entry in (fragment.get("entries") or {}).values():
            if not isinstance(entry, dict):
                continue
            staged = entry.get("stage_path")
            if not staged:
                # ``normpath("")`` is ``"."``, which never equals an absolute
                # destination -- the pre-fix loop skipped these the same way.
                continue
            names.add(os.path.normpath(str(staged)))
        mover = str(fragment.get("mover_action_key") or "")
        # Priced from the real packed object before anything is interned, so
        # a record that will not be kept leaves nothing behind.
        self._forget_fragment(key)
        base = (_RECORD_OVERHEAD_BYTES + _string_bytes(key)
                + _string_bytes(mover))
        table = _PackedPaths(names)
        if not names:
            if not self._room_for(base):
                return mover, _EMPTY_PATHS
            self._keep_fragment(key, version, (mover, _EMPTY_PATHS), base)
            return mover, _EMPTY_PATHS
        retained = self._retain_table(table, base)
        if retained is None:
            return mover, table
        record = (mover, retained)
        # Anchored on the version the read saw, not the version the stat saw:
        # the content held is the content those bytes carried.
        self._keep_fragment(key, version, record, base)
        return record

    def _material_record(self, consumer: str, mover: str,
                         observed: object = None) -> object:
        """One sidecar's reusable record, validated once per version.

        ``None`` (absent -- an undated vouch), ``"tainted"`` (unreadable or
        invalid), or a packed projection whose ``get`` returns the ordered
        ``(bytes, sha256, file_id)`` mentions one normalized staged path
        carries, exactly as the parsed mapping did.  Ordered and complete
        per path because the digest search and the identity search may
        legitimately land on different mentions of one path.

        ``reader_lease.validate_material`` still runs, so the strictness a
        sidecar is held to is unchanged; only the repeated read is removed.
        A validated mention the fixed record cannot carry exactly -- an
        arbitrary-size integer identity -- is decided from the freshly
        parsed object, uncached and unretained, never truncated.  As with
        fragments, an ``OSError`` never caches, and ``observed`` is the
        per-lookup stat when the caller took it before the lookup lock.
        """

        cache_key = (consumer, mover)
        path = reader_lease.material_path(self.residency_root, consumer, mover)
        current = _observe(path) if observed is None else observed
        if isinstance(current, FileNotFoundError):
            self._forget_material(cache_key)
            return None
        if isinstance(current, OSError):
            self._forget_material(cache_key)
            return "tainted"
        cached = self._materials.get(cache_key)
        if cached is not None and cached[0] == current:
            return cached[1]
        try:
            version, raw = _read_metadata(path)
        except FileNotFoundError:
            self._forget_material(cache_key)
            return None
        except OSError:
            self._forget_material(cache_key)
            return "tainted"
        try:
            body = reader_lease.validate_material(json.loads(raw))
        except ValueError:
            self._forget_material(cache_key)
            charge = _RECORD_OVERHEAD_BYTES + _string_bytes(consumer) \
                + _string_bytes(mover)
            if self._room_for(charge):
                self._materials[cache_key] = (version, "tainted", charge)
                self._material_cost += charge
            return "tainted"
        mentions: dict[str, tuple] = {}
        for mention in (body.get("entries") or {}).values():
            normalized = os.path.normpath(str(mention["stage_path"]))
            mentions[normalized] = mentions.get(normalized, ()) + ((
                mention.get("bytes"), mention.get("sha256"),
                mention.get("file_id")),)
        # Priced from the real packed object before anything is interned, so
        # a record that will not be kept leaves nothing behind.
        self._forget_material(cache_key)
        compact = _PackedMentions.build(mentions)
        if compact is None:
            return mentions
        base = (_RECORD_OVERHEAD_BYTES + _string_bytes(consumer)
                + _string_bytes(mover) + compact.values_bytes)
        table = self._retain_table(compact.paths, base)
        if table is None:
            return mentions
        if table is not compact.paths:
            compact = compact.shared_with(table)
        self._materials[cache_key] = (version, compact, base)
        self._material_cost += base
        return compact

    @staticmethod
    def _published_source_id(norm: str) -> str | None:
        """The origin identity a published file still carries, if any."""

        try:
            raw = os.getxattr(norm, STAGE_SOURCE_XATTR)
        except OSError:
            return None
        try:
            return raw.decode()
        except ValueError:
            return None

    def _proof_search(self, norm: str, want: int, declared: object,
                      computed: str | None = None,
                      source_id: str | None = None,
                      owners: _Owners | None = None,
                      ) -> tuple[tuple[str, dict[str, int]] | None, str,
                                 str | None]:
        """An adoptable publication of these bytes, if one is proven.

        Returns ``(proof, standing, detail)`` where ``proof`` is
        ``(record_digest, file_id)`` and ``standing`` is one of:

        * ``"proof"`` -- some consumer's fragment entry names this path
          with equal bytes, a sidecar digest that matches, and a live
          stat equal to the sidecar's file identity.  The match is the
          declared digest; the copier's computed digest for digest-less
          manifests (no repeat hash of the existing file); or the origin
          fast path (the published file still carries this copy's source
          identity) with the sidecar digest riding along;
        * ``"divergent"`` -- a record that dates the *current* incarnation
          names the path but the bytes are provably not these: a digest
          mismatch, a different size on the same inode (no legitimate
          publication writes in place), or -- when only ``mtime_ns``/
          ``ctime_ns`` drifted on a same-inode, same-size record -- a
          content check (:meth:`_content_proof`) against the record's own
          digest that failed or could not run.  Immediate refuse, never
          wait.  Same inode and size with drifted timestamps is not itself
          divergence: it is also what a freed inode number reused for the
          very next create in the same directory looks like (#1078), and
          what an NFS delegation recall looks like (#1096), so whether it
          diverges is settled by content first, exactly as #1096 settles a
          recall -- and a confirmed match falls to ``"owned"`` below, not
          to a proof, since matching bytes on a reused inode say nothing
          about an unbroken lineage the way an exact stat match does;
          A record whose ``file_id`` names a *different inode* -- the name
          was replaced by a real publication, exactly what the pre-fix
          unconditional rename did -- says nothing about the bytes that
          are there now, so it is skipped and the search keeps looking
          for one that dates the current incarnation (#755);
        * ``"owned"`` -- a fragment names the path without proving it
          (sidecar missing, undated, dating a superseded incarnation, or a
          reused-inode record whose content settled clean, #1078): defer,
          a rerun may date it;
        * ``"clean"`` -- no fragment names the path at all;
        * ``"unknown"`` -- unreadable proof state: fail closed even over
          an otherwise valid proof, since an unreadable fragment might
          name this very path.

        No payload is hashed here: the sidecar digest is the copy-time
        content proof, and stat stability is the change detection.

        The forest is read through :meth:`_shared_census`: one directory
        census, begun after this call began, serves every search waiting on
        it (#981).  Each search then decides its own path from that census
        in the same order, with the same verdicts, and takes the sidecar and
        the live destination stat itself, fresh.

        ``owners`` (#966): without it a divergence returns at once, as it
        always did.  With it the search reads the rest of the census, which
        is already in memory, and collects every other ``(consumer, mover)``
        that names the path with a record that is not a superseded
        incarnation's date; an unreadable fragment or child directory marks
        the collection incomplete.  Divergence still outranks an unreadable
        record, exactly as the early return made it.  The collection is
        filled on every search but meaningful only on a divergence.
        """

        census = self._shared_census()
        if census[0] == "absent":
            return None, "clean", None
        if census[0] == "unreadable":
            return None, "unknown", census[1]
        standing = "clean"
        unknown: str | None = None
        found: tuple[str, dict[str, int]] | None = None
        divergent = False
        own = (self.consumer, str(self.mover))
        for child, name, path, fragment in census[1]:
            if name is None:
                # The child directory itself could not be listed.
                unknown = path
                if owners is not None:
                    owners.complete = False
                continue
            if fragment is _LOOK_AGAIN:
                observed = _observe(path)
                with self._lookup_lock:
                    fragment = self._fragment_record(path, observed)
            candidate = self._candidate_from_record(
                fragment, child, norm, want, declared,
                computed=computed, source_id=source_id)
            if (owners is not None and candidate is not None
                    and candidate not in ("tainted", "stale")
                    and isinstance(fragment, tuple)
                    and (child, str(fragment[0])) != own):
                owners.pairs.add((child, str(fragment[0])))
            if candidate == "tainted":
                unknown = f"{child}/{name}: unreadable"
                if owners is not None:
                    owners.complete = False
            elif candidate == "divergent":
                if owners is None:
                    return None, "divergent", _divergent_detail(norm)
                divergent = True
            elif candidate in ("owned", "stale"):
                # ``owned``: a vouch without a usable date.  ``stale``:
                # a date for an incarnation this name no longer carries
                # -- not evidence that the current bytes differ, so a
                # later record may still prove them.  Neither adopts
                # and neither permits replacement.
                standing = "owned"
            elif candidate is not None and found is None:
                found = candidate
        if divergent:
            return None, "divergent", _divergent_detail(norm)
        if unknown is not None:
            return None, "unknown", unknown
        if found is not None:
            return found, "proof", None
        return None, standing, None

    def _shared_census(self) -> tuple:
        """A census of the residency forest that began after this call did.

        Single flight (#981).  If no census is running, this caller runs
        one.  If one is running, it began before this call, so it is not
        fresh enough: the caller waits for it to finish and for the next
        one, which the first waiter to find no census running starts.  Every
        caller that arrived while a census ran shares that next census.  A
        search therefore sees every fragment added, changed or removed
        before it began, as each search did when it listed the forest
        itself; what goes away is the same listing repeated once per entry.

        Measured on the synthetic forest (434 directories, 145 fragments):
        one census costs about 5 ms of one core.  Sixteen copy workers each
        listing it for themselves spent 45 ms of CPU per entry, because
        ``os.scandir`` gives up the GIL around every ``readdir`` and sixteen
        threads contending for it turn each handoff into a futex round
        trip.  Before #981 the stage ownership lock serialized those
        listings, one entry at a time for the whole host.
        """

        with self._census_cond:
            wanted = self._census_started + 1
            while self._census_done < wanted:
                if not self._census_running:
                    self._census_running = True
                    self._census_started += 1
                    number = self._census_started
                    break
                self._census_cond.wait()
            else:
                return self._census  # type: ignore[return-value]
        try:
            census = self._forest_census()
        except BaseException:
            with self._census_cond:
                self._census_running = False
                self._census_cond.notify_all()
            raise
        with self._census_cond:
            self._census = census
            self._census_done = number
            self._census_running = False
            self._census_cond.notify_all()
        return census

    def _forest_census(self) -> tuple:
        """List the residency forest once, with each fragment's record.

        ``("absent",)`` when the residency root does not exist,
        ``("unreadable", detail)`` when it cannot be listed, otherwise
        ``("listed", slots)``: one ``(child, name, path, record)`` slot per
        fragment file, in the order the search visits them, and a
        ``(child, None, detail, None)`` slot for a child directory that
        could not be listed.  Each record comes from the reuse index (#761),
        stat first, so an unchanged file is not parsed again.  A record the
        index would not keep is held here as :data:`_LOOK_AGAIN`, never as
        a copy: an oversized table stays out of memory between decisions,
        as it did before.

        A directory is listed only when it moved (#1004 item 4, #992).  Each
        listing's names are remembered under the stamp
        :func:`kept_reads.filesystem_trust.directory_before` took before it, and a later census
        whose ``lstat`` of the directory still returns that stamp reuses the
        names without listing: no entry was created, removed or renamed
        since.  Every fragment is still stat-ed and looked up through the
        reuse index on every census, exactly as a fresh listing's are, so a
        fragment rewritten in place, made unreadable or restored to an older
        mtime is seen by the next decision (#761).  That turns the census a
        divergent name's re-decision takes under the stage lock into one
        ``lstat`` per directory and one ``stat`` per fragment -- the
        fingerprint of the judgment's census -- rather than a listing of the
        forest per name.  A listing with an unreadable fragment, and any
        listing on a filesystem whose stamps cannot be trusted, is never
        remembered, so both are listed fresh every time, as before.
        """

        root = self.residency_root
        with self._forest_roots.acquire(root) as capture:
            if capture.hit:
                children = capture.value
            else:
                try:
                    children = sorted(
                        e.name for e in os.scandir(root)
                        if e.is_dir() and not e.name.startswith(".")
                        and e.name not in _NON_FRAGMENT_DIRS)
                except FileNotFoundError:
                    self._forest_children.retain(())
                    return ("absent",)
                except OSError as exc:
                    return ("unreadable", f"{root}: {exc}")
                self._forest_children.retain(root / child for child in children)
                capture.commit(children)
        slots: list[tuple] = []
        prefix = os.fspath(root) + os.sep
        for child in children:
            with self._forest_children.acquire(prefix + child) as capture:
                if capture.hit:
                    paths = capture.value
                else:
                    cdir = root / child
                    try:
                        paths = tuple(
                            (name, cdir / name) for name in sorted(
                                e.name for e in os.scandir(cdir)
                                if e.is_file() and e.name.endswith(".json")
                                and not e.name.endswith(".retiring.json")
                                and not e.name.endswith(".tmp")))
                    except FileNotFoundError:
                        continue
                    except OSError as exc:
                        slots.append((child, None, f"{child}: {exc}", None))
                        continue
                listed: list[tuple] = []
                for name, path in paths:
                    observed = _observe(path)
                    with self._lookup_lock:
                        record = self._fragment_record(path, observed)
                        if isinstance(record, tuple) and record[1].keys:
                            retained = self._fragments.get(str(path))
                            if retained is None or retained[1] is not record:
                                record = _LOOK_AGAIN
                    listed.append((child, name, path, record))
                slots.extend(listed)
                if not capture.hit:
                    capture.commit(paths, complete=all(
                        slot[3] != "tainted" for slot in listed))
        return ("listed", tuple(slots))

    def _proof_candidate(self, fragment_path: Path, consumer: str,
                         norm: str, want: int, declared: object,
                         computed: str | None = None,
                         source_id: str | None = None,
                         ) -> tuple[str, dict[str, int]] | str | None:
        """One fragment file's verdict: proof, "divergent", "stale",
        "owned", "tainted" or None (names nothing here).

        Identity before content: the sidecar's ``file_id`` is compared
        against the live stat first.  A record dating a different inode
        names a superseded incarnation and says nothing about the bytes
        that are there now, whatever digest it carries (#755); a record
        dating the *same* inode that has since changed in place, or a
        digest mismatch on the current incarnation, is divergence.  Only
        a record that dates the current incarnation may prove these
        bytes.

        The fragment and the sidecar are read through the publisher's
        invocation-local reuse (#761), so an unchanged file is parsed once
        per version rather than once per destination.  What is decided is
        unchanged -- same order, same verdicts, same fail-closed answers --
        and the live destination stat below is still taken fresh on every
        call.  Each file's per-lookup stat is taken before the lookup lock
        and only the compare, read and parse run under it (#981); a record
        handed out of the index is immutable, so it is read outside.
        """

        observed = _observe(fragment_path)
        with self._lookup_lock:
            fragment = self._fragment_record(fragment_path, observed)
        return self._candidate_from_record(
            fragment, consumer, norm, want, declared,
            computed=computed, source_id=source_id)

    def _candidate_from_record(self, fragment: object, consumer: str,
                               norm: str, want: int, declared: object,
                               computed: str | None = None,
                               source_id: str | None = None,
                               ) -> tuple[str, dict[str, int]] | str | None:
        """:meth:`_proof_candidate`'s verdict for an already looked-up record.

        ``fragment`` is what :meth:`_fragment_record` returned for the file;
        the sidecar and the live destination stat are taken here, fresh.
        """

        if fragment is None:
            return None
        if fragment == "tainted":
            return "tainted"
        mover, staged_paths = fragment
        if norm not in staged_paths:
            return None
        if not mover:
            return "owned"
        observed = _observe(reader_lease.material_path(
            self.residency_root, consumer, mover))
        with self._lookup_lock:
            sidecar = self._material_record(consumer, mover, observed)
        if sidecar is None:
            # Published vouch without a date: unprovable either way.
            return "owned"
        if sidecar == "tainted":
            return "tainted"
        # Every mention this sidecar makes of the path, in sidecar order:
        # the digest search and the identity search below may legitimately
        # settle on different mentions, exactly as they could before.
        mentions = sidecar.get(norm, ())
        digest: str | None = None
        for size, recorded, _identity in mentions:
            try:
                if int(size) != want:
                    continue
            except (TypeError, ValueError):
                continue
            digest = recorded
            if not isinstance(digest, str) or not digest:
                continue
            break
        else:
            return "owned"
        file_id = reader_lease.stat_identity(norm)
        published = None
        for _size, _recorded, identity in mentions:
            if isinstance(identity, dict):
                published = identity
                break
        if file_id is None or not isinstance(published, dict):
            # Identity unreadable: the record can neither prove these bytes
            # nor clear them, and the fail-closed answer stays divergent.
            return "divergent"
        if int(published.get("ino", -1)) != int(file_id["ino"]):
            # A date for an incarnation this name no longer carries: the
            # name was replaced -- a publication mints a fresh inode, which
            # is exactly what the pre-fix unconditional rename did.  Not
            # proof, not divergence, not permission to overwrite: another
            # record may still date the incarnation that is there (#755).
            return "stale"
        if not reader_lease.file_id_matches(published, file_id):
            if not reader_lease.timestamp_only_mismatch(published, file_id):
                # Same inode, different size (or an unreadable identity):
                # an in-place write, which no legitimate publication
                # performs.  The dated bytes are provably not the bytes
                # that are there -- immediate divergence.
                return "divergent"
            # Inode and size still agree; only mtime/ctime moved.  #1096's
            # NFS delegation recall is one cause; a freed inode number the
            # allocator hands straight back to the very next create in the
            # same directory is another (#1078: a stale donor's dated inode
            # can coincide with the live file's purely by reuse, with no
            # writer ever touching that inode in place).  Neither a live
            # rewrite nor a coincidental reuse can be told apart by stat
            # alone, so genuine divergence is settled the same way #1096
            # settles a recall: by the file's own bytes, against this
            # record's own digest -- never the caller's declared or
            # computed one, which is judged after this returns.
            if self._content_proof(norm, want, digest) is None:
                return "divergent"
            # The bytes are provably what this record dated, but a
            # coincidental reuse proves nothing about an unbroken lineage
            # the way an exact stat match does -- it is exactly what a
            # record dating a superseded incarnation is: not proof, not
            # divergence, not permission to overwrite.  Skip it; another
            # record, or a live pin, may still settle the name.
            return "stale"
        if isinstance(declared, str) and declared:
            if digest != declared:
                return "divergent"
            record_digest: str = declared
        elif (source_id is not None
                and self._published_source_id(norm) == source_id):
            # Origin fast path: the published file still carries this
            # copy's source identity, so the source has not changed since
            # the vouched copy -- adopt without copying.
            record_digest = digest
        elif computed is not None:
            # The necessary private copy's digest against the stored
            # material proof: same content adopts, a changed source
            # refuses -- without rehashing the existing file.
            if computed != digest:
                return "divergent"
            record_digest = digest
        else:
            return "owned"
        return record_digest, file_id



def adopt(queue: pool.PoolQueue, *, old_key: str, new_key: str,
          consumer_action_key: str, tier_id: str, phase: str,
          range_start_bytes: int, range_end_bytes: int,
          residency_root: Path,
          chunk_index: int | None = None) -> dict[str, object]:
    """Hand one resident range from a finished mover to a live consumer's (#598).

    No byte is copied and no instant has bytes on the stage that no key holds.
    The order is the whole argument:

    1. **The donor's material still dates the files that are there.**  Under
       the stage ownership lock, every dated entry's ``file_id`` is compared
       against a live stat of its staged path -- the same comparison the
       strict reader will make.  A donor whose sidecar names a superseded
       incarnation (or bytes that are gone) publishes nothing: no successor,
       no material, no transfer (#755).  A donor with no sidecar, or one
       dating only some of the files its fragment names, is declined for the
       same reason: what it would hand on is a range the reader cannot prove.
    2. **The successor vouches for the same files under its own name.**  Its
       sidecar is written before its fragment, so an interruption never
       leaves a successor vouch without a date (#1087).  Two
       fragments then name one range, which every reader already tolerates:
       ``compose`` is per consumer, and the reconciliation unions them.
       Published under the ownership lock, never before it, so no egress
       scan can interleave between the vouch and the transfer.
    3. **The tokens change owner.**  ``ResourceLedger.transfer`` renames each
       token between two directories under ``held/``, so the tier's occupancy
       is the same number throughout and a crash part-way splits the
       attribution without changing the sum.
    4. **The receipt the pin and the gate read.** An adopted mover never runs,
       so it files no terminal record. The complete receipt binds its pair and
       reservation to this donor before any donor document is withdrawn.
    5. **Only then does the old name stop accounting for the bytes.** Dropping
       the old fragment before the transfer could release tokens for resident
       bytes. After the receipt, either withdrawal boundary can be resumed
       from the exact successor pair, receipt and reservation.

    Both mover transition locks are non-blocking. A live or unknown mover
    retains its authority. The donor pair is read again under ownership;
    an unknown or different successor pair is retained there as well.
    The existing stage lock still orders publication against egress.
    After process interruption following the full handover, both unchanged
    pairs and a successor charge covering the whole range let a retry file the
    receipt and finish withdrawal. Once withdrawal starts, its receipt must
    already exist. A retry verifies its exact binding and the successor pair
    under ownership before finishing cleanup. It preserves the receipt bytes
    and moves no token a second time.
    Declining costs a copy; proceeding against an egress mid-delete would cost
    the consumer its bytes.
    """

    outcome: dict[str, object] = {
        "event": "range-adoption-declined", "adopted": False,
        "consumer": consumer_action_key, "phase": phase,
        "chunk_index": chunk_index,
        "tier_id": tier_id, "mover": new_key, "adopted_from": old_key,
    }
    ledger = queue.tier(tier_id)
    with ExitStack() as transitions:
        acquired = transitions.enter_context(
            queue.mover_transition_lock(old_key, blocking=False))
        if not acquired:
            # Its egress holds the lock, which means its bytes are going.
            return {**outcome, "reason": "range_busy"}
        if not transitions.enter_context(
                queue.mover_transition_lock(new_key, blocking=False)):
            return {**outcome, "reason": "range_busy"}
        for mover in (old_key, new_key):
            live, unreadable = residency_plan.live_state(queue, mover)
            if unreadable:
                return {**outcome, "reason": "range_state_unreadable", "error": unreadable}
            if live is not None:
                return {**outcome, "reason": "range_in_use", "active_mover": mover,
                        "state": live}
        before = ledger.holdings(old_key)
        recovering = not before
        if recovering and not ledger.holdings(new_key):
            return {**outcome, "reason": "no_longer_resident"}
        receipt = queue.move_record(old_key)
        old_consumer = (str(receipt.get("consumer_action_key") or "")
                        if isinstance(receipt, Mapping) else "")
        if len(old_consumer) != 64:
            return {**outcome, "reason": "unattributed_range"}
        completed_receipt = None
        if recovering:
            try:
                completed_receipt = queue.move_record(new_key, fresh=True)
            except (OSError, ValueError, pb.PrismaBuildError) as exc:
                return {**outcome, "reason": "successor_receipt_uncertain", "error": repr(exc)}
        source_withdrawn = False
        source_path = residency_map.fragment_path(
            residency_root, old_consumer, old_key)
        try:
            with open(source_path) as stream:
                source = residency_map.validate_fragment(json.load(stream))
        except FileNotFoundError as exc:
            if not recovering:
                return {**outcome, "reason": "range_not_named", "error": repr(exc)}
            # The completed receipt binds this successor to this exact donor.
            # Without it, an unrelated pair and reservation cannot authenticate
            # a donor whose fragment has already gone.
            if (not isinstance(completed_receipt, Mapping)
                    or completed_receipt.get(pool.MOVE_ADOPTED_FROM_FIELD) != old_key
                    or completed_receipt.get("adopted_from_consumer") != old_consumer):
                return {**outcome, "reason": "no_longer_resident"}
            try:
                source = residency_map.reissue(residency_map.validate_fragment(json.loads(
                    pb.immutable_files.read_stable_file(residency_map.fragment_path(
                        residency_root, consumer_action_key, new_key),
                        where="completed adoption successor fragment"))),
                    consumer_action_key=old_consumer, mover_action_key=old_key)
            except (OSError, ValueError, pb.PrismaBuildError) as exc:
                return {**outcome, "reason": "successor_ownership_uncertain", "error": repr(exc)}
            if any(source.get(field) != receipt.get(field)
                   for field in ("tier_id", "stage_root", "manifest_sha256")):
                return {**outcome, "reason": "successor_already_named"}
            source_withdrawn = True
        except (OSError, ValueError) as exc:
            # A fragment that is absent or unreadable cannot say which files
            # this range is, and a range nobody can name is not one to take
            # over.  The same refusal ``evict`` makes, for the same reason.
            return {**outcome, "reason": "range_not_named", "error": repr(exc)}
        # Same bytes, same generation: the successor dates its vouching with
        # the publish it took over, never a new one (a new generation is for
        # new bytes).
        if source_withdrawn:
            old_material = _read_material_nofollow(
                residency_root, consumer_action_key, new_key)
            if isinstance(old_material, dict):
                old_material = {**old_material, "consumer_action_key": old_consumer,
                                "mover_action_key": old_key}
        else:
            old_material = reader_lease.read_material(
                residency_root, old_consumer, old_key)
        if isinstance(old_material, Exception):
            return {**outcome, "reason": "range_not_named",
                    "error": repr(old_material)}
        # The dated material is the only proof that survives into the
        # successor, so a donor that cannot supply it is declined rather than
        # adopted: the reader takes material as proof, and a successor vouched
        # by a fragment alone is a range it refuses.  The next candidate for
        # the descriptor is tried; if none qualifies, the copy runs.
        named = source.get("entries")
        if not isinstance(old_material, dict):
            return {**outcome, "reason": "donor_undated"}
        dated = old_material.get("entries")
        if (not isinstance(dated, dict) or not isinstance(named, dict)
                or not named or not set(named) <= set(dated)):
            # Entries the sidecar does not date are unproven, and the reader
            # needs every one of them.
            return {**outcome, "reason": "donor_material_partial"}
        # Covering the same keys is not describing the same object: a
        # sidecar can pass live ``file_id`` validation against a different
        # valid file.  ``reader_lease.covers_for_keys`` requires tier,
        # manifest and epoch of the material itself to match before it will
        # take a cover, and adoption qualifies its donor the same way.
        if any(str(old_material.get(field) or "") != str(source.get(field) or "")
               for field in ("tier_id", "manifest_sha256", "epoch")):
            return {**outcome, "reason": "donor_material_mismatch"}
        # Per entry, the same comparison the reader makes: the sidecar dates
        # the fragment's vouching, so same path, same length, same digest or
        # the two are about different bytes.
        for key, vouched in named.items():
            mention = dated.get(key)
            if (not isinstance(vouched, Mapping) or not isinstance(mention, Mapping)
                    or str(vouched.get("stage_path") or "")
                    != str(mention.get("stage_path") or "")
                    or vouched.get("bytes") != mention.get("bytes")
                    or str(vouched.get("sha256") or "")
                    != str(mention.get("sha256") or "")):
                return {**outcome, "reason": "donor_material_mismatch",
                        "entry": str(key)}
        with queue.stage_ownership_lock(str(source["stage_root"]),
                                        blocking=False) as owned:
            if not owned:
                # An egress is mid-scan on this stage root; its snapshot
                # predates this adoption, so proceeding could interleave the
                # transfer between its scan and its unlink.  Declining costs
                # a copy, the same price as a busy transition lock.  Nothing
                # has been published yet: the successor's fragment and
                # material land only under this lock.
                return {**outcome, "reason": "ownership_busy"}
            if source_withdrawn:
                if not pb.immutable_files.confirmed_metadata_absence(source_path):
                    return {**outcome, "reason": "donor_documents_changed"}
                current_source = source
            else:
                try:
                    current_source = residency_map.validate_fragment(json.loads(
                        pb.immutable_files.read_stable_file(
                            source_path, where="adoption donor fragment")))
                except (OSError, ValueError, pb.PrismaBuildError) as exc:
                    return {**outcome, "reason": "donor_documents_changed", "error": repr(exc)}
            current_material = _read_material_nofollow(residency_root, old_consumer, old_key)
            if source_withdrawn and current_material is None:
                if not pb.immutable_files.confirmed_metadata_absence(reader_lease.material_path(
                        residency_root, old_consumer, old_key)):
                    return {**outcome, "reason": "donor_documents_changed"}
                current_material = old_material
            if current_source != source or current_material != old_material:
                return {**outcome, "reason": "donor_documents_changed"}
            if isinstance(old_material, dict):
                # A partial prune can leave the fragment a strict subset of
                # the complete range its historical receipt declares (#853).
                # Whole-range adoption transfers one reservation and files
                # one complete receipt, so it may only take over the exact
                # range the donor's own complete receipt staged: the receipt
                # must be complete, its declared and staged entry counts must
                # equal the fragment's validated entries, their byte sum must
                # equal the receipt's actual staged bytes and the requested
                # leg's span, and the receipt's range must be that leg.  A
                # shortened fragment is a per-path cache for the publisher,
                # never a whole-range donor.
                #
                # This is metadata only -- three dict reads and a sum -- so
                # it runs before the per-file stat walk below: only a donor
                # that can still stand for the whole range is worth
                # qualifying file by file.
                declared = receipt.get("entries_declared")
                staged = receipt.get("entries_staged")
                receipt_start = receipt.get("range_start_bytes")
                receipt_end = receipt.get("range_end_bytes")
                staged_bytes = receipt.get("bytes_staged")
                if (receipt.get("complete") is not True
                        or receipt.get("refusal")
                        or any(isinstance(value, bool) or not isinstance(value, int)
                               for value in (declared, staged, receipt_start,
                                             receipt_end, staged_bytes))
                        # The counts are one shortening witness: a partial
                        # prune leaves fewer fragment entries than the
                        # complete receipt declared and staged.
                        or declared != staged
                        or declared != len(source["entries"])
                        or int(receipt_start) != int(range_start_bytes)
                        or int(receipt_end) != int(range_end_bytes)
                        # The receipt's own byte accounting must still equal
                        # its range...
                        or int(staged_bytes)
                        != int(receipt_end) - int(receipt_start)
                        # ...and the fragment must cover that range exactly.
                        # ``stage_move`` adds each landed entry's own
                        # ``written`` to both the entry's ``bytes`` and
                        # ``bytes_staged`` (an adopted incarnation returns
                        # ``want`` the same way), and a run that did not land
                        # every entry files an incomplete receipt -- so for a
                        # complete receipt the two are equal, and equal entry
                        # counts do not prove equal bytes.  Anything else is a
                        # certificate for a range the fragment cannot cover.
                        or sum(int(entry["bytes"])
                               for entry in dict(source["entries"]).values())
                        != int(staged_bytes)):
                    return {**outcome, "reason": "donor_range_shortened",
                            "declared": declared, "staged": staged,
                            "entries": len(source["entries"])}
                # The dated vouch must still describe the incarnation that
                # is there: the strict reader takes the successor's material
                # as proof, so adopting a donor that names a superseded
                # incarnation would publish a refusal into the consumer
                # (#755).  Verified under the ownership lock, so no egress
                # or publisher can be changing the same name at the same
                # time; a file that is gone is the same answer.
                stale: list[str] = []
                missing: list[str] = []
                for key, mention in dict(
                        old_material["entries"]).items():
                    mention = mention if isinstance(mention, Mapping) else {}
                    live = reader_lease.stat_identity(
                        str(mention.get("stage_path") or ""))
                    if live is None:
                        missing.append(str(key))
                    elif not reader_lease.file_id_matches(
                            mention.get("file_id"), live):
                        stale.append(str(key))
                if missing:
                    return {**outcome, "reason": "donor_file_missing",
                            "missing": missing}
                if stale:
                    return {**outcome, "reason": "donor_file_changed",
                            "stale": stale}
            successor = residency_map.reissue(
                source, consumer_action_key=consumer_action_key,
                mover_action_key=new_key)
            successor_path = residency_map.fragment_path(
                residency_root, consumer_action_key, new_key)
            try:
                try:
                    current_successor = residency_map.validate_fragment(json.loads(
                        pb.immutable_files.read_stable_file(
                            successor_path, where="adoption successor fragment")))
                except FileNotFoundError:
                    if not pb.immutable_files.confirmed_metadata_absence(successor_path):
                        return {**outcome, "reason": "successor_ownership_uncertain"}
                    current_successor = None
                successor_material = _read_material_nofollow(
                    residency_root, consumer_action_key, new_key)
                if successor_material is None:
                    if (current_successor is not None or not
                            pb.immutable_files.confirmed_metadata_absence(
                                reader_lease.material_path(residency_root, consumer_action_key, new_key))):
                        return {**outcome, "reason": "successor_ownership_uncertain"}
                elif not isinstance(successor_material, dict):
                    return {**outcome, "reason": "successor_ownership_uncertain"}
                elif successor_material != {**old_material,
                        "consumer_action_key": consumer_action_key, "mover_action_key": new_key}:
                    return {**outcome, "reason": "successor_already_named"}
                if current_successor is not None and current_successor != successor:
                    return {**outcome, "reason": "successor_already_named"}
            except (OSError, ValueError, pb.PrismaBuildError) as exc:
                return {**outcome, "reason": "successor_ownership_uncertain", "error": repr(exc)}
            if recovering:
                occupancy = storage_tiers.capacity_kind_of(tier_id)
                required = _tokens_for_egressed_bytes(sum(
                    int(entry["bytes"]) for entry in dict(source["entries"]).values()))
                held = ledger.holdings(new_key)
                if (current_successor != successor or successor_material is None
                        or int(held.get(occupancy, 0)) < required):
                    return {**outcome, "reason": "handover_recovery_unproven"}
            # The sidecar before the fragment it dates, as the movers and
            # the shared-range fan-out write them (#1087): an interruption
            # leaves a date no vouch cites, which every reader ignores,
            # never a successor vouch without a date, which the publication
            # gate refuses on for as long as it stands.
            if recovering:
                moved = 0
            elif isinstance(old_material, dict):
                material_entries = old_material.get("entries")
                assert isinstance(material_entries, dict)
                publish_vouch(
                    residency_root, successor,
                    generation=reader_lease.adopted_generation(old_material),
                    material_entries=material_entries)
            else:
                publish_vouch(residency_root, successor)
            expected = sum(before.values())
            if not recovering:
                moved = queue.tier(tier_id).handover(old_key, new_key)
            if not recovering and moved != expected:
                # The reservation is split across the two keys and the sum is
                # unchanged, so nothing is over-admitted; the old key is still
                # resident, so the next cycle asks again and finishes the move.
                return {**outcome, "reason": "partial_transfer",
                        "tokens_moved": moved, "tokens_expected": expected}
            entries = dict(source["entries"])
            start, end = int(range_start_bytes), int(range_end_bytes)
            adoption_receipt = {
                "consumer_action_key": consumer_action_key, "tier_id": tier_id,
                "stage_root": str(source["stage_root"]),
                "manifest_sha256": str(source["manifest_sha256"]),
                "range_start_bytes": start, "range_end_bytes": end,
                "range_bytes": end - start, "bytes_staged": end - start,
                "bytes_copied": 0, "entries_declared": len(entries),
                "entries_staged": len(entries), "complete": True,
                pool.MOVE_ADOPTED_FROM_FIELD: old_key,
                "adopted_from_consumer": old_consumer, "phase": phase,
            }
            if completed_receipt is not None:
                if (completed_receipt.get("schema") != pool.POOL_MOVE_SCHEMA_V1
                        or completed_receipt.get("action_key") != new_key
                        or completed_receipt.get("refusal")
                        or any(completed_receipt.get(field) != value
                               for field, value in adoption_receipt.items())):
                    return {**outcome, "reason": "successor_receipt_changed"}
            else:
                # Publish the existing complete receipt before removing the
                # donor. A fresh owner can finish either withdrawal boundary
                # from this binding, the successor pair and the real charge.
                queue.record_move(new_key, {**adoption_receipt,
                    "host": socket.gethostname(), "unix": time.time()})
            withdraw_vouch(queue, residency_root, old_consumer, old_key)
        return {**outcome, "event": "range-adopted", "adopted": True,
                "tokens_moved": moved, "entries": len(entries),
                "bytes_staged": end - start}


def own_action_key(declared: str | None) -> str:
    """This node's own action key, from the flag or from the launcher.

    A movement node files its receipt and holds its tier tokens under its own
    key, and that key is ``canonical_sha256`` of the action body --- so a
    ``--action-key`` sealed into the argv would be hashed into the very value
    it states, and no fixed point exists.  The launcher sets
    :data:`prismabuild.core.ACTION_KEY_ENV` for every action it starts, from
    the action in hand, which is the one place the answer is already known.
    The flag stays, because a direct run and every test needs to say which key
    it is acting as.
    """

    key = declared or os.environ.get(pb.ACTION_KEY_ENV) or ""
    if len(key) != 64 or any(character not in "0123456789abcdef" for character in key):
        raise SystemExit(
            f"pass --action-key, or run under a launcher that sets "
            f"{pb.ACTION_KEY_ENV}; got {key!r}")
    return key


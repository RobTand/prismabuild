#!/usr/bin/env python3
"""Take a staged range off the tier and give its tokens back (#583).

The egress node.  It is the only thing that may return a mover's tier tokens,
because it is the only thing that removes the bytes they stand for, and those
two have to be one operation: **held tier tokens equal bytes on the stage, at
every instant**.  Releasing at ``finish`` instead would bound concurrent copies
rather than resident bytes --- twenty-one movers of 34.4 GB, run one after
another, leave 722 GB on a 721 GB stage while the ledger reads its full supply
free at every step --- so a mover keeps its tokens from ``finish`` until an
egress deletes its files.

There is no retained-but-unpinned state.  Bytes the ledger cannot see are the
overfill the reservation exists to prevent, arriving by another road.  Evicting
is still deleting --- what #598 changed is *when* and *whether*.  A range a live
consumer's window names is **adopted** instead: the tokens move from the
finished mover's key to the successor's and nothing is copied
(``tier_loop.adopt``).  What no window names stays resident, held and counted,
until a window cannot be placed without the room --- the ``pressure`` argument
to :func:`sweep`.  Both are the same rule stated twice: an orphan is evicted
when the tier needs its tokens, never because a clock said so.

**A pending copy handoff defers the same way (#768).**  While a live ram
promotion's sealed claim names a source leg, the egress keeps that leg's file,
the stage mover's fragment, its material sidecar and its full occupancy charge,
and retries after the claim ends.  The handoff outranks the generic co-owner
and in-flight-destination shared skip: another owner's same-path fragment
cannot stand in for the promotion's own consumer/manifest cover, so this
owner's proof and charge stay while the claim lives, and the ordinary shared
settlement resumes on the retry.  It files no retiring mark while any handoff
in the document is deferred: a mark closes one material generation to new
acquires, and the promotion takes its proof-only cover through
``reader_lease.acquire`` after its claim row exists, so the mark would refuse
the very handoff it is protecting.  The mark waits until no handoff remains,
then the ordinary pinned retirement files it; deleting stays safe meanwhile
because every pass re-reads claims and pins under the same ownership lock.  The
promotion's ram fragment names another tier and path and can never prove the
SSD incarnation it read, so retiring the source early would leave a surviving
file nothing can prove and free capacity its bytes still occupy.

**Delete, then release, then drop the fragment.**  Each order is wrong in one
direction and this one is wrong in none that matters: a crash after the deletes
and before the release leaves tokens held for bytes that are gone, which costs
capacity until the orphan sweep or a rerun returns them, and a crash after the
release and before the deletes would leave bytes on a stage the ledger thinks
is empty --- which is the failure this whole node exists to prevent.  Releasing
last is the direction that fails safe.

It runs three ways.  As an **action row** it is published by the tier loop once a
consumer's accepted phase has passed the range, and its receipt is what makes
the eviction visible.  As a **sweep** the tier loop calls :func:`evict` directly
for a mover that no ready or claimed item still names --- a consumer that was
withdrawn leaves its movers holding the stage forever otherwise.  And as a
**reconciliation** (:func:`reconcile`, #608) the same sweep compares the stage
root against the fragments of the movers the fleet still wants, because both
paths above are driven from a *key*: the ledger's held keys or a receipt's, and
bytes no key holds are invisible to either for the life of the fleet.  That is
what a withdrawn mid-copy mover leaves --- the shards it verified and renamed
into place, and its ``.partial`` temporaries --- and its tokens have already
gone back by then, so nothing keyed can find it.

**A stage root belongs to one queue, and says so (#628).**  Every rule above
decides *what* to delete; none of them asked *whose* stage was being walked.
On 2026-09-18 a test announced ``/stage/prewarm`` to a queue under
``tmp_path`` on the storage box, that queue's fragments attributed nothing,
and :func:`reconcile` deleted 671 GB of staged shards in one walk while the
run they were staged for was reading them.  So the tier loop writes
:data:`STAGE_ROOT_MARKER` beside the staged bytes when it announces the tier
(:func:`register_stage_root`), and :func:`sweep`, :func:`reconcile` and
:func:`evict` delete nothing under a root whose marker is missing, unreadable
or names another queue --- they say why in the receipt and keep the tokens.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping
import errno
import json
import os
from pathlib import Path
import socket
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import reader_lease  # noqa: E402
from prismabuild import residency_map  # noqa: E402
from prismabuild import residency_plan  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402

import prewarm_loop  # noqa: E402
from stage_move import stage_relative, whole_file_paths  # noqa: E402

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


def _prune_empty(directory: Path, stop: Path) -> None:
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

#: Residency namespaces that are never consumer fragment directories.  Pins,
#: retiring marks (``leases/``) and publish-time sidecars (``material/``)
#: live beside the fragments in dedicated subdirectories; every fragment
#: enumerator skips them, proved by
#: ``test_legacy_enumerators_ignore_lease_namespaces`` -- a sidecar parsed
#: as a fragment would taint every egress fail-closed on a healthy tier.
RESERVED_RESIDENCY_SUBDIRS = frozenset(
    {reader_lease.LEASES_SUBDIR, reader_lease.MATERIAL_SUBDIR})


def _fragment_owners(root: Path, wanted: set[str], *,
                     except_consumer: str = "",
                     except_mover: str = "") -> tuple[dict[str, set[tuple[str, str]]], list[str]]:
    """Which of ``wanted`` paths are still vouched for, and by whom, in one walk.

    A single scan of every consumer directory -- never per entry -- intersecting
    validated ``stage_path`` strings against ``wanted`` before storing.  No
    metadata walk per foreign entry: the fragment validator already guarantees
    absolute, normalized paths, so string intersection is exact and only
    matches are stored.  The egress keeps its own resolve-based containment
    fence before any unlink.  A fragment that cannot be read or validated
    taints the scan: its paths are unknowable, so nothing may be treated as
    unowned on this pass.  Fail closed, the way an unreadable own fragment
    keeps its tokens.
    """

    owners: dict[str, set[tuple[str, str]]] = {}
    tainted: list[str] = []
    try:
        consumers = sorted(entry.name for entry in os.scandir(root)
                           if entry.is_dir()
                           and entry.name not in RESERVED_RESIDENCY_SUBDIRS)
    except OSError as exc:
        return owners, [f"{root}: {exc}"]
    for consumer in consumers:
        directory = root / consumer
        try:
            names = sorted(entry.name for entry in os.scandir(directory)
                           if entry.is_file() and entry.name.endswith(".json"))
        except OSError as exc:
            tainted.append(f"{consumer}: {exc}")
            continue
        for name in names:
            if consumer == except_consumer and name == f"{except_mover}.json":
                continue
            try:
                with open(directory / name) as stream:
                    fragment = residency_map.validate_fragment(json.load(stream))
            except (OSError, ValueError) as exc:
                tainted.append(f"{consumer}/{name}: {exc}")
                continue
            mover = str(fragment["mover_action_key"])
            for entry in dict(fragment["entries"]).values():
                if not isinstance(entry, Mapping):
                    continue
                path = entry.get("stage_path")
                # Validated absolute and normalized, so this comparison is
                # exact with no metadata touch.
                if isinstance(path, str) and path in wanted:
                    owners.setdefault(path, set()).add((consumer, mover))
    return owners, tainted


_manifest_layout_cache: dict[tuple[str, str], tuple[str, list[dict[str, object]]]] = {}


def _cached_manifest_layout(cas_root: str, digest: str) -> tuple[str, list[dict[str, object]]] | None:
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
        manifest = pb.load_data_manifest(blob)
    except (OSError, ValueError, pb.PrismaBuildError):
        return None
    if not isinstance(manifest, Mapping):
        return None
    entries = prewarm_loop.manifest_read_entries(manifest)
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
    try:
        from prismabuild.produced_output import PRODUCED_OUTPUT_REF_SCHEMA_V1
    except ImportError:
        return False
    if ref.get("schema") != PRODUCED_OUTPUT_REF_SCHEMA_V1:
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


def _claimed_paths(queue: pool.PoolQueue, tier_id: str,
                   cas_root: str | Path | None = None,
                   *, exclude: set[str] | frozenset[str] | None = None,
                   ) -> tuple[set[str], list[str]]:
    """Staged paths a claimed copy may be writing, by sealed range.

    A copy in flight has no fragment yet, so fragments alone cannot attribute
    it -- but its claim already exists, and the claim's sealed request names
    its manifest and its read-order range.  Resolving those through the same
    ``stage_relative`` computation both movers use attributes exactly the
    files the copy can rename into place.  Non-movement claims (no range
    flags) are skipped, never tainting: a consumer is not a copy.  Anything
    unreadable taints the pass, the same fail-closed rule as fragments.

    The CAS root comes from each sealed claim record's own ``cas_root`` where
    present (an explicit override wins for tests); the queue-sibling default
    applies only when no record names one.

    ``exclude`` names claim keys that never count as another publisher --
    the staged-path publication gate passes its own mover key, so a mover
    never defers to itself.
    """

    paths: set[str] = set()
    tainted: list[str] = []
    try:
        keys = sorted(path.name[:-len(".json")] if path.name.endswith(".json")
                      else path.name
                      for path in pool._scan(queue.dir(pool.CLAIMED)))
    except OSError as exc:
        return paths, [f"claimed: {exc}"]
    records: list[tuple[str, dict[str, object]]] = []
    for key in keys:
        try:
            item = pool._read_json(queue.item_path(pool.CLAIMED, key))
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
        demand = resources
        kinds = {str(kind).split("@", 1)[1] for kind in demand
                 if "@" in str(kind)}
        if tier_id not in kinds:
            continue    # not a movement node on this tier; a consumer is
                        # not a copy
        try:
            request = pool._read_json(
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
                        and entry.get("id") == pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID):
                    digest = entry.get("sha256")
        if not isinstance(digest, str) or not digest:
            tainted.append(f"{key[:12]}: sealed request names no data manifest")
            continue
        layout = _cached_manifest_layout(own_cas, digest)
        if layout is None:
            tainted.append(f"{key[:12]}: manifest {digest[:12]} unreadable")
            continue
        mount_prefix, entries = layout
        whole = whole_file_paths(entries)
        try:
            window = prewarm_loop.entries_between(entries, start, end)
        except (ValueError, TypeError) as exc:
            # A window that cannot be cut is an undeterminable range: taint,
            # never unowned.
            tainted.append(f"{key[:12]}: range not cuttable: {exc}")
            continue
        for entry in window:
            path, offset = str(entry["path"]), int(entry["offset"])
            try:
                relative = stage_relative(
                    path, offset, int(entry["bytes"]),
                    mount_prefix=mount_prefix,
                    whole_file=path in whole)
            except ValueError:
                continue
            # Compared against fragment ``stage_path`` values, which join the
            # stage root with this same relative name.
            paths.add(relative)
    return paths, tainted


def evict(queue: pool.PoolQueue, mover_action_key: str, *,
          consumer_action_key: str, stage_root: str,
          residency_root: str | Path | None = None,
          reason: str = "egress") -> dict[str, object]:
    """Delete one mover's staged files and settle its tier tokens.

    Tokens for deleted bytes return; tokens for bytes staying under a
    co-owner are decharged (#733).

    Idempotent in both halves: a file already gone is counted as gone rather
    than raised on, and ``ResourceLedger.release`` is documented safe to call
    twice.  A second egress of the same range is therefore a no-op receipt, not
    a failure --- which matters, because the tier loop may publish one while a
    sweep is doing the same work.

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

    with queue.mover_transition_lock(str(mover_action_key)):
        return _evict_locked(queue, mover_action_key,
                             consumer_action_key=consumer_action_key,
                             stage_root=stage_root,
                             residency_root=residency_root, reason=reason)


def _evict_locked(queue: pool.PoolQueue, mover_action_key: str, *,
                  consumer_action_key: str, stage_root: str,
                  residency_root: str | Path | None = None,
                  reason: str = "egress") -> dict[str, object]:
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
    last, as a leaf, around the settle alone.
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
                else queue.root / pool.RESIDENCY)
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
    with queue.stage_ownership_lock(str(stage)):
        return _evict_owned(queue, mover_action_key,
                            consumer_action_key=consumer_action_key,
                            stage=stage, tier_id=tier_id, root=root,
                            fragment_path=fragment_path, entries=entries,
                            errors=errors, reason=reason)


def _claimed_source_paths(queue: pool.PoolQueue, stage: Path,
                          cas_root: str | Path | None = None
                          ) -> tuple[set[str], list[str]]:
    """Stage paths a live RAM promotion may be reading, by sealed source leg.

    The promotion copies stage -> ram, so the stage egress must treat the
    promotion's *source* window the way it treats a stage mover's
    destination: a pending copy handoff that blocks eviction until its ram
    fragment lands.  Only ram-tier mover claims are read (a stage mover's
    destinations are `_claimed_paths`' job); the sealed `--source-stage-root`
    decides which stage this attribution joins, resolved before comparing.
    Anything unreadable taints the pass, the same fail-closed rule as
    fragments and destination claims.
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
                      for path in pool._scan(queue.dir(pool.CLAIMED)))
    except OSError as exc:
        return paths, [f"claimed: {exc}"]
    records: list[tuple[str, dict[str, object]]] = []
    for key in keys:
        try:
            item = pool._read_json(queue.item_path(pool.CLAIMED, key))
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
        try:
            request = pool._read_json(
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
                continue    # another stage's source leg, not this egress
        except OSError as exc:
            tainted.append(f"{key[:12]}: {exc}")
            continue
        digest = None
        inputs = request.get("inputs")
        if isinstance(inputs, list):
            for entry in inputs:
                if (isinstance(entry, Mapping)
                        and entry.get("id") == pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID):
                    digest = entry.get("sha256")
        if not isinstance(digest, str) or not digest:
            tainted.append(f"{key[:12]}: sealed request names no data manifest")
            continue
        layout = _cached_manifest_layout(own_cas, digest)
        if layout is None:
            tainted.append(f"{key[:12]}: manifest {digest[:12]} unreadable")
            continue
        mount_prefix, entries = layout
        whole = whole_file_paths(entries)
        try:
            window = prewarm_loop.entries_between(entries, start, end)
        except (ValueError, TypeError) as exc:
            tainted.append(f"{key[:12]}: range not cuttable: {exc}")
            continue
        for entry in window:
            path, offset = str(entry["path"]), int(entry["offset"])
            try:
                relative = stage_relative(
                    path, offset, int(entry["bytes"]),
                    mount_prefix=mount_prefix,
                    whole_file=path in whole)
            except ValueError:
                continue
            paths.add(os.path.normpath(os.path.join(stage_real, relative)))
    return paths, tainted


def _evict_owned(queue: pool.PoolQueue, mover_action_key: str, *,
                 consumer_action_key: str, stage: Path, tier_id: str | None,
                 root: Path, fragment_path: Path,
                 entries: dict[str, object], errors: list[str],
                 reason: str) -> dict[str, object]:
    """Unlink what is exclusively this mover's, under the ownership lock."""

    deleted = missing = shared = deferred = 0
    bytes_deleted = bytes_shared = bytes_gone = 0
    shared_with: list[str] = []
    live_pins: list[str] = []
    deferred_handoffs: list[str] = []
    auto_reclaimed: list[str] = []
    auto_retained: dict[str, str] = {}
    retiring_written = False
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
        wanted = {os.path.normpath(str(entry.get("stage_path", "")))
                  for entry in entries.values()
                  if isinstance(entry, Mapping)}
        claimed, claimed_taint = _claimed_paths(queue, tier_id)
        owners, fragment_taint = _fragment_owners(
            root, wanted,
            except_consumer=consumer_action_key,
            except_mover=mover_action_key)
        pins, pin_taint = reader_lease.live_for(
            queue, wanted, residency_root=root)
        source_paths, source_taint = _claimed_source_paths(queue, stage)
        tainted = fragment_taint + claimed_taint + pin_taint + source_taint
        if tainted:
            # Ownership is uncertain: behave like the unreadable-fragment
            # case -- nothing is unlinked, no tokens come back, the receipt
            # says so and the next sweep retries.
            errors.extend(f"ownership uncertain: {item}" for item in tainted)
            owners, claimed = {}, set()
            pins, source_paths = {}, set()
            blind = True
        else:
            blind = False
        own_generation: str | None = None
        if not blind:
            # Automatic reclamation first: refs whose attempts are
            # provably contained (terminal broker telemetry plus
            # broker-persisted proof) retire here, so ordinary
            # completion, crash/withdrawal cleanup and old-attempt drains
            # free their pins without an operator.  Whatever stays is a
            # genuinely live reader, and only that defers.
            if pins:
                reclaimed = reader_lease.auto_reclaim(
                    queue, residency_root=root)
                auto_reclaimed.extend(reclaimed["released"])
                for ref_id, reason in reclaimed["retained"].items():
                    auto_retained.setdefault(ref_id, reason)
                if reclaimed["released"]:
                    pins, pin_taint_again = reader_lease.live_for(
                        queue, wanted, residency_root=root)
                    if pin_taint_again:
                        errors.extend(
                            f"ownership uncertain: {item}"
                            for item in pin_taint_again)
                        owners, claimed = {}, set()
                        pins, source_paths = {}, set()
                        blind = True
            # The retiring mark this deferral may file binds the material
            # generation, never the path: without a sidecar the generation
            # is unknowable, so a pinned legacy range taints instead of
            # filing a mark that could wedge the path's future generations.
            # Skipped when the reclaim re-read already went blind: the
            # pass is fail-closed and needs no further evidence.
            if not blind:
                material = reader_lease.read_material(
                    root, consumer_action_key, mover_action_key)
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
    for key, entry in entries.items():
        if blind:
            continue
        # The sharing check compares validated strings (exact, no metadata);
        # the resolve below stays as the containment fence before any unlink.
        path = Path(str(entry["stage_path"]))
        try:
            resolved = str(path.resolve())
            if stage.resolve() not in Path(resolved).parents:
                # A fragment naming a path outside the stage is not a thing to
                # act on: the writer validated it, so this is corruption or
                # someone else's file.
                errors.append(f"{key}: outside {stage}")
                continue
        except OSError as exc:
            errors.append(f"{key}: {exc}")
            continue
        norm = os.path.normpath(str(path))
        pinned = pins.get(norm, [])
        if os.path.normpath(resolved) in source_paths:
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
            deferred += 1
            deferred_handoffs.append("promotion-handoff")
            live_pins.extend(pinned)
            continue
        co_owners = sorted(owners.get(norm, set()))
        if co_owners or _relative_under(stage, resolved) in claimed:
            # Another live fragment vouches for these bytes, or a claimed
            # copy is about to land them: keep the file, drop only this
            # mover's own vouching below.  The last owner to leave deletes.
            shared += 1
            shared_with.extend(
                f"{consumer[:12]}/{mover[:12]}" for consumer, mover in co_owners)
            if _relative_under(stage, resolved) in claimed:
                shared_with.append("in-flight-copy")
            bytes_shared += int(entry["bytes"])
            continue
        if pinned:
            # A live reader holds these bytes (open FD, prefetch, mmap, or a
            # promotion source pin): defer, mark retiring for this material
            # generation, keep the file, the fragment and the charge.  The
            # next sweep deletes after the last release.
            if not own_generation:
                errors.append(f"{key}: pinned but material unqualifiable")
                continue
            deferred += 1
            live_pins.extend(pinned)
            continue
        try:
            os.unlink(path)
        except FileNotFoundError:
            missing += 1
            bytes_gone += int(entry["bytes"])
            continue
        except OSError as exc:
            errors.append(f"{key}: {exc}")
            continue
        deleted += 1
        bytes_deleted += int(entry["bytes"])
        _prune_empty(path.parent, stage)

    released = decharged = 0
    if deferred and not deferred_handoffs and not errors and own_generation:
        # A handoff-deferred pass files nothing: closing this generation
        # would refuse the promotion's own cover acquire and strand the
        # handoff holding these bytes.  The next pass files the ordinary mark
        # once no handoff remains and only readers do; a mark already on disk
        # is preserved, never cleared by a deferral.
        reader_lease.write_retiring(
            reader_lease.leases_root(queue, root),
            consumer_action_key=consumer_action_key,
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
    if errors or deferred:
        pass
    elif tier_id is None:
        # No fragment at all: nothing is known shared, so every token comes
        # back exactly as before -- holding them would cost the tier its
        # capacity for good.
        released = queue.release_tier_reservations(mover_action_key)
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
        held = queue.tier_ledger(tier_id).holder_tokens(mover_action_key)
        destroy: dict[str, int] = {}
        free: dict[str, int] = {}
        for kind, count in held.items():
            if kind in pool.TIER_RATE_KINDS or kind != occupancy:
                free[kind] = count
                continue
            freed = min(count, _tokens_for_newly_free_bytes(
                bytes_deleted + bytes_gone))
            shared_part = min(count - freed, _tokens_for_egressed_bytes(
                bytes_shared))
            if shared_part:
                destroy[kind] = shared_part
            free[kind] = count - shared_part
        with queue.tier_mint_lock(tier_id):
            outcome = queue.release_tier_holder_for_egress(
                tier_id, mover_action_key, destroy=destroy, free=free)
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
        for other_tier in queue.tier_ids():
            if other_tier == tier_id:
                continue
            try:
                released += queue.tier_ledger(other_tier).release(
                    mover_action_key)
            except (OSError, pool.PoolContractError):
                continue
    if not errors and not deferred:
        fragment_path.unlink(missing_ok=True)
        reader_lease.clear_retiring(
            reader_lease.leases_root(queue, root),
            consumer_action_key=consumer_action_key,
            mover_action_key=mover_action_key)
        try:
            reader_lease.material_path(
                root, consumer_action_key,
                mover_action_key).unlink(missing_ok=True)
        except OSError:
            pass
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
        "complete": not errors and not deferred,
        "errors": errors,
        "host": socket.gethostname(),
        "unix": time.time(),
    }


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


def _relative_under(stage: Path, resolved: str) -> str | None:
    """This stage root's relative name for a resolved path, or ``None``."""

    try:
        return str(Path(resolved).relative_to(stage.resolve()))
    except (OSError, ValueError):
        return None


def live_claims(queue: pool.PoolQueue) -> tuple[set[str], dict[str, str]]:
    """Every mover key a ready or claimed item still wants, and those items.

    Two answers because they mean different things.  ``wanted`` is the movers:
    a live consumer's declared leads *and* every mover of its frozen plan, so a
    range staged three phases ahead of where it is reading is not an orphan.
    ``owners`` is the live items themselves, keyed by their own action key,
    which is how a consumer that holds tier tokens of its own is told apart
    from a mover.

    Read once by whoever needs it: the orphan sweep asks "which held keys are
    nobody's", and the adoption asks the same question the other way round --
    a range only a *finished* consumer still names is one a successor may take
    over (#598).  One walk of ``ready/`` and ``claimed/`` answers both.
    """

    wanted: set[str] = set()
    owners: dict[str, str] = {}
    for state in (pool.READY, pool.CLAIMED):
        for path in pool._scan(queue.dir(state)):
            item = pool._read_json(path)
            residency = item.get("residency") if isinstance(item, dict) else None
            if not isinstance(residency, dict):
                continue
            for lead in residency.get("leads") or []:
                wanted.add(str(lead))
            key = path.name[:-len(".json")] if path.name.endswith(".json") else path.name
            owners[key] = key
            plan = residency_plan.read(queue, key)
            if plan is not None:
                wanted.update(residency_plan.mover_keys(plan))
    return wanted, owners


def sweep(queue: pool.PoolQueue, *, stage_roots: dict[str, str],
          residency_root: str | Path | None = None,
          pressure: Mapping[str, int] | None = None) -> list[dict[str, object]]:
    """Evict every pinned mover no live item still names as a lead.

    A consumer withdrawn between its movers finishing and its own claim would
    otherwise hold the stage for the life of the fleet: nothing publishes its
    egress, because nothing is waiting for its bytes.  The test is deliberately
    the queue's own live state --- ready or claimed --- rather than a policy: a
    lead some item may still be admitted on is not an orphan, however old.

    Then, per tier, :func:`reconcile` takes back what no key accounts for at
    all.  It runs after the held-key evictions above rather than before, so it
    reads the directory the ledger now describes rather than one eviction
    behind it.

    **A live consumer protects its whole plan, not just its leads.**  The
    consumer depends on its first phase only, so a pinned mover three phases
    ahead of it is named by nothing in the queue: testing ``leads`` alone would
    make this sweep delete the window it exists to protect, on the cycle after
    it was staged.  The frozen plan is what says a mover is still wanted.

    **``pressure`` is what makes an orphan's eviction a decision rather than a
    reflex (#598).**  Given, it is the GiB each tier's own window cannot place,
    and orphans are evicted on that tier only until the tier has that much
    free.  Not given -- a direct call, an operator, a test of this primitive --
    every orphan goes, which is the behaviour this had before.  Deferring is
    safe because an orphan's tokens are still held the whole time: the ledger
    counts every resident byte, so nothing is admitted onto capacity that is
    not there.  What it buys is that a consumer's failure no longer deletes
    731 GB before its retry is submitted, which is the measured cost this
    exists to remove; Rob, 2026-09-18: *"We should not be rerunning anything in
    bulk if avoidable."*  Oldest receipt first, so a tier under repeated
    pressure takes the same range back twice rather than alternating between
    two -- a deterministic order, not a ranking of what is worth keeping.
    """

    wanted, owners = live_claims(queue)
    swept: list[dict[str, object]] = []
    for tier_id, stage_root in stage_roots.items():
        refusal = stage_root_refusal(queue, stage_root)
        if refusal is not None:
            # One receipt per tier per cycle, and neither the held-key
            # evictions nor the reconciliation run: the root is not this
            # queue's to delete under (#628).  ``evict`` and ``reconcile``
            # refuse on the same fact for the callers that reach them directly.
            swept.append(_refused_receipt(tier_id=tier_id, stage_root=stage_root,
                                          refusal=refusal))
            continue
        try:
            held = queue.tier_ledger(tier_id).held_keys()
        except (OSError, pool.PoolContractError):
            continue
        orphans: list[tuple[float, str, str]] = []
        for key in held:
            if key in wanted or key in owners:
                continue
            receipt = queue.move_record(key)
            consumer = (str(receipt.get("consumer_action_key")) if isinstance(receipt, dict)
                        else "")
            if not consumer:
                continue
            staged_unix = 0.0
            if isinstance(receipt, dict):
                try:
                    staged_unix = float(receipt.get("unix", 0.0) or 0.0)
                except (TypeError, ValueError):
                    staged_unix = 0.0
            orphans.append((staged_unix, key, consumer))
        orphans.sort()
        needed = None if pressure is None else int(pressure.get(tier_id, 0))
        kind = storage_tiers.capacity_kind_of(tier_id)
        for _, key, consumer in orphans:
            if needed is not None:
                if needed <= 0:
                    break      # nothing on this tier is waiting for the room
                try:
                    free = int(queue.tier_ledger(tier_id).available().get(kind, 0))
                except (OSError, pool.PoolContractError):
                    break
                if free >= needed:
                    break      # the window fits now; the rest stays resident
            swept.append(evict(queue, key, consumer_action_key=consumer,
                               stage_root=stage_root,
                               residency_root=residency_root, reason="orphan-sweep"))
        # Held keys first, then the rest of the stage: the evictions above turn
        # held bytes into absent ones, so the reconciliation below sees the same
        # directory the ledger now describes rather than one eviction behind it.
        # ``wanted`` is joined with what is *still* held, because a pinned mover
        # a live plan no longer names has just been evicted and a pinned one it
        # does name is attribution.
        try:
            still_held = set(queue.tier_ledger(tier_id).held_keys())
        except (OSError, pool.PoolContractError):
            still_held = set()
        reconciled = reconcile(
            queue, tier_id=tier_id, stage_root=stage_root,
            wanted=wanted | set(owners) | still_held,
            residency_root=residency_root)
        # Reported only when it has something to report.  A window in flight
        # skips the reconciliation every cycle, and a line per cycle saying so
        # would bury the eviction it exists to announce.
        if (reconciled["entries_deleted"] or reconciled["errors"]
                or reconciled["unowned_left"]):
            swept.append(reconciled)
    return swept


def _is_mover_partial(name: str) -> bool:
    return name.startswith(PARTIAL_PREFIX) and name.endswith(PARTIAL_SUFFIX)


def _marked_by_the_prewarm_stage(path: Path) -> bool | None:
    """Whether this file is a prewarm stage object, or ``None`` if unanswerable.

    The prewarm loop stages into the same pool, and its objects carry
    ``user.pbstage.source`` set on the handle before the rename, so a completed
    one is always marked.  ``None`` is the case that matters: a filesystem
    without user extended attributes still stages, and there "no mark" would
    read as "not the prewarm loop's" for every object it owns.  So the answer is
    unknown rather than false, and the caller refuses to delete on it.
    """

    try:
        os.getxattr(path, prewarm_loop.STAGE_SOURCE_XATTR)
    except OSError as exc:
        # The attribute is absent: an answer, and it is "no".
        if exc.errno in _XATTR_ABSENT:
            return False
        # Anything else -- the filesystem does not carry user attributes, the
        # call is not permitted -- is not an answer.
        return None
    return True


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
    """

    root = Path(residency_root if residency_root is not None
                else queue.root / pool.RESIDENCY)
    out: set[str] = set()
    try:
        consumers = sorted(entry.name for entry in os.scandir(root)
                           if entry.is_dir()
                           and entry.name not in RESERVED_RESIDENCY_SUBDIRS)
    except OSError:
        return out
    for consumer in consumers:
        for fragment in residency_map.read_fragments(root, consumer):
            if (wanted is not None
                    and str(fragment.get("mover_action_key") or "") not in wanted):
                continue
            for entry in dict(fragment["entries"]).values():
                out.add(os.path.normpath(str(entry["stage_path"])))
    return out


def reconcile(queue: pool.PoolQueue, *, tier_id: str, stage_root: str,
              wanted: set[str], residency_root: str | Path | None = None,
              ) -> dict[str, object]:
    """Evict what is on the stage that nothing on the fleet accounts for.

    The half of #608 the held-key sweep structurally cannot do: it walks the
    tier ledger's held keys, so bytes no key holds are invisible to it forever.
    A withdrawn mid-copy mover leaves exactly that -- the shards it had already
    verified and renamed into place, and its ``.partial`` temporaries -- and its
    tokens come back when its worker stops it, so the ledger reads its full
    supply free over 130 GB of occupied stage.

    Three rules decide, and each one is a fact rather than a policy:

    * **Nothing is deleted while a mover could be writing.**  A mover ready or
      claimed on this tier means a copy is in flight or about to be, and its
      destination is not in any fragment until it has been verified.  So the
      whole reconciliation is skipped for that tier, named in the receipt.  A
      per-file test cannot replace this: a ready mover can be claimed between
      the walk and the unlink.
    * **A mover's own temporary is always its own.**  ``.<name>.partial`` is
      ``stage_move``'s naming and nothing else writes it, so one that no live
      copy is producing is the residue of a killed or withdrawn one.
    * **Anything else must be shown to be unowned.**  The prewarm loop stages
      into this same pool and marks its objects with ``user.pbstage.source``, so
      an unmarked file that no wanted mover's fragment names is unowned.  Where
      the filesystem cannot answer the xattr question at all, "unmarked" means
      nothing, and the file is left alone with the reason in the receipt.

    Ownership is checked the way ``evict`` checks it: the resolved path must be
    under the resolved stage root.
    """

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
        "unowned_left": 0,
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
    # The same ownership guard as the egress, held across query and
    # delete: a publisher, a reader pin, or a promotion handoff that
    # lands after this snapshot waits out there (start gate) or is seen
    # here; nothing unlinks between the check and the act.
    with queue.stage_ownership_lock(str(stage)):
        in_flight = movers_in_flight(queue, tier_id=tier_id)
        if in_flight:
            receipt["skipped"] = "movers_in_flight"
            receipt["movers_in_flight"] = sorted(in_flight)
            return receipt
        try:
            stage_resolved = stage.resolve(strict=True)
        except OSError as exc:
            receipt["skipped"] = f"stage_root_unreadable: {exc}"
            receipt["complete"] = False
            return receipt
        attributed = attributed_stage_paths(queue, wanted=wanted,
                                            residency_root=residency_root)
        pin_owners, pin_taint = reader_lease.live_for(
            queue, None, residency_root=residency_root)
        if pin_taint:
            receipt["complete"] = False
            receipt["errors"] = [
                f"ownership uncertain: {item}" for item in pin_taint]
            return receipt
        # A live pin is attribution: unattributed bytes nobody accounts for
        # go, pinned bytes never do.
        attributed |= set(pin_owners)
        deleted = bytes_deleted = partials = unowned_left = 0
        errors: list[str] = []
        for base, _directories, names in os.walk(stage):
            for name in sorted(names):
                path = Path(base) / name
                if name == STAGE_ROOT_MARKER and Path(base) == stage:
                    # The root's own ownership marker: unmarked by the prewarm
                    # stage and named by no fragment, so without this line the
                    # sweep would delete the fact that lets it sweep.
                    continue
                if (name == storage_tiers.RAM_EPOCH_MARKER and Path(base) == stage):
                    # The ram root's epoch marker, same rule one tier over
                    # (#640): unmarked, unnamed, and the one file that dates
                    # every ram range this queue admits.  Deleting it would mint
                    # a new epoch and drop every resident range the tmpfs still
                    # holds.
                    continue
                try:
                    if path.is_symlink() or not path.is_file():
                        continue
                    if stage_resolved not in path.resolve().parents:
                        continue
                except OSError as exc:
                    errors.append(f"{path.name}: {exc}")
                    continue
                if os.path.normpath(str(path)) in attributed:
                    continue
                partial = _is_mover_partial(name)
                if not partial:
                    if prewarm_loop._STAGE_TEMPORARY.search(name):
                        # The prewarm loop reaps its own, once per process, and a
                        # live one belongs to a copy in flight.
                        continue
                    marked = _marked_by_the_prewarm_stage(path)
                    if marked is None or marked:
                        unowned_left += 1
                        continue
                try:
                    size = path.stat().st_size
                    os.unlink(path)
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    errors.append(f"{path.name}: {exc}")
                    continue
                deleted += 1
                bytes_deleted += int(size)
                partials += 1 if partial else 0
                _prune_empty(path.parent, stage)
    receipt["entries_deleted"] = deleted
    receipt["bytes_deleted"] = bytes_deleted
    receipt["partials_deleted"] = partials
    receipt["unowned_left"] = unowned_left
    receipt["errors"] = errors
    receipt["complete"] = not errors
    return receipt


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

    layout = _cached_manifest_layout(cas_root, manifest_sha256)
    if layout is None:
        return None
    mount_prefix, entries = layout
    whole = whole_file_paths(entries)
    try:
        window = prewarm_loop.entries_between(entries, start, end)
    except (ValueError, TypeError):
        return None
    names: set[str] = set()
    for entry in window:
        path, offset = str(entry["path"]), int(entry["offset"])
        try:
            names.add(stage_relative(
                path, offset, int(entry["bytes"]),
                mount_prefix=mount_prefix, whole_file=path in whole))
        except ValueError:
            continue
    return names, list(window), mount_prefix


def _bounded_int(value: object) -> int | None:
    """A nonnegative integer that is not a bool, or ``None``."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _move_receipt(queue: pool.PoolQueue, action_key: str,
                  ) -> tuple[dict[str, object] | None, str]:
    """One filed move receipt, or why it cannot be evidence.

    The receipt is the positive terminal evidence this repair turns on.  An
    absent one is not "finished": it is no answer, and no answer refuses.
    """

    try:
        record = pool._read_json(queue.move_path(action_key))
    except (OSError, pool.PoolContractError) as exc:
        return None, f"{action_key[:12]}: receipt unreadable: {exc}"
    if not isinstance(record, Mapping):
        return None, f"{action_key[:12]}: no filed move receipt"
    if str(record.get("action_key") or "") != action_key:
        return None, f"{action_key[:12]}: receipt is filed under another " \
                     f"action's key"
    if record.get("schema") != pool.POOL_MOVE_SCHEMA_V1:
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


def _validated_sealed_request(
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
        raw = pb._read_regular_file_nofollow(
            path, where="orphan recovery action request")
        action = pb.validate_action(pb._decode_strict_json(
            raw, where="orphan recovery action request"))
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
        apply: bool = False) -> dict[str, object]:
    """Retire the cache copies of one retired head that nothing can prove.

    Routine ``reconcile`` structurally cannot reach these.  It decides
    ownership from the ``user.pbstage.source`` mark, and ``stage_move`` sets
    that same mark on every file it publishes, so a stage copy always reads as
    prewarm-owned and lands in ``unowned_left`` for the life of the fleet.  A
    head whose fragment and material are gone therefore leaves bytes no
    document proves and no sweep may touch, and every later head pays the
    publisher grace once per entry for them.

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
    egress, why = _move_receipt(queue, egress_action_key)
    if egress is None:
        return refuse(f"egress evidence refused: {why}")
    if egress.get("reason") != "egress":
        return refuse(
            f"{egress_action_key[:12]} is not an egress receipt")

    consumer = str(head.get("consumer_action_key") or "")
    tier_id = str(head.get("tier_id") or "")
    manifest_sha256 = str(head.get("manifest_sha256") or "")
    start = _bounded_int(head.get("range_start_bytes"))
    end = _bounded_int(head.get("range_end_bytes"))
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
        sealed, why = _validated_sealed_request(own_cas_early, key)
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
    with queue.stage_ownership_lock(str(stage)):
        for key, label in ((head_action_key, "head"), (consumer, "consumer")):
            for state in (pool.READY, pool.CLAIMED):
                try:
                    if queue.item_path(state, key).exists():
                        return refuse(
                            f"{label} {key[:12]} is still {state}; recovery "
                            f"acts only on a finished request")
                except OSError as exc:
                    return refuse(f"{label} {key[:12]} unreadable: {exc}")
        in_flight = movers_in_flight(queue, tier_id=tier_id)
        if in_flight:
            receipt["movers_in_flight"] = sorted(in_flight)
            return refuse("movers_in_flight")
        try:
            stage_resolved = stage.resolve(strict=True)
        except OSError as exc:
            return refuse(f"stage_root_unreadable: {exc}")

        layout = _cached_manifest_layout(own_cas, manifest_sha256)
        if layout is None:
            return refuse(
                f"manifest {manifest_sha256[:12]} unreadable; scope "
                f"undeterminable")
        mount_prefix, entries = layout
        whole = whole_file_paths(entries)
        try:
            window = prewarm_loop.entries_between(entries, start, end)
        except (ValueError, TypeError) as exc:
            return refuse(f"range not cuttable: {exc}")
        if not window:
            return refuse("the recorded range covers no manifest entry")
        # ``entries_between`` includes a straddling entry whole, so the
        # window is a cover of the range and not a partition of it: it may
        # exceed the span, and must never fall short of it.  The manifest is
        # the authority for what the range contains -- a manifest-wide
        # ``entry_count`` is a different number and is not interchangeable
        # with the entries one range covers.
        covered = sum(int(one.get("bytes", 0)) for one in window)
        if covered < end - start:
            return refuse(
                f"the window covers {covered} bytes, short of the "
                f"{end - start} the head recorded; scope is not the "
                f"recorded range")
        staged = _bounded_int(head.get("entries_staged"))
        if staged is not None and staged != len(window):
            return refuse(
                f"the window holds {len(window)} entries, not the {staged} "
                f"the head recorded staging; scope is not that window")
        names: dict[str, dict[str, object]] = {}
        for entry in window:
            source, offset = str(entry["path"]), int(entry["offset"])
            try:
                relative = stage_relative(
                    source, offset, int(entry["bytes"]),
                    mount_prefix=mount_prefix, whole_file=source in whole)
            except ValueError as exc:
                # A name that cannot be derived shrinks the scope silently if
                # it is skipped, and a partial scope is a different question
                # from the one the receipt asked.
                return refuse(f"{source}: staged name underivable: {exc}")
            names[relative] = entry
        receipt["scope_entries"] = len(names)

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
                return refuse("a window entry names no source path")
            offset = _bounded_int(entry.get("offset"))
            span = _bounded_int(entry.get("bytes"))
            if offset is None or span is None:
                return refuse(
                    f"{source}: window entry names an unusable extent")
            checked += 1
            try:
                original = Path(source)
                if not original.exists():
                    return refuse(
                        f"original missing for {source}; the staged copy may "
                        f"be the last surviving input")
                if stage_resolved in original.resolve().parents:
                    return refuse(
                        f"original {source} resolves inside the stage root; "
                        f"it is not a separate input")
                if not original.is_file():
                    return refuse(
                        f"original {source} is not a regular source file")
                size = original.stat().st_size
            except OSError as exc:
                return refuse(f"original {source} unreadable: {exc}")
            if size < offset + span:
                return refuse(
                    f"original {source} holds {size} bytes, short of the "
                    f"{offset + span} its window covers; the staged copy is "
                    f"not proven recapturable")
            present += 1
        receipt["originals_checked"] = checked
        receipt["originals_present"] = present

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
                             else queue.root / pool.RESIDENCY)
        scope_paths = {os.path.normpath(str(stage / relative))
                       for relative in names}
        fragment_owners, fragment_taint = _fragment_owners(
            fragment_root, scope_paths)
        if fragment_taint:
            return refuse(f"fragment census unreadable: "
                          f"{'; '.join(fragment_taint[:3])}")
        attributed = set(fragment_owners)
        pin_owners, pin_taint = reader_lease.live_for(
            queue, None, residency_root=residency_root)
        if pin_taint:
            return refuse(f"pin census unreadable: {'; '.join(pin_taint[:3])}")
        attributed |= set(pin_owners)
        claimed, claim_taint = _claimed_paths(queue, tier_id, own_cas)
        if claim_taint:
            return refuse(
                f"claim census unreadable: {'; '.join(claim_taint[:3])}")
        attributed |= {os.path.normpath(str(stage / one)) for one in claimed}
        handoffs, handoff_taint = _claimed_source_paths(queue, stage, own_cas)
        if handoff_taint:
            return refuse(
                f"promotion handoff census unreadable: "
                f"{'; '.join(handoff_taint[:3])}")
        attributed |= {os.path.normpath(str(one)) for one in handoffs}

        retained: dict[str, int] = {}

        def retain(why: str) -> None:
            retained[why] = retained.get(why, 0) + 1

        eligible: list[tuple[Path, int]] = []
        already_gone = 0
        for relative in sorted(names):
            path = stage / relative
            try:
                if not path.exists():
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
            _prune_empty(path.parent, stage)
        receipt["entries_retired"] = retired
        receipt["bytes_retired"] = bytes_retired
        receipt["entries_already_gone"] = already_gone
        receipt["entries_refused"] = len(errors)
        receipt["errors"] = errors
        receipt["complete"] = not errors
    return receipt


def movers_in_flight(queue: pool.PoolQueue, *, tier_id: str) -> set[str]:
    """Mover keys ready or claimed on this tier, i.e. copies that may be writing.

    A mover row is the one whose residency block names a *range*; a consumer's
    names leads.  Ready as well as claimed, because a ready mover can be
    claimed between a directory walk and an unlink.
    """

    out: set[str] = set()
    for state in (pool.READY, pool.CLAIMED):
        for path in pool._scan(queue.dir(state)):
            item = pool._read_json(path)
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="delete one mover's staged files and return its tier tokens")
    parser.add_argument("--pool-root", required=True,
                        help="the pull queue root this egress files its receipt under")
    parser.add_argument("--action-key", default=None,
                        help="this egress node's own action key; defaults to "
                             f"{pb.ACTION_KEY_ENV}, which the launcher sets")
    parser.add_argument("--mover-action-key", default=None,
                        help="the movement node whose staged bytes are being taken back")
    parser.add_argument("--consumer-action-key", default=None,
                        help="the action those bytes were staged for")
    recovery = parser.add_argument_group(
        "bounded orphan recovery",
        "retire the cache copies of one RETIRED head that nothing can prove. "
        "Reports and changes nothing unless --apply is given.")
    recovery.add_argument("--recover-orphaned-range", action="store_true",
                          help="run the recovery instead of an eviction")
    recovery.add_argument("--head-action-key", default=None,
                          help="the retired head, by its filed move receipt")
    recovery.add_argument("--egress-action-key", default=None,
                          help="the egress receipt that retired it")
    recovery.add_argument("--cas-root", default=None,
                          help="default <pool-root>/../cas")
    recovery.add_argument("--apply", action="store_true",
                          help="actually unlink; omit to report only")
    parser.add_argument("--stage-root", required=True,
                        help="the staging dataset's mountpoint; nothing outside it "
                             "is ever deleted")
    parser.add_argument("--residency-root", default=None,
                        help="where residency-map fragments are filed "
                             "(default <pool-root>/residency)")
    parser.add_argument("--receipt", default=None,
                        help="also write the receipt here (it is always filed in "
                             "the queue's movers directory)")
    args = parser.parse_args(argv)
    args.action_key = own_action_key(args.action_key)

    queue = pool.PoolQueue(Path(args.pool_root))
    if args.recover_orphaned_range:
        required = {"--head-action-key": args.head_action_key,
                    "--egress-action-key": args.egress_action_key}
        absent = sorted(name for name, value in required.items()
                        if value is None)
        if absent:
            parser.error(
                f"--recover-orphaned-range needs {', '.join(absent)}")
        # Consumer, tier, manifest and range are read off the filed receipts,
        # not taken here: there is no caller-supplied duplicate to disagree
        # with history.
        receipt = recover_orphaned_range(
            queue, stage_root=args.stage_root,
            head_action_key=args.head_action_key,
            egress_action_key=args.egress_action_key,
            cas_root=args.cas_root, residency_root=args.residency_root,
            apply=args.apply)
        queue.record_move(args.action_key, receipt)
        if args.receipt:
            with open(args.receipt, "w") as stream:
                json.dump(receipt, stream, indent=1, sort_keys=True)
                stream.write("\n")
        print(json.dumps(receipt, indent=1, sort_keys=True, default=str))
        return 0 if receipt["complete"] else 1
    for name, value in (("--mover-action-key", args.mover_action_key),
                        ("--consumer-action-key", args.consumer_action_key)):
        if not value:
            parser.error(f"{name} is required for an eviction")
    receipt = evict(queue, args.mover_action_key,
                    consumer_action_key=args.consumer_action_key,
                    stage_root=args.stage_root,
                    residency_root=args.residency_root)
    queue.record_move(args.action_key, receipt)
    if args.receipt:
        with open(args.receipt, "w") as stream:
            json.dump(receipt, stream, indent=1, sort_keys=True)
            stream.write("\n")
    print(json.dumps(receipt, indent=1, sort_keys=True, default=str))
    return 0 if receipt["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

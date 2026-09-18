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
from prismabuild import residency_map  # noqa: E402
from prismabuild import residency_plan  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402

import prewarm_loop  # noqa: E402

#: The errnos that mean "this file has no such attribute", as opposed to "this
#: question cannot be answered here".  Linux reports ``ENODATA``; the name
#: ``ENOATTR`` is an alias for it where it exists at all.
_XATTR_ABSENT = frozenset(
    value for value in (getattr(errno, "ENODATA", None),
                        getattr(errno, "ENOATTR", None))
    if value is not None)

#: How ``stage_move`` names the file it is copying into before it verifies the
#: digest and ``os.replace``s it into place: ``.<final name>.partial``, beside
#: the destination.  One definition, read here, because a temporary left by a
#: killed or withdrawn copy is nobody's and nothing else ever removes it.
PARTIAL_PREFIX = "."
PARTIAL_SUFFIX = ".partial"

#: The event a reconciled eviction publishes, so an operator can tell bytes a
#: mover's own fragment named from bytes nothing named at all.
UNATTRIBUTED_EVENT = "stage-unattributed-evicted"

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


def evict(queue: pool.PoolQueue, mover_action_key: str, *,
          consumer_action_key: str, stage_root: str,
          residency_root: str | Path | None = None,
          reason: str = "egress") -> dict[str, object]:
    """Delete one mover's staged files and return its tier tokens.

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
    """:func:`evict`'s body, with the mover's transition lock already held."""

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
    errors: list[str] = []
    try:
        with open(fragment_path) as stream:
            entries = dict(residency_map.validate_fragment(json.load(stream))["entries"])
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
    deleted = missing = 0
    bytes_deleted = 0
    for key, entry in entries.items():
        path = Path(str(entry["stage_path"]))
        try:
            if stage.resolve() not in path.resolve().parents:
                # A fragment naming a path outside the stage is not a thing to
                # act on: the writer validated it, so this is corruption or
                # someone else's file.
                errors.append(f"{key}: outside {stage}")
                continue
        except OSError as exc:
            errors.append(f"{key}: {exc}")
            continue
        try:
            os.unlink(path)
        except FileNotFoundError:
            missing += 1
            continue
        except OSError as exc:
            errors.append(f"{key}: {exc}")
            continue
        deleted += 1
        bytes_deleted += int(entry["bytes"])
        _prune_empty(path.parent, stage)

    released = 0 if errors else queue.release_tier_reservations(mover_action_key)
    if not errors:
        fragment_path.unlink(missing_ok=True)
    return {
        "schema": pool.POOL_EGRESS_SCHEMA_V1,
        "action_key": mover_action_key,
        "consumer_action_key": consumer_action_key,
        "stage_root": str(stage),
        "reason": reason,
        "entries_deleted": deleted,
        "entries_already_gone": missing,
        "bytes_deleted": bytes_deleted,
        "tokens_released": released,
        # Errors mean the stage still holds bytes, so the tokens stay held:
        # releasing them would let the ledger admit a mover onto capacity that
        # is not there.  The receipt says so and the next sweep retries.
        "complete": not errors,
        "errors": errors,
        "host": socket.gethostname(),
        "unix": time.time(),
    }


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


def attributed_stage_paths(queue: pool.PoolQueue, *, wanted: set[str],
                           residency_root: str | Path | None = None) -> set[str]:
    """Every stage path a mover the fleet still wants has vouched for.

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
        consumers = sorted(entry.name for entry in os.scandir(root) if entry.is_dir())
    except OSError:
        return out
    for consumer in consumers:
        for fragment in residency_map.read_fragments(root, consumer):
            if str(fragment.get("mover_action_key") or "") not in wanted:
                continue
            for entry in dict(fragment["entries"]).values():
                out.add(str(entry["stage_path"]))
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
            if str(path) in attributed:
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
    parser.add_argument("--mover-action-key", required=True,
                        help="the movement node whose staged bytes are being taken back")
    parser.add_argument("--consumer-action-key", required=True,
                        help="the action those bytes were staged for")
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

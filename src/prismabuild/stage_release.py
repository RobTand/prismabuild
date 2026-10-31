"""Take a staged range off the tier and give its tokens back (#583).

The egress node.  It is the only thing that may return a mover's tier tokens,
because it is the only thing that removes the bytes they stand for, and those
two have to be one operation: **held tier tokens equal bytes on the stage, at
every instant**.  The #853 partial prune is the one deliberate slack, and it
errs the safe way: it unlinks a positively stale destination and releases
nothing, so through that window the held tokens *cover* the stage's bytes
rather than equal them, and the whole charge settles when the owner retires.
Releasing at ``finish`` instead would bound concurrent copies
rather than resident bytes --- twenty-one movers of 34.4 GB, run one after
another, leave 722 GB on a 721 GB stage while the ledger reads its full supply
free at every step --- so a mover keeps its tokens from ``finish`` until an
egress deletes its files.

There is no retained-but-unpinned state.  Bytes the ledger cannot see are the
overfill the reservation exists to prevent, arriving by another road.  Evicting
is still deleting --- what #598 changed is *when* and *whether*.  A range a live
consumer's window names is **adopted** instead: the tokens move from the
finished mover's key to the successor's and nothing is copied
(``staged_path.adopt``).  What no window names stays resident, held and counted,
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

**The mover's own live claim is not a co-owner (#793).**  A movement node
files its final ``record_move`` receipt before the worker retires its
``claimed/`` row, so an egress can run in that gap and find the mover being
evicted still claimed.  The claim census attributes that one key's derived
paths separately, and they **defer** --- the file, this mover's fragment,
material and full charge stay, and the next sweep retries once the worker's
terminal transition has retired the claim.  Sharing them instead would
decharge this mover's own duplicate and drop its only vouch while the bytes
stayed behind nothing.  The claim is not settled on its receipt: a move
receipt carries no immutable attempt identity, so a complete-looking one
cannot be told from a previous attempt's while the same key is claimed again,
and a wall-clock stamp is not a substitute (2026-09-21 root QA).

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
what a killed or withdrawn mid-copy mover leaves --- its ``.partial``
temporaries, and the copies it renamed into place after its last fragment
(#1088) --- and its tokens have already gone back by then, so nothing keyed
can find it.

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

from prismabuild import batch_ledger as bl
from prismabuild import output_contract as oc

import argparse
from collections.abc import Iterable, Mapping
from contextlib import nullcontext, contextmanager
import contextvars
import errno
import json
import os
from pathlib import Path
import socket
import stat as statmod
import sys
import threading
import time


from prismabuild import kept_reads, manifest_layout
from prismabuild.stage_marks import (  # noqa: E402
    STAGE_MOVER_XATTR, STAGE_SOURCE_XATTR,
)
from prismabuild import core as pb  # noqa: E402
from . import data_manifest as manifest_codec
from prismabuild import movement_actions  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import prelaunch_group  # noqa: E402
from prismabuild import progress as pb_progress  # noqa: E402
from prismabuild import produced_output  # noqa: E402
from prismabuild.record_store import (  # noqa: E402
    Absent, Moving, Present, QueueRow, RecordStore, Unknown,
)
from prismabuild import reader_lease  # noqa: E402
from prismabuild import residency_map  # noqa: E402
from prismabuild import residency_plan  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
from prismabuild import window_credit  # noqa: E402

from prismabuild import prewarm_loop  # noqa: E402

from . import tier_holdings
from prismabuild import staged_path

#: The event a dead-owner eviction publishes, so an operator can tell bytes
#: whose only owner was terminal-dead (failed consumer, withdrawn mover) from
#: bytes routine reconciliation found unowned and from a named historical
#: range an operator asked about by identity.
DEAD_OWNER_EVENT = "stage-dead-owner-evicted"

#: The event an orphan sweep reports for a held mover that has no receipt and
#: no single direct fragment naming a consumer that has ended (#892), when the
#: tier's window still lacks room after the pass.
RECEIPTLESS_HOLDER_EVENT = "stage-receiptless-holder-retained"

#: The event an orphan sweep publishes when it retires a produced-output batch
#: whose producer attempt has ended (#929): the ``retire_batch`` its producer
#: never ran, run for it.
PRODUCED_ORPHAN_EVENT = "stage-produced-orphan-retired"

#: The operator report of a held key the sweep can prove neither live nor an
#: orphan (#929).  Published once per change of the reason, whatever the
#: tier's pressure, so a holder nothing can classify is seen once rather than
#: never or on every cycle.
HOLDER_UNRESOLVED_EVENT = "stage-holder-unresolved"

#: The reason each unresolved holder was last reported with, per queue and
#: tier.  Process-local on purpose: the tier loop is long-lived, and a restart
#: reporting every unresolved holder once more is the right amount of noise.
_UNRESOLVED_REPORTS: dict[tuple[str, str], dict[str, str]] = {}

#: The report of the dead owners on a tier that hold material and no token
#: (#1061): how many, and the bytes their fragments name.  The orphan sweep
#: takes them back under pressure, and nothing else counts them, so this is
#: where an operator sees them.  Published once per change of the set the
#: pass leaves, including a drop to none, and never for a set that was never
#: there.
UNCHARGED_OWNERS_EVENT = "stage-uncharged-dead-owners"

#: The movers each tier's latest uncharged-owner report named, per queue and
#: tier.  Process-local, like :data:`_UNRESOLVED_REPORTS`, and cleared with
#: it by :func:`reset_holder_reports`.
_UNCHARGED_REPORTS: dict[tuple[str, str], tuple[str, ...]] = {}

#: The record an orphan sweep leaves when it cannot read a tier's ledger
#: (#1007).  Which held movers still count as attribution is unknown then,
#: and unknown ownership never deletes: the pass that needed the read is
#: skipped for the cycle, and this record says which pass and why.
LEDGER_UNREADABLE_EVENT = "stage-sweep-ledger-unreadable"


def _ledger_unreadable_receipt(*, tier_id: str, stage_root: str | Path,
                               skipped: str, exc: BaseException,
                               ) -> dict[str, object]:
    """What a sweep that could not read its tier ledger skipped, and why (#1007).

    Nothing is deleted and nothing is released: the bytes and the tokens stay
    where they were until a later cycle can read the ledger.
    """

    reason = f"tier ledger unreadable: {exc}"
    return {
        "schema": pool.POOL_EGRESS_SCHEMA_V1,
        "event": LEDGER_UNREADABLE_EVENT,
        "action_key": "",
        "consumer_action_key": "",
        "tier_id": tier_id,
        "stage_root": str(stage_root),
        "reason": reason,
        "skipped": skipped,
        "entries_deleted": 0,
        "bytes_deleted": 0,
        "tokens_released": 0,
        "complete": False,
        "errors": [reason],
        "host": socket.gethostname(),
        "unix": time.time(),
    }


def _relative_under(stage: Path, resolved: str) -> str | None:
    """This stage root's relative name for a resolved path, or ``None``."""

    try:
        return str(Path(resolved).relative_to(stage.resolve()))
    except (OSError, ValueError):
        return None


def _evidence_live_claims(
    evidence: Iterable[Mapping[str, object]], *, live_roots: set[Path],
    plan_root: Path,
) -> tuple[set[str], dict[str, str]]:
    """Join already-read census documents without performing any I/O.

    Record location establishes row liveness; a plan's consumer_action_key
    binds its movers to that row. Evidence order is immaterial, including for
    single-pass iterables. Resolved byte paths are not liveness evidence.
    """
    wanted: set[str] = set()
    owners: dict[str, str] = {}
    plans: list[Mapping[str, object]] = []
    for record in evidence:
        document = record.get("document")
        origin = record.get("record_path")
        if not isinstance(document, dict) or not isinstance(origin, (str, Path)):
            continue
        path = Path(origin)
        if path.parent in live_roots:
            key = document.get("action_key")
            if not isinstance(key, str) or not key:
                continue
            owners[key] = key
            residency = document.get("residency")
            if isinstance(residency, dict):
                wanted.update(str(lead) for lead in residency.get("leads") or [])
        elif plan_root in path.parents:
            plans.append(document)
    for plan in plans:
        key = plan.get("consumer_action_key")
        if isinstance(key, str) and key in owners:
            wanted.update(residency_plan.mover_keys(plan))
    return wanted, owners


def live_claims(queue: pool.PoolQueue, *, store=None,
                evidence: Iterable[Mapping[str, object]] | None = None,
                ) -> tuple[set[str], dict[str, str]]:
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

    With ``evidence``, reuse the report's bounded reference census records
    (record_path, document, paths), without scanning or reading anything else.
    This is intentionally read-only liveness evidence, not sweep authority:
    owners includes *all* ready/claimed action keys, even bare fragment
    consumers without residency. Only their declared leads and matching frozen
    plan movers enter wanted. Missing evidence is not fetched or repaired;
    census errors remain the report caller's responsibility. The default
    sweep/adoption path and its narrower residency owners are unchanged.
    """
    store = queue.records if store is None else store
    if evidence is not None:
        return _evidence_live_claims(
            evidence,
            live_roots={store.dir(state) for state in
                        (pool.record_store.READY, pool.record_store.CLAIMED)},
            plan_root=residency_plan.registry_root(queue),
        )

    wanted: set[str] = set()
    owners: dict[str, str] = {}
    for state in (pool.record_store.READY, pool.record_store.CLAIMED):
        for path, item in store.state_records(state):
            if not isinstance(item, dict):
                continue
            key = path.name[:-len(".json")] if path.name.endswith(".json") else path.name
            residency = item.get("residency")
            if not isinstance(residency, dict):
                # A bare row the #1247 planner filed a plan for is a consumer
                # like any other (#1332): its movers are its own, not orphans
                # for the sweep to evict under it.
                derived, source = residency_plan.consumer_residency(queue, item)
                if source != "filed_plan" or derived is None:
                    continue
                wanted.update(str(lead) for lead in derived["leads"])  # type: ignore[union-attr]
                wanted.update(residency_plan.filed_mover_keys(queue, key))
                owners[key] = key
                continue
            for lead in residency.get("leads") or []:
                wanted.add(str(lead))
            owners[key] = key
            plan = residency_plan.read(queue, key)
            if plan is not None:
                wanted.update(residency_plan.mover_keys(plan))
    return wanted, owners


def _partial_done_receipt_matches(queue, consumer: str, mover: str,
                                  fragment: Mapping[str, object]) -> bool:
    """Exact partial-copy evidence for an unmaterialized DONE owner (#866).

    The immutable terminal proves the attempt ended; this separately binds
    its incomplete movement declaration to the fragment about to be retired.
    A complete or unknown copy is outside this narrow recovery contract.
    """
    record = json.loads(
        pb.immutable_files.read_stable_file(
            queue.records.move_path(mover),
            where="partial DONE move receipt",
        ),
    )
    if not isinstance(record, dict):
        raise pool.PoolContractError("partial DONE receipt is not an object")
    if record.get("complete") is not False:
        return False
    for field, expected in (("schema", pool.POOL_MOVE_SCHEMA_V1),
                            ("action_key", mover), ("consumer_action_key", consumer),
                            ("tier_id", fragment["tier_id"]),
                            ("stage_root", fragment["stage_root"]),
                            ("manifest_sha256", fragment["manifest_sha256"])):
        if record.get(field) != expected:
            raise pool.PoolContractError(f"partial DONE receipt differs at {field}")
    fields = ("entries_declared", "entries_staged", "bytes_staged",
              "range_start_bytes", "range_end_bytes", "range_bytes")
    if any(type(record.get(field)) is not int or record[field] < 0 for field in fields):
        raise pool.PoolContractError("partial DONE receipt has invalid counts/range")
    entries = fragment["entries"]
    if (not 0 < record["entries_staged"] < record["entries_declared"]
            or record["entries_staged"] != len(entries)
            or record["bytes_staged"] != sum(int(entry["bytes"]) for entry in entries.values())
            or not 0 < record["bytes_staged"] <= record["range_bytes"]
            or record["range_bytes"] != record["range_end_bytes"] - record["range_start_bytes"]):
        raise pool.PoolContractError("partial DONE receipt does not cover its fragment")
    return True


class _TerminalNames:
    """Whether a terminal directory names a key, looked up by key (#992).

    What a listing of ``failed/``, ``withdrawn/`` or ``done/`` was used for:
    a candidate filter, with every decision taken again under the locks by
    :func:`_metadata_absent`.  Any entry at the name counts, as it did in
    the listing.  Each directory is checked once to be a directory, so a
    terminal directory that is missing or unreadable still refuses the
    whole pass rather than reading as "nothing ended"; after that a lookup
    that fails any other way than "no such entry" raises, and the caller
    refuses the same way.  One lookup per key per pass.
    """

    def __init__(self, queue: pool.PoolQueue) -> None:
        self._queue = queue
        self._seen: dict[tuple[str, str], bool] = {}
        for state in (pool.record_store.FAILED, pool.record_store.WITHDRAWN, pool.record_store.DONE):
            queue.records.require_terminal_directory(state)

    def has(self, state: str, key: str) -> bool:
        found = self._seen.get((state, key))
        if found is None:
            found = self._queue.records.item_entry_present(state, key)
            self._seen[(state, key)] = found
        return found


#: The unit `sweep_dead_owner_fragments` asks a cycle budget for: one dead
#: consumer's validation and every census and egress of its movers (#1072).
DEAD_OWNER_UNIT = "dead-owner"
#: The unit `sweep` asks a cycle budget for: one orphan's eviction, a held
#: orphan's or an uncharged dead owner's, with the owner census it takes
#: (#1136).  A saturated pool made each one take 1-13 s, and 47 of them in
#: one cycle took 227.5 s.
ORPHAN_EVICT_UNIT = "orphan-evict"


class _UnchargedOwners:
    """The coherent dead owners no token holds, as one dead-owner pass found them.

    #1061: a failed consumer's executed DONE mover whose move receipt never
    completed holds no stage token -- the ledger released it at ``finish``
    -- yet its files, its fragment and its material stay, and nothing in
    them is stale.  The mint already counts those bytes: supply is writable
    plus landed, and bytes no token holds are outside writable and outside
    every token, so free space is right and a charge would subtract them
    twice.  What no pass did was give them back, because the held-key pass
    reads only held keys.  :func:`sweep_dead_owner_fragments` fills this;
    :func:`sweep` evicts from it under pressure and reports it.

    ``by_tier`` maps a tier to ``(mover, consumer, fragment)`` per owner:
    the fragment the discovery validated, whose entries' bytes the report
    sums only when the set it reports has changed, so a steady cycle pays a
    list append per owner and no walk of its entries.  ``known`` is false when discovery
    did not complete, and ``unknown_tiers`` names a tier whose ledger or
    one of whose owners could not be read: the collection is then not a
    census of that tier, and its report waits.
    """

    def __init__(self) -> None:
        self.known = True
        self.unknown_tiers: set[str] = set()
        self.by_tier: dict[str, list[tuple[str, str, Mapping]]] = {}

    def add(self, tier_id: str, mover: str, consumer: str,
            fragment: Mapping) -> None:
        self.by_tier.setdefault(tier_id, []).append((mover, consumer, fragment))

    def census_of(self, tier_id: str) -> bool:
        """Whether this pass's collection is a complete census of ``tier_id``."""

        return self.known and tier_id not in self.unknown_tiers


def _left_coherent(receipt: Mapping[str, object]) -> bool:
    """Whether a stale-mention verdict left its owner whole and current.

    The transaction's own "nothing to act on" verdicts (#853, #1056): the
    owner retained whole with no retention reason, or under a co-owner, and
    no error.  A retention reason -- a taint, a live pin or claim, a
    handoff -- is not coherence, and neither is an eviction or a changed
    document.  Nor is a partial prune, whose receipt carries no retention
    reason either: the fragment the discovery read is no longer the one on
    disk, so the owner is collected on the next pass, from the rewritten
    fragment.
    """

    return (receipt.get("event") == staged_path.STALE_MENTION_EVENT
            and receipt.get("retained_reason") in ("", "co-owner")
            and not receipt.get("partial")
            and not receipt.get("errors"))


def _fragment_bytes(fragment: Mapping[str, object]) -> int:
    """The bytes a validated fragment's entries name."""

    entries = fragment.get("entries")
    if not isinstance(entries, Mapping):
        return 0
    return sum(int(entry.get("bytes") or 0) for entry in entries.values()
               if isinstance(entry, Mapping))


def _evict_uncharged_owner(
    queue: pool.PoolQueue,
    mover: str,
    consumer: str,
    *,
    tier_id: str,
    stage_root: str,
    residency_root: str | Path | None,
    store=None,
) -> dict[str, object] | None:
    """Give back one uncharged dead owner's bytes, or ``None`` if it revived.

    The death proof is taken again here, under the consumer's transition
    lock and then the mover's (the dead-owner pass's order): neither may be
    queued again, the consumer may hold no lease or plan, and the mover may
    hold no token on the tier.  Anything else returns ``None`` and the pass
    moves on.  The eviction is whole (#903), so a live pin, a promotion
    handoff or the mover's own live copy declines it with nothing unlinked;
    a co-owner's fragment or a claimed copy keeps its file through the
    shared verdict, and the last owner to leave deletes.
    """
    store = queue.records if store is None else store

    try:
        with queue.action_transition_lock(consumer):
            for key in (consumer, mover):
                live, why = residency_plan.live_state(queue, key)
                if live or why:
                    return None
            if (not (not pool.record_store.value_or_raise(
                queue.records.retirement_presence_at(queue.records.lease_path(consumer)),
            ))
                    or not (not pool.record_store.value_or_raise(
                        residency_plan.retirement_presence(queue, consumer),
                    ))):
                return None
            with queue.mover_transition_lock(mover):
                if not queue.tier(tier_id).holder_absent(mover):
                    return None
                return staged_path.evict(queue, mover, consumer_action_key=consumer,
                             stage_root=stage_root,
                             residency_root=residency_root,
                             reason="uncharged-owner-sweep", whole=True, store=store)
    except (OSError, ValueError, pb.PrismaBuildError) as exc:
        return {
            "schema": pool.POOL_EGRESS_SCHEMA_V1, "event": DEAD_OWNER_EVENT,
            "action_key": mover, "consumer_action_key": consumer,
            "tier_id": tier_id, "stage_root": str(stage_root),
            "reason": "uncharged-owner-sweep", "bytes_deleted": 0,
            "complete": False, "errors": [f"ownership uncertain: {exc}"],
            "host": socket.gethostname(), "unix": time.time(),
        }


def _uncharged_owner_report(queue: pool.PoolQueue, tier_id: str,
                            stage_root: str,
                            owners: list[tuple[str, str, Mapping]], *,
                            evicted: Mapping[str, int],
                            needed: int | None) -> list[dict[str, object]]:
    """The tier's uncharged dead owners, once per change of the set (#1061).

    ``owners`` is what the dead-owner pass found on the tier and
    ``evicted`` maps each owner this pass gave back to the bytes it
    deleted.  The report names what is left: the count and the bytes their
    fragments name (a file two owners name counts once per owner), plus
    what this pass evicted and the room the tier needed.  It is filed when
    what is left differs from the last report, and whenever this pass
    evicted one, so an owner found and evicted in one pass is reported.
    """

    left = [(mover, fragment) for mover, _consumer, fragment in owners
            if mover not in evicted]
    named = tuple(sorted(mover for mover, _fragment in left))
    memo = (str(queue.root), tier_id)
    if _UNCHARGED_REPORTS.get(memo, ()) == named and not evicted:
        return []
    _UNCHARGED_REPORTS[memo] = named
    return [{
        "schema": pool.POOL_EGRESS_SCHEMA_V1, "event": UNCHARGED_OWNERS_EVENT,
        "action_key": "", "consumer_action_key": "", "tier_id": tier_id,
        "stage_root": str(stage_root), "reason": "orphan-sweep",
        "owners": len(left),
        "bytes": sum(_fragment_bytes(fragment) for _mover, fragment in left),
        "owners_evicted": len(evicted),
        "bytes_evicted": sum(int(value) for value in evicted.values()),
        "pressure_gib": needed,
        "complete": True, "errors": [],
        "host": socket.gethostname(), "unix": time.time(),
    }]


def sweep_dead_owner_fragments(
        queue: pool.PoolQueue, *, stage_roots: dict[str, str],
        residency_root: str | Path | None = None,
        index: kept_reads.CensusIndex | None = None,
        budget=None,
        uncharged: "_UnchargedOwners | None" = None, store=None) -> list[dict[str, object]]:
    """Retire proven dead unmaterialized owners with no charge (#839, #866).

    A consumer is dead when it has exactly one terminal record and that
    record is proven: a failed one by its attempt-backed summary, a withdrawn
    one by its exact immutable withdrawal decision (#892).

    One complete fragment census and one ledger discovery per tier identify
    candidates, never authorize deletion. Each consumer is then held across
    validation and all its evictions; each child is held before its fresh
    state checks and existing egress. Lock order is consumer -> mover -> stage
    ownership, the same as window publication and dead-consumer withdrawal.
    Thus a successor published after discovery either wins before these locks
    and is retained, or waits until this old owner's egress has completed.

    Failure evidence is the queue's immutable attempt-backed summary, checked
    once per consumer transaction. A cancellation must match its immutable
    generation decision. A DONE mover instead needs an immutable executed
    terminal plus one of two positive shapes: an incomplete move receipt
    exactly covering an unmaterialized fragment (#866), or a material sidecar
    that is pruned of the paths whose live destination carries a different
    incarnation (#853, :func:`prune_stale_mentions`).  The second shape may
    carry a charge: a partial prune retains the whole holder and no charge
    moves, while a fully stale owner goes through the ordinary whole-owner
    egress exactly once.  Missing, unreadable, legacy or inconsistent evidence
    retains, as do any plan, live row, lease, live claim, reader pin,
    promotion handoff or unreadable census. Withdrawn movers still require no
    receipt and are never partially pruned. Other terminal shapes and all
    produced namespaces remain excluded. No age or pressure is deletion
    authority here.

    ``uncharged`` collects what this pass leaves for the held-key pass
    (#1061): each material-bearing DONE owner it proved dead that holds no
    token on its tier and whose paths the stale-mention transaction found
    whole and current -- a verdict of nothing to act on (retained whole,
    alone or under a co-owner, with no error), or a skip checkpoint's hit,
    which certifies the paths whatever retained the owner when it was
    installed.  This pass deletes none of them.  Their deletion authority is
    the same death proof, taken again under the consumer's lock by
    :func:`sweep`, and pressure only decides when; ``evict`` rechecks
    co-owners, claims, pins and handoffs under its own locks.  A discovery
    that could not complete marks the collection unknown rather than empty,
    and so does a consumer the cycle budget did not reach, for its tiers:
    what the pass did not examine is not a census.

    Discovery reads only what can name a candidate (#992).  The census is
    ``index``'s when the tier loop passes its own, so a cycle re-reads only
    the namespaces that changed; and the terminal records are looked up by
    key for the consumers and movers the census names, never listed: the
    three terminal directories hold every action the fleet ever finished
    (38,000 names on 2026-09-23) and a fragment names a few hundred.

    The stale-mention skip checkpoints follow the same discovery (#1056):
    once it completes, only the owners it found keep a checkpoint, one
    each, and ``index`` counts the owners a checkpoint skipped
    (``stale_skipped``) and the owners censused (``stale_censused``) for
    the cycle line, since a skip files no receipt, and, by reason, the
    censused owners that were idle but whose checkpoint was refused
    (``stale_cache_refused``, #1069).

    ``budget``, when the tier loop passes one (#1072), is asked before each
    candidate consumer whether its unit still fits this cycle
    (``budget.start``) and told when the unit ends (``budget.done``), after
    the consumer's locks are released.  The consumers are taken from the one
    it refused first last cycle (``budget.order``).  A consumer it refuses is
    not examined, and keeps its skip checkpoints: they are chosen from the
    whole discovery above, before any unit runs.
    """
    store = queue.records if store is None else store
    root = Path(residency_root if residency_root is not None
                else queue.root / pool.record_store.RESIDENCY)
    if root.name == oc.OUTPUT_FRAGMENTS_SUBDIR:
        if uncharged is not None:
            uncharged.known = False
        return []
    receipts: list[dict[str, object]] = []

    def unknown() -> list[dict[str, object]]:
        """End the pass with nothing discovered, and say it was not a census."""

        if uncharged is not None:
            uncharged.known = False
        return receipts

    candidates: dict[str, list[tuple[str, dict]]] = {}

    def refuse(why: str, consumer: str = "", mover: str = "") -> None:
        receipts.append({
            "schema": pool.POOL_EGRESS_SCHEMA_V1, "event": DEAD_OWNER_EVENT,
            "action_key": mover, "consumer_action_key": consumer,
            "tier_id": "", "stage_root": "", "reason": "dead-owner-sweep",
            "complete": False, "errors": [why],
            "host": socket.gethostname(), "unix": time.time(),
        })
        if uncharged is not None and consumer:
            # An owner this pass could not judge may be one the collection
            # would hold: its tier's report waits for a pass that can.
            for child, fragment in candidates.get(consumer, ()):
                if not mover or child == mover:
                    uncharged.unknown_tiers.add(str(fragment.get("tier_id")))

    fragments, tainted = staged_path.fragment_census(root, index)
    if tainted:
        refuse("ownership uncertain: " + "; ".join(tainted[:8]))
        return unknown()
    try:
        named = _TerminalNames(queue)
    except OSError as exc:
        refuse(f"ownership uncertain: queue census: {exc}")
        return unknown()
    held_by_tier: dict[str, set[str]] = {}
    for tier, stage in stage_roots.items():
        if staged_path.stage_root_refusal(queue, stage) is not None:
            continue
        try:
            # Include nonregular holder names: an unknown ledger entry is
            # not evidence that this mover has no charge.
            ledger = queue.tier(tier)
            held_by_tier[tier] = ledger.holder_names()
        except FileNotFoundError:
            held_by_tier[tier] = set()
        except (OSError, pool.PoolContractError) as exc:
            refuse(f"ownership uncertain: tier ledger: {exc}")
            if uncharged is not None:
                uncharged.unknown_tiers.add(tier)
    try:
        for consumer, mover, fragment, direct in fragments:
            tier = fragment.get("tier_id")
            if (direct and staged_path._namespace_shaped(consumer)
                    and tier in held_by_tier
                    and (named.has(pool.record_store.FAILED, consumer)
                         or named.has(pool.record_store.WITHDRAWN, consumer))
                    and (named.has(pool.record_store.WITHDRAWN, mover)
                         or named.has(pool.record_store.DONE, mover))):
                # The charge is not a discovery filter: a charged,
                # material-bearing DONE owner is the #853 shape the material
                # branch prunes.  The zero-charge gates of #839/#866 stay in
                # the per-mover block below.
                candidates.setdefault(consumer, []).append((mover, fragment))
    except OSError as exc:
        refuse(f"ownership uncertain: queue census: {exc}")
        return unknown()
    # Discovery is complete: the skip checkpoints kept are exactly those of
    # the owners it found (#1056), so an owner that left -- evicted, adopted,
    # its fragment gone -- stops holding a place in the cache, and the cache
    # holds at most one checkpoint per owner on disk.
    staged_path._retain_skip_checkpoints(
        staged_path._skip_checkpoint_key(queue, root, Path(stage_roots[str(fragment["tier_id"])]),
                             str(fragment["tier_id"]), consumer, mover)
        for consumer, children in candidates.items()
        for mover, fragment in children)
    uncertainty = (OSError, ValueError, pb.PrismaBuildError)
    order = (list(candidates) if budget is None
             else budget.order(DEAD_OWNER_UNIT, list(candidates)))
    for consumer in order:
        children = candidates[consumer]
        if budget is not None and not budget.start(DEAD_OWNER_UNIT, consumer):
            if uncharged is not None:
                # Not examined this cycle: its tiers' uncharged sets are
                # not a census, so their reports wait (#1061).
                uncharged.unknown_tiers.update(
                    str(fragment.get("tier_id")) for _mover, fragment in children)
            continue
        try:
            _dead_owner_unit(queue, consumer, children, root=root,
                             stage_roots=stage_roots, index=index,
                             refuse=refuse, receipts=receipts,
                             uncertainty=uncertainty,
                             held_by_tier=held_by_tier, uncharged=uncharged, store=store)
        finally:
            if budget is not None:
                budget.done(DEAD_OWNER_UNIT)
    return receipts


def _dead_owner_unit(queue: pool.PoolQueue, consumer: str,
                     children: list[tuple[str, dict]], *, root: Path,
                     stage_roots: dict[str, str], index: kept_reads.CensusIndex | None,
                     refuse, receipts: list[dict[str, object]],
                     uncertainty: tuple[type[BaseException], ...],
                     held_by_tier: Mapping[str, set[str]] | None = None,
                     uncharged: _UnchargedOwners | None = None, store=None) -> None:
    """One candidate consumer of `sweep_dead_owner_fragments`, under its locks.

    ``held_by_tier`` is the discovery's ledger listing and ``uncharged`` the
    collection it fills (#1061); either absent collects nothing.
    """
    store = queue.records if store is None else store

    try:
        with queue.action_transition_lock(consumer):
            live, why = residency_plan.live_state(queue, consumer)
            if why:
                refuse(f"ownership uncertain: {why}", consumer)
            if live or why:
                return
            # Exactly one terminal record: failed or withdrawn.  Two is
            # not a death but a question, and done is not a death at all.
            if not (not pool.record_store.value_or_raise(
                queue.records.retirement_presence_at(
                    queue.records.item_path(pool.record_store.DONE, consumer),
                ),
            )):
                return
            consumer_failed = not (not pool.record_store.value_or_raise(
                queue.records.retirement_presence_at(
                    queue.records.item_path(pool.record_store.FAILED, consumer),
                ),
            ))
            consumer_withdrawn = not (not pool.record_store.value_or_raise(
                queue.records.retirement_presence_at(
                    queue.records.item_path(pool.record_store.WITHDRAWN, consumer),
                ),
            ))
            if consumer_failed == consumer_withdrawn:
                return
            if (not (not pool.record_store.value_or_raise(
                queue.records.retirement_presence_at(queue.records.lease_path(consumer)),
            ))
                    or not (not pool.record_store.value_or_raise(
                        residency_plan.retirement_presence(queue, consumer),
                    ))):
                return
            if consumer_failed:
                failed = staged_path._owner_record(queue, pool.record_store.FAILED, consumer)
                # Reuse the queue's canonical history/generation/log
                # checks; no whole-model hashing, and only once for this
                # consumer.
                ending = queue.attempt_ending(failed)
                if (failed.get("status") != "failed"
                        or ending["status"] != "failed"
                        or ending["disposition"] != pool.record_store.FAILED):
                    return
            else:
                # A withdrawn consumer is as dead as a failed one when its
                # immutable decision says so -- the proof a withdrawn
                # mover already needs below.  Without it, WS-P cleared
                # three such owners by hand on 2026-09-22.
                staged_path._require_exact_withdrawal(queue, consumer)
            for mover, observed in children:
                try:
                    with queue.mover_transition_lock(mover):
                        live, why = residency_plan.live_state(queue, mover)
                        if why:
                            refuse(f"ownership uncertain: {why}", consumer, mover)
                        if live or why:
                            continue
                        if not (not pool.record_store.value_or_raise(
                            queue.records.retirement_presence_at(
                                queue.records.item_path(pool.record_store.FAILED, mover),
                            ),
                        )):
                            continue
                        done = not (not pool.record_store.value_or_raise(
                            queue.records.retirement_presence_at(
                                queue.records.item_path(pool.record_store.DONE, mover),
                            ),
                        ))
                        if done:
                            if not (not pool.record_store.value_or_raise(
                                queue.records.retirement_presence_at(
                                    queue.records.item_path(pool.record_store.WITHDRAWN, mover),
                                ),
                            )):
                                continue
                            terminal = staged_path._owner_record(queue, pool.record_store.DONE, mover)
                            ending = queue.attempt_ending(terminal)
                            if (terminal.get("status") != "executed"
                                    or ending["status"] != "executed"
                                    or ending["disposition"] != pool.record_store.DONE):
                                continue
                        else:
                            staged_path._require_exact_withdrawal(queue, mover)
                        tier = str(observed["tier_id"])
                        if not (not pool.record_store.value_or_raise(
                            queue.records.retirement_presence_at(queue.records.lease_path(mover)),
                        )):
                            continue
                        material_present = not pb.immutable_files.confirmed_metadata_absence(
                            reader_lease.material_path(root, consumer, mover),
                        )
                        if material_present:
                            # #853: a material-bearing DONE owner, charged
                            # or not.  A withdrawn one keeps whatever its
                            # sidecar dates (no re-dispatch guarantee
                            # exists for it here), exactly as before.
                            if not done:
                                continue
                            if staged_path._skip_checkpoint_hit(
                                    staged_path._skip_checkpoint_key(
                                        queue, root,
                                        Path(stage_roots[tier]), tier,
                                        consumer, mover),
                                    residency_map.fragment_path(
                                        root, consumer, mover),
                                    reader_lease.material_path(
                                        root, consumer, mover), trust=index.trust if index is not None else kept_reads.filesystem_trust):
                                # No receipt for a skip; the cycle line
                                # counts it (#1056).
                                if index is not None:
                                    index.stale_skipped += 1
                                coherent = True
                            else:
                                if index is not None:
                                    index.stale_censused += 1
                                pruned = staged_path.prune_stale_mentions(
                                    queue, mover, consumer_action_key=consumer,
                                    stage=Path(stage_roots[tier]), tier_id=tier,
                                    root=root, observed=observed, trust=index.trust if index is not None else kept_reads.filesystem_trust)
                                receipts.append(pruned)
                                refused = pruned.get("cache_refused")
                                if index is not None and refused:
                                    counts = index.stale_cache_refused
                                    counts[str(refused)] = counts.get(
                                        str(refused), 0) + 1
                                coherent = _left_coherent(pruned)
                            # #1061: an owner with no token whose paths the
                            # transaction found whole is room under pressure.
                            if (coherent and uncharged is not None
                                    and held_by_tier is not None
                                    and mover not in held_by_tier.get(tier, ())):
                                uncharged.add(tier, mover, consumer, observed)
                            continue
                        if not queue.tier(tier).holder_absent(mover):
                            continue
                        if not done and not (not pool.record_store.value_or_raise(
                            queue.records.retirement_presence_at(queue.records.move_path(mover)),
                        )):
                            continue
                        current = residency_map.validate_fragment(
                            json.loads(
                                pb.immutable_files.read_stable_file(
                                    residency_map.fragment_path(root, consumer, mover),
                                    where="dead-owner fragment",
                                ),
                            ),
                        )
                        if current != observed:
                            continue  # adoption/republication won discovery
                        if done and not _partial_done_receipt_matches(
                                queue, consumer, mover, current):
                            continue
                        receipts.append(staged_path.evict(
                            queue, mover, consumer_action_key=consumer,
                            stage_root=stage_roots[tier], residency_root=root,
                            reason="dead-owner-sweep", store=store))
                except uncertainty as exc:
                    refuse(f"ownership uncertain: {exc}", consumer, mover)
    except uncertainty as exc:
        refuse(f"ownership uncertain: {exc}", consumer)


def sweep(queue: pool.PoolQueue, *, stage_roots: dict[str, str],
          residency_root: str | Path | None = None,
          pressure: Mapping[str, int] | None = None,
          index: kept_reads.CensusIndex | None = None,
          budget=None, store=None) -> list[dict[str, object]]:
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

    **A held mover with no receipt is named by its fragment (#892).**  The
    receipt is where this reads a mover's consumer, and a mover can outlive
    its receipt: the canary leg-3 mover ``aa34e2a6e22f`` held 1 GiB for
    three days with none.  Exactly one direct fragment naming the mover is
    an exact owner, and once that owner has provably ended -- not queued, and
    exactly one outcome record -- the mover is an orphan like any other.  No
    fragment, several, a produced-output one, a tainted census, or an owner
    with no ending retains.  A retained holder is reported, with the reason,
    only when the tier's window still lacks room after the pass; otherwise it
    waits quietly, as it did before.

    **A funded produced-output mover is its lane's (#929).**  Before either
    rule above, :func:`produced_holder` asks the output funding record whether
    the key is a produced mover on this tier.  If it is, the lane decides:
    a live producer attempt, or a mover still queued, keeps it; a producer
    attempt that has ended, with the mover ended too, is retired at once
    through the producer's own ``retire_batch``, pressure or none, because a
    retried producer binds a new batch namespace and nothing can read this
    copy again (the live ``6fbc96301c6c`` held 1 GiB that way after its
    producer failed and its mover was withdrawn from ``ready``).  Everything
    else is kept.  Every held key the pass can prove neither live nor an
    orphan is reported as ``stage-holder-unresolved`` once per change of its
    reason, whatever the pressure: a holder nothing can classify is seen
    once, not never and not every cycle.

    **Dead owners are retired unconditionally (#839).**  A failed consumer's
    withdrawn mover holds no tokens and filed no receipt, so the held-key
    pass above can never see it -- yet its fragment still forbids publication
    of its paths, which is a liveness block rather than a capacity question.
    `sweep_dead_owner_fragments` runs once across tiers before the held-key
    pass and retires each exact stale owner through the
    same `evict`, which rechecks co-owners, claims, pins, and handoffs under
    its own locks; a resubmitted consumer is excluded by the locked state
    recheck before eviction, so pressure deference would only preserve the block.

    **An uncharged dead owner with material is an orphan under pressure
    (#1061).**  A failed consumer's executed DONE mover whose move receipt
    never completed holds no token -- the ledger released it at ``finish``
    -- while its coherent files, fragment and material stay.  The mint
    already counts those bytes (supply is writable plus landed, and no token
    holds them), so they are never charged: a charge would subtract them
    twice.  The dead-owner pass above names every such owner it proved dead
    and left whole, and this pass takes them as candidates beside the held
    orphans, in the same oldest-receipt order, only on a tier with pressure:
    ``{}``, zero and ``None`` alike evict none of them, since without a
    window waiting they are cache and nothing else.  Each goes through
    :func:`_evict_uncharged_owner`, which retakes the death proof under the
    consumer's lock and evicts whole, so a live pin, a handoff or the
    mover's own live copy keeps every byte, and co-owners settle through
    the shared verdict.  An owner a skip checkpoint stands for may have
    been pinned or claimed when the checkpoint was installed, since the
    checkpoint certifies paths and not retention reasons; while that
    reason lives, each pressured cycle pays one declined eviction for it,
    whose census (#1056 measured about 1.85 s per owner on the live stage)
    runs before the decline.  The bytes each eviction deletes are credited
    against the room the tier needs, in whole GiB, until the ledger's next
    mint shows them, and the pass stops once the room is covered.  The
    tier's set of such owners -- count and bytes -- is reported as
    ``stage-uncharged-dead-owners`` once per change.

    **``index`` is the tier loop's census, kept from one cycle to the next
    (#992).**  Every census this pass takes -- the dead-owner discovery, the
    receipt-less owner lookup, each tier's reconciliation -- reads it, so a
    namespace or document that has not changed since the last cycle is not
    read again.  Without it each census reads from nothing, as before.

    ``budget`` is the tier loop's cycle budget (#1072).  The pass has two
    kinds of long unit.  `sweep_dead_owner_fragments` asks it for each dead
    consumer (``DEAD_OWNER_UNIT``), and the orphan pass below asks it for
    each eviction, held orphan and uncharged dead owner alike
    (``ORPHAN_EVICT_UNIT``, #1136): ``budget.start`` after the pressure check
    and before the eviction, ``budget.done`` after it, whatever its outcome.
    So the tier record can be re-announced between evictions, and the pass
    stops at the cycle's budget.  An orphan the budget refuses stays resident
    for the next cycle, whose pass starts from it (``budget.order``) and then
    takes the rest oldest first; since the budget remembers one refused key
    per kind, the resumption holds on the tier it was refused on.  Without a
    budget, the pass evicts in age order until the pressure is met, as
    before.
    """
    store = queue.records if store is None else store

    wanted, owners = live_claims(queue, store=store)
    swept: list[dict[str, object]] = []
    uncharged = _UnchargedOwners()
    swept.extend(sweep_dead_owner_fragments(
        queue, stage_roots=stage_roots, residency_root=residency_root,
        index=index, budget=budget, uncharged=uncharged, store=store))
    # Taken once, and only when a held key has no receipt to name its
    # consumer (#892).
    fragment_owners: dict[str, list[tuple[str, bool]]] | str | None = None
    for tier_id, stage_root in stage_roots.items():
        refusal = staged_path.stage_root_refusal(queue, stage_root)
        if refusal is not None:
            # One receipt per tier per cycle, and neither the held-key
            # evictions nor the reconciliation run: the root is not this
            # queue's to delete under (#628).  ``evict`` and ``reconcile``
            # refuse on the same fact for the callers that reach them directly.
            swept.append(staged_path._refused_receipt(tier_id=tier_id, stage_root=stage_root,
                                          refusal=refusal))
            continue
        try:
            held = queue.tier(tier_id).holders()
        except (OSError, pool.PoolContractError) as exc:
            # Neither pass can run without the held set, and a skip is a
            # record, not a silence (#1007).
            swept.append(_ledger_unreadable_receipt(
                tier_id=tier_id, stage_root=stage_root,
                skipped="held-key pass and reconciliation", exc=exc))
            continue
        # (staged unix, mover, consumer, uncharged): one age order for held
        # orphans and uncharged dead owners alike (#1061).
        orphans: list[tuple[float, str, str, bool]] = []
        retained: list[dict[str, object]] = []
        unresolved: dict[str, str] = {}
        for key in held:
            if key in wanted or key in owners:
                continue
            # A prelaunch group holder is the prelaunch pass's room
            # reservation (#1594). It has no receipt and no fragment.
            # The prelaunch pass owns its release, so keep it quietly.
            if _is_prelaunch_holder(key):
                continue
            # A funded produced-output mover is its lane's, receipt or no
            # receipt (#929).  Its receipt names a batch namespace, not a
            # queue action, and its fragment lives in the produced store, so
            # neither rule below can read it: asked of the flat store, a live
            # producer's completed batch looks unowned and would lose its
            # tokens while its bytes stayed.
            verdict = produced_holder(queue, tier_id, key)
            if verdict is not None:
                if verdict["class"] == "dead":
                    # Not pressure-gated: nothing can read this copy again,
                    # so keeping it is not a cache (#598), and its token pins
                    # its funding record (`retire_terminal_output_funding`).
                    outcome = _retire_produced_orphan(
                        queue, key, verdict, tier_id=tier_id,
                        stage_root=stage_root,
                        residency_root=Path(
                            residency_root if residency_root is not None
                            else queue.root / pool.record_store.RESIDENCY))
                    if outcome["complete"]:
                        swept.append(outcome)
                    else:
                        unresolved[key] = (
                            "its producer attempt has ended and its batch "
                            "retirement did not complete: "
                            + "; ".join(str(e) for e in outcome["errors"]))
                elif verdict["class"] == "unknown":
                    unresolved[key] = str(verdict["why"])
                continue
            receipt = queue.move_record(key)
            consumer = (str(receipt.get("consumer_action_key")) if isinstance(receipt, dict)
                        else "")
            if not consumer:
                # No receipt names the consumer: ask the fragments (#892).
                if fragment_owners is None:
                    fragment_owners = _held_mover_fragment_owners(
                        Path(residency_root if residency_root is not None
                             else queue.root / pool.record_store.RESIDENCY), index)
                consumer, why = _receiptless_owner(fragment_owners, key)
                if consumer:
                    why = staged_path.unended_owner(queue, consumer)
                    if why:
                        consumer = ""
                if not consumer:
                    # A live item's own holding is named by a live claim:
                    # kept, and not reported as anybody's mystery (#929).
                    if not _held_by_a_live_item(queue, key, owners):
                        retained.append(_receiptless_refusal(
                            key, tier_id=tier_id, stage_root=stage_root,
                            why=why))
                        unresolved[key] = why
                    continue
            orphans.append((_staged_unix(receipt), key, consumer, False))
        needed = None if pressure is None else int(pressure.get(tier_id, 0))
        dead_uncharged = [
            (mover, consumer)
            for mover, consumer, _fragment in uncharged.by_tier.get(tier_id, ())
            if mover not in wanted and mover not in owners]
        if needed is not None and needed > 0:
            # Only a tier that needs room reads their receipts: the order is
            # all they are for, and no pressure evicts none of them (#1061).
            orphans.extend(
                (_staged_unix(queue.move_record(mover)), mover, consumer, True)
                for mover, consumer in dead_uncharged)
        orphans.sort()
        kind = storage_tiers.capacity_kind_of(tier_id)
        # Bytes an uncharged eviction deleted are room the ledger cannot
        # show until the next mint counts the grown writable: no token held
        # them.  Credited here, floored to whole GiB as a charged egress
        # frees them, so the pass stops once the room is covered.
        credit_bytes = 0
        evicted_uncharged: dict[str, int] = {}
        # Each eviction is a unit of the cycle's budget (#1136), taken from
        # the orphan the budget refused first last cycle, then oldest first.
        by_key = {key: (consumer, is_uncharged)
                  for _, key, consumer, is_uncharged in orphans}
        order = (list(by_key) if budget is None
                 else budget.order(ORPHAN_EVICT_UNIT, list(by_key)))
        for key in order:
            consumer, is_uncharged = by_key[key]
            if needed is not None:
                if needed <= 0:
                    break      # nothing on this tier is waiting for the room
                try:
                    free = int(queue.tier(tier_id).free_supply().get(kind, 0))
                except (OSError, pool.PoolContractError):
                    break
                if free + staged_path._tokens_for_newly_free_bytes(credit_bytes) >= needed:
                    break      # the window fits now; the rest stays resident
            if budget is not None and not budget.start(ORPHAN_EVICT_UNIT, key):
                continue       # past this cycle's budget: resident until the next
            try:
                try:
                    if not is_uncharged:
                        swept.append(staged_path.evict(queue, key, consumer_action_key=consumer,
                                           stage_root=stage_root,
                                           residency_root=residency_root,
                                           reason="orphan-sweep", store=store))
                        continue
                    outcome = _evict_uncharged_owner(
                        queue, key, consumer, tier_id=tier_id,
                        stage_root=stage_root, residency_root=residency_root, store=store)
                except pool.TransitionLockBusy:
                    # Inside the tier loop no transition lock is waited on
                    # (#1115): the refusal already named the key and its
                    # holder, and the orphan stays for the next cycle's sweep.
                    deferred = staged_path._refused_receipt(
                        tier_id=tier_id, stage_root=stage_root,
                        refusal="transition-lock-busy", mover_action_key=key,
                        consumer_action_key=consumer, reason="orphan-sweep")
                    deferred["event"] = "stage-orphan-eviction-deferred"
                    swept.append(deferred)
                    continue
                if outcome is None:
                    continue   # revived or charged since discovery: not ours
                swept.append(outcome)
                credit_bytes += int(outcome.get("bytes_deleted") or 0)
                if outcome.get("complete"):
                    evicted_uncharged[key] = int(outcome.get("bytes_deleted") or 0)
            finally:
                if budget is not None:
                    budget.done(ORPHAN_EVICT_UNIT)
        if needed is not None:
            needed_after_credit: int | None = (
                needed - staged_path._tokens_for_newly_free_bytes(credit_bytes))
        else:
            needed_after_credit = None
        if uncharged.census_of(tier_id):
            swept.extend(_uncharged_owner_report(
                queue, tier_id, stage_root, uncharged.by_tier.get(tier_id, []),
                evicted=evicted_uncharged, needed=needed))
        reported: set[str] = set()
        if retained and _still_short(queue, tier_id, kind, needed_after_credit):
            # A retained receipt-less holder is reported only when keeping it
            # costs something: the tier's window still lacks room after every
            # orphan that could go has gone.  Otherwise it is a quiet wait, as
            # before #892, rather than a line on every cycle.
            swept.extend(retained)
            reported = {str(entry["action_key"]) for entry in retained}
        # And every holder the pass could prove neither live nor an orphan is
        # reported once per change of its reason, pressure or none (#929).
        swept.extend(_unresolved_reports(queue, tier_id, stage_root, unresolved,
                                         reported=reported))
        # Held keys first, then the rest of the stage: the evictions above turn
        # held bytes into absent ones, so the reconciliation below sees the same
        # directory the ledger now describes rather than one eviction behind it.
        # ``wanted`` is joined with what is *still* held, because a pinned mover
        # a live plan no longer names has just been evicted and a pinned one it
        # does name is attribution.
        try:
            still_held = set(queue.tier(tier_id).holders())
        except (OSError, pool.PoolContractError) as exc:
            # Without the held set, a held mover no live plan names is not
            # attribution, and its bytes would read as unowned.  Unknown
            # ownership never deletes: skip the reconciliation this cycle
            # and say so (#1007).
            swept.append(_ledger_unreadable_receipt(
                tier_id=tier_id, stage_root=stage_root,
                skipped="reconciliation", exc=exc))
            continue
        reconciled = staged_path.reconcile(
            queue, tier_id=tier_id, stage_root=stage_root,
            wanted=wanted | set(owners) | still_held,
            residency_root=residency_root, index=index, store=store)
        # Reported only when it has something to report.  A window in flight
        # skips the reconciliation every cycle, and a line per cycle saying so
        # would bury the eviction it exists to announce.
        if (reconciled["entries_deleted"] or reconciled["errors"]
                or reconciled["unowned_left"]):
            swept.append(reconciled)
    return swept


def _held_mover_fragment_owners(root: Path, index: "kept_reads.CensusIndex | None" = None,
                                ) -> dict[str, list[tuple[str, bool]]] | str:
    """Every mover's fragment owners under ``root``, or why that is unknown.

    Maps each mover key to the ``(namespace, direct)`` of every fragment
    naming it.  A tainted census is returned as its reason instead: a
    fragment that cannot be read may be the one that names another owner.
    """

    fragments, tainted = staged_path.fragment_census(root, index)
    if tainted:
        return "ownership uncertain: " + "; ".join(tainted[:staged_path.ATTRIBUTION_TAINT_LIMIT])
    owners: dict[str, list[tuple[str, bool]]] = {}
    for namespace, mover, _fragment, direct in fragments:
        owners.setdefault(mover, []).append((namespace, direct))
    return owners


def _receiptless_owner(owners: dict[str, list[tuple[str, bool]]] | str,
                       mover: str) -> tuple[str, str]:
    """The consumer that owns a held mover with no receipt, or why none can.

    The fragment is the document that names whose bytes a mover staged, so
    exactly one direct fragment is an exact owner.  Once
    :func:`_unended_owner` also proves that owner ended, the orphan sweep
    treats the holder as it treats any orphan, pressure first and ``evict``
    after, and ``evict`` rechecks co-owners, claims, pins and handoffs under
    its own locks.  Anything else is not an owner (#892):

    * no fragment: no document names one.  A funded produced-output mover
      never reaches this question: :func:`produced_holder` decides it first
      (#929), from its funding record;
    * more than one fragment: two documents disagree;
    * a produced-output fragment: its batch's lifecycle owns the mover.
    """

    if isinstance(owners, str):
        return "", owners
    named = owners.get(mover, [])
    if not named:
        return "", "no fragment names this held mover and it has no receipt"
    if len(named) > 1:
        return "", (f"{len(named)} fragments name this held mover: "
                    + ", ".join(sorted(namespace[:12] for namespace, _ in named)))
    namespace, direct = named[0]
    if not direct:
        return "", (f"only a produced-output fragment ({namespace[:12]}) names "
                    f"this held mover; its batch lifecycle owns it")
    return namespace, ""


def _staged_unix(receipt: object) -> float:
    """A move receipt's time, the orphan pass's age order; 0 when it has none."""

    if not isinstance(receipt, dict):
        return 0.0
    try:
        return float(receipt.get("unix", 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _still_short(queue: pool.PoolQueue, tier_id: str, kind: str,
                 needed: int | None) -> bool:
    """Whether the tier's window still lacks room after this pass's evictions."""

    if needed is None or needed <= 0:
        return False
    try:
        free = int(queue.tier(tier_id).free_supply().get(kind, 0))
    except (OSError, pool.PoolContractError):
        return True
    return free < needed


def _receiptless_refusal(mover: str, *, tier_id: str, stage_root: str,
                         why: str) -> dict[str, object]:
    """The receipt an unresolvable receipt-less holder files each pass."""

    return {
        "schema": pool.POOL_EGRESS_SCHEMA_V1, "event": RECEIPTLESS_HOLDER_EVENT,
        "action_key": mover, "consumer_action_key": "", "tier_id": tier_id,
        "stage_root": str(stage_root), "reason": "orphan-sweep",
        "complete": False, "errors": [why],
        "host": socket.gethostname(), "unix": time.time(),
    }


def _is_prelaunch_holder(value: str) -> bool:
    """Whether one held key is a prelaunch group holder (#1594)."""

    prefix = getattr(prelaunch_group, "HOLDER_PREFIX", "prelaunch-")
    return str(value).startswith(prefix)




def _held_by_a_live_item(queue: pool.PoolQueue, key: str,
                         owners: Mapping[str, str]) -> bool:
    """Whether a live item holds this key's tokens as its own (#929).

    Two holders a live claim names that are not movers: a live item's own
    reservation -- a producer's output window, say, whose row carries no
    residency and so is not among ``owners`` -- and a window's fence grant,
    held under ``advance-<consumer>-...`` for a consumer that is.  The sweep
    keeps both, as before; this only keeps the reports -- the pressure-driven
    ``stage-receiptless-holder-retained`` and ``stage-holder-unresolved`` --
    from calling them unresolved.
    """

    if key.startswith(window_credit.GRANT_PREFIX):
        prefix = key[len(window_credit.GRANT_PREFIX):].split("-", 1)[0]
        return bool(prefix) and any(owner.startswith(prefix) for owner in owners)
    if not staged_path.is_action_key(key):
        return False
    try:
        return any(not (not pool.record_store.value_or_raise(
            queue.records.retirement_presence_at(queue.records.item_path(state, key)),
        ))
                   for state in (pool.record_store.READY, pool.record_store.CLAIMED))
    except OSError:
        return False


def produced_holder(queue: pool.PoolQueue, tier_id: str,
                    mover: str) -> dict[str, object] | None:
    """The produced-output lane's verdict on one held mover (#929), or ``None``.

    ``None`` means no output funding record on this tier names the key: it is
    not a produced mover here, and the ordinary receipt and fragment rules
    apply to it.  A funded mover is the lane's, receipt or no receipt, and
    the answer is one of three classes:

    * ``live``: its producer attempt holds its claim, or the mover itself is
      ready, claimed or in a transition.  The producer may still retire or
      restage the batch, and a queued mover may still copy into the tokens it
      holds;
    * ``dead``: the producer attempt has ended -- ``dead`` (failed, withdrawn
      or superseded) or ``succeeded`` without retiring this batch -- and so
      has the mover, and its funding is ``consumed``.  Nothing can read the
      copy again: a producer's retry binds a new instance, and with it a new
      batch namespace (#912).  The verdict carries what ``retire_batch``
      needs;
    * ``unknown``: anything else, with the reason.  An absent producer is
      unknown, not dead: no outcome at all is not an ending (#798), and a
      queue whose records are not all visible yet looks the same.

    Every record is read at its own address -- the funding record, the
    instance and template it names, the two keys' queue states -- so the cost
    is a handful of reads per funded holder, and none for any other key.
    """

    if not staged_path.is_action_key(mover):
        return None

    def unknown(why: str) -> dict[str, object]:
        return {"class": "unknown", "why": why}

    try:
        record, file_state = queue.funding.output_evidence(mover, tier_id)
    except (OSError, ValueError, pool.PoolContractError) as exc:
        return unknown(f"its output funding is unreadable: {exc}")
    if file_state == "absent":
        return None
    if file_state != "ok" or not isinstance(record, Mapping):
        return unknown(f"its output funding record is {file_state}")
    owner = str(record.get("owner_action_key") or "")
    nonce = str(record.get("owner_nonce") or "")
    template_id = str(record.get("template_id") or "")
    batch_id = str(record.get("batch_id") or "")
    if (str(record.get("mover_action_key") or "") != mover
            or str(record.get("tier_id") or "") != tier_id
            or not staged_path.is_action_key(owner) or not nonce or not template_id
            or not batch_id):
        return unknown("its output funding record does not name this mover's batch")
    residency = queue.root / pool.record_store.RESIDENCY
    scope = bl.attempt_scope(queue.root, owner, template_id, nonce)
    try:
        instance = oc.validate_instance(
            json.loads(bl.BatchLedger.at_scope(scope).instance_path.read_text()))
    except (OSError, ValueError) as exc:
        return unknown(f"its producer's instance is unreadable: {exc}")
    try:
        template = oc.validate_template(json.loads(
            (residency / oc.OUTPUT_TEMPLATES_SUBDIR
             / f"{template_id}.json").read_text()))
    except (OSError, ValueError) as exc:
        return unknown(f"its producer's template is unreadable: {exc}")
    attempt = instance.get("owner_attempt")
    if (bl.scope_dir(queue.root, instance) != scope
            or not isinstance(attempt, Mapping)
            or str(attempt.get("nonce")) != nonce
            or str(attempt.get("scope_id")) != str(record.get("owner_scope_id"))
            or oc.template_sha256(template)
            != instance.get("template_sha256")
            or str(record.get("template_sha256")) != instance.get("template_sha256")):
        return unknown("its funding, instance and template disagree")
    try:
        producer = bl.producer_attempt_state(queue, instance)
        match RecordStore(queue.root).standing(mover):
            case Present(QueueRow(mover_state, _row)):
                pass
            case Present(Moving()):
                mover_state = "moving"
            case Absent():
                mover_state = "absent"
            case Unknown():
                mover_state = "unknown"
    except (OSError, ValueError, pool.PoolContractError) as exc:
        return unknown(f"a queue state is unreadable: {exc}")
    if producer == "live":
        return {"class": "live",
                "why": f"its producer attempt {owner[:12]} holds its claim"}
    if mover_state in (pool.record_store.READY, pool.record_store.CLAIMED, "moving"):
        return {"class": "live", "why": f"the mover itself is {mover_state}"}
    if producer not in ("dead", "succeeded"):
        return unknown(f"its producer attempt {owner[:12]} is {producer}")
    if mover_state not in (pool.record_store.DONE, pool.record_store.FAILED, pool.record_store.WITHDRAWN):
        return unknown(f"the mover's own queue state is {mover_state}")
    if str(record.get("state")) not in ("consumed", "released"):
        return unknown(f"its output funding is {record.get('state')}, not a settled end")
    try:
        commitments = bl.BatchLedger.at_scope(scope).entries()
        entry = commitments.lookup(batch_id)   # type: ignore[union-attr]
        if not isinstance(entry, bl.BatchFacets):
            return unknown(f"its batch {batch_id} is not committed")
        active = entry.active
    except (OSError, ValueError) as exc:
        return unknown(f"its batch's commitments are unreadable: {exc}")
    if str(active.mover_key or '') != mover:
        return unknown(f"its batch {batch_id}'s active copy is another mover's")
    if active.retired:
        return unknown(f"its batch {batch_id} is retired and it still holds tokens")
    return {"class": "dead", "why": f"its producer attempt {owner[:12]} is {producer}",
            "instance": instance, "template": template, "batch_id": batch_id,
            "producer_action_key": owner, "producer_state": producer}


def _retire_produced_orphan(queue: pool.PoolQueue, mover: str,
                            verdict: Mapping[str, object], *, tier_id: str,
                            stage_root: str, residency_root: Path) -> dict[str, object]:
    """Run the dead producer's ``retire_batch`` for it, and say what happened.

    The lane's own retirement, never the flat orphan path: it validates the
    batch record against its commitments, runs the ordinary ``evict`` on the
    batch's fragment root -- co-owners, claims, pins and handoffs rechecked
    under its locks, files deleted only against their fragment's identity --
    and files the batch ``retired``.  A mover that never published a fragment
    staged nothing the egress can name, so its tokens come back and any
    unfragmented bytes are the reconciliation's, as for the producer's own
    call (``test_a_mover_killed_before_filing_anything_keeps_its_charge``).
    """

    # Only in this process: off the tier host `retire_batch` would publish an
    # egress action, and a sweep publishes nothing.  The tier loop runs on the
    # tier host, so this is the case there.
    if not produced_output._egress_runs_in_process(
            produced_output._announced_tier_record(queue, tier_id)):
        result: dict[str, object] = {
            "ok": False, "refusal": "this sweep is not on the tier host"}
    else:
        result = produced_output.retire_batch(
            queue, verdict["instance"], verdict["template"],  # type: ignore[arg-type]
            str(verdict["batch_id"]), stage_root=str(stage_root),
            residency_root=bl.output_fragment_root(residency_root))
    egress = result.get("receipt") if isinstance(result.get("receipt"), Mapping) else {}
    errors: list[str] = []
    if result.get("ok") is not True:
        errors.append(str(result.get("refusal") or "retirement refused"))
        if isinstance(egress, Mapping):
            errors.extend(str(error) for error in egress.get("errors") or [])
    elif result.get("duplicate"):
        errors.append("its batch was already retired")
    return {
        "schema": pool.POOL_EGRESS_SCHEMA_V1, "event": PRODUCED_ORPHAN_EVENT,
        "action_key": mover,
        "consumer_action_key": str((egress or {}).get("consumer_action_key") or ""),
        "tier_id": tier_id, "stage_root": str(stage_root),
        "reason": "dead-producer", "batch_id": str(verdict["batch_id"]),
        "producer_action_key": str(verdict["producer_action_key"]),
        "producer_state": str(verdict["producer_state"]),
        "entries_deleted": int((egress or {}).get("entries_deleted") or 0),
        "bytes_deleted": int((egress or {}).get("bytes_deleted") or 0),
        "tokens_released": int((egress or {}).get("tokens_released") or 0),
        "complete": not errors, "errors": errors,
        "host": socket.gethostname(), "unix": time.time(),
    }


def _unresolved_reports(queue: pool.PoolQueue, tier_id: str, stage_root: str,
                        unresolved: dict[str, str], *,
                        reported: set[str]) -> list[dict[str, object]]:
    """The operator report: each unresolved holder once per change of reason.

    ``reported`` names holders this pass already reported another way (the
    pressure-driven ``stage-receiptless-holder-retained`` receipt); they are
    recorded but not repeated.  A holder that resolves, or leaves the tier,
    is forgotten, so it is reported again if it comes back.
    """

    seen = _UNRESOLVED_REPORTS.setdefault((str(queue.root), tier_id), {})
    for key in [key for key in seen if key not in unresolved]:
        del seen[key]
    events: list[dict[str, object]] = []
    for key, why in sorted(unresolved.items()):
        if seen.get(key) == why:
            continue
        seen[key] = why
        if key in reported:
            continue
        events.append({
            "schema": pool.POOL_EGRESS_SCHEMA_V1, "event": HOLDER_UNRESOLVED_EVENT,
            "action_key": key, "consumer_action_key": "", "tier_id": tier_id,
            "stage_root": str(stage_root), "reason": "orphan-sweep",
            "complete": False, "errors": [why],
            "host": socket.gethostname(), "unix": time.time(),
        })
    return events


def reset_holder_reports() -> None:
    """Forget which unresolved holders and uncharged owners were reported."""

    _UNRESOLVED_REPORTS.clear()
    _UNCHARGED_REPORTS.clear()




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
    parser.add_argument("--progress-interval-s", type=float, default=None,
                        help="how often an eviction reports the bytes it has "
                             "released, when the launch opened a progress "
                             "channel (#1021); default one pool heartbeat")
    args = parser.parse_args(argv)
    args.action_key = staged_path.own_action_key(args.action_key)

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
        receipt = staged_path.recover_orphaned_range(
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
    # The eviction reports what it has released to the worker's stall check
    # (#1021), on the channel the launch opened when the egress was sealed
    # with a progress policy; without one nothing starts.
    reporter = staged_path.EgressReporter(
        action_key=args.action_key, mover_action_key=args.mover_action_key,
        interval_s=(pool.HEARTBEAT_S if args.progress_interval_s is None
                    else args.progress_interval_s))
    reporter.start()
    token = staged_path.egress_run.set(reporter)
    try:
        receipt = staged_path.evict(queue, args.mover_action_key,
                        consumer_action_key=args.consumer_action_key,
                        stage_root=args.stage_root,
                        residency_root=args.residency_root)
    finally:
        staged_path.egress_run.reset(token)
        reporter.stop()
    receipt["progress_report"] = reporter.record()
    queue.record_move(args.action_key, receipt)
    if args.receipt:
        with open(args.receipt, "w") as stream:
            json.dump(receipt, stream, indent=1, sort_keys=True)
            stream.write("\n")
    print(json.dumps(receipt, indent=1, sort_keys=True, default=str))
    return 0 if receipt["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Promote READY manifest rows onto the tiers, through the one sealing path.

PrismaBuild #1247.  A row that declares a ``pbcampaign.data-manifest`` input
states its whole input set, but only a submitter that passed ``--residency
stage`` ever got a residency plan: the G2 campaign's rows are published
bare, so nothing ever connected their declared manifests to the tier
machinery, and the ARC prewarm loop -- the only thing that looked at them --
targets a cache the RAM-tier policy caps at 22 GiB against 59.6 GiB rows.

This module is the planner the #1247 design adds to the tier role's single
writer.  For the first READY rows in claim order it does exactly what a
``--residency stage`` submitter does, off the row's own sealed request:

* ``pbrun.residency_stage_rows`` seals every movement, promotion and egress
  node this consumer will ever have and freezes the plan (first-writer), and
  ``residency_plan.seal_window`` retires predecessor cancellation markers --
  the same ownership transaction pbrun runs under the consumer's transition
  lock.  No second sealing scheme exists here; this IS the submitter's path,
  invoked by the loop instead of the CLI.
* Publication stays the tiers loop's: it adopts and publishes a filed plan's
  movers exactly as it does a submitter-sealed one, because discovery is by
  the filed plan, not by the consumer row's block.  The consumer's own row is
  never rewritten -- its action key hashes its body, and the map reaches it
  at claim through the launcher's environment (Option 1, #1247 review).
* A row that already has a filed plan stands down: a submitter that seals
  residency itself owns that consumer, and the planner never touches it.
* The bound is rows, not bytes: one row per cycle by default (the streaming
  rule), because the resident-bytes bound is the tier's own window and
  eviction, the same machinery that slides a 4.75 TB joint-pass plan today.
  Measured on the 2026-09-27 READY set: 69 unique G2 rows, 59.6 GiB each,
  zero cross-row sharing, so nothing coarser than a row is worth planning.
* Every outcome is receipted into the row's prewarm record under an additive
  ``tier`` block, so the ARC loop's receipt and the planner's share one file
  and one history.

Fail-closed everywhere: a row whose plan cannot be sealed (no tier mount,
unreadable request, a manifest the plan validator refuses) is recorded with
its reason and the loop moves on; nothing here refuses, delays or reorders a
claim, and a row with no plan runs exactly as it always has.
"""
from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from pathlib import Path
import sys

from prismabuild import core as pb
from prismabuild import movement_actions
from prismabuild import pool
from prismabuild import residency_plan
from prismabuild import storage_tiers

# tools/fleet sibling import, the way tier_loop does its own: the path is
# inserted once and unconditionally (REVIEW-1252 item 8), and the import is
# never conditional on a package context that a caller may or may not give.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import pbrun  # noqa: E402


#: The streaming rule: how many READY rows the planner seals per cycle.  One,
#: because the resident-byte bound is the tier's window and eviction, not the
#: plan -- a plan is the consumer's whole read order, and the loop slides it
#: the way it slides any staged submission's (#906).  A row is ~59.6 GiB on
#: the 2026-09-27 G2 set and the live window is 160 GiB, so two rows may be
#: resident and the third waits for eviction, never for a bigger plan.
MANIFEST_ROWS_PER_CYCLE = 1

#: How many READY rows the planner examines per cycle, in claim order.  A
#: row's manifest declaration lives in its sealed CAS request, so deciding
#: "is this a manifest row" costs one request read; the queue record does
#: not carry it.  The bound keeps a cycle's request reads finite on a fleet
#: whose ready list holds hundreds of rows, and rows beyond it are simply
#: first in line next cycle -- claim order is stable.
MANIFEST_ROWS_EXAMINED_PER_CYCLE = 64

#: What the planner's receipt block says it is, beside the ARC loop's fields.
TIER_RECEIPT_DESTINATION = "ram-tier"


def planner_args(**overrides: object) -> argparse.Namespace:
    """The submitter's own ``--residency`` defaults, spelled once.

    Everything ``pbrun.residency_stage_rows`` reads off ``args`` is here with
    the CLI's defaults: the ram leg ``auto`` (``resolve_ram_tier`` inside the
    sealing path decides from the announced tiers), sharing ``auto`` (#1026,
    so two consumers of one range share its mover), and the mover sizing the
    fleet already runs.  The planner passes no reader declaration: it is not
    the consumer's submitter and will not guess how the row reads.
    """

    args = argparse.Namespace(
        residency_ram="auto",
        residency_share="auto",
        residency_mover_max_attempts=3,
        residency_mover_mem_gb=1,
        residency_mover_readers=4,
        # The publication row's own fields: a movement node's priority is the
        # submitter's ordinary priority and its attempts are the mover
        # policy's, both of which ``publication_row`` reads off the namespace
        # exactly as it reads them off the CLI's.
        priority=-10,
        max_attempts=3,
        retry_safe=True,
    )
    for name, value in overrides.items():
        setattr(args, name, value)
    return args


def row_request(cas_root: Path, action_key: str) -> dict | None:
    """The row's sealed request, or ``None`` when it cannot be read.

    Read by exact CAS path from the action key, never by a directory scan.
    """

    path = Path(cas_root) / "requests" / action_key[:2] / f"{action_key}.json"
    try:
        import json
        with open(path, "rb") as handle:
            body = json.loads(handle.read().decode("utf-8"))
    except (OSError, UnicodeError, ValueError):
        return None
    return body if isinstance(body, dict) else None


def movement_template_of(queue: pool.PoolQueue,
                          request: Mapping[str, object],
                          key: str) -> dict | None:
    """The movement template a manifest row's movers seal off.

    The writer lane's shape (`produced_output._producer_movement_template`):
    the row's task, inputs, closure, environment and execution scope, its own
    sealed checkout addressing, its ``cwd``, and the manifest parameter the
    sealing path stages -- plus the two fleet fields a submission template
    carries and a row request does not: the container-ownership marker root
    under this queue, and the checkout identity the owner digest binds to.
    ``None`` refuses -- an unreadable template is a refusal, never a guess.
    """

    params_source = request.get("params")
    if not isinstance(params_source, Mapping):
        return None
    manifest_param = params_source.get("data_manifest")
    if not isinstance(manifest_param, Mapping) \
            or not isinstance(manifest_param.get("input"), Mapping):
        return None
    inputs = [dict(entry) for entry in request.get("inputs") or ()
              if isinstance(entry, Mapping)]
    params: dict[str, object] = {
        "cwd": str(params_source.get("cwd") or "."),
        "data_manifest": dict(manifest_param),
    }
    snapshot_sha256 = ""
    raw_snapshot = params_source.get("checkout_snapshot")
    if raw_snapshot is None:
        # A mover materializes its consumer's sealed checkout; a row without
        # one cannot name the tree its movers run from, and a guess here
        # would seal nodes no worker can start.
        return None
    try:
        snapshot = pb.validate_pbrun_checkout_snapshot(raw_snapshot)
    except pb.ActionContractError:
        return None
    snapshot_input = snapshot["input"]
    assert isinstance(snapshot_input, Mapping)
    if snapshot_input not in inputs:
        return None
    params["checkout_snapshot"] = snapshot
    snapshot_sha256 = str(snapshot_input["sha256"])
    return {
        "task": dict(request["task"]),
        "inputs": inputs,
        "code_closure": request["code_closure"],
        "environment": request["environment"],
        "execution_scope": request["execution_scope"],
        "params": params,
        "marker_root": Path(queue.root) / pool.CONTAINER_OWNERS,
        "checkout_identity": {"checkout_snapshot": snapshot_sha256},
    }


def read_request(cas_root: Path, action_key: str) -> tuple[dict | None, bool]:
    """``(body, read_ok)`` -- a failed read is not a fact about the bytes.

    REVIEW-1252-r2 item A: a request that could not be read (or is not a
    JSON object) answers ``read_ok=False`` so the caller passes the row over
    WITHOUT remembering anything -- the failure is transient, and the row is
    the planner's again the moment its request reads.  ``no_manifest`` is
    remembered only off a body that was actually read.
    """

    body = row_request(cas_root, action_key)
    return body, isinstance(body, dict)


def is_movement_row(request: Mapping[str, object] | None) -> bool:
    """Whether this sealed request is one of the tier's own movement nodes.

    A mover or egress row carries its consumer's manifest as its own input
    (the mover stages it), so a planner that judged rows by their manifest
    alone would try to plan the tier's own machinery -- sealing a plan for a
    phase-0 egress of somebody else's window.  The script names come from
    :data:`movement_actions.MOVEMENT_SCRIPTS`, their one home (REVIEW-1252-r4).
    """

    if not isinstance(request, Mapping):
        return False
    params = request.get("params")
    command = (params.get("command")
               if isinstance(params, Mapping) else None)
    if not isinstance(command, Sequence) or isinstance(command, (str, bytes)):
        return False
    return any(str(part).endswith(movement_actions.MOVEMENT_SCRIPTS)
               for part in command)


def superseded_consumers(queue: pool.PoolQueue) -> set[str] | None:
    """The consumers with live retirement markers, or ``None`` when unknown.

    One listing of ``residency-plans/superseded/`` per call, not per row
    (REVIEW-1252-r4 [P2]): the live directory holds hundreds of markers and
    the loop runs every few seconds, so the membership question is answered
    from one set.  A filed plan answers before this is ever asked; what the
    set catches is the consumer whose plan was superseded and reaped -- its
    markers are still live, and the owner that withdrew it owns its reseal
    (``handoff_safe`` checks the markers this planner must not retire).  Such
    a row stands down: the planner advises bare rows, never a lifecycle
    another flow is driving.  ``None`` is the fail-closed answer -- a
    directory that cannot be listed is history nobody can see, and every row
    stands down for that cycle.
    """

    first_ready_key = "0" * 64
    directory = (queue.residency_plan_path(first_ready_key).parent
                 / residency_plan.SUPERSEDED)
    try:
        return {marker.name.split(".", 1)[0]
                for marker in pool._scan(directory)}
    except (OSError, ValueError):
        return None


def declares_data_manifest(request: Mapping[str, object] | None) -> bool:
    """Whether a sealed request carries a ``pbcampaign.data-manifest`` input.

    The declaration is the opt-in (#1247 review, Option 1): a row that states
    its input set is a row the tiers may stage for.
    """

    if not isinstance(request, Mapping):
        return False
    for entry in request.get("inputs") or ():
        if isinstance(entry, Mapping) and \
                str(entry.get("id")) == pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID:
            return True
    return False


def promote_ready_manifest_rows(
        queue: pool.PoolQueue, cas_root: Path | None,
        stage_tier: Mapping[str, object],
        *, limit: int = MANIFEST_ROWS_PER_CYCLE,
        ready: Sequence[Mapping[str, object]] | None = None,
        stats: dict[str, int] | None = None,
        ) -> list[dict[str, object]]:
    """Seal residency plans for the first READY manifest rows (claim order).

    Returns one outcome per row -- ``planned``, ``stands_down``,
    ``no_manifest``, ``unreadable``, ``refused``/``deferred`` (with the
    reason) -- and writes the row's prewarm receipt ``tier`` block for the
    fresh decisions.  Never raises for a single row's refusal; the tier
    loop's cycle must survive any one consumer's bad request.

    ``cas_root`` may be ``None``: the planner then reads each row's request
    out of the CAS root the row's own queue record names, which is the truth
    the loop has (a fleet's rows may not all seal into one CAS).  When the
    caller passes a root, every row is read from it, as the tests do.

    Fresh decisions (rows that cost a read, and every planned, refusal and
    deferral) carry ``"fresh": True``; a replay from the memo and a
    stand-down carry ``"fresh": False`` -- the caller emits the fresh ones
    and summarizes the rest (REVIEW-1252-r3 [P2]), because a replayed answer
    is not an event, and one line per remembered row per cycle is churn on
    the shared mount.  ``stats``, when given, is filled with the cycle's
    counts: ``fresh``, ``replayed``, ``stands_down``, ``examined``.
    """

    if ready is None:
        ready = queue.ready_items()
    # REVIEW-1252-r2 nit D: the decisions that matter are the rows that are
    # still READY, so the memo is pruned to exactly them each call -- bounded
    # by the backlog, not by the loop's lifetime.
    live = {(str(queue.root), str(item.get("action_key") or ""))
            for item in ready}
    for table in (_DECISIONS, _RECEIPTS):
        for stale in [key for key in table if key not in live]:
            del table[stale]
    counters = {"fresh": 0, "replayed": 0, "stands_down": 0, "examined": 0}
    history = superseded_consumers(queue)
    outcomes: list[dict[str, object]] = []
    planned = 0
    examined = 0
    for item in list(ready):
        if planned >= max(0, int(limit)):
            break
        if examined >= MANIFEST_ROWS_EXAMINED_PER_CYCLE:
            break
        key = str(item.get("action_key") or "")
        if not key:
            continue
        # A filed plan answers before anything costs a read (REVIEW-1252
        # item 3): one lstat against a CAS request fetch, and a row another
        # submitter sealed residency for is never the planner's.
        if residency_plan.read(queue, key) is not None or \
                (history is None or key in history):
            counters["stands_down"] += 1
            outcomes.append({"action_key": key, "outcome": "stands_down",
                             "fresh": False})
            continue
        memo_key = (str(queue.root), key)
        memo = _DECISIONS.get(memo_key)
        if memo is not None:
            # A remembered answer costs no read, and the examination budget
            # exists to bound reads (REVIEW-1252-r2 item B): it is not spent
            # here, so a hundred remembered rows ahead of a manifest row
            # cannot keep the planner from reaching it.
            counters["replayed"] += 1
            replayed = dict(memo["outcome"])
            replayed["fresh"] = False
            outcomes.append(replayed)
            continue
        examined += 1
        counters["examined"] += 1
        outcome: dict[str, object] = {"action_key": key, "fresh": True}
        # Total containment for this advisory stage (REVIEW-1252 item 1):
        # the planner reads the request file raw -- no validate_action stands
        # between the CAS and it -- so a corrupted-but-JSON body, a hostile
        # shape or a sealing bug anywhere in the submitter's own path is a
        # refusal receipted for THIS row, never an exception out of the tier
        # role's single writer.  A row that gains no plan runs exactly as it
        # does today, which is what makes ``except Exception`` the correct
        # boundary here rather than a smell.
        row_cas = Path(cas_root) if cas_root is not None else Path(
            str(item.get("cas_root") or ""))
        try:
            request, read_ok = read_request(row_cas, key)
            if not read_ok:
                # Item A: an unreadable request passes the row over for this
                # cycle and remembers nothing -- the next cycle reads again.
                # Its own label (r3): the row may have a manifest, and an
                # operator reading the summary must be able to tell a quiet
                # backlog from an unreadable one.
                outcome["outcome"] = "unreadable"
                outcomes.append(outcome)
                continue
            if is_movement_row(request):
                # The tier's own mover/egress rows declare their consumer's
                # manifest by construction; they are machinery, not consumers.
                outcome["outcome"] = "no_manifest"
                outcomes.append(outcome)
                _remember(memo_key, outcome)
                continue
            if not declares_data_manifest(request):
                outcome["outcome"] = "no_manifest"
                outcomes.append(outcome)
                _remember(memo_key, outcome)
                continue
            # The submitter's mover band is the consumer's own (REVIEW-1252
            # item 4): a priority-1 consumer's staging waits behind the -10
            # band otherwise.  Zero is a band, not a missing value (item C).
            raw_priority = item.get("priority")
            args = planner_args(
                priority=-10 if raw_priority is None else int(raw_priority))
            template = movement_template_of(queue, request, key)
            if template is None:
                outcome["outcome"] = "refused"
                outcome["reason"] = ("no movement template off the sealed "
                                     "request (a mover needs the row's "
                                     "sealed checkout snapshot beside its "
                                     "manifest)")
                outcomes.append(outcome)
                _receipt_gated(queue, memo_key, key, status="refused",
                              detail=str(outcome["reason"]))
                _remember(memo_key, outcome)
                continue
            # The submitter's ownership transaction, verbatim (#708 review):
            # seal and file under the consumer's transition lock, so a
            # dead-consumer pass cannot reap a plan between its filing and
            # its adoption.
            cas = pb.PrismaBuildCAS(row_cas)
            with queue._transition_locked(key):
                staged = pbrun.residency_stage_rows(
                    template, consumer_action_key=key,
                    tier=dict(stage_tier), args=args,
                    queue=queue, cas=cas)
                if not staged.get("reused_frozen_plan"):
                    residency_plan.seal_window(
                        queue, staged["plan"], renew=True)
        except SystemExit as exc:
            # pbrun's refusal vocabulary: the sealing path says no with a
            # SystemExit, which ``except Exception`` does not see.  A sealing
            # refusal depends on tier, stage and queue state, not only on the
            # request's bytes, so it is receipted (content-gated) and NEVER
            # remembered (REVIEW-1252-r2 item A).
            outcome["outcome"] = "refused"
            outcome["reason"] = str(exc)
            outcomes.append(outcome)
            _receipt_gated(queue, memo_key, key, status="refused",
                           detail=str(exc))
            continue
        except pool.TransitionLockBusy as exc:
            # Transient, not a refusal (REVIEW-1252 item 6): the consumer's
            # own publication or retirement holds the lock this cycle, and
            # the next cycle is the retry.  Never memoized -- a deferral is
            # a fact about this instant, not about the row.
            outcome["outcome"] = "deferred"
            outcome["reason"] = repr(exc)
            outcomes.append(outcome)
            _receipt_gated(queue, memo_key, key, status="deferred",
                                 detail=repr(exc))
            continue
        except Exception as exc:  # noqa: BLE001 -- see the block comment
            # Item A again: never remembered -- the refusal is a fact about
            # this cycle's tier and queue state, not about the row.
            outcome["outcome"] = "refused"
            outcome["reason"] = repr(exc)
            outcomes.append(outcome)
            _receipt_gated(queue, memo_key, key, status="refused",
                           detail=repr(exc))
            continue
        plan = staged["plan"]
        phases = list(plan.get("phases") or ())
        # The contract ``pbrun`` refuses a staged submission without, checked
        # here and recorded rather than refused (#1332): a refused row would
        # be colder than a staged one.  Without it the consumer's progress
        # cannot say where it is in the read plan, so the window keeps every
        # phase until the row ends -- the whole plan is still staged before
        # the claim, which is what the row start needs.
        contract = residency_plan.progress_contract(
            [str(phase.get("name")) for phase in phases],
            residency_plan.sealed_progress_order(
                {"action_key": key, "cas_root": str(row_cas)}))
        outcome["outcome"] = "planned"
        outcome["phases"] = len(phases)
        outcome["phase_contract"] = contract
        counters["fresh"] += 1
        outcome["tier_id"] = str(plan.get("tier_id") or "")
        outcome["ram_tier_id"] = plan.get("ram_tier_id")
        outcomes.append(outcome)
        _record_tier_receipt(
            queue, key, status="planned",
            tier_id=str(plan.get("tier_id") or ""),
            ram_tier_id=(str(plan["ram_tier_id"])
                         if plan.get("ram_tier_id") else None),
            phases=len(phases),
            manifest_sha256=str(plan.get("manifest_sha256") or ""),
            manifest_bytes=int(plan.get("manifest_bytes") or 0),
            phase_contract=contract)
        planned += 1
    counters["fresh"] += sum(
        1 for outcome in outcomes
        if outcome.get("fresh") and outcome.get("outcome") != "planned")
    if stats is not None:
        stats.update(counters)
    return outcomes



#: The last receipt block written per row, content-gated (r3 [P3]): a
#: persistent refusal must not rewrite the row's prewarm record every cycle.
#: Separate from ``_DECISIONS`` because a refusal is deliberately NOT a
#: remembered decision, and pruned with the same live set.
_RECEIPTS: dict[tuple[str, str], dict[str, object]] = {}


def _receipt_gated(queue: pool.PoolQueue, memo_key: tuple[str, str],
                   action_key: str, **block: object) -> None:
    """Write a receipt only when its content changes (REVIEW-1252-r3 [P3])."""

    if _RECEIPTS.get(memo_key) == dict(block):
        return
    _RECEIPTS[memo_key] = dict(block)
    _record_tier_receipt(queue, action_key, **block)


#: Terminal per-row decisions, remembered for the life of the loop
#: (REVIEW-1252 item 3): a CAS request is content-addressed and immutable,
#: so ``no_manifest`` and a template refusal are facts about the row, not
#: about this cycle.  Keyed by queue root beside the action key so a test
#: fleet's rows never answer for one another.  Deferrals are never stored --
#: they are facts about an instant.
_DECISIONS: dict[tuple[str, str], dict[str, object]] = {}


def _remember(memo_key: tuple[str, str], outcome: dict[str, object]) -> None:
    """Store one terminal decision, merging into any receipt state."""

    _DECISIONS.setdefault(memo_key, {})["outcome"] = dict(outcome)


def _record_tier_receipt(queue: pool.PoolQueue, action_key: str, *,
                         status: str, tier_id: str = "",
                         ram_tier_id: str | None = None,
                         phases: int = 0, manifest_sha256: str = "",
                         manifest_bytes: int = 0,
                         detail: str = "",
                         phase_contract: str = "",
                         extra: Mapping[str, object] | None = None) -> None:
    """The additive ``tier`` block in the row's prewarm record (#1247).

    Same file the ARC loop receipts into, same schema (``pool_prewarm.v1``):
    the block is a new field old readers ignore, and a reader that wants the
    tier's answer reads ``tier.status`` -- ``planned`` when the plan is filed,
    ``refused`` with the reason when it is not, and then (#1332, written by
    the tier loop through :func:`record_landing`) ``landed`` once the
    consumer's admission verdict reads ``resident``, or
    ``claimed_before_landing`` when it was claimed first.
    """

    block: dict[str, object] = {
        "destination": TIER_RECEIPT_DESTINATION,
        "status": status,
        "phases": int(phases),
    }
    if phase_contract:
        block["phase_contract"] = phase_contract
    if extra:
        block.update(extra)
    if tier_id:
        block["tier_id"] = tier_id
    if ram_tier_id:
        block["ram_tier_id"] = ram_tier_id
    if manifest_sha256:
        block["manifest_sha256"] = manifest_sha256
    if manifest_bytes:
        block["manifest_bytes"] = int(manifest_bytes)
    if detail:
        block["detail"] = detail[:4096]
    try:
        queue.record_prewarm(action_key, {"tier": block})
    except (OSError, pool.PoolContractError):
        # The receipt is observability, never a gate: a row whose receipt
        # cannot be written keeps its plan and its ordinary claim.
        pass


def record_landing(queue: pool.PoolQueue, consumer: Mapping[str, object], *,
                   now: float) -> dict[str, object] | None:
    """Say, once, whether a planner row's first phase landed before its claim.

    #1332.  ``planned`` alone cannot certify the thing the planner exists
    for: a receipt that never changes reads the same whether the bytes
    arrived ahead of the row or never did (the 2026-09-29 rows read
    ``planned`` for the life of the row while nothing was staged).  So the
    tier loop asks this after it composes a planner consumer's map:

    * ``landed`` -- the admission verdict a sealed consumer is claimed on,
      asked with the block the filed plan implies, reads ``resident``: every
      lead executed, pinned and named by the composed map.  ``consumer_state``
      says whether that was seen while the row was still ready.
    * ``claimed_before_landing`` -- the row was claimed while its receipt
      still read ``planned`` and its verdict does not read ``resident``: the
      row start read its first phase off the pool.

    Written once: any receipt that no longer reads ``planned`` is left
    alone, so the steady cost is one small read per planner consumer per
    cycle.  The rest of the record is carried over, not replaced.  Returns
    the event to emit, or ``None``.
    """

    key = str(consumer.get("action_key") or "")
    item = consumer.get("item")
    residency = consumer.get("residency")
    if (not key or consumer.get("residency_source") != "filed_plan"
            or not isinstance(item, Mapping)
            or not isinstance(residency, Mapping)):
        return None
    try:
        record = queue.prewarm(key)
    except (OSError, ValueError, pool.PoolContractError):
        return None
    tier = record.get("tier") if isinstance(record, Mapping) else None
    if not isinstance(tier, Mapping) or tier.get("status") != "planned":
        return None
    try:
        verdict = queue.residency_verdict({**dict(item),
                                           "residency": dict(residency)})
    except (OSError, pool.PoolContractError):
        return None
    state = str(consumer.get("state") or "")
    if verdict.get("state") == "resident":
        status = "landed"
    elif state == pool.CLAIMED:
        status = "claimed_before_landing"
    else:
        return None
    block: dict[str, object] = {
        **dict(tier), "status": status, "observed_unix": float(now),
        "consumer_state": state, "verdict": str(verdict.get("state")),
    }
    if consumer.get("claimed_unix") is not None:
        block["claimed_unix"] = consumer.get("claimed_unix")
    try:
        queue.record_prewarm(key, {**dict(record), "tier": block})
    except (OSError, pool.PoolContractError):
        return None
    return {"event": f"manifest-row-{status.replace('_', '-')}",
            "consumer": key, "consumer_state": state,
            "verdict": str(verdict.get("state"))}

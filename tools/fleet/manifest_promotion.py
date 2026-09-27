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
from prismabuild import pool
from prismabuild import residency_plan
from prismabuild import storage_tiers

if __package__ in (None, ""):  # tools/fleet sibling import, as tier_loop does it
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
    if not snapshot_sha256:
        # The ownership namespace keeps the historical digest over the first
        # inherited input, else the consumer's own key.
        snapshot_sha256 = next(
            (str(entry.get("sha256")) for entry in inputs
             if isinstance(entry.get("sha256"), str)), key)
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
        queue: pool.PoolQueue, cas_root: Path, stage_tier: Mapping[str, object],
        *, limit: int = MANIFEST_ROWS_PER_CYCLE,
        ready: Sequence[Mapping[str, object]] | None = None,
        ) -> list[dict[str, object]]:
    """Seal residency plans for the first READY manifest rows (claim order).

    Returns one outcome per row examined -- ``planned``, ``stands_down``,
    ``no_manifest``, ``refused`` (with the reason) -- and writes the row's
    prewarm receipt ``tier`` block for each.  Never raises for a single row's
    refusal; the tier loop's cycle must survive any one consumer's bad
    request.
    """

    if ready is None:
        ready = queue.ready_items()
    cas = pb.PrismaBuildCAS(Path(cas_root))
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
        examined += 1
        request = row_request(Path(cas_root), key)
        outcome: dict[str, object] = {"action_key": key}
        if not declares_data_manifest(request):
            outcome["outcome"] = "no_manifest"
            outcomes.append(outcome)
            continue
        if residency_plan.read(queue, key) is not None:
            outcome["outcome"] = "stands_down"
            outcomes.append(outcome)
            continue
        template = movement_template_of(queue, request, key)
        if template is None:
            outcome["outcome"] = "refused"
            outcome["reason"] = ("no movement template off the sealed request "
                                 "(a mover needs the row's sealed checkout "
                                 "snapshot beside its manifest)")
            outcomes.append(outcome)
            _record_tier_receipt(queue, key, status="refused",
                                 detail="no movement template")
            continue
        try:
            # The submitter's ownership transaction, verbatim (#708 review):
            # seal and file under the consumer's transition lock, so a
            # dead-consumer pass cannot reap a plan between its filing and
            # its adoption.
            with queue._transition_locked(key):
                staged = pbrun.residency_stage_rows(
                    template, consumer_action_key=key,
                    tier=dict(stage_tier), args=planner_args(),
                    queue=queue, cas=cas)
                if not staged.get("reused_frozen_plan"):
                    residency_plan.seal_window(
                        queue, staged["plan"], renew=True)
        except SystemExit as exc:
            outcome["outcome"] = "refused"
            outcome["reason"] = str(exc)
            outcomes.append(outcome)
            _record_tier_receipt(queue, key, status="refused",
                                 detail=str(exc))
            continue
        except (residency_plan.ResidencyPlanError, OSError,
                pool.PoolContractError, ValueError) as exc:
            outcome["outcome"] = "refused"
            outcome["reason"] = repr(exc)
            outcomes.append(outcome)
            _record_tier_receipt(queue, key, status="refused",
                                 detail=repr(exc))
            continue
        plan = staged["plan"]
        phases = list(plan.get("phases") or ())
        outcome["outcome"] = "planned"
        outcome["phases"] = len(phases)
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
            manifest_bytes=int(plan.get("manifest_bytes") or 0))
        planned += 1
    return outcomes


def _record_tier_receipt(queue: pool.PoolQueue, action_key: str, *,
                         status: str, tier_id: str = "",
                         ram_tier_id: str | None = None,
                         phases: int = 0, manifest_sha256: str = "",
                         manifest_bytes: int = 0,
                         detail: str = "") -> None:
    """The additive ``tier`` block in the row's prewarm record (#1247).

    Same file the ARC loop receipts into, same schema (``pool_prewarm.v1``):
    the block is a new field old readers ignore, and a reader that wants the
    tier's answer reads ``tier.status`` -- ``planned`` when the plan is filed
    (``complete`` for the bytes is the composed map's, which the movers write
    as they land), ``refused`` with the reason when it is not.
    """

    block: dict[str, object] = {
        "destination": TIER_RECEIPT_DESTINATION,
        "status": status,
        "phases": int(phases),
    }
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

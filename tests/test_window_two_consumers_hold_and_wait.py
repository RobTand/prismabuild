"""Two consumers reach a hold-and-wait state through real production transitions.

PRG-04 state-transition fixture (PB main e91a55d; NEW file only, no production
edits).  Unlike the audit fixture (ready-rows-exceed-free arithmetic), this
drives the queue from empty through the transitions workers and the coordinator
actually use, then proves the resulting state admits no useful advance:

* acquisition: ``PoolQueue.claim()`` (full claim path: placement, residency
  verdict, host + tier admission, rename arbiter);
* mover completion: the mover's own ``record_move`` receipt, its
  ``residency_map`` fragment, and the production ``finish()`` terminal
  transition (complete receipt + ``executed`` keeps the tier pin: held tokens
  equal bytes on the stage);
* coordinator: ``tier_loop.residency_window`` / ``window_pressure`` /
  ``compose_map``; reclamation: ``stage_release.sweep``;
* gates: the tier ledger's ``begin_acquire`` (the primitive claim uses) and
  ``residency_verdict`` for the bytes the read frontier needs next.

Sequence (capacity 5 GiB, two consumers, 4 phases x 2 GiB each; sizes are test
parameters, never fleet defaults):

1. Empty queue. Freeze plans A/B, publish consumers A/B (ready).
2. ``residency_window`` publishes m0A, m1A, m0B, m1B (ready reserves nothing).
3. Claims in the reachable order m0A, m0B (forced via the worker loop's own
   ``ready=`` snapshot parameter; production scan order varies, so this
   interleaving is reachable): free falls 5 -> 3 -> 1. m1A/m1B stay queued:
   the next claim attempt returns None (movers tier-denied, consumers
   lead-waiting -- all real denials).
4. m0A/m0B finish complete + ``executed``: done records filed, 2 GiB pins each
   retained, free stays 1. Fragments filed, maps composed.
5. Consumers A/B claim (leads resident) and read their staged phase-0.

Stuck state (all asserted through the real gates):

* ``residency_window`` publishes no mover and no egress for either consumer,
  and files ``window-stalled`` for both: nothing publishable.
* Ledger free is 1 GiB; ``begin_acquire`` for either 2 GiB next phase returns
  None: nothing claimable.
* ``window_pressure`` reports 2 GiB (one phase) yet ``sweep`` under that
  pressure returns []: nothing reclaimable -- every held key is wanted by a
  live plan or a live lead.
* ``residency_verdict`` for either consumer's phase-1 lead answers
  ``lead_not_resident``: the read frontier cannot move there.
* Both consumers are claimed (their admission -- the last free useful advance
  -- is consumed) and both next movers sit queued.

The closed loop: freeing room needs an egress for m0A/m0B; egress publishes
only for phases *before* the accepted frontier (both frontiers are still at
the beginning: neither consumer has durably consumed anything past phase-0);
the frontier advances only by durably consuming phase-1 bytes (progress units
are cumulative durable work, never a promise); phase-1 bytes need m1 staged;
m1 needs 2 GiB free against 1. No single consumer is at fault -- either fits
alone (2 held + 2 next + one-phase room <= 5) -- and no coordinator, claim, or
sweep transition relieves it. Admission order decides which interleaving runs:
m0A-then-m1A claims serialize through the same free and one consumer advances,
while m0A-then-m0B claims wedge both.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, residency_map, residency_plan, storage_tiers  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "7" * 64
GIB = storage_tiers.GIB
PHASE_GIB = 2
CAPACITY_GIB = 5

CONSUMER_A = "a" * 64
CONSUMER_B = "b" * 64


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue, consumer: str, *, tag: str) -> dict[str, object]:
    built = []
    for ordinal in range(4):
        start = ordinal * PHASE_GIB * GIB
        end = start + PHASE_GIB * GIB
        built.append({
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end,
            "stage_gib": PHASE_GIB,
            "mover_row": {
                **_row(_hexkey(f"mover-{tag}-{ordinal}"),
                       {STAGE_KIND: PHASE_GIB, "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(_hexkey(f"egress-{tag}-{ordinal}"),
                               {"mem_gb": 1}, queue),
        })
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=1 << 30, phases=built)


def _mover_key(plan: dict[str, object], ordinal: int) -> str:
    phases = plan["phases"]
    assert isinstance(phases, list)
    return str(phases[ordinal]["mover_row"]["action_key"])  # type: ignore[index]


def _ordered_ready(queue: pool.PoolQueue, *keys: str) -> list[dict[str, object]]:
    # Keys already claimed are gone from ready/: skip them rather than fail --
    # the order selects among what is still queued, which is all a worker's
    # prefetched snapshot ever does.
    items = {str(item["action_key"]): item for item in queue.ready_items()}
    return [items[key] for key in keys if key in items]


def _fragment(queue: pool.PoolQueue, consumer: str, mover: str, *,
              path: str, stage: Path) -> None:
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "entries": {residency_map.residency_map_key(path, 0): {
            "stage_path": str(stage / Path(path).name), "bytes": 4096,
            "offset": 0, "sha256": "a" * 64}}})


def test_two_consumers_make_progress_through_the_gate(tmp_path: Path) -> None:
    """Corrected policy (#745): the wedge cannot form; both windows complete.

    The hazard this fixture used to reproduce -- two currents landing in
    room one advance needed -- is unreachable through the gated window: the
    first cycle admits exactly one consumer and gates the other
    ``joint-fit-stall`` with no mover published for it, so no joint hold
    can form.  The admitted window completes, its egress frees the room,
    and the gated window is admitted and completes, all through the same
    production transitions the hazard proof used.
    """
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.ledger().ensure_capacity({"cpu": 4, "mem_gb": 8})
    queue.mint_tier_capacity(TIER, {"stage_gib": CAPACITY_GIB})
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=str(stage)) == "registered"
    tiers = {TIER: {"tier_id": TIER, "tier": "stage",
                    "mountpoint": str(stage)}}

    plan_a = _plan(queue, CONSUMER_A, tag="aa")
    plan_b = _plan(queue, CONSUMER_B, tag="bb")
    for plan, consumer in ((plan_a, CONSUMER_A), (plan_b, CONSUMER_B)):
        residency_plan.freeze(queue, plan)
        queue.publish(
            action_key=consumer, cas_root=queue.root / "cas",
            checkout_root=queue.root / "co",
            worker_script=queue.root / "worker.py",
            resources={"cpu": 1, "mem_gb": 1},
            residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                       "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                       "leads": residency_plan.leads_for(plan)})
    m0a, m1a = _mover_key(plan_a, 0), _mover_key(plan_a, 1)
    m0b, m1b = _mover_key(plan_b, 0), _mover_key(plan_b, 1)

    # Corrected policy (#745): the coordinator admits one window, not both.
    # The gate covers each newcomer's current-plus-protected-next minimum,
    # so the second current cannot land in the room the first advance was
    # promised.  The winner is queue-scan order; the test reads it off the
    # events rather than hard-coding it.
    events = tier_loop.residency_window(queue, tiers=tiers)
    published = {e["action_key"] for e in events
                 if e.get("event") == "mover-published"}
    assert published == {m0a, m1a} or published == {m0b, m1b}
    first_consumer = CONSUMER_A if m0a in published else CONSUMER_B
    gated = [e for e in events if e.get("event") == "window-gated"]
    assert len(gated) == 1
    gated_consumer = CONSUMER_B if first_consumer == CONSUMER_A else CONSUMER_A
    assert gated[0]["consumer"] == gated_consumer
    # Both gates refuse, and since #907 the commitment names it: no
    # eviction admits the second window while the first is running.
    assert gated[0]["reason"] == "joint-commitment-stall"
    assert gated[0]["permanent"] is False
    # The wedge interleaving is unreachable through the policy: the gated
    # consumer has no published mover, so no joint hold can form.
    loser_m0 = m0b if first_consumer == CONSUMER_A else m0a
    assert not queue.item_path(pool.READY, loser_m0).exists()
    P, Q = first_consumer, gated_consumer
    p0, p1 = (m0a, m1a) if P == CONSUMER_A else (m0b, m1b)
    q0, q1 = (m0b, m1b) if P == CONSUMER_A else (m0a, m1a)

    # The admitted window runs to completion; the room it frees admits Q.
    for mover, name in ((p0, "/pool/p0.bin"), (p1, "/pool/p1.bin")):
        got = queue.claim(tags=["dl380g10"], owner="w-hw",
                          ready=_ordered_ready(queue, mover))
        assert got is not None and got["action_key"] == mover
        queue.record_move(mover, {
            "consumer_action_key": P, "tier_id": TIER,
            "stage_root": str(stage), "complete": True,
            "bytes_staged": PHASE_GIB * GIB})
        _fragment(queue, P, mover, path=name, stage=stage)
        queue.finish(mover, status="executed")
    assert tier_loop.compose_map(queue, P) is not None
    got = queue.claim(tags=["dl380g10"], owner="w-hw",
                      ready=_ordered_ready(queue, P))
    assert got is not None and got["action_key"] == P
    queue.finish(P, status="executed")
    for mover in (p0, p1):
        stage_release.evict(queue, mover, consumer_action_key=P,
                            stage_root=str(stage))

    admitted = False
    for _ in range(4):
        events = tier_loop.residency_window(queue, tiers=tiers)
        if q0 in {e["action_key"] for e in events
                  if e.get("event") == "mover-published"}:
            admitted = True
            break
    assert admitted, "gated window never admitted after egress"
    for mover, name in ((q0, "/pool/q0.bin"), (q1, "/pool/q1.bin")):
        got = queue.claim(tags=["dl380g10"], owner="w-hw",
                          ready=_ordered_ready(queue, mover))
        assert got is not None and got["action_key"] == mover
        queue.record_move(mover, {
            "consumer_action_key": Q, "tier_id": TIER,
            "stage_root": str(stage), "complete": True,
            "bytes_staged": PHASE_GIB * GIB})
        _fragment(queue, Q, mover, path=name, stage=stage)
        queue.finish(mover, status="executed")
    assert tier_loop.compose_map(queue, Q) is not None
    got = queue.claim(tags=["dl380g10"], owner="w-hw",
                      ready=_ordered_ready(queue, Q))
    assert got is not None and got["action_key"] == Q
    queue.finish(Q, status="executed")
    for mover in (q0, q1):
        stage_release.evict(queue, mover, consumer_action_key=Q,
                            stage_root=str(stage))
    assert queue.tier_ledger(TIER).available().get("stage_gib") == CAPACITY_GIB

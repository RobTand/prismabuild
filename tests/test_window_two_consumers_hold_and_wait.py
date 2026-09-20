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


def test_two_consumers_reach_hold_and_wait(tmp_path: Path) -> None:
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

    # The coordinator stages windows, not admits them: four movers queued.
    events = tier_loop.residency_window(queue, tiers=tiers)
    assert {e["action_key"] for e in events
            if e.get("event") == "mover-published"} == {m0a, m1a, m0b, m1b}

    # Reachable claim order: one current window per consumer lands first.
    got = queue.claim(tags=["dl380g10"], owner="w-hw",
                      ready=_ordered_ready(queue, m0a, m0b, m1a, m1b,
                                           CONSUMER_A, CONSUMER_B))
    assert got is not None and got["action_key"] == m0a
    got = queue.claim(tags=["dl380g10"], owner="w-hw",
                      ready=_ordered_ready(queue, m0b, m0a, m1a, m1b,
                                           CONSUMER_A, CONSUMER_B))
    assert got is not None and got["action_key"] == m0b
    # m1A/m1B need 2 GiB against 1 free; both consumers still lead-wait.
    assert queue.claim(tags=["dl380g10"], owner="w-hw") is None

    # Both copies land complete; finish keeps each 2 GiB pin.
    for mover, consumer, name in ((m0a, CONSUMER_A, "/pool/a0.bin"),
                                  (m0b, CONSUMER_B, "/pool/b0.bin")):
        queue.record_move(mover, {
            "consumer_action_key": consumer, "tier_id": TIER,
            "stage_root": str(stage), "complete": True,
            "bytes_staged": PHASE_GIB * GIB})
        _fragment(queue, consumer, mover, path=name, stage=stage)
        queue.finish(mover, status="executed")
        assert queue.item_path(pool.DONE, mover).exists()
        assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 2}
    assert queue.tier_ledger(TIER).available().get("stage_gib") == 1
    assert tier_loop.compose_map(queue, CONSUMER_A) is not None
    assert tier_loop.compose_map(queue, CONSUMER_B) is not None

    # Positive control: both consumers are admittable on their staged leads.
    got = queue.claim(tags=["dl380g10"], owner="w-hw",
                      ready=_ordered_ready(queue, CONSUMER_A, CONSUMER_B,
                                           m1a, m1b))
    assert got is not None and got["action_key"] == CONSUMER_A
    got = queue.claim(tags=["dl380g10"], owner="w-hw",
                      ready=_ordered_ready(queue, CONSUMER_B, CONSUMER_A,
                                           m1a, m1b))
    assert got is not None and got["action_key"] == CONSUMER_B

    # --- the stuck state: every gate answers through production code ---
    events = tier_loop.residency_window(queue, tiers=tiers)
    kinds = {str(e.get("event")) for e in events}
    assert "mover-published" not in kinds
    assert "egress-published" not in kinds
    stalled = {str(e.get("consumer")) for e in events
               if e.get("event") == "window-stalled"}
    assert stalled == {CONSUMER_A, CONSUMER_B}

    ledger = queue.tier_ledger(TIER)
    assert ledger.available().get("stage_gib") == 1
    assert ledger.begin_acquire("probe-hw-next", {"stage_gib": 2}) is None
    assert queue.item_path(pool.READY, m1a).exists()
    assert queue.item_path(pool.READY, m1b).exists()

    pressure = tier_loop.window_pressure(queue, tiers=tiers)
    assert pressure.get(TIER) == 2
    assert stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure=pressure) == []

    for consumer, lead in ((CONSUMER_A, m1a), (CONSUMER_B, m1b)):
        verdict = queue.residency_verdict({
            "action_key": consumer,
            "residency": {"schema": pool.RESIDENCY_SCHEMA_V1,
                          "tier_id": TIER, "manifest_sha256": MANIFEST,
                          "manifest_bytes": 1 << 30, "leads": [lead]}})
        assert verdict["state"] == "lead_not_resident"
        assert ledger.holder_tokens(lead) == {}
        assert queue.move_record(lead) is None

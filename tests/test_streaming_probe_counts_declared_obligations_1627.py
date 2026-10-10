"""The streaming probe counts admitted declared obligations (#1627).

The joint-fit gate refuses a streaming newcomer beside an admitted
declared unit's whole peak obligation, but the orphan-pressure probe
asked without it. The probe then frees too little, files no-shortfall,
and the window stalls beside reclaimable room.

Tier of 19: an admitted declared unit holds 10 of a peak of 12
(obligation 2), four orphans hold 8, and a 2 GiB streaming newcomer
waits ahead of the declared unit. The gate needs 18 + 2 + 2 = 22.
The probe without the obligation sees 18 + 2 = 20, asks 2, frees one
orphan, then files no-shortfall while the gate still needs 20. The
window never publishes. With the obligation the probe asks 4, the
sweep frees two orphans, and the window publishes its lead.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
import prelaunch_tier as pt  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    _stage_range, _tier_record)
from test_prelaunch_group_reconcile_1594 import (  # noqa: E402
    _hexkey, _queue, _row)
from test_prelaunch_tier_gate_1594 import _live  # noqa: E402
from test_prelaunch_tier_module_1594 import (  # noqa: E402
    _consumer, _declared_plan)

STAGE = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{STAGE}"
GIB = storage_tiers.GIB
CAPACITY = 19

DONE = _hexkey("ob-done-consumer")
DECLARED = _hexkey("ob-declared")
WINDOW = _hexkey("ob-window")
MANIFEST = _hexkey("ob-manifest")


def _window_plan(queue, consumer: str) -> dict:
    """One 2 GiB streaming phase, nothing staged."""
    row = _row(_hexkey("ob-wm"), {"cpu": 1, "mem_gb": 1, STAGE_KIND: 2},
               queue)
    phases = [{
        "name": "phase-0", "start_bytes": 0, "end_bytes": 2 * GIB,
        "stage_gib": 2,
        "mover_row": {
            **row, "residency": {
                "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE,
                "manifest_sha256": MANIFEST, "manifest_bytes": 2 * GIB,
                "range_start_bytes": 0, "range_end_bytes": 2 * GIB}},
        "egress_row": _row(_hexkey("ob-we"), {"mem_gb": 1}, queue),
    }]
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=STAGE,
        stage_root="/stage/prewarm", manifest_sha256=MANIFEST,
        manifest_bytes=2 * GIB, phases=phases)


def _fixture(tmp_path: Path):
    """An admitted prefix, four orphans, and a streaming waiter."""
    queue = _queue(tmp_path, stage_gib=CAPACITY)
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=STAGE, stage_root=stage)
    queue.publish(action_key=DONE, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root / "co"),
                  worker_script=str(queue.root / "worker.py"),
                  resources={"cpu": 1, "mem_gb": 1})
    queue.finish(DONE, status="executed")
    orphans = []
    for ordinal in range(4):
        mover = _hexkey(f"ob-orphan-{ordinal}")
        _stage_range(queue, mover=mover, consumer=DONE, stage=stage,
                     ordinal=ordinal, manifest="e" * 64)
        orphans.append(mover)
    plan = _declared_plan(queue, DECLARED,
                          [("a0", 10, True, 1), ("a1", 2, False, 1)],
                          tag="ob")
    _live(queue, plan, DECLARED)
    units = pt.declared_units(queue, {STAGE: {"tier": "stage"}},
                              [_consumer(DECLARED)])
    assert len(units) == 1
    events: list[dict] = []
    for _ in range(3):
        step, _authority = pt.reserve_pass(queue, STAGE, units,
                                           admitted=lambda unit: True)
        events.extend(step)
    ledger = queue.tier_ledger(STAGE)
    assert int(ledger.holder_tokens(units[0].holder).get("stage_gib", 0)) == 10, events
    window_plan = _window_plan(queue, WINDOW)
    residency_plan.freeze(queue, window_plan)
    queue.publish(
        action_key=WINDOW, cas_root=str(queue.root / "cas"),
        checkout_root=str(queue.root / "co"),
        worker_script=str(queue.root / "worker.py"),
        resources={"cpu": 1, "mem_gb": 1}, priority=1,
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE,
                   "manifest_sha256": MANIFEST, "manifest_bytes": 2 * GIB,
                   "leads": residency_plan.leads_for(window_plan)})
    return queue, stage, orphans, units[0].holder


def _tiers(stage: Path) -> dict:
    """The announced stage tier."""
    return {STAGE: _tier_record(stage, gib=CAPACITY)}


def _cycle(queue, stage: Path) -> None:
    """One whole tier cycle, as the storage box runs it."""
    tiers = _tiers(stage)
    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(),
                    discover=lambda **_kwargs: dict(tiers))


def test_the_probe_asks_for_the_obligation_it_gates_on(tmp_path: Path) -> None:
    """Pressure covers held plus the admitted obligation plus the lead."""
    queue, stage, _, _ = _fixture(tmp_path)
    skipped: list[dict] = []
    pressure = tier_loop.window_pressure(queue, tiers=_tiers(stage),
                                         skipped=skipped)
    assert pressure.get(STAGE) == 4, (pressure, skipped)


def test_the_window_publishes_once_the_orphans_go(tmp_path: Path) -> None:
    """The sweep frees two orphans, and the lead is published."""
    queue, stage, orphans, holder = _fixture(tmp_path)
    for _ in range(3):
        _cycle(queue, stage)
    assert queue.item_path(pool.READY, _hexkey("ob-wm")).exists()
    assert not queue.tier_ledger(STAGE).holder_tokens(orphans[0])
    assert not queue.tier_ledger(STAGE).holder_tokens(orphans[1])
    assert queue.tier_ledger(STAGE).holder_tokens(holder).get(
        "stage_gib") == 10

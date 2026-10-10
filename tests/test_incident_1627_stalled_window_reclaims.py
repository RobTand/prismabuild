"""The #1627 stall: a small window waits beside stranded stage and RAM holders.

On 2026-10-08 a 1 GiB window stalled 3,948 s beside 108 GiB of stage
orphans and 134 GiB of RAM tokens held by ended consumers. The loop
emitted no eviction for 75 minutes. A manual release cleared it in
one minute. This test rebuilds that shape on a tmp queue and drives
the real tier cycle:

* stage orphans of a done and a withdrawn consumer;
* a withdrawn prelaunch group's reserved prefix;
* a RAM tier full of done and failed holders;
* a live newcomer window and a live declared waiter.

Within a few cycles the loop must ask pressure on both tiers and
reclaim the orphans, keep a live claimed range, reserve the declared
prefix, and file one evidence line for each waiter it cannot ask for.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import prelaunch_tier as pt  # noqa: E402
import residency_publication as rp  # noqa: E402
import stage_release  # noqa: E402
from prismabuild import pool, prelaunch_group, residency_plan, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    _claim_with_progress, _stage_range, _tier_record)
from test_prelaunch_group_reconcile_1594 import (  # noqa: E402
    _hexkey, _queue, _row)
from test_prelaunch_tier_gate_1594 import _live  # noqa: E402
from test_prelaunch_tier_module_1594 import (  # noqa: E402
    _consumer, _declared_plan)

STAGE = "prismabuild-stage:dl380g10"
RAM = "ram:dl380g10"
STAGE_KIND = f"stage_gib@{STAGE}"
RAM_KIND = f"ram_gib@{RAM}"
GIB = storage_tiers.GIB
EPOCH = "1695052800-1a2b3c4d5e6f7a8b"

STAGE_CAP = 20
RAM_CAP = 16

DONE = _hexkey("1627-done-consumer")
GONE = _hexkey("1627-withdrawn-consumer")
PG = _hexkey("1627-withdrawn-group")
RAM_DONE = _hexkey("1627-ram-done")
RAM_FAILED = _hexkey("1627-ram-failed")
WINDOW = _hexkey("1627-window")
DECLARED = _hexkey("1627-declared")
LIVE = _hexkey("1627-live-claimed")


def _publish_bare(queue, key, resources) -> None:
    """A ready row with no residency: queued demand, never a consumer."""
    queue.publish(action_key=key, cas_root=str(queue.root / "cas"),
                 checkout_root=str(queue.root / "co"),
                 worker_script=str(queue.root / "worker.py"),
                 tags=["dl380g10"], resources=dict(resources))


def _queued_demand(queue, seed: str, tier_id: str, kind: str, gib: int) -> None:
    """A ready mover row holding no tokens: queued demand the gate counts."""
    manifest = _hexkey(f"1627-man-{seed}")
    queue.publish(action_key=_hexkey(seed),
                  cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root / "co"),
                  worker_script=str(queue.root / "worker.py"),
                  tags=["dl380g10"],
                  resources={"cpu": 1, "mem_gb": 1, kind: gib},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "tier_id": tier_id, "manifest_sha256": manifest,
                             "manifest_bytes": gib * GIB,
                             "range_start_bytes": 0,
                             "range_end_bytes": gib * GIB})


def _end(queue, key, *, how) -> None:
    """Publish one consumer row and end it the way the incident ended its."""
    _publish_bare(queue, key, {"cpu": 1, "mem_gb": 1})
    if how == "done":
        queue.finish(key, status="executed")
    elif how == "failed":
        queue.finish(key, status="failed", detail={"returncode": 1})
    else:
        queue.withdraw(key, reason="1627 fixture", by="test")


def _ram_orphan(queue, ram: Path, consumer: str, seed: str, ordinal: int) -> str:
    mover = _hexkey(f"1627-ram-{seed}-{ordinal}")
    assert queue.tier_ledger(RAM).acquire(mover, {"ram_gib": 2})
    assert queue.ledger("dl380g10").acquire(
        f"ram-host:{mover}", {"mem_gb": 2})
    rp.vouch_landed(
        queue, consumer_action_key=consumer, mover_action_key=mover,
        tier_id=RAM, stage_root=ram, manifest_sha256=_hexkey(f"man-{seed}"),
        range_start_bytes=ordinal * 2 * GIB,
        range_end_bytes=(ordinal + 1) * 2 * GIB,
        epoch=EPOCH, name=f"1627-{seed}-{ordinal}")
    return mover


def _window_plan(queue, consumer: str, stage_root: str) -> dict:
    """A two-phase window with a stage leg and a RAM leg per phase."""
    total = 4 * GIB
    phases = []
    start = 0
    for ordinal in range(2):
        end = start + 2 * GIB
        row = _row(_hexkey(f"1627-wm{ordinal}"),
                   {"cpu": 1, "mem_gb": 1, STAGE_KIND: 2}, queue)
        ram_row = _row(_hexkey(f"1627-wr{ordinal}"),
                       {"cpu": 1, "mem_gb": 1, RAM_KIND: 2}, queue)
        phases.append({
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end, "stage_gib": 2,
            "mover_row": {
                **row, "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE,
                    "manifest_sha256": _hexkey("1627-window-man"),
                    "manifest_bytes": total,
                    "range_start_bytes": start, "range_end_bytes": end}},
            "egress_row": _row(_hexkey(f"1627-we{ordinal}"),
                               {"mem_gb": 1}, queue),

            "ram_mover_row": {
                **ram_row, "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM,
                    "manifest_sha256": _hexkey("1627-window-man"),
                    "manifest_bytes": total,
                    "range_start_bytes": start, "range_end_bytes": end}},
            "ram_egress_row": _row(_hexkey(f"1627-wre{ordinal}"),
                                   {"mem_gb": 1}, queue),
        })
        start = end
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=STAGE,
        stage_root=stage_root,
        manifest_sha256=_hexkey("1627-window-man"),
        manifest_bytes=total, phases=phases, ram_tier_id=RAM)


def _publish_plan_mover(queue, plan, ordinal: int) -> str:
    """Publish one plan phase's stage mover row; return its action key."""
    phase = plan["phases"][ordinal]
    assert isinstance(phase, dict)
    row = dict(phase["mover_row"])  # type: ignore[index]
    queue.publish(action_key=str(row["action_key"]),
                  cas_root=str(row["cas_root"]),
                  checkout_root=str(row["checkout_root"]),
                  worker_script=str(row["worker_script"]),
                  tags=["dl380g10"], resources=dict(row["resources"]),  # type: ignore[arg-type]
                  residency=dict(row["residency"]))  # type: ignore[arg-type]
    return str(row["action_key"])


def _live_plan(queue, consumer: str) -> dict:
    """A claimed consumer with its head range landed on the stage."""
    total = 4 * GIB
    built = []
    start = 0
    for ordinal in range(2):
        end = start + 2 * GIB
        row = _row(_hexkey(f"1627-lm{ordinal}"),
                   {"cpu": 1, "mem_gb": 1, STAGE_KIND: 2}, queue)
        built.append({
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end, "stage_gib": 2,
            "mover_row": {
                **row, "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE,
                    "manifest_sha256": _hexkey("1627-live-man"),
                    "manifest_bytes": total,
                    "range_start_bytes": start, "range_end_bytes": end}},
            "egress_row": _row(_hexkey(f"1627-le{ordinal}"),
                               {"mem_gb": 1}, queue),
        })
        start = end
    plan = residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=STAGE,
        stage_root="/stage/prewarm",
        manifest_sha256=_hexkey("1627-live-man"),
        manifest_bytes=total, phases=built)
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=consumer, cas_root=str(queue.root / "cas"),
        checkout_root=str(queue.root / "co"),
        worker_script=str(queue.root / "worker.py"),
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE,
                   "manifest_sha256": _hexkey("1627-live-man"),
                   "manifest_bytes": total,
                   "leads": residency_plan.leads_for(plan)})
    return plan


def _tiers(stage: Path, ram: Path) -> dict:
    """The two announced tiers of the stalled box."""
    return {
        STAGE: dict(_tier_record(stage, gib=STAGE_CAP)),
        RAM: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
              "tier_id": RAM, "host": "dl380g10", "tier": "ram",
              "mountpoint": str(ram), "epoch": EPOCH,
              "capacity_bytes": RAM_CAP * GIB, "window_gib": RAM_CAP,
              "ram_admission": {"admissible": True}},
    }


def _cycle(queue, stage: Path, ram: Path) -> None:
    """One whole tier cycle over both tiers, as the storage box runs it."""
    tiers = _tiers(stage, ram)
    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(),
                    discover=lambda **_kwargs: dict(tiers))


def _jammed(tmp_path: Path):
    """The incident shape: orphans, a withdrawn group, waiters, no room."""
    queue = _queue(tmp_path, stage_gib=STAGE_CAP)
    queue.mint_tier_capacity(RAM, {"ram_gib": RAM_CAP})
    queue.ledger("dl380g10").ensure_capacity({"cpu": 80, "mem_gb": 64})
    stage = tmp_path / "stage"
    stage.mkdir()
    ram = tmp_path / "ram"
    ram.mkdir()
    stage_release.register_stage_root(queue, tier_id=STAGE, stage_root=stage)
    stage_release.register_stage_root(queue, tier_id=RAM, stage_root=ram)

    _end(queue, DONE, how="done")
    _end(queue, GONE, how="withdrawn")
    stage_orphans = []
    for ordinal in range(2):
        stage_orphans.append(_hexkey(f"1627-sorphan-{ordinal}"))
        _stage_range(queue, mover=stage_orphans[-1], consumer=DONE,
                     stage=stage, ordinal=ordinal, manifest="e" * 64)
    for ordinal in range(2):
        stage_orphans.append(_hexkey(f"1627-sgone-{ordinal}"))
        _stage_range(queue, mover=stage_orphans[-1], consumer=GONE,
                     stage=stage, ordinal=2 + ordinal, manifest="e" * 64)

    group_plan = _declared_plan(
        queue, PG, [("p0", 2, True, 1), ("p1", 2, True, 1),
                    ("p2", 2, True, 1), ("p3", 1, False, 1)], tag="pg")
    _live(queue, group_plan, PG)
    units = pt.declared_units(queue, {STAGE: {"tier": "stage"}},
                              [_consumer(PG)])
    assert len(units) == 1
    pt.reserve_pass(queue, STAGE, units, admitted=lambda unit: True)
    queue.withdraw(PG, reason="1627 fixture: group withdrawn", by="test")

    _end(queue, RAM_DONE, how="done")
    _end(queue, RAM_FAILED, how="failed")
    ram_orphans = ([_ram_orphan(queue, ram, RAM_DONE, "done", ordinal)
                    for ordinal in range(3)]
                   + [_ram_orphan(queue, ram, RAM_FAILED, "failed", ordinal)
                      for ordinal in range(2)])

    for index in range(3):
        _queued_demand(queue, f"1627-queued-stage-{index}", STAGE,
                       STAGE_KIND, 1)
    for index in range(12):
        _queued_demand(queue, f"1627-queued-ram-{index}", RAM, RAM_KIND, 1)

    window_plan = _window_plan(queue, WINDOW, str(stage))
    residency_plan.freeze(queue, window_plan)
    queue.publish(
        action_key=WINDOW, cas_root=str(queue.root / "cas"),
        checkout_root=str(queue.root / "co"),
        worker_script=str(queue.root / "worker.py"),
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE,
                   "manifest_sha256": _hexkey("1627-window-man"),
                   "manifest_bytes": 4 * GIB,
                   "leads": residency_plan.leads_for(window_plan)})
    window_head = _publish_plan_mover(queue, window_plan, 0)
    _stage_range(queue, mover=window_head, consumer=WINDOW, stage=stage,
                 ordinal=0, manifest=_hexkey("1627-window-man"))

    declared_plan = _declared_plan(
        queue, DECLARED, [("q0", 2, True, 1), ("q1", 1, False, 1)], tag="dc")
    _live(queue, declared_plan, DECLARED)

    live_plan = _live_plan(queue, LIVE)
    live_head = _hexkey("1627-lm0")
    _stage_range(queue, mover=live_head, consumer=LIVE, stage=stage,
                 ordinal=0, manifest=_hexkey("1627-live-man"))
    _claim_with_progress(queue, LIVE, phase="phase-0")
    pg_holder = prelaunch_group.holder_name(
        PG, STAGE, residency_plan.prelaunch_phase_names(group_plan))
    d_holder = prelaunch_group.holder_name(
        DECLARED, STAGE, residency_plan.prelaunch_phase_names(declared_plan))
    d_prefix = [d_holder] + [str(lead) for lead in
                             residency_plan.leads_for(declared_plan)]
    return (queue, stage, ram, stage_orphans, ram_orphans, live_head,
            pg_holder, d_prefix)


def _held(queue, tier_id: str, key: str) -> bool:
    """Whether one holder still owns tokens on one tier."""
    return bool(queue.tier_ledger(tier_id).holder_tokens(key))


def test_the_stalled_shape_asks_pressure_on_both_tiers(tmp_path: Path) -> None:
    """The declared unit and the window ask the sweep for room, both tiers."""
    queue, stage, ram, _, _, _, _, _ = _jammed(tmp_path)
    skipped: list[dict] = []
    pressure = tier_loop.window_pressure(queue, tiers=_tiers(stage, ram),
                                         skipped=skipped)
    assert pressure.get(STAGE, 0) > 0, (pressure, skipped)
    assert pressure.get(RAM, 0) > 0, (pressure, skipped)


def test_cycles_reclaim_the_stranded_holders(tmp_path: Path) -> None:
    """A few real cycles give back the orphans and the withdrawn prefix."""
    (queue, stage, ram, stage_orphans, ram_orphans, live_head, pg_holder,
     d_prefix) = _jammed(tmp_path)
    for _ in range(4):
        _cycle(queue, stage, ram)
    assert [mover for mover in stage_orphans
            if _held(queue, STAGE, mover)] == []
    assert [mover for mover in ram_orphans
            if _held(queue, RAM, mover)] == []
    assert not _held(queue, STAGE, pg_holder), "a withdrawn group lets go"
    assert _held(queue, STAGE, live_head), "a live claim stays"
    ledger = queue.tier_ledger(STAGE)
    held_prefix = sum(
        int(ledger.holder_tokens(key).get("stage_gib", 0)) for key in d_prefix)
    assert held_prefix == 2, "the declared waiter reserves its prefix"


def test_a_futile_waiter_names_its_refusal_once(tmp_path: Path) -> None:
    """A cycle that reclaims nothing files one line naming the refused path."""
    queue = _queue(tmp_path, stage_gib=STAGE_CAP)
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=STAGE, stage_root=stage)
    waiter = _hexkey("1627-too-big")
    plan = _declared_plan(queue, waiter,
                          [("p0", 25, True, 1), ("p1", 1, False, 1)], tag="b")
    _live(queue, plan, waiter)
    tiers = {STAGE: _tier_record(stage, gib=STAGE_CAP)}
    for _ in range(4):
        tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                        receipts=tier_loop.ReceiptCache(),
                        discover=lambda **_kwargs: dict(tiers))
    events = [event for event in queue.consumer_events(waiter)
              if event.get("event") == "window-pressure-skipped"]
    assert len(events) == 1, events
    assert events[0]["reason"], events

"""R12 and the native capture of 2026-09-22, replayed under the refill horizon.

``tests/fixtures/r12_stage_20260922.json`` holds what the live queue said at
22:30:26Z: GLM Stage A R12 (``683cb3caa5ea``) reading ``chain-043`` with 24
landed 22 GiB ranges (``chain-043`` to ``chain-019``, less ``chain-021``) on a
565 GiB stage, two more rows (``chain-021``, ``chain-018``) waiting in
``ready/``, and the native capture (``a92f62783e8f``) whose 3 GiB lead could
not claim.  The capture's own run at 23:03Z is replayed from the same file:
admitted, it read ``head`` to ``layer-2``, reported ``layer-3``, and waited
300 s for a 14 GiB range that never published.

The byte ranges, sizes, copy rates, claim and report times are the live ones;
the keys are the fixture's.  R12's own 34 GiB claim reservation and one 1 GiB
receipt-less holder are folded into the capacity (565 - 35 = 530), so the
tier has the 2 GiB free it had.  Timestamps are shifted to now, because a
progress observation is read against the claim it belongs to.

By ``residency_plan.refill_horizon``: R12 has read to the end of
``chain-043`` (81.2 GB) in 3919 s, 20.7 MB/s; it reserves 100 GiB of memory
and an 80 GiB GPU budget, so it can be reading anything that starts before
274.5 GB, which is ``chain-042`` to ``chain-034``.  Its slowest copy landed a
23.4 GB range at 134 MB/s, 174 s, so a copy published now lands within
30 + 60 + 174 s, in which R12 reads 5.5 GB -- less than one range --
and ``chain-033`` completes the horizon.  ``chain-032`` is the advance; the
twelve landed ranges ``chain-031`` to ``chain-019`` (264 GiB) are past it.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _claim_shortage, _cycle, _fixture_queue, _land)
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    GIB, STAGE_KIND, TIER, _hexkey, _row, _tier_record,
    assert_ledger_matches_the_stage)

DATA = json.loads((Path(__file__).resolve().parent / "fixtures"
                   / "r12_stage_20260922.json").read_text())
R12 = "e" * 64
R12_MANIFEST = "b" * 64
CAPTURE = "f" * 64
CAPTURE_MANIFEST = "c" * 64
FILL = f"{storage_tiers.FILL_KIND}{storage_tiers.TIER_DEMAND_SEPARATOR}{TIER}"
#: The stage less R12's own claim reservation and the receipt-less holder.
CAPACITY = (DATA["tier"]["capacity_gib"]
            - sum(DATA["tier"]["folded_holders"].values()))
SAMPLE_UNIX = 1790116226.6
LANDED = [entry["phase"] for entry in DATA["r12"]["landed"]]
#: By the module docstring's arithmetic.
INSIDE = [f"chain-{n:03d}" for n in range(43, 32, -1)]
ADVANCE = "chain-032"
PAST = [f"chain-{n:03d}" for n in range(31, 18, -1) if n != 21]


def _mover(label: str, name: str) -> str:
    return _hexkey(f"{label}mover{name}")


def _plan(queue: pool.PoolQueue, consumer: str, *, label: str, manifest: str,
          phases: list[dict[str, object]], fill: int) -> dict[str, object]:
    total = int(phases[-1]["end_bytes"])
    built = []
    for phase in phases:
        name, start, end = str(phase["name"]), int(phase["start_bytes"]), int(phase["end_bytes"])
        built.append({
            "name": name, "start_bytes": start, "end_bytes": end,
            "stage_gib": int(phase["stage_gib"]),
            "mover_row": {
                **_row(queue, _mover(label, name),
                       {STAGE_KIND: int(phase["stage_gib"]), FILL: fill,
                        "cpu": 2, "mem_gb": 1}),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": manifest, "manifest_bytes": total,
                    "range_start_bytes": start, "range_end_bytes": end}},
            "egress_row": _row(queue, _hexkey(f"{label}egress{name}"),
                               {"mem_gb": 1}),
        })
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=manifest, manifest_bytes=total, phases=built)


def _consumer(queue: pool.PoolQueue, consumer: str, plan: dict[str, object], *,
              manifest: str, mem_gb: int) -> None:
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=consumer, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": mem_gb},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": manifest,
                   "manifest_bytes": int(plan["manifest_bytes"]),
                   "leads": residency_plan.leads_for(plan)})


def _claim(queue: pool.PoolQueue, consumer: str, *, phase: str,
           claimed_unix: float, reported_unix: float,
           gpu_budget: int | None = None) -> None:
    source = queue.item_path(pool.READY, consumer)
    item = json.loads(source.read_text())
    source.unlink()
    item.update({"action_key": consumer, "claimed_unix": claimed_unix,
                 "claimed_by": "replay-fixture", "claimed_host": "dl380g10"})
    if gpu_budget is not None:
        item["gpu_admission"] = {"gpu_memory_budget_bytes": gpu_budget}
    queue.item_path(pool.CLAIMED, consumer).write_text(json.dumps(item))
    queue.write_lease(
        consumer, owner="replay-fixture", claim_snapshot=item,
        progress_observation={
            "source": "action-progress",
            "last_accepted": {"phase": phase, "units_completed": 1,
                              "reported_unix": reported_unix}})


def _r12(queue: pool.PoolQueue, stage: Path, shift: float) -> dict[str, object]:
    """R12 at 22:30:26Z: claimed, reading ``chain-043``, 24 ranges landed."""

    r12 = DATA["r12"]
    plan = _plan(queue, R12, label="r12", manifest=R12_MANIFEST,
                 phases=r12["phases"], fill=r12["sealed_fill_mb_s"])
    _consumer(queue, R12, plan, manifest=R12_MANIFEST,
              mem_gb=r12["resources"]["mem_gb"])
    by_name = {str(phase["name"]): phase for phase in r12["phases"]}
    for entry in r12["landed"]:
        phase = by_name[entry["phase"]]
        start, end = int(phase["start_bytes"]), int(phase["end_bytes"])
        # The receipt's own rate, carried onto the fixture's range.
        rate = entry["bytes_staged"] / entry["seconds"]
        _land(queue, stage, consumer=R12, manifest=R12_MANIFEST,
              mover=_mover("r12", entry["phase"]), name=entry["phase"],
              start=start, end=end, seconds=(end - start) / rate)
    for name in r12["ready_rows"]:
        queue.publish(**dict(next(
            phase["mover_row"] for phase in plan["phases"]  # type: ignore[union-attr]
            if phase["name"] == name)))
    _claim(queue, R12, phase=r12["accepted"]["phase"],
           claimed_unix=r12["claimed_unix"] + shift,
           reported_unix=r12["accepted"]["reported_unix"] + shift,
           gpu_budget=r12["gpu_memory_budget_bytes"])
    return plan


def _capture(queue: pool.PoolQueue) -> dict[str, object]:
    capture = DATA["capture"]
    plan = _plan(queue, CAPTURE, label="capture", manifest=CAPTURE_MANIFEST,
                 phases=capture["phases"], fill=capture["sealed_fill_mb_s"])
    _consumer(queue, CAPTURE, plan, manifest=CAPTURE_MANIFEST,
              mem_gb=capture["resources"]["mem_gb"])
    return plan


def _r12_held(queue: pool.PoolQueue) -> dict[str, int]:
    ledger = queue.tier_ledger(TIER)
    return {name: int(ledger.holder_tokens(_mover("r12", name)).get("stage_gib", 0))
            for name in LANDED}


def _free(queue: pool.PoolQueue) -> int:
    return int(queue.tier_ledger(TIER).available().get("stage_gib", 0))


def test_r12s_horizon_is_the_docstring_arithmetic(tmp_path: Path) -> None:
    queue, stage = _fixture_queue(tmp_path, CAPACITY)
    plan = _r12(queue, stage, time.time() - SAMPLE_UNIX)
    consumer = next(entry for entry in tier_loop.live_consumers(queue)
                    if entry["action_key"] == R12)

    horizon = tier_loop._stage_horizon(queue, consumer, plan,
                                       _tier_record(stage, gib=CAPACITY))

    assert horizon is not None
    assert horizon["consumption_bytes_per_s"] == pytest.approx(
        81195938468 / (DATA["r12"]["accepted"]["reported_unix"]
                       - DATA["r12"]["claimed_unix"]))
    assert round(horizon["landing_s"]) == 174                   # type: ignore[arg-type]
    assert horizon["reach_end_bytes"] == 81195938468 + 100 * GIB + 80 * GIB
    by_start = {int(phase["start_bytes"]): str(phase["name"])
                for phase in plan["phases"]}                    # type: ignore[union-attr]
    assert by_start[int(horizon["horizon_end_bytes"])] == ADVANCE  # type: ignore[arg-type]
    past_landed = [leg["phase"] for leg in horizon["beyond"]     # type: ignore[union-attr]
                   if leg["phase"] in LANDED]
    assert past_landed == PAST
    assert sum(_r12_held(queue)[name] for name in PAST) == 264
    assert set(INSIDE) | {ADVANCE} | set(PAST) == set(LANDED)


def test_the_captures_lead_claims_after_r12s_farthest_range_goes(
        tmp_path: Path) -> None:
    """22:30Z: the capture's 3 GiB lead, queued and short on a 2 GiB-free stage.

    Before the fix nothing was evicted, because every range belonged to a live
    plan, and the lead waited for R12 to read 528 GiB.  After it, one cycle
    gives back ``chain-019`` -- the range R12 reaches last -- and the lead's
    claim fits.  R12 keeps 506 GiB; nothing inside its horizon moves, and its
    two queued rows past the horizon stay queued.
    """

    queue, stage = _fixture_queue(tmp_path, CAPACITY)
    _r12(queue, stage, time.time() - SAMPLE_UNIX)
    plan = _capture(queue)
    lead = dict(plan["phases"][0]["mover_row"])                  # type: ignore[index]
    queue.publish(**lead)
    assert _free(queue) == 2
    assert _claim_shortage(queue, str(lead["action_key"]), 3) is not None

    _cycle(queue, stage, gib=CAPACITY)

    held = _r12_held(queue)
    assert [name for name in LANDED if not held[name]] == ["chain-019"]
    assert sum(held.values()) == 506
    assert _claim_shortage(queue, str(lead["action_key"]), 3) is None
    for name in DATA["r12"]["ready_rows"]:
        assert queue.item_path(pool.READY, _mover("r12", name)).exists()
    assert_ledger_matches_the_stage(queue)


def test_the_captures_layer_3_publishes_on_the_cycle_that_sees_its_report(
        tmp_path: Path) -> None:
    """23:03:47Z, the first cycle after the capture reported ``layer-3``.

    ``head`` to ``layer-2`` still hold 6 GiB and the stage has 2 GiB free.
    Before the fix this cycle published the four egresses and stalled
    ``layer-3`` for room.  After it, ``chain-019`` goes (22 GiB) and
    ``layer-3`` is published in the same cycle.
    """

    shift = time.time() - DATA["capture"]["egressed_unix"]
    queue, stage = _fixture_queue(tmp_path, CAPACITY + 6)
    _r12(queue, stage, shift)
    plan = _capture(queue)
    for phase in plan["phases"][:4]:                             # type: ignore[index]
        _land(queue, stage, consumer=CAPTURE, manifest=CAPTURE_MANIFEST,
              mover=_mover("capture", str(phase["name"])), name=str(phase["name"]),
              start=int(phase["start_bytes"]), end=int(phase["end_bytes"]),
              seconds=10.0)
    capture = DATA["capture"]
    _claim(queue, CAPTURE, phase="layer-3",
           claimed_unix=capture["claimed_unix"] + shift,
           reported_unix=capture["accepted"]["reported_unix"] + shift)
    layer_3 = _mover("capture", "layer-3")
    assert _free(queue) == 2

    _cycle(queue, stage, gib=CAPACITY + 6)

    assert queue.item_path(pool.READY, layer_3).exists()
    held = _r12_held(queue)
    assert [name for name in LANDED if not held[name]] == ["chain-019"]
    assert _claim_shortage(queue, layer_3, 14) is None
    assert_ledger_matches_the_stage(queue)


def test_the_captures_layer_3_publishes_after_its_lead_is_given_back(
        tmp_path: Path) -> None:
    """23:03:53Z on: the egresses ran and the capture's first range is gone.

    From here the live loop logged ``window-gated joint-fit-stall`` 60 times
    in 300 s, because the joint gate re-read the running capture as a
    newcomer once ``head`` was egressed.  Under #903 alone the newcomer
    relief then counted R12's two queued rows (44 GiB) beside the capture's
    14 and gave back ``chain-022``, ``chain-020`` and ``chain-019`` (66 GiB,
    R12 kept 462).  Since #908 a claimed consumer is never a newcomer:
    ``layer-3`` is an admitted window's advance, the would-publish term asks
    for its 14 GiB against the 2 free, and ``chain-019`` alone (22 GiB)
    goes.  R12 keeps 506 GiB and ``layer-3`` publishes in the same cycle.
    """

    shift = time.time() - DATA["capture"]["egressed_unix"]
    queue, stage = _fixture_queue(tmp_path, CAPACITY)
    _r12(queue, stage, shift)
    _capture(queue)
    capture = DATA["capture"]
    _claim(queue, CAPTURE, phase="layer-3",
           claimed_unix=capture["claimed_unix"] + shift,
           reported_unix=capture["accepted"]["reported_unix"] + shift)
    layer_3 = _mover("capture", "layer-3")

    _cycle(queue, stage, gib=CAPACITY)

    assert queue.item_path(pool.READY, layer_3).exists()
    held = _r12_held(queue)
    assert [name for name in LANDED if not held[name]] == ["chain-019"]
    assert sum(held.values()) == 506
    assert _claim_shortage(queue, layer_3, 14) is None
    assert_ledger_matches_the_stage(queue)

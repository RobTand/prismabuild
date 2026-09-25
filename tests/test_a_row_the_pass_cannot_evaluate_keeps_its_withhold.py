"""A row the claim pass cannot evaluate keeps this host's withhold (#1143).

The 2026-09-25 incident, on generation c71b26c64e9c: GLM-5.3 Stage B row
``776aaf70ccb3`` (``gpu 1, mem_gb 96``, image-pinned) waited more than 27
minutes and 2414 passes on an idle, SW-capped sparky while CPU rows were
admitted beside it.  #1127 (#1125) made its ``broker_jobs_present`` refusal
withhold the whole box, which is the right verdict, but every pass that read
the image inventory as unknown denied the row
``container_image_presence_unknown`` and went on to the rows behind it, with
nothing withheld.  Each CPU row admitted there was another broker job, so the
drain never finished: the #1125 livelock through another door.

A pass that cannot evaluate a row for a reason that says nothing about the
row -- another loop holds its lock (#1085), the image inventory or the
residency map does not read -- carries this host's live withhold for it, as
``transition_busy`` always has.  Unknown is still not presence (#714): the
row is not admitted.  A verdict, such as an image the box positively lacks,
still ends the withhold.

Nothing here touches the live queue, a real pool or a real device.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from prismabuild import adaptive_cpu, adaptive_gpu, pool  # noqa: E402
from prismabuild import core as pb  # noqa: E402

T0 = 2_000_000.0
GIB = adaptive_gpu.GIB
IMAGE = "content:sha256:" + "c" * 64

#: Idle GB10 limiter breakdown with every limiter off.
QUIET_LIMITERS = {
    "gpu_idle": False, "hw_power_brake_slowdown": False, "hw_slowdown": False,
    "hw_thermal_slowdown": False, "sw_power_cap": False,
    "sw_thermal_slowdown": False, "sync_boost": False,
}

#: Sparky's row 776aaf70ccb3, and the rows admitted beside it.
STAGE_B = {"cpu": 10, "gpu": 1, "mem_gb": 96}
SMALL_GPU = {"cpu": 4, "gpu": 1, "mem_gb": 6}
SMALL_CPU = {"cpu": 2, "mem_gb": 4}


def _key(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


def _denial(q: pool.PoolQueue, key: str) -> dict:
    path = adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(value for value in records.values() if value["action_key"] == key)


def _key_of(item):
    return None if item is None else item["action_key"]


class _Unheld:
    """What ``_transition_locked`` yields while another loop holds the key."""

    def __enter__(self):
        return False

    def __exit__(self, *args):
        return False


@pytest.fixture()
def box(tmp_path: Path, monkeypatch):
    """Sparky's admission path: an idle SW-capped GB10, a broker sample the
    test controls, and an image inventory each pass names.

    As in ``test_a_sw_capped_gb10_row_withholds_while_its_box_drains``:
    ``tick`` publishes a fresh broker sample whose ``jobs`` are every holder
    on the ledger, CPU-only holders included.
    """

    clock = [T0]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    monkeypatch.setattr(adaptive_cpu, "action_identity", lambda item: ("shape", False))
    monkeypatch.setattr(adaptive_gpu, "action_contract", lambda item, demand: (
        "shape", False, False, int(demand.get("mem_gb", 0)) * GIB))
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": clock[0], "busy_cpus": 0., "psi_some": 0.,
        "cpu_count": 20, "interval_s": 1.})
    device = {"uuid": "GPU-1", "name": "NVIDIA GB10", "power_w": 4.96,
              "power_limit_w": None, "power_reference_w": 140.,
              "power_reference_scope": "soc_tdp", "memory_domain": "shared_system",
              "limited": True, "sm_clock_mhz": 208., "max_sm_clock_mhz": 3003.,
              "throttle_active_mask": 0x4,
              "throttle_reasons": dict(QUIET_LIMITERS, sw_power_cap=True)}
    sample = {"schema": "prismabuild.gpu_capacity.v1", "sample_id": str(T0),
              "sampled_unix": T0, "complete": True, "attributed": True,
              "devices": [device],
              "host_total_bytes": 128 * GIB, "host_available_bytes": 120 * GIB,
              "memory_pressure_some": 0., "memory_pressure_full": 0.,
              "cpu_pressure_some": 0., "foreign_processes": [], "jobs": []}
    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda self: dict(sample))
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    capacity = {"cpu": 20, "gpu": 1, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}

    def publish(name: str, resources: dict[str, int], **kw) -> str:
        clock[0] += 0.001
        key = _key(name)
        queue.publish(action_key=key, cas_root=str(tmp_path / "cas"),
                      checkout_root=str(tmp_path), worker_script="worker.py",
                      resources=resources, needs_gpu=bool(resources.get("gpu")), **kw)
        return key

    def tick(seconds: float = 2.0) -> None:
        clock[0] += seconds
        sample.update(sampled_unix=clock[0], sample_id=str(clock[0]))
        sample["jobs"] = []
        for key in queue.ledger().held_keys():
            record = {"action_key": key, "nonce": key + "-attempt",
                      "scope_unit": key + "-scope", "sampled_unix": clock[0],
                      "cpu_seconds": .01 * (clock[0] - T0),
                      "wall_seconds": clock[0] - T0, "complete": True}
            adaptive_cpu.write_json(
                adaptive_cpu.local_telemetry_path(queue.ledger().base, key), record)
            sample["jobs"].append({"action_key": key, "nonce": record["nonce"],
                                   "scope_id": record["scope_unit"], "complete": True})

    def claim(images=(IMAGE,)):
        """One pass; ``images=None`` is an inventory that did not read."""

        observed = None if images is None else set(images)
        return _key_of(queue.claim(
            capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True, has_gpu=True,
            tags=[pb.CONTAINER_IMAGE_TAG], observed_images=observed))

    return queue, clock, publish, tick, claim


def _stage_b_withholding_the_box(box):
    """The incident's steady state, on a pass whose inventory read.

    A CPU shard holds a broker job, so the #719 first-job exception refuses
    the image-pinned Stage B row with ``broker_jobs_present``, and the row
    withholds the whole box (#1125).
    """

    queue, clock, publish, tick, claim = box
    shard = publish("band-016", SMALL_CPU)
    assert claim() == shard
    stage_b = publish("stage-b-row-023", STAGE_B, container_images=[IMAGE])
    behind = publish("canary-leg-3", SMALL_CPU)
    tick()
    assert claim() is None
    denial = _denial(queue, stage_b)
    assert denial["reason"] == "adaptive_gpu_refused_withholding"
    exception = denial["evidence"]["decision"]["sw_cap_idle_exception"]
    assert exception["exception_reason"] == "broker_jobs_present"
    assert denial["evidence"]["withhold"]["mode"] == "exclusive"
    return shard, stage_b, behind


# -- the incident: an unknown inventory keeps the drain -----------------------


def test_an_unknown_inventory_does_not_admit_a_cpu_row_behind_a_withholding_row(box) -> None:
    queue, clock, publish, tick, claim = box
    shard, stage_b, behind = _stage_b_withholding_the_box(box)
    withheld_at = clock[0]

    tick()
    assert claim(images=None) is None, (
        "a pass whose image inventory did not read admitted a CPU row behind "
        "the row withholding the box (#1143)")
    unknown = _denial(queue, stage_b)
    assert unknown["reason"] == "container_image_presence_unknown", (
        "unknown is still not presence (#714)")
    carried = unknown["evidence"]["withhold_carried"]
    assert carried["reason"] == "adaptive_gpu_refused_withholding"
    assert carried["mode"] == "exclusive"
    assert carried["epoch_unix"] == pytest.approx(withheld_at)
    assert queue.item_path(pool.READY, behind).exists()
    assert queue.item_path(pool.READY, stage_b).exists()

    # A run of unknown passes reads the same episode and never renews it.
    tick()
    assert claim(images=None) is None, "a second unknown pass forgot the withhold"
    again = _denial(queue, stage_b)["evidence"]["withhold_carried"]
    assert again["epoch_unix"] == carried["epoch_unix"]

    # The shard drains, the inventory reads, and the row the box held for runs.
    queue.finish(shard, status="executed", detail={})
    tick()
    assert claim() == stage_b


def test_the_carry_crosses_a_busy_lock_after_an_unknown_inventory(box, monkeypatch) -> None:
    """``transition_busy`` and an unknown inventory carry one episode between them."""

    queue, clock, publish, tick, claim = box
    shard, stage_b, behind = _stage_b_withholding_the_box(box)
    tick()
    assert claim(images=None) is None
    episode = _denial(queue, stage_b)["evidence"]["withhold_carried"]["epoch_unix"]

    original = queue._transition_locked
    monkeypatch.setattr(queue, "_transition_locked", lambda action_key, **kwargs: (
        _Unheld() if action_key == stage_b else original(action_key, **kwargs)))
    tick()
    assert claim() is None, "a busy lock after an unknown pass ended the drain"
    busy = _denial(queue, stage_b)
    assert busy["reason"] == "transition_busy"
    assert busy["evidence"]["withhold_carried"]["epoch_unix"] == episode
    assert busy["evidence"]["withhold_carried"]["reason"] == (
        "adaptive_gpu_refused_withholding")
    assert queue.item_path(pool.READY, behind).exists()


def test_a_carried_gpu_withhold_still_holds_back_only_gpu_rows(box) -> None:
    """The carry keeps the withhold's kind: CPU rows still fill the box (#1085)."""

    queue, clock, publish, tick, claim = box
    holder = publish("gpu-holder", SMALL_GPU)
    assert claim() == holder, "the idle SW-cap exception admits the first GPU job"
    stage_b = publish("stage-b-row-023", STAGE_B, container_images=[IMAGE])
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(stage_b)
    gpu_row = publish("gpu-test-shard", SMALL_GPU)
    first_cpu = publish("cpu-test-shard", SMALL_CPU)
    tick()
    assert claim() == first_cpu
    assert _denial(queue, stage_b)["evidence"]["withhold"]["mode"] == "gpu"

    second_cpu = publish("cpu-test-shard-2", SMALL_CPU)
    tick()
    assert claim(images=None) == second_cpu, "CPU-only work stopped filling the box"
    carried = _denial(queue, stage_b)["evidence"]["withhold_carried"]
    assert carried["mode"] == "gpu"
    waited = _denial(queue, gpu_row)
    assert waited["reason"] == "deferred_behind_withheld_row", (
        "a GPU row was judged beside the row withholding the GPU while the "
        "inventory did not read (#1143)")
    assert waited["evidence"]["withheld_for"] == stage_b
    assert queue.passes(gpu_row) == 0


@pytest.mark.parametrize("unreadable,reason", [
    (OSError("Stale file handle"), "residency_lead_record_unreadable"),
    ({"state": "map_unreadable", "map_path": "/pool/residency/map.json",
      "error": "[Errno 121] Remote I/O error"}, "residency_map_unreadable"),
], ids=["lead-record", "map"])
def test_an_unreadable_residency_keeps_the_drain(box, monkeypatch, unreadable, reason) -> None:
    """The residency gate's two stalls are not verdicts either way."""

    queue, clock, publish, tick, claim = box
    shard, stage_b, behind = _stage_b_withholding_the_box(box)
    original = queue.residency_verdict

    def stalled(item):
        if item.get("action_key") != stage_b:
            return original(item)
        if isinstance(unreadable, BaseException):
            raise unreadable
        return dict(unreadable)

    monkeypatch.setattr(queue, "residency_verdict", stalled)
    tick()
    assert claim() is None, (
        f"a {reason} pass admitted a CPU row behind the withholding row (#1143)")
    denial = _denial(queue, stage_b)
    assert denial["reason"] == reason
    assert denial["evidence"]["withhold_carried"]["mode"] == "exclusive"
    assert queue.item_path(pool.READY, behind).exists()


# -- bounds: a verdict ends it, and so does the episode -----------------------


def test_an_image_the_box_lacks_ends_the_withhold(box) -> None:
    """Absent is a verdict: the box cannot run the row, so it must not hold for it."""

    queue, clock, publish, tick, claim = box
    shard, stage_b, behind = _stage_b_withholding_the_box(box)
    tick()
    assert claim(images=()) == behind
    denial = _denial(queue, stage_b)
    assert denial["reason"] == "container_image_absent"
    assert "withhold_carried" not in denial["evidence"]


def test_a_carry_lapses_with_its_episode(box) -> None:
    queue, clock, publish, tick, claim = box
    shard, stage_b, behind = _stage_b_withholding_the_box(box)
    epoch = clock[0]
    tick()
    assert claim(images=None) is None
    clock[0] = epoch + pool.WITHHOLD_CEILING_S + 1
    tick()
    assert claim(images=None) == behind, "a carried withhold outlived its episode's ceiling"
    assert "withhold_carried" not in _denial(queue, stage_b)["evidence"]

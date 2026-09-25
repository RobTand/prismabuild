"""A pass that cannot read the image inventory keeps the drain (#1143).

The 2026-09-25 incident: GLM-5.3 Stage B row ``776aaf70ccb3`` (``gpu 1,
mem_gb 96``, image-pinned) waited more than 30 minutes on an idle sparky while
CPU rows were admitted beside it.  Its refusal was the #1125 one, and #1127
made it withhold the whole box (``broker_jobs_present``, the exclusive drain).
But sparky runs five worker loops, and whenever one of them was re-probing
the shared image inventory the others read it as unknown.  That pass recorded
``container_image_presence_unknown`` for the row and went on to the rows
behind it with no withhold in force, so a CPU row was admitted, the box kept
a broker job, and the drain never completed.

The fix treats that pass as what it is: a row not evaluated, not a verdict.
It carries this host's live withhold for the row, as ``transition_busy``
does (#1085), and records it so the next such pass reads the same episode.
Unknown is still not presence (#714): the row is not claimed.

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
from prismabuild import adaptive_cpu, adaptive_gpu, core as pb, pool  # noqa: E402

T0 = 2_000_000.0
GIB = adaptive_gpu.GIB
IMAGE = "sha256:" + "a" * 64
SEEN = [IMAGE]
UNKNOWN = None

QUIET_LIMITERS = {
    "gpu_idle": False, "hw_power_brake_slowdown": False, "hw_slowdown": False,
    "hw_thermal_slowdown": False, "sw_power_cap": False,
    "sw_thermal_slowdown": False, "sync_boost": False,
}

BIG = {"cpu": 10, "gpu": 1, "mem_gb": 96}
SMALL_CPU = {"cpu": 2, "mem_gb": 4}


def _device() -> dict:
    """Sparky on 2026-09-25: idle, SW power cap only, mask ``0x4``."""

    return {"uuid": "GPU-1", "name": "NVIDIA GB10", "power_w": 4.96,
            "power_limit_w": None, "power_reference_w": 140.,
            "power_reference_scope": "soc_tdp", "memory_domain": "shared_system",
            "limited": True, "sm_clock_mhz": 208., "max_sm_clock_mhz": 3003.,
            "throttle_active_mask": 0x4,
            "throttle_reasons": dict(QUIET_LIMITERS, sw_power_cap=True)}


def _key(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


def _denial(q: pool.PoolQueue, key: str) -> dict:
    path = adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(value for value in records.values() if value["action_key"] == key)


@pytest.fixture()
def box(tmp_path: Path, monkeypatch):
    """Sparky's admission path, as in
    ``test_a_sw_capped_gb10_row_withholds_while_its_box_drains``, with an
    image-capable worker whose inventory each claim names."""

    clock = [T0]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    monkeypatch.setattr(adaptive_cpu, "action_identity", lambda item: ("shape", False))
    monkeypatch.setattr(adaptive_gpu, "action_contract", lambda item, demand: (
        "shape", False, False, int(demand.get("mem_gb", 0)) * GIB))
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": clock[0], "busy_cpus": 0., "psi_some": 0.,
        "cpu_count": 20, "interval_s": 1.})
    sample = {"schema": "prismabuild.gpu_capacity.v1", "sample_id": str(T0),
              "sampled_unix": T0, "complete": True, "attributed": True,
              "devices": [_device()],
              "host_total_bytes": 128 * GIB, "host_available_bytes": 120 * GIB,
              "memory_pressure_some": 0., "memory_pressure_full": 0.,
              "cpu_pressure_some": 0., "foreign_processes": [], "jobs": []}
    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda self: dict(sample))
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    capacity = {"cpu": 20, "gpu": 1, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}

    def publish(name: str, resources: dict[str, int], images=None) -> str:
        clock[0] += 0.001
        key = _key(name)
        queue.publish(action_key=key, cas_root=str(tmp_path / "cas"),
                      checkout_root=str(tmp_path), worker_script="worker.py",
                      resources=resources, needs_gpu=bool(resources.get("gpu")),
                      container_images=images)
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

    def claim(observed):
        item = queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True,
                           has_gpu=True, tags=[pb.CONTAINER_IMAGE_TAG],
                           observed_images=observed)
        return None if item is None else item["action_key"]

    return queue, clock, publish, tick, claim


def _withholding(box) -> tuple[str, str, str, float]:
    """The incident's steady state: a CPU shard on the box, the image-pinned
    GPU row withholding the whole box for it, a CPU row behind."""

    queue, clock, publish, tick, claim = box
    shard = publish("band-016", SMALL_CPU)
    assert claim(SEEN) == shard
    big = publish("stage-b-row-023", BIG, images=SEEN)
    behind = publish("canary-leg-3", SMALL_CPU)
    tick()
    assert claim(SEEN) is None
    denial = _denial(queue, big)
    assert denial["reason"] == "adaptive_gpu_refused_withholding"
    assert denial["evidence"]["decision"]["sw_cap_idle_exception"][
        "exception_reason"] == "broker_jobs_present"
    assert denial["evidence"]["withhold"]["mode"] == "exclusive"
    return shard, big, behind, float(denial["denied_unix"])


def test_an_unknown_inventory_does_not_admit_a_cpu_row_into_the_drain(box) -> None:
    queue, clock, publish, tick, claim = box
    shard, big, behind, epoch = _withholding(box)
    passes = queue.passes(big)

    # A run of unknown passes, the steady state while sibling loops refresh.
    # The second one reads the first one's record, so it is the pass that
    # proves the carry survives its own denial.
    for _ in range(3):
        tick()
        assert claim(UNKNOWN) is None, (
            "a pass that could not read the image inventory admitted a CPU row "
            "into the drain its GPU row is withholding the box for (#1143)")
        denial = _denial(queue, big)
        assert denial["reason"] == "container_image_presence_unknown"
        carried = denial["evidence"]["withhold_carried"]
        assert carried["mode"] == "exclusive"
        assert carried["reason"] == "adaptive_gpu_refused_withholding"
        assert carried["epoch_unix"] == pytest.approx(epoch), "a carry renewed the episode"
    assert queue.item_path(pool.READY, behind).exists()
    # #714 stands: unknown is not presence, so no claim, no pass, no token.
    assert queue.item_path(pool.READY, big).exists()
    assert queue.passes(big) == passes

    # The drain completes and the row is admitted on a positive inventory.
    queue.finish(shard, status="executed", detail={})
    tick()
    assert claim(SEEN) == big


def test_a_carried_withhold_lapses_with_its_episode(box) -> None:
    """Unknown passes never renew the episode, so a box whose inventory stays
    unknown is held at most ``WITHHOLD_CEILING_S`` from the verdict."""

    queue, clock, publish, tick, claim = box
    shard, big, behind, epoch = _withholding(box)
    tick()
    assert claim(UNKNOWN) is None
    tick(pool.WITHHOLD_CEILING_S)
    assert claim(UNKNOWN) == behind
    assert "withhold_carried" not in _denial(queue, big)["evidence"]


def test_an_unknown_inventory_with_no_withhold_on_file_holds_nothing(box) -> None:
    """The carry is this host's own verdict, never a new one: a row this host
    has not withheld for leaves the rows behind it free, as before."""

    queue, clock, publish, tick, claim = box
    big = publish("stage-b-row-023", BIG, images=SEEN)
    behind = publish("canary-leg-3", SMALL_CPU)
    tick()
    assert claim(UNKNOWN) == behind
    denial = _denial(queue, big)
    assert denial["reason"] == "container_image_presence_unknown"
    assert "withhold_carried" not in denial["evidence"]


def test_an_unreadable_residency_lead_record_keeps_the_drain(box, monkeypatch) -> None:
    """The other pre-admission read that fails rather than answers."""

    queue, clock, publish, tick, claim = box
    shard, big, behind, epoch = _withholding(box)
    verdict = queue.residency_verdict

    def unreadable(item):
        if item.get("action_key") == big:
            raise OSError(116, "Stale file handle")
        return verdict(item)

    monkeypatch.setattr(queue, "residency_verdict", unreadable)
    tick()
    assert claim(SEEN) is None
    denial = _denial(queue, big)
    assert denial["reason"] == "residency_lead_record_unreadable"
    assert denial["evidence"]["withhold_carried"]["epoch_unix"] == pytest.approx(epoch)
    assert queue.item_path(pool.READY, behind).exists()


@pytest.mark.parametrize("reason", sorted(pool.WITHHOLD_CARRYING_REASONS))
def test_every_carrying_reason_hands_its_episode_to_the_next_pass(reason: str) -> None:
    """A ``transition_busy`` after an unknown pass, or the reverse, reads the
    episode the earlier pass carried: the host record holds only the latest
    reason, so the carry must be readable off each of them."""

    item = {"action_key": _key("row"), "published_unix": T0}
    carried = {"reason": "adaptive_gpu_refused_withholding", "mode": "exclusive",
               "epoch_unix": T0 + 1.0}
    records = {f"{item['action_key']}:{T0!r}": {
        "host": "sparky", "reason": reason, "denied_unix": T0 + 30.0,
        "evidence": {"withhold_carried": carried}}}
    assert pool.PoolQueue._carried_withhold(
        records, item, host="sparky", now=T0 + 60.0) == carried
    assert pool.PoolQueue._carried_withhold(
        records, item, host="sparky",
        now=T0 + 1.0 + pool.WITHHOLD_CEILING_S + 1.0) is None


def test_a_verdict_reason_does_not_carry() -> None:
    """``container_image_absent`` is the box's answer, not a failed read."""

    item = {"action_key": _key("row"), "published_unix": T0}
    records = {f"{item['action_key']}:{T0!r}": {
        "host": "sparky", "reason": "container_image_absent", "denied_unix": T0,
        "evidence": {"withhold_carried": {
            "reason": "adaptive_gpu_refused_withholding", "mode": "exclusive",
            "epoch_unix": T0}}}}
    assert pool.PoolQueue._carried_withhold(
        records, item, host="sparky", now=T0 + 1.0) is None

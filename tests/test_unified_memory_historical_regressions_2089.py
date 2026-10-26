"""Retained unified-memory admission claims through the current queue owners."""
from __future__ import annotations

from admitted_queue_fixture import simulated_admission

import json

import pytest

from prismabuild import box_capacity, gpu_sample_facts, pool, storage_tiers
from test_measurement_drains_gpu_backfill import cpu_sample, fleet, gpu_sample

CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 104}
TIERS = {"preferred": list(range(20)), "fallback": []}


def claim(fleet, *, capacity=CAPACITY, adaptive=True):
    queue, clock, readings, sample, _publish, tick, *_ = fleet
    sample["cpu_pressure_full"] = 0.0
    tick(0.01)
    return queue.claim(
        **simulated_admission(queue, capacity), cpu_tiers=TIERS, adaptive_cpu=adaptive,
        has_gpu=True, tags=["gb10", "sparklina"],
        cpu_sampler=lambda: cpu_sample(clock, readings),
        gpu_sampler=lambda: gpu_sample(sample, readings),
    )


def foreign(sample, gib):
    sample["foreign_processes"] = [
        {"pid": 9001, "gpu_uuid": "GPU-1", "used_bytes": gib << 30}]
    sample["host_available_bytes"] = sample["host_total_bytes"] - (max(8, gib) << 30)


def test_second_large_gpu_action_waits_until_the_first_releases(fleet):
    queue, _clock, _readings, _sample, publish, *_ = fleet
    first = publish("first-large-gpu", mem_gb=80, tags=["sparklina"])
    assert claim(fleet)["action_key"] == first
    second = publish("second-large-gpu", mem_gb=80, tags=["sparklina"])
    assert claim(fleet) is None
    assert queue.ledger().held()["mem_gb"] == 80
    assert queue.records.item_path(pool.READY, second).is_file()
    queue.finish(first, status="executed")
    assert claim(fleet)["action_key"] == second
    assert queue.ledger().held()["mem_gb"] == 80


@pytest.mark.parametrize("holder", ["claimed", "acquiring", "ram_mirror"])
def test_every_reservation_kind_counts_once_against_foreign_ceiling(fleet, holder):
    queue, _clock, _readings, sample, publish, *_rest, denial = fleet
    ledger = queue.ledger()
    ledger.ensure_capacity(CAPACITY)
    key = "a" * 64
    if holder == "ram_mirror":
        ram = queue.tier(storage_tiers.tier_id("ram", ledger.host))
        ram.mint({"ram_gib": 60})
        assert ram.take_advance(key, 60, "ram_gib") == ("taken", "")
        release = lambda: ram.cancel_fence(key)
    elif holder == "acquiring":
        handle = ledger.begin_acquire(key, {"mem_gb": 60})
        assert handle is not None and handle.startswith(pool.ACQUIRING_PREFIX)
        release = lambda: ledger.abandon_acquire(handle)
    else:
        assert ledger.acquire(key, {"mem_gb": 60})
        release = lambda: ledger.release(key)
    foreign(sample, 30)
    candidate = publish("beside-memory-reservation", gpu=0, mem_gb=31, tags=["sparklina"])
    assert claim(fleet, adaptive=False) is None
    decision = denial(candidate)["evidence"]["decision"]
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["committed_gib"] == 60
    assert ledger.held()["mem_gb"] == 60
    release()
    assert claim(fleet, adaptive=False)["action_key"] == candidate


@pytest.mark.parametrize("fault", ["absent", "stale", "incomplete", "unattributed", "domain_unknown"])
@pytest.mark.parametrize("starting", [False, True], ids=["claimed", "starting"])
def test_held_memory_remains_reserved_without_fresh_gpu_evidence(fleet, fault, starting):
    queue, _clock, readings, sample, publish, *_ = fleet
    ledger = queue.ledger()
    if starting:
        ledger.ensure_capacity(CAPACITY)
        handle = ledger.begin_acquire(
            "a" * 64, {"gpu": 1, "mem_gb": 100}, adaptive_gpu={
                "gpu_memory_budget_bytes": 98 << 30, "memory_domain": "shared_system",
                "admitted_unix": sample["sampled_unix"], "exclusive": False})
        assert handle is not None
        release = lambda: ledger.abandon_acquire(handle)
    else:
        holder = publish("held-before-telemetry-loss", mem_gb=100, tags=["sparklina"])
        assert claim(fleet)["action_key"] == holder
        release = lambda: queue.finish(holder, status="executed")
    candidate = publish("cpu-after-telemetry-loss", gpu=0, mem_gb=6, tags=["sparklina"])
    if fault == "absent":
        readings["gpu_missing"] = True
    elif fault == "stale":
        readings["gpu_stale"] = True
    elif fault == "incomplete":
        sample["complete"] = False
    elif fault == "unattributed":
        sample["attributed"] = False
    else:
        sample["devices"][0]["memory_domain"] = "unknown"
    assert claim(fleet) is None
    assert ledger.held()["mem_gb"] == 100
    assert queue.records.item_path(pool.READY, candidate).is_file()
    release()
    assert claim(fleet)["action_key"] == candidate


def test_serial_cpu_claims_cannot_reuse_unified_memory_headroom(fleet):
    queue, _clock, _readings, sample, publish, *_ = fleet
    holder = publish("gpu-before-cpu-claims", mem_gb=60, tags=["sparklina"])
    assert claim(fleet)["action_key"] == holder
    foreign(sample, 30)
    candidates = [publish(f"cpu-claim-{index}", gpu=0, mem_gb=16, tags=["sparklina"])
                  for index in range(2)]
    first = claim(fleet)
    assert first is not None and first["action_key"] in candidates
    assert claim(fleet) is None
    assert queue.ledger().held()["mem_gb"] == 76
    remaining = next(key for key in candidates if key != first["action_key"])
    assert queue.records.item_path(pool.READY, remaining).is_file()
    queue.finish(first["action_key"], status="executed")
    assert claim(fleet)["action_key"] == remaining


def test_observed_offer_and_claim_charge_foreign_memory_once(fleet):
    queue, clock, readings, sample, publish, _tick, *_ = fleet
    holder = publish("cpu-before-observation", gpu=0, mem_gb=60, tags=["sparklina"])
    assert claim(fleet)["action_key"] == holder
    foreign(sample, 30)
    observer = box_capacity.CapacityObserver(samples=3)
    observed = observer.offer(CAPACITY, queue.ledger().held, gpu_sample=sample,
                              mem_gb=98, load1=0, now=clock.now)
    assert observed["mem_gb"] == 90
    candidate = publish("fills-foreign-adjusted-offer", gpu=0, mem_gb=30, tags=["sparklina"])
    result = queue.claim(
        **simulated_admission(queue, observed), cpu_tiers=TIERS, adaptive_cpu=True,
        has_gpu=True, tags=["gb10", "sparklina"],
        cpu_sampler=lambda: cpu_sample(clock, readings),
        gpu_sampler=lambda: gpu_sample(sample, readings),
        unified_memory=observer.last.unified_memory,
    )
    assert result is not None and result["action_key"] == candidate
    assert queue.ledger().held()["mem_gb"] == 90


@pytest.mark.parametrize("identity", ["GPU-unknown", [], None], ids=["unmatched", "malformed", "missing"])
def test_mixed_devices_exclude_discrete_bytes_and_keep_unknown_attribution(fleet, identity):
    _queue, clock, _readings, sample, *_ = fleet
    sample["cpu_pressure_full"] = 0.0
    sample["devices"].append({
        "uuid": "GPU-2", "memory_domain": "discrete", "memory_total_bytes": 64 << 30,
        "memory_free_bytes": 44 << 30, "memory_used_bytes": 20 << 30})
    sample["foreign_processes"] = [
        {"gpu_uuid": "GPU-1", "used_bytes": 4 << 30},
        {"gpu_uuid": "GPU-2", "used_bytes": 20 << 30}]
    sample["jobs"] = [{"gpu_reported_bytes": 60 << 30}]
    evidence = gpu_sample_facts.unified_memory_evidence(sample, now=clock.now, max_age_s=5)
    assert evidence.foreign_bytes == 4 << 30
    assert not evidence.unknown
    sample["foreign_processes"].append({"gpu_uuid": identity, "used_bytes": 8 << 30})
    evidence = gpu_sample_facts.unified_memory_evidence(sample, now=clock.now, max_age_s=5)
    assert evidence.foreign_bytes == 4 << 30
    assert evidence.unknown



def test_known_foreign_gpu_work_keeps_its_independent_congestion_refusal(fleet):
    queue, _clock, _readings, sample, publish, *_rest, denial = fleet
    foreign(sample, 4)
    key = publish("known-foreign-gpu", gpu=1, mem_gb=16, tags=["sparklina"])
    assert claim(fleet) is None
    decision = denial(key)["evidence"]["decision"]
    assert decision["reason"] == "host_or_device_congested"
    assert decision["foreign_processes"] == sample["foreign_processes"]
    assert queue.ledger().held_keys() == []


def test_campaign_gpu_shorthand_uses_its_effective_memory_default(tmp_path):
    import pbcampaign

    row = {"argv": ["true"], "tags": ["gb10"], "gpu": True, "gpu_memory_gb": 8}
    manifest = tmp_path / "campaign.json"
    manifest.write_text(json.dumps([row]))
    assert pbcampaign.load_manifest(str(manifest), transport="pool") == [row]


@pytest.mark.parametrize("explicit_cap", [False, True], ids=["default_cap", "explicit_cap"])
def test_gpu_producer_keeps_its_cap_inside_allowance_inclusive_commitment(fleet, tmp_path, explicit_cap):
    import test_prepaid_writer_integration as producer_fixture
    from prismabuild import adaptive_cpu, core, output_contract, produced_output, produced_spool

    queue, _clock, _readings, sample, publish, *_rest, denial = fleet
    sample.update(host_total_bytes=120 << 30, host_available_bytes=112 << 30)
    holder = publish("cpu-beside-gpu-producer", gpu=0, mem_gb=4, tags=["sparklina"])
    assert claim(fleet)["action_key"] == holder
    template = producer_fixture._template(str(tmp_path / "canonical"))
    cas_root = tmp_path / "cas"
    original = producer_fixture._producer_request(tmp_path, cas_root, template)
    cas, request = produced_output._read_producer_request(cas_root, original)
    request.pop("action_key")
    demand = {"cpu": 2, "gpu": 1, "mem_gb": 100}
    request["params"].update(demand=demand, gpu_exclusive=False)
    if explicit_cap:
        request["params"]["gpu_memory_gb"] = 96
    request["environment"]["variables"].update({
        produced_spool.ROOT_ENV: str(tmp_path / "spool"),
        adaptive_cpu.EXPORT_SLOTS_ENV: "1",
    })
    action = core.seal_action(request)
    cas.publish_action_request(action)
    key = action["action_key"]
    queue.tier(producer_fixture.TIER).mint({producer_fixture.KIND: 4})
    queue.publish(
        action_key=key, cas_root=str(cas.root),
        checkout_root=str(tmp_path / "mover-checkout"), worker_script="/w.py",
        needs_gpu=True, tags=["sparklina"], resources={
            **demand, **output_contract.owner_demand_terms(template)},
        produced_output_template=template,
    )
    assert claim(fleet) is None
    decision = denial(key)["evidence"]["decision"]
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["committed_gib"] == 4
    assert decision["candidate_charge_gib"] == 101
    assert queue.ledger().held()["mem_gb"] == 4
    queue.finish(holder, status="executed")
    claimed = claim(fleet)
    assert claimed is not None and claimed["action_key"] == key
    assert claimed["gpu_admission"]["gpu_memory_budget_bytes"] == (96 if explicit_cap else 100) << 30
    assert queue.ledger().holder_tokens(key)["mem_gb"] == 101



@pytest.mark.parametrize("foreign_gpu", [False, True], ids=["release_fits", "gpu_congestion"])
def test_gpu_memory_preemption_preserves_independent_foreign_congestion(fleet, foreign_gpu):
    queue, _clock, _readings, sample, publish, *_rest, denial = fleet
    sample.update(host_total_bytes=120 << 30, host_available_bytes=112 << 30)
    capacity = {**CAPACITY, "mem_gb": 120}
    holder = publish("cpu-background-before-gpu", gpu=0, mem_gb=100,
                     tags=["sparklina"], retry_safe=True)
    assert claim(fleet, capacity=capacity)["action_key"] == holder
    if foreign_gpu:
        foreign(sample, 0)
    candidate = publish("gpu-waiting-for-memory", gpu=1, mem_gb=16,
                        tags=["sparklina"], priority=0)
    assert claim(fleet, capacity=capacity) is None
    decisions = queue.records.withdrawal_decisions(holder)
    assert bool(decisions) is (not foreign_gpu)
    if foreign_gpu:
        assert denial(candidate)["evidence"]["decision"]["reason"] == "host_or_device_congested"
    else:
        assert len(decisions) == 1
        assert decisions[0][1]["preempted_by"] == candidate
    assert queue.ledger().holder_tokens(holder)["mem_gb"] == 100
    assert queue.records.item_path(pool.READY, candidate).is_file()

"""Fresh unknown foreign GPU residency fences new unified DRAM commitments."""
from __future__ import annotations

import pytest

from prismabuild import _gang, adaptive_cpu, adaptive_gpu, census, pool
from test_gang_reservation_1517 import CAPACITY, TIERS, gang_fleet
from test_measurement_drains_gpu_backfill import (
    cpu_sample, fleet, gpu_sample,
)


def unknown(sample):
    sample["cpu_pressure_full"] = 0.0
    sample["foreign_processes"] = [
        {"pid": 9101, "gpu_uuid": "GPU-1", "used_bytes": 3 << 30},
        {"pid": 9102, "gpu_uuid": "GPU-1", "used_bytes": None},
    ]


def direct_claim(fleet, *, adaptive=False):
    queue, clock, readings, sample, _publish, tick, _claim, _denial = fleet
    tick(0.01)
    return queue.claim(
        capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=adaptive,
        has_gpu=True, tags=["gb10", "sparklina"],
        cpu_sampler=lambda: cpu_sample(clock, readings),
        gpu_sampler=lambda: gpu_sample(sample, readings),
    )


@pytest.mark.parametrize("gpu", [False, True], ids=["cpu", "gpu"])
@pytest.mark.parametrize("adaptive", [False, True], ids=["tokens", "adaptive"])
def test_unknown_residency_preserves_ready_without_an_attempt(
        fleet, gpu, adaptive):
    queue, _clock, _readings, sample, publish, _tick, _claim, denial = fleet
    unknown(sample)
    key = publish("unknown-residency", gpu=int(gpu), mem_gb=4, tags=["gb10"])
    ready = queue.records.item_path(pool.READY, key)
    original = ready.read_bytes()

    assert direct_claim(fleet, adaptive=adaptive) is None

    assert ready.read_bytes() == original
    assert queue.passes(key) == 0
    assert queue.ledger().held_keys() == []
    refused = denial(key)
    assert refused["reason"] == "unified_gpu_memory_unknown"
    assert refused["evidence"]["decision"]["sample_id"] == sample["sample_id"]
    assert refused["evidence"]["decision"]["sampled_unix"] == sample["sampled_unix"]


def test_unknown_residency_preserves_live_reservations(fleet):
    queue, _clock, _readings, sample, publish, _tick, _claim, denial = fleet
    holder = "b" * 64
    queue.ledger().configure_cpu_tiers(TIERS)
    queue.ledger().ensure_capacity(CAPACITY)
    assert queue.ledger().acquire(holder, {"cpu": 2, "mem_gb": 60})
    held = queue.ledger().holder_tokens(holder)
    unknown(sample)
    key = publish("beside-held-reservation", gpu=0, mem_gb=4, tags=["gb10"])

    assert direct_claim(fleet) is None

    assert denial(key)["reason"] == "unified_gpu_memory_unknown"
    assert queue.ledger().held_keys() == [holder]
    assert queue.ledger().holder_tokens(holder) == held
    assert queue.records.item_path(pool.READY, key).is_file()


@pytest.mark.parametrize("existing_election", [False, True])
def test_unknown_residency_neither_creates_nor_replaces_gang_elections(
        fleet, gang_fleet, existing_election):
    queue, _clock, _readings, sample, *_ = fleet
    _queue, _gclock, _publish, _finish, gclaim, denial, members = gang_fleet
    sample["cpu_pressure_full"] = 0.0
    group, keys = members("unknown-gang")
    if existing_election:
        assert gclaim("sparklina") is None
        assert _gang.elections(queue, group, 2)
    elections = _gang.elections(queue, group, 2)
    ready = {key: queue.records.item_path(pool.READY, key).read_bytes() for key in keys}
    unknown(sample)

    assert gclaim("sparklina") is None

    assert denial(keys[0])["reason"] == "unified_gpu_memory_unknown"
    assert _gang.elections(queue, group, 2) == elections
    assert queue.ledger().held_keys() == []
    for key in keys:
        assert queue.records.item_path(pool.READY, key).read_bytes() == ready[key]


def test_worker_serve_once_defers_before_execution(fleet, monkeypatch):
    queue, _clock, _readings, sample, publish, tick, _claim, denial = fleet
    unknown(sample)
    key = publish("worker-unknown", gpu=0, mem_gb=4, tags=["gb10"])
    tick(0.01)
    monkeypatch.setattr(adaptive_gpu, "trusted_sample", lambda: sample)
    monkeypatch.setattr(queue, "_sweep_due", lambda: False)

    def unexpected_execution(*_args, **_kwargs):
        pytest.fail("serve_once executed with unknown foreign residency")

    monkeypatch.setattr(queue, "execute", unexpected_execution)
    assert queue.serve_once(
        capacity=CAPACITY, has_gpu=True, tags=["gb10", "sparklina"],
    ) is None
    assert denial(key)["reason"] == "unified_gpu_memory_unknown"
    assert queue.records.item_path(pool.READY, key).is_file()


def test_a_clear_sample_recovers_and_retires_the_unknown_status(fleet):
    queue, clock, _readings, sample, publish, _tick, _claim, denial = fleet
    unknown(sample)
    key = publish("recovered", gpu=0, mem_gb=4, tags=["gb10"])
    assert direct_claim(fleet) is None
    refused = denial(key)
    assert refused["reason"] == "unified_gpu_memory_unknown"
    sample["foreign_processes"] = []

    claimed = direct_claim(fleet)

    assert claimed is not None and claimed["action_key"] == key
    assert claimed["attempts"] == 0
    assert census._claim_denials_for(claimed, [refused], now=clock.now) == []


@pytest.mark.parametrize("mem_gb,admitted", [(90, True), (91, False)])
def test_known_foreign_bytes_bound_new_cpu_commitments(fleet, mem_gb, admitted):
    queue, _clock, _readings, sample, publish, _tick, _claim, denial = fleet
    sample["cpu_pressure_full"] = 0.0
    sample["foreign_processes"] = [
        {"pid": 9101, "gpu_uuid": "GPU-1", "used_bytes": 30 << 30}]
    key = publish("known-foreign-budget", gpu=0, mem_gb=mem_gb, tags=["gb10"])

    claimed = direct_claim(fleet)

    if admitted:
        assert claimed is not None and claimed["action_key"] == key
    else:
        assert claimed is None
        assert denial(key)["reason"] == "unified_gpu_memory_budget"


def test_discrete_unknown_residency_keeps_the_cpu_memory_policy(fleet):
    _queue, _clock, _readings, sample, publish, _tick, _claim, _denial = fleet
    unknown(sample)
    sample["devices"][0].update(
        memory_domain="discrete", memory_total_bytes=32 << 30,
        memory_used_bytes=4 << 30, memory_free_bytes=28 << 30,
    )
    key = publish("discrete-foreign", gpu=0, mem_gb=4, tags=["gb10"])

    claimed = direct_claim(fleet)

    assert claimed is not None and claimed["action_key"] == key


def test_a_producer_reserves_its_export_allowance_once_against_the_ceiling(
        fleet, tmp_path):
    import test_prepaid_writer_integration as producer_fixture
    from prismabuild import core, output_contract, produced_output, produced_spool

    queue, _clock, _readings, sample, _publish, _tick, _claim, denial = fleet
    sample["cpu_pressure_full"] = 0.0
    sample["foreign_processes"] = [
        {"pid": 9101, "gpu_uuid": "GPU-1", "used_bytes": 30 << 30}]
    template = producer_fixture._template(str(tmp_path / "canonical"))
    cas_root = tmp_path / "cas"
    original = producer_fixture._producer_request(tmp_path, cas_root, template)
    cas, request = produced_output._read_producer_request(cas_root, original)
    request.pop("action_key")
    request["params"]["demand"] = {"cpu": 2, "mem_gb": 90}
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
        tags=["gb10"], resources={
            "cpu": 2, "mem_gb": 90, **output_contract.owner_demand_terms(template)},
        produced_output_template=template,
    )

    assert direct_claim(fleet, adaptive=True) is None
    refused = denial(key)
    assert refused["reason"] == "unified_gpu_memory_budget"
    assert refused["evidence"]["decision"]["candidate_charge_gib"] == 91
    sample["foreign_processes"][0]["used_bytes"] = 29 << 30

    claimed = direct_claim(fleet, adaptive=True)

    assert claimed is not None and claimed["action_key"] == key
    assert queue.ledger().holder_tokens(key)["mem_gb"] == 91


@pytest.mark.parametrize("foreign_gib,candidate_mem,preempts", [
    (30, 16, True), (115, 6, False),
])
def test_preemption_requires_release_to_close_the_physical_memory_gap(
        fleet, foreign_gib, candidate_mem, preempts):
    queue, _clock, _readings, sample, publish, _tick, _claim, denial = fleet
    sample["cpu_pressure_full"] = 0.0
    holder = publish("background-holder", gpu=0, mem_gb=80,
                     tags=["sparklina"], retry_safe=True)
    claimed = direct_claim(fleet)
    assert claimed is not None and claimed["action_key"] == holder
    sample["foreign_processes"] = [
        {"pid": 9101, "gpu_uuid": "GPU-1", "used_bytes": foreign_gib << 30}]
    candidate = publish("foreground-candidate", gpu=0, mem_gb=candidate_mem,
                        tags=["sparklina"], priority=0)

    assert direct_claim(fleet) is None

    assert denial(candidate)["reason"] == "unified_gpu_memory_budget"
    decisions = queue.records.withdrawal_decisions(holder)
    assert bool(decisions) is preempts
    if preempts:
        assert len(decisions) == 1
        assert decisions[0][1]["preempted_by"] == candidate
    assert queue.ledger().holder_tokens(holder)["mem_gb"] == 80
    assert queue.records.item_path(pool.READY, candidate).is_file()


def test_unknown_arriving_during_ordinary_preemption_proof_keeps_the_holder(
        fleet, monkeypatch):
    queue, _clock, _readings, sample, publish, _tick, _claim, denial = fleet
    sample["cpu_pressure_full"] = 0.0
    holder = publish("late-unknown-holder", cpu=19, gpu=0, mem_gb=80,
                     tags=["sparklina"], retry_safe=True)
    assert direct_claim(fleet)["action_key"] == holder
    candidate = publish("late-unknown-candidate", cpu=2, gpu=0, mem_gb=32,
                        tags=["sparklina"], priority=0)
    proofs = queue._preemption_eligibility_proofs

    def foreign_arrives(*args, **kwargs):
        result = proofs(*args, **kwargs)
        unknown(sample)
        sample["sample_id"] = "foreign-arrived-during-proof"
        return result

    monkeypatch.setattr(queue, "_preemption_eligibility_proofs", foreign_arrives)

    assert direct_claim(fleet) is None

    assert queue.records.withdrawal_decisions(holder) == []
    assert denial(candidate)["reason"] == "unified_gpu_memory_unknown"
    assert queue.ledger().holder_tokens(holder)["mem_gb"] == 80
    assert queue.records.item_path(pool.READY, candidate).is_file()


def test_unknown_arriving_during_gang_backfill_proof_preserves_elections(
        fleet, gang_fleet, monkeypatch):
    queue, _clock, _readings, sample, *_ = fleet
    _queue, _gclock, publish, finish, gclaim, denial, members = gang_fleet
    sample["cpu_pressure_full"] = 0.0
    incumbent = publish("gang-partner", measurement=True, tags=["sparky"])
    assert gclaim("sparky") == incumbent
    holder = publish("gang-backfill", priority=-10, mem_gb=24,
                     tags=["sparklina"], retry_safe=True)
    assert gclaim("sparklina") == holder
    group, keys = members("late-unknown-gang", priority=10, mem_gb=80)
    assert gclaim("sparky") is None
    assert gclaim("sparklina") is None
    election = _gang.elections(queue, group, 2)[0]
    path = queue.records.item_path(pool.CLAIMED, holder)
    row = pool.record_store.read_document(path)
    row["gang_backfill"] = [election]
    pool.immutable_files.write_json_atomic(path, row)
    finish(incumbent, "sparky")
    assert gclaim("sparky") is None
    elections = _gang.elections(queue, group, 2)
    proofs = queue._preemption_eligibility_proofs

    def foreign_arrives(*args, **kwargs):
        result = proofs(*args, **kwargs)
        unknown(sample)
        sample["sample_id"] = "foreign-arrived-during-gang-proof"
        return result

    monkeypatch.setattr(queue, "_preemption_eligibility_proofs", foreign_arrives)

    assert gclaim("sparklina") is None

    assert queue.records.withdrawal_decisions(holder) == []
    assert denial(keys[0])["reason"] == "unified_gpu_memory_unknown"
    assert _gang.elections(queue, group, 2) == elections
    assert queue.ledger("sparklina").held_keys() == [holder]
    assert queue.records.item_path(pool.READY, keys[0]).is_file()


@pytest.mark.parametrize("peer_elected", [False, True])
def test_unknown_arriving_during_prelaunch_proof_does_not_create_an_election(
        fleet, gang_fleet, monkeypatch, peer_elected):
    queue, _clock, _readings, sample, *_ = fleet
    _queue, _gclock, _publish, _finish, gclaim, denial, members = gang_fleet
    sample["cpu_pressure_full"] = 0.0
    group, keys = members("late-unknown-election", mem_gb=80)
    if peer_elected:
        assert gclaim("sparky") is None
        assert _gang.elections(queue, group, 2)
    elections = _gang.elections(queue, group, 2)
    ready = {key: queue.records.item_path(pool.READY, key).read_bytes() for key in keys}
    prelaunch = queue._gang_prelaunch_blocker
    observed = []

    def foreign_arrives(*args, **kwargs):
        result = prelaunch(*args, **kwargs)
        observed.append(result)
        unknown(sample)
        sample["sample_id"] = "foreign-arrived-during-prelaunch"
        return result

    monkeypatch.setattr(queue, "_gang_prelaunch_blocker", foreign_arrives)

    assert gclaim("sparklina") is None

    assert observed == [None]
    assert _gang.elections(queue, group, 2) == elections
    assert denial(keys[0])["reason"] == "unified_gpu_memory_unknown"
    assert queue.ledger("sparklina").held_keys() == []
    for key in keys:
        assert queue.records.item_path(pool.READY, key).read_bytes() == ready[key]

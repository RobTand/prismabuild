"""Replay #2014's claim decision with a sealed, interpreter-naming CPU shard."""
from types import SimpleNamespace
import sys

import pytest

from prismabuild import adaptive_cpu, candidate_policy, capability_eligibility as eligibility, contracts, core, pool
from prismabuild._peer_placement import PlacementHistory, PlacementPass, PeerCapacity, PeerObservation
from test_core import _body


CAPACITY = {"cpu": 8, "mem_gb": 16, "gpu": 1}
CPU_CAPACITY = {**CAPACITY, "gpu": 0}
NOW = 1000.


def interpreter_case(tmp_path, monkeypatch, answer, *, reserved=False, **fields):
    monkeypatch.setattr(pool, "_now", lambda: NOW)
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "spark")
    queue = pool.PoolQueue(tmp_path / "queue")
    body = _body(tmp_path, argv=[sys.executable, "-m", "pytest", "tests/"])
    body["params"]["interpreter"] = sys.executable
    body["params"]["retry_policy"] = {
        "retry_safe": True, "retry_scope": "interruption", "max_attempts": 3}
    action = core.seal_action(body)
    cas = core.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
        checkout_root=tmp_path, worker_script=tmp_path / "worker.py",
        interpreter=sys.executable, resources={"cpu": 2, "mem_gb": 2},
        retry_safe=True, retry_scope="interruption", max_attempts=3, **fields)
    item = queue.ready_items()[0]
    for host, gpu in (("cpu-peer", False), ("spark", True)):
        capacity = CAPACITY if gpu else CPU_CAPACITY
        queue.ledger(host).ensure_capacity(capacity)
        present = gpu or answer == "present"
        queue.announce(host=host, has_gpu=gpu, tags=[host, contracts.INTERPRETER_TAG],
            capacity=capacity, observed_capacity=capacity,
            capabilities=eligibility.OfferCapabilities(interpreters=eligibility.InterpreterAnswers(
                frozenset([sys.executable] if present else []),
                frozenset([sys.executable] if not gpu and answer == "absent" else []))),
            observed_detail={"observed_unix": NOW, "gpu_capacity_trusted": True,
                "foreign_gpu_processes": 0, "gpu_sampled_unix": NOW})
    if reserved:
        assert queue.ledger("spark").acquire("e" * 64, {"gpu": 1})
    identity = f"{item['action_key']}:{float(item['published_unix'])!r}"
    peer = PeerObservation(PeerCapacity(dict(CPU_CAPACITY), dict(CPU_CAPACITY), 8), {
        identity: {"host": "cpu-peer", "reason": "transition_busy"}})
    placement = PlacementPass(queue.offers, lambda host, offer: peer,
        history=PlacementHistory(), wall_clock=lambda: NOW)
    demand = item["resources"]
    candidate = SimpleNamespace(item=item,
        demand=pool._CandidateDemand(demand, demand, demand, demand, {}),
        scratch_intent=None, dependent_owner=adaptive_cpu._UNREAD)
    return queue, item, candidate, placement, peer


def prepare_interpreter_claim(queue, item, candidate, placement, *, ready_gpu=None):
    return queue._prepare_host_claim(candidate, candidate_policy.ScanOrder(),
        total=CAPACITY, has_gpu=True, controller=None, gpu_controller=None,
        placement=placement, ready=[item], ready_gpu=ready_gpu)


@pytest.mark.parametrize("answer", ["present", "absent", "unknown"])
@pytest.mark.parametrize("reserved", [False, True])
def test_interpreter_evidence_never_bypasses_guest_or_deferral(
        tmp_path, monkeypatch, answer, reserved):
    queue, item, candidate, placement, peer = interpreter_case(
        tmp_path, monkeypatch, answer, reserved=reserved)
    assert queue._guest_restartable(item)
    assert not queue._cpu_host_passed_on(item, "cpu-peer", peer)
    # Claim eligibility remains strict, independently of placement policy.
    matched = queue._matching_offers(item, live=placement.offers())
    assert any(offer["host"] == "cpu-peer" for offer in matched) is (answer == "present")
    result = prepare_interpreter_claim(queue, item, candidate, placement)
    if reserved and answer != "absent":
        assert isinstance(result, candidate_policy.CandidateDenial)
        assert result.reason == ("deferred_for_unresolved_cpu_host" if answer == "unknown"
                                 else "deferred_for_cpu_only_host")
    else:
        assert isinstance(result, pool._HostClaimRequest)
        assert result.cpu_guest is (answer != "absent")


@pytest.mark.parametrize("fault", ["unreadable-cas", "contradictory-prefix"])
def test_unknown_peer_cannot_turn_unproven_restartability_into_a_guest(
        tmp_path, monkeypatch, fault):
    queue, item, candidate, placement, _ = interpreter_case(tmp_path, monkeypatch, "unknown")
    if fault == "unreadable-cas":
        item["cas_root"] = str(tmp_path / "missing")
    else:
        item["attempt_history_missing_before"] = 1
    assert not queue._guest_restartable(item)
    result = prepare_interpreter_claim(queue, item, candidate, placement)
    assert isinstance(result, candidate_policy.CandidateDenial)
    assert result.reason == "deferred_for_unresolved_cpu_host"


def test_unknown_deferral_has_no_timer_and_ends_when_the_host_goes_stale(tmp_path, monkeypatch):
    queue, item, candidate, placement, peer = interpreter_case(
        tmp_path, monkeypatch, "unknown", reserved=True)
    assert prepare_interpreter_claim(queue, item, candidate, placement).reason == (
        "deferred_for_unresolved_cpu_host")
    monkeypatch.setattr(pool.time, "monotonic", lambda: 1.e9)
    assert prepare_interpreter_claim(queue, item, candidate, placement).reason == (
        "deferred_for_unresolved_cpu_host")
    monkeypatch.setattr(pool, "_now", lambda: NOW + pool.OFFER_TIMEOUT_S + 1)
    fresh_pass = PlacementPass(queue.offers, lambda host, offer: peer,
        history=PlacementHistory(), wall_clock=pool._now)
    result = prepare_interpreter_claim(queue, item, candidate, fresh_pass)
    assert isinstance(result, pool._HostClaimRequest)
    assert not result.cpu_guest


@pytest.mark.parametrize("peer_state", ["full", "passed", "unreadable"])
def test_unknown_waits_for_an_interpreter_answer_before_capacity_or_pass_on(
        tmp_path, monkeypatch, peer_state):
    queue, item, candidate, placement, peer = interpreter_case(
        tmp_path, monkeypatch, "unknown", reserved=True)
    if peer_state == "full":
        peer.charge({"cpu": 8})
    elif peer_state == "passed":
        next(iter(peer.denials.values()))["reason"] = "reservation_unavailable"
    else:
        placement = PlacementPass(queue.offers, lambda host, offer: None,
            history=PlacementHistory(), wall_clock=lambda: NOW)
    assert prepare_interpreter_claim(queue, item, candidate, placement).reason == (
        "deferred_for_unresolved_cpu_host")


@pytest.mark.parametrize("fault", ["foreign", "untrusted", "missing", "stale", "ready-gpu"])
def test_unknown_peer_requires_positive_idle_evidence_for_a_guest(tmp_path, monkeypatch, fault):
    queue, item, candidate, placement, _ = interpreter_case(tmp_path, monkeypatch, "unknown")
    offer = next(offer for offer in placement.offers() if offer["host"] == "spark")
    detail = offer["observed_detail"]
    if fault == "foreign":
        detail["foreign_gpu_processes"] = 1
    elif fault == "untrusted":
        detail["gpu_capacity_trusted"] = False
    elif fault == "missing":
        detail.pop("gpu_sampled_unix")
    elif fault == "stale":
        detail["gpu_sampled_unix"] -= 100
    result = prepare_interpreter_claim(queue, item, candidate, placement,
        ready_gpu={"action_key": "b" * 64} if fault == "ready-gpu" else None)
    assert result.reason == "deferred_for_unresolved_cpu_host"


@pytest.mark.parametrize("fault", ["tag", "capacity", "gpu-unknown", "gpu-present"])
def test_possible_cpu_peer_must_match_every_other_requirement(tmp_path, monkeypatch, fault):
    queue, item, candidate, placement, _ = interpreter_case(tmp_path, monkeypatch, "unknown")
    offer = next(offer for offer in placement.offers() if offer["host"] == "cpu-peer")
    if fault == "tag":
        offer["tags"] = []
    elif fault == "capacity":
        offer["capacity"] = {"cpu": 0, "mem_gb": 16}
    elif fault == "gpu-unknown":
        offer.pop("has_gpu")
    else:
        offer["has_gpu"] = True
    result = prepare_interpreter_claim(queue, item, candidate, placement)
    assert isinstance(result, pool._HostClaimRequest)
    assert not result.cpu_guest


@pytest.mark.parametrize("tag", ["spark", "gb10"])
def test_unknown_peer_does_not_change_explicitly_tagged_placement(tmp_path, monkeypatch, tag):
    queue, item, candidate, placement, _ = interpreter_case(tmp_path, monkeypatch, "unknown", tags=[tag])
    result = prepare_interpreter_claim(queue, item, candidate, placement)
    assert isinstance(result, pool._HostClaimRequest)
    assert not result.cpu_guest


@pytest.mark.parametrize("priority", [-20, 20])
def test_gpu_evicts_an_unknown_peer_guest_across_priority_bands(tmp_path, monkeypatch, priority):
    queue, item, _, _, _ = interpreter_case(tmp_path, monkeypatch, "unknown", priority=priority)
    tiers = {"preferred": list(range(8)), "fallback": []}
    queue.ledger("spark").configure_cpu_tiers(tiers)
    claim = lambda: queue.claim(tags=[contracts.INTERPRETER_TAG, "spark"],
        has_gpu=True, capacity=CAPACITY, cpu_tiers=tiers)
    guest = claim()
    assert guest["cpu_guest"] is True
    gpu_key = "b" * 64
    queue.publish(action_key=gpu_key, cas_root=tmp_path / "cas", checkout_root=tmp_path,
        worker_script=tmp_path / "worker.py", resources={"cpu": 8, "gpu": 1}, priority=0)
    assert claim() is None
    decisions = [row for _, row in queue.records.withdrawal_decisions(item["action_key"])]
    assert len(decisions) == 1 and decisions[0]["preempted_by"] == gpu_key
    queue.finish(item["action_key"], status="withdrawn", detail={"returncode": -15},
        claim_snapshot=guest)
    assert claim()["action_key"] == gpu_key


def test_generated_pbtest_shard_is_guest_with_unknown_cpu_interpreter(tmp_path, monkeypatch):
    from test_pbtest_seals_interpreter_1462 import _sealed

    submission = tmp_path / "submission"
    submission.mkdir()
    _, row, _ = _sealed(submission, monkeypatch)
    queue, item, candidate, placement, _ = interpreter_case(tmp_path, monkeypatch, "unknown")
    candidate.item = row
    demand = row["resources"]
    candidate.demand = pool._CandidateDemand(demand, demand, demand, demand, {})
    assert row["interpreter"] == sys.executable
    assert row["retry_scope"] == "interruption" and row["max_attempts"] == 3
    result = prepare_interpreter_claim(queue, row, candidate, placement)
    assert isinstance(result, pool._HostClaimRequest)
    assert result.cpu_guest

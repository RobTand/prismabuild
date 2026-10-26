"""Claim reporting preserves dependencies before any candidate can be denied."""
import json

import pytest

from fleet_world import ActionRows
from prismabuild import adaptive_cpu, claim_wait, host_hold, pool, resource_scope, runtime_publication
from prismabuild import _peer_placement as placement
from prismabuild import _gang


def test_empty_claim_reports_ready_and_control_dependencies(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    observed = queue.claim_or_observe(ready=[])
    assert observed.value is None
    assert {source.kind.value for source in observed.blocked_on.inputs} >= {
        "queue", "control", "host_ledger"}
    assert observed.blocked_on.coverage.complete


def test_admission_contention_reports_a_cancellable_retry(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(queue, "_claim", lambda **kwargs: (_ for _ in ()).throw(
        pool.cpu_admission.AdmissionBusy()))
    observed = queue.claim_or_observe(ready=[])
    assert observed.value is None
    assert "admission_busy" in observed.blocked_on.reasons
    assert any(deadline.reason == "lock_retry" for deadline in observed.blocked_on.deadlines)


@pytest.fixture
def clocks(monkeypatch):
    clock = {"unix": 1000.0, "monotonic": 40.0}
    monkeypatch.setattr(pool, "_now", lambda: clock["unix"])
    monkeypatch.setattr(pool.time, "monotonic", lambda: clock["monotonic"])
    monkeypatch.setattr(adaptive_cpu.time, "time", lambda: clock["unix"])
    return clock


def _fail_if_called(*args, **kwargs):
    raise AssertionError("the early refusal entered candidate scanning")


@pytest.mark.parametrize("preference", list(placement.TimedPreference))
def test_report_uses_the_preference_start_across_pass_handoff_and_hints(
        tmp_path, monkeypatch, clocks, preference):
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(runtime_publication, "live_authority", lambda: False)
    monkeypatch.setattr(queue, "_ready_gpu_for_host", lambda *args, **kwargs: None)
    row = {"action_key": "a" * 64, "published_unix": 1000.0,
           "passes": 0, "priority": 0, "tags": [], "needs_gpu": False,
           "resources": {"cpu": 1, "mem_gb": 1}}
    generation = ("a" * 64, "1000.0")
    clocks["monotonic"] = 10.0
    for instant in (10.0, 17.0, 29.999):
        clocks["monotonic"] = instant
        with claim_wait.capture(queue) as collected:
            scan = queue._start_claim_scan(ready=[row])
            assert not scan.placement.timer_expired(preference, generation)
            claim_wait.observe(queue, claim_wait.SourceKind.ACTION, "unrelated-hint")
        assert collected.observation().blocked_on.deadlines == (
            claim_wait.RetryDeadline(preference.value + "_preference", 30.0),)
    clocks["monotonic"] = 30.0
    with claim_wait.capture(queue) as collected:
        scan = queue._start_claim_scan(ready=[row])
        assert scan.placement.timer_expired(preference, generation)
    assert collected.observation().blocked_on.deadlines == ()


def test_cpu_host_yield_evidence_cannot_create_a_preference_timeout(tmp_path, clocks):
    queue = pool.PoolQueue(tmp_path / "queue")
    history = placement.PlacementHistory()
    generation = ("a" * 64, "1000.0")
    capacity = placement.PeerCapacity({"cpu": 2}, {"cpu": 2}, 2)
    view = placement.PlacementPass(
        lambda: [], lambda host, offer: None, history=history,
        wall_clock=lambda: clocks["unix"],
        on_deadline=lambda preference, generation, due: claim_wait.deadline(
            queue, "unexpected_1262_timeout", due))
    with claim_wait.capture(queue) as collected:
        assert view.cpu_host_evidence(generation, "peer", capacity, {"cpu": 1})["yielding_s"] == 0.0
        clocks["unix"] = 1_000_000.0
        assert view.cpu_host_evidence(generation, "peer", capacity, {"cpu": 1})["yielding_s"] == 999000.0
    assert collected.observation().blocked_on.deadlines == ()


def test_initial_cpu_counter_baseline_and_cached_sample_report_the_same_next_deadline(
        tmp_path, monkeypatch, clocks):
    queue = pool.PoolQueue(tmp_path / "queue")
    controller = adaptive_cpu.Controller(
        queue.ledger(), {"preferred": [0], "fallback": []})
    monkeypatch.setattr(controller, "base", tmp_path / "adaptive")
    monkeypatch.setattr(adaptive_cpu, "counters", lambda cpus: {
        "sampled_unix": 1000.0, "cpus": {"0": [10, 100]}, "psi_total": 0})
    monkeypatch.setattr(adaptive_cpu, "control_plane_counters", lambda cpus: {})
    monkeypatch.setattr(adaptive_cpu, "boot_ticks", lambda: 100)
    clocks["unix"] = 1000.5
    assert controller.sample() == {}
    assert controller.next_sample_unix == 1001.0
    with claim_wait.capture(queue, wall_clock=lambda: clocks["unix"],
                            monotonic_clock=lambda: clocks["monotonic"]) as collected:
        queue._observe_cpu_sample(controller)
    result = collected.observation().blocked_on
    assert claim_wait.RetryDeadline("cpu_baseline_next_sample", 40.5) in result.deadlines
    assert any(source.kind is claim_wait.SourceKind.RESOURCE for source in result.inputs)
    monkeypatch.setattr(adaptive_cpu, "counters", _fail_if_called)
    clocks.update(unix=1000.75, monotonic=40.25)
    assert controller.sample() == {}
    assert controller.next_sample_unix == 1001.0
    with claim_wait.capture(queue, wall_clock=lambda: clocks["unix"],
                            monotonic_clock=lambda: clocks["monotonic"]) as collected:
        queue._observe_cpu_sample(controller)
    assert claim_wait.RetryDeadline("cpu_baseline_next_sample", 40.5) in (
        collected.observation().blocked_on.deadlines)


def test_operator_hold_reports_expiry_before_candidate_scanning(tmp_path, monkeypatch, clocks):
    queue = pool.PoolQueue(tmp_path / "queue")
    host = pool.socket.gethostname()
    host_hold.change_host_hold(queue.root, host, owner="operator", reason="maintenance",
                               expires_unix=1030.0, now=1000.0)
    monkeypatch.setattr(queue, "_claim", _fail_if_called)
    observed = queue.claim_or_observe(ready=[])
    assert observed.value is None
    assert "operator_held" in observed.blocked_on.reasons
    assert claim_wait.WakeInput(claim_wait.SourceKind.CONTROL, "hold:" + host) in observed.blocked_on.inputs
    assert claim_wait.RetryDeadline("operator_hold_expiry", 70.0) in observed.blocked_on.deadlines
    assert queue.claim(ready=[]) is None


def test_admission_unavailable_reports_recovery_even_without_a_ready_row(
        tmp_path, monkeypatch, clocks):
    queue = pool.PoolQueue(tmp_path / "queue")
    def unavailable(**kwargs):
        raise adaptive_cpu.AdmissionUnavailable("missing host exclusion identity")
    monkeypatch.setattr(queue, "_claim", unavailable)
    observed = queue.claim_or_observe(ready=[])
    assert observed.value is None
    assert "host_admission_unavailable" in observed.blocked_on.reasons
    assert not observed.blocked_on.coverage.complete
    assert claim_wait.RetryDeadline("source_recovery", 45.0) in observed.blocked_on.deadlines


def test_empty_adaptive_snapshot_reports_dependencies_without_entering_the_scan(
        tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(queue, "_claim", _fail_if_called)
    observed = queue.claim_or_observe(
        ready=[], adaptive_cpu=True, capacity={"cpu": 1, "mem_gb": 1},
        cpu_tiers={"preferred": [0], "fallback": []})
    assert observed.value is None
    assert {source.kind.value for source in observed.blocked_on.inputs} >= {
        "queue", "control", "host_ledger"}
    assert observed.blocked_on.coverage.complete


def test_maintenance_deferral_reports_recovery_and_preserves_the_unstarted_attempt(
        tmp_path, monkeypatch, clocks):
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "a" * 64
    ActionRows(queue).publish(key, resources={"cpu": 1, "mem_gb": 1}, max_attempts=1)
    monkeypatch.setattr(queue, "_sweep_due", lambda: False)
    def deferred(*args, **kwargs):
        raise resource_scope.ResourceUnavailable("maintenance")
    monkeypatch.setattr(queue, "execute", deferred)
    observed = queue.serve_or_observe_once(capacity={"cpu": 1, "mem_gb": 1}, containment=True)
    assert observed.value is None
    assert claim_wait.WakeInput(claim_wait.SourceKind.CONTROL, "maintenance") in observed.blocked_on.inputs
    assert "resource_scope_unavailable" in observed.blocked_on.coverage.gaps
    assert claim_wait.RetryDeadline("source_recovery", 41.0) in observed.blocked_on.deadlines
    ready = json.loads(queue.records.item_path(pool.record_store.READY, key).read_text())
    assert ready["attempts"] == 0
    assert "claimed_by" not in ready
    assert queue.ledger().held() == {}
    assert not queue.records.lease_path(key).exists()


def test_reporting_and_legacy_claims_return_the_same_authoritative_claim_fields(tmp_path):
    claims = []
    for name, reporting in (("legacy", False), ("reporting", True)):
        queue = pool.PoolQueue(tmp_path / name)
        key = "b" * 64
        ActionRows(queue).publish(key, resources={"cpu": 1, "mem_gb": 1})
        result = (queue.claim_or_observe(capacity={"cpu": 1, "mem_gb": 1}).value
                  if reporting else queue.claim(capacity={"cpu": 1, "mem_gb": 1}))
        assert result is not None
        assert queue.ledger().held() == {"cpu": 1, "mem_gb": 1}
        filed = json.loads(queue.records.item_path(pool.record_store.CLAIMED, key).read_text())
        assert result == filed
        claims.append({field: result[field] for field in ("action_key", "resources", "attempts", "served_from")})
    assert claims[0] == claims[1]
    assert claims[0]["attempts"] == 0
    assert claims[0]["served_from"] == "canonical"


def test_carried_withhold_reports_original_deadlines_before_diagnostic_validation(
        tmp_path, clocks):
    queue = pool.PoolQueue(tmp_path / "queue")
    carried = {"epoch_unix": 990.0, "drain_until_unix": 1040.0,
               "unmeasured_until_unix": 1030.0, "measurement_cpu_sampled_unix": 999.0}
    with claim_wait.capture(queue, wall_clock=lambda: clocks["unix"],
                            monotonic_clock=lambda: clocks["monotonic"]) as collected:
        queue.record_denial({"action_key": "a" * 64}, "transition_busy",
                            {"withhold_carried": carried})
    assert set(collected.observation().blocked_on.deadlines) >= {
        claim_wait.RetryDeadline("measurement_drain_expiry", 80.0),
        claim_wait.RetryDeadline("unmeasured_drain_expiry", 70.0),
        claim_wait.RetryDeadline("withhold_cpu_freshness", 44.0)}
    with claim_wait.capture(queue, wall_clock=lambda: clocks["unix"],
                            monotonic_clock=lambda: clocks["monotonic"]) as collected:
        queue.record_denial({"action_key": "a" * 64}, "transition_busy",
                            {"withhold_carried": {"epoch_unix": 990.0}})
    assert claim_wait.RetryDeadline("withhold_ceiling", 930.0) in (
        collected.observation().blocked_on.deadlines)


def test_resident_preference_uses_publication_plus_thirty_despite_new_hints(
        tmp_path, monkeypatch, clocks):
    queue = pool.PoolQueue(tmp_path / "queue")
    monkeypatch.setattr(queue, "resident_copy_hosts", lambda set_id: {"peer"})
    monkeypatch.setattr(queue, "_matching_offers", lambda item, live: [{"host": "peer"}])
    monkeypatch.setattr(_gang, "elections", lambda *args: {})
    row = {"resident_set": "a" * 64, "published_unix": 995.0}
    for unix, monotonic in ((1000.0, 40.0), (1010.0, 50.0), (1024.999, 64.999)):
        clocks.update(unix=unix, monotonic=monotonic)
        with claim_wait.capture(queue, wall_clock=lambda: clocks["unix"],
                                monotonic_clock=lambda: clocks["monotonic"]) as collected:
            assert _gang.resident_hold(queue, {"group": "b" * 32, "size": 2},
                {"index": 0}, row, "local", unix, live=[], prefer_s=30.0) is not None
        assert collected.observation().blocked_on.deadlines == (
            claim_wait.RetryDeadline("resident_copy_preference", 65.0),)
    clocks.update(unix=1025.0, monotonic=65.0)
    with claim_wait.capture(queue) as collected:
        assert _gang.resident_hold(queue, {"group": "b" * 32, "size": 2},
            {"index": 0}, row, "local", 1025.0, live=[], prefer_s=30.0) is None
    assert collected.observation().blocked_on.deadlines == ()


def test_gang_terminal_confirmation_keeps_its_first_observation_deadline(tmp_path, clocks):
    queue = pool.PoolQueue(tmp_path / "queue")
    for unix, monotonic in ((1000.0, 40.0), (1003.0, 43.0)):
        clocks.update(unix=unix, monotonic=monotonic)
        with claim_wait.capture(queue, wall_clock=lambda: clocks["unix"],
                                monotonic_clock=lambda: clocks["monotonic"]) as collected:
            assert not _gang.note_terminal(queue, "b" * 32, "a" * 64, "same-ending", now=unix)
        assert collected.observation().blocked_on.deadlines == (
            claim_wait.RetryDeadline("gang_terminal_confirmation", 160.0),)

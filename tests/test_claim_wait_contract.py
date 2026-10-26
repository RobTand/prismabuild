"""The claim report names finite refusal domains and never invents policy time."""
from dataclasses import FrozenInstanceError
import ast
from pathlib import Path

import pytest

from prismabuild import candidate_policy, claim_wait


@pytest.fixture
def observation():
    queue = object()
    with claim_wait.capture(queue, wall_clock=lambda: 1000.0,
                            monotonic_clock=lambda: 40.0) as collector:
        yield queue, collector


def test_capture_is_queue_scoped_and_restores_the_outer_capture(observation):
    queue, outer = observation
    other = object()
    claim_wait.observe(other, claim_wait.SourceKind.ACTION, "foreign")
    with claim_wait.capture(other) as inner:
        claim_wait.observe(queue, claim_wait.SourceKind.ACTION, "hidden")
        claim_wait.observe(other, claim_wait.SourceKind.ACTION, "inner")
    claim_wait.observe(queue, claim_wait.SourceKind.ACTION, "outer")
    assert {entry.identity for entry in outer.observation().blocked_on.inputs
            if entry.kind is claim_wait.SourceKind.ACTION} == {"outer"}
    assert {entry.identity for entry in inner.observation().blocked_on.inputs
            if entry.kind is claim_wait.SourceKind.ACTION} == {"inner"}


def test_snapshot_is_frozen_and_does_not_retain_later_observations(observation):
    queue, collector = observation
    before = collector.observation()
    claim_wait.observe(queue, claim_wait.SourceKind.TIER, "fast")
    assert all(entry.kind is not claim_wait.SourceKind.TIER for entry in before.blocked_on.inputs)
    assert any(entry.kind is claim_wait.SourceKind.TIER for entry in collector.observation().blocked_on.inputs)
    with pytest.raises(FrozenInstanceError):
        before.blocked_on.coverage.gaps = ("changed",)


def test_wall_deadline_translates_once_without_changing_its_policy_start(observation):
    queue, collector = observation
    claim_wait.unix_deadline(queue, "resident_copy", 1030.0)
    claim_wait.deadline(queue, "preferred_cpu", 60.0)
    assert collector.observation().blocked_on.deadlines == (
        claim_wait.RetryDeadline("preferred_cpu", 60.0),
        claim_wait.RetryDeadline("resident_copy", 70.0),
    )


@pytest.mark.parametrize("state", [claim_wait.EvidenceState.UNKNOWN,
                                   claim_wait.EvidenceState.UNREADABLE])
def test_unavailable_source_cannot_enter_an_indefinite_wait(observation, state):
    queue, collector = observation
    claim_wait.observe(queue, claim_wait.SourceKind.RESIDENCY, "lead", state)
    result = collector.observation().blocked_on
    assert claim_wait.WakeInput(claim_wait.SourceKind.RESIDENCY, "lead", state) in result.inputs
    assert not result.coverage.complete
    assert claim_wait.RetryDeadline("source_recovery", 45.0) in result.deadlines


def test_unknown_refusal_has_bounded_recovery_and_a_coverage_diagnostic(observation, caplog):
    queue, collector = observation
    claim_wait.refusal(queue, {"action_key": "a", "published_unix": 12.0}, "new_policy_reason")
    result = collector.observation().blocked_on
    assert result.coverage.gaps == ("new_policy_reason",)
    assert result.deadlines == (claim_wait.RetryDeadline("source_recovery", 45.0),)
    assert "Unmapped claim refusal: new_policy_reason" in caplog.text


def test_unknown_nested_admission_reason_is_not_hidden_by_known_outer_refusal(observation, caplog):
    queue, collector = observation
    claim_wait.refusal(queue, {"action_key": "a"}, "adaptive_cpu_refused",
                       {"decision": {"reason": "new_admission_gate"}})
    result = collector.observation().blocked_on
    assert result.coverage.gaps == ("new_admission_gate",)
    assert claim_wait.RetryDeadline("source_recovery", 45.0) in result.deadlines
    assert "Unmapped claim decision: new_admission_gate" in caplog.text


@pytest.mark.parametrize("reason", ["transition_busy", "admission_busy"])
def test_locks_report_a_cancellable_retry_without_waiting(observation, reason):
    queue, collector = observation
    claim_wait.refusal(queue, {"action_key": "a"}, reason)
    result = collector.observation().blocked_on
    assert result.deadlines == (claim_wait.RetryDeadline("lock_retry", 41.0),)
    assert result.coverage.complete


def test_1262_yield_has_peer_dependencies_and_no_policy_timer(observation):
    queue, collector = observation
    claim_wait.refusal(queue, {"action_key": "a", "published_unix": 12.0,
                              "attempts": 2}, "deferred_for_cpu_only_host",
                       {"cpu_host": {"yielding_s": 999999}})
    result = collector.observation().blocked_on
    assert {entry.kind for entry in result.inputs} >= {
        claim_wait.SourceKind.PEER, claim_wait.SourceKind.HOST_LEDGER,
        claim_wait.SourceKind.RESOURCE}
    assert result.deadlines == ()
    assert result.coverage.complete
    assert claim_wait.WakeInput(claim_wait.SourceKind.ACTION, "a:12.0:2") in result.inputs


@pytest.mark.parametrize(("reason", "required"), [
    ("placement_mismatch", {"action"}),
    ("operator_held", {"control", "capability"}),
    ("deferred_for_preferred_cpu", {"peer", "resource", "host_ledger"}),
    ("reservation_unavailable_withholding", {"host_ledger", "ownership"}),
    ("adaptive_gpu_refused", {"resource", "host_ledger"}),
    ("dependency_not_present", {"capability"}),
    ("gang_waiting_for_peers", {"group", "action", "host_ledger"}),
    ("residency_lead_terminal", {"residency", "tier", "ownership", "receipt"}),
    ("deferred_for_pool_readers", {"tier", "receipt", "progress", "ownership"}),
])
def test_refusal_families_report_the_sources_that_can_change_them(observation, reason, required):
    queue, collector = observation
    claim_wait.refusal(queue, {"action_key": "a"}, reason)
    result = collector.observation().blocked_on
    assert {entry.kind.value for entry in result.inputs} >= required
    assert result.coverage.complete


def test_source_literals_cannot_silently_add_an_unmapped_refusal():
    source = Path(claim_wait.__file__).parent
    reasons = set()
    for name in ("pool.py", "candidate_policy.py", "capability_eligibility.py"):
        for node in ast.walk(ast.parse((source / name).read_text())):
            if not isinstance(node, ast.Call):
                continue
            function = node.func.attr if isinstance(node.func, ast.Attribute) else (
                node.func.id if isinstance(node.func, ast.Name) else "")
            index = 1 if function == "record_denial" else 0
            if function not in ("record_denial", "CandidateDenial", "CapabilityRefusal"):
                continue
            if len(node.args) > index and isinstance(node.args[index], ast.Constant):
                reason = node.args[index].value
                if isinstance(reason, str):
                    reasons.add(reason)
    assert reasons
    assert reasons - claim_wait.REFUSAL_SOURCES.keys() == set()


def test_finite_dynamic_refusal_domains_are_covered_without_open_prefix_matches():
    assert all(claim_wait.known_refusal("residency_" + state)
               for state in candidate_policy.RESIDENCY_REFUSAL_STATES)
    for prefix in ("adaptive_cpu_refused", "adaptive_gpu_refused", "adaptive_refused",
                   "reservation_unavailable"):
        for suffix in ("_withholding", "_past_ceiling", "_starved"):
            assert claim_wait.known_refusal(prefix + suffix)
    assert not claim_wait.known_refusal("residency_new_state")
    assert not claim_wait.known_refusal("adaptive_cpu_refused_new_suffix")


def test_recovery_caps_the_unavailable_source_budget(observation):
    queue, collector = observation
    claim_wait.recovery(queue, "missing_callback", delay_s=100.0)
    assert collector.observation().blocked_on.deadlines == (
        claim_wait.RetryDeadline("source_recovery", 45.0),)


def test_nested_admission_literals_cannot_silently_add_an_unmapped_reason():
    source = Path(claim_wait.__file__).parent
    reasons = set()
    for name in ("adaptive_cpu.py", "adaptive_gpu.py"):
        for node in ast.walk(ast.parse((source / name).read_text())):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                    and node.func.id == "refuse" and node.args
                    and isinstance(node.args[0], ast.Constant)):
                reasons.add(node.args[0].value)
    assert reasons - claim_wait.DECISION_REASONS == set()

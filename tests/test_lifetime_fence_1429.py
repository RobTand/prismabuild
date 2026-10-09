"""PB #1429: the opt-in lifetime fence bounds timed backfill; payload timeout does not.

Real PB fixtures delay or fail every applicable phase and prove the
declared finite predicate never holds without its enforcement and
evidence. A short sealed payload timeout is opportunity metadata, not
an admission-to-resource-release bound. Only a verified fence strictly
before the original opportunity permits timed backfill. Equality and
later bounds refuse. Capacity and isolation gates stay in force.
"""
from __future__ import annotations

import math
import platform
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
from prismabuild import _measurement_reservation as reservation  # noqa: E402
from prismabuild import adaptive_gpu, core as pb, lifetime_fence, pool  # noqa: E402
from test_measurement_drains_gpu_backfill import fleet as fleet_fixture  # noqa: E402
from test_measurement_reservation_backfill_1419 import (  # noqa: E402
    _assert_holder_unchanged,
    _bounded_measurement_wait,
    _holder_snapshot,
    _observe_real_sharing_permission,
)

fleet = fleet_fixture


def test_lifetime_fence_seal_is_versioned_and_bounded():
    import tempfile

    tmp = Path(tempfile.mkdtemp())
    (tmp / "task.py").write_text("print('fence')\n")

    def body(params):
        return {
            "schema": pb.ACTION_SCHEMA_V2,
            "task": {"definition_id": "tests/lifetime", "definition_version": "v1",
                     "task_class": "generation", "determinism": "deterministic",
                     "artifact_family": "generic", "artifact_kind": "generic",
                     "argv": [sys.executable], "working_directory": ".",
                     "result_path": "result"},
            "inputs": [], "code_closure": pb.build_code_closure(tmp, ["task.py"]),
            "params": params,
            "environment": {"variables": {}, "toolchain": {}},
            "execution_scope": {"portability": "portable", "platform_key": None,
                                "host_class": None},
        }

    fenced = pb.seal_action(body(
        {"lifetime": {"schema": lifetime_fence.LIFETIME_SCHEMA_V1, "fence_s": 300}}))
    assert pb.action_lifetime(fenced) == {
        "schema": lifetime_fence.LIFETIME_SCHEMA_V1, "fence_s": 300.0}
    assert lifetime_fence.required_tags(pb.action_lifetime(fenced)) == [
        lifetime_fence.LIFETIME_TAG]
    # An unfenced twin seals byte-identical params to before the flag existed.
    plain = pb.seal_action(body({}))
    assert "lifetime" not in plain["params"]
    assert pb.action_lifetime(plain) is None
    assert lifetime_fence.required_tags(None) == []
    # Out-of-range and malformed fences refuse at seal time.
    for bad in (5.0, 10 ** 9, 0.0, -3.0, "300", float("nan"), float("inf"), True):
        with pytest.raises(pb.ActionContractError):
            pb.seal_action(body(
                {"lifetime": {"schema": lifetime_fence.LIFETIME_SCHEMA_V1,
                              "fence_s": bad}}))
    with pytest.raises(pb.ActionContractError):
        pb.seal_action(body({"lifetime": {"schema": "other.v1", "fence_s": 300}}))
    with pytest.raises(pb.ActionContractError):
        pb.seal_action(body({"lifetime": {"fence_s": 300}}))


@pytest.mark.parametrize("phase", list(lifetime_fence.PHASES))
def test_missing_enforcement_or_evidence_in_any_phase_reads_unknown(phase):
    good = {name: {"enforced": True, "evidence": "run-proof"}
            for name in lifetime_fence.PHASES}
    assert lifetime_fence.release_bound(
        claimed_unix=1000.0, fence_s=300.0, evidence={"phases": dict(good)}) == 1300.0
    # Delay or fail this phase: drop enforcement, drop evidence, drop the phase.
    for broken in ({"enforced": False, "evidence": "run-proof"},
                   {"enforced": True, "evidence": None},
                   {"enforced": True, "evidence": ""}):
        phases = dict(good)
        phases[phase] = broken
        assert lifetime_fence.release_bound(
            claimed_unix=1000.0, fence_s=300.0,
            evidence={"phases": phases}) is None
    phases = dict(good)
    del phases[phase]
    assert lifetime_fence.release_bound(
        claimed_unix=1000.0, fence_s=300.0, evidence={"phases": phases}) is None
    # No evidence at all, no claim stamp, no fence: all UNKNOWN.
    assert lifetime_fence.release_bound(
        claimed_unix=1000.0, fence_s=300.0, evidence=None) is None
    assert lifetime_fence.release_bound(
        claimed_unix=None, fence_s=300.0, evidence={"phases": dict(good)}) is None
    assert lifetime_fence.release_bound(
        claimed_unix=1000.0, fence_s=None, evidence={"phases": dict(good)}) is None


def test_payload_only_timeout_never_proves_a_release_bound(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(fleet)
    # A five-second payload budget is still opportunity metadata only.
    candidate = publish("payload-only-five-seconds", priority=-10, timeout_s=5)
    _observe_real_sharing_permission(fleet, candidate)
    assert claim() is None
    refusal = denial(candidate)
    assert refusal["reason"] in (
        "deferred_for_measurement_reservation", "deferred_behind_withheld_row")
    if refusal["reason"] == "deferred_for_measurement_reservation":
        assert refusal["evidence"]["candidate_release_bound"] == "UNKNOWN"
    else:
        assert refusal["evidence"]["withheld_for"] == measurement


def test_timed_backfill_gate_refuses_equality_and_later_bounds():
    assert lifetime_fence.timed_backfill_allowed(
        candidate_bound=100.0, original_opportunity=200.0) is True
    assert lifetime_fence.timed_backfill_allowed(
        candidate_bound=200.0, original_opportunity=200.0) is False
    assert lifetime_fence.timed_backfill_allowed(
        candidate_bound=300.0, original_opportunity=200.0) is False
    assert lifetime_fence.timed_backfill_allowed(
        candidate_bound="UNKNOWN", original_opportunity=200.0) is False
    assert lifetime_fence.timed_backfill_allowed(
        candidate_bound=100.0, original_opportunity="UNKNOWN") is False
    assert lifetime_fence.timed_backfill_allowed(
        candidate_bound=float("nan"), original_opportunity=200.0) is False
    assert lifetime_fence.timed_backfill_allowed(
        candidate_bound=True, original_opportunity=200.0) is False


def test_unfenced_candidate_holds_the_reserved_host(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(fleet)
    candidate = publish("unfenced-timed-candidate", priority=-10, timeout_s=60)
    _observe_real_sharing_permission(fleet, candidate)
    assert claim() is None, "an unfenced candidate took the reserved host"
    assert queue.item_path(pool.READY, candidate).exists()
    assert queue.ledger().held_keys() == [incumbent]
    _assert_holder_unchanged(queue, incumbent, snapshot)
    assert queue.item_path(pool.READY, measurement).exists()
    # Past the payload-only point the host still holds: expiry never
    # returns capacity, only proved settlement does.
    tick(original_end - clock[0] + 0.1)
    _observe_real_sharing_permission(fleet, candidate)
    assert claim() is None
    _assert_holder_unchanged(queue, incumbent, snapshot)
    queue.finish(incumbent, status="executed")
    tick(adaptive_gpu.PSI_AVG10_S + 1)
    assert claim() == measurement, denial(measurement)
    assert queue.item_path(pool.READY, candidate).exists()


def test_candidate_release_bound_reads_unknown_without_fence_or_evidence(
    fleet, tmp_path,
):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(fleet)
    candidate = publish("bound-unknown-candidate", priority=-10, timeout_s=60)
    item = pool._read_json(queue.item_path(pool.READY, candidate))
    assert isinstance(item, dict)
    assert reservation.candidate_release_bound(queue, item) == "UNKNOWN"
    allowed, bound = reservation.timed_backfill_permitted(
        queue, item,
        {"opportunity_unix": original_end, "action_key": measurement})
    assert allowed is False
    assert bound == "UNKNOWN"


def test_fenced_candidate_with_full_evidence_proves_a_finite_bound(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(fleet)
    checkout = fleet[0].root.parent / "checkout"
    cas = pb.PrismaBuildCAS(fleet[0].root.parent / "cas")
    fence_s = 300.0
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/gpu-backfill", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"],
                 "working_directory": ".", "result_path": "fenced-candidate"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {"gpu_exclusive": False, "execution_timeout_s": 120,
                   "lifetime": {"schema": lifetime_fence.LIFETIME_SCHEMA_V1,
                                "fence_s": fence_s}},
        "environment": {"variables": {}, "toolchain": {
            **pb.executable_toolchain_contract(sys.executable),
            "system": platform.system(), "machine": platform.machine(),
            "libc": "-".join(platform.libc_ver()),
        }},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas.publish_action_request(action)
    key = action["action_key"]
    queue.publish(action_key=key, cas_root=str(cas.root), checkout_root=str(checkout),
                  worker_script="worker.py", resources={"cpu": 2, "gpu": 1, "mem_gb": 8},
                  needs_gpu=True, tags=["sparklina", lifetime_fence.LIFETIME_TAG],
                  priority=-10, max_attempts=3)
    item = pool._read_json(queue.item_path(pool.READY, key))
    assert isinstance(item, dict)
    # No enforcement evidence yet: still UNKNOWN, host still held.
    assert reservation.candidate_release_bound(queue, item) == "UNKNOWN"
    # Full per-phase enforcement evidence proves a finite bound.
    claimed_unix = clock[0]
    item["claimed_unix"] = claimed_unix
    item["lifetime_evidence"] = {"phases": {
        phase: {"enforced": True, "evidence": f"{phase}-proof"}
        for phase in lifetime_fence.PHASES}}
    bound = reservation.candidate_release_bound(queue, item)
    assert bound == pytest.approx(claimed_unix + fence_s)
    # The gate still refuses equality and later bounds; only strictly
    # before the original opportunity permits backfill.
    assert reservation.timed_backfill_permitted(
        queue, item, {"opportunity_unix": claimed_unix + fence_s,
                      "action_key": measurement})[0] is False
    assert reservation.timed_backfill_permitted(
        queue, item, {"opportunity_unix": claimed_unix + fence_s - 1,
                      "action_key": measurement})[0] is False
    assert reservation.timed_backfill_permitted(
        queue, item, {"opportunity_unix": claimed_unix + fence_s + 1,
                      "action_key": measurement})[0] is True
    # Drop one phase's evidence and the finite verdict goes UNKNOWN again.
    item["lifetime_evidence"]["phases"]["cleanup"] = {
        "enforced": True, "evidence": None}
    assert reservation.candidate_release_bound(queue, item) == "UNKNOWN"

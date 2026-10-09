"""PB #1429: the opt-in lifetime fence bounds timed backfill; payload timeout does not.

Real PB fixtures delay or fail every applicable phase and prove the
declared finite predicate never holds without its enforcement and
evidence. A short sealed payload timeout is opportunity metadata, not
an admission-to-resource-release bound. Only a verified fence strictly
before the original opportunity permits timed backfill. Equality and
later bounds refuse. Capacity and isolation gates stay in force.
"""
from __future__ import annotations

import platform
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
sys.path[:0] = [str(ROOT / "tests")]
from prismabuild import _measurement_reservation as reservation  # noqa: E402
from prismabuild import adaptive_gpu, core as pb, lifetime_fence, pool  # noqa: E402
from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402
from test_measurement_drains_gpu_backfill import fleet as fleet_fixture  # noqa: E402
from test_measurement_reservation_backfill_1419 import (  # noqa: E402
    _assert_holder_unchanged,
    _bounded_measurement_wait,
    _holder_snapshot,
    _observe_real_sharing_permission,
)

fleet = fleet_fixture

#: The sealed fence every real-execution fixture uses. The seal minimum
#: keeps a full wall-clock expiry out of the suite; the kill path is
#: exercised by failing a phase (slow checkout, TERM-ignoring payload,
#: unprovable cleanup) inside this bound instead of by waiting it out.
FENCE_S = 60.0

WORKER = Path(__file__).resolve().parents[1] / "tools" / "prismabuild_worker.py"


def _seal(checkout: Path, task: str, params: dict) -> dict:
    return pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/lifetime-fence", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"],
                 "working_directory": ".", "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": params,
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })


def _fenced_queue(tmp_path: Path, task_body: str):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text(task_body)
    action = _seal(checkout, task_body, {
        "lifetime": {"schema": lifetime_fence.LIFETIME_SCHEMA_V1,
                     "fence_s": FENCE_S}})
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "queue"), capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
                  checkout_root=checkout, worker_script=WORKER)
    return queue, action


def _claim_fenced(queue: AdmittedQueueFixture):
    item = queue.queue.claim(capacity={"cpu": 8, "mem_gb": 16},
                             tags=[lifetime_fence.LIFETIME_TAG])
    assert item is not None
    return item


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
    assert lifetime_fence.fence_seconds(pb.action_lifetime(fenced)) == 300.0
    assert lifetime_fence.fence_seconds(None) is None
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


def _timed_evidence(claimed_unix: float, fence_s: float, end: float | None = None):
    end = claimed_unix + fence_s if end is None else end
    return {
        "schema": lifetime_fence.EVIDENCE_SCHEMA_V1,
        "phases": {
            phase: {"enforced": True, "evidence": f"{phase}-enforced",
                    "ended_unix": end}
            for phase in lifetime_fence.PHASES}}


@pytest.mark.parametrize("phase", list(lifetime_fence.PHASES))
def test_missing_enforcement_or_evidence_in_any_phase_reads_unknown(phase):
    good = _timed_evidence(1000.0, 300.0)["phases"]
    assert lifetime_fence.release_bound(
        claimed_unix=1000.0, fence_s=300.0,
        evidence={"schema": lifetime_fence.EVIDENCE_SCHEMA_V1,
                  "phases": dict(good)}) == 1300.0
    # Fail this phase: drop enforcement, drop evidence, drop the phase,
    # end it past the deadline, or stamp no end at all.
    for broken in ({"enforced": False, "evidence": "run-proof", "ended_unix": 1200.0},
                   {"enforced": True, "evidence": None, "ended_unix": 1200.0},
                   {"enforced": True, "evidence": "", "ended_unix": 1200.0},
                   {"enforced": True, "evidence": "run-proof", "ended_unix": 1400.0},
                   {"enforced": True, "evidence": "run-proof"}):
        phases = dict(good)
        phases[phase] = broken
        assert lifetime_fence.release_bound(
            claimed_unix=1000.0, fence_s=300.0,
            evidence={"schema": lifetime_fence.EVIDENCE_SCHEMA_V1,
                      "phases": phases}) is None
    phases = dict(good)
    del phases[phase]
    assert lifetime_fence.release_bound(
        claimed_unix=1000.0, fence_s=300.0,
        evidence={"schema": lifetime_fence.EVIDENCE_SCHEMA_V1,
                  "phases": phases}) is None
    # No evidence at all, wrong schema, no claim stamp, no fence: UNKNOWN.
    assert lifetime_fence.release_bound(
        claimed_unix=1000.0, fence_s=300.0, evidence=None) is None
    assert lifetime_fence.release_bound(
        claimed_unix=1000.0, fence_s=300.0,
        evidence={"phases": dict(good)}) is None
    assert lifetime_fence.release_bound(
        claimed_unix=None, fence_s=300.0,
        evidence={"schema": lifetime_fence.EVIDENCE_SCHEMA_V1,
                  "phases": dict(good)}) is None
    assert lifetime_fence.release_bound(
        claimed_unix=1000.0, fence_s=None,
        evidence={"schema": lifetime_fence.EVIDENCE_SCHEMA_V1,
                  "phases": dict(good)}) is None


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
    # A READY row carries no claim stamp: always UNKNOWN, even unfenced.
    assert "claimed_unix" not in item
    assert reservation.candidate_release_bound(queue, item) == "UNKNOWN"
    allowed, bound = reservation.timed_backfill_permitted(
        queue, item,
        {"opportunity_unix": original_end, "action_key": measurement})
    assert allowed is False
    assert bound == "UNKNOWN"


def test_ready_row_never_proves_a_bound_even_when_fenced(fleet):
    # The reviewer's READY regression: a fenced READY row has a sealed
    # fence but no claim stamp, so the bound stays UNKNOWN until a
    # worker claims it and files enforcement evidence durably.
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, original_end, snapshot = _bounded_measurement_wait(fleet)
    checkout = fleet[0].root.parent / "checkout"
    cas = pb.PrismaBuildCAS(fleet[0].root.parent / "cas")
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
                                "fence_s": FENCE_S}},
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
                  needs_gpu=True, tags=["sparklina"],
                  priority=-10, max_attempts=3)
    row = pool._read_json(queue.item_path(pool.READY, key))
    assert isinstance(row, dict)
    # Publish projects the fence and requires the capability tag.
    assert row.get("lifetime_fence_s") == FENCE_S
    assert lifetime_fence.LIFETIME_TAG in (row.get("tags") or [])
    assert "claimed_unix" not in row
    assert reservation.candidate_release_bound(queue, row) == "UNKNOWN"
    allowed, bound = reservation.timed_backfill_permitted(
        queue, row, {"opportunity_unix": original_end, "action_key": measurement})
    assert allowed is False
    assert bound == "UNKNOWN"


def test_claim_stamps_the_absolute_fence_deadline(tmp_path):
    # Real claim path: the fence deadline is stamped where the claim
    # stamps its clock, from the same two values the worker enforces.
    queue, action = _fenced_queue(tmp_path, "open('result','w').write('ok')\n")
    row = pool._read_json(queue.item_path(pool.READY, action["action_key"]))
    assert row.get("lifetime_fence_s") == FENCE_S
    item = _claim_fenced(queue)
    assert item.get("lifetime_fence_s") == FENCE_S
    assert item.get("lifetime_deadline_unix") == (
        item["claimed_unix"] + FENCE_S)
    # An old worker offering no fence tag cannot claim fenced work.
    assert queue.queue.claim(
        capacity={"cpu": 8, "mem_gb": 16}, tags=[]) is None


def test_fast_payload_files_full_phase_evidence(tmp_path):
    # Real execution: a payload that exits inside the fence files
    # per-phase enforcement evidence with measured ends.
    queue, action = _fenced_queue(tmp_path, "open('result','w').write('ok')\n")
    item = _claim_fenced(queue)
    key = item["action_key"]
    assert reservation.candidate_release_bound(queue.queue, item) == "UNKNOWN"
    outcome = queue.execute(item, heartbeat_s=0.05, timeout_grace_s=0.2)
    assert outcome["status"] == "executed"
    evidence = outcome.get("lifetime_evidence")
    assert isinstance(evidence, dict)
    assert evidence.get("schema") == lifetime_fence.EVIDENCE_SCHEMA_V1
    assert set(evidence.get("phases", {})) == set(lifetime_fence.PHASES)
    for phase, record in evidence["phases"].items():
        assert record.get("enforced") is True, phase
        assert record.get("evidence"), phase
        assert record.get("ended_unix") <= outcome["lifetime_fence"]["deadline_unix"], phase
    # Filed durably on the live claim, the bound proves finite.
    live = pool._read_json(queue.item_path(pool.CLAIMED, key))
    live["lifetime_evidence"] = evidence
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, key), live)
    snap = pool._read_json(queue.item_path(pool.CLAIMED, key))
    bound = reservation.candidate_release_bound(queue.queue, snap)
    assert bound == live["lifetime_deadline_unix"]
    assert reservation.timed_backfill_permitted(
        queue.queue, snap,
        {"opportunity_unix": bound + 1, "action_key": "other"})[0] is True
    assert reservation.timed_backfill_permitted(
        queue.queue, snap,
        {"opportunity_unix": bound, "action_key": "other"})[0] is False


def test_slow_checkout_fails_closed_before_launch(tmp_path, monkeypatch):
    # Real delayed phase: checkout stalls past the fence, so the worker
    # refuses to launch. No payload runs, no receipt files.
    queue, action = _fenced_queue(
        tmp_path, "open('result','w').write('ok')\n")
    item = _claim_fenced(queue)
    key = item["action_key"]
    record = pool._read_json(queue.item_path(pool.CLAIMED, key))
    record["claimed_unix"] = float(record["claimed_unix"]) - FENCE_S - 1.0
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, key), record)
    lease = pool._read_json(queue.lease_path(key))
    lease["claimed_unix"] = record["claimed_unix"]
    pool._write_json_atomic(queue.lease_path(key), lease)
    item["claimed_unix"] = record["claimed_unix"]
    outcome = queue.execute(item, heartbeat_s=0.05, timeout_grace_s=0.2)
    assert outcome["status"] == "failed"
    assert outcome["termination_reason"] == lifetime_fence.FENCE_TERMINATION_REASON
    assert outcome["lifetime_fence"]["expired_phase"] == "prelaunch"
    assert "lifetime_evidence" not in outcome
    assert reservation.candidate_release_bound(queue.queue, item) == "UNKNOWN"


def test_term_ignoring_payload_dies_at_the_fence(tmp_path, monkeypatch):
    # Real failed phase: a payload that ignores TERM still dies at the
    # absolute fence with the fence verdict, not the payload verdict.
    # The child traps TERM and sleeps; the fence rung escalates to KILL.
    task = ("import signal, time\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "time.sleep(30)\n"
            "open('result','w').write('ok')\n")
    queue, action = _fenced_queue(tmp_path, task)
    item = _claim_fenced(queue)
    jumped = [False]
    real_now = pool._now

    def fake_now():
        return real_now() + (FENCE_S + 1.0 if jumped[0] else 0.0)

    monkeypatch.setattr(pool, "_now", fake_now)
    real_popen = pool.subprocess.Popen

    class TripPopen(real_popen):
        def communicate(self, *args, **kwargs):
            jumped[0] = True
            return super().communicate(*args, **kwargs)

    monkeypatch.setattr(pool.subprocess, "Popen", TripPopen)
    started = time.time()
    outcome = queue.execute(item, heartbeat_s=0.05, timeout_grace_s=0.2)
    assert time.time() - started < FENCE_S
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == lifetime_fence.FENCE_TERMINATION_REASON
    assert outcome["lifetime_fence"]["expired_phase"] == "payload"
    assert "lifetime_evidence" not in outcome
    assert reservation.candidate_release_bound(queue.queue, item) == "UNKNOWN"


def test_checkpoint_credit_never_moves_the_fence(tmp_path, monkeypatch):
    # Real credited wait: checkpoint I/O credits the payload deadline
    # but the fence stays absolute. Force heavy checkpoint credits, then
    # expire the wall-clock fence mid-payload: the payload deadline moves
    # out while the fence rung still fires with the fence verdict.
    task = ("import time\ntime.sleep(30)\nopen('result','w').write('ok')\n")
    queue, action = _fenced_queue(tmp_path, task)
    item = _claim_fenced(queue)
    jumped = [False]
    real_now = pool._now

    def fake_now():
        return real_now() + (FENCE_S + 1.0 if jumped[0] else 0.0)

    monkeypatch.setattr(pool, "_now", fake_now)
    real_observe = pool._observe_execution

    def slow_observe(process, previous=None, **kwargs):
        # A slow shared-checkpoint read: the worker credits this stall
        # to the payload deadline only, never to the fence.
        time.sleep(2.0)
        return real_observe(process, previous, **kwargs)

    monkeypatch.setattr(pool, "_observe_execution", slow_observe)
    real_popen = pool.subprocess.Popen

    class TripPopen(real_popen):
        def communicate(self, *args, **kwargs):
            jumped[0] = True
            return super().communicate(*args, **kwargs)

    monkeypatch.setattr(pool.subprocess, "Popen", TripPopen)
    outcome = queue.execute(item, heartbeat_s=0.05, timeout_grace_s=0.2)
    assert outcome["termination_reason"] == lifetime_fence.FENCE_TERMINATION_REASON
    assert outcome["lifetime_fence"]["expired_phase"] == "payload"


def test_failed_cleanup_holds_resources_and_bound(tmp_path, monkeypatch):
    # Real failed settlement: cleanup that cannot prove the scope empty
    # retains the claim and keeps the bound UNKNOWN. Only proved
    # settlement releases.
    queue, action = _fenced_queue(tmp_path, "open('result','w').write('ok')\n")
    item = _claim_fenced(queue)
    outcome = queue.execute(item, heartbeat_s=0.05, timeout_grace_s=0.2)
    assert outcome["status"] == "executed"
    key = item["action_key"]
    monkeypatch.setattr(
        queue, "cleanup_action_containers",
        lambda record, **kwargs: {"complete": False, "used": True, "removed": [],
                                  "remaining": ["container-1"],
                                  "error": "daemon unreachable"})
    dst = queue.finish(key, status=str(outcome["status"]),
                       detail=outcome, claim_snapshot=item)
    assert dst == queue.item_path(pool.CLAIMED, key)
    live = pool._read_json(queue.item_path(pool.CLAIMED, key))
    assert live.get("finish_pending") is not None
    assert reservation.candidate_release_bound(queue.queue, live) == "UNKNOWN"

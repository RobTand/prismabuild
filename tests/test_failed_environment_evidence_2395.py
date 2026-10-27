"""Failed actions retain verified producer evidence without a success receipt (#2395)."""
from __future__ import annotations

import json
import sys

import pytest

from prismabuild import action_result, core as pb, endings, pool
from test_core import _action, _run_local_argv
from test_verified_action_result import _standard_action
from pbmcp_fixture import settle_publishers


@pytest.fixture(autouse=True)
def no_outer_identity(monkeypatch):
    from prismabuild import _measurement_reservation as reservation

    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "fixture-host")
    monkeypatch.setattr(pool.cpu_admission, "action_identity", lambda item: ("environment-fixture", False))
    monkeypatch.setattr(reservation.CensusReader, "_spawn_owned",
                        lambda self, directory, section, read, budget_s: read())


def test_ordinary_failure_retains_checked_producer_and_original_status(tmp_path, monkeypatch):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _action(checkout, argv=[sys.executable, "-c", "raise SystemExit(7)"])
    action_path = tmp_path / "action.json"
    action_path.write_text(json.dumps(action))
    sidecar = tmp_path / "status.json"
    monkeypatch.setenv(pb.ACTION_STATUS_PATH_ENV, str(sidecar))

    with pytest.raises(pb.LocalActionError) as caught:
        pb.main(_run_local_argv(action_path, cas_root=tmp_path / "cas", checkout=checkout))

    body = json.loads(sidecar.read_text())
    assert caught.value.returncode == body["action_returncode"] == 7
    producer = pb.validate_worker_attestation(body["producer_attestation"], action=action)
    assert producer["executable"]["path"] == sys.executable
    assert body.get("producer_attestation_error") is None
    assert pb.PrismaBuildCAS(tmp_path / "cas").lookup(action) is None


def test_failure_with_changed_closure_keeps_status_without_qualification(tmp_path, monkeypatch):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _action(checkout, argv=[sys.executable, "-c",
        "from pathlib import Path; Path('task_code.py').write_text('changed'); raise SystemExit(7)"])
    action_path = tmp_path / "action.json"
    action_path.write_text(json.dumps(action))
    sidecar = tmp_path / "status.json"
    monkeypatch.setenv(pb.ACTION_STATUS_PATH_ENV, str(sidecar))

    with pytest.raises(pb.LocalActionError) as caught:
        pb.main(_run_local_argv(action_path, cas_root=tmp_path / "cas", checkout=checkout))

    body = json.loads(sidecar.read_text())
    assert caught.value.returncode == body["action_returncode"] == 7
    assert body.get("producer_attestation") is None
    assert "closure" in body["producer_attestation_error"]


@pytest.mark.parametrize("status", [0, 7])
def test_declared_interpreter_witness_is_checked_for_success_and_failure(tmp_path, monkeypatch, status):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _action(checkout, argv=[sys.executable, "-c",
        f"from pathlib import Path; Path('result.bin').write_bytes(b'ok'); raise SystemExit({status})"])
    body = {key: value for key, value in action.items() if key != "action_key"}
    action = pb.seal_action({**body, "params": {**body["params"], "interpreter": sys.executable}})
    action_path = tmp_path / "action.json"
    action_path.write_text(json.dumps(action))
    sidecar = tmp_path / "status.json"
    monkeypatch.setenv(pb.ACTION_STATUS_PATH_ENV, str(sidecar))
    if status:
        with pytest.raises(pb.LocalActionError):
            pb.main(_run_local_argv(action_path, cas_root=tmp_path / "cas", checkout=checkout))
    else:
        assert pb.main(_run_local_argv(action_path, cas_root=tmp_path / "cas", checkout=checkout)) == 0
    recorded = json.loads(sidecar.read_text())
    assert recorded["producer_interpreter"] == pb.identify_executable(sys.executable)
    assert recorded.get("producer_attestation_error") is None


def failed_ending(tmp_path, *, producer=True, foreign=False, interpreter=False,
                  stdout="failed test\n"):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    action = _standard_action(checkout, command=[sys.executable, "-c", "raise SystemExit(7)"])
    if interpreter:
        body = {key: value for key, value in action.items() if key != "action_key"}
        action = pb.seal_action({**body, "params": {**body["params"], "interpreter": sys.executable}})
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    attestation = pb.preflight_action(action, cas_root=cas.root, checkout_root=checkout)
    if foreign:
        body = {**attestation, "worker_id": "another-host",
                "evidence": {**attestation["evidence"], "hostname": "another-host"}}
        body.pop("attestation_sha256")
        attestation = {**body, "attestation_sha256": pb.canonical_sha256(body)}
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.offers = lambda: []
    queue.records.ensure_layout()
    key = action["action_key"]
    queue.publish(action_key=key, cas_root=cas.root, checkout_root=checkout,
                  worker_script=tmp_path / "worker.py", tags=["x86"],
                  resources={"cpu": 1, "mem_gb": 1}, max_attempts=1)
    assert queue.claim(tags=["x86"], capacity={"cpu": 8, "mem_gb": 16}) is not None, queue.latest_denials({key})
    detail = {"status": "failed", "returncode": 1, "action_returncode": 7,
              "stdout": stdout, "stderr": ""}
    if producer:
        sidecar = tmp_path / "status.json"
        sidecar.write_text(json.dumps({"producer_attestation": attestation,
            **({"producer_interpreter": pb.identify_executable(sys.executable)} if interpreter else {})}))
        detail = queue._merge_action_status(detail, sidecar)
    queue.finish(key, status="failed", detail=detail)
    settle_publishers()
    record = json.loads(queue.records.item_path("failed", key).read_text())
    return action, queue, record, attestation


def test_verified_failed_ending_exposes_bound_attestation_only_when_requested(tmp_path):
    action, queue, record, producer = failed_ending(tmp_path)
    options = {"published_unix": record["published_unix"], "attempt": 1}
    ordinary = action_result.read_verified_ending_result(queue, action["action_key"], **options)
    assert "producer_attestation" not in ordinary
    observed = action_result.read_verified_ending_result(queue, action["action_key"],
        include_producer_attestation=True, **options)
    assert observed["verified"] is True
    assert observed["producer_attestation"] == producer
    assert observed["producer_attestation_error"] is None
    assert observed["receipt_sha256"] is None
    assert observed["producer_request"] == action
    assert observed["producer_stdout"] == "failed test\n"
    assert record["detail"]["action_returncode"] == 7


def test_truncated_native_stdout_is_not_returned_as_complete(tmp_path):
    cap = endings.TERMINAL_STREAM_TAIL_BYTES
    action, queue, record, _producer = failed_ending(tmp_path, stdout="x" * (cap + 1))
    observed = action_result.read_verified_ending_result(queue, action["action_key"],
        published_unix=record["published_unix"], attempt=1,
        include_producer_attestation=True, max_log_bytes=cap)
    assert observed["verified"] is True
    assert observed["producer_stdout"] is None


def test_verified_interpreter_witness_is_bound_to_the_sealed_command(tmp_path):
    action, queue, record, _producer = failed_ending(tmp_path, interpreter=True)
    observed = action_result.read_verified_ending_result(queue, action["action_key"],
        published_unix=record["published_unix"], attempt=1, include_producer_attestation=True)
    assert observed["producer_interpreter"] == pb.identify_executable(sys.executable)
    assert observed["producer_attestation_error"] is None


@pytest.mark.parametrize("case", ["legacy", "foreign"])
def test_missing_or_foreign_producer_keeps_verified_failure_unqualified(tmp_path, case):
    action, queue, record, _producer = failed_ending(
        tmp_path, producer=case != "legacy", foreign=case == "foreign")
    observed = action_result.read_verified_ending_result(queue, action["action_key"],
        published_unix=record["published_unix"], attempt=1, include_producer_attestation=True)
    assert observed["verified"] is True
    assert observed["producer_attestation"] is None
    assert observed["producer_attestation_error"]
    assert observed["receipt_sha256"] is None
    assert record["detail"]["action_returncode"] == 7

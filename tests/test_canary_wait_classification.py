"""An unverified wait is neither a passing canary nor a contract failure."""
import hashlib
import json
from types import SimpleNamespace

import pytest
import pbcanary
import pbrun
import publish_runtime
from pbcanary_legs import leg3
from pbcanary_verdict import verdict

KEY = "b" * 64


def _run_wait(tmp_path, monkeypatch, rc):
    monkeypatch.setattr(pbcanary, "submit_leg", lambda *a, **k: (
        KEY, {"action_key": KEY, "published_unix": 1234.5}))
    monkeypatch.setattr(pbcanary, "wait_leg", lambda *a, **k: {
        "returncode": rc, "record": {}, "stdout": "", "stderr": "budget expired"})
    results = []
    pbcanary._run_single_leg(
        {}, SimpleNamespace(verify=lambda *a: (True, "verified")),
        {"wait_s": 5, "expected": {}}, "leg-1", tmp_path, "fixture", "generation",
        -10, tmp_path, tmp_path,
        {"leg": "leg-1", "ok": False, "reason": "", "receipt_ref": None}, results)
    return results


@pytest.mark.parametrize("rc", [74, 75])
def test_observer_timeout_retains_key_and_is_not_a_defect(tmp_path, monkeypatch, rc):
    results = _run_wait(tmp_path, monkeypatch, rc)
    code, summary = verdict(results)
    assert code == 2
    assert summary["verified"] is False
    assert results[0]["action_key"] == KEY
    assert results[0]["not_verified"] in {"outcome_unobserved", "wait_budget_exhausted"}


def test_actual_failed_action_stays_failed(tmp_path, monkeypatch):
    assert verdict(_run_wait(tmp_path, monkeypatch, 1))[0] == 1


@pytest.mark.parametrize("code,status,exit_code", [(0, "verified", 0), (1, "failed", 1),
                                                   (2, "not_run", 2)])
def test_rollout_keeps_three_verdicts(tmp_path, monkeypatch, code, status, exit_code):
    monkeypatch.setattr(publish_runtime, "_invoke_canary_driver", lambda g: (code, "fixture"))
    result = publish_runtime._run_rollout_canary(
        store=tmp_path, generation_name="fixture", commit="a" * 40,
        enabled=True, shape_gate={"verdict": "waived", "reason": "unit fixture"})
    assert result == exit_code
    record = json.loads((tmp_path / "fixture.canary.json").read_text())
    assert record["canary_status"] == status


def test_retryable_stage_wait_is_typed_not_integrity_failure(monkeypatch):
    window = object.__new__(leg3._StagedWindow)
    window.entries = [{"path": "/fixture/chunk", "offset": 0}]
    window.residency_map = SimpleNamespace(residency_map_key=lambda *a: "key")
    window.wait_s = 0
    monkeypatch.setattr(window, "_prepare", lambda *a: {"ok": False, "refusal": "unpublished"})
    with pytest.raises(leg3._StagedReadRefusal) as exc:
        window.read_chunk(0, hashlib.sha256())
    assert type(exc.value).__name__ == "_StagedReadWaitExpired"
    monkeypatch.setattr(window, "_prepare", lambda *a: {"ok": False, "refusal": "changed-identity"})
    with pytest.raises(leg3._StagedReadRefusal) as exc:
        window.read_chunk(0, hashlib.sha256())
    assert type(exc.value).__name__ == "_StagedReadRefusal"


def test_real_failure_is_not_hidden_by_another_starved_leg():
    rows = [{"leg": "leg-1", "ok": False, "reason": "digest mismatch"},
            {"leg": "leg-3", "ok": False, "reason": "staging wait expired",
             "not_verified": "staging_wait", "action_key": KEY}]
    assert verdict(rows)[0] == 1


@pytest.mark.parametrize("wait", [True, False])
def test_action_emits_typed_wait_but_never_green(tmp_path, monkeypatch, capsys, wait):
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    monkeypatch.setenv(leg3._ACTION_ENV_MANIFEST, str(manifest))
    monkeypatch.setattr(leg3, "_validated_entries", lambda m: [{}])
    monkeypatch.setattr(leg3, "_progress_channel", lambda: None)
    def read(*args):
        error = leg3._StagedReadWaitExpired if wait else leg3._StagedReadRefusal
        raise error("fixture")
    monkeypatch.setattr(leg3._StagedWindow, "open", lambda *a, **k: SimpleNamespace(read_chunk=read))
    assert leg3.run_action() == 1
    envelope = json.loads(capsys.readouterr().out)
    assert envelope["ok"] is False
    assert (envelope.get("not_verified") == "staging_wait") is wait


@pytest.mark.parametrize("same_generation,latest_wait", [(True, True), (False, True), (True, False)])
def test_staging_evidence_binds_the_generation_and_latest_attempt(
        tmp_path, monkeypatch, same_generation, latest_wait):
    marker = json.dumps({"schema": leg3.LEG3_SCHEMA, "ok": False,
                         "not_verified": "staging_wait"})
    monkeypatch.setattr(pbrun, "bounded_outcome_observation", lambda *a, **k: (
        (tmp_path / "failed.json", {}), 1234.5 if same_generation else 1235.0))
    monkeypatch.setattr(pbrun, "bounded_outcome_render", lambda *a, **k: {
        "summary": {"action_key": KEY, "status": "failed", "detail": {}},
        "attempts": [{"stdout": marker}, {"stdout": marker if latest_wait else "bad digest"}]})
    expected = latest_wait if same_generation else None
    assert pbcanary.staging_wait_evidence(
        {"queue_root": tmp_path / "queue"}, KEY, 1234.5) is expected


def test_rollout_retains_driver_keys_and_reason(tmp_path, monkeypatch):
    def driver(*, generation, report):
        report.update(run_id="fixture", results=[{"action_key": KEY,
                      "not_verified": "staging_wait"}])
        return 2
    monkeypatch.setattr(publish_runtime, "_load_canary_driver", lambda: driver)
    code, detail = publish_runtime._invoke_canary_driver("fixture")
    assert code == 2 and KEY in detail and "staging_wait" in detail

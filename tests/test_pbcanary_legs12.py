"""Crew-A unit tests for the pbcanary driver skeleton + legs 1-2 (#688).

Pure build/verify contract tests plus driver precondition/helper paths.
No fleet contact: submission-path tests stop at refused preconditions, and
the live canary run itself is crew A's smoke evidence, not this file.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

FLEET_DIR = Path(__file__).resolve().parents[1] / "tools" / "fleet"
if str(FLEET_DIR) not in sys.path:
    sys.path.insert(0, str(FLEET_DIR))

import pbcanary  # noqa: E402
from pbcanary_legs import LegBuildRefused, leg1, leg2  # noqa: E402

PINNED = "example.org/pb-campaign@sha256:" + "ab" * 32


def run_leg1_argv_locally() -> str:
    spec = leg1.build()
    completed = subprocess.run(
        spec["argv"], capture_output=True, text=True, check=True, timeout=60,
    )
    return completed.stdout


def make_receipt(artifact: str, *, tamper_sha: bool = False,
                 tamper_bytes: bool = False) -> dict:
    raw = artifact.encode()
    digest = hashlib.sha256(raw).hexdigest()
    if tamper_sha:
        digest = "0" * 64
    size = len(raw) + (1 if tamper_bytes else 0)
    return {
        "action_key": "k" * 64,
        "receipt": {"result": {"sha256": digest, "bytes": size}},
        "artifact": artifact,
    }


# Leg 1 --------------------------------------------------------------------


def test_leg1_build_shape():
    spec = leg1.build()
    assert spec["name"] == "leg-1"
    assert spec["argv"][0] == "python3"
    assert "gpu" not in spec["demand"]
    assert spec["container_image"] is None
    assert spec["wait_s"] == 300
    assert set(spec) == {"name", "argv", "demand", "wait_s",
                         "container_image", "expected"}


def test_leg1_action_output_matches_expectation():
    assert run_leg1_argv_locally() == leg1.build()["expected"]["artifact_exact"]


def test_leg1_verify_ok():
    spec = leg1.build()
    ok, reason = leg1.verify(
        make_receipt(spec["expected"]["artifact_exact"]), spec["expected"])
    assert ok, reason
    assert "leg-1" in reason


def test_leg1_verify_corrupted_artifact_names_leg():
    spec = leg1.build()
    ok, reason = leg1.verify(
        make_receipt("deadbeef\n"), spec["expected"])
    assert not ok
    assert reason.startswith("leg-1")


def test_leg1_verify_tampered_result_fails():
    spec = leg1.build()
    receipt = make_receipt(spec["expected"]["artifact_exact"], tamper_sha=True)
    ok, reason = leg1.verify(receipt, spec["expected"])
    assert not ok and "leg-1" in reason
    receipt = make_receipt(spec["expected"]["artifact_exact"], tamper_bytes=True)
    ok, reason = leg1.verify(receipt, spec["expected"])
    assert not ok and "leg-1" in reason


def test_leg1_verify_malformed_inputs_fail_closed():
    spec = leg1.build()
    for bad_receipt in (None, [], "x", {}):
        ok, reason = leg1.verify(bad_receipt, spec["expected"])
        assert not ok and "leg-1" in reason
    ok, reason = leg1.verify(make_receipt("x\n"), {"leg": "leg-9"})
    assert not ok and "leg-1" in reason


# Leg 2 --------------------------------------------------------------------


def test_leg2_build_refuses_missing_image():
    with pytest.raises(LegBuildRefused):
        leg2.image_ref(env={})


def test_leg2_build_refuses_floating_tag():
    with pytest.raises(LegBuildRefused):
        leg2.image_ref(env={leg2.IMAGE_ENV: "example.org/img:latest"})


def test_leg2_build_shape_pinned(monkeypatch):
    monkeypatch.setenv(leg2.IMAGE_ENV, PINNED)
    spec = leg2.build()
    assert spec["name"] == "leg-2"
    assert spec["demand"]["gpu"] == 1
    assert spec["wait_s"] == 600
    assert spec["container_image"] == PINNED
    assert "--gpus" in spec["argv"][-1] and PINNED in spec["argv"][-1]
    # The entrypoint is cleared exactly as the production container wrapper
    # does: the campaign image family's own entrypoint runs vLLM platform
    # inference and refuses to pass a command through when it stands (found
    # live 2026-09-19 wiring the release; the wrapper is
    # tools/tessera_campaign_container.py).
    assert '--entrypoint ""' in spec["argv"][-1]
    assert spec["expected"]["artifact_prefix"].startswith(f"IMAGE {PINNED}\n")


def _leg2_artifact(*, devices: int = 2, kernel: bool = True,
                   prefix_ok: bool = True) -> str:
    lines = []
    lines.append(f"IMAGE {PINNED if prefix_ok else 'other:tag'}")
    lines.append(f"PAYLOAD {leg2.PAYLOAD_SHA256 if prefix_ok else '00'}")
    lines.append(f"TORCH 2.7.0 CUDA 12.6 DEVICES {devices}")
    if kernel:
        lines.append(f"KERNEL {leg2.KERNEL_VALUE}")
    return "\n".join(lines) + "\n"


def test_leg2_verify_ok(monkeypatch):
    monkeypatch.setenv(leg2.IMAGE_ENV, PINNED)
    spec = leg2.build()
    ok, reason = leg2.verify(
        make_receipt(_leg2_artifact()), spec["expected"])
    assert ok, reason
    assert "leg-2" in reason


def test_leg2_verify_zero_devices_fails_cuda_visibility(monkeypatch):
    monkeypatch.setenv(leg2.IMAGE_ENV, PINNED)
    spec = leg2.build()
    ok, reason = leg2.verify(
        make_receipt(_leg2_artifact(devices=0)), spec["expected"])
    assert not ok and reason.startswith("leg-2")


def test_leg2_verify_missing_kernel_fails(monkeypatch):
    monkeypatch.setenv(leg2.IMAGE_ENV, PINNED)
    spec = leg2.build()
    ok, reason = leg2.verify(
        make_receipt(_leg2_artifact(kernel=False)), spec["expected"])
    assert not ok and "leg-2" in reason


def test_leg2_verify_wrong_image_prefix_fails(monkeypatch):
    monkeypatch.setenv(leg2.IMAGE_ENV, PINNED)
    spec = leg2.build()
    ok, reason = leg2.verify(
        make_receipt(_leg2_artifact(prefix_ok=False)), spec["expected"])
    assert not ok and "leg-2" in reason


# Driver helpers + preconditions (no fleet contact) ------------------------


def test_first_json_with_key_skips_noise():
    text = "pbrun: hello\n{\"other\": 1}\n{\"action_key\": \"abc\", \"x\": 1}\n"
    found = pbcanary.first_json_with_stdout_key(text, "action_key")
    assert found == {"action_key": "abc", "x": 1}
    assert pbcanary.first_json_with_stdout_key("no json here", "action_key") is None


def test_last_json_object_reads_pbwait_tail():
    text = "row one\n{\"a\": 1}\nnot json\n{\"b\": 2}\n"
    assert pbcanary.last_json_object(text) == {"b": 2}


def test_fresh_namespace_refuses_reuse(tmp_path):
    first = pbcanary.fresh_namespace(tmp_path, "run-1")
    assert first.is_dir()
    with pytest.raises(pbcanary.PreconditionRefused):
        pbcanary.fresh_namespace(tmp_path, "run-1")


def test_unknown_leg_is_exit_2(tmp_path, capsys):
    code = pbcanary.main(["--legs", "leg-9", "--fleet-root", str(tmp_path)])
    assert code == 2
    assert "unknown leg" in capsys.readouterr().err


def test_missing_queue_is_exit_2_before_any_submission(tmp_path, capsys):
    checkout = tmp_path / "checkout"
    (checkout / ".git").mkdir(parents=True)
    code = pbcanary.main([
        "--legs", "leg-1",
        "--fleet-root", str(tmp_path / "fleet"),
        "--published-root", "/mnt/shared/prismabuild-fleet/repo",
        "--checkout", str(checkout),
        "--run-id", "test-run",
    ])
    assert code == 2
    assert "queue unreachable" in capsys.readouterr().err
    # Nothing submitted, no namespace sealed for a run that never began.
    assert not (tmp_path / "fleet" / "pb-canary" / "test-run").exists()


def test_verdict_contract_shape():
    """Crew C's module is the driver's exit-code authority when present."""
    try:
        module = __import__("pbcanary_verdict")
    except ImportError:
        pytest.skip(
            "crew C verdict module not yet merged; driver import-guards it")
    assert callable(module.verdict)
    code, _summary = module.verdict([])
    assert code == 2  # no leg results is did-not-test, never passed

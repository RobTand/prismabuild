"""Invalid compatibility captures are refused even by optimized interpreters."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("tool", ["refresh", "adopt"])
@pytest.mark.parametrize("optimization", ["flag", "environment"])
def test_capture_intake_retains_its_guard_under_optimization(tmp_path, tool, optimization):
    capture = tmp_path / "capture"
    capture.mkdir()
    report = {"passed": False, "failures": ["deliberate invalid input"],
              "independent_writer_reproduction_required": False}
    (capture / "qualification.json").write_text(json.dumps(report))
    audit = tmp_path / "primary.json"
    audit.write_text("[]")
    destination = tmp_path / "destination"
    destination.mkdir()
    output = tmp_path / "refresh-report.json"
    if tool == "refresh":
        script = ROOT / "tools/maintenance/refresh_compat_runtime.py"
        arguments = ["--root", str(destination), "--report", str(output)]
        refusal = "capture did not pass qualification"
    else:
        script = ROOT / "tests/compat/support/adopt_compat_capture.py"
        arguments = ["--destination", str(destination), "--suffix", "refused"]
        refusal = "rerun the generator with independent byte reproduction"
    environment = dict(os.environ)
    environment.pop("PYTHONOPTIMIZE", None)
    command = [sys.executable]
    if optimization == "flag":
        command.append("-O")
    else:
        environment["PYTHONOPTIMIZE"] = "1"
    command += [str(script), "--capture", str(capture),
                "--primary-audit", str(audit), *arguments]
    result = subprocess.run(command, env=environment, text=True, capture_output=True, timeout=30)
    assert result.returncode != 0
    assert refusal in result.stderr, result.stderr
    assert list(destination.iterdir()) == []
    assert not output.exists()


@pytest.mark.parametrize("mutation", ["roster", "agent_digest"])
@pytest.mark.parametrize("optimization", ["flag", "environment"])
def test_intent_refresh_rejects_non_source_or_unbound_changes(tmp_path, mutation, optimization):
    root = tmp_path / "source"
    data = root / "tests/compat"
    data.mkdir(parents=True)
    members = root / "tools/fleet"
    members.mkdir(parents=True)
    for name in ("upgrade_client.py", "publish_runtime.py"):
        (members / name).write_bytes(b"# selected source identity\n")
    identity = hashlib.sha256((members / "upgrade_client.py").read_bytes()).hexdigest()
    before = dict(agent_sha256="a" * 64, coordinator_sha256="b" * 64, roster=["compat-host"])
    after = dict(before, agent_sha256=identity, coordinator_sha256=identity)
    if mutation == "roster":
        after["roster"] = ["unexpected-host"]
        refusal = "upgrade intent changed outside source identities"
    else:
        after["agent_sha256"] = "c" * 64
        refusal = "upgrade intent does not bind actual selected source bytes"

    def record(body):
        raw = json.dumps(body).encode()
        return json.dumps(dict(case="upgrade-intent/armed", raw=base64.b64encode(raw).decode(),
                               sha256=hashlib.sha256(raw).hexdigest())) + "\n"

    original = record(before)
    (data / "goldens-rollout.jsonl").write_text(original)
    (data / "historical-rollout.jsonl").write_text(original)
    subprocess.run(["git", "init", "-q", str(root)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(root), "add", "."], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(root), "-c", "core.hooksPath=/dev/null",
                    "-c", "user.name=Compatibility control", "-c", "user.email=compat@example.invalid",
                    "commit", "-qm", "Private invalid capture control"], check=True, capture_output=True)
    head = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    capture = tmp_path / "capture"
    capture.mkdir()
    (capture / "qualification.json").write_text(json.dumps(dict(
        passed=True, failures=[], source_git=dict(parents=[head]), selected_cases=["upgrade-intent/armed"])))
    (capture / "goldens.jsonl").write_text(record(after))
    (capture / "historical.jsonl").write_text(original)
    audit = tmp_path / "primary.json"
    audit.write_text("[]")
    output = tmp_path / "report.json"
    environment = dict(os.environ)
    environment.pop("PYTHONOPTIMIZE", None)
    command = [sys.executable]
    if optimization == "flag":
        command.append("-O")
    else:
        environment["PYTHONOPTIMIZE"] = "1"
    command += [str(ROOT / "tools/maintenance/refresh_compat_runtime.py"), "--profile", "upgrade-intent",
                "--root", str(root), "--capture", str(capture), "--primary-audit", str(audit),
                "--report", str(output)]
    result = subprocess.run(command, env=environment, text=True, capture_output=True, timeout=30)
    assert result.returncode != 0
    assert refusal in result.stderr, result.stderr
    assert (data / "goldens-rollout.jsonl").read_text() == original
    assert not (data / "history-controls").exists()
    assert not output.exists()

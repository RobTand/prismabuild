"""Invalid compatibility captures are refused even by optimized interpreters."""
from __future__ import annotations

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

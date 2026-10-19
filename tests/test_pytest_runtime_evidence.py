"""Unavailable runtime evidence does not change the native test outcome."""
import json
from pathlib import Path
import runpy
import sys
from types import SimpleNamespace


def test_an_unreadable_runtime_is_reported_as_unavailable(monkeypatch):
    hook = runpy.run_path(str(Path(__file__).with_name("conftest.py")))[
        "pytest_terminal_summary"]
    original = Path.read_bytes

    def read_runtime(path):
        if path == Path("/proc/self/exe"):
            raise PermissionError("runtime evidence denied")
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", read_runtime)
    lines = []
    config = SimpleNamespace(args=["runtime-evidence-control"])
    hook(SimpleNamespace(write_line=lines.append), 0, config)
    assert len(lines) == 1
    record = json.loads(lines[0].split(": ", 1)[1])
    assert record["complete"] is False
    assert record["reason"] == "PermissionError: runtime evidence denied"
    assert record["version"] == list(sys.version_info[:3])
    assert record["files"] == config.args

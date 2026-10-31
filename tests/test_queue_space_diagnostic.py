"""Quota-aware queue observations add diagnostics without new admission policy."""
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import filesystem_capacity as capacity


@pytest.mark.parametrize("available,state", [(20, "ok"), (15, "warning"), (3, "critical")])
def test_queue_space_thresholds_use_available_bytes(tmp_path, monkeypatch, available, state):
    class Sample:
        f_blocks = 100 * 1024 ** 3
        f_frsize = 1
        f_bavail = available * 1024 ** 3
        f_bfree = 100 * 1024 ** 3  # Privileged reserve does not hide quota pressure.
        f_files = 1000
        f_favail = 900
    monkeypatch.setattr(capacity.os, "statvfs", lambda path: Sample())
    result = capacity.queue_space_diagnostic(tmp_path)
    assert result["state"] == state and result["available_bytes"] == available * 1024 ** 3


def test_queue_inode_pressure_and_unknown_are_reported(tmp_path, monkeypatch):
    class Sample:
        f_blocks = 100 * 1024 ** 3
        f_frsize = 1
        f_bavail = 40 * 1024 ** 3
        f_files = 1000
        f_favail = 1
    monkeypatch.setattr(capacity.os, "statvfs", lambda path: Sample())
    assert capacity.queue_space_diagnostic(tmp_path)["inode_pressure"]
    def unavailable(path):
        raise PermissionError("scratch-only refusal")
    monkeypatch.setattr(capacity.os, "statvfs", unavailable)
    assert capacity.queue_space_diagnostic(tmp_path)["state"] == "unknown"

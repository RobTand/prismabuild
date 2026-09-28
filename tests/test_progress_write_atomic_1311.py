"""progress._write lands crash-atomic (issue #1311).

The report body is flushed + fsynced to the temp file before os.replace, so
a crash never leaves a torn temp and a power loss after the replace never
loses the record. The non-raising contract holds: any OSError returns False.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))

from prismabuild import progress  # noqa: E402


def test_write_round_trips_record(tmp_path):
    path = tmp_path / "progress.json"
    assert progress._write(path, {"phase": "run", "count": 3}) is True
    assert json.loads(path.read_text(encoding="utf-8")) == {
        "phase": "run",
        "count": 3,
    }


def test_write_replaces_whole_record(tmp_path):
    path = tmp_path / "progress.json"
    assert progress._write(path, {"phase": "run"}) is True
    assert progress._write(path, {"phase": "done", "extra": [1]}) is True
    assert json.loads(path.read_text(encoding="utf-8")) == {
        "phase": "done",
        "extra": [1],
    }


def test_write_failure_returns_false_without_raising(tmp_path):
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory", encoding="utf-8")
    assert progress._write(blocker / "progress.json", {"phase": "run"}) is False

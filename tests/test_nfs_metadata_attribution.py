"""Only calls wholly within the selected trace window contribute to rates."""

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from maintenance import nfs_metadata_attribution as attribution


@pytest.mark.parametrize("entered, completed, counted", [
    (100.2, 100.4, True),
    (99.9, 100.2, False),
    (100.8, 101.2, False),
    (100.0, 100.2, True),
    (100.2, 101.0, False),
])
def test_resumed_call_must_enter_and_complete_inside_window(entered, completed, counted):
    lines = [
        f'10 {entered:.6f} openat(AT_FDCWD</>, "/mnt/shared/data", '
        'O_RDONLY <unfinished ...>\n',
        f'10 {completed:.6f} <... openat resumed>) = 3</mnt/shared/data>\n',
    ]

    rows = attribution.summarize_trace(lines, {"10": "worker"}, 100.0, 101.0)

    assert rows == ([{
        "pid": 10, "root_pid": 10, "role": "worker", "syscall": "openat",
        "path": "/mnt/shared/data", "outcome": "success", "count": 1,
        "calls_per_s": 1.0,
    }] if counted else [])

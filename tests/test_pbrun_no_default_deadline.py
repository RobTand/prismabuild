"""``pbrun`` sends no deadline unless one was asked for.

Rob's rule: a worker that is progressing is never killed on elapsed time.
The pull queue parsed ``--timeout-s`` and dropped it (issue #32); the SLURM
lane enforces it, so a default of 7200 s would have turned the cutover into a
two-hour kill on every submission that never mentioned a deadline.
"""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys

PBRUN = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "pbrun.py"


def test_the_timeout_flag_defaults_to_no_deadline() -> None:
    text = subprocess.run(
        [sys.executable, str(PBRUN), "--help"], capture_output=True,
        text=True, check=True,
    ).stdout
    flag = text.split("--timeout-s", 1)[1]
    assert "unset means the action runs while it is running" in " ".join(flag.split())
    assert "default: 7200" not in text

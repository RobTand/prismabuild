"""Scratch detached-publication checkout, worker offer and pbrun driver."""
import json
from pathlib import Path
import socket
import subprocess
import sys

import pytest

from submission_support import patch_store

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402
from prismabuild import submission as submit_core  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402

def _checkout(tmp_path: Path) -> Path:
    work = tmp_path / "work"
    work.mkdir()
    (work / "seed.txt").write_text("sealed\n", encoding="utf-8")
    for args in (
        ("init", "-q"),
        ("config", "user.email", "test@example.invalid"),
        ("config", "user.name", "PrismaBuild test"),
        ("add", "seed.txt"),
        ("commit", "-qm", "sealed tree"),
    ):
        completed = subprocess.run(
            ["git", "-C", str(work), *args], capture_output=True, text=True,
        )
        assert completed.returncode == 0, completed.stderr
    return work



def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(
        host="sparky", tags=["sparky", "gb10"], has_gpu=True,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
    )
    return queue


def _run_pbrun(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, work: Path,
    *options: str, command=("/bin/bash", "-lc", "printf ok"),
) -> int:
    patch_store(monkeypatch, client=pbrun, root=tmp_path)
    monkeypatch.setattr(submit_core.action_wait, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--cwd", str(work), "--wait-s", "0.01", *options,
        "--", *command,
    ])
    return pbrun.main()


def _one_json_line(captured) -> dict:
    """The single object on stdout.  Prose goes to stderr; this is the contract."""

    lines = [line for line in captured.out.splitlines() if line.strip()]
    assert len(lines) == 1, f"stdout carried {len(lines)} lines: {lines!r}"
    return json.loads(lines[0])

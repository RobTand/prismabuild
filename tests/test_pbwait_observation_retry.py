"""A patient ``pbwait`` outlasts one slow terminal-record read (#558).

The FIFO is private to each test and stands in for a hard-NFS read: opening
the terminal record blocks after the directory listing has named it.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import time

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
from prismabuild import core as pb, pool  # noqa: E402
import pbrun  # noqa: E402
import pbwait  # noqa: E402


KEY = "d" * 64


def _queue(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    return queue


def _outcome(status: str, returncode: int) -> dict:
    return {
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "action_key": KEY,
        "status": status,
        "published_unix": 1.0,
        "finished_unix": 2.0,
        "finished_host": "worker",
        "attempts": 1,
        "detail": {"returncode": returncode, "elapsed_s": 1.0,
                   "stdout": "", "stderr": ""},
    }


def _fast_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pbwait, "PBWAIT_READ_TIMEOUT_S", 0.05)
    monkeypatch.setattr(pbrun, "POLL_S", 0.01)


@pytest.mark.parametrize(
    ("state", "status", "returncode", "exit_code"),
    [
        (pool.DONE, "executed", 0, 0),
        (pool.FAILED, "failed", 9, 1),
    ],
)
def test_patient_wait_reports_a_record_that_becomes_readable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    state: str, status: str, returncode: int, exit_code: int,
) -> None:
    queue = _queue(tmp_path)
    path = queue.item_path(state, KEY)
    os.mkfifo(path)
    readable = tmp_path / "readable.json"
    readable.write_text(json.dumps(_outcome(status, returncode)), encoding="utf-8")
    stops: list[str] = []
    original_stop = pbrun.pbstatus._stop_reader

    def stop_then_land(pid, section, started, abandoned):
        original_stop(pid, section, started, abandoned)
        stops.append(section)
        if len(stops) == 1:
            os.replace(readable, path)

    _fast_reads(monkeypatch)
    monkeypatch.setattr(pbrun.pbstatus, "_stop_reader", stop_then_land)
    row = pbwait.wait_one(
        queue, KEY, cas=pb.PrismaBuildCAS(tmp_path / "cas"),
        deadline=time.monotonic() + 5, lane_root=tmp_path / "lane",
    )
    assert stops == ["pbwait observation"]
    assert row["status"] == status, row
    assert row["returncode"] == returncode
    assert pbwait.verdict([row]) == exit_code


def test_wait_that_ends_while_the_outcome_is_unavailable_says_so(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = _queue(tmp_path)
    os.mkfifo(queue.item_path(pool.DONE, KEY))
    _fast_reads(monkeypatch)
    row = pbwait.wait_one(
        queue, KEY, cas=pb.PrismaBuildCAS(tmp_path / "cas"),
        deadline=time.monotonic() + 0.3, lane_root=tmp_path / "lane",
    )
    assert row["status"] == "record_error"
    assert pbwait.verdict([row]) == pbrun.RECORD_WRITE_FAILED_EXIT
    assert "pbwait observation timed out" in row["note"]
    assert "still unavailable when the wait ended" in row["note"]
    assert "may already have landed" in row["note"]

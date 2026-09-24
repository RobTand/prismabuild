"""``pbwait`` and ``pbcampaign`` outlast an outcome reader stuck in the kernel (#1048).

#1033 taught ``pbrun``'s patient wait to wait out a reader that timed out and
could not be reaped. ``pbwait`` still ended the key at the first such reader,
and ``pbcampaign`` waits through ``pbwait.wait_for_keys``, so both reported
74 with hours of ``--wait-s`` left. A FIFO stands in for the stuck read and a
reap that fails until the "kernel" lets go stands in for the ``D`` state.
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

from test_pbrun_retained_reader_wait import Readers, _kernel_holds_the_reader_until  # noqa: E402


KEY = "e" * 64


def _outcome() -> dict:
    return {
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "action_key": KEY,
        "status": "executed",
        "published_unix": 1.0,
        "finished_unix": 2.0,
        "finished_host": "worker",
        "attempts": 1,
        "detail": {"returncode": 0, "elapsed_s": 1.0, "stdout": "", "stderr": ""},
    }


def _stuck_queue(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    record = queue.item_path(pool.DONE, KEY)
    os.mkfifo(record)

    def land():
        # The mount recovers: the ending the worker wrote is readable.
        record.unlink()
        record.write_text(json.dumps(_outcome()), encoding="utf-8")

    return queue, land


def _fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pbwait, "PBWAIT_READ_TIMEOUT_S", 0.05)
    monkeypatch.setattr(pbrun, "POLL_S", 0.1)


def test_a_patient_pbwait_outlasts_a_reader_stuck_in_the_kernel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``wait_for_keys`` is ``pbwait``'s and ``pbcampaign``'s wait (M1)."""

    queue, land = _stuck_queue(tmp_path)
    _fast(monkeypatch)
    readers = Readers(monkeypatch)
    _kernel_holds_the_reader_until(
        monkeypatch, release_at=time.monotonic() + 1.0, on_release=land)
    try:
        rows = pbwait.wait_for_keys(
            queue, [KEY], cas=pb.PrismaBuildCAS(tmp_path / "cas"), wait_s=30,
            lane_root=tmp_path / "lane")
        assert [row["status"] for row in rows] == ["executed"], rows
        assert pbwait.verdict(rows) == 0
        assert readers.peak == 1
        assert len(readers.forked) >= 2
        err = capsys.readouterr().err
        assert "reader has exited and was reaped" in err
        assert "pbstatus: retained reader" not in err
    finally:
        readers.cleanup()


def test_a_reader_still_retained_when_pbwait_ends_is_named_with_no_second_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, _land = _stuck_queue(tmp_path)
    _fast(monkeypatch)
    readers = Readers(monkeypatch)
    _kernel_holds_the_reader_until(monkeypatch, release_at=None)
    try:
        started = time.monotonic()
        row = pbwait.wait_one(
            queue, KEY, cas=pb.PrismaBuildCAS(tmp_path / "cas"),
            deadline=started + 1.0, lane_root=tmp_path / "lane")
        assert time.monotonic() - started < 1.0 + 2 * pbrun.POLL_S + 1.0
        assert row["status"] == "record_error"
        assert pbwait.verdict([row]) == pbrun.RECORD_WRITE_FAILED_EXIT
        assert "still retained when the wait ended" in row["note"]
        assert "no second reader was started" in row["note"]
        assert f'"pid": {readers.forked[0]}' in row["note"]
        assert len(readers.forked) == 1
    finally:
        readers.cleanup()


def test_zero_patience_still_answers_at_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Guard: ``--wait-s 0`` is one observation, retained reader or not."""

    queue, _land = _stuck_queue(tmp_path)
    _fast(monkeypatch)
    readers = Readers(monkeypatch)
    _kernel_holds_the_reader_until(monkeypatch, release_at=None)
    try:
        started = time.monotonic()
        rows = pbwait.wait_for_keys(
            queue, [KEY], cas=pb.PrismaBuildCAS(tmp_path / "cas"), wait_s=0,
            lane_root=tmp_path / "lane")
        assert time.monotonic() - started < 2
        assert rows[0]["status"] == "record_error"
        assert [child["pid"] for child in rows[0]["retained_readers"]] == \
            [readers.forked[0]]
        assert len(readers.forked) == 1
    finally:
        readers.cleanup()

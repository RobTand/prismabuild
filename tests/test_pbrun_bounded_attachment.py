"""Detached attachment discovery must not park its caller on a queue read."""
from __future__ import annotations

from pathlib import Path
import json
import os
import subprocess
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
import pbrun  # noqa: E402
from prismabuild import pool  # noqa: E402


def test_detached_fifo_attachment_read_refuses_before_publication(tmp_path: Path) -> None:
    """Re-run a real detached submission, blocking its READY observation.

    The private FIFO and causal marker establish the unchanged implementation
    reached the actual read. The outer watchdog keeps a failing baseline from
    consuming its admitted job's entire budget.
    """

    marker = tmp_path / "entered-ready-read"
    code = f'''\
import contextlib, io, json, os, sys
from pathlib import Path
sys.path[:0] = [{str(ROOT / 'tests')!r}, {str(ROOT / 'tools' / 'fleet')!r}, {str(ROOT / 'src')!r}]
import pytest, pbrun
from test_pbrun_detach import _checkout, _queue, _run_pbrun
root = Path({str(tmp_path)!r})
patch = pytest.MonkeyPatch()
patch.setenv("PRISMABUILD_SLURM_LANE_ROOT", str(root / "lane"))
patch.setenv("PRISMABUILD_SLURM_JOB_STATE_ROOT", str(root / "job-state"))
work = _checkout(root)
queue = _queue(root)
first = io.StringIO()
with contextlib.redirect_stdout(first):
    assert _run_pbrun(root, patch, work, "--detach", "--transport", "pool") == 0
row = json.loads(first.getvalue())
fifo = Path(row["submission"])
fifo.unlink()
os.mkfifo(fifo)
original = Path.read_text
def observed(path, *args, **kwargs):
    if path == fifo:
        Path({str(marker)!r}).write_text("entered", encoding="utf-8")
    return original(path, *args, **kwargs)
Path.read_text = observed
def no_publish(*args, **kwargs):
    raise AssertionError("unavailable attachment must not publish")
pbrun.publish_or_refuse = no_publish
pbrun.ATTACHMENT_READ_TIMEOUT_S = 0.05
rc = _run_pbrun(root, patch, work, "--detach", "--transport", "pool")
assert fifo.is_fifo()
assert sorted(p.name for p in fifo.parent.iterdir()) == [fifo.name]
print(rc)
'''
    try:
        completed = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True,
            timeout=5.0, check=False,
        )
    except subprocess.TimeoutExpired:
        pytest.fail("detached attachment exceeded its reader budget; causal-marker="
                    + str(marker.exists()))
    assert completed.returncode == 0, completed.stderr
    assert marker.read_text(encoding="utf-8") == "entered"
    assert completed.stdout == f"{pbrun.RECORD_WRITE_FAILED_EXIT}\n"
    assert "unavailable detached attachment" in completed.stderr
    assert "attachment discovery timed out" in completed.stderr


@pytest.mark.parametrize("state,age,ended,attaches", [
    (pool.READY, None, False, True),
    (pool.CLAIMED, 0.0, False, True),
    (pool.CLAIMED, pool.LEASE_TIMEOUT_S + 60, False, False),
    (pool.READY, None, True, False),
])
def test_pool_attachment_keeps_generation_and_liveness(
    tmp_path: Path, state, age, ended, attaches,
) -> None:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    key = "b" * 64
    generation = 123.0
    row = {"action_key": key, "published_unix": generation}
    record = queue.item_path(state, key)
    record.write_text(json.dumps(row))
    if age is not None:
        queue.lease_path(key).write_text(json.dumps({"heartbeat_unix": time.time() - age}))
    if ended:
        queue.item_path(pool.DONE, key).write_text(json.dumps({
            **row, "status": "executed", "detail": {"returncode": 0}}))
    result = pbrun.bounded_attachment(queue, key, lane_root=tmp_path / "lane")
    if attaches:
        assert result == {"transport": "pool", "generation": generation,
                          "submission": str(record), "job_id": ""}
    else:
        assert result is None


@pytest.mark.parametrize("state,attaches", [("RUNNING", True), ("COMPLETED", False),
                                           (None, False)])
def test_slurm_query_remains_in_parent_after_bounded_record_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, state, attaches,
) -> None:
    parent = os.getpid()
    observed = tmp_path / "reader-pid"
    directory = tmp_path / "lane"
    submission = {"job_id": "41", "attempt": 2, "directory": str(directory)}

    def records(*_args, **_kwargs):
        assert os.getpid() != parent
        observed.write_text(str(os.getpid()))
        return "slurm", 200.0, submission

    queries = []

    def query(job_id, **kwargs):
        assert os.getpid() == parent
        queries.append((job_id, kwargs))
        return None if state is None else (state, {})

    monkeypatch.setattr(pbrun, "outstanding_submission", records)
    monkeypatch.setattr(pbrun, "landed_outcome", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pbrun.slurm_lane, "query_state", query)
    result = pbrun.bounded_attachment(
        pool.PoolQueue(tmp_path / "queue"), "c" * 64, squeue="private-squeue")
    assert int(observed.read_text()) != parent
    assert queries == [("41", {"squeue": "private-squeue"})]
    if attaches:
        assert result["generation"] == 200.0
        assert result["job_id"] == "41"
        assert result["submission"] == str(pbrun.slurm_lane.submission_record_path(
            directory, published_unix=200.0, attempt=2))
    else:
        assert result is None


def test_retained_reader_refuses_before_scheduler_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def retained(_section, _read, *, deadline, abandoned):
        abandoned.append({"pid": 12345, "starttime_ticks": 67890})
        return {"status": "ok", "value": {"transport": "slurm",
                "generation": 200.0, "job_id": "41", "submission": "unused"}}

    monkeypatch.setattr(pbrun.pbstatus, "bounded", retained)
    monkeypatch.setattr(pbrun.slurm_lane, "query_state", lambda *_args, **_kwargs:
                        pytest.fail("retained reader must stop discovery"))
    with pytest.raises(pbrun.OutcomeReadUnavailable, match='"starttime_ticks": 67890'):
        pbrun.bounded_attachment(pool.PoolQueue(tmp_path / "queue"), "c" * 64)


def test_claim_lease_read_is_inside_attachment_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    key = "d" * 64
    queue.item_path(pool.CLAIMED, key).write_text(json.dumps({
        "action_key": key, "published_unix": 123.0}))
    os.mkfifo(queue.lease_path(key))
    monkeypatch.setattr(pbrun, "ATTACHMENT_READ_TIMEOUT_S", 0.05)
    with pytest.raises(pbrun.OutcomeReadUnavailable, match="attachment discovery timed out"):
        pbrun.bounded_attachment(queue, key, lane_root=tmp_path / "lane")


def test_attachment_setup_failure_returns_74_without_publishing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    from test_pbrun_detach import _checkout, _queue, _run_pbrun

    work = _checkout(tmp_path)
    queue = _queue(tmp_path)

    def failed(*_args, **_kwargs):
        raise OSError("reader setup refused")

    monkeypatch.setattr(pbrun, "bounded_attachment", failed)
    monkeypatch.setattr(pbrun, "publish_or_refuse", lambda *_args, **_kwargs:
                        pytest.fail("failed attachment must not publish"))
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach") == 74
    assert "reader setup refused" in capsys.readouterr().err
    assert not list((queue.root / pool.READY).glob("*.json"))

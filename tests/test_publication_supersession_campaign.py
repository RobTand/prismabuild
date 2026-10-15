"""A publication ending stops a campaign while real queue occupancy remains."""
from __future__ import annotations

import errno
import json
from pathlib import Path

import pytest

from prismabuild import pool
import pbcampaign
import pbrun

from test_pool_requeue_preserves_newer_generation import (
    DEMAND, KEY, _end_claim, _publish, clock,
)
from test_publication_supersession import _ready


def _campaign(tmp_path, monkeypatch, clock, successor_state):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    _publish(queue)
    original = queue.claim(owner="original", capacity=DEMAND)
    assert original is not None
    clock[0] += 1
    _publish(queue)
    successor_generation = _ready(queue)["published_unix"]
    _end_claim(queue, original, "finish_retry", clock)
    if successor_state != "ready":
        successor = queue.claim(owner="successor", capacity=DEMAND)
        assert successor is not None
        assert successor["published_unix"] == successor_generation
        if successor_state == "cleanup":
            def unavailable_release(*args, **kwargs):
                raise OSError(errno.EIO, "release interrupted")

            with monkeypatch.context() as patch:
                patch.setattr(queue, "_release_reservation", unavailable_release)
                with pytest.raises(OSError, match="release interrupted"):
                    queue.finish(KEY, status="executed", claim_snapshot=successor)
            assert list(queue.dir(pool.CLAIMED).glob(f"{KEY}.*{pool.TOMBSTONE_SUFFIX}"))
        elif successor_state == "drained":
            queue.finish(KEY, status="executed", claim_snapshot=successor)
    submitted = []

    def attached(row, *, transport):
        submitted.append(row["argv"][-1])
        return {"status": "attached", "transport": "pool", "action_key": KEY,
                "published_unix": original["published_unix"]}

    monkeypatch.setattr(pbcampaign, "submit_row", attached)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([
        {"argv": ["true", "prefix"], "cwd": str(tmp_path)},
        {"argv": ["true", "suffix"], "cwd": str(tmp_path)},
    ]))
    return queue, manifest, submitted


@pytest.mark.parametrize("successor_state", ["ready", "claimed", "cleanup", "drained"])
def test_supersession_stops_suffix_and_preserves_actual_work_or_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: list[float],
    successor_state: str, capsys: pytest.CaptureFixture[str],
) -> None:
    queue, manifest, submitted = _campaign(tmp_path, monkeypatch, clock, successor_state)
    before = {
        path: path.read_bytes()
        for state in (pool.READY, pool.CLAIMED)
        for path in queue.dir(state).iterdir() if path.is_file()
    }
    held = queue.ledger().held()

    code = pbcampaign.main([
        "--transport", "pool", "--max-inflight", "1", "--wait-s", "0", str(manifest),
    ])

    assert code == 76
    assert submitted == ["prefix"]
    output = capsys.readouterr().out
    assert "superseded" in output
    assert "not_submitted" in output
    assert pbcampaign._pool_slot_occupied(queue, KEY) is (successor_state != "drained")
    assert {path: path.read_bytes() for path in before} == before
    assert queue.ledger().held() == held


def test_unknown_occupancy_reports_io_error_and_never_submits_the_suffix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: list[float],
) -> None:
    queue, manifest, submitted = _campaign(tmp_path, monkeypatch, clock, "ready")
    ready = queue.item_path(pool.READY, KEY)
    raw = ready.read_bytes()
    real_lstat = Path.lstat

    def unavailable_occupancy(path, *args, **kwargs):
        if path == ready:
            raise OSError(errno.EIO, "ready occupancy unavailable")
        return real_lstat(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "lstat", unavailable_occupancy)
        code = pbcampaign.main([
            "--transport", "pool", "--max-inflight", "1", "--wait-s", "0", str(manifest),
        ])

    assert code == 74
    assert submitted == ["prefix"]
    assert ready.read_bytes() == raw

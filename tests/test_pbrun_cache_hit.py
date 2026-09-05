"""An attached ``pbrun --transport slurm`` asks the CAS before ``sbatch``.

Before this, only ``--detach`` did.  The attached path submitted a job for
work the CAS already held, and the node reported the cache hit after
materializing a checkout for nothing (three-node smoke, row M8 as first
written).
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool  # noqa: E402
import pbrun  # noqa: E402

KEY = "ab" * 32


class _Receipted:
    def __init__(self, receipt):
        self.receipt = receipt

    def lookup(self, action):
        return self.receipt


def _outcome(monkeypatch, tmp_path, *, receipt):
    def never(*args, **kwargs):
        raise AssertionError("sbatch was reached for receipted work")

    monkeypatch.setattr(pbrun.slurm_lane, "run", never)
    monkeypatch.setattr(pbrun.slurm_lane, "resume", never)
    return pbrun.slurm_outcome(
        {"action_key": KEY}, cas=_Receipted(receipt), request_path="r.json",
        tags=["gb10"], demand={"cpu": 2, "mem_gb": 8}, exclusive=False,
        timeout_s=None, wait_s=30.0, retry_safe=False, max_attempts=1,
        queue_root=tmp_path / "queue", lane_root=tmp_path / "lane",
    )


def test_receipted_work_is_not_submitted_and_is_reported_done(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    rc = _outcome(monkeypatch, tmp_path, receipt={"result_digest": "d" * 64})
    err = capsys.readouterr().err
    assert rc == 0
    assert f"{KEY[:12]} is already in the CAS; nothing submitted" in err
    assert "cache_hit on" in err
    assert not (tmp_path / "lane").exists()


def test_a_key_with_no_done_record_gets_a_cache_hit_record(
    monkeypatch, tmp_path: Path
) -> None:
    _outcome(monkeypatch, tmp_path, receipt={"result_digest": "d" * 64})
    filed = json.loads(
        (tmp_path / "queue" / pool.DONE / f"{KEY}.json").read_text())
    assert filed["status"] == "cache_hit"
    assert filed["transport"] == "slurm"
    assert filed["attempts"] == 0
    assert filed["tags"] == ["gb10"]
    assert filed["resources"] == {"cpu": 2, "mem_gb": 8}
    assert filed["detail"]["receipt_published"] is True
    assert filed["detail"]["result_digest"] == "d" * 64
    assert not (tmp_path / "queue" / pool.FAILED).exists()


def test_the_runs_own_done_record_is_left_as_it_was(
    monkeypatch, tmp_path: Path
) -> None:
    done = tmp_path / "queue" / pool.DONE
    done.mkdir(parents=True)
    original = json.dumps({"status": "executed", "action_key": KEY,
                           "detail": {"slurm": {"job_id": "41"}}})
    (done / f"{KEY}.json").write_text(original)
    _outcome(monkeypatch, tmp_path, receipt={"result_digest": "d" * 64})
    assert (done / f"{KEY}.json").read_text() == original


def test_unreceipted_work_still_reaches_the_scheduler(
    monkeypatch, tmp_path: Path
) -> None:
    reached = {}

    def record(action, **kwargs):
        reached["called"] = True
        raise pbrun.slurm_lane.SlurmLaneError("stop here")

    monkeypatch.setattr(pbrun.slurm_lane, "run", record)
    with pytest.raises(SystemExit):
        pbrun.slurm_outcome(
            {"action_key": KEY}, cas=_Receipted(None), request_path="r.json",
            tags=[], demand={"cpu": 1, "mem_gb": 4}, exclusive=False,
            timeout_s=None, wait_s=30.0, retry_safe=False, max_attempts=1,
            queue_root=tmp_path / "queue", lane_root=tmp_path / "lane",
        )
    assert reached == {"called": True}

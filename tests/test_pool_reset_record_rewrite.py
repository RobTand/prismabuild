"""What ``pool_reset`` leaves behind when it marks an ending ``reset``.

The tool resets the work, not the record, so the record it rewrites has three
jobs to keep doing: another box's reader must never see it half written, a
reader must still be able to summarize it, and the failure evidence must still
be there to read. The rewrite did none of the three. It used
``path.write_text``, which truncates and then writes, while every other writer
in the queue publishes by rename. It changed ``status`` without touching
``attempt_history``, so ``pbrun.outcome_summary`` refused the record against the
attempt it adopted. And it replaced ``detail`` outright with the reason,
destroying the returncode and the output tails with no copy taken.
"""

from __future__ import annotations

import contextlib
import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import pbrun  # noqa: E402
import pool_reset  # noqa: E402


KEY_POOL = "d" * 64
KEY_SLURM = "e" * 64


@pytest.fixture()
def endings(tmp_path: Path) -> dict:
    """One pull-queue failure and one lane-filed failure, in ``failed/``.

    The two shapes differ in the way that matters here: the pull queue links
    immutable attempts and the lane files none, so only one of them can reach
    the adoption at all, while both carry evidence the reset can destroy.
    """

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.publish(
        action_key=KEY_POOL,
        cas_root=str(tmp_path / "cas"),
        checkout_root=str(tmp_path / "checkout"),
        worker_script=str(tmp_path / "worker.py"),
        max_attempts=1,
    )
    assert queue.claim() is not None
    pool_path = queue.finish(
        KEY_POOL,
        status="failed",
        detail={
            "returncode": 7,
            "stdout": "3 failed, 1 passed\n",
            "stderr": "declared result path must be absent before execution\n",
            "elapsed_s": 12.0,
        },
    )
    assert pool_path == queue.item_path(pool.FAILED, KEY_POOL)

    lane_path = queue.item_path(pool.FAILED, KEY_SLURM)
    lane_path.write_text(json.dumps({
        "schema": "prismaquant.prismabuild.slurm_outcome.v1",
        "action_key": KEY_SLURM,
        "published_unix": 1000.0,
        "status": "failed",
        "attempts": 1,
        "transport": "slurm",
        "detail": {
            "returncode": 9,
            "stderr": "sbatch job hit its wall clock\n",
            "slurm": {"job_id": "1001", "state": "TIMEOUT",
                      "gres": pool_reset.EXCLUSIVE_GRES},
        },
    }), encoding="utf-8")

    return {"queue": queue, "pool_path": pool_path, "lane_path": lane_path}


def test_the_rewrite_is_published_by_rename(
    endings: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No reader on another box can see a half-written terminal record.

    main: the reset never reaches truncate-then-write, so a failure inside
    ``write_text`` cannot tear the record.
    branch: what is on disk afterwards is a complete record that says ``reset``.
    """

    path = endings["pool_path"]

    def _tears(self: Path, data: str, *args: object, **kwargs: object) -> int:
        """Truncate the file, write half the bytes, then fail."""

        raw = data.encode("utf-8")
        self.write_bytes(raw[: len(raw) // 2])
        raise OSError("the writer died between the truncate and the write")

    monkeypatch.setattr(Path, "write_text", _tears)
    with contextlib.suppress(OSError):
        pool_reset._file_reset({"paths": [path]}, reason="operator reset")

    record = pool._read_json(path)
    assert record is not None
    assert record["status"] == "reset"


def test_a_reset_record_is_still_readable(endings: dict) -> None:
    """``pool_reset`` must not leave an ending its own readers refuse.

    main: ``outcome_summary`` returns the reset rather than raising on the
    attempt it would otherwise adopt.
    branch: the ending is still counted as this key's, under the new status.
    """

    path = endings["pool_path"]
    pool_reset._file_reset({"paths": [path]}, reason="operator reset")

    record = json.loads(path.read_text(encoding="utf-8"))
    summary = pbrun.outcome_summary(endings["queue"], path, record)
    assert summary["status"] == "reset"
    assert summary["action_key"] == KEY_POOL


@pytest.mark.parametrize(
    "which,returncode,tail",
    [
        ("pool_path", 7, "declared result path must be absent"),
        ("lane_path", 9, "wall clock"),
    ],
)
def test_the_failure_evidence_survives_the_reset(
    endings: dict, which: str, returncode: int, tail: str
) -> None:
    """The tool resets the work, not the record.

    main: the returncode and the output tail the failure recorded are still
    readable after the reset, under both transports.
    branch: so is the GRES the lane recorded, which is how a later reset knows
    the action had the device to itself.
    """

    path = endings[which]
    pool_reset._file_reset({"paths": [path]}, reason="operator reset")
    record = json.loads(path.read_text(encoding="utf-8"))

    assert record["status"] == "reset"
    assert record["detail"]["returncode"] == returncode
    assert tail in record["detail"]["stderr"]
    if which == "lane_path":
        assert pool_reset.record_exclusive(record)

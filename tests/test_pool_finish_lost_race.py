"""A reaper's conclusion is the terminal, and a lost race writes no second one.

``finish`` reaches its lost-race branch when a reaper concluded the claim while
the launcher was still running it. The branch used to pick ``done/`` or
``failed/`` from the launcher's own success alone, check only that one path, and
write a record carrying no ``published_unix``. When the reaper had already filed
``failed/<key>.json`` for the same generation and the launcher then succeeded,
both directories held a record for one key. The unstamped ``done/`` record
scores ``-inf`` in ``pbrun.landed_outcome``, so the caller was told the action
failed while its receipt sat in the CAS, ``pool_reset`` offered to re-run it,
and ``reclaim_terminal_reservation`` refused the key for having two terminals.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import pbrun  # noqa: E402


KEY = "c" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    value = pool.PoolQueue(tmp_path / "queue")
    value.ensure_layout()
    value.publish(
        action_key=KEY,
        cas_root=str(tmp_path / "cas"),
        checkout_root=str(tmp_path / "checkout"),
        worker_script=str(tmp_path / "worker.py"),
        max_attempts=1,
    )
    return value


def _expire_the_lease(queue: pool.PoolQueue) -> None:
    """Age this claim's heartbeat past the reaper's timeout."""

    path = queue.lease_path(KEY)
    lease = json.loads(path.read_text(encoding="utf-8"))
    lease["heartbeat_unix"] = time.time() - 10_000
    path.write_text(json.dumps(lease), encoding="utf-8")


def test_a_reaper_that_already_concluded_the_claim_keeps_the_terminal(
    queue: pool.PoolQueue,
) -> None:
    """The winner's record stands, and the loser writes nothing beside it.

    main: ``done/`` stays empty and ``finish`` returns the reaper's ``failed/``
    record, so one key has one ending.
    branch: ``pbrun`` reports that ending rather than scoring an unstamped
    record against it.
    """

    snapshot = queue.claim()
    assert snapshot is not None
    _expire_the_lease(queue)
    assert queue.reap_stale() == [KEY]
    assert queue.item_path(pool.FAILED, KEY).exists()

    landed = queue.finish(
        KEY,
        status="executed",
        detail={"returncode": 0, "elapsed_s": 5.0},
        claim_snapshot=snapshot,
    )

    assert not queue.item_path(pool.DONE, KEY).exists()
    assert landed == queue.item_path(pool.FAILED, KEY)

    found = pbrun.landed_outcome(queue, KEY, wait_s=0.1)
    assert found is not None
    path, record = found
    assert Path(path).parent.name == pool.FAILED
    assert record["status"] == "lease_lost_max_attempts"


def test_an_uncovered_lost_race_files_a_generation_stamped_record(
    queue: pool.PoolQueue,
) -> None:
    """Nobody filed a terminal, so this one is filed and is addressable.

    main: the record carries the claim snapshot's ``published_unix``, so a
    caller that names the generation finds it.
    branch: it links no immutable attempt, because this worker archived none.
    """

    snapshot = queue.claim()
    assert snapshot is not None
    generation = snapshot["published_unix"]
    # The claim file vanishes under the launcher, with no terminal filed: the
    # shape a reaper leaves when it concludes a claim it cannot describe.
    queue.item_path(pool.CLAIMED, KEY).unlink()

    landed = queue.finish(
        KEY,
        status="failed",
        detail={"returncode": 1, "stderr": "boom\n"},
        claim_snapshot=snapshot,
    )

    assert landed == queue.item_path(pool.FAILED, KEY)
    record = json.loads(landed.read_text(encoding="utf-8"))
    assert record["published_unix"] == generation
    assert record["status"] == "finish_lost_race"
    assert "attempt_history" not in record
    assert "attempt_history_missing_before" not in record

    found = pbrun.landed_outcome(queue, KEY, wait_s=0.1, generation=generation)
    assert found is not None
    assert pbrun.outcome_summary(queue, found[0], found[1])["status"] == (
        "finish_lost_race"
    )

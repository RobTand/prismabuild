"""One unreadable terminal record must not take a worker's own ending with it.

``finish``'s lost-race branch, reached when a reaper concluded the claim while
the launcher was still running, asks ``terminal_outcome_covers`` which
generation has already been filed. That read goes through ``_read_json``,
which raises ``PoolContractError`` on a record that is not valid JSON. Before
PR #52 the branch wrote its outcome without reading anything, so a torn
terminal for one key now ends the ``serve_once`` call rather than the key.

The exposure is narrow: the raise costs this worker one consecutive error and
the key is already concluded by the winner. It is still the wrong shape. The
key HAS a terminal, filed by the reaper; it is only unreadable, and the reader
that must report that is the one an operator is looking at. Filing a second
ending beside it would be the two-terminals defect PR #52 fixed, so this
branch reports the terminal it found and writes nothing.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))

from prismabuild import pool  # noqa: E402

KEY = "a" * 64


@pytest.fixture()
def lost(tmp_path: Path) -> tuple[pool.PoolQueue, dict]:
    """A claim a reaper concluded, whose terminal record is torn."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.publish(
        action_key=KEY,
        cas_root=str(tmp_path / "cas"),
        checkout_root=str(tmp_path / "checkout"),
        worker_script=str(tmp_path / "worker.py"),
    )
    item = queue.claim()
    assert item is not None
    # The reaper's half: the claim is gone and a terminal stands in its place.
    # Torn, which is what a foreign writer or a half-landed copy leaves.
    queue.item_path(pool.CLAIMED, KEY).unlink()
    queue.lease_path(KEY).unlink(missing_ok=True)
    queue.item_path(pool.FAILED, KEY).write_text(
        '{"action_key": "' + KEY + '", "status": "fail', encoding="utf-8"
    )
    return queue, item


def test_a_torn_terminal_does_not_raise_out_of_finish(
    lost: tuple[pool.PoolQueue, dict]
) -> None:
    """The launcher's own ending, against a terminal nobody can read.

    main: ``finish`` returns the terminal that is already there instead of
    raising, so the worker survives the item.
    branch: nothing is written, because a second ending beside the first is
    the defect PR #52 removed and the record is unreadable, not absent.
    """

    queue, item = lost

    filed = queue.finish(
        KEY, status="executed", detail={"returncode": 0}, claim_snapshot=item
    )

    assert filed == queue.item_path(pool.FAILED, KEY)
    assert not queue.item_path(pool.DONE, KEY).exists()
    # Untouched: the operator's reader reports it unreadable, and a repair
    # that guessed at its content would be inventing an ending.
    assert queue.item_path(pool.FAILED, KEY).read_text(
        encoding="utf-8").endswith('"status": "fail')


def test_a_readable_terminal_still_decides(lost: tuple[pool.PoolQueue, dict]) -> None:
    """The handler covers an unreadable record and nothing else.

    branch: a terminal of this generation that reads is still returned as it
    stands, which is PR #52's contract for this branch.
    """

    queue, item = lost
    queue.item_path(pool.FAILED, KEY).write_text(json.dumps({
        "action_key": KEY,
        "status": "failed",
        "published_unix": item["published_unix"],
    }), encoding="utf-8")

    filed = queue.finish(
        KEY, status="executed", detail={"returncode": 0}, claim_snapshot=item
    )

    assert filed == queue.item_path(pool.FAILED, KEY)
    assert not queue.item_path(pool.DONE, KEY).exists()

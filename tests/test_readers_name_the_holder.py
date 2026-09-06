"""``pbstatus`` and ``pbwait`` name the box the work was on.

A concluded record names two boxes and never conflates them: ``claimed_host``
is where the action was, ``finished_host`` is who filed the ending.  For a
lost claim those are different machines, because ``reap_stale`` stamps the
reaping box -- correctly, it is the box that wrote the record.

Both readers preferred ``finished_host``.  The reaper files most of the
fleet's lost claims, so that field is the sweeping box far more often than it
is the box the work was on, and an operator reading either surface was told
the failure belonged to whichever machine happened to notice.  The bias is not
cosmetic: the box that reaps most is the box that runs most, so misattributed
failures accumulate on the machine that already looks busiest.

#227 fixed this in ``pbrun`` and in ``reap_stale``.  These two readers were
outside its scope.

Issues #227, #262.
"""
from __future__ import annotations

import json
from pathlib import Path
import socket
import sys
from unittest import mock

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402

import pbstatus  # noqa: E402
import pbwait  # noqa: E402

KEY = "e" * 64
#: A claiming box that is never the reaping box.  Derived, not named: the
#: suite runs on the fleet, and a literal hostname is the local host on one of
#: the boxes that runs it -- where the two collapse into one name and the test
#: stops being able to tell them apart.
HELD_BY = f"not-{socket.gethostname()}"
REAPED_BY = socket.gethostname()


@pytest.fixture()
def lost_claim(tmp_path: Path) -> tuple[pool.PoolQueue, Path, dict]:
    """One claim held on ``HELD_BY`` and reaped here, as a terminal record.

    Produced by the queue rather than written by hand: the two hostnames come
    apart only because ``claim`` and ``reap_stale`` ran on different boxes,
    and a record typed out with both fields would assert the typing.
    """

    q = pool.PoolQueue(tmp_path / "queue")
    q.publish(
        action_key=KEY, cas_root=q.root / "cas", checkout_root=q.root / "co",
        worker_script=q.root / "worker.py", resources={"cpu": 1},
        max_attempts=1, retry_safe=True,
    )
    with mock.patch.object(pool.socket, "gethostname", lambda: HELD_BY):
        assert q.claim(owner=f"{HELD_BY}:1:abcd1234", capacity={"cpu": 1})

    later = pool._now() + pool.LEASE_TIMEOUT_S
    with mock.patch.object(pool, "_now", lambda: later):
        assert q.reap_stale(timeout_s=-1) == [KEY]

    path = q.item_path(pool.FAILED, KEY)
    record = json.loads(path.read_text())
    assert record["claimed_host"] == HELD_BY
    assert record["finished_host"] == REAPED_BY
    return q, path, record


def test_pbstatus_names_the_holder(lost_claim) -> None:
    """The HOST column is where the action was."""

    q, _, _ = lost_claim
    rows = pbstatus.read_endings(q.root, limit=10)

    row = next(row for row in rows if row["action_key"] == KEY)
    assert row["host"] == HELD_BY, (
        "pbstatus sent the reader at the box that swept, not the box that ran")


def test_pbwait_names_the_holder(lost_claim) -> None:
    """So is the one ``pbwait`` prints when the wait ends."""

    q, path, record = lost_claim

    row = pbwait._from_record(q, path, record)
    assert row["host"] == HELD_BY


def test_an_ending_with_no_claimant_still_names_a_box(tmp_path: Path) -> None:
    """The fallback is kept, because some endings name only their filer.

    A cache hit and a waiter-filed SLURM record carry ``finished_host`` and no
    claimant.  Preferring the holder must not turn those rows into ``-``: an
    unnamed box is the answer that sends nobody anywhere, which is the failure
    this change exists to stop.
    """

    q = pool.PoolQueue(tmp_path / "queue")
    q.ensure_layout()
    pool._write_json_atomic(q.item_path(pool.DONE, KEY), {
        "action_key": KEY, "status": "cache_hit", "transport": "slurm",
        "finished_host": REAPED_BY, "finished_unix": pool._now(),
        "detail": {"elapsed_s": 0.0, "returncode": 0},
    })
    record = json.loads(q.item_path(pool.DONE, KEY).read_text())

    rows = pbstatus.read_endings(q.root, limit=10)
    assert next(row for row in rows if row["action_key"] == KEY)["host"] == REAPED_BY

    row = pbwait._from_record(q, q.item_path(pool.DONE, KEY), record)
    assert row["host"] == REAPED_BY


def test_the_two_readers_agree_with_pbrun(lost_claim) -> None:
    """One vocabulary across all three surfaces, or the operator picks one.

    ``pbrun`` says "held by X, reaped by Y" for this record.  A status table
    and a waiter that answered ``Y`` to the same question left the reader to
    decide which tool to believe.
    """

    q, path, record = lost_claim
    import pbrun

    summary = pbrun.outcome_summary(q, path, record)
    assert f"held by {HELD_BY}" in pbrun.outcome_headline(summary)

    status_row = next(row for row in pbstatus.read_endings(q.root, limit=10)
                      if row["action_key"] == KEY)
    wait_row = pbwait._from_record(q, path, record)
    assert status_row["host"] == wait_row["host"] == summary["claimed_host"]

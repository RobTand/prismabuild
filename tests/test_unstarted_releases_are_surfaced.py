"""A release without an attempt is counted, and now something reads the count.

Issue #263, following #222.  ``reap_stale`` returns a claim that never wrote a
lease and never published an attempt to ``ready`` with its attempt count
untouched, and raises ``unstarted_releases`` on the item.  Nothing read that
counter: not ``pbstatus``, not ``pbmetrics``.  A key bouncing between a box
that cannot get its lease written and the reaper that releases it never
reaches a terminal state, so ``pbrun`` waits out its whole ``--wait-s`` while
the queue looks quiet.

Two readers, because they answer different questions:

*   ``pbstatus`` -- a column and a reason, for "why is this not moving?"
*   ``pbmetrics`` -- two series, for "is this happening more than it was?"

The per-box series cannot come from the item.  ``_shape_as_ready_item`` pops
every claim-scoped field, ``claimed_host`` among them, so by the time the
counter is readable in ``ready`` the record no longer says which box was
holding it.  The filing under ``withdrawn/superseded/`` does, because
``_file_superseded`` is handed the claimed record before the pops.  So the
queue-wide count is read from the census and the per-box count from the
filings, and this file pins both to the same released claim.

Every fixture here is produced by the real reaper rather than written by hand:
the input is a claim whose lease was removed, and ``pool.reap_stale`` decides
what that becomes.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools/fleet"))

from prismabuild import pool  # noqa: E402
import pbstatus  # noqa: E402
import pbmetrics  # noqa: E402

BOUNCING = "d" * 64
QUIET = "e" * 64
BOX = "sparklina"


@pytest.fixture()
def released(tmp_path, monkeypatch):
    """One action the reaper released, and one it never touched.

    Returns the queue and the moment the release was filed at, which is what
    the metrics window is measured against.
    """

    monkeypatch.setattr(pool.socket, "gethostname", lambda: BOX)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.announce(host=BOX, tags=[BOX], has_gpu=False,
                   capacity={"cpu": 4, "mem_gb": 8},
                   observed_capacity={"cpu": 4, "mem_gb": 8})
    for key in (BOUNCING, QUIET):
        queue.publish(action_key=key, cas_root=tmp_path / "cas",
                      checkout_root=tmp_path, tags=[BOX],
                      worker_script=ROOT / "tools/prismabuild_worker.py",
                      resources={"cpu": 1})
    assert queue.claim(tags=[BOX], has_gpu=False,
                       owner=f"{BOX}:43944:a4bcd411") is not None
    claimed = [path.stem for path in queue.dir(pool.CLAIMED).glob("*.json")]
    assert claimed == [BOUNCING], "the fixture claimed the wrong action"
    # The claimant blocked between the rename and ``write_lease``.  This is
    # the measured shape of issue #222, not a synthesized record.
    queue.lease_path(BOUNCING).unlink()

    later = pool._now() + pool.LEASE_TIMEOUT_S
    with mock.patch.object(pool, "_now", lambda: later):
        assert queue.reap_stale(timeout_s=pool.LEASE_TIMEOUT_S) == [BOUNCING]

    item = json.loads(queue.item_path(pool.READY, BOUNCING).read_text())
    assert item["unstarted_releases"] == 1, "the fixture did not produce a release"
    return queue, later


def _row(queue, key):
    census = pbstatus.read_pool(queue.root)
    return next(job for job in census["jobs"] if job["action_key"] == key)


def test_pbstatus_carries_the_count_on_the_row(released):
    queue, _ = released
    assert _row(queue, BOUNCING)["unstarted_releases"] == 1


def test_pbstatus_says_it_in_the_reason_an_operator_reads(released):
    """The reason column is the "why is this not moving" answer.

    Without this it reads "awaiting admission", which is true of a key nobody
    has got to yet and of a key being handed back every thirty seconds.
    """

    reason = _row(queue := released[0], BOUNCING)["reason"]
    assert "released 1 time before starting" in reason
    assert "released" not in _row(queue, QUIET)["reason"]


def test_the_job_table_has_a_releases_column(released):
    queue, _ = released
    census = pbstatus.read_pool(queue.root)
    lines = pbstatus.pool_job_lines(census["jobs"], census["queue"])
    assert "RELEASES" in lines[0]
    column = lines[0].index("RELEASES")
    bouncing = next(line for line in lines[1:] if line.startswith(BOUNCING[:12]))
    assert bouncing[column:].split()[0] == "1"


def test_an_action_never_released_reads_absent_rather_than_zero(released):
    """A measured zero and "this never happened" are different answers."""

    assert _row(released[0], QUIET)["unstarted_releases"] is None


def _samples(text, name):
    return sorted(line for line in text.splitlines()
                  if line.startswith(f"{name}{{") or line.startswith(f"{name} "))


def test_pbmetrics_counts_what_the_queue_is_still_holding(released):
    queue, later = released
    text = pbmetrics.collect_metrics(queue.root, now=later)
    assert 'prismabuild_queue_unstarted_releases{state="ready"} 1' in text
    assert 'prismabuild_queue_unstarted_releases{state="claimed"} 0' in text


def test_pbmetrics_names_the_box_that_held_the_claim(released):
    """The signal is a rate on ONE box: its own filesystem latency."""

    queue, later = released
    text = pbmetrics.collect_metrics(queue.root, now=later)
    assert _samples(text, "prismabuild_unstarted_release_events") == [
        f'prismabuild_unstarted_release_events{{host="{BOX}"}} 1',
    ]
    assert "prismabuild_unstarted_release_events_complete 1" in text


def test_a_release_older_than_the_window_is_not_in_the_rate(released):
    """A gauge over a bounded window, like every other recent-window family."""

    queue, later = released
    text = pbmetrics.collect_metrics(
        queue.root, now=later + 7200.0, terminal_window_seconds=3600.0)
    assert _samples(text, "prismabuild_unstarted_release_events") == []
    # The item is still in the queue carrying its count, and that series is
    # not windowed: it reports what the queue holds now.
    assert 'prismabuild_queue_unstarted_releases{state="ready"} 1' in text


def test_a_fleet_that_never_released_a_claim_reports_zero(tmp_path, monkeypatch):
    """No directory is not a failed scrape."""

    monkeypatch.setattr(pool.socket, "gethostname", lambda: BOX)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    text = pbmetrics.collect_metrics(queue.root, now=pool._now())
    assert _samples(text, "prismabuild_unstarted_release_events") == []
    assert "prismabuild_unstarted_release_events_complete 1" in text
    assert 'prismabuild_queue_unstarted_releases{state="ready"} 0' in text

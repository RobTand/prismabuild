"""A live box's offer, and a slow offer scan, must not refuse runnable work (#560).

A GLM campaign under load hit both of these:

* ``no recorded worker can run this action. required tags: ['dl380g10']``
  while dl380g10 was live.  Its loops rewrite ``workers/dl380g10.json`` with
  ``os.replace`` every poll.  On NFS a read that opens the old file can get
  ``ESTALE``, and ``_offer_records`` dropped the host for that scan.
* ``worker-offer discovery timed out after 5.005s; refusing submission``.
  ``run_windowed`` stopped the whole campaign on that one refusal, although
  pbrun had published nothing.

The guards pin the refusals that must stay: a tag no worker offers, an offer
that stays unreadable, and a reader that survives cleanup.
"""

from __future__ import annotations

import errno
import os
import signal
import socket
import sys
import time
from pathlib import Path

import pytest

from test_pbcampaign import fleet_paths, _manifest, _row  # noqa: F401
from prismabuild import pool
import pbcampaign
import pbrun


#: The production offer-read budget, before a test shortens it for a FIFO.
PRODUCTION_OFFER_READ_TIMEOUT_S = pbrun.SUBMISSION_OFFER_READ_TIMEOUT_S

X86 = {"tags": ["x86"], "needs_gpu": False, "resources": {"mem_gb": 4}}


def _announce_x86(queue: pool.PoolQueue) -> None:
    queue.announce(host="dl380g10", tags=["cpu", "x86", "dl380g10"], has_gpu=False,
                   capacity={"gpu": 0, "mem_gb": 60, "cpu": 80})


def _unreadable(monkeypatch, name: str, failure: str, *, times: float) -> None:
    """Make the first ``times`` reads of one offer fail the way a replace race does."""

    real = Path.read_bytes
    failed = [0]

    def fake(self: Path) -> bytes:
        if self.name == name and failed[0] < times:
            failed[0] += 1
            if failure == "ESTALE":
                raise OSError(errno.ESTALE, "Stale file handle", str(self))
            if failure == "ENOENT":
                raise FileNotFoundError(errno.ENOENT, "No such file", str(self))
            return b""
        return real(self)

    monkeypatch.setattr(Path, "read_bytes", fake)


@pytest.mark.parametrize("failure", ["ESTALE", "ENOENT", "empty"])
def test_one_failed_read_of_a_live_offer_does_not_drop_the_host(
    tmp_path, monkeypatch, failure,
):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(host="sparky", tags=["gb10", "sparky"], has_gpu=True,
                   capacity={"gpu": 2, "mem_gb": 48, "cpu": 10})
    _announce_x86(queue)
    _unreadable(monkeypatch, "dl380g10.json", failure, times=1)

    assert queue.placeable(X86, max_age_s=float("inf")) is True


def test_an_offer_that_stays_unreadable_is_still_left_out(tmp_path, monkeypatch):
    """The re-read is one more look, not a guess that the box is there (#208)."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(host="sparky", tags=["gb10", "sparky"], has_gpu=True,
                   capacity={"gpu": 2, "mem_gb": 48, "cpu": 10})
    _announce_x86(queue)
    _unreadable(monkeypatch, "dl380g10.json", "ESTALE", times=float("inf"))

    assert [r["host"] for r in queue.offers(max_age_s=float("inf"))] == ["sparky"]
    assert queue.placeable(X86, max_age_s=float("inf")) is False


#: A runtime every box can open, as ``test_pbrun_placement`` declares it.  A
#: tagged submission from a box-local runtime is refused before placement is
#: checked (#292), and placement is what these tests are about.
PUBLISHED_RUNTIME = Path(
    "/mnt/shared/prismabuild-fleet/runtime-generations/test-generation")


def _submit(monkeypatch, work: Path, *tags: str) -> int:
    monkeypatch.setattr(pbrun, "RUNTIME_ROOT", PUBLISHED_RUNTIME)
    # Zero is one observation with the full read budget, then 75 when
    # nothing has landed.  A positive wait caps the first forked reader's
    # budget at the time left, so 10 ms timed it out on a loaded box and the
    # wait ended 74 (#918).
    argv = ["pbrun.py", "--cwd", str(work), "--wait-s", "0"]
    for tag in tags:
        argv += ["--tag", tag]
    monkeypatch.setattr(sys, "argv", [*argv, "--", "echo", "hi"])
    return pbrun.main()


def test_submission_survives_a_stale_read_of_the_only_matching_offer(
    tmp_path, fleet_paths, monkeypatch, capsys,  # noqa: F811
):
    """The first signature, driven through ``pbrun.main`` and its forked reader."""

    work, queue = fleet_paths
    _announce_x86(queue)
    _unreadable(monkeypatch, "dl380g10.json", "ESTALE", times=1)

    assert _submit(monkeypatch, work, "dl380g10") == 75   # queued; nothing claims it
    err = capsys.readouterr().err
    assert "no recorded worker" not in err
    assert "pbrun: queued" in err
    assert len(list(queue.dir(pool.READY).glob("*.json"))) == 1


def test_a_tag_no_worker_offers_still_refuses_at_once(
    tmp_path, fleet_paths, monkeypatch,  # noqa: F811
):
    work, queue = fleet_paths
    _announce_x86(queue)

    with pytest.raises(SystemExit, match="no recorded worker can run this action") as caught:
        _submit(monkeypatch, work, "nosuchbox")
    assert not isinstance(caught.value, getattr(pbrun, "OfferDiscoveryTimedOut", ()))
    assert not list(queue.dir(pool.READY).glob("*.json"))


def test_a_reaped_offer_timeout_is_its_own_refusal(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "pb-queue")

    def blocked_read():
        time.sleep(30)

    monkeypatch.setattr(queue, "_offer_records", blocked_read)
    monkeypatch.setattr(pbrun, "SUBMISSION_OFFER_READ_TIMEOUT_S", 0.2)
    with pytest.raises(SystemExit, match="worker-offer discovery timed out") as caught:
        pbrun.bounded_offer_snapshot(queue)
    assert type(caught.value).__name__ == "OfferDiscoveryTimedOut"
    assert isinstance(caught.value.code, str)          # plain pbrun still exits 1


def test_a_retained_offer_reader_is_not_retryable(tmp_path, monkeypatch):
    """Retrying while the last reader is still alive would race it."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    children = []

    def retain(pid, section, started, abandoned):
        children.append(pid)
        abandoned.append({"pid": pid, "section": section, "starttime_ticks": None})

    def blocked_read():
        time.sleep(30)

    monkeypatch.setattr(queue, "_offer_records", blocked_read)
    monkeypatch.setattr(pbrun.pbstatus, "_stop_reader", retain)
    monkeypatch.setattr(pbrun, "SUBMISSION_OFFER_READ_TIMEOUT_S", 0.2)
    try:
        with pytest.raises(SystemExit, match="retained reader") as caught:
            pbrun.bounded_offer_snapshot(queue)
        assert type(caught.value) is SystemExit
    finally:
        for pid in children:
            try:
                done, _ = os.waitpid(pid, os.WNOHANG)
                if not done:
                    os.kill(pid, signal.SIGKILL)
                    os.waitpid(pid, 0)
            except ChildProcessError:
                pass


def test_campaign_window_retries_an_offer_discovery_timeout(
    tmp_path, fleet_paths, monkeypatch, capsys,  # noqa: F811
):
    """The second signature, through the real ``submit_row`` and ``pbrun.main``.

    The FIFO stands in for a hard-NFS offer read that blocks.  It goes away
    after the first attempt, as a loaded server recovers.
    """

    work, queue = fleet_paths
    fifo = queue.root / pool.WORKERS / "wedged.json"
    os.mkfifo(fifo, 0o600)
    monkeypatch.setattr(pbrun, "SUBMISSION_OFFER_READ_TIMEOUT_S", 0.2)
    attempts = []
    real = pbcampaign._submit_record

    def once_wedged(row, **kwargs):
        attempts.append(kwargs["index"])
        try:
            return real(row, **kwargs)
        finally:
            fifo.unlink(missing_ok=True)
            # The short budget exists to time out the FIFO. The retry's offer
            # read gets the production budget, so a loaded box cannot time it
            # out too (#1170).
            monkeypatch.setattr(pbrun, "SUBMISSION_OFFER_READ_TIMEOUT_S",
                                PRODUCTION_OFFER_READ_TIMEOUT_S)

    monkeypatch.setattr(pbcampaign, "_submit_record", once_wedged)
    manifest = _manifest(tmp_path, [_row(work, "printf retried")])

    code = pbcampaign.main([
        "--transport", "pool", "--max-inflight", "1", "--wait-s", "1", manifest,
    ])

    err = capsys.readouterr().err
    assert attempts == [0, 0]
    assert "worker-offer discovery timed out" in err
    assert len(list(queue.dir(pool.READY).glob("*.json"))) == 1
    assert code == pbcampaign.pbwait.GAVE_UP_EXIT       # submitted, still waiting


def test_offer_discovery_retries_end_at_the_campaign_deadline(monkeypatch):
    """A scan that never recovers leaves the rows not_submitted: exit 75, not 1."""

    marker = getattr(pbcampaign, "OFFER_DISCOVERY_TIMED_OUT", "offer_discovery_timed_out")
    clock = [100.0]
    attempts, sleeps = [], []

    def timed_out(row, **kwargs):
        attempts.append(row["index"])
        clock[0] += 0.2
        return {"status": "refused", "retryable": marker, "flags": [],
                "error": "pbrun: worker-offer discovery timed out after 0.2s"}

    def poll(*args, **kwargs):
        pytest.fail("nothing was published, so there is nothing to poll")

    def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(pbcampaign, "submit_row", timed_out)
    monkeypatch.setattr(pbcampaign.pbwait, "wait_for_keys", poll)
    monkeypatch.setattr(pbcampaign.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(pbcampaign.time, "sleep", sleep)
    monkeypatch.setattr(pbcampaign.pbrun, "POLL_S", 1.0)

    submissions, waited = pbcampaign.run_windowed(
        [{"index": 0}, {"index": 1}], transport="pool", max_inflight=2, wait_s=5,
    )

    assert len(attempts) >= 2 and set(attempts) == {0}
    assert sleeps and all(0 < s <= 1.0 for s in sleeps)
    assert [s["status"] for s in submissions] == ["not_submitted", "not_submitted"]
    table = pbcampaign.rows_for(submissions, waited)
    assert pbcampaign.pbwait.verdict(
        [dict(r, status="waiting") if r["status"] == "not_submitted" else r
         for r in table]) == pbcampaign.pbwait.GAVE_UP_EXIT

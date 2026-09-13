"""A hold is worth paying only when there is a client to pay it to.

The pacer exists to protect the fleet's NFS clients (#499).  Holding the
reader costs the campaign a warm and buys a client faster reads, so the
decision has two halves: is anybody reading, and is the pool hurting them.

Measured on dl380g10 at 2026-09-13 04:05Z: a ``zpool scrub`` drove sdb to 88 %
utilization at 153.8 MB/s with 9.4 ms read await, the storage role's pacer had
held for 5709 s cumulative and was holding at every sample, and no NFS client
read a byte for the whole window.  The loop warmed nothing for hours and
protected nobody.  Utilization answered "is the disk busy"; nobody had asked
that question.
"""
from __future__ import annotations

from pathlib import Path
import sys
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prewarm_loop  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_the_prewarm_reader_is_paced_by_the_disks import (  # noqa: E402
    LOADED, FakeDisk, accumulate,
)

#: One second of the scrub measured on 2026-09-13: the disk is pinned, and the
#: service time is still inside every cap a client would notice.
SCRUB = dict(reads=1000, read_ms=9400, io_ticks=1000, weighted=1000)


class Clients:
    """What this host's NFS server has served, on the disk's own timeline.

    One step of the disk is one step of the clients, so a test states a rate
    in MB/s and the counter follows it the way ``/proc/net/rpc/nfsd`` does:
    monotonic bytes, differenced by the reader.
    """

    def __init__(self, disk: FakeDisk, mb_per_s: float) -> None:
        self.disk = disk
        self.mb_per_s = mb_per_s

    def text(self) -> str:
        served = int(self.mb_per_s * 1e6 * self.disk.step_s * self.disk.index)
        return f"rc 0 0 0\nio {served} 0\nth 8 0\n"

    def rate(self) -> prewarm_loop.ClientReadRate:
        return prewarm_loop.ClientReadRate(
            path="fixture:/proc/net/rpc/nfsd", clock=self.disk.clock,
            reader=self.text)


def pacer(disk: FakeDisk, mb_per_s: float, **overrides) -> prewarm_loop.DiskPacer:
    clients = Clients(disk, mb_per_s)
    settings = dict(max_util_pct=40.0, max_read_await_ms=15.0,
                    max_backlog_ms=4000.0, sample_s=0.0, hold_s=0.25)
    settings.update(overrides)
    return prewarm_loop.DiskPacer(
        ["sdb"], stat_source=disk.stat, clock=disk.clock, sleep=disk.sleep,
        client_rate=clients.rate(), client_active_mb_s=2.6, **settings)


def test_a_pool_pinned_by_a_scrub_with_no_client_reading_never_holds() -> None:
    """The 2026-09-13 defect: 100 % busy, 9.4 ms await, nobody reading."""

    disk = FakeDisk(accumulate([SCRUB] * 6))
    paced = pacer(disk, mb_per_s=0.0)

    assert paced._verdict() is True          # the baseline sample, as always
    disk.tick()
    paced.wait(threading.Event())

    report = paced.report()
    assert disk.slept == 0.0, "utilization alone must never hold the reader"
    assert report["max_util_pct"] == 100.0, "and it is still measured"
    assert report["holds"] == 0
    assert report["clients_active"] is False
    assert report["client_read_mb_s"] == 0.0


def test_a_pool_that_is_hurting_nobody_never_holds_either() -> None:
    """Over every cap, and still no client behind it: still no hold.

    This is the half of the rule that is not about utilization.  A resilver or
    another tenant's batch can push service time and backlog past the caps
    with no NFS client on the box at all, and stopping the warm for it buys
    nothing back.
    """

    disk = FakeDisk(accumulate([LOADED] * 6))
    paced = pacer(disk, mb_per_s=0.0)

    assert paced._verdict() is True
    disk.tick()
    paced.wait(threading.Event())

    report = paced.report()
    assert disk.slept == 0.0
    assert report["max_read_await_ms"] == 45.0, "the pool really was over"
    assert report["holds"] == 0


def test_a_client_reading_through_a_loaded_pool_holds_the_reader() -> None:
    """Both halves true is the case the pacer was built for."""

    disk = FakeDisk(accumulate([LOADED] * 6))
    paced = pacer(disk, mb_per_s=120.0)
    stop = threading.Event()

    assert paced._verdict() is True
    disk.tick()

    def release(seconds: float) -> None:
        disk.sleep(seconds)
        stop.set()

    paced.sleep = release
    paced.wait(stop)

    report = paced.report()
    assert disk.slept > 0.0
    assert report["holds"] == 1
    assert report["clients_active"] is True
    assert report["client_read_mb_s"] == 120.0
    assert report["held_while_clients_active_s"] > 0.0
    assert report["held_while_clients_idle_s"] == 0.0


def test_a_counter_it_cannot_read_paces_as_though_clients_were_reading() -> None:
    """Blind is not idle.

    A host whose NFS counters are missing must behave like the host whose
    clients are busy.  The other way round, one unreadable file would turn the
    pacer off on the box that needs it most.
    """

    disk = FakeDisk(accumulate([LOADED] * 6))
    blind = prewarm_loop.ClientReadRate(
        path="fixture:/absent", clock=disk.clock, reader=lambda: None)
    paced = prewarm_loop.DiskPacer(
        ["sdb"], max_util_pct=40.0, max_read_await_ms=15.0,
        max_backlog_ms=4000.0, sample_s=0.0, hold_s=0.25,
        stat_source=disk.stat, clock=disk.clock, sleep=disk.sleep,
        client_rate=blind, client_active_mb_s=2.6)
    stop = threading.Event()
    stop.set()

    assert paced._verdict() is True
    disk.tick()
    assert paced._verdict() is True
    paced.wait(stop)

    report = paced.report()
    assert report["clients_active"] is True
    assert report["client_read_mb_s"] is None
    assert report["holds"] == 1


def test_the_report_says_how_much_of_the_hold_had_a_client_behind_it() -> None:
    """Two hold-seconds numbers, because they answer different questions.

    "How long did pacing cost us" and "how long did pacing cost us for
    nothing" are the two an operator needs, and the 5709 s of 2026-09-13 were
    entirely the second.  A hold with no client is still possible -- missing
    disk telemetry holds whoever is reading, because a blind pacer must not
    authorize an unpaced read -- and that time is charged where it belongs.
    """

    disk = FakeDisk(accumulate([LOADED] * 8))
    paced = pacer(disk, mb_per_s=120.0)
    stop = threading.Event()

    assert paced._verdict() is True
    disk.tick()

    def release(seconds: float) -> None:
        disk.sleep(seconds)
        stop.set()

    paced.sleep = release
    paced.wait(stop)                          # a hold with a client behind it
    active = paced.report()["held_while_clients_active_s"]
    assert active > 0.0

    quiet = FakeDisk(accumulate([LOADED] * 8))
    blind = pacer(quiet, mb_per_s=0.0)
    assert blind._verdict() is True           # the baseline sample
    quiet.tick()
    assert blind._verdict() is False          # a loud pool, and nobody reading
    blind.stat_source = lambda device: None   # telemetry gone, clients idle
    stop = threading.Event()

    def release_blind(seconds: float) -> None:
        quiet.sleep(seconds)
        stop.set()

    blind.sleep = release_blind
    blind.wait(stop)

    report = blind.report()
    assert report["telemetry_state"] == "missing"
    assert report["held_while_clients_idle_s"] > 0.0
    assert report["held_while_clients_active_s"] == 0.0
    assert report["held_seconds_total"] == (
        report["held_while_clients_idle_s"]
        + report["held_while_clients_active_s"])


def test_hold_seconds_total_outlives_the_pacer_of_one_cycle() -> None:
    """The role rebuilds its pacer every poll; the question spans polls.

    ``held_seconds`` prices one row and resets with it.  An operator asking
    what pacing has cost since the role started is asking about the ledger,
    which the cycles share.
    """

    ledger = prewarm_loop.HoldLedger()
    ledger.add(4.0, clients_active=True)
    ledger.add(9.0, clients_active=False)
    first = prewarm_loop.DiskPacer([], max_util_pct=0.0, max_read_await_ms=0.0,
                                   max_backlog_ms=0.0, ledger=ledger)
    second = prewarm_loop.DiskPacer([], max_util_pct=0.0, max_read_await_ms=0.0,
                                    max_backlog_ms=0.0, ledger=ledger)

    assert first.report()["held_seconds"] == 0.0
    assert second.report()["held_seconds_total"] == 13.0
    assert second.report()["held_while_clients_idle_s"] == 9.0

"""The pacer never holds the warm against the action the warm is for (#580).

Measured on the GLM-5.3-Flash joint-AURA ``prepare`` (PB action ``8b53c37c``,
dl380g10, 2026-09-17): the prepare was the only NFS client on the box,
reading cold at 160-280 MB/s through the very bytes the storage role was
fetching for it.  The pacer counted that as a client to protect, held
39 714.9 s of 20.6 h -- 3 516 s of the last hour -- and the warm advanced at
22.8 MB/s behind a reader it could never get ahead of.  The prefetcher backed
off from load that existed because it backed off.

The served action's reads are the same one-pass bytes read earlier and at
depth, not extra I/O.  So the pacer reads the server's *per-client* counter,
follows the claim to the box running the action and the box's offer to its
client addresses, and holds only for everyone else.  #499's protection is not
weakened by this: a third party reading beside the served action still holds
the reader, at exactly the numbers it held at before.
"""
from __future__ import annotations

from pathlib import Path
import sys
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prewarm_loop  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet, phase_table  # noqa: E402
from test_the_prewarm_reader_is_paced_by_the_disks import (  # noqa: E402
    LOADED, QUIET, FakeDisk, accumulate,
)

SPARKY = "10.100.98.1"
LINA = "10.100.99.2"


class Exports:
    """``/proc/fs/nfsd/export_stats`` and the ``io`` line, on the disk's timeline.

    One step of the disk is one step of every client: each has a rate in
    MB/s and its counter accumulates it per step, so a rate a test changes
    mid-way applies from the next step on rather than rewriting the past.
    The aggregate ``io`` line is the sum of the rows, as it is on the real
    server, which is what makes a pacer that reads only the aggregate see
    the served action's own bytes as a client.  ``restart_at`` names the
    step at which a client's block is recreated and its counter restarts
    from zero, the way the live box's blocks do every 901 s.
    """

    def __init__(self, disk: FakeDisk, rates: dict[str, float],
                 restart_at: dict[str, int] | None = None) -> None:
        self.disk = disk
        self.rates = dict(rates)
        self.restart_at = dict(restart_at or {})
        self.counters = {address: 0 for address in rates}
        self._seen = 0

    def _advance(self) -> None:
        while self._seen < self.disk.index:
            self._seen += 1
            for address, rate in self.rates.items():
                if self.restart_at.get(address) == self._seen:
                    self.counters[address] = 0
                self.counters[address] += int(rate * 1e6 * self.disk.step_s)

    def io_text(self) -> str:
        self._advance()
        return f"rc 0 0 0\nio {sum(self.counters.values())} 0\nth 8 0\n"

    def text(self) -> str:
        self._advance()
        blocks = "".join(
            f"/storage_pool/shared\t{address}\t1000\n\tfh_stale: 0\n"
            f"\tio_read: {served}\n\tio_write: 0\n\n"
            for address, served in self.counters.items())
        return "# Version 1.1\n# Path Client Start-time\n#\tStats\n" + blocks

    def rate(self) -> prewarm_loop.ClientReadRate:
        return prewarm_loop.ClientReadRate(
            path="fixture:/proc/net/rpc/nfsd", clock=self.disk.clock,
            reader=self.io_text,
            export_stats="fixture:/proc/fs/nfsd/export_stats",
            export_reader=self.text)

    def aggregate_only(self) -> prewarm_loop.ClientReadRate:
        """The counter a host without ``export_stats`` has: one number."""

        return prewarm_loop.ClientReadRate(
            path="fixture:/proc/net/rpc/nfsd", clock=self.disk.clock,
            reader=self.io_text, export_stats="",
            export_reader=lambda: None)


def pacer(disk: FakeDisk, exports: Exports, **overrides) -> prewarm_loop.DiskPacer:
    settings = dict(max_util_pct=40.0, max_read_await_ms=15.0,
                    max_backlog_ms=4000.0, sample_s=0.0, hold_s=0.25,
                    client_active_mb_s=2.6)
    settings.update(overrides)
    return prewarm_loop.DiskPacer(
        ["sdb"], stat_source=disk.stat, clock=disk.clock, sleep=disk.sleep,
        client_rate=exports.rate(), **settings)


def release_on_first_sleep(paced: prewarm_loop.DiskPacer,
                           disk: FakeDisk) -> threading.Event:
    stop = threading.Event()

    def release(seconds: float) -> None:
        disk.sleep(seconds)
        stop.set()

    paced.sleep = release
    return stop


# ------------------------------------------------------------ the pacer


def test_the_served_action_reading_through_a_loaded_pool_never_holds() -> None:
    """The 2026-09-17 defect, at the pacer: 260 MB/s of self, a hurting pool."""

    disk = FakeDisk(accumulate([LOADED] * 6))
    paced = pacer(disk, Exports(disk, {SPARKY: 260.0}))
    paced.begin_row(served_host="sparky", served_addresses=(SPARKY,))

    assert paced._verdict() is True          # the baseline sample, as always
    disk.tick()
    paced.wait(threading.Event())

    report = paced.report()
    assert disk.slept == 0.0, "the served action's own reads must never hold its warm"
    assert report["holds"] == 0
    assert report["max_read_await_ms"] == 45.0, "the pool really was over"
    assert report["clients_active"] is False
    assert report["client_read_mb_s"] == 260.0, "still measured, still reported"
    assert report["self_read_mb_s"] == 260.0
    assert report["other_read_mb_s"] == 0.0
    assert report["served_host"] == "sparky"
    assert report["served_client_addresses"] == [SPARKY]
    assert report["served_attribution"] == "attributed"


def test_a_third_party_reading_beside_the_served_action_still_holds() -> None:
    """#499, kept: the hold answers to the other client, at its own rate."""

    disk = FakeDisk(accumulate([LOADED] * 6))
    paced = pacer(disk, Exports(disk, {SPARKY: 260.0, LINA: 120.0}))
    paced.begin_row(served_host="sparky", served_addresses=(SPARKY,))
    stop = release_on_first_sleep(paced, disk)

    assert paced._verdict() is True
    disk.tick()
    paced.wait(stop)

    report = paced.report()
    assert disk.slept > 0.0
    assert report["holds"] == 1
    assert report["clients_active"] is True
    assert report["client_read_mb_s"] == 380.0
    assert report["self_read_mb_s"] == 260.0
    assert report["other_read_mb_s"] == 120.0, "the number the hold read"
    assert report["held_while_clients_active_s"] > 0.0


def test_a_row_nobody_has_claimed_has_no_self() -> None:
    """A ready row is read for a future claimant; everyone reading now is a client."""

    disk = FakeDisk(accumulate([LOADED] * 6))
    paced = pacer(disk, Exports(disk, {SPARKY: 260.0}))
    paced.begin_row()
    stop = release_on_first_sleep(paced, disk)

    assert paced._verdict() is True
    disk.tick()
    paced.wait(stop)

    report = paced.report()
    assert report["holds"] == 1
    assert report["self_read_mb_s"] == 0.0
    assert report["other_read_mb_s"] == 260.0
    assert report["served_attribution"] == "no served action"


def test_a_client_whose_counter_restarted_keeps_its_last_rate_for_one_interval() -> None:
    """The live box recreates every client's block every 901 s, from zero.

    Differenced naively that interval reads as a client that stopped, or,
    subtracted from a total that did not restart in step, as a third party
    reading at the served action's whole rate.  Either turns a hold on for
    nobody once every fifteen minutes.  The last rate carries across the one
    interval; the next one is exact again.
    """

    disk = FakeDisk(accumulate([LOADED] * 8))
    paced = pacer(disk, Exports(disk, {SPARKY: 260.0}, restart_at={SPARKY: 3}))
    paced.begin_row(served_host="sparky", served_addresses=(SPARKY,))

    assert paced._verdict() is True
    for _ in range(4):                       # samples at steps 1, 2, 3 (restart), 4
        disk.tick()
        paced.wait(threading.Event())
        report = paced.report()
        assert report["self_read_mb_s"] == 260.0, (disk.index, report)
        assert report["other_read_mb_s"] == 0.0
    assert paced.report()["holds"] == 0
    assert disk.slept == 0.0


def test_a_host_without_the_per_client_counter_paces_exactly_as_before() -> None:
    """No ``export_stats``: nothing can be attributed, and nothing is.

    The aggregate line is every client's bytes, so the served action counts
    as a client again -- the pre-#580 verdict, not a blind one: a scrub with
    nobody reading still never holds (2026-09-13).
    """

    disk = FakeDisk(accumulate([LOADED] * 6))
    exports = Exports(disk, {SPARKY: 120.0})
    paced = prewarm_loop.DiskPacer(
        ["sdb"], max_util_pct=40.0, max_read_await_ms=15.0,
        max_backlog_ms=4000.0, sample_s=0.0, hold_s=0.25,
        stat_source=disk.stat, clock=disk.clock, sleep=disk.sleep,
        client_rate=exports.aggregate_only(), client_active_mb_s=2.6)
    paced.begin_row(served_host="sparky", served_addresses=(SPARKY,))
    stop = release_on_first_sleep(paced, disk)

    assert paced._verdict() is True
    disk.tick()
    paced.wait(stop)

    report = paced.report()
    assert report["holds"] == 1
    assert report["client_read_mb_s"] == 120.0
    assert report["self_read_mb_s"] is None
    assert report["other_read_mb_s"] == 120.0
    assert report["client_rate_source"] == "fixture:/proc/net/rpc/nfsd"

    quiet = FakeDisk(accumulate([LOADED] * 6))
    idle = Exports(quiet, {SPARKY: 0.0})
    scrub = prewarm_loop.DiskPacer(
        ["sdb"], max_util_pct=40.0, max_read_await_ms=15.0,
        max_backlog_ms=4000.0, sample_s=0.0, hold_s=0.25,
        stat_source=quiet.stat, clock=quiet.clock, sleep=quiet.sleep,
        client_rate=idle.aggregate_only(), client_active_mb_s=2.6)
    assert scrub._verdict() is True
    quiet.tick()
    scrub.wait(threading.Event())
    assert quiet.slept == 0.0
    assert scrub.report()["holds"] == 0


# ------------------------------------------------------------- the depth


def test_the_depth_follows_who_is_reading_with_the_holds_own_hysteresis() -> None:
    """Sixteen while alone, one while shared, and no flap on the way back.

    A third party reading in bursts around the threshold must not flip the
    depth once per sample: it counts as reading above the cap and stops
    counting below half of it, exactly as a hold is released at half the
    number that started it.
    """

    disk = FakeDisk(accumulate([QUIET] * 40))
    exports = Exports(disk, {SPARKY: 260.0, LINA: 0.0})
    paced = pacer(disk, exports, readers=1, max_readers=16)
    paced.begin_row(served_host="sparky", served_addresses=(SPARKY,))

    assert paced._verdict() is True
    disk.tick()
    assert paced.depth() == 16, "alone: the pool is the served action's and ours"
    assert paced.report()["depth"] == 16

    exports.rates[LINA] = 120.0
    disk.tick()
    assert paced.depth() == 1, "shared: the depth #499 measured to pass"
    exports.rates[LINA] = 2.0                # under the cap, over half of it
    disk.tick()
    assert paced.depth() == 1, "a client under the cap still counts until it is under half"
    exports.rates[LINA] = 1.0
    disk.tick()
    assert paced.depth() == 16
    report = paced.report()
    assert report["readers"] == 1 and report["max_readers"] == 16
    assert 0.0 < report["shared_sample_fraction"] < 1.0


def test_one_tier_when_max_readers_is_not_set() -> None:
    disk = FakeDisk(accumulate([QUIET] * 6))
    paced = pacer(disk, Exports(disk, {SPARKY: 260.0}), readers=2)
    paced.begin_row(served_host="sparky", served_addresses=(SPARKY,))
    assert paced._verdict() is True
    disk.tick()
    assert paced.depth() == 2
    assert prewarm_loop.inactive_pacing()["depth"] == 1


def test_admission_bounds_the_outstanding_reads_and_integrates_the_depth() -> None:
    now = [100.0]
    admission = prewarm_loop.Admission(clock=lambda: now[0])
    limit = [3]
    stop = threading.Event()

    assert admission.acquire(lambda: limit[0], stop, 0.0)
    now[0] += 1.0
    assert admission.acquire(lambda: limit[0], stop, 0.0)
    now[0] += 1.0
    assert admission.acquire(lambda: limit[0], stop, 0.0)
    stop.set()
    assert admission.acquire(lambda: limit[0], stop, 0.0) is False, "full"
    assert admission.active == 3 and admission.peak == 3
    now[0] += 1.0
    for _ in range(3):
        admission.release()
    admission.close()
    # 1 reader for the first second, 2 for the next, 3 for the last.
    assert admission.reader_seconds == 6.0

    limit[0] = 1
    stop = threading.Event()
    assert admission.acquire(lambda: limit[0], stop, 0.0)
    stop.set()
    assert admission.acquire(lambda: limit[0], stop, 0.0) is False


def test_the_reader_goes_as_deep_as_the_pacer_admits(tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    entries = [{"path": path, "offset": 0, "bytes": size, "sha256": None}
               for path, size in (fleet.file(f"{i}.pt", 1 << 20) for i in range(8))]
    mounts = prewarm_loop.MountMap([f"{fleet.mount}={fleet.mount}"])

    disk = FakeDisk(accumulate([QUIET] * 200), advance_on_read=True)
    exports = Exports(disk, {SPARKY: 260.0, LINA: 120.0})
    shared = pacer(disk, exports, readers=1, max_readers=4)
    shared._verdict()
    result = prewarm_loop.Reader(1, mounts, pacer=shared, max_readers=4).read(
        entries, budget_bytes=8 << 20, stop=threading.Event(),
        served={"served_host": "sparky", "served_addresses": (SPARKY,)})
    assert result["bytes_warmed"] == 8 << 20
    assert result["readers"] == 4
    assert result["readers_peak"] == 1, "a third party is reading: one at a time"
    assert result["disk_pacing"]["depth"] == 1

    disk = FakeDisk(accumulate([QUIET] * 200), advance_on_read=True)
    exports = Exports(disk, {SPARKY: 260.0})
    alone = pacer(disk, exports, readers=1, max_readers=4)
    alone._verdict()
    result = prewarm_loop.Reader(1, mounts, pacer=alone, max_readers=4).read(
        entries, budget_bytes=8 << 20, stop=threading.Event(),
        served={"served_host": "sparky", "served_addresses": (SPARKY,)})
    assert result["bytes_warmed"] == 8 << 20
    assert 1 <= result["readers_peak"] <= 4
    assert result["disk_pacing"]["holds"] == 0
    assert result["per_reader_mb_s"] > 0.0
    assert result["keep_pace_depth"] >= 1, "derived from the row's own rates"
    assert result["disk_pacing"]["mean_self_read_mb_s"] == 260.0


# --------------------------------------------------------------- the loop

#: Three phases, one pool-sized budget: the first cycle warms the first phase
#: as a ready row; after the claim and a progress report the loop advances the
#: window through the second, which is the read the pacer decides on.
PHASES = [("layer-0", 2 << 20), ("layer-1", 2 << 20), ("layer-2", 2 << 20)]
CACHE = 2 << 20


def running_fleet(tmp_path: Path) -> tuple[Fleet, str, str]:
    fleet = Fleet(tmp_path)
    key = fleet.action(
        "running",
        [fleet.file(f"{name}-{part}.pt", size // 2)
         for name, size in PHASES for part in ("a", "b")],
        annotations={"phases": phase_table(PHASES)},
        progress_phases=[name for name, _ in PHASES],
    )
    stats = fleet.arcstats(size=0, c=CACHE, c_max=CACHE)
    first = fleet.cycle(fleet.args(arcstats=stats, lookahead=1))
    assert [w["action_key"] for w in first["warmed"]] == [key]
    assert fleet.queue.prewarm(key)["warmed_bytes"] == 2 << 20
    return fleet, key, stats


def advance(fleet: Fleet, key: str, stats: str, exports: Exports,
            disk: FakeDisk) -> dict:
    """One cycle advancing the running row's window under a loaded pool.

    The client rate is built the way a pacer before #580 could build it --
    the aggregate counter first, the per-client file set beside it -- so the
    same test run against that pacer fails on its verdict rather than on its
    signature: it holds, because the aggregate line carries the served
    action's own bytes.
    """

    rate = prewarm_loop.ClientReadRate(
        path="fixture:/proc/net/rpc/nfsd", clock=disk.clock, reader=exports.io_text)
    rate.export_stats = "fixture:/proc/fs/nfsd/export_stats"
    rate.export_reader = exports.text
    paced = disk.pacer(client_rate=rate, client_active_mb_s=2.6)
    paced._verdict()                          # the baseline, before the row
    event = fleet.cycle(fleet.args(arcstats=stats, lookahead=1, readers=1,
                                   max_readers=4), pacer=paced)
    advanced = [row for row in event["advanced"] if row["action_key"] == key]
    assert advanced and advanced[0]["trigger"] == "progress", event
    return fleet.queue.prewarm(key)


def test_a_served_action_reading_hard_does_not_hold_its_own_warm(tmp_path: Path) -> None:
    """The acceptance case: claimed on sparky, sparky reading at 260 MB/s, pool over."""

    fleet, key, stats = running_fleet(tmp_path)
    fleet.claim(key, host="sparky")
    fleet.offer("sparky", addresses=["192.168.1.180", SPARKY])
    fleet.report_progress(key, "layer-1", units=1)
    disk = FakeDisk(accumulate([LOADED] * 6 + [QUIET] * 200), advance_on_read=True)

    record = advance(fleet, key, stats, Exports(disk, {SPARKY: 260.0}), disk)

    pacing = record["disk_pacing"]
    assert record["warmed_bytes"] == 4 << 20, record
    assert pacing["holds"] == 0, pacing
    assert pacing["held_seconds"] == 0.0
    assert pacing["max_read_await_ms"] == 45.0, "the pool was over, and it did not matter"
    assert pacing["served_host"] == "sparky"
    assert pacing["served_client_addresses"] == ["192.168.1.180", SPARKY]
    assert pacing["served_attribution"] == "attributed"
    assert pacing["mean_self_read_mb_s"] == 260.0
    assert pacing["other_read_mb_s"] == 0.0
    assert pacing["offer_age_s"] >= 0.0
    assert record["readers"] == 4
    assert record["readers_peak"] >= 1


def test_an_unrelated_box_reading_beside_the_served_action_holds_the_warm(
        tmp_path: Path) -> None:
    """The same row, lina reading too: #499's hold, charged to lina's rate."""

    fleet, key, stats = running_fleet(tmp_path)
    fleet.claim(key, host="sparky")
    fleet.offer("sparky", addresses=[SPARKY])
    fleet.report_progress(key, "layer-1", units=1)
    disk = FakeDisk(accumulate([LOADED] * 6 + [QUIET] * 200), advance_on_read=True)

    record = advance(fleet, key, stats, Exports(disk, {SPARKY: 260.0, LINA: 120.0}), disk)

    pacing = record["disk_pacing"]
    assert record["warmed_bytes"] == 4 << 20
    assert pacing["holds"] >= 1
    assert pacing["held_seconds"] > 0.0
    assert pacing["held_while_clients_active_s"] > 0.0
    assert pacing["served_attribution"] == "attributed"
    assert pacing["mean_self_read_mb_s"] == 260.0
    assert pacing["max_read_await_ms"] == 45.0


def test_an_offer_naming_another_boxs_addresses_leaves_the_hold_in_place(
        tmp_path: Path) -> None:
    """The identity chain is followed, not assumed: a wrong link protects the reader."""

    fleet, key, stats = running_fleet(tmp_path)
    fleet.claim(key, host="sparky")
    fleet.offer("sparky", addresses=["10.0.0.9"])
    fleet.report_progress(key, "layer-1", units=1)
    disk = FakeDisk(accumulate([LOADED] * 6 + [QUIET] * 200), advance_on_read=True)

    record = advance(fleet, key, stats, Exports(disk, {SPARKY: 260.0}), disk)

    pacing = record["disk_pacing"]
    assert pacing["holds"] >= 1
    assert pacing["served_attribution"] == "attributed"
    assert pacing["served_client_addresses"] == ["10.0.0.9"]
    assert pacing["mean_self_read_mb_s"] == 0.0


def test_a_missing_link_is_named_and_protects_every_client(tmp_path: Path) -> None:
    """A runtime that announced no addresses gets the old behaviour, out loud."""

    fleet, key, stats = running_fleet(tmp_path)
    fleet.claim(key, host="sparky")
    fleet.offer("sparky", addresses=None)
    fleet.report_progress(key, "layer-1", units=1)
    disk = FakeDisk(accumulate([LOADED] * 6 + [QUIET] * 200), advance_on_read=True)

    record = advance(fleet, key, stats, Exports(disk, {SPARKY: 260.0}), disk)

    pacing = record["disk_pacing"]
    assert pacing["holds"] >= 1
    assert pacing["served_host"] == "sparky"
    assert pacing["served_client_addresses"] == []
    assert pacing["served_attribution"] == "offer for sparky announces no addresses"

    fleet, key, stats = running_fleet(tmp_path / "unhosted")
    fleet.claim(key)                          # a claim that names no box
    fleet.report_progress(key, "layer-1", units=1)
    disk = FakeDisk(accumulate([LOADED] * 6 + [QUIET] * 200), advance_on_read=True)
    record = advance(fleet, key, stats, Exports(disk, {SPARKY: 260.0}), disk)
    assert record["disk_pacing"]["holds"] >= 1
    assert record["disk_pacing"]["served_attribution"] == "claim names no host"


def test_a_ready_row_is_warmed_for_nobody_in_particular(tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    key = fleet.action("cold", [fleet.file("cold.pt", 4 << 20)])
    disk = FakeDisk(accumulate([LOADED] * 6 + [QUIET] * 200), advance_on_read=True)
    exports = Exports(disk, {SPARKY: 260.0})
    paced = pacer(disk, exports, readers=1, max_readers=4)
    paced._verdict()

    fleet.cycle(fleet.args(readers=1, max_readers=4), pacer=paced)

    pacing = fleet.queue.prewarm(key)["disk_pacing"]
    assert pacing["holds"] >= 1
    assert pacing["served_attribution"] == "row not claimed"
    assert pacing["other_read_mb_s"] == 260.0

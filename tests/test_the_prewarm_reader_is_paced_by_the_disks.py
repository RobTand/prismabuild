"""The reader holds while the pool's disks are loaded, and says what it saw.

A thread count fixes concurrency, not load.  The eight-reader warm of
2026-09-11 (#499) drove a four-spindle raidz1 to 73-83% utilization and
11-14 s of backlog, which reset every NFS-over-RDMA client on the box; the
same bytes at 8-12% cost nobody anything.  So what the loop bounds is the
disks' state, sampled from ``/sys/block/<dev>/stat``, and these tests pin the
properties that make or break it: it holds when the pool is over, it resumes
when the pool recovers, it records the numbers it decided on, and it never
treats the first sample -- which has no interval, and so no rate -- as a reason
to hold or as a division to attempt.
"""
from __future__ import annotations

from pathlib import Path
import sys
import threading
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prewarm_loop  # noqa: E402


def stat_row(*, reads: int, read_ms: int, io_ticks: int, weighted: int,
             in_flight: int = 0) -> list[int]:
    """One ``/sys/block/<dev>/stat`` row, by field name rather than position."""

    row = [0] * 17
    row[prewarm_loop.STAT_READS_COMPLETED] = reads
    row[prewarm_loop.STAT_READ_MS] = read_ms
    row[prewarm_loop.STAT_IN_FLIGHT] = in_flight
    row[prewarm_loop.STAT_IO_TICKS] = io_ticks
    row[prewarm_loop.STAT_WEIGHTED_IO_MS] = weighted
    return row


#: One second of a quiet pool: 100 reads at 1 ms, 10% busy, 400 ms of backlog.
#: The state #499 measured while the campaign alone read the pool.
QUIET = dict(reads=100, read_ms=100, io_ticks=100, weighted=400)
#: One second of the state that reset the clients: 300 reads at 45 ms, 80%
#: busy, 13 500 ms of backlog.
LOADED = dict(reads=300, read_ms=13500, io_ticks=800, weighted=13500)


def accumulate(steps: list[dict[str, int]]) -> list[list[int]]:
    """Counters are monotonic, so a test states deltas and this sums them."""

    totals = dict(reads=0, read_ms=0, io_ticks=0, weighted=0)
    rows = [stat_row(**totals)]
    for step in steps:
        for key, value in step.items():
            totals[key] += value
        rows.append(stat_row(**totals))
    return rows


class FakeDisk:
    """A stat source and a clock that advance only when the test says so.

    Every hold in these tests is therefore a decision the pacer made, not a
    race the test happened to win, and nothing sleeps for real.
    """

    def __init__(self, rows: list[list[int]], step_s: float = 1.0,
                 advance_on_read: bool = False) -> None:
        self.rows = rows
        self.step_s = step_s
        #: A test driving the pacer directly ticks the disk itself, one step
        #: per assertion.  A test driving it *through* the reader cannot, so
        #: there the disk advances one step per sample instead -- the real
        #: shape, where every sample sees a later disk than the last.
        self.advance_on_read = advance_on_read
        self.index = 0
        self.now = 100.0
        self.slept = 0.0

    def stat(self, device: str) -> list[int]:
        row = self.row_at(self.index)
        if self.advance_on_read:
            self.tick()
        return row

    def row_at(self, index: int) -> list[int]:
        """The counters at ``index``, past the end of the script as well.

        Off the end the last state *continues* rather than freezing: a frozen
        row is a disk doing no work at all, so a test that asked for a pool
        that stays busy would get one that goes idle the moment the reader
        polls faster than the script is long.
        """

        if index < len(self.rows):
            return self.rows[index]
        last = self.rows[-1]
        previous = self.rows[-2] if len(self.rows) > 1 else [0] * len(last)
        steps = index - (len(self.rows) - 1)
        return [value + (value - before) * steps
                for value, before in zip(last, previous)]

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        # Sleeping is what advances both the clock and the disk: the next
        # verdict must be able to differ, or a hold could never end.
        self.slept += seconds
        self.tick()

    def tick(self) -> None:
        self.now += self.step_s
        self.index += 1

    def pacer(self, **overrides) -> prewarm_loop.DiskPacer:
        settings = dict(max_util_pct=40.0, max_read_await_ms=15.0,
                        max_backlog_ms=4000.0, sample_s=0.0, hold_s=0.25)
        settings.update(overrides)
        return prewarm_loop.DiskPacer(
            ["sdb"], stat_source=self.stat, clock=self.clock,
            sleep=self.sleep, **settings)


def test_the_first_sample_waits_for_a_complete_interval_and_obeys_stop() -> None:
    """Configured disks do not authorize a read until their baseline is fresh."""

    disk = FakeDisk(accumulate([LOADED]))
    pacer = disk.pacer()
    stop = threading.Event()
    stop.set()

    pacer.wait(stop)

    assert disk.slept == 0.0
    assert pacer.report()["holds"] == 1
    assert pacer.report()["samples"] == 0
    assert pacer.report()["telemetry_state"] == "awaiting_interval"


def test_an_interval_with_no_completed_read_reports_no_await() -> None:
    """Zero completed reads is an absent average, not an infinite one.

    The disk can be busy with writes for a whole interval.  Dividing the read
    service time by zero reads is the obvious crash; reporting the last known
    await instead would be the quiet one, and would hold on a number no disk
    produced.
    """

    # Busy with writes for the whole interval: the disk ticks, no read
    # completes.
    disk = FakeDisk(accumulate([dict(reads=0, read_ms=0, io_ticks=900,
                                     weighted=2000)]))
    pacer = disk.pacer()

    pacer._verdict()                   # first sample establishes the baseline
    disk.tick()
    over = pacer._verdict()

    report = pacer.report()
    assert report["max_read_await_ms"] == 0.0
    assert report["max_util_pct"] == 90.0
    assert over is True, "utilization is over on its own"


def test_the_reader_holds_while_the_pool_is_loaded_and_resumes_when_it_is_not(
) -> None:
    """The whole mechanism in one run: quiet, loaded, quiet again."""

    disk = FakeDisk(accumulate([QUIET, LOADED, LOADED, QUIET, QUIET]))
    pacer = disk.pacer()

    assert pacer._verdict() is True    # sample 1 is only a baseline
    disk.tick()
    pacer.wait(threading.Event())      # sample 2: quiet, no hold
    assert disk.slept == 0.0, "a quiet pool must not be paced"

    disk.tick()
    pacer.wait(threading.Event())      # sample 3: loaded -- holds until quiet

    report = pacer.report()
    assert disk.slept > 0.0, "a loaded pool must stop the reader"
    assert report["holds"] == 1
    assert report["held_seconds"] > 0.0
    assert report["max_util_pct"] == 80.0
    assert report["max_read_await_ms"] == 45.0
    assert report["max_backlog_ms"] == 13500.0
    assert 0.0 < report["mean_util_pct"] < report["max_util_pct"], (
        "the mean must average every sample, not repeat the worst")


def test_a_hold_ends_when_the_reader_is_stopped() -> None:
    """A stop must be obeyed even by a pool that never recovers.

    Otherwise a warm the supervisor is trying to end outlives it, holding a
    budget the next cycle has already re-spent.
    """

    disk = FakeDisk(accumulate([LOADED] * 50))
    pacer = disk.pacer()
    assert pacer._verdict() is True    # first sample establishes the baseline
    disk.tick()
    stop = threading.Event()
    stop.set()

    pacer.wait(stop)

    assert pacer.report()["holds"] == 1, "the pool was over, so it did hold"
    assert disk.slept == 0.0, (
        "a set stop must end the hold before it waits out an interval")


def test_the_seconds_held_are_wall_clock_not_a_sum_over_readers() -> None:
    """Two readers held through the same second cost the pool one second.

    A per-thread sum would report eight seconds for an eight-reader hold of
    one, and the number exists to be compared against the warm's own duration:
    "held 40 s of a 300 s warm" is a sentence, and "held 320 s" is not.
    """

    disk = FakeDisk(accumulate([LOADED, LOADED]))
    pacer = disk.pacer()
    assert pacer._verdict() is True    # first sample establishes the baseline
    disk.tick()
    stop = threading.Event()

    threads = [threading.Thread(target=pacer.wait, args=(stop,))
               for _ in range(2)]
    for thread in threads:
        thread.start()
    # The pool never recovers, so both readers stay held until they are told
    # to stop -- which is what lets the test observe two concurrent holds
    # rather than racing the first one's recovery.
    deadline = time.time() + 10.0
    while pacer.report()["holds"] < 2 and time.time() < deadline:
        time.sleep(0.005)
    stop.set()
    for thread in threads:
        thread.join(timeout=10.0)

    report = pacer.report()
    assert report["holds"] == 2, "both readers were held"
    assert not any(thread.is_alive() for thread in threads)
    assert 0.0 < report["held_seconds"] <= disk.now - 100.0, (
        "held seconds cannot exceed the wall clock the run occupied")


def test_a_pacer_with_no_disks_is_inactive_rather_than_a_refusal() -> None:
    """A worker box has no pool, and reading there is free.

    Pacing that refused where it cannot measure would turn every test host and
    every non-storage box into a failure, so "inactive" is a recorded value.
    """

    pacer = prewarm_loop.DiskPacer(
        [], max_util_pct=40.0, max_read_await_ms=15.0, max_backlog_ms=4000.0)

    pacer.wait(threading.Event())
    report = pacer.report()

    assert pacer.active is False
    assert report["active"] is False
    assert report["devices"] == []
    assert report["held_seconds"] == 0.0
    assert report["reason"] == "no pool devices"


def test_the_thresholds_the_pacer_used_are_in_its_report() -> None:
    """A warm held by a tight cap and one held by a sick disk look alike
    without the caps beside the numbers."""

    disk = FakeDisk(accumulate([QUIET, QUIET]))

    report = disk.pacer(max_util_pct=11.0).report()

    assert report["thresholds"] == {
        "max_util_pct": 11.0, "max_read_await_ms": 15.0,
        "max_backlog_ms": 4000.0}


def test_a_zero_cap_is_off_rather_than_a_cap_of_zero() -> None:
    """0 means "do not pace on this", which is how a cap is retired."""

    disk = FakeDisk(accumulate([QUIET, LOADED, LOADED]))
    pacer = disk.pacer(max_util_pct=0.0, max_read_await_ms=0.0,
                       max_backlog_ms=0.0)

    assert pacer._verdict() is True    # establish the required baseline
    disk.tick()
    pacer.wait(threading.Event())

    assert disk.slept == 0.0
    assert pacer.report()["holds"] == 0


def fake_sysfs(root: Path, partitions: dict[str, str]) -> str:
    """A ``/sys/class/block`` where each partition hangs under its disk.

    The mapping under test is the real one -- a partition's sysfs node is a
    child of its disk's node -- so the fixture builds that shape rather than a
    lookup table, and a rule that stripped trailing digits would still fail
    here for the right reason.
    """

    devices = root / "devices" / "block"
    class_block = root / "class" / "block"
    class_block.mkdir(parents=True)
    for partition, disk in partitions.items():
        disk_dir = devices / disk
        disk_dir.mkdir(parents=True, exist_ok=True)
        (disk_dir / "dev").write_text("8:16\n")
        if partition == disk:
            node = disk_dir
        else:
            node = disk_dir / partition
            node.mkdir(exist_ok=True)
            (node / "dev").write_text("8:17\n")
        (class_block / partition).symlink_to(node)
    return str(class_block)


def test_only_the_pools_own_data_disks_pace_the_reader(tmp_path: Path) -> None:
    """Discovery reads the pool's topology, not a device glob.

    Two things must not pace this loop: a disk the pool does not own, and the
    L2ARC, which is an SSD that was never the constraint.  ``zpool status -P``
    is the only place both facts are written down.
    """

    status = (
        "  pool: storage_pool\n"
        " state: ONLINE\n"
        "config:\n"
        "\n"
        "\tNAME                     STATE     READ WRITE CKSUM\n"
        "\tstorage_pool             ONLINE       0     0     0\n"
        "\t  raidz1-0               ONLINE       0     0     0\n"
        "\t    /dev/sdb1            ONLINE       0     0     0\n"
        "\t    /dev/sdc1            ONLINE       0     0     0\n"
        "\tcache\n"
        "\t  /dev/nvme1n1p5         ONLINE       0     0     0\n"
        "\tlogs\n"
        "\t  /dev/nvme1n1p6         ONLINE       0     0     0\n"
        "\n"
        "errors: No known data errors\n"
    ).expandtabs(2)

    sysfs = fake_sysfs(tmp_path, {"sdb1": "sdb", "sdc1": "sdc",
                                  "nvme1n1p5": "nvme1n1",
                                  "nvme1n1p6": "nvme1n1"})

    devices = prewarm_loop.pool_member_devices(
        "storage_pool", runner=lambda argv: status, sysfs=sysfs)

    assert devices == ["sdb", "sdc"], (
        "the cache and log devices are not the queue the loop must stay off")


def test_an_unresolvable_data_member_refuses_the_whole_pool(tmp_path: Path) -> None:
    """A quiet resolved vdev cannot stand in for its missing sibling."""

    status = (
        "  pool: storage_pool\n"
        "config:\n\n"
        "\tNAME STATE READ WRITE CKSUM\n"
        "\tstorage_pool ONLINE 0 0 0\n"
        "\t  raidz1-0 ONLINE 0 0 0\n"
        "\t    /dev/sdb1 ONLINE 0 0 0\n"
        "\t    /dev/sdc1 ONLINE 0 0 0\n"
        "\nerrors: No known data errors\n"
    ).expandtabs(2)
    sysfs = fake_sysfs(tmp_path, {"sdb1": "sdb"})

    assert prewarm_loop.pool_member_devices(
        "storage_pool", runner=lambda argv: status, sysfs=sysfs) == []


def test_a_whole_disk_vdev_is_its_own_pacing_device(tmp_path: Path) -> None:
    """A pool given a bare disk has no partition to climb out of."""

    sysfs = fake_sysfs(tmp_path, {"sdb": "sdb"})

    assert prewarm_loop.whole_disk_of("/dev/sdb", sysfs=sysfs) == "sdb"


def test_a_host_with_no_pool_paces_nothing_and_says_why() -> None:
    """``zpool`` missing is the normal case everywhere but the storage box."""

    def missing(argv: list[str]) -> str:
        raise FileNotFoundError("zpool")

    assert prewarm_loop.pool_member_devices("storage_pool",
                                            runner=missing) == []


@pytest.mark.parametrize("readable_disk", [False, True],
                         ids=["all-missing", "one-member-missing"])
def test_missing_required_disk_feedback_prevents_payload_reads(
        tmp_path: Path, readable_disk: bool) -> None:
    """A quiet peer cannot make an unreadable vdev safe to prewarm against."""

    stop = threading.Event()
    clock = [10.0]

    def now() -> float:
        clock[0] += 1.0
        return clock[0]

    def sleep(seconds: float) -> None:
        clock[0] += seconds
        stop.set()

    def stats(device: str) -> list[int] | None:
        return [0] * 11 if readable_disk and device == "sdc" else None

    pacer = prewarm_loop.DiskPacer(
        ["sdb", "sdc"], max_util_pct=40, max_read_await_ms=15,
        max_backlog_ms=4000, sample_s=0, stat_source=stats,
        clock=now, sleep=sleep,
    )
    payload = tmp_path / "input"
    payload.write_bytes(b"x" * 4096)
    result = prewarm_loop.Reader(
        1, prewarm_loop.MountMap([f"{tmp_path}={tmp_path}"]), pacer=pacer,
    ).read(
        [{"path": str(payload), "offset": 0, "bytes": 4096}],
        budget_bytes=4096, stop=stop,
    )

    assert result["bytes_warmed"] == 0, result
    pacing = result["disk_pacing"]
    assert pacing["telemetry_state"] == "missing"
    assert pacing["missing_devices"] == (["sdb", "sdc"]
                                          if not readable_disk else ["sdb"])
    assert pacing["telemetry_gaps"] == 1


def test_disk_telemetry_recovers_only_after_a_fresh_complete_interval() -> None:
    """Restoration cannot reuse the last pre-gap row as a healthy interval."""

    disk = FakeDisk(accumulate([QUIET, QUIET, QUIET]))
    available = [False]

    def stats(device: str) -> list[int] | None:
        return disk.stat(device) if available[0] else None

    def sleep(seconds: float) -> None:
        disk.sleep(seconds)
        available[0] = True

    pacer = prewarm_loop.DiskPacer(
        ["sdb"], max_util_pct=40, max_read_await_ms=15,
        max_backlog_ms=4000, sample_s=0, stat_source=stats,
        clock=disk.clock, sleep=sleep,
    )

    pacer.wait(threading.Event())

    report = pacer.report()
    assert disk.slept >= 0.5, "one sleep restores a baseline; another proves it"
    assert report["telemetry_state"] == "complete"
    assert report["telemetry_gaps"] == 1
    assert report["samples"] == 1


def test_losing_a_member_after_a_healthy_interval_stops_the_next_payload_read(
        tmp_path: Path) -> None:
    """A prior quiet verdict expires when one configured member disappears."""

    disk = FakeDisk(accumulate([QUIET, QUIET, QUIET]))
    pacer = disk.pacer()
    assert pacer._verdict() is True
    disk.tick()
    assert pacer._verdict() is False

    stop = threading.Event()

    def gone(device: str) -> None:
        return None

    def stop_after_hold(seconds: float) -> None:
        disk.sleep(seconds)
        stop.set()

    pacer.stat_source = gone
    pacer.sleep = stop_after_hold
    payload = tmp_path / "input"
    payload.write_bytes(b"x" * 4096)
    result = prewarm_loop.Reader(
        1, prewarm_loop.MountMap([f"{tmp_path}={tmp_path}"]), pacer=pacer,
    ).read(
        [{"path": str(payload), "offset": 0, "bytes": 4096}],
        budget_bytes=4096, stop=stop,
    )

    assert result["bytes_warmed"] == 0, result
    assert result["disk_pacing"]["telemetry_state"] == "missing"
    assert result["disk_pacing"]["telemetry_gaps"] == 1


def test_a_duplicate_configured_member_needs_one_complete_stat_row() -> None:
    """A repeated ``--disks`` name is one vdev, not missing telemetry."""

    disk = FakeDisk(accumulate([QUIET, QUIET]))
    pacer = prewarm_loop.DiskPacer(
        ["sdb", "sdb"], max_util_pct=40, max_read_await_ms=15,
        max_backlog_ms=4000, sample_s=0, stat_source=disk.stat,
        clock=disk.clock, sleep=disk.sleep,
    )

    assert pacer.devices == ["sdb"]
    assert pacer._verdict() is True
    disk.tick()
    pacer.wait(threading.Event())

    assert pacer.report()["telemetry_state"] == "complete"
    assert pacer.report()["holds"] == 0


def test_a_row_boundary_keeps_an_existing_pacing_hold() -> None:
    """Accounting reset must never turn an already-over pool into a read."""

    disk = FakeDisk(accumulate([LOADED, LOADED]))
    pacer = disk.pacer()
    assert pacer._verdict() is True
    disk.tick()
    assert pacer._verdict() is True
    pacer._enter_hold()

    pacer.begin_row()

    assert pacer._verdict() is True
    assert pacer.report()["holds"] == 1
    pacer._leave_hold()


def test_zpool_is_resolved_off_path_as_well() -> None:
    """A supervisor unit's ``PATH`` need not carry ``/usr/sbin``.

    Losing discovery to that would put the loop back on #499's unpaced reads,
    so the binary is looked up by name and then by the two places it lives.
    """

    resolved = prewarm_loop.zpool_binary()

    assert resolved.endswith("zpool")
    assert resolved == "zpool" or Path(resolved).is_absolute()

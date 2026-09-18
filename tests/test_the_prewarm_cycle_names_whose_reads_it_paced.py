"""The cycle event names whose reads it paced, whether it held or not (#575).

#575 holds the measurement for the NFS-over-RDMA ``remote access error``
bursts: with the prewarm reader paced to one reader the errors persist at a
roughly 15-minute cadence, and nothing says whether they track the prewarm
reader, the consumer's own cold reads, or neither.  Rob's 2026-09-17 natural
experiment then moved the prewarm reader 4.6x on the same fabric and watched
the error rate move 5.0x against only a 1.33x move in RDMA bytes -- the
errors track the storage host's *local* I/O load, not the fabric's, which
fits a server-side MR-invalidation race widened by local disk latency, with
retransmits unchanged because the RDMA layer recovers below the RPC layer.

Settling that needs the PB side of the controlled window to be
self-certifying, and it was not (#585): the pacer stamped its client
attribution -- whose reads counted as *self*, what everyone else read, and
whether the telemetry behind the verdict was even there -- only on hold
events.  A cycle that paced correctly and held nothing filed nothing about
why, so the best case read exactly like a disabled pacer, and the claimed
window advances -- the running action's own read frontier, the highest-rate
local reads on the box -- carried no pacing verdict on the cycle event at
all.

The cycle event therefore stamps ``client_attribution``: one entry per
window warmed this cycle, in warm order, plus this cycle's own hold
counters diffed against the role lifetime ledger.  A cycle that warmed
nothing reports no rows and no holds, which beside ``pacing_active`` stays
visibly different from pacing switched off; a row read blind says
``missing`` where a row read alone says ``complete``; and two rows served
for two different boxes keep their own ``served_host`` -- the cycle never
merges them into one verdict.
"""
from __future__ import annotations

from pathlib import Path
import sys
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prewarm_loop  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet  # noqa: E402
from test_the_prewarm_pacer_does_not_hold_against_the_action_it_serves import (  # noqa: E402
    SPARKY,
    Exports,
    pacer,
    running_fleet,
)
from test_the_prewarm_reader_is_paced_by_the_disks import (  # noqa: E402
    LOADED,
    QUIET,
    FakeDisk,
    accumulate,
)


def test_an_advanced_window_carries_its_attribution_on_the_cycle_event(
        tmp_path: Path) -> None:
    """The claimed window's verdict is on the event, not only in its receipt."""

    fleet, key, stats = running_fleet(tmp_path)
    fleet.claim(key, host="sparky")
    fleet.offer("sparky", addresses=["192.168.1.180", SPARKY])
    fleet.report_progress(key, "layer-1", units=1)
    disk = FakeDisk(accumulate([LOADED] * 6 + [QUIET] * 200),
                    advance_on_read=True)
    paced = pacer(disk, Exports(disk, {SPARKY: 260.0}),
                  readers=1, max_readers=4)
    paced._verdict()                          # the baseline, before the row
    event = fleet.cycle(fleet.args(arcstats=stats, lookahead=1, readers=1,
                                   max_readers=4), pacer=paced)

    advanced = [row for row in event["advanced"] if row["action_key"] == key]
    assert advanced and advanced[0]["trigger"] == "progress", event
    assert advanced[0]["disk_pacing"]["served_attribution"] == "attributed"
    assert advanced[0]["disk_pacing"]["holds"] == 0

    attribution = event["client_attribution"]
    assert attribution["holds"] == 0
    assert attribution["held_seconds"] == 0.0
    assert attribution["telemetry_states"] == ["complete"]
    assert attribution["missing_devices"] == []
    assert len(attribution["rows"]) == 1, attribution
    row = attribution["rows"][0]
    assert row["action_key"] == key
    assert row["trigger"] == "progress"
    assert row["served_host"] == "sparky"
    assert row["served_attribution"] == "attributed"
    assert row["self_read_mb_s"] == 260.0
    assert row["other_read_mb_s"] == 0.0
    assert "served_host" not in attribution, (
        "one verdict per row; the cycle merges nothing")


def test_a_cycle_that_warmed_nothing_reports_no_rows_and_no_holds(
        tmp_path: Path) -> None:
    """An idle cycle is evidence, not an absence: pacing on, nothing to read."""

    fleet = Fleet(tmp_path)
    disk = FakeDisk(accumulate([QUIET] * 4))
    event = fleet.cycle(fleet.args(), pacer=disk.pacer())

    assert event["warmed"] == [] and event["advanced"] == []
    assert event["pacing_active"] is True
    assert event["pacing_devices"] == ["sdb"]
    attribution = event["client_attribution"]
    assert attribution["rows"] == []
    assert attribution["holds"] == 0
    assert attribution["held_seconds"] == 0.0
    assert attribution["telemetry_states"] == []
    assert attribution["missing_devices"] == []

    quiet = Fleet(tmp_path / "off")
    idle = quiet.cycle(quiet.args())
    assert idle["pacing_active"] is False, "no disks: pacing explicitly off"
    assert idle["client_attribution"]["rows"] == []
    assert idle["client_attribution"]["holds"] == 0


def test_two_rows_in_one_cycle_keep_their_own_attribution(
        tmp_path: Path) -> None:
    """A claimed window beside a ready row: two boxes, two verdicts, no merge."""

    fleet, key, stats = running_fleet(tmp_path)
    big = fleet.arcstats(size=0, c=1 << 30, c_max=1 << 30)
    other = fleet.action("ready", [fleet.file("other.pt", 1 << 20)])
    fleet.claim(key, host="sparky")
    fleet.offer("sparky", addresses=[SPARKY])
    fleet.report_progress(key, "layer-1", units=1)

    disk = FakeDisk(accumulate([QUIET] * 300), advance_on_read=True)
    paced = pacer(disk, Exports(disk, {SPARKY: 260.0}),
                  readers=1, max_readers=4)
    paced._verdict()
    event = fleet.cycle(fleet.args(arcstats=big, lookahead=2, readers=1,
                                   max_readers=4), pacer=paced)

    assert [row["action_key"] for row in event["advanced"]] == [key]
    assert [row["action_key"] for row in event["warmed"]] == [other]
    attribution = event["client_attribution"]
    assert attribution["holds"] == 0, attribution
    assert len(attribution["rows"]) == 2, attribution
    claimed, ready = attribution["rows"]
    assert (claimed["action_key"], claimed["served_attribution"]) == (
        key, "attributed")
    assert claimed["self_read_mb_s"] == 260.0
    assert (ready["action_key"], ready["served_attribution"]) == (
        other, "row not claimed")
    assert ready["other_read_mb_s"] == 260.0
    assert "served_host" not in attribution, (
        "the claimed window's box must not leak onto the ready row's verdict")


def test_a_row_read_blind_names_the_missing_devices_on_the_cycle_event(
        tmp_path: Path) -> None:
    """Fail closed, out loud: a blind row says ``missing``, never ``complete``."""

    fleet = Fleet(tmp_path)
    key = fleet.action("cold", [fleet.file("cold.pt", 1 << 20)])
    stop = threading.Event()
    clock = [10.0]

    def now() -> float:
        clock[0] += 1.0
        return clock[0]

    def sleep(seconds: float) -> None:
        clock[0] += seconds
        stop.set()

    paced = prewarm_loop.DiskPacer(
        ["sdb"], max_util_pct=40.0, max_read_await_ms=15.0,
        max_backlog_ms=4000.0, sample_s=0.0,
        stat_source=lambda device: None, clock=now, sleep=sleep)
    args = fleet.args(readers=1)
    mounts = prewarm_loop.MountMap(list(args.mount_map))
    event = prewarm_loop.cycle(args, fleet.queue, mounts, stop, pacer=paced)

    assert [w["action_key"] for w in event["warmed"]] == [key]
    assert fleet.queue.prewarm(key)["disk_pacing"]["telemetry_state"] == "missing"
    attribution = event["client_attribution"]
    assert len(attribution["rows"]) == 1, attribution
    row = attribution["rows"][0]
    assert row["served_attribution"] == "row not claimed"
    assert row["telemetry_state"] == "missing"
    assert row["missing_devices"] == ["sdb"]
    assert attribution["telemetry_states"] == ["missing"]
    assert attribution["missing_devices"] == ["sdb"]
    assert attribution["holds"] >= 1, (
        "the blind read held rather than reading through")

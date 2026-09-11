"""The record says what the warm cost the pool, not only how fast it was.

``mb_per_s`` alone hid #499 for as long as it existed: the fastest warm the
loop ever recorded (465 MB/s, eight readers) was the one that reset every
NFS-over-RDMA client on the box and idled both Sparks' GPUs for two minutes.
A rate is not a price.  So the record carries the disks' own numbers beside
it -- what the worst disk did while the warm ran, what the caps were, and how
long the reader was held off -- and the defaults that produced #499 are no
longer the defaults.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet  # noqa: E402
from test_the_prewarm_reader_is_paced_by_the_disks import (  # noqa: E402
    LOADED, QUIET, FakeDisk, accumulate,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prewarm_loop  # noqa: E402

REPO = Path(__file__).resolve().parents[1]


def test_the_record_carries_the_disk_numbers_beside_the_rate(
        tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    # Four blocks a file: the reader consults the pacer once per block, and
    # the loaded interval is the third sample.
    key = fleet.action("row", [fleet.file("a.pt", 4 << 20),
                               fleet.file("b.pt", 4 << 20)])
    disk = FakeDisk(accumulate([QUIET, LOADED] + [QUIET] * 40),
                    advance_on_read=True)
    pacer = disk.pacer()

    fleet.cycle(fleet.args(readers=1), pacer=pacer)
    record = fleet.queue.prewarm(key)

    pacing = record["disk_pacing"]
    assert record["status"] == "complete"
    assert pacing["active"] is True
    assert pacing["devices"] == ["sdb"]
    assert pacing["samples"] > 0
    assert pacing["max_util_pct"] >= pacing["mean_util_pct"] > 0.0
    assert pacing["max_read_await_ms"] >= pacing["mean_read_await_ms"] > 0.0
    assert pacing["max_backlog_ms"] >= pacing["mean_backlog_ms"] > 0.0
    assert pacing["held_seconds"] > 0.0, "the loaded sample must have held"
    assert pacing["holds"] >= 1
    assert pacing["thresholds"]["max_util_pct"] == 40.0
    # Still a receipt for the bytes: pacing adds a price, it does not replace
    # the claim that the bytes are resident.
    assert record["bytes_warmed"] == record["manifest_bytes"] == 8 << 20


def test_the_record_names_the_row_and_the_instants_it_covers(
        tmp_path: Path) -> None:
    """A receipt nobody can place against a row is a receipt nobody reads.

    The claim that answers "was this still resident when the worker started?"
    needs the campaign's own row name and both instants, and the person asking
    it is reading the record, not the content-addressed manifest behind it.
    """

    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("a.pt", 4096)],
                       annotations={"row_id": "row-0079", "group": "experts"})

    fleet.cycle(fleet.args())
    record = fleet.queue.prewarm(key)

    assert record["row_id"] == "row-0079"
    assert record["started_utc"].endswith("Z")
    assert record["finished_utc"] >= record["started_utc"]
    assert record["started_utc"][:11] == record["finished_utc"][:11]
    # The rest of the annotations stay behind the digest they are addressed
    # by: the record carries a label, not a copy of the submitter's metadata.
    assert "group" not in json.dumps(record)


def test_a_manifest_with_no_row_id_records_an_empty_label(
        tmp_path: Path) -> None:
    """An absent label is empty, not missing: a reader never branches on the
    key's existence, and a submitter is never required to name a row."""

    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("a.pt", 4096)])

    fleet.cycle(fleet.args())

    assert fleet.queue.prewarm(key)["row_id"] == ""


def test_an_unpaced_host_records_that_pacing_was_off(tmp_path: Path) -> None:
    """"Nobody paced this" and "pacing found nothing to hold for" are
    different facts, and the record must not spell them the same way."""

    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("a.pt", 4096)])

    fleet.cycle(fleet.args())
    pacing = fleet.queue.prewarm(key)["disk_pacing"]

    assert pacing["active"] is False
    assert pacing["devices"] == []
    assert pacing["held_seconds"] == 0.0
    assert pacing["samples"] == 0


def test_a_dry_run_reports_the_pacing_it_would_have_used(
        tmp_path: Path) -> None:
    """The dry run is the plan, and the caps are part of the plan.

    The record and the event are built from the same dict, so a dry run that
    omitted the field would give two shapes for one receipt.
    """

    fleet = Fleet(tmp_path)
    fleet.action("row", [fleet.file("a.pt", 4096)])
    disk = FakeDisk(accumulate([QUIET, QUIET]), advance_on_read=True)

    event = fleet.cycle(fleet.args(dry_run=True), pacer=disk.pacer())

    pacing = event["warmed"][0]["disk_pacing"]
    assert pacing["active"] is True
    assert pacing["held_seconds"] == 0.0
    assert pacing["thresholds"]["max_backlog_ms"] == 4000.0


def test_the_cycle_event_says_whether_it_was_pacing(tmp_path: Path) -> None:
    """One line per poll, and the line answers "was this warm bounded?"."""

    fleet = Fleet(tmp_path)
    fleet.action("row", [fleet.file("a.pt", 4096)])
    disk = FakeDisk(accumulate([QUIET, QUIET]), advance_on_read=True)

    event = fleet.cycle(fleet.args(), pacer=disk.pacer())

    assert event["pacing_active"] is True
    assert event["pacing_devices"] == ["sdb"]


def test_the_shipped_defaults_are_the_paced_ones() -> None:
    """The command line the storage role gets with no arguments at all.

    8 readers and 2 lookahead are what #499 measured; they must not be what a
    fresh invocation picks up again.
    """

    parsed = _parse(["--mount-map", "/mnt/shared=/storage_pool/shared",
                     "--once", "--dry-run"])
    assert parsed.readers == 2
    assert parsed.lookahead == 1
    assert parsed.max_util_pct == 40.0
    assert parsed.max_read_await_ms == 15.0
    assert parsed.max_backlog_ms == 4000.0
    assert parsed.pace_pool == "storage_pool"


def test_the_storage_role_declares_the_paced_shape() -> None:
    """``fleet_boxes.json`` is what actually runs on dl380g10.

    A default fixed only in the parser is a default the live box never sees,
    because the role passes its arguments explicitly.
    """

    boxes = json.loads((REPO / "tools/fleet/fleet_boxes.json").read_text())
    role = boxes["boxes"]["dl380g10"]["roles"]["storage"]

    assert role[role.index("--readers") + 1] == "2"
    assert role[role.index("--lookahead") + 1] == "1"
    assert role[role.index("--pace-pool") + 1] == "storage_pool"
    why = boxes["boxes"]["dl380g10"]["_roles_why"]
    assert "#499" in why and "c_max" in why, (
        "the why text must carry the ARC arithmetic the lookahead rests on")


def _parse(argv: list[str]):
    """Run the module's own parser, so the test cannot drift from it."""

    import contextlib
    import io

    captured: dict[str, object] = {}

    class Stop(Exception):
        pass

    def capture(args, *rest, **kwargs):
        captured["args"] = args
        raise Stop

    original = prewarm_loop.cycle
    prewarm_loop.cycle = capture
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            prewarm_loop.main(argv)
    except Stop:
        pass
    except SystemExit as exit_code:          # pragma: no cover - a real refusal
        raise AssertionError(f"parser refused: {exit_code}") from exit_code
    finally:
        prewarm_loop.cycle = original
    return captured["args"]


def test_the_pacer_the_loop_builds_matches_its_arguments() -> None:
    """``--disks`` names the devices outright, for a pool discovery cannot see."""

    parsed = _parse(["--mount-map", "/mnt/shared=/storage_pool/shared",
                     "--disks", "sdb,sdc", "--max-util-pct", "25",
                     "--once", "--dry-run"])
    pacer = prewarm_loop.pacer_from_args(parsed)

    assert pacer.devices == ["sdb", "sdc"]
    assert pacer.max_util_pct == 25.0
    assert pacer.report()["reason"] == "from --disks"

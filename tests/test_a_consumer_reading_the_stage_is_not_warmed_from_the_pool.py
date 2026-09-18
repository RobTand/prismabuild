"""A claimed action that reads the stage is not warmed from the pool (#638).

The claim-time warm reads the *pool* path -- ``/mnt/shared/...`` -- because
that is the path a consumer opened before PrismaBuild published a residency
map.  A consumer admitted on a resident window opens the *stage* path instead,
and the two are different datasets, so those reads are wrong twice over: they
warm blocks nobody will read, and they evict the stage blocks the consumer is
reading to do it.

The signal is the verdict the pool already wrote on the claim record
(``residency_verdict.state == "resident"``), not the presence of a residency
block: a consumer whose map is not composed yet was never admitted, and one
whose later phases are not staged still reads the pool for them.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from prewarm_fixture import Fleet, phase_table  # noqa: E402


def _row(tmp_path: Path, *, verdict: dict | None):
    """One claimed, phase-declaring row, with or without a residency verdict."""

    fleet = Fleet(tmp_path)
    files = [fleet.file("shard-1.bin", 1 << 20), fleet.file("shard-2.bin", 1 << 20)]
    key = fleet.action(
        "consumer", files,
        annotations={"phases": phase_table([("phase-0", 1 << 20),
                                            ("phase-1", 1 << 20)])},
        progress_phases=["phase-0", "phase-1"])
    claimed = fleet.claim(key, host="sparky")
    if verdict is not None:
        record = json.loads(claimed.read_text())
        record["residency_verdict"] = verdict
        claimed.write_text(json.dumps(record))
    return fleet, key


def test_a_claimed_row_with_no_residency_is_warmed_as_before(tmp_path: Path) -> None:
    """The control arm: without a verdict this is the warm that exists today."""

    fleet, key = _row(tmp_path, verdict=None)

    event = fleet.cycle(fleet.args())

    assert [row["action_key"] for row in event["advanced"]] == [key]
    assert int(fleet.queue.prewarm(key)["bytes_warmed"]) > 0


def test_a_resident_consumer_is_not_warmed_from_the_pool(tmp_path: Path) -> None:
    fleet, key = _row(tmp_path, verdict={"state": "resident", "leads": ["a" * 64]})

    event = fleet.cycle(fleet.args())

    assert event["advanced"] == []
    assert fleet.queue.prewarm(key) is None, "the pool path was warmed anyway"
    skipped = [row for row in event["skipped"] if row.get("action_key") == key]
    assert skipped, "a warm that was skipped must say so"
    assert skipped[0]["reason"] == "the consumer reads the stage"


def test_a_consumer_still_waiting_on_its_map_is_warmed(tmp_path: Path) -> None:
    """Only ``resident`` means the consumer is reading the stage.

    Every other verdict is a consumer that is either not admitted or reading
    the pool, and a loop that read the block rather than the state would stop
    warming rows nothing else warms."""

    fleet, key = _row(tmp_path, verdict={"state": "map_not_composed",
                                         "leads": ["a" * 64]})

    event = fleet.cycle(fleet.args())

    assert [row["action_key"] for row in event["advanced"]] == [key]

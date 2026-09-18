"""Staging writes bytes.  It does not make anybody read them.

This is the honest half of #582 and the reason the receipt has a ``consumer``
block at all.  No consumer reads the stage export: PrismaBuild publishes no
residency map, the export is served from the pool path, and the ARC is keyed
by the on-pool block pointer, so a staged copy does not warm the path a
consumer reads.  A receipt that reported bytes written as bytes saved would be
the #585 shape one level up -- a record that reads as a success because nobody
wrote down what did not happen.

Every stage record therefore carries the same verdict, in every state, and no
field anywhere claims a saving.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet, StagePool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prewarm_loop  # noqa: E402


def test_the_receipt_says_nobody_reads_what_it_staged(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("row.pt", 8192)])
    StagePool(tmp_path).install(monkeypatch)

    event = fleet.cycle(fleet.args(stage=True, stage_free_floor_bytes=0))

    for verdict in (event["stage"]["consumer"],
                    fleet.queue.prewarm(key)["stage"]["consumer"]):
        assert verdict["reads_stage"] is False
        assert verdict["verified_reads"] is None
        assert verdict["effect"] == "copy_only"
        assert "block pointer" in verdict["reason"]


def test_the_verdict_is_recorded_even_where_there_is_no_stage(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The claim is about the consumer, not about the tier, so it does not
    depend on a tier being there to make it."""

    fleet = Fleet(tmp_path)
    monkeypatch.setattr(prewarm_loop, "run_tool", lambda argv: "")

    event = fleet.cycle(fleet.args(stage=True))
    assert event["stage"]["state"] == "absent"
    assert event["stage"]["consumer"]["reads_stage"] is False


def test_staged_bytes_are_reported_apart_from_warmed_bytes(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Two numbers, two claims.  ``bytes_warmed`` is what the read moved into
    the ARC the consumer actually reads through; ``staged_bytes`` is what was
    copied somewhere nobody reads.  Adding them would be the overstatement."""

    fleet = Fleet(tmp_path)
    key = fleet.action("row", [fleet.file("row.pt", 8192)])
    StagePool(tmp_path).install(monkeypatch)

    fleet.cycle(fleet.args(stage=True, stage_free_floor_bytes=0))
    record = fleet.queue.prewarm(key)

    assert record["bytes_warmed"] == 8192
    assert record["warmed_bytes"] == 8192
    assert record["stage"]["staged_bytes"] == 8192
    assert record["status"] == "complete"
    # The layout is the consumer contract, and it is written down rather than
    # left for a reader of the tree to infer.
    assert "residency-map source" in record["stage"]["layout"]
    text = json.dumps(record).lower()
    for word in ("saved", "speedup", "faster"):
        assert word not in text

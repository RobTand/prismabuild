"""A prewarm cycle neither warms nor stages a released consumer's ready row (#963).

#954 made the claim fail a ready row whose key an operator released from an
origin batch, and ``tier_loop.live_consumers`` leave it out of what it stages.
The prewarm loop walked ``ready_items()`` itself and asked neither, so it
warmed that row into the ARC -- a wasted lookahead read -- and with ``--stage``
on it would copy the window for a consumer that can no longer run.

Both loops now ask one check, ``prewarm_loop.released_origin_consumer``: a key
listed in the queue-wide release index and confirmed by its release record, or
whose release cannot be read (unknown counts as released, as at the claim).

Fixture concession: the confirmed release is ``origin_consumer_release``
answering a record for the listed key; the listing is a real index entry, and
the unknown case is a real unreadable one, so the listing and the unknown path
are the production code's.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet, StagePool, prewarm_loop  # noqa: E402
from prismabuild import produced_output  # noqa: E402


def _index_entry(fleet: Fleet, key: str, body: str = "{}") -> Path:
    directory = fleet.queue.released_origin_consumers_dir()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{key}.{'b' * 16}.json"
    path.write_text(body)
    return path


def _confirm(monkeypatch: pytest.MonkeyPatch, released: set[str]) -> None:
    real = produced_output.origin_consumer_release

    def release(queue, key):
        if key in released:
            return {"consumer_action_key": key, "state": "unpublished"}
        return real(queue, key)

    monkeypatch.setattr(produced_output, "origin_consumer_release", release)


@pytest.mark.parametrize("stage_on", [False, True])
def test_a_released_ready_row_is_neither_warmed_nor_staged(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage_on: bool) -> None:
    fleet = Fleet(tmp_path)
    released = fleet.action("released", [fleet.file("r.pt", 8192)])
    live = fleet.action("live", [fleet.file("l.pt", 4096)])
    _index_entry(fleet, released)
    _confirm(monkeypatch, {released})
    overrides = {}
    if stage_on:
        stage = StagePool(tmp_path)
        stage.install(monkeypatch)
        overrides = {"stage": True, "stage_free_floor_bytes": 0}

    event = fleet.cycle(fleet.args(lookahead=1, **overrides))

    # The released row does not spend the lookahead of one: the live row
    # behind it is warmed.
    assert [row["action_key"] for row in event["warmed"]] == [live]
    assert {"action_key": released,
            "reason": "origin consumer released"} in event["skipped"]
    assert fleet.queue.prewarm(released) is None
    if stage_on:
        assert stage.objects() == ["l.pt.pbstage@0+4096"]
        assert event["stage"]["staged_bytes"] == 4096


def test_an_unreadable_release_is_not_warmed(tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    key = fleet.action("unknown", [fleet.file("u.pt", 4096)])
    _index_entry(fleet, key, body="not json")

    event = fleet.cycle(fleet.args())

    assert event["warmed"] == []
    assert event["skipped"] == [{"action_key": key,
                                 "reason": "origin consumer released"}]
    assert fleet.queue.prewarm(key) is None


def test_a_listed_key_without_a_release_record_is_still_warmed(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An index entry is a lead; the record decides, as at the claim."""

    fleet = Fleet(tmp_path)
    key = fleet.action("lead", [fleet.file("x.pt", 4096)])
    _index_entry(fleet, key)
    monkeypatch.setattr(produced_output, "origin_consumer_release",
                        lambda queue, k: None)

    event = fleet.cycle(fleet.args())

    assert [row["action_key"] for row in event["warmed"]] == [key]


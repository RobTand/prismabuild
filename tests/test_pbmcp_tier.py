"""``pb_tier``: every announced tier record, curated, with the bad files named.

Tier records are filed with the producer's own ``announce_tier``, so the
tests read what a tier loop actually files rather than a hand-typed
imitation of it.  The RAM-shaped record carries ``ram_admission`` and an
epoch; the ARC-shaped one carries neither, and both must read as ``null``
rather than as missing -- "this tier has no admission" and "the tiers
directory did not answer" call for opposite responses.
"""

from __future__ import annotations

from pathlib import Path
import sys

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(REPOSITORY / "tests"))

from prismabuild import pool  # noqa: E402
import pbmcp  # noqa: E402
import pbmcp_fixture as fx  # noqa: E402
import pytest


@pytest.fixture()
def fleet(tmp_path: Path) -> fx.Fleet:
    return fx.build(tmp_path)


@pytest.fixture()
def session(fleet: fx.Fleet) -> pbmcp.Session:
    return pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                         repo_link=fleet.repo_link)


def _announce(fleet: fx.Fleet) -> None:
    fleet.queue.announce_tier({
        "schema": "prismabuild.storage_tier.v1",
        "tier": "ram",
        "tier_id": "ram:fixture-box",
        "host": "fixture-box",
        "mountpoint": "/ram/prewarm",
        "epoch": "1700000000-aaaaaaaaaaaa",
        "tokens": {"fill_mb_s_pool_side": 311, "ram_gib": 112},
        "fill_supply": {"best_mb_s": 299.7, "ceiling_mb_s": 311.7,
                        "may_grow": False},
        "ram_admission": {"admissible": True, "reason": None},
        "window_gib": 112,
        "size_bytes": 257698037760,
        "capacity_bytes": 257698029568,
        "sampled_unix": 1700000000.0,
    })
    fleet.queue.announce_tier({
        "schema": "prismabuild.storage_tier.v1",
        "tier": "arc",
        "tier_id": "arc:fixture-box",
        "host": "fixture-box",
        "tokens": {"fill_mb_s_pool_side": 40},
        "fill_supply": {"best_mb_s": 38.1, "ceiling_mb_s": 40.0,
                        "may_grow": True},
        "sampled_unix": 1700000000.0,
    })


def test_reports_every_record_with_the_fields_debugging_reaches_for(
    session: pbmcp.Session, fleet: fx.Fleet,
) -> None:
    _announce(fleet)

    body = session.call("pb_tier")
    assert body["complete"] is True, (body["timed_out"], body["unavailable"])
    assert body["invalid"] == []
    rows = {row["tier_id"]: row for row in body["tiers"]}
    assert set(rows) == {"ram:fixture-box", "arc:fixture-box"}

    ram = rows["ram:fixture-box"]
    assert ram["tokens"] == {"fill_mb_s_pool_side": 311, "ram_gib": 112}
    assert ram["fill_supply"]["ceiling_mb_s"] == 311.7
    assert ram["ram_admission"] == {"admissible": True, "reason": None}
    assert ram["epoch"] == "1700000000-aaaaaaaaaaaa"
    assert ram["window_gib"] == 112
    assert ram["mountpoint"] == "/ram/prewarm"
    assert ram["age_s"] is not None and ram["age_s"] >= 0

    arc = rows["arc:fixture-box"]
    assert arc["ram_admission"] is None
    assert arc["epoch"] is None
    assert arc["window_gib"] is None


def test_a_tier_with_no_records_is_empty_not_missing(
    session: pbmcp.Session,
) -> None:
    body = session.call("pb_tier")
    assert body["complete"] is True
    assert body["tiers"] == []
    assert body["invalid"] == []


def test_a_misfiled_record_is_named_and_does_not_hide_the_rest(
    session: pbmcp.Session, fleet: fx.Fleet,
) -> None:
    _announce(fleet)
    wrong = fleet.queue_root / pool.TIERS / "ram:intruder.json"
    wrong.write_text('{"tier_id": "ram:someone-else"}', encoding="utf-8")
    broken = fleet.queue_root / pool.TIERS / "broken.json"
    broken.write_text("{not json", encoding="utf-8")

    body = session.call("pb_tier")
    assert body["complete"] is True
    assert {row["tier_id"] for row in body["tiers"]} == {
        "ram:fixture-box", "arc:fixture-box"}
    assert {entry["tier_id"] for entry in body["invalid"]} == {
        "ram:intruder", "broken"}

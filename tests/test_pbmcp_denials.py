"""``pb_denials``: the claim-denial histogram, against snapshots the fleet writes.

The snapshots are written by hand rather than through ``record_denial``
because that helper files to the host's *local* ledger base, not to the
shared queue this tool reads -- the shared copy arrives via the snapshot
publisher.  What is asserted here is the tool's half: the window, the
grouping by reason, and naming the most recently denied action per reason.
The record shape is the producer's own (``prismabuild.claim_denials.v1``),
so a change to it breaks these fixtures rather than letting a stale
expectation pass.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(REPOSITORY / "tests"))

from prismabuild import pool  # noqa: E402
import pbmcp  # noqa: E402
import pbmcp_fixture as fx  # noqa: E402

OLD_KEY = "e" * 64
RECENT_KEY = "f" * 63 + "0"
OTHER_KEY = "f" * 63 + "1"


@pytest.fixture()
def fleet(tmp_path: Path) -> fx.Fleet:
    return fx.build(tmp_path)


@pytest.fixture()
def session(fleet: fx.Fleet) -> pbmcp.Session:
    return pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                         repo_link=fleet.repo_link)


def _write_snapshot(fleet: fx.Fleet, host: str, records: dict) -> None:
    path = (fleet.queue_root / pool.RESERVATIONS / host / "adaptive"
            / pool.CLAIM_DENIALS)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"schema": pool.CLAIM_DENIALS_SCHEMA_V1,
                                "records": records}), encoding="utf-8")


def _denial(key: str, host: str, reason: str, denied_unix: float,
            published_unix: float | None = None) -> dict:
    return {
        "action_key": key,
        "published_unix": published_unix if published_unix is not None
        else denied_unix - 1.0,
        "host": host,
        "reason": reason,
        "evidence": {},
        "denied_unix": denied_unix,
    }


def test_groups_by_reason_and_names_the_most_recent_denial(
    session: pbmcp.Session, fleet: fx.Fleet,
) -> None:
    now = time.time()
    _write_snapshot(fleet, "fixture-box", {
        "first": _denial(OLD_KEY, "fixture-box", "already_claimed", now - 60.0),
        "second": _denial(RECENT_KEY, "fixture-box", "already_claimed",
                          now - 10.0),
        "other": _denial(OTHER_KEY, "fixture-box", "transition_busy",
                         now - 20.0),
    })

    body = session.call("pb_denials", {"hours": 1})
    assert body["complete"] is True, (body["timed_out"], body["unavailable"])
    host = body["hosts"]["fixture-box"]
    assert host["denials_in_window"] == 3
    assert body["total_in_window"] == 3
    claimed = host["by_reason"]["already_claimed"]
    assert claimed["count"] == 2
    assert claimed["latest_action_key"] == RECENT_KEY
    assert claimed["latest_action_key_prefix"] == RECENT_KEY[:12]
    busy = host["by_reason"]["transition_busy"]
    assert busy["count"] == 1
    assert busy["latest_action_key"] == OTHER_KEY


def test_the_window_leaves_out_older_denials(
    session: pbmcp.Session, fleet: fx.Fleet,
) -> None:
    now = time.time()
    _write_snapshot(fleet, "fixture-box", {
        "old": _denial(OLD_KEY, "fixture-box", "already_claimed",
                       now - 48 * 3600.0),
        "new": _denial(RECENT_KEY, "fixture-box", "already_claimed",
                       now - 60.0),
    })

    body = session.call("pb_denials", {"hours": 24})
    host = body["hosts"]["fixture-box"]
    assert host["denials_in_window"] == 1
    assert host["by_reason"]["already_claimed"]["latest_action_key"] == RECENT_KEY

    everything = session.call("pb_denials", {"hours": 0})
    assert everything["hosts"]["fixture-box"]["denials_in_window"] == 2
    assert everything["cutoff_unix"] is None


def test_hosts_without_a_snapshot_are_not_hosts(
    session: pbmcp.Session, fleet: fx.Fleet,
) -> None:
    now = time.time()
    _write_snapshot(fleet, "denying-box", {
        "one": _denial(OLD_KEY, "denying-box", "already_claimed", now - 5.0),
    })
    (fleet.queue_root / pool.RESERVATIONS / "quiet-box" / "adaptive").mkdir(
        parents=True)

    body = session.call("pb_denials", {"hours": 1})
    assert set(body["hosts"]) == {"denying-box"}


def test_an_invalid_snapshot_is_named_and_does_not_hide_the_rest(
    session: pbmcp.Session, fleet: fx.Fleet,
) -> None:
    now = time.time()
    _write_snapshot(fleet, "good-box", {
        "one": _denial(OLD_KEY, "good-box", "already_claimed", now - 5.0),
    })
    _write_snapshot(fleet, "bad-box", {"whatever": "not a denial snapshot"})

    body = session.call("pb_denials", {"hours": 1})
    assert body["complete"] is True
    assert set(body["hosts"]) == {"good-box"}
    assert any("bad-box" in str(note) for note in body["reader_notes"])


def test_an_empty_queue_is_an_empty_histogram(
    session: pbmcp.Session,
) -> None:
    body = session.call("pb_denials")
    assert body["complete"] is True
    assert body["hosts"] == {}
    assert body["total_in_window"] == 0


def test_hours_below_zero_is_refused_before_reading_the_mount(
    session: pbmcp.Session,
) -> None:
    with pytest.raises(pbmcp.ToolError):
        session.call("pb_denials", {"hours": -1})

"""``pb_movers``: movement-node states and queue depth per tier.

An agent watching a data-waiting consumer wants to know whether its movers
are running, waiting or done -- and "done" here has two halves: the mover
action's terminal record, and the movement receipt the mover filed beside it
in ``movers/``, which outlives the action's conclusion and is what the next
submission prices itself from.  This reads both, per tier, reusing the pool's
own readers: ``movers_claimed_on_tier`` for who holds a claim now, the
residency block on live records for who is waiting, and ``record_move``'s
sidecars for what already landed.

Egress take-backs are deliberately not here: an egress removes bytes rather
than staging them, and mixing the two would let "movement is keeping up"
hide a stage that is only draining.
"""

from __future__ import annotations

from pathlib import Path
import sys
import time

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(REPOSITORY / "tests"))

from prismabuild import pool, storage_tiers  # noqa: E402
import pbmcp  # noqa: E402
import pbmcp_fixture as fx  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"{storage_tiers.capacity_kind_of(TIER)}@{TIER}"
FILL_KIND = f"{storage_tiers.FILL_KIND}@{TIER}"
MANIFEST = "9" * 64
MOVER_READY = "1" * 64
MOVER_CLAIMED = "2" * 64
MOVER_FILED = "3" * 64
GIB = storage_tiers.GIB


def _residency(start: int = 0, end: int = 2 * GIB) -> dict[str, object]:
    return {
        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
        "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 40,
        "range_start_bytes": start, "range_end_bytes": end,
    }


def _publish(fleet: fx.Fleet, key: str) -> None:
    fleet.queue.publish(
        action_key=key, tags=["mover"], priority=-10,
        resources={"cpu": 1, "mem_gb": 1, STAGE_KIND: 2, FILL_KIND: 188},
        residency=_residency(), max_attempts=1, retry_safe=False,
        cas_root=fleet.cas_root, checkout_root=str(fleet.checkout),
        worker_script=str(fleet.base / "worker.py"))


@pytest.fixture()
def fleet(tmp_path: Path) -> fx.Fleet:
    built = fx.build(tmp_path)
    queue = built.queue
    queue.mint_tier_capacity(TIER, {"stage_gib": 8,
                                    storage_tiers.FILL_KIND: 200})
    queue.announce_tier({
        "schema": "prismabuild.storage_tier.v1",
        "tier": "stage", "tier_id": TIER, "host": "fixture-box",
        "tokens": {"fill_mb_s_pool_side": 311},
        "fill_supply": {"best_mb_s": 299.7, "ceiling_mb_s": 311.7,
                        "may_grow": False},
        "sampled_unix": 1700000000.0,
    })
    # One waiting, one running, one already landed as a sidecar receipt.
    _publish(built, MOVER_CLAIMED)
    claimed = queue.claim(tags=["mover"],
                          capacity={"cpu": 8, "mem_gb": 16})
    assert claimed is not None and claimed["action_key"] == MOVER_CLAIMED
    _publish(built, MOVER_READY)
    queue.record_move(MOVER_FILED, {
        "consumer_action_key": fx.CLAIMED_KEY,
        "tier_id": TIER,
        "stage_root": "/stage/dl380g10",
        "manifest_sha256": MANIFEST,
        "range_start_bytes": 0,
        "range_end_bytes": 2 * GIB,
        "range_bytes": 2 * GIB,
        "bytes_staged": 2 * GIB,
        "entries_declared": 4,
        "entries_staged": 4,
        "complete": True,
        "seconds": 12.5,
        "unix": 1700000001.0,
    })
    return built


@pytest.fixture()
def session(fleet: fx.Fleet) -> pbmcp.Session:
    return pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                         repo_link=fleet.repo_link)


def test_each_tier_names_its_waiting_running_and_landed_movers(
    session: pbmcp.Session,
) -> None:
    body = session.call("pb_movers")
    assert body["complete"] is True, (body["timed_out"], body["unavailable"])
    assert body["invalid"] == []
    (row,) = [row for row in body["tiers"] if row["tier_id"] == TIER]

    assert row["queue_depth"] == 2
    assert [mover["action_key"] for mover in row["ready_movers"]] == [MOVER_READY]
    assert [mover["action_key"] for mover in row["claimed_movers"]] == [
        MOVER_CLAIMED]
    waiting = row["ready_movers"][0]
    assert waiting["range_bytes"] == 2 * GIB
    assert waiting["tier_id"] == TIER

    assert row["receipts_filed"] == 1
    assert row["receipts_truncated"] is False
    (receipt,) = row["filed_receipts"]
    assert receipt["action_key"] == MOVER_FILED
    assert receipt["consumer_action_key"] == fx.CLAIMED_KEY
    assert receipt["bytes_staged"] == 2 * GIB
    assert receipt["complete"] is True


def test_a_tier_filter_keeps_only_that_tier(session: pbmcp.Session) -> None:
    body = session.call("pb_movers", {"tier_id": TIER})
    assert body["complete"] is True
    assert [row["tier_id"] for row in body["tiers"]] == [TIER]

    missing = session.call("pb_movers", {"tier_id": "stage:nowhere"})
    assert missing["complete"] is True
    assert missing["tiers"] == []


def test_a_mover_scan_that_does_not_answer_is_null_not_empty(
    fleet: fx.Fleet, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A wedged live-mover read must not present as a tier with no movers."""

    def never(*_args, **_kwargs):
        time.sleep(600)

    monkeypatch.setattr(pbmcp, "_mover_live_rows", never)
    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link, deadline_s=1.0)
    body = session.call("pb_movers", {"tier_id": TIER})
    assert body["complete"] is False
    (row,) = body["tiers"]
    assert row["ready_movers"] is None
    assert row["claimed_movers"] is None
    assert row["queue_depth"] is None
    # The abandoned reader suppresses the sections behind it, so the filed
    # receipts read as unread too rather than as a tier with no landings.
    assert row["filed_receipts"] is None


def test_a_queue_with_no_tiers_is_empty_not_missing(
    tmp_path: Path,
) -> None:
    bare = fx.build(tmp_path)
    session = pbmcp.Session(queue_root=bare.queue_root,
                            cas_root=bare.cas_root,
                            repo_link=bare.repo_link)
    body = session.call("pb_movers")
    assert body["complete"] is True
    assert body["tiers"] == []
    assert body["invalid"] == []

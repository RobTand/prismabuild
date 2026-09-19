"""Terminal actions must not leave reservations behind.

The queue's terminal record and its resource ledger are two views of one run.
If a finish loses the claimed-record race, it still has the claim snapshot it
executed from; dropping that identity leaks the reservation forever.
"""

from __future__ import annotations

import json
from pathlib import Path
import socket
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402


KEY = "d" * 64


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    return queue


def test_finish_lost_race_releases_from_the_claim_snapshot(
    tmp_path: Path, monkeypatch
) -> None:
    queue = _queue(tmp_path)
    queue.publish(
        action_key=KEY,
        cas_root="/cas",
        checkout_root="/checkout",
        worker_script="/worker.py",
        resources={"gpu": 1, "mem_gb": 8},
    )

    def claim_disappears_after_execution(item, **_kwargs):
        queue.item_path(pool.CLAIMED, KEY).unlink()
        return {"status": "executed", "returncode": 0}

    monkeypatch.setattr(queue, "execute", claim_disappears_after_execution)
    outcome = queue.serve_once(capacity={"gpu": 1, "mem_gb": 8})

    assert outcome["status"] == "executed"
    assert queue.item_path(pool.DONE, KEY).exists()
    assert queue.ledger().held_keys() == []
    assert queue.ledger().available() == {"gpu": 1, "mem_gb": 8}


def test_a_terminal_orphan_can_be_reclaimed_by_verified_key(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    queue.publish(
        action_key=KEY,
        cas_root="/cas",
        checkout_root="/checkout",
        worker_script="/worker.py",
        resources={"cpu": 1, "mem_gb": 3},
    )
    claimed = queue.claim(capacity={"cpu": 1, "mem_gb": 3})
    terminal = dict(claimed)
    terminal.update({"status": "executed", "finished_host": socket.gethostname()})
    queue.item_path(pool.DONE, KEY).write_text(json.dumps(terminal))
    queue.item_path(pool.CLAIMED, KEY).unlink()
    queue.lease_path(KEY).unlink()

    result = queue.reclaim_terminal_reservation(KEY)

    assert result["action_key"] == KEY
    assert result["released"] == 4
    assert len(result["hosts"]) == 1
    assert queue.ledger().held_keys() == []


def test_reclaim_refuses_while_a_claim_is_live(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    queue.publish(
        action_key=KEY,
        cas_root="/cas",
        checkout_root="/checkout",
        worker_script="/worker.py",
        resources={"cpu": 1},
    )
    queue.claim(capacity={"cpu": 1})

    try:
        queue.reclaim_terminal_reservation(KEY)
    except pool.PoolContractError as exc:
        assert "claimed" in str(exc)
    else:
        raise AssertionError("a live claim's reservation was reclaimable")


TIER = "prismabuild-stage:dl380g10"
TIER_KIND = "stage_gib"
OWNER = "e" * 64


def _tier_only_terminal(queue: pool.PoolQueue, *, owner: str) -> None:
    """A DONE mover that held tier tokens and no host reservation (#594).

    No ``ready/``, ``claimed/`` or lease record, exactly one terminal record,
    and no residency block, so the only guards left are the holder check and
    the container-lifecycle check -- which is the point: the early return for
    an empty holder list used to skip the second one.
    """

    ledger = queue.tier_ledger(TIER)
    queue.mint_tier_capacity(TIER, {TIER_KIND: 2})
    handle = ledger.begin_acquire(KEY, {TIER_KIND: 2})
    assert handle is not None
    assert ledger.commit_acquire(KEY, handle) == 2
    assert ledger.holder_tokens(KEY) == {TIER_KIND: 2}
    queue.item_path(pool.DONE, KEY).write_text(json.dumps({
        "action_key": KEY, "status": "executed",
        "finished_host": socket.gethostname(), "container_owner": owner,
    }))


def test_reclaim_of_a_tier_only_key_verifies_the_container_is_gone(
    tmp_path: Path,
) -> None:
    queue = _queue(tmp_path)
    _tier_only_terminal(queue, owner=OWNER)
    marker = queue.container_marker(OWNER)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()

    try:
        queue.reclaim_terminal_reservation(KEY)
    except pool.PoolContractError as exc:
        assert "container lifecycle" in str(exc)
    else:
        raise AssertionError(
            "a tier-only reservation came back while its container lives")
    assert queue.tier_ledger(TIER).holder_tokens(KEY) == {TIER_KIND: 2}


def test_reclaim_of_a_tier_only_key_releases_when_the_container_is_gone(
    tmp_path: Path,
) -> None:
    """The control: nothing held anywhere, container settled, tokens return."""

    queue = _queue(tmp_path)
    _tier_only_terminal(queue, owner=OWNER)
    assert not queue.container_marker(OWNER).exists()

    result = queue.reclaim_terminal_reservation(KEY)

    assert result == {"action_key": KEY, "released": 2, "hosts": []}
    assert queue.tier_ledger(TIER).holder_tokens(KEY) == {}

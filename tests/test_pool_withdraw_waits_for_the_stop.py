"""Capacity comes back when the action stops, not when the operator asks.

For an action with no Docker marker the container census reports completion at
once, and ``withdraw`` treated that as licence to release the holder's tokens
and remove its claim and lease even though nothing had been signalled. From
another box the operator cannot signal the launcher, so the holder notices the
withdrawal only at its next heartbeat: about thirty seconds plus termination
time in which a replacement action can be admitted on a token the original
payload is still using.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402

import pbrun  # noqa: E402

KEY_A = "a" * 64
KEY_B = "b" * 64
HOLDER = "holder-box"
OPERATOR = "operator-box"


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, key: str) -> None:
    q.publish(
        action_key=key,
        cas_root=q.root / "cas",
        checkout_root=q.root / "co",
        worker_script=q.root / "worker.py",
        resources={"cpu": 1},
    )


def _claim_on_the_holder(q: pool.PoolQueue, key: str) -> dict:
    """Claim as the holder box, with a launcher-style lease and no process."""

    with mock.patch.object(pool.socket, "gethostname", return_value=HOLDER):
        claimed = q.claim(owner="holder-worker", capacity={"cpu": 1})
        assert claimed is not None
        q.write_lease(key, owner="holder-worker", child_pid=99999999)
    return claimed


def _withdraw_from_elsewhere(q: pool.PoolQueue, key: str, **kw: object) -> dict:
    """Withdraw from a box that can neither see nor signal the action."""

    with mock.patch.object(pool.socket, "gethostname", return_value=OPERATOR), \
            mock.patch.object(pool, "find_launcher_pids", return_value=[]), \
            mock.patch.object(pool, "launcher_owns_action", return_value=False), \
            mock.patch.object(
                pool, "terminate_action",
                side_effect=AssertionError("must not signal")):
        return q.withdraw(key, **kw)


def test_a_remote_withdrawal_keeps_the_reservation_until_the_holder_stops(
    queue: pool.PoolQueue,
) -> None:
    """The issue #64 probe, inverted: no replacement is admitted meanwhile."""

    _publish(queue, KEY_A)
    _claim_on_the_holder(queue, KEY_A)
    assert queue.ledger(HOLDER).held() == {"cpu": 1}

    result = _withdraw_from_elsewhere(queue, KEY_A, reason="cancel remotely")

    assert result["signalled"] is None
    assert result["released"] == 0
    assert result["container_cleanup"]["deferred"] is True
    assert result["host"] == HOLDER
    assert result["stop_pending"]["holder_host"] == HOLDER
    # The claim and lease stay: they are the ownership record for a payload
    # that has not stopped, and the reaper needs them to conclude it.
    assert queue.item_path(pool.CLAIMED, KEY_A).exists()
    assert queue.lease_path(KEY_A).exists()
    assert queue.ledger(HOLDER).available() == {}
    assert queue.ledger(HOLDER).held() == {"cpu": 1}

    # The decision itself is durable straight away, as it always was.
    filed = json.loads(queue.item_path(pool.WITHDRAWN, KEY_A).read_text())
    assert filed["status"] == "withdrawn"
    assert filed["reason"] == "cancel remotely"

    # Withdrawal never rewrites the claiming worker's ownership record.
    live = json.loads(queue.item_path(pool.CLAIMED, KEY_A).read_text())
    assert "stop_pending" not in live
    assert "withdrawn_note" not in live

    # And nothing else can run on the holder's only token.
    _publish(queue, KEY_B)
    with mock.patch.object(pool.socket, "gethostname", return_value=HOLDER):
        assert queue.claim(owner="another-worker", capacity={"cpu": 1}) is None


def test_the_holder_completes_the_withdrawal_and_releases_then(
    queue: pool.PoolQueue,
) -> None:
    """What the holder's launcher checkpoint does next, and what it frees."""

    _publish(queue, KEY_A)
    claimed = _claim_on_the_holder(queue, KEY_A)
    _withdraw_from_elsewhere(queue, KEY_A, reason="cancel remotely")
    assert queue.ledger(HOLDER).held() == {"cpu": 1}

    # The launcher's cross-box checkpoint sees the marker and stops its child,
    # and serve_once then files the outcome with the claim it executed.
    assert queue.withdrawal_covers(claimed) is not None
    with mock.patch.object(pool.socket, "gethostname", return_value=HOLDER):
        destination = queue.finish(
            KEY_A, status="withdrawn", detail={"returncode": -15},
            claim_snapshot=claimed,
        )

    assert destination == queue.item_path(pool.WITHDRAWN, KEY_A)
    assert not queue.item_path(pool.CLAIMED, KEY_A).exists()
    assert not queue.lease_path(KEY_A).exists()
    assert queue.ledger(HOLDER).held() == {}
    assert queue.ledger(HOLDER).available() == {"cpu": 1}
    # Nothing is filed as a failure: a cancellation is a decision.
    assert not queue.item_path(pool.FAILED, KEY_A).exists()

    # Only now is the holder's token available to somebody else.
    _publish(queue, KEY_B)
    with mock.patch.object(pool.socket, "gethostname", return_value=HOLDER):
        other = queue.claim(owner="another-worker", capacity={"cpu": 1})
    assert other is not None and other["action_key"] == KEY_B


def test_a_local_withdrawal_defers_release_to_the_claiming_worker(
    queue: pool.PoolQueue,
) -> None:
    """The gate must not hold capacity when there is nothing left to stop."""

    _publish(queue, KEY_A)
    claimed = queue.claim(owner="local-worker", capacity={"cpu": 1})
    assert claimed is not None
    result = queue.withdraw(KEY_A, reason="changed my mind")
    assert result["stop_pending"] is not None
    assert result["released"] == 0
    assert queue.item_path(pool.CLAIMED, KEY_A).exists()
    queue.finish(KEY_A, status="withdrawn", claim_snapshot=claimed)
    assert not queue.item_path(pool.CLAIMED, KEY_A).exists()
    assert not queue.lease_path(KEY_A).exists()
    assert queue.ledger().available() == {"cpu": 1}


def test_pbrun_says_the_release_is_pending_on_the_holder(
    queue: pool.PoolQueue, capsys
) -> None:
    """An operator reading "released 0" must not read it as "nothing to do"."""

    _publish(queue, KEY_A)
    _claim_on_the_holder(queue, KEY_A)

    with mock.patch.object(pool.socket, "gethostname", return_value=OPERATOR), \
            mock.patch.object(pool, "find_launcher_pids", return_value=[]), \
            mock.patch.object(pool, "launcher_owns_action", return_value=False), \
            mock.patch.object(pbrun, "published_commit", return_value=""), \
            mock.patch.object(
                pool, "terminate_action",
                side_effect=AssertionError("must not signal")):
        assert pbrun.withdraw_main(queue, [KEY_A[:12]], reason="stale") == 0

    said = capsys.readouterr().err
    assert "released 0 token(s)" in said
    assert f"release pending on {HOLDER}" in said
    assert "returns the tokens when it stops the action" in said

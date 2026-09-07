"""Contradictory ownership cannot become a released or runnable claim (#301)."""
from unittest import mock

import pytest

from prismabuild import pool
from test_pool_claim_race_names_the_winner import (
    DEMAND, KEY, LOST_BY, WON_BY, _reap_on_a_third_box,
    _win_but_die_before_naming_itself, queue,
)


def _contradictory_claim(queue):
    _win_but_die_before_naming_itself(queue)
    # This is a fabricated invariant violation, not a claimed live interleaving.
    (queue.root / pool.RESERVATIONS / LOST_BY / "held" / KEY).mkdir(parents=True)
    return queue.item_path(pool.CLAIMED, KEY).read_bytes()


def _assert_preserved(queue, original):
    assert queue.item_path(pool.CLAIMED, KEY).read_bytes() == original
    assert not queue.item_path(pool.READY, KEY).exists()
    assert queue.claim_reservation_hosts(KEY) == sorted([WON_BY, LOST_BY])
    assert queue.ledger(WON_BY).held() == DEMAND


def test_reaper_retains_and_reports_ambiguous_claim(queue, capsys):
    original = _contradictory_claim(queue)
    with mock.patch.object(queue, "cleanup_action_containers", wraps=queue.cleanup_action_containers) as cleanup:
        assert _reap_on_a_third_box(queue) == []
    _assert_preserved(queue, original)
    cleanup.assert_not_called()
    message = capsys.readouterr().err
    assert "ambiguous claim holder" in message
    assert KEY in message and WON_BY in message and LOST_BY in message


def test_withdraw_refuses_ambiguous_claim_without_removing_ownership(queue):
    original = _contradictory_claim(queue)
    with mock.patch.object(queue, "cleanup_action_containers", wraps=queue.cleanup_action_containers) as cleanup:
        with pytest.raises(pool.PoolContractError, match="ambiguous claim holder"):
            queue.withdraw(KEY, reason="test withdrawal", signal_child=False)
    _assert_preserved(queue, original)
    assert not queue.item_path(pool.WITHDRAWN, KEY).exists()
    cleanup.assert_not_called()


def test_ambiguous_claim_does_not_block_reaping_another_claim(queue):
    original = _contradictory_claim(queue)
    other = "e" * 64
    queue.publish(action_key=other, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
                  resources={}, max_attempts=1, retry_safe=True)
    queue.item_path(pool.READY, other).rename(queue.item_path(pool.CLAIMED, other))
    assert _reap_on_a_third_box(queue) == [other]
    _assert_preserved(queue, original)
    assert queue.item_path(pool.READY, other).exists()


def test_recovery_resumes_when_contradictory_evidence_is_removed(queue):
    original = _contradictory_claim(queue)
    assert _reap_on_a_third_box(queue) == []
    _assert_preserved(queue, original)
    (queue.root / pool.RESERVATIONS / LOST_BY / "held" / KEY).rmdir()
    assert _reap_on_a_third_box(queue) == [KEY]
    assert queue.ledger(WON_BY).held() == {}

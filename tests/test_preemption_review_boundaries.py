"""Independent #365 review: private queues, logical tokens, no GPU work."""
import json
import uuid

from prismabuild import pool


def setup_holder(tmp_path, *, retry_safe=True, max_attempts=3):
    q = pool.PoolQueue(tmp_path / 'queue')
    q.ensure_layout()
    background, foreground = (uuid.uuid4().hex * 2 for _ in range(2))
    def publish(key, priority, **kw):
        q.publish(action_key=key, cas_root=tmp_path / 'cas',
                  checkout_root=tmp_path, worker_script=tmp_path / 'worker.py',
                  priority=priority, resources={'gpu': 1}, **kw)
    publish(background, -10, retry_safe=retry_safe, max_attempts=max_attempts)
    holder = q.claim(capacity={'gpu': 1})
    assert holder['action_key'] == background
    publish(foreground, 0)
    return q, background, foreground, holder


def test_explicit_non_retry_safe_holder_is_not_automatically_restarted(tmp_path):
    q, bg, fg, holder = setup_holder(tmp_path, retry_safe=False, max_attempts=1)
    assert q.claim(capacity={'gpu': 1}) is None
    assert not q.withdrawal_decisions(bg), 'priority alone cancelled non-idempotent work'
    assert not q.item_path(pool.READY, bg).exists()


def test_preemption_does_not_reset_already_spent_retry_attempts(tmp_path):
    q, bg, fg, holder = setup_holder(tmp_path)
    # Let the background action fail once without a competing ready action.
    q.item_path(pool.READY, fg).unlink()
    q.finish(bg, status='failed', detail={'returncode': 1}, claim_snapshot=holder)
    holder = q.claim(capacity={'gpu': 1})
    assert holder['attempts'] == 1
    q.publish(action_key=fg, cas_root=tmp_path / 'cas', checkout_root=tmp_path,
              worker_script=tmp_path / 'worker.py', priority=0, resources={'gpu': 1})
    assert q.claim(capacity={'gpu': 1}) is None
    requeued = json.loads(q.item_path(pool.READY, bg).read_text())
    assert requeued['attempts'] >= holder['attempts'], 'preemption refunded a prior failed attempt'
    assert requeued['attempt_history'] == holder['attempt_history']


def test_operator_withdrawal_winning_selection_race_is_not_reversed(tmp_path, monkeypatch):
    q, bg, fg, holder = setup_holder(tmp_path)
    withdraw = q.withdraw
    def operator_wins(key, **kw):
        assert key == bg
        withdraw(key, by='operator', reason='cancel this work', signal_child=False)
        return withdraw(key, **kw)
    monkeypatch.setattr(q, 'withdraw', operator_wins)
    assert q.claim(capacity={'gpu': 1}) is None
    assert not q.item_path(pool.READY, bg).exists(), 'scheduler revived operator-cancelled work'
    assert q.item_path(pool.WITHDRAWN, bg).exists()


def test_holder_finishing_before_withdraw_is_not_published_again(tmp_path, monkeypatch):
    q, bg, fg, holder = setup_holder(tmp_path)
    withdraw = q.withdraw
    def finish_wins(key, **kw):
        assert key == bg
        q.finish(bg, status='executed', detail={'returncode': 0}, claim_snapshot=holder)
        return withdraw(key, **kw)
    monkeypatch.setattr(q, 'withdraw', finish_wins)
    q.claim(capacity={'gpu': 1})
    assert q.item_path(pool.DONE, bg).exists()
    assert not q.item_path(pool.READY, bg).exists(), 'scheduler republished completed work'


def test_new_foreground_generation_winning_selection_race_is_not_preempted(tmp_path, monkeypatch):
    q, bg, fg, holder = setup_holder(tmp_path)
    withdraw = q.withdraw
    successor = {}
    def successor_wins(key, **kw):
        waiting = q.item_path(pool.READY, fg)
        saved = waiting.read_bytes()
        waiting.unlink()
        q.finish(bg, status='executed', detail={'returncode': 0}, claim_snapshot=holder)
        q.publish(action_key=bg, cas_root=tmp_path / 'cas', checkout_root=tmp_path,
                  worker_script=tmp_path / 'worker.py', priority=0,
                  resources={'gpu': 1}, retry_safe=False, max_attempts=1)
        successor.update(q.claim(capacity={'gpu': 1}))
        waiting.write_bytes(saved)
        return withdraw(key, **kw)
    monkeypatch.setattr(q, 'withdraw', successor_wins)
    q.claim(capacity={'gpu': 1})
    assert successor['priority'] == 0
    assert q.withdrawal_covers(successor, action_key=bg) is None
    assert not q.item_path(pool.READY, bg).exists()

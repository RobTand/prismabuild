"""Independent #365 review: private queues, logical tokens, no GPU work."""
import json
import uuid
import pytest

from prismabuild import pool

_REAL_ACTION_IDENTITY = pool.cpu_admission.action_identity


@pytest.fixture(autouse=True)
def known_generation_action(monkeypatch):
    # These private logical-token fixtures stand for a verified generation
    # request; individual tests override this to exercise unknown/measurement.
    monkeypatch.setattr(pool.cpu_admission, "action_identity", lambda item: ("shape", False))


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
    path = q.item_path(pool.READY, bg)
    retained = json.loads((path if path.exists() else q.item_path(pool.CLAIMED, bg)).read_text())
    assert retained['attempts'] >= holder['attempts'], 'preemption refunded a prior failed attempt'
    assert retained['attempt_history'] == holder['attempt_history']
    assert q.attempt_outcomes(retained)[0]['status'] == 'failed'


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


@pytest.mark.parametrize('identity', [(None, False), ('shape', True)])
def test_unknown_or_measurement_holder_is_not_preempted(tmp_path, monkeypatch, identity):
    q, bg, fg, holder = setup_holder(tmp_path)
    monkeypatch.setattr(pool.cpu_admission, 'action_identity', lambda item: identity)
    assert q.claim(capacity={'gpu': 1}) is None
    assert not q.withdrawal_decisions(bg)


def test_repeated_preemptions_and_failure_share_the_original_attempt_budget(tmp_path, monkeypatch):
    monkeypatch.setattr(pool.cpu_admission, 'action_identity', lambda item: ('shape', False))
    q, bg, fg, holder = setup_holder(tmp_path, max_attempts=3)
    generations = []
    for consumed in (1, 2):
        generations.append(holder['published_unix'])
        assert q.claim(capacity={'gpu': 1}) is None
        requeued = json.loads(q.item_path(pool.READY, bg).read_text())
        assert requeued['attempts'] == consumed
        assert requeued['max_attempts'] == 3
        assert requeued['supersedes_withdrawal']['published_unix'] == holder['published_unix']
        q.finish(bg, status='withdrawn', claim_snapshot=holder)
        foreground = q.claim(capacity={'gpu': 1})
        assert foreground['action_key'] == fg
        q.finish(fg, status='executed', claim_snapshot=foreground)
        holder = q.claim(capacity={'gpu': 1})
        assert holder['action_key'] == bg
        q.publish(action_key=fg, cas_root=tmp_path / 'cas', checkout_root=tmp_path,
                  worker_script=tmp_path / 'worker.py', priority=0, resources={'gpu': 1})
    # The last allowed launch must finish. Another foreground arrival cannot
    # manufacture a fourth launch, and its failure consumes the final attempt.
    assert q.claim(capacity={'gpu': 1}) is None
    assert not q.item_path(pool.READY, bg).exists()
    assert len(q.withdrawal_decisions(bg)) == 2
    for generation in generations:
        assert len(q.withdrawal_decisions(bg, generation=generation)) == 1
    q.finish(bg, status='failed', detail={'returncode': 1}, claim_snapshot=holder)
    ending = json.loads(q.item_path(pool.FAILED, bg).read_text())
    assert ending['attempts'] == 3
    assert q.adopted_attempt_summary(ending)['disposition'] == pool.FAILED
    assert q.ledger().held() == {}
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools' / 'fleet'))
    import pbrun
    landed = pbrun.landed_outcome(q, bg, wait_s=0, generation=generations[0])
    assert landed is not None and landed[1]['status'] == 'failed'
    assert landed[1]['published_unix'] == holder['published_unix']


def test_unattributed_attempt_prefix_is_not_restart_permission(tmp_path):
    q, bg, fg, holder = setup_holder(tmp_path)
    path = q.item_path(pool.CLAIMED, bg)
    # Existing legacy records can lack their earlier attempt evidence. Those
    # launches must not be misclassified as proven preemptions.
    record = json.loads(path.read_text())
    record.update(attempts=1, attempt_history_missing_before=1)
    path.write_text(json.dumps(record))
    assert q.claim(capacity={'gpu': 1}) is None
    assert not q.withdrawal_decisions(bg)


@pytest.mark.parametrize('measurement', [False, True])
def test_preemption_reads_the_sealed_action_class(tmp_path, monkeypatch, measurement):
    from prismabuild import core
    from test_core import _body
    monkeypatch.setattr(pool.cpu_admission, 'action_identity', _REAL_ACTION_IDENTITY)
    body = _body(tmp_path, task_class='measurement' if measurement else 'generation',
                 portability='host_class_keyed' if measurement else 'portable',
                 host_class='gb10' if measurement else None)
    action = core.seal_action(body)
    cas = core.PrismaBuildCAS(tmp_path / 'cas')
    cas.publish_action_request(action)
    q = pool.PoolQueue(tmp_path / 'queue')
    key = action['action_key']
    q.publish(action_key=key, cas_root=cas.root, checkout_root=tmp_path,
              worker_script=tmp_path / 'worker.py', priority=-10,
              resources={'gpu': 1}, retry_safe=True, max_attempts=2)
    holder = q.claim(capacity={'gpu': 1})
    assert holder['action_key'] == key
    q.publish(action_key='f' * 64, cas_root=cas.root, checkout_root=tmp_path,
              worker_script=tmp_path / 'worker.py', resources={'gpu': 1})
    assert q.claim(capacity={'gpu': 1}) is None
    assert bool(q.withdrawal_decisions(key)) is not measurement

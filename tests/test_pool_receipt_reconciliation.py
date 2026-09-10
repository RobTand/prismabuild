"""Explicit receipt recovery preserves the failed transport's causal evidence."""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools/fleet'))
from prismabuild import pool, core as pb
import pbwait
from test_pool_broker_disconnect import make_failed_attempt


@pytest.fixture
def failure(tmp_path, monkeypatch):
    return make_failed_attempt(tmp_path, monkeypatch)


def recover(fixture, **kwargs):
    from prismabuild.pool_reconcile import reconcile
    q, cas, action, ending = fixture
    return reconcile(q, action['action_key'], cas=cas,
                     generation=kwargs.pop('generation', q.attempt_generation(ending)),
                     attempt=kwargs.pop('attempt', ending['attempts']), **kwargs)


def snapshots(q, key):
    roots = [q.item_path(pool.FAILED, key), *sorted((q.root / pool.ATTEMPTS / key).rglob('*'))]
    return {str(p): p.read_bytes() for p in roots if p.is_file()}


def rewrite_ending(fixture, edit):
    q, _, action, ending = fixture
    edit(ending)
    q.item_path(pool.FAILED, action['action_key']).write_text(json.dumps(ending))


def test_explicit_recovery_verifies_action_payload_without_rewriting_attempt(failure):
    q, cas, action, ending = failure
    key = action['action_key']
    before = snapshots(q, key)
    result = recover(failure)
    assert result['payload_status'] == 'verified'
    assert result['result_scope'] == 'action'
    assert result['transport_status'] == 'failed'
    assert result['returncode'] == 125
    assert result['receipt_sha256'] == cas.lookup(action)['receipt_sha256']
    assert result['result'] == cas.lookup(action)['result']
    for path, raw in before.items():
        assert Path(path).read_bytes() == raw
    record = Path(result['reconciliation_path'])
    assert record.is_file() and not record.stat().st_mode & 0o222
    assert recover(failure) == result, 'repeated reconciliation reuses immutable evidence'
    assert not q.item_path(pool.DONE, key).exists()
    assert not q.item_path(pool.READY, key).exists()
    # The ordinary wait still reports the actual transport ending. Explicit
    # recovery is not an implicit license to reinterpret a failed attempt.
    rows = pbwait.wait_for_keys(q, [key], cas=cas, wait_s=0)
    assert rows[0]['status'] == 'failed'
    assert rows[0]['returncode'] == 125
    assert pbwait.verdict(rows) == 1


@pytest.mark.parametrize('kwargs', [{'generation': '0' * 64}, {'attempt': 2}, {'attempt': True}])
def test_generation_and_attempt_must_identify_current_failed_ending(failure, kwargs):
    with pytest.raises(ValueError):
        recover(failure, **kwargs)


def test_no_receipt_eof_is_not_recoverable(tmp_path, monkeypatch):
    failure = make_failed_attempt(tmp_path, monkeypatch, publish_receipt=False)
    with pytest.raises(ValueError):
        recover(failure)


@pytest.mark.parametrize('state', [pool.READY, pool.CLAIMED, pool.INTENT, pool.DONE, pool.WITHDRAWN])
def test_active_work_or_another_terminal_is_never_reconciled(failure, state):
    q, _, action, ending = failure
    path = q.item_path(state, action['action_key'])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(ending))
    before = snapshots(q, action['action_key'])
    with pytest.raises(ValueError):
        recover(failure)
    assert snapshots(q, action['action_key']) == before


def test_leftover_lease_is_not_cleanup_proof(failure):
    q, _, action, _ = failure
    q.lease_path(action['action_key']).write_text('{}')
    with pytest.raises(ValueError, match='lease'):
        recover(failure)


def test_held_reservation_is_not_released_by_reconciliation(failure):
    q, _, action, _ = failure
    holder = q.ledger().held_dir / action['action_key']
    holder.mkdir(parents=True)
    (holder / 'cpu.0000').touch()
    with pytest.raises(ValueError, match='reservation'):
        recover(failure)
    assert (holder / 'cpu.0000').exists()


@pytest.mark.parametrize('detail', [
    {'termination_reason': 'memory_limit_oom'},
    {'termination_reason': 'gpu_memory_limit'},
    {'termination_reason': 'host_memory_pressure'},
    {'action_survived_kill': True},
    {'action_returncode': 7},
    {'action_signal': 15},
    {'execution_timeout_s': 0.000001},
    {'stderr': 'PrismaBuild resource execution refused: unrelated failure\n'},
])
def test_termination_or_a_different_refusal_has_precedence(tmp_path, monkeypatch, detail):
    failure = make_failed_attempt(tmp_path, monkeypatch, finish_detail=detail)
    with pytest.raises(ValueError):
        recover(failure)


def test_timeout_ending_has_precedence_even_with_receipt(tmp_path, monkeypatch):
    failure = make_failed_attempt(tmp_path, monkeypatch, finish_status='timeout')
    with pytest.raises(ValueError):
        recover(failure)


@pytest.mark.parametrize('change', [
    lambda e: e.pop('resource_scope_cleanup'),
    lambda e: e['resource_scope_cleanup'].update(complete=False),
    lambda e: e['resource_scope_cleanup'].update(nonce='0' * 32),
    lambda e: e['resource_scope_cleanup']['released'].update(ok=False),
    lambda e: e['resource_scope_cleanup']['telemetry'].update(complete=False),
    lambda e: e['resource_scope_cleanup']['telemetry'].update(action_key='0' * 64),
    lambda e: e['resource_scope_cleanup']['telemetry'].update(oom_local=1),
    lambda e: e['resource_scope_cleanup']['telemetry'].update(oom_kill=1),
    lambda e: e['resource_scope_cleanup']['telemetry'].update(termination_reason='gpu_memory_limit'),
    lambda e: e['resource_scope_cleanup']['released'].update(stop_reason='launcher received signal 15'),
    lambda e: e['resource_scope_cleanup']['telemetry'].pop('oom_local'),
])
def test_missing_contradictory_or_resource_kill_cleanup_is_refused(failure, change):
    rewrite_ending(failure, change)
    with pytest.raises(ValueError):
        recover(failure)


def test_mutable_summary_cannot_replace_archived_failure(failure):
    rewrite_ending(failure, lambda e: e['detail'].update(returncode=0))
    with pytest.raises(ValueError):
        recover(failure)


@pytest.mark.parametrize('target', ['request', 'receipt', 'payload', 'attempt', 'stderr'])
def test_corrupt_canonical_evidence_is_not_accepted(failure, target):
    q, cas, action, ending = failure
    key = action['action_key']
    if target == 'request':
        path = Path(cas.root) / 'requests' / key[:2] / f'{key}.json'
    elif target == 'receipt':
        path = cas._receipt_path(key)
    elif target == 'payload':
        path = cas.result_path(cas.lookup(action), action)
    elif target == 'attempt':
        path = q.attempt_path(ending, ending['attempts'])
    else:
        path = q.root / q.attempt_outcomes(ending)[-1]['logs']['stderr']['path']
    path.chmod(0o644)
    path.write_bytes(b'corrupt evidence\n')
    path.chmod(0o444)
    with pytest.raises((OSError, ValueError, pb.CASTamperError)):
        recover(failure)


def test_existing_conflicting_reconciliation_is_preserved(failure):
    result = recover(failure)
    path = Path(result['reconciliation_path'])
    path.chmod(0o644)
    path.write_text('{}')
    path.chmod(0o444)
    with pytest.raises((OSError, ValueError, pb.CASTamperError)):
        recover(failure)
    assert path.read_text() == '{}'


def test_cli_recovery_is_explicit_and_preserves_original_exit(failure, monkeypatch, capsys):
    q, _, action, ending = failure
    monkeypatch.setattr(pbwait.pbrun, 'SH', q.root.parent)
    # The CLI's live layout is pb-queue. Give its factory the fixture queue;
    # all authority is still the fixture's canonical CAS and records.
    monkeypatch.setattr(pbwait.pool, 'PoolQueue', lambda _root: q)
    status = pbwait.main(['--reconcile-pool', '--generation', q.attempt_generation(ending),
                          '--attempt', '1', action['action_key']])
    assert status == 0
    output = capsys.readouterr().out
    assert 'payload_verified' in output and '125' in output
    assert 'failed' in output and 'reconciliation' in output
    assert json.loads(q.item_path(pool.FAILED, action['action_key']).read_text())['status'] == 'failed'


@pytest.mark.parametrize('argv', [
    ['--reconcile-pool', 'a' * 64],
    ['--generation', 'b' * 64, 'a' * 64],
    ['--attempt', '1', 'a' * 64],
    ['--reconcile-pool', '--generation', 'b' * 64, '--attempt', '1', 'a' * 64, 'c' * 64],
])
def test_cli_refuses_ambiguous_reconciliation_before_reading_fleet(argv):
    with pytest.raises(SystemExit) as exc:
        pbwait.main(argv)
    assert exc.value.code == 2


def test_newer_intent_is_refused_without_deleting_it(failure):
    q, _, action, ending = failure
    path = q.item_path(pool.INTENT, action['action_key'])
    intent = json.loads(path.read_text())
    intent['intent_unix'] = ending['finished_unix'] + 1
    path.write_text(json.dumps(intent))
    with pytest.raises(ValueError, match='claim intent'):
        recover(failure)
    assert json.loads(path.read_text()) == intent


def test_a_missing_completed_intent_does_not_invent_active_work(failure):
    q, _, action, _ = failure
    q.item_path(pool.INTENT, action['action_key']).unlink()
    assert recover(failure)['payload_status'] == 'verified'


def test_changed_ending_during_cas_verification_never_files_supplement(failure, monkeypatch):
    q, cas, action, _ = failure
    original_lookup = cas.lookup
    def lookup(action):
        receipt = original_lookup(action)
        rewrite_ending(failure, lambda e: e.update(published_unix=e['published_unix'] + 1))
        return receipt
    monkeypatch.setattr(cas, 'lookup', lookup)
    with pytest.raises(ValueError):
        recover(failure)
    assert not list((q.root / pool.ATTEMPTS / action['action_key']).rglob('*.receipt-reconciliation.json'))


def test_withdrawal_during_cas_verification_has_precedence(failure, monkeypatch):
    q, cas, action, ending = failure
    original_lookup = cas.lookup
    def lookup(action):
        receipt = original_lookup(action)
        q.item_path(pool.WITHDRAWN, action['action_key']).write_text(json.dumps(ending))
        return receipt
    monkeypatch.setattr(cas, 'lookup', lookup)
    with pytest.raises(ValueError, match='withdrawn'):
        recover(failure)
    assert not list((q.root / pool.ATTEMPTS / action['action_key']).rglob('*.receipt-reconciliation.json'))


def test_cli_reports_cas_corruption_as_refusal(failure, monkeypatch, capsys):
    q, cas, action, ending = failure
    monkeypatch.setattr(pbwait.pbrun, 'SH', q.root.parent)
    monkeypatch.setattr(pbwait.pool, 'PoolQueue', lambda _root: q)
    path = cas._receipt_path(action['action_key'])
    path.chmod(0o644)
    path.write_text('{}')
    path.chmod(0o444)
    assert pbwait.main(['--reconcile-pool', '--generation', q.attempt_generation(ending),
                        '--attempt', '1', action['action_key']]) == 1
    assert 'reconciliation refused' in capsys.readouterr().err


@pytest.mark.parametrize('same_generation', [True, False])
def test_retired_withdrawal_decision_is_scoped_to_its_generation(failure, same_generation):
    q, _, action, ending = failure
    decision = {**ending, 'status': 'withdrawn'}
    if not same_generation:
        decision['published_unix'] -= 1
    q._persist_withdrawal_decision(decision)
    assert not q.item_path(pool.WITHDRAWN, action['action_key']).exists()
    if same_generation:
        with pytest.raises(ValueError, match='withdrawal decision'):
            recover(failure)
    else:
        assert recover(failure)['payload_status'] == 'verified'


def test_boolean_action_returncode_is_not_a_successful_exit(tmp_path, monkeypatch):
    failure = make_failed_attempt(tmp_path, monkeypatch, finish_detail={'action_returncode': False})
    with pytest.raises(ValueError, match='termination'):
        recover(failure)


def test_recorded_failed_cleanup_reason_is_distinct_from_resource_termination(failure):
    _, _, _, ending = failure
    def production_cleanup(e):
        e['resource_scope_cleanup']['released'] = {
            'ok': True, 'scope_id': e['resource_scope']['scope_id'],
            'stop_reason': 'failed', 'stopped_unix': e['resource_scope_cleanup']['checked_unix'],
        }
        e['resource_scope_cleanup']['telemetry']['termination_reason'] = 'failed'
    rewrite_ending(failure, production_cleanup)
    assert recover(failure)['scope_cleanup'] == ending['resource_scope_cleanup']


def test_invalid_reservation_namespace_does_not_mean_no_reservations(failure):
    q, _, _, _ = failure
    path = q.root / pool.RESERVATIONS
    retained = q.root / 'retained-reservations'
    path.rename(retained)
    path.write_text('unreadable reservation inventory')
    with pytest.raises(ValueError, match='reservation root'):
        recover(failure)
    assert retained.is_dir()

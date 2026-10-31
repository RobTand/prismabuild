"""A refused create leaves a verifiable debt, never an unproved release."""
from contextlib import contextmanager
import os
from pathlib import Path
import socket
import threading

import pytest

from prismabuild import pool, resource_scope
from test_pool_resource_scope import scoped
from test_resource_broker import Backend, module

_REAL_REQUEST = resource_scope.ResourceScope._request


@pytest.fixture
def refused(scoped, tmp_path, monkeypatch):
    queue, item, _ = scoped
    key = item['action_key']
    item['max_attempts'] = 1
    queue.records.replace_record_at(queue.records.item_path(pool.CLAIMED, key), item)
    directory = tmp_path / 'socket'
    directory.mkdir()
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    endpoint = Path(f'/proc/{os.getpid()}/fd/{fd}/broker.sock')
    blocker = socket.socket(socket.AF_UNIX)
    blocker.bind(str(endpoint))  # Bound without listening gives ECONNREFUSED.
    monkeypatch.setattr(resource_scope, 'BROKER_SOCKET', endpoint)
    monkeypatch.setitem(resource_scope.ResourceScope.__init__.__kwdefaults__, 'socket_path', endpoint)
    monkeypatch.setattr(resource_scope.ResourceScope, '_request', _REAL_REQUEST)
    monkeypatch.setattr(queue, '_sweep_due', lambda: False)
    monkeypatch.setattr(queue, 'claim', lambda **kw: item)
    with pytest.raises(ConnectionRefusedError):
        queue.serve_once(containment=True)
    blocker.close()
    endpoint.unlink()
    broker = module()
    if os.environ.get('PB2270_MUTANT') == 'scope':
        original_handle = broker.Authority.handle
        def lying_kernel(self, uid, pid, request):
            if request.get('op') != 'cleanup_verdict':
                return original_handle(self, uid, pid, request)
            unit = broker.scope_id(request['action_key'], request['nonce'])
            group = self.backend.groups.pop(unit, None)
            if unit in self.records:
                self.records[unit]['released_unix'] = 1
            try:
                return original_handle(self, uid, pid, request)
            finally:
                if group is not None:
                    self.backend.groups[unit] = group
        monkeypatch.setattr(broker.Authority, 'handle', lying_kernel)
    kernel = Backend()
    authority = broker.Authority(tmp_path / 'broker-state', os.getuid(), kernel,
                                 max_memory_bytes=8 * 1024**3)

    @contextmanager
    def connected():
        server = broker.Server(str(endpoint), broker.Handler)
        server.authority = authority
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield authority, kernel
        finally:
            server.shutdown()
            thread.join(timeout=60)
            server.server_close()
            endpoint.unlink(missing_ok=True)
    try:
        yield queue, item, connected
    finally:
        os.close(fd)


def test_refusal_records_debt_and_holds_capacity_until_verified(refused):
    queue, item, connected = refused
    key = item['action_key']
    pending = pool.record_store.read_document(queue.records.item_path(pool.CLAIMED, key))
    assert pending['cleanup_obligation']['state'] == 'pending'
    assert queue.ledger().held() == {'cpu': 1, 'mem_gb': 2}
    with connected():
        queue.reap_stale()
    terminal = pool.record_store.read_document(queue.records.item_path(pool.FAILED, key))
    assert terminal['resource_scope_cleanup']['complete'] is True
    assert terminal['container_cleanup']['complete'] is True
    assert terminal['cleanup_obligation']['state'] == 'resolved'
    assert queue.ledger().held() == {}


def _terminal_without_proof(queue, item, monkeypatch, *, legacy=False):
    """Materialize the historical outage shape in a private fixture queue."""
    key = item['action_key']
    path = queue.records.item_path(pool.CLAIMED, key)
    pending = pool.record_store.read_document(path)
    if legacy:
        pending.pop('cleanup_obligation', None)
        queue.records.replace_record_at(path, pending)
    with monkeypatch.context() as patch:
        patch.setattr(queue, 'cleanup_action_containers',
                      lambda *a, **kw: {'complete': True, 'used': False})
        queue.finish(key, status='failed', detail={'exception': 'ConnectionRefusedError(111)'},
                     claim_snapshot=pending)
    return queue.records.item_path(pool.FAILED, key)


@pytest.mark.parametrize('legacy', [False, True])
def test_operator_recovers_terminal_with_released_tokens_and_preserves_history(refused, monkeypatch, legacy):
    queue, item, connected = refused
    path = _terminal_without_proof(queue, item, monkeypatch, legacy=legacy)
    before = pool.record_store.read_document(path)
    generation = queue.records.attempt_generation(before)
    attempt_path = queue.records.attempt_path(before, 1)
    archived = attempt_path.read_bytes()
    with connected() as (authority, kernel):
        result = queue.reconcile_cleanup(item['action_key'], generation, 1)
        again = queue.reconcile_cleanup(item['action_key'], generation, 1)
        nonce = before['resource_scope_intent']['nonce']
        with pytest.raises(ValueError, match='released|budget'):
            authority.handle(os.getuid(), os.getpid(), {
                'op': 'create', 'action_key': item['action_key'], 'nonce': nonce,
                'memory_max_bytes': 2 * 1024**3})
        assert kernel.groups == {}
    after = pool.record_store.read_document(path)
    assert result['resource_scope_cleanup']['complete'] is True
    assert result == again
    assert after['status'] == 'failed'
    assert after['detail'] == before['detail']
    assert attempt_path.read_bytes() == archived
    assert queue.ledger().held() == {}
    supplement = attempt_path.with_suffix('.cleanup-recovery.json')
    assert pool.record_store.read_document(supplement)['owner_record'] == before


def test_operator_recovers_lease_lost_cleanup_pending_after_reboot(refused, monkeypatch):
    queue, item, connected = refused
    key = item['action_key']
    claim = queue.records.item_path(pool.CLAIMED, key)
    abandoned = pool.record_store.read_document(claim)
    abandoned.pop('finish_pending')
    abandoned.pop('cleanup_obligation')
    queue.records.replace_record_at(claim, abandoned)
    with monkeypatch.context() as patch:
        patch.setattr(queue, 'cleanup_action_containers',
                      lambda *a, **kw: {'complete': True, 'used': False})
        queue.reap_stale(timeout_s=-1)
    path = queue.records.item_path(pool.FAILED, key)
    before = pool.record_store.read_document(path)
    assert before['status'] == 'lease_lost_max_attempts'
    # Retain the refusal written by pre-fix runtimes, even after prevention lands.
    before['container_cleanup_pending'] = abandoned['container_cleanup_pending']
    queue.records.replace_record_at(path, before)
    archived_path = queue.records.attempt_path(before, 1)
    archived = archived_path.read_bytes()
    generation = queue.records.attempt_generation(before)
    with connected():
        result = queue.reconcile_cleanup(key, generation, 1)
        assert queue.reconcile_cleanup(key, generation, 1) == result
    after = pool.record_store.read_document(path)
    assert 'container_cleanup_pending' not in after
    for field in ('status', 'detail', 'attempts', 'attempt_history', 'finished_unix', 'finished_host'):
        assert after[field] == before[field]
    assert archived_path.read_bytes() == archived
    assert queue.ledger().held() == {}


@pytest.mark.parametrize('field', ['scratch_declaration_record', 'scratch_lifetime_record'])
def test_scope_recovery_preserves_separate_scratch_cleanup(refused, monkeypatch, field):
    queue, item, connected = refused
    path = _terminal_without_proof(queue, item, monkeypatch)
    record = pool.record_store.read_document(path)
    record[field] = {'unavailable': True}
    pending = {'complete': False, 'error': 'scratch cleanup evidence unavailable'}
    record['container_cleanup_pending'] = pending
    queue.records.replace_record_at(path, record)
    with connected():
        queue.reconcile_cleanup(item['action_key'], queue.records.attempt_generation(record), 1)
    assert pool.record_store.read_document(path)['container_cleanup_pending'] == pending


@pytest.mark.parametrize('fault', ['nonce', 'attempt', 'host', 'scope', 'stale', 'unknown_scope', 'tickets'])
def test_reconciliation_refuses_wrong_or_unproved_identity(refused, monkeypatch, fault):
    queue, item, connected = refused
    path = _terminal_without_proof(queue, item, monkeypatch)
    record = pool.record_store.read_document(path)
    generation = queue.records.attempt_generation(record)
    key = item['action_key']
    before = path.read_bytes()
    with connected() as (authority, kernel):
        with monkeypatch.context() as patch:
            if fault == 'nonce':
                changed = {**record, 'resource_scope_intent': {
                    **record['resource_scope_intent'], 'nonce': 'f'*32}}
                queue.records.replace_record_at(path, changed)
                before = path.read_bytes()
            elif fault == 'host':
                patch.setattr(socket, 'gethostname', lambda: 'foreign-owner')
            elif fault in ('scope', 'unknown_scope', 'tickets'):
                intent = record['resource_scope_intent']
                if fault in ('scope', 'tickets'):
                    created = authority.handle(os.getuid(), os.getpid(), {
                        **{k: intent[k] for k in ('action_key', 'nonce', 'memory_max_bytes')}, 'op': 'create'})
                    if fault == 'tickets':
                        for operation in ('stop', 'release'):
                            authority.handle(os.getuid(), os.getpid(), {
                                'action_key': key, 'nonce': intent['nonce'],
                                'op': operation, 'token': created['token']})
                        authority.records[created['scope_id']]['container_tickets'] = {'unresolved': {}}
                else:
                    from prismabuild.produced_output import _broker_scope_id
                    kernel.create(_broker_scope_id(key, intent['nonce']), 1024)
            elif fault == 'stale':
                request = resource_scope.broker_request
                def replay(message, **kw):
                    return request({**message, 'challenge': '0'*64}, **kw)
                patch.setattr(resource_scope, 'broker_request', replay)
            with pytest.raises((ValueError, OSError)):
                queue.reconcile_cleanup(key, generation, 2 if fault == 'attempt' else 1)
        assert path.read_bytes() == before
        assert not queue.records.attempt_path(record, 1).with_suffix('.cleanup-recovery.json').exists()


def test_permits_are_single_use_and_refuse_forgery_and_changed_owners(refused, monkeypatch, tmp_path):
    import copy
    import pickle
    from prismabuild import cleanup_obligation as debt
    queue, item, connected = refused
    path = _terminal_without_proof(queue, item, monkeypatch)
    record = pool.record_store.read_document(path)
    key = item['action_key']
    generation = queue.records.attempt_generation(record)
    with connected():
        permit = debt._verify_cleanup_identity(queue, key, generation, 1)
        for operation in (copy.copy, copy.deepcopy, pickle.dumps):
            with pytest.raises(TypeError):
                operation(permit)
        with pytest.raises(TypeError):
            debt._CleanupPermit()
        subclass = type('ForgedPermit', (debt._CleanupPermit,), {})
        for forged in (None, {}, object.__new__(debt._CleanupPermit),
                       object.__new__(subclass), object()):
            with pytest.raises(TypeError):
                debt._publish_cleanup_permit(queue, forged)
        with pytest.raises(TypeError):
            debt._publish_cleanup_permit(pool.PoolQueue(tmp_path / 'foreign'), permit)
        permit = debt._verify_cleanup_identity(queue, key, generation, 1)
        queue.records.replace_record_at(path, {**record, 'claimed_by': 'new-attempt'})
        with pytest.raises(ValueError):
            debt._publish_cleanup_permit(queue, permit)
        queue.records.replace_record_at(path, record)
        permit = debt._verify_cleanup_identity(queue, key, generation, 1)
        debt._publish_cleanup_permit(queue, permit)
        with pytest.raises(TypeError):
            debt._publish_cleanup_permit(queue, permit)


def _guard_cleanup_authority(source):
    import ast
    tree = ast.parse(source)
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    for call in ast.walk(tree):
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == '_CleanupPermit':
            parent = parents[call]
            while not isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
                parent = parents[parent]
            assert parent.name == '_verify_cleanup_identity'


def _guard_cleanup_signatures(source):
    import ast
    tree = ast.parse(source)
    for name in ('reconcile_cleanup', '_recover_cleanup_obligation'):
        method = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)
        assert [a.arg for a in method.args.args] == ['self', 'action_key', 'generation', 'attempt']
        assert method.args.posonlyargs == []
        assert method.args.kwonlyargs == []
        assert method.args.defaults == []
        assert method.args.kw_defaults == []
        assert method.args.kwarg is None
        assert method.args.vararg is None


def test_authority_shape_and_bypass_mutant_are_guarded():
    from prismabuild import cleanup_obligation as debt
    source = Path(debt.__file__).read_text()
    _guard_cleanup_authority(source)
    root = Path(pool.__file__).resolve().parents[2]
    for directory in ('src', 'tools'):
        for path in (root / directory).rglob('*.py'):
            candidate = path.read_text()
            if '_CleanupPermit' in candidate:
                _guard_cleanup_authority(candidate)
    with pytest.raises(AssertionError):
        _guard_cleanup_authority(source + '\ndef bypass():\n    return _CleanupPermit(_MINT)\n')
    _guard_cleanup_signatures(Path(pool.__file__).read_text())
    with pytest.raises(AssertionError):
        _guard_cleanup_signatures(
            'class Queue:\n'
            ' def reconcile_cleanup(self, action_key, generation, attempt, *, proof=None): pass\n'
            ' def _recover_cleanup_obligation(self, action_key, generation, attempt): pass\n')


def test_private_verifier_rejects_foreign_queue_objects(refused):
    from types import SimpleNamespace
    from prismabuild import cleanup_obligation as debt
    queue, item, connected = refused
    with pytest.raises(TypeError):
        debt._verify_cleanup_identity(SimpleNamespace(records=queue.records),
            item['action_key'], queue.records.attempt_generation(item), 1)


@pytest.fixture(autouse=True)
def _named_cleanup_mutant(monkeypatch):
    """Only the admitted mutation driver enables these deliberate defects."""
    mutant = os.environ.get('PB2270_MUTANT')
    if not mutant or mutant == 'scope':
        return
    from prismabuild import cleanup_obligation as debt
    if mutant == 'no-debt':
        original = pool.PoolQueue._note_cleanup_attempt
        def forget(self, pending, prior, cleanup):
            original(self, pending, prior, cleanup)
            pending.pop('cleanup_obligation', None)
        monkeypatch.setattr(pool.PoolQueue, '_note_cleanup_attempt', forget)
    elif mutant == 'missing-evidence':
        original = pool.PoolQueue._cleanup_payload_containers
        def omit_proof(self, record, **kw):
            if record.get('resource_scope') is None and record.get('resource_scope_intent'):
                try:
                    recovered = self._recover_resource_scope_creation(record)
                except Exception:
                    return original(self, record, **kw)
                if not recovered:
                    return self._cleanup_action_containers(record)
            return original(self, record, **kw)
        monkeypatch.setattr(pool.PoolQueue, '_cleanup_payload_containers', omit_proof)
    elif mutant == 'attempt':
        original = pool.PoolQueue.reconcile_cleanup
        monkeypatch.setattr(pool.PoolQueue, 'reconcile_cleanup',
            lambda self, key, generation, attempt: original(self, key, generation, 1))
    elif mutant == 'companion-bypass':
        monkeypatch.setattr(debt, '_check_companion_records', lambda *a, **kw: None)
    elif mutant == 'chronological-successor':
        original = debt._check_companion_records
        def require_chronology(queue, key, record, **kw):
            ready = pool.record_store.read_document(queue.records.item_path(pool.READY, key))
            if ready is not None:
                queue._successor_generation(record, ready)
            return original(queue, key, record, **kw)
        monkeypatch.setattr(debt, '_check_companion_records', require_chronology)
    elif mutant in ('reject-withdrawal', 'reject-successor'):
        original = debt._check_companion_records
        state = pool.WITHDRAWN if mutant == 'reject-withdrawal' else pool.READY
        def reject_companion(queue, key, record, **kw):
            if pool.record_store.read_document(queue.records.item_path(state, key)) is not None:
                raise ValueError('injected blanket companion refusal')
            return original(queue, key, record, **kw)
        monkeypatch.setattr(debt, '_check_companion_records', reject_companion)
    elif mutant == 'permit-bypass':
        monkeypatch.setattr(debt, '_publish_cleanup_permit', lambda queue, permit: {})
    else:
        import inspect
        import textwrap
        function = (debt._publish_cleanup_permit if mutant == 'retry-metadata' else
                    debt._cleanup_owner if mutant in ('nonce', 'owner-key', 'lease-key') else
                    debt._verify_cleanup_identity)
        source = textwrap.dedent(inspect.getsource(function))
        if mutant == 'nonce':
            source = source.replace("if debt is not None and (", "if False and (")
        elif mutant in ('owner-key', 'lease-key'):
            source = source.replace(
                "if record.get('action_key') != key or (lease is not None and lease.get('action_key') != key):",
                'if False:')
        elif mutant == 'retry-metadata':
            source = source.replace(
                "for field in ('action_key', 'published_unix',",
                "for field in ('container_cleanup_attempts', 'action_key', 'published_unix',")
        elif mutant == 'stale':
            source = source.replace("'challenge': challenge, 'empty': True", "'challenge': answer.get('challenge'), 'empty': True")
        else:
            raise ValueError('unknown named cleanup mutant')
        namespace = {}
        exec(compile(source, function.__code__.co_filename, 'exec'), debt.__dict__, namespace)
        monkeypatch.setattr(debt, function.__name__, namespace[function.__name__])


def test_operator_cli_and_pb_action_separate_cleanup_from_failure(refused, monkeypatch, capsys):
    import importlib
    import json
    from types import SimpleNamespace
    queue, item, connected = refused
    path = _terminal_without_proof(queue, item, monkeypatch)
    record = pool.record_store.read_document(path)
    key = item['action_key']
    generation = queue.records.attempt_generation(record)
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / 'tools/fleet'))
    pbwait = importlib.import_module('pbwait')
    pbmcp = importlib.import_module('pbmcp')
    destination = queue.root.with_name('pb-queue')
    queue.root.rename(destination)
    queue = pool.PoolQueue(destination)
    session = pbmcp.Session(queue_root=queue.root, cas_root=Path(item['cas_root']),
                           repo_link=Path(__file__).resolve().parents[1])
    pending = session.call('pb_action', {'key_prefix': key})
    assert pending['outcome']['status'] == 'failed'
    assert pending['outcome']['cleanup_obligation']['state'] == 'pending'
    monkeypatch.setattr(pbwait, 'store_for', lambda _: SimpleNamespace(root=queue.root.parent))
    args = ['--reconcile-cleanup', '--generation', generation, '--attempt', '1', key]
    assert pbwait.main(args) == 1
    assert 'reconciliation refused' in capsys.readouterr().err
    with connected():
        assert pbwait.main(args) == 0
        result = json.loads(capsys.readouterr().out)
        assert result['cleanup_obligation']['state'] == 'resolved'
    observed = session.call('pb_action', {'key_prefix': key})
    assert observed['outcome']['status'] == 'failed'
    assert observed['outcome']['cleanup_obligation']['state'] == 'resolved'


def test_operator_verifies_pending_debt_then_ordinary_finish_returns_tokens(refused):
    queue, item, connected = refused
    key = item['action_key']
    generation = queue.records.attempt_generation(item)
    with connected():
        queue.reconcile_cleanup(key, generation, 1)
        assert queue.ledger().held() == {'cpu': 1, 'mem_gb': 2}
        queue.reap_stale()
    failed = pool.record_store.read_document(queue.records.item_path(pool.FAILED, key))
    assert failed['cleanup_obligation']['state'] == 'resolved'
    assert queue.ledger().held() == {}


def test_previously_released_scope_can_be_verified(refused, monkeypatch):
    queue, item, connected = refused
    path = _terminal_without_proof(queue, item, monkeypatch)
    record = pool.record_store.read_document(path)
    with connected() as (authority, kernel):
        intent = record['resource_scope_intent']
        request = {k: intent[k] for k in ('action_key', 'nonce', 'memory_max_bytes')}
        created = authority.handle(os.getuid(), os.getpid(), {**request, 'op': 'create'})
        for operation in ('stop', 'release'):
            authority.handle(os.getuid(), os.getpid(), {**request, 'op': operation, 'token': created['token']})
        queue.reconcile_cleanup(item['action_key'], queue.records.attempt_generation(record), 1)
        assert kernel.groups == {}
    after = pool.record_store.read_document(path)
    assert after['resource_scope_cleanup']['released']['creation_cancelled'] is False
    assert after['resource_scope_cleanup']['complete'] is True


def test_planted_recovery_record_cannot_supply_cleanup_authority(refused, monkeypatch):
    from prismabuild import cleanup_obligation as debt
    queue, item, connected = refused
    path = _terminal_without_proof(queue, item, monkeypatch)
    record = pool.record_store.read_document(path)
    generation = queue.records.attempt_generation(record)
    supplement = queue.records.attempt_path(record, 1).with_suffix('.cleanup-recovery.json')
    queue.records.file_record_if_absent_at(supplement, {
        'schema': debt.RECOVERY_SCHEMA, 'identity': record['cleanup_obligation'],
        'checked_host': socket.gethostname(), 'resource_scope_cleanup': {'complete': True},
        'container_cleanup': {'complete': True}})
    before = path.read_bytes()
    with connected():
        with pytest.raises(ValueError, match='supplement'):
            queue.reconcile_cleanup(item['action_key'], generation, 1)
    assert path.read_bytes() == before



def test_crash_after_absence_fence_is_recovered_without_losing_capacity_proof(refused):
    from prismabuild import cleanup_obligation as debt
    queue, item, connected = refused
    key = item['action_key']
    with connected():
        debt._verify_cleanup_identity(queue, key, queue.records.attempt_generation(item), 1)
        # Simulated death before consuming the permit: only the broker fence
        # and the original pending claim survive, then a fresh owner retries.
        assert queue.ledger().held() == {'cpu': 1, 'mem_gb': 2}
        queue.reap_stale()
    failed = pool.record_store.read_document(queue.records.item_path(pool.FAILED, key))
    assert failed['resource_scope_cleanup']['complete'] is True
    assert queue.ledger().held() == {}


@pytest.mark.parametrize('fault', ['claim', 'lease'])
def test_legacy_claim_cannot_redirect_cleanup_to_another_action(refused, fault):
    queue, item, connected = refused
    key = item['action_key']
    path = queue.records.item_path(pool.CLAIMED, key)
    record = pool.record_store.read_document(path)
    record.pop('cleanup_obligation')
    other = ('0' if key[0] != '0' else '1') + key[1:]
    if fault == 'claim':
        record['action_key'] = other
    else:
        lease = pool.record_store.read_document(queue.records.lease_path(key))
        queue.records.replace_record_at(queue.records.lease_path(key), {**lease, 'action_key': other})
    queue.records.replace_record_at(path, record)
    before = path.read_bytes()
    with connected():
        with pytest.raises(ValueError):
            queue.reconcile_cleanup(key, queue.records.attempt_generation(record), 1)
    assert path.read_bytes() == before
    assert queue.ledger().held() == {'cpu': 1, 'mem_gb': 2}


def test_recovery_replays_after_summary_write_failure_advances_retry_metadata(refused, monkeypatch):
    queue, item, connected = refused
    key = item['action_key']
    replace = queue.records.replace_record_at
    interrupted = []
    def fail_summary(path, record):
        if (record.get('cleanup_obligation') or {}).get('state') == 'resolved' and not interrupted:
            interrupted.append(True)
            raise OSError('injected failure after immutable cleanup proof')
        return replace(path, record)
    with connected():
        with monkeypatch.context() as patch:
            patch.setattr(queue.records, 'replace_record_at', fail_summary)
            queue.reap_stale()
        assert interrupted == [True]
        assert queue.ledger().held() == {'cpu': 1, 'mem_gb': 2}
        queue.reap_stale()
    failed = pool.record_store.read_document(queue.records.item_path(pool.FAILED, key))
    assert failed is not None, 'cleanup did not recover after pending metadata advanced'
    assert failed['resource_scope_cleanup']['complete'] is True
    assert queue.ledger().held() == {}


def test_withdrawn_refused_create_resolves_cleanup_after_broker_returns(refused):
    queue, item, connected = refused
    key = item['action_key']
    claim_path = queue.records.item_path(pool.CLAIMED, key)
    withdrawal_path = queue.records.item_path(pool.WITHDRAWN, key)
    before = pool.record_store.read_document(claim_path)
    nonce = before['resource_scope_intent']['nonce']

    # The public operation persists its generation-scoped decision without
    # taking cleanup ownership from the claiming worker.
    decision = queue.withdraw(key, reason='review regression', by='review', signal_child=False)
    assert decision['status'] == 'withdrawn'
    assert decision['container_cleanup']['deferred'] is True
    assert pool.record_store.read_document(withdrawal_path)['status'] == 'withdrawn'
    decision_path = queue.records.withdrawal_decision_path(before)
    decision_bytes = decision_path.read_bytes()
    assert queue.ledger().held() == {'cpu': 1, 'mem_gb': 2}

    with connected() as (authority, kernel):
        queue.reap_stale()
        with pytest.raises(ValueError, match='released|budget'):
            authority.handle(os.getuid(), os.getpid(), {
                'op': 'create', 'action_key': key, 'nonce': nonce,
                'memory_max_bytes': 2 * 1024**3})
        assert kernel.groups == {}

    remaining = pool.record_store.read_document(claim_path)
    assert remaining is None, (
        'restored broker could not conclude withdrawn refused-create claim: '
        + repr((remaining or {}).get('container_cleanup_pending'))
    )
    withdrawn = pool.record_store.read_document(withdrawal_path)
    assert withdrawn['status'] == 'withdrawn'
    assert withdrawn['withdrawn_attempt']['resource_scope_cleanup']['complete'] is True
    assert withdrawn['withdrawn_attempt']['resource_scope_cleanup']['nonce'] == nonce
    assert withdrawn['withdrawn_attempt']['container_cleanup']['complete'] is True
    assert withdrawn['withdrawn_attempt']['cleanup_obligation']['state'] == 'resolved'
    assert decision_path.read_bytes() == decision_bytes
    assert not queue.records.lease_path(key).exists()
    assert queue.ledger().held() == {}


@pytest.mark.parametrize('cancel,publication_offset', [
    pytest.param('neither', 100, id='neither'),
    pytest.param('original', 100, id='original'),
    pytest.param('both', 100, id='both'),
    pytest.param('neither', -100, id='rollback'),
    pytest.param('original', -100, id='rollback-cancelled'),
    pytest.param('both', -100, id='rollback-both')])
def test_refused_create_preserves_successor_during_cleanup(refused, monkeypatch, cancel, publication_offset):
    queue, item, connected = refused
    key = item['action_key']
    claim_path = queue.records.item_path(pool.CLAIMED, key)
    original = pool.record_store.read_document(claim_path)
    if cancel != 'neither':
        queue.withdraw(key, signal_child=False)
    monkeypatch.setattr(pool, '_now', lambda: item['published_unix'] + publication_offset)
    queue.publish(action_key=key, cas_root=item['cas_root'],
                  checkout_root=item['checkout_root'], worker_script=item['worker_script'],
                  resources=item['resources'], max_attempts=1, retry_safe=True)
    successor_path = queue.records.item_path(pool.READY, key)
    if cancel == 'both':
        queue.withdraw(key, signal_child=False)
        successor_path = queue.records.item_path(pool.WITHDRAWN, key)
    successor = successor_path.read_bytes()
    with connected():
        queue.reap_stale()
    assert not claim_path.exists(), 'restored broker did not conclude the old claim'
    assert not queue.records.lease_path(key).exists()
    assert successor_path.read_bytes() == successor
    if cancel == 'neither':
        ending_path = queue.records.item_path(pool.FAILED, key)
        retained = pool.record_store.read_document(ending_path)
        assert retained['status'] == 'failed'
    else:
        ending_path = queue.records.withdrawn_finish_path(key, original)
        retained = pool.record_store.read_document(ending_path)['withdrawn_attempt']
        assert retained['status'] == 'withdrawn'
        assert queue.withdrawal_covers(original) is not None
    assert retained['resource_scope_cleanup']['nonce'] == original['resource_scope_intent']['nonce']
    assert retained['cleanup_obligation']['state'] == 'resolved'
    assert queue.ledger().held() == {}


@pytest.mark.parametrize('fault', ['ready-generation', 'ready-key', 'ready-schema',
    'withdrawn-key', 'withdrawn-status', 'withdrawn-generation', 'done', 'failed'])
def test_cleanup_refuses_contradictory_companion_records(refused, monkeypatch, fault):
    queue, item, connected = refused
    key = item['action_key']
    claim_path = queue.records.item_path(pool.CLAIMED, key)
    original = pool.record_store.read_document(claim_path)
    if fault.startswith('withdrawn'):
        queue.withdraw(key, signal_child=False)
        state = pool.WITHDRAWN
        companion = pool.record_store.read_document(queue.records.item_path(state, key))
    else:
        state = {'done': pool.DONE, 'failed': pool.FAILED}.get(fault, pool.READY)
        companion = {**item, 'published_unix': item['published_unix'] + 100}
    if fault.endswith('-key'):
        companion['action_key'] = 'f' * 64 if key != 'f' * 64 else 'e' * 64
    elif fault == 'ready-generation':
        companion['published_unix'] = item['published_unix']
    elif fault == 'ready-schema':
        companion['schema'] = 'invalid'
    elif fault == 'withdrawn-status':
        companion['status'] = 'executed'
    elif fault == 'withdrawn-generation':
        companion['published_unix'] = None
    companion_path = queue.records.item_path(state, key)
    queue.records.replace_record_at(companion_path, companion)
    claim_bytes, lease_bytes = claim_path.read_bytes(), queue.records.lease_path(key).read_bytes()
    companion_bytes = companion_path.read_bytes()
    with connected() as (authority, kernel):
        with pytest.raises(ValueError):
            queue.reconcile_cleanup(key, queue.records.attempt_generation(original), 1)
        assert authority.records == {}
        assert kernel.groups == {}
    assert claim_path.read_bytes() == claim_bytes
    assert queue.records.lease_path(key).read_bytes() == lease_bytes
    assert companion_path.read_bytes() == companion_bytes
    assert not queue.records.attempt_path(original, 1).with_suffix('.cleanup-recovery.json').exists()
    assert queue.ledger().held() == {'cpu': 1, 'mem_gb': 2}

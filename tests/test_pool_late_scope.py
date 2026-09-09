"""A replaced finisher releases only its own scope, including after a crash."""
import json

import pytest
from prismabuild import adaptive_cpu, pool, resource_scope
from test_pool_resource_scope import scoped  # noqa: F401


def _replaced_scope(scoped, monkeypatch):
    queue, first, calls = scoped
    key = first['action_key']
    queue._start_resource_scope(first)
    # Reproduce an old runtime/operator handoff that discarded the claim
    # without releasing the predecessor's separately owned broker scope.
    queue.item_path(pool.CLAIMED, key).unlink()
    queue.lease_path(key).unlink()
    queue.ledger().release(key)
    queue.publish(action_key=key, cas_root=first['cas_root'],
                  checkout_root=first['checkout_root'], worker_script=first['worker_script'],
                  resources=first['resources'], max_attempts=2, retry_safe=True)
    newer = queue.claim(owner='newer-attempt', capacity=first['resources'])
    assert newer is not None
    queue._start_resource_scope(newer)
    assert not pool._same_claim(first, newer)
    assert first['resource_scope']['nonce'] != newer['resource_scope']['nonce']
    observed = []
    request = resource_scope.ResourceScope._request

    def track(scope, op, **extra):
        observed.append((scope.nonce, op))
        return request(scope, op, **extra)

    monkeypatch.setattr(resource_scope.ResourceScope, '_request', track)

    action_cleanup = pool.PoolQueue._cleanup_action_containers

    def no_action_cleanup(self, record):
        if (record.get('resource_scope') or {}).get('nonce') == first['resource_scope']['nonce']:
            pytest.fail('late finisher reached action-wide Docker owner cleanup')
        return action_cleanup(self, record)

    monkeypatch.setattr(pool.PoolQueue, '_cleanup_action_containers', no_action_cleanup)
    # These are the newer attempt's live diagnostic files too. Cleanup of a
    # predecessor must not replace them with its old nonce or stop reason.
    telemetry = queue.ledger().base / 'telemetry' / (key + '.json')
    authority = adaptive_cpu.local_telemetry_path(queue.ledger().base, key)
    for path in (telemetry, authority):
        resource_scope._atomic_json(path, {'nonce': newer['resource_scope']['nonce']})
    resource_scope._atomic_json(telemetry.with_suffix('.termination.json'),
                                {'scope_unit': newer['resource_scope']['scope_id']})
    saved = {path: path.read_bytes() for path in (
        queue.item_path(pool.CLAIMED, key), queue.lease_path(key),
        telemetry, telemetry.with_suffix('.termination.json'), authority,
    )}
    return queue, first, newer, observed, saved


def _assert_successor_untouched(queue, newer, observed, saved):
    assert all(path.read_bytes() == data for path, data in saved.items())
    assert queue.ledger().held() == newer['resources']
    assert not any(nonce == newer['resource_scope']['nonce'] for nonce, _ in observed)
    key = newer['action_key']
    assert not queue.item_path(pool.DONE, key).exists()
    assert not queue.item_path(pool.FAILED, key).exists()
    assert not queue.item_path(pool.READY, key).exists()


def test_late_finisher_releases_its_scope_without_touching_the_live_attempt(scoped, monkeypatch):
    queue, first, newer, observed, saved = _replaced_scope(scoped, monkeypatch)
    result = queue.finish(first['action_key'], status='executed',
                          detail={'returncode': 0}, claim_snapshot=first)
    old = first['resource_scope']['nonce']
    assert (old, 'stop') in observed
    assert (old, 'release') in observed
    assert observed.index((old, 'stop')) < observed.index((old, 'release'))
    _assert_successor_untouched(queue, newer, observed, saved)
    archived = json.loads(result.read_text())
    assert archived['status'] == 'executed'
    cleanup = archived['detail']['resource_scope_cleanup']
    assert cleanup['complete'] is True and cleanup['nonce'] == old
    assert not list(queue.dir(pool.CLAIMED).glob('*' + pool.LATE_FINISH_SUFFIX))


@pytest.mark.parametrize('operation', ['stop', 'release'])
def test_late_scope_refusal_retains_authority_and_reaper_retries_after_finisher_exit(
    scoped, monkeypatch, operation,
):
    queue, first, newer, observed, saved = _replaced_scope(scoped, monkeypatch)
    request = resource_scope.ResourceScope._request

    def unavailable(scope, op, **extra):
        if op == operation:
            raise OSError('broker operation unavailable')
        return request(scope, op, **extra)

    monkeypatch.setattr(resource_scope.ResourceScope, '_request', unavailable)
    pending_path = queue.finish(first['action_key'], status='executed',
                                detail={'returncode': 0, 'stdout': 'finished payload'},
                                claim_snapshot=first)
    pending = json.loads(pending_path.read_text())
    assert pending['resource_scope'] == first['resource_scope']
    assert pending['finish_pending']['status'] == 'executed'
    assert pending['container_cleanup_pending']['complete'] is False
    assert 'broker operation unavailable' in pending['container_cleanup_pending']['error']
    assert pending['container_cleanup_attempts'] == 1
    assert not queue.attempt_path(first, 1).exists()
    _assert_successor_untouched(queue, newer, observed, saved)

    # A new queue instance represents the surviving worker's later sweep;
    # there is no in-memory continuation of the old finisher.
    recovered = pool.PoolQueue(queue.root)
    assert recovered.sweep_finish_tombstones(grace_s=-1) == []
    assert json.loads(pending_path.read_text())['container_cleanup_attempts'] == 2
    monkeypatch.setattr(resource_scope.ResourceScope, '_request', request)
    assert recovered.sweep_finish_tombstones(grace_s=-1) == [first['action_key']]
    assert not pending_path.exists()
    result = json.loads(queue.attempt_path(first, 1).read_text())
    assert result['status'] == 'executed' and result['detail']['returncode'] == 0
    assert result['detail']['resource_scope_cleanup']['complete'] is True
    _assert_successor_untouched(queue, newer, observed, saved)


def test_conflicting_live_nonce_refuses_cleanup_before_any_broker_operation(scoped, monkeypatch):
    queue, first, newer, observed, saved = _replaced_scope(scoped, monkeypatch)
    first['resource_scope'] = dict(newer['resource_scope'])
    path = queue.finish(first['action_key'], status='executed',
                        detail={'returncode': 0}, claim_snapshot=first)
    pending = json.loads(path.read_text())
    assert pending['container_cleanup_pending']['complete'] is False
    assert 'also belongs to the live claim' in pending['container_cleanup_pending']['error']
    assert observed == []
    _assert_successor_untouched(queue, newer, observed, saved)


def test_cleanup_preserves_an_existing_immutable_attempt_and_retains_its_proof(scoped, monkeypatch):
    queue, first, newer, observed, saved = _replaced_scope(scoped, monkeypatch)
    queue.archive_attempt(first, attempt=1, status='lease_lost',
                          disposition='requeued', detail={'reason': 'old reaper verdict'})
    path = queue.attempt_path(first, 1)
    original = path.read_bytes()
    assert queue.finish(first['action_key'], status='executed',
                        detail={'returncode': 0}, claim_snapshot=first) == path
    assert path.read_bytes() == original
    evidence = list(queue.superseded_dir().glob('*.late-finish.json'))
    assert len(evidence) == 1
    cleanup = json.loads(evidence[0].read_text())['resource_scope_cleanup']
    assert cleanup['complete'] and cleanup['nonce'] == first['resource_scope']['nonce']
    _assert_successor_untouched(queue, newer, observed, saved)


def test_crash_before_stop_leaves_a_record_only_the_owner_host_can_recover(scoped, monkeypatch):
    queue, first, newer, observed, saved = _replaced_scope(scoped, monkeypatch)

    class ProcessDied(BaseException):
        pass

    def crash(scope, reason):
        raise ProcessDied()

    with monkeypatch.context() as patch:
        patch.setattr(resource_scope.ResourceScope, 'terminate_owned', crash)
        with pytest.raises(ProcessDied):
            queue.finish(first['action_key'], status='executed',
                         detail={'returncode': 0}, claim_snapshot=first)
    paths = list(queue.dir(pool.CLAIMED).glob('*' + pool.LATE_FINISH_SUFFIX))
    assert len(paths) == 1
    before = paths[0].read_bytes()
    with monkeypatch.context() as patch:
        patch.setattr(pool.socket, 'gethostname', lambda: 'foreign-worker')
        assert pool.PoolQueue(queue.root).sweep_finish_tombstones(grace_s=-1) == []
    assert paths[0].read_bytes() == before and observed == []
    assert pool.PoolQueue(queue.root).sweep_finish_tombstones() == [first['action_key']]
    assert not paths[0].exists()
    _assert_successor_untouched(queue, newer, observed, saved)


def test_pending_late_cleanup_blocks_a_new_claim_without_changing_its_ready_bytes(scoped, monkeypatch):
    queue, first, newer, observed, saved = _replaced_scope(scoped, monkeypatch)
    request = resource_scope.ResourceScope._request

    def refuse_old(scope, op, **extra):
        if scope.nonce == first['resource_scope']['nonce'] and op == 'release':
            raise OSError('old scope still populated')
        return request(scope, op, **extra)

    monkeypatch.setattr(resource_scope.ResourceScope, '_request', refuse_old)
    pending = queue.finish(first['action_key'], status='executed',
                           detail={'returncode': 0}, claim_snapshot=first)
    queue.finish(newer['action_key'], status='failed', detail={'returncode': 1},
                 claim_snapshot=newer)
    ready_path = queue.item_path(pool.READY, newer['action_key'])
    ready = ready_path.read_bytes()
    assert queue.claim(owner='third-attempt', capacity=newer['resources']) is None
    assert ready_path.read_bytes() == ready and pending.exists()
    monkeypatch.setattr(resource_scope.ResourceScope, '_request', request)
    assert queue.sweep_finish_tombstones() == [first['action_key']]
    assert ready_path.read_bytes() == ready
    assert queue.claim(owner='third-attempt', capacity=newer['resources']) is not None


def test_late_finish_reconciles_an_interrupted_creation_by_exact_nonce(scoped, monkeypatch):
    queue, first, newer, observed, saved = _replaced_scope(scoped, monkeypatch)
    control = first.pop('resource_scope')
    request = resource_scope.ResourceScope._request

    def recover(scope, op, **extra):
        if op == 'recover_create':
            observed.append((scope.nonce, op))
            assert scope.nonce == control['nonce']
            return {'ok': True, **control}
        return request(scope, op, **extra)

    monkeypatch.setattr(resource_scope.ResourceScope, '_request', recover)
    path = queue.finish(first['action_key'], status='failed',
                        detail={'returncode': 1}, claim_snapshot=first)
    assert (control['nonce'], 'recover_create') in observed
    assert (control['nonce'], 'release') in observed
    assert not any(op == 'create' for _, op in observed)
    cleanup = json.loads(path.read_text())['detail']['resource_scope_cleanup']
    assert cleanup['complete'] is True and cleanup['nonce'] == control['nonce']
    _assert_successor_untouched(queue, newer, observed, saved)


def test_repeated_late_finish_uses_pending_authority_after_successor_concludes(scoped, monkeypatch):
    queue, first, newer, observed, saved = _replaced_scope(scoped, monkeypatch)
    request = resource_scope.ResourceScope._request

    def refuse_old(scope, op, **extra):
        if scope.nonce == first['resource_scope']['nonce'] and op == 'release':
            raise OSError('old scope still populated')
        return request(scope, op, **extra)

    monkeypatch.setattr(resource_scope.ResourceScope, '_request', refuse_old)
    pending = queue.finish(first['action_key'], status='executed',
                           detail={'returncode': 0}, claim_snapshot=first)
    terminal = queue.finish(newer['action_key'], status='executed',
                            detail={'returncode': 0}, claim_snapshot=newer)
    original = terminal.read_bytes()
    monkeypatch.setattr(resource_scope.ResourceScope, '_request', request)
    path = queue.finish(first['action_key'], status='executed',
                        detail={'returncode': 0}, claim_snapshot=first)
    assert path == queue.attempt_path(first, 1) and not pending.exists()
    assert terminal.read_bytes() == original
    assert json.loads(path.read_text())['detail']['resource_scope_cleanup']['complete']


@pytest.mark.parametrize("status", ["executed", "failed"])
@pytest.mark.parametrize("reused_owner", [False, True])
def test_same_host_stale_claim_cannot_clean_up_or_replace_successor(
    scoped, monkeypatch, status, reused_owner,
):
    queue, first, newer, observed, saved = _replaced_scope(scoped, monkeypatch)
    key = first["action_key"]
    if reused_owner:
        # A long-lived worker can own successive attempts. The lease's claim
        # timestamp must distinguish them even when owner and host agree.
        first["claimed_by"] = newer["claimed_by"]
        first["published_unix"] = newer["published_unix"]
    read = pool._read_json
    claim_path = queue.item_path(pool.CLAIMED, key)

    def stale_claim(path):
        return dict(first) if path == claim_path else read(path)

    with monkeypatch.context() as patch:
        patch.setattr(pool, "_read_json", stale_claim)
        with pytest.raises(pool.AmbiguousClaimHolder, match="lease"):
            queue.finish(key, status=status, detail={"returncode": 0 if status == "executed" else 7},
                         claim_snapshot=first)
    _assert_successor_untouched(queue, newer, observed, saved)
    assert observed == [], "contradictory identity must refuse before broker cleanup"


@pytest.mark.parametrize("reused_owner", [False, True])
@pytest.mark.parametrize("with_snapshot", [False, True])
def test_stale_heartbeat_cannot_erase_successor_lease_evidence(
    scoped, monkeypatch, reused_owner, with_snapshot,
):
    queue, first, newer, observed, saved = _replaced_scope(scoped, monkeypatch)
    key = first["action_key"]
    if reused_owner:
        first["claimed_by"] = newer["claimed_by"]
        first["published_unix"] = newer["published_unix"]
    read = pool._read_json
    claim_path = queue.item_path(pool.CLAIMED, key)

    def stale_claim(path):
        return dict(first) if path == claim_path else read(path)

    with monkeypatch.context() as patch:
        patch.setattr(pool, "_read_json", stale_claim)
        with pytest.raises(pool.AmbiguousClaimHolder, match="lease"):
            queue.write_lease(
                key, owner=first["claimed_by"], child_pid=123456,
                claim_snapshot=first if with_snapshot else None,
                execution_observation={"last_progress_unix": 1.0},
            )
        # The fresh lease remains available to the stale finisher's guard.
        with pytest.raises(pool.AmbiguousClaimHolder, match="lease"):
            queue.finish(key, status="executed", detail={"returncode": 0},
                         claim_snapshot=first)
    _assert_successor_untouched(queue, newer, observed, saved)
    assert observed == []

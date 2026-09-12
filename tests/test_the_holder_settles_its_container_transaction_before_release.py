"""The holder tells the broker its Docker transaction closed, before release.

`release` is where the broker chooses between removing an attempt's scope and
retaining an empty frozen parent for a container that might still arrive, so
settlement has to reach it first or it decides without the one fact that makes
removal safe.

Settlement is housekeeping for a payload that has already stopped, and it is
wired as such: a broker that refuses it, or that predates the operation
entirely, leaves exactly the behaviour that was here before -- the tombstone
retained -- and must never cost the claim.  The surrounding handler answers
``complete: False`` to anything that escapes it, which retains the claim and
its tokens; that is right for a cleanup that could not prove the payload
stopped, and wrong for a memory charge nobody reclaimed.
"""
import hashlib
import json
import socket

import pytest

from prismabuild import pool, resource_scope

KEY = 'd' * 64
NONCE = 'e' * 32
OWNER = 'f' * 64
UNIT = 'prismabuild-job' + hashlib.sha256((KEY + NONCE).encode()).hexdigest()[:32] + '.slice'


@pytest.fixture
def holder(tmp_path, monkeypatch):
    """A claimed action with a scope and a Docker owner, ready to be concluded."""
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.publish(action_key=KEY, cas_root=tmp_path / 'cas',
                  checkout_root=tmp_path / 'checkout', worker_script=tmp_path / 'worker.py',
                  resources={'cpu': 1}, container_owner=OWNER)
    record = queue.claim(capacity={'cpu': 1})
    record['resource_scope'] = {
        'action_key': KEY, 'nonce': NONCE, 'scope_id': UNIT,
        'cgroup_path': '/sys/fs/cgroup/prismabuild.slice/' + UNIT,
        'token': 'a' * 64, 'socket_path': str(resource_scope.BROKER_SOCKET),
        'memory_max_bytes': 1024**3, 'create_recovered': True}
    record['claimed_host'] = socket.gethostname()
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, KEY), record)

    marker = queue.container_marker(OWNER)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(OWNER + '\n')

    calls = []

    def request(scope, op, **extra):
        calls.append((op, extra))
        return {'ok': True}

    monkeypatch.setattr(resource_scope.ResourceScope, '_request', request)
    monkeypatch.setattr(resource_scope.ResourceScope, 'sample',
                        lambda scope: {'complete': True})
    monkeypatch.setattr(pool.cpu_admission, 'record_completion', lambda *args: None)
    monkeypatch.setattr(pool, '_docker_remove_containers', lambda ids: list(ids))
    queries = []

    def labelled(label, value):
        queries.append((label, value))
        return []

    monkeypatch.setattr(pool, '_docker_containers_with_label', labelled)
    monkeypatch.setattr(pool, '_docker_owned_container_ids',
                        lambda owner: labelled(pool.CONTAINER_OWNER_LABEL, owner))
    return queue, record, calls, queries, marker


def _ops(calls):
    return [op for op, _ in calls]


def test_settlement_reaches_the_broker_between_the_stop_and_the_release(holder):
    queue, record, calls, queries, marker = holder
    outcome = queue.cleanup_action_containers(record)
    assert outcome['complete'] is True
    assert 'settle_error' not in outcome['resource_scope']
    ops = _ops(calls)
    assert ops.index('stop') < ops.index('settle') < ops.index('release'), (
        'release is what decides between removing the scope and retaining a '
        'frozen parent, so it must already hold the settlement')
    evidence = dict(calls[ops.index('settle')][1])['evidence']
    assert evidence == {
        'schema': resource_scope.CONTAINER_SETTLEMENT_SCHEMA,
        'marker_absent': True, 'owner_container_ids': [], 'scope_container_ids': [],
        'checked_unix': evidence['checked_unix']}
    assert isinstance(evidence['checked_unix'], float)


def test_settlement_asks_the_daemon_about_this_slice_and_not_only_this_action(holder):
    """The owner label spans an action's attempts; the scope label is this one.

    A scope is being removed, so the question the evidence has to answer is
    about that slice.  ``prismabuild.scope`` is a reserved label the shim
    refuses to let a caller set, so asking for it is an identity match rather
    than a name match.
    """
    queue, record, calls, queries, marker = holder
    queue.cleanup_action_containers(record)
    assert (pool.CONTAINER_SCOPE_LABEL, UNIT) in queries
    assert (pool.CONTAINER_OWNER_LABEL, OWNER) in queries


def test_a_settlement_the_broker_refuses_still_concludes_the_claim(holder):
    """An old broker answers `unknown operation`; that must not cost the action."""
    queue, record, calls, queries, marker = holder
    original = resource_scope.ResourceScope._request

    def refuse(scope, op, **extra):
        if op == 'settle':
            raise OSError('resource broker refused request: unknown operation')
        return original(scope, op, **extra)

    resource_scope.ResourceScope._request = refuse
    try:
        outcome = queue.cleanup_action_containers(record)
    finally:
        resource_scope.ResourceScope._request = original
    assert outcome['complete'] is True
    assert 'release' in _ops(calls)
    assert 'unknown operation' in outcome['resource_scope']['settle_error']


def test_an_unprovable_container_cleanup_never_settles(holder, monkeypatch):
    """No settlement without the proof it is made of."""
    queue, record, calls, queries, marker = holder
    monkeypatch.setattr(pool, '_docker_owned_container_ids', lambda owner: ['9c1f0e'])
    monkeypatch.setattr(pool, '_docker_remove_containers', lambda ids: [])
    outcome = queue.cleanup_action_containers(record)
    assert outcome['complete'] is False
    assert 'settle' not in _ops(calls) and 'release' not in _ops(calls)
    assert marker.exists()


def test_a_late_finisher_settles_nothing_it_does_not_own(holder):
    """`scope_only` owns a broker nonce, never the action-wide Docker owner."""
    queue, record, calls, queries, marker = holder
    live = dict(record)
    live['resource_scope'] = {**record['resource_scope'], 'nonce': 'c' * 32}
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, KEY), live)
    outcome = queue.cleanup_action_containers(record, scope_only=True)
    assert outcome['complete'] is True
    assert 'settle' not in _ops(calls) and queries == []
    assert marker.exists(), 'the successor still owns this marker'


def test_an_action_that_never_touched_docker_settles_nothing(tmp_path, monkeypatch):
    """With no owner there is no shim, no ticket, and nothing to retain."""
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.publish(action_key=KEY, cas_root=tmp_path / 'cas',
                  checkout_root=tmp_path / 'checkout', worker_script=tmp_path / 'worker.py',
                  resources={'cpu': 1})
    record = queue.claim(capacity={'cpu': 1})
    record['resource_scope'] = {
        'action_key': KEY, 'nonce': NONCE, 'scope_id': UNIT,
        'cgroup_path': '/sys/fs/cgroup/prismabuild.slice/' + UNIT,
        'token': 'a' * 64, 'socket_path': str(resource_scope.BROKER_SOCKET),
        'memory_max_bytes': 1024**3, 'create_recovered': True}
    record['claimed_host'] = socket.gethostname()
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, KEY), record)
    calls = []
    monkeypatch.setattr(resource_scope.ResourceScope, '_request',
                        lambda scope, op, **extra: (calls.append(op), {'ok': True})[1])
    monkeypatch.setattr(resource_scope.ResourceScope, 'sample',
                        lambda scope: {'complete': True})
    monkeypatch.setattr(pool.cpu_admission, 'record_completion', lambda *args: None)
    assert queue.cleanup_action_containers(record)['complete'] is True
    assert 'settle' not in calls and 'release' in calls
    assert json.loads(queue.item_path(pool.CLAIMED, KEY).read_text())[
        'resource_scope_cleanup']['complete'] is True

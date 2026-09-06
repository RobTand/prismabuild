"""Lost broker-create replies retain durable exact-attempt recovery identity."""
import hashlib
import json

import pytest
from prismabuild import pool, resource_scope
from test_pool_resource_scope import scoped


def test_lost_create_reply_is_reconciled_from_durable_intent(scoped, monkeypatch):
    queue, item, calls = scoped
    authority = {}
    def broker(scope, operation, **extra):
        calls.append(operation)
        if operation == 'create':
            unit = 'prismabuild-job' + hashlib.sha256((scope.action_key + scope.nonce).encode()).hexdigest()[:32] + '.slice'
            authority.update(nonce=scope.nonce, budget=scope.memory_max_bytes,
                             reply={'ok': True, 'scope_id': unit, 'token': 'b'*64,
                                    'cgroup_path': '/sys/fs/cgroup/prismabuild.slice/' + unit})
            raise TimeoutError('broker created scope but reply was lost')
        if operation == 'recover_create':
            assert scope.nonce == authority['nonce']
            assert scope.memory_max_bytes == authority['budget']
            return authority['reply']
        if operation == 'release':
            authority.clear()
        return {'ok': True}
    monkeypatch.setattr(resource_scope.ResourceScope, '_request', broker)
    with pytest.raises(TimeoutError, match='reply was lost'):
        queue.execute(item, containment=True)
    durable = json.loads(queue.item_path(pool.CLAIMED, item['action_key']).read_text())
    assert durable['resource_scope_intent']['nonce'] == authority['nonce']
    lease = json.loads(queue.lease_path(item['action_key']).read_text())
    assert lease['resource_scope_intent'] == durable['resource_scope_intent']
    assert queue.cleanup_action_containers(durable, reason='failed')['complete']
    assert calls.count('create') == 1
    assert 'recover_create' in calls and 'stop' in calls and 'release' in calls
    assert not authority


def test_refused_recovery_protocol_requeues_without_creating_or_burning_attempt(scoped,monkeypatch):
    queue,item,calls=scoped
    monkeypatch.setattr(queue,'claim',lambda **kwargs:item)
    def legacy(scope,operation,**extra):
        assert operation=='create' and extra['recovery_protocol']==1
        raise resource_scope.ResourceUnavailable('installed broker lacks recovery protocol')
    monkeypatch.setattr(resource_scope.ResourceScope,'_request',legacy)
    assert queue.serve_once(containment=True) is None
    ready=json.loads(queue.item_path(pool.READY,item['action_key']).read_text())
    assert ready['attempts']==0
    assert 'resource_scope_intent' not in ready and 'resource_scope' not in ready
    assert queue.ledger().held()=={}
    assert not queue.lease_path(item['action_key']).exists()


def test_intent_recovery_failure_retains_claim_and_capacity(scoped,monkeypatch):
    queue,item,calls=scoped
    def unavailable(scope,operation,**extra):raise TimeoutError('broker unavailable')
    monkeypatch.setattr(resource_scope.ResourceScope,'_request',unavailable)
    with pytest.raises(TimeoutError):queue.execute(item,containment=True)
    result=queue.cleanup_action_containers(item,reason='failed')
    assert result['complete'] is False
    assert queue.ledger().held()=={'cpu':1,'mem_gb':2}
    assert queue.item_path(pool.CLAIMED,item['action_key']).exists()


def test_orphan_lease_preserves_unanswered_creation_for_exact_recovery(scoped,monkeypatch):
    queue,item,calls=scoped;seen=[]
    def unavailable(scope,operation,**extra):
        seen.append((operation,scope.nonce))
        if operation=='create':raise TimeoutError('reply lost')
        if operation=='recover_create':
            unit='prismabuild-job'+hashlib.sha256((scope.action_key+scope.nonce).encode()).hexdigest()[:32]+'.slice'
            return {'ok':True,'scope_id':unit,'missing':True}
        return {'ok':True}
    monkeypatch.setattr(resource_scope.ResourceScope,'_request',unavailable)
    with pytest.raises(TimeoutError):queue.execute(item,containment=True)
    queue.item_path(pool.CLAIMED,item['action_key']).unlink()
    assert queue.sweep_widowed_leases(timeout_s=-1)==[item['action_key']]
    assert seen[0][0]=='create' and seen[1]==('recover_create',seen[0][1])
    assert queue.ledger().held()=={}

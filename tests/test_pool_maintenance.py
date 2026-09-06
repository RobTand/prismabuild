"""Maintenance defers only unstarted attempts and preserves later publication."""
import json
from pathlib import Path

import pytest
from prismabuild import pool, resource_scope
from test_worker_loop_reloads_unversioned import _worker_loop

KEY = 'a' * 64


def _publish(queue, **kwargs):
    queue.publish(action_key=KEY, cas_root='/cas', checkout_root='/checkout',
                  worker_script='/worker.py', resources={'cpu': 1, 'mem_gb': 1}, max_attempts=1, **kwargs)


def test_maintenance_after_claim_returns_capacity_without_burning_attempt(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    _publish(queue)
    def execute(*args, **kwargs):
        raise resource_scope.ResourceUnavailable('draining')
    monkeypatch.setattr(queue, 'execute', execute)
    assert queue.serve_once(capacity={'cpu': 1, 'mem_gb': 1}, containment=True) is None
    ready = json.loads(queue.item_path(pool.READY, KEY).read_text())
    assert ready['attempts'] == 0
    assert 'claimed_by' not in ready
    assert queue.ledger().held() == {}
    assert not queue.lease_path(KEY).exists()
    assert not list((queue.root / pool.ATTEMPTS).rglob('*.json'))


def test_maintenance_deferral_never_overwrites_a_later_submission(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    _publish(queue)
    def execute(*args, **kwargs):
        _publish(queue, priority=9)
        raise resource_scope.ResourceUnavailable('draining')
    monkeypatch.setattr(queue, 'execute', execute)
    assert queue.serve_once(capacity={'cpu': 1, 'mem_gb': 1}, containment=True) is None
    ready = json.loads(queue.item_path(pool.READY, KEY).read_text())
    assert ready['priority'] == 9 and ready['attempts'] == 0
    assert queue.ledger().held() == {}


def test_ordinary_broker_failure_is_not_misreported_as_maintenance(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    _publish(queue)
    def execute(*args, **kwargs):
        raise OSError('broker disconnected')
    monkeypatch.setattr(queue, 'execute', execute)
    with pytest.raises(OSError, match='disconnected'):
        queue.serve_once(capacity={'cpu': 1, 'mem_gb': 1}, containment=True)
    assert queue.item_path(pool.FAILED, KEY).exists()


def test_same_commit_new_generation_exits_before_any_queue_access(tmp_path, monkeypatch):
    worker = _worker_loop()
    loaded, active = tmp_path / 'loaded.json', tmp_path / 'active.json'
    loaded.write_text(json.dumps({'commit': 'same', 'generation': 'first'}))
    active.write_text(json.dumps({'commit': 'same', 'generation': 'second'}))
    monkeypatch.setattr(worker, 'GENERATION_VERSION', loaded)
    monkeypatch.setattr(worker, 'RUNTIME_VERSION', active)
    monkeypatch.setattr(worker, 'SH', tmp_path)
    class Untouchable:
        def __getattr__(self, name):
            raise AssertionError('stale generation touched queue: ' + name)
    monkeypatch.setattr(worker.pool, 'PoolQueue', lambda *args: Untouchable())
    monkeypatch.setattr(worker.sys, 'argv', ['worker_loop.py', '--once', '--assume-idle', '--all-cores'])
    assert worker.main() == 0


def test_maintenance_gate_blocks_claims_before_queue_reads(tmp_path, monkeypatch):
    worker = _worker_loop()
    gate = tmp_path / 'maintenance.json'
    gate.write_text(json.dumps({'draining': True}))
    monkeypatch.setattr(worker, 'MAINTENANCE_GATE', gate, raising=False)
    monkeypatch.setattr(worker, 'loaded_runtime_commit', lambda: 'same')
    monkeypatch.setattr(worker, 'published_commit', lambda: 'same')
    class Untouchable:
        def __getattr__(self, name):
            raise AssertionError('maintenance worker touched queue: ' + name)
    monkeypatch.setattr(worker.pool, 'PoolQueue', lambda *args: Untouchable())
    monkeypatch.setattr(worker.sys, 'argv', ['worker_loop.py', '--once', '--assume-idle', '--all-cores'])
    assert worker.main() == 0

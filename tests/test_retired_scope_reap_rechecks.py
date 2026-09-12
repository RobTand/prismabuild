"""Retired scopes retain page-charge retries and reject stale stop targets."""
import errno

import pytest

from test_resource_broker import authority
from test_a_settled_tombstone_is_reaped_and_a_live_scope_is_not import _retired, _pass


@pytest.mark.parametrize('failure', ['partial', 'missing_control'])
def test_incomplete_reclaim_keeps_the_group_available_for_retry(authority, monkeypatch, failure):
    a, b = authority
    _, _, scope = _retired(a, b, monkeypatch)

    def reclaim(name):
        b.ops.append(('reclaim', name))
        if failure == 'missing_control':
            raise FileNotFoundError(errno.ENOENT, 'memory.reclaim unavailable')
        b.charge[name] = 341_000_000
        return {'before': 341_319_680, 'after': 341_000_000, 'complete': False,
                'page_bytes_after': 340_000_000}

    monkeypatch.setattr(b, 'reclaim', reclaim)
    status = _pass(a)
    assert status['health'] is True
    assert scope in b.groups, (failure, b.ops, a.records[scope].get('reclaim_after_bytes'))
    assert not a.records[scope].get('released_unix')

    def completed(name):
        b.ops.append(('reclaim', name))
        before = b.charge[name]
        b.charge[name] = 0
        return {'before': before, 'after': 0, 'complete': True}

    monkeypatch.setattr(b, 'reclaim', completed)
    assert _pass(a)['health'] is True
    assert scope not in b.groups
    assert a.records[scope]['released_unix'] > 0


@pytest.mark.parametrize('replace_identity', [True, False])
def test_population_during_reclaim_is_not_stopped(authority, monkeypatch, replace_identity):
    a, b = authority
    _, _, scope = _retired(a, b, monkeypatch)

    def reclaim(name):
        b.ops.append(('reclaim', name))
        # The operation finishes while an external actor replaces the path.
        # Inventory's original row is now stale; the replacement is populated.
        if replace_identity:
            b.identity[name] = [64, 700_999]
        b.groups[name]['populated'] = True
        b.stopped.clear()
        return {'before': 341_319_680, 'after': 0, 'complete': True}

    monkeypatch.setattr(b, 'reclaim', reclaim)
    _pass(a)
    assert ('stop', scope) not in b.ops, b.ops
    assert b.groups[scope]['populated'] is True


def test_identity_is_still_checked_after_stop(authority, monkeypatch):
    a, b = authority
    _, _, scope = _retired(a, b, monkeypatch)
    stop = b.stop

    def replaced_after_stop(name):
        stop(name)
        b.identity[name] = [64, 701_000]

    monkeypatch.setattr(b, 'stop', replaced_after_stop)
    assert _pass(a)['health'] is False
    assert ('release', scope) not in b.ops
    assert scope in b.groups


def test_partial_reclaim_with_only_kernel_charge_can_be_reaped(authority, monkeypatch):
    a, b = authority
    _, _, scope = _retired(a, b, monkeypatch)

    def kernel_only(name):
        b.ops.append(('reclaim', name))
        b.charge[name] = 90112
        return {'before': 341_319_680, 'after': 90112, 'complete': False,
                'page_bytes_after': 0}

    monkeypatch.setattr(b, 'reclaim', kernel_only)
    assert _pass(a)['health'] is True
    assert scope not in b.groups
    assert a.records[scope]['reclaim_after_bytes'] == 90112
    assert a.records[scope]['reclaim_page_bytes_after'] == 0


def test_backend_reports_kernel_only_partial_reclaim(tmp_path, monkeypatch):
    from test_resource_broker import module
    from test_a_retired_tombstone_gives_its_memory_back_without_being_removed import _cgroup_tree, SCOPE
    broker = module()
    backend = broker.SystemdBackend(root=tmp_path)
    group = _cgroup_tree(tmp_path, SCOPE, current=90112)
    (group / 'memory.stat').write_text('kernel 90112\nfile 0\nanon 0\n')

    def partial(path, value):
        raise OSError(errno.EAGAIN, 'kernel residual')

    monkeypatch.setattr(broker, '_write_control', partial)
    result = backend.reclaim(SCOPE)
    assert result['complete'] is False
    assert result['after'] == 90112 and result['page_bytes_after'] == 0

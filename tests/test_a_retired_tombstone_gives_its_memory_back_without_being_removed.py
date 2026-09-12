"""A retained tombstone must give its charge back, and keep its containment.

`release` keeps an empty frozen parent whenever a container ticket was never
resolved: a killed client cannot prove the daemon RPC completed, so the frozen
group stays as the thing a late container would land in. Nothing ever asked
that group for its memory back, so every withdrawn or killed action that
touched the Docker shim left a residual charge on the host until it rebooted
(#486: 21 such slices on sparklina holding 361,988,096 B).

Reclaiming is not removing, and this file is deliberately only about the first.
The charge goes back with the containment argument fully intact: the group is
still there, still frozen, still owned. Removal needs evidence this commit does
not have, and the group is never removed here.

Order is the part that has to be proved rather than assumed. A memory cgroup
removed while it still holds LRU page cache goes offline as a zombie -- the
kernel reparents kernel memory on offline but not page cache -- so a reclaim
that runs after a removal reclaims nothing. That is why the fake backend logs
its operations in order.
"""
import errno
import json
import os

import pytest

from test_resource_broker import auth, authority, create, container_ticket, module


def _tombstone(a, b, monkeypatch, *, identity=(64, 4242), charge=8_060_928):
    """A retired, empty, frozen scope with a kernel identity the broker knows.

    Built the only way the real one is built: a container ticket that is never
    resolved, a stop, and a release that retires instead of removing.
    """
    request, record = create(a)
    container_ticket(a, record, monkeypatch)
    scope = record['scope_id']
    a.records[scope]['cgroup_identity'] = list(identity)
    b.identity[scope] = list(identity)
    b.charge[scope] = charge
    a.handle(os.getuid(), os.getpid(), auth(request, record, 'stop'))
    assert a.handle(os.getuid(), os.getpid(),
                    auth(request, record, 'release'))['retired'] is True
    b.ops.clear()
    return request, record, scope


def _pass(a):
    return a.handle(0, os.getpid(), {'op': 'maintenance_status'})


def test_a_retired_tombstone_is_reclaimed_and_stays_contained(authority, monkeypatch):
    a, b = authority
    request, record, scope = _tombstone(a, b, monkeypatch)
    status = _pass(a)
    assert status['health'] is True and status['active_scopes'] == 0
    assert b.ops == [('reclaim', scope)], 'reclaim must not stop, remove or adopt anything'
    assert scope in b.groups and b.charge[scope] == 0
    stored = json.loads((a.state_dir / (scope + '.json')).read_text())
    assert stored['reclaim_before_bytes'] == 8_060_928
    assert stored['reclaim_after_bytes'] == 0 and stored['reclaim_complete'] is True
    assert stored['reclaim_error'] is None and stored['reclaimed_unix'] > 0
    assert not stored.get('released_unix'), 'commit one removes nothing'


def test_a_populated_scope_is_never_reclaimed_and_never_touched(authority, monkeypatch):
    """The rule that protects a live campaign job, asserted at the backend.

    A populated group must not reach any backend call at all -- not reclaim,
    not stop, not release. Asserting only "it was not removed" would pass even
    if the pass had frozen and killed it first, because this fake's ``stop``
    clears ``populated`` and would hide exactly that.
    """
    a, b = authority
    request, record, scope = _tombstone(a, b, monkeypatch)
    b.groups[scope]['populated'] = True
    status = _pass(a)
    assert status['active_scope_ids'] == [scope]
    assert b.ops == []
    assert b.charge[scope] == 8_060_928 and scope in b.groups
    assert 'reclaim_before_bytes' not in json.loads(
        (a.state_dir / (scope + '.json')).read_text())


def test_an_unfrozen_retired_scope_is_never_reclaimed(authority, monkeypatch):
    """Frozen is half the containment argument; without it there is no tombstone."""
    a, b = authority
    request, record, scope = _tombstone(a, b, monkeypatch)
    b.stopped.clear()
    status = _pass(a)
    assert status['active_scope_ids'] == [scope]
    assert b.ops == [] and b.charge[scope] == 8_060_928


def test_a_changed_kernel_identity_reports_and_reclaims_nothing(authority, monkeypatch):
    """A group this record no longer names is somebody else's; say so, touch nothing."""
    a, b = authority
    request, record, scope = _tombstone(a, b, monkeypatch)
    b.identity[scope] = [64, 9999]
    status = _pass(a)
    assert status['health'] is False
    assert any('identity changed' in error for error in status['errors'])
    assert b.ops == [] and b.charge[scope] == 8_060_928


def test_a_tombstone_without_a_recorded_identity_is_left_exactly_as_it_was(
        authority, monkeypatch):
    """The pre-identity record keeps its old treatment: inactive, and untouched."""
    a, b = authority
    request, record = create(a)
    container_ticket(a, record, monkeypatch)
    scope = record['scope_id']
    b.charge[scope] = 4096
    a.handle(os.getuid(), os.getpid(), auth(request, record, 'stop'))
    a.handle(os.getuid(), os.getpid(), auth(request, record, 'release'))
    b.ops.clear()
    assert _pass(a)['active_scopes'] == 0
    assert b.ops == [] and b.charge[scope] == 4096


def test_a_partial_reclaim_is_progress_rather_than_an_unhealthy_pass(
        authority, monkeypatch):
    """EAGAIN means the kernel dropped some of it, which is the good outcome."""
    a, b = authority
    request, record, scope = _tombstone(a, b, monkeypatch)

    def partial(name):
        b.ops.append(('reclaim', name))
        b.charge[name] = 2048
        return {'before': 8_060_928, 'after': 2048, 'complete': False}

    monkeypatch.setattr(b, 'reclaim', partial)
    status = _pass(a)
    assert status['health'] is True and status['errors'] == []
    assert status['active_scopes'] == 0
    stored = json.loads((a.state_dir / (scope + '.json')).read_text())
    assert stored['reclaim_after_bytes'] == 2048 and stored['reclaim_complete'] is False
    assert stored['reclaim_error'] is None


def test_a_failed_reclaim_is_recorded_and_never_closes_the_maintenance_gate(
        authority, monkeypatch):
    """A kernel with no ``memory.reclaim`` must not be able to stop an upgrade."""
    a, b = authority
    request, record, scope = _tombstone(a, b, monkeypatch)

    def missing(name):
        raise FileNotFoundError(errno.ENOENT, 'no such file', 'memory.reclaim')

    monkeypatch.setattr(b, 'reclaim', missing)
    status = _pass(a)
    assert status['health'] is True and status['errors'] == []
    stored = json.loads((a.state_dir / (scope + '.json')).read_text())
    assert 'memory.reclaim' in stored['reclaim_error']
    assert stored['reclaim_before_bytes'] is None
    assert a.handle(0, os.getpid(), {'op': 'maintenance_begin'})['health'] is True


def test_an_unchanged_observation_does_not_rewrite_the_state_file(
        authority, monkeypatch):
    """A tombstone with nothing left to give back is quiet, not a write loop."""
    a, b = authority
    request, record, scope = _tombstone(a, b, monkeypatch, charge=0)
    path = a.state_dir / (scope + '.json')
    _pass(a)
    first = path.stat().st_mtime_ns
    stamp = json.loads(path.read_text())['reclaimed_unix']
    for _ in range(3):
        _pass(a)
    assert path.stat().st_mtime_ns == first
    assert json.loads(path.read_text())['reclaimed_unix'] == stamp
    assert b.ops == [('reclaim', scope)] * 4, 'it still asks; it just stops rewriting'


def _cgroup_tree(root, scope, *, current=829_808_640, reclaim_file=True):
    group = root / 'prismabuild.slice' / scope
    group.mkdir(parents=True)
    (group / 'memory.current').write_text(f'{current}\n')
    if reclaim_file:
        (group / 'memory.reclaim').write_text('')
    return group


SCOPE = 'prismabuild-job' + 'a' * 32 + '.slice'


def test_the_backend_asks_the_kernel_for_the_exact_charge_it_read(tmp_path):
    """Against a fake cgroup tree: the request is the group's own ``memory.current``."""
    backend = module().SystemdBackend(root=tmp_path)
    group = _cgroup_tree(tmp_path, SCOPE)
    result = backend.reclaim(SCOPE)
    assert (group / 'memory.reclaim').read_text() == '829808640'
    assert result == {'before': 829_808_640, 'after': 829_808_640, 'complete': True}


def test_the_backend_asks_for_nothing_when_there_is_nothing_charged(tmp_path):
    backend = module().SystemdBackend(root=tmp_path)
    group = _cgroup_tree(tmp_path, SCOPE, current=0)
    assert backend.reclaim(SCOPE) == {'before': 0, 'after': 0, 'complete': True}
    assert (group / 'memory.reclaim').read_text() == ''


@pytest.mark.parametrize('code', [errno.EAGAIN, errno.EBUSY])
def test_the_backend_reads_a_partial_reclaim_as_progress(tmp_path, monkeypatch, code):
    broker = module()
    backend = broker.SystemdBackend(root=tmp_path)
    group = _cgroup_tree(tmp_path, SCOPE)

    def refuse(path, value):
        raise OSError(code, 'partial reclaim')

    monkeypatch.setattr(broker, '_write_control', refuse)
    assert backend.reclaim(SCOPE)['complete'] is False
    assert (group / 'memory.reclaim').read_text() == ''


def test_the_backend_raises_for_a_kernel_without_a_reclaim_control(tmp_path):
    """An absent control is a kernel that does not offer it, not a file to make.

    `write_text` opens O_CREAT, which on a plain directory would quietly create
    `memory.reclaim` and report a reclaim that never happened.
    """
    backend = module().SystemdBackend(root=tmp_path)
    group = _cgroup_tree(tmp_path, SCOPE, reclaim_file=False)
    with pytest.raises(FileNotFoundError):
        backend.reclaim(SCOPE)
    assert not (group / 'memory.reclaim').exists()


@pytest.mark.parametrize('name', ['../system.slice', 'system.slice', SCOPE[:-6]])
def test_the_backend_refuses_to_reclaim_a_name_outside_its_namespace(tmp_path, name):
    backend = module().SystemdBackend(root=tmp_path)
    with pytest.raises(ValueError, match='invalid scope'):
        backend.reclaim(name)

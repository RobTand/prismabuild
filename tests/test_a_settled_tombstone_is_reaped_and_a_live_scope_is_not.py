"""A retained tombstone is removed on proof, and on nothing weaker.

The broker keeps an empty frozen parent whenever a container ticket was never
resolved, because a killed client cannot prove the daemon RPC completed and a
late container must have somewhere contained to land. #486 proposed removing
such a scope once its reservation was gone and a grace period had passed. A
grace period is not a containment argument -- nothing about elapsed time makes
an accepted daemon operation impossible -- so what removes it here is the
settlement the fleet already trusts: the holder's proof, under the same attempt
token its `release` carries, that its ownership marker is gone and the local
daemon lists no container under either of the shim's reserved labels.

What each test in this file is for:

*   settled, empty, frozen, identity-matching: removed, and reclaimed first.
*   not settled: kept, exactly as before this change.
*   populated: never removed, never stopped, never even asked for its memory.
    This is the rule that protects a live campaign job and it is asserted
    against the backend's whole operation log, not against the end state.
*   unfrozen, identity changed at the row, identity changed underneath the
    removal: refused, reported, and nothing removed.
*   the `settle` op itself: exact token, complete evidence, stopped scope.
"""
import json
import os

import pytest

from test_resource_broker import auth, authority, create, container_ticket, module

SETTLEMENT = {'schema': 'prismabuild.container-settlement.v1', 'marker_absent': True,
              'owner_container_ids': [], 'scope_container_ids': [],
              'checked_unix': 1_789_000_000.0}
IDENTITY = [64, 629_624]


def _pass(a):
    return a.handle(0, os.getpid(), {'op': 'maintenance_status'})


def _settle(request, record, evidence=SETTLEMENT):
    """A settle request and nothing else.

    Not `auth`: that helper carries the creation request's `memory_max_bytes`,
    and `settle` refuses it, which is the broker being right about its own
    field set rather than anything this file is testing.  `None` omits the
    evidence entirely.
    """
    sent = {'op': 'settle', 'action_key': request['action_key'],
            'nonce': request['nonce'], 'token': record['token']}
    if evidence is not None:
        sent['evidence'] = dict(evidence) if isinstance(evidence, dict) else evidence
    return sent


def _retired(a, b, monkeypatch, *, settle=True, charge=341_319_680):
    """The exact shape #486 reproduced: a ticket nobody resolved, then release."""
    request, record = create(a)
    container_ticket(a, record, monkeypatch)
    scope = record['scope_id']
    a.records[scope]['cgroup_identity'] = list(IDENTITY)
    b.identity[scope] = list(IDENTITY)
    b.charge[scope] = charge
    a.handle(os.getuid(), os.getpid(), auth(request, record, 'stop'))
    if settle:
        a.handle(os.getuid(), os.getpid(), _settle(request, record))
    assert a.handle(os.getuid(), os.getpid(),
                    auth(request, record, 'release'))['retired'] is True
    assert scope in b.groups
    b.ops.clear()
    return request, record, scope


def test_a_settled_tombstone_is_reclaimed_first_and_then_removed(authority, monkeypatch):
    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    status = _pass(a)
    assert status['health'] is True and status['errors'] == []
    assert scope not in b.groups, 'the slice is gone after the next inventory pass'
    names = [op for op, _ in b.ops]
    assert names == ['reclaim', 'stop', 'observe', 'release']
    assert names.index('reclaim') < names.index('release'), (
        'a memcg removed while it still holds page cache goes offline as a '
        'zombie and keeps the charge: reclaim cannot follow the removal')
    stored = json.loads((a.state_dir / (scope + '.json')).read_text())
    assert stored['released_unix'] > 0
    assert stored['maintenance_cleanup'] == 'settled container transaction'
    assert stored['reclaim_before_bytes'] == 341_319_680
    assert stored['container_settlement'] == SETTLEMENT


def test_a_reaped_tombstone_leaves_a_clean_pass_behind_it(authority, monkeypatch):
    """A released record with no kernel group is not a complaint."""
    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    _pass(a)
    b.ops.clear()
    status = _pass(a)
    assert status['health'] is True and status['active_scopes'] == 0
    assert status['errors'] == [] and b.ops == []


def test_an_unsettled_tombstone_is_kept_exactly_as_it_was(authority, monkeypatch):
    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch, settle=False)
    assert _pass(a)['active_scopes'] == 0
    assert scope in b.groups and [op for op, _ in b.ops] == ['reclaim']
    assert not json.loads((a.state_dir / (scope + '.json')).read_text()).get('released_unix')


def test_a_settled_but_populated_scope_is_never_touched(authority, monkeypatch):
    """The rule that protects the live campaign jobs, asserted at the backend.

    Settlement is about containers, never about processes.  A populated group
    must not reach a single backend call: not release, not stop, not reclaim.
    Asserting only "it was not removed" would pass even if the pass had frozen
    and killed it on the way, because this fake's ``stop`` clears ``populated``
    and would hide exactly that.
    """
    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    b.groups[scope]['populated'] = True
    status = _pass(a)
    assert status['active_scope_ids'] == [scope]
    assert b.ops == []
    assert scope in b.groups and b.charge[scope] == 341_319_680


def test_an_unfrozen_settled_scope_is_never_removed(authority, monkeypatch):
    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    b.stopped.clear()
    assert _pass(a)['active_scope_ids'] == [scope]
    assert b.ops == [] and scope in b.groups


def test_a_settled_scope_whose_identity_changed_is_reported_not_removed(
        authority, monkeypatch):
    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    b.identity[scope] = [64, 700_000]
    status = _pass(a)
    assert status['health'] is False
    assert any('identity changed' in error for error in status['errors'])
    assert b.ops == [] and scope in b.groups


def test_an_identity_that_changes_under_the_removal_refuses_and_reports(
        authority, monkeypatch):
    """The row is re-read because the pass's inventory predates its own stop.

    Mutating the driver rather than the fixture: the inventory the pass took
    matches, and the group is replaced between that read and the removal.  A
    check that only consulted the pass's own picture would remove somebody
    else's group here.
    """
    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)

    def replaced(name):
        b.ops.append(('observe', name))
        return {'populated': False, 'frozen': True, 'identity': [64, 700_001]}

    monkeypatch.setattr(b, 'observe', replaced)
    status = _pass(a)
    assert status['health'] is False
    assert any('changed before removal' in error for error in status['errors'])
    assert scope in b.groups and [op for op, _ in b.ops] == ['reclaim', 'stop', 'observe']


def test_a_group_that_vanished_under_the_removal_refuses_rather_than_guessing(
        authority, monkeypatch):
    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    monkeypatch.setattr(b, 'observe', lambda name: None)
    status = _pass(a)
    assert status['health'] is False
    assert any('changed before removal' in error for error in status['errors'])
    assert not json.loads((a.state_dir / (scope + '.json')).read_text()).get('released_unix')


def test_a_namespace_holding_an_unowned_group_defers_the_removal(
        authority, monkeypatch):
    """Housekeeping acts on a picture the pass believes, and this one does not.

    The unowned group is reported by a scan that runs *after* the record loop,
    so the gate names it rather than reading it off an error list that is still
    being built -- which is the difference between a check that holds and a
    check that happens to hold today.
    """
    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    b.groups['foreign.scope'] = {'budget': 1, 'populated': False}
    status = _pass(a)
    assert status['health'] is False
    assert scope in b.groups and [op for op, _ in b.ops] == ['reclaim']


def test_an_unhealthy_kernel_defers_the_removal(authority, monkeypatch):
    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    monkeypatch.setattr(b, 'healthy', lambda: False)
    status = _pass(a)
    assert status['health'] is False
    assert scope in b.groups and [op for op, _ in b.ops] == ['reclaim']


@pytest.mark.parametrize('token', [None, '0' * 64, 'not-a-token'])
def test_settlement_needs_the_exact_attempt_token(authority, monkeypatch, token):
    a, b = authority
    request, record = create(a)
    container_ticket(a, record, monkeypatch)
    a.handle(os.getuid(), os.getpid(), auth(request, record, 'stop'))
    sent = {'op': 'settle', 'action_key': request['action_key'],
            'nonce': request['nonce'], 'evidence': dict(SETTLEMENT)}
    if token is not None:
        sent['token'] = token
    with pytest.raises(PermissionError):
        a.handle(os.getuid(), os.getpid(), sent)
    assert not a.records[record['scope_id']].get('settled_unix')


def test_settlement_from_another_uid_is_refused(authority, monkeypatch):
    a, b = authority
    request, record = create(a)
    container_ticket(a, record, monkeypatch)
    a.handle(os.getuid(), os.getpid(), auth(request, record, 'stop'))
    with pytest.raises(PermissionError):
        a.handle(os.getuid() + 1, os.getpid(), _settle(request, record))
    assert not a.records[record['scope_id']].get('settled_unix')


@pytest.mark.parametrize('evidence', [
    None, 'settled', {},
    {**SETTLEMENT, 'schema': 'prismabuild.container-settlement.v2'},
    {**SETTLEMENT, 'marker_absent': False},
    {**SETTLEMENT, 'marker_absent': 1},
    {**SETTLEMENT, 'owner_container_ids': ['9c1f0e']},
    {**SETTLEMENT, 'scope_container_ids': ['9c1f0e']},
    {**SETTLEMENT, 'owner_container_ids': None},
    {**SETTLEMENT, 'checked_unix': 'now'},
    {**SETTLEMENT, 'extra': 1},
])
def test_incomplete_settlement_evidence_never_marks_a_scope_settled(
        authority, monkeypatch, evidence):
    """A settlement that still names a container is a scope that is not settled."""
    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch, settle=False)
    with pytest.raises(ValueError, match='settlement evidence'):
        a.handle(os.getuid(), os.getpid(), _settle(request, record, evidence))
    assert not a.records[scope].get('settled_unix')
    _pass(a)
    assert scope in b.groups


def test_settlement_is_refused_before_the_payload_is_stopped(authority, monkeypatch):
    a, b = authority
    request, record = create(a)
    container_ticket(a, record, monkeypatch)
    with pytest.raises(ValueError, match='not stopped'):
        a.handle(os.getuid(), os.getpid(), _settle(request, record))
    assert not a.records[record['scope_id']].get('settled_unix')


def test_settlement_of_an_incomplete_setup_is_refused(authority, monkeypatch):
    """A scope whose creation never finished has no transaction to settle."""
    a, b = authority
    original = b.create

    def partial(scope, budget):
        original(scope, budget)
        raise OSError('controller setup failed')

    monkeypatch.setattr(b, 'create', partial)
    request = {'op': 'create', 'action_key': 'a' * 64, 'nonce': 'b' * 32,
               'memory_max_bytes': 64 * 1024**2}
    with pytest.raises(OSError):
        a.handle(os.getuid(), os.getpid(), request)
    scope = next(iter(a.records))
    with pytest.raises(ValueError, match='setup incomplete'):
        a.handle(os.getuid(), os.getpid(),
                 {'op': 'settle', 'action_key': 'a' * 64, 'nonce': 'b' * 32,
                  'token': a.records[scope]['token'], 'evidence': dict(SETTLEMENT)})
    assert not a.records[scope].get('settled_unix')


def test_settlement_survives_a_broker_restart(authority, monkeypatch):
    """The proof is durable: it outlives the process that received it."""
    a, b = authority
    request, record, scope = _retired(a, b, monkeypatch)
    restored = type(a)(a.state_dir, os.getuid(), b, max_memory_bytes=1024**3)
    assert restored.records[scope]['container_settlement'] == SETTLEMENT
    assert restored.handle(0, os.getpid(), {'op': 'maintenance_status'})['health'] is True
    assert scope not in b.groups


def test_the_settlement_schema_is_one_contract_with_the_client():
    from prismabuild import resource_scope
    assert module().SETTLEMENT_SCHEMA == resource_scope.CONTAINER_SETTLEMENT_SCHEMA

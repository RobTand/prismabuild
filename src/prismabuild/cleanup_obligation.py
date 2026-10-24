"""Exact-attempt cleanup recovery. Callers supply identity, never evidence."""
from __future__ import annotations

import secrets
import socket
from weakref import WeakKeyDictionary

from . import claim_identity, core, immutable_files, record_store, resource_scope
from .produced_output import _broker_scope_id

OBLIGATION_SCHEMA = 'prismabuild.cleanup_obligation.v1'
RECOVERY_SCHEMA = 'prismabuild.cleanup_recovery.v1'
_MINT = object()
_permits = WeakKeyDictionary()


class _CleanupPermit:
    __slots__ = ('__weakref__',)

    def __new__(cls, sentinel=None):
        if cls is not _CleanupPermit or sentinel is not _MINT:
            raise TypeError('cleanup permits are minted by verification only')
        return super().__new__(cls)

    def __copy__(self):
        raise TypeError('cleanup permits cannot be copied')

    def __deepcopy__(self, memo):
        raise TypeError('cleanup permits cannot be copied')

    def __reduce_ex__(self, protocol):
        raise TypeError('cleanup permits cannot be serialized')


def _obligation_for(queue, record, nonce):
    return {'schema': OBLIGATION_SCHEMA, 'state': 'pending',
            'action_key': record['action_key'],
            'generation': queue.records.attempt_generation(record),
            'attempt': int(record.get('attempts', 0)) + 1,
            'claimed_host': record['claimed_host'], 'claimed_by': record['claimed_by'],
            'claimed_unix': record['claimed_unix'], 'nonce': nonce}


def _cleanup_owner(queue, key, generation, attempt):
    if not record_store.key_shaped(key) or not record_store.key_shaped(generation):
        raise record_store.PoolContractError('cleanup recovery requires full key and generation')
    if type(attempt) is not int or attempt < 1:
        raise record_store.PoolContractError('cleanup recovery requires a positive attempt')
    claimed = record_store.value_or_raise(queue.records.item(record_store.CLAIMED, key))
    terminal = record_store.value_or_raise(queue.records.item(record_store.FAILED, key))
    lease = record_store.read_document(queue.records.lease_path(key))
    if claimed is not None and terminal is not None:
        raise record_store.PoolContractError('cleanup recovery has conflicting owners')
    record = claimed if claimed is not None else terminal if terminal is not None else lease
    if record is None:
        raise record_store.PoolContractError('cleanup recovery has no durable owner')
    launches = record.get('attempts', 0)
    if type(launches) is not int or launches < 0:
        raise record_store.PoolContractError('cleanup owner has invalid attempt count')
    number = launches + (1 if terminal is None else 0)
    if (queue.records.attempt_generation(record) != generation or number != attempt
            or record.get('claimed_host') != socket.gethostname()):
        raise record_store.PoolContractError('cleanup recovery identity or owning host differs')
    for state in (record_store.READY, record_store.DONE, record_store.WITHDRAWN):
        if record_store.value_or_raise(queue.records.item(state, key)) is not None:
            raise record_store.PoolContractError('cleanup recovery conflicts with another owner')
    claim_identity.check_lease_identity(key, record,
        record_store.read_document(queue.records.lease_path(key)),
        invalid=record_store.PoolContractError)
    if terminal is not None:
        archived = queue.attempt_ending(record)  # Verifies immutable attempt and log bytes.
        if (archived['attempt'] != attempt or archived['status'] != 'failed'
                or archived['detail'] != record.get('detail')):
            raise record_store.PoolContractError('cleanup recovery differs from immutable attempt')
        identity = queue.attempt_outcomes(record)[-1]
        for field in ('claimed_by', 'claimed_unix', 'claimed_host', 'published_unix',
                      'finished_host', 'finished_unix'):
            if record.get(field) != identity.get(field):
                raise record_store.PoolContractError('cleanup owner differs from immutable identity')
        if not record.get('resource_scope_intent'):
            raise record_store.PoolContractError('failed attempt has no durable scope intent')
    intent = record.get('resource_scope_intent')
    if (not isinstance(intent, dict) or intent.get('action_key') != key
            or intent.get('socket_path') != str(resource_scope.BROKER_SOCKET)):
        raise record_store.PoolContractError('cleanup intent is invalid')
    nonce = intent.get('nonce')
    if not isinstance(nonce, str) or len(nonce) != 32 or any(c not in '0123456789abcdef' for c in nonce):
        raise record_store.PoolContractError('cleanup nonce is invalid')
    lease = record_store.read_document(queue.records.lease_path(key))
    if lease is not None:
        for field in ('resource_scope', 'resource_scope_intent'):
            if field in lease and lease[field] != record.get(field):
                raise record_store.PoolContractError('cleanup intent contradicts lease')
    unit = _broker_scope_id(key, nonce)
    control = record.get('resource_scope')
    if control is not None and (not isinstance(control, dict)
            or control.get('nonce') != nonce or control.get('scope_id') != unit):
        raise record_store.PoolContractError('cleanup control contradicts intent')
    expected = _obligation_for(queue, record, nonce)
    expected['attempt'] = attempt
    debt = record.get('cleanup_obligation')
    if debt is not None and (not isinstance(debt, dict) or any(debt.get(field) != value for field, value in expected.items()
                                if field != 'state')):
        raise record_store.PoolContractError('cleanup obligation contradicts owner')
    path = (queue.records.lease_path(key) if claimed is None and terminal is None
            else queue.records.item_path(record_store.CLAIMED if claimed is not None
                                         else record_store.FAILED, key))
    return path, record, expected


def _verify_cleanup_identity(queue, key, generation, attempt):
    from .pool import PoolQueue
    if type(queue) is not PoolQueue:
        raise TypeError('cleanup authority must be the owning queue')
    with queue.action_transition_lock(key):
        path, record, obligation = _cleanup_owner(queue, key, generation, attempt)
        challenge = secrets.token_hex(32)
        nonce = obligation['nonce']
        unit = _broker_scope_id(key, nonce)
        intent = record['resource_scope_intent']
        scope = resource_scope.ResourceScope(key, nonce, intent['memory_max_bytes'],
            queue.ledger().base / 'telemetry' / f'{key}.json',
            socket_path=resource_scope.BROKER_SOCKET)
        answer = scope._request('cleanup_verdict', challenge=challenge)
        expected = {'ok': True, 'action_key': key, 'nonce': nonce, 'scope_id': unit,
                    'challenge': challenge, 'empty': True, 'released': True}
        if any(type(answer.get(k)) is not type(v) or answer.get(k) != v for k, v in expected.items()):
            raise record_store.PoolContractError('fresh exact-scope cleanup verdict is unproved')
        containers = queue._cleanup_action_containers(record)
        if containers.get('complete') is not True:
            raise record_store.PoolContractError('container cleanup is unproved')
        # Re-read at the effect boundary too. The registry binds both the queue
        # instance and durable bytes; a forged value carries no release authority.
        permit = _CleanupPermit(_MINT)
        _permits[permit] = (queue, key, generation, attempt, path, core.canonical_sha256(record),
                            obligation, answer, containers)
        return permit


def _publish_cleanup_permit(queue, permit):
    if type(permit) is not _CleanupPermit or permit not in _permits:
        raise TypeError('verified cleanup permit required')
    owner, key, generation, attempt, path, digest, obligation, answer, containers = _permits.pop(permit)
    if owner is not queue:
        raise TypeError('cleanup permit belongs to another authority')
    with queue.action_transition_lock(key):
        current_path, record, _ = _cleanup_owner(queue, key, generation, attempt)
        if current_path != path or core.canonical_sha256(record) != digest:
            raise record_store.PoolContractError('cleanup owner changed after verification')
        from .pool import _now
        checked = _now()
        cleanup = {'complete': True, 'nonce': obligation['nonce'], 'checked_unix': checked,
                   'released': answer, 'export': answer}
        recovery = {'schema': RECOVERY_SCHEMA, 'identity': obligation,
                    'resource_scope_cleanup': cleanup, 'container_cleanup': containers,
                    'owner_sha256': digest, 'owner_record': record,
                    'checked_host': socket.gethostname()}
        supplement = queue.records.attempt_path(record, attempt).with_suffix('.cleanup-recovery.json')
        # A crash before the mutable summary is updated leaves immutable recovery
        # evidence. A later verified pass may adopt it, never replace its bytes.
        previous = record_store.read_document(supplement)
        if previous is None:
            immutable_files.publish_immutable(supplement, core._canonical_file_bytes(recovery))
            previous = record_store.read_document(supplement)
        original = previous.get('owner_record')
        prior_scope = previous.get('resource_scope_cleanup')
        prior_containers = previous.get('container_cleanup')
        if (previous.get('schema') != RECOVERY_SCHEMA or previous.get('identity') != obligation
                or previous.get('checked_host') != socket.gethostname()
                or not isinstance(original, dict)
                or core.canonical_sha256(original) != previous.get('owner_sha256')
                or not isinstance(prior_scope, dict) or prior_scope.get('complete') is not True
                or prior_scope.get('nonce') != obligation['nonce']
                or not isinstance(prior_scope.get('released'), dict)
                or any(prior_scope['released'].get(field) != answer[field]
                       for field in ('action_key', 'nonce', 'scope_id', 'ok', 'empty', 'released'))
                or not isinstance(prior_containers, dict) or prior_containers.get('complete') is not True
                or prior_containers.get('remaining')):
            raise record_store.PoolContractError('conflicting cleanup recovery supplement')
        annotations = ('resource_scope_cleanup', 'container_cleanup', 'cleanup_obligation')
        if ({k: v for k, v in original.items() if k not in annotations}
                != {k: v for k, v in record.items() if k not in annotations}):
            raise record_store.PoolContractError('cleanup supplement belongs to a changed owner')
        record.update(resource_scope_cleanup=previous['resource_scope_cleanup'],
                      container_cleanup=previous['container_cleanup'],
                      cleanup_obligation={**obligation, 'state': 'resolved'})
        queue.records.replace_record_at(path, record)
        return record

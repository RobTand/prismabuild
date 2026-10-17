"""Interruption after donor withdrawal must leave a completed, clean adoption."""
from pathlib import Path
import hashlib
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import pool, reader_lease, residency_map, staged_path

TIER = 'prismabuild-stage:adoption-control'
DONOR, SUCCESSOR, FIRST, SECOND = (c * 64 for c in 'de12')


class AdoptionCompletionInterrupted(BaseException):
    pass


@pytest.fixture
def adoption_completion_files(tmp_path):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    queue.tier(TIER).mint({'stage_gib': 2})
    assert queue.tier(TIER).take(DONOR, {'stage_gib': 1})
    stage = tmp_path / 'stage'
    stage.mkdir()
    staged_path.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    payload = b'actual adopted payload\0' * 64
    path = stage / 'payload'
    path.write_bytes(payload)
    manifest = hashlib.sha256(payload).hexdigest()
    key = residency_map.residency_map_key('/origin/payload', 0)
    entry = {'stage_path': str(path), 'bytes': len(payload), 'offset': 0,
             'sha256': hashlib.sha256(payload).hexdigest()}
    fragment = {'schema': residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
                'consumer_action_key': FIRST, 'mover_action_key': DONOR,
                'tier_id': TIER, 'stage_root': str(stage),
                'manifest_sha256': manifest, 'entries': {key: entry}}
    root = queue.records.residency_fragment_root()
    staged_path.publish_vouch(root, fragment, generation='a' * 32,
        material_entries={key: {**entry, 'file_id': reader_lease.stat_identity(str(path))}})
    queue.record_move(DONOR, {'consumer_action_key': FIRST, 'tier_id': TIER,
        'stage_root': str(stage), 'manifest_sha256': manifest,
        'range_start_bytes': 0, 'range_end_bytes': len(payload),
        'range_bytes': len(payload), 'bytes_staged': len(payload),
        'entries_declared': 1, 'entries_staged': 1, 'complete': True})
    mark = reader_lease.write_retiring(reader_lease.leases_root(queue, root),
        consumer_action_key=FIRST, mover_action_key=DONOR, generation='a' * 32)
    args = dict(old_key=DONOR, new_key=SUCCESSOR, consumer_action_key=SECOND,
                tier_id=TIER, phase='phase-0', range_start_bytes=0,
                range_end_bytes=len(payload), residency_root=root)
    return queue, root, path, payload, manifest, mark, args


@pytest.mark.parametrize('boundary', ['fragment-unlinked', 'withdrawn'])
@pytest.mark.parametrize('successor_state', ['valid', 'unknown-fragment', 'unknown-material', 'live',
    'unknown-receipt', 'different-receipt', 'float-receipt',
    'boolean-receipt', 'wrong-schema-receipt', 'empty-receipt'])
def test_an_interrupted_withdrawal_finishes_from_a_fresh_owner(
        adoption_completion_files, monkeypatch, boundary, successor_state):
    queue, root, path, payload, manifest, mark, args = adoption_completion_files
    identity = reader_lease.stat_identity(str(path))
    donor_fragment = residency_map.fragment_path(root, FIRST, DONOR)
    if boundary == 'fragment-unlinked':
        unlink = Path.unlink
        def interrupt_unlink(target, *a, **kw):
            result = unlink(target, *a, **kw)
            if target == donor_fragment:
                raise AdoptionCompletionInterrupted('after real donor fragment unlink')
            return result
        monkeypatch.setattr(Path, 'unlink', interrupt_unlink)
    else:
        withdraw = staged_path.withdraw_vouch
        def interrupt_withdraw(*a, **kw):
            withdraw(*a, **kw)
            raise AdoptionCompletionInterrupted('after real donor withdrawal')
        monkeypatch.setattr(staged_path, 'withdraw_vouch', interrupt_withdraw)
    with pytest.raises(AdoptionCompletionInterrupted):
        staged_path.adopt(queue, **args)
    monkeypatch.undo()
    # Recreate the queue/RecordStore/TierLedger owners, retaining only real files.
    queue = pool.PoolQueue(queue.root)
    assert queue.tier(TIER).holdings(DONOR) == {}
    assert queue.tier(TIER).holdings(SUCCESSOR) == {'stage_gib': 1}
    assert not donor_fragment.exists()
    if successor_state == 'unknown-fragment':
        residency_map.fragment_path(root, SECOND, SUCCESSOR).write_bytes(b'{unknown')
    elif successor_state == 'unknown-material':
        reader_lease.material_path(root, SECOND, SUCCESSOR).write_bytes(b'{unknown')
    elif successor_state == 'live':
        queue.publish(action_key=SUCCESSOR, cas_root=queue.root / 'cas',
            checkout_root=queue.root / 'co', worker_script=queue.root / 'worker.py',
            resources={'cpu': 1, 'mem_gb': 1})
    elif successor_state == 'unknown-receipt':
        queue.records.move_path(SUCCESSOR).write_bytes(b'{unknown')
    elif successor_state == 'different-receipt':
        receipt = queue.move_record(SUCCESSOR, fresh=True)
        queue.record_move(SUCCESSOR, {**receipt, 'adopted_from_consumer': 'f' * 64})
    elif successor_state in ('float-receipt', 'boolean-receipt'):
        receipt = queue.move_record(SUCCESSOR, fresh=True)
        queue.record_move(SUCCESSOR, {**receipt,
            'range_start_bytes': 0.0 if successor_state == 'float-receipt' else False})
    elif successor_state == 'wrong-schema-receipt':
        queue.records.move_path(SUCCESSOR).write_text('{"schema":"unknown"}\n')
    elif successor_state == 'empty-receipt':
        queue.records.move_path(SUCCESSOR).write_bytes(b'')
    donor_material = reader_lease.material_path(root, FIRST, DONOR)
    before_material = donor_material.read_bytes() if donor_material.exists() else None
    before_mark = mark.read_bytes() if mark.exists() else None
    result = staged_path.adopt(queue, **args)
    assert path.read_bytes() == payload
    assert reader_lease.stat_identity(str(path)) == identity
    assert queue.tier(TIER).holdings(SUCCESSOR) == {'stage_gib': 1}
    if successor_state != 'valid':
        assert result['adopted'] is False, result
        assert (donor_material.read_bytes() if donor_material.exists() else None) == before_material
        assert (mark.read_bytes() if mark.exists() else None) == before_mark
        return
    assert result['adopted'] is True, result
    assert result['tokens_moved'] == 0
    assert not donor_material.exists()
    assert not mark.exists()
    receipt = queue.move_record(SUCCESSOR, fresh=True)
    assert {k: receipt[k] for k in ('consumer_action_key', 'tier_id', 'stage_root',
        'manifest_sha256', 'range_start_bytes', 'range_end_bytes', 'bytes_staged',
        'bytes_copied', 'entries_declared', 'entries_staged', 'complete',
        pool.MOVE_ADOPTED_FROM_FIELD, 'adopted_from_consumer', 'phase')} == {
        'consumer_action_key': SECOND, 'tier_id': TIER, 'stage_root': str(path.parent),
        'manifest_sha256': manifest, 'range_start_bytes': 0,
        'range_end_bytes': len(payload), 'bytes_staged': len(payload), 'bytes_copied': 0,
        'entries_declared': 1, 'entries_staged': 1, 'complete': True,
        pool.MOVE_ADOPTED_FROM_FIELD: DONOR, 'adopted_from_consumer': FIRST, 'phase': 'phase-0'}
    # Adoption files a move receipt; the normal map composer is a separate
    # consumer. Compose its real successor fragment before testing admission.
    residency_map.write_map(queue.records.residency_map_path(SECOND),
        residency_map.compose(residency_map.read_fragments(root, SECOND)))
    assert queue.residency_verdict({'action_key': SECOND, 'residency': {
        'schema': pool.RESIDENCY_SCHEMA_V1, 'tier_id': TIER,
        'manifest_sha256': manifest, 'manifest_bytes': len(payload),
        'leads': [SUCCESSOR]}})['state'] == 'resident'
    receipt_bytes = queue.records.move_path(SUCCESSOR).read_bytes()
    repeated = staged_path.adopt(pool.PoolQueue(queue.root), **args)
    assert repeated['adopted'] is True, repeated
    assert repeated['tokens_moved'] == 0
    assert queue.records.move_path(SUCCESSOR).read_bytes() == receipt_bytes

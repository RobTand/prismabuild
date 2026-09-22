"""Missing-material partial DONE owners cannot permanently block a successor (#866)."""
from __future__ import annotations

import hashlib
import json
import os

import pytest

import test_dead_owner_fragment_blocks_then_retires as base
from test_dead_owner_fragment_blocks_then_retires import fleet
from prismabuild import core as pb, reader_lease


def dead_done_owner(fleet):
    queue, stage, _cas = fleet
    consumer, _ = base._fail_consumer(queue)
    mover = base._key()
    base._publish(queue, mover, max_attempts=1)
    for name in base.NAMES:
        base._stage_marked(stage, name)
    base._write_fragment(queue, stage, consumer, mover, base.NAMES)
    queue.record_move(mover, {
        'consumer_action_key': consumer, 'tier_id': base.TIER, 'stage_root': str(stage),
        'manifest_sha256': 'a' * 64, 'complete': False,
        'entries_declared': 3, 'entries_staged': 2,
        'bytes_staged': 2 * base.SIZE, 'range_bytes': 3 * base.SIZE,
        'range_start_bytes': 0, 'range_end_bytes': 3 * base.SIZE,
        'errors': ['interrupted partial publication']})
    queue.finish(mover, status='executed', detail={'returncode': 0})
    assert not queue.tier_ledger(base.TIER).holder_tokens(mover)
    assert reader_lease.read_material(queue.residency_fragment_root(), consumer, mover) is None
    return consumer, mover


def test_housekeeping_retires_partial_done_and_successor_copies_and_pins(fleet, tmp_path):
    queue, stage, cas = fleet
    consumer, mover = dead_done_owner(fleet)
    successor, copier = base._key(), base._key()
    publisher = base._publisher(fleet, copier, successor)
    assert publisher._decide(stage / base.NAMES[0], base.SIZE, 'c' * 64,
                             computed=None, source_id=None, heal=True)[0] == 'refuse'
    receipts = base.stage_release.sweep(queue, stage_roots={base.TIER: str(stage)}, pressure={})
    assert any(r.get('action_key') == mover and r.get('complete') for r in receipts)
    assert not base.residency_map.fragment_path(queue.residency_fragment_root(), consumer, mover).exists()
    assert not any((stage / name).exists() for name in base.NAMES)

    mount = tmp_path / 'sources'
    mount.mkdir()
    payload = b'x' * base.SIZE
    digest = hashlib.sha256(payload).hexdigest()
    entries = []
    for name in base.NAMES:
        source = mount / name
        source.write_bytes(payload)
        entries.append({'path': str(source), 'offset': 0, 'bytes': base.SIZE, 'sha256': digest})
    manifest = {'schema': pb.DATA_MANIFEST_SCHEMA_V1, 'produced_by': {}, 'annotations': {},
                'mount_prefix': str(mount), 'entries': entries, 'entry_count': len(entries),
                'total_bytes': len(entries) * base.SIZE}
    raw = pb._canonical_file_bytes(pb.validate_data_manifest(manifest))
    manifest_digest = hashlib.sha256(raw).hexdigest()
    path = tmp_path / 'manifest.json'
    path.write_bytes(raw)
    base._publish(queue, successor, max_attempts=1)
    args = base.stage_move.build_parser().parse_args([
        '--pool-root', str(queue.root), '--cas-root', str(cas), '--action-key', copier,
        '--consumer-action-key', successor, '--tier-id', base.TIER,
        '--stage-root', str(stage), '--manifest', str(path), '--manifest-sha256', manifest_digest,
        '--range-start-bytes', '0', '--range-end-bytes', str(len(entries) * base.SIZE),
        '--residency-root', str(queue.residency_fragment_root()), '--readers', '1',
        '--max-readers', '1', '--unpaced'])
    result = base.stage_move.move(args)
    assert result['complete'] and result['entries_staged'] == len(entries)
    queue.record_move(copier, result)
    key = base.residency_map.residency_map_key(entries[0]['path'], 0)
    pin = reader_lease.acquire(
        queue, consumer_action_key=successor, attempt={'nonce': 'n1', 'scope_id': 's1'},
        tier_id=base.TIER, epoch='', span={'start_bytes': 0, 'end_bytes': base.SIZE},
        holder={'host': 'fixture', 'pid': os.getpid()}, acquire_token='successor-proof',
        covers=[{'mover_action_key': copier, 'manifest_sha256': manifest_digest}],
        expected={key: {'bytes': base.SIZE, 'sha256': digest}})
    assert pin['ok'], pin
    fd, _ = reader_lease.open_pinned(queue, pin['pin'], pin['ref_id'], key)
    try:
        assert os.read(fd, base.SIZE) == payload
    finally:
        os.close(fd)
        reader_lease.release(queue, pin['pin_id'], pin['ref_id'], consumer_action_key=successor)


@pytest.mark.parametrize('damage', ['complete', 'counts', 'manifest', 'bytes', 'material',
                                  'live_owner', 'live_mover', 'credits', 'terminal_proof',
                                  'missing_receipt', 'receipt_symlink'])
def test_uncertain_or_live_done_owner_retains(fleet, damage):
    queue, stage, _cas = fleet
    consumer, mover = dead_done_owner(fleet)
    if damage in ('complete', 'counts', 'manifest', 'bytes'):
        record = queue.move_record(mover)
        record.update({'complete': {'complete': True}, 'counts': {'entries_staged': 1},
                       'manifest': {'manifest_sha256': 'f' * 64},
                       'bytes': {'bytes_staged': 1}}[damage])
        queue.record_move(mover, record)
    elif damage == 'material':
        path = reader_lease.material_path(queue.residency_fragment_root(), consumer, mover)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{}')
    elif damage in ('live_owner', 'live_mover'):
        key = consumer if damage == 'live_owner' else mover
        queue.publish(action_key=key, cas_root='/cas', checkout_root='/co', worker_script='/w.py',
                      resources={'cpu': 1}, recompute=True)
    elif damage == 'terminal_proof':
        terminal = json.loads(queue.item_path(base.pool.DONE, mover).read_text())
        queue.attempt_path(terminal, terminal['attempts']).unlink()
    elif damage in ('missing_receipt', 'receipt_symlink'):
        path = queue.move_path(mover)
        if damage == 'receipt_symlink':
            other = path.with_suffix('.captured')
            path.rename(other)
            path.symlink_to(other)
        else:
            path.unlink()
    else:
        queue.mint_tier_capacity(base.TIER, {'stage_gib': 1})
        assert queue.tier_ledger(base.TIER).acquire(mover, {'stage_gib': 1})
    base.stage_release.sweep_dead_owner_fragments(queue, stage_roots={base.TIER: str(stage)})
    assert all((stage / name).exists() for name in base.NAMES)
    assert base.residency_map.fragment_path(queue.residency_fragment_root(), consumer, mover).exists()


def test_done_retirement_preserves_a_live_coowners_bytes(fleet, monkeypatch):
    monkeypatch.setattr(base, '_dead_owner', dead_done_owner)
    base.test_a_path_shared_with_a_live_owner_is_kept(fleet)


@pytest.mark.parametrize('owner', ['consumer', 'mover'])
def test_done_republication_after_census_retains(fleet, monkeypatch, owner):
    monkeypatch.setattr(base, '_dead_owner', dead_done_owner)
    base.test_republication_after_census_retains_new_generation(fleet, monkeypatch, owner)

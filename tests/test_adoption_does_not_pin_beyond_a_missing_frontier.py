"""A retry keeps distant donors reclaimable until its missing frontier arrives."""
from __future__ import annotations

import hashlib

import test_a_resident_range_is_adopted_rather_than_recopied as base
from test_a_resident_range_is_adopted_rather_than_recopied import queue, stage


def test_missing_frontier_keeps_future_donors_available_to_pressure(queue, stage, monkeypatch):
    # Exact observed credits, with tiny physical fixture files: 13 free, each
    # missing input and its following advance priced at 14 GiB. These are
    # synthetic ledger prices, not a 69 GiB physical-storage benchmark.
    monkeypatch.setattr(base, 'PHASE_GIB', 14)
    monkeypatch.setattr(base, 'DIGEST', hashlib.sha256(b'x' * 32).hexdigest())
    queue.mint_tier_capacity(base.TIER, {'stage_gib': 69})
    ledger = queue.tier_ledger(base.TIER)
    for ordinal in (0, 3, 4):
        base._stage_range(queue, mover=base._hexkey(f'oldmover{ordinal}'),
                          consumer=base.FIRST, stage=stage, ordinal=ordinal)
    plan = base._plan(queue, base.SECOND, phases=5, label='new')
    base._publish_consumer(queue, base.SECOND, plan)
    base._claim_with_progress(queue, base.SECOND, phase='phase-0')
    # Represents prepaid producer output credit, protected by the live owner.
    assert ledger.acquire(base.SECOND, {'stage_gib': 14})
    assert ledger.available()['stage_gib'] == 13
    tiers = {base.TIER: base._tier_record(stage, gib=69)}
    events = base.tier_loop.adopt_resident_ranges(queue, tiers=tiers)
    assert [event['phase'] for event in events if event.get('adopted')] == ['phase-0']
    wanted, owners = base.stage_release.live_claims(queue)
    assert base.SECOND in owners
    for ordinal in (3, 4):
        old = base._hexkey(f'oldmover{ordinal}')
        assert old not in wanted and old not in owners
        assert ledger.holder_tokens(old) == {'stage_gib': 14}
        assert not ledger.holder_tokens(base._hexkey(f'newmover{ordinal}'))
    # No pressure preserves the useful distant bytes under their old owners.
    base.stage_release.sweep(queue, stage_roots={base.TIER: str(stage)}, pressure={})
    assert ledger.available()['stage_gib'] == 13
    # Real tier cycles own reclamation and mover publication, including the
    # frontier/advance grant. The test must reach a real READY frontier.
    for _ in range(3):
        base._cycle(queue, stage, gib=69)
    assert queue.item_path(base.pool.READY, base._hexkey('newmover1')).exists()
    assert ledger.holder_tokens(base.SECOND) == {'stage_gib': 14}
    assert ledger.holder_tokens(base._hexkey('newmover0')) == {'stage_gib': 14}


def test_frontier_is_claimed_copied_and_read_through_a_real_pin(queue, stage, monkeypatch, tmp_path):
    import json
    import os
    import stage_move
    from prismabuild import core as pb

    monkeypatch.setattr(base, 'PHASE_GIB', 14)
    queue.mint_tier_capacity(base.TIER, {'stage_gib': 69})
    ledger = queue.tier_ledger(base.TIER)
    mount = tmp_path / 'sources'
    mount.mkdir()
    payload = b'qualified-frontier-bytes' * 4
    size = len(payload)
    entries = []
    for ordinal in range(5):
        source = mount / f'phase-{ordinal}.bin'
        source.write_bytes(payload)
        entries.append({'path': str(source), 'offset': 0, 'bytes': size,
                        'sha256': hashlib.sha256(payload).hexdigest()})
    manifest = {'schema': pb.DATA_MANIFEST_SCHEMA_V1, 'produced_by': {},
                'annotations': {}, 'mount_prefix': str(mount), 'entries': entries,
                'entry_count': len(entries), 'total_bytes': size * len(entries)}
    raw = pb._canonical_file_bytes(pb.validate_data_manifest(manifest))
    digest = hashlib.sha256(raw).hexdigest()
    manifest_path = tmp_path / 'manifest.json'
    manifest_path.write_bytes(raw)
    monkeypatch.setattr(base, 'MANIFEST', digest)
    plan = base._plan(queue, base.SECOND, phases=5, label='actual')
    plan['stage_root'], plan['manifest_bytes'] = str(stage), len(raw)
    for ordinal, phase in enumerate(plan['phases']):
        phase['start_bytes'], phase['end_bytes'] = ordinal * size, (ordinal + 1) * size
        phase['mover_row']['residency'].update(
            manifest_bytes=len(raw), range_start_bytes=ordinal * size,
            range_end_bytes=(ordinal + 1) * size)
    plan = base.residency_plan.validate_plan(plan)

    def copy(consumer, mover, ordinal):
        args = stage_move.build_parser().parse_args([
            '--pool-root', str(queue.root), '--cas-root', str(tmp_path / 'cas'),
            '--action-key', mover, '--consumer-action-key', consumer,
            '--tier-id', base.TIER, '--stage-root', str(stage),
            '--manifest-sha256', digest, '--manifest', str(manifest_path),
            '--range-start-bytes', str(ordinal * size),
            '--range-end-bytes', str((ordinal + 1) * size),
            '--residency-root', str(queue.residency_fragment_root()),
            '--readers', '1', '--max-readers', '1', '--unpaced'])
        result = stage_move.move(args)
        assert result['complete'], result
        queue.record_move(mover, result)
        return result

    for ordinal in (0, 3, 4):
        mover = base._hexkey(f'previous{ordinal}')
        assert ledger.acquire(mover, {'stage_gib': 14})
        copy(base.FIRST, mover, ordinal)
    base._publish_consumer(queue, base.SECOND, plan)
    base._claim_with_progress(queue, base.SECOND, phase='phase-0')
    assert ledger.acquire(base.SECOND, {'stage_gib': 14})
    assert ledger.available()['stage_gib'] == 13
    # Actual production tier loop: adoption -> pressure -> sweep -> fence ->
    # publication. Repeated cycles model asynchronous completed ownership.
    for _ in range(3):
        base._cycle(queue, stage, gib=69)
    mover = base._hexkey('actualmover1')
    ready = base.pool._read_json(queue.item_path(base.pool.READY, mover))
    claim = queue.claim(ready=[ready], tags=['dl380g10'], owner='frontier-reader-test',
                        capacity={'cpu': 4, 'mem_gb': 4})
    assert claim and claim['action_key'] == mover
    result = copy(base.SECOND, mover, 1)
    assert result['bytes_staged'] == size and result['entries_staged'] == 1
    key = base.residency_map.residency_map_key(entries[1]['path'], 0)
    acquired = base.reader_lease.acquire(
        queue, consumer_action_key=base.SECOND, attempt={'nonce': 'n1', 'scope_id': 's1'},
        tier_id=base.TIER, epoch='', span={'start_bytes': size, 'end_bytes': size * 2},
        holder={'host': 'test-host', 'pid': os.getpid()}, acquire_token='frontier-pin',
        covers=[{'mover_action_key': mover, 'manifest_sha256': digest}],
        expected={key: {'bytes': size, 'sha256': entries[1]['sha256']}})
    assert acquired['ok'], acquired
    fd, serving = base.reader_lease.open_pinned(queue, acquired['pin'], acquired['ref_id'], key)
    try:
        assert os.read(fd, size) == payload
    finally:
        os.close(fd)
        assert base.reader_lease.release(queue, acquired['pin']['pin_id'],
                                         acquired['ref_id'], consumer_action_key=base.SECOND)
    assert ledger.holder_tokens(base.SECOND) == {'stage_gib': 14}

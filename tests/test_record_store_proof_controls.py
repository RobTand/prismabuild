"""Explicit Store/C11 owners must honor acquire, hit and commit proof loss."""
import json
import os
from pathlib import Path
import pytest
from prismabuild import kept_reads, pool, produced_output as po, record_store
from record_store_trust import ObservedTrust
from prismabuild import stage_release
import tier_loop
import test_stale_material_done_owner_retires as owner
from prismabuild import staged_path

fleet = owner.fleet
KEY = 'a' * 64


def _store(tmp_path):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    path = queue.records.item_path(record_store.READY, KEY)
    path.write_text('{"v":1}')
    trust = ObservedTrust()
    reader = kept_reads.DirectoryRecords(trust=trust)
    return queue, path, trust, reader, record_store.RecordStore.kept(queue.root, records=reader)


@pytest.mark.parametrize('phase', ['acquire', 'hit', 'commit'])
def test_store_does_not_restore_a_previous_capture_after_proof_loss(tmp_path, monkeypatch, phase):
    _queue, path, trust, reader, store = _store(tmp_path)
    assert store.state_records(record_store.READY) == [(path, {'v': 1})]
    original = trust.directory_current(path.parent)
    trust.directory_versions[str(path.parent)] = original
    if phase == 'acquire':
        trust.available = False
        trust.phase_error = 'acquire'
        with pytest.raises(RuntimeError, match='acquisition proof interruption'):
            store.state_records(record_store.READY)
    elif phase == 'hit':
        trust.phase_error = 'hit'
        with pytest.raises(RuntimeError, match='hit proof interruption'):
            store.state_records(record_store.READY)
    else:
        path.write_text('{"v":2}')
        rewritten = path.stat()
        os.utime(path, ns=(rewritten.st_atime_ns, rewritten.st_mtime_ns + 1))
        assert path.stat().st_mtime_ns != rewritten.st_mtime_ns
        trust.directory_versions[str(path.parent)] = (*original[:3], original[3] + 1)
        native_read = Path.read_bytes
        reads = []
        def read(current):
            raw = native_read(current)
            if current == path:
                reads.append(current)
                trust.reject_commit = True
            return raw
        with monkeypatch.context() as fault:
            fault.setattr(Path, 'read_bytes', read)
            assert store.state_records(record_store.READY) == [(path, {'v': 2})]
        assert reads == [path]
        trust.reject_commit = False
        trust.directory_versions[str(path.parent)] = original
    expected = 3 if phase == 'commit' else 2
    path.write_text(json.dumps({'v': expected}))
    written = path.stat()
    os.utime(path, ns=(written.st_atime_ns, written.st_mtime_ns + 1))
    assert path.stat().st_mtime_ns != written.st_mtime_ns
    trust.available = True
    assert trust.directory_current(path.parent) == original
    assert store.state_records(record_store.READY) == [(path, {'v': expected})], (
        'a failed/unqualified capture restored an older answer under repeated metadata')
    assert reader.parsed >= 2
    assert any(kind == 'file' for kind, _ in trust.events)


def test_store_key_authority_is_fresh_beside_an_explicit_kept_frame(tmp_path):
    queue, path, trust, reader, store = _store(tmp_path)
    assert queue.records is not store
    trust.directory_versions[str(path.parent)] = trust.directory_current(path.parent)
    store.begin_read()
    try:
        first = store.state_records(record_store.READY)[0][1]
        first['v'] = 99
        replacement = path.with_suffix('.tmp')
        replacement.write_text('{"action_key":"' + KEY + '","v":2}')
        replacement.replace(path)
        assert store.state_records(record_store.READY)[0][1] == {'v': 1}
        standing = store.standing(KEY)
        assert isinstance(standing, record_store.Present)
        assert standing.value.record['v'] == 2
        assert queue.records.item(record_store.READY, KEY).value['v'] == 2
    finally:
        store.end_read()
    assert reader.parsed == 1


def test_receipt_cache_shares_its_supplied_trust_across_native_readers(tmp_path):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    path = queue.records.move_path(KEY)
    path.write_text('{"action_key":"' + KEY + '","v":1}')
    trust = ObservedTrust()
    cache = tier_loop.ReceiptCache(trust=trust)
    assert cache.records.trust is cache.census.trust is cache.ledger_records.trust is trust
    assert cache.fragment_epochs.records is cache.records
    assert cache.read([path.parent], queue=queue) == [{'action_key': KEY, 'v': 1}]
    assert cache.read([path.parent], queue=queue) == [{'action_key': KEY, 'v': 1}]
    assert cache.records.parsed == 1
    trust.available = False
    path.write_text('{"action_key":"' + KEY + '","v":2}')
    assert cache.read([path.parent], queue=queue) == [{'action_key': KEY, 'v': 2}]
    assert cache.records.parsed == 2


def test_output_tick_uses_its_supplied_trust_for_hit_and_proof_loss(tmp_path):
    scope = tmp_path / 'scope'
    path = scope / 'commitments.json'
    po._write_commitments(path, {'batches': {'b': {'state': 'filed'}}, 'admission': None})
    trust = ObservedTrust()
    reads = po._TickReads(None, trust=trust)
    with kept_reads.file_version_scope():
        assert reads.batches(scope) == {'b': {'state': 'filed'}}
        assert reads.batches(scope) == {'b': {'state': 'filed'}}
        assert (reads.commitments_read, reads.commitments_reused) == (1, 1)
        trust.available = False
        assert reads.batches(scope) == {'b': {'state': 'filed'}}
        assert (reads.commitments_read, reads.commitments_reused) == (2, 1)
    assert [kind for kind, _ in trust.events].count('fence') == 3
    assert [kind for kind, _ in trust.events].count('file') >= 5


def test_real_dead_owner_checkpoint_uses_the_census_trust_owner(fleet):
    queue, stage, _cas = fleet
    consumer, mover = owner.stale_owner(fleet, coherent_names=set(owner.NAMES), replace=False)
    root = queue.records.residency_fragment_root()
    trust = ObservedTrust()
    index = kept_reads.CensusIndex(trust=trust)
    staged_path.reset_skip_checkpoints()
    try:
        def sweep():
            return stage_release.sweep_dead_owner_fragments(
                queue, stage_roots={owner.TIER: str(stage)}, residency_root=root, index=index)
        first = [row for row in sweep() if row.get('event') == staged_path.STALE_MENTION_EVENT]
        assert len(first) == 1 and first[0]['cacheable'], first
        assert sweep() == []
        assert index.stale_skipped == 1
        trust.available = False
        after = [row for row in sweep() if row.get('event') == staged_path.STALE_MENTION_EVENT]
        assert after and not after[0]['cacheable']
        assert index.stale_censused >= 2
        assert any(kind == 'fence' for kind, _ in trust.events)
        assert all((stage / owner.staged_name(name)).exists() for name in owner.NAMES)
    finally:
        staged_path.reset_skip_checkpoints()

"""Real kept consumers must not retain a caller's nested record mutation."""
import json

import pytest
from prismabuild import kept_reads, pool, storage_tiers
from prismabuild import census as fleet_census
import pbmetrics
from prismabuild import tier_cycle as tier_loop
from record_store_trust import ObservedTrust


KEY = 'a' * 64
PAYLOAD = {'action_key': KEY, 'nested': {'values': [1, {'x': 2}]}}


def test_receipt_consumer_keeps_its_native_record_private(tmp_path):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    path = queue.records.move_path(KEY)
    path.write_text(json.dumps(PAYLOAD))
    trust = ObservedTrust()
    receipts = tier_loop.ReceiptCache()
    receipts.records = kept_reads.DirectoryRecords(trust=trust)
    first = receipts.read([path.parent], queue=queue)
    assert first == [PAYLOAD]
    first[0]['nested']['values'][1]['x'] = 99
    first[0]['nested']['values'].append('caller-owned')
    second = receipts.read([path.parent], queue=queue)
    assert second == [PAYLOAD], 'nested caller mutation reached the retained receipt'
    assert receipts.records.parsed == 1, 'isolation must retain native parse reuse'
    assert trust.events
    assert json.loads(path.read_text()) == PAYLOAD


def test_metrics_consumers_receive_independent_nested_records(tmp_path):
    directory = tmp_path / 'queue' / pool.record_store.CLAIMED
    directory.mkdir(parents=True)
    path = directory / (KEY + '.json')
    path.write_text(json.dumps(PAYLOAD))
    trust = ObservedTrust()
    reader = fleet_census.KeptReads(trust=trust)
    reader.begin()
    try:
        first = reader.queue_records(directory)[0][1].record
        assert first == PAYLOAD
        first['nested']['values'][1]['x'] = 99
        second = reader.queue_records(directory)[0][1].record
        assert second == PAYLOAD, 'one scrape exposed its retained JSON to another consumer'
    finally:
        reader.end()
    reader.begin()
    try:
        assert reader.queue_records(directory)[0][1].record == PAYLOAD
        assert reader.records.parsed == 1
    finally:
        reader.end()
    assert json.loads(path.read_text()) == PAYLOAD


def test_retired_receipt_mutation_cannot_change_the_next_public_fold(tmp_path):
    import test_a_terminal_receipt_retires_behind_a_covered_fold as history
    from prismabuild import storage_tiers

    loop = history._Loop(tmp_path)
    queue = loop.queue
    terminal = history._key('isolated-retired-consumer')
    history._terminal(queue, pool.record_store.DONE, terminal)
    key = history._move(queue, 'isolated-retired', consumer=terminal,
                        unix=history.T0)
    history._sweep(history._survey(queue))
    assert not queue.records.move_path(key).exists()
    expected = queue.read_mover_retirements()[key]
    expected_supply = storage_tiers.fill_supply_from_records(
        [expected], pool_identity=history.IDENTITY_A)
    trust = ObservedTrust()
    receipts = tier_loop.ReceiptCache()
    receipts.records = kept_reads.DirectoryRecords(trust=trust)
    first = receipts.read([queue.records.move_path(key).parent], queue=queue)
    assert first == [expected]
    first[0]['disk_pacing']['mean_pool_read_mb_s'] = 1
    first[0]['pool_identity']['stage']['members'].append('caller-owned')
    assert receipts.fill_supply(history.IDENTITY_A) == expected_supply, (
        'a returned projection changed the cache-owned pricing input')
    assert receipts.read([queue.records.move_path(key).parent], queue=queue) == [expected]
    assert receipts.records.parsed == 2, 'both checkpoint records must retain native parse reuse'
    assert queue.read_mover_retirements()[key] == expected


def test_unchanged_receipts_do_not_repeat_the_store_thaw(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    path = queue.records.move_path(KEY)
    path.write_text(json.dumps(PAYLOAD))
    receipts = tier_loop.ReceiptCache(trust=ObservedTrust())
    receipts.read([path.parent], queue=queue)

    def repeated_thaw(*_args):
        raise AssertionError('unchanged receipt was thawed again by the store')

    monkeypatch.setattr(pool.record_store, '_thawed_document', repeated_thaw)
    assert receipts.read([path.parent], queue=queue) == [PAYLOAD]


def test_private_receipt_refresh_reuses_documents_and_the_merged_sequence(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    path = queue.records.move_path(KEY)
    path.write_text(json.dumps(PAYLOAD))
    cache = tier_loop.ReceiptCache(trust=ObservedTrust())
    cache.refresh([path.parent], queue=queue)
    original = cache._records

    def unexpected_copy(*_args):
        raise AssertionError('steady refresh copied a retained JSON tree')

    monkeypatch.setattr(pool.record_store, '_thaw_json', unexpected_copy)
    cache.refresh([path.parent], queue=queue)
    assert cache._records is original
    assert cache.records.parsed == 1


def test_receipt_thaw_reuse_follows_native_invalidation_and_forgets_removed_names(tmp_path):
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    path = queue.records.move_path(KEY)
    path.write_text(json.dumps(PAYLOAD))
    trust = ObservedTrust()
    cache = tier_loop.ReceiptCache(trust=trust)
    cache.refresh([path.parent], queue=queue)
    first = cache._records[0]
    replacement = path.with_suffix('.tmp')
    replacement.write_text(json.dumps({**PAYLOAD, 'v': 2}))
    replacement.replace(path)
    cache.refresh([path.parent], queue=queue)
    assert cache._records[0] is not first
    assert cache._records[0]['v'] == 2
    assert cache.records.parsed == 2
    trust.available = False
    cache.refresh([path.parent], queue=queue)
    cache.refresh([path.parent], queue=queue)
    assert cache.records.parsed == 4
    path.unlink()
    cache.refresh([path.parent], queue=queue)
    assert cache._records == []
    assert str(path) not in cache._receipt_thaws


def test_receipt_reuse_refuses_a_same_tick_replacement(tmp_path, monkeypatch):
    from test_a_same_tick_record_version_is_not_kept import _replace_in_the_same_tick

    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    path = queue.records.move_path(KEY)
    path.write_bytes(b'{"v": 1}')
    trust = ObservedTrust()
    trust.fence = path.stat().st_ctime_ns
    cache = tier_loop.ReceiptCache(trust=trust)
    assert cache.read([path.parent], queue=queue) == [{'v': 1}]
    _replace_in_the_same_tick(monkeypatch, path, b'{"v": 2}')
    trust.fence = 2**63 - 1
    assert cache.read([path.parent], queue=queue) == [{'v': 2}]
    assert cache.records.parsed == 2


def test_extension_discovery_cannot_mutate_private_receipt_inputs(tmp_path):
    from test_a_steady_tier_cycle_rereads_only_what_changed import _Loop, _small_shape

    loop = _Loop(tmp_path, _small_shape())
    loop.receipts = tier_loop.ReceiptCache(trust=ObservedTrust())
    key = 'b' * 64
    loop.queue.records.move_path(key).write_text(json.dumps({**PAYLOAD, 'action_key': key}))

    def change_discovery_inputs(**kwargs):
        record = next(r for r in kwargs['fill_records'] if r.get('action_key') == key)
        record['nested']['values'].clear()
        kwargs['fill_records'].clear()
        return {}

    tier_loop.cycle(loop.queue, host='receipt-test', source_pool='storage_pool',
                    receipts=loop.receipts, discover=change_discovery_inputs)
    record = next(r for r in loop.receipts._records if r.get('action_key') == key)
    assert record['nested'] == PAYLOAD['nested']
    assert len(loop.receipts._records) > 1


@pytest.mark.parametrize('consumer', ['discovery', 'fill_supply', 'mover_cap'])
def test_native_receipt_consumers_leave_retained_documents_unchanged(tmp_path, consumer):
    import test_a_terminal_receipt_retires_behind_a_covered_fold as history
    from test_storage_tiers import _runner

    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.records.ensure_layout()
    for index, delivered in enumerate((200.0, 300.0)):
        history._move(queue, f'read-only-{index}', consumer=KEY,
                      unix=history.T0 + index, delivered=delivered)
    directories = [queue.records.move_path(KEY).parent]
    cache = tier_loop.ReceiptCache(trust=ObservedTrust())
    cache.refresh(directories, queue=queue)
    retained = cache._records
    arcstats = tmp_path / 'arcstats'
    arcstats.write_text('c_max 4 257698037760\narc_meta_used 4 5423149896\n')
    missing = str(tmp_path / 'missing')

    def consume(records):
        if consumer == 'discovery':
            return storage_tiers.discover_tiers(
                host='receipt-test', runner=_runner(pools='', statuses={}),
                arcstats_path=str(arcstats), proc_pressure=missing,
                sysfs=missing, by_id=missing, fill_records=records, now=history.T0)
        if consumer == 'fill_supply':
            return storage_tiers.fill_supply_from_records(
                records, pool_identity=history.IDENTITY_A)
        return storage_tiers.mover_cap_from_records(
            records, tier_id=history.TIER, pool_identity=history.IDENTITY_A)

    actual = consume(retained)
    cold = tier_loop.ReceiptCache(trust=ObservedTrust())
    fresh = cold.read(directories, queue=queue)
    assert retained == fresh, f'{consumer} mutated a retained receipt'
    assert actual == consume(fresh)
    if consumer == 'discovery':
        assert actual['arc:receipt-test'][storage_tiers.FILL_RECORD_FIELD] is not None
    elif consumer == 'mover_cap':
        assert actual['receipts'] == 2
    cache.refresh(directories, queue=queue)
    assert cache._records is retained
    assert cache._records == cold.read(directories, queue=queue)

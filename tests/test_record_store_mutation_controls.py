"""Real kept consumers must not retain a caller's nested record mutation."""
import json
from prismabuild import kept_reads, pool
import pbmetrics
import tier_loop
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
    reader = pbmetrics.KeptReads(trust=trust)
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

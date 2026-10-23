"""Paired #2172 receipt/decision replay, using only private recorded inputs.

Run through an ordinary portable published pbrun action, per Rob's #2172 brief. The baseline fixture is the exact
ReceiptCache.read method at 43234d0fbd, sealed inside this checkout snapshot. A retained scratch corpus, built by
bench_tier_cycle, is read by both arms. Every fill-supply, mover-cap and fill-rate decision is compared.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import resource
import statistics
import sys
import tempfile
import time

from prismabuild import pool, storage_tiers, tier_cycle


def bench_receipt_thaws_main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True,
                        help='Path to the recorded baseline ReceiptCache.read method')
    parser.add_argument('--receipts', type=int, default=6434,
                        help='Number of synthetic receipt files in the replay corpus (default: 6434)')
    parser.add_argument('--cycles', type=int, default=10,
                        help='Warm receipt cycles per arm in each pair (default: 10)')
    parser.add_argument('--pairs', type=int, default=5,
                        help='Interleaved baseline/current sample pairs (default: 5)')
    args = parser.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'fleet'))
    import bench_tier_cycle as corpus

    namespace = dict(vars(tier_cycle))
    exec('class BaselineReceiptCache(ReceiptCache):\n' + args.baseline.read_text(), namespace)
    baseline_cache = namespace['BaselineReceiptCache']
    samples = []
    with tempfile.TemporaryDirectory(prefix='receipt-replay-') as scratch:
        queue = pool.PoolQueue(Path(scratch) / 'queue')
        queue.records.ensure_layout()
        corpus._receipts(queue, args.receipts)
        for index, path in enumerate(sorted((queue.root / pool.record_store.MOVERS).glob('*.json'))):
            record = pool.record_store.read_document(path)
            delivered = 220.0 + index % 20
            record.update(disk_pacing={'mean_pool_read_mb_s': delivered,
                                       'pool_read_bytes': record['bytes_staged']},
                          movers_claimed_on_tier=1 + index % 4,
                          fill_demand_mb_s_pool_side=150.0,
                          mb_per_s_file_side=100.0)
            path.write_text(json.dumps(record))
        directories = [queue.root / pool.record_store.PREWARM,
                       queue.root / pool.record_store.MOVERS]
        # These are recorded JSON files, reread by both implementations. No
        # live queue, host ledger, worker or GPU is consulted by this replay.
        recorded = [pool.record_store.read_document(path)
                    for path in sorted(directories[1].glob('*.json'))]
        expected = (storage_tiers.fill_rate_from_records(recorded),
                    storage_tiers.fill_supply_from_records(recorded),
                    storage_tiers.mover_cap_from_records(recorded, tier_id=corpus.TIER))
        assert expected[0] is not None
        assert any(expected[1][field] is not None
                   for field in ('best_mb_s', 'ceiling_mb_s'))
        assert expected[2]['curve'], 'recording must exercise measured mover pricing'
        old, new = baseline_cache(), tier_cycle.ReceiptCache()
        old.read(directories, queue=queue)
        new.refresh(directories, queue=queue)
        for pair in range(args.pairs):
            for arm in (('before', 'after') if pair % 2 == 0 else ('after', 'before')):
                cache = old if arm == 'before' else new
                load_before = os.getloadavg()
                usage, wall = resource.getrusage(resource.RUSAGE_SELF), time.perf_counter()
                for _ in range(args.cycles):
                    if arm == 'before':
                        records = cache.read(directories, queue=queue)
                    else:
                        cache.refresh(directories, queue=queue)
                        records = cache._records
                    decisions = (storage_tiers.fill_rate_from_records(records),
                                 cache.fill_supply(None), cache.mover_cap(corpus.TIER, None))
                    assert decisions == expected, 'recorded tier decisions changed'
                ending = resource.getrusage(resource.RUSAGE_SELF)
                cpu = ending.ru_utime + ending.ru_stime - usage.ru_utime - usage.ru_stime
                wall = time.perf_counter() - wall
                samples.append({'pair': pair, 'arm': arm, 'cpu_s_per_cycle': cpu / args.cycles,
                                'wall_s_per_cycle': wall / args.cycles,
                                'load_before': load_before, 'load_after': os.getloadavg()})
        assert old.records.parsed == new.records.parsed == args.receipts
        assert new.records.kept > 0, 'scratch filesystem did not qualify warm reuse'
        # Replay a recorded input replacement too; only its file identity moves.
        replacement = dict(recorded[0])
        replacement['disk_pacing'] = {'mean_pool_read_mb_s': 123.0}
        path = queue.records.move_path(str(replacement['action_key']))
        temporary = path.with_suffix('.replacement')
        temporary.write_text(json.dumps(replacement))
        temporary.replace(path)
        left = old.read(directories, queue=queue)
        right = new.read(directories, queue=queue)
        assert left == right
        assert old.fill_supply(None) == new.fill_supply(None)
        assert old.mover_cap(corpus.TIER, None) == new.mover_cap(corpus.TIER, None)
    medians = {arm: statistics.median(s['cpu_s_per_cycle'] for s in samples if s['arm'] == arm)
               for arm in ('before', 'after')}
    print(json.dumps({'receipts': args.receipts, 'cycles_per_arm': args.cycles,
                      'pairs': args.pairs, 'tier_decisions_equal': True, 'samples': samples,
                      'median_cpu_s_per_cycle': medians,
                      'cpu_reduction_percent': 100 * (1 - medians['after'] / medians['before'])}))


if __name__ == '__main__':
    bench_receipt_thaws_main()

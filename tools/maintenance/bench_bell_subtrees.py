"""Paired #2173 replay over one batch of host-directory deletion events.

Run through an ordinary portable published pbrun action, per Rob's #2173 brief. The baseline fixture is the exact
_drop_host_tree method at 43234d0fbd, sealed inside this checkout snapshot. Both arms consume identical packets in private scratch.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import resource
import statistics
import tempfile
import time

from prismabuild import queue_bell, queue_bell_events


def bench_bell_subtrees_main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--events', type=int, default=10000)
    parser.add_argument('--watches', type=int, default=1000)
    parser.add_argument('--pairs', type=int, default=5)
    args = parser.parse_args()
    namespace = dict(vars(queue_bell_events))
    exec('class BaselineInotifyQueue(InotifyQueue):\n' + args.baseline.read_text(), namespace)
    baseline_queue = namespace['BaselineInotifyQueue']
    samples = []
    with tempfile.TemporaryDirectory(prefix='bell-replay-') as scratch:
        root = Path(scratch) / 'queue'
        for name in queue_bell.REQUIRED_STATE_DIRS:
            (root / name).mkdir(parents=True)
        for index in range(args.watches):
            (root / 'reservations' / f'host-{index}').mkdir(parents=True)
        old = baseline_queue(root)
        new = queue_bell_events.InotifyQueue(root)
        try:
            for pair in range(args.pairs):
                results = {}
                for arm in (('before', 'after') if pair % 2 == 0 else ('after', 'before')):
                    watch = old if arm == 'before' else new
                    ring = queue_bell_events.EventRing()
                    ring.set_coverage(watch.covered_topics)
                    wd = watch.root_watches[Path('reservations')]
                    leaf = b'gone\0'
                    packet = (queue_bell_events.EVENT_HEADER.pack(
                        wd, queue_bell_events.IN_DELETE | queue_bell_events.IN_ISDIR,
                        0, len(leaf)) + leaf) * args.events
                    load_before = os.getloadavg()
                    usage, wall = resource.getrusage(resource.RUSAGE_SELF), time.perf_counter()
                    watch.consume(packet, ring)
                    ending = resource.getrusage(resource.RUSAGE_SELF)
                    cpu = ending.ru_utime + ending.ru_stime - usage.ru_utime - usage.ru_stime
                    wall = time.perf_counter() - wall
                    samples.append({'pair': pair, 'arm': arm, 'cpu_s': cpu, 'wall_s': wall,
                                    'load_before': load_before, 'load_after': os.getloadavg()})
                    results[arm] = (list(ring.events), ring.seq, ring.floor, ring.reset_count,
                                    ring.coverage_version, sorted(ring.covered_topics),
                                    dict(watch.directories), dict(watch.root_watches),
                                    dict(watch.file_watches))
                # Watch descriptor integers are kernel-assigned per fd. Paths
                # and every externally visible ring/coverage value must agree.
                before, after = results['before'], results['after']
                assert before[:6] == after[:6], 'bell delivery changed'
                assert sorted(map(str, before[6].values())) == sorted(map(str, after[6].values()))
                assert set(before[7]) == set(after[7]) and set(before[8]) == set(after[8])
        finally:
            old.close()
            new.close()
    medians = {arm: statistics.median(s['cpu_s'] for s in samples if s['arm'] == arm)
               * 10000 / args.events for arm in ('before', 'after')}
    print(json.dumps({'events_per_arm': args.events, 'watches': args.watches,
                      'pairs': args.pairs, 'delivery_equal': True, 'samples': samples,
                      'median_cpu_s_per_10k': medians,
                      'cpu_reduction_percent': 100 * (1 - medians['after'] / medians['before'])}))


if __name__ == '__main__':
    bench_bell_subtrees_main()

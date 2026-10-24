"""Observation-only main-era worker wrapper for the #2156 baseline.

Copy this file onto the adopted main checkout before starting a drained
baseline worker. Nothing here selects CPUs or changes a claim's return value.
Use: python cpu_lending_baseline_worker.py --output-root DIR WORKER [ARGS...]
A failed sidecar write aborts that cohort; never silently accept partial data.
"""
from __future__ import annotations

import argparse
from functools import wraps
from pathlib import Path
import runpy
import sys


def capture_main_cpu_attempt(row, ledger, tiers, output_root):
    from prismabuild import core, immutable_files

    if ledger is None or tiers is None:
        raise RuntimeError('baseline capture has no ledger or topology')
    with ledger._mutation_locked(blocking=False) as captured:
        if not captured:
            raise RuntimeError('baseline capture census reader busy')
        key = row['action_key']
        allocation = row.get('cpu_allocation')
        assigned = (set(allocation['preferred'] + allocation['fallback'])
                    if allocation is not None else None)
        tokens = immutable_files.match_optional(ledger.held_dir / key, 'cpu-*')
        owned = set(ledger.cpu_ids(tokens, tiers))
        metadata = ledger.holder_census().cpu_record(key).fields
        borrowed = (None if assigned is None else
                    [] if metadata.get('funded_by') else sorted(assigned - owned))
        identity = {name: row.get(name) for name in
                    ('action_key', 'published_unix', 'claimed_by', 'claimed_unix', 'claimed_host')}
        identity['attempt'] = int(row.get('attempts', 0)) + 1
        evidence = {**identity, 'source_commit': '43234d0fbd42d6ed76da38658cb9f447ed19a606',
                    'topology_sha256': core.canonical_sha256(tiers),
                    'assigned_cpus': sorted(assigned) if assigned is not None else None,
                    'physical_cpus': sorted(owned),
                    'borrowed_cpus': borrowed, 'funded_by': metadata.get('funded_by')}
    path = Path(output_root) / key / (core.canonical_sha256(identity) + '.json')
    immutable_files.publish_immutable(path, core._canonical_bytes(evidence))
    return path


def install_main_cpu_baseline_observer(output_root):
    from prismabuild import pool

    from prismabuild import adaptive_cpu, digest_primitives
    expected = {'pool': 'ac9f3a70e4d2fe9b6135be18cd9c3224b8da5a9c0bdedd25f5c6ea5f774762f7', 'adaptive_cpu': 'dadf5d5c4575bdd18e5cc8723da7d3e5c35f50a0bd8d64a2726c99c719fa10e6'}
    for name, module in (('pool', pool), ('adaptive_cpu', adaptive_cpu)):
        if digest_primitives.stream_sha256(Path(module.__file__)) != expected[name]:
            raise RuntimeError('baseline observer requires the adopted unmodified main source: ' + name)
    original = pool.PoolQueue._build_claim_record

    @wraps(original)
    def observed(queue, committed, scan_pass):
        row = original(queue, committed, scan_pass)
        try:
            capture_main_cpu_attempt(row, scan_pass.ledger, scan_pass.cpu_tiers, output_root)
        except Exception as exc:
            # Observation cannot refuse or crash main's claim. This marker,
            # or any missing attempt sidecar, invalidates the whole cohort.
            print('CPU_BASELINE_CAPTURE_FAILED ' + str(row.get('action_key'))
                  + ' ' + type(exc).__name__, file=sys.stderr, flush=True)
        return row

    pool.PoolQueue._build_claim_record = observed


def baseline_worker_main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-root', type=Path, required=True)
    parser.add_argument('worker')
    parser.add_argument('worker_args', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    install_main_cpu_baseline_observer(args.output_root)
    sys.argv = [args.worker, *args.worker_args]
    runpy.run_path(args.worker, run_name='__main__')


if __name__ == '__main__':
    baseline_worker_main()

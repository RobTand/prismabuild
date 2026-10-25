#!/usr/bin/env python3
"""Run a PrismaBuild test tier through the published fleet client."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
FLEET = Path('/mnt/shared/prismabuild-fleet')
PBTEST = FLEET / 'repo/tools/pbtest.py'
WORKER = FLEET / 'repo/tools/prismabuild_worker.py'
CPU_PYTHON = '/home/rob/venvs/pb-cpu/bin/python'
GPU_PYTHON = '/home/rob/venvs/pq-cu130/bin/python'
FAST_BUDGET_S = 600
FAST = (
    'tests/test_sorted_json_byte_profile.py',
    'tests/test_action_keys_survive_vocabulary_removal.py',
    'tests/test_digest_primitives_1547.py',
    'tests/test_dependency_digest_contract.py',
    'tests/test_cas_request_lookup_1313.py',
    'tests/test_cas_staging_descriptor_ownership.py',
    'tests/test_a_data_manifest_can_revisit_entries.py',
    'tests/test_a_data_manifest_names_bytes_exactly.py',
    'tests/test_compressed_data_manifests.py',
    'tests/test_prelaunch_manifest_1594.py',
    'tests/test_manifest_map_env_injection.py',
    'tests/test_park_proof_identity.py',
    'tests/test_pbtest_requested_paths.py',
    'tests/test_pbtest_plan_only.py',
    'tests/test_pbtest_seals_interpreter_1462.py',
    'tests/test_pbtest_worker_threads_1192.py',
    'tests/test_pbtest_workers.py',
    'tests/test_pbtest_cpu_shards_declare_their_bound.py',
    'tests/test_pbtest_incomplete_summary_1365.py',
    'tests/test_pbtest_unobserved_outcome.py',
    'tests/test_pbtest_recovers_unobserved.py',
    'tests/test_progress_write_atomic_1311.py',
    'tests/test_test_tiers.py',
    'tests/test_regression_test_gate.py',
)
GPU = ('tests/qualification/test_cuda.py',)


def populations(root: Path) -> dict[str, list[str]]:
    all_tests = {p.relative_to(root).as_posix()
                 for p in (root / 'tests').rglob('test_*.py')}
    declared = set(FAST) | set(GPU)
    missing = declared - all_tests
    if missing or set(FAST) & set(GPU) or len(FAST) != len(set(FAST)):
        raise ValueError(f'invalid tier membership: missing={sorted(missing)}')
    return {'fast': sorted(FAST), 'integration': sorted(all_tests - declared),
            'gpu': sorted(GPU)}


def command(root: Path, tier: str, python: str, report: Path) -> list[str]:
    files = populations(root)[tier]
    gpu = tier == 'gpu'
    cmd = [sys.executable, str(PBTEST), '--checkout', str(root),
           '--python', python, '--workers-per-shard', '1',
           '--threads-per-shard', '1', '--mem-gb', '8' if gpu else '4',
           '--priority', '-10', '--timeout-s', '540' if tier == 'fast' else '1800',
           '--test-timeout-s', '60' if tier == 'fast' else '300',
           '--json', str(report)]
    if gpu:
        cmd += ['--gpu', '--gpu-memory-gb', '2',
                '--env', 'PRISMABUILD_GPU_QUALIFICATION=1']
    return cmd + files


def verdict(rows: list[dict], selected: list[str], elapsed: float, tier: str) -> dict:
    covered = [name for row in rows for name in row['files']]
    green = bool(rows) and all(row['verdict']['green'] for row in rows)
    complete = sorted(covered) == sorted(selected) and len(covered) == len(set(covered))
    skips = [skip for row in rows for skip in row.get('skipped', []) or []]
    budget = elapsed < FAST_BUDGET_S if tier == 'fast' else None
    passed = green and complete and (tier != 'gpu' or not skips) and budget is not False
    counts: dict[str, int] = {}
    for row in rows:
        for outcome, count in (row['verdict'].get('reconciliation') or {}).get('record_counts', {}).items():
            counts[outcome] = counts.get(outcome, 0) + count
    return {'verdict': 'PASS' if passed else 'FAIL', 'tier': tier, 'counts': counts,
            'wall_s': round(elapsed, 3), 'fast_under_600_s': budget,
            'coverage_complete': complete, 'selected_files': selected,
            'skipped': skips, 'action_keys': [row.get('action_key') for row in rows]}


def verify_cas(rows: list[dict]) -> list[dict]:
    evidence = []
    for row in rows:
        key = row.get('action_key') or ''
        if not re.fullmatch(r'[0-9a-f]{64}', key):
            raise ValueError('shard has no full action key for CAS verification')
        request = FLEET / 'cas/requests' / key[:2] / f'{key}.json'
        result = subprocess.run(
            [sys.executable, str(WORKER), 'verify', '--action', str(request),
             '--cas-root', str(FLEET / 'cas')],
            capture_output=True, text=True, timeout=30, check=False)
        if result.returncode:
            raise ValueError(f'CAS verification failed for {key}: {result.stderr[-1000:]}')
        receipt = json.loads(result.stdout)
        if receipt['action_key'] != key:
            raise ValueError(f'CAS receipt names a different action: {key}')
        evidence.append({'action_key': key, 'request': str(request),
                         'receipt_sha256': receipt['receipt_sha256'],
                         'result': receipt['result']})
    return evidence


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('tier', choices=('fast', 'integration', 'gpu'))
    parser.add_argument('--python', help='absolute worker Python dependency')
    parser.add_argument('--report-dir', type=Path, help='evidence directory outside the checkout')
    parser.add_argument('--plan', action='store_true', help='print selection and command without submitting')
    args = parser.parse_args(argv)
    python = args.python or (GPU_PYTHON if args.tier == 'gpu' else CPU_PYTHON)
    if not Path(python).is_absolute():
        parser.error('--python must be an absolute worker path')
    if args.report_dir and args.report_dir.resolve().is_relative_to(ROOT):
        parser.error('--report-dir must be outside the checkout so evidence does not change its identity')
    try:
        selected = populations(ROOT)[args.tier]
        report_dir = args.report_dir.resolve() if args.report_dir else None
        if args.plan:
            report = (report_dir or Path('/tmp/pb-verify-plan')) / 'pbtest.json'
            print(json.dumps({'tier': args.tier, 'selected_files': selected,
                              'command': command(ROOT, args.tier, python, report)}, indent=2))
            return 0
        report_dir = report_dir or Path(tempfile.mkdtemp(prefix=f'pb-verify-{args.tier}-'))
        report_dir.mkdir(parents=True, exist_ok=True)
        report = report_dir / 'pbtest.json'
        if any((report_dir / name).exists() for name in ('pbtest.json', 'verdict.json')):
            parser.error('evidence already exists; use a fresh --report-dir')
        cmd = command(ROOT, args.tier, python, report)
        print(f'Evidence: {report_dir}\n{shlex.join(cmd)}', flush=True)
        started = time.monotonic()
        result = subprocess.run(cmd, cwd=ROOT, check=False)
        elapsed = time.monotonic() - started
        if report.is_file():
            rows = json.loads(report.read_text())
            summary = verdict(rows, selected, elapsed, args.tier)
            try:
                summary['verified_receipts'] = verify_cas(rows)
                summary['cas_verified'] = True
            except (OSError, ValueError, KeyError, subprocess.TimeoutExpired) as exc:
                summary.update(verdict='FAIL', cas_verified=False, reason=str(exc))
            summary['wall_s'] = round(time.monotonic() - started, 3)
            if args.tier == 'fast':
                summary['fast_under_600_s'] = summary['wall_s'] < FAST_BUDGET_S
                if not summary['fast_under_600_s']:
                    summary['verdict'] = 'FAIL'
        else:
            summary = {'verdict': 'INCONCLUSIVE', 'tier': args.tier,
                       'wall_s': round(elapsed, 3), 'reason': 'pbtest produced no report'}
        summary['pbtest_exit_code'] = result.returncode
        if result.returncode and summary['verdict'] == 'PASS':
            summary['verdict'] = 'FAIL'
        (report_dir / 'verdict.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(json.dumps(summary, indent=2))
        return 0 if summary['verdict'] == 'PASS' else 1
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.exit(2, f'verify: {exc}\n')


if __name__ == '__main__':
    raise SystemExit(main())

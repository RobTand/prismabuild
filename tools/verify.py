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
PBRUN = FLEET / 'repo/tools/pbrun.py'
PBWAIT = FLEET / 'repo/tools/pbwait.py'
WORKER = FLEET / 'repo/tools/prismabuild_worker.py'
CPU_PYTHON = '/home/rob/venvs/pb-cpu/bin/python'
GPU_PYTHON = '/home/rob/venvs/pq-cu130/bin/python'
FAST_BUDGET_S = 600
ROOT_WAIT_S = 14400
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
# Fixtures that take the first two PB-assigned CPUs; one-CPU shards refuse them.
TWO_CPU = (
    'tests/test_a_kill_names_what_it_waited_on.py',
    'tests/test_pool_cpu_tiers.py',
    'tests/test_worker_disk_metadata_admission.py',
)
# The integration tier is three lanes: one-CPU pbtest, two-CPU pbtest, and the
# root proof population in its pinned container (docs/proof_test_execution.md).
LANES = {'fast': ('fast',), 'integration': ('integration', 'two_cpu', 'root'),
         'gpu': ('gpu',)}


def root_population(root: Path) -> set[str]:
    """The files whose fixtures assert real UID/GID 0, as the root adapter loads them."""
    profile = json.loads((root / 'tests/proof_execution.json').read_text())
    receipt = root / 'tests/receipt_execution.json'
    extra = json.loads(receipt.read_text())['root_files'] if receipt.exists() else {}
    return set(profile['root']['files']) | set(extra)


def populations(root: Path) -> dict[str, list[str]]:
    """Every lane's files; the lanes are disjoint and cover every test file."""
    all_tests = {p.relative_to(root).as_posix()
                 for p in (root / 'tests').rglob('test_*.py')}
    declared = {'fast': FAST, 'gpu': GPU, 'two_cpu': TWO_CPU,
                'root': tuple(sorted(root_population(root)))}
    named = [name for files in declared.values() for name in files]
    missing = set(named) - all_tests
    repeated = {name for name in named if named.count(name) > 1}
    if missing or repeated:
        raise ValueError(f'invalid tier membership: missing={sorted(missing)} '
                         f'in_two_lanes={sorted(repeated)}')
    lanes = {lane: sorted(files) for lane, files in declared.items()}
    lanes['integration'] = sorted(all_tests - set(named))
    return lanes


def tier_files(root: Path, tier: str) -> list[str]:
    lanes = populations(root)
    return sorted(name for lane in LANES[tier] for name in lanes[lane])


def pbtest_command(root: Path, lane: str, python: str, report: Path) -> list[str]:
    files = populations(root)[lane]
    gpu = lane == 'gpu'
    cmd = [sys.executable, str(PBTEST), '--checkout', str(root),
           '--python', python, '--workers-per-shard', '1',
           '--threads-per-shard', '1', '--mem-gb', '8' if gpu else '4',
           '--priority', '-10',
           '--timeout-s', {'fast': '540', 'gpu': '1800'}.get(lane, '3600'),
           '--test-timeout-s', '60' if lane == 'fast' else '300',
           '--json', str(report)]
    if lane == 'two_cpu':
        cmd += ['--cpus-per-shard', '2']
    if gpu:
        cmd += ['--gpu', '--gpu-memory-gb', '2',
                '--env', 'PRISMABUILD_GPU_QUALIFICATION=1']
    return cmd + files


def root_command(root: Path, python: str) -> list[str]:
    """One detached action that prepares the pinned inputs and runs every root file."""
    required = json.loads((root / 'tests/proof_execution.json').read_text())['root']
    threads = str(required['native_threads'])
    # The pinned image is amd64 and only x86 workers hold it.
    return [sys.executable, str(PBRUN), '--cwd', str(root), '--tag', 'x86',
            '--cpus', str(required['cpus']), '--demand', f"mem_gb={required['memory_gib']}",
            '--container-image', required['image'],
            *(arg for name in ('OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS')
              for arg in ('--env', f'{name}={threads}')),
            '--priority', '-10', '--timeout-s', '3600', '--detach', '--',
            python, 'tests/run_root_population.py']


def tier_verdict(rows: list[dict], selected: list[str], elapsed: float, tier: str) -> dict:
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
            'skipped': skips, 'action_keys': [row.get('action_key') for row in rows],
            'failed_shards': [{'lane': row.get('lane'), 'action_key': row.get('action_key'),
                               'files': row['files']}
                              for row in rows if not row['verdict']['green']]}


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


def run_root_lane(python: str, report_dir: Path) -> dict:
    """Submit the root action, wait for its ending, and return it as a shard row."""
    files = populations(ROOT)['root']
    with (report_dir / 'root.log').open('w') as log:
        submitted = subprocess.run(root_command(ROOT, python), cwd=ROOT, stdout=subprocess.PIPE,
                                   stderr=log, text=True, check=False)
    (report_dir / 'root.submission.json').write_text(submitted.stdout)
    key = json.loads(submitted.stdout.strip().splitlines()[-1])['action_key'] \
        if submitted.returncode == 0 else None
    row = {'lane': 'root', 'files': files, 'action_key': key, 'verdict': {'green': False}}
    if key is None:
        return row
    waited = subprocess.run([sys.executable, str(PBWAIT), '--endings', '--wait-s', str(ROOT_WAIT_S),
                             key], capture_output=True, text=True, check=False)
    (report_dir / 'root.endings.json').write_text(waited.stdout)
    endings = json.loads(waited.stdout) if waited.stdout.strip() else {}
    ending = endings['endings'][-1] if endings.get('complete') and endings.get('endings') else {}
    row['verdict'] = {'green': ending.get('succeeded') is True, 'status': ending.get('status'),
                      'exit_code': ending.get('exit_code')}
    return row


def run_lanes(tier: str, python: str, report_dir: Path) -> tuple[list[dict], dict[str, int]]:
    """Run every lane of the tier at once; return their shard rows and exit codes."""
    running = {}
    for lane in LANES[tier]:
        if lane == 'root':
            continue
        report = report_dir / f'{lane}.pbtest.json'
        cmd = pbtest_command(ROOT, lane, python, report)
        log = (report_dir / f'{lane}.log').open('w')
        print(f'{lane}: {report_dir / f"{lane}.log"}\n{shlex.join(cmd)}', flush=True)
        running[lane] = (subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT),
                         report, log)
    rows: list[dict] = []
    codes: dict[str, int] = {}
    if 'root' in LANES[tier]:
        print(f'root: {report_dir / "root.log"}\n{shlex.join(root_command(ROOT, python))}', flush=True)
        row = run_root_lane(python, report_dir)
        rows.append(row)
        codes['root'] = 0 if row['verdict']['green'] else 1
    for lane, (process, report, log) in running.items():
        codes[lane] = process.wait()
        log.close()
        if not report.is_file():
            raise FileNotFoundError(f'{lane} lane: pbtest produced no report')
        rows += [{**row, 'lane': lane} for row in json.loads(report.read_text())]
    return rows, codes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('tier', choices=tuple(LANES))
    parser.add_argument('--python', help='absolute worker Python dependency')
    parser.add_argument('--report-dir', type=Path, help='evidence directory outside the checkout')
    parser.add_argument('--plan', action='store_true', help='print selection and commands without submitting')
    args = parser.parse_args(argv)
    python = args.python or (GPU_PYTHON if args.tier == 'gpu' else CPU_PYTHON)
    if not Path(python).is_absolute():
        parser.error('--python must be an absolute worker path')
    if args.report_dir and args.report_dir.resolve().is_relative_to(ROOT):
        parser.error('--report-dir must be outside the checkout so evidence does not change its identity')
    try:
        selected = tier_files(ROOT, args.tier)
        report_dir = args.report_dir.resolve() if args.report_dir else None
        if args.plan:
            plan_dir = report_dir or Path('/tmp/pb-verify-plan')
            lanes = populations(ROOT)
            print(json.dumps({'tier': args.tier, 'selected_files': selected, 'lanes': {
                lane: {'files': lanes[lane],
                       'command': root_command(ROOT, python) if lane == 'root'
                       else pbtest_command(ROOT, lane, python, plan_dir / f'{lane}.pbtest.json')}
                for lane in LANES[args.tier]}}, indent=2))
            return 0
        report_dir = report_dir or Path(tempfile.mkdtemp(prefix=f'pb-verify-{args.tier}-'))
        report_dir.mkdir(parents=True, exist_ok=True)
        if any(report_dir.iterdir()):
            parser.error('evidence already exists; use a fresh --report-dir')
        print(f'Evidence: {report_dir}', flush=True)
        started = time.monotonic()
        try:
            rows, codes = run_lanes(args.tier, python, report_dir)
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            rows, codes, missing = [], {}, str(exc)
        else:
            missing = None
        elapsed = time.monotonic() - started
        if missing is None:
            summary = tier_verdict(rows, selected, elapsed, args.tier)
            try:
                summary['verified_receipts'] = verify_cas([row for row in rows if row['verdict']['green']])
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
                       'wall_s': round(elapsed, 3), 'reason': missing}
        summary['lane_exit_codes'] = codes
        if any(codes.values()) and summary['verdict'] == 'PASS':
            summary['verdict'] = 'FAIL'
        (report_dir / 'verdict.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(json.dumps(summary, indent=2))
        return 0 if summary['verdict'] == 'PASS' else 1
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.exit(2, f'verify: {exc}\n')


if __name__ == '__main__':
    raise SystemExit(main())

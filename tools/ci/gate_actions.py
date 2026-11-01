#!/usr/bin/env python3
"""Evaluate hosted gate attempt records without retrying test failures."""
from __future__ import annotations

import argparse
from collections import Counter
import importlib.util
import json
from pathlib import Path
import re
import shutil
import sys


def _verdict(plan, files, record):
    if not isinstance(record, dict):
        return None
    if any(record.get(field) != plan.get(field) for field in ('base_sha', 'head_sha', 'mode')):
        return None
    if record.get('population_version') != plan['population']['version']:
        return None
    if (not isinstance(record.get('files'), list) or
            any(not isinstance(path, str) for path in record['files']) or
            sorted(record['files']) != sorted(files)):
        return None
    code = record.get('pytest_exit_code')
    budget = record.get('budget')
    if (record.get('verdict') == 'fail' and isinstance(budget, dict) and
            budget.get('status') == 'fail' and (code is None or type(code) is int and code in {0, 1})):
        return 'fail'
    if type(code) is not int:
        return None
    verdict = record.get('verdict')
    if not isinstance(verdict, str):
        return None
    return verdict if verdict == 'fail' or (verdict, code) == ('pass', 0) else None


def assess_attempts(plan, assignments, attempts):
    """Return retry indices and the final verdict, preserving any test failure."""
    indices = [row['index'] for row in assignments]
    assigned = Counter(path for row in assignments for path in row['files'])
    selected = [row['path'] for row in plan['selected']]
    if (len(indices) != len(set(indices)) or len(selected) != len(set(selected)) or
            set(assigned) != set(selected) or any(count != 1 for count in assigned.values())):
        raise ValueError('assignments must cover every selected file exactly once, with unique indices')
    retry, failed, missing, effective = [], [], [], {}
    for row in assignments:
        index, files = row['index'], row['files']
        first = _verdict(plan, files, attempts.get(1, {}).get(index))
        second = _verdict(plan, files, attempts.get(2, {}).get(index))
        if first is None:
            retry.append(index)
        if 'fail' in (first, second):
            verdict, attempt = 'fail', 1 if first == 'fail' else 2
        elif first is None:
            verdict, attempt = second, 2
        else:
            verdict, attempt = first, 1
        if verdict is None:
            missing.append(index)
        else:
            effective[index] = attempt
            if verdict == 'fail':
                failed.append(index)
    return {'schema': 'prismabuild.gate_aggregate.v1', 'retry_indices': sorted(retry),
            'failed_indices': sorted(failed), 'missing_indices': sorted(missing),
            'effective_attempts': effective, 'exit_code': 1 if failed else 75 if missing else 0}


def _read_result(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def workflow_artifacts(jobs, assignments, prefix, plan_attempt, workflow_attempt):
    """Resolve inherited GitHub jobs to their actual immutable result artifacts."""
    if (type(plan_attempt) is not int or type(workflow_attempt) is not int or
            not 1 <= plan_attempt <= workflow_attempt or not isinstance(jobs, list)):
        raise ValueError('workflow lineage requires a job list and positive ordered attempts')
    executions = {}
    indices = {row['index'] for row in assignments}
    for job in jobs:
        if not isinstance(job, dict):
            raise ValueError('invalid workflow job inventory')
        match = re.fullmatch(r'shard-(\d+)-a([12])', job.get('name', ''))
        if not match:
            continue
        index, stage = map(int, match.groups())
        attempt, started = job.get('run_attempt'), job.get('started_at')
        if type(attempt) is not int or attempt < 1 or not isinstance(started, str) or not started:
            raise ValueError('shard job is missing its execution identity')
        if index not in indices or attempt > workflow_attempt:
            continue
        # GitHub gives inherited successful jobs new IDs/run_attempt values but
        # preserves started_at. filter=all retains their original execution too.
        identity = (index, stage, started)
        if identity not in executions or attempt < executions[identity]['run_attempt']:
            executions[identity] = job
    latest = {}
    for (index, stage, _), job in executions.items():
        attempt = job['run_attempt']
        if attempt < plan_attempt:
            continue
        key = (index, stage)
        if key not in latest or attempt > latest[key]['run_attempt']:
            latest[key] = job
        elif attempt == latest[key]['run_attempt']:
            raise ValueError('ambiguous duplicate shard execution in workflow inventory')
    if any((index, 1) not in latest for index in indices):
        raise ValueError('workflow job inventory does not cover every first-attempt shard')
    sources = {index: {} for index in indices}
    for index in indices:
        first = latest[index, 1]['run_attempt']
        for stage in (1, 2):
            job = latest.get((index, stage))
            if job and job['run_attempt'] >= first and job.get('status') == 'completed':
                sources[index][stage] = f"{prefix}-{job['run_attempt']}-a{stage}-{index}"
    return sources


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['retries', 'finish'])
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--matrix', type=Path, required=True)
    parser.add_argument('--durations', type=Path, default=Path('tools/ci/gate_durations.json'))
    parser.add_argument('--artifacts', type=Path, required=True)
    parser.add_argument('--prefix', default='gate-result')
    parser.add_argument('--jobs', type=Path, help='paginated REST filter=all workflow job inventory')
    parser.add_argument('--plan-attempt', type=int)
    parser.add_argument('--workflow-attempt', type=int)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--effective-output', type=Path)
    parser.add_argument('--summary', type=Path)
    args = parser.parse_args(argv)
    try:
        spec = importlib.util.spec_from_file_location('gate_partition', Path(__file__).with_name('gate_partition.py'))
        partition = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(partition)
        plan = json.loads(args.plan.read_text())
        matrix = json.loads(args.matrix.read_text())
        costs = json.loads(args.durations.read_text())['files']
        balance = partition.setup_shards if matrix.get('partition') == 'setup-v2' else partition.shards
        options = {'setup_seconds': matrix['setup_seconds'], 'isolation_seconds': matrix['isolation_seconds']} if matrix.get('partition') == 'setup-v2' else {}
        assignments = balance([row['path'] for row in plan['selected']], costs,
                              matrix['shards'], unknown_seconds=partition.default_unknown_seconds(costs), **options)
        if (len(matrix['include']) != len(assignments) or
                sorted(row['index'] for row in matrix['include']) != list(range(len(assignments)))):
            raise ValueError('matrix indices do not match the complete partition')
        if args.jobs is not None:
            sources = workflow_artifacts(json.loads(args.jobs.read_text()), assignments,
                                         args.prefix, args.plan_attempt, args.workflow_attempt)
        elif args.plan_attempt is not None or args.workflow_attempt is not None:
            raise ValueError('workflow attempts require the complete job inventory')
        else:
            sources = {row['index']: {stage: f"{args.prefix}-a{stage}-{row['index']}"
                                     for stage in (1, 2)} for row in assignments}
        attempts = {stage: {index: _read_result(args.artifacts / names[stage] / 'result.json')
                            for index, names in sources.items() if stage in names}
                    for stage in (1, 2)}
        result = assess_attempts(plan, assignments, attempts)
        result['artifact_sources'] = sources
        if args.command == 'retries':
            include = [row for row in matrix['include'] if row['index'] in result['retry_indices']]
            # Keep a valid nonempty expression even when the job's `if` skips the matrix.
            output = {'include': include or matrix['include'][:1]}
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(output) + '\n')
            print('retry=' + str(bool(include)).lower())
            print('matrix=' + json.dumps(output, separators=(',', ':')))
            return 0
        if args.effective_output:
            for index, attempt in result['effective_attempts'].items():
                source = args.artifacts / sources[index][attempt]
                target = args.effective_output / str(index)
                target.mkdir(parents=True, exist_ok=True)
                for name in ('result.json', 'junit.xml'):
                    if (source / name).is_file():
                        shutil.copyfile(source / name, target / name)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + '\n')
        marker = {0: 'GATE_PASS', 1: 'GATE_TEST_FAILURE', 75: 'NO_VERDICT'}[result['exit_code']]
        message = f"{marker}: failed shards {result['failed_indices']}; missing verdicts {result['missing_indices']}"
        print(message)
        if args.summary:
            with args.summary.open('a') as handle:
                handle.write('## Gate verdict\n\n' + message + '\n')
        return result['exit_code']
    except (OSError, ValueError, KeyError, TypeError) as error:
        print('NO_VERDICT: ' + str(error), file=sys.stderr)
        return 75


if __name__ == '__main__':
    raise SystemExit(main())

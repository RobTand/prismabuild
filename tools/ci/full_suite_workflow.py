#!/usr/bin/env python3
"""Hosted nightly planning and artifact adapters around the pure reporter."""
from __future__ import annotations

import argparse
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import subprocess
import urllib.error
import urllib.request
import zipfile


def sibling(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


report = sibling('full_suite_report')
gate_select = sibling('gate_select')
gate_partition = sibling('gate_partition')
RUNNERS = 'ubuntu-24.04,ubuntu-24.04-arm'


def cpu_plan(root: Path, lanes: dict, sha: str, *, environment_lanes=()) -> dict:
    excluded = {name for name, lane in lanes.items() if lane['lane'] in {'root', 'gpu'}}
    deferred = list(environment_lanes)
    excluded.update(row['target'] for row in deferred if '::' not in row['target'])
    files = sorted(p.relative_to(root).as_posix() for p in (root / 'tests').rglob('test_*.py')
                   if p.relative_to(root).as_posix() not in excluded)
    return {'schema': 'prismabuild.gate_plan.v1', 'mode': 'full-suite',
            'base_sha': sha, 'head_sha': sha,
            'deselect': sorted(row['target'] for row in deferred if '::' in row['target']),
            'deferred': deferred,
            'population': {'version': sha}, 'selected': [{'path': name} for name in files]}


def collect_shards(directory: Path, manifest: dict) -> dict:
    tests = {}
    for entry in manifest['include']:
        folder = directory / str(entry['index'])
        result = json.loads((folder / 'result.json').read_text())
        if (result.get('verdict') not in {'pass', 'fail'} or result.get('head_sha') != manifest['sha']
                or sorted(result.get('files', [])) != sorted(entry['files'])):
            raise ValueError('shard has no complete verdict or mismatched identity')
        rows = report.parse_junit((folder / 'junit.xml').read_bytes())
        if not rows:
            raise ValueError('shard JUnit contains no outcomes')
        if tests.keys() & rows.keys():
            raise ValueError('duplicate tests across shards')
        for row in rows.values():
            row['runner'] = entry['runner']
        tests.update(rows)
    return tests


def artifact_results(api: report.IssueAPI, artifact_id: int) -> dict:
    # The authorization header must never follow GitHub's signed-URL redirect.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None
    request = urllib.request.Request(api.root + f'actions/artifacts/{artifact_id}/zip',
                                     headers={'Authorization': f'Bearer {api.token}'})
    try:
        urllib.request.build_opener(NoRedirect).open(request, timeout=60)
    except urllib.error.HTTPError as exc:
        if exc.code != 302:
            raise
        location = exc.headers['Location']
    else:
        raise ValueError('artifact response did not redirect')
    if not location.startswith('https://'):
        raise ValueError('artifact download must use HTTPS')
    with urllib.request.urlopen(location, timeout=60) as response:
        archive = response.read(64 * 1024 * 1024 + 1)
    if len(archive) > 64 * 1024 * 1024:
        raise ValueError('artifact exceeds 64 MiB')
    with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
        info = zipped.getinfo('results.json')
        if info.file_size > 256 * 1024 * 1024:
            raise ValueError('results exceed 256 MiB')
        return json.loads(zipped.read(info))


def previous_results(api: report.IssueAPI, current_run: int) -> dict:
    try:
        runs = api.pages('actions/workflows/full-suite.yml/runs?branch=main&status=completed', 'workflow_runs')
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return {}
        raise
    for run in sorted(runs, key=lambda r: r['run_number'], reverse=True):
        if run['id'] == current_run:
            continue
        artifacts = api.pages(f"actions/runs/{run['id']}/artifacts", 'artifacts')
        artifact = next((a for a in artifacts if a['name'] == 'full-suite-results' and not a['expired']), None)
        if artifact:
            result = artifact_results(api, artifact['id'])
            if result.get('complete') and not result.get('dry_run'):
                return result
    return {}


def write_report(directory: Path, manifest: dict, output: Path, dry_run: bool) -> int:
    api = report.GitHub.from_environment(os.environ['GITHUB_REPOSITORY'])
    previous = previous_results(api, int(os.environ['GITHUB_RUN_ID']))
    complete = True
    try:
        tests = collect_shards(directory, manifest)
    except (OSError, ValueError, KeyError, report.ET.ParseError):
        complete, tests = False, {}
    quarantine = report.quarantine_entries(Path('tools/ci/gate_quarantine.json'))
    issues = api.pages('issues?state=open&labels=full-suite') if complete else []
    prs = report.merged_prs(api, previous.get('sha'), manifest['sha']) if complete and not dry_run else []
    run_url = f"https://github.com/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
    result = report.build_report(tests, previous, quarantine, issues, manifest['sha'], prs, run_url, complete=complete)
    result['dry_run'] = dry_run
    result['filed'] = report.apply_plan(api, result['plans']) if complete and not dry_run else []
    result['shards'] = manifest['include']
    serialized = json.dumps(result, sort_keys=True)
    token = os.environ.get('GITHUB_TOKEN')
    if token:
        serialized = serialized.replace(token, '[REDACTED]')
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(serialized + '\n')
    summary = {'complete': complete, 'dry_run': dry_run,
               'classification': {key: len(nodes) for key, nodes in result['classification'].items()},
               'filed': result['filed']}
    print(json.dumps(summary, sort_keys=True))
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as handle:
            handle.write('## Full suite\n\n```json\n' + json.dumps(summary, indent=2) + '\n```\n')
    return 2 if not complete else int(any(row['outcome'] == 'failed' for row in tests.values()))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    plan = commands.add_parser('plan')
    plan.add_argument('--output', type=Path, required=True)
    plan.add_argument('--sha', required=True)
    plan.add_argument('--shards', type=int)
    plan.add_argument('--population-only', action='store_true')
    collect = commands.add_parser('report')
    collect.add_argument('--input', type=Path, required=True)
    collect.add_argument('--manifest', type=Path, required=True)
    collect.add_argument('--output', type=Path, required=True)
    collect.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)
    if args.command == 'report':
        return write_report(args.input, json.loads(args.manifest.read_text()), args.output, args.dry_run)
    root = Path.cwd()
    _, lanes = gate_select.tier_declarations(root)
    quarantine = json.loads((root / 'tools/ci/gate_quarantine.json').read_text()) if (root / 'tools/ci/gate_quarantine.json').exists() else {}
    capabilities = quarantine.get('environment_lanes', []) + [dict(row, lane='archival_replay')
        for row in quarantine.get('entries', []) if row.get('issue') == 1997]
    population = cpu_plan(root, lanes, args.sha, environment_lanes=capabilities)
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'plan.json').write_text(json.dumps(population) + '\n')
    files = [row['path'] for row in population['selected']]
    (args.output / 'files.txt').write_text('\n'.join(files) + '\n')
    if args.population_only:
        return 0
    if not args.shards:
        parser.error('--shards is required for matrix planning')
    partition = Path(__file__).with_name('gate_partition.py')
    common = ['--files-from', str(args.output / 'files.txt'), '--durations',
              str(root / 'tools/ci/gate_durations.json'), '--shards', str(args.shards)]
    matrix_path = args.output / 'matrix.json'
    subprocess.run([sys.executable, str(partition), 'matrix', *common,
                    '--runners', RUNNERS, '--output', str(matrix_path)], check=True)
    matrix = json.loads(matrix_path.read_text())
    include = []
    for row in matrix['include']:
        assigned = args.output / 'assignment.txt'
        subprocess.run([sys.executable, str(partition), 'shard', *common,
                        '--index', str(row['index']), '--output-files', str(assigned)], check=True)
        include.append(dict(row, files=assigned.read_text().splitlines()))
        assigned.unlink()
    (args.output / 'manifest.json').write_text(json.dumps({'sha': args.sha, 'include': include}) + '\n')
    with open(os.environ['GITHUB_OUTPUT'], 'a') as handle:
        handle.write('matrix=' + json.dumps({'include': matrix['include']}) + '\n')
        handle.write('shards=' + str(matrix['shards']) + '\n')


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError, zipfile.BadZipFile, report.ET.ParseError) as exc:
        print(f'Full-suite CI could not complete ({type(exc).__name__}).', file=sys.stderr)
        raise SystemExit(2)

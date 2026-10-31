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
ARCHITECTURES = {'x64': 'ubuntu-latest', 'arm64': 'ubuntu-24.04-arm'}


def cpu_plan(root: Path, lanes: dict, sha: str) -> dict:
    excluded = {name for name, lane in lanes.items() if lane['lane'] in {'root', 'gpu'}}
    files = sorted(p.relative_to(root).as_posix() for p in (root / 'tests').rglob('test_*.py')
                   if p.relative_to(root).as_posix() not in excluded)
    return {'schema': 'prismabuild.gate_plan.v1', 'mode': 'full-suite',
            'base_sha': sha, 'head_sha': sha, 'deselect': [],
            'population': {'version': sha}, 'selected': [{'path': name} for name in files]}


def combine_architectures(rows: dict, architectures: list[str]) -> dict:
    combined = {}
    for node in sorted({node for tests in rows.values() for node in tests}):
        outcomes = [rows[arch].get(node, {}).get('outcome', 'skipped') for arch in architectures]
        outcome = 'failed' if 'failed' in outcomes else ('passed' if set(outcomes) == {'passed'} else 'skipped')
        combined[node] = {
            'outcome': outcome,
            'duration': sum(rows[arch].get(node, {}).get('duration', 0) for arch in architectures),
            'failure': '\n'.join(f'[{arch}]\n{rows[arch][node]["failure"]}'
                                 for arch in architectures if rows[arch].get(node, {}).get('outcome') == 'failed'),
            'architectures': {arch: rows[arch].get(node, {}).get('outcome', 'absent') for arch in architectures}}
    return combined


def collect_shards(directory: Path, manifest: dict) -> dict:
    by_arch = {entry['arch']: {} for entry in manifest['include']}
    for entry in manifest['include']:
        folder = directory / f"full-suite-shard-{entry['arch']}-{entry['shard']}"
        result = json.loads((folder / 'result.json').read_text())
        if (result.get('verdict') not in {'pass', 'fail'} or result.get('head_sha') != manifest['sha']
                or sorted(result.get('files', [])) != sorted(entry['files'])):
            raise ValueError('shard has no complete verdict or mismatched identity')
        rows = report.parse_junit((folder / 'junit.xml').read_bytes())
        if not rows:
            raise ValueError('shard JUnit contains no outcomes')
        previous = by_arch[entry['arch']]
        if previous.keys() & rows.keys():
            raise ValueError('duplicate tests across shards')
        previous.update(rows)
    return combine_architectures(by_arch, sorted(by_arch))


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
    plan.add_argument('--shards', type=int, default=8)
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
    population = cpu_plan(root, lanes, args.sha)
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'plan.json').write_text(json.dumps(population) + '\n')
    # The gate owns the duration-balancing implementation shared by both workflows.
    assignments = gate_partition.balanced_shards(population, gate_select.read_json(
        root / 'tools/ci/gate_durations.json'), args.shards)
    include = []
    for arch, runner in ARCHITECTURES.items():
        for shard, files in enumerate(assignments):
            if files:
                include.append({'arch': arch, 'runner': runner, 'shard': shard, 'files': files})
                (args.output / f'{arch}-{shard}.txt').write_text('\n'.join(files) + '\n')
    manifest = {'sha': args.sha, 'include': include}
    (args.output / 'manifest.json').write_text(json.dumps(manifest) + '\n')
    matrix = {'include': [{k: row[k] for k in ['arch', 'runner', 'shard']} for row in include]}
    with open(os.environ['GITHUB_OUTPUT'], 'a') as handle:
        handle.write('matrix=' + json.dumps(matrix) + '\n')


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError, zipfile.BadZipFile, report.ET.ParseError) as exc:
        print(f'Full-suite CI could not complete ({type(exc).__name__}).', file=sys.stderr)
        raise SystemExit(2)

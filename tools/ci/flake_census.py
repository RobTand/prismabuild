#!/usr/bin/env python3
"""Classify repeated hosted main test results without retrying test failures."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
from pathlib import Path
import subprocess
import sys

SPEC = importlib.util.spec_from_file_location('census_nightly', Path(__file__).with_name('full_suite_workflow.py'))
nightly = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(nightly)
report = nightly.report
ARCHITECTURES = {'x64': 'ubuntu-24.04', 'arm64': 'ubuntu-24.04-arm'}


def classify_samples(samples, repeats, quarantine):
    """Count observations, keeping architecture differences distinct from flakes."""
    if type(repeats) is not int or not 2 <= repeats <= 10:
        raise ValueError('repeats must be an integer from 2 to 10')
    observed = {}
    for sample in samples:
        arch, repeat = sample['architecture'], sample['repeat']
        if (arch not in ARCHITECTURES or type(repeat) is not int or not 0 <= repeat < repeats
                or (arch, repeat) in observed):
            raise ValueError('duplicate or unexpected census sample identity')
        observed[arch, repeat] = sample
    groups = {key: [] for key in ('intermittent', 'deterministic_failure', 'architecture_difference',
              'passing', 'unresolved', 'quarantined', 'ready_for_retirement', 'environmental')}
    tests = {}
    for node in sorted({node for sample in samples for node in sample['tests']}):
        counts, failures, causes = {}, [], set()
        for arch in ARCHITECTURES:
            count = dict.fromkeys(('passed', 'failed', 'skipped', 'missing'), 0)
            for repeat in range(repeats):
                sample = observed.get((arch, repeat), {})
                row = sample.get('tests', {}).get(node)
                outcome = 'missing' if row is None else row['outcome']
                if outcome not in count:
                    raise ValueError('unexpected test outcome')
                count[outcome] += 1
                if outcome == 'failed':
                    warning = re.search(r'pbrun: queue space warning: [0-9]+ available bytes; [0-9]+ available inodes', row['failure'])
                    excerpt = '\n'.join(row['failure'].splitlines()[:20])[:4000]
                    if warning:
                        causes.add('queue_space_warning')
                        if warning.group() not in excerpt:
                            excerpt += '\n...\n' + warning.group()
                    failures.append({'architecture': arch, 'repeat': repeat,
                        'excerpt': excerpt,
                        'run_url': sample['run_url'], 'disk': row.get('disk', sample.get('disk'))})
            counts[arch] = count
        values = list(counts.values())
        if any(c['passed'] and c['failed'] for c in values):
            status = 'intermittent'
        elif all(c['failed'] == repeats for c in values):
            status = 'deterministic_failure'
        elif any(c['failed'] == repeats for c in values) and any(c['passed'] == repeats for c in values):
            status = 'architecture_difference'
        elif all(c['passed'] == repeats for c in values):
            status = 'passing'
        else:
            status = 'unresolved'
        owners = sorted({entry['issue'] for entry in quarantine
                         if node == entry['target'] or node.startswith((entry['target'] + '::', entry['target'] + '['))})
        tests[node] = {'counts': counts, 'status': status, 'owners': owners, 'failures': failures, 'causes': sorted(causes)}
        groups['quarantined' if owners else 'environmental' if causes else status].append(node)
        if owners and status == 'passing':
            groups['ready_for_retirement'].append(node)
    return {'repeats': repeats, 'tests': tests, 'classification': groups,
            'disk_samples': [disk for sample in samples for disk in sample.get('disk_samples', [])],
            'environmental_causes': {'queue_space_warning': {'issues': list(range(2508, 2515)),
                'description': 'Queue filesystem below 16 GiB prints a warning before the queued line.'}}}


def census_owners(issues):
    """Share the nightly's positioned marker parser, including unlabeled owners."""
    owned = {}
    for issue in sorted(issues, key=lambda row: row['number']):
        if not issue.get('pull_request'):
            for node in report.ownership_nodes(issue.get('body') or ''):
                owned.setdefault(node, issue['number'])
    return owned


def census_issue_plans(result, issues):
    if not result['complete']:
        return []
    owned, plans = census_owners(issues), []
    for node in result['classification']['intermittent']:
        if node in owned:
            continue
        row = result['tests'][node]
        proposed = {'target': node, 'issue': '<new issue number>',
            'reason': 'Intermittent on hosted CI at ' + result['sha'] + '; repair the nondeterministic test.',
            'evidence': result['run_url']}
        body = (report.marker(node) + '\n\nIntermittent test: ' + report.literal_report_text(node)
            + f"\n\nMain SHA: {result['sha']}\nCensus: {result['run_url']}\n\n"
            + 'Both passed and failed in independent repeats on the same architecture.\n\n')
        body += '\n'.join(f"- {arch}: {count['passed']} passed, {count['failed']} failed, "
                            f"{count['skipped']} skipped, {count['missing']} missing"
                            for arch, count in row['counts'].items())
        for failure in row['failures']:
            body += (f"\n\n{failure['architecture']}, repeat {failure['repeat'] + 1}: {failure['run_url']}\n\n"
                + '\n'.join('    ' + report.safe_text(line) for line in failure['excerpt'].splitlines()))
        body += ('\n\nProposed tools/ci/gate_quarantine.json entry. Replace the placeholder with '
                 'this issue number and review before applying:\n\n'
                 + '\n'.join('    ' + line for line in json.dumps(proposed, indent=2).replace(chr(64), r'\u0040').splitlines()))
        # Keep the ID exact while emitting no literal mention in the Bash command.
        node_argument = "'" + node.replace("'", "'\"'\"'").replace(chr(64), r"'$'\x40''") + "'"
        command = ('python3 /mnt/shared/prismabuild-fleet/repo/tools/pbtest.py '
                   '--checkout <clean-checkout> --python /home/rob/venvs/pb-cpu/bin/python '
                   '--workers-per-shard 1 --threads-per-shard 1 --mem-gb 4 --priority -10 ' + node_argument)
        body += '\n\nReproduce through PrismaBuild in Bash at the main SHA above:\n\n    ' + report.safe_text(command) + '\n'
        plans.append({'title': report.safe_text('[P2] Flake census: ' + node)[:256],
                      'body': body, 'labels': [report.LABEL]})
    return plans


def file_new_issues(api, plans):
    """Recheck owners before each create; never comment on an existing owner."""
    if not plans:
        return []
    labels = {row['name'] for row in api.pages('labels')}
    filed = []
    for plan in plans:
        owned = census_owners(api.pages('issues?state=open'))
        if any(node in owned for node in report.ownership_nodes(plan['body'])):
            continue
        payload = dict(plan, labels=[label for label in (report.LABEL, 'P2') if label in labels])
        issue = api.request('issues', payload)
        filed.append({'number': issue['number'], 'url': issue['html_url']})
    return filed


def census_manifest(root, sha, repeats, shard_count, *, quarantine_issue=None):
    root = Path(root)
    if type(repeats) is not int or not 2 <= repeats <= 10:
        raise ValueError('repeats must be an integer from 2 to 10')
    if type(shard_count) is not int or not 1 <= shard_count <= 8:
        raise ValueError('shards per architecture must be from 1 to 8')
    quarantine = json.loads((root / 'tools/ci/gate_quarantine.json').read_text())
    policy = dict(quarantine, entries=[row for row in quarantine.get('entries', []) if row['issue'] == 1997],
                  cost_exclusions=[])
    durations = json.loads((root / 'tools/ci/gate_durations.json').read_text())
    plan = nightly.gate_select.create_plan(root, ['.github/workflows/gate.yml'],
        durations=durations, quarantine=policy, mode='full')
    plan.update(base_sha=sha, head_sha=sha)
    if quarantine_issue is not None:
        entries = quarantine.get('entries', []) + quarantine.get('cost_exclusions', [])
        targets = {row['target'].split('::')[0] for row in entries if row['issue'] == quarantine_issue}
        if type(quarantine_issue) is not int or quarantine_issue <= 0 or not targets:
            raise ValueError('unknown or invalid quarantine owner')
        unavailable = targets - {row['path'] for row in plan['selected']}
        if unavailable:
            raise ValueError('quarantine owner targets unavailable in hosted census: ' + ', '.join(sorted(unavailable)))
        plan['selected'] = [row for row in plan['selected'] if row['path'] in targets]
        plan['shadow_selected'] = [row for row in plan['shadow_selected'] if row['path'] in targets]
    files = [row['path'] for row in plan['selected']]
    plan['estimate'] = nightly.gate_select.estimate(files, durations)
    count = min(shard_count, len(files)) if quarantine_issue is not None else shard_count
    partitions = nightly.gate_partition.shards(files, durations['files'], count,
        unknown_seconds=nightly.gate_partition.default_unknown_seconds(durations['files']))
    include = []
    for repeat in range(repeats):
        for arch, runner in ARCHITECTURES.items():
            for partition in partitions:
                include.append({'index': len(include), 'repeat': repeat, 'architecture': arch,
                    'runner': runner, 'shard': partition['index'], 'files': partition['files']})
    return plan, {'schema': 1, 'sha': sha, 'repeats': repeats, 'shards': len(partitions), 'include': include,
                  **({'quarantine_issue': quarantine_issue} if quarantine_issue is not None else {}),
                  'quarantine': quarantine.get('entries', []) + quarantine.get('cost_exclusions', []),
                  'deferred': plan['deferred']}


def collect_census(directory, manifest, run_url, artifact_prefix=''):
    """Keep independently validated shard observations when other shards are unavailable."""
    samples, errors, indices = {}, [], set()
    expected = {(arch, repeat) for arch in ARCHITECTURES for repeat in range(manifest['repeats'])}
    for entry in manifest['include']:
        key = (entry['architecture'], entry['repeat'])
        identity = {name: entry[name] for name in ('index', 'architecture', 'repeat', 'files')}
        if entry['index'] in indices or key not in expected:
            errors.append(dict(identity, stage='assignment', error='duplicate or unexpected shard identity'))
            continue
        indices.add(entry['index'])
        source = dict(entry, index=artifact_prefix + str(entry['index']))
        sample = samples.setdefault(key, {'architecture': key[0], 'repeat': key[1], 'tests': {},
            'run_url': run_url, 'disk_samples': []})
        disk = None
        try:
            available, inodes, tmpdir = (directory / str(source['index']) / 'disk.txt').read_text().strip().split(maxsplit=2)
            disk = {'available_bytes': int(available), 'available_inodes': int(inodes), 'tmpdir': tmpdir,
                    'architecture': key[0], 'repeat': key[1], 'shard': entry['index'], 'run_url': run_url}
            if disk['available_bytes'] < 0 or disk['available_inodes'] < 0 or not tmpdir.startswith('/'):
                raise ValueError('invalid shard disk evidence')
            sample['disk_samples'].append(disk)
        except (OSError, ValueError) as error:
            disk = None
            errors.append(dict(identity, stage='disk', error=str(error)))
        try:
            tests = nightly.collect_shards(directory, {'sha': manifest['sha'], 'include': [source]})
            if sample['tests'].keys() & tests.keys():
                raise ValueError('duplicate tests across census shards')
        except (OSError, ValueError, KeyError, report.ET.ParseError) as error:
            errors.append(dict(identity, stage='results', error=str(error)))
            continue
        for row in tests.values():
            row['disk'] = disk
        sample['tests'].update(tests)
    for arch, repeat in sorted(expected - samples.keys()):
        errors.append({'architecture': arch, 'repeat': repeat, 'stage': 'assignment',
                       'error': 'incomplete architecture/repeat coverage'})
    return {'samples': list(samples.values()), 'errors': errors, 'complete': not errors}


def census_summary(result):
    lines = ['# Flake census', '', f"Main SHA: {result['sha']}",
             f"Complete: {result['complete']}; dry run: {result['dry_run']}; samples per architecture: {result['repeats']}",
             '', 'Full counts, excerpts and sample URLs are in the flake-census-results artifact.', '']
    if result.get('error'):
        lines += ['Incomplete: ' + report.literal_report_text(result['error']), '']
        for error in result.get('errors', [])[:40]:
            label = f"shard {error['index']}" if 'index' in error else f"{error['architecture']} repeat {error['repeat'] + 1}"
            lines.append('- ' + label + ' (' + error['stage'] + '): ' + report.literal_report_text(error['error']))
        lines.append('')
    if result['disk_samples']:
        lines += ['## Runner disk under TMPDIR before tests', '',
                  '| Architecture | Repeat | Shard | Available bytes | Available inodes | TMPDIR |',
                  '| --- | --- | --- | --- | --- | --- |']
        for disk in result['disk_samples']:
            lines.append(f"| {disk['architecture']} | {disk['repeat'] + 1} | {disk['shard']} | "
                         f"{disk['available_bytes']} | {disk['available_inodes']} | "
                         + report.literal_report_text(disk['tmpdir']) + ' |')
        lines.append('')
    lines += ['Queue-space-warning failures share the environmental cause tracked in #2508 through #2514. '
              'They do not create individual census issues.', '']
    for group, nodes in result['classification'].items():
        lines.append(f'## {group}: {len(nodes)}')
        # Bound GitHub's summary independently of suite size.
        if group != 'passing':
            for node in nodes[:40]:
                row = result['tests'][node]
                counts = '; '.join(f"{arch} {c['passed']}P/{c['failed']}F/{c['skipped']}S/{c['missing']}M"
                                   for arch, c in row['counts'].items())
                owner = result.get('open_owners', {}).get(node)
                details = ('; open owner #' + str(owner)) if owner else ''
                if row['causes']:
                    details += '; cause: ' + ', '.join(row['causes'])
                lines.append('- ' + report.literal_report_text(node) + ': ' + counts + details)
            if len(nodes) > 40:
                lines.append(f'{len(nodes) - 40} more in the artifact.')
        lines.append('')
    return '\n'.join(lines) + '\n'


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    plan = commands.add_parser('plan')
    plan.add_argument('--root', type=Path, required=True)
    plan.add_argument('--repeats', type=int, default=5)
    plan.add_argument('--shards', type=int, default=4)
    plan.add_argument('--quarantine-issue', type=int, help='repeat only files owned by this quarantine issue')
    plan.add_argument('--output', type=Path, required=True)
    assign = commands.add_parser('assign')
    assign.add_argument('--root', type=Path, required=True)
    assign.add_argument('--manifest', type=Path, required=True)
    assign.add_argument('--index', type=int, required=True)
    assign.add_argument('--output', type=Path, required=True)
    finish = commands.add_parser('report')
    finish.add_argument('--manifest', type=Path, required=True)
    finish.add_argument('--input', type=Path, required=True)
    finish.add_argument('--artifact-prefix', default='')
    finish.add_argument('--output', type=Path, required=True)
    finish.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)
    if args.command == 'plan':
        sha = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=args.root,
                             check=True, capture_output=True, text=True).stdout.strip()
        population, manifest = census_manifest(args.root, sha, args.repeats, args.shards,
                                               quarantine_issue=args.quarantine_issue)
        args.output.mkdir(parents=True, exist_ok=True)
        (args.output / 'plan.json').write_text(json.dumps(population) + '\n')
        (args.output / 'manifest.json').write_text(json.dumps(manifest) + '\n')
        matrix = {'include': [{k: v for k, v in row.items() if k != 'files'} for row in manifest['include']]}
        with open(os.environ['GITHUB_OUTPUT'], 'a') as handle:
            handle.write('sha=' + sha + '\nmatrix=' + json.dumps(matrix) + '\n')
        return 0
    manifest = json.loads(args.manifest.read_text())
    if args.command == 'assign':
        sha = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=args.root,
                             check=True, capture_output=True, text=True).stdout.strip()
        _, recomputed = census_manifest(args.root, sha, manifest['repeats'], manifest['shards'],
                                        quarantine_issue=manifest.get('quarantine_issue'))
        if recomputed != manifest:
            raise ValueError('main snapshot or assignment changed')
        row = next(row for row in recomputed['include'] if row['index'] == args.index)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text('\n'.join(row['files']) + '\n')
        return 0
    run_url = f"https://github.com/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
    result = {'sha': manifest['sha'], 'complete': False, 'dry_run': args.dry_run, 'run_url': run_url}
    collected = collect_census(args.input, manifest, run_url, args.artifact_prefix)
    result.update(classify_samples(collected['samples'], manifest['repeats'], manifest['quarantine']),
                  complete=collected['complete'], errors=collected['errors'])
    if result['errors']:
        result['error'] = f"{len(result['errors'])} evidence errors; valid observations retained."
    result['deferred'], result['shards'] = manifest['deferred'], manifest['include']
    result['quarantine_snapshot'] = manifest['quarantine']
    api = report.GitHub.from_environment(os.environ['GITHUB_REPOSITORY'])
    issues = api.pages('issues?state=open') if result['complete'] else []
    result['open_owners'] = {node: issue for node, issue in census_owners(issues).items()
                             if node in result.get('tests', {})}
    plans = census_issue_plans(result, issues)
    result['plans'] = plans
    result['filed'] = file_new_issues(api, plans) if not args.dry_run else []
    args.output.parent.mkdir(parents=True, exist_ok=True)
    summary = census_summary(result)
    token = os.environ.get('GITHUB_TOKEN')
    serialized = json.dumps(result)
    if token:
        serialized, summary = serialized.replace(token, '[REDACTED]'), summary.replace(token, '[REDACTED]')
    args.output.write_text(serialized + '\n')
    (args.output.parent / 'summary.md').write_text(summary)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as handle:
            handle.write(summary)
    print(json.dumps({'complete': result['complete'], 'dry_run': args.dry_run,
                     'classification': {key: len(nodes) for key, nodes in result['classification'].items()},
                     'filed': result['filed']}))
    return 0 if result['complete'] else 2


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError, report.ET.ParseError) as error:
        print(f'Census could not complete ({type(error).__name__}).', file=sys.stderr)
        raise SystemExit(2)

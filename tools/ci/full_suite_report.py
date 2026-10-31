#!/usr/bin/env python3
"""Compare full-suite artifacts and report new failures using GitHub REST only."""
from __future__ import annotations

import argparse
import html
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Protocol
import xml.etree.ElementTree as ET

LABEL = 'full-suite'
MARKER = re.compile(r'<!-- full-suite: (.*?) -->')


def safe_text(text: str) -> str:
    return str(text).replace('@', '＠')


def marker(node: str) -> str:
    return '<!-- full-suite: ' + html.escape(node).replace('@', '&#64;') + ' -->'


def classify(now: dict, before: dict, quarantine: set[str]) -> dict[str, list[str]]:
    groups = {'new': [], 'known': [], 'recovered': [], 'quarantined': []}
    for node, row in sorted(now.items()):
        old = before.get(node, {}).get('outcome')
        if row['outcome'] == 'failed':
            if any(node == q or node.startswith(q + '::') for q in quarantine):
                groups['quarantined'].append(node)
            else:
                groups['known' if old == 'failed' else 'new'].append(node)
        elif row['outcome'] == 'passed' and old == 'failed':
            groups['recovered'].append(node)
    return groups


def owners(issues: list[dict]) -> dict[str, int]:
    result = {}
    for issue in sorted(issues, key=lambda issue: issue['number']):
        if not issue.get('pull_request') and LABEL in [label['name'] for label in issue.get('labels', [])]:
            for node in MARKER.findall(issue.get('body') or ''):
                result.setdefault(html.unescape(node), issue['number'])
    return result


def issue_plan(now: dict, before: dict, quarantine: set[str], owned: dict[str, int],
               previous_sha: str, sha: str, prs: list[dict], run_url: str) -> list[dict]:
    groups = classify(now, before, quarantine)
    plans = []
    new = groups['new']
    pr_text = '\n'.join(f"- #{pr['number']}: {safe_text(pr['title'])}" for pr in prs) or '- None found.'

    def failure_body(node):
        base = before.get(node, {}).get('last_pass_sha') or previous_sha
        failure = safe_text('\n'.join(now[node]['failure'].splitlines()[:20]))[:4000]
        # Indentation keeps failure text from escaping into active issue Markdown.
        excerpt = '\n'.join('    ' + line for line in failure.splitlines())
        quoted = shlex.quote(node)
        command = ('python3 /mnt/shared/prismabuild-fleet/repo/tools/pbtest.py '
                   '--checkout <clean-checkout> --python /home/rob/venvs/pb-cpu/bin/python '
                   '--workers-per-shard 1 --threads-per-shard 1 --mem-gb 4 --priority -10 ' + quoted)
        return (f'{marker(node)}\n\nFailed at {sha}. Last passing commit or previous baseline: {base}.\n'
                f'Commit range: {base}...{sha}\n\nMerged PRs in the range:\n{pr_text}\n\n'
                f'Failure excerpt:\n\n{excerpt}\n\nRun: {run_url}\n\n'
                f'Reproduce through PrismaBuild from a clean checkout at {sha}:\n\n'
                f'    {safe_text(command)}\n')

    if len(new) > 20:
        # Every test marker owns its failure, including after the baseline recovers.
        body = ('More than 20 new failures may indicate infrastructure trouble.\n\n'
                + '\n'.join(marker(node) for node in new)
                + f'\n\nCommit range: {previous_sha}...{sha}\nRun: {run_url}\n\n'
                + f'Merged PRs:\n{pr_text}\n\n' + failure_body(new[0]))
        plans.append({'kind': 'issue', 'title': f'[P2] Full suite: {len(new)} new failures on main',
                      'body': body, 'labels': [LABEL]})
    else:
        for node in new:
            body = failure_body(node)
            if node in owned:
                plans.append({'kind': 'comment', 'number': owned[node], 'body': body})
            else:
                title = safe_text(f'[P2] Full suite: {node} fails on main')
                plans.append({'kind': 'issue', 'title': title[:256], 'body': body, 'labels': [LABEL]})
    for node in groups['recovered']:
        if node in owned:
            plans.append({'kind': 'comment', 'number': owned[node],
                          'body': f'{marker(node)}\n\n{safe_text(node)} passed at {sha}.\n\nRun: {run_url}\n'})
    return plans




class IssueAPI(Protocol):
    def pages(self, path: str, field: str | None = None) -> list: ...
    def request(self, path: str, data=None, method=None): ...


class GitHub:
    """Thin GitHub REST adapter. Tokens come from the caller environment."""

    def __init__(self, repository: str, token: str):
        if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repository):
            raise ValueError('invalid repository')
        self.root = f'https://api.github.com/repos/{repository}/'
        if not token:
            raise ValueError('workflow token is empty')
        self.token = token

    @classmethod
    def from_environment(cls, repository: str):
        return cls(repository, os.environ['GITHUB_TOKEN'])

    def request(self, path: str, data=None, method=None):
        payload = None if data is None else json.dumps(data).replace(self.token, '[REDACTED]').encode()
        request = urllib.request.Request(self.root + path, data=payload, method=method,
            headers={'Authorization': f'Bearer {self.token}', 'Accept': 'application/vnd.github+json',
                     'X-GitHub-Api-Version': '2022-11-28'})
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)

    def pages(self, path: str, field: str | None = None) -> list:
        result = []
        for page in range(1, 1001):
            separator = '&' if '?' in path else '?'
            value = self.request(f'{path}{separator}per_page=100&page={page}')
            rows = value[field] if field else value
            result.extend(rows)
            if len(rows) < 100:
                return result
        raise ValueError('REST pagination exceeded 1000 pages')





def merged_prs(api: IssueAPI, before: str | None, sha: str) -> list[dict]:
    if not before or before == sha:
        return []
    commits = api.pages(f'compare/{before}...{sha}', 'commits')
    prs = {}
    for commit in commits:
        for pr in api.pages(f"commits/{commit['sha']}/pulls"):
            if pr.get('merged_at') and pr['base']['ref'] == 'main':
                prs[pr['number']] = {'number': pr['number'], 'title': pr['title']}
    return [prs[n] for n in sorted(prs)]


def quarantine_entries(path: Path) -> set[str]:
    if not path.exists():
        return set()
    value = json.loads(path.read_text())
    # The gate owns this schema. Accept its list of records or keyed entries.
    entries = value.get('tests', value.get('quarantine', value)) if isinstance(value, dict) else value
    if isinstance(entries, dict):
        return set(entries)
    return {entry if isinstance(entry, str) else entry['nodeid'] for entry in entries}


def apply_plan(api: IssueAPI, plans: list[dict]) -> list[dict]:
    results = []
    if any(p['kind'] == 'issue' for p in plans):
        labels = {label['name'] for label in api.pages('labels')}
        if LABEL not in labels:
            api.request('labels', {'name': LABEL, 'color': 'd876e3',
                                   'description': 'Regressions found by the full CPU suite'})
        for plan in plans:
            if plan['kind'] == 'issue' and 'P2' in labels:
                plan['labels'] = [LABEL, 'P2']
    for plan in plans:
        if plan['kind'] == 'issue':
            # Reruns can happen after filing but before the artifact upload.
            # Look again at current owners before each create, without Search's lag.
            current = owners(api.pages('issues?state=open&labels=full-suite'))
            nodes = [html.unescape(node) for node in MARKER.findall(plan['body'])]
            existing = sorted({current[node] for node in nodes if node in current})
            if existing:
                for number in existing:
                    results.extend(apply_plan(api, [{'kind': 'comment', 'number': number,
                                                      'body': plan['body']}]))
                unowned = [node for node in nodes if node not in current]
                if not unowned:
                    continue
                plan = dict(plan, body='\n'.join(
                    line for line in plan['body'].splitlines()
                    if not MARKER.fullmatch(line) or html.unescape(
                        MARKER.fullmatch(line).group(1)) in unowned))
            payload = {k: v for k, v in plan.items() if k != 'kind'}
            result = api.request('issues', payload)
        else:
            path = f"issues/{plan['number']}/comments"
            if any(c.get('body') == plan['body'] for c in api.pages(path)):
                continue
            result = api.request(path, {'body': plan['body']})
        results.append({'number': result.get('number', plan.get('number')), 'url': result['html_url']})
    return results




def parse_junit(xml: str | bytes) -> dict[str, dict]:
    """Parse pytest JUnit without importing or executing the producing checkout."""
    root = ET.fromstring(xml)
    if root.tag not in {'testsuite', 'testsuites'}:
        raise ValueError('expected JUnit testsuite or testsuites')
    rows = {}
    for case in root.iter('testcase'):
        node = next((p.get('value') for p in case.findall('./properties/property')
                     if p.get('name') == 'nodeid'), None)
        if not node:
            filename = case.get('file')
            collection_error = (not filename and not case.get('classname')
                                and case.find('error') is not None)
            if collection_error:
                # Pytest collection errors identify the file in name alone.
                filename = case.get('name')
            if not filename:
                raise ValueError('JUnit needs an exact nodeid property or file attribute; '
                                 'use pytest -o junit_family=legacy')
            path = PurePosixPath(filename.removeprefix('./'))
            if path.is_absolute() or '..' in path.parts or path.suffix != '.py':
                raise ValueError('JUnit file must be a repository-relative Python path')
            module = str(path.with_suffix('')).replace('/', '.')
            classname = case.get('classname', '')
            if classname == module:
                classname = ''
            elif classname.startswith(module + '.'):
                classname = classname[len(module) + 1:]
            name = case.get('name')
            if not name:
                raise ValueError('JUnit testcase needs a name')
            node = str(path) if collection_error else '::'.join(
                [str(path), *(classname.split('.') if classname else []), name])
        duration = float(case.get('time', '0'))
        if not math.isfinite(duration) or duration < 0:
            raise ValueError('JUnit duration must be finite and nonnegative')
        failure = case.find('failure')
        if failure is None:
            failure = case.find('error')
        row = {'outcome': 'failed' if failure is not None else (
                   'skipped' if case.find('skipped') is not None else 'passed'),
               'duration': duration,
               'failure': '' if failure is None else (failure.text or failure.get('message', ''))}
        if node in rows:
            # Pytest can emit separate call and teardown failures for one item.
            previous = rows[node]
            row['duration'] += previous['duration']
            if previous['outcome'] == 'failed':
                row['outcome'] = 'failed'
                row['failure'] = previous['failure'] + '\n' + row['failure']
        rows[node] = row
    return rows


def read_results(directory: Path) -> dict[str, dict]:
    files = sorted(directory.rglob('*.xml'))
    if not files:
        raise ValueError('no JUnit XML files found')
    rows = {}
    for path in files:
        tests = parse_junit(path.read_bytes())
        if rows.keys() & tests.keys():
            raise ValueError('a test appeared in multiple JUnit files')
        rows.update(tests)
    if not rows:
        raise ValueError('JUnit contains no test outcomes')
    return rows


def build_report(now: dict, previous: dict, quarantine: set[str], issues: list[dict],
                 sha: str, prs: list[dict], run_url: str, *, complete: bool = True) -> dict:
    """Build an outcome snapshot and issue plan. This function performs no I/O."""
    if previous and (not previous.get('complete') or previous.get('dry_run')):
        raise ValueError('previous results must be complete and previously reported')
    before = previous.get('tests', {})
    tests = {node: dict(row, last_pass_sha=(sha if row['outcome'] == 'passed'
                         else before.get(node, {}).get('last_pass_sha')))
             for node, row in now.items()}
    groups = classify(tests, before, quarantine) if complete else {
        'new': [], 'known': [], 'recovered': [], 'quarantined': []}
    plans = issue_plan(tests, before, quarantine, owners(issues),
                       previous.get('sha') or 'no previous baseline', sha, prs, run_url) if complete else []
    return {'schema': 1, 'complete': complete, 'sha': sha, 'run_url': run_url,
            'tests': tests, 'classification': groups, 'plans': plans}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('--input', type=Path, required=True, help='Directory of JUnit XML files')
    parser.add_argument('--previous', type=Path, help='Last complete, reported results.json')
    parser.add_argument('--output', type=Path, required=True, help='Output results.json file')
    parser.add_argument('--sha', required=True)
    parser.add_argument('--run-url', required=True)
    parser.add_argument('--repository', default=os.environ.get('GITHUB_REPOSITORY'))
    parser.add_argument('--quarantine', type=Path, default=Path('tools/ci/gate_quarantine.json'))
    parser.add_argument('--issues', type=Path, help='Offline open-issue snapshot for dry runs')
    parser.add_argument('--merged-prs', type=Path, help='Offline merged-PR metadata')
    parser.add_argument('--incomplete', action='store_true', help='Do not file or claim recoveries')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)
    previous = json.loads(args.previous.read_text()) if args.previous else {}
    tests = read_results(args.input) if not args.incomplete else {}
    api = None
    issues = json.loads(args.issues.read_text()) if args.issues else []
    prs = json.loads(args.merged_prs.read_text()) if args.merged_prs else []
    if not args.dry_run and not args.incomplete:
        if not args.repository:
            parser.error('--repository is required for filing')
        api = GitHub.from_environment(args.repository)
        issues = api.pages('issues?state=open&labels=full-suite')
        if not args.merged_prs and classify(tests, previous.get('tests', {}),
                                           quarantine_entries(args.quarantine))['new']:
            prs = merged_prs(api, previous.get('sha'), args.sha)
    result = build_report(tests, previous, quarantine_entries(args.quarantine), issues,
                          args.sha, prs, args.run_url, complete=not args.incomplete)
    result['dry_run'] = args.dry_run
    result['filed'] = apply_plan(api, result['plans']) if api else []
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Report artifacts must not contain the publishing credential either.
    serialized = json.dumps(result, sort_keys=True)
    token = os.environ.get('GITHUB_TOKEN')
    if token:
        serialized = serialized.replace(token, '[REDACTED]')
    args.output.write_text(serialized + '\n')
    print(json.dumps({'complete': result['complete'], 'dry_run': args.dry_run,
                      'classification': {k: len(v) for k, v in result['classification'].items()},
                      'actions': len(result['plans'])}, sort_keys=True))
    return 2 if args.incomplete else int(any(r['outcome'] == 'failed' for r in tests.values()))


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, ET.ParseError) as exc:
        print(f'Full-suite report could not complete ({type(exc).__name__}).', file=sys.stderr)
        raise SystemExit(2)

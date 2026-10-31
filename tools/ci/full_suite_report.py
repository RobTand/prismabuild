#!/usr/bin/env python3
"""Compare full-suite artifacts and report new failures using GitHub REST only."""
from __future__ import annotations

import argparse
import html
import io
import json
import os
from pathlib import Path
import re
import shlex
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile

LABEL = 'full-suite'
ARTIFACT = 'full-suite-results'
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


def previous_run(runs: list[dict], run_id: int, run_number: int) -> dict | None:
    eligible = [run for run in runs if run['id'] != run_id and run['run_number'] < run_number
                and run['head_branch'] == 'main' and run['status'] == 'completed']
    return max(eligible, key=lambda run: run['run_number'], default=None)


def collect_results(directory: Path, count: int) -> dict:
    shards = {}
    for path in sorted(directory.rglob('result.json')):
        row = json.loads(path.read_text())
        index = row['shard']
        if not 0 <= index < count:
            raise ValueError('unexpected shard index')
        if index not in shards or not shards[index]['verdict']:
            shards[index] = row
        elif row['verdict']:
            raise ValueError('two test verdicts for one shard')
    missing = [i for i in range(count) if i not in shards or not shards[i]['verdict']]
    tests = {}
    for row in shards.values():
        overlap = tests.keys() & row['tests'].keys()
        if overlap:
            raise ValueError('tests appeared in multiple shards')
        tests.update(row['tests'])
    return {'complete': not missing, 'missing_shards': missing, 'tests': tests,
            'shards': [{k: v for k, v in row.items() if k != 'tests'} for _, row in sorted(shards.items())]}


class GitHub:
    """Bounded REST requests. Authorization is never sent to artifact redirect hosts."""

    def __init__(self, repository: str, token: str):
        if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repository):
            raise ValueError('invalid repository')
        self.root = f'https://api.github.com/repos/{repository}/'
        if not token:
            raise ValueError('workflow token is empty')
        self.token = token

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

    def artifact(self, artifact_id: int) -> dict:
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs):
                return None
        request = urllib.request.Request(self.root + f'actions/artifacts/{artifact_id}/zip',
            headers={'Authorization': f'Bearer {self.token}'})
        try:
            urllib.request.build_opener(NoRedirect).open(request, timeout=60)
        except urllib.error.HTTPError as exc:
            if exc.code != 302:
                raise
            location = exc.headers['Location']
        else:
            raise ValueError('artifact endpoint did not redirect')
        if urllib.parse.urlparse(location).scheme != 'https':
            raise ValueError('artifact redirect must use HTTPS')
        # The signed URL is secret too. Do not log it, including in HTTP errors.
        try:
            with urllib.request.urlopen(location, timeout=60) as response:
                archive = response.read(64 * 1024 * 1024 + 1)
        except Exception:
            raise ValueError('artifact download failed') from None
        if len(archive) > 64 * 1024 * 1024:
            raise ValueError('artifact exceeds 64 MiB')
        with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
            info = zipped.getinfo('results.json')
            if info.file_size > 256 * 1024 * 1024:
                raise ValueError('results exceed 256 MiB')
            return json.loads(zipped.read(info))


def load_history(api: GitHub, run_id: int, run_number: int) -> dict:
    runs = api.pages('actions/workflows/full-suite.yml/runs?branch=main&status=completed', 'workflow_runs')
    # An incomplete run cannot replace the last usable baseline. Expired artifacts
    # are explicit bootstrap gaps, rather than evidence that every old test passed.
    while (run := previous_run(runs, run_id, run_number)) is not None:
        artifacts = api.pages(f"actions/runs/{run['id']}/artifacts", 'artifacts')
        match = next((a for a in artifacts if a['name'] == ARTIFACT and not a['expired']), None)
        if match:
            history = api.artifact(match['id'])
            if history['complete'] and not history.get('dry_run', False):
                return history
        runs = [r for r in runs if r['id'] != run['id']]
    return {'tests': {}, 'sha': None, 'complete': False}


def merged_prs(api: GitHub, before: str | None, sha: str) -> list[dict]:
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


def apply_plan(api: GitHub, plans: list[dict]) -> list[dict]:
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


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--shards', type=int, default=4)
    parser.add_argument('--quarantine', type=Path, default=Path('tools/ci/gate_quarantine.json'))
    parser.add_argument('--history-only', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)
    api = GitHub(os.environ['GITHUB_REPOSITORY'], os.environ['GITHUB_TOKEN'])
    history = load_history(api, int(os.environ['GITHUB_RUN_ID']), int(os.environ['GITHUB_RUN_NUMBER']))
    args.output.mkdir(parents=True, exist_ok=True)
    if args.history_only:
        (args.output / 'results.json').write_text(json.dumps(history, sort_keys=True) + '\n')
        return 0
    if args.input is None:
        parser.error('--input is required unless --history-only')
    result = collect_results(args.input, args.shards)
    sha = os.environ['GITHUB_SHA']
    result.update(schema=1, sha=sha)
    for node, row in result['tests'].items():
        row['last_pass_sha'] = sha if row['outcome'] == 'passed' else history['tests'].get(node, {}).get('last_pass_sha')
    quarantine = quarantine_entries(args.quarantine)
    result['classification'] = classify(result['tests'], history['tests'], quarantine)
    run_url = f"https://github.com/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
    plans = []
    if result['complete']:
        owned = owners(api.pages('issues?state=open&labels=full-suite'))
        prs = merged_prs(api, history['sha'], sha) if result['classification']['new'] else []
        plans = issue_plan(result['tests'], history['tests'], quarantine, owned,
                           history['sha'] or 'no previous baseline', sha, prs, run_url)
    else:
        # Infrastructure is tracked separately, never as recovered tests.
        plans = [{'kind': 'issue', 'title': '[P2] Full suite: shards ended without test verdicts',
                  'labels': [LABEL], 'body': marker('__infrastructure__') +
                  f"\n\nMissing verdicts after one infrastructure retry: {result['missing_shards']}\n"
                  f'Commit: {sha}\nRun: {run_url}\nRerun the workflow through workflow_dispatch.\n'}]
    result['dry_run'] = args.dry_run
    result['planned_actions'] = [{k: v for k, v in p.items() if k != 'body'} for p in plans]
    result['filed'] = [] if args.dry_run else apply_plan(api, plans)
    (args.output / 'results.json').write_text(json.dumps(result, sort_keys=True) + '\n')
    summary = {'complete': result['complete'], 'missing_shards': result['missing_shards'],
               'classification': {k: len(v) for k, v in result['classification'].items()},
               'dry_run': args.dry_run, 'actions': len(plans)}
    print(json.dumps(summary, sort_keys=True))
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as handle:
            handle.write('Full suite report\n\n```json\n' + json.dumps(summary, indent=2) + '\n```\n\n')
            handle.write('| Shard | Wall seconds | Verdict |\n|---|---|---|\n')
            for row in result['shards']:
                handle.write(f"| {row['shard']} | {row['wall_seconds']} | {row['verdict']} |\n")
    return int(not result['complete'] or any(row['outcome'] == 'failed' for row in result['tests'].values()))


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, zipfile.BadZipFile) as exc:
        # Never print exception strings from authenticated requests or signed URLs.
        print(f'Full-suite report could not complete ({type(exc).__name__}).', file=sys.stderr)
        raise SystemExit(2)

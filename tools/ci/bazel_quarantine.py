#!/usr/bin/env python3
"""Refuse Bazel quarantine entries whose deflake issue is no longer open.

Run on the GitHub-hosted Gate controller. Consume its own job token here;
never launch a child or execute the Starlark table as code.
"""
import argparse
import ast
import json
import os
from pathlib import Path
import urllib.request

ROOT = Path(__file__).resolve().parents[2]


def read_quarantine(path):
    for node in ast.parse(path.read_text()).body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name) and node.targets[0].id == 'QUARANTINE'):
            return ast.literal_eval(node.value)
    raise ValueError('QUARANTINE table is missing')


def check_issues(table, repository, token, *, urlopen=None):
    """One request per distinct issue. API errors and unknown states fail closed."""
    urlopen = urlopen or urllib.request.urlopen
    failures = []
    for issue in sorted({entry['issue'] for entry in table.values()}):
        request = urllib.request.Request(
            f'https://api.github.com/repos/{repository}/issues/{issue}',
            headers={'Authorization': f'Bearer {token}',
                     'Accept': 'application/vnd.github+json',
                     'X-GitHub-Api-Version': '2022-11-28'})
        try:
            with urlopen(request, timeout=60) as response:
                record = json.load(response)
            if not isinstance(record, dict) or record.get('number') != issue:
                raise ValueError('unexpected issue response')
        except (OSError, ValueError) as error:
            # An exception may embed request credentials; never echo its text.
            failures.append(f'#{issue}: cannot verify issue state ({type(error).__name__})')
            continue
        state = record.get('state')
        if state != 'open':
            state = 'closed' if state == 'closed' else 'unknown'
            labels = ', '.join(sorted(label for label, entry in table.items() if entry['issue'] == issue))
            failures.append(f'#{issue} is {state}; remove its quarantine entries with the fix: {labels}')
    return failures


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--table', type=Path, default=ROOT / 'tools/bazel/nonhermetic.bzl')
    args = parser.parse_args(argv)
    token = os.environ.pop('GITHUB_TOKEN', '')
    for name in tuple(os.environ):
        if name.endswith(('_API_KEY', '_TOKEN')):
            del os.environ[name]
    if not token:
        print('::error::GITHUB_TOKEN is required for the quarantine issue check')
        return 1
    try:
        table = read_quarantine(args.table)
        failures = check_issues(table, os.environ['GITHUB_REPOSITORY'], token)
    except (OSError, ValueError, SyntaxError, KeyError, TypeError) as error:
        print(f'::error::Cannot check Bazel quarantine ({type(error).__name__})')
        return 1
    for failure in failures:
        print(f'::error::{failure}')
    if failures:
        return 1
    print(f'Bazel quarantine: {len(table)} targets, all linked issues open')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

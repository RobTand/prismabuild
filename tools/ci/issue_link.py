"""Validate ordinary issue links and authenticated Mergify batch members."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import os
from pathlib import Path
import re
from urllib.error import HTTPError
from urllib.request import Request, urlopen


_BRANCH = re.compile(r'^prismabuild-([1-9][0-9]*)(-[a-z0-9][a-z0-9-]*)?$')
_CUTOFF = datetime.fromisoformat('2026-10-09T17:00:00+00:00')
_NO_ISSUE = (
    'This pull request links no issue in this repository. Satisfy the policy either way: '
    'write "Refs #123" in the pull request body (Fixes, Closes and Resolves also count, and close the issue on merge), '
    'or link the issue under "Development" in the pull request sidebar. '
    'Use "Refs #123" when the issue must stay open after the merge.'
)


@dataclass(frozen=True)
class Verdict:
    allowed: bool
    reason: str


# JavaScript's \s without the u flag: ECMAScript WhiteSpace and LineTerminator.
# re.ASCII keeps \b and case folding ASCII, as the JavaScript's were, so the
# whitespace class is spelled out instead of narrowed with them.
_JS_SPACE = r'[\t\n\v\f\r    -     　﻿]'


def referenced_issues(body: str | None, repository: str) -> tuple[int, ...]:
    pattern = re.compile(
        r'\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?|refs?|references?)' + _JS_SPACE + '+'
        r'(?:https://github\.com/' + re.escape(repository) + r'/issues/|'
        + re.escape(repository) + r'#|#)([1-9][0-9]*)\b', re.IGNORECASE | re.ASCII,
    )
    return tuple(dict.fromkeys(int(match[1]) for match in pattern.finditer(body or '')))


def branch_exempt(pull: dict) -> bool:
    try:
        older = datetime.fromisoformat(pull['created_at'].replace('Z', '+00:00')) < _CUTOFF
    except (ValueError, TypeError):
        older = False
    return pull['head']['ref'].startswith(('ig/', 'pg/', 'pg-train/', 'tl/', 'release')) or older


def ordinary_decision(pull: dict, repository: str, issues: dict, sidebar=()) -> Verdict:
    """Pure policy decision over PR metadata and already-fetched issue data."""
    numbers = referenced_issues(pull.get('body'), repository)
    exempt = branch_exempt(pull)
    branch = _BRANCH.fullmatch(pull['head']['ref'])
    for number in numbers:
        if not exempt and (not branch or number != int(branch[1])):
            continue
        issue = issues.get(number)
        if issue is not None and issue.get('pull_request') is None:
            return Verdict(True, f'Accepted the pull request body reference to issue #{number}.')
    if not exempt:
        expected = (f'prismabuild-{numbers[0]}' if numbers else
                    f'prismabuild-{branch[1]}' if branch else 'prismabuild-<issue>')
        return Verdict(False,
                       'This pull request violates the branch naming rule (rule 12). '
                       'Use prismabuild-<issue> or prismabuild-<issue>-<word>, '
                       f'for example {expected}. '
                       'The body must reference the branch issue number with "Refs #NUMBER" or a supported keyword. '
                       'The reference must name an actual issue in this repository.')
    for node in sidebar:
        if (node and (node.get('repository') or {}).get('nameWithOwner') == repository
                and type(node.get('number')) is int):
            return Verdict(True, f'Accepted linked issue #{node["number"]} from the pull request sidebar.')
    return Verdict(False, _NO_ISSUE)


def authenticated_batch(pull: dict, repository: str) -> bool:
    author = pull.get('user') or {}
    head = pull.get('head') or {}
    return (pull.get('draft') is True
            and head.get('ref', '').startswith('mergify/merge-queue/')
            and author.get('login') == 'mergify[bot]'
            and author.get('id') == 37929162
            and author.get('type') == 'Bot'
            and (head.get('repo') or {}).get('full_name') == repository)


def batch_members(body: str | None) -> tuple[int, ...]:
    """Parse Mergify's number/scopes YAML subset, rejecting ambiguous lists."""
    sections = re.findall(
        r'<summary>Metadata for CI tooling[^\n]*</summary>\s*'
        r'```yaml\s*\n(.*?)\n```\s*</details>', body or '', re.DOTALL,
    )
    if len(sections) != 1:
        raise ValueError('expected one Metadata for CI tooling YAML block')
    lines = sections[0].splitlines()
    keys = [i for i, line in enumerate(lines) if line.startswith('pull_requests:')]
    if len(keys) != 1 or lines[keys[0]].rstrip() != 'pull_requests:':
        raise ValueError('expected one pull_requests list')
    numbers = []
    scopes = False
    for line in lines[keys[0] + 1:]:
        if not line.strip():
            continue
        if not line.startswith(' '):
            if not re.fullmatch(r'[a-z_]+:.*|\.\.\.', line):
                raise ValueError('invalid metadata after pull_requests')
            break
        member = re.fullmatch(r'  - number: ([1-9][0-9]*)\s*', line)
        if member:
            number = int(member[1])
            if number in numbers:
                raise ValueError(f'duplicate member #{number}')
            numbers.append(number)
            scopes = False
        elif numbers and not scopes and line.rstrip() in ('    scopes: []', '    scopes:'):
            scopes = True
        elif scopes and re.fullmatch(r'      - [^\n]+', line):
            continue
        else:
            raise ValueError(f'invalid batch member metadata: {line.strip()}')
    if not numbers:
        raise ValueError('batch is empty')
    return tuple(numbers)


def _check_ordinary(pull, repository, fetch_issue, fetch_sidebar):
    exempt = branch_exempt(pull)
    branch = _BRANCH.fullmatch(pull['head']['ref'])
    issues = {}
    for number in referenced_issues(pull.get('body'), repository):
        if not exempt and (not branch or number != int(branch[1])):
            continue
        issues[number] = fetch_issue(number)
        verdict = ordinary_decision(pull, repository, issues)
        if verdict.allowed:
            return verdict
    verdict = ordinary_decision(pull, repository, issues)
    if not verdict.allowed and exempt:
        return ordinary_decision(pull, repository, issues, fetch_sidebar(pull['number']))
    return verdict


def issue_link_verdict(pull, repository, fetch_issue, fetch_pull, fetch_sidebar) -> Verdict:
    if not authenticated_batch(pull, repository):
        return _check_ordinary(pull, repository, fetch_issue, fetch_sidebar)
    try:
        members = batch_members(pull.get('body'))
    except ValueError as exc:
        return Verdict(False, f'Invalid Mergify batch metadata: {exc}.')
    for number in members:
        member = fetch_pull(number)
        if member is None:
            return Verdict(False, f'Mergify batch member #{number} is missing or inaccessible.')
        verdict = _check_ordinary(member, repository, fetch_issue, fetch_sidebar)
        if not verdict.allowed:
            return Verdict(False, f'Mergify batch member #{number}: {verdict.reason}')
    return Verdict(True, 'Accepted Mergify batch members ' + ', '.join(f'#{n}' for n in members) + '.')


class GitHub:
    """Workflow-token API adapter. Only missing REST objects become None."""

    def __init__(self, repository: str, token: str):
        self.repository = repository
        self.token = token

    def request(self, endpoint: str, data=None):
        request = Request('https://api.github.com/' + endpoint,
                          data=None if data is None else json.dumps(data).encode(),
                          headers={'Authorization': f'Bearer {self.token}',
                                   'Accept': 'application/vnd.github+json',
                                   'X-GitHub-Api-Version': '2022-11-28',
                                   'Content-Type': 'application/json'})
        try:
            with urlopen(request, timeout=30) as response:
                return json.load(response)
        except HTTPError as exc:
            if exc.code == 404 and data is None:
                return None
            raise

    def issue(self, number):
        return self.request(f'repos/{self.repository}/issues/{number}')

    def pull(self, number):
        return self.request(f'repos/{self.repository}/pulls/{number}')

    def sidebar(self, number):
        owner, repo = self.repository.split('/')
        response = self.request('graphql', {
            'query': '''query($owner: String!, $repo: String!, $number: Int!) {
              repository(owner: $owner, name: $repo) {
                pullRequest(number: $number) {
                  closingIssuesReferences(first: 100) {
                    nodes { number repository { nameWithOwner } }
                  }
                }
              }
            }''',
            'variables': {'owner': owner, 'repo': repo, 'number': number},
        })
        if response.get('errors'):
            raise ValueError('GitHub sidebar lookup failed')
        return response['data']['repository']['pullRequest']['closingIssuesReferences']['nodes'] or []


def main() -> int:
    try:
        event = json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text())
        github = GitHub(os.environ['GITHUB_REPOSITORY'], os.environ['GITHUB_TOKEN'])
        verdict = issue_link_verdict(event['pull_request'], github.repository,
                                     github.issue, github.pull, github.sidebar)
    except (OSError, ValueError, KeyError, TypeError):
        print('Issue-link API or event data unavailable; rerun the check.')
        return 1
    print(verdict.reason)
    return 0 if verdict.allowed else 1


if __name__ == '__main__':
    raise SystemExit(main())

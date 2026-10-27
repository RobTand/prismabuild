#!/usr/bin/env python3
"""Check new tickets, then create native GitHub sub-issues on explicit apply."""
from __future__ import annotations

import argparse
from collections.abc import Iterator
from dataclasses import dataclass
import json
from pathlib import Path
import re
import subprocess

from census import REPOS
from duplicates import PREFIX, likely_duplicate, normalize_title

PRIORITIES = ("P0", "P1", "P2+", "P2", "P3")
EVIDENCE = re.compile(r"^\s*(?:#{1,6}\s+)?(?:\*\*)?(reproduction|measurement)(?:\*\*)?\s*(?::(?:\*\*)?\s*(.*))?\s*$", re.I)
FIELD = re.compile(r"^\s*(?:\*\*)?\w[\w /()-]*(?:\*\*)?\s*:(?:\*\*)?(?:\s|$)")
FENCE = re.compile(r"^\s{0,3}(`{3,}|~{3,})(.*)$")
SECTION = re.compile(r"^\s*(?:#{1,6}\s+|\*\*[^*]+\*\*\s*:)")
PLACEHOLDER = re.compile(r"^(?:todo|tbd|n/?a|none|pending|not available|not yet|\.\.\.|<[^>]*>)\W*$", re.I)
CREATE = """mutation($input:CreateIssueInput!) {
 createIssue(input:$input) { issue { number url parent { id } } }
}"""


class Refused(Exception):
    def __init__(self, reason: str, **details):
        self.report = {"status": "refused", "reason": reason, **details}
        super().__init__(reason)


class GitHubError(Exception):
    pass


@dataclass(frozen=True)
class IssueRef:
    repo: str
    number: int

    @classmethod
    def parse(cls, text: str, repo: str) -> IssueRef:
        match = re.fullmatch(r"(?:(?:https://github\.com/([\w.-]+/[\w.-]+)/issues/)|(?:([\w.-]+/[\w.-]+)#)|#)?([1-9]\d*)", text)
        if not match:
            raise ValueError("parent must be a positive number, owner/repo#number, or GitHub issue URL")
        parent_repo = match[1] or match[2] or repo
        if parent_repo not in REPOS:
            raise ValueError("parent repository is outside the three managed repositories")
        return cls(parent_repo, int(match[3]))


@dataclass(frozen=True)
class Ticket:
    repo: str
    parent: IssueRef
    priority: str
    title: str
    body: str
    labels: tuple[str, ...] = ()


def evidence_lines(body: str) -> Iterator[str]:
    body = re.sub(r"<!--.*?-->", "", body, flags=re.S)
    active = False
    fence = ""
    for line in body.splitlines():
        delimiter = FENCE.fullmatch(line)
        if fence:
            if (delimiter and delimiter[1][0] == fence[0]
                    and len(delimiter[1]) >= len(fence) and not delimiter[2].strip()):
                fence = ""
            elif active:
                yield line
            continue
        if delimiter:
            fence = delimiter[1]
            continue
        match = EVIDENCE.fullmatch(line)
        if match:
            active = True
            line = match[2] or ""
        elif SECTION.match(line) or FIELD.match(line):
            active = False
        if active:
            yield line


def has_evidence(body: str) -> bool:
    for line in evidence_lines(body):
        content = re.sub(r"^\s*(?:[-*]|\d+[.)])\s+(?:\[[ xX]\]\s*)?", "", line).strip().strip("`*_ -\t")
        if content and not PLACEHOLDER.fullmatch(content):
            return True
    return False


class GitHub:
    def call(self, path: str, payload: dict | None = None, *, pages=False):
        command = ["gh", "api", path]
        if payload is not None:
            command += ["--method", "POST", "--input", "-"]
        elif pages:
            command += ["--paginate"]
        else:
            command += ["--method", "GET"]
        try:
            result = subprocess.run(command, input=json.dumps(payload) if payload is not None else None,
                                    text=True, capture_output=True, timeout=120, check=False)
        except (OSError, subprocess.TimeoutExpired) as error:
            raise GitHubError(f"GitHub request unavailable: {error}. Inspect GitHub before retrying a write.") from error
        if result.returncode:
            raise GitHubError(f"GitHub request failed: {result.stderr.strip()}. Inspect GitHub before retrying a write.")
        try:
            if pages:
                response = []
                remaining = result.stdout.strip()
                decoder = json.JSONDecoder()
                while remaining:
                    page, end = decoder.raw_decode(remaining)
                    response.append(page)
                    remaining = remaining[end:].lstrip()
            else:
                response = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise GitHubError("GitHub returned invalid JSON; no successful outcome established") from error
        if isinstance(response, dict) and response.get("errors"):
            raise GitHubError(f"GitHub GraphQL errors: {response['errors']}")
        return response

    def listing(self, path: str) -> list[dict]:
        separator = "&" if "?" in path else "?"
        pages = self.call(path + separator + "per_page=100", pages=True)
        if not isinstance(pages, list) or not pages or any(not isinstance(page, list) for page in pages):
            raise GitHubError("GitHub pagination response is incomplete or invalid")
        return [row for page in pages for row in page]

    def open_issues(self) -> list[dict]:
        issues = []
        for repo in REPOS:
            for row in self.listing(f"repos/{repo}/issues?state=open"):
                if "pull_request" not in row:
                    if row["state"] != "open" or not isinstance(row["title"], str):
                        raise GitHubError("GitHub returned an invalid open issue")
                    issues.append({"title": row["title"], "url": row["html_url"]})
        return issues

    def prepare(self, ticket: Ticket) -> dict:
        repo = self.call(f"repos/{ticket.repo}")
        parent = self.call(f"repos/{ticket.parent.repo}/issues/{ticket.parent.number}")
        if parent["state"] != "open" or "pull_request" in parent:
            raise Refused("parent_must_be_an_open_issue")
        labels = {row["name"]: row["node_id"] for row in self.listing(f"repos/{ticket.repo}/labels")}
        requested = set(ticket.labels)
        conflicting = requested.intersection((*PRIORITIES, "P4")) - {ticket.priority}
        if conflicting:
            raise Refused("conflicting_priority_labels", labels=sorted(conflicting))
        if ticket.priority in labels:
            requested.add(ticket.priority)
        missing = requested - labels.keys()
        if missing:
            raise Refused("unknown_labels", labels=sorted(missing))
        return {"repositoryId": repo["node_id"], "parentIssueId": parent["node_id"],
                "title": ticket.title, "body": ticket.body,
                "labelIds": [labels[name] for name in sorted(requested)]}

    def create(self, payload: dict) -> dict:
        response = self.call("graphql", {"query": CREATE, "variables": {"input": payload}})
        issue = response["data"]["createIssue"]["issue"]
        if issue["parent"]["id"] != payload["parentIssueId"]:
            raise GitHubError(f"Created issue has an unexpected parent: {issue['url']}")
        return {"status": "created", "url": issue["url"], "number": issue["number"]}


def submit(ticket: Ticket, github: GitHub, *, apply=False) -> dict:
    if ticket.priority in {"P0", "P1"} and not has_evidence(ticket.body):
        raise Refused("high_priority_requires_reproduction_or_measurement")
    issues = github.open_issues()
    duplicates = sorted(row["url"] for row in issues if likely_duplicate(ticket.title, row["title"]))
    if duplicates:
        raise Refused("likely_duplicate", duplicates=duplicates)
    payload = github.prepare(ticket)
    if apply:
        return github.create(payload)
    return {"status": "dry_run", "open_issues_checked": len(issues), "input": payload}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, choices=REPOS)
    parser.add_argument("--parent", required=True)
    parser.add_argument("--priority", required=True, choices=PRIORITIES)
    parser.add_argument("--title", required=True)
    parser.add_argument("--body-file", required=True, type=Path)
    parser.add_argument("--label", action="append", default=[])
    parser.add_argument("--apply", action="store_true", help="Create the issue after all checks pass")
    args = parser.parse_args(argv)
    try:
        parent = IssueRef.parse(args.parent, args.repo)
        title = args.title.strip()
        prefix = PREFIX.match(title)
        if prefix and prefix[1] != args.priority:
            raise ValueError("title prefix conflicts with --priority")
        title = PREFIX.sub("", title).strip()
        if not normalize_title(title) or PREFIX.match(title):
            raise ValueError("title must contain work and at most one leading priority")
        body = args.body_file.read_text()
        if not body.strip():
            raise ValueError("body must not be empty")
        ticket = Ticket(args.repo, parent, args.priority, f"[{args.priority}] {title}", body, tuple(args.label))
        report = submit(ticket, GitHub(), apply=args.apply)
        code = 0
    except Refused as error:
        report, code = error.report, 2
    except (ValueError, OSError, GitHubError, KeyError, TypeError) as error:
        report, code = {"status": "error", "reason": str(error)}, 1
    print(json.dumps(report, indent=2, sort_keys=True))
    return code


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Collect a read-only GitHub snapshot for the offline backlog census."""

from __future__ import annotations

import argparse
from collections import deque
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
from urllib.parse import quote

from census import PARENT, REPOS, SCOPE, blocker_references, priority, reference


ISSUE_FIELDS = """
number title body state createdAt updatedAt closedAt url
repository { nameWithOwner }
labels(first:100) { nodes { name } pageInfo { hasNextPage endCursor } }
parent { number repository { nameWithOwner } }
"""
LIST_QUERY = """query($owner:String!, $name:String!, $cursor:String, $states:[IssueState!]) {
 repository(owner:$owner,name:$name) {
  defaultBranchRef { name target { oid } }
  issues(first:100,after:$cursor,states:$states,orderBy:{field:UPDATED_AT,direction:DESC}) {
   nodes { %s } pageInfo { hasNextPage endCursor }
  }
 }
}""" % ISSUE_FIELDS
ISSUE_QUERY = """query($owner:String!, $name:String!, $number:Int!) {
 repository(owner:$owner,name:$name) { issue(number:$number) { %s } }
}""" % ISSUE_FIELDS
PULL_QUERY = """query($owner:String!, $name:String!, $number:Int!, $cursor:String) {
 repository(owner:$owner,name:$name) { pullRequest(number:$number) {
  url merged mergeCommit { oid }
  closingIssuesReferences(first:100,after:$cursor) {
   nodes { number repository { nameWithOwner } } pageInfo { hasNextPage endCursor }
  }
 } }
}"""
RELATION_LINE = re.compile(
    r"(?im)^\s*(?:[-*]\s*)?(?:duplicate of|duplicates|blocked by|depends on)\b[^\n]*"
)
REFERENCE = re.compile(
    r"https://github\.com/([\w.-]+/[\w.-]+)/issues/(\d+)"
    r"|(?<![\w/])(?:([\w.-]+/[\w.-]+))?#(\d+)"
)


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def issue_key(repo, number):
    return f"{repo}#{number}"


def node_key(node):
    return issue_key(node["repository"]["nameWithOwner"], node["number"])


def split_key(key):
    repo, number = key.rsplit("#", 1)
    return repo, int(number)


def in_scope(node):
    labels = {item["name"] for item in node["labels"]["nodes"]}
    return priority(node["title"], labels) in SCOPE


def relation_targets(body, repo):
    targets, _ = blocker_references(body or "", repo)
    targets.update(reference(match, repo) for match in PARENT.finditer(body or ""))
    for line in RELATION_LINE.findall(body or ""):
        for match in REFERENCE.finditer(line):
            target_repo = match.group(1) or match.group(3) or repo
            number = int(match.group(2) or match.group(4))
            targets.add(issue_key(target_repo, number))
    return targets


class GitHubError(RuntimeError):
    pass


class GitHub:
    """The only network boundary: fixed GraphQL queries and REST GET requests."""

    def __init__(self, raw_dir):
        self.raw_dir = Path(raw_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.receipts = []

    def _call(self, command, request):
        started = utc_now()
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=120, check=False)
        except subprocess.TimeoutExpired as error:
            result = subprocess.CompletedProcess(command, 124, "", f"Request exceeded {error.timeout} seconds")
        try:
            response = json.loads(result.stdout)
        except json.JSONDecodeError:
            response = None
        record = {"started_at": started, "completed_at": utc_now(), "request": request,
                  "returncode": result.returncode, "response": response, "stderr": result.stderr}
        if response is None:
            record["unparsed_stdout"] = result.stdout
        payload = json.dumps(record, sort_keys=True, indent=2) + "\n"
        digest = hashlib.sha256(payload.encode()).hexdigest()
        path = self.raw_dir / f"{len(self.receipts):06d}-{digest}.json"
        path.write_text(payload)
        self.receipts.append({"path": path.name, "sha256": digest})
        if result.returncode or response is None:
            raise GitHubError(f"GitHub request failed; see {path}")
        if isinstance(response, dict) and response.get("errors"):
            raise GitHubError(f"GraphQL returned errors; see {path}")
        return response

    def graphql(self, query, **variables):
        if not query.lstrip().startswith("query("):
            raise ValueError("Only fixed GraphQL queries are permitted")
        command = ["gh", "api", "graphql", "-f", f"query={query}"]
        for key, value in variables.items():
            if value is not None:
                if isinstance(value, list):
                    command.extend(arg for item in value for arg in ("-f", f"{key}[]={item}"))
                else:
                    command += ["-F", f"{key}={value}"]
        data = self._call(command, {"query": query, "variables": variables})
        if data.get("data") is None:
            raise GitHubError("GraphQL response omitted data")
        return data["data"]

    def rest(self, path):
        if not path.startswith("repos/") or any(part in (".", "..") for part in path.split("/")):
            raise ValueError("Only repository REST reads are permitted")
        return self._call(["gh", "api", "--method", "GET", "-H", "Accept: application/vnd.github+json",
                           "-H", "X-GitHub-Api-Version: 2022-11-28", path], {"method": "GET", "path": path})

    def pages(self, path):
        result, page = [], 1
        separator = "&" if "?" in path else "?"
        while True:
            values = self.rest(f"{path}{separator}per_page=100&page={page}")
            if not isinstance(values, list):
                raise GitHubError(f"Expected a list at {path}")
            result.extend(values)
            if len(values) < 100:
                return result
            page += 1


def advance(page, cursor):
    info = page["pageInfo"]
    if not info["hasNextPage"]:
        return None
    following = info["endCursor"]
    if not following or following == cursor:
        raise GitHubError("GraphQL pagination did not advance")
    return following


class Collector:
    def __init__(self, api, since, repos=REPOS):
        self.api, self.since, self.repos = api, since, tuple(repos)
        self.issues, self.repositories, self.nodes = {}, {}, {}
        self.pulls = {}
        self.limitations = []

    def repository_issues(self, repo, states):
        owner, name = repo.split("/")
        cursor = None
        while True:
            result = self.api.graphql(LIST_QUERY, owner=owner, name=name, cursor=cursor, states=states)
            repository = result["repository"]
            if repository is None or repository["defaultBranchRef"] is None:
                raise GitHubError(f"Repository/default branch unavailable: {repo}")
            branch = repository["defaultBranchRef"]
            self.repositories.setdefault(repo, {"default_branch": branch["name"], "default_oid": branch["target"]["oid"]})
            page = repository["issues"]
            for node in page["nodes"]:
                if states is None and node["updatedAt"] < self.since:
                    return
                self.nodes[node_key(node)] = node
            cursor = advance(page, cursor)
            if cursor is None:
                return

    def fetch_node(self, key):
        repo, number = split_key(key)
        owner, name = repo.split("/")
        node = self.api.graphql(ISSUE_QUERY, owner=owner, name=name, number=number)["repository"]["issue"]
        if node is None:
            raise GitHubError(f"Referenced issue unavailable: {key}")
        self.nodes[key] = node
        return node

    def labels(self, node):
        if not node["labels"]["pageInfo"]["hasNextPage"]:
            return [label["name"] for label in node["labels"]["nodes"]]
        repo = node["repository"]["nameWithOwner"]
        return [label["name"] for label in self.api.pages(f"repos/{repo}/issues/{node['number']}/labels")]

    def pull_fix(self, repo, number, target):
        cache_key = issue_key(repo, number)
        if cache_key not in self.pulls:
            owner, name = repo.split("/")
            cursor, closes = None, set()
            while True:
                pull = self.api.graphql(PULL_QUERY, owner=owner, name=name, number=number, cursor=cursor)["repository"]["pullRequest"]
                if pull is None:
                    raise GitHubError(f"Referenced pull request unavailable: {cache_key}")
                closes.update(node_key(node) for node in pull["closingIssuesReferences"]["nodes"])
                cursor = advance(pull["closingIssuesReferences"], cursor)
                if cursor is None:
                    break
            default_oid = self.repositories.get(repo, {}).get("default_oid")
            commit = (pull.get("mergeCommit") or {}).get("oid")
            on_default = False
            if pull["merged"] and commit and default_oid:
                comparison = self.api.rest(f"repos/{repo}/compare/{quote(commit, safe='')}...{quote(default_oid, safe='')}")
                on_default = comparison["status"] in ("ahead", "identical")
            self.pulls[cache_key] = {"proof": pull["url"], "commit": commit, "default_oid": default_oid,
                                    "on_default": on_default, "closes": closes}
        found = self.pulls[cache_key]
        return {key: value for key, value in found.items() if key != "closes"} | {"closes_issue": target in found["closes"]}

    def native_blockers(self, repo, number):
        try:
            values = self.api.pages(f"repos/{repo}/issues/{number}/dependencies/blocked_by")
        except GitHubError as error:
            self.limitations.append(f"Native blockers unavailable for {issue_key(repo, number)}: {error}")
            return None
        return [issue_key(value["repository_url"].split("/repos/", 1)[1], value["number"])
                for value in values]

    def normalize(self, node):
        repo, number = node["repository"]["nameWithOwner"], node["number"]
        key = node_key(node)
        labels = self.labels(node)
        node = node | {"labels": {"nodes": [{"name": label} for label in labels]}}
        issue = {"repo": repo, "number": number, "title": node["title"], "body": node["body"] or "",
                 "state": node["state"], "labels": labels,
                 "native_parent": node_key(node["parent"]) if node["parent"] else None,
                 "created_at": node["createdAt"], "updated_at": node["updatedAt"], "closed_at": node["closedAt"],
                 "blockers": [], "blockers_complete": False, "comments": [], "comments_complete": False,
                 "timeline": [{"kind": "opened", "at": node["createdAt"], "proof": node["url"]}],
                 "timeline_complete": False, "fixes": []}
        targets = relation_targets(issue["body"], repo)
        if issue["native_parent"]:
            targets.add(issue["native_parent"])
        if in_scope(node):
            base = f"repos/{repo}/issues/{number}"
            issue["comments"] = [{"url": comment["html_url"], "body": comment.get("body") or ""}
                                 for comment in self.api.pages(f"{base}/comments")]
            issue["comments_complete"] = True
            blockers = self.native_blockers(repo, number)
            if blockers is not None:
                issue["blockers"] = blockers
                issue["blockers_complete"] = True
                targets.update(blockers)
            for comment in issue["comments"]:
                targets.update(relation_targets(comment["body"], repo))
        if in_scope(node) or node["updatedAt"] >= self.since:
            timeline = self.api.pages(f"repos/{repo}/issues/{number}/timeline")
            for event in timeline:
                if event["event"] in ("closed", "reopened"):
                    issue["timeline"].append({"kind": event["event"], "at": event["created_at"],
                                              "proof": event.get("url") or node["url"]})
                if in_scope(node) and event["event"] == "cross-referenced":
                    source = (event.get("source") or {}).get("issue") or {}
                    if source.get("pull_request"):
                        source_repo = source["repository_url"].split("/repos/", 1)[1]
                        issue["fixes"].append(self.pull_fix(source_repo, source["number"], key))
            issue["timeline_complete"] = True
        return issue, targets

    def collect(self):
        started = utc_now()
        for repo in self.repos:
            self.repository_issues(repo, ["OPEN"])
            self.repository_issues(repo, None)
        pending, queued = deque(sorted(self.nodes)), set(self.nodes)
        while pending:
            key = pending.popleft()
            node = self.nodes.get(key) or self.fetch_node(key)
            issue, targets = self.normalize(node)
            self.issues[key] = issue
            for target in sorted(targets - queued):
                repo, _ = split_key(target)
                if repo not in self.repos:
                    self.limitations.append(f"External relationship was not fetched: {key} -> {target}")
                    continue
                pending.append(target)
                queued.add(target)
        return {"schema": "backlog-census/v1", "collected_at": utc_now(),
                "repositories": self.repositories, "issues": [self.issues[key] for key in sorted(self.issues)],
                "coverage": {"started_at": started, "since": self.since, "all_open_issues": True,
                             "updated_since_included": True, "historical_labels_and_parents": False,
                             "atomic_snapshot": False, "limitations": sorted(set(self.limitations)),
                             "raw_receipts": getattr(self.api, "receipts", [])}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", required=True, help="UTC ISO timestamp for the flow window, e.g. 2026-10-18T00:00:00Z")
    parser.add_argument("--output", required=True, type=Path, help="New snapshot JSON path; raw replies go beside it")
    parser.add_argument("--repo", action="append", choices=REPOS, help="Defaults to all three repositories")
    args = parser.parse_args()
    since = datetime.fromisoformat(args.since.replace("Z", "+00:00"))
    if since.tzinfo is None:
        parser.error("--since requires a timezone")
    since = since.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    raw = args.output.with_suffix(".raw")
    if args.output.exists() or raw.exists():
        parser.error("Choose a new output path so existing evidence remains unchanged")
    api = GitHub(raw)
    snapshot = Collector(api, since, args.repo or REPOS).collect()
    args.output.write_text(json.dumps(snapshot, sort_keys=True, indent=2) + "\n")
    print(f"Saved {len(snapshot['issues'])} issues and {len(api.receipts)} raw replies to {args.output}")


if __name__ == "__main__":
    main()

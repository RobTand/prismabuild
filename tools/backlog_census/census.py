#!/usr/bin/env python3
"""Analyze saved GitHub evidence without changing GitHub issues."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
import hashlib
import json
from pathlib import Path
import re

PRIORITIES = ("P0", "P1", "P2+", "P2", "P3", "P4")
SCOPE = PRIORITIES[:3]
REPOS = tuple(f"RobTand/{name}" for name in ("prismaquant", "prismabuild", "tessera"))
BLOCKED = {"tl:blocked", "ig:blocked"}
PREFIX = re.compile(r"^\s*\[(P0|P1|P2\+|P2|P3|P4)\]")
REFERENCE = r"(?:https://github\.com/([\w.-]+/[\w.-]+)/issues/|([\w.-]+/[\w.-]+)?#)(\d+)"
PARENT = re.compile(r"^\s*(?:[-*]\s+)?(?:part of|sub-issue of|parent)\s*:?\s*" + REFERENCE, re.I | re.M)
BLOCKER = re.compile(r"^(?:[-*][ \t]+)?(?:blocked by|depends on|blockers?|dependencies)\b[ \t]*:?[ \t]*(.*)$", re.I)
BLOCKER_WORD = re.compile(r"\b(?:blocked by|depends on|blockers?|dependencies)\b", re.I)
REF = re.compile(REFERENCE)
CLAIM = re.compile(r"\b(?:(?:fixed|implemented|resolved|completed)\s+(?:in|by)|landed\s+in)\b", re.I)
BODY_STATUS = re.compile(r"^\s*(?:[-*]\s+)?(?:\*\*)?(?:status|resolution)(?:\*\*)?\s*:|"
                         r"^\s*(?:[-*]\s+)?(?:fixed|implemented|resolved|completed|landed)\s+(?:in|by)\b", re.I)
PROOF = re.compile(r"https://github\.com/[\w.-]+/[\w.-]+/(?:pull/\d+|commit/[0-9a-f]{7,40})\b", re.I)


def timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp must include a timezone: {value}")
    return parsed.astimezone(timezone.utc)


def key(repo: str, number: int) -> str:
    return f"{repo}#{number}"


def url(issue_key: str) -> str:
    repo, number = issue_key.rsplit("#", 1)
    return f"https://github.com/{repo}/issues/{number}"


def reference(match: re.Match, repo: str) -> str:
    return key(match[1] or match[2] or repo, int(match[3]))


def priority(title: str, labels) -> str | None:
    displayed = next((p for p in PRIORITIES if p in labels), None)
    match = PREFIX.match(title)
    return displayed or (match[1] if match else None)


def blocker_references(body: str, repo: str) -> tuple[set[str], bool]:
    lines = body.splitlines()
    refs = set()
    complete = True
    for n, line in enumerate(lines):
        text = line.strip().lstrip("#").strip()
        declaration = BLOCKER.match(text)
        if not declaration:
            if BLOCKER_WORD.search(text):
                complete = False
            continue
        content = declaration[1]
        if not content:
            following = []
            for following_line in lines[n + 1:]:
                if not re.match(r"^[ \t]*[-*][ \t]+", following_line):
                    break
                following.append(following_line)
            content = "\n".join(following)
        matches = list(REF.finditer(content))
        remainder = re.sub(r"\b(?:and|or)\b|[-*\s,;&.():\[\]`]+", "", REF.sub("", content), flags=re.I)
        if not matches or remainder or any(not REF.search(item) for item in content.splitlines()):
            complete = False
        refs.update(reference(m, repo) for m in matches)
    return refs, complete


@dataclass(frozen=True)
class Issue:
    repo: str
    number: int
    title: str
    body: str
    state: str
    labels: frozenset[str]
    native_parent: str | None
    created_at: str | None
    updated_at: str | None
    closed_at: str | None
    evidence: dict

    @property
    def key(self) -> str:
        return key(self.repo, self.number)

    @property
    def priority(self) -> str | None:
        return priority(self.title, self.labels)

    @property
    def title_priority(self) -> str | None:
        match = PREFIX.match(self.title)
        return match[1] if match else None

    @property
    def in_scope(self) -> bool:
        return self.repo in REPOS and self.state == "OPEN" and self.priority in SCOPE

    @classmethod
    def parse(cls, row: dict) -> Issue:
        if row["state"] not in {"OPEN", "CLOSED"}:
            raise ValueError(f"invalid issue state: {row['state']}")
        if not re.fullmatch(r"[\w.-]+/[\w.-]+", row["repo"]):
            raise ValueError("issue repo must be owner/name")
        if type(row["number"]) is not int or row["number"] <= 0:
            raise ValueError("issue number must be positive")
        for field in ("created_at", "updated_at", "closed_at"):
            if row.get(field):
                timestamp(row[field])
        native = row.get("native_parent")
        if native and not re.fullmatch(r"[\w.-]+/[\w.-]+#[1-9]\d*", native):
            raise ValueError(f"invalid native parent: {native}")
        return cls(row["repo"], row["number"], row["title"], row.get("body") or "",
                   row["state"], frozenset(row["labels"]), native,
                   row["created_at"], row["updated_at"], row.get("closed_at"), row)


@dataclass(frozen=True)
class Snapshot:
    collected_at: str
    issues: dict[str, Issue]
    repositories: dict
    coverage: dict

    @classmethod
    def parse(cls, data: dict) -> Snapshot:
        if data["schema"] != "backlog-census/v1":
            raise ValueError("unsupported snapshot schema")
        timestamp(data["collected_at"])
        issues = [Issue.parse(row) for row in data["issues"]]
        if len({i.key for i in issues}) != len(issues):
            raise ValueError("duplicate issue key in snapshot")
        return cls(data["collected_at"], {i.key: i for i in issues},
                   data.get("repositories", {}), data.get("coverage", {}))


@dataclass(frozen=True)
class ParentLink:
    target: str | None
    source: str
    references: tuple[str, ...] = ()


class Graph:
    def __init__(self, issues: dict[str, Issue]):
        self.issues = issues
        self.links = {k: self.parent(i) for k, i in issues.items()}

    @staticmethod
    def parent(issue: Issue) -> ParentLink:
        refs = tuple(sorted({reference(m, issue.repo) for m in PARENT.finditer(issue.body)}))
        if issue.native_parent:
            return ParentLink(issue.native_parent, "native", refs)
        if len(refs) > 1:
            return ParentLink(None, "ambiguous", refs)
        return ParentLink(refs[0], "text", refs) if refs else ParentLink(None, "none")

    def trace(self, issue_key: str) -> tuple[str, list[dict]]:
        path = []
        diagnostics = []
        current = issue_key
        root = issue_key
        while current in self.issues:
            if current in path:
                cycle = path[path.index(current):]
                open_cycle = [k for k in cycle if self.issues[k].state == "OPEN"]
                if open_cycle:
                    root = min(open_cycle)
                diagnostics.append({"kind": "cycle", "members": sorted(cycle)})
                break
            path.append(current)
            issue = self.issues[current]
            if issue.evidence.get("metadata_complete") is False:
                diagnostics.append({"kind": "incomplete_ancestor", "issue": current})
            if issue.state == "OPEN":
                root = current
            link = self.links[current]
            if link.source == "ambiguous":
                diagnostics.append({"kind": "ambiguous_parent", "issue": current,
                                    "references": link.references})
            if link.source == "native" and any(r != link.target for r in link.references):
                diagnostics.append({"kind": "native_text_conflict", "issue": current,
                                    "native": link.target, "text": link.references})
            if not link.target:
                break
            if link.target not in self.issues:
                diagnostics.append({"kind": "unknown_parent", "issue": current, "target": link.target})
                break
            current = link.target
        return root, diagnostics


def rollup(snapshot: Snapshot) -> dict:
    graph = Graph(snapshot.issues)
    tickets = {k: i for k, i in snapshot.issues.items() if i.in_scope}
    units = defaultdict(list)
    diagnostics = []
    for k in sorted(tickets):
        root, notes = graph.trace(k)
        units[root].append(k)
        diagnostics.extend({"ticket": k, **note} for note in notes)
        issue = tickets[k]
        displayed = set(issue.labels) & set(PRIORITIES)
        if len(displayed) > 1 or (displayed and issue.title_priority and issue.priority != issue.title_priority):
            diagnostics.append({"ticket": k, "kind": "priority_conflict",
                                "labels": sorted(displayed), "title": issue.title_priority})
    unit_priority = {root: min((tickets[k].priority for k in members), key=SCOPE.index)
                     for root, members in units.items()}
    repos = {}
    for repo in sorted(set(REPOS) | {r.rsplit("#", 1)[0] for r in units}):
        members = [i for i in tickets.values() if i.repo == repo]
        roots = [r for r in units if r.rsplit("#", 1)[0] == repo]
        repos[repo] = {
            "tickets": len(members), "work_units": len(roots),
            "tickets_by_priority": {p: sum(i.priority == p for i in members) for p in SCOPE},
            "units_by_priority": {p: sum(unit_priority[r] == p for r in roots) for p in SCOPE},
            "title_fallback": sum(not (i.labels & set(PRIORITIES)) for i in members),
            "blocked": sum(bool(i.labels & BLOCKED) for i in members),
        }
    return {"collected_at": snapshot.collected_at, "repositories": repos,
            "tickets": {k: {"priority": i.priority, "root": graph.trace(k)[0]} for k, i in sorted(tickets.items())},
            "units": {r: {"priority": unit_priority[r], "members": units[r]} for r in sorted(units)},
            "diagnostics": diagnostics, "coverage": snapshot.coverage}


def candidate(kind: str, tickets: list[str], proof: list[str], **details) -> dict:
    identity = json.dumps([kind, sorted(tickets)], separators=(",", ":"))
    return {"id": hashlib.sha256(identity.encode()).hexdigest()[:16], "kind": kind,
            "tickets": sorted(tickets), "proof": proof, **details}


def duplicate_clusters(scope: list[Issue]) -> list[list[str]]:
    titles = {i.key: re.sub(r"\W+", " ", PREFIX.sub("", i.title).casefold()).strip() for i in scope}
    roots = {i.key: i.key for i in scope}

    def root(k):
        while roots[k] != k:
            roots[k] = roots[roots[k]]
            k = roots[k]
        return k

    for n, issue in enumerate(scope):
        for other in scope[n + 1:]:
            left, right = titles[issue.key], titles[other.key]
            if left and right and (left == right or (len(left) >= 30 and len(right) >= 30 and
                                                       SequenceMatcher(None, left, right).ratio() >= .92)):
                a, b = sorted((root(issue.key), root(other.key)))
                roots[b] = a
    groups = defaultdict(list)
    for k in sorted(roots):
        groups[root(k)].append(k)
    return [members for _, members in sorted(groups.items()) if len(members) > 1]


def claim_sources(issue: Issue) -> list[str]:
    texts = [(url(issue.key), line) for line in issue.body.splitlines() if BODY_STATUS.match(line)]
    texts.extend((c["url"], c["body"]) for c in issue.evidence.get("comments", []))
    return sorted({source for source, body in texts if CLAIM.search(body) and not PROOF.search(body)})


def candidates(snapshot: Snapshot) -> list[dict]:
    scope = sorted((i for i in snapshot.issues.values() if i.in_scope), key=lambda i: i.key)
    result = [candidate("duplicate_cluster", members, [url(k) for k in members],
                        reason="Title similarity requires semantic review; it does not prove an exact duplicate.")
              for members in duplicate_clusters(scope)]
    for issue in scope:
        fixes = issue.evidence.get("fixes", [])
        default_oid = snapshot.repositories.get(issue.repo, {}).get("default_oid")
        verified = [f for f in fixes if f.get("on_default") is True and f.get("closes_issue") is True
                    and default_oid and f.get("default_oid") == default_oid and f.get("commit") and f.get("proof")]
        if fixes:
            result.append(candidate("default_branch_fix", [issue.key], [f["proof"] for f in fixes if f.get("proof")],
                                    verified=verified, reason="Close only for an explicit closing relationship and default-branch ancestry."))
        if issue.labels & BLOCKED:
            refs = set(issue.evidence.get("blockers", []))
            bodies = [issue.body] + [c["body"] for c in issue.evidence.get("comments", [])]
            declarations = [blocker_references(body, issue.repo) for body in bodies]
            refs.update(r for targets, _ in declarations for r in targets)
            states = {r: snapshot.issues[r].state if r in snapshot.issues else "UNKNOWN" for r in sorted(refs)}
            complete = (issue.evidence.get("blockers_complete") is True
                        and issue.evidence.get("comments_complete") is True
                        and all(parsed for _, parsed in declarations))
            if complete and states and all(s == "CLOSED" for s in states.values()):
                result.append(candidate("dead_blocked_chain", [issue.key], [url(issue.key)] + [url(r) for r in states],
                                        blockers=states, labels=sorted(issue.labels & BLOCKED)))
        unproven = claim_sources(issue)
        if unproven:
            result.append(candidate("claim_without_evidence", [issue.key], unproven,
                                    reason="Completion phrase in a comment or body status line without a linked commit or PR; review context."))
    return sorted(result, key=lambda c: (c["kind"], c["tickets"]))


def write_list(snapshot: Snapshot, found: list[dict], hindsight: dict | None = None) -> list[dict]:
    graph = Graph(snapshot.issues)
    rows = []
    roots_with_children = {link.target for link in graph.links.values() if link.target}
    for k, issue in sorted(snapshot.issues.items()):
        if not issue.in_scope:
            continue
        link = graph.links[k]
        parent = snapshot.issues.get(link.target)
        root, diagnostics = graph.trace(k)
        if parent and parent.state == "CLOSED":
            if root != k and not diagnostics:
                rows.append({"action": "reparent", "issue": k, "target": root,
                             "proof": [url(k), url(parent.key), url(root)], "status": "proposed",
                             "preconditions": ["Recheck the closed parent chain to the known open ancestor before replacing the parent."]})
            else:
                rows.append({"action": "create_parent", "issue": k, "target": None,
                             "proof": [url(k), url(parent.key)], "status": "needs_parent_design",
                             "preconditions": ["Choose an open parent and review its scope; do not reopen the closed parent implicitly."]})
        elif link.source == "text" and parent and not diagnostics:
            rows.append({"action": "native_sub_issue", "issue": k, "target": parent.key,
                         "proof": [url(k), url(parent.key)], "status": "proposed",
                         "preconditions": ["Text relationship, open parent, and absent native parent still match the snapshot."]})
        elif link.source == "none" and k not in roots_with_children:
            rows.append({"action": "link_parent", "issue": k, "target": None, "proof": [url(k)],
                         "status": "needs_parent_design", "preconditions": ["Choose a justified parent; no parent inferred from similarity."]})
    for c in found:
        if c["kind"] == "dead_blocked_chain":
            rows.append({"action": "remove_blocked_labels", "issue": c["tickets"][0], "labels": c["labels"],
                         "proof": c["proof"], "candidate_id": c["id"], "status": "proposed",
                         "preconditions": ["Every native and declared text blocker is still closed; no unknown dependency."]})
        if c["kind"] == "default_branch_fix" and c["verified"]:
            rows.append({"action": "close", "issue": c["tickets"][0], "proof": c["proof"],
                         "candidate_id": c["id"], "status": "proposed",
                         "preconditions": ["Recheck closing relationship and ancestry at the current default branch."]})
    for row in rows:
        identity = json.dumps([row["action"], row["issue"], row.get("target")])
        row["id"] = hashlib.sha256(identity.encode()).hexdigest()[:16]
        receipt = (hindsight or {}).get(row.get("candidate_id", row["id"]))
        row["hindsight"] = receipt
        if row["status"] == "proposed":
            if not receipt:
                row["status"] = "needs_hindsight"
            elif receipt.get("sources_reviewed") is not True:
                row["status"] = "needs_hindsight_source_review"
    return rows


def flow(snapshot: Snapshot, since: str, until: str, before: Snapshot | None = None) -> dict:
    start, end = timestamp(since), timestamp(until)
    if start >= end:
        raise ValueError("flow requires since < until")
    if end > timestamp(snapshot.collected_at):
        raise ValueError("flow cannot extend beyond the snapshot collection time")
    counts = Counter()
    grouped = {repo: {p: Counter() for p in SCOPE} for repo in REPOS}
    incomplete = []
    for issue in snapshot.issues.values():
        if issue.repo not in REPOS or issue.priority not in SCOPE:
            continue
        events = issue.evidence.get("timeline", [])
        if not issue.evidence.get("timeline_complete"):
            incomplete.append(issue.key)
        for event in events:
            if event["kind"] in {"opened", "closed", "reopened"} and start < timestamp(event["at"]) <= end:
                counts[event["kind"]] += 1
                grouped[issue.repo][issue.priority][event["kind"]] += 1
    result = {"since_exclusive": since, "until_inclusive": until,
              "ticket_events_current_scope": {k: counts[k] for k in ("opened", "closed", "reopened")},
              "event_net_current_scope": None if incomplete else counts["opened"] + counts["reopened"] - counts["closed"],
              "incomplete_timelines": sorted(incomplete),
              "ticket_events_by_repository_priority": {
                  repo: {p: {kind: grouped[repo][p][kind] for kind in ("opened", "closed", "reopened")}
                         for p in SCOPE} for repo in REPOS},
              "limitation": "Event counts use endpoint priorities. Historical label and parent edits require endpoint snapshots.",
              "endpoint_flow": None}
    if before:
        if timestamp(before.collected_at) != start or timestamp(snapshot.collected_at) != end:
            raise ValueError("endpoint flow boundaries must equal the two snapshot collection timestamps")
        old, new = rollup(before), rollup(snapshot)
        result["endpoint_flow"] = {}
        for field in ("tickets", "units"):
            was, now = set(old[field]), set(new[field])
            result["endpoint_flow"][field] = {"entered": sorted(now - was), "left": sorted(was - now), "net": len(now) - len(was)}
    else:
        result["work_unit_flow_gap"] = "Exact unit flow needs two endpoint snapshots; today's relationships cannot reconstruct historical membership."
    return result


def reconcile(report: dict, baseline: list[dict]) -> dict:
    old = {}
    totals = {}
    for repo in baseline:
        name = f"RobTand/{repo['repo']}"
        totals[name] = {"tickets": repo["in_scope"], "work_units": repo["work_units"]}
        for root, members in repo["units"].items():
            for number in members:
                old[key(name, number)] = key(name, int(root))
    new = report["tickets"]
    changes = []
    for k in sorted(old.keys() | new.keys()):
        if k not in new:
            changes.append({"issue": k, "kind": "left_scope", "baseline_root": old[k]})
        elif k not in old:
            changes.append({"issue": k, "kind": "entered_scope", "root": new[k]["root"]})
        elif old[k] != new[k]["root"]:
            changes.append({"issue": k, "kind": "root_changed", "baseline_root": old[k], "root": new[k]["root"]})
    return {"baseline_totals": totals, "differences": changes,
            "limitation": "Baseline rollups retain membership but not per-ticket priority. Use raw snapshots to audit label/title changes."}


def import_planning(raw: Path, collected_at: str) -> dict:
    rows = []
    parent_states = {}
    for repo in REPOS:
        for row in json.loads((raw / f"{repo.split('/')[1]}.json").read_text()):
            parent = row.get("parent")
            if parent:
                parent_states[key(repo, parent["number"])] = parent["state"]
            rows.append({"repo": repo, "number": row["number"], "title": row["title"], "body": row.get("body") or "",
                         "state": "OPEN", "labels": [n["name"] for n in row["labels"]["nodes"]],
                         "native_parent": key(repo, parent["number"]) if parent else None,
                         "created_at": row["createdAt"], "updated_at": row["updatedAt"], "closed_at": None})
    known = {key(row["repo"], row["number"]) for row in rows}
    for parent_key, state in parent_states.items():
        if parent_key not in known and state == "CLOSED":
            repo, number = parent_key.rsplit("#", 1)
            rows.append({"repo": repo, "number": int(number), "title": "", "body": "", "state": state,
                         "labels": [], "native_parent": None, "created_at": None, "updated_at": None,
                         "closed_at": None, "metadata_complete": False})
    return {"schema": "backlog-census/v1", "collected_at": collected_at, "issues": rows, "repositories": {},
            "coverage": {"source": "planning open-only snapshot", "limitations": [
                "Closed native parent states are retained, but their further ancestry and metadata are unknown.",
                "Native parents in the original snapshot lack repository identity and are assumed local to the issue.",
                "No comments, dependency states, default-branch proof, or historical timelines.",
                "Planning collection time is supplied by the operator; it was not recorded by the original scripts."]}}


def render(report: dict) -> str:
    lines = ["# Backlog census", "", f"Snapshot: {report['collected_at']}", "",
             "| Repository | P0 | P1 | P2+ | Tickets | Work units | P0 units | P1 units | P2+ units |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for repo, counts in report["repositories"].items():
        values = [counts["tickets_by_priority"][p] for p in SCOPE] + [counts["tickets"], counts["work_units"]] + [counts["units_by_priority"][p] for p in SCOPE]
        lines.append(f"| {repo} | " + " | ".join(map(str, values)) + " |")
    lines.extend(["", f"Total: {len(report['tickets'])} tickets in {len(report['units'])} work units.", "",
                  "Units belong to the root repository and take the highest member priority. Ticket totals belong to the ticket repository.",
                  "", "## Evidence coverage", "", "```json", json.dumps(report["coverage"], indent=2), "```",
                  "", "## Candidates", "", "Candidates require source review and a saved Hindsight cross-check. Title similarity does not authorize closure.", ""])
    for c in report["candidates"]:
        links = ", ".join(f"[{k}]({url(k)})" for k in c["tickets"])
        receipt = c.get("hindsight")
        state = "sources reviewed" if receipt and receipt.get("sources_reviewed") is True else "recall saved, sources pending" if receipt else "pending"
        lines.append(f"- `{c['id']}` {c['kind']}: {links}. Hindsight: {state}.")
    for name in ("flow", "reconciliation", "diagnostics"):
        if name in report:
            lines.extend(["", f"## {name.capitalize()}", "", "```json", json.dumps(report[name], indent=2), "```"])
    return "\n".join(lines) + "\n"


def render_writes(rows: list[dict]) -> str:
    lines = ["# Phase 1 write list", "", "Dry run only. No GitHub issues changed. The Chief of Staff must audit this list before Phase 1.",
             "Rows without a chosen parent or completed evidence review remain unresolved.", "",
             "| Row | Action | Issue | Target | Status | Proof |", "|---|---|---|---|---|---|"]
    for row in rows:
        proofs = ", ".join(f"[source {n}]({p})" for n, p in enumerate(row["proof"], 1))
        lines.append(f"| {row['id']} | {row['action']} | [{row['issue']}]({url(row['issue'])}) | {row.get('target') or 'unresolved'} | {row['status']} | {proofs} |")
    lines.extend(["", "## Preconditions and saved evidence", "", "```json", json.dumps(rows, indent=2), "```"])
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--snapshot", type=Path)
    source.add_argument("--planning-raw", type=Path)
    parser.add_argument("--planning-time", help="Explicit provenance timestamp for legacy data, not a new collection")
    parser.add_argument("--baseline-report", type=Path)
    parser.add_argument("--before", type=Path)
    parser.add_argument("--since")
    parser.add_argument("--until")
    parser.add_argument("--hindsight", type=Path, help="Saved candidate/row ID to recall receipt mapping")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.planning_raw and not args.planning_time:
        parser.error("--planning-raw requires --planning-time")
    if bool(args.since) != bool(args.until):
        parser.error("--since and --until must be supplied together")
    if args.before and not args.since:
        parser.error("--before requires --since and --until")
    data = import_planning(args.planning_raw, args.planning_time) if args.planning_raw else json.loads(args.snapshot.read_text())
    snapshot = Snapshot.parse(data)
    hindsight = json.loads(args.hindsight.read_text()) if args.hindsight else {}
    report = rollup(snapshot)
    found = candidates(snapshot)
    for c in found:
        c["hindsight"] = hindsight.get(c["id"])
    report["candidates"] = found
    if args.since:
        before = Snapshot.parse(json.loads(args.before.read_text())) if args.before else None
        report["flow"] = flow(snapshot, args.since, args.until, before)
    if args.baseline_report:
        report["reconciliation"] = reconcile(report, json.loads(args.baseline_report.read_text()))
    writes = write_list(snapshot, found, hindsight)
    report["write_list"] = writes
    args.output.mkdir(parents=True, exist_ok=True)
    for name, value in (("snapshot.json", data), ("census.json", report), ("phase1-write-list.json", writes)):
        (args.output / name).write_text(json.dumps(value, indent=2) + "\n")
    (args.output / "census.md").write_text(render(report))
    (args.output / "phase1-write-list.md").write_text(render_writes(writes))
    print(json.dumps({"tickets": len(report["tickets"]), "work_units": len(report["units"]),
                      "candidates": len(found), "write_rows": len(writes), "output": str(args.output)}))


if __name__ == "__main__":
    main()

"""Offline fixtures for the read-only GitHub collector."""

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import fetch_open_issues as fetch


REPO = "RobTand/prismabuild"
SINCE = "2026-10-18T00:00:00Z"
NOW = "2026-10-25T00:00:00Z"


def page(nodes, cursor=None):
    return {"nodes": nodes, "pageInfo": {"hasNextPage": cursor is not None, "endCursor": cursor}}


def node(number=1, *, priority="P1", state="OPEN", parent=None, updated=NOW):
    return {"number": number, "title": "An issue", "body": "", "state": state,
            "createdAt": SINCE, "updatedAt": updated, "closedAt": NOW if state == "CLOSED" else None,
            "url": f"https://github.com/{REPO}/issues/{number}", "repository": {"nameWithOwner": REPO},
            "parent": parent, "labels": page([{"name": priority}])}


class FakeAPI:
    def __init__(self):
        self.calls = []
        self.graphql_results = []
        self.rest_results = {}
        self.page_results = {}

    def graphql(self, query, **variables):
        self.calls.append(("graphql", query, variables))
        return self.graphql_results.pop(0)

    def rest(self, path):
        self.calls.append(("rest", path))
        return self.rest_results[path]

    def pages(self, path):
        self.calls.append(("pages", path))
        return self.page_results[path]


class PaginationTests(unittest.TestCase):
    def test_rest_pages_do_not_truncate_a_full_page(self):
        api = fetch.GitHub.__new__(fetch.GitHub)
        api.rest = unittest.mock.Mock(side_effect=[list(range(100)), [100]])
        self.assertEqual(api.pages("repos/a/b/issues"), list(range(101)))
        self.assertEqual(api.rest.call_args_list[1].args, ("repos/a/b/issues?per_page=100&page=2",))

    def test_graphql_nonadvancing_cursor_is_error(self):
        with self.assertRaises(fetch.GitHubError):
            fetch.advance(page([], "same"), "same")
        with self.assertRaises(fetch.GitHubError):
            fetch.advance({"pageInfo": {"hasNextPage": True, "endCursor": None}}, None)

    def test_list_collects_all_open_and_stops_updated_scan_at_since(self):
        api = FakeAPI()
        collector = fetch.Collector(api, SINCE, [REPO])
        def repository(values, cursor=None):
            return {"repository": {"defaultBranchRef": {"name": "main", "target": {"oid": "head"}},
                                   "issues": page(values, cursor)}}
        api.graphql_results = [repository([node(1)], "next"), repository([node(2, updated="2026-01-01T00:00:00Z")]),
                               repository([node(3, state="CLOSED"), node(4, updated="2026-01-01T00:00:00Z")], "ignored")]
        collector.repository_issues(REPO, ["OPEN"])
        collector.repository_issues(REPO, None)
        self.assertEqual(set(collector.nodes), {f"{REPO}#1", f"{REPO}#2", f"{REPO}#3"})
        self.assertEqual(api.calls[1][2]["cursor"], "next")
        self.assertEqual(collector.repositories[REPO], {"default_branch": "main", "default_oid": "head"})

    def test_labels_over_100_use_complete_rest_listing(self):
        api = FakeAPI()
        value = node()
        value["labels"] = page([{"name": "irrelevant"}], "more")
        api.page_results[f"repos/{REPO}/issues/1/labels"] = [{"name": "P1"}, {"name": "last"}]
        self.assertEqual(fetch.Collector(api, SINCE).labels(value), ["P1", "last"])


class BoundaryTests(unittest.TestCase):
    def test_graphql_errors_are_not_treated_as_empty_data(self):
        with tempfile.TemporaryDirectory() as directory:
            api = fetch.GitHub(directory)
            result = subprocess.CompletedProcess([], 0, json.dumps({"data": {}, "errors": [{"message": "denied"}]}), "")
            with patch.object(fetch.subprocess, "run", return_value=result):
                with self.assertRaises(fetch.GitHubError):
                    api.graphql(fetch.ISSUE_QUERY, owner="a", name="b", number=1)
            receipts = list(Path(directory).glob("*.json"))
            self.assertEqual(len(receipts), 1)
            self.assertEqual(json.loads(receipts[0].read_text())["response"]["errors"][0]["message"], "denied")

    def test_only_get_and_queries_reach_the_process(self):
        with tempfile.TemporaryDirectory() as directory:
            api = fetch.GitHub(directory)
            with self.assertRaises(ValueError):
                api.graphql("mutation { closeIssue }")
            with self.assertRaises(ValueError):
                api.rest("user")
            result = subprocess.CompletedProcess([], 0, "[]", "")
            with patch.object(fetch.subprocess, "run", return_value=result) as run:
                api.rest("repos/a/b/issues")
            command = run.call_args.args[0]
            self.assertEqual(command[command.index("--method") + 1], "GET")

    def test_timeout_preserves_a_failure_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            api = fetch.GitHub(directory)
            with patch.object(fetch.subprocess, "run", side_effect=subprocess.TimeoutExpired([], 120)):
                with self.assertRaises(fetch.GitHubError):
                    api.rest("repos/a/b/issues")
            record = json.loads(next(Path(directory).glob("*.json")).read_text())
            self.assertEqual(record["returncode"], 124)

    def test_failed_rest_call_preserves_raw_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            api = fetch.GitHub(directory)
            result = subprocess.CompletedProcess([], 1, "", "HTTP 403")
            with patch.object(fetch.subprocess, "run", return_value=result):
                with self.assertRaises(fetch.GitHubError):
                    api.rest("repos/a/b/issues")
            self.assertEqual(json.loads(next(Path(directory).glob("*.json")).read_text())["stderr"], "HTTP 403")


class EvidenceTests(unittest.TestCase):
    def test_fix_needs_closing_relationship_and_default_ancestry(self):
        api = FakeAPI()
        target = f"{REPO}#1"
        pull = {"url": f"https://github.com/{REPO}/pull/9", "merged": True, "mergeCommit": {"oid": "merge"},
                "closingIssuesReferences": page([{"number": 1, "repository": {"nameWithOwner": REPO}}])}
        api.graphql_results = [{"repository": {"pullRequest": pull}}]
        api.rest_results[f"repos/{REPO}/compare/merge...head"] = {"status": "ahead"}
        collector = fetch.Collector(api, SINCE)
        collector.repositories[REPO] = {"default_branch": "main", "default_oid": "head"}
        proof = collector.pull_fix(REPO, 9, target)
        self.assertTrue(proof["on_default"])
        self.assertTrue(proof["closes_issue"])
        other = collector.pull_fix(REPO, 9, f"{REPO}#2")
        self.assertFalse(other["closes_issue"])
        self.assertEqual(len(api.calls), 2)

    def test_diverged_or_behind_default_is_not_proof(self):
        for status in ("diverged", "behind"):
            with self.subTest(status=status):
                api = FakeAPI()
                api.graphql_results = [{"repository": {"pullRequest": {
                    "url": "pr", "merged": True, "mergeCommit": {"oid": "merge"}, "closingIssuesReferences": page([])}}}]
                api.rest_results[f"repos/{REPO}/compare/merge...head"] = {"status": status}
                collector = fetch.Collector(api, SINCE)
                collector.repositories[REPO] = {"default_oid": "head"}
                self.assertFalse(collector.pull_fix(REPO, 9, f"{REPO}#1")["on_default"])

    def test_closing_reference_pagination_is_not_truncated(self):
        api = FakeAPI()
        def pull(refs, cursor=None):
            return {"repository": {"pullRequest": {"url": "pr", "merged": False, "mergeCommit": None,
                                                   "closingIssuesReferences": page(refs, cursor)}}}
        api.graphql_results = [pull([], "next"), pull([{"number": 1, "repository": {"nameWithOwner": REPO}}])]
        collector = fetch.Collector(api, SINCE)
        proof = collector.pull_fix(REPO, 9, f"{REPO}#1")
        self.assertTrue(proof["closes_issue"])
        self.assertFalse(proof["on_default"])
        self.assertEqual(api.calls[1][2]["cursor"], "next")

    def test_normalize_preserves_reopens_blockers_and_parent_targets(self):
        api = FakeAPI()
        value = node(parent={"number": 2, "repository": {"nameWithOwner": REPO}})
        value["body"] = "Part of #3\nBlocked by #4 and RobTand/tessera#5"
        base = f"repos/{REPO}/issues/1"
        api.page_results = {
            f"{base}/comments": [{"html_url": "comment", "body": "Duplicate of #6"}],
            f"{base}/dependencies/blocked_by": [{"repository_url": f"https://api.github.com/repos/{REPO}", "number": 7}],
            f"{base}/timeline": [{"event": "closed", "created_at": SINCE}, {"event": "reopened", "created_at": NOW}],
        }
        issue, targets = fetch.Collector(api, SINCE).normalize(value)
        self.assertEqual(issue["native_parent"], f"{REPO}#2")
        self.assertEqual(issue["blockers"], [f"{REPO}#7"])
        self.assertTrue(issue["blockers_complete"])
        self.assertTrue(issue["timeline_complete"])
        self.assertEqual([event["kind"] for event in issue["timeline"]], ["opened", "closed", "reopened"])
        self.assertEqual(targets, {f"{REPO}#{number}" for number in (2, 3, 4, 6, 7)} | {"RobTand/tessera#5"})

    def test_collect_follows_closed_ancestors_and_records_external_gaps(self):
        api = FakeAPI()
        first = node(priority="P3", updated="2026-01-01T00:00:00Z",
                     parent={"number": 2, "repository": {"nameWithOwner": REPO}})
        second = node(2, priority="P3", state="CLOSED", updated="2026-01-01T00:00:00Z",
                      parent={"number": 3, "repository": {"nameWithOwner": "other/repo"}})
        def repository(values):
            return {"repository": {"defaultBranchRef": {"name": "main", "target": {"oid": "head"}},
                                   "issues": page(values)}}
        api.graphql_results = [repository([first]), repository([first]), {"repository": {"issue": second}}]
        snapshot = fetch.Collector(api, SINCE, [REPO]).collect()
        self.assertEqual([value["number"] for value in snapshot["issues"]], [1, 2])
        self.assertTrue(snapshot["coverage"]["all_open_issues"])
        self.assertFalse(snapshot["coverage"]["atomic_snapshot"])
        self.assertEqual(len(snapshot["coverage"]["limitations"]), 1)
        self.assertIn("other/repo#3", snapshot["coverage"]["limitations"][0])

    def test_related_issue_outside_scope_is_not_claimed_complete(self):
        api = FakeAPI()
        issue, _ = fetch.Collector(api, SINCE).normalize(node(priority="P3", updated="2026-01-01T00:00:00Z"))
        self.assertFalse(issue["blockers_complete"])
        self.assertFalse(issue["comments_complete"])
        self.assertFalse(issue["timeline_complete"])
        self.assertEqual(api.calls, [])


if __name__ == "__main__":
    unittest.main()

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from census import Graph, Issue, Snapshot, candidates, flow, import_planning, reconcile, rollup, write_list

START = "2026-10-18T00:00:00Z"
END = "2026-10-25T00:00:00Z"
REPO = "RobTand/prismabuild"


def issue(number, **updates):
    row = {"repo": REPO, "number": number, "title": "[P1] task", "body": "", "state": "OPEN",
           "labels": [], "native_parent": None, "created_at": START, "updated_at": END,
           "closed_at": None, "timeline": [], "timeline_complete": True, "comments_complete": True}
    row.update(updates)
    return row


def snapshot(*rows, at=END, repositories=None):
    return Snapshot.parse({"schema": "backlog-census/v1", "collected_at": at, "issues": rows,
                           "repositories": repositories or {}})


def k(number, repo=REPO):
    return f"{repo}#{number}"


class ScopeTests(unittest.TestCase):
    def test_priority_label_wins_even_when_it_excludes_title(self):
        i = Issue.parse(issue(1, title="[P0] task", labels=["P2"]))
        self.assertEqual(i.priority, "P2")
        self.assertFalse(i.in_scope)

    def test_exact_priority_labels_and_title_fallback(self):
        rows = [issue(1, title=" [P2+] included"), issue(2, title="[P2] excluded"),
                issue(3, title="[P4] excluded", labels=["P1"]), issue(4, title="prefix [P0] excluded"),
                issue(5, title="[P1] closed", state="CLOSED"), issue(6, title="[P3] excluded")]
        self.assertEqual(set(rollup(snapshot(*rows))["tickets"]), {k(1), k(3)})

    def test_multiple_labels_choose_highest_and_report_conflict(self):
        report = rollup(snapshot(issue(1, labels=["P1", "P0"])))
        self.assertEqual(report["tickets"][k(1)]["priority"], "P0")
        self.assertEqual(report["diagnostics"][0]["kind"], "priority_conflict")

    def test_invalid_external_data_fails(self):
        for row in (issue(0), issue(1, state="UNKNOWN"), issue(1, native_parent="#2")):
            with self.assertRaises(ValueError):
                snapshot(row)
        with self.assertRaises(ValueError):
            snapshot(issue(1), issue(1))


class GraphTests(unittest.TestCase):
    def test_native_over_text_and_closed_intermediate_to_open_ancestor(self):
        s = snapshot(issue(1, native_parent=k(2), body="Part of #4"),
                     issue(2, state="CLOSED", native_parent=k(3)),
                     issue(3, title="[P3] out of scope parent"), issue(4))
        graph = Graph(s.issues)
        self.assertEqual(graph.trace(k(1))[0], k(3))
        self.assertEqual(graph.links[k(1)].source, "native")
        self.assertEqual(graph.trace(k(1))[1][0]["kind"], "native_text_conflict")

    def test_cross_repo_numbers_do_not_collide(self):
        repo2 = "RobTand/tessera"
        s = snapshot(issue(1, body=f"Sub-issue of {repo2}#1"), issue(1, repo=repo2, title="[P0] root"))
        report = rollup(s)
        self.assertEqual(report["tickets"][k(1)]["root"], k(1, repo2))
        self.assertEqual(report["repositories"][repo2]["units_by_priority"]["P0"], 1)
        self.assertEqual(report["repositories"][REPO]["work_units"], 0)

    def test_full_url_and_multilevel_text(self):
        s = snapshot(issue(1, body="- Part of: https://github.com/RobTand/prismabuild/issues/2"),
                     issue(2, body="Sub-issue of #3"), issue(3))
        self.assertEqual(Graph(s.issues).trace(k(1))[0], k(3))

    def test_parent_declaration_keeps_d50_descendants_together(self):
        repo = "RobTand/prismaquant"
        s = snapshot(issue(2411, repo=repo),
                     issue(2430, repo=repo, body="Parent: #2411. D50 item 4."),
                     issue(2458, repo=repo, body="Part of #2430"),
                     issue(2911, repo=repo, native_parent=k(2430, repo)))
        self.assertEqual(set(rollup(s)["units"]), {k(2411, repo)})

    def test_split_from_does_not_declare_parent(self):
        s = snapshot(issue(1, body="Split from #2"), issue(2))
        self.assertEqual(Graph(s.issues).links[k(1)].source, "none")

    def test_closed_native_parent_never_falls_back_to_text(self):
        s = snapshot(issue(1, native_parent=k(2), body="Part of #3"), issue(2, state="CLOSED"), issue(3))
        self.assertEqual(Graph(s.issues).trace(k(1))[0], k(1))

    def test_unknown_and_ambiguous_parent_diagnostics(self):
        s = snapshot(issue(1, body="Part of #2\nSub-issue of #3"), issue(4, native_parent=k(99)))
        report = rollup(s)
        self.assertEqual(len(report["units"]), 2)
        self.assertEqual({d["kind"] for d in report["diagnostics"]}, {"ambiguous_parent", "unknown_parent"})

    def test_follow_up_and_tracked_by_are_not_parent_relationships(self):
        s = snapshot(issue(1, body="Follow-up to #2\nTracked by #2"), issue(2))
        self.assertEqual(len(rollup(s)["units"]), 2)

    def test_cycles_and_tails_have_one_deterministic_root(self):
        rows = [issue(1, native_parent=k(2)), issue(2, native_parent=k(1)), issue(3, native_parent=k(2))]
        first = rollup(snapshot(*rows))
        self.assertEqual(list(first["units"]), [k(1)])
        self.assertEqual(first["units"], rollup(snapshot(*reversed(rows)))["units"])

    def test_each_ticket_occurs_once_and_priority_totals_add_up(self):
        s = snapshot(issue(1, title="[P0] high", native_parent=k(2)), issue(2), issue(3, title="[P2+] low"))
        report = rollup(s)
        self.assertEqual(sum(len(u["members"]) for u in report["units"].values()), 3)
        self.assertEqual(report["units"][k(2)]["priority"], "P0")
        counts = report["repositories"][REPO]
        self.assertEqual(sum(counts["units_by_priority"].values()), counts["work_units"])


class EvidenceTests(unittest.TestCase):
    def test_similarity_does_not_propose_close(self):
        s = snapshot(issue(1), issue(2))
        found = candidates(s)
        self.assertEqual(found[0]["kind"], "duplicate_cluster")
        self.assertFalse(any(r["action"] == "close" for r in write_list(s, found)))

    def test_three_similar_titles_are_one_cluster(self):
        s = snapshot(issue(1), issue(2), issue(3), issue(4, title="[P1] distinct task"))
        clusters = [c for c in candidates(s) if c["kind"] == "duplicate_cluster"]
        self.assertEqual([c["tickets"] for c in clusters], [[k(1), k(2), k(3)]])

    def test_similarity_clusters_are_transitive_and_order_independent(self):
        rows = [issue(1, title="a" * 70 + "x" * 10),
                issue(2, title="a" * 70 + "x" * 5 + "y" * 5),
                issue(3, title="a" * 70 + "y" * 10), issue(4, title="distinct")]
        first = [c for c in candidates(snapshot(*rows)) if c["kind"] == "duplicate_cluster"]
        self.assertEqual([c["tickets"] for c in first], [[k(1), k(2), k(3)]])
        second = [c for c in candidates(snapshot(*reversed(rows))) if c["kind"] == "duplicate_cluster"]
        self.assertEqual(first, second)

    def test_done_when_checklist_and_prose_are_not_completion_claims(self):
        s = snapshot(issue(1, body="Done when:\n- [ ] fixed behavior is tested\nImplementation will be completed."))
        self.assertFalse(any(c["kind"] == "claim_without_evidence" for c in candidates(s)))

    def test_completion_claims_are_scoped_to_comments_or_body_status(self):
        s = snapshot(issue(1, body="Status: fixed in the serving adapter"),
                     issue(2, comments=[{"url": "https://github.com/RobTand/prismabuild/issues/2#issuecomment-1",
                                          "body": "This is resolved by the new adapter."}]),
                     issue(3, body="Status: landed in https://github.com/RobTand/prismabuild/pull/4"),
                     issue(4, body="We will test what was fixed in the previous version."))
        claims = [c for c in candidates(s) if c["kind"] == "claim_without_evidence"]
        self.assertEqual([c["tickets"] for c in claims], [[k(1)], [k(2)]])

    def test_closed_parent_with_open_ancestor_proposes_known_target(self):
        s = snapshot(issue(1, native_parent=k(2)), issue(2, state="CLOSED", native_parent=k(3)),
                     issue(3))
        row = next(r for r in write_list(s, []) if r["issue"] == k(1))
        self.assertEqual((row["action"], row["target"], row["status"]),
                         ("reparent", k(3), "needs_hindsight"))
        self.assertIn("https://github.com/RobTand/prismabuild/issues/3", row["proof"])

    def test_closed_parent_with_ambiguous_ancestry_stays_unresolved(self):
        s = snapshot(issue(1, native_parent=k(2)),
                     issue(2, state="CLOSED", body="Part of #3\nPart of #4"), issue(3), issue(4))
        row = next(r for r in write_list(s, []) if r["issue"] == k(1))
        self.assertEqual((row["action"], row["target"]), ("create_parent", None))

    def test_default_branch_fix_requires_issue_link_ancestry_and_matching_head(self):
        proof = {"proof": "https://github.com/RobTand/prismabuild/pull/3", "commit": "abc",
                 "default_oid": "head", "on_default": True, "closes_issue": True}
        for change in ({}, {"on_default": False}, {"closes_issue": False}, {"default_oid": "old"}):
            s = snapshot(issue(1, fixes=[{**proof, **change}]), repositories={REPO: {"default_oid": "head"}})
            closes = [r for r in write_list(s, candidates(s)) if r["action"] == "close"]
            self.assertEqual(len(closes), int(not change))
            if closes:
                self.assertEqual(closes[0]["status"], "needs_hindsight")

    def test_no_vacuous_or_unknown_dead_chain(self):
        for blockers, complete in (([], True), ([k(2)], False), ([k(99)], True)):
            s = snapshot(issue(1, labels=["P1", "tl:blocked"], blockers=blockers, blockers_complete=complete),
                         issue(2, state="CLOSED"))
            self.assertFalse(any(c["kind"] == "dead_blocked_chain" for c in candidates(s)))

    def test_open_text_blocker_prevents_native_closed_chain_proposal(self):
        s = snapshot(issue(1, labels=["P1", "ig:blocked"], blockers=[k(2)], blockers_complete=True,
                           body="Depends on #3"), issue(2, state="CLOSED"), issue(3))
        self.assertFalse(any(c["kind"] == "dead_blocked_chain" for c in candidates(s)))

    def test_multiline_and_unknown_blockers_prevent_removal(self):
        for body in ("Blockers:\n- #2\n- #3", "Depends on approval from Rob", "A dependency says blocked by #3",
                     "Depends on #2 and approval from Rob"):
            s = snapshot(issue(1, labels=["P1", "ig:blocked"], blockers=[k(2)], blockers_complete=True,
                               body=body), issue(2, state="CLOSED"), issue(3))
            self.assertFalse(any(c["kind"] == "dead_blocked_chain" for c in candidates(s)))

    def test_all_closed_blockers_propose_label_removal(self):
        s = snapshot(issue(1, labels=["P1", "ig:blocked"], blockers=[k(2)], blockers_complete=True),
                     issue(2, state="CLOSED"))
        writes = write_list(s, candidates(s))
        row = next(r for r in writes if r["action"] == "remove_blocked_labels")
        self.assertEqual(row["labels"], ["ig:blocked"])
        self.assertEqual(row["status"], "needs_hindsight")

    def test_saved_recall_does_not_mark_sources_reviewed(self):
        s = snapshot(issue(1, body="Part of #2"), issue(2))
        first = write_list(s, [])
        row = first[0]
        receipts = {row["id"]: {"sha256": "receipt", "sources_reviewed": False}}
        self.assertEqual(write_list(s, [], receipts)[0]["status"], "needs_hindsight_source_review")

    def test_closed_parent_and_standalone_need_design(self):
        s = snapshot(issue(1, native_parent=k(2)), issue(2, state="CLOSED"), issue(3))
        rows = write_list(s, [])
        self.assertEqual({r["action"] for r in rows}, {"create_parent", "link_parent"})
        self.assertTrue(all(r["target"] is None for r in rows))

    def test_text_conversion_requires_no_cycle_or_ambiguity(self):
        s = snapshot(issue(1, body="Part of #2"), issue(2))
        row = next(r for r in write_list(s, []) if r["action"] == "native_sub_issue")
        self.assertEqual(row["target"], k(2))
        cycle = snapshot(issue(1, body="Part of #2"), issue(2, body="Part of #1"))
        self.assertFalse(write_list(cycle, []))


class FlowTests(unittest.TestCase):
    def test_boundaries_and_reopen(self):
        events = [{"kind": kind, "at": at} for kind, at in (
            ("opened", START), ("closed", "2026-10-19T00:00:00Z"), ("reopened", END),
            ("closed", "2026-10-26T00:00:00Z"))]
        result = flow(snapshot(issue(1, timeline=events)), START, END)
        self.assertEqual(result["ticket_events_current_scope"], {"opened": 0, "closed": 1, "reopened": 1})
        self.assertEqual(result["event_net_current_scope"], 0)
        self.assertIsNone(result["endpoint_flow"])

    def test_work_unit_flow_uses_actual_endpoints(self):
        before = snapshot(issue(1), issue(2, native_parent=k(1)), at=START)
        after = snapshot(issue(1, state="CLOSED"), issue(2), issue(3, native_parent=k(2)))
        result = flow(after, START, END, before)["endpoint_flow"]
        self.assertEqual(result["tickets"]["net"], 0)
        self.assertEqual(result["units"], {"entered": [k(2)], "left": [k(1)], "net": 0})
        with self.assertRaises(ValueError):
            flow(after, "2026-10-17T00:00:00Z", END, before)

    def test_incomplete_timelines_and_future_window_are_not_exact(self):
        s = snapshot(issue(1, timeline_complete=False))
        self.assertIsNone(flow(s, START, END)["event_net_current_scope"])
        with self.assertRaises(ValueError):
            flow(s, START, "2026-10-26T00:00:00Z")


class CliTests(unittest.TestCase):
    def test_legacy_unknown_open_parent_is_not_an_invented_work_unit(self):
        row = {"number": 1, "title": "[P1] task", "body": "", "labels": {"nodes": []},
               "parent": {"number": 99, "state": "OPEN"}, "createdAt": START, "updatedAt": END}
        with tempfile.TemporaryDirectory() as tmp:
            for repo in ("prismaquant", "prismabuild", "tessera"):
                Path(tmp, repo + ".json").write_text(json.dumps([row] if repo == "prismabuild" else []))
            imported = Snapshot.parse(import_planning(Path(tmp), END))
        self.assertNotIn(k(99), imported.issues)
        self.assertEqual(list(rollup(imported)["units"]), [k(1)])

    def test_snapshot_to_both_documents_and_machine_readable_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data = {"schema": "backlog-census/v1", "collected_at": END, "issues": [issue(1)]}
            (base / "input.json").write_text(json.dumps(data))
            run = subprocess.run([sys.executable, str(Path(__file__).with_name("census.py")),
                                  "--snapshot", str(base / "input.json"), "--output", str(base / "out")],
                                 check=True, capture_output=True, text=True)
            self.assertEqual(json.loads(run.stdout)["tickets"], 1)
            self.assertIn("1 tickets in 1 work units", (base / "out/census.md").read_text())
            writes = json.loads((base / "out/phase1-write-list.json").read_text())
            self.assertEqual(writes[0]["action"], "link_parent")

    def test_baseline_reconciliation_records_each_changed_membership(self):
        report = rollup(snapshot(issue(1), issue(3)))
        old = [{"repo": "prismabuild", "in_scope": 2, "work_units": 1, "units": {"2": [1, 2]}}]
        changes = reconcile(report, old)["differences"]
        self.assertEqual({c["kind"] for c in changes}, {"root_changed", "entered_scope", "left_scope"})


if __name__ == "__main__":
    unittest.main()

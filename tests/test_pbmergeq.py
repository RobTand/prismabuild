"""The batching merge queue judges by failure set and never posts untested heads (#1417)."""

from __future__ import annotations

import importlib.util
import json
import os
import tempfile
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
FLEET = ROOT / "tools" / "fleet"
sys.path.insert(0, str(FLEET))
SPEC = importlib.util.spec_from_file_location("pbmergeq", FLEET / "pbmergeq.py")
assert SPEC is not None and SPEC.loader is not None
mq = importlib.util.module_from_spec(SPEC)
sys.modules["pbmergeq"] = mq
SPEC.loader.exec_module(mq)  # type: ignore[union-attr]
pbtest_outcomes = mq.load_outcomes(FLEET / "pbtest.py")

SHA = "0123456789abcdef0123456789abcdef01234567"


def shard(files, rows=(), ran=True, record=True, never_ran=(), missing=()):
    files = list(files)
    rows = list(rows) or [(f + "::test_one", "call", "passed", None, None)
                          for f in files if f not in missing]
    represented = {row[0].split("::", 1)[0] for row in rows}
    rows += [(f + "::test_one", "call", "passed", None, None)
             for f in files if f not in represented and f not in missing]
    selected = list(dict.fromkeys([r[0] for r in rows if r[1] != "collect"] + list(never_ran)))
    counts = {}
    for row in rows:
        counts[row[2]] = counts.get(row[2], 0) + 1
    summary = ", ".join(f"{n} {category}" for category, n in counts.items()) or "no tests ran"
    out = summary + " in 1.0s" if ran else ""
    if record and ran:
        out += "\n" + pbtest_outcomes.PREFIX + json.dumps({"schema": pbtest_outcomes.SCHEMA,
            "rootdir_relative": ".", "collect_only": False, "collected": selected,
            "deselected": [], "collect_seen": [], "reports": [list(row) for row in rows], "uncounted": []})
    return {"files": files, "ran": ran, "output": out, "receipt_path": "/r/x",
            "returncode": 1 if not ran or any(r[2] in ("failed", "error") for r in rows) else 0,
            "timed_out": False, "action_key": "k-" + files[0]}


def test_a_report_reduces_to_failing_node_ids_and_unobserved_files():
    report = [
        shard(["tests/test_a.py"], rows=[
            ("tests/test_a.py::t_ok", "call", "passed", None, None),
            ("tests/test_a.py::t_bad", "call", "failed", None, None),
            ("tests/test_a.py::t_err", "setup", "error", None, None),
            ("tests/test_a.py::t_skip", "setup", "skipped", "why", None)],
            never_ran=["tests/test_a.py::t_lost"]),
        shard(["tests/test_b.py"], rows=[("tests/test_b.py", "collect", "error", None, None)]),
        shard(["tests/test_c.py"], ran=False),
        shard(["tests/test_d.py"], record=False),
        shard(["tests/test_e.py"], missing=["tests/test_e.py"]),
    ]
    result = mq.reduce_report(report, pbtest_outcomes,
                              requested_files=[f for row in report for f in row["files"]])
    assert result.failed == {"tests/test_a.py::t_bad", "tests/test_a.py::t_err",
                             "tests/test_a.py::t_lost (never ran)", "tests/test_b.py"}
    assert result.inconclusive == ["tests/test_a.py", "tests/test_c.py", "tests/test_d.py", "tests/test_e.py"]
    assert mq.files_of(result.failed) == ["tests/test_a.py", "tests/test_b.py"]
    assert result.actions == ["k-tests/test_a.py", "k-tests/test_b.py", "k-tests/test_c.py",
                              "k-tests/test_d.py", "k-tests/test_e.py"]


@pytest.mark.parametrize("culprit", range(1, 9))
def test_prefix_bisection_finds_the_first_failing_prefix(culprit):
    probes = []

    def fails(k):
        probes.append(k)
        return k >= culprit

    assert mq.first_failing_prefix(8, fails) == culprit
    assert len(probes) <= 3 and 8 not in probes


def test_descriptions_fit_githubs_limit_and_name_the_verdict():
    long_node = "tests/test_" + "x" * 300 + ".py::test_y"
    green = mq.describe("b00012", SHA, [], 23, "dl380g10:~/pbmergeq/prismaquant/batches/b00012")
    red = mq.describe("b00012", SHA, [long_node, "b"], 0, "")
    assert len(green) <= mq.DESCRIPTION_LIMIT and len(red) <= mq.DESCRIPTION_LIMIT
    assert green.startswith("b00012 base 01234567: 0 new failing node IDs vs main (set diff")
    assert "batches/b00012" in green
    assert red.startswith("b00012 base 01234567: 2 new failing vs main (set diff)")


def config(tmp_path, **extra):
    raw = {"repo": "RobTand/proj", "context": "pb-tests", "author": "RobTand",
           "state_dir": str(tmp_path / "state"), "pbtest": str(FLEET / "pbtest.py"),
           "client_python": sys.executable, "test_python": "/py",
           "pbtest_args": ["--priority", "-10", "--timeout-s", "60"], **extra}
    path = tmp_path / "config.json"
    path.write_text(json.dumps(raw))
    return mq._pbmergeq_load_config(path)


def test_config_refuses_runs_without_a_deadline_or_with_queue_owned_flags(tmp_path):
    with pytest.raises(SystemExit, match="--timeout-s"):
        config(tmp_path, pbtest_args=["--priority", "-10"])
    with pytest.raises(SystemExit, match="--checkout"):
        config(tmp_path, pbtest_args=["--priority", "-10", "--timeout-s", "9",
                                      "--checkout", "x"])


def test_only_same_repository_heads_by_the_author_are_eligible(tmp_path):
    cfg = config(tmp_path)
    good = {"state": "OPEN", "isDraft": False, "isCrossRepository": False,
            "headRepositoryOwner": {"login": "RobTand"}, "author": {"login": "RobTand"},
            "baseRefName": "main"}
    assert mq.eligibility(good, cfg) is None
    for change, word in [({"isCrossRepository": True}, "fork"),
                         ({"headRepositoryOwner": {"login": "eve"}}, "owner"),
                         ({"author": {"login": "eve"}}, "author"),
                         ({"isDraft": True}, "draft"),
                         ({"state": "MERGED"}, "MERGED"),
                         ({"baseRefName": "dev"}, "base")]:
        assert word in mq.eligibility({**good, **change}, cfg)


# --------------------------------------------------------------------------
# A whole batch against real Git, a fake GitHub and a fake pbtest.


def git(*args, cwd):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True,
                          text=True).stdout.strip()


class Origin:
    """A repository with ``main`` and ``refs/pull/N/head`` like GitHub's."""

    def __init__(self, root: Path):
        self.vehicles = {}
        self.vehicle_creations = []
        self.work = root / "work"
        self.work.mkdir(parents=True)
        git("init", "-q", "-b", "main", cwd=self.work)
        git("config", "user.email", "t@t", cwd=self.work)
        git("config", "user.name", "t", cwd=self.work)
        (self.work / "base.txt").write_text("base\n")
        (self.work / "tests").mkdir()
        for name in ("test_a.py", "test_b.py"):
            (self.work / "tests" / name).write_text("")
        git("add", "-A", cwd=self.work)
        git("commit", "-q", "-m", "base", cwd=self.work)

    def pr(self, number: int, files: dict[str, str]) -> str:
        # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
        git("checkout", "-q", "-b", f"pr{number}", "main", cwd=self.work)
        for name, text in files.items():
            (self.work / name).parent.mkdir(parents=True, exist_ok=True)
            (self.work / name).write_text(text)
        git("add", "-A", cwd=self.work)
        git("commit", "-q", "-m", f"pr {number}", cwd=self.work)
        sha = git("rev-parse", "HEAD", cwd=self.work)
        # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
        git("update-ref", f"refs/pull/{number}/head", sha, cwd=self.work)
        # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
        git("checkout", "-q", "main", cwd=self.work)
        return sha

    def advance_main(self) -> None:
        (self.work / "moved.txt").write_text("moved\n")
        git("add", "-A", cwd=self.work)
        git("commit", "-q", "-m", "moved", cwd=self.work)


class FakeGitHub(mq.GitHub):
    def __init__(self, cfg, store, origin, *, dry=False):
        super().__init__(cfg, store, dry=dry)
        self.origin, self.calls, self.merged = origin, [], []
        self.issue_link_state = "success"
        self.comments = []

    def contains(self, sha):
        return subprocess.run(["git", "merge-base", "--is-ancestor", sha, "main"],
                              cwd=self.origin.work, capture_output=True).returncode == 0

    def pr(self, number):
        vehicle = next((v for v in self.origin.vehicles.values()
                        if v["number"] == number), None)
        if vehicle is not None:
            return dict(vehicle)
        # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
        sha = git("rev-parse", f"refs/pull/{number}/head", cwd=self.origin.work)
        return {"number": number,
                "state": "MERGED" if number in self.merged or self.contains(sha) else "OPEN",
                "isDraft": False, "isCrossRepository": False,
                "headRepositoryOwner": {"login": "RobTand"}, "author": {"login": "RobTand"},
                "baseRefName": "main", "headRefOid": sha, "headRefName": f"pr{number}",
                "title": f"pr {number}", "body": f"Refs #{number + 100}",
                "closingIssuesReferences": [{"number": number + 100}],
                "url": f"https://github.com/{self.cfg.repo}/pull/{number}"}

    def find_vehicle(self, branch):
        found = self.origin.vehicles.get(branch)
        return dict(found) if found is not None else None

    def issue(self, number):
        return {"number": number}

    def create_vehicle(self, branch, title, body, *, draft=False):
        self.calls.append(["vehicle", "create", branch])
        self.origin.vehicle_creations.append(branch)
        number = 10000 + len(self.origin.vehicle_creations)
        vehicle = {"number": number, "state": "OPEN", "headRefOid": git(
            "rev-parse", branch, cwd=self.origin.work), "headRefName": branch,
            "url": f"https://github.com/{self.cfg.repo}/pull/{number}",
            "title": title, "body": body, "isDraft": draft}
        self.origin.vehicles[branch] = vehicle
        return dict(vehicle)

    def issue_link(self, number, sha):
        assert self.pr(number)["headRefOid"] == sha
        return self.issue_link_state

    def close_vehicle(self, batch, number):
        self.calls.append(["vehicle", "close", str(number)])
        for vehicle in self.origin.vehicles.values():
            if vehicle["number"] == number:
                vehicle["state"] = "CLOSED"
                return True
        return False

    def _write(self, what, command, stdin=None):
        if self.dry:
            return super()._write(what, command, stdin)
        self.calls.append(command)
        if command[:3] == ["gh", "pr", "comment"]:
            self.comments.append((int(command[3]), stdin))
        if command[:3] == ["gh", "pr", "ready"]:
            vehicle = next(v for v in self.origin.vehicles.values()
                           if v["number"] == int(command[3]))
            vehicle["isDraft"] = "--undo" in command
        if command[:3] == ["gh", "pr", "merge"]:
            number = int(command[3])
            pr = self.pr(number)
            matched = command[command.index("--match-head-commit") + 1]
            if matched != pr["headRefOid"]:
                return False
            ancestor = subprocess.run(["git", "merge-base", "--is-ancestor",
                                       "main", matched], cwd=self.origin.work,
                                      capture_output=True)
            if ancestor.returncode != 0:
                return False
            git("merge", "--no-ff", "-q", "-m", f"Merge PR #{number}", matched,
                cwd=self.origin.work)
            self.merged.append(number)
            for vehicle in self.origin.vehicles.values():
                if vehicle["number"] == number:
                    vehicle["state"] = "MERGED"
        return True

    def file_flake(self, batch, nodeid, candidate):
        self.calls.append(["flake", nodeid])
        return "#999"


class FakeRunner:
    """Each checkout file ``fail-<anything>`` holds node IDs that fail there.

    A line ``flaky:<node>`` fails only on a checkout's first run. Like
    pbtest, a job naming a file the checkout lacks observes nothing.
    """

    def __init__(self):
        self.runs = []
        self.seen = set()

    def discover(self, checkout):
        return sorted(str(f.relative_to(checkout))
                      for f in Path(checkout).glob("tests/test_*.py")), []

    def run_many(self, batch, outdir, jobs):
        results = {}
        for name, checkout, files in jobs:
            self.runs.append((name, sorted(files)))
            if any(not (Path(checkout) / f).exists() for f in files):
                results[name] = mq.RunResult(failed=set(), inconclusive=list(files),
                                             files=list(files), receipts=[])
                continue
            failed = set()
            for path in Path(checkout).glob("fail-*"):
                for line in path.read_text().split():
                    flaky = line.startswith("flaky:")
                    node = line.removeprefix("flaky:")
                    if mq.node_file(node) not in files:
                        continue
                    if flaky and (node, path.name) in self.seen:
                        continue
                    self.seen.add((node, path.name))
                    failed.add(node)
            results[name] = mq.RunResult(failed=failed, inconclusive=[], files=list(files),
                                         receipts=[f"/receipts/{name}"], report="", wall_s=1.0)
        return results


def make_queue(tmp_path, mode="status"):
    origin = Origin(tmp_path / "origin")
    cfg = config(tmp_path, remote=str(origin.work))
    store = mq.Store(cfg.state_dir)
    mirror = mq.Mirror(cfg, cfg.state_dir)
    mirror.ensure()
    github = FakeGitHub(cfg, store, origin, dry=mode == "dry-run")
    runner = FakeRunner()
    return mq.Queue(cfg, store, github, mirror, runner, mode), origin, github, runner


def statuses(github, context="pb-tests"):
    return [(c[4].rsplit("/", 1)[1], c[6].split("=", 1)[1]) for c in github.calls
            if c[:2] == ["gh", "api"] and c[8] == f"context={context}"]


def entries(github, *numbers):
    return [{"pr": n, "sha": github.pr(n)["headRefOid"], "at": "now"}
            for n in numbers]


def test_green_batch_opens_a_vehicle_and_posts_required_success_only_on_candidate(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path)
    one = origin.pr(1, {"one.txt": "1\n"})
    two = origin.pr(2, {"two.txt": "2\n"})
    # main already fails a test: shared, not new.
    (origin.work / "fail-main").write_text("tests/test_a.py::old\n")
    git("add", "-A", cwd=origin.work)
    git("commit", "-q", "-m", "red main", cwd=origin.work)
    batch = queue.run_batch(entries(github, 1, 2))
    assert batch["verdict"] == "green", batch
    assert batch["shared"] == ["tests/test_a.py::old"] and batch["new"] == []
    assert statuses(github) == [(batch["candidate"], "success")]
    vehicle = batch["vehicle"]
    assert vehicle["branch"] == f"release-mq/{batch['id']}-{batch['candidate'][:12]}"
    assert github.pr(vehicle["number"])["headRefOid"] == batch["candidate"]
    body = github.pr(vehicle["number"])["body"]
    assert all(text in body for text in ("#1", one, "Refs #101", "#2", two, "Refs #102"))
    informational = [c for c in github.calls if c[:2] == ["gh", "api"]
                     and c[8] != "context=pb-tests"]
    assert {c[4].rsplit("/", 1)[1] for c in informational} == {one, two}
    assert not [c for c in github.calls if c[:3] == ["gh", "pr", "merge"]]
    queue.github.post_status(batch["id"], batch["candidate"], "success", batch["description"])
    assert statuses(github) == [(batch["candidate"], "success")]


def test_red_batch_blames_the_culprit_only_and_requeues_the_rest(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    bad = origin.pr(2, {"fail-2": "tests/test_b.py::broken\n"})
    origin.pr(3, {"three.txt": "3\n"})
    batch = queue.run_batch(entries(github, 1, 2, 3))
    assert batch["verdict"] == "red", batch
    assert batch["culprit"]["pr"] == 2 and batch["new"] == ["tests/test_b.py::broken"]
    assert statuses(github) == [(bad, "failure")]
    comments = [c for c in github.calls if c[:3] == ["gh", "pr", "comment"]]
    assert [c[3] for c in comments] == ["2"]
    assert [e["pr"] for e in queue.store.state["queue"]] == [1, 3]
    # Bisection and the base ran only the failing file.
    assert all(files == ["tests/test_b.py"] for name, files in runner.runs if name != "candidate")


def test_a_new_test_file_is_judged_where_it_exists_and_passes_where_it_does_not(tmp_path):
    # b00006: a pull request that adds a failing test file. The base and the
    # prefixes before it lack the file; asking pbtest for it there made the
    # whole judgment inconclusive and wedged the queue.
    queue, origin, github, runner = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    bad = origin.pr(2, {"tests/test_new.py": "", "fail-2": "tests/test_new.py::broken\n"})
    batch = queue.run_batch(entries(github, 1, 2))
    assert batch["verdict"] == "red", batch
    assert batch["culprit"]["pr"] == 2 and batch["new"] == ["tests/test_new.py::broken"]
    assert statuses(github) == [(bad, "failure")]
    assert [e["pr"] for e in queue.store.state["queue"]] == [1]
    # Only checkouts that hold the file were asked to run it.
    assert ("base", ["tests/test_new.py"]) not in runner.runs
    assert ("prefix1", ["tests/test_new.py"]) not in runner.runs


def test_a_flake_is_recorded_and_does_not_block(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path)
    one = origin.pr(1, {"fail-1": "flaky:tests/test_a.py::wobbly\n"})
    batch = queue.run_batch(entries(github, 1))
    assert batch["verdict"] == "green" and batch["flakes"] == ["tests/test_a.py::wobbly"]
    assert ["flake", "tests/test_a.py::wobbly"] in github.calls
    assert statuses(github) == [(batch["candidate"], "success")]
    assert queue.store.state["flakes"]["tests/test_a.py::wobbly"]["issue"] == "#999"


def test_a_conflicting_pr_drops_out_untested_and_the_rest_continue(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path)
    one = origin.pr(1, {"base.txt": "one\n"})
    origin.pr(2, {"base.txt": "two\n"})
    batch = queue.run_batch(entries(github, 1, 2))
    assert [p["pr"] for p in batch["included"]] == [1]
    assert batch["dropped"][0]["why"] == "conflicts with an earlier batch member"
    assert statuses(github) == [(batch["candidate"], "success")]
    assert [e["pr"] for e in queue.store.state["queue"]] == [2]


def test_dry_run_posts_nothing(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path, mode="dry-run")
    origin.pr(1, {"fail-1": "tests/test_a.py::broken\n"})
    batch = queue.run_batch(entries(github, 1))
    assert batch["verdict"] == "red"
    assert github.calls == [] and queue.store.ledger() == []
    assert any(w.startswith("post pb-tests=failure on ") for w in github.would)


def test_fake_github_refuses_a_behind_member_under_strict_protection(tmp_path):
    queue, origin, github, _ = make_queue(tmp_path, mode="merge")
    one = origin.pr(1, {"one.txt": "1\n"})
    two = origin.pr(2, {"two.txt": "2\n"})
    assert github.merge("strict-proof", 1, one)
    assert not github.merge("strict-proof", 2, two)
    assert github.pr(1)["state"] == "MERGED"
    assert github.pr(2)["state"] == "OPEN"


def test_merge_mode_merges_the_tested_vehicle_and_confirms_each_member(tmp_path):
    queue, origin, github, _ = make_queue(tmp_path, mode="merge")
    one = origin.pr(1, {"one.txt": "1\n"})
    two = origin.pr(2, {"two.txt": "2\n"})
    batch = queue.run_batch(entries(github, 1, 2))
    assert batch["verdict"] == "merged", batch
    vehicle = batch["vehicle"]
    merges = [c for c in github.calls if c[:3] == ["gh", "pr", "merge"]]
    assert [(c[3], c[-1]) for c in merges] == [(str(vehicle["number"]), batch["candidate"])]
    assert statuses(github) == [(batch["candidate"], "success")]
    assert git("rev-parse", "main^{tree}", cwd=origin.work) == batch["candidate_tree"]
    assert [github.pr(n)["state"] for n in (1, 2, vehicle["number"])] == ["MERGED"] * 3
    assert github.contains(one) and github.contains(two)
    ledger = queue.store.ledger()
    assert all(any(row.get("pr") == n and row.get("sha") == sha for row in ledger)
               for n, sha in ((1, one), (2, two)))
    assert any(row.get("sha") == batch["candidate"] and
               (row.get("pr") == vehicle["number"] or row.get("number") == vehicle["number"])
               for row in ledger)


def test_main_move_during_vehicle_merge_closes_it_and_requeues_members_uncharged(
        tmp_path, monkeypatch):
    queue, origin, github, _ = make_queue(tmp_path, mode="merge")
    origin.pr(1, {"one.txt": "1\n"})
    origin.pr(2, {"two.txt": "2\n"})
    approved = [{**entry, "attempts": 2} for entry in entries(github, 1, 2)]
    write = github._write

    def move_before_merge(what, command, stdin=None):
        if command[:3] == ["gh", "pr", "merge"]:
            origin.advance_main()
        return write(what, command, stdin)

    monkeypatch.setattr(github, "_write", move_before_merge)
    batch = queue.run_batch(approved)
    assert batch["verdict"] == "green-stale", batch
    assert github.pr(batch["vehicle"]["number"])["state"] == "CLOSED"
    assert [(e["pr"], e["sha"], e["attempts"]) for e in queue.store.state["queue"]] == [
        (e["pr"], e["sha"], 2) for e in approved]
    assert github.merged == []


def test_restart_after_vehicle_creation_recovers_it_without_opening_another(
        tmp_path, monkeypatch):
    queue, origin, github, runner = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    create = github.create_vehicle

    def crash_after_create(*args, **kwargs):
        create(*args, **kwargs)
        raise SystemExit("restart before vehicle receipt")

    monkeypatch.setattr(github, "create_vehicle", crash_after_create)
    with pytest.raises(SystemExit, match="restart before vehicle receipt"):
        queue.run_batch(entries(github, 1))
    assert len(origin.vehicle_creations) == 1
    completed = list(runner.runs)
    store = mq.Store(queue.cfg.state_dir)
    github = FakeGitHub(queue.cfg, store, origin)
    resumed = mq.Queue(queue.cfg, store, github, queue.mirror, runner, "status")
    resumed.resume()
    resumed.tick()
    assert len(origin.vehicle_creations) == 1
    assert runner.runs == completed
    assert len(origin.vehicles) == 1
    assert next(iter(origin.vehicles.values()))["state"] == "CLOSED"
    assert store.state["batch"] is None


def test_pending_issue_link_resumes_the_same_vehicle_without_retesting(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path, mode="merge")
    origin.pr(1, {"one.txt": "1\n"})
    github.issue_link_state = "pending"
    pending = queue.run_batch(entries(github, 1))
    assert pending["verdict"] == "waiting", pending
    assert not [c for c in github.calls if c[:3] == ["gh", "pr", "merge"]]
    vehicle = dict(pending["vehicle"])
    completed = list(runner.runs)

    github.issue_link_state = "success"
    merged = queue.tick()
    assert merged["verdict"] == "merged", merged
    assert merged["id"] == pending["id"] and merged["vehicle"] == vehicle
    assert len(origin.vehicle_creations) == 1
    assert runner.runs == completed
    assert [github.pr(n)["state"] for n in (1, vehicle["number"])] == ["MERGED", "MERGED"]


def test_restart_reconciles_a_vehicle_merged_before_its_local_receipt(tmp_path, monkeypatch):
    queue, origin, github, runner = make_queue(tmp_path, mode="merge")
    one = origin.pr(1, {"one.txt": "1\n"})
    write = github._write

    def crash_after_merge(what, command, stdin=None):
        accepted = write(what, command, stdin)
        if command[:3] == ["gh", "pr", "merge"] and accepted:
            raise SystemExit("restart before merge receipt")
        return accepted

    monkeypatch.setattr(github, "_write", crash_after_merge)
    with pytest.raises(SystemExit, match="restart before merge receipt"):
        queue.run_batch(entries(github, 1))
    vehicle = dict(next(iter(origin.vehicles.values())))
    assert vehicle["state"] == "MERGED"
    assert not any(row.get("kind") == "merge" for row in queue.store.ledger())
    completed = list(runner.runs)

    store = mq.Store(queue.cfg.state_dir)
    github = FakeGitHub(queue.cfg, store, origin)
    resumed = mq.Queue(queue.cfg, store, github, queue.mirror, runner, "merge")
    resumed.resume()
    merged = resumed.tick()
    assert merged["verdict"] == "merged", merged
    assert merged["vehicle"]["number"] == vehicle["number"]
    assert len(origin.vehicle_creations) == 1 and runner.runs == completed
    assert not [c for c in github.calls if c[:3] == ["gh", "pr", "merge"]]
    for number, sha in ((1, one), (vehicle["number"], vehicle["headRefOid"])):
        assert any(row.get("kind") == "merge" and row.get("pr") == number
                   and row.get("sha") == sha for row in store.ledger())


def test_refused_candidate_status_blocks_merge_and_retries_the_same_candidate(
        tmp_path, monkeypatch):
    queue, origin, github, runner = make_queue(tmp_path, mode="merge")
    origin.pr(1, {"one.txt": "1\n"})
    write = github._write
    refused = []

    def refuse_first_required_status(what, command, stdin=None):
        if (command[:2] == ["gh", "api"] and "context=pb-tests" in command
                and not refused):
            refused.append(command)
            return False
        return write(what, command, stdin)

    monkeypatch.setattr(github, "_write", refuse_first_required_status)
    pending = queue.run_batch(entries(github, 1))
    assert pending["verdict"] == "waiting", pending
    candidate, vehicle = pending["candidate"], dict(pending["vehicle"])
    assert not [c for c in github.calls if c[:3] == ["gh", "pr", "merge"]]
    assert not any(row.get("kind") == "status" and row.get("context") == "pb-tests"
                   and row.get("sha") == candidate for row in queue.store.ledger())
    completed = list(runner.runs)

    merged = queue.tick()
    assert merged["verdict"] == "merged", merged
    assert merged["candidate"] == candidate and merged["vehicle"] == vehicle
    assert len(origin.vehicle_creations) == 1 and runner.runs == completed
    assert statuses(github) == [(candidate, "success")]


def test_a_head_moved_during_testing_closes_the_vehicle_and_requires_new_review(
        tmp_path, monkeypatch):
    queue, origin, github, runner = make_queue(tmp_path, mode="merge")
    original = origin.pr(1, {"one.txt": "1\n"})
    run_many = runner.run_many

    def push_during_test(batch, outdir, jobs):
        results = run_many(batch, outdir, jobs)
        # pb-history-ok: Updates the temporary fake-origin branch and pull ref during its own test run.
        git("checkout", "-q", "pr1", cwd=origin.work)
        (origin.work / "one.txt").write_text("unreviewed\n")
        git("commit", "-q", "-am", "push after approval", cwd=origin.work)
        moved = git("rev-parse", "HEAD", cwd=origin.work)
        # pb-history-ok: Updates the temporary fake-origin branch and pull ref during its own test run.
        git("update-ref", "refs/pull/1/head", moved, cwd=origin.work)
        # pb-history-ok: Returns the temporary fake-origin fixture to its main branch.
        git("checkout", "-q", "main", cwd=origin.work)
        return results

    monkeypatch.setattr(runner, "run_many", push_during_test)
    batch = queue.run_batch(entries(github, 1))
    assert batch["verdict"] != "merged", batch
    assert batch["included"][0]["sha"] == original
    assert github.pr(1)["headRefOid"] != original
    assert github.pr(batch["vehicle"]["number"])["state"] == "CLOSED"
    assert not [c for c in github.calls if c[:3] == ["gh", "pr", "merge"]]
    assert github.comments == [(1, "head moved after approval; re-review and enqueue the new SHA")]
    assert queue.store.state["queue"] == []
    assert statuses(github) == []


def test_selected_head_is_fetched_when_github_pull_ref_lags(tmp_path, monkeypatch):
    queue, origin, github, runner = make_queue(tmp_path)
    stale = origin.pr(1, {"one.txt": "old\n"})
    # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
    git("checkout", "-q", "pr1", cwd=origin.work)
    (origin.work / "one.txt").write_text("new\n")
    git("commit", "-q", "-am", "branch advanced before pull ref", cwd=origin.work)
    selected = git("rev-parse", "HEAD", cwd=origin.work)
    # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
    git("checkout", "-q", "main", cwd=origin.work)
    # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
    assert git("rev-parse", "refs/pull/1/head", cwd=origin.work) == stale
    # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
    assert git("rev-parse", "pr1", cwd=origin.work) == selected
    advertised = github.pr
    monkeypatch.setattr(github, "pr",
                        lambda number: {**advertised(number), **(
                            {"headRefOid": selected} if number == 1 else {})})

    batch = queue.run_batch([{"pr": 1, "sha": selected, "at": "now"}])

    assert batch["verdict"] == "green", batch
    assert batch["dropped"] == []
    assert [(p["pr"], p["sha"]) for p in batch["included"]] == [(1, selected)]
    assert queue.mirror.rev("refs/mq/pr/1") == selected
    assert queue.mirror.git("cat-file", "-t", selected).stdout.strip() == "commit"
    assert queue.mirror.git("show", batch["candidate"] + ":one.txt").stdout == "new\n"
    assert statuses(github) == [(batch["candidate"], "success")]
    assert len(runner.runs) == 1


@pytest.mark.parametrize("object_kind", ["missing", "blob"])
def test_unfetchable_or_noncommit_selected_head_is_an_explicit_error(
        tmp_path, monkeypatch, object_kind):
    queue, origin, github, runner = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "old\n"})
    selected = (SHA if object_kind == "missing" else
                # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
                git("rev-parse", "main:base.txt", cwd=origin.work))
    advertised = github.pr
    monkeypatch.setattr(github, "pr",
                        lambda number: {**advertised(number), **(
                            {"headRefOid": selected} if number == 1 else {})})

    batch = queue.run_batch([{"pr": 1, "sha": selected, "at": "now"}])

    assert batch["verdict"] == "error", batch
    assert ("CalledProcessError" if object_kind == "missing" else
            "is not a commit") in batch["summary"]
    assert runner.runs == []
    assert github.calls == []


def test_changed_fetched_ref_is_rejected_before_composition(tmp_path, monkeypatch):
    queue, origin, github, runner = make_queue(tmp_path)
    selected = origin.pr(1, {"one.txt": "new\n"})
    # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
    base = git("rev-parse", "main", cwd=origin.work)
    original_git = queue.mirror.git

    def fetch_then_change_ref(*args, **kwargs):
        # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
        result = original_git(*args, **kwargs)
        if args[0] == "fetch":
            # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
            original_git("update-ref", "refs/mq/pr/1", base)
        return result

    monkeypatch.setattr(queue.mirror, "git", fetch_then_change_ref)
    batch = queue.run_batch([{"pr": 1, "sha": selected, "at": "now"}])

    assert batch["verdict"] == "error", batch
    assert "differs from selected" in batch["summary"]
    assert runner.runs == [] and github.calls == []


@pytest.mark.parametrize("selected", ["short", SHA.upper(), SHA + ":refs/mq/base"])
def test_selected_head_requires_a_full_lowercase_commit(tmp_path, selected):
    queue, origin, github, runner = make_queue(tmp_path)

    with pytest.raises(ValueError, match="not a full Git commit"):
        queue.mirror.fetch({1: selected})

    assert runner.runs == [] and github.calls == []


def test_an_enqueued_pin_refuses_a_later_push_and_requests_review(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    batch = queue.run_batch([{"pr": 1, "sha": SHA, "at": "now"}])
    assert batch["verdict"] == "empty" and "not the enqueued" in batch["skipped"][0]["why"]
    assert github.comments == [(1, "head moved after approval; re-review and enqueue the new SHA")]
    assert runner.runs == []
    assert statuses(github) == []


@pytest.mark.parametrize("sha", [None, "", "abc", "A" * 40])
def test_enqueue_refuses_an_absent_or_incomplete_approved_sha(tmp_path, sha):
    store = mq.Store(tmp_path / "s")
    with pytest.raises((ValueError, SystemExit), match="SHA|sha|commit"):
        store.enqueue_request(5, sha)
    assert not list(store.inbox.iterdir())
    assert store.state["queue"] == []


def test_enqueue_cli_requires_an_approved_sha(tmp_path):
    config(tmp_path)
    with pytest.raises(SystemExit) as refused:
        mq.main(["--config", str(tmp_path / "config.json"), "enqueue", "5"])
    assert refused.value.code != 0
    assert not list((tmp_path / "state" / "inbox").glob("*.json"))


def test_legacy_unpinned_queue_entry_is_refused_before_testing(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    batch = queue.run_batch([{"pr": 1, "sha": None, "at": "now"}])
    assert batch["verdict"] == "empty", batch
    assert runner.runs == []
    assert statuses(github) == []
    assert origin.vehicle_creations == []


def test_enqueue_goes_through_the_inbox_and_keeps_order(tmp_path):
    store = mq.Store(tmp_path / "s")
    store.enqueue_request(5, SHA)
    store.enqueue_request(3, SHA)
    store.enqueue_request(5, SHA)
    store.ingest()
    assert [(e["pr"], e["sha"]) for e in store.state["queue"]] == [(3, SHA), (5, SHA)]
    assert list(store.inbox.iterdir()) == []
    assert "#3" in store.status_text() and "queue (2)" in store.status_text()


def test_inconclusive_fleet_work_requeues_without_charging_attempts(tmp_path, monkeypatch):
    queue, origin, github, runner = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    approved = [{**entry, "attempts": 2} for entry in entries(github, 1)]

    def unobserved(batch, outdir, jobs):
        return {name: mq.RunResult(failed=set(), inconclusive=list(files),
                                  files=list(files), receipts=[])
                for name, _, files in jobs}

    monkeypatch.setattr(runner, "run_many", unobserved)
    monkeypatch.setattr(mq.time, "time", lambda: 1000.0)
    batch = queue.run_batch(approved)
    assert batch["verdict"] == "inconclusive", batch
    assert [(e["pr"], e["sha"], e["attempts"]) for e in queue.store.state["queue"]] == [
        (e["pr"], e["sha"], 2) for e in approved]
    assert queue.store.state["not_before"] == 1000.0 + queue.cfg.stuck_backoff_s
    assert github.calls == []


PIN_REFUSAL = ("pbtest: dependency pin refused before pytest: "
               "tools/resolve_fleetfixture_dev_pin.py: expected commit=" + "a" * 40
               + "; distribution=fleetfixture installed commit=" + "b" * 40
               + "; require a non-editable Git install at the reviewed commit "
                 "(local-directory installs do not record a Git commit)")


_COORDINATOR_REFUSAL = r"""
import importlib.util, pathlib, sys
spec = importlib.util.spec_from_file_location('native_refused_pbtest', sys.argv[1])
pbtest = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pbtest)
from prismabuild import capability_eligibility, contracts
from dataclasses import replace
configured = replace(pbtest.store_for(pbtest.__file__), root=pathlib.Path(sys.argv[2]) / 'fleet',
    queue_root=pathlib.Path(sys.argv[2]) / 'fleet' / 'pb-queue',
    cas_root=pathlib.Path(sys.argv[2]) / 'fleet' / 'cas')
pbtest.store_for = lambda *args, **kwargs: configured
missing = '/no/native-mergeq/python'
pbtest.fleet_queue().announce(host='fixture', tags=[contracts.INTERPRETER_TAG], has_gpu=False,
    capacity={'cpu': 1, 'mem_gb': 8}, timeout_ceiling_s=300,
    capabilities=capability_eligibility.OfferCapabilities(
        interpreters=capability_eligibility.InterpreterAnswers.from_observed([], [missing])))
sys.argv = ['pbtest.py', '--checkout', sys.argv[2], '--python', missing,
    '--threads-per-shard', '1', '--shards', '1', '--timeout-s', '60', '--json', sys.argv[3], 'tests']
raise SystemExit(pbtest.main())
"""


def refused_process(monkeypatch, launches, message=PIN_REFUSAL, report=...):
    original = mq.subprocess.Popen
    native = None
    temporary = Path(tempfile.mkdtemp(prefix='mergeq-refusal-'))
    if message == PIN_REFUSAL:
        from test_pbtest_dependency_pins import fixture_checkout, pbtest
        checkout, _ = fixture_checkout(temporary)
        done = subprocess.run([*pbtest.shard_entry(sys.executable, checkout), '-q', 'tests'],
            cwd=checkout, env={**os.environ, 'PYTHONPATH': 'src'}, capture_output=True, text=True, timeout=30)
        assert done.returncode == 1 and not (checkout / 'pytest-ran').exists()
        native = done.stdout + done.stderr
        decision = next(json.loads(line[len(pbtest_outcomes.PREFLIGHT_PREFIX):])
                        for line in native.splitlines() if line.startswith(pbtest_outcomes.PREFLIGHT_PREFIX))
        assert decision['runtime_refusal']['message'] == PIN_REFUSAL
    class Refused:
        def __init__(self, command, *, stdout, env, **kwargs):
            launches.append(command)
            destination = Path(command[command.index('--json') + 1])
            if message.startswith('pbtest: every recorded worker'):
                from test_pbtest_dependency_pins import fixture_checkout
                checkout, _ = fixture_checkout(temporary)
                done = subprocess.run([sys.executable, '-c', _COORDINATOR_REFUSAL,
                    str(FLEET / 'pbtest.py'), str(checkout), str(destination)],
                    env=env, capture_output=True, text=True, timeout=30)
                assert done.returncode == 2 and not destination.exists()
                captured = done.stdout + done.stderr
                self.returncode = 2
            else:
                captured = native if native is not None else message
                rows = report
                if rows is ...:
                    files = [arg for arg in command[2:] if arg.endswith('.py') and not arg.startswith('/')]
                    rows = [shard(files, ran=False)]
                if rows is not None:
                    if native is not None:
                        rows[0]['output'] = native
                    destination.write_text(json.dumps(rows))
                self.returncode = 1
            stdout.write(captured + "\n")
            stdout.flush()
            self.pid = 4242
        def wait(self):
            return self.returncode
    # Completed doubles expose readable, owned descriptors in place of pidfds.
    monkeypatch.setattr(mq.os, 'pidfd_open', lambda pid: os.open(os.devnull, os.O_RDONLY))
    def launch(command, **kwargs):
        if command[0] == 'git' or command[0] == sys.executable and command[1] == '-c':
            return original(command, **kwargs)
        return Refused(command, **kwargs)
    monkeypatch.setattr(mq.subprocess, 'Popen', launch)


def test_pin_runtime_refusal_is_not_retried_as_an_unobserved_test(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    store = mq.Store(cfg.state_dir)
    runner = mq.Runner(cfg, store)
    launches = []
    refused_process(monkeypatch, launches)
    result = runner.run_many("b00001", tmp_path / "reports",
                             [("candidate", tmp_path, ["tests/test_a.py"])])["candidate"]
    assert len(launches) == 1, "an unchanged bad runtime cannot observe another retry"
    assert result.inconclusive == ["tests/test_a.py"]  # coverage is still absent
    assert result.failed == set()  # no pytest failure was observed


def test_pin_runtime_refusal_is_named_and_does_not_charge_or_blame_pr(tmp_path, monkeypatch):
    queue, _, github, _ = make_queue(tmp_path)
    runner = mq.Runner(queue.cfg, queue.store)
    launches = []
    refused_process(monkeypatch, launches)

    def drive(batch, outdir):
        batch["included"] = [{"pr": 1, "sha": SHA}]
        return queue.checked(batch, "candidate", runner.run_many(
            batch["id"], outdir, [("candidate", tmp_path, ["tests/test_a.py"])]))

    monkeypatch.setattr(queue, "_run", drive)
    batch = queue.run_batch([{"pr": 1, "sha": SHA, "at": "now"}])
    assert batch["verdict"] == "runtime-blocked", batch
    assert queue.store.state["queue"][0]["attempts"] == 0
    assert batch["runs"]["candidate"]["runtime_refusal"] == PIN_REFUSAL
    assert batch["runs"]["candidate"]["runtime"]["python"] == queue.cfg.test_python
    assert github.calls == []  # neither success nor code-failure status


def known_red_descendant(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    bad = origin.pr(2, {"fail-2": "tests/test_b.py::broken\n"})
    # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
    git("checkout", "-q", "-b", "pr3", bad, cwd=origin.work)
    (origin.work / "three.txt").write_text("3\n")
    git("add", "-A", cwd=origin.work)
    git("commit", "-q", "-m", "descendant", cwd=origin.work)
    descendant = git("rev-parse", "HEAD", cwd=origin.work)
    # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
    git("update-ref", "refs/pull/3/head", descendant, cwd=origin.work)
    # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
    git("checkout", "-q", "main", cwd=origin.work)

    original = queue.run_batch(entries(github, 1, 2, 3))
    assert original["verdict"] == "red"
    assert original["culprit"]["sha"] == bad
    return queue, origin, github, runner, original, bad


def test_a_descendant_carrying_the_known_red_tree_is_held_without_retesting(tmp_path):
    queue, origin, github, runner, original, bad = known_red_descendant(tmp_path)
    completed = list(runner.runs)
    repeat = queue.tick()
    assert repeat["candidate_tree"] == original["candidate_tree"]
    assert repeat["verdict"] == "known-failure-blocked"
    assert runner.runs == completed
    assert repeat["known_failure"]["batch"] == original["id"]
    assert repeat["known_failure"]["new"] == original["new"]
    assert statuses(github) == [(bad, "failure")]
    assert queue.tick() is None

    # A corrected descendant still contains the historical culprit commit.
    # Its changed source must be admitted for fresh qualification.
    # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
    git("checkout", "-q", "pr3", cwd=origin.work)
    (origin.work / "fail-2").write_text("")
    git("add", "-A", cwd=origin.work)
    git("commit", "-q", "-m", "fix failure", cwd=origin.work)
    fixed = git("rev-parse", "HEAD", cwd=origin.work)
    # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
    git("update-ref", "refs/pull/3/head", fixed, cwd=origin.work)
    # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
    git("checkout", "-q", "main", cwd=origin.work)
    queue.store.add(3, fixed, front=False)
    queue.store.save()
    repaired = queue.tick()
    assert repaired["verdict"] == "green"
    assert repaired["candidate_tree"] != original["candidate_tree"]
    assert runner.runs != completed
    assert (repaired["candidate"], "success") in statuses(github)


@pytest.mark.parametrize("changed", ["runtime", "config", "base", "files"])
def test_a_held_head_is_reconsidered_when_its_qualification_domain_changes(
        tmp_path, monkeypatch, changed):
    queue, origin, _github, runner, original, _bad = known_red_descendant(tmp_path)
    held = queue.tick()
    assert held["verdict"] == "known-failure-blocked"
    completed = list(runner.runs)
    if changed == "runtime":
        runtime = mq.checkout_runtime
        monkeypatch.setattr(mq, "checkout_runtime",
                            lambda cfg, path: {**runtime(cfg, path), "python": "/repaired-py"})
    elif changed == "config":
        queue.cfg = mq.dataclasses.replace(queue.cfg,
                                          pbtest_args=["--priority", "-9", "--timeout-s", "60"])
    elif changed == "base":
        origin.advance_main()
    else:
        discover = runner.discover
        monkeypatch.setattr(runner, "discover",
                            lambda path: (discover(path)[0][1:], discover(path)[1]))
    fresh = queue.tick()
    assert fresh is not None, "the same pinned head needs fresh qualification in a new domain"
    assert fresh["verdict"] != "known-failure-blocked"
    assert runner.runs != completed
    assert held["id"] not in queue.store.state.get("known_failure_blocked", {})

@pytest.mark.parametrize("damage", ["missing", "dirty", "untracked", "retargeted"])
def test_an_untrusted_retained_view_is_rebuilt_before_suppressing_work(tmp_path, damage):
    queue, _origin, _github, runner, _original, _bad = known_red_descendant(tmp_path)
    held = queue.tick()
    view = queue.mirror.worktrees / f"{held['id']}-candidate"
    assert view.is_dir()
    completed = list(runner.runs)
    if damage == "missing":
        queue.mirror.drop(view)
    elif damage == "dirty":
        (view / "tests/test_a.py").write_text("changed\n")
    elif damage == "untracked":
        (view / "tests/test_added.py").write_text("")
    else:
        # pb-history-ok: Builds and inspects branches and pull refs created by the temporary fake-origin fixture.
        git("checkout", "-q", "--detach", held["base"], cwd=view)
    rebuilt = queue.tick()
    assert rebuilt is not None and rebuilt["id"] != held["id"]
    assert not view.exists()
    assert held["id"] not in queue.store.state["known_failure_blocked"]
    # A fresh immutable reconstruction may still match the original real
    # negative identity. It does not need another full suite merely because
    # the optional retained view was damaged.
    assert rebuilt["verdict"] == "known-failure-blocked"
    assert rebuilt["candidate_tree"] == held["candidate_tree"]
    assert runner.runs == completed


@pytest.mark.parametrize("leaves", ["empty", "closed"])
def test_a_hold_retires_its_view_when_entries_leave_the_queue(tmp_path, leaves):
    queue, _origin, github, _runner, _original, _bad = known_red_descendant(tmp_path)
    held = queue.tick()
    view = queue.mirror.worktrees / f"{held['id']}-candidate"
    if leaves == "empty":
        queue.store.state["queue"] = []
    else:
        github.merged.append(3)
    queue.store.save()
    queue.tick()
    assert not view.exists()
    assert held["id"] not in queue.store.state["known_failure_blocked"]


def test_known_failure_once_retains_no_queue_hold_or_worktree(tmp_path):
    queue, _origin, github, runner, _original, _bad = known_red_descendant(tmp_path)
    queued = list(queue.store.state["queue"])
    completed = list(runner.runs)
    queue.once = True
    held = queue.run_batch(entries(github, 1, 3))
    assert held["verdict"] == "known-failure-blocked"
    assert queue.store.state["queue"] == queued
    assert runner.runs == completed
    assert not queue.store.state.get("known_failure_blocked")
    assert not list(queue.mirror.worktrees.iterdir())


@pytest.mark.parametrize("replacement", ["foreign", "symlink"])
def test_an_unowned_retained_view_is_preserved_when_its_hold_releases(
        tmp_path, replacement):
    queue, _origin, _github, runner, _original, _bad = known_red_descendant(tmp_path)
    held = queue.tick()
    view = queue.mirror.worktrees / f"{held['id']}-candidate"
    if replacement == "foreign":
        (view / ".git").unlink()
        git("init", "-q", cwd=view)
        foreign = view
    else:
        queue.mirror.drop(view)
        foreign = tmp_path / "foreign"
        foreign.mkdir()
        git("init", "-q", cwd=foreign)
        view.symlink_to(foreign, target_is_directory=True)
    sentinel = foreign / "intervening-user-work"
    sentinel.write_text("must survive\n")
    completed = list(runner.runs)
    rebuilt = queue.tick()
    assert rebuilt is not None and rebuilt["verdict"] == "known-failure-blocked"
    assert held["id"] not in queue.store.state["known_failure_blocked"]
    assert foreign.is_dir() and sentinel.read_text() == "must survive\n"
    assert view.exists(), "unowned data is retained for explicit recovery"
    assert runner.runs == completed


@pytest.mark.parametrize(('latest', 'expected'), [
    ({'status': 'queued', 'conclusion': None}, 'pending'),
    ({'status': 'completed', 'conclusion': 'failure'}, 'failure'),
    ({'status': 'completed', 'conclusion': 'success'}, 'success'),
])
def test_issue_link_waits_for_latest_github_actions_check(tmp_path, monkeypatch, latest, expected):
    cfg = config(tmp_path)
    github = mq.GitHub(cfg, mq.Store(cfg.state_dir), dry=False)
    checks = [
        {'id': 1, 'name': 'issue-link', 'app': {'slug': 'github-actions'},
         'status': 'completed', 'conclusion': 'success'},
        {'id': 2, 'name': 'issue-link', 'app': {'slug': 'github-actions'}, **latest},
        {'id': 3, 'name': 'issue-link', 'app': {'slug': 'other-app'},
         'status': 'completed', 'conclusion': 'success'},
    ]

    def check_runs(command):
        assert command == ['gh', 'api', '--paginate',
                           f'repos/{cfg.repo}/commits/{SHA}/check-runs',
                           '--jq', '.check_runs[] | @json']
        return subprocess.CompletedProcess(command, 0, '\n'.join(map(json.dumps, checks)))

    monkeypatch.setattr(mq, 'call_tool', check_runs)
    assert github.issue_link(1, SHA) == expected


def test_vehicle_issue_refs_keep_same_repository_body_and_sidebar_links():
    pr = {'body': 'Refs #101; Fixes RobTand/prismabuild#102; '
                  'References https://github.com/RobTand/prismabuild/issues/103; '
                  'Refs https://github.com/Other/repo/issues/999',
          'closingIssuesReferences': [
              {'number': 104, 'url': 'https://github.com/RobTand/prismabuild/issues/104'},
              {'number': 999, 'url': 'https://github.com/Other/repo/issues/999'}]}
    assert mq.issue_numbers(pr, 'RobTand/prismabuild') == [101, 102, 103, 104]



def test_once_cli_restart_reconciles_the_retained_vehicle_before_a_new_batch(
        tmp_path, monkeypatch):
    queue, origin, github, runner = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    queue.once = True
    create = github.create_vehicle

    def crash_after_create(*args, **kwargs):
        create(*args, **kwargs)
        raise SystemExit("restart before vehicle receipt")

    monkeypatch.setattr(github, "create_vehicle", crash_after_create)
    monkeypatch.setattr(mq, "open_queue", lambda *args, **kwargs: queue)
    argv = ["--config", str(tmp_path / "config.json"), "once", "--mode", "status", "1"]
    with pytest.raises(SystemExit, match="restart before vehicle receipt"):
        mq.main(argv)
    completed = list(runner.runs)
    store = mq.Store(queue.cfg.state_dir)
    github = FakeGitHub(queue.cfg, store, origin)
    resumed = mq.Queue(queue.cfg, store, github, queue.mirror, runner, "status", once=True)
    monkeypatch.setattr(mq, "open_queue", lambda *args, **kwargs: resumed)

    assert mq.main(argv) == 0
    assert len(origin.vehicle_creations) == 1 and runner.runs == completed
    assert store.state["batch"] is None


def test_member_push_during_vehicle_merge_requests_review_and_unblocks_queue(
        tmp_path, monkeypatch):
    queue, origin, github, _ = make_queue(tmp_path, mode='merge')
    approved = origin.pr(1, {'one.txt': '1\n'})
    write = github._write

    def merge_then_push(what, command, stdin=None):
        accepted = write(what, command, stdin)
        if command[:3] == ['gh', 'pr', 'merge'] and accepted:
            # pb-history-ok: Push a new head to this test's already-composed fake PR.
            git('checkout', '-q', 'pr1', cwd=origin.work)
            (origin.work / 'one.txt').write_text('unreviewed\n')
            git('commit', '-q', '-am', 'push during vehicle merge', cwd=origin.work)
            moved = git('rev-parse', 'HEAD', cwd=origin.work)
            # pb-history-ok: Simulate the GitHub PR ref updating after the vehicle merged.
            git('update-ref', 'refs/pull/1/head', moved, cwd=origin.work)
            # pb-history-ok: Restore the fake origin fixture to its main branch.
            git('checkout', '-q', 'main', cwd=origin.work)
        return accepted

    monkeypatch.setattr(github, '_write', merge_then_push)
    batch = queue.run_batch(entries(github, 1))
    assert batch['verdict'] == 'merged-head-moved', batch
    assert github.pr(batch['vehicle']['number'])['state'] == 'MERGED'
    assert github.pr(1)['state'] == 'OPEN'
    assert github.comments == [(1, 'head moved after approval; re-review and enqueue the new SHA')]
    assert not any(row.get('kind') == 'merge' and row.get('pr') == 1
                   for row in queue.store.ledger())
    assert any(row.get('kind') == 'member-head-moved' and row.get('pr') == 1
               and row.get('sha') == approved for row in queue.store.ledger())
    assert queue.store.state['batch'] is None

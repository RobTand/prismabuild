"""The batching merge queue judges by failure set and never posts untested heads (#1417)."""

from __future__ import annotations

import importlib.util
import json
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
    out = "== 1 passed in 1.0s =="
    if record:
        out += "\n" + pbtest_outcomes.PREFIX + json.dumps(
            {"schema": pbtest_outcomes.SCHEMA, "reports": [list(r) for r in rows]})
    return {"files": list(files), "ran": ran, "output": out, "receipt_path": "/r/x",
            "action_key": "k-" + files[0],
            "reconciliation": {"never_ran": list(never_ran), "missing_files": list(missing)}}


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
    result = mq.reduce_report(report, pbtest_outcomes)
    assert result.failed == {"tests/test_a.py::t_bad", "tests/test_a.py::t_err",
                             "tests/test_a.py::t_lost (never ran)", "tests/test_b.py"}
    assert result.inconclusive == ["tests/test_c.py", "tests/test_d.py", "tests/test_e.py"]
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
    return mq.load_config(path)


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
        git("checkout", "-q", "-b", f"pr{number}", "main", cwd=self.work)
        for name, text in files.items():
            (self.work / name).parent.mkdir(parents=True, exist_ok=True)
            (self.work / name).write_text(text)
        git("add", "-A", cwd=self.work)
        git("commit", "-q", "-m", f"pr {number}", cwd=self.work)
        sha = git("rev-parse", "HEAD", cwd=self.work)
        git("update-ref", f"refs/pull/{number}/head", sha, cwd=self.work)
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

    def pr(self, number):
        sha = git("rev-parse", f"refs/pull/{number}/head", cwd=self.origin.work)
        return {"number": number, "state": "MERGED" if number in self.merged else "OPEN",
                "isDraft": False, "isCrossRepository": False,
                "headRepositoryOwner": {"login": "RobTand"}, "author": {"login": "RobTand"},
                "baseRefName": "main", "headRefOid": sha, "title": f"pr {number}"}

    def _write(self, what, command, stdin=None):
        if self.dry:
            return super()._write(what, command, stdin)
        self.calls.append(command)
        if command[:3] == ["gh", "pr", "merge"]:
            self.merged.append(int(command[3]))
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


def statuses(github):
    return [(c[4].rsplit("/", 1)[1], c[6].split("=", 1)[1]) for c in github.calls
            if c[:2] == ["gh", "api"]]


def entries(*numbers):
    return [{"pr": n, "sha": None, "at": "now"} for n in numbers]


def test_green_batch_posts_once_on_each_tested_head_and_merges_nothing(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path)
    one = origin.pr(1, {"one.txt": "1\n"})
    two = origin.pr(2, {"two.txt": "2\n"})
    # main already fails a test: shared, not new.
    (origin.work / "fail-main").write_text("tests/test_a.py::old\n")
    git("add", "-A", cwd=origin.work)
    git("commit", "-q", "-m", "red main", cwd=origin.work)
    batch = queue.run_batch(entries(1, 2))
    assert batch["verdict"] == "green", batch
    assert batch["shared"] == ["tests/test_a.py::old"] and batch["new"] == []
    assert statuses(github) == [(one, "success"), (two, "success")]
    assert not [c for c in github.calls if c[:3] == ["gh", "pr", "merge"]]
    # A restart that re-runs the batch's posting posts nothing twice.
    for p in batch["included"]:
        queue.github.post_status(batch["id"], p["sha"], "success", batch["description"])
    assert len(statuses(github)) == 2


def test_red_batch_blames_the_culprit_only_and_requeues_the_rest(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    bad = origin.pr(2, {"fail-2": "tests/test_b.py::broken\n"})
    origin.pr(3, {"three.txt": "3\n"})
    batch = queue.run_batch(entries(1, 2, 3))
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
    batch = queue.run_batch(entries(1, 2))
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
    batch = queue.run_batch(entries(1))
    assert batch["verdict"] == "green" and batch["flakes"] == ["tests/test_a.py::wobbly"]
    assert ["flake", "tests/test_a.py::wobbly"] in github.calls
    assert statuses(github) == [(one, "success")]
    assert queue.store.state["flakes"]["tests/test_a.py::wobbly"]["issue"] == "#999"


def test_a_conflicting_pr_drops_out_untested_and_the_rest_continue(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path)
    one = origin.pr(1, {"base.txt": "one\n"})
    origin.pr(2, {"base.txt": "two\n"})
    batch = queue.run_batch(entries(1, 2))
    assert [p["pr"] for p in batch["included"]] == [1]
    assert batch["dropped"][0]["why"] == "conflicts with an earlier batch member"
    assert statuses(github) == [(one, "success")]
    assert [e["pr"] for e in queue.store.state["queue"]] == [2]


def test_dry_run_posts_nothing(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path, mode="dry-run")
    origin.pr(1, {"fail-1": "tests/test_a.py::broken\n"})
    batch = queue.run_batch(entries(1))
    assert batch["verdict"] == "red"
    assert github.calls == [] and queue.store.ledger() == []
    assert any(w.startswith("post pb-tests=failure on ") for w in github.would)


def test_merge_mode_merges_in_order_only_while_main_is_the_tested_base(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path, mode="merge")
    one = origin.pr(1, {"one.txt": "1\n"})
    two = origin.pr(2, {"two.txt": "2\n"})
    batch = queue.run_batch(entries(1, 2))
    assert batch["verdict"] == "merged", batch
    merges = [c for c in github.calls if c[:3] == ["gh", "pr", "merge"]]
    assert [(c[3], c[-1]) for c in merges] == [("1", one), ("2", two)]

    queue2, origin2, github2, _ = make_queue(tmp_path / "moved", mode="merge")
    origin2.pr(1, {"one.txt": "1\n"})
    real = queue2.mirror.remote_base
    queue2.mirror.remote_base = lambda: (origin2.advance_main(), real())[1]
    batch = queue2.run_batch(entries(1))
    assert batch["verdict"] == "green-stale"
    assert not [c for c in github2.calls if c[:3] == ["gh", "pr", "merge"]]
    assert [e["pr"] for e in queue2.store.state["queue"]] == [1]


def test_selected_head_is_fetched_when_github_pull_ref_lags(tmp_path, monkeypatch):
    queue, origin, github, runner = make_queue(tmp_path)
    stale = origin.pr(1, {"one.txt": "old\n"})
    git("checkout", "-q", "pr1", cwd=origin.work)
    (origin.work / "one.txt").write_text("new\n")
    git("commit", "-q", "-am", "branch advanced before pull ref", cwd=origin.work)
    selected = git("rev-parse", "HEAD", cwd=origin.work)
    git("checkout", "-q", "main", cwd=origin.work)
    assert git("rev-parse", "refs/pull/1/head", cwd=origin.work) == stale
    assert git("rev-parse", "pr1", cwd=origin.work) == selected
    advertised = github.pr
    monkeypatch.setattr(github, "pr",
                        lambda number: {**advertised(number), "headRefOid": selected})

    batch = queue.run_batch([{"pr": 1, "sha": selected, "at": "now"}])

    assert batch["verdict"] == "green", batch
    assert batch["dropped"] == []
    assert [(p["pr"], p["sha"]) for p in batch["included"]] == [(1, selected)]
    assert queue.mirror.rev("refs/mq/pr/1") == selected
    assert queue.mirror.git("cat-file", "-t", selected).stdout.strip() == "commit"
    assert queue.mirror.git("show", batch["candidate"] + ":one.txt").stdout == "new\n"
    assert statuses(github) == [(selected, "success")]
    assert len(runner.runs) == 1


@pytest.mark.parametrize("object_kind", ["missing", "blob"])
def test_unfetchable_or_noncommit_selected_head_is_an_explicit_error(
        tmp_path, monkeypatch, object_kind):
    queue, origin, github, runner = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "old\n"})
    selected = (SHA if object_kind == "missing" else
                git("rev-parse", "main:base.txt", cwd=origin.work))
    advertised = github.pr
    monkeypatch.setattr(github, "pr",
                        lambda number: {**advertised(number), "headRefOid": selected})

    batch = queue.run_batch([{"pr": 1, "sha": selected, "at": "now"}])

    assert batch["verdict"] == "error", batch
    assert ("CalledProcessError" if object_kind == "missing" else
            "is not a commit") in batch["summary"]
    assert runner.runs == []
    assert github.calls == []


def test_changed_fetched_ref_is_rejected_before_composition(tmp_path, monkeypatch):
    queue, origin, github, runner = make_queue(tmp_path)
    selected = origin.pr(1, {"one.txt": "new\n"})
    base = git("rev-parse", "main", cwd=origin.work)
    original_git = queue.mirror.git

    def fetch_then_change_ref(*args, **kwargs):
        result = original_git(*args, **kwargs)
        if args[0] == "fetch":
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


def test_an_enqueued_pin_refuses_a_later_push(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    batch = queue.run_batch([{"pr": 1, "sha": SHA, "at": "now"}])
    assert batch["verdict"] == "empty" and "not the enqueued" in batch["skipped"][0]["why"]
    assert github.calls == []


def test_enqueue_goes_through_the_inbox_and_keeps_order(tmp_path):
    store = mq.Store(tmp_path / "s")
    store.enqueue_request(5, None)
    store.enqueue_request(3, SHA)
    store.enqueue_request(5, SHA)
    store.ingest()
    assert [(e["pr"], e["sha"]) for e in store.state["queue"]] == [(3, SHA), (5, SHA)]
    assert list(store.inbox.iterdir()) == []
    assert "#3" in store.status_text() and "queue (2)" in store.status_text()


def test_a_merge_github_keeps_refusing_leaves_the_queue_after_max_attempts(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path, mode="merge")
    origin.pr(1, {"one.txt": "1\n"})
    origin.pr(2, {"two.txt": "2\n"})
    refuse = github._write

    def refusing(what, command, stdin=None):
        if command[:4] == ["gh", "pr", "merge", "1"]:
            return False
        return refuse(what, command, stdin)

    github._write = refusing
    queue.store.add(1, None, front=False)
    queue.store.add(2, None, front=False)
    verdicts = []
    while queue.store.state["queue"] and len(verdicts) < 6:
        verdicts.append(queue.tick()["verdict"])
    # #1 is refused three times, then dropped with a comment; #2, bumped by it,
    # is never charged and merges once #1 is gone.
    assert verdicts == ["partial"] * 3 + ["merged"], verdicts
    comments = [c for c in github.calls if c[:4] == ["gh", "pr", "comment", "1"]]
    assert len(comments) == 1
    assert [c for c in github.calls
            if c[:4] == ["gh", "pr", "merge", "2"]][0][:4] == ["gh", "pr", "merge", "2"]


PIN_REFUSAL = ("pbtest: dependency pin refused before pytest: "
               "tools/resolve_prismabuild_dev_pin.py: expected commit=" + "a" * 40
               + "; distribution=prismabuild installed commit=" + "b" * 40)


def refused_process(monkeypatch, launches, message=PIN_REFUSAL, report=None):
    original = mq.subprocess.Popen

    class Refused:
        def __init__(self, command, *, stdout, **kwargs):
            launches.append(command)
            stdout.write(message + "\n")
            stdout.flush()
            if report is not None:
                Path(command[command.index("--json") + 1]).write_text(json.dumps(report))
            self.pid, self.returncode = 4242, 1

        def poll(self):
            return self.returncode

    def launch(command, **kwargs):
        if command[0] == "git":
            return original(command, **kwargs)
        return Refused(command, **kwargs)

    monkeypatch.setattr(mq.subprocess, "Popen", launch)


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
    batch = queue.run_batch(entries(1))
    assert batch["verdict"] == "runtime-blocked", batch
    assert queue.store.state["queue"][0]["attempts"] == 0
    assert batch["runs"]["candidate"]["runtime_refusal"] == PIN_REFUSAL
    assert batch["runs"]["candidate"]["runtime"]["python"] == queue.cfg.test_python
    assert github.calls == []  # neither success nor code-failure status


def known_red_descendant(tmp_path):
    queue, origin, github, runner = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    bad = origin.pr(2, {"fail-2": "tests/test_b.py::broken\n"})
    git("checkout", "-q", "-b", "pr3", bad, cwd=origin.work)
    (origin.work / "three.txt").write_text("3\n")
    git("add", "-A", cwd=origin.work)
    git("commit", "-q", "-m", "descendant", cwd=origin.work)
    descendant = git("rev-parse", "HEAD", cwd=origin.work)
    git("update-ref", "refs/pull/3/head", descendant, cwd=origin.work)
    git("checkout", "-q", "main", cwd=origin.work)

    original = queue.run_batch(entries(1, 2, 3))
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
    git("checkout", "-q", "pr3", cwd=origin.work)
    (origin.work / "fail-2").write_text("")
    git("add", "-A", cwd=origin.work)
    git("commit", "-q", "-m", "fix failure", cwd=origin.work)
    fixed = git("rev-parse", "HEAD", cwd=origin.work)
    git("update-ref", "refs/pull/3/head", fixed, cwd=origin.work)
    git("checkout", "-q", "main", cwd=origin.work)
    queue.store.add(3, fixed, front=False)
    queue.store.save()
    repaired = queue.tick()
    assert repaired["verdict"] == "green"
    assert repaired["candidate_tree"] != original["candidate_tree"]
    assert runner.runs != completed
    assert (fixed, "success") in statuses(github)


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
    queue, _origin, _github, runner, _original, _bad = known_red_descendant(tmp_path)
    queued = list(queue.store.state["queue"])
    completed = list(runner.runs)
    queue.once = True
    held = queue.run_batch(entries(1, 3))
    assert held["verdict"] == "known-failure-blocked"
    assert queue.store.state["queue"] == queued
    assert runner.runs == completed
    assert not queue.store.state.get("known_failure_blocked")
    assert not list(queue.mirror.worktrees.iterdir())

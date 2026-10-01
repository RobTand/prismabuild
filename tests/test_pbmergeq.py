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
mq = importlib.util.module_from_spec(SPEC)
sys.modules["pbmergeq"] = mq
SPEC.loader.exec_module(mq)  # type: ignore[union-attr]
import pbtest_outcomes  # noqa: E402

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
        git("add", "-A", cwd=self.work)
        git("commit", "-q", "-m", "base", cwd=self.work)

    def pr(self, number: int, files: dict[str, str]) -> str:
        git("checkout", "-q", "-b", f"pr{number}", "main", cwd=self.work)
        for name, text in files.items():
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

    A line ``flaky:<node>`` fails only on a checkout's first run.
    """

    def __init__(self):
        self.runs = []
        self.seen = set()

    def discover(self, checkout):
        return ["tests/test_a.py", "tests/test_b.py"], []

    def run_many(self, batch, outdir, jobs):
        results = {}
        for name, checkout, files in jobs:
            self.runs.append((name, sorted(files)))
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

"""Causal P1 regressions from independent PB1427 review; no real submissions."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest
from test_pbmergeq import (
    PIN_REFUSAL,
    FakeRunner,
    config,
    entries,
    git,
    make_queue,
    mq,
    refused_process,
    shard,
    statuses,
)
from test_pbmergeq_runtime import NEW, OLD, pin, policy

from prismabuild import core


@pytest.mark.parametrize("category", ["failed", "error"])
def test_observed_nonzero_report_echoing_refusal_never_becomes_runtime_blocker(tmp_path, monkeypatch, category):
    cfg = config(tmp_path)
    node = "tests/test_a.py::observed_code_failure"
    report = shard(["tests/test_a.py"], rows=[(node, "call", category, None, None)])
    report["output"] = PIN_REFUSAL + "\n" + report["output"]
    report["receipt_path"] = None
    launches = []
    # The failed client's log echoes the same prefix as its valid report.
    refused_process(monkeypatch, launches, message=report["output"], report=[report])
    runner = mq.Runner(cfg, mq.Store(cfg.state_dir))
    got = runner.run_many("b1", tmp_path / "out", [
        ("candidate", tmp_path, ["tests/test_a.py"])])["candidate"]
    assert got.failed == {node} and not got.inconclusive
    assert not got.runtime_refusal, "observed code failure was reclassified from the client echo"
    assert len(launches) == 1


def test_concurrent_base_refusal_outranks_inconclusive_rerun_without_attempt_charge(tmp_path):
    queue, origin, github, _ = make_queue(tmp_path)
    origin.pr(1, {"fail-new": "tests/test_b.py::new\n"})

    class MixedResults(FakeRunner):
        def run_many(self, batch, outdir, jobs):
            results = super().run_many(batch, outdir, jobs)
            for name, _, files in jobs:
                if name == "rerun":
                    results[name] = mq.RunResult(set(), list(files), list(files), [],
                                                actions=["unobserved-rerun"])
                if name == "base":
                    results[name] = mq.RunResult(set(), list(files), list(files), [],
                        actions=["refused-base"], runtime_refusal=PIN_REFUSAL, log="base.log")
            return results

    queue.runner = MixedResults()
    batch = queue.run_batch([{**entries(1)[0], "attempts": 2}])
    assert batch["verdict"] == "runtime-blocked", batch
    assert queue.store.state["queue"][0]["attempts"] == 2
    assert batch["runs"]["rerun"]["actions"] == ["unobserved-rerun"]
    assert batch["runs"]["base"]["actions"] == ["refused-base"]
    assert "base" in batch["summary"]
    assert not github.calls and not queue.store.state["flakes"]
    assert queue.tick() is None


def published_fixture(tmp_path):
    generations = []
    outcomes = (Path(__file__).resolve().parents[1] / "tools/fleet/pbtest_outcomes.py").read_text()
    for generation in ("A", "B"):
        root = tmp_path / generation
        root.mkdir()
        (root / "pbtest.py").write_text(
            f"GENERATION = {generation!r}\n"
            "def discover(checkout, paths):\n"
            "    return ['tests/test_a.py', 'tests/test_b.py']\n"
            "def fleet_data_files(checkout, files):\n"
            "    return []\n")
        (root / "pbtest_outcomes.py").write_text(outcomes + f"\nGENERATION = {generation!r}\n")
        generations.append(root)
    link = tmp_path / "published"
    link.symlink_to(generations[0], target_is_directory=True)
    return generations, link


def test_generation_module_loading_uses_its_owned_path_not_process_import_cache(tmp_path):
    (a, b), link = published_fixture(tmp_path)
    cfg_a = config(tmp_path, pbtest=str(link / "pbtest.py"))
    runner_a = mq.Runner(cfg_a, mq.Store(cfg_a.state_dir))
    link.unlink()
    link.symlink_to(b, target_is_directory=True)
    cfg_b = config(tmp_path, pbtest=str(link / "pbtest.py"))
    runner_b = mq.Runner(cfg_b, runner_a.store)
    for runner, expected in [(runner_a, a), (runner_b, b)]:
        runner.discover(tmp_path)
        assert Path(runner.outcomes.__file__).parent == expected
        assert Path(runner._pbtest.__file__).parent == expected
        assert runner.outcomes.GENERATION == runner._pbtest.GENERATION == expected.name


def test_generation_replacement_mid_run_cannot_relabel_baselines_or_any_batch_phase(tmp_path, monkeypatch):
    queue, origin, github, _ = make_queue(tmp_path)
    (a, b), link = published_fixture(tmp_path)
    cfg = policy(tmp_path, pbtest=str(link / "pbtest.py"), remote=str(origin.work), inconclusive_retries=1)
    pin(origin.work, OLD)
    (origin.work / "fail-main").write_text("tests/test_a.py::old\n")
    git("add", "-A", cwd=origin.work)
    git("commit", "-q", "-m", "base pin", cwd=origin.work)
    origin.pr(1, {"one.txt": "1\n"})
    guilty = origin.pr(2, {"pins.py": f"SDK_PIN = {NEW!r}\n", "fail-new": "tests/test_b.py::new\n"})
    origin.pr(3, {"three.txt": "3\n"})
    queue.cfg = cfg
    runner = mq.Runner(cfg, queue.store)
    queue.runner = runner
    outcomes = FakeRunner()
    original = mq.subprocess.Popen
    launched = []

    class Completed:
        pid, returncode = 4242, 1

        def poll(self):
            return self.returncode

    def launch(command, **kwargs):
        if command[0] == "git":
            return original(command, **kwargs)
        report = Path(command[command.index("--json") + 1])
        name = report.stem
        launched.append((name, str(Path(command[1]).resolve()), command[1]))
        if len(launched) == 1:
            link.unlink()
            link.symlink_to(b, target_is_directory=True)
        files = [arg for arg in command if arg.startswith("tests/")]
        if name == "candidate":
            rows = [shard(files, ran=False, record=False)]
        else:
            checkout = Path(command[command.index("--checkout") + 1])
            result = outcomes.run_many("b", report.parent, [(name, checkout, files)])[name]
            rows = [shard(files, rows=[(node, "call", "failed", None, None)
                                      for node in sorted(result.failed)])]
        report.write_text(json.dumps(rows))
        kwargs["stdout"].write(rows[0]["output"] + "\n")
        kwargs["stdout"].flush()
        return Completed()

    monkeypatch.setattr(mq.subprocess, "Popen", launch)
    batch = queue.run_batch(entries(1, 2, 3))
    assert batch["verdict"] == "red" and statuses(github) == [(guilty, "failure")]
    assert {name for name, _, _ in launched} == {
        "candidate", "candidate.retry1", "rerun", "base", "prefix1", "prefix2"}
    assert all(actual == declared == str(a / "pbtest.py") for _, actual, declared in launched), launched
    assert all(run["runtime"]["pbtest"] == str(a / "pbtest.py") for run in batch["runs"].values())
    candidate = batch["runs"]["candidate"]["runtime"]
    base = batch["runs"]["base"]["runtime"]
    # Save under the launched A generation despite publication now naming B.
    a_cfg = dataclasses.replace(cfg, pbtest=a / "pbtest.py")
    expected = mq.baseline_key(a_cfg, batch["candidate_tree"], candidate)
    assert queue.store.state["history_runtime"] == expected
    assert queue.store.baseline(expected)
    assert queue.store.baseline(mq.baseline_key(a_cfg, batch["base_tree"], base))
    b_cfg = dataclasses.replace(cfg, pbtest=link / "pbtest.py")
    b_runtime = {**candidate, "pbtest": str(b / "pbtest.py")}
    assert not queue.store.baseline(mq.baseline_key(b_cfg, batch["candidate_tree"], b_runtime))


def test_version_two_post_run_generation_evidence_is_not_reused(tmp_path):
    cfg = config(tmp_path)
    runtime = mq.checkout_runtime(cfg, tmp_path)
    legacy = {"schema": "pbmergeq.baseline.v2", "tree": "tree",
              "runtime": {key: runtime[key] for key in ("python", "pins", "pin_sources")},
              "config": {**dataclasses.asdict(cfg), "pbtest_resolved": str(cfg.pbtest)}}
    legacy_key = core.canonical_sha256(json.loads(json.dumps(legacy, default=str)))
    store = mq.Store(cfg.state_dir)
    store.remember_baseline(legacy_key, ["tests/test_a.py"], ["tests/test_a.py::old"])
    assert store.baseline(legacy_key)
    assert not store.baseline(mq.baseline_key(cfg, "tree", runtime))

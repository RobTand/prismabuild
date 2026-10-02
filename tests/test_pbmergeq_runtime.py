"""Checkout-bound runtime selection and evidence reuse for mergeq's pin family (#1427)."""

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

OLD = "a" * 40
NEW = "b" * 40
DECLARATION = {"sdk": {"source": "pins.py", "name": "SDK_PIN"}}


def policy(tmp_path, **extra):
    return config(tmp_path, runtime_pins=DECLARATION,
                  test_python="/venvs/sdk-{sdk:.8}/bin/python", **extra)


def pin(checkout, value):
    (checkout / "pins.py").write_text(f"SDK_PIN: str = {value!r}\n")


def test_static_interpreter_and_queue_owned_equals_flags(tmp_path):
    cfg = config(tmp_path, test_python="/literal-{not-a-template}/python")
    runner = mq.Runner(cfg, mq.Store(cfg.state_dir))
    command = runner.command(tmp_path, ["tests/test_a.py"], tmp_path / "result.json")
    assert command[command.index("--python") + 1] == cfg.test_python
    for flag in ("--python=/wrong", "--checkout=x", "--json=x", "--history=x"):
        with pytest.raises(SystemExit, match="queue owns"):
            config(tmp_path, pbtest_args=["--priority", "-10", "--timeout-s", "60", flag])


@pytest.mark.parametrize("template", ["/{unknown}/python", "/{sdk.name}/python",
    "/{sdk[0]}/python", "/{sdk!r}/python", "/{sdk:>{other}}/python",
    "/{sdk:.0}/python", "/{sdk:.41}/python", "/{sdk/python", "relative/{sdk}", "/static"])
def test_templates_are_only_declared_full_pins_or_explicit_prefixes(tmp_path, template):
    with pytest.raises(SystemExit, match="pbmergeq:"):
        config(tmp_path, runtime_pins=DECLARATION, test_python=template)


@pytest.mark.parametrize("declaration", [[], {"sdk.x": {"source": "pins.py", "name": "PIN"}},
    {"sdk": {"source": "/pins.py", "name": "PIN"}},
    {"sdk": {"source": "../pins.py", "name": "PIN"}},
    {"sdk": {"source": "pins.py", "name": "PIN.x"}},
    {"sdk": {"source": "pins.py"}}, {"sdk": {"source": "pins.py", "name": "PIN", "extra": 1}}])
def test_pin_declarations_are_strict(tmp_path, declaration):
    with pytest.raises(SystemExit, match="pbmergeq:"):
        config(tmp_path, runtime_pins=declaration, test_python="/{sdk}/python")


@pytest.mark.parametrize("source", [None, "SDK_PIN = 'main'\n", "SDK_PIN = 'A' * 40\n",
    "SDK_PIN = factory()\n", f"SDK_PIN = {OLD!r}\nSDK_PIN = {NEW!r}\n",
    "SDK_PIN: str\n", "def f():\n    SDK_PIN = 'hidden'\n", "SDK_PIN = [1]\n", "= syntax\n"])
def test_missing_malformed_or_ambiguous_pins_are_named_before_launch(tmp_path, monkeypatch, source):
    cfg = policy(tmp_path)
    if source is not None:
        (tmp_path / "pins.py").write_text(source)
    launches = []
    refused_process(monkeypatch, launches)
    runner = mq.Runner(cfg, mq.Store(cfg.state_dir))
    result = runner.run_many("b1", tmp_path / "out", [("candidate", tmp_path, ["t.py"])])["candidate"]
    assert not launches
    assert result.runtime_refusal.startswith("pbmergeq: runtime selection refused:")
    assert "pins.py" in result.runtime_refusal
    assert result.inconclusive == ["t.py"] and not result.failed
    assert Path(result.log).is_file()


def test_pin_source_symlink_cannot_escape_checkout(tmp_path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    pin(tmp_path, OLD)
    (checkout / "pins.py").symlink_to(tmp_path / "pins.py")
    with pytest.raises(mq.RuntimeSelectionError, match="escapes checkout"):
        mq.checkout_runtime(policy(tmp_path), checkout)


def test_selection_only_reads_ast_and_retains_full_pin_and_source_digest(tmp_path):
    cfg = policy(tmp_path)
    marker = tmp_path / "executed"
    (tmp_path / "pins.py").write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).touch()\nSDK_PIN = {OLD!r}\n")
    (tmp_path / "tools").mkdir()
    (tmp_path / "tools/resolve_sdk_dev_pin.py").write_text("raise RuntimeError('untrusted')\n")
    runtime = mq.checkout_runtime(cfg, tmp_path)
    assert not marker.exists()
    assert runtime["python"] == "/venvs/sdk-aaaaaaaa/bin/python"
    assert runtime["pins"] == {"sdk": OLD}
    assert len(runtime["pin_sources"]["sdk"]["sha256"]) == 64


@pytest.mark.parametrize("reported", [False, True])
def test_runtime_refusal_retains_report_action_and_never_retries(tmp_path, monkeypatch, reported):
    cfg = policy(tmp_path)
    pin(tmp_path, NEW)
    launches = []
    report = [shard(["t.py"], ran=False)] if reported else None
    if report:
        report[0]["output"] = PIN_REFUSAL
    refused_process(monkeypatch, launches, report=report)
    result = mq.Runner(cfg, mq.Store(cfg.state_dir)).run_many(
        "b1", tmp_path / "out", [("base", tmp_path, ["t.py"])])["base"]
    assert len(launches) == 1
    assert result.runtime_refusal == PIN_REFUSAL
    assert result.runtime["pins"] == {"sdk": NEW}
    assert result.actions == (["k-t.py"] if reported else [])


def test_explicit_missing_worker_interpreter_is_blocked_not_generic_client_errors(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    launches = []
    message = ("pbtest: every recorded worker that could take these shards names the interpreter "
               "absent: /py. Install it on an eligible box.")
    refused_process(monkeypatch, launches, message)
    result = mq.Runner(cfg, mq.Store(cfg.state_dir)).run_many(
        "b1", tmp_path / "out", [("candidate", tmp_path, ["t.py"])])["candidate"]
    assert result.runtime_refusal == message and len(launches) == 1
    launches.clear()
    refused_process(monkeypatch, launches, "pbtest.py: error: unrecognized arguments: -k")
    result = mq.Runner(cfg, mq.Store(cfg.state_dir)).run_many(
        "b2", tmp_path / "out2", [("candidate", tmp_path, ["t.py"])])["candidate"]
    assert not result.runtime_refusal and result.inconclusive == ["t.py"]
    assert len(launches) == cfg.inconclusive_retries + 1


def test_candidate_base_rerun_and_bisect_each_select_their_own_checkout(tmp_path):
    queue, origin, github, _ = make_queue(tmp_path)
    cfg = policy(tmp_path, remote=str(origin.work))
    queue.cfg = cfg
    pin(origin.work, OLD)
    (origin.work / "fail-main").write_text("tests/test_a.py::old\n")
    git("add", "-A", cwd=origin.work)
    git("commit", "-q", "-m", "old reviewed runtime", cwd=origin.work)
    origin.pr(1, {"one.txt": "1\n"})
    guilty = origin.pr(2, {"pins.py": f"SDK_PIN = {NEW!r}\n",
                           "fail-new": "tests/test_b.py::new\n"})
    origin.pr(3, {"three.txt": "3\n"})

    class SelectedRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self.runtime_commands = {}

        def run_many(self, batch, outdir, jobs):
            runner = mq.Runner(cfg, queue.store)
            for name, checkout, files in jobs:
                command = runner.command(checkout, files, outdir / f"{name}.json")
                self.runtime_commands[name] = command[command.index("--python") + 1]
            return super().run_many(batch, outdir, jobs)

    queue.runner = SelectedRunner()
    batch = queue.run_batch(entries(1, 2, 3))
    assert batch["verdict"] == "red" and batch["shared"] == ["tests/test_a.py::old"]
    assert statuses(github) == [(guilty, "failure")]
    assert queue.runner.runtime_commands == {
        "candidate": "/venvs/sdk-bbbbbbbb/bin/python", "rerun": "/venvs/sdk-bbbbbbbb/bin/python",
        "base": "/venvs/sdk-aaaaaaaa/bin/python", "prefix2": "/venvs/sdk-bbbbbbbb/bin/python",
        "prefix1": "/venvs/sdk-aaaaaaaa/bin/python"}
    assert all(len(run["runtime"]["source"]) == 2 for run in batch["runs"].values())
    assert batch["runs"]["base"]["runtime"]["pins"]["sdk"] == OLD
    assert batch["runs"]["candidate"]["runtime"]["pins"]["sdk"] == NEW


@pytest.mark.parametrize("changed", ["tree", "python", "full_pin", "source_bytes", "config", "published"])
def test_baselines_ignore_legacy_and_incompatible_source_runtime_or_invocation(tmp_path, changed):
    cfg = policy(tmp_path)
    pin(tmp_path, OLD)
    runtime = mq.checkout_runtime(cfg, tmp_path)
    tree = "tree"
    identity = mq.baseline_key(cfg, tree, runtime)
    store = mq.Store(cfg.state_dir)
    store.remember_baseline(tree, ["t.py"], ["t.py::legacy"])
    assert store.baseline(identity) == {}
    store.remember_baseline(identity, ["t.py"], ["t.py::old"])
    assert store.baseline(identity) == {"t.py": ["t.py::old"]}
    other = json.loads(json.dumps(runtime))
    if changed == "tree":
        tree = "another-tree"
    elif changed == "python":
        other["python"] = "/another/python"
    elif changed == "full_pin":
        # Same abbreviated path, different full pin: cannot reuse.
        other["pins"]["sdk"] = OLD[:8] + "b" * 32
    elif changed == "source_bytes":
        other["pin_sources"]["sdk"]["sha256"] = "different"
    elif changed == "config":
        cfg = dataclasses.replace(cfg, pbtest_args=(*cfg.pbtest_args, '--pytest-args=["-k","one"]'))
    else:
        cfg = dataclasses.replace(cfg, pbtest=tmp_path / "other-published/pbtest.py")
    assert store.baseline(mq.baseline_key(cfg, tree, other)) == {}


def test_history_evidence_is_used_only_with_matching_runtime_source_and_config(tmp_path):
    cfg = policy(tmp_path)
    pin(tmp_path, OLD)
    runtime = mq.checkout_runtime(cfg, tmp_path)
    runtime["source"] = ["commit", "tree"]
    store = mq.Store(cfg.state_dir)
    report = tmp_path / "previous.json"
    report.write_text("[]")
    store.state["history_report"] = str(report)
    runner = mq.Runner(cfg, store)
    assert runner.history(runtime) == []
    store.state["history_runtime"] = mq.baseline_key(cfg, "tree", runtime)
    assert runner.history(runtime) == ["--history", str(report)]
    assert runner.history({**runtime, "source": ["commit", "different"]}) == []


def test_block_persists_across_restart_skips_only_affected_prs_and_resume_rechecks(tmp_path, monkeypatch):
    queue, origin, github, _ = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    origin.pr(2, {"two.txt": "2\n"})
    good_runner = queue.runner
    queue.runner = mq.Runner(queue.cfg, queue.store)
    monkeypatch.setattr(queue.runner, "discover", good_runner.discover)
    launches = []
    refused_process(monkeypatch, launches)
    batch = queue.run_batch([{**entries(1)[0], "attempts": 2}])
    assert batch["verdict"] == "runtime-blocked" and not github.calls
    assert queue.store.state["queue"][0]["attempts"] == 2
    assert "resume-runtime b00001" in queue.store.status_text()
    # New Store is the restart boundary. No duplicate attempt is automatic.
    queue.store = mq.Store(queue.cfg.state_dir)
    queue.resume()
    assert queue.tick() is None and len(launches) == 1
    queue.runner = good_runner
    queue.store.add(2, None, front=False)
    assert queue.tick()["verdict"] == "green"  # unrelated queue entries keep moving
    saved = queue.store.state["runtime_blocked"]["b00001"]
    assert saved["runs"]["candidate"]["runtime_refusal"] == PIN_REFUSAL
    assert len(saved["runs"]["candidate"]["runtime"]["source"]) == 2
    assert Path(saved["runs"]["candidate"]["log"]).is_file()
    assert mq.main(["--config", str(tmp_path / "config.json"), "resume-runtime", "b00001"]) == 0
    queue.store = mq.Store(queue.cfg.state_dir)
    queue.runner = mq.Runner(queue.cfg, queue.store)
    monkeypatch.setattr(queue.runner, "discover", good_runner.discover)
    again = queue.tick()
    assert again["verdict"] == "runtime-blocked" and len(launches) == 2
    assert queue.store.state["queue"][0]["attempts"] == 2
    assert not [state for _, state in statuses(github) if state == "failure"]


@pytest.mark.parametrize("phase", ["base", "rerun", "prefix1"])
def test_later_phase_runtime_refusal_never_becomes_shared_flake_or_pr_blame(tmp_path, phase):
    queue, origin, github, _ = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    origin.pr(2, {"fail-new": "tests/test_b.py::new\n"})

    class BlockedRunner(FakeRunner):
        def run_many(self, batch, outdir, jobs):
            results = super().run_many(batch, outdir, jobs)
            for name, _, files in jobs:
                if name == phase:
                    results[name] = mq.RunResult(set(), list(files), list(files), [],
                        actions=["failed-action"], log="guard.log", runtime_refusal=PIN_REFUSAL)
            return results

    queue.runner = BlockedRunner()
    batch = queue.run_batch(entries(1, 2))
    assert batch["verdict"] == "runtime-blocked" and not github.calls
    assert batch["runs"][phase]["actions"] == ["failed-action"]
    assert all(entry["attempts"] == 0 for entry in queue.store.state["queue"])
    assert not queue.store.state["flakes"]
    assert queue.tick() is None
    if phase in {"base", "rerun"}:
        assert {"candidate", "base", "rerun"}.issubset(batch["runs"])


def test_refusal_text_in_an_observed_pytest_result_is_not_a_preflight_block():
    report = shard(["t.py"], rows=[("t.py::ok", "call", "passed", None, None)])
    report["output"] = PIN_REFUSAL + "\n" + report["output"]
    result = mq.reduce_report([report], __import__("pbtest_outcomes"))
    assert not result.runtime_refusal and not result.inconclusive

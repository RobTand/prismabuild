"""Consumer gate contract, with fixture rows and no live store (#1896)."""
from __future__ import annotations

import dataclasses
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_pbmergeq import config, mq, pbtest_outcomes as outcomes, shard

FILE = "tests/test_a.py"
RUNTIME = {"python": "/py", "pins": {}, "pin_sources": {},
           "source": ["a" * 40, "b" * 40], "checkout": "/fixture"}
FIELDS = {"schema", "verdict", "failed", "reason", "files", "left_out", "runtime", "evidence"}


@pytest.fixture
def gate_fixture(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    launches, waits, reports = [], [], []
    monkeypatch.setattr(mq, "checkout_runtime", lambda cfg, path: {
        **RUNTIME, "pbtest": str(cfg.pbtest)})
    monkeypatch.setattr(mq.Runner, "inventory", lambda self, path: (
        [FILE], [{"file": "tests/test_data.py", "reason": "fleet data"}]))

    class Child:
        pid = 4242

        def __init__(self, command, **kwargs):
            self.returncode = None
            launches.append(command)
            report = Path(command[command.index("--json") + 1])
            rows = reports[len(launches) - 1]
            report.write_text(json.dumps(rows))
            self.code = 1 if any(row["returncode"] for row in rows) else 0

        def wait(self):
            waits.append(self.pid)
            self.returncode = self.code
            return self.code

        def poll(self):
            pytest.fail("gate must block on child exit")

    monkeypatch.setattr(mq.subprocess, "Popen", Child)
    monkeypatch.setattr(mq.time, "sleep", lambda seconds: pytest.fail("gate must not sleep"))
    output = tmp_path / "verdict.json"

    def invoke(extra=()):
        rc = mq.main(["gate", "--config", str(tmp_path / "config.json"),
                      "--checkout", str(tmp_path), "--json", str(output), *extra])
        result = json.loads(output.read_text())
        assert set(result) == FIELDS
        assert result["schema"] == "prismabuild.mergeq_gate.v1"
        return rc, result

    return SimpleNamespace(cfg=cfg, launches=launches, waits=waits, reports=reports,
                           output=output, invoke=invoke)


@pytest.mark.parametrize("failed", [False, True])
def test_gate_verdict_and_history_are_owned_by_config_state_dir(gate_fixture, failed):
    f = gate_fixture
    rows = [(FILE + "::named_failure", "call", "failed", None, None)] if failed else ()
    f.reports.append([shard([FILE], rows=rows)])
    rc, result = f.invoke(["--tmpdir", "/scratch", "--env", "TL_GATE_TOKEN=a=b"])
    assert rc == int(failed)
    assert result["verdict"] == ("fail" if failed else "pass")
    assert result["failed"] == ([FILE + "::named_failure"] if failed else [])
    assert result["reason"] == ""
    assert result["files"] == [FILE]
    assert result["left_out"] == [{"file": "tests/test_data.py", "reason": "fleet data"}]
    assert result["runtime"] == {**RUNTIME, "pbtest": str(f.cfg.pbtest)}
    evidence = result["evidence"]
    assert set(evidence) == {"directory", "report", "log", "actions", "receipts",
                             "problems", "inconclusive", "wall_s"}
    assert evidence["actions"] == ["k-" + FILE] and evidence["receipts"] == ["/r/x"]
    assert Path(evidence["report"]).is_relative_to(f.cfg.state_dir)
    assert Path(evidence["log"]).is_file()
    history = json.loads((f.cfg.state_dir / "state.json").read_text())
    assert history["history_report"] == evidence["report"]
    assert history["history_source"] == RUNTIME["source"]
    effective = dataclasses.replace(f.cfg, pbtest_args=(*f.cfg.pbtest_args, "--tmpdir", "/scratch"),
                                    env=("TL_GATE_TOKEN=a=b",))
    assert history["history_runtime"] == mq.baseline_key(effective, RUNTIME["source"][1], result["runtime"])
    assert f.waits == [4242]
    command = f.launches[0]
    assert command[2:4] == ["--env", "TL_GATE_TOKEN=a=b"]
    assert command[command.index("--tmpdir") + 1] == "/scratch"
    f.reports.append([shard([FILE])])
    assert f.invoke(["--tmpdir", "/scratch", "--env", "TL_GATE_TOKEN=a=b"])[0] == 0
    assert f.launches[1][f.launches[1].index("--history") + 1] == evidence["report"]


@pytest.mark.parametrize("flag", ["collect_only", "deselected"])
def test_gate_refuses_partial_population_without_history_or_retry(gate_fixture, flag):
    row = shard([FILE])
    record = outcomes.parse(row["output"])
    record[flag] = True if flag == "collect_only" else [FILE + "::left_out"]
    row["output"] = "1 passed in 1.0s\n" + outcomes.PREFIX + json.dumps(record)
    gate_fixture.reports.append([row])
    rc, result = gate_fixture.invoke()
    assert rc == 125 and result["verdict"] == "no_verdict"
    assert result["reason"] == "filtered or collect-only execution is not the queue's suite"
    assert len(gate_fixture.launches) == 1
    assert not (gate_fixture.cfg.state_dir / "state.json").exists()


def test_gate_runtime_refusal_reason_is_verbatim(gate_fixture):
    message = "runtime refused: exact diagnostic\nkeep this second line"
    row = shard([FILE], ran=False)
    row["output"] = outcomes.preflight_record(1, {"code": "dependency_pin", "message": message})
    gate_fixture.reports.append([row])
    rc, result = gate_fixture.invoke()
    assert rc == 125 and result["verdict"] == "no_verdict"
    assert result["reason"] == message
    assert len(gate_fixture.launches) == 1
    assert not (gate_fixture.cfg.state_dir / "state.json").exists()


def test_gate_retries_and_merges_using_existing_runner(gate_fixture):
    f = gate_fixture
    f.reports.extend([[shard([FILE], ran=False)], [shard([FILE])]])
    rc, result = f.invoke(["--env", "TL_GATE_TOKEN=retry"])
    assert rc == 0 and result["verdict"] == "pass"
    assert len(f.launches) == 2 and len(f.waits) == 2
    assert result["evidence"]["actions"] == ["k-" + FILE, "k-" + FILE]
    assert not result["evidence"]["inconclusive"]
    assert all(json.loads(command[4]) == ["TL_GATE_TOKEN=retry"] for command in f.launches)


def test_gate_inconclusive_reason_preserves_reducer_messages(gate_fixture):
    row = shard([FILE], ran=False)
    gate_fixture.reports.extend([[row]] * 3)
    rc, result = gate_fixture.invoke()
    expected = mq.reduce_report([row], outcomes, requested_files=[FILE], client_returncode=1)
    assert rc == 125 and result["verdict"] == "no_verdict"
    assert result["reason"] == "\n".join(dict.fromkeys(p["message"] for p in expected.problems))
    assert result["evidence"]["inconclusive"] == [FILE]
    assert len(gate_fixture.launches) == 3


@pytest.mark.parametrize("extra, reason", [
    (["--env", "TMPDIR=x"], "TMPDIR"),
    (["--env", "PRISMABUILD_TEST_TIMEOUT_S=1"], "PRISMABUILD_TEST_TIMEOUT_S"),
    (["--env", "bad"], "KEY=VALUE"),
    (["--env", "TOKEN=a", "--env", "TOKEN=b"], "repeats key TOKEN"),
    (["--tmpdir", "relative"], "absolute"),
])
def test_gate_input_refusals_are_json_without_submission(gate_fixture, extra, reason):
    rc, result = gate_fixture.invoke(extra)
    assert rc == 125 and result["verdict"] == "no_verdict" and reason in result["reason"]
    assert not gate_fixture.launches


def test_gate_inventory_reuses_discovery_and_explains_exclusions(tmp_path):
    cfg = config(tmp_path, exclude=["tests/test_skip.py"], skip_fleet_data_files=True)
    runner = mq.Runner(cfg, mq.Store(cfg.state_dir))
    runner._pbtest = SimpleNamespace(
        discover=lambda checkout, paths: [FILE, "tests/test_skip.py", "tests/test_data.py"],
        fleet_data_files=lambda checkout, files: ["tests/test_data.py"])
    assert runner.inventory(tmp_path) == ([FILE], [
        {"file": "tests/test_skip.py", "reason": "config.exclude"},
        {"file": "tests/test_data.py", "reason": "fleet data"}])
    assert runner.discover(tmp_path) == ([FILE], ["tests/test_data.py"])


def test_gate_env_forwards_each_pair_to_pbtest(tmp_path):
    values = ("TL_GATE_TOKEN=a=b c", "EMPTY=")
    command = mq.test_command(dataclasses.replace(config(tmp_path), env=values),
                              tmp_path, [], tmp_path / "out.json", [], python="/py")
    assert command[2:6] == ["--env", "TL_GATE_TOKEN=a=b c", "--env", "EMPTY="]
    assert command[6:8] == ["--checkout", str(tmp_path)]


def test_unchanged_queue_config_keeps_its_existing_baseline_identity(tmp_path):
    cfg = config(tmp_path)
    raw = dataclasses.asdict(cfg)
    raw.pop("env")
    runtime = {**RUNTIME, "pbtest": str(cfg.pbtest)}
    assert mq.baseline_key(cfg, RUNTIME["source"][1], runtime) == mq.baseline_identity(
        RUNTIME["source"][1], runtime, raw)


def test_gate_empty_inventory_has_no_verdict(gate_fixture, monkeypatch):
    monkeypatch.setattr(mq.Runner, "inventory", lambda self, path: ([], []))
    rc, result = gate_fixture.invoke()
    assert rc == 125 and result["verdict"] == "no_verdict"
    assert result["reason"] == "queue inventory has no test files"
    assert not gate_fixture.launches


def test_gate_bad_rows_remain_inconclusive(gate_fixture):
    row = {**shard([FILE]), "output": 123}
    gate_fixture.reports.extend([[row]] * 3)
    rc, result = gate_fixture.invoke()
    assert rc == 125 and result["verdict"] == "no_verdict"
    assert "invalid shard observation" in result["reason"]


def test_gate_does_not_overwrite_previous_history_on_no_verdict(gate_fixture):
    f = gate_fixture
    f.reports.append([shard([FILE])])
    assert f.invoke()[0] == 0
    state = f.cfg.state_dir / "state.json"
    previous = state.read_bytes()
    f.reports.extend([[shard([FILE], ran=False)]] * 3)
    assert f.invoke()[0] == 125
    assert state.read_bytes() == previous


def test_gate_honors_existing_state_lock(gate_fixture):
    with mq.single_instance(gate_fixture.cfg.state_dir):
        rc, result = gate_fixture.invoke()
    assert rc == 125 and "another instance holds" in result["reason"]
    assert not gate_fixture.launches

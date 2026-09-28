"""pbtest balances shards by measured per-file duration (#1246).

Round-robin by file balances only when files cost roughly the same, and they
do not: one 1513 s file shared with 18 others lost all 19 files' results when
its shard hit the 3600 s ceiling (action ``1afea851``).  The measurements to
pack by already flow through the shard: the recorder sees every report, and
each report carries its own duration.  So the outcome record carries each
file's summed wall time; a later run loads those numbers out of prior run
reports, isolates a file that alone fills a large share of the ceiling, packs
the rest longest-processing-time first on a high quantile of each file's
history, and prints predicted against actual seconds per shard.  With no
history anywhere, sharding stays round-robin.  A test that starts late in its
shard is bounded by the time left in the shard, not by the ceiling alone, so
the stall is named before the pool kills the shard.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

from pbtest_shard_output import ShardProcess  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pbtest_balance_subject", ROOT / "tools" / "fleet" / "pbtest.py")
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)  # type: ignore[union-attr]
outcomes = pbtest.pbtest_outcomes
REAL_POPEN = subprocess.Popen

SLOW_TESTS = '''\
import time


def test_slow():
    time.sleep(0.5)


def test_quick():
    pass
'''

FAST_TESTS = '''\
def test_one():
    pass


def test_two():
    pass
'''


def _checkout(tmp_path: Path) -> Path:
    checkout = tmp_path / "project"
    (checkout / "tests").mkdir(parents=True)
    (checkout / "pytest.ini").write_text("[pytest]\n")
    (checkout / "tests" / "test_slow_file.py").write_text(SLOW_TESTS)
    (checkout / "tests" / "test_fast_file.py").write_text(FAST_TESTS)
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    return checkout


def _dispatch(checkout: Path, monkeypatch, extra: list[str]):
    def admitted_payload(command, **kwargs):
        if str(pbtest.PBRUN) not in [str(part) for part in command]:
            return REAL_POPEN(command, **kwargs)
        return REAL_POPEN(command[command.index("--") + 1:], cwd=checkout, **kwargs)

    monkeypatch.setattr(pbtest.subprocess, "Popen", admitted_payload)
    report = checkout.parent / "shards.json"
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(checkout), "--python", sys.executable,
        "--threads-per-shard", "1", "--json", str(report), *extra, "tests",
    ])
    return pbtest.main(), json.loads(report.read_text())


def _prior_run(tmp_path: Path, durations: dict[str, float]) -> Path:
    """A prior run report whose outcome record carries ``durations``."""

    record = {
        "schema": outcomes.SCHEMA,
        "rootdir_relative": ".",
        "collect_only": False,
        "collected": [f"tests/{name}" for name in durations],
        "deselected": [],
        "reports": [],
        "uncounted": [],
        "file_durations": dict(durations),
    }
    path = tmp_path / "prior.json"
    path.write_text(json.dumps([{
        "shard": 0,
        "files": [f"tests/{name}" for name in durations],
        "returncode": 0,
        "action_key": None,
        "receipt_path": None,
        "summary": "2 passed in 1.50s",
        "ran": True,
        "skipped": [],
        "attempts": 1,
        "output": "2 passed in 1.50s\n" + outcomes.PREFIX + json.dumps(record) + "\n",
    }]))
    return path


def test_the_outcome_record_carries_per_file_wall_time(tmp_path, monkeypatch):
    """Each file's summed phase durations ride in its shard's record."""

    code, results = _dispatch(_checkout(tmp_path), monkeypatch, ["--shards", "1"])
    assert code == 0
    record = outcomes.parse(results[0]["output"])
    assert record is not None
    durations = record.get("file_durations")
    assert durations is not None, "the record names no per-file durations"
    assert set(durations) == {
        "tests/test_slow_file.py", "tests/test_fast_file.py"}
    assert durations["tests/test_slow_file.py"] >= 0.4
    assert durations["tests/test_fast_file.py"] < durations["tests/test_slow_file.py"]


def test_a_slow_file_is_isolated_from_its_shard_mates():
    """A file predicted past half the ceiling gets its own shard."""

    files = ["tests/test_slow.py"] + [f"tests/test_small_{n}.py" for n in range(18)]
    durations = {"tests/test_slow.py": 1513.0}
    durations.update({name: 10.0 for name in files[1:]})
    buckets = pbtest.shard(files, 4, durations=durations, ceiling=3600.0)
    alone = [bucket for bucket in buckets if "tests/test_slow.py" in bucket]
    assert len(alone) == 1
    assert alone[0] == ["tests/test_slow.py"]


def test_shards_pack_by_longest_processing_time_first():
    """The rest fill the least-loaded bucket, largest prediction first."""

    files = ["tests/test_a.py", "tests/test_b.py", "tests/test_c.py", "tests/test_d.py"]
    durations = {"tests/test_a.py": 100.0, "tests/test_b.py": 90.0,
                 "tests/test_c.py": 80.0, "tests/test_d.py": 10.0}
    assert pbtest.shard(files, 2, durations=durations, ceiling=None) == [
        ["tests/test_a.py", "tests/test_d.py"],
        ["tests/test_b.py", "tests/test_c.py"]]


def test_files_without_history_take_the_measured_default():
    """No magic constant: the default is the median of the measured files."""

    files = ["tests/test_a.py", "tests/test_b.py", "tests/test_new.py"]
    history = {"tests/test_a.py": [100.0], "tests/test_b.py": [10.0]}
    predicted = pbtest.predict_durations(history)
    assert predicted == {"tests/test_a.py": 100.0, "tests/test_b.py": 10.0}
    buckets = pbtest.shard(files, 2, durations=predicted, ceiling=None)
    assert buckets == [["tests/test_a.py"], ["tests/test_new.py", "tests/test_b.py"]]


def test_no_history_keeps_round_robin():
    """Without measurements sharding is exactly today's rotation."""

    files = [f"tests/test_{n}.py" for n in range(6)]
    assert pbtest.shard(files, 4) == [
        ["tests/test_0.py", "tests/test_4.py"],
        ["tests/test_1.py", "tests/test_5.py"],
        ["tests/test_2.py"],
        ["tests/test_3.py"]]
    assert pbtest.shard(files, 4, durations={}, ceiling=None) == pbtest.shard(files, 4)


def test_the_summary_reports_predicted_against_actual(tmp_path, monkeypatch, capsys):
    """Every shard prints its model's prediction beside pytest's own seconds."""

    checkout = _checkout(tmp_path)
    prior = _prior_run(tmp_path, {"test_slow_file.py": 0.5, "test_fast_file.py": 0.01})
    code, results = _dispatch(checkout, monkeypatch,
                              ["--shards", "2", "--history", str(prior)])
    printed = capsys.readouterr().out
    assert code == 0, printed
    assert "predicted" in printed and "pytest" in printed
    for result in results:
        assert result["predicted_s"] > 0
        assert result["pytest_s"] is not None and result["pytest_s"] >= 0


def test_a_late_test_is_bound_by_the_time_left_in_the_shard(tmp_path, monkeypatch):
    """The alarm arms at min(bound, seal - elapsed), never past the lease."""

    from prismabuild import pytest_test_bound
    bound = pytest_test_bound.remaining_bound(
        bound_s=3570.0, budget_s=3600.0, elapsed_s=3500.0)
    assert bound == pytest.approx(3600.0 - 3500.0 - 30.0)
    floored = pytest_test_bound.remaining_bound(
        bound_s=3570.0, budget_s=3600.0, elapsed_s=3599.0)
    assert floored == pytest.approx(1.0)


def test_the_shard_exports_its_remaining_budget(tmp_path, monkeypatch):
    """The worker learns the seal from the environment, like the bound."""

    from prismabuild import pytest_test_bound
    checkout = tmp_path / "checkout"
    (checkout / "tests").mkdir(parents=True)
    (checkout / "tests" / "test_one.py").write_text("def test_one():\n    pass\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    class _Done(ShardProcess):
        returncode = 0

        def __init__(self, command):
            self.output = "1 passed in 0.01s\n"

        def communicate(self):
            return self.output, None

    calls: list[list[str]] = []

    def _popen(command, **kwargs):
        calls.append(list(command))
        return _Done(command)

    monkeypatch.setattr(pbtest.subprocess, "Popen", _popen)
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
        "--shards", "1", "--timeout-s", "600", "tests"])
    assert pbtest.main() == 0
    exported = [part for command in calls for part in command
                if part.startswith(pytest_test_bound.SHARD_BUDGET_ENV + "=")]
    assert exported == [f"{pytest_test_bound.SHARD_BUDGET_ENV}=600"]

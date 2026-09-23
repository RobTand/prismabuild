"""pbtest reconciles every shard's outcomes with its collection by node ID (#941).

A 40-shard run of the PrismaQuant suite reported 11,867 outcomes against a
collection of 11,861, and nothing said which six.  Each of the six was a
module that skipped at import: pytest's summary counts it as ``1 skipped``,
and ``--collect-only`` counts no item for it.  No test ran twice.  Nothing
could tell that from a double-run or a missing test, because the report
compared nothing.

pbtest now matches each shard's collected node IDs against the outcomes its
recorder saw, and every shard's collection against the other shards'.  A
collected test with no outcome, a test two shards collected, a record that
disagrees with its summary, or a shard with no record at all is a shard that
is not green, whatever its exit code.  How a summary exceeds its tests --
outcomes at collection, and tests counted in more than one phase -- is
printed by name.

The first tests run the real shard program: only the pbrun submission is
replaced, and its payload runs here, inside this already-admitted action.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pbtest_reconcile_subject", ROOT / "tools" / "fleet" / "pbtest.py")
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)  # type: ignore[union-attr]
outcomes = pbtest.pbtest_outcomes
REAL_POPEN = subprocess.Popen

TESTS = '''\
import pytest


@pytest.fixture
def skips_on_teardown():
    yield
    pytest.skip("skipped in teardown")


def test_counted_twice(skips_on_teardown):
    pass


@pytest.mark.parametrize("n", [1, 2, 3])
def test_param(n):
    pass


def test_skipped():
    pytest.skip("skipped in call")
'''

MODULE_SKIP = '''\
import pytest

pytest.importorskip("pbtest_no_such_module_941")


def test_never_collected():
    pass
'''


def _checkout(tmp_path: Path) -> Path:
    checkout = tmp_path / "project"
    (checkout / "tests").mkdir(parents=True)
    (checkout / "pytest.ini").write_text("[pytest]\n")
    (checkout / "tests" / "test_a_counts.py").write_text(TESTS)
    (checkout / "tests" / "test_b_module_skip.py").write_text(MODULE_SKIP)
    (checkout / "tests" / "test_c_passes.py").write_text("def test_passes():\n    pass\n")
    (checkout / "tests" / "test_d_passes.py").write_text("def test_passes():\n    pass\n")
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


@pytest.mark.parametrize("extra", [
    pytest.param(["--shards", "2"], id="two-shards"),
    pytest.param(["--shards", "1", "--workers-per-shard", "2"], id="xdist"),
])
def test_a_summary_that_exceeds_its_tests_reconciles_by_name(
        tmp_path, monkeypatch, capsys, extra):
    """7 tests, 9 outcomes: 1 skip at collection, 1 test counted twice."""

    if "--workers-per-shard" in extra:
        pytest.importorskip("xdist")
    code, results = _dispatch(_checkout(tmp_path), monkeypatch, extra)
    printed = capsys.readouterr().out
    assert code == 0, printed

    reconciled = [r["reconciliation"] for r in results]
    assert all(not rec["problems"] for rec in reconciled), reconciled
    assert sum(rec["collected"] for rec in reconciled) == 7
    assert sum(rec["ran"] for rec in reconciled) == 7
    assert sum(rec["outcomes"] for rec in reconciled) == 9
    at_collection = [item for rec in reconciled for item in rec["at_collection"]]
    assert [(item["nodeid"], item["category"]) for item in at_collection] == [
        ("tests/test_b_module_skip.py", "skipped")]
    extra_phases = {nodeid: kinds for rec in reconciled
                    for nodeid, kinds in rec["extra_phases"].items()}
    assert extra_phases == {
        "tests/test_a_counts.py::test_counted_twice": ["call:passed", "teardown:skipped"]}
    assert ("reconciliation: 7 collected, 7 ran; the summaries count 9 outcome(s) "
            "= 7 test(s) + 1 at collection + 1 extra phase(s)") in printed
    assert "at collection: tests/test_b_module_skip.py skipped" in printed
    assert "counted 2 times: tests/test_a_counts.py::test_counted_twice" in printed
    assert "UNRECONCILED" not in printed


def test_a_collect_only_run_reconciles_its_item_count(tmp_path, monkeypatch, capsys):
    code, results = _dispatch(_checkout(tmp_path), monkeypatch,
                              ["--shards", "1", "--pytest-args", '["--co"]'])
    printed = capsys.readouterr().out
    assert code == 0, printed
    (rec,) = [r["reconciliation"] for r in results]
    assert rec["collect_only"] is True
    assert rec["collected"] == 7 and rec["problems"] == []


def test_a_test_stopped_by_maxfail_is_named_as_never_run(tmp_path, monkeypatch, capsys):
    checkout = _checkout(tmp_path)
    (checkout / "tests" / "test_a_counts.py").write_text(
        TESTS + "\n\ndef test_fails():\n    assert False\n\n\ndef test_after():\n    pass\n")
    code, results = _dispatch(checkout, monkeypatch,
                              ["--shards", "1", "--pytest-args", '["-x"]'])
    printed = capsys.readouterr().out
    assert code == 1
    (rec,) = [r["reconciliation"] for r in results]
    assert "tests/test_a_counts.py::test_after" in rec["never_ran"]
    assert "collected test(s) never ran" in printed
    assert "never ran: tests/test_a_counts.py::test_after" in printed


# --- stand-in shards: the failures a real run cannot be made to produce ----


def _record(collected, reports, *, collect_only=False) -> str:
    return outcomes.PREFIX + json.dumps({
        "schema": outcomes.SCHEMA, "collect_only": collect_only,
        "collected": collected, "reports": reports, "uncounted": []})


def _stand_in(tmp_path: Path, monkeypatch, outputs: list[str], *, returncode=0):
    checkout = tmp_path / "project"
    (checkout / "tests").mkdir(parents=True)
    for index in range(len(outputs)):
        (checkout / "tests" / f"test_{index}.py").write_text("def test_x():\n    pass\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    queue = list(outputs)

    class Finished:
        def __init__(self, output):
            self.output = output
            self.returncode = returncode

        def communicate(self):
            return self.output, None

    def popen(command, **kwargs):
        if str(pbtest.PBRUN) in [str(part) for part in command]:
            return Finished(queue.pop(0))
        return REAL_POPEN(command, **kwargs)

    monkeypatch.setattr(pbtest.subprocess, "Popen", popen)
    report = tmp_path / "shards.json"
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
        "--shards", str(len(outputs)), "--json", str(report), "tests"])
    return pbtest.main(), json.loads(report.read_text())


def test_a_collected_test_with_no_outcome_fails_the_shard(tmp_path, monkeypatch, capsys):
    """Exit 0 and a clean summary do not cover a test that never reported."""

    lost = "tests/test_0.py::test_lost"
    output = _record(["tests/test_0.py::test_x", lost],
                     [["tests/test_0.py::test_x", "call", "passed", None, None]])
    code, results = _stand_in(tmp_path, monkeypatch, [f"{output}\n1 passed in 0.01s\n"])
    printed = capsys.readouterr().out
    assert code == 1
    assert results[0]["reconciliation"]["never_ran"] == [lost]
    assert f"never ran: {lost}" in printed
    assert "0/1 shards green" in printed


def test_a_test_two_shards_collected_fails_both(tmp_path, monkeypatch, capsys):
    both = "tests/shared.py::test_twice"
    outputs = [
        _record([both], [[both, "call", "passed", None, None]]) + "\n1 passed in 0.01s\n",
        _record([both], [[both, "call", "passed", None, None]]) + "\n1 passed in 0.01s\n",
    ]
    code, results = _stand_in(tmp_path, monkeypatch, outputs)
    printed = capsys.readouterr().out
    assert code == 1
    for result in results:
        assert result["reconciliation"]["in_other_shards"] == [both]
    assert printed.count(f"also in another shard: {both}") == 2


def test_a_record_that_disagrees_with_its_summary_fails_the_shard(
        tmp_path, monkeypatch, capsys):
    one = "tests/test_0.py::test_x"
    output = _record([one], [[one, "call", "passed", None, None]])
    code, results = _stand_in(tmp_path, monkeypatch, [f"{output}\n2 passed in 0.01s\n"])
    printed = capsys.readouterr().out
    assert code == 1
    assert results[0]["reconciliation"]["problems"] == [
        "the summary counts {'passed': 2} and the record holds {'passed': 1}"]
    assert "UNRECONCILED shard   0" in printed


def test_a_shard_with_a_summary_and_no_record_is_not_green(tmp_path, monkeypatch, capsys):
    """A recorder that printed nothing is not a shard with nothing to report."""

    code, results = _stand_in(tmp_path, monkeypatch, ["1 passed in 0.01s\n"])
    printed = capsys.readouterr().out
    assert code == 1
    assert results[0]["reconciliation"]["problems"][0].startswith("no outcome record")


@pytest.mark.parametrize("summary, counts, collected", [
    ("1 passed in 0.01s", {"passed": 1}, None),
    ("1 failed, 531 passed, 1 skipped, 2 warnings in 17.82s",
     {"failed": 1, "passed": 531, "skipped": 1}, None),
    ("2 failed, 1 error in 12.34s", {"failed": 2, "error": 1}, None),
    ("3 errors, 4 deselected in 0.10s", {"error": 3}, None),
    ("16 passed, 14 warnings, 6 subtests passed in 2.91s",
     {"passed": 16, "subtests passed": 6}, None),
    ("= 1 passed in 0.02s =", {"passed": 1}, None),
    ("no tests ran in 0.01s", {}, None),
    ("11861 tests collected in 41.46s", None, 11861),
    ("11/20 tests collected (9 deselected) in 0.30s", None, 11),
    ("no tests collected (2 deselected) in 0.01s", None, 0),
])
def test_summary_outcomes_read_the_categories_the_record_uses(summary, counts, collected):
    assert pbtest.summary_outcomes(summary) == (counts, collected)

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

from pbtest_shard_output import ShardProcess  # noqa: E402


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


@pytest.mark.parametrize("workers", [1, 2])
@pytest.mark.parametrize("selector", ["-k", "-m"])
def test_a_fully_deselected_file_is_accounted_for(
        tmp_path, monkeypatch, capsys, workers, selector):
    if workers > 1:
        pytest.importorskip("xdist")
    checkout = tmp_path / "project"
    (checkout / "tests").mkdir(parents=True)
    (checkout / "tests" / "test_keep.py").write_text(
        "import pytest\n\n@pytest.mark.keep\ndef test_keep():\n    pass\n")
    (checkout / "tests" / "test_drop.py").write_text(
        "def test_drop():\n    pass\n")
    (checkout / "pytest.ini").write_text("[pytest]\n")
    # Hermetic rootdir: without an inifile of its own, pytest walks the
    # ancestor chain and any config in $HOME hijacks the rootdir, so nodeids
    # stop matching the assigned files (#1257).
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    code, results = _dispatch(checkout, monkeypatch,
                              ["--shards", "1", "--workers-per-shard", str(workers),
                               "--pytest-args", json.dumps([selector, "keep"])])
    assert code == 0, capsys.readouterr().out
    assert results[0]["reconciliation"]["missing_files"] == []


def test_nested_pytest_root_still_matches_assigned_files(tmp_path, monkeypatch, capsys):
    checkout = tmp_path / "project"
    (checkout / "tests").mkdir(parents=True)
    (checkout / "tests" / "pytest.ini").write_text("[pytest]\n")
    (checkout / "tests" / "test_nested.py").write_text(
        "def test_nested():\n    pass\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    code, results = _dispatch(checkout, monkeypatch, ["--shards", "1"])
    assert code == 0, capsys.readouterr().out
    assert results[0]["reconciliation"]["missing_files"] == []


@pytest.mark.parametrize("workers", [1, 2])
def test_selector_does_not_excuse_a_file_with_no_collection(
        tmp_path, monkeypatch, capsys, workers):
    if workers > 1:
        pytest.importorskip("xdist")
    checkout = tmp_path / "project"
    (checkout / "tests").mkdir(parents=True)
    (checkout / "tests" / "test_keep.py").write_text(
        "def test_keep():\n    pass\n")
    (checkout / "tests" / "test_empty.py").write_text("# no tests\n")
    (checkout / "pytest.ini").write_text("[pytest]\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    code, results = _dispatch(checkout, monkeypatch,
                              ["--shards", "1", "--workers-per-shard", str(workers),
                               "--pytest-args", '["-k","keep"]'])
    assert code == 1, capsys.readouterr().out
    assert results[0]["reconciliation"]["missing_files"] == ["tests/test_empty.py"]


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


def _stand_in(tmp_path: Path, monkeypatch, outputs: list[str], *, returncode=0,
              extra_file=False):
    checkout = tmp_path / "project"
    (checkout / "tests").mkdir(parents=True)
    for index in range(len(outputs)):
        (checkout / "tests" / f"test_{index}.py").write_text("def test_x():\n    pass\n")
    if extra_file:
        (checkout / "tests" / "test_missing.py").write_text(
            "def test_missing():\n    pass\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    queue = list(outputs)

    class Finished(ShardProcess):
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


def test_an_assigned_file_with_no_collection_or_outcome_fails(tmp_path, monkeypatch, capsys):
    present = "tests/test_0.py::test_x"
    output = _record([present], [[present, "call", "passed", None, None]])
    code, results = _stand_in(tmp_path, monkeypatch, [output + "\n1 passed in 0.01s\n"],
                              extra_file=True)
    assert code == 1
    assert results[0]["reconciliation"]["missing_files"] == ["tests/test_missing.py"]
    assert "tests/test_missing.py" in capsys.readouterr().out


def test_a_collection_skip_covers_its_assigned_file():
    result = {"shard": 0, "files": ["tests/test_skipped.py"], "ran": True,
              "summary": "1 skipped in 0.01s",
              "output": _record([], [["tests/test_skipped.py", "collect", "skipped",
                                       "optional dependency absent", None]])}
    pbtest.reconcile_shards([result])
    assert result["reconciliation"]["problems"] == []
    assert result["reconciliation"]["missing_files"] == []


@pytest.mark.parametrize("selection,error,valid", [
    ({"files": ["tests/test_ignored.py"], "ignored": ["tests/test_ignored.py"]}, None, True),
    (None, "missing worker evidence", False),
    ({"files": ["tests/test_ignored.py"], "ignored": ["tests/test_ignored.py"]},
     "inconsistent workers", False),
    ({"files": ["tests/test_other.py"], "ignored": ["tests/test_ignored.py"]}, None, False),
    ({"files": ["tests/test_ignored.py"], "ignored": ["tests/test_other.py"]}, None, False),
    ({"files": ["tests/test_ignored.py"],
      "ignored": ["tests/test_ignored.py", "tests/test_ignored.py"]}, None, False),
])
def test_only_complete_consistent_ignore_evidence_covers_a_file(selection, error, valid):
    record = outcomes.parse(_record([], []))
    record.update(file_selection=selection, file_selection_error=error)
    result = {"shard": 0, "files": ["tests/test_ignored.py"], "ran": True,
              "summary": "no tests ran in 0.01s",
              "output": outcomes.PREFIX + json.dumps(record)}
    pbtest.reconcile_shards([result])
    rec = result["reconciliation"]
    assert (rec["problems"] == []) is valid
    assert rec["missing_files"] == ([] if valid else ["tests/test_ignored.py"])


def test_a_collected_file_cannot_also_claim_to_be_ignored():
    name = "tests/test_one.py"
    nodeid = name + "::test_one"
    record = outcomes.parse(_record([nodeid], [[nodeid, "call", "passed", None, None]]))
    record.update(file_selection={"files": [name], "ignored": [name]},
                  file_selection_error=None)
    result = {"shard": 0, "files": [name], "ran": True,
              "summary": "1 passed in 0.01s",
              "output": outcomes.PREFIX + json.dumps(record)}
    pbtest.reconcile_shards([result])
    assert "invalid or inconsistent file selection evidence" in result["reconciliation"]["problems"]


def test_failed_shard_shows_full_failure_section(tmp_path, monkeypatch, capsys):
    nodeid = "tests/test_0.py::test_x"
    record = _record([nodeid], [[nodeid, "call", "failed", None, None]])
    failure = "AssertionError: original failure context"
    output = ("=== FAILURES ===\n" + failure + "\n" +
              "\n".join(f"trace detail {n}" for n in range(40)) + "\n" +
              record + "\n1 failed in 0.01s\n")
    _stand_in(tmp_path, monkeypatch, [output], returncode=1)
    assert failure in capsys.readouterr().out


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

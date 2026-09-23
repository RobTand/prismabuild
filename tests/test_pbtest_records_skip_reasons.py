"""A pbtest receipt names every skipped test and why it skipped (#942).

A summary that says ``2 skipped`` certifies nothing about those two: a skip
is only evidence once its reason is on record.  pytest's ``-rs`` would not
fix that, because it folds skips by location and prints no node IDs.  Every
shard therefore runs pytest under ``pbtest_outcomes``' recorder, and pbtest
reads the record back into the receipt.

These tests run the real shard program.  Only the pbrun submission is
replaced: its payload runs here, inside this already-admitted test action,
against a private fixture checkout, which is how the dependency-pin tests
exercise the sealed program too.
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
    "pbtest_skip_reasons_subject", ROOT / "tools" / "fleet" / "pbtest.py")
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)  # type: ignore[union-attr]
outcomes = pbtest.pbtest_outcomes
REAL_POPEN = subprocess.Popen

MISSING = "pbtest_no_such_module_942"

SKIPS = '''\
import pytest


@pytest.mark.skip(reason="marker reason")
def test_marked():
    pass


@pytest.mark.skipif(True, reason="condition reason")
def test_conditional():
    pass


def test_imperative():
    pytest.skip("imperative reason")


@pytest.mark.parametrize("n", [1, 2])
def test_param(n):
    if n == 2:
        pytest.skip("second parameter")


@pytest.fixture
def unavailable():
    pytest.skip("fixture reason")


def test_fixture(unavailable):
    pass


@pytest.mark.xfail(reason="expected failure")
def test_xfail():
    assert False


def test_passes():
    pass
'''

MODULE_SKIP = f'''\
import pytest

pytest.importorskip("{MISSING}")


def test_never_collected():
    pass
'''

#: Every skip the fixture raises, by node ID, with its reason and phase.
EXPECTED = {
    "tests/test_a_skips.py::test_marked": ("setup", "marker reason"),
    "tests/test_a_skips.py::test_conditional": ("setup", "condition reason"),
    "tests/test_a_skips.py::test_imperative": ("call", "imperative reason"),
    "tests/test_a_skips.py::test_param[2]": ("call", "second parameter"),
    "tests/test_a_skips.py::test_fixture": ("setup", "fixture reason"),
    "tests/test_b_module_skip.py": (
        "collect", f"could not import '{MISSING}': No module named '{MISSING}'"),
}


def _checkout(tmp_path: Path) -> Path:
    checkout = tmp_path / "project"
    (checkout / "tests").mkdir(parents=True)
    # Its own rootdir, as a real project has: an ini file in an ancestor of
    # the temporary directory would otherwise root the node IDs above it.
    (checkout / "pytest.ini").write_text("[pytest]\n")
    (checkout / "tests" / "test_a_skips.py").write_text(SKIPS)
    (checkout / "tests" / "test_b_module_skip.py").write_text(MODULE_SKIP)
    # Two shards take files round-robin, so the module skip shares its shard
    # with a test that runs: a shard that only skips at collection exits 5.
    for name in ("test_c_passes.py", "test_d_passes.py"):
        (checkout / "tests" / name).write_text("def test_passes():\n    pass\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    return checkout


def _dispatch(checkout: Path, monkeypatch, extra: list[str]):
    calls = []

    def admitted_payload(command, **kwargs):
        if str(pbtest.PBRUN) not in [str(part) for part in command]:
            return REAL_POPEN(command, **kwargs)
        calls.append(command)
        return REAL_POPEN(command[command.index("--") + 1:], cwd=checkout, **kwargs)

    monkeypatch.setattr(pbtest.subprocess, "Popen", admitted_payload)
    report = checkout.parent / "shards.json"
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(checkout), "--python", sys.executable,
        "--threads-per-shard", "1", "--json", str(report), *extra, "tests",
    ])
    return pbtest.main(), calls, json.loads(report.read_text())


@pytest.mark.parametrize("extra", [
    pytest.param(["--shards", "2"], id="one-file-per-shard"),
    pytest.param(["--shards", "1", "--workers-per-shard", "2"], id="xdist"),
])
def test_every_skip_reaches_the_receipt_with_its_reason(tmp_path, monkeypatch,
                                                         capsys, extra):
    if "--workers-per-shard" in extra:
        pytest.importorskip("xdist")
    checkout = _checkout(tmp_path)
    code, calls, results = _dispatch(checkout, monkeypatch, extra)
    printed = capsys.readouterr().out
    assert code == 0, printed
    assert calls, "no shard was submitted"

    recorded = {skip["nodeid"]: (skip["when"], skip["reason"])
                for result in results for skip in result["skipped"]}
    assert recorded == EXPECTED
    # The receipt's skips are exactly the ones each summary line counts.
    for result in results:
        assert len(result["skipped"]) == pbtest.summary_count(
            result["summary"], "skipped"), result["summary"]
    # The xfail is its own outcome, not a skip.
    assert not any("test_xfail" in nodeid for nodeid in recorded)
    # And the printed report says it too, one line per skip.
    assert f"{len(EXPECTED)} skipped, each with its reason:" in printed
    for nodeid, (_, reason) in EXPECTED.items():
        assert any(nodeid in line and reason in line for line in printed.splitlines()), (
            nodeid, printed)
    # The record is data for the receipt, not text for a person.
    assert outcomes.PREFIX not in printed


def test_a_skip_location_names_where_it_was_raised(tmp_path, monkeypatch, capsys):
    checkout = _checkout(tmp_path)
    _, _, results = _dispatch(checkout, monkeypatch, ["--shards", "1"])
    capsys.readouterr()
    located = {skip["nodeid"]: skip["location"]
               for result in results for skip in result["skipped"]}
    assert located["tests/test_a_skips.py::test_imperative"] == "tests/test_a_skips.py:15"
    assert located["tests/test_b_module_skip.py"].startswith("tests/test_b_module_skip.py:")


def _mocked_shard(tmp_path: Path, monkeypatch, output: str):
    checkout = tmp_path / "project"
    (checkout / "tests").mkdir(parents=True)
    (checkout / "tests" / "test_one.py").write_text("def test_one():\n    pass\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    class Finished:
        returncode = 0

        def communicate(self):
            return output, None

    def popen(command, **kwargs):
        if str(pbtest.PBRUN) in [str(part) for part in command]:
            return Finished()
        return REAL_POPEN(command, **kwargs)

    monkeypatch.setattr(pbtest.subprocess, "Popen", popen)
    report = tmp_path / "shards.json"
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
        "--shards", "1", "--json", str(report), "tests"])
    return pbtest.main(), json.loads(report.read_text())


def test_skips_without_a_record_are_named_as_unrecorded(tmp_path, monkeypatch, capsys):
    """No record is "reasons unknown", never "nothing skipped"."""

    code, results = _mocked_shard(
        tmp_path, monkeypatch, "..ss\n2 passed, 2 skipped in 0.10s\n")
    printed = capsys.readouterr().out
    assert code == 0
    assert results[0]["skipped"] is None
    assert "2 skip(s) with NO RECORDED REASON" in printed


def test_a_record_that_disagrees_with_the_summary_is_named(tmp_path, monkeypatch, capsys):
    record = outcomes.PREFIX + json.dumps({
        "schema": outcomes.SCHEMA, "collect_only": False,
        "collected": ["tests/test_one.py::test_one"],
        "reports": [["tests/test_one.py::test_one", "setup", "skipped", "why", None]],
    })
    _, results = _mocked_shard(
        tmp_path, monkeypatch, f"s\n{record}\n0 passed, 2 skipped in 0.10s\n")
    printed = capsys.readouterr().out
    assert [skip["nodeid"] for skip in results[0]["skipped"]] == ["tests/test_one.py::test_one"]
    assert "records 1 skip(s) and its summary counts 2" in printed


def test_a_foreign_record_is_not_read_as_an_empty_one():
    foreign = outcomes.PREFIX + json.dumps({"schema": "other.v9", "reports": []})
    assert outcomes.parse(f"{foreign}\n1 passed in 0.01s\n") is None
    assert outcomes.parse("1 passed in 0.01s\n") is None
    assert outcomes.parse(outcomes.PREFIX + "{not json\n") is None

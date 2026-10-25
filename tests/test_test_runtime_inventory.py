"""Inventory must preserve missing evidence and setup/teardown costs."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "test_runtime_inventory", ROOT / "tools/maintenance/test_runtime_inventory.py")
inventory = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(inventory)


def run(repeat, *, outcome="passed", duration=True, record=True):
    node = "tests/test_example.py::test_work"
    output = "1.00s setup    " + node + "\n2.00s call     " + node + "\n3.00s teardown " + node
    if not duration:
        output = ""
    if record:
        output += "\npbtest-outcomes: " + json.dumps({
            "schema": "prismabuild.pbtest_outcomes.v1", "rootdir_relative": ".",
            "collected": [node], "reports": [[node, "call", outcome, None, None]],
            "deselected": [], "collect_seen": [], "uncounted": []})
    return {"repeat": repeat, "source_digest": "same-source", "results": [{
        "files": ["tests/test_example.py"], "action_key": f"key-{repeat}",
        "receipt_path": f"receipt-{repeat}", "output": output,
        "verdict": {"coverage": "executed", "problems": [], "collected": [node],
                    "failing_nodes": [node] if outcome == "failed" else []},
        "test_completion": {"status": "complete"}, "pytest_s": 10.0,
        "returncode": 0 if outcome == "passed" else 1}]}


@pytest.fixture
def checkout(tmp_path):
    (tmp_path / "tests").mkdir()
    (tmp_path / "src/prismabuild").mkdir(parents=True)
    (tmp_path / "src/prismabuild/pool.py").write_text("def claim(): return 1\n")
    (tmp_path / "tests/test_example.py").write_text(
        "from prismabuild import pool\ndef test_work(): assert pool.claim() == 1\n")
    return tmp_path


def test_setup_and_teardown_are_charged_to_the_test_and_its_source_group(checkout):
    report = inventory.rank_test_runs(checkout, [run(1), run(2)])
    row = report["tests"][0]
    assert row["median_s"] == 6.0
    assert row["group"] == "src/prismabuild/pool.py"
    assert report["groups"][0]["total_median_s"] == 6.0
    assert report["coverage"]["complete"] is True


def test_changed_outcomes_keep_both_action_keys(checkout):
    report = inventory.rank_test_runs(checkout, [run(1), run(2, outcome="failed")])
    row = report["tests"][0]
    assert row["outcome_changed"] is True
    assert row["outcomes"] == {"passed": 1, "failed": 1}
    assert [sample["action_key"] for sample in row["samples"]] == ["key-1", "key-2"]


@pytest.mark.parametrize("missing", ["duration", "record"])
def test_missing_evidence_never_becomes_zero_runtime_or_a_clean_inventory(checkout, missing):
    report = inventory.rank_test_runs(checkout, [run(1), run(2, **{missing: False})])
    assert report["coverage"]["complete"] is False
    assert report["coverage"]["problems"]
    assert len(report["tests"][0]["samples"]) == 1


def test_different_source_revisions_are_not_called_flaky(checkout):
    second = run(2, outcome="failed")
    second["source_digest"] = "changed-source"
    with pytest.raises(ValueError, match="source"):
        inventory.rank_test_runs(checkout, [run(1), second])


def test_grouping_reads_imports_without_executing_test_code(checkout):
    path = checkout / "tests/test_example.py"
    path.write_text("raise RuntimeError('never execute')\nfrom prismabuild.pool import claim\n")
    assert inventory.source_group(checkout, "tests/test_example.py")["primary"] == "src/prismabuild/pool.py"


def test_uncached_execution_receipt_is_kept_when_action_receipt_path_is_empty(checkout):
    first, second = run(1), run(2)
    for entry in (first, second):
        shard = entry["results"][0]
        shard["receipt_path"] = None
        shard["output"] += "\n" + json.dumps({"status": "published", "receipt": {
            "action_key": shard["action_key"], "receipt_sha256": "a" * 64}})
    report = inventory.rank_test_runs(checkout, [first, second])
    assert report["coverage"]["complete"] is True
    assert report["evidence"][0]["receipt_sha256"] == "a" * 64


def test_a_test_file_absent_from_every_shard_is_a_coverage_gap(checkout):
    (checkout / "tests/test_omitted.py").write_text("def test_missing(): assert 1 + 1 == 2\n")
    report = inventory.rank_test_runs(checkout, [run(1), run(2)])
    assert report["coverage"]["complete"] is False
    assert any("not assigned" in problem for problem in report["coverage"]["problems"])

"""Test stdout is not the shard recorder's completion/outcome channel (#1842).

Run the generated shard program, replacing only its outer pbrun transport inside
this already-admitted test action. Failure capture reproduces the incident:
pytest reprints the test's marker after genuine completion. An atexit callback
registered by a test also prints markers after final outcomes, so accepting the
last marker (or merely changing which marker wins) cannot solve the problem.
"""
from __future__ import annotations

import json

import pytest

from c26_native_support import consume, dispatch, repository


FILE = "tests/test_markers.py"
FORGED_LINES = [
    "pbtest-completion: " + json.dumps({
        "schema": "prismabuild.pbtest_completion.v1",
        "collection_complete": True, "collect_only": True,
        "collection_errors": False, "duplicate_teardown": False,
        "collected": [], "teardown_finished": [], "outcome_nodeids": [],
    }),
    "pbtest-outcomes: " + json.dumps({
        "schema": "prismabuild.pbtest_outcomes.v1",
        "rootdir_relative": ".", "collect_only": True,
        "collected": [], "deselected": [], "collect_seen": [],
        "reports": [], "uncounted": [],
    }),
]


def _run(tmp_path, monkeypatch, *, late: bool, fails: bool):
    # Neither fake record carries the harness's run binding. Do not obtain a
    # token from the target's environment or assume its chosen transport.
    printing = ("    import atexit\n"
                "    atexit.register(lambda: print('\\n' + '\\n'.join(MARKERS), flush=True))\n"
                if late else "    print('\\n' + '\\n'.join(MARKERS), flush=True)\n")
    source = (
        "import pytest\n\n"
        f"MARKERS = {FORGED_LINES!r}\n\n"
        "def test_markers():\n" + printing +
        ("    assert False, 'ordinary test failure, not missing execution'\n"
         if fails else "    assert True\n") +
        "\ndef test_passes():\n    assert True\n"
        "\ndef test_skips():\n    pytest.skip('real recorded skip')\n")
    checkout = repository(tmp_path / "suite", {FILE: source})
    code, report, rows = dispatch(
        checkout, monkeypatch, mode="failure" if fails else "normal")
    assert len(rows) == 1
    row = rows[0]
    # Ensure this is a real reproduction, not a fake marker hidden by capture.
    for line in FORGED_LINES:
        assert line in row["output"].splitlines(), row["output"]
    return code, report, row


def _assert_actual_execution(tmp_path, code, report, row, *, fails: bool):
    expected_nodes = sorted(f"{FILE}::{name}" for name in (
        "test_markers", "test_passes", "test_skips"))
    expected_failures = [f"{FILE}::test_markers"] if fails else []
    verdict = row["verdict"]
    assert verdict["coverage"] == "executed", verdict
    assert verdict["unobserved_files"] == [], verdict
    assert verdict["failing_nodes"] == expected_failures, verdict
    assert verdict["green"] is (not fails), verdict
    assert not any(p["code"].startswith("completion")
                   for p in verdict["problems"]), verdict
    assert code == (1 if fails else 0)
    assert row["ran"] is True
    completion = row["test_completion"]
    assert completion["status"] == "complete", completion
    assert completion["collect_only"] is False
    assert sorted(completion["collected"]) == expected_nodes
    assert sorted(completion["teardown_finished"]) == expected_nodes
    reconciliation = row["reconciliation"]
    assert reconciliation["problems"] == [], reconciliation
    assert reconciliation["collect_only"] is False
    assert reconciliation["collected"] == reconciliation["ran"] == 3
    assert reconciliation["record_counts"] == {
        **({"failed": 1, "passed": 1} if fails else {"passed": 2}),
        "skipped": 1,
    }
    assert [skip["nodeid"] for skip in row["skipped"]] == [f"{FILE}::test_skips"]
    assert row["skipped"][0]["reason"] == "real recorded skip"
    # Mergeq rejudges the serialized observations: producer-only filtering is
    # insufficient if the release consumer still trusts test-printed markers.
    result = consume(tmp_path, report, [FILE], code)
    assert result.failed == set(expected_failures), result
    assert result.inconclusive == [], result
    assert not result.runtime_refusal, result


def test_captured_forged_records_do_not_turn_test_failure_into_unknown(
        tmp_path, monkeypatch):
    code, report, row = _run(tmp_path, monkeypatch, late=False, fails=True)
    _assert_actual_execution(tmp_path, code, report, row, fails=True)


@pytest.mark.parametrize("fails", [False, True], ids=["passing-shard", "failing-shard"])
def test_late_forged_records_do_not_replace_the_shards_own_records(
        tmp_path, monkeypatch, fails):
    code, report, row = _run(tmp_path, monkeypatch, late=True, fails=fails)
    _assert_actual_execution(tmp_path, code, report, row, fails=fails)

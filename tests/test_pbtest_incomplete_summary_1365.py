"""A missing terminal summary does not prove zero execution (PB #1365)."""
from __future__ import annotations

from pathlib import Path

import pytest

from test_pbtest_reports_whether_pytest_ran import _one_shard
from pbtest_shard_output import execution_reply


@pytest.mark.parametrize("output,returncode,timed_out", [
    pytest.param(
        "................................F [82%]\n"
        "pbrun: timeout on dl380g10 in 3620s\n",
        1, True, id="partial-pytest-action-timeout"),
    pytest.param(
        "PrismaBuild per-test bound: tests/test_one.py::test_one exceeded "
        "the per-test bound of 300s during call\n",
        1, False, id="known-call-without-final-roster"),
    pytest.param("", -9, False, id="signal-with-unknown-execution"),
    pytest.param("........ [53%]\n", 0, False, id="clean-exit-without-summary"),
])
def test_missing_final_summary_reports_unknown_coverage(
    tmp_path: Path, monkeypatch, output: str, returncode: int, timed_out: bool,
) -> None:
    exit_code, record = _one_shard(
        tmp_path, monkeypatch, output, returncode, with_exit=True,
        client_reply=execution_reply(output, returncode, timed_out=True) if timed_out else None)

    # The compatibility flag means a verified terminal summary exists, not
    # that no case ran when it is false. Missing counts never admit green.
    assert record["ran"] is False
    assert exit_code == 1
    assert record["summary"].startswith("NO PYTEST SUMMARY")
    assert "1 file(s) have no verified final result" in record["summary"]
    assert "execution/coverage unknown" in record["summary"]
    assert "did not run" not in record["summary"]
    assert "before or outside pytest" not in record["summary"]

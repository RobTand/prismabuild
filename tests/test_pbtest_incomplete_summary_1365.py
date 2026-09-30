"""A missing terminal summary does not prove zero execution (PB #1365)."""
from __future__ import annotations

from pathlib import Path

import pytest

from test_pbtest_reports_whether_pytest_ran import _one_shard


@pytest.mark.parametrize("output,returncode", [
    pytest.param(
        "................................F [82%]\n"
        "pbrun: timeout on dl380g10 in 3620s\n",
        1, id="partial-pytest-action-timeout"),
    pytest.param(
        "PrismaBuild per-test bound: tests/test_one.py::test_one exceeded "
        "the per-test bound of 300s during call\n",
        1, id="known-call-without-final-roster"),
    pytest.param("", -9, id="signal-with-unknown-execution"),
    pytest.param("........ [53%]\n", 0, id="clean-exit-without-summary"),
])
def test_missing_final_summary_reports_unknown_coverage(
    tmp_path: Path, monkeypatch, output: str, returncode: int,
) -> None:
    exit_code, record = _one_shard(
        tmp_path, monkeypatch, output, returncode, with_exit=True)

    # The compatibility flag means a verified terminal summary exists, not
    # that no case ran when it is false. Missing counts never admit green.
    assert record["ran"] is False
    assert exit_code == 1
    assert record["summary"].startswith("NO PYTEST SUMMARY")
    assert "1 file(s) have no verified final result" in record["summary"]
    assert "execution/coverage unknown" in record["summary"]
    assert "did not run" not in record["summary"]
    assert "before or outside pytest" not in record["summary"]

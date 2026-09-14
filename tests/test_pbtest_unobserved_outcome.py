"""A shard whose ending pbrun could not read did not "not run" (#558).

On 2026-09-14 two shards printed ``rc=74 NO PYTEST SUMMARY -- 39 file(s) did
not run`` while one of their actions later landed in ``done/`` with 658 passed
and the other was still running. The shard is still not green; its summary
must say the outcome was not observed, and name the key to read.
"""
from __future__ import annotations

import pytest

from test_pbtest_reports_whether_pytest_ran import _one_shard


UNOBSERVED = [
    pytest.param(
        "pbrun: unavailable pool outcome for 13e6c8621baf: "
        "pool outcome observation timed out after 5.0s\n",
        id="zero-patience-or-retained-reader"),
    pytest.param(
        "pbrun: pool outcome for 13e6c8621baf not observed yet (pool outcome "
        "observation timed out after 5.0s); 1 consecutive unavailable read(s), "
        "retrying inside --wait-s\n"
        "pbrun: unavailable pool outcome for 13e6c8621baf when the wait ended: "
        "3 consecutive unavailable read(s), the last: pool outcome observation "
        "timed out after 5.0s.\n",
        id="patient-wait-ended-unobserved"),
]


@pytest.mark.parametrize("output", UNOBSERVED)
def test_unobserved_outcome_is_not_reported_as_files_that_did_not_run(
    tmp_path, monkeypatch, output,
):
    exit_code, record = _one_shard(tmp_path, monkeypatch, output, 74, with_exit=True)
    assert "did not run" not in record["summary"]
    assert record["summary"].startswith("OUTCOME UNOBSERVED")
    assert "13e6c8621baf" in record["summary"]
    assert record["ran"] is False
    assert exit_code == 1


def test_other_exit_74_still_reports_files_that_did_not_run(tmp_path, monkeypatch):
    """Guard: only pbrun's unobserved-outcome line changes the summary."""

    record = _one_shard(
        tmp_path, monkeypatch,
        "pbrun: cannot record the submission: [Errno 13] Permission denied\n", 74)
    assert record["summary"].startswith("NO PYTEST SUMMARY")
    assert record["ran"] is False

"""Recover observation, never resubmit successful shard work (#1033)."""
from pathlib import Path
import runpy
import sys
import time
from types import SimpleNamespace

import pytest

from pbtest_shard_output import ONE_PASS, shard_output
from test_pbtest_retries_an_offer_discovery_timeout import _shard, pbtest

KEY = "a" * 64
QUEUED = f"pbrun: queued {KEY} published_unix=1234.5 tags=['x86']\n"
UNOBSERVED = f"pbrun: unavailable pool outcome for {KEY[:12]}: reader failed\n"


def test_unobserved_shard_recovers_summary_and_outcomes(tmp_path, monkeypatch):
    code, record, launches, clock = _shard(
        tmp_path, monkeypatch, [(QUEUED + UNOBSERVED, 74), (ONE_PASS, 0)])
    assert code == 0
    assert record["returncode"] == 0
    assert record["summary"] == "1 passed in 0.01s"
    assert record["reconciliation"]["problems"] == []
    assert len(launches) == 2
    assert "--checkout" not in launches[1] and "--cwd" not in launches[1]
    assert launches[1] != launches[0]  # read-back, not another submission
    assert KEY in launches[1] and "1234.5" in launches[1]
    assert clock.now < 1000 + 10800


@pytest.mark.parametrize("prefix,wait_s", [
    (QUEUED.replace(" published_unix=1234.5", ""), 10800),
    (QUEUED + 'pbrun: retained reader=[{"pid": 4242}]\n', 10800),
    (QUEUED, 1),
])
def test_unknown_generation_retained_reader_or_expired_budget_stays_unobserved(
        tmp_path, monkeypatch, prefix, wait_s):
    code, record, launches, _ = _shard(
        tmp_path, monkeypatch, [(prefix + UNOBSERVED, 74), (ONE_PASS, 0)],
        wait_s=wait_s)
    assert code == 1
    assert record["returncode"] == 74
    assert len(launches) == 1


def test_recovered_failure_is_not_green(tmp_path, monkeypatch):
    failed = shard_output(passed=1).replace("1 passed", "1 failed")
    code, record, launches, _ = _shard(
        tmp_path, monkeypatch, [(QUEUED + UNOBSERVED, 74), (failed, 1)])
    assert len(launches) == 2
    assert code == 1 and record["returncode"] == 1


def test_unavailable_recovery_does_not_loop_or_reset_deadline(tmp_path, monkeypatch):
    code, record, launches, clock = _shard(
        tmp_path, monkeypatch, [(QUEUED + UNOBSERVED, 74)], wait_s=12)
    assert len(launches) == 2
    assert launches[1][-1] == "1012.0"  # absolute original monotonic deadline
    assert code == 1 and record["returncode"] == 74
    assert clock.now == 1010


@pytest.mark.parametrize("now,expected", [(1005, 0), (1012, 75)])
def test_recovery_program_uses_remaining_original_budget(monkeypatch, now, expected):
    calls = []
    monkeypatch.setattr(time, "monotonic", lambda: now)
    monkeypatch.setattr(runpy, "run_path", lambda path: {
        "SH": Path("/unused-fixture"), "GAVE_UP_EXIT": 75,
        "pool": SimpleNamespace(PoolQueue=lambda path: path),
        "await_outcome": lambda queue, key, **kw: calls.append((key, kw)) or 0,
    })
    monkeypatch.setattr(sys, "argv", ["-c", "fixture-pbrun", KEY, "1234.5", "1012"])
    with pytest.raises(SystemExit) as exc:
        exec(pbtest._RECOVER_OUTCOME, {})
    assert exc.value.code == expected
    assert calls == ([(KEY, {"wait_s": 7.0, "generation": 1234.5})] if now < 1012 else [])

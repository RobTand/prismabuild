"""Recover observation, never resubmit successful shard work (#1033)."""
from pathlib import Path
import sys
import time
from types import SimpleNamespace

import pytest

from pbtest_shard_output import ONE_PASS, shard_output, ClientReply, pool_identity
from prismabuild import action_client as client, action_wait, client_recovery
from test_pbtest_retries_an_offer_discovery_timeout import _shard, pbtest

KEY = "a" * 64
QUEUED = f"pbrun: queued {KEY} published_unix=1234.5 tags=['x86']\n"
UNOBSERVED = f"pbrun: unavailable pool outcome for {KEY[:12]}: reader failed\n"


def _unobserved(output=QUEUED + UNOBSERVED, *, generation=1234.5, retained=False):
    reader = action_wait.RetainedReader('earlier read', 4242, 17, 1.0)
    return ClientReply(output, client.ClientUnavailable(KEY, generation, action_wait.WaitEnd.FINAL_EMPTY),
        pool_identity(KEY, generation), action_wait.ReaderHistory(1, (reader,), True)
        if retained else action_wait.ReaderHistory())


def test_unobserved_shard_recovers_summary_and_outcomes(tmp_path, monkeypatch):
    code, record, launches, clock = _shard(
        tmp_path, monkeypatch, [(_unobserved(), 74), (ONE_PASS, 0)])
    assert code == 0
    assert record["returncode"] == 0
    assert record["summary"] == "1 passed in 0.01s"
    assert record["reconciliation"]["problems"] == []
    assert len(launches) == 2
    assert "--checkout" not in launches[1] and "--cwd" not in launches[1]
    assert launches[1] != launches[0]  # read-back, not another submission
    assert KEY in launches[1] and "1234.5" in launches[1]
    assert clock.now < 1000 + 10800


@pytest.mark.parametrize("prefix,wait_s,generation,retained", [
    pytest.param(QUEUED.replace(" published_unix=1234.5", ""), 10800, None, False, id="pbrun: queued aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa tags=['x86']\n-10800"),
    pytest.param(QUEUED + 'pbrun: retained reader=[{"pid": 4242}]\n', 10800, 1234.5, True, id='pbrun: queued aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa published_unix=1234.5 tags=[\'x86\']\npbrun: retained reader=[{"pid": 4242}]\n-10800'),
    pytest.param(QUEUED, 1, 1234.5, False, id="pbrun: queued aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa published_unix=1234.5 tags=['x86']\n-1"),
])
def test_unknown_generation_retained_reader_or_expired_budget_stays_unobserved(
        tmp_path, monkeypatch, prefix, wait_s, generation, retained):
    code, record, launches, _ = _shard(
        tmp_path, monkeypatch, [(_unobserved(prefix + UNOBSERVED, generation=generation, retained=retained), 74), (ONE_PASS, 0)],
        wait_s=wait_s)
    assert code == 1
    assert record["returncode"] == 74
    assert len(launches) == 1


def test_recovered_failure_is_not_green(tmp_path, monkeypatch):
    failed = shard_output(passed=1).replace("1 passed", "1 failed")
    code, record, launches, _ = _shard(
        tmp_path, monkeypatch, [(_unobserved(), 74), (failed, 1)])
    assert len(launches) == 2
    assert code == 1 and record["returncode"] == 1


def test_unavailable_recovery_does_not_loop_or_reset_deadline(tmp_path, monkeypatch):
    code, record, launches, clock = _shard(
        tmp_path, monkeypatch, [(_unobserved(), 74)], wait_s=12)
    assert len(launches) == 2
    assert launches[1][launches[1].index("--deadline") + 1] == "1012.0"  # absolute original monotonic deadline
    assert code == 1 and record["returncode"] == 74
    assert clock.now == 1010


@pytest.mark.parametrize("now,expected", [(1005, 0), (1012, 75)])
def test_recovery_program_uses_remaining_original_budget(monkeypatch, now, expected):
    calls = []
    monkeypatch.setattr(time, "monotonic", lambda: now)
    def await_typed_outcome(queue, key, *, patience, generation, **kwargs):
        calls.append((key, {'wait_s': patience.deadline - now, 'generation': generation}))
        return SimpleNamespace(ending=client.ClientEnding(client.ClientCommand(0)))

    monkeypatch.setattr(client_recovery.pool, 'PoolQueue', lambda path: path)
    monkeypatch.setattr(client, 'await_pool_outcome', await_typed_outcome)
    monkeypatch.setattr(client, 'render_pool_outcome', lambda outcome: None)
    with pytest.raises(SystemExit) as exc:
        raise SystemExit(client_recovery.main(['--action-key', KEY, '--published-unix', '1234.5',
            '--deadline', '1012', '--queue-root', '/unused-fixture/queue', '--cas-root', '/unused-fixture/cas']))
    assert exc.value.code == expected
    assert calls == ([(KEY, {"wait_s": 7.0, "generation": 1234.5})] if now < 1012 else [])

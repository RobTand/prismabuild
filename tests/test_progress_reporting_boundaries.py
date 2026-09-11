"""Progress reports are untrusted input, and zero completed work is not work."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from test_progress_keeps_a_working_action_alive import _policy, _run, pool, pb


def _watch(tmp_path):
    path = tmp_path / "action.progress"
    policy = pool.ProgressPolicy((pool.ProgressPhase("startup", 10, None),), None)
    return pool.ProgressWatch(path, "launch", policy, started=0)


def _record(**changes):
    return dict(schema=pb.PROGRESS_RECORD_SCHEMA_V1, token="launch",
                phase="startup", units_completed=1, **changes)


@pytest.mark.parametrize("raw", [
    b"\xff",
    b"[" * 2000 + b"]" * 2000,
    json.dumps(_record(unit="x" * 65536)).encode(),
    b'{"schema":"prismabuild.action_progress.v1","token":"other",'
    b'"token":"launch","phase":"startup","units_completed":1}',
], ids=["invalid-utf8", "deep-json", "oversized", "duplicate-token"])
def test_malformed_or_oversized_report_does_not_advance_or_escape(tmp_path, raw):
    watch = _watch(tmp_path)
    watch.path.write_bytes(raw)
    assert watch.sample(now=5) is False
    assert watch.accepted == 0
    assert watch.rejected == 1
    assert watch.stall_deadline() == 10


def test_symlink_report_is_not_followed(tmp_path):
    watch = _watch(tmp_path)
    target = tmp_path / "elsewhere"
    target.write_text(json.dumps(_record()))
    watch.path.symlink_to(target)
    assert watch.sample(now=5) is False
    assert watch.stall_deadline() == 10


def test_fifo_report_cannot_block_the_watchdog(tmp_path):
    watch = _watch(tmp_path)
    os.mkfifo(watch.path)
    # The pre-fix read blocks on open; isolate only that read so this regression
    # fails promptly and cleans up the exact child it created.
    code = '''
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[2])
from prismabuild import pool
policy = pool.ProgressPolicy((pool.ProgressPhase("startup", 10, None),), None)
watch = pool.ProgressWatch(Path(sys.argv[1]), "launch", policy, started=0)
assert watch.sample(now=5) is False
'''
    result = subprocess.run(
        [sys.executable, "-c", code, str(watch.path),
         str(Path(pool.__file__).resolve().parents[1])],
        capture_output=True, text=True, timeout=3,
    )
    assert result.returncode == 0, result.stderr


def test_first_zero_count_does_not_grant_a_second_startup_allowance(tmp_path):
    watch = _watch(tmp_path)
    record = _record()
    record["units_completed"] = 0
    watch.path.write_text(json.dumps(record))
    assert watch.sample(now=9) is False
    assert watch.stall_deadline() == 10


def test_integer_advancement_keeps_its_precision(tmp_path):
    watch = _watch(tmp_path)
    record = _record()
    for units in (2**53, 2**53 + 1):
        record["units_completed"] = units
        watch.path.write_text(json.dumps(record))
        assert watch.sample(now=1) is True
        assert watch.last_accepted["units_completed"] == units


def test_large_integer_report_does_not_crash_numeric_validation(tmp_path):
    watch = _watch(tmp_path)
    record = _record(reported_unix=10**400)
    record["units_completed"] = 10**400
    watch.path.write_text(json.dumps(record))
    # Integers are finite and must not be converted through a lossy float.
    assert watch.sample(now=1) is True
    assert watch.last_accepted["units_completed"] == 10**400


def test_corrupt_report_ends_as_a_stall_with_terminal_evidence(tmp_path, monkeypatch):
    import test_progress_keeps_a_working_action_alive as fixtures

    reporter = '''
import os, time
open(os.environ["PRISMABUILD_ACTION_PROGRESS_PATH"], "wb").write(b"\\xff")
time.sleep(30)
'''
    monkeypatch.setattr(fixtures, "REPORTER", reporter)
    outcome = _run(tmp_path, mode="unused", seconds=30, ceiling=5,
                   policy=_policy(0.4))
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    assert outcome["action_survived_kill"] is False
    assert outcome["progress_observation"]["accepted_count"] == 0
    assert outcome["progress_observation"]["rejected_count"] > 0


def test_completion_samples_the_final_report_before_removing_it(tmp_path):
    from test_progress_keeps_a_working_action_alive import _claimed

    queue, item = _claimed(tmp_path, mode="report", seconds=0.2,
                           policy=_policy(60, 60, 60))
    outcome = queue.execute(item, timeout_s=60, heartbeat_s=30,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "executed"
    assert outcome["progress_observation"]["accepted_count"] == 1
    assert outcome["progress_observation"]["last_accepted"]["units_completed"] >= 3


def test_unrepresentable_grace_is_a_contract_refusal():
    with pytest.raises(pb.ActionContractError, match="positive finite"):
        pb.validate_progress_policy(_policy(10**400))


def test_first_lease_delay_does_not_spend_startup_grace(tmp_path, monkeypatch):
    from test_progress_keeps_a_working_action_alive import _claimed

    queue, item = _claimed(tmp_path, mode="report", seconds=0.5,
                           policy=_policy(0.4, 0.4, 0.4))
    offset = [0.0]
    monkeypatch.setattr(pool, "time", SimpleNamespace(
        monotonic=lambda: time.monotonic() + offset[0],
        time=time.time, sleep=time.sleep))
    original = queue.write_lease

    def lease(*args, **kwargs):
        original(*args, **kwargs)
        offset[0] += 10

    monkeypatch.setattr(queue, "write_lease", lease)
    outcome = queue.execute(item, timeout_s=5, heartbeat_s=0.05,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "executed"


def test_checkpoint_before_an_accepted_report_is_refunded_only_once(tmp_path, monkeypatch):
    from test_progress_keeps_a_working_action_alive import _claimed

    queue, item = _claimed(tmp_path, mode="replay", seconds=1.5,
                           policy=_policy(0.3, 0.3, 0.3))
    offset = [0.0]
    monkeypatch.setattr(pool, "time", SimpleNamespace(
        monotonic=lambda: time.monotonic() + offset[0],
        time=time.time, sleep=time.sleep))
    original = queue.withdrawal_covers

    def withdrawal(*args, **kwargs):
        result = original(*args, **kwargs)
        offset[0] += 10
        return result

    monkeypatch.setattr(queue, "withdrawal_covers", withdrawal)
    outcome = queue.execute(item, timeout_s=5, heartbeat_s=0.05,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    assert outcome["elapsed_s"] < 1.2


def test_documented_package_reporter_is_available():
    import prismabuild

    assert prismabuild.report_action_progress is pb.report_action_progress


def test_progress_submit_notice_distinguishes_deadline_from_phase_clamp(
    tmp_path, monkeypatch, capsys,
):
    from test_pbrun_detach import _checkout, _run_pbrun

    work = _checkout(tmp_path)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(host="sparky", tags=["sparky", "gb10", pb.PROGRESS_TAG],
                   has_gpu=True, capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
                   timeout_ceiling_s=600,
                   progress_contracts=[pb.PROGRESS_RECORD_SCHEMA_V1])
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach",
                      "--progress-phase", "startup=900", "--timeout-s", "1800") == 0
    notice = capsys.readouterr().err
    assert "may be killed at 600s" not in notice
    assert "will be killed at 600s" not in notice
    assert "startup" in notice and "600s" in notice and "900s" in notice

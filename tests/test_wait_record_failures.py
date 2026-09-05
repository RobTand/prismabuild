"""Record faults retain the job and cancellation facts at the CLI boundary."""
from pathlib import Path
import json
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'src'), str(ROOT / 'tools' / 'fleet')]
from prismabuild import core as pb, pool, slurm_lane
import pbrun
import pbwait
from test_slurm_lane import fleet, _runnable_action
import fleet_submit


@pytest.mark.parametrize('waiter', ['pbrun', 'pbwait'])
def test_corruption_landing_during_pool_wait_ends_the_wait(tmp_path, monkeypatch, capsys, waiter):
    q = pool.PoolQueue(tmp_path / 'queue')
    q.ensure_layout()
    key = 'a' * 64
    q.item_path(pool.READY, key).write_text(json.dumps({'action_key': key, 'published_unix': 1.0}))
    sleeps = []
    def land(_):
        sleeps.append(1)
        q.item_path(pool.FAILED, key).write_text('{')
        if len(sleeps) > 1:
            pytest.fail('wait polled again after a malformed terminal record landed')
    monkeypatch.setattr(pbrun.time, 'sleep', land)
    if waiter == 'pbrun':
        assert pbrun.await_outcome(q, key, wait_s=60, generation=1.0) == 1
        assert 'not valid JSON' in capsys.readouterr().err
    else:
        row = pbwait.wait_one(q, key, cas=pb.PrismaBuildCAS(tmp_path / 'cas'), deadline=time.monotonic() + 60)
        assert row['status'] == 'unreadable'
        assert 'not valid JSON' in row['note']
    assert len(sleeps) == 1


def _submitted(tmp_path, monkeypatch):
    monkeypatch.setenv('FAKE_SBATCH_VERDICT', 'exit:7')
    cas = pb.PrismaBuildCAS(tmp_path / 'cas')
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    q = pool.PoolQueue(tmp_path / 'queue')
    q.ensure_layout()
    fleet_submit.submit(action, cas=cas, request_path=request, transport='slurm', queue_root=q.root)
    return q, cas, action


def test_lane_can_heal_a_malformed_terminal_record(tmp_path, monkeypatch, fleet):
    q, cas, action = _submitted(tmp_path, monkeypatch)
    key = action['action_key']
    q.item_path(pool.FAILED, key).write_text('{')
    publish = slurm_lane._publish_json_if_absent
    attempts = []
    def bounded_publish(*args, **kwargs):
        attempts.append(1)
        assert len(attempts) <= 3, 'malformed existing record caused a publication loop'
        return publish(*args, **kwargs)
    monkeypatch.setattr(slurm_lane, '_publish_json_if_absent', bounded_publish)
    rows = pbwait.wait_for_keys(q, [key], cas=cas, wait_s=1, poll_s=0)
    assert rows[0]['status'] == 'failed'
    assert json.loads(q.item_path(pool.FAILED, key).read_text())['status'] == 'failed'


@pytest.mark.parametrize('receipt', [False, True])
def test_pbwait_reports_an_unwritable_ending(tmp_path, monkeypatch, fleet, capsys, receipt):
    q, cas, action = _submitted(tmp_path, monkeypatch)
    key = action['action_key']
    if receipt:
        monkeypatch.setattr(cas, 'lookup', lambda _: {'result': 'present'})
    directory = q.dir(pool.DONE if receipt else pool.FAILED)
    directory.chmod(0o555)
    try:
        rows = pbwait.wait_for_keys(q, [key], cas=cas, wait_s=1, poll_s=0)
    finally:
        directory.chmod(0o755)
    assert rows[0]['status'] == 'record_error'
    assert rows[0]['job'].isdigit()
    assert pbwait.verdict(rows) == pbrun.RECORD_WRITE_FAILED_EXIT
    assert 'Permission denied' in rows[0]['note']
    assert 'pbwait.py' in capsys.readouterr().err


@pytest.mark.parametrize('stage', ['before', 'after'])
def test_withdraw_write_failure_reports_whether_cancel_was_sent(tmp_path, monkeypatch, fleet, capsys, stage):
    q, cas, action = _submitted(tmp_path, monkeypatch)
    key = action['action_key']
    cancelled = []
    monkeypatch.setattr(pbrun, '_jobs_to_cancel', lambda key, job_id, **kw: [job_id])
    monkeypatch.setattr(slurm_lane, 'cancel', lambda job, **kw: cancelled.append(job) or True)
    def refuse(*args, **kwargs):
        raise PermissionError(13, 'Permission denied', str(q.root / 'withdrawn' / '.record.tmp'))
    monkeypatch.setattr(pbrun, '_file_slurm_withdrawal' if stage == 'before' else '_stamp_scancel_accepted', refuse)
    assert pbrun.withdraw_slurm_main([key], queue_root=q.root) == pbrun.RECORD_WRITE_FAILED_EXIT
    err = capsys.readouterr().err
    assert bool(cancelled) == (stage == 'after')
    assert ('no cancellation was sent' if stage == 'before' else 'scancel accepted') in err
    assert 'Permission denied' in err
    assert '--withdraw' in err

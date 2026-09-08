"""A heartbeat and observed output answer different liveness questions."""
import json
import time
import uuid

import pytest

from prismabuild import pool
from test_pbstatus import pbstatus


@pytest.mark.parametrize('prints', [True, False])
def test_execution_ticks_report_output_without_inventing_quiet_progress(tmp_path, monkeypatch, prints):
    script = tmp_path / 'worker.py'
    script.write_text(
        'import sys, time\n'
        + ('print("hello", flush=True)\nprint("warning", file=sys.stderr, flush=True)\n' if prints else '')
        + 'time.sleep(0.35)\n')
    queue = pool.PoolQueue(tmp_path / 'queue')
    key = uuid.uuid4().hex * 2
    queue.publish(action_key=key, cas_root='/cas', checkout_root=tmp_path, worker_script=script)
    item = queue.claim()
    ticks = []
    original = queue.write_lease

    def record(*args, **kwargs):
        original(*args, **kwargs)
        ticks.append(json.loads(queue.lease_path(key).read_text()))

    monkeypatch.setattr(queue, 'write_lease', record)
    result = queue.execute(item, heartbeat_s=0.05)
    assert result['status'] == 'executed'
    observations = [tick.get('execution_observation') for tick in ticks]
    assert observations and all(isinstance(value, dict) for value in observations), (
        'execution heartbeats omit launcher liveness and last output observation')
    assert all(value['launcher_alive'] is True for value in observations)
    assert all(value['sampled_unix'] <= tick['heartbeat_unix']
               for value, tick in zip(observations, ticks))
    if prints:
        noisy = [value for value in observations if value['stdout_bytes']]
        assert noisy and noisy[-1]['stdout_bytes'] == len(b'hello\n')
        assert noisy[-1]['stderr_bytes'] == len(b'warning\n')
        assert len({value['last_output_unix'] for value in noisy}) == 1
    else:
        assert all(value['last_output_unix'] is None for value in observations)
        assert all(value['stdout_bytes'] == value['stderr_bytes'] == 0 for value in observations)
    assert result['execution_observation'] == observations[-1]


def test_exited_launcher_does_not_report_its_pipe_holding_descendant_as_alive(tmp_path, monkeypatch):
    script = tmp_path / 'worker.py'
    script.write_text('import subprocess, sys\n'
                      'subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.4)"])\n')
    queue = pool.PoolQueue(tmp_path / 'queue')
    key = uuid.uuid4().hex * 2
    queue.publish(action_key=key, cas_root='/cas', checkout_root=tmp_path, worker_script=script)
    item = queue.claim()
    ticks = []
    original = queue.write_lease

    def record(*args, **kwargs):
        original(*args, **kwargs)
        ticks.append(json.loads(queue.lease_path(key).read_text()))

    monkeypatch.setattr(queue, 'write_lease', record)
    result = queue.execute(item, heartbeat_s=0.05)
    assert result['status'] == 'executed'
    assert any(tick['execution_observation']['launcher_alive'] is False for tick in ticks)
    assert queue.item_path(pool.CLAIMED, key).exists()
    assert not queue.item_path(pool.READY, key).exists()


def _observed_claim(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    key = 'b' * 64
    queue.publish(action_key=key, cas_root='/cas', checkout_root=tmp_path, worker_script='/worker.py')
    item = queue.claim(owner='worker')
    now = time.time()
    monkeypatch.setattr(pbstatus.time, 'time', lambda: now)
    lease = json.loads(queue.lease_path(key).read_text())
    lease['published_unix'] = item['published_unix']
    lease['execution_observation'] = dict(sampled_unix=now, launcher_alive=True,
                                        stdout_bytes=6, stderr_bytes=0, last_output_unix=now)
    return queue, key, lease, now


def _row(queue, key, lease):
    queue.lease_path(key).write_text(json.dumps(lease))
    return next(row for row in pbstatus.read_pool(queue.root)['jobs'] if row['action_key'] == key)


def test_status_shows_lease_and_output_ages_separately(tmp_path, monkeypatch):
    queue, key, lease, now = _observed_claim(tmp_path, monkeypatch)
    lease['heartbeat_unix'] = now
    row = _row(queue, key, lease)
    observation = row.get('execution_observation')
    assert observation and observation['state'] == 'fresh'
    assert observation['launcher_alive'] is True
    assert observation['last_output_age_s'] == 0
    rendered = '\n'.join(pbstatus.pool_job_lines([row], {'empty': False}))
    assert 'LEASE' in rendered and 'OUTPUT' in rendered and 'launcher running' in rendered


@pytest.mark.parametrize('bad', ['old', 'future', 'nan', 'boolean', 'owner', 'generation',
                                 'claim', 'host', 'negative-count', 'future-output', 'overflow'])
def test_new_heartbeat_cannot_freshen_stale_or_foreign_execution_evidence(tmp_path, monkeypatch, bad):
    queue, key, lease, now = _observed_claim(tmp_path, monkeypatch)
    lease['heartbeat_unix'] = now
    observation = lease['execution_observation']
    if bad == 'old':
        observation['sampled_unix'] = observation['last_output_unix'] = now - pool.LEASE_TIMEOUT_S - 1
    elif bad == 'future':
        observation['sampled_unix'] = now + 10
    elif bad == 'nan':
        observation['sampled_unix'] = float('nan')
    elif bad == 'overflow':
        observation['sampled_unix'] = 10 ** 1000
    elif bad == 'boolean':
        observation['launcher_alive'] = 'true'
    elif bad == 'owner':
        lease['owner'] = 'predecessor'
    elif bad == 'generation':
        lease['published_unix'] -= 1
    elif bad == 'claim':
        lease['claimed_unix'] -= 1
    elif bad == 'host':
        lease['host'] = 'other-box'
    elif bad == 'negative-count':
        observation['stdout_bytes'] = -1
    elif bad == 'future-output':
        observation['last_output_unix'] = now + 10
    row = _row(queue, key, lease)
    observed = row.get('execution_observation')
    assert observed and observed['state'] != 'fresh'
    assert observed['launcher_alive'] is None


def test_legacy_lease_reports_missing_observation_as_unknown(tmp_path, monkeypatch):
    queue, key, lease, _ = _observed_claim(tmp_path, monkeypatch)
    del lease['execution_observation']
    row = _row(queue, key, lease)
    assert row['execution_observation']['state'] == 'unavailable'
    assert row['execution_observation']['launcher_alive'] is None


def test_status_distinguishes_silent_work_from_exited_launcher(tmp_path, monkeypatch):
    queue, key, lease, now = _observed_claim(tmp_path, monkeypatch)
    lease['heartbeat_unix'] = now
    lease['execution_observation'].update(stdout_bytes=0, last_output_unix=None)
    row = _row(queue, key, lease)
    assert row['execution_observation']['state'] == 'fresh'
    assert 'launcher running; no output observed' in row['reason']
    lease['execution_observation']['launcher_alive'] = False
    row = _row(queue, key, lease)
    assert row['execution_observation']['launcher_alive'] is False
    assert 'launcher exited; descendant liveness unknown' in row['reason']

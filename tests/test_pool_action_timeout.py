"""A submitter's sealed deadline is enforced independently of queue waiting."""
import json
import sys
from pathlib import Path

import pytest
from prismabuild import core as pb, pool
from test_pbrun_detach import _checkout, _queue, _run_pbrun


def test_pbrun_seals_the_explicit_execution_timeout(tmp_path, monkeypatch, capsys):
    work = _checkout(tmp_path)
    _queue(tmp_path)
    assert _run_pbrun(tmp_path, monkeypatch, work, '--detach', '--timeout-s', '0.25') == 0
    key = json.loads(capsys.readouterr().out)['action_key']
    action = json.loads((tmp_path / 'cas' / 'requests' / key[:2] / f'{key}.json').read_text())
    assert action['params']['execution_timeout_s'] == 0.25


@pytest.mark.parametrize('value', ['0', '-1', 'nan', 'inf'])
def test_invalid_submission_deadline_is_refused(tmp_path, monkeypatch, value):
    with pytest.raises(SystemExit, match='positive finite'):
        _run_pbrun(tmp_path, monkeypatch, _checkout(tmp_path), '--detach', '--timeout-s', value)


def _claimed(tmp_path, timeout):
    checkout = tmp_path / 'checkout'
    checkout.mkdir()
    script = checkout / 'task.py'
    script.write_text('import time\ntime.sleep(1.5)\nopen("result", "w").write("ok")\n')
    action = pb.seal_action({
        'schema': pb.ACTION_SCHEMA_V2,
        'task': {'definition_id': 'tests/deadline', 'definition_version': 'v1',
                 'task_class': 'generation', 'determinism': 'deterministic',
                 'artifact_family': 'generic', 'artifact_kind': 'generic',
                 'argv': [sys.executable, 'task.py'], 'working_directory': '.', 'result_path': 'result'},
        'inputs': [], 'code_closure': pb.build_code_closure(checkout, ['task.py']),
        'params': {} if timeout is None else {'execution_timeout_s': timeout},
        'environment': {'variables': {}, 'toolchain': {}},
        'execution_scope': {'portability': 'portable', 'platform_key': None, 'host_class': None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / 'cas')
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.publish(action_key=action['action_key'], cas_root=cas.root,
                  checkout_root=checkout,
                  worker_script=Path(__file__).resolve().parents[1] / 'tools' / 'prismabuild_worker.py')
    return queue, queue.claim()


@pytest.mark.parametrize('requested,ceiling', [(0.3, 10), (10, 0.3), (None, 0.3), (0.3, None)])
def test_shorter_execution_deadline_wins_without_waiting_for_heartbeat(tmp_path, requested, ceiling):
    queue, item = _claimed(tmp_path, requested)
    outcome = queue.execute(item, timeout_s=ceiling, heartbeat_s=30, timeout_grace_s=0.2)
    assert outcome['status'] == 'timeout'
    assert outcome['returncode'] is None


@pytest.mark.parametrize('invalid', [0, -1, '1', True])
def test_worker_refuses_invalid_sealed_deadline(tmp_path, invalid):
    queue, item = _claimed(tmp_path, invalid)
    with pytest.raises(pool.PoolContractError, match='positive finite'):
        queue.execute(item)


def test_timeout_cannot_be_changed_without_resealing(tmp_path):
    queue, item = _claimed(tmp_path, 0.3)
    key = item['action_key']
    request = tmp_path / 'cas' / 'requests' / key[:2] / f'{key}.json'
    request.chmod(0o644)
    action = json.loads(request.read_text())
    action['params']['execution_timeout_s'] = 60
    request.write_text(json.dumps(action))
    with pytest.raises(pb.ActionContractError):
        queue.execute(item)


def test_old_queue_timestamp_does_not_consume_execution_budget(tmp_path):
    queue, item = _claimed(tmp_path, 5)
    item['published_unix'] = 1
    item['claimed_unix'] = 1
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, item['action_key']), item)
    # Age the same ownership generation in both records; a contradictory lease
    # tests stale-owner refusal instead of execution-budget accounting.
    lease_path = queue.lease_path(item['action_key'])
    lease = json.loads(lease_path.read_text())
    lease.update(published_unix=1, claimed_unix=1)
    pool._write_json_atomic(lease_path, lease)
    outcome = queue.execute(item, heartbeat_s=30)
    assert outcome['status'] == 'executed'
    assert outcome['returncode'] == 0


@pytest.mark.parametrize('stage', ['withdrawal', 'scope', 'status_cleanup'])
def test_prelaunch_delay_does_not_consume_execution_budget(tmp_path, monkeypatch, stage):
    """Preparation can stall before a launcher exists; its budget stays intact."""
    import time
    from types import SimpleNamespace

    queue, item = _claimed(tmp_path, 5)
    offset = [0.0]
    monkeypatch.setattr(pool, 'time', SimpleNamespace(
        monotonic=lambda: time.monotonic() + offset[0],
        time=time.time, sleep=time.sleep))
    delayed = []

    def delay():
        delayed.append(stage)
        offset[0] += 10

    if stage == 'withdrawal':
        original = queue.withdrawal_covers

        def withdrawal(item):
            if not delayed:
                delay()
            return original(item)

        monkeypatch.setattr(queue, 'withdrawal_covers', withdrawal)
    elif stage == 'scope':
        def start_scope(item):
            delay()
            return None

        monkeypatch.setattr(queue, '_start_resource_scope', start_scope)
    else:
        original = Path.unlink
        status_path = queue.action_status_path(item['action_key'])

        def unlink(path, *args, **kwargs):
            if path == status_path and not delayed:
                delay()
            return original(path, *args, **kwargs)

        monkeypatch.setattr(Path, 'unlink', unlink)

    outcome = queue.execute(item, containment=(stage == 'scope'),
                            heartbeat_s=30, timeout_grace_s=0.2)
    assert delayed == [stage]
    assert outcome['status'] == 'executed'
    assert outcome['returncode'] == 0
    cas = pb.PrismaBuildCAS(item['cas_root'])
    key = item['action_key']
    action = json.loads((cas.root / 'requests' / key[:2] / f'{key}.json').read_text())
    receipt = cas.lookup(action)
    assert receipt is not None
    assert cas.result_path(receipt, action).read_text() == 'ok'


def test_postlaunch_delay_still_consumes_execution_budget(tmp_path, monkeypatch):
    """Time spent waiting for the payload still consumes the sealed budget."""
    import time
    from types import SimpleNamespace

    queue, item = _claimed(tmp_path, 5)
    offset = [0.0]
    monkeypatch.setattr(pool, 'time', SimpleNamespace(
        monotonic=lambda: time.monotonic() + offset[0],
        time=time.time, sleep=time.sleep))
    # Keep subprocess's own clock real; inject delay only into the pool's
    # view of a real timed-out wait. Checkpoint I/O is tested separately.
    import subprocess

    class DelayedWait(subprocess.Popen):
        def communicate(self, *args, **kwargs):
            try:
                return super().communicate(*args, **kwargs)
            except subprocess.TimeoutExpired:
                offset[0] += 10
                raise

    monkeypatch.setattr(pool, 'subprocess', SimpleNamespace(
        Popen=DelayedWait, PIPE=subprocess.PIPE,
        TimeoutExpired=subprocess.TimeoutExpired))
    outcome = queue.execute(item, heartbeat_s=0.05, timeout_grace_s=0.2)
    assert outcome['status'] == 'timeout'
    assert outcome['returncode'] is None

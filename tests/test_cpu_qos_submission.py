"""QoS is explicit publication policy; cache identity and timing shards stay stable."""
import json

import pytest

import pbrun
from prismabuild import contracts, pool, resource_scope, submission
from prismabuild.submission_types import SubmissionRequest
from test_pbrun_detach import _checkout, _queue, _run_pbrun, _one_json_line


def test_opportunistic_and_protected_share_identity_but_preserve_original_policy(tmp_path, monkeypatch, capsys):
    work, queue = _checkout(tmp_path), _queue(tmp_path)
    assert _run_pbrun(tmp_path, monkeypatch, work, '--detach', '--priority', '-10',
                      '--cpu-qos', 'opportunistic') == 0
    first = _one_json_line(capsys.readouterr())
    assert _run_pbrun(tmp_path, monkeypatch, work, '--detach', '--priority', '-10') == 0
    second = _one_json_line(capsys.readouterr())
    assert first['action_key'] == second['action_key'] and second['status'] == 'attached'
    ready = json.loads(queue.records.item_path(pool.record_store.READY, first['action_key']).read_text())
    assert ready['cpu_qos'] == 'opportunistic' and contracts.CPU_QOS_TAG in ready['tags']
    assert 'cpu_qos' not in json.loads((tmp_path / 'cas' / 'requests' / first['action_key'][:2]
                                      / (first['action_key'] + '.json')).read_text())['params']


def test_default_negative_priority_has_no_opportunistic_declaration(tmp_path, monkeypatch, capsys):
    work, queue = _checkout(tmp_path), _queue(tmp_path)
    assert _run_pbrun(tmp_path, monkeypatch, work, '--detach', '--priority', '-10') == 0
    key = _one_json_line(capsys.readouterr())['action_key']
    ready = json.loads(queue.records.item_path(pool.record_store.READY, key).read_text())
    assert 'cpu_qos' not in ready and contracts.CPU_QOS_TAG not in ready['tags']


def test_qos_requires_a_compatible_worker_and_pool_transport():
    assert contracts.CPU_QOS_TAG not in contracts.offered_contract_tags(contracts.WorkerFacts())
    assert contracts.CPU_QOS_TAG in contracts.offered_contract_tags(contracts.WorkerFacts(adaptive_cpu=True))
    with pytest.raises(submission.SubmissionRefused, match='pool transport'):
        submission.validate_request(SubmissionRequest(cpu_qos='opportunistic', transport='slurm'))


@pytest.mark.parametrize('weight', [10, 100])
def test_scope_weight_survives_control_and_recovery(tmp_path, monkeypatch, weight):
    key, nonce = 'a' * 64, 'b' * 32
    scope = resource_scope.ResourceScope(key, nonce, 1024, tmp_path / 'telemetry', cpu_weight=weight)
    from prismabuild.produced_output import _broker_scope_id
    unit = _broker_scope_id(key, nonce)
    response = dict(ok=True, scope_id=unit, token='c' * 64,
                    cgroup_path='/sys/fs/cgroup/prismabuild.slice/' + unit, cpu_weight=weight)
    calls = []
    def request(op, **extra):
        calls.append((op, extra))
        return response
    monkeypatch.setattr(scope, '_request', request)
    control = scope.create()
    assert control['cpu_weight'] == weight
    record = {'cpu_weight': weight, 'resource_scope': control,
              'resource_scope_intent': {'cpu_weight': weight}}
    assert resource_scope.scope_cpu_weight(record, control) == weight
    if weight == 100:
        assert 'cpu_weight' not in calls[0][1]
    else:
        assert calls[0][1]['cpu_weight'] == 10
    record['resource_scope_intent']['cpu_weight'] = 10 if weight == 100 else 100
    with pytest.raises(ValueError, match='CPU weight differ'):
        resource_scope.scope_cpu_weight(record, control)


@pytest.mark.parametrize('ack', [None, 100, True, '10'])
def test_opportunistic_scope_never_accepts_missing_or_wrong_weight(tmp_path, ack):
    from prismabuild.produced_output import _broker_scope_id
    key, nonce = 'a' * 64, 'b' * 32
    scope = resource_scope.ResourceScope(key, nonce, 1024, tmp_path / 'telemetry', cpu_weight=10)
    unit = _broker_scope_id(key, nonce)
    response = dict(scope_id=unit, token='c' * 64,
                    cgroup_path='/sys/fs/cgroup/prismabuild.slice/' + unit)
    if ack is not None:
        response['cpu_weight'] = ack
    with pytest.raises(OSError, match='CPU weight'):
        scope._adopt_created_scope(response)

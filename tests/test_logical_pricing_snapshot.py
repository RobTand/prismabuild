"""A logical freeze prices sibling movements from one receipt observation."""
from copy import deepcopy

import test_logical_task_data_manifests as logical
import test_pbrun_residency_stage_submission as staging
from prismabuild import pool, residency_plan
import pbrun


def test_one_receipt_observation_prices_all_children_and_replay_reads_none(tmp_path, monkeypatch):
    _, request = logical.setup_request(tmp_path, monkeypatch)
    observed = []
    real = pool.PoolQueue.move_records

    def census(queue, **kwargs):
        observed.append(queue.root)
        return real(queue, **kwargs)

    monkeypatch.setattr(pool.PoolQueue, 'move_records', census)
    first, group = logical.decompose(request)
    assert len(first) == 4
    assert len(observed) == 1
    replay, recovered = logical.decompose(request)
    assert [row['action_key'] for row in replay] == [row['action_key'] for row in first]
    assert recovered['plan'] == group['plan']
    assert len(observed) == 1


def test_new_freeze_observes_fresh_receipts_and_no_read_children_observe_none(tmp_path, monkeypatch):
    _, request = logical.setup_request(tmp_path, monkeypatch)
    observed = []
    monkeypatch.setattr(pool.PoolQueue, 'move_records', lambda queue, **_kwargs: observed.append(queue.root) or [])
    logical.decompose(request)
    assert len(observed) == 1
    changed = deepcopy(request)
    changed['task_data_manifest']['mover_readers'] = 2
    logical.decompose(changed)
    assert len(observed) == 2
    empty = deepcopy(request)
    for task in empty['roster']['tasks']:
        task['payload']['reads'] = []
    logical.decompose(empty)
    assert len(observed) == 2


def test_receipt_arriving_during_freeze_does_not_reprice_later_siblings(tmp_path, monkeypatch):
    fixture, request = logical.setup_request(tmp_path, monkeypatch)
    queue = fixture['queue']
    staging._price_receipt(queue, 'a' * 64, delivered_mb_s=20)
    original = pbrun.residency_stage_rows
    calls = []

    def advancing_history(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append(result)
        if len(calls) == 1:
            staging._price_receipt(queue, 'b' * 64, delivered_mb_s=300)
        return result

    monkeypatch.setattr(pbrun, 'residency_stage_rows', advancing_history)
    _, group = logical.decompose(request)
    demands = [residency_plan.read(queue, child['action_key'])['phases'][0]
               ['mover_row']['resources'][staging.FILL_KIND] for child in group['children']]
    assert demands == [20] * 4

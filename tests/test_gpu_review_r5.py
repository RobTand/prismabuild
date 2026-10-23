"""CPU-only regressions for the two surviving r4 mutants and reader failures."""
import ast
import gc
import json
import weakref

import pytest

from gpu_evidence_fixtures import frozen_gpu
from prismabuild import census, core as pb, endings, pool
from test_gpu_review_r4 import (e2e, _finish_and_read, _lifetime, _replace_function,
                                _run_core)
from test_resource_broker import authority  # noqa: F401
from test_pbstatus import pbstatus


def _contradictory_attempt(state):
    frozen = frozen_gpu({'gpu_uuids': ['GPU-real'] if state == 'used' else [],
                         'gpu_evidence': {'sample_failed': state == 'unobserved',
                                          'live_sample': True, 'final_succeeded': True}})
    projected = pb.gpu_execution_judgment(frozen).as_dict()
    projected.update(gpu_use='used' if state == 'zero' else 'zero', gpu_use_reason='stored lie')
    return {'status': 'failed', 'disposition': 'failed' if state == 'zero' else 'requeued',
            'attempt': 1, 'max_attempts': 2, 'finished_unix': 1.0, 'finished_host': 'fixture',
            'detail': {'gpu_use': projected['gpu_use'], 'gpu_use_reason': 'stored lie',
                       'termination_reason': 'other' if state == 'zero' else 'zero_gpu_devices_used',
                       'resource_telemetry': projected}}


def _assert_frozen_resolution(resolve, state):
    result = resolve([_contradictory_attempt(state)], invalid=pool.PoolContractError,
                     validate_output_retry_stop=lambda *a: None)
    assert result['disposition'] == ('failed' if state == 'zero' else 'requeued')
    assert result['detail']['gpu_use'] == state
    assert result['detail']['resource_telemetry']['gpu_use'] == state
    assert result['detail']['gpu_use_reason'] != 'stored lie'


@pytest.mark.parametrize('state', ['used', 'zero', 'unobserved'])
def test_endings_routes_and_projects_only_the_frozen_judgment(state):
    _assert_frozen_resolution(endings.resolve_attempt, state)


def test_endings_top_level_gpu_projection_without_evidence_is_unobserved(monkeypatch):
    attempt = _contradictory_attempt('used')
    del attempt['detail']['resource_telemetry']
    resolve = lambda: endings.resolve_attempt([attempt], invalid=pool.PoolContractError,
                                              validate_output_retry_stop=lambda *a: None)
    assert resolve()['detail']['gpu_use'] == 'unobserved'
    monkeypatch.setattr(pb, 'gpu_execution_judgment', lambda *a, **k: {'gpu_use': 'used'})
    with pytest.raises(pb.ActionContractError, match='GpuUseJudgment'):
        resolve()


@pytest.mark.parametrize('mutant', ['keeps_stored_projection', 'routes_on_stored_reason'])
def test_review_r4_endings_mutants_change_behavior_and_are_caught(mutant):
    def change(source):
        tree = ast.parse(source)
        changed = []
        for node in ast.walk(tree):
            if (mutant == 'keeps_stored_projection' and isinstance(node, ast.Expr)
                    and isinstance(node.value, ast.Call)
                    and isinstance(node.value.func, ast.Attribute)
                    and isinstance(node.value.func.value, ast.Name)
                    and node.value.func.value.id == 'detail' and node.value.func.attr == 'update'
                    and any(k.arg == 'resource_telemetry' for k in node.value.keywords)):
                node.value = ast.Constant(None)
                changed.append(node)
            if (mutant == 'routes_on_stored_reason' and isinstance(node, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == 'gpu_refusal_final' for t in node.targets)):
                node.value = ast.parse('raw_detail.get("termination_reason") == "zero_gpu_devices_used"',
                                       mode='eval').body
                changed.append(node)
        assert len(changed) == 1
        return ast.unparse(ast.fix_missing_locations(tree))
    _assert_frozen_resolution(endings.resolve_attempt, 'used')
    changed = _replace_function(endings, 'resolve_attempt', change)
    with pytest.raises((AssertionError, pool.PoolContractError)):
        _assert_frozen_resolution(changed, 'used')


@pytest.mark.parametrize('kept', [False, True])
@pytest.mark.parametrize('malformed', ['unknown_key', 'oversized_deadline'])
def test_malformed_gpu_row_does_not_hide_other_endings_or_break_views(e2e, kept, malformed):
    _lifetime(e2e, ['used'])
    _finish_and_read(e2e, _run_core(e2e, 'deliver'))
    key = e2e.action['action_key']
    path = e2e.queue.records.item_path(pool.record_store.DONE, key)
    good = json.loads(path.read_text())
    other = {**good, 'action_key': 'b' * 64}
    (path.parent / ('b' * 64 + '.json')).write_text(json.dumps(other))
    broken = good['detail']['resource_telemetry']['gpu_record']
    if malformed == 'unknown_key':
        broken['resource_telemetry']['gpu_evidence']['added_field'] = True
    else:
        broken['deadline_monotonic'] = 10 ** 1000
    path.write_text(json.dumps(good))
    reader = census.KeptReads() if kept else None
    if reader:
        reader.begin()
    try:
        rows = census.endings(e2e.queue.root, reader=reader)
    finally:
        if reader:
            reader.end()
    by_key = {row['action_key']: row for row in rows}
    assert by_key[key]['status'] == 'unreadable'
    assert 'GPU' in by_key[key]['unreadable']
    assert by_key['b' * 64]['gpu_use'] == 'used'
    table = '\n'.join(pbstatus.ending_lines(rows))
    assert 'unreadable' in table and 'bbbbbbbbbbbb' in table
    # pbstatus may also receive a row directly, without the census boundary.
    direct = {**by_key['b' * 64], 'gpu_record': good['detail']['resource_telemetry']['gpu_record']}
    table = '\n'.join(pbstatus.ending_lines([direct, by_key['b' * 64]]))
    assert 'invalid GPU evidence' in table and 'used' in table


def test_historical_cache_note_is_visible_in_census_and_pbstatus(e2e):
    result = pb.run_local_action(e2e.action, cas_root=e2e.cas.root, checkout_root=e2e.checkout)
    path = e2e.queue.finish(e2e.item['action_key'], status='cache_hit',
                           detail={'returncode': 0, 'stdout': json.dumps(result) + '\n'},
                           claim_snapshot=e2e.item)
    row, = census.endings(e2e.queue.root)
    assert row['gpu_use_reason'] == 'final_observation_failed'
    assert row['gpu_cache_note'] == 'receipt_predates_gpu_observation'
    table = '\n'.join(pbstatus.ending_lines([row]))
    assert 'receipt_predates_gpu_observation' in table
    assert 'unobserved' in table
    assert path.exists()


def test_issued_capability_does_not_expose_minting_authority():
    judgment = pb.gpu_execution_judgment(None)
    assert not hasattr(judgment, '_authority')
    with pytest.raises(pb.ActionContractError):
        type(judgment)({'gpu_use': 'used'}, _authority=getattr(judgment, '_authority', None))
    forged = object.__new__(type(judgment))
    object.__setattr__(forged, '_encoded', pb._canonical_bytes(judgment.as_dict()))
    with pytest.raises(pb.ActionContractError):
        pb.GpuUseJudgment.require(forged)
    reference = weakref.ref(judgment)
    del judgment
    gc.collect()
    assert reference() is None

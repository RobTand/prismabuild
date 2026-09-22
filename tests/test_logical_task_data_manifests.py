"""Logical task subset manifests use the ordinary PB staging protocol."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

import test_pbrun_residency_stage_submission as staging
import test_a_decomposed_campaign_closes_on_an_exact_cover as closing
from prismabuild import core as pb, decomposition as dc, pool, residency_plan
import pbrun
import pbcampaign


def setup_request(tmp_path, monkeypatch, *, cached_first=False):
    fixture = staging._prepare(tmp_path, monkeypatch)
    request = closing._request(fixture['work'])
    request['roster']['tasks'] = request['roster']['tasks'][:4]
    request['batch_policy']['residencies'][0]['setup_seconds'] = 0
    request['batch_policy']['max_setup_fraction'] = 1
    request['batch_policy']['max_estimated_wall_seconds'] = 2
    origin = tmp_path / 'origins'
    origin.mkdir()
    for index, task in enumerate(request['roster']['tasks']):
        task['estimated_seconds'] = 1
        task['payload']['reads'] = []
        if cached_first and index == 0:
            continue
        for kind in ('wire', 'render'):
            path = origin / f'{index}-{kind}.bin'
            raw = f'{index}-{kind}'.encode()
            path.write_bytes(raw)
            task['payload']['reads'].append({'path': path.name, 'offset': 0,
                'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest() if kind == 'wire' else None})
    request['task_data_manifest'] = {'schema': 'prismabuild.task_data_manifest.v1',
        'payload_field': 'reads', 'mount_prefix': str(origin),
        'residency_tier': staging.TIER, 'residency_ram': 'off',
        'mover_readers': 1, 'mover_mem_gb': 1}
    return fixture, dc.validate_logical_request(request)


def decompose(request):
    return pbcampaign.decompose(request, transport='pool', priority=-10)


def test_children_have_only_their_declared_reads_and_all_graphs_freeze_first(tmp_path, monkeypatch):
    fixture, request = setup_request(tmp_path, monkeypatch, cached_first=True)
    original = pbcampaign.child_record
    observations = []
    def publish(child, **kwargs):
        logical = child['params'][dc.LOGICAL_BATCH_PARAM]
        path = pbcampaign.decomposition_dir(kwargs['cas'], logical['parent_key']) / 'data-plans.json'
        body = json.loads(path.read_text())
        assert len(body['children']) == 4  # ALL graphs already durable
        observations.append(child['action_key'])
        return original(child, **kwargs)
    monkeypatch.setattr(pbcampaign, 'child_record', publish)
    records, group = decompose(request)
    assert len(records) == 4 and {r['status'] for r in records} == {'submitted'}
    assert observations == [c['action_key'] for c in group['children']]
    cas = pb.PrismaBuildCAS(tmp_path / 'cas')
    all_reads = []
    for child in group['children']:
        ids = child['params'][dc.LOGICAL_BATCH_PARAM]['ordered_task_ids']
        wanted = [t for t in request['roster']['tasks'] if t['id'] in ids]
        expected = [{**entry, 'path': request['task_data_manifest']['mount_prefix'] + '/' + entry['path']}
                    for t in wanted for entry in t['payload']['reads']]
        manifest_ref = child['params'].get('data_manifest')
        row = pool._read_json(fixture['queue'].item_path(pool.READY, child['action_key']))
        if not expected:
            assert manifest_ref is None and 'residency' not in row
            continue
        manifest, _ = pb.read_data_manifest(cas.input_path(manifest_ref['input']))
        assert manifest['entries'] == expected
        all_reads.extend(manifest['entries'])
        plan = residency_plan.read(fixture['queue'], child['action_key'])
        assert plan['manifest_sha256'] == manifest_ref['input']['sha256']
        assert row['residency']['leads'] == residency_plan.leads_for(plan)
        assert all(not fixture['queue'].item_path(pool.READY, key).exists()
                   for key in residency_plan.mover_keys(plan))
    assert len(all_reads) == 6


def test_replay_reuses_membership_and_graphs_without_replanning(tmp_path, monkeypatch):
    _, request = setup_request(tmp_path, monkeypatch)
    first, initial = decompose(request)
    def unexpected(*args, **kwargs):
        raise AssertionError('replay rebuilt an immutable graph')
    monkeypatch.setattr(pbrun, 'residency_stage_rows', unexpected)
    second, replay = decompose(request)
    assert [r['action_key'] for r in first] == [r['action_key'] for r in second]
    assert replay['plan'] == initial['plan']
    assert {r['status'] for r in second} == {'attached'}


def test_projection_policy_is_part_of_parent_identity(tmp_path, monkeypatch):
    _, request = setup_request(tmp_path, monkeypatch)
    _, first = decompose(request)
    changed = deepcopy(request)
    changed['task_data_manifest']['mover_readers'] = 2
    _, second = decompose(changed)
    assert first['plan']['parent_key'] != second['plan']['parent_key']
    assert first['plan']['partitions'] == second['plan']['partitions']


def test_explicit_empty_reads_support_receipt_only_children(tmp_path, monkeypatch):
    _, request = setup_request(tmp_path, monkeypatch)
    for task in request['roster']['tasks']:
        task['payload']['reads'] = []
    records, group = decompose(dc.validate_logical_request(request))
    assert len(records) == 4
    assert all('data_manifest' not in child['params'] for child in group['children'])


@pytest.mark.parametrize('damage', ['missing-reads', 'path-escape', 'unknown-policy', 'shared-manifest'])
def test_invalid_projection_refuses_before_any_queue_publication(tmp_path, monkeypatch, damage):
    fixture, request = setup_request(tmp_path, monkeypatch)
    if damage == 'missing-reads':
        del request['roster']['tasks'][-1]['payload']['reads']
    elif damage == 'path-escape':
        request['roster']['tasks'][-1]['payload']['reads'][0]['path'] = '../outside'
    elif damage == 'unknown-policy':
        request['task_data_manifest']['worker_count'] = 9
    else:
        request['common']['data_manifest'] = '/shared/all-cells.json'
    with pytest.raises(pb.ActionContractError):
        dc.validate_logical_request(request)
    assert not fixture['queue'].ready_items()


def test_manifest_deduplicates_identical_reads_but_refuses_conflicts(tmp_path, monkeypatch):
    _, request = setup_request(tmp_path, monkeypatch)
    tasks = request['roster']['tasks']
    tasks[1]['payload']['reads'] = deepcopy(tasks[0]['payload']['reads'])
    manifest = dc.task_data_manifest(request['task_data_manifest'], {'tasks': tasks[:2]})
    assert manifest['entry_count'] == 2
    tasks[1]['payload']['reads'][0]['bytes'] += 1
    with pytest.raises(pb.ActionContractError, match='conflicting'):
        dc.task_data_manifest(request['task_data_manifest'], {'tasks': tasks[:2]})


def test_corrupt_frozen_graph_binding_refuses_before_republishing(tmp_path, monkeypatch):
    fixture, request = setup_request(tmp_path, monkeypatch)
    records, group = decompose(request)
    path = pbcampaign.decomposition_dir(pb.PrismaBuildCAS(tmp_path / 'cas'), group['plan']['parent_key']) / 'data-plans.json'
    body = json.loads(path.read_text())
    body['children'][0]['action_key'] = '0' * 64
    path.chmod(0o600)  # Deliberately corrupt this private immutable fixture.
    path.write_text(json.dumps(body))
    before = {item['action_key'] for item in fixture['queue'].ready_items()}
    with pytest.raises(pbcampaign.ManifestError, match='identity differs'):
        decompose(request)
    assert {item['action_key'] for item in fixture['queue'].ready_items()} == before


def test_prepared_membership_reuses_one_roster_hash_and_preserves_wire_bytes(tmp_path, monkeypatch):
    _, request = setup_request(tmp_path, monkeypatch)
    _, group = decompose(request)
    original = dc.canonical_sha256
    seen = []
    def counted(value):
        if value is request['roster']:
            seen.append('roster')
        return original(value)
    monkeypatch.setattr(dc, 'canonical_sha256', counted)
    prepared = dc.PreparedBatches(request, group['plan'])
    for ordinal, task_ids in enumerate(group['plan']['partitions']):
        expected = {'schema': dc.LOGICAL_BATCH_SCHEMA_V1,
            'parent_key': group['plan']['parent_key'], 'plan_key': group['plan']['plan_key'],
            'roster_sha256': original(request['roster']),
            'batch_policy_sha256': original(request['batch_policy']),
            'child_ordinal': ordinal, 'ordered_task_ids': task_ids}
        assert dc.document_bytes(prepared.membership(ordinal)) == dc.document_bytes(expected)
        assert [task['id'] for task in prepared.envelope(ordinal)['tasks']] == task_ids
    assert seen == ['roster']

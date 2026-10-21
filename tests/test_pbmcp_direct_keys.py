"""Queue namespace bounds and opt-in status samples for #1897."""
import os
import json
import time
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools' / 'fleet'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import pbmcp
import pbmcp_fixture as fx
from prismabuild import census, pool


def test_full_keys_never_enumerate_queue_namespaces(tmp_path, monkeypatch):
    fleet = fx.build(tmp_path)
    session = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link)
    native = os.scandir
    native_listdir = os.listdir
    forbidden = {fleet.queue_root / state for state in pbmcp.STATES}
    forbidden.update({fleet.queue_root / 'attempts',
                      fleet.queue_root / 'withdrawn' / 'decisions'})

    def scandir(path):
        if not isinstance(path, int):
            assert Path(path) not in forbidden, f'enumerated {path}'
        return native(path)

    def listdir(path):
        if not isinstance(path, int):
            assert Path(path) not in forbidden, f'enumerated {path}'
        return native_listdir(path)

    monkeypatch.setattr(os, 'scandir', scandir)
    monkeypatch.setattr(os, 'listdir', listdir)
    action = session.call('pb_action', {'key_prefix': fx.DONE_KEY.upper()})
    assert action['complete'] and action['found'] and action['state'] == 'done'
    keys = [fx.READY_KEY, fx.CLAIMED_KEY, fx.DONE_KEY]
    actions = session.call('pb_actions', {'keys': keys})
    assert actions['complete']
    assert {row['action_key'] for row in actions['actions']} == set(keys)
    missing = session.call('pb_action', {'key_prefix': 'f' * 64})
    assert missing['complete'] and missing['found'] is False


def test_prefix_still_reports_ambiguity(tmp_path):
    fleet = fx.build(tmp_path)
    session = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link)
    fleet.queue.publish(action_key=fx.TWIN_KEY, cas_root=fleet.cas_root,
                        checkout_root=str(fleet.checkout),
                        worker_script=str(fleet.base / 'worker.py'))
    with pytest.raises(pbmcp.ToolError, match='names'):
        session.call('pb_action', {'key_prefix': fx.DONE_KEY[:12]})


@pytest.mark.parametrize('keyed', [False, True])
def test_state_filter_does_not_visit_other_states(tmp_path, monkeypatch, keyed):
    fleet = fx.build(tmp_path)
    native = os.scandir
    native_read = pool.record_store.inspection_document

    def scandir(path):
        if not isinstance(path, int):
            assert Path(path) not in {fleet.queue_root / state
                                     for state in ['ready', 'claimed', 'failed', 'withdrawn']}
        return native(path)

    def read(path):
        assert Path(path).parent == fleet.queue_root / 'done'
        return native_read(path)

    monkeypatch.setattr(os, 'scandir', scandir)
    monkeypatch.setattr(pool.record_store, 'inspection_document', read)
    scan = pbmcp._scan_actions(fleet.queue_root, states=['done'],
                               keys=[fx.DONE_KEY] if keyed else [], limit=1)
    assert [row['action_key'] for row in scan['rows']] == [fx.DONE_KEY]


def test_status_sample_records_require_detail(tmp_path):
    fleet = fx.build(tmp_path)
    fleet.queue.announce(host='fixture-box', tags=['x86'], has_gpu=False,
                         capacity={'cpu': 8, 'mem_gb': 16})
    sample = fleet.queue_root / pool.RESERVATIONS / 'fixture-box' / 'adaptive' / 'cpu-sample.json'
    sample.parent.mkdir(parents=True, exist_ok=True)
    sample.write_text(json.dumps({'sampled_unix': time.time(),
                                   'control_threads': [{'pid': 123}]}))
    evidence = {'decision': {'reason': 'busy', 'fresh': True, 'sample': {'busy_cpus': 8}},
                'cpu_decision': {'sample': {'busy_cpus': 8}},
                'gpu_decision': {'sample': {'busy': True}}}
    denial = {'action_key': fx.READY_KEY, 'published_unix': fleet.record('ready', fx.READY_KEY)['published_unix'],
              'host': 'fixture-box', 'reason': 'adaptive_cpu_refused',
              'evidence': evidence, 'denied_unix': time.time()}
    (sample.parent / 'claim-denials.json').write_text(json.dumps({
        'schema': pool.CLAIM_DENIALS_SCHEMA_V1, 'records': {'one': denial}}))
    claimed = fleet.record('claimed', fx.CLAIMED_KEY)
    claimed['gpu_admission'] = {'reason': 'admitted', 'sample': {'busy': False}}
    fleet.queue.records.item_path('claimed', fx.CLAIMED_KEY).write_text(json.dumps(claimed))
    session = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link)
    compact = session.call('pb_status')
    detail = session.call('pb_status', {'detail': True})
    assert compact['complete'] and detail['complete']
    detailed_samples = [sample for node in detail['nodes']
                        for sample in node['admission'].values()]
    assert any('record' in sample for sample in detailed_samples)
    assert all('record' not in sample for node in compact['nodes']
               for sample in node['admission'].values())
    for small, full in zip(compact['nodes'], detail['nodes']):
        for kind, sample in small['admission'].items():
            assert sample['state'] == full['admission'][kind]['state']
    small_jobs = {job['action_key']: job for job in compact['jobs']}
    full_jobs = {job['action_key']: job for job in detail['jobs']}
    small_denial = small_jobs[fx.READY_KEY]['admission_denials'][0]
    full_denial = full_jobs[fx.READY_KEY]['admission_denials'][0]
    assert small_denial['reason'] == full_denial['reason'] == 'adaptive_cpu_refused'
    assert small_denial['evidence']['decision'] == {'reason': 'busy', 'fresh': True}
    assert 'sample' not in json.dumps(small_denial['evidence'])
    assert full_denial['evidence'] == evidence
    assert small_jobs[fx.CLAIMED_KEY]['gpu_admission'] == {'reason': 'admitted'}
    assert 'sample' in full_jobs[fx.CLAIMED_KEY]['gpu_admission']
    # Other census consumers retain their existing detailed contract.
    assert any('record' in sample for node in census.action_rows(fleet.queue_root).value['nodes']
               for sample in node['admission'].values())

"""Bootstrap policy and fixed root-helper boundaries, without root operations."""
import ast
from copy import deepcopy
import importlib.util
from pathlib import Path
import time

import pytest
from bootstrap_fixture import ROOT, bootstrap, assess_case
from prismabuild import compatibility_window as window
from prismabuild import installed_bootstrap as collector
from prismabuild.digest_primitives import canonical_sha256

spec = importlib.util.spec_from_file_location('bootstrap_client', ROOT / 'tools/fleet/bootstrap_client.py')
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)


def test_final_installed_closure_has_separate_operator_restoration_mode(bootstrap):
    result = assess_case(bootstrap)
    assert result['automatic_reachability'] is False
    assert result['hosts'] == ['one', 'two']


@pytest.mark.parametrize('field,value', [
    ('nonce', '0' * 64), ('observed_unix', 0), ('boot_id', 'other-boot'),
    ('captured_sha256', '0' * 64), ('executing_sha256', '0' * 64),
    ('fenced', False), ('oneshot_exited', False), ('reader_loaded', False), ('proof_loaded', False),
    ('automatic_journal', True), ('epoch', 'active'), ('enrollment_sha256', '0' * 64),
    ('plan_sha256', '0' * 64), ('host', 'unknown'), ('journal_sha256', ''),
])
def test_refuses_incomplete_or_replayed_root_responses(bootstrap, field, value):
    bootstrap[1].rows['one'][field] = value
    with pytest.raises(window.WindowError):
        assess_case(bootstrap)


@pytest.mark.parametrize('mutation', ['leaf', 'metadata', 'broker', 'loaded', 'owner', 'lock', 'host'])
def test_checks_root_custody_service_identity_gate_and_full_roster(bootstrap, mutation):
    row = bootstrap[1].rows['one']
    if mutation == 'leaf':
        row['files'].pop('digest_primitives.py')
    elif mutation == 'metadata':
        row['metadata']['park_proof.py']['uid'] = 1000
    elif mutation == 'broker':
        row['broker']['health'] = False
    elif mutation == 'loaded':
        row['broker']['installed_sha256']['gpu_memory.py'] = '0' * 64
    elif mutation == 'owner':
        row['gate']['owner'] = 'foreign'
    elif mutation == 'lock':
        row['upgrade_lock']['inode'] += 1
    else:
        bootstrap[1].rows.pop('two')
    with pytest.raises(window.WindowError):
        assess_case(bootstrap)


def test_shared_root_looking_json_is_not_authority(bootstrap):
    plan, observations, qualification, designation = bootstrap
    with pytest.raises(window.WindowError, match='shared evidence'):
        window.require_installed_bootstrap(Path(plan['source']['directory']), Path(plan['target']['directory']),
            observations.rows, roster=plan['roster'], qualification=qualification, rollback_designation=designation)


def test_collector_uses_fresh_fixed_authenticated_argv(bootstrap, monkeypatch):
    calls = []
    def run(argv, **options):
        import json
        from types import SimpleNamespace
        calls.append((argv, options))
        return SimpleNamespace(stdout=json.dumps({'nonce': argv[-1]}).encode())
    monkeypatch.setattr(collector.subprocess, 'run', run)
    first = collector.collect_responses(bootstrap[0])
    second = collector.collect_responses(bootstrap[0])
    assert first.nonce != second.nonce
    assert len(calls) == 4
    assert all('/usr/bin/sudo' in argv and '-I' in argv and options['check'] for argv, options in calls)
    assert all('shell' not in options for _, options in calls)


def test_helper_cannot_commit_service_members(tmp_path):
    with pytest.raises(ValueError, match='cannot commit'):
        helper.commit_member(tmp_path, 'resource_broker.py')


def test_helper_reuses_full_map_copy_only_in_private_stage():
    tree = ast.parse((ROOT / 'tools/fleet/bootstrap_client.py').read_text())
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    assert not any(isinstance(node.func, ast.Attribute) and node.func.attr == 'run'
                   and isinstance(node.func.value, ast.Name) and node.func.value.id == 'updater' for node in calls)
    assert helper.REPLACE == ('digest_primitives.py', 'generation_receipt.py', 'park_proof.py', 'upgrade_client.py')
    assert helper.UPGRADE_UNITS == ('prismabuild-client-upgrade.timer', 'prismabuild-client-upgrade.service')


def test_persistent_fence_preserves_locally_defined_units(tmp_path, monkeypatch):
    monkeypatch.setattr(helper, 'STATE', tmp_path / 'state')
    monkeypatch.setattr(helper, 'UNITS', tmp_path / 'units')
    plan = {'id': 'fixed'}
    block, paths = helper.fence_paths(plan)
    assert all(path.name == '90-prismabuild-bootstrap-fixed.conf' for path in paths)
    assert helper.fence_content(block) == ('[Unit]\nConditionPathExists=!' + str(block) + '\n').encode()
    assert all(path.parent.name.endswith('.d') for path in paths)


def test_qualification_index_never_accepts_a_verdict_without_actions(tmp_path):
    path = tmp_path / 'assessment.json'
    path.write_text('{"schema":"prismabuild.installed_bootstrap.assessment.v1","plan":{"schema":"prismabuild.client_bootstrap.plan.v1","id":"test"},"qualification_actions":[]}')
    with pytest.raises(window.WindowError, match='qualification'):
        collector.read_assessment(path)


def test_reverse_assessment_retains_leaves_without_automatic_reachability(bootstrap):
    plan, observations, _, _ = bootstrap
    plan['direction'] = 'restore'
    for row in observations.rows.values():
        row['plan_sha256'] = canonical_sha256(plan)
        row['files']['upgrade_client.py'] = plan['source_files']['upgrade_client.py']
        row['executing_sha256'] = row['captured_sha256'] = plan['source_files']['upgrade_client.py']
    assert assess_case(bootstrap)['direction'] == 'restore'
    assert assess_case(bootstrap)['automatic_reachability'] is False
    assert all(leaf in row['files'] for row in observations.rows.values() for leaf in window.BOOTSTRAP_LEAVES)


@pytest.mark.parametrize('fault', ['helper', 'paths', 'source', 'aliases', 'automatic-rollback'])
def test_bootstrap_is_bound_to_exact_source_tool_paths_and_operator_mode(bootstrap, fault):
    plan, _, _, designation = bootstrap
    if fault == 'helper':
        plan['tool_sha256'] = '0' * 64
    elif fault == 'paths':
        plan['members']['park_proof.py'] = 'src/prismabuild/other.py'
    elif fault == 'source':
        plan['source']['generation'] = 'substituted'
    elif fault == 'aliases':
        plan['roster']['two'].append('one')
    else:
        designation['automatic_reachability'] = True
    with pytest.raises(window.WindowError):
        assess_case(bootstrap)


@pytest.mark.parametrize('fault', [None, 'incomplete', 'missing-host', 'unknown-writer', 'ambiguous'])
def test_publication_rechecks_the_live_record_window(bootstrap, monkeypatch, fault):
    import json
    from types import SimpleNamespace
    plan = bootstrap[0]
    status = {'complete': True, 'nodes': [
        {'node': host, 'state': 'live', 'runtime_commit': plan['source']['commit']} for host in plan['roster']]}
    if fault == 'incomplete':
        status['complete'] = False
    elif fault == 'missing-host':
        status['nodes'].pop()
    elif fault == 'unknown-writer':
        status['nodes'][0]['runtime_commit'] = '0' * 40
    elif fault == 'ambiguous':
        other = Path(plan['target']['directory']).parent / 'duplicate'
        other.mkdir()
        (other / 'RUNTIME_VERSION.json').write_bytes((Path(plan['source']['directory']) / 'RUNTIME_VERSION.json').read_bytes())
    monkeypatch.setattr(collector.subprocess, 'run', lambda *a, **kw: SimpleNamespace(stdout=json.dumps(status).encode()))
    if fault is None:
        assert collector.require_live_window(plan, target=plan['target']['directory'])['hosts'] == ['one', 'two']
    else:
        with pytest.raises(window.WindowError):
            collector.require_live_window(plan, target=plan['target']['directory'])

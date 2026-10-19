"""Qualification population/route controls with CAS I/O replaced at its boundary."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from prismabuild import core, pool, movement_actions, installed_bootstrap as owner
from prismabuild.compatibility_window import WindowError
from prismabuild.digest_primitives import raw_sha256
spec = importlib.util.spec_from_file_location('_qualification_outcomes',
    Path(__file__).resolve().parents[1] / 'tools/fleet/pbtest_outcomes.py')
pbtest_outcomes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pbtest_outcomes)


@pytest.fixture
def qualification(tmp_path, monkeypatch):
    files = {'root': list(owner.ROOT_FILES), 'corpus': list(owner.CORPUS_FILES)}
    populations = {kind: {'files': names, 'nodes': [name + '::test_case[' + case + ']'
        for name in names for case in ('one', 'two')]} for kind, names in files.items()}
    profiles = {}
    source_bytes = {name: name.encode() for name in owner.QUALIFICATION_SOURCES}
    routes = {}
    for machine, elf in (('aarch64', 183), ('x86_64', 62)):
        primary = {'version': [3, 12, 3], 'executable': {'path': '/python', 'sha256': 'a' * 64}, 'elf_machine': elf}
        profile = {'primary': primary, 'installed_reader': primary, 'image': 'sha256:' + 'b' * 64}
        profile_path = 'qualification/' + machine + '.json'
        raw = json.dumps(profile).encode()
        source_bytes[profile_path] = raw
        profiles[machine] = profile
        routes[machine] = {'profile': profile_path, 'profile_sha256': raw_sha256(raw)}
    sources = {name: raw_sha256(raw) for name, raw in source_bytes.items()}
    index = {'schema': 'prismabuild.bootstrap_qualification_index.v1', 'target_commit': 'f' * 40,
        'populations': populations, 'source_sha256': sources, 'root_routes': routes,
        'corpus_routes': {'x86_64': {'commands': [['/python', '-m', 'pytest', *files['corpus']]]}}}
    monkeypatch.setattr(owner, '_qualification_source', lambda checkout, commit, name: source_bytes[name])
    path = tmp_path / 'index.json'
    requests, receipts, logs = {}, {}, {}
    for key, kind, machine in [('arm', 'root', 'aarch64'), ('x86', 'root', 'x86_64'), ('corpus', 'corpus', 'x86_64')]:
        profile = profiles[machine]
        primary = profile['primary']
        profile_path = routes[machine]['profile']
        profile_raw = source_bytes[profile_path]
        commands = (['/python', 'tests/run_proof_container.py', '--runtime-profile', profile_path, *files[kind]]
                    if kind == 'root' else ['/python', '-m', 'pytest', *files[kind]])
        snapshot_input = {'id': core.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID, 'sha256': 'c' * 64, 'bytes': 1}
        code = tmp_path / 'closure.py'; code.write_text('# fixture closure\n')
        requests[key] = core.seal_action({'schema': core.ACTION_SCHEMA_V2,
            'task': {'definition_id': 'fleet/pbrun', 'definition_version': 'v1',
                'task_class': 'generation', 'determinism': 'deterministic',
                'artifact_family': 'generic', 'artifact_kind': 'generic',
                'argv': movement_actions.standard_capture_argv(commands, 'result.txt', path_prefix='/usr/bin'),
                'working_directory': '.', 'result_path': 'result.txt'},
            'inputs': [snapshot_input], 'code_closure': core.build_code_closure(tmp_path, ['closure.py']),
            'params': {'command': commands, 'interpreter': '/python', 'cwd': '.',
                'checkout_snapshot': {'schema': core.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1,
                    'commit': 'e' * 40, 'input': snapshot_input, 'subdirectory': '.'}},
            'environment': {'variables': {'PATH': '/usr/bin:/bin'}, 'toolchain': {}},
            'execution_scope': {'portability': 'portable', 'platform_key': None, 'host_class': None}})
        receipts[key] = {'producer': {'evidence': {'machine': machine}}}
        nodes = populations[kind]['nodes']
        record = {'schema': pbtest_outcomes.SCHEMA, 'collected': nodes, 'deselected': [],
            'reports': [[node, 'call', 'passed', None, None] for node in nodes], 'uncounted': []}
        attestation = {'complete': True, 'mode': 'execution', 'exitstatus': 0, 'installed_reader': primary,
            'outer': primary, 'inner': primary, 'profile_sha256': raw_sha256(profile_raw),
            'source_sha256': sources, 'image': {'expected': profile['image'], 'actual': profile['image']}}
        logs[key] = 'pbtest-outcomes: ' + json.dumps(record) + '\n' + str(len(nodes)) + ' passed in 0.01s\n'
        if kind == 'root':
            logs[key] += 'prismabuild-root-runtime: ' + json.dumps(attestation) + '\n'
    class CAS:
        def __init__(self, *_): pass
        def read_action_request(self, key): return requests[key]
        def lookup(self, action): return receipts[next(k for k, v in requests.items() if v is action)]
        def input_path(self, _): return tmp_path / 'bundle'
        def result_path(self, receipt, action):
            key = next(k for k, v in requests.items() if v is action)
            target = tmp_path / (key + '.log'); target.write_text(logs[key]); return target
    gate = SimpleNamespace(resolve_action_key=lambda _, key: (key, key), closure_stamp_only=lambda *a: False)
    gate._git = lambda *a: ''
    original = importlib.util.module_from_spec
    monkeypatch.setattr(importlib.util, 'module_from_spec', lambda spec:
        gate if spec.name == '_bootstrap_shape_gate' else pbtest_outcomes if spec.name == '_bootstrap_pbtest_outcomes' else original(spec))
    # Keep the production module loading boundary; only external gate/CAS reads are replaced.
    class Loader:
        def exec_module(self, _): pass
    spec_from = importlib.util.spec_from_file_location
    monkeypatch.setattr(importlib.util, 'spec_from_file_location', lambda name, path:
        SimpleNamespace(name=name, loader=Loader()) if name.startswith('_bootstrap_') else spec_from(name, path))
    monkeypatch.setattr(core, 'PrismaBuildCAS', CAS)
    monkeypatch.setattr(pool.record_store, 'read_document', lambda _: {'status': 'executed', 'detail': {'returncode': 0}})
    assessment = {'plan': {'target': {'commit': 'f' * 40}}, 'qualification_actions': list(requests)}
    def seal():
        path.chmod(0o600) if path.exists() else None
        path.write_text(json.dumps(index)); path.chmod(0o444)
        assessment['qualification_index'] = {'path': str(path), 'sha256': raw_sha256(path.read_bytes())}
        assessment['plan']['qualification_index_sha256'] = assessment['qualification_index']['sha256']
    seal()
    return assessment, index, requests, logs, seal


@pytest.mark.parametrize('damage', ['subset', 'runner', 'profile', 'source-value', 'missing-index'])
def test_qualification_refuses_successful_subset_or_unapproved_root_route(qualification, tmp_path, damage):
    assessment, index, requests, logs, seal = qualification
    if damage == 'subset':
        lines = logs['corpus'].splitlines()
        record = json.loads(lines[0].removeprefix('pbtest-outcomes: '))
        record['collected'] = record['collected'][::2]
        record['reports'] = record['reports'][::2]
        logs['corpus'] = 'pbtest-outcomes: ' + json.dumps(record) + '\n8 passed in 0.01s\n'
    elif damage == 'runner':
        requests['arm']['params']['command'] = ['/python', '-c', 'print("attestation")']
    elif damage == 'profile':
        requests['arm']['params']['command'][3] = 'other-profile.json'
    elif damage == 'source-value':
        logs['arm'] = logs['arm'].replace(index['source_sha256'][owner.ROOT_FILES[0]], '0' * 64)
    else:
        assessment.pop('qualification_index')
    with pytest.raises(WindowError):
        owner.verify_qualification(assessment, checkout=Path(__file__).resolve().parents[1], fleet_root=tmp_path)


def test_complete_external_population_and_approved_route_pass(qualification, tmp_path):
    assessment, _, _, _, _ = qualification
    result = owner.verify_qualification(assessment, checkout=Path(__file__).resolve().parents[1], fleet_root=tmp_path)
    assert list(result.action_keys) == ['arm', 'x86', 'corpus']


@pytest.mark.parametrize('lane', ['arm', 'corpus'])
@pytest.mark.parametrize('damage', ['argv', 'cwd', 'input', 'metadata-command'])
def test_qualification_binds_executed_recipe_and_checkout(qualification, tmp_path, lane, damage):
    assessment, _, requests, _, _ = qualification
    action = requests[lane]
    if damage == 'argv':
        # Approved metadata, source pins and successful indexed outcomes survive.
        action['task']['argv'] = ['/python', '-c', 'print("forged outcomes")']
    elif damage == 'cwd':
        action['task']['working_directory'] = 'other'
    elif damage == 'input':
        action['inputs'] = []
    else:
        action['params']['command'] = ['/python', '-c', 'print("forged outcomes")']
        action['task']['argv'] = movement_actions.standard_capture_argv(
            action['params']['command'], 'result.txt', path_prefix='/usr/bin')
    requests[lane] = core.seal_action({k: v for k, v in action.items() if k != 'action_key'})
    with pytest.raises(WindowError):
        owner.verify_qualification(assessment, checkout=Path(__file__).resolve().parents[1], fleet_root=tmp_path)

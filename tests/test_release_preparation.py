"""Real recorder/prepare adapter integration; only admitted client/CAS I/O is replaced."""
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest

from prismabuild import core, movement_actions

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('prepare_release_test', REPO / 'tools/fleet/prepare_release.py')
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)


@pytest.fixture
def recorder_checkout(tmp_path):
    checkout = tmp_path / 'checkout'
    (checkout / 'tools/fleet').mkdir(parents=True)
    shutil.copyfile(REPO / 'tools/fleet/pbtest_outcomes.py', checkout / 'tools/fleet/pbtest_outcomes.py')
    (checkout / 'tests').mkdir()
    (checkout / 'tests/test_population.py').write_text(
        'import pytest\n@pytest.mark.parametrize("value", [1, 2])\n'
        'def test_population(value):\n    assert value > 0\n')
    return checkout


def recorder_command(*, callable_entry=False):
    entry = (['-c', 'import runpy; raise SystemExit(runpy.run_path('
              '"tools/fleet/pbtest_outcomes.py")["main"]())'] if callable_entry else
             ['tools/fleet/pbtest_outcomes.py'])
    return [sys.executable, *entry, '-q', '-o', 'addopts=', '-p', 'no:cacheprovider']


def recorder_run(checkout, command):
    # Already inside the outer PB action: its children execute directly.
    return subprocess.run(command, cwd=checkout, capture_output=True, text=True,
        env={**os.environ, 'PYTHONPATH': str(REPO / 'src')}, timeout=30)


@pytest.mark.parametrize('collect_only', [False, True])
def test_documented_recorder_route_runs_pytest(recorder_checkout, collect_only):
    command = recorder_command() + (['--collect-only'] if collect_only else [])
    result = recorder_run(recorder_checkout, [*command, 'tests/test_population.py'])
    assert result.returncode == 0, result.stdout + result.stderr
    outcomes = prepare.load_tool('pbtest_outcomes')
    record = outcomes.parse(result.stdout)
    assert record is not None, 'documented script route produced no outcomes'
    assert len(record['collected']) == 2
    assert record['collect_only'] is collect_only
    complete = outcomes.completion(result.stdout)
    assert complete['collection_complete'] is True
    assert complete['collect_only'] is collect_only
    assert complete['collected'] == record['collected']
    if not collect_only:
        assert complete['status'] == 'complete'
        assert len(record['reports']) == 2


@pytest.mark.parametrize('callable_entry', [False, True])
@pytest.mark.parametrize('damage', [None, 'missing-completion', 'incomplete', 'mismatch', 'deselected'])
def test_collector_consumes_real_collection_records(recorder_checkout, tmp_path, monkeypatch, damage, callable_entry):
    # Exercise the documented script and the root runner's callable entry, so
    # parser behavior is checked independently of the script-entry regression.
    command_prefix = recorder_command(callable_entry=callable_entry)
    fleet = tmp_path / 'fleet'
    pbrun = fleet / 'repo/tools/pbrun.py'
    pbrun.parent.mkdir(parents=True)
    pbrun.write_text('# admitted client boundary\n')
    machine = 'x86_64'
    config = {'fleet_root': str(fleet), 'pbrun': str(pbrun), 'client_python': sys.executable,
        'collection': {'corpus': {machine: {'cpus': 1, 'mem_gb': 2, 'timeout_s': 30,
            'wait_s': 60, 'command_prefix': command_prefix}}}}
    evidence = tmp_path / 'evidence'
    collector = prepare.PublishedCollector(recorder_checkout, 'f' * 40, config, {}, evidence)
    files = ['tests/test_population.py']
    observed = {}

    def launch(argv, **kwargs):
        command = argv[argv.index('--') + 1:]
        result = recorder_run(recorder_checkout, command)
        assert result.returncode == 0, result.stdout + result.stderr
        output = result.stdout
        # Corrupt only recorded collection evidence after real pytest collection.
        if damage:
            lines = []
            for line in output.splitlines():
                if line.startswith(collector.outcomes.COMPLETION_PREFIX):
                    if damage == 'missing-completion':
                        continue
                    row = json.loads(line[len(collector.outcomes.COMPLETION_PREFIX):])
                    if damage == 'incomplete':
                        row['collection_complete'] = False
                    if damage == 'mismatch':
                        row['collected'] = row['collected'][:-1]
                    line = collector.outcomes.COMPLETION_PREFIX + json.dumps(row)
                if damage == 'deselected' and line.startswith(collector.outcomes.PREFIX):
                    row = json.loads(line[len(collector.outcomes.PREFIX):])
                    row['deselected'] = [row['collected'][0]]
                    line = collector.outcomes.PREFIX + json.dumps(row)
                lines.append(line)
            output = '\n'.join(lines)
        observed.update(command=command, output=output)
        return SimpleNamespace(process=SimpleNamespace(stdout=io.StringIO(output)),
            wait=lambda: SimpleNamespace(problem=None, ending=SimpleNamespace(exit_code=0),
                                         identity=SimpleNamespace(action_key='collection')))

    monkeypatch.setattr(prepare.client_channel.ClientProcess, 'launch', launch)
    collector.gate.resolve_action_key = lambda *args: ('collection', tmp_path / 'terminal')
    collector.gate._git = lambda *args: ''
    monkeypatch.setattr(prepare.pool.record_store, 'read_document',
                        lambda *args: {'status': 'executed', 'detail': {'returncode': 0}})
    snapshot_input = {'id': core.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID, 'sha256': 'a' * 64, 'bytes': 1}
    class CAS:
        def __init__(self, *args): pass
        def read_action_request(self, key):
            command = observed['command']
            code = tmp_path / 'closure.py'
            code.write_text('# test closure\n')
            return core.seal_action({'schema': core.ACTION_SCHEMA_V2,
                'task': {'definition_id': 'fleet/pbrun', 'definition_version': 'v1',
                    'task_class': 'generation', 'determinism': 'deterministic',
                    'artifact_family': 'generic', 'artifact_kind': 'generic',
                    'argv': movement_actions.standard_capture_argv(command, 'result.txt', path_prefix='/usr/bin'),
                    'working_directory': '.', 'result_path': 'result.txt'},
                'code_closure': core.build_code_closure(tmp_path, ['closure.py']),
                'environment': {'variables': {'PATH': '/usr/bin:/bin'}, 'toolchain': {}},
                'execution_scope': {'portability': 'portable', 'platform_key': None, 'host_class': None},
                'inputs': [snapshot_input], 'params': {'command': command, 'cwd': '.',
                    'checkout_snapshot': {'schema': core.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1,
                        'commit': 'e' * 40, 'input': snapshot_input, 'subdirectory': '.'}}})
        def lookup(self, action): return {'producer': {'evidence': {'machine': machine}}}
        def input_path(self, value): return tmp_path / 'bundle'
        def result_path(self, receipt, action):
            path = tmp_path / 'result.txt'
            path.write_text(observed['output'])
            return path
    monkeypatch.setattr(prepare.pb, 'PrismaBuildCAS', CAS)
    if damage:
        with pytest.raises(ValueError, match='complete independent collection'):
            collector(kind='corpus', machine=machine, files=files)
        assert not (evidence / 'corpus-x86_64.json').exists()
    else:
        nodes = collector(kind='corpus', machine=machine, files=files)
        assert nodes == ['tests/test_population.py::test_population[1]',
                         'tests/test_population.py::test_population[2]']
        sealed = evidence / 'corpus-x86_64.json'
        assert not sealed.stat().st_mode & 0o222
        saved = json.loads(sealed.read_bytes())
        assert saved['collection']['collected'] == nodes
        assert saved['completion']['collection_complete'] is True
        assert saved['completion']['collected'] == nodes


def test_documented_recorder_route_propagates_pytest_failure(recorder_checkout):
    path = recorder_checkout / 'tests/test_population.py'
    path.write_text(path.read_text().replace('value > 0', 'value < 0'))
    result = recorder_run(recorder_checkout, [*recorder_command(), 'tests/test_population.py'])
    assert result.returncode == 1
    record = prepare.load_tool('pbtest_outcomes').parse(result.stdout)
    assert record is not None
    assert [row[2] for row in record['reports']] == ['failed', 'failed']

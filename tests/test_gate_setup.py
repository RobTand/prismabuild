"""Sandbox setup is charged once and setup failures never become test failures."""
import importlib.util
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'tools/ci' / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_sandbox_files_share_one_setup_charge_without_losing_coverage():
    partition = load('gate_partition')
    files = ['tests/test_record_compatibility.py',
             'tests/test_current_receipt_compat_composition.py', 'plain-a.py', 'plain-b.py']
    durations = dict(zip(files, [9, 1, 8, 4]))
    result = partition.setup_shards(files, durations, 2, unknown_seconds=2,
                                    setup_seconds=8)
    assert [row['files'] for row in result] == [sorted(files[:2]), files[2:]]
    assert [row['estimated_seconds'] for row in result] == [18, 12]
    assert [row['sandbox'] for row in result] == [True, False]
    assert partition.setup_shards(list(reversed(files)), durations, 2,
                                  unknown_seconds=2, setup_seconds=8) == result


def test_setup_partition_preserves_unknown_files_and_normal_api():
    partition = load('gate_partition')
    files = ['new.py', 'known.py']
    result = partition.setup_shards(files, {'known.py': 1}, 2,
                                    unknown_seconds=5, setup_seconds=8)
    assert result[0]['files'] == ['new.py'] and result[0]['unknown_files'] == ['new.py']
    assert result[0]['estimated_seconds'] == 5
    assert not any(row['sandbox'] for row in result)
    assert partition.shards(files, {'known.py': 1}, 2, unknown_seconds=5) == [
        {'index': 0, 'files': ['new.py'], 'estimated_seconds': 5.0, 'unknown_files': ['new.py']},
        {'index': 1, 'files': ['known.py'], 'estimated_seconds': 1.0, 'unknown_files': []}]


@pytest.mark.parametrize('failures,expected', [(0, ['install']),
                                             (1, ['install', 'update', 'install'])])
def test_package_install_uses_existing_index_before_update(failures, expected):
    setup = load('gate_setup')
    commands = []
    def execute(command, **kwargs):
        operation = next(word for word in command if word in {'install', 'update'})
        commands.append(operation)
        if len(commands) <= failures:
            raise subprocess.CalledProcessError(100, command)
        return SimpleNamespace(returncode=0)
    setup.install_bubblewrap(execute)
    assert commands == expected


def test_package_fallback_failure_propagates_for_no_verdict_classification():
    setup = load('gate_setup')
    def refused(command, **kwargs):
        raise subprocess.CalledProcessError(100, command)
    with pytest.raises(subprocess.CalledProcessError):
        setup.install_bubblewrap(refused)


def test_setup_fault_is_no_verdict(tmp_path, monkeypatch, capsys):
    setup = load('gate_setup')
    files = tmp_path / 'files.txt'; files.write_text('plain.py\n')
    def unavailable(*args, **kwargs):
        raise OSError('injected hosted setup failure')
    monkeypatch.setattr(setup, 'prepare_assigned_gate', unavailable)
    assert setup.main(['--files-from', str(files)]) == 75
    assert 'NO_VERDICT: injected hosted setup failure' in capsys.readouterr().out


def test_relocation_moves_the_prefix_and_keeps_old_entry_points_valid(tmp_path):
    import shutil
    setup = load('gate_setup')
    prefix, target = tmp_path / 'cache/python', tmp_path / 'usr/python'
    (prefix / 'bin').mkdir(parents=True)
    (prefix / 'bin/python').write_bytes(b'interpreter bytes')
    (prefix / 'bin/pytest').write_text(f'#!{prefix}/bin/python\n')
    operations = []
    def execute(command, **kwargs):
        operations.append(command[1])
        if command[1] == 'mv':
            shutil.move(command[2], command[3])
        elif command[1] == 'ln':
            Path(command[-1]).symlink_to(command[-2], target_is_directory=True)
        else:
            raise AssertionError('unexpected relocation command: ' + repr(command))
        return SimpleNamespace(returncode=0)
    setup.relocate_python(prefix, target, execute)
    assert operations == ['mv', 'ln']
    assert prefix.is_symlink()
    assert (target / 'bin/python').read_bytes() == b'interpreter bytes'
    assert (prefix / 'bin/python').read_bytes() == b'interpreter bytes'
    assert (target / 'bin/pytest').read_text() == f'#!{prefix}/bin/python\n'


def test_setup_matrix_shard_and_retry_aggregation_use_the_same_assignment(tmp_path):
    import json
    import sys
    files = ['tests/test_record_compatibility.py',
             'tests/test_current_receipt_compat_composition.py', 'tests/test_plain.py']
    listing = tmp_path / 'files.txt'; listing.write_text('\n'.join(files) + '\n')
    durations = tmp_path / 'durations.json'
    durations.write_text(json.dumps({'files': dict(zip(files, [9, 1, 8]))}))
    matrix = tmp_path / 'matrix.json'
    common = ['--files-from', str(listing), '--durations', str(durations),
              '--shards', '2', '--setup-aware']
    def cli(script, *arguments):
        return subprocess.run([sys.executable, str(ROOT / 'tools/ci' / script), *arguments],
                              capture_output=True, text=True, check=False)
    result = cli('gate_partition.py', 'matrix', *common, '--runners', 'x64,arm64',
                 '--output', str(matrix))
    assert result.returncode == 0, result.stderr
    data = json.loads(matrix.read_text())
    assert [row['sandbox'] for row in data['include']] == [True, False]
    plan = {'selected': [{'path': path} for path in files], 'mode': 'full',
            'population': {'version': 'v1'}, 'head_sha': 'head', 'base_sha': 'base'}
    plan_path = tmp_path / 'plan.json'; plan_path.write_text(json.dumps(plan))
    for index, expected in enumerate([sorted(files[:2]), files[2:]]):
        assigned = tmp_path / f'assigned-{index}.txt'
        result = cli('gate_partition.py', 'shard', *common, '--index', str(index),
                     '--output-files', str(assigned))
        assert result.returncode == 0, result.stderr
        assert assigned.read_text().splitlines() == expected
        directory = tmp_path / 'artifacts' / f'proof-a1-{index}'
        directory.mkdir(parents=True)
        (directory / 'result.json').write_text(json.dumps(dict(
            mode='full', population_version='v1', head_sha='head', base_sha='base',
            files=expected, verdict='pass', pytest_exit_code=0)))
    result = cli('gate_actions.py', 'finish', '--plan', str(plan_path), '--matrix', str(matrix),
                 '--durations', str(durations), '--artifacts', str(tmp_path / 'artifacts'),
                 '--prefix', 'proof', '--output', str(tmp_path / 'final.json'))
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads((tmp_path / 'final.json').read_text())['missing_indices'] == []


def test_root_owned_interpreter_fixture_needs_no_namespace_or_relocation(monkeypatch):
    setup = load('gate_setup')
    operations = []
    monkeypatch.setattr(setup, '_export', lambda *args: None)
    def execute(command, **kwargs):
        operations.append(command)
        return SimpleNamespace(returncode=0)
    setup.prepare_assigned_gate(['tests/test_proof_runtime_observation.py'], execute=execute)
    assert ['sudo', 'chown', 'root:root', str(Path(setup.sys.executable).resolve())] in operations
    assert not any(word in {'apt-get', 'bwrap', 'mv', 'cp', 'ldconfig'}
                   for command in operations for word in command)



def test_long_files_avoid_other_collection_and_fixture_work():
    partition = load('gate_partition')
    files = ['long-a.py', 'long-b.py', *[f'short-{i}.py' for i in range(8)]]
    durations = dict(zip(files, [100, 80, *([30] * 8)]))
    rows = partition.setup_shards(files, durations, 4, unknown_seconds=2)
    assert rows[0]['files'] == ['long-a.py']
    assert rows[1]['files'] == ['long-b.py']
    assert [row['estimated_seconds'] for row in rows] == [100, 80, 120, 120]
    assert sorted(path for row in rows for path in row['files']) == sorted(files)
    assert rows == partition.setup_shards(list(reversed(files)), durations, 4, unknown_seconds=2)


def test_sandbox_group_can_be_isolated_without_repeating_its_setup_charge():
    partition = load('gate_partition')
    files = ['tests/test_record_compatibility.py',
             'tests/test_current_receipt_compat_composition.py', 'large.py',
             *[f'short-{i}.py' for i in range(8)]]
    durations = dict(zip(files, [65, 10, 100, *([30] * 8)]))
    rows = partition.setup_shards(files, durations, 4, unknown_seconds=2, setup_seconds=8)
    assert rows[0]['files'] == ['large.py']
    assert rows[1]['files'] == sorted(files[:2])
    assert rows[1]['estimated_seconds'] == 83
    assert sum(row['sandbox'] for row in rows) == 1
    assert sorted(path for row in rows for path in row['files']) == sorted(files)


def test_cpu_cost_guard_is_isolated_from_other_shard_work():
    partition = load('gate_partition')
    guard = 'tests/test_no_timing_sensitive_tests.py'
    files = ['long.py', guard, *[f'short-{i}.py' for i in range(8)]]
    durations = dict(zip(files, [100, 27, *([30] * 8)]))
    rows = partition.setup_shards(files, durations, 4, unknown_seconds=2)
    assert rows[0]['files'] == ['long.py']
    assert rows[1]['files'] == [guard]
    assert [row['estimated_seconds'] for row in rows] == [100, 27, 120, 120]
    assert sorted(path for row in rows for path in row['files']) == sorted(files)

"""Tier selection must cover the suite exactly and refuse incomplete proof."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import runpy
import subprocess
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('tier_verification', ROOT / 'tools' / 'verify.py')
verify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify)


def proof_tree(root, root_files=('tests/test_root_owner.py',), receipt_files=()):
    """A tree holding every declared file, its root manifests and one new CPU test."""
    tests = root / 'tests'
    tests.mkdir(parents=True, exist_ok=True)
    (tests / 'proof_execution.json').write_text(json.dumps(
        {'root': {'files': {name: 1 for name in root_files}}}))
    (tests / 'receipt_execution.json').write_text(json.dumps(
        {'root_files': {name: 1 for name in receipt_files}}))
    for name in (*verify.FAST, *verify.GPU, *verify.TWO_CPU, *root_files, *receipt_files,
                 'tests/test_future_integration.py'):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('def test_behavior(): pass\n')


def test_lanes_are_disjoint_and_cover_every_test_file(tmp_path):
    proof_tree(tmp_path, receipt_files=('tests/test_receipt_root.py',))
    lanes = verify.populations(tmp_path)
    files = [name for group in lanes.values() for name in group]
    assert len(files) == len(set(files))
    assert set(files) == {p.relative_to(tmp_path).as_posix()
                          for p in tmp_path.rglob('test_*.py')}
    assert lanes['integration'] == ['tests/test_future_integration.py']
    assert lanes['root'] == ['tests/test_receipt_root.py', 'tests/test_root_owner.py']
    assert lanes['two_cpu'] == sorted(verify.TWO_CPU)
    assert set(lanes['fast']) == set(verify.FAST)
    assert lanes['gpu'] == list(verify.GPU)
    assert verify.tier_files(tmp_path, 'integration') == sorted(
        lanes['integration'] + lanes['two_cpu'] + lanes['root'])


def test_real_tree_lanes_cover_every_test_file_once():
    lanes = verify.populations(ROOT)
    files = [name for group in lanes.values() for name in group]
    assert len(files) == len(set(files))
    assert set(files) == {p.relative_to(ROOT).as_posix() for p in (ROOT / 'tests').rglob('test_*.py')}
    profile = json.loads((ROOT / 'tests/proof_execution.json').read_text())
    assert set(profile['root']['files']) <= set(lanes['root'])
    assert lanes['gpu'] == list(verify.GPU)


def test_deleted_fast_test_refuses_instead_of_shrinking_the_gate(tmp_path):
    proof_tree(tmp_path)
    (tmp_path / verify.FAST[0]).unlink()
    with pytest.raises(ValueError, match='missing='):
        verify.populations(tmp_path)


def test_a_file_declared_in_two_lanes_refuses(tmp_path):
    proof_tree(tmp_path, root_files=(verify.TWO_CPU[0],))
    with pytest.raises(ValueError, match='in_two_lanes'):
        verify.populations(tmp_path)


@pytest.mark.parametrize('lane', ['fast', 'integration', 'two_cpu', 'gpu'])
def test_pbtest_lanes_use_published_fanout_with_declared_demand(lane):
    cmd = verify.pbtest_command(ROOT, lane, '/worker/bin/python', Path('/tmp/evidence.json'))
    assert cmd[1] == '/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py'
    assert cmd[cmd.index('--python') + 1] == '/worker/bin/python'
    assert cmd[cmd.index('--threads-per-shard') + 1] == '1'
    assert cmd[cmd.index('--workers-per-shard') + 1] == '1'
    assert cmd[cmd.index('--mem-gb') + 1] == ('8' if lane == 'gpu' else '4')
    assert ('--cpus-per-shard' in cmd) == (lane == 'two_cpu')
    if lane == 'two_cpu':
        assert cmd[cmd.index('--cpus-per-shard') + 1] == '2'
    assert '--shards' not in cmd
    assert ('--gpu' in cmd) == (lane == 'gpu')
    assert cmd[-len(verify.populations(ROOT)[lane]):] == verify.populations(ROOT)[lane]
    if lane == 'gpu':
        assert cmd[cmd.index('--gpu-memory-gb') + 1] == '2'
        assert 'PRISMABUILD_GPU_QUALIFICATION=1' in cmd


def test_root_lane_is_the_documented_container_action():
    required = json.loads((ROOT / 'tests/proof_execution.json').read_text())['root']
    profile_path = '_proof_runtime_profiles/standard3144.json'
    profile = json.loads((ROOT / profile_path).read_text())
    cmd = verify.root_command(ROOT)
    assert cmd[1] == '/mnt/shared/prismabuild-fleet/repo/tools/pbrun.py'
    assert cmd[cmd.index('--container-image') + 1] == profile['image']
    assert cmd[cmd.index('--cpus') + 1] == str(required['cpus'])
    assert cmd[cmd.index('--demand') + 1] == f"mem_gb={required['memory_gib']}"
    assert '--detach' in cmd
    assert cmd[cmd.index('--') + 1:] == [profile['primary']['executable']['path'],
        'tests/run_root_population.py', '--runtime-profile', profile_path,
        *verify.populations(ROOT)['root']]
    assert (ROOT / 'tests/run_root_population.py').is_file()


def test_root_helper_forwards_native_runtime_to_the_actual_adapter(monkeypatch):
    from types import SimpleNamespace

    root_spec = importlib.util.spec_from_file_location(
        'root_population_runner', ROOT / 'tests/run_root_population.py')
    runner = importlib.util.module_from_spec(root_spec)
    root_spec.loader.exec_module(runner)
    calls = []
    monkeypatch.setattr(runner.subprocess, 'run', lambda cmd, **kwargs:
                        calls.append(cmd) or SimpleNamespace(returncode=0))
    args = ['--runtime-profile', '_proof_runtime_profiles/standard3144.json',
            'tests/test_client_bootstrap_root.py']
    assert runner.main(args) == 0
    assert calls[0][1].endswith('tests/prepare_proof_execution.py')
    assert calls[1][1].endswith('tests/run_proof_container.py')
    assert calls[1][2:] == args


def row(files, *, green=True, skipped=None):
    return {'files': files, 'verdict': {'green': green}, 'skipped': skipped or [],
            'action_key': 'a' * 64, 'receipt_path': '/cas/receipt.json'}


@pytest.mark.parametrize('rows', [[], [row(['a'], green=False)], [row(['b'])],
                                 [row(['a']), row(['a'])]])
def test_missing_failed_wrong_and_duplicate_coverage_refuse(rows):
    assert verify.tier_verdict(rows, ['a'], 1, 'fast')['verdict'] == 'FAIL'


def test_fast_verdict_requires_the_under_ten_minute_budget():
    assert verify.tier_verdict([row(['a'])], ['a'], 599.9, 'fast')['verdict'] == 'PASS'
    assert verify.tier_verdict([row(['a'])], ['a'], 600, 'fast')['verdict'] == 'FAIL'


def test_gpu_skip_is_never_qualification():
    report = verify.tier_verdict([row(['a'], skipped=[{'reason': 'no device'}])], ['a'], 1, 'gpu')
    assert report['verdict'] == 'FAIL'
    assert report['skipped'] == [{'reason': 'no device'}]


def test_report_cannot_enter_the_sealed_checkout():
    with pytest.raises(SystemExit) as stopped:
        verify.main(['fast', '--report-dir', str(ROOT / 'evidence')])
    assert stopped.value.code == 2


def test_cas_verification_rejects_missing_or_wrong_receipts(monkeypatch):
    from types import SimpleNamespace

    with pytest.raises(ValueError, match='full action key'):
        verify.verify_cas([{'action_key': None}])
    monkeypatch.setattr(verify.subprocess, 'run', lambda *a, **k:
                        SimpleNamespace(returncode=1, stderr='missing receipt'))
    with pytest.raises(ValueError, match='CAS verification failed'):
        verify.verify_cas([row(['a'])])
    monkeypatch.setattr(verify.subprocess, 'run', lambda *a, **k:
                        SimpleNamespace(returncode=0, stdout='{"action_key":"wrong"}'))
    with pytest.raises(ValueError, match='different action'):
        verify.verify_cas([row(['a'])])


def test_failed_submission_without_reconciliation_still_returns_a_verdict():
    failed = row(['a'], green=False)
    failed['verdict']['reconciliation'] = None
    assert verify.tier_verdict([failed], ['a'], 1, 'integration')['verdict'] == 'FAIL'


def test_failed_shards_name_their_lane_and_files():
    failed = {**row(['b'], green=False), 'lane': 'root'}
    report = verify.tier_verdict([row(['a']), failed], ['a', 'b'], 1, 'integration')
    assert report['verdict'] == 'FAIL'
    assert report['failed_shards'] == [{'lane': 'root', 'action_key': 'a' * 64, 'files': ['b']}]


@pytest.mark.parametrize('case', ['early_exit', 'complete', 'skip', 'missing_completion',
                                 'failed_ending'])
def test_root_lane_reconciles_real_pytest_execution(tmp_path, monkeypatch, case):
    sample = tmp_path / 'sample'
    (sample / 'tests').mkdir(parents=True)
    selected = ['tests/test_root_sample.py']
    first = ("pytest.exit('controlled early exit', returncode=0)" if case == 'early_exit'
             else "pytest.skip('recorded root skip')" if case == 'skip' else 'assert True')
    (sample / selected[0]).write_text(
        f'import pytest\ndef test_first():\n    {first}\n'
        'def test_second():\n    assert True\n')
    bootstrap = (
        'import runpy; from pathlib import Path; '
        f"owner=runpy.run_path({str(ROOT / 'tools/fleet/pbtest_outcomes.py')!r}); "
        "raise SystemExit(owner['main']("
        f"['-q', '-rA', '-o', 'addopts=', '-p', 'no:cacheprovider', *{selected!r}], "
        f"collection_spec={{'files': {selected!r}, 'roots': [[{selected[0]!r}, 'file']]}}, "
        f"collection_source=Path({str(ROOT / 'src/prismabuild/pbtest_collection.py')!r}).read_text()))"
    )
    observed = subprocess.run([sys.executable, '-c', bootstrap], cwd=sample,
                              capture_output=True, text=True, check=False)
    assert observed.returncode == 0, observed.stdout + observed.stderr
    owner = runpy.run_path(str(ROOT / 'tools/fleet/pbtest_outcomes.py'))
    record = owner['parse'](observed.stdout, record_binding=owner['RECORD_BINDING_VERSION'])
    assert len(record['collected']) == 2
    if case == 'early_exit':
        assert owner['reconcile'](record, None, None)['never_ran']
    output = observed.stdout
    if case == 'missing_completion':
        output = '\n'.join(line for line in output.splitlines()
                           if not line.startswith(owner['COMPLETION_PREFIX']))
    payload = tmp_path / 'root.stdout'
    payload.write_text(output)
    key = 'a' * 64
    ending = {'action_key': key, 'succeeded': case != 'failed_ending',
              'status': 'executed', 'exit_code': 0,
              'result': {'stdout': {'path': str(payload)}}}
    replies = iter([
        SimpleNamespace(returncode=0, stdout=json.dumps({'action_key': key})),
        SimpleNamespace(returncode=0, stdout=json.dumps({'complete': True, 'endings': [ending]})),
    ])
    monkeypatch.setattr(verify, 'populations', lambda root: {'root': selected})
    monkeypatch.setattr(verify.subprocess, 'run', lambda *args, **kwargs: next(replies))
    report = tmp_path / 'evidence'
    report.mkdir()
    result = verify.run_root_lane(report)
    verdict = verify.tier_verdict([result], selected, 1, 'integration')
    assert verdict['verdict'] == ('PASS' if case in ('complete', 'skip') else 'FAIL')
    if case in ('complete', 'skip'):
        assert result['verdict']['completion']['status'] == 'complete'
        assert verdict['counts'] == ({'passed': 1, 'skipped': 1} if case == 'skip'
                                     else {'passed': 2})
        assert len(verdict['skipped']) == (1 if case == 'skip' else 0)
        if case == 'skip':
            assert 'recorded root skip' in verdict['skipped'][0]['reason']

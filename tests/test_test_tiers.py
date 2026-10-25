"""Tier selection must cover the suite exactly and refuse incomplete proof."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

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
    cmd = verify.root_command(ROOT, '/worker/bin/python')
    assert cmd[1] == '/mnt/shared/prismabuild-fleet/repo/tools/pbrun.py'
    assert cmd[cmd.index('--container-image') + 1] == required['image']
    assert cmd[cmd.index('--cpus') + 1] == str(required['cpus'])
    assert cmd[cmd.index('--demand') + 1] == f"mem_gb={required['memory_gib']}"
    assert '--detach' in cmd
    assert cmd[cmd.index('--') + 1:] == ['/worker/bin/python', 'tests/run_root_population.py']
    assert (ROOT / 'tests/run_root_population.py').is_file()


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

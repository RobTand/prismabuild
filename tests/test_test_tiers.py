"""Tier selection must cover the suite exactly and refuse incomplete proof."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('tier_verification', ROOT / 'tools' / 'verify.py')
verify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify)


def test_tiers_partition_every_test_file_and_adopt_new_tests(tmp_path):
    for name in (*verify.FAST, *verify.GPU, 'tests/test_future_integration.py'):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('def test_behavior(): pass\n')
    populations = verify.populations(tmp_path)
    files = [name for group in populations.values() for name in group]
    assert len(files) == len(set(files))
    assert set(files) == {p.relative_to(tmp_path).as_posix()
                          for p in tmp_path.rglob('test_*.py')}
    assert populations['integration'] == ['tests/test_future_integration.py']
    assert set(populations['fast']) == set(verify.FAST)
    assert populations['gpu'] == list(verify.GPU)


def test_deleted_fast_test_refuses_instead_of_shrinking_the_gate(tmp_path):
    with pytest.raises(ValueError, match='missing='):
        verify.populations(tmp_path)


@pytest.mark.parametrize('tier', ['fast', 'integration', 'gpu'])
def test_entrypoint_uses_published_fanout_with_declared_demand(tier):
    cmd = verify.command(ROOT, tier, '/worker/bin/python', Path('/tmp/evidence.json'))
    assert cmd[1] == '/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py'
    assert cmd[cmd.index('--python') + 1] == '/worker/bin/python'
    assert cmd[cmd.index('--threads-per-shard') + 1] == '1'
    assert cmd[cmd.index('--workers-per-shard') + 1] == '1'
    assert cmd[cmd.index('--mem-gb') + 1] == ('8' if tier == 'gpu' else '4')
    assert '--shards' not in cmd
    assert ('--gpu' in cmd) == (tier == 'gpu')
    if tier == 'gpu':
        assert cmd[cmd.index('--gpu-memory-gb') + 1] == '2'
        assert 'PRISMABUILD_GPU_QUALIFICATION=1' in cmd


def row(files, *, green=True, skipped=None):
    return {'files': files, 'verdict': {'green': green}, 'skipped': skipped or [],
            'action_key': 'a' * 64, 'receipt_path': '/cas/receipt.json'}


@pytest.mark.parametrize('rows', [[], [row(['a'], green=False)], [row(['b'])],
                                 [row(['a']), row(['a'])]])
def test_missing_failed_wrong_and_duplicate_coverage_refuse(rows):
    assert verify.verdict(rows, ['a'], 1, 'fast')['verdict'] == 'FAIL'


def test_fast_verdict_requires_the_under_ten_minute_budget():
    assert verify.verdict([row(['a'])], ['a'], 599.9, 'fast')['verdict'] == 'PASS'
    assert verify.verdict([row(['a'])], ['a'], 600, 'fast')['verdict'] == 'FAIL'


def test_gpu_skip_is_never_qualification():
    report = verify.verdict([row(['a'], skipped=[{'reason': 'no device'}])], ['a'], 1, 'gpu')
    assert report['verdict'] == 'FAIL'
    assert report['skipped'] == [{'reason': 'no device'}]


def test_report_cannot_enter_the_sealed_checkout():
    with pytest.raises(SystemExit) as stopped:
        verify.main(['fast', '--report-dir', str(ROOT / 'evidence')])
    assert stopped.value.code == 2


def test_real_tree_has_all_declared_tests():
    assert verify.populations(ROOT)['gpu'] == list(verify.GPU)


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
    assert verify.verdict([failed], ['a'], 1, 'integration')['verdict'] == 'FAIL'

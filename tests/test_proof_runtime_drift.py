"""Detect host runtime drift without changing the selected filesystem."""
import runpy
from pathlib import Path

from proof_container_runtime import inventory

CHECKER = runpy.run_path(str(Path(__file__).resolve().parents[1]
                           / 'tools/maintenance/check_proof_runtime_drift.py'))


def test_drift_reports_changed_added_removed_and_symlink_entries(tmp_path):
    (tmp_path / 'changed').write_bytes(b'before')
    (tmp_path / 'removed').write_bytes(b'removed')
    (tmp_path / 'link').symlink_to('changed')
    roots = [dict(origin='host', path=str(tmp_path), entries=inventory(tmp_path))]
    assert CHECKER['proof_runtime_host_drift'](roots)['matches']

    (tmp_path / 'changed').write_bytes(b'after')
    (tmp_path / 'removed').unlink()
    (tmp_path / 'added').write_bytes(b'added')
    (tmp_path / 'link').unlink()
    (tmp_path / 'link').symlink_to('added')
    actual = inventory(tmp_path)
    report = CHECKER['proof_runtime_host_drift'](roots)
    assert not report['matches']
    assert not report['qualification']
    assert report['errors'] == []
    changes = {Path(item['path']).name: item for item in report['changes']}
    assert set(changes) == {'added', 'changed', 'link', 'removed'}
    assert changes['added']['before'] is None
    assert changes['removed']['after'] is None
    assert changes['changed']['before']['sha256'] != changes['changed']['after']['sha256']
    assert changes['link']['before']['target'] == 'changed'
    assert changes['link']['after']['target'] == 'added'
    assert inventory(tmp_path) == actual


def test_drift_ignores_image_roots_and_refuses_unknown_host_inventory(tmp_path):
    roots = [dict(origin='image', path=str(tmp_path / 'not-mounted'), entries={}),
             dict(origin='host', path=str(tmp_path / 'missing'), entries={})]
    report = CHECKER['proof_runtime_host_drift'](roots)
    assert not report['matches']
    assert len(report['errors']) == 1
    assert report['errors'][0]['path'] == str(tmp_path / 'missing')
    assert report['changes'] == []


def test_drift_observation_errors_return_unknown(tmp_path, monkeypatch, capsys):
    checker = CHECKER['main'].__globals__
    def refuse_profile(*args):
        raise ValueError('invalid sealed profile')
    monkeypatch.setitem(checker, 'load_profile', refuse_profile)
    assert CHECKER['main'](['--runtime-profile', str(tmp_path / 'profile')]) == 2
    assert 'invalid sealed profile' in capsys.readouterr().out

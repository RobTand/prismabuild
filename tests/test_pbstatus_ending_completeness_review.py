"""Review of completeness through the real endings reader, on private queues."""
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools/fleet'))
import pbstatus
from prismabuild import pool


@pytest.mark.parametrize('fault', ['malformed_record', 'unreadable_directory'])
def test_unreadable_endings_do_not_certify_a_complete_census(
        tmp_path, monkeypatch, capsys, fault):
    q = pool.PoolQueue(tmp_path / 'queue')
    q.ensure_layout()
    if fault == 'malformed_record':
        (q.dir(pool.DONE) / ('a' * 64 + '.json')).write_text('{ truncated')
    else:
        scandir = pbstatus.os.scandir
        def refuse_done(path):
            if Path(path) == q.dir(pool.DONE):
                raise PermissionError('done directory unavailable')
            return scandir(path)
        monkeypatch.setattr(pbstatus.os, 'scandir', refuse_done)
    code = pbstatus.main(['--transport', 'pool', '--json',
                          '--queue-root', str(q.root)])
    report = json.loads(capsys.readouterr().out)
    assert report['complete'] is False, report
    assert code == pbstatus.EXIT_INCOMPLETE


def test_queue_root_note_does_not_hide_a_stat_error(tmp_path, monkeypatch, capsys):
    q = pool.PoolQueue(tmp_path / 'queue')
    q.ensure_layout()
    stat = Path.stat

    def refuse_root(path, *args, **kwargs):
        if path == q.root:
            raise PermissionError('root stat unavailable')
        return stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'stat', refuse_root)
    code = pbstatus.main(['--transport', 'pool', '--json', '--queue-root', str(q.root)])
    report = json.loads(capsys.readouterr().out)
    assert report['complete'] is False, report
    assert code == 3
    assert any(row['section'] == 'queue-root' and row['type'] == 'PermissionError'
               for row in report['unavailable_sections'])

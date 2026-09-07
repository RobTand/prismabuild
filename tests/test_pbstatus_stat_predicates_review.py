"""Filesystem predicates must not turn inaccessible paths into missing ones."""
import errno
import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools/fleet'))
import pbstatus


@pytest.mark.parametrize('boundary', ['root', 'terminal'])
def test_queue_root_stat_permission_failure_remains_unavailable(
        tmp_path, monkeypatch, capsys, boundary):
    if os.geteuid() == 0:
        pytest.skip('root bypasses directory traversal permissions')
    private = tmp_path / 'private'
    private.mkdir()
    queue = private / 'queue' if boundary == 'root' else tmp_path / 'queue'
    queue.mkdir()
    if boundary == 'terminal':
        terminal = private / 'terminal'
        terminal.mkdir()
        (queue / 'done').symlink_to(terminal, target_is_directory=True)
    # Keep the test on the real queue-root helper: previous sections answered
    # before permissions changed, and this last read must report its own error.
    monkeypatch.setattr(pbstatus, 'read_pool', lambda root: {
        'nodes': [], 'jobs': [], 'notes': [], 'queue': {'complete': True}})
    monkeypatch.setattr(pbstatus, 'read_endings', lambda root, limit: [])
    private.chmod(0)
    try:
        code = pbstatus.main(['--transport', 'pool', '--json', '--queue-root', str(queue)])
        report = json.loads(capsys.readouterr().out)
        assert code == 3 and report['complete'] is False, report
        assert any(row['section'] == 'queue-root' and row['type'] == 'PermissionError'
                   for row in report['unavailable_sections']), report
        # The compatibility display caller should diagnose the read failure too.
        assert 'cannot be read' in pbstatus.queue_root_note(queue)
    finally:
        private.chmod(0o700)


def test_missing_terminal_directory_recheck_does_not_hide_a_second_error(
        tmp_path, monkeypatch):
    queue = tmp_path / 'queue'
    directory = queue / 'done'
    directory.mkdir(parents=True)
    scandir, stat = os.scandir, os.stat

    def vanished(path):
        if Path(path) == directory:
            raise FileNotFoundError(errno.ENOENT, 'directory changed during lookup', str(path))
        return scandir(path)

    def unreadable(path, *args, **kwargs):
        if Path(path) == directory:
            raise PermissionError(errno.EACCES, 'directory became inaccessible', str(path))
        return stat(path, *args, **kwargs)

    monkeypatch.setattr(os, 'scandir', vanished)
    # Patch the syscall, not Path.exists: Python 3.14's actual predicate must
    # demonstrate why its False answer is insufficient for this distinction.
    monkeypatch.setattr(os, 'stat', unreadable)
    with pytest.raises(PermissionError):
        pbstatus.read_endings(queue)

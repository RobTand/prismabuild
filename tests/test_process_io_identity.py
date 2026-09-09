"""Do not attach a replacement process's I/O to an earlier PID incarnation."""
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import resource_scope


def _stat(starttime, *, state='S', command='payload (worker)'):
    # Fields 3 through 22; starttime is field 22, not a whitespace token
    # counted from the start of the parenthesized command name.
    fields = [state, '1'] + ['0'] * 17 + [str(starttime)]
    return '4242 (' + command + ') ' + ' '.join(fields)


def _proc_reads(monkeypatch, *, after, unreadable=False):
    original = Path.read_text
    io_was_read = False

    def read(path, *args, **kwargs):
        nonlocal io_was_read
        if path == Path('/proc/4242/stat'):
            if not io_was_read:
                return _stat(100)
            if isinstance(after, Exception):
                raise after
            return after
        if path == Path('/proc/4242/io'):
            io_was_read = True
            if unreadable:
                raise PermissionError('process counters unavailable')
            return ''.join(f'{name}: 9000000\n'
                           for name in resource_scope.IO_COUNTERS)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'read_text', read)


@pytest.mark.parametrize('after, expected', [
    (_stat(200), None),
    (FileNotFoundError('reaped'), None),
    ('malformed stat', ('4242:100', 1, None)),
    (PermissionError('identity temporarily unreadable'), ('4242:100', 1, None)),
])
def test_io_is_discarded_when_the_original_process_cannot_be_revalidated(
        monkeypatch, after, expected):
    _proc_reads(monkeypatch, after=after)
    result = resource_scope.read_process_io(4242)
    assert result == expected, 'unverified counters were attached to PID incarnation 4242:100'


def test_io_survives_normal_process_state_and_command_changes(monkeypatch):
    _proc_reads(monkeypatch, after=_stat(100, state='R', command='renamed ) worker'))
    identity, parent, counters = resource_scope.read_process_io(4242)
    assert identity == '4242:100' and parent == 1
    assert counters == {name: 9000000 for name in resource_scope.IO_COUNTERS}


def test_unreadable_counters_keep_identity_without_inventing_io(monkeypatch):
    _proc_reads(monkeypatch, after=_stat(100), unreadable=True)
    assert resource_scope.read_process_io(4242) == ('4242:100', 1, None)

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


def _sampling_proc(monkeypatch, tmp_path):
    scope = resource_scope.ResourceScope('a' * 64, 'b' * 32, 1024,
                                         tmp_path / 'telemetry.json')
    scope.cgroup_path = tmp_path / 'scope'
    monkeypatch.setattr(resource_scope, 'scope_pids', lambda _, **kw: [4242, 4343])
    state = {'stat': _stat(100), 'bytes': 100, 'peer_bytes': 20}
    original = Path.read_text

    def read(path, *args, **kwargs):
        if path == Path('/proc/4242/stat'):
            if isinstance(state['stat'], Exception):
                raise state['stat']
            return state['stat']
        if path == Path('/proc/4343/stat'):
            return _stat(300).replace('4242', '4343', 1)
        if path in (Path('/proc/4242/io'), Path('/proc/4343/io')):
            amount = state['bytes'] if path.parts[2] == '4242' else state['peer_bytes']
            return ''.join(f'{name}: {amount}\n' for name in resource_scope.IO_COUNTERS)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'read_text', read)
    return scope, state


@pytest.mark.parametrize('unavailable', [
    PermissionError('stat unreadable'), OSError('transient procfs failure'),
    'malformed stat',
])
def test_initial_identity_outage_does_not_retire_a_surviving_root(
        monkeypatch, tmp_path, unavailable):
    scope, state = _sampling_proc(monkeypatch, tmp_path)
    assert scope.sample_process_io()['wchar'] == 120
    state.update(stat=unavailable, peer_bytes=30)
    during = scope.sample_process_io()
    assert during['retired']['wchar'] == 0, 'unknown identity was treated as departed'
    assert during['wchar'] == 130, 'retain old bytes while other processes update'
    assert during['processes_unreadable'] == 1 and during['errors']
    # Scope reconstruction must preserve the unknown reading too.
    import json
    scope.telemetry_path.write_text(json.dumps({'nonce': scope.nonce, 'process_io': during}))
    scope._process_io = None
    state.update(stat=_stat(100), bytes=150)
    after = scope.sample_process_io()
    assert after['wchar'] == 180, 'a surviving root was counted twice after recovery'
    assert after['processes_observed'] == 2
    assert after['processes_unreadable'] == 0


def test_confirmed_departure_still_retires_root_before_pid_reuse(monkeypatch, tmp_path):
    scope, state = _sampling_proc(monkeypatch, tmp_path)
    scope.sample_process_io()
    state['stat'] = FileNotFoundError('gone')
    gone = scope.sample_process_io()
    assert gone['retired']['wchar'] == 100
    state.update(stat=_stat(200), bytes=7)
    after = scope.sample_process_io()
    assert after['wchar'] == 127
    assert after['processes_observed'] == 3


def test_first_identity_outage_does_not_invent_a_process_or_bytes(monkeypatch, tmp_path):
    scope, state = _sampling_proc(monkeypatch, tmp_path)
    state['stat'] = PermissionError('not yet readable')
    unknown = scope.sample_process_io()
    assert unknown['processes_observed'] == 1 and unknown['wchar'] == 20
    assert unknown['processes_unreadable'] == 1
    state['stat'] = _stat(100)
    known = scope.sample_process_io()
    assert known['processes_observed'] == 2 and known['wchar'] == 120

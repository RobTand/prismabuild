"""Actual child processes exercise the event pipe independently of output EOF."""
import ctypes
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

import pytest

from prismabuild import action_client as client, client_channel as channel
from test_client_channel import CAS, IDENTITY, QUEUE


HEADER = '''
import os, sys, time
from pathlib import Path
from prismabuild import action_client as client, action_wait, client_channel as channel
fd = int(sys.argv[sys.argv.index('--client-events-fd') + 1])
identity = client.PoolClientIdentity('a' * 64, 10.0, Path('/configured/queue'),
    Path('/configured/cas'), Path('/configured/queue/ready/' + 'a' * 64 + '.json'),
    client.ClientArrival.SUBMITTED)
ending = client.ClientEnding(client.ClientUnavailable('a' * 64, 10.0,
    action_wait.WaitEnd.FINAL_EMPTY))
writer = channel.ClientEventWriter(fd,
    expected_identity=identity if os.environ.get('CLIENT_RECOVERY') else None)
'''


def _environment(**extra):
    return dict(os.environ, PYTHONPATH=str(Path(client.__file__).resolve().parents[1]), **extra)


def _launch(body, *, env=None, **kwargs):
    return channel.ClientProcess.launch([sys.executable, '-c', HEADER + body],
        queue_root=QUEUE, cas_root=CAS, env=env or _environment(), **kwargs)


def test_identity_arrives_while_the_child_waits_to_finish(tmp_path):
    gate = tmp_path / 'finish'
    arrived = threading.Event()
    identities = []

    def on_identity(identity):
        identities.append(identity)
        arrived.set()

    running = _launch('''
writer.send(identity)
gate = Path(os.environ['CLIENT_GATE'])
until = time.monotonic() + 5
while not gate.exists() and time.monotonic() < until:
    time.sleep(0.01)
print('unchanged child output', flush=True)
writer.send(ending)
sys.exit(74)
''', env=_environment(CLIENT_GATE=str(gate)), on_identity=on_identity)
    try:
        assert arrived.wait(2)
        assert identities == [IDENTITY]
        assert running.process.poll() is None
    finally:
        gate.touch()
    output, _ = running.process.communicate(timeout=3)
    result = running.wait()
    assert output == 'unchanged child output\n'
    assert result.problem is None
    assert result.may_recover(deadline=12, now=11)


def test_a_fork_only_descendant_cannot_make_the_parent_wait_for_event_eof(tmp_path):
    libc = ctypes.CDLL(None, use_errno=True)
    previous = ctypes.c_int()
    assert libc.prctl(37, ctypes.byref(previous), 0, 0, 0) == 0
    assert libc.prctl(36, 1, 0, 0, 0) == 0
    gate = tmp_path / 'release-descendant'
    running = None
    descendant = None
    finished = threading.Event()
    results = []
    waiter = None
    try:
        running = _launch('''
writer.send(identity)
pid = os.fork()
if pid == 0:
    os.close(1)
    os.close(2)
    gate = Path(os.environ['CLIENT_GATE'])
    until = time.monotonic() + 5
    while not gate.exists() and time.monotonic() < until:
        time.sleep(0.01)
    os._exit(0)
print(pid, flush=True)
writer.send(ending)
sys.exit(74)
''', env=_environment(CLIENT_GATE=str(gate)))
        output, _ = running.process.communicate(timeout=2)
        descendant = int(output)
        os.kill(descendant, 0)

        def wait():
            results.append(running.wait())
            finished.set()

        waiter = threading.Thread(target=wait)
        waiter.start()
        assert finished.wait(0.5)
        assert not gate.exists()
        os.kill(descendant, 0)
        assert results[0].problem is None
    finally:
        gate.touch()
        if descendant is not None:
            assert os.waitpid(descendant, 0) == (descendant, 0)
        if waiter is not None:
            waiter.join(timeout=2)
            assert not waiter.is_alive()
        elif running is not None:
            running.process.wait(timeout=2)
            running.wait()
        assert libc.prctl(36, previous.value, 0, 0, 0) == 0


@pytest.mark.parametrize('damage', [
    pytest.param('missing', id='missing-ending'),
    pytest.param('truncated', id='truncated-ending'),
    pytest.param('oversized', id='oversized-channel'),
    pytest.param('wrong-exit', id='contradictory-exit'),
])
def test_real_process_damage_cannot_authorize_recovery(damage):
    body = {
        'missing': 'writer.send(identity)\nsys.exit(74)\n',
        'truncated': 'writer.send(identity)\nos.write(fd, channel.encode_client_event(ending, identity=identity)[:-1])\nsys.exit(74)\n',
        'oversized': 'os.write(fd, b"x" * 2049)\nsys.exit(0)\n',
        'wrong-exit': 'writer.send(identity)\nwriter.send(ending)\nsys.exit(0)\n',
    }[damage]
    running = _launch(body)
    running.process.communicate(timeout=3)
    result = running.wait()
    assert result.problem is not None
    assert not result.may_recover(deadline=12, now=11)
    assert running.decoder.bytes_seen <= 2049


def test_recovery_uses_the_original_identity_and_an_ending_only_pipe():
    running = _launch('writer.send(ending)\nsys.exit(74)\n',
        env=_environment(CLIENT_RECOVERY='1'), expected_identity=IDENTITY)
    running.process.communicate(timeout=3)
    result = running.wait()
    assert result.problem is None
    assert result.identity == IDENTITY
    assert running.decoder.events == 1


def test_an_action_argument_named_like_the_private_flag_is_preserved():
    body = HEADER + '''
assert sys.argv[sys.argv.index('--') + 1:] == ['--client-events-fd', 'payload-value']
writer.send(identity)
writer.send(ending)
sys.exit(74)
'''
    running = channel.ClientProcess.launch(
        [sys.executable, '-c', body, '--', '--client-events-fd', 'payload-value'],
        queue_root=QUEUE, cas_root=CAS, env=_environment())
    running.process.communicate(timeout=3)
    assert running.wait().problem is None


def test_failed_launch_closes_both_private_pipe_ends(monkeypatch):
    before = set(os.listdir('/proc/self/fd'))

    def fail(*_args, **_kwargs):
        raise OSError('controlled launch failure')

    monkeypatch.setattr(subprocess, 'Popen', fail)
    with pytest.raises(OSError, match='controlled launch failure'):
        _launch('')
    assert set(os.listdir('/proc/self/fd')) == before

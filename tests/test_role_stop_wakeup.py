"""Real cooperative stop signals must wake a wait even during its setup."""
import os
from pathlib import Path
import signal
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
HEADER = '''
import faulthandler, os, select, signal, sys, threading
from prismabuild.role_session import stop_requests
signum = int(sys.argv[1])
'''


def child_result(tmp_path, body, signum=signal.SIGTERM):
    script = tmp_path / 'stop_wait.py'
    script.write_text(HEADER + body)
    child = subprocess.Popen(
        [sys.executable, '-B', str(script), str(int(signum))],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        env=dict(os.environ, PYTHONPATH=str(ROOT / 'src')),
    )
    try:
        output = child.communicate(timeout=2)[0]
    except subprocess.TimeoutExpired:
        child.kill()
        output = child.communicate(timeout=2)[0]
        pytest.fail('cooperative signal did not wake the wait:\n' + output.decode(), pytrace=False)
    return child.returncode, output


@pytest.mark.parametrize('signum', [int(signal.SIGTERM), int(signal.SIGINT)])
def test_signal_during_wait_setup_is_not_blocked_or_lost(tmp_path, signum):
    code, output = child_result(tmp_path, '''
with stop_requests(signum) as stop:
    wait_code = type(stop).wait.__code__
    armed = False
    def deliver(frame, event, value):
        global armed
        legacy = (event == 'return'
                  and frame.f_code is threading.Condition.__enter__.__code__
                  and frame.f_back.f_code is wait_code)
        native = (event == 'c_call' and value is select.select
                  and frame.f_code is wait_code)
        if not armed and (legacy or native):
            armed = True
            sys.setprofile(None)
            print('at-wait-boundary', flush=True)
            os.kill(os.getpid(), signum)
    faulthandler.dump_traceback_later(0.5)
    sys.setprofile(deliver)
    woke = stop.wait(3600)
    sys.setprofile(None)
    faulthandler.cancel_dump_traceback_later()
    assert armed, 'the real blocking boundary was not reached'
    assert woke is True and stop.is_set()
    assert stop.wait(0) is True
print('stopped', flush=True)
''', signum)
    assert code == 0, output.decode()
    assert output == b'at-wait-boundary\nstopped\n'


@pytest.mark.parametrize('signum', [int(signal.SIGTERM), int(signal.SIGINT)])
def test_signal_before_wait_remains_sticky(tmp_path, signum):
    code, output = child_result(tmp_path, '''
previous = signal.getsignal(signum)
with stop_requests(signum) as stop:
    os.kill(os.getpid(), signum)
    assert stop.is_set()
    assert stop.wait(0) is True
    assert stop.wait(3600) is True
assert signal.getsignal(signum) is previous
print('sticky', flush=True)
''', signum)
    assert code == 0, output.decode()
    assert output == b'sticky\n'


def test_wait_without_stop_expires_without_requesting_stop(tmp_path):
    code, output = child_result(tmp_path, '''
with stop_requests(signum) as stop:
    assert stop.wait(0) is False
    assert stop.wait(0.01) is False
    assert stop.wait(-1) is False
    assert not stop.is_set()
print('expired', flush=True)
''')
    assert code == 0, output.decode()
    assert output == b'expired\n'


@pytest.mark.parametrize('signum,isolated', [
    (int(signal.SIGTERM), False), (int(signal.SIGTERM), True),
    (int(signal.SIGINT), False), (int(signal.SIGINT), True),
], ids=['term-shared-fds', 'term-isolated-fds',
        'int-shared-fds', 'int-isolated-fds'])
def test_a_forked_reader_cannot_request_or_corrupt_its_parents_stop(
        tmp_path, signum, isolated):
    code, output = child_result(tmp_path, '''
import time
from prismabuild._bounded_reader import _isolate_child_fds
with stop_requests(signum) as stop:
    answer_read, answer_write = os.pipe()
    pid = os.fork()
    if pid == 0:
        writer = answer_write
        try:
            os.close(answer_read)
            if ''' + repr(isolated) + ''':
                writer = _isolate_child_fds(writer)
            os.kill(os.getpid(), signum)
            os.write(writer, b'clean')
            os._exit(0)
        except BaseException as exc:
            os.write(writer, repr(exc).encode())
            os._exit(1)
    os.close(answer_write)
    try:
        reply = os.read(answer_read, 4096)
        _, status = os.waitpid(pid, 0)
    finally:
        os.close(answer_read)
    assert os.waitstatus_to_exitcode(status) == 0, reply
    assert reply == b'clean', reply
    started = time.monotonic()
    assert stop.wait(0.05) is False
    assert time.monotonic() - started >= 0.04
    assert not stop.is_set()
print('fork-isolated', flush=True)
''', signum)
    assert code == 0, output.decode()
    assert output == b'fork-isolated\n'


@pytest.mark.parametrize('signum', [int(signal.SIGTERM), int(signal.SIGINT)])
def test_native_wakeup_precedes_the_deferred_python_stop_callback(tmp_path, signum):
    code, output = child_result(tmp_path, '''
with stop_requests(signum) as stop:
    handler = signal.getsignal(signum)
    pending = []
    signal.signal(signum, lambda *args: pending.append(args))
    os.kill(os.getpid(), signum)
    assert pending and not stop.is_set()
    assert select.select([stop._read_fd], [], [], 0.03)[0] == [stop._read_fd], 'kernel signal did not notify independently of Python callback'
    assert not stop.is_set()
    for args in pending:
        handler(*args)
    assert stop.wait(0) is True
print('native-wake', flush=True)
''', signum)
    assert code == 0, output.decode()
    assert output == b'native-wake\n'


def test_an_unselected_signal_does_not_shorten_the_stop_wait(tmp_path):
    code, output = child_result(tmp_path, '''
import time
observed = []
previous = signal.signal(signal.SIGUSR1, lambda *args: observed.append(args))
try:
    with stop_requests(signum) as stop:
        os.kill(os.getpid(), signal.SIGUSR1)
        assert observed
        started = time.monotonic()
        assert stop.wait(0.03) is False
        assert time.monotonic() - started >= 0.02
        assert not stop.is_set()
        stop.set()
        assert stop.wait(0) is True
finally:
    signal.signal(signal.SIGUSR1, previous)
print('unselected', flush=True)
''')
    assert code == 0, output.decode()
    assert output == b'unselected\n'


def test_the_role_restores_the_previous_native_wakeup_descriptor(tmp_path):
    code, output = child_result(tmp_path, '''
read_fd, write_fd = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
previous_fd = signal.set_wakeup_fd(write_fd)
previous_handler = signal.signal(signal.SIGUSR1, lambda *_: None)
try:
    with stop_requests(signum) as stop:
        assert stop.wait(0) is False
    os.kill(os.getpid(), signal.SIGUSR1)
    assert os.read(read_fd, 1) == bytes([int(signal.SIGUSR1)])
finally:
    signal.set_wakeup_fd(previous_fd)
    signal.signal(signal.SIGUSR1, previous_handler)
    os.close(write_fd)
    os.close(read_fd)
print('restored', flush=True)
''')
    assert code == 0, output.decode()
    assert output == b'restored\n'


@pytest.mark.parametrize('threaded', [False, True], ids=['main-thread', 'worker-thread'])
def test_a_forked_reader_cannot_write_a_reused_stop_descriptor(tmp_path, threaded):
    target = tmp_path / 'reused-stop-descriptor.bin'
    code, output = child_result(tmp_path, '''
import traceback
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
results = []
with stop_requests(signum) as stop:
    def fork_reader():
        pid = os.fork()
        if pid == 0:
            try:
                inherited_writer = stop._write_fd
                os.close(stop._read_fd)
                os.close(inherited_writer)
                opened = os.open(''' + repr(str(target)) + ''', os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                if opened != inherited_writer:
                    os.dup2(opened, inherited_writer)
                    os.close(opened)
                os.kill(os.getpid(), signum)
                assert os.fstat(inherited_writer).st_size == 0, 'native signal wrote into the reader file'
                os.close(inherited_writer)
                os._exit(0)
            except BaseException:
                traceback.print_exc()
                os._exit(1)
        _, status = os.waitpid(pid, 0)
        results.append(os.waitstatus_to_exitcode(status))
    if ''' + repr(threaded) + ''':
        reader = threading.Thread(target=fork_reader)
        reader.start()
        reader.join()
    else:
        fork_reader()
    assert results == [0], results
    assert not stop.is_set()
    stop.set()
    assert stop.wait(0) is True
print('reused-fd-safe', flush=True)
''')
    assert code == 0, output.decode()
    assert output == b'reused-fd-safe\n'
    assert target.read_bytes() == b''

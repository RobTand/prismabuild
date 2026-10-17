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
    assert not stop.is_set()
print('expired', flush=True)
''')
    assert code == 0, output.decode()
    assert output == b'expired\n'

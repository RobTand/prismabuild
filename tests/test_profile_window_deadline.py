"""A deadline after the profiler exits must retain its completed report (#372)."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_pool_term_during_window_settle_keeps_profile_and_reaps_action(tmp_path):
    driver = tmp_path / 'driver.py'
    driver.write_text('''
import json, os, sys, time
from contextlib import contextmanager
from pathlib import Path
sys.path[:0] = [sys.argv[1] + '/src', sys.argv[1] + '/tests']
from test_sample_profile import _FakeBackend, _action, _speedscope
from prismabuild import core as pb
base = Path(sys.argv[2])
class Early(_FakeBackend):
    exits_before_action = True
    settle_seconds = 60
    def launch_argv(self, argv, *, profile_path):
        code = "import sys,subprocess; from pathlib import Path; subprocess.Popen(sys.argv[3:]); Path(sys.argv[1]).write_text(sys.argv[2])"
        # The child is relayed, and the outer process exits immediately, as
        # nsys --duration does. Both remain in the same owned process group.
        return [sys.executable, '-c', code, str(profile_path), _speedscope('window'), *argv]
pb.PROFILE_BACKENDS['fake'] = Early()
original = pb._ProfileSession._settled_exit_status
original_signals = pb._sigterm_unwinds_this_process
pending = None
@contextmanager
def signals_ready():
    with original_signals():
        if pending is not None:
            (base/'settling.json').write_text(json.dumps(pending))
        yield
pb._sigterm_unwinds_this_process = signals_ready
def settling(self, process=None):
    global pending
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            record = self.exit_status()
            if record.get('phase') == 'launched': break
        except pb.ProfileUnusable: pass
        time.sleep(.01)
    pending = {'group': process.pid, 'child': record['child_pid']}
    return original(self, process)
pb._ProfileSession._settled_exit_status = settling
checkout = base/'checkout'; checkout.mkdir()
action = _action(checkout, profile='fake')
body = {k:v for k,v in action.items() if k != 'action_key'}
body['task'] = {**body['task'], 'argv': [sys.executable, '-c', 'import time; time.sleep(120)']}
os.environ[pb.ACTION_STATUS_PATH_ENV] = str(base/'status.json')
pb.run_local_action(pb.seal_action(body), cas_root=base/'cas', checkout_root=checkout)
''')
    process = subprocess.Popen([sys.executable, str(driver), str(ROOT), str(tmp_path)],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    owned = None
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if (tmp_path/'settling.json').exists():
                owned = json.loads((tmp_path/'settling.json').read_text())
                break
            if process.poll() is not None:
                pytest.fail(repr(process.communicate()))
            time.sleep(.01)
        assert owned is not None, 'driver never reached the window settle'
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=20)
        assert process.returncode == 143, (stdout, stderr)
        stat = Path(f"/proc/{owned['child']}/stat")
        assert not stat.exists() or stat.read_text().rsplit(')', 1)[1].split()[0] == 'Z'
        status = tmp_path/'status.json'
        assert status.exists(), 'deadline discarded the completed profile instead of filing status'
        profile = json.loads(status.read_text())['profile']
        assert profile['produced'] is True and profile['partial'] is True
        blob = tmp_path/'cas'/'blobs'/profile['blob_sha256'][:2]/profile['blob_sha256']
        assert hashlib.sha256(blob.read_bytes()).hexdigest() == profile['blob_sha256']
        assert json.loads(blob.read_text())['profiles'][0]['name'] == 'window'
        assert not (tmp_path/'checkout'/'.prismabuild-profile').exists()
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
        if owned:
            try:
                os.killpg(owned['group'], signal.SIGKILL)
            except ProcessLookupError:
                pass

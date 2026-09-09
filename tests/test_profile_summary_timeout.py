"""Optional summary work cannot spend the action's entire finish deadline."""
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

from prismabuild import core as pb
from test_sample_profile import _FakeBackend, _action


@pytest.mark.parametrize('blocked', ['leader', 'descendant'])
def test_stalled_summary_preserves_success_and_reaps_its_group(
    tmp_path, monkeypatch, blocked,
):
    # Use a real CPU subprocess, including an exited leader with a descendant
    # holding its pipes. Shorten the production budget; the old implementation
    # ignores it and completes this deliberately late summary instead.
    monkeypatch.setattr(pb, 'PROFILE_SUMMARY_TIMEOUT_SECONDS', 1.0, raising=False)
    script = tmp_path / 'nsys'
    script.write_text(f'''#!{sys.executable}
import os, signal, sys, time
from pathlib import Path
base = Path(sys.argv[sys.argv.index('--output') + 1])
if {blocked!r} == 'descendant':
    child = os.fork()
    if child: sys.exit(0)
signal.signal(signal.SIGTERM, signal.SIG_IGN)
base.with_suffix('.summary-pid').write_text(str(os.getpid()))
time.sleep(3)
base.with_name(base.name + '_cuda_gpu_kern_sum.csv').write_text('name,time\\nmm,1\\n')
''')
    script.chmod(0o755)
    nsys = pb.NsysProfileBackend()
    nsys._path = str(script)
    members = []

    class Supplement(_FakeBackend):
        def extra_blobs(self, path):
            blobs = nsys.extra_blobs(path)
            members.append(int(path.with_suffix('').with_suffix('.summary-pid').read_text()))
            return blobs

        def extra_blob_notes(self):
            return nsys.extra_blob_notes()

    monkeypatch.setitem(pb.PROFILE_BACKENDS, 'fake', Supplement())
    checkout = tmp_path / 'checkout'
    checkout.mkdir()
    action = _action(checkout, profile='fake')
    cas = pb.PrismaBuildCAS(tmp_path / 'cas')
    result = pb.run_local_action(action, cas_root=tmp_path / 'cas', checkout_root=checkout)
    assert cas.lookup(action) == result['receipt']
    profile = result['profile']
    assert profile['produced'] is True and 'partial' not in profile
    blob = Path(profile['blob_path']).read_bytes()
    assert len(blob) == profile['bytes']
    assert hashlib.sha256(blob).hexdigest() == profile['blob_sha256']
    assert json.loads(blob)['profiles'][0]['name'] == 'fake'
    assert 'kernel_summary_sha256' not in profile
    assert 'timed out' in profile['kernel_summary_absent']
    assert members, 'the controlled summary process never started'
    for pid in members:
        stat = Path(f'/proc/{pid}/stat')
        if stat.exists():
            assert stat.read_text().rsplit(')', 1)[1].split()[0] == 'Z'


def test_signal_during_summary_reaps_before_unwinding(tmp_path, monkeypatch):
    marker = tmp_path / 'started'
    script = tmp_path / 'nsys'
    script.write_text(f'''#!{sys.executable}
import signal, time
from pathlib import Path
signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path({str(marker)!r}).touch()
time.sleep(3)
''')
    script.chmod(0o755)
    backend = pb.NsysProfileBackend()
    backend._path = str(script)
    processes = []

    class SignalWhileWaiting(subprocess.Popen):
        def communicate(self, *args, **kwargs):
            processes.append(self)
            deadline = time.monotonic() + 3
            while not marker.exists():
                assert self.poll() is None, 'summary exited before signal'
                assert time.monotonic() < deadline, 'summary never started'
                time.sleep(.01)
            signal.raise_signal(signal.SIGTERM)
            pytest.fail('summary swallowed termination')

    monkeypatch.setattr(pb.subprocess, 'Popen', SignalWhileWaiting)
    with pytest.raises(SystemExit) as error:
        backend.extra_blobs(tmp_path / 'report.nsys-rep')
    assert error.value.code == 128 + signal.SIGTERM
    assert len(processes) == 1
    assert processes[0].poll() == -signal.SIGKILL

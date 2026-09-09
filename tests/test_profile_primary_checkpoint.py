"""An optional summary must not hide a primary profile already saved in CAS."""
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

from test_sample_profile import _FakeBackend, _action, _speedscope
from prismabuild import core as pb, pool


@pytest.mark.parametrize('stage', ['summary', 'extra_ingest'])
def test_hard_stop_during_supplement_preserves_primary(tmp_path, stage):
    # The driver executes a real local action inside the admitted test scope.
    # Its payload finishes before the marker: only the driver is killed, and
    # no exception/finally handler can manufacture the missing checkpoint.
    driver = tmp_path / 'driver.py'
    driver.write_text('''
import json, os, sys, time
from pathlib import Path
sys.path[:0] = [sys.argv[1] + '/src', sys.argv[1] + '/tests']
sys.path.insert(0, str(Path(sys.argv[4]).parents[1]))
from prismabuild import core as pb
assert Path(pb.__file__).resolve() == Path(sys.argv[4])
from test_sample_profile import _FakeBackend, _action
base = Path(sys.argv[2]); stage = sys.argv[3]
def pause():
    (base/'supplement.ready').write_text('ready')
    time.sleep(120)
class Supplement(_FakeBackend):
    def extra_blobs(self, path):
        if stage == 'summary': pause()
        extra = path.with_name('kernels.csv')
        extra.write_text('name,time\\nmm,1\\n')
        return [('kernel_summary', extra)]
pb.PROFILE_BACKENDS['fake'] = Supplement()
original = pb.PrismaBuildCAS.ingest_input
def ingest(self, path, *, input_id):
    if input_id == 'prismabuild.profile.kernel_summary': pause()
    return original(self, path, input_id=input_id)
pb.PrismaBuildCAS.ingest_input = ingest
checkout = base/'checkout'; checkout.mkdir()
action = _action(checkout, profile='fake')
body = {k:v for k,v in action.items() if k != 'action_key'}
body['task'] = {**body['task'], 'argv': [sys.executable, '-c',
    'from pathlib import Path; Path("result.txt").write_text("completed payload")']}
action = pb.seal_action(body)
(base/'action.json').write_text(json.dumps(action))
os.environ[pb.ACTION_STATUS_PATH_ENV] = str(base/'status.json')
pb.run_local_action(action, cas_root=base/'cas', checkout_root=checkout)
''')
    process = subprocess.Popen(
        [sys.executable, str(driver), str(Path(__file__).resolve().parents[1]),
         str(tmp_path), stage, str(Path(pb.__file__).resolve())],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 15
        while not (tmp_path / 'supplement.ready').exists():
            if process.poll() is not None:
                pytest.fail(repr(process.communicate()))
            assert time.monotonic() < deadline, 'optional supplement never started'
            time.sleep(.01)
        process.kill()
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == -signal.SIGKILL, (stdout, stderr)
        cas = pb.PrismaBuildCAS(tmp_path / 'cas')
        action = json.loads((tmp_path / 'action.json').read_text())
        assert cas.lookup(action) is None, 'an evidence checkpoint is not success'
        status = tmp_path / 'status.json'
        assert status.exists(), 'saved primary profile has no checkpoint'
        ending = pool.PoolQueue._merge_action_status({'status': 'timeout'}, status)
        profile = ending['profile']
        assert profile['produced'] is True and profile['partial'] is True
        assert 'kernel_summary_sha256' not in profile
        blob = Path(profile['blob_path']).read_bytes()
        assert len(blob) == profile['bytes']
        assert hashlib.sha256(blob).hexdigest() == profile['blob_sha256']
        assert json.loads(blob)['profiles'][0]['name'] == 'fake'
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=10)


def test_normal_completion_keeps_richer_profile(tmp_path, monkeypatch):
    class Supplement(_FakeBackend):
        def extra_blobs(self, path):
            extra = path.with_name('kernels.csv')
            extra.write_text('name,time\nmm,1\n')
            return [('kernel_summary', extra)]

    monkeypatch.setitem(pb.PROFILE_BACKENDS, 'fake', Supplement())
    status = tmp_path / 'status.json'
    monkeypatch.setenv(pb.ACTION_STATUS_PATH_ENV, str(status))
    checkout = tmp_path / 'checkout'
    checkout.mkdir()
    action = _action(checkout, profile='fake')
    result = pb.run_local_action(action, cas_root=tmp_path / 'cas', checkout_root=checkout)
    assert pb.PrismaBuildCAS(tmp_path / 'cas').lookup(action) == result['receipt']
    profile = result['profile']
    assert 'partial' not in profile
    assert Path(profile['kernel_summary_blob_path']).read_text() == 'name,time\nmm,1\n'
    ending = pool.PoolQueue._merge_action_status({'profile': profile}, status)
    assert ending['profile'] == profile and not status.exists()


@pytest.mark.parametrize('early', [False, True])
def test_successful_profile_survives_unparseable_stdout_as_complete(tmp_path, monkeypatch, early):
    class Supplement(_FakeBackend):
        exits_before_action = early
        settle_seconds = 10

        def launch_argv(self, argv, *, profile_path):
            if not early:
                return super().launch_argv(argv, profile_path=profile_path)
            # Like a duration-limited profiler: the report is ready while the
            # relayed action continues in the same owned process group.
            code = '''
import sys, subprocess, time
from pathlib import Path
child = subprocess.Popen(sys.argv[4:])
deadline = time.monotonic() + 10
while not Path(sys.argv[3]).exists():
    assert child.poll() is None, 'relay exited without its startup record'
    assert time.monotonic() < deadline, 'relay never started'
    time.sleep(.01)
Path(sys.argv[1]).write_text(sys.argv[2])
'''
            return [sys.executable, '-c', code, str(profile_path),
                    _speedscope('early'), str(profile_path.with_name('exit_status')), *argv]

        def extra_blobs(self, path):
            extra = path.with_name('kernels.csv')
            extra.write_text('name,time\nmm,1\n')
            return [('kernel_summary', extra)]

    monkeypatch.setitem(pb.PROFILE_BACKENDS, 'fake', Supplement())
    status = tmp_path / 'status.json'
    monkeypatch.setenv(pb.ACTION_STATUS_PATH_ENV, str(status))
    checkout = tmp_path / 'checkout'
    checkout.mkdir()
    action = _action(checkout, profile='fake')
    result = pb.run_local_action(action, cas_root=tmp_path / 'cas', checkout_root=checkout)
    assert pb.PrismaBuildCAS(tmp_path / 'cas').lookup(action) == result['receipt']
    # A trailing payload line hides the final JSON from the stdout parser.
    stdout = json.dumps(result) + '\nlate payload output\n'
    assert pool.profile_from_launcher_stdout(stdout) is None
    ending = pool.PoolQueue._merge_action_status({'status': 'executed', 'returncode': 0}, status)
    assert ending['profile'] == result['profile'], 'success adopted the in-flight checkpoint'
    assert 'partial' not in ending['profile']
    assert Path(ending['profile']['kernel_summary_blob_path']).read_text() == 'name,time\nmm,1\n'
    assert not status.exists()


@pytest.mark.parametrize('failure', ['invalid', 'ingest'])
def test_unvalidated_or_unstored_profile_is_not_checkpointed(tmp_path, monkeypatch, failure):
    status = tmp_path / 'status.json'
    monkeypatch.setenv(pb.ACTION_STATUS_PATH_ENV, str(status))
    session = pb._ProfileSession(mode='fake', backend=_FakeBackend(), directory=tmp_path)
    session.profile_path.write_text('invalid' if failure == 'invalid' else _speedscope('saved'))
    cas = pb.PrismaBuildCAS(tmp_path / 'cas')
    if failure == 'ingest':
        def fail(*args, **kwargs):
            raise OSError('cannot save primary')
        monkeypatch.setattr(cas, 'ingest_input', fail)
    with pytest.raises((pb.ProfileUnusable, OSError)):
        session.ingest(cas)
    assert not status.exists()


@pytest.mark.parametrize('early', [False, True])
def test_a_failed_profiled_action_records_its_complete_profile(tmp_path, monkeypatch, early):
    """A failing run is the run somebody most wants a profile of.

    The payload runs, the profiler produces a complete report of it, and the
    action then exits nonzero. Result publication never happens on that path,
    so the refresh that a successful run gets never runs either, and the only
    profile left beside the job is the in-flight checkpoint that `ingest`
    writes -- marked partial, for a report that is not partial at all. The
    error already names the CAS blob in its message text; a reader should not
    have to scrape prose for evidence the process is holding.
    """
    class Supplement(_FakeBackend):
        exits_before_action = early

        def extra_blobs(self, path):
            extra = path.with_name('kernels.csv')
            extra.write_text('name,time\nmm,1\n')
            return [('kernel_summary', extra)]

    monkeypatch.setitem(pb.PROFILE_BACKENDS, 'fake', Supplement())
    status = tmp_path / 'status.json'
    monkeypatch.setenv(pb.ACTION_STATUS_PATH_ENV, str(status))
    checkout = tmp_path / 'checkout'
    checkout.mkdir()
    action = _action(checkout, profile='fake', exit_code=7)

    with pytest.raises(pb.LocalActionError) as raised:
        pb.run_local_action(action, cas_root=tmp_path / 'cas', checkout_root=checkout)
    error = raised.value
    assert error.returncode == 7
    assert error.profile is not None, 'the failing run kept no profile'
    assert 'partial' not in error.profile
    # The blob the message names and the record the error carries are the same
    # object, so a reader has one place to look rather than two.
    assert error.profile['blob_sha256'] in str(error)

    pb._record_action_status(error)
    ending = pool.PoolQueue._merge_action_status(
        {'status': 'failed', 'returncode': 1}, status)
    assert ending['action_returncode'] == 7
    assert 'partial' not in ending['profile'], 'a complete report filed as partial'
    assert ending['profile'] == error.profile
    assert Path(ending['profile']['kernel_summary_blob_path']).read_text() == 'name,time\nmm,1\n'
    assert not status.exists()


def test_an_unprofiled_failure_still_records_only_its_ending(tmp_path, monkeypatch):
    """No profile was asked for, so none is invented: a reader can still tell
    a run that produced no profile from one whose profile went missing."""
    status = tmp_path / 'status.json'
    monkeypatch.setenv(pb.ACTION_STATUS_PATH_ENV, str(status))
    checkout = tmp_path / 'checkout'
    checkout.mkdir()
    action = _action(checkout, profile=None, exit_code=3)

    with pytest.raises(pb.LocalActionError) as raised:
        pb.run_local_action(action, cas_root=tmp_path / 'cas', checkout_root=checkout)
    assert raised.value.returncode == 3
    assert raised.value.profile is None

    pb._record_action_status(raised.value)
    ending = pool.PoolQueue._merge_action_status(
        {'status': 'failed', 'returncode': 1}, status)
    assert ending['action_returncode'] == 3
    assert 'profile' not in ending

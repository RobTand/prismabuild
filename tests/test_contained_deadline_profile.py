"""Contained deadlines flush evidence before an unconditional exact-scope stop.

Admission and scope discovery are explicit fixture inputs. The worker, relay,
payload, SIGTERM unwind, sampler SIGINT flush and CAS ingestion remain real.
"""
from __future__ import annotations

import json
import os
import select
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from prismabuild import core as pb
from prismabuild import pool, resource_scope, local_scratch
from admitted_queue_fixture import AdmittedQueueFixture
from profile_test_support import profile_checkout_boundary, wait_for_profile_file

_BLOCKED_WORK = 'import signal; from pathlib import Path; Path("ready").write_text("ready"); signal.pause()'

_FAKE_SAMPLER = r'''
import json, os, signal, subprocess, sys, threading
from pathlib import Path
sys.path.insert(0, sys.argv[3])
from profile_test_support import wait_for_profile_file
profile_path, marker, argv = sys.argv[1], sys.argv[2], sys.argv[5:]
def note(event):
    with open(marker, "a") as handle:
        handle.write(event + "\n")
def flush(_signum, _frame):
    note("SIGINT")
    document = {
        "$schema": "https://www.speedscope.app/file-format-schema.json",
        "shared": {"frames": [{"name": "work"}]},
        "profiles": [{"type": "sampled", "name": "fake", "unit": "seconds",
                      "startValue": 0, "endValue": 1,
                      "samples": [[0]], "weights": [1]}],
    }
    staged = profile_path + ".partial"
    with open(staged, "w") as handle:
        json.dump(document, handle)
    os.replace(staged, profile_path)
    sys.exit(0)
def refuse(_signum, _frame):
    note("SIGTERM")
    sys.exit(0)
signal.signal(signal.SIGINT, flush)
signal.signal(signal.SIGTERM, refuse)
child = subprocess.Popen(argv)
wait_for_profile_file(Path(profile_path).with_name("exit_status"),
                      lambda text: json.loads(text)['phase'] == 'launched')
note("group:" + str(os.getpgrp()))
note("started")
sys.exit(child.wait())
'''
_DEAF_SAMPLER = _FAKE_SAMPLER.replace(
    '    document = {',
    '    signal.signal(signal.SIGINT, signal.SIG_IGN)\n'
    '    threading.Event().wait()\n'
    '    document = {')

_SITECUSTOMIZE = r'''
import os, signal, sys
from pathlib import Path
src = os.environ.get("PB_TEST_1401_SRC")
sampler = os.environ.get("PB_TEST_1401_SAMPLER")
marker = os.environ.get("PB_TEST_1401_MARKER", "")
if src and sampler:
    sys.path.insert(0, src)
    from prismabuild import core
    class FlushOnSignal:
        name = "fake-sampler"
        version = "fake-sampler 1.0"
        flush_signal = signal.SIGINT
        flush_seconds = 3.0
        profile_suffix = "speedscope.json"
        def locate(self): return sampler
        def launch_argv(self, argv, *, profile_path):
            read_text = Path.read_text
            def retain_relay_record(path, *args, **kwargs):
                raw = read_text(path, *args, **kwargs)
                if path == profile_path.with_name("exit_status"):
                    Path(marker + ".relay.json").write_text(raw)
                return raw
            Path.read_text = retain_relay_record
            return [sys.executable, sampler, str(profile_path), marker,
                    SUPPORT_DIRECTORY, "--", *argv]
        def read_profile(self, path): return core.read_speedscope(path)
    core.PROFILE_BACKENDS["flush-on-signal"] = FlushOnSignal()
'''


class _HardStopScope:
    """Own only the worker returned by Popen and the sampler's declared group."""
    def __init__(self, root, marker):
        self.cgroup_path = root / 'scope'
        self.cgroup_path.mkdir()
        self.marker = marker
        self.process = None
        self.stops = []
        self.samples = []

    def wrap_argv(self, argv, *, worker_script):
        return list(argv)

    def sample(self):
        record = {'complete': True, 'oom_local': 0, 'sampled_unix': pool._now()}
        self.samples.append(record)
        return record

    def _request(self, op, **extra):
        return {}

    def write_telemetry(self, record):
        pass

    def terminate_owned(self, reason):
        self.stops.append(reason)
        groups = [self.process.pid]
        if self.marker.exists():
            groups.extend(int(line.split(':')[1]) for line in self.marker.read_text().splitlines()
                          if line.startswith('group:'))
        for group in groups:
            try:
                os.killpg(group, signal.SIGKILL)
            except ProcessLookupError:
                pass
        return {'stopped': True}


def _claimed(tmp_path: Path, monkeypatch, *, profile='flush-on-signal'):
    checkout = tmp_path / 'checkout'
    checkout.mkdir()
    profile_checkout_boundary(checkout)
    (checkout / 'task.py').write_text(_BLOCKED_WORK)
    params = {'execution_timeout_s': 30.0}
    if profile is not None:
        params['profile'] = profile
    action = pb.seal_action({
        'schema': pb.ACTION_SCHEMA_V2,
        'task': {'definition_id': 'tests/contained-deadline-profile',
                 'definition_version': 'v1', 'task_class': 'generation',
                 'determinism': 'deterministic', 'artifact_family': 'generic',
                 'artifact_kind': 'generic', 'argv': [sys.executable, 'task.py'],
                 'working_directory': '.', 'result_path': 'result'},
        'inputs': [], 'code_closure': pb.build_code_closure(checkout, ['task.py']),
        'params': params, 'environment': {'variables': {}, 'toolchain': {}},
        'execution_scope': {'portability': 'portable', 'platform_key': None, 'host_class': None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / 'cas')
    cas.publish_action_request(action)
    # This family tests deadline handling, not the host-local state policy.
    # Declare a local filesystem in the admission model even on an NFS runner;
    # all opened-directory identities and real ledger transitions are retained.
    def simulated_local_state(descriptor):
        return {**local_scratch._descriptor_mount(descriptor), 'filesystem_type': 'ext4'}
    monkeypatch.setattr(local_scratch, '_descriptor_state_identity', simulated_local_state)
    queue = AdmittedQueueFixture(pool.PoolQueue(tmp_path / 'queue'),
                                capacity={'cpu': 2, 'mem_gb': 4},
                                default_demand={'cpu': 1, 'mem_gb': 1})
    queue.publish(action_key=action['action_key'], cas_root=cas.root, checkout_root=checkout,
                  worker_script=Path(__file__).resolve().parents[1] / 'tools/prismabuild_worker.py',
                  max_attempts=1, retry_safe=False)
    item = queue.claim()
    assert item is not None
    return queue, item, cas, checkout


def _contained_run(tmp_path, monkeypatch, *, deaf=False, remove_request=False):
    queue, item, cas, checkout = _claimed(tmp_path, monkeypatch)
    marker = tmp_path / 'sampler.events'
    sampler = tmp_path / 'sampler.py'
    sampler.write_text(_DEAF_SAMPLER if deaf else _FAKE_SAMPLER)
    inject = tmp_path / 'inject'
    inject.mkdir()
    (inject / 'sitecustomize.py').write_text(_SITECUSTOMIZE.replace(
        'SUPPORT_DIRECTORY', repr(str(Path(__file__).parent))))
    monkeypatch.setenv('PB_TEST_1401_SRC', str(Path(pb.__file__).resolve().parents[1]))
    monkeypatch.setenv('PB_TEST_1401_SAMPLER', str(sampler))
    monkeypatch.setenv('PB_TEST_1401_MARKER', str(marker))
    monkeypatch.setenv('PYTHONPATH', os.pathsep.join(
        part for part in (str(inject), os.environ.get('PYTHONPATH', '')) if part))
    scope = _HardStopScope(tmp_path, marker)
    monkeypatch.setattr(queue, '_start_resource_scope', lambda item: scope)
    # Discovery's negative and identity transitions have dedicated tests below.
    # This execution fixture hands over its own Popen identity, never scans /proc.
    monkeypatch.setattr(pool, '_contained_worker_pid', lambda *a, **k: scope.process.pid)
    monkeypatch.setattr(pool, '_contained_worker_pidfd',
                        lambda pid, *a, **k: os.pidfd_open(pid))
    now = [time.monotonic()]
    clock = pool.ExecutionClock(lambda: now[0], lambda: now[0])
    spawn = subprocess.Popen
    request_path = cas.root / 'requests' / item['action_key'][:2] / (item['action_key'] + '.json')

    def launch(*args, **kwargs):
        process = spawn(*args, **kwargs)
        scope.process = process
        communicate = process.communicate
        first = True
        def deadline(*a, **kw):
            nonlocal first
            if first:
                first = False
                wait_for_profile_file(marker, lambda text: 'started\n' in text, process=process)
                wait_for_profile_file(checkout / 'ready', process=process)
                if remove_request:
                    request_path.unlink()
                now[0] += 31
                raise subprocess.TimeoutExpired('controlled contained deadline', 0)
            return communicate(*a, **kw)
        process.communicate = deadline
        return process
    monkeypatch.setattr(subprocess, 'Popen', launch)
    if deaf:
        # Deliver the sampler's acknowledgement before expiring the flush budget.
        def exhausted(descriptor, *, deadline):
            wait_for_profile_file(marker, lambda text: 'SIGINT\n' in text, process=scope.process)
            return False
        monkeypatch.setattr(pool, '_wait_for_pidfd_exit', exhausted)
    outcome = queue.execute(item, timeout_s=30, containment=True,
                            heartbeat_s=60, timeout_grace_s=60, clock=clock)
    return outcome, scope, cas, request_path, marker.read_text().splitlines()


def test_a_contained_deadline_preserves_the_partial_sample_profile(tmp_path, monkeypatch):
    outcome, scope, cas, request_path, events = _contained_run(tmp_path, monkeypatch)
    assert outcome['status'] == 'timeout'
    assert outcome['termination_reason'] == 'execution_deadline'
    assert outcome['returncode'] is None
    assert scope.stops == ['timeout']
    assert scope.samples
    assert outcome['profile_settle']['settled'] is True
    profile = outcome['profile']
    assert profile['partial'] is True
    assert profile['produced'] is True
    relay = json.loads((tmp_path / 'sampler.events.relay.json').read_text())
    assert profile['action_phase'] == relay['phase']
    assert relay['phase'] in {'launched', 'ended'}
    if relay['phase'] == 'ended':
        assert relay['returncode'] == -signal.SIGTERM
        assert relay['signal'] == signal.SIGTERM
    assert profile['samples'] == 1
    blob = cas.root / 'blobs' / profile['blob_sha256'][:2] / profile['blob_sha256']
    assert json.loads(blob.read_text())['$schema'] == pb.PROFILE_SPEEDSCOPE_SCHEMA
    assert 'SIGINT' in events and 'SIGTERM' not in events
    assert cas.lookup(json.loads(request_path.read_text())) is None


def test_a_flush_that_outlives_its_bound_still_reaches_the_hard_stop(tmp_path, monkeypatch):
    outcome, scope, cas, request_path, events = _contained_run(tmp_path, monkeypatch, deaf=True)
    assert 'SIGINT' in events
    assert outcome['status'] == 'timeout'
    assert outcome['returncode'] is None
    assert scope.stops == ['timeout']
    assert outcome.get('profile') is None
    assert outcome['profile_settle']['settled'] is False
    assert cas.lookup(json.loads(request_path.read_text())) is None


def test_the_flush_opportunity_needs_no_request_read_after_launch(tmp_path, monkeypatch):
    outcome, scope, cas, request_path, events = _contained_run(
        tmp_path, monkeypatch, remove_request=True)
    assert not request_path.exists()
    assert outcome['status'] == 'timeout'
    assert scope.stops == ['timeout']
    assert outcome['profile']['partial'] is True
    assert 'SIGINT' in events


def test_only_a_unique_exact_worker_is_eligible(monkeypatch) -> None:
    """Discovery refuses ambiguity, the proxy argv and unreadable identities."""

    directory = (1, 2)
    monkeypatch.setattr(
        pool, "_scope_directory_identity", lambda path: directory)
    alive = {11: True, 12: True}
    monkeypatch.setattr(pool, "_process_alive", lambda pid: alive.get(pid, False))
    cmdlines: dict[int, object] = {}
    monkeypatch.setattr(pool, "_process_cmdline", lambda pid: cmdlines.get(pid))
    worker = [b"/usr/bin/python3", b"/gen/worker.py", b"run-local",
              b"--action", b"/cas/ab/" + b"a" * 64 + b".json"]
    proxy = [b"/usr/bin/python3", b"/gen/resource_exec.py", b"--", *worker]
    cgroup = Path("/sys/fs/cgroup/scope")

    # Two exact workers are ambiguous: nothing is eligible.
    monkeypatch.setattr(
        resource_scope, "scope_pids", lambda path, **kw: [11, 12])
    cmdlines.update({11: list(worker), 12: list(worker)})
    assert pool._contained_worker_pid(
        cgroup, worker, directory=directory) is None

    # The resource_exec proxy is never the worker, so one exact match stands.
    cmdlines[12] = list(proxy)
    assert pool._contained_worker_pid(
        cgroup, worker, directory=directory) == 11

    # No exact match is not a guess.
    cmdlines[11] = list(proxy)
    assert pool._contained_worker_pid(
        cgroup, worker, directory=directory) is None

    # A census that reports errors is incomplete: one observed match is not
    # uniqueness, so nothing is eligible.
    def diagnosed(path, **kwargs):
        kwargs["errors"].append("one subtree unreadable")
        return [11]

    monkeypatch.setattr(resource_scope, "scope_pids", diagnosed)
    cmdlines[11] = list(worker)
    assert pool._contained_worker_pid(
        cgroup, worker, directory=directory) is None

    # An unreadable member identity refuses rather than guessing.
    monkeypatch.setattr(
        resource_scope, "scope_pids", lambda path, **kw: [11])

    def explode(pid):
        raise OSError("identity denied")

    monkeypatch.setattr(pool, "_process_cmdline", explode)
    assert pool._contained_worker_pid(
        cgroup, worker, directory=directory) is None

    # A scope directory rebound underneath the census is refused.
    monkeypatch.setattr(pool, "_process_cmdline", lambda pid: cmdlines.get(pid))
    rebound = iter([directory, (3, 4)])
    monkeypatch.setattr(
        pool, "_scope_directory_identity", lambda path: next(rebound))
    assert pool._contained_worker_pid(
        cgroup, worker, directory=directory) is None


def test_an_incomplete_scope_census_never_reaches_a_signal(monkeypatch) -> None:
    """One observed match is not uniqueness when discovery reports errors."""

    directory = (1, 2)
    cgroup = Path("/sys/fs/cgroup/scope")
    worker = [b"/usr/bin/python3", b"/gen/worker.py", b"run-local"]
    monkeypatch.setattr(
        pool, "_scope_directory_identity", lambda path: directory)
    monkeypatch.setattr(pool, "_process_alive", lambda pid: True)
    monkeypatch.setattr(pool, "_process_cmdline", lambda pid: list(worker))

    def incomplete(path, **kwargs):
        kwargs["errors"].append("one subtree unreadable")
        return [4242]

    monkeypatch.setattr(resource_scope, "scope_pids", incomplete)
    assert pool._contained_worker_pid(
        cgroup, worker, directory=directory) is None

    # The whole opportunity refuses: no pidfd is opened and no signal sent.
    class _Scope:
        cgroup_path = "/sys/fs/cgroup/scope"

    opened: list[int] = []
    signals: list[tuple[int, int]] = []

    def never_reached(pid, flags):
        # If this is ever called, hand back a real descriptor this test owns
        # so the production close can never touch a framework FD.
        read_end, write_end = os.pipe()
        os.close(write_end)
        opened.append(read_end)
        return read_end

    monkeypatch.setattr(pool.os, "pidfd_open", never_reached)
    monkeypatch.setattr(
        pool.signal, "pidfd_send_signal",
        lambda fd, sig: signals.append((fd, sig)))
    result = pool._settle_contained_profile(
        _Scope(), worker, profiled=True,
        deadline=float("inf"))
    assert result is not None and result["settled"] is False
    assert "no unique exact worker" in str(result["refused"])
    assert opened == []
    assert signals == []


def test_a_pidfd_proves_the_worker_or_the_opportunity_is_refused(
    monkeypatch,
) -> None:
    """Recycled, vanished, unreadable or pidfd-less identities never signal."""

    cgroup = Path("/sys/fs/cgroup/scope")
    directory = (1, 2)
    worker = ["/usr/bin/python3", "/gen/worker.py", "run-local"]
    signals: list[tuple[int, int]] = []
    opened: list[int] = []

    def owned_descriptor(pid, flags):
        # A real descriptor this test owns, so the production close reaches
        # only this test's FD and never a synthetic number that could name a
        # framework FD.  The write end is closed so the read end is pollable.
        read_end, write_end = os.pipe()
        os.close(write_end)
        opened.append(read_end)
        return read_end

    def closed(descriptor: int) -> bool:
        try:
            os.fstat(descriptor)
        except OSError:
            return True
        return False

    monkeypatch.setattr(
        pool, "_scope_directory_identity", lambda path: directory)
    monkeypatch.setattr(
        pool, "_contained_worker_start_ticks", lambda pid: next(ticks))
    monkeypatch.setattr(
        pool, "_contained_worker_pid", lambda c, a, *, directory: 4242)
    monkeypatch.setattr(pool.os, "pidfd_open", owned_descriptor)
    monkeypatch.setattr(
        pool.signal, "pidfd_send_signal",
        lambda fd, sig: signals.append((fd, sig)))

    # A pid the kernel recycled across the open: start ticks moved.
    ticks = iter([100, 999])
    assert pool._contained_worker_pidfd(
        4242, cgroup, worker, directory=directory) is None
    assert len(opened) == 1 and closed(opened[0])
    assert signals == []

    # A worker that vanished across the open: no unique identity remains.
    ticks = iter([100, 100])
    monkeypatch.setattr(
        pool, "_contained_worker_pid", lambda c, a, *, directory: None)
    assert pool._contained_worker_pidfd(
        4242, cgroup, worker, directory=directory) is None
    assert len(opened) == 2 and all(closed(fd) for fd in opened)

    # An identity that cannot be read after the open closes the pidfd.
    ticks = iter([100, 100])

    def explode(c, a, *, directory):
        raise OSError("recheck denied")

    monkeypatch.setattr(pool, "_contained_worker_pid", explode)
    assert pool._contained_worker_pidfd(
        4242, cgroup, worker, directory=directory) is None
    assert len(opened) == 3 and all(closed(fd) for fd in opened)
    assert signals == []

    # A kernel without pidfds refuses; there is no raw os.kill fallback.
    monkeypatch.setattr(
        pool, "_contained_worker_pid", lambda c, a, *, directory: 4242)
    monkeypatch.delattr(pool.os, "pidfd_open", raising=False)
    assert pool._contained_worker_pidfd(
        4242, cgroup, worker, directory=directory) is None
    assert len(opened) == 3 and all(closed(fd) for fd in opened)
    assert signals == []


def test_the_flush_opportunity_is_gated_and_fails_closed(monkeypatch) -> None:
    """Unprofiled, expired and pidfd-less paths fall through to the hard stop."""

    class _Scope:
        cgroup_path = "/sys/fs/cgroup/scope"

    discovered: list[int] = []
    monkeypatch.setattr(pool, "_scope_directory_identity", lambda path: (1, 2))
    monkeypatch.setattr(
        pool, "_contained_worker_pid",
        lambda c, a, *, directory: (discovered.append(1), 4242)[1])
    monkeypatch.setattr(
        pool, "_contained_worker_pidfd",
        lambda p, c, a, *, directory: None)

    # The prevalidated declaration is the whole gate: no request is consulted.
    assert pool._settle_contained_profile(
        _Scope(), ["w"], profiled=False,
        deadline=float("inf")) is None
    assert discovered == []

    # An expired grace refuses before discovery.
    expired = pool._settle_contained_profile(
        _Scope(), ["w"], profiled=True, deadline=float("-inf"))
    assert expired is not None and expired["settled"] is False
    assert "grace expired" in str(expired["refused"])
    assert discovered == []

    # Discovery that raises before any open is a refusal, not an escape.
    def explode(c, a, *, directory):
        raise RuntimeError("census broke")

    monkeypatch.setattr(pool, "_contained_worker_pid", explode)
    broke = pool._settle_contained_profile(
        _Scope(), ["w"], profiled=True, deadline=float("inf"))
    assert broke is not None and broke["settled"] is False
    assert "worker discovery failed" in str(broke["refused"])

    # A pidfd that cannot prove the worker records the refusal.
    monkeypatch.setattr(
        pool, "_contained_worker_pid",
        lambda c, a, *, directory: (discovered.append(1), 4242)[1])
    refused = pool._settle_contained_profile(
        _Scope(), ["w"], profiled=True, deadline=float("inf"))
    assert refused is not None and refused["settled"] is False
    assert refused["worker_pid"] == 4242
    assert "pidfd" in str(refused["refused"])
    assert discovered == [1]


def test_the_prelaunch_read_is_the_profile_authority(tmp_path: Path, monkeypatch) -> None:
    """One validated request read answers deadline and profile before launch."""

    _queue, item, cas, _checkout = _claimed(tmp_path, monkeypatch)
    assert pool._sealed_execution_policy(item, None) == (30.0, True)
    assert pool._sealed_execution_policy(item, 5.0) == (5.0, True)

    (tmp_path / "plain").mkdir()
    _queue, plain, _cas, _checkout = _claimed(tmp_path / "plain", monkeypatch, profile=None)
    assert pool._sealed_execution_policy(plain, 5.0) == (5.0, False)

    # A missing request is the legacy path: no sealed budget, so no deadline
    # (#1707) and no profile claim.
    absent = {"action_key": "c" * 64, "cas_root": tmp_path / "absent"}
    assert pool._sealed_execution_policy(absent, None) == (None, False)
    assert pool._sealed_execution_policy(absent, 4.0) == (None, False)

    # A request filed under the wrong key is refused before launch.
    other = "b" * 64
    key = item["action_key"]
    mismatch = cas.root / "requests" / other[:2] / f"{other}.json"
    mismatch.parent.mkdir(parents=True, exist_ok=True)
    mismatch.write_text(
        (cas.root / "requests" / key[:2] / f"{key}.json").read_text(
            encoding="utf-8"),
        encoding="utf-8")
    with pytest.raises(pool.PoolContractError, match="does not match"):
        pool._sealed_execution_policy(
            {"action_key": other, "cas_root": cas.root}, None)


def test_a_flush_that_outlives_its_bound_ends_at_the_bound(monkeypatch):
    now = [100.0]
    calls = []
    def advance(readables, writables, exceptional, timeout):
        calls.append(timeout)
        now[0] += timeout
        return [], [], []
    monkeypatch.setattr(pool, 'time', SimpleNamespace(monotonic=lambda: now[0]))
    monkeypatch.setattr(pool, 'select', SimpleNamespace(select=advance))
    assert pool._wait_for_pidfd_exit(123, deadline=100.125) is False
    assert calls == pytest.approx([0.05, 0.05, 0.025])

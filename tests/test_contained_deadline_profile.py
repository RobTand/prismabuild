"""A contained deadline must not destroy the partial sample evidence (#1401).

The broker's stop is ``cgroup.kill`` (``tools/fleet/resource_broker.py``,
``SystemdBackend.stop``): every process in the attempt's scope is SIGKILLed, so
the worker never reaches ``core._reap_and_settle`` and the sampler is never
asked to flush.  The original report (action ``c105a000f225``) is exactly that:
a contained 900 s timeout whose ending carries a ``resource_profile`` and no
``profile`` field at all, while the attempt's ``resource_telemetry`` says the
scope was stopped for ``timeout``.

This test runs the real pool deadline path against a real worker subprocess
and a sampler that writes a valid speedscope only on its genuine flush signal
(SIGINT).  The scope double performs the same destructive stop the broker
does.  Today the sampler is killed with the worker, the status sidecar is
never written, and the ending carries no profile; the regression is that a
contained deadline must preserve the partial profile and still end as a
timeout with no application receipt.
"""

from __future__ import annotations

import json
import os
import select
import signal
import sys
import time
from pathlib import Path

from prismabuild import core as pb
from prismabuild import pool, resource_scope


#: The action's own work: long enough that only the deadline can end it.
_SLEEP_WORK = "import time\ntime.sleep(120)\n"

#: A stand-in for py-spy: it writes the speedscope only on SIGINT and nothing
#: on SIGTERM, which is the measured 0.4.2 behaviour the settle path is built
#: around.  The marker path travels in argv rather than the environment,
#: because the action tree runs under the sealed environment, which carries
#: none of this test's variables; the marker records which signal the sampler
#: actually saw, so a test can tell "the flush hook was reached" from "the
#: process was killed".
_FAKE_SAMPLER = r'''
import json, os, signal, subprocess, sys

profile_path, marker, argv = sys.argv[1], sys.argv[2], sys.argv[4:]


def note(event):
    with open(marker, "a") as handle:
        handle.write(event + "\n")


def flush(_signum, _frame):
    note("SIGINT")
    document = {
        "$schema": "https://www.speedscope.app/file-format-schema.json",
        "shared": {"frames": [{"name": "work"}]},
        "profiles": [{
            "type": "sampled", "name": "fake", "unit": "seconds",
            "startValue": 0, "endValue": 1.0,
            "samples": [[0]], "weights": [1.0],
        }],
        "activeProfileIndex": 0,
        "exporter": "fake-sampler",
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
note("started")
sys.exit(child.wait())
'''

#: The same sampler, but its flush never finishes: it records that the flush
#: signal arrived and then outlives the pool's bounded opportunity, so the
#: hard stop must still end the attempt with no profile claim.
_DEAF_SAMPLER = r'''
import json, os, signal, subprocess, sys, time

profile_path, marker, argv = sys.argv[1], sys.argv[2], sys.argv[4:]


def note(event):
    with open(marker, "a") as handle:
        handle.write(event + "\n")


def flush(_signum, _frame):
    note("SIGINT")
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    time.sleep(30)


def refuse(_signum, _frame):
    note("SIGTERM")
    sys.exit(0)


signal.signal(signal.SIGINT, flush)
signal.signal(signal.SIGTERM, refuse)
child = subprocess.Popen(argv)
note("started")
sys.exit(child.wait())
'''

#: Registers the sampler as a profile backend inside every interpreter the
#: action tree starts.  The real worker, the real relay and the real action
#: all run; only the profiler binary is a stand-in.
_SITECUSTOMIZE = r'''
import os, signal, sys

src = os.environ.get("PB_TEST_1401_SRC")
sampler = os.environ.get("PB_TEST_1401_SAMPLER")
marker = os.environ.get("PB_TEST_1401_MARKER", "")
if src and sampler:
    if src not in sys.path:
        sys.path.insert(0, src)
    try:
        from prismabuild import core
    except Exception:
        core = None
    if core is not None and "flush-on-signal" not in core.PROFILE_BACKENDS:
        class _FlushOnSignalBackend:
            mode = "flush-on-signal"
            name = "fake-sampler"
            flush_signal = signal.SIGINT
            flush_seconds = 3.0
            profile_suffix = "speedscope.json"

            def locate(self):
                return sampler

            @property
            def version(self):
                return "fake-sampler 1.0"

            def launch_argv(self, argv, *, profile_path):
                return [sys.executable, sampler, str(profile_path), marker,
                        "--", *argv]

            def read_profile(self, path):
                return core.read_speedscope(path)

        core.PROFILE_BACKENDS["flush-on-signal"] = _FlushOnSignalBackend()
'''


def _cgroup_path() -> Path:
    """This process's own cgroup, so a cgroup-based worker lookup can see it."""

    for line in Path("/proc/self/cgroup").read_text().splitlines():
        parts = line.split(":", 2)
        if len(parts) == 3 and parts[:2] == ["0", ""] and parts[2].startswith("/"):
            return Path("/sys/fs/cgroup") / parts[2].lstrip("/")
    return Path("/sys/fs/cgroup")


def _cgroup_membership_text() -> str:
    """The membership string ``/proc/self/cgroup`` carries for this process.

    ``resource_scope.cgroup_membership`` maps the cgroup root to ``"/."``,
    which no ``/proc/<pid>/cgroup`` record carries; this test hands the real
    membership string to the same scan so the pool's discovery sees the
    worker whether the test runs in a scope or at the root.
    """

    for line in Path("/proc/self/cgroup").read_text().splitlines():
        parts = line.split(":", 2)
        if len(parts) == 3 and parts[:2] == ["0", ""] and parts[2].startswith("/"):
            return parts[2]
    return "/"


class _HardStopScope:
    """The pool's side of the broker stop, with the broker's destructive step.

    ``terminate_owned`` is the only stop the deployed broker offers: freeze
    and ``cgroup.kill``.  The double reproduces that by SIGKILLing this
    attempt's worker and every process group it leads, which is what the
    kernel does to the scope's members.

    The worker is selected by the attempt's own sealed request path, not by
    the action key alone: the same action key can run in two tests at once
    (same bytes, different CAS roots), and a key-only match would let one
    test's double kill the other's worker.  More than one match is refused
    rather than guessed at, so an unproven identity is never killed; a worker
    that is already gone is left alone.
    """

    def __init__(self, request_path: Path) -> None:
        self.request_path = Path(request_path)
        self.cgroup_path = _cgroup_path()
        self.stops: list[str] = []
        self.killed: list[int] = []
        self.ambiguous: list[int] = []
        self.gone = 0
        self.samples: list[dict] = []
        self.telemetry: list[dict] = []

    def wrap_argv(self, argv, *, worker_script):
        return list(argv)

    def _own_worker_pids(self) -> list[int]:
        needle = os.fsencode(str(self.request_path))
        found: list[int] = []
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            try:
                argv = (entry / "cmdline").read_bytes().split(b"\0")
            except OSError:
                continue
            if needle in argv and b"run-local" in argv:
                found.append(int(entry.name))
        return sorted(found)

    # -- what ``_sample_resource_scope`` asks of a live scope ---------------

    def sample(self):
        record = {
            "action_key": self.request_path.stem, "complete": True,
            "oom_local": 0, "sampled_unix": pool._now(),
        }
        self.samples.append(record)
        return record

    def _request(self, op, **extra):
        return {}

    def write_telemetry(self, record):
        self.telemetry.append(dict(record))

    def terminate_owned(self, reason):
        self.stops.append(reason)
        pids = self._own_worker_pids()
        if len(pids) > 1:
            self.ambiguous.append(len(pids))
            return {"stopped": True}
        if not pids:
            self.gone += 1
            return {"stopped": True}
        for pid in pids:
            for pgid in pool.action_process_groups(pid):
                try:
                    os.killpg(pgid, signal.SIGKILL)
                except OSError:
                    pass
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
            self.killed.append(pid)
        return {"stopped": True}


def _claimed(tmp_path: Path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text(_SLEEP_WORK, encoding="utf-8")
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "tests/contained-deadline-profile",
            "definition_version": "v1",
            "task_class": "generation",
            "determinism": "deterministic",
            "artifact_family": "generic",
            "artifact_kind": "generic",
            "argv": [sys.executable, "task.py"],
            "working_directory": ".",
            "result_path": "result",
        },
        "inputs": [],
        "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        # The sealed budget is long; the test's shorter ceiling is what fires,
        # so the sampler is up and sampling before the deadline lands.
        "params": {"execution_timeout_s": 30.0, "profile": "flush-on-signal"},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {
            "portability": "portable", "platform_key": None, "host_class": None,
        },
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.publish(
        action_key=action["action_key"], cas_root=cas.root,
        checkout_root=checkout,
        worker_script=Path(__file__).resolve().parents[1]
        / "tools" / "prismabuild_worker.py",
        max_attempts=1, retry_safe=False,
    )
    item = queue.claim()
    assert item is not None
    return queue, item, cas, checkout


def test_a_contained_deadline_preserves_the_partial_sample_profile(
    tmp_path: Path, monkeypatch
) -> None:
    queue, item, cas, _checkout = _claimed(tmp_path)
    key = item["action_key"]
    marker = tmp_path / "sampler.events"
    sampler = tmp_path / "fake_sampler.py"
    sampler.write_text(_FAKE_SAMPLER, encoding="utf-8")
    inject = tmp_path / "inject"
    inject.mkdir()
    (inject / "sitecustomize.py").write_text(_SITECUSTOMIZE, encoding="utf-8")
    monkeypatch.setenv(
        "PB_TEST_1401_SRC", str(Path(pb.__file__).resolve().parents[1]))
    monkeypatch.setenv("PB_TEST_1401_SAMPLER", str(sampler))
    monkeypatch.setenv("PB_TEST_1401_MARKER", str(marker))
    existing = os.environ.get("PYTHONPATH", "")
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(
        part for part in (str(inject), existing) if part))

    request_path = cas.root / "requests" / key[:2] / f"{key}.json"
    scope = _HardStopScope(request_path)
    monkeypatch.setattr(queue, "_start_resource_scope", lambda item: scope)
    # ``cgroup_membership`` maps the cgroup root to ``"/."``; hand the real
    # membership string to the same scan the pool uses.
    monkeypatch.setattr(
        resource_scope, "cgroup_membership",
        lambda path: _cgroup_membership_text())

    outcome = queue.execute(
        item, timeout_s=5.0, containment=True,
        heartbeat_s=0.25, timeout_grace_s=5.0,
    )

    # The sampler was genuinely running: without this the failure below could
    # be a slow box rather than the defect under test.
    events = marker.read_text(encoding="utf-8").splitlines() if marker.exists() else []
    assert "started" in events, (
        f"the fake sampler never started, so this run did not exercise the "
        f"contained deadline path: {events}")

    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "execution_deadline"
    assert outcome["returncode"] is None, "a timeout is the worker's verdict"
    assert scope.stops == ["timeout"]
    assert scope.samples, "the pool never sampled the scope before the deadline"
    assert scope.ambiguous == [], (
        f"the hard-stop double found more than one worker for this attempt's "
        f"request path and refused to kill any: {scope.ambiguous}")
    settle = outcome.get("profile_settle")
    assert isinstance(settle, dict) and settle["settled"] is True, settle
    assert 0.0 <= float(settle["elapsed_s"]) <= 5.0, settle

    profile = outcome.get("profile")
    assert isinstance(profile, dict), (
        "the contained deadline killed the worker before it could flush the "
        f"sampler; the ending carries no partial profile (sampler saw "
        f"{events})")
    assert profile["partial"] is True
    assert profile["produced"] is True
    assert profile["action_phase"] == "launched"
    assert profile["samples"] == 1
    blob = (cas.root / "blobs" / str(profile["blob_sha256"])[:2]
            / str(profile["blob_sha256"]))
    document = json.loads(blob.read_text(encoding="utf-8"))
    assert document["$schema"] == pb.PROFILE_SPEEDSCOPE_SCHEMA
    assert "SIGINT" in events, (
        "the sampler was stopped without its flush signal, so no partial "
        f"sample was ever produced: {events}")

    # The deadline is still the verdict, and no application success was filed.
    request = json.loads(
        (cas.root / "requests" / key[:2] / f"{key}.json").read_text(
            encoding="utf-8"))
    assert cas.lookup(request) is None


def test_a_flush_that_outlives_its_bound_still_reaches_the_hard_stop(
    tmp_path: Path, monkeypatch
) -> None:
    """A refused or hung flush cannot prevent the exact-attempt hard stop."""

    queue, item, cas, _checkout = _claimed(tmp_path)
    key = item["action_key"]
    marker = tmp_path / "sampler.events"
    sampler = tmp_path / "deaf_sampler.py"
    sampler.write_text(_DEAF_SAMPLER, encoding="utf-8")
    inject = tmp_path / "inject"
    inject.mkdir()
    (inject / "sitecustomize.py").write_text(_SITECUSTOMIZE, encoding="utf-8")
    monkeypatch.setenv(
        "PB_TEST_1401_SRC", str(Path(pb.__file__).resolve().parents[1]))
    monkeypatch.setenv("PB_TEST_1401_SAMPLER", str(sampler))
    monkeypatch.setenv("PB_TEST_1401_MARKER", str(marker))
    existing = os.environ.get("PYTHONPATH", "")
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(
        part for part in (str(inject), existing) if part))

    request_path = cas.root / "requests" / key[:2] / f"{key}.json"
    scope = _HardStopScope(request_path)
    monkeypatch.setattr(queue, "_start_resource_scope", lambda item: scope)
    # ``cgroup_membership`` maps the cgroup root to ``"/."``; hand the real
    # membership string to the same scan the pool uses.
    monkeypatch.setattr(
        resource_scope, "cgroup_membership",
        lambda path: _cgroup_membership_text())

    outcome = queue.execute(
        item, timeout_s=5.0, containment=True,
        heartbeat_s=0.25, timeout_grace_s=2.0,
    )

    events = marker.read_text(encoding="utf-8").splitlines() if marker.exists() else []
    assert "started" in events, f"the deaf sampler never started: {events}"
    assert "SIGINT" in events, (
        f"the contained deadline never asked the sampler to flush: {events}")
    assert outcome["status"] == "timeout"
    assert outcome["returncode"] is None
    assert scope.stops == ["timeout"]
    assert outcome.get("profile") is None, (
        "a flush that outlived its bound must not produce a profile claim")
    settle = outcome.get("profile_settle")
    assert isinstance(settle, dict) and settle["settled"] is False, settle
    assert float(settle["elapsed_s"]) <= 2.5, (
        f"the flush opportunity outlived its shared grace: {settle}")


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
    monkeypatch.setattr(pool, "_sealed_profile_requested", lambda i: True)
    monkeypatch.setattr(
        pool.os, "pidfd_open", lambda pid, flags: opened.append(pid) or 7)
    monkeypatch.setattr(
        pool.signal, "pidfd_send_signal",
        lambda fd, sig: signals.append((fd, sig)))
    result = pool._settle_contained_profile(
        _Scope(), worker, item={"action_key": "a" * 64, "cas_root": "/cas"},
        deadline=time.monotonic() + 1.0)
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
    closed: list[int] = []
    real_close = os.close
    monkeypatch.setattr(
        pool, "_scope_directory_identity", lambda path: directory)
    monkeypatch.setattr(
        pool, "_contained_worker_start_ticks", lambda pid: next(ticks))
    monkeypatch.setattr(
        pool, "_contained_worker_pid", lambda c, a, *, directory: 4242)
    monkeypatch.setattr(pool.os, "pidfd_open", lambda pid, flags: 7)
    monkeypatch.setattr(
        pool.os, "close", lambda fd: (closed.append(fd), real_close(fd))[1])
    monkeypatch.setattr(
        pool.signal, "pidfd_send_signal",
        lambda fd, sig: signals.append((fd, sig)))

    # A pid the kernel recycled across the open: start ticks moved.
    ticks = iter([100, 999])
    assert pool._contained_worker_pidfd(
        4242, cgroup, worker, directory=directory) is None
    assert closed == [7]
    assert signals == []

    # A worker that vanished across the open: no unique identity remains.
    ticks = iter([100, 100])
    monkeypatch.setattr(
        pool, "_contained_worker_pid", lambda c, a, *, directory: None)
    assert pool._contained_worker_pidfd(
        4242, cgroup, worker, directory=directory) is None
    assert closed == [7, 7]

    # An identity that cannot be read after the open closes the pidfd.
    ticks = iter([100, 100])

    def explode(c, a, *, directory):
        raise OSError("recheck denied")

    monkeypatch.setattr(pool, "_contained_worker_pid", explode)
    assert pool._contained_worker_pidfd(
        4242, cgroup, worker, directory=directory) is None
    assert closed == [7, 7, 7]
    assert signals == []

    # A kernel without pidfds refuses; there is no raw os.kill fallback.
    monkeypatch.setattr(
        pool, "_contained_worker_pid", lambda c, a, *, directory: 4242)
    monkeypatch.delattr(pool.os, "pidfd_open", raising=False)
    assert pool._contained_worker_pidfd(
        4242, cgroup, worker, directory=directory) is None
    assert closed == [7, 7, 7]
    assert signals == []


def test_the_flush_opportunity_is_gated_and_fails_closed(monkeypatch) -> None:
    """Unprofiled, expired and pidfd-less paths fall through to the hard stop."""

    class _Scope:
        cgroup_path = "/sys/fs/cgroup/scope"

    item = {"action_key": "a" * 64, "cas_root": "/cas"}
    discovered: list[int] = []
    monkeypatch.setattr(pool, "_scope_directory_identity", lambda path: (1, 2))
    monkeypatch.setattr(
        pool, "_contained_worker_pid",
        lambda c, a, *, directory: (discovered.append(1), 4242)[1])
    monkeypatch.setattr(
        pool, "_contained_worker_pidfd",
        lambda p, c, a, *, directory: None)
    monkeypatch.setattr(pool, "_sealed_profile_requested", lambda i: False)

    assert pool._settle_contained_profile(
        _Scope(), ["w"], item=item,
        deadline=time.monotonic() + 1.0) is None
    assert discovered == []

    # An expired grace refuses before discovery.
    monkeypatch.setattr(pool, "_sealed_profile_requested", lambda i: True)
    expired = pool._settle_contained_profile(
        _Scope(), ["w"], item=item, deadline=time.monotonic() - 1.0)
    assert expired is not None and expired["settled"] is False
    assert "grace expired" in str(expired["refused"])
    assert discovered == []

    # Discovery that raises before any open is a refusal, not an escape.
    def explode(c, a, *, directory):
        raise RuntimeError("census broke")

    monkeypatch.setattr(pool, "_contained_worker_pid", explode)
    broke = pool._settle_contained_profile(
        _Scope(), ["w"], item=item, deadline=time.monotonic() + 1.0)
    assert broke is not None and broke["settled"] is False
    assert "worker discovery failed" in str(broke["refused"])

    # A pidfd that cannot prove the worker records the refusal.
    monkeypatch.setattr(
        pool, "_contained_worker_pid",
        lambda c, a, *, directory: (discovered.append(1), 4242)[1])
    refused = pool._settle_contained_profile(
        _Scope(), ["w"], item=item, deadline=time.monotonic() + 1.0)
    assert refused is not None and refused["settled"] is False
    assert refused["worker_pid"] == 4242
    assert "pidfd" in str(refused["refused"])
    assert discovered == [1]


def test_an_unreadable_sealed_request_is_not_a_profile() -> None:
    """A request this process cannot read or verify refuses the opportunity."""

    key = "a" * 64
    assert pool._sealed_profile_requested({"action_key": key}) is False
    assert pool._sealed_profile_requested(
        {"action_key": key, "cas_root": "/"}) is False
    assert pool._sealed_profile_requested(
        {"action_key": key, "cas_root": "relative/cas"}) is False


def test_a_flush_that_outlives_its_bound_ends_at_the_bound(monkeypatch) -> None:
    read_end, write_end = os.pipe()
    calls: list[float] = []
    real_select = select.select

    class _Select:
        @staticmethod
        def select(readables, writables, exceptional, timeout):
            calls.append(timeout)
            return real_select(readables, writables, exceptional, timeout)

    monkeypatch.setattr(pool, "select", _Select)
    try:
        started = time.monotonic()
        assert pool._wait_for_pidfd_exit(
            read_end, deadline=started + 0.1) is False
        elapsed = time.monotonic() - started
    finally:
        os.close(read_end)
        os.close(write_end)
    assert 0.1 <= elapsed < 2.0
    # Seconds, not milliseconds: a 1 ms poll would spin ~100 times here.
    assert calls and all(0 < value <= 0.06 for value in calls), calls
    assert len(calls) <= 5, calls

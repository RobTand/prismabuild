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
import signal
import sys
from pathlib import Path

from prismabuild import core as pb
from prismabuild import pool


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


class _HardStopScope:
    """The pool's side of the broker stop, with the broker's destructive step.

    ``terminate_owned`` is the only stop the deployed broker offers: freeze
    and ``cgroup.kill``.  The double reproduces that by SIGKILLing the exact
    attempt's worker and every process group it leads, which is what the
    kernel does to the scope's members.
    """

    def __init__(self, action_key: str) -> None:
        self.action_key = action_key
        self.cgroup_path = _cgroup_path()
        self.stops: list[str] = []
        self.killed: list[int] = []

    def wrap_argv(self, argv, *, worker_script):
        return list(argv)

    def terminate_owned(self, reason):
        self.stops.append(reason)
        for pid in pool.find_launcher_pids(self.action_key):
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
    return queue, item, cas


def test_a_contained_deadline_preserves_the_partial_sample_profile(
    tmp_path: Path, monkeypatch
) -> None:
    queue, item, cas = _claimed(tmp_path)
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

    scope = _HardStopScope(key)
    monkeypatch.setattr(queue, "_start_resource_scope", lambda item: scope)

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

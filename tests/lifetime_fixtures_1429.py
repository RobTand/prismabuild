"""Shared fixtures for the lifetime contract tests (#1429).

Real queue, ledgers, materializer, launcher and broker authority. Only the
kernel half of the broker is simulated, so a scope can be made to stay
populated after its stop, and clocks and stalls are injected by the tests.
"""
from __future__ import annotations

import json
import platform
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
sys.path[:0] = [str(ROOT / "tests")]
import resource_broker  # noqa: E402
from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402
from prismabuild import (  # noqa: E402
    core as pb, lifetime_acceptance, lifetime_fence, pool, resource_scope)
from test_pool import _test_checkout_snapshot  # noqa: E402

#: A fence the new seal and the old one both accept. The tests move a
#: controlled clock; none of them waits a fence out.
FENCE_S = 300.0
WORKER = ROOT / "tools" / "prismabuild_worker.py"
TASK_OK = "open('result', 'w').write('ok')\n"
TASK_SLEEPS = ("import os, time\n"
               "open('pid', 'w').write(str(os.getpid()))\n"
               "time.sleep(120)\n")


def alive(pid: int) -> bool:
    try:
        state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
    except (FileNotFoundError, ProcessLookupError, IndexError):
        return False
    return state != "Z"


def wait_for(predicate, timeout_s: float = 10.0) -> bool:
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def seal(checkout: Path, params: dict) -> dict:
    return pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/lifetime-contract", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"],
                 "working_directory": ".", "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": params,
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })


def lifetime_param(fence_s: float = FENCE_S) -> dict:
    return {"lifetime": {"schema": lifetime_fence.LIFETIME_SCHEMA_V1, "fence_s": fence_s}}


def fenced_queue(tmp_path: Path, task_body: str = TASK_OK, *, fence_s: float = FENCE_S,
                 max_attempts: int = 1):
    """A private queue holding one published fenced action.

    One attempt, as ``pbrun`` submits unless the caller asks for retries.
    """

    checkout = tmp_path / "checkout"
    checkout.mkdir(exist_ok=True)
    (checkout / "task.py").write_text(task_body)
    action = seal(checkout, lifetime_param(fence_s))
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "queue"), capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
                  checkout_root=checkout, worker_script=WORKER,
                  max_attempts=max_attempts)
    return queue, action


def claim(queue: AdmittedQueueFixture) -> dict:
    item = queue.queue.claim(capacity={"cpu": 8, "mem_gb": 16},
                             tags=[lifetime_fence.LIFETIME_TAG])
    assert item is not None
    return item


def fenced_snapshot_queue(tmp_path: Path, *, fence_s: float = FENCE_S):
    """A fenced action whose checkout is a materialized snapshot."""

    source = tmp_path / "source"
    source.mkdir()
    for args in (["init", "-q"], ["config", "user.name", "PrismaBuild test"],
                 ["config", "user.email", "test@example.invalid"]):
        subprocess.run(["git", "-C", str(source), *args], check=True)
    (source / "task.py").write_text(TASK_OK)
    subprocess.run(["git", "-C", str(source), "add", "task.py"], check=True)
    subprocess.run(["git", "-C", str(source), "commit", "-qm", "sealed source"], check=True)
    identity = pb.git_checkout_identity(source)
    stamp_name = f"{pb.PBRUN_STAMP_PREFIX}test.json"
    (source / stamp_name).write_text(
        json.dumps({"cwd": ".", **identity}, indent=1, sort_keys=True))
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    snapshot = _test_checkout_snapshot(tmp_path, source, stamp_name, cas)
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                 "task_class": "generation", "determinism": "stochastic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [snapshot["input"]],
        "code_closure": pb.build_code_closure(source, [stamp_name]),
        "params": {"command": [sys.executable, "task.py"], "cwd": ".",
                   "demand": {"cpu": 1, "mem_gb": 1}, "checkout_snapshot": snapshot,
                   **lifetime_param(fence_s)},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas.publish_action_request(action)
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "queue"), capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
                  checkout_snapshot=snapshot, worker_script=WORKER, max_attempts=1)
    return queue, action, cas


def publish_fenced(fleet, name: str, *, fence_s: float = FENCE_S, extra_variables=None):
    """Publish a fenced GPU-sharing candidate through the real fleet queue."""

    queue = fleet[0]
    checkout = queue.root.parent / "checkout"
    cas = pb.PrismaBuildCAS(queue.root.parent / "cas")
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/gpu-backfill", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"],
                 "working_directory": ".", "result_path": name},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {"gpu_exclusive": False, "execution_timeout_s": 120,
                   **lifetime_param(fence_s)},
        "environment": {"variables": dict(extra_variables or {}), "toolchain": {
            **pb.executable_toolchain_contract(sys.executable),
            "system": platform.system(), "machine": platform.machine(),
            "libc": "-".join(platform.libc_ver())}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas.publish_action_request(action)
    key = action["action_key"]
    queue.publish(action_key=key, cas_root=str(cas.root), checkout_root=str(checkout),
                  worker_script="worker.py", resources={"cpu": 2, "gpu": 1, "mem_gb": 8},
                  needs_gpu=True, tags=["sparklina"], priority=-10, max_attempts=3)
    return key


def claim_on_host(fleet, host: str = "sparklina"):
    """Claim as a loop that offers the lifetime capability would."""

    queue = fleet[0]
    result = queue.claim(
        capacity={"cpu": 20, "gpu": 1, "mem_gb": 120}, has_gpu=True,
        cpu_tiers={"preferred": list(range(20)), "fallback": []}, adaptive_cpu=True,
        tags=["gb10", host, lifetime_fence.LIFETIME_TAG])
    return None if result is None else result["action_key"]


def accept_assumption(queue) -> dict:
    """Record a person's acceptance in the queue root, as the command does."""

    return lifetime_acceptance.record_acceptance(
        queue.root, accepted_by="a test person",
        authority="the lifetime fixtures record this decision")


def good_log(published_unix: float = 1000.0, fence_s: float = FENCE_S,
             *, mechanisms=None, ended=None) -> lifetime_fence.AttemptLog:
    """A complete, enforced record; tests break exactly one phase of it."""

    fence_clock = lifetime_fence.clock(published_unix=published_unix, fence_s=fence_s)
    log = lifetime_fence.AttemptLog(fence_clock, {"action_key": "a" * 64})
    for component in lifetime_fence.COMPONENTS:
        log.end(component.phase,
                mechanism=(mechanisms or {}).get(component.phase, component.mechanism),
                evidence=f"{component.phase}-evidence",
                ended_unix=(ended or {}).get(
                    component.phase, fence_clock.bound(component.phase) - 1.0))
    return log


class FakeKernel:
    """The kernel half of the broker: groups that stop, and sometimes do not."""

    def __init__(self) -> None:
        self.groups: dict[str, dict[str, bool]] = {}
        #: A stopped scope stays populated: a task in uninterruptible sleep.
        self.stuck = False
        #: The stop itself fails.
        self.stop_error: Exception | None = None
        self.calls: list[tuple[str, str]] = []

    def create(self, scope, budget):
        self.calls.append(("create", scope))
        self.groups[scope] = {"populated": False}
        return {"cgroup_path": f"/sys/fs/cgroup/prismabuild.slice/{scope}"}

    def stop(self, scope):
        self.calls.append(("stop", scope))
        if self.stop_error is not None:
            raise self.stop_error
        self.groups[scope]["populated"] = self.stuck

    def empty(self, scope):
        return scope not in self.groups or not self.groups[scope]["populated"]

    def exists(self, scope):
        return scope in self.groups

    def release(self, scope):
        self.calls.append(("release", scope))
        if self.groups[scope]["populated"]:
            raise ValueError("scope still populated")
        self.groups.pop(scope)


class Broker:
    """A real broker authority and socket server over a :class:`FakeKernel`."""

    def __init__(self, tmp_path: Path, monkeypatch) -> None:
        self.kernel = FakeKernel()
        self.authority = resource_broker.Authority(
            tmp_path / "broker-state", __import__("os").getuid(), self.kernel,
            max_memory_bytes=64 * 1024 ** 3)
        # A socket path holds 107 bytes, and a sharded pytest run puts a test
        # directory far deeper than that; a short private directory does not.
        self.socket_dir = Path(tempfile.mkdtemp(prefix="pb1429-"))
        self.endpoint = self.socket_dir / "broker.sock"
        self.server = resource_broker.Server(str(self.endpoint), resource_broker.Handler)
        self.server.authority = self.authority
        self.thread = threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        self.thread.start()
        # The pool reaches the broker through the scope's default socket, bound
        # when the class was defined, and checks a recovered scope against the
        # module's socket; point both at this server.
        monkeypatch.setattr(resource_scope, "BROKER_SOCKET", self.endpoint)
        monkeypatch.setitem(
            resource_scope.ResourceScope.__init__.__kwdefaults__,
            "socket_path", self.endpoint)
        # The payload runs as a plain subprocess, outside any simulated group.
        monkeypatch.setattr(
            resource_scope.ResourceScope, "wrap_argv",
            lambda scope, argv, **kwargs: argv)
        # Learning a shape and publishing the adaptive snapshot are not under test.
        monkeypatch.setattr(pool.cpu_admission, "record_completion",
                            lambda *args, **kwargs: None)
        monkeypatch.setattr(pool.cpu_admission.adaptive_snapshot, "publish",
                            lambda *args, **kwargs: None)

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        shutil.rmtree(self.socket_dir, ignore_errors=True)

    def record(self, key: str) -> dict:
        """The broker's own record of the one scope this action made."""

        (scope,) = [name for name, record in self.authority.records.items()
                    if record["action_key"] == key]
        return self.authority.records[scope]

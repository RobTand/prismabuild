"""Actual adapter/core execution through a private broker and kernel service model."""
from contextlib import contextmanager
from functools import partial
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import threading

from prismabuild import core as pb, pool, resource_scope
import resource_broker


class Kernel:
    def __init__(self, root):
        self.root, self.groups, self.calls = root, {}, []

    def create(self, scope, budget):
        directory = self.root / scope
        directory.mkdir(parents=True)
        for name, value in {"cpu.stat": "usage_usec 0\n", "memory.events": "oom_kill 0\n",
                            "memory.events.local": "oom 0\n", "memory.current": "0",
                            "memory.peak": "0"}.items():
            (directory / name).write_text(value)
        info = directory.stat()
        self.groups[scope] = {"process": None, "frozen": False, "identity": [info.st_dev, info.st_ino]}
        self.calls.append({"op": "create", "scope": scope, "budget": budget})
        return {"cgroup_path": str(Path("/sys/fs/cgroup/prismabuild.slice") / scope),
                "cgroup_identity": self.groups[scope]["identity"]}

    def run(self, scope, uid, command, stdio):
        assert uid == os.getuid()
        affinity = command["affinity"]
        assert set(affinity) <= os.sched_getaffinity(0)
        self.calls.append({"op": "run", "scope": scope, "argv": command["argv"], "affinity": affinity})
        process = subprocess.Popen(
            ["/usr/bin/taskset", "--cpu-list", ",".join(map(str, affinity)), *command["argv"]],
            cwd=command["cwd"], env=command["env"], stdin=stdio[0], stdout=stdio[1], stderr=stdio[2],
            start_new_session=True, umask=command.get("umask", -1))
        self.groups[scope]["process"] = process
        return process

    def empty(self, scope):
        row = self.groups.get(scope)
        return row is None or row["process"] is None or row["process"].poll() is not None

    def stop(self, scope):
        self.calls.append({"op": "stop", "scope": scope})
        row = self.groups[scope]
        row["frozen"] = True
        if not self.empty(scope):
            os.killpg(row["process"].pid, signal.SIGKILL)
            row["process"].wait(timeout=5)

    def exists(self, scope):
        return scope in self.groups

    def observe(self, scope):
        row = self.groups.get(scope)
        return None if row is None else {"populated": not self.empty(scope), "frozen": row["frozen"],
                                        "identity": row["identity"]}

    def inventory(self):
        return {scope: self.observe(scope) for scope in self.groups}

    def healthy(self):
        return True

    def reclaim(self, scope):
        return {"before": 0, "after": 0, "complete": True, "page_bytes_after": 0}

    def release(self, scope):
        assert self.empty(scope)
        self.calls.append({"op": "release", "scope": scope})
        self.groups.pop(scope)
        shutil.rmtree(self.root / scope)


@contextmanager
def private_broker(root):
    kernel = Kernel(root / "cgroups")
    with tempfile.TemporaryDirectory(prefix="pb-adapter-", dir="/tmp") as directory:
        socket_path = Path(directory) / "broker.sock"
        authority = resource_broker.Authority(root / "broker-state", os.getuid(), kernel,
                                              max_memory_bytes=1 << 30)
        server = resource_broker.Server(str(socket_path), resource_broker.Handler)
        server.authority = authority
        original_socket = resource_scope.BROKER_SOCKET
        original_defaults = resource_scope.ResourceScope.__init__.__kwdefaults__
        original_read = resource_scope.read_cgroup
        resource_scope.BROKER_SOCKET = socket_path
        resource_scope.ResourceScope.__init__.__kwdefaults__ = {**original_defaults, "socket_path": socket_path}
        resource_scope.read_cgroup = lambda path: original_read(kernel.root / path.name)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        try:
            yield kernel
        finally:
            server.shutdown()
            server.server_close()
            thread.join(5)
            assert not thread.is_alive()
            resource_scope.BROKER_SOCKET = original_socket
            resource_scope.ResourceScope.__init__.__kwdefaults__ = original_defaults
            resource_scope.read_cgroup = original_read
            (root / "broker-calls.json").write_text(json.dumps(kernel.calls))
            for scope in list(kernel.groups):
                kernel.stop(scope)
                kernel.release(scope)


def prepare(root, repository):
    checkout = root / "payload-checkout"
    checkout.mkdir()
    code = checkout / "payload.py"
    code.write_text('''import json, os
from pathlib import Path
Path(os.environ["MARKER"]).write_text(json.dumps({
    "pid": os.getpid(), "affinity": sorted(os.sched_getaffinity(0)),
    "action_key": os.environ["PRISMABUILD_ACTION_KEY"],
    "nonce": os.environ["PRISMABUILD_ACTION_NONCE"],
    "scope": os.environ["PRISMABUILD_ACTION_SCOPE"]}))
Path("result.txt").write_text("real child\\n")
''')
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/adapter", "definition_version": "v1",
                 "task_class": "generation", "determinism": "stochastic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": ["/usr/bin/python3", "payload.py"], "working_directory": ".", "result_path": "result.txt"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["payload.py"]),
        "params": {"demand": {"cpu": 1, "mem_gb": 1}},
        "environment": {"variables": {"MARKER": str(root / "payload.json"), "OMP_NUM_THREADS": "1",
                                      "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None, "host_class": None},
    })
    cas = pb.PrismaBuildCAS(root / "cas")
    cas.publish_action_request(action)
    queue = pool.PoolQueue(root / "pb-queue")
    queue.publish(action_key=action["action_key"], cas_root=cas.root, checkout_root=checkout,
                  worker_script=repository / "tools" / "prismabuild_worker.py",
                  resources={"cpu": 1, "mem_gb": 1}, max_attempts=1)
    return queue, cas, action


def run_adapter(root, repository):
    import sys
    import worker
    import worker_loop
    from prismabuild import adaptive_gpu, generation_receipt, role_session

    worker.SH = worker_loop.SH = root
    worker_loop.PUBLICATION_LOCK_ROOT = root / "publication-lock"
    worker_loop.container_images.InventoryCache = partial(worker_loop.container_images.InventoryCache,
                                                         root=root / "image-inventory")
    adaptive_gpu.trusted_sample.__defaults__ = (root / "absent-gpu-sample.json",)
    role_session.GENERATION_VERSION = root / "loaded.json"
    role_session.RUNTIME_VERSION = root / "published.json"
    role_session.MAINTENANCE_GATE = root / "maintenance.json"
    role_session.PARKED_ROOT = root / "parked"
    role_session.ROLE_LOCK_ROOT = root / "role-locks"
    affinity = sorted(os.sched_getaffinity(0))
    (root / "process.json").write_text(json.dumps({"pid": os.getpid(), "affinity": affinity,
        "sources": {name: str(Path(module.__file__).resolve()) for name, module in (
            ("worker", worker), ("worker_loop", worker_loop), ("role_session", role_session),
            ("generation_receipt", generation_receipt), ("pool", pool), ("core", pb))}}))

    def observed_call(frame, event, value):
        if event == "call" and frame.f_code == worker_loop.main.__code__:
            (root / "invocation.json").write_text(json.dumps(frame.f_locals["argv"]))

    with private_broker(root):
        sys.setprofile(observed_call)
        try:
            code = worker.main()
        finally:
            sys.setprofile(None)
    assert sorted(os.sched_getaffinity(0)) == affinity
    return code

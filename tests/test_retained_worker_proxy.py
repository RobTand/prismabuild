"""Retained worker runtime keeps its own sealed proxy on the real launch (PB R4).

The pool builds worker argv from the sealed ``worker_script`` (a retained
action names an earlier immutable runtime), then -- under CPU affinity --
prefixes ``taskset`` BEFORE ``scope.wrap_argv``. The wrapper therefore never
parses argv for a runtime; the pool carries ``worker_script`` explicitly and
the proxy comes from that same runtime only when the generation store
receipt, manifest digests, and sealed bits prove it. The trust rule mirrors
``supervise._proven_roots`` / ``supervise._published_generation`` (direct
non-staging store child, receipt names the generation) and
``publish_runtime._barrier_generation`` (40-hex commit, safe member paths,
no symlinks, sealed files, digest match). A self-made manifest outside that
authority refuses; dev stubs keep the current proxy. Run via published
pbtest at -10.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import resource_scope  # noqa: E402

KEY = "c" * 64
NONCE = "b" * 32
RECEIPT_SCHEMA = "prismaquant.prismabuild.runtime_version.v1"
COMMIT = "ab" * 20

_CHECKOUT = Path(__file__).resolve().parents[1]


def _copy_bytes(root: Path, rel: str, source: Path) -> None:
    dst = root / rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(source.read_bytes())


def _seal_tree(root: Path, members: dict[str, str]) -> None:
    (root / "RUNTIME_VERSION.json").write_text(json.dumps({
        "schema": RECEIPT_SCHEMA, "generation": root.name,
        "commit": COMMIT, "files": members}))
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or not path.exists():
            continue
        if path.is_dir():
            path.chmod(0o555)
        else:
            path.chmod(0o444)
    root.chmod(0o555)


def _published_generation(store: Path, name: str, *,
                          layout: str = "published") -> Path:
    """A sealed fixture generation under the fixture store.

    ``published`` carries both proxy spellings with identical bytes (as
    ``_publication_manifest`` dual-lists them); ``checkout`` carries only
    the ``tools/fleet/`` spelling. Both carry the sealed worker, the
    proxy's own imports, and a standalone core for the authority-chain
    check.
    """

    root = store / name
    members: dict[str, str] = {}

    def add(rel: str, source: Path) -> None:
        _copy_bytes(root, rel, source)
        members[rel] = hashlib.sha256((root / rel).read_bytes()).hexdigest()

    add("tools/prismabuild_worker.py",
        _CHECKOUT / "tools" / "prismabuild_worker.py")
    add("src/prismabuild/core.py",
        _CHECKOUT / "src" / "prismabuild" / "core.py")
    # resource_exec.main imports the package before entering the broker
    # scope. Keep that real import closure in the retained fixture too.
    for module in ("__init__", "resource_scope", "progress",
                   "residency_map", "storage_tiers"):
        rel = f"src/prismabuild/{module}.py"
        add(rel, _CHECKOUT / rel)
    fleet_exec = _CHECKOUT / "tools" / "fleet" / "resource_exec.py"
    fleet_broker = _CHECKOUT / "tools" / "fleet" / "resource_broker.py"
    fleet_paths = _CHECKOUT / "tools" / "fleet" / "runtime_paths.py"
    if layout == "published":
        add("tools/resource_exec.py", fleet_exec)
        add("tools/resource_broker.py", fleet_broker)
        add("tools/runtime_paths.py", fleet_paths)
    add("tools/fleet/resource_exec.py", fleet_exec)
    add("tools/fleet/resource_broker.py", fleet_broker)
    add("tools/fleet/runtime_paths.py", fleet_paths)
    _seal_tree(root, members)
    return root


@pytest.fixture
def fleet_store(tmp_path, monkeypatch):
    """A fixture generation store as this fleet's publication authority."""

    store = tmp_path / "fleet" / "runtime-generations"
    store.mkdir(parents=True)
    monkeypatch.setattr(
        resource_scope, "RETAINED_GENERATION_STORE", store)
    return store


def _scope() -> resource_scope.ResourceScope:
    scope = resource_scope.ResourceScope(
        KEY, NONCE, 64 * 1024 ** 2, Path("/tmp/no-telemetry.json"))
    scope.token = "t" * 64
    return scope


def test_affinity_wrapped_argv_keeps_retained_proxy(fleet_store) -> None:
    """The taskset prefix no longer decides the proxy; the runtime does."""

    gen_a = _published_generation(fleet_store, "gen-a-0001")
    worker = str(gen_a / "tools" / "prismabuild_worker.py")
    inner = [sys.executable, worker, "run-local", "--action", "x"]
    # The exact shape PoolQueue._execute_in_checkout builds under affinity.
    affinity = ["/usr/bin/taskset", "--cpu-list", "0-1", *inner]
    wrapped = _scope().wrap_argv(affinity, worker_script=worker)
    assert wrapped[1] == str(gen_a / "tools" / "resource_exec.py")
    assert wrapped[-len(affinity):] == affinity
    assert _scope().wrap_argv(inner, worker_script=worker)[1] == wrapped[1]


def test_checkout_layout_selects_fleet_proxy(fleet_store) -> None:
    """A source-layout retained tree serves its fleet-spelling proxy."""

    gen_a = _published_generation(
        fleet_store, "gen-a-0002", layout="checkout")
    worker = str(gen_a / "tools" / "prismabuild_worker.py")
    wrapped = _scope().wrap_argv(
        [sys.executable, worker, "run-local"], worker_script=worker)
    assert wrapped[1] == str(gen_a / "tools" / "fleet" / "resource_exec.py")


def test_self_made_manifest_outside_store_refuses(tmp_path) -> None:
    """A self-consistent receipt outside the store is not authority."""

    fake = tmp_path / "fake-gen"
    members: dict[str, str] = {}
    for rel, source in (
            ("tools/prismabuild_worker.py",
             _CHECKOUT / "tools" / "prismabuild_worker.py"),
            ("tools/fleet/resource_exec.py",
             _CHECKOUT / "tools" / "fleet" / "resource_exec.py"),
            ("tools/fleet/resource_broker.py",
             _CHECKOUT / "tools" / "fleet" / "resource_broker.py"),
            ("tools/fleet/runtime_paths.py",
             _CHECKOUT / "tools" / "fleet" / "runtime_paths.py")):
        _copy_bytes(fake, rel, source)
        members[rel] = hashlib.sha256((fake / rel).read_bytes()).hexdigest()
    _seal_tree(fake, members)
    with pytest.raises(OSError):
        _scope().wrap_argv(
            [sys.executable, str(fake / "tools" / "prismabuild_worker.py")],
            worker_script=str(fake / "tools" / "prismabuild_worker.py"))


def test_writable_store_member_refuses(fleet_store) -> None:
    """A store tree that was never sealed proves nothing."""

    gen_a = _published_generation(fleet_store, "gen-a-0003")
    # Unseal the selected (flat-layout) proxy: bytes and receipt agree,
    # but the tree was never sealed.
    (gen_a / "tools" / "resource_exec.py").chmod(0o644)
    with pytest.raises(OSError):
        _scope().wrap_argv(
            [sys.executable, str(gen_a / "tools" / "prismabuild_worker.py")],
            worker_script=str(gen_a / "tools" / "prismabuild_worker.py"))


def test_dev_stub_keeps_current_proxy() -> None:
    """Non-runtime shapes keep the established contained path, never raise."""

    current = Path(resource_scope.__file__).resolve().parents[2]
    argv = ["/usr/bin/taskset", "--cpu-list", "0", "/worker.py",
            "run-local", "--action", "x"]
    assert _scope().wrap_argv(argv, worker_script="/worker.py")[1].startswith(
        str(current))
    assert _scope().wrap_argv(["/bin/true"])[1].startswith(str(current))


@pytest.mark.parametrize("fault", ["changed-package-import", "uncovered-scope-import"])
def test_unverified_proxy_package_code_refuses(fleet_store, fault) -> None:
    """The proxy imports package code before it sends the contained command."""
    gen_a = _published_generation(fleet_store, "gen-package-proof")
    if fault == "changed-package-import":
        path = gen_a / "src/prismabuild/__init__.py"
        path.chmod(0o644)
        path.write_text(path.read_text() + "\n# changed after publication\n")
        path.chmod(0o444)
    else:
        path = gen_a / "RUNTIME_VERSION.json"
        receipt = json.loads(path.read_text())
        del receipt["files"]["src/prismabuild/resource_scope.py"]
        path.chmod(0o644)
        path.write_text(json.dumps(receipt))
        path.chmod(0o444)
    worker = str(gen_a / "tools/prismabuild_worker.py")
    with pytest.raises(OSError):
        _scope().wrap_argv([sys.executable, worker, "run-local"],
                           worker_script=worker)


class _LaunchOnlySubprocess:
    """``subprocess`` as pool.py resolves it, with only its ``Popen`` stubbed.

    The launch under test is pool.py's own ``subprocess.Popen`` call.
    Stubbing the global ``subprocess.Popen`` also captured every other
    child this process started meanwhile, and ``(argv,) = seen`` then
    failed on two argvs (#937).  In this harness the adaptive snapshot
    publisher never starts one: ``record_completion`` is stubbed out and
    the claim has no CPU controller, so ``adaptive_snapshot.publish`` is
    never called.  The finish path's GPU power reference does: when the
    box's pqteld recorder (2 Hz on the GB10s) has a row inside the
    action's window, ``box_window.read_window`` asks
    ``gpu_capacity.devices``, whose ``subprocess.run`` builds a ``Popen``
    for ``nvidia-smi``.  A longer window under load makes that row
    likelier, which is why the test passed in isolation.  Everything but
    ``Popen`` resolves to the real module.
    """

    def __init__(self, popen):
        self.Popen = popen

    def __getattr__(self, name):
        return getattr(subprocess, name)


def _contained_harness(monkeypatch, tmp_path, queue, item, seen):
    """Fake broker authority plus a Popen that records the launch argv.

    Only pool.py's ``subprocess`` is replaced (see
    :class:`_LaunchOnlySubprocess`), so ``seen`` holds the launches pool
    starts and nothing another module starts meanwhile.
    """

    def request(scope, op, **extra):
        if op == "create":
            unit = ("prismabuild-job" + hashlib.sha256(
                (scope.action_key + scope.nonce).encode()).hexdigest()[:32]
                + ".slice")
            return {"ok": True, "scope_id": unit, "token": "b" * 64,
                    "cgroup_path": "/sys/fs/cgroup/prismabuild.slice/" + unit}
        return {"ok": True}

    def sample(scope):
        value = {"action_key": scope.action_key, "nonce": scope.nonce,
                 "sampled_unix": pool._now(), "wall_seconds": 1.0,
                 "cpu_seconds": 0.25, "memory_peak_bytes": 1024,
                 "memory_current_bytes": 0, "oom_kill": 0, "complete": True}
        resource_scope._atomic_json(scope.telemetry_path, value)
        return value

    class Process:
        pid = 999999999

        def __init__(self, argv, **kw):
            seen.append(argv)
            self.returncode = 0

        def communicate(self, *, timeout):
            return "ok", ""

        def poll(self):
            return self.returncode

    monkeypatch.setattr(resource_scope.ResourceScope, "_request", request)
    monkeypatch.setattr(resource_scope.ResourceScope, "sample", sample)
    monkeypatch.setattr(pool.cpu_admission, "record_completion",
                        lambda *args: None)
    monkeypatch.setattr(pool, "subprocess", _LaunchOnlySubprocess(Process))


def _retained_item(tmp_path, worker: str):
    checkout = tmp_path / "checkout"
    checkout.mkdir(exist_ok=True)
    (checkout / "task.py").write_text('print("ok")\n')
    demand = {"mem_gb": 2, "cpu": 1}
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/retained-proxy",
                 "definition_version": "v1", "task_class": "generation",
                 "determinism": "deterministic", "artifact_family": "generic",
                 "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"],
                 "working_directory": ".", "result_path": "result"},
        "inputs": [],
        "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {"demand": demand},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
                  checkout_root=checkout, worker_script=worker,
                  resources=demand, max_attempts=2, retry_safe=True)
    return queue, queue.claim(capacity=demand)


@pytest.mark.parametrize("affinity", [False, True])
def test_real_contained_launch_uses_retained_proxy(
        fleet_store, tmp_path, monkeypatch, affinity) -> None:
    """Retained generation A under pool B, through PoolQueue.execute.

    The real ``_execute_in_checkout`` seam carries the sealed worker --
    with the ``taskset`` affinity prefix when allocated -- and the
    wrapped proxy is A's proven runtime, never the executing pool's.
    """

    gen_a = _published_generation(fleet_store, "gen-a-0004")
    worker = str(gen_a / "tools" / "prismabuild_worker.py")
    queue, item = _retained_item(tmp_path, worker)
    if affinity:
        cpu = min(os.sched_getaffinity(0))
        tiers = {"preferred": [cpu], "fallback": []}
        pool._write_json_atomic(
            queue.ledger().base / "cpu-map.json", tiers)
        item["cpu_allocation"] = queue.ledger().cpu_allocation(
            item["action_key"], tiers)
    seen: list = []
    _contained_harness(monkeypatch, tmp_path, queue, item, seen)
    outcome = queue.execute(item, containment=True)
    assert outcome["status"] == "executed"
    assert len(seen) == 1, seen
    (argv,) = seen
    assert argv[1] == str(gen_a / "tools" / "resource_exec.py")
    inner = argv[argv.index("--") + 1:]
    if affinity:
        assert inner[:3] == ["/usr/bin/taskset", "--cpu-list",
                             str(min(os.sched_getaffinity(0)))]
    assert worker in inner


@pytest.mark.parametrize("bystander", ["gpu_power_reference", "snapshot_publisher"])
def test_a_bystander_child_never_reaches_the_launch_capture(
        fleet_store, tmp_path, monkeypatch, bystander) -> None:
    """Another module's child during the launch is not the launch (#937).

    Forces, deterministically, what the broad runs hit by timing:
    ``gpu_power_reference`` is ``read_window`` finding a pqteld row inside
    the action's window, so it asks for the GPU reference and
    ``gpu_capacity`` runs ``nvidia-smi``; ``snapshot_publisher`` is
    ``adaptive_snapshot.publish`` starting its coalesced copy child while
    the launch is under way, which the issue named.  Both run for real.
    RED on the global stub: ``seen`` holds more than one argv.
    """

    gen_a = _published_generation(fleet_store, "gen-a-0004")
    worker = str(gen_a / "tools" / "prismabuild_worker.py")
    queue, item = _retained_item(tmp_path, worker)
    seen: list = []
    _contained_harness(monkeypatch, tmp_path, queue, item, seen)
    if bystander == "gpu_power_reference":
        real_read_window = pool.box_window.read_window
        asked: list = []

        def read_window(start_unix, end_unix, **kwargs):
            # A pqteld row inside the window: the reference is asked for.
            reference = kwargs.get("gpu_reference")
            assert reference is not None
            asked.append(reference(timeout_s=1.0))
            return real_read_window(start_unix, end_unix, **kwargs)

        monkeypatch.setattr(pool.box_window, "read_window", read_window)
    else:
        local, shared = tmp_path / "snapshot-local", tmp_path / "snapshot-shared"
        local.mkdir()
        shared.mkdir()
        (local / "jobs.json").write_text("{}")
        real_execute = pool.PoolQueue._execute_in_checkout
        started: list = []

        def execute_in_checkout(self, *args, **kwargs):
            started.append(pool.cpu_admission.adaptive_snapshot.publish(local, shared))
            return real_execute(self, *args, **kwargs)

        monkeypatch.setattr(pool.PoolQueue, "_execute_in_checkout",
                            execute_in_checkout)
    outcome = queue.execute(item, containment=True)
    assert outcome["status"] == "executed"
    assert len(seen) == 1, seen
    (argv,) = seen
    assert argv[1] == str(gen_a / "tools" / "resource_exec.py")
    assert worker in argv[argv.index("--") + 1:]
    # The bystander really ran: the reference was asked for, and the
    # publisher started a real child, not the stub.
    if bystander == "gpu_power_reference":
        assert len(asked) == 1
    else:
        (child,) = started
        assert child is not None and child.pid != 999999999
        child.wait(timeout=60)


def _proxy_env(proxy: Path, key: str, nonce: str) -> dict:
    """Seeded identity from a REAL proxy file in a subprocess."""

    code = ("import json, sys; sys.path.insert(0, %r); "
            "import resource_exec; print(json.dumps("
            "resource_exec.payload_identity_env({}, action_key=%r, nonce=%r)))"
            % (str(proxy.parent), key, nonce))
    probe = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True, text=True, timeout=60)
    assert probe.returncode == 0, probe.stderr
    return json.loads(probe.stdout)


def _core_module(gen: Path, tag: str):
    spec = importlib.util.spec_from_file_location(
        f"retained_core_{tag}", gen / "src" / "prismabuild" / "core.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[f"retained_core_{tag}"] = module
    spec.loader.exec_module(module)
    return module


def test_authority_chain_matches_within_generation(
        fleet_store, tmp_path, monkeypatch) -> None:
    """A's proxy seeds A's root; A's core forwards it; B's core refuses it."""

    gen_a = _published_generation(fleet_store, "gen-a-0005")
    gen_b = _published_generation(fleet_store, "gen-b-0005")
    env_a = _proxy_env(gen_a / "tools" / "fleet" / "resource_exec.py",
                       KEY, NONCE)
    assert env_a["PRISMABUILD_READER_HELPER_ROOT"] == str(gen_a)
    for name, value in env_a.items():
        monkeypatch.setenv(name, value)
    core_a = _core_module(gen_a, "a")
    core_b = _core_module(gen_b, "b")
    action = {"action_key": KEY}
    forwarded = core_a._residency_environment(action, {})
    assert forwarded["PRISMABUILD_ACTION_NONCE"] == NONCE
    assert forwarded["PRISMABUILD_ACTION_SCOPE"] == env_a[
        "PRISMABUILD_ACTION_SCOPE"]
    assert forwarded["PRISMABUILD_READER_HELPER_ROOT"] == str(gen_a)
    # The same bundle under B's core is another generation's identity:
    # refused, never fed.
    with pytest.raises(core_b.ActionContractError):
        core_b._residency_environment(action, {})

"""Unchanged runtime probes adopted from independent review r2."""
import copy

import gc

import hashlib

import json

import pickle

import time

from types import MappingProxyType, SimpleNamespace

import warnings

import weakref

import pytest

from prismabuild import fleet_roster, host_cap, pool, pool_reconcile, resource_scope

from test_capped_partial_host import capped  # noqa: F401  (fixture)

def _evidence(label, value):
    """pbtest forwards no -s/-r; the warnings summary carries the evidence."""
    text = label + " " + json.dumps(value, sort_keys=True, default=str)
    print(text)
    warnings.warn(text, UserWarning, stacklevel=2)


ROUTES = ["claim", "serve_once", "_claim", "_claim_pass", "_start_claim_scan", "commit"]

class _PickledNew:
    def __reduce__(self):
        return object.__new__, (host_cap.CappedCapacity,)

class _ClassSpoof:
    @property
    def __class__(self):
        return host_cap.CappedCapacity

    def get(self, *args):
        return 100

    def __getitem__(self, key):
        return 100

    def __iter__(self):
        return iter(("cpu", "mem_gb"))

    def __len__(self):
        return 2

def _forgery(kind):
    genuine = host_cap.enforce_host_cap({"cpu": 2, "mem_gb": 4}, None, {})
    if kind == "dict":
        return dict(genuine)
    if kind == "object_new":
        return object.__new__(host_cap.CappedCapacity)
    if kind == "pickle_new":
        return pickle.loads(pickle.dumps(_PickledNew()))
    if kind == "class_spoof":
        value = _ClassSpoof()
        assert isinstance(value, host_cap.CappedCapacity)  # isinstance is fooled
        return value
    if kind == "mapping_proxy":
        return MappingProxyType(dict(genuine))
    raise AssertionError(kind)

@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("kind", ["dict", "object_new", "pickle_new", "class_spoof", "mapping_proxy"])
def test_forged_capacity_is_refused_before_queue_io(tmp_path, route, kind):
    value = _forgery(kind)
    queue = pool.PoolQueue(tmp_path / "uncreated")
    with pytest.raises(TypeError, match="enforce_host_cap"):
        if route == "commit":
            queue._rename_and_commit_claim(None, SimpleNamespace(capacity=value))
        else:
            getattr(queue, route)(capacity=value)
    assert not queue.root.exists()

def test_duplication_and_subclassing_of_a_genuine_value_refuse():
    genuine = host_cap.enforce_host_cap({"cpu": 2, "mem_gb": 4}, None, {})
    operations = [copy.copy, copy.deepcopy, pickle.dumps,
                  lambda v: pickle.dumps(v, protocol=0), lambda v: pickle.dumps(v, protocol=5)]
    if hasattr(copy, "replace"):
        operations.append(copy.replace)
    for operation in operations:
        with pytest.raises(TypeError):
            operation(genuine)
    with pytest.raises(TypeError):
        type("Sub", (host_cap.CappedCapacity,), {})
    with pytest.raises(TypeError):
        host_cap.CappedCapacity({"cpu": 80})
    with pytest.raises(TypeError):
        host_cap.CappedCapacity({"cpu": 80}, _authority=object())
    with pytest.raises((AttributeError, TypeError)):
        vars(genuine)
    with pytest.raises(AttributeError):
        genuine.extra = 1

@pytest.fixture
def two_hosts(monkeypatch):
    roster = fleet_roster.Roster.capture(json.dumps({"boxes": {
        "claiming": {"cap": {"cpus": "4-7,12-15", "mem_gb": 32, "slice": "pbcap.slice"}},
        "local": {}}}).encode())
    asked = []

    def lookup(host, **kwargs):
        asked.append(host)
        return roster.canonical_box(host)
    monkeypatch.setattr(fleet_roster.Roster, "for_host", lookup)
    monkeypatch.setattr(resource_scope.socket, "gethostname", lambda: "local")
    return asked

def _unit(key, nonce):
    return "prismabuild-job" + hashlib.sha256((key + nonce).encode()).hexdigest()[:32] + ".slice"

@pytest.mark.parametrize("operation", ["create", "recover_create", "control"])
@pytest.mark.parametrize("parent", ["pbcap.slice", "escape.slice", "prismabuild.slice"])
def test_custom_parent_binding_holds_for_creation_and_recovery(two_hosts, tmp_path, monkeypatch,
                                                              operation, parent):
    monkeypatch.setattr(resource_scope.socket, "gethostname", lambda: "claiming")
    key, nonce = "a" * 64, "b" * 32
    unit = _unit(key, nonce)
    kernel = parent[:-6] + "-" + unit.removeprefix("prismabuild-")
    reply = {"scope_id": unit, "token": "c" * 64,
             "cgroup_path": "/sys/fs/cgroup/" + parent + "/" + kernel}
    if operation == "control":
        control = {**reply, "action_key": key, "nonce": nonce,
                   "socket_path": str(resource_scope.BROKER_SOCKET)}
        if parent == "pbcap.slice":
            assert resource_scope._checked_control_identity(key, control) == (nonce, unit)
        else:
            with pytest.raises(ValueError):
                resource_scope._checked_control_identity(key, control)
        return
    scope = resource_scope.ResourceScope(key, nonce, 1024, tmp_path / "telemetry")
    monkeypatch.setattr(scope, "_request", lambda *a, **kw: reply)
    if parent == "pbcap.slice":
        assert getattr(scope, operation)() is not False
        assert scope.unit == unit and str(scope.cgroup_path).startswith("/sys/fs/cgroup/pbcap.slice/")
    else:
        with pytest.raises(OSError, match="invalid scope identity"):
            getattr(scope, operation)()

def test_installer_recap_and_reinstall_keep_ancillary_limits(tmp_path):
    """Re-running the cap with a new CPU list rewrites both ancillary drop-ins,
    and re-running an ancillary installer afterwards keeps its own limits."""
    import configparser
    import os
    import re
    import subprocess
    import sys
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    staged, bindir, sandbox = tmp_path / "staged", tmp_path / "bin", tmp_path / "filesystem"
    staged.mkdir()
    bindir.mkdir()
    for prefix in ("etc/systemd/system", "opt", "var/lib", "home/rob"):
        (sandbox / prefix).mkdir(parents=True, exist_ok=True)
    programs = {
        "id": "print('0')",
        "stat": "import sys; print('0' if '%u' in sys.argv else '755')",
        "chown": "pass",
        "systemctl": "import sys; print('inactive') if 'show' in sys.argv else None; sys.exit(3 if 'is-active' in sys.argv else 0)",
        "install": "import os,sys\na=[]; i=1\nwhile i<len(sys.argv):\n if sys.argv[i] in ('-o','-g'): i+=2\n else: a.append(sys.argv[i]); i+=1\nos.execv('/usr/bin/install',['install',*a])",
    }
    for name, program in programs.items():
        (bindir / name).write_text('#!' + sys.executable + '\n' + program + '\n')
        (bindir / name).chmod(0o755)
    for leaf in ("digest_primitives", "generation_receipt", "upgrade_client", "runtime_publication"):
        (staged / (leaf + ".py")).write_text("# fixture staging input\n")

    def run(name, *args):
        source = (root / "tools/fleet" / name).read_text()
        source = re.sub(r"/var/lib|/home/rob|/etc|/opt",
                        lambda m: str(sandbox / m.group().lstrip('/')), source)
        (staged / name).write_text(source)
        result = subprocess.run(["bash", str(staged / name), *args], capture_output=True, text=True,
                                env={**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}"}, timeout=60)
        assert result.returncode == 0, (name, result.stdout[-2000:], result.stderr[-2000:])

    def effective(unit):
        units = sandbox / "etc/systemd/system"
        parser = configparser.ConfigParser(interpolation=None, strict=False)
        parser.read([units / unit, *sorted((units / (unit + ".d")).glob("*.conf"))])
        return {k: parser.get("Service", k, fallback=None)
                for k in ("Slice", "AllowedCPUs", "MemoryMax", "TasksMax")}

    run("install_client_upgrader.sh")
    run("install_movement_publisher.sh", "--approval-key", "0" * 64)
    run("install_supervisor_unit.sh", "--system", "--slice", "prismabuild.slice",
        "--cpus", "4-7", "--memory-gb", "32")
    run("install_supervisor_unit.sh", "--system", "--slice", "prismabuild.slice",
        "--cpus", "4-7,12-15", "--memory-gb", "32")
    # Root may overwrite its own 0400 secrets; this unprivileged sandbox may not.
    for secret in (sandbox / "etc/prismabuild").rglob("*"):
        if secret.is_file():
            secret.chmod(0o600)
    run("install_client_upgrader.sh")
    run("install_movement_publisher.sh", "--approval-key", "0" * 64)
    seen = {u: effective(u) for u in ("prismabuild-client-upgrade.service",
                                      "prismabuild-movement-publish.service",
                                      "prismabuild-resource-broker.service")}
    _evidence("RECAP EFFECTIVE", seen)
    assert seen["prismabuild-client-upgrade.service"] == {
        "Slice": "prismabuild.slice", "AllowedCPUs": "4-7,12-15", "MemoryMax": "128M", "TasksMax": "16"}
    assert seen["prismabuild-movement-publish.service"] == {
        "Slice": "prismabuild.slice", "AllowedCPUs": "4-7,12-15", "MemoryMax": "256M", "TasksMax": "16"}
    assert seen["prismabuild-resource-broker.service"]["AllowedCPUs"] == "4-7,12-15"

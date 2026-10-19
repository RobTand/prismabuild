"""Private maintenance files and captured RPC bytes; no broker or Docker service."""
from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import runpy
import socket
import threading
import sys
import stat
from types import SimpleNamespace


def authority(module, store, *, durable=False):
    run = store / "run"
    run.mkdir(mode=0o700, exist_ok=True)
    backend = SimpleNamespace(healthy=lambda: True, inventory=lambda: {})
    legacy = module.Authority(run / "jobs", os.getuid(), backend, max_memory_bytes=1 << 30)
    if not legacy.maintenance_path.exists():
        legacy._sync_maintenance_gate()
    if not durable:
        return legacy
    return module.Authority(run / "jobs", os.getuid(), backend, max_memory_bytes=1 << 30,
                            maintenance_state=store / "var/maintenance.json")


def initial_installation(broker, store):
    state = store / "initialize/maintenance.json"
    old_uid, old_stat, old_argv = os.geteuid, Path.lstat, sys.argv
    def observed_stat(path, *args, **kwargs):
        info = old_stat(path, *args, **kwargs)
        if path == state.parent:
            return SimpleNamespace(st_mode=stat.S_IFDIR | 0o700, st_uid=0)
        return info
    os.geteuid = lambda: 0
    Path.lstat = observed_stat
    sys.argv = ["resource_broker.py", "--initialize-maintenance-state", "--maintenance-state", str(state),
                "--state-dir", str(store / "run/jobs")]
    try:
        broker.main()
    finally:
        os.geteuid, Path.lstat, sys.argv = old_uid, old_stat, old_argv
    return state


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    broker = load("resource_broker")
    write = request["op"] == "write"
    if write and variant == "explicit-initialize":
        path = initial_installation(broker, store)
        if family == "broker-evidence":
            path = path.with_name(path.name + ".initialized")
        return {"raw": base64.b64encode(path.read_bytes()).decode(),
                "fixture_facts": ["supplied privileged identity and parent-ownership observations; actual private writes only; no root installation or deployment proof"]}
    if family == "broker-settlement":
        if write:
            pool, scope = load("prismabuild.pool"), load("prismabuild.resource_scope")
            queue = pool.PoolQueue(store / "queue")
            pool._docker_containers_with_label = lambda *args: [] if variant == "settled" else ["compat-container"]
            value = queue._container_settlement("c" * 64, "compat-scope")
            endpoint = store / "capture.sock"
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(str(endpoint))
            server.listen(1)
            captured, errors = [], []

            def receive():
                try:
                    server.settimeout(10)
                    client, _ = server.accept()
                    with client:
                        client.settimeout(10)
                        data = bytearray()
                        while b"\n" not in data:
                            block = client.recv(4096)
                            if not block:
                                raise ValueError("capture peer closed before its request")
                            data.extend(block)
                        captured.append(bytes(data))
                        client.sendall(b'{"ok":true}\n')
                except Exception as exc:
                    errors.append(str(exc))
                finally:
                    server.close()

            child = threading.Thread(target=receive)
            child.start()
            try:
                scope.broker_request({"op": "settle", "evidence": value}, socket_path=endpoint)
            finally:
                child.join(timeout=11)
            assert not child.is_alive() and not errors, errors
            nested = runpy.run_path(str(Path(__file__).with_name("compat_receipt_adapters.py")))["nested_bytes"]
            raw = nested(captured[0], ("evidence",))
        else:
            value = json.loads(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            try:
                result = broker._settlement_evidence(value)
            except ValueError:
                assert variant == "retained", "settled evidence was refused by the actual broker reader"
                return {"settled": False}
            assert variant == "settled", "container-bearing evidence granted settlement"
            return result
    else:
        current = authority(broker, store, durable=family == "broker-evidence" or variant in ("migrated", "boot-fence"))
        path = current.maintenance_evidence_path if family == "broker-evidence" else (
            current.maintenance_state_path if variant in ("migrated", "boot-fence") else current.maintenance_path)
        if write:
            if family == "broker-evidence":
                current._write_maintenance_evidence()
            elif variant == "boot-fence":
                current.maintenance_path.unlink()
                current._restore_maintenance()
                assert current.maintenance["owner"] == broker.MAINTENANCE_BOOT_OWNER
            elif variant == "fenced":
                assert current._force_volatile_gate_closed() is None
            elif variant not in ("bootstrap", "migrated"):
                owner = "compat-owner" if variant != "unowned" else None
                if variant == "taken-over":
                    owner = "compat-host:supervisor-42:7"
                current.admin(0, {"op": "maintenance_begin", **({"owner": owner} if owner else {}), "reason": "compat"})
                if variant in ("released", "forced"):
                    current.admin(0, {"op": "maintenance_force_end" if variant == "forced" else "maintenance_end",
                                      "owner": "different-owner" if variant == "forced" else owner})
                if variant == "taken-over":
                    broker._proc_starttime = lambda pid: "8" if pid == 2 else None
                    current.admin(0, {"op": "maintenance_takeover", "owner": "compat-host:supervisor-2:8",
                                      "expected_changed_unix": 100.0, "reason": "compat takeover"})
            raw = path.read_bytes()
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            path.chmod(0o600)
            request["_consumer_boundary"]()
            if family == "broker-evidence":
                return current._maintenance_evidence()
            result = current._maintenance_value(path)
            assert result["draining"] == (variant not in ("bootstrap", "migrated", "released", "forced", "explicit-initialize"))
            return result
    return {"raw": base64.b64encode(raw).decode()}

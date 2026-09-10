"""A broker reply can disappear after a real worker has published its receipt.

This fixture deliberately uses a Unix socket and SCM_RIGHTS rather than a
``Popen`` double: it exercises the same ``resource_exec`` boundary that turns
an absent broker reply into exit 125.  The socket server is confined to the
test process; scope create/status/release remain the narrow mock used by the
resource-scope tests, so this never contacts the privileged broker.
"""
import array
import hashlib
import json
import os
import socket
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from prismabuild import core as pb, pool, resource_scope


def _scope_reply(scope):
    unit = "prismabuild-job" + hashlib.sha256(
        (scope.action_key + scope.nonce).encode()
    ).hexdigest()[:32] + ".slice"
    return {"ok": True, "scope_id": unit, "token": "b" * 64,
            "cgroup_path": "/sys/fs/cgroup/prismabuild.slice/" + unit}


def _close_after_payload(socket_path, complete, *, disconnect_before_exit):
    """Run one SCM_RIGHTS payload, then intentionally omit its response."""
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(socket_path))
    server.listen(1)
    server.settimeout(0.1)
    stopped = threading.Event()
    failures = []

    def serve():
        client = None
        descriptors = []
        child = None
        try:
            while not stopped.is_set():
                try:
                    client, _ = server.accept()
                    break
                except TimeoutError:
                    continue
            if client is None:
                return
            client.settimeout(30)
            raw, ancillary, _, _ = client.recvmsg(
                65537, socket.CMSG_SPACE(3 * array.array("i").itemsize)
            )
            for level, kind, data in ancillary:
                if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                    values = array.array("i")
                    values.frombytes(data[:len(data) - len(data) % values.itemsize])
                    descriptors.extend(values)
            while not raw.endswith(b"\n"):
                if len(raw) >= 65536:
                    raise ValueError("resource execution request exceeds framing limit")
                more = client.recv(65537 - len(raw))
                if not more:
                    raise OSError("resource execution client closed before request")
                raw += more
            request = json.loads(raw)
            if request.get("op") != "run" or len(descriptors) != 3:
                raise ValueError("resource execution request did not carry three stdio descriptors")
            complete.update(request=request, descriptors=len(descriptors))
            child = subprocess.Popen(
                request["argv"], cwd=request["cwd"], env=request["env"],
                stdin=descriptors[0], stdout=descriptors[1], stderr=descriptors[2],
            )
            if disconnect_before_exit:
                complete["payload_pid"] = child.pid
                # Close the control reply before the owned worker exits, then
                # reap it here so this controlled fixture leaves no payload.
                client.close()
                complete["payload_returncode"] = child.wait(timeout=30)
            else:
                complete["payload_returncode"] = child.wait(timeout=30)
            # The real failure: resource_exec sees EOF rather than its result.
        except BaseException as exc:
            failures.append(exc)
        finally:
            if child is not None and child.poll() is None:
                child.kill()
                child.wait(timeout=10)
            for descriptor in descriptors:
                os.close(descriptor)
            if client is not None:
                client.close()
            server.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    return {"thread": thread, "server": server, "stopped": stopped,
            "failures": failures}


def make_failed_attempt(tmp_path, monkeypatch, *, publish_receipt=True,
                        disconnect_before_exit=False, finish_detail=None,
                        finish_status="failed"):
    """Return ``(queue, cas, action, terminal)`` for the #475 wire failure."""
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text(
        "from pathlib import Path\n"
        + ("Path('result').write_text('sealed receipt\\n')\n" if publish_receipt else "")
    )
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/broker-disconnect", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {"demand": {"cpu": 1, "mem_gb": 2}},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / "queue")
    worker = Path(__file__).resolve().parents[1] / "tools" / "prismabuild_worker.py"
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
                  checkout_root=checkout, worker_script=worker,
                  resources={"cpu": 1, "mem_gb": 2}, max_attempts=1)
    item = queue.claim(capacity={"cpu": 1, "mem_gb": 2})
    assert item is not None

    socket_path = tmp_path / "broker.sock"
    observed = {}
    broker = _close_after_payload(socket_path, observed,
                                  disconnect_before_exit=disconnect_before_exit)

    def request(scope, operation, **extra):
        if operation == "create":
            return _scope_reply(scope)
        if operation == "release":
            return {"ok": True, "released": True, "scope_id": scope.unit}
        return {"ok": True}

    def sample(scope):
        value = {"action_key": scope.action_key, "nonce": scope.nonce,
                 "host": socket.gethostname(), "scope_unit": scope.unit,
                 "sampled_unix": pool._now(), "wall_seconds": 0.01,
                 "cpu_seconds": 0.0, "memory_peak_bytes": 0,
                 "memory_current_bytes": 0, "oom_kill": 0, "oom_local": 0,
                 "complete": True, "errors": []}
        resource_scope._atomic_json(scope.telemetry_path, value)
        return value

    original_wrap = resource_scope.ResourceScope.wrap_argv
    def wrap(scope, argv):
        wrapped = original_wrap(scope, argv)
        wrapped[wrapped.index("--socket") + 1] = str(socket_path)
        return wrapped

    monkeypatch.setattr(resource_scope.ResourceScope, "_request", request)
    monkeypatch.setattr(resource_scope.ResourceScope, "sample", sample)
    monkeypatch.setattr(resource_scope.ResourceScope, "wrap_argv", wrap)
    monkeypatch.setattr(pool.cpu_admission, "record_completion", lambda *args: None)
    try:
        outcome = queue.execute(item, containment=True, heartbeat_s=0.02)
    except BaseException:
        broker["stopped"].set()
        broker["server"].close()
        raise
    finally:
        broker["thread"].join(timeout=30)
    assert not broker["thread"].is_alive()
    assert not broker["failures"]
    assert observed["descriptors"] == 3
    assert observed["payload_returncode"] == (0 if publish_receipt else 1)
    terminal_path = queue.finish(
        action["action_key"], status=finish_status,
        detail={**outcome, **(finish_detail or {})}, claim_snapshot=item,
    )
    terminal = json.loads(terminal_path.read_text())
    return queue, cas, action, terminal


@pytest.mark.parametrize("disconnect_before_exit", [False, True],
                         ids=["after-payload", "before-payload"])
def test_broker_reply_loss_files_wrapper_failure_beside_a_real_receipt(
    tmp_path, monkeypatch, disconnect_before_exit
):
    queue, cas, action, terminal = make_failed_attempt(
        tmp_path, monkeypatch, disconnect_before_exit=disconnect_before_exit
    )

    assert cas.lookup(action) is not None
    assert cas.result_path(cas.lookup(action), action).read_text() == "sealed receipt\n"
    assert terminal["status"] == "failed"
    assert terminal["detail"]["returncode"] == 125
    assert "closed without an execution result" in terminal["detail"]["stderr"]
    cleanup = terminal["resource_scope_cleanup"]
    assert cleanup["complete"] is True
    assert cleanup["released"]["ok"] is True
    assert cleanup["released"]["released"] is True
    assert cleanup["released"]["scope_id"] == terminal["resource_scope"]["scope_id"]
    telemetry = cleanup["telemetry"]
    scope = terminal["resource_scope"]
    assert telemetry["action_key"] == action["action_key"]
    assert telemetry["nonce"] == cleanup["nonce"]
    assert telemetry["nonce"] == scope["nonce"]
    assert telemetry["scope_unit"] == scope["scope_id"]
    assert telemetry["host"] == socket.gethostname()
    assert telemetry["oom_kill"] == telemetry["oom_local"] == 0
    assert "termination_reason" not in telemetry
    assert queue.ledger().held() == {}


def test_broker_reply_loss_without_receipt_stays_an_ordinary_failure(tmp_path, monkeypatch):
    queue, cas, action, terminal = make_failed_attempt(
        tmp_path, monkeypatch, publish_receipt=False
    )

    assert cas.lookup(action) is None
    assert terminal["status"] == "failed"
    assert terminal["detail"]["returncode"] == 125
    assert terminal["resource_scope_cleanup"]["complete"] is True
    assert queue.ledger().held() == {}

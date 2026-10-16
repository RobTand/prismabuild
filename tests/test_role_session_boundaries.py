"""Real signal, reader and HTTP boundaries in private role processes."""
from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import http.client
import json
import multiprocessing
import os
from pathlib import Path
import signal
import socket
import sys
import threading
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import local_tier_loop
import pbmetrics
import prewarm_loop
from prismabuild import role_session
from prewarm_fixture import Fleet

CONTEXT = multiprocessing.get_context("fork")
WORK_UNIT_ROLES = ("storage", "tiers", "localtier")


def test_production_roles_have_metrics_as_the_only_observer():
    import supervise

    assert set(WORK_UNIT_ROLES) <= supervise.ROLE_SCRIPTS.keys()
    assert set(supervise.ROLE_SCRIPTS) - set(WORK_UNIT_ROLES) == {"metrics"}


@pytest.fixture
def boundary(tmp_path, monkeypatch):
    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": False}))
    parked = tmp_path / "parked"
    parked.mkdir()
    loaded = tmp_path / "loaded.json"
    published = tmp_path / "published.json"
    for receipt in (loaded, published):
        receipt.write_text(json.dumps({"commit": "a" * 40, "generation": "first"}))
    monkeypatch.setattr(role_session, "MAINTENANCE_GATE", gate)
    monkeypatch.setattr(role_session, "PARKED_ROOT", parked)
    monkeypatch.setattr(role_session, "GENERATION_VERSION", loaded)
    monkeypatch.setattr(role_session, "RUNTIME_VERSION", published)
    monkeypatch.setattr(role_session, "ROLE_LOCK_ROOT", tmp_path / "role-locks")
    return SimpleNamespace(root=tmp_path, gate=gate, parked=parked,
                           published=published)


def _close_gate(boundary):
    boundary.gate.write_text(json.dumps({"draining": True, "changed_unix": 101.5}))


def _run_role(main, log, release_receiver=None, release_event=None):
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.default_int_handler)
    if release_receiver is not None:
        def release_reader():
            try:
                if release_receiver.recv_bytes():
                    release_event.set()
            except EOFError:
                pass
        threading.Thread(target=release_reader, daemon=True).start()
    with log.open("w") as stream, redirect_stdout(stream), redirect_stderr(stream):
        raise SystemExit(main())


def _finish(process, release=None):
    if release is not None and process.is_alive():
        try:
            release.send_bytes(b"release")
        except (BrokenPipeError, OSError):
            pass
    process.join(3)
    if process.is_alive():
        process.kill()
        process.join(3)
    process.close()
    if release is not None:
        release.close()


def _metrics_request(port, errors):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        connection.request("GET", "/metrics")
        response = connection.getresponse()
        assert response.status == 200
        assert response.read() == b"controlled_metric 1\n"
    except Exception as error:
        errors.append(error)
    finally:
        connection.close()


@pytest.mark.parametrize("release_before_deadline", [True, False])
@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
def test_metrics_stop_waits_for_real_handlers_before_park(
        boundary, monkeypatch, release_before_deadline, stop_signal):
    entered, ready, closed = (CONTEXT.Event() for _ in range(3))
    release = threading.Event()
    receiver, sender = CONTEXT.Pipe(duplex=False)
    errors = []
    port = CONTEXT.Value("i", 0)
    server_type = pbmetrics.ThreadingHTTPServer

    class ReadyServer(server_type):
        def __init__(self, address, *args, **kwargs):
            super().__init__((address[0], 0), *args, **kwargs)
            port.value = self.server_address[1]
            ready.set()

        def server_close(self):
            super().server_close()
            closed.set()

    def collect(*args, **kwargs):
        entered.set()
        assert release.wait(10), "test did not release HTTP handler"
        return "controlled_metric 1\n"

    monkeypatch.setattr(pbmetrics, "ThreadingHTTPServer", ReadyServer)
    monkeypatch.setattr(pbmetrics, "collect_metrics", collect)
    monkeypatch.setattr(pbmetrics, "SHUTDOWN_GRACE_S", 0.5, raising=False)
    argv = ["--queue-root", str(boundary.root / "queue"), "--port", "9469"]
    process = CONTEXT.Process(target=_run_role,
        args=(lambda: pbmetrics.main(argv), boundary.root / "metrics.log", receiver, release))
    process.start()
    receiver.close()
    request = None
    try:
        assert ready.wait(5), "metrics never bound the test port"
        request = threading.Thread(target=_metrics_request, args=(port.value, errors), daemon=True)
        request.start()
        assert entered.wait(5), "real HTTP handler never entered collection"
        _close_gate(boundary)
        os.kill(process.pid, stop_signal)
        process.join(0.15)
        assert process.is_alive(), "stop signal exited before the handler completed"
        assert list(boundary.parked.iterdir()) == [], "in-flight handler earned a marker"
        assert closed.wait(3), "stop did not close the listener before waiting for handlers"
        with pytest.raises(OSError):
            socket.create_connection(("127.0.0.1", port.value), timeout=0.2)
        if release_before_deadline:
            sender.send_bytes(b"release")
        process.join(5)
        assert process.exitcode == 0, (boundary.root / "metrics.log").read_text()
        markers = list(boundary.parked.iterdir())
        assert len(markers) == int(release_before_deadline)
        if markers:
            assert markers[0].name.endswith("-101.5")
        request.join(3)
        if release_before_deadline:
            assert errors == []
    finally:
        _finish(process, sender)
        if request is not None and request.ident is not None:
            request.join(3)


@pytest.mark.parametrize(("once", "gate_closed"), [(True, True), (False, False)])
def test_prewarm_sigterm_joins_reader_and_publishes_accounting(
        boundary, monkeypatch, once, gate_closed):
    fleet = Fleet(boundary.root / "fleet")
    key = fleet.action("cancelled", [fleet.file("row.pt", prewarm_loop.BLOCK * 2)])
    entered, release = CONTEXT.Event(), threading.Event()
    receiver, sender = CONTEXT.Pipe(duplex=False)
    readv = os.readv

    def blocked_read(fd, buffers):
        result = readv(fd, buffers)
        entered.set()
        assert release.wait(10), "test did not release pool reader"
        return result

    args = fleet.args(once=once, readers=1, max_readers=1,
                      log=str(boundary.root / "cycle.jsonl"))
    monkeypatch.setattr(os, "readv", blocked_read)
    monkeypatch.setattr(prewarm_loop, "require_storage_pacing", lambda _: None)
    monkeypatch.setattr(prewarm_loop, "_parser", lambda: SimpleNamespace(parse_args=lambda _: args))
    process = CONTEXT.Process(target=_run_role,
        args=(lambda: prewarm_loop.main([]), boundary.root / "prewarm.log", receiver, release))
    process.start()
    receiver.close()
    try:
        assert entered.wait(5), (boundary.root / "prewarm.log").read_text()
        boundary.published.unlink()
        if gate_closed:
            _close_gate(boundary)
        os.kill(process.pid, signal.SIGTERM)
        process.join(0.15)
        assert process.is_alive(), "SIGTERM exited before the reader joined"
        assert list(boundary.parked.iterdir()) == []
        sender.send_bytes(b"release")
        process.join(5)
        assert process.exitcode == 0, (boundary.root / "prewarm.log").read_text()
        record = fleet.queue.prewarm(key)
        assert record["status"] == "partial"
        assert record["bytes_warmed"] == prewarm_loop.BLOCK
        assert "stage" not in record
        event = json.loads((boundary.root / "cycle.jsonl").read_text())
        assert event["warmed"][0]["bytes_warmed"] == prewarm_loop.BLOCK
        assert "stage" not in event
        assert len(list(boundary.parked.iterdir())) == int(gate_closed)
    finally:
        _finish(process, sender)


@pytest.mark.parametrize("once", [True, False])
def test_local_tier_same_commit_new_generation_exits_before_cycle(
        boundary, monkeypatch, once):
    boundary.published.write_text(json.dumps({"commit": "a" * 40, "generation": "second"}))
    calls = []

    def cycle(*args):
        calls.append(True)
        os.kill(os.getpid(), signal.SIGTERM)
        return {}

    monkeypatch.setattr(local_tier_loop, "local_tier_cycle", cycle)
    monkeypatch.setattr(local_tier_loop.resident_sets, "read_policy", lambda _: {})
    argv = ["--pool-root", str(boundary.root / "queue")]
    assert local_tier_loop.main(argv + (["--once"] if once else [])) == (75 if once else 0)
    assert calls == [], "local tier served a same-commit retired generation"


def test_local_tier_unknown_loaded_receipt_refuses_the_pass(boundary, monkeypatch):
    (boundary.root / "loaded.json").unlink()
    monkeypatch.setattr(local_tier_loop.resident_sets, "read_policy", lambda _: {})
    calls = []
    monkeypatch.setattr(local_tier_loop, "local_tier_cycle", lambda *args: calls.append(True) or {})
    assert local_tier_loop.main(["--pool-root", str(boundary.root / "queue"), "--once"]) == 75
    assert calls == []


@pytest.mark.parametrize("gate_state", ["open", "closed", "missing"])
def test_metrics_serves_until_explicit_stop_for_each_gate_state(
        boundary, monkeypatch, gate_state):
    if gate_state == "closed":
        _close_gate(boundary)
    elif gate_state == "missing":
        boundary.gate.unlink()
    ready = CONTEXT.Event()
    port = CONTEXT.Value("i", 0)
    server_type = pbmetrics.ThreadingHTTPServer

    class ReadyServer(server_type):
        def __init__(self, address, *args, **kwargs):
            super().__init__((address[0], 0), *args, **kwargs)
            port.value = self.server_address[1]
            ready.set()

    monkeypatch.setattr(pbmetrics, "ThreadingHTTPServer", ReadyServer)
    monkeypatch.setattr(pbmetrics, "collect_metrics", lambda *a, **k: "controlled_metric 1\n")
    argv = ["--queue-root", str(boundary.root / "queue"), "--port", "9469"]
    process = CONTEXT.Process(target=_run_role,
        args=(lambda: pbmetrics.main(argv), boundary.root / "gate-metrics.log"))
    process.start()
    try:
        assert ready.wait(5), "metrics never bound the test port"
        errors = []
        for _ in range(2):
            _metrics_request(port.value, errors)
            assert errors == [], (errors, (boundary.root / "gate-metrics.log").read_text())
        assert process.is_alive(), "maintenance gate retired a live observer"
        assert list(boundary.parked.iterdir()) == [], "serving metrics posted a park marker"
        os.kill(process.pid, signal.SIGTERM)
        process.join(3)
        assert process.exitcode == 0, (boundary.root / "gate-metrics.log").read_text()
        assert len(list(boundary.parked.iterdir())) == int(gate_state == "closed")
    finally:
        _finish(process)




@pytest.mark.parametrize("gate_closed", [False, True])
def test_prewarm_sigterm_interrupts_idle_wait(boundary, monkeypatch, gate_closed):
    if gate_closed:
        _close_gate(boundary)
    fleet = Fleet(boundary.root / "fleet")
    entered = CONTEXT.Event()
    cycles = CONTEXT.Value("i", 0, lock=False)

    class ObservedEvent(threading.Event):
        def wait(self, timeout=None):
            entered.set()
            return super().wait(timeout)

    def cycle(*args, **kwargs):
        cycles.value += 1
        return {}

    args = fleet.args(once=False, poll_s=3600, dry_run=True)
    monkeypatch.setattr(role_session, "threading", SimpleNamespace(Event=ObservedEvent))
    monkeypatch.setattr(prewarm_loop, "cycle", cycle)
    monkeypatch.setattr(prewarm_loop, "_parser", lambda: SimpleNamespace(parse_args=lambda _: args))
    process = CONTEXT.Process(target=_run_role,
        args=(lambda: prewarm_loop.main([]), boundary.root / "idle-prewarm.log"))
    process.start()
    try:
        assert entered.wait(5), "prewarm did not enter the interruptible idle wait"
        assert cycles.value == int(not gate_closed)
        os.kill(process.pid, signal.SIGTERM)
        process.join(3)
        assert process.exitcode == 0, (boundary.root / "idle-prewarm.log").read_text()
        assert cycles.value == int(not gate_closed), "stop started another storage cycle"
        assert len(list(boundary.parked.iterdir())) == int(gate_closed)
    finally:
        _finish(process)


@pytest.mark.parametrize("role", ["worker", "storage", "localtier"])
def test_stop_context_precedes_each_roles_startup_queue_boundary(
        boundary, monkeypatch, role):
    import worker_loop

    queue_entries = CONTEXT.Value("i", 0, lock=False)
    factory = local_tier_loop.pool.PoolQueue

    def queue_boundary(*args, **kwargs):
        queue_entries.value += 1
        if role == "worker":
            os.kill(os.getpid(), signal.SIGTERM)
            return SimpleNamespace(root=boundary.root / "queue")
        return factory(*args, **kwargs)

    if role == "worker":
        monkeypatch.setattr(worker_loop, "SH", boundary.root)
        main = lambda: worker_loop.main(argv=["--all-cores", "--cpu-slots", "1",
            "--mem-gb", "1", "--assume-idle", "--poll-s", "0", "--max-idle", "1"])
    else:
        if role == "storage":
            args = Fleet(boundary.root / "fleet").args(once=False, dry_run=True)
            monkeypatch.setattr(prewarm_loop, "_parser", lambda: SimpleNamespace(parse_args=lambda _: args))
            invoke = lambda: prewarm_loop.main([])
        else:
            invoke = lambda: local_tier_loop.main(["--pool-root", str(boundary.root / "queue")])

        def observed_read_main():
            def read_finished(frame, event, value):
                if (event == "return" and isinstance(value, str) and frame.f_code.co_name == "read_text"
                        and frame.f_locals.get("self") == role_session.GENERATION_VERSION):
                    sys.setprofile(None)
                    os.kill(os.getpid(), signal.SIGTERM)
            sys.setprofile(read_finished)
            try:
                return invoke()
            finally:
                sys.setprofile(None)
        main = observed_read_main
    monkeypatch.setattr(local_tier_loop.pool, "PoolQueue", queue_boundary)
    log = boundary.root / "entry.log"
    process = CONTEXT.Process(target=_run_role, args=(main, log))
    process.start()
    try:
        process.join(5)
        assert process.exitcode == 0, log.read_text()
        assert queue_entries.value == int(role == "worker")
        assert list(boundary.parked.iterdir()) == []
        assert not (boundary.root / "pb-queue/workers").exists()
    finally:
        _finish(process)


@pytest.mark.parametrize("role", WORK_UNIT_ROLES)
def test_generation_change_after_one_cycle_prevents_a_second_unit(
        boundary, monkeypatch, role):
    import tier_loop

    cycles = CONTEXT.Value("i", 0, lock=False)
    completed = boundary.root / "completed-unit"

    def cycle(*args, **kwargs):
        cycles.value += 1
        completed.write_text(str(cycles.value))
        if cycles.value == 1:
            boundary.published.write_text(json.dumps({
                "commit": "a" * 40, "generation": "successor"}))
        else:
            os.kill(os.getpid(), signal.SIGTERM)
        return [] if role == "tiers" else {}

    if role == "tiers":
        monkeypatch.setattr(tier_loop, "cycle", cycle)
        monkeypatch.setattr(tier_loop, "_start", lambda *a, **k: (object(), object()))
        monkeypatch.setattr(tier_loop, "tier_cycle_line", lambda *a: {})
        monkeypatch.setattr(tier_loop, "CYCLE_INTERVAL_S", tier_loop.CYCLE_INTERVAL_S)
        main = lambda: tier_loop.main(["--pool-root", str(boundary.root / "queue"), "--interval-s", "0.001"])
    elif role == "storage":
        args = Fleet(boundary.root / "fleet").args(once=False, poll_s=0, dry_run=True)
        monkeypatch.setattr(prewarm_loop, "cycle", cycle)
        monkeypatch.setattr(prewarm_loop, "_parser", lambda: SimpleNamespace(parse_args=lambda _: args))
        main = lambda: prewarm_loop.main([])
    else:
        monkeypatch.setattr(local_tier_loop, "local_tier_cycle", cycle)
        monkeypatch.setattr(local_tier_loop.resident_sets, "read_policy", lambda _: {})
        main = lambda: local_tier_loop.main(["--pool-root", str(boundary.root / "queue"), "--interval-s", "0.001"])
    log = boundary.root / "second-unit.log"
    process = CONTEXT.Process(target=_run_role, args=(main, log))
    process.start()
    try:
        process.join(5)
        assert process.exitcode == 0, log.read_text()
        assert cycles.value == 1
        assert completed.read_text() == "1"
        assert list(boundary.parked.iterdir()) == []
    finally:
        _finish(process)


@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
def test_local_tier_signal_finishes_its_unit_and_starts_no_second_cycle(
        boundary, monkeypatch, stop_signal):
    cycles = CONTEXT.Value("i", 0, lock=False)
    final = boundary.root / "local-final-write"

    def cycle(*args):
        cycles.value += 1
        os.kill(os.getpid(), stop_signal)
        assert list(boundary.parked.iterdir()) == []
        final.write_text("complete")
        return {}

    monkeypatch.setattr(local_tier_loop, "local_tier_cycle", cycle)
    monkeypatch.setattr(local_tier_loop.resident_sets, "read_policy", lambda _: {})
    log = boundary.root / "local-stop.log"
    process = CONTEXT.Process(target=_run_role, args=(lambda: local_tier_loop.main([
        "--pool-root", str(boundary.root / "queue"), "--interval-s", "3600"]), log))
    process.start()
    try:
        process.join(5)
        assert process.exitcode == 0, log.read_text()
        assert cycles.value == 1
        assert final.read_text() == "complete"
        assert list(boundary.parked.iterdir()) == []
    finally:
        _finish(process)

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
import worker_loop
from prismabuild import role_session
from prewarm_fixture import Fleet, StagePool

CONTEXT = multiprocessing.get_context("fork")


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
def test_metrics_sigterm_waits_for_real_handlers_before_park(
        boundary, monkeypatch, release_before_deadline):
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
        os.kill(process.pid, signal.SIGTERM)
        process.join(0.15)
        assert process.is_alive(), "SIGTERM exited before the handler completed"
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


def test_prewarm_sigterm_aborts_partial_sink_and_publishes_accounting(
        boundary, monkeypatch):
    fleet = Fleet(boundary.root / "fleet")
    key = fleet.action("cancelled", [fleet.file("row.pt", prewarm_loop.BLOCK * 2)])
    stage = StagePool(boundary.root / "stage")
    stage.install(monkeypatch)
    entered, release = CONTEXT.Event(), threading.Event()
    receiver, sender = CONTEXT.Pipe(duplex=False)
    write = prewarm_loop.StageObject.write

    def blocked_write(sink, data):
        result = write(sink, data)
        entered.set()
        assert release.wait(10), "test did not release stage writer"
        return result

    args = fleet.args(readers=1, stage=True, stage_free_floor_bytes=0,
                      log=str(boundary.root / "cycle.jsonl"))
    monkeypatch.setattr(prewarm_loop.StageObject, "write", blocked_write)
    monkeypatch.setattr(prewarm_loop, "require_storage_pacing", lambda _: None)
    monkeypatch.setattr(prewarm_loop, "_parser", lambda: SimpleNamespace(parse_args=lambda _: args))
    process = CONTEXT.Process(target=_run_role,
        args=(lambda: prewarm_loop.main([]), boundary.root / "prewarm.log", receiver, release))
    process.start()
    receiver.close()
    try:
        assert entered.wait(5), (boundary.root / "prewarm.log").read_text()
        assert len(stage.objects()) == 1, "real partial sink was not created"
        _close_gate(boundary)
        os.kill(process.pid, signal.SIGTERM)
        process.join(0.15)
        assert process.is_alive(), "SIGTERM unwound the reader before sink cleanup"
        assert list(boundary.parked.iterdir()) == []
        sender.send_bytes(b"release")
        process.join(5)
        assert process.exitcode == 0, (boundary.root / "prewarm.log").read_text()
        assert stage.objects() == [], "partial stage object survived stop"
        record = fleet.queue.prewarm(key)
        assert record["status"] == "partial"
        assert record["bytes_warmed"] == prewarm_loop.BLOCK
        assert record["stage"]["staged_entries"] == 0
        event = json.loads((boundary.root / "cycle.jsonl").read_text())
        assert event["stage"]["aborted_entries"] == 1
        assert event["stage"]["aborted_bytes"] == prewarm_loop.BLOCK
        assert len(list(boundary.parked.iterdir())) == 1
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


def test_local_tier_unknown_loaded_commit_keeps_its_existing_policy(boundary, monkeypatch):
    monkeypatch.setattr(role_session, "loaded_runtime_commit", lambda: "")
    monkeypatch.setattr(role_session, "published_commit", lambda: "known-published-commit")
    monkeypatch.setattr(local_tier_loop.resident_sets, "read_policy", lambda _: {})
    calls = []
    monkeypatch.setattr(local_tier_loop, "local_tier_cycle", lambda *args: calls.append(True) or {})
    assert local_tier_loop.main(["--pool-root", str(boundary.root / "queue"), "--once"]) == 0
    assert calls == [True]

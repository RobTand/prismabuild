"""Real local inotify, HTTP and waiter boundaries, never a live queue (#1894)."""
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import os
from pathlib import Path
import socket
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from prismabuild import action_wait, queue_bell, queue_notifier

KEY = "a" * 64


@pytest.fixture
def bell(tmp_path):
    root = tmp_path / "queue"
    for directory in queue_bell.STATE_DIRS:
        (root / directory).mkdir(parents=True)
    with queue_notifier.BellService(root, [("127.0.0.1", 0)]) as service:
        port = service.servers[0].server_address[1]
        client = queue_bell.Client(root, host="127.0.0.1", port=port)
        yield SimpleNamespace(root=root, service=service, client=client)


def _rename_bell_record(root, directory="done", key=KEY):
    temporary = root / directory / ".temporary"
    temporary.write_text(json.dumps({"status": "executed"}))
    os.replace(temporary, root / directory / f"{key}.json")


@pytest.mark.parametrize("before_wait", [True, False])
def test_real_rename_after_now_is_delivered_even_before_wait(bell, before_wait, monkeypatch):
    cursor = bell.client.now()
    entered = threading.Event()
    real_wait = bell.service.ring.wait

    def entering_wait(*args):
        entered.set()
        return real_wait(*args)

    monkeypatch.setattr(bell.service.ring, "wait", entering_wait)
    with ThreadPoolExecutor() as executor:
        if before_wait:
            _rename_bell_record(bell.root)
        pending = executor.submit(bell.client.wait, cursor, ("done",), (KEY,), 5)
        if not before_wait:
            assert entered.wait(5)
            _rename_bell_record(bell.root)
        reply = pending.result(6)
    assert reply["events"]
    assert all(event["seq"] > cursor.seq and event["dir"] == "done"
               and event["key"] == KEY for event in reply["events"])


def test_filters_and_timeout_ignore_other_keys_dirs_and_temporary_files(bell):
    cursor = bell.client.now()
    _rename_bell_record(bell.root, "ready")
    _rename_bell_record(bell.root, key="b" * 64)
    (bell.root / "done" / ".hidden.json").write_text("{}")
    assert bell.client.wait(cursor, ("done",), (KEY,), 0.05)["events"] == []


def test_close_write_without_rename_is_a_hint_too(bell):
    cursor = bell.client.now()
    (bell.root / "done" / f"{KEY}.json").write_text("{}")
    assert bell.client.wait(cursor, ("done",), (KEY,), 5)["events"]


def test_epoch_change_and_future_cursor_reset_over_http(bell):
    cursor = bell.client.now()
    for other in (queue_bell.Cursor("0" * 32, 0),
                  queue_bell.Cursor(cursor.epoch, cursor.seq + 1)):
        assert bell.client.wait(other, (), (), 0)["reset"] is True


def test_overflow_packet_resets_an_already_waiting_client(bell, monkeypatch):
    cursor = bell.client.now()
    entered = threading.Event()
    real_wait = bell.service.ring.wait

    def entering_overflow_wait(*args):
        entered.set()
        return real_wait(*args)

    monkeypatch.setattr(bell.service.ring, "wait", entering_overflow_wait)
    with ThreadPoolExecutor() as executor:
        pending = executor.submit(bell.client.wait, cursor, (), (), 5)
        assert entered.wait(5)
        # Inject the kernel's overflow frame, not a timing-dependent flood.
        bell.service.watch.consume(queue_notifier.EVENT_HEADER.pack(
            -1, queue_notifier.IN_Q_OVERFLOW, 0, 0), bell.service.ring)
        assert pending.result(6)["reset"] is True


def test_ring_loss_resets_but_last_retained_cursor_still_works(bell):
    ring = bell.service.ring
    cursor = bell.client.now()
    for _ in range(queue_notifier.DEFAULT_RING_SIZE + 1):
        ring.append("done", KEY)
    assert bell.client.wait(cursor, (), (), 0)["reset"] is True
    boundary = queue_bell.Cursor(cursor.epoch, ring.floor)
    assert len(bell.client.wait(boundary, (), (), 0)["events"]) == ring.events.maxlen


def test_server_caps_wait_and_rejects_bad_queries(bell, monkeypatch):
    monkeypatch.setattr(queue_notifier, "MAX_WAIT_S", 0.01)
    assert bell.client.wait(bell.client.now(), (), (), 1000)["events"] == []
    for query in ("since=x&timeout=0", f"since={bell.client.now()}&timeout=nan",
                  f"since={bell.client.now()}&timeout=-1",
                  f"since={bell.client.now()}&timeout=0&dirs=not-a-state"):
        connection = http.client.HTTPConnection("127.0.0.1", bell.client.port, timeout=2)
        try:
            connection.request("GET", "/v1/wait?" + query)
            response = connection.getresponse()
            assert response.version == 11
            assert response.status == 400
            response.read()
        finally:
            connection.close()


@pytest.mark.parametrize("source,filesystem,expected", [
    ("10.100.98.3:/storage_pool/shared", "nfs4", "10.100.98.3"),
    ("10.100.99.3:/storage_pool/shared", "nfs", "10.100.99.3"),
    ("[::1]:/shared", "nfs4", "::1"),
    ("prismabuild-stage/pbqueue", "zfs", "127.0.0.1"),
])
def test_mount_source_locates_the_bell(tmp_path, source, filesystem, expected):
    table = tmp_path / "mountinfo"
    table.write_text("1 0 0:1 / / rw - ext4 /dev/root rw\n"
                     f"2 1 0:2 / /mnt/shared rw - {filesystem} {source} rw\n")
    assert queue_bell.bell_host(Path("/mnt/shared/queue"), mountinfo=table) == expected


def test_client_unavailable_and_waiter_capped_fallback(tmp_path, monkeypatch):
    # Hold a bound, non-listening socket so this port cannot be reused.
    with socket.socket() as down:
        down.bind(("127.0.0.1", 0))
        client = queue_bell.Client(tmp_path, host="127.0.0.1", port=down.getsockname()[1])
        with pytest.raises(queue_bell.Unavailable):
            client.now()
        sleeps = []
        clock = [0.0]

        def fallback_sleep(delay):
            sleeps.append(delay)
            clock[0] += delay

        monkeypatch.setattr(action_wait, "time", SimpleNamespace(
            monotonic=lambda: clock[0], sleep=fallback_sleep))
        result = action_wait.await_action(
            lambda **kw: action_wait.Pending(None), generation=None,
            patience=action_wait.WaitUntil(20), profile=action_wait.AwaitProfile.SYNCHRONOUS,
            tool="test", subject=KEY, notice=lambda event: None, bell=client, bell_key=KEY)
        assert isinstance(result, action_wait.PatienceExpired)
        assert sleeps[:6] == [0.25, 0.5, 1.0, 2.0, 4.0, 5.0]
        assert max(sleeps) <= action_wait.POLL_S and sum(sleeps) == 20


def test_action_wait_rechecks_terminal_rename_without_sleep(bell, monkeypatch):
    observed = threading.Event()
    path = bell.root / "done" / f"{KEY}.json"

    def observe_record(**kwargs):
        if path.exists():
            return action_wait.Ready(json.loads(path.read_text()), None)
        observed.set()
        return action_wait.Pending(None)

    def no_sleep(delay):
        pytest.fail(f"bell wait slept for {delay}")

    monkeypatch.setattr(action_wait, "time", SimpleNamespace(
        monotonic=time.monotonic, sleep=no_sleep))
    with ThreadPoolExecutor() as executor:
        pending = executor.submit(action_wait.await_action, observe_record,
            generation=None, patience=action_wait.WaitUntil(time.monotonic() + 5),
            profile=action_wait.AwaitProfile.SYNCHRONOUS, tool="test", subject=KEY,
            notice=lambda event: None, bell=bell.client, bell_key=KEY)
        assert observed.wait(5)
        _rename_bell_record(bell.root)
        assert pending.result(6).value == {"status": "executed"}


def test_bell_is_declared_only_on_queue_host():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
    import supervise
    assert supervise.ROLE_SCRIPTS["bell"] == "pbbell.py"
    roles = dict(supervise.declared_roles("dl380g10"))
    assert roles["bell"].count("--listen") == 3
    for host in ("sparky", "sparklina"):
        assert "bell" not in dict(supervise.declared_roles(host))


@pytest.mark.parametrize("frontend", ["pbwait", "pbrun"])
def test_frontends_use_the_real_bell_after_an_inconclusive_read(bell, monkeypatch, frontend):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
    import pbwait
    from prismabuild import action_client

    path = bell.root / "done" / f"{KEY}.json"
    observed = threading.Event()

    def pool_read(*args, **kwargs):
        if path.exists():
            return (("done", json.loads(path.read_text())), None)
        observed.set()
        return None, None

    def detached_read(*args, state, **kwargs):
        state['found'] = ('pool', None, {})
        if path.exists():
            return {'status': 'executed'}
        observed.set()
        return None

    def forbid_frontend_sleep(delay):
        pytest.fail(f"{frontend} slept for {delay} on a healthy bell")

    monkeypatch.setattr(queue_bell, "Client", lambda root: bell.client)
    monkeypatch.setattr(action_wait, "time", SimpleNamespace(
        monotonic=time.monotonic, sleep=forbid_frontend_sleep))
    monkeypatch.setattr(action_client, "bounded_outcome_observation", pool_read)
    monkeypatch.setattr(action_client, "bounded_outcome_render", lambda *a, **kw: {
        'summary': {'status': 'executed', 'detail': {'returncode': 0}}})
    monkeypatch.setattr(pbwait, "_look_once_checked", detached_read)
    queue = SimpleNamespace(root=bell.root, records=SimpleNamespace(root=bell.root))
    deadline = time.monotonic() + 5
    with ThreadPoolExecutor() as executor:
        if frontend == "pbrun":
            pending = executor.submit(action_client.await_pool_outcome, queue, KEY,
                                       patience=action_wait.WaitUntil(deadline))
        else:
            pending = executor.submit(pbwait.wait_one, queue, KEY, cas=None, deadline=deadline)
        assert observed.wait(5)
        _rename_bell_record(bell.root)
        result = pending.result(6)
    if frontend == "pbrun":
        assert isinstance(result.observed, action_wait.Ready)
    else:
        assert result['status'] == 'executed'


def test_shutdown_wakes_and_joins_a_pending_long_poll(tmp_path, monkeypatch):
    for directory in queue_bell.STATE_DIRS:
        (tmp_path / directory).mkdir()
    entered = threading.Event()
    with ThreadPoolExecutor() as executor:
        with queue_notifier.BellService(tmp_path, [("127.0.0.1", 0)]) as service:
            client = queue_bell.Client(tmp_path, host="127.0.0.1",
                                      port=service.servers[0].server_address[1])
            real_wait = service.ring.wait

            def entering_shutdown_wait(*args):
                entered.set()
                return real_wait(*args)

            monkeypatch.setattr(service.ring, "wait", entering_shutdown_wait)
            pending = executor.submit(client.wait, client.now(), (), (), 300)
            assert entered.wait(5)
        assert pending.result(2)["reset"] is True
    assert not service.thread.is_alive()


def test_entrypoint_refuses_a_second_role_without_touching_queue(tmp_path, monkeypatch):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
    import pbbell
    from prismabuild import role_session

    monkeypatch.setattr(role_session, "ROLE_LOCK_ROOT", tmp_path / "locks")
    with role_session.role_singleton(Path(pbbell.__file__)):
        assert pbbell.main(["--queue-root", str(tmp_path / "absent")]) == 3

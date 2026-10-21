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


def test_another_queue_on_the_same_host_cannot_promise_a_callback(bell):
    other = queue_bell.Client(bell.root.parent / "other-queue",
                              host="127.0.0.1", port=bell.client.port)
    with pytest.raises(queue_bell.Unavailable):
        other.now()


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
    ("192.168.1.107:/storage_pool/shared", "nfs4", "192.168.1.107"),
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
    assert [roles["bell"][i + 1] for i, argument in enumerate(roles["bell"])
            if argument == "--listen"] == [
                "127.0.0.1", "10.100.98.3", "10.100.99.3", "192.168.1.107"]
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


@pytest.mark.parametrize("relative", [
    f"publication-endings/{KEY}/generation.json",
    f"withdrawn/decisions/{KEY}/generation.json",
    f"attempts/{KEY}/generation/00000001.json",
    f"withdrawn/superseded/{KEY}.123.withdrawal.json",
    f"claimed/{KEY}.json",  # resource-scope/container cleanup is persisted here
    f"withdrawn/{KEY}.json",  # the completed cleanup summary
])
def test_every_authoritative_namespace_wakes_its_action(bell, relative):
    cursor = bell.client.now()
    path = bell.root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"resource_scope_cleanup":{"complete":true}}')
    reply = bell.client.wait(cursor, (), (KEY,), 0.3)
    assert reply.get("events"), relative
    assert all(event["key"] == KEY for event in reply["events"])


@pytest.mark.parametrize("mutation", ["hardlink", "unlink", "rename-out"])
def test_namespace_entry_changes_wake_waiters(bell, mutation):
    path = bell.root / "done" / f"{KEY}.json"
    source = bell.root / "source"
    source.write_text("{}")
    if mutation != "hardlink":
        path.write_text("{}")
        # Consume the preceding close-write before taking the mutation cursor.
        bell.client.wait(bell.client.now(), (), (), 0.05)
    cursor = bell.client.now()
    if mutation == "hardlink":
        os.link(source, path)
    elif mutation == "unlink":
        path.unlink()
    else:
        path.rename(bell.root / "removed")
    assert bell.client.wait(cursor, (), (KEY,), 0.3).get("events")


@pytest.mark.parametrize("endpoint,part", [("now", "headers"), ("now", "body"),
                                           ("wait", "headers"), ("wait", "body")])
def test_hostile_response_dribble_cannot_extend_deadline(tmp_path, endpoint, part):
    stopped = threading.Event()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        client = queue_bell.Client(tmp_path, host="127.0.0.1", port=listener.getsockname()[1])

        def dribble_bell_response():
            with listener.accept()[0] as connection:
                connection.recv(65536)
                payload = b'{"epoch":"' + b"a" * 32 + b'","seq":0,"events":[]}'
                header = f"HTTP/1.1 200 OK\r\nContent-Length: {len(payload)}\r\n\r\n".encode()
                if part == "body":
                    connection.sendall(header)
                    wire = payload
                else:
                    wire = header + payload
                for byte in wire:
                    try:
                        connection.sendall(bytes([byte]))
                    except OSError:
                        break
                    # The interval creates the hostile peer's byte schedule.
                    if stopped.wait(0.025):
                        break

        peer = threading.Thread(target=dribble_bell_response)
        peer.start()
        start = time.monotonic()
        try:
            with pytest.raises(queue_bell.Unavailable):
                if endpoint == "now":
                    client.now(budget_s=0.15)
                else:
                    client.wait(queue_bell.Cursor("a" * 32, 0), (), (), 0.15)
            assert time.monotonic() - start < 0.6
        finally:
            stopped.set()
            peer.join(3)
            assert not peer.is_alive()


@pytest.mark.parametrize("reply", [{"reset": True}, {"events": []}])
def test_repeated_unusable_bell_replies_enter_capped_backoff(monkeypatch, reply):
    clock, sleeps = [0.0], []

    def unusable_reply_sleep(delay):
        sleeps.append(delay)
        clock[0] += delay

    class BrokenBell:
        calls = 0

        def now(self, **kwargs):
            return queue_bell.Cursor("a" * 32, 0)

        def wait(self, *args):
            self.calls += 1
            assert self.calls < 30, "immediate replies caused a busy reread loop"
            return {"epoch": "a" * 32, "seq": 0, **reply}

    monkeypatch.setattr(action_wait, "time", SimpleNamespace(
        monotonic=lambda: clock[0], sleep=unusable_reply_sleep))
    result = action_wait.await_action(
        lambda **kwargs: action_wait.Pending(None), generation=None,
        patience=action_wait.WaitUntil(20), profile=action_wait.AwaitProfile.SYNCHRONOUS,
        tool="test", subject=KEY, notice=lambda event: None, bell=BrokenBell(), bell_key=KEY)
    assert isinstance(result, action_wait.PatienceExpired)
    assert sleeps[:6] == [0.25, 0.5, 1, 2, 4, 5]
    assert sum(sleeps) == 20


def test_overload_is_503_and_client_unavailable_across_listeners(tmp_path):
    for directory in queue_bell.STATE_DIRS:
        (tmp_path / directory).mkdir()
    with queue_notifier.BellService(tmp_path, [("127.0.0.1", 0), ("127.0.0.1", 0)],
                                    max_connections=1) as service:
        port = service.servers[0].server_address[1]
        second = service.servers[1].server_address[1]
        with socket.create_connection(("127.0.0.1", port)) as slow:
            slow.sendall(b"GET /v1/now HTTP/1.1\r\n")
            with service.workers.lock:
                assert service.workers.lock.wait_for(lambda: len(service.workers.active) == 1, 2)
            client = queue_bell.Client(tmp_path, host="127.0.0.1", port=second)
            with pytest.raises(queue_bell.Unavailable, match="503"):
                client.now()
    assert not service.workers.active
    assert all(not thread.is_alive() for thread in service.workers.threads)


def test_slowloris_headers_have_absolute_deadline(bell, monkeypatch):
    monkeypatch.setattr(queue_notifier, "REQUEST_TIMEOUT_S", 0.2)
    stopped = threading.Event()
    with socket.create_connection(("127.0.0.1", bell.client.port)) as connection:
        connection.settimeout(2)
        connection.sendall(b"GET /v1/now HTTP/1.1\r\nX-Slow: ")

        def drip_bell_headers():
            while not stopped.wait(0.025):
                try:
                    connection.sendall(b"x")
                except OSError:
                    break

        peer = threading.Thread(target=drip_bell_headers)
        peer.start()
        try:
            start = time.monotonic()
            assert connection.recv(1) == b""
            assert time.monotonic() - start < 0.8
        finally:
            stopped.set()
            peer.join(2)


def test_disconnected_long_poll_releases_thread_without_timer(bell, monkeypatch):
    entered = threading.Event()
    real_wait = bell.service.ring.wait

    def enter_disconnected_bell_wait(*args):
        entered.set()
        return real_wait(*args)

    monkeypatch.setattr(bell.service.ring, "wait", enter_disconnected_bell_wait)
    cursor = bell.client.now()
    with socket.create_connection(("127.0.0.1", bell.client.port)) as connection:
        connection.sendall(f"GET /v1/wait?since={cursor}&timeout=300 HTTP/1.1\r\nHost: local\r\n\r\n".encode())
        assert entered.wait(2)
        with bell.service.workers.lock:
            threads = list(bell.service.workers.active)
    for thread in threads:
        thread.join(1)
        assert not thread.is_alive()
    assert not bell.service.ring.waiters


def test_only_matching_waiter_is_notified(bell, monkeypatch):
    entered = threading.Event()
    # Observe ring scans through the actual wait function's select boundary.
    from prismabuild import queue_bell_events
    actual_select = queue_bell_events.select.select
    subscriptions, wakeups = [], []

    def select_bell_subscription(readers, *args):
        if len(readers) == 2:
            subscriptions.append(readers[0])
            if len(subscriptions) == 2:
                entered.set()
        ready = actual_select(readers, *args)
        if readers[0] in subscriptions and readers[0] in ready[0]:
            wakeups.append(readers[0])
        return ready

    monkeypatch.setattr(queue_bell_events.select, "select", select_bell_subscription)
    with ThreadPoolExecutor() as executor:
        cursor = bell.client.now()
        pending = executor.submit(bell.client.wait, cursor, ("done",), (KEY,), 2)
        other = executor.submit(bell.client.wait, cursor, ("done",), ("b" * 64,), 2)
        assert entered.wait(2)
        bell.service.ring.append("done", KEY)
        assert pending.result(2)["events"]
        assert len(wakeups) == 1
        bell.service.ring.close()
        assert other.result(2)["reset"]


def test_optional_namespace_creation_and_populated_directory_rename(bell):
    (bell.root / "publication-endings").rmdir()
    # Recreate lazily as on an old queue where this namespace never existed.
    cursor = bell.client.now()
    source = bell.root / "populated"
    (source / KEY).mkdir(parents=True)
    (source / KEY / "generation.json").write_text("{}")
    source.rename(bell.root / "publication-endings")
    reply = bell.client.wait(cursor, (), (KEY,), 2)
    assert reply.get("reset") or reply.get("events")
    cursor = bell.client.now()
    (bell.root / "publication-endings" / KEY / "next.json").write_text("{}")
    assert bell.client.wait(cursor, (), (KEY,), 2)["events"]


def test_generation_rollback_retires_real_role(tmp_path):
    import select
    import shutil
    import subprocess

    fleet = tmp_path / "fleet"
    generation = fleet / "runtime-generations" / "new"
    previous = fleet / "runtime-generations" / "previous"
    previous.mkdir(parents=True)
    (generation / "tools" / "fleet").mkdir(parents=True)
    checkout = Path(__file__).resolve().parents[1]
    (generation / "src").symlink_to(checkout / "src", target_is_directory=True)
    entrypoint = generation / "tools" / "fleet" / "pbbell.py"
    shutil.copyfile(checkout / "tools" / "fleet" / "pbbell.py", entrypoint)
    (fleet / "repo").symlink_to(generation, target_is_directory=True)
    root = tmp_path / "queue"
    for directory in queue_bell.STATE_DIRS:
        (root / directory).mkdir(parents=True)
    ready_read, ready_write = os.pipe()
    program = '''
import os, runpy, sys
sys.path.insert(0, sys.argv[1])
from prismabuild import queue_notifier, role_session
from pathlib import Path
role_session.ROLE_LOCK_ROOT = Path(sys.argv[2])
original = queue_notifier.BellService.__enter__
ready_fd = int(sys.argv[3])
def announce_bell(self):
    result = original(self)
    os.write(ready_fd, b"R")
    os.close(ready_fd)
    return result
queue_notifier.BellService.__enter__ = announce_bell
entrypoint, root, port = sys.argv[4:]
sys.argv = [entrypoint, "--queue-root", root, "--port", port]
runpy.run_path(entrypoint, run_name="__main__")
'''
    with socket.socket() as bound:
        bound.bind(("127.0.0.1", 0))
        port = bound.getsockname()[1]
    process = subprocess.Popen([sys.executable, "-c", program, str(checkout / "src"),
        str(tmp_path / "locks"), str(ready_write), str(entrypoint), str(root), str(port)],
        pass_fds=(ready_write,), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    os.close(ready_write)
    try:
        if not select.select([ready_read], [], [], 5)[0]:
            process.terminate()
            output, errors = process.communicate(timeout=5)
            pytest.fail("role startup blocked: " + errors.decode())
        assert os.read(ready_read, 1) == b"R"
        client = queue_bell.Client(root, host="127.0.0.1", port=port)
        assert client.now().epoch
        replacement = fleet / "replacement"
        replacement.symlink_to(previous, target_is_directory=True)
        replacement.replace(fleet / "repo")
        process.communicate(timeout=3)
        assert process.returncode == 0
        with pytest.raises(queue_bell.Unavailable):
            client.now()
    finally:
        os.close(ready_read)
        if process.poll() is None:
            process.terminate()
            process.communicate(timeout=5)


def test_new_directory_is_watched_before_rescan(bell, monkeypatch):
    watch = bell.service.watch
    installed = threading.Event()
    original_add = watch._add

    def add_then_publish_bell_record(path, relative):
        original_add(path, relative)
        if relative == Path("publication-endings") / KEY:
            # Land a complete record after watch installation, before scanning.
            (path / "generation.json").write_text("{}")
            installed.set()

    monkeypatch.setattr(watch, "_add", add_then_publish_bell_record)
    cursor = bell.client.now()
    (bell.root / "publication-endings" / KEY).mkdir()
    assert installed.wait(2)
    assert bell.client.wait(cursor, queue_bell.TERMINAL_DIRS, (KEY,), 2)["events"]
    cursor = bell.client.now()
    (bell.root / "publication-endings" / KEY / "next.json").write_text("{}")
    assert bell.client.wait(cursor, queue_bell.TERMINAL_DIRS, (KEY,), 2)["events"]


@pytest.mark.parametrize("relative", [
    f"publication-endings/{KEY}/generation.json",
    f"withdrawn/decisions/{KEY}/generation.json",
    f"attempts/{KEY}/generation/00000001.json",
    f"claimed/{KEY}.json", f"withdrawn/{KEY}.json",
])
def test_action_wait_observes_nested_endings_and_cleanup(bell, monkeypatch, relative):
    observed = threading.Event()
    path = bell.root / relative

    def read_bell_authoritative_input(**kwargs):
        if path.exists():
            return action_wait.Ready(json.loads(path.read_text()), None)
        observed.set()
        return action_wait.Pending(None)

    def forbid_nested_wait_sleep(delay):
        pytest.fail(f"healthy bell slept {delay}s for {relative}")

    monkeypatch.setattr(action_wait, "time", SimpleNamespace(
        monotonic=time.monotonic, sleep=forbid_nested_wait_sleep))
    with ThreadPoolExecutor() as executor:
        pending = executor.submit(action_wait.await_action, read_bell_authoritative_input,
            generation=None, patience=action_wait.WaitUntil(time.monotonic() + 3),
            profile=action_wait.AwaitProfile.SYNCHRONOUS, tool="test", subject=KEY,
            notice=lambda event: None, bell=bell.client, bell_key=KEY)
        assert observed.wait(2)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.parent / ".staged-ending"
        temporary.write_text('{"cleanup":{"complete":true}}')
        temporary.replace(path)
        assert pending.result(3).value == {"cleanup": {"complete": True}}


def test_real_requeue_supersession_wakes_exact_publication_waiter(bell):
    from prismabuild import endings, pool
    from test_pool_requeue_preserves_newer_generation import _publish, DEMAND, KEY as collision_key

    queue = pool.PoolQueue(bell.root)
    queue.announce(host=socket.gethostname(), tags=(), has_gpu=False,
        capacity=DEMAND, cpu_tiers={"preferred": sorted(os.sched_getaffinity(0))[:1], "fallback": []})
    _publish(queue)
    original = queue.claim(owner="bell-requeue-test", capacity=DEMAND)
    assert original is not None
    _publish(queue)
    cursor = bell.client.now()
    with ThreadPoolExecutor() as executor:
        pending = executor.submit(bell.client.wait, cursor, ("publication-endings",),
                                  (collision_key,), 3)
        queue.finish(collision_key, status="failed", detail={"returncode": 17},
                     claim_snapshot=original)
        assert pending.result(3)["events"]
    decision, generation = action_wait.outcome_poll(queue, collision_key, original["published_unix"])
    assert isinstance(decision, endings.PublicationSupersession)
    assert generation == original["published_unix"]
    assert not queue.ledger().held()

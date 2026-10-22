"""Additive, positive bell coverage over scratch queues only (#1939)."""
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import os
from pathlib import Path
import socket
import threading
import time
from urllib.parse import urlencode

import pytest

from prismabuild import queue_bell, queue_bell_events, queue_notifier
from prismabuild.queue_bell_topics import TOPICS
from test_queue_bell import bell, KEY


def _v2_get(bell, endpoint, **query):
    connection = http.client.HTTPConnection("127.0.0.1", bell.client.port, timeout=3)
    try:
        connection.request("GET", endpoint + ("?" + urlencode(query) if query else ""),
                           headers={"X-PrismaBuild-Queue": str(bell.root)})
        response = connection.getresponse()
        raw = response.read()
        assert response.status == 200, (response.status, raw)
        return json.loads(raw)
    finally:
        connection.close()


def _v2_poll(bell, before, **overrides):
    query = dict(since=f"{before['epoch']}:{before['seq']}",
                 coverage_version=before["coverage"]["version"], topics="done", ids=KEY, timeout=0.1)
    query.update(overrides)
    return _v2_get(bell, "/v2/wait", **query)


def test_v2_declares_covered_topics_and_projects_v1_events(bell):
    before = _v2_get(bell, "/v2/now")
    assert before["coverage"]["version"] >= 1
    assert before["coverage"]["topics"] == sorted(TOPICS)
    cursor = bell.client.now()
    bell.service.ring.append("done", KEY)
    old = bell.client.wait(cursor, ("done",), (KEY,), 1)
    new = _v2_poll(bell, before)
    assert new["events"] == [{"seq": old["events"][0]["seq"], "topic": "done", "id": KEY}]
    assert old == {"epoch": new["epoch"], "seq": new["seq"],
                   "events": [{"seq": new["seq"], "dir": "done", "key": KEY}]}
    assert bell.client._request("/v1/now", deadline=time.monotonic() + 1) == {
        "epoch": new["epoch"], "seq": new["seq"]}


def test_v2_filters_and_healthy_renewal_do_no_discovery(bell, monkeypatch):
    before = _v2_get(bell, "/v2/now")
    bell.service.ring.append("ready", KEY)
    bell.service.ring.append("done", "b" * 64)

    def forbid_renewal_scan(*args, **kwargs):
        pytest.fail("healthy renewal enumerated a queue")

    monkeypatch.setattr(bell.service.watch, "_tree", forbid_renewal_scan)
    assert _v2_poll(bell, before)["events"] == []
    assert _v2_poll(bell, before, topics="ready")["events"][0]["topic"] == "ready"
    assert len(_v2_poll(bell, before, topics="", ids="")["events"]) == 2


@pytest.mark.parametrize("field,value", [
    ("topics", "../done"), ("topics", "done," * 40),
    ("ids", "../secret"), ("ids", "A" * 64), ("ids", "x" * 1025),
    ("ids", ",".join(f"{i:064x}" for i in range(129))),
    ("coverage_version", "0"), ("coverage_version", "nan"),
    ("timeout", "nan"), ("timeout", "-1"),
], ids=["topic-path", "topic-count", "id-path", "id-case", "id-length", "id-count",
        "zero-version", "nan-version", "nan-timeout", "negative-timeout"])
def test_v2_rejects_unbounded_or_noncanonical_filters(bell, field, value):
    before = _v2_get(bell, "/v2/now")
    with pytest.raises(AssertionError, match="400"):
        _v2_poll(bell, before, **{field: value})


def test_v2_typed_identifier_contract_does_not_assume_all_ids_are_actions():
    from prismabuild.queue_bell_topics import Topic
    topic = Topic("example-host", "example-host", r"[a-z][a-z0-9-]{0,62}")
    assert topic.accepts_id("dl380g10")
    assert not topic.accepts_id("../dl380g10")
    assert not topic.accepts_id("x" * 1025)
    assert not topic.accepts_id("a" * 64)


@pytest.mark.parametrize("loss", ["epoch", "future", "ring", "reset", "coverage", "partial"])
def test_v2_loss_never_reports_complete_wait(bell, loss):
    before = _v2_get(bell, "/v2/now")
    overrides = {}
    if loss == "epoch":
        overrides["since"] = "0" * 32 + ":0"
    elif loss == "future":
        overrides["since"] = f"{before['epoch']}:{before['seq'] + 1}"
    elif loss == "ring":
        for _ in range(queue_notifier.DEFAULT_RING_SIZE + 1):
            bell.service.ring.append("done", KEY)
    elif loss == "reset":
        bell.service.ring.reset()
    elif loss == "coverage":
        bell.service.ring.set_coverage(("ready",))
    else:
        overrides["topics"] = "tiers"
    reply = _v2_poll(bell, before, **overrides)
    assert reply["reset"] is True
    assert reply["reason"]
    assert "events" not in reply
    assert reply["coverage"]["topics"] == (["ready"] if loss == "coverage"
                                                else sorted(TOPICS))


def test_v2_positive_client_negotiation_and_wrong_queue(bell):
    before = bell.client.now_v2(("done",))
    with pytest.raises(queue_bell.Unavailable, match="coverage"):
        bell.client.now_v2(("tiers",))
    other = queue_bell.Client(bell.root / "other", host="127.0.0.1", port=bell.client.port)
    with pytest.raises(queue_bell.Unavailable, match="503"):
        other.now_v2(("done",))
    bell.service.ring.append("done", KEY)
    reply = bell.client.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
        before["coverage"]["version"], ("done",), (KEY,), 1)
    assert reply["events"][0]["id"] == KEY


def test_v2_shutdown_wakes_and_joins_subscription(bell, monkeypatch):
    before = _v2_get(bell, "/v2/now")
    entered = threading.Event()
    original = bell.service.ring.wait_v2

    def entering_v2_shutdown(*args):
        entered.set()
        return original(*args)

    monkeypatch.setattr(bell.service.ring, "wait_v2", entering_v2_shutdown)
    with ThreadPoolExecutor() as executor:
        pending = executor.submit(_v2_poll, bell, before, timeout=2)
        assert entered.wait(1)
        bell.service.ring.close()
        reply = pending.result(2)
    assert reply["reset"] and reply["reason"] == "shutdown"
    assert reply["coverage"]["topics"] == []
    assert reply["coverage"]["version"] > before["coverage"]["version"]


def test_v2_overflow_failure_cannot_advertise_complete_coverage(bell, monkeypatch):
    before = _v2_get(bell, "/v2/now")

    def exhausted_v2_watch(*args, **kwargs):
        raise OSError("watch capacity exhausted")

    monkeypatch.setattr(bell.service.watch, "_tree", exhausted_v2_watch)
    with pytest.raises(OSError, match="exhausted"):
        bell.service.watch.consume(queue_notifier.EVENT_HEADER.pack(
            -1, queue_notifier.IN_Q_OVERFLOW, 0, 0), bell.service.ring)
    after = _v2_get(bell, "/v2/now")
    assert after["coverage"]["topics"] == []
    assert _v2_poll(bell, before)["reset"]


def test_v2_overflow_rebuild_and_populated_move_preserve_hints(bell):
    before = _v2_get(bell, "/v2/now")
    bell.service.watch.consume(queue_notifier.EVENT_HEADER.pack(
        -1, queue_notifier.IN_Q_OVERFLOW, 0, 0), bell.service.ring)
    assert _v2_poll(bell, before)["reset"]
    before = _v2_get(bell, "/v2/now")
    source = bell.root / "incoming"
    source.mkdir()
    (source / "generation.json").write_text("record bodies are not JSON and must not be read")
    source.rename(bell.root / "publication-endings" / KEY)
    assert _v2_poll(bell, before, topics="publication-endings", timeout=2)["events"]
    before = _v2_get(bell, "/v2/now")
    (bell.root / "publication-endings" / KEY / "next.json").write_text("unparsed")
    assert _v2_poll(bell, before, topics="publication-endings", timeout=2)["events"]


def test_v2_symlinks_are_never_traversed(bell, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / f"{KEY}.json").write_text("private")
    before = _v2_get(bell, "/v2/now")
    (bell.root / "publication-endings" / KEY).symlink_to(outside, target_is_directory=True)
    reply = _v2_poll(bell, before, topics="publication-endings")
    assert all(path != Path("publication-endings") / KEY
               for path in bell.service.watch.directories.values())
    assert not reply.get("reset")


def test_v2_connection_cap_is_shared_with_v1(bell):
    bell.service.workers.capacity = 1
    with socket.create_connection(("127.0.0.1", bell.client.port)) as slow:
        slow.sendall(b"GET /v1/now HTTP/1.1\r\n")
        with bell.service.workers.lock:
            assert bell.service.workers.lock.wait_for(lambda: len(bell.service.workers.active) == 1, 2)
        with pytest.raises(queue_bell.Unavailable, match="503"):
            bell.client.now_v2(("done",))


def test_v2_watch_and_scan_cannot_be_redirected_by_replacement(tmp_path, monkeypatch):
    root = tmp_path / "queue"
    for directory in queue_bell.STATE_DIRS:
        (root / directory).mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / ("b" * 64 + ".json")).write_text("private")
    original = queue_bell_events.InotifyQueue._add
    scanned = []
    original_scandir = os.scandir

    def replace_after_watch(self, path, relative):
        original(self, path, relative)
        if relative == Path("ready"):
            (root / "ready").rename(root / "old-ready")
            (root / "ready").symlink_to(outside, target_is_directory=True)

    def record_scan_target(path):
        if isinstance(path, int):
            scanned.append(os.fstat(path).st_ino)
        return original_scandir(path)

    monkeypatch.setattr(queue_bell_events.InotifyQueue, "_add", replace_after_watch)
    monkeypatch.setattr(os, "scandir", record_scan_target)
    watch = queue_bell_events.InotifyQueue(root)
    try:
        assert outside.stat().st_ino not in scanned
    finally:
        watch.close()


def test_v2_caps_timeout_and_coverage_change_wakes_pending_waiter(bell, monkeypatch):
    before = _v2_get(bell, "/v2/now")
    monkeypatch.setattr(queue_notifier, "MAX_WAIT_S", 0.01)
    assert _v2_poll(bell, before, timeout=3000)["events"] == []
    monkeypatch.setattr(queue_notifier, "MAX_WAIT_S", 300)
    entered = threading.Event()
    original = bell.service.ring.wait_v2

    def enter_coverage_wait(*args):
        entered.set()
        return original(*args)

    monkeypatch.setattr(bell.service.ring, "wait_v2", enter_coverage_wait)
    with ThreadPoolExecutor() as executor:
        pending = executor.submit(_v2_poll, bell, before, timeout=2)
        assert entered.wait(1)
        bell.service.ring.set_coverage(("ready",))
        reply = pending.result(2)
    assert reply["reset"] and reply["reason"] == "coverage_changed"
    assert reply["coverage"]["version"] > before["coverage"]["version"]


def test_v2_disconnect_releases_subscription_without_renewal(bell, monkeypatch):
    before = _v2_get(bell, "/v2/now")
    entered = threading.Event()
    original = bell.service.ring.wait_v2

    def enter_v2_disconnect(*args):
        entered.set()
        return original(*args)

    monkeypatch.setattr(bell.service.ring, "wait_v2", enter_v2_disconnect)
    query = urlencode(dict(since=f"{before['epoch']}:{before['seq']}",
        coverage_version=before["coverage"]["version"], timeout=300))
    with socket.create_connection(("127.0.0.1", bell.client.port)) as connection:
        connection.sendall((f"GET /v2/wait?{query} HTTP/1.1\r\nHost: local\r\n"
                            f"X-PrismaBuild-Queue: {bell.root}\r\n\r\n").encode())
        assert entered.wait(2)
        with bell.service.workers.lock:
            threads = list(bell.service.workers.active)
    for thread in threads:
        thread.join(1)
        assert not thread.is_alive()
    assert not bell.service.ring.waiters


@pytest.mark.parametrize("corruption", ["missing-coverage", "partial", "wrong-version", "wrong-epoch",
                                        "string-sequence", "path-id", "wrong-topic"])
def test_v2_client_rejects_unproved_success(tmp_path, monkeypatch, corruption):
    client = queue_bell.Client(tmp_path, host="127.0.0.1")
    reply = {"epoch": "a" * 32, "seq": 1, "coverage": {"version": 1, "topics": ["done"]},
             "events": [{"seq": 1, "topic": "done", "id": KEY}]}
    if corruption == "missing-coverage":
        reply.pop("coverage")
    elif corruption == "partial":
        reply["coverage"]["topics"] = ["ready"]
    elif corruption == "wrong-version":
        reply["coverage"]["version"] = 2
    elif corruption == "wrong-epoch":
        reply["epoch"] = "b" * 32
    elif corruption == "string-sequence":
        reply["seq"] = "1"
    elif corruption == "path-id":
        reply["events"][0]["id"] = "../secret"
    else:
        reply["events"][0]["topic"] = "ready"
    monkeypatch.setattr(client, "_request", lambda *args, **kwargs: reply)
    with pytest.raises(queue_bell.Unavailable):
        client.wait_v2(queue_bell.Cursor("a" * 32, 0), 1, ("done",), (KEY,), 1)

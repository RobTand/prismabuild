"""Adaptive/denial changes invalidate consumers without a READY publication."""
import os
from pathlib import Path
import select
import shutil

import pytest

from prismabuild import queue_bell, queue_bell_events
from test_queue_bell_host_inputs import host_feed
from test_pool_cpu_work_yields_gpu_host import fleet, _cpu_host_denies, CPU_HOST


@pytest.mark.parametrize("topic,relative", [
    ("adaptive", "reservations/worker-1.lab.example/adaptive/cpu-sample.json"),
    ("adaptive", "reservations/worker-1.lab.example/adaptive/gpu-state.json"),
    ("claim-denials", "reservations/worker-1.lab.example/adaptive/claim-denials.json"),
    ("passes", "passes/" + "a" * 64 + ".json"),
    ("denial-transitions", "denial-transitions/" + "a" * 64 + ".json"),
])
@pytest.mark.parametrize("mutation", ["in-place", "replace", "remove"])
def test_old_writer_adaptive_inputs_are_covered(host_feed, topic, relative, mutation):
    feed = host_feed
    path = feed.root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("unparsed old-writer record")
    assert select.select([feed.watch.fd], [], [], 2)[0]
    feed.watch.consume(os.read(feed.watch.fd, 65536), feed.ring)
    before = feed.ring.now_v2()
    assert topic in before["coverage"]["topics"]
    if mutation == "in-place":
        path.write_text("still names-only")
    elif mutation == "replace":
        temporary = path.with_name(".replacement")
        temporary.write_text("replacement")
        temporary.replace(path)
    else:
        path.unlink()
    assert select.select([feed.watch.fd], [], [], 2)[0], "missing kernel callback"
    feed.watch.consume(os.read(feed.watch.fd, 65536), feed.ring)
    identity = feed.host if topic in ("adaptive", "claim-denials") else "a" * 64
    reply = feed.ring.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
        {topic}, {identity}, 0, feed.ring.now_v2()["coverage"]["version"])
    assert reply.get("events"), reply
    assert all(set(event) == {"seq", "topic", "id"} for event in reply["events"])
    assert {event["id"] for event in reply["events"]} == {identity}
    assert not list((feed.root / "ready").iterdir())


def test_peer_refusal_hint_reaches_authoritative_1262_reevaluation(fleet):
    from prismabuild import adaptive_cpu
    from prismabuild.queue_bell_input_changes import InputChanges, InputObservation, claim_denial_meaning

    queue, announce, publish, claim = fleet
    item = publish("a" * 64, {"cpu": 2, "mem_gb": 2})
    _cpu_host_denies(queue, item, "transition_busy")
    assert claim() is None
    changes = InputChanges()
    path = queue.ledger(CPU_HOST).base / "adaptive" / "claim-denials.json"
    read = lambda: InputObservation(claim_denial_meaning(adaptive_cpu.read_json(path)), "confirmed")
    changes.observe("claim-denials", CPU_HOST, read())
    ring = queue_bell_events.EventRing()
    for directory in queue_bell.REQUIRED_STATE_DIRS:
        (queue.root / directory).mkdir(exist_ok=True)
    watch = queue_bell_events.InotifyQueue(queue.root)
    ring.set_coverage(watch.covered_topics)
    try:
        before = ring.now_v2()
        ready_bytes = (queue.root / "ready" / ("a" * 64 + ".json")).read_bytes()
        _cpu_host_denies(queue, item, "reservation_unavailable")
        assert select.select([watch.fd], [], [], 2)[0]
        watch.consume(os.read(watch.fd, 65536), ring)
        reply = ring.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
            {"claim-denials"}, {CPU_HOST}, 0, ring.now_v2()["coverage"]["version"])
        assert reply.get("events"), reply
        assert (queue.root / "ready" / ("a" * 64 + ".json")).read_bytes() == ready_bytes
        assert changes.observe("claim-denials", CPU_HOST, read())
        claimed = claim()
        assert claimed and claimed["action_key"] == "a" * 64
    finally:
        watch.close()


@pytest.mark.parametrize("relative,topic", [
    ("reservations/peer/adaptive/gpu-state.json", "adaptive"),
    ("reservations/peer/adaptive/claim-denials.json", "claim-denials"),
    ("passes/" + "a" * 64 + ".json", "passes"),
    ("denial-transitions/" + "a" * 64 + ".json", "denial-transitions"),
])
@pytest.mark.parametrize("shape", ["symlink", "hardlink", "fifo", "unreadable"])
def test_unwatchable_adaptive_inputs_never_claim_complete_coverage(tmp_path, relative, topic, shape):
    root = tmp_path / "queue"
    for directory in queue_bell.REQUIRED_STATE_DIRS:
        (root / directory).mkdir(parents=True)
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    outside = tmp_path / "outside"
    outside.write_text("{}")
    if shape == "symlink":
        path.symlink_to(outside)
    elif shape == "hardlink":
        os.link(outside, path)
    elif shape == "fifo":
        os.mkfifo(path)
    else:
        path.write_text("{}")
        path.chmod(0)
    watch = queue_bell_events.InotifyQueue(root)
    try:
        assert topic not in watch.covered_topics
    finally:
        watch.close()
        if shape == "unreadable":
            path.chmod(0o600)


def test_moved_in_adaptive_tree_and_parent_removal_cover_both_topics(host_feed, tmp_path):
    feed = host_feed
    tree = tmp_path / "prepared"
    tree.mkdir()
    for name in ("cpu-sample.json", "claim-denials.json"):
        (tree / name).write_text("{}")
    tree.rename(feed.base / "adaptive")
    assert select.select([feed.watch.fd], [], [], 2)[0]
    feed.watch.consume(os.read(feed.watch.fd, 65536), feed.ring)
    assert {"adaptive", "claim-denials"}.issubset(feed.watch.covered_topics)
    before = feed.ring.now_v2()
    shutil.rmtree(feed.base / "adaptive")
    assert select.select([feed.watch.fd], [], [], 2)[0]
    feed.watch.consume(os.read(feed.watch.fd, 65536), feed.ring)
    for topic in ("adaptive", "claim-denials"):
        reply = feed.ring.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
            {topic}, {feed.host}, 0, feed.ring.now_v2()["coverage"]["version"])
        assert reply.get("events") or reply.get("reset"), reply

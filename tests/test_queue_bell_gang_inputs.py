"""Gang hints reuse common coverage, never gang policy or record authority."""
import errno
import os
from pathlib import Path
import select

import pytest

from prismabuild import queue_bell, queue_bell_events
from prismabuild.queue_bell_input_reads import DirtyInputs
from prismabuild.queue_bell_input_changes import InputChanges, InputObservation
from prismabuild.queue_bell_topics import GangInputs, TopicFilters


GROUP = "a" * 32
OTHER = "b" * 32
KEY = "c" * 64
FILES = (f"{GROUP}.json", f"{GROUP}/elect-0.json", f"{GROUP}/ready-1.json",
         f"{GROUP}/terminal-{KEY}.json", f"{GROUP}/teardown.json",
         f"{GROUP}/started.json", f"{GROUP}/bounded-loan.json")


def queue_root(tmp_path):
    root = tmp_path / "queue"
    for directory in queue_bell.REQUIRED_STATE_DIRS:
        (root / directory).mkdir(parents=True)
    return root


def drain(watch, ring, *, required=True):
    if required:
        assert select.select([watch.fd], [], [], 5)[0], "missing kernel callback"
    for _attempt in range(100):
        try:
            data = os.read(watch.fd, 65536)
        except BlockingIOError:
            return
        watch.consume(data, ring)
    pytest.fail("unbounded callback reconstruction")


@pytest.fixture
def gang_feed(tmp_path):
    root = queue_root(tmp_path)
    watch = queue_bell_events.InotifyQueue(root)
    ring = queue_bell_events.EventRing()
    ring.set_coverage(watch.covered_topics)
    try:
        yield root, watch, ring
    finally:
        watch.close()


def poll(ring, before, *, topic="gangs", identity=GROUP):
    return ring.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
        {topic}, {identity}, 0, before["coverage"]["version"])


@pytest.mark.parametrize("relative", FILES)
@pytest.mark.parametrize("mutation", ["in-place", "replace", "remove"])
def test_legacy_gang_mutations_invalidate_exact_group(gang_feed, relative, mutation):
    root, watch, ring = gang_feed
    path = root / "gangs" / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("deliberately not JSON")
    drain(watch, ring)
    assert "gangs" in watch.covered_topics
    before = ring.now_v2()
    if mutation == "replace":
        temporary = path.with_name(".replacement")
        temporary.write_text("replacement")
        temporary.replace(path)
    elif mutation == "remove":
        path.unlink()
    else:
        with path.open("w") as writer:
            writer.write("legacy writer has not closed yet")
            writer.flush()
            drain(watch, ring)
    drain(watch, ring, required=mutation != "in-place")
    reply = poll(ring, before)
    assert reply.get("events"), reply
    assert {(event["topic"], event["id"]) for event in reply["events"]} == {("gangs", GROUP)}
    assert poll(ring, before, identity=OTHER)["events"] == []
    assert poll(ring, before, topic="done", identity=KEY)["events"] == []


@pytest.mark.parametrize("boundary", ["root", "group"])
def test_populated_moved_in_and_recreated_roots(gang_feed, tmp_path, boundary):
    root, watch, ring = gang_feed
    incoming = tmp_path / "incoming"
    destination = root / "gangs"
    if boundary == "group":
        destination.mkdir()
        drain(watch, ring)
        destination /= GROUP
    leaf = incoming / (GROUP if boundary == "root" else "") / "ready-0.json"
    leaf.parent.mkdir(parents=True)
    leaf.write_text("old ready")
    before = ring.now_v2()
    incoming.rename(destination)
    drain(watch, ring)
    assert poll(ring, before).get("reset") is True
    assert "gangs" in watch.covered_topics
    before = ring.now_v2()
    (destination / (GROUP if boundary == "root" else "") / "ready-0.json").unlink()
    drain(watch, ring)
    assert poll(ring, before).get("events")
    destination.rename(tmp_path / "removed")
    drain(watch, ring)
    destination.mkdir()
    drain(watch, ring)
    assert "gangs" in watch.covered_topics


def test_registration_deletion_between_watch_and_scan_is_replayed(tmp_path, monkeypatch):
    root = queue_root(tmp_path)
    state = root / "gangs" / GROUP
    state.mkdir(parents=True)
    mark = state / "elect-0.json"
    mark.write_text("elected")
    scandir = os.scandir
    deleted = []

    def race(descriptor):
        if Path(f"/proc/self/fd/{descriptor}").resolve() == state and not deleted:
            mark.unlink()
            deleted.append(True)
        return scandir(descriptor)

    monkeypatch.setattr(os, "scandir", race)
    watch = queue_bell_events.InotifyQueue(root)
    try:
        ring = queue_bell_events.EventRing()
        ring.set_coverage(watch.covered_topics)
        before = ring.now_v2()
        assert deleted == [True]
        drain(watch, ring)
        assert poll(ring, before).get("events")
    finally:
        watch.close()


@pytest.mark.parametrize("failure", [errno.ENOSPC, errno.EACCES])
def test_failed_watch_installation_cannot_claim_gang_coverage(gang_feed, monkeypatch, failure):
    root, watch, ring = gang_feed
    state = root / "gangs" / GROUP
    state.mkdir(parents=True)
    add = watch._add

    def refuse(path, relative):
        if relative == Path("gangs") / GROUP:
            raise OSError(failure, "gang watch refused")
        return add(path, relative)

    monkeypatch.setattr(watch, "_add", refuse)
    with pytest.raises(OSError, match="gang watch refused"):
        drain(watch, ring)
    assert "gangs" not in ring.now_v2()["coverage"]["topics"]


@pytest.mark.parametrize("shape", ["symlink", "fifo", "unreadable", "hardlink"])
@pytest.mark.parametrize("relative", [f"{GROUP}.json", f"{GROUP}/ready-0.json"])
def test_unsafe_gang_records_are_not_complete(tmp_path, shape, relative):
    root = queue_root(tmp_path)
    path = root / "gangs" / relative
    path.parent.mkdir(parents=True)
    if shape == "symlink":
        path.symlink_to(tmp_path / "absent")
    elif shape == "fifo":
        os.mkfifo(path)
    else:
        path.write_text("untrusted")
        if shape == "unreadable":
            path.chmod(0)
        else:
            os.link(path, tmp_path / "external")
    watch = queue_bell_events.InotifyQueue(root)
    try:
        assert "gangs" not in watch.covered_topics
    finally:
        watch.close()
        if shape == "unreadable":
            path.chmod(0o600)


def test_ring_loss_overflow_and_lost_watch_require_reconciliation(gang_feed):
    root, watch, _ring = gang_feed
    ring = queue_bell_events.EventRing(capacity=1)
    ring.set_coverage(watch.covered_topics)
    before = ring.now_v2()
    ring.append("gangs", GROUP)
    ring.append("gangs", OTHER)
    assert poll(ring, before)["reason"] == "history_lost"
    state = root / "gangs" / GROUP
    state.mkdir(parents=True)
    (state / "ready-0.json").write_text("ready")
    drain(watch, ring)
    before = ring.now_v2()
    watch.consume(queue_bell_events.EVENT_HEADER.pack(
        -1, queue_bell_events.IN_Q_OVERFLOW, 0, 0), ring)
    drain(watch, ring, required=False)
    assert poll(ring, before).get("reset") is True
    assert "gangs" in watch.covered_topics
    descriptor = next(wd for wd, relative in watch.directories.items()
                      if relative == Path("gangs") / GROUP)
    watch.consume(queue_bell_events.EVENT_HEADER.pack(
        descriptor, queue_bell_events.IN_IGNORED, 0, 0), ring)
    assert "gangs" not in ring.now_v2()["coverage"]["topics"]


def test_heartbeats_do_not_emit_gang_or_outcome_work(gang_feed):
    root, watch, ring = gang_feed
    stamps = root / "lease-heartbeats" / KEY
    stamps.mkdir(parents=True)
    drain(watch, ring)
    before = ring.now_v2()
    for _renewal in range(10):
        (root / "claimed" / f"{KEY}.lease").write_text("lease renewal")
        (stamps / f"{'d' * 64}.json").write_text("claim-bound heartbeat stamp")
    drain(watch, ring)
    assert ring.now_v2()["seq"] == before["seq"]
    assert poll(ring, before)["events"] == []
    assert poll(ring, before, topic="done", identity=KEY)["events"] == []


def test_gang_names_are_bounded_and_repeated_hints_retain_invalidation_generation():
    assert TopicFilters.parse("gangs", GROUP) == ({"gangs"}, {GROUP})
    assert GangInputs.hint(Path(f"gangs/{GROUP}/elect-15.json")) == ("gangs", GROUP)
    for relative in (f"gangs/{GROUP}/elect-16.json", f"gangs/{GROUP}/ready-01.json",
                     f"gangs/{GROUP}/nested/ready-0.json", f"gangs/{GROUP}/.temporary"):
        assert GangInputs.hint(Path(relative)) is None
    dirty = DirtyInputs()
    dirty.invalidate("gangs", GROUP, deadline=100.)
    dirty.invalidate("gangs", GROUP, deadline=200.)
    assert dirty.dirty == {("gangs", GROUP)}
    assert dirty._pending[("gangs", GROUP)].deadline == 100.
    assert dirty._pending[("gangs", GROUP)].generation == 1


def test_replaced_publication_is_not_coalesced_with_previous_member():
    changes = InputChanges()
    original = {"members": [{"index": 0, "action_key": KEY, "published_unix": 10.}]}
    assert changes.observe("gangs", GROUP, InputObservation(original, "confirmed"))
    assert not changes.observe("gangs", GROUP, InputObservation(original, "confirmed"))
    original["members"][0]["published_unix"] = 11.
    assert changes.observe("gangs", GROUP, InputObservation(original, "confirmed"))
    assert changes.observe("gangs", GROUP, InputObservation(None, "absent"))


def test_moved_state_retains_neither_old_group_nor_old_file_watch(gang_feed, tmp_path):
    root, watch, ring = gang_feed
    state = root / "gangs" / GROUP
    state.mkdir(parents=True)
    (state / "ready-0.json").write_text("ready")
    drain(watch, ring)
    state.rename(tmp_path / "outside")
    drain(watch, ring)
    assert not any(relative is not None and not relative.is_absolute()
                   and relative.parts[:2] == ("gangs", GROUP)
                   for relative in watch.directories.values())
    before = ring.now_v2()
    (tmp_path / "outside" / "ready-0.json").write_text("outside write")
    drain(watch, ring, required=False)
    assert ring.now_v2()["seq"] == before["seq"]
    (tmp_path / "outside").rename(root / "gangs" / OTHER)
    drain(watch, ring)
    before = ring.now_v2()
    (root / "gangs" / OTHER / "ready-0.json").write_text("new group")
    drain(watch, ring)
    assert poll(ring, before, identity=OTHER).get("events")
    assert poll(ring, before)["events"] == []


@pytest.mark.parametrize("relative", ["gangs", f"gangs/{GROUP}"])
def test_directory_permission_loss_withdraws_only_gang_coverage(gang_feed, relative):
    root, watch, ring = gang_feed
    path = root / relative
    path.mkdir(parents=True)
    drain(watch, ring)
    path.chmod(0)
    try:
        drain(watch, ring)
        assert "gangs" not in ring.now_v2()["coverage"]["topics"]
        assert "done" in ring.now_v2()["coverage"]["topics"]
    finally:
        path.chmod(0o700)


def test_full_existing_gang_layout_has_a_bounded_watch_footprint(gang_feed):
    root, watch, ring = gang_feed
    baseline = len(watch.directories)
    state = root / "gangs" / GROUP
    state.mkdir(parents=True)
    (state.parent / f"{GROUP}.json").write_text("group")
    for index in range(16):
        (state / f"elect-{index}.json").write_text("elected")
        (state / f"ready-{index}.json").write_text("ready")
        (state / f"terminal-{index:064x}.json").write_text("terminal")
    for name in ("teardown", "started", "bounded-loan"):
        (state / f"{name}.json").write_text(name)
    drain(watch, ring)
    assert len(watch.directories) - baseline == 54
    assert {event["id"] for event in ring.events if event["topic"] == "gangs"} == {GROUP}
    assert "gangs" in watch.covered_topics

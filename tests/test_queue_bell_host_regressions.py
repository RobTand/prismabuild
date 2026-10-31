"""Host input regressions use private scratch watches and deterministic replay."""
import os
from pathlib import Path
import select
import threading

import pytest

from prismabuild import queue_bell, queue_bell_events, queue_bell_input_reads


def drain(watch, ring):
    while select.select([watch.fd], [], [], 0)[0]:
        watch.consume(os.read(watch.fd, 65536), ring)


@pytest.fixture
def inputs(tmp_path):
    root = tmp_path / "queue"
    for name in queue_bell.REQUIRED_STATE_DIRS:
        (root / name).mkdir(parents=True)
    (root / "workers").mkdir()
    (root / "workers" / "worker.lab.json").write_text("existing offer")
    for name in ("free", "held", "minted"):
        (root / "reservations" / "worker.lab" / name).mkdir(parents=True)
    holder = root / "reservations" / "worker.lab" / "held" / ".legacy"
    holder.mkdir()
    (holder / ".adaptive.json").write_text("held metadata")
    watch = queue_bell_events.InotifyQueue(root)
    ring = queue_bell_events.EventRing()
    ring.set_coverage(watch.covered_topics)
    try:
        yield root, watch, ring
    finally:
        (root / "workers").chmod(0o700)
        (root / "reservations").chmod(0o700)
        watch.close()


@pytest.mark.parametrize("topic", ["workers", "reservations"])
def test_root_permission_recovery_invalidates_covered_topic(inputs, topic):
    root, watch, ring = inputs
    namespace = root / topic
    namespace.chmod(0)
    assert select.select([watch.fd], [], [], 1)[0]
    drain(watch, ring)
    before = ring.now_v2()
    assert topic in before["coverage"]["topics"]
    namespace.chmod(0o700)
    assert select.select([watch.fd], [], [], 1)[0]
    drain(watch, ring)
    after = ring.now_v2()
    reply = ring.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
                         {topic}, {"worker.lab"}, 0, before["coverage"]["version"])
    assert reply.get("reset") or reply.get("events"), (
        "namespace permissions recovered with complete coverage but no invalidation",
        topic, before["seq"], after["seq"], before["coverage"], after["coverage"])


def test_existing_hidden_holder_metadata_cannot_be_silently_covered(inputs):
    root, watch, ring = inputs
    before = ring.now_v2()
    assert "reservations" in before["coverage"]["topics"]
    holder = root / "reservations" / "worker.lab" / "held" / ".legacy"
    (holder / ".adaptive.json").write_text("changed held metadata")
    if select.select([watch.fd], [], [], 0.1)[0]:
        drain(watch, ring)
    reply = ring.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
                         {"reservations"}, {"worker.lab"}, 0, before["coverage"]["version"])
    assert reply.get("reset") or reply.get("events"), (
        "existing holder accepted by HolderCensus is covered but never watched")


def test_a_confirmation_cannot_consume_an_invalidation_during_its_read(monkeypatch):
    monkeypatch.setattr(queue_bell_input_reads.time, "monotonic", lambda: 0.0)
    pending = queue_bell_input_reads.DirtyInputs()
    pending.invalidate("reservations", "worker.lab", deadline=4.0)
    observed = []
    deadlines = []

    def read(topic, host, deadline):
        deadlines.append(deadline)
        observed.append("A" if not observed else "B")
        if len(observed) == 1:
            # Read A, then B becomes visible and its sole hint is delivered.
            pending.invalidate(topic, host, deadline=100.0)
        return True

    pending.confirm(read, stop=threading.Event())
    assert observed == ["A", "B"], "confirmation of A consumed B's sole invalidation"
    assert deadlines == [4.0, 4.0]


def test_holder_watches_cover_the_authoritative_census(inputs):
    from prismabuild.holder_census import HolderCensus

    root, watch, ring = inputs
    held = root / "reservations" / "worker.lab" / "held"
    (held / "ordinary").mkdir()
    (held / ".another-hidden").mkdir()
    (held / "not-a-holder").touch()
    drain(watch, ring)
    census = HolderCensus(held, lambda path: None)
    watched = {path.name for path in watch.directories.values()
               if path is not None and path.parent == held.relative_to(root)}
    assert watched == {holder.name for holder in census.holders}
    assert watched == {".legacy", "ordinary", ".another-hidden"}


def test_confirmation_keeps_concurrent_invalidation_dirty(monkeypatch):
    monkeypatch.setattr(queue_bell_input_reads.time, "monotonic", lambda: 0.0)
    pending = queue_bell_input_reads.DirtyInputs()
    pending.invalidate("workers", "worker.lab", deadline=4.0)
    read_started = threading.Event()
    invalidated = threading.Event()
    observed = []

    def invalidate_during_read():
        assert read_started.wait(2)
        pending.invalidate("workers", "worker.lab", deadline=100.0)
        invalidated.set()

    def read(topic, host, deadline):
        observed.append(deadline)
        if len(observed) == 1:
            read_started.set()
            assert invalidated.wait(2)
        return True

    writer = threading.Thread(target=invalidate_during_read)
    writer.start()
    try:
        pending.confirm(read, stop=threading.Event())
        assert observed == [4.0, 4.0]
        assert not pending.dirty
    finally:
        read_started.set()
        writer.join(2)
        assert not writer.is_alive()


def test_holder_rename_during_scan_cannot_discard_rebound_watch(tmp_path, monkeypatch):
    root = tmp_path / "queue"
    for name in queue_bell.REQUIRED_STATE_DIRS:
        (root / name).mkdir(parents=True)
    held = root / "reservations" / "worker.lab" / "held"
    old = held / "old"
    new = held / "new"
    old.mkdir(parents=True)
    (old / "cpu-0").touch()
    old_relative = old.relative_to(root)
    new_relative = new.relative_to(root)
    original_add = queue_bell_events.InotifyQueue._add
    original_scandir = os.scandir
    moved = []

    def add_then_rename(self, path, relative):
        original_add(self, path, relative)
        if relative == old_relative and not moved:
            old.rename(new)
            moved.append(True)

    class Entries:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def __iter__(self):
            # A legal concurrent listing: old was seen, then new is seen.
            with original_scandir(held) as entries:
                first = next(entries)
            yield first
            assert moved
            with original_scandir(held) as entries:
                yield next(entries)

    def racing_scandir(descriptor):
        if Path(f"/proc/self/fd/{descriptor}").resolve() == held and not moved:
            return Entries()
        return original_scandir(descriptor)

    monkeypatch.setattr(queue_bell_events.InotifyQueue, "_add", add_then_rename)
    monkeypatch.setattr(os, "scandir", racing_scandir)
    watch = queue_bell_events.InotifyQueue(root)
    try:
        assert new_relative in watch.directories.values()
        ring = queue_bell_events.EventRing()
        ring.set_coverage(watch.covered_topics)
        while select.select([watch.fd], [], [], 0)[0]:
            watch.consume(os.read(watch.fd, 65536), ring)
        assert "reservations" in watch.covered_topics, "ordinary replay rename lost topic coverage"
        assert new_relative in watch.directories.values(), "MOVE_SELF removed the new holder watch"
    finally:
        watch.close()

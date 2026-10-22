"""One acceptance category per authoritative host-input read-set row (#1940)."""
import errno
import os
from pathlib import Path
import select

import pytest

from prismabuild import adaptive_cpu, holder_census, pool, queue_bell
from prismabuild import queue_bell_events as events


def queue_tree(tmp_path):
    root = tmp_path / "parent" / "queue"
    for name in queue_bell.REQUIRED_STATE_DIRS:
        (root / name).mkdir(parents=True)
    (root / "workers").mkdir()
    for name in ("free", "held", "minted"):
        (root / "reservations" / "worker.lab" / name).mkdir(parents=True)
    return root


def drain(watch, ring):
    while select.select([watch.fd], [], [], 0)[0]:
        watch.consume(os.read(watch.fd, 65536), ring)


def changed(ring, before, topic):
    reply = ring.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
                        {topic}, {"worker.lab"}, 0, before["coverage"]["version"])
    assert reply.get("reset") or reply.get("events"), reply


@pytest.mark.parametrize("level", ["ancestor", "queue", "workers", "reservations",
                                    "host", "free", "held", "minted", "holder"])
def test_search_and_listing_permission_recovery_invalidates(tmp_path, level):
    root = queue_tree(tmp_path)
    host = root / "reservations" / "worker.lab"
    holder = host / "held" / "legacy"
    holder.mkdir()
    path = {"ancestor": root.parent, "queue": root, "workers": root / "workers",
            "reservations": root / "reservations", "host": host, "holder": holder,
            **{name: host / name for name in ("free", "held", "minted")}}[level]
    watch, ring = events.InotifyQueue(root), events.EventRing()
    ring.set_coverage(watch.covered_topics)
    try:
        path.chmod(0)
        drain(watch, ring)
        before = ring.now_v2()
        path.chmod(0o700)
        assert select.select([watch.fd], [], [], 1)[0]
        drain(watch, ring)
        changed(ring, before, "workers" if level == "workers" else "reservations")
    finally:
        path.chmod(0o700)
        watch.close()


def test_ancestor_replacement_withdraws_both_host_topics(tmp_path):
    root = queue_tree(tmp_path)
    watch, ring = events.InotifyQueue(root), events.EventRing()
    ring.set_coverage(watch.covered_topics)
    try:
        before = ring.now_v2()
        root.parent.rename(tmp_path / "moved")
        queue_tree(tmp_path)
        drain(watch, ring)
        assert not {"workers", "reservations"} & set(watch.covered_topics)
        changed(ring, before, "workers")
    finally:
        watch.close()


def test_symlinked_ancestor_cannot_claim_host_coverage(tmp_path):
    root = queue_tree(tmp_path / "real")
    alias = tmp_path / "alias"
    alias.symlink_to(root.parent, target_is_directory=True)
    watch = events.InotifyQueue(alias / "queue")
    try:
        assert not {"workers", "reservations"} & set(watch.covered_topics)
    finally:
        watch.close()


@pytest.mark.parametrize("relative,topic", [
    ("workers/worker.lab.json", "workers"),
    ("reservations/worker.lab/cpu-map.json", "reservations"),
    ("reservations/worker.lab/held/legacy/.adaptive.json", "reservations"),
    ("reservations/worker.lab/held/legacy/.gpu.json", "reservations"),
])
@pytest.mark.parametrize("shape", ["symlink", "dangling", "hardlink", "fifo", "directory", "unreadable"])
@pytest.mark.parametrize("install", [False, True])
def test_byte_read_inputs_withdraw_for_unwatchable_shapes(tmp_path, relative, topic, shape, install):
    root = queue_tree(tmp_path)
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    outside = tmp_path / "outside"
    outside.write_text("{}")
    watch = events.InotifyQueue(root) if install else None
    ring = events.EventRing()
    if watch:
        ring.set_coverage(watch.covered_topics)
    try:
        if shape in ("symlink", "dangling"):
            path.symlink_to(outside if shape == "symlink" else tmp_path / "missing")
        elif shape == "hardlink":
            os.link(outside, path)
        elif shape == "fifo":
            os.mkfifo(path)
        elif shape == "directory":
            path.mkdir()
        else:
            path.write_text("{}")
            path.chmod(0)
        if watch is None:
            watch = events.InotifyQueue(root)
        else:
            assert select.select([watch.fd], [], [], 1)[0]
            drain(watch, ring)
        assert topic not in watch.covered_topics
        if install:
            assert topic not in ring.now_v2()["coverage"]["topics"]
    finally:
        if shape == "unreadable":
            path.chmod(0o600)
        if watch:
            watch.close()


@pytest.mark.parametrize("relative,topic", [
    ("workers/.hidden.json", "workers"),
    ("workers/unknown name.json", "workers"),
    ("reservations/worker.lab/minted/dead", "reservations"),
    ("reservations/worker.lab/free/cpu-0", "reservations"),
])
@pytest.mark.parametrize("install", [False, True])
def test_unknown_names_or_nested_token_shapes_withdraw(tmp_path, relative, topic, install):
    root = queue_tree(tmp_path)
    path = root / relative
    watch = events.InotifyQueue(root) if install else None
    ring = events.EventRing()
    try:
        if watch:
            ring.set_coverage(watch.covered_topics)
        if relative.startswith("workers"):
            path.write_text("{}")
        else:
            path.mkdir()
        if watch is None:
            watch = events.InotifyQueue(root)
        else:
            drain(watch, ring)
        assert topic not in watch.covered_topics
    finally:
        if watch:
            watch.close()


@pytest.mark.parametrize("relative", ["free/.cpu-0", "minted/.cpu-0",
                                      "held/legacy/.cpu-0", "held/legacy/.unfinished-metadata"])
def test_all_immediate_names_in_authoritative_listings_invalidate(tmp_path, relative):
    root = queue_tree(tmp_path)
    path = root / "reservations" / "worker.lab" / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    ledger = pool.ResourceLedger(root / "reservations", "worker.lab")
    census = holder_census.HolderCensus(ledger.held_dir, adaptive_cpu.read_json)
    if path.name == ".unfinished-metadata":
        assert census.cpu("legacy").has_contents
    elif relative.startswith("free"):
        assert ledger.available() == {".cpu": 1}
    elif relative.startswith("held"):
        assert census.token_totals() == {".cpu": 1}
    watch, ring = events.InotifyQueue(root), events.EventRing()
    ring.set_coverage(watch.covered_topics)
    try:
        before = ring.now_v2()
        path.unlink()
        assert select.select([watch.fd], [], [], 1)[0]
        drain(watch, ring)
        changed(ring, before, "reservations")
    finally:
        watch.close()


@pytest.mark.parametrize("relative,topic", [
    ("workers/worker.lab.json", "workers"),
    ("reservations/worker.lab/cpu-map.json", "reservations"),
    ("reservations/worker.lab/held/legacy/.adaptive.json", "reservations"),
    ("reservations/worker.lab/held/legacy/.gpu.json", "reservations"),
])
def test_visible_bytes_invalidate_before_writer_closes(tmp_path, relative, topic):
    root = queue_tree(tmp_path)
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}")
    watch, ring = events.InotifyQueue(root), events.EventRing()
    ring.set_coverage(watch.covered_topics)
    descriptor = os.open(path, os.O_WRONLY)
    try:
        before = ring.now_v2()
        os.write(descriptor, b'{"updated":true}')
        assert select.select([watch.fd], [], [], 1)[0], "no pre-close invalidation"
        drain(watch, ring)
        changed(ring, before, topic)
    finally:
        os.close(descriptor)
        watch.close()


@pytest.mark.parametrize("relative,topic", [
    ("workers/worker.lab.json", "workers"),
    ("reservations/worker.lab/cpu-map.json", "reservations"),
    ("reservations/worker.lab/held/legacy/.adaptive.json", "reservations"),
    ("reservations/worker.lab/held/legacy/.gpu.json", "reservations"),
])
def test_external_hardlink_added_after_reconstruction_is_not_silent(tmp_path, relative, topic):
    root = queue_tree(tmp_path)
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}")
    watch, ring = events.InotifyQueue(root), events.EventRing()
    ring.set_coverage(watch.covered_topics)
    try:
        before = ring.now_v2()
        outside = tmp_path / "external-link"
        os.link(path, outside)
        outside.write_text('{"changed":true}')
        drain(watch, ring)
        changed(ring, before, topic)
        assert topic not in watch.covered_topics
    finally:
        watch.close()


@pytest.mark.parametrize("directory", ["free", "held", "minted"])
@pytest.mark.parametrize("target", ["missing", "file", "directory"])
def test_linked_ledger_namespaces_are_incomplete(tmp_path, directory, target):
    root = queue_tree(tmp_path)
    path = root / "reservations" / "worker.lab" / directory
    path.rmdir()
    outside = tmp_path / "external"
    if target == "file":
        outside.touch()
    elif target == "directory":
        outside.mkdir()
    path.symlink_to(outside, target_is_directory=True)
    watch = events.InotifyQueue(root)
    try:
        assert "reservations" not in watch.covered_topics
    finally:
        watch.close()


@pytest.mark.parametrize("relative", ["free/cpu-0", "minted/cpu-0", "held/legacy/cpu-0"])
def test_name_read_token_links_are_incomplete_without_following_targets(tmp_path, relative):
    root = queue_tree(tmp_path)
    path = root / "reservations" / "worker.lab" / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.symlink_to(tmp_path / "missing")
    watch = events.InotifyQueue(root)
    try:
        assert "reservations" not in watch.covered_topics
    finally:
        watch.close()


def test_ancestor_and_file_watches_survive_overflow_reconstruction(tmp_path):
    root = queue_tree(tmp_path)
    offer = root / "workers" / "worker.lab.json"
    offer.write_text("{}")
    watch, ring = events.InotifyQueue(root), events.EventRing()
    ring.set_coverage(watch.covered_topics)
    try:
        watch.consume(events.EVENT_HEADER.pack(-1, events.IN_Q_OVERFLOW, 0, 0), ring)
        drain(watch, ring)
        assert {"workers", "reservations"} <= set(watch.covered_topics)
        before = ring.now_v2()
        os.link(offer, tmp_path / "outside")
        drain(watch, ring)
        changed(ring, before, "workers")
        assert "workers" not in watch.covered_topics
    finally:
        watch.close()


@pytest.mark.parametrize("relative,topic", [
    ("workers/worker.lab.json", "workers"),
    ("reservations/worker.lab/held/legacy/.adaptive.json", "reservations"),
])
def test_failed_byte_input_watch_withdraws_coverage(tmp_path, monkeypatch, relative, topic):
    root = queue_tree(tmp_path)
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}")

    def exhausted(*args):
        raise OSError(errno.ENOSPC, "file watch limit")

    monkeypatch.setattr(events.InotifyQueue, "_watch_host_file", exhausted)
    watch = events.InotifyQueue(root)
    try:
        assert topic not in watch.covered_topics
    finally:
        watch.close()


def test_failed_ancestor_watch_withdraws_both_host_topics(tmp_path, monkeypatch):
    root = queue_tree(tmp_path)
    original = events.InotifyQueue._add

    def exhausted(self, path, relative):
        if relative.is_absolute():
            raise OSError(errno.ENOSPC, "ancestor watch limit")
        return original(self, path, relative)

    monkeypatch.setattr(events.InotifyQueue, "_add", exhausted)
    watch = events.InotifyQueue(root)
    try:
        assert not {"workers", "reservations"} & set(watch.covered_topics)
    finally:
        watch.close()


@pytest.mark.parametrize("metadata", [".adaptive.json", ".gpu.json"])
def test_rebound_holder_keeps_metadata_inode_coverage(tmp_path, metadata):
    root = queue_tree(tmp_path)
    held = root / "reservations" / "worker.lab" / "held"
    old, new = held / "old", held / ".new"
    old.mkdir()
    (old / metadata).write_text("{}")
    watch, ring = events.InotifyQueue(root), events.EventRing()
    ring.set_coverage(watch.covered_topics)
    try:
        old.rename(new)
        watch._tree(new.relative_to(root), ring)
        drain(watch, ring)
        assert "reservations" in watch.covered_topics
        before = ring.now_v2()
        os.link(new / metadata, tmp_path / "external-link")
        drain(watch, ring)
        changed(ring, before, "reservations")
        assert "reservations" not in watch.covered_topics
    finally:
        watch.close()

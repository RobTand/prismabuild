"""Host invalidations use private scratch watches, without outcome activity."""
import errno
import os
from pathlib import Path
import select
from types import SimpleNamespace

import pytest

from prismabuild import queue_bell, queue_bell_events, queue_bell_topics


@pytest.fixture
def host_feed(tmp_path):
    root = tmp_path / "queue"
    for directory in queue_bell.REQUIRED_STATE_DIRS:
        (root / directory).mkdir(parents=True)
    host = "worker-1.lab.example"
    base = root / "reservations" / host
    for directory in ("free", "held", "minted"):
        (base / directory).mkdir(parents=True)
    workers = root / "workers"
    workers.mkdir()
    offer = workers / f"{host}.json"
    offer.write_text("old writer, unparsed")
    watch = queue_bell_events.InotifyQueue(root)
    ring = queue_bell_events.EventRing()
    ring.set_coverage(watch.covered_topics)
    try:
        yield SimpleNamespace(root=root, host=host, base=base, offer=offer,
                              watch=watch, ring=ring)
    finally:
        watch.close()


def host_input_events(feed, topic, mutate):
    before = feed.ring.now_v2()
    assert topic in before["coverage"]["topics"]
    mutate()
    assert select.select([feed.watch.fd], [], [], 2)[0], "missing kernel callback"
    feed.watch.consume(os.read(feed.watch.fd, 65536), feed.ring)
    after = feed.ring.now_v2()
    reply = feed.ring.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
        {topic}, {feed.host}, 0, after["coverage"]["version"])
    assert reply.get("events"), reply
    assert all(event["topic"] == topic and event["id"] == feed.host for event in reply["events"])
    assert all(set(event) == {"seq", "topic", "id"} for event in reply["events"])
    return reply


@pytest.mark.parametrize("mutation", ["replace", "in-place", "chmod", "delete", "rename"])
def test_offer_old_writer_changes_invalidate_exact_dotted_host(host_feed, mutation):
    feed = host_feed

    def change_offer():
        if mutation == "replace":
            temporary = feed.offer.parent / ".replacement"
            temporary.write_text("not JSON")
            temporary.replace(feed.offer)
        elif mutation == "in-place":
            feed.offer.write_text("still not JSON")
        elif mutation == "chmod":
            feed.offer.chmod(0o600)
        elif mutation == "delete":
            feed.offer.unlink()
        else:
            feed.offer.rename(feed.offer.with_name("other.lab.json"))

    host_input_events(feed, "workers", change_offer)


@pytest.mark.parametrize("relative", [
    "free/cpu-0", "minted/cpu-0", "capacity.json", "cpu-map.json",
    "held/legacy-holder/cpu-0", "held/legacy-holder/cpu.json",
    "held/claiming.123.holder.worker.lab.99.abcdef12/mem_gb-0",
])
@pytest.mark.parametrize("mutation", ["write", "replace", "chmod", "delete"])
def test_host_token_and_holder_mutations_are_names_only(host_feed, relative, mutation):
    feed = host_feed
    assert "reservations" in feed.watch.covered_topics
    path = feed.base / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("unknown evidence")
    # Install a newly created holder before capturing the observation cursor.
    feed.watch.consume(os.read(feed.watch.fd, 65536), feed.ring)

    def mutate_host_input():
        if mutation == "write":
            path.write_text("malformed evidence remains unknown")
        elif mutation == "replace":
            temporary = path.with_name(".replacement")
            temporary.write_text("unparsed")
            temporary.replace(path)
        elif mutation == "chmod":
            path.chmod(0o600)
        else:
            path.unlink()

    host_input_events(feed, "reservations", mutate_host_input)
    assert all(not list((feed.root / state).iterdir()) for state in ("ready", "done", "failed"))


def test_partial_acquisition_rollback_and_release_need_no_terminal(host_feed):
    feed = host_feed
    assert "reservations" in feed.watch.covered_topics
    free = feed.base / "free" / "cpu-0"
    free.touch()
    feed.watch.consume(os.read(feed.watch.fd, 65536), feed.ring)
    holder = feed.base / "held" / "claiming.123.job.worker.lab.99.abcdef12"
    host_input_events(feed, "reservations", holder.mkdir)
    host_input_events(feed, "reservations", lambda: free.rename(holder / free.name))
    host_input_events(feed, "reservations", lambda: (holder / free.name).rename(free))
    host_input_events(feed, "reservations", holder.rmdir)
    committed = feed.base / "held" / ("a" * 64)
    host_input_events(feed, "reservations", committed.mkdir)
    host_input_events(feed, "reservations", lambda: free.rename(committed / free.name))
    host_input_events(feed, "reservations", lambda: (committed / free.name).rename(free))
    assert all(not list((feed.root / state).iterdir()) for state in ("ready", "done", "failed"))


@pytest.mark.parametrize("holder_name", ["legacy-holder", ".legacy"])
def test_populated_moved_in_holder_gets_watched_before_reconstruction(host_feed, tmp_path, holder_name):
    feed = host_feed
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    (incoming / "cpu-0").touch()
    holder = feed.base / "held" / holder_name
    host_input_events(feed, "reservations", lambda: incoming.rename(holder))
    assert holder.relative_to(feed.root) in feed.watch.directories.values()
    host_input_events(feed, "reservations", lambda: (holder / "cpu-0").unlink())


def test_changes_during_reconstruction_remain_buffered(tmp_path, monkeypatch):
    root = tmp_path / "queue"
    for directory in queue_bell.REQUIRED_STATE_DIRS:
        (root / directory).mkdir(parents=True)
    held = root / "reservations" / "worker.lab" / "held" / "legacy"
    held.mkdir(parents=True)
    token = held / "cpu-0"
    token.touch()
    original = os.scandir
    changed = []

    def change_after_watch(descriptor):
        if Path(f"/proc/self/fd/{descriptor}").resolve() == held:
            token.unlink()
            changed.append(True)
        return original(descriptor)

    monkeypatch.setattr(os, "scandir", change_after_watch)
    watch = queue_bell_events.InotifyQueue(root)
    try:
        assert changed, "holder never reconstructed"
        ring = queue_bell_events.EventRing()
        ring.set_coverage(watch.covered_topics)
        assert select.select([watch.fd], [], [], 2)[0]
        watch.consume(os.read(watch.fd, 65536), ring)
        assert any(event["topic"] == "reservations" and event["id"] == "worker.lab"
                   for event in ring.events)
    finally:
        watch.close()


def test_nested_watch_loss_withdraws_host_topic(host_feed):
    feed = host_feed
    relative = Path("reservations") / feed.host / "free"
    wd = next(wd for wd, path in feed.watch.directories.items() if path == relative)
    feed.watch.consume(queue_bell_events.EVENT_HEADER.pack(
        wd, queue_bell_events.IN_IGNORED, 0, 0), feed.ring)
    assert "reservations" not in feed.watch.covered_topics
    assert "reservations" not in feed.ring.now_v2()["coverage"]["topics"]
    assert "workers" in feed.ring.now_v2()["coverage"]["topics"]


def test_watch_exhaustion_during_holder_installation_cannot_claim_coverage(host_feed, monkeypatch):
    feed = host_feed
    holder = feed.base / "held" / "legacy"
    holder.mkdir()
    original = feed.watch._add

    def refuse_holder_watch(path, relative):
        if relative == holder.relative_to(feed.root):
            raise OSError(errno.ENOSPC, "watch limit")
        return original(path, relative)

    monkeypatch.setattr(feed.watch, "_add", refuse_holder_watch)
    with pytest.raises(OSError, match="watch limit"):
        feed.watch.consume(os.read(feed.watch.fd, 65536), feed.ring)
    assert "reservations" not in feed.ring.now_v2()["coverage"]["topics"]


def test_existing_symlinked_holder_is_unknown_not_covered(tmp_path):
    root = tmp_path / "queue"
    for directory in queue_bell.REQUIRED_STATE_DIRS:
        (root / directory).mkdir(parents=True)
    held = root / "reservations" / "worker.lab" / "held"
    held.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (held / "legacy").symlink_to(outside, target_is_directory=True)
    watch = queue_bell_events.InotifyQueue(root)
    try:
        assert "reservations" not in watch.covered_topics
        assert "workers" in watch.covered_topics
        assert not any(path is not None and path.parts and path.parts[-1] == "legacy"
                       for path in watch.directories.values())
    finally:
        watch.close()


def test_host_adapter_is_bounded_and_keeps_adaptive_out_of_this_slice():
    adapter = queue_bell_topics.HostReservations()
    assert adapter.hint(Path("reservations/worker.lab/held/legacy/cpu-0")) == (
        "reservations", "worker.lab")
    assert adapter.allows(Path("reservations/worker.lab/held/legacy"))
    assert not adapter.allows(Path("reservations/worker.lab/held/legacy/nested"))
    assert not adapter.allows(Path("reservations/worker.lab/adaptive"))
    assert adapter.hint(Path("reservations/worker.lab/adaptive/claim-denials.json")) is None
    assert queue_bell_topics.TopicFilters.parse("workers,reservations", "worker.lab") == (
        {"workers", "reservations"}, {"worker.lab"})
    with pytest.raises(ValueError):
        queue_bell_topics.TopicFilters.parse("workers", "../worker")


@pytest.mark.parametrize("metadata", [".adaptive.json", ".gpu.json"])
@pytest.mark.parametrize("holder_name", ["legacy", ".legacy"])
def test_hidden_holder_metadata_changes_invalidate_capacity(host_feed, metadata, holder_name):
    feed = host_feed
    holder = feed.base / "held" / holder_name
    host_input_events(feed, "reservations", holder.mkdir)
    host_input_events(feed, "reservations", lambda: (holder / metadata).write_text("unknown"))


def test_holder_commit_rename_and_release_keep_the_service_covered(host_feed):
    feed = host_feed
    private = feed.base / "held" / "claiming.123.job.worker.lab.99.abcdef12"
    host_input_events(feed, "reservations", private.mkdir)
    host_input_events(feed, "reservations", lambda: (private / "cpu-0").touch())
    committed = private.with_name("a" * 64)
    host_input_events(feed, "reservations", lambda: private.rename(committed))
    if select.select([feed.watch.fd], [], [], 0)[0]:
        feed.watch.consume(os.read(feed.watch.fd, 65536), feed.ring)
    assert "reservations" in feed.watch.covered_topics
    host_input_events(feed, "reservations", lambda: (committed / "cpu-0").unlink())


@pytest.mark.parametrize("confirm_after", [3, None])
def test_nfs_visibility_keeps_dirty_host_and_original_budget(monkeypatch, confirm_after):
    from prismabuild import queue_bell_host_reads
    now = [0.0]
    monkeypatch.setattr(queue_bell_host_reads.time, "monotonic", lambda: now[0])
    attempts = []
    waits = []
    pending = queue_bell_host_reads.DirtyHostInputs()
    pending.invalidate("reservations", "worker.lab", deadline=4.0)

    def confirm_host(topic, host, deadline):
        attempts.append((topic, host, deadline))
        assert pending.dirty == {("reservations", "worker.lab")}
        # Another hint must not restart the original read/discovery budget.
        if confirm_after is None or len(attempts) < confirm_after:
            pending.invalidate(topic, host, deadline=100.0)
        return confirm_after is not None and len(attempts) >= confirm_after

    class VisibilityStop:
        def is_set(self):
            return False

        def wait(self, delay):
            waits.append(delay)
            now[0] += delay
            return False

    if confirm_after is None:
        with pytest.raises(queue_bell.Unavailable, match="visibility"):
            pending.confirm(confirm_host, stop=VisibilityStop())
        assert pending.dirty == {("reservations", "worker.lab")}
        assert now[0] == 4.0
        assert 1.0 in waits
    else:
        pending.confirm(confirm_host, stop=VisibilityStop())
        assert not pending.dirty
        assert waits == pytest.approx([0.05, 0.1])
    assert all(attempt[2] == 4.0 for attempt in attempts)
    assert all(0 < delay <= 1 for delay in waits)


def test_nfs_visibility_stop_preserves_unknown_host():
    import threading
    import time
    from prismabuild.queue_bell_host_reads import DirtyHostInputs
    pending = DirtyHostInputs()
    pending.invalidate("workers", "worker.lab", deadline=time.monotonic() + 5)
    stop = threading.Event()
    stop.set()
    with pytest.raises(queue_bell.Unavailable, match="stopped"):
        pending.confirm(lambda *args: pytest.fail("read after stop"), stop=stop)
    assert pending.dirty == {("workers", "worker.lab")}

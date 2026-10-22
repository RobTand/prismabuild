"""Installed-watch proofs, reconstruction refusal and lazy coverage boundaries."""
from pathlib import Path
import shutil

import pytest

from prismabuild import queue_bell, queue_bell_events, queue_notifier
from test_queue_bell import bell, bell_startup


@pytest.mark.parametrize("invalid_kind", ["missing", "symlink"])
@pytest.mark.parametrize("namespace", [".", *queue_bell.REQUIRED_STATE_DIRS])
@pytest.mark.parametrize("reconstructing", [False, True])
def test_invalid_required_root_is_refused_with_exit_three(
        bell_startup, tmp_path, monkeypatch, capsys, namespace, reconstructing, invalid_kind):
    path = bell_startup.root / namespace
    target = tmp_path / "target"
    target.mkdir()

    def replace_required_root():
        shutil.rmtree(path)
        if invalid_kind == "symlink":
            path.symlink_to(target, target_is_directory=True)

    if reconstructing:
        def refuse_during_reconstruction(self):
            replace_required_root()
            try:
                self.watch.consume(queue_notifier.EVENT_HEADER.pack(
                    -1, queue_notifier.IN_Q_OVERFLOW, 0, 0), self.ring)
            except queue_notifier.MissingQueueDirectory as exc:
                self.error = exc
            finally:
                self.ring.close()
                self.on_exit()

        monkeypatch.setattr(queue_notifier.BellService, "_listen", refuse_during_reconstruction)
    else:
        replace_required_root()
    assert bell_startup.run() == 3
    assert "startup refused" in capsys.readouterr().err


def test_coverage_is_derived_from_installed_root_watches(bell):
    watch = bell.service.watch
    ready_wd = next(wd for wd, path in watch.directories.items() if path == Path("ready"))
    # Removing the bookkeeping witness must not leave a declaration-only proof.
    relative = watch.directories.pop(ready_wd)
    try:
        assert "ready" not in watch.covered_topics
    finally:
        watch.directories[ready_wd] = relative


def test_existing_optional_symlink_is_not_advertised(tmp_path):
    root = tmp_path / "queue"
    for directory in queue_bell.REQUIRED_STATE_DIRS:
        (root / directory).mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "attempts").symlink_to(outside, target_is_directory=True)
    with queue_notifier.BellService(root, [("127.0.0.1", 0)]) as service:
        assert "attempts" not in service.watch.covered_topics
        assert "publication-endings" in service.watch.covered_topics
        assert Path() in service.watch.directories.values()


def test_lazy_installation_withdraws_then_resets_before_restoring(tmp_path, monkeypatch):
    root = tmp_path / "queue"
    for directory in queue_bell.REQUIRED_STATE_DIRS:
        (root / directory).mkdir(parents=True)
    watch = queue_bell_events.InotifyQueue(root)
    ring = queue_bell_events.EventRing()
    ring.set_coverage(watch.covered_topics)
    before = ring.now_v2()
    original = watch._tree
    snapshots = []

    def inspect_installation(relative, *args):
        snapshots.append(ring.now_v2())
        return original(relative, *args)

    monkeypatch.setattr(watch, "_tree", inspect_installation)
    try:
        (root / "attempts").mkdir()
        name = b"attempts\0"
        wd = next(wd for wd, relative in watch.directories.items() if relative == Path())
        watch.consume(queue_bell_events.EVENT_HEADER.pack(wd,
            queue_bell_events.IN_ISDIR | queue_bell_events.IN_CREATE, 0, len(name)) + name, ring)
        after = ring.now_v2()
        assert snapshots and "attempts" not in snapshots[0]["coverage"]["topics"]
        assert after["coverage"]["version"] > before["coverage"]["version"]
        assert after["diagnostics"]["resets"] == before["diagnostics"]["resets"] + 1
        # The v2 coverage version is its reset marker. V1 history stays intact.
        assert after["seq"] == before["seq"]
        assert "attempts" in after["coverage"]["topics"]
    finally:
        watch.close()


def test_failed_lazy_watch_installation_keeps_topic_incomplete(tmp_path, monkeypatch):
    root = tmp_path / "queue"
    for directory in queue_bell.REQUIRED_STATE_DIRS:
        (root / directory).mkdir(parents=True)
    watch = queue_bell_events.InotifyQueue(root)
    ring = queue_bell_events.EventRing()
    ring.set_coverage(watch.covered_topics)

    def fail_lazy_watch(*args):
        assert "attempts" not in ring.now_v2()["coverage"]["topics"]
        raise OSError("watch capacity exhausted")

    monkeypatch.setattr(watch, "_tree", fail_lazy_watch)
    try:
        (root / "attempts").mkdir()
        name = b"attempts\0"
        wd = next(wd for wd, relative in watch.directories.items() if relative == Path())
        packet = queue_bell_events.EVENT_HEADER.pack(wd,
            queue_bell_events.IN_ISDIR | queue_bell_events.IN_CREATE, 0, len(name)) + name
        with pytest.raises(OSError, match="exhausted"):
            watch.consume(packet, ring)
        assert "attempts" not in ring.now_v2()["coverage"]["topics"]
    finally:
        watch.close()


def test_keyless_withdrawal_subtree_invalidates_an_observed_deletion(tmp_path, monkeypatch):
    import threading

    root = tmp_path / "queue"
    for directory in queue_bell.REQUIRED_STATE_DIRS:
        (root / directory).mkdir(parents=True)
    incoming = tmp_path / "incoming"
    key = "a" * 64
    record = incoming / key / "generation.json"
    record.parent.mkdir(parents=True)
    record.write_text("not parsed")
    pending = threading.Event()
    release = threading.Event()
    consumed = threading.Event()
    original = queue_bell_events.InotifyQueue.consume

    def pause_keyless_install(self, data, ring):
        from queue_bell_test_packets import contains_installation
        installing = contains_installation(self, data, Path("withdrawn"), "decisions")
        if installing:
            pending.set()
            assert release.wait(3)
        original(self, data, ring)
        if installing:
            consumed.set()

    monkeypatch.setattr(queue_bell_events.InotifyQueue, "consume", pause_keyless_install)
    with queue_notifier.BellService(root, [("127.0.0.1", 0)]) as service:
        try:
            port = service.servers[0].server_address[1]
            assert port != queue_bell.DEFAULT_PORT
            client = queue_bell.Client(root, host="127.0.0.1", port=port)
            before = client.now_v2(("withdrawn",))
            incoming.rename(root / "withdrawn" / "decisions")
            assert pending.wait(2)
            assert (root / "withdrawn" / "decisions" / key / "generation.json").exists()
            shutil.rmtree(root / "withdrawn" / "decisions" / key)
            release.set()
            assert consumed.wait(2)
            reply = client.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
                before["coverage"]["version"], ("withdrawn",), (key,), 0.2)
            assert reply.get("reset") or reply["events"]
        finally:
            release.set()

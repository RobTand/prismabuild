"""A mutation observed before lazy watches exist still needs invalidation."""
from pathlib import Path
import shutil
import threading

from prismabuild import queue_bell, queue_bell_events, queue_notifier
from queue_bell_test_packets import contains_installation


def test_populated_lazy_root_loses_observed_deletion(tmp_path, monkeypatch):
    root = tmp_path / "queue"
    for directory in queue_bell.REQUIRED_STATE_DIRS:
        (root / directory).mkdir(parents=True)
    incoming = tmp_path / "incoming"
    key = "a" * 64
    record = incoming / key / "generation" / "1.json"
    record.parent.mkdir(parents=True)
    record.write_text("not parsed")
    pending_install = threading.Event()
    release_install = threading.Event()
    consumed = threading.Event()
    original = queue_bell_events.InotifyQueue.consume
    first_packet = [True]

    def pause_before_install(self, data, ring):
        installing = contains_installation(self, data, Path(), "attempts")
        if first_packet[0] and installing:
            first_packet[0] = False
            pending_install.set()
            if not release_install.wait(3):
                raise AssertionError("probe failed to release lazy installation")
        original(self, data, ring)
        if installing:
            consumed.set()

    monkeypatch.setattr(queue_bell_events.InotifyQueue, "consume", pause_before_install)
    with queue_notifier.BellService(root, [("127.0.0.1", 0)]) as service:
        try:
            port = service.servers[0].server_address[1]
            assert port != queue_bell.DEFAULT_PORT
            client = queue_bell.Client(root, host="127.0.0.1", port=port)
            before = client.now_v2(("attempts",))
            incoming.rename(root / "attempts")
            assert pending_install.wait(2)
            assert (root / "attempts" / key / "generation" / "1.json").exists()
            shutil.rmtree(root / "attempts" / key)
            release_install.set()
            assert consumed.wait(2)
            assert Path("attempts") in service.watch.directories.values()
            reply = client.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
                before["coverage"]["version"], ("attempts",), (key,), 0.2)
            assert reply.get("reset") or reply["events"], reply
        finally:
            release_install.set()

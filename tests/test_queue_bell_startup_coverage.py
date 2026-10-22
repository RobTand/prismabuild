"""Review-only proofs for required namespace coverage, private services only."""
from pathlib import Path

from prismabuild import queue_bell, queue_bell_events, queue_notifier


def _queue(tmp_path):
    root = tmp_path / "queue"
    for directory in queue_bell.STATE_DIRS:
        (root / directory).mkdir(parents=True)
    return root


def test_required_symlink_cannot_get_positive_coverage(tmp_path):
    root = _queue(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "ready").rmdir()
    (root / "ready").symlink_to(outside, target_is_directory=True)
    try:
        with queue_notifier.BellService(root, [("127.0.0.1", 0)]) as service:
            port = service.servers[0].server_address[1]
            assert port != queue_bell.DEFAULT_PORT
            client = queue_bell.Client(root, host="127.0.0.1", port=port)
            before = client.now_v2(("ready",))
            assert Path("ready") not in service.watch.directories.values()
            key = "a" * 64
            (outside / f"{key}.json").write_text("not parsed")
            reply = client.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
                before["coverage"]["version"], ("ready",), (key,), 0.2)
            assert reply.get("reset") or "ready" not in reply["coverage"]["topics"], reply
    except (OSError, queue_bell.Unavailable):
        return


def test_required_removed_between_precheck_and_watch_cannot_get_positive_coverage(tmp_path, monkeypatch):
    root = _queue(tmp_path)
    original = queue_bell_events.InotifyQueue._tree

    def remove_before_first_watch(self, relative, *args, **kwargs):
        if relative == Path():
            (root / "ready").rmdir()
        return original(self, relative, *args, **kwargs)

    monkeypatch.setattr(queue_bell_events.InotifyQueue, "_tree", remove_before_first_watch)
    try:
        with queue_notifier.BellService(root, [("127.0.0.1", 0)]) as service:
            port = service.servers[0].server_address[1]
            assert port != queue_bell.DEFAULT_PORT
            client = queue_bell.Client(root, host="127.0.0.1", port=port)
            before = client.now_v2(("ready",))
            assert Path("ready") not in service.watch.directories.values()
            assert "ready" not in before["coverage"]["topics"], before
    except (OSError, queue_bell.Unavailable):
        return

"""Independent file-watch lifecycle and cost proofs, private scratch trees only."""
import os
from pathlib import Path
import select

import pytest

from prismabuild import queue_bell
from prismabuild import queue_bell_events as events


def make_queue(tmp_path):
    root = tmp_path / "queue"
    for name in queue_bell.REQUIRED_STATE_DIRS:
        (root / name).mkdir(parents=True)
    (root / "workers").mkdir()
    (root / "reservations" / "worker.lab" / "held" / "legacy").mkdir(parents=True)
    return root


def drain(watch, ring):
    while select.select([watch.fd], [], [], 0)[0]:
        watch.consume(os.read(watch.fd, 65536), ring)


@pytest.mark.parametrize("relative,topic", [
    ("workers/worker.lab.json", "workers"),
    ("reservations/worker.lab/cpu-map.json", "reservations"),
    ("reservations/worker.lab/held/legacy/.adaptive.json", "reservations"),
    ("reservations/worker.lab/held/legacy/.gpu.json", "reservations"),
])
def test_round_trip_move_cannot_leave_positive_coverage_without_inode_watch(tmp_path, relative, topic):
    root = make_queue(tmp_path)
    leaf = root / relative
    leaf.write_text('{"value":1}')
    watch, ring = events.InotifyQueue(root), events.EventRing()
    ring.set_coverage(watch.covered_topics)
    try:
        assert topic in watch.covered_topics
        away = tmp_path / "away"
        leaf.rename(away)
        away.rename(leaf)
        drain(watch, ring)
        before = ring.now_v2()
        alias = tmp_path / "external-alias"
        os.link(leaf, alias)
        drain(watch, ring)
        alias.write_text('{"value":2}')
        assert leaf.read_text() == '{"value":2}'
        drain(watch, ring)
        reply = ring.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
                            {topic}, {"worker.lab"}, 0, before["coverage"]["version"])
        assert reply.get("reset") or reply.get("events"), (
            "round-trip rename retired the reinstalled inode watch, then external alias bytes "
            f"changed without invalidation; coverage={reply['coverage']}, "
            f"leaf_watched={Path(relative) in watch.file_watches}")
    finally:
        watch.close()


class CountedValues(dict):
    yielded = 0

    def values(self):
        for value in super().values():
            self.yielded += 1
            yield value


def test_one_host_event_must_not_scan_every_unrelated_watch(tmp_path):
    root = make_queue(tmp_path)
    leaf = root / "workers" / "worker.lab.json"
    leaf.write_text("{}")
    for index in range(1000):
        (root / "workers" / f"unrelated-{index}.json").write_text("{}")
    watch, ring = events.InotifyQueue(root), events.EventRing()
    ring.set_coverage(watch.covered_topics)
    descriptor = os.open(leaf, os.O_WRONLY)
    try:
        assert "workers" in watch.covered_topics
        watch.directories = CountedValues(watch.directories)
        os.write(descriptor, b'{"changed":true}')
        assert select.select([watch.fd], [], [], 1)[0]
        packet = os.read(watch.fd, 65536)
        offset = 0
        while offset < len(packet):
            wd, mask, _, length = events.EVENT_HEADER.unpack_from(packet, offset)
            end = offset + events.EVENT_HEADER.size + length
            if wd == watch.file_watches[leaf.relative_to(root)] and mask & events.IN_MODIFY:
                break
            offset = end
        else:
            pytest.fail("missing target inode modify event")
        # Consume exactly one real kernel event, with all bindings installed
        # through normal reconstruction. Ancestors may also report unrelated I/O.
        watch.consume(packet[offset:end], ring)
        assert watch.directories.yielded < 1000, (
            f"one file event traversed {watch.directories.yielded} watch bindings")
    finally:
        os.close(descriptor)
        watch.close()


def test_dropping_a_host_tree_uses_no_per_watch_path_arithmetic(tmp_path, monkeypatch):
    root = make_queue(tmp_path)
    watch = events.InotifyQueue(root)
    try:
        def path_arithmetic(*_args):
            raise AssertionError('subtree removal repeated pathlib arithmetic')

        monkeypatch.setattr(Path, 'is_relative_to', path_arithmetic)
        watch._drop_host_tree(Path('reservations/absent'))
    finally:
        watch.close()


@pytest.mark.parametrize('subtree', [
    'reservations/worker.lab', 'reservations/worker', 'reservations/./worker.lab/',
    'workers', 'workers/absent', '.', '/', '//', '../outside',
])
def test_string_subtrees_remove_exactly_the_pathlib_watch_set(tmp_path, subtree):
    root = make_queue(tmp_path)
    (root / 'workers' / 'worker.lab.json').write_text('{}')
    (root / 'reservations' / 'worker.lab-other' / 'held').mkdir(parents=True)
    watch = events.InotifyQueue(root)
    try:
        relative = Path(subtree)
        expected = {wd: path for wd, path in watch.directories.items()
                    if path is not None and path.is_relative_to(relative)}
        before = dict(watch.directories)
        root_before = dict(watch.root_watches)
        files_before = dict(watch.file_watches)
        watch._drop_host_tree(relative)
        assert watch.directories == {wd: path for wd, path in before.items()
                                     if wd not in expected}
        removed = set(expected.values())
        assert watch.root_watches == {path: wd for path, wd in root_before.items()
                                      if path not in removed}
        assert watch.file_watches == {path: wd for path, wd in files_before.items()
                                      if path not in removed}
    finally:
        watch.close()

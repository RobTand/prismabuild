"""Independent round-2 proofs. Scratch paths only; no network endpoints."""
import json
import os
from pathlib import Path
import select
import threading
import time

import pytest

from prismabuild import adaptive_cpu, holder_census, pool, queue_bell
from prismabuild import queue_bell_events as events
from prismabuild.queue_bell_host_reads import DirtyHostInputs


def make_root(tmp_path):
    root = tmp_path / "queue"
    for state in queue_bell.REQUIRED_STATE_DIRS:
        (root / state).mkdir(parents=True)
    (root / "workers").mkdir()
    (root / "workers" / "worker.lab.json").write_text("offer")
    for name in ("free", "held", "minted"):
        (root / "reservations" / "worker.lab" / name).mkdir(parents=True)
    return root


def feed(root):
    watch = events.InotifyQueue(root)
    ring = events.EventRing()
    ring.set_coverage(watch.covered_topics)
    return watch, ring


def drain(watch, ring):
    while select.select([watch.fd], [], [], 0)[0]:
        watch.consume(os.read(watch.fd, 65536), ring)


def reply_since(ring, before, topic="reservations"):
    return ring.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
                        {topic}, {"worker.lab"}, 0, before["coverage"]["version"])


@pytest.mark.parametrize("initial_target", ["missing", "file"])
def test_unwatchable_holder_cannot_gain_census_membership_without_invalidation(tmp_path, initial_target):
    root = make_root(tmp_path)
    outside = tmp_path / "external-target"
    if initial_target == "file":
        outside.touch()
    held = root / "reservations" / "worker.lab" / "held"
    (held / ".legacy").symlink_to(outside, target_is_directory=True)
    census = holder_census.HolderCensus(held, adaptive_cpu.read_json)
    assert list(census.holders) == []
    watch, ring = feed(root)
    try:
        before = ring.now_v2()
        if outside.exists():
            outside.unlink()
        outside.mkdir()
        (outside / "cpu-0").touch()
        assert [holder.name for holder in census.holders] == [".legacy"]
        assert census.token_totals() == {"cpu": 1}
        drain(watch, ring)
        reply = reply_since(ring, before)
        assert reply.get("reset") or reply.get("events"), (
            "external target became authoritative with complete coverage and no hint")
    finally:
        watch.close()


def test_queue_root_permission_recovery_invalidates_host_inputs(tmp_path):
    root = make_root(tmp_path)
    watch, ring = feed(root)
    try:
        root.chmod(0)
        assert select.select([watch.fd], [], [], 1)[0]
        drain(watch, ring)
        with pytest.raises(PermissionError):
            (root / "workers" / "worker.lab.json").read_text()
        before = ring.now_v2()
        root.chmod(0o700)
        assert (root / "workers" / "worker.lab.json").read_text() == "offer"
        assert select.select([watch.fd], [], [], 1)[0]
        drain(watch, ring)
        reply = reply_since(ring, before, "workers")
        assert reply.get("reset") or reply.get("events"), (
            "queue-root recovery made offers readable without a hint or coverage reset")
    finally:
        root.chmod(0o700)
        watch.close()


@pytest.mark.parametrize("metadata", [".adaptive.json", ".gpu.json"])
def test_linked_authoritative_metadata_cannot_change_without_invalidation(tmp_path, metadata):
    root = make_root(tmp_path)
    held = root / "reservations" / "worker.lab" / "held"
    holder = held / "legacy"
    holder.mkdir()
    outside = tmp_path / "external-metadata.json"
    outside.write_text(json.dumps({"declared_cpu": 2, "reserved_mem_gb": 4}))
    (holder / metadata).symlink_to(outside)
    census = holder_census.HolderCensus(held, pool.record_store.read_document)
    record = census.cpu_record if metadata == ".adaptive.json" else census.gpu_record
    assert record("legacy").fields["reserved_mem_gb"] == 4
    watch, ring = feed(root)
    try:
        before = ring.now_v2()
        outside.write_text(json.dumps({"declared_cpu": 1, "reserved_mem_gb": 1}))
        assert record("legacy").fields["reserved_mem_gb"] == 1
        drain(watch, ring)
        reply = reply_since(ring, before)
        assert reply.get("reset") or reply.get("events"), (
            "authoritative linked metadata changed without invalidating positive coverage")
    finally:
        watch.close()


@pytest.mark.parametrize("holder_name", ["legacy", ".legacy"])
def test_an_empty_legacy_holder_refuses_until_cleanup_and_needs_a_hint(tmp_path, holder_name):
    queue = pool.PoolQueue(tmp_path / "queue")
    for state in queue_bell.REQUIRED_STATE_DIRS:
        (queue.root / state).mkdir(parents=True, exist_ok=True)
    ledger = queue.ledger("worker.lab")
    tiers = {"preferred": [8], "fallback": [10]}
    ledger.configure_cpu_tiers(tiers)
    ledger.ensure_capacity({"cpu": 2, "mem_gb": 8, "gpu": 1})
    holder = ledger.held_dir / holder_name
    holder.mkdir()
    marker = holder / ".unfinished-metadata"
    marker.touch()
    controller = adaptive_cpu.Controller(ledger, tiers, sampler=lambda: {
        "sampled_unix": time.time(), "cpu_count": 2, "interval_s": 1.0,
        "busy_cpus": 0.0, "psi_some": 0.0})
    item = {"action_key": "a" * 64}
    with controller.locked():
        assert controller.decision(item, {"cpu": 1}, identity=("shape", False)) is None
        assert controller.last_decision["reason"] == "holder_reservation_unknown"
    watch, ring = feed(queue.root)
    try:
        before = ring.now_v2()
        marker.unlink()
        assert select.select([watch.fd], [], [], 1)[0]
        drain(watch, ring)
        with controller.locked():
            assert controller.decision(item, {"cpu": 1}, identity=("shape", False)) is None
        before = ring.now_v2()
        holder.rmdir()
        assert select.select([watch.fd], [], [], 1)[0]
        drain(watch, ring)
        with controller.locked():
            assert controller.decision(item, {"cpu": 1}, identity=("shape", False)) is not None
        drain(watch, ring)
        reply = reply_since(ring, before)
        assert reply.get("reset") or reply.get("events"), (
            "holder cleanup changed refusal to admission with no host hint")
    finally:
        watch.close()


@pytest.mark.parametrize("name", ["legacy", ".legacy", "claiming.123.worker", "ram-host:fill", "a" * 64, ".adaptive.json", "unknown name"])
def test_all_real_census_holder_shapes_have_watches_and_metadata_hints(tmp_path, name):
    root = make_root(tmp_path)
    held = root / "reservations" / "worker.lab" / "held"
    holder = held / name
    holder.mkdir()
    census = holder_census.HolderCensus(held, adaptive_cpu.read_json)
    watch, ring = feed(root)
    try:
        assert {h.name for h in census.holders} == {name}
        assert holder.relative_to(root) in watch.directories.values()
        before = ring.now_v2()
        (holder / ".gpu.json").write_text("{}")
        assert select.select([watch.fd], [], [], 1)[0]
        drain(watch, ring)
        assert reply_since(ring, before).get("events")
    finally:
        watch.close()


@pytest.mark.parametrize("stale_mask", [events.IN_MOVE_SELF, events.IN_IGNORED, events.IN_MOVE_SELF | events.IN_IGNORED])
def test_rebound_descriptor_survives_old_self_events(tmp_path, stale_mask):
    root = make_root(tmp_path)
    held = root / "reservations" / "worker.lab" / "held"
    old, new = held / "old", held / ".new"
    old.mkdir()
    watch, ring = feed(root)
    try:
        old_wd = next(wd for wd, path in watch.directories.items() if path == old.relative_to(root))
        old.rename(new)
        watch._tree(new.relative_to(root), ring)
        new_wd = next(wd for wd, path in watch.directories.items() if path == new.relative_to(root))
        assert new_wd != old_wd
        watch.consume(events.EVENT_HEADER.pack(old_wd, stale_mask, 0, 0), ring)
        drain(watch, ring)
        assert "reservations" in watch.covered_topics
        assert new.relative_to(root) in watch.directories.values()
    finally:
        watch.close()


@pytest.mark.parametrize("confirmed_first", [True, False])
def test_generation_keeps_concurrent_hint_and_original_deadline(confirmed_first):
    pending = DirtyHostInputs()
    deadline = time.monotonic() + 4
    pending.invalidate("workers", "worker.lab", deadline=deadline)
    entered, updated = threading.Event(), threading.Event()
    observed = []

    def writer():
        assert entered.wait(2)
        for _ in range(25):
            pending.invalidate("workers", "worker.lab", deadline=deadline + 100)
        updated.set()

    def read(topic, host, bound):
        observed.append(bound)
        if len(observed) == 1:
            entered.set()
            assert updated.wait(2)
            return confirmed_first
        return True

    thread = threading.Thread(target=writer)
    thread.start()
    try:
        pending.confirm(read, stop=threading.Event())
        assert observed == [deadline, deadline]
        assert not pending.dirty
    finally:
        entered.set()
        thread.join(2)
        assert not thread.is_alive()

"""Tier policy invalidations retain the store's non-action identifiers."""
import os
from pathlib import Path
import select
import errno

import pytest

from prismabuild import queue_bell, queue_bell_events, record_store


TIER = "prismabuild-stage:worker-1.lab.example"
KEY = "a" * 64


@pytest.fixture
def tier_feed(tmp_path):
    root = tmp_path / "queue"
    for directory in queue_bell.REQUIRED_STATE_DIRS:
        (root / directory).mkdir(parents=True)
    watch = queue_bell_events.InotifyQueue(root)
    ring = queue_bell_events.EventRing()
    ring.set_coverage(watch.covered_topics)
    try:
        yield root, watch, ring
    finally:
        watch.close()


def consume(watch, ring):
    assert select.select([watch.fd], [], [], 5)[0], "missing kernel callback"
    watch.consume(os.read(watch.fd, 65536), ring)


def events_since(watch, ring, before, topic):
    reply = ring.wait_v2(queue_bell.Cursor(before["epoch"], before["seq"]),
        {topic}, {TIER}, 0, ring.now_v2()["coverage"]["version"])
    assert reply.get("events"), reply
    assert all(event["topic"] == topic and event["id"] == TIER
               and set(event) == {"seq", "topic", "id"} for event in reply["events"])
    return reply


@pytest.mark.parametrize("topic,relative", [
    ("tiers", f"tiers/{TIER}.json"),
    ("tier-commitments", f"tier-commitments/{TIER}.json"),
    ("tier-funding", f"tier-funding/{KEY}.{TIER}.funding.json"),
    ("tier-funding", f"tier-funding/{KEY}.{TIER}.output-funding.json"),
    ("tier-funding", f"tier-funding/retired/{KEY}.{TIER}.output-funding.json"),
])
@pytest.mark.parametrize("mutation", ["in-place", "replace", "remove"])
def test_tier_records_invalidate_exact_tier_without_ready_activity(tier_feed, topic, relative, mutation):
    root, watch, ring = tier_feed
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("old writer, deliberately not JSON")
    consume(watch, ring)
    assert topic in watch.covered_topics
    before = ring.now_v2()
    if mutation == "in-place":
        path.write_text("changed unparsed evidence")
    elif mutation == "replace":
        temporary = path.with_name(".replacement")
        temporary.write_text("replacement")
        temporary.replace(path)
    else:
        path.unlink()
    consume(watch, ring)
    events_since(watch, ring, before, topic)
    assert not list((root / record_store.READY).iterdir())


def test_tier_acquisition_rollback_commit_release_and_retirement(tier_feed):
    root, watch, ring = tier_feed
    base = root / record_store.TIER_RESERVATIONS / TIER
    for name in ("free", "held", "minted/dead"):
        (base / name).mkdir(parents=True, exist_ok=True)
    token = base / "free" / "stage_gib-0"
    token.touch()
    consume(watch, ring)

    def changed(mutate):
        before = ring.now_v2()
        mutate()
        consume(watch, ring)
        events_since(watch, ring, before, "tier-reservations")

    private = base / "held" / f"claiming.123.{KEY}.worker.lab.99.abcdef12"
    changed(private.mkdir)
    changed(lambda: token.rename(private / token.name))
    changed(lambda: (private / token.name).rename(token))
    changed(lambda: token.rename(private / token.name))
    committed = private.with_name(KEY)
    changed(lambda: private.rename(committed))
    changed(lambda: (committed / token.name).rename(token))
    changed(committed.rmdir)
    changed(lambda: token.rename(base / "minted" / "dead" / token.name))
    changed(lambda: (base / "minted" / "dead" / token.name).unlink())
    assert not list((root / record_store.READY).iterdir())


def test_output_funding_retirement_invalidates_without_a_ready_event(tier_feed):
    root, watch, ring = tier_feed
    records = record_store.RecordStore(root)
    active = records.funding_output_path(KEY, TIER)
    retired = records.funding_output_retired_path(KEY, TIER)
    active.parent.mkdir(exist_ok=True)
    retired.parent.mkdir()
    active.write_text("old writer")
    consume(watch, ring)
    before = ring.now_v2()
    active.rename(retired)
    consume(watch, ring)
    events_since(watch, ring, before, "tier-funding")


@pytest.mark.parametrize("subtree", ["", "held/.legacy", "minted/dead"])
def test_populated_moved_in_tier_tree_covers_hidden_tokens(tier_feed, tmp_path, subtree):
    root, watch, ring = tier_feed
    destination = root / record_store.TIER_RESERVATIONS / TIER / subtree
    destination.parent.mkdir(parents=True, exist_ok=True)
    incoming = tmp_path / "incoming"
    token_relative = Path("held/.legacy/.stage_gib-0") if not subtree else Path(".stage_gib-0")
    token = incoming / token_relative
    token.parent.mkdir(parents=True, exist_ok=True)
    token.touch()
    incoming.rename(destination)
    consume(watch, ring)
    assert "tier-reservations" in watch.covered_topics
    before = ring.now_v2()
    (destination / token_relative).unlink()
    consume(watch, ring)
    events_since(watch, ring, before, "tier-reservations")


def test_tier_change_during_reconstruction_is_replayed(tmp_path, monkeypatch):
    root = tmp_path / "queue"
    for directory in queue_bell.REQUIRED_STATE_DIRS:
        (root / directory).mkdir(parents=True)
    held = root / record_store.TIER_RESERVATIONS / TIER / "held" / ".legacy"
    held.mkdir(parents=True)
    token = held / "stage_gib-0"
    token.touch()
    scandir = os.scandir
    changed = []

    def delete_after_watch(descriptor):
        if Path(f"/proc/self/fd/{descriptor}").resolve() == held:
            token.unlink()
            changed.append(True)
        return scandir(descriptor)

    monkeypatch.setattr(os, "scandir", delete_after_watch)
    watch = queue_bell_events.InotifyQueue(root)
    try:
        ring = queue_bell_events.EventRing()
        ring.set_coverage(watch.covered_topics)
        before = ring.now_v2()
        assert changed == [True]
        consume(watch, ring)
        events_since(watch, ring, before, "tier-reservations")
    finally:
        watch.close()


@pytest.mark.parametrize("relative,topic", [
    (f"tiers/{TIER}.json", "tiers"),
    (f"tier-commitments/{TIER}.json", "tier-commitments"),
    (f"tier-funding/retired/{KEY}.{TIER}.output-funding.json", "tier-funding"),
    (f"tier-reservations/{TIER}/free/stage_gib-0", "tier-reservations"),
])
@pytest.mark.parametrize("shape", ["symlink", "fifo", "unreadable"])
def test_unsafe_tier_inputs_withdraw_coverage(tmp_path, relative, topic, shape):
    root = tmp_path / "queue"
    for directory in queue_bell.REQUIRED_STATE_DIRS:
        (root / directory).mkdir(parents=True)
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    if shape == "symlink":
        path.symlink_to(tmp_path / "absent")
    elif shape == "fifo":
        os.mkfifo(path)
    else:
        path.write_text("unknown")
        path.chmod(0)
    watch = queue_bell_events.InotifyQueue(root)
    try:
        # Tier tokens are read as names; their byte-read permission is irrelevant.
        if shape == "unreadable" and topic == "tier-reservations":
            assert topic in watch.covered_topics
        else:
            assert topic not in watch.covered_topics
    finally:
        watch.close()
        if shape == "unreadable":
            path.chmod(0o600)


@pytest.mark.parametrize("topic", ["tiers", "tier-commitments", "tier-funding"])
def test_external_hardlink_withdraws_byte_record_coverage(tier_feed, tmp_path, topic):
    root, watch, ring = tier_feed
    if topic == "tier-funding":
        path = root / topic / f"{KEY}.{TIER}.funding.json"
    else:
        path = root / topic / f"{TIER}.json"
    path.parent.mkdir(exist_ok=True)
    path.write_text("old writer")
    consume(watch, ring)
    assert topic in watch.covered_topics
    os.link(path, tmp_path / "external")
    consume(watch, ring)
    assert topic not in ring.now_v2()["coverage"]["topics"]


def test_tier_watch_loss_withdraws_coverage(tier_feed):
    root, watch, ring = tier_feed
    relative = Path(record_store.TIER_RESERVATIONS) / TIER / "minted" / "dead"
    (root / relative).mkdir(parents=True)
    consume(watch, ring)
    wd = next(wd for wd, path in watch.directories.items() if path == relative)
    watch.consume(queue_bell_events.EVENT_HEADER.pack(
        wd, queue_bell_events.IN_IGNORED, 0, 0), ring)
    assert "tier-reservations" not in ring.now_v2()["coverage"]["topics"]
    assert "tiers" in ring.now_v2()["coverage"]["topics"]


def test_moved_away_tier_tree_cannot_keep_its_old_identity(tier_feed, tmp_path):
    root, watch, ring = tier_feed
    tier = root / record_store.TIER_RESERVATIONS / TIER
    held = tier / "held" / KEY
    held.mkdir(parents=True)
    token = held / "stage_gib-0"
    token.touch()
    consume(watch, ring)
    assert "tier-reservations" in watch.covered_topics
    tier.rename(tmp_path / "moved")
    consume(watch, ring)
    assert not any(path is not None and not path.is_absolute()
                   and path.parts[:2] == (record_store.TIER_RESERVATIONS, TIER)
                   for path in watch.directories.values())
    before = ring.now_v2()
    (tmp_path / "moved" / "held" / KEY / token.name).unlink()
    if select.select([watch.fd], [], [], 0)[0]:
        watch.consume(os.read(watch.fd, 65536), ring)
    assert ring.now_v2()["seq"] == before["seq"]


@pytest.mark.parametrize("failure", [errno.EACCES, errno.ENOSPC])
def test_failed_tier_subtree_installation_never_advertises_coverage(tier_feed, monkeypatch, failure):
    root, watch, ring = tier_feed
    relative = Path(record_store.TIER_RESERVATIONS) / TIER
    (root / relative).mkdir(parents=True)
    add = watch._add

    def refuse(path, observed):
        if observed == relative:
            raise OSError(failure, "injected watch failure")
        return add(path, observed)

    monkeypatch.setattr(watch, "_add", refuse)
    with pytest.raises(OSError, match="injected watch failure"):
        consume(watch, ring)
    assert "tier-reservations" not in ring.now_v2()["coverage"]["topics"]


@pytest.mark.parametrize("relative,topic", [
    (f"tier-reservations/{TIER}/held", "tier-reservations"),
    ("tier-funding/retired", "tier-funding"),
])
def test_tier_directory_permission_loss_withdraws_coverage(tier_feed, relative, topic):
    root, watch, ring = tier_feed
    path = root / relative
    path.mkdir(parents=True)
    consume(watch, ring)
    assert topic in watch.covered_topics
    path.chmod(0)
    try:
        consume(watch, ring)
        assert topic not in ring.now_v2()["coverage"]["topics"]
    finally:
        path.chmod(0o700)


@pytest.mark.parametrize("identifier", ["stage space:worker", "stage:" + "x" * 128])
def test_store_valid_tier_outside_bell_bounds_withdraws_coverage(tmp_path, identifier):
    root = tmp_path / "queue"
    records = record_store.RecordStore(root)
    for directory in queue_bell.REQUIRED_STATE_DIRS:
        (root / directory).mkdir(parents=True, exist_ok=True)
    path = records.tier_record_path(identifier)
    path.parent.mkdir(exist_ok=True)
    path.write_text("not JSON")
    watch = queue_bell_events.InotifyQueue(root)
    try:
        assert "tiers" not in watch.covered_topics
    finally:
        watch.close()


@pytest.mark.parametrize("topic", ["tiers", "tier-reservations", "tier-funding", "tier-commitments"])
def test_dirty_tier_visibility_retains_id_and_original_read_budget(monkeypatch, topic):
    from prismabuild import queue_bell_input_reads as reads

    now = [0.0]
    monkeypatch.setattr(reads.time, "monotonic", lambda: now[0])
    pending = reads.DirtyInputs()
    pending.invalidate(topic, TIER, deadline=0.2)
    observed = []

    def unconfirmed(topic, identity, deadline):
        observed.append((topic, identity, deadline))
        pending.invalidate(topic, identity, deadline=100)
        return False

    class Stop:
        def is_set(self):
            return False

        def wait(self, delay):
            now[0] += delay
            return False

    with pytest.raises(queue_bell.Unavailable, match="visibility.*budget"):
        pending.confirm(unconfirmed, stop=Stop())
    assert observed == [(topic, TIER, 0.2)] * 3
    assert pending.dirty == {(topic, TIER)}
    assert now[0] == 0.2

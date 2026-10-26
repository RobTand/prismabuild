"""Tier policy invalidations retain the store's non-action identifiers."""
import os
from pathlib import Path
import select

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

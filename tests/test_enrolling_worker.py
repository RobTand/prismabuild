"""Enrollment permits pinned proofs without adding a publication participant."""
import json
import time

from prismabuild import capability_eligibility, fleet_roster, pool
import supervise
from test_the_coordinator_proves_the_fleet_before_a_barrier import (
    SHA, fleet, manifested_roster, post, publish_runtime,
)

ENROLLING = {"status": "enrolling", "status_reason": "awaiting cap proofs",
             "status_by": "rob", "status_unix": 1792713600}


def test_advisory_and_claim_placement_require_an_explicit_host_pin(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    tags = ["cpu", "x86", "celestia", "enrolling-only:celestia"]
    queue.announce(host="celestia", tags=tags, has_gpu=False,
                   capacity={"cpu": 8, "mem_gb": 29})
    for required, expected in [([], False), (["x86"], False), (["cpu"], False),
                                (["celestia"], True), (["x86", "celestia"], True)]:
        item = {"tags": required, "resources": {"cpu": 1, "mem_gb": 1}}
        assert queue.placeable(item) is expected
        assert queue._placement_matches(item, tags=frozenset(tags), has_gpu=False) is expected
    queue.announce(host="active", tags=["cpu", "x86"], has_gpu=False,
                   capacity={"cpu": 8, "mem_gb": 32})
    assert queue.class_dependency_gap("x86", {"active": ("active",)}, {}) is None


def test_enrolling_supervisor_can_start_and_class_quorums_exclude_it(tmp_path, monkeypatch):
    entry = {**ENROLLING, "loops": 2, "args": ["--class", "x86"]}
    roster = fleet_roster.Roster.capture(json.dumps({"boxes": {"proof": entry}}).encode())
    monkeypatch.setattr(fleet_roster.Roster, "for_host", lambda *a, **k: roster.canonical_box("proof"))
    assert supervise._config("proof").loops == 2
    assert fleet_roster.class_members(roster.document, "x86") == {}
    assert publish_runtime._active_roster_boxes(roster)[0] == []


def test_barrier_skips_enrolling_hosts_even_while_they_announce(fleet):
    from test_roster_presence_status import _generation_with_roster, _offer
    declarations = {"boxa": {}, "boxb": {}, "new": {"_alias": "celestia", **ENROLLING}}
    source = _generation_with_roster(fleet, "source", declarations)
    target = _generation_with_roster(fleet, "target", declarations)
    _offer(fleet, "boxa")
    _offer(fleet, "boxb")
    workers = publish_runtime.MIRROR.parent / "pb-queue/workers"
    workers.mkdir(parents=True, exist_ok=True)
    (workers / "celestia.json").write_text(json.dumps({
        "schema": "prismaquant.prismabuild.pool_offer.v1", "host": "celestia",
        "announced_unix": time.time()}))
    assert publish_runtime._barrier_roster((source, target)) == ["boxa", "boxb"]

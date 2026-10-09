"""The terminal-lead teardown beside the prelaunch gang code (#1543, #1594).

#1594 R3 defers a prelaunch gang member's election while a sibling's verdict
is unresolved, and R4 refuses a consumer whose filed plan does not carry the
prefix its sealed manifest declares.  The teardown of a gang whose member can
never start (#1543) reads the same verdicts.  These tests run both on the
real sealed members and filed plans of ``test_prelaunch_admission_1594``:

* A prelaunch gang with a dead member never fences a host: R3 defers the
  sibling's election.  The teardown still ends the gang, so the sibling does
  not wait in the queue for ever.
* A member the shape check refuses (``prelaunch_undeclared``) is not a
  terminal reading, however its leads ended.  Nothing marks it and nothing
  tears its gang down: only ``lead_not_resident`` and ``lead_unpinned`` do.
"""
from __future__ import annotations

from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from test_prelaunch_admission_1594 import (
    TIER, _compose_maps, _consumer_sha, _gang_pair, _publish_lead, _stage_lead)

from prismabuild import _gang, pool


def _fail_lead(queue, clock, monkeypatch, lead: str, sha: str) -> None:
    """Publish one movement node, claim it and end it failed."""
    _publish_lead(queue, clock, lead, sha)
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
    claimed = queue.claim(capacity={"cpu": 8, "mem_gb": 16}, tags=["sparklina"])
    assert claimed is not None and claimed["action_key"] == lead, claimed
    queue.finish(lead, status="failed")


def test_a_prelaunch_gang_with_a_dead_member_is_torn_down(
        gang_fleet, monkeypatch, tmp_path):
    """R3 keeps the fence off the host, and the teardown still ends the gang."""
    queue, clock, publish, finish, gclaim, denial, group, keys, leads = \
        _gang_pair(gang_fleet, tmp_path, "dead-prelaunch",
                   declare_manifest=True, declare_plan=True)
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    stage = tmp_path / "stage"
    stage.mkdir()
    first, second = keys
    _fail_lead(queue, clock, monkeypatch, leads[0], _consumer_sha(queue, first))
    _publish_lead(queue, clock, leads[1], _consumer_sha(queue, second))
    _stage_lead(queue, monkeypatch, leads[1], second,
                _consumer_sha(queue, second), stage)
    _compose_maps(monkeypatch, queue, {second: [leads[1]]})

    # The dead member reads terminal and leaves a mark; its resident sibling
    # is deferred by R3 and holds no election, so no host is fenced.
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "residency_lead_terminal"
    assert _gang.terminal_mark_path(queue, group, first).exists()
    assert gclaim("sparky") is None
    waiting = denial(second, "sparky")
    assert waiting["reason"] == "deferred_for_gang_prelaunch", waiting
    assert waiting["evidence"]["sibling_verdict"] == "lead_not_resident", waiting
    assert _gang.elections(queue, group, 2) == {}
    assert _gang.teardown(queue, group) is None, "one reading must not tear the gang down"

    # The reading stands for the window and the proof still agrees.
    clock[0] += _gang.TERMINAL_CONFIRM_S
    assert gclaim("sparklina") is None
    torn = _gang.teardown(queue, group)
    assert torn is not None and "residency_lead_terminal" in str(torn.get("reason")), torn
    queue.sweep_gangs()
    for key in keys:
        assert queue.item_path(pool.WITHDRAWN, key).exists(), key
        assert not queue.item_path(pool.READY, key).exists(), key


def test_a_shape_refused_member_is_never_a_terminal_reading(
        gang_fleet, monkeypatch, tmp_path):
    """R4 answers before the leads are read, so no teardown can follow it."""
    queue, clock, publish, finish, gclaim, denial, group, keys, leads = \
        _gang_pair(gang_fleet, tmp_path, "shape-dead",
                   declare_manifest=True, declare_plan=False)
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    first, second = keys
    _fail_lead(queue, clock, monkeypatch, leads[0], _consumer_sha(queue, first))

    for _ in range(3):
        assert gclaim("sparklina") is None
        refused = denial(first, "sparklina")
        assert refused["reason"] == "residency_prelaunch_undeclared", refused
        assert not _gang.terminal_mark_path(queue, group, first).exists()
        clock[0] += _gang.TERMINAL_CONFIRM_S
    assert _gang.teardown(queue, group) is None
    queue.sweep_gangs()
    assert queue.item_path(pool.READY, first).exists()
    assert queue.item_path(pool.READY, second).exists()

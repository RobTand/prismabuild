"""Tier operations retain the other half of an interrupted RAM reservation."""

import os
from pathlib import Path

import pytest

from prismabuild import pool


HOST = "tier-domain-host"
TIER = "ram:" + HOST
GRANT = "a" * 64
MOVER = "b" * 64


def seeded(queue, count):
    queue.records.ensure_layout()
    queue.ledger(HOST).ensure_capacity({"mem_gb": 8})
    queue.mint_tier_capacity(TIER, {"ram_gib": count})
    assert queue.take_tier_advance(TIER, GRANT, count, "ram_gib") == ("taken", "")


def test_short_advance_rolls_back_only_its_own_host_take(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    seeded(queue, 1)

    result = queue.take_tier_advance(TIER, GRANT, 1, "ram_gib")

    assert result == ("tier-short", "tier pool refused the take")
    assert queue.tier_ledger(TIER).holder_tokens(GRANT) == {"ram_gib": 1}
    assert queue.ledger(HOST).holder_tokens("ram-host:" + GRANT) == {"mem_gb": 1}


def test_unknown_advance_retains_the_preexisting_host_half(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    seeded(queue, 1)
    original = pool.ResourceLedger.acquire
    fault = OSError("tier take interrupted")

    def interrupted(ledger, key, demand):
        if ledger.host == TIER:
            raise fault
        return original(ledger, key, demand)

    monkeypatch.setattr(pool.ResourceLedger, "acquire", interrupted)
    result = queue.take_tier_advance(TIER, GRANT, 1, "ram_gib")

    assert result == ("unknown", f"tier ledger unreadable: {fault!r}")
    assert queue.tier_ledger(TIER).holder_tokens(GRANT) == {"ram_gib": 1}
    assert queue.ledger(HOST).holder_tokens("ram-host:" + GRANT) == {"mem_gb": 1}


@pytest.mark.parametrize("failed_count", [1, 3])
def test_failed_egress_keeps_the_host_half_of_its_retained_tokens(
        tmp_path, monkeypatch, failed_count):
    queue = pool.PoolQueue(tmp_path / "queue")
    seeded(queue, 3)
    holder = queue.root / pool.TIER_RESERVATIONS / TIER / "held" / GRANT
    blocked = {path.name for path in sorted(holder.iterdir())[:failed_count]}
    original = os.rename

    def interrupted(source, target, *args, **kwargs):
        source, target = Path(source), Path(target)
        if source.parent == holder and source.name in blocked and target.parent.name == "free":
            raise OSError("egress return interrupted")
        return original(source, target, *args, **kwargs)

    monkeypatch.setattr(os, "rename", interrupted)
    result = queue.release_tier_holder_for_egress(
        TIER, GRANT, destroy={}, free={"ram_gib": 3})

    assert result == {"destroyed": {}, "released": ({"ram_gib": 3 - failed_count}
                                                   if failed_count < 3 else {}),
                      "shortfall": {}}
    assert queue.tier_ledger(TIER).holder_tokens(GRANT) == {"ram_gib": failed_count}
    assert queue.ledger(HOST).holder_tokens("ram-host:" + GRANT) == {"mem_gb": failed_count}
    monkeypatch.undo()
    queue.release_tier_holder_for_egress(
        TIER, GRANT, destroy={}, free={"ram_gib": failed_count})
    assert queue.tier_ledger(TIER).holder_tokens(GRANT) == {}
    assert queue.ledger(HOST).holder_tokens("ram-host:" + GRANT) == {}


def test_handover_mirrors_the_actual_short_move(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    seeded(queue, 2)

    moved = queue.transfer_tier_reservation_count(TIER, GRANT, MOVER, 3)

    assert moved == 2
    assert queue.tier_ledger(TIER).holder_tokens(MOVER) == {"ram_gib": 2}
    assert queue.ledger(HOST).holder_tokens("ram-host:" + MOVER) == {"mem_gb": 2}
    assert queue.ledger(HOST).holder_tokens("ram-host:" + GRANT) == {}

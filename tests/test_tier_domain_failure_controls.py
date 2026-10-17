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
    queue.tier(TIER).mint({'ram_gib': count})
    assert queue.tier(TIER).take_advance(GRANT, count, 'ram_gib') == ('taken', '')


def test_short_advance_rolls_back_only_its_own_host_take(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    seeded(queue, 1)

    result = queue.tier(TIER).take_advance(GRANT, 1, 'ram_gib')

    assert result == ("tier-short", "tier pool refused the take")
    assert queue.tier(TIER).holdings(GRANT) == {'ram_gib': 1}
    assert queue.ledger(HOST).holder_tokens("ram-host:" + GRANT) == {"mem_gb": 1}


def test_unknown_advance_retains_the_preexisting_host_half(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    seeded(queue, 1)
    original = pool.ResourceLedger.begin_acquire
    fault = OSError("tier take interrupted")

    def interrupted(ledger, key, demand):
        if ledger.host == TIER:
            raise fault
        return original(ledger, key, demand)

    monkeypatch.setattr(pool.ResourceLedger, "begin_acquire", interrupted)
    result = queue.tier(TIER).take_advance(GRANT, 1, 'ram_gib')

    assert result == ("unknown", f"tier ledger unreadable: {fault!r}")
    assert queue.tier(TIER).holdings(GRANT) == {'ram_gib': 1}
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
    result = queue.tier(TIER).settle_egress(GRANT, destroy={}, free={'ram_gib': 3})

    assert result == {"destroyed": {}, "released": ({"ram_gib": 3 - failed_count}
                                                   if failed_count < 3 else {}),
                      "shortfall": {}}
    assert queue.tier(TIER).holdings(GRANT) == {'ram_gib': failed_count}
    assert queue.ledger(HOST).holder_tokens("ram-host:" + GRANT) == {"mem_gb": failed_count}
    monkeypatch.undo()
    queue.tier(TIER).settle_egress(GRANT, destroy={}, free={'ram_gib': failed_count})
    assert queue.tier(TIER).holdings(GRANT) == {}
    assert queue.ledger(HOST).holder_tokens("ram-host:" + GRANT) == {}


def test_handover_mirrors_the_actual_short_move(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    seeded(queue, 2)

    moved = queue.tier(TIER).handover_count(GRANT, MOVER, 3)

    assert moved == 2
    assert queue.tier(TIER).holdings(MOVER) == {'ram_gib': 2}
    assert queue.ledger(HOST).holder_tokens("ram-host:" + MOVER) == {"mem_gb": 2}
    assert queue.ledger(HOST).holder_tokens("ram-host:" + GRANT) == {}


def test_a_ram_take_charges_the_host_before_tier_occupancy(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    queue.ledger(HOST).ensure_capacity({"mem_gb": 2})
    queue.tier(TIER).mint({'ram_gib': 2})
    original = pool.ResourceLedger.begin_acquire
    observed = []

    def observe(ledger, holder, demand):
        if ledger.host == TIER:
            observed.append(queue.ledger(HOST).holder_tokens("ram-host:" + holder))
        return original(ledger, holder, demand)

    monkeypatch.setattr(pool.ResourceLedger, "begin_acquire", observe)
    assert queue.tier(TIER).take(GRANT, {'ram_gib': 2})
    assert observed == [{"mem_gb": 2}]
    assert queue.ledger(HOST).holder_tokens("ram-host:" + GRANT) == {"mem_gb": 2}


def test_an_unspent_conclusion_returns_its_ram_host_mirror(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    seeded(queue, 2)

    assert queue._release_reservation(GRANT, host=None) == 2

    assert queue.tier(TIER).holdings(GRANT) == {}
    assert queue.ledger(HOST).holder_tokens("ram-host:" + GRANT) == {}


def test_named_handover_moves_only_the_ram_occupancy_mirror(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    seeded(queue, 2)
    rate = pool.storage_tiers.FILL_KIND
    queue.tier(TIER).mint({'ram_gib': 2, rate: 3})
    ledger = queue.tier(TIER)
    assert ledger.take(GRANT, {rate: 3})
    names = ledger.holder_state(GRANT)
    chosen = [min(name for name in names if name.startswith(kind + "-"))
              for kind in ("ram_gib", rate)]

    assert ledger.handover_named(GRANT, MOVER, chosen) == 2
    assert queue.ledger(HOST).holder_tokens("ram-host:" + GRANT) == {"mem_gb": 1}
    assert queue.ledger(HOST).holder_tokens("ram-host:" + MOVER) == {"mem_gb": 1}
    assert ledger.handover_named(GRANT, MOVER, chosen) == 2
    assert queue.ledger(HOST).holder_tokens("ram-host:" + GRANT) == {"mem_gb": 1}
    assert queue.ledger(HOST).holder_tokens("ram-host:" + MOVER) == {"mem_gb": 1}


def test_a_partial_native_take_keeps_the_preexisting_named_tokens(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    seeded(queue, 1)
    queue.tier(TIER).mint({'ram_gib': 3})
    ledger = queue.tier(TIER)
    before = ledger.holder_state(GRANT)
    blocked = max(ledger.paths.free.iterdir()).name
    original = os.rename
    holder = ledger.paths.held / GRANT

    def interrupted(source, target, *args, **kwargs):
        source, target = Path(source), Path(target)
        if (source.parent.name.startswith("claiming.") and source.name == blocked
                and target.parent == holder):
            raise OSError("one native commit rename failed")
        return original(source, target, *args, **kwargs)

    monkeypatch.setattr(os, "rename", interrupted)
    assert queue.tier(TIER).take_advance(GRANT, 2, 'ram_gib') == ('tier-short', 'tier pool refused the take')
    assert ledger.holder_state(GRANT) == before
    assert queue.ledger(HOST).holder_tokens("ram-host:" + GRANT) == {"mem_gb": 1}

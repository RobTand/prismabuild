"""The ram window reads its own advance fence back into free (#906).

The stage window and the ram window share one fence rule (#745): a
consumer's advance fence holds the room its next leg needs, and that room is
readable back into free for that consumer's own window decision.  The fence
never blocks the window it protects.  Everyone else (stealers, later windows,
the pressure probe) still sees the fenced ledger.

The stage window applies it (``residency_window`` adds the consumer's own
grant back into ``free``).  ``ram_residency_window`` sums the consumer's held
ram grants and passes them to ``_ram_window_state`` as ``own_fence_gib``, but
``_ram_window_state`` never added them to ``free``.  So on a tmpfs with room
for exactly the current promotion plus its advance, the protection pass
blind-holds the advance's room and the window can then publish only the
current.  The advance cannot publish into the room held for it.

The fixture is the one ``test_the_nonfinal_window_fences_its_advance.py``
uses for the ram path, with the tmpfs cut from 3 GiB to 2 GiB: one promotion
plus its advance, and nothing to spare.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import storage_tiers, window_credit  # noqa: E402
import residency_publication  # noqa: E402
import tier_loop  # noqa: E402
from test_the_nonfinal_window_fences_its_advance import (  # noqa: E402
    CONSUMER, GIB, RAM_TIER, SPAN, TIER, _assert_one_live_advance, _movers,
    _plan, _publish_consumer, _published, _queue, _ram_tiers)


def test_the_ram_window_publishes_into_its_own_fence(tmp_path: Path) -> None:
    """Room for the current promotion and its advance, and nothing more.

    The joint gate admits the window (0 held + 1 current + 1 next against 2)
    and the protection pass holds the advance's 1 GiB under its grant.  With
    the grant read back, the window sees 2 GiB free, publishes both
    promotions, and the grant binds to the advance's own row.  Without it,
    the window sees 1 GiB and the advance never publishes.
    """

    (tmp_path / "ram").mkdir()
    queue = _queue(tmp_path, stage_gib=3, ram_gib=2)
    epoch = storage_tiers.ensure_ram_epoch(tmp_path / "ram", host="dl380g10")
    assert epoch is not None
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier": "ram",
        "tier_id": RAM_TIER, "host": "dl380g10",
        "mountpoint": str(tmp_path / "ram"), "epoch": str(epoch["epoch"]),
        "capacity_bytes": 2 * GIB})
    plan = _plan(queue, 3, ram=True)
    _publish_consumer(queue, plan, CONSUMER)
    r0, r1, r2 = _movers(plan, "ram_mover_row")
    stage_ledger = queue.tier_ledger(TIER)
    for ordinal, mover in ((0, _movers(plan)[0]), (1, _movers(plan)[1])):
        assert stage_ledger.acquire(mover, {"stage_gib": 1}) is True
        residency_publication.vouch_landed(
            queue, consumer_action_key=CONSUMER, mover_action_key=mover,
            tier_id=TIER, stage_root="/stage/prewarm",
            manifest_sha256=str(plan["manifest_sha256"]),
            range_start_bytes=ordinal * SPAN,
            range_end_bytes=(ordinal + 1) * SPAN)

    events = tier_loop.ram_residency_window(queue, tiers=_ram_tiers(tmp_path))

    published = _published(events, "ram-mover-published")
    assert published == {r0, r1}, events
    assert [entry for entry in events
            if entry.get("event") == "ram-window-gated"] == [], events
    # The advance's room is held under the promotion's own row, and the
    # tmpfs has nothing left over: the fence moved, it was not doubled.
    _assert_one_live_advance(queue, RAM_TIER, "ram_gib", CONSUMER, plan, r1,
                             "phase-1", SPAN, 2 * SPAN)
    ram_ledger = queue.tier_ledger(RAM_TIER)
    assert ram_ledger.available().get("ram_gib") == 1
    assert window_credit.held_grants(ram_ledger) == []

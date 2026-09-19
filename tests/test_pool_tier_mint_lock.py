"""Tier capacity is minted under a lock, and the probe rule is pinned (#593).

``mint_tier_capacity`` runs ``ensure_capacity`` then ``retire_free_capacity``.
Host ledgers do the same work only inside the box's serialized capacity
prelude; the tier path had no serialization at all, and ``tier_loop --once``
is an advertised operator mode that runs a second minter while the supervised
role is up.  Two minters against one tier were outside what the ``O_EXCL``
mint-marker analysis covers, so each tier mints under its own lock and a
second minter waits rather than interleaving.

The three-mover scenario is the related limit the lock does not close: with no
attributed receipt the tier offers the oldest ready mover's own demand as its
whole fill supply, and a smaller ready mover's demand still fits the free
tokens beside a running holder.  That concurrency is the window publisher's to
judge, not the mint's -- fungible tokens cannot single out one waiter -- so
this file pins the sizing rule and the residual rather than redesigning either.

Nothing here touches a real pool, a real device, or another box.
"""

from __future__ import annotations

import threading
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
OTHER_TIER = "prismabuild-stage:sparky"
KIND = "stage_gib"
FILL = storage_tiers.FILL_KIND
FILL_DEMAND = f"{FILL}@{TIER}"
STAGE_DEMAND = f"{KIND}@{TIER}"
GIB = storage_tiers.GIB


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    return queue


# -- the mint lock -----------------------------------------------------------


def test_a_second_minter_waits_for_the_first(tmp_path: Path) -> None:
    """One tier, one minter at a time; different tiers mint independently."""

    queue = _queue(tmp_path)
    held: list[bool] = []

    with queue.tier_mint_lock(TIER):
        def probe() -> None:
            with queue.tier_mint_lock(OTHER_TIER, blocking=False) as free:
                assert free is True, "an unrelated tier must not be locked"
            with queue.tier_mint_lock(TIER, blocking=False) as second:
                held.append(second)

        worker = threading.Thread(target=probe)
        worker.start()
        worker.join(timeout=60.0)
        assert not worker.is_alive(), "the probe thread hung on the lock"

    assert held == [False], "a second minter ran inside the first one's mint"


def test_concurrent_mints_converge_on_the_minted_capacity(tmp_path: Path) -> None:
    """Eight threads minting at once leave exactly what discovery measured."""

    queue = _queue(tmp_path)
    errors: list[BaseException] = []

    def mint() -> None:
        try:
            for _ in range(5):
                queue.mint_tier_capacity(TIER, {KIND: 6, FILL: 60})
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    workers = [threading.Thread(target=mint) for _ in range(8)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=120.0)
    assert not any(worker.is_alive() for worker in workers)
    assert errors == []
    assert queue.tier_ledger(TIER).capacity() == {KIND: 6, FILL: 60}


def test_minting_down_retires_only_free_tokens(tmp_path: Path) -> None:
    """The lock serializes the retire half too: a holder keeps its tokens."""

    queue = _queue(tmp_path)
    queue.mint_tier_capacity(TIER, {KIND: 4})
    ledger = queue.tier_ledger(TIER)
    handle = ledger.begin_acquire("a" * 64, {KIND: 3})
    assert handle is not None
    assert ledger.commit_acquire("a" * 64, handle) == 3

    result = queue.mint_tier_capacity(TIER, {KIND: 1})

    assert ledger.holder_tokens("a" * 64) == {KIND: 3}
    assert result["capacity"] == {KIND: 3}
    assert result["retired"] == {KIND: 1}


# -- the three-mover probe scenario ------------------------------------------


def _discover(**kwargs) -> dict[str, dict[str, object]]:
    record: dict[str, object] = {
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
        "tier_id": TIER, "host": "dl380g10", "tier": "stage",
        "mountpoint": "/stage/prewarm", "capacity_bytes": 600 * GIB,
    }
    record[storage_tiers.FILL_RECORD_FIELD] = (
        storage_tiers.fill_rate_from_records(kwargs.get("fill_records") or ()))
    return {TIER: record}


def _cycle(queue: pool.PoolQueue) -> dict[str, object]:
    return tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                           receipts=tier_loop.ReceiptCache(),
                           discover=_discover)[0]


def _mover(queue: pool.PoolQueue, key: str, fill: int) -> None:
    queue.publish(action_key=key * 64, cas_root="/cas", worker_script="w.py",
                  checkout_root="/co", tags=["dl380g10"],
                  resources={"cpu": 4, "mem_gb": 2, FILL_DEMAND: fill,
                             STAGE_DEMAND: 4})


def test_the_probe_is_sized_for_the_oldest_ready_mover(
    tmp_path: Path, monkeypatch,
) -> None:
    """A stage tier, three movers, no attributed receipt.

    A runs holding capacity and fill; B is the oldest ready mover; C is
    smaller and newer.  The tier offers B's own demand as its whole fill
    supply -- the probe rule -- and that supply is fungible: C's demand fits
    the tokens beside A's, so A+C are admittable together on a pool nothing
    has measured.  The first assert is the rule; the second is the residual
    the window publisher still owns (see #593).
    """

    queue = _queue(tmp_path)
    clock = iter(range(1_700_000_000, 1_700_000_100))
    monkeypatch.setattr(pool, "_now", lambda: next(clock))
    _mover(queue, "a", 100)
    _cycle(queue)
    claimed = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    assert claimed is not None and claimed["action_key"] == "a" * 64
    _mover(queue, "b", 210)
    _mover(queue, "c", 50)

    record = _cycle(queue)

    assert record["fill_source"] == "probe"
    assert record["tokens"][FILL] == 210
    assert record["fill_probe_mb_s"] == 210
    free = queue.tier_ledger(TIER).available().get(FILL, 0)
    assert free == 110
    assert free >= 50, (
        "C's demand fits beside A's on an unmeasured pool: the mint cannot "
        "single out B, so the window publisher must judge this concurrency")

"""Tier tokens are cluster-scoped, taken at claim and returned on every ending (#583).

A storage tier lives on one box but is reserved against from any box, so
its ledger sits under its own root and is keyed by tier id, never by
hostname.  ``_claim`` takes tier tokens after host admission and before the
rename, every path that concludes a claim returns them through one helper,
and a shortage on a tier is a denial that neither holds host tokens nor
ages the item: the box does other work while the tier is busy.
"""

from __future__ import annotations

from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import adaptive_cpu, pool  # noqa: E402

KEY_A = "a" * 64
KEY_B = "b" * 64
TIER = "prismabuild-stage:dl380g10"
STAGE = f"stage_gib@{TIER}"
FILL = f"fill_mb_s_pool_side@{TIER}"


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, key: str, resources: dict[str, int], **kw: object) -> None:
    q.publish(
        action_key=key,
        cas_root=q.root / "cas",
        checkout_root=q.root / "co",
        worker_script=q.root / "worker.py",
        resources=resources,
        **kw,
    )


def _denial(q: pool.PoolQueue, key: str) -> dict[str, object] | None:
    """This box's newest claim verdict for ``key``, from its host-local denial log."""

    path = adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    matching = [entry for entry in records.values()
                if isinstance(entry, dict) and entry.get("action_key") == key]
    if not matching:
        return None
    return max(matching, key=lambda entry: float(entry.get("denied_unix", 0.0)))


def test_publish_refuses_a_malformed_tier_demand_key(queue: pool.PoolQueue) -> None:
    for bad in ("stage_gib@", "@tier", "stage_gib@a@b", "stage_gib@bad/slash"):
        with pytest.raises(pool.PoolContractError):
            _publish(queue, KEY_A, {"cpu": 1, bad: 1})
    _publish(queue, KEY_A, {"cpu": 1, STAGE: 1})


def test_tier_ledger_is_separate_from_host_ledgers(queue: pool.PoolQueue) -> None:
    queue.mint_tier_capacity(TIER, {"stage_gib": 2})
    assert queue.tier_ids() == [TIER]
    assert queue.tier_ledger(TIER).capacity() == {"stage_gib": 2}
    # A tier is not a box: the claim-holder scan must never see it.
    assert queue.tier_ledger(TIER).acquire(KEY_A, {"stage_gib": 1})
    assert queue.claim_reservation_hosts(KEY_A) == []
    assert queue.tier_holdings(KEY_A) == {TIER: {"stage_gib": 1}}
    with pytest.raises(pool.PoolContractError):
        queue.tier_ledger("stage_gib@tier")
    with pytest.raises(pool.PoolContractError):
        queue.tier_ledger("../escape")


def test_mint_follows_discovery_down_as_well_as_up(queue: pool.PoolQueue) -> None:
    queue.mint_tier_capacity(TIER, {"stage_gib": 3, "fill_mb_s_pool_side": 2})
    assert queue.tier_ledger(TIER).capacity() == {"stage_gib": 3, "fill_mb_s_pool_side": 2}
    assert queue.tier_ledger(TIER).acquire(KEY_A, {"stage_gib": 2})
    # The stage shrank to 1 GiB and the fill kind vanished: free tokens go,
    # held ones stay until their holder finishes.
    result = queue.mint_tier_capacity(TIER, {"stage_gib": 1})
    assert result["retired"] == {"stage_gib": 1, "fill_mb_s_pool_side": 2}
    assert queue.tier_ledger(TIER).capacity() == {"stage_gib": 2}
    assert queue.tier_ledger(TIER).holder_tokens(KEY_A) == {"stage_gib": 2}
    with pytest.raises(pool.PoolContractError):
        queue.mint_tier_capacity(TIER, {"stage_gib": -1})


def test_claim_takes_tier_tokens_and_finish_returns_them(queue: pool.PoolQueue) -> None:
    queue.mint_tier_capacity(TIER, {"stage_gib": 2, "fill_mb_s_pool_side": 5})
    _publish(queue, KEY_A, {"cpu": 1, STAGE: 2, FILL: 5})
    claimed = queue.claim(owner="mover", capacity={"cpu": 1})
    assert claimed is not None and claimed["action_key"] == KEY_A
    assert claimed["tier_reservations"] == {
        TIER: {"fill_mb_s_pool_side": 5, "stage_gib": 2}}
    # Tier demand alone asks nothing about residency, so no verdict is written.
    assert "residency_verdict" not in claimed
    assert claimed["reserved_on"] == pool.socket.gethostname()
    assert queue.ledger().held() == {"cpu": 1}
    assert queue.tier_holdings(KEY_A) == {TIER: {"stage_gib": 2, "fill_mb_s_pool_side": 5}}
    assert queue.tier_ledger(TIER).available() == {}
    queue.finish(KEY_A, status="executed", claim_snapshot=claimed)
    assert queue.ledger().held() == {}
    assert queue.tier_holdings(KEY_A) == {}
    assert queue.tier_ledger(TIER).available() == {"stage_gib": 2, "fill_mb_s_pool_side": 5}
    # Nothing claim-scoped survives into the outcome or a requeue.
    done = pool._read_json(queue.item_path(pool.DONE, KEY_A))
    assert done is not None and done["tier_reservations"] == {
        TIER: {"fill_mb_s_pool_side": 5, "stage_gib": 2}}
    assert "tier_reservations" in pool.PoolQueue._CLAIM_SCOPED_FIELDS


def test_finish_returns_tier_tokens_through_the_helper(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mutate the driver: with the helper cut, finish leaves tier tokens held."""

    queue.mint_tier_capacity(TIER, {"stage_gib": 1})
    _publish(queue, KEY_A, {"cpu": 1, STAGE: 1})
    claimed = queue.claim(owner="mover", capacity={"cpu": 1})
    assert claimed is not None
    monkeypatch.setattr(pool.PoolQueue, "release_tier_reservations", lambda self, key: 0)
    queue.finish(KEY_A, status="executed", claim_snapshot=claimed)
    assert queue.ledger().held() == {}
    assert queue.tier_holdings(KEY_A) == {TIER: {"stage_gib": 1}}


@pytest.mark.parametrize(
    ("minted", "demand", "reason"),
    [
        ({}, {"cpu": 1, STAGE: 1}, "tier_unknown"),
        ({"stage_gib": 1}, {"cpu": 1, STAGE: 2}, "never_fits_tier_capacity"),
    ],
)
def test_tier_shortage_denies_without_holding_the_host_or_aging(
    queue: pool.PoolQueue, minted: dict[str, int], demand: dict[str, int], reason: str,
) -> None:
    if minted:
        queue.mint_tier_capacity(TIER, minted)
    _publish(queue, KEY_A, demand)
    assert queue.claim(owner="mover", capacity={"cpu": 1}) is None
    assert queue.ledger().held() == {}, "host tokens must be returned on a tier shortage"
    assert queue.tier_holdings(KEY_A) == {}
    assert queue.passes(KEY_A) == 0, "a cluster-scoped shortage must not withhold this box"
    assert queue.item_path(pool.READY, KEY_A).exists()
    denial = _denial(queue, KEY_A)
    assert denial is not None and denial["reason"] == reason
    assert denial["evidence"]["tier_shortage"]["tier_id"] == TIER


def test_busy_tier_denies_the_second_mover_and_frees_it_after_the_first(
    queue: pool.PoolQueue,
) -> None:
    queue.mint_tier_capacity(TIER, {"stage_gib": 2})
    _publish(queue, KEY_A, {"cpu": 1, STAGE: 2}, priority=1)
    _publish(queue, KEY_B, {"cpu": 1, STAGE: 1})
    first = queue.claim(owner="one", capacity={"cpu": 2})
    assert first is not None and first["action_key"] == KEY_A
    # The tier is full; the second item is denied, not aged, and the box
    # keeps its second CPU free for anything else.
    assert queue.claim(owner="two", capacity={"cpu": 2}) is None
    assert queue.ledger().held() == {"cpu": 1}
    assert queue.passes(KEY_B) == 0
    denial = _denial(queue, KEY_B)
    assert denial is not None and denial["reason"] == "tier_reservation_unavailable"
    assert denial["evidence"]["tier_shortage"]["available"] == {}
    queue.finish(KEY_A, status="executed", claim_snapshot=first)
    second = queue.claim(owner="two", capacity={"cpu": 2})
    assert second is not None and second["action_key"] == KEY_B
    assert queue.tier_holdings(KEY_B) == {TIER: {"stage_gib": 1}}


def test_losing_claimant_returns_only_its_own_tier_handle(queue: pool.PoolQueue) -> None:
    queue.mint_tier_capacity(TIER, {"stage_gib": 1})
    _publish(queue, KEY_A, {"cpu": 1, STAGE: 1})
    original_intent = queue._write_claim_intent
    observed: dict[str, object] = {}

    def intent(key: str, *, owner: str) -> None:
        if owner == "winner":
            observed["contender"] = queue.claim(owner="loser", capacity={"cpu": 1})
            observed["tier_held_during"] = dict(queue.tier_ledger(TIER).held())
        original_intent(key, owner=owner)

    queue._write_claim_intent = intent  # type: ignore[method-assign]
    winner = queue.claim(owner="winner", capacity={"cpu": 1})
    assert winner is not None and winner["claimed_by"] == "winner"
    assert observed["contender"] is None
    assert observed["tier_held_during"] == {"stage_gib": 1}
    assert queue.tier_holdings(KEY_A) == {TIER: {"stage_gib": 1}}
    assert queue.tier_ledger(TIER).held_keys() == [KEY_A]
    assert not [p for p in queue.tier_ledger(TIER).held_dir.iterdir()
                if pool._is_acquisition(p.name)], "no private handle may survive"


def test_reap_stale_returns_tier_tokens_with_the_host_tokens(queue: pool.PoolQueue) -> None:
    queue.mint_tier_capacity(TIER, {"stage_gib": 1})
    _publish(queue, KEY_A, {"cpu": 1, STAGE: 1})
    claimed = queue.claim(owner="mover", capacity={"cpu": 1})
    assert claimed is not None
    assert queue.reap_stale(timeout_s=-1.0) == [KEY_A]
    assert queue.ledger().held() == {}
    assert queue.tier_holdings(KEY_A) == {}
    assert queue.tier_ledger(TIER).available() == {"stage_gib": 1}
    requeued = pool._read_json(queue.item_path(pool.READY, KEY_A))
    assert requeued is not None and "tier_reservations" not in requeued


def test_withdrawing_a_claimed_action_returns_tier_tokens_when_its_owner_concludes(
    queue: pool.PoolQueue,
) -> None:
    queue.mint_tier_capacity(TIER, {"stage_gib": 1})
    _publish(queue, KEY_A, {"cpu": 1, STAGE: 1})
    claimed = queue.claim(owner="mover", capacity={"cpu": 1})
    assert claimed is not None
    queue.withdraw(KEY_A)
    # A withdrawal marks; the owner concludes the claimed attempt and that
    # conclusion, filed under withdrawn, is what returns both ledgers.
    assert queue.tier_holdings(KEY_A) == {TIER: {"stage_gib": 1}}
    queue.finish(KEY_A, status="failed", claim_snapshot=claimed)
    assert queue.ledger().held() == {}
    assert queue.tier_holdings(KEY_A) == {}
    assert queue.item_path(pool.WITHDRAWN, KEY_A).exists()


def test_stale_tier_acquisition_is_swept_from_the_tier_root(queue: pool.PoolQueue) -> None:
    queue.mint_tier_capacity(TIER, {"stage_gib": 1})
    ledger = queue.tier_ledger(TIER)
    handle = ledger.begin_acquire(KEY_A, {"stage_gib": 1})
    assert handle is not None
    assert ledger.available() == {}
    time.sleep(0.01)
    swept = queue.sweep_stale_acquisitions(grace_s=0.0)
    assert swept == [f"{pool.TIER_RESERVATIONS}/{TIER}/{handle}"]
    assert ledger.available() == {"stage_gib": 1}


def test_reclaim_terminal_reservation_reaches_tier_tokens_no_box_holds(
    queue: pool.PoolQueue,
) -> None:
    queue.mint_tier_capacity(TIER, {"stage_gib": 1})
    _publish(queue, KEY_A, {"cpu": 1, STAGE: 1})
    claimed = queue.claim(owner="mover", capacity={"cpu": 1})
    assert claimed is not None
    queue.finish(KEY_A, status="executed", claim_snapshot=claimed)
    # A pinned residency: tier tokens under a done key with no box holder.
    assert queue.tier_ledger(TIER).acquire(KEY_A, {"stage_gib": 1})
    assert queue.claim_reservation_hosts(KEY_A) == []
    result = queue.reclaim_terminal_reservation(KEY_A)
    assert result == {"action_key": KEY_A, "released": 1, "hosts": []}
    assert queue.tier_holdings(KEY_A) == {}


def test_announce_tier_files_an_advisory_record(queue: pool.PoolQueue) -> None:
    record = {"tier_id": TIER, "tier": "stage", "capacity_bytes": 5 << 30, "mountpoint": "/x"}
    path = queue.announce_tier(record)
    assert path == queue.root / pool.TIERS / f"{TIER}.json"
    tiers = queue.tiers()
    assert len(tiers) == 1 and tiers[0]["tier_id"] == TIER
    assert tiers[0]["capacity_bytes"] == 5 << 30 and "announced_unix" in tiers[0]
    with pytest.raises(pool.PoolContractError):
        queue.announce_tier({"tier_id": "bad@id"})

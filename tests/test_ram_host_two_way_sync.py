"""The RAM host mirror is a two-way sync, run by the single writer (#1245 r2).

Claim-path takes straight off the tier ledger (``begin_acquire`` of the
remainder beyond a fence, or the whole demand unfenced) hold no
``ram-host:`` tokens of their own, so the host pool would under-count
exactly those bytes -- the unsafe direction.  The per-cycle reconcile
therefore syncs both ways: every live tier holder's host hold is made
equal to its occupancy tokens (rate tokens never mirrored), and a host
half that cannot land refuses window growth until it does.
"""

from __future__ import annotations

from pathlib import Path
import sys

import pytest

import prismabuild.pool as pool  # noqa: E402
import prismabuild.storage_tiers as storage_tiers  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import tier_loop  # noqa: E402

HOST = "dl380g10"
RAM_TIER = storage_tiers.tier_id("ram", HOST)


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    ledger = queue.ledger(HOST)
    ledger.ensure_capacity({"cpu": 80, "mem_gb": 256})
    queue.mint_tier_capacity(RAM_TIER, {"ram_gib": 300,
                                        storage_tiers.FILL_KIND: 4096})
    return queue


def _mover_row(queue: pool.PoolQueue, tmp_path: Path, key: str) -> None:
    queue.publish(
        action_key=key, cas_root=tmp_path / "cas",
        checkout_root=tmp_path / "checkout",
        worker_script=tmp_path / "worker.py", tags=["x86"],
        resources={"cpu": 1, "mem_gb": 2})


def _claim_path_take(queue: pool.PoolQueue, mover: str,
                      demand: dict[str, int]) -> None:
    """The claim path exactly: a private take, then the winner's commit."""

    tier = queue.tier_ledger(RAM_TIER)
    handle = tier.begin_acquire(mover, demand)
    assert handle is not None, "fixture: the claim-path take must succeed"
    assert tier.commit_acquire(mover, handle) > 0, \
        "fixture: the claim must commit its tokens"


def test_claim_path_ram_takes_gain_their_host_half(tmp_path: Path) -> None:
    """Unfenced and partial-fence movers both mirror after one cycle."""

    queue = _queue(tmp_path)
    tier = queue.tier_ledger(RAM_TIER)
    host = queue.ledger(HOST)
    bare, fenced = "a" * 64, "b" * 64
    grant = "c" * 64
    _mover_row(queue, tmp_path, bare)
    _mover_row(queue, tmp_path, fenced)
    # A row holds 48 beside the whole story.
    assert host.acquire("d" * 64, {"mem_gb": 48}) is True
    # The claim path: the whole demand, straight off the tier ledger.
    _claim_path_take(queue, bare, {"ram_gib": 100})
    # A partial fence: 40 fenced, handed to the mover, 60 claimed beside it.
    assert queue.take_tier_advance(RAM_TIER, grant, 40, "ram_gib")[0] == "taken"
    assert queue.transfer_fence(RAM_TIER, grant, fenced) == 40
    _claim_path_take(queue, fenced, {"ram_gib": 60})

    verdict = queue.reconcile_ram_host_holds(RAM_TIER, ())

    assert verdict["converged"] is True
    assert host.holder_tokens("ram-host:" + bare) == {"mem_gb": 100}
    assert host.holder_tokens("ram-host:" + fenced) == {"mem_gb": 100}
    # The mirror is subtracted from the rows read: only the row's 48 stays.
    assert queue.rows_host_memory_held(HOST) == 48


def test_rate_tokens_are_never_mirrored(tmp_path: Path) -> None:
    """Only occupancy GiB crosses to the host; a rate kind refuses loudly."""

    queue = _queue(tmp_path)
    tier = queue.tier_ledger(RAM_TIER)
    mover = "a" * 64
    _mover_row(queue, tmp_path, mover)
    _claim_path_take(queue, mover,
                     {"ram_gib": 100, storage_tiers.FILL_KIND: 31})

    verdict = queue.reconcile_ram_host_holds(RAM_TIER, ())

    assert verdict["converged"] is True
    assert queue.ledger(HOST).holder_tokens(
        "ram-host:" + mover) == {"mem_gb": 100}
    # The primitive refuses a rate kind outright, so no caller can hand it
    # a fill ration and get GiB back.
    with pytest.raises(pool.PoolContractError):
        queue.take_tier_advance(
            RAM_TIER, "e" * 64, 8, storage_tiers.FILL_KIND)


def test_a_missing_host_mirror_refuses_window_growth(tmp_path: Path) -> None:
    """A host half that cannot land is named, and the gate refuses."""

    queue = _queue(tmp_path)
    tier = queue.tier_ledger(RAM_TIER)
    mover = "a" * 64
    _mover_row(queue, tmp_path, mover)
    # The row leaves the host pool no room: 240 of 256.
    assert queue.ledger(HOST).acquire("d" * 64, {"mem_gb": 240}) is True
    _claim_path_take(queue, mover, {"ram_gib": 100})

    verdict = queue.reconcile_ram_host_holds(RAM_TIER, ())

    assert verdict["converged"] is False
    missing = [event for event in verdict["events"]
               if event.get("reason") == "ram-host-hold-missing"]
    assert missing and missing[0]["holder"] == "ram-host:" + mover
    assert missing[0]["gib"] == 100
    # Until the sync converges the window gate reads no rows number at
    # all: None is the refusal the admission already knows.
    assert tier_loop.rows_held_for_gate(queue, HOST, False) is None
    assert tier_loop.rows_held_for_gate(
        queue, HOST, True) == queue.rows_host_memory_held(HOST)


def test_a_failed_host_transfer_is_heard_and_healed(
        tmp_path: Path, monkeypatch) -> None:
    """The tier half moves, the fault is named, the sync heals by name."""

    queue = _queue(tmp_path)
    host = queue.ledger(HOST)
    grant, mover = "a" * 64, "b" * 64
    _mover_row(queue, tmp_path, mover)
    assert host.acquire("d" * 64, {"mem_gb": 48}) is True
    assert queue.take_tier_advance(RAM_TIER, grant, 60, "ram_gib")[0] == "taken"

    real_transfer = pool.ResourceLedger.transfer

    def _host_side_transfer(self, from_holder: str, to_holder: str) -> int:
        if from_holder.startswith("ram-host:"):
            raise OSError("boom: host transfer unreadable")
        return real_transfer(self, from_holder, to_holder)

    monkeypatch.setattr(pool.ResourceLedger, "transfer", _host_side_transfer)
    faults: list[dict[str, object]] = []
    moved = queue.transfer_fence(RAM_TIER, grant, mover, faults=faults)
    monkeypatch.undo()

    # The tier half moved; the fault was named, not swallowed; the host
    # half stayed under the grant's name.
    assert moved == 60
    assert faults and faults[0]["reason"] == "ram_host_transfer_failed"
    assert faults[0]["from"] == grant
    assert faults[0]["to"] == mover
    assert host.holder_tokens("ram-host:" + grant) == {"mem_gb": 60}

    # One cycle later the sync has healed both halves by name.
    verdict = queue.reconcile_ram_host_holds(RAM_TIER, ())
    assert verdict["converged"] is True
    assert host.holder_tokens("ram-host:" + mover) == {"mem_gb": 60}
    assert host.holder_tokens("ram-host:" + grant) == {}
    reasons = {event.get("reason") for event in verdict["events"]}
    assert "orphan_ram_host_hold" in reasons

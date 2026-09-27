"""#1245 review B1/B3: host tokens follow the fence for its whole life.

A RAM fill's host mem_gb hold must move with its tier tokens: reserved
under the grant beside the tier take, transferred onto the mover when the
fence hands off, and returned at whichever end the fence meets -- a
terminal cancel, egress or evict -- so the host pool never leaks a landed
fill and the window gate never counts one twice.  Orphan host holds whose
tier holder died (a crash between the host take and the tier take) are
reconciled by name every cycle, and the rows-held read subtracts every
ram-host holder by prefix rather than a handed-in list, so a hold under
any name the loop mints is still accounted.

The take itself is tri-state: taken, short, or unknown -- an unreadable
host ledger is named with its error and deferred, never mistaken for a
free pool (#1245 review B3).
"""
from __future__ import annotations

import ast
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prismabuild.pool as pool  # noqa: E402
import prismabuild.storage_tiers as storage_tiers  # noqa: E402

HOST = "dl380g10"
RAM_TIER = storage_tiers.tier_id("ram", HOST)


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    ledger = queue.ledger(HOST)
    ledger.ensure_capacity({"cpu": 80, "mem_gb": 256})
    queue.mint_tier_capacity(RAM_TIER, {"ram_gib": 300})
    return queue


def _mover_row(queue: pool.PoolQueue, tmp_path: Path, key: str) -> None:
    queue.publish(
        action_key=key, cas_root=tmp_path / "cas",
        checkout_root=tmp_path / "checkout",
        worker_script=tmp_path / "worker.py", tags=["x86"],
        resources={"cpu": 1, "mem_gb": 2})


def _fill_to_mover(queue: pool.PoolQueue, tmp_path: Path,
                   grant: str, mover: str, gib: int) -> None:
    """Reserve a fence then hand it to the mover: both pools move."""
    state, _detail = queue.take_tier_advance(RAM_TIER, grant, gib, "ram_gib")
    assert state == "taken", "fixture: the advance must be taken to start"
    moved = queue.transfer_fence(RAM_TIER, grant, mover)
    assert moved == gib, "fixture: the whole fence moves onto the mover"


def test_the_host_hold_follows_the_fence_through_its_whole_life(
        tmp_path: Path) -> None:
    """Reserve, transfer, terminal cancel: both ledgers move together."""

    queue = _queue(tmp_path)
    tier = queue.tier_ledger(RAM_TIER)
    host = queue.ledger(HOST)
    grant, mover = "a" * 64, "b" * 64
    _mover_row(queue, tmp_path, mover)
    # A row holds 48 under its own action key beside the whole story.
    assert host.acquire("c" * 64, {"mem_gb": 48}) is True

    state, detail = queue.take_tier_advance(RAM_TIER, grant, 60, "ram_gib")
    assert state == "taken", detail
    assert tier.holder_tokens(grant) == {"ram_gib": 60}
    assert host.holder_tokens("ram-host:" + grant) == {"mem_gb": 60}
    assert queue.rows_host_memory_held(HOST) == 48

    assert queue.transfer_fence(RAM_TIER, grant, mover) == 60
    assert tier.holder_tokens(grant) == {}
    assert tier.holder_tokens(mover) == {"ram_gib": 60}
    assert host.holder_tokens("ram-host:" + grant) == {}
    assert host.holder_tokens("ram-host:" + mover) == {"mem_gb": 60}
    assert queue.rows_host_memory_held(HOST) == 48

    outcome = queue.cancel_tier_fence(RAM_TIER, mover)
    assert outcome["released"] == 60
    # held() drops zero-total kinds: nothing held is an empty answer.
    assert tier.held().get("ram_gib", 0) == 0
    assert host.holder_tokens("ram-host:" + mover) == {}
    assert queue.rows_host_memory_held(HOST) == 48


def test_egress_returns_the_host_hold_with_the_tokens(tmp_path: Path) -> None:
    """The need-gone-final route out: evict and egress settle the same way.

    ``stage_release.evict`` settles through ``release_tier_holder_for_
    egress``, so the queue-level route covers both callers.
    """

    queue = _queue(tmp_path)
    tier = queue.tier_ledger(RAM_TIER)
    host = queue.ledger(HOST)
    grant, mover = "d" * 64, "e" * 64
    _mover_row(queue, tmp_path, mover)
    assert host.acquire("f" * 64, {"mem_gb": 48}) is True
    _fill_to_mover(queue, tmp_path, grant, mover, 60)

    queue.release_tier_holder_for_egress(
        RAM_TIER, mover, destroy={}, free={"ram_gib": 60})
    assert tier.holder_tokens(mover) == {}
    assert host.holder_tokens("ram-host:" + mover) == {}
    assert queue.rows_host_memory_held(HOST) == 48


def test_orphan_host_holds_are_reconciled_by_name(tmp_path: Path) -> None:
    """A crash between the host take and the tier take is healed next cycle.

    Only holds whose holder is neither live in the tier ledger nor an
    expected in-flight grant are released; the event names the holder and
    the amount so the heal is observable.
    """

    queue = _queue(tmp_path)
    host = queue.ledger(HOST)
    assert host.acquire("c" * 64, {"mem_gb": 48}) is True
    # The crash shape: the host half of an advance landed, the tier half
    # never did.
    state, _detail = queue.hold_tier_host_memory(HOST, "a" * 64, 30)
    assert state == "taken"
    assert queue.rows_host_memory_held(HOST) == 48

    events = queue.reconcile_ram_host_holds(RAM_TIER, expected_holders=set())
    assert len(events) == 1, events
    assert events[0]["reason"] == "orphan_ram_host_hold"
    assert events[0]["holder"] == "ram-host:" + "a" * 64
    assert events[0]["released_gib"] == 30
    assert host.holder_tokens("ram-host:" + "a" * 64) == {}
    assert queue.rows_host_memory_held(HOST) == 48

    # A live advance and an expected in-flight grant are both left alone.
    state, _ = queue.take_tier_advance(RAM_TIER, "b" * 64, 20, "ram_gib")
    assert state == "taken"
    events = queue.reconcile_ram_host_holds(
        RAM_TIER, expected_holders={"9" * 64})
    assert events == []
    assert host.holder_tokens("ram-host:" + "b" * 64) == {"mem_gb": 20}


def test_the_take_is_tristate_and_names_the_unknown_case(
        tmp_path: Path, monkeypatch) -> None:
    """taken / short / unknown; unknown carries the error that made it."""

    queue = _queue(tmp_path)
    # A tight tier beside a roomy host: the tier ledger is the one that
    # can be short while the host still fits the same take.
    queue.mint_tier_capacity(RAM_TIER, {"ram_gib": 60})
    host = queue.ledger(HOST)
    assert host.acquire("c" * 64, {"mem_gb": 48}) is True
    state, detail = queue.hold_tier_host_memory(HOST, "d" * 64, 40)
    assert state == "taken"
    assert detail == ""
    # 256 - 48 - 40 = 168 free: 240 does not fit, and that is short.
    state, detail = queue.hold_tier_host_memory(HOST, "e" * 64, 240)
    assert state == "short"
    # An unreadable host ledger is unknown, never a free pool.  The ledger
    # wrapper is constructed per call, so the refusal is installed on the
    # class -- exactly the census fault the take has to survive.
    def _boom(self, holder: str, demand: dict) -> bool:
        raise OSError("boom: host census unreadable")
    monkeypatch.setattr(pool.ResourceLedger, "acquire", _boom)
    state, detail = queue.hold_tier_host_memory(HOST, "f" * 64, 8)
    assert state == "unknown"
    assert "boom" in detail
    # The advance defers on the same tri-state, for either half failing.
    state, detail = queue.take_tier_advance(RAM_TIER, "1" * 64, 8, "ram_gib")
    assert state == "unknown"
    assert "boom" in detail
    # A tier-short advance is named short too, not unknown: the tier has
    # 60, 40 of it is held, and 25 more does not fit -- while the host
    # (48 + 40 + 25 <= 256) would have taken it.
    monkeypatch.undo()
    state, detail = queue.take_tier_advance(RAM_TIER, "2" * 64, 25, "ram_gib")
    assert state == "tier-short"
    # The host half of a tier-short take rolled back: the take holds
    # nothing on the host (only the earlier "d" hold and the row remain).
    assert host.holder_tokens("ram-host:" + "2" * 64) == {}
    # A host-short advance says which pool was short.
    state, detail = queue.take_tier_advance(RAM_TIER, "3" * 64, 200, "ram_gib")
    assert state == "host-short"


def test_the_rows_read_subtracts_every_ram_host_holder_by_prefix(
        tmp_path: Path) -> None:
    """No handed-in list: any ram-host:* name is accounted as a fill."""

    queue = _queue(tmp_path)
    host = queue.ledger(HOST)
    assert host.acquire("c" * 64, {"mem_gb": 48}) is True
    _mover_row(queue, tmp_path, "b" * 64)
    state, _ = queue.take_tier_advance(RAM_TIER, "a" * 64, 40, "ram_gib")
    assert state == "taken"
    state, _ = queue.take_tier_advance(RAM_TIER, "d" * 64, 60, "ram_gib")
    assert state == "taken"
    assert queue.transfer_fence(RAM_TIER, "d" * 64, "b" * 64) == 60
    state, _ = queue.hold_tier_host_memory(HOST, "e" * 64, 30)
    assert state == "taken"
    # Fills under grant, mover and orphan names all subtract; rows stay 48.
    assert queue.rows_host_memory_held(HOST) == 48
    queue.reconcile_ram_host_holds(RAM_TIER, expected_holders=set())
    assert queue.rows_host_memory_held(HOST) == 48


_TIER_MUTATIONS = {"acquire", "release", "transfer", "transfer_tokens",
                   "retire_held", "release_count"}


def test_ram_tier_mutations_go_through_the_pool_primitive() -> None:
    """Shrink gate: tier_loop and stage_release never touch the ledgers.

    Every RAM-tier acquire/transfer/release routes through the PoolQueue
    primitives (take_tier_advance, transfer_fence, cancel_tier_fence,
    release_tier_holder, release_tier_holder_for_egress, reconcile_ram_
    host_holds), so the host tokens cannot drift from the tier tokens at
    a call site the review has not seen.  Parsing the source keeps the
    gate honest against future edits.
    """

    root = Path(__file__).resolve().parents[1]
    for name in ("tools/fleet/tier_loop.py", "tools/fleet/stage_release.py"):
        tree = ast.parse((root / name).read_text(), filename=name)
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)):
                continue
            base = ast.unparse(node.func.value)
            attr = node.func.attr
            if attr in _TIER_MUTATIONS and (
                    base == "ledger" or base.endswith(".ledger")
                    or "tier_ledger" in base):
                raise AssertionError(
                    f"{name}: {base}.{attr}() outside the pool primitive")
            if attr in ("cancel",) and "window_credit" in base:
                raise AssertionError(
                    f"{name}: window_credit.{attr}() outside "
                    "pool.cancel_tier_fence")
            if attr in ("hold_tier_host_memory", "release_tier_host_memory"):
                raise AssertionError(
                    f"{name}: {attr}() outside the pool primitive")
    # And the primitive exists to route through.
    source = (root / "src/prismabuild/pool.py").read_text()
    for primitive in ("take_tier_advance", "cancel_tier_fence",
                      "release_tier_holder", "reconcile_ram_host_holds"):
        assert f"def {primitive}(" in source, primitive

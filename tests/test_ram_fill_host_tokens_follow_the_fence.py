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
    queue.records.ensure_layout()
    ledger = queue.ledger(HOST)
    ledger.ensure_capacity({"cpu": 80, "mem_gb": 256})
    queue.tier(RAM_TIER).mint({'ram_gib': 300})
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
    state, _detail = queue.tier(RAM_TIER).take_advance(grant, gib, 'ram_gib')
    assert state == "taken", "fixture: the advance must be taken to start"
    moved = queue.transfer_fence(RAM_TIER, grant, mover)
    assert moved == gib, "fixture: the whole fence moves onto the mover"


def test_the_host_hold_follows_the_fence_through_its_whole_life(
        tmp_path: Path) -> None:
    """Reserve, transfer, terminal cancel: both ledgers move together."""

    queue = _queue(tmp_path)
    tier = queue.tier(RAM_TIER)
    host = queue.ledger(HOST)
    grant, mover = "a" * 64, "b" * 64
    _mover_row(queue, tmp_path, mover)
    # A row holds 48 under its own action key beside the whole story.
    assert host.acquire("c" * 64, {"mem_gb": 48}) is True

    state, detail = queue.tier(RAM_TIER).take_advance(grant, 60, 'ram_gib')
    assert state == "taken", detail
    assert tier.holdings(grant) == {'ram_gib': 60}
    assert host.holder_tokens("ram-host:" + grant) == {"mem_gb": 60}
    assert queue.rows_host_memory_held(HOST) == 48

    assert queue.transfer_fence(RAM_TIER, grant, mover) == 60
    assert tier.holdings(grant) == {}
    assert tier.holdings(mover) == {'ram_gib': 60}
    assert host.holder_tokens("ram-host:" + grant) == {}
    assert host.holder_tokens("ram-host:" + mover) == {"mem_gb": 60}
    assert queue.rows_host_memory_held(HOST) == 48

    outcome = queue.tier(RAM_TIER).cancel_fence(mover)
    assert outcome["released"] == 60
    # held() drops zero-total kinds: nothing held is an empty answer.
    assert tier.holdings().get('ram_gib', 0) == 0
    assert host.holder_tokens("ram-host:" + mover) == {}
    assert queue.rows_host_memory_held(HOST) == 48


def test_egress_returns_the_host_hold_with_the_tokens(tmp_path: Path) -> None:
    """The need-gone-final route out: evict and egress settle the same way.

    ``stage_release.evict`` settles through ``release_tier_holder_for_
    egress``, so the queue-level route covers both callers.
    """

    queue = _queue(tmp_path)
    tier = queue.tier(RAM_TIER)
    host = queue.ledger(HOST)
    grant, mover = "d" * 64, "e" * 64
    _mover_row(queue, tmp_path, mover)
    assert host.acquire("f" * 64, {"mem_gb": 48}) is True
    _fill_to_mover(queue, tmp_path, grant, mover, 60)

    queue.tier(RAM_TIER).settle_egress(mover, destroy={}, free={'ram_gib': 60})
    assert tier.holdings(mover) == {}
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
    state, _detail = queue.tier('ram:' + str(HOST))._hold_host('a' * 64, 30)
    assert state == "taken"
    assert queue.rows_host_memory_held(HOST) == 48

    events = queue.tier(RAM_TIER).reconcile_mirror(expected_holders=set())
    assert events["events"][0]["reason"] == "orphan_ram_host_hold"
    assert events["events"][0]["holder"] == "ram-host:" + "a" * 64
    assert events["events"][0]["released_gib"] == 30
    assert events["converged"] is True
    assert host.holder_tokens("ram-host:" + "a" * 64) == {}
    assert queue.rows_host_memory_held(HOST) == 48

    # A live advance and an expected in-flight grant are both left alone.
    state, _ = queue.tier(RAM_TIER).take_advance('b' * 64, 20, 'ram_gib')
    assert state == "taken"
    verdict = queue.tier(RAM_TIER).reconcile_mirror(expected_holders={'9' * 64})
    assert verdict["events"] == []
    assert verdict["converged"] is True
    assert host.holder_tokens("ram-host:" + "b" * 64) == {"mem_gb": 20}


def test_the_take_is_tristate_and_names_the_unknown_case(
        tmp_path: Path, monkeypatch) -> None:
    """taken / short / unknown; unknown carries the error that made it."""

    queue = _queue(tmp_path)
    # A tight tier beside a roomy host: the tier ledger is the one that
    # can be short while the host still fits the same take.
    queue.tier(RAM_TIER).mint({'ram_gib': 60})
    host = queue.ledger(HOST)
    assert host.acquire("c" * 64, {"mem_gb": 48}) is True
    state, detail = queue.tier('ram:' + str(HOST))._hold_host('d' * 64, 40)
    assert state == "taken"
    assert detail == ""
    # 256 - 48 - 40 = 168 free: 240 does not fit, and that is short.
    state, detail = queue.tier('ram:' + str(HOST))._hold_host('e' * 64, 240)
    assert state == "short"
    # An unreadable host ledger is unknown, never a free pool.  The ledger
    # wrapper is constructed per call, so the refusal is installed on the
    # class -- exactly the census fault the take has to survive.
    def _boom(self, holder: str, demand: dict) -> bool:
        raise OSError("boom: host census unreadable")
    monkeypatch.setattr(pool.ResourceLedger, "acquire", _boom)
    state, detail = queue.tier('ram:' + str(HOST))._hold_host('f' * 64, 8)
    assert state == "unknown"
    assert "boom" in detail
    # The advance defers on the same tri-state, for either half failing.
    state, detail = queue.tier(RAM_TIER).take_advance('1' * 64, 8, 'ram_gib')
    assert state == "unknown"
    assert "boom" in detail
    # A tier-short advance is named short too, not unknown.  The "d"
    # hold is host-side only, so the tier is filled by a real advance
    # first: 40 of its 60 taken, and 25 more does not fit -- while the
    # host (48 + 40 + 40 + 25 <= 256) would have taken it.
    monkeypatch.undo()
    state, detail = queue.tier(RAM_TIER).take_advance('g' * 64, 40, 'ram_gib')
    assert state == "taken"
    state, detail = queue.tier(RAM_TIER).take_advance('2' * 64, 25, 'ram_gib')
    assert state == "tier-short"
    # The host half of a tier-short take rolled back: the take holds
    # nothing on the host (only the earlier "d" hold and the row remain).
    assert host.holder_tokens("ram-host:" + "2" * 64) == {}
    # A host-short advance says which pool was short: the host would
    # need 88 + 40 + 200 > 256, and the tier (40 + 200) is short too,
    # but the host is the half that refuses first.
    state, detail = queue.tier(RAM_TIER).take_advance('3' * 64, 200, 'ram_gib')
    assert state == "host-short"


def test_the_rows_read_subtracts_every_ram_host_holder_by_prefix(
        tmp_path: Path) -> None:
    """No handed-in list: any ram-host:* name is accounted as a fill."""

    queue = _queue(tmp_path)
    host = queue.ledger(HOST)
    assert host.acquire("c" * 64, {"mem_gb": 48}) is True
    _mover_row(queue, tmp_path, "b" * 64)
    state, _ = queue.tier(RAM_TIER).take_advance('a' * 64, 40, 'ram_gib')
    assert state == "taken"
    state, _ = queue.tier(RAM_TIER).take_advance('d' * 64, 60, 'ram_gib')
    assert state == "taken"
    assert queue.transfer_fence(RAM_TIER, "d" * 64, "b" * 64) == 60
    state, _ = queue.tier('ram:' + str(HOST))._hold_host('e' * 64, 30)
    assert state == "taken"
    # Fills under grant, mover and orphan names all subtract; rows stay 48.
    assert queue.rows_host_memory_held(HOST) == 48
    queue.tier(RAM_TIER).reconcile_mirror(expected_holders=set())
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
            if attr in _TIER_MUTATIONS and (base == 'ledger' or base.endswith('.ledger') or 'tier' in base):
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
    source = (root / "src/prismabuild/tier_holdings.py").read_text()
    for primitive in ("take_advance", "cancel_fence",
                      "release_unspent", "reconcile_mirror"):
        assert f"def {primitive}(" in source, primitive

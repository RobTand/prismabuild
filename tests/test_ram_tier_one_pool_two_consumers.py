"""#1222: one pool of host memory, two consumers -- rows and RAM fills.

The old contract subtracted the *announced* worker demand from the RAM
tier's budget (``allowed_window = allowed - worker_demand``).  With the
measured offer that announce is the roof itself, so the subtraction both
starves the tier behind an offer nobody is using and double-counts once
tier fills hold tokens in the same host ledger.  The approved replacement
is one pool: the announce is the roof, rows and tier fills acquire from
the same host ledger, and the window gate checks what is actually held
beside it -- ``window + rows_held + max(c_max, arc_floor) + reserve <=
MemTotal``.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prismabuild.pool as pool  # noqa: E402
import prismabuild.storage_tiers as storage_tiers  # noqa: E402

GIB = storage_tiers.GIB
HOST = "dl380g10"
RAM_TIER = storage_tiers.tier_id("ram", HOST)


def _no_zfs(argv: list[str]) -> str:
    raise OSError(f"no {argv[0]} on this box")


def _ram_tier(tmp_path: Path, *, window_gib: int, rows_held_gib: int | None,
              memtotal_gib: float = 294.522, arc_c_max_gib: int = 22):
    mount = tmp_path / "ram"
    mount.mkdir(exist_ok=True)
    proc = tmp_path / "proc"
    proc.mkdir(exist_ok=True)
    (proc / "mounts").write_text(
        f"tmpfs {mount} tmpfs rw,relatime,noswap,size=256G,mpol=interleave:0-3 0 0\n")
    (proc / "meminfo").write_text(
        f"MemTotal:  {int(memtotal_gib * GIB) // 1024} kB\n")
    (proc / "arcstats").write_text(
        f"c_max 4 {arc_c_max_gib * GIB}\nsize 4 {arc_c_max_gib * GIB // 2}\n"
        f"arc_meta_used 4 {5 * GIB}\n")

    def statvfs(path: str):
        if path != str(mount):
            raise OSError(f"no tmpfs at {path}")
        frsize = 4096
        block = GIB // frsize
        return os.statvfs_result(
            (frsize, frsize, 256 * block, 256 * block,
             256 * block, 1_000_000, 900_000, 900_000, 0, 255))

    tiers = storage_tiers.discover_tiers(
        host=HOST, runner=_no_zfs, arcstats_path=str(proc / "arcstats"),
        ram_policy={
            "schema": storage_tiers.RAM_TIER_POLICY_SCHEMA_V1,
            "mountpoint": str(mount), "ceiling_gib_max": 256,
            "window_gib_default": window_gib, "arc_floor_gib": 20,
            "system_reserve_gib": 16, "prefill_depth": None,
        },
        statvfs=statvfs, proc_mounts=str(proc / "mounts"),
        meminfo_path=str(proc / "meminfo"), rows_held_gib=rows_held_gib)
    return tiers[RAM_TIER]


def test_an_empty_tier_with_no_rows_grows_to_the_roof(tmp_path: Path) -> None:
    """Nobody holds anything: the window may be the whole roof."""

    tier = _ram_tier(tmp_path, window_gib=256, rows_held_gib=0)
    admission = tier["ram_admission"]
    assert admission["admissible"] is True, admission
    assert admission["rows_held_bytes"] == 0


def test_rows_holding_x_cap_the_window_at_roof_minus_x(tmp_path: Path) -> None:
    """48+40 GiB of rows: the window may grow only to roof - 88."""

    tier = _ram_tier(tmp_path, window_gib=200, rows_held_gib=88)
    admission = tier["ram_admission"]
    assert admission["admissible"] is False, admission
    assert admission["reason"] == "ram_window_exceeds_memtotal_floor"
    assert admission["rows_held_bytes"] == 88 * GIB
    # 200 + 88 > 294.522 - 22 - 16 = 256.5: named, not guessed.
    assert admission["window_bytes"] == 200 * GIB
    # And the symmetric case admits: a window that fits beside the rows.
    fitting = _ram_tier(tmp_path, window_gib=168, rows_held_gib=88)
    assert fitting["ram_admission"]["admissible"] is True


def test_unknown_rows_held_fails_closed(tmp_path: Path) -> None:
    """No host ledger verdict is not evidence of no rows: refuse."""

    tier = _ram_tier(tmp_path, window_gib=160, rows_held_gib=None)
    admission = tier["ram_admission"]
    assert admission["admissible"] is False, admission
    assert admission["reason"] == "ram_rows_held_unknown"


def test_fills_hold_host_tokens_and_rows_read_what_is_left(tmp_path):
    """The pool helpers: fills hold under a ram-prefixed name, releases
    return them, and the rows-held read subtracts exactly the fills."""

    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger(HOST)
    ledger.ensure_capacity({"cpu": 80, "mem_gb": 256})
    grant = "a" * 64

    # A tier fill takes 40 GiB of host tokens under the ram-prefixed name.
    assert queue.hold_tier_host_memory(HOST, grant, 40) is True
    # A row takes 48 GiB under its own action key.
    assert ledger.acquire("b" * 64, {"mem_gb": 48}) is True
    # Rows held = total mem held (88) minus the fill's 40.
    assert queue.rows_host_memory_held(HOST, [grant]) == 48
    # The budget is one pool: 256 - 88 = 168 free; 170 does not fit.
    assert queue.hold_tier_host_memory(HOST, "c" * 64, 170) is False
    # Evicting the fill returns its host tokens.
    assert queue.release_tier_host_memory(HOST, grant) == 40
    assert queue.hold_tier_host_memory(HOST, "c" * 64, 170) is True
    # c's 170 is a fill hold too: rows held is the total less every fill.
    assert queue.rows_host_memory_held(HOST, ["c" * 64]) == 48


def test_a_host_memory_shortfall_is_returned_by_release_not_eviction(
        tmp_path: Path) -> None:
    """A host-mem shortfall asks the sweep for nothing (#1222).

    Host tokens are held only by active fills and by rows, and both return
    them deterministically: the fill when its fence is cancelled at every
    cancel site, the row when it ends.  So unlike the #901 tier-fence
    shape there is no "withdrawn consumer's orphans" deadlock to break --
    eviction cannot return host tokens a landing fill still writes, and
    asking it to would be the futile eviction the pressure walk refuses.
    The relief is the release, and the row's claim is served the moment
    the fill's hold returns."""

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
    import tier_loop  # noqa: E402

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    ledger = queue.ledger(HOST)
    ledger.ensure_capacity({"cpu": 80, "mem_gb": 256})
    # A cold fill holds 250 of 256; an 8 GiB shard row needs host memory.
    grant = "d" * 64
    assert queue.hold_tier_host_memory(HOST, grant, 250) is True
    shard = "e" * 64
    queue.publish(
        action_key=shard, cas_root=tmp_path / "cas",
        checkout_root=tmp_path / "checkout",
        worker_script=tmp_path / "worker.py", tags=["x86"],
        resources={"cpu": 2, "mem_gb": 8})
    # The shortfall is real -- the row cannot take 8 from 6 free...
    assert ledger.acquire(shard, {"mem_gb": 8}) is False
    # ...but it asks the sweep for nothing: eviction returns no host token.
    need = tier_loop.window_pressure(
        queue, tiers={RAM_TIER: {"tier_id": RAM_TIER}}, consumers=[])
    assert need.get(RAM_TIER, 0) == 0
    # The release is the relief: the fence cancels, the hold returns, and
    # the same claim takes its tokens and succeeds.
    assert queue.release_tier_host_memory(HOST, grant) == 250
    assert ledger.acquire(shard, {"mem_gb": 8}) is True

"""#1222: the worker's memory offer is the measured RAM-tier roof, per poll.

dl380g10 announced a static ``--mem-gb 96`` carved at worker start while the
box actually had ~245 GiB available beside a 22 GiB ARC, so 2-CPU/8 GiB
shards waited 23-40 minutes on ``reservation_unavailable`` for memory the
box had.  The fix follows the ``--spool-gb auto`` precedent (#1190): the
declaration is a fallback, and the live number is measured per poll.

The roof is the same arithmetic the RAM tier's floor guard uses
(``storage_tiers._ram_numbers``): ``MemTotal - max(arc_c_max, arc_floor) -
system_reserve``.  Anything unreadable fails closed to the declared value,
because a roof nobody can prove is a roof nobody may offer.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prismabuild.box_capacity as box_capacity  # noqa: E402
import prismabuild.storage_tiers as storage_tiers  # noqa: E402

GIB = storage_tiers.GIB
MEMTOTAL_KB = int(294.522 * GIB) // 1024  # 294.522 GiB, dl380g10's MemTotal
ARC_C_MAX = 22 * GIB


def _inputs(tmp_path: Path, *, c_max: int = ARC_C_MAX, arc_floor: int = 20,
            reserve: int = 16) -> tuple[Path, Path, Path]:
    proc = tmp_path / "proc"
    proc.mkdir(exist_ok=True)
    (proc / "meminfo").write_text(f"MemTotal:  {MEMTOTAL_KB} kB\n")
    (proc / "arcstats").write_text(
        f"c_max 4 {c_max}\nsize 4 {c_max // 2}\narc_meta_used 4 {5 * GIB}\n")
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({
        "schema": storage_tiers.RAM_TIER_POLICY_SCHEMA_V1,
        "mountpoint": "/ram/prewarm", "ceiling_gib_max": 256,
        "window_gib_default": 160, "arc_floor_gib": arc_floor,
        "system_reserve_gib": reserve, "prefill_depth": None,
    }))
    return policy, proc / "arcstats", proc / "meminfo"


def test_roof_is_the_floor_guard_arithmetic(tmp_path: Path) -> None:
    """MemTotal - max(c_max, arc_floor) - reserve, floored to whole GiB."""

    policy, arcstats, meminfo = _inputs(tmp_path)
    roof = box_capacity.ram_policy_mem_roof(
        policy_path=policy, arcstats_path=arcstats, meminfo_path=meminfo)
    # 294.522 - 22 - 16 = 256.522 -> 256 whole GiB.
    assert roof == 256

    numbers = storage_tiers._ram_numbers(
        ceiling_bytes=0, mem_total=MEMTOTAL_KB * 1024,
        arc={"c_max": ARC_C_MAX}, policy=json.loads(policy.read_text()))
    assert roof == numbers["allowed_ceiling_bytes"] // GIB


def test_the_floor_wins_when_the_arc_was_shrunk_below_it(tmp_path: Path) -> None:
    """max(c_max, arc_floor): a shrunken ARC does not buy capacity."""

    policy, arcstats, meminfo = _inputs(tmp_path, c_max=8 * GIB)
    roof = box_capacity.ram_policy_mem_roof(
        policy_path=policy, arcstats_path=arcstats, meminfo_path=meminfo)
    # 294.522 - max(8, 20) - 16 = 258.5 -> 258.
    assert roof == 258


def test_unreadable_inputs_fail_closed_to_none(tmp_path: Path) -> None:
    """No arcstats, no policy, no meminfo: no roof, keep the declaration."""

    policy, arcstats, meminfo = _inputs(tmp_path)
    gone = tmp_path / "gone"
    assert box_capacity.ram_policy_mem_roof(
        policy_path=gone, arcstats_path=arcstats, meminfo_path=meminfo) is None
    assert box_capacity.ram_policy_mem_roof(
        policy_path=policy, arcstats_path=gone, meminfo_path=meminfo) is None
    assert box_capacity.ram_policy_mem_roof(
        policy_path=policy, arcstats_path=arcstats, meminfo_path=gone) is None
    broken = tmp_path / "broken.json"
    broken.write_text("{not json")
    assert box_capacity.ram_policy_mem_roof(
        policy_path=broken, arcstats_path=arcstats, meminfo_path=meminfo) is None


def test_the_observer_offers_the_roof_and_rereads_it_every_poll(
        tmp_path: Path) -> None:
    """The roof replaces the declaration per poll; None keeps it."""

    policy, arcstats, meminfo = _inputs(tmp_path)
    roofs = iter([256, 256, 96, None])
    observer = box_capacity.CapacityObserver(
        samples=1, mem_roof=lambda: next(roofs))
    first = observer.offer({"mem_gb": 96, "cpu": 80}, held={},
                           mem_gb=245, load1=0.0)
    assert first["mem_gb"] == 237  # min(256, 0 + 245 - 8): honest clamp works.
    second = observer.offer({"mem_gb": 96, "cpu": 80}, held={},
                            mem_gb=245, load1=0.0)
    assert second["mem_gb"] == 237
    third = observer.offer({"mem_gb": 96, "cpu": 80}, held={},
                            mem_gb=245, load1=0.0)
    assert third["mem_gb"] == 96  # a smaller roof binds while it is the roof.

    # With no roof the declaration binds again, and MemAvailable is ample.
    fourth = observer.offer({"mem_gb": 96, "cpu": 80}, held={},
                            mem_gb=245, load1=0.0)
    assert fourth["mem_gb"] == 96


def test_the_measured_roof_lets_the_incident_shard_admit(tmp_path: Path) -> None:
    """The #1222 acceptance shape: the roof, not 96, is the offered budget."""

    policy, arcstats, meminfo = _inputs(tmp_path)
    observer = box_capacity.CapacityObserver(
        samples=1, mem_roof=lambda: 256)
    # Two rows hold 48+40; the tier holds none.  Available is 245, so the
    # honest offer is min(256, 88 + 245 - 8) = 256 today; what matters is
    # that the budget the ledger sees is 256, never 96.
    held = {"mem_gb": 88}
    offered = observer.offer({"mem_gb": 96, "cpu": 80}, held=held,
                             mem_gb=245, load1=0.0)
    assert offered["mem_gb"] >= 237

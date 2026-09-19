"""Mover receipts are keyed on pool identity: an older pool must not price the next mover (#611).

Follow-up from the #609 review.  ``mover_demand_from_receipts``,
``mover_fill_demand_from_receipts`` and ``fill_supply_from_records`` fold over
every usable receipt in ``pb-queue/movers/`` for a tier id.  Nothing on the
receipt says which pool it measured: after a resilver, a member swap, a vdev
added, or the stage dataset being recreated (the stage pool was rebuilt on
2026-09-18 01:47Z), the old receipts still price cpu, mem_gb, the fill share
and the ceiling for the new pool.

The fix keys receipts by pool identity: ``zpool status`` fields (guid, state,
scan) recorded on the tier record and copied into each receipt, with the fold
restricted to receipts whose identity equals the tier's current one.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, storage_tiers  # noqa: E402
import prismabuild.core as pb  # noqa: E402
import stage_move  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
GUID = "1234567890123456789"
SOURCE_GUID = "9999999999999999999"
GIB = storage_tiers.GIB

STATUS = """\
  pool: prismabuild-stage
 state: ONLINE
  scan: scrub repaired 0B in 00:02:13 with 0 errors on Sun Sep 13 01:02:03 2026
config:

\tNAME                          STATE     READ WRITE CKSUM
\tprismabuild-stage              ONLINE       0     0     0
\t  mirror-0                    ONLINE       0     0     0
\t    /dev/disk/by-id/nvme-AAA  ONLINE       0     0     0
\t    /dev/disk/by-id/nvme-BBB  ONLINE       0     0     0

errors: No known data errors
"""

RESILVER_EARLY = STATUS.replace(
    "scrub repaired 0B in 00:02:13 with 0 errors on Sun Sep 13 01:02:03 2026",
    "resilver in progress since Fri Sep 18 01:47:11 2026, 12.5% done, 4h12m to go")
RESILVER_LATE = STATUS.replace(
    "scrub repaired 0B in 00:02:13 with 0 errors on Sun Sep 13 01:02:03 2026",
    "resilver in progress since Fri Sep 18 01:47:11 2026, 87.5% done, 0h22m to go")
RESILVER_DONE = STATUS.replace(
    "scrub repaired 0B in 00:02:13 with 0 errors on Sun Sep 13 01:02:03 2026",
    "resilvered 745G in 04:12:33 with 0 errors on Fri Sep 18 06:01:44 2026")


def _runner(*, guid: str | None = GUID, status: str | None = STATUS,
            fail: tuple[str, ...] = (),
            guids: dict[str, str] | None = None):
    """A zpool that answers get/status/list from canned text."""

    def run(argv: list[str]) -> str:
        text = " ".join(argv)
        if any(marker in text for marker in fail):
            raise OSError("no zpool on this box")
        binary = argv[0].rsplit("/", 1)[-1]
        verb = argv[1]
        if binary == "zpool" and verb == "get":
            name = argv[-1]
            value = (guids or {}).get(name, guid)
            if value is None:
                raise OSError(f"no such pool: {name}")
            return f"{value}\n"
        if binary == "zpool" and verb == "status":
            if status is None:
                raise OSError(f"no such pool: {argv[-1]}")
            return status
        if binary == "zpool" and verb == "list":
            return ("prismabuild-stage\t100000000000\t1000\t99999999000\tONLINE\n"
                    "storage_pool\t8000000000000\t1000\t7999999999000\tONLINE\n")
        if binary == "zfs" and verb == "list":
            return ("prismabuild-stage/prewarm\t99999999000\t"
                    "/stage/prewarm\tall\n")
        if binary == "zfs" and verb == "get":
            return "/stage/prewarm\n"
        raise AssertionError(f"unexpected zpool call: {argv}")

    return run


def test_pool_identity_names_guid_state_scan_and_members() -> None:
    identity = storage_tiers.pool_identity(
        "prismabuild-stage", runner=_runner())

    assert identity["pool"] == "prismabuild-stage"
    assert identity["guid"] == GUID
    assert identity["state"] == "ONLINE"
    assert identity["scan"] == "scrub-done"
    assert identity["members"] == [
        "/dev/disk/by-id/nvme-AAA", "/dev/disk/by-id/nvme-BBB"]


def test_pool_identity_scan_is_coarse_not_a_progress_line() -> None:
    """A raw scan line carries percentages and dates that change every cycle.

    Keying the fold on it would split every receipt into its own topology and
    price nothing ever again, so only the coarse state is compared.
    """

    first = storage_tiers.pool_identity(
        "prismabuild-stage", runner=_runner(status=RESILVER_EARLY))
    second = storage_tiers.pool_identity(
        "prismabuild-stage", runner=_runner(status=RESILVER_LATE))

    assert first["scan"] == "resilver-in-progress"
    assert first == second


def test_pool_identity_scan_completion_is_a_new_topology() -> None:
    before = storage_tiers.pool_identity(
        "prismabuild-stage", runner=_runner(status=RESILVER_EARLY))
    after = storage_tiers.pool_identity(
        "prismabuild-stage", runner=_runner(status=RESILVER_DONE))

    assert before != after


def test_pool_identity_unreadable_pool_is_named_not_mistaken() -> None:
    """A pool zpool will not read is still an identity, with nothing in it.

    Always a dict, never ``None``: callers compare it, and ``None`` would read
    as "no pool here" -- which is the one answer that must never compare
    equal to a pool that is there.
    """

    identity = storage_tiers.pool_identity(
        "prismabuild-stage", runner=_runner(guid=None, status=None,
                                            fail=("status -P",)))

    assert isinstance(identity, dict)
    assert identity["pool"] == "prismabuild-stage"
    assert identity != storage_tiers.pool_identity(
        "prismabuild-stage", runner=_runner())


def _receipt(key: str, identity: dict[str, object] | None, *,
             cpu_seconds: float = 20.0, rss: int = 2 * GIB,
             rate: float = 100.0, delivered: float = 200.0, sharers: int = 2,
             unix: float = 1000.0) -> dict[str, object]:
    record: dict[str, object] = {
        "schema": pool.POOL_MOVE_SCHEMA_V1, "action_key": key,
        "tier_id": TIER, "seconds": 10.0, "unix": unix,
        "cpu_seconds": cpu_seconds, "peak_rss_bytes": rss,
        "mb_per_s_file_side": rate,
        "disk_pacing": {storage_tiers.POOL_FILL_FIELD: delivered},
        storage_tiers.MOVER_CONCURRENCY_FIELD: sharers,
        storage_tiers.MOVER_FILL_DEMAND_FIELD: 50,
    }
    if identity is not None:
        record["pool_identity"] = identity
    return record


def _identity(runner=None, **overrides) -> dict[str, object]:
    now = {"stage": storage_tiers.pool_identity(
        "prismabuild-stage", runner=runner or _runner()),
        "source": {"pool": "storage_pool", "guid": SOURCE_GUID,
                   "state": "ONLINE", "scan": "scrub-done", "members": []}}
    now.update(overrides)
    return now


def test_usable_receipts_with_identity_drop_the_old_topology() -> None:
    now = _identity()
    records = [
        _receipt("a" * 64, now),
        # A resilver ago: same tier, another pool.
        _receipt("b" * 64, _identity(runner=_runner(guid="0" * 19))),
        # Before identities existed at all.
        _receipt("c" * 64, None),
    ]

    usable = storage_tiers.usable_mover_receipts(
        records, tier_id=TIER, pool_identity=now)

    assert [record["action_key"] for record in usable] == ["a" * 64]


def test_usable_receipts_without_identity_read_everything() -> None:
    """No gating identity means no gate: a tier announced by an older
    generation prices off every usable receipt, exactly as before."""

    records = [_receipt("a" * 64, _identity()), _receipt("b" * 64, None)]

    usable = storage_tiers.usable_mover_receipts(records, tier_id=TIER)

    assert {record["action_key"] for record in usable} == {"a" * 64, "b" * 64}


def test_cpu_and_mem_price_only_the_current_topology() -> None:
    now = _identity()
    old = _identity(runner=_runner(guid="0" * 19))
    records = [
        _receipt("a" * 64, old, cpu_seconds=320.0, rss=32 * GIB),
        _receipt("b" * 64, now, cpu_seconds=20.0, rss=2 * GIB),
    ]

    priced = storage_tiers.mover_demand_from_receipts(
        records, tier_id=TIER, readers=4, fallback_mem_gb=8,
        pool_identity=now)

    assert priced["cpu"] == 2
    assert priced["mem_gb"] == 2
    assert priced["demand_source"]["pool_identity"] == now
    assert priced["demand_source"]["cpu_receipts"] == ["b" * 64]


def test_fill_demand_and_supply_price_only_the_current_topology() -> None:
    now = _identity()
    old = _identity(runner=_runner(guid="0" * 19))
    records = [
        _receipt("a" * 64, old, rate=900.0, delivered=1800.0, sharers=2),
        _receipt("b" * 64, now, rate=100.0, delivered=200.0, sharers=2),
    ]

    assert storage_tiers.mover_fill_demand_from_receipts(
        records, tier_id=TIER, pool_identity=now) == 100
    supply = storage_tiers.fill_supply_from_records(
        records, pool_identity=now)
    assert supply["best_mb_s"] == 200.0


def test_fill_folds_still_read_tierless_prewarm_records() -> None:
    """Prewarm records carry no tier and no identity; they are reads, not movers.

    Keying them is a separate change (they are stamped by another role); the
    mover gate must not silence the pool's other measurement while it waits
    for it.  A prewarm record may not *rebuild* ``best`` from nothing -- the
    #654 act-three reader gate fenced that -- so this seeds a valid
    current-pool reader first and then shows the tierless record still raises
    the standing best, which is the identity gate this test is about.
    """

    now = _identity()
    prewarm = {"schema": pool.POOL_PREWARM_SCHEMA_V1,
               "disk_pacing": {storage_tiers.POOL_FILL_FIELD: 150.0},
               "unix": 1001.0}
    reader = _receipt("a" * 64, now, delivered=120.0, unix=1000.0)

    supply = storage_tiers.fill_supply_from_records(
        [reader, prewarm], pool_identity=now)

    assert supply["best_mb_s"] == 150.0
    # The other half of the same contract: with no reader to stand on, the
    # tierless observation still cannot bootstrap a best from none.
    assert storage_tiers.fill_supply_from_records(
        [prewarm], pool_identity=now)["best_mb_s"] is None


def test_discover_tiers_stamps_stage_and_source_identity(tmp_path: Path) -> None:
    tiers = storage_tiers.discover_tiers(
        host="dl380g10", runner=_runner(), arcstats_path="/nonexistent",
        source_pool="storage_pool")

    tier = tiers[storage_tiers.tier_id("stage", "dl380g10", "prismabuild-stage")]
    identity = tier["pool_identity"]
    assert isinstance(identity, dict)
    assert identity["stage"] == storage_tiers.pool_identity(
        "prismabuild-stage", runner=_runner())
    assert identity["source"] == storage_tiers.pool_identity(
        "storage_pool", runner=_runner())


def _move_args(tmp_path: Path, manifest_path: Path, stage: Path):
    manifest = json.loads(manifest_path.read_text())
    total = sum(int(entry["bytes"]) for entry in manifest["entries"])
    args = stage_move.build_parser().parse_args([
        "--pool-root", str(tmp_path / "queue"),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", "a" * 64,
        "--consumer-action-key", "c" * 64,
        "--tier-id", TIER,
        "--stage-root", str(stage),
        "--manifest-sha256", "9" * 64,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(total),
        "--manifest", str(manifest_path),
        "--residency-root", str(tmp_path / "queue" / pool.RESIDENCY),
        "--unpaced",
    ])
    return args


def _manifest(tmp_path: Path, name: str = "manifest.json") -> Path:
    mount = tmp_path / "mnt"
    payload = b"x" * 4096
    target = mount / "shard-0.bin"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)
    manifest = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "test"},
        "mount_prefix": str(mount),
        "entries": [{"path": str(target), "offset": 0, "bytes": len(payload),
                     "sha256": hashlib.sha256(payload).hexdigest()}],
        "entry_count": 1,
        "total_bytes": len(payload),
        "annotations": {},
    }
    path = tmp_path / name
    path.write_text(json.dumps(manifest))
    return path


def test_a_mover_receipt_carries_the_announced_pool_identity(
        tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier": "stage",
        "tier_id": TIER, "host": "dl380g10",
        "pool_identity": _identity()})
    stage = tmp_path / "stage"
    stage.mkdir()

    receipt = stage_move.move(_move_args(tmp_path, _manifest(tmp_path), stage))

    assert receipt["complete"] is True
    assert receipt["pool_identity"] == _identity()


def test_a_mover_receipt_without_an_announced_identity_carries_none(
        tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()

    receipt = stage_move.move(_move_args(tmp_path, _manifest(tmp_path), stage))

    assert receipt["complete"] is True
    assert "pool_identity" not in receipt

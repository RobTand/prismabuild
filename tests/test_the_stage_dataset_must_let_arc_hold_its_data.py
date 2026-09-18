"""The stage dataset's ``primarycache`` is a tier fact, and a refusable one (#638).

Layer 2 of the cache is the file server's own ARC holding the stage blocks a
Spark reads over NFS.  ``primarycache=metadata`` on the stage dataset forbids
exactly that: ZFS caches the dataset's metadata and never its file data, so
every consumer read of a staged file goes to the SSD.  Measured on dl380g10 on
2026-09-18, sparky reading one 5.37 GB file with ``dd iflag=direct``, 16
streams of 256 MiB at matched concurrency: 2402 MB/s on an ARC miss against
10045 MB/s on an ARC hit, 4.18x.

So the setting is discovered with the rest of the tier -- never assumed, never
configured from here -- and a stage dataset that forbids data caching is
announced with the refusal on its record and a loud event beside it.  The tier
is still announced: layer 1 works without layer 2, and refusing to announce it
would take staging down to fix a cache.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

POOL = "prismabuild-stage"
TIER = f"{POOL}:dl380g10"
SIZE = 798863917056
AVAILABLE = 774166085632


def _runner(primarycache: str | None):
    """A box whose ``zfs list`` answers the column discovery now asks for.

    ``None`` is the older box that answers three columns, which is what every
    fixture written before this change returns: unknown, not permitted.
    """

    def run(argv: list[str]) -> str:
        if "zpool" in argv[0] and "list" in argv:
            return f"{POOL}\t{SIZE}\t1048576\t{SIZE - 1048576}\tONLINE\n"
        if "zpool" in argv[0] and "status" in argv:
            return ""
        if "list" in argv and "-r" in argv:
            tail = "" if primarycache is None else f"\t{primarycache}"
            return (f"{POOL}\t{AVAILABLE}\t/{POOL}{tail}\n"
                    f"{POOL}/prewarm\t{AVAILABLE}\t/stage/prewarm{tail}\n")
        if "get" in argv:
            return f"/{POOL}\n"
        return ""

    return run


def _tier(primarycache: str | None) -> dict[str, object]:
    tiers = storage_tiers.discover_tiers(
        host="dl380g10", runner=_runner(primarycache),
        arcstats_path="/nonexistent")
    return tiers[TIER]


def test_discovery_reads_the_datasets_primarycache() -> None:
    assert _tier("all")["primarycache"] == "all"
    assert _tier("metadata")["primarycache"] == "metadata"


def test_a_dataset_that_caches_its_data_may_hold_an_arc_warm() -> None:
    verdict = storage_tiers.stage_arc_eligibility(_tier("all"))

    assert verdict["eligible"] is True
    assert verdict["primarycache"] == "all"


def test_a_metadata_only_dataset_is_refused_the_warm() -> None:
    verdict = storage_tiers.stage_arc_eligibility(_tier("metadata"))

    assert verdict["eligible"] is False
    assert "primarycache" in verdict["reason"]


def test_an_unread_primarycache_is_not_a_permission() -> None:
    """Unknown is not ``all``.  A warm spends reads; an unattested setting
    does not license them."""

    verdict = storage_tiers.stage_arc_eligibility(_tier(None))

    assert verdict["eligible"] is False
    assert verdict["primarycache"] is None


def _cycle(tmp_path: Path, primarycache: str | None, capsys):
    """One tier cycle over a driver that states the setting under test."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()

    def discover(**_kwargs):
        record = {
            "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
            "tier_id": TIER, "host": "dl380g10", "tier": "stage",
            "mountpoint": str(tmp_path / "stage"),
            "capacity_bytes": 4 * storage_tiers.GIB,
        }
        if primarycache is not None:
            record["primarycache"] = primarycache
        return {TIER: record}

    records = tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                              receipts=tier_loop.ReceiptCache(), discover=discover)
    events = []
    for line in capsys.readouterr().out.splitlines():
        try:
            events.append(json.loads(line))
        except ValueError:
            continue
    return records[0], events


def test_the_loop_refuses_the_warm_on_a_metadata_only_stage(tmp_path: Path, capsys) -> None:
    """The driver states ``metadata``; the announcement and the log both say so."""

    record, events = _cycle(tmp_path, "metadata", capsys)

    assert record["arc_warm"]["eligible"] is False
    assert record["arc_warm"]["primarycache"] == "metadata"
    refusals = [e for e in events if e.get("event") == "stage-primarycache-refused"]
    assert refusals, "a stage that cannot cache its data was announced quietly"
    assert refusals[0]["tier_id"] == TIER
    assert refusals[0]["primarycache"] == "metadata"


def test_the_loop_announces_a_cacheable_stage_without_complaint(tmp_path: Path, capsys) -> None:
    record, events = _cycle(tmp_path, "all", capsys)

    assert record["arc_warm"]["eligible"] is True
    assert not [e for e in events if e.get("event") == "stage-primarycache-refused"]

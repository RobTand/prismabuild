"""Capacity comes from what the dataset may write, not from the raw geometry.

``zpool list`` ``size`` is the raw capacity of the vdevs.  ``available`` on
the dataset is what ZFS will actually let a writer write: parity, the ~3% slop
reservation, quotas and whatever the pool's other datasets already hold are all
already taken out of it.  Minting tokens from ``size`` puts the slop reserve
*inside* the accounting as an overfill margin -- the ledger hands out tokens
for bytes the pool refuses at ENOSPC, and the mover that hits it has already
read them off the disks.  Minting from ``available`` puts it outside by
construction, and the record says which source answered.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prismabuild.storage_tiers as storage_tiers  # noqa: E402

POOL = "prismabuild-stage"
SIZE = 798863917056          # what zpool list -Hp size prints
AVAILABLE = 774166085632     # what the dataset says it may write


def _runner(*, datasets: str | None = None, fail: tuple[str, ...] = ()):
    """Stand in for the box's zfs/zpool, answering only what is asked."""

    def run(argv: list[str]) -> str:
        joined = " ".join(argv)
        for fragment in fail:
            if fragment in joined:
                raise OSError(f"no {fragment} here")
        if "zpool" in argv[0] and "list" in argv:
            return f"{POOL}\t{SIZE}\t1048576\t{SIZE - 1048576}\tONLINE\n"
        if "zpool" in argv[0] and "status" in argv:
            return ""
        if "list" in argv and "-r" in argv:
            if datasets is not None:
                return datasets
            return (f"{POOL}\t{AVAILABLE}\t/{POOL}\n"
                    f"{POOL}/prewarm\t{AVAILABLE}\t/stage/prewarm\n")
        if "get" in argv:
            return f"/{POOL}\n"
        return ""

    return run


def _stage_tier(**kwargs) -> dict[str, object]:
    tiers = storage_tiers.discover_tiers(
        host="dl380g10", runner=_runner(**kwargs), arcstats_path="/nonexistent")
    return tiers[storage_tiers.tier_id("stage", "dl380g10", POOL)]


def test_capacity_is_the_datasets_available_bytes() -> None:
    tier = _stage_tier()

    assert tier["capacity_bytes"] == AVAILABLE
    assert tier["capacity_source"] == "zfs available"
    assert tier["dataset"] == f"{POOL}/prewarm"
    # The raw geometry is still recorded, so the difference is visible rather
    # than merely absent.
    assert tier["pool_size_bytes"] == SIZE
    assert tier["capacity_bytes"] < tier["pool_size_bytes"]


def test_the_mover_writes_where_the_record_says_it_does() -> None:
    """The mountpoint is the staging dataset's, not the pool root's."""

    assert _stage_tier()["mountpoint"] == "/stage/prewarm"


def test_a_pool_without_the_staging_dataset_falls_back_to_its_root() -> None:
    tier = _stage_tier(datasets=f"{POOL}\t{AVAILABLE}\t/{POOL}\n")

    assert tier["dataset"] == POOL
    assert tier["capacity_bytes"] == AVAILABLE
    assert tier["mountpoint"] == f"/{POOL}"


def test_an_unreadable_dataset_offers_nothing_rather_than_the_raw_geometry() -> None:
    """The fallback was the defect: announcing it does not make it safe.

    A pool being imported or exported makes ``zfs list`` fail for a cycle.
    Pricing that cycle from ``zpool size`` hands out tokens for parity, slop
    and every other dataset's bytes -- to a mover that discovers the truth at
    ENOSPC, after reading its range off the disks.  Offering nothing is what a
    box with no zpool already does, and the next cycle is 60 s away.
    """

    tier = _stage_tier(fail=("list -Hp -r",))

    assert tier["capacity_bytes"] == 0
    assert tier["capacity_source"] == "none (no dataset readable)"
    assert tier["dataset"] is None
    # And nothing is minted from it, which is the part that matters.
    assert storage_tiers.tier_tokens(tier) == {}
    # The geometry is still recorded, so an operator can see what was refused.
    assert tier["pool_size_bytes"] == SIZE


def test_tokens_follow_the_capacity_that_was_minted() -> None:
    tier = _stage_tier()
    tokens = storage_tiers.tier_tokens(tier)

    assert tokens[storage_tiers.STAGE_CAPACITY_KIND] == AVAILABLE // storage_tiers.GIB
    # And that is fewer than the raw geometry would have offered.
    assert tokens[storage_tiers.STAGE_CAPACITY_KIND] < SIZE // storage_tiers.GIB

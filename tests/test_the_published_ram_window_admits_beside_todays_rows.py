"""The published window must admit beside the rows the box actually carries.

CEO decision dec-1005-140823-4934 (option D) lowers ``window_gib_default``
from 160 to 96 because the published default refused at the live host-row
load: the tier loop's readings on dl380g10 (2026-10-05 14:04Z) were
MemTotal 316,241,047,552 B, ARC ``c_max`` 23,622,320,128 B (22 GiB, above
the 20 GiB floor), reserve 16 GiB, and rows holding 141,733,920,768 B
(132 GiB) of host tokens, which leaves an allowed window of
133,704,937,472 B.  A 160 GiB window refused
(``ram_window_exceeds_memtotal_floor``) and the tier minted nothing at all
-- worse than any smaller window.  This test loads the real committed
policy and replays exactly those numbers through ``ram_admission``, so a
publish that re-raises the window past what the rows beside it allow fails
here by name.  The boundary pins the arithmetic, not just the value: a 96
GiB window admits beside at most 160.5 GiB of rows
(294.5 - 96 - 22 - 16), and one GiB of rows above that refuses with the
same reason the 160 GiB default died of.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prismabuild.storage_tiers as storage_tiers  # noqa: E402

GIB = storage_tiers.GIB
POLICY = (Path(__file__).resolve().parents[1] / "tools" / "fleet"
          / "ram_tier_policy.json")

# The tier loop's live readings on dl380g10, 2026-10-05 14:04Z.
MEM_TOTAL = 316_241_047_552              # 294.5 GiB
ARC = {"c_max": 23_622_320_128,          # 22 GiB: the ARC's own permission
       "size": 11 * GIB, "arc_meta_used": 5 * GIB}
ROWS_HELD_GIB = 132                      # 141,733,920,768 B of host tokens
NODES = (0, 1)
NODE_MEMTOTAL = {0: MEM_TOTAL // 2, 1: MEM_TOTAL // 2}


def _admission(*, rows_held_gib: int) -> dict[str, object]:
    policy = storage_tiers.read_ram_policy(POLICY)
    assert policy is not None, "the policy is committed, not implied"
    return storage_tiers.ram_admission(
        ceiling_bytes=240 * GIB,
        mount_options=["rw", "noswap", "size=240G", "mpol=interleave:0-1"],
        policy=policy, mem_total=MEM_TOTAL, arc=ARC,
        rows_held_gib=rows_held_gib, memory_nodes=NODES,
        node_memtotal_bytes=NODE_MEMTOTAL)


def test_the_published_window_admits_beside_the_rows_the_box_carries() -> None:
    """The real policy, tonight's real numbers: the tier must admit."""

    admission = _admission(rows_held_gib=ROWS_HELD_GIB)

    assert admission["admissible"] is True, admission
    assert admission["reason"] is None
    # The directed default, and the live budget it must fit under:
    # 294.5 - 22 - 16 - 132 = 133,704,937,472 B.  A 160 GiB window exceeds
    # this by the whole gap and refuses; 96 GiB fits with 28.5 GiB to spare.
    assert admission["window_bytes"] == 96 * GIB
    assert admission["allowed_window_bytes"] == 133_704_937_472
    # The mount facts are otherwise sound: both memory nodes were checked
    # and neither refused the window/ARC/reserve share.
    assert admission["memory_node_count"] == 2
    assert admission["failing_memory_node"] is None


def test_the_boundary_is_the_subtraction_not_the_number() -> None:
    """One GiB of rows above the bound refuses exactly as 160 GiB did."""

    over = _admission(rows_held_gib=161)

    assert over["admissible"] is False
    assert over["reason"] == "ram_window_exceeds_memtotal_floor"
    # 294.5 - 22 - 16 - 161 = 102,566,424,576 B: a GiB short of the window.
    assert over["allowed_window_bytes"] == 102_566_424_576
    assert over["window_bytes"] == 96 * GIB

    at = _admission(rows_held_gib=160)
    assert at["admissible"] is True, at

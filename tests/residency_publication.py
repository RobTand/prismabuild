"""File the publication evidence a landed range really leaves behind.

A fixture that only acquires tier tokens describes a *reservation*: the room
is booked and nothing has been copied.  That was enough to look staged until
issue #759, when the RAM window published a promotion against a stage copy
that was still running and its own proof refused ``source-coverage-gap``.
``residency_plan.resident_movers`` now asks for what a finished copy actually
leaves -- the consumer's fragment, plus the mover's complete receipt, tied to
each other by their entry counts -- so a fixture that means "landed" has to
file both.

This is the one place that shape is written down for tests, so a fixture
saying "landed" cannot drift from what ``stage_move`` and ``ram_promote``
file.  Nothing here touches production paths: it writes exactly the two
records those two tools write on a clean finish.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool, residency_map  # noqa: E402


def vouch_landed(
    queue: pool.PoolQueue,
    *,
    consumer_action_key: str,
    mover_action_key: str,
    tier_id: str,
    stage_root: str | Path,
    manifest_sha256: str,
    range_start_bytes: int,
    range_end_bytes: int,
    entries: int = 1,
    epoch: str | None = None,
    name: str = "shard",
    unix: float = 1000.0,
) -> None:
    """File the fragment and the receipt one finished copy leaves.

    ``stage_root`` is the root the copy *landed on*: the plan's stage root
    for a stage mover, and the announced ram mountpoint for a promotion --
    never the stage it read from, which is the distinction ``ram_promote``
    makes when it writes ``stage_root: args.ram_root`` (#759).  ``epoch``
    belongs to the ram leg and must be the announced one; the stage leg has
    none.

    The two records are consistent by construction: the receipt declares and
    stages exactly the ``entries`` the fragment names, which is the tie that
    stops a historical receipt speaking for a later partial copy.
    """

    root = Path(stage_root)
    span = int(range_end_bytes) - int(range_start_bytes)
    each = max(1, span // max(1, entries))
    named: dict[str, dict[str, object]] = {}
    for index in range(entries):
        declared = f"/in/{name}-{index}.bin"
        named[residency_map.residency_map_key(declared, 0)] = {
            "stage_path": str(root / "model" / f"{name}-{index}.bin"),
            "bytes": each, "offset": 0, "sha256": f"{index:x}" * 64,
        }
    fragment: dict[str, object] = {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer_action_key,
        "mover_action_key": mover_action_key,
        "tier_id": tier_id, "stage_root": str(root),
        "manifest_sha256": manifest_sha256, "entries": named,
    }
    if epoch is not None:
        fragment["epoch"] = epoch
    residency_map.write_fragment(queue.residency_fragment_root(), fragment)
    receipt: dict[str, object] = {
        "consumer_action_key": consumer_action_key,
        "tier_id": tier_id, "stage_root": str(root),
        "manifest_sha256": manifest_sha256,
        "range_start_bytes": int(range_start_bytes),
        "range_end_bytes": int(range_end_bytes),
        "range_bytes": span, "bytes_staged": span,
        "entries_declared": entries, "entries_staged": entries,
        "complete": True, "seconds": 1.0, "unix": unix,
    }
    if epoch is not None:
        receipt["epoch"] = epoch
    queue.record_move(mover_action_key, receipt)

"""File the publication evidence a landed range really leaves behind.

A fixture that only acquires tier tokens describes a *reservation*: the room
is booked and nothing has been copied.  That was enough to look staged until
issue #759, when the RAM window published a promotion against a stage copy
that was still running and its own proof refused ``source-coverage-gap``.
``residency_plan.resident_movers`` now asks for what a finished copy actually
leaves -- the consumer's fragment, plus the mover's complete receipt, tied to
each other by their entry counts -- so a fixture that means "landed" has to
file both.

The fixture creates private sparse files, hashes their actual bytes and dates
their live identities through ``staged_path.publish_vouch``. Its complete
receipt describes exactly those published entries. The supplied manifest
reference remains the caller's plan identity; this helper does not seal CAS.
"""
from __future__ import annotations

from pathlib import Path
import sys
import hashlib

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool, reader_lease, residency_map, staged_path  # noqa: E402


def publish_landed_files(
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
    """Create real sparse payloads and publish their dated pair and receipt.

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
    if not root.resolve().is_relative_to(queue.root.parent.resolve()):
        raise ValueError('landed fixture files must stay in the private queue workspace')
    span = int(range_end_bytes) - int(range_start_bytes)
    each, remainder = divmod(span, entries)
    named: dict[str, dict[str, object]] = {}
    material: dict[str, dict[str, object]] = {}
    for index in range(entries):
        declared = f"/in/{name}-{index}.bin"
        path = root / "model" / mover_action_key / f"{name}-{index}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        size = each + (remainder if index == entries - 1 else 0)
        with path.open('wb') as stream:
            stream.truncate(size)
        with path.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        key = residency_map.residency_map_key(declared, 0)
        named[key] = {"stage_path": str(path), "bytes": size,
                      "offset": 0, "sha256": digest}
        material[key] = {**named[key], 'file_id': reader_lease.stat_identity(str(path))}
    fragment: dict[str, object] = {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer_action_key,
        "mover_action_key": mover_action_key,
        "tier_id": tier_id, "stage_root": str(root),
        "manifest_sha256": manifest_sha256, "entries": named,
    }
    if epoch is not None:
        fragment["epoch"] = epoch
    staged_path.publish_vouch(queue.records.residency_fragment_root(), fragment,
        generation=reader_lease.mint_generation(), material_entries=material)
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

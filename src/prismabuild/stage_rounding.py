"""Helpers for issue 1555: stage rounding visibility.

Holds the new pure helpers so tier_loop.py keeps its own diff small.
Imports run lazily to avoid a cycle at module load.
"""
from __future__ import annotations

from collections.abc import Mapping
import functools
from pathlib import Path


def _plan_range_legs(queue, path: Path) -> tuple[tuple[str, int], ...] | None:
    """One filed plan's ``(mover key, range bytes)`` legs, in plan order.

    ``None`` when the plan does not read or validate: a kept read does
    not remember that answer, so the plan is read again next cycle.
    """
    from prismabuild import pool as pool_mod
    from prismabuild import residency_plan as plan_mod

    try:
        plan = plan_mod.read_filed(queue, Path(path).stem)[0]
    except (OSError, pool_mod.PoolContractError, ValueError):
        return None
    if plan is None:
        return None
    try:
        phases = plan.get("phases")
    except AttributeError:
        return None
    if not isinstance(phases, list):
        return ()
    legs: list[tuple[str, int]] = []
    for phase in phases:
        if not isinstance(phase, Mapping):
            continue
        for leg in plan_mod.prelaunch_stage_legs(phase):
            try:
                mover = str(leg["mover_key"])
                start = int(leg["start_bytes"])  # type: ignore[arg-type]
                end = int(leg["end_bytes"])  # type: ignore[arg-type]
            except (TypeError, ValueError, KeyError):
                continue
            if len(mover) != 64 or end <= start or start < 0:
                continue
            legs.append((mover, end - start))
    return tuple(legs)


def declared_range_index(queue, store=None) -> dict[str, int] | None:
    """Sealed range bytes by mover key over every filed plan.

    One scan for the whole mint: lists the filed plans once and
    reads each once. Through the tier cycle's kept ``store``
    (``RecordStore.kept``) a plan whose file version holds is not
    read again from one cycle to the next: the mint read all 740
    plans, 374 MB, every cycle on dl380g10 (pb-tierscan). Returns
    None when the plan directory does not read, so the caller
    reports every in-flight holder unknown.
    """
    from prismabuild import pool as pool_mod
    from prismabuild import residency_plan as plan_mod

    records = queue.records if store is None else store
    try:
        filed = records.residency_plan_records(
            select=plan_mod.filed_plan_entry,
            parse=functools.partial(_plan_range_legs, queue),
            keep=lambda legs: legs is not None)
    except (OSError, pool_mod.PoolContractError, ValueError):
        return None
    index: dict[str, int] = {}
    for _path, legs in filed:
        for mover, size in legs or ():
            index.setdefault(mover, size)
    return index


def declared_range_bytes(queue, mover_key: str,
                         index: Mapping[str, int] | None = None,
                         ) -> int | None:
    """One mover's sealed range bytes, or None when no filed plan names it.

    Reads ``index`` when given (one scan per mint), else scans once
    for this key. A key no plan names has unknown bytes: the caller
    reports it apart and never counts it as waste. Never raises for
    queue-state reasons.
    """
    key = str(mover_key)
    if index is not None:
        return index.get(key)
    built = declared_range_index(queue)
    if built is None:
        return None
    return built.get(key)


def stage_holder_bytes(queue, key: str,
                       index: Mapping[str, int] | None = None,
                       ) -> tuple[int | None, int | None]:
    """One held key's declared bytes, split by landing state.

    Returns (landed_bytes, in_flight_bytes). A complete, unrefused
    receipt names landed bytes through bytes_staged. A holder still
    in flight names its sealed plan range. A holder with no plan leg
    has unknown bytes: (None, None). A holder is in one side only.
    """
    from prismabuild import pool as pool_mod

    try:
        receipt = queue.move_record(key)
    except (OSError, pool_mod.PoolContractError, ValueError):
        return (None, None)
    if receipt is None:
        declared = declared_range_bytes(queue, key, index)
        if declared is None:
            return (None, None)
        return (None, declared)
    if not isinstance(receipt, Mapping):
        return (None, None)
    if receipt.get("complete") is True and not receipt.get("refusal"):
        staged = receipt.get("bytes_staged")
        if isinstance(staged, int) and not isinstance(staged, bool) and staged >= 0:
            return (staged, None)
        return (0, None)
    declared = declared_range_bytes(queue, key, index)
    if declared is None:
        return (None, None)
    return (None, declared)


def landed_and_in_flight_bytes(queue, tier_id: str, kind: str, store=None,
                               ) -> tuple[int, int, int, int, int]:
    """Declared bytes beside held tokens for one stage tier.

    Returns (landed_tokens, landed_bytes, in_flight_tokens,
    in_flight_bytes, in_flight_unknown_gib) over the same held keys
    and the same complete-and-unrefused line landed_and_in_flight
    draws. Token counts equal its counts exactly. Landed bytes come
    from complete receipts; in-flight bytes come from the sealed
    plan range of each holder. A holder with no plan leg keeps its
    tokens in the counts and adds them to in_flight_unknown_gib.
    ``store`` is the tier cycle's kept record store, when there is one.
    """
    from prismabuild import pool as pool_mod

    ledger = queue.tier(tier_id)
    landed_tokens = 0
    landed_bytes = 0
    in_flight_tokens = 0
    in_flight_bytes = 0
    in_flight_unknown = 0
    index = declared_range_index(queue, store)
    for key in ledger.holders():
        tokens = int(ledger.holdings(key).get(kind, 0))
        if tokens <= 0:
            continue
        landed_b, flight_b = stage_holder_bytes(queue, key, index)
        if landed_b is not None:
            landed_tokens += tokens
            landed_bytes += landed_b
        elif flight_b is not None:
            in_flight_tokens += tokens
            in_flight_bytes += flight_b
        else:
            receipt = None
            try:
                receipt = queue.move_record(key)
            except (OSError, pool_mod.PoolContractError, ValueError):
                receipt = None
            if (isinstance(receipt, Mapping) and receipt.get("complete") is True
                    and not receipt.get("refusal")):
                landed_tokens += tokens
            else:
                in_flight_tokens += tokens
                in_flight_unknown += tokens
    return (landed_tokens, landed_bytes, in_flight_tokens, in_flight_bytes,
            in_flight_unknown)


def rounding_gib(tokens: int, byte_count: int, gib: int) -> int:
    """Rounding waste in whole GiB, never below zero.

    tokens minus floor(byte_count / gib). Guards the stale-read case
    where bytes exceed tokens by clamping at zero.
    """
    whole = int(byte_count) // int(gib)
    gap = int(tokens) - whole
    return gap if gap > 0 else 0

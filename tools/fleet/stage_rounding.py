"""Helpers for issue 1555: stage rounding visibility.

Holds the new pure helpers so tier_loop.py keeps its own diff small.
Imports run lazily to avoid a cycle at module load.
"""
from __future__ import annotations

from collections.abc import Mapping


def declared_range_index(queue) -> dict[str, int] | None:
    """Sealed range bytes by mover key over every filed plan.

    One scan for the whole mint: lists the filed plans once and
    reads each once. Returns None when the plan directory does not
    read, so the caller reports every in-flight holder unknown.
    """
    from prismabuild import pool as pool_mod
    from prismabuild import residency_plan as plan_mod

    try:
        plans_dir = queue.root / pool_mod.RESIDENCY_PLANS
        names = sorted(
            path.stem for path in pool_mod._scan(plans_dir)
            if path.suffix == ".json" and len(path.stem) == 64
            and set(path.stem) <= set("0123456789abcdef"))
    except (OSError, pool_mod.PoolContractError, ValueError):
        return None
    index: dict[str, int] = {}
    for consumer in names:
        try:
            plan = plan_mod.read_filed(queue, consumer)[0]
        except (OSError, pool_mod.PoolContractError, ValueError):
            continue
        if plan is None:
            continue
        try:
            phases = plan.get("phases")
        except AttributeError:
            continue
        if not isinstance(phases, list):
            continue
        for phase in phases:
            if not isinstance(phase, Mapping):
                continue
            for leg in plan_mod._prelaunch_stage_legs(phase):
                try:
                    mover = str(leg["mover_key"])
                    start = int(leg["start_bytes"])  # type: ignore[arg-type]
                    end = int(leg["end_bytes"])  # type: ignore[arg-type]
                except (TypeError, ValueError, KeyError):
                    continue
                if (len(mover) != 64 or end <= start or start < 0
                        or mover in index):
                    continue
                index[mover] = end - start
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


def landed_and_in_flight_bytes(queue, tier_id: str, kind: str,
                               ) -> tuple[int, int, int, int, int]:
    """Declared bytes beside held tokens for one stage tier.

    Returns (landed_tokens, landed_bytes, in_flight_tokens,
    in_flight_bytes, in_flight_unknown_gib) over the same held keys
    and the same complete-and-unrefused line landed_and_in_flight
    draws. Token counts equal its counts exactly. Landed bytes come
    from complete receipts; in-flight bytes come from the sealed
    plan range of each holder. A holder with no plan leg keeps its
    tokens in the counts and adds them to in_flight_unknown_gib.
    """
    from prismabuild import pool as pool_mod

    ledger = queue.tier_ledger(tier_id)
    landed_tokens = 0
    landed_bytes = 0
    in_flight_tokens = 0
    in_flight_bytes = 0
    in_flight_unknown = 0
    index = declared_range_index(queue)
    for key in ledger.held_keys():
        tokens = int(ledger.holder_tokens(key).get(kind, 0))
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

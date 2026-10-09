"""Helpers for issue 1555: stage rounding visibility.

Holds the new pure helpers so tier_loop.py keeps its own diff small.
Imports run lazily to avoid a cycle at module load.
"""
from __future__ import annotations

from collections.abc import Mapping


def stage_holder_bytes(queue, key: str) -> tuple[int | None, int | None]:
    """One held key's receipted bytes, split by landing state.

    Returns (landed_bytes, in_flight_bytes). A complete, unrefused
    receipt names landed bytes through bytes_staged. Any other
    holder reads as wholly in flight. A receipt with no usable
    bytes_staged still counts as landed when complete and unrefused,
    with zero bytes known. A holder is in one side only.
    """
    from prismabuild import pool as pool_mod

    try:
        receipt = queue.move_record(key)
    except (OSError, pool_mod.PoolContractError, ValueError):
        return (None, None)
    if not isinstance(receipt, Mapping):
        return (None, None)
    if receipt.get("complete") is True and not receipt.get("refusal"):
        staged = receipt.get("bytes_staged")
        if isinstance(staged, int) and not isinstance(staged, bool) and staged >= 0:
            return (staged, None)
        return (0, None)
    return (None, 0)


def landed_and_in_flight_bytes(queue, tier_id: str, kind: str,
                               ) -> tuple[int, int, int, int]:
    """Receipted bytes beside held tokens for one stage tier.

    Returns (landed_tokens, landed_bytes, in_flight_tokens,
    in_flight_bytes) over the same held keys and the same
    complete-and-unrefused line landed_and_in_flight draws. Token
    counts equal its counts exactly. A holder the byte reader cannot
    classify contributes its tokens and no bytes.
    """
    from prismabuild import pool as pool_mod

    ledger = queue.tier_ledger(tier_id)
    landed_tokens = 0
    landed_bytes = 0
    in_flight_tokens = 0
    in_flight_bytes = 0
    for key in ledger.held_keys():
        tokens = int(ledger.holder_tokens(key).get(kind, 0))
        if tokens <= 0:
            continue
        landed_b, flight_b = stage_holder_bytes(queue, key)
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
    return landed_tokens, landed_bytes, in_flight_tokens, in_flight_bytes


def rounding_gib(tokens: int, byte_count: int, gib: int) -> int:
    """Rounding waste in whole GiB, never below zero.

    tokens minus floor(byte_count / gib). Guards the stale-read case
    where bytes exceed tokens by clamping at zero.
    """
    whole = int(byte_count) // int(gib)
    gap = int(tokens) - whole
    return gap if gap > 0 else 0

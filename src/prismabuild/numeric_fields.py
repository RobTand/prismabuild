"""Validation of numeric scalars at persisted-record boundaries."""
import math


def is_finite_number(value: object) -> bool:
    """Accept finite numeric scalars, excluding booleans and overflow."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False

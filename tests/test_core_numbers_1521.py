"""Numeric fields share the core predicate without accepting invalid JSON scalars."""
import math

import pytest

from prismabuild import core


@pytest.mark.parametrize("value,expected", [
    (0, True), (-1, True), (1.5, True), (True, False), (False, False),
    (None, False), ("1", False), (math.nan, False),
    (math.inf, False), (-math.inf, False), (10 ** 400, False),
])
def test_finite_numeric_fields(value, expected):
    assert core.is_finite_number(value) is expected

"""Wire spelling for diagnostics/history remains separate from canonical JSON."""

import pytest

from prismabuild import core


@pytest.mark.parametrize("value, expected", [
    ({"z": "café λ", "a": [None, True, "\n"]},
     b'{"a": [null, true, "\\n"], "z": "caf\\u00e9 \\u03bb"}'),
    ([{"z": 1, "a": -0.0}, {"s": "é"}],
     b'[{"a": -0.0, "z": 1}, {"s": "\\u00e9"}]'),
    ({"z": float("inf"), "a": float("nan")}, b'{"a": NaN, "z": Infinity}'),
    ([], b'[]'),
])
def test_sorted_json_preserves_literal_wire_spelling(value, expected):
    assert core._sorted_json_bytes(value) == expected
    assert not expected.endswith(b"\n")


def test_mapping_line_profile_keeps_mapping_conversion_and_one_lf():
    value = [("z", "é"), ("a", 1)]
    assert core._sorted_lf_bytes(value) == b'{"a": 1, "z": "\\u00e9"}\n'
    assert core._sorted_json_bytes(value) == b'[["z", "\\u00e9"], ["a", 1]]'


def test_sorted_profile_keeps_type_errors_and_canonical_nonfinite_refusal():
    with pytest.raises(TypeError):
        core._sorted_json_bytes({"value": object()})
    with pytest.raises(core.ActionContractError, match="finite canonical"):
        core._canonical_bytes({"value": float("nan")})

"""Full-stack seam tests — PQ bindings pending root API review.

Every test here asserts fail-closed behavior: reaching for a PQ reader or
the PQ join before root provides the bindings refuses with
`FullstackPQUnavailable` naming the missing capability. That refusal is
the wiring proof. It proves no capability, stages no bytes through a
reader, and invents no parallel implementation. When bindings land, root
reviews the API before interface-rigid tests replace these.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))

import fullstack_pq_seam as seam  # noqa: E402
from fullstack_fixtures import quantum_payload, roster  # noqa: E402


@pytest.mark.parametrize("binding", seam.REQUIRED_BINDINGS)
def test_unbound_reader_refuses_fail_closed(binding: str) -> None:
    with pytest.raises(seam.FullstackPQUnavailable, match=binding.split("-")[1]):
        seam.require(binding)


def test_source_open_refuses_without_binding() -> None:
    with pytest.raises(seam.FullstackPQUnavailable):
        seam.open_source({"path": "/pool/model/shard-0.bin", "offset": 0})


def test_render_open_refuses_without_binding() -> None:
    with pytest.raises(seam.FullstackPQUnavailable):
        seam.open_render({"path": "/pool/model/render-0.bin", "offset": 0})


def test_activation_open_refuses_without_binding() -> None:
    with pytest.raises(seam.FullstackPQUnavailable):
        seam.open_activation({"entry": "boundary-0-13"})


def test_join_refuses_without_binding() -> None:
    units = ["unit-a", "unit-b"]
    with pytest.raises(seam.FullstackPQUnavailable):
        seam.join_coverage([quantum_payload("layer-000", units)], roster(units))


def test_fixture_shapes_are_deterministic() -> None:
    """Stable shapes the future join consumes: sorted keys, sha256 names."""
    units = ["unit-b", "unit-a"]
    first = quantum_payload("layer-000", units)
    second = quantum_payload("layer-000", list(reversed(units)))
    assert first == second
    assert list(first["costs"]) == sorted(units)
    assert roster(units) == roster(list(reversed(units)))

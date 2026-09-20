"""PQ-reader seam for the full-stack integration harness: fail-closed pending API.

This module is the single boundary between the PB-side chain (manifests,
movers, tiers, receipts — all real production machinery) and PQ's
source/render/activation readers. Root provides the candidate-commit API
bindings; until then every entry raises :class:`FullstackPQUnavailable`
naming the missing binding. Seam tests assert that refusal: fail-closed
wiring proof, never capability proof and never fake green.

Stable now (no PQ import anywhere in this lane): fixture payload shapes
(sorted keys, sha256-named), receipt shapes, and the coverage-input roster
format the future join consumes. Root reviews the API before any
interface-rigid test is written against it.
"""

from __future__ import annotations


class FullstackPQUnavailable(RuntimeError):
    """A PQ-reader binding the harness reached before root provided it."""


#: The bindings root owes, in the order the chain needs them. Names are
#: capability labels, not module paths — no path is invented here.
REQUIRED_BINDINGS = (
    "pq-source-reader",      # staged source shard/range open by manifest entry
    "pq-render-reader",      # staged render open with digest admission
    "pq-activation-reader",  # staged activation/boundary open
    "pq-coverage-join",      # deterministic join with coverage/gap refusal
)

#: Set by the (future) pin loader; empty until root provides bindings.
BOUND: dict[str, str] = {}


def require(binding: str) -> str:
    """Return the bound API identity, or refuse fail-closed."""
    if binding not in BOUND:
        raise FullstackPQUnavailable(
            f"{binding} is not bound: root candidate-commit API bindings "
            f"are pending; required one of {REQUIRED_BINDINGS}")
    return BOUND[binding]


def open_source(entry: dict) -> object:
    """Open one staged source entry through the real PQ source reader."""
    require("pq-source-reader")


def open_render(entry: dict) -> object:
    """Open one staged render through the real PQ render reader."""
    require("pq-render-reader")


def open_activation(entry: dict) -> object:
    """Open one staged activation through the real PQ activation reader."""
    require("pq-activation-reader")


def join_coverage(payloads: list[dict], roster: dict) -> dict:
    """Deterministic join with coverage proof and gap refusal."""
    require("pq-coverage-join")

"""Full-stack qualification gate — PQ reader bindings must exist to qualify.

The staged-only chain cannot be proven without the real PQ source,
render, and activation readers plus the join. Those bindings arrive as
`tests/fullstack_pq_bindings.json` with root's candidate commit and API
identities (module + attribute per capability, plus the source snapshot
and artifact dependency the chain runs against). Until root provides
that file, this gate FAILS: the suite reports not-qualified instead of
going green over unavailable readers as if that were desired behavior.

No module paths are invented here — every import below comes from the
bindings file. No stub, no fake parser, no parallel reader.
"""
from __future__ import annotations

import importlib
import json
from pathlib import Path

REQUIRED_CAPABILITIES = (
    "pq-source-reader",
    "pq-render-reader",
    "pq-activation-reader",
    "pq-coverage-join",
)

BINDINGS = (Path(__file__).resolve().parent / "fullstack_pq_bindings.json")


def _load_bindings() -> dict:
    assert BINDINGS.exists(), (
        "PQ reader bindings missing: tests/fullstack_pq_bindings.json "
        "is pending root's candidate-commit API bindings; the staged-only "
        "chain is NOT qualified without the real readers")
    return json.loads(BINDINGS.read_text())


def test_bindings_manifest_names_every_capability() -> None:
    bindings = _load_bindings()
    assert bindings["candidate_commit"], "bindings must pin the PQ commit"
    for capability in REQUIRED_CAPABILITIES:
        entry = bindings["capabilities"][capability]
        assert entry["module"] and entry["attribute"], capability
    assert bindings["source_snapshot"], "bindings must pin the source snapshot"
    assert bindings["artifact_dependency"], "bindings must pin artifacts"


def test_bound_reader_apis_import_and_are_callable() -> None:
    bindings = _load_bindings()
    for capability in REQUIRED_CAPABILITIES:
        entry = bindings["capabilities"][capability]
        module = importlib.import_module(entry["module"])
        assert callable(getattr(module, entry["attribute"])), capability

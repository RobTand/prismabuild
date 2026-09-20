"""Full-stack PQ contract tests — real producer spellings, pinned source.

Wires REAL current PQ functions for the policy baseline: the pure
producer constructions PQ's own joiner contract names as shared spelling
(`quantum_id`, `roster_digest`, `qname_layer`, `phase_ranges`) plus the
canonical-JSON compat rule receipts depend on. No stub, no fake parser,
no parallel reader, no invented module paths.

Source resolution: `PRISMAQUANT_CHECKOUT` (default `/home/rob/prismaquant`,
the existing fleet precedent) must exist at exactly `PQ_PINNED_COMMIT`;
anything else fails loudly — a mutable checkout is never silently
trusted. The pin updates only by explicit reviewed commit change. The
future `tools/resolve_pq_fixture_pin.py` (target-env pip case) lands
with the lease-API work; until then this file is the pin.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

PQ_CHECKOUT = Path(os.environ.get("PRISMAQUANT_CHECKOUT", "/home/rob/prismaquant"))
PQ_PINNED_COMMIT = "22149e1aa35a6190e2d7925defb085a698a74caa"


def _resolve() -> None:
    assert PQ_CHECKOUT.is_dir(), (
        f"PQ checkout missing at {PQ_CHECKOUT}: set PRISMAQUANT_CHECKOUT "
        f"to the pinned source; the chain is not qualified without it")
    head = subprocess.run(
        ["git", "-C", str(PQ_CHECKOUT), "rev-parse", "HEAD"],
        capture_output=True, text=True, timeout=30)
    assert head.returncode == 0, f"PQ checkout at {PQ_CHECKOUT} is not a git tree"
    assert head.stdout.strip() == PQ_PINNED_COMMIT, (
        f"PQ checkout at {head.stdout.strip()} != pinned {PQ_PINNED_COMMIT}: "
        f"update the pin by explicit review, never by silent drift")
    if str(PQ_CHECKOUT) not in sys.path:
        sys.path.insert(0, str(PQ_CHECKOUT))


@pytest.fixture(scope="module")
def pq():
    _resolve()
    import prismaquant.joint_layer_quanta as joint  # noqa: E402
    import prismaquant.cost_stage_checkpoint as checkpoint  # noqa: E402
    return joint, checkpoint


def test_quantum_id_grammar(pq) -> None:
    joint, _ = pq
    assert joint.quantum_id(13) == "layer-013"
    assert joint.quantum_id(0) == "layer-000"
    with pytest.raises(ValueError):
        joint.quantum_id(-1)
    with pytest.raises(ValueError):
        joint.quantum_id("13")


def test_roster_digest_is_sorted_hex_without_trailing_newline(pq) -> None:
    joint, _ = pq
    names = ["model.language_model.layers.3.mlp.up_proj",
             "model.language_model.layers.3.mlp.gate_proj"]
    digest = joint.roster_digest(names)
    assert digest == joint.roster_digest(list(reversed(names)))
    assert digest == hashlib.sha256("\n".join(sorted(names)).encode()).hexdigest()
    assert len(digest) == 64 and int(digest, 16) >= 0
    with pytest.raises(ValueError):
        joint.roster_digest([*names, names[0]])
    with pytest.raises(ValueError):
        joint.roster_digest([""])


def test_qname_layer_grammar(pq) -> None:
    joint, _ = pq
    assert joint.qname_layer("model.language_model.layers.13.mlp.gate_proj") == 13
    assert joint.qname_layer("model.language_model.layers.3.mlp.up_proj") == 3
    assert joint.qname_layer("head-weight") is None
    assert joint.qname_layer(None) is None


def test_phase_ranges_tile_and_refuse_straddle(pq) -> None:
    joint, _ = pq
    manifest = {
        "entries": [
            {"path": "/pool/a.bin", "offset": 0, "bytes": 1 << 20,
             "sha256": "ab" * 32},
            {"path": "/pool/b.bin", "offset": 0, "bytes": 2 << 20,
             "sha256": "cd" * 32},
        ],
        "entry_count": 2, "total_bytes": 3 << 20,
        "annotations": {"phases": [
            {"name": "head", "cumulative_bytes": 1 << 20},
            {"name": "tail", "cumulative_bytes": 3 << 20},
        ]},
    }
    rows = joint.phase_ranges(manifest)
    assert [(r["name"], r["start_bytes"], r["end_bytes"]) for r in rows] == [
        ("head", 0, 1 << 20), ("tail", 1 << 20, 3 << 20)]
    bad = json.loads(json.dumps(manifest))
    bad["annotations"]["phases"][0]["cumulative_bytes"] = (1 << 20) - 1
    with pytest.raises(Exception):
        joint.phase_ranges(bad)


def test_canonical_json_compat_utf8_and_no_nan(pq) -> None:
    _, checkpoint = pq
    narrow = checkpoint.canonical_json_sha256({"k": "v"}, where="fullstack")
    wide = checkpoint.canonical_json_sha256({"k": "ünïcodé"}, where="fullstack")
    assert narrow != wide
    assert wide == hashlib.sha256(
        json.dumps({"k": "ünïcodé"}, sort_keys=True, separators=(",", ":"),
                   ensure_ascii=False).encode("utf-8")).hexdigest()
    with pytest.raises(Exception):
        checkpoint.canonical_json_sha256({"k": float("nan")}, where="fullstack")


def test_harness_roster_shape_speaks_real_digest(pq) -> None:
    """The harness fixture roster hashes exactly like the real producer."""
    joint, _ = pq
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from fullstack_fixtures import roster
    units = ["unit-b", "unit-a"]
    assert roster(units)["roster_sha256"] == joint.roster_digest(units)

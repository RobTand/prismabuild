"""The pre-publish shape gate: a real campaign's manifest shape, end to end.

Not collected by the ordinary suite (the name has no ``test_`` prefix): it
moves about 176 MiB per tier, and it is a publication step rather than a
unit test.  Run it by path::

    pbtest.py --checkout <tree> --tag gb10 --priority -10 \\
        tests/gate_campaign_shape.py

The shape is the committed table of manifest ``682e7b0a859f``, the v2 read
plan of PQ consumer ``a7d31a4da9c1`` whose stage chunk 0 refused
``residency_overran_reservation`` in production (#965): 9,255 entries in six
phases, two of them larger than a chunk, at 1/1024 scale.  The harness is
``tools/fleet/shape_gate.py``; its own tests are
``tests/test_the_campaign_shape_harness.py``.

On a pass the result is printed as one JSON line, so a run's log carries
what was exercised and what each tier moved.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import storage_tiers  # noqa: E402
import pbrun  # noqa: E402
import shape_gate  # noqa: E402

TABLE = shape_gate.TABLE_ROOT / "682e7b0a859f.json.gz"
#: The committed table's bytes.  A changed table is a changed gate, and says so.
TABLE_SHA256 = "3ad4ab3a4f8f7461b203dc867d04ffa99c4a1a8f74edd37eb5bdd0deebb3af2c"


def test_the_reference_campaign_shape_stages_and_reads_back(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    raw = TABLE.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == TABLE_SHA256
    table = shape_gate.parse_table(raw, where=str(TABLE))
    monkeypatch.setattr(storage_tiers, "GIB", shape_gate.UNIT_BYTES)

    result = shape_gate.run_gate(table, root=tmp_path / "gate",
                                 shared_root=Path(pbrun.SH))

    print(json.dumps({"shape_gate": result}, sort_keys=True))
    assert result["entries"] == 9255
    assert result["coverage"]["phases_over_chunk"] == ["chain-044", "chain-040"]
    assert result["coverage"]["byte_cuts_inside_entries"] >= 1

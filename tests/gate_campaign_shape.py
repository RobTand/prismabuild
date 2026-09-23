"""The pre-publish shape gate: real campaign manifest shapes, end to end.

Not collected by the ordinary suite (the name has no ``test_`` prefix): it
moves a few hundred MiB per tier, and it is a publication step rather than a
unit test.  Run it by path, as one shard, before a publish::

    pbtest.py --checkout <tree> --python <venv>/bin/python --tag gb10 \\
        --priority 0 --timeout-s 3600 --shards 1 --json <receipt>.json \\
        tests/gate_campaign_shape.py

and hand the shard's action key to ``publish_runtime.py --shape-gate-action``.
Every table in ``shape_gate.REFERENCE_TABLES`` runs, one test each, at 1/1024
scale, against a hermetic queue, stage and RAM root under ``tmp_path``:

* ``682e7b0a859f``: the v2 read plan of PQ consumer ``a7d31a4da9c1``, whose
  stage chunk 0 refused ``residency_overran_reservation`` in production
  (#965).  9,255 entries in six phases; two phases are larger than a chunk,
  two chunk edges fall inside entries, and the total exceeds the RAM window.
* ``bc2a3bc8ad11``: the GLM layer-44 Stage B executable readset.  10,344
  entries in 22 phases, 138.08 GiB.  No phase exceeds a chunk and the total
  fits the RAM window, so it exercises neither the in-entry chunk edge nor
  the RAM slide; it is the breadth table.

The harness is ``tools/fleet/shape_gate.py``; its own tests are
``tests/test_the_campaign_shape_harness.py``.  On a pass each result is
printed as one JSON line, so the run's log carries what was exercised and
what each tier moved.
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


@pytest.mark.parametrize("name", sorted(shape_gate.REFERENCE_TABLES))
def test_the_reference_campaign_shape_stages_and_reads_back(
        name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str]) -> None:
    spec = shape_gate.REFERENCE_TABLES[name]
    path = shape_gate.TABLE_ROOT / f"{name}.json.gz"
    raw = path.read_bytes()
    # A changed table is a changed gate, and says so.
    assert hashlib.sha256(raw).hexdigest() == spec["sha256"]
    table = shape_gate.parse_table(raw, where=str(path))
    monkeypatch.setattr(storage_tiers, "GIB", shape_gate.UNIT_BYTES)

    result = shape_gate.run_gate(
        table, root=tmp_path / "gate", shared_root=Path(pbrun.SH),
        require_chunk_edge=bool(spec["chunk_edge_inside_entry"]))

    with capsys.disabled():
        print(json.dumps({"shape_gate": name, "result": result}, sort_keys=True))
    assert result["entries"] == len(table["entries"])
    assert (result["coverage"]["chunk_edge_inside_entry"]
            == spec["chunk_edge_inside_entry"])

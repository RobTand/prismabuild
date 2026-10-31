"""Production index cases and source checks before lead-owned capture adoption."""
from __future__ import annotations

import json
import shutil

import pytest

from prismabuild.digest_primitives import raw_sha256
from prismabuild.record_kinds import KINDS
from record_compat_contract import ROOT, generations
from record_compat import accepted, historical_absence, invoke, probe_wire, write


VARIANTS = ("leaf", "namespace-coverage", "bucket-coverage")


@pytest.mark.parametrize("variant", VARIANTS)
def test_publication_index_source_reproduces_and_reaches_current_consumer(variant, tmp_path):
    case = "publication-index/" + variant
    raw = write(ROOT, case, tmp_path / "first")
    assert raw == write(ROOT, case, tmp_path / "independent")
    body = json.loads(raw)
    assert body["schema"] == "prismabuild.publication_index.v1"
    expected = {"leaf": {"schema", "action_key", "published_unix", "facets"},
                "namespace-coverage": {"schema", "namespaces"},
                "bucket-coverage": {"schema", "entries", "namespaces"}}
    assert set(body) == expected[variant]
    observed = invoke(ROOT, case, tmp_path / "reader", raw=raw)
    assert accepted(observed) == {"action_key": "c" * 64, "complete": True}


@pytest.mark.parametrize("variant", VARIANTS)
def test_publication_index_source_unknown_fields_and_invalid_schema_reach_consumer(variant, tmp_path):
    case = "publication-index/" + variant
    kind = next(kind for kind in KINDS if kind.name == "publication-index")
    body = json.loads(write(ROOT, case, tmp_path / "writer"))
    body["compat_unknown_field"] = {"future": True}
    accepted(invoke(ROOT, case, tmp_path / "additive", raw=probe_wire(kind, body)))
    body["schema"] = "invalid"
    observed = invoke(ROOT, case, tmp_path / "invalid", raw=probe_wire(kind, body))
    assert observed["returncode"] == 1 and not observed.get("attestation_error"), observed
    assert observed["consumer_boundary"] and observed["catalogued_entry_verified"]


@pytest.mark.parametrize("variant", VARIANTS)
def test_publication_index_source_absence_checks_actual_complete_closures(variant, generations):
    for generation, old, manifest in generations:
        absence = historical_absence(old, "publication-index/" + variant, manifest)
        assert absence is not None
        provenance = json.loads(manifest.read_bytes())
        assert absence["generation"] == generation
        assert absence["receipt_sha256"] == provenance["receipt_sha256"]
        assert absence["closure_files_checked"] == len(provenance["files"])
        assert absence["absent_reader_modules"] == ["src/prismabuild/publication_index.py"]


def test_publication_index_source_absence_refuses_an_existing_wire(generations, tmp_path):
    _, old, manifest = generations[0]
    altered = tmp_path / "closure"
    shutil.copytree(old, altered)
    path = altered / "src/prismabuild/pool.py"
    path.chmod(0o644)
    path.write_bytes(path.read_bytes() + b'\nINDEX_WIRE = "prismabuild.publication_index.v1"\n')
    provenance = json.loads(manifest.read_bytes())
    provenance["files"]["src/prismabuild/pool.py"] = raw_sha256(path.read_bytes())
    synthetic = tmp_path / "mutant-manifest.json"
    synthetic.write_text(json.dumps(provenance))
    with pytest.raises(AssertionError, match="missing adapter module does not prove"):
        historical_absence(altered, "publication-index/leaf", synthetic)

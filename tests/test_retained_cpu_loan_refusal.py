"""Retained CPU readers must refuse real opportunistic loans without granting CPUs."""
from __future__ import annotations

import ast
import json
import shutil

import pytest

from record_compat_contract import ROOT, generations
import record_compat
from record_compat import accepted, driver_for, invoke, sha, write

CASE = "host-cpu-metadata/borrowed"
ORIGINAL = "f82bb7b65fee904a64b4e114b0c2171805efd5de9c68e59b932c356d82d44d6c"


@pytest.fixture(scope="module")
def actual_loan(tmp_path_factory):
    return write(ROOT, CASE, tmp_path_factory.mktemp("real-opportunistic-loan"))


def test_each_retained_reader_observes_its_real_refusal(actual_loan, generations, tmp_path):
    assert json.loads(actual_loan)["measurement"] is False
    for generation, old, manifest in generations:
        original = driver_for(old) / "compat_unversioned_adapters.py"
        before = original.read_bytes()
        observed = invoke(old, CASE, tmp_path / generation, raw=actual_loan, manifest=manifest)
        assert accepted(observed) == {"refusal": "borrow_evidence_unavailable", "physical_admission": False}
        proof = observed["loan_reader_fixture_derivation"]
        assert proof["original_adapter_sha256"] == ORIGINAL == sha(before)
        assert proof["derived_adapter_sha256"] == proof["observed_adapter_sha256"]
        assert proof["refusal_required"] is True
        assert proof["accepted_reasons"] == ["measurement_holder", "borrow_evidence_unavailable"]
        assert original.read_bytes() == before


def test_changed_derived_adapter_is_not_accepted(actual_loan, generations, tmp_path, monkeypatch):
    run = record_compat.subprocess.run

    def tamper(command, **kwargs):
        adapter = record_compat.Path(command[command.index("--adapters") + 1])
        path = adapter.with_name("compat_unversioned_adapters.py")
        path.write_bytes(path.read_bytes() + b"\n")
        return run(command, **kwargs)

    monkeypatch.setattr(record_compat.subprocess, "run", tamper)
    for generation, old, manifest in generations:
        observed = invoke(old, CASE, tmp_path / generation, raw=actual_loan, manifest=manifest)
        assert observed["returncode"] == 2 and observed["process_returncode"] == 0
        assert observed["attestation_error"] == "historical loan reader fixture differs from its derivation"
        with pytest.raises(AssertionError):
            accepted(observed)


def test_a_retained_decision_that_grants_capacity_fails_the_derived_assertion(actual_loan, generations, tmp_path):
    for generation, old, manifest in generations:
        mutant = tmp_path / generation / "mutant"
        shutil.copytree(old, mutant)
        source = mutant / "src/prismabuild/adaptive_cpu.py"
        original = source.read_text()
        controller = next(node for node in ast.parse(original).body if isinstance(node, ast.ClassDef)
                          and node.name == "Controller")
        decision = next(node for node in controller.body if isinstance(node, ast.FunctionDef)
                        and node.name == "decision")
        lines = original.splitlines(keepends=True)
        first = decision.body[0]
        lines.insert(first.lineno - 1, " " * first.col_offset + "return {}  # test-local admission mutation\n")
        source.chmod(0o644)
        source.write_text("".join(lines))
        provenance = json.loads(manifest.read_bytes())
        provenance["files"]["src/prismabuild/adaptive_cpu.py"] = sha(source.read_bytes())
        derived_manifest = tmp_path / generation / "mutant-manifest.json"
        derived_manifest.write_text(json.dumps(provenance))
        observed = invoke(mutant, CASE, tmp_path / generation / "reader", raw=actual_loan,
                          manifest=derived_manifest)
        assert observed["returncode"] == 1 and not observed.get("attestation_error"), observed
        assert observed["error"]["type"] == "AssertionError"
        assert any(call[3] == "Controller.decision" for call in observed["calls"])

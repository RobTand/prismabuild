"""Actual controller upgrade records and supported-generation consumers."""
import base64
import json

import pytest

from prismabuild.compatibility_window import WindowError, client_protocol, require_reachable

from record_compat_contract import ROOT, evidence, RecordContract, generations, writer_sources, pytest_generate_tests
from record_compat import accepted, invoke


pytestmark = pytest.mark.pbtest_capability("archival-replay")


class TestRecords(RecordContract):
    domain = "controller"
    family_group = "upgrade"


# This population is a separate codec control, not a historical compatibility case.
CURRENT_EXPORT_CASE = "upgrade-export/current-all-members"
CURRENT_EXPORT_REQUIRED = {"resource_broker.py", "resource_payload.py", "gpu_memory.py", "upgrade_client.py"}
CURRENT_EXPORT_OPTIONAL = {"gpu_capacity.py", "park_proof.py", "digest_primitives.py", "generation_receipt.py"}


@pytest.fixture(scope="module")
def current_all_members_export(generations, pytestconfig, tmp_path_factory):
    store = tmp_path_factory.mktemp("current-all-members-export")
    observed = invoke(ROOT, CURRENT_EXPORT_CASE, store / "writer",
                      evidence=evidence("current-all-members-export-writer.json"))
    result = accepted(observed)
    raw = base64.b64decode(result["raw"])
    population = {"case": CURRENT_EXPORT_CASE, "required_members": sorted(CURRENT_EXPORT_REQUIRED),
                  "optional_members": sorted(CURRENT_EXPORT_OPTIONAL),
                  "credit": "current export codec fixtures; no installed-member or cross-version acceptance"}
    assert result["fixture_population"] == population
    repeated = invoke(ROOT, CURRENT_EXPORT_CASE, store / "writer-repeat",
                      evidence=evidence("current-all-members-export-writer-repeat.json"))
    assert accepted(repeated) == result
    writer_evidence = pytestconfig.pluginmanager.get_plugin("compat-writer-evidence")
    writer_evidence.observe(observed, raw, fixture_origin=population)
    proof = writer_evidence.records[CURRENT_EXPORT_CASE]
    proof["population_controls"] = [{"phase": "current-writer-repeat", "observation": repeated}]
    return raw, store, proof


def test_current_all_members_export_reaches_the_current_writer_and_reader(current_all_members_export):
    raw, store, proof = current_all_members_export
    observed = invoke(ROOT, CURRENT_EXPORT_CASE, store / "current-reader", raw=raw,
                      evidence=evidence("current-all-members-export-current-reader.json"))
    version = accepted(observed)
    assert set(version["files"]) == CURRENT_EXPORT_REQUIRED | CURRENT_EXPORT_OPTIONAL
    assert observed["catalogued_entry_verified"] and observed["consumer_boundary"]
    proof["population_controls"].append({"phase": "current-self-read", "observation": observed})


def test_current_all_members_export_reaches_each_retained_exact_whitelist(current_all_members_export, generations):
    raw, store, proof = current_all_members_export
    for generation, old, manifest in generations:
        observed = invoke(old, CURRENT_EXPORT_CASE, store / generation / "retained-reader", raw=raw,
                          manifest=manifest, evidence=evidence(generation + "/current-all-members-export-refusal.json"))
        assert observed["returncode"] == 1 and not observed.get("attestation_error"), observed
        assert observed["error"] == {"type": "ValueError", "message": "invalid runtime export identity or members"}, observed
        assert observed["catalogued_operation"] == "read" and observed["catalogued_entry_verified"], observed
        assert observed["consumer_boundary"] and observed["compiled_source_code_verified"], observed
        assert observed["isolated"] and observed["no_site"] and observed["stdlib_imports"], observed
        assert observed["interpreter"]["sha256"] and observed["imports"]["upgrade_client"]["sha256"], observed
        protocol = client_protocol(old)
        captured = json.loads(manifest.read_bytes())
        assert observed["imports"]["upgrade_client"]["sha256"] == captured["files"][protocol["updater_member"]]
        proof["population_controls"].append({"phase": "retained-whitelist-refusal", "generation": generation,
                                             "observation": observed})


def test_current_all_members_source_refuses_the_frozen_designated_window(generations):
    window = json.loads((ROOT / "tests/compat/generations/window.json").read_bytes())
    assert window["complete"] is True
    selected = [(name, old, manifest) for name, old, manifest in generations
                if name == window["rollback"]["generation"]]
    assert len(selected) == 1
    name, old, manifest = selected[0]
    captured = json.loads(manifest.read_bytes())
    assert (name, captured["commit"]) == (window["rollback"]["generation"], window["rollback"]["commit"])
    assert set(client_protocol(ROOT)["present"]) == CURRENT_EXPORT_OPTIONAL
    with pytest.raises(WindowError, match="receipt-member recovery protocol"):
        require_reachable(ROOT, old)

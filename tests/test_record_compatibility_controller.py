"""Actual controller protocol writers and supported-generation consumers."""
from __future__ import annotations

import base64
from dataclasses import replace
import json
import os
import sys

import pytest

from prismabuild.compatibility_window import WindowError, client_protocol, require_reachable
from record_compat_contract import (HISTORY, ROOT, RecordContract, decoded, evidence, generations,
                                    pytest_generate_tests, runtime_observer, writer_sources)
from record_compat import accepted, archival_runtime, invoke, write
import record_compat


pytestmark = pytest.mark.pbtest_capability("archival-replay")


class TestRecords(RecordContract):
    domain = "controller"



def test_floor_sample_cannot_reuse_another_coverage_capture(tmp_path):
    case = "floor-sample/idle"
    raw = write(ROOT, case, tmp_path / "writer")
    observed = invoke(ROOT, case, tmp_path / "reader", raw=raw)
    assert accepted(observed)["allowed"] is True
    body = json.loads(raw)
    body["coverage_sha256"] = "0" * 64
    rejected = invoke(ROOT, case, tmp_path / "foreign-coverage", raw=json.dumps(body).encode())
    assert rejected["returncode"] == 1 and not rejected.get("attestation_error"), rejected
    assert rejected["catalogued_entry_verified"] and rejected["consumer_boundary"], rejected
    assert "sample coverage changed" in rejected["error"]["message"], rejected



@pytest.mark.parametrize("case,reason", [("floor-binding/registered", "mapping_unknown"),
                                       ("floor-sample/idle", "population_unknown")])
def test_legacy_floor_records_hold_without_current_authority(case, reason, generations, tmp_path):
    for generation, _, _ in generations:
        raw = decoded(HISTORY[generation, case])
        observed = invoke(ROOT, case, tmp_path / generation, raw=raw)
        result = accepted(observed)
        assert observed["catalogued_entry_verified"] and observed["consumer_boundary"], observed
        assert result["allowed"] is False and result["reason"] == reason, result


def historical_shape(generations, case):
    return next(row for row in generations if (row[0], case) in HISTORY)


def test_a_missing_archival_executable_does_not_fall_back_to_the_gate(generations, tmp_path):
    case = "shape-table/unhashed"
    generation, old, manifest = historical_shape(generations, case)
    runtime = replace(archival_runtime(), path=str(tmp_path / "missing-python"))
    store = tmp_path / "writer"
    observed = invoke(old, case, store, manifest=manifest, archival_runtime=runtime,
                      runtime_observe=runtime_observer("archival-missing-executable-control", generation, case))
    assert observed["returncode"] != 0 and observed["process_returncode"] is None
    assert observed["archival_runtime"]["observed"] is None
    assert "FileNotFoundError" in observed["attestation_error"]
    assert "result" not in observed and not store.exists()


def test_archival_executable_identity_mismatch_stops_before_the_writer(generations, tmp_path):
    case = "shape-table/unhashed"
    generation, old, manifest = historical_shape(generations, case)
    qualified = archival_runtime()
    runtime = replace(qualified, sha256="0" * 64)
    store = tmp_path / "writer"
    observed = invoke(old, case, store, manifest=manifest, archival_runtime=runtime,
                      runtime_observe=runtime_observer("archival-identity-mismatch-control", generation, case))
    assert observed["returncode"] != 0 and observed["process_returncode"] != 0
    binding = observed["archival_runtime"]
    assert binding["selected"]["sha256"] == "0" * 64
    assert binding["observed"]["sha256"] == qualified.sha256
    assert "explicit qualification" in binding["error"]
    assert "result" not in observed and not store.exists()


@pytest.mark.parametrize("native_only", [False, True], ids=["python-environment", "native-environment"])
def test_archival_writer_uses_the_selected_environment_after_ambient_change(generations, tmp_path, monkeypatch,
                                                                         native_only):
    case = "shape-table/unhashed"
    generation, old, manifest = historical_shape(generations, case)
    monkeypatch.setenv("COMPAT_ARCHIVAL_ENV_CONTROL", "selected")
    run = record_compat.subprocess.run

    def change_ambient_environment(command, **kwargs):
        if native_only:
            os.putenv("COMPAT_ARCHIVAL_ENV_CONTROL", "changed-before-spawn")
        else:
            monkeypatch.setenv("COMPAT_ARCHIVAL_ENV_CONTROL", "changed-before-spawn")
        return run(command, **kwargs)

    monkeypatch.setattr(record_compat.subprocess, "run", change_ambient_environment)
    observed = invoke(old, case, tmp_path / "writer", manifest=manifest,
                      archival_runtime=archival_runtime(),
                      runtime_observe=runtime_observer("archival-environment-snapshot-control", generation, case))
    assert observed["returncode"] == 0, observed
    assert base64.b64decode(accepted(observed)["raw"]) == decoded(HISTORY[generation, case])


def test_archival_writer_refuses_a_changed_child_environment(generations, tmp_path, monkeypatch):
    case = "shape-table/unhashed"
    generation, old, manifest = historical_shape(generations, case)
    monkeypatch.setenv("COMPAT_ARCHIVAL_ENV_CONTROL", "selected")
    run = record_compat.subprocess.run

    def change_child_environment(command, **kwargs):
        environment = dict(kwargs.get("env") or os.environ)
        environment["COMPAT_ARCHIVAL_ENV_CONTROL"] = "changed-before-spawn"
        return run(command, **{**kwargs, "env": environment})

    monkeypatch.setattr(record_compat.subprocess, "run", change_child_environment)
    store = tmp_path / "writer"
    observed = invoke(old, case, store, manifest=manifest, archival_runtime=archival_runtime(),
                      runtime_observe=runtime_observer("archival-environment-mismatch-control", generation, case))
    binding = observed["archival_runtime"]
    assert observed["returncode"] == 2 and observed["process_returncode"] != 0
    assert "explicit qualification" in binding["error"]
    assert binding["selected"]["environment_digest"] != binding["observed"]["environment_digest"]
    assert "result" not in observed and not store.exists()


def test_malformed_archival_stdout_refuses_a_complete_zero_exit_child(generations, tmp_path, monkeypatch):
    case = "shape-table/unhashed"
    generation, old, manifest = historical_shape(generations, case)
    monkeypatch.setattr(record_compat, "_ARCHIVAL_BOOTSTRAP",
                        'print("compat-archival-runtime: {}", flush=True)\n' + record_compat._ARCHIVAL_BOOTSTRAP)
    observed = invoke(old, case, tmp_path / "writer", manifest=manifest, archival_runtime=archival_runtime(),
                      runtime_observe=runtime_observer("archival-malformed-stdout-control", generation, case))
    assert observed["returncode"] == 2 and observed["process_returncode"] == 0
    assert "exactly one leading archival runtime record" in observed["archival_runtime"]["error"]
    assert observed["stdout"].count("compat-archival-runtime: ") == 2
    assert "result" not in observed


@pytest.mark.parametrize("operation", ["current-writer", "manifested-current-writer", "historical-reader"])
def test_archival_runtime_cannot_replace_the_gate_writer_or_reader(operation, generations, tmp_path):
    case = "shape-table/unhashed"
    generation, old, manifest = historical_shape(generations, case)
    raw = decoded(HISTORY[generation, case]) if operation == "historical-reader" else None
    with pytest.raises(ValueError, match="only valid for a manifested historical write"):
        invoke(old if raw is not None else ROOT, case, tmp_path / "store", raw=raw,
               manifest=manifest if operation != "current-writer" else None, archival_runtime=archival_runtime())


@pytest.mark.parametrize("variant", ["unhashed", "hashed"])
def test_historical_shape_write_keeps_its_gate_runtime_observation(variant, generations, tmp_path):
    case = "shape-table/" + variant
    generation, old, manifest = historical_shape(generations, case)
    observed = invoke(old, case, tmp_path / "old-writer", manifest=manifest,
                      runtime_observe=runtime_observer("historical-gate-writer", generation, case))
    prior = base64.b64decode(accepted(observed)["raw"])
    assert observed["python"] == sys.version and observed["interpreter"]["path"] == sys.executable
    immutable = decoded(HISTORY[generation, case])
    if sys.version_info[:2] == (3, 12):
        assert len(prior) == len(immutable)
        assert [index for index, pair in enumerate(zip(prior, immutable)) if pair[0] != pair[1]] == [9]
        assert prior[9] == 3 and immutable[9] == 255
    else:
        assert prior == immutable
    current = write(ROOT, case, tmp_path / "current-writer",
                    runtime_observe=runtime_observer("current-writer", None, case))
    for root, raw, operation, attestation in ((ROOT, prior, "forward-reader", None),
                                             (old, current, "reverse-reader", manifest)):
        observed = invoke(root, case, tmp_path / operation, raw=raw, manifest=attestation,
                          runtime_observe=runtime_observer(operation, generation, case, raw=raw))
        accepted(observed)
        assert observed["python"] == sys.version and observed["interpreter"]["path"] == sys.executable


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

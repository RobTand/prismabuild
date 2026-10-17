"""Actual negative controls, provenance refusals and accepted historical shapes."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
from record_compat import accepted, historical_absence, invoke, retained, write
from record_compat_contract import DATA, GOLDENS, decoded, evidence, generations


@pytest.mark.parametrize("case,member", [("fragment/ssd", "entry"), ("map/ssd", "entry"),
                                        ("landing/horizon", "range"), ("landing/horizon", "horizon"),
                                        ("material/ssd", "entry")])
def test_nested_closed_fields_are_rejected_by_retained_consumers(case, member, generations, tmp_path):
    body = json.loads(decoded(GOLDENS[case]))
    if member == "entry":
        value = next(iter(body["entries"].values()))
    elif member == "range":
        value = body["ranges"][0]
    else:
        value = body["horizon"]
    value["compat_unknown_field"] = True
    for generation, old, manifest in generations:
        observed = invoke(old, case, tmp_path / generation / "old", raw=json.dumps(body).encode(), manifest=manifest)
        assert observed["returncode"] == 1 and not observed.get("attestation_error"), observed


def test_actual_candidate_writer_mutation_fails_the_unchanged_old_consumer(generations, tmp_path):
    mutant = tmp_path / "mutant"
    shutil.copytree(ROOT / "src", mutant / "src", ignore=shutil.ignore_patterns("__pycache__"))
    source = mutant / "src/prismabuild/residency_map.py"
    before = source.read_text()
    needle = "return _write_atomic(Path(path), validate_landing(record))"
    assert before.count(needle) == 1
    source.write_text(before.replace(needle,
                      'return _write_atomic(Path(path), {**validate_landing(record), "compat_unknown_field": True})'))
    raw = write(mutant, "landing/horizon", tmp_path / "mutant-writer",
                evidence=evidence("mutant-writer.json"))
    assert raw != decoded(GOLDENS["landing/horizon"])
    for generation, old, manifest in generations:
        observed = invoke(old, "landing/horizon", tmp_path / generation / "old-mutant", raw=raw, manifest=manifest,
                          evidence=evidence(generation + "/mutant-old-consumer.json"))
        assert observed["returncode"] == 1 and not observed.get("attestation_error"), observed
        assert "compat_unknown_field" in observed["error"]["message"]
        assert any(call[1] == "read_landing" for call in observed["calls"])
        control = invoke(old, "landing/horizon", tmp_path / generation / "old-control",
                         raw=decoded(GOLDENS["landing/horizon"]), manifest=manifest)
        accepted(control)
        assert observed["driver_sha256"] == control["driver_sha256"]
        assert observed["adapters_sha256"] == control["adapters_sha256"]
        assert observed["imports"] == control["imports"]


def test_closure_tampering_refuses_even_when_the_old_reader_still_accepts(generations, tmp_path):
    generation, old, manifest = generations[0]
    altered = tmp_path / "altered"
    shutil.copytree(old, altered)
    source = altered / "src/prismabuild/residency_map.py"
    source.chmod(0o644)
    source.write_bytes(source.read_bytes() + b"\n")
    observed = invoke(altered, "landing/horizon", tmp_path / "store",
                      raw=decoded(GOLDENS["landing/horizon"]), manifest=manifest,
                      evidence=evidence("closure-hash-mutation.json"))
    assert observed["returncode"] == 2 and "unattested retained import" in observed["attestation_error"]


def test_pre_1018_historical_reader_refuses_actual_horizon_writer_but_forward_reads(tmp_path):
    with retained(DATA / "history-controls", history_control=True) as historical:
        generation, old, manifest = historical[0]
        original = write(old, "landing/ready", tmp_path / "history-writer", manifest=manifest,
                         evidence=evidence("history-pre-1018-writer.json"))
        accepted(invoke(ROOT, "landing/ready", tmp_path / "history-forward", raw=original,
                        evidence=evidence("history-pre-1018-forward.json")))
        current = write(ROOT, "landing/horizon", tmp_path / "horizon-writer")
        observed = invoke(old, "landing/horizon", tmp_path / "history-reverse", raw=current, manifest=manifest,
                          evidence=evidence("history-pre-1018-reverse-red.json"))
        assert observed["returncode"] == 1 and not observed.get("attestation_error"), observed
        assert "horizon" in observed["error"]["message"]
        assert any(call[1] == "read_landing" for call in observed["calls"])


def test_empty_legacy_teardown_stops_the_actual_start_barrier(generations, tmp_path):
    for generation, old, manifest in generations:
        for root, attestation, label in ((ROOT, None, "new"), (old, manifest, "old")):
            result = accepted(invoke(root, "gang-teardown/presence", tmp_path / generation / label,
                                     raw=b"{}\n", manifest=attestation,
                                     evidence=evidence(generation + "/empty-teardown-" + label + ".json")))
            assert result["termination_reason"] == "gang_torn_down"


@pytest.mark.parametrize("case,member,value", [("config-local-tier/source", "renewal_ceiling_s", "absent"),
                                              ("config-ram-tier/source", "promotion_chunk_gib", "absent"),
                                              ("config-ram-tier/source", "promotion_chunk_gib", None)])
def test_legacy_configuration_defaults_reach_both_policy_readers(case, member, value, generations, tmp_path):
    body = json.loads(decoded(GOLDENS[case]))
    if value == "absent":
        body.pop(member, None)
    else:
        body[member] = value
    for generation, old, manifest in generations:
        for root, attestation, label in ((ROOT, None, "new"), (old, manifest, "old")):
            accepted(invoke(root, case, tmp_path / generation / label,
                            raw=json.dumps(body).encode(), manifest=attestation))


@pytest.mark.parametrize("stamp", [None, "legacy", "absent"])
def test_legacy_outcome_without_numeric_generation_remains_visible(stamp, generations, tmp_path):
    body = json.loads(decoded(GOLDENS["q-outcome/executed"]))
    if stamp == "absent":
        body.pop("published_unix")
    else:
        body["published_unix"] = stamp
    for generation, old, manifest in generations:
        for root, attestation, label in ((ROOT, None, "new"), (old, manifest, "old")):
            result = accepted(invoke(root, "q-outcome/executed", tmp_path / generation / label,
                                     raw=json.dumps(body).encode(), manifest=attestation))
            assert result["status"] == "executed" and result["action_key"] == body["action_key"]


def test_pythonpath_poison_cannot_supply_a_retained_dependency(generations, tmp_path, monkeypatch):
    poison = tmp_path / "poison/prismabuild"
    poison.mkdir(parents=True)
    (poison / "__init__.py").write_text("raise RuntimeError('installed-package poison')\n")
    monkeypatch.setenv("PYTHONPATH", str(poison.parent))
    generation, old, manifest = generations[0]
    observed = invoke(old, "landing/horizon", tmp_path / "poison-control",
                      raw=decoded(GOLDENS["landing/horizon"]), manifest=manifest,
                      evidence=evidence("pythonpath-poison-control.json"))
    accepted(observed)
    assert all("site-packages" not in path for path in observed["sys_path"])
    assert str(poison.parent) not in observed["sys_path"]
    assert all(Path(row["path"]).is_relative_to(old) for row in observed["imports"].values())


def test_setup_reader_cannot_stand_in_for_the_old_consumer(generations, tmp_path):
    driver = tmp_path / "setup_only.py"
    driver.write_text('''import base64
def operate(request, store, load):
    module = load("prismabuild.residency_map")
    path = store / "landing.json"
    path.write_bytes(base64.b64decode(request["raw"]))
    cached = module.read_landing(path)
    request["_consumer_boundary"]()
    return cached
''')
    for generation, old, manifest in generations:
        observed = invoke(old, "landing/horizon", tmp_path / generation / "setup-only",
                          raw=decoded(GOLDENS["landing/horizon"]), manifest=manifest,
                          adapter_path=driver, evidence=evidence(generation + "/setup-only-control.json"))
        assert observed["returncode"] == 3 and not observed["catalogued_entry_verified"], observed
        assert any(call[1] == "read_landing" for call in observed["setup_calls"])
        assert not any(call[1] == "read_landing" for call in observed["calls"])
        assert observed["compiled_source_code_verified"]
        assert observed["selected_adapter_sha256"] == hashlib.sha256(driver.read_bytes()).hexdigest()


def test_forged_code_with_an_old_reader_filename_is_refused(generations, tmp_path):
    driver = tmp_path / "forged_reader.py"
    driver.write_text('''import types
def operate(request, store, load):
    module = load("prismabuild.residency_map")
    scope = {}
    exec(compile("def read_landing(path): return {'forged': True}", module.__file__, "exec"), scope)
    code = scope["read_landing"].__code__.replace(co_firstlineno=module.read_landing.__code__.co_firstlineno)
    module.read_landing = types.FunctionType(code, module.__dict__)
    request["_consumer_boundary"]()
    return module.read_landing(store / "incoming.json")
''')
    for generation, old, manifest in generations:
        observed = invoke(old, "landing/horizon", tmp_path / generation / "forged-reader",
                          raw=decoded(GOLDENS["landing/horizon"]), manifest=manifest,
                          adapter_path=driver, evidence=evidence(generation + "/forged-reader-control.json"))
        assert observed["returncode"] == 2, observed
        assert "executing code differs from compiled immutable source" in observed["attestation_error"]
        assert not observed["compiled_source_code_verified"]
        imported = observed["imports"]["prismabuild.residency_map"]
        assert imported["sha256"] == json.loads(manifest.read_bytes())["files"][imported["relative"]]


def test_a_missing_adapter_cannot_hide_an_existing_wire_kind(generations, tmp_path):
    _, old, manifest = generations[0]
    altered = tmp_path / "closure-with-unmapped-owner"
    shutil.copytree(old, altered)
    source = altered / "src/prismabuild/residency_map.py"
    source.chmod(0o644)
    source.write_bytes(source.read_bytes() + b'\nUNMAPPED_INPUT = "fleet.d38.plan.v1"\n')
    receipt = json.loads(manifest.read_bytes())
    receipt["files"]["src/prismabuild/residency_map.py"] = hashlib.sha256(source.read_bytes()).hexdigest()
    synthetic = tmp_path / "test-mutant-manifest.json"
    synthetic.write_text(json.dumps(receipt))
    with pytest.raises(AssertionError, match="missing adapter module does not prove"):
        historical_absence(altered, "input-d38-plan/target", synthetic)


def test_introduced_publication_namespace_is_not_a_codec_pass_for_an_old_queue_reader(generations, tmp_path):
    adapter = ROOT / "tests/compat/support/compat_absence_adapters.py"
    for generation, old, manifest in generations:
        store = tmp_path / generation
        store.mkdir()
        (store / "introduced-ending.json").write_bytes(decoded(GOLDENS["publication-ending/initial"]))
        observed = invoke(old, "q-item/plain", store, raw=decoded(GOLDENS["q-item/plain"]),
                          manifest=manifest, adapter_path=adapter,
                          evidence=evidence(generation + "/introduced-publication-namespace.json"))
        result = accepted(observed)
        assert result["ready_keys"] and result["ending_codec_credit"] is False

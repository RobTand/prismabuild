"""Production file consumers qualify a pinned, provenance-bound support window."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools/maintenance"))
from record_compat import accepted, invoke, retained, write
from prismabuild.record_kinds import KINDS, cases

DATA = ROOT / "tests/compat"
def records(pattern):
    return [json.loads(line) for path in sorted(DATA.glob(pattern))
            for line in path.read_text().splitlines()]


GOLDENS = {row["case"]: row for row in records("goldens*.jsonl")}
HISTORY = {(row["generation"], row["case"]): row
           for row in records("historical*.jsonl")}
EVIDENCE = Path(os.environ["COMPAT_EVIDENCE_DIR"]) if os.environ.get("COMPAT_EVIDENCE_DIR") else None


def evidence(name):
    return EVIDENCE / name if EVIDENCE else None


def decoded(row):
    raw = base64.b64decode(row["raw"])
    assert hashlib.sha256(raw).hexdigest() == row["sha256"]
    return raw


@pytest.fixture(scope="module")
def generations():
    with retained() as result:
        yield result


@pytest.mark.parametrize("case", cases())
def test_supported_old_consumer_and_current_consumer_read_actual_writers(case, generations, tmp_path):
    label = case.replace("/", "-")
    current = write(ROOT, case, tmp_path / "new-writer", evidence=evidence(label + "-new-writer.json"))
    assert current == decoded(GOLDENS[case]), "current writer changed golden bytes"
    kind = next(kind for kind in KINDS if kind.name == case.split("/")[0])
    reader = kind.reader.split(":")[1].rsplit(".", 1)[-1]
    for generation, old, manifest in generations:
        historical = HISTORY[generation, case]
        assert historical["receipt_sha256"] == json.loads(manifest.read_bytes())["receipt_sha256"]
        prior = write(old, case, tmp_path / generation / "old-writer", manifest=manifest,
                      evidence=evidence(generation + "/" + label + "-old-writer.json"))
        assert prior == decoded(historical), "historical writer did not reproduce captured bytes"
        for root, raw, direction, attestation in ((ROOT, prior, "forward", None),
                                                  (old, current, "reverse", manifest)):
            observed = invoke(root, case, tmp_path / generation / direction, raw=raw, manifest=attestation,
                              evidence=evidence(generation + "/" + label + "-" + direction + ".json"))
            accepted(observed)
            assert any(call[1].lstrip("_") == reader.lstrip("_") for call in observed["calls"]), observed


@pytest.mark.parametrize("kind", KINDS, ids=lambda kind: kind.name)
def test_unknown_fields_follow_real_consumer_policy(kind, generations, tmp_path):
    case = kind.name + "/" + kind.variants[0]
    body = json.loads(decoded(GOLDENS[case]))
    body["compat_unknown_field"] = {"future": True}
    raw = (json.dumps(body, sort_keys=True) + "\n").encode()
    for generation, old, manifest in generations:
        for root, attestation, suffix in ((ROOT, None, "new"), (old, manifest, "old")):
            observed = invoke(root, case, tmp_path / generation / suffix, raw=raw, manifest=attestation,
                              evidence=evidence(generation + "/" + kind.name + "-unknown-" + suffix + ".json"))
            if kind.field_policy == "closed":
                assert observed["returncode"] == 1 and not observed.get("attestation_error"), observed
            else:
                accepted(observed)


@pytest.mark.parametrize("kind", KINDS, ids=lambda kind: kind.name)
def test_invalid_record_reaches_and_fails_both_real_consumers(kind, generations, tmp_path):
    case = kind.name + "/" + kind.variants[0]
    body = json.loads(decoded(GOLDENS[case]))
    if kind.negative_field is None:
        body = []
    else:
        body[kind.negative_field] = "compat.foreign.v999"
    raw = json.dumps(body).encode()
    for generation, old, manifest in generations:
        for root, attestation, suffix in ((ROOT, None, "new"), (old, manifest, "old")):
            observed = invoke(root, case, tmp_path / generation / suffix, raw=raw, manifest=attestation,
                              evidence=evidence(generation + "/" + kind.name + "-foreign-" + suffix + ".json"))
            assert observed["returncode"] == 1 and not observed.get("attestation_error"), observed
            reader = kind.reader.split(":")[1].rsplit(".", 1)[-1]
            assert any(call[1].lstrip("_") == reader.lstrip("_") for call in observed["calls"]), observed


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

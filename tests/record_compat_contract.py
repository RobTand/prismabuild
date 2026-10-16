"""Shared actual-consumer assertions; protocol files own their native populations."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
from record_compat import accepted, decode_wire, historical_absence, historical_writer, invoke, probe_wire, retained, write
from prismabuild.record_kinds import KINDS
from prismabuild.digest_primitives import canonical_sha256

DATA = ROOT / "tests/compat"
def records(pattern):
    return [json.loads(line) for path in sorted(DATA.glob(pattern))
            for line in path.read_text().splitlines()]


GOLDENS = {row["case"]: row for row in records("goldens*.jsonl")}
HISTORY = {(row["generation"], row["case"]): row
           for row in records("historical*.jsonl")}
ABSENT_PARTICIPANTS = {(row["generation"], row["case"]): row
                       for pattern in ("absent-inputs*.jsonl", "absent-participants*.jsonl") for row in records(pattern)}
EVIDENCE = Path(os.environ["COMPAT_EVIDENCE_DIR"]) if os.environ.get("COMPAT_EVIDENCE_DIR") else None


def evidence(name):
    return EVIDENCE / name if EVIDENCE else None


def decoded(row):
    raw = base64.b64decode(row["raw"])
    assert hashlib.sha256(raw).hexdigest() == row["sha256"]
    return raw


def has_historical_participant(case, generation, root, manifest):
    absence = historical_absence(root, case, manifest)
    if absence is None:
        assert (generation, case) not in ABSENT_PARTICIPANTS
        return True
    assert absence == ABSENT_PARTICIPANTS[generation, case]
    return False


@pytest.fixture(scope="module")
def generations():
    with retained() as result:
        yield result


@pytest.fixture(scope="module")
def writer_sources():
    with retained(DATA / "writer-sources", history_control=True) as result:
        yield result


def protocol_group(kind):
    owner = kind.reader.split(":", 1)[0]
    if kind.name.startswith("dc-"):
        return "decomposition"
    if owner in ("prismabuild.core", "prismabuild.client"):
        return "core"
    if owner == "prismabuild.pool":
        return "queue"
    if owner in ("prismabuild.produced_output", "prismabuild.produced_spool"):
        return "outputs"
    if owner in ("prismabuild.residency_map", "prismabuild.reader_lease", "prismabuild.storage_tiers",
                 "prismabuild.resident_sets", "prismabuild.local_resident", "prismabuild.residency_plan"):
        return "storage"
    if owner in ("prismabuild.local_scratch", "prismabuild._bounded_reader", "prismabuild.resource_scope",
                 "prismabuild._measurement_reservation", "prismabuild.checkout_recovery"):
        return "ownership"
    if owner.startswith(("prismabuild.adaptive", "prismabuild.container_images")):
        return "host"
    return "controller"


def pytest_generate_tests(metafunc):
    if metafunc.cls is None or not issubclass(metafunc.cls, RecordContract):
        return
    kinds = [kind for kind in KINDS if protocol_group(kind) == metafunc.cls.domain]
    if "variant" in metafunc.fixturenames:
        shapes = [(kind, variant) for kind in kinds if kind.variant_readers for variant in kind.variants[1:]]
        metafunc.parametrize("kind,variant", shapes, ids=[kind.name + "-" + variant for kind, variant in shapes])
    elif "kind" in metafunc.fixturenames:
        metafunc.parametrize("kind", kinds, ids=[kind.name for kind in kinds])
    elif "case" in metafunc.fixturenames:
        metafunc.parametrize("case", [kind.name + "/" + variant for kind in kinds for variant in kind.variants])


class RecordContract:
    def test_supported_consumers_read_exact_production_writes_or_immutable_source_inputs(self, case, generations, writer_sources, tmp_path):
        label = case.replace("/", "-")
        current = write(ROOT, case, tmp_path / "new-writer", evidence=evidence(label + "-new-writer.json"))
        assert current == decoded(GOLDENS[case]), "current writer changed golden bytes"
        kind = next(kind for kind in KINDS if kind.name == case.split("/")[0])
        readers = {reference.split(":")[1].rsplit(".", 1)[-1].lstrip("_")
                   for reference in (dict(kind.variant_readers).get(case.split("/", 1)[1], kind.reader),) + kind.legacy_readers}
        for generation, old, manifest in generations:
            if not has_historical_participant(case, generation, old, manifest):
                accepted(invoke(ROOT, case, tmp_path / generation / "current-input", raw=current))
                continue
            historical = HISTORY[generation, case]
            assert historical["receipt_sha256"] == json.loads(manifest.read_bytes())["receipt_sha256"]
            writer_root, writer_manifest, provenance = historical_writer(kind, old, manifest, writer_sources, case=case)
            assert historical.get("writer_source") == provenance
            prior = write(writer_root, case, tmp_path / generation / "old-writer", manifest=writer_manifest,
                          evidence=evidence(generation + "/" + label + "-old-writer.json"))
            assert prior == decoded(historical), "historical writer did not reproduce captured bytes"
            for root, raw, direction, attestation in ((ROOT, prior, "forward", None),
                                                      (old, current, "reverse", manifest)):
                observed = invoke(root, case, tmp_path / generation / direction, raw=raw, manifest=attestation,
                                  evidence=evidence(generation + "/" + label + "-" + direction + ".json"))
                accepted(observed)
                assert any(call[1].lstrip("_") in readers for call in observed["calls"]), observed


    def test_unknown_fields_follow_real_consumer_policy(self, kind, generations, tmp_path):
        case = kind.name + "/" + kind.variants[0]
        body = decode_wire(kind, decoded(GOLDENS[case]))
        body["compat_unknown_field"] = {"future": True}
        if kind.self_digest_field:
            body[kind.self_digest_field] = canonical_sha256({key: value for key, value in body.items()
                                                           if key != kind.self_digest_field})
        raw = probe_wire(kind, body)
        for generation, old, manifest in generations:
            readers = [(ROOT, None, "new")]
            if has_historical_participant(case, generation, old, manifest):
                readers.append((old, manifest, "old"))
            for root, attestation, suffix in readers:
                observed = invoke(root, case, tmp_path / generation / suffix, raw=raw, manifest=attestation,
                                  evidence=evidence(generation + "/" + case.replace("/", "-") + "-unknown-" + suffix + ".json"))
                if kind.field_policy == "closed":
                    assert observed["returncode"] == 1 and not observed.get("attestation_error"), observed
                else:
                    accepted(observed)


    def test_invalid_record_reaches_both_real_consumer_boundaries(self, kind, generations, tmp_path):
        case = kind.name + "/" + kind.variants[0]
        body = decode_wire(kind, decoded(GOLDENS[case]))
        if kind.negative_field is None:
            body = []
        else:
            body[kind.negative_field] = kind.negative_value
        raw = probe_wire(kind, body)
        for generation, old, manifest in generations:
            readers = [(ROOT, None, "new")]
            if has_historical_participant(case, generation, old, manifest):
                readers.append((old, manifest, "old"))
            for root, attestation, suffix in readers:
                observed = invoke(root, case, tmp_path / generation / suffix, raw=raw, manifest=attestation,
                                  evidence=evidence(generation + "/" + case.replace("/", "-") + "-foreign-" + suffix + ".json"))
                if kind.field_policy == "presence-only":
                    accepted(observed)
                    assert observed["result"]["body_decoded"] is False
                    continue
                assert observed["returncode"] == 1 and not observed.get("attestation_error"), observed
                readers = {reference.split(":")[1].rsplit(".", 1)[-1].lstrip("_")
                           for reference in (dict(kind.variant_readers).get(case.split("/", 1)[1], kind.reader),) + kind.legacy_readers}
                assert any(call[1].lstrip("_") in readers for call in observed["calls"]), observed



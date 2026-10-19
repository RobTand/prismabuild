"""Shared actual-consumer assertions; protocol files own their native populations."""
from __future__ import annotations

import base64
from functools import partial
import hashlib
import gzip
import json
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
from record_compat import accepted, archival_runtime, decode_wire, historical_absence, historical_writer, invoke, probe_wire, retained, write
from prismabuild.record_kinds import KINDS
from prismabuild.digest_primitives import canonical_sha256, raw_sha256
from compat_references import generation_reference

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


class WriterEvidence:
    def __init__(self):
        self.records = {}

    def observe(self, record, raw, *, fixture_origin=None):
        case = record["request"]
        assert case not in self.records
        fields = ("returncode", "isolated", "no_site", "compiled_source_code_verified", "catalogued_entry_verified",
                  "catalogued_operation", "imports", "calls", "executed_lines", "executing_code_sha256", "driver_sha256",
                  "selected_adapter_sha256", "request", "sys_path", "interpreter")
        self.records[case] = {key: record[key] for key in fields}
        self.records[case].update(wire_sha256=hashlib.sha256(raw).hexdigest(),
                                  fixture_origin=(GOLDENS[case].get("origin", "production writer")
                                                  if fixture_origin is None else fixture_origin))

    def pytest_terminal_summary(self, terminalreporter):
        for case, value in sorted(self.records.items()):
            raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
            encoded = base64.b64encode(gzip.compress(raw, mtime=0)).decode()
            terminalreporter.write_line("compat-writer-proof: " + encoded)


WRITER_EVIDENCE = None
RUNTIME_EVIDENCE = None


class OperationRuntimeEvidence:
    def __init__(self):
        running = Path('/proc/self/exe').read_bytes()
        assert running[:6] == b'\x7fELF\x02\x01'
        assert Path(sys.executable).read_bytes() == running
        machine = int.from_bytes(running[18:20], 'little')
        assert machine == {'x86_64': 62, 'aarch64': 183}[os.uname().machine]
        self.gate_runtime = {'version': list(sys.version_info[:3]), 'elf_machine': machine,
            'executable': {'path': sys.executable, 'resolved_path': str(Path(sys.executable).resolve()),
                           'bytes': len(running), 'sha256': raw_sha256(running)}}
        self.records = []
        self.outcomes = {}

    def observe(self, operation, generation, case, record, *, raw=None):
        fields = ("returncode", "process_returncode", "isolated", "no_site", "python", "machine", "affinity",
                  "interpreter", "archival_runtime", "compiled_source_code_verified", "catalogued_entry_verified",
                  "catalogued_operation", "consumer_boundary", "request", "imports", "stdlib_imports", "calls",
                  "executed_lines", "executing_code_sha256", "wire_identity_verified",
                  "caller_input_constructor_verified", "driver_sha256",
                  "selected_adapter_sha256", "attestation_error", "entry_error", "error", "stdout", "stderr",
                  "writer_context_producer", "writer_context", "reader_fixture_derivation", "adapters_sha256")
        observed = {key: record[key] for key in fields if key in record}
        if raw is None and isinstance(record.get("result"), dict) and "raw" in record["result"]:
            raw = base64.b64decode(record["result"]["raw"])
        observed.update(schema="prismabuild.compat_operation_runtime.v1", invocation=len(self.records),
                        operation=operation, generation=generation, case=case,
                        native=os.environ.get("PYTEST_CURRENT_TEST"),
                        gate_runtime={"path": sys.executable, "python": sys.version},
                        wire_sha256=hashlib.sha256(raw).hexdigest() if raw is not None else None)
        self.records.append(observed)

    def pytest_runtest_logreport(self, report):
        self.outcomes[report.nodeid, report.when] = report.outcome

    def pytest_terminal_summary(self, terminalreporter):
        terminalreporter.write_line("compat-gate-native-runtime: " + json.dumps(self.gate_runtime))
        for record in self.records:
            native = record["native"]
            record["native_outcome"] = (self.outcomes.get((native[:-7], "call"), "unavailable")
                                        if native and native.endswith(" (call)") else "unavailable")
            raw = json.dumps(record, sort_keys=True, separators=(",", ":")).encode()
            encoded = base64.b64encode(gzip.compress(raw, mtime=0)).decode()
            terminalreporter.write_line("compat-operation-runtime-proof: " + encoded)


def runtime_observer(operation, generation, case, *, raw=None):
    return partial(RUNTIME_EVIDENCE.observe, operation, generation, case, raw=raw)


@pytest.fixture(scope="module")
def generations(pytestconfig):
    global WRITER_EVIDENCE, RUNTIME_EVIDENCE
    WRITER_EVIDENCE = pytestconfig.pluginmanager.get_plugin("compat-writer-evidence")
    if WRITER_EVIDENCE is None:
        WRITER_EVIDENCE = WriterEvidence()
        pytestconfig.pluginmanager.register(WRITER_EVIDENCE, "compat-writer-evidence")
    RUNTIME_EVIDENCE = pytestconfig.pluginmanager.get_plugin("compat-operation-runtime-evidence")
    if RUNTIME_EVIDENCE is None:
        RUNTIME_EVIDENCE = OperationRuntimeEvidence()
        pytestconfig.pluginmanager.register(RUNTIME_EVIDENCE, "compat-operation-runtime-evidence")
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
    if owner in ("prismabuild.core", "prismabuild.client", "prismabuild.data_manifest"):
        return "core"
    if owner in ("prismabuild.pool", "prismabuild.funding"):
        return "queue"
    if owner in ("prismabuild.produced_output", "prismabuild.produced_spool",
                 "prismabuild.batch_ledger", "prismabuild.output_contract"):
        return "outputs"
    if owner in ("prismabuild.residency_map", "prismabuild.reader_lease", "prismabuild.storage_tiers",
                 "prismabuild.resident_sets", "prismabuild.local_resident", "prismabuild.residency_plan"):
        return "storage"
    if owner in ("prismabuild.local_scratch", "prismabuild.scratch_lifetime", "prismabuild._bounded_reader", "prismabuild.resource_scope",
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
        shapes = [(kind, variant) for kind in kinds if kind.variant_readers for variant, _ in kind.variant_readers]
        metafunc.parametrize("kind,variant", shapes, ids=[kind.name + "-" + variant for kind, variant in shapes])
    elif "kind" in metafunc.fixturenames:
        metafunc.parametrize("kind", kinds, ids=[kind.name for kind in kinds])
    elif "case" in metafunc.fixturenames:
        metafunc.parametrize("case", [kind.name + "/" + variant for kind in kinds for variant in kind.variants])


class RecordContract:
    def verify_variant_boundaries(self, kind, variant, generations, tmp_path):
        from dataclasses import replace
        selected = replace(kind, variants=(variant,),
                           field_policy=dict(kind.variant_field_policies).get(variant, kind.field_policy),
                           negative_field=dict(kind.variant_negative_fields).get(variant, kind.negative_field))
        self.test_unknown_fields_follow_real_consumer_policy(selected, generations, tmp_path / "unknown")
        self.test_invalid_record_reaches_both_real_consumer_boundaries(selected, generations, tmp_path / "foreign")

    def test_supported_consumers_read_exact_production_writes_or_immutable_source_inputs(self, case, generations, writer_sources, tmp_path):
        label = case.replace("/", "-")
        current = write(ROOT, case, tmp_path / "new-writer", evidence=evidence(label + "-new-writer.json"),
                        observe=WRITER_EVIDENCE.observe,
                        runtime_observe=runtime_observer("current-writer", None, case))
        assert current == decoded(GOLDENS[case]), "current writer changed golden bytes"
        kind = next(kind for kind in KINDS if kind.name == case.split("/")[0])
        readers = {reference.split(":")[1].rsplit(".", 1)[-1].lstrip("_")
                   for reference in (dict(kind.variant_readers).get(case.split("/", 1)[1], kind.reader),) + kind.legacy_readers}
        for generation, old, manifest in generations:
            if not has_historical_participant(case, generation, old, manifest):
                accepted(invoke(ROOT, case, tmp_path / generation / "current-input", raw=current,
                                runtime_observe=runtime_observer("current-input-reader", generation, case, raw=current)))
                continue
            historical = HISTORY[generation, case]
            assert historical["receipt_sha256"] == json.loads(manifest.read_bytes())["receipt_sha256"]
            writer_root, writer_manifest, provenance = historical_writer(kind, old, manifest, writer_sources, case=case)
            assert historical.get("writer_source") == provenance
            prior = write(writer_root, case, tmp_path / generation / "old-writer", manifest=writer_manifest,
                          evidence=evidence(generation + "/" + label + "-old-writer.json"),
                          archival_runtime=archival_runtime(),
                          runtime_observe=runtime_observer("archival-writer", generation, case))
            assert prior == decoded(historical), "historical writer did not reproduce captured bytes"
            for root, raw, direction, attestation, source in ((ROOT, prior, "forward", None, (writer_root, writer_manifest)),
                                                              (old, current, "reverse", manifest, None)):
                observed = invoke(root, case, tmp_path / generation / direction, raw=raw, manifest=attestation,
                                  writer_source=source,
                                  evidence=evidence(generation + "/" + label + "-" + direction + ".json"),
                                  runtime_observe=runtime_observer(direction + "-reader", generation, case, raw=raw))
                accepted(observed)
                readers = {generation_reference(root, reference).split(":")[1].rsplit(".", 1)[-1].lstrip("_")
                           for reference in (dict(kind.variant_readers).get(case.split("/", 1)[1], kind.reader),) + kind.legacy_readers}
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
                readers = {generation_reference(root, reference).split(":")[1].rsplit(".", 1)[-1].lstrip("_")
                           for reference in (dict(kind.variant_readers).get(case.split("/", 1)[1], kind.reader),) + kind.legacy_readers}
                assert any(call[1].lstrip("_") in readers for call in observed["calls"]), observed

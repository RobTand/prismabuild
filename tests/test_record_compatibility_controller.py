"""Actual controller protocol writers and supported-generation consumers."""
from __future__ import annotations

import base64
from dataclasses import replace
import sys

import pytest

from record_compat_contract import (HISTORY, ROOT, RecordContract, decoded, generations,
                                    pytest_generate_tests, runtime_observer, writer_sources)
from record_compat import accepted, archival_runtime, invoke, write


pytestmark = pytest.mark.pbtest_capability("archival-replay")


class TestRecords(RecordContract):
    domain = "controller"


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


@pytest.mark.parametrize("operation", ["current-writer", "historical-reader"])
def test_archival_runtime_cannot_replace_the_gate_writer_or_reader(operation, generations, tmp_path):
    case = "shape-table/unhashed"
    generation, old, manifest = historical_shape(generations, case)
    raw = decoded(HISTORY[generation, case]) if operation == "historical-reader" else None
    with pytest.raises(ValueError, match="only valid for a manifested historical write"):
        invoke(old if raw is not None else ROOT, case, tmp_path / "store", raw=raw,
               manifest=manifest if raw is not None else None, archival_runtime=archival_runtime())


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

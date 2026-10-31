"""Archival runtime selection and refusal controls."""
from __future__ import annotations

import base64
from dataclasses import replace
import os
import sys

import pytest

from record_compat_contract import HISTORY, ROOT, decoded, generations, runtime_observer
from record_compat import accepted, archival_runtime, invoke, write
import record_compat


pytestmark = pytest.mark.pbtest_capability("archival-replay")


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
    selected_tmpdir = str(tmp_path / "selected-tmp")
    monkeypatch.setenv("TMPDIR", selected_tmpdir)
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
    assert observed["archival_runtime"]["observed"]["environment_before_driver"]["TMPDIR"] == selected_tmpdir
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

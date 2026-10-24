"""Portable launch qualification controls, not historical writer acceptance.

Exercise the real archival bootstrap with this arm's interpreter and a private
probe driver. The driver intentionally exits 7 and emits no record bytes. Actual
archival replay, source attestation and golden comparisons remain namespace-lane
gates in test_record_compatibility_controller.py and its companion populations.
"""
from __future__ import annotations

import os
from pathlib import Path
import platform
import sys

import pytest

from prismabuild.digest_primitives import compact_ascii_json_bytes, raw_sha256

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
import record_compat


@pytest.fixture
def selected_environment_probe(tmp_path, monkeypatch):
    running = Path("/proc/self/exe").read_bytes()
    runtime = record_compat.ArchivalRuntime(
        path=sys.executable, resolved_path=str(Path("/proc/self/exe").resolve()),
        sha256=raw_sha256(running), python=sys.version, machine=platform.machine(),
        bytes=len(running), elf_class=running[4], elf_data=running[5],
        elf_machine=int.from_bytes(running[18:20], "little" if running[5] == 1 else "big"))
    driver = tmp_path / "probe-driver"
    driver.mkdir()
    (driver / "compat_isolate.py").write_text('''
import hashlib, json, os, platform, sys
from pathlib import Path
running = Path("/proc/self/exe").read_bytes()
store = Path(sys.argv[sys.argv.index("--store") + 1])
store.mkdir()
(store / "probe-reached").write_text("portable qualification control only")
print(json.dumps({
    "python": sys.version, "machine": platform.machine(),
    "affinity": sorted(os.sched_getaffinity(0)),
    "interpreter": {"path": sys.executable,
                    "resolved_path": str(Path("/proc/self/exe").resolve()),
                    "sha256": hashlib.sha256(running).hexdigest()},
    "probe_tmpdir": os.environ.get("TMPDIR"),
}))
sys.exit(7)
''')
    monkeypatch.setattr(record_compat, "driver_for", lambda root: driver)
    for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS"):
        monkeypatch.setenv(name, "1")
    monkeypatch.delenv("TZ", raising=False)
    root = tmp_path / "generation"
    root.mkdir()
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")

    def launch_probe(store):
        return record_compat.invoke(root, "shape-table/unhashed", store,
                                    manifest=manifest, archival_runtime=runtime)

    return launch_probe


def assert_selected_launch(observed, environment):
    binding = observed["archival_runtime"]
    selected = binding["selected"]
    assert selected["environment_before_driver"]["TMPDIR"] == environment.get("TMPDIR")
    assert selected["environment_digest"] == {
        "algorithm": "sha256", "domain": "prismabuild.compat_environment.v1",
        "encoding": "json-sort-keys-compact-ascii",
        "sha256": raw_sha256(b"prismabuild.compat_environment.v1\0" + compact_ascii_json_bytes(environment)),
    }
    return binding


@pytest.mark.parametrize("scratch", ["scratch-a", "scratch with spaces", None],
                         ids=["first-path", "second-path", "absent"])
def test_selected_tmpdir_qualifies_without_a_deployment_path(
        selected_environment_probe, tmp_path, monkeypatch, scratch):
    if scratch is None:
        monkeypatch.delenv("TMPDIR", raising=False)
    else:
        selected = tmp_path / scratch
        selected.mkdir()
        monkeypatch.setenv("TMPDIR", str(selected))
    environment = dict(os.environ)
    store = tmp_path / "probe-store"
    observed = selected_environment_probe(store)
    binding = assert_selected_launch(observed, environment)
    assert binding["selected"] == binding["observed"] and binding["error"] is None, observed
    assert observed["returncode"] == observed["process_returncode"] == 7, observed
    assert observed["probe_tmpdir"] == environment.get("TMPDIR")
    assert (store / "probe-reached").is_file()
    assert "result" not in observed  # No historical writer or golden success credit.


@pytest.mark.parametrize("native_only", [False, True], ids=["python-environment", "native-environment"])
def test_selected_tmpdir_snapshot_survives_later_ambient_mutation(
        selected_environment_probe, tmp_path, monkeypatch, native_only):
    selected = tmp_path / "selected-scratch"
    later = tmp_path / "later-scratch"
    selected.mkdir()
    later.mkdir()
    monkeypatch.setenv("TMPDIR", str(selected))
    environment = dict(os.environ)
    run = record_compat.subprocess.run

    def change_ambient_tmpdir(command, **kwargs):
        assert kwargs["env"] == environment
        if native_only:
            os.putenv("TMPDIR", str(later))
        else:
            monkeypatch.setenv("TMPDIR", str(later))
        return run(command, **kwargs)

    monkeypatch.setattr(record_compat.subprocess, "run", change_ambient_tmpdir)
    observed = selected_environment_probe(tmp_path / "probe-store")
    binding = assert_selected_launch(observed, environment)
    assert binding["selected"] == binding["observed"] and binding["error"] is None, observed
    assert observed["returncode"] == observed["process_returncode"] == 7, observed
    assert observed["probe_tmpdir"] == str(selected)
    assert binding["driver_tmpdir"] == {
        "value": str(tmp_path / "probe-store/tmp"),
        "basis": "source-derived from unchanged isolation driver; not observed",
    }
    assert "result" not in observed


def test_changed_child_tmpdir_is_rejected_before_the_probe_driver(
        selected_environment_probe, tmp_path, monkeypatch):
    selected = tmp_path / "selected-scratch"
    changed = tmp_path / "changed-scratch"
    selected.mkdir()
    changed.mkdir()
    monkeypatch.setenv("TMPDIR", str(selected))
    environment = dict(os.environ)
    run = record_compat.subprocess.run

    def change_child_tmpdir(command, **kwargs):
        assert kwargs["env"] == environment
        child = {**kwargs["env"], "TMPDIR": str(changed)}
        return run(command, **{**kwargs, "env": child})

    monkeypatch.setattr(record_compat.subprocess, "run", change_child_tmpdir)
    store = tmp_path / "probe-store"
    observed = selected_environment_probe(store)
    binding = assert_selected_launch(observed, environment)
    assert observed["returncode"] == 2 and observed["process_returncode"] != 0, observed
    assert "explicit qualification" in binding["error"]
    assert binding["observed"]["environment_before_driver"]["TMPDIR"] == str(changed)
    assert binding["selected"]["environment_digest"] != binding["observed"]["environment_digest"]
    assert os.environ["TMPDIR"] == str(selected)
    assert "result" not in observed and not store.exists()

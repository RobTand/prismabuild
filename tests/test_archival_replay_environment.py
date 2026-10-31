"""Archival launch qualification controls, separate from historical-byte replay."""
import hashlib
import json
import os
from pathlib import Path
import platform
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
from record_compat import archival_runtime, invoke
import record_compat


@pytest.mark.parametrize("tmpdir", [None, "", "relative/scratch"], ids=["unset", "empty", "relative"])
def test_archival_replay_requires_an_absolute_tmpdir(tmpdir, tmp_path, monkeypatch):
    if tmpdir is None:
        monkeypatch.delenv("TMPDIR", raising=False)
    else:
        monkeypatch.setenv("TMPDIR", tmpdir)
    store = tmp_path / "writer"
    with pytest.raises(ValueError, match="archival replay requires an absolute TMPDIR in the sealed environment"):
        invoke(tmp_path / "historical", "shape-table/unhashed", store,
               manifest=tmp_path / "manifest.json", adapter_path=tmp_path / "compat_adapters.py",
               archival_runtime=archival_runtime())
    assert not store.exists()


@pytest.fixture
def probe(tmp_path, monkeypatch):
    driver = tmp_path / "probe-driver"
    driver.mkdir()
    marker = driver / "ran"
    (driver / "compat_isolate.py").write_text(
        "import os, sys\nfrom pathlib import Path\n"
        "Path(__file__).with_name('ran').write_text(os.environ['TMPDIR'])\n"
        "sys.exit(7)\n")
    monkeypatch.setattr(record_compat, "HERE", driver)
    running = Path("/proc/self/exe").read_bytes()
    runtime = record_compat.ArchivalRuntime(
        path=sys.executable, resolved_path=str(Path("/proc/self/exe").resolve()),
        sha256=hashlib.sha256(running).hexdigest(), python=sys.version,
        machine=platform.machine(), bytes=len(running), elf_class=running[4],
        elf_data=running[5], elf_machine=int.from_bytes(
            running[18:20], "little" if running[5] == 1 else "big"))
    return runtime, marker


def invoke_probe(probe, tmp_path):
    runtime, _ = probe
    return invoke(tmp_path / "historical", "shape-table/unhashed", tmp_path / "writer",
                  manifest=tmp_path / "manifest.json", adapter_path=tmp_path / "compat_adapters.py",
                  archival_runtime=runtime)


@pytest.mark.parametrize("directory", ["first scratch", "second scratch"])
@pytest.mark.parametrize("native", [False, True], ids=["python-environment", "native-environment"])
def test_selected_tmpdir_survives_ambient_mutation(directory, native, probe, tmp_path, monkeypatch):
    selected = str(tmp_path / directory)
    monkeypatch.setenv("TMPDIR", selected)
    environment = dict(os.environ)
    run = record_compat.subprocess.run
    supplied = []

    def change_ambient(command, **kwargs):
        if native:
            os.putenv("TMPDIR", str(tmp_path / "later"))
        else:
            monkeypatch.setenv("TMPDIR", str(tmp_path / "later"))
        supplied.append(kwargs.get("env"))
        try:
            return run(command, **kwargs)
        finally:
            os.putenv("TMPDIR", selected)

    monkeypatch.setattr(record_compat.subprocess, "run", change_ambient)
    observed = invoke_probe(probe, tmp_path)
    binding = observed["archival_runtime"]
    assert supplied == [environment]
    assert binding["selected"]["environment_before_driver"]["TMPDIR"] == selected
    expected_digest = hashlib.sha256(
        b"prismabuild.compat_environment.v1\0" +
        json.dumps(environment, sort_keys=True, separators=(",", ":")).encode("ascii")).hexdigest()
    assert binding["selected"]["environment_digest"]["sha256"] == expected_digest
    assert binding["observed"] == binding["selected"]
    assert observed["process_returncode"] == 7
    assert probe[1].read_text() == selected
    assert observed["returncode"] == 2 and "result" not in observed


def test_changed_child_tmpdir_refuses_before_driver(probe, tmp_path, monkeypatch):
    selected, changed = (str(tmp_path / name) for name in ("selected scratch", "changed scratch"))
    monkeypatch.setenv("TMPDIR", selected)
    run = record_compat.subprocess.run

    def change_child(command, **kwargs):
        return run(command, **{**kwargs, "env": {**kwargs["env"], "TMPDIR": changed}})

    monkeypatch.setattr(record_compat.subprocess, "run", change_child)
    observed = invoke_probe(probe, tmp_path)
    binding = observed["archival_runtime"]
    assert binding["selected"]["environment_before_driver"]["TMPDIR"] == selected
    assert binding["observed"]["environment_before_driver"]["TMPDIR"] == changed
    assert binding["selected"]["environment_digest"] != binding["observed"]["environment_digest"]
    assert observed["returncode"] == 2 and observed["process_returncode"] != 0
    assert "explicit qualification" in binding["error"]
    assert not probe[1].exists() and "result" not in observed

"""Digest-pinned dependencies: the one owner module, tested by its own rules.

The contract (#1495): an exact-file requirement is one absolute path and one
sha256; an installed-distribution observation imports through a pinned
interpreter and is compared digest-for-digest -- the probe's *actual* imported
module against the descriptor's ``module_sha256`` -- so a correct-looking
module shadowed onto the import path refuses instead of running.  The probe's
payload composition is proven against an independently computed expectation
over a real installed distribution, never against itself.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import venv

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "dependency_digest", REPOSITORY / "src" / "prismabuild" / "dependency_digest.py")
dependency_digest = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(dependency_digest)          # type: ignore[union-attr]

PY = "/fleet/venvs/producer/bin/python"
GOOD = "a" * 64
OTHER = "b" * 64


def _sealed(files, observations=(), names=("x86_producer",), tags=("x86",)):
    return {"names": list(names), "tags": list(tags),
            "dependencies": [*files, *observations]}


# --------------------------------------------------------------------------
# The exact-file requirement: one validator, one digest
# --------------------------------------------------------------------------

def test_requirements_normalize_sort_and_dedupe():
    entries = [{"path": "/z", "sha256": GOOD}, {"path": "/a", "sha256": OTHER},
               {"path": "/a", "sha256": OTHER}]
    assert dependency_digest.validate_requirements(entries) == [
        {"path": "/a", "sha256": OTHER}, {"path": "/z", "sha256": GOOD}]


@pytest.mark.parametrize("entries", [
    [],
    "not a list",
    [{"path": "relative/bin/python", "sha256": GOOD}],
    [{"path": "/", "sha256": GOOD}],
    [{"path": "/x", "sha256": "A" * 64}],
    [{"path": "/x", "sha256": "zz" * 32}],
    [{"path": "/x", "sha256": GOOD, "mode": "executable"}],
    [{"path": "/x", "sha256": GOOD}, {"path": "/x", "sha256": OTHER}],
])
def test_a_requirement_that_is_not_one_path_one_digest_refuses_by_name(entries):
    with pytest.raises(ValueError, match="path|sha256|digest|nonempty|exactly"):
        dependency_digest.validate_requirements(entries)


def test_a_payload_importing_through_an_unpinned_interpreter_refuses():
    files = [{"path": "/elsewhere/tool", "sha256": GOOD}]
    observation = {"kind": "installed_distribution", "interpreter_path": PY,
                   "module": "producer.plan", "module_sha256": OTHER,
                   "distribution": "producer-quant",
                   "include_prefixes": ["producer/"],
                   "include_suffixes": [".py"], "sha256": OTHER}
    with pytest.raises(ValueError, match="no exact-file requirement pins"):
        dependency_digest.validate_observations([observation], files=files)


def test_a_sealed_selection_with_an_extra_field_refuses():
    sealed = _sealed([{"path": PY, "sha256": GOOD}])
    sealed["extra"] = 1
    assert dependency_digest.verify_sealed(
        {"files": [], "roots": [], "capabilities": sealed}) == 1


# --------------------------------------------------------------------------
# The shard preflight: refuses before pytest, never skips
# --------------------------------------------------------------------------

def _dependency(tmp_path: Path, payload: bytes = b"producer bytes") -> dict:
    path = tmp_path / "producer" / "tool"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return {"path": str(path),
            "sha256": hashlib.sha256(payload).hexdigest()}


def test_a_missing_dependency_refuses_naming_the_capability_and_path(
        tmp_path, capsys):
    entry = _dependency(tmp_path)
    entry["path"] = str(tmp_path / "producer" / "absent")
    assert dependency_digest.verify_sealed(
        {"files": [], "roots": [], "capabilities": _sealed([entry])}) == 1
    err = capsys.readouterr().err
    assert "x86_producer" in err and entry["path"] in err


def test_a_drifted_dependency_refuses_with_both_digests(tmp_path, capsys):
    entry = _dependency(tmp_path)
    entry["sha256"] = OTHER
    assert dependency_digest.verify_sealed(
        {"files": [], "roots": [], "capabilities": _sealed([entry])}) == 1
    err = capsys.readouterr().err
    assert OTHER in err
    assert hashlib.sha256(b"producer bytes").hexdigest() in err


def test_a_selection_without_capabilities_verifies_nothing(capsys):
    assert dependency_digest.verify_sealed({"files": [], "roots": []}) == 0
    assert dependency_digest.verify_sealed(None) == 0
    assert capsys.readouterr().out == ""


def test_an_intact_dependency_verifies_and_prints_its_evidence(tmp_path, capsys):
    entry = _dependency(tmp_path)
    assert dependency_digest.verify_sealed(
        {"files": [], "roots": [], "capabilities": _sealed([entry])}) == 0
    out = capsys.readouterr().out
    assert out.startswith(dependency_digest.EVIDENCE_PREFIX)
    evidence = json.loads(out[len(dependency_digest.EVIDENCE_PREFIX):])
    assert evidence["files"] == [{"path": entry["path"],
                                  "sha256": entry["sha256"]}]


# --------------------------------------------------------------------------
# The installed distribution: the consumer's composition, observed for real
# --------------------------------------------------------------------------

DIST_NAME = "producer-quant"
MODULE = "producer.plan"
INCLUDED = {"producer/__init__.py": b"VERSION = 1\n",
            "producer/plan.py": b"PLAN = 1\n",
            "producer/plan.json": b'{"t": 0}\n'}


def _payload_digest() -> str:
    digest = hashlib.sha256()
    for name in sorted(INCLUDED):
        digest.update(name.encode() + b"\0" + INCLUDED[name])
    return digest.hexdigest()


def _descriptor(venv_python: Path, **overrides) -> dict:
    entry = {"kind": "installed_distribution",
             "interpreter_path": str(venv_python),
             "module": MODULE, "module_sha256": None,
             "distribution": DIST_NAME,
             "include_prefixes": ["producer/"],
             "include_suffixes": [".py", ".json"],
             "sha256": _payload_digest()}
    entry.update(overrides)
    return entry


@pytest.fixture(scope="module")
def producer_env(tmp_path_factory):
    """One real venv carrying the smallest installed distribution the probe reads."""
    root = tmp_path_factory.mktemp("producer-venv")
    venv.create(root, with_pip=False)
    site = next((root / "lib").glob("python*/site-packages"))
    for name, payload in INCLUDED.items():
        target = site / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
    dist_info = site / f"{DIST_NAME.replace('-', '_')}-1.0.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text(
        f"Metadata-Version: 2.1\nName: {DIST_NAME}\nVersion: 1.0\n")
    (dist_info / "RECORD").write_text(
        "".join(f"{name},,\n" for name in sorted(INCLUDED)))
    return {"python": root / "bin" / "python", "site": site,
            "module_path": site / "producer" / "plan.py"}


@pytest.fixture(scope="module")
def producer_files(producer_env):
    """The pinned interpreter file entry the descriptor's import rides on."""
    payload = producer_env["python"].read_bytes()
    return [{"path": str(producer_env["python"]),
             "sha256": hashlib.sha256(payload).hexdigest()}]


def _plan_module_entry(producer_env) -> dict:
    payload = producer_env["module_path"].read_bytes()
    return {"path": str(producer_env["module_path"]),
            "sha256": hashlib.sha256(payload).hexdigest()}


def test_the_probe_reports_the_consumer_composition_over_a_real_distribution(
        producer_env, producer_files):
    entry = _descriptor(producer_env["python"])
    entry["module_sha256"] = _plan_module_entry(producer_env)["sha256"]
    observed = dependency_digest.observe_distribution(entry)
    # Independently derived, not read back from the module under test.
    digest = hashlib.sha256()
    for name in sorted(INCLUDED):
        path = producer_env["site"] / name
        digest.update(name.encode() + b"\0" + path.read_bytes())
    assert observed["package_payload_sha256"] == digest.hexdigest()
    assert observed["module_path"] == str(producer_env["module_path"])
    assert (observed["executable_sha256"]
            == hashlib.sha256(
                producer_env["python"].read_bytes()).hexdigest())


def test_a_full_seal_through_a_real_distribution_verifies(
        producer_env, producer_files, capsys):
    entry = _descriptor(producer_env["python"])
    entry["module_sha256"] = _plan_module_entry(producer_env)["sha256"]
    sealed = _sealed([*producer_files, _plan_module_entry(producer_env)],
                     [entry])
    assert dependency_digest.verify_sealed(
        {"files": [], "roots": [], "capabilities": sealed}) == 0
    assert producer_env["python"].name in capsys.readouterr().out


def test_a_module_sha256_that_the_import_does_not_match_refuses(
        producer_env, producer_files, capsys):
    entry = _descriptor(producer_env["python"], module_sha256=OTHER)
    sealed = _sealed([*producer_files, _plan_module_entry(producer_env)],
                     [entry])
    assert dependency_digest.verify_sealed(
        {"files": [], "roots": [], "capabilities": sealed}) == 1
    err = capsys.readouterr().err
    assert OTHER in err and MODULE in err


def test_a_shadowed_module_on_the_import_path_refuses(
        producer_env, producer_files, tmp_path, monkeypatch, capsys):
    """A correct-looking plan elsewhere on sys.path is not the pinned module.

    The descriptor pins the imported module's digest; a shadowing copy with
    different bytes refuses on the digest, and one outside the interpreter
    prefix refuses on the origin assertion -- either way before pytest.
    """
    entry = _descriptor(producer_env["python"])
    entry["module_sha256"] = _plan_module_entry(producer_env)["sha256"]
    sealed = _sealed([*producer_files, _plan_module_entry(producer_env)],
                     [entry])
    shadow = tmp_path / "shadow"
    (shadow / "producer").mkdir(parents=True)
    (shadow / "producer" / "__init__.py").write_bytes(b"VERSION = 0\n")
    (shadow / "producer" / "plan.py").write_bytes(b"# shadowed, not the pin\n")
    monkeypatch.setenv("PYTHONPATH", str(shadow))
    assert dependency_digest.verify_sealed(
        {"files": [], "roots": [], "capabilities": sealed}) == 1
    err = capsys.readouterr().err
    assert MODULE in err


def test_a_payload_digest_that_the_distribution_does_not_hash_to_refuses(
        producer_env, producer_files, capsys):
    entry = _descriptor(producer_env["python"], sha256=OTHER)
    entry["module_sha256"] = _plan_module_entry(producer_env)["sha256"]
    sealed = _sealed([*producer_files, _plan_module_entry(producer_env)],
                     [entry])
    assert dependency_digest.verify_sealed(
        {"files": [], "roots": [], "capabilities": sealed}) == 1
    err = capsys.readouterr().err
    assert OTHER in err and DIST_NAME in err

"""#2099: portable contract checks for the opt-in #725 binding adapter.

These are synthetic binding/installed-metadata fixtures, NOT PQ behavior,
review approval of a real pairing, or recovered historical receipt evidence.
No PQ import, install, network, historical Git object, or private Python needed.
The adapter API/JSON contract is deliberately separate from run_activation.py.
"""
from __future__ import annotations

import base64
import copy
import csv
import importlib.metadata
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest

from prismabuild.digest_primitives import canonical_sha256, raw_sha256

ROOT = Path(__file__).resolve().parents[2]
ADAPTER = ROOT / "integration/pb725/binding_preflight.py"
PQ = "a3f8d01a4fdb26cfa81c5f6879ae5b17af367c89"
PB = "059953bc3793f539600d333cd3311773e592b0e6"
TESSERA = "b40c93cb73745097e57a1ba4cf5b9eee166c759a"


def _binding_adapter_subject():
    assert ADAPTER.is_file(), (
        "#2099 requires a separate portable integration binding/preflight adapter; "
        "the historical run_activation.py launcher is not that adapter")
    spec = importlib.util.spec_from_file_location("pb725_binding_subject", ADAPTER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert callable(getattr(module, "preflight_binding", None)), (
        "adapter must expose preflight_binding(binding_path, *, verify_install)")
    return module


def _write_binding_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _approve_fixture_selection(root, document):
    # Synthetic review receipt binds the WHOLE selection, not just the commits.
    review = root / "receipts/review.json"
    _write_binding_json(review, {
        "schema": "prismabuild.pb725.binding_review.v1",
        "selection_sha256": canonical_sha256(document["selection"]),
        "scope": "integration-binding-only",
    })
    document["review"] = {"path": "receipts/review.json",
                          "sha256": raw_sha256(review.read_bytes())}
    _write_binding_json(root / "binding.json", document)


@pytest.fixture
def bound_inputs(tmp_path):
    root = tmp_path / "portable-closure"
    root.mkdir()
    files = {}
    for role in ("producer", "source", "render", "wire", "activation"):
        path = root / "sources" / (role + ".py")
        path.parent.mkdir(exist_ok=True)
        # If an adapter tries probing/importing the selected APIs, it fails.
        path.write_text("raise AssertionError('preflight must not execute PQ behavior')\n")
        files[role] = {"path": path.relative_to(root).as_posix(),
                       "sha256": raw_sha256(path.read_bytes())}
    payload = root / "inputs/operand.json"
    _write_binding_json(payload, {"fixture": "synthetic bounded operand"})
    files["operand"] = {"path": "inputs/operand.json",
                          "sha256": raw_sha256(payload.read_bytes())}
    receipt = root / "receipts/dependency.json"
    _write_binding_json(receipt, {"action_key": "1" * 64,
                                "scope": "dependency-only", "returncode": 0})
    document = {
        "schema": "prismabuild.pb725.binding_preflight.v1",
        "issue": 725,
        "selection": {
            "pq": {"commit": PQ},
            "pb_tools": {"commit": PB},
            "harness_baseline": "be674a35f30da209075e237a9d380afb857c05c2",
            "pb_installed_commit": PB,
            "pb_sdk_version": 1,
            "tessera_commit": TESSERA,
            "dependency_generation": {"pb_commit": PB, "sdk_version": 1,
                                      "identity": "synthetic-sdk1-generation"},
            "apis": {
                "producer": {"file": "producer", "symbol": "build_manifest",
                             "defaults": {"fixture_only": True}},
                "source": {"file": "source", "symbol": "_source_safe_open",
                           "defaults": {"source_authentication": None}},
                "render": {"file": "render", "symbol": "ProductionWeightCache.get",
                           "defaults": {"resident_only": False}},
                "wire": {"file": "wire", "symbol": "file_load_receipt", "defaults": {}},
                "activation": {"file": "activation", "symbol": "prefetch",
                               "defaults": {"allowed_tiers": "ram,ssd", "max_workers": 4}},
            },
            "operands": {"test_paths": ["integration/pb725/test_source_render_reader.py"],
                         "names": ["covered", "omitted"], "shape": [8, 4],
                         "render_format": "BF16", "prefetch_max_workers": 1},
            "files": files,
            "prerequisites": {
                "dependency_preflight": {"path": "receipts/dependency.json",
                                         "sha256": raw_sha256(receipt.read_bytes()),
                                         "action_key": "1" * 64}},
            "continuation": {
                "issues": [859, 864],
                "source_commit": "26ae2f4c66163d5317c2c072b3052de1c1c59d85",
                "source_paths": ["tests/fleet_acceptance_runner.py",
                                 "tests/fleet_acceptance_matrix.json",
                                 "tools/fleet_qualification_driver.py"],
                "artifact_references": ["pqfleet-r7red-20260920.json",
                                        "pqfleet-r8green-final-20260920.json"],
                "complete": False,
                "verification": "source-attributed-only",
            },
        },
    }
    _approve_fixture_selection(root, document)
    return root, document


@pytest.fixture
def installed_verifier(tmp_path, monkeypatch):
    """Real pin verifier; only discovery is scoped to an uninstalled fixture."""
    spec = importlib.util.spec_from_file_location(
        "pb725_real_pin_verifier", ROOT / "tools/fleet/pbtest_pins.py")
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    site = tmp_path / "synthetic-site"
    package = site / "prismabuild"
    package.mkdir(parents=True)
    origin = package / "__init__.py"
    origin.write_text("# synthetic RECORD-owned import origin, never imported\n")
    dist_path = site / "prismabuild-0.0.dist-info"
    dist_path.mkdir()
    (dist_path / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: prismabuild\nVersion: 0.0\n")
    direct = dist_path / "direct_url.json"
    _write_binding_json(direct, {"url": "https://example.invalid/prismabuild.git",
                                "vcs_info": {"vcs": "git", "commit_id": PB}})
    record = dist_path / "RECORD"

    def seal_record():
        with record.open("w", newline="", encoding="utf-8") as target:
            writer = csv.writer(target)
            for path in (origin, dist_path / "METADATA", direct):
                data = path.read_bytes()
                encoded = base64.urlsafe_b64encode(
                    bytes.fromhex(raw_sha256(data))).rstrip(b"=").decode()
                writer.writerow([path.relative_to(site).as_posix(),
                                 "sha256=" + encoded, len(data)])

    seal_record()
    dist = importlib.metadata.PathDistribution(dist_path)
    monkeypatch.setattr(verifier.metadata, "packages_distributions",
                        lambda: {"prismabuild": ["prismabuild"]})
    monkeypatch.setattr(verifier.metadata, "distribution", lambda name: dist)
    monkeypatch.setattr(verifier.importlib.util, "find_spec", lambda name: SimpleNamespace(
        origin=str(origin), submodule_search_locations=[str(package)]))
    calls = []

    def strict_verify(module, expected, **kwargs):
        calls.append((module, expected, kwargs))
        # No identity policy: reviewed binding cannot waive installed identity drift.
        assert not kwargs.get("identity_policy"), "no pin-policy bypass in this lane"
        return verifier.verify_install(module, expected, **kwargs)

    return SimpleNamespace(call=strict_verify, calls=calls, origin=origin,
                           direct=direct, record=record, seal=seal_record)


def _assert_binding_unqualified(result):
    assert result["status"] == "unqualified", result
    assert result["behavioral_red"] is False, result
    assert result["complete"] is False, result
    assert result["reason"], "setup refusal must name the missing/mismatched prerequisite"


def test_ready_preflight_retains_exact_selection_and_real_verifier_evidence(
        bound_inputs, installed_verifier):
    adapter = _binding_adapter_subject()
    root, document = bound_inputs
    result = adapter.preflight_binding(root / "binding.json", verify_install=installed_verifier.call)
    assert result["status"] == "ready", result
    assert result["complete"] is False  # preflight is never full-path acceptance
    assert result["behavioral_red"] is False
    assert result["selection"] == document["selection"]
    assert result["selection_sha256"] == canonical_sha256(document["selection"])
    assert result["binding_sha256"] == raw_sha256((root / "binding.json").read_bytes())
    assert result["installed"]["installed_commit"] == PB
    assert result["installed"]["origin"] == str(installed_verifier.origin)
    assert result["installed"]["verified_files"] == 3
    assert installed_verifier.calls == [("prismabuild", PB, {})]
    assert result["verified_files"] == document["selection"]["files"]
    assert result["verified_prerequisites"] == document["selection"]["prerequisites"]
    assert result["review"] == document["review"]


@pytest.mark.parametrize("damage", ["commit", "record", "origin", "unrecorded"])
def test_installed_refusals_delegate_to_existing_verifier_without_waiver(
        bound_inputs, installed_verifier, damage):
    adapter = _binding_adapter_subject()
    root, _ = bound_inputs
    if damage == "commit":
        direct = json.loads(installed_verifier.direct.read_text())
        direct["vcs_info"]["commit_id"] = "d" * 40
        _write_binding_json(installed_verifier.direct, direct)
        installed_verifier.seal()
    elif damage == "record":
        installed_verifier.origin.write_text("# changed after RECORD sealing\n")
    elif damage == "origin":
        # RECORD remains syntactically valid but doesn't own the import origin.
        installed_verifier.record.write_text(
            next(line for line in installed_verifier.record.read_text().splitlines()
                 if "METADATA" in line) + "\n")
    else:
        (installed_verifier.origin.parent / "unrecorded.py").write_text("# stray\n")
    result = adapter.preflight_binding(root / "binding.json", verify_install=installed_verifier.call)
    _assert_binding_unqualified(result)
    assert installed_verifier.calls == [("prismabuild", PB, {})]


@pytest.mark.parametrize("damage", ["missing-source", "changed-source", "changed-input",
                                    "missing-receipt", "changed-receipt", "missing-review"])
def test_closure_refusals_are_setup_not_behavioral_red(bound_inputs, damage):
    adapter = _binding_adapter_subject()
    root, _ = bound_inputs
    paths = {"missing-source": "sources/source.py", "changed-source": "sources/source.py",
             "changed-input": "inputs/operand.json", "missing-receipt": "receipts/dependency.json",
             "changed-receipt": "receipts/dependency.json", "missing-review": "receipts/review.json"}
    target = root / paths[damage]
    if damage.startswith("missing"):
        target.unlink()
    else:
        target.write_bytes(target.read_bytes() + b"\nchanged\n")
    calls = []
    result = adapter.preflight_binding(root / "binding.json", verify_install=lambda *args, **kw: calls.append(args))
    _assert_binding_unqualified(result)
    assert not calls, "check the reviewed closure before installed/behavioral verification"


@pytest.mark.parametrize("mutation", ["current-pair", "sdk", "api-default", "operand", "receipt-id"])
def test_review_binds_entire_selection_not_only_historical_commits(bound_inputs, mutation):
    adapter = _binding_adapter_subject()
    root, document = bound_inputs
    changed = copy.deepcopy(document)
    selection = changed["selection"]
    if mutation == "current-pair":
        selection["pq"]["commit"] = "6d8288adc94b3c4b624c143ed372fb6513f32784"
        selection["pb_installed_commit"] = "027103d9a8417e06c7f13356e58779a313cd7088"
        selection["pb_sdk_version"] = 5
    elif mutation == "sdk":
        selection["pb_sdk_version"] = 6
    elif mutation == "api-default":
        selection["apis"]["activation"]["defaults"]["allowed_tiers"] = "origin,ram,ssd"
    elif mutation == "operand":
        selection["operands"]["shape"] = [16, 4]
    else:
        selection["prerequisites"]["dependency_preflight"]["action_key"] = "2" * 64
    _write_binding_json(root / "binding.json", changed)  # deliberately NOT reviewed
    calls = []
    result = adapter.preflight_binding(root / "binding.json", verify_install=lambda *a, **kw: calls.append(a))
    _assert_binding_unqualified(result)
    assert not calls, "unreviewed pair/flags/operands must refuse before verification or behavior"


@pytest.mark.parametrize("field", ["apis", "operands", "dependency_generation", "prerequisites", "files"])
def test_incomplete_reviewed_selection_is_not_ready(bound_inputs, field):
    adapter = _binding_adapter_subject()
    root, document = bound_inputs
    del document["selection"][field]
    _approve_fixture_selection(root, document)
    result = adapter.preflight_binding(root / "binding.json", verify_install=lambda *a, **kw: {})
    _assert_binding_unqualified(result)


def test_relocated_closure_needs_no_git_archive_install_or_behavior_probe(
        bound_inputs, installed_verifier, tmp_path, monkeypatch):
    adapter = _binding_adapter_subject()
    root, _ = bound_inputs
    relocated = tmp_path / "moved binding with spaces"
    shutil.copytree(root, relocated)
    monkeypatch.chdir(tmp_path)

    def forbidden_process(*args, **kwargs):
        pytest.fail("binding preflight must not run Git, installers, probes or nested submissions")

    monkeypatch.setattr(subprocess, "Popen", forbidden_process)
    result = adapter.preflight_binding(relocated / "binding.json", verify_install=installed_verifier.call)
    assert result["status"] == "ready", result
    assert result["selection"]["continuation"]["complete"] is False
    assert result["selection"]["continuation"]["verification"] == "source-attributed-only"
    assert result["complete"] is False


def test_cli_missing_binding_is_portable_named_setup_outcome(tmp_path):
    _binding_adapter_subject()
    output = tmp_path / "preflight-result.json"
    completed = subprocess.run([
        sys.executable, str(ADAPTER), "--binding", str(tmp_path / "absent.json"),
        "--output", str(output)], cwd=tmp_path, capture_output=True, text=True, timeout=20)
    assert completed.returncode == 2, (completed.stdout, completed.stderr)
    assert output.is_file(), "CLI must retain its setup/unqualified result"
    _assert_binding_unqualified(json.loads(output.read_text()))

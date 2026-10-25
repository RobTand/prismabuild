"""Opt-in #725 closure preflight, not behavioral or campaign acceptance.

Review receipts are supplied review references, not a new approval authority.
Selected source is hashed, never imported or executed.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _preflight_load_file_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Use the shared primitives without adding checkout src/ to the import path:
# doing so would shadow the installed package whose ownership is being checked.
try:
    from prismabuild.digest_primitives import canonical_sha256, raw_sha256
except ModuleNotFoundError:
    _digests = _preflight_load_file_module(
        "pb725_digest_primitives", ROOT / "src/prismabuild/digest_primitives.py")
    canonical_sha256 = _digests.canonical_sha256
    raw_sha256 = _digests.raw_sha256


def _preflight_require(condition, message):
    if not condition:
        raise ValueError(message)


def _preflight_hex(value, length):
    return (isinstance(value, str) and len(value) == length
            and all(char in "0123456789abcdef" for char in value))


def _preflight_selection(selection):
    _preflight_require(isinstance(selection, dict), "selection must be an object")
    for role in ("pq", "pb_tools"):
        _preflight_require(isinstance(selection.get(role), dict)
                           and _preflight_hex(selection[role].get("commit"), 40),
                           f"missing exact {role} source commit")
    for field in ("harness_baseline", "pb_installed_commit", "tessera_commit"):
        _preflight_require(_preflight_hex(selection.get(field), 40),
                           f"missing exact {field}")
    sdk = selection.get("pb_sdk_version")
    _preflight_require(type(sdk) is int and sdk > 0, "missing PB SDK generation")
    generation = selection.get("dependency_generation", {})
    _preflight_require(isinstance(generation, dict)
                       and generation.get("pb_commit") == selection["pb_installed_commit"]
                       and generation.get("sdk_version") == sdk
                       and bool(generation.get("identity")), "dependency generation mismatch or missing")
    for field in ("files", "prerequisites", "operands", "apis", "continuation"):
        _preflight_require(isinstance(selection.get(field), dict) and selection[field],
                           f"missing required selection section: {field}")
    for role in ("producer", "source", "render", "wire", "activation"):
        api = selection["apis"].get(role, {})
        _preflight_require(isinstance(api, dict) and api.get("file") in selection["files"]
                           and isinstance(api.get("symbol"), str) and api["symbol"]
                           and isinstance(api.get("defaults"), dict),
                           f"missing API/default binding: {role}")
    continuation = selection["continuation"]
    _preflight_require(continuation.get("complete") is False
                       and continuation.get("verification") == "source-attributed-only"
                       and continuation.get("issues") == [859, 864]
                       and _preflight_hex(continuation.get("source_commit"), 40)
                       and bool(continuation.get("source_paths"))
                       and bool(continuation.get("artifact_references")),
                       "missing source-only continuation attribution")


def _preflight_reference_bytes(base, reference, label):
    _preflight_require(isinstance(reference, dict) and isinstance(reference.get("path"), str)
                       and reference["path"] and _preflight_hex(reference.get("sha256"), 64),
                       f"invalid {label} path/hash reference")
    path = base / reference["path"]
    data = path.read_bytes()
    _preflight_require(raw_sha256(data) == reference["sha256"], f"{label} hash mismatch: {path}")
    return data


def preflight_binding(binding_path, *, verify_install):
    """Check the reviewed closure before delegating strict installed verification.

    ``verify_install`` is the canonical verifier (injectable for discovery-only
    fixtures). No identity policy is passed, and no behavioral API is invoked.
    """
    result = {"status": "unqualified", "complete": False, "behavioral_red": False}
    phase = "binding"
    try:
        path = Path(binding_path).resolve()
        data = path.read_bytes()
        result["binding_sha256"] = raw_sha256(data)
        document = json.loads(data)
        _preflight_require(document.get("schema") == "prismabuild.pb725.binding_preflight.v1"
                           and document.get("issue") == 725, "unsupported binding schema/issue")
        selection = document.get("selection")
        _preflight_selection(selection)
        result["selection"] = selection
        result["selection_sha256"] = canonical_sha256(selection)
        phase = "review"
        review = document.get("review")
        receipt = json.loads(_preflight_reference_bytes(path.parent, review, "review"))
        _preflight_require(receipt.get("schema") == "prismabuild.pb725.binding_review.v1"
                           and receipt.get("scope") == "integration-binding-only"
                           and receipt.get("selection_sha256") == result["selection_sha256"],
                           "review does not bind the entire selection with integration-only scope")
        result["review"] = review
        phase = "source/input"
        for name, reference in selection["files"].items():
            _preflight_reference_bytes(path.parent, reference, name)
        result["verified_files"] = selection["files"]
        phase = "prerequisite"
        for name, reference in selection["prerequisites"].items():
            _preflight_require(_preflight_hex(reference.get("action_key"), 64),
                               f"missing prerequisite action identity: {name}")
            receipt = json.loads(_preflight_reference_bytes(path.parent, reference, name))
            _preflight_require(receipt.get("action_key") == reference["action_key"],
                               f"prerequisite action identity mismatch: {name}")
        result["verified_prerequisites"] = selection["prerequisites"]
        phase = "installed"
        result["installed"] = verify_install("prismabuild", selection["pb_installed_commit"])
        result["status"] = "ready"
    except (OSError, ValueError, TypeError, KeyError, AttributeError, ImportError) as exc:
        result["reason"] = f"{phase} setup refused: {exc}"
    return result


def _preflight_cli_verify(module, expected):
    verifier = _preflight_load_file_module("pb725_pin_verifier", ROOT / "tools/fleet/pbtest_pins.py")
    return verifier.verify_install(module, expected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", type=Path, required=True, help="Reviewed closure binding JSON")
    parser.add_argument("--output", type=Path, required=True, help="Retained preflight result JSON")
    args = parser.parse_args()
    result = preflight_binding(args.binding, verify_install=_preflight_cli_verify)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return 0 if result["status"] == "ready" else 2


if __name__ == "__main__":
    raise SystemExit(main())

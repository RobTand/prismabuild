"""The x86 pool definition must retain Tessera's schema-validation dependency.

Use the CPU-environment definition owned by PB #2111 rather than introducing
another provisioning path for #1650. This checks declared dependencies, not
live pool provisioning or execution of Tessera's correctness regression.
"""
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_x86_cpu_definition_includes_jsonschema_and_its_runtime_dependencies():
    definition_path = ROOT / "tools/maintenance/pb_cpu/x86_64-cp314.json"
    assert definition_path.is_file(), (
        "The #2111-owned x86 CPU dependency definition is absent. Reuse the "
        "existing pool-environment work; do not fix #1650 with an action-local "
        "install, a second manifest, or a weakened Tessera assertion."
    )
    definition = json.loads(definition_path.read_text(encoding="utf-8"))
    assert definition["machine"] == "x86_64"
    packages = {entry["name"].lower().replace("_", "-"): entry
                for entry in definition["packages"]}
    required = {"jsonschema", "attrs", "jsonschema-specifications",
                "referencing", "rpds-py"}
    assert required <= packages.keys(), (
        f"The x86 pool definition lacks schema-validation dependencies: "
        f"{required - packages.keys()}"
    )
    for name in required:
        package = packages[name]
        assert package.get("version"), f"{name} must have a reviewed version"
        assert package.get("reason"), f"{name} must explain its pool-test purpose"
        hashes = package.get("sha256")
        assert isinstance(hashes, list) and hashes, f"{name} needs wheel hashes"
        assert all(isinstance(digest, str) and len(digest) == 64
                   and all(character in "0123456789abcdef" for character in digest)
                   for digest in hashes), f"{name} has invalid wheel hashes"

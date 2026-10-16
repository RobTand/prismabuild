"""Read immutable operator configuration; never claim it has a runtime writer."""
from __future__ import annotations

import base64
import hashlib
from pathlib import Path


def operate(request, store, load):
    family = request["case"].split("/")[0]
    if family == "config-fleet":
        module = load("fleet_membership")
        directory = Path(module.__file__).resolve().parent
        source = directory / "fleet_boxes.json"
        reader = lambda path: module.roster_entry("dl380g10", path)
    else:
        module = load("prismabuild.resident_sets" if family == "config-local-tier" else "prismabuild.storage_tiers")
        root = Path(module.__file__).resolve().parents[2]
        filename = "local_tier_policy.json" if family == "config-local-tier" else "ram_tier_policy.json"
        source = next(path for path in (root / "tools/fleet" / filename, root / "tools" / filename) if path.is_file())
        reader = module.read_policy if family == "config-local-tier" else module.read_ram_policy
    if request["op"] == "write":
        raw = source.read_bytes()
        result = reader(source)
        assert result, "immutable configuration was refused by its actual reader"
        return {"raw": base64.b64encode(raw).decode(), "source_file": str(source),
                "source_sha256": hashlib.sha256(raw).hexdigest(),
                "origin": "immutable configuration; no runtime writer"}
    path = store / "policy.json"
    path.write_bytes(base64.b64decode(request["raw"]))
    result = reader(path)
    assert result, "configuration consumer refused its policy"
    return result

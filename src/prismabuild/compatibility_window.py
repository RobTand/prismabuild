"""Read-only support-window identity and rollback reachability checks."""
from __future__ import annotations

import ast
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path


class WindowError(ValueError):
    pass


def client_protocol(root):
    root = Path(root)
    candidates = (root / "tools/fleet/upgrade_client.py", root / "tools/upgrade_client.py")
    path = next((path for path in candidates if path.is_file()), None)
    if path is None:
        raise WindowError("generation has no updater source")
    values = {}
    for node in ast.parse(path.read_bytes()).body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    try:
                        values[target.id] = ast.literal_eval(node.value)
                    except (ValueError, TypeError):
                        pass
    optional = values.get("OPTIONAL_MEMBERS")
    if not isinstance(optional, dict):
        raise WindowError("updater optional-member contract is unknown")
    present = sorted(name for name, relative in optional.items() if (root / relative).is_file())
    return {"optional": sorted(optional), "present": present,
            "optional_protocol": values.get("CLIENT_UPGRADE_PROTOCOL"),
            "proof_protocol": values.get("CLIENT_UPGRADE_PROOF_MEMBER_PROTOCOL"),
            "updater_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def require_reachable(candidate, rollback):
    forward, target = client_protocol(candidate), client_protocol(rollback)
    union = set(forward["present"]) | set(target["present"])
    if union and any(row["optional_protocol"] != 2 for row in (forward, target)):
        raise WindowError("optional member transition requires dependency-aware protocol 2")
    if "park_proof.py" in union and any(row["proof_protocol"] != 2 for row in (forward, target)):
        raise WindowError("B09 proof member requires a protocol-2 bridge rollback target; a17477 is unreachable")
    return {"candidate": forward, "rollback": target,
            "physical_rollback": "separate qualification required"}


def require_predeploy(window, status, *, max_age_s=300, now=None):
    """Deployment must rerun this with a fresh census, never reuse capture freshness."""
    if (status.get("complete") is not True or status.get("timed_out_sections")
            or status.get("unavailable_sections")):
        raise WindowError("predeploy census is incomplete")
    now = datetime.now(timezone.utc).timestamp() if now is None else now
    if not 0 <= now - status["captured_unix"] <= max_age_s:
        raise WindowError("predeploy census is stale or in the future")
    live = [row for row in status["nodes"] if row.get("state") == "live"]
    if not live or any(row.get("runtime_commit") != window["rollback"]["commit"] for row in live):
        raise WindowError("live predeploy generation changed; re-designate and rerun the corpus")
    return tuple(sorted(row["node"] for row in live))


def require_generation(directory, designation):
    directory = Path(directory)
    raw = (directory / "RUNTIME_VERSION.json").read_bytes()
    value = json.loads(raw)
    if (directory.name != designation["generation"] or value["generation"] != designation["generation"]
            or value["commit"] != designation["commit"]
            or hashlib.sha256(raw).hexdigest() != designation["receipt_sha256"]):
        raise WindowError("rollback directory, commit or receipt digest changed")
    for relative, expected in value["files"].items():
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts or (directory / path).is_symlink():
            raise WindowError("unsafe rollback source path")
        if hashlib.sha256((directory / path).read_bytes()).hexdigest() != expected:
            raise WindowError(f"rollback source content changed: {relative}")
    return value

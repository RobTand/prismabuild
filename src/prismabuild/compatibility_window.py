"""Read-only support-window identity and rollback reachability checks."""
from __future__ import annotations

import ast
from datetime import datetime, timezone
import json
from pathlib import Path

from .digest_primitives import canonical_sha256, raw_sha256


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
            "updater_sha256": raw_sha256(path.read_bytes())}


def require_reachable(candidate, rollback):
    forward, target = client_protocol(candidate), client_protocol(rollback)
    union = set(forward["present"]) | set(target["present"])
    if union and any(row["optional_protocol"] != 2 for row in (forward, target)):
        raise WindowError("optional member transition requires dependency-aware protocol 2")
    if "park_proof.py" in union and any(row["proof_protocol"] != 2 for row in (forward, target)):
        raise WindowError("B09 proof member requires a protocol-2 bridge rollback target; a17477 is unreachable")
    return {"candidate": forward, "rollback": target,
            "physical_rollback": "separate qualification required"}


def require_predeploy(window, status, *, captured_unix=None, max_age_s=300, now=None):
    """Deployment must rerun this with a fresh census, never reuse capture freshness."""
    if (status.get("complete") is not True or status.get("timed_out_sections")
            or status.get("unavailable_sections")):
        raise WindowError("predeploy census is incomplete")
    now = datetime.now(timezone.utc).timestamp() if now is None else now
    captured = status.get("captured_unix") if captured_unix is None else captured_unix
    if type(captured) not in (int, float) or not 0 <= now - captured <= max_age_s:
        raise WindowError("predeploy census is stale or in the future")
    live = [row for row in status["nodes"] if row.get("state") == "live"]
    sources = window["generations"]
    rollback = window["rollback"]
    supported = {row["commit"] for row in sources}
    observed = {row.get("runtime_commit") for row in live}
    identities = {(row["generation"], row["commit"]) for row in sources}
    if (window.get("complete") is not True or len(identities) != len(sources)
            or (rollback["generation"], rollback["commit"]) not in identities):
        raise WindowError("window does not contain its exact designated rollback identity")
    if not live or not observed <= supported:
        raise WindowError("live window changed; recapture and rerun the corpus without relabeling rollback")
    if supported != observed | {rollback["commit"]}:
        raise WindowError("retained directories outside live-plus-rollback are not supported window members")
    return tuple(sorted(row["node"] for row in live))


def content_digest(files):
    """Identity of the complete receipt file map, after checking each actual byte."""
    return canonical_sha256(files)


def require_generation(directory, designation):
    directory = Path(directory)
    if directory.is_symlink():
        raise WindowError("rollback generation must be an immutable directory, not a symlink")
    raw = (directory / "RUNTIME_VERSION.json").read_bytes()
    value = json.loads(raw)
    if (directory.name != designation["generation"] or value["generation"] != designation["generation"]
            or value["commit"] != designation["commit"]
            or raw_sha256(raw) != designation["receipt_sha256"]
            or content_digest(value["files"]) != designation["content_sha256"]):
        raise WindowError("rollback directory, commit or receipt digest changed")
    for relative, expected in value["files"].items():
        path = Path(relative)
        if (path.is_absolute() or ".." in path.parts
                or any((directory / parent).is_symlink() for parent in (path, *path.parents))):
            raise WindowError("unsafe rollback source path")
        if raw_sha256((directory / path).read_bytes()) != expected:
            raise WindowError(f"rollback source content changed: {relative}")
    return value

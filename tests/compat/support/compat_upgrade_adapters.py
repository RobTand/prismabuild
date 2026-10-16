"""Updater codecs and private write-once rollout files, with no service I/O."""
from __future__ import annotations

import base64
import json
from pathlib import Path
import runpy

EPOCH = "7" * 32


def intent(module):
    return {"schema": module.ROLLOUT_INTENT_SCHEMA, "epoch": EPOCH,
            "from_generation": "compat-source", "to_generation": "compat-target", "roster": ["compat-host"],
            "agent_sha256": "a" * 64, "coordinator_sha256": "b" * 64,
            "drain_policy": "wait", "armed_unix": 99.0, "armed_by": "compat"}


def marker(module, declared, variant):
    phase, _, direction = variant.partition("-") if variant.startswith(("resume-", "terminal-")) else (variant, "", "")
    if phase in module.ROLLOUT_HOST_PHASES:
        generation = "compat-source" if phase in ("drained", "rolled-back") else "compat-target"
        evidence = {"generation": generation, "posted_unix": 100.0}
        if phase == "drained":
            evidence.update(active_scopes=0, rollout_protocol=module.CLIENT_UPGRADE_ROLLOUT_PROTOCOL,
                            drain_changed_unix=99.0)
        if phase in ("rotated", "rolled-back", "resumed"):
            evidence["installed_agent_sha256"] = declared["agent_sha256"]
        if phase == "failed":
            evidence["error"] = "compat refusal"
        return module.marker_name("compat-host", phase), module.make_marker(declared, phase, host="compat-host", **evidence)
    direction = direction or ("rollback" if phase in ("rollback", "reverted") else "forward")
    evidence = {"generation": "compat-source" if direction == "rollback" else "compat-target",
                "decided_unix": 100.0, "decided_by": "compat", "direction": direction,
                "observed": {"compat-host": "c" * 64}}
    if phase == "rollback":
        evidence["reason"] = "compat rollback"
    if phase == "terminal":
        evidence["outcome"] = "rolled_back" if direction == "rollback" else "completed"
    return module.marker_name(None, phase), module.make_marker(declared, phase, **evidence)


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    if family in ("upgrade-intent", "upgrade-attestation", "upgrade-rollout"):
        return runpy.run_path(str(Path(__file__).with_name("compat_rollout_adapters.py")))["operate"](request, store, load)
    module = load("upgrade_client")
    write = request["op"] == "write"
    declared = intent(module)
    if family == "upgrade-export":
        if write:
            generation = store / "generations/compat-target"
            generation.mkdir(parents=True)
            source = Path(module.__file__).resolve()
            root = source.parents[2] if source.parent.name == "fleet" else source.parents[1]
            present = {name: relative for name, relative in module.OPTIONAL_MEMBERS.items()
                       if (root / relative).is_file()}
            members = {**module.MEMBERS, **(present if variant == "optional" else {})}
            files = {}
            for name, relative in members.items():
                # Bytes are deterministic supplied member fixtures. This
                # qualifies export integrity, never installed-member behavior.
                raw = ("compat member " + name + "\n").encode()
                path = generation / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(raw)
                files[relative] = module.digest(raw)
            (generation / "RUNTIME_VERSION.json").write_text(json.dumps({
                "schema": "prismaquant.prismabuild.runtime_version.v1", "generation": generation.name,
                "commit": "a" * 40, "files": files}))
            raw = module.encode_export({"runtime": str(generation), "generation_store": str(generation.parent)})
        else:
            request["_consumer_boundary"]()
            version, blobs = module.decode_export(base64.b64decode(request["raw"]))
            assert version["commit"] == "a" * 40 and set(module.MEMBERS) <= set(blobs)
            assert ("gpu_capacity.py" in blobs) == (variant == "optional")
            return version
    elif family == "upgrade-local":
        agent = module.Upgrader({"install_dir": str(store / "install"), "state_dir": str(store / "state")})
        snapshot = {"intent": declared, "intent_sha256": module.intent_sha256(declared)}
        if write:
            agent.state.mkdir(parents=True)
            agent._persist_rollout(snapshot, released=variant == "released")
            raw = agent.rollout_state.read_bytes()
        else:
            agent.rollout_state.parent.mkdir(parents=True)
            agent.rollout_state.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            result = agent._local_rollout()
            assert result and result["released"] == (variant == "released")
            return result
    elif family == "upgrade-marker":
        filename, value = marker(module, declared, variant)
        if write:
            assert module.post_marker(store / "rollout", filename, module.canonical_json(value))
            raw = (store / "rollout" / filename).read_bytes()
        else:
            request["_consumer_boundary"]()
            return module.validate_marker(filename, json.loads(base64.b64decode(request["raw"])), declared)
    else:
        raise ValueError(f"unknown updater record family: {family}")
    return {"raw": base64.b64encode(raw).decode()}

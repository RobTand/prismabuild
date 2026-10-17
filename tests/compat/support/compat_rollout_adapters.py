"""Actual rollout writers/readers in private trees, without service transitions."""
from __future__ import annotations

import base64
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import runpy
import uuid

EPOCH = "7" * 32
HOST = "compat-host"


def config(store):
    return {"runtime": str(store / "fleet/repo"), "generation_store": str(store / "fleet/runtime-generations"),
            "rollout_root": str(store / "fleet/rollout"), "install_dir": str(store / "install"),
            "state_dir": str(store / "state"), "maintenance_gate": str(store / "maintenance.json")}


def bootstrap(module, publisher, store):
    cfg = config(store)
    publisher.MIRROR = Path(cfg["runtime"])
    publisher.CHECKOUT = store / "source-loader-input"
    updater = Path(module.__file__).read_bytes()
    client_sha = module.digest(updater)
    source = publisher.CHECKOUT / "tools/fleet/upgrade_client.py"
    source.parent.mkdir(parents=True)
    source.write_bytes(updater)
    # The published closure has a flat tools export. Supply its already
    # imported, manifest-attested updater at the source-loader seam. The
    # actual coordinator still checks the private source copy's exact bytes.
    def selected_agent(*, expected_sha=None):
        assert expected_sha in (None, client_sha) and module.self_digest() == client_sha
        module._pb_source_sha256 = client_sha
        return module
    publisher._agent_definitions = selected_agent
    roster = json.dumps({"boxes": {HOST: {}}}).encode()
    members = {"tools/upgrade_client.py": updater,
               "tools/fleet/publish_runtime.py": Path(publisher.__file__).read_bytes(),
               "tools/fleet/fleet_boxes.json": roster}
    for name in ("compat-source", "compat-target"):
        root = Path(cfg["generation_store"]) / name
        for relative, raw in members.items():
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            path.chmod(0o444)
        path = root / "RUNTIME_VERSION.json"
        path.write_text(json.dumps({"schema": "prismaquant.prismabuild.runtime_version.v1", "generation": name,
            "commit": "a" * 40, "files": {key: module.digest(raw) for key, raw in members.items()}}))
        path.chmod(0o444)
        for directory in sorted((path for path in root.rglob("*") if path.is_dir()), reverse=True):
            directory.chmod(0o555)
        root.chmod(0o555)
    publisher.MIRROR.symlink_to(Path(cfg["generation_store"]) / "compat-source")
    offers = publisher.MIRROR.parent / "pb-queue/workers"
    offers.mkdir(parents=True)
    (offers / (HOST + ".json")).write_text(json.dumps({"schema": "prismaquant.prismabuild.pool_offer.v1",
                                                       "host": HOST, "announced_unix": 100.0}))
    agent = module.Upgrader(cfg, poster=lambda rel, raw: module.post_marker(Path(cfg["rollout_root"]), rel, raw))
    assert "error" not in agent.attest()
    return cfg


def snapshot_files(module, store, variant):
    cfg = config(store)
    source, target = (Path(cfg["generation_store"]) / name for name in ("compat-source", "compat-target"))
    source.mkdir(parents=True)
    target.mkdir()
    mirror = Path(cfg["runtime"])
    mirror.symlink_to(source)
    if variant == "none":
        return cfg
    declared = runpy.run_path(str(Path(__file__).with_name("compat_upgrade_adapters.py")))["intent"](module)
    root = Path(cfg["rollout_root"])
    assert module.post_marker(root, f"epochs/{EPOCH}/intent.json", module.canonical_json(declared))
    if variant == "armed":
        return cfg
    markers = {}

    def host(phase, generation):
        name = module.marker_name(HOST, phase)
        value = module.make_marker(declared, phase, host=HOST, generation=generation,
            posted_unix=100.0, active_scopes=0, drain_changed_unix=99.0,
            rollout_protocol=module.CLIENT_UPGRADE_ROLLOUT_PROTOCOL, installed_agent_sha256=declared["agent_sha256"])
        assert module.post_marker(root, f"epochs/{EPOCH}/{name}", module.canonical_json(value))
        markers[name] = value

    def decision(phase, generation, observed_phase, *, direction="forward"):
        observed = {HOST: module.marker_sha256(markers[module.marker_name(HOST, observed_phase)])}
        value = module.make_marker(declared, phase, generation=generation, direction=direction,
            decided_unix=100.0, decided_by="compat", observed=observed,
            **({"reason": "private refusal"} if phase == "rollback" else {}),
            **({"outcome": "completed" if direction == "forward" else "rolled_back"} if phase == "terminal" else {}))
        assert module.post_marker(root, f"epochs/{EPOCH}/{module.marker_name(None, phase)}", module.canonical_json(value))

    host("drained", source.name)
    if variant == "drained":
        return cfg
    decision("activated", target.name, "drained")
    mirror.unlink()
    mirror.symlink_to(target)
    if variant == "forward":
        return cfg
    reverse = variant in ("rollback", "completed-rollback")
    if reverse:
        decision("rollback", source.name, "drained", direction="rollback")
        mirror.unlink()
        mirror.symlink_to(source)
        decision("reverted", source.name, "drained", direction="rollback")
        if variant == "rollback":
            return cfg
    generation, phase, direction = ((source.name, "rolled-back", "rollback") if reverse
                                    else (target.name, "rotated", "forward"))
    host(phase, generation)
    decision("resume", generation, phase, direction=direction)
    host("resumed", generation)
    decision("terminal", generation, "resumed", direction=direction)
    return cfg


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    module = load("upgrade_client")
    write = request["op"] == "write"
    if family == "upgrade-rollout":
        if write:
            raw = module.encode_rollout(snapshot_files(module, store, variant), EPOCH if variant != "none" else None)
        else:
            request["_consumer_boundary"]()
            result = module.decode_rollout(base64.b64decode(request["raw"]))
            assert (result is None) == (variant == "none")
            return result
    elif family == "upgrade-intent" and not write:
        request["_consumer_boundary"]()
        return module.validate_intent(json.loads(base64.b64decode(request["raw"])), epoch=EPOCH)
    else:
        publisher = load("publish_runtime")
        if family == "upgrade-intent":
            cfg = bootstrap(module, publisher, store)
            publisher.FINAL_BARRIER_QUALIFICATION_GUARD = False
            publisher.uuid.uuid4 = lambda: uuid.UUID(EPOCH)
            publisher._wait_barrier = lambda epoch, *, wait_s: 0
            try:
                with redirect_stdout(io.StringIO()):
                    assert publisher._arm_barrier("compat-target", wait_s=0) == 0
                assert publisher.MIRROR.resolve().name == "compat-source"
                raw = (Path(cfg["rollout_root"]) / f"epochs/{EPOCH}/intent.json").read_bytes()
            finally:
                for path in Path(cfg["generation_store"]).rglob("*"):
                    if path.is_dir():
                        path.chmod(0o755)
        elif write:
            cfg = config(store)
            agent = module.Upgrader(cfg, poster=lambda rel, raw: module.post_marker(Path(cfg["rollout_root"]), rel, raw))
            assert "error" not in agent.attest()
            path = Path(cfg["rollout_root"]) / "agents" / module.attestation_name(HOST, agent.executing_sha256)
            raw = path.read_bytes()
            if variant == "repeated":
                assert "error" not in agent.attest() and path.read_bytes() == raw
        else:
            body = json.loads(base64.b64decode(request["raw"]))
            client = body["client_sha256"]
            publisher.MIRROR = store / "fleet/repo"
            path = publisher.MIRROR.parent / "rollout/agents" / module.attestation_name(HOST, client)
            path.parent.mkdir(parents=True)
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            result = publisher._attested_agents(module)
            assert result == {HOST: {client}}, "actual publisher ignored the attestation"
            return {host: sorted(values) for host, values in result.items()}
    return {"raw": base64.b64encode(raw).decode()}

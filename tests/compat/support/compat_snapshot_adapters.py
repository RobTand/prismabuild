"""Real private Git bundle snapshots and accepted v1 gate inputs."""
from __future__ import annotations

import base64
import json
import hashlib
import os
from pathlib import Path
import runpy
import subprocess
from types import SimpleNamespace


def git(root, *args):
    # Inherited Git identity variables override the repository's configured fixture identity.
    env = {**os.environ, "GIT_AUTHOR_NAME": "Compatibility fixture", "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
           "GIT_COMMITTER_NAME": "Compatibility fixture", "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
           "GIT_AUTHOR_DATE": "2000-01-01T00:00:00Z", "GIT_COMMITTER_DATE": "2000-01-01T00:00:00Z",
           "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"}
    return subprocess.check_output(["git", "-C", str(root), *args], env=env, stderr=subprocess.PIPE).decode().strip()


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    writing = request["op"] == "write"
    core, pbrun = load("prismabuild.core"), load("pbrun")
    if family == "checkout-v1-input":
        source = Path(pbrun.__file__).resolve().parent / "shape_gate.py"
        if writing and source.is_file():
            gate = load("shape_gate")
            owner = SimpleNamespace(root=Path("/compat/gate"), pbrun=pbrun)
            value = gate.ShapeGate._template(owner)["params"]["checkout_snapshot"]
        elif writing:
            value = {"schema": core.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1, "commit": "0" * 40,
                     "subdirectory": ".", "input": {"id": core.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID,
                     "sha256": hashlib.sha256(b"shape-gate-checkout").hexdigest(), "bytes": 4096}}
        else:
            value = json.loads(base64.b64decode(request["raw"]))
        if not writing:
            request["_consumer_boundary"]()
        value = core.validate_pbrun_checkout_snapshot(value)
        if not writing:
            assert value["subdirectory"] == "."
            return value
        return {"raw": base64.b64encode(core._canonical_file_bytes(value)).decode()}
    if not writing:
        request["_consumer_boundary"]()
        return core.validate_pbrun_checkout_snapshot(json.loads(base64.b64decode(request["raw"])))
    work = store / "work"
    work.mkdir()
    git(work, "init", "--quiet", "--template=")
    git(work, "config", "user.name", "Compatibility fixture")
    git(work, "config", "user.email", "fixture@example.invalid")
    (work / "payload").write_bytes(b"private snapshot fixture\n")
    git(work, "add", "payload")
    git(work, "commit", "--quiet", "-m", "fixture")
    if variant == "dirty":
        (work / "payload").write_bytes(b"dirty private bytes\n")
    cas = core.PrismaBuildCAS(store / "cas")
    value = load("prismabuild.submission").snapshot_checkout(work, cas=cas)
    body = runpy.run_path(str(Path(__file__).with_name("compat_core_adapters.py")))["action_body"](core, store)
    body["params"]["checkout_snapshot"] = value
    body["inputs"] = [value["input"]]
    action = core.seal_action(body)
    path = cas.publish_action_request(action)
    extract = runpy.run_path(str(Path(__file__).with_name("compat_batch_adapters.py")))["extract"]
    return {"raw": base64.b64encode(extract(path.read_bytes(), "checkout_snapshot")).decode()}

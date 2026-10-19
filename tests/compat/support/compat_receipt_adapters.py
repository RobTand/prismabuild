"""Actual local workers publish receipts; nested goldens are exact byte slices.

Host evidence is a synthetic input fixture. Loaded source, launcher and task
executable identities are measured from their actual files and never replaced.
The parent mounts each selected generation and private store at stable paths.
"""
from __future__ import annotations

import base64
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import runpy
import sys


def action_fixture(core, store):
    script = store / "compat-task.sh"
    script.write_bytes(b"#!/bin/sh\nprintf compat > result.bin\n")
    script.chmod(0o755)
    body = {"schema": core.ACTION_SCHEMA_V2,
            "task": {"definition_id": "compat/receipt", "definition_version": "v1",
                     "task_class": "generation", "determinism": "deterministic",
                     "artifact_family": "generic", "artifact_kind": "generic",
                     "argv": [str(script)], "working_directory": ".", "result_path": "result.bin"},
            "inputs": [], "code_closure": core.build_code_closure(store, [script.name]),
            "params": {}, "environment": {"variables": {}, "toolchain": {}},
            "execution_scope": {"portability": "portable", "platform_key": None, "host_class": None}}
    return core.seal_action(body)


def nested_bytes(raw, members):
    """Select a subdocument without reserializing the persisted writer bytes."""
    decoder = json.JSONDecoder()
    text = raw.decode("utf-8")
    for member in members:
        marker = json.dumps(member) + ":"
        assert text.count(marker) == 1, (member, "ambiguous persisted member")
        start = text.index(marker) + len(marker)
        while text[start].isspace():
            start += 1
        _, end = decoder.raw_decode(text, start)
        text = text[start:end]
    return text.encode("utf-8")


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    core = load("prismabuild.core")
    action = action_fixture(core, store)
    cas = core.PrismaBuildCAS(store / "cas")
    if request["op"] == "read":
        raw = base64.b64decode(request["raw"])
        if family == "core-runtime":
            request["_consumer_boundary"]()
            result = core._validate_worker_runtime(json.loads(raw))
            assert result["launch_kind"] == ("in_process" if variant == "in-process" else "script")
        elif family == "core-attestation":
            request["_consumer_boundary"]()
            result = core.validate_worker_attestation(json.loads(raw), action=action)
            assert result["action_key"] == action["action_key"]
        elif family == "core-claim":
            path = core._local_result_claim_path(cas, action, store)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            path.chmod(0o444)
            request["_consumer_boundary"]()
            result = core._load_local_result_claim(cas, action, store)
            assert result and result["action_key"] == action["action_key"]
        else:
            cas.ingest_bytes(b"compat", input_id="compat.result")
            path = cas.receipt_path(action["action_key"])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            path.chmod(0o444)
            request["_consumer_boundary"]()
            result = cas.lookup(action)
            assert result and result["result"]["bytes"] == 6, "receipt consumer refused its actual payload"
        return result

    # Keep the outer admitted action's broker/status/nonce out of this private
    # action. This is a child of admission, so its real payload executes directly.
    for key in (*core.ACTION_RESIDENCY_ENV, *core.ACTION_PROGRESS_ENV, core.ACTION_STATUS_PATH_ENV):
        os.environ.pop(key, None)
    core._collect_worker_evidence = lambda **kw: {
        "source": "local", "hostname": "compat-host", "system": "linux",
        "machine": "compat-arch", "libc": "compat-libc", "accelerators": [], "slurm": None}
    if variant == "in-process":
        result = core.run_local_action(action, cas_root=cas.root, checkout_root=store)
        assert result["status"] == "published", result
    else:
        root = Path(core.__file__).resolve().parents[2]
        path = cas.publish_action_request(action)
        sys.argv = [str(root / "tools/prismabuild_worker.py"), "run-local", "--action", str(path),
                    "--cas-root", str(cas.root), "--checkout-root", str(store)]
        output = io.StringIO()
        try:
            with redirect_stdout(output):
                runpy.run_path(sys.argv[0], run_name="__main__")
        except SystemExit as stop:
            assert stop.code == 0, stop.code
        assert json.loads(output.getvalue())["status"] == "published"
    assert (store / "result.bin").read_bytes() == b"compat"
    raw = cas.receipt_path(action["action_key"]).read_bytes()
    if family == "core-attestation":
        raw = nested_bytes(raw, ("producer",))
    elif family == "core-runtime":
        raw = nested_bytes(raw, ("producer", "runtime"))
    elif family == "core-claim":
        raw = core._local_result_claim_path(cas, action, store).read_bytes()
    return {"raw": base64.b64encode(raw).decode(),
            "fixture": "synthetic host facts; measured source/launcher/executable; actual shell payload"}

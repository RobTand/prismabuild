"""Actual private publisher bytes; authority and executable probes are fixture inputs."""
from __future__ import annotations

import base64
from contextlib import redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import stat
from types import SimpleNamespace
import uuid

NAME = "aaaaaaaaaaaa-100-777777777777"
MEMBER = "tools/fleet/stage_release.py"


def publisher_generation(store, load, variant):
    publisher = load("publish_runtime")
    source = store / "source"
    member = source / MEMBER
    member.parent.mkdir(parents=True)
    member.write_bytes(b"# private codec fixture member\n")
    publisher.CHECKOUT, publisher.MIRROR = source, store / "repo"
    facts = {"_commit_identity": lambda: "a" * 40, "_working_tree_dirty": lambda: False,
             "_publication_manifest": lambda: {MEMBER: hashlib.sha256(member.read_bytes()).hexdigest()},
             "_git_index_modes": lambda: {MEMBER: 0o100644},
             "_shape_gate_record": lambda *args, **kwargs: {"verdict": "waived", "reason": "codec fixture; no execution qualification"},
             "_roster_option_record": lambda *args: None, "_probe": lambda *args: None,
             "_sign_movement_approval": lambda *args, **kwargs: None}
    original = {key: getattr(publisher, key) for key in facts}
    nonce = uuid.uuid4
    try:
        for key, value in facts.items():
            setattr(publisher, key, value)
        uuid.uuid4 = lambda: SimpleNamespace(hex="7" * 32)
        args = SimpleNamespace(rollout="rolling", rollout_reason="private codec fixture", resume_barrier=None,
            rollback_barrier=None, rollback_reason=None, dry_run=False, stage_only=True, activate_generation=None,
            canary=False, no_canary=True, allow_dirty=False, default_transport=None if variant == "implicit" else variant,
            migrate_directory=False, barrier_wait_s=1.)
        with redirect_stdout(io.StringIO()):
            assert publisher._run_publication(args) == 0
    finally:
        uuid.uuid4 = nonce
        for key, value in original.items():
            setattr(publisher, key, value)
    return store / "runtime-generations" / NAME


def custody(publication, store):
    publication.PROTECTED_GENERATION_STORE = store / "protected/movement-generations"
    def private_custody(path):
        if not path.is_relative_to(store):
            return False
        for entry in (path, *path.parents):
            if entry == store:
                break
            info = entry.lstat()
            if info.st_mode & 0o022 or not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
                return False
        return True
    publication._protected_path = private_custody


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    writing = request["op"] == "write"
    publication = load("prismabuild.runtime_publication")
    if family == "runtime-receipt":
        if writing:
            if variant == "qualification":
                qualification, upgrade = load("qualify_rollout"), load("upgrade_client")
                source = store / "source"
                for member in set(upgrade.MEMBERS.values()) | {
                        upgrade.OPTIONAL_MEMBERS["park_proof.py"], "tools/fleet/publish_runtime.py"}:
                    target = source / member
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(b"# private qualification wire fixture member\n")
                generation, _ = qualification.build_generation(store, NAME, source, upgrade, ["compat-host"])
            else:
                generation = publisher_generation(store, load, variant)
            path = generation / "RUNTIME_VERSION.json"
        else:
            request["_consumer_boundary"]()
            result = publication._publication_receipt(base64.b64decode(request["raw"]), NAME).document
            assert result["files"] and result["generation"] == NAME
            return result
    else:
        source = publisher_generation(store, load, "implicit")
        custody(publication, store)
        root_authority = os.geteuid
        os.geteuid = lambda: 0
        try:
            protected = publication.publish_generation(source,
                receipt_sha256=hashlib.sha256((source / "RUNTIME_VERSION.json").read_bytes()).hexdigest())
        finally:
            os.geteuid = root_authority
        path = protected / publication.PUBLICATION_RECORD
        if not writing:
            protected.chmod(0o755)
            path.chmod(0o644)
            path.write_bytes(base64.b64decode(request["raw"]))
            path.chmod(0o444)
            protected.chmod(0o555)
            request["_consumer_boundary"]()
            result = publication._published_root(protected)
            assert result is not None
            return {"generation": result[0].document["generation"], "receipt_sha256": result[1]}
    return {"raw": base64.b64encode(path.read_bytes()).decode(),
            "fixture_facts": ["private manifest, Git and qualification facts; private custody model; no operational publication or root proof"]}

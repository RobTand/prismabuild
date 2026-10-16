from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

from prismabuild.compatibility_window import (
    WindowError, content_digest, require_generation, require_predeploy, require_reachable,
)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools/maintenance"))
from record_compat import retained
from compat_sources import capture_window


def test_designated_pre_b09_rollback_is_reachable_but_proof_member_requires_bridge(tmp_path):
    with retained() as generations:
        _, old, _ = generations[0]
        facts = require_reachable(ROOT, old)
        assert "park_proof.py" not in facts["candidate"]["optional"]
        candidate = tmp_path / "proof-candidate"
        path = candidate / "tools/fleet/upgrade_client.py"
        path.parent.mkdir(parents=True)
        code = (ROOT / "tools/fleet/upgrade_client.py").read_text()
        code = code.replace("OPTIONAL_MEMBERS = {", "OPTIONAL_MEMBERS = {'park_proof.py': 'src/prismabuild/park_proof.py', ")
        path.write_text(code + "\nCLIENT_UPGRADE_PROOF_MEMBER_PROTOCOL = 2\n")
        assert require_reachable(ROOT, candidate), "a bridge with no proof source must remain reachable"
        proof = candidate / "src/prismabuild/park_proof.py"
        proof.parent.mkdir(parents=True)
        proof.write_bytes((ROOT / "src/prismabuild/park_proof.py").read_bytes())
        with pytest.raises(WindowError, match="bridge rollback target"):
            require_reachable(candidate, old)


def test_predeploy_requires_fresh_complete_census_and_the_designated_generation():
    window = json.loads((ROOT / "tests/compat/generations/window.json").read_bytes())
    status = {"complete": True, "captured_unix": 1000,
              "nodes": [{"node": "one", "state": "live", "runtime_commit": window["rollback"]["commit"]},
                        {"node": "offline-wsl", "state": "retired", "runtime_commit": "0" * 40}]}
    assert require_predeploy(window, status, now=1001) == ("one",)
    with pytest.raises(WindowError, match="stale"):
        require_predeploy(window, status, now=2000)
    with pytest.raises(WindowError, match="incomplete"):
        require_predeploy(window, {**status, "complete": False}, now=1001)
    status["nodes"][0]["runtime_commit"] = "1" * 40
    with pytest.raises(WindowError, match="without relabeling"):
        require_predeploy(window, status, now=1001)
    status["nodes"][0]["runtime_commit"] = window["rollback"]["commit"]
    extra = {**window, "generations": [*window["generations"], {"generation": "unrelated", "commit": "2" * 40}]}
    with pytest.raises(WindowError, match="outside live-plus-rollback"):
        require_predeploy(extra, status, now=1001)
    with pytest.raises(WindowError, match="exact designated"):
        require_predeploy({**window, "rollback": {"generation": "other", "commit": "3" * 40}}, status, now=1001)


def generation(tmp_path):
    root = tmp_path / "generations" / "old-immutable"
    root.mkdir(parents=True)
    source = root / "src/prismabuild/reader.py"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"value = 42\n")
    receipt = {"generation": root.name, "commit": "1" * 40, "dirty": False,
               "files": {str(source.relative_to(root)): hashlib.sha256(source.read_bytes()).hexdigest()}}
    raw = json.dumps(receipt).encode()
    (root / "RUNTIME_VERSION.json").write_bytes(raw)
    designation = {"generation": root.name, "commit": receipt["commit"],
                   "receipt_sha256": hashlib.sha256(raw).hexdigest(),
                   "content_sha256": content_digest(receipt["files"])}
    return root, source, designation


def test_rollback_identity_checks_directory_receipt_commit_and_actual_content(tmp_path):
    root, source, designation = generation(tmp_path)
    assert require_generation(root, designation)["commit"] == "1" * 40
    source.write_bytes(b"value = 43\n")
    with pytest.raises(WindowError, match="content changed"):
        require_generation(root, designation)
    with pytest.raises(WindowError, match="commit or receipt"):
        require_generation(root, {**designation, "commit": "2" * 40})
    source.write_bytes(b"value = 42\n")
    moved = root / "private-reader.py"
    source.rename(moved)
    source.symlink_to(moved)
    with pytest.raises(WindowError, match="unsafe rollback"):
        require_generation(root, designation)


def test_window_capture_does_not_promote_retained_directories_or_offline_hosts(tmp_path):
    root, source, designation = generation(tmp_path)
    extra = root.parent / "unrelated-retained"
    extra.mkdir()
    receipt = json.loads((root / "RUNTIME_VERSION.json").read_bytes())
    receipt.update(generation=extra.name, commit="2" * 40)
    (extra / "RUNTIME_VERSION.json").write_text(json.dumps(receipt))
    status = tmp_path / "status.json"
    status.write_text(json.dumps({"complete": True, "nodes": [
        {"state": "live", "node": "active", "runtime_commit": designation["commit"]},
        {"state": "retired", "node": "offline-wsl", "runtime_commit": "2" * 40}]}))
    captured = capture_window(status, root.parent, root.name, designation["commit"], tmp_path / "out", ROOT)
    assert [row["generation"] for row in captured["generations"]] == [root.name]
    # A new live generation expands the union and cannot relabel the pinned rollback.
    target = extra / source.relative_to(root)
    target.parent.mkdir(parents=True)
    target.write_bytes(source.read_bytes())
    live = json.loads(status.read_bytes())
    live["nodes"][1]["state"] = "live"
    status.write_text(json.dumps(live))
    captured = capture_window(status, root.parent, root.name, designation["commit"], tmp_path / "expanded", ROOT)
    assert len(captured["generations"]) == 2 and captured["rollback"]["generation"] == root.name
    assert require_predeploy(captured, live, captured_unix=1000, now=1001) == ("active", "offline-wsl")
    # The status protocol reports commits only. Another immutable directory at
    # the same commit must refuse capture rather than guess which one is live.
    duplicate = root.parent / "same-commit-other-generation"
    duplicate.mkdir()
    (duplicate / "RUNTIME_VERSION.json").write_bytes((root / "RUNTIME_VERSION.json").read_bytes())
    with pytest.raises(ValueError, match="unambiguous generation"):
        capture_window(status, root.parent, root.name, designation["commit"], tmp_path / "ambiguous", ROOT)
    live["complete"] = False
    status.write_text(json.dumps(live))
    with pytest.raises(ValueError, match="incomplete"):
        capture_window(status, root.parent, root.name, designation["commit"], tmp_path / "refused", ROOT)

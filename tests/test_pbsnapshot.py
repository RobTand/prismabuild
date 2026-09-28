"""``pbsnapshot verify`` names the files PrismaBuild generated, and nothing else.

A client that hashes its own source leaves out the closure stamp ``pbrun``
wrote into the snapshot commit.  These tests hold the check to the rule that
matters for that client: an unverified stamp is refused, never reported as
"nothing generated", and a file that only looks like a stamp is not reported.

Every fixture is built under ``tmp_path``: a Git checkout at the path layout
``pbrun`` materializes (``<12 hex>.<suffix>/checkout``) and a sealed request in
a private CAS root.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
from prismabuild import core as pb  # noqa: E402
import pbrun  # noqa: E402
import pbsnapshot  # noqa: E402

OWNER = "e" * 64


def _git(root, *args):
    return subprocess.check_output(
        ["git", "-C", str(root), "-c", "user.name=Test",
         "-c", "user.email=test@example.invalid", *args]).decode().strip()


def _snapshot(tmp_path, name, *, extra=None, bad=None, version=1,
              schema_version=None):
    """A materialized pbrun checkout and its sealed request.

    ``bad`` breaks one link in the chain the verifier walks.
    """

    root = tmp_path / name / "pending" / "checkout"
    root.mkdir(parents=True)
    cas = tmp_path / name / "cas"
    _git(root, "init", "-q")
    (root / "source.py").write_text("same source\n")
    for path, value in (extra or {}).items():
        (root / path).write_text(value)
    stamp = {"cwd": ".", "head": "a" * 40, "dirty_sha256": "b" * 64}
    variables = {"PRISMABUILD_CONTAINER_OWNER": OWNER}
    command = ["python", "-m", "pytest", name]
    demand = {"cpu": 1, "mem_gb": 4}
    placement = {"required_tags": [name]}
    result, filename = pbrun.result_and_stamp_names(
        command, ".", demand, variables,
        identity={k: stamp[k] for k in ("head", "dirty_sha256")},
        placement=placement)
    if bad == "filename":
        filename = pb.PBRUN_STAMP_PREFIX + "0123456789abcdef.json"
    if bad == "extra-key":
        stamp["not_generated"] = True
    if bad == "cwd":
        stamp["cwd"] = "elsewhere"
    raw = json.dumps(stamp, indent=1, sort_keys=True).encode()
    (root / filename).write_bytes(raw)
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", f"PrismaBuild pbrun checkout snapshot v{version}")
    head = _git(root, "rev-parse", "HEAD")
    entry = {"path": filename, "bytes": len(raw),
             "sha256": hashlib.sha256(raw).hexdigest()}
    if bad == "closure-size":
        entry["bytes"] += 1
    if bad == "closure-hash":
        entry["sha256"] = "0" * 64
    closure = {"schema": pb.CODE_CLOSURE_SCHEMA_V1, "files": [entry]}
    closure["closure_sha256"] = pb.canonical_sha256(closure)
    if bad == "closure-null":
        closure = None
    snapshot_input = {"id": pb.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID, "bytes": 123,
                      "sha256": "c" * 64}
    schema = {1: pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1,
              2: pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V2}.get(
                  schema_version or version, "unknown")
    action = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                 "result_path": result},
        "params": {"command": command, "cwd": ".", "demand": demand,
                   "placement": placement, "checkout_snapshot": {
                       "schema": schema,
                       "commit": "0" * 40 if bad == "snapshot" else head,
                       "subdirectory": ".", "input": snapshot_input}},
        "inputs": [snapshot_input], "environment": {
            "variables": None if bad == "variables-null" else variables},
        "code_closure": closure,
    }
    action["action_key"] = pb.canonical_sha256(action)
    key = action["action_key"]
    moved = root.parent.with_name(f"{key[:12]}.fixture")
    root.parent.rename(moved)
    root = moved / "checkout"
    request = cas / "requests" / key[:2] / f"{key}.json"
    request.parent.mkdir(parents=True)
    request.write_text(json.dumps(action))
    return root, cas, request, filename


def _verify(fixture, **kwargs):
    root, cas, _, _ = fixture
    kwargs.setdefault("owner", OWNER)
    return pbsnapshot.verify(root, _git(root, "rev-parse", "HEAD"),
                             cas_root=cas, **kwargs)


@pytest.mark.parametrize("version", [1, 2])
def test_a_sealed_stamp_is_reported_with_the_action_that_wrote_it(tmp_path, version):
    fixture = _snapshot(tmp_path, "gpu", version=version)
    _, _, request, filename = fixture
    record = _verify(fixture)
    assert record["schema"] == "prismabuild.checkout_snapshot.v1"
    assert record["snapshot"] is True
    [stamp] = record["generated"]
    raw = (fixture[0] / filename).read_bytes()
    assert stamp == {"path": filename, "bytes": len(raw),
                     "sha256": hashlib.sha256(raw).hexdigest(),
                     "action_key": request.stem,
                     "request_sha256": hashlib.sha256(request.read_bytes()).hexdigest()}


def test_an_ordinary_commit_generated_nothing(tmp_path):
    root = tmp_path / "plain"
    root.mkdir()
    _git(root, "init", "-q")
    (root / "source.py").write_text("A\n")
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "ordinary source")
    record = pbsnapshot.verify(root, _git(root, "rev-parse", "HEAD"),
                               cas_root=tmp_path / "cas", owner=OWNER)
    assert record == {"schema": "prismabuild.checkout_snapshot.v1",
                      "snapshot": False, "generated": []}


def test_a_file_that_only_looks_like_a_stamp_is_not_generated(tmp_path):
    name = pb.PBRUN_STAMP_PREFIX + "ffffffffffffffff.json"
    fixture = _snapshot(tmp_path, "gpu", extra={
        name: json.dumps({"cwd": ".", "head": "a" * 40, "dirty_sha256": "c" * 64})})
    # Two closure-grammar files in a tree whose closure seals one: the sealed
    # one verifies, and the other is source the submitter committed.
    record = _verify(fixture)
    assert [row["path"] for row in record["generated"]] == [fixture[3]]
    assert name != fixture[3]


@pytest.mark.parametrize("bad", ["filename", "extra-key", "cwd", "closure-size",
                                 "closure-hash", "snapshot", "variables-null",
                                 "closure-null"])
@pytest.mark.parametrize("version", [1, 2])
def test_an_unverifiable_stamp_is_refused(tmp_path, bad, version):
    with pytest.raises(pbsnapshot.SnapshotRefused):
        _verify(_snapshot(tmp_path, "gpu", bad=bad, version=version))


@pytest.mark.parametrize("version,schema_version", [(3, 3), (2, 1)])
def test_an_unknown_or_mismatched_snapshot_version_is_refused(
        tmp_path, version, schema_version):
    with pytest.raises(pbsnapshot.SnapshotRefused, match="snapshot"):
        _verify(_snapshot(tmp_path, "gpu", version=version,
                          schema_version=schema_version))


def test_another_owner_or_no_owner_is_refused(tmp_path, monkeypatch):
    fixture = _snapshot(tmp_path, "gpu")
    with pytest.raises(pbsnapshot.SnapshotRefused, match="owner"):
        _verify(fixture, owner="f" * 64)
    monkeypatch.delenv("PRISMABUILD_CONTAINER_OWNER", raising=False)
    with pytest.raises(pbsnapshot.SnapshotRefused, match="owner"):
        _verify(fixture, owner=None)
    monkeypatch.setenv("PRISMABUILD_CONTAINER_OWNER", OWNER)
    assert _verify(fixture, owner=None)["snapshot"] is True


def test_a_missing_or_ambiguous_request_is_refused(tmp_path):
    fixture = _snapshot(tmp_path, "gpu")
    _, _, request, _ = fixture
    saved = request.read_bytes()
    request.unlink()
    with pytest.raises(pbsnapshot.SnapshotRefused, match="lookup"):
        _verify(fixture)
    request.write_bytes(saved)
    request.with_name(request.stem[:12] + "f" * 52 + ".json").write_bytes(saved)
    with pytest.raises(pbsnapshot.SnapshotRefused, match="lookup"):
        _verify(fixture)


def test_a_stamp_changed_after_materialization_is_refused(tmp_path):
    fixture = _snapshot(tmp_path, "gpu")
    root, _, _, filename = fixture
    (root / filename).write_text("{}")
    with pytest.raises(pbsnapshot.SnapshotRefused):
        _verify(fixture)


def test_the_command_prints_the_record_or_refuses_with_exit_1(tmp_path):
    fixture = _snapshot(tmp_path, "gpu")
    root, cas, _, _ = fixture
    commit = _git(root, "rev-parse", "HEAD")
    tool = ROOT / "tools" / "fleet" / "pbsnapshot.py"
    ok = subprocess.run(
        [sys.executable, str(tool), "verify", str(root), commit,
         "--cas-root", str(cas), "--owner", OWNER],
        capture_output=True, text=True, timeout=60)
    assert ok.returncode == 0, ok.stderr
    assert json.loads(ok.stdout) == _verify(fixture)
    refused = subprocess.run(
        [sys.executable, str(tool), "verify", str(root), commit,
         "--cas-root", str(cas), "--owner", "f" * 64],
        capture_output=True, text=True, timeout=60)
    assert refused.returncode == 1
    assert refused.stdout == ""
    assert "refused" in refused.stderr and "owner" in refused.stderr


def test_the_tool_is_published_with_the_generation():
    sys.path.insert(0, str(ROOT / "tools" / "fleet"))
    import publish_runtime

    assert "pbsnapshot.py" in publish_runtime.FLEET_SCRIPTS

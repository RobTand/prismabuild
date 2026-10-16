"""Drive captured roster policy through real hook, supervisor and history owners."""
from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from prismabuild import box_window, fleet_roster
import fleet_membership
import migrate_role_logs
from test_box_window_recorder_alias import START, recorder
from test_supervisor_declares_the_spool_budget import BASE_ARGS, _stat, roster, supervise
from test_the_coordinator_proves_the_fleet_before_a_barrier import (
    SHA, fleet, manifested_roster, post, publish_runtime,
)

ROOT = Path(__file__).resolve().parents[1]
MEMBER = "tools/fleet/fleet_boxes.json"


def write(root, document, *, flat=False):
    path = root / ("tools/fleet_boxes.json" if flat else MEMBER)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document))
    return path


@pytest.mark.parametrize("flat", [False, True], ids=["checkout-nested", "installed-flat"])
def test_membership_reads_both_layouts_without_using_alias_as_extra_authority(tmp_path, monkeypatch, flat):
    write(tmp_path, {"boxes": {"key": {"_alias": "os-name", "args": ["--class", "cpu"]}}}, flat=flat)
    monkeypatch.setattr(fleet_membership, "REPO_ROOT", tmp_path)
    for host in ("key", "os-name"):
        assert fleet_membership.roster_loop_args(host) == ["--class", "cpu"]
        assert fleet_membership.check_roster_active(host) == (True, "active")


def test_supervisor_keeps_entry_and_disk_default_from_one_read(roster, tmp_path, monkeypatch):
    path = roster(args=[*BASE_ARGS, "--spool-gb", "auto"], floor=5)
    _stat(monkeypatch, 1000, 100)
    monkeypatch.setattr(supervise, "_queue_root", lambda: tmp_path / "queue")
    monkeypatch.setattr(supervise, "_SPOOL_VERDICTS", {})
    original = Path.open
    reads = []

    def racing_open(self, mode="r", *args, **kwargs):
        handle = original(self, mode, *args, **kwargs)
        if self == path and mode == "rb":
            raw = handle.read()
            handle.close()
            reads.append(raw)
            replacement = json.loads(raw)
            replacement["local_disk_free_floor_percent"] = 9
            replacement["boxes"]["boxa"]["loops"] = 7
            path.write_text(json.dumps(replacement))
            return io.BytesIO(raw)
        return handle

    monkeypatch.setattr(Path, "open", racing_open)
    loops, args = supervise.declared_shape("boxa", 0, record=False)
    assert loops == 2
    assert args[args.index("--spool-gb") + 1] == "50"
    assert len(reads) == 1


@pytest.mark.parametrize("floor,expected", [(0, "100"), (8, "20")])
def test_supervisor_uses_valid_per_box_disk_floor(roster, tmp_path, monkeypatch, floor, expected):
    path = roster(args=[*BASE_ARGS, "--spool-gb", "auto"], floor=5)
    document = json.loads(path.read_bytes())
    document["boxes"]["boxa"]["local_disk_free_floor_percent"] = floor
    path.write_text(json.dumps(document))
    _stat(monkeypatch, 1000, 100)
    monkeypatch.setattr(supervise, "_queue_root", lambda: tmp_path / "queue")
    monkeypatch.setattr(supervise, "_SPOOL_VERDICTS", {})
    _, args = supervise.declared_shape("boxa", 0, record=False)
    assert args[args.index("--spool-gb") + 1] == expected


@pytest.mark.parametrize("bad", [True, 5.0, "5", 100, -1])
def test_invalid_box_floor_costs_only_the_disk_offer(roster, tmp_path, monkeypatch, bad):
    path = roster(args=[*BASE_ARGS, "--spool-gb", "auto"], floor=5)
    document = json.loads(path.read_bytes())
    document["boxes"]["boxa"]["local_disk_free_floor_percent"] = bad
    path.write_text(json.dumps(document))
    monkeypatch.setattr(supervise, "_SPOOL_VERDICTS", {})
    loops, args = supervise.declared_shape("boxa", 0, record=False)
    assert loops == 2
    assert "--spool-gb" not in args
    assert "--gpu" in args


def test_canonical_preference_and_recorder_ambiguity_remain_distinct(tmp_path, monkeypatch):
    path = write(tmp_path, {"boxes": {
        "old": {"_alias": "new", "args": ["old"]}, "new": {"args": ["new"]}}})
    assert fleet_membership.roster_loop_args("new", path) == ["new"]
    monkeypatch.setattr(box_window, "_ROOT", tmp_path)
    window = box_window.read_window(START, START + 1, host="new", csv_dir=tmp_path, netdata_url=None)
    assert window["source"] == "unavailable"
    assert "ambiguous" in window["reason"]


def test_recorder_prefers_flat_and_membership_prefers_nested(tmp_path, monkeypatch):
    write(tmp_path, {"boxes": {"key": {"_alias": "nested", "args": ["nested"]}}})
    write(tmp_path, {"boxes": {"key": {"_alias": "flat", "args": ["flat"]}}}, flat=True)
    monkeypatch.setattr(box_window, "_ROOT", tmp_path)
    monkeypatch.setattr(fleet_membership, "REPO_ROOT", tmp_path)
    flat = recorder(tmp_path, "flat", 10)
    key = recorder(tmp_path, "key", 20)
    recorder(tmp_path, "nested", 999)
    assert set(box_window._csv_files(tmp_path, "key", START, START)) == {flat, key}
    assert fleet_membership.roster_loop_args("key") == ["nested"]


def export(root, flat):
    package = root / "src/prismabuild"
    package.mkdir(parents=True)
    for name in ("__init__.py", "progress.py", "digest_primitives.py", "fleet_roster.py"):
        shutil.copyfile(ROOT / "src/prismabuild" / name, package / name)
    tools = root / ("tools" if flat else "tools/fleet")
    tools.mkdir(parents=True)
    for name in ("require_pool.py", "runtime_paths.py"):
        shutil.copyfile(ROOT / "tools/fleet" / name, tools / name)
    return tools


@pytest.mark.parametrize("flat", [False, True], ids=["checkout-nested", "installed-flat"])
def test_hook_entry_and_owner_import_are_stdlib_only(tmp_path, flat):
    tools = export(tmp_path, flat)
    write(tmp_path, {"gpu_interpreters": ["/private/venvs/c28-gpu/bin/python"]}, flat=flat)
    flag = tmp_path / "enforced"
    flag.touch()
    event = json.dumps({"tool_input": {"command": "/private/venvs/c28-gpu/bin/python train.py"}})
    environment = {**os.environ, "REQUIRE_POOL_FLAG": str(flag)}
    result = subprocess.run([sys.executable, "-I", "-S", str(tools / "require_pool.py")],
                            input=event, text=True, capture_output=True, env=environment)
    assert result.returncode == 2
    assert "GPU work goes through" in result.stderr
    code = """import sys
sys.path.insert(0, sys.argv[1])
from prismabuild import fleet_roster
assert 'prismabuild.core' not in sys.modules
assert not any(n.startswith('prismabuild._native') for n in sys.modules)
assert all(n.partition('.')[0] in sys.stdlib_module_names | {'prismabuild', '__main__'}
           for n in sys.modules)
print(fleet_roster.Roster.at(sys.argv[2]).gpu_interpreters)
"""
    result = subprocess.run([sys.executable, "-I", "-S", "-c", code,
                             str(tmp_path / "src"), str(tmp_path)],
                            text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    assert "/private/venvs/c28-gpu/bin/python" in result.stdout


@pytest.mark.parametrize("raw", [b'{', b'[]', b'{"gpu_interpreters":null}',
    b'{"gpu_interpreters":["/one"],"gpu_interpreters":["/two"]}',
    b'{"gpu_interpreters":["/one"],"extra":1e999}'])
def test_installed_hook_bad_roster_permits_repair_and_keeps_wrapper_guard(tmp_path, raw):
    tools = export(tmp_path, True)
    (tools / "fleet_boxes.json").write_bytes(raw)
    flag = tmp_path / "flag"
    flag.touch()
    for command, expected in [("/private/venvs/c28-gpu/bin/python train.py", 0),
                              ("flock /private/.gpu.lock echo work", 2),
                              ("git status", 0)]:
        result = subprocess.run([sys.executable, "-I", "-S", str(tools / "require_pool.py")],
            input=json.dumps({"tool_input": {"command": command}}), text=True,
            capture_output=True, env={**os.environ, "REQUIRE_POOL_FLAG": str(flag)})
        assert result.returncode == expected, result.stderr


@pytest.mark.parametrize("damage", ["whitespace", "rename", "absent", "missing-digest"])
def test_role_declaration_consumer_rejects_changed_exact_manifest_bytes(tmp_path, damage):
    path = write(tmp_path, {"boxes": {"key": {"roles": {
        "storage": ["--one"], "tiers": [], "metrics": []}}}})
    receipt = {"files": {MEMBER: hashlib.sha256(path.read_bytes()).hexdigest()}}
    assert migrate_role_logs._roster(tmp_path, receipt, "key") == {
        "storage": ["--one"], "tiers": [], "metrics": []}
    if damage == "missing-digest":
        receipt["files"][MEMBER] = None
    elif damage == "whitespace":
        path.write_bytes(path.read_bytes() + b"\n")
    else:
        document = json.loads(path.read_bytes())
        if damage == "rename":
            document["boxes"]["renamed"] = document["boxes"].pop("key")
            document["boxes"]["renamed"]["_alias"] = "key"
        else:
            document["boxes"]["key"].update(status="offline", status_reason="r", status_by="b", status_unix=1)
        path.write_text(json.dumps(document))
    with pytest.raises(OSError, match="invalid qualified"):
        migrate_role_logs._roster(tmp_path, receipt, "key")


def test_role_log_owner_does_not_promote_alias_to_canonical_authority(tmp_path):
    path = write(tmp_path, {"boxes": {"key": {"_alias": "alias", "roles": {
        "storage": [], "tiers": [], "metrics": []}}}})
    receipt = {"files": {MEMBER: hashlib.sha256(path.read_bytes()).hexdigest()}}
    with pytest.raises(OSError, match="invalid qualified"):
        migrate_role_logs._roster(tmp_path, receipt, "alias")


def test_fresh_attestation_uses_manifest_capture_despite_later_checkout_rename(fleet, monkeypatch):
    member = publish_runtime._agent_definitions().MEMBERS["upgrade_client.py"]
    manifest = {member: SHA, **manifested_roster()}
    for host in ("sparky", "sparklina"):
        post(fleet, host, SHA)
    monkeypatch.setattr(publish_runtime, "_commit_identity", lambda: "c" * 40)
    monkeypatch.setattr(publish_runtime, "_working_tree_dirty", lambda: False)
    monkeypatch.setattr(publish_runtime, "_publication_manifest", lambda: manifest)
    monkeypatch.setattr(publish_runtime, "_git_index_modes", lambda: {})
    path = publish_runtime.CHECKOUT / MEMBER
    real = Path.iterdir

    def rename_on_history(directory):
        if directory.name == "agents":
            path.write_text('{"boxes":{"unattested":{}}}')
        return real(directory)

    monkeypatch.setattr(Path, "iterdir", rename_on_history)
    monkeypatch.setattr(publish_runtime.sys, "argv", ["publish_runtime.py", "--dry-run",
        "--shape-gate-waiver", "private captured history control"])
    assert publish_runtime.main() == 0
    assert b"unattested" in path.read_bytes()

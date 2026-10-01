"""Shared publisher-owned integrity and unchanged barrier updater policy.

New feature qualification, not an old-source RED. Parent-admitted PB CPU only.
No generation/member/parser/hash/verdict helper is mocked.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools/fleet"))
import publish_runtime  # noqa: E402


@pytest.fixture
def sealed(tmp_path, monkeypatch):
    store = tmp_path / "runtime-generations"
    root = store / "private-integrity-generation"
    (root / "tools").mkdir(parents=True)
    member = root / "tools/upgrade_client.py"
    member.write_text("CLIENT_UPGRADE_ROLLOUT_PROTOCOL = 1\n")
    receipt_path = root / "RUNTIME_VERSION.json"
    receipt = {"schema": "prismaquant.prismabuild.runtime_version.v1",
               "generation": root.name, "commit": "b" * 40,
               "files": {"tools/upgrade_client.py": hashlib.sha256(member.read_bytes()).hexdigest()}}
    receipt_path.write_text(json.dumps(receipt))
    member.chmod(0o444)
    receipt_path.chmod(0o444)
    (root / "tools").chmod(0o555)
    root.chmod(0o555)
    monkeypatch.setattr(publish_runtime, "MIRROR", tmp_path / "repo")
    try:
        yield root, member, receipt_path, receipt
    finally:
        root.chmod(0o755)
        for path in root.rglob("*"):
            path.chmod(0o755 if path.is_dir() else 0o644)


def test_integrity_and_barrier_share_the_real_sealed_receipt(sealed):
    root, _member, _receipt_path, receipt = sealed
    assert publish_runtime._sealed_generation(root.name) == (root, receipt)
    assert publish_runtime._barrier_generation(root.name) == (root, receipt)


@pytest.mark.parametrize("damage", ["member-bytes", "member-link", "member-writable",
                                   "root-link", "root-writable", "receipt-generation",
                                   "receipt-schema", "receipt-commit", "unsafe-member"])
def test_both_consumers_refuse_real_integrity_damage(sealed, damage):
    root, member, receipt_path, receipt = sealed
    if damage.startswith("receipt") or damage == "unsafe-member":
        if damage == "unsafe-member":
            receipt["files"] = {"../outside": "f" * 64}
        else:
            receipt[damage.split("-", 1)[1]] = "invalid"
        receipt_path.chmod(0o644)
        receipt_path.write_text(json.dumps(receipt))
        receipt_path.chmod(0o444)
    elif damage == "root-writable":
        root.chmod(0o755)
    elif damage == "root-link":
        moved = root.with_name("real-root")
        root.rename(moved)
        root.symlink_to(moved, target_is_directory=True)
    elif damage == "member-link":
        member.parent.chmod(0o755)
        member.rename(member.with_name("real-member"))
        member.symlink_to("real-member")
    else:
        member.chmod(0o644)
        if damage == "member-bytes":
            member.write_text("damaged")
            member.chmod(0o444)
    for consumer in (publish_runtime._sealed_generation, publish_runtime._barrier_generation):
        with pytest.raises(SystemExit):
            consumer(root.name)


@pytest.mark.parametrize("policy", ["no-updater", "non-rollout-updater"])
def test_integrity_does_not_borrow_or_weaken_barrier_updater_authority(sealed, policy):
    root, member, receipt_path, receipt = sealed
    if policy == "no-updater":
        receipt["files"] = {}
    else:
        member.chmod(0o644)
        member.write_text("CLIENT_UPGRADE_ROLLOUT_PROTOCOL = 0\n")
        receipt["files"]["tools/upgrade_client.py"] = hashlib.sha256(member.read_bytes()).hexdigest()
        member.chmod(0o444)
    receipt_path.chmod(0o644)
    receipt_path.write_text(json.dumps(receipt))
    receipt_path.chmod(0o444)
    assert publish_runtime._sealed_generation(root.name) == (root, receipt)
    with pytest.raises(SystemExit, match="no updater|no rollout-aware updater"):
        publish_runtime._barrier_generation(root.name)


def test_real_publication_manifest_resolves_both_tool_layouts_and_dependencies(monkeypatch):
    """The actual publisher inventory/resolver, not an invented runtime alias."""
    monkeypatch.setattr(publish_runtime, "CHECKOUT", ROOT)
    published = publish_runtime._publication_manifest()
    for name in ("role_log_identity.py", "migrate_role_logs.py"):
        assert name in publish_runtime.FLEET_SCRIPTS
        source = ROOT / "tools/fleet" / name
        expected = hashlib.sha256(source.read_bytes()).hexdigest()
        for member in (f"tools/{name}", f"tools/fleet/{name}"):
            assert published[member] == expected
            assert publish_runtime._source_for(member) == source
    assert published["src/prismabuild/core.py"] == hashlib.sha256(
        (ROOT / "src/prismabuild/core.py").read_bytes()).hexdigest()
    imports = publish_runtime._imported_modules(ROOT / "tools/fleet/migrate_role_logs.py")
    assert "prismabuild" in imports
    for name in ("role_log_identity", "supervise", "publish_runtime", "fleet_roster"):
        assert name in imports
        assert f"tools/{name}.py" in published
    affected = ("tools/migrate_role_logs.py imports ", "tools/supervise.py imports ")
    assert not [message for message in publish_runtime._unshipped_imports(published)
                if message.startswith(affected)]
    incomplete = dict(published)
    del incomplete["tools/role_log_identity.py"]
    problems = publish_runtime._unshipped_imports(incomplete)
    for consumer in ("migrate_role_logs.py", "supervise.py"):
        assert any(message.startswith(f"tools/{consumer} imports role_log_identity,")
                   for message in problems)

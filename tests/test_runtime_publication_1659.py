"""Publication authority and immutable execution paths for movement roles."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat

import pytest

from prismabuild import runtime_publication as publication


@pytest.fixture
def publication_store(tmp_path, monkeypatch):
    """Model root custody within a private filesystem for unprivileged unit tests.

    The separate Docker smoke uses the real UID, ownership, and ancestor checks.
    """
    store = tmp_path / "protected" / "movement-generations"
    monkeypatch.setattr(publication, "PROTECTED_GENERATION_STORE", store)

    def private_custody(path):
        if not path.is_relative_to(tmp_path):
            return False
        for entry in (path, *path.parents):
            if entry == tmp_path:
                break
            info = entry.lstat()
            if (info.st_mode & 0o022
                    or not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode))):
                return False
        return True

    monkeypatch.setattr(publication, "_protected_path", private_custody)
    publication._VERIFIED.clear()
    yield store
    if store.exists():
        for entry in (store, *store.rglob("*")):
            if entry.is_dir():
                entry.chmod(0o755)


def source_generation(tmp_path):
    root = tmp_path / "runtime-generations" / "aaaaaaaaaaaa-1791311474-bbbbbbbbbbbb"
    (root / "tools" / "fleet").mkdir(parents=True)
    script = root / "tools" / "fleet" / "stage_release.py"
    script.write_text("raise SystemExit(0)\n")
    dependency = root / "src" / "prismabuild" / "helper.py"
    dependency.parent.mkdir(parents=True)
    dependency.write_text("VALUE = 1\n")
    receipt = {"schema": publication.RUNTIME_SCHEMA, "generation": root.name,
               "commit": "c" * 40, "files": {
                   p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                   for p in (script, dependency)}}
    (root / "RUNTIME_VERSION.json").write_text(json.dumps(receipt))
    return root


def approve(source, monkeypatch):
    with monkeypatch.context() as authority:
        authority.setattr(publication.os, "geteuid", lambda: 0)
        return publication.publish_generation(source, receipt_sha256=hashlib.sha256(
            (source / "RUNTIME_VERSION.json").read_bytes()).hexdigest())


def test_a_submitter_cannot_publish_an_authority_record(tmp_path, monkeypatch, publication_store):
    source = source_generation(tmp_path)
    digest = hashlib.sha256((source / "RUNTIME_VERSION.json").read_bytes()).hexdigest()
    monkeypatch.setattr(publication.os, "geteuid", lambda: 1000)
    with pytest.raises(PermissionError, match="root authority"):
        publication.publish_generation(source, receipt_sha256=digest)
    assert not publication_store.exists()


@pytest.mark.parametrize("umask", [0o002, 0o077])
def test_publication_preserves_custody_under_the_callers_umask(
        tmp_path, monkeypatch, publication_store, umask):
    previous = os.umask(umask)
    try:
        protected = approve(source_generation(tmp_path), monkeypatch)
    finally:
        os.umask(previous)
    member = protected / "tools" / "fleet" / "stage_release.py"
    assert publication.published_member(member) == member


def test_publication_copies_imports_and_binds_an_independent_record(
        tmp_path, monkeypatch, publication_store):
    source = source_generation(tmp_path)
    protected = approve(source, monkeypatch)
    member = protected / "tools" / "fleet" / "stage_release.py"
    assert publication.published_member(member) == member
    # A store owner can alter both source code and its own receipt, but not the copy.
    original = member.read_bytes()
    (source / "tools" / "fleet" / "stage_release.py").write_text("raise SystemExit(73)\n")
    (source / "src" / "prismabuild" / "helper.py").write_text("VALUE = 73\n")
    assert member.read_bytes() == original
    assert (protected / "src" / "prismabuild" / "helper.py").read_text() == "VALUE = 1\n"
    assert publication.published_member(source / "tools" / "fleet" / "stage_release.py") is None
    with pytest.raises(FileExistsError, match="append-only"):
        approve(source, monkeypatch)


@pytest.mark.parametrize("fault", ["missing-record", "changed-receipt", "changed-member",
                                  "writable-parent", "symlink-record"])
def test_unknown_or_changed_publication_cannot_grant_a_role(
        tmp_path, monkeypatch, publication_store, fault):
    source = source_generation(tmp_path)
    protected = approve(source, monkeypatch)
    member = protected / "tools" / "fleet" / "stage_release.py"
    assert publication.published_member(member) == member  # Populate the verification cache.
    if fault == "missing-record":
        protected.chmod(0o755)
        (protected / publication.PUBLICATION_RECORD).unlink()
    elif fault == "symlink-record":
        protected.chmod(0o755)
        record = protected / publication.PUBLICATION_RECORD
        copied = tmp_path / "forged.json"
        copied.write_bytes(record.read_bytes())
        record.unlink()
        record.symlink_to(copied)
    elif fault == "writable-parent":
        publication_store.chmod(0o777)
    else:
        target = member if fault == "changed-member" else protected / "RUNTIME_VERSION.json"
        before = target.stat()
        raw = target.read_bytes()
        target.chmod(0o644)
        # Preserve length and mtime. Ctime and receipt binding must defeat a stale verdict.
        target.write_bytes(raw.replace(b"0", b"9", 1) if fault == "changed-member"
                           else raw.replace(b"c", b"d", 1))
        target.chmod(0o444)
        os.utime(target, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert publication.published_member(member) is None


@pytest.mark.parametrize("fault", ["receipt-digest", "member-digest", "member-symlink", "traversal"])
def test_publication_refuses_changed_or_escaping_source(
        tmp_path, monkeypatch, publication_store, fault):
    source = source_generation(tmp_path)
    receipt_path = source / "RUNTIME_VERSION.json"
    digest = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
    if fault == "receipt-digest":
        digest = "f" * 64
    elif fault == "member-digest":
        (source / "tools" / "fleet" / "stage_release.py").write_text("raise SystemExit(73)\n")
    elif fault == "member-symlink":
        member = source / "tools" / "fleet" / "stage_release.py"
        alias = tmp_path / "payload.py"
        alias.write_bytes(member.read_bytes())
        member.unlink()
        member.symlink_to(alias)
    else:
        receipt = json.loads(receipt_path.read_bytes())
        receipt["files"]["../payload.py"] = "f" * 64
        receipt_path.write_text(json.dumps(receipt))
        digest = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
    with monkeypatch.context() as authority:
        authority.setattr(publication.os, "geteuid", lambda: 0)
        with pytest.raises(ValueError):
            publication.publish_generation(source, receipt_sha256=digest)
    assert not (publication_store / source.name).exists()


def test_actual_custody_rejects_a_submitter_path(tmp_path):
    # A non-root owner or the writable /tmp ancestor defeats custody.
    assert publication._protected_path(tmp_path) is False


def test_sealer_selects_only_the_approved_receipt_copy(tmp_path, monkeypatch, publication_store):
    source = source_generation(tmp_path)
    protected = approve(source, monkeypatch)
    tool = source / "tools" / "fleet" / "stage_release.py"
    copied = protected / "tools" / "fleet" / "stage_release.py"
    assert publication.movement_member(tool, retained_store=source.parent) == copied
    receipt = source / "RUNTIME_VERSION.json"
    receipt.write_bytes(receipt.read_bytes().replace(b"c", b"d", 1))
    assert publication.movement_member(tool, retained_store=source.parent) is None

"""Descriptor-bound scratch ownership; only disposable CPU fixture paths."""
import hashlib
import os
from pathlib import Path

import pytest

from prismabuild import local_scratch as scratch


@pytest.fixture
def declaration(tmp_path):
    root = tmp_path / "scratch"
    root.mkdir()
    key, nonce = "a" * 64, "b" * 32
    return {"schema": scratch.EPHEMERAL_SCRATCH_SCHEMA_V1,
            "lifetime": "ephemeral", "root_env": "ROOT", "max_env": "MAX",
            "root": str(root), "max_bytes": 1024, "name": "row-temp",
            "owner_action_key": key, "owner_published_unix": 1.0,
            "owner_host": "fixture", "owner_attempt": {
                "nonce": nonce, "scope_id": "prismabuild-job" + hashlib.sha256(
                    (key + nonce).encode()).hexdigest()[:32] + ".slice"}}


def owned(declaration):
    identity = scratch._create_scratch_directory(declaration)
    return scratch.ephemeral_scratch_path(declaration), identity


def test_create_and_repeated_cleanup_are_bound_and_leave_root(declaration):
    path, identity = owned(declaration)
    (path / "sub").mkdir()
    (path / "sub" / "payload").write_bytes(b"temporary")
    scratch._clean_scratch_directory(declaration, identity)
    scratch._clean_scratch_directory(declaration, identity)
    assert not path.exists() and Path(declaration["root"]).is_dir()


def test_existing_leaf_is_not_adopted(declaration):
    path, identity = owned(declaration)
    sentinel = path / "foreign"
    sentinel.write_bytes(b"do not adopt")
    with pytest.raises((scratch.LocalScratchError, OSError)):
        scratch._create_scratch_directory(declaration)
    assert sentinel.read_bytes() == b"do not adopt"


@pytest.mark.parametrize("level", ["root", "namespace", "parent", "leaf"])
@pytest.mark.parametrize("replacement", ["directory", "symlink"])
def test_renamed_replaced_ancestry_refuses_without_touching_foreign(
        declaration, level, replacement, tmp_path):
    path, identity = owned(declaration)
    target = {"root": Path(declaration["root"]), "namespace": Path(declaration["root"]) /
              "prismabuild-ephemeral", "parent": path.parent, "leaf": path}[level]
    moved = target.with_name(target.name + "-retained")
    target.rename(moved)
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    if replacement == "directory":
        target.mkdir()
        sentinel = target / "foreign"
    else:
        target.symlink_to(foreign, target_is_directory=True)
        sentinel = foreign / "foreign"
    sentinel.write_bytes(b"foreign stays")
    with pytest.raises((scratch.LocalScratchError, OSError)):
        scratch._clean_scratch_directory(declaration, identity)
    assert sentinel.read_bytes() == b"foreign stays"


def test_content_symlink_is_unlinked_not_followed(declaration, tmp_path):
    path, identity = owned(declaration)
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    sentinel = foreign / "persistent"
    sentinel.write_bytes(b"cache")
    (path / "link").symlink_to(foreign, target_is_directory=True)
    scratch._clean_scratch_directory(declaration, identity)
    assert sentinel.read_bytes() == b"cache" and not path.exists()


def test_partial_cleanup_interruption_resumes_idempotently(declaration, monkeypatch):
    path, identity = owned(declaration)
    for name in ("one", "two", "three"):
        (path / name).write_bytes(b"temp")
    unlink = os.unlink
    count = 0

    def interrupt(name, **kwargs):
        nonlocal count
        count += 1
        if count == 2:
            raise OSError("fixture process interruption")
        return unlink(name, **kwargs)

    monkeypatch.setattr(os, "unlink", interrupt)
    with pytest.raises(OSError, match="fixture process interruption"):
        scratch._clean_scratch_directory(declaration, identity)
    assert path.exists() and len(list(path.iterdir())) == 2
    monkeypatch.setattr(os, "unlink", unlink)
    scratch._clean_scratch_directory(declaration, identity)
    scratch._clean_scratch_directory(declaration, identity)
    assert not path.exists()


def test_ancestor_swap_after_open_does_not_follow_replacement(declaration, monkeypatch):
    path, identity = owned(declaration)
    original = scratch._remove_scratch_contents
    moved = path.parent.with_name("retained-parent")
    sentinel = path.parent / "foreign"

    def swap(fd, guard):
        path.parent.rename(moved)
        path.parent.mkdir()
        sentinel.write_bytes(b"replacement")
        return original(fd, guard)

    monkeypatch.setattr(scratch, "_remove_scratch_contents", swap)
    with pytest.raises(scratch.LocalScratchError):
        scratch._clean_scratch_directory(declaration, identity)
    assert sentinel.read_bytes() == b"replacement"
    assert (moved / path.name).is_dir()


@pytest.mark.parametrize("field", ["dev", "ino", "uid", "birth_ns", "mnt_id"])
def test_identity_tampering_refuses(declaration, field):
    path, identity = owned(declaration)
    identity[-1][field] += 1
    with pytest.raises(scratch.LocalScratchError):
        scratch._clean_scratch_directory(declaration, identity)
    assert path.is_dir()


def test_creation_refuses_symlink_root(declaration, tmp_path):
    root = Path(declaration["root"])
    root.rmdir()
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    root.symlink_to(foreign, target_is_directory=True)
    with pytest.raises((scratch.LocalScratchError, OSError)):
        scratch._create_scratch_directory(declaration)
    assert list(foreign.iterdir()) == []


def test_inode_reuse_cannot_replace_immutable_creation_identity(declaration, monkeypatch):
    path, identity = owned(declaration)
    expected_birth = identity[-1]["birth_ns"]
    real = scratch._directory_birthtime
    monkeypatch.setattr(scratch, "_directory_birthtime", lambda fd:
                        expected_birth + 1 if os.fstat(fd).st_ino == identity[-1]["ino"] else real(fd))
    with pytest.raises(scratch.LocalScratchError):
        scratch._clean_scratch_directory(declaration, identity)
    assert path.exists()


def test_unknown_birthtime_refuses_before_namespace_creation(declaration, monkeypatch):
    monkeypatch.setattr(scratch, "_directory_birthtime", lambda fd:
                        (_ for _ in ()).throw(scratch.LocalScratchError("fixture unknown birthtime")))
    with pytest.raises(scratch.LocalScratchError, match="fixture unknown birthtime"):
        scratch._create_scratch_directory(declaration)
    assert list(Path(declaration["root"]).iterdir()) == []


@pytest.mark.parametrize("level", ["namespace", "parent", "leaf"])
def test_permissions_changed_after_registration_refuse_before_deletion(declaration, level):
    path, identity = owned(declaration)
    sentinel = path / "temp"
    sentinel.write_bytes(b"do not delete with unsafe control")
    target = {"namespace": Path(declaration["root"]) / "prismabuild-ephemeral",
              "parent": path.parent, "leaf": path}[level]
    target.chmod(0o777)
    with pytest.raises(scratch.LocalScratchError, match="permissions"):
        scratch._clean_scratch_directory(declaration, identity)
    assert sentinel.read_bytes() == b"do not delete with unsafe control"

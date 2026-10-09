"""Root-controlled runtime copies for movement role classification.

This interface grants a scheduling role, not permission to launch an action.
Only an administrator can publish a copy. Ordinary runtime stores have no authority.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tempfile

PROTECTED_GENERATION_STORE = Path("/opt/prismabuild/movement-generations")
PUBLICATION_RECORD = "MOVEMENT_PUBLICATION.json"
PUBLICATION_SCHEMA = "prismabuild.movement-publication.v1"
RUNTIME_SCHEMA = "prismaquant.prismabuild.runtime_version.v1"
_DIGEST = re.compile(r"[0-9a-f]{64}")
_GENERATION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_VERIFIED: dict[tuple, dict] = {}


def _protected_path(path: Path) -> bool:
    """Require root custody from the filesystem root through this path."""
    for entry in (path, *path.parents):
        info = entry.lstat()
        if (info.st_uid != 0 or info.st_mode & 0o022
                or not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode))):
            return False
    return True


def _create_directories(path: Path) -> None:
    """Create each missing component without group or other write access."""
    missing = []
    while not path.exists():
        missing.append(path)
        path = path.parent
    for directory in reversed(missing):
        directory.mkdir(mode=0o755)
        directory.chmod(0o755)


def _regular_bytes(path: Path) -> bytes:
    """Read a regular file without following a final symlink."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError(f"not a regular publication file: {path}")
        return stream.read()


def _receipt(raw: bytes, generation: str) -> dict:
    value = json.loads(raw)
    if (not isinstance(value, dict) or value.get("schema") != RUNTIME_SCHEMA
            or value.get("generation") != generation
            or re.fullmatch(r"[0-9a-f]{40}", str(value.get("commit", ""))) is None
            or not isinstance(value.get("files"), dict) or not value["files"]):
        raise ValueError("invalid runtime publication receipt")
    for name, digest in value["files"].items():
        parts = PurePosixPath(name).parts
        if (not parts or name != "/".join(parts) or name.startswith("/")
                or any(part in (".", "..") for part in parts)
                or name in ("RUNTIME_VERSION.json", PUBLICATION_RECORD)
                or not isinstance(digest, str) or _DIGEST.fullmatch(digest) is None):
            raise ValueError("invalid runtime publication member")
    return value


def publish_generation(source: Path, *, receipt_sha256: str) -> Path:
    """Copy the administrator's selected receipt and all members without executing them.

    The administrator supplies the reviewed receipt digest. The interface never
    accepts a submission token or derives authority from a store-owned receipt.
    """
    if os.geteuid() != 0:
        raise PermissionError("movement publication requires root authority")
    if _DIGEST.fullmatch(receipt_sha256) is None:
        raise ValueError("movement publication requires a receipt sha256")
    source = Path(source).resolve(strict=True)
    if _GENERATION.fullmatch(source.name) is None:
        raise ValueError("invalid movement generation name")
    raw = _regular_bytes(source / "RUNTIME_VERSION.json")
    if hashlib.sha256(raw).hexdigest() != receipt_sha256:
        raise ValueError("runtime receipt differs from the approved digest")
    receipt = _receipt(raw, source.name)
    store = PROTECTED_GENERATION_STORE
    # Reject unsafe existing ancestors before creating anything under them.
    ancestor = store
    while not ancestor.exists():
        ancestor = ancestor.parent
    if not _protected_path(ancestor):
        raise PermissionError("movement publication store has no root custody")
    _create_directories(store)
    if not _protected_path(store):
        raise PermissionError("movement publication store has no root custody")
    target = store / source.name
    if target.exists():
        raise FileExistsError("movement generations are append-only")
    stage = Path(tempfile.mkdtemp(prefix=".publication-", dir=store))
    try:
        for name, expected in receipt["files"].items():
            member = source / name
            # Every source component must be literal, not a symlink or alias.
            if member.resolve(strict=True) != member:
                raise ValueError(f"runtime member is a symlink: {name}")
            data = _regular_bytes(member)
            if hashlib.sha256(data).hexdigest() != expected:
                raise ValueError(f"runtime member digest differs: {name}")
            copied = stage / name
            _create_directories(copied.parent)
            copied.write_bytes(data)
            copied.chmod(0o555 if member.stat().st_mode & 0o111 else 0o444)
        (stage / "RUNTIME_VERSION.json").write_bytes(raw)
        (stage / PUBLICATION_RECORD).write_text(json.dumps({
            "schema": PUBLICATION_SCHEMA, "generation": source.name,
            "receipt_sha256": receipt_sha256,
        }, sort_keys=True) + "\n", encoding="utf-8")
        for entry in stage.rglob("*"):
            if entry.is_file():
                with entry.open("rb") as stream:
                    os.fsync(stream.fileno())
                if entry.name in ("RUNTIME_VERSION.json", PUBLICATION_RECORD):
                    entry.chmod(0o444)
        directories = [entry for entry in stage.rglob("*") if entry.is_dir()]
        for directory in (*reversed(directories), stage):
            directory.chmod(0o555)
            descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        # A directory rename must not replace another administrator's publication.
        os.rename(stage, target)
        descriptor = os.open(store, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return target
    finally:
        if stage.exists():
            for entry in (stage, *stage.rglob("*")):
                if entry.is_dir():
                    entry.chmod(0o755)
            shutil.rmtree(stage)


def published_member(path: Path) -> Path | None:
    """Return a protected, receipt-bound tool; unknown authority remains ordinary."""
    try:
        store = PROTECTED_GENERATION_STORE
        resolved = Path(path).resolve(strict=True)
        relative = resolved.relative_to(store).parts
        if (len(relative) not in (3, 4) or relative[1] != "tools"
                or len(relative) == 4 and relative[2] != "fleet"
                or _GENERATION.fullmatch(relative[0]) is None
                or not _protected_path(resolved)):
            return None
        root = store / relative[0]
        authority_path = root / PUBLICATION_RECORD
        receipt_path = root / "RUNTIME_VERSION.json"
        if (not _protected_path(authority_path) or not _protected_path(receipt_path)
                or root.stat().st_mode & 0o222 or resolved.stat().st_mode & 0o222):
            return None
        # Root custody makes the entire copied namespace immutable to submitters.
        # Inode and ctime also invalidate entries after an administrator changes it.
        stamps = tuple((p.stat().st_dev, p.stat().st_ino, p.stat().st_ctime_ns,
                        p.stat().st_size) for p in (authority_path, receipt_path, resolved))
        key = (str(resolved), stamps)
        if key not in _VERIFIED:
            authority = json.loads(_regular_bytes(authority_path))
            raw = _regular_bytes(receipt_path)
            if (not isinstance(authority, dict)
                    or authority.get("schema") != PUBLICATION_SCHEMA
                    or authority.get("generation") != root.name
                    or authority.get("receipt_sha256") != hashlib.sha256(raw).hexdigest()):
                return None
            receipt = _receipt(raw, root.name)
            expected = receipt["files"].get("/".join(relative[1:]))
            if expected != hashlib.sha256(_regular_bytes(resolved)).hexdigest():
                return None
            _VERIFIED[key] = receipt
        return resolved
    except (OSError, ValueError, TypeError):
        return None


def movement_member(path: Path, *, retained_store: Path) -> Path | None:
    """Resolve an ordinary generation member to its administrator-approved copy."""
    protected = published_member(path)
    if protected is not None:
        return protected
    try:
        resolved = Path(path).resolve(strict=True)
        relative = resolved.relative_to(retained_store.resolve(strict=True))
        protected = published_member(PROTECTED_GENERATION_STORE / relative)
        if protected is None:
            return None
        # A copied receipt must match the source receipt, not merely its name.
        source_root = retained_store.resolve(strict=True) / relative.parts[0]
        protected_root = PROTECTED_GENERATION_STORE / relative.parts[0]
        if _regular_bytes(source_root / "RUNTIME_VERSION.json") != _regular_bytes(
                protected_root / "RUNTIME_VERSION.json"):
            return None
        return protected
    except (OSError, ValueError):
        return None

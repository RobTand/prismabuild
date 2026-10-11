"""Builders and fixtures for the protected movement copies (#1579, #1659).

``publication_store`` models root custody inside a private filesystem, so the
unprivileged test user can stand in for root: the fixture accepts a path when no
component up to the test's ``tmp_path`` is writable by group or other.  A
separate Docker smoke runs the real root program under a real root UID.

``movement_authority`` is a host that has done what its enrolled unit does: a
private runtime generation is published, its protected copy exists, and this
process runs that generation.  Tests that are about the reservation take it;
tests about the fallback remove or repoint the copy.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import stat

import pytest

from prismabuild import resource_scope, runtime_publication as publication

SCRIPTS = ("stage_move.py", "ram_promote.py", "stage_release.py", "produced_export.py",
           "local_resident.py")
GENERATION = "a" * 12 + "-1791311474-" + "b" * 12


def seal(root: Path) -> None:
    """Make a generation read-only the way a published one is."""
    for entry in sorted(root.rglob("*"), reverse=True):
        if entry.is_dir():
            entry.chmod(0o555)
    root.chmod(0o555)


def unseal(root: Path) -> None:
    if root.exists():
        for entry in (root, *root.rglob("*")):
            if entry.is_dir():
                entry.chmod(0o755)


def retained_generation(parent: Path, name: str = GENERATION, *, scripts=SCRIPTS,
                        commit: str = "c" * 40) -> Path:
    """One generation of a retained store: tools in both layouts, one import, a receipt."""
    root = parent / name
    files = {}
    for sub in ("tools", "tools/fleet"):
        (root / sub).mkdir(parents=True)
        for script in scripts:
            path = root / sub / script
            path.write_text(f"# {script}\n")
            files[f"{sub}/{script}"] = hashlib.sha256(path.read_bytes()).hexdigest()
            path.chmod(0o444)
    dependency = root / "src" / "prismabuild" / "helper.py"
    dependency.parent.mkdir(parents=True)
    dependency.write_text("VALUE = 1\n")
    dependency.chmod(0o444)
    files["src/prismabuild/helper.py"] = hashlib.sha256(dependency.read_bytes()).hexdigest()
    receipt = root / "RUNTIME_VERSION.json"
    receipt.write_text(json.dumps({
        "schema": publication.RUNTIME_SCHEMA, "generation": name, "commit": commit,
        "files": files}))
    receipt.chmod(0o444)
    seal(root)
    return root


def approve(source: Path, monkeypatch) -> Path:
    """Publish ``source`` as the enrolled root unit would, and return the protected copy."""
    with monkeypatch.context() as authority:
        authority.setattr(publication.os, "geteuid", lambda: 0)
        return publication.publish_generation(source, receipt_sha256=hashlib.sha256(
            (source / "RUNTIME_VERSION.json").read_bytes()).hexdigest())


@pytest.fixture
def publication_store(tmp_path, monkeypatch):
    """Model root custody within a private filesystem for unprivileged unit tests."""
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
    for cache in (publication._ROOTS, publication._MEMBERS, publication._RECEIPT_SHA):
        cache.clear()
    yield store
    unseal(store)


@pytest.fixture
def movement_authority(tmp_path, monkeypatch, publication_store):
    """This host holds the protected copy of the generation this process runs.

    Returns the protected generation.  Its retained source is the one generation
    of ``resource_scope.RETAINED_GENERATION_STORE``.
    """
    retained = retained_generation(tmp_path / "runtime-generations")
    monkeypatch.setattr(resource_scope, "RETAINED_GENERATION_STORE", retained.parent)
    protected = approve(retained, monkeypatch)
    monkeypatch.setattr(publication, "executing_generation", lambda: retained)
    yield protected
    unseal(retained.parent)

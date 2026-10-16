"""The invoked root proof API must belong to the bytes captured by its loader."""
import hashlib
import importlib.util
from pathlib import Path
import py_compile
import sys

import pytest

import upgrade_client
from client_proof_bridge_fixture import PREBRIDGE, fixed_source_artifact
from test_client_proof_prior_trust import STRICT_PROOF, STRICT_PROOF_SHA256, tmp_path


LEGACY_SHA256 = "7b951734fe4471ab012fe715cda238be1dd4c7d83c8f15c39447e1b75009bfe7"


@pytest.fixture
def installed_leaf(tmp_path):
    install = tmp_path / "installed"
    install.mkdir(mode=0o755, exist_ok=True)
    path = install / "park_proof.py"
    strict = fixed_source_artifact(
        "strict-proof.py", STRICT_PROOF, "src/prismabuild/park_proof.py", STRICT_PROOF_SHA256)
    legacy = fixed_source_artifact(
        "legacy-proof.py", PREBRIDGE, "src/prismabuild/park_proof.py", LEGACY_SHA256)
    path.write_bytes(strict)
    path.chmod(0o644)
    assert upgrade_client.trusted(path) == path
    return path, strict, legacy


def test_invoked_entrypoints_come_from_the_captured_fd_after_path_replacement(
        installed_leaf, monkeypatch):
    path, strict, legacy = installed_leaf
    original = upgrade_client.read_proof_file
    captured = []

    def replace_after_capture(candidate):
        snapshot = original(candidate)
        captured.append(snapshot)
        replacement = path.with_suffix(".replacement")
        replacement.write_bytes(legacy)
        replacement.chmod(0o644)
        replacement.replace(path)
        return snapshot

    monkeypatch.setattr(upgrade_client, "read_proof_file", replace_after_capture)
    loaded = upgrade_client.load_installed_park_proof(path.parent)
    assert len(captured) == 1 and captured[0].data == strict
    assert loaded.sha256 == hashlib.sha256(strict).hexdigest() == STRICT_PROOF_SHA256
    assert path.read_bytes() == legacy
    api = loaded.module
    # These are real strict entrypoints, whose behavior the legacy bytes lack.
    assert api.drain_identity({"draining": True, "changed_unix": 1e-20}).key == "1em20"
    assert isinstance(api.drain_identity({"draining": True, "changed_unix": True}),
                      api.ProofUnknown)
    assert api.drain_identity.__globals__ is api.__dict__
    assert api.process_identity.__globals__ is api.__dict__
    assert api.process_identity(1, "0").starttime == "0"
    assert api.__name__ not in sys.modules
    monkeypatch.setattr(upgrade_client, "read_proof_file", original)
    with pytest.raises(ValueError, match="strict"):
        upgrade_client.load_installed_park_proof(path.parent)


def test_fresh_owner_ignores_valid_unrelated_unchecked_bytecode(installed_leaf):
    path, strict, legacy = installed_leaf
    path.write_bytes(legacy)
    bytecode = Path(importlib.util.cache_from_source(str(path)))
    py_compile.compile(str(path), cfile=str(bytecode), doraise=True,
                       invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH)
    retained_pyc = bytecode.read_bytes()
    path.write_bytes(strict)
    first = upgrade_client.load_installed_park_proof(path.parent)
    second = upgrade_client.load_installed_park_proof(path.parent)
    assert first.module is not second.module
    assert first.module.__name__ != second.module.__name__
    assert first.sha256 == second.sha256 == hashlib.sha256(strict).hexdigest()
    for loaded in (first, second):
        api = loaded.module
        assert api.drain_identity({"draining": True, "changed_unix": 1e20}).key == "1ep20"
        assert isinstance(api.process_identity(True, "0"), api.ProofUnknown)
        assert api.drain_identity.__globals__ is api.__dict__
        assert api.__name__ not in sys.modules
    assert bytecode.read_bytes() == retained_pyc
    assert path.read_bytes() == strict

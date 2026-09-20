"""Retained worker runtime selects its own sealed proxy (PB R3).

Pool builds worker argv from the sealed worker_script (a retained
action names an earlier immutable runtime), while the scope wrapper
must choose resource_exec from that SAME proven runtime -- or the
older core refuses the newer helper root and --as-sealed-by retries
of post-repair actions fail. Fixture roots A (earlier) and B (later)
carry identical relevant code under different immutable roots plus
receipts; the proxy path and the full proxy->core authority chain run
through real files, never hand-built environments. Run via published
pbtest at -10.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import resource_scope  # noqa: E402

KEY = "c" * 64
NONCE = "b" * 32
RECEIPT_SCHEMA = "prismaquant.prismabuild.runtime_version.v1"

_COPIED = ("tools/fleet/resource_exec.py", "tools/fleet/resource_broker.py",
           "tools/fleet/runtime_paths.py", "tools/prismabuild_worker.py",
           "src/prismabuild/core.py")


def _generation(tmp_path: Path, name: str) -> Path:
    """A sealed-layout fixture root with real file bytes + receipt."""

    root = tmp_path / name
    members: dict[str, str] = {}
    for rel in _COPIED:
        src = Path(__file__).resolve().parents[1] / rel
        dst = root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(src.read_bytes())
        digest = hashlib.sha256(dst.read_bytes()).hexdigest()
        members[rel] = digest
    (root / "RUNTIME_VERSION.json").write_text(json.dumps({
        "schema": RECEIPT_SCHEMA, "generation": name,
        "commit": "0" * 40, "files": members}))
    return root


def _scope() -> resource_scope.ResourceScope:
    scope = resource_scope.ResourceScope(
        KEY, NONCE, 64 * 1024 ** 2, Path("/tmp/no-telemetry.json"))
    scope.token = "t" * 64
    return scope


def test_wrap_selects_worker_runtime_proxy(tmp_path) -> None:
    """argv naming generation A wraps with A's proxy, not current."""

    gen_a = _generation(tmp_path, "gen-a-0001")
    argv = ["python", str(gen_a / "tools" / "prismabuild_worker.py"),
            "run-local", "--action", "x"]
    wrapped = _scope().wrap_argv(argv)
    assert wrapped[1] == str(gen_a / "tools" / "fleet" / "resource_exec.py")


def test_wrap_falls_back_on_tampered_receipt(tmp_path) -> None:
    """A receipt that no longer covers the proxy keeps current behavior."""

    gen_a = _generation(tmp_path, "gen-a-0002")
    receipt_path = gen_a / "RUNTIME_VERSION.json"
    receipt = json.loads(receipt_path.read_text())
    receipt["files"]["tools/fleet/resource_exec.py"] = "0" * 64
    receipt_path.write_text(json.dumps(receipt))
    argv = ["python", str(gen_a / "tools" / "prismabuild_worker.py"),
            "run-local", "--action", "x"]
    wrapped = _scope().wrap_argv(argv)
    current = Path(resource_scope.__file__).resolve().parents[2]
    assert wrapped[1].startswith(str(current)), wrapped[1]


def test_wrap_falls_back_off_generation(tmp_path) -> None:
    """Dev stubs and non-runtime paths keep the current proxy."""

    argv = ["python", "/worker.py", "run-local", "--action", "x"]
    wrapped = _scope().wrap_argv(argv)
    current = Path(resource_scope.__file__).resolve().parents[2]
    assert wrapped[1].startswith(str(current)), wrapped[1]


def _proxy_env(proxy: Path, key: str, nonce: str) -> dict:
    """Seeded identity from a REAL proxy file in a subprocess."""

    code = ("import json, sys; sys.path.insert(0, %r); "
            "import resource_exec; print(json.dumps("
            "resource_exec.payload_identity_env({}, action_key=%r, nonce=%r)))"
            % (str(proxy.parent), key, nonce))
    probe = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True, text=True, timeout=60)
    assert probe.returncode == 0, probe.stderr
    return json.loads(probe.stdout)


def _core_module(gen: Path, tag: str):
    spec = importlib.util.spec_from_file_location(
        f"retained_core_{tag}", gen / "src" / "prismabuild" / "core.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[f"retained_core_{tag}"] = module
    spec.loader.exec_module(module)
    return module


def test_authority_chain_matches_within_generation(tmp_path, monkeypatch) -> None:
    """A's proxy seeds A's root; A's core forwards it; B's core drops it."""

    gen_a = _generation(tmp_path, "gen-a-0003")
    gen_b = _generation(tmp_path, "gen-b-0003")
    env_a = _proxy_env(gen_a / "tools" / "fleet" / "resource_exec.py",
                       KEY, NONCE)
    assert env_a["PRISMABUILD_READER_HELPER_ROOT"] == str(gen_a)
    for name, value in env_a.items():
        monkeypatch.setenv(name, value)
    core_a = _core_module(gen_a, "a")
    core_b = _core_module(gen_b, "b")
    action = {"action_key": KEY}
    forwarded = core_a._residency_environment(action, {})
    assert forwarded["PRISMABUILD_ACTION_NONCE"] == NONCE
    assert forwarded["PRISMABUILD_ACTION_SCOPE"] == env_a[
        "PRISMABUILD_ACTION_SCOPE"]
    assert forwarded["PRISMABUILD_READER_HELPER_ROOT"] == str(gen_a)
    # The same bundle under B's core is another generation's identity:
    # never fed, legacy preserved.
    dropped = core_b._residency_environment(action, {})
    assert "PRISMABUILD_ACTION_NONCE" not in dropped

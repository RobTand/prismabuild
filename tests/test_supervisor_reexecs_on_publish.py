"""The long-lived supervisor follows an immutable published generation."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

TOOLS = Path(__file__).resolve().parents[1] / "tools" / "fleet"
sys.path.insert(0, str(TOOLS))

import supervise  # noqa: E402


COMMIT = "a" * 40


def _generation(store: Path, name: str, *, body: str = "# supervisor\n") -> Path:
    root = store / name
    script = root / "tools" / "supervise.py"
    script.parent.mkdir(parents=True)
    script.write_text(body)
    digest = hashlib.sha256(script.read_bytes()).hexdigest()
    nested = root / "tools" / "fleet" / "supervise.py"
    nested.parent.mkdir(parents=True)
    nested.write_text(body)
    (root / "RUNTIME_VERSION.json").write_text(json.dumps({
        "schema": supervise.RUNTIME_VERSION_SCHEMA,
        "commit": COMMIT,
        "generation": name,
        "files": {"tools/supervise.py": digest,
                  "tools/fleet/supervise.py": digest},
    }))
    return root


def _fleet(tmp_path: Path, monkeypatch):
    mirror = tmp_path / "fleet"
    store = mirror / "runtime-generations"
    old = _generation(store, "aaaaaaaaaaaa-100-old")
    new = _generation(store, "aaaaaaaaaaaa-200-new", body="# replacement\n")
    (mirror / "repo").symlink_to(new)
    monkeypatch.setattr(supervise, "MIRROR", mirror)
    monkeypatch.setattr(supervise, "CLAIM", tmp_path / "supervisor.claim")
    return old, new


def test_same_commit_republication_reexecs_and_preserves_arguments_and_lock(
    tmp_path, monkeypatch,
):
    old_root, new_root = _fleet(tmp_path, monkeypatch)
    old = supervise._published_generation(old_root)
    new = supervise._published_generation()
    assert old is not None and new is not None
    assert old.commit == new.commit == COMMIT
    assert old.root != new.root

    handle = supervise._claim_handle(ensure=False)
    assert handle is not None
    monkeypatch.setattr(sys, "argv", [str(old.supervisor), "--loops", "7",
                                      "--interval-s", "4"])
    called = {}

    def execve(executable, argv, environment):
        called.update(executable=executable, argv=argv, environment=environment,
                      inheritable=os.get_inheritable(handle.fileno()))

    monkeypatch.setattr(supervise.os, "execve", execve)
    assert supervise._reexec_if_published(old, handle) is True

    assert called["argv"] == [sys.executable, str(new_root / "tools/supervise.py"),
                              "--loops", "7", "--interval-s", "4"]
    assert called["environment"][supervise.INHERITED_CLAIM_FD_ENV] == str(
        handle.fileno())
    assert called["inheritable"] is True
    assert os.get_inheritable(handle.fileno()) is False
    contender = supervise.CLAIM.open("r+")
    with pytest.raises(BlockingIOError):
        fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
    contender.close()
    handle.close()


def test_main_reexec_boundary_never_inspects_or_signals_active_workers(
    tmp_path, monkeypatch,
):
    old_root, _new_root = _fleet(tmp_path, monkeypatch)
    old = supervise._published_generation(old_root)
    new = supervise._published_generation()
    assert old is not None and new is not None

    monkeypatch.setattr(sys, "argv", ["supervise", "--loops", "9"])
    monkeypatch.setattr(supervise, "declared_shape",
                        lambda *_a, **_k: (9, ["--class", "x86"]))
    monkeypatch.setattr(
        supervise, "_loaded_published_generation", lambda: old,
    )
    monkeypatch.setattr(supervise, "_published_generation", lambda _root: new)
    monkeypatch.setattr(supervise.os, "execve", lambda *_a: None)
    monkeypatch.setattr(supervise, "_live_loops",
                        lambda: pytest.fail("reexec must precede the worker census"))
    monkeypatch.setattr(supervise.os, "kill",
                        lambda *_a: pytest.fail("reexec must not signal workers"))

    assert supervise.main() == 0


def test_once_never_reexecs(tmp_path, monkeypatch):
    old_root, _new_root = _fleet(tmp_path, monkeypatch)
    old = supervise._published_generation(old_root)
    assert old is not None
    monkeypatch.setattr(sys, "argv", ["supervise", "--once"])
    monkeypatch.setattr(supervise, "declared_shape", lambda *_a, **_k: (0, []))
    monkeypatch.setattr(supervise, "_loaded_published_generation", lambda: old)
    monkeypatch.setattr(supervise, "_reexec_if_published",
                        lambda *_a: pytest.fail("--once must not reexec"))
    monkeypatch.setattr(supervise, "_live_loops", lambda: [])
    monkeypatch.setattr(supervise, "_claim_holders", lambda: frozenset())
    monkeypatch.setattr(supervise, "_ready_backlog", lambda: False)

    assert supervise.main() == 0


def test_receipt_must_match_the_generation_name_and_supervisor_bytes(
    tmp_path, monkeypatch,
):
    _old_root, new_root = _fleet(tmp_path, monkeypatch)
    receipt = new_root / "RUNTIME_VERSION.json"
    record = json.loads(receipt.read_text())
    record["generation"] = "some-other-generation"
    receipt.write_text(json.dumps(record))
    assert supervise._published_generation() is None

    record["generation"] = new_root.name
    receipt.write_text(json.dumps(record))
    (new_root / "tools" / "supervise.py").write_text("changed after receipt\n")
    assert supervise._published_generation() is None


def test_loaded_identity_validates_the_exact_supported_entrypoint(tmp_path, monkeypatch):
    _old_root, new_root = _fleet(tmp_path, monkeypatch)
    nested = new_root / "tools" / "fleet" / "supervise.py"
    assert supervise._published_generation(new_root, entrypoint=nested) is not None
    nested.write_text("changed nested entrypoint\n")
    assert supervise._published_generation(new_root, entrypoint=nested) is None


def test_inherited_descriptor_must_name_the_claim(tmp_path, monkeypatch):
    claim = tmp_path / "supervisor.claim"
    claim.write_text("claim\n")
    unrelated = os.open(tmp_path / "unrelated", os.O_CREAT | os.O_RDWR, 0o600)
    monkeypatch.setattr(supervise, "CLAIM", claim)
    monkeypatch.setenv(supervise.INHERITED_CLAIM_FD_ENV,
                       str(unrelated))

    with pytest.raises(SystemExit, match="does not name the supervisor claim"):
        supervise._claim_handle(ensure=False)

    with pytest.raises(OSError):
        os.fstat(unrelated)


def test_inherited_flock_survives_a_real_exec_and_excludes_ensure(
    tmp_path, monkeypatch,
):
    claim = tmp_path / "supervisor.claim"
    ready = tmp_path / "ready"
    release = tmp_path / "release"
    monkeypatch.setattr(supervise, "CLAIM", claim)
    original = supervise._claim_handle(ensure=False)
    assert original is not None
    descriptor = original.fileno()
    os.set_inheritable(descriptor, True)
    child_code = f"""
import os, sys, time
from pathlib import Path
sys.path.insert(0, {str(TOOLS)!r})
import supervise
supervise.CLAIM = Path({str(claim)!r})
handle = supervise._claim_handle(ensure=False)
Path({str(ready)!r}).write_text('ready')
while not Path({str(release)!r}).exists():
    time.sleep(.01)
handle.close()
"""
    environment = {**os.environ,
                   supervise.INHERITED_CLAIM_FD_ENV: str(descriptor)}
    child = subprocess.Popen(
        [sys.executable, "-c", child_code], env=environment,
        pass_fds=(descriptor,), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True,
    )
    original.close()
    deadline = time.monotonic() + 5
    while not ready.exists() and child.poll() is None and time.monotonic() < deadline:
        time.sleep(.01)
    if not ready.exists():
        stdout, stderr = child.communicate(timeout=5)
        pytest.fail(f"exec child did not adopt the lock: {stdout=} {stderr=}")

    assert supervise._claim_handle(ensure=True) is None
    release.write_text("release")
    stdout, stderr = child.communicate(timeout=5)
    assert child.returncode == 0, (stdout, stderr)
    successor = supervise._claim_handle(ensure=False)
    assert successor is not None
    successor.close()

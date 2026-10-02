"""Real Git source checks remain clean while the existing profiler runs."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess

import pytest

from prismabuild import core as pb
from test_sample_profile import _FakeBackend, _action


CHECK = (
    "import json, subprocess, sys; "
    'status = subprocess.check_output(["git", "status", "--porcelain", '
    '"--untracked-files=all"], text=True); '
    'print(json.dumps({"source_clean": not status, "dirty": status}), flush=True); '
    "sys.exit(0 if not status else 91)"
)


def _checkout(root):
    root.mkdir()
    (root / ".gitignore").write_text("/result.txt\n/package/result.txt\n")
    (root / "task_code.py").write_text("# closure member\n")
    (root / "package").mkdir()
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    subprocess.run(["git", "-C", str(root), "add", "."], check=True)
    subprocess.run(["git", "-C", str(root), "-c", "user.name=fixture",
                    "-c", "user.email=fixture@example.invalid",
                    "commit", "-qm", "fixture"], check=True)
    return root


class _RecordingBackend(_FakeBackend):
    container_routes_unsupported = True

    def launch_argv(self, argv, *, profile_path):
        self.path = profile_path
        return super().launch_argv(argv, profile_path=profile_path)


@pytest.mark.parametrize("working_directory", [".", "package"])
def test_profiler_metadata_is_outside_the_complete_git_checkout(tmp_path, monkeypatch,
                                                               working_directory):
    checkout = _checkout(tmp_path / "checkout")
    backend = _RecordingBackend()
    monkeypatch.setitem(pb.PROFILE_BACKENDS, "sample", backend)
    # A user-selected TMPDIR below the source must not put diagnostics there.
    monkeypatch.setattr(pb.tempfile, "tempdir", str(checkout / "package"))
    action = _action(checkout, profile="sample", work=CHECK)
    body = {k: v for k, v in action.items() if k != "action_key"}
    body["task"] = {**body["task"], "working_directory": working_directory}
    action = pb.seal_action(body)
    result = pb.run_local_action(action, cas_root=tmp_path / "cas",
                                 checkout_root=checkout)
    payload = json.loads(Path(result["payload_path"]).read_text())
    assert payload == {"source_clean": True, "dirty": ""}
    assert not backend.path.is_relative_to(checkout)
    assert not backend.path.parent.exists()
    assert result["profile"]["produced"] is True


def test_true_untracked_input_is_not_hidden_or_removed(tmp_path, monkeypatch):
    checkout = _checkout(tmp_path / "checkout")
    foreign = checkout / pb.PROFILE_SCRATCH_DIRNAME / "real-input"
    foreign.parent.mkdir()
    foreign.write_bytes(b"keep the user's bytes")
    backend = _RecordingBackend()
    monkeypatch.setitem(pb.PROFILE_BACKENDS, "sample", backend)
    action = _action(checkout, profile="sample", work=CHECK)
    with pytest.raises(pb.LocalActionError) as caught:
        pb.run_local_action(action, cas_root=tmp_path / "cas", checkout_root=checkout)
    assert caught.value.returncode == 91
    assert foreign.read_bytes() == b"keep the user's bytes"
    assert not backend.path.is_relative_to(checkout)
    assert not backend.path.parent.exists()


def test_real_changed_closure_still_refuses_before_profile_launch(tmp_path, monkeypatch):
    checkout = _checkout(tmp_path / "checkout")
    backend = _RecordingBackend()
    monkeypatch.setitem(pb.PROFILE_BACKENDS, "sample", backend)
    action = _action(checkout, profile="sample", work=CHECK)
    (checkout / "task_code.py").write_text("# actual changed input\n")
    with pytest.raises(pb.ActionContractError, match="live code closure differs"):
        pb.run_local_action(action, cas_root=tmp_path / "cas", checkout_root=checkout)
    assert not hasattr(backend, "path")


def test_attempt_directories_are_private_unique_and_collisions_preserve_bytes(tmp_path,
                                                                          monkeypatch):
    checkout = _checkout(tmp_path / "checkout")
    monkeypatch.setitem(pb.PROFILE_BACKENDS, "sample", _RecordingBackend())
    action = _action(checkout, profile="sample", work=CHECK)
    first = pb._profile_session(action, working_directory=checkout / "package",
                                checkout_root=checkout)
    second = pb._profile_session(action, working_directory=checkout / "package",
                                 checkout_root=checkout)
    assert first.directory != second.directory
    assert not first.directory.exists() and not second.directory.exists()
    first.directory.mkdir()
    foreign = first.directory / "user-input"
    foreign.write_bytes(b"not owned by this session")
    with pytest.raises(pb.LocalActionError, match="already exists"):
        with pb._profile_scratch(first):
            first._open()
    assert foreign.read_bytes() == b"not owned by this session"
    with pb._profile_scratch(second):
        second._open()
        assert second.directory.stat().st_mode & 0o777 == 0o700
    assert not second.directory.exists()

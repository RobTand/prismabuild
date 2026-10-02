"""The shared sealed capture boundary preserves bytes and both exit codes."""
from __future__ import annotations

import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import movement_actions


def _capture(tmp_path, command, *, capture_status=0, producer_status=None, prefer_gnu=False):
    tools = tmp_path / "capture-tools"
    tools.mkdir()
    names = ("tee", "gnutee") if prefer_gnu else ("tee",)
    for name in names:
        status = 0 if name == "gnutee" else capture_status
        body = (
            "import pathlib,sys\n"
            "data=sys.stdin.buffer.read()\n"
            f"status={status}\n"
            "pathlib.Path(sys.argv[1]).write_bytes(data if status==0 else data[:3])\n"
            "sys.stdout.buffer.write(data if status==0 else data[:3])\n"
            "sys.exit(status)\n"
        )
        tool = tools / name
        tool.write_text(f"#!{sys.executable}\n" + body)
        tool.chmod(0o755)
    log = tmp_path / "result with spaces.log"
    argv = movement_actions.captured_command(command, str(log))
    done = subprocess.run(
        ["/bin/bash", "--noprofile", "--norc", "-c", argv],
        env={**os.environ, "PATH": str(tools)}, capture_output=True,
    )
    return done, log, tools


@pytest.mark.parametrize("producer_status,expected", [(0, 42), (7, 7)])
def test_capture_failure_cannot_publish_success_and_preserves_producer_error(
    tmp_path, producer_status, expected,
):
    producer = [sys.executable, "-c",
                f"import sys;sys.stdout.buffer.write(b'complete payload');sys.exit({producer_status})"]
    done, log, tools = _capture(tmp_path, producer, capture_status=42)
    assert log.read_bytes() == b"com"
    assert done.returncode == expected
    assert done.stdout == b"com"
    assert str(tools / "tee").encode() in done.stderr
    assert b"capture status=42" in done.stderr
    assert f"producer status={producer_status}".encode() in done.stderr


def test_gnu_capture_is_preferred_without_changing_the_action_path(tmp_path):
    data = b"complete bytes"
    producer = [sys.executable, "-c", f"import sys;sys.stdout.buffer.write({data!r})"]
    done, log, tools = _capture(tmp_path, producer, capture_status=42, prefer_gnu=True)
    assert done.returncode == 0
    assert log.read_bytes() == done.stdout == data
    assert str(tools / "gnutee").encode() in done.stderr


def test_tee_fallback_still_records_its_actual_executable(tmp_path):
    producer = [sys.executable, "-c", "print('fallback')"]
    done, log, tools = _capture(tmp_path, producer)
    assert done.returncode == 0
    assert log.read_bytes() == done.stdout == b"fallback\n"
    assert str(tools / "tee").encode() in done.stderr


def test_missing_capture_tool_is_an_explicit_failure(tmp_path):
    log = tmp_path / "missing.log"
    done = subprocess.run(
        ["/bin/bash", "--noprofile", "--norc", "-c",
         movement_actions.captured_command([sys.executable, "-c", "print('data')"], str(log))],
        env={**os.environ, "PATH": str(tmp_path)}, capture_output=True,
    )
    assert done.returncode != 0
    assert b"log capture executable unavailable" in done.stderr
    assert not log.exists()


@pytest.mark.parametrize("blocks", [1, 32])
def test_native_gnu_capture_keeps_binary_burst_bytes_exact(tmp_path, blocks):
    gnu = shutil.which("gnutee")
    if gnu is None:
        tee = shutil.which("tee")
        version = subprocess.run([tee, "--version"], capture_output=True) if tee else None
        if not version or b"GNU coreutils" not in version.stdout:
            pytest.skip("GNU tee/gnutee is not available in the scoped test environment")
        gnu = tee
    tool_dir = tmp_path / "native-capture"
    tool_dir.mkdir()
    (tool_dir / "gnutee").symlink_to(gnu)
    data = bytes(range(256)) * 128 * blocks + b"\x00\xfftrailing\n"
    producer = [sys.executable, "-c",
                f"import sys;sys.stdout.buffer.write(bytes(range(256))*128*{blocks}+b'\\x00\\xfftrailing\\n')"]
    log = tmp_path / "native.log"
    done = subprocess.run(
        ["/bin/bash", "--noprofile", "--norc", "-c",
         movement_actions.captured_command(producer, str(log))],
        env={**os.environ, "PATH": str(tool_dir)}, capture_output=True,
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout == log.read_bytes() == data
    assert str(tool_dir / "gnutee").encode() in done.stderr

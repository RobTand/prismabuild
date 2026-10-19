"""Capture must preserve exec arguments instead of joining them into one string."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import movement_actions


def _run(command, tmp_path):
    log = tmp_path / "log ' with $shell; characters.txt"
    argv = movement_actions.standard_capture_argv(command, str(log), path_prefix="/usr/bin")
    done = subprocess.run(argv, capture_output=True, env={**os.environ, "PATH": "/usr/bin:/bin"})
    return done, log


@pytest.mark.parametrize("count,width", [(3, 56_000), (1300, 120)])
def test_individually_valid_arguments_launch_above_shell_string_limit(tmp_path, count, width):
    arguments = [str(index).ljust(width, "x") for index in range(count)]
    program = "import hashlib,sys;print(hashlib.sha256(chr(0).join(sys.argv[1:]).encode()).hexdigest())"
    command = [sys.executable, "-c", program, *arguments]
    assert max(len(os.fsencode(arg)) for arg in command) < 131_072
    assert len(os.fsencode(shlex.join(command))) > 131_072
    expected = hashlib.sha256("\0".join(arguments).encode()).hexdigest().encode() + b"\n"
    done, log = _run(command, tmp_path)
    assert done.returncode == 0, done.stderr
    assert done.stdout == log.read_bytes() == expected


@pytest.mark.parametrize("arguments", [[], [""], ["", ""], [""] * 129,
    ["a b", "'\"", "$HOME", "$(touch planted)", "; exit 91", "*", "a\nb", "\\", "--", "é"]])
@pytest.mark.parametrize("status", [0, 7, 42])
def test_argument_boundaries_logs_and_exit_status(tmp_path, arguments, status):
    program = ("import json,os,sys;os.write(1,json.dumps(sys.argv[1:]).encode());"
               f"os.write(2,b' stderr');sys.exit({status})")
    done, log = _run([sys.executable, "-c", program, *arguments], tmp_path)
    assert done.returncode == status
    assert done.stdout == log.read_bytes() == json.dumps(arguments).encode() + b" stderr"
    assert b"pbrun: log capture executable=" in done.stderr
    assert not (tmp_path / "planted").exists()


@pytest.mark.parametrize("command,status", [([], 0), ([""], 127), (["/bin/true"], 0)])
def test_command_argument_count_edges(tmp_path, command, status):
    done, log = _run(command, tmp_path)
    assert done.returncode == status
    assert done.stdout == log.read_bytes()

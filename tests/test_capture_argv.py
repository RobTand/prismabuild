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
from prismabuild import action_result, core as pb, movement_actions


def _run(command, tmp_path, *, standard=True):
    log = tmp_path / "log ' with $shell; characters.txt"
    argv = (movement_actions.standard_capture_argv(command, str(log), path_prefix="/usr/bin")
            if standard else movement_actions.captured_command(command, str(log)))
    done = subprocess.run(argv, capture_output=True, env={**os.environ, "PATH": "/usr/bin:/bin"})
    return done, log


@pytest.mark.parametrize("count,width", [(3, 56_000), (1300, 120)])
@pytest.mark.parametrize("standard", [True, False])
def test_individually_valid_arguments_launch_above_shell_string_limit(tmp_path, count, width, standard):
    arguments = [str(index).ljust(width, "x") for index in range(count)]
    program = "import hashlib,sys;print(hashlib.sha256(chr(0).join(sys.argv[1:]).encode()).hexdigest())"
    command = [sys.executable, "-c", program, *arguments]
    assert max(len(os.fsencode(arg)) for arg in command) < 131_072
    assert len(os.fsencode(shlex.join(command))) > 131_072
    expected = hashlib.sha256("\0".join(arguments).encode()).hexdigest().encode() + b"\n"
    done, log = _run(command, tmp_path, standard=standard)
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


def test_s5_sealed_capture_still_verifies_and_runs(tmp_path, monkeypatch):
    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT"):
        monkeypatch.delenv(name, raising=False)
    fixture = json.loads((Path(__file__).parent / "fixtures/capture_argv_s5.json").read_text())
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task_code.py").write_text("pass\n")
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": fixture["argv"], "working_directory": ".",
                 "result_path": fixture["log_name"]},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task_code.py"]),
        "params": {"command": fixture["command"]},
        "environment": {"variables": {"PATH": "/usr/bin:/bin"}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None, "host_class": None},
    })
    before = json.dumps(action, sort_keys=True)
    assert action_result.bind_standard_capture_command(action) == fixture["argv"]
    result = pb.run_local_action(action, cas_root=tmp_path / "cas", checkout_root=checkout)
    assert Path(result["payload_path"]).read_bytes() == b"old capture\n"
    assert pb.PrismaBuildCAS(tmp_path / "cas").lookup(action) == result["receipt"]
    assert json.dumps(action, sort_keys=True) == before
    changed = {**action, "task": {**action["task"], "argv": [*fixture["argv"], "extra"]}}
    changed.pop("action_key")
    with pytest.raises(action_result.ActionResultError, match="standard captured-log"):
        action_result.bind_standard_capture_command(pb.seal_action(changed))

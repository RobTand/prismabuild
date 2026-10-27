"""Deterministic leg-2 completion checks, without a Docker daemon or GPU."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

FLEET_DIR = Path(__file__).resolve().parents[1] / "tools" / "fleet"
if str(FLEET_DIR) not in sys.path:
    sys.path.insert(0, str(FLEET_DIR))
from pbcanary_legs import leg2

IMAGE = "example.org/canary@sha256:" + "ab" * 32


class _Vector:
    shape = (16,)

    def __init__(self, values):
        self.values = values

    def __mul__(self, other):
        return _Vector([left * right for left, right in zip(self.values, other.values)])

    def sum(self):
        return sum(self.values)


def _cpu_torch(*, devices=0, bad_shape=False, bad_value=False):
    def arange(size, *, device):
        if device != "cpu":
            raise AssertionError("CPU preflight attempted a CUDA tensor")
        vector = _Vector(list(range(size)))
        if bad_shape:
            vector.shape = (8, 2)
        if bad_value:
            vector.values[-1] = 0
        return vector
    return SimpleNamespace(__version__="test", version=SimpleNamespace(cuda=None),
                           cuda=SimpleNamespace(device_count=lambda: devices), arange=arange)


def _inside_args(tmp_path):
    data = tmp_path / "input.bin"
    data.write_bytes(b"required canary input\n")
    checks = tmp_path / "checks.json"
    return checks, [
        "--inside", "--image", IMAGE, "--device", "cpu", "--checks-path", str(checks),
        "--payload-sha256", hashlib.sha256(b"pbcanary-leg2:v1").hexdigest(),
        "--source-sha256", hashlib.sha256(Path(leg2.__file__).read_bytes()).hexdigest(),
        "--input", f"fixture={data}={hashlib.sha256(data.read_bytes()).hexdigest()}",
    ]


def test_cpu_payload_reads_input_and_commits_checks(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "torch", _cpu_torch())
    checks, args = _inside_args(tmp_path)
    monkeypatch.setenv("PBCANARY_VERIFIED_IMAGE", IMAGE)
    real_open = Path.open
    reads = []
    def record_open(path, *positional, **keywords):
        if path == tmp_path / "input.bin":
            reads.append(path)
        return real_open(path, *positional, **keywords)
    monkeypatch.setattr(Path, "open", record_open)
    assert leg2.main(args) == 0
    result = json.loads(checks.read_bytes())
    assert result["schema"] == "fleet.d38.leg2_checks.v1"
    assert result["checks"] == dict.fromkeys([
        "imports", "arguments", "payload_digest", "image_digest", "shape",
        "kernel_value", "input_reads", "device_visibility",
    ], "passed")
    assert result["inputs"] == ["fixture"]
    assert result["device"] == "cpu"
    assert result["shape"] == [16]
    assert result["kernel_value"] == 1240
    assert len(reads) == 1


@pytest.mark.parametrize("failure", ["cuda_visible", "shape", "value", "input", "image", "source", "payload"])
def test_failed_check_writes_no_completion(tmp_path, monkeypatch, failure):
    monkeypatch.setitem(sys.modules, "torch", _cpu_torch(
        devices=int(failure == "cuda_visible"), bad_shape=failure == "shape",
        bad_value=failure == "value"))
    checks, args = _inside_args(tmp_path)
    monkeypatch.setenv("PBCANARY_VERIFIED_IMAGE", IMAGE)
    if failure == "input":
        (tmp_path / "input.bin").write_bytes(b"changed after sealing")
    elif failure == "image":
        monkeypatch.setenv("PBCANARY_VERIFIED_IMAGE", "example.org/other@sha256:" + "cd" * 32)
    elif failure == "source":
        args[args.index("--source-sha256") + 1] = "0" * 64
    elif failure == "payload":
        args[args.index("--payload-sha256") + 1] = "0" * 64
    assert leg2.main(args) != 0
    assert not checks.exists()


@pytest.mark.parametrize("device", ["cuda", "cpu"])
def test_container_namespace_retains_entrypoint_and_cpu_visibility(tmp_path, monkeypatch, device):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(leg2.shutil, "which", lambda _: "/pb-runtime/docker")
    args = leg2.parse_arguments([
        "--image", IMAGE, "--device", device, "--checks-path", ".d38/checks.json",
    ])
    command = leg2.container_argv(args, "0" * 64)
    assert command[:4] == ["/usr/bin/python3", "-I", "/pb-runtime/docker", "run"]
    assert command[command.index("--entrypoint") + 1] == ""
    assert command[command.index("--workdir") + 1] == "/pb-canary"
    if device == "cpu":
        assert "--gpus" not in command
        assert "CUDA_VISIBLE_DEVICES=" in command
        assert "NVIDIA_VISIBLE_DEVICES=none" in command
    else:
        assert command[command.index("--gpus") + 1] == "all"
    assert command[command.index(IMAGE) + 1:command.index("--inside")] == [
        "python3", "-I", "/pb-canary/leg2.py",
    ]


def test_oversized_input_refuses_bounded_read(tmp_path, monkeypatch, capsys):
    monkeypatch.setitem(sys.modules, "torch", _cpu_torch())
    checks, args = _inside_args(tmp_path)
    monkeypatch.setenv("PBCANARY_VERIFIED_IMAGE", IMAGE)
    data = b"x" * (64 * 1024 + 1)
    (tmp_path / "input.bin").write_bytes(data)
    args[-1] = f"fixture={tmp_path / 'input.bin'}={hashlib.sha256(data).hexdigest()}"
    assert leg2.main(args) != 0
    assert not checks.exists()
    assert "bounded canary read" in capsys.readouterr().err

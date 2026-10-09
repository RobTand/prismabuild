"""Each pbtest attempt owns its base temp; success removes it (#1542).

A passing shard leaves no ``pytest-N`` or attempt base temp under its
``TMPDIR``. A failing shard keeps the failed test's ``tmp_path``. An
explicit sealed ``--basetemp`` keeps its existing meaning, and the
compiler ``TMPDIR`` stays sealed.
"""
from __future__ import annotations

import importlib.util
import json
import os
import socket
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pbtest1542", ROOT / "tools" / "fleet" / "pbtest.py")
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)

ACTION_KEY = "ab12" * 16
NONCE = "cd34" * 8


def _checkout(parent: Path, body: str) -> Path:
    checkout = parent / "co"
    tests = checkout / "tests"
    tests.mkdir(parents=True)
    (tests / "test_one.py").write_text(
        "def test_one(tmp_path):\n"
        "    (tmp_path / 'marker').write_text('x')\n"
        f"    {body}\n")
    return checkout


def _run(checkout: Path, scratch: Path, workers: int = 1) -> subprocess.CompletedProcess:
    program = pbtest.shard_entry(sys.executable, checkout, collection=True)[2]
    assert "--basetemp" in program
    environment = dict(os.environ, TMPDIR=str(scratch),
                       PRISMABUILD_ACTION_KEY=ACTION_KEY,
                       PRISMABUILD_ACTION_NONCE=NONCE,
                       PYTHONDONTWRITEBYTECODE="1", PYTEST_ADDOPTS="")
    return subprocess.run(
        [sys.executable, "-c", program,
         json.dumps({"files": ["tests/test_one.py"],
                     "roots": [["tests", "directory"]]}),
         "-q", "--no-header", "-p", "no:cacheprovider",
         *(["-n", str(workers)] if workers > 1 else []), "tests/test_one.py"],
        cwd=checkout, env=environment, capture_output=True, text=True,
        timeout=240)


def test_a_passing_shard_removes_its_attempt_base_temp(tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    checkout = _checkout(tmp_path, "assert True")
    result = _run(checkout, scratch)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "1 passed" in result.stdout
    assert list(scratch.rglob("*")) == [], sorted(
        path.relative_to(scratch).as_posix() for path in scratch.rglob("*"))


def test_a_passing_shard_keeps_read_only_scratch_without_changing_its_exit_code(tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    checkout = _checkout(tmp_path, "assert True")
    (checkout / "tests" / "test_one.py").write_text(
        "def test_one(tmp_path_factory):\n"
        "    directory = tmp_path_factory.mktemp('readonly')\n"
        "    (directory / 'marker').write_text('kept')\n"
        "    directory.chmod(0o500)\n")
    try:
        result = _run(checkout, scratch)
        assert result.returncode == 0, result.stdout + result.stderr
        assert "1 passed" in result.stdout
        marker, = scratch.rglob("marker")
        assert marker.read_text() == "kept"
        attempt, = scratch.iterdir()
        record = json.loads((attempt / ".pbtest-owner.json").read_text())
        assert record["kept_unix"] > 0
        assert (attempt / ".pbtest-lock").is_file()
    finally:
        for marker in scratch.rglob("marker"):
            marker.parent.chmod(0o700)


def test_a_failing_shard_keeps_the_failed_tests_tmp_path(tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    checkout = _checkout(tmp_path, "assert False, 'kept for diagnosis'")
    result = _run(checkout, scratch)
    assert result.returncode == 1, result.stdout + result.stderr
    markers = list(scratch.rglob("marker"))
    assert markers, "the failed test's tmp_path is gone"
    assert all(marker.read_text() == "x" for marker in markers)


def test_success_removes_only_its_directory_and_keeps_failed_attempts(tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    checkout = _checkout(tmp_path, "assert False, 'kept for diagnosis'")
    for _ in range(2):
        result = _run(checkout, scratch)
        assert result.returncode == 1, result.stdout + result.stderr
    retained = set(scratch.iterdir())
    assert len(retained) == 2
    markers = set(scratch.rglob("marker"))
    assert len(markers) == 2
    (checkout / "tests" / "test_one.py").write_text("def test_one():\n    assert True\n")
    result = _run(checkout, scratch)
    assert result.returncode == 0, result.stdout + result.stderr
    assert set(scratch.iterdir()) == retained
    assert all(marker.read_text() == "x" for marker in markers)


def test_an_explicit_basetemp_keeps_its_meaning_and_the_compiler_tmpdir(tmp_path, monkeypatch):
    from pbtest_shard_output import ONE_PASS, ShardProcess, shard_environment
    checkout = tmp_path / "checkout"
    tests = checkout / "tests"
    tests.mkdir(parents=True)
    (tests / "test_one.py").write_text("def test_one():\n    assert True\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    calls = []

    class Finished(ShardProcess):
        returncode = 0

        def communicate(self):
            return ONE_PASS, None

    def popen(command, **_kwargs):
        calls.append(command)
        return Finished()

    with monkeypatch.context() as scoped:
        from test_pbtest import PUBLISHED_RUNTIME
        scoped.setattr(pbtest.subprocess, "Popen", popen)
        scoped.setattr(pbtest, "RUNTIME_ROOT", PUBLISHED_RUNTIME)
        scoped.setattr(sys, "argv", [
            "pbtest.py", "--checkout", str(checkout), "--python",
            "/target/python", "--shards", "1", "--basetemp", "/qualified/scratch",
            "--tmpdir", "/worker/scratch", "tests"])
        assert pbtest.main() == 0
    assert len(calls) == 1
    program = calls[0][calls[0].index("-c") + 1]
    assert "/qualified/scratch" in program
    assert "_pb_run_and_clean_attempt_base" not in program
    assert shard_environment(calls[0])["TMPDIR"] == "/worker/scratch"


def test_attempt_namespaces_do_not_overlap_without_a_launcher_nonce(tmp_path, monkeypatch):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    checkout = _checkout(tmp_path, "assert True")
    program = pbtest.shard_entry(sys.executable, checkout, collection=True)[2]
    monkeypatch.delenv("PRISMABUILD_ACTION_NONCE", raising=False)
    first, second = [], []
    for record in (first, second):
        environment = dict(os.environ, TMPDIR=str(scratch),
                           PRISMABUILD_ACTION_KEY=ACTION_KEY,
                           PYTHONDONTWRITEBYTECODE="1", PYTEST_ADDOPTS="")
        environment.pop("PRISMABUILD_ACTION_NONCE", None)
        result = subprocess.run(
            [sys.executable, "-c",
             "import json, os, sys\n"
             + program.split("# A pbtest shard:", 1)[0]
             + "\nprint('DERIVED=' + _pb_owned)\n",
             json.dumps({"files": ["tests/test_one.py"],
                         "roots": [["tests", "directory"]]}),
             "-q", "tests/test_one.py"],
            cwd=checkout, env=environment, capture_output=True, text=True,
            timeout=120)
        assert result.returncode == 0, result.stdout + result.stderr
        record.append(next(line for line in result.stdout.splitlines()
                           if line.startswith("DERIVED=")))
    assert first != second


@pytest.mark.parametrize("worker", [None, "popen-gw0"])
def test_default_shard_socket_path_fits_linux_limit(tmp_path, monkeypatch, worker):
    program = pbtest.shard_entry(sys.executable, tmp_path, collection=True)[2]
    monkeypatch.setenv("TMPDIR", "/home/rob/tmp")
    monkeypatch.setenv("PRISMABUILD_ACTION_KEY", uuid.uuid4().hex * 2)
    monkeypatch.setenv("PRISMABUILD_ACTION_NONCE", uuid.uuid4().hex)
    monkeypatch.setattr(sys, "argv", ["-c", "{}", "-q", "tests/test_one.py"])
    namespace = {}
    exec(program.split("# A pbtest shard:", 1)[0], namespace)
    base = Path(sys.argv[sys.argv.index("--basetemp") + 1])
    test_directory = base / worker if worker else base
    # pytest truncates the test name to 30 characters, then adds a counter.
    socket_path = test_directory / ("test_" + "x" * 25 + "0") / "service.sock"
    try:
        assert len(os.fsencode(socket_path)) <= 100, str(socket_path)
        socket_path.parent.mkdir(parents=True)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(socket_path))
    finally:
        namespace["_pb_run_and_clean_attempt_base"](lambda: 0)


@pytest.mark.parametrize("workers", [1, 2], ids=["single", "xdist"])
def test_real_shard_binds_socket_and_cleans_its_base_temp(tmp_path, workers):
    checkout = _checkout(tmp_path, "assert True")
    (checkout / "tests" / "test_one.py").write_text(
        "import json, os, socket\n"
        "from pathlib import Path\n"
        "def test_xxxxxxxxxxxxxxxxxxxxxxxxx(tmp_path):\n"
        "    path = tmp_path / 'service.sock'\n"
        "    assert os.environ['TMPDIR'] == '/home/rob/tmp'\n"
        "    assert len(os.fsencode(path)) <= 100, str(path)\n"
        "    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:\n"
        "        server.bind(str(path))\n"
        "    Path('socket-path.json').write_text(json.dumps(str(path)))\n")
    result = _run(checkout, Path("/home/rob/tmp"), workers)
    assert result.returncode == 0, result.stdout + result.stderr
    path = Path(json.loads((checkout / "socket-path.json").read_text()))
    base = path.parent.parent.parent if workers > 1 else path.parent.parent
    assert not base.parent.exists(), str(base.parent)

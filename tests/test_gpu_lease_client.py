"""Tests for the vendored gate GPU lease client, ``tools/gpu_lease/pb_gpu_lease.py``.

The client is one stdlib-only file that other repos copy, so these tests load it
by path rather than importing it from the package tree. Waiting is driven
through the client's ``_mono``/``_sleep`` hooks: a fake clock advances virtual
time on every poll and plays the part of PrismaBuild by writing
``pb-state.json`` at a chosen poll. Nothing here sleeps or asserts on elapsed
real time. Subprocess tests synchronize on what the child prints and on pipe
EOF; the timeouts are failure bounds only.
"""
from __future__ import annotations

import ast
import fcntl
import importlib.util
import itertools
import json
import os
import select
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
CLIENT = REPO / "tools" / "gpu_lease" / "pb_gpu_lease.py"
DESIGN = REPO / "docs" / "design.md"
BOUND_S = 120  # failure bound only: nothing is expected to take this long

STDLIB_ALLOWLIST = frozenset(
    "argparse collections contextlib ctypes fcntl json os secrets signal socket "
    "subprocess sys threading time".split()
)


def _load():
    assert CLIENT.is_file(), "missing %s" % CLIENT
    spec = importlib.util.spec_from_file_location("pb_gpu_lease_under_test", CLIENT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _client(monkeypatch):
    """Load the client with renewal parked, so only tests that want it see it."""
    module = _load()
    monkeypatch.delenv("PB_GPU_LEASE_DIR", raising=False)
    monkeypatch.delenv("TEST_TARGET", raising=False)
    monkeypatch.setattr(module, "RENEW_S", 3600)
    return module


def _write_state(tmp_path, *, updated=None, fencing=(), ready=(), tokens=0):
    """Play PrismaBuild. By default the state is fresh (dated ahead of any clock skew)."""
    doc = {
        "schema": "prismabuild.gpu_lease_state.v1",
        "host": socket.gethostname(),
        "updated_unix": time.time() + 3600 if updated is None else updated,
        "fencing": sorted(fencing),
        "ready": sorted(ready),
        "pb_gpu_tokens_held": tokens,
        "pb_mem_gb_held": 0,
        "pb_cpus_held": 0,
    }
    (tmp_path / "pb-state.json").write_text(json.dumps(doc))


def _lease_id(tmp_path):
    locks = sorted(tmp_path.glob("*.lock"))
    assert len(locks) == 1, locks
    return locks[0].stem


def _read_lease(tmp_path, lease_id=None):
    path = tmp_path / ("%s.json" % (lease_id or _lease_id(tmp_path)))
    return json.loads(path.read_text())


def _lock_is_free(tmp_path, lease_id=None):
    """The PrismaBuild liveness probe: LOCK_SH|LOCK_NB on a fresh descriptor."""
    fd = os.open(str(tmp_path / ("%s.lock" % (lease_id or _lease_id(tmp_path)))), os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False
    finally:
        os.close(fd)


class _Clock:
    """Virtual time: every sleep advances it by the requested step and then calls ``on_poll(n)``."""

    def __init__(self, on_poll):
        self.now = 1000.0
        self.polls = 0
        self.on_poll = on_poll

    def mono(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds
        self.polls += 1
        self.on_poll(self.polls)

    def install(self, module, monkeypatch):
        monkeypatch.setattr(module, "_mono", self.mono)
        monkeypatch.setattr(module, "_sleep", self.sleep)


def _script():
    assert CLIENT.is_file(), "missing %s" % CLIENT
    return str(CLIENT)


def _env(tmp_path, **extra):
    env = {k: v for k, v in os.environ.items() if k not in ("TEST_TARGET", "PB_GPU_LEASE_DIR")}
    env["PB_GPU_LEASE_DIR"] = str(tmp_path)
    env.update(extra)
    return env


def _spawn(tmp_path, args, **env_extra):
    return subprocess.Popen(
        [sys.executable, _script()] + list(args),
        env=_env(tmp_path, **env_extra),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _run_cli(tmp_path, args, **env_extra):
    proc = _spawn(tmp_path, args, **env_extra)
    out, err = proc.communicate(timeout=BOUND_S)
    return proc.returncode, out, err


# --- goal-1: stdlib only, Python 3.8, directory ----------------------------------


def test_client_imports_only_stdlib_and_parses_py38():
    assert CLIENT.is_file(), "missing %s" % CLIENT
    tree = ast.parse(CLIENT.read_text(), filename=str(CLIENT), feature_version=(3, 8))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, "relative import"
            imported.add((node.module or "").split(".")[0])
        elif isinstance(node, ast.Attribute):
            assert node.attr not in ("removeprefix", "removesuffix"), "needs Python 3.9"
    assert imported, "the client imports nothing?"
    assert not [name for name in imported if "prismabuild" in name]
    assert imported <= STDLIB_ALLOWLIST, sorted(imported - STDLIB_ALLOWLIST)
    stdlib = getattr(sys, "stdlib_module_names", None)  # Python >= 3.10
    if stdlib is not None:
        assert imported <= set(stdlib), sorted(imported - set(stdlib))


def test_default_constants():
    module = _load()
    assert module.DEFAULT_DIR == "/run/prismabuild-gpu-lease"
    assert module.RENEW_S == 5
    assert module.POLL_S == 0.5


def test_lease_dir_env_override(monkeypatch, tmp_path):
    module = _client(monkeypatch)
    assert module.lease_dir() == "/run/prismabuild-gpu-lease"
    monkeypatch.setenv("PB_GPU_LEASE_DIR", str(tmp_path))
    assert module.lease_dir() == str(tmp_path)
    assert module.lease_dir("/elsewhere") == "/elsewhere"
    with module.lease() as grant:  # no state file: granted at once, from the env directory
        assert (tmp_path / ("%s.lock" % grant.lease_id)).is_file()


# --- goal-2: acquire --------------------------------------------------------------


def test_lock_held_before_json_written_and_json_fields(monkeypatch, tmp_path):
    module = _client(monkeypatch)
    real_write = module._write_atomic
    first_write = []

    def spy(path, doc):
        if not first_write:
            lock = Path(path[: -len(".json")] + ".lock")
            first_write.append(
                {
                    "lock_exists": lock.is_file(),
                    "lock_held": not _lock_is_free(tmp_path, doc["lease_id"]),
                    "json_exists": Path(path).exists(),
                    "granted_unix": doc["granted_unix"],
                }
            )
        real_write(path, doc)

    monkeypatch.setattr(module, "_write_atomic", spy)
    with module.lease("me", directory=str(tmp_path)) as grant:
        doc = _read_lease(tmp_path, grant.lease_id)
        assert not _lock_is_free(tmp_path, grant.lease_id)
    assert first_write == [
        {"lock_exists": True, "lock_held": True, "json_exists": False, "granted_unix": None}
    ]
    assert len(doc["lease_id"]) == 32 and doc["lease_id"] == grant.lease_id
    assert set(doc["lease_id"]) <= set("0123456789abcdef")
    assert doc["schema"] == "prismabuild.gpu_lease.v1"
    assert doc["host"] == socket.gethostname()
    assert doc["holder"] == "me"
    assert (doc["mem_gb"], doc["cpus"]) == (16, 4)
    assert doc["granted_unix"] is not None and doc["pb_state_at_grant"] == "absent"
    assert doc["requested_unix"] <= doc["granted_unix"]
    assert doc["released_unix"] is None and doc["release_reason"] is None
    assert _lock_is_free(tmp_path, grant.lease_id)
    assert (tmp_path / ("%s.lock" % grant.lease_id)).is_file()  # never deleted
    assert (tmp_path / ("%s.json" % grant.lease_id)).is_file()
    assert not list(tmp_path.glob("*.tmp"))


def test_mem_gb_and_cpus_in_lease_json(tmp_path):
    code, _out, err = _run_cli(tmp_path, ["run", "--mem-gb", "8", "--cpus", "2", "--", "true"])
    assert code == 0, err
    doc = _read_lease(tmp_path)
    assert (doc["mem_gb"], doc["cpus"]) == (8, 2)


def test_holder_defaults_to_test_target(tmp_path):
    code, _out, err = _run_cli(tmp_path, ["run", "--", "true"], TEST_TARGET="//pkg:gpu_test")
    assert code == 0, err
    assert _read_lease(tmp_path)["holder"] == "//pkg:gpu_test"


def test_holder_default_name_and_truncation(monkeypatch, tmp_path):
    module = _client(monkeypatch)
    with module.lease(directory=str(tmp_path)) as grant:
        assert _read_lease(tmp_path, grant.lease_id)["holder"] == "%s:%d" % (
            socket.gethostname(),
            os.getpid(),
        )
    with module.lease("x" * 300, directory=str(tmp_path)) as grant:
        assert _read_lease(tmp_path, grant.lease_id)["holder"] == "x" * 200


# --- goal-3/4: waiting, grants, messages ------------------------------------------


def test_waits_while_not_ready_then_grants_ready(monkeypatch, tmp_path):
    module = _client(monkeypatch)
    _write_state(tmp_path, tokens=1)
    seen = []

    def on_poll(n):
        doc = _read_lease(tmp_path)
        seen.append((doc["granted_unix"], doc["pb_state_at_grant"]))
        if n == 4:
            lease_id = _lease_id(tmp_path)
            _write_state(tmp_path, fencing=[lease_id], ready=[lease_id], tokens=1)

    _Clock(on_poll).install(module, monkeypatch)
    with module.lease(directory=str(tmp_path)) as grant:
        assert grant.pb_state == "ready"
        assert grant.waited_s == 2.0  # four 0.5 s polls
        doc = _read_lease(tmp_path, grant.lease_id)
    assert seen == [(None, None)] * 4
    assert doc["pb_state_at_grant"] == "ready" and doc["granted_unix"] is not None


def test_fencing_only_keeps_waiting(monkeypatch, tmp_path):
    module = _client(monkeypatch)
    _write_state(tmp_path, tokens=1)

    def on_poll(n):
        lease_id = _lease_id(tmp_path)
        assert _read_lease(tmp_path)["granted_unix"] is None
        if n < 3:
            _write_state(tmp_path, fencing=[lease_id], tokens=1)  # fenced, not ready
        else:
            _write_state(tmp_path, fencing=[lease_id], ready=[lease_id], tokens=1)

    _Clock(on_poll).install(module, monkeypatch)
    with module.lease(directory=str(tmp_path)) as grant:
        assert grant.pb_state == "ready"
        assert grant.waited_s == 1.5  # granted by the ready poll, not the fencing ones


def test_absent_state_grants_immediately(monkeypatch, tmp_path):
    module = _client(monkeypatch)

    def on_poll(n):
        raise AssertionError("an absent state must not wait")

    _Clock(on_poll).install(module, monkeypatch)
    with module.lease(directory=str(tmp_path)) as grant:
        assert (grant.pb_state, grant.waited_s) == ("absent", 0.0)
        assert _read_lease(tmp_path)["pb_state_at_grant"] == "absent"


def test_stale_state_grants_stale(monkeypatch, tmp_path):
    module = _client(monkeypatch)
    _write_state(tmp_path, updated=time.time() - 31, tokens=2)  # nobody ready, but too old

    def on_poll(n):
        raise AssertionError("a stale state must not wait")

    _Clock(on_poll).install(module, monkeypatch)
    with module.lease(directory=str(tmp_path)) as grant:
        assert (grant.pb_state, grant.waited_s) == ("stale", 0.0)
        assert _read_lease(tmp_path)["pb_state_at_grant"] == "stale"


def test_waiting_and_granted_messages(monkeypatch, tmp_path, capsys):
    module = _client(monkeypatch)
    _write_state(tmp_path, tokens=3)

    def on_poll(n):
        if n == 61:  # 30.5 virtual seconds
            lease_id = _lease_id(tmp_path)
            _write_state(tmp_path, fencing=[lease_id], ready=[lease_id], tokens=3)

    _Clock(on_poll).install(module, monkeypatch)
    with module.lease(directory=str(tmp_path)):
        pass
    err = capsys.readouterr().err
    waiting = "gpu-lease: waiting on %s: 3 PrismaBuild GPU token(s) held" % socket.gethostname()
    assert err.splitlines().count(waiting) == 1
    assert "gpu-lease: granted after 30.5s (pb_state=ready)" in err.splitlines()
    assert err.index(waiting) < err.index("granted after")


# --- goal-5: max wait -------------------------------------------------------------


def test_max_wait_exceeded_exits_75(tmp_path):
    _write_state(tmp_path, tokens=2)  # never ready
    code, _out, err = _run_cli(tmp_path, ["run", "--max-wait-s", "1", "--", "true"])
    assert code == 75, err
    assert "gpu-lease: wait exceeded 1s (pb_gpu_tokens_held=2)" in err.splitlines()
    doc = _read_lease(tmp_path)
    assert doc["release_reason"] == "wait_exceeded" and doc["released_unix"] is not None
    assert doc["granted_unix"] is None
    assert _lock_is_free(tmp_path)


def test_api_max_wait_raises_wait_exceeded(monkeypatch, tmp_path):
    module = _client(monkeypatch)
    _write_state(tmp_path, tokens=5)
    clock = _Clock(lambda n: None)
    clock.install(module, monkeypatch)
    with pytest.raises(module.WaitExceeded) as caught:
        with module.lease(directory=str(tmp_path), max_wait_s=2):
            raise AssertionError("must not be granted")
    assert caught.value.tokens_held == 5
    assert str(caught.value) == "wait exceeded 2s (pb_gpu_tokens_held=5)"
    assert clock.polls == 4  # 2.0 virtual seconds of 0.5 s polls
    doc = _read_lease(tmp_path)
    assert doc["release_reason"] == "wait_exceeded" and doc["released_unix"] is not None
    assert _lock_is_free(tmp_path)


# --- goal-6: renewal --------------------------------------------------------------


def _spy_writes(monkeypatch, module, *, fail_first_renewal=False, want=4):
    """Record every JSON write; ``done`` is set once the renew thread has written ``want``-th."""
    real_write = module._write_atomic
    writes = []
    done = threading.Event()
    failed = []
    lock = threading.Lock()

    def spy(path, doc):
        in_renewer = threading.current_thread() is not threading.main_thread()
        if fail_first_renewal and in_renewer and not failed:
            failed.append(True)
            raise OSError("disk full")
        real_write(path, doc)
        with lock:
            writes.append((in_renewer, doc["renewed_unix"]))
            if sum(1 for renewer, _ in writes if renewer) >= want:
                done.set()

    monkeypatch.setattr(module, "_write_atomic", spy)
    return writes, done


def test_renewal_advances_renewed_unix(monkeypatch, tmp_path):
    module = _client(monkeypatch)
    ticks = itertools.count(1000)
    monkeypatch.setattr(module, "_now", lambda: float(next(ticks)))
    monkeypatch.setattr(module, "RENEW_S", 0.01)
    writes, done = _spy_writes(monkeypatch, module, want=3)
    with module.lease(directory=str(tmp_path)) as grant:
        first = _read_lease(tmp_path, grant.lease_id)["requested_unix"]
        assert done.wait(BOUND_S), "the renew thread never wrote"
        assert _read_lease(tmp_path, grant.lease_id)["renewed_unix"] > first
    renewed = [value for renewer, value in writes if renewer]
    assert renewed == sorted(set(renewed))  # fresh and increasing on every renewal


def test_renewal_write_failure_is_reported_and_retried(monkeypatch, tmp_path, capsys):
    module = _client(monkeypatch)
    monkeypatch.setattr(module, "RENEW_S", 0.01)
    writes, done = _spy_writes(monkeypatch, module, fail_first_renewal=True, want=1)
    with module.lease(directory=str(tmp_path)):
        assert done.wait(BOUND_S), "the renew thread never retried"
    err = capsys.readouterr().err
    assert "failed (disk full); will retry" in err
    assert any(renewer for renewer, _ in writes)


# --- goal-7: release on every path ------------------------------------------------


def test_run_propagates_child_exit_code_and_releases(tmp_path):
    code, _out, err = _run_cli(tmp_path, ["run", "--", "sh", "-c", "exit 7"])
    assert code == 7, err
    assert "gpu-lease: granted after 0.0s (pb_state=absent)" in err.splitlines()
    doc = _read_lease(tmp_path)
    assert doc["release_reason"] == "released" and doc["released_unix"] is not None
    assert _lock_is_free(tmp_path)
    lease_id = doc["lease_id"]
    assert (tmp_path / ("%s.lock" % lease_id)).is_file()  # PrismaBuild removes the files


def test_signalled_child_exits_128_plus_signal(tmp_path):
    code, _out, err = _run_cli(tmp_path, ["run", "--", "sh", "-c", "kill -9 $$"])
    assert code == 128 + signal.SIGKILL, err
    assert _read_lease(tmp_path)["release_reason"] == "released"


def test_api_exception_releases_lease(monkeypatch, tmp_path):
    module = _client(monkeypatch)
    with pytest.raises(RuntimeError, match="boom"):
        with module.lease(directory=str(tmp_path)) as grant:
            assert not _lock_is_free(tmp_path, grant.lease_id)
            raise RuntimeError("boom")
    doc = _read_lease(tmp_path)
    assert doc["release_reason"] == "released" and doc["released_unix"] is not None
    assert _lock_is_free(tmp_path)


def test_sigterm_forwarded_and_lease_released(tmp_path):
    child = (
        "import os, signal\n"
        "def term(*_):\n"
        "    os.write(1, b'got-term\\n')  # no buffered I/O: 'up' may still be flushing\n"
        "    os._exit(9)\n"
        "signal.signal(signal.SIGTERM, term)\n"
        "os.write(1, b'up\\n')\n"
        "while True:\n"
        "    signal.pause()\n"
    )
    proc = _spawn(tmp_path, ["run", "--", sys.executable, "-c", child])
    try:
        assert proc.stdout.readline() == "up\n", "child never started"
        proc.send_signal(signal.SIGTERM)
        out, err = proc.communicate(timeout=BOUND_S)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate()
    assert "got-term" in out, err[-1500:]
    assert proc.returncode == 9, err
    doc = _read_lease(tmp_path)
    assert doc["release_reason"] == "released" and doc["released_unix"] is not None
    assert _lock_is_free(tmp_path)


def test_sigkill_client_frees_lock_and_kills_child(tmp_path):
    proc = subprocess.Popen(
        [sys.executable, _script(), "run", "--", "sh", "-c", "echo $$; exec sleep 600"],
        env=_env(tmp_path),
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    child_pid = None
    try:
        line = proc.stdout.readline()
        assert line.strip().isdigit(), "child never started: %r" % line
        child_pid = int(line)
        lease_id = _lease_id(tmp_path)
        assert not _lock_is_free(tmp_path, lease_id)  # held while the client lives
        proc.kill()
        proc.wait(timeout=BOUND_S)
        assert _lock_is_free(tmp_path, lease_id)  # the kernel dropped it with the process
        # The client was the child's only other holder of our stdout pipe, so EOF means the
        # child is gone (PR_SET_PDEATHSIG killed it).
        readable, _, _ = select.select([proc.stdout], [], [], BOUND_S)
        assert readable, "child still running after the client was SIGKILLed"
        assert proc.stdout.read() == ""
    finally:
        if child_pid is not None:
            try:
                os.kill(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if proc.poll() is None:
            proc.kill()
        proc.wait()
        proc.stdout.close()
    assert _read_lease(tmp_path, lease_id)["released_unix"] is None  # a crash records no release


# --- goal-9: missing directory ----------------------------------------------------


def test_missing_directory_exits_3(tmp_path):
    missing = tmp_path / "nope"
    marker = tmp_path / "ran"
    code, _out, err = _run_cli(missing, ["run", "--", "touch", str(marker)])
    assert code == 3, err
    assert (
        "gpu-lease: %s missing; host not provisioned for gate GPU leases" % missing
    ) in err.splitlines()
    assert not marker.exists()


def test_missing_directory_optional_runs_command(tmp_path):
    missing = tmp_path / "nope"
    code, out, err = _run_cli(missing, ["run", "--optional", "--", "sh", "-c", "echo ran; exit 5"])
    assert code == 5, err
    assert out == "ran\n"
    assert "gpu-lease: %s missing; running without a lease" % missing in err
    assert not missing.exists()


def test_api_optional_missing_dir_yields_none_pb_state(monkeypatch, tmp_path):
    module = _client(monkeypatch)
    missing = str(tmp_path / "nope")
    with module.lease(directory=missing, optional=True) as grant:
        assert grant.pb_state is None and grant.lease_id is None
    with pytest.raises(module.LeaseDirMissing):
        with module.lease(directory=missing):
            raise AssertionError("must not be granted")


# --- goal-12: documentation -------------------------------------------------------


def test_module_docstring_documents_cli_and_api():
    doc = _load().__doc__
    for needle in (
        "pb_gpu_lease.py run",
        "--holder",
        "--mem-gb",
        "--cpus",
        "--optional",
        "--max-wait-s",
        "PB_GPU_LEASE_DIR",
        "pb_gpu_lease.lease(",
        "WaitExceeded",
        "waited_s",
        "pb_state",
        "lease_id",
        "TEST_TARGET",
        "PR_SET_PDEATHSIG",
        "75",
    ):
        assert needle in doc, needle
    design = DESIGN.read_text()
    assert "Gate GPU leases" in design
    assert "tools/gpu_lease/pb_gpu_lease.py" in design

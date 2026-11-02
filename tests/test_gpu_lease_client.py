"""Contract tests for the vendorable, SDK-free gate GPU lease client."""

import ast
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import threading
import time

import pytest


ROOT = Path(__file__).resolve().parents[1]
CLIENT = ROOT / "tools/gpu_lease/pb_gpu_lease.py"
BOUND = 20  # Failure bound only, never a timing assertion.


@pytest.fixture
def lease_dir(tmp_path):
    # Repository autouse fixtures also use tmp_path; isolate protocol artifacts.
    directory = tmp_path / "gpu-leases"
    directory.mkdir()
    return directory


def load_client():
    # Call from the test body: an absent implementation must be an assertion
    # failure, not a collection error or a fixture/setup error.
    assert CLIENT.is_file(), "the single-file GPU lease client is missing"
    spec = importlib.util.spec_from_file_location("pb_gpu_lease", CLIENT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Clock:
    def __init__(self):
        self.elapsed = 0.0

    def monotonic(self):
        return self.elapsed

    def time(self):
        return 1000.0 + self.elapsed

    def sleep(self, seconds):
        self.elapsed += seconds

    def install(self, monkeypatch, client):
        monkeypatch.setattr(client, "_monotonic", self.monotonic)
        monkeypatch.setattr(client, "_time", self.time)
        monkeypatch.setattr(client, "_sleep", self.sleep)


def state(directory, *, ready=(), fencing=(), updated=None, tokens=3):
    value = {
        "schema": "prismabuild.gpu_lease_state.v1",
        "host": socket.gethostname(),
        "updated_unix": time.time() if updated is None else updated,
        "fencing": sorted(fencing),
        "ready": sorted(ready),
        "pb_gpu_tokens_held": tokens,
        "pb_mem_gb_held": 16,
        "pb_cpus_held": 4,
    }
    temporary = directory / "state.tmp"
    temporary.write_text(json.dumps(value))
    temporary.replace(directory / "pb-state.json")


def record(directory):
    paths = [p for p in directory.glob("*.json") if p.name != "pb-state.json"]
    assert len(paths) == 1
    return json.loads(paths[0].read_text())


def unlocked(path):
    with path.open("rb") as handle:
        fcntl.flock(handle, fcntl.LOCK_SH | fcntl.LOCK_NB)


def locked(path):
    with path.open("rb") as handle:
        with pytest.raises(BlockingIOError):
            fcntl.flock(handle, fcntl.LOCK_SH | fcntl.LOCK_NB)


def run_cli(directory, args, command, **kwargs):
    assert CLIENT.is_file(), "the single-file GPU lease client is missing"
    env = dict(os.environ, PB_GPU_LEASE_DIR=str(directory))
    env.update(kwargs.pop("env", {}))
    return subprocess.run(
        [sys.executable, str(CLIENT), "run"] + args + ["--"] + command,
        env=env, capture_output=True, text=True, timeout=BOUND, **kwargs
    )


def test_module_is_stdlib_only_parses_as_py38_and_documents_cli_and_api():
    assert CLIENT.is_file(), "the single-file GPU lease client is missing"
    tree = ast.parse(CLIENT.read_text(), feature_version=(3, 8))
    allowed = getattr(sys, "stdlib_module_names", {
        "argparse", "contextlib", "collections", "ctypes", "errno", "fcntl",
        "json", "math", "os", "secrets", "signal", "socket", "subprocess",
        "sys", "tempfile", "threading", "time", "typing", "pathlib",
    })
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert all(alias.name.split(".")[0] in allowed for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0
            assert node.module.split(".")[0] in allowed
    doc = ast.get_docstring(tree)
    assert "run" in doc and "--" in doc and "lease(" in doc
    assert "PB_GPU_LEASE_DIR" in doc and "/run/prismabuild-gpu-lease" in doc


def test_acquire_writes_lock_and_full_protocol_json(monkeypatch, lease_dir):
    client = load_client()
    writes = []
    real_write = client._atomic_write

    def observe(path, value):
        locked(Path(path).with_suffix(".lock"))
        writes.append(dict(value))
        real_write(path, value)

    monkeypatch.setattr(client, "_atomic_write", observe)
    with client.lease("x" * 250, directory=lease_dir) as grant:
        assert re.fullmatch(r"[0-9a-f]{32}", grant.lease_id)
        first = writes[0]
        assert set(first) == {
            "schema", "lease_id", "host", "holder", "mem_gb", "cpus",
            "requested_unix", "granted_unix", "renewed_unix", "pb_state_at_grant",
            "released_unix", "release_reason",
        }
        assert first["schema"] == "prismabuild.gpu_lease.v1"
        assert first["lease_id"] == grant.lease_id
        assert first["host"] == socket.gethostname()
        assert first["holder"] == "x" * 200
        assert (first["mem_gb"], first["cpus"]) == (16, 4)
        assert first["requested_unix"] == first["renewed_unix"]
        assert first["requested_unix"] > 0
        for key in ("granted_unix", "pb_state_at_grant", "released_unix", "release_reason"):
            assert first[key] is None
        assert record(lease_dir)["granted_unix"] is not None


def test_mem_gb_and_cpus_appear_in_lease_json(lease_dir):
    result = run_cli(lease_dir, ["--mem-gb", "8", "--cpus", "2"], ["sh", "-c", "exit 0"])
    assert result.returncode == 0, result.stderr
    value = record(lease_dir)
    assert (value["mem_gb"], value["cpus"]) == (8, 2)


def test_waits_while_not_ready_including_fencing_only_then_grants_ready(monkeypatch, lease_dir, capsys):
    client = load_client()
    clock = Clock()
    clock.install(monkeypatch, client)
    state(lease_dir, updated=clock.time())
    requests = threading.Event()
    written = threading.Event()
    errors = []
    steps = []

    def writer():
        try:
            for step in (1, 2, 3):
                assert requests.wait(BOUND)
                requests.clear()
                value = record(lease_dir)
                assert value["granted_unix"] is None
                lease_id = value["lease_id"]
                state(lease_dir, updated=clock.time(), fencing=[lease_id],
                      ready=[lease_id] if step == 3 else [])
                steps.append(step)
                written.set()
        except BaseException as exc:
            errors.append(exc)
            written.set()

    worker = threading.Thread(target=writer, daemon=True)
    worker.start()

    def poll(seconds):
        assert seconds == 0.5
        clock.sleep(seconds)
        requests.set()
        assert written.wait(BOUND)
        written.clear()
        if errors:
            raise errors[0]

    monkeypatch.setattr(client, "_sleep", poll)
    with client.lease(directory=lease_dir) as grant:
        assert grant.pb_state == "ready"
        assert grant.waited_s == 1.5
        assert steps == [1, 2, 3]
        value = record(lease_dir)
        assert value["pb_state_at_grant"] == "ready"
        assert value["granted_unix"] == 1001.5
    worker.join(BOUND)
    assert not worker.is_alive()
    assert "gpu-lease: granted after 1.5s (pb_state=ready)" in capsys.readouterr().err


def test_absent_state_grants_at_once_as_absent(monkeypatch, lease_dir, capsys):
    client = load_client()
    clock = Clock()
    clock.install(monkeypatch, client)
    with client.lease(directory=lease_dir) as grant:
        assert grant.pb_state == "absent"
        assert grant.waited_s == 0
        assert record(lease_dir)["pb_state_at_grant"] == "absent"
    assert "(pb_state=absent)" in capsys.readouterr().err


def test_state_older_than_30s_grants_as_stale(monkeypatch, lease_dir):
    client = load_client()
    clock = Clock()
    clock.install(monkeypatch, client)
    state(lease_dir, updated=969)
    with client.lease(directory=lease_dir) as grant:
        assert grant.pb_state == "stale"
        assert grant.waited_s == 0
        assert record(lease_dir)["pb_state_at_grant"] == "stale"


def test_waiting_message_is_printed_on_the_30s_cadence(monkeypatch, lease_dir, capsys):
    client = load_client()
    clock = Clock()
    clock.install(monkeypatch, client)
    state(lease_dir, updated=clock.time(), tokens=7)
    messages = []

    def poll(seconds):
        messages.append((clock.elapsed, capsys.readouterr().err))
        clock.sleep(seconds)
        value = record(lease_dir)
        state(lease_dir, updated=clock.time(), tokens=7,
              ready=[value["lease_id"]] if clock.elapsed >= 61 else [])

    monkeypatch.setattr(client, "_sleep", poll)
    with client.lease(directory=lease_dir) as grant:
        assert grant.waited_s == 61
    waiting = [(when, message) for when, message in messages if "waiting on" in message]
    assert [when for when, _ in waiting] == [30, 60]
    expected = "gpu-lease: waiting on {}: 7 PrismaBuild GPU token(s) held".format(socket.gethostname())
    assert all(expected in message for _, message in waiting)


def test_max_wait_exceeded_exits_75_and_records_wait_exceeded(lease_dir):
    state(lease_dir, tokens=9)
    result = run_cli(lease_dir, ["--max-wait-s", "1"], ["sh", "-c", "exit 19"])
    assert result.returncode == 75, result.stderr
    assert "gpu-lease: wait exceeded 1s (pb_gpu_tokens_held=9)" in result.stderr
    value = record(lease_dir)
    assert value["granted_unix"] is None
    assert value["release_reason"] == "wait_exceeded"
    assert value["released_unix"] is not None
    unlocked(lease_dir / (value["lease_id"] + ".lock"))


def test_zero_max_wait_allows_initial_immediate_grant(monkeypatch, lease_dir):
    client = load_client()
    # Reading state takes some time even when no waiting is necessary.
    moments = iter([0.0, 0.01])
    monkeypatch.setattr(client, "_monotonic", lambda: next(moments))
    with client.lease(directory=lease_dir, max_wait_s=0) as grant:
        assert grant.pb_state == "absent"
        assert grant.waited_s == 0.01


def test_api_raises_wait_exceeded(monkeypatch, lease_dir):
    client = load_client()
    clock = Clock()
    clock.install(monkeypatch, client)
    state(lease_dir, updated=clock.time())
    with pytest.raises(client.WaitExceeded):
        with client.lease(directory=lease_dir, max_wait_s=1):
            pytest.fail("never-ready state must not grant")
    assert clock.elapsed == 1
    assert record(lease_dir)["release_reason"] == "wait_exceeded"


class RenewalSteps:
    """Inject exactly N renewal ticks, then wait for the real stop event."""

    def __init__(self, count):
        self.remaining = count
        self.ticks = threading.Semaphore(0)

    def wait(self, stop, interval):
        assert interval == 5
        if not self.remaining:
            return stop.wait()
        self.remaining -= 1
        assert self.ticks.acquire(timeout=BOUND)
        return stop.is_set()

    def tick(self):
        self.ticks.release()


def test_renewed_unix_advances_while_held(monkeypatch, lease_dir):
    client = load_client()
    clock = Clock()
    clock.install(monkeypatch, client)
    steps = RenewalSteps(1)
    done = threading.Event()
    original = client._atomic_write
    renewal_threads = []

    def write(path, value):
        original(path, value)
        if value["renewed_unix"] == 1005:
            renewal_threads.append(threading.current_thread())
            done.set()

    monkeypatch.setattr(client, "_renew_wait", steps.wait)
    monkeypatch.setattr(client, "_atomic_write", write)
    with client.lease(directory=lease_dir):
        assert record(lease_dir)["renewed_unix"] == 1000
        clock.sleep(5)
        steps.tick()
        assert done.wait(BOUND)
        assert record(lease_dir)["renewed_unix"] == 1005
        assert renewal_threads[0].daemon
        assert renewal_threads[0] is not threading.current_thread()


def test_renewal_write_failure_is_reported_and_retried(monkeypatch, lease_dir, capsys):
    client = load_client()
    clock = Clock()
    clock.install(monkeypatch, client)
    steps = RenewalSteps(2)
    failed = threading.Event()
    retried = threading.Event()
    original = client._atomic_write
    attempts = []

    def write(path, value):
        if threading.current_thread() is not threading.main_thread():
            attempts.append(value["renewed_unix"])
            if len(attempts) == 1:
                failed.set()
                raise OSError("injected write failure")
            original(path, value)
            retried.set()
        else:
            original(path, value)

    monkeypatch.setattr(client, "_renew_wait", steps.wait)
    monkeypatch.setattr(client, "_atomic_write", write)
    with client.lease(directory=lease_dir):
        clock.sleep(5)
        steps.tick()
        assert failed.wait(BOUND)
        clock.sleep(5)
        steps.tick()
        assert retried.wait(BOUND)
        assert record(lease_dir)["renewed_unix"] == 1010
    assert "injected write failure" in capsys.readouterr().err
    assert attempts[:2] == [1005, 1010]


def test_renewal_also_runs_while_waiting(monkeypatch, lease_dir):
    client = load_client()
    clock = Clock()
    clock.install(monkeypatch, client)
    renewed = threading.Event()
    steps = RenewalSteps(1)
    original = client._atomic_write
    state(lease_dir, updated=1000)

    def write(path, value):
        original(path, value)
        if value["renewed_unix"] == 1005 and value["granted_unix"] is None:
            renewed.set()

    def poll(seconds):
        clock.sleep(5)
        steps.tick()
        assert renewed.wait(BOUND)
        value = record(lease_dir)
        assert value["granted_unix"] is None
        state(lease_dir, updated=1005, ready=[value["lease_id"]])

    monkeypatch.setattr(client, "_renew_wait", steps.wait)
    monkeypatch.setattr(client, "_atomic_write", write)
    monkeypatch.setattr(client, "_sleep", poll)
    with client.lease(directory=lease_dir) as grant:
        assert grant.pb_state == "ready"
        assert record(lease_dir)["renewed_unix"] == 1005


def test_release_on_normal_exit_keeps_both_files(lease_dir):
    client = load_client()
    with client.lease(directory=lease_dir) as grant:
        lock = lease_dir / (grant.lease_id + ".lock")
        locked(lock)
    value = record(lease_dir)
    assert value["released_unix"] >= value["granted_unix"]
    assert value["release_reason"] == "released"
    assert lock.exists()
    unlocked(lock)
    assert sorted(p.suffix for p in lease_dir.iterdir()) == [".json", ".lock"]


def test_api_releases_when_body_raises(lease_dir):
    client = load_client()
    with pytest.raises(RuntimeError, match="body error"):
        with client.lease(directory=lease_dir) as grant:
            raise RuntimeError("body error")
    assert record(lease_dir)["release_reason"] == "released"
    unlocked(lease_dir / (grant.lease_id + ".lock"))


def test_wait_exceeded_raised_by_body_is_a_normal_release(lease_dir):
    client = load_client()
    with pytest.raises(client.WaitExceeded):
        with client.lease(directory=lease_dir):
            raise client.WaitExceeded("body failure, not acquisition timeout")
    assert record(lease_dir)["release_reason"] == "released"


def test_run_propagates_child_exit_status_and_releases(lease_dir):
    result = run_cli(lease_dir, [], ["sh", "-c", "exit 7"])
    assert result.returncode == 7, result.stderr
    value = record(lease_dir)
    assert value["released_unix"] is not None
    assert value["release_reason"] == "released"
    unlocked(lease_dir / (value["lease_id"] + ".lock"))


def test_run_signalled_child_exits_128_plus_signal(lease_dir):
    result = run_cli(lease_dir, [], ["sh", "-c", "kill -TERM $$"])
    assert result.returncode == 143, result.stderr
    assert record(lease_dir)["release_reason"] == "released"


def test_holder_defaults_to_test_target_then_hostname_pid(monkeypatch, lease_dir):
    client = load_client()
    monkeypatch.setenv("TEST_TARGET", "//gate:gpu_test")
    monkeypatch.setenv("PB_GPU_LEASE_DIR", str(lease_dir))
    with client.lease():
        assert record(lease_dir)["holder"] == "//gate:gpu_test"
    for p in lease_dir.iterdir():
        p.unlink()
    monkeypatch.delenv("TEST_TARGET")
    with client.lease(directory=lease_dir):
        assert record(lease_dir)["holder"] == "{}:{}".format(socket.gethostname(), os.getpid())


def start_child(directory, command):
    assert CLIENT.is_file(), "the single-file GPU lease client is missing"
    # The child's explicit readiness message avoids racing handler installation.
    process = subprocess.Popen(
        [sys.executable, str(CLIENT), "run", "--"] + command,
        env=dict(os.environ, PB_GPU_LEASE_DIR=str(directory)),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    line = []
    ready = threading.Event()

    def read():
        line.append(process.stdout.readline())
        ready.set()

    threading.Thread(target=read, daemon=True).start()
    try:
        assert ready.wait(BOUND), "child did not report readiness"
        assert line[0].startswith("ready "), line
        return process, int(line[0].split()[1])
    except BaseException:
        process.kill()
        process.communicate(timeout=BOUND)
        raise


def wait_dead(pid):
    # /proc may retain an orphan zombie until the executor's init reaps it.
    # A zombie cannot use the GPU; accepting it is not accepting a live child.
    deadline = time.monotonic() + BOUND
    wake = threading.Event()
    while time.monotonic() < deadline:
        try:
            status = Path("/proc/{}/stat".format(pid)).read_text()
        except FileNotFoundError:
            return
        if status.rsplit(")", 1)[1].split()[0] == "Z":
            return
        wake.wait(0.01)  # Poll an external process, not a scheduling delay.
    pytest.fail("child remains alive after parent death")


def test_sigkill_of_run_frees_lock_and_pdeathsig_kills_child(lease_dir):
    # exec preserves the PID reported after the parent has installed PDEATHSIG.
    process, pid = start_child(lease_dir, ["sh", "-c", "echo ready $$; exec sleep 600"])
    try:
        value = record(lease_dir)
        lock = lease_dir / (value["lease_id"] + ".lock")
        locked(lock)
        process.kill()
        assert process.wait(timeout=BOUND) == -signal.SIGKILL
        unlocked(lock)  # No retries: probe immediately once the parent has exited.
        wait_dead(pid)
        assert record(lease_dir)["released_unix"] is None
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=BOUND)
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT, signal.SIGHUP])
def test_sigterm_reaches_child_and_lease_is_released(lease_dir, sig):
    command = [sys.executable, "-u", "-c", (
        "import os,signal,threading\n"
        "def stop(sig, frame):\n"
        " print('forwarded', sig, flush=True)\n"
        " raise SystemExit(23)\n"
        "for sig in (signal.SIGTERM,signal.SIGINT,signal.SIGHUP): signal.signal(sig,stop)\n"
        "print('ready',os.getpid(),flush=True)\n"
        "threading.Event().wait()\n"
    )]
    process, pid = start_child(lease_dir, command)
    try:
        process.send_signal(sig)
        out, err = process.communicate(timeout=BOUND)
        assert process.returncode == 23, err
        assert "forwarded {}".format(sig) in out
        value = record(lease_dir)
        assert value["release_reason"] == "released"
        assert value["released_unix"] is not None
        unlocked(lease_dir / (value["lease_id"] + ".lock"))
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=BOUND)
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def test_missing_directory_exits_3_with_message(lease_dir):
    directory = lease_dir / "missing"
    result = run_cli(directory, [], ["sh", "-c", "exit 19"])
    assert result.returncode == 3
    assert "gpu-lease: {} missing; host not provisioned for gate GPU leases".format(directory) in result.stderr
    assert not directory.exists()


def test_optional_runs_command_without_lease_when_directory_missing(lease_dir):
    result = run_cli(lease_dir / "missing", ["--optional"], ["sh", "-c", "echo optional-child; exit 7"])
    assert result.returncode == 7, result.stderr
    assert "optional-child" in result.stdout
    assert "without a lease" in result.stderr
    assert not (lease_dir / "missing").exists()


def test_api_optional_yields_pb_state_none(lease_dir):
    client = load_client()
    with client.lease(directory=lease_dir / "missing", optional=True) as grant:
        assert grant.pb_state is None
        assert grant.lease_id is None
        assert grant.waited_s == 0


def test_api_yields_lease_id_waited_s_and_pb_state(lease_dir):
    client = load_client()
    with client.lease(holder="api-test", mem_gb=8, cpus=2, directory=lease_dir) as grant:
        assert re.fullmatch(r"[0-9a-f]{32}", grant.lease_id)
        assert grant.waited_s >= 0
        assert grant.pb_state == "absent"
        value = record(lease_dir)
        assert value["holder"] == "api-test"
        assert (value["mem_gb"], value["cpus"]) == (8, 2)


def test_design_doc_has_gate_gpu_leases_section_with_client_paragraph():
    text = (ROOT / "docs/design.md").read_text()
    match = re.search(r"^## Gate GPU leases\n(.*?)(?=^## |\Z)", text, re.M | re.S)
    assert match, "design doc needs a Gate GPU leases section"
    section = match.group(1)
    assert "Client" in section and "stdlib" in section
    assert "sha256" in section and "vendor" in section
    assert "pb_gpu_lease.py run" in section and "lease(" in section


def test_spawn_failure_releases_lease(lease_dir):
    result = run_cli(lease_dir, [], [str(lease_dir / "no-command")])
    assert result.returncode != 0
    value = record(lease_dir)
    assert value["release_reason"] == "released"
    unlocked(lease_dir / (value["lease_id"] + ".lock"))


@pytest.mark.parametrize("name", ["mem_gb", "cpus", "max_wait_s"])
def test_negative_resources_or_wait_rejected(lease_dir, name):
    client = load_client()
    with pytest.raises(ValueError):
        with client.lease(directory=lease_dir, **{name: -1}):
            pytest.fail("invalid lease should not grant")
    assert not list(lease_dir.iterdir())


def test_atomic_publication_and_readable_host_files(monkeypatch, lease_dir):
    client = load_client()
    replaced = []
    real_replace = os.replace

    def replace(source, destination):
        source, destination = Path(source), Path(destination)
        assert source.parent == destination.parent == lease_dir
        snapshot = json.loads(source.read_text())
        locked(destination.with_suffix(".lock"))
        # PB may have a different uid than a root action container.
        assert source.stat().st_mode & 0o444 == 0o444
        assert destination.with_suffix(".lock").stat().st_mode & 0o444 == 0o444
        replaced.append(snapshot)
        real_replace(source, destination)

    monkeypatch.setattr(client.os, "replace", replace)
    with client.lease(directory=lease_dir):
        assert len(replaced) == 2
        assert replaced[0]["granted_unix"] is None
        assert replaced[1]["granted_unix"] is not None
    assert replaced[-1]["release_reason"] == "released"


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT, signal.SIGHUP])
def test_api_signal_releases_and_restores_handlers(lease_dir, sig):
    client = load_client()
    previous = signal.getsignal(sig)
    with pytest.raises(SystemExit) as result:
        with client.lease(directory=lease_dir) as grant:
            signal.raise_signal(sig)
    assert result.value.code == 128 + sig
    assert signal.getsignal(sig) is previous
    value = record(lease_dir)
    assert value["release_reason"] == "released"
    assert value["released_unix"] is not None
    unlocked(lease_dir / (grant.lease_id + ".lock"))


def test_signal_during_spawn_is_forwarded_after_pid_is_known(monkeypatch):
    client = load_client()
    child = client._Child()
    forwarded = []

    class Process:
        def __init__(self, command, **kwargs):
            # Force the otherwise tiny interval inside Popen, after fork and
            # before the caller has the new child's process object.
            child.signal(signal.SIGTERM, None)

        def send_signal(self, sig):
            forwarded.append(sig)

        def wait(self):
            assert forwarded == [signal.SIGTERM]
            return -signal.SIGTERM

        def poll(self):
            return -signal.SIGTERM

    monkeypatch.setattr(client.subprocess, "Popen", Process)
    assert child.run(["unused"]) == 143


def test_release_write_failure_still_closes_lock(monkeypatch, lease_dir, capsys):
    client = load_client()
    original = client._atomic_write

    def fail_release(path, value):
        if value["released_unix"] is not None:
            raise OSError("injected release failure")
        original(path, value)

    monkeypatch.setattr(client, "_atomic_write", fail_release)
    with client.lease(directory=lease_dir) as grant:
        locked(lease_dir / (grant.lease_id + ".lock"))
    unlocked(lease_dir / (grant.lease_id + ".lock"))
    assert "injected release failure" in capsys.readouterr().err


def test_malformed_state_is_not_a_grant(monkeypatch, lease_dir):
    client = load_client()
    clock = Clock()
    clock.install(monkeypatch, client)
    (lease_dir / "pb-state.json").write_text("{")
    with pytest.raises(client.WaitExceeded):
        with client.lease(directory=lease_dir, max_wait_s=1):
            pytest.fail("malformed is neither absent, stale nor ready")
    assert record(lease_dir)["release_reason"] == "wait_exceeded"

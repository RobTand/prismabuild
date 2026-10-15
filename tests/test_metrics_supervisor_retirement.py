"""The real exporter waits for its supervisor to retire and record it."""
from __future__ import annotations

import http.client
import json
import os
from pathlib import Path
import select
import shutil
import socket
import stat
import subprocess
import sys
import threading

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools/fleet"))
import supervise


def _read_pipe(fd, process, log):
    readable, _, _ = select.select([fd], [], [], 10)
    assert readable, (process.poll(), log.read_text())
    data = os.read(fd, 100)
    assert data, (process.poll(), log.read_text())
    return data


@pytest.mark.parametrize("loaded_commit", ["old-commit", ""])
def test_supervisor_retires_real_metrics_and_owns_its_drift_record(
        tmp_path, monkeypatch, loaded_commit):
    fleet = tmp_path / "fleet"
    old = fleet / "runtime-generations/old"
    current = fleet / "runtime-generations/current"
    shutil.copytree(ROOT / "src", old / "src", ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copytree(ROOT / "tools/fleet", old / "tools/fleet",
                    ignore=shutil.ignore_patterns("__pycache__"))
    (current / "tools/fleet").mkdir(parents=True)
    (current / "tools/fleet/pbmetrics.py").touch()
    for root, commit in ((old, loaded_commit), (current, "new-commit")):
        (root / "RUNTIME_VERSION.json").write_text(json.dumps({
            "commit": commit, "generation": root.name}))
    (fleet / "repo").symlink_to(current, target_is_directory=True)
    gate = tmp_path / "maintenance.json"
    gate.write_text('{"draining":false}')
    owner = old / "src/prismabuild/role_session.py"
    source = owner.read_text()
    declaration = 'RUNTIME_VERSION = Path("/mnt/shared/prismabuild-fleet/repo/RUNTIME_VERSION.json")'
    assert source.count(declaration) == 1
    source = source.replace(declaration, f"RUNTIME_VERSION = Path({str(fleet / 'repo/RUNTIME_VERSION.json')!r})")
    source = source.replace('ROLE_LOCK_ROOT = Path(f"/tmp/prismabuild-roles-{os.getuid()}")',
                            f"ROLE_LOCK_ROOT = Path({str(tmp_path / 'role-locks')!r})")
    owner.write_text(source)
    ready_r, ready_w = os.pipe()
    entered_r, entered_w = os.pipe()
    release_r, release_w = os.pipe()
    script = old / "tools/fleet/pbmetrics.py"
    source = script.read_text()
    entrypoint = 'if __name__ == "__main__":'
    assert source.count(entrypoint) == 1
    setup = f'''
_server_type = ThreadingHTTPServer
class ReadyServer(_server_type):
    def __init__(self, address, *args, **kwargs):
        super().__init__((address[0], 0), *args, **kwargs)
        os.write({ready_w}, str(self.server_address[1]).encode())
        os.close({ready_w})
ThreadingHTTPServer = ReadyServer
def blocked_collection(*args, **kwargs):
    os.write({entered_w}, b"1")
    assert os.read({release_r}, 1) == b"1"
    return "controlled_metric 1\\n"
collect_metrics = blocked_collection
'''
    script.write_text(source.replace(entrypoint, setup + "\n" + entrypoint))
    argv = ["--queue-root", str(fleet / "pb-queue"), "--port", "9469"]
    log = tmp_path / "metrics.log"
    errors = []
    request = None
    with log.open("w") as output:
        process = subprocess.Popen([sys.executable, str(script), *argv],
            stdout=output, stderr=subprocess.STDOUT, start_new_session=True,
            pass_fds=(ready_w, entered_w, release_r),
            env={**os.environ, supervise.OWNERSHIP_ENV: socket.gethostname(),
                 "PRISMABUILD_MAINTENANCE_GATE": str(gate)})
    for fd in (ready_w, entered_w, release_r):
        os.close(fd)
    try:
        port = int(_read_pipe(ready_r, process, log))

        def scrape():
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
            try:
                connection.request("GET", "/metrics")
                response = connection.getresponse()
                assert response.status == 200
                assert response.read() == b"controlled_metric 1\n"
            except Exception as error:
                errors.append(error)
            finally:
                connection.close()

        request = threading.Thread(target=scrape, daemon=True)
        request.start()
        assert _read_pipe(entered_r, process, log) == b"1", "exporter retired itself"
        monkeypatch.setattr(supervise, "MIRROR", fleet)
        monkeypatch.setattr(supervise, "PROC", Path("/proc"))
        monkeypatch.setattr(supervise, "_live_role_loops", lambda *_: [process.pid])
        monkeypatch.setattr(supervise, "_spawn_role", lambda *a: pytest.fail("replacement before handler joined"))
        assert supervise.ensure_roles(socket.gethostname(), declared=[("metrics", argv)],
            census={"metrics": [process.pid]}, holders=set()) == []
        assert process.poll() is None, "supervisor stop did not wait for the real handler"
        records = list((fleet / "pb-queue/generation-drift").glob("*.json"))
        assert len(records) == 1
        record = json.loads(records[0].read_text())
        assert record["actor"] == "supervise"
        assert record["loaded_commit"] == loaded_commit
        assert record["loaded_generation"] == "old"
        assert record["published_commit"] == "new-commit"
        assert record["published_generation"] == "current"
        assert record["detail"]["role"] == "metrics"
        assert record["detail"]["pid"] == process.pid
        assert record["detail"]["reason"] == "generation"
        assert stat.S_IMODE(records[0].stat().st_mode) == 0o444
        os.write(release_w, b"1")
        assert process.wait(timeout=5) == 0, log.read_text()
        request.join(5)
        assert not request.is_alive()
        assert errors == []
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        for fd in (ready_r, entered_r, release_w):
            os.close(fd)
        if request is not None:
            request.join(5)

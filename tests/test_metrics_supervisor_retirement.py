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
    for root, commit in ((old, loaded_commit), (current, "new-commit")):
        shutil.copytree(ROOT / "src", root / "src", ignore=shutil.ignore_patterns("__pycache__"))
        shutil.copytree(ROOT / "tools/fleet", root / "tools",
                        ignore=shutil.ignore_patterns("__pycache__"))
        receipt = {"commit": commit, "generation": root.name} if commit else {}
        (root / "RUNTIME_VERSION.json").write_text(json.dumps(receipt))
        owner = root / "src/prismabuild/role_session.py"
        source = owner.read_text()
        replacements = {
            'RUNTIME_VERSION = Path("/mnt/shared/prismabuild-fleet/repo/RUNTIME_VERSION.json")':
                f"RUNTIME_VERSION = Path({str(fleet / 'repo/RUNTIME_VERSION.json')!r})",
            'ROLE_LOCK_ROOT = Path(f"/tmp/prismabuild-roles-{os.getuid()}")':
                f"ROLE_LOCK_ROOT = Path({str(tmp_path / 'role-locks')!r})",
        }
        for declaration, replacement in replacements.items():
            assert source.count(declaration) == 1
            source = source.replace(declaration, replacement)
        owner.write_text(source)
    (fleet / "repo").symlink_to(current, target_is_directory=True)
    gate = tmp_path / "maintenance.json"
    gate.write_text('{"draining":false}')
    notice = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    notice.bind(("127.0.0.1", 0))
    notice.settimeout(10)
    entered_r, entered_w = os.pipe()
    release_r, release_w = os.pipe()
    script = old / "tools/pbmetrics.py"
    entrypoint = 'if __name__ == "__main__":'
    setup = f'''
import socket
_server_type = ThreadingHTTPServer
class ReadyServer(_server_type):
    def __init__(self, address, *args, **kwargs):
        super().__init__((address[0], 0), *args, **kwargs)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as notice:
            notice.sendto(json.dumps({{"pid": os.getpid(), "port": self.server_address[1]}}).encode(), {notice.getsockname()!r})
ThreadingHTTPServer = ReadyServer
'''
    blocked = f'''
def blocked_collection(*args, **kwargs):
    os.write({entered_w}, b"1")
    assert os.read({release_r}, 1) == b"1"
    return "controlled_metric 1\\n"
collect_metrics = blocked_collection
'''
    for root in (old, current):
        path = root / "tools/pbmetrics.py"
        source = path.read_text()
        assert source.count(entrypoint) == 1
        collection = blocked if root == old else 'collect_metrics = lambda *a, **k: "controlled_metric 1\\n"\n'
        path.write_text(source.replace(entrypoint, setup + collection + "\n" + entrypoint))
    argv = ["--queue-root", str(fleet / "pb-queue"), "--port", "9469"]
    log = tmp_path / "metrics.log"
    errors = []
    request = None
    with log.open("w") as output:
        process = subprocess.Popen([sys.executable, str(script), *argv],
            stdout=output, stderr=subprocess.STDOUT, start_new_session=True,
            pass_fds=(entered_w, release_r),
            env={**os.environ, supervise.OWNERSHIP_ENV: socket.gethostname(),
                 "PRISMABUILD_MAINTENANCE_GATE": str(gate)})
    children = [process]
    for fd in (entered_w, release_r):
        os.close(fd)
    try:
        ready = json.loads(notice.recv(1024))
        assert ready["pid"] == process.pid
        port = ready["port"]

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
        monkeypatch.setattr(supervise, "LOG_DIR", tmp_path / "logs")
        monkeypatch.setattr(supervise.runtime_gate, "ROLE_LOCK_ROOT", tmp_path / "role-locks")
        monkeypatch.setattr(supervise, "_ROLE_REFUSALS", {})
        monkeypatch.setattr(supervise, "_ROLE_CHILDREN", {})
        monkeypatch.setenv("PRISMABUILD_MAINTENANCE_GATE", str(gate))
        monkeypatch.setattr(supervise, "_live_role_loops", lambda *_:
            [child.pid for child in children if child.poll() is None])
        popen = subprocess.Popen

        def owned_popen(*args, **kwargs):
            child = popen(*args, **kwargs)
            children.append(child)
            return child

        monkeypatch.setattr(supervise.subprocess, "Popen", owned_popen)
        assert supervise.ensure_roles(socket.gethostname(), declared=[("metrics", argv)],
            census={"metrics": [process.pid]}, holders=set()) == []
        assert process.poll() is None, "supervisor stop did not wait for the real handler"
        assert len(children) == 1, "replacement started before the predecessor joined"
        records = list((fleet / "pb-queue/generation-drift").glob("*.json"))
        assert len(records) == 1
        record = json.loads(records[0].read_text())
        assert record["actor"] == "supervise"
        assert record["loaded_commit"] == loaded_commit
        assert record["loaded_generation"] == ("old" if loaded_commit else "")
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
        started = supervise.ensure_roles(socket.gethostname(), declared=[("metrics", argv)],
            census={"metrics": []}, holders=set())
        assert len(children) == 2
        replacement = children[-1]
        assert started == [("metrics", replacement.pid)]
        assert replacement.args[1] == str(current / "tools/pbmetrics.py")
        ready = json.loads(notice.recv(1024))
        assert ready["pid"] == replacement.pid
        port = ready["port"]
        scrape()
        assert errors == [], (supervise.LOG_DIR / supervise._role_log_name("metrics")).read_text()
        replacement.terminate()
        assert replacement.wait(timeout=5) == 0
        assert len(list((fleet / "pb-queue/generation-drift").glob("*.json"))) == 1
        assert list((tmp_path / "rollout/parked").glob("*")) == []
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
        for fd in (entered_r, release_w):
            os.close(fd)
        notice.close()
        if request is not None:
            request.join(5)

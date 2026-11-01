"""Real pytest sessions distinguish a failed proc probe from test failures."""
from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import subprocess
import sys

import pytest

from record_compat_namespace import PROC_NAMESPACE_REFUSAL, PROC_NAMESPACE_REFUSAL_EXIT


ROOT = Path(__file__).resolve().parents[1]
PROC_ERROR = "bwrap: Can't mount proc on /newroot/proc: Operation not permitted"


@pytest.fixture
def compat_namespace_session(tmp_path):
    binary = tmp_path / "bwrap"
    binary.write_text(f'''#!{sys.executable}
import json, os, sys
from pathlib import Path
if sys.argv[1:] == ["--version"]:
    print("bubblewrap test")
    raise SystemExit(0)
with Path(os.environ["PROBE_CALLS"]).open("a") as output:
    output.write(json.dumps(sys.argv[1:]) + "\\n")
print(os.environ.get("PROBE_STDERR", ""), file=sys.stderr)
raise SystemExit(int(os.environ.get("PROBE_EXIT", "0")))
''')
    binary.chmod(0o755)
    (tmp_path / "conftest.py").write_text('''from record_compat_namespace import RecordCompatNamespacePreflight
def pytest_configure(config):
    config.pluginmanager.register(RecordCompatNamespacePreflight(), "record-compat-namespace-preflight")
''')

    def run(*, code=0, stderr="", name="test_record_compatibility.py", failure=False, collect=False,
            repository=False):
        test = ROOT / "tests/test_current_receipt_compat_composition.py" if repository else tmp_path / name
        if not repository:
            test.write_text('''from pathlib import Path
def test_first():
    Path("first-ran").touch()
def test_second():
    Path("second-ran").touch()
''' + (f'    assert False, {PROC_ERROR!r}\n' if failure else ""))
        environment = {**os.environ, "PYTHONPATH": os.pathsep.join((str(ROOT / "tests"), str(ROOT / "src"))),
                       "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
                       "PROBE_EXIT": str(code), "PROBE_STDERR": stderr,
                       "PROBE_CALLS": str(tmp_path / "probe-calls.jsonl"),
                       "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"}
        command = [sys.executable, "-m", "pytest", "-q", "--confcutdir",
                   str(ROOT if repository else tmp_path), str(test)]
        if collect:
            command.append("--collect-only")
        return subprocess.run(command, cwd=ROOT if repository else tmp_path, env=environment,
                              capture_output=True, text=True, timeout=45)

    return tmp_path, run


@pytest.mark.parametrize("name", ["test_record_compatibility.py", "test_funding_wire_compatibility.py",
                                 "test_current_receipt_compat_composition.py"])
def test_proc_mount_probe_refuses_before_any_compatibility_test(compat_namespace_session, name):
    directory, run = compat_namespace_session
    child = run(code=1, stderr=PROC_ERROR, name=name)
    output = child.stdout + child.stderr
    assert child.returncode == PROC_NAMESPACE_REFUSAL_EXIT, output
    assert child.returncode not in (0, 1, 2), output
    assert PROC_NAMESPACE_REFUSAL in output
    assert output.count(PROC_NAMESPACE_REFUSAL) == 1, output
    assert socket.gethostname() in output and PROC_ERROR in output
    for field in ("/proc/self/mountinfo", "NoNewPrivs", "Seccomp", "CapEff",
                  "max_user_namespaces", "sibling_scopes", "probe_argv", "bubblewrap test"):
        assert field in output, output
    assert not (directory / "first-ran").exists()
    assert not (directory / "second-ran").exists()
    assert len((directory / "probe-calls.jsonl").read_text().splitlines()) == 1


def test_compat_proc_probe_runs_once_per_session_and_keeps_its_pid_namespace(compat_namespace_session):
    directory, run = compat_namespace_session
    for session in range(2):
        child = run()
        assert child.returncode == 0 and "2 passed" in child.stdout, child.stdout + child.stderr
        calls = [json.loads(line) for line in (directory / "probe-calls.jsonl").read_text().splitlines()]
        assert calls == [["--unshare-user", "--uid", "1000", "--gid", "1000", "--unshare-pid",
                          "--ro-bind", "/", "/", "--dev", "/dev", "--proc", "/proc", "true"]] * (session + 1)


def test_repository_conftest_refuses_the_real_receipt_population_before_execution(compat_namespace_session):
    directory, run = compat_namespace_session
    child = run(code=1, stderr=PROC_ERROR, repository=True)
    output = child.stdout + child.stderr
    assert child.returncode == PROC_NAMESPACE_REFUSAL_EXIT, output
    assert output.count(PROC_NAMESPACE_REFUSAL) == 1, output
    assert "AssertionError" not in output
    assert len((directory / "probe-calls.jsonl").read_text().splitlines()) == 1


@pytest.mark.parametrize("code,stderr", [(1, "bwrap: Creating new namespace failed: Operation not permitted"),
                                       (0, PROC_ERROR)])
def test_only_a_failed_proc_mount_probe_changes_the_session_verdict(compat_namespace_session, code, stderr):
    _, run = compat_namespace_session
    child = run(code=code, stderr=stderr)
    assert child.returncode == 0 and "2 passed" in child.stdout, child.stdout + child.stderr
    assert PROC_NAMESPACE_REFUSAL not in child.stdout + child.stderr


def test_a_test_failure_with_proc_mount_text_is_still_an_ordinary_failure(compat_namespace_session):
    _, run = compat_namespace_session
    child = run(failure=True)
    assert child.returncode == 1 and "1 failed, 1 passed" in child.stdout, child.stdout + child.stderr
    assert PROC_NAMESPACE_REFUSAL not in child.stdout + child.stderr


@pytest.mark.parametrize("options", [{"name": "test_ordinary.py"}, {"collect": True}])
def test_unrelated_and_collection_only_sessions_do_not_probe_namespaces(compat_namespace_session, options):
    directory, run = compat_namespace_session
    child = run(code=1, stderr=PROC_ERROR, **options)
    assert child.returncode == 0, child.stdout + child.stderr
    assert not (directory / "probe-calls.jsonl").exists()

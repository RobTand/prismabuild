"""A fleet shard must not run against a different reviewed dependency commit."""
from __future__ import annotations

import importlib.util
import base64
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from pbtest_shard_output import ONE_PASS  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pbtest_pins_subject", ROOT / "tools/fleet/pbtest.py")
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)
PIN = "a" * 40


def fixture_checkout(tmp_path, *, installed="b" * 40):
    checkout = tmp_path / "project"
    (checkout / "tools").mkdir(parents=True)
    (checkout / "tools/resolve_fleetfixture_dev_pin.py").write_text(
        f"print({PIN!r})\n")
    (checkout / "tests").mkdir()
    (checkout / "tests/test_one.py").write_text(
        "from pathlib import Path\n"
        "def test_one():\n"
        "    Path('pytest-ran').write_text('yes')\n")
    dist = checkout / "src/fleetfixture-1.0.dist-info"
    dist.mkdir(parents=True)
    (dist / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: fleetfixture\nVersion: 1.0\n")
    (dist / "top_level.txt").write_text("fleetfixture\n")
    package = checkout / "src/fleetfixture"
    package.mkdir()
    (package / "__init__.py").write_text("VALUE = 1\n")
    (dist / "direct_url.json").write_text(json.dumps({
        "url": "https://example.invalid/fleetfixture.git",
        "vcs_info": {"vcs": "git", "commit_id": installed},
    }))
    record_install(checkout, dist)
    return checkout, dist


def record_install(checkout, dist):
    rows = []
    for path in sorted((checkout / "src").rglob("*")):
        if path.is_file() and path.name != "RECORD":
            data = path.read_bytes()
            digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
            rows.append(f"{path.relative_to(checkout / 'src')},sha256={digest},{len(data)}")
    (dist / "RECORD").write_text("\n".join(rows) + "\n")


def dispatch(checkout, monkeypatch):
    # Only the pbrun submission is replaced. Its actual command runs inside
    # this already-admitted test action, against a private fixture checkout.
    original = subprocess.Popen
    calls = []

    def admitted_payload(command, **kwargs):
        calls.append(command)
        return original(command[command.index("--") + 1:], cwd=checkout, **kwargs)

    monkeypatch.setattr(pbtest.subprocess, "Popen", admitted_payload)
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(checkout), "--python", sys.executable,
        "--shards", "1", "--threads-per-shard", "1",
        "--json", str(checkout / "result.json"), "tests",
    ])
    return pbtest.main(), calls


def test_mismatched_commit_refuses_before_pytest(tmp_path, monkeypatch, capsys):
    checkout, _ = fixture_checkout(tmp_path)
    result, _ = dispatch(checkout, monkeypatch)
    output = capsys.readouterr().out
    assert result == 1, output
    assert not (checkout / "pytest-ran").exists()
    assert PIN in output and "b" * 40 in output


def test_matching_pin_runs_pytest_and_records_provenance(tmp_path, monkeypatch):
    checkout, _ = fixture_checkout(tmp_path, installed=PIN)
    result, calls = dispatch(checkout, monkeypatch)
    assert result == 0
    assert (checkout / "pytest-ran").read_text() == "yes"
    # The worker command carries the guard itself, not a helper path that
    # could be absent on another host or change independently of the key.
    payload = calls[0][calls[0].index("--") + 1:]
    assert "-c" in payload
    assert "def check_pins" in payload[payload.index("-c") + 1]
    output = json.loads((checkout / "result.json").read_text())[0]["output"]
    evidence = json.loads(next(line.removeprefix("pbtest dependency pin: ")
                              for line in output.splitlines()
                              if line.startswith("pbtest dependency pin: ")))
    assert evidence["expected_commit"] == evidence["installed_commit"] == PIN
    assert evidence["verified_files"] >= 4


@pytest.mark.parametrize("change, diagnostic", [
    ("missing_distribution", "found []"),
    ("local_install", "installed commit=<unknown>"),
    ("editable", "non-editable"),
    ("missing_record", "RECORD is missing"),
    ("changed_bytes", "differ from RECORD"),
    ("unrecorded_file", "unrecorded package file"),
    ("shadow", "not owned by its RECORD"),
    ("bad_pin", "one full lowercase Git commit"),
    ("resolver_failure", "resolver exited 7"),
    ("malformed_metadata", "dependency pin refused"),
])
def test_unverifiable_install_never_reaches_pytest(tmp_path, monkeypatch, capsys,
                                                change, diagnostic):
    checkout, dist = fixture_checkout(tmp_path, installed=PIN)
    resolver = checkout / "tools/resolve_fleetfixture_dev_pin.py"
    direct = dist / "direct_url.json"
    if change == "missing_distribution":
        for path in dist.iterdir():
            path.unlink()
        dist.rmdir()
    elif change == "local_install":
        direct.write_text(json.dumps({"url": "file:///old-checkout", "dir_info": {}}))
    elif change == "editable":
        value = json.loads(direct.read_text())
        value["dir_info"] = {"editable": True}
        direct.write_text(json.dumps(value))
    elif change == "missing_record":
        (dist / "RECORD").unlink()
    elif change == "changed_bytes":
        (checkout / "src/fleetfixture/__init__.py").write_text("VALUE = 2\n")
    elif change == "unrecorded_file":
        (checkout / "src/fleetfixture/extra.py").write_text("VALUE = 2\n")
    elif change == "shadow":
        (checkout / "fleetfixture.py").write_text("VALUE = 2\n")
    elif change == "bad_pin":
        resolver.write_text("print('main')\n")
    elif change == "resolver_failure":
        resolver.write_text("raise SystemExit(7)\n")
    elif change == "malformed_metadata":
        direct.write_text("[]")
    result, _ = dispatch(checkout, monkeypatch)
    assert result == 1
    assert not (checkout / "pytest-ran").exists()
    assert diagnostic in capsys.readouterr().out


def test_pin_resolution_waits_for_worker_execution(tmp_path, monkeypatch):
    checkout, _ = fixture_checkout(tmp_path, installed=PIN)
    resolver = checkout / "tools/resolve_fleetfixture_dev_pin.py"
    resolver.write_text("raise RuntimeError('must not run on the coordinator')\n")
    calls = []

    class Queued:
        returncode = 0
        def communicate(self):
            return ONE_PASS, None

    monkeypatch.setattr(pbtest.subprocess, "Popen", lambda cmd, **kw: calls.append(cmd) or Queued())
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
        "--shards", "1", "tests",
    ])
    assert pbtest.main() == 0
    assert len(calls) == 1


def test_a_second_pin_cannot_hide_behind_a_matching_one(tmp_path, monkeypatch, capsys):
    checkout, _ = fixture_checkout(tmp_path, installed=PIN)
    (checkout / "tools/resolve_missingfixture_dev_pin.py").write_text(f"print({PIN!r})\n")
    result, _ = dispatch(checkout, monkeypatch)
    assert result == 1
    assert not (checkout / "pytest-ran").exists()
    assert "missingfixture" in capsys.readouterr().out

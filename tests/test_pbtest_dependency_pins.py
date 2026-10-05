"""A fleet shard must not run against a different reviewed dependency commit."""
from __future__ import annotations

import importlib.util
import base64
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
from pbtest_shard_output import admitted_child

import pytest

from pbtest_shard_output import ShardProcess, ONE_PASS  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pbtest_pins_subject", ROOT / "tools/fleet/pbtest.py")
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)
PIN = "a" * 40


def fixture_checkout(tmp_path, *, installed="b" * 40):
    checkout = tmp_path / "project"
    (checkout / "tools").mkdir(parents=True)
    (checkout / "pytest.ini").write_text("[pytest]\n")  # hermetic rootdir (#1257)
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
        argv, environment = admitted_child(command)
        return original(argv, cwd=checkout, env=environment, **kwargs)

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

    class Queued(ShardProcess):
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


# --- Installed-byte integrity vs. pin identity (#1544) -------------------
#
# These tests exercise the split between the two things verify_install used
# to fuse: pin/provenance identity (which D32 may stamp) and installed-byte
# integrity (which is never skipped). They run against REAL installs: pip
# installs the package from git+file://<repo>@<sha> into a venv, so
# direct_url.json carries pip's own vcs_info and RECORD carries pip's own
# digests. No digest loop is mocked; corruption is applied to installed
# bytes and read back by importlib.metadata in this process.

PINS_SOURCE = (ROOT / "tools/fleet/pbtest_pins.py").read_text(encoding="utf-8")
OTHER = "c" * 40


@pytest.fixture(scope="module")
def pins_module():
    spec = importlib.util.spec_from_file_location(
        "pbtest_pins_unit_1544", ROOT / "tools/fleet/pbtest_pins.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def pins_repo(tmp_path_factory):
    repo = tmp_path_factory.mktemp("pins1544") / "repo"
    (repo / "pinsbyte1544").mkdir(parents=True)
    (repo / "pyproject.toml").write_text(
        "[build-system]\n"
        'requires = ["setuptools>=61"]\n'
        'build-backend = "setuptools.build_meta"\n'
        "\n"
        "[project]\n"
        'name = "pinsbyte1544"\n'
        'version = "1.0"\n')
    (repo / "pinsbyte1544/__init__.py").write_text("VALUE = 1\n")
    git = ["git", "-c", "user.email=1544@t", "-c", "user.name=1544"]
    for command in (["git", "init", "-q"], [*git, "add", "-A"],
                    [*git, "commit", "-qm", "pins1544"]):
        subprocess.run(command, cwd=repo, check=True, capture_output=True)
    commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, check=True,
                            capture_output=True, text=True).stdout.strip()
    return repo, commit


def pip_installed(tmp_path, pins_repo):
    """A real non-editable pip install from git+file:// at a real commit.

    The box has no ``python3-venv``/ensurepip for 3.14, so the venv is
    created ``--without-pip`` and the ambient pip re-execs into it via
    ``pip --python``; ``--no-build-isolation`` keeps the whole path offline
    (setuptools comes from the system site packages the venv shares).
    """
    missing = [name for name in ("pip", "setuptools")
               if importlib.util.find_spec(name) is None]
    if missing:
        pytest.skip("offline real-install path needs pip and setuptools in the "
                    f"test interpreter environment; missing {missing}")
    repo, commit = pins_repo
    venv = tmp_path / "venv"
    made = subprocess.run([sys.executable, "-m", "venv", "--without-pip",
                           "--system-site-packages", str(venv)],
                          capture_output=True, text=True)
    assert made.returncode == 0, made.stderr[-2000:]
    installed = subprocess.run(
        [sys.executable, "-m", "pip", "--python", str(venv / "bin" / "python"),
         "install", "--no-build-isolation", "--no-deps",
         "--disable-pip-version-check", "-q",
         f"git+file://{repo}@{commit}#egg=pinsbyte1544"],
        capture_output=True, text=True)
    assert installed.returncode == 0, installed.stderr[-2000:]
    site = next((venv / "lib").glob("python*/site-packages"))
    direct = json.loads(
        (site / "pinsbyte1544-1.0.dist-info/direct_url.json").read_text())
    assert direct["vcs_info"]["vcs"] == "git"
    assert direct["vcs_info"]["commit_id"] == commit
    return site, commit


def test_default_identity_refusal_is_unchanged(tmp_path, monkeypatch,
                                               pins_module, pins_repo):
    site, commit = pip_installed(tmp_path, pins_repo)
    monkeypatch.syspath_prepend(str(site))
    with pytest.raises(ValueError) as excinfo:
        pins_module.verify_install("pinsbyte1544", OTHER)
    assert str(excinfo.value) == (
        f"distribution=pinsbyte1544 installed commit={commit}; "
        "require a non-editable Git install at the reviewed commit "
        "(local-directory installs do not record a Git commit)")


def test_tolerant_policy_accepts_a_real_install_at_another_commit(
        tmp_path, monkeypatch, pins_module, pins_repo):
    site, commit = pip_installed(tmp_path, pins_repo)
    monkeypatch.syspath_prepend(str(site))
    seen = {}

    def tolerate(message, facts):
        seen["message"], seen["facts"] = message, dict(facts)

    evidence = pins_module.verify_install("pinsbyte1544", OTHER,
                                          identity_policy=tolerate)
    assert evidence["installed_commit"] == commit
    assert evidence["expected_commit"] == OTHER
    assert evidence["verified_files"] >= 4
    assert evidence["identity_drift_tolerated"] == seen["message"]
    assert "non-editable Git install" in seen["message"]
    assert seen["facts"]["expected_commit"] == OTHER
    assert seen["facts"]["installed_commit"] == commit


def test_tolerant_policy_still_refuses_corrupt_installed_bytes(
        tmp_path, monkeypatch, pins_module, pins_repo):
    site, commit = pip_installed(tmp_path, pins_repo)
    init = site / "pinsbyte1544/__init__.py"
    init.write_text(init.read_text() + "# corrupted\n")
    monkeypatch.syspath_prepend(str(site))
    calls = []
    # ``OTHER`` is not the installed commit, so the identity check FAILS and the
    # policy is genuinely invoked; the refusal below therefore comes from the
    # byte phase running AFTER a tolerated drift, not from the strict path.
    with pytest.raises(ValueError) as excinfo:
        pins_module.verify_install(
            "pinsbyte1544", OTHER,
            identity_policy=lambda message, facts: calls.append(message))
    assert len(calls) == 1 and "require a non-editable Git install" in calls[0]
    assert "installed bytes differ from RECORD" in str(excinfo.value)
    assert "require a non-editable Git install" not in str(excinfo.value)


def test_tolerant_policy_missing_installed_file_still_fails(
        tmp_path, monkeypatch, pins_module, pins_repo):
    site, commit = pip_installed(tmp_path, pins_repo)
    (site / "pinsbyte1544/__init__.py").unlink()
    monkeypatch.syspath_prepend(str(site))
    calls = []
    with pytest.raises(OSError):
        pins_module.verify_install(
            "pinsbyte1544", OTHER,
            identity_policy=lambda message, facts: calls.append(message))
    assert len(calls) == 1


@pytest.mark.parametrize("change, diagnostic", [
    ("missing_record", "installed RECORD is missing"),
    ("unsupported_hash", "unsupported RECORD hash"),
    ("unrecorded_file", "unrecorded package file"),
    ("shadow", "not owned by its RECORD"),
])
def test_tolerant_policy_never_weakens_integrity(tmp_path, monkeypatch,
                                                 pins_module, pins_repo,
                                                 change, diagnostic):
    site, commit = pip_installed(tmp_path, pins_repo)
    if change == "missing_record":
        (site / "pinsbyte1544-1.0.dist-info/RECORD").unlink()
    elif change == "unsupported_hash":
        record = site / "pinsbyte1544-1.0.dist-info/RECORD"
        record.write_text(record.read_text().replace("sha256=", "md5=", 1))
    elif change == "unrecorded_file":
        (site / "pinsbyte1544/extra.py").write_text("VALUE = 2\n")
    elif change == "shadow":
        shadow = tmp_path / "shadow"
        shadow.mkdir()
        (shadow / "pinsbyte1544.py").write_text("VALUE = 2\n")
        monkeypatch.syspath_prepend(str(site))
        monkeypatch.syspath_prepend(str(shadow))
    if change != "shadow":
        monkeypatch.syspath_prepend(str(site))
    calls = []
    # ``OTHER`` makes the identity check fail so the policy is really called;
    # every integrity refusal must still follow the tolerated drift.
    with pytest.raises(ValueError) as excinfo:
        pins_module.verify_install(
            "pinsbyte1544", OTHER,
            identity_policy=lambda message, facts: calls.append(message))
    assert len(calls) == 1
    assert diagnostic in str(excinfo.value)
    assert "require a non-editable Git install" not in str(excinfo.value)


def test_standalone_byte_phase_ignores_identity_and_refuses_corruption(
        tmp_path, monkeypatch, pins_module, pins_repo):
    site, commit = pip_installed(tmp_path, pins_repo)
    monkeypatch.syspath_prepend(str(site))
    # The byte entry point takes no pin at all, so it cannot read one: it
    # passes on intact bytes of an install whose recorded commit differs
    # from any expected pin (identity drift verify_install would raise on).
    assert commit != OTHER
    evidence = pins_module.verify_record_bytes("pinsbyte1544")
    assert evidence["installed_commit"] == commit
    assert evidence["verified_files"] >= 4
    assert "expected_commit" not in evidence
    init = site / "pinsbyte1544/__init__.py"
    init.write_text(init.read_text() + "# corrupted\n")
    with pytest.raises(ValueError) as excinfo:
        pins_module.verify_record_bytes("pinsbyte1544")
    assert "installed bytes differ from RECORD" in str(excinfo.value)


def test_a_raising_policy_propagates_unchanged(tmp_path, monkeypatch,
                                               pins_module, pins_repo):
    site, commit = pip_installed(tmp_path, pins_repo)
    monkeypatch.syspath_prepend(str(site))

    def refuses(message, facts):
        raise RuntimeError("policy refuses this drift")

    with pytest.raises(RuntimeError, match="policy refuses this drift"):
        pins_module.verify_install("pinsbyte1544", OTHER, identity_policy=refuses)


def test_verify_install_routes_through_the_one_byte_implementation(
        tmp_path, monkeypatch, pins_module, pins_repo):
    site, commit = pip_installed(tmp_path, pins_repo)
    monkeypatch.syspath_prepend(str(site))
    calls = []
    original = pins_module.verify_record_bytes

    def spy(module):
        calls.append(module)
        return original(module)

    monkeypatch.setattr(pins_module, "verify_record_bytes", spy)
    evidence = pins_module.verify_install("pinsbyte1544", commit)
    assert calls == ["pinsbyte1544"]
    assert evidence["verified_files"] >= 4


def test_source_has_exactly_one_digest_construction():
    assert re.findall(r"hashlib\.\w+\(", PINS_SOURCE) == ["hashlib.new("]

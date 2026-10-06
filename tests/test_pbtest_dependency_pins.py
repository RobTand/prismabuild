"""A fleet shard must not run against a different reviewed dependency commit."""
from __future__ import annotations

import importlib.util
import base64
import csv
import hashlib
import json
from pathlib import Path
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
    result, _ = dispatch(checkout, monkeypatch)
    assert result == 0
    assert (checkout / "pytest-ran").read_text() == "yes"
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
        'version = "1.0"\n'
        "\n"
        "[project.scripts]\n"
        'pins1544 = "pinsbyte1544:main"\n')
    (repo / "pinsbyte1544/__init__.py").write_text(
        "VALUE = 1\n\n\ndef main():\n    return None\n")
    (repo / "pinsbyte1544/data.py").write_text("DATA = 1\n")
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


def test_tolerant_policy_still_refuses_a_deleted_non_imported_file(
        tmp_path, monkeypatch, pins_module, pins_repo):
    site, commit = pip_installed(tmp_path, pins_repo)
    (site / "pinsbyte1544/data.py").unlink()
    monkeypatch.syspath_prepend(str(site))
    calls = []
    # ``OTHER`` makes the identity phase genuinely drift, so the policy really
    # runs; the missing recorded file must still refuse after tolerated drift.
    with pytest.raises(ValueError) as excinfo:
        pins_module.verify_install(
            "pinsbyte1544", OTHER,
            identity_policy=lambda message, facts: calls.append(message))
    assert len(calls) == 1
    assert "require a non-editable Git install" in calls[0]
    assert "recorded file is missing" in str(excinfo.value)
    assert "require a non-editable Git install" not in str(excinfo.value)


def console_record_path(site):
    """The one hashed RECORD row outside the package: pip's console script.

    Pip records it relative to site-packages (``../../../bin/<name>``), the
    same relocated shape as the measured #1548 consumer blind spot, so the
    deletion below refuses only if raw RECORD enumeration survives the walk
    out of the package directory.
    """
    record = (site / "pinsbyte1544-1.0.dist-info/RECORD").read_text()
    rows = [line for line in record.splitlines() if line.startswith("../")]
    assert len(rows) == 1, rows
    entry = rows[0].split(",")
    assert entry[0].endswith("bin/pins1544")
    assert entry[1].startswith("sha256=")
    return entry[0]


def test_a_deleted_console_script_outside_the_package_refuses(
        tmp_path, monkeypatch, pins_module, pins_repo):
    site, commit = pip_installed(tmp_path, pins_repo)
    (site / console_record_path(site)).unlink()
    monkeypatch.syspath_prepend(str(site))
    with pytest.raises(ValueError, match="recorded file is missing"):
        pins_module.verify_install("pinsbyte1544", commit)


def test_tolerant_policy_still_refuses_a_deleted_console_script(
        tmp_path, monkeypatch, pins_module, pins_repo):
    site, commit = pip_installed(tmp_path, pins_repo)
    (site / console_record_path(site)).unlink()
    monkeypatch.syspath_prepend(str(site))
    calls = []
    with pytest.raises(ValueError) as excinfo:
        pins_module.verify_install(
            "pinsbyte1544", OTHER,
            identity_policy=lambda message, facts: calls.append(message))
    assert len(calls) == 1
    assert "recorded file is missing" in str(excinfo.value)


def test_restored_recorded_bytes_are_accepted_again(
        tmp_path, monkeypatch, pins_module, pins_repo):
    site, commit = pip_installed(tmp_path, pins_repo)
    data = site / "pinsbyte1544/data.py"
    original = data.read_bytes()
    data.unlink()
    monkeypatch.syspath_prepend(str(site))
    with pytest.raises(ValueError, match="recorded file is missing"):
        pins_module.verify_record_bytes("pinsbyte1544")
    data.write_bytes(original)
    evidence = pins_module.verify_record_bytes("pinsbyte1544")
    assert evidence["installed_commit"] == commit


def append_record_row(site, name, data):
    """Append one correctly hashed RECORD row, quoted the way pip quotes.

    The csv writer is pip's own grammar: a filename carrying a newline, a
    comma or doubled quotes travels as one field naming exactly those bytes.
    """
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
    record = site / "pinsbyte1544-1.0.dist-info/RECORD"
    with record.open("a", newline="") as handle:
        csv.writer(handle, lineterminator="\r\n").writerow(
            [name, f"sha256={digest}", len(data)])


def invalidate_record_row(site, defect):
    """Change only the grammar of pip's real, correctly hashed data row."""
    record = site / "pinsbyte1544-1.0.dist-info/RECORD"
    with record.open(newline="") as handle:
        rows = list(csv.reader(handle))
    row = next(row for row in rows if row[0] == "pinsbyte1544/data.py")
    if defect == "empty_name":
        row = row.copy()
        row[0] = ""
        rows.append(row)
    elif defect == "four_columns":
        row.append("extra")
    elif defect == "nonnumeric_size":
        row[2] = "notanumber"
    else:
        raise AssertionError(defect)
    with record.open("w", newline="") as handle:
        csv.writer(handle).writerows(rows)


@pytest.mark.parametrize("defect", ["empty_name", "four_columns", "nonnumeric_size"])
def test_a_malformed_record_row_refuses(tmp_path, monkeypatch, pins_module,
                                        pins_repo, defect):
    """Valid installed bytes cannot hide invalid RECORD grammar."""
    site, commit = pip_installed(tmp_path, pins_repo)
    invalidate_record_row(site, defect)
    monkeypatch.syspath_prepend(str(site))
    with pytest.raises(ValueError):
        pins_module.verify_install("pinsbyte1544", commit)


@pytest.mark.parametrize("defect", ["empty_name", "four_columns", "nonnumeric_size"])
def test_a_malformed_record_row_refuses_after_tolerated_drift(
        tmp_path, monkeypatch, pins_module, pins_repo, defect):
    site, commit = pip_installed(tmp_path, pins_repo)
    invalidate_record_row(site, defect)
    monkeypatch.syspath_prepend(str(site))
    calls = []
    with pytest.raises(ValueError):
        pins_module.verify_install(
            "pinsbyte1544", OTHER,
            identity_policy=lambda message, facts: calls.append(message))
    assert len(calls) == 1


@pytest.mark.parametrize("newline", [pytest.param("\r", id="carriage_return"),
                                    pytest.param("\r\n", id="carriage_return_line_feed")])
@pytest.mark.parametrize("tolerant", [False, True], ids=["strict", "tolerant"])
def test_intact_quoted_carriage_return_name_accepts(
        tmp_path, monkeypatch, pins_module, pins_repo, newline, tolerant):
    site, commit = pip_installed(tmp_path, pins_repo)
    monkeypatch.syspath_prepend(str(site))
    before = pins_module.verify_install("pinsbyte1544", commit)
    payload = b"quoted filename bytes\n"
    named = site / "pinsbyte1544" / f"carriage{newline}return.bin"
    named.write_bytes(payload)
    append_record_row(site, str(named.relative_to(site)), payload)
    calls = []
    evidence = pins_module.verify_install(
        "pinsbyte1544", OTHER if tolerant else commit,
        identity_policy=(lambda message, facts: calls.append(message)) if tolerant else None)
    assert evidence["verified_files"] == before["verified_files"] + 1
    assert evidence["installed_commit"] == commit
    assert len(calls) == int(tolerant)
    if tolerant:
        assert evidence["identity_drift_tolerated"] == calls[0]


@pytest.mark.parametrize("newline", [pytest.param("\r", id="carriage_return"),
                                    pytest.param("\r\n", id="carriage_return_line_feed")])
@pytest.mark.parametrize("tolerant", [False, True], ids=["strict", "tolerant"])
def test_missing_quoted_carriage_return_name_refuses_with_recorded_line_feed_alias(
        tmp_path, monkeypatch, pins_module, pins_repo, newline, tolerant):
    site, commit = pip_installed(tmp_path, pins_repo)
    payload = b"same bytes in two distinct files\n"
    named = site / "pinsbyte1544" / f"carriage{newline}return.bin"
    alias = site / "pinsbyte1544/carriage\nreturn.bin"
    for path in (named, alias):
        path.write_bytes(payload)
        append_record_row(site, str(path.relative_to(site)), payload)
    named.unlink()
    monkeypatch.syspath_prepend(str(site))
    calls = []
    with pytest.raises(ValueError):
        pins_module.verify_install(
            "pinsbyte1544", OTHER if tolerant else commit,
            identity_policy=(lambda message, facts: calls.append(message)) if tolerant else None)
    assert len(calls) == int(tolerant)


def test_quoted_record_names_stay_the_bytes_they_name(
        tmp_path, monkeypatch, pins_module, pins_repo):
    """A quoted newline filename is one entry naming one file.

    Pre-fix failure at bae247818a: splitlines() reassembled the recorded
    line\\nbreak.bin as the alias linebreak.bin, so the intact install was
    refused on the unrecorded real file and deleting the real file went
    unnoticed while the same-bytes alias was hashed in its place.
    """
    site, commit = pip_installed(tmp_path, pins_repo)
    package = site / "pinsbyte1544"
    payload = b"same bytes\n"
    named = package / "line\nbreak.bin"
    alias = package / "linebreak.bin"
    quoted = [named, alias, package / "comma,name.bin", package / 'quote""name.bin']
    for path in quoted:
        path.write_bytes(payload)
    for path in quoted:
        append_record_row(site, str(path.relative_to(site)), payload)
    monkeypatch.syspath_prepend(str(site))
    evidence = pins_module.verify_record_bytes("pinsbyte1544")
    assert evidence["installed_commit"] == commit
    named.unlink()
    with pytest.raises(ValueError, match="recorded file is missing"):
        pins_module.verify_record_bytes("pinsbyte1544")


def test_quoted_newline_name_refuses_after_tolerated_drift(
        tmp_path, monkeypatch, pins_module, pins_repo):
    site, commit = pip_installed(tmp_path, pins_repo)
    package = site / "pinsbyte1544"
    payload = b"same bytes\n"
    named = package / "line\nbreak.bin"
    alias = package / "linebreak.bin"
    for path in (named, alias):
        path.write_bytes(payload)
    append_record_row(site, str(named.relative_to(site)), payload)
    append_record_row(site, str(alias.relative_to(site)), payload)
    named.unlink()
    monkeypatch.syspath_prepend(str(site))
    calls = []
    with pytest.raises(ValueError) as excinfo:
        pins_module.verify_install(
            "pinsbyte1544", OTHER,
            identity_policy=lambda message, facts: calls.append(message))
    assert len(calls) == 1
    assert "recorded file is missing" in str(excinfo.value)


def test_a_deleted_non_imported_package_file_should_be_refused(
        tmp_path, monkeypatch, pins_module, pins_repo):
    """A hashed RECORD entry deleted from disk refuses, though nothing imports it.

    Pre-fix failure (the strict xfail this marker replaces): importlib.metadata's
    Distribution.files silently drops RECORD entries whose files are missing,
    so the deleted data.py was never hashed and the install reported intact.
    """
    site, commit = pip_installed(tmp_path, pins_repo)
    (site / "pinsbyte1544/data.py").unlink()
    monkeypatch.syspath_prepend(str(site))
    with pytest.raises(ValueError, match="recorded file is missing"):
        pins_module.verify_install("pinsbyte1544", commit)


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

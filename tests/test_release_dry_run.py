"""Release candidates must match their built, integrity-checked wheel."""
from __future__ import annotations

import base64
import csv
import hashlib
import importlib.metadata
import importlib.util
import io
from pathlib import Path
import subprocess
import sys
import zipfile

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("release_dry_run", ROOT / "tools/ci/release_dry_run.py")
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


def test_candidate_matches_metadata():
    assert release.candidate_version("v0.1.0", {"project": {"version": "0.1.0"}}) == "0.1.0"


@pytest.mark.parametrize("candidate", ["release", "v0.1.1", "v0.1.0-rc1", "--help", "v0.1.0\n"])
def test_invalid_candidate_is_rejected(candidate):
    with pytest.raises(ValueError, match="Candidate"):
        release.candidate_version(candidate, {"project": {"version": "0.1.0"}})


VALID_ENTRY_POINTS = b"[console_scripts]\nprismabuild = prismabuild.core:main\n"


def wheel_fixture(tmp_path, *, version="0.1.0", corrupt=False, missing=False,
                  entry_points=VALID_ENTRY_POINTS):
    prefix = f"prismabuild-{version}.dist-info/"
    files = {
        prefix + "METADATA": f"Metadata-Version: 2.1\nName: prismabuild\nVersion: {version}\n".encode(),
        prefix + "entry_points.txt": entry_points,
        "prismabuild/__init__.py": b"",
        "prismabuild/core.py": b"def main():\n    return 0\n",
    }
    record = io.StringIO()
    writer = csv.writer(record)
    for name, content in files.items():
        digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).rstrip(b"=").decode()
        writer.writerow([name, "sha256=" + digest, len(content)])
    writer.writerow([prefix + "RECORD", "", ""])
    files[prefix + "RECORD"] = record.getvalue().encode()
    if corrupt:
        files["prismabuild/core.py"] = b"corrupted"
    if missing:
        files["unrecorded"] = b"extra"
    path = tmp_path / "candidate.whl"
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return path


def test_valid_wheel_is_hashed(tmp_path):
    wheel = wheel_fixture(tmp_path)
    result = release.validate_wheel(wheel, "0.1.0")
    assert result["wheel_files"] == 5
    assert result["wheel_sha256"] == hashlib.sha256(wheel.read_bytes()).hexdigest()


@pytest.mark.parametrize("fields,reason", [
    ({"version": "0.2.0"}, "name or version"),
    ({"corrupt": True}, "RECORD mismatch"),
    ({"missing": True}, "cover every file"),
])
def test_invalid_wheel_is_rejected(tmp_path, fields, reason):
    with pytest.raises(ValueError, match=reason):
        release.validate_wheel(wheel_fixture(tmp_path, **fields), "0.1.0")


def test_invalid_candidate_does_not_mutate_source_or_build(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    subprocess.run(["git", "init", "--quiet", str(source)], check=True)
    (source / "pyproject.toml").write_text('[project]\nname = "prismabuild"\nversion = "0.1.0"\n')
    subprocess.run(["git", "-C", str(source), "add", "."], check=True)
    subprocess.run(["git", "-C", str(source), "-c", "user.name=Test", "-c", "user.email=test@invalid",
                    "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "Fixture"], check=True)
    original = subprocess.run
    environments = []

    def guarded_run(arguments, **options):
        environments.append(options["env"])
        assert arguments[0] != sys.executable
        return original(arguments, **options)

    monkeypatch.setenv("GITHUB_TOKEN", "not-a-real-token")
    monkeypatch.setenv("PYTHONPATH", "/untrusted")
    monkeypatch.setattr(release.subprocess, "run", guarded_run)
    with pytest.raises(ValueError, match="does not match project version"):
        release.dry_run_release(source, "HEAD", "v0.2.0")
    monkeypatch.setattr(release.subprocess, "run", original)
    assert environments
    assert all("GITHUB_TOKEN" not in environment and "PYTHONPATH" not in environment for environment in environments)
    assert subprocess.check_output(["git", "-C", str(source), "tag"]) == b""
    assert subprocess.check_output(["git", "-C", str(source), "status", "--porcelain"]) == b""


@pytest.mark.parametrize("entry_points", [
    b"[gui_scripts]\nprismabuild = prismabuild.core:main\n",
    b"[console_scripts]\nother = prismabuild.core:main\n[other_group]\nprismabuild = prismabuild.core:main\n",
    b"[console_scripts]\nprismabuild = prismabuild.other:main\n",
    b"[console_scripts]\nprismabuild = prismabuild.core:main\nprismabuild = prismabuild.core:main\n",
    b"prismabuild = prismabuild.core:main\n",
    b"[DEFAULT]\nprismabuild = prismabuild.core:main\n[console_scripts]\n",
    b"[DEFAULT]\nprismabuild = prismabuild.core:main\n[console_scripts]\nother = prismabuild.core:main\n",
    b"[console_scripts] # comment\nprismabuild = prismabuild.core:main\n",
    b"[console_scripts] = ignored\nprismabuild = prismabuild.core:main\n",
    b"[console_scripts]\nprismabuild =\n    prismabuild.core:main\n",
    b"[console_scripts]\n; note\nprismabuild = prismabuild.core:main\n",
], ids=["wrong-group", "only-in-another-group", "wrong-target", "duplicate-key", "no-group",
        "inherited-from-default", "inherited-beside-another-script", "header-with-comment",
        "header-with-assignment", "continuation-line", "semicolon-comment"])
def test_cli_entry_point_must_be_the_console_script(tmp_path, entry_points):
    # A RECORD-consistent wheel whose prismabuild line sits outside
    # [console_scripts], or points elsewhere, installs no prismabuild command.
    with pytest.raises(ValueError, match="entry point"):
        release.validate_wheel(wheel_fixture(tmp_path, entry_points=entry_points), "0.1.0")


@pytest.mark.parametrize("entry_points", [
    VALID_ENTRY_POINTS,
    b"[console_scripts]\nprismabuild=prismabuild.core:main\n[gui_scripts]\nprismabuild-gui = prismabuild.core:main\n",
    b"[gui_scripts]\nprismabuild = prismabuild.core:main\n",
    b"[console_scripts]\nprismabuild = prismabuild.other:main\n",
    b"[DEFAULT]\nprismabuild = prismabuild.core:main\n[console_scripts]\n",
    b"[DEFAULT]\nprismabuild = prismabuild.core:main\n[console_scripts]\nother = prismabuild.core:main\n",
    b"[console_scripts] # comment\nprismabuild = prismabuild.core:main\n",
    b"[console_scripts] = ignored\nprismabuild = prismabuild.core:main\n",
    b"[console_scripts]\nprismabuild =\n    prismabuild.core:main\n",
    b"[console_scripts]\n; note\nprismabuild = prismabuild.core:main\n",
], ids=["valid", "valid-beside-gui", "wrong-group", "wrong-target", "inherited-from-default",
        "inherited-beside-another-script", "header-with-comment", "header-with-assignment",
        "continuation-line", "semicolon-comment"])
def test_the_console_script_check_agrees_with_importlib_metadata(tmp_path, entry_points):
    # Installers read entry_points.txt with importlib.metadata's parser, which has
    # no [DEFAULT] inheritance. A name only inherited there installs no command.
    dist_info = tmp_path / "prismabuild-0.1.0.dist-info"
    dist_info.mkdir()
    (dist_info / "entry_points.txt").write_bytes(entry_points)
    try:
        installed = [entry.value for entry in importlib.metadata.PathDistribution(dist_info).entry_points
                     if entry.group == "console_scripts" and entry.name == "prismabuild"]
    except (TypeError, ValueError):  # a line the installer's reader cannot split
        installed = []
    accepted = release.console_script(entry_points.decode()) == "prismabuild.core:main"
    assert accepted == (installed == ["prismabuild.core:main"])


def test_other_entry_point_groups_may_coexist(tmp_path):
    entry_points = (b"[console_scripts]\nprismabuild=prismabuild.core:main\n"
                    b"[gui_scripts]\nprismabuild-gui = prismabuild.core:main\n")
    assert release.validate_wheel(wheel_fixture(tmp_path, entry_points=entry_points), "0.1.0")["wheel_files"] == 5

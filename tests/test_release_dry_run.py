"""Release candidates must match their built, integrity-checked wheel."""
from __future__ import annotations

import base64
import csv
import hashlib
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


def wheel_fixture(tmp_path, *, version="0.1.0", corrupt=False, missing=False):
    prefix = f"prismabuild-{version}.dist-info/"
    files = {
        prefix + "METADATA": f"Metadata-Version: 2.1\nName: prismabuild\nVersion: {version}\n".encode(),
        prefix + "entry_points.txt": b"[console_scripts]\nprismabuild = prismabuild.core:main\n",
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

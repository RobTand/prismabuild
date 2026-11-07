"""Snapshot object construction must not inherit a managed Git launcher."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys

import pytest

from prismabuild import core, submission


REAL_GIT = shutil.which("git", path=os.defpath)
STAMP = "snapshot-stamp.json"
PAYLOAD = '{"command":["python","task.py"]}\n'


def _git(root, *args):
    return subprocess.run(
        [REAL_GIT, "-C", str(root), *args], check=True,
        capture_output=True, text=True,
    ).stdout.strip()


@pytest.fixture
def checkout(tmp_path, monkeypatch):
    monkeypatch.delenv("PRISMABUILD_SNAPSHOT_GIT", raising=False)
    root = tmp_path / "checkout"
    root.mkdir()
    _git(root, "init", "-q", "--template=", "--object-format=sha1")
    (root / "task.py").write_text("print('source')\n")
    _git(root, "add", "task.py")
    _git(root, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
         "commit", "-qm", "source")
    (root / "task.py").write_text("print('dirty snapshot')\n")
    return root


@pytest.fixture
def launcher(tmp_path, monkeypatch):
    directory = tmp_path / "launcher"
    directory.mkdir()
    wrapper = directory / "git"
    log = tmp_path / "launcher-calls.jsonl"
    wrapper.write_text(f"""#!{sys.executable}
import json
import os
import sys

environment = dict(os.environ)
saved = {{key: value for key, value in environment.items()
         if key.startswith(("GIT_AUTHOR_", "GIT_COMMITTER_"))}}
for key in list(environment):
    if key.startswith(("GIT_AUTHOR_", "GIT_COMMITTER_", "GIT_CONFIG_")):
        del environment[key]
with open({str(log)!r}, "a") as handle:
    handle.write(json.dumps(sys.argv[1:]) + "\\n")
if environment.get("FIXTURE_CAPABILITY", "yes") == "yes":
    field = environment.get("FIXTURE_SUBSTITUTE")
    if field:
        environment.update(saved)
        environment[field] = ("2030-01-01T00:00:00+00:00" if field.endswith("DATE")
                              else "substituted@example.invalid" if field.endswith("EMAIL")
                              else "Substituted Identity")
    else:
        for role in ("AUTHOR", "COMMITTER"):
            environment["GIT_" + role + "_NAME"] = environment.get("FIXTURE_IDENTITY", "Managed")
            environment["GIT_" + role + "_EMAIL"] = "managed@example.invalid"
            environment["GIT_" + role + "_DATE"] = environment.get("FIXTURE_NOW", "2030-01-01T00:00:00+00:00")
os.execve({REAL_GIT!r}, [{REAL_GIT!r}, "-c", "user.useConfigOnly=true", *sys.argv[1:]], environment)
""")
    wrapper.chmod(0o755)
    monkeypatch.setenv("PATH", str(directory))
    return wrapper, log


def _seal(checkout, store):
    cas = core.PrismaBuildCAS(store)
    snapshot = submission.snapshot_checkout(
        checkout, cas=cas, stamp_name=STAMP, stamp_payload=PAYLOAD,
    )
    bundle = cas.input_path(snapshot["input"])
    restored = store.parent / (store.name + "-restored.git")
    _git(checkout, "init", "-q", "--bare", "--template=", str(restored))
    _git(restored, "fetch", "-q", str(bundle), "refs/heads/prismabuild-snapshot")
    commit = _git(restored, "cat-file", "commit", snapshot["commit"])
    stamp = _git(restored, "show", f"{snapshot['commit']}:{STAMP}")
    assert stamp == PAYLOAD.strip()
    closure = submission.build_stamp_closure(STAMP, stamp + "\n")
    return snapshot, bundle.read_bytes(), commit, closure


@pytest.mark.parametrize("capability", ("yes", "no"))
def test_snapshot_ignores_launcher_identity_and_time(
    checkout, launcher, tmp_path, monkeypatch, capability,
):
    monkeypatch.setenv("FIXTURE_CAPABILITY", capability)
    with monkeypatch.context() as clean:
        clean.setenv("PATH", os.defpath)
        baseline = _seal(checkout, tmp_path / "cas-baseline")
    results = []
    for index, identity in enumerate(("First Submitter", "Second Submitter")):
        monkeypatch.setenv("FIXTURE_IDENTITY", identity)
        monkeypatch.setenv("FIXTURE_NOW", f"2030-01-0{index + 1}T00:00:00+00:00")
        for role in ("AUTHOR", "COMMITTER"):
            monkeypatch.setenv(f"GIT_{role}_NAME", identity)
            monkeypatch.setenv(f"GIT_{role}_EMAIL", f"submitter{index}@example.invalid")
            monkeypatch.setenv(f"GIT_{role}_DATE", f"2040-01-0{index + 1}T00:00:00+00:00")
        results.append(_seal(checkout, tmp_path / f"cas-{index}"))
    first, second = results
    assert first[0]["commit"] == second[0]["commit"]
    assert first[0]["input"] == second[0]["input"]
    assert first[1] == second[1]
    assert first[3]["closure_sha256"] == second[3]["closure_sha256"]
    assert first == baseline
    for role in ("author", "committer"):
        assert f"{role} PrismaBuild <prismabuild@example.invalid> 946684800 +0000\n" in first[2]
    body = {"schema": "prismaquant.prismabuild.code_closure.v1", "files": [
        {"path": STAMP, "sha256": hashlib.sha256(PAYLOAD.encode()).hexdigest(),
         "bytes": len(PAYLOAD.encode())},
    ]}
    assert first[3]["closure_sha256"] == core.canonical_sha256(body)
    calls = [json.loads(line) for line in launcher[1].read_text().splitlines()]
    assert calls
    assert not any(verb in call for call in calls for verb in (
        "read-tree", "add", "hash-object", "update-index", "write-tree",
        "commit-tree", "update-ref", "init", "pack-objects",
    ))


def test_snapshot_refuses_when_only_launcher_is_available(checkout, launcher, monkeypatch, tmp_path):
    monkeypatch.setattr(os, "defpath", str(launcher[0].parent))
    cas = core.PrismaBuildCAS(tmp_path / "cas")
    monkeypatch.setattr(cas, "ingest_input", lambda *args, **kwargs: pytest.fail("noncanonical seal"))
    with pytest.raises(submission.SubmissionRefused, match="Git wrapper.*sealed identity"):
        submission.snapshot_checkout(checkout, cas=cas)


def test_explicit_native_git_override(checkout, launcher, tmp_path, monkeypatch):
    monkeypatch.setenv("PRISMABUILD_SNAPSHOT_GIT", REAL_GIT)
    snapshot, bundle, commit, closure = _seal(checkout, tmp_path / "cas")
    assert "author PrismaBuild <prismabuild@example.invalid> 946684800 +0000\n" in commit
    assert bundle.startswith(b"# v2 git bundle\n")


@pytest.mark.parametrize("override", ("git", "/nonexistent/prismabuild/git", ""))
def test_invalid_override_refuses_without_launcher_fallback(checkout, launcher, monkeypatch, override):
    monkeypatch.setenv("PRISMABUILD_SNAPSHOT_GIT", override)
    with pytest.raises(submission.SubmissionRefused, match="absolute executable path"):
        submission._snapshot_git(checkout, ["rev-parse", "HEAD"])


@pytest.mark.parametrize("field", (
    "GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL", "GIT_AUTHOR_DATE",
    "GIT_COMMITTER_NAME", "GIT_COMMITTER_EMAIL", "GIT_COMMITTER_DATE",
))
def test_readback_refuses_substituted_identity_before_publication(
    checkout, launcher, tmp_path, monkeypatch, field,
):
    monkeypatch.setenv("PRISMABUILD_SNAPSHOT_GIT", str(launcher[0]))
    monkeypatch.setenv("FIXTURE_SUBSTITUTE", field)
    cas = core.PrismaBuildCAS(tmp_path / "cas")
    monkeypatch.setattr(cas, "ingest_input", lambda *args, **kwargs: pytest.fail("noncanonical seal"))
    with pytest.raises(submission.SubmissionRefused, match="Git wrapper.*sealed identity"):
        submission.snapshot_checkout(checkout, cas=cas)

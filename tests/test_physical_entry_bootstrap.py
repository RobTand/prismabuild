"""Cold physical entries bind their own source or published generation."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(params=["source", "flat", "nested"])
def physical_entry(request, tmp_path):
    if request.param == "source":
        return ROOT, ROOT / "tools" / "fleet"
    # Copy the real publisher members, without publishing or inventing a receipt.
    # Keep both physical spellings, just as a generation does.
    import publish_runtime

    generation = tmp_path / "store" / "generation"
    for member in publish_runtime._publication_manifest():
        if not member.startswith(("src/", "tools/")):
            continue
        destination = generation / member
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(publish_runtime._source_for(member), destination)
    directory = generation / "tools"
    if request.param == "nested":
        directory /= "fleet"
    return generation, directory


def _cold(entry, cwd, *args):
    # -I excludes cwd, PYTHONPATH and user site; -S excludes all installed sites.
    # No path injection occurs in the child. The physical entry owns bootstrap.
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith("PYTHON")}
    result = subprocess.run([sys.executable, "-I", "-S", *args], cwd=cwd,
                            env=environment, capture_output=True, text=True,
                            timeout=30)
    return result


def _git_entry(root, *args):
    return subprocess.check_output([
        "git", "-C", str(root), "-c", "user.name=Test", "-c",
        "user.email=test@example.invalid", *args], text=True).strip()


def test_pbsnapshot_cold_physical_entry(physical_entry, tmp_path):
    generation, directory = physical_entry
    entry = directory / "pbsnapshot.py"
    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    identity = _cold(entry, unrelated, "-c", '''
import json, pathlib, runpy, sys
entry = runpy.run_path(sys.argv[1], run_name="physical_entry_control")
owners = {name: str(pathlib.Path(entry[name].__file__).resolve())
          for name in ("submission", "pb", "pool")}
print(json.dumps(owners))
''', str(entry))
    assert identity.returncode == 0, identity.stderr
    assert json.loads(identity.stdout) == {
        alias: str(generation / "src" / "prismabuild" / f"{owner}.py")
        for alias, owner in (("submission", "submission"), ("pb", "core"), ("pool", "pool"))}
    help_result = _cold(entry, unrelated, str(entry), "--help")
    assert help_result.returncode == 0, help_result.stderr
    assert "verify" in help_result.stdout
    checkout = tmp_path / "ordinary"
    checkout.mkdir()
    _git_entry(checkout, "init", "-q")
    _git_entry(checkout, "commit", "-qm", "ordinary source", "--allow-empty")
    ordinary = _cold(entry, unrelated, str(entry), "verify", str(checkout), "HEAD")
    assert ordinary.returncode == 0, ordinary.stderr
    assert json.loads(ordinary.stdout) == {
        "schema": "prismabuild.checkout_snapshot.v1", "snapshot": False, "generated": []}
    _git_entry(checkout, "commit", "-qm", "PrismaBuild pbrun checkout snapshot v1", "--allow-empty")
    refused = _cold(entry, unrelated, str(entry), "verify", str(checkout), "HEAD")
    assert refused.returncode == 1, refused.stderr
    assert refused.stdout == ""
    assert "pbsnapshot: refused: pbrun action locator is unavailable" in refused.stderr


def test_produced_export_cold_physical_entry(physical_entry, tmp_path):
    generation, directory = physical_entry
    entry = directory / "produced_export.py"
    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    identity = _cold(entry, unrelated, "-c", '''
import json, pathlib, runpy, sys
entry = runpy.run_path(sys.argv[1], run_name="physical_entry_control")
owner = sys.modules[entry["main"].__module__]
print(json.dumps({"owner": str(pathlib.Path(owner.__file__).resolve()),
                  "main": entry["main"] is owner.main}))
''', str(entry))
    assert identity.returncode == 0, identity.stderr
    assert json.loads(identity.stdout) == {
        "owner": str(generation / "src" / "prismabuild" / "produced_spool.py"), "main": True}
    help_result = _cold(entry, unrelated, str(entry), "--help")
    assert help_result.returncode == 0, help_result.stderr
    assert all(option in help_result.stdout for option in (
        "--queue", "--manifest", "--manifest-sha256", "--pace-mb-s", "--pace-tier"))
    missing = _cold(entry, unrelated, str(entry))
    assert missing.returncode == 2, missing.stderr
    assert "the following arguments are required: --queue, --manifest, --manifest-sha256" in missing.stderr
    assert missing.stdout == ""

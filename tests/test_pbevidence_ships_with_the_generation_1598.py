"""The new evidence tool travels with the generation, and pbrun still starts there (#1598).

``pbrun`` imports ``pbevidence`` without a condition.  The publisher copies only
the scripts it lists, so a missing entry makes it refuse every publication and
every canary, including commands that never use the new option.  A box without
a checkout runs only what the generation carries, so the check publishes the
real tree and starts the commands from that copy alone. The fixture stages a
sealed generation and uses the real pointer owner on its private mirror.
It does not qualify installed-reader activation.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from test_publish_runtime import ROOT, _fake_git_and_probe, publish_runtime

_RUN = subprocess.run


def _published(tmp_path, monkeypatch) -> Path:
    mirror = tmp_path / "fleet" / "repo"
    with monkeypatch.context() as publication:
        publication.setattr(publish_runtime, "CHECKOUT", ROOT)
        publication.setattr(publish_runtime, "MIRROR", mirror)
        store = mirror.parent / "runtime-generations"
        before = set(store.iterdir()) if store.exists() else set()
        publication.setattr(publish_runtime.subprocess, "run", _fake_git_and_probe("a" * 40))
        publication.setattr(sys, "argv", [
            "publish_runtime.py", "--stage-only", "--rollout", "rolling",
            "--rollout-reason", "fixture publication",
            "--shape-gate-waiver", "fixture publication"])
        assert publish_runtime.main() == 0
        [created] = set(store.iterdir()) - before
        generation, _receipt = publish_runtime._sealed_generation(created.name)
        publish_runtime._activate(generation, migrate_directory=False)
    return mirror.resolve()


def test_the_publication_carries_the_evidence_tool_in_both_layouts():
    manifest = publish_runtime._publication_manifest()
    assert "tools/pbevidence.py" in manifest
    assert "tools/fleet/pbevidence.py" in manifest


def test_no_published_script_imports_a_module_the_publication_lacks():
    assert publish_runtime._unshipped_imports(
        publish_runtime._publication_manifest()) == []


@pytest.mark.parametrize("tool,fragment", [
    ("pbrun.py", "--target-evidence"),
    ("pbgang.py", "--target-evidence"),
    ("pbevidence.py", "--out"),
    ("tier_loop.py", "--source-pool"),
    ("fleet/tier_loop.py", "--source-pool"),
])
def test_the_command_starts_from_the_generation_alone(
        tmp_path, monkeypatch, tool, fragment):
    generation = _published(tmp_path, monkeypatch)
    consumer = tmp_path / "consumer"
    consumer.mkdir()
    env = {key: value for key, value in os.environ.items()
           if key not in {"PYTHONPATH", "PYTHONHOME"}}
    completed = _RUN(
        [sys.executable, "-I", str(generation / "tools" / tool), "--help"],
        cwd=consumer, env=env, capture_output=True, text=True, check=False)
    assert completed.returncode == 0, completed.stderr
    assert fragment in completed.stdout

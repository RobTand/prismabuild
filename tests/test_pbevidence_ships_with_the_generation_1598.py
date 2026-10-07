"""The new evidence tool travels with the generation, and pbrun still starts there (#1598).

``pbrun`` imports ``pbevidence`` without a condition.  The publisher copies only
the scripts it lists, so a missing entry makes it refuse every publication and
every canary, including commands that never use the new option.  A box without
a checkout runs only what the generation carries, so the check publishes the
real tree and starts the commands from that copy alone.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from test_publish_runtime import ROOT, _fake_git_and_probe, publish_runtime

#: The publisher fixture replaces ``subprocess.run`` for the whole process, so
#: the commands below start through the function bound before any fixture runs.
_RUN = subprocess.run


def _published(tmp_path, monkeypatch) -> Path:
    mirror = tmp_path / "fleet" / "repo"
    monkeypatch.setattr(publish_runtime, "CHECKOUT", ROOT)
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    monkeypatch.setattr(publish_runtime.subprocess, "run", _fake_git_and_probe("a" * 40))
    monkeypatch.setattr(sys, "argv", [
        "publish_runtime.py", "--rollout", "rolling", "--no-canary",
        "--rollout-reason", "fixture publication",
        "--shape-gate-waiver", "fixture publication"])
    assert publish_runtime.main() == 0
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

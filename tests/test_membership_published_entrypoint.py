"""Membership must ship and resolve its own immutable runtime layout."""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tools" / "fleet"))
import publish_runtime as publisher  # noqa: E402


def test_membership_is_in_both_published_layouts(monkeypatch):
    monkeypatch.setattr(publisher, "CHECKOUT", REPO)
    manifest = publisher._publication_manifest()
    assert "tools/fleet_membership.py" in manifest
    assert "tools/fleet/fleet_membership.py" in manifest
    assert manifest["tools/fleet_membership.py"] == manifest[
        "tools/fleet/fleet_membership.py"]


@pytest.mark.parametrize("relative", [
    "tools/fleet_membership.py", "tools/fleet/fleet_membership.py"])
def test_membership_import_binds_published_generation(
        tmp_path, monkeypatch, relative):
    monkeypatch.setattr(publisher, "CHECKOUT", REPO)
    generation = tmp_path / "generations" / "candidate"
    for name in publisher._publication_manifest():
        target = generation / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(publisher._source_for(name), target)
    # Keep the root-resolution regression independent of the publication
    # inclusion check above: the old publisher omitted this command.
    entry = generation / relative
    entry.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(REPO / "tools/fleet/fleet_membership.py", entry)
    code = (
        "import importlib.util,json,sys;from pathlib import Path;"
        "p=Path(sys.argv[1]);sys.path.insert(0,str(p.parent));"
        "s=importlib.util.spec_from_file_location('published_membership',p);"
        "m=importlib.util.module_from_spec(s);s.loader.exec_module(m);"
        "print(json.dumps({'root':str(m.RUNTIME_ROOT),'src':str(m.SRC_ROOT)}))"
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(entry)],
        cwd=tmp_path, text=True, capture_output=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "root": str(generation), "src": str(generation / "src")}

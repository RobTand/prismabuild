"""A source v2 submitter does not assume the storage role was upgraded."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
import pbrun  # noqa: E402


@pytest.mark.parametrize("use_defaults", [False, True])
def test_v2_requires_published_and_verified_core_and_storage_reader(
        tmp_path: Path, monkeypatch, use_defaults) -> None:
    source = tmp_path / "source"
    published = tmp_path / "fleet" / "repo"
    monkeypatch.setattr(pbrun, "RUNTIME_ROOT", source)
    monkeypatch.setattr(pbrun, "SH", published.parent)
    roots = {} if use_defaults else {"source_root": source, "published_root": published}
    members = ("src/prismabuild/core.py", "tools/fleet/prewarm_loop.py")
    files = {}
    for member in members:
        original = source / member
        deployed = published / member
        original.parent.mkdir(parents=True, exist_ok=True)
        deployed.parent.mkdir(parents=True, exist_ok=True)
        original.write_text(f"new {member}")
        deployed.write_bytes(original.read_bytes())
        files[member] = hashlib.sha256(original.read_bytes()).hexdigest()
    (published / "RUNTIME_VERSION.json").write_text(json.dumps({"files": files}))
    pbrun.require_deployed_read_plan_storage(**roots)
    (published / members[1]).write_text("old prewarmer")
    with pytest.raises(SystemExit, match="compatible published storage"):
        pbrun.require_deployed_read_plan_storage(**roots)
    (published / members[1]).write_bytes((source / members[1]).read_bytes())
    files[members[1]] = "0" * 64
    (published / "RUNTIME_VERSION.json").write_text(json.dumps({"files": files}))
    with pytest.raises(SystemExit, match="compatible published storage"):
        pbrun.require_deployed_read_plan_storage(**roots)


def test_v2_progress_phases_must_follow_the_read_timeline() -> None:
    manifest = {"read_plan": {"phases": [{"name": "a"}, {"name": "b"}]}}
    valid = {"phases": [{"name": "startup"}, {"name": "a"},
                        {"name": "compute"}, {"name": "b"}]}
    pbrun.require_linear_read_plan_progress(manifest, valid)
    for wrong in (None, {**valid, "cycle": True},
                  {"phases": [{"name": "a"}]},
                  {"phases": [{"name": "b"}, {"name": "a"}]}):
        with pytest.raises(SystemExit):
            pbrun.require_linear_read_plan_progress(manifest, wrong)

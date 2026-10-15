"""A pbrun killed mid-snapshot leaves a workspace the next pbrun reaps.

The snapshot is built in a private ``pbrun-snapshot.*`` directory under
``TMPDIR`` holding an index, an object store and a bare repository.  A
``TemporaryDirectory`` only removed it when pbrun got to its ``finally``; a
SIGKILL left it, and four were found in one agent's ``TMPDIR`` on dl380g10.
The workspace now carries an owner lock, so the next pbrun reaps a dead one
once its retention window has passed, and never a live one.
"""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import pbrun  # noqa: E402
from prismabuild import owned_tree  # noqa: E402

FLEET = Path(__file__).resolve().parents[1] / "tools" / "fleet"
HOLDER = """\
import sys
sys.path.insert(0, sys.argv[1])
import pbrun
with pbrun.owned_snapshot_directory() as workspace:
    (workspace / "objects").mkdir()
    print(workspace, flush=True)
    sys.stdin.readline()
"""


def _holder(scratch: Path) -> tuple[subprocess.Popen, Path]:
    process = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(FLEET)], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, text=True, env={**os.environ, "TMPDIR": str(scratch)})
    line = process.stdout.readline().strip()
    assert line, f"the holder never built its workspace (rc={process.poll()})"
    return process, Path(line)


def _snapshot_once() -> None:
    with pbrun.owned_snapshot_directory() as workspace:
        (workspace / "objects" / "pack").mkdir(parents=True)


def _age(workspace: Path, seconds: float) -> None:
    then = time.time() - seconds
    for path in (workspace, *workspace.iterdir()):
        os.utime(path, (then, then), follow_symlinks=False)


def _scratch(tmp_path: Path, monkeypatch) -> Path:
    """A TMPDIR of the test's own: conftest fixtures put files in tmp_path."""
    scratch = tmp_path / "tmpdir"
    scratch.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(scratch))
    return scratch


def test_a_snapshot_workspace_leaves_nothing(tmp_path, monkeypatch):
    scratch = _scratch(tmp_path, monkeypatch)
    _snapshot_once()
    assert list(scratch.iterdir()) == []


def test_a_killed_pbruns_workspace_is_reaped_after_its_window(tmp_path, monkeypatch):
    scratch = _scratch(tmp_path, monkeypatch)
    process, workspace = _holder(scratch)
    assert workspace.parent == scratch
    try:
        _age(workspace, 30 * 86400)
        _snapshot_once()
        assert (workspace / "objects").is_dir(), "a live workspace was reaped"
    finally:
        process.kill()
        process.wait(timeout=30)
    _snapshot_once()                          # first seen dead: the clock starts
    assert workspace.is_dir()
    _age(workspace, owned_tree.RETENTION_S + 3600)
    _snapshot_once()
    assert list(scratch.iterdir()) == []

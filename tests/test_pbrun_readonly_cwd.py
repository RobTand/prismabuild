"""Read-only source checkouts can submit through a private stamp overlay.

The worker verifies the stamp and writes results in its materialized tree.
The submitting checkout only needs the generated-file excludes established.
"""

from __future__ import annotations

import os
from pathlib import Path
import stat
import subprocess
import sys

import pytest

# The local tree's ``prismabuild`` first, so this file drives the code it is
# testing.  An ordinary import rather than ``importlib``: a module executed
# from a file and left out of ``sys.modules`` is a second ``pbrun`` object,
# and conftest's live-store guard repoints ``SH`` on the registered one alone.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(
    0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet")
)

import pbrun  # noqa: E402
from test_pbrun_detach import _queue, _run_pbrun


def _git(cwd: Path, *argv: str) -> None:
    subprocess.run(
        ["git", "-C", str(cwd), *argv], check=True, capture_output=True,
        env={**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@x",
             "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@x"},
    )


@pytest.fixture
def readonly_checkout(tmp_path: Path):
    cwd = tmp_path / "checkout"
    cwd.mkdir()
    _git(cwd, "init", "-q")
    (cwd / "hello.txt").write_text("hello\n")
    _git(cwd, "add", "hello.txt")
    _git(cwd, "commit", "-q", "-m", "one")
    # The excludes are already in place, as they are on any checkout that has
    # submitted before; no new source-side file should be needed.
    pbrun.keep_droppings_out_of_git(cwd)
    made_readonly: list[Path] = []
    for directory in [cwd, *[p for p in cwd.rglob("*") if p.is_dir()]]:
        directory.chmod(stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP)
        made_readonly.append(directory)
    if os.access(cwd, os.W_OK):
        pytest.skip("this user writes read-only directories (root?)")
    try:
        yield cwd
    finally:
        for directory in made_readonly:
            directory.chmod(0o755)


def test_a_read_only_checkout_can_submit_a_snapshot(
    readonly_checkout: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    queue = _queue(tmp_path)
    assert _run_pbrun(tmp_path, monkeypatch, readonly_checkout, "--detach") == 0
    assert len(queue.ready_items()) == 1
    # Nothing of ours is left behind in a tree we could not write to.
    assert not list(readonly_checkout.glob(f"{pbrun.STAMP_PREFIX}*"))


def test_this_copy_of_pbrun_is_the_one_the_guard_repoints(tmp_path: Path) -> None:
    """A module loaded from a file and left out of ``sys.modules`` is a second
    ``pbrun``, and conftest's live-store guard repoints ``pbrun.SH`` on the
    registered one alone. These tests stayed off the mount only because
    ``main`` refuses before it reaches ``SH``, which is luck rather than a
    guard."""

    assert sys.modules.get("pbrun") is pbrun
    assert tmp_path in pbrun.SH.parents, pbrun.SH

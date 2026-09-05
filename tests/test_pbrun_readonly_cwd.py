"""A checkout that cannot hold the closure stamp is refused, not crashed on.

``pbrun`` writes its closure stamp (``.pbrun-closure.<fingerprint>.json``)
into the checkout, and the action tees its output to a result file in the
same tree, so a checkout that is not writable cannot be submitted from at all.
Issue #45: the three-node container harness mounts the repository read-only,
and the stamp write escaped ``main`` as a bare ``OSError`` with a traceback.
The refusal has to name the directory and say what the tree must hold.
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
    # submitted before; the first write pbrun then attempts is the stamp.
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


def test_a_read_only_checkout_is_refused_by_name(
    readonly_checkout: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        sys, "argv", ["pbrun.py", "--cwd", str(readonly_checkout), "--", "true"]
    )
    with pytest.raises(SystemExit) as caught:
        pbrun.main()
    message = str(caught.value)
    assert message.startswith("pbrun: "), message
    assert str(readonly_checkout) in message, message
    assert "closure stamp" in message, message
    assert "result" in message, message
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

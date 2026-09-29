"""Fleet tools run git through the repo's own core._git_run (#1318).

The tools insert the repo ``src`` on ``sys.path`` and import
``prismabuild.core`` lazily. The tests pin that the imported copy is the
repo copy the tool inserted, not whichever ``prismabuild`` happened to be
importable, and that the git answer is unchanged.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
REPO_SRC = REPOSITORY / "src"
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

import admission_shared_io  # noqa: E402
import pbcanary  # noqa: E402


@pytest.fixture
def fresh_prismabuild(monkeypatch):
    """Forget any imported prismabuild so the tool's own import decides."""
    for name in [n for n in sys.modules if n == "prismabuild" or n.startswith("prismabuild.")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.setattr(sys, "path", list(sys.path))
    yield


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "r"
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    return repo


def _assert_repo_core() -> None:
    core = sys.modules["prismabuild.core"]
    assert Path(core.__file__).resolve().is_relative_to(REPO_SRC.resolve())


def test_default_checkout_resolves_the_repo_core(fresh_prismabuild, tmp_path, monkeypatch):
    repo = _repo(tmp_path)
    monkeypatch.chdir(repo)
    assert pbcanary.default_checkout().resolve() == repo.resolve()
    _assert_repo_core()


def test_default_checkout_refuses_outside_a_git_tree(fresh_prismabuild, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path.parent))
    with pytest.raises(pbcanary.PreconditionRefused):
        pbcanary.default_checkout()


def test_admission_git_head_resolves_the_repo_core(fresh_prismabuild, tmp_path, monkeypatch):
    repo = _repo(tmp_path)
    subprocess.run(
        ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t",
         "commit", "-q", "--allow-empty", "-m", "x"],
        check=True,
    )
    head = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    monkeypatch.chdir(repo)
    assert admission_shared_io.git_head() == head
    _assert_repo_core()

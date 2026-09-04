"""The fleet runtime is one identified generation, never a bag of live copies."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "publish_runtime", ROOT / "tools" / "fleet" / "publish_runtime.py"
)
publish_runtime = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(publish_runtime)  # type: ignore[union-attr]


def _checkout(path: Path, generation: str) -> Path:
    package = path / "src" / "prismabuild"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "core.py").write_text(f"GENERATION = {generation!r}\n")
    (package / "pool.py").write_text(f"GENERATION = {generation!r}\n")
    return path


def test_publish_refuses_an_unproved_commit_before_touching_the_mirror(
    tmp_path, monkeypatch,
) -> None:
    """A blank version is not a receipt and must never accompany live bytes."""

    checkout = _checkout(tmp_path / "checkout", "new")
    mirror = tmp_path / "mirror"
    monkeypatch.setattr(publish_runtime, "CHECKOUT", checkout)
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    monkeypatch.setattr(publish_runtime, "FLEET_SCRIPTS", ())
    monkeypatch.setattr(publish_runtime, "FLEET_DATA", ())
    monkeypatch.setattr(
        publish_runtime.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=128, stdout="", stderr="fatal: not a git repository\n"
        ),
    )
    monkeypatch.setattr(sys, "argv", ["publish_runtime.py"])

    with pytest.raises(SystemExit, match="cannot prove.*40-hex Git commit"):
        publish_runtime.main()

    assert not mirror.exists(), "an unidentified generation reached the live path"


def test_untracked_published_files_make_the_runtime_tree_dirty(monkeypatch) -> None:
    """A commit cannot identify a source member Git does not track."""

    def git_result(*argv: str):
        output = "" if "--untracked-files=no" in argv else "?? src/prismabuild/new.py\n"
        return SimpleNamespace(returncode=0, stdout=output, stderr="")

    monkeypatch.setattr(publish_runtime, "_git_result", git_result)

    assert publish_runtime._working_tree_dirty() is True

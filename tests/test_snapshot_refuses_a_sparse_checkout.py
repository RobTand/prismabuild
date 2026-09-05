"""A sparse checkout cannot honestly seal the tree its key claims to cover.

Issue #57: "Sparse checkouts seal the full HEAD tree (`pbrun.py:607`)."
Measured before the fix: with ``git sparse-checkout set keep``, the sealed tree
carried ``away/b.txt`` from ``HEAD`` even though the submitter had no copy of
it on disk.  The action key then covers bytes the person who typed the command
could not read, review, or change.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import core as core_module  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "pbrun", Path(__file__).resolve().parents[1] / "tools" / "fleet" / "pbrun.py"
)
pbrun = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbrun)                       # type: ignore[union-attr]

MAX_BYTES = 16 * 1024 * 1024


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(cwd), *args], capture_output=True, text=True,
        check=False,
    )


def _checkout(tmp_path: Path) -> tuple[Path, str]:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    assert _git(checkout, "init", "-q", "-b", "main").returncode == 0
    assert _git(
        checkout, "config", "user.email", "test@example.invalid"
    ).returncode == 0
    assert _git(checkout, "config", "user.name", "PrismaBuild test").returncode == 0
    (checkout / "keep").mkdir()
    (checkout / "keep" / "a.txt").write_text("in the cone\n")
    (checkout / "away").mkdir()
    (checkout / "away" / "b.txt").write_text("out of the cone\n")
    assert _git(checkout, "add", "-A").returncode == 0
    assert _git(checkout, "commit", "-qm", "sealed tree").returncode == 0
    stamp_name = f"{pbrun.STAMP_PREFIX}sparse.json"
    (checkout / stamp_name).write_text(
        json.dumps({"cwd": ".", **pbrun._git_identity(checkout)})
    )
    return checkout, stamp_name


def _seal(checkout: Path, stamp_name: str, store: Path) -> dict[str, object]:
    return pbrun.build_git_checkout_snapshot(
        checkout,
        stamp_name=stamp_name,
        cas=core_module.PrismaBuildCAS(store),
        max_bytes=MAX_BYTES,
    )


def test_a_sparse_checkout_is_refused_before_anything_is_sealed(
    tmp_path: Path,
) -> None:
    checkout, stamp_name = _checkout(tmp_path)
    assert _git(checkout, "sparse-checkout", "set", "keep").returncode == 0
    assert not (checkout / "away").exists()

    with pytest.raises(SystemExit) as refusal:
        _seal(checkout, stamp_name, tmp_path / "cas")

    message = str(refusal.value)
    assert "away/b.txt" in message
    assert "git sparse-checkout disable" in message
    assert not (tmp_path / "cas").exists()


def test_a_path_marked_skip_worktree_by_hand_is_refused_too(
    tmp_path: Path,
) -> None:
    """The same defect without the porcelain: the bytes still come from HEAD."""

    checkout, stamp_name = _checkout(tmp_path)
    assert _git(
        checkout, "update-index", "--skip-worktree", "away/b.txt"
    ).returncode == 0
    (checkout / "away" / "b.txt").write_text("edited but invisible to add\n")

    with pytest.raises(SystemExit) as refusal:
        _seal(checkout, stamp_name, tmp_path / "cas")

    assert "away/b.txt" in str(refusal.value)


def test_an_ordinary_dense_checkout_still_seals(tmp_path: Path) -> None:
    checkout, stamp_name = _checkout(tmp_path)

    snapshot = _seal(checkout, stamp_name, tmp_path / "cas")

    assert snapshot["parent"] == pbrun._git_identity(checkout)["head"]

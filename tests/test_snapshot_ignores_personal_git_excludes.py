"""The sealed tree is a function of the repository, not of the submitter.

Issue #57: ``git ls-files --exclude-standard`` and ``git add -A`` both honour
``core.excludesFile``, which is a personal setting on the box that submits.
Measured before the fix: the same bytes sealed two different trees, and so two
different action keys, depending on whether the submitter had a global exclude
file that matched an untracked path.  The repository's own ``.gitignore`` is
part of the tree and still applies.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import core as core_module  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "pbrun", Path(__file__).resolve().parents[1] / "tools" / "fleet" / "pbrun.py"
)
pbrun = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbrun)                       # type: ignore[union-attr]

MAX_BYTES = 16 * 1024 * 1024

#: The tree an ordinary clean checkout of the fixture below seals.  Measured on
#: the code this test was written against, so a change to which bytes reach the
#: sealed tree has to move this literal deliberately.  Every input is fixed:
#: the object format, the file bytes, the file modes, and a stamp payload that
#: does not carry the fixture's commit id.
ORDINARY_TREE = "692b61776ec3e6092fa1afed8a28464315c15310"

STAMP_PAYLOAD = json.dumps(
    {"cwd": ".", "head": "0" * 40, "dirty_sha256": "1" * 64},
    indent=1,
    sort_keys=True,
)


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(cwd), *args], capture_output=True, text=True,
        check=False,
    )


def _checkout(tmp_path: Path) -> tuple[Path, str]:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    assert _git(
        checkout, "init", "-q", "-b", "main", "--object-format=sha1"
    ).returncode == 0
    assert _git(
        checkout, "config", "user.email", "test@example.invalid"
    ).returncode == 0
    assert _git(checkout, "config", "user.name", "PrismaBuild test").returncode == 0
    (checkout / "task.py").write_text("VALUE = 'sealed'\n")
    (checkout / ".gitignore").write_text("ignored.txt\n")
    assert _git(checkout, "add", "task.py", ".gitignore").returncode == 0
    assert _git(checkout, "commit", "-qm", "sealed tree").returncode == 0
    # Untracked and kept: the repository says nothing about it.
    (checkout / "notes.txt").write_text("untracked but not ignored\n")
    # Untracked and dropped: the repository's own .gitignore matches it.
    (checkout / "ignored.txt").write_text("generated\n")
    stamp_name = f"{pbrun.STAMP_PREFIX}excludes.json"
    (checkout / stamp_name).write_text(STAMP_PAYLOAD)
    return checkout, stamp_name


def _seal(checkout: Path, stamp_name: str, store: Path) -> tuple[str, list[str]]:
    cas = core_module.PrismaBuildCAS(store)
    snapshot = pbrun.build_git_checkout_snapshot(
        checkout, stamp_name=stamp_name, cas=cas, max_bytes=MAX_BYTES
    )
    materialized = store.parent / f"materialized-{store.name}"
    materialized.mkdir()
    assert _git(materialized, "init", "-q").returncode == 0
    assert _git(
        materialized, "fetch", "-q", str(cas.input_path(snapshot["input"])),
        "refs/heads/prismabuild-snapshot",
    ).returncode == 0
    assert _git(
        materialized, "checkout", "-q", "--detach", str(snapshot["commit"])
    ).returncode == 0
    tree = _git(materialized, "rev-parse", "HEAD^{tree}")
    assert tree.returncode == 0
    listing = _git(materialized, "ls-tree", "-r", "--name-only", "HEAD")
    assert listing.returncode == 0
    return tree.stdout.strip(), sorted(listing.stdout.split())


def test_a_global_exclude_file_does_not_change_the_sealed_tree(
    tmp_path: Path,
) -> None:
    checkout, stamp_name = _checkout(tmp_path)
    plain_tree, plain_roster = _seal(checkout, stamp_name, tmp_path / "cas-plain")

    excludes = tmp_path / "personal-excludes"
    excludes.write_text("notes.txt\n")
    assert _git(
        checkout, "config", "core.excludesFile", str(excludes)
    ).returncode == 0
    excluded_tree, excluded_roster = _seal(
        checkout, stamp_name, tmp_path / "cas-excluded"
    )

    assert "notes.txt" in plain_roster
    assert "notes.txt" in excluded_roster
    assert excluded_tree == plain_tree
    assert "notes.txt" in pbrun.snapshot_path_roster(checkout)


def test_the_repositorys_own_gitignore_still_keeps_a_path_out(
    tmp_path: Path,
) -> None:
    checkout, stamp_name = _checkout(tmp_path)
    _, roster = _seal(checkout, stamp_name, tmp_path / "cas")

    assert "ignored.txt" not in roster
    assert "notes.txt" in roster


def test_an_ordinary_clean_checkout_seals_the_tree_it_always_did(
    tmp_path: Path,
) -> None:
    """A tripwire: neither exclude handling nor sparse handling may move this."""

    checkout, stamp_name = _checkout(tmp_path)
    tree, _ = _seal(checkout, stamp_name, tmp_path / "cas")

    assert tree == ORDINARY_TREE

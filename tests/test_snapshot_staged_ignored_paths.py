"""A staged path the ignore rules match still belongs to the sealed tree.

``pbrun`` hashes the snapshot identity over the source index roster and then
seals a tree built in an alternate index.  Issue #69: the alternate index was
seeded from ``HEAD``, so ``git add -A`` applied the ignore rules to a path the
submitter had staged with ``git add -f`` and dropped it.  The identity said the
path was in the snapshot; the bundle did not carry it.
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


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(cwd), *args], capture_output=True, text=True,
        check=False,
    )


def _checkout(tmp_path: Path) -> Path:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    assert _git(checkout, "init", "-q").returncode == 0
    assert _git(
        checkout, "config", "user.email", "test@example.invalid"
    ).returncode == 0
    assert _git(checkout, "config", "user.name", "PrismaBuild test").returncode == 0
    (checkout / "task.py").write_text("VALUE = 'sealed'\n")
    (checkout / ".gitignore").write_text("ignored.txt\nbuild/\n")
    assert _git(checkout, "add", "task.py", ".gitignore").returncode == 0
    assert _git(checkout, "commit", "-qm", "sealed tree").returncode == 0
    return checkout


def _stamp(checkout: Path, name: str) -> str:
    stamp_name = f"{pbrun.STAMP_PREFIX}{name}.json"
    (checkout / stamp_name).write_text(
        json.dumps({"cwd": ".", **pbrun._git_identity(checkout)})
    )
    return stamp_name


def _sealed_roster(tmp_path: Path, snapshot: dict[str, object],
                   cas: core_module.PrismaBuildCAS) -> tuple[Path, list[str]]:
    materialized = tmp_path / "materialized"
    materialized.mkdir()
    assert _git(materialized, "init", "-q").returncode == 0
    assert _git(
        materialized, "fetch", "-q", str(cas.input_path(snapshot["input"])),
        "refs/heads/prismabuild-snapshot",
    ).returncode == 0
    assert _git(
        materialized, "checkout", "-q", "--detach", str(snapshot["commit"])
    ).returncode == 0
    listing = _git(materialized, "ls-tree", "-r", "--name-only", "HEAD")
    assert listing.returncode == 0
    return materialized, listing.stdout.split()


def test_a_newly_staged_ignored_file_is_sealed_with_its_working_bytes(
    tmp_path: Path,
) -> None:
    """``git add -f`` puts a path in the roster, so it must be in the tree."""

    checkout = _checkout(tmp_path)
    (checkout / "ignored.txt").write_text("required staged input\n")
    assert _git(checkout, "add", "-f", "ignored.txt").returncode == 0
    stamp_name = _stamp(checkout, "staged-ignored")
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")

    snapshot = pbrun.build_git_checkout_snapshot(
        checkout, stamp_name=stamp_name, cas=cas, max_bytes=MAX_BYTES
    )

    assert "ignored.txt" in pbrun.snapshot_path_roster(checkout)
    materialized, roster = _sealed_roster(tmp_path, snapshot, cas)
    assert "ignored.txt" in roster
    assert (materialized / "ignored.txt").read_text() == "required staged input\n"


def test_a_staged_ignored_file_seals_its_live_bytes_not_its_staged_bytes(
    tmp_path: Path,
) -> None:
    """The snapshot seals the working tree, so live edits win over the index."""

    checkout = _checkout(tmp_path)
    (checkout / "ignored.txt").write_text("staged bytes\n")
    assert _git(checkout, "add", "-f", "ignored.txt").returncode == 0
    (checkout / "ignored.txt").write_text("live bytes\n")
    stamp_name = _stamp(checkout, "staged-ignored-live")
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")

    snapshot = pbrun.build_git_checkout_snapshot(
        checkout, stamp_name=stamp_name, cas=cas, max_bytes=MAX_BYTES
    )

    materialized, roster = _sealed_roster(tmp_path, snapshot, cas)
    assert "ignored.txt" in roster
    assert (materialized / "ignored.txt").read_text() == "live bytes\n"


def test_a_staged_addition_removed_from_the_worktree_is_not_sealed(
    tmp_path: Path,
) -> None:
    """Seeding the index roster must not resurrect a path with no live bytes."""

    checkout = _checkout(tmp_path)
    (checkout / "ignored.txt").write_text("staged then deleted\n")
    assert _git(checkout, "add", "-f", "ignored.txt").returncode == 0
    (checkout / "ignored.txt").unlink()
    stamp_name = _stamp(checkout, "staged-then-deleted")
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")

    snapshot = pbrun.build_git_checkout_snapshot(
        checkout, stamp_name=stamp_name, cas=cas, max_bytes=MAX_BYTES
    )

    _, roster = _sealed_roster(tmp_path, snapshot, cas)
    assert "ignored.txt" not in roster


def test_a_tracked_deletion_stays_deleted_in_the_sealed_tree(
    tmp_path: Path,
) -> None:
    """A committed path removed from the worktree keeps its prior behaviour."""

    checkout = _checkout(tmp_path)
    (checkout / "task.py").unlink()
    stamp_name = _stamp(checkout, "tracked-deletion")
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")

    snapshot = pbrun.build_git_checkout_snapshot(
        checkout, stamp_name=stamp_name, cas=cas, max_bytes=MAX_BYTES
    )

    _, roster = _sealed_roster(tmp_path, snapshot, cas)
    assert "task.py" not in roster
    assert ".gitignore" in roster


def test_the_sealed_roster_is_the_roster_the_identity_hashes(
    tmp_path: Path,
) -> None:
    """One roster, hashed and sealed, across every staging state at once."""

    checkout = _checkout(tmp_path)
    (checkout / "ignored.txt").write_text("required staged input\n")
    assert _git(checkout, "add", "-f", "ignored.txt").returncode == 0
    (checkout / "untracked.py").write_text("VALUE = 'untracked'\n")
    (checkout / "build").mkdir()
    (checkout / "build" / "artifact.bin").write_bytes(b"never submitted")
    stamp_name = _stamp(checkout, "roster-identity")
    cas = core_module.PrismaBuildCAS(tmp_path / "cas")

    snapshot = pbrun.build_git_checkout_snapshot(
        checkout, stamp_name=stamp_name, cas=cas, max_bytes=MAX_BYTES
    )

    _, roster = _sealed_roster(tmp_path, snapshot, cas)
    assert sorted(roster) == sorted(pbrun.snapshot_path_roster(checkout))
    assert "build/artifact.bin" not in roster

"""A cached snapshot is a reference into one store, so it is keyed by store.

``seal_checkout_snapshot`` caches one bundle per checkout state, because a
dispatcher seals 120 shards out of one tree and 119 of those bundles are
identical bytes.  The cache key left out the store the caller named, and
ingestion is the half of the work that store decides: the second call over an
unchanged tree returned the first store's record without writing its blob
anywhere, so the returned ``params.checkout_snapshot`` referenced a payload the
requested CAS did not have.  Nothing refused.  The submission succeeded and the
node that won the allocation could not materialize the tree.
"""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402

import fleet_submit  # noqa: E402


def _producer_checkout(root: Path) -> Path:
    """A Git worktree with no pbrun stamp, which is what a producer has."""

    root.mkdir()
    for argv in (
        ["init", "-q"],
        ["config", "user.name", "PrismaBuild test"],
        ["config", "user.email", "t@example.invalid"],
    ):
        subprocess.run(["git", "-C", str(root), *argv], check=True)
    (root / "task.py").write_text("print(1)\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(root), "add", "task.py"], check=True)
    subprocess.run(
        ["git", "-C", str(root), "commit", "-qm", "producer source"], check=True)
    return root


def test_one_checkout_sealed_into_two_stores_lands_in_both(tmp_path: Path) -> None:
    """Both successful seals leave a verified bundle in the store they named."""

    checkout = _producer_checkout(tmp_path / "source")
    first = pb.PrismaBuildCAS(tmp_path / "first-cas")
    second = pb.PrismaBuildCAS(tmp_path / "second-cas")
    fleet_submit._SNAPSHOT_CACHE.clear()

    one = fleet_submit.seal_checkout_snapshot(checkout, cas=first)
    two = fleet_submit.seal_checkout_snapshot(checkout, cas=second)

    # One tree is one bundle, so the two records name the same content.
    assert one["input"] == two["input"]
    assert Path(first.input_path(one["input"])).is_file()
    assert Path(second.input_path(two["input"])).is_file()


def test_the_same_store_still_bundles_one_tree_once(tmp_path: Path) -> None:
    """The cache the fix narrows keeps the saving it was added for."""

    checkout = _producer_checkout(tmp_path / "source")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    fleet_submit._SNAPSHOT_CACHE.clear()

    calls = 0
    import pbrun

    original = pbrun.build_git_checkout_snapshot

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    pbrun.build_git_checkout_snapshot = counted
    try:
        for _ in range(3):
            fleet_submit.seal_checkout_snapshot(checkout, cas=cas)
    finally:
        pbrun.build_git_checkout_snapshot = original
    assert calls == 1

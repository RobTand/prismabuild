"""The prewarm loop writes one file and touches no work anybody has claimed.

Two claims, and they are the ones a reviewer of #487 has to be able to check
by reading the test rather than the loop: the loop never writes an item in any
state, and it never warms an action that is already claimed.  The second is
not politeness -- a claimed action is already reading, so warming it is at
best redundant and at worst evicts the prefix it has read.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet  # noqa: E402


def _tree(root: Path) -> dict[str, float]:
    return {str(p.relative_to(root)): p.stat().st_mtime_ns
            for p in sorted(root.rglob("*")) if p.is_file()}


def test_a_claimed_action_is_never_warmed(tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    key = fleet.action("claimed", [fleet.file("c.pt", 4096)])
    # Move it exactly as a claim does: out of ready, into claimed.
    (fleet.queue.root / "ready" / f"{key}.json").rename(
        fleet.queue.root / "claimed" / f"{key}.json")
    (fleet.queue.root / "claimed" / f"{key}.json").write_text(json.dumps(
        {"action_key": key, "claimed_unix": time.time()}))

    event = fleet.cycle(fleet.args())
    assert event["warmed"] == []
    assert event["skipped"] == []
    assert fleet.queue.prewarm(key) is None


def test_the_only_thing_the_loop_writes_is_its_own_record(
        tmp_path: Path) -> None:
    fleet = Fleet(tmp_path)
    warmable = fleet.action("warmable", [fleet.file("w.pt", 8192)])
    before_queue = _tree(fleet.queue.root)
    before_mount = _tree(fleet.mount)
    before_cas = _tree(fleet.cas_root)

    fleet.cycle(fleet.args())

    after_queue = _tree(fleet.queue.root)
    new = set(after_queue) - set(before_queue)
    assert new == {f"prewarm/{warmable}.json"}
    changed = {name for name in before_queue
               if after_queue.get(name) != before_queue[name]}
    assert changed == set()
    # The shared mount is read-only to this loop, and the CAS is too: a warm
    # is a read, and the receipt lives in the queue.
    assert _tree(fleet.mount) == before_mount
    assert _tree(fleet.cas_root) == before_cas


def test_a_manifest_entry_that_became_a_symlink_is_refused_not_followed(
        tmp_path: Path) -> None:
    """Validation approved a path; the open has to approve the inode.

    Nothing stops the bytes under an approved path from being replaced between
    submission and warm.  ``O_NOFOLLOW`` means such an entry fails and is
    counted as an error rather than sending the loop reading whatever the link
    points at -- outside the mount the manifest declared, in the worst case.
    """

    fleet = Fleet(tmp_path)
    path, _ = fleet.file("real.pt", 4096)
    key = fleet.action("swapped", [(path, 4096)])
    Path(path).unlink()
    Path(path).symlink_to(tmp_path / "elsewhere.pt")
    (tmp_path / "elsewhere.pt").write_bytes(b"\0" * 4096)

    fleet.cycle(fleet.args())
    record = fleet.queue.prewarm(key)
    assert record["bytes_warmed"] == 0
    assert record["status"] == "partial"
    assert record["errors"] and "real.pt" in record["errors"][0]

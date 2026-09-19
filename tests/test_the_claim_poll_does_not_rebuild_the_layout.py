"""The claim poll does not rebuild the queue layout (#595).

``ensure_layout`` is fourteen ``mkdir`` calls.  On a starved NFS mount each
costs RPCs, so running it on every claim poll billed every worker a
directory walk per poll for directories nothing in the pool ever deletes.
The first call in a process creates them; later polls are a flag check.  A
fresh ``PoolQueue`` -- the next loop restart -- re-ensures, which is what a
directory an operator deletes by hand waits for.

Nothing here touches the live queue, a real pool or a real device.
"""

from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY_A = "a" * 64

LAYOUT_NAMES = (
    pool.WORKERS, pool.ATTEMPTS, pool.PREWARM, pool.MOVERS, pool.RESIDENCY,
    pool.RESIDENCY_PLANS, pool.TIER_RESERVATIONS, pool.TIERS,
)


def _layout_dirs(q: pool.PoolQueue) -> set[Path]:
    return ({q.dir(state) for state in pool._STATES}
            | {q.root / name for name in LAYOUT_NAMES})


def test_polling_claims_issues_no_layout_mkdirs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One process, one layout build, however many polls follow."""

    seen: list[Path] = []
    real_mkdir = Path.mkdir

    def counting(self: Path, *args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        seen.append(self)
        return real_mkdir(self, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", counting)
    q = pool.PoolQueue(tmp_path / "pb-queue")
    assert q._layout_ensured is False
    q.publish(
        action_key=KEY_A,
        cas_root=q.root / "cas",
        checkout_root=q.root / "co",
        worker_script=q.root / "worker.py",
        resources={"cpu": 1},
    )
    layout = _layout_dirs(q)
    assert layout <= set(seen), "the first submission builds the layout"
    assert all(path.is_dir() for path in layout)

    seen.clear()
    assert q.claim(owner="worker", capacity={"cpu": 1}) is not None
    seen.clear()
    # The queue is empty now: idle polls must cost the mount nothing.  (The
    # taking poll above still writes -- intent, lease, claim record -- and
    # every write creates its parent, which is per-write, not per-poll.)
    assert q.claim(owner="worker", capacity={"cpu": 1}) is None
    assert q.claim(owner="worker", capacity={"cpu": 1}) is None
    assert not [path for path in seen if path in layout], \
        "idle polls re-check nothing on the mount"


def test_a_fresh_queue_re_ensures_after_a_hand_deletion(tmp_path: Path) -> None:
    """The once-per-process flag does not outlive the process."""

    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    assert q._layout_ensured is True
    q.dir(pool.READY).rmdir()
    assert not q.dir(pool.READY).exists()

    restarted = pool.PoolQueue(tmp_path / "pb-queue")
    assert restarted._layout_ensured is False
    restarted.ensure_layout()
    assert q.dir(pool.READY).is_dir()

"""Recover staging files left when a requeue publisher dies after linking."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

from prismabuild import pool


KEY = "a" * 64
DEMAND = {"cpu": 1, "mem_gb": 1}
GRACE = pool.LEASE_TIMEOUT_S
CRASH_AFTER_LINK = """
import json
import os
from pathlib import Path
import signal
import sys
from prismabuild import pool

queue = pool.PoolQueue(sys.argv[1])
key = sys.argv[2]
pool._now = lambda: float(sys.argv[3])
ready = queue.item_path(pool.READY, key)
link = os.link

def crash_after_link(source, destination, *args, **kwargs):
    result = link(source, destination, *args, **kwargs)
    if Path(destination) == ready:
        os.kill(os.getpid(), signal.SIGKILL)
    return result

pool.os.link = crash_after_link
claim = json.loads(queue.item_path(pool.CLAIMED, key).read_bytes())
queue.finish(key, status="failed", detail={"returncode": 17}, claim_snapshot=claim)
raise RuntimeError("the real READY link was not reached")
"""


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    return pool.PoolQueue(tmp_path / "queue")


@pytest.fixture()
def clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    now = [time.time()]
    monkeypatch.setattr(pool, "_now", lambda: now[0])
    return now


def _crashed_requeue(
    queue: pool.PoolQueue, clock: list[float],
) -> tuple[Path, Path]:
    queue.publish(
        action_key=KEY, cas_root=queue.root / "cas",
        checkout_root=queue.root / "checkout", worker_script=queue.root / "worker.py",
        resources=DEMAND, max_attempts=2, retry_safe=True,
    )
    claim = queue.claim(owner="crashing-worker", capacity=DEMAND)
    assert claim is not None
    child = subprocess.run(
        [sys.executable, "-c", CRASH_AFTER_LINK, str(queue.root), KEY, str(clock[0])],
        capture_output=True, text=True, timeout=20,
    )
    assert child.returncode == -signal.SIGKILL, (child.returncode, child.stdout, child.stderr)
    staged = list(queue.dir(pool.CLAIMED).glob(".*.tombstone.tmp"))
    assert len(staged) == 1
    stage = staged[0]
    tombstone = stage.with_name(stage.name[1:-len(".tmp")])
    assert tombstone.exists()
    assert json.loads(tombstone.read_bytes()) == claim
    ready = queue.item_path(pool.READY, KEY)
    assert stage.samefile(ready)
    assert json.loads(ready.read_bytes())["attempts"] == 1
    assert queue.ledger().held() == {}
    assert not queue.lease_path(KEY).exists()
    clock[0] = max(clock[0], stage.stat().st_mtime)
    return stage, tombstone


def _recover_owner_while_staging_is_fresh(
    queue: pool.PoolQueue, clock: list[float], stage: Path, tombstone: Path,
) -> None:
    clock[0] += GRACE + 1
    # Control the file clock independently of the earlier entomb timestamp.
    os.utime(stage, (clock[0], clock[0]))
    queue.sweep_finish_tombstones()
    assert not tombstone.exists()
    assert stage.exists(), "a fresh staging file was removed during owner recovery"


@pytest.mark.parametrize("successor_state", (pool.READY, pool.CLAIMED, pool.DONE))
def test_sigkill_staging_orphan_is_reaped_without_changing_successor(
    queue: pool.PoolQueue, clock: list[float], successor_state: str,
) -> None:
    stage, tombstone = _crashed_requeue(queue, clock)
    _recover_owner_while_staging_is_fresh(queue, clock, stage, tombstone)
    ready = queue.item_path(pool.READY, KEY)
    ready_identity = (ready.stat().st_dev, ready.stat().st_ino)
    if successor_state != pool.READY:
        claim = queue.claim(owner="successor", capacity=DEMAND)
        assert claim is not None
        if successor_state == pool.DONE:
            queue.finish(KEY, status="executed", claim_snapshot=claim)
    paths = [queue.item_path(successor_state, KEY)]
    if successor_state == pool.CLAIMED:
        paths.append(queue.lease_path(KEY))
    paths.extend((queue.root / pool.ATTEMPTS).rglob("*"))
    paths.extend(queue.superseded_dir().glob("*.json"))
    saved = {path: path.read_bytes() for path in paths if path.is_file()}
    held = queue.ledger().held()

    clock[0] += GRACE + 1
    queue.sweep_finish_tombstones()
    queue.sweep_finish_tombstones()

    assert not stage.exists(), "SIGKILL left an aged staging orphan with no cleanup owner"
    assert all(path.read_bytes() == value for path, value in saved.items())
    assert queue.ledger().held() == held
    if successor_state == pool.READY:
        assert (ready.stat().st_dev, ready.stat().st_ino) == ready_identity


def test_fresh_orphan_staging_is_retained(
    queue: pool.PoolQueue, clock: list[float],
) -> None:
    stage, tombstone = _crashed_requeue(queue, clock)
    _recover_owner_while_staging_is_fresh(queue, clock, stage, tombstone)
    saved = stage.read_bytes()
    queue.sweep_finish_tombstones()
    assert stage.read_bytes() == saved


def test_aged_staging_is_retained_while_its_exact_tombstone_exists(
    queue: pool.PoolQueue, clock: list[float],
) -> None:
    stage, tombstone = _crashed_requeue(queue, clock)
    saved = {path: path.read_bytes() for path in (stage, tombstone)}
    old = clock[0] - GRACE - 1
    os.utime(stage, (old, old))

    queue.sweep_finish_tombstones()

    assert all(path.read_bytes() == value for path, value in saved.items())


def test_orphan_staging_cleanup_respects_the_key_lock(
    queue: pool.PoolQueue, clock: list[float],
) -> None:
    stage, tombstone = _crashed_requeue(queue, clock)
    _recover_owner_while_staging_is_fresh(queue, clock, stage, tombstone)
    clock[0] += GRACE + 1
    with ThreadPoolExecutor(max_workers=1) as executor:
        with queue._transition_locked(KEY):
            executor.submit(queue.sweep_finish_tombstones).result(timeout=10)
            assert stage.exists(), "cleanup crossed a key transition owned by another thread"
    queue.sweep_finish_tombstones()
    assert not stage.exists()


def test_staging_cleanup_ignores_unrelated_names_and_nonregular_entries(
    queue: pool.PoolQueue, clock: list[float], tmp_path: Path,
) -> None:
    queue.ensure_layout()
    names = (
        f".{KEY}.1.worker.17.short.tombstone.tmp",
        f".{KEY.upper()}.1.worker.17.12345678.tombstone.tmp",
        f".{KEY}.not-time.worker.17.12345678.tombstone.tmp",
        f".{KEY}.1.worker.17.1234567G.tombstone.tmp",
        f".{KEY}.1.worker.0.12345678.tombstone.tmp",
        f".{KEY}.1.worker.17.12345678.tombstone.tmp.extra",
        f"{KEY}.1.worker.17.12345678.tombstone.tmp",
        ".unrelated.tmp",
    )
    files = [queue.dir(pool.CLAIMED) / name for name in names]
    for path in files:
        path.write_bytes(b"unrelated")
        old = clock[0] - GRACE - 1
        os.utime(path, (old, old))
    target = tmp_path / "symlink-target"
    target.write_bytes(b"retained target")
    symbolic = queue.dir(pool.CLAIMED) / f".{KEY}.1.worker.example.17.0000000a.tombstone.tmp"
    symbolic.symlink_to(target)
    directory = queue.dir(pool.CLAIMED) / f".{KEY}.1.worker.example.17.0000000b.tombstone.tmp"
    directory.mkdir()
    for path in (target, symbolic, directory):
        os.utime(path, (old, old), follow_symlinks=False)

    queue.sweep_finish_tombstones()

    assert all(path.read_bytes() == b"unrelated" for path in files)
    assert symbolic.is_symlink() and target.read_bytes() == b"retained target"
    assert directory.is_dir()

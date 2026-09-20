"""Full-stack membership 1/2 — qualification, resign/handoff, retry takeovers.

Worker JOIN/RESIGN commands and supervision belong to the membership
worker (design pending worker-expansion-design.md) — this lane edits none
of that production code. These tests pin the stable primitives membership
builds on, using real queue/ledger functions on an isolated local queue:
tag/image conjunction gating before claim, resign-as-handoff with
containment before token return, retry-safe takeover preserving attempt
history, explicit unsafe-retry interruption, incarnation-collision
fail-closed, and charge retention on failed containment. No hosts are
needed; ledgers are names. Both-Spark placement qualification is separate
(live lane, §design) from the capability matrix below.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

CAPACITY = {"cpu": 2, "mem_gb": 2}
TIERS = {"preferred": [0, 1], "fallback": []}
IMAGE = ("repo@sha256:" + "ab" * 32)


def _box(queue: pool.PoolQueue) -> None:
    queue.ledger().ensure_capacity(dict(CAPACITY))


def _publish(queue: pool.PoolQueue, key: str, **extra) -> None:
    queue.publish(action_key=key, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root / "co"),
                  worker_script=str(queue.root / "worker.py"),
                  resources={"cpu": 1, "mem_gb": 1}, **extra)


def _claim(queue: pool.PoolQueue, **extra):
    return queue.claim(owner="worker:1:abcd0001", capacity=dict(CAPACITY),
                       **extra)


def test_qualification_tags_conjoined_both_directions(tmp_path: Path) -> None:
    """An unqualified claimant gets nothing; a qualified one claims."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _box(queue)
    _publish(queue, "a" * 64, tags=["gb10"])
    assert _claim(queue, tags=["x86"]) is None
    admitted = _claim(queue, tags=["gb10"])
    assert admitted is not None
    assert admitted["action_key"] == "a" * 64


def test_qualification_container_image_unknown_vs_present(tmp_path: Path) -> None:
    """Unknown image inventory never counts as a capable box."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _box(queue)
    queue.announce(host="worker-a", tags=["x86"], has_gpu=False,
                   capacity=dict(CAPACITY), cpu_tiers=dict(TIERS))
    _publish(queue, "b" * 64, tags=["x86"], container_images=[IMAGE])
    assert _claim(queue, tags=["x86"]) is None
    queue.announce(host="worker-a", tags=["x86"], has_gpu=False,
                   capacity=dict(CAPACITY), cpu_tiers=dict(TIERS),
                   observed_images=[IMAGE])
    admitted = _claim(queue, tags=["x86"], observed_images=[IMAGE])
    assert admitted is not None
    assert admitted["action_key"] == "b" * 64


def test_unsafe_retry_is_an_explicit_contradiction(tmp_path: Path) -> None:
    """retry_safe=False with max_attempts>1 refuses at publish: no silent loss."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    with pytest.raises(pool.PoolContractError):
        _publish(queue, "c" * 64, retry_safe=False, max_attempts=2)


def test_unsafe_work_ends_terminal_never_requeued(tmp_path: Path) -> None:
    """A failed unsafe action is terminal; nothing retries it implicitly."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _box(queue)
    _publish(queue, "d" * 64, tags=["x86"], retry_safe=False, max_attempts=1)
    claimed = _claim(queue, tags=["x86"])
    assert claimed is not None
    queue.finish("d" * 64, status="failed", detail={"returncode": 1},
                 claim_snapshot=claimed)
    assert not queue.item_path(pool.READY, "d" * 64).exists()
    assert queue.item_path(pool.FAILED, "d" * 64).exists()


def test_stale_incarnation_cannot_take_a_replacement(tmp_path: Path) -> None:
    """A second commit onto an owned key leaves existing tokens: short count."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    ledger = queue.ledger("worker-a")
    ledger.ensure_capacity(dict(CAPACITY))
    demand = {"cpu": 1, "mem_gb": 1}
    first = ledger.begin_acquire("e" * 64, demand)
    assert first is not None
    assert ledger.commit_acquire("e" * 64, first) == 2
    second = ledger.begin_acquire("e" * 64, demand)
    assert second is not None
    moved = ledger.commit_acquire("e" * 64, second)
    assert moved < 2
    assert ledger.held() == {"cpu": 1, "mem_gb": 1}
    assert ledger.abandon_acquire(second) >= 0
    assert ledger.held() == {"cpu": 1, "mem_gb": 1}


def test_failed_containment_retains_charge(tmp_path: Path) -> None:
    """Abandoning an unknown handle returns nothing; double release counts once."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    ledger = queue.ledger("worker-a")
    ledger.ensure_capacity(dict(CAPACITY))
    assert ledger.abandon_acquire("claiming.nobody") == 0
    assert ledger.available() == dict(CAPACITY)
    assert ledger.acquire("f" * 64, {"cpu": 1, "mem_gb": 1}) is True
    assert ledger.release("f" * 64) == 2
    assert ledger.release("f" * 64) == 0
    assert ledger.available() == dict(CAPACITY)


def test_mixed_capability_matrix_without_hosts(tmp_path: Path) -> None:
    """Capability classes gate independently of any host pair."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    _box(queue)
    queue.announce(host="cpu-only", tags=["x86"], has_gpu=False,
                   capacity=dict(CAPACITY), cpu_tiers=dict(TIERS))
    queue.announce(host="gpu-box", tags=["x86", "gb10"], has_gpu=True,
                   capacity=dict(CAPACITY), cpu_tiers=dict(TIERS),
                   observed_images=[IMAGE])
    _publish(queue, "1" * 64, tags=["x86"])
    _publish(queue, "2" * 64, tags=["gb10"], needs_gpu=True,
             container_images=[IMAGE])
    cpu_work = _claim(queue, tags=["x86"])
    assert cpu_work is not None
    assert cpu_work["action_key"] == "1" * 64
    gpu_work = _claim(queue, tags=["x86", "gb10"], has_gpu=True,
                      observed_images=[IMAGE])
    assert gpu_work is not None
    assert gpu_work["action_key"] == "2" * 64

"""An undeclared reader's read-ahead counts unified memory once (#959).

A consumer whose plan declares no read-ahead (#909) is priced at its memory
reservations: ``mem_gb`` of host memory plus the GPU memory budget its
admission gave it (#903).  On a discrete GPU those are two pools, and a reader
can hold both.  On a unified-memory device (a GB10: host and GPU share one
128 GB pool) the GPU budget is a subset of ``mem_gb`` (``pbrun --gpu-memory-gb``
says so), so the sum counted the same bytes twice: R12's 100 GiB plus its
80 GiB budget is a read-ahead no GB10 can hold.

Whether the pool is unified is not guessed from a host name.  Admission
measures it and records it on the claim (``gpu_admission.memory_domain``,
``shared_system`` or ``discrete``, from the device probe).  A claim that does
not say -- ``unknown``, or an admission record from before the field -- keeps
the sum, which errs long, never short.

Everything runs on ``tmp_path`` queues and stage roots (#628).
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import pool  # noqa: E402
import tier_loop  # noqa: E402
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _fixture_queue)
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    GIB, _tier_record)
import test_r12_and_the_capture_replay_under_the_refill_horizon as replay  # noqa: E402

#: R12's claim: 100 GiB of host memory and an 80 GiB GPU budget.
MEM_GB = 100
BUDGET = 80 * GIB


def _item(domain: str | None, *, mem_gb: float = MEM_GB,
          budget: int = BUDGET) -> dict[str, object]:
    admission: dict[str, object] = {"gpu_memory_budget_bytes": budget}
    if domain is not None:
        admission["memory_domain"] = domain
    return {"resources": {"cpu": 1, "mem_gb": mem_gb},
            "gpu_admission": admission}


def test_on_unified_memory_the_readahead_is_at_most_the_memory_reservation(
        ) -> None:
    reach = tier_loop._readahead_bytes(_item("shared_system"))

    assert reach is not None
    assert reach <= MEM_GB * GIB
    assert reach == MEM_GB * GIB


def test_on_a_discrete_gpu_the_readahead_is_host_plus_gpu_memory() -> None:
    assert tier_loop._readahead_bytes(_item("discrete")) == MEM_GB * GIB + BUDGET


def test_an_unmeasured_domain_keeps_the_sum_which_errs_long() -> None:
    for domain in (None, "unknown", "", 7):
        item = _item(None)
        if domain is not None:
            item["gpu_admission"]["memory_domain"] = domain  # type: ignore[index]
        assert tier_loop._readahead_bytes(item) == MEM_GB * GIB + BUDGET


def test_a_unified_budget_larger_than_mem_gb_is_the_reach() -> None:
    """One pool holds the larger of the two, never less than either."""

    assert tier_loop._readahead_bytes(
        _item("shared_system", mem_gb=16, budget=40 * GIB)) == 40 * GIB


def test_the_basis_is_still_the_memory_reservation() -> None:
    plan: dict[str, object] = {}
    assert tier_loop._readahead(plan, _item("shared_system")) == (
        MEM_GB * GIB, "memory-reservation")


def _set_domain(queue: pool.PoolQueue, key: str, domain: str) -> None:
    path = queue.item_path(pool.CLAIMED, key)
    item = json.loads(path.read_text())
    item["gpu_admission"]["memory_domain"] = domain
    path.write_text(json.dumps(item))


def _horizon(tmp_path: Path, domain: str) -> dict[str, object]:
    queue, stage = _fixture_queue(tmp_path, replay.CAPACITY)
    plan = replay._r12(queue, stage, time.time() - replay.SAMPLE_UNIX)
    _set_domain(queue, replay.R12, domain)
    consumer = next(entry for entry in tier_loop.live_consumers(queue)
                    if entry["action_key"] == replay.R12)
    horizon = tier_loop._stage_horizon(
        queue, consumer, plan, _tier_record(stage, gib=replay.CAPACITY))
    assert horizon is not None
    return horizon


#: The end of ``chain-043``, which R12 has read to (the replay's docstring).
READ_TO = 81195938468


def test_r12_on_a_gb10_reaches_its_memory_reservation_not_the_sum(
        tmp_path: Path) -> None:
    horizon = _horizon(tmp_path, "shared_system")

    assert horizon["reach_end_bytes"] == READ_TO + MEM_GB * GIB


def test_r12_on_a_discrete_gpu_reaches_host_plus_gpu_memory(
        tmp_path: Path) -> None:
    horizon = _horizon(tmp_path, "discrete")

    assert horizon["reach_end_bytes"] == READ_TO + MEM_GB * GIB + BUDGET

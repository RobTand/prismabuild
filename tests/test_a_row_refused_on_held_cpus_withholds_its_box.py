"""A row refused ``host_pressure`` on CPUs pool holders hold withholds (#1160).

The 2026-09-25 incident: GLM-5.3 Stage B row 010 (``{cpu: 9, gpu: 1}``) was
refused ``adaptive_cpu_refused`` / ``host_pressure`` on sparklina, whose GPU
was idle, because the CPUs its free tokens map to were held by pool holders:
2-CPU pbtest shards.  ``adaptive_cpu.decision`` counts a predicted CPU as
busy when a holder's ``cpu_allocation`` names it.  The drain mapping gave that
refusal no mode, so the row never withheld its box, and each CPU-only shard
that finished was replaced by the next one in the stream.

A predicted CPU is held while its token is free when a holder borrowed it from
another holder's idle reservation and that lender has since released: the
borrower is still pinned there, and its allocation still names it.  The
fixture below writes that borrowed allocation into one real, claimed shard's
metadata, as ``test_adaptive_cpu.py`` does for the disjoint-CPU proof.

Draining the holders on those CPUs is exactly what clears the refusal, so it
now withholds while those holders drain soon.  Busy CPUs no holder holds are
load the pool does not own, which no drain clears, so they still do not.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src")]
from prismabuild import adaptive_cpu, core as pb, pool  # noqa: E402

T0 = 3_000_000.0
CPUS = 20
TIERS = {"preferred": list(range(CPUS)), "fallback": []}
CAPACITY = {"cpu": CPUS, "gpu": 1, "mem_gb": 120}

#: The deadline ``pbtest --timeout-s 1800`` seals on each shard: a transient
#: holder by :meth:`PoolQueue.holder_bound`, as the incident's shards were.
SHARD_TIMEOUT_S = 1800


def _key(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


@pytest.fixture()
def clock(monkeypatch):
    now = [T0]
    monkeypatch.setattr(pool, "_now", lambda: now[0])
    monkeypatch.setattr(adaptive_cpu.time, "time", lambda: now[0])
    return now


@pytest.fixture(autouse=True)
def generation_actions(monkeypatch):
    monkeypatch.setattr(adaptive_cpu, "action_identity", lambda item: ("shape", False))


@pytest.fixture()
def host(clock, monkeypatch):
    """The host sample every decision reads; tests change it in place."""

    state: dict[str, object] = {"psi_some": 0., "busy": {}}

    def sample(self):
        busy = {cpu: float(state["busy"].get(cpu, 0.)) for cpu in range(CPUS)}  # type: ignore[union-attr]
        return {"sampled_unix": clock[0], "cpu_count": CPUS, "interval_s": 1.,
                "psi_some": state["psi_some"], "busy_cpus": sum(busy.values()),
                "per_cpu_busy": {str(cpu): value for cpu, value in busy.items()}}

    monkeypatch.setattr(adaptive_cpu.Controller, "sample", sample)
    return state


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, clock, key: str, resources, **kw) -> str:
    clock[0] += 0.001
    q.publish(action_key=key, cas_root=kw.pop("cas_root", "/cas"),
              checkout_root=kw.pop("checkout_root", "/co"),
              worker_script="/w.py", resources=resources, **kw)
    return key


def _sealed(tmp_path: Path, name: str, *, timeout_s: int):
    checkout = tmp_path / f"checkout-{name}"
    checkout.mkdir()
    (checkout / "task.py").write_text("print('fixture')\n")
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": f"tests/{name}", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {"execution_timeout_s": timeout_s},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    return action["action_key"], str(cas.root), str(checkout)


def _denial(q: pool.PoolQueue, key: str) -> dict:
    path = adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(value for value in records.values() if value["action_key"] == key)


def _claim(q: pool.PoolQueue) -> str | None:
    item = q.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True)
    return None if item is None else item["action_key"]


def _box_with_shards(queue, clock, host, tmp_path: Path):
    """Three claimed 2-CPU shards, sealed with pbtest's 1800 s deadline."""

    shards = []
    for n in range(3):
        key, cas_root, checkout = _sealed(tmp_path, f"shard-{n}", timeout_s=SHARD_TIMEOUT_S)
        _publish(queue, clock, key, {"cpu": 2, "mem_gb": 5},
                 cas_root=cas_root, checkout_root=checkout)
        assert _claim(queue) == key
        shards.append(key)
    return shards


def _borrow(queue: pool.PoolQueue, holder: str, cpus: list[int]) -> None:
    """Record ``cpus`` in ``holder``'s allocation: a borrow whose lender left."""

    ledger = queue.ledger()
    path = ledger.held_dir / holder / adaptive_cpu.METADATA
    metadata = json.loads(path.read_text())
    allocation = metadata["allocation"]
    allocation["preferred"] = sorted(set(allocation["preferred"]) | set(cpus))
    metadata["allocation"] = allocation
    metadata["borrowed_cpu"] = len(cpus)
    path.write_text(json.dumps(metadata))


def test_a_gpu_row_refused_on_held_cpus_withholds_until_they_drain(
    queue: pool.PoolQueue, clock, host, tmp_path: Path,
) -> None:
    """#1160's acceptance fixture: an idle GPU behind CPU-only shards.

    The GPU row's first denial is on a box whose GPU token is free, so it is
    eligible at once (``gpu_first``).  The refusal names only held CPUs, and
    the shard holding them is transient, so the row withholds the box: the
    CPU-only row published after it is not admitted.  When that shard
    finishes, the row claims.
    """

    shards = _box_with_shards(queue, clock, host, tmp_path)
    predicted = queue.ledger().free_cpu_allocation(9, TIERS)
    assert predicted is not None
    # The row's last two CPUs, not the CPU-only row's first two: that row's
    # own CPUs are idle and unheld, so only a withhold keeps it off the box.
    borrowed = predicted[-2:]
    _borrow(queue, shards[-1], borrowed)

    host["psi_some"] = .2
    gpu_row = _publish(queue, clock, _key("stage-b-row"), {"cpu": 9, "gpu": 1, "mem_gb": 96})
    cpu_row = _publish(queue, clock, _key("next-shard"), {"cpu": 2, "mem_gb": 5})

    assert _claim(queue) is None, (
        "a CPU-only row overtook a GPU row refused only on CPUs pool holders hold")
    denial = _denial(queue, gpu_row)
    assert denial["reason"] == "adaptive_cpu_refused_withholding"
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "host_pressure"
    assert decision["held_cpus"] == sorted(borrowed)
    assert decision["foreign_cpus"] == []
    verdict = denial["evidence"]["withhold"]
    assert verdict["mode"] == "held_cpus"
    assert verdict["why"] == "drains_soon"
    assert verdict["gpu_first"] is True
    assert queue.passes(gpu_row) == 1
    assert queue.item_path(pool.READY, cpu_row).exists()
    assert queue.holder_bound(shards[-1])["bound"] == "transient"

    # While the shard on its CPUs runs, the row keeps withholding.
    clock[0] += 30
    assert _claim(queue) is None
    assert _denial(queue, gpu_row)["reason"] == "adaptive_cpu_refused_withholding"
    assert queue.item_path(pool.READY, cpu_row).exists()

    # The shard on the row's CPUs drains, and the row claims.
    clock[0] += 30
    queue.finish(shards[-1], status="executed")
    assert _claim(queue) == gpu_row


def test_a_held_cpu_holder_that_does_not_drain_soon_is_not_withheld_for(
    queue: pool.PoolQueue, clock, host, tmp_path: Path,
) -> None:
    """``WITHHOLD_CEILING_S`` per holder (#924) still bounds the new drain.

    The shard on the row's CPUs is past the pool's transient line with its
    declared end a day off, so the row keeps its place, is denied starved and
    names that holder, and the CPU-only row behind it is admitted.
    """

    key, cas_root, checkout = _sealed(tmp_path, "long-shard", timeout_s=86_400)
    clock[0] = T0 - 2 * pool.WITHHOLD_CEILING_S
    _publish(queue, clock, key, {"cpu": 2, "mem_gb": 5},
             cas_root=cas_root, checkout_root=checkout)
    assert _claim(queue) == key
    clock[0] = T0
    predicted = queue.ledger().free_cpu_allocation(9, TIERS)
    assert predicted is not None
    _borrow(queue, key, predicted[-1:])

    host["psi_some"] = .2
    gpu_row = _publish(queue, clock, _key("stage-b-row"), {"cpu": 9, "gpu": 1, "mem_gb": 96})
    cpu_row = _publish(queue, clock, _key("next-shard"), {"cpu": 2, "mem_gb": 5})

    assert _claim(queue) == cpu_row
    denial = _denial(queue, gpu_row)
    assert denial["reason"] == "adaptive_cpu_refused_starved"
    assert denial["evidence"]["starved"]["why"] == "holder_does_not_drain_soon"
    [named] = denial["evidence"]["starved"]["holders"]
    assert named["action_key"] == key[:12] and named["bound"] == "long"


@pytest.mark.parametrize("held", [False, True], ids=["foreign-only", "held-and-foreign"])
def test_busy_cpus_no_pool_holder_holds_do_not_withhold(
    queue: pool.PoolQueue, clock, host, tmp_path: Path, held: bool,
) -> None:
    """Load the pool does not own is not cleared by any drain.

    The row's last predicted CPU is busy and no holder holds it: the refusal
    records it as foreign, the row does not withhold, and the CPU-only row
    behind it (whose own CPUs are idle and unheld) is admitted.  A refusal
    that also names held CPUs is the same: draining the holders would still
    leave the foreign CPU busy.
    """

    shards = _box_with_shards(queue, clock, host, tmp_path)
    predicted = queue.ledger().free_cpu_allocation(9, TIERS)
    assert predicted is not None
    foreign = predicted[-1]
    host["busy"] = {foreign: 1.}
    if held:
        _borrow(queue, shards[-1], [predicted[-2]])

    host["psi_some"] = .2
    gpu_row = _publish(queue, clock, _key("stage-b-row"), {"cpu": 9, "gpu": 1, "mem_gb": 96})
    cpu_row = _publish(queue, clock, _key("next-shard"), {"cpu": 2, "mem_gb": 5})

    assert _claim(queue) == cpu_row, "a row withheld its box for load no drain clears"
    denial = _denial(queue, gpu_row)
    assert denial["reason"] == "adaptive_cpu_refused"
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "host_pressure"
    assert decision["foreign_cpus"] == [foreign]
    assert decision["held_cpus"] == ([predicted[-2]] if held else [])
    assert "withhold" not in denial["evidence"]


@pytest.mark.parametrize("decision,expected", [
    ({"reason": "host_pressure"}, (None, False)),
    ({"reason": "host_pressure", "held_cpus": [3], "foreign_cpus": []}, ("held_cpus", False)),
    ({"reason": "host_pressure", "held_cpus": [], "foreign_cpus": [3]}, (None, False)),
    ({"reason": "host_pressure", "held_cpus": [2], "foreign_cpus": [3]}, (None, False)),
    ({"reason": "host_pressure", "held_cpus": [], "foreign_cpus": []}, (None, False)),
], ids=["pre-1160-record", "held-only", "foreign-only", "held-and-foreign", "neither"])
def test_the_drain_mapping_reads_held_and_foreign_cpus(decision, expected) -> None:
    assert pool._adaptive_refusal_drains(
        "adaptive_cpu_refused", decision, demand={"cpu": 9, "gpu": 1},
        measurement=False, cpu_count=CPUS) == expected


def test_an_exclusive_host_pressure_need_keeps_the_exclusive_drain() -> None:
    """A measurement's host_pressure still needs the host quiet, not its CPUs."""

    decision = {"reason": "host_pressure", "held_cpus": [3], "foreign_cpus": []}
    assert pool._adaptive_refusal_drains(
        "adaptive_cpu_refused", decision, demand={"cpu": 2},
        measurement=True, cpu_count=CPUS) == ("exclusive", False)

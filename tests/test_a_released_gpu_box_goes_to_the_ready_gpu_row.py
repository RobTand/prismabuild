"""A CPU-only row does not take the room a ready GPU row needs on a free GPU (#1169).

The 2026-09-25 incident, all times UTC.  sparky offers
``{cpu: 20, gpu: 1, mem_gb: 104}``.  GLM-5.3 Stage B row 009 held the GPU and
97 GB.  Row 001 (``5c25c72680ca``, sealed ``mem_gb: 96``, charged 97 with its
producer's export allowance) was READY behind it, and so was a CPU-only pbtest
shard (``57d92765f065``, ``{cpu: 2, mem_gb: 8}``), both at priority -10.
Row 009 released at 14:34:10.4.  At 14:34:11.55 the shard claimed sparky,
and 80 ms later row 001 was refused ``reservation_unavailable_withholding``:
97 + 8 GB is more than 104.  The withhold came one claim too late, and a
withhold never evicts a claimed action, so the GPU idled until the shard was
withdrawn by hand at 14:38.

The shard was ahead of the row in the ready order: the order within a band is
the denial count, and the shard had been denied on every scan since 14:16
while the row's residency lead was not yet resident, which counts no pass.  A
withhold holds back only the rows behind the row that withholds, and only
after that row's own refusal.  A second route reaches the same claim: a scan
that meets the GPU row's transition lock held (``transition_busy``) carries a
withhold only when one is on file, and none was.

The rule now, while the box's GPU token is free: a ready GPU row that is
placeable here and fits the free tokens is scanned first in its band, so its
own evaluation decides before a CPU-only row can take its room; and when a
scan cannot evaluate it because another loop holds its lock, a row behind it
that demands no GPU is admitted only if the GPU row still fits afterwards.
CPU work that fits beside it is still admitted, and nothing is evicted.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src")]
from prismabuild import adaptive_cpu, core as pb, pool  # noqa: E402

T0 = 4_000_000.0
#: sparky's offer, 2026-09-25.
CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 104}
TIERS = {"preferred": list(range(20)), "fallback": []}
#: Row 009: a progress-governed Stage B row with no total timeout, which
#: ``holder_bound`` reads as ``unbounded``.  Row 001 before the fix: the row
#: waiting for the GPU is denied starved behind it, and withholds nothing.
HOLDER = {"cpu": 9, "gpu": 1, "mem_gb": 97}
#: Row 001 as PB charges it.  The box's offer less this is a 7 GB margin.
GPU_ROW = {"cpu": 10, "gpu": 1, "mem_gb": 97}
#: The shard: a CPU-only demand larger than the margin.
SHARD = {"cpu": 2, "mem_gb": 8}
#: A CPU-only demand that fits beside the GPU row.
SMALL = {"cpu": 2, "mem_gb": 5}
BAND = -10


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
def idle_host(clock, monkeypatch):
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": clock[0], "cpu_count": 20, "interval_s": 1.,
        "psi_some": 0., "busy_cpus": 0.})


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _sealed(tmp_path: Path, name: str, *, progress: bool = False,
            variables: dict[str, str] | None = None):
    checkout = tmp_path / f"checkout-{name}"
    checkout.mkdir()
    (checkout / "task.py").write_text("print('fixture')\n")
    params: dict[str, object] = {}
    if progress:
        params[pb.PROGRESS_PARAM] = {
            "schema": pb.PROGRESS_POLICY_SCHEMA_V1,
            "phases": [{"name": "run", "grace_s": 600}]}
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": f"tests/{name}", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": params,
        "environment": {"variables": dict(variables or {}), "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    return action["action_key"], str(cas.root), str(checkout)


def _publish(q: pool.PoolQueue, clock, key: str, resources, **kw) -> str:
    # A distinct publish time per row, so the ready order within a band is
    # the order this file publishes in, then the denial count.
    clock[0] += 0.001
    q.publish(action_key=key, cas_root=kw.pop("cas_root", "/cas"),
              checkout_root=kw.pop("checkout_root", "/co"),
              worker_script="/w.py", resources=resources,
              priority=kw.pop("priority", BAND), **kw)
    return key


def _denial(q: pool.PoolQueue, key: str) -> dict:
    path = adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(value for value in records.values() if value["action_key"] == key)


def _claimer(q: pool.PoolQueue, *, adaptive: bool):
    def claim() -> str | None:
        item = (q.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True)
                if adaptive else q.claim(capacity=CAPACITY))
        return None if item is None else str(item["action_key"])
    return claim


def _holder(q: pool.PoolQueue, clock, tmp_path: Path, claim) -> str:
    """Row 009: progress-governed, unbounded, on the GPU and 97 GB."""

    key, cas_root, checkout = _sealed(tmp_path, "row-009", progress=True)
    clock[0] = T0 - 7200
    _publish(q, clock, key, HOLDER, cas_root=cas_root, checkout_root=checkout)
    assert claim() == key
    clock[0] = T0
    return key


@contextmanager
def _lock_busy(q: pool.PoolQueue, monkeypatch, busy_key: str):
    """Another loop holds ``busy_key``'s transition lock for these passes."""

    real = q._timed_transition_hold

    @contextmanager
    def hold(action_key, holds):
        if action_key == busy_key:
            yield False
            return
        with real(action_key, holds) as acquired:
            yield acquired

    with monkeypatch.context() as patch:
        patch.setattr(q, "_timed_transition_hold", hold)
        yield


# -- the incident -------------------------------------------------------------


@pytest.mark.parametrize("order", ["published-first", "out-aged"])
@pytest.mark.parametrize("adaptive", [False, True], ids=["ledger", "adaptive_cpu"])
def test_a_released_gpu_goes_to_the_ready_gpu_row_not_the_cpu_row_ahead_of_it(
    queue: pool.PoolQueue, clock, idle_host, tmp_path: Path, adaptive: bool, order: str,
) -> None:
    """#1169's acceptance fixture: the shard is ahead of the row in the ready order.

    ``published-first`` is the incident's publish order.  ``out-aged`` puts
    the row first and gives the shard the denial count it had built up while
    the row's residency lead was not yet resident.
    """

    claim = _claimer(queue, adaptive=adaptive)
    holder = _holder(queue, clock, tmp_path, claim)
    if order == "published-first":
        shard = _publish(queue, clock, _key("shard"), SHARD)
        gpu_row = _publish(queue, clock, _key("row-001"), GPU_ROW)
    else:
        gpu_row = _publish(queue, clock, _key("row-001"), GPU_ROW)
        shard = _publish(queue, clock, _key("shard"), SHARD)
        for _ in range(pool.STARVATION_FLOOR + 2):
            queue.record_pass(shard)

    # While row 009 holds the GPU and 97 GB, neither fits.
    for _ in range(pool.STARVATION_FLOOR):
        clock[0] += 5
        assert claim() is None
    assert [str(item["action_key"]) for item in queue.ready_items()] == [shard, gpu_row]

    # Row 009 ends; its tokens come back.
    clock[0] += 5
    queue.finish(holder, status="executed")
    assert claim() == gpu_row, (
        "a CPU-only row took the room the ready GPU row needs on the released GPU")
    assert queue.item_path(pool.READY, shard).exists()
    # The shard does not fit beside the row, and nothing is evicted.
    assert claim() is None
    assert queue.item_path(pool.CLAIMED, gpu_row).exists()


@pytest.mark.parametrize("adaptive", [False, True], ids=["ledger", "adaptive_cpu"])
def test_a_gpu_row_whose_lock_is_busy_still_keeps_its_room(
    queue: pool.PoolQueue, clock, idle_host, tmp_path: Path, monkeypatch, adaptive: bool,
) -> None:
    """The second route: the row is first, but another loop is deciding it.

    The scan cannot evaluate the row, and before the release this host's last
    word on it was ``_starved``, so no withhold is on file to carry.
    """

    claim = _claimer(queue, adaptive=adaptive)
    holder = _holder(queue, clock, tmp_path, claim)
    gpu_row = _publish(queue, clock, _key("row-001"), GPU_ROW)
    shard = _publish(queue, clock, _key("shard"), SHARD)
    for _ in range(pool.STARVATION_FLOOR):
        clock[0] += 5
        assert claim() is None
    assert _denial(queue, gpu_row)["reason"].endswith("_starved")

    clock[0] += 5
    queue.finish(holder, status="executed")
    with _lock_busy(queue, monkeypatch, gpu_row):
        assert claim() is None, (
            "a CPU-only row took the released GPU's room while the ready GPU "
            "row's lock was busy")
    assert queue.item_path(pool.READY, shard).exists()
    denial = _denial(queue, shard)
    assert denial["reason"] == "deferred_for_ready_gpu_row"
    assert denial["evidence"]["gpu_row"] == gpu_row[:12]
    assert denial["evidence"]["room"] == GPU_ROW
    assert queue.passes(shard) == pool.STARVATION_FLOOR, "a deferral counted a pass"
    busy = _denial(queue, gpu_row)
    assert busy["reason"] == "transition_busy"
    assert busy["evidence"]["gpu_room_kept"] == GPU_ROW
    assert claim() == gpu_row


def test_the_room_is_the_demand_the_claim_charges_with_its_export_allowance(
    queue: pool.PoolQueue, clock, idle_host, tmp_path: Path, monkeypatch,
) -> None:
    """Row 001 was sealed ``mem_gb: 96`` and charged 97.

    The export allowance a producer's sealed environment declares (#985) is
    part of what its claim takes.  Against the sealed 96 GB the 8 GB shard
    would fit beside it, 96 + 8 = 104; against the charged 97 it does not.
    The row's lock is busy for the first pass after the release, so the room
    it keeps, not its own evaluation, is what stops the shard.
    """

    claim = _claimer(queue, adaptive=True)
    holder = _holder(queue, clock, tmp_path, claim)
    shard = _publish(queue, clock, _key("shard"), SHARD)
    key, cas_root, checkout = _sealed(tmp_path, "row-001", variables={
        adaptive_cpu.SPOOL_ROOT_ENV: str(tmp_path / "spool"),
        adaptive_cpu.EXPORT_SLOTS_ENV: "1"})
    gpu_row = _publish(queue, clock, key, {"cpu": 9, "gpu": 1, "mem_gb": 96},
                       cas_root=cas_root, checkout_root=checkout)
    # Fixture concession: the produced-output reference ``publish`` writes
    # for a Stage B row, which is what makes the claim ask for its allowance.
    path = queue.item_path(pool.READY, gpu_row)
    record = json.loads(path.read_text())
    record["produced_output"] = {"schema": "prismabuild.produced_output_ref.v1",
                                 "template_id": "fixture", "template_sha256": "0" * 64}
    path.write_text(json.dumps(record))

    clock[0] += 5
    queue.finish(holder, status="executed")
    with _lock_busy(queue, monkeypatch, gpu_row):
        assert claim() is None, (
            "the room was read from the sealed demand, not the charged one")
    assert _denial(queue, shard)["evidence"]["room"]["mem_gb"] == 97
    assert claim() == gpu_row
    assert queue.ledger().holder_tokens(gpu_row).get("mem_gb") == 97
    assert queue.item_path(pool.READY, shard).exists()


# -- CPU fill is kept --------------------------------------------------------


@pytest.mark.parametrize("busy", [False, True], ids=["ahead-in-order", "row-lock-busy"])
@pytest.mark.parametrize("adaptive", [False, True], ids=["ledger", "adaptive_cpu"])
def test_cpu_work_that_fits_beside_the_gpu_row_is_still_admitted(
    queue: pool.PoolQueue, clock, idle_host, tmp_path: Path, monkeypatch,
    adaptive: bool, busy: bool,
) -> None:
    """A 5 GB shard leaves the row its 97 GB, so it still claims.

    Ahead of the row in the ready order, the shard claims on the pass after
    the row's.  Behind a row whose lock is busy, it claims on the same pass.
    """

    claim = _claimer(queue, adaptive=adaptive)
    holder = _holder(queue, clock, tmp_path, claim)
    if busy:
        gpu_row = _publish(queue, clock, _key("row-001"), GPU_ROW)
        small = _publish(queue, clock, _key("small"), SMALL)
    else:
        small = _publish(queue, clock, _key("small"), SMALL)
        gpu_row = _publish(queue, clock, _key("row-001"), GPU_ROW)
    clock[0] += 5
    queue.finish(holder, status="executed")
    if busy:
        with _lock_busy(queue, monkeypatch, gpu_row):
            assert claim() == small
        assert claim() == gpu_row
    else:
        assert claim() == gpu_row
        assert claim() == small


# -- nothing is held back for a row that cannot take the GPU ------------------


@pytest.mark.parametrize("case", [
    "gpu-held", "lower-band", "never-fits", "not-placeable", "not-resident",
    "not-resident-lock-busy", "does-not-fit-free-tokens",
])
def test_cpu_work_is_not_held_for_a_gpu_row_that_cannot_take_the_box(
    queue: pool.PoolQueue, clock, idle_host, tmp_path: Path, monkeypatch, case: str,
) -> None:
    """The rule holds back CPU work only for a row that fits the box now.

    ``gpu-held``: the GPU is still held, and CPU work fills the box beside
    its holder as before (the 2026-09-04 constraint).  ``lower-band``: aging
    never crosses a band, and neither does this.  ``never-fits`` and
    ``not-placeable``: the box can never run the row.  ``not-resident``: the
    row's bytes are not there, so the box runs other work meanwhile (#583),
    whether the scan evaluates the row or meets its lock busy.
    ``does-not-fit-free-tokens``: a CPU holder the pool owns takes the room,
    and the row's own withhold, not this rule, decides that wait.
    """

    capacity = dict(CAPACITY, mem_gb=120)
    claim = lambda: (lambda item: None if item is None else item["action_key"])(  # noqa: E731
        queue.claim(capacity=capacity, tags=["gb10"]))
    if case == "gpu-held":
        holder_key, cas_root, checkout = _sealed(tmp_path, "gpu-holder", progress=True)
        _publish(queue, clock, holder_key, {"cpu": 2, "gpu": 1, "mem_gb": 10},
                 cas_root=cas_root, checkout_root=checkout)
        assert claim() == holder_key
    if case == "does-not-fit-free-tokens":
        cpu_holder = _publish(queue, clock, _key("cpu-holder"), {"cpu": 2, "mem_gb": 20})
        assert claim() == cpu_holder
    shard = _publish(queue, clock, _key("shard"), SHARD,
                     priority=0 if case == "lower-band" else BAND)
    row: dict[str, object] = {}
    demand = dict(GPU_ROW, mem_gb=110)
    if case == "never-fits":
        demand = dict(GPU_ROW, mem_gb=121)
    if case == "not-placeable":
        row["tags"] = ["sparklina-only"]
    if case.startswith("not-resident"):
        row["residency"] = {"schema": "prismabuild.residency.v1",
                            "leads": [_key("mover")], "manifest_sha256": "0" * 64,
                            "manifest_bytes": 1}
    gpu_row = _publish(queue, clock, _key("gpu-row"), demand, **row)
    with (_lock_busy(queue, monkeypatch, gpu_row) if case.endswith("lock-busy")
          else monkeypatch.context()):
        assert claim() == shard, (
            f"CPU work was held back for a GPU row that cannot take the box ({case})")

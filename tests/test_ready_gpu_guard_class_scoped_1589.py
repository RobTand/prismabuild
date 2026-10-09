"""The READY-GPU guard lets class-scoped CPU work through, beside the GPU row (#1589).

#1526: a GPU host leaves portable CPU-only rows READY while an eligible GPU row waits.  That has no
timer, and it deferred a genuine arm64 CPU qualification row (tagged for the GPU hosts' class, so no
host without a GPU can run it) behind every ready GPU row on both Sparks, for ever.  Now a row whose
required tags name something no host without a GPU offers is let past the guard when it fits beside
the eligible GPU row's own room against the free tokens at the token boundary; portable work still waits.

The class is read from every offer on file, stale ones included, so a brief x86 outage does not turn
portable work into class-scoped work.
"""
from pathlib import Path
import hashlib
import json
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_core import _body  # noqa: E402

from prismabuild import adaptive_cpu, adaptive_gpu, core as pb, pool

CAPACITY = {"cpu": 8, "gpu": 1, "mem_gb": 32}
TIERS = {"preferred": list(range(8)), "fallback": []}
CLASS_KEY = "c" * 64
PORTABLE_KEY = "d" * 64
GPU_KEY = "a" * 64


@pytest.fixture(params=["sparky", "sparklina"])
def host(request):
    return request.param


def _fleet(tmp_path, monkeypatch, host, *, cpu_host: str):
    """A Spark worker plus, as ``cpu_host`` says, a fresh, a stale or no x86 offer on file."""
    monkeypatch.setattr(pool.socket, "gethostname", lambda: host)
    queue = pool.PoolQueue(tmp_path / "queue")
    worker_tags = [host, "gb10", pb.INTERPRETER_TAG, pb.CONTAINER_IMAGE_TAG]
    queue.announce(host=host, tags=worker_tags, has_gpu=True, capacity=CAPACITY,
                   observed_capacity=CAPACITY, interpreters=[sys.executable], observed_images=[])
    if cpu_host != "none":
        queue.announce(host="dl380g10", tags=["dl380g10", "x86", pb.INTERPRETER_TAG, pb.CONTAINER_IMAGE_TAG],
                       has_gpu=False, capacity={"cpu": 40, "mem_gb": 200},
                       observed_capacity={"cpu": 40, "mem_gb": 200},
                       interpreters=[sys.executable], observed_images=[])
        if cpu_host == "stale":
            path = queue.root / pool.WORKERS / "dl380g10.json"
            record = json.loads(path.read_text())
            record["announced_unix"] = time.time() - 3600
            path.write_text(json.dumps(record))
    (tmp_path / "task_code.py").write_text("# closure member\n")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")

    def seal(*, task_class="generation", resources, gpu=False):
        body = _body(tmp_path, task_class=task_class,
                     **({"portability": "platform_keyed", "platform_key": "linux-aarch64-sm121"}
                         if task_class == "measurement" else {}))
        params: dict[str, object] = {"demand": dict(resources)}
        if gpu:
            params.update({"gpu_exclusive": False, "gpu_memory_gb": 8})
        body["params"] = params
        action = pb.seal_action(body)
        cas.publish_action_request(action)
        return str(action["action_key"])

    def publish(key, *, gpu=False, tags, resources, priority=0, **fields):
        task_class = fields.pop("task_class", "generation")
        sealed = seal(task_class=task_class, resources=resources, gpu=gpu)
        queue.publish(action_key=sealed, cas_root=str(cas.root), checkout_root=str(tmp_path),
                      worker_script="worker.py", tags=tags, needs_gpu=gpu, priority=priority,
                      resources=resources, **fields)
        return sealed

    def claim():
        return queue.claim(tags=worker_tags, has_gpu=True, capacity=CAPACITY, cpu_tiers=TIERS)

    gpu_sealed = publish(GPU_KEY, gpu=True, tags=["gb10"],
                         resources={"cpu": 2, "gpu": 1, "mem_gb": 8})
    keys = {GPU_KEY: gpu_sealed}
    return queue, publish, claim, keys, seal


# --- review 4 of PR 1590: aggregate adaptive headroom, under the real controllers --------

T0 = 2_000_000.0


def _key(seed):
    return hashlib.sha256(seed.encode()).hexdigest()


def _adaptive_fleet(tmp_path, monkeypatch, host, *, cpu=8, mem_gb=32):
    """A Spark-shaped box with both adaptive controllers live, like production.

    The worker loop claims with ``adaptive_cpu=True``; ``has_gpu=True`` builds
    the GPU controller.  The samples are scripted (fresh and idle), the telemetry
    paths are the real local ones, and every row is sealed through the real CAS.
    """
    clock = [T0]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    monkeypatch.setattr(pool.socket, "gethostname", lambda: host)
    capacity = {"cpu": cpu, "gpu": 1, "mem_gb": mem_gb}
    tiers = {"preferred": list(range(cpu)), "fallback": []}
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": clock[0], "busy_cpus": 0., "psi_some": 0.,
        "cpu_count": cpu, "interval_s": 1.})
    gpu_sample = {"schema": "prismabuild.gpu_capacity.v1", "sample_id": str(clock[0]),
                  "sampled_unix": clock[0], "complete": True, "attributed": True,
                  "devices": [{"uuid": "GPU-1", "name": "NVIDIA GB10", "power_w": 15.,
                               "power_limit_w": None, "power_reference_w": 140.,
                               "power_reference_scope": "soc_tdp",
                               "memory_domain": "shared_system", "limited": False}],
                  "host_total_bytes": 128 * adaptive_gpu.GIB,
                  "host_available_bytes": 120 * adaptive_gpu.GIB,
                  "memory_pressure_some": 0., "memory_pressure_full": 0.,
                  "cpu_pressure_some": 0., "foreign_processes": [], "jobs": []}
    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda self: dict(gpu_sample))
    queue = pool.PoolQueue(tmp_path / "queue")
    worker_tags = [host, "gb10", pb.INTERPRETER_TAG, pb.CONTAINER_IMAGE_TAG]
    queue.announce(host=host, tags=worker_tags, has_gpu=True, capacity=capacity,
                   observed_capacity=capacity, interpreters=[sys.executable], observed_images=[])
    queue.announce(host="dl380g10", tags=["dl380g10", "x86", pb.INTERPRETER_TAG, pb.CONTAINER_IMAGE_TAG],
                   has_gpu=False, capacity={"cpu": 40, "mem_gb": 200},
                   observed_capacity={"cpu": 40, "mem_gb": 200},
                   interpreters=[sys.executable], observed_images=[])
    (tmp_path / "task_code.py").write_text("# closure member\n")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")

    issued = [0]

    def seal(*, task_class="generation", resources, gpu=False):
        issued[0] += 1
        body = _body(tmp_path, task_class=task_class,
                     result_path=f"result-{issued[0]}.bin",
                     **({"portability": "platform_keyed", "platform_key": "linux-aarch64-sm121"}
                         if task_class == "measurement" else {}))
        params = {"demand": dict(resources)}
        if gpu:
            params.update({"gpu_exclusive": False, "gpu_memory_gb": 8})
        body["params"] = params
        action = pb.seal_action(body)
        cas.publish_action_request(action)
        return str(action["action_key"])

    def publish(key, *, gpu=False, tags, resources, priority=0, **fields):
        task_class = fields.pop("task_class", "generation")
        sealed = seal(task_class=task_class, resources=resources, gpu=gpu)
        clock[0] += 0.001
        queue.publish(action_key=sealed, cas_root=str(cas.root), checkout_root=str(tmp_path),
                      worker_script="worker.py", tags=tags, needs_gpu=gpu, priority=priority,
                      resources=resources, **fields)
        return sealed

    def claim():
        return queue.claim(tags=worker_tags, has_gpu=True, capacity=capacity, cpu_tiers=tiers,
                           adaptive_cpu=True)

    def tick(seconds=2.0):
        clock[0] += seconds
        gpu_sample.update(sampled_unix=clock[0], sample_id=str(clock[0]))
        gpu_sample["jobs"] = []
        for held in queue.ledger().held_keys():
            path = adaptive_cpu.local_telemetry_path(queue.ledger().base, held)
            keep = adaptive_cpu.read_json(path)
            record = {"action_key": held, "nonce": held + "-attempt",
                      "scope_unit": held + "-scope", "sampled_unix": clock[0],
                      "cpu_seconds": 0.01 * (clock[0] - T0),
                      "wall_seconds": clock[0] - T0, "complete": True}
            if keep.get("nonce") not in (None, held + "-attempt"):
                record["nonce"] = keep["nonce"]
                record["cpu_seconds"] = keep.get("cpu_seconds", record["cpu_seconds"])
                record["wall_seconds"] = keep.get("wall_seconds", record["wall_seconds"])
                record["sampled_unix"] = keep.get("sampled_unix", record["sampled_unix"])
            adaptive_cpu.write_json(path, record)
            gpu_sample["jobs"].append({"action_key": held, "nonce": record["nonce"],
                                       "scope_id": record["scope_unit"], "complete": True})

    def telemetry(key, *, cpu_seconds, wall_seconds, nonce=None, cpu_per_s=None):
        path = adaptive_cpu.local_telemetry_path(queue.ledger().base, key)
        keep = adaptive_cpu.read_json(path)
        record = {"action_key": key, "sampled_unix": clock[0], "cpu_seconds": cpu_seconds,
                  "wall_seconds": wall_seconds, "memory_current_bytes": 100,
                  "memory_peak_bytes": 100, "complete": True,
                  **({"nonce": nonce} if nonce is not None else {})}
        adaptive_cpu.write_json(path, record)
        # Seed the controller's cached previous record with a stated prior
        # interval, so the owner's delta and the replay read one rule.  A
        # steady rate extends the same line back two seconds; an explicit
        # quiet past is stated through ``cpu_per_s=None`` below.
        if nonce is not None and cpu_per_s is not None and keep.get("nonce") == nonce:
            prior = {"action_key": key, "nonce": nonce, "sampled_unix": clock[0] - 2.0,
                     "cpu_seconds": cpu_seconds - 2.0 * cpu_per_s,
                     "wall_seconds": wall_seconds - 2.0, "complete": True}
            jobs_path = adaptive_cpu.local_state_base(queue.ledger().base) / "jobs.json"
            jobs = adaptive_cpu.read_json(jobs_path)
            jobs[key] = prior
            adaptive_cpu.write_json(jobs_path, jobs)

    return {"queue": queue, "publish": publish, "claim": claim, "tick": tick,
            "telemetry": telemetry, "clock": clock, "capacity": capacity, "tiers": tiers,
            "gpu_sample": gpu_sample}


def _denial(queue, key):
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(value for value in records.values() if value["action_key"] == key)


@pytest.mark.parametrize("cpu_host", ["fresh", "stale"])
def test_a_class_scoped_cpu_row_is_not_held_behind_a_ready_gpu_row(tmp_path, monkeypatch, host, cpu_host):
    """The arm64 smoke: tagged for the GPU hosts' class, so no x86 box can ever run it."""
    queue, publish, claim, keys, _seal = _fleet(tmp_path, monkeypatch, host, cpu_host=cpu_host)
    class_key = publish(CLASS_KEY, tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=1)
    claimed = claim()
    assert claimed is not None and claimed["action_key"] == class_key, claimed
    assert queue.item_path(pool.READY, keys[GPU_KEY]).exists(), "the GPU row is still waiting"
    # ... and it is not crowded out: it claims beside the class-scoped row.
    again = claim()
    assert again is not None and again["action_key"] == keys[GPU_KEY], again


@pytest.mark.parametrize("cpu_host", ["fresh", "stale"])
def test_portable_cpu_work_still_waits_for_the_ready_gpu_row(tmp_path, monkeypatch, host, cpu_host):
    """Every box carries these tags, so it is portable: x86 down for a while changes nothing."""
    queue, publish, claim, keys, _seal = _fleet(tmp_path, monkeypatch, host, cpu_host=cpu_host)
    portable = publish(PORTABLE_KEY, tags=[pb.INTERPRETER_TAG], resources={"cpu": 2, "mem_gb": 4}, priority=1,
                       interpreter=sys.executable)
    first = claim()
    assert first is not None and first["action_key"] == keys[GPU_KEY], first
    queue.finish(keys[GPU_KEY], status="executed")
    second = claim()
    assert second is not None and second["action_key"] == portable, second


def test_a_class_scoped_row_that_does_not_fit_beside_the_gpu_reservation_still_waits(tmp_path, monkeypatch, host):
    """GPU safety: the eligible GPU row's reservation is held out of the total first."""
    queue, publish, claim, keys, _seal = _fleet(tmp_path, monkeypatch, host, cpu_host="fresh")
    class_key = publish(CLASS_KEY, tags=["gb10"], resources={"cpu": 7, "mem_gb": 4}, priority=1)   # 8 - 2 = 6 CPUs beside it
    first = claim()
    assert first is not None and first["action_key"] == keys[GPU_KEY], first
    assert queue.item_path(pool.READY, class_key).exists()


def test_with_no_host_without_a_gpu_on_file_nothing_is_excluded(tmp_path, monkeypatch, host):
    """No evidence about the non-GPU side: the row is not guessed to be class-scoped."""
    queue, publish, claim, keys, _seal = _fleet(tmp_path, monkeypatch, host, cpu_host="none")
    publish(CLASS_KEY, tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=1)
    first = claim()
    assert first is not None and first["action_key"] == keys[GPU_KEY], first


def test_a_higher_priority_gpu_row_still_goes_first(tmp_path, monkeypatch, host):
    """The exemption leaves the priority order alone."""
    queue, publish, claim, keys, _seal = _fleet(tmp_path, monkeypatch, host, cpu_host="fresh")
    queue.withdraw(keys[GPU_KEY], reason="republish at a higher priority", by="test")
    gpu_key = publish(GPU_KEY, gpu=True, tags=["gb10"], resources={"cpu": 2, "gpu": 1, "mem_gb": 8}, priority=5)
    class_key = publish(CLASS_KEY, tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=0)
    first = claim()
    assert first is not None and first["action_key"] == gpu_key, first
    second = claim()
    assert second is not None and second["action_key"] == class_key, second


@pytest.mark.parametrize("tags,records,excluded", [
    (["gb10"], [("sparky", True, ["gb10", "x"]), ("dl", False, ["x86", "x"])], True),
    (["x"], [("sparky", True, ["gb10", "x"]), ("dl", False, ["x86", "x"])], False),   # a CPU host offers it
    (["gb10", "x"], [("sparky", True, ["gb10", "x"]), ("dl", False, ["x86", "x"])], True),   # one excluding tag is enough
    (["nobody"], [("sparky", True, ["gb10"]), ("dl", False, ["x86"])], False),      # no GPU host offers it either
    (["gb10"], [("sparky", True, ["gb10"])], False),                                  # no host without a GPU on file
    (["gb10"], [("dl", False, ["x86"])], False),                                      # no GPU host on file
    ([], [("sparky", True, ["gb10"]), ("dl", False, ["x86"])], False),               # no required tag
    (["gb10"], [], False),
])
def test_excluded_from_cpu_hosts_table(tags, records, excluded):
    offers = [{"host": name, "has_gpu": gpu, "tags": offered} for name, gpu, offered in records]
    assert pool.PoolQueue._excluded_from_cpu_hosts({"tags": tags}, offers) is excluded


def test_a_malformed_tag_list_is_not_excluded():
    assert pool.PoolQueue._excluded_from_cpu_hosts({"tags": "gb10"}, []) is False
    assert pool.PoolQueue._excluded_from_cpu_hosts({"tags": [5, None]}, []) is False


# --- review 1 of PR 1590: GPU safety is the GPU row's own room, against the FREE tokens --------

CLASS_KEY_B = "e" * 64
INCUMBENT_KEY = "f" * 64


def test_an_incumbent_holding_tokens_keeps_the_class_scoped_row_out_of_the_gpu_rows_room(tmp_path, monkeypatch, host):
    """Total capacity would admit it (32 - 8 >= 5 GiB); the free tokens after the incumbent do not."""
    queue, publish, claim, keys, _seal = _fleet(tmp_path, monkeypatch, host, cpu_host="fresh")
    incumbent = publish(INCUMBENT_KEY, tags=[host], resources={"cpu": 4, "mem_gb": 20}, priority=2)   # host-pinned: not held back
    assert claim()["action_key"] == incumbent
    class_key = publish(CLASS_KEY, tags=["gb10"], resources={"cpu": 2, "mem_gb": 5}, priority=1)
    # free: cpu 4, mem 12.  After the class row: mem 7 < 8, the GPU row's room.
    claimed = claim()
    assert claimed is not None and claimed["action_key"] == keys[GPU_KEY], claimed
    assert queue.item_path(pool.READY, class_key).exists()


def test_two_class_scoped_rows_that_fit_alone_do_not_fit_together_beside_the_gpu_row(tmp_path, monkeypatch, host):
    """Cumulative: the second admission sees the tokens the first took."""
    queue, publish, claim, keys, _seal = _fleet(tmp_path, monkeypatch, host, cpu_host="fresh")
    first_key = publish(CLASS_KEY, tags=["gb10"], resources={"cpu": 3, "mem_gb": 4}, priority=1)
    second_key = publish(CLASS_KEY_B, tags=["gb10"], resources={"cpu": 4, "mem_gb": 4}, priority=1)
    first = claim()
    assert first is not None and first["action_key"] == first_key, first      # free cpu 8 -> 5 >= the room's 2
    second = claim()
    assert second is not None and second["action_key"] == keys[GPU_KEY], second     # B would leave 1 < 2: held; the GPU row starts
    assert queue.item_path(pool.READY, second_key).exists()


def _bare(tmp_path, monkeypatch):
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparky")
    return pool.PoolQueue(tmp_path / "queue")


ROOM = {"action_key": GPU_KEY, "room": {"cpu": 2, "gpu": 1, "mem_gb": 8}}


class _Tokens:
    def __init__(self, **tokens):
        self.tokens = tokens

    def available(self):
        if self.tokens.get("unreadable"):
            raise OSError("estale")
        return dict(self.tokens)


def test_a_measurement_row_is_not_let_past_the_guard():
    """Its exclusivity contracts are its own: the identity's measurement flag holds it."""
    result = pool.PoolQueue._class_scoped_beside_room(
        ROOM, ledger=_Tokens(cpu=8, gpu=1, mem_gb=32), identity=("identity", True), demand={"cpu": 2, "mem_gb": 4})
    assert result == {"class_scoped": "measurement_row"}


def test_the_boundary_reads_only_the_free_tokens_and_leaves_the_gpu_room(tmp_path, monkeypatch):
    """No sealed request, no record, no shared read: arithmetic on the free tokens, after the row's own."""
    decide = pool.PoolQueue._class_scoped_beside_room
    demand = {"cpu": 2, "mem_gb": 4}
    quiet = {"active": 0.0, "pending": 0.0, "busy_cpus": 0.0}
    assert decide(ROOM, ledger=_Tokens(cpu=4, gpu=1, mem_gb=12), identity=None, demand=demand,
                  cpu_count=8, holder_costs=quiet) is None   # 2, 8 left; 0 + 2 + 2 <= 8
    held = decide(ROOM, ledger=_Tokens(cpu=4, gpu=1, mem_gb=11), identity=None, demand=demand,
                  cpu_count=8, holder_costs=quiet)        # 7 < 8
    assert held["kept_for"] == "class_scoped_cpu_beside_ready_gpu" and held["gpu_row"] == GPU_KEY[:12]
    assert decide(ROOM, ledger=_Tokens(cpu=8, gpu=0, mem_gb=32), identity=None, demand=demand,
                  cpu_count=8, holder_costs=quiet)["room"]["gpu"] == 1
    assert decide(ROOM, ledger=_Tokens(unreadable=True), identity=None, demand=demand,
                  cpu_count=8, holder_costs=quiet) == {
        "class_scoped": "free_tokens_unreadable"}


def test_the_boundary_keeps_adaptive_headroom_for_the_gpu_row(tmp_path, monkeypatch):
    """Review 3 finding 1, replayed under review 4's aggregate rule: the owner's arithmetic decides.

    Free tokens fit the room (8 - 2 >= 6).  With no other holder the candidate at
    its full reservation plus the GPU row still fits (0 + 2 + 6 <= 8); a busy
    2-CPU incumbent beside them does not (2.5 + 2 + 6 > 8).  The token fit alone
    admits both; the aggregate headroom holds the second.
    """
    decide = pool.PoolQueue._class_scoped_beside_room
    big_room = {"action_key": GPU_KEY, "room": {"cpu": 6, "gpu": 1, "mem_gb": 8}}
    demand = {"cpu": 2, "mem_gb": 4}
    quiet = {"active": 0.0, "pending": 0.0, "busy_cpus": 0.0}
    # No incumbent: the pair fits the owner's rule, so the token fit admits.
    assert decide(big_room, ledger=_Tokens(cpu=8, gpu=1, mem_gb=32), identity=None, demand=demand,
                  cpu_count=8, holder_costs=quiet) is None
    # A busy incumbent at 2.5 beside the candidate and the GPU row: held.
    held = decide(big_room, ledger=_Tokens(cpu=8, gpu=1, mem_gb=32), identity=None, demand=demand,
                  cpu_count=8, holder_costs={"active": 2.5, "pending": 0.0, "busy_cpus": 0.0})
    assert held["kept_for"] == "class_scoped_cpu_projected_cpu_cost", held
    assert held["holder_cost"] == 2.0 and held["gpu_cost"] == 6.0
    assert held["active_cpu_cost"] == 2.5
    # The same triple on a 20-CPU host passes: 2.5 + 2 + 6 <= 20.
    assert decide(big_room, ledger=_Tokens(cpu=20, gpu=1, mem_gb=120), identity=None, demand=demand,
                  cpu_count=20, holder_costs={"active": 2.5, "pending": 0.0, "busy_cpus": 0.0}) is None
    # Arithmetic only: no sealed request, no record, no shared read.
    assert pool.PoolQueue._class_scoped_projected_headroom(
        big_room, demand, cpu_count=8,
        holder_costs={"active": 2.5, "pending": 0.0, "busy_cpus": 0.0}
    )["kept_for"] == "class_scoped_cpu_projected_cpu_cost"
    assert pool.PoolQueue._class_scoped_projected_headroom(
        big_room, demand, cpu_count=20,
        holder_costs={"active": 2.5, "pending": 0.0, "busy_cpus": 0.0}) is None




def test_the_gpu_row_must_be_one_a_running_cpu_holder_cannot_keep_from_starting(tmp_path, monkeypatch):
    """Review 2: a measurement row needs an idle host; an unbounded row is refused when the box holds anything;
    a gang member's election fences the host."""
    queue = _bare(tmp_path, monkeypatch)
    (tmp_path / "task_code.py").write_text("# closure member\n")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")

    def seal(*, task_class="generation", resources):
        body = _body(tmp_path, task_class=task_class,
                     **({"portability": "platform_keyed", "platform_key": "linux-aarch64-sm121"}
                         if task_class == "measurement" else {}))
        body["params"] = {"demand": dict(resources), "gpu_exclusive": False, "gpu_memory_gb": 8}
        action = pb.seal_action(body)
        cas.publish_action_request(action)
        key = str(action["action_key"])
        return {"action_key": key, "cas_root": str(cas.root), "needs_gpu": True,
                "resources": dict(resources)}

    real_contract = pool.gpu_admission.action_contract
    monkeypatch.setattr(queue, "_ready_gpu_row_room", lambda *args, **kw: dict(ROOM))
    generation = seal(resources={"cpu": 2, "gpu": 1, "mem_gb": 8})
    assert queue._class_scoped_room(
        generation, ledger=_Tokens(cpu=8, gpu=1, mem_gb=32), total=CAPACITY, controller=None,
        gpu_controller=None, observed_images=None, container_class_policy=None,
        container_inventory=None) == (ROOM, None)
    assert queue._class_scoped_room(
        {**generation, "gang": {"group": "g" * 32, "size": 2, "index": 0}},
        ledger=_Tokens(cpu=8, gpu=1, mem_gb=32), total=CAPACITY, controller=None,
        gpu_controller=None, observed_images=None, container_class_policy=None,
        container_inventory=None) == (None, "gpu_row_is_gang_member")
    unbounded = seal(resources={"gpu": 1, "mem_gb": 8})
    assert queue._class_scoped_room(
        unbounded, ledger=_Tokens(cpu=8, gpu=1, mem_gb=32), total=CAPACITY, controller=None,
        gpu_controller=None, observed_images=None, container_class_policy=None,
        container_inventory=None) == (None, "gpu_row_cpu_unbounded")
    no_mem = seal(resources={"cpu": 2, "gpu": 1})
    assert queue._class_scoped_room(
        no_mem, ledger=_Tokens(cpu=8, gpu=1, mem_gb=32), total=CAPACITY, controller=None,
        gpu_controller=None, observed_images=None, container_class_policy=None,
        container_inventory=None) == (None, "gpu_row_cpu_unbounded")
    measurement = seal(task_class="measurement", resources={"cpu": 2, "gpu": 1, "mem_gb": 8})
    assert queue._class_scoped_room(
        measurement, ledger=_Tokens(cpu=8, gpu=1, mem_gb=32), total=CAPACITY, controller=None,
        gpu_controller=None, observed_images=None, container_class_policy=None,
        container_inventory=None) == (None, "gpu_row_is_measurement")
    # Review 3 finding 2: the real reader answers an unreadable request as
    # (None, measurement, True, budget), not as an exception.  The exemption
    # must still see unknown, never an ordinary non-measurement contract.
    missing = {"action_key": "f" * 64, "cas_root": str(tmp_path / "empty"),
               "needs_gpu": True, "resources": {"cpu": 2, "gpu": 1, "mem_gb": 8}}
    assert real_contract(missing, {"cpu": 2, "mem_gb": 8})[0] is None
    assert queue._class_scoped_room(
        missing, ledger=_Tokens(cpu=8, gpu=1, mem_gb=32), total=CAPACITY, controller=None,
        gpu_controller=None, observed_images=None, container_class_policy=None,
        container_inventory=None) == (None, "gpu_row_contract_unreadable")
    assert queue._class_scoped_gpu_shape(missing) == (None, None, "gpu_row_request_unreadable")
    shape, is_measurement, why = queue._class_scoped_gpu_shape(generation)
    assert why is None and shape is not None and is_measurement is False
    shape, is_measurement, why = queue._class_scoped_gpu_shape(measurement)
    assert why is None and shape is not None and is_measurement is True


def test_a_gpu_room_that_cannot_be_established_holds_the_row(tmp_path, monkeypatch):
    """The GPU row's own claim facts (a clean fresh sample, its images, its residency) are not met."""
    queue = _bare(tmp_path, monkeypatch)
    (tmp_path / "task_code.py").write_text("# closure member\n")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    body = _body(tmp_path)
    body["params"] = {"demand": {"cpu": 2, "gpu": 1, "mem_gb": 8},
                      "gpu_exclusive": False, "gpu_memory_gb": 8}
    action = pb.seal_action(body)
    cas.publish_action_request(action)
    row = {"action_key": str(action["action_key"]), "cas_root": str(cas.root),
           "needs_gpu": True, "resources": {"cpu": 2, "gpu": 1, "mem_gb": 8}}
    monkeypatch.setattr(queue, "_ready_gpu_row_room", lambda *args, **kw: None)
    assert queue._class_scoped_room(
        row, ledger=_Tokens(cpu=8, gpu=1, mem_gb=32), total=CAPACITY, controller=None,
        gpu_controller=None, observed_images=None, container_class_policy=None,
        container_inventory=None) == (None, "gpu_room_unknown")

def test_a_class_scoped_row_without_projected_headroom_still_waits(tmp_path, monkeypatch, host):
    """Review 3 finding 1 end to end, under review 4's aggregate rule and controllers.

    A host-pinned 2-CPU incumbent runs at 2 CPUs beside a 2-CPU class row and
    a 6-CPU GPU row on 8 CPUs.  The class row cannot take the GPU row's room
    (8 - 2 - 2 < 6), so the token fit holds it before any headroom check;
    the GPU row itself cannot start on this sample either (2.5 + 6 > 8) and
    waits for the incumbent to drain, as the owner's rule requires.  The
    headroom replay is pinned by the tests below; this one pins the token
    boundary under the real controllers.
    """
    rig = _adaptive_fleet(tmp_path, monkeypatch, host)
    queue, publish, claim = rig["queue"], rig["publish"], rig["claim"]
    gpu_key = publish(_key("gpu"), gpu=True, tags=["gb10"],
                      resources={"cpu": 6, "gpu": 1, "mem_gb": 8})
    incumbent = publish(_key("incumbent"), tags=[host], resources={"cpu": 2, "mem_gb": 4}, priority=2)
    assert claim()["action_key"] == incumbent
    rig["telemetry"](incumbent, cpu_seconds=4.0, wall_seconds=2.0, cpu_per_s=2.0, nonce=incumbent + "-n1")
    rig["tick"]()
    class_key = publish(_key("class"), tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=1)
    first = claim()
    assert first is not None and first["action_key"] == gpu_key, (
        f"the GPU row claims first: {_denial(queue, gpu_key)}")
    assert queue.item_path(pool.READY, class_key).exists()
    denial = _denial(queue, class_key)
    assert denial["reason"] == "deferred_for_ready_gpu_row", denial
    assert denial["evidence"]["kept_for"] == "class_scoped_cpu_beside_ready_gpu", denial



def test_a_measurement_gpu_row_keeps_the_class_scoped_row_waiting_for_the_whole_claim(tmp_path, monkeypatch, host):
    """End to end: the eligible GPU row is a measurement, so no CPU holder may run beside it."""
    queue, publish, claim, keys, _seal = _fleet(tmp_path, monkeypatch, host, cpu_host="fresh")
    queue.withdraw(keys[GPU_KEY], reason="republish as measurement", by="test")
    gpu_key = publish(GPU_KEY, gpu=True, tags=["gb10"], resources={"cpu": 2, "gpu": 1, "mem_gb": 8},
                      task_class="measurement")
    class_key = publish(CLASS_KEY, tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=1)
    claimed = claim()
    assert claimed is not None and claimed["action_key"] == gpu_key, claimed
    assert queue.item_path(pool.READY, class_key).exists()


def test_a_gpu_row_with_no_explicit_cpu_keeps_the_class_scoped_row_waiting(tmp_path, monkeypatch, host):
    queue, publish, claim, keys, _seal = _fleet(tmp_path, monkeypatch, host, cpu_host="fresh")
    queue.withdraw(keys[GPU_KEY], reason="republish unbounded", by="test")
    gpu_key = publish(GPU_KEY, gpu=True, tags=["gb10"], resources={"gpu": 1, "mem_gb": 8})
    class_key = publish(CLASS_KEY, tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=1)
    claimed = claim()
    assert claimed is not None and claimed["action_key"] == gpu_key, claimed
    assert queue.item_path(pool.READY, class_key).exists()


def test_the_gpu_rows_room_is_read_once_per_pass_before_any_admission(tmp_path, monkeypatch, host):
    """Review 2: nothing that reads the shared filesystem runs inside host admission, once per pass.

    Review 3 notes the old form admits the first candidate and returns, so it
    never evaluates the second.  Here both candidates refuse the token fit
    (each alone fits, together they do not leave the room), so one pass
    evaluates both against one cached room read.
    """
    queue, publish, claim, keys, _seal = _fleet(tmp_path, monkeypatch, host, cpu_host="fresh")
    calls = []
    real = queue._class_scoped_room

    def counted(*args, **kw):
        calls.append(1)
        return real(*args, **kw)

    monkeypatch.setattr(queue, "_class_scoped_room", counted)
    # GPU row needs cpu 2; two class rows of cpu 7 each refuse the fit
    # (8 - 7 < 2), so one pass evaluates both against one cached room read.
    first_key = publish(CLASS_KEY, tags=["gb10"], resources={"cpu": 7, "mem_gb": 4}, priority=1)
    second_key = publish(CLASS_KEY_B, tags=["gb10"], resources={"cpu": 7, "mem_gb": 4}, priority=1)
    first = claim()
    assert first is not None and first["action_key"] == keys[GPU_KEY], first
    assert calls == [1], "two refused candidates in one pass read the GPU row's room once"
    assert queue.item_path(pool.READY, first_key).exists()
    assert queue.item_path(pool.READY, second_key).exists()


def test_a_gang_member_is_never_a_candidate(tmp_path, monkeypatch):
    """Review 2: a gang election is written before any later check and outlives a refusal."""
    queue = _bare(tmp_path, monkeypatch)
    offers = [{"host": "sparky", "has_gpu": True, "tags": ["gb10"]}, {"host": "dl", "has_gpu": False, "tags": ["x86"]}]
    item = {"action_key": CLASS_KEY, "tags": ["gb10"], "resources": {"cpu": 2, "mem_gb": 4}}
    assert queue._class_scoped_candidate(item, offers) is True
    assert queue._class_scoped_candidate({**item, "gang": {"group": "g" * 32, "size": 2, "index": 0}}, offers) is False


@pytest.mark.parametrize("resources,candidate", [
    ({"cpu": 2, "mem_gb": 4}, True),
    ({"cpu": 2, "mem_gb": 4, "stage_gib@prismabuild-stage:sparky": 2}, False),   # a tier demand: what the GPU claim may need
    ({"cpu": 2, "mem_gb": 4, "fill_mb_s@prismabuild-stage:sparky": 50}, False),
    ({"cpu": 2, "mem_gb": 4, "scratch_gib": 1}, False),                           # any kind but cpu and mem_gb
    ({"cpu": 2, "mem_gb": 4, "gpu": 1}, False),
    ({"mem_gb": 4}, False),                                                        # no explicit cpu: unbounded
    ({"cpu": 2}, False),
])
def test_only_a_plain_bounded_host_demand_makes_a_candidate(tmp_path, monkeypatch, resources, candidate):
    queue = _bare(tmp_path, monkeypatch)
    offers = [{"host": "sparky", "has_gpu": True, "tags": ["gb10"]},
              {"host": "dl380g10", "has_gpu": False, "tags": ["x86"]}]
    item = {"action_key": CLASS_KEY, "tags": ["gb10"], "resources": resources}
    assert queue._class_scoped_candidate(item, offers) is candidate


# --- review 4, end to end under both controllers --------------------------------------


def test_two_class_holders_that_each_fit_do_not_together_block_the_gpu_row(tmp_path, monkeypatch, host):
    """Review 4 finding 1: the headroom counts incumbents, not just the one new holder.

    The review's counterexample: on an 8-CPU host a 4-CPU GPU row waits, and two
    2-CPU class rows each fit alone (2.5 + 4 <= 8).  The second must wait: with
    both holders running at 2 CPUs each the GPU row's own decision refuses
    (2.5 + 2.5 + 4 > 8).  Tokens alone would admit both (8 - 2 - 2 >= 4).
    """
    rig = _adaptive_fleet(tmp_path, monkeypatch, host)
    queue, publish, claim = rig["queue"], rig["publish"], rig["claim"]
    gpu_key = publish(_key("gpu"), gpu=True, tags=["gb10"],
                      resources={"cpu": 4, "gpu": 1, "mem_gb": 8})
    first_key = publish(_key("class-a"), tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=1)
    second_key = publish(_key("class-b"), tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=1)
    first = claim()
    assert first is not None and first["action_key"] == first_key, first
    rig["telemetry"](first_key, cpu_seconds=4.0, wall_seconds=2.0, cpu_per_s=2.0, nonce=first_key + "-n1")
    rig["tick"]()
    rig["telemetry"](first_key, cpu_seconds=8.0, wall_seconds=4.0, cpu_per_s=2.0, nonce=first_key + "-n1")
    run = claim()
    assert run is not None and run["action_key"] == gpu_key, run
    assert queue.item_path(pool.READY, second_key).exists(), (
        "the second class row waits: both holders beside the 4-CPU GPU row exceed the host")
    queue.finish(gpu_key, status="executed")
    rig["tick"]()
    assert claim()["action_key"] == second_key


def test_a_busy_incumbent_counts_against_the_gpu_rows_headroom(tmp_path, monkeypatch, host):
    """Review 4 finding 1: a holder running hot charges its measured cost, not zero.

    A host-pinned 2-CPU incumbent runs at 2 CPUs; the class row needs 2 CPUs and
    the GPU row 4.  Tokens fit (8 - 2 - 2 >= 4), but the owner's rule refuses the
    GPU row beside both (2.5 + 2.5 + 4 > 8), so the class row waits.
    """
    rig = _adaptive_fleet(tmp_path, monkeypatch, host)
    queue, publish, claim = rig["queue"], rig["publish"], rig["claim"]
    gpu_key = publish(_key("gpu"), gpu=True, tags=["gb10"],
                      resources={"cpu": 4, "gpu": 1, "mem_gb": 8})
    incumbent = publish(_key("incumbent"), tags=[host], resources={"cpu": 2, "mem_gb": 4}, priority=2)
    assert claim()["action_key"] == incumbent
    rig["telemetry"](incumbent, cpu_seconds=4.0, wall_seconds=2.0, cpu_per_s=2.0, nonce=incumbent + "-n1")
    rig["tick"]()
    rig["telemetry"](incumbent, cpu_seconds=8.0, wall_seconds=4.0, cpu_per_s=2.0, nonce=incumbent + "-n1")
    class_key = publish(_key("class"), tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=1)
    run = claim()
    assert run is not None and run["action_key"] == gpu_key, run
    assert queue.item_path(pool.READY, class_key).exists()


def test_a_holder_without_telemetry_charges_its_full_reservation(tmp_path, monkeypatch, host):
    """Review 4 finding 1: startup/unknown attribution holds the exemption, like the owner.

    A 3-CPU class row beside a 4-CPU GPU row passes bare token arithmetic
    (8 - 3 >= 4) and the old single-holder check (3.75 + 4 <= 8).  With no
    telemetry for a running 3-CPU holder the owner charges the reservation in
    full and double-counts it against the busy baseline; the exemption holds.
    """
    rig = _adaptive_fleet(tmp_path, monkeypatch, host)
    queue, publish, claim = rig["queue"], rig["publish"], rig["claim"]
    gpu_key = publish(_key("gpu"), gpu=True, tags=["gb10"],
                      resources={"cpu": 4, "gpu": 1, "mem_gb": 8})
    holder = publish(_key("holder"), tags=[host], resources={"cpu": 3, "mem_gb": 4}, priority=2)
    assert claim()["action_key"] == holder
    rig["tick"](seconds=0.0)
    for path in (adaptive_cpu.local_telemetry_path(queue.ledger().base, holder),):
        if path.exists():
            path.unlink()
    class_key = publish(_key("class"), tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=1)
    run = claim()
    assert run is not None and run["action_key"] == gpu_key, run
    assert queue.item_path(pool.READY, class_key).exists()


def test_a_quiet_holder_leaves_room_for_a_small_class_row_and_its_gpu_row(tmp_path, monkeypatch, host):
    """The exemption still admits when the aggregate truly fits, under both controllers.

    A host-pinned 2-CPU holder idles near zero; the class row needs 1 CPU and the
    GPU row 2.  The owner's rule admits the GPU row beside both, so the class row
    claims first and the GPU row follows on the same box.
    """
    rig = _adaptive_fleet(tmp_path, monkeypatch, host)
    queue, publish, claim = rig["queue"], rig["publish"], rig["claim"]
    gpu_key = publish(_key("gpu"), gpu=True, tags=["gb10"],
                      resources={"cpu": 2, "gpu": 1, "mem_gb": 8})
    incumbent = publish(_key("incumbent"), tags=[host], resources={"cpu": 2, "mem_gb": 4}, priority=2)
    assert claim()["action_key"] == incumbent
    rig["tick"]()
    rig["tick"]()
    class_key = publish(_key("class"), tags=["gb10"], resources={"cpu": 1, "mem_gb": 4}, priority=1)
    first = claim()
    assert first is not None and first["action_key"] == class_key, (
        f"the fitting class row claims first: {_denial(queue, class_key)}")
    rig["tick"]()
    second = claim()
    assert second is not None and second["action_key"] == gpu_key, (
        f"the GPU row follows beside it: {_denial(queue, gpu_key)}")


def test_an_unreadable_holder_ledger_holds_the_exemption(tmp_path, monkeypatch, host):
    """Fail closed: holder costs that do not read are unknown, never zero."""
    rig = _adaptive_fleet(tmp_path, monkeypatch, host)
    queue, publish, claim = rig["queue"], rig["publish"], rig["claim"]
    gpu_key = publish(_key("gpu"), gpu=True, tags=["gb10"],
                      resources={"cpu": 4, "gpu": 1, "mem_gb": 8})
    incumbent = publish(_key("incumbent"), tags=[host], resources={"cpu": 2, "mem_gb": 4}, priority=2)
    assert claim()["action_key"] == incumbent
    real_costs = pool.PoolQueue._class_scoped_holder_costs

    def unreadable(*args, **kwargs):
        raise OSError("estale")

    monkeypatch.setattr(pool.PoolQueue, "_class_scoped_holder_costs", unreadable)
    try:
        class_key = publish(_key("class"), tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=1)
        run = claim()
        assert run is not None and run["action_key"] == gpu_key, run
        assert queue.item_path(pool.READY, class_key).exists()
        assert _denial(queue, class_key)["reason"] == "deferred_for_ready_gpu_row"
    finally:
        monkeypatch.setattr(pool.PoolQueue, "_class_scoped_holder_costs", real_costs)


def test_a_class_row_claims_beside_a_ready_gpu_row_on_a_private_pool(tmp_path, monkeypatch, host):
    """Review 4 finding 3: the production-path smoke on an isolated private pool.

    A fresh queue root (no shared fleet state), both adaptive controllers from
    the worker loop's own claim arguments, one eligible GPU row READY, one
    class-scoped CPU row: the class row claims, the GPU row follows, and the
    ledger tokens balance afterwards.
    """
    rig = _adaptive_fleet(tmp_path, monkeypatch, host)
    queue, publish, claim = rig["queue"], rig["publish"], rig["claim"]
    gpu_key = publish(_key("gpu"), gpu=True, tags=["gb10"],
                      resources={"cpu": 2, "gpu": 1, "mem_gb": 8})
    class_key = publish(_key("class"), tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=1)
    first = claim()
    assert first is not None and first["action_key"] == class_key, (
        f"the class row passes the guard: {_denial(queue, class_key)}")
    rig["tick"]()
    second = claim()
    assert second is not None and second["action_key"] == gpu_key, (
        f"the GPU row is not crowded out: {_denial(queue, gpu_key)}")
    held = queue.ledger().held()
    assert held == {"cpu": 4, "mem_gb": 12, "gpu": 1}, held
    assert queue.ledger().available() == {"cpu": 4, "mem_gb": 20}, queue.ledger().available()

# --- review 4, the aggregate helper -------------------------------------------------


class _Holders:
    """A ledger stub with free tokens and holder costs for the boundary helper."""

    def __init__(self, *, available, costs=None, unreadable=False):
        self._available = dict(available)
        self._costs = costs
        self._unreadable = unreadable

    def available(self):
        if self._unreadable:
            raise OSError("estale")
        return dict(self._available)


def test_the_aggregate_headroom_counts_incumbent_startup_and_busy_costs(tmp_path, monkeypatch):
    """The helper replays the owner's rule: busy baseline plus pending, or active, plus both sides."""
    queue = _bare(tmp_path, monkeypatch)
    room = {"action_key": GPU_KEY, "room": {"cpu": 4, "gpu": 1, "mem_gb": 8}}
    demand = {"cpu": 2, "mem_gb": 4}
    ledger = _Holders(available={"cpu": 6, "gpu": 1, "mem_gb": 28})
    # One busy incumbent at 2.5 plus this candidate at full reservation:
    # max(0 + 0, 2.5) + 2 + 4 > 8.  Held, and the evidence names every term.
    held = queue._class_scoped_beside_room(
        room, ledger=ledger, identity=None, demand=demand, cpu_count=8,
        holder_costs={"active": 2.5, "pending": 0.0, "busy_cpus": 0.0})
    assert held["kept_for"] == "class_scoped_cpu_projected_cpu_cost", held
    assert held["active_cpu_cost"] == 2.5 and held["pending_cpu_cost"] == 0.0
    assert held["holder_cost"] == 2.0 and held["gpu_cost"] == 4.0
    # A quiet box admits the same pair: 0 + 2 + 4 <= 8.
    assert queue._class_scoped_beside_room(
        room, ledger=ledger, identity=None, demand=demand, cpu_count=8,
        holder_costs={"active": 0.0, "pending": 0.0, "busy_cpus": 0.0}) is None
    # Unknown attribution double-counts against the busy baseline, like the owner.
    held = queue._class_scoped_beside_room(
        room, ledger=ledger, identity=None, demand=demand, cpu_count=8,
        holder_costs={"active": 3.0, "pending": 3.0, "busy_cpus": 3.0})
    assert held["kept_for"] == "class_scoped_cpu_projected_cpu_cost", held


def test_the_aggregate_headroom_holds_when_holder_costs_do_not_read(tmp_path, monkeypatch):
    """Unreadable holder costs are unknown, never zero; unknown CPU count binds nothing new."""
    queue = _bare(tmp_path, monkeypatch)
    room = {"action_key": GPU_KEY, "room": {"cpu": 4, "gpu": 1, "mem_gb": 8}}
    demand = {"cpu": 2, "mem_gb": 4}
    ledger = _Holders(available={"cpu": 6, "gpu": 1, "mem_gb": 28})
    held = queue._class_scoped_beside_room(
        room, ledger=ledger, identity=None, demand=demand, cpu_count=8,
        holder_costs=None)
    assert held == {"class_scoped": "holder_costs_unreadable"}, held
    assert queue._class_scoped_beside_room(
        room, ledger=ledger, identity=None, demand=demand, cpu_count=None,
        holder_costs={"active": 99.0, "pending": 99.0, "busy_cpus": 99.0}) is None


# --- review 5: the owner's interval rule, and the funded candidate's full cost ---


def test_a_quiet_past_does_not_hide_a_busy_present(tmp_path, monkeypatch, host):
    """Review 5 finding 1: the replay uses the interval delta, not the lifetime average.

    A 2-CPU incumbent idled for 100 s (cpu 0), then burned 4 CPU-s in the last
    2 s. The lifetime average reads near zero. The owner charges the interval
    rate (2 * 1.25 = 2.5). The class row (2 CPUs) beside the 4-CPU GPU row on
    8 CPUs must wait: 2.5 + 2 + 4 > 8.
    """
    rig = _adaptive_fleet(tmp_path, monkeypatch, host)
    queue, publish, claim = rig["queue"], rig["publish"], rig["claim"]
    gpu_key = publish(_key("gpu"), gpu=True, tags=["gb10"],
                      resources={"cpu": 4, "gpu": 1, "mem_gb": 8})
    incumbent = publish(_key("incumbent"), tags=[host], resources={"cpu": 2, "mem_gb": 4}, priority=2)
    assert claim()["action_key"] == incumbent
    meta = adaptive_cpu.read_json(queue.ledger().held_dir / incumbent / adaptive_cpu.METADATA)
    admitted = meta["admitted_unix"]
    # A quiet past: 100 s of wall, no CPU. The cached previous record matches
    # the fresh record's nonce, so the owner reads the delta between them.
    adaptive_cpu.write_json(
        adaptive_cpu.local_state_base(queue.ledger().base) / "jobs.json", {
            incumbent: {"action_key": incumbent, "nonce": "n1", "sampled_unix": admitted,
                        "cpu_seconds": 0.0, "wall_seconds": 100.0, "complete": True}})
    rig["clock"][0] = admitted + 102.0
    rig["gpu_sample"].update(sampled_unix=rig["clock"][0], sample_id=str(rig["clock"][0]))
    base = adaptive_cpu.local_state_base(queue.ledger().base)
    adaptive_cpu.write_json(base / "telemetry" / f"{incumbent}.json", {
        "action_key": incumbent, "nonce": "n1", "sampled_unix": rig["clock"][0],
        "cpu_seconds": 4.0, "wall_seconds": 102.0, "memory_current_bytes": 100,
        "memory_peak_bytes": 100, "complete": True})
    now_holder = adaptive_cpu.Controller(queue.ledger(), rig["tiers"])
    now_holder._host_sample = {"sampled_unix": rig["clock"][0], "busy_cpus": 2.0,
                               "cpu_count": 8, "interval_s": 1.0}
    costs = pool.PoolQueue._class_scoped_holder_costs(queue.ledger(), now_holder)
    assert costs is not None and costs["active"] == 2.5, costs
    class_key = publish(_key("class"), tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=1)
    run = claim()
    assert run is not None and run["action_key"] == gpu_key, run
    assert queue.item_path(pool.READY, class_key).exists()


def test_a_superseded_interval_cache_prices_nothing(tmp_path, monkeypatch, host):
    """A new nonce since the cached record prices nothing: unknown, never zero.

    The fresh telemetry carries nonce n2; the cache still holds n1. The owner
    drops the stale cache entry and finds no interval, so it charges the full
    reservation (2.0 active and pending). The replay reports the same terms,
    deterministically: same holder files, same answer.
    """
    rig = _adaptive_fleet(tmp_path, monkeypatch, host)
    queue = rig["queue"]
    incumbent = rig["publish"](_key("incumbent"), tags=[host],
                               resources={"cpu": 2, "mem_gb": 4}, priority=2)
    assert rig["claim"]()["action_key"] == incumbent
    meta = adaptive_cpu.read_json(queue.ledger().held_dir / incumbent / adaptive_cpu.METADATA)
    admitted = meta["admitted_unix"]
    adaptive_cpu.write_json(
        adaptive_cpu.local_state_base(queue.ledger().base) / "jobs.json", {
            incumbent: {"action_key": incumbent, "nonce": "n1", "sampled_unix": admitted,
                        "cpu_seconds": 0.0, "wall_seconds": 1.0, "complete": True}})
    rig["clock"][0] = admitted + 3.0
    rig["gpu_sample"].update(sampled_unix=rig["clock"][0], sample_id=str(rig["clock"][0]))
    base = adaptive_cpu.local_state_base(queue.ledger().base)
    adaptive_cpu.write_json(base / "telemetry" / f"{incumbent}.json", {
        "action_key": incumbent, "nonce": "n2", "sampled_unix": rig["clock"][0],
        "cpu_seconds": 0.1, "wall_seconds": 4.0, "memory_current_bytes": 100,
        "memory_peak_bytes": 100, "complete": True})
    probe = adaptive_cpu.Controller(queue.ledger(), rig["tiers"])
    probe._host_sample = {"sampled_unix": rig["clock"][0], "busy_cpus": 2.0,
                          "cpu_count": 8, "interval_s": 1.0}
    costs = pool.PoolQueue._class_scoped_holder_costs(queue.ledger(), probe)
    assert costs is not None, costs
    assert costs["active"] == 2.0 and costs["pending"] == 2.0, costs
    again = pool.PoolQueue._class_scoped_holder_costs(queue.ledger(), probe)
    assert again == costs, (again, costs)


def test_a_superseded_interval_cache_holds_the_row(tmp_path, monkeypatch, host):
    """A superseded cache charges the full reservation in the headroom replay.

    The fresh telemetry carries nonce n2; the cache still holds n1. The owner
    drops the stale cache entry and finds no interval, so it charges the full
    reservation (2.0 active and pending). The replayed headroom must hold a
    3-CPU class row beside the 2-CPU GPU row on 8 CPUs with those terms
    (max(2 + 2, 2) + 3 + 2 > 8) and name the projected-cost headroom. The
    determinism unit above pins the replay terms; this pins the boundary
    answer on the same terms.
    """
    rig = _adaptive_fleet(tmp_path, monkeypatch, host)
    queue, publish = rig["queue"], rig["publish"]
    room = {"action_key": _key("gpu"), "room": {"cpu": 2, "gpu": 1, "mem_gb": 8}}
    held = queue._class_scoped_beside_room(
        room, ledger=_Holders(available={"cpu": 8, "gpu": 1, "mem_gb": 32}), identity=None,
        demand={"cpu": 3, "mem_gb": 4}, cpu_count=8,
        holder_costs={"active": 2.0, "pending": 2.0, "busy_cpus": 2.0},
        candidate_demand={"cpu": 3, "mem_gb": 4})
    assert held["kept_for"] == "class_scoped_cpu_projected_cpu_cost", held
    assert held["active_cpu_cost"] == 2.0 and held["pending_cpu_cost"] == 2.0, held
    assert held["holder_cost"] == 3.0 and held["gpu_cost"] == 2.0, held


def test_a_funded_candidate_pays_its_full_cost(tmp_path, monkeypatch):
    """Review 5 finding 1: a fully funded row costs its reservation, not its remainder.

    The headroom unit: a 2-CPU candidate whose token remainder is zero still
    charges 2.0 beside a 4-CPU GPU row on 8 CPUs with a 2.5 incumbent cost.
    """
    queue = _bare(tmp_path, monkeypatch)
    room = {"action_key": GPU_KEY, "room": {"cpu": 4, "gpu": 1, "mem_gb": 8}}
    ledger = _Holders(available={"cpu": 8, "gpu": 1, "mem_gb": 32})
    costs = {"active": 2.5, "pending": 0.0, "busy_cpus": 0.0}
    held = queue._class_scoped_beside_room(
        room, ledger=ledger, identity=None, demand={},
        cpu_count=8, holder_costs=costs,
        candidate_demand={"cpu": 2, "mem_gb": 4})
    assert held["kept_for"] == "class_scoped_cpu_projected_cpu_cost", held
    assert held["holder_cost"] == 2.0 and held["gpu_cost"] == 4.0


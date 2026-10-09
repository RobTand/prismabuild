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
import json
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_core import _body  # noqa: E402

from prismabuild import core as pb, pool

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
    assert decide(ROOM, ledger=_Tokens(cpu=4, gpu=1, mem_gb=12), identity=None, demand=demand,
                  cpu_count=8) is None   # 2, 8 left; 2.5 + 2 <= 8
    held = decide(ROOM, ledger=_Tokens(cpu=4, gpu=1, mem_gb=11), identity=None, demand=demand,
                  cpu_count=8)        # 7 < 8
    assert held["kept_for"] == "class_scoped_cpu_beside_ready_gpu" and held["gpu_row"] == GPU_KEY[:12]
    assert decide(ROOM, ledger=_Tokens(cpu=8, gpu=0, mem_gb=32), identity=None, demand=demand,
                  cpu_count=8)["room"]["gpu"] == 1
    assert decide(ROOM, ledger=_Tokens(unreadable=True), identity=None, demand=demand,
                  cpu_count=8) == {
        "class_scoped": "free_tokens_unreadable"}


def test_the_boundary_keeps_adaptive_headroom_for_the_gpu_row(tmp_path, monkeypatch):
    """Review 3 finding 1: a 2-CPU holder costs 2.5 CPUs, so six free tokens do not admit a 6-CPU GPU row."""
    decide = pool.PoolQueue._class_scoped_beside_room
    big_room = {"action_key": GPU_KEY, "room": {"cpu": 6, "gpu": 1, "mem_gb": 8}}
    demand = {"cpu": 2, "mem_gb": 4}
    # Free tokens fit the room (8 - 2 >= 6), but 2.5 + 6 > 8: held.
    held = decide(big_room, ledger=_Tokens(cpu=8, gpu=1, mem_gb=32), identity=None, demand=demand,
                  cpu_count=8)
    assert held["kept_for"] == "class_scoped_cpu_projected_cpu_cost", held
    assert held["holder_cost"] == 2.5 and held["gpu_cost"] == 6.0
    # The same pair on a 20-CPU host passes: 2.5 + 6 <= 20.
    assert decide(big_room, ledger=_Tokens(cpu=20, gpu=1, mem_gb=120), identity=None, demand=demand,
                  cpu_count=20) is None
    # Arithmetic only: no sealed request, no record, no shared read.
    assert pool.PoolQueue._class_scoped_projected_headroom(
        big_room, demand, cpu_count=8)["kept_for"] == "class_scoped_cpu_projected_cpu_cost"
    assert pool.PoolQueue._class_scoped_projected_headroom(big_room, demand, cpu_count=20) is None




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
    """Review 3 finding 1 end to end: 2-CPU class row beside a 6-CPU GPU row on 8 CPUs.

    Free tokens fit (8 - 2 >= 6), but the adaptive projected-cost gate charges
    the starter 2.5 CPUs: 2.5 + 6 > 8, so the GPU row's own decision would
    refuse it.  The exemption holds the class row; the GPU row claims first.
    """
    queue, publish, claim, keys, _seal = _fleet(tmp_path, monkeypatch, host, cpu_host="fresh")
    queue.withdraw(keys[GPU_KEY], reason="republish larger", by="test")
    gpu_key = publish(GPU_KEY, gpu=True, tags=["gb10"], resources={"cpu": 6, "gpu": 1, "mem_gb": 8})
    class_key = publish(CLASS_KEY, tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=1)
    first = claim()
    assert first is not None and first["action_key"] == gpu_key, first
    assert queue.item_path(pool.READY, class_key).exists()



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

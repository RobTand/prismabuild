"""The READY-GPU guard lets class-scoped CPU work through, beside the GPU row (#1589).

#1526: a GPU host leaves portable CPU-only rows READY while an eligible GPU row waits.  That has no
timer, and it deferred a genuine arm64 CPU qualification row (tagged for the GPU hosts' class, so no
host without a GPU can run it) behind every ready GPU row on both Sparks, for ever.  Now a row whose
required tags name something no host without a GPU offers is let past the guard when it fits beside
the eligible GPU row's own room against the free tokens at the token boundary; portable work still waits.

The class is read from every offer on file, stale ones included, so a brief x86 outage does not turn
portable work into class-scoped work.
"""
import json
import sys
import time

import pytest

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

    def publish(key, *, gpu=False, tags, resources, priority=0, **fields):
        queue.publish(action_key=key, cas_root=str(tmp_path / "cas"), checkout_root=str(tmp_path),
                      worker_script="worker.py", tags=tags, needs_gpu=gpu, priority=priority,
                      resources=resources, **fields)

    def claim():
        return queue.claim(tags=worker_tags, has_gpu=True, capacity=CAPACITY, cpu_tiers=TIERS)

    publish(GPU_KEY, gpu=True, tags=["gb10"], resources={"cpu": 2, "gpu": 1, "mem_gb": 8})
    return queue, publish, claim


@pytest.mark.parametrize("cpu_host", ["fresh", "stale"])
def test_a_class_scoped_cpu_row_is_not_held_behind_a_ready_gpu_row(tmp_path, monkeypatch, host, cpu_host):
    """The arm64 smoke: tagged for the GPU hosts' class, so no x86 box can ever run it."""
    queue, publish, claim = _fleet(tmp_path, monkeypatch, host, cpu_host=cpu_host)
    publish(CLASS_KEY, tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=1)
    claimed = claim()
    assert claimed is not None and claimed["action_key"] == CLASS_KEY, claimed
    assert queue.item_path(pool.READY, GPU_KEY).exists(), "the GPU row is still waiting"
    # ... and it is not crowded out: it claims beside the class-scoped row.
    again = claim()
    assert again is not None and again["action_key"] == GPU_KEY, again


@pytest.mark.parametrize("cpu_host", ["fresh", "stale"])
def test_portable_cpu_work_still_waits_for_the_ready_gpu_row(tmp_path, monkeypatch, host, cpu_host):
    """Every box carries these tags, so it is portable: x86 down for a while changes nothing."""
    queue, publish, claim = _fleet(tmp_path, monkeypatch, host, cpu_host=cpu_host)
    publish(PORTABLE_KEY, tags=[pb.INTERPRETER_TAG], resources={"cpu": 2, "mem_gb": 4}, priority=1,
            interpreter=sys.executable)
    first = claim()
    assert first is not None and first["action_key"] == GPU_KEY, first
    queue.finish(GPU_KEY, status="executed")
    second = claim()
    assert second is not None and second["action_key"] == PORTABLE_KEY, second


def test_a_class_scoped_row_that_does_not_fit_beside_the_gpu_reservation_still_waits(tmp_path, monkeypatch, host):
    """GPU safety: the eligible GPU row's reservation is held out of the total first."""
    queue, publish, claim = _fleet(tmp_path, monkeypatch, host, cpu_host="fresh")
    publish(CLASS_KEY, tags=["gb10"], resources={"cpu": 7, "mem_gb": 4}, priority=1)   # 8 - 2 = 6 CPUs beside it
    first = claim()
    assert first is not None and first["action_key"] == GPU_KEY, first
    assert queue.item_path(pool.READY, CLASS_KEY).exists()


def test_with_no_host_without_a_gpu_on_file_nothing_is_excluded(tmp_path, monkeypatch, host):
    """No evidence about the non-GPU side: the row is not guessed to be class-scoped."""
    queue, publish, claim = _fleet(tmp_path, monkeypatch, host, cpu_host="none")
    publish(CLASS_KEY, tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=1)
    first = claim()
    assert first is not None and first["action_key"] == GPU_KEY, first


def test_a_higher_priority_gpu_row_still_goes_first(tmp_path, monkeypatch, host):
    """The exemption leaves the priority order alone."""
    queue, publish, claim = _fleet(tmp_path, monkeypatch, host, cpu_host="fresh")
    queue.withdraw(GPU_KEY, reason="republish at a higher priority", by="test")
    publish(GPU_KEY, gpu=True, tags=["gb10"], resources={"cpu": 2, "gpu": 1, "mem_gb": 8}, priority=5)
    publish(CLASS_KEY, tags=["gb10"], resources={"cpu": 2, "mem_gb": 4}, priority=0)
    first = claim()
    assert first is not None and first["action_key"] == GPU_KEY, first
    second = claim()
    assert second is not None and second["action_key"] == CLASS_KEY, second


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
    queue, publish, claim = _fleet(tmp_path, monkeypatch, host, cpu_host="fresh")
    publish(INCUMBENT_KEY, tags=[host], resources={"cpu": 4, "mem_gb": 20}, priority=2)   # host-pinned: not held back
    assert claim()["action_key"] == INCUMBENT_KEY
    publish(CLASS_KEY, tags=["gb10"], resources={"cpu": 2, "mem_gb": 5}, priority=1)
    # free: cpu 4, mem 12.  After the class row: mem 7 < 8, the GPU row's room.
    claimed = claim()
    assert claimed is not None and claimed["action_key"] == GPU_KEY, claimed
    assert queue.item_path(pool.READY, CLASS_KEY).exists()


def test_two_class_scoped_rows_that_fit_alone_do_not_fit_together_beside_the_gpu_row(tmp_path, monkeypatch, host):
    """Cumulative: the second admission sees the tokens the first took."""
    queue, publish, claim = _fleet(tmp_path, monkeypatch, host, cpu_host="fresh")
    publish(CLASS_KEY, tags=["gb10"], resources={"cpu": 3, "mem_gb": 4}, priority=1)
    publish(CLASS_KEY_B, tags=["gb10"], resources={"cpu": 4, "mem_gb": 4}, priority=1)
    first = claim()
    assert first is not None and first["action_key"] == CLASS_KEY, first      # free cpu 8 -> 5 >= the room's 2
    second = claim()
    assert second is not None and second["action_key"] == GPU_KEY, second     # B would leave 1 < 2: held; the GPU row starts
    assert queue.item_path(pool.READY, CLASS_KEY_B).exists()


def _bare(tmp_path, monkeypatch):
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparky")
    return pool.PoolQueue(tmp_path / "queue")


def _decide(queue, *, identity=None, demand=None):
    gpu_row = {"action_key": GPU_KEY}
    return queue._class_scoped_beside_ready_gpu(
        {"action_key": CLASS_KEY, "cas_root": "/nowhere"}, gpu_row, ledger=None, total=CAPACITY,
        controller=None, gpu_controller=None, observed_images=None, container_class_policy=None,
        container_inventory=None, identity=identity, demand=demand or {"cpu": 2, "mem_gb": 4})


def test_a_measurement_row_is_not_let_past_the_guard(tmp_path, monkeypatch):
    """Its exclusivity contracts are its own: the identity's measurement flag holds it."""
    queue = _bare(tmp_path, monkeypatch)
    assert _decide(queue, identity=("identity", True)) == {"class_scoped": "measurement_row"}


def test_a_progress_governed_row_without_a_total_bound_is_not_let_past_the_guard(tmp_path, monkeypatch):
    queue = _bare(tmp_path, monkeypatch)
    monkeypatch.setattr(pool, "_declared_run_bound", lambda item, **kw: ("progress", None))
    assert _decide(queue) == {"class_scoped": "no_total_run_bound"}
    monkeypatch.setattr(pool, "_declared_run_bound",
                        lambda item, **kw: (_ for _ in ()).throw(pool.PoolContractError("unreadable")))
    assert _decide(queue) == {"class_scoped": "run_bound_unreadable"}


def test_a_gpu_room_that_cannot_be_established_holds_the_row(tmp_path, monkeypatch):
    """The GPU row's own claim facts (a clean fresh sample, its images, its residency) are not met."""
    queue = _bare(tmp_path, monkeypatch)
    monkeypatch.setattr(pool, "_declared_run_bound", lambda item, **kw: ("deadline", 600.0))
    monkeypatch.setattr(queue, "_ready_gpu_row_room", lambda *args, **kw: None)
    assert _decide(queue) == {"class_scoped": "gpu_room_unknown"}


def test_unreadable_free_tokens_hold_the_row(tmp_path, monkeypatch):
    queue = _bare(tmp_path, monkeypatch)
    monkeypatch.setattr(pool, "_declared_run_bound", lambda item, **kw: ("deadline", 600.0))
    monkeypatch.setattr(queue, "_ready_gpu_row_room",
                        lambda *args, **kw: {"action_key": GPU_KEY, "room": {"cpu": 2, "gpu": 1, "mem_gb": 8}})

    class Ledger:
        def available(self):
            raise OSError("estale")

    result = queue._class_scoped_beside_ready_gpu(
        {"action_key": CLASS_KEY, "cas_root": "/nowhere"}, {"action_key": GPU_KEY}, ledger=Ledger(),
        total=CAPACITY, controller=None, gpu_controller=None, observed_images=None,
        container_class_policy=None, container_inventory=None, identity=None, demand={"cpu": 2, "mem_gb": 4})
    assert result == {"class_scoped": "free_tokens_unreadable"}


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

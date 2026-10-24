import time

import pytest
from prismabuild import host_cap, pool
from test_capped_partial_host import capped

def test_private_claim_rechecks_cap_after_sampling(capped, tmp_path, monkeypatch):
    worker, _, _, membership = capped
    monkeypatch.setattr(
        worker.cpu_topology,
        "inherited_tiers",
        lambda: {"preferred": [4, 5, 6, 7], "fallback": [12, 13, 14, 15]},
    )
    queue = pool.PoolQueue(tmp_path / "private-queue")
    key = "d" * 64
    queue.publish(
        action_key=key,
        cas_root=queue.root / "cas",
        checkout_root=queue.root / "co",
        worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
    )
    parameters = worker.private_claim_parameters(queue)
    membership.write_text("0::/user.slice/worker\n")
    claimed = queue.claim(
        **parameters,
        cpu_sampler=lambda: {
            "sampled_unix": time.time(),
            "busy_cpus": 0.0,
            "psi_some": 0.0,
            "cpu_count": 8,
            "interval_s": 1.0,
        },
    )
    assert claimed is None
    assert queue.ledger().held_keys() == []


def test_worker_final_fence_observes_cap_change(capped, tmp_path, monkeypatch):
    from test_worker_loop_offers_what_is_free import _run

    _, _, _, membership = capped
    checked = []

    def inspect_claim_fence(self, **kwargs):
        fence = kwargs["admission_open"]
        assert fence()
        membership.write_text("0::/user.slice/worker\n")
        checked.append(fence())
        return None

    monkeypatch.setattr(pool.PoolQueue, "serve_once", inspect_claim_fence)
    _run(
        tmp_path,
        ["--class", "x86", "--mem-gb", "32", "--cpu-slots", "8", "--all-cores",
         "--poll-s", "0", "--once", "--assume-idle"],
        gpu_sample=None,
    )
    assert checked == [False]


@pytest.mark.parametrize("route", ["private", "ordinary", "simulation"])
@pytest.mark.parametrize("fault", [
    "membership", "cpuset", "memory", "memory_current", "host_headroom", "maintenance",
])
def test_a_change_at_the_last_transition_cannot_claim(capped, tmp_path, monkeypatch, route, fault):
    worker, _, parent, membership = capped
    monkeypatch.setattr(worker.cpu_topology, "inherited_tiers", lambda: {
        "preferred": [4, 5, 6, 7], "fallback": [12, 13, 14, 15]})
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "e" * 64
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1})
    if route == "private":
        parameters = worker.private_claim_parameters(queue)
    elif route == "simulation":
        parameters = worker.capped_simulation_parameters(queue, {"cpu": 80, "mem_gb": 214})
        assert parameters["capacity"] == {"cpu": 8, "mem_gb": 29}
    else:
        args = worker.build_parser().parse_args(["--assume-idle"])
        _, capacity, _ = worker.observed_claim_capacity(
            queue, args=args, base_declared={"cpu": 8, "mem_gb": 32},
            observer=None, gpu_sample=None, known_physical_gpus=0)
        parameters = {"capacity": capacity,
                      "admission_open": worker.claim_capacity_fence(queue, capacity)}
    original = queue._rename_and_commit_claim
    reached = []

    def change_then_commit(tiers, scan):
        assert parameters["admission_open"]()
        reached.append(True)
        if fault == "membership":
            membership.write_text("0::/user.slice/worker\n")
        elif fault == "cpuset":
            (parent / "cpuset.cpus.effective").write_text("0-15")
        elif fault == "memory":
            (parent / "memory.max").write_text("max")
        elif fault == "memory_current":
            (parent / "memory.current").write_text(str(20 * host_cap.GIB))
        elif fault == "host_headroom":
            monkeypatch.setattr(worker.box_capacity, "mem_available_gb", lambda: 10)
        else:
            monkeypatch.setattr(worker.role_session, "read_maintenance_gate", lambda: {"draining": True})
        if fault in ("memory_current", "host_headroom"):
            detail = {}
            fresh = host_cap.recheck_host_cap(parameters["capacity"], detail=detail)
            assert "host_cap_refusal" not in detail
            # The candidate still fits. Refusal must come from the changed
            # envelope, rather than a zero offer or failed containment check.
            assert 1 <= fresh["mem_gb"] < parameters["capacity"]["mem_gb"]
        return original(tiers, scan)

    monkeypatch.setattr(queue, "_rename_and_commit_claim", change_then_commit)
    assert queue.claim(**parameters, cpu_sampler=lambda: {
        "sampled_unix": time.time(), "busy_cpus": 0., "psi_some": 0.,
        "cpu_count": 8, "interval_s": 1.}) is None
    assert reached == [True]
    assert not queue.ledger().held_keys()
    assert queue.records.item_path(pool.record_store.READY, key).exists()
    assert not queue.records.item_path(pool.record_store.INTENT, key).exists()
    assert not queue.records.item_path(pool.record_store.CLAIMED, key).exists()

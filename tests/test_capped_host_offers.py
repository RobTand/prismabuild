"""Property checks on the announcement written by a complete worker poll."""
import socket

import pytest

from prismabuild import host_cap, pool
from test_capped_partial_host import capped
from test_worker_loop_offers_what_is_free import _offer, _run


@pytest.mark.parametrize("cpu,memory", [(1, 16), (8, 32), (80, 214)])
@pytest.mark.parametrize("used", [0, 20, 31])
@pytest.mark.parametrize("headroom", [10, 200])
@pytest.mark.parametrize("idle", [False, True])
def test_a_capped_worker_never_announces_more_than_its_cap(capped, tmp_path, monkeypatch, cpu, memory, used, headroom, idle):
    _, _, parent, _ = capped
    (parent / "memory.current").write_text(str(used * host_cap.GIB))
    monkeypatch.setattr(pool.PoolQueue, "serve_once", lambda *a, **k: None)
    argv = ["--class", "x86", "--mem-gb", str(memory), "--cpu-slots", str(cpu),
            "--all-cores", "--poll-s", "0", "--once"]
    if idle:
        argv.append("--assume-idle")
    queue = _run(tmp_path, argv, gpu_sample=None, mem_gb=headroom)
    offer = _offer(queue, socket.gethostname())
    for field in ("capacity", "observed_capacity"):
        assert 0 <= offer[field]["cpu"] <= min(cpu, 8)
        assert 0 <= offer[field]["mem_gb"] <= min(memory, 32)
    roof = min(32, max(0, 32 - used - 1), max(0, headroom - 8))
    assert offer["observed_capacity"]["mem_gb"] <= roof
    assert "host_cap_refusal" not in offer["observed_detail"]


@pytest.mark.parametrize("cpu,memory", [(1, 16), (8, 32), (80, 214)])
def test_a_capped_worker_starts_draining_with_a_capped_declaration(capped, tmp_path, monkeypatch, cpu, memory):
    worker, _, _, _ = capped
    monkeypatch.setattr(worker.box_capacity, "worker_loops", lambda: [])
    monkeypatch.setattr(worker.box_capacity, "ipv4_addresses", lambda: None)
    queue = _run(
        tmp_path,
        ["--class", "x86", "--mem-gb", str(memory), "--cpu-slots", str(cpu),
         "--all-cores", "--poll-s", "0", "--once"],
        gpu_sample=None, draining=True,
    )
    offer = _offer(queue, socket.gethostname())
    assert offer["state"] == "draining"
    assert offer["capacity"]["cpu"] == min(cpu, 8)
    assert offer["capacity"]["mem_gb"] == min(memory, 32)
    assert all(value == 0 for value in offer["observed_capacity"].values())

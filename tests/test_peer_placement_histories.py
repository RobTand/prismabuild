"""Real publication/claim histories with synthetic clock and offer inputs."""
from __future__ import annotations

import json
from pathlib import Path
import socket
import sys
import tempfile
import time

import pytest

from prismabuild import pool


LOCAL, PEER = "placement-local", "placement-peer"
CAPACITY = {"cpu": 4, "mem_gb": 4, "gpu": 1}
TIERS = {"preferred": [0, 1, 2, 3], "fallback": []}
RULES = ("cpu_host", "cross_resource", "preferred_cpu")
OBSERVATIONS = ("fresh", "partial", "stale", "zero", "map_mismatch", "ledger_failure")
SCRATCH_OBSERVATIONS = (*OBSERVATIONS, "replacement")


def _history(root, monkeypatch, rule, observation, *, mixed=False, ticks=(10., 31.)):
    monkeypatch.setattr(socket, "gethostname", lambda: LOCAL)
    monkeypatch.setattr(time, "time", lambda: 1_800_000_000.0)
    clock = [10.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    queue = pool.PoolQueue(root / "queue")
    local_tiers = ({"preferred": [0], "fallback": [1, 2, 3]}
                   if rule == "preferred_cpu" else TIERS)
    peer_gpu = rule == "cross_resource"
    for host, has_gpu, tiers in ((LOCAL, rule != "preferred_cpu", local_tiers),
                                 (PEER, peer_gpu, TIERS)):
        capacity = dict(CAPACITY)
        if not has_gpu:
            capacity.pop("gpu")
        ledger = queue.ledger(host)
        ledger.configure_cpu_tiers(tiers)
        ledger.ensure_capacity(capacity)
        observed = dict(capacity)
        age = 10 if host == PEER and observation == "stale" else 0
        if host == PEER and observation == "partial":
            observed.pop("mem_gb")
        if host == PEER and observation == "zero":
            observed["mem_gb"] = 0
        power = .9 if host == LOCAL and (rule == "cross_resource" or mixed) else .05
        queue.announce(host=host, tags=[], has_gpu=has_gpu, capacity=capacity,
                       observed_capacity=observed, cpu_tiers=tiers,
                       observed_detail={"observed_unix": time.time() - age, "load1": 0.,
                                        "gpu_power_fraction": power,
                                        "gpu_power_sampled_unix": time.time() - age})
    if observation == "map_mismatch":
        pool._write_json_atomic(queue.ledger(PEER).base / "cpu-map.json",
                                {"preferred": [4, 5, 6, 7], "fallback": []})
    if observation == "ledger_failure":
        original = pool.ResourceLedger.available

        def unavailable(ledger):
            if ledger.base == queue.ledger(PEER).base:
                raise OSError("injected peer ledger read failure")
            return original(ledger)

        monkeypatch.setattr(pool.ResourceLedger, "available", unavailable)
    key = "a" * 64
    queue.publish(action_key=key, cas_root=str(root / "cas"), checkout_root=str(root),
                  worker_script="worker.py", resources={"cpu": 3, "mem_gb": 1})
    other_key = "c" * 64
    if mixed:
        queue.publish(action_key=other_key, cas_root=str(root / "cas"), checkout_root=str(root),
                      worker_script="worker.py", resources={"cpu": 3, "mem_gb": 1})
    steps = []
    for tick in ticks:
        clock[0] = tick
        try:
            claimed = queue.claim(capacity=CAPACITY, cpu_tiers=local_tiers,
                                  has_gpu=rule != "preferred_cpu")
            records = queue.latest_denials({key}).get(key) or []
            denial = next((record for record in records if record.get("host") == LOCAL), {})
            evidence = denial.get("evidence", {})
            remote = evidence.get("cpu_host", evidence.get("remote_offer", {}))
            steps.append({"claimed": bool(claimed), "reason": denial.get("reason"),
                          "remote": remote,
                          "ready": queue.records.item_path(pool.record_store.READY, key).exists(),
                          "held": bool(queue.ledger().held_keys()),
                          "passes": queue.records.passes_path(key).exists()})
            if mixed:
                others = queue.latest_denials({other_key}).get(other_key) or []
                steps[-1]["other_reason"] = next((record.get("reason") for record in others
                                                   if record.get("host") == LOCAL), None)
        except OSError as exc:
            steps.append({"error": type(exc).__name__, "message": str(exc)})
            break
        if claimed:
            break
    return steps


@pytest.mark.parametrize("rule,observation", [
    pytest.param(rule, observation, id=f"{observation}-{rule}")
    for observation in OBSERVATIONS for rule in RULES
    if observation != "ledger_failure" or rule == "cpu_host"
])
def test_real_claim_preserves_rule_evidence_requirements(tmp_path, monkeypatch, rule, observation):
    steps = _history(tmp_path, monkeypatch, rule, observation)
    yields = observation == "fresh" or (
        rule == "preferred_cpu" and observation in {"partial", "stale"})
    assert steps[0]["claimed"] is not yields, steps
    if yields:
        expected = {"cpu_host": "deferred_for_cpu_only_host",
                    "cross_resource": "deferred_for_cross_resource_placement",
                    "preferred_cpu": "deferred_for_preferred_cpu"}[rule]
        assert steps[0]["reason"] == expected
        assert steps[0]["remote"]["available"]["cpu"] == 4
        assert steps[0]["ready"] and not steps[0]["held"] and not steps[0]["passes"]
        assert steps[1]["claimed"] is (rule != "cpu_host")
    else:
        assert not steps[0]["ready"] and steps[0]["held"]


@pytest.mark.parametrize("rule", ("cross_resource", "preferred_cpu"))
def test_characterization_timer_peer_read_error_aborts_claim(tmp_path, monkeypatch, rule):
    """Retained behavior. Follow-up: treat an unreadable timer peer as no candidate."""
    assert _history(tmp_path, monkeypatch, rule, "ledger_failure") == [
        {"error": "OSError", "message": "injected peer ledger read failure"}]


def _scratch_history(root, monkeypatch, observation):
    from test_local_scratch_measured_placement import FAST, SLOW, ScratchFleet

    fleet = ScratchFleet(root, monkeypatch)
    fleet.clock[0] = 1_800_000_000.0
    for host in (SLOW, FAST):
        fleet.profiles[host] = [fleet.profile(host, write_elapsed_s=20. if host == SLOW else 2.)]
        fleet.announce(host)
    key = fleet.publish()
    peer_path = fleet.queue.root / pool.record_store.WORKERS / f"{FAST}.json"
    if observation in {"partial", "stale", "zero"}:
        offer = pool.record_store.read_queue_record(peer_path)
        if observation == "partial":
            offer["observed_capacity"].pop("mem_gb")
        elif observation == "zero":
            offer["observed_capacity"]["mem_gb"] = 0
        else:
            offer["observed_detail"]["observed_unix"] -= 10
        pool._write_json_atomic(peer_path, offer)
    if observation == "map_mismatch":
        offer = pool.record_store.read_queue_record(peer_path)
        fleet.queue.announce(
            host=FAST, tags=offer["tags"], has_gpu=True, capacity=offer["capacity"],
            observed_capacity=offer["observed_capacity"], observed_detail=offer["observed_detail"],
            cpu_tiers={"preferred": [4, 5, 6, 7], "fallback": []})
    if observation == "ledger_failure":
        original = pool.ResourceLedger.available

        def unavailable(ledger):
            if ledger.base == fleet.queue.ledger(FAST).base:
                raise OSError("injected scratch peer ledger read failure")
            return original(ledger)

        monkeypatch.setattr(pool.ResourceLedger, "available", unavailable)
    replacements = []
    if observation == "replacement":
        original = pool.ResourceLedger.available

        def replace_after_read(ledger):
            available = original(ledger)
            if ledger.base == fleet.queue.ledger(FAST).base and not replacements:
                offer = pool.record_store.read_queue_record(peer_path)
                offer["observed_capacity"]["mem_gb"] = 0
                pool._write_json_atomic(peer_path, offer)
                replacements.append(True)
            return available

        monkeypatch.setattr(pool.ResourceLedger, "available", replace_after_read)
    claimed = fleet.claim(SLOW)
    records = fleet.queue.latest_denials({key}).get(key) or []
    denial = next((record for record in records if record.get("host") == SLOW), {})
    placement = (claimed or {}).get("local_scratch_io_placement", denial.get("evidence", {}))
    candidates = [{"host": candidate["host"], "fit": candidate["fit"],
                   "exclusion": candidate["exclusion"]}
                  for candidate in placement.get("candidates", [])]
    return {"claimed": bool(claimed), "reason": denial.get("reason"),
            "yield_to": placement.get("yield_to"), "candidates": candidates,
            "replacement_read": bool(replacements)}


@pytest.mark.parametrize("observation", SCRATCH_OBSERVATIONS)
def test_scratch_measured_fit_and_replacement_offer(tmp_path, monkeypatch, observation):
    history = _scratch_history(tmp_path, monkeypatch, observation)
    assert history["claimed"] is (observation != "fresh"), history
    if observation == "fresh":
        assert history["reason"] == "deferred_for_local_scratch_io"
    if observation in {"map_mismatch", "ledger_failure"}:
        assert any(candidate["exclusion"] == "host_view_unknown"
                   for candidate in history["candidates"])
    if observation == "replacement":
        assert history["replacement_read"]
        assert any(candidate["exclusion"] == "full_demand_does_not_fit"
                   for candidate in history["candidates"])


def test_peer_observation_is_reused_across_cpu_host_and_timer_queries(tmp_path, monkeypatch):
    reads = []
    original = pool.ResourceLedger.available

    def observe(ledger):
        if ledger.base.name == PEER:
            reads.append(ledger.base)
        return original(ledger)

    monkeypatch.setattr(pool.ResourceLedger, "available", observe)
    _history(tmp_path, monkeypatch, "cpu_host", "fresh", mixed=True, ticks=(10.,))
    assert len(reads) == 1


def test_timer_only_yields_do_not_charge_peer_capacity(tmp_path, monkeypatch):
    steps = _history(tmp_path, monkeypatch, "cross_resource", "fresh", mixed=True, ticks=(10.,))
    assert not steps[0]["claimed"] and not steps[0]["held"]
    assert steps[0]["reason"] == steps[0]["other_reason"] == "deferred_for_cross_resource_placement"


def _replay_origins():
    source = Path(__file__).resolve().parents[1]
    origins = {}
    for name, module in tuple(sys.modules.items()):
        if name == "prismabuild" or name.startswith("prismabuild."):
            path = Path(module.__file__).resolve()
            if not path.is_relative_to(source / "src"):
                raise RuntimeError(f"replay import outside selected source: {name}={path}")
            origins[name] = path.relative_to(source).as_posix()
    for name, relative in (
        ("test_local_scratch_measured_placement", "tests/test_local_scratch_measured_placement.py"),
        ("pbrun", "tools/fleet/pbrun.py"), ("worker_loop", "tools/fleet/worker_loop.py"),
    ):
        if name in sys.modules:
            path = Path(sys.modules[name].__file__).resolve()
            if path != source / relative:
                raise RuntimeError(f"replay fixture import outside selected source: {name}={path}")
            origins[name] = relative
    return {"source": str(source), "modules": origins}


def replay():
    _replay_origins()
    histories = {}
    for rule in RULES:
        for observation in OBSERVATIONS:
            with tempfile.TemporaryDirectory() as root, pytest.MonkeyPatch.context() as patch:
                histories[f"{rule}/{observation}"] = _history(Path(root), patch, rule, observation)
    for observation in SCRATCH_OBSERVATIONS:
        with tempfile.TemporaryDirectory() as root, pytest.MonkeyPatch.context() as patch:
            histories[f"scratch/{observation}"] = _scratch_history(Path(root), patch, observation)
    for rule in ("cpu_host", "cross_resource"):
        with tempfile.TemporaryDirectory() as root, pytest.MonkeyPatch.context() as patch:
            histories[f"mixed/{rule}"] = _history(Path(root), patch, rule, "fresh", mixed=True)
    return {"histories": histories, "origins": _replay_origins()}


if __name__ == "__main__":
    print(json.dumps(replay()))

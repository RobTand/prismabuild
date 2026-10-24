"""A foreign-starved measurement must eventually let ordinary work claim.

Only adaptive CPU measurement verdicts and the Unix clock are injected. Sealed
requests, elections, census reads, claim ordering, denials and token settlement
are real, in a private CPU-only fleet with one host.
"""
from __future__ import annotations

import platform
import sys

import pytest

from fleet_world import Clock, FleetWorld
from prismabuild import adaptive_cpu, core, pool
from prismabuild import _measurement_reservation as reservation


@pytest.mark.parametrize("measurement_count", [1, 2], ids=["one-measurement", "two-measurements"])
def test_foreign_starvation_cannot_keep_an_empty_host_reserved(
    tmp_path, monkeypatch, measurement_count,
):
    clock = Clock(now=2_000_000.0)
    clock.install(monkeypatch)
    FleetWorld.use_host(monkeypatch, "measurement-host")
    queue = FleetWorld(tmp_path, clock=clock).queue
    capacity = {"cpu": 4, "mem_gb": 8}
    tiers = {"preferred": [0, 1, 2, 3], "fallback": []}
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text("print('fixture')\n")
    cas = core.PrismaBuildCAS(tmp_path / "cas")

    def publish(name, *, measurement=False, priority=-10):
        clock.advance(0.001)
        action = core.seal_action({
            "schema": core.ACTION_SCHEMA_V2,
            "task": {
                "definition_id": "tests/measurement-reservation-starved-2267",
                "definition_version": "v1",
                "task_class": "measurement" if measurement else "generation",
                "determinism": "deterministic", "artifact_family": "generic",
                "artifact_kind": "generic", "argv": [sys.executable, "task.py"],
                "working_directory": ".", "result_path": name,
            },
            "inputs": [], "code_closure": core.build_code_closure(checkout, ["task.py"]),
            "params": {"execution_timeout_s": 1200},
            "environment": {
                "variables": {},
                "toolchain": {
                    **core.executable_toolchain_contract(sys.executable),
                    "system": platform.system(), "machine": platform.machine(),
                    "libc": "-".join(platform.libc_ver()),
                } if measurement else {},
            },
            "execution_scope": {
                "portability": "host_class_keyed" if measurement else "portable",
                "platform_key": None, "host_class": "fixture-cpu" if measurement else None,
            },
        })
        cas.publish_action_request(action)
        key = action["action_key"]
        queue.publish(
            action_key=key, cas_root=str(cas.root), checkout_root=str(checkout),
            worker_script="worker.py", resources={"cpu": 1, "mem_gb": 1}, priority=priority,
        )
        return key

    def sample():
        return {
            "sampled_unix": clock.now, "cpu_count": 4, "interval_s": 1.0,
            "busy_cpus": 0.0, "psi_some": 0.0,
            "per_cpu_busy": {str(cpu): 0.0 for cpu in range(4)},
            "foreign_per_cpu_busy": {str(cpu): 0.0 for cpu in range(4)},
        }

    def claim(active, **kwargs):
        return active.claim(
            capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True,
            cpu_sampler=sample, **kwargs,
        )

    def denial(key):
        path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
        records = adaptive_cpu.read_json(path).get("records", {})
        return next(row for row in records.values() if row["action_key"] == key)

    holder = publish("incumbent")
    assert claim(queue)["action_key"] == holder
    foreign = False
    real_decision = adaptive_cpu.Controller.decision

    def measurement_verdict(controller, item, demand, **kwargs):
        if not adaptive_cpu.action_identity(item)[1]:
            return real_decision(controller, item, demand, **kwargs)
        observed = sample()
        if foreign:
            observed.update(busy_cpus=0.4)
            observed["per_cpu_busy"]["1"] = 0.4
            observed["foreign_per_cpu_busy"]["1"] = 0.4
        controller.last_decision = {
            "reason": "measurement_foreign_ambient" if foreign else "measurement_holder",
            "fresh": True, "sample": observed,
            **({"foreign_cpus": [1], "per_cpu_foreign_busy": {"1": 0.4}}
               if foreign else {
                   "holder": holder, "isolated_by": None,
                   "measurement_pool_drain": {"foreign_clear": True, "sampled_unix": clock.now},
               }),
        }
        return None

    monkeypatch.setattr(adaptive_cpu.Controller, "decision", measurement_verdict)
    measurements = [publish(f"measurement-{index}", measurement=True, priority=0)
                    for index in range(measurement_count)]
    for key in measurements:
        # Give each same-priority measurement its own genuine election pass.
        # No selector or drain history is planted by the fixture.
        row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, key))
        assert claim(queue, ready=[row]) is None
        chosen = reservation.selection(pool.record_store.read_document(queue.records.passes_path(key)))
        assert chosen is not None and chosen["host"] == "measurement-host"

    foreign = True
    queue.finish(holder, status="executed")
    assert queue.ledger().held_keys() == []
    ordinary = publish("ordinary")
    started = clock.now
    observations = []
    claimed = None
    # These are simulated scheduling opportunities, never real-time waits.
    for offset in (1, pool.WITHHOLD_CEILING_S + 1, 10 * pool.WITHHOLD_CEILING_S + 1, 86400):
        clock.now = started + offset
        claimed = claim(pool.PoolQueue(queue.root))
        if claimed is not None:
            break
        waiting = [denial(key) for key in measurements]
        ordinary_denial = denial(ordinary)
        observations.append({
            "simulated_age_s": offset,
            "measurement_reasons": [row["reason"] for row in waiting],
            "starved_why": [row["evidence"].get("starved", {}).get("why") for row in waiting],
            "ordinary_reason": ordinary_denial["reason"],
            "held": queue.ledger().held_keys(),
        })

    assert claimed is not None and claimed["action_key"] == ordinary, (
        "ordinary work stayed deferred on an empty host behind foreign-starved measurements",
        observations,
    )

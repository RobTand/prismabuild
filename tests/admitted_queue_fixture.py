"""Explicit simulated admission for private queue state-machine fixtures.

Opt-in at a family-owned factory, never autouse or a production policy. Defaults
are fixed declared test inputs; explicit None/empty/zero reaches the real API.
All transitions, decisions, ledgers, reservations and execution remain PoolQueue's.
"""
from __future__ import annotations

from collections.abc import Mapping

from prismabuild import pool, host_cap

_OMITTED = object()


def simulated_capacity(value):
    """Mint explicit fixture observations through the real uncapped clamp."""
    if value is None:
        return None
    if type(value) is host_cap.CappedCapacity:
        return host_cap.require_capped_capacity(value)
    return host_cap.enforce_host_cap(value, None, {})


def simulated_claim_parameters(parameters, *, queue):
    """Convert only an explicitly supplied test capacity, at the call."""
    result = dict(parameters)
    if "capacity" in result:
        result["capacity"] = simulated_capacity(result["capacity"])
        if result["capacity"] is not None and "admission_open" not in result:
            owner = queue.queue if isinstance(queue, AdmittedQueueFixture) else queue
            result["admission_open"] = host_cap.prepare_claim_fence(owner, result["capacity"])
    return result


def simulated_admission(queue, capacity):
    """Supply a queue-bound fence with one explicit simulated observation."""
    return simulated_claim_parameters({"capacity": capacity}, queue=queue)


class AdmittedQueueFixture:
    def __init__(self, queue: pool.PoolQueue, *, capacity: Mapping[str, int],
                 default_demand: Mapping[str, int]) -> None:
        if not capacity or not default_demand:
            raise ValueError("simulated admission requires fixed positive declared inputs")
        if (any(type(value) is not int or value < 0 for value in capacity.values())
                or any(type(value) is not int or value < 0 for value in default_demand.values())
                or not any(value > 0 for value in capacity.values())
                or not any(value > 0 for value in default_demand.values())):
            raise ValueError("invalid simulated admission inputs")
        object.__setattr__(self, "queue", queue)
        object.__setattr__(self, "capacity", dict(capacity))
        object.__setattr__(self, "default_demand", dict(default_demand))

    def __getattr__(self, name):
        return getattr(self.queue, name)

    def __setattr__(self, name, value):
        # Fixture fault injection still targets the real owner. No copied
        # transition/ledger implementation or separate scheduler state.
        setattr(self.queue, name, value)

    def publish(self, *args, resources=_OMITTED, **kwargs):
        declared = dict(self.default_demand) if resources is _OMITTED else resources
        return self.queue.publish(*args, resources=declared, **kwargs)

    def claim(self, *args, capacity=_OMITTED, **kwargs):
        observed = simulated_capacity(self.capacity) if capacity is _OMITTED else capacity
        return self.queue.claim(*args, **simulated_claim_parameters({"capacity": observed, **kwargs}, queue=self.queue))

    def serve_once(self, *args, capacity=_OMITTED, **kwargs):
        observed = simulated_capacity(self.capacity) if capacity is _OMITTED else capacity
        return self.queue.serve_once(*args, **simulated_claim_parameters({"capacity": observed, **kwargs}, queue=self.queue))

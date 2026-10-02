"""Explicit simulated admission for private queue state-machine fixtures.

Opt-in at a family-owned factory, never autouse or a production policy. Defaults
are fixed declared test inputs; explicit None/empty/zero reaches the real API.
All transitions, decisions, ledgers, reservations and execution remain PoolQueue's.
"""
from __future__ import annotations

from collections.abc import Mapping

from prismabuild import pool

_OMITTED = object()


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
        observed = dict(self.capacity) if capacity is _OMITTED else capacity
        return self.queue.claim(*args, capacity=observed, **kwargs)

    def serve_once(self, *args, capacity=_OMITTED, **kwargs):
        observed = dict(self.capacity) if capacity is _OMITTED else capacity
        return self.queue.serve_once(*args, capacity=observed, **kwargs)

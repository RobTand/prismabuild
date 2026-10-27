"""Admission policy for work borrowing a gang's elected capacity."""
from __future__ import annotations

from . import _gang


def preemptible_candidate(queue, item) -> bool:
    return (item.get("priority") == -10 and item.get("gang") is None
            and _gang.backfill_enabled() and queue._preemption_eligible(item))

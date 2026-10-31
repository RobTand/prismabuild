"""Read host-local claim evidence independently of admission policy."""
from __future__ import annotations

from prismabuild import adaptive_cpu, pool


def latest_claim_denial(queue: pool.PoolQueue, key: str) -> dict[str, object] | None:
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    matching = [entry for entry in records.values()
                if isinstance(entry, dict) and entry.get("action_key") == key]
    if not matching:
        return None
    return max(matching, key=lambda entry: float(entry.get("denied_unix", 0.0)))

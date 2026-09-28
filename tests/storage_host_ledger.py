"""The storage host's worker ledger, minted as that worker mints it (#1258).

Since #1222 a RAM fill takes host ``mem_gb`` tokens beside its tier tokens
in the storage host's own ledger (#1245 review B1), and that ledger is
minted only by the storage host's worker loop, on its claim path, at its
offer.  A fixture that drives the RAM window without that worker has no
host pool, so every RAM fence is host-short.  This helper mints it the way
the worker does: at the worker's offer, whose ``mem_gb`` is the RAM
policy's measured roof read by ``box_capacity.ram_policy_mem_roof`` over
the storage host's recorded facts -- the same offer the shape gate uses.
"""

from __future__ import annotations

from pathlib import Path
import sys

_FLEET = Path(__file__).resolve().parents[1] / "tools" / "fleet"
if str(_FLEET) not in sys.path:
    sys.path.insert(0, str(_FLEET))

import shape_gate  # noqa: E402
import tier_loop  # noqa: E402

from prismabuild import pool  # noqa: E402

#: The storage host every RAM-tier fixture names.
STORAGE_HOST = str(shape_gate.HOST_PROFILE["host"])


def mint_storage_host_ledger(queue: pool.PoolQueue,
                             facts_dir: Path | None = None) -> dict[str, int]:
    """Mint the storage host's ledger at its worker's offer; return the offer."""

    policy = tier_loop.load_ram_policy()
    assert policy is not None, "fixture: the RAM tier policy must load"
    offer = shape_gate.storage_host_worker_offer(
        facts_dir if facts_dir is not None
        else queue.root.parent / "storage-host-worker",
        policy=policy)
    queue.ledger(STORAGE_HOST).ensure_capacity(offer)
    return offer

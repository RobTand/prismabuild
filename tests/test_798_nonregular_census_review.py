"""Independent acceptance checks: lost metadata shape is unknown ownership."""
from __future__ import annotations

import os
import json

import pytest

from test_a_produced_namespace_is_not_a_consumer import (
    PRODUCED_MOVER,
    LEGACY_MOVER,
    LEGACY_CONSUMER,
    TIER,
    _reconcile_receipt,
    _sweep,
    world,
)


@pytest.mark.parametrize("shape", ["fragment_directory", "fragment_fifo", "namespace_file"])
@pytest.mark.parametrize("layout", ["produced", "legacy"])
def test_nonregular_fragment_state_retains_live_staged_bytes(world, shape, layout):
    mover = PRODUCED_MOVER if layout == "produced" else LEGACY_MOVER
    namespace = (world.ns_dir if layout == "produced" else
                 world.queue.root / "residency" / LEGACY_CONSUMER)
    staged = world.produced_path if layout == "produced" else world.legacy_path
    fragment = namespace / f"{mover}.json"
    fragment.unlink()
    if shape == "fragment_directory":
        fragment.mkdir()
    elif shape == "fragment_fifo":
        os.mkfifo(fragment)
    else:
        namespace.rmdir()
        namespace.write_text("corrupt namespace\n")
    held = world.queue.tier_ledger(TIER).holder_tokens(mover)
    events = _sweep(world.queue, world.stage)
    receipt = _reconcile_receipt(events)
    observed = {
        "shape": shape,
        "layout": layout,
        "receipt": receipt,
        "staged_bytes_exist": staged.exists(),
        "held_before": held,
        "held_after": world.queue.tier_ledger(TIER).holder_tokens(mover),
    }
    print("OBSERVATION", json.dumps(observed, sort_keys=True))
    assert receipt is not None and receipt["complete"] is False, observed
    assert receipt["skipped"] == "attribution_unreadable", observed
    assert receipt["entries_deleted"] == 0 and staged.exists(), observed
    assert observed["held_after"] == held, observed

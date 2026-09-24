"""A shared range's registration follows the tier's announcement (#1026).

A stage mover's argv names the interpreter and the tool root the stage tier
announced when the mover was sealed (``movement_actions.movement_tools``).
The tool root is one runtime generation's directory.  The registry
(``residency_plan.register_shared_range``) hands every later consumer of a
range the first registrant's mover row, so without a check a registration
would pin its first registrant's generation for as long as it lives: every
later consumer, sealed after a publish, would run the old generation's
mover.

A registration records the announcement it was sealed against and is
reused only by a submission that reads the same one, or while its mover is
live (queued or running), because sealing a second mover then would copy
the range twice.  Otherwise the submission seals the range afresh and the
old registration is retired under ``.stale``.  The fresh mover's window
adopts the old mover's resident range rather than copying it again.

Everything runs on ``tmp_path`` queues and stage roots (#628).
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import pool, residency_map, residency_plan  # noqa: E402
import tier_loop  # noqa: E402
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _claim, _fixture_queue, _land, _publish_consumer)
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    GIB, PHASE_GIB, TIER, _hexkey, _tier_record)
from test_n_consumers_of_one_range_share_one_copy import (  # noqa: E402
    FIRST, MANIFEST, SECOND, TOOLS_A, _announce, _files, _legs, _namespace,
    _seal_for, _shared_plan)

THIRD = _hexkey("1026third-generation")
#: The next runtime generation's tool root, as the tier loop announces it.
GEN_B_ROOT = "/gen-b/tools/fleet"
TOOLS_B = {"mover_python": "/gen-b/venv/bin/python",
           "mover_tools_root": GEN_B_ROOT}


def _mover_rows(plan: dict[str, object]) -> list[dict[str, object]]:
    """Every stage leg's mover row, in the order :func:`_legs` lists them."""

    return [dict(leg["mover_row"])
            for phase in plan["phases"]                        # type: ignore[union-attr]
            for leg in (phase.get("stage_chunks") or [phase])]


def _queued(queue: pool.PoolQueue, row: dict[str, object]) -> None:
    """``row``'s mover is live: its record sits in ``ready/``, which is all
    ``residency_plan.live_state`` reads."""

    path = queue.item_path(pool.READY, str(row["action_key"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(row))


def test_a_registration_from_another_tool_root_is_reused_only_while_live(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The first consumer seals every range under the tier loop's own tool
    root.  A publish moves the root, and the first consumer's lead mover is
    queued when the second consumer seals: the second reuses only that
    lead.  Every other range is sealed afresh, its argv names the new root,
    the old registration is retired under ``.stale``, and the old mover's
    index still reads, for the plan that names it.  Once the lead has run,
    a third consumer reseals it too, and reuses the second one's movers."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    first, first_cas, digest, stage_tier = _seal_for(tmp_path, queue, FIRST)
    first_legs = _legs(first["plan"])
    assert len(first_legs) > 1, "one leg cannot show reuse beside a reseal"
    for mover, _egress, _start, _end in first_legs:
        command = first_cas.actions[mover]["params"]["command"]
        assert command[1].startswith(tier_loop.MOVER_TOOLS_ROOT + "/"), command

    monkeypatch.setattr(tier_loop, "MOVER_TOOLS_ROOT", GEN_B_ROOT)
    _announce(tmp_path, queue)
    lead = _mover_rows(first["plan"])[0]
    _queued(queue, lead)
    second, second_cas, _digest, _tier = _seal_for(tmp_path, queue, SECOND,
                                                   announce=False)
    second_legs = _legs(second["plan"])

    assert [leg[2:] for leg in second_legs] == [leg[2:] for leg in first_legs]
    # The live lead is reused: sealing another would copy the range twice.
    assert second_legs[0][0] == first_legs[0][0]
    assert first_legs[0][0] not in second_cas.actions
    for (old, _e1, start, end), (new, _e2, _s, _t) in zip(
            first_legs[1:], second_legs[1:]):
        assert new != old, "a registration from another tool root was reused"
        command = second_cas.actions[new]["params"]["command"]
        assert command[1].startswith(GEN_B_ROOT + "/"), command
        namespace = residency_plan.share_namespace(digest, stage_tier, start, end)
        assert command[command.index("--consumer-action-key") + 1] == namespace
        record = residency_plan.read_shared_range(queue, namespace)
        assert record is not None
        assert record["mover_action_key"] == new
        assert record["sealed_against"]["mover_tools_root"] == GEN_B_ROOT
        assert record["replaces"] == {"mover_action_key": old, "reason": "stale"}
        retired = residency_plan.shared_range_path(queue, namespace).with_name(
            f"{namespace}.{old}.stale")
        assert json.loads(retired.read_text())["mover_action_key"] == old
        # The first consumer's frozen plan still names the old mover; its
        # fan-out, interest and egress find the range through this index.
        assert residency_plan.share_namespace_of(queue, old) == namespace
        assert residency_plan.share_namespace_of(queue, new) == namespace

    # The lead ran: its record leaves ``ready/`` for ``done/``.
    lead_key = str(lead["action_key"])
    queue.item_path(pool.READY, lead_key).unlink()
    done = queue.item_path(pool.DONE, lead_key)
    done.parent.mkdir(parents=True, exist_ok=True)
    done.write_text(json.dumps({**lead, "status": "done"}))

    third, third_cas, _digest, _tier = _seal_for(tmp_path, queue, THIRD)
    third_legs = _legs(third["plan"])
    assert [leg[0] for leg in third_legs[1:]] == [
        leg[0] for leg in second_legs[1:]]
    assert not set(third_cas.actions) & {leg[0] for leg in second_legs[1:]}
    fresh = third_legs[0][0]
    assert fresh != lead_key
    command = third_cas.actions[fresh]["params"]["command"]
    assert command[1].startswith(GEN_B_ROOT + "/"), command
    namespace = residency_plan.share_namespace(
        digest, stage_tier, third_legs[0][2], third_legs[0][3])
    record = residency_plan.read_shared_range(queue, namespace)
    assert record is not None
    assert record["replaces"] == {"mover_action_key": lead_key,
                                  "reason": "stale"}


def test_a_fresh_mover_adopts_the_stale_movers_resident_range(
        tmp_path: Path) -> None:
    """The first consumer's mover staged ``phase-0`` under tool root A, and
    nothing live names it any more.  The second consumer registers under
    tool root B and gets a mover of its own; one tier cycle hands it the
    resident range with the tokens, and no byte is copied again."""

    queue, stage = _fixture_queue(tmp_path, 20)
    first = _shared_plan(queue, FIRST, label="gen-a", tools=TOOLS_A)
    old = str(first["phases"][0]["mover_row"]["action_key"])  # type: ignore[index]
    start, end = 0, PHASE_GIB * GIB
    namespace = _namespace(FIRST, MANIFEST, start, end)
    _land(queue, stage, consumer=namespace, manifest=MANIFEST, mover=old,
          name="phase-0", start=start, end=end)

    second = _shared_plan(queue, SECOND, label="gen-b", tools=TOOLS_B)
    new = str(second["phases"][0]["mover_row"]["action_key"])  # type: ignore[index]
    assert new != old, "a registration from another tool root was reused"
    assert residency_plan.share_namespace_of(queue, new) == namespace
    _publish_consumer(queue, SECOND, second, manifest=MANIFEST)
    now = time.time()
    _claim(queue, SECOND, phase="phase-0", claimed_unix=now - 1000.0,
           reported_unix=now - 10.0)

    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(),
                    discover=lambda **_kwargs: {TIER: _tier_record(stage, gib=20)})

    ledger = queue.tier_ledger(TIER)
    assert ledger.holder_tokens(new) == {"stage_gib": PHASE_GIB}
    assert ledger.holder_tokens(old) == {}
    assert not queue.item_path(pool.READY, new).exists()
    receipt = queue.move_record(new)
    assert receipt is not None
    assert receipt[pool.MOVE_ADOPTED_FROM_FIELD] == old
    assert receipt["bytes_copied"] == 0
    assert _files(stage, 0).exists()
    # The fresh mover vouches under the range's namespace, and the fan-out
    # gives the reading consumer its own copy of the vouch.
    root = queue.residency_fragment_root()
    assert residency_map.fragment_path(root, namespace, new).exists()
    assert residency_map.fragment_path(root, SECOND, new).exists()
    _published, staged = tier_loop._mover_state(queue, second, TIER)
    assert new in staged

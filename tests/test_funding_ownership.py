"""Real record effects and typed refusals at the C12 domain boundary."""
from __future__ import annotations

from dataclasses import FrozenInstanceError
import json

import pytest

from prismabuild import funding, pool


TIER = "stage:c12"
MOVER = "a" * 64
OWNER = "b" * 64
GENERATION = "c" * 32


def binding(variant):
    value = dict(schema=funding.TIER_FUNDING_SCHEMA_V1, tier_id=TIER,
                 mover_action_key=MOVER, range_start_bytes=0,
                 range_end_bytes=8, kind="stage_gib",
                 tokens=["stage_gib-000000"], generation=GENERATION,
                 published_unix=50.0, state="reserved", unix=100.0)
    if variant == "window":
        value.update(consumer_action_key=OWNER, plan_sha256="d" * 64)
    else:
        value.update(schema=funding.TIER_FUNDING_OUTPUT_SCHEMA_V1,
                     owner_action_key=OWNER, owner_nonce=GENERATION,
                     owner_scope_id="scope-c12", owner_published_unix=40.0,
                     template_id="c12", template_sha256="d" * 64,
                     batch_id="batch", manifest_digest="e" * 64)
    return value


@pytest.mark.parametrize("variant", ["window", "output"])
def test_both_bindings_share_the_same_durable_state_table(tmp_path, variant):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    ledger = queue.funding
    value = binding(variant)
    write = ledger.write_window if variant == "window" else ledger.write_output
    read = ledger.read_window if variant == "window" else ledger.read_output
    path = write(value)
    assert json.loads(path.read_bytes()) == value
    assert not ledger.advance_locked(MOVER, TIER, expect="reserved",
                                     advance_to="consumed", binding=variant)
    assert read(MOVER, TIER) == value
    assert ledger.advance_locked(MOVER, TIER, expect="reserved",
                                 advance_to="transferring", binding=variant,
                                 generation=GENERATION)
    assert read(MOVER, TIER)["generation"] == GENERATION
    assert not ledger.advance_locked(MOVER, TIER, expect="transferring",
                                     advance_to="released", binding=variant,
                                     generation="f" * 32)
    assert ledger.advance_locked(MOVER, TIER, expect="transferring",
                                 advance_to="consumed", binding=variant,
                                 generation=GENERATION)
    assert not ledger.advance_locked(MOVER, TIER, expect="consumed",
                                     advance_to="released", binding=variant)
    assert json.loads(path.read_bytes())["state"] == "consumed"


@pytest.mark.parametrize("variant", ["window", "output"])
def test_cover_keeps_absence_separate_from_unknown(tmp_path, variant):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    row = {"action_key": MOVER}
    if variant == "output":
        row["produced_output_batch"] = {
            "schema": pool.PRODUCED_OUTPUT_BATCH_REF_SCHEMA_V1}
    cover = (queue.funding.cover_window if variant == "window"
             else queue.funding.output_funded_cover)
    path = (queue.records.funding_path(MOVER, TIER) if variant == "window"
            else queue.records.funding_output_path(MOVER, TIER))
    assert cover(TIER, row, "stage_gib", 1).state == "absent"
    path.parent.mkdir(parents=True, exist_ok=True)
    for raw in (b"", b"{", b"null", b"{}"):
        path.write_bytes(raw)
        verdict = cover(TIER, row, "stage_gib", 1)
        assert verdict.state == "unknown"
        assert verdict.credit == (0, None)
        assert path.read_bytes() == raw


def test_release_keeps_unknown_for_dead_input_retry(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    assert queue.funding.release_output(MOVER, TIER) is funding.ReleaseVerdict.REFUSED
    path = queue.records.funding_output_path(MOVER, TIER)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"{")
    assert queue.funding.release_output(MOVER, TIER) is funding.ReleaseVerdict.UNKNOWN
    assert queue.funding._release_never_started_funding_locked(
        MOVER, TIER).result is None
    assert path.read_bytes() == b"{"


def test_pass_facts_never_infer_permission_from_absence():
    facts = funding.AdvanceProtection()
    assert (OWNER, TIER) not in facts.permitted
    facts.permitted[(OWNER, TIER)] = {"advance": "held"}
    assert facts.permitted[(OWNER, TIER)] == {'advance': 'held'}
    facts.unknown_tiers.add(TIER)
    assert TIER in facts.unknown_tiers
    assert not funding.AdvanceProtection().unknown_tiers


def test_funding_construction_is_lazy_and_binding_is_immutable():
    class UnreadableQueue:
        def __getattr__(self, name):
            raise AssertionError("constructor read " + name)

    source = UnreadableQueue()
    assert funding.FundingLedger(source).queue is source
    value = binding("window")
    value.pop("state")
    value.pop("unix")
    value["tokens"] = tuple(value["tokens"])
    bound = funding.WindowBinding(**value)
    with pytest.raises(FrozenInstanceError):
        bound.generation = "f" * 32


def test_the_owned_zero_credit_branch_does_not_acquire(tmp_path):
    from prismabuild import batch_ledger, output_contract
    from test_commit_requires_existing_funding import _world
    from test_produced_output_scope import STAGE_TIER, STAGE_BARE
    world = _world(tmp_path)
    queue, template, instance, _, _, _, funding = world
    ledger = queue.tier(STAGE_TIER)
    free = ledger.free_supply()
    manifest = output_contract.output_manifest_sha256([])
    namespace = output_contract.batch_namespace(instance, 'batch', manifest)
    with batch_ledger.path_locks(queue, instance):
        result = queue.funding.commit_stage(
            instance, template, batch_id='batch', manifest_digest=manifest,
            batch_ns=namespace, tier=STAGE_TIER, mover='a' * 64,
            kind=STAGE_BARE, batch_total=0, batch_gib=0)
    assert result.refusal is None
    assert result.prepaid is None
    assert result.path == funding
    assert ledger.free_supply() == free
    assert ledger.holdings(namespace) == {}
    assert ledger.holdings('a' * 64) == {}
    assert not funding.exists()

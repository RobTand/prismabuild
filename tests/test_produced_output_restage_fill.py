"""A restaged batch's mover can reserve pool fill like any other mover (#747).

A restage (``ensure_batch_materialized``) copies a retired batch back from
its origin on the pool, cold, the way a consumer's input mover does.  Input
movers reserve ``fill_mb_s_pool_side@<tier>`` priced by
``storage_tiers.current_fill_offer`` over the movement receipts; the
restage mover reserved none, so it read the spindles outside the ledger that
rations them.

Off by default.  The producer opts in with
``PRISMABUILD_PRODUCED_OUTPUT_RESTAGE_FILL=1`` in its sealed environment, or
one call opts in or out with ``restage_fill=True|False``.  A first
publication (generation 0) never reserves fill: it reads what the producer
has just written.  A resumed restage keeps the price it was sealed at.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prismabuild import pool, produced_output as po, storage_tiers  # noqa: E402

import test_produced_output_restage as restage  # noqa: E402
from test_produced_output_restage import (  # noqa: E402,F401
    _isolated_synthetic_launch_context)

TIER = restage.TIER
KIND = restage.KIND
FILL = storage_tiers.FILL_KIND
FILL_DEMAND = f"{FILL}{storage_tiers.TIER_DEMAND_SEPARATOR}{TIER}"
ENV = "PRISMABUILD_PRODUCED_OUTPUT_RESTAGE_FILL"


def _offer_fill(queue: pool.PoolQueue, mb_s: int) -> None:
    """Mint ``mb_s`` fill tokens and announce them, as the tier loop does."""

    queue.mint_tier_capacity(TIER, {KIND: 4, FILL: mb_s})
    path = Path(queue.root) / "tiers" / f"{TIER}.json"
    record = json.loads(path.read_text())
    record["tokens"] = {KIND: 4, FILL: mb_s}
    pool._write_json_atomic(path, record)


def _sealed(world, key: str) -> dict:
    path = Path(world.cas_root) / "requests" / key[:2] / f"{key}.json"
    return json.loads(path.read_text())


def _row(world, key: str) -> dict:
    for state in (pool.READY, pool.CLAIMED):
        row = pool._read_json(world.q.item_path(state, key))
        if row is not None:
            return row
    raise AssertionError(f"no live row for {key[:12]}")


def _with_env(monkeypatch, value: str) -> None:
    """Seal the producer's request with ``ENV=value`` in its environment."""

    original = restage._producer_request
    real = restage.pb.seal_action  # the one sealing API

    def seal(body):
        body = json.loads(json.dumps(body))
        body["environment"]["variables"][ENV] = value
        return real(body)

    def producer_request(tmp_path, cas_root, template):
        with pytest.MonkeyPatch.context() as sealing:
            sealing.setattr(restage.pb, "seal_action", seal)
            return original(tmp_path, cas_root, template)

    monkeypatch.setattr(restage, "_producer_request", producer_request)


def test_off_by_default_a_restage_reserves_no_fill(tmp_path: Path) -> None:
    world, _descs = restage._staged_world(tmp_path)
    _offer_fill(world.q, 100)
    ensured = world.ensure("b1")
    assert ensured.get("ok") is True, ensured
    mover = str(ensured["mover_key"])
    demand = _sealed(world, mover)["params"]["demand"]
    assert demand == {"cpu": 1, "mem_gb": 1, f"{KIND}@{TIER}": 1}
    assert "--fill-mb-s-pool-side" not in _sealed(world, mover)["params"]["command"]
    assert _row(world, mover)["resources"] == demand
    assert "fill_mb_s_pool_side" not in world.entry("b1")["materializations"][0]


def test_an_opted_in_restage_reserves_the_offer_and_runs(tmp_path: Path) -> None:
    world, _descs = restage._staged_world(tmp_path)
    _offer_fill(world.q, 100)
    ensured = po.ensure_batch_materialized(
        world.q, world.inst, world.template, batch_id="b1",
        cas_root=world.cas_root, restage_fill=True, **world.publish_kwargs)
    assert ensured.get("ok") is True, ensured
    mover = str(ensured["mover_key"])
    action = _sealed(world, mover)
    # Nothing has measured the pool, so the price is the tier's offer.
    assert action["params"]["demand"][FILL_DEMAND] == 100
    command = action["params"]["command"]
    assert command[command.index("--fill-mb-s-pool-side") + 1] == "100"
    assert _row(world, mover)["resources"] == action["params"]["demand"]
    assert world.entry("b1")["materializations"][0][
        "fill_mb_s_pool_side"] == 100
    # The stage window still comes from the producer by transfer; the fill
    # comes from free at the claim, and goes back when the mover stops.
    restage._claim_mover(world.q, "w-fill")
    assert world.ledger.holder_tokens(mover) == {KIND: 1, FILL: 100}
    receipt = restage._execute_mover(world.q, mover)
    assert receipt["complete"] is True, receipt
    world.q.finish(mover, status="executed")
    assert world.ledger.available().get(FILL) == 100


def test_the_sealed_environment_opts_the_producer_in(
        tmp_path: Path, monkeypatch) -> None:
    _with_env(monkeypatch, "1")
    world, _descs = restage._staged_world(tmp_path)
    _offer_fill(world.q, 100)
    ensured = world.ensure("b1")
    assert ensured.get("ok") is True, ensured
    demand = _sealed(world, str(ensured["mover_key"]))["params"]["demand"]
    assert demand[FILL_DEMAND] == 100


def test_a_call_can_opt_out_of_the_sealed_setting(
        tmp_path: Path, monkeypatch) -> None:
    _with_env(monkeypatch, "1")
    world, _descs = restage._staged_world(tmp_path)
    _offer_fill(world.q, 100)
    ensured = po.ensure_batch_materialized(
        world.q, world.inst, world.template, batch_id="b1",
        cas_root=world.cas_root, restage_fill=False, **world.publish_kwargs)
    assert ensured.get("ok") is True, ensured
    assert FILL_DEMAND not in _sealed(
        world, str(ensured["mover_key"]))["params"]["demand"]


@pytest.mark.parametrize("value", ["", "0"])
def test_an_unset_or_zero_setting_is_off(tmp_path: Path, monkeypatch,
                                        value: str) -> None:
    _with_env(monkeypatch, value)
    world, _descs = restage._staged_world(tmp_path)
    _offer_fill(world.q, 100)
    ensured = world.ensure("b1")
    assert ensured.get("ok") is True, ensured
    assert FILL_DEMAND not in _sealed(
        world, str(ensured["mover_key"]))["params"]["demand"]


def test_any_other_setting_is_refused_before_anything_is_filed(
        tmp_path: Path, monkeypatch) -> None:
    _with_env(monkeypatch, "yes")
    world, _descs = restage._staged_world(tmp_path)
    ensured = world.ensure("b1")
    assert ensured.get("ok") is False, ensured
    assert ENV in str(ensured.get("refusal")), ensured
    assert "materializations" not in world.entry("b1")


def test_a_first_publication_never_reserves_fill(
        tmp_path: Path, monkeypatch) -> None:
    _with_env(monkeypatch, "1")
    world = restage._World(tmp_path)
    _offer_fill(world.q, 100)
    descs = restage._descriptors(world.template, world.inst, "p1", b"X" * 600)
    restage._prewrite(world.q, world.inst, world.template, "b1", descs)
    first = world.first_publish("b1", descs)
    assert FILL_DEMAND not in _sealed(
        world, str(first["mover_key"]))["params"]["demand"]


def test_a_tier_that_offers_no_fill_leaves_the_restage_unreserved(
        tmp_path: Path) -> None:
    world, _descs = restage._staged_world(tmp_path)
    ensured = po.ensure_batch_materialized(
        world.q, world.inst, world.template, batch_id="b1",
        cas_root=world.cas_root, restage_fill=True, **world.publish_kwargs)
    assert ensured.get("ok") is True, ensured
    action = _sealed(world, str(ensured["mover_key"]))
    assert FILL_DEMAND not in action["params"]["demand"]
    assert "--fill-mb-s-pool-side" not in action["params"]["command"]


def test_a_resumed_restage_keeps_the_price_it_was_sealed_at(
        tmp_path: Path) -> None:
    """The row republished on resume carries the sealed demand, whatever the
    switch or the offer says now: a row whose resources differ from its
    sealed demand is refused at launch."""

    world, _descs = restage._staged_world(tmp_path)
    _offer_fill(world.q, 100)

    def _boom(*args, **kwargs):
        raise restage._Crash("before-publish")

    with pytest.MonkeyPatch.context() as crash:
        crash.setattr(po, "_publish_output_mover_row", _boom)
        with pytest.raises(restage._Crash):
            po.ensure_batch_materialized(
                world.q, world.inst, world.template, batch_id="b1",
                cas_root=world.cas_root, restage_fill=True,
                **world.publish_kwargs)
    _offer_fill(world.q, 60)
    resumed = world.ensure("b1")
    assert resumed.get("ok") is True and resumed.get("resumed") is True, resumed
    mover = str(resumed["mover_key"])
    demand = _sealed(world, mover)["params"]["demand"]
    assert demand[FILL_DEMAND] == 100
    assert _row(world, mover)["resources"] == demand

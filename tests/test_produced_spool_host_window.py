"""A producer's local spool window is a host resource admitted at claim (#747).

Until now a produced-output producer's local precommit spool was bounded per
owner by ``PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES`` and a ``statvfs`` check,
so two producers on one box could together overrun its disk.  With the
opt-in, the owner's window is a ``spool_gb`` reservation against the budget
the box declares with ``worker_loop.py --spool-gb``, charged through the
ordinary host ledger at claim.

Both switches default off.  These tests pin the off path first -- a worker
declares exactly what it declared before, a producer checks nothing new --
and then the on path: the host kind passes through the observed offer and
the claim unchanged, a claim without enough of it is refused, and a producer
whose claimed row did not reserve its window refuses to spool.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

import test_prepaid_writer_integration as fx
from prismabuild import box_capacity, core, pool, produced_output as po
from prismabuild import produced_spool as ps

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import worker_loop  # noqa: E402

GIB = 1 << 30
KIND = "spool_gb"


# -- the pure helper ----------------------------------------------------------


@pytest.mark.parametrize("switch", [None, "", "0"])
def test_off_derives_no_host_term_even_without_a_byte_bound(switch):
    variables = {} if switch is None else {ps.HOST_WINDOW_ENV: switch}
    assert ps.host_window_terms(variables) == {}
    assert ps.host_window_terms({**variables, ps.MAX_ENV: str(5 * GIB)}) == {}


@pytest.mark.parametrize("maximum, gib", [
    (1, 1), (256, 1), (GIB, 1), (GIB + 1, 2), (3 * GIB, 3), (32 << 30, 32),
])
def test_on_derives_the_window_in_whole_gib_rounded_up(maximum, gib):
    variables = {ps.HOST_WINDOW_ENV: "1", ps.MAX_ENV: str(maximum)}
    assert ps.HOST_WINDOW_KIND == KIND
    assert ps.host_window_terms(variables) == {KIND: gib}


@pytest.mark.parametrize("maximum", [None, "", "0", "-5", "1.5", "x", " 7"])
def test_on_refuses_a_missing_or_non_positive_byte_bound(maximum):
    variables = {ps.HOST_WINDOW_ENV: "1"}
    if maximum is not None:
        variables[ps.MAX_ENV] = maximum
    with pytest.raises(ps.SpoolError, match=ps.MAX_ENV):
        ps.host_window_terms(variables)


@pytest.mark.parametrize("switch", ["yes", "true", "2", " 1"])
def test_a_switch_that_is_not_zero_or_one_is_refused(switch):
    with pytest.raises(ps.SpoolError, match="must be 0 or 1"):
        ps.host_window_terms({ps.HOST_WINDOW_ENV: switch, ps.MAX_ENV: "256"})


# -- the worker's declaration -------------------------------------------------


def test_a_worker_without_the_flag_declares_exactly_what_it_did_before():
    args = worker_loop.build_parser().parse_args([])
    assert args.spool_gb == 0
    assert worker_loop.declared_host_capacity(args, cores=10) == {"mem_gb": 96, "cpu": 10}
    args = worker_loop.build_parser().parse_args(["--mem-gb", "40", "--spool-gb", "0"])
    assert worker_loop.declared_host_capacity(args, cores=3) == {"mem_gb": 40, "cpu": 3}


def test_a_worker_with_the_flag_declares_its_spool_budget():
    args = worker_loop.build_parser().parse_args(["--spool-gb", "64"])
    assert worker_loop.declared_host_capacity(args, cores=10) == {
        "mem_gb": 96, "cpu": 10, KIND: 64}


def test_a_negative_spool_budget_is_an_argument_error(capsys):
    with pytest.raises(SystemExit):
        worker_loop.validate_args(
            worker_loop.build_parser(),
            worker_loop.build_parser().parse_args(["--spool-gb", "-1"]))
    assert "--spool-gb" in capsys.readouterr().err


# -- the host kind passes through the offer and the claim ---------------------


def test_the_observed_offer_passes_the_spool_budget_through_unchanged():
    declared = {"mem_gb": 8, "cpu": 2, KIND: 64}
    seen = box_capacity.observe(declared, {}, gpu_sample=None, mem_gb=None, load1=None)
    assert seen.capacity[KIND] == 64
    # A host the ledger already knows, which has never offered the kind: the
    # seed pads the window at zero, and the first real reading still offers it.
    observer = box_capacity.CapacityObserver(ledger_total={"mem_gb": 8, "cpu": 2})
    offer = observer.offer(declared, {}, gpu_sample=None, mem_gb=None, load1=None)
    assert offer[KIND] == 64
    assert box_capacity.CapacityObserver().offer(
        declared, {}, gpu_sample=None, mem_gb=None, load1=None)[KIND] == 64


def _publish(queue, key, resources):
    queue.publish(action_key=key, cas_root="/cas",
                  worker_script=str(fx.REPO / "tools" / "prismabuild_worker.py"),
                  checkout_root=str(Path(queue.root).parent / "checkout"),
                  resources=resources)


def test_a_claim_reserves_spool_on_a_box_that_declares_it(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    key = "a" * 64
    _publish(queue, key, {"cpu": 1, "mem_gb": 1, KIND: 3})
    claimed = queue.claim(owner="w", capacity={"cpu": 2, "mem_gb": 4, KIND: 5})
    assert claimed is not None and claimed["resources"][KIND] == 3
    ledger = queue.ledger()
    assert ledger.capacity()[KIND] == 5
    assert ledger.available()[KIND] == 2


def test_a_box_that_declares_no_spool_never_fits_and_placement_calls_it_unknown(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.announce(host="box", tags=["box"], has_gpu=False,
                   capacity={"cpu": 2, "mem_gb": 4})
    key = "b" * 64
    intent = {"tags": [], "needs_gpu": False, "resources": {"cpu": 1, "mem_gb": 1, KIND: 3}}
    # The census reads a kind an offer does not name as unknown, not zero, so
    # pbrun's placement check passes the submission through ...
    assert queue.placeable(intent) is True
    _publish(queue, key, intent["resources"])
    # ... and every claim refuses it for want of the kind.
    assert queue.claim(owner="w", capacity={"cpu": 2, "mem_gb": 4}) is None
    local = pool.cpu_admission.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    denial = next(iter(pool.cpu_admission.read_json(local)["records"].values()))
    assert denial["reason"] == "never_fits_capacity"
    assert denial["evidence"]["demand"][KIND] == 3
    assert KIND not in denial["evidence"]["capacity_total"]
    # A box that names a smaller budget is a refusal at submission.
    queue.announce(host="box", tags=["box"], has_gpu=False,
                   capacity={"cpu": 2, "mem_gb": 4, KIND: 2})
    assert queue.placeable(intent) is False


# -- the producer checks its claimed row --------------------------------------


def _spool(tmp_path, *, env=None, reserved=None, maximum=256):
    """A claimed producer whose row reserved ``reserved`` spool_gb, and its spool."""

    cas_root = tmp_path / "cas"
    template = fx._template(str(tmp_path / "canonical"))
    initial = fx._producer_request(tmp_path, cas_root, template)
    cas, request = po._read_producer_request(cas_root, initial)
    request.pop("action_key")
    request["environment"]["variables"].update({
        ps.ROOT_ENV: str(tmp_path / "local"), ps.MAX_ENV: str(maximum), **(env or {})})
    action = core.seal_action(request)
    cas.publish_action_request(action)
    owner = action["action_key"]
    q = fx._queue(tmp_path)
    resources = {"cpu": 1, "mem_gb": 1, **po.owner_demand_terms(template)}
    capacity = {"cpu": 4, "mem_gb": 8}
    if reserved is not None:
        resources[KIND] = reserved
        capacity[KIND] = reserved
    q.publish(action_key=owner, cas_root=str(cas_root),
              worker_script=str(fx.REPO / "tools" / "prismabuild_worker.py"),
              checkout_root=str(tmp_path / "mover-checkout"),
              resources=resources, produced_output_template=template)
    claimed = q.claim(owner="finite-producer", capacity=capacity)
    assert claimed is not None and claimed["action_key"] == owner
    control = fx._broker_control(q, owner)
    po.declare_template(q.root, template)
    inst = po.bind_instance(q, template, owner_action_key=owner,
        claim_snapshot=claimed, env={"PRISMABUILD_ACTION_KEY": owner,
            "PRISMABUILD_ACTION_NONCE": control["nonce"],
            "PRISMABUILD_ACTION_SCOPE": control["scope_id"]})
    po.declare_instance(q.root, inst)
    assert po.admit_instance(q, inst, template)["ok"]
    fx._announce_tier(q, tmp_path / "stage")
    return lambda: ps.ProducedSpool(q, inst, template, cas_root=cas_root,
                                    root=tmp_path / "local", max_bytes=maximum)


@pytest.mark.parametrize("env", [None, {ps.HOST_WINDOW_ENV: ""}, {ps.HOST_WINDOW_ENV: "0"}])
def test_an_off_producer_checks_no_host_window(tmp_path, env):
    spool = _spool(tmp_path, env=env)()
    assert spool.host_window == {}


def test_an_on_producer_whose_row_reserved_its_window_spools(tmp_path):
    spool = _spool(tmp_path, env={ps.HOST_WINDOW_ENV: "1"}, reserved=1)()
    assert spool.host_window == {KIND: 1}
    assert spool.reserve_group  # the ordinary spool, unchanged past the check


def test_an_on_producer_whose_row_reserved_nothing_refuses(tmp_path):
    make = _spool(tmp_path, env={ps.HOST_WINDOW_ENV: "1"})
    with pytest.raises(ps.SpoolError, match=r"spool_gb.*0.*1|1.*spool_gb.*0"):
        make()


def test_an_on_producer_whose_row_reserved_too_little_refuses(tmp_path):
    make = _spool(tmp_path, env={ps.HOST_WINDOW_ENV: "1"}, reserved=1,
                  maximum=GIB + 1)
    with pytest.raises(ps.SpoolError, match=r"\b1\b.*\b2\b|\b2\b.*\b1\b"):
        make()


def test_an_on_producer_refuses_an_invalid_switch(tmp_path):
    make = _spool(tmp_path, env={ps.HOST_WINDOW_ENV: "on"}, reserved=1)
    with pytest.raises(ps.SpoolError, match="must be 0 or 1"):
        make()

"""What kills an action is said at submit and recorded in the receipt.

Every worker loop enforces an execution ceiling of its own -- 7200 s by
``worker_loop.py``'s default -- and ``pool._execution_timeout`` applies it as a
silent ``min`` against the submitter's sealed ``execution_timeout_s``.  Nothing
carried either number anywhere a reader could reach it: not the offer, not the
submission ack, not the outcome.

So the PrismaQuant #275 campaign asked for 13000 s, was admitted without a
word, and was killed at 7200 s.  Its own ``--deadline-seconds`` logic -- which
would have sealed the checkpoint as ``stopped_early`` -- never fired, the
attempt ended rc=1 with no sealed payload, and 554 anchor rows survived only
because that job happens to checkpoint every ten.  A second submission at
``--timeout-s 28800`` would have met the same ceiling just as quietly.

Three places, because the number has to survive the whole trip:

*   the **offer** carries the box's ceiling, so it can be asked for at all;
*   ``pbrun`` **says at submit** when an eligible box would cut the request
    short, naming the boxes and the deadline that would really apply;
*   the **outcome** records what governed -- requested, ceiling, effective,
    and whether the ceiling was what chose -- so a receipt read later says
    why time ran out rather than only that it did.

Warned, not refused: an action that asks for more than it needs and finishes
inside the ceiling is not wrong, and refusing would break every long
submission on a fleet whose loops all default to 7200.  What was wrong was
being told nothing.

Issue #293.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402

import pbrun  # noqa: E402
import pbstatus  # noqa: E402

from test_pool_action_timeout import _claimed  # noqa: E402

#: A demand every box in these fixtures can fit, so placement never becomes
#: the reason a ceiling is or is not reported.
FITS = {"cpu": 1, "mem_gb": 2}


def _intent(tags):
    return {"tags": list(tags), "needs_gpu": False, "resources": dict(FITS)}


def _fleet(tmp_path, boxes):
    """A queue whose boxes announce ``{host: ceiling or None}``."""

    queue = pool.PoolQueue(tmp_path / "queue")
    for host, ceiling in boxes.items():
        queue.announce(host=host, tags=["cpu", host, "x86"], has_gpu=False,
                       capacity={"gpu": 0, "mem_gb": 60, "cpu": 80},
                       timeout_ceiling_s=ceiling)
    return queue


# --------------------------------------------------------------------------
# The budget, and what it says chose
# --------------------------------------------------------------------------

def test_the_ceiling_is_reported_as_the_thing_that_chose(tmp_path):
    _queue, item = _claimed(tmp_path, 13000)
    budget = pool.execution_budget(item, 7200)
    assert budget.effective == 7200
    assert budget.requested == 13000
    assert budget.ceiling == 7200
    assert budget.clamped is True


def test_a_request_inside_the_ceiling_is_not_reported_as_clamped(tmp_path):
    _queue, item = _claimed(tmp_path, 600)
    budget = pool.execution_budget(item, 7200)
    assert budget.effective == 600
    assert budget.clamped is False


def test_an_unbounded_submitter_runs_under_the_ceiling_but_is_not_clamped(tmp_path):
    # Nothing was cut short: the submitter named no deadline, so the ceiling
    # is the only number there ever was.  Calling that "clamped" would put a
    # false positive in front of every ordinary submission.
    _queue, item = _claimed(tmp_path, None)
    budget = pool.execution_budget(item, 7200)
    assert budget.effective == 7200
    assert budget.requested is None
    assert budget.clamped is False


def test_an_unbounded_worker_grants_what_was_asked(tmp_path):
    _queue, item = _claimed(tmp_path, 13000)
    budget = pool.execution_budget(item, None)
    assert budget.effective == 13000
    assert budget.clamped is False


def test_the_outcome_records_what_governed_it(tmp_path):
    # The receipt half.  The action sleeps 1.5 s and the ceiling is 0.3 s, so
    # this is the #275 shape in miniature: killed by a limit the submitter did
    # not choose and, until now, could not see afterwards either.
    queue, item = _claimed(tmp_path, 10)
    outcome = queue.execute(item, timeout_s=0.3, heartbeat_s=30,
                            timeout_grace_s=0.2)
    assert outcome["status"] == "timeout"
    assert outcome["execution_timeout_s"] == 0.3
    assert outcome["execution_timeout_requested_s"] == 10
    assert outcome["execution_timeout_ceiling_s"] == 0.3
    assert outcome["execution_timeout_clamped"] is True


def test_an_ending_that_is_not_a_timeout_still_says_what_governed(tmp_path):
    # A receipt should answer "what was the budget" whatever the ending, not
    # only when the budget is what ended it.
    queue, item = _claimed(tmp_path, 60)
    outcome = queue.execute(item, timeout_s=60, heartbeat_s=30)
    assert outcome["status"] != "timeout"
    assert outcome["execution_timeout_s"] == 60
    assert outcome["execution_timeout_clamped"] is False


# --------------------------------------------------------------------------
# The offer, and the boxes that do not carry one
# --------------------------------------------------------------------------

def test_a_box_announces_the_ceiling_it_will_kill_at(tmp_path):
    queue = _fleet(tmp_path, {"dl380g10": 7200.0})
    assert queue.offers()[0]["timeout_ceiling_s"] == 7200.0


def test_an_offer_that_names_no_ceiling_carries_no_field(tmp_path):
    # Absent means "not measured", and it has to be distinguishable from a
    # value.  A loop predating this field must not be read as unbounded.
    queue = _fleet(tmp_path, {"dl380g10": None})
    assert "timeout_ceiling_s" not in queue.offers()[0]


def test_the_ceilings_are_read_per_eligible_box(tmp_path):
    queue = _fleet(tmp_path, {"dl380g10": 7200.0, "sparky": 20000.0,
                              "sparklina": None})
    ceilings = queue.placement_timeout_ceilings(_intent([]))
    assert ceilings == {"dl380g10": 7200.0, "sparky": 20000.0,
                        "sparklina": None}


def test_only_the_boxes_that_could_run_it_are_asked(tmp_path):
    queue = _fleet(tmp_path, {"dl380g10": 7200.0, "sparky": 20000.0})
    assert queue.placement_timeout_ceilings(_intent(["sparky"])) == {
        "sparky": 20000.0}


def test_nobody_announcing_is_an_empty_answer_not_a_zero(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    assert queue.placement_timeout_ceilings(_intent([])) == {}


# --------------------------------------------------------------------------
# What pbrun says
# --------------------------------------------------------------------------

def test_every_eligible_box_below_the_request_is_stated_as_a_certainty(tmp_path):
    queue = _fleet(tmp_path, {"dl380g10": 7200.0, "sparky": 7200.0})
    said = pbrun.timeout_ceiling_notice(queue, _intent([]), requested=13000)
    assert "will be killed at 7200s, not 13000s" in said
    assert "every eligible worker" in said
    assert "dl380g10 7200s" in said and "sparky 7200s" in said


def test_a_mixed_fleet_is_stated_as_a_risk_not_a_certainty(tmp_path):
    queue = _fleet(tmp_path, {"dl380g10": 7200.0, "sparky": 20000.0})
    said = pbrun.timeout_ceiling_notice(queue, _intent([]), requested=13000)
    assert "may be killed at 7200s" in said
    assert "some eligible worker" in said
    assert "sparky" not in said.split("\n")[0]


def test_the_deadline_named_is_the_smallest_a_claiming_box_could_impose(tmp_path):
    # The submitter does not choose which box claims, so the number to act on
    # is the smallest ceiling any eligible box would impose, not the largest
    # -- two boxes BOTH below the request, at different ceilings, because a
    # fixture where only one cuts cannot tell ``min`` from ``max``.
    queue = _fleet(tmp_path, {"dl380g10": 3600.0, "sparklina": 7200.0,
                              "sparky": 20000.0})
    said = pbrun.timeout_ceiling_notice(queue, _intent([]), requested=13000)
    assert "may be killed at 3600s" in said
    assert "7200s" not in said.split(" so this action ")[1]


def test_a_request_every_box_can_grant_is_not_worth_a_word(tmp_path):
    queue = _fleet(tmp_path, {"dl380g10": 7200.0, "sparky": 20000.0})
    assert pbrun.timeout_ceiling_notice(queue, _intent([]), requested=600) == ""


def test_asking_for_no_deadline_is_not_worth_a_word(tmp_path):
    queue = _fleet(tmp_path, {"dl380g10": 7200.0})
    assert pbrun.timeout_ceiling_notice(queue, _intent([]), requested=None) == ""


def test_a_silent_box_is_named_as_unknown_rather_than_unlimited(tmp_path):
    queue = _fleet(tmp_path, {"sparklina": None})
    said = pbrun.timeout_ceiling_notice(queue, _intent([]), requested=13000)
    assert "sparklina" in said
    assert "unknown rather than unlimited" in said
    assert "will be killed" not in said


def test_an_unannounced_fleet_says_nothing_rather_than_guessing(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    assert pbrun.timeout_ceiling_notice(queue, _intent([]), requested=13000) == ""


# --------------------------------------------------------------------------
# What pbstatus shows
# --------------------------------------------------------------------------

def test_pbstatus_carries_the_ceiling_from_the_offer_it_read(tmp_path):
    # Read through ``read_pool`` rather than from a row built by hand: a row
    # this test writes itself proves the renderer and nothing about whether
    # the field ever leaves the offer.
    queue = _fleet(tmp_path, {"dl380g10": 7200.0, "old": None})
    nodes = {node["node"]: node
             for node in pbstatus.read_pool(queue.root)["nodes"]}

    assert nodes["dl380g10"]["timeout_ceiling_s"] == 7200.0
    assert nodes["old"]["timeout_ceiling_s"] is None
    lines = pbstatus.pool_node_lines(list(nodes.values()))
    assert "KILL AT" in lines[0]
    assert "7200s" in [line for line in lines if line.startswith("dl380g10")][0]
    assert pbstatus.ABSENT in [line for line in lines if line.startswith("old")][0]


# --------------------------------------------------------------------------
# The wiring: the loop announces its OWN flag, not a constant
# --------------------------------------------------------------------------

def _poll(tmp_path: Path, *extra: str) -> dict:
    """One real poll of a real worker loop; returns the offer it wrote."""

    import importlib.util
    import json
    from unittest import mock

    from prismabuild import box_capacity  # noqa: F401  (patched below)
    import test_worker_offer_counts_the_boxs_loops as counted

    spec = importlib.util.spec_from_file_location(
        "worker_loop_ceiling_test", REPOSITORY / "tools/fleet/worker_loop.py")
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    with mock.patch.object(worker, "SH", tmp_path), \
         mock.patch.object(worker.cpu_topology, "pin_to_preferred", return_value=None), \
         mock.patch.object(worker.cpu_topology, "inherited_tiers", return_value=None), \
         mock.patch.object(worker, "loaded_runtime_commit", return_value="deadbeef"), \
         mock.patch.object(worker, "published_commit", return_value="deadbeef"), \
         mock.patch.object(worker.box_capacity, "worker_loops", mock.Mock(return_value=())), \
         mock.patch.object(worker.box_capacity, "trusted_gpu_sample",
                           return_value=counted._sample()), \
         mock.patch.object(worker.box_capacity, "mem_available_gb", return_value=100), \
         mock.patch.object(worker.box_capacity, "run_queue", return_value=0.0), \
         mock.patch.object(sys, "argv", ["worker_loop.py", *counted.BASE, *extra]):
        assert worker.main() == 0
    offers = list((tmp_path / "pb-queue" / "workers").glob("*.json"))
    assert len(offers) == 1
    return json.loads(offers[0].read_text())


def test_a_real_loop_announces_the_ceiling_it_would_enforce(tmp_path):
    # 7200 is worker_loop's own default and the number #275 met.
    assert _poll(tmp_path)["timeout_ceiling_s"] == 7200.0


def test_the_announced_ceiling_follows_the_loops_flag(tmp_path):
    # Reading the flag, not repeating a constant: a fleet that raises the
    # ceiling on one box has to be able to say so, or the notice pbrun prints
    # is about a limit nobody enforces.
    assert _poll(tmp_path, "--timeout-s", "20000")["timeout_ceiling_s"] == 20000.0

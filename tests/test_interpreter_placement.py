"""Placement eligibility comes from the interpreter a host has (#1263).

An item that names an absolute interpreter is placeable exactly on the boxes
whose offers positively report the path -- the same "presence is positive
evidence only" rule declared images follow (#714).  A submitter tag stays
available for a real constraint; what it stops being is the spelling for "the
interpreter exists", which is how CPU-only work pinned itself onto the GPU
hosts and idled both GPUs for 100 minutes on 2026-09-28.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import sys

import pytest

from prismabuild import core as pb, pool

CAPACITY = {"cpu": 4, "mem_gb": 16}
PQ_PYTHON = "/home/rob/venvs/pq-pb461728e4-tessera-43da1c39/bin/python"
TF516_PYTHON = "/home/rob/venvs/pq-pb461728e4-tessera-43da1c39-tf516/bin/python"


def _queue_at(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    return queue


def _announce(queue, host, *, interpreters=None, tags=("gb10",),
              capacity=CAPACITY, gpu=False):
    queue.announce(host=host, tags=list(tags), has_gpu=gpu,
                   capacity=dict(capacity), interpreters=interpreters)


def _publish(queue, key, **kwargs):
    queue.publish(action_key=key, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", **kwargs)


def _item_of(queue, key):
    return json.loads(queue.item_path(pool.READY, key).read_text())


def _denials(queue):
    from prismabuild import adaptive_cpu
    base = adaptive_cpu.local_state_base(queue.ledger().base)
    records = adaptive_cpu.read_json(base / pool.CLAIM_DENIALS).get(
        "records", {})
    return list(records.values())


# --------------------------------------------------------------------------
# The acceptance bullets, one test each
# --------------------------------------------------------------------------

def test_a_shared_venv_interpreter_is_placeable_on_every_box_that_has_it(tmp_path):
    """The pq venv: named, untagged, eligible exactly where it exists."""

    queue = _queue_at(tmp_path)
    for host in ("dl380g10", "sparky", "sparklina"):
        _announce(queue, host, tags=(pb.INTERPRETER_TAG,),
                  interpreters=[PQ_PYTHON])
    _publish(queue, "a" * 64, interpreter=PQ_PYTHON)

    probe = {"tags": [pb.INTERPRETER_TAG], "interpreter": PQ_PYTHON,
             "resources": {"cpu": 1, "mem_gb": 1}}
    assert queue.placeable(probe) is True
    assert set(queue.placeable_hosts(probe)) == {
        "dl380g10", "sparky", "sparklina"}


def test_a_sparks_only_interpreter_is_placeable_on_the_sparks_only(tmp_path):
    """The tf516 venv: portable work, one real constraint, no submitter tag."""

    queue = _queue_at(tmp_path)
    _announce(queue, "dl380g10", tags=(pb.INTERPRETER_TAG,),
              interpreters=[PQ_PYTHON])
    _announce(queue, "sparky", tags=(pb.INTERPRETER_TAG,),
              interpreters=[PQ_PYTHON, TF516_PYTHON])
    _announce(queue, "sparklina", tags=(pb.INTERPRETER_TAG,),
              interpreters=[PQ_PYTHON, TF516_PYTHON])

    probe = {"tags": [], "interpreter": TF516_PYTHON,
             "resources": {"cpu": 1, "mem_gb": 1}}

    # Untagged, the interpreter itself is the placement claim: the row may
    # land on either Spark and never on the box without the path.  This is
    # the RED shape -- on main, tags alone answer and dl380g10 matches too.
    assert set(queue.placeable_hosts(probe)) == {"sparky", "sparklina"}


def test_an_interpreter_no_recorded_worker_reports_is_refused(tmp_path):
    """A nonexistent path is a refusal naming it, not a 127 on a worker."""

    queue = _queue_at(tmp_path)
    _announce(queue, "dl380g10", tags=(), interpreters=[PQ_PYTHON])
    _announce(queue, "sparky", tags=(), interpreters=[PQ_PYTHON])

    probe = {"tags": [], "interpreter": "/no/such/venv/bin/python",
             "resources": {"cpu": 1, "mem_gb": 1}}

    assert queue.placeable(probe) is False


def test_an_offer_without_answers_is_not_a_match_for_a_naming_item(tmp_path):
    """Unknown is not capable: the rolling-publish fence, one rule with #714."""

    queue = _queue_at(tmp_path)
    _announce(queue, "old-generation", tags=(), interpreters=None)
    _announce(queue, "new-generation", tags=(pb.INTERPRETER_TAG,),
              interpreters=[PQ_PYTHON])

    probe = {"tags": [], "interpreter": PQ_PYTHON,
             "resources": {"cpu": 1, "mem_gb": 1}}

    assert set(queue.placeable_hosts(probe)) == {"new-generation"}


# --------------------------------------------------------------------------
# The declaration
# --------------------------------------------------------------------------

def test_publish_seals_the_path_and_adds_the_capability_tag(tmp_path):
    queue = _queue_at(tmp_path)
    _publish(queue, "b" * 64, interpreter=PQ_PYTHON)
    record = _item_of(queue, "b" * 64)
    assert record["interpreter"] == PQ_PYTHON
    assert pb.INTERPRETER_TAG in record["tags"]


def test_publish_refuses_a_relative_path_and_a_bare_capability(tmp_path):
    queue = _queue_at(tmp_path)
    with pytest.raises(pool.PoolContractError, match="absolute"):
        _publish(queue, "c" * 64, interpreter="venv/bin/python")
    with pytest.raises(pool.PoolContractError, match="interpreter"):
        _publish(queue, "d" * 64, tags=[pb.INTERPRETER_TAG])
    assert not queue.item_path(pool.READY, "c" * 64).exists()
    assert not queue.item_path(pool.READY, "d" * 64).exists()


def test_an_item_without_an_interpreter_is_byte_identical_to_before(tmp_path):
    queue = _queue_at(tmp_path)
    _publish(queue, "e" * 64)
    record = _item_of(queue, "e" * 64)
    assert "interpreter" not in record
    assert pb.INTERPRETER_TAG not in record["tags"]


def test_the_offer_records_only_what_it_positively_answers(tmp_path):
    queue = _queue_at(tmp_path)
    _announce(queue, "answered", interpreters=[TF516_PYTHON, PQ_PYTHON])
    _announce(queue, "silent", interpreters=None)
    answered = json.loads((queue.root / "workers" / "answered.json").read_text())
    assert answered["interpreters"] == [TF516_PYTHON, PQ_PYTHON]
    silent = json.loads((queue.root / "workers" / "silent.json").read_text())
    assert "interpreters" not in silent


# --------------------------------------------------------------------------
# The worker's lookup and the claim's local check
# --------------------------------------------------------------------------

def test_the_lookup_answers_exactly_the_paths_items_ask_about(tmp_path):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]
                           / "tools" / "fleet"))
    import worker_loop

    def make(name, *, executable=True):
        path = tmp_path / name
        path.write_text("#!/bin/sh\n")
        mode = path.stat().st_mode
        os.chmod(path, mode | (stat.S_IXUSR if executable else 0))
        return str(path)

    present_a = make("python-a")
    present_b = make("python-b")
    make("not-executable", executable=False)
    items = [
        {"interpreter": present_b},
        {"interpreter": present_a},
        {"interpreter": str(tmp_path / "missing" / "bin" / "python")},
        {"interpreter": "relative/bin/python"},
        {},
    ]

    present, absent = worker_loop.interpreter_lookup(items)
    assert present == [present_a, present_b]
    assert absent == [str(tmp_path / "missing" / "bin" / "python"),
                      "relative/bin/python"]


def test_the_claim_denies_an_interpreter_this_box_does_not_have(tmp_path):
    queue = _queue_at(tmp_path)
    _publish(queue, "f" * 64, resources={"cpu": 1, "mem_gb": 1},
             interpreter="/no/such/bin/python")

    assert queue.claim(capacity=CAPACITY,
                       tags=[pb.INTERPRETER_TAG, "gb10"]) is None
    denial = _denials(queue)[-1]
    assert denial["reason"] == "interpreter_not_present"
    assert denial["evidence"]["interpreter"] == "/no/such/bin/python"
    assert queue.item_path(pool.READY, "f" * 64).exists()


def test_the_claim_takes_an_interpreter_this_box_has(tmp_path):
    queue = _queue_at(tmp_path)
    _publish(queue, "1" * 64, resources={"cpu": 1, "mem_gb": 1},
             interpreter=sys.executable)

    claimed = queue.claim(capacity=CAPACITY,
                          tags=[pb.INTERPRETER_TAG, "gb10"])

    assert claimed is not None


# --------------------------------------------------------------------------
# What the submit tools derive and refuse
# --------------------------------------------------------------------------

def test_pbrun_derives_the_interpreter_from_an_absolute_python():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]
                           / "tools" / "fleet"))
    import pbrun

    assert pbrun.interpreter_of(
        ["/home/rob/venvs/x/bin/python", "-m", "pytest"]) == \
        "/home/rob/venvs/x/bin/python"
    assert pbrun.interpreter_of(["python3", "-m", "pytest"]) is None
    assert pbrun.interpreter_of(["/usr/bin/env", "python"]) is None
    assert pbrun.interpreter_of([]) is None


def test_pbtest_refuses_a_path_no_recorded_worker_reports(tmp_path, capsys):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]
                           / "tools" / "fleet"))
    import pbrun
    import pbtest

    # A unanimous absent is the refusal: every capable offer answered, and
    # every answer named the path missing (the fleet below is the new
    # generation with a READY row already asking for this path).
    queue = _queue_at(tmp_path)
    _announce_live(queue, "dl380g10", absent=["/no/such/venv/bin/python"])

    kind, message = pbtest.interpreter_refusal(
        queue, "/no/such/venv/bin/python",
        tags=[], resources={"cpu": 1}, needs_gpu=False)
    assert kind == "refusal"
    assert "/no/such/venv/bin/python" in message

    fine_kind, _ = pbtest.interpreter_refusal(
        queue, PQ_PYTHON, tags=[], resources={"cpu": 1}, needs_gpu=False)
    assert fine_kind is None or fine_kind == "notice"

    # A fleet whose offers predate the field offers no capability, so the
    # pre-flight stays a notice rather than a suite-wide guess: the
    # per-shard pbrun refusal is the fail-closed answer there.
    legacy = _queue_at(tmp_path / "legacy")
    _announce(legacy, "old-box", tags=(), interpreters=None)
    kind, _ = pbtest.interpreter_refusal(
        legacy, "/no/such/venv/bin/python",
        tags=[], resources={"cpu": 1}, needs_gpu=False)
    assert kind in (None, "notice")


# --- review round 1 (#1266): the first submission must pass the pre-flight ---

def _announce_live(queue, host, *, present=(), absent=(),
                   tags=(pb.INTERPRETER_TAG,)):
    _announce(queue, host, tags=tags, interpreters=list(present) or None)
    record_path = queue.root / "workers" / f"{host}.json"
    record = json.loads(record_path.read_text())
    if absent:
        record["interpreters_absent"] = sorted(absent)
    record_path.write_text(json.dumps(record))


def test_a_first_submission_passes_the_preflight(tmp_path, capsys):
    """REVIEW-1266 r1 [P1]: a path no READY row names yet is on no offer.

    The live shape of a fresh new-generation offer: the capability is
    offered, and both answer lists are empty, because nothing in READY asks
    about any path.  A first submission naming a real venv must pass --
    refused here would refuse every pbtest after the publish.
    """

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]
                           / "tools" / "fleet"))
    import pbtest

    queue = _queue_at(tmp_path)
    _announce_live(queue, "sparky")
    _announce_live(queue, "sparklina")

    kind, message = pbtest.interpreter_refusal(
        queue, PQ_PYTHON, tags=[], resources={"cpu": 1}, needs_gpu=False)

    assert kind in (None, "notice"), (kind, message)
    if kind == "notice":
        assert PQ_PYTHON in message


def test_pbrun_publishes_a_first_submission_with_a_notice(tmp_path, capsys):
    """The same first-submission shape at pbrun's refusal site: no raise."""

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]
                           / "tools" / "fleet"))
    import pbrun

    queue = _queue_at(tmp_path)
    _announce_live(queue, "sparky")
    _announce_live(queue, "sparklina")

    verdict = pbrun.interpreter_submission_verdict(
        queue, PQ_PYTHON, tags=[])

    assert verdict[0] in ("unknown", "present"), verdict


def test_a_unanimous_absent_is_refused_naming_the_path(tmp_path):
    """Every capable offer answered, and every answer was absent."""

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]
                           / "tools" / "fleet"))
    import pbtest

    queue = _queue_at(tmp_path)
    _announce_live(queue, "sparky", absent=[PQ_PYTHON])
    _announce_live(queue, "sparklina", absent=[PQ_PYTHON])

    kind, message = pbtest.interpreter_refusal(
        queue, PQ_PYTHON, tags=[], resources={"cpu": 1}, needs_gpu=False)

    assert kind == "refusal"
    assert PQ_PYTHON in message


def test_one_present_answer_beats_every_absent_one(tmp_path):
    """A split fleet answers present somewhere: the row is placeable."""

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]
                           / "tools" / "fleet"))
    import pbtest

    queue = _queue_at(tmp_path)
    _announce_live(queue, "sparky", absent=[PQ_PYTHON])
    _announce_live(queue, "sparklina", present=[PQ_PYTHON])

    kind, message = pbtest.interpreter_refusal(
        queue, PQ_PYTHON, tags=[], resources={"cpu": 1}, needs_gpu=False)

    assert kind is None

"""A box-local checkout reaches the SLURM launcher the way it reaches the pool.

The launcher builds the materializer's item from the sealed action.  It
accepted only snapshot-addressed actions, so an action pinned to its own box
with ``--here`` -- a checkout root, not bytes -- was refused on the node after
the scheduler had already placed it there.  Both shapes the pool carries are
now carried; an action addressed neither way still has no tree to run in.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

LAUNCHER = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "slurm_job.py"
KEY = "k" * 64


@pytest.fixture(scope="module")
def slurm_job():
    spec = importlib.util.spec_from_file_location("slurm_job_under_test", LAUNCHER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_a_snapshot_addressed_action_carries_its_snapshot(slurm_job) -> None:
    snapshot = {"schema": 2, "commit": "c" * 40, "input": "i" * 64}
    item = slurm_job._queue_item(
        {"action_key": KEY, "params": {"checkout_snapshot": snapshot}},
        cas_root=Path("/cas"),
    )
    assert item == {
        "action_key": KEY, "cas_root": "/cas", "checkout_snapshot": snapshot,
    }


def test_a_root_addressed_action_carries_its_root(slurm_job) -> None:
    item = slurm_job._queue_item(
        {"action_key": KEY, "params": {"checkout_root": "/home/rob/prismabuild"}},
        cas_root=Path("/cas"),
    )
    assert item == {
        "action_key": KEY, "cas_root": "/cas",
        "checkout_root": "/home/rob/prismabuild",
    }


def test_a_snapshot_wins_over_a_root_when_both_are_present(slurm_job) -> None:
    """The pool refuses the pair at publish time; the launcher, given one
    anyway, takes the immutable address."""

    snapshot = {"schema": 2, "commit": "c" * 40, "input": "i" * 64}
    item = slurm_job._queue_item(
        {"action_key": KEY, "params": {
            "checkout_snapshot": snapshot, "checkout_root": "/elsewhere"}},
        cas_root=Path("/cas"),
    )
    assert "checkout_root" not in item


@pytest.mark.parametrize("params", [None, {}, {"checkout_root": ""}])
def test_an_action_addressed_neither_way_is_refused(slurm_job, params) -> None:
    with pytest.raises(SystemExit, match="neither a checkout snapshot nor"):
        slurm_job._queue_item(
            {"action_key": KEY, "params": params}, cas_root=Path("/cas")
        )

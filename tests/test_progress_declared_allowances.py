"""The action sees the stall allowance the worker enforces, not only names.

#1242: an action that paces its own reports must repeat, in its own
arguments, a number the worker already holds.  That copy can drift from the
sealed policy, including a clamp the worker applied, and nothing checks it.
The launch environment therefore carries ``PRISMABUILD_ACTION_PROGRESS_
ALLOWANCES``, a JSON object ``{name: effective_grace_s}`` in declared order
-- the resolved number after defaults and overrides, the same shape the
controller enforces -- and ``progress.declared_allowances()`` reads it back.
"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb, pool, progress  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_progress_keeps_a_working_action_alive import (  # noqa: E402
    _claimed, _policy,
)


def test_declared_allowances_round_trips_in_declared_order(monkeypatch):
    """The reader returns the enforced numbers, in the order sealed."""

    monkeypatch.setenv(pb.ACTION_PROGRESS_ALLOWANCES_ENV,
                       json.dumps({"startup": 60.0, "run": 30.0,
                                   "publish": 60.0}))
    assert progress.declared_allowances() == {"startup": 60.0, "run": 30.0,
                                              "publish": 60.0}


def test_declared_allowances_is_none_without_the_export(monkeypatch):
    """No contract, or a worker generation that predates the export."""

    monkeypatch.delenv(pb.ACTION_PROGRESS_ALLOWANCES_ENV, raising=False)
    assert progress.declared_allowances() is None


@pytest.mark.parametrize("raw", [
    "",
    "[]",
    "{}",
    "not json",
    '{"startup": "60"}',
    '{"startup": true}',
    '{"startup": null}',
    '{"startup": "NaN"}',
    '{"startup": NaN}',
    '{"startup": Infinity}',
    '{"": 60.0}',
    '{"startup": 60.0, "run": "x"}',
])
def test_declared_allowances_rejects_an_unusable_export(monkeypatch, raw):
    """A corrupt export reads as unknown, never as a number to pace by."""

    monkeypatch.setenv(pb.ACTION_PROGRESS_ALLOWANCES_ENV, raw)
    assert progress.declared_allowances() is None


def _launched_env(tmp_path, policy, timeout_s):
    queue, item = _claimed(tmp_path, mode="report", seconds=0.1,
                           policy=policy)
    launched: dict[str, str] = {}
    original = pool.subprocess.Popen

    def record(argv, **kwargs):
        launched.update(kwargs["env"])
        return original(argv, **kwargs)

    pool.subprocess.Popen = record
    try:
        outcome = queue.execute(item, timeout_s=timeout_s,
                                heartbeat_s=0.05, timeout_grace_s=0.2)
    finally:
        pool.subprocess.Popen = original
    assert outcome["status"] == "executed", outcome.get("stderr")
    return launched


def test_the_worker_exports_the_enforced_allowances(tmp_path):
    """The export is post-clamp: the number the watchdog will enforce."""

    launched = _launched_env(tmp_path, _policy(60, 60, 60), timeout_s=30)
    assert json.loads(launched[pb.ACTION_PROGRESS_ALLOWANCES_ENV]) == {
        "startup": 30, "run": 30, "publish": 30}
    # The names export is unchanged: names only, as before.
    assert json.loads(launched[pb.ACTION_PROGRESS_PHASES_ENV]) == [
        "startup", "run", "publish"]


def test_the_worker_exports_unclamped_allowances_whole(tmp_path):
    """A ceiling above every request changes nothing in the export."""

    launched = _launched_env(tmp_path, _policy(60, 60, 60), timeout_s=600)
    assert json.loads(launched[pb.ACTION_PROGRESS_ALLOWANCES_ENV]) == {
        "startup": 60, "run": 60, "publish": 60}


def test_an_action_that_did_not_declare_is_told_no_allowances(tmp_path):
    """No contract, no variables: an ordinary action's launch is unchanged."""

    launched = _launched_env(tmp_path, None, timeout_s=5)
    assert pb.ACTION_PROGRESS_ALLOWANCES_ENV not in launched

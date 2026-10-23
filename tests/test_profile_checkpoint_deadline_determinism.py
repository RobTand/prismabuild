"""Acceptance for #1969: exercise the existing regression without a live deadline.

The test being repaired remains in test_profile_window_deadline.py. These guards
allow either simulated execution/clock or a deadline derived from a completed
relay record; they must not let deleting the end-time regression count as a fix.
"""
import inspect
import subprocess

import pytest


@pytest.mark.parametrize(
    ('action_sleep', 'times_out'),
    [(0.30, True), (0.02, False)],
    ids=('action-ended-after-deadline', 'action-ended-before-deadline'),
)
def test_checkpoint_deadline_regression_is_independent_of_live_scheduling(
    tmp_path, monkeypatch, action_sleep, times_out,
):
    from prismabuild import core as pb
    from test_profile_window_deadline import (
        test_profile_checkpoint_delay_uses_the_action_end_time_for_a_deadline
        as regression,
    )

    real_wait = subprocess.Popen.wait
    runner_code = pb.run_local_action.__code__

    def wait_without_live_action_budget(self, timeout=None):
        # A simulated process may have a logical timeout. A real profiler must
        # not need to start/exit within the very deadline whose semantics this
        # test is trying to establish. Polling/cleanup watchdogs are unaffected.
        caller = inspect.currentframe().f_back
        if caller.f_code is runner_code:
            assert timeout is None, (
                'checkpoint end-time regression still budgets real process '
                'scheduling; simulate the process/clock or derive the tested '
                'deadline from its recorded end time'
            )
        return real_wait(self, timeout=timeout)

    monkeypatch.setattr(subprocess.Popen, 'wait', wait_without_live_action_budget)
    real_ingest = pb._ProfileSession.ingest
    real_action_returncode = pb._ProfileSession.action_returncode
    ingests = []
    comparisons = []

    def observe_ingest(self, cas):
        record = real_ingest(self, cas)
        ingests.append(record)
        return record

    def observe_end_time(self, process=None, *, deadline=None):
        comparisons.append((
            self.exit_status(), deadline, pb.time.monotonic(), len(ingests),
        ))
        return real_action_returncode(self, process, deadline=deadline)

    monkeypatch.setattr(pb._ProfileSession, 'ingest', observe_ingest)
    monkeypatch.setattr(pb._ProfileSession, 'action_returncode', observe_end_time)
    regression(tmp_path, monkeypatch, action_sleep, times_out)

    assert comparisons, 'retain the production recorded-end-time comparison'
    status, deadline, checkpoint_time, checkpoint_count = comparisons[0]
    assert checkpoint_count, 'retain the completed profile checkpoint before settlement'
    assert status['phase'] == 'ended', 'observe completion instead of guessing a sleep'
    assert deadline is not None, 'retain an explicit deadline comparison'
    assert checkpoint_time > deadline, 'checkpoint delay must cross the tested deadline'
    if times_out:
        assert status['ended_monotonic'] > deadline
    else:
        assert status['ended_monotonic'] <= deadline

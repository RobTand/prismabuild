"""A run's own exit status never impersonates one of ``pbrun``'s conditions.

``pbrun`` reserves four non-zero exit codes for things it decides itself: 2 for
a refusal, 74 for a record it could not write, 75 for no verdict yet, and 143
for a withdrawal.  Both report paths used to return the number out of the
terminal record verbatim, so a run whose launcher exited on one of those
numbers reached the caller as ``pbrun``'s own word for something else.

143 is not hypothetical.  ``core._sigterm_unwinds_this_process`` raises
``SystemExit(128 + signum)``, so any SIGTERM that is not a withdrawal leaves
the launcher exiting 143: an operator's ``kill``, a worker loop restarting
under it, a supervisor tidying up.  ``PoolQueue.execute`` files that under
``failed/`` with ``detail.returncode`` 143, and the caller read it as "an
operator withdrew this on purpose" with no marker anywhere to back that up.

75 is the mirror image.  A launcher exiting 75 read as "no verdict yet, run
``pbwait`` again", which sends the operator to wait on work that already
finished and lost.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402

import pbrun  # noqa: E402

from test_slurm_lane import _paper_action, fleet  # noqa: E402,F401

KEY = "cd" * 32


def _failed_record(root: Path, *, returncode: int) -> pool.PoolQueue:
    """A pull-queue ``failed/`` ending whose launcher exited ``returncode``."""

    queue = pool.PoolQueue(root)
    directory = root / pool.FAILED
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{KEY}.json").write_text(json.dumps({
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "action_key": KEY,
        "status": "failed",
        "attempts": 1,
        "published_unix": 100.0,
        "finished_host": "sparky",
        "detail": {
            "returncode": returncode,
            "stdout": "",
            "stderr": "",
            "elapsed_s": 1.0,
        },
    }), encoding="utf-8")
    return queue


def test_a_launcher_killed_by_a_stray_term_does_not_read_as_a_withdrawal(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """143 from the run is a failure, not ``WITHDRAWN_EXIT``.

    Nothing withdrew this action.  There is no marker in ``withdrawn/``, the
    record is in ``failed/``, and ``pool_reset`` would resubmit it.  Reporting
    it as 143 told the operator a decision had been made.
    """

    queue = _failed_record(tmp_path / "pb-queue", returncode=143)

    code = pbrun.await_outcome(queue, KEY, wait_s=0.0, generation=100.0)

    assert code != pbrun.WITHDRAWN_EXIT
    assert code == 1
    assert "143" in capsys.readouterr().err


def test_a_run_that_exits_seventy_five_is_not_no_verdict_yet(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """75 from the run is a failure, not ``GAVE_UP_EXIT``."""

    queue = _failed_record(tmp_path / "pb-queue", returncode=75)

    code = pbrun.await_outcome(queue, KEY, wait_s=0.0, generation=100.0)

    assert code != pbrun.GAVE_UP_EXIT
    assert code == 1
    assert "75" in capsys.readouterr().err


#: What the fix reserves.  Spelled out here rather than imported, so this file
#: still collects against the code that has no such constant.
RESERVED = (2, 74, 75, 143)


@pytest.mark.parametrize("returncode", RESERVED)
def test_every_reserved_code_is_reported_as_a_failure(
    tmp_path: Path, returncode: int, capsys: pytest.CaptureFixture[str]
) -> None:
    """The whole reserved vocabulary, not the two the audit happened to name."""

    queue = _failed_record(tmp_path / f"pb-queue-{returncode}",
                           returncode=returncode)

    assert pbrun.await_outcome(
        queue, KEY, wait_s=0.0, generation=100.0) == 1
    assert str(returncode) in capsys.readouterr().err


def test_a_status_pbrun_does_not_reserve_still_reaches_the_caller(
    tmp_path: Path
) -> None:
    """Every other number is passed through, which is the whole contract."""

    queue = _failed_record(tmp_path / "pb-queue", returncode=7)

    assert pbrun.await_outcome(queue, KEY, wait_s=0.0, generation=100.0) == 7


def test_the_lane_reports_a_job_that_exited_143_as_a_failure(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The SLURM path has its own return site, and it reserves the same words.

    A guard on one of the two would leave the transport an action rode decide
    whether its exit status can be trusted.
    """

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:143")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "signalled")
    request = cas.publish_action_request(action)

    code = pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=[], demand={},
        exclusive=False, timeout_s=600.0, wait_s=60.0, retry_safe=False,
        max_attempts=1, runtime_root=REPOSITORY, poll_s=0.0,
    )

    assert code != pbrun.WITHDRAWN_EXIT
    assert code == 1
    assert "143" in capsys.readouterr().err


def test_the_reserved_set_is_exactly_pbruns_own_words() -> None:
    """2, 74, 75 and 143 are the codes ``pbrun`` decides for itself."""

    assert sorted(pbrun.RESERVED_EXITS) == sorted(RESERVED)
    assert pbrun.GAVE_UP_EXIT in pbrun.RESERVED_EXITS
    assert pbrun.WITHDRAWN_EXIT in pbrun.RESERVED_EXITS
    assert pbrun.RECORD_WRITE_FAILED_EXIT in pbrun.RESERVED_EXITS

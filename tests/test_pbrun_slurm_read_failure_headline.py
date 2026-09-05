"""A read the lane could not make is not a record that would not write.

PR #54 gave ``pbrun`` a handler for the one failure it was written for: a
terminal or submission record that will not write at a point where the job is
real and the work may be finished. It prints the job id, the path, the errno
text and the ``pbwait`` that files the ending, and exits 74.

The handler was hung on the whole ``slurm_lane.run``/``resume`` call, and that
call does more than write records. It asks the CAS whether the receipt landed,
and that lookup verifies the result blob, which on a rendered model is
gigabytes hashed over NFS. An ``OSError`` from there was reported under "pbrun
could not write its record". The path and the errno stayed true, so nobody was
lied to about facts, but the headline sent an operator to look for a record
nothing was writing and the advice told them to clear what blocked a write that
never happened.

``slurm_lane._naming_job`` wraps exactly the three post-``sbatch`` write sites
and stamps ``job_id`` on whatever they raise, so the attribute's presence is
the lane's own answer to which failure this was. A read carries no stamp.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402

import pbrun  # noqa: E402

from test_slurm_lane import _paper_action, fleet  # noqa: E402,F401


class _CASThatStopsAnswering:
    """A CAS whose ``lookup`` refuses once the job has run.

    The first lookup is ``slurm_outcome``'s own pre-submission check, which has
    to answer so that the action is submitted at all. The next is the one
    ``slurm_lane.run`` makes after the job ends, and that is the read this test
    is about.
    """

    def __init__(self, real, *, answers: int = 1):
        self._real = real
        self._left = answers
        self.root = real.root

    def __getattr__(self, name):
        return getattr(self._real, name)

    def lookup(self, action):
        if self._left > 0:
            self._left -= 1
            return self._real.lookup(action)
        raise OSError(5, "Input/output error",
                      str(Path(self.root) / "receipts"))


def test_a_read_the_lane_could_not_make_is_not_reported_as_an_unwritten_record(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The facts stay; the headline stops naming a write that never happened."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:0")
    real = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "unreadable-cas")
    request = real.publish_action_request(action)
    cas = _CASThatStopsAnswering(real)

    code = pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=[], demand={},
        exclusive=False, timeout_s=600.0, wait_s=60.0, retry_safe=False,
        max_attempts=1, runtime_root=REPOSITORY, poll_s=0.0,
    )

    err = capsys.readouterr().err
    assert code == pbrun.RECORD_WRITE_FAILED_EXIT
    assert "could not write its record" not in err
    # The facts PR #54 put on the line are the reason this is not a traceback,
    # so they have to survive the narrowing.
    assert "Input/output error" in err
    assert "receipts" in err
    assert f"pbwait.py {action['action_key'][:12]}" in err


def test_a_record_that_will_not_write_keeps_the_headline_it_had(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The narrowing must not cost the case the handler was written for."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:0")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "unwritable-queue")
    request = cas.publish_action_request(action)
    queue = tmp_path / "pb-queue"
    (queue / "failed").mkdir(parents=True)
    (queue / "failed").chmod(0o555)
    try:
        code = pbrun.slurm_outcome(
            action, cas=cas, request_path=request, tags=[], demand={},
            exclusive=False, timeout_s=600.0, wait_s=60.0, retry_safe=False,
            max_attempts=1, runtime_root=REPOSITORY, queue_root=queue,
            poll_s=0.0,
        )
    finally:
        (queue / "failed").chmod(0o755)

    err = capsys.readouterr().err
    assert code == pbrun.RECORD_WRITE_FAILED_EXIT
    assert "could not write its record" in err
    assert "slurm job:" in err


def test_a_pre_submission_filesystem_failure_does_not_claim_slurm_took_the_action(
    tmp_path, fleet, monkeypatch, capsys,
):
    """Creating the lane directory can fail before sbatch is invoked."""
    action = _paper_action(tmp_path, 'pre-submit-io')
    cas = pb.PrismaBuildCAS(tmp_path / 'cas')
    request = cas.publish_action_request(action)
    def fail(*args, **kwargs):
        raise PermissionError(13, 'Permission denied', str(tmp_path / 'lane'))
    monkeypatch.setattr(pbrun.slurm_lane, 'submit', fail)
    assert pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=[], demand={},
        exclusive=False, timeout_s=600, wait_s=1, retry_safe=False,
        max_attempts=1, runtime_root=REPOSITORY, poll_s=0,
    ) == pbrun.RECORD_WRITE_FAILED_EXIT
    err = capsys.readouterr().err
    assert 'slurm took this action' not in err
    assert 'could not write its record' not in err
    assert 'Permission denied' in err
    assert 'retry the original command' in err

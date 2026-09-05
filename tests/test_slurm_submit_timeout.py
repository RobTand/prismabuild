"""An ``sbatch`` that is accepted and then hangs must not lose its job.

``_run`` gives every scheduler command ``COMMAND_TIMEOUT_S``.  For ``sbatch``
that bound sat in the wrong place: the controller can accept a submission and
the client can then hang past it, so the job id was lost and the lane reported
``slurm refused this action`` for a job that was queued.  Nothing recorded it,
so nothing could wait on it, withdraw it or report it -- and a retry would have
been a second job of the same action.

So every invocation names itself in the job's ``Comment`` and a timeout asks
the controller whether it took the job.  Issue #42.
"""
from __future__ import annotations

import json
from pathlib import Path
import re
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import slurm_lane as sl  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402

from test_slurm_singleton import ACTION, FakeCAS, KEY, REPOSITORY  # noqa: E402

LANE_JOB = REPOSITORY / "tools" / "fleet" / "slurm_job.py"
WORKER = REPOSITORY / "tools" / "prismabuild_worker.py"


def _reply(stdout: str = "", *, returncode: int = 0, stderr: str = ""):
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=stdout, stderr=stderr)


def _hanging_sbatch(seen: list[list[str]]):
    """Accepted by the controller, and then no answer to the client."""

    def command(argv):
        seen.append(list(argv))
        raise subprocess.TimeoutExpired(
            cmd=["sbatch", *argv], timeout=sl.COMMAND_TIMEOUT_S)

    return command


def _sent_comment(seen: list[list[str]]) -> str:
    return next(flag.split("=", 1)[1] for flag in seen[0]
                if flag.startswith("--comment="))


def _controller(seen, asked, *, jobs=(), reply=None):
    """A ``squeue`` that lists ``jobs`` carrying this invocation's comment."""

    def command(argv):
        asked.append(list(argv))
        if reply is not None:
            return reply
        comment = _sent_comment(seen)
        return _reply("".join(f"{job}|{comment}\n" for job in jobs))

    return command


def _submit(tmp_path: Path, **kwargs):
    return sl.submit(
        ACTION,
        cas=FakeCAS(tmp_path / "cas"),
        request_path=tmp_path / "request.json",
        resources=sl.LaneResources(cpus=2, memory_mib=4096),
        timeout_s=None,
        worker_script=WORKER,
        job_entry=LANE_JOB,
        root=tmp_path / "lane",
        **kwargs,
    )


# --------------------------------------------------------------------------
# Naming one invocation
# --------------------------------------------------------------------------

def test_every_submission_names_this_invocation_of_sbatch(
    tmp_path: Path
) -> None:
    """The job name cannot do this: every attempt of every submission of one
    action key shares it, so it identifies the action and not the invocation.
    """

    seen: list[list[str]] = []

    def accepts(argv):
        seen.append(list(argv))
        return _reply("1000\n")

    first = _submit(tmp_path, sbatch=accepts)
    second = _submit(tmp_path / "again", sbatch=accepts)

    comments = [_sent_comment([argv]) for argv in seen]
    assert all(
        re.fullmatch(rf"pb:{KEY}:1:[0-9a-f]{{16}}", text) for text in comments)
    assert comments[0] != comments[1]
    # And it is sealed with the rest of the argv, so an operator holding the
    # record can find the job by its comment.
    recorded = json.loads(first.record_path.read_text(encoding="utf-8"))
    assert f"--comment={comments[0]}" in recorded["argv"]
    assert second.job_id == "1000"


def test_the_adoption_query_asks_the_controller_for_every_state() -> None:
    """A job accepted and finished inside the same timeout window is invisible
    to a default ``squeue``.  A job held behind a sibling and released into a
    cache hit is exactly that fast, and matching on the comment is what makes
    the wider listing safe."""

    argv = [str(part) for part in sl.adoption_argv(KEY, squeue="squeue")]
    assert argv[0] == "squeue"
    assert "--states=all" in argv
    assert f"--name=pb-{KEY[:12]}" in argv
    assert argv[-2:] == ["-o", "%i|%k"]
    assert "-u" in argv


# --------------------------------------------------------------------------
# Finding the job again
# --------------------------------------------------------------------------

def test_a_hung_sbatch_the_controller_took_is_adopted(tmp_path: Path) -> None:
    """The record is written exactly as if ``sbatch`` had printed that id."""

    seen: list[list[str]] = []
    asked: list[list[str]] = []
    job = _submit(tmp_path, sbatch=_hanging_sbatch(seen),
                  squeue=_controller(seen, asked, jobs=["4711"]))

    assert job.job_id == "4711"
    assert asked and "--states=all" in asked[0]
    recorded = json.loads(job.record_path.read_text(encoding="utf-8"))
    assert recorded["job_id"] == "4711"
    assert recorded["stdout"] == str(job.directory / "4711.out")
    # And ``latest.json`` points at it, which is what ``--withdraw``,
    # ``pbwait`` and ``pool_reset`` all read.
    latest = json.loads(
        (job.directory / "latest.json").read_text(encoding="utf-8"))
    assert latest["job_id"] == "4711"


def test_a_hung_sbatch_the_controller_never_took_is_still_a_refusal(
    tmp_path: Path
) -> None:
    """An empty answer is an answer: no job carries this invocation's comment,
    so the submission was refused and the refusal stands."""

    seen: list[list[str]] = []
    asked: list[list[str]] = []
    with pytest.raises(sl.SlurmLaneError) as raised:
        _submit(tmp_path, sbatch=_hanging_sbatch(seen),
                squeue=_controller(seen, asked, jobs=[]))

    assert not isinstance(raised.value, sl.SubmissionFateUnknown)
    assert "sbatch refused this action" in str(raised.value)
    assert f"pb-{KEY[:12]}" in str(raised.value)
    assert not list((tmp_path / "lane").glob("*/submissions/*.json"))


def test_a_hung_sbatch_the_controller_cannot_settle_is_not_a_refusal(
    tmp_path: Path
) -> None:
    """Not knowing is not the same as knowing it was refused.

    A plain refusal here is the same false negative issue #42 describes, with
    better prose on it: a job of this action may be queued right now.  So the
    message names the job name, this invocation's comment, and the ``squeue``
    an operator can run, and nothing is filed.
    """

    seen: list[list[str]] = []
    asked: list[list[str]] = []
    down = _reply(
        returncode=1,
        stderr="slurm_load_jobs error: Unable to contact slurm controller "
               "(connect failure)\n")
    with pytest.raises(sl.SubmissionFateUnknown) as raised:
        _submit(tmp_path, sbatch=_hanging_sbatch(seen),
                squeue=_controller(seen, asked, reply=down))

    said = str(raised.value)
    assert f"pb-{KEY[:12]}" in said
    assert _sent_comment(seen) in said
    assert "squeue -h -u " in said and "--states=all" in said
    assert "Nothing has been filed" in said
    assert not list((tmp_path / "lane").glob("*/submissions/*.json"))


def test_a_squeue_that_hangs_too_leaves_the_fate_unknown(
    tmp_path: Path
) -> None:
    """The same answer for the same reason: no answer is not a refusal."""

    seen: list[list[str]] = []

    def hangs(argv):
        raise subprocess.TimeoutExpired(cmd=["squeue", *argv], timeout=60.0)

    with pytest.raises(sl.SubmissionFateUnknown):
        _submit(tmp_path, sbatch=_hanging_sbatch(seen), squeue=hangs)


def test_two_jobs_carrying_one_comment_are_not_adopted(
    tmp_path: Path
) -> None:
    """One invocation cannot have produced two jobs, so neither is claimed.

    Impossible from this lane -- the nonce is fresh per invocation -- and the
    guard is what keeps a hand-replayed submission (smoke row 15a replays a
    recorded argv, comment included) from being adopted as this one's.
    """

    seen: list[list[str]] = []
    asked: list[list[str]] = []
    with pytest.raises(sl.SubmissionFateUnknown) as raised:
        _submit(tmp_path, sbatch=_hanging_sbatch(seen),
                squeue=_controller(seen, asked, jobs=["4711", "4712"]))

    assert "4711, 4712" in str(raised.value)
    assert not list((tmp_path / "lane").glob("*/submissions/*.json"))


# --------------------------------------------------------------------------
# What the submitter is told
# --------------------------------------------------------------------------

def test_pbrun_reports_an_unknown_fate_as_no_verdict(
    tmp_path: Path, capsys
) -> None:
    """Exit 75, which already means the work has no verdict yet, and not 1,
    which means the action failed."""

    seen: list[list[str]] = []
    down = _reply(
        returncode=1,
        stderr="slurm_load_jobs error: Unable to contact slurm controller "
               "(connect failure)\n")
    code = pbrun.slurm_outcome(
        ACTION,
        cas=FakeCAS(tmp_path / "cas"),
        request_path=tmp_path / "request.json",
        tags=[], demand={"cpu": 1, "mem_gb": 4}, exclusive=False,
        timeout_s=None, wait_s=30.0, retry_safe=False, max_attempts=1,
        queue_root=tmp_path / "queue", lane_root=tmp_path / "lane",
        sbatch=_hanging_sbatch(seen),
        squeue=_controller(seen, [], reply=down),
    )

    err = capsys.readouterr().err
    assert code == pbrun.GAVE_UP_EXIT
    assert "the fate of this submission is unknown" in err
    assert "slurm refused this action" not in err
    assert f"pb-{KEY[:12]}" in err
    assert "squeue -h -u " in err
    assert not list((tmp_path / "queue").glob("*/*.json"))

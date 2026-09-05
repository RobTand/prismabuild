"""One job per action key at a time, and an honest word for the second one.

Two ``pbrun``s of one key used to become two jobs of one action: the key is a
content hash, so asking for the same work twice is the ordinary case, and the
window between one caller's CAS lookup and the other's ``sbatch`` is not one
any submitter-side check can close.  The scheduler closes it instead --
``--dependency=singleton`` under the job name ``pb-<key12>`` -- and the tests
here cover what that costs the second caller: it waits, it is told it is
waiting and for which job, it materializes nothing, and its ending says
``cache_hit`` rather than ``executed`` with somebody else's elapsed time.

Issue #43.
"""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import slurm_lane as sl  # noqa: E402

from test_slurm_lane import _runnable_action  # noqa: E402

REPOSITORY = Path(__file__).resolve().parents[1]
LAUNCHER = REPOSITORY / "tools" / "fleet" / "slurm_job.py"
KEY = "ab" * 32
DIGEST = "d" * 64

#: What ``squeue`` lists.  A finished job is the controller's business only
#: until ``MinJobAge``, and this is the set that makes ``squeue`` the wrong
#: tool for one and the right tool for a queued job.
LIVE = {"PENDING", "RUNNING", "CONFIGURING", "COMPLETING"}


# --------------------------------------------------------------------------
# One controller, driven in process
# --------------------------------------------------------------------------

class FakeScheduler:
    """``sbatch``/``sacct``/``scontrol``/``squeue`` as ``Command`` callables.

    In process rather than on PATH because these tests drive ``wait`` through
    a whole queue's worth of polls on a fake clock, and because the question
    under test is what the lane *asks* -- which is an argv, readable here
    without a process to read it out of.

    ``sacct`` refuses, as it does on this fleet: there is no slurmdbd, so
    ``scontrol`` and ``squeue`` are the two that answer.
    """

    def __init__(self, *, state: str = "COMPLETED") -> None:
        self.next_id = 1000
        self.jobs: dict[str, dict[str, str]] = {}
        self.submissions: list[list[str]] = []
        self.default_state = state
        self.cancelled: list[str] = []
        self.name_queries: list[str] = []

    # -- what the lane gets back -------------------------------------------
    @staticmethod
    def _reply(stdout: str = "", *, returncode: int = 0, stderr: str = ""):
        return subprocess.CompletedProcess(
            args=[], returncode=returncode, stdout=stdout, stderr=stderr)

    def place(self, *, name: str, state: str, reason: str = "None") -> str:
        """Put one job in the controller's memory and return its id."""

        job_id = str(self.next_id)
        self.next_id += 1
        self.jobs[job_id] = {
            "name": name, "state": state, "reason": reason, "exit": "0:0"}
        return job_id

    def _render(self, job_id: str, fmt: str) -> str:
        job = self.jobs[job_id]
        return (fmt.replace("%i", job_id)
                   .replace("%T", job["state"])
                   .replace("%j", job["name"])
                   .replace("%r", job["reason"]))

    # -- the commands ------------------------------------------------------
    def sbatch(self, argv: list[str]):
        self.submissions.append(list(argv))
        name = next(
            (a.split("=", 1)[1] for a in argv if a.startswith("--job-name=")),
            "",
        )
        return self._reply(
            self.place(name=name, state=self.default_state) + "\n")

    def sacct(self, argv: list[str]):
        return self._reply(
            returncode=1,
            stderr="sacct: error: Slurm accounting storage is disabled\n")

    def scontrol(self, argv: list[str]):
        job = self.jobs.get(argv[-1])
        if job is None:
            return self._reply(
                returncode=1,
                stderr="slurm_load_jobs error: Invalid job id specified\n")
        return self._reply(
            f"JobId={argv[-1]} JobName={job['name']} JobState={job['state']} "
            f"Reason={job['reason']} ExitCode={job['exit']} "
            f"StartTime=2026-09-05T10:00:00 EndTime=2026-09-05T10:00:10 "
            f"RunTime=00:00:10 NodeList=sparky Partition=all\n")

    def squeue(self, argv: list[str]):
        fmt = argv[argv.index("-o") + 1] if "-o" in argv else "%T"
        name = next(
            (a.split("=", 1)[1] for a in argv if a.startswith("--name=")), None)
        if name is not None:
            self.name_queries.append(name)
            listed = [
                self._render(job_id, fmt)
                for job_id, job in self.jobs.items()
                if job["name"] == name and job["state"] in LIVE
            ]
            return self._reply("".join(f"{line}\n" for line in listed))
        job = self.jobs.get(argv[argv.index("-j") + 1])
        if job is None or job["state"] not in LIVE:
            return self._reply("")
        return self._reply(
            self._render(argv[argv.index("-j") + 1], fmt) + "\n")

    def sstat(self, argv: list[str]):
        return self._reply(
            returncode=1, stderr="sstat: error: no steps running for job\n")

    def scancel(self, argv: list[str]):
        self.cancelled.append(argv[-1])
        return self._reply()

    @property
    def commands(self) -> dict[str, object]:
        return {"sacct": self.sacct, "scontrol": self.scontrol,
                "squeue": self.squeue, "sstat": self.sstat}


class FakeClock:
    """A clock that only moves when somebody sleeps on it."""

    def __init__(self) -> None:
        self.t = 0.0

    def now(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += float(seconds)


class FakeCAS:
    """Enough of ``PrismaBuildCAS`` for the lane: a root and a lookup."""

    def __init__(self, root: Path, receipt: dict | None = None) -> None:
        self.root = root
        self.receipt = receipt
        self.lookups = 0

    def lookup(self, action):
        self.lookups += 1
        return self.receipt


ACTION = {"action_key": KEY, "params": {"checkout_root": "/srv/checkout"}}


def _submit(tmp_path: Path, fake: FakeScheduler, **kwargs):
    return sl.submit(
        ACTION,
        cas=FakeCAS(tmp_path / "cas"),
        request_path=tmp_path / "request.json",
        resources=sl.LaneResources(cpus=2, memory_mib=4096),
        timeout_s=None,
        worker_script=REPOSITORY / "tools" / "prismabuild_worker.py",
        job_entry=LAUNCHER,
        root=tmp_path / "lane",
        sbatch=fake.sbatch,
        **kwargs,
    )


def _run(tmp_path: Path, fake: FakeScheduler, *, cas: FakeCAS, **kwargs):
    clock = FakeClock()
    return sl.run(
        ACTION,
        cas=cas,
        request_path=tmp_path / "request.json",
        resources=sl.LaneResources(cpus=2, memory_mib=4096),
        timeout_s=None,
        worker_script=REPOSITORY / "tools" / "prismabuild_worker.py",
        job_entry=LAUNCHER,
        root=tmp_path / "lane",
        queue_root=tmp_path / "queue",
        sbatch=fake.sbatch,
        poll_s=1.0,
        sleep=clock.sleep,
        clock=clock.now,
        **fake.commands,
        **kwargs,
    )


# --------------------------------------------------------------------------
# What the lane asks the controller for
# --------------------------------------------------------------------------

def test_every_submission_asks_the_scheduler_to_hold_the_key_to_one_job(
    tmp_path: Path
) -> None:
    """``--dependency=singleton``, scoped by the ``pb-<key12>`` job name.

    The two flags are one mechanism and the test says so: the dependency is
    per name and per user, so the name is what makes it mean "this action key"
    rather than "everything this user submitted".
    """

    fake = FakeScheduler()
    job = _submit(tmp_path, fake)

    assert f"--job-name=pb-{KEY[:12]}" in job.argv
    assert "--dependency=singleton" in job.argv
    assert fake.submissions[0] == job.argv[1:]
    # And it is in the sealed record, which is what ``pool_reset`` and
    # ``--withdraw`` rebuild a submission out of.
    recorded = json.loads(job.record_path.read_text(encoding="utf-8"))
    assert "--dependency=singleton" in recorded["argv"]


# --------------------------------------------------------------------------
# What a held job is told
# --------------------------------------------------------------------------

def test_a_job_held_behind_its_own_key_is_reported_as_waiting(
    tmp_path: Path
) -> None:
    """PENDING with reason ``Dependency`` is the design working.

    It is not a stall (nothing sampled it, nothing to sample -- the job has no
    steps yet) and it is not a refusal.  So the wait says which job it is
    behind, in one line, and goes on waiting.
    """

    fake = FakeScheduler()
    running = fake.place(name=f"pb-{KEY[:12]}", state="RUNNING")
    job = _submit(tmp_path, fake)
    fake.jobs[job.job_id].update(state="PENDING", reason="Dependency")

    clock = FakeClock()
    notices: list[str] = []
    outcome = sl.wait(
        job, poll_s=1.0, wait_s=10.0, sleep=clock.sleep, clock=clock.now,
        on_notice=notices.append, **fake.commands,
    )

    assert outcome.state == sl.WAIT_TIMEOUT_STATE
    assert outcome.provenance is not None
    assert outcome.provenance.reason == "Dependency"
    # Once, not once per poll: a bound on chatter, the same one the outage
    # notice keeps.
    assert len(notices) == 1
    assert f"waiting for slurm job {running}" in notices[0]
    assert KEY[:12] in notices[0]
    # A report and only a report.
    assert fake.cancelled == []
    assert outcome.liveness is None or not outcome.liveness.get("stalled_since")


def test_the_wait_says_what_it_is_waiting_for_when_squeue_cannot_say_which(
    tmp_path: Path
) -> None:
    """Naming the job ahead is an enrichment, so losing it costs a name.

    A ``squeue`` that hangs past ``COMMAND_TIMEOUT_S`` must not end the wait,
    and must not turn "queued behind the first caller" into "the scheduler
    cannot be asked" -- which is what an operator reads as an outage.
    """

    fake = FakeScheduler()
    fake.place(name=f"pb-{KEY[:12]}", state="RUNNING")
    job = _submit(tmp_path, fake)
    fake.jobs[job.job_id].update(state="PENDING", reason="Dependency")

    def hangs(argv):
        raise subprocess.TimeoutExpired(cmd=["squeue", *argv], timeout=60.0)

    clock = FakeClock()
    notices: list[str] = []
    commands = {**fake.commands, "squeue": hangs}
    outcome = sl.wait(
        job, poll_s=1.0, wait_s=5.0, sleep=clock.sleep, clock=clock.now,
        on_notice=notices.append, **commands,
    )

    assert outcome.state == sl.WAIT_TIMEOUT_STATE
    assert len(notices) == 1
    assert "waiting for another job" in notices[0]
    assert "could not be asked" not in notices[0]


def test_a_job_pending_on_resources_is_not_reported_as_a_dependency(
    tmp_path: Path
) -> None:
    """Only the controller's own word ``Dependency`` means this."""

    fake = FakeScheduler()
    job = _submit(tmp_path, fake)
    fake.jobs[job.job_id].update(state="PENDING", reason="Resources")

    clock = FakeClock()
    notices: list[str] = []
    sl.wait(job, poll_s=1.0, wait_s=5.0, sleep=clock.sleep, clock=clock.now,
            on_notice=notices.append, **fake.commands)

    assert notices == []
    assert fake.name_queries == []


# --------------------------------------------------------------------------
# What the second caller's ending says
# --------------------------------------------------------------------------

def _node_found_it(fake: FakeScheduler, cas: FakeCAS):
    """Stand in for the node: mark the hit and publish the receipt."""

    def mark(job):
        sl.write_cache_hit(
            sl.cache_hit_path(job.directory, job.job_id),
            action_key=KEY, job_id=job.job_id, result_digest=DIGEST,
        )
        cas.receipt = {"result_digest": DIGEST}

    return mark


def test_a_job_that_only_read_the_receipt_files_cache_hit(
    tmp_path: Path
) -> None:
    """Not ``executed``, and not with this job's elapsed time on it."""

    fake = FakeScheduler()
    cas = FakeCAS(tmp_path / "cas")
    result = _run(tmp_path, fake, cas=cas,
                  on_submit=_node_found_it(fake, cas))

    assert result.receipt is not None
    filed = json.loads(
        (tmp_path / "queue" / pool.DONE / f"{KEY}.json").read_text())
    assert filed["status"] == "cache_hit"
    assert filed["detail"]["status"] == "cache_hit"
    assert filed["detail"]["receipt_published"] is True
    assert filed["detail"]["result_digest"] == DIGEST
    assert not (tmp_path / "queue" / pool.FAILED).exists()


def test_a_job_that_did_the_work_still_files_executed(tmp_path: Path) -> None:
    """The marker is what tells them apart, and only the node writes it."""

    fake = FakeScheduler()
    cas = FakeCAS(tmp_path / "cas")

    def publish(job):
        cas.receipt = {"result_digest": DIGEST}

    result = _run(tmp_path, fake, cas=cas, on_submit=publish)

    assert result.receipt is not None
    filed = json.loads(
        (tmp_path / "queue" / pool.DONE / f"{KEY}.json").read_text())
    assert filed["status"] == "executed"


def test_a_cache_hit_leaves_the_record_of_the_run_that_did_the_work(
    tmp_path: Path
) -> None:
    """``pbrun.cached_outcome``'s rule, applied to the job that hit.

    The record every reader wants is the one belonging to the run that did the
    work.  A re-run that ran nothing has nothing to add to it, and overwriting
    it would replace a real node, a real elapsed time and a real log tail with
    zeroes.
    """

    done = tmp_path / "queue" / pool.DONE
    done.mkdir(parents=True)
    original = json.dumps({"status": "executed", "action_key": KEY,
                           "detail": {"slurm": {"job_id": "41"}}})
    (done / f"{KEY}.json").write_text(original)

    fake = FakeScheduler()
    cas = FakeCAS(tmp_path / "cas")
    _run(tmp_path, fake, cas=cas, on_submit=_node_found_it(fake, cas))

    assert (done / f"{KEY}.json").read_text() == original


# --------------------------------------------------------------------------
# What the node does before it materializes anything
# --------------------------------------------------------------------------

def test_the_node_reads_the_cas_before_it_materializes_anything(
    tmp_path: Path
) -> None:
    """The second job of one key costs a job id, not a checkout.

    Driven without any scheduler: ``slurm_job.py`` is what the batch script
    execs, so running it twice is running the held job after the first one
    left.  ``run-local`` asks the same question, but only once the snapshot is
    checked out -- which for a large snapshot is minutes of git and disk to
    learn what one lookup on the shared mount already knows.
    """

    cas_root = tmp_path / "cas"
    cas = pb.PrismaBuildCAS(cas_root)
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    checkouts = tmp_path / "materialized"
    state_root = tmp_path / "jobs"
    lane = tmp_path / "lane" / "dir"
    lane.mkdir(parents=True)

    def job(job_id: str):
        return subprocess.run(
            [sys.executable, str(LAUNCHER),
             "--action", str(request), "--cas-root", str(cas_root),
             "--worker", str(REPOSITORY / "tools" / "prismabuild_worker.py"),
             "--worker-python", sys.executable,
             "--job-state-root", str(state_root), "--job-id", job_id,
             "--lane-dir", str(lane), "--checkout-root", str(checkouts)],
            capture_output=True, text=True,
        )

    first = job("4242")
    assert first.returncode == 0, first.stderr
    receipt = cas.lookup(action)
    assert receipt is not None
    assert not sl.read_cache_hit(sl.cache_hit_path(lane, "4242"))

    second = job("4243")
    assert second.returncode == 0, second.stderr
    assert f"{str(action['action_key'])[:12]} is already in the CAS" \
        in second.stdout
    # Nothing was materialized, so the Epilog has nothing to remove and is
    # left no state file claiming otherwise.  The first job's file stays: it
    # made a tree, and the Epilog owns node-side cleanup.
    assert (state_root / "4242.job").exists()
    assert not (state_root / "4243.job").exists()
    marker = sl.read_cache_hit(sl.cache_hit_path(lane, "4243"))
    assert marker["action_key"] == str(action["action_key"])
    assert marker["job_id"] == "4243"

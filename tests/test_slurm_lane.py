"""The SLURM lane, driven against fake scheduler binaries on PATH.

SLURM is not installed on any box in this fleet, so nothing here may import it,
call it, or assume it answered.  What it can do is pin the two halves that a
first install would otherwise discover the hard way: the exact command line the
lane sends, and the behaviour it derives from what comes back.

The argv assertions are the point of the first half.  ``sbatch`` refuses an
unknown Feature and an impossible GRES at submit time, which is the capability
gate this transport keeps -- so the flags have to be right in the same sense a
schema has to be right, and a fake binary that records its argv checks exactly
that without a controller.

The second half runs the real thing.  The fake ``sbatch`` executes the script it
was handed, synchronously, so a "job" materializes the sealed snapshot through
``prismabuild.materialize``, runs the canonical ``run-local`` worker argv inside
it, and publishes a receipt to a real CAS.  That is the claim the lane actually
makes -- that an action executed under SLURM is the same execution the pull
queue would have performed -- and only an end-to-end run can support it.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap
import uuid

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import slurm_lane as sl  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import pbrun  # noqa: E402

REPOSITORY = Path(__file__).resolve().parents[1]
WORKER = REPOSITORY / "tools" / "prismabuild_worker.py"
JOB_ENTRY = REPOSITORY / "tools" / "fleet" / "slurm_job.py"


# --------------------------------------------------------------------------
# The fake fleet: four scripts on PATH and one directory of job state.
# --------------------------------------------------------------------------

_SBATCH = '''\
import json, os, subprocess, sys
from pathlib import Path

state = Path(os.environ["FAKE_SLURM_STATE"])
state.mkdir(parents=True, exist_ok=True)
argv = sys.argv[1:]

if os.environ.get("FAKE_SBATCH_REFUSE") == "1":
    sys.stderr.write("sbatch: error: Requested node configuration is not available\\n")
    raise SystemExit(1)

counter = state / "counter"
number = int(counter.read_text()) + 1 if counter.exists() else 1000
counter.write_text(str(number))

with (state / "submissions.jsonl").open("a") as handle:
    handle.write(json.dumps({"job_id": number, "argv": argv}) + "\\n")

script = argv[-1]
directory = [a.split("=", 1)[1] for a in argv if a.startswith("--chdir=")][0]
verdict = os.environ.get("FAKE_SBATCH_VERDICT", "run")
if verdict == "run":
    out = Path(directory) / f"{number}.out"
    err = Path(directory) / f"{number}.err"
    with out.open("wb") as o, err.open("wb") as e:
        code = subprocess.run(
            ["/bin/bash", script], stdout=o, stderr=e, cwd=directory,
        ).returncode
    slurm_state = "COMPLETED" if code == 0 else "FAILED"
elif verdict.startswith("exit:"):
    code = int(verdict.split(":", 1)[1])
    slurm_state = "COMPLETED" if code == 0 else "FAILED"
else:
    code, slurm_state = 0, verdict
# A job SLURM killed at its limit or on scancel reports ExitCode=0:15 (smoke
# row 5, 2026-09-04): exit code zero, signal fifteen.  The fake says the same.
signal = 15 if slurm_state in {"TIMEOUT", "CANCELLED"} else 0
(state / f"{number}.state").write_text(f"{slurm_state}|{code}:{signal}\\n")
print(number)
'''

_SACCT = '''\
import os, sys
from pathlib import Path

if os.environ.get("FAKE_SACCT_DISABLED") == "1":
    sys.stderr.write("sacct: error: Slurm accounting storage is disabled\\n")
    raise SystemExit(1)
job = sys.argv[sys.argv.index("-j") + 1]
record = Path(os.environ["FAKE_SLURM_STATE"]) / f"{job}.state"
if not record.exists():
    raise SystemExit(0)
state, code = record.read_text().strip().split("|")
extra = "|".join([
    os.environ.get("FAKE_SACCT_START", "2026-09-04T10:00:00"),
    os.environ.get("FAKE_SACCT_END", "2026-09-04T10:00:10"),
    os.environ.get("FAKE_SACCT_ELAPSED", "00:00:10"),
    os.environ.get("FAKE_SACCT_NODELIST", "sparky"),
    os.environ.get("FAKE_SACCT_PARTITION", "all"),
])
print(f"{job}|{state}|{code}|{extra}")
print(f"{job}.batch|{state}|{code}|{extra}")
'''

_SCONTROL = '''\
import os, sys
from pathlib import Path

job = sys.argv[-1]
record = Path(os.environ["FAKE_SLURM_STATE"]) / f"{job}.state"
if not record.exists():
    sys.stderr.write(f"slurm_load_jobs error: Invalid job id specified\\n")
    raise SystemExit(1)
state, code = record.read_text().strip().split("|")
print(
    f"JobId={job} JobName=pb-test JobState={state} Reason=None ExitCode={code} "
    f"StartTime={os.environ.get('FAKE_SACCT_START', '2026-09-04T10:00:00')} "
    f"EndTime={os.environ.get('FAKE_SACCT_END', '2026-09-04T10:00:10')} "
    f"RunTime={os.environ.get('FAKE_SACCT_ELAPSED', '00:00:10')} "
    f"NodeList={os.environ.get('FAKE_SACCT_NODELIST', 'sparky')} "
    f"Partition={os.environ.get('FAKE_SACCT_PARTITION', 'all')}"
)
'''

_SQUEUE = '''\
import os, sys
from pathlib import Path

job = sys.argv[sys.argv.index("-j") + 1]
record = Path(os.environ["FAKE_SLURM_STATE"]) / f"{job}.state"
if not record.exists():
    raise SystemExit(0)
state, _ = record.read_text().strip().split("|")
if state in {"PENDING", "RUNNING"}:
    print(state)
'''

_SCANCEL = '''\
import os, sys
from pathlib import Path

job = sys.argv[-1]
state = Path(os.environ["FAKE_SLURM_STATE"])
(state / "cancelled").open("a").write(job + "\\n")
(state / f"{job}.state").write_text("CANCELLED|0:15\\n")
'''


@pytest.fixture()
def fleet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Put a fake scheduler on PATH and point the lane at a private root."""

    binaries = tmp_path / "bin"
    binaries.mkdir()
    for name, body in (
        ("sbatch", _SBATCH), ("sacct", _SACCT), ("scontrol", _SCONTROL),
        ("squeue", _SQUEUE), ("scancel", _SCANCEL),
    ):
        script = binaries / name
        script.write_text(
            f"#!{sys.executable}\n" + textwrap.dedent(body), encoding="utf-8"
        )
        script.chmod(0o755)
    state = tmp_path / "slurm-state"
    state.mkdir()
    monkeypatch.setenv("PATH", f"{binaries}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_SLURM_STATE", str(state))
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:0")
    monkeypatch.setenv(sl.LANE_ROOT_ENV, str(tmp_path / "lane"))
    # The node-side job-state root, which is a different question from the
    # lane root and is answered on the node.  Set here because the fake
    # ``sbatch`` runs the job script in this process's environment, and the
    # default is the fleet's real shared mount.
    monkeypatch.setenv(sl.JOB_STATE_ROOT_ENV, str(tmp_path / "node-jobs"))
    return state


def _submissions(state: Path) -> list[dict]:
    path = state / "submissions.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]


# --------------------------------------------------------------------------
# Actions
# --------------------------------------------------------------------------

def _sealed_source(tmp_path: Path) -> tuple[Path, str, dict]:
    """A one-file git checkout, its stamp name, and its identity."""

    source = tmp_path / "source"
    source.mkdir()
    subprocess.run(["git", "-C", str(source), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(source), "config", "user.name", "PrismaBuild test"],
        check=True)
    subprocess.run(
        ["git", "-C", str(source), "config", "user.email", "t@example.invalid"],
        check=True)
    (source / "task.py").write_text(
        "import pathlib\n"
        "pathlib.Path('result.txt').write_text("
        "pathlib.Path('payload.txt').read_text())\n"
    )
    (source / "payload.txt").write_text("sealed by slurm\n")
    subprocess.run(
        ["git", "-C", str(source), "add", "task.py", "payload.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(source), "commit", "-qm", "sealed source"], check=True)
    identity = pb.git_checkout_identity(source)
    stamp_name = f"{pb.PBRUN_STAMP_PREFIX}slurm.json"
    (source / stamp_name).write_text(
        json.dumps({"cwd": ".", **identity}, indent=1, sort_keys=True))
    return source, stamp_name, identity


def _snapshot(tmp_path: Path, source: Path, stamp_name: str,
              cas: pb.PrismaBuildCAS) -> dict:
    clone = tmp_path / f"snapshot-{uuid.uuid4().hex[:8]}"
    subprocess.run(["git", "clone", "-q", str(source), str(clone)], check=True)
    (clone / stamp_name).write_bytes((source / stamp_name).read_bytes())
    subprocess.run(["git", "-C", str(clone), "add", "-f", stamp_name], check=True)
    subprocess.run(
        ["git", "-C", str(clone), "-c", "user.name=PrismaBuild test",
         "-c", "user.email=t@example.invalid", "commit", "-qm", "sealed"],
        check=True)
    commit = subprocess.run(
        ["git", "-C", str(clone), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True).stdout.strip()
    bundle = tmp_path / f"{clone.name}.bundle"
    subprocess.run(
        ["git", "-C", str(clone), "bundle", "create", str(bundle), "HEAD"],
        check=True, capture_output=True)
    entry, _ = cas.ingest_input(bundle, input_id="pbrun.checkout-snapshot")
    return {
        "schema": "prismaquant.prismabuild.pbrun_checkout_snapshot.v1",
        "commit": commit,
        "subdirectory": ".",
        "input": entry,
    }


def _runnable_action(tmp_path: Path, cas: pb.PrismaBuildCAS,
                     *, owner: str = "") -> dict:
    """An action a real worker can execute: sealed snapshot, real closure."""

    source, stamp_name, _identity = _sealed_source(tmp_path)
    snapshot = _snapshot(tmp_path, source, stamp_name, cas)
    variables: dict[str, str] = {}
    if owner:
        variables["PRISMABUILD_CONTAINER_OWNER"] = owner
    return pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "fleet/pbrun",
            "definition_version": "v1",
            "task_class": "generation",
            "determinism": "stochastic",
            "artifact_family": "generic",
            "artifact_kind": "generic",
            "argv": [sys.executable, "task.py"],
            "working_directory": ".",
            "result_path": "result.txt",
        },
        "inputs": [snapshot["input"]],
        "code_closure": pb.build_code_closure(source, [stamp_name]),
        "params": {
            "command": [sys.executable, "task.py"],
            "cwd": ".",
            "demand": {},
            "checkout_snapshot": snapshot,
        },
        "environment": {"variables": variables, "toolchain": {}},
        "execution_scope": {
            "portability": "portable", "platform_key": None, "host_class": None,
        },
    })


def _paper_action(tmp_path: Path, key_seed: str = "paper") -> dict:
    """An action key and nothing that has to run: for argv assertions."""

    empty = tmp_path / f"closure-{key_seed}"
    empty.mkdir(exist_ok=True)
    (empty / "seed.txt").write_text(key_seed, encoding="utf-8")
    return pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "fleet/pbrun",
            "definition_version": "v1",
            "task_class": "generation",
            "determinism": "stochastic",
            "artifact_family": "generic",
            "artifact_kind": "generic",
            "argv": ["/bin/true", key_seed],
            "working_directory": ".",
            "result_path": "result.txt",
        },
        "inputs": [],
        "code_closure": pb.build_code_closure(empty, ["seed.txt"]),
        "params": {"command": ["/bin/true"], "cwd": ".", "demand": {}},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {
            "portability": "portable", "platform_key": None, "host_class": None,
        },
    })


def _submit(tmp_path: Path, *, resources: sl.LaneResources,
            placement=(), timeout_s: float = 7200.0, **kwargs) -> sl.SubmittedJob:
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, kwargs.pop("seed", "paper"))
    request = cas.publish_action_request(action)
    return sl.submit(
        action, cas=cas, request_path=request, placement=placement,
        resources=resources, timeout_s=timeout_s, worker_script=WORKER,
        job_entry=JOB_ENTRY, **kwargs,
    )


# --------------------------------------------------------------------------
# What the lane sends
# --------------------------------------------------------------------------

def test_a_gpu_slot_action_asks_for_shards_its_tags_and_its_own_time(
    tmp_path: Path, fleet: Path
) -> None:
    """The whole submit contract, on the shape ``pbrun --gpu`` produces.

    Every flag here answers something the pull queue answered another way, and
    the mapping is the part a first install cannot check for itself:
    ``--gres=shard:N`` is the GPU slot (shards schedule concurrency without
    fencing memory, which is what a GB10's unified pool needs), ``--constraint``
    is the tag conjunction the queue matcher enforced, and ``--time`` is the
    submitter's ``--timeout-s`` -- which the pool path parses and drops.
    """

    resources = sl.LaneResources.from_demand(
        {"gpu": 1, "mem_gb": 72, "cpu": 4})
    job = _submit(tmp_path, resources=resources,
                  placement=["gb10", "sparklina"], timeout_s=7200.0)

    argv = _submissions(fleet)[0]["argv"]
    key = job.action_key
    directory = job.directory
    assert argv == [
        "--parsable",
        "--no-requeue",
        "--export=NIL",
        f"--job-name=pb-{key[:12]}",
        f"--chdir={directory}",
        f"--output={directory}/%j.out",
        f"--error={directory}/%j.err",
        "--mem=73728M",
        "--cpus-per-task=4",
        "--nice=10000",
        "--time=02:00:00",
        "--gres=shard:1",
        "--constraint=gb10&sparklina",
        str(directory / "job.sh"),
    ]


def test_a_cpu_action_asks_for_no_device_at_all(
    tmp_path: Path, fleet: Path
) -> None:
    """No GPU demand, no GRES.  A CPU slot that can touch a device is the bug
    ``pbrun`` masks ``CUDA_VISIBLE_DEVICES`` for; asking SLURM for nothing is
    the same rule enforced a layer lower, by the scheduler rather than by the
    environment."""

    job = _submit(
        tmp_path,
        resources=sl.LaneResources.from_demand({"mem_gb": 4, "cpu": 1}),
        placement=["x86"], timeout_s=3600.0,
    )
    argv = _submissions(fleet)[0]["argv"]
    assert not [flag for flag in argv if flag.startswith("--gres")]
    assert "--constraint=x86" in argv
    assert "--time=01:00:00" in argv
    assert "--mem=4096M" in argv
    assert job.stdout_path.name.endswith(".out")


def test_an_exclusive_action_asks_for_the_whole_device_not_more_shards(
    tmp_path: Path, fleet: Path
) -> None:
    """``gpu:1`` and ``shard:N`` are mutually exclusive requests against one
    GPU, so exclusivity is a different GRES name rather than a bigger count."""

    resources = sl.LaneResources.from_demand(
        {"gpu": 3, "mem_gb": 16, "cpu": 2}, exclusive=True)
    _submit(tmp_path, resources=resources, seed="exclusive")
    assert "--gres=gpu:1" in _submissions(fleet)[0]["argv"]


def test_no_requested_deadline_sends_no_time_limit(
    tmp_path: Path, fleet: Path
) -> None:
    """Elapsed time is not evidence of death.  A submission that asked for no
    deadline carries no --time, so under a partition whose MaxTime is
    UNLIMITED the job runs while it is running; the pull queue never enforced
    the old 7200 s default either (issue #32), and enforcing it here would have
    made the cutover a two-hour kill on every default submission."""

    job = _submit(tmp_path, resources=sl.LaneResources(), timeout_s=None)
    argv = _submissions(fleet)[0]["argv"]
    assert not [flag for flag in argv if flag.startswith("--time")]
    record = json.loads(job.record_path.read_text(encoding="utf-8"))
    assert record["time_limit"] == ""


def test_a_requested_deadline_still_becomes_a_time_limit(
    tmp_path: Path, fleet: Path
) -> None:
    job = _submit(tmp_path, resources=sl.LaneResources(), timeout_s=90.0)
    argv = _submissions(fleet)[0]["argv"]
    assert "--time=00:01:30" in argv
    record = json.loads(job.record_path.read_text(encoding="utf-8"))
    assert record["time_limit"] == "00:01:30"


@pytest.mark.parametrize(
    ("priority", "expected"),
    [(0, "--nice=10000"), (5, "--nice=9995"), (-10, "--nice=10010")],
)
def test_the_submitters_priority_becomes_the_nice_slurm_can_honour(
    tmp_path: Path, fleet: Path, priority: int, expected: str
) -> None:
    """The pool sorted its ready queue on ``--priority``; SLURM subtracts a
    nice from the base priority its own scheduler assigned.  Higher priority is
    therefore a smaller nice, and the base keeps every one of them non-negative
    -- a boost needs SlurmUser privilege the submitting user does not have.

    ``pool_reset`` is the case that made this a defect rather than a gap: it
    re-submits in bulk at ``--priority -10``, which under the pool sat behind
    interactive work and under SLURM ran alongside it.
    """

    job = _submit(tmp_path, resources=sl.LaneResources.from_demand({"cpu": 1}),
                  priority=priority, seed=f"nice{priority}")
    record = [r for r in _submissions(fleet) if r["job_id"] == int(job.job_id)][0]

    assert expected in record["argv"]
    assert len([f for f in record["argv"] if f.startswith("--nice=")]) == 1


def test_a_priority_past_the_base_asks_for_the_most_it_can_be_given(
    tmp_path: Path, fleet: Path
) -> None:
    """``sbatch`` refuses a negative nice from an unprivileged submitter, so a
    priority past the base is clamped to zero rather than turned into a
    submission the scheduler rejects."""

    assert sl.nice_for(sl.NICE_BASE + 1) == 0
    job = _submit(tmp_path, resources=sl.LaneResources.from_demand({"cpu": 1}),
                  priority=sl.NICE_BASE + 1, seed="clamped")
    record = [r for r in _submissions(fleet) if r["job_id"] == int(job.job_id)][0]
    assert "--nice=0" in record["argv"]


def test_the_submission_record_says_what_the_priority_became(
    tmp_path: Path, fleet: Path
) -> None:
    """Beside the partition, because both are how this submission was placed
    and neither is part of the action's identity."""

    job = _submit(tmp_path, resources=sl.LaneResources.from_demand({"cpu": 1}),
                  priority=-10, seed="recorded")
    record = json.loads(job.record_path.read_text(encoding="utf-8"))
    assert record["nice"] == 10010
    assert "partition" in record


def test_the_partition_is_read_off_the_demand_and_the_placement() -> None:
    """The fleet's rule, CPU-only work goes to the CPU box, without naming a
    box: shards exist only in the GPU partition; an untagged action with no
    GPU demand goes to the CPU partition; a tagged one goes to the default
    partition and lets its constraint decide."""

    gpu = sl.LaneResources.from_demand({"gpu": 1, "mem_gb": 16})
    cpu = sl.LaneResources.from_demand({"cpu": 8, "mem_gb": 32})
    assert sl.partition_for(gpu, []) == "gpu"
    assert sl.partition_for(gpu, ["gb10", "sparklina"]) == "gpu"
    assert sl.partition_for(cpu, []) == "cpu"
    assert sl.partition_for(cpu, ["x86"]) is None
    assert sl.partition_for(cpu, [""]) == "cpu"


def test_a_pinned_cpu_action_is_not_forced_into_the_cpu_partition() -> None:
    """The case that bites: a CPU-only action whose argv[0] is a GPU box's
    venv.  Placement pins it to that box by hostname; sending it to the CPU
    partition as well would make it unschedulable, because no node there
    carries the pin's feature."""

    cpu = sl.LaneResources.from_demand({"cpu": 4, "mem_gb": 8})
    assert sl.partition_for(cpu, ["sparky"]) is None


def test_a_named_partition_reaches_sbatch(tmp_path: Path, fleet: Path) -> None:
    resources = sl.LaneResources.from_demand({"cpu": 2, "mem_gb": 4})
    job = _submit(tmp_path, resources=resources,
                  partition=sl.partition_for(resources, []))
    argv = _submissions(fleet)[0]["argv"]
    assert "--partition=cpu" in argv
    assert not [flag for flag in argv if flag.startswith("--constraint")]
    record = json.loads(job.record_path.read_text(encoding="utf-8"))
    assert record["partition"] == "cpu"


def test_an_untagged_action_carries_no_constraint(
    tmp_path: Path, fleet: Path
) -> None:
    """An action that may run anywhere must not be pinned to a Feature nobody
    asked for; ``--constraint=`` with an empty value matches nothing at all."""

    _submit(tmp_path, resources=sl.LaneResources(), placement=[])
    assert not [
        flag for flag in _submissions(fleet)[0]["argv"]
        if flag.startswith("--constraint")
    ]


@pytest.mark.parametrize(
    "seconds,expected",
    [(7200, "02:00:00"), (3600, "01:00:00"), (1, "00:01:00"), (61, "00:01:01"),
     (90000, "1-01:00:00"), (7200.5, "02:00:01")],
)
def test_the_time_limit_is_the_submitters_timeout_rounded_up(
    seconds: float, expected: str
) -> None:
    """Up, never down: the limit is where SLURM sends TERM and then KILL, so
    rounding down would kill an action inside the budget it was given.  The
    floor is a minute because that is SLURM's own enforcement granularity."""

    assert sl.format_time_limit(seconds) == expected


@pytest.mark.parametrize("bad", [0, -1, float("inf"), float("nan")])
def test_a_timeout_that_is_not_a_duration_is_refused(bad: float) -> None:
    with pytest.raises(sl.SlurmLaneError):
        sl.format_time_limit(bad)


def test_the_submission_record_seals_the_job_id_and_the_exact_argv(
    tmp_path: Path, fleet: Path
) -> None:
    """A job id nobody wrote down is a job nobody can withdraw, and an argv
    nobody wrote down is a submission nobody can audit."""

    job = _submit(tmp_path, resources=sl.LaneResources(), placement=["x86"])
    record = json.loads(job.record_path.read_text())
    assert record["schema"] == sl.SUBMISSION_SCHEMA_V1
    assert record["job_id"] == job.job_id
    assert record["argv"] == job.argv
    assert record["attempt"] == 1
    assert record["constraint"] == ["x86"]
    assert record["partition"] == ""
    latest = json.loads((job.directory / "latest.json").read_text())
    assert latest == record


def test_sbatch_refusing_the_request_is_raised_not_swallowed(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unknown Feature is refused at submit time -- that is the capability
    gate this transport keeps -- so it has to reach the submitter as a failure
    and not as an action that silently never runs."""

    monkeypatch.setenv("FAKE_SBATCH_REFUSE", "1")
    with pytest.raises(sl.SlurmLaneError, match="node configuration"):
        _submit(tmp_path, resources=sl.LaneResources(), placement=["nosuchbox"])


# --------------------------------------------------------------------------
# What the lane makes of what comes back
# --------------------------------------------------------------------------

def test_wait_reads_scontrol_when_accounting_storage_is_disabled(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """This fleet starts without slurmdbd, and ``sacct`` then fails for every
    job forever.  The lane must not depend on it -- and must still try it, so
    that deploying slurmdbd later is configuration and not a code change."""

    monkeypatch.setenv("FAKE_SACCT_DISABLED", "1")
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:3")
    job = _submit(tmp_path, resources=sl.LaneResources())
    outcome = sl.wait(job, poll_s=0.0)
    assert (outcome.state, outcome.exit_code) == ("FAILED", 3)


def test_wait_gives_up_on_the_callers_clock_without_cancelling_anything(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--wait-s`` bounds the person waiting, never the work.  A job still
    queued when they stop watching keeps its allocation and its job id."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "PENDING")
    job = _submit(tmp_path, resources=sl.LaneResources())
    outcome = sl.wait(job, poll_s=0.0, wait_s=0.0, sleep=lambda _s: None)
    assert outcome.state == sl.WAIT_TIMEOUT_STATE
    assert not (fleet / "cancelled").exists()


def test_a_purged_job_nobody_can_describe_is_unknown_not_failed(
    tmp_path: Path, fleet: Path
) -> None:
    """Past ``MinJobAge`` with no accounting behind it, every scheduler command
    answers nothing.  That is not a verdict on the work; the CAS holds that."""

    job = _submit(tmp_path, resources=sl.LaneResources())
    for record in fleet.glob("*.state"):
        record.unlink()
    assert sl.wait(job, poll_s=0.0).state == sl.UNKNOWN_STATE


def test_a_failed_retry_safe_action_is_resubmitted_to_its_declared_bound(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retries are new job ids, never ``--requeue``: SLURM uses one flag for
    operator requeue and automatic restart, so a requeued job cannot be told
    apart from a rescheduled one afterwards."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:7")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "retry")
    request = cas.publish_action_request(action)
    result = sl.run(
        action, cas=cas, request_path=request,
        resources=sl.LaneResources(), timeout_s=600.0,
        worker_script=WORKER, job_entry=JOB_ENTRY,
        retry_safe=True, max_attempts=3, poll_s=0.0,
    )
    assert len(result.attempts) == 3
    assert len({job.job_id for job, _ in result.attempts}) == 3
    assert [job.attempt for job, _ in result.attempts] == [1, 2, 3]
    assert result.receipt is None
    assert all(
        "--no-requeue" in submission["argv"] for submission in _submissions(fleet)
    )


def test_one_action_key_can_be_submitted_more_than_once(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Asking for the same work again is the same action key, and the lane has
    to accept it.

    An action key is a content hash and the lane directory is per key, so the
    second run of a key writes into the first one's directory.  Naming the
    sealed submission record by the attempt alone made those two collide:
    ``submissions/001.json`` already existed with different bytes, the
    first-writer publish refused, and ``pbrun`` reported ``slurm refused this
    action`` -- for a refusal that came from this module and not from
    ``sbatch``.  Measured in the container smoke on 2026-09-04: every re-run of
    an action failed at submit, which took the CAS hit -- the point of a
    content-addressed build -- out of reach on this transport entirely.
    """

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:0")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "resubmitted")
    request = cas.publish_action_request(action)

    runs = [
        sl.run(
            action, cas=cas, request_path=request,
            resources=sl.LaneResources(), timeout_s=600.0,
            worker_script=WORKER, job_entry=JOB_ENTRY, poll_s=0.0,
        )
        for _ in range(2)
    ]

    assert [len(run.attempts) for run in runs] == [1, 1]
    first, second = (run.attempts[0][0] for run in runs)
    assert first.job_id != second.job_id
    assert first.directory == second.directory
    # Two sealed records, neither overwritten, both still readable.
    assert first.record_path != second.record_path
    for job in (first, second):
        recorded = json.loads(job.record_path.read_text(encoding="utf-8"))
        assert recorded["job_id"] == job.job_id
    assert len(list((first.directory / "submissions").iterdir())) == 2


def test_an_action_that_is_not_retry_safe_is_submitted_exactly_once(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Numerical determinism is not retry safety.  An arbitrary command may
    write external state and then fail, so re-running it is the producer's
    call to make and nobody else's."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:7")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "once")
    request = cas.publish_action_request(action)
    result = sl.run(
        action, cas=cas, request_path=request,
        resources=sl.LaneResources(), timeout_s=600.0,
        worker_script=WORKER, job_entry=JOB_ENTRY,
        retry_safe=False, max_attempts=5, poll_s=0.0,
    )
    assert len(result.attempts) == 1


def test_a_cancelled_job_is_never_retried_around(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A withdrawal is a person's decision.  Resubmitting it would overrule
    them, retry-safe or not."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "CANCELLED")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "cancelled")
    request = cas.publish_action_request(action)
    result = sl.run(
        action, cas=cas, request_path=request,
        resources=sl.LaneResources(), timeout_s=600.0,
        worker_script=WORKER, job_entry=JOB_ENTRY,
        retry_safe=True, max_attempts=4, poll_s=0.0,
    )
    assert len(result.attempts) == 1
    assert result.attempts[0][1].state == "CANCELLED"


def test_a_timed_out_job_is_retried_exactly_as_the_pull_queue_retries_one(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The queue's ``finish`` counts only ``executed`` and ``cache_hit`` as
    success, so an action it killed at ``timeout_s`` is requeued while attempts
    remain.  ``--retries 3`` has to mean three runs on either transport, or the
    flag means two different things depending on how the action was posted."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "TIMEOUT")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "timed-out")
    request = cas.publish_action_request(action)
    result = sl.run(
        action, cas=cas, request_path=request,
        resources=sl.LaneResources(), timeout_s=600.0,
        worker_script=WORKER, job_entry=JOB_ENTRY,
        retry_safe=True, max_attempts=3, poll_s=0.0,
    )
    assert [job.attempt for job, _ in result.attempts] == [1, 2, 3]
    assert {outcome.state for _, outcome in result.attempts} == {"TIMEOUT"}
    assert result.receipt is None


# --------------------------------------------------------------------------
# Withdrawal
# --------------------------------------------------------------------------

def test_withdraw_cancels_the_job_the_record_names(
    tmp_path: Path, fleet: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    job = _submit(tmp_path, resources=sl.LaneResources(), seed="withdraw")
    assert pbrun.withdraw_slurm_main([job.action_key[:12]], by="test") == 0
    assert (fleet / "cancelled").read_text().split() == [job.job_id]
    assert f"cancelled slurm job {job.job_id}" in capsys.readouterr().err


def test_withdraw_refuses_a_prefix_that_names_two_actions(
    tmp_path: Path, fleet: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The wrong guess here kills somebody else's work, so an ambiguous prefix
    is refused rather than resolved."""

    _submit(tmp_path, resources=sl.LaneResources(), seed="a")
    _submit(tmp_path, resources=sl.LaneResources(), seed="b")
    assert pbrun.withdraw_slurm_main([""], by="test") == 2
    assert pbrun.withdraw_slurm_main(["0123456789ab"], by="test") == 2
    assert not (fleet / "cancelled").exists()
    assert "no slurm submission matches" in capsys.readouterr().err


def test_withdrawing_four_and_missing_one_still_withdraws_three(
    tmp_path: Path, fleet: Path
) -> None:
    """One bad name must not stop the rest: cancelling several suites at once
    is the case this verb exists for."""

    jobs = [
        _submit(tmp_path, resources=sl.LaneResources(), seed=str(index))
        for index in range(3)
    ]
    keys = [job.action_key for job in jobs] + ["deadbeefdead"]
    assert pbrun.withdraw_slurm_main(keys, by="test") == 2
    assert sorted((fleet / "cancelled").read_text().split()) == sorted(
        job.job_id for job in jobs)


# --------------------------------------------------------------------------
# The job itself
# --------------------------------------------------------------------------

def test_the_job_materializes_the_snapshot_and_publishes_a_real_receipt(
    tmp_path: Path
) -> None:
    """The end-to-end claim: a SLURM job's execution is the pull queue's.

    Driven without any scheduler at all -- ``slurm_job.py`` is what the batch
    script execs, so running it directly is running the job.  It has to
    reconstruct the sealed tree on this box, run the canonical ``run-local``
    argv inside it, and leave a receipt the submitter can look up.
    """

    cas_root = tmp_path / "cas"
    cas = pb.PrismaBuildCAS(cas_root)
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    checkouts = tmp_path / "materialized"
    state_root = tmp_path / "jobs"

    # No SLURM_* in the environment, and the job id passed instead.  Under a
    # real scheduler ``core._collect_worker_evidence`` reads the whole trio and
    # then attests it against this process's cgroup membership; claiming half of
    # it here would be claiming a job that does not exist.
    completed = subprocess.run(
        [sys.executable, str(JOB_ENTRY),
         "--action", str(request), "--cas-root", str(cas_root),
         "--worker", str(WORKER), "--worker-python", sys.executable,
         "--job-state-root", str(state_root), "--job-id", "4242",
         "--checkout-root", str(checkouts)],
        capture_output=True, text=True,
    )
    assert completed.returncode == 0, completed.stderr

    receipt = cas.lookup(action)
    assert receipt is not None
    assert cas.result_path(receipt, action).read_text() == "sealed by slurm\n"
    # The per-action tree does not survive the job.  The state file does: the
    # Epilog runs after every ending, and it is the only thing on the node that
    # can remove a container the action started -- a container is reparented to
    # containerd-shim and outlives a job that ended normally just as completely
    # as one that was killed.  So the launcher leaves it the file it needs, and
    # the Epilog deletes it.
    assert not list(checkouts.glob("*/checkout"))
    survivor = state_root / "4242.job"
    assert survivor.exists()
    assert "container_owner=" in survivor.read_text(encoding="utf-8")


def test_the_job_leaves_the_epilog_the_owner_and_the_tree_while_it_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A job killed at its time limit cleans nothing up, so what the Epilog
    needs has to be on disk *before* the work starts: the container-ownership
    label the Docker shim stamps, and the tree to remove."""

    cas_root = tmp_path / "cas"
    cas = pb.PrismaBuildCAS(cas_root)
    owner = "ab" * 32
    action = _runnable_action(tmp_path, cas, owner=owner)
    request = cas.publish_action_request(action)
    state_root = tmp_path / "jobs"
    checkouts = tmp_path / "materialized"

    sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
    import slurm_job  # noqa: PLC0415

    seen: list[str] = []
    real = slurm_job._write_job_state

    def record(path, **kwargs):
        real(path, **kwargs)
        seen.append(path.read_text())

    monkeypatch.setattr(slurm_job, "_write_job_state", record)
    code = slurm_job.main([
        "--action", str(request), "--cas-root", str(cas_root),
        "--worker", str(WORKER), "--worker-python", sys.executable,
        "--job-state-root", str(state_root), "--job-id", "9001",
        "--checkout-root", str(checkouts),
    ])

    assert code == 0
    assert len(seen) == 2
    assert all(f"container_owner={owner}" in text for text in seen)
    assert "checkout_dir=\n" in seen[0]            # nothing to remove yet
    tree = [
        line.split("=", 1)[1]
        for line in seen[1].splitlines() if line.startswith("checkout_dir=")
    ][0]
    assert tree and Path(tree).parent == checkouts


def test_the_submitter_does_not_decide_where_the_epilog_state_file_goes(
    tmp_path: Path, fleet: Path
) -> None:
    """The batch script carries no job-state path, because that is the node's.

    A submitter that exported ``PRISMABUILD_SLURM_LANE_ROOT`` -- which the
    fixture above does -- used to bake its own answer into the script, while
    the Epilog on the node kept reading its own default.  The two disagreed,
    the Epilog found no state file, and a killed job leaked its checkout and
    its containers with nothing said.
    """

    job = _submit(tmp_path, resources=sl.LaneResources.from_demand({"cpu": 1}))
    script = job.script.read_text(encoding="utf-8")

    assert "--job-state-root" not in script
    assert str(tmp_path / "lane" / sl.JOB_STATE_DIRNAME) not in script


def test_the_launcher_reads_the_job_state_root_the_epilog_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same variable, same default, resolved on the node that runs the job."""

    monkeypatch.setenv(sl.LANE_ROOT_ENV, str(tmp_path / "submitter-lane"))
    monkeypatch.setenv(sl.JOB_STATE_ROOT_ENV, str(tmp_path / "node-jobs"))
    assert sl.job_state_directory() == tmp_path / "node-jobs"

    cas_root = tmp_path / "cas"
    cas = pb.PrismaBuildCAS(cas_root)
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)

    sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
    import slurm_job  # noqa: PLC0415

    written: list[Path] = []
    real = slurm_job._write_job_state

    def record(path, **kwargs):
        written.append(Path(path))
        real(path, **kwargs)

    monkeypatch.setattr(slurm_job, "_write_job_state", record)
    code = slurm_job.main([
        "--action", str(request), "--cas-root", str(cas_root),
        "--worker", str(WORKER), "--worker-python", sys.executable,
        "--job-id", "5150",
        "--checkout-root", str(tmp_path / "materialized"),
    ])

    assert code == 0
    assert written and set(written) == {tmp_path / "node-jobs" / "5150.job"}


def test_the_job_state_root_default_is_not_the_submitters_lane_root(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unset override answers the fleet's own path, whatever the lane root
    was set to."""

    monkeypatch.setenv(sl.LANE_ROOT_ENV, "/somewhere/else")
    monkeypatch.delenv(sl.JOB_STATE_ROOT_ENV, raising=False)
    assert str(sl.job_state_directory()) == sl.DEFAULT_JOB_STATE_ROOT


# --------------------------------------------------------------------------
# pbrun's side of it
# --------------------------------------------------------------------------

def test_pbrun_reports_a_slurm_execution_the_way_it_reports_a_pool_one(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The transport is a dispatcher, not a new contract.  Same exit code, same
    lines, and the verdict from the CAS rather than from ``sbatch``."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "run")
    monkeypatch.setenv("PRISMABUILD_LOCAL_CHECKOUT_ROOT",
                       str(tmp_path / "materialized"))
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)

    code = pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=["x86"],
        demand={"cpu": 1, "mem_gb": 4}, exclusive=False,
        timeout_s=600.0, wait_s=60.0, retry_safe=False, max_attempts=1,
        runtime_root=REPOSITORY, poll_s=0.0,
    )
    assert code == 0
    assert cas.lookup(action) is not None
    err = capsys.readouterr().err
    assert "submitted" in err and "attempt 1/1" in err and "executed via" in err


def test_the_slurm_path_prints_the_placement_notices_the_pool_path_prints(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Both notices printed on the pool path only, though the pin is just as
    real under SLURM and the GPU mask is applied before the transport branch.

    A pin is a consequence of a checkout path rather than of a flag, so a
    submitter that is not told has narrowed the fleet to one box without
    knowing; and a slot with no device silently turns a suite's CUDA tests into
    skips, which read as the same green.
    """

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:0")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "notices")
    request = cas.publish_action_request(action)

    pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=["x86"],
        demand={"cpu": 1, "mem_gb": 4}, exclusive=False,
        timeout_s=600.0, wait_s=60.0, retry_safe=False, max_attempts=1,
        runtime_root=REPOSITORY, poll_s=0.0,
        placement_notice="pbrun: PINNED to sparky -- test notice",
    )

    err = capsys.readouterr().err
    assert "pbrun: PINNED to sparky -- test notice" in err
    assert "[no GPU: CUDA_VISIBLE_DEVICES='']" in err


def test_a_gpu_action_is_not_told_its_slot_has_no_device(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:0")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "has-a-device")
    request = cas.publish_action_request(action)

    pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=["gb10"],
        demand={"gpu": 1, "mem_gb": 16}, exclusive=False,
        timeout_s=600.0, wait_s=60.0, retry_safe=False, max_attempts=1,
        runtime_root=REPOSITORY, poll_s=0.0,
    )

    assert "no GPU" not in capsys.readouterr().err


def test_a_job_that_exits_zero_without_a_receipt_is_not_reported_as_success(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The CAS is the truth.  Reporting the scheduler's status instead would
    report success for an action nothing can look up afterwards."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:0")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "ghost")
    request = cas.publish_action_request(action)
    code = pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=[], demand={},
        exclusive=False, timeout_s=600.0, wait_s=60.0, retry_safe=False,
        max_attempts=1, runtime_root=REPOSITORY, poll_s=0.0,
    )
    assert code == 1
    assert "published no receipt" in capsys.readouterr().err


def test_a_job_slurm_killed_at_the_time_limit_is_reported_as_a_timeout(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """SLURM reports a job it killed at the time limit as ``ExitCode=0:15``.
    Reading the exit code alone made that indistinguishable from a job that
    ran to completion and published nothing, so the operator was pointed at a
    log that says nothing instead of at the state that explains it."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "TIMEOUT")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "over-time")
    request = cas.publish_action_request(action)
    code = pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=[], demand={},
        exclusive=False, timeout_s=600.0, wait_s=60.0, retry_safe=False,
        max_attempts=1, runtime_root=REPOSITORY, poll_s=0.0,
    )
    err = capsys.readouterr().err
    assert code == 1
    assert "failed (TIMEOUT)" in err
    assert "published no receipt" not in err


def test_a_cancelled_job_exits_the_way_a_withdrawal_does(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "CANCELLED")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "stopped")
    request = cas.publish_action_request(action)
    code = pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=[], demand={},
        exclusive=False, timeout_s=600.0, wait_s=60.0, retry_safe=False,
        max_attempts=1, runtime_root=REPOSITORY, poll_s=0.0,
    )
    assert code == pbrun.WITHDRAWN_EXIT


def _slurm_main_kwargs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *argv_tail: str
) -> dict[str, object]:
    """Drive ``pbrun.main --transport slurm`` and return what it sent the lane.

    Building a ``PoolQueue`` is an assertion failure rather than a mock:
    everything below the branch reads worker offers, and a retained offer from
    a loop stopped for the cutover would refuse a submission the scheduler can
    place perfectly well.
    """

    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "-c", "user.name=PrismaBuild test",
         "-c", "user.email=t@example.invalid", "commit", "--allow-empty",
         "-qm", "fixture"], check=True)

    def refuse(*_a, **_kw):
        raise AssertionError("the slurm path built a PoolQueue")

    seen: dict[str, object] = {}
    monkeypatch.setattr(pbrun.pool, "PoolQueue", refuse)
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    monkeypatch.setattr(pbrun, "git_repository_root", lambda _cwd: tmp_path)
    payload = tmp_path / "bundle.bin"
    payload.write_bytes(b"not really a bundle\n")
    entry, _ = pb.PrismaBuildCAS(tmp_path / "fleet" / "cas").ingest_input(
        payload, input_id="pbrun.checkout-snapshot")
    monkeypatch.setattr(
        pbrun, "build_git_checkout_snapshot", lambda *_a, **_kw: {"input": entry})
    monkeypatch.setattr(
        pbrun.pb.PrismaBuildCAS, "publish_action_request",
        lambda self, action: Path("/requests/test.json"))

    def capture(action, **kwargs):
        seen.update(kwargs)
        seen["action_key"] = action["action_key"]
        return 0

    monkeypatch.setattr(pbrun, "slurm_outcome", capture)
    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--transport", "slurm", "--cwd", str(tmp_path),
        *argv_tail, "--", "/bin/true",
    ])
    assert pbrun.main() == 0
    return seen


def test_the_slurm_transport_never_touches_the_pull_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The helper refuses a ``PoolQueue``; this is the case it exists for."""

    seen = _slurm_main_kwargs(
        tmp_path, monkeypatch, "--tag", "x86", "--timeout-s", "1800")
    assert seen["tags"] == ["x86"]
    assert seen["timeout_s"] == 1800.0
    assert seen["exclusive"] is False


def test_main_hands_the_slurm_path_the_pin_notice_it_would_have_printed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Built in ``main``, which is the only place that knows the checkout and
    the flags.  The census is unavailable rather than empty: this branch builds
    no ``PoolQueue`` on purpose, and worker offers do not describe a SLURM
    fleet, so the notice says so instead of borrowing the queue's sentence."""

    seen = _slurm_main_kwargs(tmp_path, monkeypatch, "--here")
    notice = str(seen["placement_notice"])
    assert "PINNED" in notice
    assert pbrun.NO_CENSUS in notice
    assert pbrun.UNANNOUNCED_CENSUS not in notice


def test_the_priority_a_caller_asked_for_reaches_the_lane(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--priority`` was parsed and then dropped on the way to SLURM, so
    ``pool_reset``'s bulk ``--priority -10`` ran alongside interactive work
    instead of behind it."""

    seen = _slurm_main_kwargs(tmp_path, monkeypatch, "--priority", "-10")
    assert seen["priority"] == -10


def test_the_transport_default_is_still_the_pull_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SLURM is installed box by box, and the day a controller comes up is not
    the day every agent's ``pbrun`` starts talking to it.  Cutover is one
    environment variable and rollback is unsetting it.

    Read through ``main`` rather than off the parser, so that the assertion is
    about which dispatcher an operator's command line actually reached.  A test
    that recomputed the default expression could only ever agree with itself.
    """

    routed: list[str] = []
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    monkeypatch.setattr(pbrun.pool, "PoolQueue", lambda root: ("queue", root))
    monkeypatch.setattr(
        pbrun, "withdraw_main",
        lambda queue, prefixes, **_kw: routed.append("pool") or 0)
    monkeypatch.setattr(
        pbrun, "withdraw_slurm_main",
        lambda prefixes, **_kw: routed.append("slurm") or 0)
    monkeypatch.setattr(sys, "argv", ["pbrun.py", "--withdraw", "abc123abc123"])

    monkeypatch.delenv(pbrun.DEFAULT_TRANSPORT_ENV, raising=False)
    assert pbrun.main() == 0
    assert routed == ["pool"]

    monkeypatch.setenv(pbrun.DEFAULT_TRANSPORT_ENV, "slurm")
    assert pbrun.main() == 0
    assert routed == ["pool", "slurm"]

    # And the flag still outranks the environment, in both directions.
    monkeypatch.setattr(
        sys, "argv",
        ["pbrun.py", "--transport", "pool", "--withdraw", "abc123abc123"])
    assert pbrun.main() == 0
    assert routed == ["pool", "slurm", "pool"]


def test_a_refusal_from_sbatch_reaches_the_caller_as_a_refusal(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The pool path refuses a tag no box offers, with the tags and the demand
    on screen.  SLURM refuses the same thing at submit time, and the submitter
    has to read the same kind of sentence rather than a traceback."""

    monkeypatch.setenv("FAKE_SBATCH_REFUSE", "1")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "refused")
    request = cas.publish_action_request(action)
    with pytest.raises(SystemExit) as refusal:
        pbrun.slurm_outcome(
            action, cas=cas, request_path=request, tags=["nosuchbox"],
            demand={"cpu": 1, "mem_gb": 4}, exclusive=False, timeout_s=600.0,
            wait_s=60.0, retry_safe=False, max_attempts=1,
            runtime_root=REPOSITORY, poll_s=0.0,
        )
    message = str(refusal.value)
    assert "slurm refused this action" in message
    assert "nosuchbox" in message
    assert "node configuration" in message

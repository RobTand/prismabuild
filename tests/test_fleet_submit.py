"""The one submit path the fleet's producers share, on both transports.

Three tools seal their own actions and enqueued them by calling
``PoolQueue.publish`` directly.  That is a bypass once there are two
dispatchers: after the cutover a direct publish puts 120 export shards in a
queue no worker drains, and it *succeeds*, so nothing anywhere says so.

What is asserted here is that the transport actually decides -- an ``sbatch``
and no pull-queue item under SLURM, a pull-queue item and no ``sbatch`` under
the pool -- and that the one action shape these producers currently build is
refused before anything is submitted rather than after the scheduler has
placed it.
"""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import slurm_lane as sl  # noqa: E402

import dispatch_tessera_ladder as ladder  # noqa: E402
import dispatch_tessera_shards as shards  # noqa: E402
import fleet_submit  # noqa: E402
import seal_and_publish  # noqa: E402

from test_slurm_lane import (  # noqa: E402
    _paper_action,
    _runnable_action,
    _submissions,
    fleet,
)

__all__ = ["fleet"]


def _cas(tmp_path: Path) -> pb.PrismaBuildCAS:
    return pb.PrismaBuildCAS(tmp_path / "cas")


def _ready(queue_root: Path) -> list[str]:
    directory = queue_root / pool.READY
    if not directory.is_dir():
        return []
    return sorted(path.stem for path in directory.glob("*.json"))


def test_the_slurm_transport_sbatches_and_never_touches_the_queue(
    tmp_path: Path, fleet: Path,
) -> None:
    """The producer's demand vocabulary reaches the scheduler as scheduler flags."""

    cas = _cas(tmp_path)
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    queue_root = tmp_path / "pb-queue"

    submission = fleet_submit.submit(
        action, cas=cas, request_path=request, transport="slurm",
        tags=["gb10"], needs_gpu=True, resources={"gpu": 1, "mem_gb": 16},
        queue_root=queue_root, timeout_s=600.0,
    )

    assert submission.transport == "slurm"
    assert submission.job_id
    assert submission.describe() == f"slurm job {submission.job_id}"
    assert Path(submission.where).exists()

    rows = _submissions(fleet)
    assert len(rows) == 1
    argv = rows[0]["argv"]
    assert "--gres=shard:1" in argv
    assert "--constraint=gb10" in argv
    assert "--partition=gpu" in argv
    assert "--mem=16384M" in argv
    assert _ready(queue_root) == []


def test_a_producer_that_asks_for_no_deadline_gets_none(
    tmp_path: Path, fleet: Path,
) -> None:
    cas = _cas(tmp_path)
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    fleet_submit.submit(
        action, cas=cas, request_path=request, transport="slurm",
        resources={"cpu": 1, "mem_gb": 4}, queue_root=tmp_path / "pb-queue",
    )
    argv = _submissions(fleet)[0]["argv"]
    assert not [flag for flag in argv if flag.startswith("--time")]


def test_a_producers_priority_reaches_the_scheduler(
    tmp_path: Path, fleet: Path,
) -> None:
    """The pool branch records ``priority`` and sorts on it; the SLURM branch
    used to accept the same argument and drop it."""

    cas = _cas(tmp_path)
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    fleet_submit.submit(
        action, cas=cas, request_path=request, transport="slurm",
        resources={"cpu": 1, "mem_gb": 4}, priority=-10,
        queue_root=tmp_path / "pb-queue",
    )
    assert f"--nice={sl.NICE_BASE + 10 * sl.NICE_SCALE}" in _submissions(fleet)[0]["argv"]


def test_an_untagged_cpu_action_goes_to_the_cpu_partition(
    tmp_path: Path, fleet: Path,
) -> None:
    """A producer that declares no GPU and names no box is asking for the CPU
    box, and the lane says so to the scheduler rather than leaving it to the
    default partition, where the work could land on a GPU box and take the
    host memory a GPU action is budgeted against."""

    cas = _cas(tmp_path)
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)

    fleet_submit.submit(
        action, cas=cas, request_path=request, transport="slurm",
        resources={"cpu": 8, "mem_gb": 32},
        queue_root=tmp_path / "pb-queue", timeout_s=600.0,
    )
    argv = _submissions(fleet)[0]["argv"]
    assert "--partition=cpu" in argv
    assert not [flag for flag in argv if flag.startswith("--gres")]


def test_a_tagged_cpu_action_lets_its_constraint_decide(
    tmp_path: Path, fleet: Path,
) -> None:
    cas = _cas(tmp_path)
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)

    fleet_submit.submit(
        action, cas=cas, request_path=request, transport="slurm",
        tags=["sparky"], resources={"cpu": 4, "mem_gb": 8},
        queue_root=tmp_path / "pb-queue", timeout_s=600.0,
    )
    argv = _submissions(fleet)[0]["argv"]
    assert "--constraint=sparky" in argv
    assert not [flag for flag in argv if flag.startswith("--partition")]


def test_the_pool_transport_publishes_and_never_sbatches(
    tmp_path: Path, fleet: Path,
) -> None:
    """The pull queue stays live until the cutover, byte for byte as before."""

    cas = _cas(tmp_path)
    action = _paper_action(tmp_path, "pool")
    request = cas.publish_action_request(action)
    queue_root = tmp_path / "pb-queue"
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    submission = fleet_submit.submit(
        action, cas=cas, request_path=request, transport="pool",
        checkout_root=checkout, tags=["gb10"], needs_gpu=True,
        resources={"gpu": 1, "mem_gb": 16}, queue_root=queue_root,
    )

    assert submission.transport == "pool"
    assert submission.describe() == "queued"
    key = str(action["action_key"])
    assert _ready(queue_root) == [key]
    item = json.loads((queue_root / pool.READY / f"{key}.json").read_text())
    assert item["checkout_root"] == str(checkout)
    assert item["resources"] == {"gpu": 1, "mem_gb": 16}
    assert _submissions(fleet) == []


def test_an_action_with_no_sealed_snapshot_is_refused_before_it_is_submitted(
    tmp_path: Path, fleet: Path,
) -> None:
    """An action that names no tree at all, which the lane cannot place.

    A ``checkout_root`` is sealed into a snapshot now, so the refusal narrows
    to the case nothing can answer: no snapshot and no tree to make one from.
    Refusing here costs one message; refusing on the node costs one message
    per placed job, after the scheduler has queued every one of them.
    """

    cas = _cas(tmp_path)
    action = _paper_action(tmp_path, "unsnapshotted")
    request = cas.publish_action_request(action)

    with pytest.raises(fleet_submit.SubmitRefused) as refusal:
        fleet_submit.submit(
            action, cas=cas, request_path=request, transport="slurm",
            tags=["gb10"], queue_root=tmp_path / "pb-queue",
        )
    assert "snapshot-addressed" in str(refusal.value)
    assert _submissions(fleet) == []


def test_a_submission_retires_a_live_withdrawal_on_the_lane_too(
    tmp_path: Path, fleet: Path,
) -> None:
    """``publish`` does this and says why; the lane path has to do it itself.

    A marker left in place makes the re-submitted action unrunnable and the
    only remedy a hand edit of the live queue.  The decision is kept, not
    deleted.
    """

    cas = _cas(tmp_path)
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    key = str(action["action_key"])
    queue_root = tmp_path / "pb-queue"
    marker = queue_root / pool.WITHDRAWN / f"{key}.json"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps({
        "action_key": key, "status": "withdrawn", "published_unix": 5.0,
        "withdrawn_by": "rob@sparky",
    }), encoding="utf-8")

    fleet_submit.submit(
        action, cas=cas, request_path=request, transport="slurm",
        tags=["gb10"], resources={"mem_gb": 4}, queue_root=queue_root,
        timeout_s=600.0,
    )

    assert not marker.exists()
    kept = sorted((queue_root / pool.WITHDRAWN / "superseded").glob("*.json"))
    assert len(kept) == 1
    assert json.loads(kept[0].read_text())["withdrawn_by"] == "rob@sparky"


def test_a_refused_submission_retires_no_withdrawal_on_the_lane_either(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same rule as ``run``: the marker moves when ``sbatch`` has
    accepted, and a refusal has submitted nothing."""

    monkeypatch.setenv("FAKE_SBATCH_REFUSE", "1")
    cas = _cas(tmp_path)
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    key = str(action["action_key"])
    queue_root = tmp_path / "pb-queue"
    marker = queue_root / pool.WITHDRAWN / f"{key}.json"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps({
        "action_key": key, "status": "withdrawn", "published_unix": 5.0,
        "withdrawn_by": "rob@sparky",
    }), encoding="utf-8")

    with pytest.raises(sl.SlurmLaneError, match="node configuration"):
        fleet_submit.submit(
            action, cas=cas, request_path=request, transport="slurm",
            tags=["nosuchbox"], resources={"mem_gb": 4}, queue_root=queue_root,
            timeout_s=600.0,
        )

    assert marker.exists()
    assert not list((queue_root / pool.WITHDRAWN / "superseded").glob("*.json"))


def test_the_job_entry_is_a_sibling_of_this_module(tmp_path: Path) -> None:
    """Both deployed layouts, one path.

    ``publish_runtime`` writes every fleet script twice -- ``tools/<name>`` and
    ``tools/fleet/<name>`` -- so a sibling resolves wherever the caller was
    published, while a path assembled from the runtime root has to guess.
    """

    assert fleet_submit.JOB_ENTRY.is_file()
    assert fleet_submit.JOB_ENTRY.name == "slurm_job.py"
    assert fleet_submit.JOB_ENTRY.parent == Path(
        fleet_submit.__file__).resolve().parent


# --------------------------------------------------------------------------
# The producers themselves: no tool may keep its own publish
# --------------------------------------------------------------------------

def _point_at_this_tree(
    producer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Point one producer's live-store constants at this test's own tree.

    Each dispatcher addresses ``/mnt/shared`` in module constants, so driving
    its ``main`` means repointing every one of them first.  Nothing here needs
    a fleet: ``fleet_submit.submit`` is replaced by the caller, so the run
    stops at exactly the boundary these tests are about.
    """

    monkeypatch.setattr(producer, "SH", tmp_path / "fleet")
    monkeypatch.setattr(producer, "RUNTIME_ROOT", REPOSITORY)
    if producer is seal_and_publish:
        return

    checkout = tmp_path / "checkout"
    (checkout / "tessera" / "src").mkdir(parents=True)
    (checkout / "tessera" / "src" / "encoder.py").write_text(
        "VALUE = 1\n", encoding="utf-8")
    (checkout / producer.WRAPPER).write_text("# wrapper\n", encoding="utf-8")
    monkeypatch.setattr(producer, "CHECKOUT", checkout)
    monkeypatch.setattr(producer, "PYTHON", sys.executable)
    monkeypatch.setattr(producer, "SOURCE", str(tmp_path / "unused-model"))
    if producer is ladder:
        # Staged into the checkout by ``main`` itself, out of a path that
        # exists on sparky and nowhere else.
        wrapper = tmp_path / producer.WRAPPER
        wrapper.write_text("# wrapper\n", encoding="utf-8")
        monkeypatch.setattr(producer, "LOCAL_WRAPPER", wrapper)
    else:
        plan = tmp_path / "plan.json"
        plan.write_text("{}\n", encoding="utf-8")
        monkeypatch.setattr(producer, "PLAN", str(plan))
        monkeypatch.setattr(producer, "PARTS", str(tmp_path / "parts"))


def drive_producer(
    producer, argv: list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> dict[str, object]:
    """Run one producer's ``main`` and record what reached the submit path.

    No producer may keep a publish of its own: a tool that calls
    ``PoolQueue.publish`` bypasses the lane, and after the cutover that queues
    work where no worker drains it and *succeeds*, so nothing says so.  Driven
    rather than read off the source, because a tool can name the shared submit
    in a branch it never takes and publish from the branch it does.
    """

    _point_at_this_tree(producer, tmp_path, monkeypatch)
    seen: dict[str, object] = {}

    def capture(action, **kwargs):
        seen["sealed_key"] = str(action["action_key"])
        seen.update(kwargs)
        return fleet_submit.Submission(
            transport=str(kwargs["transport"]), where=tmp_path / "latest.json",
            job_id="7", action_key="f" * 64,
        )

    def refuse_a_direct_publish(*args, **kwargs):
        raise AssertionError(
            f"{producer.__name__} published straight to the pool queue")

    monkeypatch.setattr(fleet_submit, "submit", capture)
    monkeypatch.setattr(pool.PoolQueue, "publish", refuse_a_direct_publish)

    if producer is seal_and_publish:
        assert producer.main(argv) == 0
    else:
        monkeypatch.setattr(sys, "argv", [f"{producer.__name__}.py", *argv])
        # Both of these fall off the end of ``main``; ``SystemExit(None)`` is 0.
        assert producer.main() in (None, 0)
    assert seen, f"{producer.__name__} never reached fleet_submit.submit"
    return seen


@pytest.mark.parametrize("transport", ["pool", "slurm"])
def test_the_ladder_dispatcher_routes_through_the_shared_submit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, transport: str,
) -> None:
    seen = drive_producer(
        ladder, ["--shards", "1", "--transport", transport],
        tmp_path, monkeypatch)

    assert seen["transport"] == transport
    assert seen["checkout_root"] == str(tmp_path / "checkout")


@pytest.mark.parametrize("transport", ["pool", "slurm"])
def test_the_shard_dispatcher_routes_through_the_shared_submit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, transport: str,
) -> None:
    seen = drive_producer(
        shards, ["--shards", "61", "--transport", transport],
        tmp_path, monkeypatch)

    assert seen["transport"] == transport
    assert seen["checkout_root"] == str(tmp_path / "checkout")


@pytest.mark.parametrize("transport", ["pool", "slurm"])
def test_the_smoke_publisher_routes_through_the_shared_submit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, transport: str,
) -> None:
    seen = drive_producer(
        seal_and_publish, ["--transport", transport], tmp_path, monkeypatch)

    assert seen["transport"] == transport
    assert seen["checkout_root"] == str(tmp_path / "fleet" / "checkout")


# --------------------------------------------------------------------------
# Sealing a producer's checkout for the lane
# --------------------------------------------------------------------------

def _producer_checkout(tmp_path: Path) -> Path:
    """A Git worktree with no pbrun stamp, which is what a producer has."""

    root = tmp_path / "producer-checkout"
    root.mkdir()
    for argv in (
        ["init", "-q"],
        ["config", "user.name", "PrismaBuild test"],
        ["config", "user.email", "t@example.invalid"],
    ):
        subprocess.run(["git", "-C", str(root), *argv], check=True)
    (root / "task.py").write_text(
        "import pathlib\n"
        "pathlib.Path('shard.json').write_text("
        "pathlib.Path('payload.txt').read_text())\n",
        encoding="utf-8",
    )
    (root / "payload.txt").write_text("sealed by a producer\n", encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(root), "add", "task.py", "payload.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(root), "commit", "-qm", "producer source"], check=True)
    return root


def _producer_action(checkout: Path, *, shard: int = 1) -> dict:
    """The shape both Tessera dispatchers build: no snapshot, no stamp."""

    return pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "tessera/glm53-export-shard",
            "definition_version": "v1",
            "task_class": "generation",
            "determinism": "deterministic",
            "artifact_family": "generic",
            "artifact_kind": "tessera-shard",
            "argv": [sys.executable, "task.py"],
            "working_directory": ".",
            "result_path": "shard.json",
        },
        "inputs": [],
        "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {"shard": shard, "of_shards": 120},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {
            "portability": "portable", "platform_key": None, "host_class": None,
        },
    })


def test_a_producers_checkout_root_is_sealed_into_the_action_for_the_lane(
    tmp_path: Path, fleet: Path,
) -> None:
    """The defect: 120 shards, every one refused for the shape they all build.

    Both dispatchers seal actions that carry no ``params.checkout_snapshot``
    and name a tree instead.  Refusing that named no way forward, so the lane
    seals the tree here -- through ``pbrun``'s own sealer -- and the action the
    scheduler receives is the snapshot-addressed action ``slurm_job`` needs.
    """

    cas = _cas(tmp_path)
    checkout = _producer_checkout(tmp_path)
    action = _producer_action(checkout)
    sealed_key = str(action["action_key"])
    request = cas.publish_action_request(action)

    submission = fleet_submit.submit(
        action, cas=cas, request_path=request, transport="slurm",
        checkout_root=checkout, tags=["gb10"],
        resources={"gpu": 1, "mem_gb": 16}, queue_root=tmp_path / "pb-queue",
    )

    assert submission.transport == "slurm"
    # The snapshot is an input and a param, so the key is a different hash.
    # The producer prints the submitted key, not the one it sealed.
    assert submission.action_key != sealed_key
    rows = _submissions(fleet)
    assert len(rows) == 1
    assert f"--job-name=pb-{submission.action_key[:12]}" in rows[0]["argv"]

    request_path = (
        cas.root / "requests" / submission.action_key[:2]
        / f"{submission.action_key}.json"
    )
    submitted = json.loads(request_path.read_text(encoding="utf-8"))
    snapshot = submitted["params"]["checkout_snapshot"]
    assert snapshot["subdirectory"] == "."
    assert snapshot["input"] in submitted["inputs"]
    assert Path(cas.input_path(snapshot["input"])).is_file()


def test_sealing_never_moves_the_key_of_an_action_that_had_a_snapshot(
    tmp_path: Path, fleet: Path,
) -> None:
    """A pbrun-sealed action means the same thing under either transport.

    Its key is a promise about what will run, and re-sealing it around a
    second answer to the same question would break the CAS memoization the
    whole system is built on.
    """

    cas = _cas(tmp_path)
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)

    submission = fleet_submit.submit(
        action, cas=cas, request_path=request, transport="slurm",
        resources={"mem_gb": 4}, queue_root=tmp_path / "pb-queue",
    )
    assert submission.action_key == str(action["action_key"])


def test_one_tree_is_bundled_once_however_many_shards_it_dispatches(
    tmp_path: Path, fleet: Path,
) -> None:
    """120 shards out of one checkout is one bundle, not 120 identical ones."""

    cas = _cas(tmp_path)
    checkout = _producer_checkout(tmp_path)
    fleet_submit._SNAPSHOT_CACHE.clear()

    inputs = set()
    for shard in (1, 2, 3):
        action = _producer_action(checkout, shard=shard)
        request = cas.publish_action_request(action)
        snapshot = fleet_submit.seal_checkout_snapshot(checkout, cas=cas)
        inputs.add(snapshot["input"]["sha256"])
        fleet_submit.submit(
            action, cas=cas, request_path=request, transport="slurm",
            checkout_root=checkout, resources={"mem_gb": 4},
            queue_root=tmp_path / "pb-queue",
        )
    assert len(inputs) == 1
    assert len(_submissions(fleet)) == 3
    assert len({row["argv"][3] for row in _submissions(fleet)}) == 3


def test_a_checkout_that_is_not_a_git_worktree_is_refused_by_name(
    tmp_path: Path, fleet: Path,
) -> None:
    """The refusal a producer can act on, instead of a process that exits.

    ``pbrun``'s sealer refuses by ``SystemExit``, which is right for a
    terminal and wrong inside a dispatcher's loop: it would stop the run
    without naming which action it was on.
    """

    cas = _cas(tmp_path)
    plain = tmp_path / "not-a-repo"
    plain.mkdir()
    (plain / "task.py").write_text("pass\n", encoding="utf-8")
    action = _producer_action(plain)
    request = cas.publish_action_request(action)

    with pytest.raises(fleet_submit.SubmitRefused) as refusal:
        fleet_submit.submit(
            action, cas=cas, request_path=request, transport="slurm",
            checkout_root=plain, resources={"mem_gb": 4},
            queue_root=tmp_path / "pb-queue",
        )
    assert "non-Git checkout" in str(refusal.value)
    assert _submissions(fleet) == []


def test_a_sealed_producer_action_runs_on_the_node_that_took_it(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The claim the seal makes: the node materializes the tree and executes.

    The fake ``sbatch`` runs the script it was handed, so this is the whole
    path -- bundle out of the CAS, private checkout, canonical ``run-local``
    worker argv, receipt -- rather than an assertion about argv.
    """

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "run")
    monkeypatch.setenv(
        "PRISMABUILD_LOCAL_CHECKOUT_ROOT", str(tmp_path / "local"))
    cas = _cas(tmp_path)
    checkout = _producer_checkout(tmp_path)
    action = _producer_action(checkout)
    request = cas.publish_action_request(action)

    submission = fleet_submit.submit(
        action, cas=cas, request_path=request, transport="slurm",
        resources={"mem_gb": 4}, checkout_root=checkout,
        queue_root=tmp_path / "pb-queue",
        worker_script=REPOSITORY / "tools" / "prismabuild_worker.py",
        job_entry=REPOSITORY / "tools" / "fleet" / "slurm_job.py",
    )

    submitted = json.loads(
        (cas.root / "requests" / submission.action_key[:2]
         / f"{submission.action_key}.json").read_text(encoding="utf-8"))
    receipt = cas.lookup(submitted)
    assert receipt is not None, Path(
        json.loads(Path(submission.where).read_text())["stderr"]
    ).read_text(encoding="utf-8")


def test_the_smoke_publisher_names_its_checkout_on_the_lane_too(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture,
) -> None:
    """The dispatchers' defect, in the tool that smoke-tests the dispatcher.

    All three producers passed ``checkout_root`` only under the pull queue, so
    the one command an operator runs to prove the lane works was refused by
    it.  Driven through ``main`` rather than read off the source, because the
    property is what the tool hands the submit path.
    """

    import seal_and_publish

    monkeypatch.setattr(seal_and_publish, "SH", tmp_path)
    seen: dict[str, object] = {}

    def capture(action, **kwargs):
        seen.update(kwargs)
        seen["sealed_key"] = str(action["action_key"])
        return fleet_submit.Submission(
            transport="slurm", where=tmp_path / "latest.json", job_id="7",
            action_key="f" * 64,
        )

    monkeypatch.setattr(seal_and_publish.fleet_submit, "submit", capture)
    assert seal_and_publish.main(["--transport", "slurm"]) == 0

    assert seen["checkout_root"] == str(tmp_path / "checkout")
    printed = json.loads(capsys.readouterr().out)
    assert printed["action_key"] == "f" * 64
    assert printed["sealed_action_key"] == seen["sealed_key"]


def test_a_producers_submission_is_findable_by_the_key_it_went_under(
    tmp_path: Path, fleet: Path,
) -> None:
    """What makes a producer's job accountable after the producer has exited.

    ``submit`` returns as soon as the scheduler has the job, so nothing here
    ever sees the ending and no terminal record is filed: ``tessera_status``
    reads ``done/`` and ``failed/`` and would see nothing at all.  ``pbwait``
    closes that, and the only thing it has to work from is the lane's
    submission record -- so the record has to be there, under the key the
    action was actually submitted with, and resolvable from the twelve
    characters an operator's log line prints.
    """

    cas = _cas(tmp_path)
    checkout = _producer_checkout(tmp_path)
    action = _producer_action(checkout)
    request = cas.publish_action_request(action)

    submission = fleet_submit.submit(
        action, cas=cas, request_path=request, transport="slurm",
        tags=["gb10"], resources={"gpu": 1, "mem_gb": 16},
        checkout_root=checkout, queue_root=tmp_path / "pb-queue",
    )

    found = sl.resolve_recorded(submission.action_key[:12])
    assert len(found) == 1
    record = found[0]
    assert record["action_key"] == submission.action_key
    assert record["job_id"] == submission.job_id
    assert record["request"] == str(
        cas.root / "requests" / submission.action_key[:2]
        / f"{submission.action_key}.json")
    # The generation and the retry policy, which is what lets `pbwait` build a
    # complete terminal record from this file alone.
    assert record["published_unix"] > 0
    assert record["max_attempts"] == 1
    assert record["resources"] == {"cpu": 1, "gpu": 1, "mem_gb": 16}

    # And nothing files an ending: that is `pbwait`'s, and both directories
    # are still absent.
    assert not (tmp_path / "pb-queue" / "done").exists()
    assert not (tmp_path / "pb-queue" / "failed").exists()

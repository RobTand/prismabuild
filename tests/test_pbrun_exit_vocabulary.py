"""A run's own exit status never impersonates one of ``pbrun``'s conditions.

``pbrun`` reserves five non-zero exit codes for things it decides itself: 2 for
a refusal, 12 for a GPU run that exited zero on zero devices, 74 for a record
it could not write, 75 for no verdict yet, and 143 for a withdrawal. Both
report paths used to return the number out of the terminal record verbatim, so
a run whose launcher exited on one of those numbers reached the caller as
``pbrun``'s own word for something else.

143 is not hypothetical.  ``core._sigterm_unwinds_this_process`` raises
``SystemExit(128 + signum)``, so any SIGTERM that is not a withdrawal leaves
the launcher exiting 143: an operator's ``kill``, a worker loop restarting
under it, a supervisor tidying up.  ``PoolQueue.execute`` files that under
``failed/`` with ``detail.returncode`` 143, and the caller read it as "an
operator withdrew this on purpose" with no marker anywhere to back that up.

75 is the mirror image.  A launcher exiting 75 read as "no verdict yet, run
``pbwait`` again", which sends the operator to wait on work that already
finished and lost.

12 is the GPU closure.  A GPU-demanded argv that exits zero with zero devices
visible in the sealed launch environment did not do GPU work, so the closure
refuses the close instead of certifying it.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import types

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402

import pbrun  # noqa: E402

from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402
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
RESERVED = (2, 12, 74, 75, 143)


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
    """2, 12, 74, 75 and 143 are the codes ``pbrun`` decides for itself."""

    assert sorted(pbrun.RESERVED_EXITS) == sorted(RESERVED)
    assert pbrun.GAVE_UP_EXIT in pbrun.RESERVED_EXITS
    assert pbrun.WITHDRAWN_EXIT in pbrun.RESERVED_EXITS
    assert pbrun.RECORD_WRITE_FAILED_EXIT in pbrun.RESERVED_EXITS
    assert pbrun.ZERO_DEVICES_EXIT in pbrun.RESERVED_EXITS

_ABSENT = object()


def _done_record(
    root: Path, *, status: str = "executed", returncode: int | None = 0,
    devices: object = _ABSENT, needs_gpu: bool = True,
    resources: dict | None = None, action_returncode: int | None = None,
) -> pool.PoolQueue:
    """A ``done/`` ending with a device count and a GPU demand, or neither."""

    queue = pool.PoolQueue(root)
    directory = root / pool.DONE
    directory.mkdir(parents=True, exist_ok=True)
    detail: dict[str, object] = {
        "stdout": "",
        "stderr": "",
        "elapsed_s": 1.0,
    }
    if returncode is not None:
        detail["returncode"] = returncode
    if action_returncode is not None:
        detail["action_returncode"] = action_returncode
    if devices is not _ABSENT:
        detail["devices"] = devices
    record: dict[str, object] = {
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "action_key": KEY,
        "status": status,
        "attempts": 1,
        "published_unix": 100.0,
        "finished_host": "sparky",
        "needs_gpu": needs_gpu,
        "tags": ["gb10"] if needs_gpu else ["x86"],
        "resources": ({"cpu": 1, "mem_gb": 1, "gpu": 1} if needs_gpu
                      else {"cpu": 1, "mem_gb": 1}) if resources is None else resources,
        "detail": detail,
    }
    (directory / f"{KEY}.json").write_text(json.dumps(record), encoding="utf-8")
    return queue


def _fake_probe_run(monkeypatch: pytest.MonkeyPatch, *, count: str | None = None,
                    returncode: int = 0, stderr: str = "",
                    error: BaseException | None = None) -> list[dict]:
    """A CUDA driver child that answers ``count``, fails, or never answers."""

    seen: list[dict] = []

    def fake_run(argv, *, env, stdin, stdout, stderr, text, timeout):
        seen.append(dict(env))
        if error is not None:
            raise error
        return types.SimpleNamespace(returncode=returncode,
                                     stdout=f"{count}\n" if count is not None else "",
                                     stderr=stderr)

    monkeypatch.setattr(pb.subprocess, "run", fake_run)
    return seen


@pytest.mark.parametrize("count", [0, 1, 3])
def test_probe_visible_devices_counts_zero_and_one_or_more(
    monkeypatch: pytest.MonkeyPatch, count: int,
) -> None:
    """Fake driver children that see zero, one, and three devices."""

    seen = _fake_probe_run(monkeypatch, count=str(count))

    devices, probe_error = pb.probe_visible_devices({"CUDA_VISIBLE_DEVICES": ""})

    assert (devices, probe_error) == (count, None)
    assert seen[0]["CUDA_VISIBLE_DEVICES"] == ""


@pytest.mark.parametrize("failure", ["missing-library", "failed-call", "timeout"])
def test_probe_gap_is_not_a_zero(
    monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    """A probe that cannot answer reads as a gap, never as zero devices."""

    if failure == "missing-library":
        _fake_probe_run(monkeypatch, count=None, returncode=1,
                        stderr="OSError: libcuda.so.1: cannot open shared object file")
    elif failure == "failed-call":
        _fake_probe_run(monkeypatch, count=None, returncode=10)
    else:
        _fake_probe_run(monkeypatch,
                        error=pb.subprocess.TimeoutExpired("probe", 10.0))

    devices, probe_error = pb.probe_visible_devices({"CUDA_VISIBLE_DEVICES": ""})

    assert devices is None
    assert isinstance(probe_error, str) and probe_error


@pytest.mark.parametrize("count", [0, 1])
def test_devices_lift_reads_zero_and_one_or_more_from_launcher_stdout(
    count: int,
) -> None:
    """The pool lift carries a zero and a one-or-more count off the last line."""

    stdout = "application output only\n" + json.dumps(
        {"status": "published", "devices": count}) + "\n"

    assert pool.devices_from_launcher_stdout(stdout) == {"devices": count}


def test_devices_lift_reads_no_count_as_a_gap() -> None:
    """No count on the last line lifts nothing, not a zero."""

    assert pool.devices_from_launcher_stdout("application output only\n") == {}
    assert pool.devices_from_launcher_stdout(
        json.dumps({"status": "cache_hit"})) == {}
    assert pool.devices_from_launcher_stdout(
        json.dumps({"status": "cache_hit", "devices": None,
                    "devices_probe_error": "device probe timed out"})) == {
        "devices": None,
        "devices_probe_error": "device probe timed out",
    }


def test_sidecar_devices_reach_the_outcome(tmp_path: Path) -> None:
    """The status sidecar carries the count when the last line is lost."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    (tmp_path / "pb-queue" / pool.CLAIMED).mkdir(parents=True, exist_ok=True)
    path = queue.action_status_path(KEY)
    path.write_text(json.dumps({"devices": 0}), encoding="utf-8")

    assert pool.PoolQueue._merge_action_status({}, path) == {"devices": 0}
    assert not path.exists()


def test_stdout_devices_win_over_the_sidecar(tmp_path: Path) -> None:
    """The last line is newer evidence than the pre-launch sidecar."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    (tmp_path / "pb-queue" / pool.CLAIMED).mkdir(parents=True, exist_ok=True)
    path = queue.action_status_path(KEY)
    path.write_text(json.dumps({"devices": 0}), encoding="utf-8")

    assert pool.PoolQueue._merge_action_status({"devices": 1}, path) == {
        "devices": 1}


def test_pool_lift_places_devices_on_the_adopted_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A finished GPU run files its count where the closure reads it."""

    base = pool.PoolQueue(tmp_path / "queue")
    queue = AdmittedQueueFixture(base, capacity={"cpu": 8, "mem_gb": 16},
                                 default_demand={"cpu": 1, "mem_gb": 1})
    queue.ensure_layout()
    queue.publish(action_key=KEY, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", tags=[], needs_gpu=True,
                  resources={"cpu": 1, "mem_gb": 1}, max_attempts=1)
    claimed = queue.claim(has_gpu=True)
    assert claimed is not None
    terminal_path = queue.finish(
        KEY, status="executed",
        detail={"returncode": 0, "stdout": "", "stderr": "",
                "elapsed_s": 1.0, "devices": 0},
        claim_snapshot=claimed)
    terminal = json.loads(terminal_path.read_text(encoding="utf-8"))

    assert queue.adopted_attempt_summary(terminal)["detail"]["devices"] == 0

    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    assert pbrun.await_outcome(base, KEY, wait_s=1.0) == pbrun.ZERO_DEVICES_EXIT
    assert "zero devices" in capsys.readouterr().err


def test_zero_device_exit_zero_gpu_run_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """The closure refuses the record and names the reason."""

    queue = _done_record(tmp_path / "pb-queue", devices=0)

    code = pbrun.await_outcome(queue, KEY, wait_s=0.0, generation=100.0)

    assert code == pbrun.ZERO_DEVICES_EXIT
    assert code not in (0, 1)
    err = capsys.readouterr().err
    assert "zero devices" in err
    assert KEY[:12] in err


def test_zero_device_cache_hit_resubmit_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """A resubmit lands as ``cache_hit`` and still does not close 0."""

    queue = _done_record(tmp_path / "pb-queue", status="cache_hit",
                         returncode=None, devices=0)

    assert pbrun.await_outcome(
        queue, KEY, wait_s=0.0, generation=100.0) == pbrun.ZERO_DEVICES_EXIT
    assert "zero devices" in capsys.readouterr().err


def test_explicit_gpu_demand_refuses_without_a_record_flag(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """The submit path passes its sealed demand, which outranks the record."""

    queue = _done_record(tmp_path / "pb-queue", devices=0, needs_gpu=False,
                         resources={"cpu": 1, "mem_gb": 1})

    assert pbrun.await_outcome(queue, KEY, wait_s=0.0, generation=100.0,
                               needs_gpu=True) == pbrun.ZERO_DEVICES_EXIT
    assert "zero devices" in capsys.readouterr().err


def test_one_device_gpu_run_closes_as_before(tmp_path: Path) -> None:
    """A run that saw a device keeps exit 0."""

    queue = _done_record(tmp_path / "pb-queue", devices=1)

    assert pbrun.await_outcome(queue, KEY, wait_s=0.0, generation=100.0) == 0


def test_pre_field_record_without_devices_closes_as_before(tmp_path: Path) -> None:
    """Absence of the field is a recorded gap, not a refusal."""

    queue = _done_record(tmp_path / "pb-queue")

    assert pbrun.await_outcome(queue, KEY, wait_s=0.0, generation=100.0) == 0


def test_zero_devices_without_gpu_demand_closes_as_before(tmp_path: Path) -> None:
    """A CPU run that saw zero devices is exactly where it belongs."""

    queue = _done_record(tmp_path / "pb-queue", devices=0, needs_gpu=False,
                         resources={"cpu": 1, "mem_gb": 1})

    assert pbrun.await_outcome(queue, KEY, wait_s=0.0, generation=100.0) == 0


def test_nonzero_exit_with_zero_devices_is_a_failure_not_a_refusal(
    tmp_path: Path,
) -> None:
    """The refusal gates exit-zero only; a failed run keeps its own status."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    directory = tmp_path / "pb-queue" / pool.FAILED
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{KEY}.json").write_text(json.dumps({
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "action_key": KEY,
        "status": "failed",
        "attempts": 1,
        "published_unix": 100.0,
        "finished_host": "sparky",
        "needs_gpu": True,
        "resources": {"cpu": 1, "mem_gb": 1, "gpu": 1},
        "detail": {"returncode": 3, "stdout": "", "stderr": "",
                   "elapsed_s": 1.0, "devices": 0},
    }), encoding="utf-8")

    assert pbrun.await_outcome(queue, KEY, wait_s=0.0, generation=100.0) == 3


def test_device_free_module_routes_to_cpu_and_reuses_the_open_run(
    tmp_path: Path,
) -> None:
    """Explicit ``--tag x86`` skips the ``gb10`` queue and joins the open run."""

    tags, _, _, _ = pbrun.placement_contract(
        tmp_path, explicit=["x86"], here=False, hostname="dl380g10")
    assert tags == ["x86"]
    assert "gb10" not in tags

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    publication = {
        "action_key": KEY,
        "cas_root": str(tmp_path / "cas"),
        "worker_script": str(tmp_path / "worker.py"),
        "tags": tags,
        "needs_gpu": False,
        "resources": {"cpu": 1, "mem_gb": 1},
        "max_attempts": 1,
        "retry_safe": True,
    }
    pbrun.publish_or_refuse(queue, publication)
    ready = queue.ready_items()
    assert len(ready) == 1
    assert "gb10" not in list(ready[0].get("tags") or [])
    assert ready[0].get("needs_gpu") is False
    assert not [item for item in ready if "gb10" in list(item.get("tags") or [])]

    claimed = queue.claim(tags=["x86"], capacity={"cpu": 8, "mem_gb": 16},
                          owner="dl380g10:1:q1")
    assert claimed is not None and claimed["action_key"] == KEY

    queued_path, generation = pbrun.publish_or_attach(
        queue, dict(publication), key=KEY)
    assert queued_path is None
    assert isinstance(generation, float)

"""The cutover fences the pull queue instead of trusting one scan of it.

``cutover.sh`` read ``ready`` and ``claimed`` once, then spent the rest of the
run editing crontabs and stopping supervisors and loops one box at a time, and
published the SLURM generation only afterwards.  A producer that submitted
anywhere in that interval had its action accepted by a transport that was
being retired underneath it: the item sat in ``ready`` with no worker left to
claim it, reported as pending, forever.

The fence is a filesystem fact rather than a flag.  During those steps every
producer and loop on the fleet is still running the currently published
generation, which predates the fence and reads no marker, so a marker checked
only by new ``publish`` code would protect nothing.  What old and new bytes
both obey is the write bit: ``chmod a-w`` on ``pb-queue/ready`` makes the
rename that publishes an item fail with EACCES in the producer that is still
holding the action.  The marker written beside it is the explanation, and it
records the mode to go back to so a rollback restores 2775 rather than a
guessed 775.

Two scans follow the fence: one immediately, which catches a submission that
landed between the first scan and the fence, and one once every loop is
stopped, which catches a claim a dying worker did not unwind.

Nothing here touches the live queue.  Every path is under ``tmp_path``, every
remote snippet reaches the fake ``ssh`` from ``test_slurm_install_scripts``,
and the ``chmod`` tests need no privilege because the directory is the test's
own.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prismabuild import pool  # noqa: E402

import pbrun  # noqa: E402

from test_slurm_install_scripts import (  # noqa: E402
    FAKE_BOX,
    FLEET,
    SUPERVISE_LINE,
    _cutover,
    _live_cutover,
)

#: `chmod` on a directory means nothing to root, so the refusal these tests
#: are about cannot be produced while running as one.
requires_an_unprivileged_user = pytest.mark.skipif(
    os.geteuid() == 0,
    reason="root writes into a directory with no write bit, so there is no "
           "EACCES to refuse on",
)

KEY = "d" * 64


def _mode(path: Path) -> str:
    return oct(stat.S_IMODE(path.stat().st_mode))[2:]


def _fence_environment(tmp_path: Path, *, publish_exit: int = 0) -> dict[str, str]:
    """A live cutover whose queue looks like the fleet's: ready at 2775."""

    environment = _live_cutover(
        tmp_path, crontab=SUPERVISE_LINE + "\n", publish_exit=publish_exit)
    # Step 4 of a rollback sends a supervisor's output to an absolute path,
    # and the one in the script is the fleet's real log.
    environment["PB_SUPERVISOR_LOG"] = str(tmp_path / "pb-supervisor.log")
    fakes = tmp_path / "fakes"
    fakes.mkdir(exist_ok=True)
    # ``_live_cutover``'s ``crontab`` knows ``-l`` and ``-``, which is all the
    # cutover uses.  A rollback installs a file by name, and step 2 now reads
    # the crontab back, so the fake has to actually install it.
    live = tmp_path / "live-crontab"
    live.write_text("", encoding="utf-8")
    (fakes / "crontab").write_text(
        "#!/bin/sh\n"
        f"echo \"crontab $*\" >> '{tmp_path}/calls'\n"
        "case \"$1\" in\n"
        f"    -l) cat '{live}' ;;\n"
        f"    -) cat > '{live}' ;;\n"
        f"    *) cp \"$1\" '{live}' ;;\n"
        "esac\n"
        "exit 0\n",
        encoding="utf-8",
    )
    # Step 4 detaches a long-lived supervisor.  These tests are about the
    # fence, and starting a real process to prove something about a directory
    # mode is not a trade worth making.
    (fakes / "setsid").write_text(
        "#!/bin/sh\n"
        f"echo \"setsid $*\" >> '{tmp_path}/calls'\n"
        "exit 0\n",
        encoding="utf-8",
    )
    for name in ("crontab", "setsid"):
        (fakes / name).chmod(0o755)
    ready = Path(environment["PB_QUEUE_ROOT"]) / "ready"
    ready.chmod(0o2775)
    return environment


def _ready(environment: dict[str, str]) -> Path:
    return Path(environment["PB_QUEUE_ROOT"]) / "ready"


def _fence_marker(environment: dict[str, str]) -> Path:
    return Path(environment["PB_QUEUE_ROOT"]) / pool.PoolQueue.FENCE_NAME


# -- the fence goes up, and stays up ----------------------------------------


@requires_an_unprivileged_user
def test_a_finished_cutover_leaves_the_queue_write_protected(
    tmp_path: Path,
) -> None:
    """The fence outlives the run on purpose.

    After step 5 the pull queue has no execution plane, and every box is still
    running the old generation until it next reloads.  Reopening the queue at
    the end of the cutover would give those producers a directory nothing
    reads.
    """

    environment = _fence_environment(tmp_path)
    ready = _ready(environment)
    assert _mode(ready) == "2775"

    result = _cutover(environment, "--yes")

    assert result.returncode == 0, result.stderr
    assert "cutover complete" in result.stdout
    assert not os.access(ready, os.W_OK)
    record = json.loads(_fence_marker(environment).read_text(encoding="utf-8"))
    assert record["prior_ready_mode"] == "2775"
    assert record["fenced_by"]
    assert "cutover.sh" in record["reason"]


def test_the_cutover_state_file_records_the_queue_and_the_prior_mode(
    tmp_path: Path,
) -> None:
    """Rollback reads the marker first and this second, because a marker
    somebody removed by hand must not leave the mode unknowable."""

    environment = _fence_environment(tmp_path)

    result = _cutover(environment, "--yes")

    assert result.returncode == 0, result.stderr
    state_files = sorted(Path(environment["PB_STATE_DIR"]).glob("cutover-*.json"))
    assert len(state_files) == 1, result.stdout
    state = json.loads(state_files[0].read_text(encoding="utf-8"))
    assert state["queue_root"] == environment["PB_QUEUE_ROOT"]
    assert state["ready_prior_mode"] == "2775"


def test_a_dry_run_fences_nothing(tmp_path: Path) -> None:
    """A dry run changes nothing, and the fence is a change."""

    environment = _fence_environment(tmp_path)

    result = _cutover(environment, "--dry-run", "--yes")

    assert result.returncode == 0, result.stderr
    assert "chmod a-w" in result.stdout
    assert _mode(_ready(environment)) == "2775"
    assert not _fence_marker(environment).exists()


# -- work that arrives in the window ----------------------------------------


def _inject_on_the_first_chmod(tmp_path: Path, key: str) -> Path:
    """A ``chmod`` that files one ready item before it raises the fence.

    The fence's ``chmod a-w`` is the only ``chmod`` this script runs, and the
    fakes go on the cutover's own PATH ahead of ``/usr/bin``.  So this models
    exactly the race under test: a producer's rename lands in ``ready`` after
    the scan read it and before the write bit goes away.  Injecting through
    the fake publisher instead would land after the fence, where the write
    fails and there is nothing to catch.
    """

    fakes = tmp_path / "fakes"
    fakes.mkdir(exist_ok=True)
    script = fakes / "chmod"
    script.write_text(
        "#!/bin/sh\n"
        f"echo \"chmod $*\" >> '{tmp_path}/calls'\n"
        f"if [ ! -e '{tmp_path}/injected' ]; then\n"
        f"    touch '{tmp_path}/injected'\n"
        f"    printf '{{}}' > \"$2/{key}.json\" 2>/dev/null || true\n"
        "fi\n"
        "exec /usr/bin/chmod \"$@\"\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    return script


@requires_an_unprivileged_user
def test_work_that_lands_between_the_scan_and_the_fence_aborts_the_cutover(
    tmp_path: Path,
) -> None:
    """The scan is only true for the instant it ran, and every question after
    it takes time."""

    environment = _fence_environment(tmp_path)
    _inject_on_the_first_chmod(tmp_path, KEY)

    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stdout
    assert "work arrived in the pull queue between the scan and the fence" \
        in result.stderr
    assert f"{KEY}.json" in result.stderr
    # Nothing was stopped, and the item is claimable again.
    assert "step 1" not in result.stdout
    assert (_ready(environment) / f"{KEY}.json").exists()


@requires_an_unprivileged_user
def test_that_abort_lifts_the_fence_and_removes_the_marker(
    tmp_path: Path,
) -> None:
    """A refusal before the loops are stopped leaves a working pull queue.

    The fence exists so that nothing is stranded; a fence left up over a fleet
    whose loops are all still running strands everything instead.
    """

    environment = _fence_environment(tmp_path)
    _inject_on_the_first_chmod(tmp_path, KEY)

    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stdout
    assert _mode(_ready(environment)) == "2775"
    assert not _fence_marker(environment).exists()
    assert "lifted the fence" in result.stdout


@requires_an_unprivileged_user
def test_a_later_refusal_also_lifts_the_fence(tmp_path: Path) -> None:
    """Every question after the fence is asked with the fence up.

    ``sinfo`` is the last of them, and a fleet that is down is refused with
    nothing changed -- which has to include the queue's mode.
    """

    environment = _fence_environment(tmp_path)
    from test_slurm_install_scripts import write_fake_sinfo

    write_fake_sinfo(tmp_path, f"{FAKE_BOX} down")

    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stdout
    assert "does not report every box as usable" in result.stderr
    assert _mode(_ready(environment)) == "2775"
    assert not _fence_marker(environment).exists()


@requires_an_unprivileged_user
def test_the_second_run_of_a_cutover_keeps_the_recorded_prior_mode(
    tmp_path: Path,
) -> None:
    """Re-fencing must not record the fenced mode as the mode to go back to.

    The failure this prevents is silent: a rollback would restore 2555 and
    the pull queue would stay closed with every loop running.
    """

    environment = _fence_environment(tmp_path, publish_exit=1)
    first = _cutover(environment, "--yes")
    assert first.returncode == 1, first.stdout
    # Publication failed after the loops were stopped, so the fence stays up.
    assert not os.access(_ready(environment), os.W_OK)
    marker = _fence_marker(environment)
    assert json.loads(marker.read_text(encoding="utf-8"))["prior_ready_mode"] \
        == "2775"

    second = _cutover(environment, "--yes")

    assert "is already fenced" in second.stdout
    assert json.loads(marker.read_text(encoding="utf-8"))["prior_ready_mode"] \
        == "2775"


@requires_an_unprivileged_user
def test_a_failure_after_the_loops_stop_leaves_the_fence_up(
    tmp_path: Path,
) -> None:
    """Lifting it here would reopen a queue with nothing left to drain it,
    which is the stranding the fence exists to prevent.  The refusal points
    at rollback.sh, which restores the loops and the mode together."""

    environment = _fence_environment(tmp_path, publish_exit=1)

    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stdout
    assert "publication failed" in result.stderr
    assert not os.access(_ready(environment), os.W_OK)
    assert _fence_marker(environment).exists()
    assert "rollback.sh" in result.stderr


# -- rollback lifts it ------------------------------------------------------


@requires_an_unprivileged_user
def test_rollback_restores_the_recorded_mode_and_clears_the_marker(
    tmp_path: Path,
) -> None:
    """The exact prior mode, from the record, because 2775 and 775 differ by
    the setgid bit that keeps new items in the queue's group."""

    environment = _fence_environment(tmp_path)
    cutover = _cutover(environment, "--yes")
    assert cutover.returncode == 0, cutover.stderr
    ready = _ready(environment)
    assert not os.access(ready, os.W_OK)

    rollback = subprocess.run(
        ["bash", str(FLEET / "rollback.sh")],
        capture_output=True, text=True, check=False, env=environment,
    )

    assert rollback.returncode == 0, rollback.stderr
    assert _mode(ready) == "2775"
    assert not _fence_marker(environment).exists()
    assert "step 1b" in rollback.stdout
    assert "rollback complete" in rollback.stdout


@requires_an_unprivileged_user
def test_rollback_lifts_the_fence_before_it_restores_the_workers(
    tmp_path: Path,
) -> None:
    """A producer reading the restored generation is told to use the pull
    queue, so the queue has to accept submissions by then."""

    environment = _fence_environment(tmp_path)
    assert _cutover(environment, "--yes").returncode == 0

    rollback = subprocess.run(
        ["bash", str(FLEET / "rollback.sh")],
        capture_output=True, text=True, check=False, env=environment,
    )

    assert rollback.returncode == 0, rollback.stderr
    assert rollback.stdout.index("step 1b") < rollback.stdout.index(
        "step 2: restore each box's crontab")


def test_rollback_of_a_state_file_that_predates_the_fence_says_so(
    tmp_path: Path,
) -> None:
    """A cutover run before this existed named no queue root, and there is
    then nothing to lift rather than something unknown."""

    environment = _fence_environment(tmp_path)
    state = Path(environment["PB_STATE_DIR"]) / "cutover-1788600000.json"
    state.write_text(
        '{\n'
        ' "schema": "prismaquant.prismabuild.slurm_cutover.v1",\n'
        f' "boxes": "{FAKE_BOX}",\n'
        ' "sparks": "",\n'
        ' "previous_generation": "gen-old",\n'
        f' "crontab_backup": "{tmp_path / "backup"}"\n'
        '}\n',
        encoding="utf-8",
    )

    result = subprocess.run(
        ["bash", str(FLEET / "rollback.sh"), "--dry-run", "--state", str(state)],
        capture_output=True, text=True, check=False, env=environment,
    )

    assert result.returncode == 0, result.stderr
    assert "names no queue root" in result.stdout


# -- what a producer is told -------------------------------------------------


def _publication(tmp_path: Path) -> dict[str, object]:
    return {
        "action_key": KEY,
        "cas_root": str(tmp_path / "cas"),
        "worker_script": str(tmp_path / "worker.py"),
        "checkout_root": str(tmp_path / "checkout"),
    }


@requires_an_unprivileged_user
def test_publish_into_a_fenced_queue_raises_the_named_refusal(
    tmp_path: Path,
) -> None:
    """Not a ``PermissionError`` from inside a rename.

    The producer is the one who can resubmit through SLURM or wait, so the
    refusal has to say which operation refused and why.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    (queue.root / pool.PoolQueue.FENCE_NAME).write_text(
        json.dumps({
            "schema": "prismaquant.prismabuild.slurm_cutover_fence.v1",
            "fenced_unix": "1788600000",
            "fenced_by": "sparky",
            "prior_ready_mode": "2775",
            "reason": "fleet/slurm/cutover.sh is retiring the pull queue's "
                      "execution plane",
        }), encoding="utf-8")
    (queue.root / pool.READY).chmod(0o2555)

    with pytest.raises(pool.PoolContractError) as refusal:
        queue.publish(**_publication(tmp_path))   # type: ignore[arg-type]

    assert "the pull queue is fenced" in str(refusal.value)
    assert "cutover.sh" in str(refusal.value)
    assert "sparky" in str(refusal.value)
    assert not (queue.root / pool.READY / f"{KEY}.json").exists()


@requires_an_unprivileged_user
def test_an_unwritable_ready_with_no_marker_still_refuses_by_name(
    tmp_path: Path,
) -> None:
    """The write bit is the mechanism and the marker is the explanation, so a
    missing explanation is not permission to proceed."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    (queue.root / pool.READY).chmod(0o2555)

    with pytest.raises(pool.PoolContractError) as refusal:
        queue.publish(**_publication(tmp_path))   # type: ignore[arg-type]

    assert "is not writable" in str(refusal.value)
    assert str(queue.root / pool.READY) in str(refusal.value)


def test_an_open_queue_still_publishes(tmp_path: Path) -> None:
    """The check runs before ``ensure_layout``, so a queue whose ``ready``
    does not exist yet must not read as fenced."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")

    path = queue.publish(**_publication(tmp_path))   # type: ignore[arg-type]

    assert path.exists()
    assert path.parent.name == pool.READY


@requires_an_unprivileged_user
def test_pbrun_surfaces_the_refusal_as_a_line_not_a_traceback(
    tmp_path: Path,
) -> None:
    """The submitter reads this, so it is a message and an exit status."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    (queue.root / pool.PoolQueue.FENCE_NAME).write_text(
        json.dumps({"reason": "fleet/slurm/cutover.sh is retiring the pull "
                              "queue's execution plane",
                    "fenced_by": "sparky", "fenced_unix": "1788600000"}),
        encoding="utf-8")
    (queue.root / pool.READY).chmod(0o2555)

    with pytest.raises(SystemExit) as exit_status:
        pbrun.publish_or_refuse(queue, _publication(tmp_path))

    message = str(exit_status.value)
    assert message.startswith("pbrun: ")
    assert "the pull queue is fenced" in message

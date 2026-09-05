"""A Docker census that could not be taken is not a census that found nothing.

The Epilog asks the daemon twice: once for this job's containers, so it can
remove them, and once on the action's owner label alone, so it can decide
whether the ownership marker may be retired.  Both asked with the status
dropped, and `docker ps` prints nothing on a failure exactly as it prints
nothing on an empty answer.  So a job that ended while dockerd or its socket
was down looked identical to a job whose containers were already gone:
`owner_settled` stayed 1, the ownership marker was deleted, and the state file
went with it.

A container started by an action is reparented to containerd-shim and runs
under dockerd's cgroup, so it survives every kill below the job.  Deleting
both records leaves it running with nothing recording which action or checkout
it belonged to, and it may be holding a GPU.

The Epilog still exits 0 on this path.  A non-zero Epilog drains the node, and
a cleanup that could not reach the daemon is not a reason to take a box out of
the fleet.  What changes is that the failure is in the log and the two records
survive for reconciliation; `verify.sh` row 8 reads leftovers in `jobs/` back
out, which is where that starts.

The fake daemon here is never contacted: it is a script that prints the real
error text and exits 1.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_slurm_epilog import (  # noqa: E402
    OWNER,
    _marker,
    _run,
    _state,
    node,
)

__all__ = ["node"]


def _daemon_is_down(node: dict[str, Path], *, exit_code: int = 1) -> None:
    """Replace the fixture's recording ``docker`` with one that cannot answer.

    The text is what a real client prints when it cannot reach the socket, and
    it goes to stderr with an empty stdout, which is the shape that used to be
    read as a settled cleanup.
    """

    docker = node["bin"] / "docker"
    docker.write_text(
        "#!/bin/sh\n"
        f'printf "%s\\n" "$*" >> {node["calls"]}\n'
        'echo "Cannot connect to the Docker daemon at '
        'unix:///var/run/docker.sock." >&2\n'
        f"exit {exit_code}\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)


def test_a_failed_census_keeps_the_ownership_marker(node: dict[str, Path]) -> None:
    """The marker is the action's, and removing it while a container still
    carries the label would tell the next reader the action never used
    Docker."""

    _daemon_is_down(node)
    marker = _marker(node, OWNER)
    _state(node, job_id="1400", owner=OWNER, checkout_dir="",
           local_root=str(node["checkouts"]), marker=str(marker),
           container_job="1400")

    result = _run(node, "1400")

    assert result.returncode == 0, result.stderr
    assert marker.exists()


def test_a_failed_census_keeps_the_job_state_record(node: dict[str, Path]) -> None:
    """The state file names the owner label, the job, the marker and the
    checkout, which is everything a later sweep needs.  Deleting it leaves a
    container whose label matches nothing anybody still has written down."""

    _daemon_is_down(node)
    marker = _marker(node, OWNER)
    state = _state(node, job_id="1401", owner=OWNER, checkout_dir="",
                   local_root=str(node["checkouts"]), marker=str(marker),
                   container_job="1401")

    result = _run(node, "1401")

    assert result.returncode == 0, result.stderr
    assert state.exists()
    assert "keeping" in result.stderr
    assert str(state) in result.stderr


def test_a_failed_census_says_so_in_the_epilog_log(node: dict[str, Path]) -> None:
    """An operator reading slurmd.log has nothing else to find this by, and
    the pre-fix run logged nothing about Docker at all."""

    _daemon_is_down(node, exit_code=1)
    marker = _marker(node, OWNER)
    _state(node, job_id="1402", owner=OWNER, checkout_dir="",
           local_root=str(node["checkouts"]), marker=str(marker),
           container_job="1402")

    result = _run(node, "1402")

    assert result.returncode == 0, result.stderr
    assert f"docker ps for {OWNER[:12]} exited 1" in result.stderr
    assert "could not be asked" in result.stderr
    # The daemon's own text is deliberately not captured into the variable the
    # removal reads, so it must not appear as a container id anywhere.
    assert "docker rm" not in node["calls"].read_text(encoding="utf-8")


def test_a_successful_empty_census_still_retires_both_records(
    node: dict[str, Path]
) -> None:
    """The distinction is the whole fix: an action that started no containers
    is as clean as one whose containers were removed, and the ordinary ending
    must stay ordinary."""

    marker = _marker(node, OWNER)
    state = _state(node, job_id="1403", owner=OWNER, checkout_dir="",
                   local_root=str(node["checkouts"]), marker=str(marker),
                   container_job="1403")

    result = _run(node, "1403")

    assert result.returncode == 0, result.stderr
    assert not marker.exists()
    assert not state.exists()
    assert "keeping" not in result.stderr


def test_a_failed_census_never_drains_the_node(node: dict[str, Path]) -> None:
    """A non-zero Epilog takes the box out of the fleet, and a daemon that was
    briefly unreachable is not a reason to lose a node."""

    _daemon_is_down(node, exit_code=125)
    _state(node, job_id="1404", owner=OWNER, checkout_dir="",
           local_root=str(node["checkouts"]), container_job="1404")

    result = _run(node, "1404")

    assert result.returncode == 0, result.stderr


def test_a_failed_sweep_with_no_state_file_says_the_answer_is_unknown(
    node: dict[str, Path]
) -> None:
    """The same shape on the path that has no state file to retain.

    Cases 2 and 3 in the script's own list -- a launcher killed before it
    wrote the file, and a job-state root this node cannot read -- sweep on the
    job label alone.  There is nothing to keep there, so this is a log line
    and not a decision, but it is still the difference between "this job left
    nothing" and "nobody could ask".
    """

    _daemon_is_down(node)
    node["jobs"].mkdir(parents=True, exist_ok=True)

    result = _run(node, "1405")

    assert result.returncode == 0, result.stderr
    assert "docker ps for prismabuild.job=1405 exited 1" in result.stderr
    assert "unknown" in result.stderr


def test_the_checkout_is_still_removed_when_the_census_fails(
    node: dict[str, Path]
) -> None:
    """Docker being unreachable says nothing about a materialized tree, and a
    tree the job could not remove is this script's to remove."""

    _daemon_is_down(node)
    tree = node["checkouts"] / "pb-1406"
    tree.mkdir(parents=True)
    (tree / "file").write_text("x", encoding="utf-8")
    _state(node, job_id="1406", owner=OWNER, checkout_dir=str(tree),
           local_root=str(node["checkouts"]), container_job="1406")

    result = _run(node, "1406")

    assert result.returncode == 0, result.stderr
    assert not tree.exists()

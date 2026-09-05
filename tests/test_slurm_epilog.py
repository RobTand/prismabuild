"""The Epilog, driven at the shell with a fake ``docker`` on PATH.

A job that ends on its own cleans up after itself.  The one this exists for did
not end on its own: SLURM sent TERM and then KILL at its time limit, and two
things survived that.  A container started by the action was reparented to
containerd-shim and runs under dockerd's cgroup, so no signal to the job's
process group ever reached it and no cgroup limit ever charged it.  A
materialized checkout was left where the job could no longer remove it.

So this is the backstop, and it is tested the way it will run: as a shell
script, as root would run it, against a ``docker`` that records its argv.  The
assertions are about what it matches on and what it refuses to touch -- a
cleanup that can be talked into an unbounded ``rm -rf`` is worse than a leaked
directory, and a container matched by anything other than the action's own
ownership label is somebody else's container.
"""
from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest

EPILOG = Path(__file__).resolve().parents[1] / "fleet" / "slurm" / "epilog.sh"
OWNER = "ab" * 32


@pytest.fixture()
def node(tmp_path: Path) -> dict[str, Path]:
    """A fake compute node: a docker that records, and a job-state root."""

    binaries = tmp_path / "bin"
    binaries.mkdir()
    calls = tmp_path / "docker-calls"
    listed = tmp_path / "docker-ps-output"
    listed.write_text("")
    docker = binaries / "docker"
    # `rm` empties the listing, so a second `ps` answers what a real daemon
    # would after a successful removal.  The Epilog asks twice on purpose: the
    # ownership marker may be retired only when the label has nothing left
    # behind it.  PB_FAKE_DOCKER_STUBBORN=1 is the daemon that did not remove.
    docker.write_text(
        "#!/bin/bash\n"
        f'printf "%s\\n" "$*" >> {calls}\n'
        'if [ "$1" = "ps" ]; then\n'
        f'    cat {listed}\n'
        "fi\n"
        'if [ "$1" = "rm" ] && [ "${PB_FAKE_DOCKER_STUBBORN:-0}" != "1" ]; then\n'
        f'    : > {listed}\n'
        "fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    # A `runuser` that records who it was asked to become and then does the
    # work.  The Epilog's deletes under the lane root go through it because
    # that mount is NFS with root_squash on this fleet, and root's unlink there
    # fails silently; what has to be asserted is the user it drops to.
    runuser_calls = tmp_path / "runuser-calls"
    runuser = binaries / "runuser"
    runuser.write_text(
        "#!/bin/bash\n"
        f'printf "%s\\n" "$*" >> {runuser_calls}\n'
        'shift 2\n'                       # -u <user>
        '[ "${1:-}" = "--" ] && shift\n'
        'exec "$@"\n',
        encoding="utf-8",
    )
    runuser.chmod(0o755)
    return {
        "bin": binaries, "calls": calls, "listed": listed,
        "runuser": runuser_calls,
        "jobs": tmp_path / "lane" / "jobs",
        "checkouts": tmp_path / "checkouts",
    }


def _state(node: dict[str, Path], *, job_id: str, owner: str,
           checkout_dir: str, local_root: str, marker: str = "",
           container_job: str = "") -> Path:
    jobs = node["jobs"]
    jobs.mkdir(parents=True, exist_ok=True)
    path = jobs / f"{job_id}.job"
    path.write_text(
        f"action_key={'cd' * 32}\n"
        f"container_owner={owner}\n"
        f"container_marker={marker}\n"
        f"container_job={container_job}\n"
        f"checkout_dir={checkout_dir}\n"
        f"local_checkout_root={local_root}\n"
        "host=sparky\n"
        "pid=1\n",
        encoding="utf-8",
    )
    return path


def _run(
    node: dict[str, Path], job_id: str, *, job_user: str | None = "rob",
    stubborn_docker: bool = False,
) -> subprocess.CompletedProcess[str]:
    environment = {
        "PB_FAKE_DOCKER_STUBBORN": "1" if stubborn_docker else "0",
        **os.environ,
        "PATH": f"{node['bin']}{os.pathsep}{os.environ['PATH']}",
        "SLURM_JOB_ID": job_id,
        "PRISMABUILD_SLURM_JOB_STATE_ROOT": str(node["jobs"]),
    }
    # SLURM sets this in the Epilog's environment.  ``None`` is a controller
    # that did not, which must not take the script down under ``set -u``.
    environment.pop("SLURM_JOB_USER", None)
    if job_user is not None:
        environment["SLURM_JOB_USER"] = job_user
    return subprocess.run(
        ["/bin/bash", str(EPILOG)],
        capture_output=True, text=True, env=environment,
    )


def test_it_removes_the_containers_carrying_this_actions_label(
    node: dict[str, Path]
) -> None:
    """By label, and by nothing else.  The label is the action's complete
    identity, and it is what distinguishes this job's container from an
    identical one somebody is using right now."""

    node["listed"].write_text("c0ffee01\nc0ffee02\n")
    _state(node, job_id="1234", owner=OWNER, checkout_dir="",
           local_root=str(node["checkouts"]))
    result = _run(node, "1234")

    assert result.returncode == 0
    calls = node["calls"].read_text().splitlines()
    assert calls[0] == f"ps -aq --filter label=prismabuild.action={OWNER}"
    assert calls[1] == "rm -f c0ffee01 c0ffee02"


def test_it_removes_the_checkout_a_killed_job_could_not(
    node: dict[str, Path]
) -> None:
    tree = node["checkouts"] / "abcdef012345.tmpdir"
    (tree / "checkout").mkdir(parents=True)
    (tree / "checkout" / "payload.txt").write_text("left behind\n")
    state = _state(node, job_id="1235", owner="", checkout_dir=str(tree),
                   local_root=str(node["checkouts"]))

    assert _run(node, "1235").returncode == 0
    assert not tree.exists()
    assert node["checkouts"].is_dir()      # the root itself is never the target
    assert not state.exists()              # and the job is done being cleaned


def test_it_refuses_a_recorded_path_outside_the_checkout_root(
    node: dict[str, Path], tmp_path: Path
) -> None:
    """The bound is the point.  A cleanup that can be pointed anywhere by a
    file on a shared mount is a remote delete with extra steps."""

    elsewhere = tmp_path / "not-a-checkout"
    elsewhere.mkdir()
    (elsewhere / "keep.txt").write_text("mine\n")
    _state(node, job_id="1236", owner="", checkout_dir=str(elsewhere),
           local_root=str(node["checkouts"]))

    result = _run(node, "1236")
    assert result.returncode == 0
    assert (elsewhere / "keep.txt").exists()
    assert "is not below" in result.stderr


def test_it_refuses_to_match_containers_on_a_malformed_owner(
    node: dict[str, Path]
) -> None:
    """A truncated or empty label would match a filter far wider than this
    action, so it is refused rather than narrowed."""

    node["listed"].write_text("c0ffee01\n")
    _state(node, job_id="1237", owner="ab", checkout_dir="",
           local_root=str(node["checkouts"]))

    result = _run(node, "1237")
    assert result.returncode == 0
    assert not node["calls"].exists()
    assert "64-hex digest" in result.stderr or "64 characters" in result.stderr


def test_a_job_that_cleaned_up_after_itself_leaves_nothing_to_do(
    node: dict[str, Path]
) -> None:
    """The normal ending: the state file is gone because the job removed it,
    and the Epilog must not go looking for work that is not there."""

    (node["jobs"]).mkdir(parents=True)
    result = _run(node, "9999")
    assert result.returncode == 0
    assert not node["calls"].exists()


def test_it_never_drains_the_node(node: dict[str, Path]) -> None:
    """A non-zero Epilog puts the node in DRAIN.  A cleanup that could not find
    a container must not take a box out of the fleet, so every path exits 0 --
    including the one where the state file names nothing usable at all."""

    jobs = node["jobs"]
    jobs.mkdir(parents=True)
    (jobs / "1238.job").write_text("garbage\n", encoding="utf-8")
    assert _run(node, "1238").returncode == 0


def test_it_deletes_its_state_file_as_the_jobs_user_not_as_root(
    node: dict[str, Path]
) -> None:
    """Because the lane root is NFS and this fleet exports it with root_squash.

    Measured on dl380g10, 2026-09-04::

        /storage_pool/shared 192.168.1.180(rw,async,no_subtree_check) \\
                             192.168.1.110(rw,async,no_subtree_check)

    No ``no_root_squash``, so the compute node's ``root`` is ``nobody`` on
    ``/mnt/shared`` and its unlink of a file the job wrote as ``rob`` fails.
    The Epilog swallows every failure by design, so the symptom is not an error
    -- it is ``jobs/`` filling up for months while every run looks clean.
    """

    state = _state(node, job_id="1240", owner="", checkout_dir="",
                   local_root=str(node["checkouts"]))

    result = _run(node, "1240", job_user="rob")

    assert result.returncode == 0
    assert not state.exists()
    calls = node["runuser"].read_text().splitlines()
    assert calls == [f"-u rob -- rm -f -- {state}"], calls


def test_the_checkout_is_still_removed_as_root(node: dict[str, Path]) -> None:
    """Only the shared mount is squashed.  The materialized checkout is on the
    node's own disk, where root is root and the job's user may no longer be
    able to reach a tree a container wrote into."""

    tree = node["checkouts"] / "abcdef012345.tmpdir"
    (tree / "checkout").mkdir(parents=True)
    _state(node, job_id="1241", owner="", checkout_dir=str(tree),
           local_root=str(node["checkouts"]))

    assert _run(node, "1241", job_user="rob").returncode == 0
    assert not tree.exists()
    calls = node["runuser"].read_text().splitlines()
    assert not any("rm -rf" in call for call in calls), calls


def test_a_controller_that_names_no_job_user_still_cleans_up(
    node: dict[str, Path]
) -> None:
    """``set -u`` and an absent ``SLURM_JOB_USER`` must not meet.  A lane root
    that is not on NFS -- a single-box deployment, the container smoke -- has
    nothing to drop privileges for, and the cleanup still has to happen."""

    state = _state(node, job_id="1242", owner="", checkout_dir="",
                   local_root=str(node["checkouts"]))

    result = _run(node, "1242", job_user=None)

    assert result.returncode == 0
    assert not state.exists()
    assert not node["runuser"].exists()


def test_it_cleans_up_after_a_job_that_ended_normally(
    node: dict[str, Path]
) -> None:
    """The ending the Epilog now owns as well as the killed one.

    A job that ran to completion removed its own checkout on the way out but
    could not remove the container the action started: that is reparented to
    containerd-shim and outlives the job either way.  So the launcher leaves
    the state file behind, this runs, and the tree that is already gone must
    not be reported as a failed removal.
    """

    node["listed"].write_text("c0ffee03\n")
    tree = node["checkouts"] / "already-removed.tmpdir"
    node["checkouts"].mkdir(parents=True, exist_ok=True)
    state = _state(node, job_id="1240", owner=OWNER, checkout_dir=str(tree),
                   local_root=str(node["checkouts"]))

    result = _run(node, "1240")

    assert result.returncode == 0
    calls = node["calls"].read_text().splitlines()
    assert calls[0] == f"ps -aq --filter label=prismabuild.action={OWNER}"
    assert calls[1] == "rm -f c0ffee03"
    assert "could not remove" not in result.stderr
    assert not state.exists()


def test_the_epilog_and_the_lane_name_the_same_job_state_root() -> None:
    """The one thing the shell script and the Python launcher must agree on.

    The Epilog cannot import ``slurm_lane``, so the variable name and the
    default are spelled once on each side.  If they ever drift, a killed job
    leaks its checkout and its containers and nothing says so; this is the only
    thing that would notice.
    """

    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    from prismabuild import slurm_lane  # noqa: PLC0415

    text = EPILOG.read_text(encoding="utf-8")
    expected = (
        f'JOB_STATE_ROOT="${{{slurm_lane.JOB_STATE_ROOT_ENV}:'
        f'-{slurm_lane.DEFAULT_JOB_STATE_ROOT}}}"'
    )
    assert expected in text


def _marker(node: dict[str, Path], owner: str) -> Path:
    """The file the Docker shim writes on first container creation."""

    owners = node["checkouts"].parent / "pb-queue" / "container-owners"
    owners.mkdir(parents=True, exist_ok=True)
    path = owners / f"{owner}.used"
    path.write_text(owner + "\n", encoding="utf-8")
    return path


def test_it_retires_the_container_ownership_marker(
    node: dict[str, Path]
) -> None:
    """The shim writes the marker and the pull queue's `finish` unlinks it once
    the containers are gone.  Under SLURM nothing did, so `container-owners/`
    grew one file per containerized action and never shrank."""

    node["listed"].write_text("c0ffee04\n")
    marker = _marker(node, OWNER)
    state = _state(node, job_id="1250", owner=OWNER, checkout_dir="",
                   local_root=str(node["checkouts"]), marker=str(marker))

    result = _run(node, "1250")

    assert result.returncode == 0
    assert not marker.exists()
    assert not state.exists()
    # As the job's user, for the same reason the state file is: root is
    # squashed to `nobody` on the shared mount and its unlink fails silently.
    calls = node["runuser"].read_text().splitlines()
    assert f"-u rob -- rm -f -- {marker}" in calls


def test_it_keeps_the_marker_while_a_container_still_carries_the_label(
    node: dict[str, Path]
) -> None:
    """A marker removed while a container is still labelled would tell the next
    reader the action never used Docker.  The daemon is asked again after the
    removal rather than trusting its exit status."""

    node["listed"].write_text("c0ffee05\n")   # and this docker does not remove
    marker = _marker(node, OWNER)
    _state(node, job_id="1251", owner=OWNER, checkout_dir="",
           local_root=str(node["checkouts"]), marker=str(marker))

    result = _run(node, "1251", stubborn_docker=True)

    assert result.returncode == 0
    assert marker.exists()
    assert "remain" in result.stderr


def test_it_refuses_a_marker_that_does_not_name_this_action(
    node: dict[str, Path]
) -> None:
    """Bounded like the checkout removal: an absolute path whose last component
    is exactly this action's own `<owner>.used`.  A cleanup that can be talked
    into deleting an arbitrary path is worse than a leaked file."""

    node["listed"].write_text("")
    stranger = node["checkouts"].parent / "somebody-elses.file"
    stranger.parent.mkdir(parents=True, exist_ok=True)
    stranger.write_text("not a marker\n", encoding="utf-8")
    _state(node, job_id="1252", owner=OWNER, checkout_dir="",
           local_root=str(node["checkouts"]), marker=str(stranger))

    result = _run(node, "1252")

    assert result.returncode == 0
    assert stranger.exists()
    assert "left alone" in result.stderr


def test_a_relative_marker_path_is_left_alone(node: dict[str, Path]) -> None:
    node["listed"].write_text("")
    _state(node, job_id="1253", owner=OWNER, checkout_dir="",
           local_root=str(node["checkouts"]), marker="../../etc/passwd")

    result = _run(node, "1253")

    assert result.returncode == 0
    assert "not an absolute path" in result.stderr


def test_it_matches_containers_by_this_jobs_label_as_well_as_the_actions(
    node: dict[str, Path]
) -> None:
    """The owner label is the ACTION's identity, and under the pull queue that
    was also one execution.  SLURM has no claim, so two jobs of one action can
    run on one node; removing on the owner label alone took a sibling job's
    containers with it."""

    node["listed"].write_text("c0ffee06\n")
    _state(node, job_id="1260", owner=OWNER, checkout_dir="",
           local_root=str(node["checkouts"]), container_job="1260")

    assert _run(node, "1260").returncode == 0
    calls = node["calls"].read_text().splitlines()
    assert calls[0] == (
        f"ps -aq --filter label=prismabuild.action={OWNER} "
        f"--filter label=prismabuild.job=1260")
    assert calls[1] == "rm -f c0ffee06"
    # And the marker question is asked on the owner label alone, because the
    # marker is the action's: a sibling job's container has to keep it alive.
    assert calls[2] == f"ps -aq --filter label=prismabuild.action={OWNER}"


def test_it_refuses_a_container_job_that_is_not_this_job(
    node: dict[str, Path]
) -> None:
    """``container_job`` is written by ``slurm_job`` from its own
    ``SLURM_JOB_ID`` and goes into a docker filter unquoted, from a file in a
    directory every job's user can write.  Pre-fix any value was passed to
    the daemon as extra arguments; a state file that names a job other than
    this one is refused, and the marker is left alone with it."""

    node["listed"].write_text("c0ffee08\n")
    _state(node, job_id="1262", owner=OWNER, checkout_dir="",
           local_root=str(node["checkouts"]),
           container_job="1262 --filter label=x=y", marker="m")

    result = _run(node, "1262")

    assert result.returncode == 0
    assert not node["calls"].exists()
    assert "refusing to match on it" in result.stderr
    assert "but this is job 1262" in result.stderr


def test_a_state_file_with_no_job_id_still_matches_on_the_owner(
    node: dict[str, Path]
) -> None:
    """A job already running when the runtime generation rolled wrote no job
    id.  Matching on the owner alone is what this did for all of them, so it
    stays the fallback -- and says so, because in that window a sibling job's
    container can still be caught."""

    node["listed"].write_text("c0ffee07\n")
    _state(node, job_id="1261", owner=OWNER, checkout_dir="",
           local_root=str(node["checkouts"]))

    result = _run(node, "1261")

    assert result.returncode == 0
    calls = node["calls"].read_text().splitlines()
    assert calls[0] == f"ps -aq --filter label=prismabuild.action={OWNER}"
    assert "matching on the owner label alone" in result.stderr


def test_the_shim_and_the_epilog_read_the_same_cgroup_job(tmp_path: Path) -> None:
    """The shim derives the job id the Epilog matches on, and it derives it the
    way ``core._slurm_job_from_cgroup`` does -- from the kernel-owned cgroup,
    never from an environment anything in the job could have exported."""

    import importlib.util
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    from prismabuild import core  # noqa: PLC0415

    shim_path = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "docker"
    spec = importlib.util.spec_from_loader(
        "pb_docker_shim",
        importlib.machinery.SourceFileLoader("pb_docker_shim", str(shim_path)),
    )
    shim = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(shim)

    assert shim.CGROUP_JOB_RE.pattern == core._CGROUP_JOB_RE.pattern

    cgroup = tmp_path / "cgroup"
    cgroup.write_text("0::/system.slice/slurmstepd.scope/job_4242/step_batch\n",
                      encoding="utf-8")
    assert shim.CGROUP_JOB_RE.search(cgroup.read_text()).group(1) == "4242"

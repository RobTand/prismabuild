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
    """A fake compute node: a docker that records, and a lane root."""

    binaries = tmp_path / "bin"
    binaries.mkdir()
    calls = tmp_path / "docker-calls"
    listed = tmp_path / "docker-ps-output"
    listed.write_text("")
    docker = binaries / "docker"
    docker.write_text(
        "#!/bin/bash\n"
        f'printf "%s\\n" "$*" >> {calls}\n'
        'if [ "$1" = "ps" ]; then\n'
        f'    cat {listed}\n'
        "fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    return {
        "bin": binaries, "calls": calls, "listed": listed,
        "lane": tmp_path / "lane", "checkouts": tmp_path / "checkouts",
    }


def _state(node: dict[str, Path], *, job_id: str, owner: str,
           checkout_dir: str, local_root: str) -> Path:
    jobs = node["lane"] / "jobs"
    jobs.mkdir(parents=True, exist_ok=True)
    path = jobs / f"{job_id}.job"
    path.write_text(
        f"action_key={'cd' * 32}\n"
        f"container_owner={owner}\n"
        f"checkout_dir={checkout_dir}\n"
        f"local_checkout_root={local_root}\n"
        "host=sparky\n"
        "pid=1\n",
        encoding="utf-8",
    )
    return path


def _run(node: dict[str, Path], job_id: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["/bin/bash", str(EPILOG)],
        capture_output=True, text=True,
        env={
            **os.environ,
            "PATH": f"{node['bin']}{os.pathsep}{os.environ['PATH']}",
            "SLURM_JOB_ID": job_id,
            "PRISMABUILD_SLURM_LANE_ROOT": str(node["lane"]),
        },
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

    (node["lane"] / "jobs").mkdir(parents=True)
    result = _run(node, "9999")
    assert result.returncode == 0
    assert not node["calls"].exists()


def test_it_never_drains_the_node(node: dict[str, Path]) -> None:
    """A non-zero Epilog puts the node in DRAIN.  A cleanup that could not find
    a container must not take a box out of the fleet, so every path exits 0 --
    including the one where the state file names nothing usable at all."""

    jobs = node["lane"] / "jobs"
    jobs.mkdir(parents=True)
    (jobs / "1238.job").write_text("garbage\n", encoding="utf-8")
    assert _run(node, "1238").returncode == 0

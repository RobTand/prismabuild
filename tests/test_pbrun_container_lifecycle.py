"""pbrun owns Docker payloads after they leave the process tree."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import pbrun  # noqa: E402


def _submission_variables(argv, monkeypatch, tmp_path):
    captured = []
    assert subprocess.run(
        ["git", "init", "-q", str(tmp_path)], check=False
    ).returncode == 0
    assert subprocess.run(
        [
            "git", "-C", str(tmp_path),
            "-c", "user.name=PrismaBuild test",
            "-c", "user.email=test@example.invalid",
            "commit", "--allow-empty", "-qm", "fixture",
        ],
        check=False,
    ).returncode == 0

    class Stop(Exception):
        pass

    def stop(body, *_args, **_kwargs):
        captured.append(body)
        raise Stop()

    monkeypatch.setattr(pbrun.pb, "seal_action", stop)
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    monkeypatch.setattr(
        pbrun, "CONTAINER_WRAPPER_DIR", tmp_path / "fleet" / "repo" / "tools"
    )
    monkeypatch.setattr(pbrun, "git_repository_root", lambda _cwd: tmp_path)
    monkeypatch.setattr(
        pbrun,
        "build_git_checkout_snapshot",
        lambda *_args, **_kwargs: {"input": {"id": "test"}},
    )
    monkeypatch.setattr(sys, "argv", ["pbrun.py", "--cwd", str(tmp_path), *argv])
    with pytest.raises(Stop):
        pbrun.main()
    return captured[0]["environment"]["variables"]


def test_every_submission_gets_a_sealed_container_owner(
    monkeypatch, tmp_path: Path
) -> None:
    variables = _submission_variables(["--", "true"], monkeypatch, tmp_path)
    owner = variables["PRISMABUILD_CONTAINER_OWNER"]
    marker = variables["PRISMABUILD_CONTAINER_MARKER"]
    assert len(owner) == 64 and set(owner) <= set("0123456789abcdef")
    assert marker.endswith(f"/container-owners/{owner}.used")
    assert variables["PATH"].split(":")[0] == str(
        tmp_path / "fleet" / "repo" / "tools"
    )


def test_pool_item_carries_the_same_container_owner(tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "queue")
    owner = "a" * 64
    queue.publish(
        action_key="b" * 64,
        cas_root="/cas",
        checkout_root="/checkout",
        worker_script="/worker.py",
        container_owner=owner,
    )
    item = json.loads(queue.item_path(pool.READY, "b" * 64).read_text())
    assert item["container_owner"] == owner


def test_docker_shim_marks_and_labels_a_created_container(tmp_path: Path) -> None:
    shim = ROOT / "tools" / "fleet" / "docker"
    called = tmp_path / "called.json"
    real = tmp_path / "real-docker"
    real.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, pathlib, sys\n"
        "pathlib.Path(os.environ['CALLED']).write_text(json.dumps(sys.argv[1:]))\n"
    )
    real.chmod(0o755)
    owner = "1" * 64
    marker = tmp_path / "owner.used"
    environment = dict(os.environ)
    environment.update({
        "CALLED": str(called),
        "PRISMABUILD_CONTAINER_OWNER": owner,
        "PRISMABUILD_CONTAINER_MARKER": str(marker),
        "PRISMABUILD_DOCKER_REAL": str(real),
        "PRISMABUILD_DOCKER_TESTING": "1",
    })

    result = subprocess.run(
        [str(shim), "run", "--rm", "example:image"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert marker.read_text().strip() == owner
    argv = json.loads(called.read_text())
    assert argv[:3] == ["run", "--label", f"prismabuild.action={owner}"]


def test_docker_shim_adds_the_job_label_inside_a_slurm_job(tmp_path: Path) -> None:
    """A second label naming the job, so one action's two concurrent SLURM jobs
    do not share an ownership label and remove each other's containers.

    Read from the kernel-owned cgroup, never from the environment: SLURM puts
    every process of a job under ``job_<id>`` and nothing in the job can move
    itself out, whereas anything can export a variable.
    """

    shim = ROOT / "tools" / "fleet" / "docker"
    called = tmp_path / "called.json"
    real = tmp_path / "real-docker"
    real.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, pathlib, sys\n"
        "pathlib.Path(os.environ['CALLED']).write_text(json.dumps(sys.argv[1:]))\n"
    )
    real.chmod(0o755)
    cgroup = tmp_path / "cgroup"
    cgroup.write_text(
        "0::/system.slice/slurmstepd.scope/job_4242/step_batch/user\n",
        encoding="utf-8")
    owner = "1" * 64
    environment = dict(os.environ)
    environment.update({
        "CALLED": str(called),
        "PRISMABUILD_CONTAINER_OWNER": owner,
        "PRISMABUILD_CONTAINER_MARKER": str(tmp_path / "owner.used"),
        "PRISMABUILD_DOCKER_REAL": str(real),
        "PRISMABUILD_DOCKER_TESTING": "1",
        "PRISMABUILD_CGROUP_FILE": str(cgroup),
        # Exported, and deliberately a lie: the cgroup decides.
        "SLURM_JOB_ID": "9999",
    })

    result = subprocess.run(
        [str(shim), "run", "--rm", "example:image"],
        env=environment, capture_output=True, text=True, check=False,
    )

    assert result.returncode == 0, result.stderr
    argv = json.loads(called.read_text())
    assert argv[:5] == [
        "run",
        "--label", f"prismabuild.action={owner}",
        "--label", "prismabuild.job=4242",
    ]


def test_docker_shim_refuses_a_caller_set_job_label(tmp_path: Path) -> None:
    """Both labels are reserved: a caller that could set the job label could
    hand its container to another job's Epilog, or hide it from its own."""

    shim = ROOT / "tools" / "fleet" / "docker"
    environment = dict(os.environ)
    environment.update({
        "PRISMABUILD_CONTAINER_OWNER": "1" * 64,
        "PRISMABUILD_CONTAINER_MARKER": str(tmp_path / "owner.used"),
        "PRISMABUILD_DOCKER_TESTING": "1",
    })

    result = subprocess.run(
        [str(shim), "run", "--label", "prismabuild.job=1", "example:image"],
        env=environment, capture_output=True, text=True, check=False,
    )

    assert result.returncode == 125
    assert "may not set the reserved" in result.stderr

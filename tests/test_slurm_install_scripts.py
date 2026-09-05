"""The install scripts, read back as the command lists an operator would run.

None of these has run on the fleet, and none of them can: SLURM is installed on
no box and every one of them needs either root or three live machines.  What
can be checked here is everything short of that, and it is not nothing --
`--dry-run` on install.sh, cutover.sh and rollback.sh prints exactly the
commands they would run, so the rendered list is the artifact under test.

The per-box assertions are the ones a mistake would be silent in.  A Spark that
apt-installs Ubuntu 24.04's own slurm-wlm gets 23.11.4, which a 25.11
controller refuses; a second `slurmctld` enabled on a Spark is a second
controller; a `slurm` user at any uid but 64030 breaks controller/node state
exchange with an error naming neither box.  And the munge key must never touch
/tmp (an out-of-memory event cleared it on this fleet once) or /mnt/shared (it
is the fleet's shared secret and an NFS export is the wrong place for one).
"""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
FLEET = ROOT / "fleet" / "slurm"
SCRIPTS = sorted(FLEET.rglob("*.sh"))
BOXES = ("dl380g10", "sparky", "gx10-6b77")
DEB_GLOB = "/home/rob/slurm-build/arm64-24.04/*.deb"
KEY_B64 = "/home/rob/.munge-key.b64"


def _fake_hostname(tmp_path: Path, name: str) -> dict[str, str]:
    """A PATH whose `hostname` answers `name`, so one script can be all boxes."""

    binaries = tmp_path / f"bin-{name}"
    binaries.mkdir(exist_ok=True)
    script = binaries / "hostname"
    script.write_text(f"#!/bin/sh\necho {name}\n", encoding="utf-8")
    script.chmod(0o755)
    environment = dict(os.environ)
    environment["PATH"] = f"{binaries}{os.pathsep}{environment['PATH']}"
    return environment


def _dry_run(tmp_path: Path, box: str) -> str:
    result = subprocess.run(
        ["bash", str(FLEET / "install.sh"), "--dry-run"],
        capture_output=True, text=True, check=False,
        env=_fake_hostname(tmp_path, box),
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


@pytest.fixture(scope="module")
def rendered(tmp_path_factory) -> dict[str, str]:
    tmp_path = tmp_path_factory.mktemp("boxes")
    return {box: _dry_run(tmp_path, box) for box in BOXES}


def _commands(text: str) -> list[str]:
    """Every line that is a command rather than a comment or a blank."""

    return [
        line for line in text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


# -- every script is a script ------------------------------------------------


def test_there_are_scripts_to_check() -> None:
    names = {path.name for path in SCRIPTS}
    assert {"install.sh", "verify.sh", "cutover.sh", "rollback.sh"} <= names


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.name)
def test_bash_accepts_the_script(script: Path) -> None:
    result = subprocess.run(
        ["bash", "-n", str(script)], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.name)
def test_every_script_is_executable(script: Path) -> None:
    assert os.access(script, os.X_OK), f"{script} is not executable"


@pytest.mark.parametrize(
    "script", [FLEET / n for n in ("install.sh", "verify.sh", "cutover.sh", "rollback.sh")],
    ids=lambda p: p.name,
)
def test_help_works_without_root_and_without_a_fleet(script: Path) -> None:
    result = subprocess.run(
        ["bash", str(script), "--help"], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout


# -- install.sh, per box -----------------------------------------------------


def test_a_box_that_is_not_in_the_fleet_is_refused(tmp_path: Path) -> None:
    result = subprocess.run(
        ["bash", str(FLEET / "install.sh"), "--dry-run"],
        capture_output=True, text=True, check=False,
        env=_fake_hostname(tmp_path, "somebody-elses-laptop"),
    )
    assert result.returncode == 2
    assert "not a fleet box" in result.stderr


def test_the_controller_installs_slurm_from_apt(rendered) -> None:
    text = rendered["dl380g10"]
    assert (
        "env DEBIAN_FRONTEND=noninteractive apt-get install -y "
        "slurm-wlm slurmctld slurmd munge" in text
    )
    assert DEB_GLOB not in text, "the controller must not install the Sparks' arm64 debs"


@pytest.mark.parametrize("box", ("sparky", "gx10-6b77"))
def test_a_spark_installs_the_prebuilt_debs_and_never_the_archive(rendered, box) -> None:
    text = rendered[box]
    assert (
        f"env DEBIAN_FRONTEND=noninteractive apt-get install -y {DEB_GLOB} munge"
        in text
    )
    # Ubuntu 24.04's own slurm-wlm is 23.11.4 and a 25.11 controller refuses it.
    assert "apt-get install -y slurm-wlm" not in text


@pytest.mark.parametrize("box", BOXES)
def test_the_slurm_user_is_created_at_the_fleet_uid(rendered, box) -> None:
    text = rendered[box]
    assert "groupadd -g 64030 slurm" in text
    assert (
        "useradd -u 64030 -g slurm -s /usr/sbin/nologin -M -d /var/spool/slurm slurm"
        in text
    )


@pytest.mark.parametrize("box", BOXES)
def test_all_four_configuration_files_are_installed(rendered, box) -> None:
    text = rendered[box]
    for name in ("slurm.conf", "gres.conf", "cgroup.conf"):
        assert f"install -m 644 {FLEET / name} /etc/slurm/{name}" in text, name
    assert f"install -m 755 {FLEET / 'epilog.sh'} /etc/slurm/epilog.sh" in text
    # Four files, not five.  On cgroup v2 SLURM parses AllowedDevicesFile only
    # to warn about it and containment is an eBPF program, so a file listing
    # the NVIDIA control interfaces was answering a question nothing asks.
    assert "cgroup_allowed_devices_file.conf" not in text


@pytest.mark.parametrize("box", BOXES)
def test_the_munge_key_never_travels_through_tmp_or_the_shared_mount(
    rendered, box,
) -> None:
    """/tmp was cleared by an OOM once; /mnt/shared is an NFS export."""

    touching_the_key = [
        line for line in rendered[box].splitlines()
        if "munge.key" in line or ".munge-key.b64" in line
    ]
    assert touching_the_key, "no line in this box's run touches the key at all"
    for line in touching_the_key:
        assert "/tmp" not in line, line
        assert "/mnt/shared" not in line, line
        assert KEY_B64 in line or "/etc/munge/" in line, line


def test_only_the_controller_runs_a_controller(rendered) -> None:
    assert "systemctl enable slurmctld" in rendered["dl380g10"]
    for box in ("sparky", "gx10-6b77"):
        assert "slurmctld" not in rendered[box], (
            f"{box} would enable a second controller"
        )


@pytest.mark.parametrize("box", BOXES)
def test_every_box_runs_a_node_daemon_and_munge(rendered, box) -> None:
    assert "systemctl enable slurmd" in rendered[box]
    assert "systemctl enable munge" in rendered[box]


@pytest.mark.parametrize("box", BOXES)
def test_the_shared_job_directory_is_created_as_rob_not_as_root(rendered, box) -> None:
    """dl380g10 exports /mnt/shared with root_squash: root there is `nobody`."""

    text = rendered[box]
    assert (
        "runuser -u rob -- mkdir -p /mnt/shared/prismabuild-fleet/slurm/jobs" in text
    )
    assert (
        "runuser -u rob -- chmod 1777 /mnt/shared/prismabuild-fleet/slurm/jobs" in text
    )


@pytest.mark.parametrize("box", BOXES)
def test_a_dry_run_runs_nothing_at_all(rendered, box) -> None:
    """Every printed line is a comment or a command; none of them was executed."""

    text = rendered[box]
    assert "dry run, nothing is executed" in text
    # A dry run that had executed anything would have reported a failure, and
    # every failure names its step.
    assert "FAILED at step" not in text
    assert _commands(text), "a dry run that prints no commands is not a plan"


# -- cutover.sh refuses ------------------------------------------------------


def _cutover_environment(tmp_path: Path) -> dict[str, str]:
    queue = tmp_path / "pb-queue"
    (queue / "claimed").mkdir(parents=True)
    (queue / "ready").mkdir(parents=True)
    state = tmp_path / "state"
    state.mkdir()
    runtime = tmp_path / "runtime"
    (runtime / "runtime-generations" / "gen-old").mkdir(parents=True)
    (runtime / "repo").symlink_to("runtime-generations/gen-old")
    environment = dict(os.environ)
    environment.update(
        PB_QUEUE_ROOT=str(queue),
        PB_RUNTIME_DIR=str(runtime),
        PB_STATE_DIR=str(state),
        # One box, and it is this one, so nothing here can reach the fleet even
        # if a refusal failed to fire.
        PB_BOXES=subprocess.run(
            ["hostname", "-s"], capture_output=True, text=True, check=True
        ).stdout.strip(),
        PB_SPARKS="",
        PB_SSH="false",
    )
    return environment


def _cutover(environment: dict[str, str], *argv: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(FLEET / "cutover.sh"), *argv],
        capture_output=True, text=True, check=False, env=environment,
    )


def test_cutover_refuses_without_yes(tmp_path: Path) -> None:
    environment = _cutover_environment(tmp_path)
    (Path(environment["PB_STATE_DIR"]) / "slurm-verify-passed.json").write_text("{}")
    result = _cutover(environment)
    assert result.returncode == 1
    assert "pass --yes" in result.stderr


def test_cutover_refuses_when_the_queue_still_holds_a_claim(tmp_path: Path) -> None:
    environment = _cutover_environment(tmp_path)
    (Path(environment["PB_STATE_DIR"]) / "slurm-verify-passed.json").write_text("{}")
    claim = Path(environment["PB_QUEUE_ROOT"]) / "claimed" / ("a" * 64 + ".json")
    claim.write_text("{}")
    result = _cutover(environment, "--yes")
    assert result.returncode == 1
    assert "claimed is not empty" in result.stderr
    assert claim.name in result.stderr


def test_cutover_refuses_when_the_queue_still_holds_a_ready_item(tmp_path: Path) -> None:
    """An item in ready is one no SLURM job would ever pick up."""

    environment = _cutover_environment(tmp_path)
    (Path(environment["PB_STATE_DIR"]) / "slurm-verify-passed.json").write_text("{}")
    (Path(environment["PB_QUEUE_ROOT"]) / "ready" / ("b" * 64 + ".json")).write_text("{}")
    result = _cutover(environment, "--yes")
    assert result.returncode == 1
    assert "ready is not empty" in result.stderr


def test_cutover_refuses_before_the_kills_when_publication_would_refuse(
    tmp_path: Path,
) -> None:
    """Step 5 is the step that cannot be re-run.

    It publishes the generation, and by the time it runs the crontab is edited
    and every loop on all three boxes is dead.  ``publish_runtime.py`` refuses
    a dirty tree, and the checkout the runbook names is a worktree that
    collects untracked files, so that refusal is a thing that happens.  It has
    to happen before anything is stopped.
    """

    environment = _cutover_environment(tmp_path)
    (Path(environment["PB_STATE_DIR"]) / "slurm-verify-passed.json").write_text("{}")
    refusing = tmp_path / "publish_stub.sh"
    refusing.write_text(
        "#!/bin/sh\n"
        "echo 'refusing to publish a dirty tree' >&2\n"
        "exit 1\n",
        encoding="utf-8",
    )
    refusing.chmod(0o755)
    environment["PB_PUBLISH"] = f"sh {refusing}"

    result = _cutover(environment, "--yes")

    assert result.returncode == 1
    assert "refusing to publish a dirty tree" in result.stderr
    # Before any box was touched at all: no step ran, and the fleet-wide pbrun
    # scan did not even happen.
    assert "step 1" not in result.stdout
    assert "no pbrun is waiting" not in result.stdout


def test_cutover_publishes_through_the_interpreter(tmp_path: Path) -> None:
    """publish_runtime.py is checked in mode 644.

    Running it as a command is a "Permission denied" at the one step that has
    no cheap retry, so both scripts name an interpreter.
    """

    for name in ("cutover.sh", "rollback.sh"):
        text = (FLEET / name).read_text(encoding="utf-8")
        assert 'PUBLISH="${PB_PUBLISH:-python3 ' in text, name
        assert '"$REPO/tools/fleet/publish_runtime.py"' not in text, name
    assert not os.access(ROOT / "tools" / "fleet" / "publish_runtime.py", os.X_OK)


def test_cutover_refuses_when_verification_did_not_pass_here(tmp_path: Path) -> None:
    result = _cutover(_cutover_environment(tmp_path), "--yes")
    assert result.returncode == 1
    assert "verify.sh" in result.stderr


def test_a_cutover_dry_run_names_its_refusals_and_publishes_the_transport(
    tmp_path: Path,
) -> None:
    result = _cutover(_cutover_environment(tmp_path), "--dry-run", "--yes")
    assert result.returncode == 0, result.stderr
    assert "publish_runtime.py --default-transport slurm" in result.stdout
    assert "a live run refuses unless all five of these hold" in result.stdout
    # The order that makes the cutover stick.
    crontab = result.stdout.index("step 1: take the supervise line")
    supervisors = result.stdout.index("step 2: stop the supervisors")
    loops = result.stdout.index("step 3: stop the worker loops")
    assert crontab < supervisors < loops


# -- rollback.sh -------------------------------------------------------------


def test_rollback_restores_the_previous_generation_by_name(tmp_path: Path) -> None:
    environment = _cutover_environment(tmp_path)
    state = Path(environment["PB_STATE_DIR"]) / "cutover-1788600000.json"
    state.write_text(
        '{\n'
        ' "schema": "prismaquant.prismabuild.slurm_cutover.v1",\n'
        f' "boxes": "{environment["PB_BOXES"]}",\n'
        ' "sparks": "",\n'
        ' "previous_generation": "gen-old",\n'
        ' "crontab_backup": "/home/rob/.prismabuild/crontab.pre-cutover"\n'
        '}\n',
        encoding="utf-8",
    )
    result = subprocess.run(
        ["bash", str(FLEET / "rollback.sh"), "--dry-run"],
        capture_output=True, text=True, check=False, env=environment,
    )
    assert result.returncode == 0, result.stderr
    assert "--activate-generation gen-old" in result.stdout
    assert "crontab '/home/rob/.prismabuild/crontab.pre-cutover'" in result.stdout
    # Runtime first: a window where workers drain the queue while producers are
    # told to use SLURM is the one order that can double-execute an action.
    assert result.stdout.index("step 1: point the live runtime") < result.stdout.index(
        "step 4: start one supervisor"
    )


def test_rollback_refuses_when_there_is_no_cutover_to_reverse(tmp_path: Path) -> None:
    environment = _cutover_environment(tmp_path)
    result = subprocess.run(
        ["bash", str(FLEET / "rollback.sh"), "--dry-run"],
        capture_output=True, text=True, check=False, env=environment,
    )
    assert result.returncode == 1
    assert "no cutover state file" in result.stderr


# -- shellcheck, when it can be had ------------------------------------------


@pytest.mark.skipif(
    shutil.which("docker") is None, reason="shellcheck is run through docker"
)
def test_shellcheck_is_clean() -> None:
    """shellcheck 0.11.0 via koalaman/shellcheck-alpine; skipped without docker."""

    result = subprocess.run(
        [
            "docker", "run", "--rm", "-v", f"{ROOT}:/mnt:ro", "-w", "/mnt",
            "koalaman/shellcheck-alpine:stable", "shellcheck",
            *[str(path.relative_to(ROOT)) for path in SCRIPTS],
        ],
        capture_output=True, text=True, check=False,
    )
    if result.returncode != 0 and "Unable to find image" in result.stderr:
        pytest.skip("shellcheck image is not available and cannot be pulled")
    assert result.returncode == 0, result.stdout + result.stderr

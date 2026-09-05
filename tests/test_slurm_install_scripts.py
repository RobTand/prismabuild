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

import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import time

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
def test_the_munge_self_test_fails_when_munge_does(
    rendered, box, tmp_path: Path
) -> None:
    """``munge -n | unmunge | head`` reports ``head``'s status, so pre-fix a
    munge that could not round-trip a credential passed the only check the
    install makes of it, and the first ``sbatch`` failed instead."""

    lines = [line for line in rendered[box].splitlines() if "munge -n" in line]
    assert len(lines) == 1, lines
    self_test = lines[0]
    assert self_test.startswith("set -o pipefail;"), self_test

    fakes = tmp_path / f"fakes-{box}"
    fakes.mkdir()
    (fakes / "munge").write_text(
        "#!/bin/sh\n"
        "if [ \"${FAKE_MUNGE_BROKEN:-0}\" = 1 ]; then\n"
        "    echo 'munge: Error: Failed to access \"/run/munge/munge.socket.2\"' >&2\n"
        "    exit 1\n"
        "fi\n"
        "echo MUNGE:AwQDAAA=:\n",
        encoding="utf-8")
    # The real unmunge prints about a dozen lines.  This one pauses after the
    # fifth, so a truncation that closes the pipe early is still being written
    # to when it does: under pipefail that SIGPIPE is the pipeline's status.
    (fakes / "unmunge").write_text(
        "#!/bin/sh\n"
        "cat >/dev/null\n"
        "for n in 1 2 3 4 5; do echo \"LINE $n\"; done\n"
        "sleep 0.3\n"
        "for n in 6 7 8 9 10 11 12; do echo \"LINE $n\" || exit 141; done\n",
        encoding="utf-8")
    for name in ("munge", "unmunge"):
        (fakes / name).chmod(0o755)
    environment = dict(os.environ)
    environment["PATH"] = f"{fakes}{os.pathsep}{environment['PATH']}"

    broken = subprocess.run(
        ["bash", "-c", self_test], capture_output=True, text=True,
        check=False, env={**environment, "FAKE_MUNGE_BROKEN": "1"},
    )
    assert broken.returncode != 0
    assert "munge.socket" in broken.stderr

    healthy = subprocess.run(
        ["bash", "-c", self_test], capture_output=True, text=True,
        check=False, env=environment,
    )
    assert healthy.returncode == 0, (healthy.returncode, healthy.stderr)
    assert healthy.stdout.splitlines() == [f"LINE {n}" for n in range(1, 6)]


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


#: One box, and it is no box.  Naming this box instead made ``on_box`` take
#: its local branch, so a live cutover under test ran the STOP_FUNCTIONS
#: snippet -- ``kill`` and ``kill -9`` included -- on the developer's machine,
#: and only a fake ``pgrep`` that exits 1 kept it from finding a pid.  A name
#: no box answers to sends every snippet down the remote branch instead, which
#: is also the branch the fleet actually uses.  It must not contain "sparky" or
#: "sparklina": one test asserts neither name reaches step 4.
FAKE_BOX = "pb-no-such-box"


def _fake_ssh(tmp_path: Path) -> Path:
    """An ``ssh`` that runs the snippet here, against the fakes.

    ``cutover.sh`` pipes the snippet into ``$SSH <box> bash -s``, so dropping
    the box name and exec-ing the rest reads the same snippet from the same
    stdin.  The fakes go on ``PATH`` first, which is what makes ``pgrep`` and
    ``crontab`` inside the snippet the test's own.
    """

    fakes = tmp_path / "fakes"
    fakes.mkdir(exist_ok=True)
    script = fakes / "ssh"
    script.write_text(
        "#!/bin/sh\n"
        f"echo \"ssh $*\" >> '{tmp_path}/calls'\n"
        "shift\n"
        f"PATH='{fakes}':\"$PATH\"\n"
        "export PATH\n"
        'exec "$@"\n',
        encoding="utf-8",
    )
    script.chmod(0o755)
    return script


#: The marker ``fleet/slurm/verify.sh`` writes on a pass, in its shape.
#: ``cutover.sh`` reads ``slurm_conf_sha256`` and ``verified_unix`` back out
#: and refuses a marker that verified a slurm.conf this checkout no longer
#: contains, so ``{}`` no longer stands in for a passing verification.  Pass
#: ``key=None`` to leave a field out.
def write_verify_marker(environment: dict[str, str], **overrides: object) -> Path:
    fields: dict[str, object] = {
        "schema": "prismaquant.prismabuild.slurm_verify.v1",
        "host": "sparky",
        # Three hours and twelve minutes ago, so the age line has both units.
        "verified_unix": int(time.time()) - 11520,
        "checkout": str(ROOT),
        "commit": "0123456789abcdef0123456789abcdef01234567",
        "slurm_conf_sha256": hashlib.sha256(
            (FLEET / "slurm.conf").read_bytes()).hexdigest(),
        "rows": 21,
    }
    fields.update(overrides)
    path = Path(environment["PB_STATE_DIR"]) / "slurm-verify-passed.json"
    path.write_text(
        json.dumps({name: value for name, value in fields.items()
                    if value is not None}, indent=1) + "\n",
        encoding="utf-8",
    )
    return path


#: A ``sinfo -h -N -o '%N %T'`` that answers with ``table``, one
#: ``<node> <state>`` line per row.  ``cutover.sh`` asks the controller
#: whether every box is usable before it changes anything, and SLURM is
#: installed on no box in this fleet, so the answer has to be faked here.
def write_fake_sinfo(tmp_path: Path, table: str) -> Path:
    fakes = tmp_path / "fakes"
    fakes.mkdir(exist_ok=True)
    script = fakes / "sinfo"
    script.write_text(
        "#!/bin/sh\n"
        f"echo \"sinfo $*\" >> '{tmp_path}/calls'\n"
        f"cat <<'PBNODES'\n{table}\nPBNODES\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    return script


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
        PB_BOXES=FAKE_BOX,
        PB_SPARKS="",
        PB_SSH=str(_fake_ssh(tmp_path)),
    )
    return environment


def _cutover(environment: dict[str, str], *argv: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(FLEET / "cutover.sh"), *argv],
        capture_output=True, text=True, check=False, env=environment,
    )


def test_install_reads_its_configuration_from_beside_itself(tmp_path: Path) -> None:
    """The install has to work on a box with no checkout.

    Measured 2026-09-05: neither dl380g10 nor sparklina has
    ``/home/rob/prismabuild``, and ``fleet/`` is not among the files
    ``publish_runtime`` mirrors to ``/mnt/shared``, so the way this script
    reaches those two boxes is a copy of the one directory.  Resolving the
    config files through ``../..`` would have looked for them in ``/home``.
    """

    copied = tmp_path / "pb-slurm"
    copied.mkdir()
    for source in FLEET.iterdir():
        if source.is_file():
            shutil.copy2(source, copied / source.name)

    result = subprocess.run(
        ["bash", str(copied / "install.sh"), "--dry-run"],
        capture_output=True, text=True, check=False,
        env=_fake_hostname(tmp_path, "sparky"),
    )

    assert result.returncode == 0, result.stderr
    assert f"configs    : {copied}" in result.stdout
    assert str(copied / "slurm.conf") in result.stdout


def test_cutover_refuses_without_yes(tmp_path: Path) -> None:
    environment = _cutover_environment(tmp_path)
    write_verify_marker(environment)
    result = _cutover(environment)
    assert result.returncode == 1
    assert "pass --yes" in result.stderr


def test_cutover_refuses_when_the_queue_still_holds_a_claim(tmp_path: Path) -> None:
    environment = _cutover_environment(tmp_path)
    write_verify_marker(environment)
    claim = Path(environment["PB_QUEUE_ROOT"]) / "claimed" / ("a" * 64 + ".json")
    claim.write_text("{}")
    result = _cutover(environment, "--yes")
    assert result.returncode == 1
    assert "claimed is not empty" in result.stderr
    assert claim.name in result.stderr


def test_cutover_refuses_when_the_queue_still_holds_a_ready_item(tmp_path: Path) -> None:
    """An item in ready is one no SLURM job would ever pick up."""

    environment = _cutover_environment(tmp_path)
    write_verify_marker(environment)
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
    write_verify_marker(environment)
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


@pytest.mark.parametrize("name, marker, boundary, expected", [
    ("cutover.sh", "# step 5: publish", "# -- the state file, completed",
     ["--default-transport", "slurm"]),
    ("rollback.sh", "# step 1: point", "# -- 1b.",
     ["--activate-generation", "gen-old"]),
])
def test_cutover_publishes_through_the_interpreter(
    tmp_path: Path, name: str, marker: str, boundary: str, expected: list[str],
) -> None:
    """publish_runtime.py is checked in mode 644.

    Running it as a command is a "Permission denied" at the one step that has
    no cheap retry, so both scripts name an interpreter.
    """

    publisher = tmp_path / "tools" / "fleet" / "publish_runtime.py"
    publisher.parent.mkdir(parents=True)
    publisher.write_text("import json, sys\nprint(json.dumps(sys.argv[1:]))\n")
    publisher.chmod(0o644)
    source = (FLEET / name).read_text(encoding="utf-8")
    # Evaluate configuration, including later reassignments, then the live
    # publication step. No stop commands or runtime mutations are executed.
    config = source[source.index('\n#', source.index('REPO="')) + 1:]
    config = config[:config.index('\nexec 3>&1')]
    start = source.rindex('say ', 0, source.index(marker))
    step = source[start:source.index(boundary, start)]
    program = '\n'.join([
        'set -eu', 'REPO="$TEST_REPO"', config,
        'DRY_RUN=0', 'PREVIOUS=gen-old',
        'say() { :; }', 'die() { echo "$*" >&2; exit 1; }', step,
    ])
    environment = {k: v for k, v in os.environ.items() if not k.startswith("PB_")}
    environment["TEST_REPO"] = str(tmp_path)
    result = subprocess.run(
        ["/bin/bash", "-c", program], env=environment,
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == expected


def test_cutover_refuses_when_verification_did_not_pass_here(tmp_path: Path) -> None:
    result = _cutover(_cutover_environment(tmp_path), "--yes")
    assert result.returncode == 1
    assert "verify.sh" in result.stderr


def test_cutover_honours_an_empty_pb_sparks(tmp_path: Path) -> None:
    """``PB_SPARKS`` is the list of boxes with a pqwork unit, and the tests set
    it empty so a run reaches no Spark.  Pre-fix the script read it with
    ``:-``, which treats empty as unset, and step 4 went to both Sparks."""

    environment = _cutover_environment(tmp_path)
    assert environment["PB_SPARKS"] == ""
    result = _cutover(environment, "--dry-run", "--yes")
    assert result.returncode == 0, result.stderr
    step4 = result.stdout.split("step 4:", 1)[1].split("step 5:", 1)[0]
    assert "systemctl --user stop pqwork.service" not in step4
    assert "sparklina" not in step4 and "sparky" not in step4


def _live_cutover(tmp_path: Path, *, crontab: str, publish_exit: int) -> dict[str, str]:
    """An environment in which a live cutover touches only fakes.

    Every snippet reaches ``_fake_ssh`` rather than this box's shell, and the
    fakes it puts on ``PATH`` answer the commands inside: ``pgrep`` finds
    nothing, so the stop steps have nothing to signal and the pbrun scan finds
    no waiter; ``crontab`` reads and writes one file under ``tmp_path``; the
    publish stub accepts ``--dry-run`` and exits ``publish_exit`` on the real
    publication; ``sinfo`` reports the one box idle, which is the last thing
    the refusal block asks before it writes anything.  The fakes log every
    call, and the tests read the log before trusting that the real commands
    were never reached.
    """

    environment = _cutover_environment(tmp_path)
    write_verify_marker(environment)
    fakes = tmp_path / "fakes"
    fakes.mkdir(exist_ok=True)
    (fakes / "pgrep").write_text(
        "#!/bin/sh\n"
        f"echo \"pgrep $*\" >> '{tmp_path}/calls'\n"
        "exit 1\n",
        encoding="utf-8",
    )
    crontab_file = tmp_path / "crontab"
    crontab_file.write_text(crontab, encoding="utf-8")
    (fakes / "crontab").write_text(
        "#!/bin/sh\n"
        f"echo \"crontab $*\" >> '{tmp_path}/calls'\n"
        f"case \"$1\" in -l) cat '{crontab_file}' ;; -) cat > '{crontab_file}' ;; esac\n",
        encoding="utf-8",
    )
    for name in ("pgrep", "crontab"):
        (fakes / name).chmod(0o755)
    write_fake_sinfo(tmp_path, f"{FAKE_BOX} idle")
    publish = tmp_path / "publish_stub.sh"
    publish.write_text(
        "#!/bin/sh\n"
        f"echo \"publish $*\" >> '{tmp_path}/calls'\n"
        "case \"$*\" in *--dry-run*) exit 0 ;; esac\n"
        f"echo 'publication failed' >&2; exit {publish_exit}\n",
        encoding="utf-8",
    )
    publish.chmod(0o755)
    environment["PB_PUBLISH"] = f"sh {publish}"
    environment["PATH"] = f"{fakes}{os.pathsep}{environment['PATH']}"
    return environment


SUPERVISE_LINE = (
    "*/5 * * * * /usr/bin/python3 /mnt/shared/prismabuild-fleet/repo/tools/"
    "supervise.py --ensure >> /home/rob/tmp/pb-supervisor.log 2>&1"
)


def test_cutover_writes_the_state_file_rollback_needs_before_it_stops_anything(
    tmp_path: Path,
) -> None:
    """Step 5's failure message says to run rollback.sh, and rollback.sh
    refuses without a state file.  Pre-fix the state file was written after
    step 5, so the one failure that points at rollback left it nothing to
    read, with the crontab already edited and every loop dead."""

    environment = _live_cutover(
        tmp_path, crontab=SUPERVISE_LINE + "\n", publish_exit=1)
    result = _cutover(environment, "--yes")
    assert result.returncode == 1, result.stderr
    assert "run fleet/slurm/rollback.sh" in result.stderr
    calls = (tmp_path / "calls").read_text(encoding="utf-8")
    assert "crontab -" in calls and "pgrep" in calls and "publish" in calls

    state_files = sorted(Path(environment["PB_STATE_DIR"]).glob("cutover-*.json"))
    assert len(state_files) == 1, state_files
    state = json.loads(state_files[0].read_text(encoding="utf-8"))
    assert state["previous_generation"] == "gen-old"
    assert state["boxes"] == environment["PB_BOXES"]
    assert state["crontab_backup"] == str(
        Path(environment["PB_STATE_DIR"]) / "crontab.pre-cutover")
    # The one field only the end of the run can know is empty, not guessed.
    assert state["new_generation"] == ""

    rollback = subprocess.run(
        ["bash", str(FLEET / "rollback.sh"), "--dry-run"],
        capture_output=True, text=True, check=False, env=environment,
    )
    assert rollback.returncode == 0, rollback.stderr
    assert "--activate-generation gen-old" in rollback.stdout


def test_a_rerun_after_a_partial_cutover_keeps_the_crontab_backup(
    tmp_path: Path,
) -> None:
    """The backup is what rollback restores.  A cutover that failed at step 5
    has already taken the supervise line out; the re-run the failure message
    suggests then saw a crontab without the line.  Pre-fix it saved that
    crontab over the backup, and a rollback restored a fleet with no cron
    entry keeping a supervisor alive."""

    environment = _live_cutover(
        tmp_path, crontab="MAILTO=rob\n" + SUPERVISE_LINE + "\n", publish_exit=1)
    backup = Path(environment["PB_STATE_DIR"]) / "crontab.pre-cutover"

    first = _cutover(environment, "--yes")
    assert first.returncode == 1, first.stderr
    assert "supervise line removed" in first.stdout
    assert SUPERVISE_LINE in backup.read_text(encoding="utf-8")
    assert SUPERVISE_LINE not in (tmp_path / "crontab").read_text(encoding="utf-8")

    second = _cutover(environment, "--yes")
    assert second.returncode == 1, second.stderr
    assert "no supervise line in the crontab" in second.stdout
    saved = backup.read_text(encoding="utf-8")
    assert SUPERVISE_LINE in saved
    assert "MAILTO=rob" in saved


def test_a_cutover_dry_run_names_its_refusals_and_publishes_the_transport(
    tmp_path: Path,
) -> None:
    result = _cutover(_cutover_environment(tmp_path), "--dry-run", "--yes")
    assert result.returncode == 0, result.stderr
    assert "publish_runtime.py --default-transport slurm" in result.stdout
    assert "a live run refuses unless all six of these hold" in result.stdout
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


# -- what the runbook's scripts execute --------------------------------------


def test_every_tool_the_scripts_run_directly_is_executable() -> None:
    """`verify.sh` row 7 runs `tools/fleet/pbrun.py` as a command, not as an
    argument to an interpreter, and so do the README and the runbook.

    That is three documents agreeing on how the tool is invoked, against a file
    that was mode 644 with no shebang. Every one of them failed the same way,
    and none of the tests caught it because they all spell it
    ``sys.executable, str(PBRUN)``. Found on 2026-09-05, the first time
    `verify.sh` ran anywhere -- in `fleet/slurm/smoke/multinode`, against a
    real controller:

        [FAIL] 7    pbrun --transport slurm --here runs an action end to end
                  exit 126
                  timeout: failed to run command 'tools/fleet/pbrun.py':
                  Permission denied
    """

    executed: set[Path] = set()
    pattern = re.compile(r"(?<![\w/.-])(tools/fleet/[A-Za-z0-9_.-]+\.py)")
    for script in SCRIPTS:
        for line in script.read_text(encoding="utf-8").splitlines():
            bare = line.strip()
            if bare.startswith("#"):
                continue
            for name in pattern.findall(bare):
                # An argument to an interpreter does not need the bit.
                head = bare[: bare.index(name)]
                if re.search(r"python3?\s+\S*$", head):
                    continue
                executed.add(ROOT / name)

    assert executed, "no tool invocation found; has the pattern gone stale?"
    wrong = []
    for tool in sorted(executed):
        if not tool.is_file():
            wrong.append(f"{tool}: does not exist")
            continue
        if not os.access(tool, os.X_OK):
            wrong.append(f"{tool}: not executable")
        if not tool.read_text(encoding="utf-8").startswith("#!"):
            wrong.append(f"{tool}: no shebang")
    assert not wrong, "\n".join(wrong)


@pytest.mark.parametrize("status", [0, 17, 127])
def test_verify_row_4_reports_nvidia_smis_own_status(tmp_path: Path, status: int) -> None:
    """`$?` after a pipeline is the last command's, and the last command was
    `sed`.

    The verdict never read `smi-rc` -- it reads the `smi:` lines and the open
    probe -- but the operator reading a failure does. Measured on 2026-09-05 in
    `fleet/slurm/smoke/multinode`, where row 4's own evidence read:

        smi: /usr/bin/bash: line 2: nvidia-smi: command not found
        smi-rc=0
    """

    source = (FLEET / "verify.sh").read_text(encoding="utf-8")
    row = source.split("# -- row 4:", 1)[1].split("# -- row 5:", 1)[0]
    setup = row[:row.index('out="$(srun_here')]
    # Replace only the device path: no real GPU or scheduler is accessed.
    setup = setup.replace("/dev/nvidia0", str(tmp_path / "absent-device"))
    program = '\n'.join([
        setup,
        "hostname() { echo fake-spark; }",
        'nvidia-smi() { echo "driver result"; return "$TEST_SMI_STATUS"; }',
        'eval "$probe"',
    ])
    result = subprocess.run(
        ["/bin/bash", "-c", program], capture_output=True, text=True, timeout=10,
        env={**os.environ, "TEST_SMI_STATUS": str(status)},
    )
    assert result.returncode == 0, result.stderr
    assert "smi: driver result" in result.stdout
    assert f"smi-rc={status}" in result.stdout.splitlines()


def test_a_live_cutover_under_test_never_runs_a_stop_snippet_on_this_box(
    tmp_path: Path,
) -> None:
    """``PB_BOXES`` was this box's hostname, so ``on_box`` took the branch that
    runs the snippet locally: ``bash -c "$STOP_FUNCTIONS ..."`` with the real
    ``kill`` at cutover.sh:162 and ``kill -9`` at :172, on the machine running
    the suite.  Nothing about that was intended, and the remote branch the
    fleet uses was never exercised.
    """

    environment = _live_cutover(
        tmp_path, crontab=SUPERVISE_LINE + "\n", publish_exit=1)
    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stderr
    assert "(this box)" not in result.stdout
    calls = (tmp_path / "calls").read_text(encoding="utf-8")
    assert f"ssh {FAKE_BOX} bash -s" in calls


# -- cutover.sh reading a pid that has already gone --------------------------


def _stop_functions() -> str:
    """The snippet cutover.sh ships to every box, as it ships it."""

    text = (FLEET / "cutover.sh").read_text(encoding="utf-8")
    body = text.split("<<'SNIPPET'\n", 1)[1].split("\nSNIPPET\n", 1)[0]
    assert "pb_pids()" in body, body[:200]
    return body


def test_a_pid_that_exits_between_pgrep_and_the_read_is_silently_gone(
    tmp_path: Path,
) -> None:
    """`pgrep` lists a pid; by the time its argv is read it may be gone.

    Measured on dl380g10: the cutover printed
    ``bash: /proc/N/cmdline: No such file or directory``, because bash applies
    redirections left to right and the `2>/dev/null` came after the input
    redirect that failed.  Cosmetic, and it printed during the one operation
    where an unexplained error line is worst.
    """

    fakes = tmp_path / "fakes"
    fakes.mkdir()
    # A pid no process can hold: above this box's own pid_max.
    dead = int(Path("/proc/sys/kernel/pid_max").read_text()) + 1
    (fakes / "pgrep").write_text(f"#!/bin/sh\necho {dead}\n", encoding="utf-8")
    (fakes / "pgrep").chmod(0o755)
    assert not Path(f"/proc/{dead}").exists()

    environment = dict(os.environ)
    environment["PATH"] = f"{fakes}{os.pathsep}{environment['PATH']}"
    result = subprocess.run(
        ["bash", "-c", _stop_functions() + "\npb_pids supervise.py\n"],
        capture_output=True, text=True, check=False, env=environment,
    )

    assert result.returncode == 0, result.stderr
    assert result.stderr == "", result.stderr
    assert result.stdout == "", result.stdout


# -- the munge key does not outlive the run that needed it -------------------


def _shell_function(text: str, name: str) -> str:
    """One function definition, `name() {` to the closing brace in column 1."""

    start = text.index(f"{name}() {{")
    end = text.index("\n}\n", start) + len("\n}\n")
    return text[start:end]


def test_the_key_is_removed_on_a_box_that_has_no_shred(tmp_path: Path) -> None:
    """`shred -u` alone leaves the fleet's shared secret in a home directory
    on any box that does not have it.

    shred is coreutils and is on all three boxes today, so this is about the
    removal not depending on that staying true: cleanup that silently does
    nothing is the failure mode a secret cannot afford.
    """

    lines = [
        line for line in _dry_run(tmp_path, "sparky").splitlines()
        if "shred" in line
    ]
    assert len(lines) == 1, lines
    key = tmp_path / "key.b64"
    key.write_text("bXVuZ2U=\n", encoding="utf-8")

    # A PATH holding everything the removal needs except shred.
    coreutils = tmp_path / "no-shred"
    coreutils.mkdir()
    for name in ("rm", "cat"):
        (coreutils / name).symlink_to(shutil.which(name))
    assert shutil.which("shred", path=str(coreutils)) is None
    result = subprocess.run(
        ["/bin/bash", "-c", lines[0].replace(KEY_B64, str(key))],
        capture_output=True, text=True, check=False,
        env={**os.environ, "PATH": str(coreutils)},
    )

    assert result.returncode == 0, result.stderr
    assert not key.exists(), "the key survived a box with no shred"


def test_a_run_that_stops_after_reading_the_key_does_not_leave_it_behind(
    tmp_path: Path,
) -> None:
    """The install refuses rather than repairs, so it exits partway by design:
    a slurm uid it will not accept, a topology the box does not report, a SLURM
    older than the controller's.  Every one of those on a Spark happens after
    the key has been decoded, and on the controller after it has been exported.

    The exit path removes the copy in that window, and only in it: a run that
    finishes keeps the controller's export, which is the copy the operator
    carries.
    """

    handler = _shell_function(
        (FLEET / "install.sh").read_text(encoding="utf-8"),
        "forget_key_b64_on_exit",
    )

    def run_with(*, finished: str) -> Path:
        key = tmp_path / f"key-{finished}.b64"
        key.write_text("bXVuZ2U=\n", encoding="utf-8")
        program = "\n".join([
            f'KEY_B64="{key}"',
            "DRY_RUN=0",
            "KEY_B64_LEFT=1",
            f"RUN_FINISHED={finished}",
            handler,
            "trap forget_key_b64_on_exit EXIT",
            "exit 1",
        ])
        result = subprocess.run(
            ["bash", "-c", program], capture_output=True, text=True, check=False)
        assert result.stderr == "", result.stderr
        assert result.stdout == "", result.stdout
        return key

    assert not run_with(finished="0").exists(), (
        "a run that stopped partway left the fleet's key in a home directory"
    )
    assert run_with(finished="1").exists(), (
        "a finished run must keep the export the operator carries"
    )

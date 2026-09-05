"""A required remote operation that fails must not be reported as done.

Every step of the two transition scripts runs as a snippet inside another
bash, and three of those snippets ended in an `echo`.  A snippet's status is
its last command's, so `systemctl --user stop` failing, `systemctl --user
start` failing and `crontab <backup>` failing were each followed by a
successful `echo` and reported as success.  The outer scripts' `set -uo
pipefail` does not reach inside a snippet run by another shell, and it carries
no `errexit` to reach with.

What that costs is not a cosmetic log line.  A cutover whose step 4 could not
stop the legacy executor went on to publish the SLURM generation with that
executor still draining the pull queue, and a rollback whose crontab restore
or `pqwork` start failed printed `rollback complete` over a fleet with nothing
keeping a supervisor alive.

Every snippet here reaches the fake `ssh` from `test_slurm_install_scripts`
and the fakes it puts on `PATH`; no `systemctl`, `crontab` or box in this
fleet is touched.
"""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_slurm_install_scripts import (  # noqa: E402
    FAKE_BOX,
    FLEET,
    SUPERVISE_LINE,
    _cutover,
    _live_cutover,
)


def _write_fake_systemctl(
    tmp_path: Path, *, stop_exit: int = 0, start_exit: int = 0,
    after_stop: str = "inactive", after_start: str = "active",
) -> Path:
    """A ``systemctl --user`` whose stop, start and state are the test's.

    ``$1`` is ``--user`` and ``$2`` is the verb, which is how both scripts
    invoke it.  ``is-active`` answers differently before and after a stop,
    because that is the question the fix added: a unit that is still up after
    a successful-looking stop is not stopped.
    """

    fakes = tmp_path / "fakes"
    fakes.mkdir(exist_ok=True)
    script = fakes / "systemctl"
    script.write_text(
        "#!/bin/sh\n"
        f"echo \"systemctl $*\" >> '{tmp_path}/calls'\n"
        "case \"$2\" in\n"
        "    list-unit-files) exit 0 ;;\n"
        f"    stop) touch '{tmp_path}/stopped'; exit {stop_exit} ;;\n"
        f"    start) touch '{tmp_path}/started'; exit {start_exit} ;;\n"
        "    is-active)\n"
        f"        if [ -e '{tmp_path}/started' ]; then echo '{after_start}'\n"
        f"        elif [ -e '{tmp_path}/stopped' ]; then echo '{after_stop}'\n"
        "        else echo active; fi\n"
        "        exit 0 ;;\n"
        "esac\n"
        "exit 0\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    return script


def _calls(tmp_path: Path) -> str:
    log = tmp_path / "calls"
    return log.read_text(encoding="utf-8") if log.exists() else ""


# -- cutover step 4 ----------------------------------------------------------


def _cutover_with_a_spark(tmp_path: Path, **systemctl: object) -> dict[str, str]:
    """A live cutover that reaches step 4, which needs a Spark to reach."""

    environment = _live_cutover(
        tmp_path, crontab=SUPERVISE_LINE + "\n", publish_exit=0)
    # The fixture leaves this empty so an ordinary run reaches no Spark.  Step
    # 4 is the step under test, so this run needs one, and it is the same box
    # no box answers to.
    environment["PB_SPARKS"] = FAKE_BOX
    _write_fake_systemctl(tmp_path, **systemctl)   # type: ignore[arg-type]
    return environment


def test_a_failed_stop_stops_the_cutover_before_it_publishes(
    tmp_path: Path,
) -> None:
    """Step 5 is the step with no cheap retry, and it must not run behind a
    step 4 that left the legacy executor up."""

    environment = _cutover_with_a_spark(tmp_path, stop_exit=42)

    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stdout
    assert "step 4 could not stop pqwork.service" in result.stderr
    assert FAKE_BOX in result.stderr
    # The point of failing here: the generation was never published.
    assert "publish --default-transport slurm" not in _calls(tmp_path)
    assert "cutover complete" not in result.stdout


def test_a_unit_still_active_after_the_stop_stops_the_cutover(
    tmp_path: Path,
) -> None:
    """`stop` exiting 0 is not the claim; the unit being down is.

    The pre-fix snippet printed that state and returned 0 anyway, so the one
    line that could have said the executor was still running was also the line
    that reported success.
    """

    environment = _cutover_with_a_spark(tmp_path, after_stop="active")

    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stdout
    assert "step 4 could not stop pqwork.service" in result.stderr
    assert "still active after stop" in result.stdout + result.stderr
    assert "publish --default-transport slurm" not in _calls(tmp_path)


@pytest.mark.parametrize("state", ("inactive", "failed"))
def test_a_unit_that_is_down_lets_the_cutover_finish(
    tmp_path: Path, state: str
) -> None:
    """`is-active` prints `inactive` or `failed` for a unit that is not
    running, and both of those are stopped."""

    environment = _cutover_with_a_spark(tmp_path, after_stop=state)

    result = _cutover(environment, "--yes")

    assert result.returncode == 0, result.stderr
    assert f"pqwork.service {state}" in result.stdout
    assert "cutover complete" in result.stdout


# -- rollback steps 2 and 3 --------------------------------------------------


def _rollback_environment(
    tmp_path: Path, *, crontab_exit: int = 0, crontab_keeps_the_line: bool = True,
    **systemctl: object,
) -> dict[str, str]:
    """A live rollback whose every snippet reaches the fakes.

    The state file is the one ``cutover.sh`` writes, because that is what
    ``rollback.sh`` reads: the boxes and the Sparks come out of it, so the
    Spark under test is named there rather than in the environment.
    """

    environment = _live_cutover(
        tmp_path, crontab=SUPERVISE_LINE + "\n", publish_exit=0)
    state_dir = Path(environment["PB_STATE_DIR"])
    backup = state_dir / "crontab.pre-cutover"
    backup.write_text(SUPERVISE_LINE + "\n", encoding="utf-8")
    (state_dir / "cutover-1788600000.json").write_text(
        '{\n'
        ' "schema": "prismaquant.prismabuild.slurm_cutover.v1",\n'
        f' "boxes": "{FAKE_BOX}",\n'
        f' "sparks": "{FAKE_BOX}",\n'
        ' "previous_generation": "gen-old",\n'
        f' "crontab_backup": "{backup}"\n'
        '}\n',
        encoding="utf-8",
    )
    # Step 4 appends a supervisor's output to an absolute path, and the one
    # in the script is the fleet's real log.  Nothing here should reach step
    # 4, and a test that does must not write there.
    environment["PB_SUPERVISOR_LOG"] = str(tmp_path / "pb-supervisor.log")
    fakes = tmp_path / "fakes"
    live = tmp_path / "live-crontab"
    live.write_text("", encoding="utf-8")
    # A `crontab` whose install can refuse, and whose read-back can disagree
    # with the file it was given.  The second is the failure the pre-fix
    # snippet could not see at all: `crontab` exiting 0 having installed
    # nothing.
    (fakes / "crontab").write_text(
        "#!/bin/sh\n"
        f"echo \"crontab $*\" >> '{tmp_path}/calls'\n"
        "case \"$1\" in\n"
        f"    -l) cat '{live}' ;;\n"
        f"    -) cat > '{live}' ;;\n"
        f"    *) [ {crontab_exit} -eq 0 ] || exit {crontab_exit}\n"
        + (f"       cp \"$1\" '{live}' ;;\n" if crontab_keeps_the_line
           else f"       : > '{live}' ;;\n")
        + "esac\n"
        "exit 0\n",
        encoding="utf-8",
    )
    (fakes / "crontab").chmod(0o755)
    _write_fake_systemctl(tmp_path, **systemctl)   # type: ignore[arg-type]
    return environment


def _rollback(environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(FLEET / "rollback.sh")],
        capture_output=True, text=True, check=False, env=environment,
    )


def test_a_failed_crontab_install_stops_the_rollback(tmp_path: Path) -> None:
    """The supervise line is what keeps a supervisor alive, so a rollback that
    could not restore it has not restored the pull queue's plane."""

    environment = _rollback_environment(tmp_path, crontab_exit=42)

    result = _rollback(environment)

    assert result.returncode == 1, result.stdout
    assert "step 2 could not restore the crontab" in result.stderr
    assert FAKE_BOX in result.stderr
    assert "rollback complete" not in result.stdout
    # And it stopped there, rather than going on to the later steps.
    assert "step 3" not in result.stdout
    assert "systemctl --user start" not in _calls(tmp_path)


def test_a_crontab_that_installs_nothing_stops_the_rollback(
    tmp_path: Path,
) -> None:
    """`crontab <file>` exiting 0 is not the claim; the line being in the
    crontab afterwards is."""

    environment = _rollback_environment(
        tmp_path, crontab_keeps_the_line=False)

    result = _rollback(environment)

    assert result.returncode == 1, result.stdout
    assert "step 2 could not restore the crontab" in result.stderr
    assert "the supervise line is not in the crontab" in (
        result.stdout + result.stderr)


def test_a_failed_pqwork_start_stops_the_rollback(tmp_path: Path) -> None:
    """The unit is the Spark's half of the pull queue's execution plane."""

    environment = _rollback_environment(tmp_path, start_exit=42)

    result = _rollback(environment)

    assert result.returncode == 1, result.stdout
    assert "step 3 could not start pqwork.service" in result.stderr
    assert FAKE_BOX in result.stderr
    assert "rollback complete" not in result.stdout
    assert "step 4" not in result.stdout


@pytest.mark.parametrize("state", ("inactive", "failed"))
def test_a_unit_that_did_not_come_up_stops_the_rollback(
    tmp_path: Path, state: str
) -> None:
    """`start` exiting 0 while the unit is `failed` is the shape that reported
    `rollback complete` over a Spark with no executor."""

    environment = _rollback_environment(tmp_path, after_start=state)

    result = _rollback(environment)

    assert result.returncode == 1, result.stdout
    assert "step 3 could not start pqwork.service" in result.stderr
    assert f"pqwork.service is {state} after start" in (
        result.stdout + result.stderr)
    assert "rollback complete" not in result.stdout


def test_the_fakes_never_reached_a_real_command(tmp_path: Path) -> None:
    """Belt and braces: the fake ssh is what carried every snippet."""

    environment = _rollback_environment(tmp_path, crontab_exit=42)
    _rollback(environment)

    calls = _calls(tmp_path)
    assert f"ssh {FAKE_BOX} bash -s" in calls
    assert os.path.sep in environment["PB_SSH"]

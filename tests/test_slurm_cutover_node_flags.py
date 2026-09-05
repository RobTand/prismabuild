"""What a trailing ``sinfo`` state flag does to the cutover's last gate.

``sinfo -h -N -o '%N %T'`` reports a node as a base state plus zero or more
flag characters, and the flag is the part that says whether the controller can
currently reach the node: ``idle*`` is a node it is getting no response from,
``idle~`` one that is powered off.  The gate used to remove every trailing
flag character before reading the word, so both of those arrived at the branch
that accepts ``idle`` and the cutover proceeded to stop the pull queue's loops
on a fleet whose nodes could not run the work that replaced them.

Any flag now refuses, and the refusal quotes the state as the controller
reported it.  A flag is the controller saying something is happening to that
node, and this gate is asked in the moment before the only execution plane is
taken away, so the boxes it wants are the ones nothing is happening to.

Nothing here reaches a real controller: ``sinfo`` is the fake from
``test_slurm_install_scripts`` and no box in this fleet has SLURM installed.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_slurm_install_scripts import (  # noqa: E402
    FAKE_BOX,
    SUPERVISE_LINE,
    _cutover,
    _live_cutover,
    write_fake_sinfo,
)


def _healthy(tmp_path: Path) -> dict[str, str]:
    """A live cutover whose publication fails, so only the gates decide."""

    return _live_cutover(tmp_path, crontab=SUPERVISE_LINE + "\n", publish_exit=1)


def _state_files(environment: dict[str, str]) -> list[Path]:
    return sorted(Path(environment["PB_STATE_DIR"]).glob("cutover-*.json"))


#: The flag characters and the sentence each one earns in the refusal.
FLAGS = {
    "*": "getting no response",
    "~": "powered off",
    "#": "powering up",
    "!": "power-down is pending",
    "%": "powering down",
    "$": "maintenance flag",
    "@": "reboot is pending",
    "^": "reboot has been issued",
    "-": "backfill scheduler",
}


@pytest.mark.parametrize("base", ("idle", "mixed", "allocated"))
@pytest.mark.parametrize("flag", tuple(FLAGS))
def test_a_flagged_state_is_refused_naming_the_state_the_controller_reported(
    tmp_path: Path, base: str, flag: str
) -> None:
    """The base word is usable and the flag is why the node is not."""

    environment = _healthy(tmp_path)
    write_fake_sinfo(tmp_path, f"{FAKE_BOX} {base}{flag}")

    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stdout
    assert f"{FAKE_BOX}: {base}{flag}" in result.stderr
    assert FLAGS[flag] in result.stderr
    # Refused before the first irreversible act, which is the state file.
    assert _state_files(environment) == []
    assert not (Path(environment["PB_STATE_DIR"]) / "crontab.pre-cutover").exists()


def test_the_refusal_reads_out_every_flag_on_the_state(tmp_path: Path) -> None:
    """A state can carry more than one, and each one is a separate fact."""

    environment = _healthy(tmp_path)
    write_fake_sinfo(tmp_path, f"{FAKE_BOX} idle*~")

    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stdout
    assert f"{FAKE_BOX}: idle*~" in result.stderr
    assert "getting no response" in result.stderr
    assert "powered off" in result.stderr


def test_a_flagged_state_on_the_second_box_is_refused_too(tmp_path: Path) -> None:
    """One healthy box does not answer for the fleet."""

    environment = _healthy(tmp_path)
    second = f"{FAKE_BOX}-two"
    environment["PB_BOXES"] = f"{FAKE_BOX} {second}"
    write_fake_sinfo(tmp_path, f"{FAKE_BOX} idle\n{second} mixed*\n")

    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stdout
    assert f"{second}: mixed*" in result.stderr
    assert _state_files(environment) == []


@pytest.mark.parametrize("state", ("idle", "MIXED", "Allocated"))
def test_an_unflagged_usable_state_still_gets_past_the_gate(
    tmp_path: Path, state: str
) -> None:
    """Case is not a claim, and a node with nothing happening to it passes."""

    environment = _healthy(tmp_path)
    write_fake_sinfo(tmp_path, f"{FAKE_BOX} {state}")

    result = _cutover(environment, "--yes")

    assert f"# every one of {FAKE_BOX} is idle, mixed or allocated" in result.stdout
    # Past the gate means the state file rollback.sh reads was written.
    assert len(_state_files(environment)) == 1, result.stdout


def test_a_dry_run_reports_the_flag_and_still_refuses_nothing(tmp_path: Path) -> None:
    """``sinfo`` only reads, so the answer is worth having while choosing the
    window."""

    environment = _healthy(tmp_path)
    write_fake_sinfo(tmp_path, f"{FAKE_BOX} idle~")

    result = _cutover(environment, "--dry-run", "--yes")

    assert result.returncode == 0, result.stderr
    assert "a live run would refuse; these are not usable right now" in result.stdout
    assert f"{FAKE_BOX}: idle~" in result.stdout
    assert "powered off" in result.stdout
    assert "REFUSED" not in result.stderr

"""What ``fleet/slurm/cutover.sh`` establishes before it changes anything.

The cutover is the one command that takes the fleet's execution plane away
from the pull queue, and its first two acts -- the state file and each box's
crontab -- are the ones with no cheap undo.  Two of its questions used to be
answerable by things that are not answers.

The verification marker was accepted by existence.  ``verify.sh`` writes it
with the sha256 of the ``slurm.conf`` it verified against, so a marker left by
a run against an older configuration, or from weeks ago, or against a fleet
that has since gone down, satisfied a gate that only ran ``[ -f ]``.  The
marker is now read: the hash must be this checkout's, and the age is printed
rather than judged, because how old is too old is Rob's call and not the
script's.

And nothing asked the controller whether the nodes were up.  The marker is a
claim about a fleet that passed once; liveness is a claim about now, which is
why ``--verified`` does not skip it either.  A cutover onto a drained node
stops the loops that were the only thing still running work.

Every test here fails against the pre-fix ``cutover.sh``.  The fixtures
come from ``test_slurm_install_scripts``, which owns the fake ``ssh``,
``pgrep``, ``crontab``, ``sinfo`` and publish stubs; nothing in this file
reaches a real box, and none of these boxes has SLURM installed anyway.
"""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_slurm_install_scripts import (  # noqa: E402
    FAKE_BOX,
    SUPERVISE_LINE,
    _cutover,
    _live_cutover,
    write_fake_sinfo,
    write_verify_marker,
)


def _calls(tmp_path: Path) -> str:
    """The fakes' log, or nothing when a refusal fired before any fake ran."""

    log = tmp_path / "calls"
    return log.read_text(encoding="utf-8") if log.exists() else ""


def _state_files(environment: dict[str, str]) -> list[Path]:
    return sorted(Path(environment["PB_STATE_DIR"]).glob("cutover-*.json"))


def _crontab_backup(environment: dict[str, str]) -> Path:
    return Path(environment["PB_STATE_DIR"]) / "crontab.pre-cutover"


def _nothing_was_changed(environment: dict[str, str]) -> None:
    """No state file, no crontab backup: the refusal fired before step 1."""

    assert _state_files(environment) == []
    assert not _crontab_backup(environment).exists()


def _healthy(tmp_path: Path) -> dict[str, str]:
    """A live cutover whose publication fails, so only the gates decide.

    ``publish_exit=1`` means a run that gets all the way through the refusals
    still exits 1 -- which is why no test here reads the exit code alone.
    """

    return _live_cutover(tmp_path, crontab=SUPERVISE_LINE + "\n", publish_exit=1)


# -- the marker is read, not counted -----------------------------------------


def test_a_marker_for_another_slurm_conf_is_refused(tmp_path: Path) -> None:
    """The hash is the whole point of the marker carrying one.

    A marker written against a ``slurm.conf`` this checkout no longer contains
    verified a different fleet than the one the cutover would produce.  Before
    the gate read it, that marker was indistinguishable from one written a
    minute ago against this file.
    """

    environment = _healthy(tmp_path)
    write_verify_marker(environment, slurm_conf_sha256="a" * 64)

    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stdout
    assert "verify.sh" in result.stderr
    assert "verified a different slurm.conf" in result.stderr
    assert "a" * 64 in result.stderr
    _nothing_was_changed(environment)
    # The refusal is the first thing in the live block, so no fake ran.
    assert "crontab" not in _calls(tmp_path)


def test_a_marker_with_no_verified_unix_is_refused(tmp_path: Path) -> None:
    """A marker whose age cannot be read is not evidence about this fleet."""

    environment = _healthy(tmp_path)
    write_verify_marker(environment, verified_unix=None)

    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stdout
    assert "verified_unix" in result.stderr
    assert "verify.sh" in result.stderr
    _nothing_was_changed(environment)


def test_a_marker_with_no_hash_is_refused(tmp_path: Path) -> None:
    environment = _healthy(tmp_path)
    write_verify_marker(environment, slurm_conf_sha256=None)

    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stdout
    assert "slurm_conf_sha256" in result.stderr
    assert "verify.sh" in result.stderr
    _nothing_was_changed(environment)


def test_a_good_marker_reports_its_age_and_where_it_came_from(
    tmp_path: Path,
) -> None:
    """Reported, never a refusal: Rob decides what too old means.

    The commit line is the same kind of statement -- a marker written at
    another commit is worth knowing about and is not by itself wrong.
    """

    environment = _healthy(tmp_path)
    marker = json.loads(write_verify_marker(environment).read_text(encoding="utf-8"))

    result = _cutover(environment, "--yes")

    assert f"verified 3h 12m ago on {marker['host']}" in result.stdout
    assert marker["commit"] in result.stdout
    assert "not judged" in result.stdout
    # It got past the marker, so the refusal it did hit is the publication's.
    assert "publication failed" in result.stderr


# -- and the controller is asked whether the fleet is up ---------------------


@pytest.mark.parametrize("state", ("down*", "drained", "DOWN"))
def test_a_node_that_is_not_up_is_refused_before_anything_changes(
    tmp_path: Path, state: str
) -> None:
    """Step 3 stops the loops that are the fleet's only execution plane until
    SLURM takes over.  A node the controller will not schedule onto is a box
    that runs nothing afterwards, and the marker cannot know that: it says a
    fleet passed once, not that it is up now."""

    environment = _healthy(tmp_path)
    write_fake_sinfo(tmp_path, f"{FAKE_BOX} {state}")

    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stdout
    assert FAKE_BOX in result.stderr
    assert state.lower().rstrip("*") in result.stderr
    assert "scontrol update NodeName=" in result.stderr
    _nothing_was_changed(environment)
    assert "crontab" not in _calls(tmp_path)


def test_a_box_the_controller_does_not_report_at_all_is_refused(
    tmp_path: Path,
) -> None:
    """A node that never registered is absent from ``sinfo -N``, not down."""

    environment = _healthy(tmp_path)
    write_fake_sinfo(tmp_path, "some-other-node idle")

    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stdout
    assert f"{FAKE_BOX}: not reported by sinfo" in result.stderr
    _nothing_was_changed(environment)


def test_verified_does_not_skip_the_liveness_gate(tmp_path: Path) -> None:
    """``--verified`` says the verification happened on another box.  It says
    nothing about whether the fleet is up now, which is the question."""

    environment = _healthy(tmp_path)
    write_fake_sinfo(tmp_path, f"{FAKE_BOX} down*")

    result = _cutover(environment, "--yes", "--verified")

    assert result.returncode == 1, result.stdout
    assert FAKE_BOX in result.stderr
    _nothing_was_changed(environment)


@pytest.mark.skipif(
    shutil.which("sinfo") is not None,
    reason="the refusal under test is what happens when sinfo is not installed",
)
def test_no_sinfo_at_all_is_refused(tmp_path: Path) -> None:
    """SLURM is installed on no box in this fleet, so this is also the state
    the gate meets today: a controller that cannot be asked is a fleet whose
    execution plane would be gone the moment step 1 runs."""

    environment = _healthy(tmp_path)
    (tmp_path / "fakes" / "sinfo").unlink()

    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stdout
    assert "SLURM is not installed" in result.stderr
    assert "slurmctld is unreachable" in result.stderr
    assert "verify.sh" in result.stderr
    _nothing_was_changed(environment)


def test_usable_states_carrying_flags_get_past_the_gate(tmp_path: Path) -> None:
    """``sinfo -N`` prints one line per node per partition, and a state comes
    with flags: ``idle*`` is a node the controller cannot reach right now,
    ``mixed~`` one that is powered down.  The base word is what decides, the
    same node may be reported more than once, and case is not a claim.
    """

    environment = _healthy(tmp_path)
    second = f"{FAKE_BOX}-two"
    environment["PB_BOXES"] = f"{FAKE_BOX} {second}"
    write_fake_sinfo(
        tmp_path,
        f"{FAKE_BOX} IDLE\n{second} Mixed~\n{FAKE_BOX} allocated\n",
    )

    result = _cutover(environment, "--yes")

    assert f"# every one of {FAKE_BOX} {second} is idle, mixed or allocated" \
        in result.stdout
    assert "sinfo -h -N" in _calls(tmp_path)
    # Past the gate means the state file rollback.sh reads was written.
    assert len(_state_files(environment)) == 1, result.stdout


def test_a_dry_run_answers_the_liveness_question_without_refusing(
    tmp_path: Path,
) -> None:
    """``sinfo`` only reads, so the answer is worth having while choosing the
    window.  A dry run refuses nothing, including this."""

    environment = _healthy(tmp_path)
    write_fake_sinfo(tmp_path, f"{FAKE_BOX} down*")

    result = _cutover(environment, "--dry-run", "--yes")

    assert result.returncode == 0, result.stderr
    assert "a live run refuses unless all six of these hold" in result.stdout
    assert "a live run would refuse; these are not usable right now" in result.stdout
    assert f"{FAKE_BOX}: down" in result.stdout
    assert "sinfo -h -N" in _calls(tmp_path)
    assert "REFUSED" not in result.stderr

"""An empty endings table has to say which kind of empty it is.

``pbstatus`` printed one sentence for three different situations: a fleet that
has filed nothing yet, a ``--queue-root`` that does not exist, and a path that
exists but is not a queue.  The three call for opposite responses, and the one
the operator reads out of "no endings filed" is the first, so a mistyped root
reads as work that has not landed yet and is waited for rather than looked at.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402

import pbstatus  # noqa: E402


def _no_controller(tmp_path: Path) -> list[str]:
    """Argv naming three scheduler binaries that are not there.

    The tool shells out to ``sinfo``, ``squeue`` and ``scontrol``, and leaving
    them at their defaults makes these cases read whatever is installed on the
    box running them.  SLURM is installed on no box today, so the default is
    fast and deterministic by accident; the pre-built debs are waiting to make
    it neither.  Naming absent binaries pins the endings half of the screen,
    which is the half under test.
    """

    absent = str(tmp_path / "absent")
    return ["--sinfo", absent, "--squeue", absent, "--scontrol", absent]


def _queue(root: Path) -> Path:
    """A queue root with the three ending directories and nothing in them."""

    for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
        (root / state).mkdir(parents=True)
    return root


def test_a_queue_root_that_does_not_exist_is_named_as_missing(
    tmp_path: Path, capsys
) -> None:
    missing = tmp_path / "pb-queue"

    assert pbstatus.queue_root_note(missing) == (
        f"no queue root at {missing}: the path does not exist")

    # And the screen says it where the empty table would have been.
    assert pbstatus.main(
        ["--queue-root", str(missing)] + _no_controller(tmp_path)) == 0
    out = capsys.readouterr().out
    assert str(missing) in out
    assert "does not exist" in out
    assert "no endings filed" not in out


def test_a_path_that_is_not_a_queue_is_named_as_not_a_queue(
    tmp_path: Path
) -> None:
    """A directory with none of the three ending directories files nothing.

    This is the shape a root one level too high has, and it is the reading an
    operator is least likely to reach unaided.
    """

    elsewhere = tmp_path / "prismabuild-fleet"
    elsewhere.mkdir()

    note = pbstatus.queue_root_note(elsewhere)
    assert note is not None
    assert str(elsewhere) in note
    assert "not a queue root" in note

    a_file = tmp_path / "pb-queue.json"
    a_file.write_text("{}", encoding="utf-8")
    file_note = pbstatus.queue_root_note(a_file)
    assert file_note is not None
    assert "not a directory" in file_note


def test_a_real_queue_with_nothing_in_it_still_says_no_endings_filed(
    tmp_path: Path, capsys
) -> None:
    """The healthy case keeps its own answer, which is the point of the split."""

    queue = _queue(tmp_path / "pb-queue")

    assert pbstatus.queue_root_note(queue) is None

    assert pbstatus.main(
        ["--queue-root", str(queue)] + _no_controller(tmp_path)) == 0
    assert "no endings filed under done/, failed/ or withdrawn/" in (
        capsys.readouterr().out)


def test_the_json_screen_carries_the_same_diagnosis(
    tmp_path: Path, capsys
) -> None:
    """``--json`` is what a wrapper reads, and it printed an empty list only."""

    missing = tmp_path / "pb-queue"
    assert pbstatus.main(
        ["--queue-root", str(missing), "--json"] + _no_controller(tmp_path)
    ) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["endings"] == []
    assert any("does not exist" in note for note in payload["scheduler"])

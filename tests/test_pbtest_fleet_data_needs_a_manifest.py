"""A test that reads fleet data runs under ``pbtest`` only with a data manifest (#915).

Some suites read the shared mount without declaring it, so the fleet can
neither see the read nor stage it.  ``pbtest`` now takes ``--data-manifest``
(and ``--residency``), forwards them to every shard's ``pbrun`` only when given,
and refuses a run that includes a file marked ``fleet_data`` when no manifest
is given.
"""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

from test_pbtest_seals_its_shard_deadline import (  # noqa: E402
    _FinishedProcess, pbtest,
)

MARKED = (
    "import pytest\n\n"
    "@pytest.mark.fleet_data\n"
    "def test_reads_a_declared_file():\n"
    "    assert True\n"
)


def _run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra, *, marked=True):
    """pbtest's exit code and every shard's ``pbrun`` argv."""

    checkout = tmp_path / "checkout"
    (checkout / "tests").mkdir(parents=True, exist_ok=True)
    (checkout / "tests" / "test_plain.py").write_text(
        "def test_one():\n    assert True\n", encoding="utf-8")
    if marked:
        (checkout / "tests" / "test_reads_fleet_data.py").write_text(
            MARKED, encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    calls: list[list[str]] = []

    def _popen(command, **_kwargs):
        calls.append(list(command))
        return _FinishedProcess(command)

    monkeypatch.setattr(pbtest.subprocess, "Popen", _popen)
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
         "--shards", "2", *extra, "tests"],
    )
    return pbtest.main(), calls


def _manifest(tmp_path: Path) -> Path:
    path = tmp_path / "manifest.json"
    path.write_text("{}", encoding="utf-8")
    return path


def test_a_marked_file_without_a_manifest_is_refused_before_anything_is_submitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """main: a fleet-data file runs with no declared data."""

    code, calls = _run(tmp_path, monkeypatch, [])

    assert code == 2
    assert calls == []
    err = capsys.readouterr().err
    assert "test_reads_fleet_data.py" in err
    assert "--data-manifest" in err


def test_a_marked_file_with_a_manifest_runs_and_every_shard_forwards_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """main: the manifest reaches each shard's pbrun, with the residency mode."""

    manifest = _manifest(tmp_path)

    code, calls = _run(
        tmp_path, monkeypatch,
        ["--data-manifest", str(manifest), "--residency", "stage"])

    assert code == 0
    assert calls
    for command in calls:
        assert command[command.index("--data-manifest") + 1] == str(manifest.resolve())
        assert command[command.index("--residency") + 1] == "stage"


def test_without_the_flags_no_shard_carries_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """main: an unmarked run keeps its argv, so its action keys do not move."""

    code, calls = _run(tmp_path, monkeypatch, [], marked=False)

    assert code == 0
    assert calls
    for command in calls:
        assert "--data-manifest" not in command
        assert "--residency" not in command


def test_stage_residency_without_a_manifest_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """main: ``--residency stage`` has nothing to stage."""

    code, calls = _run(tmp_path, monkeypatch, ["--residency", "stage"], marked=False)

    assert code == 2
    assert calls == []
    assert "--data-manifest" in capsys.readouterr().err


def test_fleet_data_files_finds_both_spellings(tmp_path: Path) -> None:
    """fleet_data_files: a decorator and a module ``pytestmark`` both count."""

    (tmp_path / "a.py").write_text(
        "import pytest\npytestmark = pytest.mark.fleet_data\n", encoding="utf-8")
    (tmp_path / "b.py").write_text(MARKED, encoding="utf-8")
    (tmp_path / "c.py").write_text("def test_x():\n    pass\n", encoding="utf-8")

    assert pbtest.fleet_data_files(tmp_path, ["a.py", "b.py", "c.py"]) == ["a.py", "b.py"]

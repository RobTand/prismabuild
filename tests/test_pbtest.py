"""The suite dispatcher relies on pbrun's immutable checkout transport."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pbtest", ROOT / "tools" / "fleet" / "pbtest.py"
)
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)  # type: ignore[union-attr]


def test_box_local_git_checkout_is_dispatched_without_leaking_its_path(
    tmp_path: Path, monkeypatch,
) -> None:
    """A local source tree is portable; only pbrun's --cwd may name it."""

    checkout = tmp_path / "checkout"
    test_file = checkout / "tests" / "test_one.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("def test_one():\n    assert True\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    calls: list[list[str]] = []

    class FinishedProcess:
        returncode = 0

        def communicate(self):
            return "1 passed in 0.01s\n", None

    def popen(command, **_kwargs):
        calls.append(command)
        return FinishedProcess()

    monkeypatch.setattr(pbtest.subprocess, "Popen", popen)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pbtest.py", "--checkout", str(checkout),
            "--python", "/target/python", "--shards", "1", "tests",
        ],
    )

    assert pbtest.main() == 0
    assert len(calls) == 1
    command = calls[0]
    separator = command.index("--")
    assert command[command.index("--cwd") + 1] == str(checkout.resolve())
    assert str(checkout.resolve()) not in " ".join(command[separator + 1:])
    assert "PYTHONPATH=src:experiments" in command

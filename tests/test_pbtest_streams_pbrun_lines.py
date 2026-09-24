"""A shard's ``pbrun:`` lines reach the operator while the shard runs (#1048).

#1033 lets a patient ``pbrun`` wait out a reader stuck in the kernel for as
long as ``--wait-s`` allows, and says so every minute. ``pbtest`` read each
shard only at ``communicate()``, one shard after another, so those lines sat
in a pipe: a 20-shard run on a mount that never recovered printed nothing for
10,800 s. A pipe nobody drains also fills, and then ``pbrun`` blocks in
``write(2)``. Each shard is now drained as it runs, and its ``pbrun:`` lines
are printed at once.
"""
from __future__ import annotations

import importlib.util
import io
from pathlib import Path
import subprocess
import sys
import threading

import pytest

from pbtest_shard_output import ONE_PASS


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pbtest", ROOT / "tools" / "fleet" / "pbtest.py"
)
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)  # type: ignore[union-attr]

REAL_POPEN = subprocess.Popen

NOTICE = ("pbrun: pool outcome for 13e6c8621baf not observed yet: its reader "
          "is still retained after 60s")


class _Stream:
    """A shard's stdout that checks, before the shard ends, what was printed."""

    def __init__(self, sink: io.StringIO) -> None:
        self.lines = ["pbrun: queued 13e6c8621baf tags=['x86']\n", NOTICE + "\n",
                      *ONE_PASS.splitlines(keepends=True)]
        self.sink = sink
        self.seen_before_end: bool | None = None

    def readline(self) -> str:
        if not self.lines:
            # The shard is about to end: its notice must already be on screen.
            self.seen_before_end = NOTICE in self.sink.getvalue()
            return ""
        return self.lines.pop(0)

    def __iter__(self):
        return iter(self.readline, "")

    def close(self) -> None:
        pass


def test_pbrun_lines_are_printed_while_the_shard_is_still_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout = tmp_path / "checkout"
    (checkout / "tests").mkdir(parents=True)
    (checkout / "tests" / "test_one.py").write_text("def test_one():\n    pass\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    sink = io.StringIO()
    streams: list[_Stream] = []

    class Shard:
        returncode = 0

        def __init__(self) -> None:
            self.stdout = _Stream(sink)
            streams.append(self.stdout)

        def communicate(self):
            # The old reader: everything at once, after the shard ended.
            self.stdout.seen_before_end = False
            return "".join(self.stdout.lines), None

        def wait(self, timeout=None):
            return self.returncode

    def popen(command, **kwargs):
        if str(pbtest.PBRUN) in [str(part) for part in command]:
            return Shard()
        return REAL_POPEN(command, **kwargs)

    monkeypatch.setattr(pbtest.subprocess, "Popen", popen)
    monkeypatch.setattr(sys, "stdout", sink)
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
        "--shards", "1", "tests"])
    assert pbtest.main() == 0
    assert [stream.seen_before_end for stream in streams] == [True]
    printed = sink.getvalue().splitlines()
    assert f"shard   0 {NOTICE}" in printed
    assert any(line.startswith("shard   0 pbrun: queued 13e6c8621baf")
               for line in printed)
    # The shard's own pytest lines stay in its result, not in the stream.
    assert not any(line.startswith("shard   0 1 passed") for line in printed)


def test_every_shard_is_drained_at_once_not_in_turn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later shard's pipe is read while an earlier shard still runs."""

    checkout = tmp_path / "checkout"
    (checkout / "tests").mkdir(parents=True)
    for name in ("a", "b"):
        (checkout / "tests" / f"test_{name}.py").write_text("def test_one():\n    pass\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    second_read = threading.Event()

    class Blocking(io.StringIO):
        def readline(self, *args):
            if not second_read.wait(timeout=10):
                raise AssertionError("shard 1 was not drained while shard 0 ran")
            return super().readline(*args)

    class Signalling(io.StringIO):
        def readline(self, *args):
            second_read.set()
            return super().readline(*args)

    made = []

    class Shard:
        returncode = 0

        def __init__(self, command) -> None:
            files = [part for part in command if str(part).endswith(".py")
                     and str(part).startswith("tests/")]
            text = pbtest_shard_output.shard_output_for(command)
            self.stdout = (Blocking if not made else Signalling)(text)
            made.append(files)

        def communicate(self):
            # The old reader read this pipe too, one shard after another.
            return "".join(iter(self.stdout.readline, "")), None

        def wait(self, timeout=None):
            return self.returncode

    import pbtest_shard_output

    def popen(command, **kwargs):
        if str(pbtest.PBRUN) in [str(part) for part in command]:
            return Shard(command)
        return REAL_POPEN(command, **kwargs)

    monkeypatch.setattr(pbtest.subprocess, "Popen", popen)
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
        "--shards", "2", "tests"])
    assert pbtest.main() == 0
    assert len(made) == 2

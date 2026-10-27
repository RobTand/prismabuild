"""Private procfs, candidate and signal fixtures for supervisor tests."""
from __future__ import annotations

from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest
import supervise

HOST = "dl380g10"


def write_process(proc: Path, pid: int, argv: list[str], *,
                  mark: str | None = HOST, children: list[int] | None = None) -> int:
    """Write the ``/proc`` bytes one candidate process would have."""

    directory = proc / str(pid)
    directory.mkdir()
    (directory / "cmdline").write_bytes(
        b"".join(part.encode() + b"\0" for part in argv))
    entries = ["HOME=/home/rob", "PATH=/usr/bin:/bin"]
    if mark is not None:
        entries.append(f"{supervise.OWNERSHIP_ENV}={mark}")
    (directory / "environ").write_bytes(
        b"".join(entry.encode() + b"\0" for entry in entries))
    (directory / "stat").write_text(f"{pid} (python) S 1 {pid} {pid}\n")
    task = directory / "task" / str(pid)
    task.mkdir(parents=True)
    (task / "children").write_text(
        " ".join(str(child) for child in (children or [])))
    return pid


@pytest.fixture()
def candidates(monkeypatch: pytest.MonkeyPatch):
    """Stand in for ``pgrep``, per script name, the way the real one answers."""

    offered: dict[str, list[int]] = {}

    def fake_run(argv, *_args, **_kwargs):
        assert argv[0] == "pgrep", argv
        return subprocess.CompletedProcess(
            argv, 0, "\n".join(str(pid) for pid in offered.get(argv[-1], [])), "")

    monkeypatch.setattr(supervise.subprocess, "run", fake_run)
    return offered


@pytest.fixture()
def signals(monkeypatch: pytest.MonkeyPatch):
    """Every signal the supervisor would have sent, sent to nothing.

    ``_is_idle`` is faked the way the rest of the supervisor suite fakes it:
    it reads ``/proc/<pid>/task/*/children`` at the real ``/proc``, which a
    fake tree cannot answer.  What these tests are for is which pids reach
    that question, not what it answers -- ``busy`` is how a test says the
    answer is "no" for one of them.
    """

    box = SimpleNamespace(sent=[], busy=set())
    monkeypatch.setattr(supervise.os, "kill",
                        lambda pid, sig: box.sent.append((pid, int(sig))))
    monkeypatch.setattr(supervise, "_is_idle",
                        lambda pid, *_a: pid not in box.busy)
    # An empty holder set rather than ``None``: ``None`` means "the queue
    # could not be read", which licenses no signal at all and would hide the
    # defect behind a refusal that is right for another reason.
    monkeypatch.setattr(supervise, "_claim_holders", lambda: frozenset())
    return box

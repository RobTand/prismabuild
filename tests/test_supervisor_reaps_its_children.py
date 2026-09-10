"""The supervisor collects its own children, including across a re-exec.

``_spawn`` builds a ``subprocess.Popen`` and keeps only its pid, so nothing in
``supervise.py`` ever waits on a worker loop.  Inside one process image that is
invisible: CPython files a dropped handle into ``subprocess._active`` and polls
that list at the top of the next ``Popen.__init__``, which ``_live_loops``
performs once a tick for ``pgrep``.

``_reexec_if_published`` is what turns the omission into a leak.  ``os.execve``
replaces the image to adopt a published generation; the kernel keeps the child
list and ``_active`` does not survive, so every loop alive at that moment can
no longer be reaped by the subprocess module.  This file reproduces that state
directly -- clearing ``_active`` is what the exec does to this process -- and
checks that the tick-top drain collects what the back-channel cannot.
"""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools" / "fleet"
sys.path.insert(0, str(TOOLS))

import supervise  # noqa: E402


STRANDED = 5

# Run in a child interpreter rather than in the test process.  ``_reap`` waits
# on ``-1``, so it collects any child of whatever process calls it, and the
# test runner is entitled to children of its own.
SCENARIO = '''
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, sys.argv[1])
import supervise

STRANDED = %d


def zombies(pids):
    """Which of ``pids`` the kernel is holding an uncollected status for."""
    held = []
    for pid in pids:
        try:
            stat = Path("/proc/%%d/stat" %% pid).read_text()
        except OSError:
            continue
        # ``comm`` can contain spaces and parentheses; state is the field
        # after the last ``)``.
        if stat.rsplit(")", 1)[1].split()[0] == "Z":
            held.append(pid)
    return held


# The shape ``_spawn`` uses: construct it, keep the pid, drop the handle.  The
# child outlives the statement, so ``Popen.__del__`` files it in ``_active``.
spawned = [subprocess.Popen(
    [sys.executable, "-c", "import time; time.sleep(0.3)"]).pid
    for _ in range(STRANDED)]

deadline = time.monotonic() + 60
while time.monotonic() < deadline and len(zombies(spawned)) < STRANDED:
    time.sleep(0.05)
print("exited", len(zombies(spawned)))

# What ``os.execve`` does to this process.  The kernel still has the children.
subprocess._active.clear()

# The only thing that would have reaped them, and the one the supervisor
# performs every tick.
subprocess.run([sys.executable, "-c", ""], check=True)
print("after_cleanup", len(zombies(spawned)))

print("reaped", supervise._reap())
print("after_reap", len(zombies(spawned)))
print("second_call", supervise._reap())
''' % STRANDED


@pytest.mark.skipif(not Path("/proc/self/stat").exists(),
                    reason="the zombie state is read out of /proc")
def test_children_stranded_by_a_re_exec_are_reaped_at_the_next_tick():
    proc = subprocess.run([sys.executable, "-c", SCENARIO, str(TOOLS)],
                          capture_output=True, text=True, timeout=180)
    assert proc.returncode == 0, proc.stderr
    seen = dict(line.split() for line in proc.stdout.split("\n") if line)

    # The premise: the children really did exit and really were held.
    assert seen["exited"] == str(STRANDED), proc.stdout

    # The regression.  ``_active`` is what the next ``Popen`` polls, and the
    # exec took it, so the tick's own ``pgrep`` reaps nothing.
    assert seen["after_cleanup"] == str(STRANDED), proc.stdout

    # The fix asks the kernel instead, so the exec does not reach it.
    assert seen["reaped"] == str(STRANDED), proc.stdout
    assert seen["after_reap"] == "0", proc.stdout

    # And it is not a one-shot: a drain with nothing to collect is quiet
    # rather than an error, which is what every tick after the first sees.
    assert seen["second_call"] == "0", proc.stdout


def test_a_supervisor_with_no_children_drains_to_zero():
    """``waitpid`` raises ``ECHILD`` with no children at all; that is not news."""
    proc = subprocess.run(
        [sys.executable, "-c",
         "import sys; sys.path.insert(0, sys.argv[1]); "
         "import supervise; print(supervise._reap())", str(TOOLS)],
        capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "0"


def test_the_drain_is_bounded(monkeypatch):
    """The loop cannot spin on a box whose children keep exiting under it."""
    served = iter(range(1, supervise.REAP_BUDGET * 2))
    monkeypatch.setattr(supervise.os, "waitpid",
                        lambda pid, flags: (next(served), 0))
    assert supervise._reap() == supervise.REAP_BUDGET


def test_the_supervisor_installs_no_signal_handler():
    """The tick-top drain is the whole mechanism, deliberately.

    ``signal.signal(SIGCHLD, SIG_IGN)`` was the obvious alternative: it
    survives ``execve`` too, so it would clear the same backlog.  It is
    rejected because it does not stop at this process.  A ``SIG_IGN``
    disposition is inherited across ``exec`` by children, and ``_spawn`` execs
    a worker loop which execs an action, so an ignored ``SIGCHLD`` reaches the
    ``communicate()`` in ``pool.py`` that reads an action's exit status.  Every
    ``waitpid`` in that chain answers ``ECHILD``, and ``subprocess`` turns that
    into returncode 0: a failing action reporting success.  Measured on sparky,
    python 3.12.3, a grandchild running ``false`` reports 0 rather than 1.

    So the assertion is on the disposition, not on the drain: this process must
    leave ``SIGCHLD`` alone for the loops it spawns to inherit an honest one.
    """
    import ast
    import signal

    assert signal.getsignal(signal.SIGCHLD) is signal.SIG_DFL

    # Importing the module is not the only way it could install one, so read
    # the file for the call rather than for the words: the docstring above
    # names ``signal.signal`` on purpose, and a text search would catch that.
    tree = ast.parse((TOOLS / "supervise.py").read_text())
    installs = [node for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "signal"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "signal"]
    assert installs == [], [node.lineno for node in installs]

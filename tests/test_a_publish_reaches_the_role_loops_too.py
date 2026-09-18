"""A publish has to reach the singleton roles, not only the worker loops (#615).

After the 10:31Z publish on dl380g10 every ``worker_loop.py`` and the
``prewarm_loop.py`` role ran the new generation and ``tier_loop.py`` (pid
2138405) still ran one from two publishes earlier.  The consumer half of that
is issue #615's 25 idle minutes; the cause is two gaps that look like one.

**The loops move themselves.**  ``worker_loop`` and ``prewarm_loop`` each read
the live generation at the top of every poll and exit when it has moved, and
the installed unit runs ``supervise.py --ensure --systemd`` -- no
``--cycle-stale``.  So nothing cycled the workers either: they cycled
themselves, and ``tier_loop`` was the one loop with no such check.  That is
the first test below.

**And ``cycle_stale`` cannot reach a role.**  ``--cycle-stale`` is the
operator's verb for the generation that predates the check
(``docs/slurm_runbook_2026-09-04.md``), and it walks ``_live_loops()``, which
is worker loops only; ``_stop_idle_loops`` then re-proves ownership against
``LOOP_SCRIPT``, so a role pid handed to it is dropped a second time.  The
rest of the tests are that verb.

A role is a singleton, so unlike a pooled worker it is cycled only when its
own generation differs from the published one: stopping a live-generation
prewarm mid-copy would cost a cycle of storage service for nothing.

Nothing here reads a real process, sends a real signal or spawns anything: the
candidate list, the ``/proc`` bytes, ``os.kill`` and ``Popen`` are all this
test's own.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import supervise  # noqa: E402
import tier_loop  # noqa: E402

HOST = "dl380g10"
#: Far outside any pid this box could hand out, so a mistake cannot land on a
#: real process even if a patch were to slip.
STALE_ROLE = 1000000021
LIVE_ROLE = 1000000022
WORKER = 1000000023
BUSY_ROLE = 1000000024


@pytest.fixture()
def fleet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A fake mount: two published generations, the live link, a fake ``/proc``."""

    mirror = tmp_path / "fleet"
    store = mirror / "runtime-generations"
    for name in ("gen-old", "gen-live"):
        generation = store / name
        (generation / "tools").mkdir(parents=True)
        for script in ("worker_loop.py", "prewarm_loop.py", "tier_loop.py"):
            (generation / "tools" / script).write_text("# a loop\n")
        (generation / "RUNTIME_VERSION.json").write_text(
            json.dumps({"commit": name}))
    (mirror / "repo").symlink_to(store / "gen-live")

    proc = tmp_path / "proc"
    proc.mkdir()
    monkeypatch.setattr(supervise, "MIRROR", mirror)
    monkeypatch.setattr(supervise, "PROC", proc)
    monkeypatch.setattr(supervise, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(supervise, "CONFIG", tmp_path / "fleet_boxes.json")
    monkeypatch.setattr(supervise.socket, "gethostname", lambda: HOST)
    (tmp_path / "fleet_boxes.json").write_text(json.dumps({
        HOST: {"loops": 1, "args": ["--class", "x86"],
               "roles": {"tiers": ["--source-pool", "storage_pool"]}}}))
    return mirror, store, proc


def _process(proc: Path, pid: int, argv: list[str], *,
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
    """Every signal the supervisor would have sent, sent to nothing."""

    sent: list[tuple[int, int]] = []
    monkeypatch.setattr(supervise.os, "kill",
                        lambda pid, sig: sent.append((pid, int(sig))))
    # The queue is not on this box, so nothing can be read from it.  An empty
    # holder set rather than ``None``: ``None`` means "unknown", which is the
    # answer that licenses no signal at all and would hide the defect.
    monkeypatch.setattr(supervise, "_claim_holders", lambda: frozenset())
    return sent


# -- the loop that moves itself ---------------------------------------------


def test_the_tier_loop_exits_when_the_published_runtime_moves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The check ``worker_loop`` and ``prewarm_loop`` carry, and this one did not.

    Without it the loop holds the modules it imported for its whole life, and
    #609's new ``demand_source`` key is a plan its ``validate_plan`` refuses --
    so the fix that shipped reaches the box and not the reader.
    """

    loaded = tmp_path / "gen-live" / "RUNTIME_VERSION.json"
    loaded.parent.mkdir(parents=True)
    loaded.write_text(json.dumps({"commit": "a078cababe0d"}))
    live = tmp_path / "repo" / "RUNTIME_VERSION.json"
    live.parent.mkdir(parents=True)
    live.write_text(json.dumps({"commit": "6bdd75d7b738"}))
    monkeypatch.setattr(tier_loop.runtime_gate, "GENERATION_VERSION", loaded)
    monkeypatch.setattr(tier_loop.runtime_gate, "RUNTIME_VERSION", live)

    def refuse(*_args, **_kwargs):
        raise AssertionError("a moved runtime must be read before the cycle")

    monkeypatch.setattr(tier_loop, "cycle", refuse)

    assert tier_loop.main([
        "--pool-root", str(tmp_path / "pb-queue"), "--once"]) == 75


def test_the_tier_loop_runs_its_cycle_while_the_runtime_stands_still(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: the check must not park a loop whose generation is live."""

    version = tmp_path / "gen-live" / "RUNTIME_VERSION.json"
    version.parent.mkdir(parents=True)
    version.write_text(json.dumps({"commit": "6bdd75d7b738"}))
    monkeypatch.setattr(tier_loop.runtime_gate, "GENERATION_VERSION", version)
    monkeypatch.setattr(tier_loop.runtime_gate, "RUNTIME_VERSION", version)
    ran: list[bool] = []
    monkeypatch.setattr(tier_loop, "cycle",
                        lambda *_a, **_k: ran.append(True) or [])

    assert tier_loop.main([
        "--pool-root", str(tmp_path / "pb-queue"), "--once"]) == 0
    assert ran == [True]


# -- the operator's verb ----------------------------------------------------


def test_a_stale_role_loop_is_cycled_so_ensure_roles_respawns_it(
    fleet, candidates, signals, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The issue's own reproduction, in the one place that can still fix it.

    Before this, ``cycle_stale`` walked worker loops only and the role kept
    running bytes two publishes old for as long as the box was up.
    """

    mirror, store, proc = fleet
    role = _process(proc, STALE_ROLE, [
        "/usr/bin/python3", str(store / "gen-old" / "tools" / "tier_loop.py"),
        "--source-pool", "storage_pool"])
    candidates["tier_loop.py"] = [role]

    stopped = supervise.cycle_stale("gen-live")

    assert stopped == [role], "the stale tiers role survived the publish"
    assert signals == [(role, 15)]

    # ...and the supervisor's next tick puts one back, on the live generation.
    spawned: list[list[str]] = []

    class _Spawned:
        pid = 4242

    monkeypatch.setattr(supervise.subprocess, "Popen",
                        lambda argv, **_kw: spawned.append(argv) or _Spawned())
    candidates["tier_loop.py"] = []           # the stale one has now exited

    assert supervise.ensure_roles(HOST) == [("tiers", 4242)]
    assert spawned and spawned[0][1] == str(
        mirror / "repo" / "tools" / "tier_loop.py")


def test_a_role_already_on_the_published_generation_is_left_running(
    fleet, candidates, signals
) -> None:
    """A role is a singleton: cycling a live one costs a storage cycle for nothing."""

    mirror, _store, proc = fleet
    role = _process(proc, LIVE_ROLE, [
        "/usr/bin/python3", str(mirror / "repo" / "tools" / "tier_loop.py"),
        "--source-pool", "storage_pool"])
    candidates["tier_loop.py"] = [role]

    assert supervise.cycle_stale("gen-live") == []
    assert signals == []


def test_a_stale_role_holding_a_child_is_left_for_the_next_cycle(
    fleet, candidates, signals
) -> None:
    """The same idle rule the worker path keeps: nothing here may kill work.

    A role's cycle shells out -- ``zpool status``, ``arcstat``, ``lsblk`` --
    and a stale role inside one of those is mid-discovery, not idle.
    """

    _mirror, store, proc = fleet
    role = _process(proc, BUSY_ROLE, [
        "/usr/bin/python3", str(store / "gen-old" / "tools" / "tier_loop.py")],
        children=[BUSY_ROLE + 1])
    candidates["tier_loop.py"] = [role]

    assert supervise.cycle_stale("gen-live") == []
    assert signals == []


def test_the_worker_loops_are_still_cycled_beside_the_roles(
    fleet, candidates, signals
) -> None:
    """Regression: the role sweep is added to the worker sweep, not in place of it."""

    _mirror, store, proc = fleet
    worker = _process(proc, WORKER, [
        "/usr/bin/python3", str(store / "gen-old" / "tools" / "worker_loop.py"),
        "--class", "x86"])
    role = _process(proc, STALE_ROLE, [
        "/usr/bin/python3", str(store / "gen-old" / "tools" / "tier_loop.py")])
    candidates["worker_loop.py"] = [worker]
    candidates["tier_loop.py"] = [role]

    assert sorted(supervise.cycle_stale("gen-live")) == sorted([worker, role])
    assert sorted(signals) == sorted([(worker, 15), (role, 15)])


def test_a_role_script_outside_this_fleet_is_never_signalled(
    fleet, candidates, signals
) -> None:
    """A basename is not ownership, and that rule does not weaken for a role."""

    _mirror, _store, proc = fleet
    candidates["tier_loop.py"] = [_process(proc, STALE_ROLE, [
        "/usr/bin/python3", "/another-project/tier_loop.py"])]

    assert supervise.cycle_stale("gen-live") == []
    assert signals == []

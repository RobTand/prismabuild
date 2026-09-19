"""A loop must refuse work sealed for a fleet it is not (2026-09-19).

Tonight the active generation stuck on a Sep-15 tree while ``origin/main``
advanced through ~8 republications, and the two halves of the fleet kept
different books about which code was running:

* a worker agrees with the published generation at the top of its poll, the
  publisher activates a successor while that worker is publishing its offer
  and discovering the queue, and the worker then *claims and executes* an
  action under bytes the fleet has already retired.  The reload check reads
  the generation once per poll, but a claim is not taken at the top of a
  poll -- it is taken after publication and discovery, seconds later;
* ``supervise --ensure`` keeps roles *present*, nothing more.  A
  ``prewarm_loop`` started from the old generation (or with the old
  ``--readers 1`` argv from before #703) is present, so it is never
  restarted, and it went on serving the old shape for hours after the fleet
  had moved.

Nothing refused either drift, and neither wrote anything a reader could
find afterwards; a human noticed.  So three contracts, tested here:

1. the **claim-time handshake**: at the claim boundary, immediately before
   ``serve_once``, the worker re-reads the one tiny ``RUNTIME_VERSION.json``
   through the live ``repo`` name and compares it with the generation its
   own bytes were loaded from.  On mismatch it refuses the claim, stamps one
   immutable ``generation-drift/`` record naming both generations, its pid,
   host and a timestamp, and exits for the supervisor to respawn it.
2. **role freshness**: a role whose argv is not the one the current
   ``fleet_boxes.json`` declares, or whose executable names a non-active
   generation, is stopped when idle and respawned from the active
   generation, and that restart is recorded in the same drift namespace.
3. an agreeing worker still claims, and writes nothing: the record is one
   per incident, not one per poll.

Nothing here reads a real process table, sends a real signal or spawns a
real child: the ``/proc`` bytes, ``pgrep``, ``os.kill`` and ``Popen`` are
this file's own.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import stat as statmod
import subprocess
import sys
from types import SimpleNamespace
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

WORKER_LOOP = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "worker_loop.py"
FLEET_TOOLS = Path(__file__).resolve().parents[1] / "tools" / "fleet"
sys.path.insert(0, str(FLEET_TOOLS))

import supervise  # noqa: E402

HOST = "dl380g10"
#: Far outside any pid this box could hand out, so a mistake cannot land on a
#: real process even if a patch were to slip.
STALE_ARGV_ROLE = 1000000031
STALE_GEN_ROLE = 1000000032
CURRENT_ROLE = 1000000033
BUSY_ROLE = 1000000034

LOADED = {"commit": "e" * 40, "generation": "gen-loaded"}
MOVED = {"commit": "f" * 40, "generation": "gen-moved"}


# -- the worker's claim-time handshake --------------------------------------


def _worker_loop():
    spec = importlib.util.spec_from_file_location(
        "wl_drift_under_test", WORKER_LOOP)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Ledger:
    """The one fake the loop touches between the fence and the claim."""

    def capacity(self) -> dict:
        return {}

    held: dict = {}

    def retire_free_capacity(self, _capacity) -> None:
        pass

    def available(self) -> dict:
        return {}


class _Queue:
    """Records the claim a stale worker must not take."""

    def __init__(self) -> None:
        self.claims: list[dict] = []

    def ledger(self) -> _Ledger:
        return _Ledger()

    def placement_census(self) -> dict:
        return {"ready": 0}

    def serve_once(self, **kwargs) -> None:
        self.claims.append(kwargs)
        return None


def _receipt(path: Path, values: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(values))
    return path


def _drift_records(queue_root: Path) -> list[dict]:
    """Every drift record stamped under one queue root, parsed."""

    directory = queue_root / "generation-drift"
    if not directory.is_dir():
        return []
    return [json.loads(path.read_text())
            for path in sorted(directory.glob("*.json"))]


def _run_worker(tmp_path: Path, capsys, *, move_during_publication: bool):
    """One worker start whose fleet moves at the worst moment, if asked.

    The loaded generation always starts out agreeing with the published one,
    so the poll-top fence passes exactly as it did tonight.  When
    ``move_during_publication`` is set, the publisher activates the
    successor *between* that check and the claim -- the window the handshake
    exists to close.
    """

    wl = _worker_loop()
    queue = _Queue()
    loaded = _receipt(tmp_path / "gen-loaded" / "RUNTIME_VERSION.json", LOADED)
    live = _receipt(tmp_path / "repo" / "RUNTIME_VERSION.json", LOADED)

    def publish(_announce, **_kwargs):
        if move_during_publication:
            _receipt(live, MOVED)
        return wl.PublicationResult("published", 0.001, None, "")

    with mock.patch.object(wl, "SH", tmp_path), \
         mock.patch.object(wl, "GENERATION_VERSION", loaded), \
         mock.patch.object(wl, "RUNTIME_VERSION", live), \
         mock.patch.object(wl, "MAINTENANCE_GATE",
                           _receipt(tmp_path / "gate.json",
                                    {"draining": False})), \
         mock.patch.object(wl.cpu_topology, "pin_to_preferred",
                           return_value=None), \
         mock.patch.object(wl.pool, "PoolQueue",
                           return_value=queue), \
         mock.patch.object(wl, "publish_offer", publish), \
         mock.patch.object(
             wl, "discover_ready_snapshot",
             lambda *_a, **_k: wl.DiscoveryResult("ready", [], None, "")), \
         mock.patch.object(
             sys, "argv",
             ["worker_loop.py", "--once", "--gpu-slots", "0", "--mem-gb", "8",
              "--class", "x86", "--all-cores", "--assume-idle",
              "--poll-s", "0"]):
        assert wl.main() == 0
    return wl, queue, capsys.readouterr().out


def test_a_worker_whose_fleet_moves_at_the_claim_refuses_it(
    tmp_path: Path, capsys,
) -> None:
    """The incident's own shape: agreement at the poll top, a move, a claim.

    Before the handshake this executed the action anyway -- a worker started
    from generation X kept claiming actions the fleet had already moved off
    of, which is the silent half of tonight's incident.
    """

    _wl, queue, out = _run_worker(
        tmp_path, capsys, move_during_publication=True)

    assert queue.claims == [], (
        "a stale worker claimed and executed an action sealed for a fleet "
        "it is not"
    )
    assert "refusing the claim" in out


def test_the_refusal_stamps_one_record_naming_both_generations(
    tmp_path: Path, capsys,
) -> None:
    """The loud stamp: one immutable record, both identities, pid and host."""

    wl, _queue, out = _run_worker(
        tmp_path, capsys, move_during_publication=True)

    records = _drift_records(tmp_path / "pb-queue")
    assert len(records) == 1, (
        f"one record per incident, not one per poll: {records}"
    )
    record = records[0]
    assert record["schema"] == wl.GENERATION_DRIFT_SCHEMA
    assert record["actor"] == "worker_loop"
    assert record["loaded_commit"] == LOADED["commit"]
    assert record["loaded_generation"] == LOADED["generation"]
    assert record["published_commit"] == MOVED["commit"]
    assert record["published_generation"] == MOVED["generation"]
    assert record["pid"] > 0
    assert record["host"]
    assert isinstance(record["unix"], (int, float))
    # The record is immutable: nobody, including its writer, can edit it.
    stamped = next(iter((tmp_path / "pb-queue" / "generation-drift").glob(
        "*.json")))
    assert statmod.S_IMODE(stamped.stat().st_mode) == 0o444
    # It is written under the queue's own root, where the queue's other
    # records live, not in a new place a reader would not look.
    assert (tmp_path / "pb-queue" / "generation-drift").is_dir()


def test_an_agreeing_worker_claims_and_stamps_nothing(
    tmp_path: Path, capsys,
) -> None:
    """The control: agreement costs a claim's worth of one tiny file read."""

    _wl, queue, _out = _run_worker(
        tmp_path, capsys, move_during_publication=False)

    assert len(queue.claims) == 1, "an agreeing worker must still claim"
    assert _drift_records(tmp_path / "pb-queue") == [], (
        "per-claim churn: the record exists per incident, not per poll"
    )


# -- the supervisor's role freshness ----------------------------------------


@pytest.fixture()
def fleet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A fake mount: two published generations, the live link, fake ``/proc``."""

    mirror = tmp_path / "fleet"
    store = mirror / "runtime-generations"
    for name in ("gen-old", "gen-live"):
        generation = store / name
        (generation / "tools").mkdir(parents=True)
        for script in ("worker_loop.py", "prewarm_loop.py", "tier_loop.py"):
            (generation / "tools" / script).write_text("# a loop\n")
        (generation / "RUNTIME_VERSION.json").write_text(
            json.dumps({"commit": f"{name}-commit",
                        "generation": name}))
    (mirror / "repo").symlink_to(store / "gen-live")

    proc = tmp_path / "proc"
    proc.mkdir()
    monkeypatch.setattr(supervise, "MIRROR", mirror)
    monkeypatch.setattr(supervise, "PROC", proc)
    monkeypatch.setattr(supervise, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(supervise, "CONFIG", tmp_path / "fleet_boxes.json")
    monkeypatch.setattr(supervise.socket, "gethostname", lambda: HOST)
    (tmp_path / "fleet_boxes.json").write_text(json.dumps({"boxes": {
        HOST: {"loops": 1, "args": ["--class", "x86"],
               "roles": {"storage": ["--readers", "4",
                                     "--max-readers", "8"]}}}}))
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

    box = SimpleNamespace(sent=[], busy=set())
    monkeypatch.setattr(supervise.os, "kill",
                        lambda pid, sig: box.sent.append((pid, int(sig))))
    monkeypatch.setattr(supervise, "_is_idle",
                        lambda pid, *_a: pid not in box.busy)
    monkeypatch.setattr(supervise, "_claim_holders", lambda: frozenset())
    return box


def _spawned(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """Capture ``Popen`` argv, the way the role respawn test reads it back."""

    spawned: list[list[str]] = []

    class _Spawned:
        pid = 4242

    monkeypatch.setattr(supervise.subprocess, "Popen",
                        lambda argv, **_kw: spawned.append(argv) or _Spawned())
    return spawned


def test_a_role_with_a_stale_argv_is_restarted_from_the_declaration(
    fleet, candidates, signals, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact prewarm failure: present, active generation, old ``argv``.

    ``--ensure`` ensured presence and nothing else, so a ``prewarm_loop``
    kept ``--readers 1`` for hours after #703's declaration said 4.
    """

    mirror, _store, proc = fleet
    role = _process(proc, STALE_ARGV_ROLE, [
        "/usr/bin/python3", str(mirror / "repo" / "tools" / "prewarm_loop.py"),
        "--readers", "1"])
    candidates["prewarm_loop.py"] = [role]
    spawned = _spawned(monkeypatch)

    assert supervise.ensure_roles(HOST) == [("storage", 4242)]

    assert signals.sent == [(role, 15)], (
        "the stale-argv role was left running the old shape"
    )
    # Resolved through the live link, so the replacement's imports are pinned
    # to one immutable root even if a later publish moves ``repo`` again.
    assert spawned and spawned[0][1] == str(
        (mirror / "repo" / "tools" / "prewarm_loop.py").resolve()), (
        "the replacement did not come from the active generation"
    )
    assert spawned[0][2:] == ["--readers", "4", "--max-readers", "8"], (
        "the replacement did not carry the declared argv"
    )
    records = _drift_records(fleet[0] / "pb-queue")
    assert len(records) == 1, records
    assert records[0]["detail"]["role"] == "storage"
    assert records[0]["detail"]["pid"] == role
    assert records[0]["detail"]["running_argv"] == ["--readers", "1"]
    assert records[0]["detail"]["declared_argv"] == [
        "--readers", "4", "--max-readers", "8"]
    assert records[0]["actor"] == "supervise"


def test_a_role_from_a_non_active_generation_is_restarted(
    fleet, candidates, signals, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other half: right argv, retired bytes.

    Only the operator's ``--cycle-stale`` verb reached this before; the
    always-running ``--ensure`` path left it there for the box's uptime.
    """

    _mirror, store, proc = fleet
    role = _process(proc, STALE_GEN_ROLE, [
        "/usr/bin/python3", str(store / "gen-old" / "tools" / "prewarm_loop.py"),
        "--readers", "4", "--max-readers", "8"])
    candidates["prewarm_loop.py"] = [role]
    spawned = _spawned(monkeypatch)

    assert supervise.ensure_roles(HOST) == [("storage", 4242)]

    assert signals.sent == [(role, 15)]
    assert spawned and spawned[0][1] == str(
        (fleet[0] / "repo" / "tools" / "prewarm_loop.py").resolve())
    records = _drift_records(fleet[0] / "pb-queue")
    assert len(records) == 1, records
    assert records[0]["loaded_commit"] == "gen-old-commit"
    assert records[0]["published_commit"] == "gen-live-commit"


def test_a_current_role_is_left_running(
    fleet, candidates, signals, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A role on the active generation with the declared argv is healthy."""

    mirror, _store, proc = fleet
    role = _process(proc, CURRENT_ROLE, [
        "/usr/bin/python3", str(mirror / "repo" / "tools" / "prewarm_loop.py"),
        "--readers", "4", "--max-readers", "8"])
    candidates["prewarm_loop.py"] = [role]
    spawned = _spawned(monkeypatch)

    assert supervise.ensure_roles(HOST) == []
    assert signals.sent == []
    assert spawned == []
    assert _drift_records(fleet[0] / "pb-queue") == []


def test_a_busy_stale_role_finishes_its_cycle_first(
    fleet, candidates, signals, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same idle rule every restart path keeps: nothing here kills work.

    A role's cycle shells out -- a stale prewarm inside its paced reads is
    mid-service, not idle.  It is stopped on a later tick, and until then no
    drift record is written, because no restart has happened.
    """

    _mirror, store, proc = fleet
    role = _process(proc, BUSY_ROLE, [
        "/usr/bin/python3", str(store / "gen-old" / "tools" / "prewarm_loop.py"),
        "--readers", "1"], children=[BUSY_ROLE + 1])
    candidates["prewarm_loop.py"] = [role]
    signals.busy.add(role)
    spawned = _spawned(monkeypatch)

    assert supervise.ensure_roles(HOST) == []
    assert signals.sent == []
    assert spawned == []
    assert _drift_records(fleet[0] / "pb-queue") == []

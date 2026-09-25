"""A successor's adopted file survives a nested prefix's retirement (#1063).

A path can be named by any template whose output prefix contains it, and two
such prefixes always nest.  Each step that claims or deletes an origin path
took the ownership lock of its own template's prefix only, so two templates
whose prefixes nest took different locks and nothing ordered them:

1. `origin_retirement_tick` reads, under the lock of a consumed batch's
   prefix, every other attempt's claim on its paths (`_TickReads.path_owners`,
   #1053) and finds none;
2. a successor of a template whose prefix contains that one's, or lies inside
   it, prewrites the path and commits the file already there under its own
   lock.  A direct commit takes the identity `lstat` finds, so it records the
   committed inode: it adopts the file;
3. the retirement's identity check still matches, and its delete
   (`_unlink_if_committed`) removes the successor's committed file.

Every such step now holds the ownership locks of every filed template whose
prefix overlaps its own, taken in one order (`_output_prefix_locks`), so the
successor waits for the retirement to finish, and a retirement that comes
second sees the successor's commit.  The same read under one lock also missed
a template of the *same* prefix filed after the tick first listed the
templates: the retirement now lists them again under its lock.

The interleavings are forced, not slept for: the successor runs in a thread
started at the retirement's owner read, or at its delete, and the retirement
goes on once the successor has either finished or been refused a lock that
the retirement holds.  Before this change it finishes, and its file is gone.
"""
from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
import sys
import threading

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

from prismabuild import produced_output as po, reader_lease  # noqa: E402

import test_prepaid_writer_integration as fx  # noqa: E402
import test_write_only_produced_output as wo  # noqa: E402
from test_a_dead_producers_batches_and_paths_are_released import (  # noqa: E402
    _events,
)
from test_consumed_origin_retirement import _bind_owner  # noqa: E402

_isolated_synthetic_launch_context = fx._isolated_synthetic_launch_context

#: The R13 shape the issue's census names: a layer's write-only handoff, and
#: a template whose prefix is the whole overlay above it.
OUTER = Path("overlay")
INNER = OUTER / "layer-quanta" / "layer-044" / "handoff"
DEAD_BYTES = b"the dead attempt's layer-44 handoff"
FRESH_BYTES = b"the successor's own layer-44 handoff, written anew"
MOVER = "e" * 64


@pytest.fixture(autouse=True)
def _reports():
    po._UNFILED_REPORTS.clear()
    yield
    po._UNFILED_REPORTS.clear()


def _write_only(prefix: Path, template_id: str) -> dict:
    return po.validate_template({**wo._template(prefix),
                                 "template_id": template_id})


def _read_back(prefix: Path, template_id: str) -> dict:
    """A staged template, as every live one is (the issue's census)."""

    return po.validate_template({
        "schema": po.TEMPLATE_SCHEMA_V1, "version": 1,
        "template_id": template_id, "output_prefix": str(prefix),
        "slots": {"s0": {"class": "payload"}},
        "durable_maxima": {"payload_max_bytes": 1 << 20,
                           "checkpoint_max_bytes": 1 << 20,
                           "temp_max_bytes": 1 << 20},
        "working_demands": {wo.TIER: {"minimum_gib": 1, "window_gib": 2}},
        "permitted_tiers": [wo.TIER],
    })


def _dead_handoff(queue, template: dict, path: Path, seed: str = "dead"
                  ) -> dict:
    """A dead producer's consumed origin-only batch over ``path``, due."""

    instance = _bind_owner(queue, template, fx._hexkey(seed))
    assert wo._prewrite(queue, instance, template, "b1", [path],
                        len(DEAD_BYTES))["ok"] is True
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(DEAD_BYTES)
    committed = po.commit_origin_batch(
        queue, instance, template,
        [wo._descriptor(instance, template, path, DEAD_BYTES)],
        batch_id="b1", lifetime=po.ORIGIN_LIFETIME_CONSUMED)
    assert committed["ok"] is True, committed
    queue.finish(instance["owner_action_key"], status="failed")
    assert po._producer_attempt_state(queue, instance) == "dead"
    return instance


class _Successor:
    """A live producer that commits ``path``: the file there if there is one.

    It prewrites first, as the contract says, and then adopts a present
    file -- a direct commit records whatever ``lstat`` finds -- or writes its
    own by rename.
    """

    def __init__(self, queue, template: dict, kind: str, path: Path,
                 seed: str = "successor") -> None:
        self.queue, self.template, self.kind, self.path = (
            queue, template, kind, path)
        if kind == "read-back":
            self.instance = fx._bind(queue, template, fx._hexkey(seed))
        else:
            self.instance = _bind_owner(queue, template, fx._hexkey(seed))
        self.prewritten: dict | None = None
        self.committed: dict | None = None
        self.adopted: bool | None = None

    def run(self) -> None:
        self.prewritten = wo._prewrite(
            self.queue, self.instance, self.template, "s1", [self.path],
            max(len(DEAD_BYTES), len(FRESH_BYTES)))
        if self.prewritten.get("ok") is not True:
            return
        self.adopted = self.path.exists()
        if self.adopted:
            payload = self.path.read_bytes()
        else:
            payload = FRESH_BYTES
            temporary = self.path.with_name(self.path.name + ".successor.tmp")
            temporary.write_bytes(payload)
            os.replace(temporary, self.path)
        descriptor = wo._descriptor(self.instance, self.template, self.path,
                                    payload)
        if self.kind == "read-back":
            self.committed = po.commit_batch(
                self.queue, self.instance, self.template, [descriptor],
                batch_id="s1", tier=wo.TIER, mover_key=MOVER)
        else:
            self.committed = po.commit_origin_batch(
                self.queue, self.instance, self.template, [descriptor],
                batch_id="s1")

    def recorded(self) -> dict:
        return json.loads((Path(self.queue.root) / "residency"
                           / po.OUTPUT_BATCHES_SUBDIR
                           / po.instance_namespace(self.instance)
                           / "s1.json").read_text())["origin_identity"]


class _Race:
    """Run a successor in its own thread from inside the retirement.

    `fire` starts it and returns once it has finished or has been refused an
    ownership lock another thread holds -- the retirement's.  Its lock
    requests try without waiting first, so that refusal is observed, not
    guessed at; then it waits for the lock as it always would.
    """

    def __init__(self, monkeypatch, successor: _Successor) -> None:
        self.successor = successor
        self.contended: list[str] = []
        self.settled = threading.Event()
        self.failure: list[BaseException] = []
        self.worker = threading.Thread(target=self._run, daemon=True)
        real = successor.queue.stage_ownership_lock

        @contextlib.contextmanager
        def lock(stage_root, *args, **kwargs):
            if (threading.current_thread() is self.worker
                    and kwargs.get("blocking", True)):
                with real(stage_root, blocking=False) as got:
                    if got:
                        yield got
                        return
                self.contended.append(str(stage_root))
                self.settled.set()
            with real(stage_root, *args, **kwargs) as got:
                yield got

        monkeypatch.setattr(successor.queue, "stage_ownership_lock", lock)

    def _run(self) -> None:
        try:
            self.successor.run()
        except BaseException as exc:  # reported by `join`
            self.failure.append(exc)
        finally:
            self.settled.set()

    def fire(self) -> None:
        if self.worker.is_alive() or self.settled.is_set():
            return
        self.worker.start()
        assert self.settled.wait(timeout=60), (
            "the successor neither finished nor waited for a lock")

    def join(self) -> None:
        self.worker.join(timeout=60)
        assert not self.worker.is_alive(), "the successor never finished"
        if self.failure:
            raise self.failure[0]


def _at_the_owner_read(monkeypatch, instance: dict, action) -> None:
    """Run ``action`` once the retirement of ``instance`` read the owners."""

    real = po._TickReads.path_owners

    def path_owners(self, owner_instance, *args, **kwargs):
        owners = real(self, owner_instance, *args, **kwargs)
        if owner_instance["owner_action_key"] == instance["owner_action_key"]:
            action()
        return owners

    monkeypatch.setattr(po._TickReads, "path_owners", path_owners)


def _at_the_delete(monkeypatch, path: Path, action) -> None:
    """Run ``action`` as the retirement asks to delete ``path``."""

    real = po._unlink_if_committed

    def unlink_if_committed(target, *args, **kwargs):
        if os.fspath(target) == str(path):
            action()
        return real(target, *args, **kwargs)

    monkeypatch.setattr(po, "_unlink_if_committed", unlink_if_committed)


INTERLEAVINGS = {"after-the-owner-read": _at_the_owner_read,
                 "at-the-delete": _at_the_delete}


@pytest.mark.parametrize("kind", ["write-only", "read-back"])
@pytest.mark.parametrize("successor_prefix", ["outer", "inner"])
@pytest.mark.parametrize("moment", sorted(INTERLEAVINGS))
def test_a_successors_adopted_file_survives_a_nested_prefix_retirement(
        tmp_path: Path, monkeypatch, kind: str, successor_prefix: str,
        moment: str) -> None:
    """The issue's red: the consumed batch's retirement and a successor that
    adopts its inode, under templates whose prefixes nest."""

    outer, inner = tmp_path / OUTER, tmp_path / INNER
    path = inner / "handoff-044.pt"
    dead_prefix, own_prefix = ((inner, outer) if successor_prefix == "outer"
                               else (outer, inner))
    queue = wo._queue(tmp_path)
    dead = _dead_handoff(queue, _write_only(dead_prefix, "handoff-v1"), path)
    make = _read_back if kind == "read-back" else _write_only
    successor = _Successor(queue, make(own_prefix, f"successor-{kind}-v1"),
                           kind, path)
    race = _Race(monkeypatch, successor)
    if moment == "after-the-owner-read":
        _at_the_owner_read(monkeypatch, dead, race.fire)
    else:
        _at_the_delete(monkeypatch, path, race.fire)

    events = po.origin_retirement_tick(queue)
    race.join()

    assert successor.prewritten and successor.prewritten["ok"] is True, (
        successor.prewritten)
    assert successor.committed and successor.committed["ok"] is True, (
        successor.committed)
    assert path.exists(), (
        f"the retirement deleted the file the successor committed "
        f"(adopted: {successor.adopted})")
    recorded = successor.recorded()[str(path)]
    assert reader_lease.file_id_matches(
        recorded, reader_lease.portable_identity(os.lstat(path))), (
        "the successor's committed identity is not the file at its path")
    # The successor waited for the retirement's lock, and wrote its own
    # file once the retirement had deleted the dead attempt's.
    assert race.contended, "the successor was never ordered by the retirement"
    assert successor.adopted is False
    assert path.read_bytes() == FRESH_BYTES
    retired = _events(events, po.ORIGIN_RETIRED_EVENT)
    assert [event["unlinked"] for event in retired] == [[str(path)]], events


@pytest.mark.parametrize("successor_prefix", ["outer", "inner"])
def test_a_retirement_after_the_adoption_leaves_the_file(
        tmp_path: Path, successor_prefix: str) -> None:
    """The order the lock allows the other way: the successor commits first."""

    outer, inner = tmp_path / OUTER, tmp_path / INNER
    path = inner / "handoff-044.pt"
    dead_prefix, own_prefix = ((inner, outer) if successor_prefix == "outer"
                               else (outer, inner))
    queue = wo._queue(tmp_path)
    dead = _dead_handoff(queue, _write_only(dead_prefix, "handoff-v1"), path)
    committed_inode = os.lstat(path).st_ino
    successor = _Successor(queue, _write_only(own_prefix, "successor-v1"),
                           "write-only", path)
    successor.run()
    assert successor.adopted is True
    assert successor.committed and successor.committed["ok"] is True

    events = po.origin_retirement_tick(queue)

    retired = _events(events, po.ORIGIN_RETIRED_EVENT)
    assert len(retired) == 1, events
    assert retired[0]["unlinked"] == [] and retired[0]["superseded"] == [
        str(path)]
    assert retired[0]["superseded_by"] == {str(path): {
        "owner_action_key": successor.instance["owner_action_key"],
        "nonce": successor.instance["owner_attempt"]["nonce"],
        "batch_id": "s1"}}
    assert os.lstat(path).st_ino == committed_inode
    assert path.read_bytes() == DEAD_BYTES
    entry = po._read_commitments(
        po._commitments_path(queue.root, dead))["batches"]["b1"]
    assert entry["origin_reclaimed"] is True


def test_a_template_filed_during_the_tick_is_read_under_the_lock(
        tmp_path: Path, monkeypatch) -> None:
    """The same-lock variant: a template of the same prefix filed mid-tick.

    The tick listed the templates once, at its first owner read, and every
    later retirement in the tick read owners through that list.  A template
    filed after it, whose successor committed a due batch's file before
    that batch's retirement took the lock, was not in it: the lock ordered
    the two, and the retirement still deleted the successor's file.
    """

    prefix = tmp_path / INNER
    queue = wo._queue(tmp_path)
    template = _write_only(prefix, "handoff-v1")
    paths = {}
    for seed in ("dead-a", "dead-b"):
        instance = _dead_handoff(queue, template, prefix / f"{seed}.pt", seed)
        paths[instance["owner_action_key"]] = prefix / f"{seed}.pt"
    successors: list[_Successor] = []
    real = po._retire_consumed_batch

    def retire(queue_, instance, *args, **kwargs):
        event = real(queue_, instance, *args, **kwargs)
        if not successors:
            # Between the tick's two retirements, and holding no lock: a new
            # action on a new template of the same prefix adopts the file of
            # the batch the tick has not reached yet.
            (other,) = set(paths) - {instance["owner_action_key"]}
            successor = _Successor(
                queue_, _write_only(prefix, "relaunch-v1"), "write-only",
                paths[other], seed="relaunch")
            successor.run()
            successors.append(successor)
        return event

    monkeypatch.setattr(po, "_retire_consumed_batch", retire)

    events = po.origin_retirement_tick(queue)

    (successor,) = successors
    assert successor.adopted is True
    assert successor.committed and successor.committed["ok"] is True
    assert successor.path.exists(), (
        "the retirement deleted the file a template filed during the tick "
        "had committed")
    assert successor.path.read_bytes() == DEAD_BYTES
    retired = _events(events, po.ORIGIN_RETIRED_EVENT)
    assert sorted(event["superseded"] for event in retired) == [
        [], [str(successor.path)]], events


@pytest.mark.parametrize("successor_prefix", ["outer", "inner"])
def test_each_step_asks_for_every_overlapping_prefix_lock_in_one_order(
        tmp_path: Path, monkeypatch, successor_prefix: str) -> None:
    """Prewrite, both commits and the retirement take the same locks, sorted.

    One order for every caller that holds more than one of these locks is
    what keeps two of them from each holding the lock the other wants.
    """

    outer, inner = tmp_path / OUTER, tmp_path / INNER
    dead_prefix, own_prefix = ((inner, outer) if successor_prefix == "outer"
                               else (outer, inner))
    queue = wo._queue(tmp_path)
    dead = _dead_handoff(queue, _write_only(dead_prefix, "handoff-v1"),
                         inner / "handoff-044.pt")
    write_only = _Successor(queue, _write_only(own_prefix, "successor-v1"),
                            "write-only", inner / "write-only.pt")
    read_back = _Successor(queue, _read_back(own_prefix, "read-back-v1"),
                           "read-back", inner / "read-back.pt",
                           seed="read-back")
    asked: list[str] = []
    real = queue.stage_ownership_lock

    def lock(stage_root, *args, **kwargs):
        asked.append(str(stage_root))
        return real(stage_root, *args, **kwargs)

    monkeypatch.setattr(queue, "stage_ownership_lock", lock)
    expected = sorted([str(outer), str(inner)])

    steps = {}
    for name, successor in (("write-only", write_only),
                            ("read-back", read_back)):
        asked.clear()
        successor.prewritten = wo._prewrite(
            queue, successor.instance, successor.template, "s1",
            [successor.path], len(FRESH_BYTES))
        steps[f"{name} prewrite"] = list(asked)
        successor.path.write_bytes(FRESH_BYTES)
        asked.clear()
        successor.adopted = False
        descriptor = wo._descriptor(successor.instance, successor.template,
                                    successor.path, FRESH_BYTES)
        if name == "read-back":
            successor.committed = po.commit_batch(
                queue, successor.instance, successor.template, [descriptor],
                batch_id="s1", tier=wo.TIER, mover_key=MOVER)
        else:
            successor.committed = po.commit_origin_batch(
                queue, successor.instance, successor.template, [descriptor],
                batch_id="s1")
        steps[f"{name} commit"] = list(asked)
        assert successor.prewritten["ok"] is True, successor.prewritten
        assert successor.committed["ok"] is True, successor.committed
    asked.clear()
    retired = _events(po.origin_retirement_tick(queue),
                      po.ORIGIN_RETIRED_EVENT)
    steps["retirement"] = list(asked)

    assert len(retired) == 1 and retired[0]["ref"]["owner_action_key"] == (
        dead["owner_action_key"]), retired
    assert steps == {step: expected for step in steps}

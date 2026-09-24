"""A dead producer's retirement backlog writes its commitments once (#1072).

Before #1072 the retirement tick filed each staged batch of a dead producer
through its own `retire_batch`: four whole-document reads of the instance's
commitments and one whole-document write, fsync included, per batch.  R13's
document holds 436 batches in 9.2 MB, and the live first cycle after the
09-24 publish spent 129.7 s retiring 137 of them.  `retire_staged_batches`
now selects every batch of an instance under one lock, runs each egress with
no lock held, and files every complete receipt under one lock with one read
and one write.

The queue is R13's dead instance (`tests/r13_1053_replay.py`): ten unretired
batches whose movers ended, whose funding is consumed and whose stage copies
are gone, on a tier record naming this box.

What must hold:

* the ten batches are filed with one commitments write;
* a crash at that write leaves every batch unretired, and the next tick
  files every one of them: the egress it runs again finds nothing to delete;
* a batch whose egress is incomplete stays unretired, and the rest are filed;
* a batch whose record changed while its egress ran is not filed by the
  tick, and the change is kept.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

import prismabuild.produced_output as po  # noqa: E402
import stage_release  # noqa: E402

import r13_1053_replay as r13  # noqa: E402
import test_prepaid_writer_integration as fx  # noqa: E402

UNRETIRED = 10


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT", "PRISMABUILD_ACTION_KEY"):
        monkeypatch.delenv(name, raising=False)
    stage_release.reset_holder_reports()
    po._UNFILED_REPORTS.clear()
    yield
    stage_release.reset_holder_reports()
    po._UNFILED_REPORTS.clear()


class _Crash(BaseException):
    """The process dying at a point: nothing in the tick catches it."""


class _World:
    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.q = fx._queue(tmp_path, gib=64)
        self.stage = tmp_path / "stage"
        fx._announce_tier(self.q, self.stage)
        self.replay = r13.install(self.q, prefix=tmp_path / "origin" / "adjoint",
                                  stage=self.stage, origin_files=False)
        assert len(self.replay.unretired) == UNRETIRED
        self.commitments = self.replay.scope / "commitments.json"
        #: Whole-document reads and writes of the instance's commitments.
        self.reads = 0
        self.writes = 0
        #: Set to make the next write of the instance's commitments crash.
        self.crash_at_write = False
        real_read = po._read_commitments
        real_write = po._write_commitments

        def read(path, *args, **kwargs):
            if Path(path) == self.commitments:
                self.reads += 1
            return real_read(path, *args, **kwargs)

        def write(path, *args, **kwargs):
            if Path(path) == self.commitments:
                if self.crash_at_write:
                    self.crash_at_write = False
                    raise _Crash("crashed before the commitments write")
                self.writes += 1
            return real_write(path, *args, **kwargs)

        monkeypatch.setattr(po, "_read_commitments", read)
        monkeypatch.setattr(po, "_write_commitments", write)

    def mover(self, batch_id: str) -> str:
        """The mover of the batch's active copy: a restaged batch's successor."""

        return str(po._active_materialization(
            self.replay.entry(self.q.root, batch_id))["mover_key"])

    def retired(self) -> list[str]:
        return [batch_id for batch_id in self.replay.unretired
                if po._batch_stage_retired(self.replay.entry(self.q.root,
                                                             batch_id))]

    def tick(self) -> list[dict]:
        self.reads = self.writes = 0
        return po.origin_retirement_tick(self.q)

    def edit(self, batch_id: str, change) -> None:
        """Change one batch's commitments entry as another writer would."""

        with self.q.stage_ownership_lock(
                str(self.replay.instance["output_prefix"])):
            record = json.loads(self.commitments.read_text())
            entry = dict(record["batches"][batch_id])
            change(entry)
            record["batches"][batch_id] = entry
            self.commitments.write_text(json.dumps(record, sort_keys=True) + "\n")


def _retired_events(events: list[dict]) -> list[str]:
    return sorted(str(event["batch"]).rpartition("/")[2] for event in events
                  if event.get("event") == po.DEAD_PRODUCER_BATCH_RETIRED_EVENT)


def test_ten_retirements_write_the_commitments_once(tmp_path, monkeypatch):
    world = _World(tmp_path, monkeypatch)
    events = world.tick()
    reads, writes = world.reads, world.writes
    assert _retired_events(events) == sorted(world.replay.unretired)
    assert world.retired() == world.replay.unretired
    assert writes == 1, f"{writes} commitments writes for ten batches"
    # The tick's own read, and one under the lock on each side of the egresses.
    assert reads == 3, reads
    # Nothing is left for the next tick.
    assert _retired_events(world.tick()) == []
    assert world.writes == 0


def test_a_crash_at_the_write_files_nothing_and_the_next_tick_files_all(
        tmp_path, monkeypatch):
    world = _World(tmp_path, monkeypatch)
    evicted: list[str] = []
    real_evict = stage_release.evict

    def evict(queue, mover, *args, **kwargs):
        receipt = real_evict(queue, mover, *args, **kwargs)
        evicted.append(mover)
        return receipt

    monkeypatch.setattr(stage_release, "evict", evict)
    world.crash_at_write = True
    with pytest.raises(_Crash):
        world.tick()
    # Every egress ran, and not one retirement is on record: the state a
    # crash between `retire_batch`'s egress and its write already leaves.
    assert sorted(evicted) == sorted(world.mover(batch_id)
                                     for batch_id in world.replay.unretired)
    assert world.retired() == []
    evicted.clear()
    events = world.tick()
    # The egresses run again, find nothing to delete, and every batch files.
    assert len(evicted) == UNRETIRED
    assert _retired_events(events) == sorted(world.replay.unretired)
    assert world.retired() == world.replay.unretired
    assert world.writes == 1


def test_an_incomplete_egress_stays_unretired(tmp_path, monkeypatch):
    world = _World(tmp_path, monkeypatch)
    held = world.replay.unretired[0]
    held_mover = world.mover(held)
    real_evict = stage_release.evict

    def evict(queue, mover, *args, **kwargs):
        receipt = real_evict(queue, mover, *args, **kwargs)
        if mover == held_mover:
            return dict(receipt, complete=False,
                        errors=["a reader still holds the copy"])
        return receipt

    monkeypatch.setattr(stage_release, "evict", evict)
    events = world.tick()
    assert world.retired() == world.replay.unretired[1:]
    assert _retired_events(events) == sorted(world.replay.unretired[1:])
    refused = [event for event in events
               if event.get("event") == po.ORIGIN_RETIREMENT_REFUSED_EVENT
               and str(event.get("batch", "")).endswith(held)]
    assert [event["reason"] for event in refused] == ["egress-incomplete"]
    assert world.writes == 1


@pytest.mark.parametrize("change", ["retired-by-another", "mover-changed"])
def test_a_batch_that_changed_during_its_egress_is_not_filed(
        tmp_path, monkeypatch, change):
    world = _World(tmp_path, monkeypatch)
    moved = world.replay.unretired[3]
    moved_mover = world.mover(moved)
    real_evict = stage_release.evict

    def other_writer(entry: dict) -> None:
        if change == "retired-by-another":
            entry["retired"] = True
            entry["staged_paths"] = ["/written/by/another/retirement"]
        else:
            entry["mover_key"] = "f" * 64

    def evict(queue, mover, *args, **kwargs):
        receipt = real_evict(queue, mover, *args, **kwargs)
        if mover == moved_mover:
            world.edit(moved, other_writer)
        return receipt

    monkeypatch.setattr(stage_release, "evict", evict)
    events = world.tick()
    others = [batch_id for batch_id in world.replay.unretired if batch_id != moved]
    assert _retired_events(events) == sorted(others)
    assert world.writes == 1
    entry = world.replay.entry(world.q.root, moved)
    if change == "retired-by-another":
        # The other writer's retirement stands as it wrote it.
        assert entry["retired"] is True
        assert entry["staged_paths"] == ["/written/by/another/retirement"]
        assert sorted(world.retired()) == sorted(world.replay.unretired)
    else:
        # The tick files nothing for a selection that moved, and keeps the
        # change.
        assert entry["mover_key"] == "f" * 64
        assert entry.get("retired") is not True
        assert sorted(world.retired()) == sorted(others)

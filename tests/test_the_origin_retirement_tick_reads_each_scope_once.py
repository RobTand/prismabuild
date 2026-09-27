"""One origin-retirement tick parses each scope's commitments once (#992).

RC1 of the 2026-09-26 hot-path audit: `origin_retirement_tick` reads each
scope's commitments once to build the due list (`_TickReads.batches`), then
`_retire_consumed_batch_locked` re-read the whole file once per due batch,
bypassing the tick's memo.  The live shape -- 39 write-only stage-B handoff
scopes, each with 33 consumed batches whose succeeded producer declared no
consumer -- therefore parsed 414 KB x 39 x 34 per cycle and deleted nothing.

What must hold now:

* a quiet scope (nothing changed, every batch waiting for a consumer) is
  parsed once for the tick, whatever its number of due batches, and its
  output-prefix lock order is computed once;
* the parse is still authoritative: a replacement or a corrupt document
  between two batches is read, never remembered over;
* a failed write leaves neither the file nor the memo carrying the decision
  that was not filed, and the next tick re-decides from disk;
* a consumer declared between cycles still retires its batch, and the bound
  admission record survives every retirement write.

Fixture concessions: owners and consumers are published, claimed and
finished through the real ``PoolQueue``, as in
``test_consumed_origin_retirement``.
"""
from __future__ import annotations

import collections
import json
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_prepaid_writer_integration as fx  # noqa: E402
from prismabuild import produced_output as po  # noqa: E402
from test_consumed_origin_retirement import (  # noqa: E402
    _bind_owner, _publish_consumer, _run_consumer,
)
from test_write_only_produced_output import (  # noqa: E402
    _descriptor, _prewrite, _queue, _template,
)

CONSUMED = po.ORIGIN_LIFETIME_CONSUMED


@pytest.fixture(autouse=True)
def _isolated():
    po._UNFILED_REPORTS.clear()
    yield
    po._UNFILED_REPORTS.clear()


class _World:
    """One write-only owner with N consumed origin-only batches."""

    def __init__(self, tmp_path: Path, *, batches: int = 5,
                 owner_seed: str = "write-only-owner") -> None:
        self.queue = _queue(tmp_path)
        self.template = _template(tmp_path / "canonical")
        self.instance = _bind_owner(self.queue, self.template,
                                    fx._hexkey(owner_seed))
        self.owner = self.instance["owner_action_key"]
        self.paths: dict[str, Path] = {}
        self.refs: dict[str, dict] = {}
        for index in range(batches):
            batch_id = f"b{index}"
            path, ref = self._commit(batch_id)
            self.paths[batch_id] = path
            self.refs[batch_id] = ref
        self.commitments = po._commitments_path(self.queue.root, self.instance)
        self.admission = po._read_commitments(self.commitments)["admission"]

    def _commit(self, batch_id: str,
                payload: bytes = b"handoff bytes") -> tuple[Path, dict]:
        path = Path(self.template["output_prefix"]) / f"{batch_id}.bin"
        assert _prewrite(self.queue, self.instance, self.template, batch_id,
                         [path], len(payload))["ok"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        committed = po.commit_origin_batch(
            self.queue, self.instance, self.template,
            [_descriptor(self.instance, self.template, path, payload)],
            batch_id=batch_id, lifetime=CONSUMED)
        assert committed["ok"], committed
        return path, dict(committed["ref"])

    def entry(self, batch_id: str) -> dict:
        return po._read_commitments(self.commitments)["batches"][batch_id]

    def replace(self, change) -> None:
        """Rewrite the commitments under the ownership lock, in place."""

        with self.queue.stage_ownership_lock(
                str(self.instance["output_prefix"])):
            change(self.commitments)

    def settle(self) -> None:
        """Let one clock tick pass, so a version may be kept (#1045)."""

        time.sleep(0.05)

    def tick(self) -> list[dict]:
        return po.origin_retirement_tick(self.queue)


def _version_keepable(path: Path) -> bool:
    """Whether this host can vouch for a file version here (#1045).

    The memo keeps a commitments parse only under `stage_move`'s trusted
    version: a fence read before the stat, on a filesystem whose times come
    from this kernel's clock.  A host whose fixture directory cannot vouch
    for one re-reads by design, and the count assertions below do not apply.
    """

    trusted = po._trusted_version_facilities()
    return (trusted is not None
            and trusted[0](path.stat(), trusted[1]()) is not None)


class _Reads:
    """Counts this scope's whole-document reads and lock-order derivations."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch,
                 commitments: Path) -> None:
        self.commitments = Path(commitments)
        self.counts: collections.Counter = collections.Counter()
        real_read = po._read_commitments
        real_order = po._output_prefix_lock_order

        def read(path, *args, **kwargs):
            if Path(path) == self.commitments:
                self.counts["commitments_read"] += 1
            return real_read(path, *args, **kwargs)

        def order(own, templates):
            self.counts["lock_order"] += 1
            return real_order(own, templates)

        monkeypatch.setattr(po, "_read_commitments", read)
        monkeypatch.setattr(po, "_output_prefix_lock_order", order)


def test_a_quiet_scope_parses_its_commitments_once(tmp_path: Path,
                                                   monkeypatch
                                                   ) -> None:
    """Five due batches, a succeeded producer, no consumer: one parse."""

    world = _World(tmp_path, batches=5)
    world.queue.finish(world.owner, status="executed")
    world.settle()
    if not _version_keepable(world.commitments):
        pytest.skip("this host's filesystem cannot vouch for a file version")
    reads = _Reads(monkeypatch, world.commitments)

    assert world.tick() == []

    assert reads.counts["commitments_read"] == 1, dict(reads.counts)
    assert reads.counts["lock_order"] == 1, dict(reads.counts)
    assert all(path.exists() for path in world.paths.values())
    assert po._read_commitments(world.commitments)["admission"] == world.admission


def test_a_consumer_declared_between_cycles_retires_its_batch(
        tmp_path: Path) -> None:
    world = _World(tmp_path, batches=3)
    world.queue.finish(world.owner, status="executed")
    assert world.tick() == []
    consumer = fx._hexkey("late-reader")
    assert po.declare_origin_consumer(
        world.queue, world.refs["b0"], consumer_action_key=consumer) == {
            "ok": True, "declared": True}
    _publish_consumer(world.queue, consumer)
    _run_consumer(world.queue, consumer, "executed")

    events = world.tick()

    assert [(event["event"], event["ref"]["batch_id"]) for event in events] == [
        (po.ORIGIN_RETIRED_EVENT, "b0")]
    assert events[0]["ref"] == world.refs["b0"]
    assert not world.paths["b0"].exists()
    assert all(world.paths[batch_id].exists() for batch_id in ("b1", "b2"))
    assert po._read_commitments(world.commitments)["admission"] == world.admission
    assert world.tick() == []


def test_a_stall_reports_once_and_retires_when_the_consumer_succeeds(
        tmp_path: Path) -> None:
    world = _World(tmp_path, batches=1)
    consumer = fx._hexkey("stalled-reader")
    po.declare_origin_consumer(world.queue, world.refs["b0"],
                               consumer_action_key=consumer)
    _publish_consumer(world.queue, consumer)
    _run_consumer(world.queue, consumer, "failed")

    first = world.tick()

    assert [(event["event"], event["consumers"]) for event in first] == [
        (po.ORIGIN_RETIREMENT_STALLED_EVENT,
         [{"action_key": consumer, "state": "failed"}])]
    assert world.entry("b0")["retirement_report"]
    assert world.paths["b0"].exists()
    assert world.tick() == [], "the same stall is not reported again"
    assert world.entry("b0")["retirement_report"], (
        "the report memo is on disk, not only in a memo")

    _publish_consumer(world.queue, consumer)
    _run_consumer(world.queue, consumer, "executed")
    events = world.tick()
    assert [event["event"] for event in events] == [po.ORIGIN_RETIRED_EVENT]
    assert not world.paths["b0"].exists()


def test_a_replacement_between_batches_is_read(tmp_path: Path,
                                               monkeypatch
                                               ) -> None:
    """A document replaced after one batch decides the next batch."""

    world = _World(tmp_path, batches=2)
    world.queue.finish(world.owner, status="failed")
    real = po._retire_consumed_batch
    calls: list[str] = []

    def hooked(*args, **kwargs):
        event = real(*args, **kwargs)
        calls.append(str(args[3]))
        if len(calls) == 1:
            def change(path: Path) -> None:
                record = json.loads(path.read_text())
                record["batches"]["b1"]["origin_only"] = False
                path.write_text(json.dumps(record, sort_keys=True) + "\n")

            world.replace(change)
        return event

    monkeypatch.setattr(po, "_retire_consumed_batch", hooked)
    events = world.tick()

    assert calls == ["b0", "b1"]
    assert [(event["event"], event["ref"]["batch_id"]) for event in events] == [
        (po.ORIGIN_RETIRED_EVENT, "b0")]
    assert not world.paths["b0"].exists()
    assert world.paths["b1"].exists(), "the replacement's answer decides"
    assert world.entry("b1")["origin_only"] is False


def test_a_corrupt_replacement_between_batches_refuses(tmp_path: Path,
                                                       monkeypatch
                                                       ) -> None:
    """An unreadable document is unknown state, never the last parse."""

    world = _World(tmp_path, batches=2)
    world.queue.finish(world.owner, status="failed")
    real = po._retire_consumed_batch
    calls: list[str] = []

    def hooked(*args, **kwargs):
        event = real(*args, **kwargs)
        calls.append(str(args[3]))
        if len(calls) == 1:
            world.replace(lambda path: path.write_text("{ not json\n"))
        return event

    monkeypatch.setattr(po, "_retire_consumed_batch", hooked)
    events = world.tick()

    assert calls == ["b0", "b1"]
    assert [(event["event"],
             None if event.get("ref") is None
             else event["ref"]["batch_id"]) for event in events] == [
        (po.ORIGIN_RETIRED_EVENT, "b0"),
        (po.ORIGIN_RETIREMENT_REFUSED_EVENT, None)]
    assert "corrupt" in events[1]["reason"], events[1]
    assert not world.paths["b0"].exists()
    assert world.paths["b1"].exists()


def test_a_failed_write_leaves_no_cached_mutation(tmp_path: Path,
                                                  monkeypatch
                                                  ) -> None:
    world = _World(tmp_path, batches=1)
    world.queue.finish(world.owner, status="failed")
    real_write = po._write_commitments
    failed: list[bool] = []

    def write(path, record):
        if Path(path) == world.commitments and not failed:
            failed.append(True)
            raise po.ProducedOutputError("commitments write failed: test")
        return real_write(path, record)

    monkeypatch.setattr(po, "_write_commitments", write)
    events = world.tick()

    assert failed
    refused = [event for event in events
               if event["event"] == po.ORIGIN_RETIREMENT_REFUSED_EVENT]
    assert len(refused) == 1 and "commitments write failed" in refused[0]["reason"]
    assert world.paths["b0"].exists()
    assert not world.entry("b0").get("retiring")
    assert not world.entry("b0").get("origin_reclaimed")
    scope = po.instance_dir(world.queue.root, world.instance)
    assert not po._TickReads(world.queue).batches(scope)["b0"].get("retiring")

    monkeypatch.setattr(po, "_write_commitments", real_write)
    events = world.tick()
    assert [event["event"] for event in events] == [po.ORIGIN_RETIRED_EVENT]
    assert not world.paths["b0"].exists()


def test_the_lock_order_is_kept_until_the_listing_changes(tmp_path: Path
                                                          ) -> None:
    reads = po._TickReads(_queue(tmp_path))
    listing = {"a": {"output_prefix": "/p/own"}}

    first = reads.lock_order("/p/own", listing)

    assert first == ["/p/own"]
    assert reads.lock_order("/p/own",
                            {"a": {"output_prefix": "/p/own"}}) == first
    assert reads.lock_orders == 1

    grown = reads.lock_order("/p/own", {
        "a": {"output_prefix": "/p/own"},
        "b": {"output_prefix": "/p/own/nested"}})

    assert grown == ["/p/own", "/p/own/nested"]
    assert reads.lock_orders == 2, "a filed template invalidates the order"

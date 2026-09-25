"""One retirement tick reads each owner key and each sibling record once (#977).

`origin_retirement_tick` runs once per tier-loop cycle and works from one
snapshot of what it read (`_TickReads`).  Two paths re-read it:

1. The #914 retirement asked `_producer_attempt_state` for every due
   consumed batch that no consumer declared, and that reads the owner key
   (`_key_generation`, itself two directory walks) afresh each time.  A
   running producer that commits consumed batches ahead of their consumers
   cost one owner-key read per batch per cycle, a round trip each on NFS.
2. The #949 sweep of an ended attempt's prewrites and the #914 retirement of
   its batches both ask who else names their paths
   (`_TickReads.path_owners`), which reads every sibling attempt's
   commitments and prewrite records.

Read counts, not timings: each owner key is read once per tick, each sibling
record once per tick, and the next tick reads them again (a cycle's snapshot
is not carried into the next one).  The instance's own commitments are read
again under its output-prefix lock by design -- a read-modify-write that must
see every write made before the lock was taken -- and are not counted here.

Fixture concessions: owners are published, claimed and finished through the
real ``PoolQueue``; run with the ``nobroker_plugin`` like the #914 tests.
"""
from __future__ import annotations

import collections
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_prepaid_writer_integration as fx  # noqa: E402
from prismabuild import pool, produced_output as po  # noqa: E402
from test_consumed_origin_retirement import _bind_owner  # noqa: E402
from test_write_only_produced_output import (  # noqa: E402
    _descriptor, _prewrite, _queue, _template)

CONSUMED = po.ORIGIN_LIFETIME_CONSUMED
RETAIN = po.ORIGIN_LIFETIME_RETAIN


def _commit(queue, template, instance, batch_id: str, *,
            lifetime: str = CONSUMED) -> Path:
    payload = b"handoff bytes"
    path = (Path(template["output_prefix"])
            / f"{instance['owner_action_key'][:12]}-{batch_id}.bin")
    assert _prewrite(queue, instance, template, batch_id, [path],
                     len(payload))["ok"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    got = po.commit_origin_batch(
        queue, instance, template,
        [_descriptor(instance, template, path, payload)],
        batch_id=batch_id, lifetime=lifetime)
    assert got["ok"], got
    return path


class _Reads:
    """Counts every owner-key, commitments and prewrite-record read."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.counts: collections.Counter = collections.Counter()
        for name, which in (("_key_generation", 1), ("_read_commitments", 0),
                            ("_read_prewrite", 0)):
            real = getattr(po, name)

            def counted(*args, _real=real, _name=name, _which=which, **kw):
                self.counts[(_name, str(args[_which]))] += 1
                return _real(*args, **kw)

            monkeypatch.setattr(po, name, counted)

    def of(self, name: str, subject: object) -> int:
        return self.counts[(name, str(subject))]

    def reset(self) -> None:
        self.counts.clear()


def _scope(queue, instance) -> Path:
    return po.instance_dir(queue.root, instance)


def test_a_running_producers_key_is_read_once_for_all_its_due_batches(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """#914: three consumed batches, no consumer yet, one owner-key read."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    running = _bind_owner(queue, template, fx._hexkey("running-producer"))
    paths = [_commit(queue, template, running, f"b{n}") for n in range(3)]
    owner = running["owner_action_key"]
    reads = _Reads(monkeypatch)

    assert po.origin_retirement_tick(queue) == []

    assert reads.of("_key_generation", owner) == 1
    assert all(path.exists() for path in paths), "a live producer keeps them"
    # The next cycle reads it again: a snapshot is one cycle's.
    reads.reset()
    assert po.origin_retirement_tick(queue) == []
    assert reads.of("_key_generation", owner) == 1


def test_an_ended_attempts_sweep_and_retirement_share_one_snapshot(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """#949 and #914 over one ended attempt: its key and siblings read once.

    The ended attempt has a consumed batch nobody declared (retired as an
    orphan) and a prewrite whose planned file is still on disk (swept, and
    reported orphaned).  Both ask who else names their paths.  The sibling
    is another key's live attempt of the same template, with a retained
    batch and a prewrite of its own.
    """

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    sibling = _bind_owner(queue, template, fx._hexkey("live-sibling"))
    _commit(queue, template, sibling, "kept", lifetime=RETAIN)
    elsewhere = Path(template["output_prefix"]) / "sibling-planned.bin"
    assert _prewrite(queue, sibling, template, "next", [elsewhere], 4)["ok"]
    ended = _bind_owner(queue, template, fx._hexkey("ended-attempt"))
    handoff = _commit(queue, template, ended, "b0")
    planned = Path(template["output_prefix"]) / "ended-planned.bin"
    assert _prewrite(queue, ended, template, "b1", [planned], 4)["ok"]
    planned.write_bytes(b"left")
    queue.finish(ended["owner_action_key"], status="failed")
    assert queue.item_path(pool.FAILED, ended["owner_action_key"]).exists()
    reads = _Reads(monkeypatch)

    events = po.origin_retirement_tick(queue)

    assert sorted(event["event"] for event in events) == sorted([
        po.ORIGIN_RETIRED_EVENT, "output-prewrite-orphaned"])
    assert not handoff.exists() and planned.exists()
    # Each owner key once.
    assert reads.of("_key_generation", ended["owner_action_key"]) == 1
    assert reads.of("_key_generation", sibling["owner_action_key"]) == 1
    # Each sibling record once: its commitments and its prewrite record.
    sibling_scope = _scope(queue, sibling)
    assert reads.of("_read_commitments",
                    sibling_scope / "commitments.json") == 1
    assert reads.of("_read_prewrite",
                    sibling_scope / "prewrites" / "next.prewrite.json") == 1
    # The next cycle reads each again, once.
    reads.reset()
    po.origin_retirement_tick(queue)
    assert reads.of("_key_generation", sibling["owner_action_key"]) == 1
    assert reads.of("_read_commitments",
                    sibling_scope / "commitments.json") == 1

"""R13's dead Stage A instance, replayed: its paths come back, its origin stays (#1053).

`tests/r13_1053_replay.py` files the records R13 (``03f50d8e390b``) left when
it failed at chain-042 into a queue under ``tmp_path``: 436 committed staged
batches, ten of them never retired, and 28 outstanding prewrites naming the
at-43 plane.  Every origin file the live prefix held is created under a
synthetic prefix.  Then the tier cycle's two steps run as they do on the tier
host, before and after a relaunch -- a new action key on the same template --
writes the same paths.

What each batch class must come to:

* the 392 batches the relaunch reads keep every origin file, through every
  pass, whatever else happens;
* the ten unretired batches (the eight the relaunch regenerates, and two it
  reads) are retired: stage records closed, tokens released, origin kept;
* the 28 prewrites are reported once each, with the one remedy, while the
  files they name belong to nobody; once the relaunch commits those paths
  they are reclaimed as superseded, and the relaunch's files stay;
* the 36 batches whose origin was already reclaimed are left as they are.

Nothing here reads or writes the live queue, stage or origin.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

import prismabuild.produced_output as po  # noqa: E402
import stage_release  # noqa: E402

import r13_1053_replay as r13  # noqa: E402
import test_prepaid_writer_integration as fx  # noqa: E402
from test_a_dead_producers_batches_and_paths_are_released import (  # noqa: E402
    DEAD_BATCH_RETIRED, PREWRITE_ORPHANED, PREWRITE_RECLAIMED, TIER, _events,
    _Producer,
)

SLOT = "boundary_entries"


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


class _Cycle:
    """The replayed queue, and the two tier-cycle steps #1053 changes."""

    def __init__(self, tmp_path: Path) -> None:
        # The relaunch's own window is 48 GiB on the tier.
        self.q = fx._queue(tmp_path, gib=64)
        self.stage = tmp_path / "stage"
        fx._announce_tier(self.q, self.stage)
        self.replay = r13.install(self.q, prefix=tmp_path / "origin" / "adjoint",
                                  stage=self.stage)
        self.fleet = SimpleNamespace(q=self.q, tmp=tmp_path,
                                     cas_root=tmp_path / "cas",
                                     template=self.replay.template)

    def run(self) -> list[dict]:
        """One sweep and one tick; the tick's events.

        The sweep's receipts are not the tick's, and each pass here is
        checked through the files and records it leaves.
        """

        stage_release.sweep(self.q, stage_roots={TIER: str(self.stage)},
                            pressure={TIER: 0})
        return po.origin_retirement_tick(self.q)

    def descriptor(self, producer: _Producer, path: str, payload: bytes) -> dict:
        return po.validate_descriptor({
            "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": SLOT,
            "artifact_class": "payload", "path": path, "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "producer_generation": po.mint_generation(),
            "owner_action_key": producer.owner,
            "owner_attempt": dict(producer.inst["owner_attempt"]),
        }, self.replay.template, producer.inst)

    def prewrite(self, producer: _Producer, batch_id: str,
                 paths: list[str]) -> dict:
        return po.require_prewrite(
            self.q, producer.inst, self.replay.template, batch_id=batch_id,
            tier=TIER, class_bytes={"payload": 1 << 20, "checkpoint": 0,
                                    "temp": 0},
            paths=paths)

    def regenerate(self, producer: _Producer, batch_id: str,
                   paths: list[str], *, prewritten: bool = False
                   ) -> dict[str, bytes]:
        """The relaunch's own batch over ``paths``: prewrite, rename, commit."""

        written = {path: f"relaunch {batch_id} {os.path.basename(path)}".encode()
                   for path in paths}
        if not prewritten:
            pre = self.prewrite(producer, batch_id, paths)
            assert pre.get("ok") is True, pre
        for path, payload in written.items():
            _Producer.write(Path(path), payload)
        res = po.publish_prepaid_batch(
            self.q, producer.inst, self.replay.template,
            [self.descriptor(producer, path, payload)
             for path, payload in written.items()],
            batch_id=batch_id, tier=TIER, cas_root=self.fleet.cas_root,
            producer_action_key=producer.owner, command_extra=["--unpaced"])
        assert res.get("ok") is True, res
        return written


def test_the_r13_dead_instance_replayed(tmp_path: Path, monkeypatch) -> None:
    cycle = _Cycle(tmp_path)
    replay = cycle.replay
    assert (len(replay.unretired), len(replay.colliding), len(replay.read),
            len(replay.reclaimed), len(replay.prewrites)) == (10, 8, 392, 36, 28)
    read = r13.origin_identities(replay, replay.read)
    colliding = r13.origin_identities(replay, replay.colliding)
    planned = sorted(path for paths in replay.prewrite_paths.values()
                     for path in paths if not path.endswith(".tmp"))
    assert len(planned) == 1792

    # -- before a successor writes ------------------------------------------
    events = cycle.run()

    assert not _events(events, po.ORIGIN_RETIREMENT_REFUSED_EVENT), events
    retired = _events(events, DEAD_BATCH_RETIRED)
    assert sorted(event["batch"] for event in retired) == sorted(
        replay.coordinates(batch_id) for batch_id in replay.unretired)
    for event in retired:
        assert event["origin_kept"] is True, event
        assert event["producer_state"] == "dead", event
        assert event["superseded"] == [], event
    for batch_id in replay.unretired:
        entry = replay.entry(cycle.q.root, batch_id)
        assert po._batch_stage_retired(entry), batch_id
        assert entry.get("origin_reclaimed") is False, batch_id
    orphaned = _events(events, PREWRITE_ORPHANED)
    assert sorted(event["prewrite"] for event in orphaned) == sorted(
        replay.coordinates(batch_id) for batch_id in replay.prewrites)
    for event in orphaned:
        assert event["remedy"] == po.ORPHANED_PREWRITE_REMEDY, event
        assert len(event["paths"]) == 64, event
    assert not _events(events, PREWRITE_RECLAIMED)
    listed = po.blocked_origin_batches(cycle.q)["orphaned_prewrites"]
    assert len(listed) == 28
    assert {item["remedy"] for item in listed} == {po.ORPHANED_PREWRITE_REMEDY}

    assert cycle.run() == [], "settled: a second pass reports nothing new"
    assert cycle.run() == []
    assert r13.origin_identities(replay, replay.read) == read
    assert r13.origin_identities(replay, replay.colliding) == colliding
    assert all(not os.path.lexists(path) for batch_id in replay.reclaimed
               for path in replay.paths[batch_id])

    # -- a relaunch on the same template, a new action key ------------------
    relaunch = _Producer(cycle.fleet, "r13-relaunch")
    read_commitments = po._read_commitments
    looked: list[Path] = []
    monkeypatch.setattr(po, "_read_commitments", lambda path: (
        looked.append(Path(path)), read_commitments(path))[1])
    regenerated = cycle.regenerate(
        relaunch, "relaunch-at-44",
        sorted(path for batch_id in replay.colliding
               for path in replay.paths[batch_id]))
    assert replay.scope / "commitments.json" not in looked, (
        "a dead attempt's records are never read by another action's prewrite")
    monkeypatch.setattr(po, "_read_commitments", read_commitments)

    assert cycle.run() == [], "the dead prewrites are still orphaned, unchanged"

    # Its prewrite of the at-43 plane holds the dead prewrites, quietly.
    pre = cycle.prewrite(relaunch, "relaunch-at-43", planned)
    assert pre.get("ok") is True, pre
    assert cycle.run() == []
    assert po.blocked_origin_batches(cycle.q)["orphaned_prewrites"] == []

    # It commits them: each dead prewrite is superseded, and reclaimed.
    regenerated.update(cycle.regenerate(relaunch, "relaunch-at-43", planned,
                                        prewritten=True))
    events = cycle.run()

    reclaimed = _events(events, PREWRITE_RECLAIMED)
    assert sorted(event["prewrite"] for event in reclaimed) == sorted(
        replay.coordinates(batch_id) for batch_id in replay.prewrites)
    for event in reclaimed:
        assert event["reason"] == "superseded", event
        assert len(event["superseded"]) == 64, event
        assert {(item["owner_action_key"], item["batch_id"])
                for item in event["superseded"]} == {
            (relaunch.owner, "relaunch-at-43")}, event
    assert not _events(events, PREWRITE_ORPHANED), events
    assert not _events(events, po.ORIGIN_RETIREMENT_REFUSED_EVENT), events
    assert all(not (replay.scope / "prewrites" / f"{batch_id}.prewrite.json"
                    ).exists() for batch_id in replay.prewrites)
    assert po.blocked_origin_batches(cycle.q)["orphaned_prewrites"] == []

    # Nothing the relaunch wrote, and nothing it reads, was touched.
    assert cycle.run() == []
    for path, payload in regenerated.items():
        assert Path(path).read_bytes() == payload, path
    assert r13.origin_identities(replay, replay.read) == read
    for batch_id in replay.read + replay.colliding:
        assert replay.entry(cycle.q.root, batch_id).get(
            "origin_reclaimed") is False, batch_id

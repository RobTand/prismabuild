"""A terminal-failed producer cannot repair its missing READY mover input (#1184).

All queue/CAS/origin paths are private tmp_path fixtures. No stage mover is
executed and no real mountpoint is read or removed.
"""
import json
from pathlib import Path

from prismabuild import pool, produced_output as po
from prewarm_fixture import Fleet
from test_a_dead_producers_held_mover_is_retired import _World, _isolated
from test_prepaid_writer_integration import _tier_host


def _warm(world, tmp_path):
    control = Fleet(tmp_path / "prewarm-control")
    control.queue = world.q
    control.cas_root = world.cas_root
    control.mount = Path(world.template["output_prefix"])
    return control.cycle(control.args(readers=1))


def test_terminal_failed_producer_missing_input_ends_ready_mover(tmp_path):
    world = _World(tmp_path)
    origin = Path(world.descs[0]["path"])
    origin.unlink()
    before = json.loads(world.q.item_path(pool.READY, world.mover).read_text())
    assert before["produced_output_batch"]["owner_nonce"] == world.inst["owner_attempt"]["nonce"]
    world.fail_the_producer()
    assert po._producer_attempt_state(world.q, world.inst) == "dead"

    _warm(world, tmp_path)

    assert not world.q.item_path(pool.READY, world.mover).exists(), (
        "terminal-failed owner plus missing sealed input left the mover READY")
    ending = json.loads(world.q.item_path(pool.FAILED, world.mover).read_text())
    assert ending["published_unix"] == before["published_unix"]
    assert ending["status"] == "failed"
    assert ending["detail"]["termination_reason"] == "input_dependency_failed"
    assert ending["detail"]["input_dependency"]["owner_nonce"] == before["produced_output_batch"]["owner_nonce"]
    assert ending["detail"]["input_dependency"]["path"] == str(origin)
    assert ending["detail"].get("action_returncode") is None  # never executed


def test_live_producer_missing_input_remains_ready_and_can_land(tmp_path):
    world = _World(tmp_path)
    origin = Path(world.descs[0]["path"])
    payload = origin.read_bytes()
    origin.unlink()
    assert po._producer_attempt_state(world.q, world.inst) == "live"
    _warm(world, tmp_path)
    assert world.q.item_path(pool.READY, world.mover).exists()
    assert not world.q.item_path(pool.FAILED, world.mover).exists()
    origin.write_bytes(payload)
    _warm(world, tmp_path)
    assert world.q.item_path(pool.READY, world.mover).exists()


def test_terminal_failed_producer_with_present_input_is_not_cancelled(tmp_path):
    world = _World(tmp_path)
    world.fail_the_producer()
    _warm(world, tmp_path)
    assert world.q.item_path(pool.READY, world.mover).exists()
    assert not world.q.item_path(pool.FAILED, world.mover).exists()


def test_unknown_producer_evidence_does_not_prove_death(tmp_path, monkeypatch):
    world = _World(tmp_path)
    Path(world.descs[0]["path"]).unlink()
    world.fail_the_producer()
    monkeypatch.setattr(po, "_producer_attempt_state", lambda *a, **k: "unknown")
    _warm(world, tmp_path)
    assert world.q.item_path(pool.READY, world.mover).exists()
    assert not world.q.item_path(pool.FAILED, world.mover).exists()


def test_claim_fails_the_mover_before_any_admission(tmp_path):
    """The claim path refuses the same row a warm would (#1184).

    A claim that reaches the mover without a prewarm cycle must not admit
    it either: the serialized refusal runs before the key's transition
    hold, so a mover whose dead producer's bound origin is gone is failed
    without executing anything.
    """
    world = _World(tmp_path)
    Path(world.descs[0]["path"]).unlink()
    world.fail_the_producer()
    claimed = world.q.claim(owner="w-mover", tags=[_tier_host(world.q)])
    assert claimed is None, "a dead producer's missing-input mover was admitted"
    assert not world.q.item_path(pool.READY, world.mover).exists()
    ending = json.loads(world.q.item_path(pool.FAILED, world.mover).read_text())
    assert ending["status"] == "failed"
    assert ending["detail"]["termination_reason"] == "input_dependency_failed"
    assert ending["detail"].get("action_returncode") is None  # never executed

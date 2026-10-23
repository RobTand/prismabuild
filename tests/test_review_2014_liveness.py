"""Independent review reproductions; isolated queues only."""
import json
import math
import sys
from threading import Event

import pytest

from prismabuild import pool
from prismabuild import capability_eligibility as eligibility
from test_unknown_cpu_interpreter_placement import (
    interpreter_case, prepare_interpreter_claim,
)
from test_a_draining_worker_stays_visible import _load_loop, DRAIN_GATE


@pytest.mark.parametrize("count", [33, 65, 97])
def test_every_requested_interpreter_eventually_gets_an_answer(tmp_path, monkeypatch, count):
    queue, item, candidate, placement, _ = interpreter_case(
        tmp_path, monkeypatch, "unknown", reserved=True)
    target = item["interpreter"]
    # The CPU peer lacks this interpreter. The remaining paths belong to rows
    # that can remain READY because their tags or demand exclude the peer.
    rows = [item] + [
        {"interpreter": target + f".zz-{index:02d}",
         "tags": ["gpu-only"], "resources": {"gpu": 1}, "priority": 100}
        for index in range(count - 1)
    ]
    monkeypatch.setattr(eligibility.LiveInterpreters, "answer",
                        lambda self, path: eligibility.Presence.ABSENT)
    cpu_offer = next(o for o in placement.offers() if o["host"] == "cpu-peer")
    assert prepare_interpreter_claim(queue, item, candidate, placement).reason == (
        "deferred_for_unresolved_cpu_host")
    seen = set()
    resolved = False
    for _ in range(math.ceil(count / eligibility.INTERPRETER_INVENTORY_LIMIT)):
        answers = eligibility.observe_interpreters(
            rows, inventory_path=tmp_path / "inventory.json")
        assert len(answers.present | answers.absent) <= eligibility.INTERPRETER_INVENTORY_LIMIT
        seen.update(answers.present | answers.absent)
        cpu_offer.update(answers.offer_fields())
        if answers.answer(target) is not eligibility.Presence.UNKNOWN:
            result = prepare_interpreter_claim(queue, item, candidate, placement)
            assert isinstance(result, pool._HostClaimRequest)
            assert not result.cpu_guest
            resolved = True
        else:
            assert prepare_interpreter_claim(queue, item, candidate, placement).reason == (
                "deferred_for_unresolved_cpu_host")
        # Empty and smaller sibling snapshots share the saved rotation. Neither
        # may reset progress through the stable, overflowing READY snapshot.
        for sibling in ([], [rows[-1]]):
            sibling_answers = eligibility.observe_interpreters(
                sibling, inventory_path=tmp_path / "inventory.json")
            assert len(sibling_answers.present | sibling_answers.absent) <= (
                eligibility.INTERPRETER_INVENTORY_LIMIT)
    assert seen == {row["interpreter"] for row in rows}
    assert resolved


def test_a_draining_host_cannot_hold_an_unresolved_wait(tmp_path, monkeypatch):
    queue, item, candidate, placement, _ = interpreter_case(
        tmp_path, monkeypatch, "unknown", reserved=True)
    parked = tmp_path / "parked"
    parked.mkdir()
    loop = _load_loop(parked, monkeypatch, DRAIN_GATE,
                     extra_args=("--cpu-slots", "8", "--mem-gb", "16"))
    monkeypatch.setattr(loop.socket, "gethostname", lambda: "cpu-peer")
    assert loop._run_loop(Event()) == 0
    drained_offer = loop.pool.PoolQueue(parked / "pb-queue")._offer_records()[0]
    assert drained_offer["state"] == "draining"
    assert not any(drained_offer["observed_capacity"].values())
    assert eligibility.InterpreterAnswers.from_offer(drained_offer).answer(sys.executable) is (
        eligibility.Presence.UNKNOWN)
    cpu_offer = next(o for o in placement.offers() if o["host"] == "cpu-peer")
    cpu_offer.clear()
    cpu_offer.update(drained_offer)
    monkeypatch.setattr(loop.socket, "gethostname", lambda: "spark")
    assert queue._defer_to_cpu_host(
        item, candidate.demand.reservation, has_gpu=True, placement=placement) is None


def test_drain_publications_retain_and_restat_open_poll_interpreters(tmp_path, monkeypatch):
    python = tmp_path / "python"
    python.write_text("#!/bin/sh\n")
    python.chmod(0o700)
    path = str(python)
    loop = _load_loop(tmp_path, monkeypatch, {"draining": False}, once=False)
    monkeypatch.setattr(loop, "discover_ready_snapshot", lambda *a, **k:
        loop.DiscoveryResult("ready", [{"interpreter": path}], None, ""))
    monkeypatch.setattr(loop.pool.PoolQueue, "serve_once", lambda *a, **k: None)
    real_publish = loop.publish_offer
    stop = Event()
    observed = []

    def publish(announce, **kwargs):
        result = real_publish(announce, **kwargs)
        assert result.status == "published"
        offer = loop.pool.PoolQueue(tmp_path / "pb-queue")._offer_records()[0]
        observed.append((offer.get("state"),
            eligibility.InterpreterAnswers.from_offer(offer).answer(path)))
        if len(observed) == 1:
            (tmp_path / "maintenance.json").write_text(json.dumps(DRAIN_GATE))
        elif len(observed) == 2:
            python.unlink()
        else:
            stop.set()
        return result

    monkeypatch.setattr(loop, "publish_offer", publish)
    assert loop._run_loop(stop) == 0
    assert observed == [(None, eligibility.Presence.PRESENT),
                        ("draining", eligibility.Presence.PRESENT),
                        ("draining", eligibility.Presence.ABSENT)]

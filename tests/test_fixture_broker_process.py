"""The private broker owner keeps service threads outside the calling test."""
import multiprocessing
import os
from pathlib import Path

import pytest

import output_producer_fixture as producer
from fleet_fixture import actions, broker, outputs, queue  # noqa: F401
from prismabuild import pool

TIER = "prismabuild-stage:dl380g10"


def _never_ready(_socket, _state, _uid, control):
    try:
        while os.read(control.fileno(), 1):
            pass
    finally:
        control.close()


def test_unused_broker_is_lazy_and_close_is_repeatable(broker):
    assert broker.process is None
    assert broker.directory is None
    assert broker.socket is None
    broker.close()
    broker.close()
    assert broker.process is None


def test_real_broker_inherits_affinity_and_releases_before_exit(outputs, broker):
    batch = outputs.fund_ready(tier=TIER, owner_seed="process-owner", mover_seed="process-mover")
    assert batch.funded["ok"] is True
    process = broker.process
    pid = process.pid
    directory = Path(broker.directory.name)
    assert os.sched_getaffinity(pid) == os.sched_getaffinity(0)
    assert broker.socket.is_socket()
    broker.close()
    assert pid not in {child.pid for child in multiprocessing.active_children()}
    assert not directory.exists()
    broker.close()


def test_socket_bind_damage_refuses_before_scope_creation(queue, actions, broker, monkeypatch):
    """An occupied socket name must surface startup failure and leave no child."""
    original = producer.tempfile.TemporaryDirectory
    allocated = []

    def occupied(*args, **kwargs):
        directory = original(*args, **kwargs)
        path = Path(directory.name)
        allocated.append(path)
        (path / "broker.sock").write_text("occupied socket name", encoding="utf-8")
        return directory

    monkeypatch.setattr(producer.tempfile, "TemporaryDirectory", occupied)
    before = {child.pid for child in multiprocessing.active_children()}
    claimed = actions.claim_new(actions.key())
    with pytest.raises(RuntimeError, match="private broker startup failed"):
        broker.scope_for_claim(queue, claimed)
    assert allocated and all(not path.exists() for path in allocated)
    assert {child.pid for child in multiprocessing.active_children()} == before
    assert broker.process is None
    live = pool.read_queue_record(queue.records.item_path(pool.record_store.CLAIMED, claimed["action_key"]))
    assert live is not None and live.get("resource_scope") is None


def test_missing_readiness_times_out_and_reaps_child(queue, actions, broker, monkeypatch):
    """A child that never signals ready or obeys stop cannot hold the test."""
    monkeypatch.setattr(producer, "_run_private_broker", _never_ready)
    monkeypatch.setattr(producer, "BROKER_STARTUP_S", 0.25)
    monkeypatch.setattr(producer, "BROKER_STOP_S", 0.25)
    before = {child.pid for child in multiprocessing.active_children()}
    claimed = actions.claim_new(actions.key())
    with pytest.raises(TimeoutError, match="private broker startup timed out") as failure:
        broker.scope_for_claim(queue, claimed)
    assert any("forced=True" in note for note in failure.value.__notes__)
    assert broker.process is None
    assert broker.directory is None
    assert {child.pid for child in multiprocessing.active_children()} == before


def test_scope_teardown_error_preserved_while_child_is_reaped(outputs, broker, monkeypatch):
    outputs.fund_ready(tier=TIER, owner_seed="stop-error-owner", mover_seed="stop-error-mover")
    pid = broker.process.pid
    directory = Path(broker.directory.name)
    original = ValueError("injected scope stop failure")

    def fail_stop(_reason):
        raise original

    monkeypatch.setattr(broker.scopes[0], "terminate_owned", fail_stop)
    with pytest.raises(ValueError) as failure:
        broker.close()
    assert failure.value is original
    assert pid not in {child.pid for child in multiprocessing.active_children()}
    assert not directory.exists()
    broker.close()

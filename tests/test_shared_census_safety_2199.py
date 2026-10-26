"""Shared reuse cannot erase pass authority or retain H on optional source I/O."""
from __future__ import annotations

import json
import os
import signal
import socket
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from contextlib import ExitStack, contextmanager, suppress

import pytest

from prismabuild import _bounded_reader as reader
from prismabuild import _measurement_reservation as reservation, adaptive_cpu, pool

from test_census_tmpfs_state_1451 import tmpfs_mount, tmpfs_state  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from test_ordinary_census_admission_2199 import _fixture_selection


@pytest.mark.parametrize("state", ["ready", "missing", "busy", "holder"])
def test_sibling_publication_cannot_retire_a_remembered_selection(
        fleet, monkeypatch, state):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    monkeypatch.setattr(reservation, "SHARED_CENSUS_MAX_AGE_S", 60)
    key = publish("remembered-before-sibling", measurement=True, priority=10)
    chosen = _fixture_selection(queue, key)
    candidate = publish("ordinary-after-sibling", priority=-10)
    item = pool.record_store.value_or_raise(queue.records.item(pool.READY, candidate))
    ledger = queue.ledger()
    gate = adaptive_cpu.AdmissionGate(ledger)
    saved = reservation.PassCensus(queue, ledger, gate)
    with reservation.locked_census(queue, ledger, gate) as census:
        saved.store(census)
        assert census["elections"][key] == chosen
    queue.records.passes_path(key).unlink()
    if state != "ready":
        queue.records.item_path(pool.READY, key).unlink()  # no retirement authority
    if state == "holder":
        ledger.ensure_capacity({"cpu": 20})
        assert ledger.acquire(key, {"cpu": 1})
    with reservation.locked_census(queue, ledger, gate) as sibling:
        assert sibling["selections"] == {}
    assert reservation.CensusReader(queue, ledger).shared_census()["census"]["selections"] == {}

    events = []
    attempts = {}
    real_transition = queue.action_transition_lock
    real_locked = gate.locked

    @contextmanager
    def transition(action_key, **kwargs):
        events.append(("M", action_key))
        with real_transition(action_key, **kwargs) as acquired:
            attempts[action_key] = acquired
            yield acquired

    @contextmanager
    def admission():
        events.append(("H", None))
        with real_locked() as snapshot:
            yield snapshot

    def check():
        with reservation.candidate_admission_census(
                saved, queue, ledger, gate, item, dependent_owner=None) as current:
            assert "unavailable" not in current
            assert current["selections"][key] == chosen
            assert key in current["keys"]
            assert reservation.blocking_selection(
                current, item, host=ledger.base.name, funded_by=None) == chosen
            assert events.index(("M", key)) < events.index(("H", None))
            assert attempts[key] is (state != "busy")

    with ExitStack() as stack:
        if state == "busy":
            assert stack.enter_context(real_transition(key, blocking=False))
        monkeypatch.setattr(queue, "action_transition_lock", transition)
        monkeypatch.setattr(gate, "locked", admission)
        if state == "busy":
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(check).result(timeout=10)
        else:
            check()


def test_busy_optional_source_guard_does_not_take_the_census_fence(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    key = publish("busy-source-live-election", measurement=True, priority=10)
    chosen = _fixture_selection(queue, key)
    ledger = queue.ledger()
    gate = adaptive_cpu.AdmissionGate(ledger)
    observer = reservation.CensusReader(queue, ledger)

    def unexpected_sources(*args):
        pytest.fail("a busy source owner must not launch another helper")

    # Observe the spawn boundary in the parent: an assertion inside the optional
    # child would be translated into an unavailable result and could go unnoticed.
    monkeypatch.setattr(reservation._SourceGenerationReader, "_spawn_owned", unexpected_sources)
    with reservation._SourceGenerationReader(observer).held():
        with reservation.locked_census(queue, ledger, gate) as census:
            assert census["elections"][key] == chosen
        assert observer.shared_census() is None
    assert _take_host_gate(ledger)


def _take_host_gate(ledger):
    try:
        with adaptive_cpu.AdmissionGate(ledger).locked():
            return True
    except adaptive_cpu.AdmissionBusy:
        return False


@pytest.mark.parametrize("phase", ["before", "after", "validation"])
def test_stalled_optional_source_read_is_bounded_and_restart_owned(
        fleet, tmpfs_state, monkeypatch, phase):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmpfs_state / "box-state")
    monkeypatch.setattr(reservation, "SHARED_CENSUS_MAX_AGE_S", 60)
    ledger = queue.ledger()
    gate = adaptive_cpu.AdmissionGate(ledger)
    saved = reservation.PassCensus(queue, ledger, gate)
    observer = reservation.CensusReader(queue, ledger)
    if phase == "validation":
        # An empty cache must not authorize admission after a new election.
        with reservation.locked_census(queue, ledger, gate) as census:
            assert census["selections"] == {}
    key = publish("source-read-live-election", measurement=True, priority=10)
    chosen = _fixture_selection(queue, key)
    candidate = publish("source-read-ordinary", priority=-10)
    item = pool.record_store.value_or_raise(queue.records.item(pool.READY, candidate))
    monkeypatch.setattr(reservation, "REFRESH_BUDGET_S", 0.3)
    # Both ordinary and shared shortcuts must decline, so the authoritative
    # fallback has to find the election after optional verification times out.
    monkeypatch.setattr(reservation, "_ordinary_election_census", lambda *args: None)
    endpoint = tmpfs_state / "sources.sock"
    real_generation = reservation._admission_source_generation

    def parked_generation(active_queue, census):
        first_read = endpoint.with_suffix(".first")
        if phase == "after" and not first_read.exists():
            first_read.touch()
            return real_generation(active_queue, census)
        try:
            descriptor = os.open(endpoint.with_suffix(".seen"), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            return real_generation(active_queue, census)
        os.close(descriptor)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.connect(str(endpoint))
            connection.sendall(str(os.getpid()).encode() + b"\n")
            assert connection.recv(1) == b"R"
        return real_generation(active_queue, census)

    def retain(pid, pidfd, ownership, section, started, abandoned, pool_identity):
        # Model an uninterruptible read: timeout does not establish settlement.
        abandoned.append({"ownership": ownership.to_record()})

    monkeypatch.setattr(reservation, "_admission_source_generation", parked_generation)
    monkeypatch.setattr(reader, "_stop_owned_reader", retain)

    def acquire():
        boundary = (reservation.candidate_admission_census(saved, queue, ledger, gate, item, dependent_owner=None)
                    if phase == "validation" else reservation.locked_census(queue, ledger, gate))
        with boundary as census:
            assert "unavailable" not in census
            return census

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(str(endpoint))
        server.listen(1)
        server.settimeout(10)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(acquire)
            connection, _ = server.accept()
            connection.settimeout(10)
            caller = int(connection.recv(64).strip())
            ownership = None
            try:
                try:
                    current = future.result(timeout=5)
                except TimeoutError:
                    pytest.fail("optional shared-source I/O retained host admission H")
                assert caller != os.getpid(), "shared source reads must run in an owned child"
                assert current["elections"][key] == chosen
                assert reservation.blocking_selection(current, item, host=ledger.base.name, funded_by=None) == chosen
                assert executor.submit(_take_host_gate, ledger).result(timeout=5)
                with queue.action_transition_lock(key, blocking=False) as acquired:
                    assert acquired, "the unresolved source child must not inherit M"
                # A distinct owner stays fenced across restarts without taking
                # the main census guard or preventing authoritative discovery.
                source_owner = reservation._SourceGenerationReader(observer)
                marker = source_owner.directory / source_owner.name
                raw = marker.read_bytes()
                ownership = reader.ReaderOwnership.from_record(json.loads(raw))
                assert ownership.pid == caller
                assert reader.reader_liveness(
                    ownership, pool_identity=observer.pool_identity,
                    section=ownership.section) == "running"
                with pytest.raises(reservation.CensusUnavailable, match="retained"):
                    reservation.CensusReader(queue, ledger).source_generation(current)
                assert marker.read_bytes() == raw
                assert reservation.CensusReader(queue, ledger).capture()["selections"][key] == chosen
                if phase != "validation":
                    assert observer.shared_census() is None, "unverified sources cannot be published"
            finally:
                with suppress(OSError):
                    connection.sendall(b"R")
                connection.close()
                if caller != os.getpid():
                    # The socket was opened by this test's owned callback.
                    with suppress(ProcessLookupError):
                        os.kill(caller, signal.SIGKILL)
                    with suppress(ChildProcessError):
                        os.waitpid(caller, 0)
                # Releases the original inline implementation on a regression,
                # before the executor joins, rather than leaving a stuck test.
                future.result(timeout=10)

"""Shared reuse cannot erase pass authority or retain H on optional source I/O."""
from __future__ import annotations

import json
import os
import signal
import socket
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager, suppress
from contextvars import ContextVar
from types import SimpleNamespace

import pytest

from prismabuild import _bounded_reader as reader
from prismabuild import _measurement_reservation as reservation, adaptive_cpu, pool

from test_census_tmpfs_state_1451 import tmpfs_mount, tmpfs_state  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from test_ordinary_census_admission_2199 import _fixture_selection


@pytest.fixture
def shared_age_clock(monkeypatch):
    # Only cache age uses this clock; bounded readers keep their real deadlines.
    monkeypatch.setattr(reservation, "time", SimpleNamespace(
        monotonic=lambda: 100.0, time=reservation.time.time, sleep=reservation.time.sleep))


@pytest.mark.parametrize("cache", ["stale", "sibling-omission"])
def test_one_claim_pass_remembers_an_election_first_seen_in_shared_reuse(
        fleet, monkeypatch, cache, shared_age_clock):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("shared-first-election", measurement=True, priority=10, gpu=0)
    chosen = _fixture_selection(queue, selected)
    candidates = [publish(f"shared-first-follower-{i}", priority=-10, gpu=0) for i in range(2)]
    rows = [pool.record_store.value_or_raise(queue.records.item(pool.READY, key))
            for key in candidates]
    ledger = queue.ledger()
    gate = adaptive_cpu.AdmissionGate(ledger)
    with reservation.locked_census(queue, ledger, gate) as census:
        assert census["elections"][selected] == chosen

    seen = []
    owned_reads = []
    original_candidate = reservation.candidate_admission_census
    original_owned = reservation.pass_admission_census

    @contextmanager
    def observe_owned(*args, **kwargs):
        owned_reads.append(len(seen))
        with original_owned(*args, **kwargs) as current:
            yield current

    @contextmanager
    def lose_sidecar_between_candidates(saved, active, host_ledger, controller, item, *args, **kwargs):
        with original_candidate(saved, active, host_ledger, controller, item, *args, **kwargs) as current:
            seen.append((item["action_key"], current.get("elections", {}).get(selected)))
            yield current
        if item["action_key"] == candidates[0]:
            assert owned_reads == [], "the first observation must come from shared reuse"
            # No retirement record: the exact elected measurement is still READY.
            queue.records.passes_path(selected).unlink()
            if cache == "sibling-omission":
                with reservation.locked_census(queue, ledger, gate) as sibling:
                    assert sibling["selections"] == {}

    monkeypatch.setattr(reservation, "pass_admission_census", observe_owned)
    monkeypatch.setattr(reservation, "candidate_admission_census", lose_sidecar_between_candidates)
    result = queue.claim(capacity={"cpu": 20, "mem_gb": 120}, ready=rows)

    assert result is None, "a later candidate undercut an election learned through shared reuse"
    assert seen == [(key, chosen) for key in candidates]
    assert owned_reads == ([1] if cache == "stale" else [])
    for key in candidates:
        refusal = denial(key)
        assert refusal["reason"] == "deferred_for_measurement_reservation"
        assert refusal["evidence"]["withheld_for"] == selected
    assert all(queue.records.item_path(pool.READY, key).is_file()
               for key in [selected, *candidates])
    assert queue.ledger().held_keys() == []


@pytest.mark.parametrize("sibling_cache", [False, True])
def test_busy_discovery_keeps_its_election_across_later_candidates(
        fleet, monkeypatch, shared_age_clock, sibling_cache):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("busy-discovery-election", measurement=True, priority=10, gpu=0)
    chosen = _fixture_selection(queue, selected)
    candidates = [publish(f"busy-discovery-follower-{i}", priority=-10, gpu=0)
                  for i in range(2)]
    rows = [pool.record_store.value_or_raise(queue.records.item(pool.READY, key))
            for key in candidates]
    ledger = queue.ledger()
    gate = adaptive_cpu.AdmissionGate(ledger)
    original_capture = reservation.CensusReader.capture
    original_candidate = reservation.candidate_admission_census
    captures = []
    seen = []

    def lose_sidecar_after_discovery(reader):
        current = original_capture(reader)
        captures.append(current)
        if len(captures) == 1:
            assert current["elections"][selected] == chosen
            queue.records.passes_path(selected).unlink()
        return current

    @contextmanager
    def observe_candidate(*args, **kwargs):
        with original_candidate(*args, **kwargs) as current:
            seen.append(current.get("elections", {}).get(selected))
            yield current
        if len(seen) == 1 and sibling_cache:
            with reservation.locked_census(queue, ledger, gate) as sibling:
                assert sibling["selections"] == {}

    monkeypatch.setattr(reservation.CensusReader, "capture", lose_sidecar_after_discovery)
    monkeypatch.setattr(reservation, "candidate_admission_census", observe_candidate)
    with queue.action_transition_lock(selected, blocking=False) as acquired:
        assert acquired
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = executor.submit(
                queue.claim, capacity={"cpu": 20, "mem_gb": 120}, ready=rows).result(timeout=10)

    assert result is None, "a later candidate undercut the busy-discovery election"
    assert seen == [chosen, chosen]
    for key in candidates:
        refusal = denial(key)
        assert refusal["reason"] == "deferred_for_measurement_reservation"
        assert refusal["evidence"]["withheld_for"] == selected
    assert queue.records.item_path(pool.READY, selected).is_file()
    assert ledger.held_keys() == []


def test_an_ordinary_negative_proof_keeps_elections_learned_from_shared_reuse(
        fleet, shared_age_clock):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("shared-negative-election", measurement=True, priority=10, gpu=0)
    chosen = _fixture_selection(queue, selected)
    higher = publish("shared-negative-higher", priority=20, gpu=0)
    lower = publish("shared-negative-lower", priority=-10, gpu=0)
    high_row, low_row = [pool.record_store.value_or_raise(queue.records.item(pool.READY, key))
                         for key in (higher, lower)]
    ledger = queue.ledger()
    gate = adaptive_cpu.AdmissionGate(ledger)
    with reservation.locked_census(queue, ledger, gate):
        pass
    saved = reservation.PassCensus(queue, ledger, gate)
    with reservation.candidate_admission_census(
            saved, queue, ledger, gate, low_row, dependent_owner=None) as first:
        assert reservation.blocking_selection(
            first, low_row, host="sparklina", funded_by=None) == chosen
    queue.records.passes_path(selected).unlink()
    with reservation.candidate_admission_census(
            saved, queue, ledger, gate, high_row, dependent_owner=None) as irrelevant:
        assert irrelevant["elections"] == {}
    with reservation.candidate_admission_census(
            saved, queue, ledger, gate, low_row, dependent_owner=None) as later:
        assert reservation.blocking_selection(
            later, low_row, host="sparklina", funded_by=None) == chosen


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

    class SourceDeadline(reader.Deadline):
        def remaining(self):
            return self.timeout_s

    in_source = ContextVar("in_source", default=False)
    real_spawn = reservation._SourceGenerationReader._spawn_owned
    real_select = reader.select.select

    def controlled_source(self, *args, **kwargs):
        token = in_source.set(True)
        try:
            return real_spawn(self, *args, **kwargs)
        finally:
            in_source.reset(token)

    def controlled_select(ready, write, error, timeout):
        if not in_source.get():
            return real_select(ready, write, error)
        readable, writable, errors = real_select([*ready, expiry_read], write, error)
        replies = [descriptor for descriptor in ready if descriptor in readable]
        if replies:
            return replies, writable, errors
        os.read(expiry_read, 1)
        return [], [], []

    with ExitStack() as handles, socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        expiry_read, expiry_write = os.pipe()
        handles.callback(os.close, expiry_read)
        handles.callback(os.close, expiry_write)
        monkeypatch.setattr(reader, "Deadline", SourceDeadline)
        monkeypatch.setattr(reader, "select", SimpleNamespace(select=controlled_select))
        monkeypatch.setattr(reservation._SourceGenerationReader, "_spawn_owned", controlled_source)
        server.bind(str(endpoint))
        server.listen(1)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(acquire)
            connection, _ = server.accept()
            caller = int(connection.recv(64).strip())
            ownership = None
            try:
                assert caller != os.getpid(), "shared source reads must run in an owned child"
                os.write(expiry_write, b"E")
                current = future.result()
                assert current["elections"][key] == chosen
                assert reservation.blocking_selection(current, item, host=ledger.base.name, funded_by=None) == chosen
                assert executor.submit(_take_host_gate, ledger).result()
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
                future.result()

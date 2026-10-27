"""A stalled ordinary proof cannot retain host admission or lose reader custody."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
import json
import os
import signal
import socket
import threading
from types import SimpleNamespace

import pytest

from prismabuild import _bounded_reader as reader
from prismabuild import _measurement_reservation as reservation, adaptive_cpu, pool
from test_census_tmpfs_state_1451 import tmpfs_mount, tmpfs_state  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from test_ordinary_census_admission_2199 import _fixture_selection


@pytest.mark.parametrize("phase", ["discovery", "recheck", "claimed", "retirement", "gang"])
def test_stalled_ordinary_proof_releases_locks_and_preserves_reader_custody(
        fleet, tmpfs_state, monkeypatch, phase):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmpfs_state / "box-state")
    selected = publish("ordinary-io-retired", measurement=True, priority=10, gpu=0)
    chosen = _fixture_selection(queue, selected)
    queue.withdraw(selected)
    candidate = publish("ordinary-io-follower", gpu=0)
    item = pool.record_store.value_or_raise(queue.records.item(pool.READY, candidate))
    ledger = queue.ledger()
    gate = adaptive_cpu.AdmissionGate(ledger)
    endpoint = tmpfs_state / "ordinary.sock"
    accepted = threading.Event()
    real_sources = reservation._ordinary_selection_sources
    real_scandir, real_read = os.scandir, reservation._read
    real_bounded, real_select = reader.bounded, reader.select.select
    real_stop = reader._stop_owned_reader
    source_phase = None
    expire_wait = False
    source_calls = 0

    def park():
        try:
            descriptor = os.open(endpoint.with_suffix(".seen"),
                                 os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            return
        os.close(descriptor)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.connect(str(endpoint))
            connection.sendall(str(os.getpid()).encode() + b"\n")
            assert connection.recv(1) == b"R"

    def sources(active_queue, cached, *, recheck=False):
        nonlocal source_phase
        source_phase = "recheck" if recheck else "discovery"
        try:
            return real_sources(active_queue, cached, recheck=recheck)
        finally:
            source_phase = None

    def scandir(path):
        if ((phase == source_phase and path == queue.root / pool.record_store.PASSES)
                or (phase == "claimed" and path == queue.root / pool.CLAIMED)
                or (phase == "gang" and path == queue.root / "gangs")):
            park()
        return real_scandir(path)

    def read(path, **kwargs):
        if phase == "retirement" and path == queue.records.item_path(pool.WITHDRAWN, selected):
            park()
        return real_read(path, **kwargs)

    def bounded(section, read, **kwargs):
        nonlocal expire_wait, source_calls
        if section == "ordinary-selection-sources-v1":
            source_calls += 1
        expire_wait = ((section == "ordinary-selection-sources-v1"
                        and source_calls == (2 if phase == "recheck" else 1)
                        and phase in {"discovery", "recheck"})
                       or (section == "ordinary-election-proof-v1"
                           and phase in {"claimed", "retirement", "gang"}))
        try:
            kwargs["deadline"] = reader.Deadline(60)
            return real_bounded(section, read, **kwargs)
        finally:
            expire_wait = False

    def select(reads, writes, errors, timeout):
        if expire_wait:
            assert accepted.wait(30), "ordinary reader never reached the stalled syscall"
            return [], [], []
        return real_select(reads, writes, errors, timeout)

    def stop(pid, pidfd, ownership, section, started, abandoned, pool_identity):
        if expire_wait:
            abandoned.append({"ownership": ownership.to_record()})
        else:
            real_stop(pid, pidfd, ownership, section, started, abandoned, pool_identity)

    monkeypatch.setattr(reservation, "_ordinary_selection_sources", sources)
    monkeypatch.setattr(os, "scandir", scandir)
    monkeypatch.setattr(reservation, "_read", read)
    monkeypatch.setattr(reader, "bounded", bounded)
    monkeypatch.setattr(reader, "select", SimpleNamespace(select=select))
    monkeypatch.setattr(reader, "_stop_owned_reader", stop)
    monkeypatch.setattr(reservation, "REFRESH_BUDGET_S", 60)

    def acquire():
        saved = reservation.PassCensus(queue, ledger, gate)
        with reservation.candidate_admission_census(
                saved, queue, ledger, gate, item, dependent_owner=None) as census:
            return census

    def take_host_gate():
        with adaptive_cpu.AdmissionGate(ledger).locked():
            return True

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(str(endpoint))
        server.listen(1)
        server.settimeout(30)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(acquire)
            connection, _ = server.accept()
            connection.settimeout(30)
            caller = int(connection.recv(64).strip())
            accepted.set()
            try:
                try:
                    current = future.result(timeout=10)
                except TimeoutError:
                    pytest.fail("stalled ordinary proof I/O retained the admission caller")
                assert caller != os.getpid(), "ordinary proof reads must run in an owned child"
                assert selected not in current["elections"]
                assert executor.submit(take_host_gate).result(timeout=10)
                with queue.action_transition_lock(selected, blocking=False) as acquired:
                    assert acquired, "the unresolved ordinary child inherited M"
                observer = reservation.CensusReader(queue, ledger)
                marker = observer.directory / (observer.name + ".ordinary")
                raw = marker.read_bytes()
                ownership = reader.ReaderOwnership.from_record(json.loads(raw))
                assert ownership.pid == caller
                assert reader.reader_liveness(
                    ownership, pool_identity=observer.pool_identity,
                    section=ownership.section) == "running"
                # A restart must use the independent full reader, without
                # replacing the unresolved ordinary reader's ownership.
                assert executor.submit(acquire).result(timeout=10)["selections"][selected] == chosen
                assert marker.read_bytes() == raw
                assert observer.capture()["selections"][selected] == chosen
            finally:
                with suppress(OSError):
                    connection.sendall(b"R")
                connection.close()
                if caller != os.getpid():
                    with suppress(ProcessLookupError):
                        os.kill(caller, signal.SIGKILL)
                    with suppress(ChildProcessError):
                        os.waitpid(caller, 0)
                future.result(timeout=30)

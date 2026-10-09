"""A reused census must preserve the guard and exact reader ownership."""
from __future__ import annotations

import json
import os
import select
import signal
import socket
from contextlib import suppress

import pytest

from prismabuild import _bounded_reader as reader
from prismabuild import _measurement_reservation as reservation, adaptive_cpu

from test_census_tmpfs_state_1451 import tmpfs_mount, tmpfs_state  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401

TIERS = {"preferred": list(range(20)), "fallback": []}


def _saved_pass(queue):
    controller = adaptive_cpu.Controller(queue.ledger(), TIERS)
    saved = reservation.PassCensus(queue, queue.ledger(), controller)
    with reservation.locked_census(queue, queue.ledger(), controller) as census:
        saved.store(census)
    return saved


def _blocked_read(server_path):
    # The child opens its own descriptor after bounded-reader FD isolation.
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.connect(str(server_path))
        assert connection.recv(1) == b"R"


def test_discovery_excludes_reuse_before_transition_and_admission_locks(
        fleet, tmpfs_state, monkeypatch):
    queue, *_ = fleet
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmpfs_state / "box-state")
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0.05)
    saved = _saved_pass(queue)
    census_reader = reservation.CensusReader(queue, queue.ledger())
    marker = census_reader.directory / census_reader.name
    server_path = tmpfs_state / "discovery.sock"
    real_capture = reservation._capture

    def parked_capture(active):
        _blocked_read(server_path)
        return real_capture(active)

    monkeypatch.setattr(reservation, "_capture", parked_capture)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(str(server_path))
        server.listen(1)
        server.settimeout(5)
        report_read, report_write = os.pipe()
        observer = os.fork()
        if observer == 0:
            os.close(report_read)
            server.close()
            try:
                census_reader.capture()
                os.write(report_write, b"ok")
            except BaseException:
                os.write(report_write, b"error")
            finally:
                os._exit(0)
        os.close(report_write)
        connection = None
        stack = None
        try:
            connection, _ = server.accept()
            original = marker.read_bytes()
            ownership = reader.ReaderOwnership.from_record(json.loads(original))
            assert ownership.section == reservation.READ_SECTION
            assert reader.reader_liveness(
                ownership, pool_identity=census_reader.pool_identity,
                section=ownership.section) == "running"
            locks = []
            real_transition = queue._transition_locked
            real_admission = queue._admission_lock

            def transition(*args, **kwargs):
                locks.append("transition")
                return real_transition(*args, **kwargs)

            def admission(*args, **kwargs):
                locks.append("admission")
                return real_admission(*args, **kwargs)

            monkeypatch.setattr(queue, "_transition_locked", transition)
            monkeypatch.setattr(queue, "_admission_lock", admission)
            with pytest.raises(reservation.CensusUnavailable, match="reader busy"):
                stack = saved.acquire()
            assert locks == [], "the guard must precede M and H"
            assert marker.read_bytes() == original
        finally:
            if stack is not None:
                stack.close()
            if connection is not None:
                with suppress(OSError):
                    connection.sendall(b"R")
                connection.close()
            try:
                assert select.select([report_read], [], [], 5)[0]
                assert os.read(report_read, 16) == b"ok"
            finally:
                os.close(report_read)
                waited, _ = os.waitpid(observer, os.WNOHANG)
                if not waited:
                    with suppress(ProcessLookupError):
                        os.kill(observer, signal.SIGKILL)
                    os.waitpid(observer, 0)


def test_a_retained_refresh_reader_keeps_its_marker_and_refuses_all_readers(
        fleet, tmpfs_state, monkeypatch):
    queue, *_ = fleet
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmpfs_state / "box-state")
    monkeypatch.setattr(reservation, "REFRESH_BUDGET_S", 0.2)
    saved = _saved_pass(queue)
    census_reader = reservation.CensusReader(queue, queue.ledger())
    marker = census_reader.directory / census_reader.name
    server_path = tmpfs_state / "retained.sock"

    def parked_refresh(_queue):
        _blocked_read(server_path)
        return {"selections": {}, "gang_elections": {}}

    def retain(pid, pidfd, ownership, section, started, abandoned, pool_identity):
        # Model a child whose stop did not settle. Keep the actual child live.
        abandoned.append({"ownership": ownership.to_record()})

    real_refresh = reservation._scan_election_refresh
    monkeypatch.setattr(reservation, "_scan_election_refresh", parked_refresh)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(str(server_path))
        server.listen(1)
        server.settimeout(5)
        with monkeypatch.context() as stop:
            stop.setattr(reader, "_stop_owned_reader", retain)
            with census_reader.held():
                with pytest.raises(reservation.CensusUnavailable, match="timed_out"):
                    census_reader.refresh_elections()
        connection, _ = server.accept()
        original = marker.read_bytes()
        ownership = reader.ReaderOwnership.from_record(json.loads(original))
        pidfd = os.pidfd_open(ownership.pid)
        stack = None
        try:
            assert ownership.section == reservation.REFRESH_SECTION
            assert reader.reader_liveness(
                ownership, pool_identity=census_reader.pool_identity,
                section=ownership.section) == "running"
            with pytest.raises(reservation.CensusUnavailable, match="retained"):
                reservation.CensusReader(queue, queue.ledger()).capture()
            assert marker.read_bytes() == original
            with pytest.raises(reservation.CensusUnavailable, match="retained"):
                stack = saved.acquire()
            assert marker.read_bytes() == original
        finally:
            if stack is not None:
                stack.close()
            signal.pidfd_send_signal(pidfd, signal.SIGKILL)
            os.waitpid(ownership.pid, 0)
            os.close(pidfd)
            connection.close()
    # Exact settlement permits a fresh reader and a reused pass again.
    reservation.CensusReader(queue, queue.ledger()).capture()
    monkeypatch.setattr(reservation, "_scan_election_refresh", real_refresh)
    stack = saved.acquire()
    assert stack is not None
    with stack:
        assert saved.reused()["elections"] == {}

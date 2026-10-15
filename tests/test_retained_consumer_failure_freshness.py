"""Real retained consumers over scripted, exact-object Linux observations.

Repeated metadata is an external observation, not a replacement cache verdict.
The tests write actual commitments and partial bytes, and call production readers.
"""
from __future__ import annotations

import io
import json
import os
from pathlib import Path
import sys

import pytest

from prismabuild import kept_reads, pool, produced_output
import stage_move
from test_btrfs_subvolume_records_are_retained import (
    OBSERVED_DEVICE, _KernelFacts,
)


pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Linux procfs kernel observations")


@pytest.fixture
def filesystem(monkeypatch):
    opened = []

    def observe(directory: Path, record: Path):
        facts = _KernelFacts(directory, record, monkeypatch)
        facts.table_device = OBSERVED_DEVICE
        opened.append(facts)
        return facts

    yield observe
    for facts in opened:
        facts.close()


@pytest.fixture
def commitments(tmp_path, filesystem):
    scope = tmp_path / "scope"
    path = scope / "commitments.json"
    produced_output._write_commitments(
        path, {"batches": {"old": {"state": "filed"}}, "admission": None})
    return scope, path, filesystem(scope, path)


@pytest.fixture
def staged(tmp_path, filesystem):
    directory = tmp_path / "stage"
    directory.mkdir()
    destination = directory / "shard.safetensors"
    destination.write_bytes(b"staged payload")
    publisher = stage_move._StagedPublisher(
        queue=pool.PoolQueue(tmp_path / "queue"), stage_root=directory,
        residency_root=tmp_path / "residency", mover_action_key="a" * 64,
        manifest_sha256="b" * 64, tier_id="test-stage", cas_root=tmp_path / "cas")
    return publisher, destination, filesystem(directory, destination)


def test_scoped_batches_read_replaced_bytes_after_current_trust_loss(
        commitments, monkeypatch):
    scope, path, facts = commitments
    reads = produced_output._TickReads(None)
    with kept_reads.file_version_scope():
        assert reads.batches(scope) == {"old": {"state": "filed"}}
        assert reads.batches(scope) == {"old": {"state": "filed"}}
        assert (reads.commitments_read, reads.commitments_reused) == (1, 1)
        old_observation = os.stat(path)
        produced_output._write_commitments(
            path, {"batches": {"new": {"state": "filed"}}, "admission": None})
        assert facts.real_stat(path).st_ino != old_observation.st_ino
        assert facts.real_stat(path).st_size == old_observation.st_size
        facts.objects[str(path)] = facts.identity(facts.real_lstat(path))
        current_stat = os.stat

        def cached_stat(target, *args, **kwargs):
            observed = current_stat(target, *args, **kwargs)
            if not isinstance(target, int) and os.fsdecode(target) == str(path):
                return old_observation
            return observed

        monkeypatch.setattr(os, "stat", cached_stat)
        facts.fstype = "nfs4"
        facts.generation += 1
        assert os.stat(path).st_ino == old_observation.st_ino
        assert json.loads(path.read_text())["batches"] == {"new": {"state": "filed"}}
        assert reads.batches(scope) == {"new": {"state": "filed"}}, (
            "current trust loss must re-read the actual replacement commitments")


def test_valid_scoped_batch_hit_keeps_its_parsed_result(commitments, monkeypatch):
    scope, path, _facts = commitments
    reads = produced_output._TickReads(None)
    with kept_reads.file_version_scope():
        assert reads.batches(scope) == {"old": {"state": "filed"}}
        original_open = io.open

        def unreadable_record(target, *args, **kwargs):
            if not isinstance(target, int) and os.fsdecode(target) == str(path):
                raise PermissionError("unchanged commitments must not be opened again")
            return original_open(target, *args, **kwargs)

        monkeypatch.setattr(io, "open", unreadable_record)
        assert reads.batches(scope) == {"old": {"state": "filed"}}
        assert (reads.commitments_read, reads.commitments_reused) == (1, 1)


def test_partial_scan_failure_cannot_resurrect_old_names_on_retry(staged, monkeypatch):
    publisher, destination, facts = staged
    assert publisher._inflight_partials(destination) == []
    original_stamp = facts.directory_stamp
    original_scan = os.scandir
    failure = {"enabled": True, "observed": 0}

    def scan(directory="."):
        if (not isinstance(directory, int)
                and os.fsdecode(directory) == str(destination.parent)
                and failure["enabled"]):
            failure["observed"] += 1
            raise PermissionError("range scan interrupted")
        return original_scan(directory)

    monkeypatch.setattr(os, "scandir", scan)
    facts.directory_stamp += 1
    assert publisher._inflight_partials(destination) is None
    assert failure["observed"] == 1
    foreign = destination.parent / f".{destination.name}.{'c' * 16}.partial"
    foreign.write_bytes(b"foreign copy still in flight")
    failure["enabled"] = False
    facts.directory_stamp = original_stamp
    assert foreign.read_bytes() == b"foreign copy still in flight"
    assert publisher._inflight_partials(destination) == [foreign.name], (
        "a failed fresh scan must not resurrect the earlier empty names capture")


def test_valid_partial_names_hit_still_checks_the_actual_candidate(staged, monkeypatch):
    publisher, destination, _facts = staged
    foreign = destination.parent / f".{destination.name}.{'c' * 16}.partial"
    foreign.write_bytes(b"foreign copy still in flight")
    assert publisher._inflight_partials(destination) == [foreign.name]
    original_scan = os.scandir

    def unreadable_directory(directory="."):
        if (not isinstance(directory, int)
                and os.fsdecode(directory) == str(destination.parent)):
            raise PermissionError("unchanged names must not be scanned again")
        return original_scan(directory)

    monkeypatch.setattr(os, "scandir", unreadable_directory)
    assert publisher._inflight_partials(destination) == [foreign.name]
    assert foreign.read_bytes() == b"foreign copy still in flight"
    foreign.unlink()
    assert publisher._inflight_partials(destination) is None


def test_partial_scan_error_keeps_its_original_refusal_result(staged, monkeypatch):
    publisher, destination, _facts = staged
    original_scan = os.scandir

    def unreadable_directory(directory="."):
        if (not isinstance(directory, int)
                and os.fsdecode(directory) == str(destination.parent)):
            raise PermissionError("range directory unreadable")
        return original_scan(directory)

    with monkeypatch.context() as failure:
        failure.setattr(os, "scandir", unreadable_directory)
        assert publisher._inflight_partials(destination) is None
    foreign = destination.parent / f".{destination.name}.{'c' * 16}.partial"
    foreign.write_bytes(b"foreign copy still in flight")
    assert publisher._inflight_partials(destination) == [foreign.name]
    assert foreign.read_bytes() == b"foreign copy still in flight"

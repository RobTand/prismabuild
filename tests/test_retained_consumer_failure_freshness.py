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


def test_partial_acquisition_clock_failure_cannot_resurrect_old_names(staged, monkeypatch):
    import time

    publisher, destination, facts = staged
    assert publisher._inflight_partials(destination) == []
    stamp = facts.directory_stamp
    facts.directory_stamp += 1
    failure = RuntimeError("coarse clock acquisition interrupted")
    real_clock = time.clock_gettime_ns

    def clock(clock_id):
        if clock_id == kept_reads._COARSE_REALTIME:
            raise failure
        return real_clock(clock_id)

    with monkeypatch.context() as interrupted:
        interrupted.setattr(time, "clock_gettime_ns", clock)
        with pytest.raises(RuntimeError) as raised:
            publisher._inflight_partials(destination)
    assert raised.value is failure
    foreign = destination.parent / f".{destination.name}.{'c' * 16}.partial"
    foreign.write_bytes(b"foreign copy after acquisition failure")
    facts.directory_stamp = stamp
    assert publisher._inflight_partials(destination) == [foreign.name]
    assert foreign.read_bytes() == b"foreign copy after acquisition failure"


@pytest.mark.parametrize("older_finishes", ["commit", "abort"])
def test_older_partial_reader_cannot_replace_a_newer_capture(staged, monkeypatch, older_finishes):
    import threading

    publisher, destination, facts = staged
    assert publisher._inflight_partials(destination) == []
    facts.directory_stamp += 1
    ready, release = threading.Event(), threading.Event()
    original_scan = os.scandir
    answers, errors = [], []
    older = None

    def scan(directory="."):
        if (threading.current_thread() is older
                and not isinstance(directory, int)
                and os.fsdecode(directory) == str(destination.parent)):
            names = list(original_scan(directory))
            ready.set()
            if not release.wait(5):
                raise TimeoutError("newer range reader did not finish")
            if older_finishes == "abort":
                raise PermissionError("older range scan interrupted")
            return iter(names)
        return original_scan(directory)

    def read():
        try:
            answers.append(publisher._inflight_partials(destination))
        except BaseException as exc:
            errors.append(exc)

    monkeypatch.setattr(os, "scandir", scan)
    older = threading.Thread(target=read)
    older.start()
    try:
        assert ready.wait(5), "older reader did not reach the range scan"
        foreign = destination.parent / f".{destination.name}.{'c' * 16}.partial"
        foreign.write_bytes(b"newer foreign copy")
        assert publisher._inflight_partials(destination) == [foreign.name]
    finally:
        release.set()
        older.join(5)
    assert not older.is_alive()
    assert errors == []
    assert answers == ([[]] if older_finishes == "commit" else [None])

    def no_scan(directory="."):
        if (not isinstance(directory, int)
                and os.fsdecode(directory) == str(destination.parent)):
            raise PermissionError("newer qualified names must remain reusable")
        return original_scan(directory)

    monkeypatch.setattr(os, "scandir", no_scan)
    assert publisher._inflight_partials(destination) == [foreign.name]
    assert foreign.read_bytes() == b"newer foreign copy"


def test_forgetting_during_a_record_parse_revokes_its_late_result(
        tmp_path, filesystem, monkeypatch):
    import threading

    directory = tmp_path / "records"
    directory.mkdir()
    record = directory / "one.json"
    record.write_text('{"value":"old"}')
    filesystem(directory, record)
    reader = kept_reads.DirectoryRecords()
    ready, release = threading.Event(), threading.Event()
    answers, errors = [], []
    older = None

    def parse(path):
        value = json.loads(path.read_text())
        if threading.current_thread() is older:
            ready.set()
            if not release.wait(5):
                raise TimeoutError("record invalidation did not finish")
        return value

    def read():
        try:
            answers.append(reader.read(directory, select=lambda entry: True, parse=parse))
        except BaseException as exc:
            errors.append(exc)

    older = threading.Thread(target=read)
    older.start()
    try:
        assert ready.wait(5), "older reader did not reach the real record parse"
        reader.retain(())
        record.write_text('{"value":"new"}')
    finally:
        release.set()
        older.join(5)
    assert not older.is_alive()
    assert errors == []
    assert answers == [[(record, {"value": "old"})]]
    assert json.loads(record.read_text()) == {"value": "new"}
    assert reader.read(directory, select=lambda entry: True, parse=parse) == [
        (record, {"value": "new"})]
    original_open = io.open

    def no_record_open(target, *args, **kwargs):
        if not isinstance(target, int) and os.fsdecode(target) == str(record):
            raise PermissionError("new record must remain a qualified warm hit")
        return original_open(target, *args, **kwargs)

    monkeypatch.setattr(io, "open", no_record_open)
    assert reader.read(directory, select=lambda entry: True, parse=parse) == [
        (record, {"value": "new"})]


@pytest.mark.parametrize("qualified_candidate", [True, False])
def test_incomplete_records_keep_only_currently_qualified_candidates(
        tmp_path, filesystem, monkeypatch, qualified_candidate):
    directory = tmp_path / "records"
    directory.mkdir()
    one, two = directory / "one.json", directory / "two.json"
    one.write_text('{"value":"old-one","keep":true}')
    two.write_text('{"value":"old-two","keep":true}')
    facts = filesystem(directory, one)
    facts.objects[str(two)] = facts.identity(facts.real_lstat(two))
    reader = kept_reads.DirectoryRecords()
    parsed = []

    def parse(path):
        parsed.append(path.name)
        return json.loads(path.read_text())

    def read():
        return reader.read(directory, select=lambda entry: entry.name.endswith(".json"),
                           parse=parse, keep=lambda value: value["keep"])

    assert [value["value"] for _path, value in read()] == ["old-one", "old-two"]
    for path in ([two] if qualified_candidate else [one, two]):
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps({"value": "new-" + path.stem, "keep": False}))
        os.replace(temporary, path)
        facts.objects[str(path)] = facts.identity(facts.real_lstat(path))
    facts.directory_stamp += 1
    expected = ["old-one" if qualified_candidate else "new-one", "new-two"]
    assert [value["value"] for _path, value in read()] == expected
    parsed.clear()
    original_open = io.open

    def open_record(target, *args, **kwargs):
        if (qualified_candidate and not isinstance(target, int)
                and os.fsdecode(target) == str(one)):
            raise PermissionError("the unchanged qualified candidate must be reused")
        return original_open(target, *args, **kwargs)

    monkeypatch.setattr(io, "open", open_record)
    assert [value["value"] for _path, value in read()] == expected
    assert parsed == (["two.json"] if qualified_candidate else ["one.json", "two.json"])
    assert json.loads(two.read_text()) == {"value": "new-two", "keep": False}


def test_history_requalifies_detached_entries_without_persisting_candidates(
        tmp_path, filesystem, monkeypatch):
    import pbmetrics
    from test_btrfs_subvolume_records_are_retained import _ObservedStat

    directory = tmp_path / "history"
    directory.mkdir()
    record = directory / "one.json"
    record.write_text('{"value":"history-row"}')
    facts = filesystem(directory, record)
    reader = pbmetrics.KeptReads()

    def entries():
        reader.begin()
        found = list(reader.history(directory))
        assert len(found) == 1
        value = reader.derive("row", found[0], lambda: json.loads(Path(found[0].path).read_text()))
        reader.end()
        assert value == {"value": "history-row"}
        return found[0]

    first = entries()
    actual_lstat = os.lstat

    def foreign_directory(path, *args, **kwargs):
        info = actual_lstat(path, *args, **kwargs)
        if not isinstance(path, int) and os.fsdecode(path) == str(directory):
            return _ObservedStat(info, device=OBSERVED_DEVICE + 1)
        return info

    monkeypatch.setattr(os, "lstat", foreign_directory)
    assert entries() is first, "the detached complete history remains a per-file candidate"
    assert entries() is not first, "an unqualified history commit adds no candidate-only persistence"
    assert json.loads(record.read_text()) == {"value": "history-row"}

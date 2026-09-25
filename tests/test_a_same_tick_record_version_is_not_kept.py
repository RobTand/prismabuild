"""A record read in the clock tick of its last change is not kept (#1045).

The kept readers reuse a record while a later ``stat`` of its file returns
the same #761 version ``(dev, ino, size, mtime_ns, ctime_ns)``.  A file
unlinked and another created under the same name in one coarse clock tick
(1 ms on the Sparks) can be given the freed inode number, and with the same
size all five fields match: the old record would be served until the file
changed again.  The directory stamp already refuses a version not strictly
before a clock read taken first (``stage_move._trusted_directory_stamp``);
these tests hold each kept file version to the same rule.

The tick is modelled, not raced: the coarse clock is pinned to the file's
own ctime (the tick it changed in), and the "same-tick replacement" is a
real replacement whose ``stat``/``fstat`` is pinned to the version the
replaced file had -- what a reused inode number gives.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

import pbmetrics  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

COARSE = stage_move._COARSE_REALTIME
TICK_NS = 1_000_000

pytestmark = pytest.mark.skipif(
    COARSE is None, reason="the version fence needs Linux's coarse clock")


def _write(path: Path, raw: bytes) -> None:
    temporary = path.parent / f".{path.name}.tmp"
    temporary.write_bytes(raw)
    os.replace(temporary, path)


@pytest.fixture()
def clock(monkeypatch):
    """Pin the coarse clock: ``state["now"]`` or, when ``None``, the real one."""

    state: dict[str, int | None] = {"now": None}
    real = time.clock_gettime_ns

    def pinned(clock_id):
        if clock_id == COARSE and state["now"] is not None:
            return state["now"]
        return real(clock_id)

    monkeypatch.setattr(time, "clock_gettime_ns", pinned)
    monkeypatch.setattr(stage_move, "_filesystem_type", lambda device: "zfs")
    return state


def _replace_in_the_same_tick(monkeypatch, path: Path, raw: bytes) -> None:
    """Replace ``path``; its ``stat`` and ``fstat`` keep the old version.

    What a new file given the freed inode number in the same tick, at the
    same size, looks like to every reader: all five fields match.
    """

    old = os.stat(path)
    _write(path, raw)
    assert os.stat(path).st_size == old.st_size
    real_stat, real_fstat = os.stat, os.fstat

    def stat(target, *args, **kwargs):
        if not isinstance(target, int) and os.fspath(target) == str(path):
            return old
        return real_stat(target, *args, **kwargs)

    def fstat(descriptor):
        try:
            name = os.readlink(f"/proc/self/fd/{descriptor}")
        except OSError:
            name = None
        return old if name == str(path) else real_fstat(descriptor)

    monkeypatch.setattr(os, "stat", stat)
    monkeypatch.setattr(os, "fstat", fstat)


def _parse(path: Path) -> object:
    return json.loads(path.read_bytes())


def test_directory_records_rereads_a_record_filed_in_the_fence_tick(
        tmp_path, monkeypatch, clock):
    directory = tmp_path / "ready"
    directory.mkdir()
    record = directory / "r.json"
    _write(record, b'{"v": 1}')
    reader = stage_release.DirectoryRecords()

    clock["now"] = os.stat(record).st_ctime_ns
    first = reader.read(directory, select=lambda _entry: True, parse=_parse)
    assert [value for _path, value in first] == [{"v": 1}]

    clock["now"] = None
    _replace_in_the_same_tick(monkeypatch, record, b'{"v": 2}')
    second = reader.read(directory, select=lambda _entry: True, parse=_parse)
    assert [value for _path, value in second] == [{"v": 2}], (
        "a record kept at a version stat-ed in its own change's tick was "
        "reused for a same-tick replacement that reproduced that version")


def test_directory_records_keeps_a_record_older_than_the_fence(
        tmp_path, clock):
    directory = tmp_path / "ready"
    directory.mkdir()
    record = directory / "r.json"
    _write(record, b'{"v": 1}')
    reader = stage_release.DirectoryRecords()

    clock["now"] = os.stat(record).st_ctime_ns + TICK_NS
    reader.read(directory, select=lambda _entry: True, parse=_parse)
    # The directory's own stamp holds as well; move it so the entries are
    # stat-ed and compared, which is the path under test.
    (directory / "other.json").write_bytes(b"{}")
    os.utime(directory)
    parsed = reader.parsed
    clock["now"] = time.clock_gettime_ns(COARSE) + TICK_NS * 10**6
    reader.read(directory, select=lambda _entry: True, parse=_parse)
    assert reader.parsed == parsed + 1, "only the new record is parsed"


def test_no_version_is_kept_on_a_network_filesystem(
        tmp_path, monkeypatch, clock):
    directory = tmp_path / "ready"
    directory.mkdir()
    record = directory / "r.json"
    _write(record, b'{"v": 1}')
    monkeypatch.setattr(stage_move, "_filesystem_type", lambda device: "nfs4")
    reader = stage_release.DirectoryRecords()

    clock["now"] = os.stat(record).st_ctime_ns + TICK_NS * 10**6
    reader.read(directory, select=lambda _entry: True, parse=_parse)
    reader.read(directory, select=lambda _entry: True, parse=_parse)
    assert reader.parsed == 2, (
        "a server-clock ctime cannot be fenced by this kernel's clock")


def test_the_fragment_memo_rereads_a_fragment_filed_in_the_fence_tick(
        tmp_path, monkeypatch, clock):
    monkeypatch.setattr(stage_release.residency_map, "validate_fragment",
                        lambda document: document)
    fragment = tmp_path / "fragment.json"
    _write(fragment, b'{"entries": {}, "v": 1}')
    memo = stage_release._CensusMemo()

    clock["now"] = os.stat(fragment).st_ctime_ns
    document = stage_release._read_fragment(fragment, memo)
    assert document["v"] == 1
    # Nor may a skip checkpoint be fenced on it (#1056 reads this version).
    assert memo.version_of(document) is None

    clock["now"] = None
    _replace_in_the_same_tick(monkeypatch, fragment, b'{"entries": {}, "v": 2}')
    assert stage_release._read_fragment(fragment, memo)["v"] == 2


def test_the_fragment_memo_keeps_a_fragment_older_than_the_fence(
        tmp_path, monkeypatch, clock):
    monkeypatch.setattr(stage_release.residency_map, "validate_fragment",
                        lambda document: document)
    fragment = tmp_path / "fragment.json"
    _write(fragment, b'{"entries": {}, "v": 1}')
    memo = stage_release._CensusMemo()

    clock["now"] = os.stat(fragment).st_ctime_ns + TICK_NS
    document = stage_release._read_fragment(fragment, memo)
    assert memo.version_of(document) is not None
    assert stage_release._read_fragment(fragment, memo) is document
    assert memo.counts() == (1, 1)


def test_the_material_memo_rereads_a_sidecar_filed_in_the_fence_tick(
        tmp_path, monkeypatch, clock):
    monkeypatch.setattr(stage_release.reader_lease, "validate_material",
                        lambda document: document)
    path = tmp_path / "material.json"
    monkeypatch.setattr(stage_release.reader_lease, "material_path",
                        lambda root, consumer, mover: path)
    _write(path, b'{"v": 1}')
    memo = stage_release._CensusMemo()

    clock["now"] = os.stat(path).st_ctime_ns
    assert stage_release._read_own_material(tmp_path, "c", "m", memo) == {"v": 1}

    clock["now"] = None
    _replace_in_the_same_tick(monkeypatch, path, b'{"v": 2}')
    assert stage_release._read_own_material(tmp_path, "c", "m", memo) == {"v": 2}


def test_a_kept_history_entry_changed_in_the_fence_tick_is_derived_again(
        tmp_path, monkeypatch, clock):
    directory = tmp_path / "movers"
    directory.mkdir()
    record = directory / "r.json"
    _write(record, b'{"v": 1}')
    # An in-place change moves the file's ctime and not the directory's, so
    # the directory's own stamp is trusted while the entry is in its tick.
    time.sleep(0.02)
    os.chmod(record, 0o640)
    reader = pbmetrics.KeptReads()

    def scrape() -> list:
        reader.begin()
        entries = list(reader.history(directory, select=lambda _entry: True))
        for entry in entries:
            reader.derive("row", entry, lambda: _parse(Path(entry.path)))
        reader.end()
        return entries

    clock["now"] = os.stat(record).st_ctime_ns
    assert os.lstat(directory).st_mtime_ns < clock["now"]
    first = scrape()
    assert first[0].derived == {"v": 1}

    clock["now"] = None
    _replace_in_the_same_tick(monkeypatch, record, b'{"v": 2}')
    second = scrape()
    assert second[0].derived == {"v": 2}, (
        "an entry kept at a version stat-ed in its own change's tick "
        "carried what was derived from the replaced file")

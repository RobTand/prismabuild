"""A warm listing is kept while its filesystem is trusted, and not after (#1208).

``stage_release.DirectoryRecords`` keeps a directory's listing and its parsed
records under a stamp taken by ``stage_move._trusted_directory_stamp``.  The
stamp's argument rests on the directory's times coming from this kernel's
clock, and every read compares the remembered stamp with
``stage_move._current_directory_version``.  That comparison must re-check the
filesystem's trust too: a mount can change under a kept listing, and the
stamp's argument leaves with it.  Without the re-check a warm cache reuses
records the filesystem no longer answers for, and a checkpoint whose header
fields were kept -- the revision and the self-digest -- is served validated
once more instead of being read and rejected.

The transition is direct: the filesystem-type probe is pinned, so it is
exercised on any host and does not depend on the temp storage's real type.
The fixture is settled past the coarse clock's tick so the first listing's
stamp is trustworthy, as the production fixtures are.
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

import stage_move  # noqa: E402
import stage_release  # noqa: E402

COARSE = stage_move._COARSE_REALTIME

pytestmark = pytest.mark.skipif(
    COARSE is None, reason="the trusted stamp needs Linux's coarse clock")


def _settle() -> None:
    """Let the coarse clock tick so the first listing's stamp is trusted."""

    time.sleep(0.05)


def _write(path: Path, raw: bytes) -> None:
    temporary = path.parent / f".{path.name}.tmp"
    temporary.write_bytes(raw)
    os.replace(temporary, path)


def _parse(path: Path) -> object:
    return json.loads(path.read_bytes())


def _select(entry: os.DirEntry) -> bool:
    return entry.name.endswith(".json")


def _counts(reader: stage_release.DirectoryRecords) -> tuple[int, int, int]:
    return (reader.listed, reader.kept, reader.parsed)


@pytest.fixture()
def clock(monkeypatch: pytest.MonkeyPatch) -> dict[str, int | None]:
    """Pin the coarse clock, with the filesystem-type probe naming ZFS."""

    state: dict[str, int | None] = {"now": None}
    real = time.clock_gettime_ns

    def pinned(clock_id: int) -> int:
        if clock_id == COARSE and state["now"] is not None:
            return state["now"]
        return real(clock_id)

    monkeypatch.setattr(time, "clock_gettime_ns", pinned)
    monkeypatch.setattr(stage_move, "_filesystem_type", lambda device: "zfs")
    return state


def test_the_current_version_is_refused_once_trust_leaves(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The compared version is ``None`` on a device no longer trusted."""

    directory = tmp_path / "ready"
    directory.mkdir()
    _settle()

    monkeypatch.setattr(stage_move, "_filesystem_type", lambda device: "zfs")
    assert stage_move._current_directory_version(directory) is not None

    monkeypatch.setattr(stage_move, "_filesystem_type", lambda device: "nfs4")
    assert stage_move._current_directory_version(directory) is None

    monkeypatch.setattr(stage_move, "_filesystem_type", lambda device: None)
    assert stage_move._current_directory_version(directory) is None


def test_a_warm_listing_is_read_again_when_its_trust_leaves(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A kept listing is reused under trust and re-read once trust leaves."""

    directory = tmp_path / "ready"
    directory.mkdir()
    record = directory / "r.json"
    _write(record, b'{"v": 1}')
    _settle()

    monkeypatch.setattr(stage_move, "_filesystem_type", lambda device: "zfs")
    reader = stage_release.DirectoryRecords()
    first = reader.read(directory, select=_select, parse=_parse)
    assert [value for _path, value in first] == [{"v": 1}]
    assert _counts(reader) == (1, 0, 1)

    second = reader.read(directory, select=_select, parse=_parse)
    assert [value for _path, value in second] == [{"v": 1}]
    assert _counts(reader) == (1, 1, 1), (
        "an unchanged trusted directory is not listed or parsed again")

    monkeypatch.setattr(stage_move, "_filesystem_type", lambda device: None)
    third = reader.read(directory, select=_select, parse=_parse)
    assert [value for _path, value in third] == [{"v": 1}]
    assert _counts(reader) == (2, 1, 2), (
        "a listing whose trust left is listed and read again")

    fourth = reader.read(directory, select=_select, parse=_parse)
    assert [value for _path, value in fourth] == [{"v": 1}]
    assert _counts(reader) == (3, 1, 3), (
        "an untrusted filesystem keeps no listing and no parse")


def test_a_record_whose_stat_was_cached_over_is_still_read_again(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A trust loss reads a record again even when a bare stat says it held.

    A network client answers a ``stat`` from its attribute cache -- the reason
    the trust guard exists.  The kept version matches and the old parse is
    still an entry's evidence unless the trust loss drops it, so a rewrite
    that the cached stat hides is read again as a plain read reads it.
    """

    directory = tmp_path / "ready"
    directory.mkdir()
    record = directory / "r.json"
    _write(record, b'{"v": 1}')
    _settle()

    monkeypatch.setattr(stage_move, "_filesystem_type", lambda device: "zfs")
    reader = stage_release.DirectoryRecords()
    assert [value for _path, value in
            reader.read(directory, select=_select, parse=_parse)] == [{"v": 1}]

    cached = os.stat(record)
    monkeypatch.setattr(stage_move, "_filesystem_type", lambda device: None)
    _write(record, b'{"v": 2}')     # the ctime moves; the cached stat does not
    real_stat = os.stat

    def cached_stat(target, *args, **kwargs):  # type: ignore[no-untyped-def]
        if not isinstance(target, int) and os.fspath(target) == str(record):
            return cached
        return real_stat(target, *args, **kwargs)

    monkeypatch.setattr(os, "stat", cached_stat)
    assert [value for _path, value in
            reader.read(directory, select=_select, parse=_parse)] == [{"v": 2}]


def test_a_directory_refused_in_its_tick_still_reuses_old_record_versions(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        clock: dict[str, int | None]) -> None:
    """A tick-refused directory stamp is not a loss of the filesystem's trust.

    The directory is listed again, but its entries' versions were proved by
    the same trusted clock, so an unchanged record is not parsed a second
    time.  Only a trust loss voids them.
    """

    directory = tmp_path / "ready"
    directory.mkdir()
    record = directory / "r.json"
    _write(record, b'{"v": 1}')
    time.sleep(0.05)
    os.utime(directory)                 # the tick the directory changed in
    info = os.lstat(directory)
    clock["now"] = max(info.st_mtime_ns, info.st_ctime_ns)

    reader = stage_release.DirectoryRecords()
    first = reader.read(directory, select=_select, parse=_parse)
    assert [value for _path, value in first] == [{"v": 1}]
    assert _counts(reader) == (1, 0, 1), (
        "the directory's stamp is refused in its tick; the record is kept")

    clock["now"] = None
    time.sleep(0.05)
    second = reader.read(directory, select=_select, parse=_parse)
    assert [value for _path, value in second] == [{"v": 1}]
    assert _counts(reader) == (2, 0, 1), (
        "the directory is listed again, but its unchanged record is reused")

    third = reader.read(directory, select=_select, parse=_parse)
    assert _counts(reader) == (2, 1, 1)

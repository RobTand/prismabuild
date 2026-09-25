#!/usr/bin/env python3
"""Export the pull queue's read-only operational state as Prometheus text."""
from __future__ import annotations

import argparse
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
import errno
import heapq
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import re
import socket
import stat as statmod
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
import pbstatus  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

pool = pbstatus.pool

DEFAULT_QUEUE_ROOT = pbstatus.DEFAULT_QUEUE_ROOT
DEFAULT_LISTEN = "127.0.0.1"
DEFAULT_PORT = 9469
DEFAULT_CACHE_SECONDS = 10.0
DEFAULT_TERMINAL_WINDOW_SECONDS = 3600.0
DEFAULT_TERMINAL_LIMIT = 500
GIB = 1024 ** 3
#: How far ahead of this reader another box's clock may be before its sample
#: stops being credible. A telemetry record is stamped by the box executing the
#: action and read by whichever box runs the exporter, so their clocks are not
#: the same clock: dl380g10's runs milliseconds ahead of sparky's, and a record
#: written "in the future" was being rejected as unusable. Rejecting the
#: freshest record is the wrong way round -- and it fell hardest on the busiest
#: box, whose records are the ones most likely to be seconds old rather than
#: minutes. The bound is the sampler's own period rather than a new constant:
#: a stamp further ahead than one sampling interval is not skew, it is wrong.
_SKEW_S = pool.cpu_admission.MAX_SAMPLE_AGE_S
HOST = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z")
OUTCOMES = frozenset({
    "executed", "cache_hit", "failed", "timeout", "withdrawn", "reset",
    "finish_lost_race", "unreadable", "unknown",
})
#: What ``_file_superseded`` names an unstarted-claim release: the action key,
#: the epoch second it was filed at, and the kind.  The timestamp is IN the
#: name, so the window can be applied without stat'ing a record the scan is
#: about to discard -- the same reason ``_ending_paths`` stats entries rather
#: than reading them.
RELEASE_FILING = re.compile(
    r"\A(?P<key>[0-9a-f]{64})\.(?P<when>[0-9]+\.[0-9]{6})\.unstarted-claim\.json\Z")

RESOURCE_NAMES = {
    "cpu": ("cpu", 1),
    "gpu": ("gpu", 1),
    "mem_gb": ("memory_bytes", GIB),
    "gpu_mem_gb": ("gpu_memory_bytes", GIB),
    "gpu_memory_gb": ("gpu_memory_bytes", GIB),
}


def _number(value: object) -> float | None:
    if type(value) not in (int, float):
        return None
    try:
        result = float(value)
    except (OverflowError, ValueError):
        return None
    return result if math.isfinite(result) and result >= 0 else None


def _host(value: object) -> str | None:
    text = str(value or "")
    return text if HOST.fullmatch(text) else None


def _labels(values: Mapping[str, str]) -> str:
    if not values:
        return ""
    def escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
    return "{" + ",".join(
        f'{key}="{escape(str(value))}"' for key, value in sorted(values.items())
    ) + "}"


def _value(value: float) -> str:
    return str(int(value)) if value.is_integer() else format(value, ".15g")


@dataclass
class Family:
    name: str
    help: str
    samples: list[tuple[dict[str, str], float]] = field(default_factory=list)

    def add(self, value: object, **labels: str) -> None:
        number = _number(value)
        if number is not None:
            self.samples.append((dict(labels), number))

    def render(self) -> list[str]:
        help_text = self.help.replace("\\", "\\\\").replace("\n", "\\n")
        lines = [f"# HELP {self.name} {help_text}", f"# TYPE {self.name} gauge"]
        lines.extend(
            f"{self.name}{_labels(labels)} {_value(value)}"
            for labels, value in sorted(
                self.samples, key=lambda sample: tuple(sorted(sample[0].items()))
            )
        )
        return lines


class Metrics:
    def __init__(self) -> None:
        self.families: dict[str, Family] = {}

    def family(self, name: str, help_text: str) -> Family:
        family = self.families.get(name)
        if family is None:
            family = self.families[name] = Family(name, help_text)
        return family

    def render(self) -> str:
        lines: list[str] = []
        for name in sorted(self.families):
            lines.extend(self.families[name].render())
        return "\n".join(lines) + "\n"


def _resource_samples(values: object) -> list[tuple[str, float]]:
    if not isinstance(values, Mapping):
        return []
    result: list[tuple[str, float]] = []
    for source, value in values.items():
        mapping = RESOURCE_NAMES.get(str(source))
        number = _number(value)
        if mapping is not None and number is not None:
            result.append((mapping[0], number * mapping[1]))
    return result


# --------------------------------------------------------------------------
# Kept reads (#1020)
# --------------------------------------------------------------------------
#
# A scrape used to read the queue from nothing: every state directory listed,
# every terminal record and movement receipt stat-ed, every residency plan
# parsed and censused.  ``KeptReads`` is what one exporter keeps between
# scrapes instead, under the fences the tier loop's census already trusts
# (#992): a directory is not listed again while its ``lstat`` version equals
# a stamp taken before its last listing (``stage_move._trusted_directory_
# stamp``), and a record is not read again while its file version holds
# (``stage_release.DirectoryRecords``).  Every queue writer files by rename,
# which is what both fences stand on.  A stamp is never trusted on a network
# filesystem or inside the clock tick it was taken in, so on NFS every
# listing repeats, by design: the exporter belongs on the queue's own host.
#
# What is kept is what the gauges need, never whole records: a terminal
# record's row, a receipt's projection, a plan's census totals.  Whatever
# failed to read is never kept, so unreadable stays freshly unreadable, and
# anything no longer on disk is dropped when its directory is next listed.
# A listing that could not ``stat`` every entry fails the scrape exactly as
# the plain walk's raise does, and is not kept either.


class _Stat:
    """The two ``stat`` fields a kept entry answers ``stat()`` with."""

    __slots__ = ("st_mtime", "st_size")

    def __init__(self, info: os.stat_result) -> None:
        self.st_mtime = info.st_mtime
        self.st_size = info.st_size


class _KeptEntry:
    """One file as a kept listing holds it: its name and the version it was listed at.

    It stands in for the ``os.DirEntry`` the plain reader handed to
    ``pbstatus.ending_row``: ``path``, ``name`` and ``stat()``, where
    ``stat()`` is the ``stat`` taken when the entry was listed, exactly as a
    ``DirEntry`` caches its own.  ``derived`` is whatever a gauge computed
    from the file's bytes, kept on the entry because the entry is replaced
    whenever the file's version changes (``DirectoryRecords``).
    """

    __slots__ = ("path", "name", "mtime", "version", "_stat", "derived")

    def __init__(self, path: str, name: str, info: os.stat_result,
                 version: tuple) -> None:
        self.path = path
        self.name = name
        self.mtime = info.st_mtime
        #: The #761 version (``stage_move._metadata_version``).
        self.version = version
        self._stat = _Stat(info)
        self.derived: object = None

    def stat(self) -> _Stat:
        return self._stat


def _kept_entry(path: Path, info: os.stat_result | None) -> _KeptEntry | None:
    """``DirectoryRecords``' parse for a listing of files: no read at all.

    ``info`` is the ``stat`` the listing took for the file's version.
    ``None`` for a file that could not be stat-ed: the listing is then not
    kept, and a caller must count the entry as unreadable, never skip it.
    """

    if info is None:
        return None
    return _KeptEntry(str(path), path.name, info,
                      stage_move._metadata_version(info))


def _entry_kept(entry: _KeptEntry | None) -> bool:
    return entry is not None


@dataclass
class _Parsed:
    """One queue record's plain read: the record, or why it could not be had."""

    record: dict | None
    error: Exception | None


def _parse_queue_record(path: Path) -> _Parsed:
    """``pbstatus._pool_records``' read of one record, failure retained."""

    try:
        if not statmod.S_ISREG(path.lstat().st_mode):
            raise ValueError("not a regular queue record")
        record = pool._read_json(path)
        if record is None:
            # A rename may have raced this census. Unknown, rather than a
            # confident empty answer; a later screen can settle it.
            raise ValueError("record disappeared or is empty")
        return _Parsed(record, None)
    except (OSError, ValueError) as exc:
        return _Parsed(None, exc)


def _parsed_ok(parsed: _Parsed) -> bool:
    return parsed.error is None


def _parse_json(path: Path) -> _Parsed:
    """``pool._read_json`` of one record, failure retained."""

    try:
        return _Parsed(pool._read_json(path), None)
    except (OSError, ValueError) as exc:
        return _Parsed(None, exc)


def _read_or_exception(path: Path) -> dict | None | Exception:
    """``pbstatus._pool_sidecar``: the record, ``None``, or the failure."""

    try:
        return pool._read_json(path)
    except (OSError, ValueError) as exc:
        return exc


def _json_name(entry: os.DirEntry) -> bool:
    return entry.name.endswith(".json")


def _json_file(entry: os.DirEntry) -> bool:
    return entry.name.endswith(".json") and entry.is_file()


def _visible_json(entry: os.DirEntry) -> bool:
    # ``Path.glob("*.json")``, which ``PoolQueue.tiers`` lists with, never
    # matches a leading dot.
    return entry.name.endswith(".json") and not entry.name.startswith(".")


def _a_directory(entry: os.DirEntry) -> bool:
    return entry.is_dir()


def _visible_directory(entry: os.DirEntry) -> bool:
    return entry.is_dir() and not entry.name.startswith(".")


def _listed(path: Path) -> bool:
    return True


class KeptReads:
    """What one exporter keeps from one scrape to the next (#1020).

    ``MetricsCache`` holds one for the life of the server, and a one-shot
    collection takes a fresh one, which reads everything exactly as the
    plain census does.  :meth:`begin` starts a scrape and :meth:`end`
    finishes one: a directory's version is read at most once per scrape, a
    directory is read at most once per scrape, and whatever the scrape did
    not touch is dropped at its end, so what is kept is bounded by what is
    on disk now.

    Counters, exported per scrape by :meth:`scrape_counts`: ``listed`` and
    ``kept`` count directories listed and directories answered from a kept
    listing, ``parsed`` the records ``DirectoryRecords`` read, and
    ``computed``/``reused`` the per-file, per-record and per-plan values
    computed from bytes and reused.

    The history directories -- ``done/``, ``failed/``, ``withdrawn/`` and
    ``movers/``, 50,000 names on the live queue and growing -- are kept by
    :meth:`history` instead of ``DirectoryRecords``: one ``scandir`` and one
    ``stat`` per name when the directory changed, exactly the plain walk's
    cost, so a changed scrape costs no more than a fresh one.
    """

    def __init__(self) -> None:
        self.records = stage_release.DirectoryRecords()
        #: Directory -> ``(stamp, {name: entry})`` for :meth:`history`.
        self._histories: dict[str, tuple[tuple, dict[str, _KeptEntry]]] = {}
        self._history_listed = 0
        self._history_kept = 0
        self._at_begin: tuple[int, ...] = (0, 0, 0, 0, 0)
        #: path -> ``(fenced directory, stamp, value)`` for single files.
        self._files: dict[str, tuple[str, tuple, object]] = {}
        #: memo key -> ``(fence, value)`` for derived values.
        self._memos: dict[object, tuple[tuple, object]] = {}
        self._versions: dict[str, tuple | None] = {}
        self._reads: dict[str, tuple[object, object, list]] = {}
        self._named: set[str] = set()
        self._touched: set[object] = set()
        #: Entries whose ``derived`` value the previous scrape used, by kind.
        self._derived_now: dict[str, dict[int, _KeptEntry]] = {}
        self._derived_entries: dict[str, dict[int, _KeptEntry]] = {}
        self.computed = 0
        self.reused = 0

    # -- scrape boundaries -------------------------------------------------

    def _totals(self) -> tuple[int, ...]:
        return (self.records.listed + self._history_listed,
                self.records.kept + self._history_kept,
                self.records.parsed, self.computed, self.reused)

    def scrape_counts(self) -> dict[str, int]:
        """What this scrape has done so far: listed, kept, parsed, computed, reused."""

        return {name: now - then for name, now, then in zip(
            ("listed", "kept", "parsed", "computed", "reused"),
            self._totals(), self._at_begin)}

    def begin(self) -> None:
        self._versions = {}
        self._reads = {}
        self._named = set()
        self._touched = set()
        self._derived_now = {}
        self._at_begin = self._totals()

    def end(self) -> None:
        # A directory this scrape did not read -- a withdrawal decision's,
        # once the decision is gone -- is forgotten with its records.
        wanted = set(self._reads) | self._named
        self.records.retain(wanted)
        for name in [name for name in self._histories if name not in wanted]:
            del self._histories[name]
        for name in [name for name in self._files if name not in self._touched]:
            del self._files[name]
        for key in [key for key in self._memos if key not in self._touched]:
            del self._memos[key]
        # A value derived for an entry the scrape did not use is dropped:
        # the newest-first windows move on, and an entry that has left them
        # must not keep its row for the rest of the fleet's history.
        for kind, previous in self._derived_entries.items():
            current = self._derived_now.get(kind, {})
            for ident, entry in previous.items():
                if ident not in current:
                    entry.derived = None
        self._derived_entries = self._derived_now

    def used(self, kind: str, entry: _KeptEntry) -> None:
        """Record that this scrape used ``entry.derived`` under ``kind``."""

        self._derived_now.setdefault(kind, {})[id(entry)] = entry

    def derive(self, kind: str, entry: _KeptEntry, compute, *, keep=None):
        """``compute()`` of ``entry``'s bytes, kept on the entry while it lasts.

        The entry is replaced whenever its file's version changes, so a value
        kept on it is a value of the bytes now on disk.  ``keep(value)``
        false (a read that failed) is returned and computed again next time.
        """

        self.used(kind, entry)
        if entry.derived is not None:
            self.reused += 1
            return entry.derived
        value = compute()
        self.computed += 1
        if keep is None or keep(value):
            entry.derived = value
        return value

    # -- fences ------------------------------------------------------------

    def version(self, directory: Path | str) -> tuple | None:
        """``directory``'s version, read once per scrape."""

        name = os.fspath(directory)
        if name not in self._versions:
            self._versions[name] = stage_move._current_directory_version(Path(name))
        return self._versions[name]

    def fence(self, directory: Path) -> tuple[str, tuple] | None:
        """A trusted stamp on ``directory``, or on its nearest existing ancestor.

        A directory that does not exist yet is fenced by its parent: making
        it changes the parent.  ``None`` when no stamp can be trusted (a
        network filesystem, or a change inside the current clock tick).
        """

        current = Path(directory)
        while True:
            stamp = stage_move._trusted_directory_stamp(current)
            if stamp is not None:
                return os.fspath(current), stamp
            if os.path.lexists(current) or current.parent == current:
                return None
            current = current.parent

    def holds(self, fences: tuple) -> bool:
        """Whether every fence in ``fences`` still holds this scrape."""

        return all(fence is not None and self.version(fence[0]) == fence[1]
                   for fence in fences)

    def still(self, fences: tuple) -> bool:
        """Whether every fence holds now, read fresh, not from this scrape."""

        return all(fence is not None
                   and stage_move._current_directory_version(Path(fence[0])) == fence[1]
                   for fence in fences)

    def memo(self, key: object, make_fences, compute, *, keep=None):
        """``compute()``, reused while every fence ``make_fences()`` took holds.

        The fences are taken before ``compute`` runs and checked again after
        it, and only a value with every fence trusted and unmoved across the
        computation is kept -- the protocol ``DirectoryRecords`` keeps for a
        listing.  ``make_fences`` returns ``None`` when no fence can be
        trusted, and the value is then computed every scrape.
        """

        self._touched.add(key)
        kept = self._memos.pop(key, None)
        if kept is not None and self.holds(kept[0]):
            self._memos[key] = kept
            self.reused += 1
            # A fenced directory is unchanged, so what ``DirectoryRecords``
            # keeps for it is still good: it must survive this scrape's end
            # even though nothing read it.
            self._named.update(fence[0] for fence in kept[0])
            return kept[1]
        # Nothing is kept while ``compute`` runs: one that raises leaves no
        # value behind, stale or otherwise.
        fences = make_fences()
        value = compute()
        self.computed += 1
        if (fences is not None and self.still(fences)
                and (keep is None or keep(value))):
            self._memos[key] = (fences, value)
        else:
            self._memos.pop(key, None)
        return value

    def fences(self, directories) -> tuple | None:
        """A trusted fence on each of ``directories``, or ``None``."""

        taken = tuple(self.fence(Path(directory)) for directory in directories)
        return None if any(fence is None for fence in taken) else taken

    def ledger_fences(self, ledger: pool.ResourceLedger) -> tuple | None:
        """Fences on everything a tier ledger's tokens are read from.

        ``free/``, ``held/`` and each holder directory under ``held/``: a
        token moves by rename between them, and a holder appears or leaves
        by rename into or out of ``held/``.  The holders are listed after
        ``held/``'s stamp is taken, so the list is the one that stamp
        covers.  Taken at most once a scrape, and kept while it holds.
        """

        key = ("ledger-fences", os.fspath(ledger.base))
        self._touched.add(key)
        kept = self._memos.get(key)
        if kept is not None and self.holds(kept[0]):
            return kept[0]
        free = self.fence(ledger.free_dir)
        held = self.fence(ledger.held_dir)
        if free is None or held is None:
            self._memos.pop(key, None)
            return None
        try:
            holders = sorted(os.listdir(held[0])) if held[0] == os.fspath(
                ledger.held_dir) else []
        except OSError:
            self._memos.pop(key, None)
            return None
        taken = (free, held, *(self.fence(Path(held[0]) / name)
                               for name in holders))
        if any(fence is None for fence in taken):
            self._memos.pop(key, None)
            return None
        self._memos[key] = (taken, None)
        return taken

    # -- reads ---------------------------------------------------------------

    def entries(self, directory: Path, *, select=_json_file) -> list[tuple[Path, _KeptEntry | None]]:
        """The files in ``directory``, each with the version it was listed at."""

        return self._read(directory, select, _kept_entry, _entry_kept)

    def history(self, directory: Path, *, select=_json_file,
                absent_ok: bool = True) -> Iterable[_KeptEntry]:
        """Every entry ``select`` keeps in ``directory``, in listing order.

        ``pbstatus._ending_paths``' walk, kept: while the directory's stamp
        holds its kept entries are returned without a listing, and when it
        moved it is listed and every entry ``stat``-ed -- one ``scandir`` and
        one ``stat`` per name, what the plain walk costs -- and an entry whose
        #761 version held is the same object as before, carrying what was
        derived from it.  Nothing else is done per name: no ``Path``, no
        sort, so a changed history directory costs what a fresh walk does.

        Failures are the plain walk's.  ``FileNotFoundError`` while listing
        is an absent directory only if a second ``stat`` of the directory
        says so (``absent_ok``, else it is raised); any other error, or a
        name that vanished from a directory still there, is raised.  A
        listing that raised is not kept.
        """

        name = os.fspath(directory)
        done = self._reads.get(name)
        if done is not None:
            if done[0] is not _HISTORY or done[1] is not select:
                raise AssertionError(f"{name} read under two meanings")
            return done[2]
        kept = self._histories.pop(name, None)
        if kept is not None and self.version(name) == kept[0]:
            self._histories[name] = kept
            self._history_kept += 1
            out = kept[1].values()
        else:
            out = self._list_history(name, select, absent_ok,
                                     kept[1] if kept is not None else {})
        self._reads[name] = (_HISTORY, select, out)
        return out

    def _list_history(self, name: str, select, absent_ok: bool,
                      previous: dict[str, _KeptEntry]) -> Iterable[_KeptEntry]:
        directory = Path(name)
        stamp = stage_move._trusted_directory_stamp(directory)
        entries: dict[str, _KeptEntry] = {}
        stat = os.stat
        version_of = stage_move._metadata_version
        try:
            with os.scandir(name) as scan:
                for entry in scan:
                    if not select(entry):
                        continue
                    info = stat(entry.path)
                    version = version_of(info)
                    kept = previous.get(entry.name)
                    if kept is None or kept.version != version:
                        kept = _KeptEntry(entry.path, entry.name, info, version)
                    entries[entry.name] = kept
        except FileNotFoundError:
            try:
                os.stat(name)
            except FileNotFoundError:
                if absent_ok:
                    return ()
            # The directory exists, but a selected entry was not readable.
            raise
        self._history_listed += 1
        if (stamp is not None
                and stage_move._current_directory_version(directory) == stamp):
            self._histories[name] = (stamp, entries)
        return entries.values()

    def names(self, directory: Path, *, select) -> frozenset[str]:
        """The names ``select(name)`` keeps in ``directory``; nothing stat-ed."""

        self._named.add(os.fspath(directory))
        return self.records.names(Path(directory), select=select)

    def subdirectories(self, directory: Path, *, select=_a_directory) -> list[Path]:
        """The subdirectories of ``directory``; empty when it does not exist."""

        return [path for path, _ in self._read(directory, select, _listed, None)]

    def queue_records(self, directory: Path) -> list[tuple[Path, _Parsed]]:
        """Every ``.json`` record in one queue state directory, parsed."""

        return self._read(directory, _json_name, _parse_queue_record, _parsed_ok)

    def json_records(self, directory: Path, *, select=_visible_json,
                     ) -> list[tuple[Path, _Parsed]]:
        """Every record ``select`` names, read as ``pool._read_json`` reads it."""

        return self._read(directory, select, _parse_json, _parsed_ok)

    def _read(self, directory: Path, select, parse, keep):
        # One read per directory per scrape: ``ready/`` and ``claimed/`` were
        # read three times a scrape, once by each census that wanted them.
        # ``DirectoryRecords`` keeps one meaning per directory, so a second
        # reading of one directory under another parse is a defect here.
        name = os.fspath(directory)
        done = self._reads.get(name)
        if done is not None:
            if done[0] is not parse or done[1] is not select:
                raise AssertionError(f"{name} read under two meanings")
            return done[2]
        out = self.records.read(Path(directory), select=select, parse=parse,
                                keep=keep, stat_parse=parse is _kept_entry)
        self._reads[name] = (parse, select, out)
        return out

    def sidecar(self, path: Path) -> dict | None | Exception:
        """One small record by path, reused while its directory's stamp holds.

        What ``pbstatus._pool_sidecar`` returns: the record, ``None`` when
        it is absent, or the failure.  A failure is never kept.
        """

        name = os.fspath(path)
        self._touched.add(name)
        kept = self._files.get(name)
        if kept is not None and self.version(kept[0]) == kept[1]:
            self.reused += 1
            return kept[2]
        fence = self.fence(Path(name).parent)
        value = _read_or_exception(Path(name))
        self.computed += 1
        if (fence is not None and not isinstance(value, Exception)
                and self.still((fence,))):
            self._files[name] = (fence[0], fence[1], value)
        else:
            self._files.pop(name, None)
        return value

    # -- the pbstatus census's reads (``pbstatus.kept_reads``) -----------------

    def pool_records(self, directory: Path) -> tuple[dict[str, dict | None], list[str]]:
        """``pbstatus._pool_records``, from the kept listing."""

        try:
            listed = self.queue_records(Path(directory))
        except OSError as exc:
            return {}, [f"pool {directory}: {pbstatus._unreadable_reason(exc)}"]
        records: dict[str, dict | None] = {}
        notes: list[str] = []
        for path, parsed in listed:
            if parsed.error is not None:
                records[path.stem] = None
                notes.append(f"pool {path}: {parsed.error}")
            else:
                records[path.stem] = parsed.record
        return records, notes

    def endings(self, queue_root: Path, limit: int) -> list[dict]:
        """``pbstatus.read_endings``, from the kept terminal listings."""

        return _kept_endings(self, queue_root, limit)

    def record(self, directory: Path, key: str) -> dict | None:
        """One record of an already-read state directory, or ``None``."""

        try:
            listed = self.queue_records(directory)
        except OSError:
            return None
        wanted = f"{key}.json"
        for path, parsed in listed:
            if path.name == wanted:
                return parsed.record
        return None


#: The ``_reads`` meaning of a :meth:`KeptReads.history` listing.
_HISTORY = object()


def _fresh(reader: KeptReads | None) -> KeptReads:
    """``reader``, or a reader that keeps nothing: one read of everything."""

    if reader is not None:
        return reader
    fresh = KeptReads()
    fresh.begin()
    return fresh


def _read_claim(queue: pool.PoolQueue, key: str,
                reader: KeptReads | None = None) -> dict | None:
    return _fresh(reader).record(queue.dir(pool.CLAIMED), key)


@dataclass
class _HostAttempts:
    """What one host's live claims reported, including what they did not.

    The aggregate is deliberately all-or-nothing, but the counts beside it are
    not: a host whose aggregate is withheld still says how many live claims it
    has and how stale the oldest of them is, so "nothing running here" and
    "the claim that would have told you is the one that stopped reporting" stop
    rendering identically.
    """

    cpu: float = 0.0
    memory_bytes: float = 0.0
    jobs: int = 0
    unavailable: int = 0
    oldest_age_s: float | None = None
    #: ``(action_key, nonce) -> (cpu_seconds, wall_seconds)`` for the claims
    #: with complete, fresh, exact-scope samples, even if another claim makes
    #: the host aggregate unavailable.
    counters: dict[tuple[str, str], tuple[float, float]] = field(default_factory=dict)
    #: ``resource -> value`` for the host's live claims: the largest peak any
    #: one of them reported, and the bytes they have moved between them.
    #: Absent, not zero, for a resource none of them measured -- a sampler that
    #: could not read ``/proc`` and a claim that did no I/O must not read the
    #: same.
    peaks: dict[str, float] = field(default_factory=dict)

    @property
    def complete(self) -> bool:
        return self.jobs > 0 and self.unavailable == 0


def _attempt_telemetry(
    queue: pool.PoolQueue,
    live_jobs: Mapping[str, list[Mapping[str, object]]],
    now: float,
    reader: KeptReads | None = None,
) -> dict[str, _HostAttempts]:
    """Read each live claim's resource-scope telemetry, by claiming host.

    The aggregate a host publishes stays all-or-nothing: a partial sum reads as
    a low whole-host total, which is worse than no number. What changed is that
    withholding it is no longer silent. Every live claim is examined rather than
    abandoning the host at the first unusable record, and the count that could
    not be used is reported beside the count that could.

    That distinction is the whole point. A claim blocked on the shared mount
    keeps its lease -- the loop is alive and heartbeating -- while the sampler
    that writes its telemetry does not run, so its record goes stale. Under the
    old early exit that claim erased its host from the observation entirely, and
    the box in the most trouble was the box that reported nothing. The signal
    was strongest exactly where it was being deleted.

    Raw counters come back too, per claim, because the ratio the aggregate
    carries is a lifetime average: a job that ran hot for fifty minutes and has
    been blocked for ten still averages high. Differencing consecutive readings
    is what tells those apart, and that is done by the caller, which is the only
    party that has a previous reading.
    """
    reader = _fresh(reader)
    answer: dict[str, _HostAttempts] = {}
    for host, jobs in live_jobs.items():
        host_attempts = _HostAttempts(jobs=len(jobs))
        for job in jobs:
            key = str(job.get("action_key") or "")
            claim = _read_claim(queue, key, reader)
            scope = claim.get("resource_scope") if isinstance(claim, dict) else None
            nonce = scope.get("nonce") if isinstance(scope, dict) else None
            record = reader.sidecar(
                queue.ledger(host).base / "telemetry" / f"{key}.json")
            record = None if isinstance(record, Exception) else record
            sampled = _number(record.get("sampled_unix")) if isinstance(record, dict) else None
            cpu_seconds = _number(record.get("cpu_seconds")) if isinstance(record, dict) else None
            wall_seconds = _number(record.get("wall_seconds")) if isinstance(record, dict) else None
            current = _number(record.get("memory_current_bytes")) if isinstance(record, dict) else None
            # Age is reported for any record whose timestamp is credible, even
            # one too old for the aggregate -- that is precisely the reading a
            # reader wants when the aggregate is missing.
            if sampled is not None and now - sampled >= -_SKEW_S:
                # Floored at zero: a stamp from a clock a few milliseconds ahead
                # is as new as this reader can tell, and a negative age is not a
                # reading anyone can act on. The tolerance above is what decides
                # whether the sample counts; this only decides how it reads.
                age = max(0.0, now - sampled)
                if (host_attempts.oldest_age_s is None
                        or age > host_attempts.oldest_age_s):
                    host_attempts.oldest_age_s = age
            matched = (isinstance(record, dict) and record.get("action_key") == key
                       and bool(nonce) and record.get("nonce") == nonce)
            if (not isinstance(record, dict) or record.get("complete") is not True
                    or not matched or sampled is None
                    or not -_SKEW_S <= now - sampled <= pool.cpu_admission.MAX_SAMPLE_AGE_S
                    or cpu_seconds is None or wall_seconds is None or wall_seconds <= 0
                    or current is None):
                host_attempts.unavailable += 1
                continue
            host_attempts.counters[(key, str(nonce))] = (cpu_seconds, wall_seconds)
            host_attempts.cpu += cpu_seconds / wall_seconds
            host_attempts.memory_bytes += current
            peak = _number(record.get("memory_peak_bytes"))
            if peak is not None:
                host_attempts.peaks["memory_peak_bytes"] = max(
                    host_attempts.peaks.get("memory_peak_bytes", peak), peak)
            measured_io = record.get("process_io")
            if isinstance(measured_io, Mapping):
                for source, name in (("read_bytes", "io_read_bytes"),
                                     ("write_bytes", "io_write_bytes")):
                    moved = _number(measured_io.get(source))
                    if moved is not None:
                        host_attempts.peaks[name] = (
                            host_attempts.peaks.get(name, 0.0) + moved)
        answer[host] = host_attempts
    return answer


def _recent_cores(
    observations: Mapping[str, _HostAttempts],
    previous: dict[tuple[str, str], tuple[float, float]] | None,
) -> dict[str, tuple[float, int]]:
    """Cores used since the previous reading of the same claim, by host.

    The exporter carries no counter of its own, by design -- one would reset on
    every restart and lie about a rate. This is not that: the counters belong to
    the attempt's cgroup and are differenced only against a previous reading of
    *that same attempt*, identified by action key and scope nonce together, so a
    key reused by a later attempt cannot be differenced against an earlier one.

    The first refresh after a start has nothing to difference and reports
    nothing, which is the honest answer to "what has been happening" when the
    answer is "I have seen one moment". ``previous`` is replaced in place with
    the current reading, and only for claims that are live now, so it cannot
    grow with the queue's history.
    """
    answer: dict[str, tuple[float, int]] = {}
    if previous is None:
        return answer
    current: dict[tuple[str, str], tuple[float, float]] = {}
    for host, host_attempts in observations.items():
        cores = 0.0
        covered = 0
        for identity, (cpu_seconds, wall_seconds) in host_attempts.counters.items():
            current[identity] = (cpu_seconds, wall_seconds)
            before = previous.get(identity)
            if before is None:
                continue
            cpu_delta = cpu_seconds - before[0]
            wall_delta = wall_seconds - before[1]
            # A counter that went backwards is not this attempt's; a wall that
            # did not advance gives no rate. Neither is an error worth a false
            # zero, so the claim simply is not covered this refresh.
            if wall_delta <= 0 or cpu_delta < 0:
                continue
            cores += cpu_delta / wall_delta
            covered += 1
        if covered:
            answer[host] = (cores, covered)
    previous.clear()
    previous.update(current)
    return answer


def _kept_endings(reader: KeptReads, queue_root: Path, limit: int) -> list[dict]:
    """``pbstatus.read_endings``, from the kept terminal listings.

    The same selection ``pbstatus._ending_paths`` makes -- the ``limit``
    newest records by modification time across ``done``, ``failed`` and
    ``withdrawn``, and a withdrawal decision only where it is newer than the
    summary filed for it -- taken from listings kept between scrapes, and
    the same projection, ``pbstatus.ending_row``, of each selected record,
    kept on its entry while its version holds.  An unreadable record is
    projected again every scrape, and an entry that cannot be ``stat``-ed
    raises, as the plain walk does.  Records with equal modification times
    are taken in listing order, as the plain walk's stable sort takes them.

    The whole selection is also kept, fenced on every directory it came
    from, so a scrape of unchanged history does not even walk the kept
    listings: 40,000 names sorted by time every ten seconds was most of an
    idle scrape's CPU.
    """

    queue_root = Path(queue_root)
    decisions = queue_root / pool.WITHDRAWN / "decisions"
    states = [queue_root / state for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN)]

    def fences():
        taken = reader.fences([*states, decisions])
        if taken is None:
            return None
        # Listed after ``decisions/``' own stamp, so the list is the one
        # that stamp covers; a decision filed since moves that stamp.
        subdirectories = reader.fences(
            entry.path for entry in reader.history(decisions, select=_a_directory))
        return None if subdirectories is None else taken + subdirectories

    rows, used = reader.memo(
        ("endings", os.fspath(queue_root), int(limit)), fences,
        lambda: _select_endings(reader, queue_root, states, decisions, limit),
        keep=lambda value: not any(row.get("unreadable") for row in value[0]))
    for entry in used:
        reader.used("ending", entry)
    return list(rows)


def _mtime(entry: _KeptEntry) -> float:
    return entry.mtime


def _select_endings(reader: KeptReads, queue_root: Path, states: list[Path],
                    decisions: Path, limit: int) -> tuple[list[dict], list[_KeptEntry]]:
    """``pbstatus._ending_paths``' selection, from :meth:`KeptReads.history`.

    ``heapq.nlargest`` is ``sorted(..., reverse=True)[:limit]`` by
    definition, ties included, over the same sequence the plain walk sorts:
    ``done``, ``failed``, ``withdrawn`` and the decisions, each in listing
    order.
    """

    entries: list[_KeptEntry] = []
    withdrawal_mtimes: dict[str, float] = {}
    for directory in states:
        listed = reader.history(directory)
        entries.extend(listed)
        if directory.name == pool.WITHDRAWN:
            for entry in listed:
                withdrawal_mtimes[entry.name[:-5]] = entry.mtime
    # A cancellation is durable before its visible summary is written. Keep
    # that ending visible if the operator crashed between the two writes.
    for directory in list(reader.history(decisions, select=_a_directory)):
        candidates = list(reader.history(Path(directory.path), select=_json_name,
                                         absent_ok=False))
        if candidates:
            newest = max(candidates, key=_mtime)
            if newest.mtime > withdrawal_mtimes.get(directory.name, float("-inf")):
                entries.append(newest)
    chosen = heapq.nlargest(max(0, int(limit)), entries, key=_mtime)
    rows = [reader.derive("ending", entry,
                          lambda entry=entry: pbstatus.ending_row(entry, queue_root),
                          keep=lambda row: not row.get("unreadable"))
            for entry in chosen]
    rows.sort(key=lambda row: row["finished_unix"], reverse=True)
    return rows, chosen


def _terminal_metrics(
    metrics: Metrics,
    queue_root: Path,
    *,
    now: float,
    window_seconds: float,
    limit: int,
    reader: KeptReads | None = None,
) -> bool:
    reader = _fresh(reader)
    # A state directory that is not there, or is not a directory, makes the
    # window unproven.  The plain reader listed each one to find out; its
    # version answers the same question for one ``lstat``, and a directory
    # that exists but cannot be listed still fails the read below.
    accessible = all(reader.version(Path(queue_root) / state) is not None
                     for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN))
    with pbstatus.kept_reads(reader):
        endings = pbstatus.read_endings(queue_root, limit=limit)
    selected = [
        row for row in endings
        if (age := _number(now - float(row.get("finished_unix", -math.inf)))) is not None
        and age <= window_seconds
    ]
    complete = (
        accessible
        and len(endings) < limit
        and all(not row.get("unreadable") for row in selected)
    )
    metrics.family(
        "prismabuild_terminal_outcomes_window_seconds",
        "Configured lookback window for recent terminal outcome gauges.",
    ).add(window_seconds)
    metrics.family(
        "prismabuild_terminal_outcomes_window_jobs",
        "Terminal records included in the current bounded lookback window.",
    ).add(len(selected))
    metrics.family(
        "prismabuild_terminal_outcomes_window_complete",
        "Whether the terminal record scan proved the configured window was not truncated by its record limit.",
    ).add(1 if complete else 0)
    terminal_success = metrics.family(
        "prismabuild_terminal_collection_success",
        "Whether terminal directories and every selected terminal record were readable for this snapshot.",
    )

    outcomes: dict[tuple[str, str], int] = defaultdict(int)
    timings: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in selected:
        host = _host(row.get("host")) or "unknown"
        outcome = str(row.get("status") or "unknown")
        outcome = outcome if outcome in OUTCOMES else "unknown"
        outcomes[(host, outcome)] += 1
        if row.get("unreadable"):
            continue
        # ``read_endings`` already projected these fields from its bounded
        # read. Opening the complete record again doubled both NFS traffic and
        # the allocation spike from rows that embedded action logs.
        published = _number(row.get("published_unix"))
        claimed = _number(row.get("claimed_unix"))
        finished = _number(row.get("timing_finished_unix"))
        if published is not None and claimed is not None and claimed >= published:
            timings[(host, "queue_wait")].append(claimed - published)
        if claimed is not None and finished is not None and finished >= claimed:
            timings[(host, "execution")].append(finished - claimed)

    outcomes_family = metrics.family(
        "prismabuild_terminal_outcomes",
        "Terminal outcomes observed in the bounded recent window; this is a restart-safe gauge, not a counter.",
    )
    for (host, outcome), count in outcomes.items():
        outcomes_family.add(count, host=host, outcome=outcome)

    queue_timing = metrics.family(
        "prismabuild_queue_wait_seconds",
        "Mean or maximum publish-to-claim time among terminal records with usable timestamps in the bounded window.",
    )
    execution_timing = metrics.family(
        "prismabuild_execution_seconds",
        "Mean or maximum claim-to-finish time among terminal records with usable timestamps in the bounded window.",
    )
    timing_jobs = metrics.family(
        "prismabuild_terminal_timing_window_jobs",
        "Terminal records contributing to each timing gauge in the bounded window.",
    )
    for (host, phase), values in timings.items():
        target = queue_timing if phase == "queue_wait" else execution_timing
        target.add(sum(values) / len(values), host=host, stat="mean")
        target.add(max(values), host=host, stat="max")
        timing_jobs.add(len(values), host=host, phase=phase)
    _box_window_peaks(metrics, selected)

    readable = accessible and all(not row.get("unreadable") for row in selected)
    terminal_success.add(1 if readable else 0)
    return readable


#: The GPU metrics that are a reading *against a reference*, and so have to be
#: separated by the scope of the reference they were taken against.  Watts and
#: bytes are absolute and compare across any reference; a fraction and the
#: denominator it used do not (#806).
_SCOPED_BOX_WINDOW_METRICS = frozenset((
    "gpu_power_peak_watts", "gpu_power_reference_watts", "gpu_power_peak_fraction",
))


def _box_window_peaks(metrics: Metrics, rows: Iterable[Mapping[str, object]]) -> None:
    """Publish the heaviest box-window reading each host reported, by scope.

    Absent rather than zero for a host whose endings carried no box window,
    which is how a box with no flight recorder differs from a box that idled.

    ``scope`` is the reference the GPU power readings were taken against:
    ``measured_peak`` and ``declared_fallback`` are GPU-only, ``soc_tdp`` is
    the whole-module envelope, and ``unknown`` is a record filed before the
    reference was scoped at all.  It is a label and not a suffix because the
    alternative is a ``max`` over two denominators, which is a number taken
    against neither.  Readings with no reference carry an empty scope, which
    Prometheus reads as no label, so the family keeps one label set.
    """

    peaks: dict[tuple[str, str, str], float] = {}
    for row in rows:
        if row.get("unreadable"):
            continue
        host = _host(row.get("host")) or "unknown"
        scope = row.get("gpu_power_reference_scope")
        scope = scope if isinstance(scope, str) and scope else "unknown"
        for field, metric in (
            ("gpu_power_peak_w", "gpu_power_peak_watts"),
            ("gpu_power_reference_w", "gpu_power_reference_watts"),
            ("gpu_power_peak_fraction", "gpu_power_peak_fraction"),
            ("gpu_framebuffer_used_peak_bytes", "gpu_framebuffer_used_peak_bytes"),
            ("gpu_framebuffer_total_bytes", "gpu_framebuffer_total_bytes"),
            ("memory_peak_bytes", "memory_peak_bytes"),
            ("io_write_bytes", "io_write_bytes"),
            ("io_read_bytes", "io_read_bytes"),
        ):
            value = _number(row.get(field))
            if value is None:
                continue
            key = (host, metric, scope if metric in _SCOPED_BOX_WINDOW_METRICS else "")
            peaks[key] = max(peaks.get(key, value), value)
    box = metrics.family(
        "prismabuild_terminal_box_window",
        "Heaviest per-action resource reading among the host's terminal records in the bounded window; GPU power is measured against the GPU-only reference named by the scope label, and every metric is absent rather than zero where no record carried it.",
    )
    for (host, metric, scope), value in sorted(peaks.items()):
        box.add(value, host=host, metric=metric, scope=scope)


def _release_metrics(
    metrics: Metrics,
    queue_root: Path,
    *,
    now: float,
    window_seconds: float,
    limit: int,
    reader: KeptReads | None = None,
) -> bool:
    """Count the unstarted-claim releases each box produced in the window.

    Issue #263.  ``reap_stale`` releases a claim whose lease never appeared
    and whose attempt was never published: the action returns to ``ready``
    unrunnable-through-no-fault-of-its-own, uncharged, and counted.  A rising
    release rate on ONE box is the signal, because the window it comes out of
    -- between the claim rename and the first lease write -- is filesystem
    latency on that box, measured at 45 s against a 30 s grace (issue #222).

    The box is not on the requeued item.  ``_shape_as_ready_item`` pops every
    claim-scoped field, ``claimed_host`` included, so by the time the counter
    is readable in ``ready`` the record no longer says who was holding it.
    The filing under ``withdrawn/superseded/`` does: ``_file_superseded`` is
    called with the claimed record, before the pops.  So the per-box series is
    read there and the queue-wide one from the census.

    Bounded exactly as ``_terminal_metrics`` is, and for the same reason: this
    directory keeps every release this fleet has ever filed, and a scrape that
    reads all of them to report a handful is the slow path.  ``complete`` says
    whether the window was proved rather than truncated.
    """

    reader = _fresh(reader)
    directory = Path(queue_root) / pool.WITHDRAWN / "superseded"
    selected: list[tuple[float, str]] = []
    readable = True
    try:
        # Names only: the filing time is in the name, so the window is
        # applied to a listing kept between scrapes, with nothing stat-ed.
        names = reader.names(directory, select=RELEASE_FILING.match)
    except OSError:
        names = frozenset()
        readable = False
    for name in names:
        when = float(RELEASE_FILING.match(name).group("when"))
        age = now - when
        if -_SKEW_S <= age <= window_seconds:
            selected.append((when, os.path.join(directory, name)))
    selected.sort(reverse=True)
    complete = readable and len(selected) <= limit

    counts: dict[str, int] = defaultdict(int)
    for _, path in selected[:limit]:
        record = reader.sidecar(Path(path))
        if isinstance(record, Exception):
            record = None
        if not isinstance(record, dict):
            readable = False
            continue
        owner = str(record.get("claimed_by") or "").split(":", 1)[0]
        counts[_host(record.get("claimed_host")) or _host(owner) or "unknown"] += 1

    events = metrics.family(
        "prismabuild_unstarted_release_events",
        "Claims released without an attempt in the bounded recent window, by the box that held the claim; this is a restart-safe gauge, not a counter.",
    )
    for host, count in counts.items():
        events.add(count, host=host)
    metrics.family(
        "prismabuild_unstarted_release_events_complete",
        "Whether the unstarted-release scan proved the configured window was not truncated by its record limit and every selected filing was readable.",
    ).add(1 if complete and readable else 0)
    return readable


def _tiers(reader: KeptReads, queue: pool.PoolQueue) -> list[dict[str, object]]:
    """``PoolQueue.tiers``, from the kept ``tiers/`` read.

    The same records in the same order, and the first unreadable one raises
    as the plain read raises it.
    """

    records = []
    for path, parsed in reader.json_records(queue.root / pool.TIERS):
        if parsed.error is not None:
            raise parsed.error
        record = parsed.record
        if isinstance(record, dict) and record.get("tier_id") == path.stem:
            records.append(record)
    return records


def _tier_ids(reader: KeptReads, queue: pool.PoolQueue) -> list[str]:
    """``PoolQueue.tier_ids``, from a kept listing; any failure reads empty."""

    try:
        return sorted(path.name for path in reader.subdirectories(
            queue.root / pool.TIER_RESERVATIONS, select=_visible_directory))
    except OSError:
        return []


def _ledger_counts(reader: KeptReads, queue: pool.PoolQueue, tier_id: str):
    """``(capacity, available)`` of one tier ledger, or the failure raised."""

    ledger = queue.tier_ledger(tier_id)

    def count():
        try:
            return ledger.capacity(), ledger.available()
        except Exception as exc:  # noqa: BLE001 -- reported, as the plain read did
            return exc

    return reader.memo(("ledger", tier_id), lambda: reader.ledger_fences(ledger),
                       count, keep=lambda value: not isinstance(value, Exception))


def _starvation_metrics(
    metrics: Metrics,
    queue_root: Path,
    *,
    now: float,
    window_seconds: float,
    reader: KeptReads | None = None,
) -> bool:
    """Fleet starvation gauges: tier fill, denials, mover depth (issue #661).

    Read-only derivation from records the fleet already files, following this
    module's conventions: restart-safe gauges (never counters), absent rather
    than zero where nothing was measured, no action-key labels, and a partial
    snapshot that reports its failure rather than dropping the scrape.
    """

    reader = _fresh(reader)
    ok = True
    try:
        queue = pool.PoolQueue(queue_root)
    except Exception:
        return False

    # -- per-tier fill supply and token occupancy -------------------------
    try:
        announced = {str(record.get("tier_id")): record
                     for record in _tiers(reader, queue)
                     if isinstance(record, dict) and record.get("tier_id")}
    except Exception:
        announced = {}
        ok = False
    try:
        tier_ids = _tier_ids(reader, queue)
    except Exception:
        tier_ids = sorted(announced)
        ok = False
    fill = metrics.family(
        "prismabuild_tier_fill_supply_mb_s",
        "Learned pool-side fill bandwidth per tier from the tier loop's latest announcement; "
        "best is the highest delivery since the last ceiling, ceiling the most recent measured "
        "shortfall, and probe_offer the live selected offer over a standing ceiling -- the "
        "larger of the fold's ceiling-plus-median reader and the ceiling plus the oldest ready "
        "mover's sealed demand. Absent, not zero, where the tier announced none.",
    )
    tokens = metrics.family(
        "prismabuild_tier_tokens",
        "Tier ledger tokens by kind and state; capacity kinds count GiB, the fill kind counts "
        "pool-side MB/s, and held is capacity less available.",
    )
    announce_age = metrics.family(
        "prismabuild_tier_announce_age_seconds",
        "Seconds since the tier loop last announced this tier (announced_unix). A live loop, "
        "idle or mid-cycle, keeps it below pool.OFFER_TIMEOUT_S; a loop stuck inside a cycle "
        "stops writing and it grows past that (#1115). Absent where no record was announced.",
    )
    for tier_id in sorted(set(tier_ids) | set(announced)):
        record = announced.get(tier_id)
        if isinstance(record, Mapping):
            stamped = _number(record.get("announced_unix"))
            if stamped is not None:
                announce_age.add(max(0.0, now - stamped), tier=tier_id)
        supply = record.get("fill_supply") if isinstance(record, Mapping) else None
        if isinstance(supply, Mapping):
            fill.add(supply.get("best_mb_s"), tier=tier_id, stat="best")
            fill.add(supply.get("ceiling_mb_s"), tier=tier_id, stat="ceiling")
            fill.add(supply.get("probe_offer_mb_s"), tier=tier_id,
                     stat="probe_offer")
        try:
            counted = _ledger_counts(reader, queue, tier_id)
        except Exception as exc:  # noqa: BLE001 -- as the plain read's failure
            counted = exc
        if isinstance(counted, Exception):
            ok = False
            continue
        capacity, available = counted
        if not isinstance(capacity, dict) or not isinstance(available, dict):
            ok = False
            continue
        for kind in sorted(set(capacity) | set(available)):
            held_back = _number(capacity.get(kind, 0))
            free_back = _number(available.get(kind, 0))
            if held_back is None or free_back is None:
                ok = False
                continue
            tokens.add(held_back, tier=tier_id, resource=str(kind), state="capacity")
            tokens.add(free_back, tier=tier_id, resource=str(kind), state="available")
            tokens.add(held_back - free_back, tier=tier_id, resource=str(kind), state="held")

    # -- per-host denial counts by reason, windowed ------------------------
    try:
        denials, _denial_notes = pbstatus._pool_claim_denials(queue)
    except Exception:
        denials = []
        ok = False
    counts: dict[tuple[str, str], int] = defaultdict(int)
    for denial in denials:
        if not isinstance(denial, dict):
            ok = False
            continue
        host = _host(denial.get("host"))
        reason = denial.get("reason")
        reason = reason if isinstance(reason, str) and reason else "unknown"
        age = _number(now - float(denial.get("denied_unix", -math.inf)))
        if host is None or age is None:
            ok = False
            continue
        if age <= window_seconds:
            counts[(host, reason)] += 1
    metrics.family(
        "prismabuild_claim_denial_window_seconds",
        "Configured lookback window for the per-host claim-denial gauges.",
    ).add(window_seconds)
    denials_family = metrics.family(
        "prismabuild_claim_denials",
        "Latest claim denial per action generation observed in the window, by host and reason; "
        "this is a restart-safe gauge, not a counter.",
    )
    for (host, reason), count in counts.items():
        denials_family.add(count, host=host, reason=reason)

    # -- mover queue depth --------------------------------------------------
    # An active action with movement-node shape: its residency block names a
    # byte range to stage, where a consumer's names leads.  The same range
    # test ``movers_claimed_on_tier`` stages on, counted here rather than
    # joined there so the depth needs no tier argument.
    depth = metrics.family(
        "prismabuild_mover_queue_depth",
        "Active actions with movement-node shape (a residency range to stage), by queue state.",
    )
    for state in (pool.READY, pool.CLAIMED):
        movers = 0
        try:
            listed = reader.queue_records(Path(queue_root) / state)
        except OSError:
            ok = False
            continue
        for _path, parsed in listed:
            record = parsed.record
            if record is None:
                ok = False
                continue
            if not isinstance(record, dict):
                ok = False
                continue
            residency = record.get("residency")
            if isinstance(residency, Mapping) and "range_start_bytes" in residency:
                movers += 1
        depth.add(movers, state=state)
    return ok


#: How many of the newest mover receipts one snapshot examines. The
#: ``movers/`` directory is append-only with one JSON per movement node, so
#: the scan is bounded by file modification time and the content window is
#: applied after the read -- the same shape as the terminal record limit,
#: with its own constant because the two populations differ.
_MOVE_RECEIPT_CAP = 1000

#: Tier label for a record that names no usable tier. One value, never an
#: action key: the unknown bucket keeps the series accounting complete
#: without growing label cardinality with the queue.
_UNKNOWN_TIER = "unknown"


def _tier(value: object) -> str | None:
    text = value if isinstance(value, str) else None
    # The shape ``PoolQueue._check_tier_id`` enforces: a tier id names one
    # tier, never a demand kind and never a path.
    if not text or text.startswith(".") or "/" in text or "@" in text:
        return None
    return text


#: A record that could not be read or did not parse: never kept, so it is read
#: again next scrape.
_UNREADABLE = object()
#: A file in ``movers/`` that is not a movement receipt (an egress record).
_NOT_A_RECEIPT = object()


def _receipt_projection(entry: _KeptEntry):
    """The six numbers the move gauges read from one receipt.

    Kept on the receipt's entry instead of the record, because 7,500
    receipts' records are tens of megabytes an exporter has no use for.
    """

    try:
        record = pool._read_json(Path(entry.path))
    except (OSError, ValueError):
        record = None
    if not isinstance(record, dict):
        return _UNREADABLE
    if record.get("schema") != pool.POOL_MOVE_SCHEMA_V1:
        return _NOT_A_RECEIPT
    pacing = record.get("disk_pacing")
    pacing = pacing if isinstance(pacing, Mapping) else {}
    return (
        _number(record.get("unix")),
        _tier(record.get("tier_id")) or _UNKNOWN_TIER,
        # One definition of "this window measured the pool": the same share
        # test the fill fold prices from, so the gauge and the mint cannot
        # disagree about which receipts count.
        bool(pbstatus.storage_tiers._measured_the_pool(record)),
        _number(pacing.get("mean_pool_read_mb_s")),
        _number(record.get("bytes_staged")),
        _number(pacing.get("pool_read_bytes")),
    )


@dataclass(frozen=True)
class _PlanCensus:
    """What the plan gauges take from one ``pbstatus._starvation_plan_entry``.

    ``legs`` is ``None`` when the entry carried no cursor gap mapping, and a
    leg's counts are ``None`` when its gap was malformed; both fail the
    collection, as they did when the gauges read the entry directly.
    """

    valid: bool
    unreadable: bool
    tier_id: str = ""
    state: str = ""
    legs: tuple | None = None


def _plan_slot(reader: KeptReads, entry: _KeptEntry) -> dict:
    reader.used("plan", entry)
    if entry.derived is None:
        entry.derived = {}
    return entry.derived  # type: ignore[return-value]


def _plan_tier(reader: KeptReads, entry: _KeptEntry):
    """The tier a filed plan names; ``None`` for an empty file.

    ``_UNREADABLE`` for a plan that cannot be read or does not validate.  A
    plan file is written once (``residency_plan.freeze``), so its tier is
    kept for as long as the file's version holds.
    """

    slot = _plan_slot(reader, entry)
    if "tier" in slot:
        reader.reused += 1
        return slot["tier"]
    reader.computed += 1
    try:
        raw = pool._read_json(Path(entry.path))
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        return _UNREADABLE
    if raw is None:
        return None
    try:
        tier_id = str(pbstatus.residency_plan.validate_plan(raw)["tier_id"])
    except (ValueError, KeyError):
        return _UNREADABLE
    slot["tier"] = tier_id
    return tier_id


def _project_plan(entry: Mapping[str, object], unreadable: list[str]) -> _PlanCensus:
    """``_promotion_metrics``' reading of one plan entry, as numbers."""

    if not entry.get("valid"):
        return _PlanCensus(valid=False, unreadable=bool(unreadable))
    gap = entry.get("cursor_gap")
    legs: tuple | None = None
    if isinstance(gap, Mapping):
        counted = []
        for leg in ("stage", "ram"):
            leg_gap = gap.get(leg)
            if not isinstance(leg_gap, Mapping):
                counted.append((leg, None))
                continue
            staged = leg_gap.get("staged_phases")
            phases = leg_gap.get("unstaged_phases")
            missing = leg_gap.get("unstaged_bytes")
            if (type(staged) is not int or not isinstance(phases, list)
                    or type(missing) not in (int, float)):
                counted.append((leg, None))
                continue
            counted.append((leg, (staged, len(phases), float(missing))))
        legs = tuple(counted)
    return _PlanCensus(valid=True, unreadable=bool(unreadable),
                       tier_id=str(entry["tier_id"]), state=str(entry["state"]),
                       legs=legs)


def _plan_inputs(reader: KeptReads, queue: pool.PoolQueue, key: str,
                 movers: frozenset[str], ram_tier_id: str | None, *,
                 ready: set[str], claimed: set[str], claimed_listed: set[str],
                 announced: Mapping[str, Mapping[str, object]],
                 receipts: Mapping[str, _KeptEntry] | None) -> tuple | None:
    """Everything one plan's census reads outside its fenced directories.

    ``pbstatus._starvation_plan_entry`` reads, besides the plan itself, the
    fragment directory and the tier ledgers (fenced by their stamps), and:

    * the consumer's queue state and, when it is claimed, the phase its
      lease accepted -- the only thing it takes from the lease;
    * whether each of the plan's movers is ready or claimed, and whether a
      claim file is listed for it at all (``resident_movers`` asks the
      filesystem, which also sees a claim too torn to parse);
    * each mover's receipt in ``movers/``, compared by the kept entry, which
      is replaced whenever its file's version changes;
    * for a ram leg, the announced tier's ``epoch`` and ``mountpoint``, the
      two fields ``residency_plan.resident_movers`` reads from it.  The
      record itself is re-announced every tier-loop cycle, so its version
      would miss every scrape.

    ``None`` when one of them could not be read, which is never kept.  Each
    is read from this scrape's listings, and the census reads them afresh
    when it runs, so a change between the two is caught the scrape after:
    the listing then differs from what was kept.
    """

    if receipts is None:
        return None
    state = ("claimed" if key in claimed else "ready" if key in ready else "absent")
    accepted = None
    if state == "claimed":
        lease = pbstatus._starvation_sidecar(queue.lease_path(key))
        if isinstance(lease, Exception):
            return None
        accepted = pbstatus._starvation_accepted_phase(lease)
    ram = None
    if ram_tier_id is not None:
        record = announced.get(ram_tier_id)
        ram = (None if record is None
               else (record.get("epoch"), record.get("mountpoint")))
    ordered = sorted(movers)
    return (state, accepted, frozenset(movers & ready), frozenset(movers & claimed),
            frozenset(movers & claimed_listed),
            tuple(receipts.get(mover) for mover in ordered), ram_tier_id, ram)


def _plan_fences(reader: KeptReads, queue: pool.PoolQueue, key: str,
                 tiers: list[str | None]) -> tuple | None:
    """Fences on the fragment directory and the ledgers one plan's census reads.

    ``None`` when any of them cannot be trusted; the census is then taken
    every scrape.
    """

    fragments = reader.fence(queue.residency_fragment_root() / key)
    if fragments is None:
        return None
    taken = [fragments]
    for tier in tiers:
        if tier is None:
            continue
        try:
            ledger = queue.tier_ledger(tier)
        except (OSError, ValueError, pool.PoolContractError):
            return None
        ledger_fences = reader.ledger_fences(ledger)
        if ledger_fences is None:
            return None
        taken.extend(ledger_fences)
    return tuple(taken)


def _plan_census(reader: KeptReads, queue: pool.PoolQueue, path: Path,
                 entry: _KeptEntry, *, ready: set[str], claimed: set[str],
                 claimed_listed: set[str],
                 announced: Mapping[str, Mapping[str, object]],
                 receipts: Mapping[str, _KeptEntry] | None,
                 now: float) -> _PlanCensus:
    """One plan's census, reused while nothing it read has changed.

    The census (``pbstatus._starvation_plan_entry``) lists the consumer's
    fragment directory, reads every fragment, counts each mover's ledger
    tokens and reads their receipts: on the live queue, 330 plans and 97 MB
    of plan documents every scrape.  It is kept on the plan's entry with a
    fence on the fragment directory and on the ledgers of the plan's tiers
    (:meth:`KeptReads.ledger_fences`), and with :func:`_plan_inputs`, and is
    reused while all of them hold.  ``now`` is passed through; the census
    does not read it.
    """

    slot = _plan_slot(reader, entry)
    key = path.stem
    kept = slot.get("census")
    if kept is not None:
        fences, movers, ram_tier_id, inputs, census = kept
        if (_plan_inputs(reader, queue, key, movers, ram_tier_id, ready=ready,
                         claimed=claimed, claimed_listed=claimed_listed,
                         announced=announced,
                         receipts=receipts) == inputs
                and reader.holds(fences)):
            reader.reused += 1
            return census
        slot.pop("census", None)
    reader.computed += 1
    raw = _read_or_exception(path)
    if isinstance(raw, Exception):
        return _PlanCensus(valid=False, unreadable=True)
    fences = inputs = movers = ram_tier_id = None
    try:
        plan = pbstatus.residency_plan.validate_plan(raw) if isinstance(raw, Mapping) else None
    except ValueError:
        plan = None
    if plan is not None:
        movers = frozenset(pbstatus.residency_plan.mover_keys(plan))
        ram = plan.get("ram_tier_id")
        ram_tier_id = str(ram) if isinstance(ram, str) else None
        fences = _plan_fences(reader, queue, key,
                              [str(plan["tier_id"]), ram_tier_id])
        inputs = _plan_inputs(reader, queue, key, movers, ram_tier_id, ready=ready,
                              claimed=claimed, claimed_listed=claimed_listed,
                              announced=announced,
                              receipts=receipts)
    notes: list[str] = []
    unreadable: list[str] = []
    census = _project_plan(pbstatus._starvation_plan_entry(
        queue, key, raw, ready=ready, claimed=claimed, notes=notes,
        unreadable=unreadable, now=now), unreadable)
    # Only a census with nothing to report is kept: a ledger holder that
    # could not be read is a note, not an unreadable plan, and its ``False``
    # must be read again rather than kept.
    if (census.valid and not census.unreadable and not notes
            and fences is not None and inputs is not None
            and reader.still(fences)):
        slot["census"] = (fences, movers, ram_tier_id, inputs, census)
    return census


def _promotion_metrics(
    metrics: Metrics,
    queue_root: Path,
    *,
    now: float,
    window_seconds: float,
    reader: KeptReads | None = None,
) -> bool:
    """Per-tier promotion lag, mover depth, and delivery (issue #660 backend).

    Read-only derivation from records the fleet already files, following this
    module's conventions: restart-safe gauges (never counters), absent rather
    than zero where nothing was measured, no action-key labels, and a partial
    snapshot that reports its failure rather than dropping the scrape.

    Three questions feed the ``pb_starvation`` / ``pb_cursors`` / ``pb_movers``
    observation tools: how many movers are queued per tier and state, how long
    the oldest waiter on each tier has waited for its data, and what the
    tier's recent movers reported delivering. Waiting reuses
    :mod:`pbstatus`' quiet/grace rule so the gauge and the ``--starvation``
    blob cannot disagree about who is waiting; the cursor-gap backlog reuses
    its plan census for the same reason. The per-tier move series are what
    the fill fold prices from: jobs beside measured jobs is the resident-hit
    share (adoption served resident versus windows that read the pool), and
    the best pool-side rate is the demonstrated delivery.

    Costs one walk over the active queue, one bounded walk over the newest
    mover receipts, one lease read per claimed consumer, and one plan census
    over the filed residency plans (leases plus fragments per plan, exactly
    as ``read_starvation`` reads them).
    """

    reader = _fresh(reader)
    ok = True
    try:
        queue = pool.PoolQueue(queue_root)
    except Exception:
        return False

    try:
        announced = {str(record.get("tier_id")): record
                     for record in _tiers(reader, queue)
                     if isinstance(record, dict) and record.get("tier_id")}
    except Exception:
        announced = {}
        ok = False
    try:
        tier_ids = _tier_ids(reader, queue)
    except Exception:
        tier_ids = sorted(announced)
        ok = False
    tiers = sorted(set(tier_ids) | set(announced))

    # -- one active-queue walk: per-tier mover depth, consumer candidates --
    depth: dict[tuple[str, str], int] = defaultdict(int)
    ready: set[str] = set()
    claimed: set[str] = set()
    claimed_listed: set[str] = set()
    consumers: list[str] = []
    for state in (pool.READY, pool.CLAIMED):
        try:
            listed = reader.queue_records(Path(queue_root) / state)
        except OSError:
            ok = False
            continue
        for path, parsed in listed:
            key = path.name[:-len(".json")]
            if state == pool.CLAIMED:
                claimed_listed.add(key)
            record = parsed.record
            if not isinstance(record, dict) or record.get("action_key") != key:
                # The census sets below classify plans; a row nobody could
                # read classifies nothing, exactly as read_pool treats it.
                if not isinstance(record, dict):
                    ok = False
                continue
            (claimed if state == pool.CLAIMED else ready).add(key)
            residency = record.get("residency")
            if not isinstance(residency, Mapping):
                continue
            if "range_start_bytes" in residency:
                depth[(_tier(residency.get("tier_id")) or _UNKNOWN_TIER,
                       state)] += 1
            elif state == pool.CLAIMED and residency.get("leads"):
                consumers.append(key)
    tier_depth = metrics.family(
        "prismabuild_mover_queue_depth_tier",
        "Active actions with movement-node shape (a residency range to stage), "
        "by tier and queue state. Known tiers always read, zero included; the "
        "single unknown tier reads only when a record names no usable tier.",
    )
    for tier_id in tiers:
        for state in (pool.READY, pool.CLAIMED):
            tier_depth.add(depth.get((tier_id, state), 0),
                           tier=tier_id, state=state)
    for (tier_id, state), count in sorted(depth.items()):
        if tier_id not in tiers:
            tier_depth.add(count, tier=tier_id, state=state)

    # -- waiting consumers per tier: the promotion lag ----------------------
    # The tier join is the consumer's frozen plan: a waiting claim names its
    # leads, the plan names the tier. A waiter with no readable plan still
    # counts, under the unknown tier, because "waiting and tierless" is the
    # signal, not a reason to drop the row.
    plan_root = queue.root / pool.RESIDENCY_PLANS
    try:
        plan_listing = reader.entries(plan_root, select=_json_name)
        plan_listing_error: OSError | None = None
    except OSError as exc:
        plan_listing, plan_listing_error = [], exc
    # A plan that could not be stat-ed is counted below as an unreadable
    # plan, which is what main's read of it reported; it names no tier here.
    plan_files = {path.name: entry for path, entry in plan_listing
                  if entry is not None}
    waiting: dict[str, int] = defaultdict(int)
    quiet_max: dict[str, float] = {}
    for key in consumers:
        lease = pbstatus._starvation_sidecar(queue.lease_path(key))
        if isinstance(lease, Exception):
            ok = False
            continue
        quiet, grace = pbstatus._starvation_quiet(
            lease if isinstance(lease, dict) else None)
        child = pbstatus._starvation_child(
            lease if isinstance(lease, dict) else None)
        is_waiting, _rule = pbstatus._starvation_waiting(quiet, grace, child)
        if not is_waiting:
            continue
        # No plan filed is the ordinary answer for a consumer the coordinator
        # never staged, which still counts as a waiter whose tier is unknown
        # rather than as a read failure.
        plan_entry = plan_files.get(queue.residency_plan_path(key).name)
        tier_id = _UNKNOWN_TIER
        if plan_entry is not None:
            named = _plan_tier(reader, plan_entry)
            if isinstance(named, str):
                tier_id = named
            elif named is _UNREADABLE:
                ok = False
        waiting[tier_id] += 1
        if quiet is not None:
            quiet_max[tier_id] = max(quiet_max.get(tier_id, quiet), quiet)
    waiting_family = metrics.family(
        "prismabuild_starvation_waiting_consumers",
        "Claimed consumers the quiet/grace rule reads as waiting on data, by "
        "the tier their frozen plan names. Known tiers always read, zero "
        "included; the unknown tier counts waiters with no readable plan.",
    )
    for tier_id in tiers:
        waiting_family.add(waiting.get(tier_id, 0), tier=tier_id)
    for tier_id, count in sorted(waiting.items()):
        if tier_id not in tiers:
            waiting_family.add(count, tier=tier_id)
    quiet_family = metrics.family(
        "prismabuild_starvation_waiting_quiet_max_seconds",
        "Longest progress quiet among the tier's waiting consumers; absent, "
        "not zero, where none is waiting.",
    )
    for tier_id, value in sorted(quiet_max.items()):
        quiet_family.add(value, tier=tier_id)

    # -- plan backlog per tier: the cursor-gap census as gauges --------------
    plans: dict[tuple[str, str], int] = defaultdict(int)
    staged_phases: dict[tuple[str, str], int] = defaultdict(int)
    unstaged_phases: dict[tuple[str, str], int] = defaultdict(int)
    unstaged_bytes: dict[tuple[str, str], float] = defaultdict(float)
    invalid_plans = 0
    unreadable = False
    # ``DirectoryRecords`` reads an absent directory as empty: no staged
    # consumer has ever filed a plan.
    if plan_listing_error is not None:
        ok = False
    try:
        receipts = {entry.name[:-len(".json")]: entry for entry
                    in reader.history(queue.root / pool.MOVERS, select=_json_name)}
    except OSError:
        receipts = None
    for path, entry in plan_listing:
        if entry is None:
            invalid_plans += 1
            unreadable = True
            continue
        census = _plan_census(reader, queue, path, entry, ready=ready,
                              claimed=claimed, claimed_listed=claimed_listed,
                              announced=announced,
                              receipts=receipts, now=now)
        unreadable = unreadable or census.unreadable
        if not census.valid:
            invalid_plans += 1
            continue
        plans[(census.tier_id, census.state)] += 1
        if census.legs is None:
            ok = False
            continue
        for leg, counted in census.legs:
            if counted is None:
                ok = False
                continue
            staged, phases, missing = counted
            staged_phases[(census.tier_id, leg)] += staged
            unstaged_phases[(census.tier_id, leg)] += phases
            unstaged_bytes[(census.tier_id, leg)] += missing
    if unreadable:
        ok = False
    plans_family = metrics.family(
        "prismabuild_residency_plans",
        "Filed residency plans by tier and consumer queue state; the invalid "
        "state counts unreadable or rejected plans under the unknown tier.",
    )
    for tier_id in tiers:
        for state in ("claimed", "ready", "absent"):
            plans_family.add(plans.get((tier_id, state), 0),
                             tier=tier_id, state=state)
    for (tier_id, state), count in sorted(plans.items()):
        if tier_id not in tiers:
            plans_family.add(count, tier=tier_id, state=state)
    if invalid_plans:
        plans_family.add(invalid_plans, tier=_UNKNOWN_TIER, state="invalid")
    staged_family = metrics.family(
        "prismabuild_residency_staged_phases",
        "Plan phases at or behind the read cursor the tier ledger says are "
        "staged, by tier and leg: the promoted frontier behind the cursor.",
    )
    unstaged_family = metrics.family(
        "prismabuild_residency_unstaged_phases",
        "Plan phases at or ahead of the read cursor the tier ledger does not "
        "say are staged, by tier and leg: the promotion backlog.",
    )
    backlog_family = metrics.family(
        "prismabuild_residency_unstaged_bytes",
        "Bytes in the promotion backlog, by tier and leg.",
    )
    for tier_id in tiers:
        for leg in ("stage", "ram"):
            staged_family.add(staged_phases.get((tier_id, leg), 0),
                              tier=tier_id, leg=leg)
            unstaged_family.add(unstaged_phases.get((tier_id, leg), 0),
                                tier=tier_id, leg=leg)
            backlog_family.add(unstaged_bytes.get((tier_id, leg), 0.0),
                               tier=tier_id, leg=leg)

    # -- mover receipts per tier: demonstrated delivery in the window --------
    # Bounded by newest file modification time, windowed by content unix: the
    # receipts directory is append-only, one JSON per movement node, and a
    # scrape that reads all of them to report a handful is the slow path.
    # The pool-read sum can exceed the staged sum: it counts every pool
    # reader's sectors during each mover's window, not one copy's bytes, so
    # the adoption signal is jobs beside measured jobs, never the byte
    # ratio. Byte sums gate on receipts carrying BOTH counters, so each sum
    # covers one population, not two.
    # ``KeptReads.history`` reads an absent directory as empty: no mover has
    # ever filed a receipt.  Any other failure, a receipt that could not be
    # stat-ed included, is main's: no window, and an incomplete scrape.
    if receipts is None:
        receipts = {}
        ok = False
    candidates = heapq.nlargest(_MOVE_RECEIPT_CAP,
                                ((entry.mtime, entry.path, entry)
                                 for entry in receipts.values()))
    move_complete = len(receipts) <= _MOVE_RECEIPT_CAP
    move_jobs: dict[str, int] = defaultdict(int)
    measured_jobs: dict[str, int] = defaultdict(int)
    move_staged: dict[str, float] = defaultdict(float)
    move_pool_read: dict[str, float] = defaultdict(float)
    move_rates: dict[str, list[float]] = defaultdict(list)
    for _mtime, _path, entry in candidates:
        receipt = reader.derive("receipt", entry,
                                lambda entry=entry: _receipt_projection(entry),
                                keep=lambda projected: projected is not _UNREADABLE)
        if receipt is _UNREADABLE:
            ok = False
            continue
        if receipt is _NOT_A_RECEIPT:
            continue
        stamped, tier_id, measured, rate, staged, pool_read = receipt
        if stamped is None or not -_SKEW_S <= now - stamped <= window_seconds:
            continue
        move_jobs[tier_id] += 1
        if measured:
            measured_jobs[tier_id] += 1
        if rate is not None and rate > 0:
            move_rates[tier_id].append(rate)
        if staged is not None and pool_read is not None:
            move_staged[tier_id] += staged
            move_pool_read[tier_id] += pool_read
    metrics.family(
        "prismabuild_tier_move_window_seconds",
        "Configured lookback window for the per-tier movement receipt gauges.",
    ).add(window_seconds)
    metrics.family(
        "prismabuild_tier_move_window_complete",
        "Whether the mover-receipt scan proved the window was not truncated "
        "by its file cap and every selected receipt was readable.",
    ).add(1 if move_complete and ok else 0)
    move_jobs_family = metrics.family(
        "prismabuild_tier_move_jobs",
        "Movement receipts filed in the window, by tier; a restart-safe "
        "gauge, not a counter.",
    )
    for tier_id in tiers:
        move_jobs_family.add(move_jobs.get(tier_id, 0), tier=tier_id)
    for tier_id, count in sorted(move_jobs.items()):
        if tier_id not in tiers:
            move_jobs_family.add(count, tier=tier_id)
    measured_family = metrics.family(
        "prismabuild_tier_move_measured_jobs",
        "In-window receipts whose window read the staged bytes off the pool, "
        "by tier, under the fill fold's own share test; the remainder is "
        "adoption served resident. Jobs beside measured is the hit rate.",
    )
    for tier_id in tiers:
        measured_family.add(measured_jobs.get(tier_id, 0), tier=tier_id)
    for tier_id, count in sorted(measured_jobs.items()):
        if tier_id not in tiers:
            measured_family.add(count, tier=tier_id)
    staged_bytes_family = metrics.family(
        "prismabuild_tier_move_staged_bytes",
        "Bytes movers reported staging in the window, by tier, over receipts "
        "carrying both byte counters; absent, not zero, where none did.",
    )
    for tier_id, value in sorted(move_staged.items()):
        staged_bytes_family.add(value, tier=tier_id)
    pool_read_family = metrics.family(
        "prismabuild_tier_move_pool_read_bytes",
        "Pool sector reads during movers' windows in the window, by tier, "
        "over the same receipts as the staged sum. It counts every pool "
        "reader, not one copy's bytes, so it can exceed the staged sum; "
        "read it beside measured jobs, never as a ratio's denominator.",
    )
    for tier_id, value in sorted(move_pool_read.items()):
        pool_read_family.add(value, tier=tier_id)
    rate_family = metrics.family(
        "prismabuild_tier_move_pool_rate_mb_s",
        "Pool-side delivery the tier's movers demonstrated in the window, by "
        "tier: best is the highest pool delivery any receipt measured, mean "
        "the average over receipts. Absent, not zero, with no measured rate.",
    )
    rate_jobs_family = metrics.family(
        "prismabuild_tier_move_rate_jobs",
        "Receipts contributing to the tier's pool-rate gauge in the window.",
    )
    for tier_id, rates in sorted(move_rates.items()):
        rate_family.add(max(rates), tier=tier_id, stat="best")
        rate_family.add(sum(rates) / len(rates), tier=tier_id, stat="mean")
        rate_jobs_family.add(len(rates), tier=tier_id)
    return ok


def collect_metrics(
    queue_root: str | Path = DEFAULT_QUEUE_ROOT,
    *,
    now: float | None = None,
    terminal_window_seconds: float = DEFAULT_TERMINAL_WINDOW_SECONDS,
    terminal_limit: int = DEFAULT_TERMINAL_LIMIT,
    previous: dict[tuple[str, str], tuple[float, float]] | None = None,
    reader: KeptReads | None = None,
) -> str:
    """Collect one non-atomic, read-only snapshot in Prometheus text format.

    ``reader`` is what the caller keeps between scrapes (``MetricsCache``
    holds one); omitted, every record is read afresh, as ``--once`` wants.
    The text is the same either way: a kept read is reused only while the
    directory it came from provably has not changed.

    ``previous`` is the caller's store of the last refresh's per-attempt
    counters, replaced in place. Passing it opts into the recent-cores rate;
    omitting it -- which ``--once`` does, having only one moment to report --
    simply leaves that family out. The store belongs to the caller rather than
    to this module so that two collectors cannot silently difference against
    each other's readings.
    """
    started = time.monotonic()
    reader = KeptReads() if reader is None else reader
    reader.begin()
    try:
        with pbstatus.kept_reads(reader):
            return _collect(queue_root, now=now,
                            terminal_window_seconds=terminal_window_seconds,
                            terminal_limit=terminal_limit, previous=previous,
                            reader=reader, started=started)
    finally:
        reader.end()


def _collect(
    queue_root: str | Path,
    *,
    now: float | None,
    terminal_window_seconds: float,
    terminal_limit: int,
    previous: dict[tuple[str, str], tuple[float, float]] | None,
    reader: KeptReads,
    started: float,
) -> str:
    sampled = time.time() if now is None else float(now)
    root = Path(queue_root).absolute()
    metrics = Metrics()
    success = True
    try:
        census = pbstatus.read_pool(root)
    except Exception:  # A scrape reports the failure rather than dropping HTTP.
        census = {"nodes": [], "jobs": [], "queue": {"ready": None, "claimed": None}}
        success = False
    # The census lists ``workers/`` already; this only asks that it be there.
    if reader.version(root / pool.WORKERS) is None:
        success = False

    queue_summary = census.get("queue", {})
    queue_items = metrics.family(
        "prismabuild_queue_items", "Readable pull-queue records by active state.")
    oldest = metrics.family(
        "prismabuild_queue_oldest_age_seconds",
        "Age of the oldest readable record in each active queue state; zero means that readable state is empty.",
    )
    jobs = census.get("jobs", []) if isinstance(census.get("jobs"), list) else []
    releases = metrics.family(
        "prismabuild_queue_unstarted_releases",
        "Releases without an attempt carried by readable records now in each active queue state; this counts what the queue is still holding, not what happened in a window.",
    )
    for state in ("ready", "claimed"):
        count = queue_summary.get(state) if isinstance(queue_summary, Mapping) else None
        if _number(count) is None:
            success = False
            continue
        queue_items.add(count, state=state)
        state_jobs = [row for row in jobs if row.get("state") == state.upper()]
        releases.add(sum(int(row.get("unstarted_releases") or 0) for row in state_jobs),
                     state=state)
        if not state_jobs:
            oldest.add(0, state=state)
        else:
            ages = [_number(row.get("age_s")) for row in state_jobs]
            if all(age is not None for age in ages):
                oldest.add(max(ages), state=state)
            else:
                success = False

    worker_up = metrics.family(
        "prismabuild_worker_up",
        "Whether a syntactically valid worker offer is fresh enough for pool placement.",
    )
    offer_age = metrics.family(
        "prismabuild_worker_offer_age_seconds", "Age of a valid worker's last offer.")
    capacity = metrics.family(
        "prismabuild_worker_capacity",
        "Configured fresh-worker capacity; cpu is cores, gpu is devices, and memory resources are bytes.",
    )
    observed_capacity = metrics.family(
        "prismabuild_worker_observed_capacity",
        "Windowed fresh-worker capacity after foreign work; units follow worker capacity.",
    )
    memory_domain = metrics.family(
        "prismabuild_worker_memory_domain_info",
        "Fresh worker GPU memory domains; value is always one.",
    )
    memory_available = metrics.family(
        "prismabuild_worker_memory_available_bytes",
        "Coarse available host memory from the fresh worker offer, converted from integer GiB.",
    )
    loops = metrics.family(
        "prismabuild_worker_loops",
        "PrismaBuild worker loops running on the box that wrote this fresh offer; "
        "absent, not zero, when the offer did not measure it.",
    )
    evidence_age = metrics.family(
        "prismabuild_admission_evidence_age_seconds",
        "Age of the last persisted admission evidence; this is not continuous hardware monitoring.",
    )
    plateau = metrics.family(
        "prismabuild_admission_plateau",
        "Whether the last persisted GPU admission decision recorded a power-response plateau; this is not live hardware state.",
    )

    valid_hosts: set[str] = set()
    fresh_hosts: set[str] = set()
    resource_dimensions: dict[str, set[str]] = defaultdict(set)
    for node in census.get("nodes", []):
        host = _host(node.get("node"))
        state = node.get("state")
        if host is None or state not in {"live", "stale"}:
            success = False
            continue
        valid_hosts.add(host)
        fresh = state == "live"
        worker_up.add(1 if fresh else 0, host=host)
        age = _number(node.get("age_s"))
        if age is not None:
            offer_age.add(age, host=host)
        else:
            success = False
        if fresh:
            fresh_hosts.add(host)
            for resource, value in _resource_samples(node.get("capacity")):
                capacity.add(value, host=host, resource=resource)
                resource_dimensions[host].add(resource)
            for resource, value in _resource_samples(node.get("observed_capacity")):
                observed_capacity.add(value, host=host, resource=resource)
            detail = node.get("observed_detail")
            if isinstance(detail, Mapping):
                domains = detail.get("gpu_memory_domains")
                if isinstance(domains, list):
                    normalized = {
                        domain if isinstance(domain, str) and domain in {
                            "shared_system", "discrete", "unknown"
                        } else "unknown"
                        for domain in domains
                    }
                    for domain in sorted(normalized):
                        memory_domain.add(1, host=host, domain=domain)
                available_gib = _number(detail.get("mem_available_gb"))
                if available_gib is not None:
                    memory_available.add(available_gib * GIB, host=host)
            # A pre-#254 offer carries no count.  That is a missing series and
            # not a zero one, and it is emphatically not a scrape failure:
            # every loop published before the field existed announces without
            # it, so setting ``success = False`` here would mark the whole
            # scrape bad for the duration of a rolling generation change.
            count = node.get("loops")
            if isinstance(count, int) and not isinstance(count, bool) and count >= 0:
                loops.add(count, host=host)
        admission = node.get("admission")
        if isinstance(admission, Mapping):
            for resource in ("cpu", "gpu"):
                sample = admission.get(resource)
                if not isinstance(sample, Mapping) or sample.get("state") == "not applicable":
                    continue
                age = _number(sample.get("age_s"))
                if age is not None:
                    evidence_age.add(age, host=host, resource=resource)
                record = sample.get("record")
                if resource == "gpu" and isinstance(record, Mapping):
                    feedback = record.get("power_feedback")
                    is_plateau = isinstance(feedback, Mapping) and feedback.get("status") == "plateau"
                    plateau.add(1 if is_plateau else 0, host=host, resource="gpu")

    claimed_known = _number(queue_summary.get("claimed")) is not None
    active = metrics.family(
        "prismabuild_active_jobs",
        "Claims with fresh leases by host and mutually exclusive cpu or gpu job kind.",
    )
    reserved = metrics.family(
        "prismabuild_reserved_resources",
        "Declared resources in valid claimed queue records; units follow worker capacity.",
    )
    active_by_host: dict[str, dict[str, int]] = defaultdict(lambda: {"cpu": 0, "gpu": 0})
    reserved_by_host: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    live_jobs: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    unknown_active_hosts: set[str] = set()
    if claimed_known:
        for job in jobs:
            if job.get("state") != "CLAIMED":
                continue
            host = _host(job.get("node"))
            if host is None:
                success = False
                continue
            valid_hosts.add(host)
            for resource, value in _resource_samples(job.get("resources")):
                reserved_by_host[host][resource] += value
            if job.get("stale") is False:
                kind = "gpu" if dict(job.get("resources") or {}).get("gpu", 0) else "cpu"
                active_by_host[host][kind] += 1
                live_jobs[host].append(job)
            else:
                unknown_active_hosts.add(host)
        reported_hosts = fresh_hosts | set(active_by_host) | set(reserved_by_host)
        for host in sorted(reported_hosts):
            dimensions = resource_dimensions[host] | set(reserved_by_host[host])
            for resource in sorted(dimensions):
                reserved.add(reserved_by_host[host].get(resource, 0),
                             host=host, resource=resource)
            if host not in unknown_active_hosts:
                for kind in ("cpu", "gpu"):
                    active.add(active_by_host[host][kind], host=host, kind=kind)

    # A claim retained because its cleanup could not be proved is retried by a
    # local reaper forever, deliberately: concluding it would release tokens
    # for a payload nobody showed had stopped.  What that costs is a claim
    # nobody is looking at, so the retry loop has to be able to raise its hand
    # (#288).  Two aggregate series, not one per action: an alert asks "is
    # anything stuck and for how long", and an action key is unbounded
    # cardinality.
    pinned = metrics.family(
        "prismabuild_cleanup_pending_claims",
        "Claims on the host retained because their payload could not be proved stopped; the reservation is still held.",
    )
    pinned_age = metrics.family(
        "prismabuild_cleanup_pending_oldest_seconds",
        "Age of the oldest unproven cleanup on the host, from its first failure; absent when nothing is pending.",
    )
    if claimed_known:
        pinned_by_host: dict[str, int] = defaultdict(int)
        oldest_by_host: dict[str, float] = {}
        for job in jobs:
            if job.get("state") != "CLAIMED" or not job.get("cleanup_pending"):
                continue
            host = _host(job.get("node"))
            if host is None:
                continue
            pinned_by_host[host] += 1
            age = _number(job.get("cleanup_pending_s"))
            # A record written before #288 carries no first-failure stamp.
            # That is a missing series, never a zero one: reporting 0 would
            # say "just started" about a cleanup that may have been pending
            # for hours.
            if age is not None:
                oldest_by_host[host] = max(oldest_by_host.get(host, age), age)
        for host in sorted(fresh_hosts | set(pinned_by_host)):
            pinned.add(pinned_by_host.get(host, 0), host=host)
            if host in oldest_by_host:
                pinned_age.add(oldest_by_host[host], host=host)

    observations = _attempt_telemetry(pool.PoolQueue(root), live_jobs, sampled,
                                      reader=reader)
    observed = metrics.family(
        "prismabuild_attempt_observed_resources",
        "Aggregate complete fresh exact-scope observations for every live claim on a host; cpu is lifetime-average cores and memory is current bytes.",
    )
    observed_jobs = metrics.family(
        "prismabuild_attempt_telemetry_jobs",
        "Live claims represented in the host's complete aggregate attempt telemetry.",
    )
    unavailable_jobs = metrics.family(
        "prismabuild_attempt_telemetry_unavailable_jobs",
        "Live claims on the host whose resource-scope telemetry could not be used; the aggregate is withheld whenever this is above zero.",
    )
    telemetry_age = metrics.family(
        "prismabuild_attempt_telemetry_age_seconds",
        "Age of the oldest credible resource-scope sample among the host's live claims; reported even when the aggregate is withheld.",
    )
    recent = metrics.family(
        "prismabuild_attempt_recent_cores",
        "Cores used by the host's live claims since the previous refresh, differenced per attempt; absent until a second refresh has something to difference.",
    )
    recent_jobs = metrics.family(
        "prismabuild_attempt_recent_cores_jobs",
        "Live claims the host's recent-cores figure was differenced over; it is the denominator, not a total.",
    )
    peaks = metrics.family(
        "prismabuild_attempt_peak_resources",
        "What the host's live claims have used at their heaviest: memory_peak_bytes is the largest single claim's peak, and the io_ figures are the bytes they have moved between them. Absent rather than zero where the sampler measured nothing.",
    )
    for host, values in sorted(observations.items()):
        for name, value in sorted(values.peaks.items()):
            peaks.add(value, host=host, resource=name)
    for host, values in sorted(observations.items()):
        # The count of live claims and how stale the oldest is are reported for
        # every host that has any, so a withheld aggregate is legible as a
        # withheld aggregate rather than as an absent host.
        unavailable_jobs.add(values.unavailable, host=host)
        if values.oldest_age_s is not None:
            telemetry_age.add(values.oldest_age_s, host=host)
        if values.complete:
            observed.add(values.cpu, host=host, resource="cpu")
            observed.add(values.memory_bytes, host=host, resource="memory_bytes")
            observed_jobs.add(float(values.jobs), host=host)
    for host, (cores, covered) in sorted(_recent_cores(observations, previous).items()):
        recent.add(cores, host=host)
        recent_jobs.add(float(covered), host=host)

    try:
        success = _terminal_metrics(
            metrics, root, now=sampled,
            window_seconds=terminal_window_seconds, limit=terminal_limit,
            reader=reader,
        ) and success
    except Exception:
        success = False
        metrics.family(
            "prismabuild_terminal_collection_success",
            "Whether terminal directories and every selected terminal record were readable for this snapshot.",
        ).add(0)

    try:
        success = _release_metrics(
            metrics, root, now=sampled,
            window_seconds=terminal_window_seconds, limit=terminal_limit,
            reader=reader,
        ) and success
    except Exception:  # A scrape reports the failure rather than dropping HTTP.
        success = False
        metrics.family(
            "prismabuild_unstarted_release_events_complete",
            "Whether the unstarted-release scan proved the configured window was not truncated by its record limit and every selected filing was readable.",
        ).add(0)

    try:
        success = _starvation_metrics(
            metrics, root, now=sampled,
            window_seconds=terminal_window_seconds, reader=reader,
        ) and success
    except Exception:  # A scrape reports the failure rather than dropping HTTP.
        success = False

    try:
        success = _promotion_metrics(
            metrics, root, now=sampled,
            window_seconds=terminal_window_seconds, reader=reader,
        ) and success
    except Exception:  # A scrape reports the failure rather than dropping HTTP.
        success = False
        metrics.family(
            "prismabuild_tier_move_window_complete",
            "Whether the mover-receipt scan proved the window was not truncated "
            "by its file cap and every selected receipt was readable.",
        ).add(0)

    _exporter_metrics(metrics, reader, started)
    metrics.family(
        "prismabuild_collection_success",
        "Whether critical queue inputs and selected terminal records were read and validated for this snapshot.",
    ).add(1 if success else 0)
    return metrics.render()


#: The ceiling the exporter's resident memory is read against (#1020).  The
#: role runs under the supervisor, which bounds no role's memory, so the
#: bound is this gauge pair rather than a unit's ``MemoryMax``.  Derived in
#: docs/pb_metrics.md ("How much memory the exporter may use") from the
#: measured peak on the live-shaped fixture and the growth of the history
#: directories.
RESIDENT_CEILING_BYTES = 680 * 1024 * 1024

#: The per-scrape counters of :meth:`KeptReads.scrape_counts`.
SCRAPE_READ_KINDS = ("listed", "kept", "parsed", "computed", "reused")

#: The families this module adds about its own work, none of them a reading
#: of the queue.  A comparison of two snapshots' queue readings leaves
#: these out: two scrapes of one queue take different times.
EXPORTER_FAMILIES = frozenset((
    "prismabuild_scrape_seconds", "prismabuild_scrape_reads",
    "prismabuild_exporter_resident_bytes",
    "prismabuild_exporter_resident_peak_bytes",
    "prismabuild_exporter_resident_ceiling_bytes",
))


def _resident_bytes() -> tuple[float | None, float | None]:
    """``VmRSS`` and ``VmHWM`` of this process, in bytes; ``None`` if unread."""

    try:
        with open("/proc/self/status", "rb") as stream:
            status = stream.read()
    except OSError:
        return None, None
    found: dict[bytes, float] = {}
    for line in status.splitlines():
        field_name, _, value = line.partition(b":")
        if field_name in (b"VmRSS", b"VmHWM"):
            parts = value.split()
            if len(parts) == 2 and parts[1] == b"kB" and parts[0].isdigit():
                found[field_name] = float(int(parts[0]) * 1024)
    return found.get(b"VmRSS"), found.get(b"VmHWM")


def _exporter_metrics(metrics: Metrics, reader: KeptReads, started: float) -> None:
    """What this snapshot cost, and what the exporter holds (#1020).

    The kept reads are justified by these, not by the fixture: a scrape's
    wall time and its reads, per snapshot, show what keeping saved on the
    live queue, and resident memory against its ceiling shows what keeping
    costs.
    """

    reads = metrics.family(
        "prismabuild_scrape_reads",
        "What this snapshot did: directories listed, directories answered "
        "from a kept listing (kept), records parsed, values computed from "
        "record bytes (computed) and values reused because their file had "
        "not changed (reused).",
    )
    for kind, count in reader.scrape_counts().items():
        reads.add(count, kind=kind)
    resident, peak = _resident_bytes()
    metrics.family(
        "prismabuild_exporter_resident_bytes",
        "The exporter process's resident memory (VmRSS) at this snapshot.",
    ).add(resident)
    metrics.family(
        "prismabuild_exporter_resident_peak_bytes",
        "The exporter process's peak resident memory (VmHWM) since it started.",
    ).add(peak)
    metrics.family(
        "prismabuild_exporter_resident_ceiling_bytes",
        "The resident memory the exporter is sized for; above it, what it "
        "keeps has outgrown the derivation in docs/pb_metrics.md.",
    ).add(RESIDENT_CEILING_BYTES)
    # Last, so it covers everything this snapshot did before rendering.
    metrics.family(
        "prismabuild_scrape_seconds",
        "Wall time this snapshot took to collect, up to rendering it.",
    ).add(time.monotonic() - started)


class MetricsCache:
    def __init__(self, queue_root: Path, cache_seconds: float,
                 terminal_window_seconds: float, terminal_limit: int) -> None:
        self.queue_root = queue_root
        self.cache_seconds = cache_seconds
        self.terminal_window_seconds = terminal_window_seconds
        self.terminal_limit = terminal_limit
        # One store per collector, so the rate is differenced only against
        # this collector's own previous reading.
        self.previous: dict[tuple[str, str], tuple[float, float]] = {}
        # What this server keeps between scrapes (#1020): on the queue's own
        # filesystem an unchanged queue costs one ``lstat`` per directory.
        self.reader = KeptReads()
        self._lock = threading.Lock()
        self._expires = 0.0
        self._text = ""

    def get(self) -> str:
        with self._lock:
            now = time.monotonic()
            if self._text and now < self._expires:
                return self._text
            self._text = collect_metrics(
                self.queue_root,
                terminal_window_seconds=self.terminal_window_seconds,
                terminal_limit=self.terminal_limit,
                previous=self.previous,
                reader=self.reader,
            )
            self._expires = now + self.cache_seconds
            return self._text


def _handler(cache: MetricsCache) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path.split("?", 1)[0] != "/metrics":
                self.send_error(404)
                return
            body = cache.get().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--queue-root", type=Path, default=DEFAULT_QUEUE_ROOT,
                        help="shared pull-queue directory to observe without writing")
    result.add_argument("--listen", default=DEFAULT_LISTEN,
                        help="HTTP bind address (default: loopback only)")
    result.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help="HTTP port serving /metrics")
    result.add_argument("--cache-seconds", type=float, default=DEFAULT_CACHE_SECONDS,
                        help="reuse a snapshot for this many seconds between scrapes")
    result.add_argument("--terminal-window-seconds", type=float,
                        default=DEFAULT_TERMINAL_WINDOW_SECONDS,
                        help="lookback interval for retained terminal outcome gauges")
    result.add_argument("--terminal-limit", type=int, default=DEFAULT_TERMINAL_LIMIT,
                        help="maximum newest terminal records read in one snapshot")
    result.add_argument("--once", action="store_true",
                        help="print one Prometheus snapshot and exit")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser().error("--port must be between 1 and 65535")
    if not math.isfinite(args.cache_seconds) or args.cache_seconds < 0:
        parser().error("--cache-seconds must be finite and nonnegative")
    if (not math.isfinite(args.terminal_window_seconds)
            or args.terminal_window_seconds <= 0):
        parser().error("--terminal-window-seconds must be positive and finite")
    if args.terminal_limit <= 0:
        parser().error("--terminal-limit must be positive")
    if args.once:
        sys.stdout.write(collect_metrics(
            args.queue_root,
            terminal_window_seconds=args.terminal_window_seconds,
            terminal_limit=args.terminal_limit,
        ))
        return 0
    # The server is the fleet's ``metrics`` role (#1020): one per fleet, on
    # the queue's host, spawned by that box's supervisor from the published
    # generation and stopped by it when a publish retires that generation.
    # The role's host-local singleton lock is taken before the port is bound,
    # so a second copy on the box -- a hand-started one, or an installed unit
    # beside the supervised role -- refuses instead of scanning the queue
    # twice.  ``--once`` answers one question and keeps nothing, so it takes
    # no lock.
    import worker_loop as runtime_gate  # noqa: PLC0415 -- the server only

    refused = runtime_gate.ROLE_SINGLETON_HELD_EXIT
    lock = runtime_gate.role_lock_path(Path(__file__))
    try:
        with runtime_gate.role_singleton(Path(__file__)):
            return _serve(args, refused=refused)
    except runtime_gate.RoleLockHeld as held:
        print(f"pbmetrics: refusing a second metrics exporter; {lock} is held by "
              + (f"pid {held.holder}" if held.holder is not None
                 else "an unreadable holder"),
              file=sys.stderr, flush=True)
        _refusal_record("role-lock-held", args, refused,
                        lock=str(lock), holder_pid=held.holder)
        return refused
    except runtime_gate.RoleLockUnavailable as exc:
        print(f"pbmetrics: refusing to serve without the metrics role "
              f"singleton lock: {exc}", file=sys.stderr, flush=True)
        _refusal_record("role-lock-unavailable", args, refused,
                        lock=str(lock), detail=str(exc))
        return refused


#: The schema of the one-line record a refusing exporter writes to its log.
REFUSAL_SCHEMA = "prismabuild.pbmetrics.refusal.v1"


def _refusal_record(reason: str, args: argparse.Namespace, code: int,
                    **detail: object) -> None:
    """One JSON line on stderr -- the role's log -- saying why it will not serve.

    The supervisor sends a role's output to ``pb-role-metrics.log``, so the
    refusal is found where the role's other output is, as one parseable
    line rather than a traceback.
    """

    record = {"schema": REFUSAL_SCHEMA, "role": "metrics", "reason": reason,
              "exit": code, "host": socket.gethostname(), "pid": os.getpid(),
              "unix": round(time.time(), 3), "listen": args.listen,
              "port": args.port, **detail}
    print(json.dumps(record, sort_keys=True), file=sys.stderr, flush=True)


def _port_holder(port: int) -> tuple[int | None, list[str] | None]:
    """The pid and argv listening on TCP ``port``, where cheap to find.

    The listening socket's inode from ``/proc/net/tcp`` and ``tcp6``, then
    the process whose descriptor table holds it.  Only processes this user
    may inspect are found; ``(None, None)`` otherwise, which is the answer
    for a holder owned by another user.
    """

    inodes: set[str] = set()
    for table in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(table) as stream:
                next(stream, None)
                for line in stream:
                    fields = line.split()
                    if len(fields) < 10 or fields[3] != "0A":   # LISTEN
                        continue
                    if int(fields[1].rsplit(":", 1)[1], 16) == port:
                        inodes.add(fields[9])
        except (OSError, ValueError, IndexError):
            continue
    wanted = {f"socket:[{inode}]" for inode in inodes if inode != "0"}
    if not wanted:
        return None, None
    try:
        pids = [name for name in os.listdir("/proc") if name.isdigit()]
    except OSError:
        return None, None
    for pid in pids:
        try:
            descriptors = os.listdir(f"/proc/{pid}/fd")
        except OSError:
            continue
        for descriptor in descriptors:
            try:
                if os.readlink(f"/proc/{pid}/fd/{descriptor}") not in wanted:
                    continue
            except OSError:
                continue
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as stream:
                    argv = [part.decode(errors="replace")
                            for part in stream.read().split(b"\0") if part]
            except OSError:
                argv = None
            return int(pid), argv
    return None, None


def _serve(args: argparse.Namespace, *, refused: int) -> int:
    cache = MetricsCache(
        args.queue_root.absolute(), args.cache_seconds,
        args.terminal_window_seconds, args.terminal_limit,
    )
    try:
        server = ThreadingHTTPServer((args.listen, args.port), _handler(cache))
    except OSError as exc:
        if exc.errno != errno.EADDRINUSE:
            raise
        # Another exporter -- an installed unit beside the supervised role,
        # whose lock this role cannot see -- already serves the port.  A
        # refusal, not a crash: exit as a held lock does, and say who.
        holder, argv = _port_holder(args.port)
        print(f"pbmetrics: refusing to serve; {args.listen}:{args.port} is "
              "already in use by "
              + (f"pid {holder}" if holder is not None else "another process"),
              file=sys.stderr, flush=True)
        _refusal_record("port-in-use", args, refused, holder_pid=holder,
                        holder_argv=argv)
        return refused
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

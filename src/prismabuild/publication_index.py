"""Advisory publication candidates, addressed by exact facets or UTC day.

Canonical queue slots own state, publication identity and filter values. This
index only supplies bounded candidates; readers never repair it or enumerate
the canonical namespaces. Explicit reconciliation supplies coverage evidence.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import math
import os
from pathlib import Path
import stat
import time

from . import digest_primitives, immutable_files, kept_reads
from .record_store import key_shaped, read_bounded_document


FACETS = ("checkout_root", "snapshot_parent", "snapshot_commit", "published_by")
STATES = ("ready", "claimed", "done", "failed", "withdrawn")
SCHEMA = "prismabuild.publication_index.v1"
MAX_LEAF_BYTES = 32768
MAX_COVERAGE_BYTES = 32768
DAY_SECONDS = 86400


@dataclass(frozen=True)
class PublicationCandidate:
    action_key: str
    published_unix: float
    facets: dict[str, str]


@dataclass(frozen=True)
class CandidateCapture:
    candidates: tuple[PublicationCandidate, ...]
    complete: bool
    reasons: tuple[str, ...]
    entries_consumed: int
    pages_consumed: int


def _root(root: Path) -> Path:
    return root / "publication-index" / "v1"


def _hash(value: str) -> str:
    return digest_primitives.raw_sha256(value.encode())


def _publication_candidate(record: Mapping[str, object]) -> PublicationCandidate:
    key, published = record.get("action_key"), record.get("published_unix")
    if (not key_shaped(key) or isinstance(published, bool)
            or not isinstance(published, (int, float))):
        raise ValueError("invalid publication identity")
    try:
        published = float(published)
    except OverflowError as error:
        raise ValueError("unrepresentable publication time") from error
    if not math.isfinite(published) or published < 0:
        raise ValueError("invalid publication identity")
    snapshot = record.get("checkout_snapshot")
    snapshot = snapshot if isinstance(snapshot, Mapping) else {}
    values = {"checkout_root": record.get("checkout_root"),
              "published_by": record.get("published_by"),
              "snapshot_parent": snapshot.get("parent"),
              "snapshot_commit": snapshot.get("commit")}
    return PublicationCandidate(key, published, {
        field: value for field, value in values.items() if isinstance(value, str)})


def _body(candidate: PublicationCandidate) -> dict[str, object]:
    return {"schema": SCHEMA, "action_key": candidate.action_key,
            "published_unix": candidate.published_unix, "facets": candidate.facets}


def _leaf(candidate: PublicationCandidate) -> str:
    return f"{candidate.action_key}.{_hash(candidate.published_unix.hex())}.json"


def _buckets(root: Path, candidate: PublicationCandidate) -> tuple[Path, ...]:
    index = _root(root)
    return (*(index / "facets" / field / _hash(value)
              for field, value in candidate.facets.items()),
            index / "days" / str(math.floor(candidate.published_unix / DAY_SECONDS)))


def _read_leaf(root: Path, path: Path) -> PublicationCandidate:
    record = read_bounded_document(path, max_bytes=MAX_LEAF_BYTES)
    if record is None or record.get("schema") != SCHEMA:
        raise ValueError("missing or invalid publication leaf")
    facets = record.get("facets")
    if (not isinstance(facets, dict) or not set(facets) <= set(FACETS)
            or not all(isinstance(value, str) for value in facets.values())):
        raise ValueError("invalid publication facets")
    candidate = _publication_candidate(record)
    candidate = PublicationCandidate(candidate.action_key, candidate.published_unix, facets)
    if (path.name != _leaf(candidate) or path.parent.name != "entries"
            or path.parent.parent not in _buckets(root, candidate)):
        raise ValueError("publication leaf identity disagrees with its address")
    return candidate


def index_publication(root: Path, record: Mapping[str, object]) -> bool:
    """Best-effort sidecars after canonical publication; replay is idempotent."""
    try:
        candidate = _publication_candidate(record)
        body = _body(candidate)
        for bucket in _buckets(root, candidate):
            path = bucket / "entries" / _leaf(candidate)
            try:
                previous = read_bounded_document(path, max_bytes=MAX_LEAF_BYTES)
            except (OSError, digest_primitives.PrismaBuildError):
                previous = None
            if previous != body:
                immutable_files.write_json_atomic(path, body)
        return True
    except (OSError, ValueError, digest_primitives.PrismaBuildError):
        return False


def _publication_stamp(path: Path) -> list[int] | None:
    try:
        info = path.stat(follow_symlinks=False)
    except FileNotFoundError:
        return None
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError("publication namespace is not a directory")
    return list(kept_reads.metadata_version(info))


def _namespace_stamps(root: Path) -> dict[str, list[int] | None]:
    index = _root(root)
    paths = [root, *(root / state for state in STATES),
             *(index / "facets" / field for field in FACETS), index / "days"]
    return {str(path.relative_to(root)) or ".": _publication_stamp(path) for path in paths}


def _clock_fence() -> int:
    try:
        return time.clock_gettime_ns(getattr(time, "CLOCK_REALTIME_COARSE", 5))
    except (OSError, ValueError, AttributeError):
        return 0


def _trust(root: Path, stamps: Mapping[str, list[int] | None]) -> list[str]:
    reasons = []
    fence = _clock_fence()
    for stamp in stamps.values():
        if stamp is None:
            continue
        if kept_reads._filesystem_type(stamp[0]) not in kept_reads._LOCAL_CLOCK_FILESYSTEMS:
            reasons.append("coverage-untrusted-filesystem")
            break
    if any(stamp is not None and max(stamp[3:5]) >= fence for stamp in stamps.values()):
        reasons.append("coverage-unsettled")
    return reasons


def _publication_index_coverage(root: Path, stamps: Mapping[str, object]) -> list[str]:
    reasons = _trust(root, stamps)
    try:
        coverage = read_bounded_document(_root(root) / "coverage.json",
                                         max_bytes=MAX_COVERAGE_BYTES)
    except (OSError, digest_primitives.PrismaBuildError, ValueError):
        return [*reasons, "coverage-unreadable"]
    if coverage is None:
        return [*reasons, "coverage-missing"]
    if coverage.get("schema") != SCHEMA or not isinstance(coverage.get("namespaces"), dict):
        return [*reasons, "coverage-invalid"]
    if coverage["namespaces"] != stamps:
        reasons.append("coverage-changed")
    return reasons


def publication_candidates(
    root: Path, *, facets: Mapping[str, str], published_after: float | None = None,
    published_before: float | None = None, max_entries: int = 256, max_pages: int = 32,
) -> CandidateCapture:
    """Consume no more than the declared index entries and bucket pages."""
    if not set(facets) <= set(FACETS) or not all(isinstance(v, str) for v in facets.values()):
        raise ValueError("unknown or invalid publication facet")
    if (isinstance(max_entries, bool) or not isinstance(max_entries, int) or max_entries < 1
            or isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages < 1):
        raise ValueError("candidate budgets must be positive integers")
    for bound in (published_after, published_before):
        if bound is not None and (isinstance(bound, bool)
                                  or not isinstance(bound, (int, float))
                                  or not math.isfinite(bound)):
            raise ValueError("publication time bound must be finite")
    index = _root(root)
    reasons: list[str] = []
    entries = pages = 0
    candidates: dict[tuple[str, float], PublicationCandidate] = {}
    observed: dict[Path, list[int] | None] = {}
    try:
        before = _namespace_stamps(root)
        reasons.extend(_publication_index_coverage(root, before))
        if facets:
            field = next(field for field in FACETS if field in facets)
            buckets = iter((index / "facets" / field / _hash(facets[field]),))
        else:
            if published_after is None:
                return CandidateCapture((), False, ("time-range-required",), 0, 0)
            upper = time.time() if published_before is None else published_before
            if published_before is None:
                reasons.append("future-range-incomplete")
            first = max(0, math.floor(published_after / DAY_SECONDS))
            last = math.floor(upper / DAY_SECONDS)
            buckets = (index / "days" / str(day) for day in range(first, last + 1))
        for bucket in buckets:
            if pages >= max_pages:
                reasons.append("page-budget-exhausted")
                break
            pages += 1
            directory = bucket / "entries"
            observed[bucket] = _publication_stamp(bucket)
            observed[directory] = _publication_stamp(directory)
            if observed[bucket] is not None:
                reasons.extend(_trust(root, {str(bucket): observed[bucket]}))
                reasons.extend(_trust(root, {str(directory): observed[directory]}))
                try:
                    marker = read_bounded_document(bucket / "coverage.json", max_bytes=MAX_COVERAGE_BYTES)
                except (OSError, ValueError, digest_primitives.PrismaBuildError):
                    marker = None
                    reasons.append("bucket-coverage-unreadable")
                if (marker is None or marker.get("schema") != SCHEMA
                        or observed[directory] is None
                        or marker.get("entries") != observed[directory]
                        or marker.get("namespaces") != before):
                    reasons.append("bucket-coverage-missing-or-changed")
            try:
                listing = os.scandir(directory)
            except FileNotFoundError:
                continue
            with listing:
                while True:
                    if entries >= max_entries:
                        reasons.append("entry-budget-exhausted")
                        break
                    try:
                        entry = next(listing)
                    except StopIteration:
                        break
                    entries += 1
                    try:
                        candidate = _read_leaf(root, Path(entry.path))
                    except (OSError, ValueError, digest_primitives.PrismaBuildError):
                        reasons.append("index-leaf-unreadable")
                        continue
                    if (all(candidate.facets.get(field) == value for field, value in facets.items())
                            and (published_after is None or candidate.published_unix >= published_after)
                            and (published_before is None or candidate.published_unix <= published_before)):
                        candidates[candidate.action_key, candidate.published_unix] = candidate
            if entries >= max_entries:
                break
        if _namespace_stamps(root) != before or any(_publication_stamp(path) != stamp for path, stamp in observed.items()):
            reasons.append("coverage-changed")
    except (OSError, ValueError, digest_primitives.PrismaBuildError):
        reasons.append("index-unreadable")
    reasons = list(dict.fromkeys(reasons))
    return CandidateCapture(tuple(candidates.values()), not reasons, tuple(reasons), entries, pages)


class Reconciliation:
    """An explicit bounded, restartable backfill cursor, never a reader side effect."""

    def __init__(self, store):
        self.store = store
        self.root = store.root
        index = _root(self.root)
        for directory in (*(index / "facets" / field for field in FACETS), index / "days"):
            directory.mkdir(parents=True, exist_ok=True)
        self.before = _namespace_stamps(self.root)
        self.states = iter(STATES)
        self.listing = None
        self.touched: set[Path] = set()
        self.markers = None
        self.finished = self.complete = False
        self.reasons = _trust(self.root, self.before)

    def close(self) -> None:
        if self.listing is not None:
            self.listing.close()
            self.listing = None
        if not self.finished:
            self.reasons.append("reconciliation-cancelled")
            self.finished = True

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()

    def _unchanged(self) -> bool:
        after = _namespace_stamps(self.root)
        return all(after[state] == self.before[state] for state in (".", *STATES))

    def step(self, *, max_entries: int = 256) -> dict[str, object]:
        if isinstance(max_entries, bool) or not isinstance(max_entries, int) or max_entries < 1:
            raise ValueError("reconciliation budget must be a positive integer")
        consumed = 0
        try:
            while not self.finished and consumed < max_entries:
                if self.markers is not None:
                    try:
                        bucket = next(self.markers)
                    except StopIteration:
                        if self._unchanged() and not self.reasons:
                            namespaces = _namespace_stamps(self.root)
                            immutable_files.write_json_atomic(_root(self.root) / "coverage.json", {
                                "schema": SCHEMA, "namespaces": namespaces})
                            if not self._unchanged() or _namespace_stamps(self.root) != namespaces:
                                self.reasons.append("coverage-changed")
                            self.reasons.extend(_trust(self.root, namespaces))
                            self.complete = not self.reasons
                        else:
                            self.reasons.append("coverage-changed")
                        self.finished = True
                        continue
                    consumed += 1
                    if not self._unchanged():
                        self.reasons.append("coverage-changed")
                        self.finished = True
                        continue
                    immutable_files.write_json_atomic(bucket / "coverage.json", {
                        "schema": SCHEMA, "entries": _publication_stamp(bucket / "entries"),
                        "namespaces": _namespace_stamps(self.root)})
                    continue
                if self.listing is None:
                    try:
                        state = next(self.states)
                    except StopIteration:
                        self.markers = iter(self.touched)
                        continue
                    try:
                        self.listing = os.scandir(self.root / state)
                    except FileNotFoundError:
                        continue
                try:
                    entry = next(self.listing)
                except StopIteration:
                    self.listing.close()
                    self.listing = None
                    continue
                consumed += 1
                if not entry.name.endswith(".json") or not key_shaped(entry.name[:-5]):
                    continue
                record = read_bounded_document(Path(entry.path), max_bytes=1024 * 1024)
                if record is None or record.get("action_key") != entry.name[:-5]:
                    self.reasons.append("canonical-record-unreadable")
                    continue
                candidate = _publication_candidate(record)
                if not self.store.index_publication(record):
                    self.reasons.append("index-write-failed")
                else:
                    self.touched.update(_buckets(self.root, candidate))
        except (OSError, ValueError, digest_primitives.PrismaBuildError):
            self.reasons.append("reconciliation-unavailable")
            self.close()
        return {"finished": self.finished, "complete": self.complete,
                "entries_consumed": consumed, "reasons": list(dict.fromkeys(self.reasons))}

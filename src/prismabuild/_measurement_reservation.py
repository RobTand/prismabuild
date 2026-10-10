"""Conservative measurement selection, not resource-release prediction (#1419).

The existing passes sidecar carries one host election per publication. A complete
read is refreshed under measurement transition keys and host admission; directory
absence alone never retires an election. Payload deadlines are opportunity
metadata only. No current candidate has a proved admission-to-release bound.
"""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager, ExitStack
import fcntl
import math
import os
from pathlib import Path
import stat
import time
from typing import TYPE_CHECKING

from . import adaptive_cpu, core, local_scratch, storage_tiers
from . import _bounded_reader as reader

if TYPE_CHECKING:
    from .pool import PoolQueue, ResourceLedger

FIELD = "measurement_reservation"
SCHEMA = "prismabuild.measurement_reservation.v1"
READ_SECTION = "measurement-publications-v1"
READ_BUDGET_S = 5.0
REFRESH_SECTION = "measurement-election-refresh-v1"
#: The reuse refresh reads far fewer, much smaller records than a full
#: census, so it runs on a smaller share of the same bounded-reader
#: contract: an abandonable owned child with durable reader ownership,
#: and a refusal -- never a stall -- past the budget (#1571 review).
REFRESH_BUDGET_S = 2.0
MAX_RECORDS = 4096
MAX_RECORD_BYTES = 4 * 1024 * 1024
MAX_FENCE_BYTES = 4096
#: How long a census waits for a sibling loop's fence before it refuses as
#: "reader busy" (#1498). Taken before any M key or H, so a waiter holds
#: nothing a holder needs; a holder never waits on a waiter.
FENCE_WAIT_S = 2.0
FENCE_POLL_S = 0.02
#: How many times one census rescans when a listed publication vanishes under it
#: (#1571); the census child's own deadline still bounds the total.
CAPTURE_RESCANS = 3


class CensusUnavailable(RuntimeError):
    """An incomplete census cannot authorize lower-priority acquisition."""


class PublicationDisappeared(CensusUnavailable):
    """A listed publication was gone when the census read it (#1571)."""


def _read(path: Path, *, optional: bool = False, limit: int = MAX_RECORD_BYTES) -> dict | None:
    try:
        raw = core._read_regular_file_nofollow(
            path, where=READ_SECTION, max_bytes=limit, replaced_leaf=True)
    except FileNotFoundError:
        if optional:
            return None
        raise PublicationDisappeared(f"publication disappeared during census: {path}") from None
    value = core._decode_strict_json(raw, where=READ_SECTION)
    if not isinstance(value, dict):
        raise CensusUnavailable(f"non-object census record: {path}")
    return value


def selection(record: dict) -> dict | None:
    value = record.get(FIELD)
    if FIELD not in record:
        return None
    if (not isinstance(value, dict) or set(value) != {
            "schema", "action_key", "generation", "host", "published_unix",
            "priority", "epoch_unix", "opportunity_unix"}
            or value.get("schema") != SCHEMA):
        raise CensusUnavailable("malformed canonical measurement selection")
    for field in ("action_key", "generation"):
        text = value[field]
        if not isinstance(text, str) or len(text) != 64 or any(c not in "0123456789abcdef" for c in text):
            raise CensusUnavailable(f"invalid selection {field}")
    host = value["host"]
    if not isinstance(host, str) or not host or host != host.strip():
        raise CensusUnavailable("invalid selection host")
    if type(value["priority"]) is not int:
        raise CensusUnavailable("invalid selection priority")
    for field in ("published_unix", "epoch_unix", "opportunity_unix"):
        number = value[field]
        if type(number) not in (int, float) or not math.isfinite(number):
            raise CensusUnavailable(f"invalid selection {field}")
    if value["opportunity_unix"] < value["epoch_unix"]:
        raise CensusUnavailable("selection opportunity precedes election")
    if core.canonical_sha256({"action_key": value["action_key"],
                              "published_unix": float(value["published_unix"])}) != value["generation"]:
        raise CensusUnavailable("selection publication identity mismatch")
    return value


def _capture(queue: PoolQueue) -> dict:
    """One strict census, rescanned when a listed publication vanishes (#1571).

    A publication is claimed, finished or withdrawn at any moment, so the
    directory listing can name a file that is gone by the time it is read. That
    is a race with the queue, not an unreadable census: refusing it denied the
    whole pass ('publication disappeared during census') on a queue whose
    rows were moving. Rescan a bounded number of times; a queue that keeps
    changing under every scan, or any other unreadable record, still refuses.
    """
    for attempt in range(CAPTURE_RESCANS):
        try:
            return _scan_publications(queue)
        except PublicationDisappeared:
            if attempt + 1 == CAPTURE_RESCANS:
                raise
    raise AssertionError("unreachable")  # pragma: no cover


def _scan_publications(queue: PoolQueue) -> dict:
    """Strict full-publication discovery; called only by the read-only child.

    Include finish marks and elected sidecars even when neither live directory
    names M. The subsequent locked refresh, not this sequential discovery,
    closes CLAIMED -> tombstone -> READY. New M keys force another pass.
    """
    from . import pool
    rows: dict[str, list[dict]] = {}
    unorderable: dict[str, list[dict]] = {}   # live members, never candidates
    selected: dict[str, dict] = {}
    opportunities: dict[str, dict] = {}
    count = 0
    #: Publication generations the census saw READY or CLAIMED (#1721). The
    #: gang fence reads them to tell a waiting member (reserves) from a
    #: running one (already holds its demand in the ledger) or an ended one
    #: (holds nothing).
    ready_versions: set[tuple[str, float]] = set()
    claimed_versions: set[tuple[str, float]] = set()
    for state in (pool.READY, pool.CLAIMED, "passes"):
        directory = queue.root / state
        try:
            entries = os.scandir(directory)
        except FileNotFoundError:
            if state == pool.PASSES:
                continue  # the existing sidecar directory is created lazily
            raise
        with entries:
            for entry in entries:
                count += 1
                if count > MAX_RECORDS:
                    raise CensusUnavailable("measurement census record cap exceeded")
                name = entry.name
                is_mark = state == pool.CLAIMED and name.endswith((pool.TOMBSTONE_SUFFIX, pool.LATE_FINISH_SUFFIX))
                if not name.endswith(".json") and not is_mark:
                    continue  # leases/status/progress are not publications
                key = name[:64]
                if not pool._is_hex64(key):
                    raise CensusUnavailable(f"unaddressable publication: {entry.path}")
                record = _read(Path(entry.path))
                if record is None:
                    raise CensusUnavailable("missing census record")
                if record.get("action_key") != key:
                    raise CensusUnavailable(f"census action identity mismatch: {entry.path}")
                if state == "passes":
                    chosen = selection(record)
                    if chosen is not None:
                        if chosen["action_key"] != key:
                            raise CensusUnavailable("selection sidecar key mismatch")
                        selected[key] = chosen
                else:
                    queue.attempt_generation(record)  # strict publication identity
                    if type(record.get("priority", 0)) is not int:
                        unorderable_field = pool.PoolQueue._unorderable_queue_field(record)
                        if (state == pool.READY and unorderable_field is not None
                                and unorderable_field[0] == "priority"):
                            # A READY record the queue itself cannot order
                            # holds no tokens and runs nothing, and the queue
                            # files it by name instead of raising
                            # (``ready_items``).  Refusing it denied every
                            # good claim on the host (#1506).  It is never
                            # claimed, so it cannot reach the CLAIMED census.
                            # It is still a live gang member: only candidate
                            # classification skips it.  A priority the queue
                            # CAN order (5.5, True, "5") stays strict, even
                            # when another ordering field is unreadable: the
                            # helper names the FIRST bad field, and the queue
                            # reads a bad ``passes`` as 0 before it orders, so
                            # it still lists and can claim such a record.  A
                            # CLAIMED one stays strict too: a running
                            # incumbent the census cannot read is unknown.
                            unorderable.setdefault(key, []).append(record)
                            try:
                                ready_versions.add((key, float(record.get("published_unix", math.nan))))
                            except (TypeError, ValueError):
                                pass
                            continue
                        raise CensusUnavailable("unreadable publication priority")
                    rows.setdefault(key, []).append(record)
                    try:
                        version = (key, float(record.get("published_unix", math.nan)))
                    except (TypeError, ValueError):
                        version = None
                    if version is not None:
                        if state == pool.CLAIMED and not is_mark:
                            claimed_versions.add(version)
                        elif state == pool.READY:
                            ready_versions.add(version)
                    if state == pool.CLAIMED and not is_mark:
                        # Every claimed action is an incumbent; only a sealed
                        # deadline contributes a finite opportunity (#1419).
                        governed, requested = pool._declared_run_bound(record, max_bytes=MAX_RECORD_BYTES)
                        opportunities[key] = {
                            "host": record.get("claimed_host"),
                            "requested": requested if governed == "deadline" else None,
                            "claimed_unix": record.get("claimed_unix"),
                            "generation": queue.attempt_generation(record)}
    measurements: dict[str, list[dict]] = {}
    for key, versions in rows.items():
        for record in versions:
            # An absent request is the supported legacy direct-publication
            # path, not a sealed measurement. Corrupt/unreadable is NOT absent.
            action = pool._sealed_action_request(str(record.get("cas_root")), key,
                                                max_bytes=MAX_RECORD_BYTES)
            task = action.get("task") if action is not None else None
            if isinstance(task, Mapping) and task.get("task_class") == "measurement":
                measurements.setdefault(key, []).append(record)
    elections: dict[str, dict] = {}
    for key, chosen in selected.items():
        versions = rows.get(key, [])
        same = [row for row in versions if queue.attempt_generation(row) == chosen["generation"]]
        if same:
            if key not in measurements:
                raise CensusUnavailable("elected measurement sealed identity unavailable")
            elections[key] = chosen
            continue
        # Absence is not retirement. Exact ending or a strictly newer live
        # publication under this key may retire it, but marks/claims of the
        # old generation above take precedence over any success/cancellation.
        endings = []
        for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
            row = _read(queue.item_path(state, key), optional=True)
            if row is None:
                continue
            stamp = row.get("withdrawn_unix" if state == pool.WITHDRAWN else "finished_unix")
            if (row.get("schema") != pool.POOL_OUTCOME_SCHEMA_V1 or row.get("action_key") != key
                    or not isinstance(row.get("status"), str) or not row["status"]
                    or (state == pool.WITHDRAWN and row["status"] != "withdrawn")
                    or not isinstance(stamp, (int, float)) or isinstance(stamp, bool)
                    or not math.isfinite(stamp)):
                raise CensusUnavailable("incomplete measurement retirement proof")
            endings.append(row)
        if any(queue.attempt_generation(row) == chosen["generation"] for row in endings):
            continue
        if versions and all(float(row["published_unix"]) > chosen["published_unix"] for row in versions):
            continue
        elections[key] = chosen  # missing authority stays fenced, indefinitely
    return {"measurements": measurements, "elections": elections, "selections": selected,
            "opportunities": opportunities, "keys": sorted(set(measurements) | set(selected)),
            "gang_elections": _gang_elections(
                queue, {key: rows.get(key, []) + unorderable.get(key, [])
                        for key in rows.keys() | unorderable.keys()}, count,
                ready_versions=ready_versions, claimed_versions=claimed_versions)}


def _member_demand(rows: dict[str, list[dict]], record: dict, key: str) -> dict | None:
    """The elected member's own declared demand, or ``None`` when it does not read (#1721).

    Read off the member's census row (``resources``, already read strictly), at
    the publication generation the gang record names. ``None`` is not "no
    demand": the reservation then covers the whole host (fail safe).
    """
    for member in record["members"]:
        if member["action_key"] != key:
            continue
        for row in rows.get(key, []):
            if float(row.get("published_unix", math.nan)) != member["published_unix"]:
                continue
            resources = row.get("resources")
            if (isinstance(resources, dict)
                    and all(isinstance(kind, str) and type(count) is int and count >= 0
                            for kind, count in resources.items())):
                return dict(resources)
    return None


def _elected_member_state(record: dict, key: str, *, ready_versions: set,
                          claimed_versions: set) -> str:
    """Whether the elected member is pending, running, or terminal (#1721)."""
    published = None
    for member in record.get("members", []):
        if isinstance(member, dict) and member.get("action_key") == key:
            try:
                published = float(member.get("published_unix", math.nan))
            except (TypeError, ValueError):
                published = None
            break
    if published is None or not math.isfinite(published):
        return "terminal"
    if (key, published) in claimed_versions:
        return "claimed"
    if (key, published) in ready_versions:
        return "ready"
    return "terminal"


def _gang_elections(queue: PoolQueue, rows: dict[str, list[dict]], count: int, *,
                    ready_versions: set | None = None,
                    claimed_versions: set | None = None) -> dict:
    """Live gang host elections (#1517), read in the same bounded child.

    A gang election fences its host while any member row of a gang without a
    teardown is still READY or CLAIMED. An unreadable or malformed gang
    record fails the census closed, exactly as a malformed publication does.
    Elections are written only by the elected host's own pass under its host
    admission, which also serializes this host's readers, so no member key
    joins the M lock set.

    ``member_state`` distinguishes READY, CLAIMED, and terminal publications.
    Only READY members reserve. CLAIMED members already hold ledger tokens.
    The lower-priority fence lasts until the gang ends.
    """
    from . import _gang
    directory = _gang.root(queue)
    try:
        entries = os.scandir(directory)
    except FileNotFoundError:
        return {}  # gang admission was never used on this pool
    found: dict[str, dict] = {}
    ready = ready_versions if ready_versions is not None else set()
    claimed = claimed_versions if claimed_versions is not None else set()
    try:
        with entries:
            for entry in entries:
                count += 1
                if count > MAX_RECORDS:
                    raise CensusUnavailable("measurement census record cap exceeded")
                if not entry.name.endswith(".json") or entry.name.startswith("."):
                    continue
                group = entry.name[:-5]
                record = _gang.read_group(queue, group)
                if record is None or _gang.teardown(queue, group) is not None:
                    continue
                live = [member for member in record["members"]
                        if any(float(row.get("published_unix", math.nan)) == member["published_unix"]
                               for row in rows.get(member["action_key"], []))]
                if not live:
                    continue
                rank = list(_gang.rank(record))
                for index, election in _gang.elections(queue, group, record["size"]).items():
                    found[election["action_key"]] = {
                        "group": group, "index": index, "action_key": election["action_key"],
                        "host": election["host"], "priority": election["priority"], "rank": rank,
                        "demand": _member_demand(rows, record, election["action_key"]),
                        "member_state": _elected_member_state(
                            record, election["action_key"], ready_versions=ready,
                            claimed_versions=claimed)}
    except _gang.GangContractError as exc:
        raise CensusUnavailable(str(exc)) from exc
    return found

def _scan_election_refresh(queue: PoolQueue) -> dict:
    """Changed elections only; called only by the read-only refresh child.

    Read pass sidecars and exact live gang members, not the full queue.
    Use the complete census record cap. A refresh refusal ends admission
    for this pass; it must not trigger complete scans for later candidates.
    A vanished sidecar is not retirement authority.
    """
    from . import _gang
    from . import pool as pool_mod
    count = 0
    try:
        gang_names = sorted(
            entry.name for entry in os.scandir(_gang.root(queue)))
    except FileNotFoundError:
        gang_names = []
    count += len(gang_names)
    if count > MAX_RECORDS:
        raise CensusUnavailable("measurement refresh record cap exceeded")
    groups: dict[str, dict] = {}
    for name in gang_names:
        if not name.endswith(".json") or name.startswith("."):
            continue
        group = name[:-5]
        record = _gang.read_group(queue, group)
        if record is None or _gang.teardown(queue, group) is not None:
            continue
        groups[group] = record
    try:
        pass_names = sorted(
            entry.name for entry in os.scandir(queue.root / pool_mod.PASSES))
    except FileNotFoundError:
        pass_names = []
    count += len(pass_names)
    if count > MAX_RECORDS:
        raise CensusUnavailable("measurement refresh record cap exceeded")
    selections: dict[str, dict] = {}
    for name in pass_names:
        if not name.endswith(".json") or not pool_mod._is_hex64(name[:-5]):
            continue
        try:
            record = _read(queue.root / pool_mod.PASSES / name)
        except PublicationDisappeared:
            continue
        if record.get("action_key") != name[:-5]:
            raise CensusUnavailable(f"refresh sidecar identity mismatch: {name}")
        chosen = selection(record)
        if chosen is None:
            continue
        if chosen["action_key"] != name[:-5]:
            raise CensusUnavailable("refresh selection sidecar key mismatch")
        selections[chosen["action_key"]] = chosen
    members: dict[str, list] = {}
    ready_versions: set[tuple[str, float]] = set()
    claimed_versions: set[tuple[str, float]] = set()
    for group, record in groups.items():
        for member in record["members"]:
            key = member["action_key"]
            if key in members:
                continue
            rows: list = []
            for state in (pool_mod.READY, pool_mod.CLAIMED):
                count += 1
                if count > MAX_RECORDS:
                    raise CensusUnavailable("measurement refresh record cap exceeded")
                row = _read(queue.item_path(state, key), optional=True)
                if row is None:
                    continue
                queue.attempt_generation(row)
                version = (key, float(row["published_unix"]))
                (ready_versions if state == pool_mod.READY else claimed_versions).add(version)
                rows.append({"published_unix": version[1], "resources": row.get("resources")})
            members[key] = rows
    gangs: dict[str, dict] = {}
    for group, record in groups.items():
        try:
            rank = list(_gang.rank(record))
            found = _gang.elections(queue, group, record["size"])
        except _gang.GangContractError as exc:
            raise CensusUnavailable(str(exc)) from exc
        count += len(found)
        if count > MAX_RECORDS:
            raise CensusUnavailable("measurement refresh record cap exceeded")
        live = [member for member in record["members"]
                if any(float(row["published_unix"]) == member["published_unix"]
                       for row in members.get(member["action_key"], []))]
        if not live:
            continue
        for index, election in found.items():
            gangs[election["action_key"]] = {
                "group": group, "index": index, "action_key": election["action_key"],
                "host": election["host"], "priority": election["priority"], "rank": rank,
                "demand": _member_demand(members, record, election["action_key"]),
                "member_state": _elected_member_state(
                    record, election["action_key"], ready_versions=ready_versions,
                    claimed_versions=claimed_versions)}
    return {"selections": selections, "gang_elections": gangs}


class CensusReader:
    """One protected host-local restart fence, shared by every queue observer.

    This storage is the existing supported box-state root, which MUST be local
    and preserved while readers survive. Small regular-file operations are
    cooperative; no filesystem syscall is claimed to have a hard deadline.
    Clearing/repointing that root requires stopped readers, just as clearing
    admission state requires stopped claimants. No arbitrary callback or shared
    persistence is used by the owned-reader grant.
    """

    def __init__(self, queue: PoolQueue, ledger: ResourceLedger):
        self.queue = queue
        directory, digest = adaptive_cpu.box_state(ledger.base)
        self.directory = directory
        self.name = digest + ".measurement-reader-v1"
        self.pool_identity = str(queue.root.resolve())
        self._held: tuple[int, int] | None = None

    @contextmanager
    def held(self):
        """Own the restart fence across a whole two-phase census (#1498).

        ``locked_census`` reads outside H for discovery and again under H for
        its refresh. Releasing the fence between the phases let a concurrent
        queue observer take it in the gap and deny the refresh with
        "measurement census reader busy" after the caller had already taken
        the measurement transition keys and host admission: the phase-two
        steal behind the sustained admission denials of #1498. One fence
        acquisition per ``locked_census`` keeps the single-reader contract
        exactly -- the ownership marker is persisted per acquisition, the
        retained-reader liveness check runs per acquisition, and at most one
        bounded census child runs at a time -- while the two reads of one
        census share that acquisition. Everything acquired inside the hold
        stays nonblocking, so a refused fence, transition key or admission
        gate still denies the pass and releases the fence.
        """
        directory = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        lock = None
        try:
            # Only acquiring the fence is a census failure. The caller's body
            # runs outside this translation: a body exception (a GPU sample
            # write in ``reserve_probe``, a claim error) keeps its own type
            # instead of becoming CensusUnavailable (#1506).
            try:
                info = os.fstat(directory)
                if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                        or info.st_mode & 0o077):
                    raise CensusUnavailable("unsafe local census directory")
                # Reuse the exact open-descriptor local mount observer under the
                # host-local state policy: a path named BOX_STATE_ROOT is not
                # itself evidence of local storage, and this small-file rendezvous
                # supports tmpfs while shared storage still refuses (#1451).
                local_scratch._descriptor_state_identity(directory)
                lock = os.open(self.name + ".guard", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC,
                               0o600, dir_fd=directory)
                info = os.fstat(lock)
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                        or info.st_nlink != 1 or info.st_mode & 0o077):
                    raise CensusUnavailable("unsafe local census fence lock")
                # Bounded wait, not an instant refusal (#1498): every loop on a
                # box censuses each candidate, so a nonblocking fence turned one
                # sibling's census into this candidate's denial. The waiter holds
                # only its own candidate key, never M or H, and still refuses once
                # FENCE_WAIT_S passes; nothing is read without the fence.
                waited = reader.Deadline(FENCE_WAIT_S)
                while True:
                    try:
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        remaining = waited.remaining()
                        if remaining is None or remaining <= 0:
                            raise CensusUnavailable("measurement census reader busy") from None
                        time.sleep(min(FENCE_POLL_S, remaining))
                self._check_settled_reader()
            except (OSError, ValueError, core.PrismaBuildError, reader.ReaderOwnershipUnavailable,
                    local_scratch.LocalScratchError) as exc:
                raise CensusUnavailable(str(exc)) from exc
            self._held = (directory, lock)
            try:
                yield
            finally:
                self._held = None
        finally:
            if lock is not None:
                os.close(lock)
            os.close(directory)

    def _check_settled_reader(self) -> None:
        """Refuse live or unknown ownership while the caller holds the guard."""
        previous = _read(self.directory / self.name, optional=True, limit=MAX_FENCE_BYTES)
        if previous is None:
            return
        ownership = reader.ReaderOwnership.from_record(previous)
        if (ownership.section not in (READ_SECTION, REFRESH_SECTION)
                or reader.reader_liveness(
                    ownership, pool_identity=self.pool_identity,
                    section=ownership.section) != "settled"):
            raise CensusUnavailable("retained measurement census reader unresolved")

    def capture(self) -> dict:
        if self._held is not None:
            return self._census(self._held[0])
        with self.held():
            return self._census(self._held[0])

    def _spawn_owned(self, directory: int, section: str, read, budget_s: float) -> dict:
        """Start an owned child only under the guard, after exact settlement."""
        if self._held is None or directory != self._held[0]:
            raise CensusUnavailable("measurement reader requires its guard")
        try:
            self._check_settled_reader()
            abandoned: list[dict] = []
            def persist(ownership: reader.ReaderOwnership) -> None:
                payload = core._canonical_bytes(ownership.to_record())
                if len(payload) > MAX_FENCE_BYTES:
                    raise CensusUnavailable("census ownership exceeds local cap")
                temporary = self.name + ".writing"
                descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                     0o600, dir_fd=directory)
                try:
                    reader._write_reader_payload(descriptor, payload)
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                os.replace(temporary, self.name, src_dir_fd=directory, dst_dir_fd=directory)
                os.fsync(directory)
            reply = reader.bounded(section, read,
                                   deadline=reader.Deadline(budget_s), abandoned=abandoned,
                                   on_spawn=persist, pool_identity=self.pool_identity,
                                   announce_retained=False)
            if reply.get("status") != "ok" or abandoned:
                raise CensusUnavailable(f"measurement census unavailable: {reply}")
            value = reply.get("value")
            if not isinstance(value, dict):
                raise CensusUnavailable("incomplete measurement census reply")
            return value
        except (OSError, ValueError, core.PrismaBuildError, reader.ReaderOwnershipUnavailable,
                local_scratch.LocalScratchError) as exc:
            raise CensusUnavailable(str(exc)) from exc

    def refresh_elections(self) -> dict:
        """Refresh elections under the reader guard, M, and host admission.

        The caller takes the guard before M and H. Exact reader settlement
        precedes each child. A failed or retained read refuses the pass.
        """
        if self._held is None:
            raise CensusUnavailable("measurement refresh requires reader guard")
        value = self._spawn_owned(
            self._held[0],
            REFRESH_SECTION, lambda: _scan_election_refresh(self.queue),
            REFRESH_BUDGET_S)
        if (set(value) != {"selections", "gang_elections"}
                or not all(isinstance(value[field], dict) for field in value)
                or len(value["selections"]) + len(value["gang_elections"]) > MAX_RECORDS):
            raise CensusUnavailable("incomplete measurement refresh reply")
        for key, chosen in value["selections"].items():
            checked = selection({FIELD: chosen})
            if checked is None or checked["action_key"] != key:
                raise CensusUnavailable("refresh election identity mismatch")
        return value

    def _census(self, directory: int) -> dict:
        """One strict bounded census read; the caller owns the held fence."""
        value = self._spawn_owned(
            directory, READ_SECTION, lambda: _capture(self.queue), READ_BUDGET_S)
        if (set(value) != {"measurements", "elections", "selections", "opportunities", "keys",
                                  "gang_elections"}
                or not all(isinstance(value[field], dict)
                           for field in ("measurements", "elections", "selections", "opportunities",
                                         "gang_elections"))
                or not isinstance(value["keys"], list)
                or len(value["keys"]) > MAX_RECORDS
                or any(not isinstance(key, str) or len(key) != 64
                       or any(c not in "0123456789abcdef" for c in key) for key in value["keys"])
                or not (set(value["measurements"]) | set(value["elections"])).issubset(value["keys"])):
            raise CensusUnavailable("incomplete measurement census reply")
        for key, chosen in value["elections"].items():
            checked = selection({FIELD: chosen})
            if checked is None or checked["action_key"] != key:
                raise CensusUnavailable("census election identity mismatch")
        return value


class PassCensus:
    """One successful census a claim pass reuses for later candidates (#1571).

    The census reads the whole queue, not the candidate, so a pass with a
    free fence paid two bounded-child scans per candidate: the discovery
    read outside host admission and the refresh read under it. Both run
    inside one held reader fence, and the refresh runs while the pass
    holds host admission and the elected measurement keys of this host.
    Reuse keeps the first successful census for the rest of the pass:
    later candidates take the reader guard before M and H, then refresh
    only election sources through an owned child. No reuse performs a
    complete publication scan.

    Each refresh detects new and replacement elections under host admission.
    Stored measurement elections remain conservative through claims and
    retirement. Only a complete census proves their retirement. A refusal
    ends this pass; the next pass starts with a complete census.
    """

    def __init__(self, queue: PoolQueue, ledger: ResourceLedger, controller):
        self._queue = queue
        self._ledger = ledger
        self._controller = controller
        self._census: dict | None = None
        self._selections: dict | None = None
        self._gang_elections: dict | None = None
        self._busy: set[str] = set()

    @property
    def census(self) -> dict | None:
        """The census the pass reuses, or ``None`` before the first read."""
        return self._census

    def invalidate(self) -> None:
        """Drop the reused census after authority may have changed (#1571)."""
        self._census = None
        self._selections = None
        self._gang_elections = None
        self._busy = set()

    def store(self, census: dict) -> None:
        """Keep one successful locked census for the rest of the pass."""
        self._census = census
        self._selections = dict(census.get("selections") or {})
        self._gang_elections = dict(census.get("gang_elections") or {})

    def _refresh_dynamic_elections(self, census_reader: CensusReader) -> None:
        """Refresh live elections through a bounded owned child (#1571).

        Run only while the elected measurement keys of this host and
        host admission are held. A sibling loop's gang or measurement
        election lands between this pass's candidates, while these
        locks are released. The stored elections would miss it, and
        the blocking checks would admit work a live election fences
        -- the unsafe direction. So every reuse re-reads the small
        election sources through the same owned-reader contract as a
        census: an abandonable child, durable reader ownership, a
        small budget, and the census record cap. A timeout or retained child
        refuses this pass. Election writers hold host admission (#1517).
        A completed refresh reads a stable election state for this host.

        Merge errs toward fencing. A changed or new measurement
        selection joins the stored selections and elections at once:
        the full comparison (generation, host, priority, stamps) sees
        a replacement election under a retired action key, not just a
        missing one. A stored election that ended between candidates
        stays fenced until the next pass reads fresh: only a full
        census proves retirement, never this small read. A vanished
        sidecar is the queue race :func:`_capture` already names, not
        authority, and the child skips it. Gang elections carry their
        live-member proof from the same child read: a group whose
        members all left READY/CLAIMED -- a completed gang that files
        no teardown -- contributes no fence, exactly as in
        :func:`_gang_elections`. An unreadable election refuses the pass.
        """
        assert self._census is not None
        refreshed = census_reader.refresh_elections()
        selections = dict(self._selections or {})
        elections = dict((self._census.get("elections") or {}))
        for key, chosen in refreshed["selections"].items():
            if selections.get(key) != chosen:
                selections[key] = chosen
                elections[key] = chosen
        current = dict(self._census)
        current["gang_elections"] = dict(refreshed["gang_elections"])
        current["selections"] = selections
        current["elections"] = elections
        self._census = current
        self._gang_elections = dict(refreshed["gang_elections"])
        self._selections = selections

    def acquire(self, census_reader: CensusReader | None = None) -> ExitStack | None:
        """Take the reader guard, sorted M keys, then host admission.

        A busy elected key stays fenced. A busy reader refuses before M
        or H. Any setup failure closes the complete lock stack.
        """
        if self._census is None:
            return None
        owned_reader = census_reader if census_reader is not None else CensusReader(
            self._queue, self._ledger)
        held = ExitStack()
        try:
            held.enter_context(owned_reader.held())
            busy: set[str] = set()
            here = self._ledger.base.name
            for key in sorted(self._selections or {}):
                chosen = (self._selections or {}).get(key)
                if not isinstance(chosen, dict) or chosen.get("host") != here:
                    continue
                if not held.enter_context(
                        self._queue._transition_locked(key, blocking=False)):
                    busy.add(key)
            held.enter_context(
                self._queue._admission_lock(self._controller))
            self._refresh_dynamic_elections(owned_reader)
        except BaseException:
            held.close()
            raise
        self._busy = busy
        return held

    def reused(self) -> dict:
        """The stored census, with the locks :meth:`acquire` holds (#1571).

        Applies the same two adjustments :func:`locked_census` makes
        after its refresh read: an elected key another loop holds
        mid-transition stays a live election for this candidate, and a
        held elected key on this host fences it. Call only while the
        stack :meth:`acquire` returned is held.
        """
        current = dict(self._census or {})
        elections = dict(current.get("elections") or {})
        for key in self._busy:
            prior = (self._selections or {}).get(key)
            kept = current.get("selections", {}).get(key, prior)
            if kept is not None:
                elections[key] = kept
        for key in self._ledger.held_keys():
            chosen = current.get("selections", {}).get(key)
            if (isinstance(chosen, dict)
                    and chosen.get("host") == self._ledger.base.name):
                elections[key] = chosen
        current["elections"] = elections
        self._census = current
        return current


@contextmanager
def locked_census(queue: PoolQueue, ledger: ResourceLedger, controller):
    """Elections on this host (sorted/nonblocking) BEFORE H; refresh through acquire.

    Only a measurement elected for *this* host can fence its admission
    (``blocking_selection`` is host-filtered), so only those keys are locked.
    Locking every READY/CLAIMED measurement key instead turned a large
    measurement batch into a fleet-wide livelock: each of a box's worker loops
    holds its own candidate's transition lock through its pass, so almost
    every census met one busy key and denied every row on every host
    (2026-10-05, 38-49 READY PACT rows, 6 loops per Spark).

    An elected key another loop holds mid-transition is not a refusal: it is
    kept as a live election for this pass, the conservative reading, so lower
    priority work stays fenced and the pass still decides everything else.
    Unlocked reads only ever err toward fencing: a missing row is not
    retirement, and retirement needs an exact ending or a strictly newer
    publication, both durable. Election writes stay serialized as before: a
    measurement elects only in its own claim pass, under its own transition
    key and the elected host's H, and this refresh runs under this host's H.
    The candidate key may already be owned by the caller (the reentrant
    transition contract). The reader fence is acquired once for both phases
    (#1498); a fence or admission refusal releases what was taken.
    """
    census_reader = CensusReader(queue, ledger)
    here = ledger.base.name
    with census_reader.held():
        discovered = census_reader.capture()
        with ExitStack() as held:
            busy: set[str] = set()
            for key in sorted(key for key, chosen in discovered["selections"].items()
                              if chosen["host"] == here):
                if not held.enter_context(queue._transition_locked(key, blocking=False)):
                    busy.add(key)
            held.enter_context(queue._admission_lock(controller))
            current = census_reader.capture()
            for key in busy:
                # Mid-transition under another loop: fenced for this pass.
                chosen = current["selections"].get(key, discovered["selections"][key])
                current["elections"][key] = chosen
            # A success/cancellation slot is not physical ownership settlement.
            # Under H, retained tokens on this elected host still prevent refill.
            for key in ledger.held_keys():
                chosen = current["selections"].get(key)
                if chosen is not None and chosen["host"] == here:
                    current["elections"][key] = chosen
            yield current


@contextmanager
def pass_admission_census(pass_census: PassCensus, queue: PoolQueue,
                           ledger: ResourceLedger, controller):
    """One census for one candidate of a claim pass, scanned once (#1571).

    The first candidate reads through :func:`admission_census`: the
    reader fence plus the discovery scan outside host admission and
    the refresh scan under it, holding the elected measurement keys
    of this host and host admission for its body. That census is
    stored. Each later candidate takes the reader guard before M and H,
    and refreshes election sources without a complete scan.

    The candidate holds these locks through resource reservation. Elections
    on this host therefore serialize against its refresh. A refusal ends
    this pass, and the next pass reads fresh. A successful claim returns
    from the pass; no later candidate consumes its old census.
    """
    try:
        reused = pass_census.acquire()
    except CensusUnavailable as exc:
        pass_census.invalidate()
        yield {"unavailable": str(exc)}
        return
    if reused is not None:
        with reused:
            yield pass_census.reused()
        return
    with admission_census(queue, ledger, controller) as census:
        if "unavailable" in census:
            pass_census.invalidate()
            yield census
            return
        pass_census.store(census)
        yield census


@contextmanager
def admission_census(queue: PoolQueue, ledger: ResourceLedger, controller):
    # Catch setup refusals only, never an exception thrown by the claim body.
    with ExitStack() as held:
        try:
            census = held.enter_context(locked_census(queue, ledger, controller))
        except CensusUnavailable as exc:
            yield {"unavailable": str(exc)}
            return
        yield census


def blocking_selection(census: dict, item: dict, *, host: str, funded_by: str | None) -> dict | None:
    """UNKNOWN-first: no current timed candidate proves a safe finish."""
    for key, chosen in sorted(census["elections"].items()):
        if (chosen["host"] == host and key != item["action_key"]
                and int(item.get("priority", 0)) < chosen["priority"] and funded_by != key):
            return chosen
    return None


#: How long an elected gang waits before it reserves its member's demand on each
#: elected host (#1721). See ``docs/design.md``, "Priority rule".
GANG_RESERVE_AFTER_S = 600.0

#: The dimensions a row that omits them is unknown in, so it is taken to demand
#: the whole host: ``adaptive_cpu`` reads a missing or zero ``cpu`` as unbounded
#: CPU use and refuses it on a held box. Any other dimension a row omits it
#: demands none of, as the ledger itself counts it (``gpu`` too, unless the row
#: sets ``needs_gpu``).
_UNKNOWN_WHEN_ABSENT = ("cpu", "mem_gb")


def reserves_after(first_published: object, now: float, *, authority: bool) -> bool:
    """Whether a gang first published at ``first_published`` reserves its hosts at ``now``.

    Two things hold: the gang has waited past :data:`GANG_RESERVE_AFTER_S`,
    and the executing host holds live authority (``authority``): its own
    worker announcement is fresh, so the mover-path terms the reservation is
    computed from describe this host now, not a stale picture of it. Without
    that copy nothing a gang waits on can be told from other work. The host
    then keeps what it had before the reservation: the fence against strictly
    lower priority, and nothing more.
    """
    return (authority is True
            and isinstance(first_published, (int, float)) and not isinstance(first_published, bool)
            and now - first_published > GANG_RESERVE_AFTER_S)


def _gang_reserves(chosen: dict, now: float, authority: bool) -> bool:
    """Whether the gang behind election ``chosen`` reserves its host (:func:`reserves_after`)."""
    state = chosen.get("member_state")
    if state != "ready":
        return False
    rank = chosen.get("rank")
    return reserves_after(rank[1] if isinstance(rank, list) and len(rank) == 3 else None, now,
                          authority=authority)


def reservation_priority_on(census: dict, *, host: str, now: float, authority: bool,
                            exclude_group: str | None = None) -> int | None:
    """The highest priority among gangs that reserve ``host`` now, else ``None``.

    A gang reserves a host once its wait passed the bound, on a host that holds
    live authority (``authority``, :func:`reserves_after`). The reservation
    wins over an equal-priority single there; a strictly higher-priority row
    keeps its place ahead of the gang. That is the priority order, not a
    reservation exception (#1721).
    """
    reserving = [chosen["priority"] for chosen in census.get("gang_elections", {}).values()
                 if chosen["host"] == host and chosen["group"] != exclude_group
                 and _gang_reserves(chosen, now, authority)]
    return max(reserving) if reserving else None


def _reserved(member_demand: object, capacity: Mapping) -> dict[str, int]:
    """What the elected member holds back on this host, per ledger dimension.

    An unknown demand reserves the whole host. Dimensions the host ledger does
    not carry (tier tokens, which live on a tier ledger) are not host capacity
    and are not reserved.
    """
    whole = {kind: int(count) for kind, count in capacity.items()
             if type(count) is int and count > 0}
    if not (isinstance(member_demand, dict)
            and all(isinstance(kind, str) and type(count) is int and count >= 0
                    for kind, count in member_demand.items())):
        return whole
    reserved = {kind: min(count, whole[kind]) for kind, count in member_demand.items()
                if count > 0 and kind in whole}
    if reserved:
        # A member that takes anything on this host and leaves its CPU or memory
        # out, or declares zero CPU, is unknown in it: admission reads that as an
        # unbounded consumer that needs the box empty (``adaptive_cpu``). Unknown
        # is consuming, so it reserves all of that dimension. A member that
        # declares no host dimension at all has nothing to reserve here.
        for kind in _UNKNOWN_WHEN_ABSENT:
            if kind in whole and (kind not in member_demand or (kind == "cpu" and not member_demand[kind])):
                reserved[kind] = whole[kind]
    return reserved


def reservation_shortfall(item: dict, member_demand: object, *, held: object,
                          capacity: object, demand: object = None) -> dict | None:
    """``None`` when ``item`` fits beside the member's reservation; else why not.

    For every dimension the member reserves::

        held[d] + row[d] + reserved[d] <= capacity[d]

    A row dimension that is unknown counts as the whole host's capacity, never
    as zero: unknown is consuming, not exempt. Unknown is a missing or zero
    ``cpu``, a missing ``mem_gb`` or ``gpu`` of a GPU row, and any value that
    does not read. Any other omitted dimension demands none of it. A host
    ledger that did not read is the same, fail safe.
    """
    if not (isinstance(held, dict) and isinstance(capacity, dict)):
        return {"unknown": "host ledger unreadable"}
    resources = demand if demand is not None else item.get("resources")
    short: dict[str, int] = {}
    for kind, reserved in _reserved(member_demand, capacity).items():
        total = int(capacity[kind])
        asked = resources.get(kind) if isinstance(resources, dict) else None
        if asked is None and isinstance(resources, dict) and (
                kind not in _UNKNOWN_WHEN_ABSENT
                and not (kind == "gpu" and item.get("needs_gpu") is True)):
            asked = 0
        if type(asked) is not int or asked < 0 or (kind == "cpu" and asked == 0):
            asked = total
        used = held.get(kind, 0)
        if type(used) is not int or used < 0:
            used = total
        over = used + asked + reserved - total
        if over > 0:
            short[kind] = over
    return short or None


def gang_blocking(census: dict, item: dict, *, host: str, group: str | None,
                  now: float | None = None, held: object = None,
                  capacity: object = None, authority: bool = False,
                  demand: object = None) -> dict | None:
    """What a live gang election does to ``item`` on its host (#1517, #1721).

    Strictly lower priority is fenced from election, as before, with no wait
    and no authority needed. Equal priority is not touched while the gang is
    young, or on a host without live authority that lets a reservation tell
    the gang's demand from other work (``authority``, :func:`reserves_after`;
    absent means without). Past :data:`GANG_RESERVE_AFTER_S`, on a host with
    live authority, the gang RESERVES its elected member's declared demand on
    the host, and a row is admitted only if the reservation survives it
    (:func:`reservation_shortfall`). Never held: the gang's own members and
    any gang's (two gangs of one priority are ordered by ``rank``), and a
    verified publication canary slot (its own contract). Higher priority is
    never held. The returned election carries ``reservation`` (the shortfall)
    when it is this rule that holds.
    """
    now = time.time() if now is None else now
    for key, chosen in sorted(census.get("gang_elections", {}).items()):
        if chosen["host"] != host or key == item["action_key"] or chosen["group"] == group:
            continue
        if int(item.get("priority", 0)) < chosen["priority"]:
            return chosen
        if (item.get("gang") is not None or int(item.get("priority", 0)) != chosen["priority"]
                or isinstance(item.get("publication_canary"), dict)
                or not _gang_reserves(chosen, now, authority)):
            continue
        short = reservation_shortfall(item, chosen.get("demand"), held=held, capacity=capacity,
                                      demand=demand)
        if short is not None:
            return {**chosen, "reservation": short}
    return None


def elect(queue: PoolQueue, ledger: ResourceLedger, controller, item: dict,
          verdict: dict, *, sampled_unix: object, gpu_sample: Mapping | None) -> dict | None:
    """Choose a host once; a finite incumbent *opportunity* is metadata, not a bound."""
    from . import pool
    generation = queue.attempt_generation(item)
    with locked_census(queue, ledger, controller) as census:
        prior = _read(queue.passes_path(item["action_key"]), optional=True) or {}
        chosen = selection(prior)
        if chosen is not None and chosen["generation"] == generation:
            return chosen
        if chosen is not None and item["action_key"] in census["elections"]:
            raise CensusUnavailable("previous measurement election has not retired")
        holders = verdict.get("holders")
        if (not isinstance(sampled_unix, (int, float)) or isinstance(sampled_unix, bool)
                or not 0 <= pool._now() - sampled_unix <= adaptive_cpu.MAX_SAMPLE_AGE_S):
            return None
        if item.get("needs_gpu"):
            stamp = gpu_sample.get("sampled_unix") if gpu_sample is not None else None
            if (gpu_sample is None or gpu_sample.get("schema") != "prismabuild.gpu_capacity.v1"
                    or gpu_sample.get("complete") is not True or gpu_sample.get("attributed") is not True
                    or gpu_sample.get("foreign_processes") != []
                    or not isinstance(stamp, (int, float)) or isinstance(stamp, bool)
                    or not 0 <= pool._now() - stamp <= adaptive_cpu.MAX_SAMPLE_AGE_S):
                return None
        if (verdict.get("withhold") is not True or verdict.get("why") != "draining_for_measurement"
                or not isinstance(holders, list) or not holders):
            return None
        if not any(queue.attempt_generation(row) == generation
                   for row in census["measurements"].get(item["action_key"], [])):
            raise CensusUnavailable("measurement publication changed before election")
        # Re-read sealed incumbent opportunities while H serializes admission.
        # No optimistic timeout-derived safe-fit/backfill permission follows.
        # Every incumbent must be a claimed action on this host, whose own
        # lifetime bounds the wait. A claimed action that declared no finite
        # deadline (``pbrun`` without ``--timeout-s``, a progress-governed
        # action) still elects: refusing left the host open to refill once
        # the bounded attention lapsed, and a continuous lower-priority stream
        # starved the measurement (#1419). A RAM-tier fill hold (#1222) or a
        # raw holder with no readable claim is not an action lifetime: no
        # election, exactly as before, so the bounded episode still lapses.
        holders = [key for key in ledger.held_keys()
                   if not key.startswith(storage_tiers.RAM_HOST_MEMORY_PREFIX)]
        if not holders or len(holders) != len(ledger.held_keys()):
            return None
        ends = []
        for key in holders:
            opportunity = census["opportunities"].get(key)
            claimed = opportunity.get("claimed_unix") if isinstance(opportunity, dict) else None
            if (opportunity is None or opportunity.get("host") != ledger.base.name
                    or not isinstance(claimed, (int, float)) or isinstance(claimed, bool)):
                return None
            requested = opportunity.get("requested")
            if isinstance(requested, (int, float)) and not isinstance(requested, bool):
                end = float(claimed) + float(requested)
                if math.isfinite(end):
                    ends.append(end)
        chosen = {"schema": SCHEMA, "action_key": item["action_key"], "generation": generation,
                  "host": ledger.base.name, "published_unix": float(item["published_unix"]),
                  "priority": int(item.get("priority", 0)), "epoch_unix": pool._now(),
                  "opportunity_unix": max([pool._now(), *ends])}
        prior[FIELD] = chosen
        prior["action_key"] = item["action_key"]
        pool._write_json_atomic(queue.passes_path(item["action_key"]), prior)
        return chosen

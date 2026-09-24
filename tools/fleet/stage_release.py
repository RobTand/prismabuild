#!/usr/bin/env python3
"""Take a staged range off the tier and give its tokens back (#583).

The egress node.  It is the only thing that may return a mover's tier tokens,
because it is the only thing that removes the bytes they stand for, and those
two have to be one operation: **held tier tokens equal bytes on the stage, at
every instant**.  The #853 partial prune is the one deliberate slack, and it
errs the safe way: it unlinks a positively stale destination and releases
nothing, so through that window the held tokens *cover* the stage's bytes
rather than equal them, and the whole charge settles when the owner retires.
Releasing at ``finish`` instead would bound concurrent copies
rather than resident bytes --- twenty-one movers of 34.4 GB, run one after
another, leave 722 GB on a 721 GB stage while the ledger reads its full supply
free at every step --- so a mover keeps its tokens from ``finish`` until an
egress deletes its files.

There is no retained-but-unpinned state.  Bytes the ledger cannot see are the
overfill the reservation exists to prevent, arriving by another road.  Evicting
is still deleting --- what #598 changed is *when* and *whether*.  A range a live
consumer's window names is **adopted** instead: the tokens move from the
finished mover's key to the successor's and nothing is copied
(``tier_loop.adopt``).  What no window names stays resident, held and counted,
until a window cannot be placed without the room --- the ``pressure`` argument
to :func:`sweep`.  Both are the same rule stated twice: an orphan is evicted
when the tier needs its tokens, never because a clock said so.

**A pending copy handoff defers the same way (#768).**  While a live ram
promotion's sealed claim names a source leg, the egress keeps that leg's file,
the stage mover's fragment, its material sidecar and its full occupancy charge,
and retries after the claim ends.  The handoff outranks the generic co-owner
and in-flight-destination shared skip: another owner's same-path fragment
cannot stand in for the promotion's own consumer/manifest cover, so this
owner's proof and charge stay while the claim lives, and the ordinary shared
settlement resumes on the retry.  It files no retiring mark while any handoff
in the document is deferred: a mark closes one material generation to new
acquires, and the promotion takes its proof-only cover through
``reader_lease.acquire`` after its claim row exists, so the mark would refuse
the very handoff it is protecting.  The mark waits until no handoff remains,
then the ordinary pinned retirement files it; deleting stays safe meanwhile
because every pass re-reads claims and pins under the same ownership lock.  The
promotion's ram fragment names another tier and path and can never prove the
SSD incarnation it read, so retiring the source early would leave a surviving
file nothing can prove and free capacity its bytes still occupy.

**The mover's own live claim is not a co-owner (#793).**  A movement node
files its final ``record_move`` receipt before the worker retires its
``claimed/`` row, so an egress can run in that gap and find the mover being
evicted still claimed.  The claim census attributes that one key's derived
paths separately, and they **defer** --- the file, this mover's fragment,
material and full charge stay, and the next sweep retries once the worker's
terminal transition has retired the claim.  Sharing them instead would
decharge this mover's own duplicate and drop its only vouch while the bytes
stayed behind nothing.  The claim is not settled on its receipt: a move
receipt carries no immutable attempt identity, so a complete-looking one
cannot be told from a previous attempt's while the same key is claimed again,
and a wall-clock stamp is not a substitute (2026-09-21 root QA).

**Delete, then release, then drop the fragment.**  Each order is wrong in one
direction and this one is wrong in none that matters: a crash after the deletes
and before the release leaves tokens held for bytes that are gone, which costs
capacity until the orphan sweep or a rerun returns them, and a crash after the
release and before the deletes would leave bytes on a stage the ledger thinks
is empty --- which is the failure this whole node exists to prevent.  Releasing
last is the direction that fails safe.

It runs three ways.  As an **action row** it is published by the tier loop once a
consumer's accepted phase has passed the range, and its receipt is what makes
the eviction visible.  As a **sweep** the tier loop calls :func:`evict` directly
for a mover that no ready or claimed item still names --- a consumer that was
withdrawn leaves its movers holding the stage forever otherwise.  And as a
**reconciliation** (:func:`reconcile`, #608) the same sweep compares the stage
root against the fragments of the movers the fleet still wants, because both
paths above are driven from a *key*: the ledger's held keys or a receipt's, and
bytes no key holds are invisible to either for the life of the fleet.  That is
what a withdrawn mid-copy mover leaves --- the shards it verified and renamed
into place, and its ``.partial`` temporaries --- and its tokens have already
gone back by then, so nothing keyed can find it.

**A stage root belongs to one queue, and says so (#628).**  Every rule above
decides *what* to delete; none of them asked *whose* stage was being walked.
On 2026-09-18 a test announced ``/stage/prewarm`` to a queue under
``tmp_path`` on the storage box, that queue's fragments attributed nothing,
and :func:`reconcile` deleted 671 GB of staged shards in one walk while the
run they were staged for was reading them.  So the tier loop writes
:data:`STAGE_ROOT_MARKER` beside the staged bytes when it announces the tier
(:func:`register_stage_root`), and :func:`sweep`, :func:`reconcile` and
:func:`evict` delete nothing under a root whose marker is missing, unreadable
or names another queue --- they say why in the receipt and keep the tokens.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from contextlib import contextmanager
import contextvars
import errno
import json
import os
from pathlib import Path
import socket
import stat as statmod
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import produced_output  # noqa: E402
from prismabuild import reader_lease  # noqa: E402
from prismabuild import residency_map  # noqa: E402
from prismabuild import residency_plan  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
from prismabuild import window_credit  # noqa: E402

import prewarm_loop  # noqa: E402
from stage_move import (  # noqa: E402
    RANGE_SUFFIX, _current_directory_version, _metadata_version,
    _trusted_directory_stamp, paths_named_once, pre_range_stage_relative,
    stage_relative,
)

#: The errnos that mean "this file has no such attribute", as opposed to "this
#: question cannot be answered here".  Linux reports ``ENODATA``; the name
#: ``ENOATTR`` is an alias for it where it exists at all.
_XATTR_ABSENT = frozenset(
    value for value in (getattr(errno, "ENODATA", None),
                        getattr(errno, "ENOATTR", None))
    if value is not None)

#: How ``stage_move`` names the file it is copying into before it verifies the
#: digest and ``os.replace``s it into place: ``.<final name>.partial`` beside
#: the destination, or ``.<final name>.<owner>.partial`` once the mover keys
#: its temporary by its own action key (#620).  One definition, read here,
#: because a temporary left by a killed or withdrawn copy is nobody's and
#: nothing else ever removes it.
PARTIAL_PREFIX = "."
PARTIAL_SUFFIX = ".partial"

#: The event a reconciled eviction publishes, so an operator can tell bytes a
#: mover's own fragment named from bytes nothing named at all.
UNATTRIBUTED_EVENT = "stage-unattributed-evicted"

#: The event a bounded orphan recovery publishes.  Deliberately not
#: ``UNATTRIBUTED_EVENT``: that one reports what routine reconciliation found
#: unowned by walking the stage, and this one reports a named historical range
#: an operator asked about by identity.  Telling them apart is the audit.
ORPHAN_RECOVERY_EVENT = "stage-orphan-recovered"

#: The file a stage root carries to say which queue it belongs to (#628), and
#: the event a sweep publishes when a root does not belong to it.
STAGE_ROOT_MARKER = ".prismabuild-stage.json"
STAGE_ROOT_MARKER_SCHEMA_V1 = "prismabuild.stage-root.v1"
STAGE_ROOT_REFUSED_EVENT = "stage-root-refused"

#: The event a dead-owner eviction publishes, so an operator can tell bytes
#: whose only owner was terminal-dead (failed consumer, withdrawn mover) from
#: bytes routine reconciliation found unowned and from a named historical
#: range an operator asked about by identity.
DEAD_OWNER_EVENT = "stage-dead-owner-evicted"

#: The event an orphan sweep reports for a held mover that has no receipt and
#: no single direct fragment naming a consumer that has ended (#892), when the
#: tier's window still lacks room after the pass.
RECEIPTLESS_HOLDER_EVENT = "stage-receiptless-holder-retained"

#: The event an orphan sweep publishes when it retires a produced-output batch
#: whose producer attempt has ended (#929): the ``retire_batch`` its producer
#: never ran, run for it.
PRODUCED_ORPHAN_EVENT = "stage-produced-orphan-retired"

#: The operator report of a held key the sweep can prove neither live nor an
#: orphan (#929).  Published once per change of the reason, whatever the
#: tier's pressure, so a holder nothing can classify is seen once rather than
#: never or on every cycle.
HOLDER_UNRESOLVED_EVENT = "stage-holder-unresolved"

#: The reason each unresolved holder was last reported with, per queue and
#: tier.  Process-local on purpose: the tier loop is long-lived, and a restart
#: reporting every unresolved holder once more is the right amount of noise.
_UNRESOLVED_REPORTS: dict[tuple[str, str], dict[str, str]] = {}

#: The record an orphan sweep leaves when it cannot read a tier's ledger
#: (#1007).  Which held movers still count as attribution is unknown then,
#: and unknown ownership never deletes: the pass that needed the read is
#: skipped for the cycle, and this record says which pass and why.
LEDGER_UNREADABLE_EVENT = "stage-sweep-ledger-unreadable"

#: The event a bounded prune of positively stale mentions publishes (#853).
#: Unlike a dead-owner eviction this keeps the whole old holder: no charge
#: moves until the last fragment goes through the ordinary whole-owner egress,
#: which settles it exactly once.  The receipt says so explicitly.
STALE_MENTION_EVENT = "stage-stale-mention-pruned"

#: The process-local, skip-only checkpoint cache (#853, #1056) is bounded by
#: what is on disk, not by literals: at most one checkpoint per owner the
#: dead-owner sweep discovered on its latest pass
#: (:func:`_retain_skip_checkpoints`), and each checkpoint's fences are the
#: parent directories of its own fragment's paths plus the co-owner
#: fragments that name them -- documents the census already holds parsed.
#: A full cache refuses a newcomer, which then takes the uncached scan
#: (slower, never weaker), and never evicts an owner the next pass visits.
#: A checkpoint may only skip the cleanup scan -- never a mutable-state
#: check, a deletion, or an adoption.


def queue_identity(queue: pool.PoolQueue) -> str:
    """The string a marker names a queue by: its root's real path.

    The tier loop, the worker loops and the egress CLI on the storage box all
    spell the queue as ``/mnt/shared/prismabuild-fleet/pb-queue``, and the real
    path is what they agree on even where one of them is handed a symlink.
    """

    return os.path.realpath(str(queue.root))


def read_stage_root_marker(stage_root: str | Path) -> dict[str, object] | None:
    """The marker under ``stage_root``, or ``None`` when there is none.

    Raises ``OSError`` for anything but absence and ``ValueError`` for a file
    that is not the object this module writes; both are "cannot answer", which
    every caller reads as "not mine".
    """

    path = Path(stage_root) / STAGE_ROOT_MARKER
    try:
        with open(path) as stream:
            marker = json.load(stream)
    except FileNotFoundError:
        return None
    if (not isinstance(marker, dict)
            or marker.get("schema") != STAGE_ROOT_MARKER_SCHEMA_V1):
        raise ValueError(f"{path}: not a stage-root marker")
    return marker


def stage_root_refusal(queue: pool.PoolQueue, stage_root: str | Path) -> str | None:
    """Why this queue may not delete under ``stage_root``; ``None`` when it may.

    Owned means a marker is there and names this queue.  Missing, unreadable,
    malformed and another queue's marker all refuse: the failure this guards
    is deleting what is not one's own, so every answer short of "yes, mine" is
    "no".  Nothing here is a policy about *when* a root is claimable --- an
    unregistered root refuses too, and only :func:`register_stage_root` turns
    it into an owned one.
    """

    try:
        marker = read_stage_root_marker(stage_root)
    except OSError as exc:
        return f"stage_root_marker_unreadable: {exc}"
    except ValueError as exc:
        return f"stage_root_marker_invalid: {exc}"
    if marker is None:
        return "stage_root_unregistered"
    owner = str(marker.get("queue_root") or "")
    if owner != queue_identity(queue):
        return f"stage_root_belongs_to_another_queue: {owner}"
    return None


def register_stage_root(queue: pool.PoolQueue, *, tier_id: str,
                        stage_root: str | Path) -> str:
    """Mark ``stage_root`` as this queue's, or say why it could not be.

    Returns ``"registered"`` --- written now, or already naming this queue ---
    or the refusal.  A marker naming another queue is never overwritten: the
    box's loop claiming a root a test's queue registered, or the reverse, is
    the two-owners state this exists to refuse, and the operator removes the
    marker by hand when the queue really has moved.  A root this queue cannot
    write (the Sparks mount the stage read-only) is reported, not raised: the
    tier is still announced, and the sweep refuses it on the same fact.
    Written by temporary and ``os.replace`` so a reader never sees half of it.
    """

    refusal = stage_root_refusal(queue, stage_root)
    if refusal is None:
        return "registered"
    if refusal != "stage_root_unregistered":
        return refusal
    path = Path(stage_root) / STAGE_ROOT_MARKER
    marker = {
        "schema": STAGE_ROOT_MARKER_SCHEMA_V1,
        "queue_root": queue_identity(queue),
        "queue_root_given": str(queue.root),
        "tier_id": str(tier_id),
        "host": socket.gethostname(),
        "unix": time.time(),
    }
    temporary = path.with_name(f".{STAGE_ROOT_MARKER}.{os.getpid()}.tmp")
    try:
        with open(temporary, "w") as stream:
            json.dump(marker, stream, indent=2, sort_keys=True)
            stream.write("\n")
        os.replace(temporary, path)
    except OSError as exc:
        try:
            temporary.unlink()
        except OSError:
            pass
        return f"stage_root_marker_unwritable: {exc}"
    return "registered"


def _refused_receipt(*, tier_id: str, stage_root: str | Path, refusal: str,
                     mover_action_key: str = "", consumer_action_key: str = "",
                     reason: str = "orphan-sweep") -> dict[str, object]:
    """The receipt a refused deletion leaves: nothing deleted, nothing released."""

    return {
        "schema": pool.POOL_EGRESS_SCHEMA_V1,
        "event": STAGE_ROOT_REFUSED_EVENT,
        "action_key": mover_action_key,
        "consumer_action_key": consumer_action_key,
        "tier_id": tier_id,
        "stage_root": str(stage_root),
        "reason": reason,
        "entries_deleted": 0,
        "entries_already_gone": 0,
        "bytes_deleted": 0,
        "tokens_released": 0,
        "skipped": refusal,
        # Not complete: whatever this key's bytes are, they are still where
        # they were, and the tokens that stand for them stay held.
        "complete": False,
        "errors": [refusal],
        "host": socket.gethostname(),
        "unix": time.time(),
    }


def _ledger_unreadable_receipt(*, tier_id: str, stage_root: str | Path,
                               skipped: str, exc: BaseException,
                               ) -> dict[str, object]:
    """What a sweep that could not read its tier ledger skipped, and why (#1007).

    Nothing is deleted and nothing is released: the bytes and the tokens stay
    where they were until a later cycle can read the ledger.
    """

    reason = f"tier ledger unreadable: {exc}"
    return {
        "schema": pool.POOL_EGRESS_SCHEMA_V1,
        "event": LEDGER_UNREADABLE_EVENT,
        "action_key": "",
        "consumer_action_key": "",
        "tier_id": tier_id,
        "stage_root": str(stage_root),
        "reason": reason,
        "skipped": skipped,
        "entries_deleted": 0,
        "bytes_deleted": 0,
        "tokens_released": 0,
        "complete": False,
        "errors": [reason],
        "host": socket.gethostname(),
        "unix": time.time(),
    }


def _prune_empty(directory: Path, stop: Path) -> None:
    """Remove the directories a deleted range leaves behind, never past the stage."""

    stop = stop.resolve()
    while directory != stop:
        try:
            if directory.resolve() == stop or stop not in directory.resolve().parents:
                return
            directory.rmdir()
        except OSError:
            return
        directory = directory.parent


#: How many co-owners one egress receipt names before it counts the rest.
SHARED_WITH_LIMIT = 5

#: Residency subdirectories that hold records, never fragments.  Pins and
#: retiring marks (``leases/``) and publish-time sidecars (``material/``)
#: live beside the fragments in dedicated subdirectories, and the
#: produced-output template, scope and batch records live under their own
#: exported subdirectories.  Every fragment enumerator skips them: a record
#: parsed as a fragment would taint -- or, before #798, crash -- every egress
#: and sweep on a healthy tier.  The produced *fragments* are a real fragment
#: namespace one level deeper (``OUTPUT_FRAGMENTS_SUBDIR``, reached through
#: :func:`produced_output.output_fragment_root`) and are traversed by
#: :func:`_fragment_census`, never skipped.
RESERVED_RESIDENCY_SUBDIRS = frozenset({
    reader_lease.LEASES_SUBDIR,
    reader_lease.MATERIAL_SUBDIR,
    produced_output.OUTPUT_TEMPLATES_SUBDIR,
    produced_output.OUTPUT_SCOPES_SUBDIR,
    produced_output.OUTPUT_BATCHES_SUBDIR,
})


def _namespace_shaped(name: str) -> bool:
    """A 64-character lowercase-hex directory name: an action key or namespace."""

    return (len(name) == 64
            and all(character in "0123456789abcdef" for character in name))


class _CensusMemo:
    """Parsed census documents, reused while their file version holds (#988).

    An egress takes its ownership censuses twice: once before the stage
    ownership lock, as a hint, and again under it, where the delete decision
    is made.  The pass under the lock lists every directory again and opens
    every document again, so a file that was added, removed, replaced or
    rewritten is seen before any decision uses it.  Only a document whose
    version is unchanged skips its read and parse, and the version is taken
    with ``fstat`` on the descriptor the pass just opened.  It is the
    publication gate's own reuse fence (``stage_move._metadata_version``,
    #761): device, inode, size, mtime and ctime.

    A read that failed is never remembered: unreadable stays freshly
    unreadable.  A claim's derived stage paths are remembered by claim key,
    because they come from the sealed request and the data manifest, and both
    are immutable under their digests; the claim listing and each claim
    record are still read fresh.  One memo lives for one call; nothing
    carries over between calls.  (:class:`CensusIndex`, the tier loop's
    subclass, is the exception, and its docstring says what it keeps.)

    It counts what it parses and what it reuses (fragments and material
    sidecars; for pins, what it parses), so a caller can record how much
    census work ran inside its hold (:func:`_locked_parse_record`).
    """

    def __init__(self) -> None:
        self.fragments: dict[str, tuple[tuple, dict[str, object]]] = {}
        self.paths: dict[int, frozenset[str]] = {}
        self.normalized: dict[int, frozenset[str]] = {}
        self.claims: dict[tuple, frozenset[str]] = {}
        self.sources: dict[tuple, frozenset[str]] = {}
        self.pins = _PinMemo()
        self.materials: dict[str, tuple[tuple, object]] = {}
        #: A remembered fragment's file, by the parsed document's identity.
        self.files: dict[int, str] = {}
        self.parses = 0     # fragments and material sidecars parsed
        self.reuses = 0     # fragments and material sidecars reused

    def counts(self) -> tuple[int, int]:
        """``(parsed, reused)`` so far: every document kind, pins included."""

        return self.parses + self.pins.parses, self.reuses

    def paths_of(self, document: Mapping[str, object]) -> frozenset[str] | None:
        """The stage paths a remembered fragment names, or ``None``."""

        return self.paths.get(id(document))

    def normalized_paths_of(self, document: Mapping[str, object],
                            ) -> frozenset[str] | None:
        """The same paths, ``normpath``-ed once per parse, or ``None``."""

        named = self.paths.get(id(document))
        if named is None:
            return None
        normalized = self.normalized.get(id(document))
        if normalized is None:
            normalized = frozenset(os.path.normpath(path) for path in named)
            self.normalized[id(document)] = normalized
        return normalized

    def version_of(self, document: Mapping[str, object],
                   ) -> tuple[str, tuple] | None:
        """The file and version a remembered fragment was read from, or ``None``.

        The version is the one the parse, or its latest reuse, was fenced
        on: ``fstat`` of the descriptor that read it (#1056 fences a skip
        checkpoint on the co-owner fragments a verdict relied on).
        """

        key = self.files.get(id(document))
        if key is None:
            return None
        hit = self.fragments.get(key)
        if hit is None or hit[1] is not document:
            return None
        return key, hit[0]

    def forget(self, key: str) -> None:
        """Drop one fragment file's parse, and the path set keyed by it."""

        old = self.fragments.pop(key, None)
        if old is not None:
            self.paths.pop(id(old[1]), None)
            self.normalized.pop(id(old[1]), None)
            self.files.pop(id(old[1]), None)


class _PinMemo(dict):
    """``reader_lease.live_for``'s pin memo, counting the pins it parses.

    ``live_for`` stores a pin's parse exactly once per parse and never on a
    reuse, so the number of stores is the number of pins it parsed.
    """

    def __init__(self) -> None:
        super().__init__()
        self.parses = 0

    def __setitem__(self, key: str, value: object) -> None:
        self.parses += 1
        super().__setitem__(key, value)


class CensusIndex(_CensusMemo):
    """A census memo the tier loop keeps from one cycle to the next (#992).

    Every cycle took the residency census from nothing: it listed every
    namespace directory and parsed every fragment, 429 namespaces, 1,957
    produced-output directories and 240,705 fragment entries on the live
    shape, and nothing had changed since the cycle before.  This keeps what
    a census read, and re-reads only what changed.

    Two layers, both fences the census already trusts:

    * **A document** is reused while its file version holds -- device,
      inode, size, mtime and ctime, taken with ``fstat`` on the descriptor
      that read it (:class:`_CensusMemo`, the publication gate's #761 fence).
    * **A directory** is not listed again while its ``lstat`` version equals
      a stamp taken before its last listing, and no document in it is opened
      while its ``stat`` version still equals the one it was parsed at
      (:func:`stage_move._trusted_directory_stamp` has the argument for the
      directory; a stamp is refused inside the clock tick it was taken in,
      on a network filesystem, and on anything that is not a directory).
      The stamp is taken before the listing and compared again after it,
      and only an equal pair with no unreadable entry is remembered, the way
      a skip checkpoint is (:func:`_install_skip_checkpoint`).  So a census
      under the stage lock still sees a fragment added, removed, replaced or
      rewritten since the hint, as :class:`_CensusMemo` promises, and costs
      one ``lstat`` per directory and one ``stat`` per fragment.

    A directory whose listing produced taint is never remembered: unreadable
    stays freshly unreadable.  A document that left its directory is
    forgotten when the directory is listed again, and a directory that left
    its level when the level is, so what is kept is bounded by what is on
    disk.

    Reader pins are not kept across calls: :func:`reader_lease.live_for`
    lists and opens every pin each census, and its memo remembers every pin
    it ever parsed, so each call that wants one takes a fresh
    :class:`_PinMemo` (:meth:`fresh_pins`).  Neither are the claim and source
    path sets, which only an egress reads.
    """

    def __init__(self) -> None:
        super().__init__()
        #: Namespace directory -> ``(stamp, [(mover, document)], file keys)``.
        self.namespaces: dict[str, tuple[tuple, list[tuple[str, dict[str, object]]],
                                         frozenset[str]]] = {}
        #: Level directory -> ``(stamp, [(name, kind)])``, ``kind`` one of
        #: ``dir``, ``link``, ``other`` or an ``error: ...`` string.
        self.levels: dict[str, tuple[tuple, list[tuple[str, str]]]] = {}
        #: File keys the last listing of each namespace directory read.
        self._files: dict[str, frozenset[str]] = {}
        self.listed = 0       # directories listed
        self.kept = 0         # directories whose listing was reused
        #: Dead owners whose stale-mention census a skip checkpoint stood
        #: for, and dead owners censused (#1056): the cycle line counts both,
        #: because a skip emits no per-owner receipt.
        self.stale_skipped = 0
        self.stale_censused = 0

    def fresh_pins(self) -> _PinMemo:
        """A pin memo for one call (see the class docstring)."""

        self.pins = _PinMemo()
        return self.pins

    def namespace_hit(self, directory: Path,
                      ) -> list[tuple[str, dict[str, object]]] | None:
        """The fragments a remembered listing of ``directory`` holds, or ``None``."""

        kept = self.namespaces.get(str(directory))
        if kept is None:
            return None
        if (_current_directory_version(directory) != kept[0]
                or not self._documents_hold(kept[2])):
            self.namespaces.pop(str(directory), None)
            return None
        self.kept += 1
        return kept[1]

    def _documents_hold(self, keys: frozenset[str]) -> bool:
        """Whether every document a kept listing read is the version parsed.

        One ``stat`` per fragment.  The directory's stamp says no fragment
        was filed, removed or renamed; this says none was rewritten in
        place, made unreadable or restored to an older mtime, each of which
        moves the file's ctime.  No writer changes a fragment in place, but
        the #988 census under the stage lock promised to see one, and this
        keeps that promise for less than a listing costs.
        """

        for key in keys:
            parsed = self.fragments.get(key)
            if parsed is None:
                return False
            try:
                info = os.stat(key)
            except OSError:
                return False
            if _metadata_version(info) != parsed[0]:
                return False
        return True

    def remember_namespace(self, directory: Path, stamp: tuple | None,
                           found: list[tuple[str, dict[str, object]]],
                           read: set[str], tainted: bool) -> None:
        """Record one listing of ``directory``: what it found and what it read."""

        name = str(directory)
        self.listed += 1
        for gone in self._files.get(name, frozenset()) - read:
            self.forget(gone)
        self._files[name] = frozenset(read)
        if (stamp is None or tainted
                or _current_directory_version(directory) != stamp):
            self.namespaces.pop(name, None)
            return
        self.namespaces[name] = (stamp, list(found), frozenset(read))

    def forget_namespace(self, directory: str) -> None:
        """Drop a namespace directory that left its level, and its documents."""

        self.namespaces.pop(directory, None)
        for gone in self._files.pop(directory, frozenset()):
            self.forget(gone)

    def level_hit(self, directory: Path) -> list[tuple[str, str]] | None:
        """A remembered classification of ``directory``'s children, or ``None``."""

        kept = self.levels.get(str(directory))
        if kept is None:
            return None
        if _current_directory_version(directory) != kept[0]:
            self.levels.pop(str(directory), None)
            return None
        self.kept += 1
        return kept[1]

    def remember_level(self, directory: Path, stamp: tuple | None,
                       children: list[tuple[str, str]]) -> None:
        """Record one listing of a level; forget namespaces that left it."""

        name = str(directory)
        self.listed += 1
        present = {os.path.join(name, child) for child, kind in children
                   if kind == "dir"}
        prefix = name.rstrip(os.sep) + os.sep
        for known in [path for path in self._files
                      if path.startswith(prefix)
                      and os.sep not in path[len(prefix):]
                      and path not in present]:
            self.forget_namespace(known)
        if (stamp is None
                or any(kind.startswith("error") for _child, kind in children)
                or _current_directory_version(directory) != stamp):
            self.levels.pop(name, None)
            return
        self.levels[name] = (stamp, list(children))


class DirectoryRecords:
    """Queue records re-read only where they changed (#992).

    The tier loop reads the same small records every cycle: every movement
    and prewarm receipt, and every ``ready/`` and ``claimed/`` record, three
    times over.  This keeps each directory's parsed records by file version
    and each directory's listing by the stamp of
    :func:`stage_move._trusted_directory_stamp`, so a directory nothing was
    filed in since is not listed, and a record whose version holds is not
    read.  Every writer of these records files by rename
    (``pool._write_json_atomic``), which is what both fences stand on.

    A read that raises is never remembered, and the raise reaches the caller
    exactly as the plain read's would.  One reader keeps one meaning per
    directory: every read of a directory must pass the same ``select`` and
    ``parse``, because what it returns from a kept listing is what the first
    one parsed.

    A changed directory stats every name it lists, not only the names it has
    not seen.  These records are replaced under their own names -- a mover
    re-run under the same content-hash key files its receipt again, and a
    prewarm receipt is pruned and filed afresh -- and a file created after an
    unlink can be given the unlinked file's inode number, so neither the name
    nor the listing's inode number says a record is unchanged.  The #761
    version, ctime included, does.
    """

    def __init__(self) -> None:
        #: Directory -> ``(stamp or None, {name: (version, record)})``.
        self._directories: dict[str, tuple[tuple | None,
                                           dict[str, tuple[tuple, object]]]] = {}
        #: Directory -> a count that moves whenever its record set changes.
        self._generations: dict[str, int] = {}
        #: Directory -> ``(stamp, names)`` for :meth:`names`.
        self._names: dict[str, tuple[tuple, frozenset[str]]] = {}
        self.listed = 0
        self.kept = 0
        self.parsed = 0

    def names(self, directory: Path, *, select) -> frozenset[str]:
        """The names in ``directory`` that ``select(name)`` keeps.

        A names-only listing, kept under the same stamp as :meth:`read`'s:
        creating, removing or renaming an entry moves the directory's
        ``mtime`` and ``ctime``, so while its stamp holds the set is the
        same.  Where no stamp can be trusted (a network filesystem, the tick
        the directory last changed in), the directory is listed every call,
        as a plain ``os.listdir`` is.  A directory that does not exist reads
        as empty and is not remembered; any other ``OSError`` reaches the
        caller, never an empty set.
        """

        name = str(directory)
        kept = self._names.get(name)
        if kept is not None and _current_directory_version(directory) == kept[0]:
            self.kept += 1
            return kept[1]
        self._names.pop(name, None)
        stamp = _trusted_directory_stamp(directory)
        try:
            listed = os.listdir(directory)
        except FileNotFoundError:
            return frozenset()
        self.listed += 1
        found = frozenset(child for child in listed if select(child))
        if stamp is not None and _current_directory_version(directory) == stamp:
            self._names[name] = (stamp, found)
        return found

    def retain(self, directories) -> None:
        """Forget the listing and records of every directory not named.

        For a caller that reads a changing set of directories -- one per
        withdrawal decision, say -- so what is kept stays bounded by what it
        still reads.  A forgotten directory is listed afresh on its next
        read.  Its :meth:`generation` counter is kept, so a number a caller
        remembered from before is never handed out again.
        """

        wanted = {str(directory) for directory in directories}
        for kept in (self._directories, self._names):
            for name in [name for name in kept if name not in wanted]:
                del kept[name]

    def generation(self, directory: Path) -> int:
        """A number that changes whenever ``directory``'s records change.

        Equal before and after a :meth:`read` exactly when that read returned
        the same names with the same record objects, so a pure function of
        the records can be remembered under it.
        """

        return self._generations.get(str(directory), 0)

    def read(self, directory: Path, *, select, parse, thaw=None,
             keep=None, stat_parse: bool = False) -> list[tuple[Path, object]]:
        """``(path, record)`` for each selected name, in name order.

        ``select(entry)`` takes an ``os.DirEntry`` and says whether the name
        is read at all; ``parse(path)`` reads one record and may return
        ``None`` (listed without a record) or raise.  ``keep(record)``, when
        given, says whether a parse may be remembered: one it refuses is
        returned but parsed again next time, and the directory is listed
        again next time too.  ``thaw``, when given, turns what ``parse``
        returned into what the caller receives, on every read: a caller that
        may change the record it is handed keeps the file's bytes here and
        parses them per read, so no change leaks into the next cycle.  A
        directory that does not exist reads as empty and is not remembered.
        With ``stat_parse`` the parse is called ``parse(path, info)``, where
        ``info`` is the ``os.stat`` its version was taken from (``None`` when
        that failed), so a parse that wants the file's metadata does not
        stat the file a second time.
        """

        out = self._read(directory, select=select, parse=parse, keep=keep,
                         stat_parse=stat_parse)
        if thaw is None:
            return out
        return [(path, thaw(path, kept)) for path, kept in out]

    def _read(self, directory: Path, *, select, parse, keep,
              stat_parse: bool = False) -> list[tuple[Path, object]]:
        name = str(directory)
        kept = self._directories.get(name)
        if (kept is not None and kept[0] is not None
                and _current_directory_version(directory) == kept[0]):
            self.kept += 1
            return [(directory / child, record)
                    for child, (_version, record) in sorted(kept[1].items())]
        previous = kept[1] if kept is not None else {}
        stamp = _trusted_directory_stamp(directory)
        try:
            entries = sorted((entry for entry in os.scandir(directory)
                              if select(entry)),
                             key=lambda entry: entry.name)
        except (FileNotFoundError, NotADirectoryError):
            if self._directories.pop(name, None) is not None:
                self._generations[name] = self._generations.get(name, 0) + 1
            return []
        self.listed += 1
        records: dict[str, tuple[tuple, object]] = {}
        out: list[tuple[Path, object]] = []
        complete = True
        changed = False
        try:
            for entry in entries:
                path = directory / entry.name
                info = None
                try:
                    # The listing's own string, not ``path``: converting a
                    # ``Path`` back to a string for every name was most of a
                    # 30,000-name listing's cost.
                    info = os.stat(entry.path)
                    version = _metadata_version(info)
                except FileNotFoundError:
                    continue
                except OSError:
                    complete = False
                    version = None
                hit = previous.get(entry.name)
                if version is not None and hit is not None and hit[0] == version:
                    record = hit[1]
                else:
                    record = parse(path, info) if stat_parse else parse(path)
                    self.parsed += 1
                    changed = True
                    if keep is not None and not keep(record):
                        complete = False
                        version = None
                if version is not None:
                    records[entry.name] = (version, record)
                out.append((path, record))
        except BaseException:
            self._directories.pop(name, None)
            self._generations[name] = self._generations.get(name, 0) + 1
            raise
        if changed or set(previous) != set(records) or not complete:
            self._generations[name] = self._generations.get(name, 0) + 1
        if (not complete or stamp is None
                or _current_directory_version(directory) != stamp):
            stamp = None
        self._directories[name] = (stamp, records)
        return out


#: The tier loop's :class:`DirectoryRecords` while one of its cycles runs,
#: so every step that reads ``ready/`` and ``claimed/`` shares one read of
#: them (#992).  ``None`` everywhere else, where the plain read runs.
_QUEUE_RECORDS: contextvars.ContextVar["DirectoryRecords | None"] = (
    contextvars.ContextVar("stage_release_queue_records", default=None))


@contextmanager
def queue_records_from(reader: "DirectoryRecords"):
    """Serve :func:`queue_records` from ``reader`` inside the block."""

    token = _QUEUE_RECORDS.set(reader)
    try:
        yield reader
    finally:
        _QUEUE_RECORDS.reset(token)


def _queue_record_bytes(path: Path) -> bytes | None:
    """One queue record's bytes, ``None`` when it is not there to read."""

    try:
        return path.read_bytes()
    except FileNotFoundError:
        return None


def _queue_record_from_bytes(path: Path, raw: bytes | None,
                             ) -> dict[str, object] | None:
    """``pool._read_json``'s answer for bytes already read: same checks."""

    if not raw:
        return None
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise pool.PoolContractError(
            f"queue record is not valid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise pool.PoolContractError(f"queue record is not an object: {path}")
    return value


def queue_records(queue: pool.PoolQueue, state: str,
                  ) -> list[tuple[Path, dict[str, object] | None]]:
    """Every entry of one queue state directory with its record, name order.

    ``pool._scan`` then ``pool._read_json`` per entry, with the same answers
    and the same raises.  Inside a tier-loop cycle
    (:func:`queue_records_from`) the directory is listed only when it changed
    and a record read only when its file did; each caller still gets its own
    freshly parsed record.
    """

    reader = _QUEUE_RECORDS.get()
    if reader is None:
        return [(path, pool._read_json(path))
                for path in pool._scan(queue.dir(state))]
    return reader.read(queue.dir(state), select=lambda _entry: True,
                       parse=_queue_record_bytes,
                       thaw=_queue_record_from_bytes)  # type: ignore[return-value]


def _locked_parse_record(memo: "_CensusMemo | None",
                         before: tuple[int, int] | None) -> dict[str, object]:
    """How many census documents a hold parsed and reused (#988).

    ``locked_parses`` counts every fragment, pin and material sidecar the
    pass under the lock parsed; ``locked_reuses`` the fragments and sidecars
    it reused because their file version had not changed since the census
    before the lock.  ``None`` when the caller kept no memo.
    """

    if memo is None or before is None:
        return {"locked_parses": None, "locked_reuses": None}
    parsed, reused = memo.counts()
    return {"locked_parses": parsed - before[0],
            "locked_reuses": reused - before[1]}


def _read_own_material(root: Path, consumer_action_key: str,
                       mover_action_key: str, memo: "_CensusMemo | None"):
    """``reader_lease.read_material``, reused while its file version holds.

    The same three answers: the sidecar, ``None`` when it is absent, or the
    error.  With a ``memo`` the file is still opened, and its ``fstat``
    version decides whether the earlier parse is reused (#988).
    """

    if memo is None:
        return reader_lease.read_material(root, consumer_action_key,
                                          mover_action_key)
    path = reader_lease.material_path(root, consumer_action_key,
                                      mover_action_key)
    key = str(path)
    try:
        with open(path) as stream:
            version = _metadata_version(os.fstat(stream.fileno()))
            hit = memo.materials.get(key)
            if hit is not None and hit[0] == version:
                memo.reuses += 1
                return hit[1]
            memo.parses += 1
            material = reader_lease.validate_material(json.load(stream))
    except FileNotFoundError:
        memo.materials.pop(key, None)
        return None
    except (OSError, ValueError) as exc:
        memo.materials.pop(key, None)
        return exc
    memo.materials[key] = (version, material)
    return material


def _fragment_stage_paths(document: Mapping[str, object]) -> frozenset[str]:
    """Every ``stage_path`` one validated fragment names, as written."""

    out: set[str] = set()
    for entry in dict(document["entries"]).values():
        if isinstance(entry, Mapping):
            path = entry.get("stage_path")
            if isinstance(path, str):
                out.add(path)
    return frozenset(out)


def _read_fragment(path: Path, memo: _CensusMemo | None = None,
                   ) -> dict[str, object] | str:
    """One validated fragment, or the reason it is not one.  Never a skip.

    Skipping is right for a consumer composing its own map
    (``residency_map.read_fragments``) and wrong for every caller that
    deletes: a document that cannot be read is unknown ownership, and
    unknown ownership reported as "no owner" is exactly how staged bytes
    are lost.

    With a ``memo``, a document whose version is unchanged since the memo
    parsed it is returned without a second parse (see :class:`_CensusMemo`).
    """

    key = str(path)
    try:
        with open(path) as stream:
            version = None
            if memo is not None:
                version = _metadata_version(os.fstat(stream.fileno()))
                hit = memo.fragments.get(key)
                if hit is not None and hit[0] == version:
                    memo.reuses += 1
                    return hit[1]
                memo.parses += 1
            document = residency_map.validate_fragment(json.load(stream))
    except (OSError, ValueError) as exc:
        if memo is not None:
            memo.forget(key)
        return str(exc)
    if memo is not None:
        memo.forget(key)
        memo.fragments[key] = (version, document)
        memo.paths[id(document)] = _fragment_stage_paths(document)
        memo.files[id(document)] = key
    return document


def _census_fragment_directory(directory: Path, namespace: str, *,
                               direct: bool,
                               fragments: list[tuple[str, str, dict[str, object], bool]],
                               tainted: list[str],
                               memo: _CensusMemo | None = None) -> None:
    """Every valid fragment filed directly under one namespace directory.

    ``direct`` is true only for a namespace that is an immediate child of the
    root the census was asked about; the callers use it to scope
    self-exclusion to that root's own namespace domain.  A symlinked entry is
    taint without being followed: a link can leave the store or point back
    into it, and neither is a fragment.  The same rule covers a ``.json``
    entry that is not a regular file -- a directory, fifo, socket or device
    that has displaced a fragment file.  It hides the only document that
    vouches for the mover's staged bytes, so it is unknown ownership, never a
    skip: a caller that deletes would otherwise read the loss as "no owner".
    """

    index = memo if isinstance(memo, CensusIndex) else None
    stamp = None
    if index is not None:
        kept = index.namespace_hit(directory)
        if kept is not None:
            # Unchanged since a listing whose stamp predates it (#992): the
            # same documents, and not one file opened to learn that.
            for mover, document in kept:
                fragments.append((namespace, mover, document, direct))
            return
        stamp = _trusted_directory_stamp(directory)
    found: list[tuple[str, dict[str, object]]] = []
    read: set[str] = set()
    taint_before = len(tainted)
    try:
        entries = sorted(os.scandir(directory), key=lambda entry: entry.name)
    except OSError as exc:
        tainted.append(f"{namespace}: {exc}")
        if index is not None:
            index.remember_namespace(directory, None, [], set(), True)
        return
    for entry in entries:
        if not entry.name.endswith(".json"):
            continue
        try:
            if entry.is_symlink():
                tainted.append(
                    f"{namespace}/{entry.name}: symlink is not a fragment")
                continue
            is_file = entry.is_file()
        except OSError as exc:
            tainted.append(f"{namespace}/{entry.name}: {exc}")
            continue
        if not is_file:
            tainted.append(
                f"{namespace}/{entry.name}: not a regular file")
            continue
        read.add(entry.path)
        document = _read_fragment(Path(entry.path), memo)
        if isinstance(document, str):
            tainted.append(f"{namespace}/{entry.name}: {document}")
            continue
        if str(document["consumer_action_key"]) != namespace:
            # The directory is what attributes this document; a fragment
            # filed under another namespace is malformed ownership.
            tainted.append(
                f"{namespace}/{entry.name}: fragment is filed under another namespace")
            continue
        mover = str(document["mover_action_key"])
        if entry.name != f"{mover}.json":
            # The file name is the mover's identity in the directory, and the
            # egress excludes its own document by the mover it carries.
            # A name that disagrees could hide another owner's copy (or
            # impersonate one): unknown ownership, never self.
            tainted.append(
                f"{namespace}/{entry.name}: fragment names another mover")
            continue
        found.append((mover, document))
        fragments.append((namespace, mover, document, direct))
    if index is not None:
        index.remember_namespace(directory, stamp, found, read,
                                 len(tainted) > taint_before)


class _LevelChild:
    """The two ``os.DirEntry`` fields :func:`_census_level` reads, by name."""

    __slots__ = ("name", "path")

    def __init__(self, directory: Path, name: str) -> None:
        self.name = name
        self.path = os.path.join(directory, name)


def _census_level(directory: Path, *, direct: bool, allow_nested: bool,
                  fragments: list[tuple[str, str, dict[str, object], bool]],
                  tainted: list[str],
                  skip: frozenset[str] = frozenset(),
                  memo: _CensusMemo | None = None) -> None:
    """Classify one directory level of the fragment store.

    One rule for both layouts: reserved bookkeeping is skipped, a
    ``produced-output-fragments`` child is the nested produced namespace, a
    64-character directory is a fragment namespace, and anything else is
    taint.  The traversal is bounded to the two known layouts: the produced
    container is descended **once**, only from the base store
    (``allow_nested``), and a container name inside the produced store, or a
    symlink anywhere, is unknown ownership -- taint, never a walk.  ``skip``
    names children this call must not revisit -- the produced store itself
    when its own parent level is walked for co-owner domains.

    A non-directory where the level expects a namespace is the other half of
    the same rule: a 64-character name, or the ``produced-output-fragments``
    container, standing as a regular file, fifo or socket hides every
    fragment filed below it, so it is taint -- unknown ownership, never a
    skip.  A composed ``<consumer>.map.json`` beside its namespace directory
    (``residency_map.map_path``) is not such a name -- it is nine
    characters longer than a namespace and is the document a reader reads --
    so it keeps the old behaviour: a stray file that names no layout taints
    nothing.
    """

    index = memo if isinstance(memo, CensusIndex) else None
    children = index.level_hit(directory) if index is not None else None
    if children is None:
        stamp = (_trusted_directory_stamp(directory)
                 if index is not None else None)
        try:
            entries = sorted(os.scandir(directory),
                             key=lambda entry: entry.name)
        except OSError as exc:
            tainted.append(f"{directory}: {exc}")
            return
        children = []
        for entry in entries:
            try:
                if entry.is_symlink():
                    kind = "link"
                elif entry.is_dir():
                    kind = "dir"
                else:
                    kind = "other"
            except OSError as exc:
                kind = f"error: {exc}"
            children.append((entry.name, kind))
        if index is not None:
            index.remember_level(directory, stamp, children)
    for name, kind in children:
        # One classification of each child, whether this pass listed the
        # level or reused a listing whose stamp still holds (#992).
        entry = _LevelChild(directory, name)
        if entry.name in skip:
            continue
        if kind == "link":
            tainted.append(
                f"{entry.name}: symlink is not a fragment namespace")
            continue
        if kind.startswith("error"):
            tainted.append(f"{entry.name}: {kind[len('error: '):]}")
            continue
        is_directory = kind == "dir"
        if not is_directory:
            if entry.name == produced_output.OUTPUT_FRAGMENTS_SUBDIR:
                tainted.append(
                    f"{entry.name}: produced namespace is not a directory")
                continue
            if _namespace_shaped(entry.name):
                tainted.append(
                    f"{entry.name}: namespace is not a directory")
            continue
        namespace = entry.name
        if namespace in RESERVED_RESIDENCY_SUBDIRS:
            # Reader pins/retiring marks (``leases/``) and material sidecars
            # (``material/``) live inside the produced store too, because
            # reader_lease resolves them from the supplied residency root.
            continue
        if namespace == produced_output.OUTPUT_FRAGMENTS_SUBDIR:
            if not allow_nested:
                tainted.append(
                    f"{namespace}: nested produced namespace is not a "
                    f"fragment namespace")
                continue
            _census_level(Path(entry.path), direct=False, allow_nested=False,
                          fragments=fragments, tainted=tainted, memo=memo)
            continue
        if not _namespace_shaped(namespace):
            tainted.append(f"{namespace}: unknown residency directory")
            continue
        _census_fragment_directory(Path(entry.path), namespace, direct=direct,
                                   fragments=fragments, tainted=tainted,
                                   memo=memo)


def _fragment_census(root: Path, memo: _CensusMemo | None = None,
                     ) -> tuple[list[tuple[str, str, dict[str, object], bool]],
                                list[str]]:
    """Every fragment under one residency root, and what cannot be read.

    Two layouts share the store.  Legacy flat fragments sit at
    ``<root>/<consumer action key>/<mover>.json``.  Produced-output batches
    file theirs at ``<root>/produced-output-fragments/<batch namespace>/
    <mover>.json``; that namespace is a material namespace, not a queue
    action.

    A walk of the store descends into the produced subdirectory exactly
    once; a walk of the produced store itself -- the root an egress or
    retirement is given for a produced mover -- also walks the flat
    namespaces beside it, because a legacy co-owner may vouch for the same
    physical staged bytes.  Nothing else is a layout: a second container
    name, or a symlink, is taint.  Self-exclusion is the caller's business
    and is scoped to the root walked: only fragments found directly under it
    carry ``direct=True``.

    Returns ``(fragments, tainted)``: each fragment as ``(namespace, mover,
    document, direct)``, each taint a bounded one-line reason.  A directory
    that is neither reserved bookkeeping nor namespace-shaped, a ``.json``
    file that cannot be read or does not validate, a ``.json`` entry that is
    not a regular file, and a namespace-shaped or produced-container name
    that is not a directory, are taint -- never a silent skip, never a crash,
    never followed.  A caller that deletes treats taint as unknown ownership
    and retains.
    """

    fragments: list[tuple[str, str, dict[str, object], bool]] = []
    tainted: list[str] = []
    if root.name == produced_output.OUTPUT_FRAGMENTS_SUBDIR:
        _census_level(root, direct=True, allow_nested=False,
                      fragments=fragments, tainted=tainted, memo=memo)
        _census_level(root.parent, direct=False, allow_nested=False,
                      fragments=fragments, tainted=tainted,
                      skip=frozenset({root.name}), memo=memo)
    else:
        _census_level(root, direct=True, allow_nested=True,
                      fragments=fragments, tainted=tainted, memo=memo)
    return fragments, tainted


def _fragment_owners(root: Path, wanted: set[str], *,
                     except_consumer: str = "",
                     except_mover: str = "",
                     memo: _CensusMemo | None = None,
                     named_by: list[dict[str, object]] | None = None,
                     ) -> tuple[dict[str, set[tuple[str, str]]], list[str]]:
    """Which of ``wanted`` paths are still vouched for, and by whom, in one walk.

    A single scan of every fragment namespace -- never per entry --
    intersecting validated ``stage_path`` strings against ``wanted`` before
    storing.  No metadata walk per foreign entry: the fragment validator
    already guarantees absolute, normalized paths, so string intersection is
    exact and only matches are stored.  The egress keeps its own resolve-based
    containment fence before any unlink.  A fragment that cannot be read or
    validated taints the scan: its paths are unknowable, so nothing may be
    treated as unowned on this pass.  Fail closed, the way an unreadable own
    fragment keeps its tokens.

    The exclusion is scoped to the root being walked: only a fragment filed
    directly under ``root`` -- legacy flat, or the produced store when that
    is the root -- can be this caller's own.  A fragment carrying the same
    key in a nested produced namespace is a *different* owner's copy and
    keeps its protection; "same key" is never automatically self.

    ``named_by``, when given, collects every document that names at least
    one wanted path: the co-owner fragments a verdict relies on (#1056).
    """

    owners: dict[str, set[tuple[str, str]]] = {}
    fragments, tainted = _fragment_census(root, memo)
    for namespace, mover, fragment, direct in fragments:
        if direct and namespace == except_consumer and mover == except_mover:
            continue
        named = memo.paths_of(fragment) if memo is not None else None
        if named is not None:
            # The same exact string intersection as below, over the path
            # set the memo built when it parsed this version (#988).
            shared = named & wanted
            for path in shared:
                owners.setdefault(path, set()).add((namespace, mover))
            if shared and named_by is not None:
                named_by.append(fragment)
            continue
        names_one = False
        for entry in dict(fragment["entries"]).values():
            if not isinstance(entry, Mapping):
                continue
            path = entry.get("stage_path")
            # Validated absolute and normalized, so this comparison is
            # exact with no metadata touch.
            if isinstance(path, str) and path in wanted:
                owners.setdefault(path, set()).add((namespace, mover))
                names_one = True
        if names_one and named_by is not None:
            named_by.append(fragment)
    return owners, tainted


_manifest_layout_cache: dict[tuple[str, str], tuple[str, list[dict[str, object]]]] = {}


def _cached_manifest_layout(cas_root: str, digest: str) -> tuple[str, list[dict[str, object]]] | None:
    """One manifest's mount prefix and entries by content digest, or ``None``.

    Manifests are immutable under their digest.  Only successful loads are
    cached (bounded); a transient miss is re-read next pass rather than
    remembered indefinitely, so a short CAS outage cannot pin every later
    egress into skipping.
    """

    key = (cas_root, digest)
    hit = _manifest_layout_cache.get(key)
    if hit is not None:
        return hit
    try:
        blob = pb.PrismaBuildCAS(Path(cas_root)).blob_path(digest)
        manifest = pb.load_data_manifest(blob)
    except (OSError, ValueError, pb.PrismaBuildError):
        return None
    if not isinstance(manifest, Mapping):
        return None
    entries = prewarm_loop.manifest_read_entries(manifest)
    if not isinstance(entries, list):
        return None
    prefix = manifest.get("mount_prefix")
    if not isinstance(prefix, str) or not prefix.startswith("/"):
        return None
    layout = (prefix, [dict(entry) for entry in entries])
    if len(_manifest_layout_cache) >= 4:
        _manifest_layout_cache.clear()
    _manifest_layout_cache[key] = layout
    return layout


def _produced_hold_verified(item: Mapping, request: Mapping) -> bool:
    """Whether a tier-demand claim is a verified producer reservation.

    Shape alone is not verification: the mutable queue projection could
    name any template id and digest. The sealed request carries the
    authoritative declaration (`params.produced_output_template`,
    validated here through the existing core seam against the request's
    own inputs), and it must equal the item's projected ref exactly --
    same template id and digest. A substituted claim ref, or a request
    whose declaration disagrees or is absent, is not a verified hold
    and stays on the taint path. Callers additionally require the
    sealed command to carry no movement range flags (mover/non-mover
    classification unchanged); no separate receipt protocol exists.
    """

    ref = item.get("produced_output")
    if not isinstance(ref, Mapping):
        return False
    try:
        from prismabuild.produced_output import PRODUCED_OUTPUT_REF_SCHEMA_V1
    except ImportError:
        return False
    if ref.get("schema") != PRODUCED_OUTPUT_REF_SCHEMA_V1:
        return False
    params = request.get("params")
    if not isinstance(params, Mapping):
        return False
    declaration = params.get(pb.PRODUCED_OUTPUT_TEMPLATE_PARAM)
    if not isinstance(declaration, Mapping):
        return False
    inputs = request.get("inputs")
    if not isinstance(inputs, list):
        return False
    try:
        checked = pb.validate_produced_output_declaration(declaration, inputs)
    except (pb.ActionContractError, ValueError, TypeError):
        return False
    return (checked.get("template_id") == ref.get("template_id")
            and checked.get("template_sha256") == ref.get("template_sha256"))


def _claimed_paths(queue: pool.PoolQueue, tier_id: str,
                   cas_root: str | Path | None = None,
                   *, exclude: set[str] | frozenset[str] | None = None,
                   memo: _CensusMemo | None = None,
                   ) -> tuple[set[str], list[str]]:
    """Staged paths a claimed copy may be writing, by sealed range.

    A copy in flight has no fragment yet, so fragments alone cannot attribute
    it -- but its claim already exists, and the claim's sealed request names
    its manifest and its read-order range.  Resolving those through the same
    ``stage_relative`` computation both movers use attributes exactly the
    files the copy can rename into place.  Non-movement claims are skipped,
    never tainting: a claim with no demand on the tier, or one that demands
    only a rate there (the paced produced export's pool fill, #1060), is not
    a copy.  A claim that demands occupancy on the tier is a copy, and one
    whose sealed command carries no range taints unless it is a verified
    producer reservation.  Anything unreadable taints the pass, the same
    fail-closed rule as fragments.

    The CAS root comes from each sealed claim record's own ``cas_root`` where
    present (an explicit override wins for tests); the queue-sibling default
    applies only when no record names one.

    ``exclude`` names claim keys that never count as another publisher --
    the staged-path publication gate passes its own mover key, so a mover
    never defers to itself.

    The egress needs one more distinction -- the mover being evicted's own
    claim against a foreign one -- and reads
    :func:`_claimed_paths_attributed`; this wrapper is the two-value view the
    publication gate and the focused tests already write against.
    """

    paths, tainted, _own = _claimed_paths_attributed(
        queue, tier_id, cas_root, exclude=exclude, memo=memo)
    return paths, tainted


def _claimed_paths_attributed(queue: pool.PoolQueue, tier_id: str,
                              cas_root: str | Path | None = None, *,
                              exclude: set[str] | frozenset[str] | None = None,
                              own_key: str = "",
                              memo: _CensusMemo | None = None,
                              ) -> tuple[set[str], list[str], set[str]]:
    """:func:`_claimed_paths`, attributing one key's paths separately.

    Everything above holds.  ``own_key`` is the key an egress is retiring: its
    claim's derived paths come back as the third element instead of joining
    the competing set, because the evicted mover's own claim is never a
    distinct co-owner (#793).  A live own claim is still a *possible writer*,
    though: it defers the retire rather than sharing it, and the claim stops
    being visible at all once the worker's terminal transition retires it.
    There is deliberately no receipt-based shortcut here.  A move receipt
    carries no immutable attempt identity -- no nonce or scope -- so a
    complete-looking one cannot be told from a previous attempt's while the
    same key is claimed again, and wall-clock stamps are no substitute
    (2026-09-21 root QA).  ``exclude`` keeps its unconditional meaning and is
    checked first.

    With a ``memo``, a range mover's derived stage paths are remembered by
    claim key and CAS root: they come from its sealed request and its data
    manifest, both immutable under their digests.  The claim listing and
    each claim record are still read on every call (#988).
    """

    paths: set[str] = set()
    own_paths: set[str] = set()
    tainted: list[str] = []
    try:
        keys = sorted(path.name[:-len(".json")] if path.name.endswith(".json")
                      else path.name
                      for path in pool._scan(queue.dir(pool.CLAIMED)))
    except OSError as exc:
        return paths, [f"claimed: {exc}"], own_paths
    records: list[tuple[str, dict[str, object]]] = []
    for key in keys:
        try:
            item = pool._read_json(queue.item_path(pool.CLAIMED, key))
        except (OSError, pool.PoolContractError) as exc:
            tainted.append(f"{key[:12]}: {exc}")
            continue
        if item is None:
            continue    # finished between the scan and the read: its
                        # fragment, if it published one, still vouches for it
        if not isinstance(item, dict):
            tainted.append(f"{key[:12]}: unreadable claim record")
            continue
        records.append((key, item))
    default_cas = (str(cas_root) if cas_root is not None
                   else str(queue.root.parent / "cas"))
    for key, item in records:
        if exclude and key in exclude:
            continue    # this publisher's own claim: never another publisher
        own_claim = bool(own_key) and key == own_key
        # No first-record inheritance: each claim resolves its own CAS root --
        # the explicit override wins, else the record's own root, else the
        # queue-sibling default.  A rootless claim among rooted claims reads
        # the default, never another record's root.
        if cas_root is not None:
            own_cas = str(cas_root)
        else:
            root = item.get("cas_root")
            own_cas = (str(root) if isinstance(root, str) and root
                       else default_cas)
        resources = item.get("resources")
        if not isinstance(resources, Mapping):
            # Absent or malformed: a claim without a demand shape cannot
            # establish non-mover.  Every published row seals ``resources``,
            # and no queue transition writes a claim without one, so there is
            # no safe behavior but taint.  (The old code threw mid-scan on
            # ``None`` and skipped the unknown silently.)
            tainted.append(f"{key[:12]}: malformed resources")
            continue
        on_tier = {str(kind).split("@", 1)[0] for kind in resources
                   if "@" in str(kind) and str(kind).split("@", 1)[1] == tier_id}
        if not on_tier:
            continue    # not a movement node on this tier; a consumer is
                        # not a copy
        if on_tier <= pool.TIER_RATE_KINDS:
            # A rate reservation names no bytes (#636, #1060): the paced
            # produced export reserves this tier's pool-side fill and
            # writes under its template's output prefix, never onto the
            # stage.  The pool's publish gate draws the same line -- only
            # occupancy kinds need a range or a working window -- and
            # every copy onto a tier carries the tier's capacity kind
            # (``storage_tiers.residency_demand``, the produced-output
            # mover).  An occupancy or unknown kind still reads as a copy
            # below, so a mover that seals no range still taints.
            continue
        memo_key = (key, own_cas, tier_id)
        remembered = memo.claims.get(memo_key) if memo is not None else None
        if remembered is not None:
            (own_paths if own_claim else paths).update(remembered)
            continue
        try:
            request = pool._read_json(
                Path(own_cas) / "requests" / key[:2] / f"{key}.json")
        except (OSError, pool.PoolContractError) as exc:
            tainted.append(f"{key[:12]}: {exc}")
            continue
        if not isinstance(request, Mapping):
            tainted.append(f"{key[:12]}: unreadable sealed request")
            continue
        params = request.get("params")
        command = params.get("command") if isinstance(params, Mapping) else None
        if not isinstance(command, list):
            # Identified as a mover by its tier demand, but seals no argv:
            # corrupt, not a consumer -- consumers never reach this branch.
            tainted.append(f"{key[:12]}: mover seals no command")
            continue
        if "--range-start-bytes" not in command:
            # A verified produced-output producer reservation, not a copy:
            # the item's projected ref must equal the sealed request's
            # own validated declaration (template id + digest bound to
            # its inputs). A substituted ref, or a request whose
            # declaration disagrees or is absent, stays unknown and
            # taints below. Unknown rows are never skipped, and an
            # active copy's protection is untouched.
            if _produced_hold_verified(item, request):
                continue
            tainted.append(f"{key[:12]}: mover seals no range")
            continue
        try:
            start = command[command.index("--range-start-bytes") + 1]
            end = command[command.index("--range-end-bytes") + 1]
            # Sealed argv bounds are digit strings by schema: no bool, float,
            # or whitespace-tolerant coercion may accept a malformed bound.
            for bound in (start, end):
                if (isinstance(bound, bool) or not isinstance(bound, str)
                        or not bound.isdigit()):
                    raise ValueError(
                        "range bounds must be nonnegative integer strings")
            start, end = int(start), int(end)
            if end < start:
                raise ValueError("range end precedes start")
        except (ValueError, IndexError, TypeError):
            # An identified mover whose exact range cannot be determined must
            # not silently read as unowned.
            tainted.append(f"{key[:12]}: mover seals an invalid range")
            continue
        digest = None
        # The manifest rides on the sealed request's top-level inputs (verified
        # against a live fixture), never under params.
        inputs = request.get("inputs")
        if isinstance(inputs, list):
            for entry in inputs:
                if (isinstance(entry, Mapping)
                        and entry.get("id") == pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID):
                    digest = entry.get("sha256")
        if not isinstance(digest, str) or not digest:
            tainted.append(f"{key[:12]}: sealed request names no data manifest")
            continue
        layout = _cached_manifest_layout(own_cas, digest)
        if layout is None:
            tainted.append(f"{key[:12]}: manifest {digest[:12]} unreadable")
            continue
        mount_prefix, entries = layout
        named_once = paths_named_once(entries)
        try:
            window = prewarm_loop.entries_between(entries, start, end)
        except (ValueError, TypeError) as exc:
            # A window that cannot be cut is an undeterminable range: taint,
            # never unowned.
            tainted.append(f"{key[:12]}: range not cuttable: {exc}")
            continue
        derived: set[str] = set()
        for entry in window:
            path, offset = str(entry["path"]), int(entry["offset"])
            try:
                # Both spellings: a claimed mover may be a frozen plan row
                # of a generation from before range-only naming, still
                # writing the bare name.  A retention census over-retains.
                relatives = {
                    stage_relative(path, offset, int(entry["bytes"]),
                                   mount_prefix=mount_prefix),
                    pre_range_stage_relative(
                        path, offset, int(entry["bytes"]),
                        mount_prefix=mount_prefix,
                        named_once=path in named_once)}
            except ValueError:
                continue
            # Compared against fragment ``stage_path`` values, which join the
            # stage root with this same relative name.
            derived.update(relatives)
        (own_paths if own_claim else paths).update(derived)
        if memo is not None:
            memo.claims[memo_key] = frozenset(derived)
    return paths, tainted, own_paths


def evict(queue: pool.PoolQueue, mover_action_key: str, *,
          consumer_action_key: str, stage_root: str,
          residency_root: str | Path | None = None,
          reason: str = "egress", whole: bool = False) -> dict[str, object]:
    """Delete one mover's staged files and settle its tier tokens.

    ``whole`` makes the eviction all or nothing (#903).  An egress of a passed
    phase may delete part of a range and defer the rest behind a reader,
    because nobody will read the part it deleted.  A range a live consumer
    has not reached yet is different: it will be read, so deleting part of
    it while its tokens and fragment stay would leave it looking staged with
    holes in it, and nothing would copy it again.  With ``whole`` every entry
    is judged under the ownership lock before anything is unlinked, and if
    any entry would defer -- a reader's pin, a promotion's handoff, the
    mover's own live copy -- or cannot be judged, the eviction declines:
    nothing is unlinked, no retiring mark is filed, the tokens and the
    fragment stay, and the receipt names the reason in ``declined``.

    Tokens for deleted bytes return; tokens for bytes staying under a
    co-owner are decharged (#733).

    Idempotent in both halves: a file already gone is counted as gone rather
    than raised on, and ``ResourceLedger.release`` is documented safe to call
    twice.  A second egress of the same range is therefore a no-op receipt, not
    a failure --- which matters, because the tier loop may publish one while a
    sweep is doing the same work.

    Containment reclamation happens between the two locks (#780): after the
    transition lock, which makes the fragment this reads stable, and before
    the ownership lock, because reclamation takes the ownership lock of every
    root its pins name and a second root requested under the first is a
    cycle.  The egress can wait on another root there, but it waits holding
    only this mover's own transition lock, which is strictly less than the
    root it used to hold while waiting.

    Held under the mover's transition lock since #598, because a second party
    can now decide the same range's ownership: an adoption hands these tokens
    to a successor's mover and re-issues the fragment under it.  Read-delete-
    release and transfer-then-drop-the-fragment are each safe alone, and
    interleaved either way one of them acts on half the other's decision --- so
    they exclude each other rather than being ordered.  After the lock this
    sees one of two settled states: the fragment is here and the tokens are
    this key's, or the fragment is gone and so are the tokens, which is the
    no-op above.
    """

    with queue.mover_transition_lock(str(mover_action_key)):
        return _evict_locked(queue, mover_action_key,
                             consumer_action_key=consumer_action_key,
                             stage_root=stage_root,
                             residency_root=residency_root, reason=reason,
                             whole=whole)


def _evict_locked(queue: pool.PoolQueue, mover_action_key: str, *,
                  consumer_action_key: str, stage_root: str,
                  residency_root: str | Path | None = None,
                  reason: str = "egress", whole: bool = False) -> dict[str, object]:
    """:func:`evict`'s body, with the mover's transition lock already held.

    One staged file can have two owners: forward and reverse passes stage the
    same source extent through different movers onto one content-addressed
    name, and two read phases of one v2 plan do the same inside one consumer.
    Deleting on one owner's egress while another owner's fragment still
    vouches for the file leaves a hole behind a live map, so a path another
    live fragment -- or a claimed copy with no fragment yet -- still names is
    kept, and this mover's tokens for those bytes are decharged rather than
    freed, while its own fragment is still dropped (#733).  The last owner
    to leave deletes the file.  The whole check-and-act runs under
    the stage root's ownership lock (taken here, inside the transition lock --
    adoption takes them in the same order), so two concurrent egresses order
    instead of both concluding "unshared", and the tier mint lock is taken
    last, as a leaf, around the settle alone.  Exactly one stage root is held
    at a time: anything that would take a second one -- containment
    reclamation is the only such thing here -- runs before this lock, never
    inside it (#780).
    """

    refusal = stage_root_refusal(queue, stage_root)
    if refusal is not None:
        # Not this queue's stage (#628).  The fragment is not even read: a
        # fragment is authority over which files are this mover's, never over
        # whose stage they sit on.
        return _refused_receipt(tier_id="", stage_root=stage_root,
                                refusal=refusal,
                                mover_action_key=mover_action_key,
                                consumer_action_key=consumer_action_key,
                                reason=reason)
    root = Path(residency_root if residency_root is not None
                else queue.root / pool.RESIDENCY)
    fragment_path = residency_map.fragment_path(
        root, consumer_action_key, mover_action_key)
    entries: dict[str, object] = {}
    tier_id: str | None = None
    errors: list[str] = []
    try:
        with open(fragment_path) as stream:
            checked = residency_map.validate_fragment(json.load(stream))
            entries = dict(checked["entries"])
            tier_value = checked.get("tier_id")
            tier_id = str(tier_value) if isinstance(tier_value, str) else None
    except FileNotFoundError:
        # No fragment at all.  Either the mover never published one -- in which
        # case it staged nothing -- or an earlier egress already removed it.
        # Both mean nothing of this mover's is on the stage, so the tokens come
        # back; holding them would cost the tier its capacity for good.
        entries = {}
    except (OSError, ValueError) as exc:
        # A fragment that exists and cannot be read is the opposite case: its
        # bytes may well still be there and this egress cannot name them.
        # Releasing on that would let the ledger admit a mover onto capacity
        # that is occupied, so the tokens stay and the next sweep retries.
        errors.append(f"{fragment_path.name}: {exc}")
    stage = Path(stage_root)
    auto_reclaimed: list[str] = []
    auto_retained: dict[str, str] = {}
    memo = _CensusMemo()
    fences: dict[str, tuple] | None = None
    census_s = 0.0
    if entries and tier_id is not None:
        # Containment reclamation runs here, holding no stage ownership lock
        # (#780).  ``auto_reclaim`` walks every pin owner and ``release_refs``
        # takes the root each pin names -- root B for a pin filed on another
        # stage -- so called from inside this root's exclusion it is an A->B
        # request, while a second egress holding B walking the same lease tree
        # asks for A.  ``posix_lock.held`` nests on the same path only, so
        # same-root reentrancy does not prevent that cycle.
        #
        # It needs no exclusion of ours to be correct: each release takes its
        # own pin's root lock around its own check-and-act, and the delete
        # decision below is still one atomic unit, because the census it acts
        # on is taken under the lock -- after this, never before it.
        #
        # This census only decides whether to reclaim at all: the same
        # question the in-lock pass asked, asked without the lock.  It is a
        # hint and is allowed to be stale in both directions.  A ref whose
        # containment evidence lands between it and the lock is not freed on
        # this pass; the entry defers and the next sweep retries, which is the
        # direction this node already fails in.
        census_started = time.perf_counter()
        hint, hint_tainted = reader_lease.live_for(
            queue, _wanted_stage_paths(entries), residency_root=root,
            memo=memo.pins)
        if hint or hint_tainted:
            reclaimed = reader_lease.auto_reclaim(queue, residency_root=root)
            auto_reclaimed.extend(reclaimed["released"])
            for ref_id, why in reclaimed["retained"].items():
                auto_retained.setdefault(ref_id, why)
        # The rest of the census, and each entry's containment fence, are
        # hints too (#988).  They run here, without the lock, so the pass
        # under it re-reads only what changed (see :class:`_CensusMemo`)
        # and resolves no path.  Nothing decided here is acted on: every
        # verdict is judged again under the lock against the census taken
        # there.
        _ownership_census(queue, mover_action_key,
                          consumer_action_key=consumer_action_key,
                          stage=stage, tier_id=tier_id, root=root,
                          entries=entries, memo=memo)
        _read_own_material(root, consumer_action_key, mover_action_key, memo)
        fences = _entry_fences(stage, entries)
        census_s = time.perf_counter() - census_started
    parents: set[Path] = set()
    asked = time.perf_counter()
    with queue.stage_ownership_lock(str(stage)):
        granted = time.perf_counter()
        receipt = _evict_owned(queue, mover_action_key,
                               consumer_action_key=consumer_action_key,
                               stage=stage, tier_id=tier_id, root=root,
                               fragment_path=fragment_path, entries=entries,
                               errors=errors, reason=reason,
                               auto_reclaimed=auto_reclaimed,
                               auto_retained=auto_retained, whole=whole,
                               memo=memo, fences=fences, prune_after=parents)
    released = time.perf_counter()
    # Empty directories go after the lock.  A publisher's rename lands in a
    # directory that already holds its temporary, so ``rmdir`` cannot take
    # it from under the rename; its ``mkdir`` runs outside the lock today,
    # so the window between that and its temporary is no wider for this.
    pruned_started = time.perf_counter()
    for parent in sorted(parents, key=lambda one: len(one.parts), reverse=True):
        _prune_empty(parent, stage)
    receipt.update(_hold_record(
        lock_wait_s=granted - asked, lock_held_s=released - granted,
        census_s=census_s, prune_s=time.perf_counter() - pruned_started))
    return receipt


#: The lock-scope fields an egress receipt carries since #988, in the order
#: an event copies them: the queue for the lock, the hold, the entries the
#: hold judged, the census before the lock, the census under it, the
#: unlinks under it and the prune after it, and the census documents the
#: hold parsed and reused.
LOCK_SCOPE_FIELDS = ("lock_wait_s", "lock_held_s", "entries_judged",
                     "census_s", "census_validate_s", "unlink_s", "prune_s",
                     "locked_parses", "locked_reuses")


def lock_scope(receipt: Mapping[str, object]) -> dict[str, object]:
    """The :data:`LOCK_SCOPE_FIELDS` one egress receipt carries, for an event."""

    return {field: receipt[field] for field in LOCK_SCOPE_FIELDS
            if field in receipt}


def _hold_record(*, lock_wait_s: float, lock_held_s: float,
                 census_s: float, prune_s: float = 0.0) -> dict[str, object]:
    """The lock-scope fields an egress receipt carries (#988).

    ``lock_held_s`` is the one stage ownership hold this call took, from the
    grant to the release; ``lock_wait_s`` is the time it queued for it.
    ``census_s`` is the census taken before the lock, as a hint, and
    ``prune_s`` the empty-directory prune after it.  The census under the
    lock (``census_validate_s``), the unlinks (``unlink_s``) and
    ``entries_judged`` come from the pass under the lock itself.
    """

    return {"lock_wait_s": round(lock_wait_s, 6),
            "lock_held_s": round(lock_held_s, 6),
            "census_s": round(census_s, 6),
            "prune_s": round(prune_s, 6)}


def _entry_fences(stage: Path, entries: Mapping[str, object],
                  ) -> dict[str, tuple]:
    """Each entry's containment fence, computed once, outside the lock (#988).

    ``("ok", path, norm, resolved, relative)``: the entry's path, its
    normalized spelling, its resolved spelling and its name relative to the
    resolved stage root.  ``("error", message)`` for a path that resolves
    outside the stage or cannot be resolved.

    The fence is the same ``resolve`` the judge made under the lock before
    #988, taken earlier.  The stage tree's directories are made by movers
    (``mkdir``) and removed by egresses (``rmdir``); nothing in PrismaBuild
    makes a symlink in it.  So no PrismaBuild actor can change what a path
    resolves to between this call and the lock, and nothing outside
    PrismaBuild takes the lock, so holding it never made the fence atomic
    with the unlink against one.  The stage root itself is resolved once
    here instead of twice per entry.
    """

    fences: dict[str, tuple] = {}
    try:
        stage_resolved = stage.resolve()
    except OSError as exc:
        return {str(key): ("error", f"{key}: {exc}") for key in entries}
    for key, entry in entries.items():
        if not isinstance(entry, Mapping):
            continue
        path = Path(str(entry["stage_path"]))
        try:
            resolved = path.resolve()
        except OSError as exc:
            fences[str(key)] = ("error", f"{key}: {exc}")
            continue
        if stage_resolved not in resolved.parents:
            # A fragment naming a path outside the stage is not a thing to
            # act on: the writer validated it, so this is corruption or
            # someone else's file.
            fences[str(key)] = ("error", f"{key}: outside {stage}")
            continue
        fences[str(key)] = ("ok", path, os.path.normpath(str(path)),
                            os.path.normpath(str(resolved)),
                            str(resolved.relative_to(stage_resolved)))
    return fences


def _ownership_census(queue: pool.PoolQueue, mover_action_key: str, *,
                      consumer_action_key: str, stage: Path, tier_id: str,
                      root: Path, entries: Mapping[str, object],
                      memo: _CensusMemo | None = None) -> dict[str, object]:
    """The four ownership censuses one egress decides on, in snapshot order.

    Claimed movers first, then fragment directories, then pins, then
    promotion sources: the order :func:`_evict_owned` argues from.  Called
    twice per egress since #988 -- once before the lock as a hint that fills
    ``memo``, and once under it, where the result is acted on.
    ``co_owner_documents`` are the fragments that name any of ``entries``'
    paths, for a skip checkpoint to fence on (#1056).
    """

    wanted = _wanted_stage_paths(dict(entries))
    claimed, claimed_taint, own_claimed = _claimed_paths_attributed(
        queue, tier_id, own_key=mover_action_key, memo=memo)
    named_by: list[dict[str, object]] = []
    owners, fragment_taint = _fragment_owners(
        root, wanted,
        except_consumer=consumer_action_key,
        except_mover=mover_action_key, memo=memo, named_by=named_by)
    pins, pin_taint = reader_lease.live_for(
        queue, wanted, residency_root=root,
        memo=memo.pins if memo is not None else None)
    source_paths, source_taint = _claimed_source_paths(queue, stage, memo=memo)
    return {"claimed": claimed, "own_claimed": own_claimed, "owners": owners,
            "co_owner_documents": named_by,
            "pins": pins, "source_paths": source_paths,
            "tainted": fragment_taint + claimed_taint + pin_taint + source_taint}


def _claimed_source_paths(queue: pool.PoolQueue, stage: Path,
                          cas_root: str | Path | None = None,
                          memo: _CensusMemo | None = None,
                          ) -> tuple[set[str], list[str]]:
    """Stage paths a live RAM promotion may be reading, by sealed source leg.

    The promotion copies stage -> ram, so the stage egress must treat the
    promotion's *source* window the way it treats a stage mover's
    destination: a pending copy handoff that blocks eviction until its ram
    fragment lands.  Only ram-tier mover claims are read (a stage mover's
    destinations are `_claimed_paths`' job); the sealed `--source-stage-root`
    decides which stage this attribution joins, resolved before comparing.
    Anything unreadable taints the pass, the same fail-closed rule as
    fragments and destination claims.  With a ``memo``, a promotion's derived
    source paths are remembered by claim key, as
    :func:`_claimed_paths_attributed` remembers a mover's (#988).
    """

    try:
        stage_real = os.path.realpath(stage)
    except OSError as exc:
        return set(), [f"promotion-source: {exc}"]
    paths: set[str] = set()
    tainted: list[str] = []
    try:
        keys = sorted(path.name[:-len(".json")] if path.name.endswith(".json")
                      else path.name
                      for path in pool._scan(queue.dir(pool.CLAIMED)))
    except OSError as exc:
        return paths, [f"claimed: {exc}"]
    records: list[tuple[str, dict[str, object]]] = []
    for key in keys:
        try:
            item = pool._read_json(queue.item_path(pool.CLAIMED, key))
        except (OSError, pool.PoolContractError) as exc:
            tainted.append(f"{key[:12]}: {exc}")
            continue
        if item is None:
            continue
        if not isinstance(item, dict):
            tainted.append(f"{key[:12]}: unreadable claim record")
            continue
        records.append((key, item))
    default_cas = (str(cas_root) if cas_root is not None
                   else str(queue.root.parent / "cas"))
    for key, item in records:
        if cas_root is not None:
            own_cas = str(cas_root)
        else:
            root = item.get("cas_root")
            own_cas = (str(root) if isinstance(root, str) and root
                       else default_cas)
        resources = item.get("resources")
        if not isinstance(resources, Mapping):
            tainted.append(f"{key[:12]}: malformed resources")
            continue
        kinds = {str(kind).split("@", 1)[1] for kind in resources
                 if "@" in str(kind)}
        if not any(kind.startswith(storage_tiers.RAM_TIER_PREFIX)
                   for kind in kinds):
            continue    # not a promotion; stage destinations are elsewhere
        memo_key = (key, own_cas, stage_real)
        remembered = memo.sources.get(memo_key) if memo is not None else None
        if remembered is not None:
            paths.update(remembered)
            continue
        try:
            request = pool._read_json(
                Path(own_cas) / "requests" / key[:2] / f"{key}.json")
        except (OSError, pool.PoolContractError) as exc:
            tainted.append(f"{key[:12]}: {exc}")
            continue
        if not isinstance(request, Mapping):
            tainted.append(f"{key[:12]}: unreadable sealed request")
            continue
        params = request.get("params")
        command = params.get("command") if isinstance(params, Mapping) else None
        if not isinstance(command, list):
            tainted.append(f"{key[:12]}: promotion seals no command")
            continue
        try:
            source_root = command[command.index("--source-stage-root") + 1]
            start = command[command.index("--range-start-bytes") + 1]
            end = command[command.index("--range-end-bytes") + 1]
            if (not isinstance(source_root, str) or not source_root):
                raise ValueError("source stage root must be a path")
            for bound in (start, end):
                if (isinstance(bound, bool) or not isinstance(bound, str)
                        or not bound.isdigit()):
                    raise ValueError(
                        "range bounds must be nonnegative integer strings")
            start, end = int(start), int(end)
            if end < start:
                raise ValueError("range end precedes start")
        except (ValueError, IndexError, TypeError):
            tainted.append(f"{key[:12]}: promotion seals an invalid source range")
            continue
        try:
            if os.path.realpath(source_root) != stage_real:
                if memo is not None:
                    memo.sources[memo_key] = frozenset()
                continue    # another stage's source leg, not this egress
        except OSError as exc:
            tainted.append(f"{key[:12]}: {exc}")
            continue
        digest = None
        inputs = request.get("inputs")
        if isinstance(inputs, list):
            for entry in inputs:
                if (isinstance(entry, Mapping)
                        and entry.get("id") == pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID):
                    digest = entry.get("sha256")
        if not isinstance(digest, str) or not digest:
            tainted.append(f"{key[:12]}: sealed request names no data manifest")
            continue
        layout = _cached_manifest_layout(own_cas, digest)
        if layout is None:
            tainted.append(f"{key[:12]}: manifest {digest[:12]} unreadable")
            continue
        mount_prefix, entries = layout
        named_once = paths_named_once(entries)
        try:
            window = prewarm_loop.entries_between(entries, start, end)
        except (ValueError, TypeError) as exc:
            tainted.append(f"{key[:12]}: range not cuttable: {exc}")
            continue
        derived: set[str] = set()
        for entry in window:
            path, offset = str(entry["path"]), int(entry["offset"])
            try:
                # Both spellings, as in :func:`_claimed_paths_attributed`: a
                # promotion sealed before range-only naming reads the bare
                # name its stage leg wrote.
                relatives = {
                    stage_relative(path, offset, int(entry["bytes"]),
                                   mount_prefix=mount_prefix),
                    pre_range_stage_relative(
                        path, offset, int(entry["bytes"]),
                        mount_prefix=mount_prefix,
                        named_once=path in named_once)}
            except ValueError:
                continue
            for relative in relatives:
                derived.add(os.path.normpath(os.path.join(stage_real, relative)))
        paths.update(derived)
        if memo is not None:
            memo.sources[memo_key] = frozenset(derived)
    return paths, tainted


def _wanted_stage_paths(entries: dict[str, object]) -> set[str]:
    """The staged paths one fragment's entries name, normalized.

    The same set the pin census is asked about inside the ownership lock and
    outside it, so the hint that decides whether to reclaim and the census
    that decides what to delete are asking about the same bytes.
    """

    return {os.path.normpath(str(entry.get("stage_path", "")))
            for entry in entries.values() if isinstance(entry, Mapping)}


def _evict_owned(queue: pool.PoolQueue, mover_action_key: str, *,
                 consumer_action_key: str, stage: Path, tier_id: str | None,
                 root: Path, fragment_path: Path,
                 entries: dict[str, object], errors: list[str],
                 reason: str, auto_reclaimed: list[str],
                 auto_retained: dict[str, str],
                 whole: bool = False,
                 memo: _CensusMemo | None = None,
                 fences: Mapping[str, tuple] | None = None,
                 prune_after: set[Path] | None = None) -> dict[str, object]:
    """Unlink what is exclusively this mover's, under the ownership lock.

    ``auto_reclaimed``/``auto_retained`` are what containment reclamation did
    before this lock was taken (#780); nothing here reclaims, because
    reclamation takes other roots' locks and this one is already held.

    The mover being retired's own live claim is never a distinct co-owner
    (#793).  It is not settled on its receipt either: a move receipt carries
    no immutable attempt identity, so a complete-looking one cannot be told
    from a previous attempt's while the same key is claimed again (2026-09-21
    root QA).  A live own claim therefore defers the retire -- file, fragment,
    material and charge stay -- and the claim stops being visible at all once
    the worker's terminal transition retires it; the next sweep then deletes
    and releases exactly once.  Foreign claims keep the shared skip unchanged.

    What runs here is the act, not the census (#988).  ``memo`` carries the
    censuses :func:`_evict_locked` took before the lock, so the census taken
    here re-lists every directory and re-opens every document but re-parses
    only what changed.  ``fences`` carries each entry's containment fence,
    resolved before the lock.  ``prune_after`` collects the directories the
    unlinks may have emptied, for the caller to prune after the lock.  A
    caller that passes none of them (``prune_stale_mentions``) gets the
    same verdicts, computed here.
    """

    deleted = missing = shared = deferred = 0
    validate_started = time.perf_counter()
    parse_counts = memo.counts() if memo is not None else None
    bytes_deleted = bytes_shared = bytes_gone = 0
    shared_with: list[str] = []
    live_pins: list[str] = []
    deferred_handoffs: list[str] = []
    own_deferred = False
    own_claimed: set[str] = set()
    retiring_written = False
    if entries and tier_id is not None:
        # Snapshot order is the argument: claimed movers first, then fragment
        # dirs, then pins.  A claim exists before its copy starts (the start
        # gate), a fragment exists before its claim is gone (publication
        # precedes the terminal marking), and a pin is filed under the
        # ownership lock before its first read -- so one view in this order
        # covers a publisher, a reader, or a promotion in either transition.
        # Pins join the same snapshot (same lock) rather than a second
        # unlocked check: delete is blocked by ANY current ref, and the
        # check-and-act is one atomic unit with the unlink below.
        census = _ownership_census(
            queue, mover_action_key, consumer_action_key=consumer_action_key,
            stage=stage, tier_id=tier_id, root=root, entries=entries,
            memo=memo)
        claimed = census["claimed"]
        own_claimed = census["own_claimed"]
        owners = census["owners"]
        pins = census["pins"]
        source_paths = census["source_paths"]
        tainted = census["tainted"]
        if tainted:
            # Ownership is uncertain: behave like the unreadable-fragment
            # case -- nothing is unlinked, no tokens come back, the receipt
            # says so and the next sweep retries.
            errors.extend(f"ownership uncertain: {item}" for item in tainted)
            owners, claimed = {}, set()
            pins, source_paths = {}, set()
            own_claimed = set()
            blind = True
        else:
            blind = False
        own_generation: str | None = None
        if not blind:
            # The pins above are the post-reclamation census: refs whose
            # attempts are provably contained (terminal broker telemetry plus
            # broker-persisted proof) were retired before this lock was taken,
            # so ordinary completion, crash/withdrawal cleanup and old-attempt
            # drains free their pins without an operator.  Whatever this
            # snapshot still shows is a genuinely live reader, and only that
            # defers.
            #
            # The retiring mark this deferral may file binds the material
            # generation, never the path: without a sidecar the generation
            # is unknowable, so a pinned legacy range taints instead of
            # filing a mark that could wedge the path's future generations.
            material = _read_own_material(
                root, consumer_action_key, mover_action_key, memo)
            if isinstance(material, dict):
                own_generation = str(material.get("generation") or "")
            elif material is not None:
                errors.append(
                    f"ownership uncertain: material unreadable for "
                    f"{mover_action_key[:12]}")
                owners, claimed = {}, set()
                pins, source_paths = {}, set()
                blind = True
    else:
        owners, claimed = {}, set()
        pins, source_paths = {}, set()
        own_generation = None
        blind = False
    census_validate_s = time.perf_counter() - validate_started
    locked_parses = _locked_parse_record(memo, parse_counts)
    if fences is None and entries and not blind:
        # No fences from before the lock: resolve them here, as before #988.
        fences = _entry_fences(stage, entries)

    def judge(key: str, entry: Mapping[str, object]) -> tuple[str, object]:
        """One entry's verdict under this lock, with nothing done yet.

        ``("error", message)``, ``("handoff", pins)``, ``("own", None)``,
        ``("shared", (co_owners, in_flight))``, ``("pinned", pins)`` or
        ``("unlink", path)``.  Judged separately from acting so an
        all-or-nothing eviction (``whole``, #903) can see every entry's
        verdict before the first unlink; the ordinary pass acts on the same
        verdicts in the same order, under the same lock.
        """

        # The sharing check compares validated strings (exact, no metadata);
        # the resolve is the containment fence before any unlink, taken
        # once per entry by :func:`_entry_fences` (#988).
        assert fences is not None
        fence = fences.get(str(key))
        if fence is None:
            return ("error", f"{key}: no containment fence")
        if fence[0] == "error":
            return ("error", fence[1])
        _ok, path, norm, resolved_norm, relative = fence
        pinned = pins.get(norm, [])
        if resolved_norm in source_paths:
            # A live promotion is reading this source leg into RAM: a pending
            # copy handoff, deferred exactly like a live pin.  The file, this
            # mover's fragment, its material sidecar and its full occupancy
            # charge all stay until the handoff ends -- the promotion's ram
            # fragment names another tier and path and can never prove this
            # SSD incarnation, so dropping the same-path proof here is what
            # left an unprovable surviving file behind.
            #
            # This check precedes the co-owner/in-flight skip.  Another
            # consumer's same-path fragment proves the bytes for a general
            # stage publisher, but a promotion resolves its source cover in
            # its own consumer/manifest namespace (ram_promote's coverage
            # loop and ``reader_lease.acquire``), so a co-owner cannot stand
            # in for this owner's pending proof acquisition.  The handoff
            # wins while it lives; the ordinary shared decharge or last-owner
            # deletion settles on the retry after the claim is gone.
            #
            # No retiring mark is filed while any handoff in this mover's
            # document is deferred.  A mark closes one material generation to
            # new acquires, and ``ram_promote`` takes its proof-only cover
            # through ``reader_lease.acquire`` *after* its claim row exists,
            # so a mark filed here would refuse the very handoff this defers
            # for.  The mark is per mover, not per entry, so a pinned entry
            # on the same mover waits for the handoff to end before its own
            # mark is filed; deleting stays safe meanwhile because every pass
            # re-reads claims and pins under this same ownership lock.
            return ("handoff", pinned)
        if relative in own_claimed:
            # This mover's own copy is still live: its claim is not a
            # distinct co-owner, so this is a deferral, not a shared skip.
            # The file, this mover's fragment, its material and its full
            # charge stay, and the next sweep retries once the worker's
            # terminal transition has retired the claim.  Sharing here would
            # decharge this mover's own duplicate and drop its only vouch
            # while the bytes stayed behind nothing (#793).
            return ("own", None)
        co_owners = sorted(owners.get(norm, set()))
        in_flight = relative in claimed
        if co_owners or in_flight:
            # Another live fragment vouches for these bytes, or a claimed
            # copy is about to land them: keep the file, drop only this
            # mover's own vouching below.  The last owner to leave deletes.
            return ("shared", (co_owners, in_flight))
        if pinned:
            # A live reader holds these bytes (open FD, prefetch, mmap, or a
            # promotion source pin): defer, mark retiring for this material
            # generation, keep the file, the fragment and the charge.  The
            # next sweep deletes after the last release.
            if not own_generation:
                return ("error", f"{key}: pinned but material unqualifiable")
            return ("pinned", pinned)
        return ("unlink", path)

    verdicts: dict[str, tuple[str, object]] = (
        {} if blind else {key: judge(key, entry)
                          for key, entry in entries.items()})
    entries_judged = len(verdicts)
    declined: list[str] = []
    if whole:
        # All or nothing (#903): a range a live consumer will still read is
        # either given back whole or kept whole.  Any entry that would defer
        # or cannot be judged declines the eviction before anything moves --
        # no unlink, no retiring mark, the tokens and the fragment held --
        # and the pins that caused it are named so the caller can say why.
        declined = sorted({verdict for verdict, _detail in verdicts.values()
                           if verdict in ("error", "handoff", "own", "pinned")})
        if blind or errors:
            declined = sorted(set(declined) | {"ownership-uncertain"})
        if declined:
            for verdict, detail in verdicts.values():
                if verdict in ("handoff", "pinned"):
                    live_pins.extend(detail)              # type: ignore[arg-type]
            verdicts = {}
    unlink_started = time.perf_counter()
    for key, (verdict, detail) in verdicts.items():
        entry = entries[key]
        if verdict == "error":
            errors.append(str(detail))
            continue
        if verdict == "handoff":
            deferred += 1
            deferred_handoffs.append("promotion-handoff")
            live_pins.extend(detail)                      # type: ignore[arg-type]
            continue
        if verdict == "own":
            deferred += 1
            own_deferred = True
            continue
        if verdict == "shared":
            co_owners, in_flight = detail                 # type: ignore[misc]
            shared += 1
            shared_with.extend(
                f"{consumer[:12]}/{mover[:12]}" for consumer, mover in co_owners)
            if in_flight:
                shared_with.append("in-flight-copy")
            bytes_shared += int(entry["bytes"])
            continue
        if verdict == "pinned":
            deferred += 1
            live_pins.extend(detail)                      # type: ignore[arg-type]
            continue
        path = detail                                     # type: ignore[assignment]
        try:
            os.unlink(path)
        except FileNotFoundError:
            missing += 1
            bytes_gone += int(entry["bytes"])
            continue
        except OSError as exc:
            errors.append(f"{key}: {exc}")
            continue
        deleted += 1
        bytes_deleted += int(entry["bytes"])
        if prune_after is not None:
            prune_after.add(path.parent)
        else:
            _prune_empty(path.parent, stage)
    unlink_s = time.perf_counter() - unlink_started

    released = decharged = 0
    if (deferred and not deferred_handoffs and not errors and own_generation
            and not own_deferred):
        # A handoff-deferred pass files nothing: closing this generation
        # would refuse the promotion's own cover acquire and strand the
        # handoff holding these bytes.  The next pass files the ordinary mark
        # once no handoff remains and only readers do; a mark already on disk
        # is preserved, never cleared by a deferral.  An own-copy deferral
        # files nothing either, for the same shape of reason: the live copy
        # is republishing this material and a mark would close the generation
        # it is producing.
        reader_lease.write_retiring(
            reader_lease.leases_root(queue, root),
            consumer_action_key=consumer_action_key,
            mover_action_key=mover_action_key, generation=own_generation)
        retiring_written = True
    # Retiring retains the charge until the actual delete with the last live
    # ref already absent; release-before-reclaim stays forbidden.
    # A shared skip is not an error, but its tokens no longer come back as
    # free: the bytes live on under another accounted owner (same lock, so
    # the last owner cannot disappear between the check and the act), so
    # the duplicate ownership is decharged (#733) while this mover's own
    # vouching is dropped.  Ledger release is idempotent: no double-free.
    released = decharged = 0
    if errors or deferred or declined:
        pass
    elif tier_id is None:
        # No fragment at all: nothing is known shared, so every token comes
        # back exactly as before -- holding them would cost the tier its
        # capacity for good.
        released = queue.release_tier_reservations(mover_action_key)
    else:
        # Freed share: whole GiB actually leaving the stage now (deleted
        # here).  Floor, not ceil: a 1-token holder with 0.5 GiB deleted
        # and 0.5 GiB shared must not free a whole token for half a GiB of
        # new room.  Already-gone files count here too, and that is still
        # single-count: bytes gone before discovery are already inside the
        # minted writable (and were inside landed while a complete holder
        # stood for them), bytes gone after it are not, and never-landed
        # bytes moved nothing at all -- while every retry caps both shares
        # at the still-held remainder, so no pass frees twice.
        # Decharged share: enough to cover every staying byte (ceil), so
        # the duplicate can never leak into free through a fraction.
        # Anything left over (slack between demand and ceil, unknown
        # kinds, rates which are not byte-backed) returns, as before.
        # The mint lock is the leaf here (transition -> ownership ->
        # mint; nothing takes it in the other order), so a stale-snapshot
        # mint cannot slip between the decharge and the fragment drop.
        occupancy = storage_tiers.capacity_kind_of(tier_id)
        held = queue.tier_ledger(tier_id).holder_tokens(mover_action_key)
        destroy: dict[str, int] = {}
        free: dict[str, int] = {}
        for kind, count in held.items():
            if kind in pool.TIER_RATE_KINDS or kind != occupancy:
                free[kind] = count
                continue
            freed = min(count, _tokens_for_newly_free_bytes(
                bytes_deleted + bytes_gone))
            shared_part = min(count - freed, _tokens_for_egressed_bytes(
                bytes_shared))
            if shared_part:
                destroy[kind] = shared_part
            free[kind] = count - shared_part
        with queue.tier_mint_lock(tier_id):
            outcome = queue.release_tier_holder_for_egress(
                tier_id, mover_action_key, destroy=destroy, free=free)
        decharged = sum(outcome["destroyed"].values())
        released = sum(outcome["released"].values())
        shortfall = outcome["shortfall"]
        if shortfall:
            # A decharge that failed partway keeps its tokens and fails
            # loudly: freeing the duplicate it was meant to destroy would
            # reopen the phantom, and dropping the fragment would lose the
            # retry.  The next sweep converges.
            assert isinstance(shortfall, dict)
            errors.append("decharge-incomplete: %s" % (sorted(
                "%s=%d" % item for item in shortfall.items()),))
        # Movers hold on one tier; sweep any other tier the old path freed
        # -- but never this one, where a destroy shortfall above is still
        # retained and must stay held.
        for other_tier in queue.tier_ids():
            if other_tier == tier_id:
                continue
            try:
                released += queue.tier_ledger(other_tier).release(
                    mover_action_key)
            except (OSError, pool.PoolContractError):
                continue
    if not errors and not deferred and not declined:
        fragment_path.unlink(missing_ok=True)
        reader_lease.clear_retiring(
            reader_lease.leases_root(queue, root),
            consumer_action_key=consumer_action_key,
            mover_action_key=mover_action_key)
        try:
            reader_lease.material_path(
                root, consumer_action_key,
                mover_action_key).unlink(missing_ok=True)
        except OSError:
            pass
    return {
        "schema": pool.POOL_EGRESS_SCHEMA_V1,
        "action_key": mover_action_key,
        "consumer_action_key": consumer_action_key,
        "stage_root": str(stage),
        "reason": reason,
        "entries_deleted": deleted,
        "entries_already_gone": missing,
        "entries_shared": shared,
        "shared_with": sorted(set(shared_with))[:SHARED_WITH_LIMIT],
        "entries_deferred": deferred,
        "live_pins": sorted(set(live_pins)),
        "deferred_handoffs": sorted(set(deferred_handoffs)),
        # An own-copy deferral names itself, so the retry loop can tell a
        # claim that was still writing from a reader pin or a promotion.
        "deferred_own": ["own-copy-in-flight"] if own_deferred else [],
        "auto_reclaimed": sorted(set(auto_reclaimed)),
        "auto_retained": dict(sorted(auto_retained.items())),
        "retiring": retiring_written,
        "bytes_deleted": bytes_deleted,
        "bytes_shared": bytes_shared,
        "tokens_released": released,
        "tokens_decharged": decharged,
        # Errors -- or a deferral -- mean the stage still holds bytes, so
        # the tokens stay held: releasing them would let the ledger admit a
        # mover onto capacity that is not there.  The receipt says so and
        # the next sweep retries.  A shared skip is not an error, but its
        # tokens no longer come back as free either: the bytes live on
        # under another owner, so the duplicate ownership is decharged
        # (#733) and only this mover's own vouching is dropped.  A deferred
        # handoff keeps this mover's own vouching too: the promotion's ram
        # fragment cannot prove the SSD incarnation it read (#768).
        "complete": not errors and not deferred and not declined,
        "errors": errors,
        "host": socket.gethostname(),
        "unix": time.time(),
        # Only an all-or-nothing eviction declines (#903), and only its
        # receipt names the key: the egress receipt keeps its shape.
        **({"declined": declined} if whole else {}),
        # What the pass under the lock cost (#988): the entries it judged,
        # the census it re-took there, the documents that census parsed
        # and reused, and its unlinks.  The caller adds the hold itself and
        # the census it took before the lock.
        "entries_judged": entries_judged,
        "census_validate_s": round(census_validate_s, 6),
        **locked_parses,
        "unlink_s": round(unlink_s, 6),
    }


_skip_checkpoints: dict[tuple, dict[str, object]] = {}
_skip_checkpoints_lock = threading.Lock()
_skip_checkpoint_usage = {"fences": 0, "bytes": 0}
#: The owners the dead-owner sweep discovered on its latest pass, as
#: checkpoint keys, and how many checkpoints that allows
#: (:func:`_retain_skip_checkpoints`).  Empty until a sweep has run: only an
#: owner a sweep discovered can be skipped by one.
_skip_checkpoint_scope: dict[str, object] = {"keys": frozenset(), "capacity": 0}


def reset_skip_checkpoints() -> None:
    """Forget every skip checkpoint (tests, and callers that want a reset)."""

    with _skip_checkpoints_lock:
        _skip_checkpoints.clear()
        _skip_checkpoint_usage["fences"] = 0
        _skip_checkpoint_usage["bytes"] = 0
        _skip_checkpoint_scope["keys"] = frozenset()
        _skip_checkpoint_scope["capacity"] = 0


def skip_checkpoint_usage() -> dict[str, int]:
    """How many checkpoints, fences and fence path bytes the cache holds."""

    with _skip_checkpoints_lock:
        return {"checkpoints": len(_skip_checkpoints),
                "capacity": int(_skip_checkpoint_scope["capacity"]),
                **_skip_checkpoint_usage}


def _skip_checkpoint_capacity(discovered: frozenset) -> int:
    """How many checkpoints a sweep that discovered ``discovered`` may keep.

    One per discovered owner (#1056).  Not a literal: the owners are the
    fragments on disk the sweep's census found, and each checkpoint's fences
    are bounded by its own fragment's entries and the co-owner fragments
    that name them.
    """

    return len(discovered)


def _retain_skip_checkpoints(discovered) -> None:
    """Keep only the checkpoints of the owners a sweep just discovered.

    The dead-owner sweep calls this once its discovery completes, before it
    consults any checkpoint.  An owner it no longer discovers -- evicted,
    adopted, its fragment gone -- loses its checkpoint here, so what the
    cache holds follows what is on disk, and the capacity is what this
    discovery allows.
    """

    keys = frozenset(discovered)
    with _skip_checkpoints_lock:
        for key in [key for key in _skip_checkpoints if key not in keys]:
            _forget_checkpoint_locked(key)
        _skip_checkpoint_scope["keys"] = keys
        _skip_checkpoint_scope["capacity"] = _skip_checkpoint_capacity(keys)


def _path_version(path: Path | str) -> tuple[int, int, int, int, int] | None:
    """Change evidence for one regular metadata file, or ``None``."""

    try:
        info = os.lstat(path)
    except OSError:
        return None
    if not statmod.S_ISREG(info.st_mode):
        return None
    return _metadata_version(info)


def _directory_version(path: Path | str) -> tuple[int, int, int, int] | None:
    """Change evidence for one immediate parent directory, or ``None``.

    ``lstat`` says what the name is, so a symlink or anything else is never a
    directory the skip cache may stand on.  Device and inode catch a
    replacement of the directory itself (an ancestor rename resolves to a
    fresh inode), mtime catches a rename into or out of it, and ctime catches
    a metadata replacement that leaves mtime alone.
    """

    try:
        info = os.lstat(path)
    except OSError:
        return None
    if not statmod.S_ISDIR(info.st_mode):
        return None
    return (info.st_dev, info.st_ino, info.st_mtime_ns,
            int(getattr(info, "st_ctime_ns", 0)))


def _skip_checkpoint_key(queue: pool.PoolQueue, root: Path, stage: Path,
                         tier_id: str, consumer_action_key: str,
                         mover_action_key: str) -> tuple:
    """The cache key: both roots and the owner identity, never just the key.

    Two queues, two residency roots or two stage roots can name the same
    action key; a checkpoint is a statement about one root's files only, so
    the roots travel in the key.
    """

    return (str(queue.root), str(root), str(stage), tier_id,
            consumer_action_key, mover_action_key)


def _forget_checkpoint_locked(key: tuple) -> None:
    record = _skip_checkpoints.pop(key, None)
    if record is not None:
        _skip_checkpoint_usage["fences"] -= int(record["fence_count"])
        _skip_checkpoint_usage["bytes"] -= int(record["bytes"])


def _co_owner_fences(memo: _CensusMemo,
                     documents: list[dict[str, object]],
                     ) -> dict[str, tuple] | None:
    """The file version of every co-owner fragment a verdict relied on.

    Each is the version the census under the lock read the document at
    (:meth:`_CensusMemo.version_of`), never a fresh sample, so a co-owner
    removed or rewritten after that read is seen by the next pass.  ``None``
    when any document's version is unknown: then nothing may be cached.
    """

    fences: dict[str, tuple] = {}
    for document in documents:
        seen = memo.version_of(document)
        if seen is None:
            return None
        fences[seen[0]] = seen[1]
    return fences


def _install_skip_checkpoint(key: tuple, fragment_version, material_version,
                             stamps: dict[str, tuple[int, int, int, int]],
                             documents: dict[str, tuple] | None = None,
                             ) -> bool:
    """Install the EXACT verified versions of one owner with nothing to act on.

    The caller passes the trusted stamps it sampled before its scan
    (:func:`stage_move._trusted_directory_stamp`, #1062) and proved equal to
    a sample after it, and the co-owner fragment versions its census read
    (``documents``, which ``None`` refuses); nothing is sampled again here, so
    a rename that lands between the scan and this call can never be blessed
    as clean -- the next pass reads the recorded (older) stamp, sees the
    difference and re-scans.  Every stamp must be present: a directory the
    trusted rule refused, because it changed in the tick its stamp was taken
    in, is one a later rename might not move, so nothing is installed on it.
    Only an owner the latest sweep discovered is cached, one checkpoint each
    (:func:`_retain_skip_checkpoints`); when the cache is full the newcomer
    is refused and nothing is evicted.  A cache entry only ever skips a
    cleanup scan: it holds no deletion or adoption authority.
    """

    if fragment_version is None or material_version is None or not stamps:
        return False
    if documents is None:
        return False
    if any(version is None for version in stamps.values()):
        return False
    if any(version is None for version in documents.values()):
        return False
    fences = len(stamps) + len(documents)
    chars = sum(len(name) for name in stamps) + sum(len(name) for name in documents)
    with _skip_checkpoints_lock:
        _forget_checkpoint_locked(key)
        if (key not in _skip_checkpoint_scope["keys"]  # type: ignore[operator]
                or len(_skip_checkpoints)
                >= int(_skip_checkpoint_scope["capacity"])):  # type: ignore[arg-type]
            return False
        _skip_checkpoints[key] = {
            "fragment": fragment_version, "material": material_version,
            "dirs": dict(stamps), "documents": dict(documents),
            "fence_count": fences, "bytes": chars,
        }
        _skip_checkpoint_usage["fences"] += fences
        _skip_checkpoint_usage["bytes"] += chars
    return True


def _skip_checkpoint_hit(key: tuple, fragment_path: Path,
                         material_path: Path) -> bool:
    """Whether an unchanged owner with nothing to act on may skip its scan.

    Every recorded stamp is re-read -- this owner's fragment and material,
    each parent directory of its paths, and each co-owner fragment its
    verdict relied on; any difference forgets the checkpoint and runs the
    uncached check, so a rename, a replacement, a removal or a metadata
    rewrite is seen before any skip.  A hit only ever skips the per-entry
    scan: the owner's terminal, live, lease and plan state were checked
    before this is consulted, and no deletion, adoption or mutable-state
    check is skipped.
    """

    with _skip_checkpoints_lock:
        record = _skip_checkpoints.get(key)
    if record is None:
        return False
    fresh = (_path_version(fragment_path) == record["fragment"]
             and _path_version(material_path) == record["material"])
    if fresh:
        for name, version in dict(record["documents"]).items():
            if _path_version(name) != version:
                fresh = False
                break
    if fresh:
        for name, version in dict(record["dirs"]).items():
            if _directory_version(name) != version:
                fresh = False
                break
    if not fresh:
        with _skip_checkpoints_lock:
            _forget_checkpoint_locked(key)
    return fresh


def _read_material_nofollow(root: Path, consumer_action_key: str,
                            mover_action_key: str):
    """The sidecar read with the cleanup authority's regular/no-follow rules.

    ``reader_lease.read_material`` follows a symlink standing where the
    sidecar belongs; a deletion authority may not.  Returns the validated
    document, ``None`` for absence, or a reason string for anything that is
    not a readable, valid regular file.
    """

    path = reader_lease.material_path(
        root, consumer_action_key, mover_action_key)
    try:
        raw = pb._read_regular_file_nofollow(path, where="prune material")
    except FileNotFoundError:
        return None
    except (OSError, ValueError, pb.PrismaBuildError) as exc:
        return f"material unreadable: {exc}"
    try:
        return reader_lease.validate_material(json.loads(raw))
    except (ValueError, reader_lease.ReaderLeaseError, pb.PrismaBuildError) as exc:
        return f"material invalid: {exc}"


def _containment_state(stage: Path, target: Path) -> str:
    """``"ok"``, ``"absent"`` or ``"unknown"`` for one path under a stage.

    Every component is ``lstat``ed from the stage root down, so a symlinked
    intermediate directory, a file standing where a directory belongs, or a
    path that leaves the stage is unknown ownership, never a pathname to act
    on.  Only the final component may be missing, and that is ``"absent"``:
    the caller may prune the mention but never unlink anything.

    A staged range's final name is two components, ``<rel>.pbrange`` and
    ``<offset>-<size>``, and the unlink that retires the range prunes the
    then-empty ``<rel>.pbrange`` directory with it.  That directory missing
    is therefore the range missing, ``"absent"``, not an unknown
    intermediate: every component above it was still ``lstat``ed.
    """

    try:
        # The configured root is resolved once (a stage root may itself be a
        # symlink); every component *below* it is lstat'ed, so a symlinked
        # intermediate directory is never followed.
        nominal = Path(os.path.abspath(str(stage)))
        base = Path(os.path.realpath(str(stage)))
        target_abs = Path(os.path.abspath(str(target)))
    except OSError:
        return "unknown"
    try:
        relative = target_abs.relative_to(nominal)
    except ValueError:
        return "unknown"
    if not relative.parts:
        return "unknown"
    current = base
    for index, part in enumerate(relative.parts):
        current = current / part
        last = index == len(relative.parts) - 1
        range_directory = (index == len(relative.parts) - 2
                           and part.endswith(RANGE_SUFFIX))
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            return "absent" if last or range_directory else "unknown"
        except OSError:
            return "unknown"
        if last:
            return "ok"
        if not statmod.S_ISDIR(info.st_mode):
            return "unknown"
    return "unknown"


def prune_stale_mentions(queue: pool.PoolQueue, mover_action_key: str, *,
                         consumer_action_key: str, stage: Path, tier_id: str,
                         root: Path, observed: Mapping[str, object],
                         ) -> dict[str, object]:
    """Prune positively stale paths from one terminal owner's documents (#853).

    A FAILED consumer's executed DONE mover can leave a material sidecar
    dating an incarnation the live destination no longer carries.  The shared
    publisher is right to refuse both adoption and replacement of that name,
    so the owner's stale vouch has to go before a successor can publish the
    path -- but a *mixed* document also holds coherent entries whose proof,
    bytes and charge are reusable cache, and whole-owner ``evict`` would
    destroy them.

    This transaction selects, classifies and acts inside one stage ownership
    hold (the lock a publisher's decide-and-rename holds), under the mover
    transition lock the caller already holds.  The rules, in order:

    * the stage root must positively belong to this queue
      (:func:`stage_root_refusal`), and the fragment must be the snapshot the
      terminal checks authorized and bind exactly to this tier and stage;
    * the material is read with the cleanup authority's regular/no-follow
      rules, and must bind the same consumer, mover, tier, stage root,
      manifest and epoch as the fragment;
    * **every** fragment entry must bind exactly to its own material key --
      key present, matching stage path, matching bytes, and matching digest
      where the fragment declares one -- before any path state is classified,
      so unknown metadata can never authorize a deletion;
    * a live claim, live reader pin, promotion handoff or same-key claim
      overlapping the candidate retains the whole owner: the bounded #877
      mover ends before the later ordinary sweep recovers the still-owned
      paths, and no mention is dropped merely because a foreign claim promises
      a future proof;
    * a path component outside the stage, or a symlinked intermediate
      directory, file or anything else standing where a directory belongs, is
      unknown ownership and retains the whole owner.  Only a missing leaf is
      ``absent``;
    * positive staleness for an existing regular file is inode difference,
      exactly as :meth:`stage_move._StagedPublisher._proof_candidate` reads it
      (#755).  Same-inode size/time change is divergence, not permission.  A
      co-owner fragment protects its physical file, so that entry and its date
      stay untouched;
    * the fragment and material file versions are sampled before their reads
      and again after the scan; a change during classification retains the
      whole owner and installs no checkpoint;
    * a fully stale, unprotected owner goes through :func:`_evict_owned` --
      the ordinary whole-owner egress -- inside this same transaction (no
      containment reclamation: that runs before the ownership lock by
      design), so its holder is settled exactly once;
    * a mixed owner is partially pruned: each positively stale destination is
      unlinked after a fresh identity comparison, then the fragment is
      rewritten to its survivors and the material to the same survivors'
      mentions, under the same generation and epoch.  A crash between the two
      document writes is safe: the fragment is authoritative and first, so a
      surviving material mention that no fragment entry names is a date
      without an owner, never ownership.

    The old holder is deliberately left with its entire charge.  That is a
    conservative reservation, including any slack for the deleted entries:
    when the final old fragment disappears, the ordinary whole-owner egress
    releases it exactly once, and ordinary pressure eviction may retire the
    smaller fragment and return the remainder in the meantime.  No partial
    capacity reclamation is claimed.

    The receipt reports committed metadata prunes (only after the fragment
    write succeeds), already-absent entries, entries actually unlinked and
    their bytes, whether the fragment/material pair completed, and that the
    whole charge was retained.

    An owner with nothing to act on -- no stale or absent path and no
    superset material this pass would trim (only an owner no co-owner
    protects is trimmed), whether coherent, protected by co-owners, or
    retained for any reason above -- changes nothing and installs a skip
    checkpoint (#1056).  It fences this owner's fragment and material, every
    parent directory of its paths, and every co-owner fragment the census
    under the lock read, each at the version verified here.  The next pass
    skips only while all of them are unchanged, and a skip only ever
    retains, so a removed or rewritten co-owner re-runs this transaction
    before anything it protected can prune or evict.  An owner with a stale
    or absent path is never cached, whatever retains it.
    """

    fragment_path = residency_map.fragment_path(
        root, consumer_action_key, mover_action_key)
    material_path = reader_lease.material_path(
        root, consumer_action_key, mover_action_key)
    errors: list[str] = []
    retained_reason = ""

    def receipt(*, pruned: int = 0, absent: int = 0, retained: int = 0,
                unlinked: int = 0, unlinked_bytes: int = 0,
                pair_complete: bool = True, partial: bool = False,
                complete: bool = False, cacheable: bool = False,
                charge_retained: bool = True) -> dict[str, object]:
        return {
            "schema": pool.POOL_EGRESS_SCHEMA_V1, "event": STALE_MENTION_EVENT,
            "action_key": mover_action_key,
            "consumer_action_key": consumer_action_key,
            "tier_id": tier_id, "stage_root": str(stage),
            "reason": "stale-mention-prune",
            "entries_pruned": pruned, "entries_already_absent": absent,
            "entries_retained": retained, "entries_unlinked": unlinked,
            "bytes_unlinked": unlinked_bytes,
            "document_pair_complete": pair_complete,
            "charge_retained": charge_retained, "partial": partial,
            "cacheable": cacheable, "retained_reason": retained_reason,
            "complete": complete, "errors": errors,
            "host": socket.gethostname(), "unix": time.time(),
        }

    refusal = stage_root_refusal(queue, str(stage))
    if refusal is not None:
        retained_reason = "stage-root-refused"
        errors.append(f"stage root refused: {refusal}")
        return receipt(retained=len(observed.get("entries") or {}))

    # Versions before the reads and again after the scan: metadata that
    # changed while this transaction classified it is not acted on, and the
    # versions a checkpoint installs are exactly the ones verified here.
    fragment_version_before = _path_version(fragment_path)
    material_version_before = _path_version(material_path)

    # The censuses and the containment fences go first, as hints, without
    # the lock (#988): the pass under it re-reads only what changed, and the
    # observed entries are the ones it acts on, because it retains unless
    # the fragment under the lock equals ``observed``.
    memo = _CensusMemo()
    census_started = time.perf_counter()
    observed_entries = observed.get("entries")
    hint_fences: dict[str, tuple] | None = None
    if isinstance(observed_entries, Mapping) and observed_entries:
        _ownership_census(queue, mover_action_key,
                          consumer_action_key=consumer_action_key,
                          stage=stage, tier_id=tier_id, root=root,
                          entries=observed_entries, memo=memo)
        _read_own_material(root, consumer_action_key, mover_action_key, memo)
        hint_fences = _entry_fences(stage, observed_entries)
    census_s = time.perf_counter() - census_started
    emptied: set[Path] = set()

    def _held_prune_stale() -> dict[str, object]:
        """The transaction itself, under the stage ownership lock."""

        nonlocal retained_reason
        try:
            current = residency_map.validate_fragment(json.loads(
                pb._read_regular_file_nofollow(
                    fragment_path, where="stale-mention fragment")))
        except (OSError, ValueError, pb.PrismaBuildError) as exc:
            errors.append(f"ownership uncertain: {exc}")
            return receipt(retained=len(observed.get("entries") or {}))
        entries = dict(current["entries"])
        total = len(entries)
        if current != observed:
            retained_reason = "fragment-changed"
            return receipt(retained=total)
        if str(current.get("tier_id")) != tier_id:
            retained_reason = "ownership-uncertain"
            errors.append("ownership uncertain: fragment tier does not match")
            return receipt(retained=total)
        if str(current.get("stage_root")) != str(stage):
            retained_reason = "ownership-uncertain"
            errors.append("ownership uncertain: fragment stage does not match")
            return receipt(retained=total)
        material = _read_material_nofollow(
            root, consumer_action_key, mover_action_key)
        if material is None:
            retained_reason = "material-absent"
            return receipt(retained=total)
        if isinstance(material, str):
            retained_reason = "material-unreadable"
            errors.append(f"ownership uncertain: {material}")
            return receipt(retained=total)
        if (str(material.get("consumer_action_key")) != consumer_action_key
                or str(material.get("mover_action_key")) != mover_action_key
                or str(material.get("tier_id")) != str(current.get("tier_id"))
                or str(material.get("stage_root")) != str(current.get("stage_root"))
                or str(material.get("manifest_sha256"))
                != str(current.get("manifest_sha256"))):
            retained_reason = "material-does-not-bind"
            return receipt(retained=total)
        # The strict reader's exact sidecar convention, read the way
        # ``reader_lease.acquire`` and ``covers_for_keys`` read it: a
        # non-RAM tier's fragment and sidecar carry no epoch at all
        # (absent or empty), and a RAM tier's carry the same non-empty
        # one.  Agreement is not validity -- two documents that agree on
        # a staged epoch are two documents the reader refuses
        # ("ownership-uncertain: staged epoch set"), and a deletion
        # authority may never read metadata the reader will not read as
        # ownership.  Anything else retains the whole candidate.
        fragment_epoch = current.get("epoch")
        material_epoch = material.get("epoch")
        if tier_id.startswith(storage_tiers.RAM_TIER_PREFIX):
            if not (isinstance(fragment_epoch, str) and fragment_epoch):
                retained_reason = "epoch-invalid"
                return receipt(retained=total)
            if material_epoch != fragment_epoch:
                retained_reason = "material-epoch-mismatch"
                return receipt(retained=total)
        elif (fragment_epoch not in (None, "")
                or material_epoch not in (None, "")):
            retained_reason = "epoch-invalid"
            return receipt(retained=total)
        # Every fragment entry binds exactly to its own material key before
        # any path state is classified: a by-path or first-mention match would
        # let unknown metadata authorize a deletion.  Extra material keys are
        # the crash superset and are ignored; every surviving key must bind.
        material_entries = dict(material.get("entries") or {})
        bound: dict[str, Mapping] = {}
        for key, entry in entries.items():
            mention = material_entries.get(key)
            if not isinstance(mention, Mapping):
                retained_reason = "material-key-missing"
                return receipt(retained=total)
            if (os.path.normpath(str(mention["stage_path"]))
                    != os.path.normpath(str(entry["stage_path"]))
                    or int(mention["bytes"]) != int(entry["bytes"])):
                retained_reason = "material-key-mismatch"
                return receipt(retained=total)
            declared = entry.get("sha256")
            if (isinstance(declared, str) and declared
                    and str(mention["sha256"]) != declared):
                retained_reason = "material-digest-mismatch"
                return receipt(retained=total)
            if not isinstance(mention.get("file_id"), Mapping):
                retained_reason = "material-key-undated"
                return receipt(retained=total)
            bound[key] = mention
        census = _ownership_census(
            queue, mover_action_key, consumer_action_key=consumer_action_key,
            stage=stage, tier_id=tier_id, root=root, entries=entries,
            memo=memo)
        claimed = census["claimed"]
        own_claimed = census["own_claimed"]
        owners = census["owners"]
        pins = census["pins"]
        source_paths = census["source_paths"]
        tainted = census["tainted"]
        checkpoint_key = _skip_checkpoint_key(
            queue, root, stage, tier_id, consumer_action_key, mover_action_key)
        # The whole owner is retained, whatever its paths say, for any of
        # these, in the order the transaction has always tested them.  Since
        # #1056 they no longer end the classification: the path state below
        # decides whether a skip checkpoint may stand for this owner.  The
        # checkpoint certifies only that nothing is stale, absent or to trim,
        # and a skip only ever retains, so a retention reason ending changes
        # nothing it stands for -- while a stale path under any reason is
        # never cached, because the retention only postpones its prune.
        held_reason = ""
        held_errors: list[str] = []
        if tainted:
            held_reason = "ownership-uncertain"
            held_errors = [f"ownership uncertain: {item}" for item in tainted]
        elif own_claimed:
            held_reason = "same-key-claimed"
        elif pins:
            held_reason = "live-pin"

        def uncached(reason: str, *why: str) -> dict[str, object]:
            """Retain the whole owner uncached; the first reason found wins."""

            nonlocal retained_reason
            if held_reason:
                retained_reason = held_reason
                errors.extend(held_errors)
            else:
                retained_reason = reason
                errors.extend(why)
            return receipt(retained=total)

        stage_abs = Path(os.path.abspath(str(stage)))
        overlap_claim = overlap_handoff = False
        contained: dict[str, str] = {}
        for key, entry in entries.items():
            path = Path(str(entry["stage_path"]))
            state = _containment_state(stage, path)
            contained[key] = state
            if state == "unknown":
                return uncached(
                    "ownership-uncertain",
                    f"ownership uncertain: {entry['stage_path']} is not a "
                    f"contained stage path")
            try:
                relative = str(Path(os.path.abspath(str(path))).relative_to(
                    stage_abs))
            except (OSError, ValueError):
                relative = None
            if relative is not None and relative in claimed:
                overlap_claim = True
            try:
                resolved = os.path.normpath(str(path.resolve()))
            except OSError:
                resolved = ""
            if resolved in source_paths:
                overlap_handoff = True
        if not held_reason and overlap_claim:
            held_reason = "live-claim"
        elif not held_reason and overlap_handoff:
            held_reason = "promotion-handoff"
        # The verified directory stamps, sampled around the classification
        # scan; nothing is sampled a third time for installation.  The
        # sample before the scan is a TRUSTED stamp (#1062): two changes in
        # one coarse clock tick share a stamp, so a stamp sampled between
        # them would be one the second change never moves, and a checkpoint
        # standing on it would skip a rename made in that tick.
        # ``stage_move._trusted_directory_stamp`` refuses a directory changed
        # in the current tick (and one on a filesystem whose times are not
        # this kernel's clock); a refused stamp still takes part in the
        # before/after comparison, at its bare version, but the checkpoint
        # refuses to install on it, so the owner is scanned again next pass.
        parents = {Path(str(entry["stage_path"])).parent
                   for entry in entries.values()}
        dirs_trusted = {str(parent): _trusted_directory_stamp(parent)
                        for parent in parents}
        dirs_before = {name: (stamp if stamp is not None
                              else _directory_version(name))
                       for name, stamp in dirs_trusted.items()}
        prune: list[str] = []
        absent: list[str] = []
        retained_paths = 0
        expected_ino: dict[str, int] = {}
        for key, entry in entries.items():
            if held_reason and (prune or absent):
                # Retained whatever the rest says, and never cacheable once
                # a path is stale or absent: the scan has nothing left to
                # decide, so it stops here rather than hold the lock for it.
                return uncached(held_reason)
            if contained[key] == "absent":
                absent.append(key)
                continue
            path = Path(str(entry["stage_path"]))
            norm = os.path.normpath(str(path))
            # The path's own state is classified before the co-owner branch:
            # a co-owner's fragment protects a physical file, but it can never
            # make a nonregular, unstatable or unknown path clean, and it must
            # not let another stale path of the same candidate delete.
            try:
                info = os.lstat(path)
            except FileNotFoundError:
                absent.append(key)
                continue
            except OSError as exc:
                return uncached("ownership-uncertain",
                                f"ownership uncertain: {key}: {exc}")
            if not statmod.S_ISREG(info.st_mode):
                return uncached(
                    "ownership-uncertain",
                    f"ownership uncertain: {key} is not a regular file")
            if owners.get(norm):
                # A co-owner's fragment protects the physical file: keep this
                # entry and its date exactly as they are.
                retained_paths += 1
                continue
            if int(bound[key]["file_id"].get("ino", -1)) != int(info.st_ino):
                # Positive staleness: the name carries an incarnation this
                # record does not date (#755), so the vouch can never prove
                # or protect it.
                prune.append(key)
                expected_ino[key] = int(info.st_ino)
                continue
            live = reader_lease.stat_identity(str(path))
            if live is None or not reader_lease.file_id_matches(
                    bound[key]["file_id"], live):
                # Same inode, changed in place: divergence, not permission.
                return uncached(
                    "ownership-uncertain",
                    f"ownership uncertain: {key} changed in place")
        # The after sample needs no clock: it is only compared with the
        # before sample.  Equal means no change moved a stamp during the
        # scan, so each trusted stamp is still the directory's version, and
        # that trusted stamp (never this sample) is what a checkpoint records.
        dirs_after = {str(parent): _directory_version(parent) for parent in parents}
        if dirs_before != dirs_after:
            return uncached("ownership-uncertain",
                            "ownership uncertain: a parent directory changed")
        fragment_version_after = _path_version(fragment_path)
        material_version_after = _path_version(material_path)
        if (fragment_version_after is None or material_version_after is None
                or fragment_version_after != fragment_version_before
                or material_version_after != material_version_before):
            return uncached("documents-changed")
        # What a skip checkpoint fences beyond this owner's own documents and
        # directories: every co-owner fragment that names one of its paths,
        # at the version the census under this lock read it (#1056).  One
        # removed or rewritten re-runs this census on the next pass, where
        # the file it protected may now prune or evict.
        co_owner_fences = _co_owner_fences(memo, census["co_owner_documents"])
        exact_material = set(material_entries) == set(entries)
        if held_reason:
            retained_reason = held_reason
            errors.extend(held_errors)
            # Idle means a pass without the reason would change nothing:
            # nothing stale or absent, and no superset material that pass
            # would trim (it trims only an owner no co-owner protects).
            idle = (not prune and not absent
                    and (retained_paths > 0 or exact_material))
            cacheable = idle and _install_skip_checkpoint(
                checkpoint_key, fragment_version_after,
                material_version_after, dirs_trusted, co_owner_fences)
            return receipt(retained=total, cacheable=cacheable)
        if not prune and not absent:
            if retained_paths:
                # Protected by co-owners and otherwise coherent: nothing
                # changes until a document it read does (#1056).
                retained_reason = "co-owner"
                cacheable = _install_skip_checkpoint(
                    checkpoint_key, fragment_version_after,
                    material_version_after, dirs_trusted, co_owner_fences)
                return receipt(retained=total, cacheable=cacheable)
            # A crash between the fragment and material writes leaves the
            # material a superset.  The strict reader walks every material
            # entry, so the pair is complete only when the material dates
            # exactly the fragment's validated keys: trim it before caching
            # this otherwise-coherent owner, under the same generation.
            material_final_version = material_version_after
            if not exact_material:
                try:
                    reader_lease.write_material(
                        root, consumer_action_key=consumer_action_key,
                        mover_action_key=mover_action_key, tier_id=tier_id,
                        stage_root=str(current["stage_root"]),
                        manifest_sha256=str(current["manifest_sha256"]),
                        generation=str(material["generation"]),
                        entries={key: material_entries[key] for key in entries},
                        epoch=material.get("epoch"))
                except (OSError, ValueError, pb.PrismaBuildError) as exc:
                    errors.append(f"material trim: {exc}")
                    return receipt(retained=total)
                material_final_version = _path_version(material_path)
            cacheable = _install_skip_checkpoint(
                checkpoint_key, fragment_version_after,
                material_final_version, dirs_trusted, co_owner_fences)
            return receipt(retained=total, cacheable=cacheable)
        if not retained_paths and len(prune) + len(absent) == total:
            # Fully stale and unprotected: the ordinary whole-owner egress is
            # exact and settles the holder once.  Called here, inside the same
            # ownership transaction; containment reclamation deliberately does
            # not run under this lock.
            return _evict_owned(
                queue, mover_action_key, consumer_action_key=consumer_action_key,
                stage=stage, tier_id=tier_id, root=root,
                fragment_path=fragment_path, entries=entries, errors=errors,
                reason="stale-mention-prune", auto_reclaimed=[],
                auto_retained={}, memo=memo, fences=hint_fences,
                prune_after=emptied)
        # Partial prune: unlink the positively stale destinations, then write
        # the fragment to its survivors and the material to the same
        # survivors' mentions.  No ledger call: the whole old charge stays.
        surviving = {key: entry for key, entry in entries.items()
                     if key not in expected_ino and key not in absent}
        unlinked: list[str] = []
        unlinked_bytes = 0

        def partial_receipt(*, fragment_committed: bool,
                            pair_complete: bool) -> dict[str, object]:
            # Only the committed fragment write makes a metadata prune real;
            # the physical deletions are reported as they actually happened,
            # even when the pair never completed.
            return receipt(
                pruned=(len(prune) + len(absent)) if fragment_committed else 0,
                absent=len(absent) if fragment_committed else 0,
                retained=len(surviving) if fragment_committed else total,
                unlinked=len(unlinked), unlinked_bytes=unlinked_bytes,
                pair_complete=pair_complete, partial=True)

        for key in prune:
            path = Path(str(entries[key]["stage_path"]))
            try:
                info = os.lstat(path)
            except FileNotFoundError:
                continue
            except OSError as exc:
                errors.append(f"{key}: {exc}")
                return partial_receipt(fragment_committed=False,
                                       pair_complete=False)
            if (not statmod.S_ISREG(info.st_mode)
                    or int(info.st_ino) != expected_ino[key]):
                # Fresh identity comparison at the act itself: what the scan
                # classified is what is unlinked, or nothing is.
                errors.append(f"{key}: destination changed under the lock")
                return partial_receipt(fragment_committed=False,
                                       pair_complete=False)
            try:
                os.unlink(path)
            except FileNotFoundError:
                continue
            except OSError as exc:
                errors.append(f"{key}: {exc}")
                return partial_receipt(fragment_committed=False,
                                       pair_complete=False)
            unlinked.append(key)
            unlinked_bytes += int(info.st_size)
            emptied.add(path.parent)
        # Survivor material is filtered by the fragment's exact validated key
        # set, never by matching path: an extra material key naming the same
        # path must not survive the rewrite, and safe extra material never
        # grants ownership or deletion authority.
        survivor_mentions = {key: material_entries[key] for key in surviving}
        document = {key: current[key] for key in current if key != "entries"}
        document["entries"] = surviving
        try:
            residency_map.write_fragment(root, document)
        except (OSError, ValueError, pb.PrismaBuildError) as exc:
            errors.append(f"fragment rewrite: {exc}")
            return partial_receipt(fragment_committed=False,
                                   pair_complete=False)
        try:
            reader_lease.write_material(
                root, consumer_action_key=consumer_action_key,
                mover_action_key=mover_action_key, tier_id=tier_id,
                stage_root=str(current["stage_root"]),
                manifest_sha256=str(current["manifest_sha256"]),
                generation=str(material["generation"]),
                entries=survivor_mentions,
                epoch=material.get("epoch"))
        except (OSError, ValueError, pb.PrismaBuildError) as exc:
            errors.append(f"material rewrite: {exc}")
            return partial_receipt(fragment_committed=True,
                                   pair_complete=False)
        return partial_receipt(fragment_committed=True, pair_complete=True)

    # One hold for the whole transaction, as before #988: its per-entry
    # ``lstat`` classification is the act's own identity evidence, and a
    # partial prune rewrites the fragment and material inside it.  What
    # left the hold is the census parse and the fence (above) and the
    # empty-directory prune (below).
    asked = time.perf_counter()
    with queue.stage_ownership_lock(str(stage)):
        granted = time.perf_counter()
        result = _held_prune_stale()
    released = time.perf_counter()
    pruned_started = time.perf_counter()
    for parent in sorted(emptied, key=lambda one: len(one.parts), reverse=True):
        _prune_empty(parent, stage)
    result.update(_hold_record(
        lock_wait_s=granted - asked, lock_held_s=released - granted,
        census_s=census_s, prune_s=time.perf_counter() - pruned_started))
    return result


def _tokens_for_newly_free_bytes(stage_bytes: int) -> int:
    """Token count actually leaving the stage settles, in whole GiB, floored.

    The conservative sibling of :func:`_tokens_for_egressed_bytes`: only
    whole GiB of genuinely new room return as free.  A 1-token holder with
    0.5 GiB deleted and 0.5 GiB shared frees nothing here -- the ceil
    would hand a whole token back for half a GiB of new room -- while the
    staying half is covered by the decharge share instead.  Slack the
    floor leaves behind is restored by the next fresh mint counting the
    actually-grown writable, never by freeing what is still occupied.
    """

    if stage_bytes <= 0:
        return 0
    return int(stage_bytes) // storage_tiers.GIB


def _tokens_for_egressed_bytes(stage_bytes: int) -> int:
    """Token count a bucket of egressed bytes settles, in whole GiB.

    The same ceil the demand was sealed with
    (:func:`storage_tiers.stage_tokens_for_bytes`): a sealed demand is
    always at least this floor, so capping the freed share at this number
    can only decharge more, never free more -- the safe direction.  Zero
    bytes settle zero tokens.
    """

    if stage_bytes <= 0:
        return 0
    return storage_tiers.stage_tokens_for_bytes(int(stage_bytes))


def _relative_under(stage: Path, resolved: str) -> str | None:
    """This stage root's relative name for a resolved path, or ``None``."""

    try:
        return str(Path(resolved).relative_to(stage.resolve()))
    except (OSError, ValueError):
        return None


def live_claims(queue: pool.PoolQueue) -> tuple[set[str], dict[str, str]]:
    """Every mover key a ready or claimed item still wants, and those items.

    Two answers because they mean different things.  ``wanted`` is the movers:
    a live consumer's declared leads *and* every mover of its frozen plan, so a
    range staged three phases ahead of where it is reading is not an orphan.
    ``owners`` is the live items themselves, keyed by their own action key,
    which is how a consumer that holds tier tokens of its own is told apart
    from a mover.

    Read once by whoever needs it: the orphan sweep asks "which held keys are
    nobody's", and the adoption asks the same question the other way round --
    a range only a *finished* consumer still names is one a successor may take
    over (#598).  One walk of ``ready/`` and ``claimed/`` answers both.
    """

    wanted: set[str] = set()
    owners: dict[str, str] = {}
    for state in (pool.READY, pool.CLAIMED):
        for path, item in queue_records(queue, state):
            residency = item.get("residency") if isinstance(item, dict) else None
            if not isinstance(residency, dict):
                continue
            for lead in residency.get("leads") or []:
                wanted.add(str(lead))
            key = path.name[:-len(".json")] if path.name.endswith(".json") else path.name
            owners[key] = key
            plan = residency_plan.read(queue, key)
            if plan is not None:
                wanted.update(residency_plan.mover_keys(plan))
    return wanted, owners


def _metadata_absent(path: Path) -> bool:
    """Positive absence with strict close-to-open NFS revalidation.

    Opening the parent follows the queue's ``_read_json_fresh`` discipline,
    without its best-effort fallback or another full directory listing for
    each candidate. Any object at the address retains, including symlinks;
    unreadable/non-directory parents raise instead of becoming absence.
    """
    try:
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY
                             | os.O_CLOEXEC | os.O_NOFOLLOW)
    except FileNotFoundError:
        # The parent can itself have a negatively cached lookup. Revalidate
        # its parent before concluding that this subtree does not exist.
        if _metadata_absent(path.parent):
            return True
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY
                             | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        os.stat(path.name, dir_fd=descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return True
    finally:
        os.close(descriptor)
    return False


def _owner_record(queue: pool.PoolQueue, state: str, key: str) -> dict:
    """Read a regular, identity-bound queue outcome; filenames are not proof."""
    record = json.loads(pb._read_regular_file_nofollow(
        queue.item_path(state, key), where="dead-owner outcome"))
    if (not isinstance(record, dict)
            or record.get("schema") != pool.POOL_OUTCOME_SCHEMA_V1
            or record.get("action_key") != key):
        raise pool.PoolContractError("dead-owner outcome identity mismatch")
    queue.attempt_generation(record)  # validates a finite generation and key
    return record


def _require_exact_withdrawal(queue: pool.PoolQueue, key: str) -> dict:
    """The withdrawn record of ``key``, proven by its one immutable decision.

    A withdrawn marker by filename alone is not proof: the decision filed for
    the marker's generation must be exactly one, must equal the marker, and
    must be the one ``withdrawal_covers`` answers with.  Anything else raises,
    and the caller retains.
    """

    marker = _owner_record(queue, pool.WITHDRAWN, key)
    decisions = queue.withdrawal_decisions(
        key, generation=marker["published_unix"])
    if (marker.get("status") != "withdrawn"
            or len(decisions) != 1
            or decisions[0][1] != marker
            or queue.withdrawal_covers(marker, action_key=key) != marker):
        raise pool.PoolContractError(
            "withdrawal lacks its exact immutable decision")
    return marker


def _partial_done_receipt_matches(queue, consumer: str, mover: str,
                                  fragment: Mapping[str, object]) -> bool:
    """Exact partial-copy evidence for an unmaterialized DONE owner (#866).

    The immutable terminal proves the attempt ended; this separately binds
    its incomplete movement declaration to the fragment about to be retired.
    A complete or unknown copy is outside this narrow recovery contract.
    """
    record = json.loads(pb._read_regular_file_nofollow(
        queue.move_path(mover), where="partial DONE move receipt"))
    if not isinstance(record, dict):
        raise pool.PoolContractError("partial DONE receipt is not an object")
    if record.get("complete") is not False:
        return False
    for field, expected in (("schema", pool.POOL_MOVE_SCHEMA_V1),
                            ("action_key", mover), ("consumer_action_key", consumer),
                            ("tier_id", fragment["tier_id"]),
                            ("stage_root", fragment["stage_root"]),
                            ("manifest_sha256", fragment["manifest_sha256"])):
        if record.get(field) != expected:
            raise pool.PoolContractError(f"partial DONE receipt differs at {field}")
    fields = ("entries_declared", "entries_staged", "bytes_staged",
              "range_start_bytes", "range_end_bytes", "range_bytes")
    if any(type(record.get(field)) is not int or record[field] < 0 for field in fields):
        raise pool.PoolContractError("partial DONE receipt has invalid counts/range")
    entries = fragment["entries"]
    if (not 0 < record["entries_staged"] < record["entries_declared"]
            or record["entries_staged"] != len(entries)
            or record["bytes_staged"] != sum(int(entry["bytes"]) for entry in entries.values())
            or not 0 < record["bytes_staged"] <= record["range_bytes"]
            or record["range_bytes"] != record["range_end_bytes"] - record["range_start_bytes"]):
        raise pool.PoolContractError("partial DONE receipt does not cover its fragment")
    return True


class _TerminalNames:
    """Whether a terminal directory names a key, looked up by key (#992).

    What a listing of ``failed/``, ``withdrawn/`` or ``done/`` was used for:
    a candidate filter, with every decision taken again under the locks by
    :func:`_metadata_absent`.  Any entry at the name counts, as it did in
    the listing.  Each directory is checked once to be a directory, so a
    terminal directory that is missing or unreadable still refuses the
    whole pass rather than reading as "nothing ended"; after that a lookup
    that fails any other way than "no such entry" raises, and the caller
    refuses the same way.  One lookup per key per pass.
    """

    def __init__(self, queue: pool.PoolQueue) -> None:
        self._queue = queue
        self._seen: dict[tuple[str, str], bool] = {}
        for state in (pool.FAILED, pool.WITHDRAWN, pool.DONE):
            if not statmod.S_ISDIR(os.stat(queue.dir(state)).st_mode):
                raise NotADirectoryError(
                    errno.ENOTDIR, "terminal directory is not a directory",
                    str(queue.dir(state)))

    def has(self, state: str, key: str) -> bool:
        found = self._seen.get((state, key))
        if found is None:
            try:
                os.lstat(self._queue.item_path(state, key))
                found = True
            except FileNotFoundError:
                found = False
            self._seen[(state, key)] = found
        return found


#: The unit `sweep_dead_owner_fragments` asks a cycle budget for: one dead
#: consumer's validation and every census and egress of its movers (#1072).
DEAD_OWNER_UNIT = "dead-owner"


def sweep_dead_owner_fragments(
        queue: pool.PoolQueue, *, stage_roots: dict[str, str],
        residency_root: str | Path | None = None,
        index: CensusIndex | None = None,
        budget=None,
) -> list[dict[str, object]]:
    """Retire proven dead unmaterialized owners with no charge (#839, #866).

    A consumer is dead when it has exactly one terminal record and that
    record is proven: a failed one by its attempt-backed summary, a withdrawn
    one by its exact immutable withdrawal decision (#892).

    One complete fragment census and one ledger discovery per tier identify
    candidates, never authorize deletion. Each consumer is then held across
    validation and all its evictions; each child is held before its fresh
    state checks and existing egress. Lock order is consumer -> mover -> stage
    ownership, the same as window publication and dead-consumer withdrawal.
    Thus a successor published after discovery either wins before these locks
    and is retained, or waits until this old owner's egress has completed.

    Failure evidence is the queue's immutable attempt-backed summary, checked
    once per consumer transaction. A cancellation must match its immutable
    generation decision. A DONE mover instead needs an immutable executed
    terminal plus one of two positive shapes: an incomplete move receipt
    exactly covering an unmaterialized fragment (#866), or a material sidecar
    that is pruned of the paths whose live destination carries a different
    incarnation (#853, :func:`prune_stale_mentions`).  The second shape may
    carry a charge: a partial prune retains the whole holder and no charge
    moves, while a fully stale owner goes through the ordinary whole-owner
    egress exactly once.  Missing, unreadable, legacy or inconsistent evidence
    retains, as do any plan, live row, lease, live claim, reader pin,
    promotion handoff or unreadable census. Withdrawn movers still require no
    receipt and are never partially pruned. Other terminal shapes and all
    produced namespaces remain excluded. No age or pressure is deletion
    authority.

    Discovery reads only what can name a candidate (#992).  The census is
    ``index``'s when the tier loop passes its own, so a cycle re-reads only
    the namespaces that changed; and the terminal records are looked up by
    key for the consumers and movers the census names, never listed: the
    three terminal directories hold every action the fleet ever finished
    (38,000 names on 2026-09-23) and a fragment names a few hundred.

    The stale-mention skip checkpoints follow the same discovery (#1056):
    once it completes, only the owners it found keep a checkpoint, one
    each, and ``index`` counts the owners a checkpoint skipped
    (``stale_skipped``) and the owners censused (``stale_censused``) for
    the cycle line, since a skip files no receipt.

    ``budget``, when the tier loop passes one (#1072), is asked before each
    candidate consumer whether its unit still fits this cycle
    (``budget.start``) and told when the unit ends (``budget.done``), after
    the consumer's locks are released.  The consumers are taken from the one
    it refused first last cycle (``budget.order``).  A consumer it refuses is
    not examined, and keeps its skip checkpoints: they are chosen from the
    whole discovery above, before any unit runs.
    """
    root = Path(residency_root if residency_root is not None
                else queue.root / pool.RESIDENCY)
    if root.name == produced_output.OUTPUT_FRAGMENTS_SUBDIR:
        return []
    receipts: list[dict[str, object]] = []

    def refuse(why: str, consumer: str = "", mover: str = "") -> None:
        receipts.append({
            "schema": pool.POOL_EGRESS_SCHEMA_V1, "event": DEAD_OWNER_EVENT,
            "action_key": mover, "consumer_action_key": consumer,
            "tier_id": "", "stage_root": "", "reason": "dead-owner-sweep",
            "complete": False, "errors": [why],
            "host": socket.gethostname(), "unix": time.time(),
        })

    fragments, tainted = _fragment_census(root, index)
    if tainted:
        refuse("ownership uncertain: " + "; ".join(tainted[:8]))
        return receipts
    try:
        named = _TerminalNames(queue)
    except OSError as exc:
        refuse(f"ownership uncertain: queue census: {exc}")
        return receipts
    held_by_tier: dict[str, set[str]] = {}
    for tier, stage in stage_roots.items():
        if stage_root_refusal(queue, stage) is not None:
            continue
        try:
            # Include nonregular holder names: an unknown ledger entry is
            # not evidence that this mover has no charge.
            held_by_tier[tier] = set(os.listdir(queue.tier_ledger(tier).held_dir))
        except FileNotFoundError:
            held_by_tier[tier] = set()
        except (OSError, pool.PoolContractError) as exc:
            refuse(f"ownership uncertain: tier ledger: {exc}")
    candidates: dict[str, list[tuple[str, dict]]] = {}
    try:
        for consumer, mover, fragment, direct in fragments:
            tier = fragment.get("tier_id")
            if (direct and _namespace_shaped(consumer)
                    and tier in held_by_tier
                    and (named.has(pool.FAILED, consumer)
                         or named.has(pool.WITHDRAWN, consumer))
                    and (named.has(pool.WITHDRAWN, mover)
                         or named.has(pool.DONE, mover))):
                # The charge is not a discovery filter: a charged,
                # material-bearing DONE owner is the #853 shape the material
                # branch prunes.  The zero-charge gates of #839/#866 stay in
                # the per-mover block below.
                candidates.setdefault(consumer, []).append((mover, fragment))
    except OSError as exc:
        refuse(f"ownership uncertain: queue census: {exc}")
        return receipts
    # Discovery is complete: the skip checkpoints kept are exactly those of
    # the owners it found (#1056), so an owner that left -- evicted, adopted,
    # its fragment gone -- stops holding a place in the cache, and the cache
    # holds at most one checkpoint per owner on disk.
    _retain_skip_checkpoints(
        _skip_checkpoint_key(queue, root, Path(stage_roots[str(fragment["tier_id"])]),
                             str(fragment["tier_id"]), consumer, mover)
        for consumer, children in candidates.items()
        for mover, fragment in children)
    uncertainty = (OSError, ValueError, pb.PrismaBuildError)
    order = (list(candidates) if budget is None
             else budget.order(DEAD_OWNER_UNIT, list(candidates)))
    for consumer in order:
        children = candidates[consumer]
        if budget is not None and not budget.start(DEAD_OWNER_UNIT, consumer):
            continue
        try:
            _dead_owner_unit(queue, consumer, children, root=root,
                             stage_roots=stage_roots, index=index,
                             refuse=refuse, receipts=receipts,
                             uncertainty=uncertainty)
        finally:
            if budget is not None:
                budget.done(DEAD_OWNER_UNIT)
    return receipts


def _dead_owner_unit(queue: pool.PoolQueue, consumer: str,
                     children: list[tuple[str, dict]], *, root: Path,
                     stage_roots: dict[str, str], index: CensusIndex | None,
                     refuse, receipts: list[dict[str, object]],
                     uncertainty: tuple[type[BaseException], ...]) -> None:
    """One candidate consumer of `sweep_dead_owner_fragments`, under its locks."""

    try:
        with queue._transition_locked(consumer):
            live, why = residency_plan.live_state(queue, consumer)
            if why:
                refuse(f"ownership uncertain: {why}", consumer)
            if live or why:
                return
            # Exactly one terminal record: failed or withdrawn.  Two is
            # not a death but a question, and done is not a death at all.
            if not _metadata_absent(queue.item_path(pool.DONE, consumer)):
                return
            consumer_failed = not _metadata_absent(
                queue.item_path(pool.FAILED, consumer))
            consumer_withdrawn = not _metadata_absent(
                queue.item_path(pool.WITHDRAWN, consumer))
            if consumer_failed == consumer_withdrawn:
                return
            if (not _metadata_absent(queue.lease_path(consumer))
                    or not _metadata_absent(queue.residency_plan_path(consumer))):
                return
            if consumer_failed:
                failed = _owner_record(queue, pool.FAILED, consumer)
                # Reuse the queue's canonical history/generation/log
                # checks; no whole-model hashing, and only once for this
                # consumer.
                ending = queue.adopted_attempt_summary(failed)
                if (failed.get("status") != "failed"
                        or ending["status"] != "failed"
                        or ending["disposition"] != pool.FAILED):
                    return
            else:
                # A withdrawn consumer is as dead as a failed one when its
                # immutable decision says so -- the proof a withdrawn
                # mover already needs below.  Without it, WS-P cleared
                # three such owners by hand on 2026-09-22.
                _require_exact_withdrawal(queue, consumer)
            for mover, observed in children:
                try:
                    with queue.mover_transition_lock(mover):
                        live, why = residency_plan.live_state(queue, mover)
                        if why:
                            refuse(f"ownership uncertain: {why}", consumer, mover)
                        if live or why:
                            continue
                        if not _metadata_absent(queue.item_path(pool.FAILED, mover)):
                            continue
                        done = not _metadata_absent(queue.item_path(pool.DONE, mover))
                        if done:
                            if not _metadata_absent(queue.item_path(pool.WITHDRAWN, mover)):
                                continue
                            terminal = _owner_record(queue, pool.DONE, mover)
                            ending = queue.adopted_attempt_summary(terminal)
                            if (terminal.get("status") != "executed"
                                    or ending["status"] != "executed"
                                    or ending["disposition"] != pool.DONE):
                                continue
                        else:
                            _require_exact_withdrawal(queue, mover)
                        tier = str(observed["tier_id"])
                        if not _metadata_absent(queue.lease_path(mover)):
                            continue
                        material_present = not _metadata_absent(
                            reader_lease.material_path(root, consumer, mover))
                        if material_present:
                            # #853: a material-bearing DONE owner, charged
                            # or not.  A withdrawn one keeps whatever its
                            # sidecar dates (no re-dispatch guarantee
                            # exists for it here), exactly as before.
                            if not done:
                                continue
                            if _skip_checkpoint_hit(
                                    _skip_checkpoint_key(
                                        queue, root,
                                        Path(stage_roots[tier]), tier,
                                        consumer, mover),
                                    residency_map.fragment_path(
                                        root, consumer, mover),
                                    reader_lease.material_path(
                                        root, consumer, mover)):
                                # No receipt for a skip; the cycle line
                                # counts it (#1056).
                                if index is not None:
                                    index.stale_skipped += 1
                                continue
                            if index is not None:
                                index.stale_censused += 1
                            receipts.append(prune_stale_mentions(
                                queue, mover, consumer_action_key=consumer,
                                stage=Path(stage_roots[tier]), tier_id=tier,
                                root=root, observed=observed))
                            continue
                        if not _metadata_absent(
                                queue.tier_ledger(tier).held_dir / mover):
                            continue
                        if not done and not _metadata_absent(queue.move_path(mover)):
                            continue
                        current = residency_map.validate_fragment(json.loads(
                            pb._read_regular_file_nofollow(
                                residency_map.fragment_path(root, consumer, mover),
                                where="dead-owner fragment")))
                        if current != observed:
                            continue  # adoption/republication won discovery
                        if done and not _partial_done_receipt_matches(
                                queue, consumer, mover, current):
                            continue
                        receipts.append(evict(
                            queue, mover, consumer_action_key=consumer,
                            stage_root=stage_roots[tier], residency_root=root,
                            reason="dead-owner-sweep"))
                except uncertainty as exc:
                    refuse(f"ownership uncertain: {exc}", consumer, mover)
    except uncertainty as exc:
        refuse(f"ownership uncertain: {exc}", consumer)


def sweep(queue: pool.PoolQueue, *, stage_roots: dict[str, str],
          residency_root: str | Path | None = None,
          pressure: Mapping[str, int] | None = None,
          index: CensusIndex | None = None,
          budget=None) -> list[dict[str, object]]:
    """Evict every pinned mover no live item still names as a lead.

    A consumer withdrawn between its movers finishing and its own claim would
    otherwise hold the stage for the life of the fleet: nothing publishes its
    egress, because nothing is waiting for its bytes.  The test is deliberately
    the queue's own live state --- ready or claimed --- rather than a policy: a
    lead some item may still be admitted on is not an orphan, however old.

    Then, per tier, :func:`reconcile` takes back what no key accounts for at
    all.  It runs after the held-key evictions above rather than before, so it
    reads the directory the ledger now describes rather than one eviction
    behind it.

    **A live consumer protects its whole plan, not just its leads.**  The
    consumer depends on its first phase only, so a pinned mover three phases
    ahead of it is named by nothing in the queue: testing ``leads`` alone would
    make this sweep delete the window it exists to protect, on the cycle after
    it was staged.  The frozen plan is what says a mover is still wanted.

    **``pressure`` is what makes an orphan's eviction a decision rather than a
    reflex (#598).**  Given, it is the GiB each tier's own window cannot place,
    and orphans are evicted on that tier only until the tier has that much
    free.  Not given -- a direct call, an operator, a test of this primitive --
    every orphan goes, which is the behaviour this had before.  Deferring is
    safe because an orphan's tokens are still held the whole time: the ledger
    counts every resident byte, so nothing is admitted onto capacity that is
    not there.  What it buys is that a consumer's failure no longer deletes
    731 GB before its retry is submitted, which is the measured cost this
    exists to remove; Rob, 2026-09-18: *"We should not be rerunning anything in
    bulk if avoidable."*  Oldest receipt first, so a tier under repeated
    pressure takes the same range back twice rather than alternating between
    two -- a deterministic order, not a ranking of what is worth keeping.

    **A held mover with no receipt is named by its fragment (#892).**  The
    receipt is where this reads a mover's consumer, and a mover can outlive
    its receipt: the canary leg-3 mover ``aa34e2a6e22f`` held 1 GiB for
    three days with none.  Exactly one direct fragment naming the mover is
    an exact owner, and once that owner has provably ended -- not queued, and
    exactly one outcome record -- the mover is an orphan like any other.  No
    fragment, several, a produced-output one, a tainted census, or an owner
    with no ending retains.  A retained holder is reported, with the reason,
    only when the tier's window still lacks room after the pass; otherwise it
    waits quietly, as it did before.

    **A funded produced-output mover is its lane's (#929).**  Before either
    rule above, :func:`produced_holder` asks the output funding record whether
    the key is a produced mover on this tier.  If it is, the lane decides:
    a live producer attempt, or a mover still queued, keeps it; a producer
    attempt that has ended, with the mover ended too, is retired at once
    through the producer's own ``retire_batch``, pressure or none, because a
    retried producer binds a new batch namespace and nothing can read this
    copy again (the live ``6fbc96301c6c`` held 1 GiB that way after its
    producer failed and its mover was withdrawn from ``ready``).  Everything
    else is kept.  Every held key the pass can prove neither live nor an
    orphan is reported as ``stage-holder-unresolved`` once per change of its
    reason, whatever the pressure: a holder nothing can classify is seen
    once, not never and not every cycle.

    **Dead owners are retired unconditionally (#839).**  A failed consumer's
    withdrawn mover holds no tokens and filed no receipt, so the held-key
    pass above can never see it -- yet its fragment still forbids publication
    of its paths, which is a liveness block rather than a capacity question.
    `sweep_dead_owner_fragments` runs once across tiers before the held-key
    pass and retires each exact stale owner through the
    same `evict`, which rechecks co-owners, claims, pins, and handoffs under
    its own locks; a resubmitted consumer is excluded by the locked state
    recheck before eviction, so pressure deference would only preserve the block.

    **``index`` is the tier loop's census, kept from one cycle to the next
    (#992).**  Every census this pass takes -- the dead-owner discovery, the
    receipt-less owner lookup, each tier's reconciliation -- reads it, so a
    namespace or document that has not changed since the last cycle is not
    read again.  Without it each census reads from nothing, as before.

    ``budget`` is the tier loop's cycle budget (#1072), handed to
    `sweep_dead_owner_fragments`, whose per-consumer units are the pass's
    long ones.
    """

    wanted, owners = live_claims(queue)
    swept: list[dict[str, object]] = []
    swept.extend(sweep_dead_owner_fragments(
        queue, stage_roots=stage_roots, residency_root=residency_root,
        index=index, budget=budget))
    # Taken once, and only when a held key has no receipt to name its
    # consumer (#892).
    fragment_owners: dict[str, list[tuple[str, bool]]] | str | None = None
    for tier_id, stage_root in stage_roots.items():
        refusal = stage_root_refusal(queue, stage_root)
        if refusal is not None:
            # One receipt per tier per cycle, and neither the held-key
            # evictions nor the reconciliation run: the root is not this
            # queue's to delete under (#628).  ``evict`` and ``reconcile``
            # refuse on the same fact for the callers that reach them directly.
            swept.append(_refused_receipt(tier_id=tier_id, stage_root=stage_root,
                                          refusal=refusal))
            continue
        try:
            held = queue.tier_ledger(tier_id).held_keys()
        except (OSError, pool.PoolContractError) as exc:
            # Neither pass can run without the held set, and a skip is a
            # record, not a silence (#1007).
            swept.append(_ledger_unreadable_receipt(
                tier_id=tier_id, stage_root=stage_root,
                skipped="held-key pass and reconciliation", exc=exc))
            continue
        orphans: list[tuple[float, str, str]] = []
        retained: list[dict[str, object]] = []
        unresolved: dict[str, str] = {}
        for key in held:
            if key in wanted or key in owners:
                continue
            # A funded produced-output mover is its lane's, receipt or no
            # receipt (#929).  Its receipt names a batch namespace, not a
            # queue action, and its fragment lives in the produced store, so
            # neither rule below can read it: asked of the flat store, a live
            # producer's completed batch looks unowned and would lose its
            # tokens while its bytes stayed.
            verdict = produced_holder(queue, tier_id, key)
            if verdict is not None:
                if verdict["class"] == "dead":
                    # Not pressure-gated: nothing can read this copy again,
                    # so keeping it is not a cache (#598), and its token pins
                    # its funding record (`retire_terminal_output_funding`).
                    outcome = _retire_produced_orphan(
                        queue, key, verdict, tier_id=tier_id,
                        stage_root=stage_root,
                        residency_root=Path(
                            residency_root if residency_root is not None
                            else queue.root / pool.RESIDENCY))
                    if outcome["complete"]:
                        swept.append(outcome)
                    else:
                        unresolved[key] = (
                            "its producer attempt has ended and its batch "
                            "retirement did not complete: "
                            + "; ".join(str(e) for e in outcome["errors"]))
                elif verdict["class"] == "unknown":
                    unresolved[key] = str(verdict["why"])
                continue
            receipt = queue.move_record(key)
            consumer = (str(receipt.get("consumer_action_key")) if isinstance(receipt, dict)
                        else "")
            if not consumer:
                # No receipt names the consumer: ask the fragments (#892).
                if fragment_owners is None:
                    fragment_owners = _held_mover_fragment_owners(
                        Path(residency_root if residency_root is not None
                             else queue.root / pool.RESIDENCY), index)
                consumer, why = _receiptless_owner(fragment_owners, key)
                if consumer:
                    why = _unended_owner(queue, consumer)
                    if why:
                        consumer = ""
                if not consumer:
                    # A live item's own holding is named by a live claim:
                    # kept, and not reported as anybody's mystery (#929).
                    if not _held_by_a_live_item(queue, key, owners):
                        retained.append(_receiptless_refusal(
                            key, tier_id=tier_id, stage_root=stage_root,
                            why=why))
                        unresolved[key] = why
                    continue
            staged_unix = 0.0
            if isinstance(receipt, dict):
                try:
                    staged_unix = float(receipt.get("unix", 0.0) or 0.0)
                except (TypeError, ValueError):
                    staged_unix = 0.0
            orphans.append((staged_unix, key, consumer))
        orphans.sort()
        needed = None if pressure is None else int(pressure.get(tier_id, 0))
        kind = storage_tiers.capacity_kind_of(tier_id)
        for _, key, consumer in orphans:
            if needed is not None:
                if needed <= 0:
                    break      # nothing on this tier is waiting for the room
                try:
                    free = int(queue.tier_ledger(tier_id).available().get(kind, 0))
                except (OSError, pool.PoolContractError):
                    break
                if free >= needed:
                    break      # the window fits now; the rest stays resident
            swept.append(evict(queue, key, consumer_action_key=consumer,
                               stage_root=stage_root,
                               residency_root=residency_root, reason="orphan-sweep"))
        reported: set[str] = set()
        if retained and _still_short(queue, tier_id, kind, needed):
            # A retained receipt-less holder is reported only when keeping it
            # costs something: the tier's window still lacks room after every
            # orphan that could go has gone.  Otherwise it is a quiet wait, as
            # before #892, rather than a line on every cycle.
            swept.extend(retained)
            reported = {str(entry["action_key"]) for entry in retained}
        # And every holder the pass could prove neither live nor an orphan is
        # reported once per change of its reason, pressure or none (#929).
        swept.extend(_unresolved_reports(queue, tier_id, stage_root, unresolved,
                                         reported=reported))
        # Held keys first, then the rest of the stage: the evictions above turn
        # held bytes into absent ones, so the reconciliation below sees the same
        # directory the ledger now describes rather than one eviction behind it.
        # ``wanted`` is joined with what is *still* held, because a pinned mover
        # a live plan no longer names has just been evicted and a pinned one it
        # does name is attribution.
        try:
            still_held = set(queue.tier_ledger(tier_id).held_keys())
        except (OSError, pool.PoolContractError) as exc:
            # Without the held set, a held mover no live plan names is not
            # attribution, and its bytes would read as unowned.  Unknown
            # ownership never deletes: skip the reconciliation this cycle
            # and say so (#1007).
            swept.append(_ledger_unreadable_receipt(
                tier_id=tier_id, stage_root=stage_root,
                skipped="reconciliation", exc=exc))
            continue
        reconciled = reconcile(
            queue, tier_id=tier_id, stage_root=stage_root,
            wanted=wanted | set(owners) | still_held,
            residency_root=residency_root, index=index)
        # Reported only when it has something to report.  A window in flight
        # skips the reconciliation every cycle, and a line per cycle saying so
        # would bury the eviction it exists to announce.
        if (reconciled["entries_deleted"] or reconciled["errors"]
                or reconciled["unowned_left"]):
            swept.append(reconciled)
    return swept


def _held_mover_fragment_owners(root: Path, index: "CensusIndex | None" = None,
                                ) -> dict[str, list[tuple[str, bool]]] | str:
    """Every mover's fragment owners under ``root``, or why that is unknown.

    Maps each mover key to the ``(namespace, direct)`` of every fragment
    naming it.  A tainted census is returned as its reason instead: a
    fragment that cannot be read may be the one that names another owner.
    """

    fragments, tainted = _fragment_census(root, index)
    if tainted:
        return "ownership uncertain: " + "; ".join(tainted[:ATTRIBUTION_TAINT_LIMIT])
    owners: dict[str, list[tuple[str, bool]]] = {}
    for namespace, mover, _fragment, direct in fragments:
        owners.setdefault(mover, []).append((namespace, direct))
    return owners


def _receiptless_owner(owners: dict[str, list[tuple[str, bool]]] | str,
                       mover: str) -> tuple[str, str]:
    """The consumer that owns a held mover with no receipt, or why none can.

    The fragment is the document that names whose bytes a mover staged, so
    exactly one direct fragment is an exact owner.  Once
    :func:`_unended_owner` also proves that owner ended, the orphan sweep
    treats the holder as it treats any orphan, pressure first and ``evict``
    after, and ``evict`` rechecks co-owners, claims, pins and handoffs under
    its own locks.  Anything else is not an owner (#892):

    * no fragment: no document names one.  A funded produced-output mover
      never reaches this question: :func:`produced_holder` decides it first
      (#929), from its funding record;
    * more than one fragment: two documents disagree;
    * a produced-output fragment: its batch's lifecycle owns the mover.
    """

    if isinstance(owners, str):
        return "", owners
    named = owners.get(mover, [])
    if not named:
        return "", "no fragment names this held mover and it has no receipt"
    if len(named) > 1:
        return "", (f"{len(named)} fragments name this held mover: "
                    + ", ".join(sorted(namespace[:12] for namespace, _ in named)))
    namespace, direct = named[0]
    if not direct:
        return "", (f"only a produced-output fragment ({namespace[:12]}) names "
                    f"this held mover; its batch lifecycle owns it")
    return namespace, ""


def _unended_owner(queue: pool.PoolQueue, consumer: str) -> str:
    """Why a fragment-named consumer has not provably ended, or ``""``.

    The fragment names who staged the bytes; only an ending says nobody will
    read them.  The consumer must be neither ready nor claimed, and exactly
    one outcome record must say how it ended: ``done`` (its inputs are
    spent), ``failed``, or ``withdrawn`` with its one immutable decision.  No
    outcome at all is not an ending -- #798's legacy consumer is that shape,
    and a queue whose records are not all visible yet looks the same -- and
    two outcomes are a question, never an answer.
    """

    try:
        live, why = residency_plan.live_state(queue, consumer)
        if why:
            return f"its consumer's queue state is uncertain: {why}"
        if live:
            return f"its consumer {consumer[:12]} is still queued ({live})"
        ended = [state for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN)
                 if not _metadata_absent(queue.item_path(state, consumer))]
        if not ended:
            return f"its consumer {consumer[:12]} has no outcome record"
        if len(ended) > 1:
            return (f"its consumer {consumer[:12]} has {len(ended)} outcome "
                    f"records: {', '.join(ended)}")
        if ended[0] == pool.WITHDRAWN:
            _require_exact_withdrawal(queue, consumer)
        else:
            _owner_record(queue, ended[0], consumer)
    except (OSError, ValueError, pb.PrismaBuildError) as exc:
        return f"its consumer's outcome is unreadable: {exc}"
    return ""


def _still_short(queue: pool.PoolQueue, tier_id: str, kind: str,
                 needed: int | None) -> bool:
    """Whether the tier's window still lacks room after this pass's evictions."""

    if needed is None or needed <= 0:
        return False
    try:
        free = int(queue.tier_ledger(tier_id).available().get(kind, 0))
    except (OSError, pool.PoolContractError):
        return True
    return free < needed


def _receiptless_refusal(mover: str, *, tier_id: str, stage_root: str,
                         why: str) -> dict[str, object]:
    """The receipt an unresolvable receipt-less holder files each pass."""

    return {
        "schema": pool.POOL_EGRESS_SCHEMA_V1, "event": RECEIPTLESS_HOLDER_EVENT,
        "action_key": mover, "consumer_action_key": "", "tier_id": tier_id,
        "stage_root": str(stage_root), "reason": "orphan-sweep",
        "complete": False, "errors": [why],
        "host": socket.gethostname(), "unix": time.time(),
    }


def _is_action_key(value: str) -> bool:
    return len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _held_by_a_live_item(queue: pool.PoolQueue, key: str,
                         owners: Mapping[str, str]) -> bool:
    """Whether a live item holds this key's tokens as its own (#929).

    Two holders a live claim names that are not movers: a live item's own
    reservation -- a producer's output window, say, whose row carries no
    residency and so is not among ``owners`` -- and a window's fence grant,
    held under ``advance-<consumer>-...`` for a consumer that is.  The sweep
    keeps both, as before; this only keeps the reports -- the pressure-driven
    ``stage-receiptless-holder-retained`` and ``stage-holder-unresolved`` --
    from calling them unresolved.
    """

    if key.startswith(window_credit.GRANT_PREFIX):
        prefix = key[len(window_credit.GRANT_PREFIX):].split("-", 1)[0]
        return bool(prefix) and any(owner.startswith(prefix) for owner in owners)
    if not _is_action_key(key):
        return False
    try:
        return any(not _metadata_absent(queue.item_path(state, key))
                   for state in (pool.READY, pool.CLAIMED))
    except OSError:
        return False


def produced_holder(queue: pool.PoolQueue, tier_id: str,
                    mover: str) -> dict[str, object] | None:
    """The produced-output lane's verdict on one held mover (#929), or ``None``.

    ``None`` means no output funding record on this tier names the key: it is
    not a produced mover here, and the ordinary receipt and fragment rules
    apply to it.  A funded mover is the lane's, receipt or no receipt, and
    the answer is one of three classes:

    * ``live``: its producer attempt holds its claim, or the mover itself is
      ready, claimed or in a transition.  The producer may still retire or
      restage the batch, and a queued mover may still copy into the tokens it
      holds;
    * ``dead``: the producer attempt has ended -- ``dead`` (failed, withdrawn
      or superseded) or ``succeeded`` without retiring this batch -- and so
      has the mover, and its funding is ``consumed``.  Nothing can read the
      copy again: a producer's retry binds a new instance, and with it a new
      batch namespace (#912).  The verdict carries what ``retire_batch``
      needs;
    * ``unknown``: anything else, with the reason.  An absent producer is
      unknown, not dead: no outcome at all is not an ending (#798), and a
      queue whose records are not all visible yet looks the same.

    Every record is read at its own address -- the funding record, the
    instance and template it names, the two keys' queue states -- so the cost
    is a handful of reads per funded holder, and none for any other key.
    """

    if not _is_action_key(mover):
        return None

    def unknown(why: str) -> dict[str, object]:
        return {"class": "unknown", "why": why}

    try:
        record, file_state = queue.output_funding_file_state(mover, tier_id)
    except (OSError, ValueError, pool.PoolContractError) as exc:
        return unknown(f"its output funding is unreadable: {exc}")
    if file_state == "absent":
        return None
    if file_state != "ok" or not isinstance(record, Mapping):
        return unknown(f"its output funding record is {file_state}")
    owner = str(record.get("owner_action_key") or "")
    nonce = str(record.get("owner_nonce") or "")
    template_id = str(record.get("template_id") or "")
    batch_id = str(record.get("batch_id") or "")
    if (str(record.get("mover_action_key") or "") != mover
            or str(record.get("tier_id") or "") != tier_id
            or not _is_action_key(owner) or not nonce or not template_id
            or not batch_id):
        return unknown("its output funding record does not name this mover's batch")
    residency = queue.root / pool.RESIDENCY
    scope = (residency / produced_output.OUTPUT_SCOPES_SUBDIR / owner
             / f"{template_id}.{nonce}")
    try:
        instance = produced_output.validate_instance(
            json.loads((scope / "instance.json").read_text()))
    except (OSError, ValueError) as exc:
        return unknown(f"its producer's instance is unreadable: {exc}")
    try:
        template = produced_output.validate_template(json.loads(
            (residency / produced_output.OUTPUT_TEMPLATES_SUBDIR
             / f"{template_id}.json").read_text()))
    except (OSError, ValueError) as exc:
        return unknown(f"its producer's template is unreadable: {exc}")
    attempt = instance.get("owner_attempt")
    if (produced_output.instance_dir(queue.root, instance) != scope
            or not isinstance(attempt, Mapping)
            or str(attempt.get("nonce")) != nonce
            or str(attempt.get("scope_id")) != str(record.get("owner_scope_id"))
            or produced_output.template_sha256(template)
            != instance.get("template_sha256")
            or str(record.get("template_sha256")) != instance.get("template_sha256")):
        return unknown("its funding, instance and template disagree")
    try:
        producer = produced_output._producer_attempt_state(queue, instance)
        mover_state, _row = produced_output._key_generation(queue, mover)
    except (OSError, ValueError, pool.PoolContractError) as exc:
        return unknown(f"a queue state is unreadable: {exc}")
    if producer == "live":
        return {"class": "live",
                "why": f"its producer attempt {owner[:12]} holds its claim"}
    if mover_state in (pool.READY, pool.CLAIMED, "moving"):
        return {"class": "live", "why": f"the mover itself is {mover_state}"}
    if producer not in ("dead", "succeeded"):
        return unknown(f"its producer attempt {owner[:12]} is {producer}")
    if mover_state not in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
        return unknown(f"the mover's own queue state is {mover_state}")
    if str(record.get("state")) != "consumed":
        return unknown(f"its output funding is {record.get('state')}, not consumed")
    try:
        commitments = produced_output._read_commitments(
            scope / "commitments.json")
        entry = commitments["batches"].get(batch_id)   # type: ignore[union-attr]
        if not isinstance(entry, Mapping):
            return unknown(f"its batch {batch_id} is not committed")
        active = produced_output._active_materialization(entry)
    except (OSError, ValueError) as exc:
        return unknown(f"its batch's commitments are unreadable: {exc}")
    if str(active.get("mover_key") or "") != mover:
        return unknown(f"its batch {batch_id}'s active copy is another mover's")
    if active.get("retired"):
        return unknown(f"its batch {batch_id} is retired and it still holds tokens")
    return {"class": "dead", "why": f"its producer attempt {owner[:12]} is {producer}",
            "instance": instance, "template": template, "batch_id": batch_id,
            "producer_action_key": owner, "producer_state": producer}


def _retire_produced_orphan(queue: pool.PoolQueue, mover: str,
                            verdict: Mapping[str, object], *, tier_id: str,
                            stage_root: str, residency_root: Path) -> dict[str, object]:
    """Run the dead producer's ``retire_batch`` for it, and say what happened.

    The lane's own retirement, never the flat orphan path: it validates the
    batch record against its commitments, runs the ordinary ``evict`` on the
    batch's fragment root -- co-owners, claims, pins and handoffs rechecked
    under its locks, files deleted only against their fragment's identity --
    and files the batch ``retired``.  A mover that never published a fragment
    staged nothing the egress can name, so its tokens come back and any
    unfragmented bytes are the reconciliation's, as for the producer's own
    call (``test_a_mover_killed_before_filing_anything_keeps_its_charge``).
    """

    # Only in this process: off the tier host `retire_batch` would publish an
    # egress action, and a sweep publishes nothing.  The tier loop runs on the
    # tier host, so this is the case there.
    if not produced_output._egress_runs_in_process(
            produced_output._announced_tier_record(queue, tier_id)):
        result: dict[str, object] = {
            "ok": False, "refusal": "this sweep is not on the tier host"}
    else:
        result = produced_output.retire_batch(
            queue, verdict["instance"], verdict["template"],  # type: ignore[arg-type]
            str(verdict["batch_id"]), stage_root=str(stage_root),
            residency_root=produced_output.output_fragment_root(residency_root))
    egress = result.get("receipt") if isinstance(result.get("receipt"), Mapping) else {}
    errors: list[str] = []
    if result.get("ok") is not True:
        errors.append(str(result.get("refusal") or "retirement refused"))
        if isinstance(egress, Mapping):
            errors.extend(str(error) for error in egress.get("errors") or [])
    elif result.get("duplicate"):
        errors.append("its batch was already retired")
    return {
        "schema": pool.POOL_EGRESS_SCHEMA_V1, "event": PRODUCED_ORPHAN_EVENT,
        "action_key": mover,
        "consumer_action_key": str((egress or {}).get("consumer_action_key") or ""),
        "tier_id": tier_id, "stage_root": str(stage_root),
        "reason": "dead-producer", "batch_id": str(verdict["batch_id"]),
        "producer_action_key": str(verdict["producer_action_key"]),
        "producer_state": str(verdict["producer_state"]),
        "entries_deleted": int((egress or {}).get("entries_deleted") or 0),
        "bytes_deleted": int((egress or {}).get("bytes_deleted") or 0),
        "tokens_released": int((egress or {}).get("tokens_released") or 0),
        "complete": not errors, "errors": errors,
        "host": socket.gethostname(), "unix": time.time(),
    }


def _unresolved_reports(queue: pool.PoolQueue, tier_id: str, stage_root: str,
                        unresolved: dict[str, str], *,
                        reported: set[str]) -> list[dict[str, object]]:
    """The operator report: each unresolved holder once per change of reason.

    ``reported`` names holders this pass already reported another way (the
    pressure-driven ``stage-receiptless-holder-retained`` receipt); they are
    recorded but not repeated.  A holder that resolves, or leaves the tier,
    is forgotten, so it is reported again if it comes back.
    """

    seen = _UNRESOLVED_REPORTS.setdefault((str(queue.root), tier_id), {})
    for key in [key for key in seen if key not in unresolved]:
        del seen[key]
    events: list[dict[str, object]] = []
    for key, why in sorted(unresolved.items()):
        if seen.get(key) == why:
            continue
        seen[key] = why
        if key in reported:
            continue
        events.append({
            "schema": pool.POOL_EGRESS_SCHEMA_V1, "event": HOLDER_UNRESOLVED_EVENT,
            "action_key": key, "consumer_action_key": "", "tier_id": tier_id,
            "stage_root": str(stage_root), "reason": "orphan-sweep",
            "complete": False, "errors": [why],
            "host": socket.gethostname(), "unix": time.time(),
        })
    return events


def reset_holder_reports() -> None:
    """Forget which unresolved holders were reported (tests, and restarts)."""

    _UNRESOLVED_REPORTS.clear()


def _is_mover_partial(name: str) -> bool:
    return name.startswith(PARTIAL_PREFIX) and name.endswith(PARTIAL_SUFFIX)


def _marked_by_the_prewarm_stage(path: Path) -> bool | None:
    """Whether this file is a prewarm stage object, or ``None`` if unanswerable.

    The prewarm loop stages into the same pool, and its objects carry
    ``user.pbstage.source`` set on the handle before the rename, so a completed
    one is always marked.  ``None`` is the case that matters: a filesystem
    without user extended attributes still stages, and there "no mark" would
    read as "not the prewarm loop's" for every object it owns.  So the answer is
    unknown rather than false, and the caller refuses to delete on it.
    """

    try:
        os.getxattr(path, prewarm_loop.STAGE_SOURCE_XATTR)
    except OSError as exc:
        # The attribute is absent: an answer, and it is "no".
        if exc.errno in _XATTR_ABSENT:
            return False
        # Anything else -- the filesystem does not carry user attributes, the
        # call is not permitted -- is not an answer.
        return None
    return True


#: How many census taints one receipt names before it counts the rest.
ATTRIBUTION_TAINT_LIMIT = 8


def _attributed_census(queue: pool.PoolQueue, *, wanted: set[str] | None,
                       residency_root: str | Path | None = None,
                       memo: _CensusMemo | None = None,
                       ) -> tuple[set[str], list[str]]:
    """The strict attribution census behind :func:`attributed_stage_paths`.

    Same selection, with the taint channel a deletion pass needs: a caller
    that deletes must retain when ownership is unknown, and a fragment the
    reader tolerance skipped is unknown ownership, not an unowned file.
    """

    root = Path(residency_root if residency_root is not None
                else queue.root / pool.RESIDENCY)
    fragments, tainted = _fragment_census(root, memo)
    out: set[str] = set()
    for _namespace, mover, fragment, _direct in fragments:
        if wanted is not None and mover not in wanted:
            continue
        named = (memo.normalized_paths_of(fragment) if memo is not None
                 else None)
        if named is not None:
            out.update(named)
            continue
        for entry in dict(fragment["entries"]).values():
            out.add(os.path.normpath(str(entry["stage_path"])))
    return out, tainted


def attributed_stage_paths(queue: pool.PoolQueue, *, wanted: set[str] | None,
                           residency_root: str | Path | None = None) -> set[str]:
    """Every stage path a mover the fleet still wants has vouched for.

    ``wanted=None`` means *every* fragment counts, wanted or not.  The
    reconciliation cannot use that -- a withdrawn mover's fragment is exactly
    what it exists to clear -- but a bounded repair can, and prefers to: there
    the question is not "whose bytes are these" but "is there any reason at
    all to keep them", and one more retained file is a pass.

    Read off the fragments, because a fragment is the only document that says
    "this file is that mover's": the plan names ranges and the ledger names
    keys, and neither can be compared against a directory entry.  Only the
    fragments of ``wanted`` movers count.  A withdrawn mover's fragment is still
    on disk -- nothing deletes it, since no egress ran -- and treating it as
    attribution is exactly how its bytes became invisible.

    Both fragment layouts count: legacy flat consumers and the nested
    produced-output namespaces (see :func:`_fragment_census`).  This is the
    paths-only view; a caller that deletes reads :func:`_attributed_census`
    and retains on its taint instead.
    """

    out, _taint = _attributed_census(queue, wanted=wanted,
                                     residency_root=residency_root)
    return out


def reconcile(queue: pool.PoolQueue, *, tier_id: str, stage_root: str,
              wanted: set[str], residency_root: str | Path | None = None,
              index: CensusIndex | None = None,
              ) -> dict[str, object]:
    """Evict what is on the stage that nothing on the fleet accounts for.

    The half of #608 the held-key sweep structurally cannot do: it walks the
    tier ledger's held keys, so bytes no key holds are invisible to it forever.
    A withdrawn mid-copy mover leaves exactly that -- the shards it had already
    verified and renamed into place, and its ``.partial`` temporaries -- and its
    tokens come back when its worker stops it, so the ledger reads its full
    supply free over 130 GB of occupied stage.

    Three rules decide, and each one is a fact rather than a policy:

    * **Nothing is deleted while a mover could be writing.**  A mover ready or
      claimed on this tier means a copy is in flight or about to be, and its
      destination is not in any fragment until it has been verified.  So the
      whole reconciliation is skipped for that tier, named in the receipt.  A
      per-file test cannot replace this: a ready mover can be claimed between
      the walk and the unlink.
    * **A mover's own temporary is always its own.**  ``.<name>.partial`` is
      ``stage_move``'s naming and nothing else writes it, so one that no live
      copy is producing is the residue of a killed or withdrawn one.
    * **Anything else must be shown to be unowned.**  The prewarm loop stages
      into this same pool and marks its objects with ``user.pbstage.source``, so
      an unmarked file that no wanted mover's fragment names is unowned.  Where
      the filesystem cannot answer the xattr question at all, "unmarked" means
      nothing, and the file is left alone with the reason in the receipt.

    **Unknown ownership never deletes.**  The attribution census is strict
    (see :func:`_fragment_census`): a fragment that cannot be read or
    validated, or a residency directory that is neither reserved bookkeeping
    nor a namespace shape, returns an incomplete receipt with bounded reasons
    and no unlinks, and the tier cycle continues.  The reader tolerance that
    skips a bad fragment is for a consumer composing its map, never for a
    pass that deletes.

    Ownership is checked the way ``evict`` checks it: the resolved path must be
    under the resolved stage root.
    """

    stage = Path(stage_root)
    receipt: dict[str, object] = {
        "schema": pool.POOL_EGRESS_SCHEMA_V1,
        "event": UNATTRIBUTED_EVENT,
        "action_key": "",
        "consumer_action_key": "",
        "tier_id": tier_id,
        "stage_root": str(stage),
        "reason": "unattributed-reconcile",
        "entries_deleted": 0,
        "entries_already_gone": 0,
        "bytes_deleted": 0,
        "tokens_released": 0,
        "partials_deleted": 0,
        "unowned_left": 0,
        "left_since_walk": 0,
        "complete": True,
        "errors": [],
        "host": socket.gethostname(),
        "unix": time.time(),
    }
    refusal = stage_root_refusal(queue, stage)
    if refusal is not None:
        # Whose stage this is comes before what is on it (#628).
        receipt["event"] = STAGE_ROOT_REFUSED_EVENT
        receipt["skipped"] = refusal
        receipt["complete"] = False
        receipt["errors"] = [refusal]
        return receipt
    # The walk and the censuses go first, without the lock (#988).  The walk
    # names candidates; nothing it decides is acted on.  Under the lock,
    # every candidate is judged again against a fresh mover check, a fresh
    # attribution census and a fresh pin census, and unlinked only while its
    # ``lstat`` identity still equals the one the walk saw: a file replaced,
    # rewritten or re-marked (``setxattr`` moves ctime) since is left for the
    # next pass.  A file that appears after the walk is not a candidate on
    # this pass at all.  So the rules below are decided under the lock, as
    # before; only the listing and the per-file classification left it.
    early = movers_in_flight(queue, tier_id=tier_id)
    if early:
        # A pre-check only: the answer that counts is taken under the lock.
        # Skipping on it costs nothing, since the pass would skip anyway.
        receipt["skipped"] = "movers_in_flight"
        receipt["movers_in_flight"] = sorted(early)
        return receipt
    try:
        stage_resolved = stage.resolve(strict=True)
    except OSError as exc:
        receipt["skipped"] = f"stage_root_unreadable: {exc}"
        receipt["complete"] = False
        return receipt
    # The tier loop's index when it passes one (#992): then neither census
    # below re-reads a namespace or document unchanged since the last cycle,
    # and the one under the lock compares each directory's stamp again
    # rather than listing it (:class:`CensusIndex`).  Reader pins are read
    # fresh by every call either way.
    memo: _CensusMemo = index if index is not None else _CensusMemo()
    if index is not None:
        index.fresh_pins()
    census_started = time.perf_counter()
    hinted, _hint_taint = _attributed_census(
        queue, wanted=wanted, residency_root=residency_root, memo=memo)
    hint_pins, _hint_pin_taint = reader_lease.live_for(
        queue, None, residency_root=residency_root, memo=memo.pins)
    hinted |= set(hint_pins)
    candidates, walk_errors = _unattributed_candidates(
        stage, stage_resolved, hinted)
    census_s = time.perf_counter() - census_started
    emptied: set[Path] = set()

    def _held_reconcile() -> None:
        """The decision and the act, under the stage ownership lock."""

        validate_started = time.perf_counter()
        parse_counts = memo.counts()
        in_flight = movers_in_flight(queue, tier_id=tier_id)
        if in_flight:
            receipt["skipped"] = "movers_in_flight"
            receipt["movers_in_flight"] = sorted(in_flight)
            return
        attributed, attribution_taint = _attributed_census(
            queue, wanted=wanted, residency_root=residency_root, memo=memo)
        if attribution_taint:
            # A fragment that cannot be read, or a directory that cannot be
            # classified, is unknown ownership -- never an unowned file.  The
            # pass deletes nothing, says why with bounded reasons, and the
            # tier cycle continues; the next sweep retries once the state is
            # readable again.
            receipt["skipped"] = "attribution_unreadable"
            receipt["complete"] = False
            receipt["errors"] = [
                f"ownership uncertain: {item}"
                for item in attribution_taint[:ATTRIBUTION_TAINT_LIMIT]]
            if len(attribution_taint) > ATTRIBUTION_TAINT_LIMIT:
                receipt["errors"].append(
                    f"ownership uncertain: {len(attribution_taint)} entry(ies) "
                    f"unreadable")
            return
        pin_owners, pin_taint = reader_lease.live_for(
            queue, None, residency_root=residency_root, memo=memo.pins)
        if pin_taint:
            receipt["complete"] = False
            receipt["errors"] = [
                f"ownership uncertain: {item}" for item in pin_taint]
            return
        # A live pin is attribution: unattributed bytes nobody accounts for
        # go, pinned bytes never do.
        attributed |= set(pin_owners)
        receipt["census_validate_s"] = round(
            time.perf_counter() - validate_started, 6)
        receipt.update(_locked_parse_record(memo, parse_counts))
        receipt["entries_judged"] = len(candidates)
        deleted = bytes_deleted = partials = left = 0
        errors: list[str] = list(walk_errors)
        for path, identity, partial in candidates:
            if os.path.normpath(str(path)) in attributed:
                left += 1   # an owner or a pin named it after the walk
                continue
            try:
                info = os.lstat(path)
            except FileNotFoundError:
                continue
            except OSError as exc:
                errors.append(f"{path.name}: {exc}")
                continue
            if (not statmod.S_ISREG(info.st_mode)
                    or _metadata_version(info) != identity):
                left += 1   # changed since the walk: the next pass decides
                continue
            try:
                os.unlink(path)
            except FileNotFoundError:
                continue
            except OSError as exc:
                errors.append(f"{path.name}: {exc}")
                continue
            deleted += 1
            bytes_deleted += int(info.st_size)
            partials += 1 if partial else 0
            emptied.add(path.parent)
        receipt["entries_deleted"] = deleted
        receipt["bytes_deleted"] = bytes_deleted
        receipt["partials_deleted"] = partials
        receipt["unowned_left"] = unowned_left
        # Candidates the walk found that the locked re-check kept: a
        # fragment or pin named them, or the file changed, after the walk.
        receipt["left_since_walk"] = left
        receipt["errors"] = errors
        receipt["complete"] = not errors

    unowned_left = sum(1 for _path, identity, _partial in candidates
                       if identity is None)
    candidates = [one for one in candidates if one[1] is not None]
    asked = time.perf_counter()
    with queue.stage_ownership_lock(str(stage)):
        granted = time.perf_counter()
        _held_reconcile()
    released = time.perf_counter()
    pruned_started = time.perf_counter()
    for parent in sorted(emptied, key=lambda one: len(one.parts), reverse=True):
        _prune_empty(parent, stage)
    receipt.update(_hold_record(
        lock_wait_s=granted - asked, lock_held_s=released - granted,
        census_s=census_s, prune_s=time.perf_counter() - pruned_started))
    return receipt


def _unattributed_candidates(stage: Path, stage_resolved: Path,
                             attributed: set[str],
                             ) -> tuple[list[tuple[Path, tuple | None, bool]],
                                        list[str]]:
    """The files :func:`reconcile` may delete, found without the lock (#988).

    The walk and every per-file rule of the reconciliation: the root's own
    markers are skipped, a symlink or non-regular file is skipped, an
    attributed path is skipped, a prewarm temporary is skipped, a file the
    prewarm stage marked -- or whose mark cannot be read -- is left, and a
    file that would be deleted but resolves outside the stage is skipped.

    Containment is checked only for that last kind, a file about to carry an
    identity (#1073).  It guards a deletion and nothing else, and resolving
    every file of the live stage (54,400, 38,422 of them marked and only
    counted) took 48% of the tier loop.  It still runs after the ``lstat``
    that captures the identity: a directory swapped for a symlink before that
    ``lstat`` is caught here, and one swapped after it changes the identity
    the locked re-check compares, which never re-checks containment.  Returns
    ``(candidates, errors)``: each candidate as ``(path, identity,
    partial)`` where ``identity`` is the ``lstat`` version the caller must
    see again under the lock, or ``None`` for a file left as unowned-but-
    marked (counted, never deleted).
    """

    candidates: list[tuple[Path, tuple | None, bool]] = []
    errors: list[str] = []
    # ``stage_resolved in resolved.parents``, as a string test: the resolved
    # path lies strictly below the resolved stage.
    inside = os.path.join(str(stage_resolved), "")
    for base, _directories, names in os.walk(stage):
        for name in sorted(names):
            path = Path(base) / name
            if name == STAGE_ROOT_MARKER and Path(base) == stage:
                # The root's own ownership marker: unmarked by the prewarm
                # stage and named by no fragment, so without this line the
                # sweep would delete the fact that lets it sweep.
                continue
            if (name == storage_tiers.RAM_EPOCH_MARKER and Path(base) == stage):
                # The ram root's epoch marker, same rule one tier over
                # (#640): unmarked, unnamed, and the one file that dates
                # every ram range this queue admits.  Deleting it would mint
                # a new epoch and drop every resident range the tmpfs still
                # holds.
                continue
            try:
                info = os.lstat(path)
                if not statmod.S_ISREG(info.st_mode):
                    continue
            except OSError as exc:
                errors.append(f"{path.name}: {exc}")
                continue
            if os.path.normpath(str(path)) in attributed:
                continue
            partial = _is_mover_partial(name)
            if not partial:
                if prewarm_loop._STAGE_TEMPORARY.search(name):
                    # The prewarm loop reaps its own, once per process, and a
                    # live one belongs to a copy in flight.
                    continue
                marked = _marked_by_the_prewarm_stage(path)
                if marked is None or marked:
                    candidates.append((path, None, False))
                    continue
            try:
                if not str(path.resolve()).startswith(inside):
                    continue
            except OSError as exc:
                errors.append(f"{path.name}: {exc}")
                continue
            candidates.append((path, _metadata_version(info), partial))
    return candidates, errors

def _scope_for_range(cas_root: str, manifest_sha256: str,
                     start: int, end: int,
                     ) -> tuple[set[str], list[dict[str, object]], str] | None:
    """The staged names one historical range covers, by the usual mapping.

    The same derivation ``_claimed_paths`` runs for a claim in flight, driven
    from a pinned manifest digest and range instead of a sealed claim: the
    manifest is immutable under its digest, so the names it yields are a fact
    about that request and not about anything currently on the tier.  Returns
    ``None`` when the manifest or the window cannot be read, which the caller
    turns into a refusal -- an undeterminable scope never reads as empty.
    """

    layout = _cached_manifest_layout(cas_root, manifest_sha256)
    if layout is None:
        return None
    mount_prefix, entries = layout
    try:
        window = prewarm_loop.entries_between(entries, start, end)
    except (ValueError, TypeError):
        return None
    names: set[str] = set()
    for entry in window:
        path, offset = str(entry["path"]), int(entry["offset"])
        try:
            names.add(stage_relative(
                path, offset, int(entry["bytes"]),
                mount_prefix=mount_prefix))
        except ValueError:
            continue
    return names, list(window), mount_prefix


def _bounded_int(value: object) -> int | None:
    """A nonnegative integer that is not a bool, or ``None``."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _move_receipt(queue: pool.PoolQueue, action_key: str,
                  ) -> tuple[dict[str, object] | None, str]:
    """One filed move receipt, or why it cannot be evidence.

    The receipt is the positive terminal evidence this repair turns on.  An
    absent one is not "finished": it is no answer, and no answer refuses.
    """

    try:
        record = pool._read_json(queue.move_path(action_key))
    except (OSError, pool.PoolContractError) as exc:
        return None, f"{action_key[:12]}: receipt unreadable: {exc}"
    if not isinstance(record, Mapping):
        return None, f"{action_key[:12]}: no filed move receipt"
    if str(record.get("action_key") or "") != action_key:
        return None, f"{action_key[:12]}: receipt is filed under another " \
                     f"action's key"
    if record.get("schema") != pool.POOL_MOVE_SCHEMA_V1:
        return None, f"{action_key[:12]}: receipt is not a pool move record"
    if record.get("complete") is not True:
        return None, f"{action_key[:12]}: receipt does not report complete"
    return dict(record), ""


def _sealed_command_flags(
        sealed: Mapping[str, object], *,
        required: tuple[str, ...],
        forbidden: tuple[str, ...]) -> tuple[dict[str, str] | None, str]:
    """Read named flags out of a sealed request's argv, as list elements.

    The command is read positionally and never re-parsed as a shell string.
    Each named flag must appear exactly once: these requests are built by a
    tool, so a duplicate is a real signal about how this one was built, not a
    quirk to tolerate.  Only the named values are returned, so no caller can
    print the whole argv.
    """

    params = sealed.get("params")
    if not isinstance(params, Mapping):
        return None, "carries no params"
    command = params.get("command")
    if not isinstance(command, list) or not command:
        return None, "carries no command list"
    if not all(isinstance(one, str) for one in command):
        return None, "command is not a list of strings"
    out: dict[str, str] = {}
    for flag in tuple(required) + tuple(forbidden):
        seen = [i for i, one in enumerate(command) if one == flag]
        if len(seen) > 1:
            return None, f"names {flag} {len(seen)} times"
        if flag in forbidden:
            if seen:
                return None, f"names {flag}, which carries no authority here"
            continue
        if not seen:
            return None, f"names no {flag}"
        after = seen[0] + 1
        if after >= len(command):
            return None, f"{flag} names no value"
        out[flag] = command[after]
    return out, ""


def _validated_sealed_request(
        cas_root: str, action_key: str,
        ) -> tuple[dict[str, object] | None, str]:
    """A historical CAS request proven to be the action asked for.

    ``prewarm_loop.sealed_request`` is a bare JSON load: any bytes at that
    path answer for the sealed request, including a hand-edited body wearing
    the key it is read under.  This repair treats the request's flags as the
    authority for a destructive scope, so it applies the same sequence the
    pool applies to a claimed action -- stable read, ``validate_action``
    over the whole v2 contract, and the recorded key equal to the key asked
    for -- and no bytes are re-hashed beyond that: the action key is the
    digest work, and no payload is touched.
    """

    path = (Path(cas_root) / "requests" / action_key[:2]
            / f"{action_key}.json")
    try:
        raw = pb._read_regular_file_nofollow(
            path, where="orphan recovery action request")
        action = pb.validate_action(pb._decode_strict_json(
            raw, where="orphan recovery action request"))
    except FileNotFoundError:
        return None, f"{action_key[:12]}: not sealed in the CAS"
    except (OSError, ValueError, pb.PrismaBuildError) as exc:
        return None, f"{action_key[:12]}: not a validly sealed action: {exc}"
    if str(action.get("action_key") or "") != action_key:
        return None, f"{action_key[:12]}: sealed for another action"
    return action, ""


def recover_orphaned_range(
        queue: pool.PoolQueue, *, stage_root: str,
        head_action_key: str, egress_action_key: str,
        cas_root: str | Path | None = None,
        residency_root: str | Path | None = None,
        apply: bool = False) -> dict[str, object]:
    """Retire the cache copies of one retired head that nothing can prove.

    Routine ``reconcile`` structurally cannot reach these.  It decides
    ownership from the ``user.pbstage.source`` mark, and ``stage_move`` sets
    that same mark on every file it publishes, so a stage copy always reads as
    prewarm-owned and lands in ``unowned_left`` for the life of the fleet.  A
    head whose fragment and material are gone therefore leaves bytes no
    document proves and no sweep may touch, and every later head pays the
    publisher grace once per entry for them.

    Scope is bound to history, not to the caller.  The only identities taken
    are the head's own filed move receipt and the egress receipt that retired
    it; the tier, the stage root, the consumer, the manifest and the exact
    range are **read off those receipts**, so there is no caller-supplied
    digest or window to get wrong.  Both must be filed, well-formed and
    ``complete``: absence is not terminal evidence, it is no evidence.  Each
    historical request is then read as an action and validated under its own
    key (``validate_action`` plus the key comparison) before anything is
    derived from it -- a bare JSON load would let any bytes at that path
    answer for the sealed request -- and the surviving flag checks bind the
    two requests to each other and to the receipts: the egress must name the
    head as its mover, both must name the same consumer and stage root, the
    head alone carries tier and range, and a filed receipt may not widen the
    window its request authorized.

    Permission is that identity-bound scope **plus** the positive absence of
    every other ownership, established fresh under the lock the egress holds.
    The old source mark is never permission -- only a necessary condition, so
    an unmarked or unanswerable file is retained.  Every fragment retains,
    wanted or not -- a fragment that cannot be read or validated refuses the
    whole pass instead of reading as absent, and a withdrawn mover's fragment
    retains like any other; live pins, claims in flight and promotion source
    handoffs retain; and any census that cannot be read **refuses the pass**
    rather than reading as absence.

    Originals are proven before anything is destroyed: every entry the window
    names must still exist at its source path as a **regular file** that
    holds at least the ``offset + bytes`` extent the manifest gives it, and
    that path must resolve outside the stage root, so a staged copy is never
    the last surviving input and a directory or a shrunken source is not an
    input at all.  Ambiguity retains, and any refused or unreadable entry
    fails the whole pass -- over-retaining is a pass and over-removing is a
    failure.

    ``apply`` is false by default: the pass reports what it *would* retire and
    changes nothing.
    """

    stage = Path(stage_root)
    receipt: dict[str, object] = {
        "schema": pool.POOL_EGRESS_SCHEMA_V1,
        "event": ORPHAN_RECOVERY_EVENT,
        "action_key": "",
        "head_action_key": head_action_key,
        "egress_action_key": egress_action_key,
        "consumer_action_key": "",
        "tier_id": "",
        "stage_root": str(stage),
        "reason": "orphan-recovery",
        "applied": bool(apply),
        "scope_entries": 0,
        "entries_eligible": 0,
        "entries_retired": 0,
        "entries_retained": 0,
        "entries_already_gone": 0,
        "entries_refused": 0,
        "bytes_eligible": 0,
        "bytes_retired": 0,
        "retained_reasons": {},
        "originals_checked": 0,
        "originals_present": 0,
        "complete": True,
        "errors": [],
        "host": socket.gethostname(),
        "unix": time.time(),
    }

    def refuse(why: str) -> dict[str, object]:
        receipt["skipped"] = why
        receipt["complete"] = False
        errs = list(receipt["errors"])
        errs.append(why)
        receipt["errors"] = errs
        return receipt

    # Whose stage this is comes before anything else on it (#628): a root
    # that does not belong to this queue is refused before its receipts are
    # even read.
    refusal = stage_root_refusal(queue, stage)
    if refusal is not None:
        receipt["event"] = STAGE_ROOT_REFUSED_EVENT
        return refuse(refusal)

    head, why = _move_receipt(queue, head_action_key)
    if head is None:
        return refuse(f"head evidence refused: {why}")
    egress, why = _move_receipt(queue, egress_action_key)
    if egress is None:
        return refuse(f"egress evidence refused: {why}")
    if egress.get("reason") != "egress":
        return refuse(
            f"{egress_action_key[:12]} is not an egress receipt")

    consumer = str(head.get("consumer_action_key") or "")
    tier_id = str(head.get("tier_id") or "")
    manifest_sha256 = str(head.get("manifest_sha256") or "")
    start = _bounded_int(head.get("range_start_bytes"))
    end = _bounded_int(head.get("range_end_bytes"))
    if not consumer or not tier_id or not manifest_sha256:
        return refuse("the head receipt names no consumer, tier or manifest")
    if start is None or end is None or end <= start:
        return refuse("the head receipt names no usable byte range")
    if str(egress.get("consumer_action_key") or "") != consumer:
        return refuse("the egress retired a different consumer's window")
    for label, record in (("head", head), ("egress", egress)):
        named = str(record.get("stage_root") or "")
        if named and os.path.normpath(named) != os.path.normpath(str(stage)):
            return refuse(
                f"the {label} receipt names stage root {named}, not {stage}")
    own_cas_early = (str(cas_root) if cas_root is not None
                     else str(queue.root.parent / "cas"))
    flags: dict[str, dict[str, str]] = {}
    for label, key, want, deny in (
            ("head", head_action_key,
             ("--consumer-action-key", "--tier-id", "--stage-root",
              "--range-start-bytes", "--range-end-bytes"),
             ("--mover-action-key",)),
            ("egress", egress_action_key,
             ("--mover-action-key", "--consumer-action-key", "--stage-root"),
             ("--tier-id", "--range-start-bytes", "--range-end-bytes"))):
        sealed, why = _validated_sealed_request(own_cas_early, key)
        if sealed is None:
            return refuse(f"the {label} request {why}; its scope cannot "
                          f"be bound")
        params = sealed.get("params")
        declared = (params.get("data_manifest")
                    if isinstance(params, Mapping) else None)
        named = (declared.get("input") if isinstance(declared, Mapping)
                 else None)
        digest = (named.get("sha256") if isinstance(named, Mapping) else None)
        if digest != manifest_sha256:
            return refuse(
                f"the sealed {label} request names manifest "
                f"{str(digest)[:12]}, not the {manifest_sha256[:12]} its "
                f"receipt recorded")
        # Manifest equality is far too weak on its own.  Both phases of one
        # campaign share a digest, so it would admit the whole manifest
        # against authority for one phase's window.  The flags are what
        # actually scope this.
        got, why = _sealed_command_flags(sealed, required=want, forbidden=deny)
        if got is None:
            return refuse(f"the sealed {label} request {why}")
        flags[label] = got

    if flags["egress"]["--mover-action-key"] != head_action_key:
        return refuse(
            f"the sealed egress request retires mover "
            f"{flags['egress']['--mover-action-key'][:12]}, not the head "
            f"{head_action_key[:12]} this recovery is scoped to")
    for label in ("head", "egress"):
        if flags[label]["--consumer-action-key"] != consumer:
            return refuse(
                f"the sealed {label} request names consumer "
                f"{flags[label]['--consumer-action-key'][:12]}, not the "
                f"{consumer[:12]} its receipt recorded")
        named = flags[label]["--stage-root"]
        if os.path.normpath(named) != os.path.normpath(str(stage)):
            return refuse(
                f"the sealed {label} request names stage root {named}, "
                f"not {stage}")
    if flags["head"]["--tier-id"] != tier_id:
        return refuse(
            f"the sealed head request names tier {flags['head']['--tier-id']},"
            f" not the {tier_id} its receipt recorded")
    try:
        sealed_start = int(flags["head"]["--range-start-bytes"])
        sealed_end = int(flags["head"]["--range-end-bytes"])
    except ValueError:
        return refuse("the sealed head request names an unreadable range")
    if sealed_start < 0 or sealed_end <= sealed_start:
        return refuse("the sealed head request names no usable byte range")
    if (sealed_start, sealed_end) != (start, end):
        return refuse(
            f"the head receipt claims {start}..{end} but its sealed request "
            f"authorizes only {sealed_start}..{sealed_end}")
    # Equal by the check above; take them from the sealed request anyway, so
    # the authorized window is the one a filed receipt cannot widen.
    start, end = sealed_start, sealed_end
    receipt["scope_authority"] = "sealed_head_request"
    receipt["consumer_action_key"] = consumer
    receipt["tier_id"] = tier_id
    receipt["manifest_sha256"] = manifest_sha256
    receipt["range_start_bytes"] = start
    receipt["range_end_bytes"] = end

    own_cas = (str(cas_root) if cas_root is not None
               else str(queue.root.parent / "cas"))

    def _scope_outside_the_lock() -> dict[str, object]:
        """The range's scope and its originals, which no lock guards (#988).

        The manifest window, the staged names it derives and the originals
        it proves recapturable are read from immutable CAS records and from
        source files outside the stage.  None of it is state the stage
        ownership lock orders, and the originals are stats on the shared
        mount, one or more NFS round trips per entry, so they no longer run
        under it.  A refusal found here is returned by the pass under the
        lock, after its own head, consumer and mover checks, so the order
        in which refusals are reported is unchanged.
        """

        fields: dict[str, object] = {}

        def scoped(why: str) -> dict[str, object]:
            return {"refusal": why, "fields": fields}

        try:
            stage_resolved = stage.resolve(strict=True)
        except OSError as exc:
            return scoped(f"stage_root_unreadable: {exc}")

        layout = _cached_manifest_layout(own_cas, manifest_sha256)
        if layout is None:
            return scoped(
                f"manifest {manifest_sha256[:12]} unreadable; scope "
                f"undeterminable")
        mount_prefix, entries = layout
        try:
            window = prewarm_loop.entries_between(entries, start, end)
        except (ValueError, TypeError) as exc:
            return scoped(f"range not cuttable: {exc}")
        if not window:
            return scoped("the recorded range covers no manifest entry")
        # ``entries_between`` includes a straddling entry whole, so the
        # window is a cover of the range and not a partition of it: it may
        # exceed the span, and must never fall short of it.  The manifest is
        # the authority for what the range contains -- a manifest-wide
        # ``entry_count`` is a different number and is not interchangeable
        # with the entries one range covers.
        covered = sum(int(one.get("bytes", 0)) for one in window)
        if covered < end - start:
            return scoped(
                f"the window covers {covered} bytes, short of the "
                f"{end - start} the head recorded; scope is not the "
                f"recorded range")
        staged = _bounded_int(head.get("entries_staged"))
        if staged is not None and staged != len(window):
            return scoped(
                f"the window holds {len(window)} entries, not the {staged} "
                f"the head recorded staging; scope is not that window")
        names: dict[str, dict[str, object]] = {}
        #: The bare name a head from before range-only naming may have
        #: written instead, per derived name.  Never deleted here: a bare
        #: name was shared by every read of that path from offset zero, so it
        #: is not this head's identity.  Found, it is retained and named in
        #: the receipt rather than counted as already gone.
        pre_range: dict[str, str] = {}
        named_once = paths_named_once(entries)
        for entry in window:
            source, offset = str(entry["path"]), int(entry["offset"])
            try:
                relative = stage_relative(
                    source, offset, int(entry["bytes"]),
                    mount_prefix=mount_prefix)
                legacy = pre_range_stage_relative(
                    source, offset, int(entry["bytes"]),
                    mount_prefix=mount_prefix,
                    named_once=source in named_once)
            except ValueError as exc:
                # A name that cannot be derived shrinks the scope silently if
                # it is skipped, and a partial scope is a different question
                # from the one the receipt asked.
                return scoped(f"{source}: staged name underivable: {exc}")
            names[relative] = entry
            if legacy != relative:
                pre_range[relative] = legacy
        fields["scope_entries"] = len(names)

        # Originals first: nothing is destroyed before the inputs they stand
        # for are proven to survive it.  ``exists`` is not that proof -- a
        # directory exists, and a source shrunken below the extent its window
        # covers exists too -- so each original must be a regular file that
        # still holds the whole extent ``offset + bytes`` the manifest names,
        # resolved outside the stage root.  Size, never a digest: proving
        # recapturability is a stat, not a model-sized hash.
        checked = present = 0
        for entry in window:
            source = str(entry.get("path") or "")
            if not source:
                return scoped("a window entry names no source path")
            offset = _bounded_int(entry.get("offset"))
            span = _bounded_int(entry.get("bytes"))
            if offset is None or span is None:
                return scoped(
                    f"{source}: window entry names an unusable extent")
            checked += 1
            try:
                original = Path(source)
                if not original.exists():
                    return scoped(
                        f"original missing for {source}; the staged copy may "
                        f"be the last surviving input")
                if stage_resolved in original.resolve().parents:
                    return scoped(
                        f"original {source} resolves inside the stage root; "
                        f"it is not a separate input")
                if not original.is_file():
                    return scoped(
                        f"original {source} is not a regular source file")
                size = original.stat().st_size
            except OSError as exc:
                return scoped(f"original {source} unreadable: {exc}")
            if size < offset + span:
                return scoped(
                    f"original {source} holds {size} bytes, short of the "
                    f"{offset + span} its window covers; the staged copy is "
                    f"not proven recapturable")
            present += 1
        fields["originals_checked"] = checked
        fields["originals_present"] = present

        return {"refusal": "", "fields": fields,
                "stage_resolved": stage_resolved,
                "mount_prefix": mount_prefix, "names": names,
                "pre_range": pre_range}

    scope = _scope_outside_the_lock()
    memo = _CensusMemo()
    census_started = time.perf_counter()
    if not scope["refusal"]:
        # The censuses once, as hints that fill the memo; the pass under
        # the lock takes them again and acts only on that.
        hint_paths = {os.path.normpath(str(stage / relative))
                      for relative in scope["names"]}
        _fragment_owners(Path(residency_root if residency_root is not None
                              else queue.root / pool.RESIDENCY),
                         hint_paths, memo=memo)
        reader_lease.live_for(queue, None, residency_root=residency_root,
                              memo=memo.pins)
        _claimed_paths(queue, tier_id, own_cas, memo=memo)
        _claimed_source_paths(queue, stage, own_cas, memo=memo)
    census_s = time.perf_counter() - census_started
    emptied: set[Path] = set()

    def _held_recover() -> dict[str, object]:
        """The ownership decision and the act, under the stage lock."""

        for key, label in ((head_action_key, "head"), (consumer, "consumer")):
            for state in (pool.READY, pool.CLAIMED):
                try:
                    if queue.item_path(state, key).exists():
                        return refuse(
                            f"{label} {key[:12]} is still {state}; recovery "
                            f"acts only on a finished request")
                except OSError as exc:
                    return refuse(f"{label} {key[:12]} unreadable: {exc}")
        in_flight = movers_in_flight(queue, tier_id=tier_id)
        if in_flight:
            receipt["movers_in_flight"] = sorted(in_flight)
            return refuse("movers_in_flight")
        receipt.update(scope["fields"])
        if scope["refusal"]:
            return refuse(str(scope["refusal"]))
        stage_resolved = scope["stage_resolved"]
        mount_prefix = scope["mount_prefix"]
        names = scope["names"]
        pre_range = scope["pre_range"]
        # Every fragment retains, wanted or not: the question here is not
        # whose bytes these are but whether anything at all still names them.
        # ``_fragment_owners`` -- not ``attributed_stage_paths`` -- because
        # the repair's half of the question is answered against the exact
        # staged-path set this scope derived, and because the map-composition
        # reader deliberately skips a fragment that cannot be read or
        # validated so a consumer still finds its other movers' copies.
        # Readiness must not reuse that tolerance: skipped is how unknown
        # collapses into unowned, and unowned is what deletes.  A taint
        # refuses the whole pass, the same fail-closed rule the egress
        # applies to its own fragment.  No mover is excepted, so a withdrawn
        # mover's fragment retains too -- one more kept file is a pass.
        fragment_root = Path(residency_root if residency_root is not None
                             else queue.root / pool.RESIDENCY)
        scope_paths = {os.path.normpath(str(stage / relative))
                       for relative in names}
        parse_counts = memo.counts()
        fragment_owners, fragment_taint = _fragment_owners(
            fragment_root, scope_paths, memo=memo)
        if fragment_taint:
            return refuse(f"fragment census unreadable: "
                          f"{'; '.join(fragment_taint[:3])}")
        attributed = set(fragment_owners)
        pin_owners, pin_taint = reader_lease.live_for(
            queue, None, residency_root=residency_root, memo=memo.pins)
        if pin_taint:
            return refuse(f"pin census unreadable: {'; '.join(pin_taint[:3])}")
        attributed |= set(pin_owners)
        claimed, claim_taint = _claimed_paths(queue, tier_id, own_cas,
                                              memo=memo)
        if claim_taint:
            return refuse(
                f"claim census unreadable: {'; '.join(claim_taint[:3])}")
        attributed |= {os.path.normpath(str(stage / one)) for one in claimed}
        handoffs, handoff_taint = _claimed_source_paths(
            queue, stage, own_cas, memo=memo)
        if handoff_taint:
            return refuse(
                f"promotion handoff census unreadable: "
                f"{'; '.join(handoff_taint[:3])}")
        attributed |= {os.path.normpath(str(one)) for one in handoffs}
        receipt.update(_locked_parse_record(memo, parse_counts))

        retained: dict[str, int] = {}

        def retain(why: str) -> None:
            retained[why] = retained.get(why, 0) + 1

        eligible: list[tuple[Path, int]] = []
        already_gone = 0
        for relative in sorted(names):
            path = stage / relative
            try:
                if not path.exists():
                    legacy = pre_range.get(relative)
                    if legacy is not None and os.path.lexists(stage / legacy):
                        retain("pre_range_name_not_this_heads_identity")
                    else:
                        already_gone += 1
                    continue
                if path.is_symlink() or not path.is_file():
                    retain("not_a_regular_file")
                    continue
                if stage_resolved not in path.resolve().parents:
                    retain("outside_the_owned_stage_root")
                    continue
                size = path.stat().st_size
            except OSError as exc:
                # An entry nothing can read is not an entry proven unowned.
                return refuse(f"{relative}: {exc}")
            if os.path.normpath(str(path)) in attributed:
                retain("attributed_pinned_claimed_or_handed_off")
                continue
            if _marked_by_the_prewarm_stage(path) is not True:
                retain("not_marked_by_the_stage")
                continue
            eligible.append((path, int(size)))

        receipt["entries_eligible"] = len(eligible)
        receipt["bytes_eligible"] = sum(size for _path, size in eligible)
        receipt["entries_retained"] = sum(retained.values())
        receipt["entries_already_gone"] = already_gone
        receipt["retained_reasons"] = dict(sorted(retained.items()))
        receipt["mount_prefix"] = mount_prefix
        if not apply:
            return receipt

        retired = bytes_retired = 0
        errors: list[str] = []
        for path, size in eligible:
            try:
                os.unlink(path)
            except FileNotFoundError:
                already_gone += 1
                continue
            except OSError as exc:
                errors.append(f"{path.name}: {exc}")
                continue
            retired += 1
            bytes_retired += size
            emptied.add(path.parent)
        receipt["entries_retired"] = retired
        receipt["bytes_retired"] = bytes_retired
        receipt["entries_already_gone"] = already_gone
        receipt["entries_refused"] = len(errors)
        receipt["errors"] = errors
        receipt["complete"] = not errors
        return receipt

    # The per-entry classification and the unlinks stay in one hold.  This
    # is an operator's repair, run by hand through the CLI and never by a
    # tier cycle, and each entry's ``exists``/``resolve``/mark reads are the
    # evidence its unlink stands on; what left the hold is the scope, the
    # originals and the census parse (above) and the directory prune.
    asked = time.perf_counter()
    with queue.stage_ownership_lock(str(stage)):
        granted = time.perf_counter()
        result = _held_recover()
    released = time.perf_counter()
    pruned_started = time.perf_counter()
    for parent in sorted(emptied, key=lambda one: len(one.parts), reverse=True):
        _prune_empty(parent, stage)
    result.update(_hold_record(
        lock_wait_s=granted - asked, lock_held_s=released - granted,
        census_s=census_s, prune_s=time.perf_counter() - pruned_started))
    return result


def movers_in_flight(queue: pool.PoolQueue, *, tier_id: str) -> set[str]:
    """Mover keys ready or claimed on this tier, i.e. copies that may be writing.

    A mover row is the one whose residency block names a *range*; a consumer's
    names leads.  Ready as well as claimed, because a ready mover can be
    claimed between a directory walk and an unlink.
    """

    out: set[str] = set()
    for state in (pool.READY, pool.CLAIMED):
        for path, item in queue_records(queue, state):
            residency = item.get("residency") if isinstance(item, dict) else None
            if not isinstance(residency, dict):
                continue
            if "range_start_bytes" not in residency:
                continue
            if str(residency.get("tier_id") or "") != tier_id:
                continue
            key = path.name[:-len(".json")] if path.name.endswith(".json") else path.name
            out.add(key)
    return out


def own_action_key(declared: str | None) -> str:
    """This node's own action key, from the flag or from the launcher.

    A movement node files its receipt and holds its tier tokens under its own
    key, and that key is ``canonical_sha256`` of the action body --- so a
    ``--action-key`` sealed into the argv would be hashed into the very value
    it states, and no fixed point exists.  The launcher sets
    :data:`prismabuild.core.ACTION_KEY_ENV` for every action it starts, from
    the action in hand, which is the one place the answer is already known.
    The flag stays, because a direct run and every test needs to say which key
    it is acting as.
    """

    key = declared or os.environ.get(pb.ACTION_KEY_ENV) or ""
    if len(key) != 64 or any(character not in "0123456789abcdef" for character in key):
        raise SystemExit(
            f"pass --action-key, or run under a launcher that sets "
            f"{pb.ACTION_KEY_ENV}; got {key!r}")
    return key


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="delete one mover's staged files and return its tier tokens")
    parser.add_argument("--pool-root", required=True,
                        help="the pull queue root this egress files its receipt under")
    parser.add_argument("--action-key", default=None,
                        help="this egress node's own action key; defaults to "
                             f"{pb.ACTION_KEY_ENV}, which the launcher sets")
    parser.add_argument("--mover-action-key", default=None,
                        help="the movement node whose staged bytes are being taken back")
    parser.add_argument("--consumer-action-key", default=None,
                        help="the action those bytes were staged for")
    recovery = parser.add_argument_group(
        "bounded orphan recovery",
        "retire the cache copies of one RETIRED head that nothing can prove. "
        "Reports and changes nothing unless --apply is given.")
    recovery.add_argument("--recover-orphaned-range", action="store_true",
                          help="run the recovery instead of an eviction")
    recovery.add_argument("--head-action-key", default=None,
                          help="the retired head, by its filed move receipt")
    recovery.add_argument("--egress-action-key", default=None,
                          help="the egress receipt that retired it")
    recovery.add_argument("--cas-root", default=None,
                          help="default <pool-root>/../cas")
    recovery.add_argument("--apply", action="store_true",
                          help="actually unlink; omit to report only")
    parser.add_argument("--stage-root", required=True,
                        help="the staging dataset's mountpoint; nothing outside it "
                             "is ever deleted")
    parser.add_argument("--residency-root", default=None,
                        help="where residency-map fragments are filed "
                             "(default <pool-root>/residency)")
    parser.add_argument("--receipt", default=None,
                        help="also write the receipt here (it is always filed in "
                             "the queue's movers directory)")
    args = parser.parse_args(argv)
    args.action_key = own_action_key(args.action_key)

    queue = pool.PoolQueue(Path(args.pool_root))
    if args.recover_orphaned_range:
        required = {"--head-action-key": args.head_action_key,
                    "--egress-action-key": args.egress_action_key}
        absent = sorted(name for name, value in required.items()
                        if value is None)
        if absent:
            parser.error(
                f"--recover-orphaned-range needs {', '.join(absent)}")
        # Consumer, tier, manifest and range are read off the filed receipts,
        # not taken here: there is no caller-supplied duplicate to disagree
        # with history.
        receipt = recover_orphaned_range(
            queue, stage_root=args.stage_root,
            head_action_key=args.head_action_key,
            egress_action_key=args.egress_action_key,
            cas_root=args.cas_root, residency_root=args.residency_root,
            apply=args.apply)
        queue.record_move(args.action_key, receipt)
        if args.receipt:
            with open(args.receipt, "w") as stream:
                json.dump(receipt, stream, indent=1, sort_keys=True)
                stream.write("\n")
        print(json.dumps(receipt, indent=1, sort_keys=True, default=str))
        return 0 if receipt["complete"] else 1
    for name, value in (("--mover-action-key", args.mover_action_key),
                        ("--consumer-action-key", args.consumer_action_key)):
        if not value:
            parser.error(f"{name} is required for an eviction")
    receipt = evict(queue, args.mover_action_key,
                    consumer_action_key=args.consumer_action_key,
                    stage_root=args.stage_root,
                    residency_root=args.residency_root)
    queue.record_move(args.action_key, receipt)
    if args.receipt:
        with open(args.receipt, "w") as stream:
            json.dump(receipt, stream, indent=1, sort_keys=True)
            stream.write("\n")
    print(json.dumps(receipt, indent=1, sort_keys=True, default=str))
    return 0 if receipt["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

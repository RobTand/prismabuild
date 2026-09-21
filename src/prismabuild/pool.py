"""Shared-filesystem pull-queue transport: dispatch without a scheduler.

``slurm.py`` and ``dagster.py`` both assume a scheduler that is not installed on
this fleet, so neither is its live transport.  This module is the deployed
transport: workers pull sealed actions from a directory on the shared NFS mount
and execute them through the *same* canonical worker argv SLURM would have
submitted.

**Every primitive here is ported from ``pqwork``**, the predecessor this
replaces, because those primitives were argued out against real NFS behaviour
and two boxes of production use:

* **Claiming uses atomic ``rename()`` under per-key POSIX exclusion.**
  Permanent lock inodes serialize ownership transitions across NFS clients
  and the server; queue mounts must provide cross-host POSIX lock visibility.
* **A claim is a lease, not a grant.**  The claimant refreshes a heartbeat file;
  any worker may return a stale claim to ``ready``.  That is what makes a dead
  box self-healing rather than a permanent hold on the work.
* **Intent is written before the claim**, mirroring ``slurm.py``'s
  intent-before-``sbatch`` discipline, so a crash between the two is
  diagnosable rather than invisible.

**Admission is capacity-aware, and it is built from that live defect rather than
around it.**  The one documented failure on this fleet
(``/mnt/shared/pq-ops/starvation/REPRO-2026-08-30``) was an admission failure,
not a transport failure, and it named three bugs.  This ledger answers each
structurally rather than by policy:

* **Hold-while-gated.**  There, an actor acquired both boxes' whole memory
  budget and *then* waited for a drain condition its own hold prevented from
  ever being observed.  Here a reservation is acquired inside ``claim`` and
  released in ``finish``, so a holder is by construction *running*, never
  waiting.  There is no window in which holding and waiting overlap, so the
  circularity has nowhere to form.
* **No aging.**  There, ``evicted_5x_does_not_fit`` was counted and then the job
  was abandoned: "an eviction counter that only counts is a starvation detector
  wired to nothing."  Here a denial increments ``passes``, ``passes`` orders
  the ready set within a priority band, and past ``STARVATION_FLOOR`` a denied
  item *withholds the host* -- a worker that cannot admit the starved item
  declines to admit a smaller one instead of leapfrogging it.  The counter is
  wired to the decision it describes.  Aging stays inside the band: an item
  published at a negative priority yields to everything above it however long
  it has waited (#362), and once admitted it yields the box as well -- a
  foreground denial withdraws one background holder whose release admits it,
  through the ordinary withdrawal ladder, and requeues that holder at its own
  priority (#364).
* **Partial-hold waste.**  Acquisition is all-or-nothing: a demand that cannot
  be met in full releases every token it took before returning.

The deadlock the floor could otherwise cause is handled explicitly: an item
whose demand exceeds this host's *total* capacity can never run here, so it is
skipped rather than allowed to withhold a box it would never use.

**A terminal generation stays terminal.**  ``run_local_action`` looks the
action key up in the CAS first and normally makes a retry a cheap
``cache_hit``, but that is not permission to resurrect a generation which the
queue has already filed under ``done`` or ``failed``.  Stale reaping and the
claim boundary both refuse a ready/claimed copy carrying the terminal record's
``published_unix``.  The generation check matters: the same action key may be
submitted again deliberately, and that later request is still work.

**A retry is a producer contract, and an attempt is immutable evidence.**
The pool preserves the explicit ``max_attempts`` supplied by each transport;
``fleet/pbrun`` gives arbitrary commands one attempt unless their producer
declares the whole action retry-safe.  Numerical determinism is deliberately
separate: an action can deterministically write external state before failing.
Every success, failure, or lease loss concluded from its live queue record is
first-writer-published below ``attempts/<action-key>/<generation>/`` with
separate immutable stdout and stderr, and the mutable ready/terminal summary
links that ordered history.  A later refusal can therefore never replace the
causal attempt that did the work.  An operator withdrawal remains the decision
record rather than inventing a worker result for work it cancelled.

**Withdrawal is an operator's decision, and it is filed as one.**  ``finish``,
``reap_stale`` and ``quarantine_orphans`` each describe a *worker's* health; none
of them says "I have changed my mind", so cancelling meant rewriting
``max_attempts`` into a live claimed record and then racing the retry that the
kill would otherwise trigger.  ``withdraw`` is that missing verb.  It writes its
marker *before* it removes anything, and ``claim``, ``finish`` and ``reap_stale``
all consult that marker, so from the moment it exists the action cannot be
claimed, cannot be requeued and cannot be filed as a defect -- whatever a
concurrent worker is doing at the time.  The withdrawal wins the race by
construction rather than by the operator being quick.

**It cancels a run, not a name.**  An action key is a content hash, so the same
command against the same tree fingerprints identically and re-submitting it is
how anybody asks for the same work again.  A marker that blacklisted the key
would therefore make the queue eat that request -- ``claim`` deleting the fresh
record, ``pbrun`` answering the new run with the old run's reason -- with the
only remedy a hand edit of the live queue, which is the thing this verb exists
to abolish.  So the marker is scoped to the generation it was filed against
(``published_unix``, which ``publish`` stamps fresh and every requeue carries
forward), one predicate reads it at every guard site (``withdrawal_covers``),
and a later ``publish`` retires it into ``withdrawn/superseded/``.  Nothing is
ever removed silently: a record the queue drops is filed there first.

**A detached container is still the action.**  ``pbrun`` seals a derived
container-owner id and puts its Docker shim first on ``PATH``.  The shim labels
every container it creates and leaves a durable marker; ``finish``, withdrawal
and stale reaping query that label, force-remove its containers and verify the
answer is empty before releasing capacity.  A remote host, a busy creation
transaction or a Docker error keeps the claim and tokens: uncertainty is not
permission to schedule a second action onto the same GPU.

Offer readers tolerate bounded cross-host clock skew. Lease recovery still
uses cross-host wall time; future heartbeats delay recovery rather than expire
immediately. Execution deadlines and progress watches use local monotonic time.
"""

from __future__ import annotations

from collections.abc import Callable, Container, Iterable, Iterator, Mapping, Sequence
from typing import NamedTuple
from contextlib import contextmanager, nullcontext, suppress
import errno
import fcntl
import fnmatch
from functools import wraps
from inspect import signature
import hashlib
import json
import math
import os
from pathlib import Path
import resource
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import uuid

from . import core as pb
from . import progress as pb_progress
from .materialize import (  # relocated verbatim; see materialize.py
    _cleanup_execution_checkout,
    _now,
    _run_materializer_git,
    _write_json_atomic,
)
from . import materialize, cpu_topology
from . import adaptive_cpu as cpu_admission
from . import adaptive_gpu as gpu_admission
from . import container_images as image_inventory
from . import residency_map
from . import storage_tiers
from . import box_capacity
from . import box_window
from . import resource_scope
from . import posix_lock

POOL_ITEM_SCHEMA_V1 = "prismaquant.prismabuild.pool_item.v1"
POOL_CLAIM_INTENT_SCHEMA_V1 = "prismaquant.prismabuild.pool_claim_intent.v1"
POOL_LEASE_SCHEMA_V1 = "prismaquant.prismabuild.pool_lease.v1"
POOL_OUTCOME_SCHEMA_V1 = "prismaquant.prismabuild.pool_outcome.v1"
#: What one run cost and how loaded its box was, filed with the outcome.  Issue
#: #372 Tier 0: metadata about a run, never part of the action it was a run of,
#: so a receipt that carries it and one from before it are the same action.
RESOURCE_PROFILE_SCHEMA_V1 = "prismabuild.resource_profile.v1"
POOL_ATTEMPT_SCHEMA_V1 = "prismaquant.prismabuild.pool_attempt.v1"
POOL_OFFER_SCHEMA_V1 = "prismaquant.prismabuild.pool_offer.v1"


def _membership_withdrawal_owner(value: object) -> bool:
    """Whether ``value`` names a membership supervisor drain owner.

    The fleet_membership owner kind is exactly
    ``{host}:supervisor-{pid}:{starttime}`` with a positive pid. This is a
    shape check only; liveness, host binding, and epoch continuity are
    enforced by the broker mutex and the membership caller, never here.
    Shape alone authorizes nothing: only ``membership_handoff_authorized``
    reads a proven handoff.
    """

    if not isinstance(value, str):
        return False
    _host, _, rest = value.partition(":")
    kind, _, starttime = rest.partition(":")
    label, _, pid_text = kind.partition("-")
    if not _host or label != "supervisor" or not starttime:
        return False
    try:
        return int(pid_text) > 0
    except (ValueError, TypeError):
        return False


def membership_handoff_authorized(decision: object) -> bool:
    """Whether a filed withdrawal decision carries a proven handoff.

    The one precise carrier check shared by the withdrawal classifier,
    the owed/requeue lineage and the tier-loop window: the decision must
    carry a ``membership_handoff`` mapping whose owner is the membership
    caller kind and equals the decision's own ``withdrawn_by``, and whose
    attempt, budget and generation are typed and equal to the decision's
    own -- including a finite numeric generation. Shape-only,
    malformed and tampered decisions all answer False: they file
    ordinary cancellations and never authorize retry or staging.
    """

    if not isinstance(decision, Mapping):
        return False
    proof = decision.get("membership_handoff")
    if not isinstance(proof, Mapping):
        return False
    owner = proof.get("owner")
    if (not isinstance(owner, str) or not owner
            or not _membership_withdrawal_owner(owner)):
        return False
    if owner != decision.get("withdrawn_by"):
        return False
    if (type(proof.get("attempts")) is not int
            or proof.get("attempts") != decision.get("attempts")):
        return False
    if (type(proof.get("max_attempts")) is not int
            or proof.get("max_attempts") != decision.get("max_attempts")):
        return False
    published = proof.get("published_unix")
    if (type(published) not in (int, float)
            or isinstance(published, bool)
            or not math.isfinite(float(published))):
        return False
    return published == decision.get("published_unix")
POOL_PREWARM_SCHEMA_V1 = "prismaquant.prismabuild.pool_prewarm.v1"
#: What one movement node says it staged, and what the pool delivered while
#: it did.  Read by the ``tiers`` role for the fill measurement, so it carries
#: the pacer's pool-side attribution like a prewarm record does.
POOL_MOVE_SCHEMA_V1 = "prismaquant.prismabuild.pool_move.v1"
#: What one egress node says it took back off a tier, and the tokens it
#: returned for it.  Filed beside the move receipts so one directory answers
#: "what is on the stage and who holds it".
POOL_EGRESS_SCHEMA_V1 = "prismaquant.prismabuild.pool_egress.v1"
#: The move-receipt field naming the mover a range was taken over from (#598).
#: A receipt carrying it copied nothing: the bytes were already on the tier and
#: the tokens standing for them changed owner.  It is the only way to tell an
#: adopted range from a copied one, and the gate reads it because an adopted
#: mover has no terminal record -- it never ran.
MOVE_ADOPTED_FROM_FIELD = "adopted_from"

# The `prismaquant.` prefix is kept on purpose.  It is the namespace grammar of
# every receipt already published to this CAS; mixing prefixes inside one store
# would be worse than carrying the history.  See the package docstring.

READY = "ready"
CLAIMED = "claimed"
DONE = "done"
FAILED = "failed"
INTENT = "intent"
#: Where an operator's cancellation is filed.  Deliberately not ``failed``: a
#: withdrawn action is a decision, and putting it in the failure record makes
#: the failure record lie about the fleet.  Four withdrawn test suites are in
#: the live ``failed/`` for exactly that reason.
WITHDRAWN = "withdrawn"
_STATES = (READY, CLAIMED, DONE, FAILED, INTENT, WITHDRAWN)

#: Suffix of a claim that has been moved out of the way while its finisher
#: publishes the item's next home.  Every reader of ``claimed/`` addresses it
#: as ``<key>.json`` or ``<key>.lease`` -- ``reap_stale`` and
#: ``sweep_widowed_leases`` by glob, ``item_path`` and ``find_key`` by name --
#: so a suffix that is neither is invisible to all of them, which is the point.
TOMBSTONE_SUFFIX = ".tombstone"
# Old sweepers must ignore exact-attempt cleanup beside a newer live claim;
# they retire ordinary tombstones in that situation without scope cleanup.
LATE_FINISH_SUFFIX = ".late-finish"
CONTAINER_OWNERS = "container-owners"
CONTAINER_OWNER_LABEL = "prismabuild.action"
#: The slice a container was created inside.  The owner label is the action's
#: identity and spans its attempts; this one names a single attempt's scope.
#: Both are reserved: the shim refuses a caller that tries to set either, and
#: writes this one from the kernel cgroup it is running in, so a query on it is
#: an identity match rather than a name match.
CONTAINER_SCOPE_LABEL = "prismabuild.scope"

#: The sealed environment variable whose value becomes that label, and the
#: sealed path of the marker the shim writes on first container creation.  They
#: live beside the label because they are one contract with it: a submitter
#: seals them, the Docker shim reads them, and finish, withdrawal and the SLURM
#: Epilog all find an action's payloads by joining the three.  Three files
#: spelled them separately, which is one edit away from a reaper querying a
#: label nothing carries.
CONTAINER_OWNER_ENV = "PRISMABUILD_CONTAINER_OWNER"
CONTAINER_MARKER_ENV = "PRISMABUILD_CONTAINER_MARKER"
DOCKER = "/usr/bin/docker"
ATTEMPTS = "attempts"

# Ported verbatim from pqwork: 30 s refresh, 300 s expiry.  The 10x margin is
# what absorbs an NFS stall or a long GC pause without a spurious requeue.
HEARTBEAT_S = 30.0
LEASE_TIMEOUT_S = 300.0

# How long the timeout path gives the launcher's process group to go down.
# The launcher does not just exit when signalled: it relays the signal into the
# action's own session -- ``run_local_action`` gives the action
# ``start_new_session=True`` -- and that relay is itself TERM, grace, KILL,
# grace, i.e. two of core's windows.  SIGKILLing the launcher before it
# finishes would orphan the very action this timeout exists to stop, so the
# budget is core's two windows and not a number of its own; the 5 s on top is
# margin for the launcher's own exit once the relay has returned.
TIMEOUT_GRACE_S = 2.0 * pb._PROCESS_GROUP_GRACE_SECONDS + 5.0

# The pool is also a lower-level transport for specialized producers whose
# existing contract explicitly chooses its bound.  ``fleet/pbrun`` supplies a
# separate safe default of one for arbitrary commands; changing this legacy
# pool API default would silently rewrite those producers' policy.
DEFAULT_MAX_ATTEMPTS = 3

# How many admission denials before a ready item stops being overtaken.  The
# repro's job died at `evicted_5x_does_not_fit`, so five is the count at which
# the old system gave up; the floor has to bite strictly before that or it
# inherits the same outcome.  Three is chosen on that ground alone -- it is a
# policy knob, not a derived constant, and nothing downstream depends on its
# value beyond "small, and less than five".
STARVATION_FLOOR = 3

#: How long a starved item may withhold a host before it keeps its place in the
#: ordering but loses its veto.
#:
#: The withhold exists so small work cannot indefinitely overtake big work.  It
#: assumed the block is transient -- the box is busy *now* and will free up.
#: When the blocking resource is held for hours that assumption inverts and the
#: guard becomes the deadlock it was written to prevent.  Measured on the live
#: fleet 2026-09-04: one GPU action at the head of sparky's queue accumulated
#: **293** denied passes while two multi-hour GPU actions held both slots, and
#: withheld the box the whole time.  Forty-one items queued behind it, 24 of
#: them CPU-only and admissible against the five free cores it was not using.
#:
#: Past this ceiling the item stops *blocking* but keeps every pass it has
#: earned, and passes order the ready set within a priority band -- so it still
#: gets first refusal on every claim, on every box, ahead of everything behind
#: it in its band.  It loses the veto, not its place.  Fifteen minutes is longer than
#: any transient this pool produces (the lease timeout is five) and far shorter
#: than the multi-hour actions that turn the guard pathological.
WITHHOLD_CEILING_S = 900.0

#: How much of an unparseable record is kept inline with the evidence.  Enough
#: to recognise a writer's handwriting, little enough that a runaway producer
#: cannot fill the queue root with the file it already failed to write.
UNREADABLE_HEAD_BYTES = 2048

RESERVATIONS = "reservations"
#: Cluster-scoped ledgers, one per storage tier (#583).  A separate root,
#: because every directory under ``reservations/`` is read as a *box* by
#: ``claim_reservation_hosts``, and a tier that held the same key would
#: make the claim's holder ambiguous.  A tier lives on one box but its
#: tokens are taken by claimants on any box: a mover on dl380g10 fills a
#: stage that a consumer on a Spark reads, and both reserve against the
#: same ledger.
TIER_RESERVATIONS = "tier-reservations"
#: Where one mover's advance-credit funding record lives (window progress
#: protection): bound to exact tier/mover/range/generation, read by the claim
#: path only to cover pre-positioned fence tokens, never to invent capacity.
TIER_FUNDING = "tier-funding"
TIER_FUNDING_SCHEMA_V1 = "prismabuild.tier_funding.v1"
#: The funding state machine.  ``reserved`` (fence held under the grant) ->
#: ``transferring`` (fence moved under the mover, awaiting its claim) ->
#: ``consumed`` (the claim counted it); any live state -> ``released``.
#: Terminal states never advance.  Only ``transferring`` authorizes a
#: subtraction, and only for the named tokens still held under the mover.
TIER_FUNDING_STATES = frozenset({
    "reserved", "transferring", "consumed", "released",
})
#: Legal funding state steps, the single table every writer enforces.
#: :meth:`PoolQueue._advance_funding_state_locked` and
#: :meth:`PoolQueue._write_funding_locked` both read it; there is no second
#: state machine.  Fresh generations are born ``reserved``, and only the
#: reserve path mints them (see
#: :meth:`PoolQueue._rotate_funding_locked`).
_FUNDING_TRANSITIONS = {
    "reserved": frozenset({"transferring", "released"}),
    "transferring": frozenset({"consumed", "released"}),
}
#: Binding fields, immutable within one generation.  Everything except
#: ``state`` (which advances through :data:`_FUNDING_TRANSITIONS`) and
#: ``unix`` (a diagnostic stamp) -- a same-generation rewrite changing any
#: of these is a different binding wearing a spent generation, and refuses.
_FUNDING_BINDING_FIELDS = frozenset({
    "schema", "tier_id", "consumer_action_key", "plan_sha256",
    "mover_action_key", "range_start_bytes", "range_end_bytes",
    "kind", "tokens", "generation", "published_unix",
})
#: Output prepaid-window funding: same ledger, same directory, same state
#: table and mover lock as the window (V1) binding above, but a distinct
#: versioned binding type for sealed produced-output batches.  V1 validation
#: is unchanged; this variant is validated by
#: :meth:`PoolQueue.validate_output_funding` and covered by
#: :meth:`PoolQueue.output_funded_cover`.  One authoritative intent per
#: mover per tier per variant, owned by the pool; the produced-output lane
#: references its ``generation`` and never mirrors it.
TIER_FUNDING_OUTPUT_SCHEMA_V1 = "prismabuild.tier_funding.output.v1"
#: Immutable output binding fields within one generation (``state``/``unix``
#: advance as in V1).  ``published_unix`` is the sealed mover publication;
#: ``owner_*`` binds the live producer claim that prepaid the window.
_FUNDING_OUTPUT_BINDING_FIELDS = frozenset({
    "schema", "tier_id", "kind", "mover_action_key", "tokens",
    "generation", "published_unix", "owner_action_key", "owner_nonce",
    "owner_scope_id", "owner_published_unix", "template_id",
    "template_sha256", "batch_id", "manifest_digest",
    "range_start_bytes", "range_end_bytes",
})
#: Typed immutable produced-output batch reference in a mover's sealed params
#: (R4 required admission carrier). Identifies producer action + existing
#: attempt/instance identity, batch id, manifest digest, target tier and
#: range/canonical batch namespace. NEVER the mover's own action key in its
#: own key, NEVER the mutable funding generation/publication timestamp: the
#: action key commits to required-output semantics permanently while the
#: funding generation rotates (0.0 sentinel -> live publication) beside it.
PRODUCED_OUTPUT_BATCH_REF_SCHEMA_V1 = "prismabuild.produced_output_batch_ref.v1"
#: Which plan leg role one funding kind pays for.  A fence funds staged
#: (or promoted) occupancy; rate kinds and tiers with no movement legs
#: never carry advance credit, so a record naming any other kind covers
#: nothing.
_FUNDING_KIND_ROLES = {
    "stage_gib": "mover_row",
    "ram_gib": "ram_mover_row",
}
#: Where a tier loop files what it discovered about one tier, for readers.
TIERS = "tiers"
#: The residency block an item may carry (#583): what a movement node moves,
#: and which movement nodes a compute node waits on.  Absent on every item the
#: fleet publishes today, and the whole mechanism is inert without it.
RESIDENCY_SCHEMA_V1 = "prismabuild.residency.v1"
_RESIDENCY_KEYS = frozenset({
    "schema", "tier_id", "manifest_sha256", "manifest_bytes",
    "range_start_bytes", "range_end_bytes", "leads",
})
PASSES = "passes"
CLAIM_DENIALS = "claim-denials.json"
CLAIM_DENIALS_SCHEMA_V1 = "prismabuild.claim_denials.v1"
MAX_CLAIM_DENIALS = 256
MAX_DENIAL_VALUE_DEPTH = 6
MAX_DENIAL_VALUE_ITEMS = 32
MAX_DENIAL_VALUE_TEXT = 256
WORKERS = "workers"
#: Where a storage-role loop files what it made resident for one action.
#: A sidecar for the same reason ``passes`` is one: the only safe moment to
#: write a ready item is never.  The prewarm runs while the item is still in
#: ``ready``, so writing its result into the item would race the claim that
#: may already have moved it and resurrect a claimed action.
PREWARM = "prewarm"

#: The claim record's pointer at that sidecar, and the digest it must still
#: name when the terminal record resolves it.  The claim carries a reference
#: rather than a copy (#596): the receipt keeps growing after the claim --
#: later windows extend it -- and a copy frozen at claim time describes a
#: counter that no longer exists on a record no admission decision ever reads.
PREWARM_RECEIPT_REF = "prewarm_receipt"

#: A prewarm receipt for a key nobody queues any more is garbage, but only
#: the queue knows it is garbage: the storage loop that wrote it never sees
#: the finish that retired it.  ``sweep_prewarm_receipts`` deletes such a
#: receipt, and this is how long a swept, queue-absent receipt without any
#: terminal or withdrawal record survives first -- the safety valve for a
#: receipt whose key vanished by a path no state directory records.  Terminal
#: and withdrawn keys are pruned without waiting: their evidence already
#: reached the terminal record through the claim's reference.
PREWARM_RECEIPT_RETENTION_S = 7 * 24 * 3600.0

#: Where a movement node files what it staged.  A sidecar for the same reason
#: ``prewarm`` is one, and read by ``tier_loop`` for the fill measurement: a
#: mover's receipt is the pool-side rate the tier mints its fill tokens from.
MOVERS = "movers"

#: Tier resources that price a transfer *rate* rather than occupancy, and so
#: are returned the moment a copy ends even when its bytes stay on the device.
#: Occupancy kinds are absent on purpose -- see
#: :meth:`PoolQueue.release_tier_rate_reservations` (#636).
TIER_RATE_KINDS = frozenset({storage_tiers.FILL_KIND})

#: Where movers file their residency-map fragments, one directory per consumer
#: and one file per mover inside it.  The composed map a consumer reads is
#: written from these; see :mod:`prismabuild.residency_map`.
RESIDENCY = "residency"

#: Where a consumer's frozen window plan lives: every movement node it
#: will ever have, named before the first one is published, so a restart
#: resumes the same decomposition instead of cutting a new one.
RESIDENCY_PLANS = "residency-plans"

#: How long each rung of a withdrawal's signal ladder waits before escalating.
#: Matched to ``core._PROCESS_GROUP_GRACE_SECONDS``, which is the grace the
#: launcher itself gives the action group it reaps on the way out.
WITHDRAW_GRACE_S = 5.0

#: How long a worker's offer stays believable.  A loop re-announces on every
#: poll, and the default poll is 10 s, so two minutes is a dozen missed polls:
#: long enough that a slow NFS write or a long action never makes a live box
#: look dead, short enough that a box taken down does not keep vouching for
#: work nobody can run.
OFFER_TIMEOUT_S = 120.0

#: Offer discovery tolerates up to one minute of future skew. This bound stays
#: finite even when a submitter reads retained capability with an infinite TTL.
#: It grants no freshness credit to CPU/GPU admission samples or claim leases.
OFFER_FUTURE_TOLERANCE_S = 60.0


class OfferTiming(NamedTuple):
    age_s: float | None
    clock_skew_s: float | None


def _residency_action_key(value: object) -> str:
    """One action key, checked, because these two names index shared files.

    The residency map and the frozen plan are named after the consumer; a
    caller that passed a path fragment instead of a key would name a file
    somewhere else under the shared mount.
    """

    if (not isinstance(value, str) or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)):
        raise PoolContractError("a residency file is named by a 64-character action key")
    return value


def offer_timing(announced: object, *, now: float) -> OfferTiming:
    """Usable age and observed future skew, relative to the reader's clock.

    A bounded future announcement counts as age zero. Excessive future skew
    has no usable age but keeps its discrepancy for diagnostics. These are
    reader/record differences, not a measurement against a trusted time source.
    """
    if type(announced) not in (int, float):
        return OfferTiming(None, None)
    try:
        stamp = float(announced)
    except OverflowError:
        return OfferTiming(None, None)
    if not math.isfinite(stamp):
        return OfferTiming(None, None)
    age = now - stamp
    skew = max(0.0, -age)
    return OfferTiming(max(0.0, age) if skew <= OFFER_FUTURE_TOLERANCE_S else None, skew)

DEFAULT_POOL_ROOT = Path(
    os.environ.get("PRISMABUILD_POOL_ROOT", "/mnt/shared/pb-queue")
)

#: Every box mounts this at the same path.  A checkout underneath it is
#: visible to all of them; a checkout outside it exists on exactly one box.
SHARED_ROOT = Path("/mnt/shared")

# The snapshot bundle travels through the shared CAS, but execution trees stay
# on each worker's local disk. Same spelling on every box, different storage.
# Read at call time rather than bound once, so that overriding it here steers
# what this transport materializes and nothing else.
LOCAL_CHECKOUT_ROOT = materialize.LOCAL_CHECKOUT_ROOT


def is_box_local_path(path: object) -> bool:
    """Does this absolute path exist on exactly one box?

    The rule that decides an action's placement lives here rather than in
    ``pbrun`` because two readers need it and they must not disagree: the
    submitter turns it into a pin (``pbrun.placement_tags``), and the queue
    turns it into a width (``placement_census``).  A second copy is how the
    pin and the measurement of the pin end up describing different fleets.

    A pure string test, deliberately.  The census reads paths recorded by
    OTHER boxes, and ``resolve()`` would follow the reading box's symlinks
    through a tree it does not have -- so resolution belongs at submit time,
    where the path is local and real, and ``pbrun`` does it before this is
    ever asked (``pbrun.py`` resolves ``--cwd`` and publishes the resolved
    string).

    An absent path answers ``False``: an item with no ``checkout_root``
    recorded is a thing we know nothing about, and inventing a pin for it
    would put a number in the census that no path put there.
    """

    text = str(path or "")
    if not text:
        return False
    return not Path(text).is_relative_to(SHARED_ROOT)


def normalize_placement_tags(tags: Sequence[object]) -> list[str]:
    """Canonicalize the exact tag conjunction the queue matcher enforces."""

    if isinstance(tags, (str, bytes)):
        raise PoolContractError("pool item tags must be a sequence of tags")
    normalized = {str(tag) for tag in tags}
    if any(not tag or tag.strip() != tag for tag in normalized):
        raise PoolContractError("pool item tags must be nonempty trimmed strings")
    return sorted(normalized)


class PoolError(pb.PrismaBuildError):
    """A queue-level failure, distinct from an action-level one."""


class PoolContractError(PoolError, ValueError):
    """A queue record does not satisfy its schema."""


class AmbiguousClaimHolder(PoolContractError):
    """Contradictory committed reservations forbid concluding a claim."""


class WithdrawnActionError(PoolContractError):
    """A publication refused because the key carries a live cancellation.

    Automatic republishing -- the window handing out a mover again -- must
    never retire an operator's withdrawal marker by writing over it.  The
    check is made inside ``publish``'s transition lock, so a cancellation
    racing the publication either wins it outright or is cancelled again;
    it is never lost between read and write (#708 review).
    """


class ActionAlreadyLiveError(PoolContractError):
    """A ``refuse_if_live`` publication refused: the queue already has the key.

    ``publish`` overwrites ``ready/<key>`` whatever state the key is in, which
    for a publisher that did not mean to duplicate costs twice: it loses the
    first submission's ``published_unix``, so every waiter pinned to that
    generation is left with no ending to find (#812), and after a claim it
    queues a second run of an action that is still going, which ``recompute``
    then executes rather than answering from the receipt (#810).

    Raised only when the publisher passed ``refuse_if_live``.  A fresh
    generation over a live key stays the default, because that is how an
    operator asks for the same work again.

    The refusal carries what a duplicate submitter needs in order to stop
    being one: ``state`` is ``ready`` or ``claimed``, and ``generation`` is
    the live row's ``published_unix`` -- the generation to wait on -- or
    ``None`` when that row states no readable one.  ``pbrun`` turns the
    refusal into an attachment rather than a second copy.

    It is a ``PoolContractError`` so the automatic republishers that already
    catch one (``tier_loop``, ``produced_output``) keep the handling they
    have.  The preemption requeue and the resign handoff never meet it: both
    name the claim they revive through ``preempted_claim``.
    """

    def __init__(
        self, action_key: str, *, state: str, generation: float | None,
    ) -> None:
        self.action_key = action_key
        self.state = state
        self.generation = generation
        super().__init__(
            f"{action_key[:12]} is already live in {state}"
            f"{f' as generation {generation}' if generation is not None else ''}"
            "; wait on that generation, or withdraw it before publishing "
            "again. A republication that revives an interrupted claim names "
            "it with preempted_claim.")


class ExecutionBudget(NamedTuple):
    """How long this action may run, and why that is the number.

    ``requested`` is the submitter's sealed ``execution_timeout_s``;
    ``ceiling`` is the worker loop's own safety limit; ``effective`` is what
    actually kills the action.  All three, because the two-field version of
    this -- one clamped float -- is what made #293 undiagnosable: the #275
    campaign asked for 13000 s, was silently given 7200 s, and was killed at
    7200 s with nothing anywhere recording that a clamp had happened.  A
    receipt has to be able to say what governed, not just that time ran out.

    ``None`` in any field means unbounded, which is a real answer and not a
    missing one: neither the submitter nor the loop is obliged to name a limit.
    """

    effective: float | None
    requested: float | None
    ceiling: float | None

    @property
    def clamped(self) -> bool:
        """Did the worker's ceiling, and not the submitter, decide?"""

        return (self.requested is not None and self.ceiling is not None
                and self.ceiling < self.requested)

    def as_record(self) -> dict[str, object]:
        """The fields an outcome carries so a receipt can be read years later."""

        return {
            "execution_timeout_s": self.effective,
            "execution_timeout_requested_s": self.requested,
            "execution_timeout_ceiling_s": self.ceiling,
            "execution_timeout_clamped": self.clamped,
        }


def execution_budget(
    item: Mapping[str, object], ceiling: float | None
) -> ExecutionBudget:
    """The deadline this action runs under, with both numbers behind it."""

    requested = _requested_execution_timeout(item)
    if requested is None:
        return ExecutionBudget(ceiling, None, ceiling)
    effective = requested if ceiling is None else min(requested, ceiling)
    return ExecutionBudget(effective, requested, ceiling)


class ProgressPhase(NamedTuple):
    """One declared phase of an action, and the quiet it is allowed in it."""

    name: str
    requested_grace_s: float
    ceiling_s: float | None

    @property
    def effective_grace_s(self) -> float:
        if self.ceiling_s is None:
            return self.requested_grace_s
        return min(self.requested_grace_s, self.ceiling_s)

    @property
    def clamped(self) -> bool:
        return (self.ceiling_s is not None
                and self.ceiling_s < self.requested_grace_s)

    def as_record(self) -> dict[str, object]:
        return {
            "name": self.name,
            "grace_requested_s": self.requested_grace_s,
            "grace_ceiling_s": self.ceiling_s,
            "grace_s": self.effective_grace_s,
            "grace_clamped": self.clamped,
        }


class ProgressPolicy(NamedTuple):
    """What quiet this action is allowed, and why that is the number.

    ``ExecutionBudget`` says what governs a *deadline*; this says what governs
    a *stall*, and reports itself the same way and for the same reason.  #293
    was undiagnosable because a clamp left no trace, and a stall allowance
    silently cut by a worker ceiling would be the same defect wearing the new
    contract's clothes.

    ``no_progress_bound_s`` is the honest total: every declared phase is
    entered at most once (per committed count in cyclic mode), so an action that
    never commits anything at all ends within the sum, and that sum is a
    number a submitter chose from the phases the work actually has.
    """

    phases: tuple[ProgressPhase, ...]
    ceiling_s: float | None
    cycle: bool = False

    @property
    def no_progress_bound_s(self) -> float:
        return sum(phase.effective_grace_s for phase in self.phases)

    @property
    def clamped(self) -> bool:
        return any(phase.clamped for phase in self.phases)

    def index_of(self, name: str) -> int | None:
        for index, phase in enumerate(self.phases):
            if phase.name == name:
                return index
        return None

    def as_record(self) -> dict[str, object]:
        return {
            "progress_contract": pb.PROGRESS_RECORD_SCHEMA_V1,
            "progress_phases": [phase.as_record() for phase in self.phases],
            "progress_stall_ceiling_s": self.ceiling_s,
            "progress_stall_clamped": self.clamped,
            "progress_no_progress_bound_s": self.no_progress_bound_s,
            "progress_cycle": self.cycle,
        }


def progress_policy(
    item: Mapping[str, object], ceiling: float | None
) -> ProgressPolicy | None:
    """The sealed stall policy of this action, with the worker's ceiling on it.

    Read from the same sealed request the deadline is, and for the same
    reason: a policy the receipt reports and a policy the worker enforces that
    disagreed would be worse than either alone.
    """

    declared = _sealed_progress_policy(item)
    if declared is None:
        return None
    phases = declared["phases"]
    assert isinstance(phases, Sequence)
    return ProgressPolicy(
        tuple(
            ProgressPhase(str(phase["name"]), float(phase["grace_s"]), ceiling)
            for phase in phases
        ),
        ceiling,
        cycle=bool(declared.get("cycle")),
    )


def _sealed_produced_output_batch(
    cas_root: str | Path,
    action_key: str,
) -> tuple[Mapping[str, object] | None, bool]:
    """Read the sealed batch reference and request presence (R5).

    Returns ``(value_or_None, request_present)``. Only key ABSENCE means no
    requirement: a present non-mapping value -- including explicit JSON
    ``null`` -- is malformed and refuses. No request file (legacy direct
    publish) declares nothing. A request file that exists but is unreadable,
    undecodable, invalid, key-mismatched, or has non-object params refuses:
    missing/corrupt authority never silently becomes legacy. Shape validation
    belongs to the caller (filed template + namespace + residency bind),
    which never trusts this mapping beyond it being the real sealed params.
    """

    key = str(action_key)
    request = Path(str(cas_root)) / "requests" / key[:2] / f"{key}.json"
    try:
        raw = pb._read_regular_file_nofollow(request, where="pool action request")
    except FileNotFoundError:
        return (None, False)
    try:
        action = pb.validate_action(
            pb._decode_strict_json(raw, where="pool action request"))
    except (pb.ActionContractError, pb.CASTamperError, pb.CASUnavailableError,
            ValueError, OSError) as exc:
        raise PoolContractError(
            f"pool action request unreadable: {exc}") from exc
    if action["action_key"] != key:
        raise PoolContractError("pool action request does not match the claimed key")
    params = action.get("params")
    if not isinstance(params, Mapping):
        raise PoolContractError("pool action request params must be an object")
    if "produced_output_batch" not in params:
        return (None, True)
    value = params["produced_output_batch"]
    if not isinstance(value, Mapping):
        raise PoolContractError(
            "action.params.produced_output_batch must be an object")
    return (value, True)


def _sealed_progress_policy(
    item: Mapping[str, object],
) -> Mapping[str, object] | None:
    """Read the declared policy from the sealed request, or None."""

    key = str(item["action_key"])
    request = Path(str(item["cas_root"])) / "requests" / key[:2] / f"{key}.json"
    try:
        raw = pb._read_regular_file_nofollow(request, where="pool action request")
    except FileNotFoundError:
        # Same rule as the deadline: a legacy launcher with no request declares
        # nothing, and the canonical worker refuses a missing request itself.
        return None
    action = pb.validate_action(pb._decode_strict_json(raw, where="pool action request"))
    if action["action_key"] != key:
        raise PoolContractError("pool action request does not match the claimed key")
    try:
        return pb.action_progress_policy(action)
    except pb.ActionContractError as exc:
        raise PoolContractError(str(exc)) from exc


class ProgressWatch:
    """Accept semantic advancement from one launch, and refuse everything else.

    The rule, in one place because every way of getting it slightly wrong ends
    with a stuck action kept alive:

    * the record's schema is exactly the versioned one;
    * its token is the one this launch minted, so a previous attempt that
      outlived SIGKILL and still holds the path cannot report for this one;
    * its phase is one the sealed policy declared;
    * ``units_completed`` is finite and not negative;
    * and it either passes the highest counter accepted so far, or enters a
      phase later than the highest entered so far. In cyclic mode, a phase
      may instead grant its allowance once per cumulative committed count;
      a regressing count is always rejected.

    A report with neither a new count nor an eligible phase grant, an
    undeclared phase, a foreign token and unparsable bytes are rejected and
    counted rather than discarded so a terminal record can say what the
    reporter was actually doing.  Launcher liveness and pipe bytes are not
    considered here at all: :func:`_observe_execution` samples those, on
    purpose, as a different source that proves a different thing.
    """

    def __init__(
        self, path: Path, token: str, policy: ProgressPolicy, *, started: float
    ) -> None:
        self.path = path
        self.token = token
        self.policy = policy
        # Launch already grants the first phase's allowance. Reporting zero
        # completed units in that phase cannot grant it again.
        self.units_high_water: int | float = 0
        # In cyclic mode, a phase gets one allowance per committed count.
        # Publishing a new count permits returning to a long earlier phase;
        # cycling names without more durable work cannot renew forever.
        self.phase_index = 0
        self.phases_granted = {0}
        self.phases_entered = 1
        self.last_advance_monotonic = started
        self.last_advance_unix: float | None = None
        self.last_accepted: dict[str, object] | None = None
        self.accepted = 0
        self.rejected = 0
        self.last_rejection: str | None = None
        self.sampled_unix: float | None = None

    @property
    def grace_s(self) -> float:
        """The allowance of the last accepted phase (highest in linear mode).

        Cyclic re-entry consumes a bounded per-count grant. Rejected phase
        reports never alter the allowance.
        """

        return self.policy.phases[self.phase_index].effective_grace_s

    def stall_deadline(self) -> float:
        return self.last_advance_monotonic + self.grace_s

    def _reject(self, reason: str) -> None:
        self.rejected += 1
        self.last_rejection = reason

    def sample(self, *, now: float) -> bool:
        """Read the reporter's file once; return whether it advanced.

        ``now`` is a ``time.monotonic()`` reading, deliberately: a wall clock
        that jumps -- forwards over a stalled action or backwards over a
        working one -- must not decide either.
        """

        self.sampled_unix = _now()
        try:
            raw = pb._read_regular_file_nofollow(
                self.path, where="action progress report",
                max_bytes=pb.MAX_ACTION_PROGRESS_BYTES,
            )
        except FileNotFoundError:
            # Before the first report, and after a reporter that never came.
            # Neither is advancement and neither is an error; the phase's own
            # allowance is what bounds it.
            return False
        except OSError as exc:
            self._reject(f"unreadable: {type(exc).__name__}")
            return False
        except (pb.ActionContractError, pb.CASTamperError, pb.CASUnavailableError) as exc:
            self._reject(f"unreadable: {type(exc).__name__}")
            return False
        try:
            record = pb._decode_strict_json(raw, where="action progress report")
        except (pb.ActionContractError, RecursionError):
            # A torn read is possible in principle even behind os.replace on a
            # shared filesystem; the next poll reads the whole file.
            self._reject("unparsable")
            return False
        if not isinstance(record, dict):
            self._reject("not an object")
            return False
        if record.get("schema") != pb.PROGRESS_RECORD_SCHEMA_V1:
            self._reject("wrong schema")
            return False
        if record.get("token") != self.token:
            self._reject("foreign token")
            return False
        index = self.policy.index_of(str(record.get("phase")))
        if index is None:
            self._reject("undeclared phase")
            return False
        units = record.get("units_completed")
        if (type(units) not in (int, float)
                or (type(units) is float and not math.isfinite(units)) or units < 0):
            self._reject("units_completed is not a finite count")
            return False
        advanced_units = units > self.units_high_water
        advanced_phase = index > self.phase_index
        if self.policy.cycle:
            if units < self.units_high_water:
                self._reject("regressing")
                return False
            advanced_phase = index not in self.phases_granted
        if not advanced_units and not advanced_phase:
            self._reject("replayed")
            return False
        if advanced_units:
            self.units_high_water = units
        if self.policy.cycle:
            if advanced_units:
                self.phases_granted.clear()
            self.phases_granted.add(index)
            if index != self.phase_index:
                self.phases_entered += 1
            self.phase_index = index
        elif advanced_phase:
            self.phases_entered += index - self.phase_index
            self.phase_index = index
        self.accepted += 1
        self.last_advance_monotonic = now
        reported = record.get("reported_unix")
        try:
            reported = (float(reported) if type(reported) in (int, float)
                        else None)
        except OverflowError:
            reported = None
        self.last_advance_unix = (
            reported if reported is not None and math.isfinite(reported)
            else _now()
        )
        self.last_accepted = {
            "phase": self.policy.phases[self.phase_index].name,
            "units_completed": self.units_high_water,
            "unit": (str(record["unit"]) if isinstance(record.get("unit"), str)
                     else None),
            "reported_unix": self.last_advance_unix,
        }
        return True

    def shift(self, seconds: float) -> None:
        """Retain quiet spent on shared I/O this loop, not the action, did."""

        self.last_advance_monotonic += seconds

    def as_record(self, *, now: float) -> dict[str, object]:
        """What a receipt carries so a reader can see what the action reported."""

        return {
            "source": "action-progress",
            "sampled_unix": self.sampled_unix,
            "accepted_count": self.accepted,
            "rejected_count": self.rejected,
            "last_rejection": self.last_rejection,
            "last_accepted": self.last_accepted,
            "quiet_s": max(0.0, now - self.last_advance_monotonic),
            "grace_s": self.grace_s,
            "phase": self.policy.phases[self.phase_index].name,
            "phases_entered": self.phases_entered,
        }


def _execution_timeout(item: Mapping[str, object], ceiling: float | None) -> float | None:
    """Read the deadline from the sealed request, never mutable queue metadata."""
    key = str(item["action_key"])
    request = Path(str(item["cas_root"])) / "requests" / key[:2] / f"{key}.json"
    try:
        raw = pb._read_regular_file_nofollow(request, where="pool action request")
    except FileNotFoundError:
        # Legacy/custom launchers can have no request. The canonical worker
        # independently refuses a missing request before executing any action.
        return ceiling
    action = pb.validate_action(pb._decode_strict_json(raw, where="pool action request"))
    if action["action_key"] != key:
        raise PoolContractError("pool action request does not match the claimed key")
    requested = action["params"].get("execution_timeout_s")
    if requested is None:
        return ceiling
    if (type(requested) not in (int, float) or not math.isfinite(requested)
            or requested <= 0):
        raise PoolContractError("execution_timeout_s must be a positive finite number")
    return float(requested) if ceiling is None else min(float(requested), ceiling)


def _requested_execution_timeout(item: Mapping[str, object]) -> float | None:
    """What the submitter sealed, before any ceiling is applied.

    Reads the same sealed request ``_execution_timeout`` does, and refuses the
    same values, because a budget the receipt reports and a budget the worker
    enforces that disagreed would be worse than either alone.
    """

    return _execution_timeout(item, None)


@contextmanager
def _execution_checkout(item: Mapping[str, object]) -> Iterator[Path]:
    """Yield the live path or a private checkout of the sealed snapshot.

    The sequence itself is ``materialize._execution_checkout``; SLURM runs the
    same one.  What this adds is the root: ``LOCAL_CHECKOUT_ROOT`` is read here,
    at call time, so the queue's root is the queue's to state.
    """

    try:
        with materialize._execution_checkout(
            item, local_checkout_root=LOCAL_CHECKOUT_ROOT
        ) as checkout_root:
            yield checkout_root
    except materialize.MaterializationContractError as exc:
        # The queue's callers and tests speak the queue's contract error; the
        # sequence's own type is the SLURM job's to see.
        raise PoolContractError(str(exc)) from exc


def _publish_immutable(path: Path, raw: bytes, *, where: str) -> None:
    """First-writer-publish one attempt artifact; refuse conflicting bytes."""

    won = pb._atomic_publish(path, raw)
    if won:
        return
    observed = pb._read_regular_file_nofollow(
        path, where=where, require_readonly=True
    )
    if observed != raw:
        raise PoolContractError(
            f"{where} conflicts with the immutable record already filed: {path}"
        )


def read_queue_record(path: Path) -> dict[str, object] | None:
    """Read one JSON object without mutation; missing/empty returns None.

    Invalid records raise PoolContractError; I/O errors including ESTALE stay
    loud. This direct read has no deadline: callers on shared mounts must bound
    it. Stale-entry tolerance belongs to offer enumeration, not this API.
    """
    return _read_json(path)


def _read_json(
    path: Path, *, tolerate_stale: bool = False
) -> dict[str, object] | None:
    """The record at ``path``, or ``None`` if it is not there to read.

    ``tolerate_stale`` extends "not there" to cover ``ESTALE``, and belongs
    only to a caller enumerating a directory whose entries come and go.  A
    worker offer is exactly that: it appears when a loop starts and is gone
    when the box leaves, so a file that vanishes between the ``glob`` and the
    read is ordinary.  ``ENOENT`` has always been read that way here; on NFS
    the same event arrives as ``ESTALE`` through a directory handle the client
    had already cached, and untreated it left this function, ``offers()`` and
    ``placeable_hosts()`` and killed a submission before it queued anything --
    a whole ``pbtest`` shard, 74 tests never run, for two offer files an
    operator had tidied away (#208).

    It is opt-in because ``ESTALE`` is not only "this file vanished": it is
    also what a dead mount or a stale parent handle returns.  An enumerator
    has already had its ``glob`` succeed, so it holds the evidence that the
    directory is live and the entry is not.  A caller addressing one record by
    key holds no such evidence -- ``reclaim`` asserting exactly one terminal
    record, a lease read, a pass record -- and answering "absent" there would
    turn a broken mount into a confident wrong verdict.  Those callers keep
    the default and stay loud.
    """

    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        if tolerate_stale and exc.errno == errno.ESTALE:
            return None
        raise
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise PoolContractError(f"queue record is not valid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise PoolContractError(f"queue record is not an object: {path}")
    return value


def _read_json_fresh(path: Path) -> dict[str, object] | None:
    """``_read_json`` for a record another box files under a key this one polls.

    The queue is on NFS with default attribute caching.  A lookup of a name
    that does not exist yet is cached as a negative entry, and the client keeps
    answering ``ENOENT`` from it until the parent directory's attributes are
    revalidated -- up to ``acdirmin``, 30 s here.  A poller always looks first,
    so it always caches the miss.  On sparky, ``move_record`` saw a receipt
    dl380g10 had already filed 26.3 s to 26.6 s late with a plain read, and
    0.04 s to 0.25 s late with the parent opened first
    (``tools/fleet/qualify_record_visibility.py``, #808).  A produced-output
    owner waited that out on every staged group, after a copy that took 4 s.

    ``slurm_lane._read_json_object`` and ``pbrun.terminal_record`` follow the
    same rule by listing the parent.  Opening it is used here because these
    reads are polled several times a second and a listing costs what the
    directory holds (``done`` held 17,117 names when this was written): an
    open is one round trip, and close-to-open consistency makes it the
    revalidation.

    A stale "absent" is not only slow.  ``produced_output`` republishes a
    mover row it reads as absent, and an egress whose row it does not read as
    live and whose receipt it does not see.  In the 2026-09-21 live cycle
    those stale misses republished each egress three times, so each ran four
    times and each retirement took more than 20 s for a 4 s action.  The
    re-runs were no-ops (``stage_release.evict`` is idempotent), and the last
    one overwrote the receipt the retirement had been filed from.

    Only a miss pays for the revalidation: a record that is there is one
    read, as before.  It is best effort.  A parent that cannot be opened
    leaves the answer what the first read said, which is what this function
    answered before it revalidated anything, so no caller meets a failure it
    did not meet before; that read can still be stale.
    """

    record = _read_json(path)
    if record is not None:
        return record
    try:
        descriptor = os.open(
            path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    except OSError:
        return None
    os.close(descriptor)
    return _read_json(path)


def worker_argv(
    *,
    worker_script: str | Path,
    action_key: str,
    cas_root: str | Path,
    checkout_root: str | Path,
    recompute: bool = False,
) -> list[str]:
    """The canonical worker launch, identical to SLURM's minus its own gate.

    ``recompute`` appends ``--recompute``, and only the pool transport ever
    sets it: a movement node (a mover, an egress) is published with it because
    its effect is bytes on a tier, not a result the CAS can replay.  SLURM
    refuses the flag outright (``run-local --require-slurm-initial-start``),
    so the default keeps the two launches byte-identical for everything else.

    ``slurm.py`` pins ``worker_argv`` to an exact list and refuses anything else,
    so that what a scheduler runs is what the action key describes.  This
    transport holds the same line: the only difference is the absence of
    ``--require-slurm-initial-start``, which asserts SLURM job membership and is
    meaningless without a scheduler.  Keeping the rest byte-identical is what
    makes a result portable between transports -- an action executed here and
    the same action executed under SLURM must be the same execution, or the CAS
    receipt is comparing two different things.
    """

    return [
        str(worker_script),
        "run-local",
        "--action",
        str(Path(cas_root) / "requests" / action_key[:2] / f"{action_key}.json"),
        "--cas-root",
        str(cas_root),
        "--checkout-root",
        str(checkout_root),
    ] + (["--recompute"] if recompute else [])


def _drain(
    process: subprocess.Popen[str], *, timeout_s: float
) -> tuple[str, str, bool]:
    """Collect what the pipes hold without waiting on whoever still holds them.

    The action inherits the launcher's stdout and stderr, so the read side sees
    EOF only when the *action* exits -- not when the launcher does.  An
    unbounded ``communicate()`` after a kill therefore blocks for exactly as
    long as the runaway it was called to stop.  Take the partial output the
    timeout carries instead, close the pipes, and say so.

    Returns ``(stdout, stderr, survived)``, where ``survived`` is True when EOF
    never arrived.
    """

    try:
        out, err = process.communicate(timeout=timeout_s)
        return out or "", err or "", False
    except subprocess.TimeoutExpired as exc:
        # ``communicate`` attaches what it had read to the timeout, undecoded
        # even under ``text=True``.  A truncated log beats no log.
        partial = []
        for chunk in (exc.output, exc.stderr):
            if chunk is None:
                partial.append("")
            elif isinstance(chunk, bytes):
                partial.append(chunk.decode("utf-8", errors="replace"))
            else:
                partial.append(str(chunk))
        for pipe in (process.stdout, process.stderr):
            if pipe is not None:
                pipe.close()
        return partial[0], partial[1], True


def _scan(directory: Path):
    """List a directory that another process may be deleting underneath us.

    Every ledger scan walks entries a concurrent worker is free to remove --
    ``release`` is documented safe to call twice, so two of them will have one
    ``rmdir`` the directory the other is mid-iteration over.  At one worker per
    box that never happens; at sixty it happens within minutes, and the worker
    dies with ``FileNotFoundError`` on ``iterdir`` rather than losing a lease
    gracefully.  A directory that vanished held nothing this caller can still
    act on, so the honest answer is an empty listing, not an exception.
    """

    try:
        return sorted(directory.iterdir())
    except (FileNotFoundError, NotADirectoryError):
        return []


def _glob(directory: Path, pattern: str):
    """``Path.glob`` with the same disappearing-directory contract as `_scan`."""

    try:
        return sorted(directory.glob(pattern))
    except (FileNotFoundError, NotADirectoryError):
        return []


def _scan_visible(directory: Path):
    """List a directory LOUDLY: every error propagates to the caller.

    The counterpart to :func:`_scan` for authoritative censuses (mint
    grow, dead-name reclaim): a holder this listing cannot read makes
    the census unknown, and the caller must refuse the decision --
    retain rather than mint -- instead of narrowing the view.  Reads
    (``capacity``, ``available``) and shrink-only scans keep the
    tolerant :func:`_scan`, whose errors understate toward refusal.
    """

    return sorted(directory.iterdir())


def _glob_visible(directory: Path, pattern: str):
    """Error-visible name match for authoritative censuses.

    Same names as :func:`_glob` on a readable tree (``Path.glob`` uses
    ``fnmatch`` for the leaf), but unreadable directories raise
    instead of reading empty: a hidden live holder must abort a
    reissue, never excuse one.  See :func:`_scan_visible`.
    """

    return sorted(path for path in _scan_visible(directory)
                  if fnmatch.fnmatchcase(path.name, pattern))


def held_names_visible(ledger, key: str) -> set[str]:
    """One holder's capacity-token names, error-visible.

    True absence (no holder directory) reads empty, exactly like the
    tolerant scans: a key that holds nothing holds nothing.  Every other
    listing failure -- an unreadable holder directory above all -- raises,
    because an authoritative mutation cannot tell "holds nothing" from
    "cannot see it holding" and must retain (:func:`_glob_visible` is the
    accepted #742 census semantics this reuses; ``Path.glob`` hides
    ``EACCES`` as an empty listing, which is the trap this exists for).
    """

    try:
        return {path.name
                for path in _glob_visible(ledger.held_dir / key, "*-*")}
    except (FileNotFoundError, NotADirectoryError):
        return set()


def _process_alive(pid: int) -> bool:
    """True while ``pid`` exists and has not already exited.

    ``os.kill(pid, 0)`` is the usual test and it is the wrong one here.  When
    ``execute`` stops its own child, that child is a zombie between its exit and
    the ``communicate()`` that reaps it -- and a zombie answers signal 0 quite
    happily.  A stop that believed it would climb its whole escalation ladder,
    signalling harder and harder at a process that had already died.
    """

    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as handle:
            # The comm field can contain spaces and parentheses; everything
            # after the last ")" is fixed-width, and state is its first field.
            after_comm = handle.read().rsplit(") ", 1)[-1].split()
    except OSError:
        return False
    return bool(after_comm) and after_comm[0] != "Z"


def _observe_child(scope, *, sampled: float,
                   last_output_unix: float | None) -> dict[str, object]:
    """The daemon-spawned payload, seen through the cgroup it was put in.

    ``resource_exec.py`` is a proxy: it hands the broker its stdio and waits on
    the socket, and the *broker* forks the payload.  So the payload is not a
    descendant of this worker and no process tree from here reaches it.  Action
    ``766d7ae5e0382b755a1189d4c1c3a42407d90fdb3cea56898b02b26855908489`` is
    what that costs: ``launcher_alive: true`` with ``stdout_bytes: 218`` for
    the whole 3600 s ceiling, while a pytest child burned 15% of a core in a
    futex wait.  A shard that hangs 31 s in looked exactly like one that was
    working, for an hour.

    The cgroup is what the worker and the broker's payload do have in common,
    so it is read here: the pids in it, whether any is still a live process,
    and the CPU the kernel has charged it.  "CPU advancing, output not" is the
    signature the hung action had and the one a claim-loop consumer needs;
    neither number alone says it.

    Fails closed.  When there is no scope, no cgroup, or nothing readable, the
    record is ``{"source": "unobserved", ...}`` with **no** ``alive`` field:
    an unreadable cgroup is not an empty one, and a liveness this worker
    cannot see is one it must not report.
    """

    if scope is None:
        return {"source": "unobserved",
                "detail": "action runs outside a resource scope, so no "
                          "daemon-spawned payload exists to observe"}
    path = getattr(scope, "cgroup_path", None)
    if path is None:
        return {"source": "unobserved",
                "detail": "resource scope has no cgroup yet"}
    errors: list[str] = []
    try:
        pids = resource_scope.scope_pids(Path(path), errors=errors)
    except Exception as exc:  # a sample must never end the attempt
        return {"source": "unobserved",
                "errors": [f"{type(exc).__name__}: {exc}"]}
    if errors and not pids:
        # Refused reads and an empty group are indistinguishable from here.
        return {"source": "unobserved", "errors": errors[-8:]}
    record: dict[str, object] = {
        "source": "resource-scope-cgroup",
        # Bounded: a fan-out payload can hold thousands, and a lease is read
        # far more often than this list is needed in full.
        "pids": sorted(pids)[:64],
        "pid_count": len(pids),
        "alive": any(_process_alive(pid) for pid in pids),
    }
    if last_output_unix is not None:
        record["silent_s"] = max(0.0, sampled - float(last_output_unix))
    try:
        counters = resource_scope.read_cgroup(Path(path))
    except (OSError, ValueError, KeyError) as exc:
        errors.append(f"{type(exc).__name__}: {exc}")
    else:
        for name in ("cpu_seconds", "cpu_user_seconds", "cpu_system_seconds"):
            if name in counters:
                record[name] = counters[name]
    if errors:
        record["errors"] = errors[-8:]
    return record


def _observe_execution(
    process: subprocess.Popen,
    previous: Mapping[str, object] | None = None,
    *, stdout: bytes | None = None, stderr: bytes | None = None,
    scope=None,
) -> dict[str, object]:
    """Sample our child and cumulative pipe buffers, without shared I/O.

    A live launcher can wait on a silent or blocked payload. Neither its
    existence nor output is proof of useful application progress or permission
    to retry. The sample time must survive a delayed heartbeat publication.
    ``TimeoutExpired`` carries bytes even for a text-mode Popen.

    ``scope`` adds the half the pipes cannot see: the payload the resource
    daemon forked, which is nobody's descendant here.  See `_observe_child`.
    """
    before = previous or {}
    counts = {"stdout_bytes": len(stdout or b""), "stderr_bytes": len(stderr or b"")}
    alive = process.poll() is None
    sampled = _now()
    changed = any(count > before.get(name, 0) for name, count in counts.items())
    last_output = sampled if changed else before.get("last_output_unix")
    return {"source": "launcher-pipes", "sampled_unix": sampled, "launcher_alive": alive, **counts,
            "last_output_unix": last_output,
            "child": _observe_child(scope, sampled=sampled,
                                    last_output_unix=last_output)}


def action_process_groups(launcher_pid: int) -> list[int]:
    """The process groups this launcher's children lead -- i.e. the action.

    ``execute`` launches ``worker.py run-local``, and that launcher is the only
    pid this queue ever holds.  The *action* -- the pytest, the encode, the
    thing holding the cores and the GPU -- is one level further down, and
    ``core.run_local_action`` starts it with ``start_new_session=True``, so it
    leads its own process group and **no signal aimed at the launcher reaches
    it**.  That is precisely why cancelling by hand meant ``kill -TERM -$pgid``
    with a pgid found by eye.  This function is that lookup, done by the queue.

    Only a child that leads its own group is returned (``getpgid(c) == c``).  A
    child sharing someone else's group is sharing *this worker's* -- ``execute``
    does not start a new session -- and signalling that group would take the
    worker loop down with the action.
    """

    try:
        raw = Path(f"/proc/{launcher_pid}/task/{launcher_pid}/children").read_text()
    except OSError:
        return []
    groups: list[int] = []
    for token in raw.split():
        try:
            child = int(token)
        except ValueError:
            continue
        try:
            if os.getpgid(child) == child:
                groups.append(child)
        except OSError:
            continue          # it exited between the read and the lookup
    return groups


def launcher_owns_action(pid: int, action_key: str) -> bool:
    """Is ``pid`` still this action's launcher, or a recycled number?

    The launcher's argv names the action's request file, so the key is in its
    command line.  A lease can outlive the process it describes -- that is what
    makes it a lease -- and a withdrawal that signalled a recycled pid would
    kill whatever the box started next.

    The same predicate as ``find_launcher_pids``, and for the same reason: the
    key alone is not enough, because a command line that *mentions* the key is
    not a launcher.  ``pbrun --withdraw <full digest>`` is one, and a stale
    foreign ``child_pid`` colliding with that shell's pid would have this
    function signal the operator's own terminal.  The canonical ``run-local``
    verb is what separates running the action from talking about it.
    """

    if not action_key:
        return False
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as handle:
            raw = handle.read()
    except OSError:
        return False
    return action_key.encode() in raw and b"run-local" in raw


def find_launcher_pids(action_key: str) -> list[int]:
    """Every live launcher of this action on this box, found from ``/proc``.

    The lease names the process to signal, but only since withdrawal existed: a
    worker started on earlier bytes writes a lease with no ``child_pid``, and a
    cancellation is reached for on a bad night rather than after a fleet roll.
    So the lease is the fast path and this is the one that always works.

    The scan is exact, not a name match.  A launcher's argv carries the 64-hex
    action key *and* the canonical ``run-local`` verb ``worker_argv`` pins, so a
    process with both is a launcher for this action and nothing else is.  The
    verb is required as well as the key so that a withdrawal invoked with the
    full digest cannot match the operator's own command line and signal itself.
    """

    if len(action_key) != 64:
        return []
    needle = action_key.encode()
    mine = os.getpid()
    found: list[int] = []
    for entry in _scan(Path("/proc")):
        if not entry.name.isdigit() or int(entry.name) == mine:
            continue
        try:
            with open(entry / "cmdline", "rb") as handle:
                raw = handle.read()
        except OSError:
            continue          # it exited, or it is not ours to read
        if needle in raw and b"run-local" in raw:
            found.append(int(entry.name))
    return found


def _docker_containers_with_label(label: str, value: str) -> list[str]:
    """Every container id the local daemon holds under one exact label.

    ``-a``, so a created-but-never-started container is listed too: that is the
    one form of leftover a running-process census cannot see, and the only one
    left once an action's payload has stopped.  A failed query is an unknown
    answer and therefore an exception; callers retain capacity on it.
    """

    result = subprocess.run(
        [DOCKER, "ps", "-aq", "--filter", f"label={label}={value}"],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise PoolContractError(
            f"docker ownership query failed ({result.returncode}): {detail}")
    return sorted({line.strip() for line in result.stdout.splitlines() if line.strip()})


def _docker_owned_container_ids(owner: str) -> list[str]:
    """Container ids carrying this action's ownership label.

    Docker payloads are children of ``containerd-shim``, not of the action
    group, so the daemon's label index is the authoritative join back to the
    action.
    """

    return _docker_containers_with_label(CONTAINER_OWNER_LABEL, owner)


def _docker_remove_containers(container_ids: list[str]) -> list[str]:
    """Force-remove exactly the container ids the ownership query returned."""

    if not container_ids:
        return []
    result = subprocess.run(
        [DOCKER, "rm", "-f", *container_ids],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise PoolContractError(
            f"docker cleanup failed ({result.returncode}): {detail}")
    return sorted({line.strip() for line in result.stdout.splitlines() if line.strip()})


def terminate_action(
    launcher_pid: int, *, grace_s: float = WITHDRAW_GRACE_S
) -> dict[str, object]:
    """Stop a running action and everything it started.  Safe when it is gone.

    Three rungs, each with its own reason, and each climbed only when the one
    below went unanswered:

    1. ``SIGTERM`` to the **action's** process group.  This is the operator's
       own manual move, and it is the rung that reaches the work: the launcher
       is not in that group and signalling it does nothing to the pytest.
    2. ``SIGINT`` to the launcher, if it is still alive.  Not ``SIGTERM``:
       Python's default disposition for ``SIGTERM`` kills the interpreter where
       it stands, while ``SIGINT`` raises ``KeyboardInterrupt``, and
       ``core.run_local_action``'s ``except BaseException`` branch then reaps
       its own action group and releases the result lock on the way out.  The
       handled signal is the one that unwinds in order.
    3. ``SIGKILL`` to both, for whatever answers neither.

    Every signal is best effort: a process that has already gone raises
    ``ProcessLookupError``, and that is the successful case, not a failure.
    Which is what makes the whole verb idempotent -- withdrawing twice is two
    lookups and no signals.
    """

    groups = action_process_groups(launcher_pid)
    sent: list[str] = []

    def alive() -> bool:
        return _process_alive(launcher_pid) or any(_process_alive(p) for p in groups)

    def settle(seconds: float) -> bool:
        deadline = _now() + seconds
        while _now() < deadline:
            if not alive():
                return False
            time.sleep(0.05)
        return alive()

    for pgid in groups:
        try:
            os.killpg(pgid, signal.SIGTERM)
            sent.append(f"TERM -{pgid}")
        except OSError:
            pass
    if settle(grace_s / 2.0):
        try:
            os.kill(launcher_pid, signal.SIGINT)
            sent.append(f"INT {launcher_pid}")
        except OSError:
            pass
    if settle(grace_s / 2.0):
        for pgid in groups:
            try:
                os.killpg(pgid, signal.SIGKILL)
                sent.append(f"KILL -{pgid}")
            except OSError:
                pass
        try:
            os.kill(launcher_pid, signal.SIGKILL)
            sent.append(f"KILL {launcher_pid}")
        except OSError:
            pass
    return {
        "launcher_pid": int(launcher_pid),
        "action_pgids": groups,
        "signals": sent,
        "still_alive": alive(),
    }


class _Insufficient(Exception):
    """Internal: a demand could not be met in full."""


#: Prefix of a claimant-private acquisition directory under ``held/``.  An
#: action key is 64 hex characters, so a name carrying a dot cannot collide
#: with one, and every reader that addresses a holder by key sees nothing.
ACQUIRING_PREFIX = "claiming."


#: What makes two readings of ``claimed/<key>.json`` the same claim.  The owner
#: alone would be enough for two different workers, but not for one worker's
#: two attempts at the same action, which is the case the reaper creates.
_CLAIM_IDENTITY = ("claimed_by", "claimed_unix", "published_unix", "attempts")


#: The resource fields every ending row carries, absent on a record filed
#: before issue #372 Tier 0.  Present as ``None`` rather than missing, for the
#: same reason ``unreadable`` is: a reader tests one field instead of the
#: absence of one, and "not measured" never renders as a measurement of zero.
RESOURCE_SUMMARY_FIELDS = ("memory_peak_bytes", "io_read_bytes", "io_write_bytes",
                           "gpu_power_peak_w", "gpu_power_reference_w",
                           "gpu_power_peak_fraction", "gpu_framebuffer_used_peak_bytes",
                           "gpu_framebuffer_total_bytes")


def _measured(value: object) -> float | int | None:
    """A finite measurement, or ``None`` for anything that is not one.

    ``bool`` is excluded on purpose: ``True`` is an ``int`` in Python and would
    otherwise render as a peak of one byte.
    """

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value if math.isfinite(float(value)) else None


def resource_profile_summary(detail: Mapping[str, object]) -> dict[str, object]:
    """The three facts an ending can report about what its run cost.

    Peak memory and I/O come from the exact attempt's own accounting; GPU power
    comes from the box window, against the reference the device published, so
    the fraction says what it is a fraction of.
    """

    summary: dict[str, object] = {field: None for field in RESOURCE_SUMMARY_FIELDS}
    profile = detail.get("resource_profile")
    if not isinstance(profile, Mapping):
        return summary
    scope = profile.get("scope")
    if isinstance(scope, Mapping):
        summary["memory_peak_bytes"] = _measured(scope.get("memory_peak_bytes"))
    measured_io = profile.get("process_io")
    if isinstance(measured_io, Mapping):
        summary["io_read_bytes"] = _measured(measured_io.get("read_bytes"))
        summary["io_write_bytes"] = _measured(measured_io.get("write_bytes"))
    window = profile.get("box_window")
    gpu = window.get("gpu") if isinstance(window, Mapping) else None
    if isinstance(gpu, Mapping):
        summary["gpu_power_peak_w"] = _measured(gpu.get("power_w_peak"))
        summary["gpu_power_reference_w"] = _measured(gpu.get("power_reference_w"))
        summary["gpu_power_peak_fraction"] = _measured(
            gpu.get("power_peak_fraction_of_reference"))
        summary["gpu_framebuffer_used_peak_bytes"] = _measured(
            gpu.get("framebuffer_used_bytes_peak"))
        summary["gpu_framebuffer_total_bytes"] = _measured(
            gpu.get("framebuffer_total_bytes"))
    return summary


def _si_bytes(value: object) -> str:
    """Bytes at a glance, without pretending to a precision nobody reads."""

    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return "-"
    size = float(value)
    for unit in ("B", "K", "M", "G", "T"):
        if abs(size) < 1024 or unit == "T":
            return f"{size:.0f}{unit}" if unit == "B" else f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}T"


def describe_resource_profile(ending: Mapping[str, object]) -> str:
    """One column for the three, or ``-`` where none of them was measured.

    Kept beside the measurement rather than at each call site, for the same
    reason ``describe_placement_census`` is: a number two tools describe
    differently is a number nobody can grep for.  ``pbstatus`` prints it in the
    endings table and ``pbrun`` in the line it ends a run with.
    """

    parts = []
    if ending.get("memory_peak_bytes") is not None:
        parts.append(f"rss={_si_bytes(ending['memory_peak_bytes'])}")
    written, read = ending.get("io_write_bytes"), ending.get("io_read_bytes")
    if written is not None or read is not None:
        parts.append(f"io w={_si_bytes(written)} r={_si_bytes(read)}")
    peak = ending.get("gpu_power_peak_w")
    if peak is not None:
        reference = ending.get("gpu_power_reference_w")
        fraction = ending.get("gpu_power_peak_fraction")
        cell = f"gpu={float(peak):.1f}W"
        if reference is not None:
            cell += f"/{float(reference):.1f}W"
        if fraction is not None:
            cell += f"({float(fraction) * 100:.0f}%)"
        parts.append(cell)
    used = ending.get("gpu_framebuffer_used_peak_bytes")
    total = ending.get("gpu_framebuffer_total_bytes")
    if used is not None:
        parts.append(f"vram={_si_bytes(used)}/{_si_bytes(total)}")
    return " ".join(parts) if parts else "-"


def _reaped_children(
    before: resource.struct_rusage, after: resource.struct_rusage
) -> dict[str, object]:
    """What this parent's own kernel accounting says the child cost.

    Bracketing ``RUSAGE_CHILDREN`` around one launch gives that launch's
    figures, because every field but one is a running sum.  The exception is
    ``ru_maxrss``, which is a high-water mark over every child the process has
    ever reaped: it can be raised by this child but never lowered by it, so a
    mark that did not move is not this action's peak and is reported as absent
    rather than as somebody else's number.  The watermark itself is kept, since
    a peak this child could not have exceeded is still a bound on it.

    ``scope`` is on the record because the answer is not the action on a
    contained run.  There the payload is a child of the root resource broker
    and what this parent launched and reaped is the stdio proxy, so these
    figures cover the launcher.  The action's own peak and I/O come from its
    cgroup and from the ``/proc`` counters of the processes inside it.
    """

    watermark = int(after.ru_maxrss) * 1024   # ru_maxrss is KiB on Linux
    return {
        "source": "getrusage_children",
        "scope": "this parent's launched child and the descendants it reaped",
        "user_seconds": after.ru_utime - before.ru_utime,
        "system_seconds": after.ru_stime - before.ru_stime,
        "voluntary_context_switches": int(after.ru_nvcsw - before.ru_nvcsw),
        "involuntary_context_switches": int(after.ru_nivcsw - before.ru_nivcsw),
        "max_rss_watermark_bytes": watermark,
        "max_rss_bytes": (watermark if after.ru_maxrss > before.ru_maxrss
                          else None),
    }


def _same_claim(
    live: Mapping[str, object], snapshot: Mapping[str, object]
) -> bool:
    """Whether a live claimed record is the claim a worker actually ran.

    Only fields the queue writes once per claim, so the guards that rewrite a
    claimed record in place -- a container-cleanup retry, a withdrawal's
    ``max_attempts`` poison, a pending stop -- do not read as a different
    claim.
    """

    return all(
        live.get(field) == snapshot.get(field) for field in _CLAIM_IDENTITY
    )


def _check_claim_lease_identity(
    action_key: str, claim: Mapping[str, object], lease: Mapping[str, object] | None,
) -> None:
    """Refuse conflicting observations before replacing or cleaning ownership.

    Host-level reservations do not distinguish attempts on the same box.
    Missing legacy fields supply no extra proof; jointly stale reads remain
    outside this consistency check.
    """
    if lease is None:
        return
    conflicts = [
        claim_field for claim_field, lease_field in (
            ("claimed_by", "owner"), ("claimed_host", "host"),
            ("claimed_unix", "claimed_unix"),
            ("published_unix", "published_unix"),
        )
        if claim.get(claim_field) is not None
        and lease.get(lease_field) is not None
        and claim[claim_field] != lease[lease_field]
    ]
    if conflicts:
        raise AmbiguousClaimHolder(
            f"contradictory claim and lease identity for {action_key}: "
            f"{', '.join(conflicts)}; claim and reservations retained"
        )


def _is_acquisition(name: str) -> bool:
    """Whether a holder directory belongs to a claimant rather than an action.

    One predicate for both readers, because the two must agree exactly.  A name
    the sweep declines to recognise but ``held_keys`` reports as an action key
    is a leak nothing owns: no queue directory holds that name, so no reaper
    looks for it, and no sweep frees it.
    """

    return name.startswith(ACQUIRING_PREFIX)


def _acquisition_clock(holder: Path) -> float | None:
    """When a claimant began this acquisition, in seconds since the epoch.

    The clock is in the *name* because there is nowhere else to put it that
    survives: ``rename`` does not touch mtime, and the directory's own mtime is
    bumped by every token moved into it, so a claimant that took one token and
    died looks as fresh as one still working.  A name whose stamp this version
    cannot read falls back to mtime, which is wrong in the conservative
    direction -- too fresh, so swept later -- and never leaves the directory
    unowned.  ``None`` only when the directory has gone.
    """

    parts = holder.name.split(".", 3)
    if len(parts) >= 3:
        try:
            return int(parts[1]) / 1_000_000.0
        except ValueError:
            pass
    try:
        return holder.stat().st_mtime
    except OSError:
        return None


def _acquisition_claimant(name: str) -> tuple[str, int] | None:
    """The host and pid a claimant stamped on its acquisition, if readable."""

    parts = name.split(".")
    if len(parts) < 6:
        return None
    try:
        return parts[3], int(parts[4])
    except ValueError:
        return None


def _guarded_mutation(*, blocking: bool):
    """Run a ``ResourceLedger`` mutator under its mutation exclusion.

    Tier ledgers built by :meth:`PoolQueue.tier_ledger` carry the tier's
    mint lock as that exclusion, so every token rename on the ledger
    serializes against the reclaim headroom scan (see
    :meth:`PoolQueue._reclaim_dead_markers`): a held/private token renamed
    to free between the free listing and the holder listing is missed by
    both, and the overstated headroom reissues a dead name with no backing
    -- then a claimant takes the phantom before the same apply's retire
    can trim it (#733 R6).  Host ledgers carry no guard and behave exactly
    as before.

    ``blocking=False`` is the admission shape: the caller declines with
    its existing unavailable vocabulary instead of waiting (only
    :meth:`ResourceLedger.begin_acquire` uses it, returning ``None``).
    Every other mutator waits and completes under the lock, so a
    contended commit/abandon is never reported as success nor silently
    dropped.  A contended non-blocking guard returns ``None`` from the
    wrapped call.

    Every ``ResourceLedger`` mutator -- including the pending
    ``transfer_tokens`` extension (PR748), which must add the blocking
    guard on integration -- takes this.  Readers take nothing.  Internal
    helpers that assume the caller holds the guard say so; they take
    nothing themselves.
    """

    def decorator(fn):
        @wraps(fn)
        def wrapper(self, *args, **kwargs):
            with self._mutation_locked(blocking=blocking) as acquired:
                if not acquired:
                    return None
                return fn(self, *args, **kwargs)
        return wrapper
    return decorator


class ResourceLedger:
    """Per-host capacity, held as tokens that are acquired by ``rename``.

    Capacity is expressed as *countable* tokens rather than as a number in a
    file that everyone read-modify-writes, because this queue has exactly one
    concurrency primitive it trusts on NFS -- ``rename`` -- and a ledger that
    needed a second one would be a ledger with a second failure mode.  One
    token is one indivisible unit of a resource (``gpu`` is a device, ``mem_gb``
    is a gigabyte), so acquiring is renaming N of them out of ``free/`` and
    releasing is renaming them back.  A worker that dies holding tokens is
    recovered by the same reaper that recovers its claim, since the tokens are
    filed under the action key.

    Capacity is grown but never shrunk here: removing a token that another
    process holds is not expressible as a rename, and a box whose capacity
    dropped mid-flight is a configuration change, not a queue operation.
    """

    def __init__(self, root: str | Path, host: str | None = None, *,
                 mutation_guard=None) -> None:
        self.root = Path(root)
        self.host = host or socket.gethostname()
        self.last_token_shortage: dict[str, object] | None = None
        # Optional ``(*, blocking: bool) -> context manager`` yielding the
        # acquisition status.  Set only by PoolQueue.tier_ledger (the
        # tier's mint lock); host ledgers keep None and no new behavior.
        # The guard is chosen by the explicit factory, never by sniffing
        # the host/tier name.
        self._mutation_guard = mutation_guard

    def _mutation_locked(self, *, blocking: bool = True):
        """The ledger's mutation exclusion, or a no-op for host ledgers."""

        if self._mutation_guard is None:
            return nullcontext(True)
        return self._mutation_guard(blocking=blocking)

    def _strict_census(self) -> bool:
        """Whether this ledger refuses to mint from a partial view.

        Tier ledgers built by :meth:`PoolQueue.tier_ledger` (guard
        present) enumerate their grow/reclaim census error-visibly and
        abort the mint/reissue on any census error; host ledgers keep
        the legacy tolerant scans.  The fork is by explicit factory,
        never by name.
        """

        return self._mutation_guard is not None

    def _census_scan(self, directory: Path):
        """Authoritative-or-tolerant directory listing by ledger kind."""

        if self._strict_census():
            return _scan_visible(directory)
        return _scan(directory)

    def _census_glob(self, directory: Path, pattern: str):
        """Authoritative-or-tolerant name match by ledger kind."""

        if self._strict_census():
            return _glob_visible(directory, pattern)
        return _glob(directory, pattern)

    @property
    def base(self) -> Path:
        return self.root / self.host

    @property
    def free_dir(self) -> Path:
        return self.base / "free"

    @property
    def held_dir(self) -> Path:
        return self.base / "held"

    @property
    def minted_dir(self) -> Path:
        """Where the mint right for each token index is recorded, permanently.

        One file per index, created with ``O_EXCL`` and never renamed.  It is
        the only thing that decides whether an index has been minted, because
        the tokens themselves move and a scan of where they move cannot be
        made atomic.
        """

        return self.base / "minted"

    def configure_cpu_tiers(
        self, tiers: Mapping[str, Sequence[int]], *, initialize: bool = True,
    ) -> dict | None:
        """Bind token ordinals to CPUs once; all loops on a host must agree.

        Changing this map requires stopping workers, draining reservations,
        and removing cpu-map.json before restarting. Never reinterpret a held
        ordinal under another affinity or topology.

        With ``initialize=False``, only validate an existing map; absence
        returns ``None`` without touching holders or publishing a map. This
        immutable read needs no host admission. Initialization still belongs
        to the serialized capacity prelude.
        """
        record = {kind: list(tiers[kind]) for kind in ("preferred", "fallback")}
        cpus = record["preferred"] + record["fallback"]
        if (not cpus or any(type(c) is not int or c < 0 for c in cpus)
                or len(cpus) != len(set(cpus))):
            raise PoolContractError("CPU tiers must contain distinct nonnegative CPU IDs")
        path = self.base / "cpu-map.json"
        existing = _read_json(path)
        if existing is None:
            if not initialize:
                return None
            if self.held().get("cpu", 0):
                raise PoolContractError("drain legacy CPU reservations before enabling CPU tiers")
            self.base.mkdir(parents=True, exist_ok=True)
            pb._atomic_publish(path, pb._canonical_bytes(record))
            existing = _read_json(path)
        if existing != record:
            raise PoolContractError("CPU tier map differs: stop workers, drain reservations "
                                    "and remove cpu-map.json before changing topology")
        return record

    def cpu_allocation(self, holder: str, tiers: Mapping, *,
                       metadata: Mapping[str, object] | None = None) -> dict:
        """The actual CPUs represented by this claimant's held tokens.

        A caller that has already read the holder's metadata passes it, so the
        admission loop does not open the same shared file twice per holder.
        """
        if metadata is None:
            metadata = _read_json(self.held_dir / holder / cpu_admission.METADATA)
        if metadata is not None and "allocation" in metadata:
            return metadata["allocation"]
        cpus = self.cpu_ids(_glob(self.held_dir / holder, "cpu-*"), tiers)
        return {kind: [c for c in tiers[kind] if c in cpus]
                for kind in ("preferred", "fallback")}

    def cpu_ids(self, tokens, tiers: Mapping) -> list[int]:
        """The CPUs a sequence of ``cpu-<ordinal>`` tokens represents.

        One home for the rule that ``begin_acquire`` selects by and
        ``cpu_allocation`` reports: the suffix is a *token ordinal* into
        ``preferred + fallback``, never a CPU id.  An ordinal outside the
        configured topology is a contract error, because the token map is
        fixed for the life of a holder while the topology must not shrink
        underneath it.
        """
        ordered = list(tiers["preferred"]) + list(tiers["fallback"])
        cpus = []
        for token in tokens:
            index = int(token.name.split("-")[-1])
            if index >= len(ordered):
                raise PoolContractError("CPU token exceeds configured topology")
            cpus.append(ordered[index])
        return cpus

    def free_cpu_allocation(self, need: int, tiers: Mapping) -> list[int] | None:
        """The CPUs the next ``need`` free ``cpu-*`` tokens would be given.

        ``begin_acquire`` takes the first ``need`` free tokens in ``_glob``
        (sorted) order and maps them through the same ordinal rule, so a caller
        asking "which CPUs is this claim about to get?" reads one rule rather
        than keeping a second copy of the selection policy in step with it.
        ``None`` is the honest answer when the question cannot be answered --
        fewer free tokens than the demand, or an ordinal the topology no longer
        covers -- and callers treat it as unknown, never as idle.
        """
        tokens = _glob(self.free_dir, "cpu-*")[:need]
        if len(tokens) < need:
            return None
        try:
            return self.cpu_ids(tokens, tiers)
        except PoolContractError:
            return None

    def free_preferred(self, tiers: Mapping) -> int:
        return sum(int(token.name.split("-")[-1]) < len(tiers["preferred"])
                   for token in _glob(self.free_dir, "cpu-*"))

    @_guarded_mutation(blocking=True)
    def ensure_capacity(self, capacity: Mapping[str, int]) -> None:
        """Create any missing token of each declared kind, idempotently.

        **The marker mints, not the scan.**  This used to snapshot ``free/``,
        then scan ``held/``, then create anything neither listing had shown.
        Movement between the two listings makes a present token invisible to
        both: a token held by A, released while ``held/`` is being scanned and
        re-acquired by B, appears in neither, and ``O_EXCL`` at its free
        pathname does not protect a token of the same name under a holder.  The
        ledger then reported ``cpu=2`` for a configured ``cpu=1`` and admitted
        work against capacity that does not exist.  ``claim`` calls this on
        every poll, so the interleaving overlaps ordinary action turnover
        rather than only initialization.

        So the decision to mint index ``i`` is an ``O_EXCL`` create of
        ``minted/<kind>-<index>``, which never moves and is therefore never
        invisible.  An index whose marker exists is skipped: its token exists
        somewhere, or was deliberately retired.

        **Adoption mints nothing.**  Every ledger already on the shared store
        has free and held tokens and no markers, so the first call creates a
        marker for each token it finds and leaves the totals alone.

        Adoption is a scan, so it inherits the scan's blind spot: a union of
        ``free/`` and ``held/`` is missable in either order, by a concurrent
        release in one and a concurrent acquire in the other.  A token missed
        by adoption is minted a second time here, and that residual duplicate
        is *transient rather than permanent*, which is the property that makes
        it tolerable: the duplicate can only be the free copy of a name whose
        real token is held, and ``release`` renames a held token onto
        ``free/<name>``, replacing it.  The two copies therefore collapse to
        one the moment the holder finishes, without anything ever removing a
        token a holder is using.  An earlier revision of this fix tried to
        remove the duplicate on sight by inode; that reintroduced exactly the
        check-then-act over a set a concurrent rename mutates that the markers
        exist to retire, and it could take a token a second claimant had
        already acquired.

        Ordering note for the one window that remains: a process killed between
        the marker create and the token create loses that index until an
        operator removes the marker.  That direction under-declares capacity,
        which is the safe one; the marker cannot be created second without
        making the duplicate permanent again.

        Census refusal (#733 R6): on a tier ledger the grow census above
        (marker listing, adoption, per-name held check) is error-visible:
        an unreadable directory aborts the whole call having minted
        nothing -- a just-created marker with no proven token is removed
        again -- so capacity is never minted from a partial view.  The
        next cycle retries.  Host ledgers keep the legacy tolerant scans
        and propagation.
        """

        self.free_dir.mkdir(parents=True, exist_ok=True)
        self.held_dir.mkdir(parents=True, exist_ok=True)
        self.minted_dir.mkdir(parents=True, exist_ok=True)
        # One listing per call rather than an O_EXCL attempt per index: a
        # 96-token memory ledger is polled every few seconds, and the marker
        # remains the arbiter for anything this listing did not show.
        try:
            minted = {path.name for path in self._census_scan(self.minted_dir)}
            minted |= self._adopt_present_tokens(minted)
        except OSError:
            if not self._strict_census():
                raise
            return
        for kind, count in sorted(capacity.items()):
            total = int(count)
            if total < 0:
                raise PoolContractError(f"capacity for {kind!r} must not be negative")
            for index in range(total):
                name = f"{kind}-{index:04d}"
                if name in minted:
                    continue
                try:
                    descriptor = os.open(
                        self.minted_dir / name,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                        0o644,
                    )
                except FileExistsError:
                    continue
                os.close(descriptor)
                minted.add(name)
                try:
                    held = self._token_is_held(name)
                except OSError:
                    if not self._strict_census():
                        raise
                    # Unknown whether a holder has this name: the marker
                    # just created would strand the index.  Remove it and
                    # mint nothing this cycle; the next cycle retries.
                    (self.minted_dir / name).unlink(missing_ok=True)
                    return
                if held:
                    # Adoption did not see it, but a holder has it: the marker
                    # now accounts for that token and nothing is minted.  The
                    # check is a scan and can still miss, which is what the
                    # docstring's transient duplicate is.
                    continue
                try:
                    descriptor = os.open(
                        self.free_dir / name,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                        0o644,
                    )
                except FileExistsError:
                    continue      # a free token of this name already exists
                os.close(descriptor)

    def _token_is_held(self, name: str) -> bool:
        """Whether any holder here currently contains a token called ``name``.

        Error-visible on a tier ledger (an unreadable holder aborts the
        grow census); tolerant on a host ledger, as before.
        """

        if self._strict_census():
            return any(
                name in {path.name for path in _scan_visible(holder)}
                for holder in _scan_visible(self.held_dir)
                if holder.is_dir()
            )
        return any(
            (holder / name).exists()
            for holder in _scan(self.held_dir)
            if holder.is_dir()
        )

    def _adopt_present_tokens(self, minted: Container[str]) -> set[str]:
        """Record the mint right for every token this ledger already has.

        Called before minting so a ledger that predates the markers keeps the
        capacity it has instead of having it minted a second time.  Creates
        markers only; it never creates or removes a token.

        ``minted`` is the marker listing already read, so the steady state
        costs the directory walk and no syscall per token: every name is
        already known and only a ledger being adopted opens anything.

        The census is error-visible on a tier ledger: an unreadable free
        or holder directory propagates and aborts the grow, so adoption
        never narrows the view it marks from.  Host ledgers keep the
        tolerant scans.
        """

        adopted: set[str] = set()
        present = {path.name for path in self._census_glob(self.free_dir, "*-*")}
        for holder in self._census_scan(self.held_dir):
            if holder.is_dir():
                present.update(
                    path.name for path in self._census_glob(holder, "*-*"))
        for name in sorted(present):
            if name in minted:
                continue
            try:
                descriptor = os.open(
                    self.minted_dir / name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o644,
                )
            except FileExistsError:
                adopted.add(name)
                continue
            except OSError:
                continue
            os.close(descriptor)
            adopted.add(name)
        return adopted

    @_guarded_mutation(blocking=True)
    def retire_free_capacity(self, capacity: Mapping[str, int]) -> dict[str, int]:
        """Lower a kind's total to ``capacity`` by deleting FREE tokens only.

        ``ensure_capacity`` is monotonically increasing on purpose -- two
        workers declaring the same box converge, and neither takes back a
        token the other is using.  But a box's honest offer *falls* when work
        arrives that the pool did not schedule, and with only an increasing
        primitive the ledger keeps advertising the high-water mark: a GB10
        offering 96 GB while holding 10 GB free is not admission control, it
        is a promise the box cannot keep.

        Only free tokens are retired, so a running action never loses the
        reservation it is executing under; the total falls as holders finish
        and their tokens are not re-created.  Returns what was retired.
        """

        retired: dict[str, int] = {}
        for kind, count in sorted(capacity.items()):
            target = int(count)
            if target < 0:
                raise PoolContractError(f"capacity for {kind!r} must not be negative")
            free = _glob(self.free_dir, f"{kind}-*")
            held = sum(
                1 for holder in _scan(self.held_dir)
                if holder.is_dir()
                for _ in _glob(holder, f"{kind}-*")
            )
            # Never retire below what is already held: those tokens exist.
            excess = max(0, len(free) + held - target)
            # Retire the HIGHEST-indexed free tokens, not the lowest.
            #
            # ``ensure_capacity`` runs on every claim attempt and fills the
            # slots ``kind-0000 .. kind-{target-1}``, so a retire that ate the
            # low names left a hole the next poll re-minted: a ledger dropped
            # from 96 GB to 40 kept 8 high tokens, and eight of the forty slots
            # below them came straight back.  Measured, not reasoned: gpu 4 ->
            # 1 settled at 2, mem 96 -> 40 settled at 48.  Retiring downward
            # leaves a contiguous prefix, which is exactly the set
            # ``ensure_capacity`` then finds already present.
            #
            # A holder sitting on a high index still causes a partial re-mint,
            # and that is the documented behaviour: the total falls the rest of
            # the way as holders finish and their tokens are not re-created.
            for token in sorted(free, reverse=True)[:excess] if excess else []:
                # The mint right goes FIRST, then the token.  Both orders have
                # a two-syscall window, and they fail in opposite directions.
                # Token first leaves a marker with no token, which
                # ``ensure_capacity`` skips forever: capacity silently and
                # permanently lost, undetectable without a scan that cannot be
                # made safe.  Marker first leaves a token with no marker, which
                # the next poll's adoption re-marks and this retire retires
                # again -- the retire simply did not happen, which is the
                # recoverable direction.  Adoption is why: a token with no
                # marker is adopted, never minted a second time.
                (self.minted_dir / token.name).unlink(missing_ok=True)
                try:
                    token.unlink()
                except OSError:
                    continue
                retired[kind] = retired.get(kind, 0) + 1
        return retired

    @_guarded_mutation(blocking=True)
    def retire_held(self, action_key: str, counts: Mapping[str, int]) -> dict[str, int]:
        """Destroy up to ``counts`` HELD tokens of one holder, atomically.

        The shared-egress decharge (#733): a mover whose bytes stay on the
        stage under a co-owner must not hand its tokens back as writable
        free capacity, and it must not keep them either -- keeping them
        leaks the tier, freeing them mints a phantom.  Destroying them drops
        the holder and the total together, so free never moves.

        The disposition is one atomic rename per token, from its holder
        directory into the ledger's dead namespace (``minted/dead/``):
        either the token is still held (rename not yet done) or it is
        dead (rename done) -- no unlink-then-record gap for a crash to
        split, and no separate record whose creation can fail apart from
        the disposition itself.  The mint marker is never touched, so the
        name can never be re-minted into free while the bytes remain;
        honest regrowth reissues exactly these filed names when backing
        exists (see :meth:`PoolQueue._reclaim_dead_markers`).  A second
        destroy of the same name finds it already dead and counts it
        without moving anything; a name live elsewhere is never taken.
        There is no second ledger: the dead namespace lives in the same
        ledger directory as the markers themselves.  Missing tokens count
        as already gone, so this is safe to call twice; only actual
        dispositions are returned.
        """

        destroyed: dict[str, int] = {}
        holder = self.held_dir / action_key
        if not holder.is_dir():
            return destroyed
        dead_dir: Path | None = None
        for kind, count in sorted(counts.items()):
            want = int(count)
            if want <= 0:
                continue
            taken = 0
            for token in sorted(_glob(holder, f"{kind}-*"), reverse=True):
                if taken >= want:
                    break
                if token.name in (cpu_admission.METADATA, gpu_admission.METADATA):
                    continue
                if dead_dir is None:
                    dead_dir = self.minted_dir / "dead"
                    try:
                        dead_dir.mkdir(parents=True, exist_ok=True)
                    except OSError:
                        dead_dir = None
                if dead_dir is None:
                    # Nowhere to journal the disposition: retain the token
                    # and surface the shortfall through the missing count.
                    continue
                try:
                    os.rename(token, dead_dir / token.name)
                except FileNotFoundError:
                    if (dead_dir / token.name).exists():
                        taken += 1
                    continue
                except OSError:
                    continue
                taken += 1
            if taken:
                destroyed[kind] = taken
        self._drop_empty_holder(holder)
        return destroyed

    def _drop_empty_holder(self, holder: Path) -> None:
        """Remove a holder directory left with no token files, best-effort.

        Mirrors :meth:`_empty_into_free`'s tail: readers like
        :meth:`held_keys` list holder *directories*, so a dir emptied by a
        count-capped settle must go, exactly as a full release removes it.
        Adaptive metadata goes only once no token remains, so accounting
        can never disappear while tokens do.
        """

        try:
            remaining = [path for path in _scan(holder)
                         if path.name not in (cpu_admission.METADATA,
                                              gpu_admission.METADATA)]
        except OSError:
            return
        if remaining:
            return
        for marker in (cpu_admission.METADATA, gpu_admission.METADATA):
            try:
                (holder / marker).unlink(missing_ok=True)
            except OSError:
                return
        try:
            holder.rmdir()
        except OSError:
            pass

    @_guarded_mutation(blocking=True)
    def release_count(self, action_key: str, counts: Mapping[str, int]) -> dict[str, int]:
        """Return up to ``counts`` held tokens per kind to free, counting actuals.

        The count-capped sibling of :meth:`release`: an egress that must
        retain a shortfall (a decharge that failed partway) keeps exactly
        what it names and frees no more.  Missing tokens count as already
        gone; only actual renames are returned.
        """

        released: dict[str, int] = {}
        holder = self.held_dir / action_key
        if not holder.is_dir():
            return released
        self.free_dir.mkdir(parents=True, exist_ok=True)
        for kind, count in sorted(counts.items()):
            want = int(count)
            if want <= 0:
                continue
            taken = 0
            for token in sorted(_glob(holder, f"{kind}-*")):
                if taken >= want:
                    break
                if token.name in (cpu_admission.METADATA, gpu_admission.METADATA):
                    continue
                try:
                    os.rename(token, self.free_dir / token.name)
                except OSError:
                    continue
                taken += 1
            if taken:
                released[kind] = taken
        self._drop_empty_holder(holder)
        return released

    def capacity(self) -> dict[str, int]:
        """Total tokens of each kind, free or held."""

        counts: dict[str, int] = {}
        for path in _glob(self.free_dir, "*-*"):
            counts[path.name.rsplit("-", 1)[0]] = counts.get(path.name.rsplit("-", 1)[0], 0) + 1
        for holder in _scan(self.held_dir):
            if not holder.is_dir():
                continue
            for path in _glob(holder, "*-*"):
                kind = path.name.rsplit("-", 1)[0]
                counts[kind] = counts.get(kind, 0) + 1
        return counts

    def available(self) -> dict[str, int]:
        """Tokens of each kind not currently held."""

        counts: dict[str, int] = {}
        for path in _glob(self.free_dir, "*-*"):
            kind = path.name.rsplit("-", 1)[0]
            counts[kind] = counts.get(kind, 0) + 1
        return counts

    @_guarded_mutation(blocking=False)
    def begin_acquire(
        self, action_key: str, demand: Mapping[str, int], *,
        adaptive: dict | None = None, cpu_tiers: Mapping | None = None,
        adaptive_gpu: dict | None = None,
    ) -> str | None:
        """Take the whole demand into a directory only this claimant owns.

        Admission runs *before* the ready-to-claimed rename, so at the moment
        tokens are taken it is not yet known which contender will own the
        action.  Filing them under ``held/<action_key>`` gave every contender
        for one key the same rollback target: the loser's ``release`` returned
        the winner's tokens, and a third action was then admitted on capacity
        the winner was already executing against.  The reservation therefore
        belongs to the *claimant* until the rename decides, and only then to
        the action.

        The private directory lives under ``held/`` so that every reader which
        counts what is not free -- ``ensure_capacity``'s holder scan,
        ``capacity``, ``held``, ``retire_free_capacity`` -- accounts for tokens
        in flight without knowing this mechanism exists.  Returns the handle to
        commit or abandon, or ``None`` when the demand could not be met in
        full.

        All-or-nothing on the demand: a multi-resource actor that keeps what it
        managed to get while blocked on what it did not is holding resources it
        cannot use.  That holds for *every* ending, not only the insufficient
        one: an exception raised anywhere between the first rename and the last
        metadata write empties the private directory back into ``free/`` before
        it leaves, because a caller that never receives the handle has no way to
        return the tokens itself.

        On a guarded (tier) ledger the take runs under the tier's mint
        lock, non-blocking: a contended guard returns ``None`` exactly
        like a shortage, and the tier claim path reports its existing
        ``tier_reservation_unavailable`` -- never a wait on admission,
        never a new vocabulary.
        """

        self.last_token_shortage = None
        wanted = {k: int(v) for k, v in demand.items() if int(v) > 0}
        handle = (
            f"{ACQUIRING_PREFIX}{int(_now() * 1_000_000)}.{action_key}"
            f".{socket.gethostname()}.{os.getpid()}.{uuid.uuid4().hex[:8]}"
        )
        if not wanted:
            return handle
        destination = self.held_dir / handle
        destination.mkdir(parents=True, exist_ok=True)
        try:
            for kind, need in sorted(wanted.items()):
                if kind == "cpu" and adaptive is not None:
                    need -= int(adaptive.get("preferred_borrow", 0))
                taken = 0
                for token in _glob(self.free_dir, f"{kind}-*"):
                    if taken >= need:
                        break
                    try:
                        os.rename(token, destination / token.name)
                    except (FileNotFoundError, NotADirectoryError):
                        continue      # another worker took it first
                    taken += 1
                if taken < need and not ((kind == "cpu" and adaptive is not None
                                           and adaptive.get("borrowing"))
                                          or (kind == "gpu" and adaptive_gpu is not None
                                              and adaptive_gpu.get("probe"))):
                    # Capture the tokens this acquisition could actually obtain,
                    # before rollback. No extra shared scan, and no claim that a
                    # later reader sees the same free capacity (issue #520).
                    self.last_token_shortage = {
                        "resource": kind, "requested": need, "available": taken,
                    }
                    raise _Insufficient(kind)
            if adaptive_gpu:
                metadata = dict(adaptive_gpu, borrowed_gpu=max(
                    0, wanted.get("gpu", 0) - len(_glob(destination, "gpu-*"))))
                _write_json_atomic(destination / gpu_admission.METADATA, metadata)
            if adaptive is not None and cpu_tiers is not None:
                allocation = self.cpu_allocation(handle, cpu_tiers)
                assigned = set(allocation["preferred"] + allocation["fallback"])
                # Proven idle preferred reservations may be shared before
                # consuming free SMT/efficiency tokens. Unknown donors retain
                # ordinary disjoint physical-token admission.
                for tier in ("preferred", "fallback"):
                    for cpu in adaptive.get("borrowable_cpus", []):
                        if cpu not in cpu_tiers[tier]:
                            continue
                        if len(assigned) >= wanted.get("cpu", 0):
                            break
                        if cpu not in assigned:
                            allocation[tier].append(cpu)
                            assigned.add(cpu)
                if len(assigned) != wanted.get("cpu", 0):
                    raise _Insufficient("cpu")
                for tier in allocation:
                    allocation[tier] = [c for c in cpu_tiers[tier] if c in assigned]
                metadata = dict(adaptive, allocation=allocation,
                                borrowed_cpu=max(0, len(assigned) - len(_glob(destination, "cpu-*"))))
                _write_json_atomic(destination / cpu_admission.METADATA, metadata)
        except _Insufficient:
            self._empty_into_free(destination)
            return None
        except BaseException:
            # All-or-nothing cannot depend on *which* exception ends the
            # attempt.  ``_Insufficient`` is the only ending this function
            # authors, and it was the only ending that returned the tokens;
            # every other one -- ``cpu_allocation`` refusing a token index the
            # configured topology no longer covers, ``_read_json`` on a torn
            # ``.adaptive.json``, an ESTALE or ENOSPC out of ``os.rename`` or
            # either ``_write_json_atomic`` -- left the whole demand under a
            # private holder.  The caller cannot clean that up: the function
            # never returns, so no ``handle`` reaches it and
            # ``abandon_acquire`` has nothing to name.  Only
            # ``sweep_stale_acquisitions`` recovers it, and its grace is
            # ``LEASE_TIMEOUT_S``, so a repeating cause starves the box one
            # five-minute reservation at a time.
            self._empty_into_free(destination)
            raise
        return handle

    @_guarded_mutation(blocking=True)
    def commit_acquire(self, action_key: str, handle: str) -> int:
        """Move a claimant's private tokens under its action.  Count moved.

        Called by the winner of the ready-to-claimed rename, and by nobody
        else.  The move is per token rather than one directory rename: a
        leftover ``held/<action_key>`` from a release that could not remove its
        own directory makes a directory rename fail with ``ENOTEMPTY``, and the
        count this returns has to be exact so the caller can fail closed.  Per
        token loses nothing, because both directories are under ``held/``: at
        no point in the merge is a token countable as free, and at no point can
        a second claimant take one.

        The count is what the caller checks, and it has to be exact, which is
        the other reason the move is per token.  A claimant swept as stale (see
        :meth:`sweep_stale_acquisitions`) and then winning its rename would
        otherwise proceed to run an action with no reservation, which is the
        same over-admission by another road.  One directory rename would be
        atomic but countable only by listing the source *before* it, and a
        sweep landing between the count and the rename would inflate the count
        -- the one direction the caller must not be lied to in.

        A name already present under the destination is left where it is rather
        than renamed over.  There is one token per index, so a collision means
        some earlier incarnation's tokens are filed under this key; replacing
        the file would delete a token with no retire and no marker, and the
        short count instead makes the caller fail closed.

        On a guarded (tier) ledger the move completes under the tier's
        mint lock: a contended commit waits rather than reporting success
        it did not earn, and the exact count still fails the caller
        closed on a swept handle.
        """

        source = self.held_dir / handle
        destination = self.held_dir / action_key
        if not source.is_dir():
            return 0
        moved = 0
        destination.mkdir(parents=True, exist_ok=True)
        for token in _scan(source):
            landing = destination / token.name
            if landing.exists():
                continue
            try:
                os.rename(token, landing)
            except OSError:
                continue
            if token.name == cpu_admission.METADATA:
                moved += int((_read_json(landing) or {}).get("borrowed_cpu", 0))
            elif token.name == gpu_admission.METADATA:
                moved += int((_read_json(landing) or {}).get("borrowed_gpu", 0))
            else:
                moved += 1
        try:
            source.rmdir()
        except OSError:
            pass
        return moved

    @_guarded_mutation(blocking=True)
    def transfer(self, from_key: str, to_key: str) -> int:
        """Move one holder's whole reservation to another key.  Count moved.

        The ledger operation behind adopting a resident range (#598): a later
        consumer's mover takes over bytes that are already on the stage, so the
        tokens standing for those bytes have to change owner **without ever
        being free**.  Release-then-reacquire cannot do that -- between the two
        the ledger reads capacity it does not have, and a third mover admitted
        in that window lands on a full stage, which is the over-admission the
        whole reservation exists to prevent.

        Per token, and between two directories that are both under ``held/``,
        exactly as :meth:`commit_acquire` moves a claimant's private handle
        onto its action key.  That is what makes it safe to interrupt: at no
        instant in the loop is a token countable as free, so :meth:`capacity`,
        :meth:`held` and :meth:`available` read the same number before, during
        and after.  A crash part-way leaves the reservation split across two
        holders -- the sum is unchanged, nothing is lost and nothing is
        over-admitted -- and calling it again finishes the move.

        A name already present under the destination is left where it is, for
        the reason ``commit_acquire`` gives: there is one token per index, so a
        collision means some other incarnation's tokens are filed there, and
        renaming over it would delete a token with no retire and no marker.
        The short count is what the caller fails closed on.

        Refuses to move *to* or *from* a claimant-private acquisition: those
        are named for a claimant rather than an action, and their owner is not
        decided yet.
        """

        if not from_key or not to_key or from_key == to_key:
            return 0
        if _is_acquisition(str(from_key)) or _is_acquisition(str(to_key)):
            raise PoolContractError(
                "a reservation transfer names two action keys, never a "
                "claimant-private acquisition")
        source = self.held_dir / str(from_key)
        destination = self.held_dir / str(to_key)
        if not source.is_dir():
            return 0
        moved = 0
        destination.mkdir(parents=True, exist_ok=True)
        for token in _scan(source):
            landing = destination / token.name
            if landing.exists():
                continue
            try:
                os.rename(token, landing)
            except OSError:
                continue
            # Adaptive CPU/GPU metadata travels with the tokens it describes
            # and is not itself capacity, so it moves and is not counted --
            # the same split ``commit_acquire`` makes.
            if token.name not in (cpu_admission.METADATA, gpu_admission.METADATA):
                moved += 1
        try:
            source.rmdir()
        except OSError:
            pass
        return moved

    @_guarded_mutation(blocking=True)
    def transfer_tokens(self, from_key: str, to_key: str,
                        names: Sequence[str]) -> int:
        """Move an exact token subset between holders, never via free.

        The prepaid-output primitive: fund one sealed batch from the
        producer's already-reserved window without a second reservation.
        Per-token renames under ``held/`` (same no-free-interval rule as
        :meth:`transfer`): at no instant is a token countable as free, so
        capacity/held/available read the same number before, during and
        after.  A crash part-way leaves the named set split across the two
        holders -- sum unchanged, attributable -- and calling again with
        the same set finishes the move.

        Names are validated before the first rename.  Per name:

        * destination-has + source-missing: already moved (idempotent
          retry), counts;
        * source-has + destination-missing: rename, counts on success;
        * both-have: collision (two tokens one name), left in place,
          counts nothing, caller fails closed on the short count;
        * missing-both: unknown provenance, counts nothing and is never
          silently counted as success.

        Refuses claimant-private endpoints.  Never moves unrelated
        kinds/tokens: only the named set is attempted.  Metadata files are
        never valid names.  Returns moved+already count; short vs
        ``len(set(names))`` is unknown-retain for the caller.

        The existing tier mutation guard covers every rename, so concurrent
        capacity census cannot miss a token moving between holders. The
        blocking guard completes the ownership transition; host ledgers
        retain their no-op guard. Concurrent exclusion qualification remains
        part of this candidate's pending integration checks.
        """

        if not from_key or not to_key or from_key == to_key:
            raise PoolContractError(
                "a token-subset transfer names two distinct holders")
        if _is_acquisition(str(from_key)) or _is_acquisition(str(to_key)):
            raise PoolContractError(
                "a reservation transfer names two action keys, never a "
                "claimant-private acquisition")
        if (not isinstance(names, (list, tuple)) or not names
                or len(set(str(name) for name in names)) != len(names)):
            raise PoolContractError(
                "transfer_tokens needs a non-empty list of distinct token names")
        checked: list[str] = []
        for name in names:
            if (not isinstance(name, str) or not name or "/" in name
                    or name in (cpu_admission.METADATA,
                                gpu_admission.METADATA)
                    or "-" not in name):
                raise PoolContractError(
                    f"transfer_tokens refuses token name {name!r}")
            checked.append(name)
        source = self.held_dir / str(from_key)
        destination = self.held_dir / str(to_key)
        destination.mkdir(parents=True, exist_ok=True)
        done = 0
        for name in checked:
            landing = destination / name
            origin = source / name
            if landing.exists():
                if origin.exists():
                    # Collision: two tokens share one name.  Renaming over
                    # would delete capacity with no retire.  Leave both,
                    # count nothing, caller fails closed.
                    continue
                done += 1
                continue
            if not origin.is_file() and not origin.is_dir():
                # Missing from both: unknown, never counted success.
                continue
            if origin.is_dir():
                # Token names are files; a directory here is not capacity.
                continue
            try:
                os.rename(origin, landing)
            except OSError:
                continue
            done += 1
        try:
            source.rmdir()
        except OSError:
            pass
        return done

    @_guarded_mutation(blocking=True)
    def abandon_acquire(self, handle: str) -> int:
        """Return a claimant's own private tokens.  Its tokens, nothing else.

        On a guarded (tier) ledger the return completes under the tier's
        mint lock; a count short of the handle is retained for the stale
        sweep, never reported as freed.
        """

        return self._empty_into_free(self.held_dir / handle)

    def acquire(self, action_key: str, demand: Mapping[str, int]) -> bool:
        """Take every token the demand asks for, or none of them.

        The uncontended spelling of begin-then-commit, for a caller that has
        already decided the action is its own.  ``claim`` does not use it: the
        rename that decides ownership sits between the two halves.

        On a guarded (tier) ledger this is two leaf lock operations --
        a non-blocking begin, then a blocking commit -- which is the
        accepted shape: the begin declines rather than waits, and the
        commit completes under the lock.  The private handle between
        them is recovered by the stale sweep and fail-closed by the
        commit count, as before.
        """

        handle = self.begin_acquire(action_key, demand)
        if handle is None:
            return False
        wanted = sum(int(v) for v in demand.values() if int(v) > 0)
        if self.commit_acquire(action_key, handle) < wanted:
            self.abandon_acquire(handle)
            self.release(action_key)
            return False
        return True

    @_guarded_mutation(blocking=True)
    def sweep_stale_acquisitions(
        self, *, grace_s: float = LEASE_TIMEOUT_S
    ) -> list[str]:
        """Free tokens a claimant took and never committed.

        The window between ``begin_acquire`` and ``commit_acquire`` is one
        claim-intent write and one rename, so a private directory older than
        the grace belongs to a claimant that died inside it.  Nothing else
        recovers those tokens: they are filed under a claimant, not an action
        key, so no claimed record names them and ``reap_stale``'s release by
        key cannot see them.

        **The grace is the lease timeout, not the heartbeat.**  This sweep
        decides that a claimant is dead with no heartbeat behind it, and that
        is the judgement ``reap_stale``'s own docstring records getting wrong
        at heartbeat length: it requeued a live seven-second action within a
        second of its claim (issue #36).  Waiting costs almost nothing here,
        because the private directory is under ``held/`` and is honestly
        counted as consumed the whole time, whereas sweeping a live claimant
        costs it its claim.  A sweep that does fire early is still not an
        over-admission: ``commit_acquire`` counts what it moved and its caller
        puts the item back rather than running it unreserved.

        The stamped host and pid buy back the common case.  When the claimant
        was on this host and its process is gone, there is nothing to wait for
        and the heartbeat interval is enough.  A reused pid reads as alive and
        waits the full grace, which is the safe direction.
        """

        swept: list[str] = []
        now = _now()
        local = socket.gethostname()
        for holder in _scan(self.held_dir):
            if not _is_acquisition(holder.name) or not holder.is_dir():
                continue
            started = _acquisition_clock(holder)
            if started is None:
                continue
            bound = grace_s
            claimant = _acquisition_claimant(holder.name)
            if (claimant is not None and claimant[0] == local
                    and not _process_alive(claimant[1])):
                bound = min(bound, HEARTBEAT_S)
            if now - started <= bound:
                continue
            self._empty_into_free(holder)
            swept.append(holder.name)
        return swept

    @_guarded_mutation(blocking=True)
    def release(self, action_key: str) -> int:
        """Return every token held for this action.  Safe to call twice."""

        return self._empty_into_free(self.held_dir / action_key)

    @_guarded_mutation(blocking=True)
    def release_kinds(self, action_key: str, kinds: Container[str]) -> int:
        """Return only the named kinds, leaving the holder's others held.

        A residency pin is *occupancy*: those bytes are on the device and must
        stay charged to somebody until an egress deletes them.  A fill rate is
        not occupancy -- once a copy ends nothing is reading, so a mover that
        keeps its pool-side bandwidth holds a share of a supply it no longer
        draws, and the next mover is refused against a rate nobody is using
        (#636).  The holder directory is deliberately left in place: what
        stays is exactly what a release-by-key would have taken.

        Safe to call twice, and contained per token for the reason
        ``_empty_into_free`` is: a rename this call could not do is done by
        the next sweep rather than costing the caller its own ending.
        """

        holder = self.held_dir / action_key
        if not holder.is_dir():
            return 0
        self.free_dir.mkdir(parents=True, exist_ok=True)
        released = 0
        for token in _scan(holder):
            if token.name in (cpu_admission.METADATA, gpu_admission.METADATA):
                continue
            if token.name.rsplit("-", 1)[0] not in kinds:
                continue
            try:
                os.rename(token, self.free_dir / token.name)
            except OSError:
                continue
            released += 1
        return released

    @_guarded_mutation(blocking=True)
    def release_except(self, action_key: str, keep_names: Container[str]) -> int:
        """Return every token except the named ones; count returned.

        Funded-claim rollback (liveness lane): unwinding a claim whose fence
        must stay fused releases only the remainder this attempt newly took
        from free, never the fence the funding record still names.  Releasing
        by key would free the fence into a stealer window; keeping everything
        would double-hold on retry.  The keep set is the exact token list the
        claim verified against the funding record under the key's transition
        lock, so names outside it are this attempt's remainder and nothing
        else.  Contained per token like :meth:`release_kinds`: a rename this
        call could not do is done by the next attempt rather than costing the
        caller its unwind.  This is claim rollback, not egress: it never
        deletes bytes and never touches shared-physical decharge, which the
        shared-cache lane owns.
        """

        keep = set(keep_names)
        holder = self.held_dir / action_key
        if not holder.is_dir():
            return 0
        self.free_dir.mkdir(parents=True, exist_ok=True)
        released = 0
        for token in _scan(holder):
            if token.name in (cpu_admission.METADATA, gpu_admission.METADATA):
                continue
            if token.name in keep:
                continue
            try:
                os.rename(token, self.free_dir / token.name)
            except OSError:
                continue
            released += 1
        return released

    def _empty_into_free(self, holder: Path) -> int:
        """Return physical tokens before removing their adaptive metadata.

        A failed metadata unlink must not prevent physical capacity from
        returning. Conversely, a partial token return retains the metadata
        until a retry finishes the reservation, so its accounting cannot
        disappear while tokens remain held.

        Assumes the caller holds the ledger's mutation guard (every
        public caller -- ``abandon_acquire``, ``release``,
        ``sweep_stale_acquisitions`` -- takes it); takes nothing itself.
        """

        if not holder.is_dir():
            return 0
        released = 0
        metadata = []
        incomplete = False
        self.free_dir.mkdir(parents=True, exist_ok=True)
        for token in _scan(holder):
            if token.name in (cpu_admission.METADATA, gpu_admission.METADATA):
                metadata.append(token)
                continue
            try:
                os.rename(token, self.free_dir / token.name)
            except OSError:
                incomplete = True
                continue
            released += 1
        if incomplete:
            return released
        for marker in metadata:
            marker.unlink(missing_ok=True)
        try:
            holder.rmdir()
        except OSError:
            pass
        return released

    def held(self) -> dict[str, int]:
        """Tokens of each kind a running action currently holds on this host.

        This is the pool's own statement of what it is consuming here, and it
        is what ``box_capacity`` subtracts from the box's live load: the set of
        pids under a claimed action is not knowable from a process tree once
        docker or ``setsid`` is involved, but the reservation always is.
        """

        counts: dict[str, int] = {}
        for holder in _scan(self.held_dir):
            if not holder.is_dir():
                continue
            for path in _glob(holder, "*-*"):
                kind = path.name.rsplit("-", 1)[0]
                counts[kind] = counts.get(kind, 0) + 1
        return counts

    def holder_tokens(self, action_key: str) -> dict[str, int]:
        """Tokens of each kind ONE action holds here.

        :meth:`held` is the box's total; this is one holder's share of it, and
        it is what says whether releasing that holder closes a denied item's
        gap.  Counted from the token files rather than from the action's
        declared demand, because adaptive CPU and GPU admission can seat an
        action on fewer physical tokens than it asked for: the demand would
        promise capacity the release does not actually return.
        """

        counts: dict[str, int] = {}
        for path in _glob(self.held_dir / action_key, "*-*"):
            kind = path.name.rsplit("-", 1)[0]
            counts[kind] = counts.get(kind, 0) + 1
        return counts

    def held_keys(self) -> list[str]:
        """Which actions hold tokens here.

        Claimant-private acquisitions are excluded: they are named for the
        claimant, not for an action, and the contender that will own the action
        is not decided until its rename.  A caller asking which actions hold
        capacity would otherwise be handed a name no queue directory has.
        """

        return sorted(
            path.name for path in _scan(self.held_dir)
            if path.is_dir() and not _is_acquisition(path.name)
        )


def _serialized_key(method):
    """Keep one public mutation inside its key's shared transition lock."""
    identity_parameter = list(signature(method).parameters)[1]
    @wraps(method)
    def invoke(self, *args, **kwargs):
        value = args[0] if args else kwargs[identity_parameter]
        key = value.get("action_key") if isinstance(value, Mapping) else value
        with self._transition_locked(str(key)):
            return method(self, *args, **kwargs)
    return invoke


class PoolQueue:
    """A directory on a shared filesystem that two or more boxes pull from."""

    def __init__(self, root: str | Path | None = None) -> None:
        # Read the module attribute at call time, not at definition time, so
        # a caller (or the test guard) that re-points ``DEFAULT_POOL_ROOT``
        # after import gets the root it named rather than the live store.
        self.root = Path(DEFAULT_POOL_ROOT if root is None else root)
        self._cpu_deferrals: dict[tuple[str, str], float] = {}
        self._cross_resource_deferrals: dict[tuple[str, str], float] = {}
        self._claim_denial_bases: dict[str, Path] = {}
        self._admission_busy_logged_at: float | None = None
        # Queue directories are created once per process (see
        # ``ensure_layout``): nothing in the pool ever removes them.
        self._layout_ensured = False
        if not self.root.is_absolute():
            raise PoolContractError("pool root must be absolute")

    # -- layout ---------------------------------------------------------

    def dir(self, state: str) -> Path:
        if state not in _STATES:
            raise PoolContractError(f"unknown queue state: {state!r}")
        return self.root / state

    def item_path(self, state: str, action_key: str) -> Path:
        return self.dir(state) / f"{action_key}.json"

    def lease_path(self, action_key: str) -> Path:
        return self.dir(CLAIMED) / f"{action_key}.lease"

    def action_status_path(self, action_key: str) -> Path:
        """Where the launcher may leave facts its exit status cannot carry.

        The launcher exits 1 for every failed action, so ``detail.returncode``
        on this lane is the *launcher's* status and the action's own was simply
        unavailable here -- ``core`` has written it to this file since the
        status contract was added, and nothing on this lane set the variable
        that names the file.  A killed run's partial profile comes back the
        same way, because the launcher is being killed and cannot print it.

        Beside the lease, with a suffix that is neither ``.json`` nor
        ``.lease``: every reader of ``claimed/`` addresses those two names, so
        a third is invisible to all of them.  It is unlinked as it is read.
        """

        return self.dir(CLAIMED) / f"{action_key}.status"

    def action_progress_path(self, action_key: str) -> Path:
        """Where an action reports advancement while it is still running.

        A file of its own, beside the lease and the status sidecar and equally
        invisible to every reader of ``claimed/``, because it is the one thing
        here written repeatedly *during* execution by a process this loop does
        not own.  Merging it into the status sidecar would put a ticking writer
        into an unlocked read-merge-write shared with the launcher's own ending
        facts, and would ask a heartbeat to read a file whose reader unlinks it.

        The read costs one open of a small file in a directory this loop
        already writes to every heartbeat, at the heartbeat's own cadence.
        """

        return self.dir(CLAIMED) / f"{action_key}.progress"

    @staticmethod
    def _merge_action_status(
        outcome: dict[str, object], path: Path
    ) -> dict[str, object]:
        """Lift the sidecar's fields onto the ending, and remove the file."""

        try:
            body = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            body = None
        with suppress(OSError):
            path.unlink()
        if not isinstance(body, dict):
            return outcome
        for field in ("action_returncode", "action_signal"):
            value = body.get(field)
            if isinstance(value, int) and not isinstance(value, bool):
                outcome[field] = value
        profile = body.get("profile")
        if isinstance(profile, dict) and outcome.get("profile") is None:
            # A stopped run can leave a partial checkpoint; a successful one
            # refreshes it with the final profile. Prefer stdout when parsed,
            # and otherwise retain the evidence from the sidecar.
            outcome["profile"] = profile
        return outcome

    def _entomb_claim(
        self, action_key: str, *, expect: Mapping[str, object] | None = None
    ) -> tuple[Path | None, bool]:
        """Move a claim aside so its own cleanup cannot delete its successor.

        ``finish`` and ``reap_stale`` both used to publish the item's next home
        and only afterwards unlink ``claimed/<key>.json`` and its lease.  A
        worker polling inside that window claims the newly published retry --
        its rename lands on the same claimed filename -- and the old finisher
        then deletes the *new* claim and the *new* lease on its way out.  The
        retry disappears from the live queue, and because no claimed record and
        no lease survive it, a crash of that second worker leaves a reservation
        no reaper can find.

        So the claim moves out of the way first, atomically, to a name no
        reader of ``claimed/`` treats as a claim, and only the tombstone is
        deleted afterwards.  A crash inside the window leaves the tombstone,
        which :meth:`sweep_finish_tombstones` recovers.

        ``expect`` is the claim the caller judged.  A caller reads a claim,
        decides what becomes of it, and only then moves it aside, and the
        claim can conclude and be re-claimed inside that gap -- the reaper's
        gap spans an ``archive_attempt`` write, which is an NFS round trip.
        Without the comparison the mover entombs a *live* claim it never
        judged, deletes that claim's lease, releases its reservation and
        republishes the item, leaving a second worker running an action
        nothing records it holds.  Returns the tombstone and whether the claim
        was the caller's: ``(None, False)`` says ownership could not be
        established, either because the move failed or a different claim is
        there now, and the caller must leave the key alone. With no ``expect`` the
        move is unconditional, which is what a caller holding the only claim
        on the key wants.
        """

        tombstone = self.dir(CLAIMED) / (
            f"{action_key}.{int(_now() * 1_000_000)}.{socket.gethostname()}"
            f".{os.getpid()}.{uuid.uuid4().hex[:8]}{TOMBSTONE_SUFFIX}"
        )
        try:
            os.rename(self.item_path(CLAIMED, action_key), tombstone)
        except OSError as exc:
            # No move means no exclusive ownership of the bytes being
            # concluded. In particular a stale handle or denied rename must
            # not authorize the reaper to release capacity and publish a retry.
            print(f"pool: cannot entomb claim {action_key}: {exc}; "
                  "claim, lease and reservation retained", file=sys.stderr)
            return None, False
        if expect is None:
            return tombstone, True
        entombed = _read_json(tombstone)
        if entombed is not None and not _same_claim(entombed, expect):
            # The rename is what makes this decidable.  Comparing before it
            # tests a name another process can replace between the read and
            # the move; afterwards these bytes are held exclusively and can be
            # put back.  Link rather than rename, so a claim that appeared in
            # the meantime is never replaced; if the link fails,
            # ``sweep_finish_tombstones`` recovers the record and the caller
            # has still touched nothing that was not its own.
            try:
                os.link(tombstone, self.item_path(CLAIMED, action_key))
            except OSError:
                pass
            else:
                tombstone.unlink(missing_ok=True)
            return None, False
        return tombstone, True

    def attempt_generation(self, record: Mapping[str, object]) -> str:
        """Stable directory name for one submission of a content-addressed key."""

        key = str(record.get("action_key") or "")
        published = record.get("published_unix")
        if len(key) != 64 or any(ch not in "0123456789abcdef" for ch in key):
            raise PoolContractError("attempt history requires a full action key")
        if (
            type(published) not in (int, float)
            or not math.isfinite(float(published))
        ):
            raise PoolContractError("attempt history requires published_unix")
        return pb.canonical_sha256(
            {"action_key": key, "published_unix": float(published)}
        )

    def attempt_path(self, record: Mapping[str, object], attempt: int) -> Path:
        """Immutable outcome path for one numbered attempt of one generation."""

        if type(attempt) is not int or attempt < 1:
            raise PoolContractError("attempt number must be a positive integer")
        key = str(record.get("action_key") or "")
        return (
            self.root
            / ATTEMPTS
            / key
            / self.attempt_generation(record)
            / f"{attempt:08d}.json"
        )

    def attempt_log_path(
        self,
        record: Mapping[str, object],
        attempt: int,
        stream: str,
        sha256: str,
    ) -> Path:
        """Immutable stdout/stderr path beside an attempt outcome."""

        if stream not in {"stdout", "stderr"}:
            raise PoolContractError(f"unknown attempt log stream: {stream!r}")
        if (
            not isinstance(sha256, str)
            or len(sha256) != 64
            or any(ch not in "0123456789abcdef" for ch in sha256)
        ):
            raise PoolContractError("attempt log requires a full SHA-256 digest")
        outcome = self.attempt_path(record, attempt)
        return outcome.parent / f"{attempt:08d}.{stream}.{sha256}.log"

    def ensure_layout(self) -> None:
        """Create the queue directories, once per process.

        Fourteen ``mkdir`` calls: one per directory in ``_STATES`` plus one
        per auxiliary root.  On a starved NFS mount each costs RPCs (a ``MKDIR``
        plus a stat on ``FileExistsError``), so running this on every claim
        poll billed every worker a directory walk per poll for directories
        nothing ever deletes (#595).  The first call in this process creates
        them; later calls are a flag check.  A directory an operator deletes
        by hand returns on the next loop restart, which is the same window
        any other queue repair already needs.
        """

        if self._layout_ensured:
            return
        for state in _STATES:
            self.dir(state).mkdir(parents=True, exist_ok=True)
        (self.root / WORKERS).mkdir(parents=True, exist_ok=True)
        (self.root / ATTEMPTS).mkdir(parents=True, exist_ok=True)
        (self.root / PREWARM).mkdir(parents=True, exist_ok=True)
        (self.root / MOVERS).mkdir(parents=True, exist_ok=True)
        (self.root / RESIDENCY).mkdir(parents=True, exist_ok=True)
        (self.root / RESIDENCY_PLANS).mkdir(parents=True, exist_ok=True)
        (self.root / TIER_RESERVATIONS).mkdir(parents=True, exist_ok=True)
        (self.root / TIERS).mkdir(parents=True, exist_ok=True)
        self._layout_ensured = True

    # -- what the fleet can actually run ---------------------------------

    def announce(
        self,
        *,
        host: str,
        tags: Sequence[str],
        has_gpu: bool,
        capacity: Mapping[str, int] | None = None,
        cpu_tiers: Mapping[str, Sequence[int]] | None = None,
        runtime_commit: str = "",
        observed_capacity: Mapping[str, int] | None = None,
        foreign: Mapping[str, int] | None = None,
        observed_detail: Mapping[str, object] | None = None,
        loops: int | None = None,
        timeout_ceiling_s: float | None = None,
        progress_contracts: Sequence[str] | None = None,
        addresses: Sequence[str] | None = None,
        observed_images: Sequence[str] | None = None,
    ) -> None:
        """Record what this worker offers, so a submitter can be told the truth.

        Without this the queue knows what work has been asked for and nothing
        at all about what the fleet can do, so an item whose required tags no
        box offers is indistinguishable from an item whose box is merely busy:
        it sits in ``ready``, reported as pending, while every worker polls
        past it forever.  That is not hypothetical -- a suite submitted with
        ``--tag dl380`` waited ten minutes in front of fifteen idle workers
        that offer ``x86``, and would have waited a day.

        The offer is a *claim about this box, refreshed by this box*, and it
        expires; a stale file is not evidence.  Placement is still decided by
        the matching in ``claim()``, and what a claimant reads from other
        boxes' offers only ever makes it wait a bounded moment for a better
        placement -- the preferred-CPU deferral and the cross-resource
        preference -- so a wrong or missing offer costs a diagnostic or a
        missed preference, never a misplacement.

        ``capacity`` is what this box is *configured* to offer and is the field
        ``placeable`` reads, because the question a submitter asks is "can any
        box ever run this", not "is a box free this second".
        ``observed_capacity`` is what the box could honestly take right now,
        with work the pool did not schedule subtracted (see
        ``prismabuild.box_capacity``), and ``foreign`` and ``observed_detail``
        say by how much and on what evidence.  The live figure is what the
        ledger is retired to; it is deliberately *not* what ``placeable``
        reads, because a box busy with someone else's work is a slow
        submission, and answering ``False`` there would turn it into a refused
        one.  ``observed_capacity`` is the *windowed* offer, so in the falling
        direction it can lag ``foreign`` by up to ``--observe-samples`` polls:
        a record reading ``observed_capacity {'gpu': 1}`` beside ``foreign
        {'gpu': 2}`` is a box that has seen the foreign work and has not yet
        agreed with itself about it.  The lag is deliberate while a loop is
        polling, and was a hole at the end of an action, where a window full of
        pre-action readings re-minted retired tokens; ``CapacityObserver.
        rejoin`` empties the window there, so a record written on the first
        poll after an action carries no reading older than that poll.  Older
        workers announce none of the three, and a reader must treat their
        absence as "not measured" rather than as zero.

        ``loops`` is how many PrismaBuild worker loops are running on the box
        that wrote this record, and it exists because the record itself cannot
        otherwise say: every loop on a box announces into the same
        ``workers/<host>.json``, so the file states *that the box is offering*
        and never *how many loops are*.  That number was load-bearing in the
        2026-09-06 diagnosis and had to be recovered by a hand process census
        on each box, because no series carried it -- twelve loops on one box
        against three on another means nothing as a bare number until you can
        see they were spawned in tranches with none exiting, which is a
        supervisor ratchet.

        It counts loops on the **box**, not loops announcing under this host
        name, because that is the only one of the two a loop can honestly
        measure: ``worker_loop.py`` reads ``socket.gethostname()`` once before
        its poll loop and holds that name for life, so after a rename the box
        runs loops announcing under two names and no loop can tell which of its
        siblings uses which.  The arithmetic that falls out of counting the box
        is the useful one: two host records whose ``loops`` agree, and whose
        sum exceeds either, are one physical box read as two live nodes (#244).

        ``None`` means the census was not taken or could not be read, and is
        written as an absent field for the same reason as the three above: a
        reader must not turn "not measured" into zero, and every loop published
        before this field existed announces without it.
        """

        record = {
            "schema": POOL_OFFER_SCHEMA_V1,
            "host": host,
            "tags": sorted({str(t) for t in tags}),
            "has_gpu": bool(has_gpu),
            "capacity": {str(k): int(v) for k, v in (capacity or {}).items()},
            # What the box can honestly take right now, and the readings
            # behind it.  Additive, optional fields on the same schema: no
            # reader validates the offer record against a field list, and a
            # bump would only invalidate every offer a running loop had
            # already written.
            "observed_capacity": {
                str(k): int(v) for k, v in (observed_capacity or {}).items()},
            "foreign": {str(k): int(v) for k, v in (foreign or {}).items()},
            "observed_detail": dict(observed_detail or {}),
            # Which published bytes are answering for this box.  A loop holds
            # the module it imported at start for its whole life, so without
            # this a fleet running four generations of the code at once looks
            # uniform from the queue.
            "cpu_tiers": dict(cpu_tiers or {}),
            "runtime_commit": str(runtime_commit),
            "announced_unix": _now(),
        }
        if loops is not None:
            record["loops"] = int(loops)
        if timeout_ceiling_s is not None:
            record["timeout_ceiling_s"] = float(timeout_ceiling_s)
        if progress_contracts is not None:
            # Which progress record schemas this loop's code can accept.  A
            # loop published before the contract existed announces nothing,
            # and a submitter must read that silence as "this box will apply
            # its ceiling to your total duration", not as support (#480).
            record["progress_contracts"] = sorted({str(c) for c in progress_contracts})
        if addresses is not None:
            # The IPv4 addresses this box's kernel holds, so the storage host
            # can tell the NFS bytes it serves *to the box running an action
            # it is warming for* from the bytes it serves to anyone else
            # (#580).  Its ``export_stats`` counts per client address; the
            # claim names a host; this is the only place the fleet joins the
            # two.  Absent, not empty, when the box could not read them: a
            # storage role that finds no addresses protects every client, as
            # it did before the field existed.
            record["addresses"] = sorted({str(a) for a in addresses})
        if observed_images is not None:
            # The exact local references this box positively holds, as the
            # inventory reports them: bare image IDs and repository-qualified
            # RepoDigests.  Present-but-empty says the box looked and has
            # none; absent says the box could not look, and an item that
            # declares images must read that absence as unknown (#714).
            record["container_images"] = sorted(
                {str(entry) for entry in observed_images})
        directory = self.root / WORKERS
        directory.mkdir(parents=True, exist_ok=True)
        _write_json_atomic(directory / f"{host}.json", record)

    def _offer_records(self) -> list[dict[str, object]]:
        directory = self.root / WORKERS
        if not directory.is_dir():
            return []
        records: list[dict[str, object]] = []
        for path in sorted(directory.glob("*.json")):
            # An offer that went away while we were reading the list is a box
            # that left, which is the same answer as an offer that expired:
            # not currently placeable.  It is never a reason to refuse the
            # submission -- the box it described was at worst one candidate
            # among several.  ``tolerate_stale`` is what makes that true on
            # the shared filesystem the pool actually lives on (#208).
            record = _read_json(path, tolerate_stale=True)
            if record is None:
                # ...but one failed read does not show that the box left.
                # Every loop on a live box rewrites its offer each poll with
                # ``_write_json_atomic``, whose ``os.replace`` never removes the
                # name.  A reader that opened the old file can still get
                # ``ESTALE`` once the server frees it, so one read can miss a
                # live host.  A pbcampaign then refused "no recorded worker can
                # run this action" for a tag only that host offered (#560).  A
                # read-only probe of the live ``workers/`` directory saw it in
                # 2 of 589 scans, and the immediate re-read returned the offer
                # both times.  A second read opens the name again, so it gets
                # the replacement.  An offer that is really gone fails both
                # reads and is still left out.
                record = _read_json(path, tolerate_stale=True)
            if record is not None:
                records.append(record)
        return records

    def offers(self, *, max_age_s: float = OFFER_TIMEOUT_S) -> list[dict[str, object]]:
        """Every worker offer still fresh enough to believe, allowing bounded skew."""

        records = self._offer_records()
        # Directory enumeration or any later read can stall on the shared
        # filesystem. All offers must still be fresh after the complete scan;
        # checking against its start would extend their lifetimes by the stall.
        now = _now()
        live: list[dict[str, object]] = []
        for record in records:
            age = offer_timing(record.get("announced_unix"), now=now).age_s
            if age is not None and age <= max_age_s:
                live.append(record)
        return live

    def offer_clock_skews(self) -> dict[str, float]:
        """Future-dated offers, including those beyond the discovery tolerance."""
        records = self._offer_records()
        now = _now()
        skews = {}
        for record in records:
            skew = offer_timing(record.get("announced_unix"), now=now).clock_skew_s
            if skew is not None and skew > 0:
                skews[str(record.get("host") or "?")] = skew
        return skews

    def _matching_offers(
        self, item: Mapping[str, object], *, live: Sequence[Mapping[str, object]]
    ) -> list[Mapping[str, object]]:
        """The offers among ``live`` that could run ``item``.

        One matcher, several readers: "can this run at all", "on how many
        boxes", and "which boxes" are the same question asked three ways, and
        a second copy of the rule would be a way for the answers to disagree.
        """

        required = item.get("tags") or []
        if not isinstance(required, list):
            raise PoolContractError("pool item tags must be a list")
        wanted = {str(t) for t in required}
        declared_images = item.get("container_images")
        if declared_images is not None and (
                not isinstance(declared_images, list)
                or not all(isinstance(entry, str) for entry in declared_images)):
            raise PoolContractError(
                "pool item container_images must be a list of strings")
        demand = self.demand_of(item)
        needs_gpu = bool(item.get("needs_gpu")) or demand.get("gpu", 0) > 0
        matches: list[Mapping[str, object]] = []
        for offer in live:
            tags = {str(t) for t in (offer.get("tags") or [])}
            if not wanted.issubset(tags):
                continue
            if needs_gpu and not offer.get("has_gpu"):
                continue
            if declared_images:
                # Presence is positive evidence only.  An offer that did not
                # report an inventory -- a loop from before the field, a box
                # whose Docker could not be read -- is unknown, and unknown
                # must not count as a capable box (#714).
                present = offer.get("container_images")
                if not isinstance(present, list):
                    continue
                seen = {str(entry) for entry in present}
                if any(image not in seen for image in declared_images):
                    continue
            capacity = offer.get("capacity") or {}
            # A kind the offer does not MENTION is unknown, not zero.  The
            # difference is what a publish looks like from the queue: capacity
            # gains a kind (``cpu``, on 2026-09-04), the offer file is one
            # last-writer-wins record per host, and loops of both generations
            # write it -- so sparky's offer alternated between
            # ``{"gpu": 2, "mem_gb": 48}`` and ``{"cpu": 10, "gpu": 2,
            # "mem_gb": 48}``, 32 and 28 samples of 60 taken one second apart.
            # Read as zero, the older record makes every action carrying the
            # new ``cpu=1`` default unplaceable on a box that plainly runs it:
            # 17 of 60 identical queries answered "no live worker can run this
            # action" for a box whose offer was one to eight seconds old.
            # Refuse on what a box says it cannot fit; never on what it did
            # not say.
            if isinstance(capacity, Mapping) and any(
                int(capacity[kind]) < need
                for kind, need in demand.items() if kind in capacity
            ):
                continue          # this box can never fit it, however idle
            matches.append(offer)
        return matches

    def placeable(
        self, item: Mapping[str, object], *, max_age_s: float = OFFER_TIMEOUT_S
    ) -> bool | None:
        """Can any live worker run this item?  ``None`` means nobody has said.

        Answered from the *declared* capacity, never the observed one.  The
        question is capability -- "this box can never fit it, however idle" --
        and a box temporarily occupied by work the pool did not schedule is
        idle-in-the-future, not incapable.  Reading the live figure here would
        make a busy fleet refuse the submission outright (``pbrun`` raises on
        ``False``) instead of queueing it.

        The three-valued answer is deliberate.  ``False`` is a fact worth
        refusing a submission over; but an empty registry means only that no
        worker has announced yet -- a fleet running loops that predate this
        code, or a queue whose workers are down -- and refusing on *that*
        would turn a missing diagnostic into a broken submit path.  Unknown
        stays unknown.
        """

        live = self.offers(max_age_s=max_age_s)
        if not live:
            return None
        return bool(self._matching_offers(item, live=live))

    def placeable_hosts(
        self, item: Mapping[str, object], *, max_age_s: float = OFFER_TIMEOUT_S
    ) -> list[str] | None:
        """Which live boxes could run this item.  ``None`` means nobody has said.

        The width of an item -- how many boxes it can land on -- is the number
        this fleet had no way to ask for.  A queue that reports only "pending"
        makes an item pinned to one busy box look exactly like an item waiting
        its turn among three, and on 2026-09-03/04 that difference was the
        whole problem: 131 of 391 items carried a hostname tag -- 129 of
        those a consequence of a box-local path, 114 of them pinning
        ``sparky`` from a ``/home/rob/tmp/ts*`` worktree -- while other boxes
        idled.

        A nameless offer is reported as ``"?"`` rather than dropped: it still
        matched, so dropping it would make ``placeable_hosts`` disagree with
        ``placeable`` about whether anything can run the item at all.
        """

        live = self.offers(max_age_s=max_age_s)
        if not live:
            return None
        return sorted({
            str(offer.get("host") or "?")
            for offer in self._matching_offers(item, live=live)
        })

    def placement_timeout_ceilings(
        self, item: Mapping[str, object], *, max_age_s: float = OFFER_TIMEOUT_S
    ) -> dict[str, float | None]:
        """The execution ceiling each box that could run this item announces.

        ``None`` for a box whose offer predates the field, which a reader must
        treat as "not measured" and never as unbounded -- the loops that
        starved #275 for six hours announced nothing, and reading their
        silence as "no limit" would reproduce the same false confidence one
        layer up.  An empty dict means nobody eligible has announced at all.

        Separate from ``placeable_hosts`` because the questions differ: that
        one asks whether any box CAN run the item, this one asks how long the
        boxes that can would let it.  A submitter needs both, and needs to be
        able to tell "no box will grant this" from "no box said".
        """

        live = self.offers(max_age_s=max_age_s)
        if not live:
            return {}
        ceilings: dict[str, float | None] = {}
        for offer in self._matching_offers(item, live=live):
            host = str(offer.get("host") or "?")
            announced = offer.get("timeout_ceiling_s")
            ceilings[host] = (
                float(announced)
                if isinstance(announced, (int, float)) and not isinstance(announced, bool)
                else None
            )
        return ceilings

    def placement_progress_contracts(
        self, item: Mapping[str, object], *, max_age_s: float = OFFER_TIMEOUT_S
    ) -> dict[str, list[str] | None]:
        """Which progress contracts each box that could run this item accepts.

        ``None`` for a box whose offer predates the field, read as "did not
        say" and never as support, for the reason
        ``placement_timeout_ceilings`` gives about ceilings: a box that does
        not understand the policy applies its ceiling to the whole run, which
        is the exact silent kill this contract exists to end.
        """

        live = self.offers(max_age_s=max_age_s)
        if not live:
            return {}
        contracts: dict[str, list[str] | None] = {}
        for offer in self._matching_offers(item, live=live):
            host = str(offer.get("host") or "?")
            announced = offer.get("progress_contracts")
            contracts[host] = (
                sorted(str(entry) for entry in announced)
                if isinstance(announced, list) else None
            )
        return contracts

    def placement_census(
        self, *, max_age_s: float = OFFER_TIMEOUT_S
    ) -> dict[str, object]:
        """How wide the waiting queue is, bucketed by how many boxes fit each item.

        "Placeable on exactly one box" is the number worth watching: it is the
        fleet's depth-vs-width, and it was previously obtainable only by
        reading the queue by hand.  ``pinned_to`` names the boxes those items
        are waiting on, because *which* box is queueing is what tells a
        person whether the pin is the reason the fleet looks busy.

        Width is the number of boxes that can RUN an item, which is not the
        number whose tags match it.  A ``checkout_root`` outside
        ``/mnt/shared`` exists on exactly one box, so it caps the width at one
        however many boxes the tags allow -- and ``one_box_by_path`` counts
        the items where the path is what did the capping, because that is the
        number commit-addressed checkouts are meant to drive down and the
        only one that says the migration is working rather than that a box
        went away.

        ``known`` is false when no worker has announced.  The buckets are then
        zero and mean nothing -- the same unknown-stays-unknown rule
        ``placeable`` follows, kept as a field rather than as three ``None``s
        so a printer can read one flag.  ``unreadable`` counts ready records
        this cannot price at all; see the handler below for why it is a count
        and not an exception.
        """

        live = self.offers(max_age_s=max_age_s)
        ready = self.ready_items()
        census: dict[str, object] = {
            "ready": len(ready),
            "offers": len(live),
            "known": bool(live),
            "unplaceable": 0,
            "one_box": 0,
            "one_box_by_path": 0,
            "wide": 0,
            "unreadable": 0,
            "pinned_to": {},
        }
        if not live:
            return census
        pinned: dict[str, int] = {}
        for item in ready:
            # A census is a diagnostic, and a diagnostic must never be the
            # thing that fails.  ``claim`` skips an item tagged for another
            # box at ``_placement_matches``, before ``demand_of`` is reached,
            # so a record with a non-Mapping ``resources`` was harmless to
            # every existing reader; counting it here made one out-of-band
            # write able to raise on every box in the fleet.  Count it and
            # move on -- and report the count, because an item nothing can
            # read is a fact about the queue, not a rounding error.
            try:
                hosts = sorted({
                    str(offer.get("host") or "?")
                    for offer in self._matching_offers(item, live=live)
                })
            except (PoolContractError, ValueError, TypeError):
                census["unreadable"] = int(census["unreadable"]) + 1
                continue
            # Tags say which boxes are ALLOWED to claim it; the checkout says
            # which box can actually run it.  A box-local ``checkout_root``
            # exists on exactly one box, so it caps the width at one however
            # many boxes the tags match -- and without this cap the metric
            # under-reported the very pin it exists to report: an action
            # tagged ``gb10`` over a ``/home/rob/tmp/ts101`` worktree matches
            # two boxes and can run on one, and counted as ``wide``.
            by_path = is_box_local_path(item.get("checkout_root"))
            if not hosts:
                census["unplaceable"] = int(census["unplaceable"]) + 1
                continue
            if not (by_path or len(hosts) == 1):
                census["wide"] = int(census["wide"]) + 1
                continue
            census["one_box"] = int(census["one_box"]) + 1
            if by_path:
                census["one_box_by_path"] = int(census["one_box_by_path"]) + 1
            # Which box: the tags when they answer alone, otherwise the box
            # that published it, which is the box whose tree it is.  When
            # neither answers -- a box-local item whose publisher is not
            # among the boxes its tags match -- the item is still one box
            # wide and simply goes unattributed, so ``pinned_to`` may sum to
            # less than ``one_box``.  A name we cannot prove is worse than a
            # missing one.
            holder = hosts[0] if len(hosts) == 1 else None
            if holder is None:
                published_by = str(item.get("published_by") or "")
                holder = published_by if published_by in hosts else None
            if holder is not None:
                pinned[holder] = pinned.get(holder, 0) + 1
        census["pinned_to"] = dict(sorted(pinned.items()))
        return census

    def offered_tags(self, *, max_age_s: float = OFFER_TIMEOUT_S) -> list[str]:
        """Every tag some live worker offers -- what to print when nothing fits."""

        seen: set[str] = set()
        for offer in self.offers(max_age_s=max_age_s):
            seen.update(str(t) for t in (offer.get("tags") or []))
        return sorted(seen)

    # -- producer -------------------------------------------------------

    #: The fence marker ``fleet/slurm/cutover.sh`` writes in the queue root
    #: while it retires the pull queue's execution plane.  Same spelling as
    #: that script's ``FENCE_MARKER`` and ``rollback.sh``'s, which cannot
    #: import this module.
    FENCE_NAME = "cutover-fence.json"

    def fence(self) -> dict[str, object] | None:
        """What fenced this queue, or ``None`` when nothing has.

        The marker is the explanation and never the mechanism: the mechanism
        is the write bit on ``ready``, because during a cutover every producer
        and loop on the fleet is still running the published generation, which
        predates the fence and reads no marker.  A refusal that depended on
        this file would protect only the callers that already know about it.
        """

        try:
            data = json.loads(
                (self.root / self.FENCE_NAME).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        return data if isinstance(data, dict) else None

    def _refuse_if_fenced(self) -> None:
        """Turn the fence's EACCES into a refusal that names the cutover.

        Asked before ``ensure_layout``, so a queue whose ``ready`` does not
        exist yet is not mistaken for a fenced one.  A write into a fenced
        directory fails either way -- that is the point of doing it with the
        filesystem -- and what this adds is a producer being told which
        operation refused it and what to do instead, rather than a
        ``PermissionError`` raised from inside a rename.
        """

        ready = self.dir(READY)
        if not ready.is_dir() or os.access(ready, os.W_OK):
            return
        fenced = self.fence()
        if fenced is None:
            raise PoolContractError(
                f"{ready} is not writable, so this submission was refused "
                "rather than left in a queue it could not enter. No cutover "
                "fence marker explains it, so read the directory's mode."
            )
        raise PoolContractError(
            f"the pull queue is fenced: {ready} is not writable. "
            f"{fenced.get('reason') or 'fleet/slurm/cutover.sh fenced it'}. "
            f"Fenced at {fenced.get('fenced_unix')} from "
            f"{fenced.get('fenced_by')}, recorded in "
            f"{self.root / self.FENCE_NAME}. This submission was refused "
            "rather than stranded in a queue whose workers are being retired."
        )

    @_serialized_key
    def publish(
        self,
        *,
        action_key: str,
        cas_root: str | Path,
        worker_script: str | Path,
        checkout_root: str | Path | None = None,
        checkout_snapshot: object | None = None,
        tags: Sequence[str] = (),
        needs_gpu: bool = False,
        priority: int = 0,
        resources: Mapping[str, int] | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        retry_safe: bool | None = None,
        container_owner: str | None = None,
        container_images: Sequence[str] | None = None,
        preempted_claim: Mapping[str, object] | None = None,
        handoff_by: str | None = None,
        residency: Mapping[str, object] | None = None,
        produced_output_template: Mapping[str, object] | None = None,
        produced_output_batch: Mapping[str, object] | None = None,
        recompute: bool = False,
        refuse_withdrawn: bool = False,
        refuse_if_live: bool = False,
    ) -> Path:
        """Enqueue one sealed action.  The action itself already lives in the CAS.

        ``resources`` is what this action needs to run on one box -- e.g.
        ``{"gpu": 1, "mem_gb": 8}``.  It is a claim about the action, made by
        the producer that knows it; a worker's ``capacity`` is the matching
        claim about the box.  Omitting it means the action is admitted on
        placement alone, which is the pre-ledger behaviour.

        ``container_images`` is the action's sealed set of exact local image
        references, copied here so the claim can check the box's inventory
        before it spends an attempt.  Absent means the action declares none,
        and the item is byte-identical to what it was before the field
        existed (#714).

        ``refuse_withdrawn`` is for *automatic* republication: inside this
        method's transition lock a live cancellation marker refuses the
        submission with :class:`WithdrawnActionError` instead of retiring it.
        An explicit submission leaves it False -- re-submitting a key is how
        a person asks for the same work again, and the marker is retired as
        evidence either way.

        ``refuse_if_live`` is the same shape of declaration for a publisher
        that must not duplicate: inside the same lock, a key this queue is
        already carrying in ``ready`` or ``claimed`` refuses with
        :class:`ActionAlreadyLiveError` naming the generation to wait on,
        instead of replacing it.  It is False by default because a fresh
        generation over a live key is a designed operation -- it is how an
        operator asks for the same work again, and ``_claim`` treats the new
        generation as uncovered by the old cancellation on purpose.  The
        publishers that know a second row would be a duplicate say so here:
        ``pbrun``/``pbcampaign``, which attach to the refused generation
        rather than submit a second copy (#812), and the automatic
        republishers in ``tier_loop`` and ``produced_output``, whose own
        look-before-publishing was not, and outside this lock could not be,
        atomic (#810).  A live cancellation still wins: the marker means the
        submission was asked for as a replacement, so the check is skipped and
        the ordinary supersession runs.
        """

        self._refuse_if_fenced()
        if not isinstance(action_key, str) or len(action_key) != 64:
            raise PoolContractError("action_key must be a 64-character digest")
        image_refs: list[str] = []
        if container_images is not None:
            # The queue item's copy of the action's sealed requirement.  It is
            # validated here so a direct producer cannot publish a requirement
            # the claim check would have to treat as malformed.  It is a
            # projection of the sealed params -- the direct API trusts its
            # producer to have derived it, and never re-reads the CAS request
            # to prove that -- never a second authority.
            try:
                image_refs = list(image_inventory.normalize_refs(container_images))
            except ValueError as exc:
                raise PoolContractError(f"container_images: {exc}") from exc
        # Every declaration check is a precondition, ahead of the first side
        # effect below: a publication this method refuses must not have
        # retired a live withdrawal, created a directory or answered an
        # adoption on its way to refusing (#714 review, #708's cancellation
        # contract).
        normalized_tags = normalize_placement_tags(tags)
        if image_refs:
            # The capability the claim check rides must travel with the
            # requirement, never be forgotten by a producer: a box that does
            # not offer it is a loop from before the check, which is exactly
            # the worker #714 must not reach.
            normalized_tags = normalize_placement_tags(
                [*normalized_tags, pb.CONTAINER_IMAGE_TAG])
        elif pb.CONTAINER_IMAGE_TAG in normalized_tags:
            # One spelling of the declaration: the tag is the capability, and
            # an item carrying it without references is a producer that
            # dropped the sealed requirement.  Refused rather than run
            # unchecked.
            raise PoolContractError(
                f"{pb.CONTAINER_IMAGE_TAG} requires container_images; an item "
                "may not require the declared-image capability without "
                "declaring one")
        demand = {str(k): int(v) for k, v in dict(resources or {}).items()}
        if any(v < 0 for v in demand.values()):
            raise PoolContractError("resource demand must not be negative")
        try:
            tier_demand = storage_tiers.split_demand(demand)[1]
            for tier_id in tier_demand:
                self._check_tier_id(tier_id)
        except ValueError as exc:
            raise PoolContractError(str(exc)) from exc
        residency_block = (
            None if residency is None else self.validate_residency(residency, demand))
        produced_ref = None
        validated_produced_template = None
        if produced_output_template is not None:
            try:
                validated_produced_template, produced_ref = (
                    self.validate_produced_output(
                        produced_output_template, demand,
                        residency_block=residency_block))
            except PoolContractError:
                raise
            except ValueError as exc:
                raise PoolContractError(str(exc)) from exc
        produced_batch_ref = None
        # Required immutable admission carrier (R4/R5): bound to the REAL
        # sealed params, never to the kwarg alone. The CAS-filed action
        # request is read through the existing request loader with key
        # validation (`_sealed_produced_output_batch`, same machinery as the
        # sealed progress policy): no request file means legacy direct
        # publish; a sealed reference is derived from it even when the kwarg
        # is omitted; a contradictory kwarg refuses; a VALID filed request
        # with no batch field plus a nonempty kwarg is contradictory (the
        # kwarg cannot invent semantics for an actually sealed request) --
        # only the pre-existing no-request direct-API path accepts a kwarg.
        # An unreadable/invalid sealed request refuses before READY exposure
        # and never silently becomes legacy.
        try:
            sealed_batch_raw, sealed_request_present = (
                _sealed_produced_output_batch(cas_root, action_key))
        except PoolContractError:
            raise
        except (ValueError, OSError) as exc:
            raise PoolContractError(
                f"pool action request unreadable: {exc}") from exc
        if sealed_batch_raw is not None:
            if produced_output_template is not None:
                raise PoolContractError(
                    "produced-output batch and template are mutually exclusive: "
                    "a mover carries a batch reference, an owner a template")
            try:
                produced_batch_ref = self.validate_produced_output_batch(
                    sealed_batch_raw, demand,
                    residency_block=residency_block)
            except PoolContractError:
                raise
            except ValueError as exc:
                raise PoolContractError(str(exc)) from exc
            if produced_output_batch is not None:
                try:
                    kwarg_checked = self.validate_produced_output_batch(
                        produced_output_batch, demand,
                        residency_block=residency_block)
                except PoolContractError:
                    raise
                except ValueError as exc:
                    raise PoolContractError(str(exc)) from exc
                if kwarg_checked != produced_batch_ref:
                    raise PoolContractError(
                        "produced-output batch kwarg contradicts the sealed "
                        "action request: the sealed params govern")
        elif produced_output_batch is not None:
            if sealed_request_present:
                raise PoolContractError(
                    "produced-output batch kwarg contradicts the sealed "
                    "action request: a filed request with no batch field "
                    "cannot gain output semantics from a kwarg")
            if produced_output_template is not None:
                raise PoolContractError(
                    "produced-output batch and template are mutually exclusive: "
                    "a mover carries a batch reference, an owner a template")
            try:
                produced_batch_ref = self.validate_produced_output_batch(
                    produced_output_batch, demand,
                    residency_block=residency_block)
            except PoolContractError:
                raise
            except ValueError as exc:
                raise PoolContractError(str(exc)) from exc
            if tier_demand and residency is None:
                raise PoolContractError(
                    "a produced-output batch mover must carry the residency "
                    "block its reference binds")
        if produced_batch_ref is not None:
            # Publication precondition (R5): an output mover row is exposed
            # only with matching staged intent or explicit committed recovery
            # authority. Reserved intent naming the same batch/manifest is the
            # staged precondition (covers stage->publish republication, which
            # is idempotent while the intent exists); transferring intent plus
            # the durable prewrite, or transferring/consumed intent plus the
            # filed commit, is committed recovery (post-producer republication
            # included). Missing/mismatched/corrupt intent refuses here,
            # before READY exposure; the claim gate remains the hard barrier.
            # 744 holds the mover lock across stage->publish->drive->commit.
            try:
                _prec_rec, _prec_state = self.output_funding_file_state(
                    action_key, str(produced_batch_ref["tier_id"]))
            except (OSError, PoolContractError, ValueError):
                _prec_rec, _prec_state = None, "corrupt"
            if _prec_state == "corrupt":
                raise PoolContractError(
                    "unknown-retain: output funding unreadable for publication")
            if _prec_state == "absent":
                raise PoolContractError("output-funding-missing")
            assert isinstance(_prec_rec, dict)
            if (str(_prec_rec.get("batch_id"))
                    != str(produced_batch_ref["batch_id"])
                    or str(_prec_rec.get("manifest_digest"))
                    != str(produced_batch_ref["manifest_digest"])):
                raise PoolContractError("mover-publication-mismatch")
            _prec_ok = False
            if str(_prec_rec.get("state")) == "reserved":
                _prec_ok = True
            else:
                try:
                    _prec_ok = bool(self._output_precommit_authority(_prec_rec))
                except (OSError, PoolContractError, ValueError):
                    _prec_ok = False
                except Exception:
                    _prec_ok = False
                if not _prec_ok:
                    try:
                        _prec_ok = bool(self._output_batch_authority(_prec_rec))
                    except (OSError, PoolContractError, ValueError):
                        _prec_ok = False
                    except Exception:
                        _prec_ok = False
            if not _prec_ok:
                raise PoolContractError("output-funding-missing")
        if tier_demand and residency is None and produced_ref is None:
            # Derived, never typed (#595): every tier demand the fleet's own
            # submitters seal travels beside the residency block whose
            # manifest range (mover) or leads (consumer) it accounts for --
            # or, since this lane, beside the declared produced-output
            # template whose bounded working window it reserves. Demand on a
            # tier with neither names bytes no manifest maps and no working
            # window, so the pool refuses it rather than reserving capacity
            # nothing can attribute.
            raise PoolContractError(
                "tier demand requires a residency block or a declared "
                "produced-output template: "
                f"{sorted(tier_demand)} names no manifest range, leads, or "
                "working window")
        if type(max_attempts) is not int or max_attempts < 1:
            raise PoolContractError("max_attempts must be a positive integer")
        if retry_safe is not None and type(retry_safe) is not bool:
            raise PoolContractError("retry_safe must be boolean or null")
        if retry_safe is False and max_attempts > 1:
            raise PoolContractError(
                "max_attempts greater than 1 contradicts retry_safe=false"
            )
        if container_owner is not None:
            self.container_marker(str(container_owner))  # validates the digest
        if checkout_snapshot is None:
            if checkout_root is None:
                raise PoolContractError(
                    "publish requires checkout_root or checkout_snapshot"
                )
            addressing: dict[str, object] = {
                "checkout_root": str(checkout_root)
            }
        else:
            if checkout_root is not None:
                raise PoolContractError(
                    "checkout_root and checkout_snapshot are mutually exclusive"
                )
            addressing = {
                "checkout_snapshot": pb.validate_pbrun_checkout_snapshot(
                    checkout_snapshot
                )
            }
        self.ensure_layout()
        if refuse_withdrawn:
            # Under this method's transition lock, so a cancellation can only
            # win outright (the marker is already filed and this refuses) or
            # lose outright (``withdraw`` runs after and cancels the fresh
            # record).  An operator's marker is never retired by an automatic
            # republish (#708 review).
            cancellation = self.live_withdrawal(action_key)
            if cancellation is not None:
                raise WithdrawnActionError(
                    f"{action_key[:12]} carries a live withdrawal"
                    f"{' (' + str(cancellation.get('reason')) + ')' if cancellation.get('reason') else ''}"
                    "; an automatic publication does not supersede one")
        if (refuse_if_live and preempted_claim is None
                and self.live_withdrawal(action_key) is None):
            # This publisher says a second row for a key the queue is already
            # carrying would be a duplicate, not a new generation.  Both
            # halves of the defect it closes are one fact: ``publish``
            # overwrites ``ready/<key>`` whatever state the key is in.
            #
            # Overwrite before the claim (#812): identical submissions seal
            # one content-addressed key, so three ``pbtest`` shards published
            # the same row three times.  Each client read back a different
            # ``published_unix`` and waited pinned to it, the worker ran the
            # surviving row once and filed one terminal, and every client
            # holding a superseded generation polled an empty queue until its
            # wait budget expired.
            #
            # Overwrite after the claim (#810): a caller whose view of the row
            # was stale republished a key that was live in ``claimed``.  With
            # ``recompute`` the duplicate is not answered from the receipt, so
            # the action really ran again -- four times per egress key in the
            # 2026-09-21 Stage A cycle, ordered by timing and refused by
            # nothing.
            #
            # The check is exact rather than advisory: ``claim``, ``finish``,
            # ``withdraw`` and the reapers all take this key's transition
            # lock, which this method already holds, so nothing can move the
            # key between the reads below and the write at the end.  The
            # automatic republishers already look before they publish; what
            # they could not do outside this lock is look atomically.
            #
            # Not the default, and not a rule about the key.  An explicit
            # submission that lands on a live key is publishing a NEW
            # generation on purpose -- ``_requeue_arguments`` depends on it,
            # a resubmission while a stop is in flight depends on it, and
            # ``_claim`` treats the new generation as uncovered by the old
            # cancellation for exactly that reason.  Only a publisher that
            # knows its second row would be a duplicate asks for this.
            #
            # A live cancellation skips the check: the marker is what makes
            # the submission a replacement rather than a duplicate, so it
            # falls through to the supersession below.  (``refuse_withdrawn``
            # ran first, so an automatic republisher never reaches here with
            # one.)
            #
            # Live means a row this queue can read.  A name that is present
            # but unreadable -- a truncated write, a tombstone or late-finish
            # sidecar -- is not a generation anybody is waiting on, and
            # republishing is how such a key is repaired, so it stays
            # publishable.  ``_read_json_fresh`` is used because a stale
            # negative lookup on NFS is what produced #810's republications in
            # the first place.
            for state in (READY, CLAIMED):
                live = _read_json_fresh(self.item_path(state, action_key))
                if live is None:
                    continue
                stamped = live.get("published_unix")
                generation = (
                    float(stamped)
                    if isinstance(stamped, (int, float))
                    and not isinstance(stamped, bool)
                    else None
                )
                raise ActionAlreadyLiveError(
                    action_key, state=state, generation=generation)
        # A submission is what retires a withdrawal.  The key is a content
        # hash -- ``result_and_stamp_names`` says so: *"the same command at the
        # same commit still fingerprints identically"* -- so re-submitting one
        # is the normal way to ask for the same work again, not an attempt to
        # defeat somebody's cancellation.  Treating the marker as a permanent
        # blacklist on the key meant the re-submitted record was deleted by
        # ``claim``'s guard and ``pbrun`` answered the new run with the old
        # run's ``withdrawn_by``, at exit 143, with nothing filed anywhere; the
        # only remedy was ``rm withdrawn/<key>.json`` on the live queue, which
        # is the hand edit this whole verb exists to remove.  The decision is
        # kept -- moved to ``superseded/``, not deleted -- and the new item
        # carries what it revived.  ``refuse_if_live`` never stands in the way
        # of this: a live marker means the submission was asked for as a
        # replacement, so the duplicate check above skips it.
        if preempted_claim is not None:
            # Only a handoff that can name the cancellation it revives may
            # carry an interrupted attempt into a new generation: the
            # admission path via the withdrawal's ``preempted_by``, or a
            # resign driver via ``handoff_by`` exactly equal to the
            # withdrawal's ``withdrawn_by``. An intervening publication or
            # operator decision wins; never overwrite it with the earlier
            # selection snapshot.
            decision = self.withdrawal_covers(preempted_claim, action_key=action_key)
            handoff = decision.get("preempted_by") if decision is not None else None
            if (handoff is None and handoff_by is not None
                    and decision is not None
                    and decision.get("withdrawn_by") == handoff_by):
                handoff = handoff_by
            visible = _read_json(self.item_path(WITHDRAWN, action_key))
            live = _read_json(self.item_path(CLAIMED, action_key))
            if (decision is None or not handoff
                    or visible is None
                    or visible.get("published_unix") != preempted_claim.get("published_unix")
                    or self.item_path(READY, action_key).exists()
                    or (live is not None and not _same_claim(live, preempted_claim))
                    or not self._preemption_eligible(preempted_claim)):
                raise PoolContractError("preemption handoff changed before requeue")
        if validated_produced_template is not None:
            # File the immutable template BEFORE any queue mutation: a
            # conflicting body for the same id refuses here
            # (foreign/tampered) with no withdrawal retired and no row
            # written. Declaration is first-writer-wins and atomic, so a
            # concurrent conflicting filing loses here rather than after a
            # withdrawal was already retired. All precondition checks above
            # already passed.
            try:
                from . import produced_output as produced_mod

                produced_mod.declare_template(
                    self.root, validated_produced_template)
            except produced_mod.ProducedOutputError as exc:
                raise PoolContractError(
                    f"produced-output template conflict: {exc}") from exc
        superseded = self._supersede_withdrawal(action_key)
        item = {
            "schema": POOL_ITEM_SCHEMA_V1,
            "action_key": action_key,
            "cas_root": str(cas_root),
            "worker_script": str(worker_script),
            "tags": normalized_tags,
            "needs_gpu": bool(needs_gpu),
            "priority": int(priority),
            "resources": demand,
            "attempts": 0,
            "max_attempts": max_attempts,
            "published_unix": _now(),
            "published_by": socket.gethostname(),
            **addressing,
        }
        if residency_block is not None:
            item["residency"] = residency_block
        if produced_ref is not None:
            item["produced_output"] = produced_ref
        if produced_batch_ref is not None:
            item["produced_output_batch"] = produced_batch_ref
        if recompute:
            # A movement node.  Its key is a content hash and its receipt is
            # filed in the CAS like any other, so a republish of the same key
            # -- the window asking for a range again after an egress, or after
            # a copy that landed short -- would be answered with the old
            # receipt as a ``cache_hit`` that moves no bytes, and republished
            # again next cycle, forever (2026-09-18: the GLM run's 11 GiB head
            # mover, 25 replays at 0.39 s each, five entries never staged).
            # The pool cannot tell a copy from a computation by its key; the
            # publisher can, and says so here.  Not claim-scoped: a requeue
            # of a movement node is still a movement node.
            item["recompute"] = True
        if retry_safe is not None:
            item["retry_safe"] = retry_safe
        if container_owner is not None:
            item["container_owner"] = str(container_owner)
        if image_refs:
            item["container_images"] = image_refs
        if superseded is not None:
            item["supersedes_withdrawal"] = {
                "published_unix": superseded.get("published_unix"),
                "withdrawn_unix": superseded.get("withdrawn_unix"),
                "withdrawn_by": superseded.get("withdrawn_by"),
                "withdrawn_host": superseded.get("withdrawn_host"),
                "reason": superseded.get("reason"),
                "preempted_by": superseded.get("preempted_by"),
            }
            if superseded.get("preempted_by") is not None:
                # Derived from the cancellation, never passed in: a submission
                # can only claim to be a preemption's requeue if a withdrawal
                # that said so is the one it is reviving.  Top-level as well as
                # nested because this is the live row ``pbstatus`` shows once
                # the marker is retired, and the visible cost of #364 has to be
                # on it rather than one dereference away.
                item["preempted_by"] = str(superseded["preempted_by"])
            elif (handoff_by is not None
                    and superseded.get("withdrawn_by") == handoff_by):
                # The resign-handoff analogue: derived from the resign
                # withdrawal this successor revives, so a reader can tell a
                # resign requeue from an admission preemption.
                item["resigned_by"] = str(handoff_by)
        if preempted_claim is not None:
            # Reuse the ordinary bounded attempt counter. Earlier interrupted
            # launches belong to the linked immutable withdrawal generations,
            # not to this generation's canonical attempt-outcome directory.
            # The existing missing-prefix field accounts for those launches;
            # their complete cancellation lineage remains in the decisions.
            item["attempts"] = int(preempted_claim.get("attempts", 0)) + 1
            item["attempt_history_missing_before"] = item["attempts"]
        path = self.item_path(READY, action_key)
        _write_json_atomic(path, item)
        return path

    #: Everything that describes the claim that has just ended.  A ready item
    #: is claimed by nobody and holds no tokens, so none of it may survive a
    #: requeue.  ``passes`` belongs to the aging sidecar, not to the item.
    _CLAIM_SCOPED_FIELDS = (
        "claimed_by", "claimed_unix", "claimed_host", "reserved_on", "passes", "gpu_admission",
        "cpu_allocation", "tier_reservations", "tier_funding", "residency_verdict",
        "container_cleanup_pending", "container_cleanup_checked_unix",
        "container_cleanup_attempts", "container_cleanup_first_failed_unix",
        "stop_pending", "resource_scope", "resource_scope_cleanup",
        "finish_pending", "resource_scope_intent",
    )

    def _shape_as_ready_item(
        self, record: dict[str, object], *, action_key: str
    ) -> Path:
        """Turn a concluded claim back into a ready item; say where it goes.

        Three producers write into ``ready``: ``publish`` above, ``finish``'s
        retry branch and ``reap_stale``'s.  The two requeues each spelled the
        shape out for themselves and had drifted apart -- one stamped the
        outcome schema onto a queue item, the other left ``action_key`` to
        whatever the claim happened to carry -- so one directory held records
        a consumer could tell apart by which writer produced them.  Teaching
        the readers to accept both is the fix that drifts again; one writer is
        the fix that cannot.

        The rules are the ones ``publish`` already keeps.  ``schema`` says what
        kind of record this is, and a record in ``ready`` is an item, not an
        outcome.  The filename is the identity, because every consumer
        addresses an item by key.  Nothing claim-scoped survives.

        What does survive is the item's own history: ``attempts`` is what the
        next try is counted against, and ``status``, ``detail`` and
        ``attempt_history`` are how the last one went.
        """

        record["schema"] = POOL_ITEM_SCHEMA_V1
        record["action_key"] = action_key
        record["requeued_unix"] = _now()
        for field in self._CLAIM_SCOPED_FIELDS:
            record.pop(field, None)
        return self.item_path(READY, action_key)

    # -- consumer -------------------------------------------------------

    def _placement_matches(
        self, item: Mapping[str, object], *, tags: frozenset[str], has_gpu: bool
    ) -> bool:
        if item.get("needs_gpu") and not has_gpu:
            return False
        required = item.get("tags") or []
        if not isinstance(required, list):
            raise PoolContractError("pool item tags must be a list")
        return all(str(t) in tags for t in required)

    #: What the ready order is made of, and how each part is read.  One
    #: definition, because two callers ask about it and a second spelling that
    #: drifted is the permanent silent resident this guard exists to prevent:
    #: ``ready_items`` decides whether a record can be listed, and
    #: ``_ready_record_usable`` decides whether the sweep may file it.
    _QUEUE_ORDER_FIELDS: tuple[tuple[str, object], ...] = (
        ("priority", int), ("passes", int), ("published_unix", float))

    @staticmethod
    def _unorderable_queue_field(
        record: Mapping[str, object],
    ) -> tuple[str, object] | None:
        """The first ordering field this record states in a foreign way."""

        for name, parse in PoolQueue._QUEUE_ORDER_FIELDS:
            value = record.get(name, 0)
            try:
                parse(value)                          # type: ignore[operator]
            except (TypeError, ValueError):
                return name, value
        return None

    @staticmethod
    def _queue_order_of(
        record: Mapping[str, object],
    ) -> tuple[int, int, float] | None:
        """This record's place in the ready order, or ``None`` if it has none.

        The same three readings the sort has always used, with the raise
        turned into an answer.  ``publish`` refuses a ``priority`` that is not
        an integer and writes ``published_unix`` itself, so a record that
        states either some other way was written by something that is not this
        queue -- and the failure it used to cause was total: the sort runs
        after the per-record parse guard, on the survivors, so one foreign
        record took down the whole listing for every caller at once, ahead of
        any per-item denial (#612, the site #592 left).
        """

        if PoolQueue._unorderable_queue_field(record) is not None:
            return None
        return (-int(record.get("priority", 0)),
                -int(record.get("passes", 0)),
                float(record.get("published_unix", 0.0)))

    def ready_items(self) -> list[dict[str, object]]:
        ordered: list[tuple[tuple[int, int, float], dict[str, object]]] = []
        ready = self.dir(READY)
        if not ready.is_dir():
            return []
        for path in sorted(ready.glob("*.json")):
            try:
                # An item claimed out from under this listing is one this poll
                # does not offer, which is the answer ``None`` already gives.
                # ``tolerate_stale`` is what keeps that true when the vanishing
                # reaches a cached directory handle as ``ESTALE`` rather than
                # ``ENOENT`` (#212, following #208).  The ``glob`` has already
                # succeeded, so the directory is live and the entry is not.
                record = _read_json(path, tolerate_stale=True)
            except PoolContractError:
                # A record nobody can parse is nobody's work.  Raising it out
                # of here took ``claim`` down on every box at once for one
                # foreign writer's truncated file, and ``reap_stale`` with it
                # -- so the sweep that files the thing was itself among the
                # casualties.  Skip it and keep serving; ``quarantine_orphans``
                # files it, and ``serve_once`` reaches that sweep before its
                # next claim.
                continue
            if record is not None:
                record["passes"] = self.passes(str(record.get("action_key", "")))
                order = self._queue_order_of(record)
                if order is None:
                    # A record the queue cannot place in its own order is a
                    # record no consumer can address, which is the defect
                    # ``quarantine_orphans`` files -- the same route #212 gave
                    # a record nobody can parse, one step later.  Skipping it
                    # here is what keeps the listing, and therefore every
                    # claim scan on every box, serving.  It is not the end of
                    # the story: ``_ready_record_usable`` reads the same three
                    # fields, so the sweep files this one into ``failed/``
                    # under ``orphaned_stub``, where ``pbstatus`` counts it.
                    continue
                ordered.append((order, record))
        # Priority band first, then aging, then oldest.  An item that has been
        # denied admission repeatedly is not merely unlucky -- it is being
        # overtaken -- so within its band its denial count outranks its place
        # in line.  Aging never crosses a band: a negative priority is the
        # producer saying "only when nothing else wants the box", and a hint
        # that expired after three denials was not that (#362).  ``claim``
        # walks this order and a withhold ends the pass, so nothing at a lower
        # priority is even considered until every item above it has been
        # tried.  Within a band a long queue still drains in the order it was
        # filled rather than by digest.  Not a scheduler; a tie-break
        # predictable enough to debug.
        #
        # The key is the one computed above rather than recomputed here, and
        # the sort reads only the key: two records that tie would otherwise
        # send Python on to compare the item dictionaries themselves, which is
        # the same class of raise one layer down.
        ordered.sort(key=lambda pair: pair[0])
        return [record for _order, record in ordered]

    # -- aging ----------------------------------------------------------

    def passes_path(self, action_key: str) -> Path:
        return self.root / PASSES / f"{action_key}.json"

    def passes(self, action_key: str) -> int:
        record = _read_json(self.passes_path(action_key))
        if record is None:
            return 0
        value = record.get("passes", 0)
        return int(value) if isinstance(value, (int, float)) else 0

    # -- prewarm receipts -----------------------------------------------

    def prewarm_path(self, action_key: str) -> Path:
        return self.root / PREWARM / f"{action_key}.json"

    def prewarm(self, action_key: str) -> dict[str, object] | None:
        """What a storage-role loop made resident for this action, if any.

        Absent is the normal answer and never an error: no host declares the
        storage role on most fleets, the loop only reaches the head of the
        queue, and an action can be claimed before the loop has looked at it.
        Every caller treats ``None`` as "nothing was warmed".
        """

        record = _read_json(self.prewarm_path(action_key))
        if not isinstance(record, dict):
            return None
        if record.get("schema") != POOL_PREWARM_SCHEMA_V1:
            return None
        return record

    def record_prewarm(self, action_key: str, record: Mapping[str, object]) -> Path:
        """File one prewarm result.

        This is the *only* thing the prewarm loop writes into the queue, and
        it writes nothing at all anywhere else under the shared mount.  It
        never touches the item, so a claim racing this write is unaffected:
        the worst case is a receipt nobody reads because the claim beat it.
        """

        path = self.prewarm_path(action_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        _write_json_atomic(
            path,
            {**dict(record), "schema": POOL_PREWARM_SCHEMA_V1,
             "action_key": action_key},
        )
        return path

    def resolve_prewarm_reference(
        self, reference: Mapping[str, object]
    ) -> dict[str, object] | None:
        """The receipt a claim-time reference names, verified, or ``None``.

        A claim carries ``{PREWARM_RECEIPT_REF: key, manifest_sha256}`` rather
        than the receipt itself.  The receipt is verified before it is
        trusted: the key must match the reference and the digest must match
        the manifest the claim was admitted against, so a receipt rewritten
        for a same-key successor -- a later publication under the same content
        hash -- is never mistaken for this generation's window.  Anything
        unverifiable resolves to ``None``: the terminal record omits the
        prewarm block rather than citing a receipt it cannot produce.
        """

        key = reference.get(PREWARM_RECEIPT_REF)
        digest = reference.get("manifest_sha256")
        if (PREWARM_RECEIPT_REF not in reference
                or not isinstance(key, str) or not key
                or not isinstance(digest, str) or not digest):
            # Not a reference at all: a legacy claim filed before the
            # reference, carrying the full receipt.  Copied as before so a
            # mixed-generation fleet still files complete terminal records.
            return dict(reference)
        try:
            record = self.prewarm(key)
        except OSError:
            # Unavailable evidence is not evidence of absence: a broken mount
            # must not rewrite what the receipt said, and it must not fail
            # the finish either.  The terminal record simply carries no
            # prewarm block.
            return None
        if record is None:
            return None
        if (record.get("action_key", key) != key
                or record.get("manifest_sha256") != digest):
            return None
        return dict(record)

    def prune_prewarm_receipt(
        self, action_key: str, *, live: bool,
        now: float | None = None,
    ) -> dict[str, object]:
        """Delete one prewarm sidecar the queue no longer needs, or say why not.

        The receipt is a cache, not evidence: the claim-time reference has
        already resolved whatever the terminal record keeps, so a receipt for
        a key nobody queues any more is garbage.  Deletion is fail-closed --
        a receipt is removed only when the queue positively shows the key
        needs nothing more from it:

        * a live key (still in ``ready``, ``claimed`` or ``intent``) always
          keeps its receipt;
        * a key with a terminal outcome (``done``/``failed``) is pruned: the
          finish already resolved the reference into the terminal record;
        * a withdrawn key is pruned once its stage band is released or was
          never staged (``swept``, no stage block, or nothing staged) -- the
          orphan sweep still has to release an unstaged band first;
        * anything else is kept, except a swept receipt older than
          ``PREWARM_RECEIPT_RETENTION_S``, the safety valve for a key that
          vanished by a path no state directory records.

        ``live`` is caller-supplied because the caller already holds the live
        set: re-listing three state directories per receipt would price this
        at one queue scan per file.  A blocked receipt -- one whose manifest
        the sweep could not read -- is never pruned here: that is a leak with
        a receipt, and deleting the receipt would delete the leak report.
        """

        key = str(action_key)
        if live:
            return {"action_key": key, "pruned": False, "reason": "live"}
        path = self.prewarm_path(key)
        try:
            present = path.is_file()
        except OSError:
            return {"action_key": key, "pruned": False,
                    "reason": "receipt unreadable"}
        if not present:
            return {"action_key": key, "pruned": False, "reason": "absent"}
        if self.item_path(DONE, key).exists() or self.item_path(
                FAILED, key).exists():
            try:
                path.unlink()
            except OSError:
                return {"action_key": key, "pruned": False,
                        "reason": "unlink failed"}
            return {"action_key": key, "pruned": True, "reason": "terminal"}
        if self.item_path(WITHDRAWN, key).exists():
            record = self.prewarm(key)
            block = (record.get("stage") if isinstance(record, dict)
                     else None)
            releasable = (
                not isinstance(block, dict)
                or bool(block.get("swept"))
                or int(block.get("staged_through_bytes", 0) or 0) <= 0)
            if not releasable:
                return {"action_key": key, "pruned": False,
                        "reason": "stage band pending"}
            try:
                path.unlink()
            except OSError:
                return {"action_key": key, "pruned": False,
                        "reason": "unlink failed"}
            return {"action_key": key, "pruned": True, "reason": "withdrawn"}
        record = self.prewarm(key)
        block = record.get("stage") if isinstance(record, dict) else None
        if isinstance(block, dict) and block.get("swept"):
            moment = now if now is not None else _now()
            try:
                age = moment - path.stat().st_mtime
            except OSError:
                return {"action_key": key, "pruned": False,
                        "reason": "receipt unreadable"}
            if age >= PREWARM_RECEIPT_RETENTION_S:
                try:
                    path.unlink()
                except OSError:
                    return {"action_key": key, "pruned": False,
                            "reason": "unlink failed"}
                return {"action_key": key, "pruned": True, "reason": "stale"}
        return {"action_key": key, "pruned": False, "reason": "unknown state"}

    def sweep_prewarm_receipts(
        self, live_keys: "set[str] | frozenset[str]",
    ) -> list[dict[str, object]]:
        """Prune every pruneable prewarm sidecar outside ``live_keys``.

        One directory listing, then at most a bounded handful of small reads
        per non-live receipt -- and in the steady state there are no non-live
        receipts at all, because every cycle prunes what the last one left.
        That is what bounds the storage loop's per-cycle sweep (#596): the
        directory it lists holds only live keys' receipts, so the orphan
        sweep behind it reads nothing that has already been swept.
        """

        rows: list[dict[str, object]] = []
        try:
            names = sorted(os.listdir(self.root / PREWARM))
        except OSError:
            return rows
        for name in names:
            if not name.endswith(".json"):
                continue
            key = name[: -len(".json")]
            if key in live_keys:
                continue
            rows.append(self.prune_prewarm_receipt(key, live=False))
        return rows

    #: What :meth:`selection_live` can answer about one selected generation.
    #:
    #: ``live`` means the generation is still queued where the warm can serve
    #: it; ``terminal``/``withdrawn``/``superseded`` mean no further read can
    #: serve it and the warm must stop issuing new ones; ``unknown`` means the
    #: evidence was absent or unreadable, and the warm carries on -- stopping
    #: on unavailable evidence would let a broken mount cancel another
    #: action's scope.
    SELECTION_LIVE = "live"
    SELECTION_TERMINAL = "terminal"
    SELECTION_WITHDRAWN = "withdrawn"
    SELECTION_SUPERSEDED = "superseded"
    SELECTION_UNKNOWN = "unknown"

    def selection_live(
        self, action_key: str, *, published_unix: float | None,
    ) -> str:
        """Is this generation still queued to be served, or definitively gone?

        Generation-scoped by ``published_unix``, the queue's own generation
        rule: a later publication under the same action key is a different
        request, and its presence must read as ``superseded`` rather than
        ``live`` so a warm selected for the old generation stops instead of
        serving the new one on the old window.  Absence everywhere reads as
        ``unknown`` -- a requeue or a reaper may hold the key between two
        atomic renames -- and so does any unreadable record: unavailable
        evidence never cancels a warm.
        """

        key = str(action_key)
        if not isinstance(published_unix, (int, float)):
            return self.SELECTION_UNKNOWN

        def generation_of(record: dict[str, object] | None) -> float | None:
            if not isinstance(record, dict):
                return None
            stamp = record.get("published_unix")
            if isinstance(stamp, (int, float)):
                return float(stamp)
            return None

        for state in (CLAIMED, READY):
            try:
                record = _read_json(self.item_path(state, key))
            except (OSError, PoolContractError):
                return self.SELECTION_UNKNOWN
            if record is None:
                continue
            stamp = generation_of(record)
            if stamp is None:
                return self.SELECTION_UNKNOWN
            if stamp == float(published_unix):
                return self.SELECTION_LIVE
            return self.SELECTION_SUPERSEDED
        for state in (DONE, FAILED):
            try:
                record = _read_json(self.item_path(state, key))
            except (OSError, PoolContractError):
                return self.SELECTION_UNKNOWN
            if record is None:
                continue
            stamp = generation_of(record)
            if stamp is None:
                return self.SELECTION_UNKNOWN
            if stamp == float(published_unix):
                return self.SELECTION_TERMINAL
            return self.SELECTION_SUPERSEDED
        try:
            record = _read_json(self.item_path(WITHDRAWN, key))
        except (OSError, PoolContractError):
            return self.SELECTION_UNKNOWN
        if record is None:
            return self.SELECTION_UNKNOWN
        stamp = generation_of(record)
        if stamp is None:
            return self.SELECTION_UNKNOWN
        if stamp == float(published_unix):
            return self.SELECTION_WITHDRAWN
        return self.SELECTION_SUPERSEDED

    def move_path(self, action_key: str) -> Path:
        return self.root / MOVERS / f"{action_key}.json"

    def move_record(self, action_key: str) -> dict[str, object] | None:
        """What one movement node staged, if it has finished and filed it.

        Every caller is asking whether a mover on another box has filed yet,
        and most of them poll, so the read revalidates before answering no
        (``_read_json_fresh``): a stale "not yet" held a produced-output owner
        for 26 s after a 4 s copy (#808).
        """

        record = _read_json_fresh(self.move_path(action_key))
        if not isinstance(record, dict):
            return None
        if record.get("schema") != POOL_MOVE_SCHEMA_V1:
            return None
        return record

    def movers_claimed_on_tier(self, tier_id: str) -> list[str]:
        """Mover keys holding a claim on this tier at this instant.

        The number a mover's receipt needs to make its own pool-side rate mean
        anything: ``mean_pool_read_mb_s`` is what the *pool* delivered while it
        ran, whoever was reading, so one copy's share of it is only readable
        beside the count of copies that shared it.  A mover row is the one whose
        residency block names a *range*; a consumer's names leads.  Claimed
        only -- a ready mover is not reading yet.
        """

        out: list[str] = []
        for path in _scan(self.dir(CLAIMED)):
            item = _read_json(path)
            residency = item.get("residency") if isinstance(item, dict) else None
            if not isinstance(residency, dict):
                continue
            if "range_start_bytes" not in residency:
                continue
            if str(residency.get("tier_id") or "") != str(tier_id):
                continue
            name = path.name
            out.append(name[:-len(".json")] if name.endswith(".json") else name)
        return sorted(out)

    def move_records(self) -> list[dict[str, object]]:
        """Every filed movement receipt, oldest first by the time it records.

        The history a next submission prices itself from: ``mem_gb`` and
        ``cpu`` off ``peak_rss_bytes`` and ``cpu_seconds``, fill capacity off
        the pool-side rate.  Append-only and small (one JSON per mover), so it
        is read whole rather than indexed.  A record that is unreadable or not
        a move receipt is skipped, never raised: a submission must not fail
        because one older receipt was truncated.
        """

        directory = self.root / MOVERS
        out: list[dict[str, object]] = []
        try:
            paths = sorted(directory.glob("*.json"))
        except OSError:
            return out
        for path in paths:
            try:
                record = _read_json(path, tolerate_stale=True)
            except PoolContractError:
                continue
            if not isinstance(record, dict):
                continue
            if record.get("schema") != POOL_MOVE_SCHEMA_V1:
                continue
            out.append(record)
        out.sort(key=lambda r: float(r.get("unix", 0.0) or 0.0))
        return out

    def record_move(self, action_key: str, record: Mapping[str, object]) -> Path:
        """File one movement result.

        A sidecar, never the item, exactly as ``record_prewarm`` is: the mover
        writes this while its own claim is still live, and the tier loop reads
        it beside the prewarm records to learn what the pool delivers.  What
        makes it worth writing separately from the ``done`` record is that the
        fill measurement must survive the action's conclusion -- a tier prices
        its next mover from receipts, and a receipt inside a terminal record
        would be read through a different path on every reaper generation.

        Under the receipt tier's mint lock when the receipt names one: the
        mint's writable sample and landed snapshot run under the same lock,
        so filing here is totally ordered against them -- a completion that
        files before the sample counts with bytes the sample saw, one that
        files after waits for the next mint (#733 R4).  No payload moves
        under this lock; the copy already ran.  The mint lock is a leaf
        everywhere (never held while taking transition/ownership), and
        same-thread nesting is supported, so filing from inside a mint
        section cannot deadlock.  A receipt naming no tier files exactly as
        before (egress receipts are not move receipts and never count).
        """

        tier_id = record.get("tier_id") if isinstance(record, Mapping) else None
        if isinstance(tier_id, str) and tier_id:
            try:
                lock = self.tier_mint_lock(tier_id)
            except (PoolContractError, OSError):
                lock = None
            if lock is not None:
                with lock:
                    return self._file_move(action_key, record)
        return self._file_move(action_key, record)

    def _file_move(self, action_key: str, record: Mapping[str, object]) -> Path:
        """The atomic receipt write behind :meth:`record_move`."""

        path = self.move_path(action_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        _write_json_atomic(
            path,
            {**dict(record), "schema": POOL_MOVE_SCHEMA_V1,
             "action_key": action_key},
        )
        return path

    # -- residency: the map an action reads, and the plan it was cut from ---

    def residency_fragment_root(self) -> Path:
        """Where movement nodes file their fragments, one directory per consumer."""

        return self.root / RESIDENCY

    def residency_map_path(self, consumer_action_key: str) -> Path:
        """The composed map an action reads its staged inputs through.

        One definition, because two parties depend on this name being the same
        string: the ``tiers`` loop is its single writer, and this launcher puts
        it in the action's environment.  A second spelling would be a consumer
        reading a map nobody writes, which fails by falling back to the pool --
        that is, silently, at full cost.
        """

        return residency_map.map_path(
            self.residency_fragment_root(),
            _residency_action_key(consumer_action_key))

    def residency_plan_path(self, consumer_action_key: str) -> Path:
        """The frozen window plan: every mover this consumer will ever have."""

        return (self.root / RESIDENCY_PLANS
                / f"{_residency_action_key(consumer_action_key)}.json")

    def residency_map_environment(self, item: Mapping[str, object]) -> dict[str, str]:
        """``{RESIDENCY_MAP_ENV: path}`` for an action with a map to read.

        Nothing for everything else, so an ordinary action launches in
        byte-identical surroundings to before #583 existed.  The file has to
        be there: an action told to read a map that does not exist would open
        nothing and fall back to the pool anyway, but it would do it after
        deciding it had been staged, and the difference is invisible in the
        receipt.  Existence here makes the variable mean what it says.
        """

        residency = item.get("residency") if isinstance(item, Mapping) else None
        if not isinstance(residency, Mapping) or not residency.get("leads"):
            return {}
        key = item.get("action_key")
        if not isinstance(key, str):
            return {}
        try:
            path = self.residency_map_path(key)
        except PoolContractError:
            return {}
        try:
            present = path.exists()
        except OSError:
            # Same containment as the gate: a stat that raises says nothing
            # about the file, and naming a map this process could not stat
            # would hand the action a path it may not be able to open either.
            # Unset is the honest answer, and it is the one the action already
            # knows how to act on.
            return {}
        return {pb.RESIDENCY_MAP_ENV: str(path)} if present else {}

    def record_pass(self, action_key: str) -> int:
        """Count one admission denial.

        Kept in a sidecar rather than in the item, because rewriting a ready
        item races the claim that may already have moved it: the writer would
        resurrect a claimed action into ``ready`` and hand it to a second
        worker.  A lost increment under contention costs a little ordering
        fairness; a resurrected item costs correctness.
        """

        now = _now()
        # ``first_unix`` is set once and carried forward: it is the clock the
        # withhold ceiling reads, so it must measure the age of the *block*,
        # not the age of the most recent denial.
        prior = _read_json(self.passes_path(action_key)) or {}
        first = prior.get("first_unix")
        if not isinstance(first, (int, float)):
            first = now
        count = self.passes(action_key) + 1
        _write_json_atomic(
            self.passes_path(action_key),
            {"action_key": action_key, "passes": count,
             "first_unix": float(first), "updated_unix": now},
        )
        return count

    @staticmethod
    def _bounded_denial_value(value: object, depth: int = 0) -> object:
        """Keep host-local denial evidence useful without making it a log."""
        if depth >= MAX_DENIAL_VALUE_DEPTH:
            return "<truncated>"
        if value is None or isinstance(value, (bool, int, float)):
            return value
        if isinstance(value, str):
            return value[:MAX_DENIAL_VALUE_TEXT]
        if isinstance(value, Mapping):
            return {str(key)[:MAX_DENIAL_VALUE_TEXT]: PoolQueue._bounded_denial_value(value[key], depth + 1)
                    for key in list(sorted(value, key=str))[:MAX_DENIAL_VALUE_ITEMS]}
        if isinstance(value, (list, tuple, set)):
            return [PoolQueue._bounded_denial_value(item, depth + 1)
                    for item in list(value)[:MAX_DENIAL_VALUE_ITEMS]]
        return repr(value)[:MAX_DENIAL_VALUE_TEXT]

    def record_denial(
        self, item: Mapping[str, object], reason: str, evidence: Mapping[str, object] | None = None,
    ) -> None:
        """Best-effort, coalesced claim evidence; never admission authority.

        This deliberately has no shared queue write.  A host records its latest
        verdict for an action generation locally; the existing independent
        snapshot publisher copies this bounded file no more than once a second.
        A contended local diagnostics lock drops an observation rather than
        delaying or changing a claim.
        """
        if not isinstance(item.get("published_unix"), (int, float)):
            return
        host = socket.gethostname()
        ledger = self.ledger()
        ledger_name = str(ledger.base)
        base = self._claim_denial_bases.get(ledger_name)
        if base is None:
            try:
                base = cpu_admission.local_state_base(ledger.base)
            except (OSError, ValueError, TypeError):
                return
            self._claim_denial_bases[ledger_name] = base
        lock = base / "claim-denials.lock"
        descriptor = None
        try:
            descriptor = os.open(lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            path = base / CLAIM_DENIALS
            records = cpu_admission.read_json(path).get("records", {})
            if not isinstance(records, Mapping):
                records = {}
            key = str(item.get("action_key", ""))
            generation = repr(float(item["published_unix"]))
            identity = f"{key}:{generation}"
            now = _now()
            records = dict(records)
            records[identity] = {
                "action_key": key, "published_unix": float(item["published_unix"]),
                "host": host, "reason": reason, "evidence": self._bounded_denial_value(evidence or {}),
                "denied_unix": now,
            }
            newest = sorted(records.items(), key=lambda entry: (
                float(entry[1].get("denied_unix", 0))
                if isinstance(entry[1], Mapping) and isinstance(entry[1].get("denied_unix", 0), (int, float))
                else 0.0), reverse=True)
            cpu_admission.write_json(path, {"schema": CLAIM_DENIALS_SCHEMA_V1,
                                            "records": dict(newest[:MAX_CLAIM_DENIALS])})
            # This starts only a local child and coalesces shared copies at 1 Hz.
            cpu_admission.adaptive_snapshot.publish(base, ledger.base / "adaptive")
        except (OSError, ValueError, TypeError):
            return
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def withhold_age(self, action_key: str) -> float:
        """Seconds since this item was first denied admission; 0.0 if never."""

        record = _read_json(self.passes_path(action_key)) or {}
        first = record.get("first_unix")
        if not isinstance(first, (int, float)):
            return 0.0
        return max(0.0, _now() - float(first))

    def _write_claim_intent(self, action_key: str, *, owner: str) -> None:
        _write_json_atomic(
            self.item_path(INTENT, action_key),
            {
                "schema": POOL_CLAIM_INTENT_SCHEMA_V1,
                "action_key": action_key,
                "owner": owner,
                "host": socket.gethostname(),
                "pid": os.getpid(),
                "intent_unix": _now(),
            },
        )

    def _discard_claim_intent(self, action_key: str, *, owner: str) -> None:
        """Remove this claimant's intent marker, and only ever its own."""

        path = self.item_path(INTENT, action_key)
        marker = _read_json(path)
        if isinstance(marker, Mapping) and marker.get("owner") == owner:
            path.unlink(missing_ok=True)

    @_serialized_key
    def write_lease(
        self,
        action_key: str,
        *,
        owner: str,
        child_pid: int | None = None,
        container_owner: str | None = None,
        claim_snapshot: Mapping[str, object] | None = None,
        execution_observation: Mapping[str, object] | None = None,
        progress_observation: Mapping[str, object] | None = None,
    ) -> None:
        """Refresh the claim's heartbeat, and say what is running under it.

        ``pid`` is this *loop's* pid and always has been.  It is not the process
        that runs the action, and signalling it would kill the worker rather
        than the work, so it cannot be what a cancellation aims at.
        ``child_pid`` is the launcher ``execute`` started, which is one lookup
        away from the action's own process group -- see
        ``action_process_groups``.  It is ``None`` in the lease ``claim``
        writes, because at that moment nothing is running yet.
        """

        lease = {
                "schema": POOL_LEASE_SCHEMA_V1,
                "action_key": action_key,
                "owner": owner,
                "host": socket.gethostname(),
                "pid": os.getpid(),
                "child_pid": int(child_pid) if child_pid is not None else None,
                "heartbeat_unix": _now(),
            }
        if container_owner is not None:
            lease["container_owner"] = str(container_owner)
        if execution_observation is not None:
            lease["execution_observation"] = dict(execution_observation)
        if progress_observation is not None:
            # So ``pbstatus`` can say "last advanced 40 s ago, 768 anchors" while
            # the action is still running.  Nobody has to *authorize* anything
            # -- continuation is the loop's decision and stays the loop's -- but
            # an operator who cannot see what governs cannot review it either.
            lease["progress_observation"] = dict(progress_observation)
        claim = _read_json(self.item_path(CLAIMED, action_key))
        if (claim is None or claim.get("claimed_by") != owner
                or (claim_snapshot is not None and not _same_claim(claim, claim_snapshot))):
            raise PoolContractError("claim changed before heartbeat publication")
        # A stale claim read must not erase the successor lease's ownership
        # evidence, including the evidence used by finish's cleanup guard.
        _check_claim_lease_identity(action_key, claim, _read_json(self.lease_path(action_key)))
        for field in ("resource_scope", "resource_scope_intent", "resource_scope_cleanup",
                      "claimed_unix", "published_unix"):
            if field in claim:
                lease[field] = claim[field]
        _write_json_atomic(self.lease_path(action_key), lease)

    def ledger(self, host: str | None = None) -> ResourceLedger:
        return ResourceLedger(self.root / RESERVATIONS, host=host)

    # -- storage tiers (#583) --------------------------------------------

    @staticmethod
    def _check_tier_id(tier_id: str) -> str:
        if (not isinstance(tier_id, str) or not tier_id or tier_id.startswith(".")
                or "/" in tier_id or storage_tiers.TIER_DEMAND_SEPARATOR in tier_id):
            raise PoolContractError(f"invalid tier id {tier_id!r}")
        return tier_id

    def tier_ledger(self, tier_id: str) -> ResourceLedger:
        """The cluster-scoped ledger of one storage tier.

        The same ``ResourceLedger`` a box uses, keyed by tier id instead of
        hostname and rooted under ``TIER_RESERVATIONS`` so that no reader
        of ``reservations/`` mistakes a tier for a box.  A tier id carries
        a ``:`` (``prismabuild-stage:dl380g10``), which no hostname does.

        The ledger carries this tier's mint lock as its mutation guard
        (see ``_guarded_mutation``): every token rename through it --
        claim begin/commit/abandon, release variants, transfer, mint
        grow/shrink, egress decharge, stale-handle sweep, and the grant
        ``acquire`` in :meth:`_reserve_fence_locked` -- serializes against
        the reclaim headroom scan.  Callers need no per-site locking; the
        factory attaches the guard.  Host ledgers from :meth:`ledger`
        carry no guard.  Deployment note: workers without this guard
        (older generations) admit outside the exclusion, so mixed-version
        operation can still overissue -- see the quiescence requirement
        on :meth:`_reclaim_dead_markers`.
        """

        checked = self._check_tier_id(tier_id)

        def _tier_guard(*, blocking: bool = True):
            return self.tier_mint_lock(checked, blocking=blocking)

        return ResourceLedger(self.root / TIER_RESERVATIONS, host=checked,
                              mutation_guard=_tier_guard)

    def tier_mint_lock(self, tier_id: str, *, blocking: bool = True):
        """Serialize the minters of one tier's capacity, never other tiers.

        ``mint_tier_capacity`` runs ``ensure_capacity`` then
        ``retire_free_capacity``; the ``O_EXCL`` mint-marker analysis that
        makes each half safe assumes one minter per ledger, and the host
        ledgers do the same work only inside the box's serialized capacity
        prelude (#593).  A second minter -- ``tier_loop --once`` beside the
        supervised role -- takes this lock rather than interleaving with the
        first, so two minters against one tier are inside the analysis
        instead of outside it.  Per tier, so two boxes' loops still mint
        their own tiers concurrently; the lock file lives beside the queue
        rather than in the ledger so retiring a tier never removes it.
        Non-blocking acquisition yields ``False`` instead of raising, for a
        caller that would rather decline than wait.
        """

        name = hashlib.sha256(
            f"tier-mint:{self._check_tier_id(tier_id)}".encode()).hexdigest()
        return posix_lock.held(self.root / "tier-mint-locks" / f"{name}.lock",
                               blocking=blocking)

    def stage_ownership_lock(self, stage_root, *, blocking: bool = True):
        """Serialize the owners of one stage root's files, never other roots.

        The lock covers the check-and-act pairs that decide a shared staged
        file's fate: an egress's scan-then-unlink-then-drop-fragment-then-
        release, an adoption's reissue-then-transfer-then-drop, and a mover's
        start gate before its first rename.  Per stage root (stage and ram
        roots are different paths) so tiers do not contend; the lock file
        lives beside the queue.  Lock order is transition-then-ownership
        everywhere: ``evict`` already holds the mover transition lock when it
        takes this one, and adoption takes this one only after acquiring the
        transition lock non-blocking (declining instead on contention, which
        costs a copy and never correctness).  Nothing takes them in the other
        order, and a mover's start gate holds nothing else, so no cycle.

        **One root at a time.**  The ladder above orders the three lock
        families; it says nothing about two locks from *this* family, and
        that is where the cycle lives: a caller holding root A that asks for
        root B deadlocks against one holding B that asks for A, and
        ``posix_lock.held`` nests on the same path only, so same-root
        reentrancy does not prevent it.  So no caller may hold one root's
        lock while requesting another's.  ``reader_lease.release_refs`` takes
        the root each pin names, which is why the egress reclaims before
        taking this lock rather than under it (#780).
        """

        root = str(Path(stage_root).absolute())
        name = hashlib.sha256(f"stage-ownership:{root}".encode()).hexdigest()
        return posix_lock.held(self.root / "stage-ownership-locks" / f"{name}.lock",
                               blocking=blocking)

    def ownership_start_gate(self, stage_root) -> None:
        """Order this copy's first rename against an in-progress egress snapshot.

        The claim already exists, so an egress that snapshots after this point
        attributes the copy through the claim; one that snapshotted before
        waits out here until its delete completes.  Acquired and released --
        nothing is held during the copy itself, so movers keep their full
        concurrency and this costs one lock round trip per mover, not per
        entry.  Both movers call it; the egress holds the same lock throughout
        its snapshot-to-release.
        """

        with self.stage_ownership_lock(str(stage_root)):
            pass

    def tier_ids(self) -> list[str]:
        """Every tier that has a ledger, whether or not it holds anything.

        Contained against every ``OSError``, not only the two ``_scan``
        tolerates, and deliberately here rather than in ``_scan``: a host
        ledger's scan must keep failing loudly, because a box that cannot read
        its own reservations does not know what it holds.  This directory is
        different.  It is brand new, it is on the shared mount, nothing touches
        it while the feature is off, and its readers are ``finish`` and the
        reapers -- so a ``PermissionError`` or an NFS ``ESTALE`` on a cold
        handle would abort a conclusion *after* the claim was entombed and its
        lease unlinked but *before* the outcome was written, and would abort a
        whole reaper cycle on every box for as long as it persisted.  An empty
        listing is the safe answer: release is documented safe to call twice,
        so a token this call could not return is returned by the next release
        on the same key or by the reaper's reclaim.
        """

        try:
            return sorted(
                directory.name for directory in _scan(self.root / TIER_RESERVATIONS)
                if directory.is_dir() and not directory.name.startswith(".")
            )
        except OSError:
            return []

    def tier_holdings(self, action_key: str) -> dict[str, dict[str, int]]:
        """The tier tokens one action holds, by tier id; empty when it holds none."""

        holdings: dict[str, dict[str, int]] = {}
        for tier_id in self.tier_ids():
            tokens = self.tier_ledger(tier_id).holder_tokens(action_key)
            if tokens:
                holdings[tier_id] = tokens
        return holdings

    def release_tier_rate_reservations(self, action_key: str) -> int:
        """Return the tier tokens that price a rate, keeping the ones that
        price occupancy.

        The kinds to *release* are enumerated rather than the kinds to keep,
        so a tier resource added later is kept by default: keeping a token too
        long costs admission, releasing an occupancy token early costs the
        accounting for bytes that are still on the device, and the tier loop's
        sweep then reads capacity that is already spent (#636, and the
        argument ``_release_reservation`` already makes).
        """

        released = 0
        for tier_id in self.tier_ids():
            try:
                released += self.tier_ledger(tier_id).release_kinds(
                    action_key, TIER_RATE_KINDS)
            except (OSError, PoolContractError):
                continue
        return released

    def release_tier_reservations(self, action_key: str) -> int:
        """Return every tier token filed under this action, on every tier.

        By key and with no holder to resolve: a tier ledger has exactly one
        directory per tier, so a key can hold on several tiers without any
        of them being a second claim holder.  Safe to call twice.

        Tier by tier, and each one contained: this runs inside ``finish`` and
        the reapers, between the entombed claim and the written outcome, so a
        shared-mount read that fails must cost one tier's tokens until the next
        release rather than the action's own ending.  A directory name that is
        not a tier id (``PoolContractError`` out of ``tier_ledger``) is skipped
        for the same reason -- nothing of ours writes one, and a stray name is
        not a reason to lose an outcome.
        """

        released = 0
        for tier_id in self.tier_ids():
            try:
                released += self.tier_ledger(tier_id).release(action_key)
            except (OSError, PoolContractError):
                continue
        return released

    def transfer_tier_reservation(self, tier_id: str, from_key: str,
                                  to_key: str) -> int:
        """Hand one tier's whole reservation from one mover key to another (#598).

        The bytes do not move; only the name the ledger files them under does.
        ``ResourceLedger.transfer`` never lets a token be free in between, so
        the tier's occupancy is the same number at every instant of the hand-
        over -- which is the invariant the stage reservation rests on, stated
        for a transfer rather than for a release.
        """

        return self.tier_ledger(tier_id).transfer(str(from_key), str(to_key))

    # -- advance-credit funding records (window progress protection) --------

    def funding_path(self, mover_action_key: str, tier_id: str) -> Path:
        """One mover's funding record on one tier (newest generation wins)."""

        return (self.root / TIER_FUNDING
                / f"{mover_action_key}.{tier_id}.funding.json")

    @staticmethod
    def validate_funding(value: object) -> dict[str, object]:
        """Refuse a funding record that is not exactly one binding.

        Unknown fields refuse: a writer meaning more than the reader checks
        is the quiet half of a disagreement about whose bytes are funded.
        Anything here that does not validate authorizes nothing -- callers
        treat an unreadable record as no funding, never as zero or as proof.
        """

        if not isinstance(value, Mapping):
            raise PoolContractError("a tier funding record must be an object")
        unknown = sorted(set(value) - {
            "schema", "tier_id", "consumer_action_key", "plan_sha256",
            "mover_action_key", "range_start_bytes", "range_end_bytes",
            "kind", "tokens", "generation", "state", "unix",
            "published_unix",
        })
        if unknown:
            raise PoolContractError(
                f"unknown tier funding fields: {unknown}")
        if value.get("schema") != TIER_FUNDING_SCHEMA_V1:
            raise PoolContractError(
                f"funding schema must be {TIER_FUNDING_SCHEMA_V1!r}")
        for field in ("tier_id", "kind"):
            if (not isinstance(value.get(field), str) or not value[field]):
                raise PoolContractError(
                    f"funding {field} must be a non-empty string")
        for field in ("consumer_action_key", "mover_action_key"):
            key = value.get(field)
            if (not isinstance(key, str) or len(key) != 64
                    or any(c not in "0123456789abcdef" for c in key)):
                raise PoolContractError(
                    f"funding {field} must be a 64-character action key")
        digest = value.get("plan_sha256")
        if (not isinstance(digest, str) or len(digest) != 64
                or any(c not in "0123456789abcdef" for c in digest)):
            raise PoolContractError(
                "funding plan_sha256 must be a 64-character digest")
        for field in ("range_start_bytes", "range_end_bytes"):
            number = value.get(field)
            if isinstance(number, bool) or not isinstance(number, int):
                raise PoolContractError(
                    f"funding {field} must be a whole number of bytes")
        tokens = value.get("tokens")
        if (not isinstance(tokens, list) or not tokens
                or any(not isinstance(name, str) or not name
                       for name in tokens)):
            raise PoolContractError(
                "funding tokens must be a non-empty list of token names")
        if len(set(tokens)) != len(tokens):
            raise PoolContractError("funding tokens must not repeat a name")
        generation = value.get("generation")
        if (not isinstance(generation, str) or len(generation) != 32
                or any(c not in "0123456789abcdef" for c in generation)):
            raise PoolContractError(
                "funding generation must be a 32-character nonce")
        if value.get("state") not in TIER_FUNDING_STATES:
            raise PoolContractError(
                f"funding state must be one of {sorted(TIER_FUNDING_STATES)}")
        published = value.get("published_unix")
        if (isinstance(published, bool) or not isinstance(published, (int, float))
                or float(published) < 0):
            raise PoolContractError(
                "funding published_unix must be a non-negative timestamp")
        return dict(value)

    def read_funding(self, mover_action_key: str,
                     tier_id: str) -> dict[str, object] | None:
        """A mover's funding record, or ``None`` when absent or unparsable.

        The tolerant spelling for reads that decide nothing: ``None`` covers
        both true absence and unreadable/corrupt/empty records.  A mutation
        that would destroy or replace the record's authority (reserve,
        settle, cancel) must instead use :meth:`read_funding_evidence`,
        which names the difference -- a present but unreadable record is
        unproved authority, never absence, and may not be unlinked, closed,
        or fenced beside.
        """

        status, record, _reason = self.read_funding_evidence(
            mover_action_key, tier_id)
        return record if status == "record" else None

    def read_funding_evidence(self, mover_action_key: str,
                              tier_id: str) -> tuple[str,
                                                     dict[str, object] | None,
                                                     str | None]:
        """``("record", record, None)`` / ``("absent", None, None)`` /
        ``("unknown", None, reason)`` for one funding record.

        The authoritative spelling (see :meth:`read_funding`): I/O errors, a
        present-but-empty file, unparsable bytes, and validation failures
        are all *unknown* -- the record exists and may still bind tokens, so
        an unknown answer retains authority instead of licensing a fresh
        fence or a replace.  Only a path that is genuinely not there reads
        absent.
        """

        path = self.funding_path(mover_action_key, tier_id)
        try:
            raw = _read_json(path)
        except OSError as exc:
            return ("unknown", None, f"funding record unreadable: {exc!r}")
        except PoolContractError as exc:
            return ("unknown", None, f"funding record unparsable: {exc}")
        if raw is None:
            try:
                present = path.exists()
            except OSError as exc:
                return ("unknown", None,
                        f"funding record census unreadable: {exc!r}")
            if present:
                return ("unknown", None,
                        "funding record present but empty")
            return ("absent", None, None)
        try:
            return ("record", self.validate_funding(raw), None)
        except (PoolContractError, ValueError) as exc:
            return ("unknown", None, f"funding record invalid: {exc}")

    def write_funding(self, record: Mapping[str, object], *,
                      expect_generation: str | None = None) -> Path:
        """File one generation's binding, atomically (coordinator calls this).

        The binding (tier, consumer, plan, mover, range, kind, tokens,
        generation) is immutable per generation: a fresh reservation mints a
        fresh generation rather than editing one.  Only ``state`` advances,
        through :meth:`advance_funding_state`.

        Every writer holds the mover's transition lock: this call acquires
        it non-blocking and refuses with ``PoolContractError`` when a claim
        (or another coordinator cycle) holds it, so generation replacement
        never races a verifying claim.  The generation check itself is
        read-check-replace -- a loud refusal on rotation, not an atomic
        compare-and-swap; the lock is what makes concurrent writers safe.
        See :meth:`_write_funding_locked` for the body.
        """

        checked = self.validate_funding(record)
        mover = str(checked["mover_action_key"])
        with self.mover_transition_lock(mover, blocking=False) as acquired:
            if not acquired:
                raise PoolContractError(
                    "funding writer lost the transition race")
            return self._write_funding_locked(
                checked, expect_generation=expect_generation)

    def _write_funding_locked(self, record: Mapping[str, object], *,
                              expect_generation: str | None = None) -> Path:
        """Advance one generation's record; caller holds the mover lock.

        ``expect_generation`` names the record this write replaces (``None``
        when creating).  Enforces, against the filed record: the generation
        still matches (a mismatch refuses instead of overwriting), the
        binding is unchanged (every field but ``state``/``unix`` must equal
        the filed record -- a rewrite changing consumer, plan, range, tokens
        or generation itself is a different binding wearing a live
        generation), and the state step is legal in
        :data:`_FUNDING_TRANSITIONS` (same-state rewrites are idempotent and
        allowed; ``consumed`` -> ``transferring`` and every other backward
        or skipping step refuses, so physical holdings can never be
        reclassified as credit).  Births must be ``reserved``.  Rotation to
        a fresh generation is not this function's job -- see
        :meth:`_rotate_funding_locked`, owned by the reserve path alone.
        """

        checked = self.validate_funding(record)
        path = self.funding_path(str(checked["mover_action_key"]),
                                 str(checked["tier_id"]))
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            raw = _read_json(path)
        except (OSError, PoolContractError):
            raise PoolContractError("funding record unreadable for CAS")
        if raw is None:
            if expect_generation is not None:
                raise PoolContractError(
                    "funding record vanished underneath this write")
            if checked.get("state") != "reserved":
                raise PoolContractError(
                    "funding records are born reserved")
        else:
            try:
                current = self.validate_funding(raw)
            except (PoolContractError, ValueError):
                raise PoolContractError(
                    "funding record unreadable for CAS")
            if (expect_generation is None
                    or str(current.get("generation")) != str(expect_generation)
                    or str(checked.get("generation")) != str(
                        current.get("generation"))):
                raise PoolContractError(
                    "funding generation rotated underneath this write")
            for field in _FUNDING_BINDING_FIELDS:
                if current.get(field) != checked.get(field):
                    raise PoolContractError(
                        f"funding {field} is immutable within one generation")
            if (str(checked.get("state")) != str(current.get("state"))
                    and str(checked.get("state")) not in _FUNDING_TRANSITIONS.get(
                        str(current.get("state")), frozenset())):
                raise PoolContractError(
                    "funding state step "
                    f"{current.get('state')!r}->{checked.get('state')!r} "
                    "is not a legal advance")
        _write_json_atomic(path, checked)
        return path

    def _rotate_funding_locked(self, record: Mapping[str, object], *,
                               expect_generation: str | None) -> Path:
        """Replace one generation with a fresh ``reserved`` one, reserve path.

        Caller holds the mover lock and has already applied the reserve
        path's keep-or-rotate rules (see :meth:`_reserve_fence_locked`): a
        live ``reserved`` binding whose names still match is kept, never
        rotated, and anything handed off or spent is left alone.  This only
        checks the replaced generation is still the one the reserve read,
        the replacement mints a strictly fresh generation, and the fresh
        record is ``reserved`` -- it cannot advance a state, edit a binding,
        or reclassify physical holdings, because those go through
        :meth:`_write_funding_locked`.  Fresh creates (no filed record)
        require ``expect_generation=None``.
        """

        checked = self.validate_funding(record)
        if checked.get("state") != "reserved":
            raise PoolContractError(
                "rotated funding generations are born reserved")
        path = self.funding_path(str(checked["mover_action_key"]),
                                 str(checked["tier_id"]))
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            raw = _read_json(path)
        except (OSError, PoolContractError):
            raise PoolContractError("funding record unreadable for CAS")
        if raw is None:
            if expect_generation is not None:
                raise PoolContractError(
                    "funding record vanished underneath this write")
        else:
            try:
                current = self.validate_funding(raw)
            except (PoolContractError, ValueError):
                raise PoolContractError(
                    "funding record unreadable for CAS")
            if (expect_generation is None
                    or str(current.get("generation")) != str(expect_generation)):
                raise PoolContractError(
                    "funding generation rotated underneath this write")
            if str(checked.get("generation")) == str(current.get("generation")):
                raise PoolContractError(
                    "rotation must mint a fresh generation")
        _write_json_atomic(path, checked)
        return path

    def advance_funding_state(self, mover_action_key: str, tier_id: str, *,
                              expect: str, advance_to: str,
                              generation: str | None = None) -> bool:
        """Move a funding record one legal step, or refuse with ``False``.

        Serialized on the mover's transition lock (non-blocking: whoever
        holds it -- a claim from its tier acquire through its
        consumed-marking, or the coordinator's settle -- decides; the other
        defers).  Same-thread nesting re-acquires safely, so the claim path
        (which holds the lock throughout) may call either spelling.  See
        :meth:`_advance_funding_state_locked` for the body.
        """

        with self.mover_transition_lock(str(mover_action_key),
                                        blocking=False) as acquired:
            if not acquired:
                return False
            return self._advance_funding_state_locked(
                mover_action_key, tier_id, expect=expect,
                advance_to=advance_to, generation=generation)

    def _advance_funding_state_locked(self, mover_action_key: str, tier_id: str, *,
                                      expect: str, advance_to: str,
                                      generation: str | None = None) -> bool:
        """Move a funding record one legal step, or refuse with ``False``.

        Caller holds the mover's transition lock (see
        :meth:`advance_funding_state`).

        Legal steps are :data:`_FUNDING_TRANSITIONS` -- the same table
        :meth:`_write_funding_locked` enforces, so the two can never
        disagree about what a step is.  When ``generation`` is given it must
        match the filed record: the check turns a stale writer into a loud
        ``False``, while the mover lock (held by the caller) is what keeps
        two writers from interleaving the read and the replace.  A fresh
        reservation never edits a live binding: it mints a new generation
        through the reserve path instead.
        """

        current = self.read_funding(mover_action_key, tier_id)
        if current is None or current.get("state") != expect:
            return False
        if (generation is not None
                and str(current.get("generation")) != str(generation)):
            return False
        if expect == advance_to:
            return True
        if advance_to not in _FUNDING_TRANSITIONS.get(str(expect), frozenset()):
            return False
        updated = dict(current)
        updated["state"] = advance_to
        updated["unix"] = time.time()
        try:
            self.write_funding(
                updated,
                expect_generation=(str(current.get("generation"))
                                   if isinstance(current.get("generation"),
                                                str) else None))
        except (OSError, PoolContractError, ValueError):
            return False
        return True

    def funded_cover(self, tier_id: str, item: Mapping[str, object],
                     kind: str, need: int) -> tuple[int, str | None]:
        """What this claim's funding record covers, with its generation.

        Strict or nothing, against the sealed row itself: the record must
        parse, sit in ``transferring``, name this tier/mover/kind, carry the
        row's own ``published_unix`` (a republished content-hash key never
        inherits an older generation's credit), repeat the row's sealed
        residency range, be cut as a leg for this mover over exactly that
        range by the live frozen plan in the role this kind funds, and name
        token files that are all still held under this key right now with
        this kind's prefix.  Consumed records never cover anything: a
        successful mover intentionally keeps its whole token set after
        landing bytes, so full holdings prove physical occupancy, not
        unspent credit.  Stale physical holdings -- a landed range's
        complete receipt, a previous attempt's leftovers, another
        generation's fence, a superseded plan's credit, a mover outside the
        sealed plan -- never match, so an old copy always pays its full
        demand.  Never raises for queue-state reasons; unknown is
        ``(0, None)``.
        """

        if need <= 0 or not isinstance(item, Mapping):
            return (0, None)
        key = item.get("action_key")
        if not isinstance(key, str):
            return (0, None)
        record = self.read_funding(key, tier_id)
        if record is None or record.get("state") != "transferring":
            return (0, None)
        if (str(record.get("tier_id")) != str(tier_id)
                or str(record.get("mover_action_key")) != key
                or str(record.get("kind")) != str(kind)):
            return (0, None)
        try:
            row_published = float(item.get("published_unix"))  # type: ignore[arg-type]
            bound_published = float(record.get("published_unix"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return (0, None)
        if row_published != bound_published:
            return (0, None)
        residency = item.get("residency")
        if not isinstance(residency, Mapping):
            return (0, None)
        for field in ("tier_id", "range_start_bytes", "range_end_bytes"):
            if str(residency.get(field)) != str(record.get(field)):
                return (0, None)
        # Sealed consumer/plan: the credit belongs to one frozen window.
        # The mover row carries no consumer field, so the record's binding
        # is checked against the live frozen plan itself.  Anything else --
        # no plan, an unreadable plan, a replaced decomposition under a new
        # digest -- authorizes nothing, and the coordinator re-fences next
        # cycle if the live window still wants this mover.
        try:
            from . import residency_plan as _residency_plan
            consumer_key = record.get("consumer_action_key")
            live_plan = (_residency_plan.read(
                self, str(consumer_key))
                if isinstance(consumer_key, str) else None)
            live_digest = (_residency_plan.plan_sha256(live_plan)
                           if live_plan is not None else None)
        except (OSError, PoolContractError, ValueError):
            return (0, None)
        except Exception:
            return (0, None)
        if (not isinstance(live_digest, str)
                or str(record.get("plan_sha256")) != live_digest):
            return (0, None)
        # Sealed-plan membership: the live plan must actually cut this mover
        # as a leg in the role this kind funds, over exactly the bound
        # range.  A matching digest plus a caller-supplied row range is not
        # enough: neither proves the plan names this mover for these bytes,
        # so a scope/consumer/manifest/entry outside the sealed plan never
        # inherits credit however well its row is typed.
        try:
            role = _FUNDING_KIND_ROLES.get(str(kind))
            leg = (None if role is None else
                   _residency_plan.find_mover_leg(live_plan, key))
        except (OSError, PoolContractError, ValueError):
            return (0, None)
        except Exception:
            return (0, None)
        if leg is None or str(leg.get("mover_role")) != str(role):
            return (0, None)
        try:
            plan_range = (int(leg["start_bytes"]), int(leg["end_bytes"]))
            bound_range = (int(record.get("range_start_bytes")),  # type: ignore[arg-type]
                           int(record.get("range_end_bytes")))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return (0, None)
        if plan_range != bound_range:
            return (0, None)
        tokens = record.get("tokens")
        if not isinstance(tokens, list) or not tokens:
            return (0, None)
        try:
            held_names = {path.name for path in _glob(
                self.tier_ledger(tier_id).held_dir / key, "*-*")}
        except (OSError, PoolContractError, ValueError):
            return (0, None)
        if any(not isinstance(name, str)
               or not name.startswith(str(kind) + "-")
               or name not in held_names for name in tokens):
            return (0, None)
        generation = record.get("generation")
        if not isinstance(generation, str):
            return (0, None)
        return (min(int(len(tokens)), int(need)), generation)

    def reserve_fence(self, tier_id: str, grant: str, fields: Mapping[str, object],
                      demand_gib: int) -> bool:
        """Hold one next-need under the grant key and bind it, idempotently.

        Serialized on the mover's transition lock (non-blocking: a claim
        holding it makes this defer rather than queue behind execution).
        See :meth:`_reserve_fence_locked` for the body.
        """

        mover = str(fields["mover_action_key"])
        with self.mover_transition_lock(mover, blocking=False) as acquired:
            if not acquired:
                return False
            return self._reserve_fence_locked(tier_id, grant, fields, demand_gib)

    def _reserve_fence_locked(self, tier_id: str, grant: str,
                              fields: Mapping[str, object],
                              demand_gib: int) -> bool:
        """Hold one next-need under the grant key and bind it, idempotently.

        Caller holds the mover's transition lock (see :meth:`reserve_fence`):
        the generation this call reads, writes, or replaces is never pulled
        out from under a verifying claim, because a claim holds the same
        lock from its tier acquire through its consumed-marking.

        If the grant already holds enough, only the record is ensured (a
        crash between ``acquire`` and ``write_funding`` repairs itself here
        by binding the already-held names).  A live ``reserved`` binding
        whose names still match what is held is likewise kept, never
        rotated -- rotating under a verifying claim would break its
        generation pin.  Otherwise takes the deficit from free and files a
        fresh ``reserved`` generation.  A fence already handed off (record
        ``transferring`` with its tokens verified under the mover, or
        ``consumed``) is left alone: re-reserving beside it would double-fence
        one advance.  A stale ``transferring`` split re-home verifies
        exact post-transfer ownership across mover+grant before closing
        (a short or failed per-token rename retains the record and the
        split for the next cycle).  ``fields`` must carry the mover row's
        ``published_unix`` alongside the plan binding.  Returns whether the
        fence is now held and bound.  Never raises for queue-state reasons;
        unknown -- an unreadable funding record or holder census -- is
        ``False`` (the caller defers), never a replace or a fresh take
        beside unknown authority.

        NOTE for the next output integration unit (not this primitive): this
        call acquires a fresh grant from free.  A later produced batch funded
        from the producer's already-declared prepaid working window must
        arrive by exact bound transfer from that existing window, not by a
        second acquire from free here -- no second output funding protocol;
        see output admission (PR735) via root.
        """

        mover = str(fields["mover_action_key"])
        status, current, funding_reason = self.read_funding_evidence(
            mover, str(tier_id))
        if status == "unknown":
            # A present-but-unreadable record may still bind held tokens:
            # retain it (defer), never fence beside unknown authority.
            return False
        current_generation = (str(current.get("generation"))
                              if isinstance(current, dict)
                              and isinstance(current.get("generation"), str)
                              else None)
        if current is not None and current.get("state") == "consumed":
            try:
                ledger = self.tier_ledger(tier_id)
                kind = str(fields["kind"])
                mover_holds = int(ledger.holder_tokens(mover).get(kind, 0))
            except (OSError, PoolContractError, ValueError, KeyError, TypeError):
                return False
            try:
                demand_holds = int(demand_gib) <= mover_holds
            except (TypeError, ValueError):
                return False
            if demand_holds:
                return True
            # Spent and released (finished, republished under a new
            # publication): fall through and fence afresh with a new
            # generation rather than wedge on a terminal record.
        if current is not None and current.get("state") == "transferring":
            try:
                ledger = self.tier_ledger(str(tier_id))
                ledger_names = held_names_visible(ledger, mover)
            except (OSError, PoolContractError, ValueError):
                return False
            bound = current.get("tokens")
            if (isinstance(bound, list) and bound
                    and all(str(name) in ledger_names for name in bound)):
                # Bound, but to which publication?  A republished mover (new
                # published_unix), replaced plan, or new range never inherits
                # an older generation's fence: the claim path would refuse
                # cover while the tokens stay held, stranding the fence and
                # double-holding the retry.  Re-home exactly the bound names
                # mover->grant without touching free (per-token renames under
                # held/, never via free_dir, so a competing key acquiring
                # from free cannot take them mid-move; a crash part-way
                # leaves the split sum recoverable by retrying the same
                # transfer), close the record through the legal
                # transferring->released step (checked: False is not a
                # completed transition), and fence afresh below -- all under
                # this mover's lock, same order as every reserve/settle path
                # (mover lock -> ledger -> funding file), so the #742 tier
                # lock composes outside without a new order and no second
                # queue or lifetime authority is introduced.  Output
                # accounting stays unenforced here (#748 owns the prepaid
                # variant; this still acquires fresh grant credit from free,
                # never a second output protocol).
                try:
                    fields_published = float(  # type: ignore[arg-type]
                        fields["published_unix"])
                    record_published = float(  # type: ignore[arg-type]
                        current.get("published_unix"))
                    stale = (
                        record_published != fields_published
                        or str(current.get("consumer_action_key")) != str(
                            fields["consumer_action_key"])
                        or str(current.get("plan_sha256")) != str(
                            fields["plan_sha256"])
                        or str(current.get("range_start_bytes")) != str(
                            fields["range_start_bytes"])
                        or str(current.get("range_end_bytes")) != str(
                            fields["range_end_bytes"]))
                except (KeyError, TypeError, ValueError):
                    stale = True
                if stale:
                    bound_set = {str(name) for name in bound}
                    keep = set(ledger_names) - bound_set
                    if keep:
                        # Mover holds more than the bound fence (pins,
                        # physical, or another live use the stale comparison
                        # does not establish as unused): retain, do not
                        # free or move anything.  The caller defers.
                        return False
                    # Mover holds exactly the bound set (no extras): move
                    # it to the grant without a free interval.
                    try:
                        moved = int(self.transfer_tier_reservation(
                            str(tier_id), mover, grant))
                    except (OSError, PoolContractError, ValueError):
                        return False
                    # transfer moves the whole holder; keep is empty so the
                    # count must equal the bound size -- anything else is a
                    # partial move (crash/race) to finish next cycle.
                    if moved != len(bound_set):
                        # Complete the remainder if split, else retain.
                        try:
                            rest = held_names_visible(ledger, mover)
                        except (OSError, PoolContractError, ValueError):
                            return False
                        if rest:
                            return False
                        # Nothing left under the mover but the count is
                        # short: names collided under the grant (another
                        # incarnation's tokens) -- retain, do not double.
                        return False
                    if not self._advance_funding_state_locked(
                            mover, str(tier_id), expect="transferring",
                            advance_to="released",
                            generation=(str(current.get("generation"))
                                        if isinstance(
                                            current.get("generation"), str)
                                        else None)):
                        return False
                    # Re-read: the released record below rotates (never
                    # unlinks) so the generation chain stays auditable,
                    # including across a metadata write failure (a failed
                    # rotate returns False below with the released record
                    # preserved, never torn away).  An unknown re-read
                    # defers: fencing afresh beside an unreadable record
                    # is fencing beside unknown authority.
                    _status, current, _reason = self.read_funding_evidence(
                        mover, str(tier_id))
                    if _status == "unknown":
                        return False
                else:
                    return True
            elif (isinstance(bound, list) and bound):
                # Bound names not all under the mover: either a previous
                # stale re-home moved them to the grant but the close is
                # pending, or an owner path freed them.  Complete a pending
                # re-home (bound all accounted for across mover+grant, mover
                # holding no extras) instead of fencing afresh beside the
                # remainder; otherwise retain -- stale evidence alone does
                # not establish unused authority for a fresh take.
                try:
                    fields_published = float(  # type: ignore[arg-type]
                        fields["published_unix"])
                    record_published = float(  # type: ignore[arg-type]
                        current.get("published_unix"))
                    maybe_stale = (
                        record_published != fields_published
                        or str(current.get("consumer_action_key")) != str(
                            fields["consumer_action_key"])
                        or str(current.get("plan_sha256")) != str(
                            fields["plan_sha256"])
                        or str(current.get("range_start_bytes")) != str(
                            fields["range_start_bytes"])
                        or str(current.get("range_end_bytes")) != str(
                            fields["range_end_bytes"]))
                except (KeyError, TypeError, ValueError):
                    maybe_stale = True
                if maybe_stale:
                    try:
                        grant_names = held_names_visible(
                            self.tier_ledger(str(tier_id)), grant)
                    except (OSError, PoolContractError, ValueError):
                        return False
                    bound_set = {str(name) for name in bound}
                    if (bound_set <= (set(ledger_names) | grant_names)
                            and not (set(ledger_names) - bound_set)):
                        # All bound names accounted for, mover holds no
                        # extras: finish moving the remainder grant-ward,
                        # then close (both checked).
                        try:
                            self.transfer_tier_reservation(
                                str(tier_id), mover, grant)
                        except (OSError, PoolContractError, ValueError):
                            return False
                        # Verify exact post-transfer ownership before
                        # closing: ``transfer`` suppresses individual
                        # rename errors and returns a short count, and a
                        # count alone cannot be read here anyway -- the
                        # previously moved names are already grant-ward
                        # and never appear in this call's count.  The
                        # mover must now hold nothing and the grant must
                        # cover every bound name; anything else is a
                        # partial move (real failed rename, collision) to
                        # finish next cycle -- never close
                        # transferring->released beside a leftover, never
                        # take a fresh deficit beside the split.
                        try:
                            ledger_after = self.tier_ledger(str(tier_id))
                            mover_left = held_names_visible(
                                ledger_after, mover)
                            grant_after = held_names_visible(
                                ledger_after, grant)
                        except (OSError, PoolContractError, ValueError):
                            return False
                        if mover_left or not (bound_set <= grant_after):
                            return False
                        if not self._advance_funding_state_locked(
                                mover, str(tier_id), expect="transferring",
                                advance_to="released",
                                generation=(str(current.get("generation"))
                                            if isinstance(
                                                current.get("generation"),
                                                str) else None)):
                            return False
                        _status, current, _reason = (
                            self.read_funding_evidence(
                                mover, str(tier_id)))
                        if _status == "unknown":
                            return False
                    else:
                        return False
                # else: live binding for this publication but tokens moved
                # (owner/claim path): fall through and fence afresh below
                # only when the record is not a stale transferring one.
                # A non-stale transferring record whose tokens moved belongs
                # to a live handoff -- report held (the claim verifies).
                if not maybe_stale:
                    return True
            # Bound tokens gone (released by an owner path): fall through and
            # fence afresh with a new generation rather than report held.
        if (current is not None and current.get("state") == "reserved"
                and isinstance(current.get("tokens"), list)):
            try:
                grant_names = held_names_visible(
                    self.tier_ledger(str(tier_id)), grant)
            except (OSError, PoolContractError, ValueError):
                return False
            try:
                fields_published = float(fields["published_unix"])  # type: ignore[arg-type]
                record_published = float(current.get("published_unix"))  # type: ignore[arg-type]
                published_same = record_published == fields_published
            except (KeyError, TypeError, ValueError):
                published_same = False
            if (set(str(name) for name in current["tokens"]) == grant_names  # type: ignore[index]
                    and str(current.get("consumer_action_key")) == str(
                        fields["consumer_action_key"])
                    and str(current.get("plan_sha256")) == str(
                        fields["plan_sha256"])
                    and str(current.get("mover_action_key")) == str(
                        fields["mover_action_key"])
                    and str(current.get("kind")) == str(fields["kind"])
                    and str(current.get("range_start_bytes")) == str(
                        fields["range_start_bytes"])
                    and str(current.get("range_end_bytes")) == str(
                        fields["range_end_bytes"])
                    and published_same):
                return True
            # Stale binding (republished mover, replaced plan, new range):
            # fall through and fence afresh with a new generation rather
            # than keep a binding the claim path would refuse.

        try:
            ledger = self.tier_ledger(tier_id)
            kind = str(fields["kind"])
            have = int(ledger.holder_tokens(grant).get(kind, 0))
        except (OSError, PoolContractError, ValueError, KeyError, TypeError):
            return False
        deficit = int(demand_gib) - have
        if deficit > 0:
            try:
                if not ledger.acquire(grant, {kind: int(deficit)}):
                    # ``acquire`` is all-or-nothing: a ``False`` leaves
                    # nothing half-held, so there is nothing to unwind.
                    return False
            except (OSError, PoolContractError, ValueError):
                return False
        try:
            names = sorted(held_names_visible(ledger, grant))
        except (OSError, PoolContractError, ValueError):
            return False
        generation = uuid.uuid4().hex
        try:
            published_unix = float(fields["published_unix"])  # type: ignore[arg-type]
        except (KeyError, TypeError, ValueError):
            return False
        record = {"schema": TIER_FUNDING_SCHEMA_V1, "tier_id": str(tier_id),
                  "consumer_action_key": str(fields["consumer_action_key"]),
                  "plan_sha256": str(fields["plan_sha256"]),
                  "mover_action_key": str(fields["mover_action_key"]),
                  "range_start_bytes": int(fields["range_start_bytes"]),
                  "range_end_bytes": int(fields["range_end_bytes"]),
                  "kind": kind, "tokens": names, "generation": generation,
                  "state": "reserved", "unix": time.time(),
                  "published_unix": published_unix}
        # Compare-and-swap on generation: a live binding is replaced only
        # against the generation this cycle read, so a verifying claim's pin
        # is never pulled out from under it -- a lost race simply defers to
        # next cycle.  Creates are births and require TRUE absence: a
        # present-but-unreadable record (torn write, empty file, I/O error)
        # may still bind held tokens, and unlinking it to unblock a fresh
        # fence destroys unknown authority -- R4's rule is that only a path
        # that is genuinely not there reads absent.  Replacements go
        # through rotation, which mints a fresh ``reserved`` generation and
        # cannot advance a state or edit a binding.
        try:
            if current is None:
                if self.funding_path(mover, str(tier_id)).exists():
                    # Appeared or unreadable since the read above (the
                    # mover lock makes the funding writer race impossible,
                    # so this is unknown evidence): retain, defer.
                    return False
                self.write_funding(record)
            else:
                self._rotate_funding_locked(
                    record, expect_generation=current_generation)
        except (OSError, PoolContractError, ValueError):
            return False
        return True

    def transfer_fence(self, tier_id: str, grant: str, mover: str) -> int:
        """Move a reserved fence onto its mover without a free interval.

        Per-token renames under ``held/``: at no instant is a token countable
        as free, so unrelated claims cannot take it mid-move, and a crash
        part-way leaves the sum split across the two holders -- recoverable by
        calling this again (it moves the remainder) or by the record-state
        rules.  The caller holds the mover's transition lock and advances the
        record itself; this moves only tokens.  Refuses (with ``0``) unless
        the mover already has a queue row: fusing onto a key that will never
        run strands the fence under a plan member the sweep spares.
        """

        try:
            if not (self.item_path(READY, mover).exists()
                    or self.item_path(CLAIMED, mover).exists()):
                return 0
            return int(self.transfer_tier_reservation(tier_id, grant, mover))
        except (OSError, PoolContractError, ValueError):
            return 0

    # -- prepaid-output funding records (same ledger/dir/table/lock, new binding)
    #
    # Funds one sealed produced-output batch from the producer's existing
    # prepaid window: exact token-subset transfer, never a second
    # reservation from free.  V1 (window) validation is untouched; this
    # variant carries its own schema and closed field set in the same
    # TIER_FUNDING directory, enforced through the same
    # _FUNDING_TRANSITIONS table under the same mover transition lock.
    # One authoritative intent per mover per tier per variant, owned here;
    # the produced-output lane references its generation and never mirrors
    # it with a second protocol.

    def funding_output_path(self, mover_action_key: str, tier_id: str) -> Path:
        """One output mover's funding record on one tier."""

        return (self.root / TIER_FUNDING
                / f"{mover_action_key}.{tier_id}.output-funding.json")

    @staticmethod
    def validate_output_funding(value: object) -> dict[str, object]:
        """Refuse an output funding record that is not exactly one binding.

        Closed field set like V1: unknown fields refuse, and anything that
        does not validate authorizes nothing (callers read it as no
        funding, never as zero or proof).
        """

        if not isinstance(value, Mapping):
            raise PoolContractError("an output funding record must be an object")
        unknown = sorted(set(value) - {
            "schema", "tier_id", "kind", "mover_action_key", "tokens",
            "generation", "state", "unix", "published_unix",
            "owner_action_key", "owner_nonce", "owner_scope_id",
            "owner_published_unix", "template_id", "template_sha256",
            "batch_id", "manifest_digest",
            "range_start_bytes", "range_end_bytes",
        })
        if unknown:
            raise PoolContractError(
                f"unknown output funding fields: {unknown}")
        if value.get("schema") != TIER_FUNDING_OUTPUT_SCHEMA_V1:
            raise PoolContractError(
                f"output funding schema must be {TIER_FUNDING_OUTPUT_SCHEMA_V1!r}")
        for field in ("tier_id", "kind", "template_id", "batch_id",
                      "owner_scope_id"):
            if (not isinstance(value.get(field), str) or not value[field]):
                raise PoolContractError(
                    f"output funding {field} must be a non-empty string")
        for field in ("owner_action_key", "mover_action_key"):
            key = value.get(field)
            if (not isinstance(key, str) or len(key) != 64
                    or any(c not in "0123456789abcdef" for c in key)):
                raise PoolContractError(
                    f"output funding {field} must be a 64-character action key")
        nonce = value.get("owner_nonce")
        if (not isinstance(nonce, str) or len(nonce) != 32
                or any(c not in "0123456789abcdef" for c in nonce)):
            raise PoolContractError(
                "output funding owner_nonce must be a 32-character nonce")
        for field in ("template_sha256", "manifest_digest"):
            digest = value.get(field)
            if (not isinstance(digest, str) or len(digest) != 64
                    or any(c not in "0123456789abcdef" for c in digest)):
                raise PoolContractError(
                    f"output funding {field} must be a 64-character digest")
        for field in ("range_start_bytes", "range_end_bytes"):
            number = value.get(field)
            if isinstance(number, bool) or not isinstance(number, int):
                raise PoolContractError(
                    f"output funding {field} must be a whole number of bytes")
        try:
            if int(value["range_end_bytes"]) <= int(value["range_start_bytes"]):  # type: ignore[arg-type]
                raise PoolContractError(
                    "output funding range must be non-empty half-open")
        except (KeyError, TypeError, ValueError):
            raise PoolContractError(
                "output funding range must be a whole number of bytes")
        tokens = value.get("tokens")
        if (not isinstance(tokens, list) or not tokens
                or any(not isinstance(name, str) or not name
                       for name in tokens)):
            raise PoolContractError(
                "output funding tokens must be a non-empty list of token names")
        if len(set(tokens)) != len(tokens):
            raise PoolContractError("output funding tokens must not repeat a name")
        kind = str(value.get("kind"))
        for name in tokens:
            assert isinstance(name, str)
            if not name.startswith(kind + "-"):
                raise PoolContractError(
                    "output funding tokens must all carry the bound kind prefix")
        generation = value.get("generation")
        if (not isinstance(generation, str) or len(generation) != 32
                or any(c not in "0123456789abcdef" for c in generation)):
            raise PoolContractError(
                "output funding generation must be a 32-character nonce")
        if value.get("state") not in TIER_FUNDING_STATES:
            raise PoolContractError(
                f"output funding state must be one of {sorted(TIER_FUNDING_STATES)}")
        for field in ("published_unix", "owner_published_unix"):
            published = value.get(field)
            if (isinstance(published, bool) or not isinstance(published, (int, float))
                    or not math.isfinite(float(published))
                    or float(published) < 0):
                raise PoolContractError(
                    f"output funding {field} must be a finite non-negative timestamp")
        for field in ("template_id", "batch_id", "owner_scope_id"):
            text = value.get(field)
            if (not isinstance(text, str) or not text or "/" in text
                    or "\x00" in text):
                raise PoolContractError(
                    f"output funding {field} must be a non-empty name with no '/'")
        for name in tokens:
            assert isinstance(name, str)
            if "/" in name or "\x00" in name:
                raise PoolContractError(
                    "output funding token names must not contain '/'")
        return dict(value)

    def read_output_funding(self, mover_action_key: str,
                            tier_id: str) -> dict[str, object] | None:
        """One output mover's funding record, or None when absent/unparsable.

        Note: None conflates absent with malformed/unreadable. Claim and
        census paths MUST use `output_funding_file_state` (absent vs unknown)
        instead of treating this None as legacy/no-funding; only the cover
        path (which requires a parsed transferring record) may use this.
        """

        try:
            raw = _read_json(self.funding_output_path(mover_action_key, tier_id))
        except (OSError, PoolContractError):
            return None
        if raw is None:
            return None
        try:
            return self.validate_output_funding(raw)
        except (PoolContractError, ValueError):
            return None

    def output_funding_file_state(
            self, mover_action_key: str, tier_id: str
    ) -> tuple[dict[str, object] | None, str]:
        """(record|None, file_state) with absent vs unknown split (R3).

        file_state: "absent" (proven ENOENT: no file, never had one or
        deliberately removed and detectable as required-absent by the claim
        gate via the filed-batch signal); "ok" (parsed valid record
        returned); "corrupt" (file exists but unreadable/unparsable/invalid:
        UNKNOWN, never fresh acquisition, never legacy). Other-owner files
        are untouched (per mover/tier path by construction).
        """

        path = self.funding_output_path(mover_action_key, tier_id)
        try:
            with open(path, "rb") as handle:
                raw_bytes = handle.read(1024 * 1024 + 1)
        except FileNotFoundError:
            # Proven ENOENT only is absent. Any other failure (including
            # NotADirectoryError: a path component is a file, i.e. corrupt
            # namespace, never proof an intent never existed) is UNKNOWN.
            return (None, "absent")
        except OSError:
            return (None, "corrupt")
        if len(raw_bytes) > 1024 * 1024:
            return (None, "corrupt")
        try:
            import json as _json
            raw = _json.loads(raw_bytes.decode())
        except (ValueError, UnicodeDecodeError):
            return (None, "corrupt")
        try:
            return (self.validate_output_funding(raw), "ok")
        except (PoolContractError, ValueError):
            return (None, "corrupt")

    def write_output_funding(self, record: Mapping[str, object], *,
                             expect_generation: str | None = None) -> Path:
        """File one output generation's binding, atomically.

        Same CAS + binding-immutability + table rules as V1, under the
        mover's transition lock (non-blocking; refuse when a claim holds
        it).  See _write_output_funding_locked for the body.
        """

        checked = self.validate_output_funding(record)
        mover = str(checked["mover_action_key"])
        with self.mover_transition_lock(mover, blocking=False) as acquired:
            if not acquired:
                raise PoolContractError(
                    "output funding writer lost the transition race")
            return self._write_output_funding_locked(
                checked, expect_generation=expect_generation)

    def _write_output_funding_locked(
            self, record: Mapping[str, object], *,
            expect_generation: str | None = None) -> Path:
        """File one output generation; caller holds the mover lock."""

        checked = self.validate_output_funding(record)
        path = self.funding_output_path(str(checked["mover_action_key"]),
                                        str(checked["tier_id"]))
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            raw = _read_json(path)
        except (OSError, PoolContractError):
            raise PoolContractError("output funding record unreadable for CAS")
        if raw is None:
            if expect_generation is not None:
                raise PoolContractError(
                    "output funding record vanished underneath this write")
            if checked.get("state") != "reserved":
                raise PoolContractError(
                    "output funding records are born reserved")
        else:
            try:
                current = self.validate_output_funding(raw)
            except (PoolContractError, ValueError):
                raise PoolContractError(
                    "output funding record unreadable for CAS")
            if (expect_generation is None
                    or str(current.get("generation")) != str(expect_generation)
                    or str(checked.get("generation")) != str(
                        current.get("generation"))):
                raise PoolContractError(
                    "output funding generation rotated underneath this write")
            for field in _FUNDING_OUTPUT_BINDING_FIELDS:
                if current.get(field) != checked.get(field):
                    raise PoolContractError(
                        f"output funding {field} is immutable within one generation")
            if (str(checked.get("state")) != str(current.get("state"))
                    and str(checked.get("state")) not in _FUNDING_TRANSITIONS.get(
                        str(current.get("state")), frozenset())):
                raise PoolContractError(
                    "output funding state step "
                    f"{current.get('state')!r}->{checked.get('state')!r} "
                    "is not a legal advance")
        _write_json_atomic(path, checked)
        return path

    def _rotate_output_funding_locked(
            self, record: Mapping[str, object], *,
            expect_generation: str | None) -> Path:
        """Replace one output generation with a fresh reserved one."""

        checked = self.validate_output_funding(record)
        if checked.get("state") != "reserved":
            raise PoolContractError(
                "rotated output funding generations are born reserved")
        path = self.funding_output_path(str(checked["mover_action_key"]),
                                        str(checked["tier_id"]))
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            raw = _read_json(path)
        except (OSError, PoolContractError):
            raise PoolContractError("output funding record unreadable for CAS")
        if raw is None:
            if expect_generation is not None:
                raise PoolContractError(
                    "output funding record vanished underneath this write")
        else:
            try:
                current = self.validate_output_funding(raw)
            except (PoolContractError, ValueError):
                raise PoolContractError(
                    "output funding record unreadable for CAS")
            if (expect_generation is None
                    or str(current.get("generation")) != str(expect_generation)):
                raise PoolContractError(
                    "output funding generation rotated underneath this write")
            if str(checked.get("generation")) == str(current.get("generation")):
                raise PoolContractError(
                    "rotation must mint a fresh generation")
        _write_json_atomic(path, checked)
        return path

    def advance_output_funding_state(
            self, mover_action_key: str, tier_id: str, *,
            expect: str, advance_to: str,
            generation: str | None = None) -> bool:
        """Move an output funding record one legal step, or refuse False."""

        with self.mover_transition_lock(str(mover_action_key),
                                        blocking=False) as acquired:
            if not acquired:
                return False
            return self._advance_output_funding_state_locked(
                mover_action_key, tier_id, expect=expect,
                advance_to=advance_to, generation=generation)

    def _advance_output_funding_state_locked(
            self, mover_action_key: str, tier_id: str, *,
            expect: str, advance_to: str,
            generation: str | None = None) -> bool:
        """Move an output record one step; caller holds the mover lock."""

        current = self.read_output_funding(mover_action_key, tier_id)
        if current is None or current.get("state") != expect:
            return False
        if (generation is not None
                and str(current.get("generation")) != str(generation)):
            return False
        if expect == advance_to:
            return True
        if advance_to not in _FUNDING_TRANSITIONS.get(str(expect), frozenset()):
            return False
        updated = dict(current)
        updated["state"] = advance_to
        updated["unix"] = time.time()
        try:
            self.write_output_funding(
                updated,
                expect_generation=(str(current.get("generation"))
                                   if isinstance(current.get("generation"),
                                                str) else None))
        except (OSError, PoolContractError, ValueError):
            return False
        return True

    def _output_live_owner(self, owner_key: str) -> tuple[dict | None, str | None]:
        """Live owner CLAIMED row + its published_unix, or (None, refusal)."""

        try:
            live = _read_json(self.item_path(CLAIMED, owner_key))
        except (OSError, PoolContractError):
            return None, "unknown-retain: owner-claim-unreadable"
        if live is None:
            return None, "owner-not-running"
        if not isinstance(live, Mapping):
            return None, "unknown-retain: owner-claim-shape"
        try:
            published = float(live.get("published_unix"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None, "unknown-retain: owner-claim-publication"
        control = live.get("resource_scope")
        nonce = scope = ""
        if isinstance(control, Mapping):
            candidate = control.get("nonce")
            if isinstance(candidate, str) and candidate:
                nonce = candidate
            for field in ("scope_id", "scope_unit", "unit"):
                unit = control.get(field)
                if isinstance(unit, str) and unit:
                    scope = unit
                    break
        if not nonce or not scope:
            return None, "unknown-retain: owner-claim-control"
        return {"row": live, "nonce": nonce, "scope": scope,
                "published_unix": published}, None

    def _output_live_mover(self, mover_key: str) -> tuple[dict | None, str | None]:
        """Sealed mover READY/CLAIMED row, or (None, refusal)."""

        for state in (READY, CLAIMED):
            try:
                row = _read_json(self.item_path(state, mover_key))
            except (OSError, PoolContractError):
                return None, "unknown-retain: mover-row-unreadable"
            if isinstance(row, Mapping):
                return {"row": row, "state": state}, None
        return None, "mover-not-published"

    def _output_owner_authority(self, record: Mapping[str, object]) -> bool:
        """Live-owner OR terminal-proof authority for one output intent (R1).

        Creation needs the exact live owner (fund path enforces it).  Recovery
        (cover/drive after producer finish, including finish-before-mover-claim
        and crash-with-partial-transfer) accepts EITHER:

        * live CLAIMED row still naming the bound nonce/scope/publication
          (owner still running same attempt), OR
        * no live CLAIMED row, but a terminal DONE/FAILED row for the same
          key naming the same published_unix + nonce/scope (owner finished
          the same attempt; credit belongs to copy/egress lifetime, not to
          the producer's remaining lifetime).

        Distinguishes completion from stale/tampered/unknown: a live row for
        another attempt (retry owns the key now) refuses without consulting
        the terminal (stale-superseded, never resurrected); a terminal with
        mismatched publication/nonce/scope refuses; missing/unreadable
        terminal refuses unknown (retain, never free).  Never admits a NEW
        batch from an old terminal: fund still requires live owner, so this
        helper is recovery-only (cover/drive), never creation.
        """

        try:
            owner_key = str(record.get("owner_action_key"))
            exp_nonce = str(record.get("owner_nonce"))
            exp_scope = str(record.get("owner_scope_id"))
            exp_published = float(record.get("owner_published_unix"))  # type: ignore[arg-type]
        except (TypeError, ValueError, KeyError):
            return False
        try:
            live = _read_json(self.item_path(CLAIMED, owner_key))
        except (OSError, PoolContractError):
            return False
        if isinstance(live, Mapping):
            try:
                live_published = float(live.get("published_unix"))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                return False
            control = live.get("resource_scope")
            live_nonce = live_scope = ""
            if isinstance(control, Mapping):
                candidate = control.get("nonce")
                if isinstance(candidate, str) and candidate:
                    live_nonce = candidate
                for field in ("scope_id", "scope_unit", "unit"):
                    unit = control.get(field)
                    if isinstance(unit, str) and unit:
                        live_scope = unit
                        break
            if (live_published == exp_published and live_nonce == exp_nonce
                    and live_scope == exp_scope):
                return True
            # Live row for another attempt: stale-superseded, never fall
            # through to an old terminal to resurrect it.
            return False
        if live is not None:
            return False
        # No live claim: consult the terminal for the same attempt.
        for state in (DONE, FAILED):
            try:
                terminal = _read_json(self.item_path(state, owner_key))
            except (OSError, PoolContractError):
                return False
            if not isinstance(terminal, Mapping):
                continue
            try:
                term_published = float(terminal.get("published_unix"))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                continue
            if term_published != exp_published:
                continue
            control = terminal.get("resource_scope")
            term_nonce = term_scope = ""
            if isinstance(control, Mapping):
                candidate = control.get("nonce")
                if isinstance(candidate, str) and candidate:
                    term_nonce = candidate
                for field in ("scope_id", "scope_unit", "unit"):
                    unit = control.get(field)
                    if isinstance(unit, str) and unit:
                        term_scope = unit
                        break
            if term_nonce == exp_nonce and term_scope == exp_scope:
                return True
        return False

    def _output_batch_authority(self, record: Mapping[str, object]) -> bool:
        """Filed-commit authority via R4 loader, deterministic paths (R2).

        Cover (claim) requires a PREVIOUSLY COMMITTED exact-attempt batch;
        it must NOT promote an unfinished prewrite. Uses the merged R4
        candidate's single strict loader (`_load_batch_record`: bounded 4MB
        intake, exact schema/id/instance binding, strict ints, non-empty
        entries re-validated as descriptors with recomputed manifest),
        never a hand-written batch validator. Deterministic paths only
        (no scope/template scans, no unbounded reads): instance at
        `scopes/{owner}/{template_id}.{nonce}/instance.json` (bounded 64KB),
        template at `produced-output-templates/{template_id}.json` (bounded
        1MB), commitments via existing `_read_commitments`, batch file via
        the loader. A missing/unreadable/mismatched commit is unknown (no
        credit); a failed loader never downgrades to prewrite authorization.
        Validates exact instance/template/batch/range using existing
        validators; rejects non-finite/malformed via the loader + record
        validation (record itself already refused non-finite timestamps and
        '/' names at write time).
        """

        try:
            from . import produced_output as produced_mod
            import json as _json
        except ImportError:
            return False
        try:
            owner_key = str(record.get("owner_action_key"))
            template_id = str(record.get("template_id"))
            template_sha = str(record.get("template_sha256"))
            batch_id = str(record.get("batch_id"))
            manifest = str(record.get("manifest_digest"))
            mover_key = str(record.get("mover_action_key"))
            tier_id = str(record.get("tier_id"))
            bound_range = (int(record.get("range_start_bytes")),  # type: ignore[arg-type]
                           int(record.get("range_end_bytes")))  # type: ignore[arg-type]
            owner_nonce = str(record.get("owner_nonce"))
            owner_scope = str(record.get("owner_scope_id"))
        except (TypeError, ValueError, KeyError):
            return False
        if (not owner_key or not template_id or not batch_id or "/" in batch_id
                or "/" in template_id):
            return False
        if bound_range[0] != 0 or bound_range[1] <= 0:
            return False
        total = bound_range[1] - bound_range[0]
        # Deterministic instance path (no scan).
        try:
            inst_path = (Path(self.root) / "residency"
                         / produced_mod.OUTPUT_SCOPES_SUBDIR / owner_key
                         / f"{template_id}.{owner_nonce}" / "instance.json")
            try:
                with open(inst_path, "rb") as handle:
                    raw_inst = handle.read(64 * 1024 + 1)
            except FileNotFoundError:
                return False
            except OSError:
                return False
            if len(raw_inst) > 64 * 1024:
                return False
            try:
                instance = produced_mod.validate_instance(
                    _json.loads(raw_inst.decode()))
            except (ValueError, UnicodeDecodeError,
                    produced_mod.ProducedOutputError):
                return False
        except (OSError, PoolContractError, ValueError):
            return False
        if (str(instance.get("owner_action_key")) != owner_key
                or str(instance.get("template_sha256")) != template_sha
                or str(instance.get("template_id")) != template_id):
            return False
        attempt = instance.get("owner_attempt")
        if (not isinstance(attempt, dict)
                or str(attempt.get("nonce")) != owner_nonce
                or str(attempt.get("scope_id")) != owner_scope):
            return False
        # Deterministic template path (no scan).
        try:
            tmpl_path = (Path(self.root) / "residency"
                         / produced_mod.OUTPUT_TEMPLATES_SUBDIR
                         / f"{template_id}.json")
            try:
                with open(tmpl_path, "rb") as handle:
                    raw_tmpl = handle.read(1024 * 1024 + 1)
            except FileNotFoundError:
                return False
            except OSError:
                return False
            if len(raw_tmpl) > 1024 * 1024:
                return False
            try:
                checked_template = produced_mod.validate_template(
                    _json.loads(raw_tmpl.decode()))
            except (ValueError, UnicodeDecodeError,
                    produced_mod.ProducedOutputError):
                return False
        except (OSError, PoolContractError, ValueError):
            return False
        if produced_mod.template_sha256(checked_template) != template_sha:
            return False
        # Commitments entry (existing helper; small file per instance).
        try:
            commitments = produced_mod._read_commitments(
                produced_mod._commitments_path(self.root, instance))
        except produced_mod.ProducedOutputError:
            return False
        batches = commitments.get("batches")
        if not isinstance(batches, dict):
            return False
        entry = batches.get(batch_id)
        if not isinstance(entry, dict):
            return False
        if (str(entry.get("manifest_digest")) != manifest
                or str(entry.get("tier")) != tier_id):
            return False
        # WHICH mover this committed batch authorizes. The batch's own first
        # publication always does. A RE-materialization of the same batch
        # (`produced_output.ensure_batch_materialized`) stages the identical
        # manifest over the identical origins under a successor mover whose
        # key PB sealed and filed on the batch's materialization list, so the
        # committed batch authorizes that key too -- and ONLY while it is the
        # single live row on that list, with the batch's own tier. A
        # malformed list is unknown and authorizes nothing (no credit), never
        # a downgrade to the prewrite path.
        if str(entry.get("mover_key")) != mover_key:
            try:
                live = produced_mod._live_materialization(entry)
            except produced_mod.ProducedOutputError:
                return False
            except (OSError, ValueError):
                return False
            if (live is None
                    or str(live.get("mover_key")) != mover_key
                    or str(live.get("tier")) != tier_id):
                return False
        # Strict loader (R4): bounded, exact binding, re-validated entries,
        # recomputed manifest. Missing/empty entries refuse inside (never
        # vacuously True); corrupt/mismatched commitments never fall through
        # to prewrite (no downgrade).
        try:
            filed, _ = produced_mod._load_batch_record(
                self.root, instance, checked_template, entry, batch_id)
        except produced_mod.ProducedOutputError:
            return False
        except (OSError, ValueError):
            return False
        if (str(filed.get("manifest_digest")) != manifest
                or str(filed.get("tier")) != tier_id
                or int(filed.get("total_bytes", -1)) != total):
            return False
        # The immutable record names the FIRST mover; a successor is
        # authorized only through the filed materialization list checked
        # above, and the record's own mover must still agree with the entry
        # (a changed mover in commitments authorizes nothing).
        if (str(filed.get("mover_key")) != str(entry.get("mover_key"))
                or (str(filed.get("mover_key")) != mover_key
                    and not any(
                        str(item.get("mover_key")) == mover_key
                        for item in (entry.get("materializations") or [])
                        if isinstance(item, Mapping)))):
            return False
        return True

    def _output_precommit_authority(self, record: Mapping[str, object]) -> bool:
        """Durable-prewrite authority for drive/fund recovery (R2).

        Deterministic instance path (no scan), bounded prewrite read (64KB
        via `_read_prewrite` helper which reads one file; prewrites are
        tiny). Checks tier/owner/attempt match + class_bytes total == intent
        range total. Used by drive (transfer remainder before commit) and
        fund (creation); NEVER by cover/claim (which requires filed commit
        via `_output_batch_authority`, so an unfinished prewrite is never
        promoted to a committed output).
        """

        try:
            from . import produced_output as produced_mod
            import json as _json
        except ImportError:
            return False
        try:
            owner_key = str(record.get("owner_action_key"))
            template_id = str(record.get("template_id"))
            template_sha = str(record.get("template_sha256"))
            batch_id = str(record.get("batch_id"))
            tier_id = str(record.get("tier_id"))
            bound_range = (int(record.get("range_start_bytes")),  # type: ignore[arg-type]
                           int(record.get("range_end_bytes")))  # type: ignore[arg-type]
            owner_nonce = str(record.get("owner_nonce"))
        except (TypeError, ValueError, KeyError):
            return False
        if bound_range[0] != 0 or bound_range[1] <= 0:
            return False
        total = bound_range[1] - bound_range[0]
        try:
            inst_path = (Path(self.root) / "residency"
                         / produced_mod.OUTPUT_SCOPES_SUBDIR / owner_key
                         / f"{template_id}.{owner_nonce}" / "instance.json")
            try:
                with open(inst_path, "rb") as handle:
                    raw_inst = handle.read(64 * 1024 + 1)
            except (FileNotFoundError, OSError):
                return False
            if len(raw_inst) > 64 * 1024:
                return False
            try:
                instance = produced_mod.validate_instance(
                    _json.loads(raw_inst.decode()))
            except (ValueError, UnicodeDecodeError,
                    produced_mod.ProducedOutputError):
                return False
        except (OSError, PoolContractError, ValueError):
            return False
        if (str(instance.get("template_sha256")) != template_sha):
            return False
        try:
            prewrite = produced_mod._read_prewrite(
                produced_mod._prewrites_dir(self.root, instance)
                / f"{batch_id}.prewrite.json")
        except produced_mod.ProducedOutputError:
            return False
        if prewrite is None:
            return False
        if str(prewrite.get("tier")) != tier_id:
            return False
        if str(prewrite.get("owner_action_key")) != owner_key:
            return False
        try:
            if dict(prewrite.get("owner_attempt", {})) != dict(
                    instance.get("owner_attempt", {})):
                return False
            classes = dict(prewrite.get("class_bytes", {}))
            pre_total = (int(classes.get("payload", 0))
                         + int(classes.get("checkpoint", 0))
                         + int(classes.get("temp", 0)))
        except (TypeError, ValueError):
            return False
        # Ceiling reconciliation: the durable prewrite admits per-class
        # upper bounds; the intent's bound range is the actual total AT OR
        # UNDER the prewrite's admitted total (exact sizes are the special
        # case of an exact ceiling).
        return pre_total >= total


    def stage_output_intent(self, *, tier_id: str, owner_key: str,
                              mover_key: str, instance, template,
                              batch_id: str,
                              descriptors: list[Mapping[str, object]],
                              token_names: Sequence[str] | None = None,
                              mover_published: float | None = None) -> dict:
        """Stage an output funding intent (reserved, no transfer) (R2).

        Claim-safe writer order: prewrite -> stage intent (this call, no mover
        row required, no tokens moved) -> publish mover -> drive/commit
        (transfer + `transferring`) -> commit batch (filed) -> claim. Because
        READY publication happens AFTER the intent exists, a crash after READY
        always leaves a durable intent for recovery, and the claim gate below
        (pending intent => no fresh-acquisition fallback) never faces a READY
        output mover with no intent. Caller may pass `mover_published` when
        the mover row already exists (e.g., tests using publish-before-stage);
        otherwise the publication is bound at drive/commit time and re-checked
        there. Holds owner-outer/mover-inner (non-blocking, defer on
        contention). Creation authority only (exact live owner + precommit);
        never admits from a terminal.
        """

        try:
            tier = self._check_tier_id(str(tier_id))
        except (PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        for label, key in (("owner", owner_key), ("mover", mover_key)):
            if (not isinstance(key, str) or len(key) != 64
                    or any(c not in "0123456789abcdef" for c in key)):
                return {"ok": False, "refusal": f"bad-{label}-key"}
        if not isinstance(batch_id, str) or not batch_id or "/" in batch_id:
            return {"ok": False, "refusal": "bad-batch-id"}
        if not isinstance(descriptors, list) or not descriptors:
            return {"ok": False, "refusal": "bad-descriptors"}
        try:
            from . import produced_output as produced_mod
            binding = produced_mod.describe_output_precommit_for_funding(
                self, instance, template, batch_id, descriptors,
                tier, str(mover_key))
        except produced_mod.ProducedOutputError as exc:
            text = str(exc)
            if "template-mismatch" in text:
                return {"ok": False, "refusal": "template-mismatch"}
            if "stale" in text or "superseded" in text:
                return {"ok": False, "refusal": "stale-superseded-owner"}
            if "owner-not-running" in text:
                return {"ok": False, "refusal": "owner-not-running"}
            if "prewrite-reservation-missing" in text:
                return {"ok": False, "refusal": "prewrite-reservation-missing"}
            if "prewrite-mismatch" in text:
                return {"ok": False, "refusal": "prewrite-mismatch"}
            if text.startswith("restage-") or text.startswith(
                    "materialization-"):
                # A re-materialization whose origins no longer carry the
                # identity the first commit recorded, or that never had that
                # proof. Named, not collapsed to unknown: funding must refuse
                # for the reason that refused it.
                return {"ok": False, "refusal": text}
            if text.startswith("unknown-retain"):
                return {"ok": False, "refusal": text}
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        except (OSError, PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        except Exception as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        try:
            kind = storage_tiers.capacity_kind_of(tier)
            total = int(binding["total_bytes"])
            batch_gib = storage_tiers.stage_tokens_for_bytes(total)
            checked_template = produced_mod.validate_template(template)
            window = int(checked_template["working_demands"][tier]["window_gib"])
        except Exception as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if batch_gib > window:
            return {"ok": False, "refusal": "batch-exceeds-window",
                    "batch_gib": batch_gib, "window_gib": window}
        owner_live, refusal = self._output_live_owner(str(owner_key))
        if owner_live is None:
            return {"ok": False, "refusal": refusal}
        if (str(owner_live["nonce"]) != str(binding.get("owner_nonce"))
                or str(owner_live["scope"]) != str(binding.get("owner_scope_id"))):
            return {"ok": False, "refusal": "stale-superseded-owner"}
        try:
            ledger = self.tier_ledger(tier)
            held_names = sorted(path.name for path in _glob(
                ledger.held_dir / str(owner_key), "*-*")
                if path.name.startswith(kind + "-"))
        except (OSError, PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if self.read_output_funding(str(mover_key), tier) is not None:
            # Re-drive of an already-filed intent needs no fresh selection:
            # its own names are already filed and would collide with the
            # disjointness rule below; the in-lock duplicate branch returns
            # the filed set unchanged. A record that vanishes before the
            # in-lock read fails closed at validation (empty selection).
            selected = []
        else:
            # Sequential per-batch funding (R7 liveness): a name already
            # promised to one of this owner's outstanding intents on this
            # tier is spoken for. Selecting it again files two intents over
            # one credit, and whichever funds first strands the other in a
            # permanent transfer-short. Refuse deterministically instead.
            spoken, spoken_unknown = self._output_spoken_token_names(
                str(owner_key), tier)
            if spoken_unknown:
                return {"ok": False, "refusal": "unknown-retain: funding-census"}
            unspoken_names = [name for name in held_names
                              if name not in spoken]
            if token_names is not None:
                try:
                    selected = sorted(str(name) for name in token_names)
                except (TypeError, ValueError):
                    return {"ok": False, "refusal": "bad-token-names"}
                if len(set(selected)) != len(selected) or not selected:
                    return {"ok": False, "refusal": "bad-token-names"}
                if len(selected) != batch_gib:
                    return {"ok": False, "refusal": "token-names-unknown"}
                for name in selected:
                    if not name.startswith(kind + "-") or name not in held_names:
                        return {"ok": False, "refusal": "token-names-unknown"}
                    if name in spoken:
                        return {"ok": False, "refusal": "token-names-spoken"}
            else:
                if len(unspoken_names) < batch_gib:
                    return {"ok": False, "refusal": "tier-reservation-unavailable",
                            "available": ledger.available()}
                selected = unspoken_names[:batch_gib]
        with self._transition_locked(str(owner_key),
                                     blocking=False) as owner_acquired:
            if not owner_acquired:
                return {"ok": False, "refusal": "funding-race-deferred"}
            with self.mover_transition_lock(str(mover_key),
                                            blocking=False) as mover_acquired:
                if not mover_acquired:
                    return {"ok": False, "refusal": "funding-race-deferred"}
                # Re-validate live owner under locks.
                owner_live2, refusal2 = self._output_live_owner(str(owner_key))
                if owner_live2 is None:
                    return {"ok": False, "refusal": refusal2}
                if (str(owner_live2["nonce"]) != str(binding.get("owner_nonce"))
                        or str(owner_live2["scope"]) != str(
                            binding.get("owner_scope_id"))
                        or float(owner_live2["published_unix"]) != float(
                            owner_live["published_unix"])):
                    return {"ok": False, "refusal": "stale-superseded-owner"}
                current = self.read_output_funding(str(mover_key), tier)
                if current is not None:
                    if (str(current.get("batch_id")) != str(binding.get("batch_id"))
                            or str(current.get("manifest_digest")) != str(
                                binding.get("manifest_digest"))
                            or str(current.get("owner_action_key")) != str(owner_key)
                            or str(current.get("template_sha256")) != str(
                                binding.get("template_sha256"))):
                        return {"ok": False, "refusal": "batch-id-in-use"}
                    if str(current.get("state")) in ("consumed", "released"):
                        return {"ok": False, "refusal": "batch-id-in-use"}
                    return {"ok": True,
                            "generation": str(current.get("generation")),
                            "tokens": [str(n) for n in current.get("tokens", [])],  # type: ignore[union-attr]
                            "moved": 0, "staged": True,
                            "duplicate": True}
                generation = uuid.uuid4().hex
                record = {
                    "schema": TIER_FUNDING_OUTPUT_SCHEMA_V1,
                    "tier_id": tier, "kind": kind,
                    "mover_action_key": str(mover_key),
                    "tokens": sorted(selected),
                    "generation": generation, "state": "reserved",
                    "unix": time.time(),
                    "published_unix": (float(mover_published)
                                       if isinstance(mover_published,
                                                     (int, float))
                                       and math.isfinite(float(mover_published))
                                       and float(mover_published) >= 0
                                       else 0.0),
                    "owner_action_key": str(owner_key),
                    "owner_nonce": str(binding.get("owner_nonce")),
                    "owner_scope_id": str(binding.get("owner_scope_id")),
                    "owner_published_unix": float(owner_live["published_unix"]),
                    "template_id": str(binding.get("template_id")),
                    "template_sha256": str(binding.get("template_sha256")),
                    "batch_id": str(binding.get("batch_id")),
                    "manifest_digest": str(binding.get("manifest_digest")),
                    "range_start_bytes": int(binding.get("range_start_bytes")),
                    "range_end_bytes": int(binding.get("range_end_bytes")),
                }
                if float(record["published_unix"]) == 0.0:
                    # Unpublished at stage time (claim-safe writer order:
                    # stage intent before READY publication). 0.0 never
                    # matches a real mover publication, so this intent
                    # authorizes nothing until drive rotates it to the real
                    # mover publication after publish (single-file rotation,
                    # never a second intent).
                    pass
                try:
                    self._write_output_funding_locked(
                        record, expect_generation=None)
                except (OSError, PoolContractError, ValueError) as exc:
                    return {"ok": False, "refusal": f"unknown-retain: {exc}"}
                return {"ok": True, "generation": generation,
                        "tokens": sorted(selected), "moved": 0,
                        "staged": True}

    def fund_output_batch(self, *, tier_id: str, owner_key: str,
                          mover_key: str, instance, template,
                          batch_id: str,
                          descriptors: list[Mapping[str, object]],
                          token_names: Sequence[str] | None = None) -> dict:
        """Fund one precommitted batch from the owner's existing window (R1).

        Future writer order (744 wires; this lane does not edit commit):
        prewrite (durable budget) -> publish mover (from precommit
        descriptors) -> fund (this intent, authoritative) -> transfer ->
        commit batch (filed batch references funding generation) -> claim
        mover.  No second authoritative funding record: the pool intent is
        authoritative; prewrite is budget; filed batch is commit.

        Exact subset transfer, never a second reservation from free.
        Validates authority from LIVE precommit records via
        ``produced_output.describe_output_precommit_for_funding`` (bound
        template/live owner/prewrite/descriptors/manifest; filed batch NOT
        required) plus live mover publication; caller hashes prove nothing.

        Lock order (R1 fix): owner transition lock outer, mover inner,
        both non-blocking; ``funding-race-deferred`` on contention.  This
        serializes token selection/intent publication against the owner
        finish/reaper transition (which holds the owner lock around its
        tier release; see ``_release_reservation``).  Listing intents is
        not a lock; this is.
        """

        try:
            tier = self._check_tier_id(str(tier_id))
        except (PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        for label, key in (("owner", owner_key), ("mover", mover_key)):
            if (not isinstance(key, str) or len(key) != 64
                    or any(c not in "0123456789abcdef" for c in key)):
                return {"ok": False, "refusal": f"bad-{label}-key"}
        if not isinstance(batch_id, str) or not batch_id or "/" in batch_id:
            return {"ok": False, "refusal": "bad-batch-id"}
        if not isinstance(descriptors, list) or not descriptors:
            return {"ok": False, "refusal": "bad-descriptors"}
        try:
            from . import produced_output as produced_mod
            binding = produced_mod.describe_output_precommit_for_funding(
                self, instance, template, batch_id, descriptors,
                tier, str(mover_key))
        except produced_mod.ProducedOutputError as exc:
            text = str(exc)
            if "template-mismatch" in text:
                return {"ok": False, "refusal": "template-mismatch"}
            if "stale" in text or "superseded" in text:
                return {"ok": False, "refusal": "stale-superseded-owner"}
            if "owner-not-running" in text:
                return {"ok": False, "refusal": "owner-not-running"}
            if "prewrite-reservation-missing" in text:
                return {"ok": False, "refusal": "prewrite-reservation-missing"}
            if "prewrite-mismatch" in text:
                return {"ok": False, "refusal": "prewrite-mismatch"}
            if text.startswith("restage-") or text.startswith(
                    "materialization-"):
                # A re-materialization whose origins no longer carry the
                # identity the first commit recorded, or that never had that
                # proof. Named, not collapsed to unknown: funding must refuse
                # for the reason that refused it.
                return {"ok": False, "refusal": text}
            if text.startswith("unknown-retain"):
                return {"ok": False, "refusal": text}
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        except (OSError, PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        except Exception as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if str(binding.get("owner_action_key")) != str(owner_key):
            return {"ok": False, "refusal": "owner-mismatch"}
        try:
            kind = storage_tiers.capacity_kind_of(tier)
        except (ValueError, PoolContractError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        try:
            total = int(binding["total_bytes"])
            batch_gib = storage_tiers.stage_tokens_for_bytes(total)
        except (KeyError, TypeError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        try:
            from . import produced_output as produced_mod2
            checked_template = produced_mod2.validate_template(template)
            window = int(checked_template["working_demands"][tier]["window_gib"])
        except Exception as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if batch_gib > window:
            return {"ok": False, "refusal": "batch-exceeds-window",
                    "batch_gib": batch_gib, "window_gib": window}
        # Live reads before locking (selection is pre-lock; intent filing
        # + transfer are under owner->mover locks below).
        owner_live, refusal = self._output_live_owner(str(owner_key))
        if owner_live is None:
            return {"ok": False, "refusal": refusal}
        if (str(owner_live["nonce"]) != str(binding.get("owner_nonce"))
                or str(owner_live["scope"]) != str(binding.get("owner_scope_id"))):
            return {"ok": False, "refusal": "stale-superseded-owner"}
        mover_live, refusal = self._output_live_mover(str(mover_key))
        if mover_live is None:
            return {"ok": False, "refusal": refusal}
        mover_row = mover_live["row"]
        assert isinstance(mover_row, dict)
        try:
            mover_published = float(mover_row.get("published_unix"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return {"ok": False, "refusal": "unknown-retain: mover-publication"}
        residency = mover_row.get("residency")
        if not isinstance(residency, Mapping):
            return {"ok": False, "refusal": "mover-publication-mismatch"}
        if (str(residency.get("tier_id")) != tier
                or str(residency.get("manifest_sha256")) != str(
                    binding.get("manifest_digest"))):
            return {"ok": False, "refusal": "mover-publication-mismatch"}
        try:
            mover_range = (int(residency.get("range_start_bytes")),  # type: ignore[arg-type]
                           int(residency.get("range_end_bytes")))  # type: ignore[arg-type]
            bound_range = (int(binding.get("range_start_bytes")),
                           int(binding.get("range_end_bytes")))
        except (TypeError, ValueError):
            return {"ok": False, "refusal": "mover-publication-mismatch"}
        if mover_range != bound_range:
            return {"ok": False, "refusal": "mover-publication-mismatch"}
        # Sealed requirement (R4): funding binds only to a mover row carrying
        # the matching immutable `produced_output_batch` projection. A row
        # without it (legacy, or omitted requirement) or with a contradictory
        # one refuses here, before any token moves.
        sealed_ref = mover_row.get("produced_output_batch")
        if (not isinstance(sealed_ref, Mapping)
                or sealed_ref.get("schema")
                != PRODUCED_OUTPUT_BATCH_REF_SCHEMA_V1
                or str(sealed_ref.get("batch_id")) != str(binding.get("batch_id"))
                or str(sealed_ref.get("manifest_digest")) != str(
                    binding.get("manifest_digest"))
                or str(sealed_ref.get("tier_id")) != tier
                or str(sealed_ref.get("owner_action_key")) != str(
                    binding.get("owner_action_key"))
                or str(sealed_ref.get("template_sha256")) != str(
                    binding.get("template_sha256"))):
            return {"ok": False, "refusal": "mover-publication-mismatch"}
        try:
            ledger = self.tier_ledger(tier)
        except (OSError, PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        try:
            held_names = sorted(path.name for path in _glob(
                ledger.held_dir / str(owner_key), "*-*")
                if path.name.startswith(kind + "-"))
        except (OSError, PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        # Same disjointness rule as stage_output_intent (R7 liveness): a
        # name already promised to one of this owner's outstanding intents
        # is spoken for. This block only selects for a FRESH intent: when
        # a record already exists for this mover/tier, its own filed set
        # is authoritative (and is itself spoken, by itself), so selection
        # is skipped and the in-lock reconciliation drives the filed set.
        if self.read_output_funding(str(mover_key), tier) is not None:
            selected = []
        else:
            spoken, spoken_unknown = self._output_spoken_token_names(
                str(owner_key), tier)
            if spoken_unknown:
                return {"ok": False,
                        "refusal": "unknown-retain: funding-census"}
            unspoken_names = [name for name in held_names
                              if name not in spoken]
            if token_names is not None:
                try:
                    selected = sorted(str(name) for name in token_names)
                except (TypeError, ValueError):
                    return {"ok": False, "refusal": "bad-token-names"}
                if len(set(selected)) != len(selected) or not selected:
                    return {"ok": False, "refusal": "bad-token-names"}
                if len(selected) != batch_gib:
                    return {"ok": False, "refusal": "token-names-unknown"}
                for name in selected:
                    if not name.startswith(kind + "-"):
                        return {"ok": False,
                                "refusal": "token-names-unknown"}
                    if name not in held_names:
                        return {"ok": False,
                                "refusal": "token-names-unknown"}
                    if name in spoken:
                        return {"ok": False, "refusal": "token-names-spoken"}
            else:
                if len(unspoken_names) < batch_gib:
                    return {"ok": False,
                            "refusal": "tier-reservation-unavailable",
                            "available": ledger.available()}
                selected = unspoken_names[:batch_gib]
        # Owner outer, mover inner (R1): serializes against owner finish.
        with self._transition_locked(str(owner_key),
                                     blocking=False) as owner_acquired:
            if not owner_acquired:
                return {"ok": False, "refusal": "funding-race-deferred"}
            with self.mover_transition_lock(str(mover_key),
                                            blocking=False) as mover_acquired:
                if not mover_acquired:
                    return {"ok": False, "refusal": "funding-race-deferred"}
                return self._fund_output_batch_locked(
                    tier=tier, kind=kind, owner_key=str(owner_key),
                    mover_key=str(mover_key), binding=binding,
                    batch_gib=batch_gib, selected=selected,
                    owner_published=float(owner_live["published_unix"]),
                    mover_published=float(mover_published))

    def _fund_output_batch_locked(self, *, tier: str, kind: str,
                                  owner_key: str, mover_key: str,
                                  binding: Mapping[str, object],
                                  batch_gib: int, selected: list[str],
                                  owner_published: float,
                                  mover_published: float) -> dict:
        """Fund body; caller holds owner outer + mover inner (R1 order)."""

        # Re-read live rows under the lock: a republish between selection
        # and filing must not fund stale credit.
        owner_live, refusal = self._output_live_owner(owner_key)
        if owner_live is None:
            return {"ok": False, "refusal": refusal}
        if (str(owner_live["nonce"]) != str(binding.get("owner_nonce"))
                or str(owner_live["scope"]) != str(binding.get("owner_scope_id"))
                or float(owner_live["published_unix"]) != float(owner_published)):
            return {"ok": False, "refusal": "stale-superseded-owner"}
        mover_live, refusal = self._output_live_mover(mover_key)
        if mover_live is None:
            return {"ok": False, "refusal": refusal}
        try:
            live_mover_published = float(mover_live["row"].get("published_unix"))  # type: ignore[union-attr,arg-type]
        except (TypeError, ValueError, AttributeError):
            return {"ok": False, "refusal": "unknown-retain: mover-publication"}
        if live_mover_published != float(mover_published):
            return {"ok": False, "refusal": "mover-publication-mismatch"}
        try:
            ledger = self.tier_ledger(tier)
            held_now = {path.name for path in _glob(
                ledger.held_dir / owner_key, "*-*")}
        except (OSError, PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        # Existing record for this mover: idempotent re-drive, never a
        # second authoritative intent.
        current = self.read_output_funding(mover_key, tier)
        if current is not None:
            if (str(current.get("batch_id")) != str(binding.get("batch_id"))
                    or str(current.get("manifest_digest")) != str(
                        binding.get("manifest_digest"))
                    or str(current.get("owner_action_key")) != owner_key
                    or str(current.get("template_sha256")) != str(
                        binding.get("template_sha256"))):
                return {"ok": False, "refusal": "batch-id-in-use"}
            if current.get("state") in ("consumed", "released"):
                # Spent or retired: a funded batch never re-funds.  If the
                # mover still holds the full set under the same publication
                # this is a duplicate fund call after claim; report it as
                # duplicate rather than funding again.
                if current.get("state") == "consumed":
                    return {"ok": True, "generation": str(current.get("generation")),
                            "tokens": list(current.get("tokens") or []),
                            "moved": int(batch_gib), "duplicate": True}
                return {"ok": False, "refusal": "batch-id-in-use"}
            # Live reserved/transferring for the same batch: reuse its
            # generation + token set (never rotate under a verifying
            # claim); a caller-selected subset differing from the filed
            # set refuses rather than forking the intent.
            filed_tokens = current.get("tokens")
            if (not isinstance(filed_tokens, list) or not filed_tokens
                    or sorted(str(n) for n in filed_tokens) != sorted(selected)):
                # Allow retry with no explicit token_names (selected from
                # current holdings) to re-drive the filed set even when the
                # owner's other tokens moved: the filed set is authoritative.
                selected = sorted(str(n) for n in filed_tokens) \
                    if isinstance(filed_tokens, list) else selected
            # Publication rebind (R4/R5): a staged reserved intent filed
            # before publication carries the 0.0 unpublished sentinel; rotate
            # it once (single-file rotation, fresh generation, same tokens)
            # to the live mover publication before the first rename, so the
            # transferring record the claim covers names the real publication.
            # ONLY the 0.0 sentinel may rebind: a nonzero bound publication
            # that mismatches the live row is stale and refuses (never adopts
            # old credit under a fresh publication).
            try:
                _bound_pub = float(current.get("published_unix"))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                return {"ok": False, "refusal": "unknown-retain: mover-publication"}
            if _bound_pub != float(live_mover_published):
                if not (str(current.get("state")) == "reserved"
                        and _bound_pub == 0.0):
                    return {"ok": False, "refusal": "mover-publication-mismatch"}
                if not (math.isfinite(float(live_mover_published))
                        and float(live_mover_published) > 0):
                    return {"ok": False, "refusal": "unknown-retain: mover-publication"}
                _rotated = dict(current)
                _rotated["published_unix"] = float(live_mover_published)
                _rotated["generation"] = uuid.uuid4().hex
                _rotated["unix"] = time.time()
                try:
                    self._rotate_output_funding_locked(
                        _rotated, expect_generation=str(
                            current.get("generation")))
                except (OSError, PoolContractError, ValueError) as exc:
                    return {"ok": False, "refusal": f"unknown-retain: {exc}"}
                current = self.read_output_funding(mover_key, tier)
                if (current is None
                        or str(current.get("state")) != "reserved"):
                    return {"ok": False, "refusal": "funding-race-deferred"}
            generation = str(current.get("generation"))
        else:
            generation = uuid.uuid4().hex
            record = {
                "schema": TIER_FUNDING_OUTPUT_SCHEMA_V1,
                "tier_id": tier, "kind": kind,
                "mover_action_key": mover_key, "tokens": sorted(selected),
                "generation": generation, "state": "reserved",
                "unix": time.time(), "published_unix": float(mover_published),
                "owner_action_key": owner_key,
                "owner_nonce": str(binding.get("owner_nonce")),
                "owner_scope_id": str(binding.get("owner_scope_id")),
                "owner_published_unix": float(owner_published),
                "template_id": str(binding.get("template_id")),
                "template_sha256": str(binding.get("template_sha256")),
                "batch_id": str(binding.get("batch_id")),
                "manifest_digest": str(binding.get("manifest_digest")),
                "range_start_bytes": int(binding.get("range_start_bytes")),
                "range_end_bytes": int(binding.get("range_end_bytes")),
            }
            try:
                self._write_output_funding_locked(record, expect_generation=None)
            except (OSError, PoolContractError, ValueError) as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            current = self.read_output_funding(mover_key, tier)
            if current is None:
                return {"ok": False, "refusal": "unknown-retain: funding-unreadable"}
            generation = str(current.get("generation"))
        # Transfer the exact filed set (re-drive safe: already-moved counts).
        filed = self.read_output_funding(mover_key, tier)
        if filed is None:
            return {"ok": False, "refusal": "unknown-retain: funding-unreadable"}
        names = [str(n) for n in filed.get("tokens", [])]  # type: ignore[union-attr]
        try:
            moved = int(ledger.transfer_tokens(owner_key, mover_key, names))
        except (OSError, PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if moved < len(names):
            return {"ok": False, "refusal": "transfer-short",
                    "moved": moved, "expected": len(names),
                    "generation": generation}
        if not self._advance_output_funding_state_locked(
                mover_key, tier, expect="reserved",
                advance_to="transferring", generation=generation):
            # Already transferring with same generation is success (retry
            # after crash between transfer and advance).
            check = self.read_output_funding(mover_key, tier)
            if (check is not None and check.get("state") == "transferring"
                    and str(check.get("generation")) == generation):
                return {"ok": True, "generation": generation,
                        "tokens": names, "moved": moved}
            return {"ok": False, "refusal": "funding-race-deferred",
                    "generation": generation}
        return {"ok": True, "generation": generation,
                "tokens": names, "moved": moved}

    def output_funded_cover(self, tier_id: str, item: Mapping[str, object],
                            kind: str, need: int) -> tuple[int, str | None]:
        """What this output mover's funding record covers, with generation (R1).

        Additive to V1 (claim tries V1 first; this only runs when V1
        covers 0).  Strict or nothing against the sealed mover row itself
        plus recovery-safe authority: output record in ``transferring``
        naming this tier/mover/kind, mover ``published_unix`` equal to the
        sealed row, sealed residency naming the bound manifest over exactly
        the bound range, owner authority via live-owner OR terminal-proof
        (``_output_owner_authority``: live same attempt, else terminal same
        attempt after finish; live-mismatched never falls through), batch
        authority via precommit-OR-commit (``_output_batch_authority``:
        filed commit with same manifest/mover/tier, else durable prewrite
        with same total), and every bound token still held under this key
        with this kind's prefix.  Consumed never covers.  Never raises for
        queue-state reasons; unknown is ``(0, None)``.
        """

        if need <= 0 or not isinstance(item, Mapping):
            return (0, None)
        key = item.get("action_key")
        if not isinstance(key, str):
            return (0, None)
        # Sealed requirement (R4): output cover applies ONLY to movers whose
        # sealed item carries the immutable `produced_output_batch` projection.
        # Legacy movers without it retain existing admission and never scan
        # output history. A sealed requirement with absent/unknown/invalid
        # proof defers via the claim gate below, never fresh acquisition.
        sealed_ref = item.get("produced_output_batch")
        if not isinstance(sealed_ref, Mapping):
            return (0, None)
        if sealed_ref.get("schema") != PRODUCED_OUTPUT_BATCH_REF_SCHEMA_V1:
            return (0, None)
        record = self.read_output_funding(key, str(tier_id))
        if record is None or record.get("state") != "transferring":
            return (0, None)
        # The committed generation/record must bind back to the sealed
        # reference (stable batch identity; generation/publication stay
        # mutable beside it and are checked separately).
        for field in ("batch_id", "manifest_digest", "tier_id",
                      "owner_action_key", "owner_nonce", "owner_scope_id",
                      "template_id", "template_sha256"):
            if str(record.get(field)) != str(sealed_ref.get(field)):
                return (0, None)
        try:
            if (int(sealed_ref.get("range_start_bytes")) != int(  # type: ignore[arg-type]
                    record.get("range_start_bytes"))  # type: ignore[arg-type]
                    or int(sealed_ref.get("range_end_bytes")) != int(  # type: ignore[arg-type]
                        record.get("range_end_bytes"))):  # type: ignore[arg-type]
                return (0, None)
        except (TypeError, ValueError):
            return (0, None)
        if (str(record.get("tier_id")) != str(tier_id)
                or str(record.get("mover_action_key")) != key
                or str(record.get("kind")) != str(kind)):
            return (0, None)
        try:
            row_published = float(item.get("published_unix"))  # type: ignore[arg-type]
            bound_published = float(record.get("published_unix"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return (0, None)
        if row_published != bound_published:
            return (0, None)
        residency = item.get("residency")
        if not isinstance(residency, Mapping):
            return (0, None)
        if (str(residency.get("tier_id")) != str(record.get("tier_id"))
                or str(residency.get("manifest_sha256")) != str(
                    record.get("manifest_digest"))):
            return (0, None)
        try:
            row_range = (int(residency.get("range_start_bytes")),  # type: ignore[arg-type]
                         int(residency.get("range_end_bytes")))  # type: ignore[arg-type]
            bound_range = (int(record.get("range_start_bytes")),  # type: ignore[arg-type]
                           int(record.get("range_end_bytes")))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return (0, None)
        if row_range != bound_range:
            return (0, None)
        # Owner authority: live same attempt OR terminal same attempt (R1).
        # Finish-before-mover-claim recovers via the terminal; a live row
        # for another attempt never falls through to an old terminal.
        try:
            if not self._output_owner_authority(record):
                return (0, None)
        except (OSError, PoolContractError, ValueError):
            return (0, None)
        except Exception:
            return (0, None)
        # Batch authority: filed commit only (R2). An unfinished prewrite is
        # never promoted to a claimable output; drive (transfer remainder)
        # accepts precommit, cover/claim requires the immutable filed batch
        # via the R4 loader.
        try:
            if not self._output_batch_authority(record):
                return (0, None)
        except (OSError, PoolContractError, ValueError):
            return (0, None)
        except Exception:
            return (0, None)
        tokens = record.get("tokens")
        if not isinstance(tokens, list) or not tokens:
            return (0, None)
        try:
            held_names = {path.name for path in _glob(
                self.tier_ledger(str(tier_id)).held_dir / key, "*-*")}
        except (OSError, PoolContractError, ValueError):
            return (0, None)
        if any(not isinstance(name, str)
               or not name.startswith(str(kind) + "-")
               or name not in held_names for name in tokens):
            return (0, None)
        generation = record.get("generation")
        if not isinstance(generation, str):
            return (0, None)
        return (min(int(len(tokens)), int(need)), generation)


    def drive_output_funding(self, mover_action_key: str,
                             tier_id: str) -> dict:
        """Re-drive an output intent to completion (R1, idempotent).

        Reads the filed intent (never infers one), re-validates recovery
        authority (owner live-OR-terminal via ``_output_owner_authority``,
        batch precommit-OR-commit via ``_output_batch_authority``), moves
        the remainder owner->mover, ensures ``transferring``.  Holds owner
        outer + mover inner (same order as fund; non-blocking, defer on
        contention) so a concurrent owner finish serializes instead of
        freeing source-held names mid-transfer.  Works after producer
        finish via terminal proof (finish-before-mover-claim,
        crash-with-partial-transfer).  Short transfer preserves split;
        stale authority refuses without moving.
        """

        try:
            tier = self._check_tier_id(str(tier_id))
        except (PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        mover = str(mover_action_key)
        # Read owner without locks to learn which owner lock to take.
        probe = self.read_output_funding(mover, tier)
        if probe is None:
            return {"ok": False, "refusal": "unknown-batch"}
        if probe.get("state") not in ("reserved", "transferring"):
            return {"ok": False, "refusal": "batch-id-in-use",
                    "state": str(probe.get("state"))}
        try:
            owner_key = str(probe.get("owner_action_key"))
            exp_generation = str(probe.get("generation"))
        except (TypeError, ValueError, KeyError):
            return {"ok": False, "refusal": "unknown-retain: funding-binding"}
        with self._transition_locked(owner_key, blocking=False) as owner_acquired:
            if not owner_acquired:
                return {"ok": False, "refusal": "funding-race-deferred"}
            with self.mover_transition_lock(mover, blocking=False) as mover_acquired:
                if not mover_acquired:
                    return {"ok": False, "refusal": "funding-race-deferred"}
                record = self.read_output_funding(mover, tier)
                if record is None:
                    return {"ok": False, "refusal": "unknown-batch"}
                if str(record.get("generation")) != exp_generation:
                    return {"ok": False, "refusal": "funding-race-deferred"}
                if record.get("state") not in ("reserved", "transferring"):
                    return {"ok": False, "refusal": "batch-id-in-use",
                            "state": str(record.get("state"))}
                # Mover publication binding (R2/R3): transfer requires the mover
                # row to exist (never strand onto an unqueued key). A staged
                # intent filed before publication carries the 0.0 unpublished
                # sentinel; rotate it once (single-file rotation, fresh
                # generation, same tokens) to the real mover publication
                # before the first rename. ONLY the 0.0 sentinel may rebind:
                # a nonzero bound publication that mismatches the live row is
                # stale (republished key adopting old credit) and refuses;
                # reserved-or-not, it never adopts (R3 regression).
                mover_live, mover_refusal = self._output_live_mover(mover)
                if mover_live is None:
                    return {"ok": False, "refusal": mover_refusal}
                try:
                    live_pub = float(mover_live["row"].get("published_unix"))  # type: ignore[union-attr,arg-type]
                    bound_pub = float(record.get("published_unix"))  # type: ignore[arg-type]
                except (TypeError, ValueError, AttributeError):
                    return {"ok": False, "refusal": "unknown-retain: mover-publication"}
                if bound_pub != live_pub:
                    if not (str(record.get("state")) == "reserved"
                            and bound_pub == 0.0):
                        return {"ok": False,
                                "refusal": "mover-publication-mismatch"}
                    if not (math.isfinite(live_pub) and live_pub > 0):
                        return {"ok": False, "refusal": "unknown-retain: mover-publication"}
                    rotated = dict(record)
                    rotated["published_unix"] = live_pub
                    rotated["generation"] = uuid.uuid4().hex
                    rotated["unix"] = time.time()
                    try:
                        self._rotate_output_funding_locked(
                            rotated,
                            expect_generation=str(record.get("generation")))
                    except (OSError, PoolContractError, ValueError) as exc:
                        return {"ok": False, "refusal": f"unknown-retain: {exc}"}
                    record = self.read_output_funding(mover, tier)
                    if (record is None
                            or str(record.get("published_unix")) != str(live_pub)
                            or record.get("state") != "reserved"):
                        return {"ok": False, "refusal": "funding-race-deferred"}
                    exp_generation = str(record.get("generation"))
                if not self._output_owner_authority(record):
                    return {"ok": False, "refusal": "stale-superseded-owner"}
                # Batch authority (R3): live owner accepts precommit-OR-commit
                # (transfer remainder before the filed commit exists); terminal
                # recovery (producer dead) requires the filed commit and never
                # promotes an unfinished prewrite. Determine liveness from the
                # live row directly (owner authority above conflates both).
                try:
                    _live_row = _read_json(self.item_path(
                        CLAIMED, str(record.get("owner_action_key"))))
                except (OSError, PoolContractError):
                    _live_row = None
                _live_matches = False
                if isinstance(_live_row, Mapping):
                    try:
                        _live_matches = (
                            float(_live_row.get("published_unix"))  # type: ignore[arg-type]
                            == float(record.get("owner_published_unix"))  # type: ignore[arg-type]
                            and isinstance(_live_row.get("resource_scope"), Mapping)
                            and str(_live_row["resource_scope"].get("nonce")) == str(record.get("owner_nonce"))  # type: ignore[index]
                            and str((_live_row["resource_scope"].get("scope_id")  # type: ignore[index]
                                     or _live_row["resource_scope"].get("scope_unit")  # type: ignore[index]
                                     or _live_row["resource_scope"].get("unit"))) == str(record.get("owner_scope_id")))  # type: ignore[index]
                    except (TypeError, ValueError, KeyError, AttributeError):
                        _live_matches = False
                if _live_matches:
                    try:
                        batch_ok = bool(self._output_batch_authority(record))
                    except (OSError, PoolContractError, ValueError):
                        batch_ok = False
                    except Exception:
                        batch_ok = False
                    if not batch_ok:
                        try:
                            batch_ok = bool(self._output_precommit_authority(record))
                        except (OSError, PoolContractError, ValueError):
                            batch_ok = False
                        except Exception:
                            batch_ok = False
                else:
                    try:
                        batch_ok = bool(self._output_batch_authority(record))
                    except (OSError, PoolContractError, ValueError):
                        batch_ok = False
                    except Exception:
                        batch_ok = False
                if not batch_ok:
                    return {"ok": False, "refusal": "unknown-retain: batch-authority"}
                names = [str(n) for n in record.get("tokens", [])]  # type: ignore[union-attr]
                if not names:
                    return {"ok": False, "refusal": "unknown-retain: funding-tokens"}
                try:
                    ledger = self.tier_ledger(tier)
                    moved = int(ledger.transfer_tokens(owner_key, mover, names))
                except (OSError, PoolContractError, ValueError) as exc:
                    return {"ok": False, "refusal": f"unknown-retain: {exc}"}
                if moved < len(names):
                    return {"ok": False, "refusal": "transfer-short",
                            "moved": moved, "expected": len(names),
                            "generation": str(record.get("generation"))}
                if record.get("state") == "reserved":
                    if not self._advance_output_funding_state_locked(
                            mover, tier, expect="reserved",
                            advance_to="transferring",
                            generation=str(record.get("generation"))):
                        check = self.read_output_funding(mover, tier)
                        if not (isinstance(check, dict)
                                and check.get("state") == "transferring"
                                and str(check.get("generation")) == str(
                                    record.get("generation"))):
                            return {"ok": False, "refusal": "funding-race-deferred"}
                return {"ok": True, "generation": str(record.get("generation")),
                        "tokens": names, "moved": moved}

    def release_output_funding(self, mover_action_key: str, tier_id: str, *,
                               generation: str | None = None) -> bool:
        """Retire an output intent only when mover nonexecution is proven (R2).

        Holds the mover lock throughout. Refuses (retain, never free) unless
        the mover provably never started: no CLAIMED row, no DONE row, no
        FAILED row, no move receipt with staged bytes, no lease, and no
        claimed terminal carrying this funding generation. Missing
        marker/status is NOT proof a copy never ran (failed/uncertain
        prefixes retain both names and accounting); only the true
        not-started cancellation (published READY, never claimed, no
        receipt/lease/terminal) retires credit once via
        reserved|transferring -> released. Tokens stay where they are;
        ordinary owner/mover release then frees them.
        """

        mover = str(mover_action_key)
        with self.mover_transition_lock(mover, blocking=False) as acquired:
            if not acquired:
                return False
            current = self.read_output_funding(mover, str(tier_id))
            if current is None:
                return False
            if (generation is not None
                    and str(current.get("generation")) != str(generation)):
                return False
            state = str(current.get("state"))
            if state == "released":
                return True
            if state not in ("reserved", "transferring"):
                return False
            # Committed batches are recovery, not cancellation (R7 liveness):
            # once the immutable batch record + commitments entry exist, the
            # credit belongs to that batch's claim (or committed recovery
            # after producer finish), and retiring it here would leave a
            # filed batch whose sealed mover key can never claim or re-fund.
            try:
                if self._output_batch_authority(current):
                    return False
            except (OSError, PoolContractError, ValueError):
                return False
            exp_gen = str(current.get("generation"))
            # Durable claim: CLAIMED row of any shape means the mover may hold
            # the fence while the consumed marker failed.
            try:
                claimed = _read_json(self.item_path(CLAIMED, mover))
            except (OSError, PoolContractError):
                return False
            if isinstance(claimed, Mapping):
                return False
            # Terminals: DONE or FAILED of any status means the mover started
            # (or a successor did); a funded generation carried into the
            # terminal proves it took this fence. Uncertain/missing terminal
            # reads fail closed (retain).
            for terminal_state in (DONE, FAILED):
                try:
                    terminal = _read_json(self.item_path(terminal_state, mover))
                except (OSError, PoolContractError):
                    return False
                if not isinstance(terminal, Mapping):
                    continue
                # Any terminal for this key is proof of execution start.
                return False
            # Physical lifetime: move receipt with any staged bytes (complete
            # or partial, refused or not) means bytes may be on the stage.
            try:
                receipt = self.move_record(mover)
            except (OSError, PoolContractError, ValueError):
                return False
            if isinstance(receipt, Mapping):
                try:
                    staged = receipt.get("bytes_staged")
                    if isinstance(staged, int) and not isinstance(staged, bool) and staged > 0:
                        return False
                    if receipt.get("complete") is True:
                        return False
                except (TypeError, ValueError):
                    return False
            # Lease: a live lease file means the mover may still hold the key.
            try:
                lease = _read_json(self.lease_path(mover))
            except (OSError, PoolContractError):
                return False
            if isinstance(lease, Mapping):
                return False
            _ = exp_gen
            return self._advance_output_funding_state_locked(
                mover, str(tier_id), expect=state, advance_to="released",
                generation=str(current.get("generation")))

    def output_census_for_owner(self, owner_key: str) -> tuple[list[dict], bool]:
        """Outstanding output intents for owner + census-unknown flag (R2).

        Fail-retain semantics (per #741): a proven missing namespace (no
        `TIER_FUNDING` dir, or dir enumerates cleanly with no intent for
        this owner) is empty (`unknown=False`); any unreadable/corrupt step
        that could hide this owner's partially transferred intent is UNKNOWN
        (`unknown=True`) and the caller must NOT free potentially covered
        tier tokens (retain all held for reaper retry, preserving
        attribution including partial transfers).

        Unknown when: funding dir unreadable; any `*.output-funding.json`
        file unreadable; any such file unparsable as JSON; any parsed
        Mapping with matching `owner_action_key` (or unreadable owner field)
        that fails `validate_output_funding` (corrupt binding that may own
        source-held names). Files for other owners that parse cleanly (or
        fail with a proven different owner) do not taint this census.
        """

        intents: list[dict] = []
        funding_dir = self.root / TIER_FUNDING
        try:
            entries = os.scandir(funding_dir)
        except FileNotFoundError:
            return ([], False)
        except NotADirectoryError:
            return ([], True)
        except OSError:
            return ([], True)
        with entries:
            try:
                names = sorted(entry.name for entry in entries
                               if entry.name.endswith(".output-funding.json"))
            except OSError:
                return ([], True)
        unknown = False
        for name in names:
            path = funding_dir / name
            try:
                raw = _read_json(path)
            except (OSError, PoolContractError):
                unknown = True
                continue
            if raw is None:
                # Missing file raced with glob: not proven for/against;
                # treat as unknown only if the name could be ours? Name is
                # {mover}.{tier}.output-funding.json (mover unknown here),
                # so any vanishing file taints (conservative, rare).
                unknown = True
                continue
            if not isinstance(raw, Mapping):
                # Non-object file: could it be ours? Owner field unreadable
                # => unknown (fail closed).
                unknown = True
                continue
            try:
                owner_field = raw.get("owner_action_key")
            except (AttributeError, ValueError):
                unknown = True
                continue
            if not isinstance(owner_field, str) or owner_field != str(owner_key):
                # Proven other owner (or missing owner field on a Mapping?
                # Missing owner field => cannot prove other => unknown).
                if not isinstance(owner_field, str):
                    unknown = True
                continue
            try:
                record = self.validate_output_funding(raw)
            except (PoolContractError, ValueError):
                # Corrupt binding that names this owner: may own
                # source-held names => unknown, retain.
                unknown = True
                continue
            if str(record.get("state")) in ("reserved", "transferring"):
                intents.append(record)
        return (intents, unknown)

    def _output_spoken_token_names(
            self, owner_key: str, tier_id: str) -> tuple[set[str], bool]:
        """Token names already promised to this owner's outstanding intents.

        R7 liveness for sequential per-batch funding: one name must never be
        filed into two live intents of the same owner on the same tier,
        because whichever intent funds first removes the name from the owner
        and strands the other in a permanent transfer-short. The set is the
        union of ``tokens`` over every ``reserved``/``transferring`` census
        record of this owner on this tier. Fail-retain: a census that cannot
        prove the set returns ``(set(), True)`` and the caller refuses.
        """

        intents, unknown = self.output_census_for_owner(str(owner_key))
        if unknown:
            return (set(), True)
        spoken: set[str] = set()
        for record in intents:
            if str(record.get("tier_id")) != str(tier_id):
                continue
            tokens = record.get("tokens")
            if not isinstance(tokens, list):
                return (set(), True)
            for name in tokens:
                if not isinstance(name, str):
                    return (set(), True)
                spoken.add(name)
        return (spoken, False)

    def _output_outstanding_window_tokens(
            self, owner_key: str, tier_id: str, kind: str
    ) -> tuple[int, bool]:
        """Window tokens this owner has live OUTSIDE its own holdings.

        R7 lifecycle accounting for the bounded refill: reserved/transferring
        names no longer held by the owner (already transferred toward their
        movers) plus, for every ``consumed`` record, the tokens its mover
        STILL holds -- a claimed-but-unretired batch's fence and staged bytes
        are live spending of the same aggregate window, right up to the
        retirement/egress that releases them. ``released`` records are proven
        retired and count nothing; any unreadable/corrupt funding file that
        may belong to this owner returns ``(count, True)`` so the caller
        retains. Owner-held reserved names are deliberately NOT counted
        here: the caller already counts them as holdings.
        """

        try:
            ledger = self.tier_ledger(str(tier_id))
            held_names = {path.name for path in _glob(
                ledger.held_dir / str(owner_key), "*-*")}
        except (OSError, PoolContractError, ValueError):
            return (0, True)
        spoken, unknown = self.output_census_for_owner(str(owner_key))
        if unknown:
            return (0, True)
        outstanding = 0
        for record in spoken:
            if str(record.get("tier_id")) != str(tier_id):
                continue
            tokens = record.get("tokens")
            if not isinstance(tokens, list):
                return (0, True)
            for name in tokens:
                if (not isinstance(name, str) or not name.startswith(
                        str(kind) + "-")):
                    return (0, True)
                if name not in held_names:
                    outstanding += 1
        # Consumed-but-unretired: the mover's live holdings are the batch's
        # remaining share of the window until retirement releases them.
        funding_dir = self.root / TIER_FUNDING
        try:
            names = sorted(entry.name for entry in os.scandir(funding_dir)
                           if entry.name.endswith(".output-funding.json"))
        except FileNotFoundError:
            names = []
        except OSError:
            return (outstanding, True)
        for name in names:
            path = funding_dir / name
            # The file name is f"{mover}.{tier}.output-funding.json"; parse
            # the mover from the stem rather than guessing keys.
            try:
                stem = path.name[: -len(".output-funding.json")]
                mover, _, tier_part = stem.rpartition(".")
                if tier_part != str(tier_id):
                    continue
                record, file_state = self.output_funding_file_state(
                    mover, str(tier_id))
            except (OSError, PoolContractError, ValueError):
                return (outstanding, True)
            if file_state == "corrupt":
                return (outstanding, True)
            if record is None:
                continue
            if str(record.get("owner_action_key")) != str(owner_key):
                continue
            state = str(record.get("state"))
            if state == "released":
                continue  # proven retired; counts nothing
            if state == "consumed":
                tokens = record.get("tokens")
                if not isinstance(tokens, list):
                    return (outstanding, True)
                try:
                    mover_held = int(ledger.holder_tokens(
                        str(record.get("mover_action_key"))).get(kind, 0))
                except (OSError, PoolContractError, ValueError):
                    return (outstanding, True)
                live = min(len(tokens), mover_held)
                if live > 0:
                    outstanding += live
        return (outstanding, False)

    def output_intents_sourcing_from(self, owner_key: str) -> list[dict]:
        """Outstanding output intents whose source window is this owner.

        Deprecated wrapper (fail-open on unknown); prefer
        `output_census_for_owner` + `output_keep_names_for_owner` which
        propagate UNKNOWN. Kept for diagnostics only; finish/reaper paths
        must use the keep-names helper below, never this list alone.
        """

        intents, _ = self.output_census_for_owner(str(owner_key))
        return intents

    def output_keep_names_for_owner(
            self, owner_key: str, tier_id: str) -> tuple[set[str], bool]:
        """(keep_set, unknown) for owner finish (R2 fail-retain + R3 scandir).

        Holdings enumerated with explicit `os.scandir` classification: proven
        ENOENT/NotADirectory (no holder dir) is empty; any other read failure
        is UNKNOWN (retain all). Census UNKNOWN likewise retains all.
        """

        try:
            holder_dir = self.tier_ledger(str(tier_id)).held_dir / str(owner_key)
            try:
                with os.scandir(holder_dir) as entries:
                    held = {entry.name for entry in entries}
            except FileNotFoundError:
                held = set()
            except NotADirectoryError:
                return (set(), True)
            except OSError:
                return (set(), True)
        except (OSError, PoolContractError, ValueError):
            return (set(), True)
        intents, unknown = self.output_census_for_owner(str(owner_key))
        if unknown:
            return (set(held), True)
        keep: set[str] = set()
        for record in intents:
            if str(record.get("tier_id")) != str(tier_id):
                continue
            tokens = record.get("tokens")
            if not isinstance(tokens, list):
                return (set(held), True)
            for name in tokens:
                if not isinstance(name, str):
                    return (set(held), True)
                if name in held:
                    keep.add(name)
        return (keep, False)

    def validate_output_mover_publishable(
            self, *, instance, template, batch_id: str,
            descriptors: list[Mapping[str, object]],
            mover_key: str, tier_id: str,
            residency: Mapping[str, object]) -> dict:
        """Writer-side publication check (R3, no publish edit).

        744 calls this BEFORE publishing a mover READY row; it rejects a
        contradictory/missing declared reference before exposure using
        existing validators only: bound precommit (live owner + prewrite +
        descriptors + manifest via `describe_output_precommit_for_funding`),
        staged output intent exists for (mover, tier) naming the same
        batch/manifest (reserved, any publication incl. 0.0 sentinel), and
        sealed residency matching the precommit manifest/tier/range.
        Returns {"ok": True, ...} or typed refusal; never raises for
        queue-state reasons.
        """

        try:
            tier = self._check_tier_id(str(tier_id))
        except (PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        try:
            from . import produced_output as produced_mod
            binding = produced_mod.describe_output_precommit_for_funding(
                self, instance, template, batch_id, descriptors,
                tier, str(mover_key))
        except produced_mod.ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        except (OSError, PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        except Exception as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        record = self.read_output_funding(str(mover_key), tier)
        if record is None:
            # Distinguish absent file (missing staged intent => refuse) from
            # corrupt file (unknown => refuse); both refuse publication, with
            # different reasons for diagnostics.
            _, file_state = self.output_funding_file_state(
                str(mover_key), tier)
            if file_state == "absent":
                return {"ok": False, "refusal": "output-funding-missing"}
            return {"ok": False, "refusal": "unknown-retain: funding-unreadable"}
        if str(record.get("state")) not in ("reserved", "transferring"):
            return {"ok": False, "refusal": "output-funding-not-pending"}
        if (str(record.get("batch_id")) != str(binding.get("batch_id"))
                or str(record.get("manifest_digest")) != str(
                    binding.get("manifest_digest"))):
            return {"ok": False, "refusal": "mover-publication-mismatch"}
        if not isinstance(residency, Mapping):
            return {"ok": False, "refusal": "mover-publication-mismatch"}
        try:
            if (str(residency.get("tier_id")) != tier
                    or str(residency.get("manifest_sha256")) != str(
                        binding.get("manifest_digest"))
                    or int(residency.get("range_start_bytes")) != int(  # type: ignore[arg-type]
                        binding.get("range_start_bytes"))
                    or int(residency.get("range_end_bytes")) != int(  # type: ignore[arg-type]
                        binding.get("range_end_bytes"))):
                return {"ok": False, "refusal": "mover-publication-mismatch"}
        except (TypeError, ValueError):
            return {"ok": False, "refusal": "mover-publication-mismatch"}
        return {"ok": True, "batch_id": str(binding.get("batch_id")),
                "manifest_digest": str(binding.get("manifest_digest")),
                "generation": str(record.get("generation")),
                "state": str(record.get("state"))}

    def mover_transition_lock(self, mover_action_key: str, *,
                              blocking: bool = True):
        """Exclude two parties from deciding one staged range's ownership.

        An egress deletes a mover's files and then releases its key; an
        adoption hands the same key's tokens to a successor and then drops its
        fragment.  Each is safe alone and neither ordering of the two is safe
        against the other: an egress that reads the fragment before the
        adoption and unlinks after it deletes bytes a live consumer now holds
        tokens for, and one that reads it after would release tokens for bytes
        that are still there.  Ordering cannot fix that, so the two exclude
        each other on the mover's own key -- the same lock that serializes a
        key's other ownership transitions, taken on the *mover*, because the
        egress row's key is a different action.

        The egress waits; an adoption that cannot take the lock declines and
        the range is copied instead, which costs time and never correctness.
        """

        return self._transition_locked(str(mover_action_key), blocking=blocking)

    def staged_range_of(self, mover_action_key: str) -> dict[str, object] | None:
        """The range one mover has on a tier now, or ``None`` if it has none.

        Read off the mover's own receipt, which is the only document that says
        what a copy actually achieved, and only when that receipt records a
        complete, unrefused copy.  The four fields are exactly the ones
        ``core.residency_descriptor`` binds, so two movers whose answers are
        equal have made the same bytes of the same manifest resident on the
        same tier however they were sealed -- which is what lets a later
        consumer's phase recognise its own range in somebody else's copy.
        """

        receipt = self.move_record(str(mover_action_key))
        if not isinstance(receipt, Mapping) or receipt.get("refusal"):
            return None
        if receipt.get("complete") is not True:
            return None
        tier_id = receipt.get("tier_id")
        digest = receipt.get("manifest_sha256")
        start = receipt.get("range_start_bytes")
        end = receipt.get("range_end_bytes")
        if not isinstance(tier_id, str) or not tier_id:
            return None
        if not isinstance(digest, str) or len(digest) != 64:
            return None
        for value in (start, end):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                return None
        assert isinstance(start, int) and isinstance(end, int)
        if end <= start:
            return None
        return {"tier_id": tier_id, "manifest_sha256": digest,
                "range_start_bytes": start, "range_end_bytes": end}

    def residency_pin_holds(self, record: Mapping[str, object] | None,
                            action_key: str) -> bool:
        """Does this concluding mover keep its tier tokens past ``finish``?

        The invariant the whole tier reservation rests on is that **held tier
        tokens equal bytes on the stage**, at every instant.  Releasing at
        ``finish`` bounds concurrent copies instead: twenty-one movers of 34.4
        GB each, run one after another, leave 722 GB on a 721 GB stage while
        the ledger reads its full supply free at every step, and the twenty-
        second is admitted on tokens for bytes that will ENOSPC.  So a mover
        that staged what it declared keeps its tokens, and an egress node
        returns them when it deletes the files.

        Everything else releases, because everything else left nothing behind:
        a failure, a timeout, a reaped lease, a withdrawal, a ``cache_hit``
        that moved no bytes, a mover whose receipt is missing, and a mover that
        staged fewer bytes than it declared or refused for an overrun.  The
        receipt is the evidence and the range is the test; a status alone
        cannot distinguish a mover that copied 34 GB from one that copied none,
        because both end ``executed``.

        "Left nothing behind" is true of the *range*, not of the bytes: a
        mover that staged half its batch left half a batch on the stage.
        Where those leftovers have a named owner to return the tokens -- a
        produced-output batch, whose egress runs for this mover key --
        :meth:`output_partial_pin_holds` keeps them charged, and
        :meth:`pin_holds_tier_tokens` is the union every concluding path
        asks.  This predicate keeps answering the complete case alone.
        """

        if not isinstance(record, Mapping):
            return False
        residency = record.get("residency")
        if not isinstance(residency, Mapping):
            return False
        tier_id = residency.get("tier_id")
        start = residency.get("range_start_bytes")
        end = residency.get("range_end_bytes")
        if not isinstance(tier_id, str) or not isinstance(start, int) or not isinstance(end, int):
            return False
        if record.get("status") != "executed":
            return False
        receipt = self.move_record(str(action_key))
        if not isinstance(receipt, Mapping) or receipt.get("refusal"):
            return False
        if receipt.get("tier_id") != tier_id or receipt.get("complete") is not True:
            return False
        staged = receipt.get("bytes_staged")
        return isinstance(staged, int) and staged == end - start

    def output_partial_pin_holds(self, record: Mapping[str, object] | None,
                                 action_key: str) -> bool:
        """Does a PRODUCED-OUTPUT mover keep tokens for a partial stage?

        :meth:`residency_pin_holds` answers the complete case and releases
        everything else, "because everything else left nothing behind".  A
        mover that copied half its batch and then failed left plenty behind,
        and measurement says so: 700 of 1200 declared bytes on the stage, the
        mover's tokens back in ``free`` at ``finish``, the owner's bounded
        refill then re-acquiring against occupancy no holder is charged for --
        exactly the attribution loss :meth:`_release_reservation` warns about.

        Retention is only safe where the leftovers have a named owner that
        will return the tokens, and a produced-output batch has one:
        ``produced_output.retire_batch`` runs the egress for this very mover
        key and ``stage_release.evict`` frees its holder when the files go.
        So the charge is retained until the bytes are gone, and this stays
        scoped to that lane by the funding record's own existence.  The
        consumer window's twin (#627 -- the same partial bytes, held by
        nobody) has no such owner and keeps the tier loop's eviction-candidate
        sweep instead; nothing here changes it.

        Three states, not two: **occupied** retains, **proven empty**
        releases, and **unknown** retains.  ``stage_move`` renames each
        destination into place (:1247, or :421 when the published-readiness
        publisher decides it), publishes a residency fragment for it (~:1324),
        and files its move receipt once and last (:1965).  Bytes
        therefore exist before either record does, so BOTH records can be
        missing while the stage is occupied, and neither absence is a report
        of zero.

        Releasing requires a POSITIVE report of emptiness and agreement from
        publication -- a conjunction, not a fallback.  Every way this
        function can answer "no" is one of exactly two kinds, and they are
        listed here because each one has to be classified separately:

        SCOPE gates (this is not a produced-output tier reservation to
        judge, so ``residency_pin_holds`` decides it alone, exactly as
        before) -- no claim record, no ``residency`` block, no ``tier_id``
        in it, or a PROVEN-ENOENT funding file.  None of these is a claim
        about the stage.

        EMPTINESS, which needs both halves: the mover's own receipt names
        this tier and reports ``bytes_staged`` as EXACT non-boolean integer
        zero, AND :meth:`_output_published_material` proves no fragment.  An
        overrun reports bytes above its declaration, so it never qualifies;
        a receipt about another tier says nothing about this one; a missing
        receipt, a negative count, a ``bool`` or any other shape is
        malformed metadata that proves nothing; and any unreadable probe
        answers unknown.  All of those retain.
        """

        # SCOPE gates: without a claim record carrying a tier residency
        # block there is no produced-output tier reservation for this
        # predicate to extend, and ``residency_pin_holds`` has already
        # judged it.  These are not statements that the stage is empty.
        if not isinstance(record, Mapping):
            return False
        residency = record.get("residency")
        if not isinstance(residency, Mapping):
            return False
        tier_id = residency.get("tier_id")
        if not isinstance(tier_id, str) or not tier_id:
            return False
        try:
            funding, file_state = self.output_funding_file_state(
                str(action_key), tier_id)
        except (OSError, PoolContractError, ValueError):
            return True
        if file_state == "absent":
            # A prepaid-output intent is PROVEN never to have been filed, so
            # this is another lane's mover: judged by ``residency_pin_holds``
            # alone, exactly as before.  An unreadable intent is not that
            # proof, and falls through to the occupancy question below.
            return False
        del funding
        try:
            receipt = self.move_record(str(action_key))
        except (OSError, PoolContractError):
            return True
        if not isinstance(receipt, Mapping):
            # No report at all.  The bytes land before the receipt is filed,
            # so this is the crash window, not a statement about the stage.
            return True
        if receipt.get("tier_id") != tier_id:
            # A report about some other tier proves nothing about this one.
            return True
        staged = receipt.get("bytes_staged")
        if not (isinstance(staged, int) and not isinstance(staged, bool)
                and staged == 0):
            # EXACT non-boolean integer zero is the only report of emptiness.
            # A positive count is occupancy (an overrun lands here: it
            # refused for staging MORE than it declared, and its bytes are
            # on the stage).  A negative count, a ``bool`` -- for which
            # ``isinstance(x, int)`` is True and ``False > 0`` is False, the
            # trap this file already avoids at :7570 on this same field --
            # and any other shape are malformed metadata, which proves
            # nothing and therefore retains.
            return True
        # The mover's own count says nothing landed on this tier -- the only
        # receipt that can prove emptiness.  It still has to agree with
        # publication: proven-no-fragment releases, anything else retains.
        return self._output_published_material(str(action_key)) is not False

    def _output_published_material(self, action_key: str) -> bool | None:
        """Does any produced-output fragment name this mover? (3-valued)

        ``True`` a fragment names it, ``False`` proven none, ``None``
        unknown.  Fragments are the publication evidence that exists BEFORE
        the move receipt does, which is what makes the crash window between
        them answerable at all.  Every read failure that is not a proven
        ENOENT answers unknown, so a namespace that cannot be scanned never
        becomes a statement that nothing was published there.
        """

        from . import produced_output as produced_mod

        root = produced_mod.output_fragment_root(self.root / RESIDENCY)
        name = f"{action_key}.json"
        try:
            namespaces = list(os.scandir(root))
        except FileNotFoundError:
            return False
        except OSError:
            return None
        for entry in namespaces:
            try:
                if not entry.is_dir():
                    continue
                os.stat(Path(entry.path) / name)
            except FileNotFoundError:
                continue
            except OSError:
                return None
            return True
        return False

    def pin_holds_tier_tokens(self, record: Mapping[str, object] | None,
                              action_key: str) -> bool:
        """Either reason a concluding claim keeps its tier tokens.

        One predicate for every path that concludes a claim, so a mover
        cannot be judged complete-or-nothing by one caller and partial by
        another.
        """

        return (self.residency_pin_holds(record, action_key)
                or self.output_partial_pin_holds(record, action_key))

    def _filed_pin_holds(self, action_key: str) -> bool:
        """Does this key's already-filed ending still pin bytes on the stage?

        For the cleanup paths that have no claim record to judge -- a finish
        tombstone whose finisher died, a lease widowed by a record that is
        gone.  They conclude a claim whose ending is already filed, so the
        question is not "did this claim stage anything" but "does the ending
        that *was* filed hold tokens for bytes that are there".  Contained:
        a read failure answers ``True``, because a cleanup that cannot see the
        ending must not be the thing that releases its capacity.
        """

        seen = False
        for state in (DONE, FAILED):
            try:
                record = _read_json(self.item_path(state, str(action_key)))
            except (OSError, PoolContractError):
                return True
            if isinstance(record, Mapping):
                seen = True
                if self.pin_holds_tier_tokens(record, str(action_key)):
                    return True
        if seen:
            # An ending was found and judged: it does not pin.
            return False
        # NO ending at all is the strongest form of "cannot see the ending",
        # not proof that nothing is pinned.  A box that died mid-copy files
        # no terminal, and its bytes are on the stage; releasing here is the
        # same free-on-absence this predicate family has been wrong about
        # three times already.  Scoped by positive evidence that this key is
        # a funded produced-output mover whose fence has not been retired.
        return self._output_funding_unretired(str(action_key))

    def _output_funding_unretired(self, action_key: str) -> bool:
        """Does a prepaid-output fence for this key exist and still stand?

        ``False`` only on a PROVEN-absent funding directory or entry, or a
        record proven ``released`` (its egress already ran and returned the
        tokens).  Any record in another state, and any read failure, answers
        ``True``: a cleanup that cannot establish the fence is retired must
        not be the thing that frees it.  Keys with no produced-output
        funding at all -- every consumer-window mover -- answer ``False``
        and are swept exactly as before.
        """

        funding_dir = self.root / TIER_FUNDING
        suffix = ".output-funding.json"
        prefix = f"{action_key}."
        try:
            names = [entry.name for entry in os.scandir(funding_dir)
                     if entry.name.startswith(prefix)
                     and entry.name.endswith(suffix)]
        except FileNotFoundError:
            return False
        except OSError:
            return True
        for name in names:
            tier_id = name[len(prefix):-len(suffix)]
            if not tier_id:
                return True
            try:
                record, file_state = self.output_funding_file_state(
                    str(action_key), tier_id)
            except (OSError, PoolContractError, ValueError):
                return True
            if file_state != "ok" or not isinstance(record, Mapping):
                return True
            if str(record.get("state")) != "released":
                return True
        return False

    def _release_reservation(self, action_key: str, *, host: str | None,
                             keep_tier: bool = False) -> int:
        """Give a concluded claim's capacity back: host tokens, then tier tokens.

        Every path that concludes a claim -- finish, the reapers, withdrawal,
        the lost-race branches of ``_claim`` -- goes through here, so a
        claim that reserved on a tier cannot be concluded on one ledger and
        forgotten on the other.  ``host`` is the box whose ledger holds the
        claim's host tokens, or ``None`` when no box is named and there is
        nothing to release there; the tier release needs no host.

        **One default is not safe for every caller, and this docstring used to
        imply it was.**  ``keep_tier`` is off by default because a claim that
        is *being* concluded has released nothing yet, and almost every caller
        here is concluding one.  Three are not: they clean up after an ending
        that already concluded, and that ending may have kept its tier tokens
        on purpose because its bytes are on the stage.

        * ``reap_stale``'s terminal-claim branch judges ``keep_tier`` on the
          filed ``done``/``failed`` record its generation match returned.
        * ``sweep_finish_tombstones`` and ``sweep_widowed_leases`` have no
          claim record to judge, so they ask ``_filed_pin_holds``.

        Getting this wrong does not lose a token; it loses the *attribution*.
        Every path that reclaims stage capacity -- an egress node, the tier
        loop's orphan sweep -- walks the tier ledger's held keys, so a key
        released while its files are still there leaves occupancy that nothing
        can ever charge to anyone: the ledger reads it free, the next mover is
        admitted against capacity already spent, and the stage ENOSPCs.  The
        window does eventually republish that mover -- ``_mover_state`` reads an
        unpinned, unqueued mover as unpublished -- so the consumer's gate is
        not stuck forever, but it pays a second full copy of the range and the
        over-admission happens first.
        """

        released = self.ledger(host).release(action_key) if host is not None else 0
        if keep_tier:
            # The host tokens go -- the box is free for other work the instant
            # the copy stops -- and the tier tokens that price *occupancy* stay,
            # because the bytes did.  The ones that price a *rate* do not: the
            # copy has stopped, so the pool-side bandwidth it reserved is being
            # drawn by nobody, and a finished mover holding it refuses the next
            # one against a supply that is idle (#636).
            return released + self.release_tier_rate_reservations(action_key)
        # Prepaid-output protection (R1+R2): an owner finish/reaper must not
        # free an outstanding output intent's still-source-held tokens between
        # partial transfers, yet a cancelled producer must not strand its
        # unspent grant.  Hold the owner transition lock (blocking; nested
        # re-entry safe, reapers already hold it) around the tier release so
        # fund's owner-outer/mover-inner critical section serializes.  Census
        # is fail-retain (R2 #741 semantics): proven-empty frees unspent
        # grant; UNKNOWN census (unreadable/corrupt intent that may own
        # source-held names, including partial transfers) retains ALL held
        # tier tokens for reaper retry, preserving attribution.
        with self._transition_locked(str(action_key), blocking=True):
            tier_released = 0
            for tier_id in self.tier_ids():
                try:
                    ledger = self.tier_ledger(tier_id)
                except (OSError, PoolContractError):
                    continue
                try:
                    keep, unknown = self.output_keep_names_for_owner(
                        str(action_key), tier_id)
                except (OSError, PoolContractError, ValueError):
                    continue
                try:
                    if unknown:
                        continue
                    if keep:
                        tier_released += ledger.release_except(
                            str(action_key), keep)
                    else:
                        tier_released += ledger.release(str(action_key))
                except (OSError, PoolContractError, ValueError):
                    continue
            return released + tier_released

    def mint_tier_capacity(self, tier_id: str, tokens: Mapping[str, int]) -> dict[str, object]:
        """Make a tier's ledger say what discovery measured, up or down.

        Up is ``ensure_capacity``; down is ``retire_free_capacity``, which
        deletes free tokens only, so a mover mid-flight keeps its
        reservation and the total falls as holders finish.  A kind that
        discovery no longer reports is retired to zero: a stage pool that
        was exported and is gone must stop admitting movers.

        Under the tier's mint lock, because the two halves are only safe in
        one minter's hands: the marker analysis ``ensure_capacity`` rests on
        assumes no second minter adopts and mints concurrently, and a retire
        racing another minter's ensure could take a token the other just
        freed for re-adoption.  The lock is per tier, so the supervised loop
        and an operator ``--once`` run serialize only against each other.
        """

        with self.tier_mint_lock(tier_id):
            ledger = self.tier_ledger(tier_id)
            wanted = {str(kind): int(count) for kind, count in tokens.items()}
            if any(count < 0 for count in wanted.values()):
                raise PoolContractError("tier capacity must not be negative")
            return self._apply_tier_capacity(tier_id, ledger, wanted)

    def _apply_tier_capacity(
        self, tier_id: str, ledger: ResourceLedger, wanted: dict[str, int],
    ) -> dict[str, object]:
        """Grow then shrink one tier ledger to ``wanted``, sans lock.

        The body of :meth:`mint_tier_capacity` once inside the tier mint
        lock.  Factored so a caller that must snapshot the wanted number
        under the SAME lock (the tier loop's landed count, which an egress
        decharge may change between a read and a mint) can do so without a
        second minter interleaving; see :meth:`mint_tier_capacity_guarded`.
        Never call without holding :meth:`tier_mint_lock` for the tier:
        the sequence (reclaim scan, ensure, retire) is atomic only under
        it.  The ledger calls below re-acquire the same lock through
        their mutation guard (same-thread nesting is safe); that
        re-entrancy is what keeps direct ledger users serialized too.
        """

        reclaimed = self._reclaim_dead_markers(ledger, wanted)
        ledger.ensure_capacity({kind: count for kind, count in wanted.items() if count > 0})
        total = ledger.capacity()
        lower = {kind: wanted.get(kind, 0) for kind in total if total[kind] > wanted.get(kind, 0)}
        retired = ledger.retire_free_capacity(lower) if lower else {}
        return {"tier_id": tier_id, "capacity": ledger.capacity(),
                "retired": retired, "reclaimed": reclaimed}

    def _reclaim_dead_markers(
        self, ledger: ResourceLedger, wanted: Mapping[str, int],
    ) -> dict[str, int]:
        """Reissue destroyed names the wanted number has headroom for (#733 R5).

        The authority is the durable dead set (``minted/dead/``): each entry
        IS the destroyed token file itself, renamed there atomically by
        :meth:`ResourceLedger.retire_held`, so no scan infers absence and
        no unlink-then-record gap exists.  Reissue is one atomic rename
        back to free; the original marker was never touched, so the
        reissued token is immediately consistent -- no ensure pass needed,
        no transient unmarked state.  Only names with no live token
        anywhere are reissued, up to per-kind ``wanted - live`` headroom;
        a name live in free is deduped (its dead file removed, nothing
        counted), a name live in held drops its stale dead file and stays
        charged.  Dead beyond headroom wait for honest growth.

        Exclusion, stated precisely (#733 R6): the headroom gate counts
        live tokens with two directory listings, and a token renamed
        held/private -> free between the free scan and the holder scan
        would be missed by both, overstating headroom and reissuing a
        dead name with no backing -- a phantom a claimant could then
        take before the same apply's retire trims it.  That interleaving
        is closed by the ledger's mutation guard: this body runs inside
        the tier mint lock (see :meth:`_apply_tier_capacity`), and every
        token rename through a factory-built tier ledger takes the same
        lock, so no such rename lands between these two listings.  The
        same apply's retire then trims only genuine excess, and no
        prefix of the apply exposes unbacked free credit.  The live
        census itself is error-visible on a tier ledger: an unreadable
        free or holder directory aborts the reclaim with the dead set
        retained, so a hidden live holder can never read as headroom.

        Deployment: the exclusion holds only among workers carrying the
        guard.  Mixed-version operation -- a worker or storage role on a
        generation without ``_guarded_mutation`` admitting, releasing, or
        minting on the same tier -- can still land the rename between
        the scans and overissue with no bounded-overshoot allowance.
        Deploying this generation requires a quiescent queue and reader
        state with no new tier-admitted workloads until worker AND
        storage roles converge on the guarded generation (root reviews
        the actual publication).
        """

        reclaimed: dict[str, int] = {}
        dead_dir = ledger.minted_dir / "dead"
        try:
            names = sorted(path.name for path in ledger._census_scan(dead_dir)
                           if path.is_file())
        except OSError:
            # Unknown dead set: retain everything, decide nothing.
            return reclaimed
        if not names:
            return reclaimed
        try:
            live: set[str] = set()
            for path in ledger._census_glob(ledger.free_dir, "*-*"):
                live.add(path.name)
            for holder in ledger._census_scan(ledger.held_dir):
                if holder.is_dir():
                    live.update(
                        path.name
                        for path in ledger._census_glob(holder, "*-*"))
        except OSError:
            # Unknown live set: a hidden holder would read as empty and
            # overstate headroom, so reissue nothing and retain the dead
            # set for the next cycle.  On a tier ledger the census above
            # is error-visible; host readers keep their tolerant scans.
            return reclaimed
        try:
            live_count: dict[str, int] = {}
            for name in live:
                kind = name.rsplit("-", 1)[0]
                live_count[kind] = live_count.get(kind, 0) + 1
        except (AttributeError, TypeError):
            return reclaimed
        for name in names:
            kind, _, _ = name.rpartition("-")
            if not kind or kind not in wanted:
                continue
            if name in live:
                # Live again (a rename the destroy raced, or a duplicate
                # aftermath): the slot needs no reissue, just convergence.
                # Free keeps its token; held keeps its charge; either way
                # the dead file goes and nothing is counted.
                try:
                    (dead_dir / name).unlink()
                except OSError:
                    pass
                continue
            if live_count.get(kind, 0) >= int(wanted[kind]):
                continue
            try:
                os.rename(dead_dir / name, ledger.free_dir / name)
            except OSError:
                continue
            live_count[kind] = live_count.get(kind, 0) + 1
            live.add(name)
            reclaimed[kind] = reclaimed.get(kind, 0) + 1
        return reclaimed

    def mint_tier_capacity_guarded(self, tier_id: str, wanted_fn) -> dict[str, object]:
        """Mint one tier's capacity to a number snapshotted under the lock.

        ``wanted_fn(ledger)`` is called holding the tier mint lock and must
        return the wanted ``{kind: count}`` mapping; the ensure+retire then
        applies before the lock is released.  This closes the read-then-mint
        race a shared-egress decharge would otherwise lose to: a landed
        count read before the egress and minted after it would reintroduce
        the very credits the egress just decharged.  Callers that need no
        snapshot use :meth:`mint_tier_capacity`.
        """

        with self.tier_mint_lock(tier_id):
            ledger = self.tier_ledger(tier_id)
            wanted = wanted_fn(ledger)
            wanted = {str(kind): int(count) for kind, count in dict(wanted).items()}
            if any(count < 0 for count in wanted.values()):
                raise PoolContractError("tier capacity must not be negative")
            return self._apply_tier_capacity(tier_id, ledger, wanted)

    def release_tier_holder_for_egress(
        self, tier_id: str, action_key: str, *,
        destroy: Mapping[str, int], free: Mapping[str, int],
    ) -> dict[str, object]:
        """Settle one egressing mover's tier hold: decharge, then free.

        ``destroy`` names per-kind token counts whose bytes stay on the
        stage under a co-owner (the shared-egress duplicate): they are
        destroyed first via :meth:`ResourceLedger.retire_held`, and only
        then are up to ``free`` per-kind counts returned with
        :meth:`ResourceLedger.release_count`.  Destroy-before-release is
        what makes an interrupted settle retry-safe: the retry recomputes
        both counts from the fragment's stable byte buckets capped at the
        still-held remainder, so a crash after the destroy frees exactly
        the freed bytes' worth, and a crash before it leaves everything
        held.  A destroy shortfall (unlink failure) is reported and its
        tokens stay held: a failed decharge must never be freed as the
        duplicate it was meant to destroy.  Free-side rename failures
        likewise stay held and converge on retry.

        No lock is taken here beyond what the ledger methods take
        themselves: the caller (the egress, under the mover transition
        lock, the stage ownership lock and the tier mint lock as its leaf)
        already excludes concurrent ownership decisions and
        stale-snapshot mints, and each ledger call below re-acquires the
        same tier mint lock (same-thread nesting re-acquires safely), so
        a direct caller without the outer lock is still serialized
        against the reclaim scan.  Lock order stays
        transition -> ownership -> mint throughout.  Returns
        ``{"destroyed": {...}, "released": {...}, "shortfall": {...}}``;
        all three count actual token files.
        """

        ledger = self.tier_ledger(tier_id)
        destroyed = ledger.retire_held(action_key, destroy)
        shortfall = {kind: int(count) - int(destroyed.get(kind, 0))
                     for kind, count in destroy.items()
                     if int(count) - int(destroyed.get(kind, 0)) > 0}
        released = ledger.release_count(action_key, free)
        return {"destroyed": destroyed, "released": released,
                "shortfall": shortfall}

    def tier_record_path(self, tier_id: str) -> Path:
        return self.root / TIERS / f"{self._check_tier_id(tier_id)}.json"

    def announce_tier(self, record: Mapping[str, object]) -> Path:
        """File what a tier loop discovered about one tier, for submitters and readers.

        Not the worker offer: an offer is one record per *box*, last writer
        wins, and a tier is not a box.  The record is advisory -- the ledger
        is the admission authority -- and it says where the tier is mounted
        and what its members are, which a mover needs and a ledger does not
        carry.
        """

        tier_id = self._check_tier_id(str(record.get("tier_id", "")))
        path = self.tier_record_path(tier_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        _write_json_atomic(path, dict(record, announced_unix=_now()))
        return path

    def tiers(self) -> list[dict[str, object]]:
        """Every announced tier record, by tier id."""

        records = []
        for path in _glob(self.root / TIERS, "*.json"):
            record = _read_json(path)
            if isinstance(record, dict) and record.get("tier_id") == path.stem:
                records.append(record)
        return records

    def _begin_tier_acquire(
        self, action_key: str, tier_demand: Mapping[str, Mapping[str, int]],
        handles: dict[str, str], funded: dict[str, dict[str, object]],
        cas_root: str | Path | None = None,
    ) -> dict[str, object] | None:
        """Take every tier's tokens, or say which tier stopped it.

        Tier by tier in id order, all-or-nothing per tier as ``begin_acquire``
        already is; the caller abandons every handle in ``handles`` when this
        returns a shortage, so all-or-nothing holds across tiers as well.
        Deliberately outside the host admission lock: these ledgers are on
        the shared mount, and holding host admission across a mount stall is
        the #351 shape.  The shortage's ``reason`` is the denial the caller
        records: a tier with no ledger at all, a tier whose whole capacity is
        below the demand (which no waiting will fix), or one that is merely
        busy.

        ``funded`` is filled per tier with what this claim's funding record
        covers (``{tier_id: {"kinds": {kind: count}, "generation": ...,
        "tokens": [...]}}``): pre-positioned fence tokens the coordinator
        transferred under this key, verified by name against what is actually
        held, with the verified token names bound into the entry so a later
        rollback can return the remainder this attempt took while keeping
        exactly the fence.  The ``begin_acquire`` below takes only the
        remainder from free, so a funded claim never double-holds.  Anything
        else the key holds -- a landed range's tokens, a previous attempt's
        leftovers -- is physical occupancy and is never subtracted here: only
        a strict funding record in ``transferring`` authorizes a subtraction.
        """

        # The sealed row funds at most once: its publication (including its
        # generation) is read once here and shared by every tier below, so a
        # republish between tiers cannot fund half a claim on stale credit.
        # A failed reread is UNKNOWN evidence, never an empty row: the
        # requiredness verdict below must not authorize fresh acquisition
        # from an unreadable row.
        try:
            sealed = _read_json(self.item_path(READY, action_key))
            sealed_read_error = False
        except (OSError, PoolContractError):
            sealed = None
            sealed_read_error = True
        # The immutable CAS-filed request is read ONCE for this action here
        # (no history scan, no per-token lookup) and shared by every tier
        # below: requiredness and the cover binding derive from it, and the
        # READY projection must agree with it. An unreadable/invalid request
        # is UNKNOWN evidence, never legacy. No request file is the narrow
        # pre-existing direct-API compatibility path (kwarg-supplied
        # reference, fully validated at publish); it never validates an
        # ordinary production row.
        try:
            immutable_ref, immutable_present = _sealed_produced_output_batch(
                cas_root if cas_root is not None else "", action_key)
            immutable_error = False
        except (OSError, PoolContractError, ValueError):
            immutable_ref, immutable_present = None, False
            immutable_error = True
        sealed_has_key = (
            isinstance(sealed, Mapping)
            and "produced_output_batch" in sealed)
        # Projection agreement, once per action: when the immutable request
        # carries the reference, the READY projection must carry the same
        # stable batch identity. Missing, malformed, or contradictory READY
        # evidence never authorizes fresh tier acquisition.
        projection_agrees: bool | None = None
        if immutable_ref is not None:
            projection_agrees = self._output_projection_matches_request(
                sealed.get("produced_output_batch")
                if isinstance(sealed, Mapping) else None,
                immutable_ref)
        for tier_id, needs in sorted(tier_demand.items()):
            ledger = self.tier_ledger(tier_id)
            if not ledger.base.is_dir():
                return {"tier_id": tier_id, "reason": "tier_unknown", "demand": dict(needs)}
            total = ledger.capacity()
            if any(total.get(kind, 0) < need for kind, need in needs.items()):
                return {"tier_id": tier_id, "reason": "never_fits_tier_capacity",
                        "capacity_total": total, "demand": dict(needs)}
            covered: dict[str, int] = {}
            generation: str | None = None
            variant: str | None = None
            if isinstance(sealed, Mapping):
                for kind, need in needs.items():
                    try:
                        count, covered_generation = self.funded_cover(
                            tier_id, sealed, kind, int(need))
                    except (OSError, PoolContractError, ValueError):
                        continue
                    if count:
                        covered[kind] = count
                        generation = covered_generation
                        variant = "window"
                # Output variant is additive and never weakens V1: only when
                # V1 covers nothing for this tier, try the prepaid-output
                # binding (same strictness, different scope proof).
                if not any(covered.values()):
                    for kind, need in needs.items():
                        try:
                            count, covered_generation = self.output_funded_cover(
                                tier_id, sealed, kind, int(need))
                        except (OSError, PoolContractError, ValueError):
                            continue
                        if count:
                            covered[kind] = count
                            generation = covered_generation
                            variant = "output"
            if any(covered.values()):
                # Bind the verified token names now, under this key's
                # transition lock: the rollback below must tell the fence it
                # keeps from the remainder it returns, and names read later
                # could be a rotated generation's.  Anything off -- moved
                # state, rotated generation, unnamed tokens -- fails closed
                # to no cover, and the claim pays its full demand.
                names: list[str] = []
                if variant == "output":
                    proof = self.read_output_funding(action_key, tier_id)
                else:
                    proof = self.read_funding(action_key, tier_id)
                if (proof is not None and proof.get("state") == "transferring"
                        and isinstance(proof.get("generation"), str)
                        and str(proof.get("generation")) == generation
                        and isinstance(proof.get("tokens"), list)
                        and proof.get("tokens")):
                    names = sorted(str(name) for name in proof["tokens"])  # type: ignore[union-attr]
                if not names:
                    covered = {}
                elif variant != "output":
                    funded[tier_id] = {"kinds": dict(covered),
                                       "generation": generation,
                                       "tokens": names,
                                       "variant": variant or "window"}
                elif (immutable_error
                        or (immutable_present and immutable_ref is None)
                        or (immutable_ref is not None
                            and projection_agrees is not True)
                        or (immutable_ref is not None
                            and not self._output_record_matches_request(
                                proof, immutable_ref))):
                    # R7: a successful mutable cover rests on immutable
                    # authority; it never replaces it. Unknown request
                    # evidence (unreadable/invalid) never authorizes prepaid
                    # credit; a valid filed request that declares no output
                    # reference can never gain one from a mutable projection;
                    # and a real reference demands a READY projection that
                    # agrees with it plus a funding record that binds back to
                    # it. Only the narrow no-request direct-API path (kwarg
                    # reference, fully validated at publish) covers without
                    # a filed request. Dropping the cover makes the gate
                    # below defer; it never pays fresh.
                    covered = {}
                else:
                    funded[tier_id] = {"kinds": dict(covered),
                                       "generation": generation,
                                       "tokens": names,
                                       "variant": "output"}
            remainder = {kind: int(need) - int(covered.get(kind, 0))
                         for kind, need in needs.items()}
            if not any(covered.values()):
                # Output claim gate (R6): requiredness derives from the
                # immutable CAS request (read once above) OR the sealed READY
                # projection KEY (valid or corrupt: a present-but-malformed
                # projection is tampering, never legacy) OR a funding file in
                # any state -- with no history-wide admission scans. Required
                # rows defer/refuse on absent/unknown/pending/invalid/terminal
                # proof unless cover succeeded above; never fresh acquisition,
                # even if every mutable output file is absent (precommit crash
                # with a sealed ref but no intent/commit yet defers, because
                # the immutable request still says REQUIRED). Unknown READY
                # or request evidence defers as well, never legacy fresh.
                # Key ABSENCE alone (no request ref, no sealed key, no funding
                # file) is legacy and retains existing admission behavior.
                if sealed_read_error or immutable_error:
                    return {"tier_id": tier_id,
                            "reason": "output_funding_unknown",
                            "demand": dict(needs)}
                try:
                    _rec, _fstate = self.output_funding_file_state(
                        action_key, tier_id)
                except (OSError, PoolContractError, ValueError):
                    _rec, _fstate = None, "corrupt"
                _required = (
                    immutable_ref is not None
                    or sealed_has_key
                    or _fstate in ("ok", "corrupt"))
                if not _required:
                    # Legacy (or a vanished row with no other signal): the
                    # rename below decides; existing admission behavior.
                    pass
                else:
                    if (immutable_ref is not None
                            and projection_agrees is not True):
                        return {"tier_id": tier_id,
                                "reason": "output_funding_unknown",
                                "demand": dict(needs)}
                    if (immutable_present and immutable_ref is None):
                        # A valid filed request that declares no output
                        # reference cannot gain one from a mutable row:
                        # contradiction, never legacy and never fresh.
                        return {"tier_id": tier_id,
                                "reason": "output_funding_unknown",
                                "demand": dict(needs)}
                    if _fstate == "ok":
                        _st = str((_rec or {}).get("state"))
                        return {"tier_id": tier_id,
                                "reason": ("output_funding_pending"
                                           if _st in ("reserved",
                                                      "transferring")
                                           else "output_funding_terminal"),
                                "demand": dict(needs)}
                    if _fstate == "corrupt":
                        return {"tier_id": tier_id,
                                "reason": "output_funding_unknown",
                                "demand": dict(needs)}
                    return {"tier_id": tier_id,
                            "reason": "output_funding_required_absent",
                            "demand": dict(needs)}
            handle = ledger.begin_acquire(action_key, remainder)
            if handle is None:
                return {"tier_id": tier_id, "reason": "tier_reservation_unavailable",
                        "token_shortage": ledger.last_token_shortage,
                        "capacity_total": total, "available": ledger.available(),
                        "demand": dict(needs)}
            handles[tier_id] = handle
        return None

    def _abandon_tier_acquire(self, handles: Mapping[str, str]) -> int:
        """Return every claimant-private tier handle; a committed one owns nothing and is a no-op."""

        return sum(self.tier_ledger(tier_id).abandon_acquire(handle)
                   for tier_id, handle in sorted(handles.items()))

    def _commit_tier_acquire(self, action_key: str, handles: Mapping[str, str]) -> int:
        """File every tier handle under the action key; returns the tokens that moved."""

        return sum(self.tier_ledger(tier_id).commit_acquire(action_key, handle)
                   for tier_id, handle in sorted(handles.items()))

    def _unwind_funded_tier_commit(
        self, action_key: str, tier_demand: Mapping[str, Mapping[str, int]],
        tier_funded: Mapping[str, Mapping[str, object]],
    ) -> None:
        """Return a rolled-back claim's remainder, keep its verified fences.

        Past ``_commit_tier_acquire`` the handles are empty, so abandoning
        them returns nothing: the remainder this attempt newly took from free
        is filed under the key beside the fence.  Per tier: unfunded tiers
        release by key (everything there is remainder); funded tiers release
        everything EXCEPT the exact token names the claim verified against
        the funding record, so the fence stays fused with no free interval
        for a stealer while the remainder goes home and the retry cannot add
        it again.  Releasing the fence by key would open that steal gap;
        keeping everything would double-hold on retry.  Contained per tier
        like :meth:`release_tier_reservations`: a tier this call cannot read
        keeps its tokens for the next attempt rather than costing the unwind.
        """

        for tier_id in sorted(tier_demand):
            try:
                ledger = self.tier_ledger(tier_id)
            except (OSError, PoolContractError, ValueError):
                continue
            entry = tier_funded.get(tier_id)
            names = (entry.get("tokens") if isinstance(entry, dict) else None)
            try:
                if isinstance(names, list) and names:
                    ledger.release_except(
                        action_key, {str(name) for name in names})
                else:
                    ledger.release(action_key)
            except (OSError, PoolContractError, ValueError):
                continue

    # -- residency (#583) -------------------------------------------------

    @classmethod
    def validate_residency(
        cls, residency: Mapping[str, object], demand: Mapping[str, int],
    ) -> dict[str, object]:
        """Refuse a residency block that is not arithmetic over its manifest.

        Two halves, either or both.  A **mover** names the byte range of the
        manifest's read order it makes resident; its ``stage_gib`` demand on
        that tier must be at least the range's own ceiling in GiB, so the
        number in the claim record is a quotation from the manifest rather
        than a number somebody typed (``pb_demand_must_be_measured_not_
        habitual``).  A **consumer** names its lead movers; the gate admits it
        only once each one has moved the bytes.

        The manifest digest travels with both so the range is readable: a
        range is meaningless without the list that maps it to files, and that
        list is content-addressed in the CAS like any other input.
        """

        block = dict(residency)
        unknown = sorted(set(block) - _RESIDENCY_KEYS)
        if unknown:
            raise PoolContractError(f"unknown residency fields: {unknown}")
        if block.get("schema") != RESIDENCY_SCHEMA_V1:
            raise PoolContractError(f"residency schema must be {RESIDENCY_SCHEMA_V1!r}")
        # The manifest is what turns a byte range into files, and a digest is
        # what makes two blocks provably about the same list.  ``core``'s
        # ``residency_descriptor`` validates exactly these two fields for the
        # mover's result; a block that declared them loosely would let the two
        # validators disagree about one object.  The pool cannot open the
        # manifest -- ``publish`` takes a ``cas_root`` and never reads it -- so
        # what it enforces is the shape, and ``residency_verdict`` enforces the
        # agreement between a consumer's block and its leads'.
        digest = block.get("manifest_sha256")
        if (not isinstance(digest, str)
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)):
            raise PoolContractError(
                "residency.manifest_sha256 must be a 64-character lowercase digest")
        size = block.get("manifest_bytes")
        if isinstance(size, bool) or type(size) is not int or size <= 0:
            raise PoolContractError(
                "residency.manifest_bytes must be a positive integer")
        tier_id = block.get("tier_id")
        if tier_id is not None:
            cls._check_tier_id(str(tier_id))
        leads = block.get("leads") or []
        if not isinstance(leads, list):
            raise PoolContractError("residency.leads must be an array of action keys")
        for lead in leads:
            if not isinstance(lead, str) or len(lead) != 64:
                raise PoolContractError(
                    "residency.leads must be 64-character action keys")
        if len(set(leads)) != len(leads):
            raise PoolContractError("residency.leads must not repeat a key")
        start = block.get("range_start_bytes")
        end = block.get("range_end_bytes")
        if (start is None) != (end is None):
            raise PoolContractError(
                "residency range needs both range_start_bytes and range_end_bytes")
        if start is not None:
            for name, value in (("range_start_bytes", start), ("range_end_bytes", end)):
                if isinstance(value, bool) or type(value) is not int or value < 0:
                    raise PoolContractError(
                        f"residency.{name} must be a non-negative integer")
            if int(end) <= int(start):
                raise PoolContractError(
                    "residency range must be non-empty and half-open (start < end)")
            if tier_id is None:
                raise PoolContractError("a residency range must name the tier it lands on")
            floor = storage_tiers.stage_tokens_for_bytes(int(end) - int(start))
            # The tier id declares which token kind it deals in; the same rule
            # ``residency_demand`` derives the demand with, so a block this
            # refuses is never one PB's own derivation would have produced.
            kind = (f"{storage_tiers.capacity_kind_of(str(tier_id))}"
                    f"{storage_tiers.TIER_DEMAND_SEPARATOR}{tier_id}")
            declared = int(demand.get(kind, 0))
            if declared < floor:
                # The range is the measurement; the demand is a claim about
                # it.  A claim below the measurement would let a mover pin
                # bytes the tier never counted, which is precisely the
                # accounting #583 exists to close.
                raise PoolContractError(
                    f"residency demand {kind}={declared} is below the "
                    f"{floor} GiB its declared range occupies")
        if not leads and start is None:
            raise PoolContractError(
                "a residency block must declare a range, leads, or both")
        return block

    @classmethod
    def validate_produced_output(
        cls,
        template: Mapping[str, object],
        demand: Mapping[str, int],
        *,
        residency_block: Mapping[str, object] | None = None,
    ) -> tuple[dict[str, object], dict[str, object]]:
        """Refuse a produced-output declaration that is not arithmetic.

        The template is validated by its owner (``produced_output.
        validate_template``: closed field set, authorized stage/ram tiers,
        minimum-within-window, permitted == demands). The tier demand the
        action carries must then be exactly the bounded working window the
        template derives (``owner_demand_terms``: window GiB, never the
        durable corpus), plus the input range floor when an input residency
        range lands on the same tier. Input leads carry no tier demand, so a
        producer with input residency and an output template still owes
        exactly the output window. Underdeclared, mismatched, foreign, or
        extra tier demand refuses; the existing ledger channel then admits
        the combined host + tier capacity atomically at claim.
        """

        try:
            from . import produced_output as produced_mod
        except ImportError as exc:
            raise PoolContractError(
                f"produced-output template needs produced_output: {exc}"
            ) from None
        try:
            validated = produced_mod.validate_template(template)
        except produced_mod.ProducedOutputError as exc:
            raise PoolContractError(f"produced-output template: {exc}") from exc
        try:
            terms = produced_mod.owner_demand_terms(validated)
        except produced_mod.ProducedOutputError as exc:
            raise PoolContractError(f"produced-output demand: {exc}") from exc
        _, expected_grouped = storage_tiers.split_demand(
            {str(k): int(v) for k, v in terms.items()})
        expected: dict[str, dict[str, int]] = {
            tier: dict(needs) for tier, needs in expected_grouped.items()}
        if residency_block is not None:
            start = residency_block.get("range_start_bytes")
            end = residency_block.get("range_end_bytes")
            tier_id = residency_block.get("tier_id")
            if start is not None and end is not None and tier_id is not None:
                floor = storage_tiers.stage_tokens_for_bytes(
                    int(end) - int(start))
                kind = (f"{storage_tiers.capacity_kind_of(str(tier_id))}"
                        f"{storage_tiers.TIER_DEMAND_SEPARATOR}{tier_id}")
                _, tier_only = storage_tiers.split_demand_key(kind)
                assert tier_only is not None
                bare = kind.split(
                    storage_tiers.TIER_DEMAND_SEPARATOR, 1)[0]
                expected.setdefault(str(tier_id), {})
                expected[str(tier_id)][bare] = int(
                    expected[str(tier_id)].get(bare, 0)) + int(floor)
        _, declared_grouped = storage_tiers.split_demand(
            {str(k): int(v) for k, v in dict(demand).items()})
        if declared_grouped != expected:
            raise PoolContractError(
                "produced-output tier demand must exactly cover the declared "
                f"working window (plus input range floor where present): "
                f"declared {sorted(declared_grouped.items())} != "
                f"expected {sorted(expected.items())}")
        ref = {
            "schema": produced_mod.PRODUCED_OUTPUT_REF_SCHEMA_V1,
            "template_id": str(validated["template_id"]),
            "template_sha256": produced_mod.template_sha256(validated),
        }
        return validated, ref

    @staticmethod
    def build_produced_output_batch_ref(*, instance, template,
                                        batch_id: str,
                                        descriptors: list,
                                        tier_id: str) -> dict[str, object]:
        """Build the sealed immutable batch reference for an output mover (R4).

        Writer-facing builder so #744 wires stage->publish->drive->commit
        without inventing another field: validates the bound precommit with
        existing produced_output validators (bound contract, descriptors,
        manifest recompute) and returns the closed reference the mover's
        sealed params must carry. Carries NO mover key and NO funding
        generation/publication timestamp (stable batch identity across
        generation rotation). Raises PoolContractError on any mismatch.
        """

        try:
            from . import produced_output as produced_mod
        except ImportError as exc:
            raise PoolContractError(
                f"produced-output batch needs produced_output: {exc}"
            ) from None
        try:
            checked_template = produced_mod.validate_template(template)
            checked_instance = produced_mod.validate_instance(instance)
        except produced_mod.ProducedOutputError as exc:
            raise PoolContractError(f"batch reference: {exc}") from exc
        if (str(checked_instance.get("template_sha256"))
                != produced_mod.template_sha256(checked_template)):
            raise PoolContractError("batch reference: template-mismatch")
        if str(tier_id) not in checked_template.get("permitted_tiers", []):
            raise PoolContractError("batch reference: tier-not-permitted")
        if (not isinstance(batch_id, str) or not batch_id or "/" in batch_id
                or "\x00" in batch_id):
            raise PoolContractError(
                "batch reference batch_id must be a non-empty name with no '/'")
        if not isinstance(descriptors, list) or not descriptors:
            raise PoolContractError("batch reference descriptors required")
        try:
            sealed = [produced_mod.validate_descriptor(
                dict(d), checked_template, checked_instance)
                for d in descriptors]
        except produced_mod.ProducedOutputError as exc:
            raise PoolContractError(f"batch reference: {exc}") from exc
        manifest = produced_mod.output_manifest_sha256(sealed)
        total = sum(int(d["bytes"]) for d in sealed)
        if total <= 0:
            raise PoolContractError("batch reference total must be positive")
        attempt = checked_instance.get("owner_attempt")
        if not isinstance(attempt, dict):
            raise PoolContractError("batch reference: bad owner attempt")
        try:
            _ns, batch_ns = produced_mod.namespace_for_batch_reference(
                owner_action_key=str(checked_instance["owner_action_key"]),
                template_sha256=str(checked_instance["template_sha256"]),
                nonce=str(attempt["nonce"]), scope_id=str(attempt["scope_id"]),
                template_id=str(checked_template["template_id"]),
                output_prefix=str(checked_template["output_prefix"]),
                batch_id=batch_id, manifest_digest=manifest)
        except produced_mod.ProducedOutputError as exc:
            raise PoolContractError(f"batch reference: {exc}") from exc
        return {
            "schema": PRODUCED_OUTPUT_BATCH_REF_SCHEMA_V1,
            "owner_action_key": str(checked_instance["owner_action_key"]),
            "owner_nonce": str(attempt["nonce"]),
            "owner_scope_id": str(attempt["scope_id"]),
            "template_id": str(checked_template["template_id"]),
            "template_sha256": produced_mod.template_sha256(checked_template),
            "batch_id": batch_id,
            "manifest_digest": manifest,
            "tier_id": str(tier_id),
            "range_start_bytes": 0,
            "range_end_bytes": total,
            "batch_namespace": batch_ns,
        }

    def validate_produced_output_batch(
            self, ref, demand: Mapping[str, int],
            residency_block: Mapping[str, object] | None = None) -> dict[str, object]:
        """Refuse a produced-output batch reference that is not exact (R4).

        Strict scalar shapes (closed set; hex widths; no '/' or NUL names;
        finite ranges; total>0 with start 0); filed template by template_id
        must exist and its sha must equal the reference; namespace recomputed
        through produced_output's existing validators must equal the
        reference; tier must be permitted with demand exactly the range floor
        on that tier alone (single-tier output movers); sealed residency, when
        given, must name the same tier/manifest/range. Returns the checked
        reference. Publication stores this projection immutably in the item.
        """

        if not isinstance(ref, Mapping):
            raise PoolContractError("produced-output batch must be an object")
        unknown = sorted(set(ref) - {
            "schema", "owner_action_key", "owner_nonce", "owner_scope_id",
            "template_id", "template_sha256", "batch_id", "manifest_digest",
            "tier_id", "range_start_bytes", "range_end_bytes",
            "batch_namespace",
        })
        if unknown:
            raise PoolContractError(
                f"unknown produced-output batch fields: {unknown}")
        if ref.get("schema") != PRODUCED_OUTPUT_BATCH_REF_SCHEMA_V1:
            raise PoolContractError(
                "produced-output batch schema must be "
                f"{PRODUCED_OUTPUT_BATCH_REF_SCHEMA_V1!r}")
        owner = ref.get("owner_action_key")
        if (not isinstance(owner, str) or len(owner) != 64
                or any(c not in "0123456789abcdef" for c in owner)):
            raise PoolContractError(
                "batch reference owner_action_key must be a 64-character key")
        nonce = ref.get("owner_nonce")
        if (not isinstance(nonce, str) or len(nonce) != 32
                or any(c not in "0123456789abcdef" for c in nonce)):
            raise PoolContractError(
                "batch reference owner_nonce must be a 32-character nonce")
        for field in ("owner_scope_id", "template_id", "batch_id"):
            text = ref.get(field)
            if (not isinstance(text, str) or not text or "/" in text
                    or "\x00" in text):
                raise PoolContractError(
                    f"batch reference {field} must be a non-empty name with no '/'")
        for field in ("template_sha256", "manifest_digest", "batch_namespace"):
            digest = ref.get(field)
            if (not isinstance(digest, str) or len(digest) != 64
                    or any(c not in "0123456789abcdef" for c in digest)):
                raise PoolContractError(
                    f"batch reference {field} must be a 64-character digest")
        tier_id = ref.get("tier_id")
        if not isinstance(tier_id, str) or not tier_id:
            raise PoolContractError(
                "batch reference tier_id must be a non-empty string")
        try:
            kind = storage_tiers.capacity_kind_of(str(tier_id))
        except ValueError as exc:
            raise PoolContractError(f"batch reference: {exc}") from exc
        start = ref.get("range_start_bytes")
        end = ref.get("range_end_bytes")
        if (isinstance(start, bool) or type(start) is not int or start != 0
                or isinstance(end, bool) or type(end) is not int
                or int(end) <= 0):
            raise PoolContractError(
                "batch reference range must be 0..positive total")
        total = int(end)
        try:
            from . import produced_output as produced_mod
        except ImportError as exc:
            raise PoolContractError(
                f"produced-output batch needs produced_output: {exc}"
            ) from None
        try:
            with open(self.root / "residency"
                      / produced_mod.OUTPUT_TEMPLATES_SUBDIR
                      / f"{ref['template_id']}.json", "rb") as handle:
                raw_tmpl = handle.read(1024 * 1024 + 1)
        except FileNotFoundError:
            raise PoolContractError(
                "batch reference template is not declared") from None
        except OSError as exc:
            raise PoolContractError(
                f"batch reference template unreadable: {exc}") from None
        if len(raw_tmpl) > 1024 * 1024:
            raise PoolContractError("batch reference template oversize")
        try:
            import json as _json
            filed_template = produced_mod.validate_template(
                _json.loads(raw_tmpl.decode()))
        except (ValueError, UnicodeDecodeError,
                produced_mod.ProducedOutputError) as exc:
            raise PoolContractError(
                f"batch reference template: {exc}") from exc
        if (produced_mod.template_sha256(filed_template)
                != str(ref.get("template_sha256"))):
            raise PoolContractError("batch reference template-mismatch")
        if str(tier_id) not in filed_template.get("permitted_tiers", []):
            raise PoolContractError("batch reference tier-not-permitted")
        try:
            _inst_ns, batch_ns = produced_mod.namespace_for_batch_reference(
                owner_action_key=str(owner),
                template_sha256=str(ref.get("template_sha256")),
                nonce=str(nonce),
                scope_id=str(ref.get("owner_scope_id")),
                template_id=str(ref.get("template_id")),
                output_prefix=str(filed_template["output_prefix"]),
                batch_id=str(ref.get("batch_id")),
                manifest_digest=str(ref.get("manifest_digest")))
        except produced_mod.ProducedOutputError as exc:
            raise PoolContractError(f"batch reference: {exc}") from exc
        if batch_ns != str(ref.get("batch_namespace")):
            raise PoolContractError("batch reference namespace-mismatch")
        try:
            floor = storage_tiers.stage_tokens_for_bytes(total)
        except ValueError as exc:
            raise PoolContractError(f"batch reference: {exc}") from exc
        try:
            _, declared_grouped = storage_tiers.split_demand(
                {str(k): int(v) for k, v in dict(demand).items()})
        except (TypeError, ValueError) as exc:
            raise PoolContractError(f"batch reference demand: {exc}") from exc
        tier_needs = declared_grouped.get(str(tier_id), {})
        if (set(tier_needs) != {kind} or int(tier_needs[kind]) != int(floor)
                or len(declared_grouped) != 1):
            raise PoolContractError(
                "batch reference tier demand must be exactly the range floor "
                f"on {tier_id} alone: expected {{{kind}: {floor}}}")
        if residency_block is not None:
            if not isinstance(residency_block, Mapping):
                raise PoolContractError("batch reference needs a residency block")
            if (str(residency_block.get("tier_id")) != str(tier_id)
                    or str(residency_block.get("manifest_sha256")) != str(
                        ref.get("manifest_digest"))
                    or residency_block.get("range_start_bytes") != 0
                    or residency_block.get("range_end_bytes") != total):
                raise PoolContractError(
                    "batch reference residency mismatch")
        return dict(ref)

    #: Stable batch-identity fields shared by the sealed reference, the READY
    #: projection, and the funding record (R6). The funding record carries no
    #: ``batch_namespace`` (checked separately at publication); the schemas
    #: differ per carrier and are checked at their own validation sites.
    _OUTPUT_BATCH_IDENTITY_FIELDS = (
        "owner_action_key", "owner_nonce", "owner_scope_id", "template_id",
        "template_sha256", "batch_id", "manifest_digest", "tier_id",
        "range_start_bytes", "range_end_bytes",
    )

    @staticmethod
    def _output_batch_identity_matches(candidate: object, ref: object, *,
                                       projection_namespace: bool) -> bool:
        """Do the stable batch-identity fields of two carriers agree (R6/R7)?

        ``candidate`` is a READY projection (all fields incl. namespace) or
        a funding record (no namespace); ``ref`` is the validated immutable
        CAS request reference, which always carries ``batch_namespace``.
        Non-mapping, missing, or mistyped fields are disagreement, never
        agreement; ranges compare as integers.

        ``batch_namespace`` is a projection-only field. With
        ``projection_namespace`` (projection vs ref) both carriers must carry
        the same string. Without it (funding record vs ref) the binding is
        the shared identity fields alone: the namespace is a pure function
        of those fields plus the filed template's ``output_prefix``, the
        reference's own namespace was recomputed against them at
        publication (``validate_produced_output_batch``), and the funding
        record's closed schema deliberately has no such field -- demanding
        it from either carrier rejects every valid production record. A
        record that nonetheless carries the field is not a valid
        closed-schema record and does not match.
        """

        fields = PoolQueue._OUTPUT_BATCH_IDENTITY_FIELDS
        if not isinstance(candidate, Mapping) or not isinstance(ref, Mapping):
            return False
        try:
            for field in fields:
                lhs = candidate.get(field)
                rhs = ref.get(field)
                if field in ("range_start_bytes", "range_end_bytes"):
                    if int(lhs) != int(rhs):  # type: ignore[arg-type]
                        return False
                else:
                    if (not isinstance(lhs, str) or not isinstance(rhs, str)
                            or lhs != rhs):
                        return False
            if projection_namespace:
                lhs_ns = candidate.get("batch_namespace")
                rhs_ns = ref.get("batch_namespace")
                if (not isinstance(lhs_ns, str) or not isinstance(rhs_ns, str)
                        or lhs_ns != rhs_ns):
                    return False
            elif "batch_namespace" in candidate:
                return False
        except (TypeError, ValueError, AttributeError):
            return False
        return True

    @staticmethod
    def _output_projection_matches_request(projection: object,
                                           ref: object) -> bool:
        """Does the READY projection carry the immutable request identity (R6)?

        Full stable identity incl. namespace; the schema is checked by the
        caller alongside (projection schema constant). Explicit null,
        non-mapping, missing, or contradictory values are disagreement.
        """

        if not isinstance(projection, Mapping) or not isinstance(ref, Mapping):
            return False
        if (projection.get("schema")
                != PRODUCED_OUTPUT_BATCH_REF_SCHEMA_V1):
            return False
        return PoolQueue._output_batch_identity_matches(
            projection, ref, projection_namespace=True)

    @staticmethod
    def _output_record_matches_request(record: object, ref: object) -> bool:
        """Does a funding record bind back to the immutable request ref (R6/R7)?

        Shared stable identity (the record carries no namespace and its own
        schema); generation/publication stay mutable beside it and are
        checked separately by the cover path.
        """

        if not isinstance(record, Mapping) or not isinstance(ref, Mapping):
            return False
        return PoolQueue._output_batch_identity_matches(
            record, ref, projection_namespace=False)

    @staticmethod
    def _residency_manifest_of(record: Mapping[str, object] | None) -> str | None:
        """The manifest digest a record's own residency block names, if any."""

        if not isinstance(record, Mapping):
            return None
        block = record.get("residency")
        if not isinstance(block, Mapping):
            return None
        declared = block.get("manifest_sha256")
        return str(declared) if isinstance(declared, str) else None

    def _superseded_status_of(self, action_key: str) -> str | None:
        """The newest drop filed for this key, or ``None`` if it was never dropped.

        ``_file_superseded`` writes ``<key>.<unix>.<kind>.json`` under
        ``withdrawn/superseded/``, which is deliberately invisible to every
        reader that addresses a state directory by ``<key>.json``.  A lead
        dropped by the terminal-claim branch, the withdrawal race or the reaper
        therefore has no record in ``done/``, ``failed/`` or ``withdrawn/`` at
        all, and reporting it as ``absent`` is the denial nobody can act on.
        """

        newest: tuple[str, str] | None = None
        for path in _glob(self.superseded_dir(), f"{action_key}.*.json"):
            record = _read_json(path)
            if not isinstance(record, Mapping):
                continue
            status = record.get("status")
            stamp = path.name
            if newest is None or stamp > newest[0]:
                newest = (stamp, str(status) if status is not None else "superseded")
        return None if newest is None else newest[1]

    def residency_verdict(self, item: Mapping[str, object]) -> dict[str, object]:
        """Whether this item's declared bytes are resident, and why not.

        ``not_requested`` for everything the fleet publishes today; written onto
        the claim record whenever an item carries a block.  A lead counts as
        resident only when its ``done/`` record says ``executed`` **and** that
        record names the same manifest the consumer does: a ``cache_hit``
        finished without moving a byte, and a mover that made a range of some
        *other* manifest resident has made none of this consumer's bytes
        resident.  Both are traps a deterministic descriptor invites, because
        it is what lets a consumer bind a mover's result before the mover runs.
        """

        residency = item.get("residency")
        if not isinstance(residency, Mapping):
            return {"state": "not_requested"}
        leads = residency.get("leads") or []
        if not isinstance(leads, list) or not leads:
            return {"state": "no_leads"}
        wanted = residency.get("manifest_sha256")
        pending: list[dict[str, object]] = []
        for lead in leads:
            record = _read_json(self.item_path(DONE, str(lead)))
            status = record.get("status") if isinstance(record, Mapping) else None
            if status == "executed":
                declared = self._residency_manifest_of(record)
                if wanted is None or declared is None or declared == wanted:
                    if self._lead_is_pinned(residency, str(lead)):
                        continue
                    # ``executed`` is not residency once tokens are pinned.  A
                    # mover that moved nothing, or that refused for an overrun,
                    # ends ``executed`` exactly like one that staged 34 GB; the
                    # difference is whether it still holds tokens, because the
                    # pin is filed only when its receipt matches its range.
                    # Reading the ledger rather than the receipt also covers a
                    # mover whose bytes an egress has since deleted.
                    pending.append({"lead": str(lead), "status": "unpinned"})
                    continue
                # The pool cannot open the manifest -- it holds records, not
                # the CAS -- but it holds both blocks, and two blocks naming
                # two digests are two manifests whatever the bytes say.
                pending.append({"lead": str(lead), "status": "manifest_mismatch",
                                "declared_manifest_sha256": declared,
                                "expected_manifest_sha256": str(wanted)})
                continue
            if status is None:
                if self._lead_was_adopted(residency, str(lead)):
                    # No terminal record because it never ran: this range was
                    # already on the tier and the coordinator handed it the
                    # tokens instead of publishing a copy (#598).  Checked
                    # here rather than before the ``done/`` read, so a lead
                    # that *did* run costs the claim scan no extra read of the
                    # shared mount -- this mount is where a claim's latency
                    # comes from, and the adopted case is exactly the case
                    # where that read found nothing.
                    continue
                # A lead that ended badly will never become resident, and a
                # denial that could not tell that from "has not started yet"
                # would be a denial nobody can act on.  A *drop* is filed under
                # ``withdrawn/superseded/`` rather than in any of the three
                # state directories, so it is read there or it reads as absent.
                # No drop policy is implied for the consumer: the item stays
                # ready, exactly as it does while its mover is still queued.
                for state in (FAILED, WITHDRAWN):
                    ended = _read_json(self.item_path(state, str(lead)))
                    if isinstance(ended, Mapping):
                        status = str(ended.get("status") or state)
                        break
                else:
                    status = self._superseded_status_of(str(lead))
            pending.append({"lead": str(lead),
                            "status": status if status is not None else "absent"})
        if pending:
            # Two denials, because they mean different things to whoever reads
            # them: a lead that has not finished may still finish, while a lead
            # that finished holding nothing will never become resident without
            # being republished.
            unpinned = all(entry.get("status") == "unpinned" for entry in pending)
            return {"state": "lead_unpinned" if unpinned else "lead_not_resident",
                    "pending": pending,
                    "leads": [str(lead) for lead in leads]}
        # Pinned bytes the consumer cannot find are bytes it does not read.
        # The launcher passes ``RESIDENCY_MAP_ENV`` only when the composed map
        # is on disk, and the loop composes it from the fragments a mover
        # files -- so between a mover pinning its range and the next tier
        # cycle there is a window in which every lead is resident and the map
        # is not there yet.  Admitting in that window launches the consumer
        # with no map, which is not a failure: it reads the pool at full cost
        # and reports a clean run, which is exactly the outcome #583 exists to
        # remove.  The item stays ready and is admitted on a later scan.
        key = item.get("action_key")
        composed: Path | None = None
        if isinstance(key, str):
            try:
                composed = self.residency_map_path(key)
            except PoolContractError:
                composed = None
        if composed is None:
            return {"state": "map_not_composed", "map_path": None,
                    "leads": [str(lead) for lead in leads]}
        try:
            present = composed.exists()
        except OSError as exc:
            # ``Path.exists`` answers False for ENOENT and ENOTDIR and *raises*
            # for everything else, and everything else is what this mount
            # does: the fleet sees RDMA remote-access errors on a roughly
            # quarter-hour cadence (#575), and ESTALE or EIO out of a stat
            # would leave the claim scan through an exception no caller
            # handles -- one stalled lookup taking down a box's whole scan.
            # A stall is not a verdict, so it becomes a denial of its own
            # rather than either a refusal to serve or, worse, a silent
            # admission onto a map nobody could read.
            return {"state": "map_unreadable", "map_path": str(composed),
                    "error": str(exc),
                    "leads": [str(lead) for lead in leads]}
        if not present:
            # Not always the ordinary race.  ``map_not_composed`` says "the
            # tier loop has not got to it yet", which is a wait of one cycle;
            # a plan the coordinator refuses is a map nobody will ever
            # compose, and the two must not read the same to whoever is
            # looking at a queue that has stopped moving (#615).
            unreadable = self._residency_plan_refusal(key)
            if unreadable is not None:
                return {"state": "plan_unreadable", "map_path": str(composed),
                        "error": unreadable,
                        "leads": [str(lead) for lead in leads]}
            return {"state": "map_not_composed", "map_path": str(composed),
                    "leads": [str(lead) for lead in leads]}
        # A map that exists is not yet a map that answers.  The loop composes
        # once a cycle from the fragments on disk, so between a lead pinning
        # its range and the next cycle the document is real, readable, and
        # missing exactly the range this gate just certified -- and the faster
        # the mover, the more certain that is, because admission waits for the
        # pin and the pin is what the composed map does not know about yet.
        # Admitting there is the full-cost pool read wearing a clean receipt
        # that ``map_not_composed`` exists to prevent (#634).  ``compose``
        # names the movers whose fragments it merged, so the map answers this
        # itself; an adopted lead files a fragment under its own key too, so
        # the ranges nobody had to copy are in that list as well.
        try:
            document = residency_map.read_map(composed)
        except (OSError, residency_map.ResidencyMapError) as exc:
            # Same reasoning as ``map_unreadable`` above: a document this
            # reader cannot parse is not a verdict either way, and must not
            # take the box's whole claim scan down with it.
            return {"state": "map_unreadable", "map_path": str(composed),
                    "error": str(exc),
                    "leads": [str(lead) for lead in leads]}
        composed_leads = set(document.get("leads") or ())
        missing = [str(lead) for lead in leads if str(lead) not in composed_leads]
        if missing:
            return {"state": "map_stale", "map_path": str(composed),
                    "missing_leads": missing,
                    "generation": document.get("generation"),
                    "leads": [str(lead) for lead in leads]}
        # A ram overlay is resident only within the epoch it landed under
        # (#640).  tmpfs empties on reboot while the map survives, so a map
        # still naming ram paths is compared against the epoch the ram tier
        # announces *now*; a mismatch is the same one-cycle wait as
        # ``map_not_composed``, because the loop drops every prior-epoch
        # fragment and recomposes from what survives.  A map that names no
        # ram range never asks the question: the stage residency the verdict
        # has always gated on is durable and checkable, and the ram tier is a
        # performance tier in front of it.
        ram_epoch = document.get("ram_epoch")
        if isinstance(ram_epoch, str) and ram_epoch:
            ram_tier_id = str(document.get("ram_tier_id") or "")
            announced_epoch = None
            for record in self.tiers():
                if (str(record.get("tier_id")) == ram_tier_id
                        and record.get("tier") == "ram"):
                    announced_epoch = str(record.get("epoch") or "")
                    break
            if announced_epoch != ram_epoch:
                return {"state": "ram_epoch_stale", "map_path": str(composed),
                        "ram_tier_id": ram_tier_id, "ram_epoch": ram_epoch,
                        "leads": [str(lead) for lead in leads]}
        return {"state": "resident", "leads": [str(lead) for lead in leads],
                "map_path": str(composed)}

    def _residency_plan_refusal(self, consumer_action_key: object) -> str | None:
        """Why this consumer's frozen plan will not validate, or ``None``.

        ``None`` for the two ordinary answers -- no plan filed, or one that
        reads -- so this only ever turns a wait into a refusal, never the
        other way.  The import is local because ``residency_plan`` imports
        this module: the plan schema is built on the queue's own paths, and
        the queue needs the schema only at this one call.
        """

        if not isinstance(consumer_action_key, str):
            return None
        from . import residency_plan

        refusals: list[Exception] = []
        residency_plan.read(self, consumer_action_key,
                            on_unreadable=refusals.append)
        return repr(refusals[0]) if refusals else None

    def _lead_was_adopted(self, residency: Mapping[str, object], lead: str) -> bool:
        """Did this lead take over a range that was already on the tier (#598)?

        An adopted mover is never published and never claimed, so it files no
        terminal record and the ``done/`` read below it answers ``absent``.
        What it does file is a move receipt naming the mover it took the range
        from, and what it holds is that mover's tier tokens.  Both are
        required here: the receipt alone would let a range that has since been
        evicted read as resident, exactly as ``executed`` alone does for a
        mover that copied nothing.

        The manifest and the tier are checked for the reason the ``executed``
        branch checks them -- a deterministic descriptor is what lets a
        consumer bind a result before it exists, so a receipt about some other
        manifest is a trap the binding invites rather than an impossibility.
        """

        receipt = self.move_record(str(lead))
        if not isinstance(receipt, Mapping):
            return False
        if not receipt.get(MOVE_ADOPTED_FROM_FIELD):
            return False
        if receipt.get("refusal") or receipt.get("complete") is not True:
            return False
        wanted = residency.get("manifest_sha256")
        if wanted is not None and receipt.get("manifest_sha256") != wanted:
            return False
        tier_id = residency.get("tier_id")
        if tier_id is not None and receipt.get("tier_id") != tier_id:
            return False
        return self._lead_is_pinned(residency, str(lead))

    def _lead_is_pinned(self, residency: Mapping[str, object], lead: str) -> bool:
        """Does this finished lead still hold tokens for the bytes it staged?

        Contained, and ``True`` on a read failure that is not a missing
        directory: a shared-mount stall must not turn a resident lead into a
        denial and send a consumer's box off to do other work.  The gate exists
        to refuse admission onto bytes that are not there, not to refuse it
        whenever the mount hiccups.
        """

        tier_id = residency.get("tier_id")
        if not isinstance(tier_id, str) or not tier_id:
            # Nothing to check against.  A block that declares leads without a
            # tier predates the pin and is read as it was before.
            return True
        try:
            if not self.tier_ledger(tier_id).holder_tokens(lead):
                return False
        except PoolContractError:
            return True
        except OSError as exc:
            return getattr(exc, "errno", None) != errno.ENOENT
        # Tokens alone are not the pin.  They are filed under the key at
        # *claim*, before a byte is written, and only kept past ``finish``
        # when the receipt says the range landed -- so a lead that is claimed
        # right now holds tokens exactly like one that finished and pinned,
        # and a ``done`` record left by an earlier generation makes the two
        # read alike above.  2026-09-18 (#625): a consumer was admitted inside
        # one such window onto a head range whose receipt said ``complete:
        # false`` and whose 7 GB anchors file was never staged.  A claim-time
        # reservation is a promise; the pin is the receipt.
        try:
            if self.item_path(CLAIMED, lead).exists():
                return False
            receipt = self.move_record(lead)
        except PoolContractError:
            return True
        except OSError as exc:
            return getattr(exc, "errno", None) != errno.ENOENT
        # The receipt half of ``residency_pin_holds``: the copy this key's
        # tokens stand for was complete, unrefused, and onto this tier.
        return (isinstance(receipt, Mapping) and receipt.get("complete") is True
                and not receipt.get("refusal") and receipt.get("tier_id") == tier_id)

    def _transition_locked(self, action_key: str, *, blocking: bool = True):
        """Serialize one key's ownership transitions, never independent keys."""
        name = hashlib.sha256(str(action_key).encode()).hexdigest()
        return posix_lock.held(self.root / "transition-locks" / f"{name}.lock",
                               blocking=blocking)

    def container_marker(self, owner: str) -> Path:
        """The durable signal that this action invoked the Docker shim."""

        if (len(owner) != 64
                or any(character not in "0123456789abcdef" for character in owner)):
            raise PoolContractError(
                "container_owner must be a 64-character hex digest")
        return self.root / CONTAINER_OWNERS / f"{owner}.used"

    def _container_settlement(self, owner: str, unit: str) -> dict[str, object]:
        """What this holder can prove about its own Docker transaction.

        Two label queries, not one.  The owner label is the action's identity
        and spans its attempts; ``prismabuild.scope`` names this exact slice.
        The marker is the third leg: ``_cleanup_action_containers`` unlinks it
        only once its own re-query came back empty, so its absence is that
        proof rather than a separate guess.

        Whatever this returns is sent as-is.  The broker refuses an incomplete
        settlement, which is the correct outcome: a scope that still has a
        container is not settled, and nothing should pretend otherwise.
        """

        marker = self.container_marker(owner)
        return {
            "schema": resource_scope.CONTAINER_SETTLEMENT_SCHEMA,
            "marker_absent": not marker.exists(),
            "owner_container_ids": _docker_containers_with_label(
                CONTAINER_OWNER_LABEL, owner),
            "scope_container_ids": _docker_containers_with_label(
                CONTAINER_SCOPE_LABEL, unit),
            "checked_unix": _now(),
        }

    def _scope_from_record(self, record: Mapping[str, object]) -> resource_scope.ResourceScope:
        control = record.get("resource_scope")
        if not isinstance(control, dict):
            raise PoolContractError("resource scope control must be an object")
        key = str(record.get("action_key") or "")
        if (record.get("claimed_host") or record.get("host")) != socket.gethostname():
            raise PoolContractError("resource scope cleanup must run on its claiming host")
        nonce = control.get("nonce")
        unit = "prismabuild-job" + hashlib.sha256(
            (key + str(nonce)).encode()).hexdigest()[:32] + ".slice"
        if (control.get("action_key") != key or control.get("scope_id") != unit
                or control.get("cgroup_path") != "/sys/fs/cgroup/prismabuild.slice/" + unit
                or control.get("socket_path") != str(resource_scope.BROKER_SOCKET)
                or not isinstance(control.get("token"), str)
                or len(control["token"]) != 64
                or any(c not in "0123456789abcdef" for c in control["token"])):
            raise PoolContractError("invalid resource scope recovery identity")
        scope = resource_scope.ResourceScope(
            key, nonce, control.get("memory_max_bytes"),
            self.ledger().base / "telemetry" / f"{key}.json",
            authority_path=cpu_admission.local_telemetry_path(self.ledger().base, key),
            docker_owner=record.get("container_owner"),
            shape_key=cpu_admission.shape_key(record) if record.get("cas_root") else None,
            **({"gpu_memory_max_bytes": control["gpu_memory_max_bytes"]}
               if control.get("gpu_memory_max_bytes") is not None else {}),
            # The validated control value, not the module default: identical
            # in production (the check above enforces it) and the only way
            # a recovered scope reaches its own broker anywhere else.
            socket_path=Path(control["socket_path"]),
        )
        scope.unit, scope.token = unit, control["token"]
        scope.cgroup_path = Path(control["cgroup_path"])
        started = control.get("started_monotonic")
        valid = (not control.get("create_recovered") and type(started) in (int, float) and math.isfinite(started)
                 and 0 <= started <= time.monotonic()
                 and control.get("boot_id") == Path("/proc/sys/kernel/random/boot_id").read_text().strip())
        # A reboot invalidates elapsed-time accounting, not exact broker
        # authority. Recovery must still stop/release the old scope safely.
        scope._pool_accounting_valid = valid
        if valid:
            scope.started = started
        return scope

    @_serialized_key
    def _recover_resource_scope_creation(self, record: Mapping[str, object]) -> bool:
        """Reconcile durable pre-create identity; never create a kernel group."""
        intent = record.get("resource_scope_intent")
        key = str(record.get("action_key") or "")
        if (not isinstance(intent, dict) or intent.get("action_key") != key
                or intent.get("socket_path") != str(resource_scope.BROKER_SOCKET)
                or (record.get("claimed_host") or record.get("host")) != socket.gethostname()):
            raise PoolContractError("invalid resource scope creation recovery identity")
        scope = resource_scope.ResourceScope(
            key, intent.get("nonce"), intent.get("memory_max_bytes"),
            self.ledger().base / "telemetry" / f"{key}.json",
            authority_path=cpu_admission.local_telemetry_path(self.ledger().base, key),
            docker_owner=record.get("container_owner"),
            **({"gpu_memory_max_bytes": intent["gpu_memory_max_bytes"]}
               if intent.get("gpu_memory_max_bytes") is not None else {}),
        )
        if not scope.recover_create():
            return False
        control = {**scope.control_record(), "create_recovered": True}
        path = self.item_path(CLAIMED, key)
        live = _read_json(path)
        if live is not None and _same_claim(live, record):
            # A recovered broker reply must not overwrite a successor before
            # the subsequent heartbeat detects its contradictory lease.
            _check_claim_lease_identity(key, live, _read_json(self.lease_path(key)))
            live["resource_scope"] = control
            _write_json_atomic(path, live)
            self.write_lease(key, owner=str(record.get("claimed_by") or ""),
                             claim_snapshot=record, container_owner=record.get("container_owner"))
        if not isinstance(record, dict):
            raise PoolContractError("resource scope recovery record must be mutable")
        record["resource_scope"] = control
        return True

    @staticmethod
    def _sample_resource_scope(scope: resource_scope.ResourceScope) -> dict:
        telemetry = scope.sample()
        framebuffer = getattr(scope, "_framebuffer_window", None)
        if isinstance(framebuffer, box_window.DiscreteFramebufferWindow):
            # Read the broker's already-published public sample beside the
            # existing exact-scope sampler.  This is deliberately a read, not
            # a per-action HIP/NVML probe; the broker remains the producer.
            try:
                framebuffer.observe(gpu_admission.trusted_sample(), now=_now())
            except Exception as exc:                             # noqa: BLE001
                # GPU-window telemetry is descriptive. A broken public read
                # must neither stop the payload nor discard prior samples.
                telemetry["gpu_framebuffer_error"] = (
                    f"broker GPU capacity snapshot unavailable: {type(exc).__name__}")
        try:
            status = scope._request("status")
            if status.get("stop_reason"):
                telemetry["termination_reason"] = status["stop_reason"]
            if status.get("termination_evidence"):
                telemetry["termination_evidence"] = status["termination_evidence"]
        except (OSError, ValueError) as exc:
            telemetry["complete"] = False
            telemetry["errors"] = [*telemetry.get("errors", []),
                                   f"scope broker status unavailable: {exc}"]
        if not getattr(scope, "_pool_accounting_valid", True):
            telemetry["complete"] = False
            telemetry["errors"] = [*telemetry.get("errors", []),
                                   "scope accounting start belongs to another boot or is invalid"]
        scope.write_telemetry(telemetry)
        return telemetry

    @staticmethod
    def _resource_failure(telemetry: Mapping[str, object]) -> str | None:
        if telemetry.get("termination_evidence") and telemetry.get("termination_reason"):
            return str(telemetry["termination_reason"])
        # Descendant OOM victims do not prove this attempt exhausted its cap.
        # Parent-local OOM does, even before the kernel accounts a victim.
        if telemetry.get("oom_local", 0) > 0:
            return "memory_limit_oom"
        return None

    @_serialized_key
    def _start_resource_scope(self, item: Mapping[str, object]) -> resource_scope.ResourceScope:
        key = str(item["action_key"])
        request = Path(str(item["cas_root"])) / "requests" / key[:2] / f"{key}.json"
        raw = pb._read_regular_file_nofollow(request, where="contained pool action request")
        action = pb.validate_action(pb._decode_strict_json(raw, where="contained pool action request"))
        demand = action["params"].get("demand")
        if action["action_key"] != key:
            raise PoolContractError("contained action request differs from claimed key")
        if demand is None and action["task"]["definition_id"] != "fleet/pbrun":
            # Existing generic producers (including Tessera) declare resources
            # through publish rather than action params. Preserve that trusted
            # producer contract; pbrun always binds demand into the sealed key.
            demand = item.get("resources")
        if not isinstance(demand, dict) or demand != item.get("resources"):
            raise PoolContractError("pool resource demand differs from sealed action demand")
        memory = demand.get("mem_gb")
        if type(memory) is not int or memory <= 0:
            raise PoolContractError("contained action needs a positive sealed mem_gb demand")
        gpu_memory = action["params"].get("gpu_memory_gb")
        gpu_kwargs = {}
        if gpu_memory is not None:
            if not demand.get("gpu"):
                raise PoolContractError("gpu_memory_gb requires GPU demand")
            try:
                gpu_kwargs["gpu_memory_max_bytes"] = gpu_admission.memory_budget_bytes(gpu_memory)
            except ValueError as exc:
                raise PoolContractError(f"gpu_memory_gb: {exc}") from exc
        if item.get("resource_scope") is not None or item.get("resource_scope_intent") is not None:
            raise PoolContractError("claim already owns a resource scope or creation intent")
        scope = resource_scope.ResourceScope(
            key, uuid.uuid4().hex, memory * 1024 ** 3,
            self.ledger().base / "telemetry" / f"{key}.json",
            authority_path=cpu_admission.local_telemetry_path(self.ledger().base, key),
            docker_owner=item.get("container_owner"),
            shape_key=cpu_admission.shape_key(item),
            **gpu_kwargs,
        )
        path = self.item_path(CLAIMED, key)
        live = _read_json(path)
        if live is None or not _same_claim(live, item):
            raise PoolContractError("claim changed before scope creation")
        # Heartbeat validation happens after the claim write. Refuse a stale
        # caller here so that write cannot first erase a successor's scope.
        _check_claim_lease_identity(key, live, _read_json(self.lease_path(key)))
        intent = {"action_key": key, "nonce": scope.nonce,
                  "memory_max_bytes": scope.memory_max_bytes,
                  "socket_path": str(scope.socket_path), **gpu_kwargs}
        live["resource_scope_intent"] = intent
        _write_json_atomic(path, live)
        if isinstance(item, dict):
            item["resource_scope_intent"] = intent
        self.write_lease(key, owner=str(item.get("claimed_by") or ""),
                         claim_snapshot=item, container_owner=item.get("container_owner"))
        # The broker may finish after a client timeout or worker crash. Both
        # claim and lease now retain the exact nonce needed for reconciliation.
        control = scope.create()
        if demand.get("gpu"):
            # Kept on the live scope only.  The broker has now validated the
            # canonical scope identity; a reconstructed scope at finish
            # cannot turn its one final snapshot into an action-time peak.
            scope._framebuffer_window = box_window.DiscreteFramebufferWindow(
                key, scope.nonce, scope.unit, start_unix=_now())
        control["started_monotonic"] = scope.started
        control["boot_id"] = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        try:
            live = _read_json(path)
            if live is None or not _same_claim(live, item):
                raise PoolContractError("claim changed before scope launch")
            _check_claim_lease_identity(key, live, _read_json(self.lease_path(key)))
            live["resource_scope"] = control
            _write_json_atomic(path, live)
            if isinstance(item, dict):
                item["resource_scope"] = control
            self.write_lease(key, owner=str(item.get("claimed_by") or ""),
                             claim_snapshot=item, container_owner=item.get("container_owner"))
        except BaseException:
            # Nothing has launched yet, so this scope can be stopped without
            # any process census. Ownership may now belong to a successor;
            # retain our stop marker only in this attempt's diagnostic archive.
            # Broker failures remain visible to recovery.
            scope.telemetry_path = (scope.telemetry_path.parent / "attempts"
                                    / scope.nonce / scope.telemetry_path.name)
            scope.authority_path = None
            scope.terminate_owned("scope ownership could not be persisted")
            scope.release()
            raise
        return scope

    @_serialized_key
    def _persist_reader_scope_proof(self, record: Mapping[str, object],
                                      nonce: str, scope_id: str,
                                      export: object) -> bool:
        """File the broker's export verdict for reader containment.

        Called with the token-gated ``export_stopped`` verdict in hand
        (pool resource-scope cleanup owns this hunk, not the membership
        retry branch) -- never the release reply, which carries no proof
        on first success.  ``scope_empty`` is True ONLY for a complete
        authoritative proof under
        :func:`reader_lease.export_verdict_proves_empty`: the verdict's
        own scope id names this scope, stopped time is positive finite,
        empty is exactly True, tickets_pending is exactly False (missing
        is unknown, never proof of none), and release/retirement are
        exact booleans proving a clean release or a settled retirement.
        Anything else files False (retain) or nothing at all, storing
        the verdict's raw fields so readers re-validate rather than
        trusting the flag.  Never manufactures true from a helper's
        return alone.  Host is the PB-qualified claim holder (fleet
        alias, never the local hostname); worker and incarnation are the
        claim's full ``claimed_by`` holder identity (repository
        convention), matching what SDK refs record.  Best-effort:
        returns whether a proof file was filed; the caller never fails a
        cleanup over it.
        """

        from prismabuild import reader_lease

        action_key = str(record.get("action_key") or "")
        if len(action_key) != 64 or not nonce or not scope_id:
            return False
        verdict = export if isinstance(export, Mapping) else {}
        proven, _reason = reader_lease.export_verdict_proves_empty(
            verdict, scope_id=scope_id)
        try:
            host = self.resolve_claim_holder(action_key, record)
        except (AttributeError, OSError, ValueError):
            host = None
        worker = record.get("claimed_by")
        payload = {
            "schema": reader_lease.ATTESTATION_SCHEMA_V1,
            "action_key": action_key,
            "nonce": nonce,
            "scope_id": scope_id,
            "host": host if isinstance(host, str) and host else "",
            "worker": worker if isinstance(worker, str) else "",
            "incarnation": worker if isinstance(worker, str) else "",
            "scope_empty": bool(proven),
            "released": verdict.get("released"),
            "retired": verdict.get("retired"),
            "settled": verdict.get("settled"),
            "empty": verdict.get("empty"),
            "tickets_pending": verdict.get("tickets_pending"),
            "stopped_unix": verdict.get("stopped_unix"),
            "termination_evidence": (
                dict(verdict["termination_evidence"])
                if isinstance(verdict.get("termination_evidence"), Mapping)
                else None),
            "unix": time.time(),
        }
        if not payload["host"] or not payload["worker"]:
            return False
        path = reader_lease.attestation_path(self, action_key, nonce)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        with open(tmp, "w") as stream:
            json.dump(payload, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        return True

    def _recover_reader_scope_proof(self, record: Mapping[str, object],
                                      prior: Mapping[str, object]) -> None:
        """Republish proof from broker export evidence for the exact attempt.

        The ``prior.complete`` shortcut returns without touching the
        broker, so a proof lost to a shared-mount blip while the CLAIMED
        row still stands would stay lost until the row goes away.
        Recovery republishes in two tiers, preferring no broker contact:

        1. A validated-complete stored export -- the prior cleanup
           persisted the token-gated verdict beside the release reply --
           republishes with no broker RPC at all.  The repeat cleanup
           stays effect-idempotent: no re-stop, no re-release, no new
           nonce.
        2. Otherwise a fresh token-gated export through the scope
           reconstructed from this claim row (same as any cleanup),
           covering a settlement that completed after the prior ran.

        Never a replay of a possibly stale prior beyond its validated
        export, and never anything involving capability tokens outside
        the broker RPC itself.  An already-filed validated-complete
        proof (exact attempt, exact export booleans, positive finite
        stop -- not any dict reading ``scope_empty`` True) is left
        alone; best-effort throughout: export failure retains silently.
        Once ``finish`` files the terminal, this path has no row left
        to run from, and the egress tick's terminal-export replay owns
        recovery instead.
        """

        from prismabuild import reader_lease

        action_key = str(record.get("action_key") or "")
        nonce = str(prior.get("nonce") or "")
        control = record.get("resource_scope")
        unit = (control.get("scope_id") if isinstance(control, Mapping)
                else None)
        if len(action_key) != 64 or not nonce or not unit:
            return
        try:
            ok, _proof = reader_lease.attestation_proves_empty(
                self, action_key, nonce, str(unit))
        except Exception:                                        # noqa: BLE001
            ok = False
        if ok:
            return
        stored = prior.get("export")
        if isinstance(stored, Mapping):
            try:
                valid, _reason = reader_lease.export_verdict_proves_empty(
                    stored, scope_id=str(unit))
            except Exception:                                    # noqa: BLE001
                valid = False
            if valid:
                try:
                    self._persist_reader_scope_proof(
                        record, nonce, str(unit), stored)
                except Exception:                                # noqa: BLE001
                    pass
                return
        try:
            scope = self._scope_from_record(record)
        except Exception:                                        # noqa: BLE001
            return
        if scope.nonce != nonce:
            return
        try:
            export = scope.export_stopped_verdict()
        except Exception:                                        # noqa: BLE001
            return
        try:
            self._persist_reader_scope_proof(
                record, nonce, str(unit), export)
        except Exception:                                        # noqa: BLE001
            pass

    def cleanup_action_containers(
        self, record: Mapping[str, object], *, reason: str = "completion",
        scope_only: bool = False,
    ) -> dict[str, object]:
        """Prove payload cleanup, optionally limited to one replaced scope.

        A late finisher owns its broker nonce, never the action-wide Docker
        owner or the successor's live telemetry and reservation.
        """
        if scope_only:
            try:
                live = _read_json(self.item_path(CLAIMED, str(record["action_key"])))
                if live is not None and not _same_claim(live, record):
                    old = record.get("resource_scope") or record.get("resource_scope_intent")
                    if not isinstance(old, dict) or not old.get("nonce"):
                        raise PoolContractError("late finish has no exact scope authority")
                    if any(isinstance(live.get(field), dict)
                           and live[field].get("nonce") == old["nonce"]
                           for field in ("resource_scope", "resource_scope_intent")):
                        raise PoolContractError("late scope identity also belongs to the live claim")
            except Exception as exc:                                 # noqa: BLE001
                return {"complete": False, "used": True, "removed": [], "remaining": [],
                        "error": f"late scope ownership unavailable: {type(exc).__name__}: {exc}"}
        if record.get("resource_scope") is None and record.get("resource_scope_intent") is not None:
            try:
                self._recover_resource_scope_creation(record)
            except Exception as exc:                                 # noqa: BLE001
                # The third enumerated tuple on this path, and it is retired
                # for the reason the other two were (#286, #288): the list is
                # of the errors somebody thought of, and this call reaches the
                # broker socket, the shared mount and JSON.  An escape here is
                # fail-OPEN in the expensive direction -- the caller never
                # receives the dict it indexes ``["complete"]`` on, so the
                # claim is never concluded and the lease decays to
                # ``lease_lost_max_attempts`` for a payload that already ran.
                #
                # ``Exception`` and not ``BaseException``: a KeyboardInterrupt
                # or SystemExit still stops the process.
                return {"complete": False, "used": True, "removed": [], "remaining": [],
                        "error": f"resource scope creation reconciliation incomplete: {type(exc).__name__}: {exc}"}
        if record.get("resource_scope") is None:
            if scope_only:
                return {"complete": True, "used": False, "removed": [], "remaining": []}
            return self._cleanup_action_containers(record)
        try:
            prior = record.get("resource_scope_cleanup")
            if (isinstance(prior, dict) and prior.get("complete") is True
                    and prior.get("nonce") == record["resource_scope"].get("nonce")):
                # The shortcut must not permanently bypass proof
                # publication: if the shared mount blipped while filing,
                # cleanup recorded complete and future calls return here.
                # Recover from the prior's authoritative broker verdict
                # for this exact attempt (no tokens involved anywhere).
                try:
                    self._recover_reader_scope_proof(record, prior)
                except Exception:                                # noqa: BLE001
                    pass
                return {"complete": True, "used": True, "removed": [], "remaining": [],
                        "resource_scope": prior}
            scope = self._scope_from_record(record)
            if scope_only:
                scope.telemetry_path = (scope.telemetry_path.parent / "attempts"
                                        / scope.nonce / scope.telemetry_path.name)
                # The live host-local record belongs to the successor attempt
                # of this key; a predecessor's cleanup sample must not become
                # the attribution admission credits to the live holder.
                scope.authority_path = None
            scope.terminate_owned(reason)
            containers = ({"complete": True, "used": True, "removed": [], "remaining": []}
                          if scope_only else self._cleanup_action_containers(record))
            if not containers["complete"]:
                return containers
            telemetry = self._sample_resource_scope(scope)
            settle_error: str | None = None
            if not scope_only and record.get("container_owner"):
                # Before release, because release is where the broker decides
                # between removing this scope and retaining it frozen: a ticket
                # the shim could not resolve -- an ordinary nonzero ``docker``
                # exit is enough -- makes it keep an empty frozen parent for a
                # container that might still arrive.  Settlement is the holder
                # saying none can, and it is the only evidence that lets the
                # broker's inventory pass ever take that parent away (#486).
                #
                # Its own handler, and deliberately not the outer one.  This is
                # housekeeping for a payload that has already stopped: if the
                # broker refuses or never hears it, the tombstone is retained
                # exactly as it is today.  Letting it reach the outer handler
                # would answer ``complete: False`` and pin a claim and its
                # tokens on a failed cleanup of somebody's memory charge, which
                # is the fail-OPEN trade the comments above refuse to make.
                #
                # ``Exception`` and not ``BaseException``: a KeyboardInterrupt
                # or SystemExit still stops the process.
                try:
                    scope.settle_containers(self._container_settlement(
                        str(record["container_owner"]), str(scope.unit)))
                except Exception as exc:                             # noqa: BLE001
                    settle_error = f"{type(exc).__name__}: {exc}"
            released = scope.release()
            # The release reply is NOT the proof (the first successful
            # release carries no released flag, and ticket retirement
            # carries no stopped/settled fields): read the token-gated
            # export verdict through the same scope and persist THAT.
            # Best-effort and contained: proof persistence must never fail
            # a cleanup that already proved emptiness -- a missing file
            # retains, exactly as before.
            try:
                export = scope.export_stopped_verdict()
            except Exception:
                export = None
            try:
                self._persist_reader_scope_proof(
                    record, scope.nonce, scope.unit, export)
            except Exception as exc:                             # noqa: BLE001
                telemetry = dict(telemetry) if isinstance(telemetry, Mapping) else {}
                telemetry.setdefault(
                    "proof_persistence_error",
                    f"{type(exc).__name__}: {exc}")
            # Reader-containment hold: an export that cannot prove this
            # attempt contained must not conclude a claim whose readers
            # still pin bytes.  Return incomplete so the claim -- with
            # its finish_pending authority and its charge -- survives
            # for the existing worker reaper to retry once settlement
            # lands; only a proven export, or explicitly released refs,
            # lets the terminal publish.  A positive export with a lost
            # proof file still completes here (the egress tick replays
            # the persisted export), so this holds exactly the unproven.
            from prismabuild import reader_lease
            proof_ok, proof_reason = (
                reader_lease.export_verdict_proves_empty(
                    export if isinstance(export, Mapping) else {},
                    scope_id=scope.unit))
            if not proof_ok:
                held, hold_reason = reader_lease.attempt_refs_live(
                    self, str(record.get("action_key") or ""),
                    scope.nonce, scope.unit)
                if held:
                    return {"complete": False, "used": True,
                            "removed": [], "remaining": [],
                            "error": f"reader refs live, containment "
                                     f"unproven ({proof_reason}; "
                                     f"{hold_reason})",
                            "nonce": scope.nonce, "export": export}
            if scope.authority_path is not None:
                # The scope is empty: nothing will sample it again, and no
                # holder remains for admission to attribute it to. The shared
                # copy stays as the attempt's last observation.
                scope.authority_path.unlink(missing_ok=True)
            cleanup = {"complete": True, "released": released, "telemetry": telemetry,
                       "checked_unix": _now(), "nonce": scope.nonce,
                       "export": export}
            if settle_error is not None:
                cleanup["settle_error"] = settle_error
            key = str(record["action_key"])
            path = self.item_path(CLAIMED, key)
            live = _read_json(path)
            if not scope_only and live is not None and _same_claim(live, record):
                live["resource_scope_cleanup"] = cleanup
                _write_json_atomic(path, live)
            if isinstance(record, dict):
                record["resource_scope_cleanup"] = cleanup
            if scope_only:
                # A delayed cleanup is not a fresh runtime measurement and
                # must not train the live admission model for this action.
                return {**containers, "resource_scope": cleanup}
            try:
                cpu_admission.record_completion(self.ledger(), record, telemetry)
            except Exception as exc:                                 # noqa: BLE001
                # Deliberately every exception, and the narrow tuple that was
                # here is the defect.  By this line the payload has stopped,
                # the tokens are back and the cleanup record is written; all
                # that is left is learning a shape, and ``record_completion``
                # says of itself that it is "worth having and never worth
                # waiting for", with "failure to attribute produces no learned
                # credit" as its own contract.  A call never worth waiting for
                # is never worth losing an action over.
                #
                # Enumerating what it can raise is what failed.  It reaches a
                # whole subsystem -- the admission lock, ``/proc``, the shared
                # mount, JSON -- and two of that subsystem's honest refusals
                # are bare ``RuntimeError``: ``box_state`` on a directory this
                # uid does not own, and ``Controller.locked`` on a lock file
                # that is not a private regular file.  ``AdmissionBusy`` is a
                # *subclass* of ``RuntimeError`` and is caught inside, which is
                # exactly what made the gap easy to miss.
                #
                # Neither was in the tuple, so the raise escaped this method
                # after ``scope.release()`` and before the caller could finish
                # the claim: the payload had completed, the claim had not, and
                # the lease stopped being renewed until the reaper recorded
                # ``lease_lost_max_attempts``.  Observed on sparky and
                # dl380g10 on 2026-09-06 while their admission directories
                # were mode 0770 (#281, #286).
                #
                # ``Exception`` and not ``BaseException``: a KeyboardInterrupt
                # or SystemExit still stops the process.
                cleanup["learning_error"] = f"{type(exc).__name__}: {exc}"
            return {**containers, "resource_scope": cleanup}
        except Exception as exc:                                     # noqa: BLE001
            # Every exception, and for the opposite reason to the inner
            # handler above.  This block is the part that PROVES the payload
            # stopped -- the resource broker over a socket, Docker, the shared
            # mount -- and an unexpected raise here escaped the method
            # entirely.  All four callers (``finish``, ``reap_stale``, the
            # lease sweep, ``withdraw``) index ``["complete"]`` on a dict they
            # then never receive, so the payload had run, the claim was never
            # concluded, the lease stopped being renewed, and the reaper
            # recorded ``lease_lost_max_attempts``.  That is fail-OPEN in the
            # way that costs the work (#286, #288).
            #
            # ``complete: False`` is the honest answer instead: cleanup could
            # not be proved.  It is fail-closed -- the claim and its tokens are
            # retained and a local reaper retries -- and it is deliberately NOT
            # a decision to release capacity for a payload nobody has shown to
            # have stopped.  A GPU an action still holds must not be handed to
            # somebody else because the box gave up asking.
            #
            # What the old crash bought was a signal: it ran up
            # ``MAX_CONSECUTIVE_ERRORS`` and took the box out of service.  That
            # signal is replaced rather than dropped -- ``_note_cleanup_attempt``
            # counts the retries and dates the first failure, so a cleanup that
            # can never succeed is a visible pinned claim instead of an
            # invisible one.  Relying on the crash was relying on a handler
            # written for bad ITEMS, which had already misfired once: a 0770
            # admission directory made every box run that counter up while
            # announcing full capacity (#281).
            #
            # ``Exception`` and not ``BaseException``: a KeyboardInterrupt or
            # SystemExit still stops the process.
            return {"complete": False, "used": True, "removed": [], "remaining": [],
                    "error": f"resource scope cleanup incomplete: {type(exc).__name__}: {exc}"}

    @staticmethod
    def _note_cleanup_attempt(
        pending: dict[str, object], prior: Mapping[str, object] | None,
        cleanup: Mapping[str, object],
    ) -> None:
        """Record that cleanup was tried again and still could not be proved.

        One writer for all three sites that retain a claim on unproven cleanup
        (``finish``, ``reap_stale``, ``withdraw``), because a count only two of
        them increment measures nothing.

        The two numbers answer the question the retry loop cannot answer about
        itself: a cleanup pending for three seconds and one pending for six
        hours and four hundred attempts write the same ``container_cleanup_
        pending`` record, and an operator acts on them completely differently.
        ``container_cleanup_checked_unix`` already said when it was last tried,
        which is the one thing that is always recent.

        No bound is applied here on purpose.  Concluding such a claim means
        releasing tokens for a payload nobody proved had stopped, and that is a
        fleet policy decision about hardware, not a defect fix -- see #288.
        What this makes possible is deciding it on evidence.
        """

        attempts = (prior or {}).get("container_cleanup_attempts")
        pending["container_cleanup_attempts"] = (
            int(attempts) + 1 if isinstance(attempts, int) and not isinstance(attempts, bool)
            else 1)
        first = (prior or {}).get("container_cleanup_first_failed_unix")
        pending["container_cleanup_first_failed_unix"] = (
            float(first) if isinstance(first, (int, float)) and not isinstance(first, bool)
            else _now())
        pending["container_cleanup_pending"] = dict(cleanup)
        pending["container_cleanup_checked_unix"] = _now()

    def _cleanup_action_containers(
        self, record: Mapping[str, object]
    ) -> dict[str, object]:
        """Remove and verify this action's detached Docker payloads.

        No marker means the Docker shim was never entered.  Once it exists,
        uncertainty is fail-closed: only the claiming host may consult its
        local daemon, an in-flight shim lock is not raced, and every query or
        removal error leaves the claim and reservation in place.
        """

        raw_owner = record.get("container_owner")
        if raw_owner is None:
            return {"complete": True, "used": False, "removed": [], "remaining": []}

        descriptor: int | None = None
        # The boundary starts here, not at ``os.open``.  Everything between
        # this line and the payload proof reads the shared mount -- the marker
        # path, its stat, the hostname -- and a raise from any of it used to
        # leave this method entirely.  ``marker.exists()`` was the live one:
        # ``Path.exists`` re-raises an errno outside ``ENOENT/ENOTDIR/EBADF/
        # ELOOP``, and ESTALE on an NFS handle is outside it, so a stale marker
        # handle escaped rather than answering ``complete: False``.  This is
        # the same fail-OPEN shape as #286/#288 in the sibling path: all four
        # callers index ``["complete"]`` on a dict they never receive, so the
        # payload has run, the claim is never concluded, and the lease decays
        # to ``lease_lost_max_attempts``.  The no-scope path reaches here
        # through ``cleanup_action_containers``'s early return, *outside* that
        # method's broad handler, so this is where it has to be caught.
        try:
            try:
                marker = self.container_marker(str(raw_owner))
            except PoolContractError as exc:
                return {
                    "complete": False,
                    "used": True,
                    "removed": [],
                    "remaining": [],
                    "error": str(exc),
                }
            if not marker.exists():
                return {"complete": True, "used": False, "removed": [], "remaining": []}

            holder = record.get("claimed_host") or record.get("host")
            local = socket.gethostname()
            if isinstance(holder, str) and holder and holder != local:
                return {
                    "complete": False,
                    "used": True,
                    "removed": [],
                    "remaining": [],
                    "error": f"container belongs to {holder}; cleanup must run there",
                }

            descriptor = os.open(
                marker,
                os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return {
                    "complete": False,
                    "used": True,
                    "removed": [],
                    "remaining": [],
                    "error": "Docker ownership transaction is still running",
                }
            before = _docker_owned_container_ids(str(raw_owner))
            removed = _docker_remove_containers(before)
            remaining = _docker_owned_container_ids(str(raw_owner))
            complete = not remaining
            if complete:
                marker.unlink(missing_ok=True)
            return {
                "complete": complete,
                "used": True,
                "removed": removed,
                "remaining": remaining,
            }
        except Exception as exc:                                     # noqa: BLE001
            # Every exception, for the reason the sibling path already records
            # (#286, #288): this block is the part that PROVES the payload
            # stopped -- Docker, the shared mount, a marker stat -- and the
            # enumerated tuple was written against the errors somebody thought
            # of.  ``complete: False`` is the honest answer to any of them:
            # the claim and its tokens are retained, ``_note_cleanup_attempt``
            # counts the retry, and nothing releases capacity for a payload
            # nobody has shown to have stopped.
            #
            # ``Exception`` and not ``BaseException``: a KeyboardInterrupt or
            # SystemExit still stops the process.
            return {
                "complete": False,
                "used": True,
                "removed": [],
                "remaining": [],
                "error": f"{type(exc).__name__}: {exc}",
            }
        finally:
            if descriptor is not None:
                os.close(descriptor)

    @staticmethod
    def demand_of(item: Mapping[str, object]) -> dict[str, int]:
        raw = item.get("resources") or {}
        if not isinstance(raw, Mapping):
            raise PoolContractError("pool item resources must be an object")
        return {str(k): int(v) for k, v in raw.items() if int(v) > 0}

    def _defer_fallback(self, item: Mapping, demand: Mapping) -> dict[str, object] | None:
        """Give a compatible host with free preferred CPUs up to 20s to claim.

        Offers and remote ledger scans are advisory snapshots, not an atomic
        fleet allocation. The bounded wait prevents stale-but-fresh offers
        from stranding work. A host that cannot fit the whole demand never
        delays another host, nor does incompatible placement.
        """
        identity = (str(item["action_key"]), repr(item.get("published_unix")))
        started = self._cpu_deferrals.setdefault(identity, time.monotonic())
        if time.monotonic() - started >= 20.0:
            return None
        for offer in self._matching_offers(item, live=self.offers()):
            host = str(offer.get("host") or "")
            if not host or host == socket.gethostname():
                continue
            tiers = offer.get("cpu_tiers")
            if not isinstance(tiers, Mapping) or not tiers.get("preferred"):
                continue
            remote = self.ledger(host)
            if _read_json(remote.base / "cpu-map.json") != tiers:
                continue
            free = remote.available()
            observed = offer.get("observed_capacity") or {}
            free_preferred = remote.free_preferred(tiers)
            if (free_preferred >= demand.get("cpu", 0)
                    and all(free.get(k, 0) >= n and observed.get(k, free[k]) >= n
                            for k, n in demand.items())):
                return {"host": host, "free_preferred": free_preferred,
                        "available": free,
                        "observed_capacity": observed, "demand": dict(demand)}
        return None

    @staticmethod
    def _opposite_resource_load(offer: Mapping, *, gpu_job: bool) -> float | None:
        """How busy this box is on the resource the item does NOT want.

        A placement proxy and nothing more.  It is not a thermal measurement,
        it certifies no throughput, and no admission decision reads it: the
        only thing it can do is make a claimant wait a bounded moment.

        ``None`` means "not known here", which the caller reads as no
        preference.  A reading older than ``GPU_SAMPLE_MAX_AGE_S`` is not
        known: an offer file is last-writer-wins per host and a claimant must
        not prefer a box on a stale picture of either side.
        """

        detail = offer.get("observed_detail") or {}

        def number(value: object) -> bool:
            return type(value) in (int, float) and math.isfinite(value) and value >= 0

        stamp = detail.get("observed_unix")
        if not number(stamp) or not 0 <= _now() - stamp <= box_capacity.GPU_SAMPLE_MAX_AGE_S:
            return None
        if gpu_job:
            # CPU load per preferred core, so boxes with different core counts
            # compare.  Load is already what the CPU admission controller reads.
            cores = len((offer.get("cpu_tiers") or {}).get("preferred", []))
            load = detail.get("load1")
            return load / cores if cores and number(load) else None
        if not offer.get("has_gpu"):
            return 0.0            # no GPU to take power from; nothing to prefer away
        stamp = detail.get("gpu_power_sampled_unix")
        # Placement is about drawn power, not the limiter flag: an idle
        # SW-capped device draws ~3% of its SoC envelope while the legacy
        # congestion proxy reads 1.0.  Prefer the raw measured fraction when
        # the offer carries it; fall back to the legacy proxy for old offers.
        measured = detail.get("gpu_power_measured_fraction")
        legacy = detail.get("gpu_power_fraction")
        load = measured if number(measured) else legacy
        if (number(stamp) and 0 <= _now() - stamp <= box_capacity.GPU_SAMPLE_MAX_AGE_S
                and number(load)):
            return load
        return None

    # The preference's two knobs.  Both are heuristic -- they are thresholds on
    # a proxy, not quantities derived from an objective -- which is why they
    # bound a wait and never a decision.  ``BUSY`` is where a box counts as
    # working on the other resource; ``MARGIN`` is how much better an
    # alternative must look before it is worth waiting for, so that two boxes
    # reading nearly the same never take turns deferring to each other.
    CROSS_RESOURCE_BUSY = 0.60
    CROSS_RESOURCE_MARGIN = 0.20

    def _defer_cross_resource_placement(
        self, item: Mapping, demand: Mapping, *, live: Sequence,
    ) -> dict[str, object] | None:
        """Prefer not to spend a box's GPU power on work that can go elsewhere.

        Best-effort and nothing more.  When this box is already working the
        resource the item does *not* want -- GPU work arriving at a box busy
        on CPU, CPU-only work arriving at a box busy on its GPU -- and a
        compatible box looks materially freer on that axis and can fit the
        whole demand, give that box up to 20 seconds to claim.  After that
        this box claims it anyway.

        So the work is never refused, never starved and never placed worse
        than it would have been without this: the only outcome is a short wait
        that a better placement may or may not win.  If no alternative exists,
        or either reading is stale, there is no preference and the caller
        proceeds unchanged.
        """

        host = socket.gethostname()
        local = next((offer for offer in live if offer.get("host") == host), {})
        gpu_job = bool(demand.get("gpu"))
        load = self._opposite_resource_load(local, gpu_job=gpu_job)
        if load is None or load < self.CROSS_RESOURCE_BUSY:
            return None           # this box is not taking anything from anyone
        identity = (str(item["action_key"]), repr(item.get("published_unix")))
        started = self._cross_resource_deferrals.setdefault(identity, time.monotonic())
        if time.monotonic() - started >= 20.0:
            return None           # preference spent; place it here
        # Only now, behind that local check, does this read other boxes'
        # ledgers: at most one remote read per compatible offer, on the rare
        # scans where this box is genuinely cross-loaded.
        for offer in self._matching_offers(item, live=live):
            remote_host = str(offer.get("host") or "")
            if not remote_host or remote_host == host:
                continue
            remote_load = self._opposite_resource_load(offer, gpu_job=gpu_job)
            if remote_load is None or remote_load > load - self.CROSS_RESOURCE_MARGIN:
                continue
            remote = self.ledger(remote_host)
            tiers = offer.get("cpu_tiers") or {}
            if _read_json(remote.base / "cpu-map.json") != tiers:
                continue
            free = remote.available()
            observed = offer.get("observed_capacity") or {}
            if (remote.free_preferred(tiers) >= demand.get("cpu", 0)
                    and all(min(free.get(k, 0), observed.get(k, 0)) >= n
                            for k, n in demand.items())):
                return {"host": remote_host, "local_load": load,
                        "remote_load": remote_load, "gpu_job": gpu_job,
                        "available": free, "observed_capacity": observed,
                        "demand": dict(demand)}
        return None

    def claim(
        self, *, tags: Iterable[str] = (), has_gpu: bool = False,
        owner: str | None = None, capacity: Mapping[str, int] | None = None,
        cpu_tiers: Mapping[str, Sequence[int]] | None = None,
        adaptive_cpu: bool = False,
        ready: list[dict[str, object]] | None = None,
        observed_images: Container[str] | None = None,
        admission_open: Callable[[], bool] | None = None,
    ) -> dict[str, object] | None:
        """Take one ready item, atomically.  ``None`` when nothing matches.

        ``admission_open`` is an optional caller-owned fence (a worker resign
        drain): ``_claim`` re-checks it under the per-key transition lock
        immediately before the intent write, so a fence that closed after the
        poll check is still observed. ``None`` preserves current behavior.
        """
        ledger = self.ledger()
        tiers = cpu_tiers or _read_json(ledger.base / "cpu-map.json")
        if adaptive_cpu and capacity is not None and tiers is not None:
            controller = cpu_admission.Controller(ledger, tiers)
            # Resolving the ledger identity may stat the shared mount. Prepare
            # both controllers before taking the host-wide admission lock.
            gpu_controller = (gpu_admission.Controller(ledger, publisher=controller)
                              if has_gpu else None)
            evaluating = False
            try:
                # Preserve the cheap busy refusal before starting shared I/O.
                # Discovery holds no reservation and needs no host exclusion:
                # a stalled reader must not prevent a sibling from admitting.
                with controller.locked():
                    pass
                # Past the probe, a refusal can only come from ``_claim``'s
                # per-candidate lock, which is taken after evaluation has
                # begun. The same ``except`` catches both, so it has to be
                # told which one it caught rather than assert the earlier one.
                evaluating = True
                if ready is None:
                    # The worker loop prefetches this snapshot in an
                    # abandonable child and passes it in, so a wedged mount
                    # parks the child rather than this process (#16).  A
                    # caller without a snapshot scans here, in-process, as
                    # before.  Either way the list is advisory: an
                    # intervening claim wins at the rename.
                    ready = self.ready_items()
                if not ready:
                    # No candidate needs capacity reconciled on this pass. The
                    # shared ledger prelude can stall while holding admission;
                    # do not let an empty snapshot block a sibling's new work.
                    # Match _claim's retirement of absent-generation hints.
                    self._cpu_deferrals.clear()
                    self._cross_resource_deferrals.clear()
                    return None
                # ``_claim`` takes admission itself, once per candidate and only
                # around the decision that has to be exclusive. Wrapping the
                # whole of it here was the second half of #351: everything it
                # does after the decision -- the record rename that IS the
                # claim, the lease write, the token renames -- is on the shared
                # mount, so one stall in there held the host-wide lock for its
                # whole duration and every sibling loop on the box answered
                # ``None``. The box then claimed nothing at all while ready work
                # waited with free memory and a free GPU.
                return self._claim(tags=tags, has_gpu=has_gpu, owner=owner,
                                   capacity=capacity, cpu_tiers=tiers,
                                   controller=controller,
                                   gpu_controller=gpu_controller, ready=ready,
                                   observed_images=observed_images,
                                   admission_open=admission_open)
            except cpu_admission.AdmissionBusy as exc:
                # Another loop on this box is mid-decision. Waiting here means
                # waiting on a host-local lock whose holder is deciding, and
                # under the enclosing form it meant waiting on a filesystem a
                # different machine controls, with the whole box waiting too.
                #
                # ``None`` is already this method's answer for "nothing this
                # box may admit right now", and ``serve_once`` documents it as
                # back-pressure to poll against rather than an empty queue.
                # Returning it hands the loop straight back to its own poll
                # cadence, where announcing lives: the box keeps saying what
                # it is while a sibling is slow, instead of going silent and
                # letting its offer expire.
                self._report_admission_busy(exc, evaluating=evaluating)
                return None
        return self._claim(tags=tags, has_gpu=has_gpu, owner=owner,
                           capacity=capacity, cpu_tiers=cpu_tiers,
                           ready=ready, observed_images=observed_images,
                           admission_open=admission_open)

    @staticmethod
    def _admission_lock(controller: cpu_admission.Controller | None):
        """Hold host admission for the block, when there is one to hold.

        ``_claim`` runs both with and without adaptive admission -- the legacy
        path passes no controller and has no host-wide lock at all -- and the
        two must not be two spellings of the decision.  ``nullcontext`` keeps
        one body for both.
        """

        return controller.locked() if controller is not None else nullcontext()

    @staticmethod
    def _return_borrow(controller: cpu_admission.Controller | None, borrow) -> None:
        """Return a borrow whose claim did not happen, when the lock is free.

        Host-local and bounded, but it still takes the lock, because restoring
        the record has to see the record a concurrent ``admitted`` wrote.  A
        refusal here leaves the borrow spent, which only ever refuses the next
        borrow and never authorizes a second one against one sample.
        """

        if controller is None or borrow is None:
            return
        metadata, previous = borrow
        with suppress(cpu_admission.AdmissionBusy):
            with controller.locked():
                controller.withdrew(metadata, previous)

    @staticmethod
    def _return_gpu_probe(controller, gpu_controller, ticket) -> None:
        """Return ordinary unlaunched probe credit; lock contention loses credit."""
        if controller is None or gpu_controller is None or ticket is None:
            return
        with suppress(cpu_admission.AdmissionBusy):
            with controller.locked():
                gpu_controller.return_probe(ticket)

    def _report_admission_busy(self, refusal: cpu_admission.AdmissionBusy,
                               *, evaluating: bool = False) -> None:
        """Expose the admission gate that refused, without shared I/O.

        Bound output per queue instance (one per worker loop), even when the
        holder changes or acquisitions succeed between refusals. The PID is
        an observation from the failed flock, not durable process ownership.

        ``evaluating`` says which of the two gates refused, because since #351
        there are two and one caller catches both: the cheap probe before any
        shared I/O, where evaluation genuinely has not started, and ``_claim``'s
        per-candidate lock, which is taken after the candidate list is in hand.
        Reporting the first unconditionally would name the wrong gate every
        time the second one refused, which is a false reading of a diagnostic
        whose whole job is to say where the loop stopped.
        """
        now = time.monotonic()
        previous = self._admission_busy_logged_at
        if previous is not None and now - previous < 60.0:
            return
        self._admission_busy_logged_at = now
        holder = refusal.holder if refusal.holder is not None else "unknown"
        stage = ("refused while evaluating candidates" if evaluating
                 else "candidate evaluation not reached")
        try:
            print(f"pool: worker pid={os.getpid()}: host admission lock busy; "
                  f"observed holder pid={holder}; {stage}; "
                  "retrying on the normal poll cadence",
                  file=sys.stderr, flush=True)
        except (OSError, ValueError):
            # A broken/closed log must not turn back-pressure into a worker
            # failure. No shared record is written as a fallback.
            pass

    def _preemption_eligible(self, record: Mapping[str, object]) -> bool:
        """Restart permission and remaining budget, with known generation work.

        Preserve failed-attempt history in its original generation by leaving
        any holder with recorded outcomes running. A new generation may carry
        only earlier interruptions, counted by the existing attempt prefix and
        linked through the immutable withdrawal decisions.
        """
        attempts = record.get("attempts", 0)
        missing = record.get("attempt_history_missing_before", 0)
        limit = record.get("max_attempts", DEFAULT_MAX_ATTEMPTS)
        if (record.get("retry_safe") is not True
                or type(attempts) is not int or attempts < 0
                or type(missing) is not int or missing != attempts
                or record.get("attempt_history")
                or type(limit) is not int or attempts + 1 >= limit):
            return False
        if not self._preemption_prefix_valid(record, attempts, limit):
            return False
        shape, measurement = cpu_admission.action_identity(record)
        return shape is not None and not measurement

    def _preemption_prefix_valid(
        self, record: Mapping[str, object], attempts: int, limit: int
    ) -> bool:
        """Verify interrupted launches against their immutable decisions."""
        # A legacy missing prefix is not proof of restartable interruptions.
        # Verify the exact chain already filed by this preemption mechanism;
        # each parent must account for exactly one fewer consumed launch.
        parent = record
        for consumed in range(attempts, 0, -1):
            link = parent.get("supersedes_withdrawal")
            if not isinstance(link, Mapping):
                return False
            generation = link.get("published_unix")
            if type(generation) not in (int, float) or not math.isfinite(generation):
                return False
            try:
                decisions = self.withdrawal_decisions(
                    str(record["action_key"]), generation=float(generation))
            except PoolContractError:
                return False
            if len(decisions) != 1:
                return False
            parent = decisions[0][1]
            # Either the admission handoff (preempted_by) or the proven
            # membership handoff: the decision carries the explicit
            # `membership_handoff` identity `pool.withdraw` persists only
            # after proving the live claim, budget and lineage, read back
            # here through the one shared carrier check.  The two linkages
            # are disjoint by construction — admission decisions never
            # carry resigned proof, resign decisions never preempted_by —
            # and a supervisor-shaped `withdrawn_by` with no (or a broken)
            # proof is an ordinary cancellation, never a revival.  This
            # admits chained resign requeues (B resigning what A requeued)
            # without admitting anything the old rule refused.
            preempted = bool(parent.get("preempted_by"))
            resigned = (not preempted
                        and membership_handoff_authorized(parent))
            if ((not preempted and not resigned)
                    or parent.get("attempts") != consumed - 1
                    or parent.get("max_attempts") != limit
                    or parent.get("retry_safe") is not True
                    or parent.get("attempt_history_before_withdrawal")
                    or parent.get("attempt_history_missing_before_withdrawal", 0) != consumed - 1):
                return False
        return True

    def _requeue_arguments(
        self, record: Mapping[str, object], *, action_key: str
    ) -> dict[str, object] | None:
        """The ``publish`` call that re-submits a claim, or ``None``.

        Built BEFORE anything is stopped, because a claim this queue could not
        re-publish must be left running: a preemption that cannot requeue is a
        cancellation, and #364 asks for a retry, not a loss.

        One writer, not a second record shape.  ``publish`` stamps a fresh
        ``published_unix``, and that is the whole reason this works: the
        withdrawal that stopped the holder names the generation it cancelled,
        so a new generation of the same content-addressed key is not covered by
        it -- which ``_claim`` already calls the ordinary way to ask for the
        same work again. The admission-only handoff charges the interrupted
        launch to the existing attempt budget and links its immutable
        generation-scoped withdrawal. It never refunds a previous attempt.

        The projection carries every ``publish``-supported binding the
        record holds -- addressing, budget, container fields, and the
        staged-action bindings (``residency``, ``recompute``) -- so a
        retry re-enters the same gates the original passed instead of
        slipping past them or refusing after the work was stopped.  A
        binding ``publish`` would refuse (including a corrupt residency
        block) returns ``None`` rather than being silently erased; a
        future publish-supported binding (e.g. the stacked
        produced-output template) needs a coordinated extension here,
        never a quiet drop -- coordinate with root once it is admitted.
        """

        addressing: dict[str, object] = {}
        if record.get("checkout_snapshot") is not None:
            addressing["checkout_snapshot"] = record["checkout_snapshot"]
        elif record.get("checkout_root") is not None:
            addressing["checkout_root"] = record["checkout_root"]
        else:
            return None
        if record.get("cas_root") is None or record.get("worker_script") is None:
            return None
        arguments: dict[str, object] = {
            "action_key": action_key,
            "cas_root": record["cas_root"],
            "worker_script": record["worker_script"],
            "tags": list(record.get("tags") or []),
            "needs_gpu": bool(record.get("needs_gpu")),
            "priority": int(record.get("priority", 0)),
            "resources": dict(record.get("resources") or {}),
            "preempted_claim": dict(record),
            **addressing,
        }
        for field in ("max_attempts", "retry_safe", "container_owner",
                      "container_images"):
            if record.get(field) is not None:
                arguments[field] = record[field]
        residency = record.get("residency")
        if residency is not None:
            # The sealed staged binding: a consumer's leads, a mover's
            # range/tier/manifest.  Carried by value; ``publish``
            # re-validates the same sealed arithmetic against the same
            # demand, so a retry cannot bypass lead readiness and a
            # tier-demanded mover is not refused after being stopped.
            if not isinstance(residency, Mapping):
                return None
            arguments["residency"] = dict(residency)
        if record.get("recompute") is True:
            # A movement node stays a movement node: without this the
            # retry's key could be answered by an old CAS receipt for
            # bytes that need restaging.
            arguments["recompute"] = True
        return arguments

    def plan_requeue(self, record: Mapping[str, object]) -> dict[str, object]:
        """Build the successor publication for a withdrawn owned claim.

        Pure constructor: no withdraw, no publish, no side effects. A claim
        that cannot be re-published (or carries no retry budget) raises
        instead of losing the work — the caller leaves it running. The
        successor may only be published after the original attempt's exact
        terminal is filed: publishing while the holder is still live races
        the holder's own finish, whose requeue disposition would overwrite
        this successor. See ``publish(..., handoff_by=...)`` for the linkage
        the eventual publication must prove.
        """

        if not isinstance(record, Mapping):
            raise PoolContractError("requeue needs the claimed record")
        key = record.get("action_key")
        if not isinstance(key, str) or len(key) != 64:
            raise PoolContractError("requeue needs a 64-character action key")
        snapshot = dict(record)
        arguments = self._requeue_arguments(snapshot, action_key=key)
        if arguments is None:
            raise PoolContractError(
                f"{key[:12]} cannot be re-published from its claim")
        if not self._preemption_eligible(snapshot):
            raise PoolContractError(
                f"{key[:12]} carries no retry budget for a requeue")
        arguments.pop("preempted_claim", None)
        return {"arguments": arguments, "snapshot": snapshot}

    def _preempt_background_holder(
        self,
        ledger: ResourceLedger,
        *,
        action_key: str,
        demand: Mapping[str, int],
        priority: int,
        controller: cpu_admission.Controller | None = None,
    ) -> str | None:
        """Take the box back for a denied foreground item.  Name who yielded.

        #363 put the priority band ahead of aging, so a ``--priority -10`` item
        is never *considered* while foreground work is ready.  An item already
        admitted was outside that: it keeps its reservation until it finishes
        or hits its own ``--timeout-s``, so "does not displace real work" held
        in the queue and not at the box.  This is the box half (#364).

        The stop is the existing withdrawal ladder, not a new kill path:
        ``withdraw`` files the immutable cancellation and, on the holding box,
        asks the broker to stop the exact attempt.  It releases nothing --
        ``released`` is ``0`` on every path -- so the denied item is admitted on
        a later pass, once the holder's own ``finish`` returns the tokens.  The
        requeue is published straight away and simply waits: ``_claim`` skips a
        ready record whose key is still in ``claimed/``.

        Four bounds, and each is the objective rather than a threshold:

        * **Only a foreground denial.**  Intra-band fairness is aging's job; a
          background item that cancelled another would spend its own band's
          work to buy a place in it.
        * **Only a restartable background holder.** Negative priority is not
          retry permission. The verified action must be generation work with
          explicit ``retry_safe``, an unused attempt after this interruption,
          and no recorded failures to move across generation boundaries.
          Foreground, measurement and unknown actions are protected.
        * **Only when the release closes the gap.**  Stopping work that does
          not admit the denied item is pure loss on both sides.  Measured from
          the holder's actual tokens, not its declared demand, because adaptive
          admission can seat an action on fewer.
        * **One holder, and none while a release is in flight.**  Tokens a
          withdrawn holder is about to return are counted as already promised,
          so the next pass does not cancel a second action for the same gap.

        A holder that cannot be stopped through the ladder is skipped, never
        forced: one already covered by a withdrawal or waiting on cleanup is
        counted as pending, one whose claim or requeue cannot be read is left
        alone, and one on another box was never a candidate -- the holders
        considered here are the ones on this ledger.

        Selection reads capacity under host admission, but withdrawal and
        retry publication do not hold that gate. A separate nonblocking local
        lock spans both phases: while a handoff stalls, another loop may admit
        fitting work but cannot select a second victim before the first
        cancellation is visible. Neither phase returns the victim's tokens.
        """

        if priority < 0:
            return None
        wanted = {kind: int(need) for kind, need in demand.items() if int(need) > 0}
        if not wanted:
            return None
        with self._preemption_locked(ledger) as acquired:
            if not acquired:
                return None
            # Verifying restartability follows immutable withdrawal links and
            # reads the sealed request.  Neither read changes capacity, and a
            # slow one must not occupy host admission.  The snapshot is only
            # advice: selection below still reads the live holder set, tokens,
            # claim and current withdrawal state under admission, and uses a
            # proof only for the exact claim it described.
            proofs = self._preemption_eligibility_proofs(
                ledger, action_key=action_key)
            with self._admission_lock(controller):
                selected = self._select_background_holder(
                    ledger, action_key=action_key, wanted=wanted, proofs=proofs)
            if selected is None:
                return None
            holder, record = selected
            return self._preempt_selected_holder(
                holder, record, action_key=action_key)

    def _preemption_eligibility_proofs(
        self, ledger: ResourceLedger, *, action_key: str,
    ) -> dict[str, tuple[dict[str, object], bytes, bool]]:
        """Read immutable restartability proofs before host admission.

        This discovery is deliberately advisory.  A holder can finish, be
        withdrawn, or be replaced while it runs, so the caller must re-read
        live capacity, tokens, claim and pending-release state under admission.
        A proof only applies when that live claim has the same exact identity;
        missing or changed evidence refuses preemption for this pass.
        """

        proofs: dict[str, tuple[dict[str, object], bytes, bool]] = {}
        for holder in ledger.held_keys():
            if holder == action_key:
                continue
            try:
                record = _read_json(self.item_path(CLAIMED, holder))
            except PoolContractError:
                continue
            if record is None:
                continue
            # Protected holders cannot become preemptable from a sealed action
            # read. Skip the expensive immutable proof; live selection still
            # accounts for their tokens as a pending release when applicable.
            try:
                protected = (int(record.get("priority", 0)) >= 0
                             or record.get("finish_pending") is not None
                             or record.get("container_cleanup_pending") is not None)
            except (TypeError, ValueError):
                protected = True
            if not protected:
                binding = self._preemption_proof_binding(record)
                if binding is not None:
                    proofs[holder] = (
                        record, binding, self._preemption_eligible(record))
        return proofs

    @staticmethod
    def _preemption_proof_binding(record: Mapping[str, object]) -> bytes | None:
        """The live fields whose values make an eligibility proof applicable.

        Keep this extraction beside ``_preemption_eligible`` rather than
        reimplementing its decision.  The sealed action proof reads ``cas_root``
        and ``action_key`` and incorporates ``resources``; the prefix proof and
        retry gate consume every other field named here.  Fields such as a
        resource-scope checkpoint can change on a live claim without changing
        restartability, so comparing the whole record would needlessly defer.
        """

        # Preserve both JSON type and field presence. Python equality would
        # otherwise collapse true with 1, 3 with 3.0, and an absent field with
        # explicit null, while the eligibility gate intentionally does not.
        try:
            return pb._canonical_bytes({
                field: record[field]
                for field in (
                    "action_key", "cas_root", "resources", "retry_safe",
                    "attempts", "attempt_history_missing_before", "max_attempts",
                    "attempt_history", "supersedes_withdrawal",
                )
                if field in record
            })
        except pb.ActionContractError:
            # JSON readers can accept nonfinite values. A corrupt holder is
            # ineligible; it must not abort the whole host's candidate pass.
            return None

    @staticmethod
    @contextmanager
    def _preemption_locked(ledger: ResourceLedger):
        """Serialize handoffs without excluding ordinary host admission.

        Resolve the box identity before admission. Never unlink the permanent
        inode, inherit it across exec, or release it on an assumed timeout:
        a process stuck in a shared syscall must retain the handoff slot.
        """
        directory, digest = cpu_admission.box_state(ledger.base)
        # The admission monitor inventories *.lock in this directory. This
        # distinct suffix keeps a stalled handoff out of its host-gate census.
        descriptor = os.open(directory / (digest + '.preemption'),
                             os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(descriptor)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_nlink != 1):
                raise RuntimeError('unsafe PrismaBuild preemption lock file')
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                yield False
                return
            yield True
        finally:
            os.close(descriptor)

    def _select_background_holder(
        self, ledger: ResourceLedger, *, action_key: str,
        wanted: Mapping[str, int],
        proofs: Mapping[str, tuple[Mapping[str, object], bytes, bool]] | None = None,
    ) -> tuple[str, dict[str, object]] | None:
        """Read the current gap and pending releases under host admission.

        ``proofs`` was prepared before host admission.  Its CAS and immutable
        withdrawal-prefix reads may be slow, so it is usable only for a live
        claim with the same identity.  Capacity, holder tokens, the live claim
        set and current withdrawal coverage remain this method's authority.
        """
        try:
            self._refuse_if_fenced()
        except PoolContractError:
            # A fenced queue takes no submissions, so the requeue this owes the
            # holder could not be published.  Stopping it anyway would turn a
            # preemption into a cancellation.
            return None

        available = ledger.available()
        pending: dict[str, int] = {}
        candidates: list[tuple[int, float, str, dict[str, object], dict[str, int]]] = []
        for holder in ledger.held_keys():
            if holder == action_key:
                continue
            tokens = ledger.holder_tokens(holder)
            if not tokens:
                continue
            try:
                record = _read_json(self.item_path(CLAIMED, holder))
            except PoolContractError:
                continue      # a reservation whose claim nobody can read
            if record is None:
                # A reservation with no claim belongs to a reaper, not to this
                # decision.  Neither preemptable nor promised.
                continue
            try:
                covered = self.withdrawal_covers(record, action_key=holder) is not None
            except PoolContractError:
                continue
            if (covered or record.get("finish_pending") is not None
                    or record.get("container_cleanup_pending") is not None):
                for kind, count in tokens.items():
                    pending[kind] = pending.get(kind, 0) + count
                continue
            try:
                holder_priority = int(record.get("priority", 0))
                claimed_unix = float(record.get("claimed_unix") or 0.0)
            except (TypeError, ValueError):
                continue
            proof = None if proofs is None else proofs.get(holder)
            if (holder_priority >= 0 or proof is None
                    or not _same_claim(record, proof[0])
                    or self._preemption_proof_binding(record) != proof[1]
                    or not proof[2]):
                continue
            candidates.append(
                (holder_priority, claimed_unix, holder, record, tokens))

        def fits(extra: Mapping[str, int]) -> bool:
            return all(
                available.get(kind, 0) + pending.get(kind, 0)
                + int(extra.get(kind, 0)) >= need
                for kind, need in wanted.items()
            )

        if fits({}):
            # Enough is already coming back.  Nothing left to stop.
            return None
        usable = [entry for entry in candidates if fits(entry[4])]
        if not usable:
            return None
        # Lowest priority first, then the youngest claim: of two holders that
        # yield equally, the one that has run least loses least by starting over.
        usable.sort(key=lambda entry: (entry[0], -entry[1]))
        _, _, holder, record, _ = usable[0]
        return holder, record

    def _preempt_selected_holder(
        self, holder: str, record: Mapping[str, object], *, action_key: str,
    ) -> str | None:
        """Complete one handoff outside admission, rechecking exact ownership."""
        # Keep cancellation and replacement publication in one transition.
        # Waiters take this same key lock before following the cancellation,
        # so an intermediate marker cannot become a terminal verdict.
        with self._transition_locked(holder, blocking=False) as acquired:
            if not acquired:
                return None
            requeue = self._requeue_arguments(record, action_key=holder)
            if requeue is None:
                return None
            try:
                filed = self.withdraw(
                    holder,
                    reason=(
                        f"preempted on {socket.gethostname()} so foreground action "
                        f"{action_key[:12]} could be admitted; requeued at priority "
                        f"{requeue['priority']}"
                    ),
                    by=f"prismabuild admission on {socket.gethostname()}",
                    preempted_by=action_key,
                    expected_claim=record,
                )
            except PoolContractError:
                # The holder concluded, or its reservations contradict each other,
                # between reading its claim and taking its lock.  Neither is this
                # pass's business to resolve, and raising here would end a claim
                # pass over a bookkeeping fact about somebody else's action.
                return None
            if filed.get("status") != "withdrawn" or filed.get("state") != CLAIMED:
                # Nothing was stopped: the claim had already finished, or an
                # earlier cancellation already covers this generation.  Publishing
                # the requeue anyway would re-run work that just completed, on a
                # fresh generation no terminal-claim guard catches.  The tokens are
                # already back or on their way, so the denied item is admitted on a
                # later pass without this.
                return None
            # Immediately after, and in this order: ``withdraw`` retires a ready
            # record its own cancellation covers, and ``publish`` retires the
            # visible marker into ``superseded/`` so the holder's submitter does
            # not read its requeued action as terminally withdrawn.  The immutable
            # decision survives that, which is what the holder's own checkpoint and
            # ``finish`` read.  The aging sidecar is untouched by both.
            try:
                self.publish(**requeue)
            except PoolContractError as exc:
                # The cancellation is already durable, so this cannot be silent:
                # say which action was stopped without being requeued, and let the
                # pass continue.  Raising would take the claim loop down with it.
                print(
                    f"prismabuild: preempted {holder[:12]} for {action_key[:12]} "
                    f"but could not requeue it: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
            return holder

    def _claim(
        self,
        *,
        tags: Iterable[str] = (),
        has_gpu: bool = False,
        owner: str | None = None,
        capacity: Mapping[str, int] | None = None,
        cpu_tiers: Mapping[str, Sequence[int]] | None = None,
        controller: cpu_admission.Controller | None = None,
        gpu_controller: gpu_admission.Controller | None = None,
        ready: list[dict[str, object]] | None = None,
        observed_images: Container[str] | None = None,
        admission_open: Callable[[], bool] | None = None,
    ) -> dict[str, object] | None:
        """Take one ready item, atomically.  ``None`` when nothing matches.

        The claim IS the ``rename``.  Two workers racing the same item both call
        it; exactly one succeeds and the loser sees ``FileNotFoundError`` and
        moves on.  Nothing else in this method may fail in a way that leaves the
        item in neither directory.

        When ``capacity`` is given, admission runs *before* the rename and the
        tokens are released again if the rename is lost -- so a worker never
        holds capacity it is not about to use, and never waits while holding.

        With a ``controller``, the host-wide admission lock is taken *here*,
        and it covers the capacity prelude and, per candidate, the decision
        through ``begin_acquire``.  That is what has to be exclusive between
        the loops of one box, because ``begin_acquire`` moves the tokens out of
        ``free/`` and into a directory every sibling's ``decision`` and
        ``available`` already counts, so the same headroom cannot be spent
        twice once it returns.  Everything after it runs outside the lock,
        including the rename: ownership is decided fleet-wide by that rename
        and by the per-key transition lock, neither of which a host-local flock
        adds anything to, and holding it across the mount is what emptied whole
        boxes out of the claiming population when the mount was slow (#351).

        Nothing reacquires the lock on the way to a claim.  The borrow record
        that ``admitted`` writes is taken inside the same block as the
        decision it belongs to, so once the rename happens there is no
        host-local gate left that could refuse and cost this loop a claim it
        already made.  The lock is taken again only on the branches that give
        the reservation back, to give the borrow back with it: a claimant that
        lost the rename occupied no borrowed CPU, and a refusal there costs
        one sample's borrow rather than a claim.
        An item that declares container images (``container_images``, sealed
        from the action's own params) is refused before any resource decision
        unless ``observed_images`` positively holds every reference.  ``None``
        is unknown, an absent reference is named in the denial, and neither
        path records a pass or touches an attempt or a token: the item stays
        ready, which is what keeps it portable to the box that has the image
        (#714).

        A starved item (``passes >= STARVATION_FLOOR``) that this host could
        eventually fit withholds the host rather than being overtaken; one it
        could never fit is skipped, because withholding a box for work that
        will never run there is the deadlock, not the fix.

        A denied *foreground* item may also take the box back from an admitted
        background holder -- see :meth:`_preempt_background_holder`, which
        bounds that to one holder per pass whose release actually admits the
        denied item.  The stop is asynchronous, so this pass ends exactly as it
        did before and a later one makes the claim.

        ``capacity`` is the box's offer *now*, not its configuration -- a
        worker clamps it to what work the pool did not schedule has left free
        (``prismabuild.box_capacity``).  The never-fits test above reads the
        ledger's *total*, and the retire deletes free tokens only, so the two
        cases part on whether anything is holding: a kind the clamp has taken
        to zero with no holder drops the total under the demand and the item is
        skipped, recording no ``passes`` on a box that cannot presently run it;
        a kind whose holders keep the total at or above the demand is denied
        and aged exactly as before, since from the item's side that is an
        ordinary busy box.  Either way it unwinds by itself, because the offer
        recovers as soon as the foreign work exits.
        """

        owner = owner or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        tagset = frozenset(str(t) for t in tags)
        self.ensure_layout()
        # The load-bearing half of ``withdraw``.  A withdrawal that lands while
        # a worker is mid-``finish`` can leave a requeued ready record behind
        # it, and without this guard that record is claimed and the cancelled
        # work runs again -- which is the race the operator used to have to win
        # by hand.  Read once per scan, not once per item.
        withdrawn = self.withdrawn_keys()
        # One offer snapshot per scan, read only if something asks for it.
        # The cross-resource preference is the only caller and it asks on the
        # rare scans where this box is working the other resource, so an
        # ordinary scan still reads the worker registry not at all.
        placement_offers: list[dict[str, object]] | None = None

        def offer_snapshot() -> list[dict[str, object]]:
            nonlocal placement_offers
            if placement_offers is None:
                placement_offers = self.offers()
            return placement_offers

        ledger = None
        total: dict[str, int] = {}
        if capacity is not None:
            ledger = self.ledger()
            # The CPU map is immutable while workers run. Validate an existing
            # map before admission so a shared read cannot block sibling work.
            # A missing map still needs serialized initialization below; no
            # capacity or reservation state is prefetched here.
            configured_tiers = (ledger.configure_cpu_tiers(cpu_tiers, initialize=False)
                                if cpu_tiers is not None else None)
            # Minting and retiring tokens is a read-modify-write of this box's
            # own capacity, so it stays exclusive -- two loops retiring against
            # different clamped offers must not interleave.  It is bounded and
            # it is not the claim: no rename, lease or token move happens here.
            with self._admission_lock(controller):
                if cpu_tiers is None:
                    cpu_tiers = _read_json(ledger.base / "cpu-map.json")
                if cpu_tiers is not None:
                    cpu_tiers = (configured_tiers if configured_tiers is not None else
                                 ledger.configure_cpu_tiers(cpu_tiers))
                    if int(capacity.get("cpu", 0)) > sum(map(len, cpu_tiers.values())):
                        raise PoolContractError("CPU capacity exceeds the inherited CPU map")
                    ledger.retire_free_capacity({"cpu": int(capacity.get("cpu", 0))})
                ledger.ensure_capacity(capacity)
                total = ledger.capacity()
        if ready is None:
            ready = self.ready_items()
        live_generations = {(str(item.get("action_key", "")), repr(item.get("published_unix")))
                            for item in ready}
        for deferrals in (self._cpu_deferrals, self._cross_resource_deferrals):
            for generation in list(deferrals):
                if generation not in live_generations:
                    deferrals.pop(generation, None)
        preempted = False
        for item in ready:
            key = str(item.get("action_key", ""))
            with self._transition_locked(key, blocking=False) as acquired:
                if not acquired:
                    if key:
                        self.record_denial(item, "transition_busy")
                    continue
                # Refresh the directory under exclusion before consulting names:
                # a cached negative lookup can outlive another NFS client's claim.
                claimed_names = os.listdir(self.dir(CLAIMED))
                if (f"{key}.json" in claimed_names
                        or any(name.startswith(f"{key}.")
                               and name.endswith((TOMBSTONE_SUFFIX, LATE_FINISH_SUFFIX))
                               for name in claimed_names)):
                    self.record_denial(item, "already_claimed")
                    continue
                if not key or not self._placement_matches(item, tags=tagset, has_gpu=has_gpu):
                    if key:
                        self.record_denial(item, "placement_mismatch", {
                            "worker_tags": sorted(tagset), "worker_has_gpu": has_gpu,
                            "required_tags": item.get("tags"), "needs_gpu": item.get("needs_gpu"),
                        })
                    continue
                declared_images = item.get("container_images")
                item_tags = item.get("tags")
                if (not declared_images and isinstance(item_tags, list)
                        and pb.CONTAINER_IMAGE_TAG in item_tags):
                    # A record this pool did not write (publish refuses the
                    # pair) that requires the capability but states no
                    # reference.  Fail closed: an unstated requirement is
                    # not an absent one.
                    self.record_denial(item, "container_image_requirement_missing", {
                        "tags": item_tags,
                    })
                    continue
                if declared_images:
                    if (not isinstance(declared_images, list)
                            or not all(isinstance(image, str)
                                       for image in declared_images)):
                        # The pool writes this field from the sealed action;
                        # a foreign shape is a denial for the item rather than
                        # a raise out of a poll whose only handler re-raises
                        # (#592).
                        self.record_denial(item, "malformed_container_images", {
                            "container_images": item.get("container_images")})
                        continue
                    observed = None
                    if observed_images is not None:
                        try:
                            observed = {str(entry) for entry in observed_images}
                        except TypeError:
                            observed = None
                    if observed is None:
                        # Unknown is not presence.  Fail closed: the item
                        # stays ready for a box that can show the reference.
                        self.record_denial(
                            item, "container_image_presence_unknown", {
                                "required": declared_images,
                            })
                        continue
                    absent = image_inventory.missing(declared_images, observed)
                    if absent:
                        # Named, and deliberately no ``record_pass`` and no
                        # token work: this box is not being skipped while it
                        # waits, it simply cannot run the item, and only a box
                        # that positively holds the image may claim it (#714).
                        self.record_denial(item, "container_image_absent", {
                            "absent": list(absent), "required": declared_images,
                        })
                        continue
                if self.withdrawal_covers(
                        item, action_key=key, withdrawn=withdrawn) is not None:
                    # Already filed under ``withdrawn``, and of the generation that
                    # was withdrawn: this record is the losing half of a race, not
                    # work.  Drop it rather than leave it at the head of ``ready``
                    # for every future poll to step over -- but FILE it first.  A
                    # queue that removes a record it will not run and says nothing
                    # anywhere is the shape ``quarantine_orphans`` names in its own
                    # docstring, and the reason this guard was a blocker.
                    #
                    # A record of a LATER generation falls through and is claimed:
                    # somebody asked for this work again after the cancellation,
                    # which a content-addressed key makes the ordinary way to ask.
                    cancelled = self._withdraw_ready(key)
                    if cancelled is not None:
                        self._file_superseded(
                            cancelled, key=key, kind="dropped", status="dropped",
                            dropped_unix=_now(), dropped_host=socket.gethostname(),
                            reason="requeued into ready after the withdrawal that "
                                   "cancelled this generation",
                        )
                    continue
                # A lead's record that cannot be read -- corrupt bytes, or an
                # ``ESTALE`` off a cold NFS handle -- must be a denial for the
                # item naming that lead, not an escape out of the poll: this
                # call is made before the admission try, and the caller is the
                # worker loop, which has no handler to spare for it (#592).
                # The wrap is here, at the one call site, rather than
                # ``tolerate_stale`` inside the by-key readers, whose loudness
                # is what turns a broken mount into a visible failure instead
                # of a confident wrong verdict.
                try:
                    residency = self.residency_verdict(item)
                except (OSError, PoolContractError) as exc:
                    residency_block = item.get("residency")
                    leads = (residency_block.get("leads")
                             if isinstance(residency_block, Mapping) else None)
                    self.record_denial(item, "residency_lead_record_unreadable", {
                        "error": str(exc), "leads": leads})
                    continue
                if residency["state"] in ("lead_not_resident", "lead_unpinned",
                                          "map_not_composed", "map_stale",
                                          "map_unreadable", "plan_unreadable"):
                    # Before any token is taken, and without ``record_pass``:
                    # the bytes are not there, so this box should go do other
                    # work rather than age an item nothing on this box can
                    # advance.  Rob, #583: schedule compute when its
                    # dependencies are met, not while it spins on I/O.
                    # ``lead_unpinned`` is the same refusal for a lead that
                    # finished holding no tokens -- it moved nothing, it
                    # overran, or an egress has already taken its bytes back.
                    # ``map_not_composed`` is the refusal for bytes that are
                    # there and unreachable: without the map the action reads
                    # the pool and says nothing went wrong.  ``map_unreadable``
                    # is the same refusal when the mount would not say either
                    # way; both leave the item ready for the next scan.
                    # ``map_stale`` is the third of that family: the map is
                    # there and readable but was composed before this item's
                    # lead landed, so it addresses every range but the one the
                    # gate just waited for (#634).
                    # ``plan_unreadable`` is the one that does not resolve on
                    # its own: no coordinator can compose a map from a plan it
                    # refuses, so the item names the refusal rather than
                    # waiting out a cycle that will not come (#615).
                    reason = f"residency_{residency['state']}"
                    if residency["state"] in ("lead_not_resident", "lead_unpinned"):
                        pending = residency.get("pending")
                        if (isinstance(pending, list) and pending
                                and all(isinstance(entry, Mapping)
                                        and entry.get("status") not in (None, "absent")
                                        for entry in pending)):
                            # Every lead ended somewhere no later poll repairs:
                            # failed, withdrawn, dropped, unpinned, or bound
                            # to another manifest.  Admission is unchanged --
                            # the item stays ready, as documented above -- but
                            # the denial names the terminal state, so the
                            # fleet-wide denial snapshot tells it apart from a
                            # mover that simply has not finished (#595).
                            reason = "residency_lead_terminal"
                    self.record_denial(item, reason, {"residency": residency})
                    continue
                try:
                    sealed_demand = self.demand_of(item)
                except (TypeError, ValueError) as exc:
                    # The same class as the tier id below, one step earlier and
                    # for the same reason: ``publish`` refuses a ``resources``
                    # block that is not an object of counts, so one that
                    # reaches here arrived in a record this pool never wrote.
                    # It is a denial for the item carrying it, not a raise out
                    # of a poll whose only handler re-raises (#592).  ``int()``
                    # over a foreign value raises ``TypeError`` as readily as
                    # ``ValueError``, and ``demand_of`` raises
                    # ``PoolContractError`` for a non-object, which is both.
                    self.record_denial(item, "malformed_demand", {
                        "resources": item.get("resources"), "error": str(exc)})
                    continue
                try:
                    demand, tier_demand = storage_tiers.split_demand(sealed_demand)
                    # ``publish`` refuses these ids on the way in, so one that
                    # parses to ``split_demand`` yet fails ``_check_tier_id``
                    # arrived in a record this pool never wrote.  A foreign
                    # writer's mistake is a denial for that item, not a raise
                    # whose only handler re-raises and leaves every worker's
                    # loop dead on the same record (#592).  In-memory string
                    # checks over an already-parsed dict: nothing here adds
                    # work to an item with no tier demand.
                    for tier_id in tier_demand:
                        self._check_tier_id(tier_id)
                except ValueError as exc:
                    self.record_denial(item, "malformed_tier_demand", {
                        "demand": sealed_demand, "error": str(exc)})
                    continue
                reservation_demand = dict(demand)
                if gpu_controller is not None and demand.get("gpu"):
                    # Historical slot counts expressed sharing, not device count.
                    # Preserve the sealed demand but reserve this worker's single
                    # physical GPU; the controller keeps multi-slot work exclusive.
                    reservation_demand["gpu"] = 1
                handle: str | None = None
                tier_handles: dict[str, str] = {}
                tier_funded: dict[str, dict[str, object]] = {}
                adaptive = None
                adaptive_gpu = None
                borrow = None
                gpu_probe = None
                if controller is not None and not demand:
                    # Adaptive admission needs a durable reservation to make an
                    # unknown CPU consumer visible to subsequent measurements.
                    # Empty legacy demand has no holder; keep it queued instead.
                    self.record_pass(key)
                    self.record_denial(item, "empty_demand")
                    continue
                # From the reservation to ``commit_acquire`` the tokens exist
                # only under a handle this frame holds: nothing else can name
                # them, and no sweep will return them before
                # ``LEASE_TIMEOUT_S``.  Every ending in that window therefore
                # has to give them back -- including the ones this code does
                # not author, which is what the bare ``except`` is for.
                committed = False
                try:
                    if ledger is not None and demand:
                        if any(total.get(kind, 0) < need for kind, need in reservation_demand.items()):
                            self.record_denial(item, "never_fits_capacity", {
                                "capacity_total": total, "demand": demand,
                                "reservation_demand": reservation_demand,
                            })
                            continue      # never fits this box; not this box's to hold
                        # Soft placement preference, deliberately outside the
                        # admission lock below: it reads the worker registry
                        # and other boxes' ledgers over the shared mount, and
                        # holding host admission across a mount stall is #351.
                        cross_resource = self._defer_cross_resource_placement(
                            item, reservation_demand, live=offer_snapshot())
                        if cross_resource is not None:
                            # Not a refusal and not starvation: no ``record_pass``,
                            # for the same reason ``deferred_for_preferred_cpu``
                            # records none.  The denial is what makes a bounded
                            # wait visible instead of silent.
                            self.record_denial(item, "deferred_for_cross_resource_placement", {
                                "demand": demand, "reservation_demand": reservation_demand,
                                "remote_offer": cross_resource,
                            })
                            continue
                        # These facts belong to the sealed action, not changing
                        # host capacity. A slow CAS request read must not hold
                        # admission. Retain this candidate's transition lock and
                        # pass even unknown facts explicitly: no locked reread.
                        identity = (cpu_admission.action_identity(item)
                                    if controller is not None else None)
                        contract = (gpu_admission.action_contract(item, demand)
                                    if gpu_controller is not None and demand.get("gpu")
                                    else None)
                        # Host admission's exclusive half, and only that half: read
                        # the headroom, decide against it, and take the tokens out
                        # of ``free/`` before letting go.  ``begin_acquire`` moves
                        # them into ``held/<handle>/``, where every sibling's
                        # ``decision`` and ``available`` already counts them, so the
                        # headroom cannot be spent twice once this block returns.
                        # The claim itself -- the rename that decides ownership --
                        # is deliberately outside: it is arbitrated fleet-wide by
                        # the rename, not by a host-local lock, and holding this one
                        # across it is what took whole boxes out of the claiming
                        # population when the mount was slow (#351).
                        refused = False
                        refusal_source = None
                        cpu_decision = gpu_decision = token_shortage = None
                        with self._admission_lock(controller):
                            if controller is not None:
                                adaptive = controller.decision(item, demand, identity=identity)
                                cpu_decision = getattr(controller, "last_decision", None)
                                refused = adaptive is None
                                if refused:
                                    refusal_source = "adaptive_cpu_refused"
                            if not refused and gpu_controller is not None and demand.get("gpu"):
                                adaptive_gpu = gpu_controller.decision(item, demand, contract=contract)
                                gpu_decision = getattr(gpu_controller, "last_decision", None)
                                refused = adaptive_gpu is None
                                if refused:
                                    refusal_source = "adaptive_gpu_refused"
                            if not refused:
                                if adaptive_gpu is not None:
                                    handle = ledger.begin_acquire(
                                        key, reservation_demand, adaptive=adaptive,
                                        cpu_tiers=cpu_tiers, adaptive_gpu=adaptive_gpu)
                                    asked = reservation_demand
                                else:
                                    handle = (ledger.begin_acquire(key, demand) if adaptive is None else
                                              ledger.begin_acquire(key, demand, adaptive=adaptive,
                                                                   cpu_tiers=cpu_tiers))
                                    asked = demand
                                token_shortage = ledger.last_token_shortage
                                if handle is not None:
                                    # A funded reservation spends its probe and borrow
                                    # freshness under the same exclusion as its decision.
                                    # Ordinary abandonment returns only owned credit;
                                    # failure to persist rolls the reservation back.
                                    if adaptive_gpu is not None:
                                        gpu_probe = gpu_controller.reserve_probe(adaptive_gpu)
                                    if adaptive is not None:
                                        borrow = (adaptive, controller.admitted(adaptive))
                        if refused:
                            # Aging is shared diagnostic/fairness bookkeeping, not
                            # capacity authority. Keep its I/O outside host admission.
                            # The per-key transition lock still protects this item.
                            self.record_pass(key)
                            decision = (gpu_decision
                                        if refusal_source == "adaptive_gpu_refused"
                                        else cpu_decision)
                            self.record_denial(item, refusal_source or "adaptive_refused",
                                               {"decision": decision or {}})
                            continue
                        if handle is None:
                            denials = self.record_pass(key)
                            if not preempted:
                                # Selection reacquires admission, while the separate
                                # handoff lock spans withdrawal/requeue as well. A
                                # stalled handoff cannot stop ordinary fitting work.
                                preempted = self._preempt_background_holder(
                                    ledger, action_key=key, demand=asked,
                                    priority=int(item.get("priority", 0)),
                                    controller=controller) is not None
                            withholding = denials >= STARVATION_FLOOR
                            age = self.withhold_age(key) if withholding else None
                            self.record_denial(item,
                                               "reservation_unavailable_withholding" if withholding
                                               and age <= WITHHOLD_CEILING_S else
                                               "reservation_unavailable_past_ceiling" if withholding else
                                               "reservation_unavailable", {
                                "demand": demand, "reservation_demand": reservation_demand,
                                "token_shortage": token_shortage,
                                "capacity_total": total, "denials": denials,
                                "withhold_age_s": age, "withhold_ceiling_s": WITHHOLD_CEILING_S,
                                "cpu_decision": cpu_decision,
                                "gpu_decision": gpu_decision,
                            })
                            if withholding and age <= WITHHOLD_CEILING_S:
                                return None
                            # Past the ceiling, retain aging but let smaller work run.
                            continue
                    allocation = (ledger.cpu_allocation(handle, cpu_tiers)
                                  if ledger is not None and handle is not None and cpu_tiers is not None else None)
                    fallback_deferral = (self._defer_fallback(item, demand)
                                          if allocation is not None and allocation["fallback"] else None)
                    if fallback_deferral is not None:
                        ledger.abandon_acquire(handle)
                        self._return_borrow(controller, borrow)
                        self._return_gpu_probe(controller, gpu_controller, gpu_probe)
                        self.record_denial(item, "deferred_for_preferred_cpu", {
                            "demand": demand,
                            "cpu_allocation": allocation,
                            "remote_offer": fallback_deferral,
                        })
                        continue
                    if tier_demand:
                        # After host admission, so a box that cannot seat the
                        # work never touches the shared tier ledgers, and
                        # before the rename, so a claim is never won on tier
                        # capacity it does not hold.  No ``record_pass``: the
                        # tier is cluster-scoped, and withholding this box for
                        # a shortage every box shares would idle it for nothing
                        # (Rob, #583: the box does other work meanwhile).
                        shortage = self._begin_tier_acquire(
                            key, tier_demand, tier_handles, tier_funded,
                            cas_root=item.get("cas_root"))
                        if shortage is not None:
                            self._abandon_tier_acquire(tier_handles)
                            tier_handles.clear()
                            if ledger is not None and handle is not None:
                                ledger.abandon_acquire(handle)
                                self._return_borrow(controller, borrow)
                                self._return_gpu_probe(controller, gpu_controller, gpu_probe)
                            self.record_denial(item, str(shortage["reason"]), {
                                "demand": sealed_demand, "tier_demand": tier_demand,
                                "tier_shortage": shortage,
                            })
                            continue
                    # Intent precedes the claim, so a crash in between leaves evidence.
                    # Resign fence, re-checked under this key's transition
                    # exclusion. This narrows the poll-check to rename race
                    # but does NOT lock the broker's gate: a drain can still
                    # begin after this check. A claim that wins then cannot
                    # execute — broker scope ``create`` refuses under its
                    # mutex and the loop's cleanup path releases it — and the
                    # resign proof (repeated census, bracketed park acks,
                    # empty scopes) accounts for it before SUCCESS. ``None``
                    # preserves current behavior for fenceless callers. The
                    # unwind mirrors the tier-shortage path above it.
                    if admission_open is not None and not admission_open():
                        self._abandon_tier_acquire(tier_handles)
                        tier_handles.clear()
                        if ledger is not None and handle is not None:
                            ledger.abandon_acquire(handle)
                            self._return_borrow(controller, borrow)
                            self._return_gpu_probe(controller, gpu_controller,
                                                   gpu_probe)
                        self.record_denial(item, "resign_fenced", {
                            "action_key": key,
                        })
                        continue
                    self._write_claim_intent(key, owner=owner)
                    src = self.item_path(READY, key)
                    dst = self.item_path(CLAIMED, key)
                    try:
                        os.rename(src, dst)
                    except (FileNotFoundError, NotADirectoryError):
                        if ledger is not None and handle is not None:
                            # Lost the race: hold nothing -- and return only what THIS
                            # claimant took.  Releasing by action key here returned the
                            # winner's reservation and let a third action be admitted
                            # on top of it.
                            ledger.abandon_acquire(handle)
                            self._return_borrow(controller, borrow)
                            self._return_gpu_probe(controller, gpu_controller, gpu_probe)
                        self._abandon_tier_acquire(tier_handles)
                        # Leave no evidence of a claim that did not happen.  The marker
                        # is written by rename, so this claimant's copy replaced
                        # whatever was there -- and if the winner wrote first, the
                        # marker now names the box that LOST while still passing the
                        # generation check (#272).
                        #
                        # Only while it is still this claimant's own: ``owner`` is
                        # unique per claimant and the marker carries it.  The check and
                        # the unlink are two operations on a shared mount, so a marker
                        # written between them is removed as well -- but that leaves no
                        # marker, which ``resolve_claim_holder`` already answers as
                        # "nobody said", rather than a marker naming the wrong box.
                        # The ledger is the exact answer either way; this only keeps
                        # the fallback from being confidently wrong.
                        self._discard_claim_intent(key, owner=owner)
                        self.record_denial(item, "claim_rename_lost_race", {
                            "demand": demand, "reservation_demand": reservation_demand,
                            "had_reservation": handle is not None,
                        })
                        continue
                    moved = _read_json(dst) or item
                    if (not self._placement_matches(moved, tags=tagset, has_gpu=has_gpu)
                            or self.demand_of(moved) != sealed_demand):
                        # Admission described the scanned generation. A replacement
                        # may need a different host or more tokens; put it back for a
                        # fresh admission before committing this claimant's tokens.
                        if ledger is not None and handle is not None:
                            ledger.abandon_acquire(handle)
                            self._return_borrow(controller, borrow)
                            self._return_gpu_probe(controller, gpu_controller, gpu_probe)
                        self._abandon_tier_acquire(tier_handles)
                        try:
                            os.link(dst, src)
                        except OSError:
                            # A still newer submission may own ready already. Leave
                            # the moved record for the reaper, as below.
                            pass
                        else:
                            dst.unlink(missing_ok=True)
                            self.item_path(INTENT, key).unlink(missing_ok=True)
                        self.record_denial(item, "claimed_record_changed", {
                            "scanned_demand": demand, "moved_demand": self.demand_of(moved),
                            "moved_tags": moved.get("tags"), "moved_needs_gpu": moved.get("needs_gpu"),
                        })
                        continue
                    # Tier handles commit first: a failure here leaves the host
                    # handle uncommitted, so the guard below still abandons it,
                    # and a committed tier token answers to the key, which every
                    # release path returns.
                    tier_filed = self._commit_tier_acquire(key, tier_handles)
                    tier_wanted = sum(sum(needs.values()) for needs in tier_demand.values())
                    tier_funded_total = sum(
                        sum(entry["kinds"].values())
                        for entry in tier_funded.values()
                        if isinstance(entry, dict)
                        and isinstance(entry.get("kinds"), dict))
                    # Funded credit counts exactly once: the fence the
                    # coordinator transferred under this key now belongs to
                    # this claim, and only the remainder came from free.
                    incomplete = tier_filed + tier_funded_total < tier_wanted
                    filed = 0
                    if ledger is not None and handle is not None:
                        # Won the rename, so the reservation stops belonging to this
                        # claimant and starts belonging to the action.  Every branch
                        # below releases by action key, which is correct only once the
                        # tokens are filed under it.
                        filed = ledger.commit_acquire(key, handle)
                        # Past this call the handle directory is empty and the
                        # tokens answer to the action key, so the branches
                        # below release by key and the guard must stop trying
                        # to abandon a handle that owns nothing.
                        committed = True
                        if (filed < sum(reservation_demand.values())
                                or (adaptive is not None and _read_json(
                                    ledger.held_dir / key / cpu_admission.METADATA) is None)
                                or (adaptive_gpu is not None and _read_json(
                                    ledger.held_dir / key / gpu_admission.METADATA) is None)):
                            incomplete = True
                    # ``_unwind_funded_claim`` below serves every rollback on
                    # this item: host take and committed remainder go home,
                    # verified fences stay fused, the row goes back.
                    def _unwind_funded_claim(
                        reason: str, detail: dict[str, object], *,
                        post_persist: bool,
                    ) -> bool:
                        """Roll one tier/funded claim back to ready, fenced.

                        Host take and committed remainder go home; verified
                        fences stay fused (see
                        ``_unwind_funded_tier_commit``); the row goes back.
                        ``post_persist`` restorations rewrite ``dst`` from
                        ``moved`` first, because past persistence ``dst`` may
                        hold claim content that must never be linked back as
                        a ready row.  Returns whether the row was restored to
                        ``ready`` (``False`` leaves a lease-less claimed row
                        plus the denial for the reaper: a disk failure inside
                        persistence is already catastrophic territory, and a
                        claimed-content row in ``ready`` would be worse).
                        """

                        if ledger is not None and handle is not None:
                            ledger.abandon_acquire(handle)
                            ledger.release(key)
                            self._return_borrow(controller, borrow)
                            self._return_gpu_probe(
                                controller, gpu_controller, gpu_probe)
                        self._abandon_tier_acquire(tier_handles)
                        self._unwind_funded_tier_commit(
                            key, tier_demand, tier_funded)
                        if post_persist:
                            with suppress(Exception):
                                self.lease_path(key).unlink()
                            try:
                                _write_json_atomic(dst, moved)
                            except (OSError, PoolContractError, ValueError):
                                self.record_denial(item, reason, detail)
                                return False
                        try:
                            os.link(dst, src)
                        except OSError:
                            self.record_denial(item, reason, detail)
                            return False
                        else:
                            dst.unlink(missing_ok=True)
                            self.item_path(INTENT, key).unlink(missing_ok=True)
                        self.record_denial(item, reason, detail)
                        return True

                    if incomplete:
                        # A stale-acquisition sweep took part of the reservation,
                        # or tokens of an earlier incarnation are filed under this
                        # key.  Fail closed rather than run unreserved: ``dst`` is
                        # still byte-identical to the ready record, because the
                        # rewrite below has not happened yet, so putting it back
                        # restores the item exactly as it was.
                        #
                        # A funded claim unwinds fence-first: the verified fence
                        # stays fused under this key (the record is untouched,
                        # same generation), so the entitlement survives the
                        # failure and the next attempt re-verifies the same
                        # binding instead of re-taking it from free -- while the
                        # remainder this attempt newly committed goes home via
                        # ``_unwind_funded_tier_commit`` (release-by-key would
                        # free the fence into a stealer window; keeping it all
                        # would double-hold on retry).  Physical occupancy keeps
                        # the pre-existing release semantics everywhere else,
                        # unchanged here.
                        _unwind_funded_claim("committed_reservation_incomplete", {
                            "filed_tokens": filed,
                            "expected_tokens": sum(reservation_demand.values()),
                            "tier_filed_tokens": tier_filed,
                            "tier_expected_tokens": tier_wanted,
                            "adaptive_cpu": adaptive is not None,
                            "adaptive_gpu": adaptive_gpu is not None,
                        }, post_persist=False)
                        # Link rather than rename (inside the closure):
                        # ``publish`` writes ``ready`` unconditionally, so a
                        # re-submission of this key can already be sitting
                        # there, and a rename would replace that new
                        # generation with these older bytes and lose the
                        # request.  If it is there, leave the claim for the
                        # reaper instead: an extra reaper cycle costs one
                        # attempt, a clobbered generation costs the whole
                        # submission.
                        continue
                except BaseException:
                    # The handle is the only name these tokens have, and it
                    # lives in this frame.  ``begin_acquire`` keeps its own
                    # all-or-nothing promise up to the point it returns; after
                    # that an ESTALE out of the intent marker, an ENOSPC in the
                    # rename, a torn ``.adaptive.json`` -- anything at all --
                    # left the whole demand under ``held/claiming.<...>/`` with
                    # no caller able to return it, recoverable only by
                    # ``sweep_stale_acquisitions`` a ``LEASE_TIMEOUT_S`` later
                    # and once per poll for as long as the cause repeated.
                    if ledger is not None and handle is not None and not committed:
                        with suppress(Exception):
                            # Already failing; a failure to roll back must not
                            # replace the ending that is on its way out.
                            ledger.abandon_acquire(handle)
                            self._return_borrow(controller, borrow)
                    if tier_handles:
                        with suppress(Exception):
                            # Whatever ``committed`` says about the host handle: a
                            # tier handle that was never committed owns tokens no
                            # key names, and one that was is empty and a no-op.
                            self._abandon_tier_acquire(tier_handles)
                    raise
                terminal = self.terminal_outcome_covers(moved, action_key=key)
                if terminal is not None:
                    # A stale reaper can put a generation back in ``ready`` after
                    # its outcome was filed, or the outcome can land between the
                    # ready scan and this rename.  The rename is the last boundary
                    # at which the payload is definitely not executing.
                    if ledger is not None:
                        ledger.release(key)
                    # The tier half goes through the one concluding-path
                    # helper, not the blanket per-key release.  This claim
                    # took tokens and never ran, so its own acquisition must
                    # go back -- but the key may ALSO hold names an
                    # outstanding output intent still owns (an owner that
                    # concluded with a staged, unfunded batch keeps exactly
                    # those, and the mover draws them at
                    # ``fund_output_batch``).  ``release_tier_reservations``
                    # cannot tell the two apart and frees both, which left
                    # the intent citing a name the ledger reads as free.
                    # ``_release_reservation`` is the selective release every
                    # other concluding path already uses; ``host=None``
                    # because the host tokens went back on the line above.
                    self._release_reservation(key, host=None)
                    state, outcome = terminal
                    self._file_superseded(
                        moved, key=key, kind="terminal-claim", status="dropped",
                        dropped_unix=_now(), dropped_host=socket.gethostname(),
                        reason="claim lost to an outcome for the same generation "
                               f"filed under {state}",
                        terminal_status=outcome.get("status"),
                    )
                    dst.unlink(missing_ok=True)
                    self.passes_path(key).unlink(missing_ok=True)
                    continue
                if self.withdrawal_covers(moved, action_key=key) is not None:
                    # Withdrawn between the scan above and this rename.  The window
                    # is microseconds wide and closing it here costs one listing on
                    # a path taken once per claim; leaving it open costs a cancelled
                    # action a full run before ``execute`` notices.
                    if ledger is not None:
                        ledger.release(key)
                    # Selective for the same reason as the terminal branch
                    # above: a withdrawal cancels THIS claim, not an output
                    # intent that another key is still going to draw from.
                    self._release_reservation(key, host=None)
                    self._file_superseded(
                        moved, key=key, kind="dropped", status="dropped",
                        dropped_unix=_now(), dropped_host=socket.gethostname(),
                        reason="withdrawn between the ready scan and the claim",
                    )
                    dst.unlink(missing_ok=True)
                    continue
                # From ``moved``, not from ``item``: past the rename the bytes in
                # ``claimed`` are the item, and the scan's copy may be a generation
                # ``publish`` has already replaced.  Rebuilding the claim from the
                # scan wrote that replaced generation back over the one the rename
                # moved, so the worker ran a submission nobody had asked for and
                # ``finish`` filed the outcome under the retired ``published_unix``
                # -- where the waiter on the live one never looked.  The terminal
                # and withdrawal guards above already read ``moved`` for this
                # reason; the record this method returns is the last place that
                # still did not.
                claimed = dict(moved)
                # ``passes`` is not a field of the item; it is the aging sidecar,
                # which ``ready_items`` stamps on its copy so the ready ordering
                # can read it and which this method deletes four lines below.
                # Copying it into the claim freezes a denial count into the
                # claimed record, into every attempt archived from it, and into
                # the done or failed record it becomes -- a number describing a
                # counter that no longer exists, on a record no admission decision
                # ever reads.
                claimed.pop("passes", None)
                claimed.pop("cpu_allocation", None)
                claimed.pop("gpu_admission", None)
                generation = (key, repr(moved.get("published_unix")))
                self._cpu_deferrals.pop(generation, None)
                self._cross_resource_deferrals.pop(generation, None)
                claimed["claimed_by"] = owner
                claimed["claimed_unix"] = _now()
                claimed["claimed_host"] = socket.gethostname()
                if ledger is not None and cpu_tiers is not None and demand.get("cpu", 0):
                    claimed["cpu_allocation"] = ledger.cpu_allocation(key, cpu_tiers)
                if adaptive_gpu is not None:
                    claimed["gpu_admission"] = _read_json(ledger.held_dir / key / gpu_admission.METADATA)
                claimed["reserved_on"] = socket.gethostname() if demand else None
                if tier_demand:
                    claimed["tier_reservations"] = {
                        tier_id: dict(sorted(needs.items()))
                        for tier_id, needs in sorted(tier_demand.items())
                    }
                if residency["state"] != "not_requested":
                    claimed["residency_verdict"] = residency
                if tier_funded:
                    # Re-verify each funding against the renamed record before
                    # persisting: the claim holds this key's transition lock,
                    # so no coordinator transfer/cancel interleaves, but the
                    # check still runs against ``moved`` (post-rename bytes)
                    # with the generation this attempt verified at begin.
                    # Nothing is marked here: marking tier1 then failing
                    # tier2 -- or failing persistence itself -- must leave a
                    # recoverable entitlement (both records still
                    # ``transferring`` for the next attempt to re-verify),
                    # never consumed-but-no-executable-claim stranded credit.
                    # The fence stays fused while the remainder this attempt
                    # committed goes home (see ``_unwind_funded_tier_commit``);
                    # the row goes back byte-identical, and the next attempt
                    # re-verifies the same binding: preserved, never freed
                    # into a stealer window, never double-spent.
                    funding_verified = True
                    for funded_tier, entry in tier_funded.items():
                        pinned = (entry.get("generation")
                                  if isinstance(entry, dict) else None)
                        kinds = (entry.get("kinds")
                                 if isinstance(entry, dict) else None)
                        bound_names = (entry.get("tokens")
                                       if isinstance(entry, dict) else None)
                        variant = (str(entry.get("variant") or "window")
                                   if isinstance(entry, dict) else "window")
                        needs = tier_demand.get(funded_tier, {})
                        ok = (isinstance(kinds, dict) and bool(kinds)
                              and isinstance(bound_names, list)
                              and bool(bound_names))
                        if ok:
                            for kind_name, count in kinds.items():
                                try:
                                    if variant == "output":
                                        covered, live_generation = self.output_funded_cover(
                                            funded_tier, moved, str(kind_name),
                                            int(needs.get(kind_name, 0)))
                                    else:
                                        covered, live_generation = self.funded_cover(
                                            funded_tier, moved, str(kind_name),
                                            int(needs.get(kind_name, 0)))
                                except (OSError, PoolContractError, ValueError):
                                    ok = False
                                    break
                                if (not covered
                                        or int(covered) < int(count)
                                        or live_generation != pinned):
                                    ok = False
                                    break
                        if ok:
                            # Exact token set: the fence the rollback keeps
                            # must be exactly what is still held -- a rotated
                            # generation's names fail closed here, never as a
                            # half-kept fence.
                            if variant == "output":
                                live = self.read_output_funding(key, funded_tier)
                            else:
                                live = self.read_funding(key, funded_tier)
                            live_names = (live.get("tokens")
                                          if isinstance(live, dict) else None)
                            if (not isinstance(live, dict)
                                    or live.get("state") != "transferring"
                                    or live.get("generation") != pinned
                                    or not isinstance(live_names, list)
                                    or sorted(str(name) for name in live_names)
                                    != sorted(str(name)
                                              for name in bound_names)):
                                ok = False
                            elif variant == "output":
                                # R6/R7: carry the immutable requirement
                                # through the rename: the funding record must
                                # bind back to the CAS-filed request ref read
                                # here for the renamed row (one read for this
                                # phase; the begin phase read its own), the
                                # row's projection must still agree with it,
                                # and a filed request that declares no ref
                                # can never gain one from the renamed row.
                                # A direct-API row with no filed request
                                # keeps the projection binding cover already
                                # proved; unreadable request evidence fails
                                # closed.
                                try:
                                    _req_ref, _req_present = (
                                        _sealed_produced_output_batch(
                                            moved.get("cas_root"), key))
                                except (OSError, PoolContractError, ValueError):
                                    ok = False
                                    _req_ref = None
                                    _req_present = False
                                else:
                                    if (_req_present and _req_ref is None
                                            and "produced_output_batch"
                                            in moved):
                                        ok = False
                                    elif _req_ref is not None:
                                        if not (
                                                self._output_projection_matches_request(
                                                    moved.get(
                                                        "produced_output_batch"),
                                                    _req_ref)):
                                            ok = False
                                        elif not (
                                                self._output_record_matches_request(
                                                    live, _req_ref)):
                                            ok = False
                        if not ok:
                            funding_verified = False
                            break
                    if not funding_verified:
                        _unwind_funded_claim("committed_reservation_incomplete", {
                            "filed_tokens": filed,
                            "expected_tokens": sum(reservation_demand.values()),
                            "tier_filed_tokens": tier_filed,
                            "tier_expected_tokens": tier_wanted,
                            "adaptive_cpu": adaptive is not None,
                            "adaptive_gpu": adaptive_gpu is not None,
                        }, post_persist=False)
                        continue
                if tier_funded:
                    # Bind the exact funding generations to this durable
                    # attempt: every later release/recovery decision (settle,
                    # reapers, egress) proves against these names instead of
                    # trusting record state alone.  A ``transferring`` record
                    # whose generation and token set match a live
                    # claimed/terminal attempt is entitlement-or-physical
                    # under that attempt, never free credit.
                    claimed["tier_funding"] = {
                        funded_tier: {
                            "generation": entry.get("generation"),
                            "kinds": dict(entry.get("kinds") or {}),
                            "tokens": [str(name) for name in
                                       (entry.get("tokens") or [])],
                            "variant": str(entry.get("variant") or "window"),
                        }
                        for funded_tier, entry in sorted(tier_funded.items())
                        if isinstance(entry, dict)
                    }
                # Read here, where the claim record is being written anyway,
                # so the receipt costs no extra write and cannot race: after
                # this point the prewarm loop has already skipped this key,
                # because it only ever looks at ``ready``.  Absent is normal.
                # A reference, never a copy (#596): the receipt keeps growing
                # after the claim as later windows extend it, and the terminal
                # record resolves this pointer at finish, verifying the key
                # and the digest so a same-key successor's receipt is never
                # mistaken for this generation's.
                warmed = self.prewarm(key)
                if warmed is not None:
                    digest = warmed.get("manifest_sha256")
                    if isinstance(digest, str) and digest:
                        claimed["prewarm"] = {
                            PREWARM_RECEIPT_REF: key,
                            "manifest_sha256": digest,
                        }
                try:
                    _write_json_atomic(dst, claimed)
                except (OSError, PoolContractError, ValueError) as exc:
                    # The claim record never landed (atomic replace leaves
                    # ``dst`` with its old bytes): nothing may execute.
                    # Fence-first unwind, row still byte-identical.
                    _unwind_funded_claim("claim_persistence_failed", {
                        "error": str(exc),
                        "phase": "claim-record",
                        "filed_tokens": filed,
                        "expected_tokens": sum(reservation_demand.values()),
                        "tier_filed_tokens": tier_filed,
                        "tier_expected_tokens": tier_wanted,
                        "adaptive_cpu": adaptive is not None,
                        "adaptive_gpu": adaptive_gpu is not None,
                    }, post_persist=False)
                    continue
                try:
                    self.write_lease(
                        key,
                        owner=owner, claim_snapshot=claimed,
                        container_owner=(str(claimed["container_owner"])
                                         if claimed.get("container_owner") else None),
                    )
                except (OSError, PoolContractError, ValueError) as exc:
                    # The lease never landed but ``dst`` now holds claim
                    # content: restore it from ``moved`` before linking back,
                    # so a claimed-content row never lands in ``ready``.
                    _unwind_funded_claim("claim_persistence_failed", {
                        "error": str(exc),
                        "phase": "lease",
                        "filed_tokens": filed,
                        "expected_tokens": sum(reservation_demand.values()),
                        "tier_filed_tokens": tier_filed,
                        "tier_expected_tokens": tier_wanted,
                        "adaptive_cpu": adaptive is not None,
                        "adaptive_gpu": adaptive_gpu is not None,
                    }, post_persist=True)
                    continue
                if tier_funded:
                    # Fuse each funding shut only after the claim is durable.
                    # A mark that fails here unwinds the whole claim back to
                    # ready instead of returning it: no execution without
                    # complete durable proof.  One claim carries at most one
                    # funded tier in practice (a row has a single residency
                    # block, and ``funded_cover`` requires the row's
                    # residency tier to equal the record's tier).  The claim
                    # holds this key's transition lock throughout, so no
                    # coordinator transfer/cancel interleaves between the
                    # pre-persistence verification and these marks; only I/O
                    # can still fail, and I/O failure keeps the entitlement
                    # (records stay ``transferring``) for the next attempt.
                    funding_sealed = True
                    failed_tier: str | None = None
                    for funded_tier, entry in tier_funded.items():
                        pinned = (entry.get("generation")
                                  if isinstance(entry, dict) else None)
                        variant = (str(entry.get("variant") or "window")
                                   if isinstance(entry, dict) else "window")
                        # Locked spelling: the claim holds this key's
                        # transition lock from its tier acquire through here.
                        if variant == "output":
                            marked = self._advance_output_funding_state_locked(
                                key, funded_tier, expect="transferring",
                                advance_to="consumed",
                                generation=pinned if isinstance(
                                    pinned, str) else None)
                            if marked:
                                continue
                            current = self.read_output_funding(
                                key, funded_tier)
                        else:
                            marked = self._advance_funding_state_locked(
                                key, funded_tier, expect="transferring",
                                advance_to="consumed",
                                generation=pinned if isinstance(
                                    pinned, str) else None)
                            if marked:
                                continue
                            current = self.read_funding(key, funded_tier)
                        if (current is not None
                                and current.get("state") == "consumed"
                                and (pinned is None or current.get(
                                    "generation") == pinned)):
                            continue
                        funding_sealed = False
                        failed_tier = funded_tier
                        break
                    if not funding_sealed:
                        _unwind_funded_claim("funding_not_durable", {
                            "tier_id": failed_tier,
                            "filed_tokens": filed,
                            "expected_tokens": sum(reservation_demand.values()),
                            "tier_filed_tokens": tier_filed,
                            "tier_expected_tokens": tier_wanted,
                            "adaptive_cpu": adaptive is not None,
                            "adaptive_gpu": adaptive_gpu is not None,
                        }, post_persist=True)
                        continue
                self.passes_path(key).unlink(missing_ok=True)
                return claimed
        return None

    # -- self-healing ---------------------------------------------------

    def lease_age(self, action_key: str) -> float | None:
        record = _read_json(self.lease_path(action_key))
        if record is None:
            return None
        beat = record.get("heartbeat_unix")
        if not isinstance(beat, (int, float)):
            raise PoolContractError(f"lease has no heartbeat: {action_key}")
        return _now() - float(beat)

    def claim_intent_age(self, action_key: str) -> float | None:
        """Seconds since a claimant declared intent, or ``None`` without one.

        The intent marker precedes the claim rename, so it is the only clock
        that exists for a claimed record ``claim()`` has not yet rewritten.
        """

        record = _read_json(self.item_path(INTENT, action_key))
        if record is None:
            return None
        declared = record.get("intent_unix")
        if not isinstance(declared, (int, float)):
            return None
        return _now() - float(declared)

    def claim_intent_host(self, action_key: str, record: Mapping[str, object]) -> str | None:
        """The box that declared intent to claim this generation, if it said.

        ``claim`` writes the intent marker *before* the rename and rewrites the
        record with ``claimed_host`` after it, so a claimant blocked in between
        leaves a claim that names no box at all.  Reaped, that becomes a
        terminal record whose only hostname is the reaper's -- and a claim must
        not be able to be lost more anonymously than it was taken.

        Generation-scoped, because the marker outlives the claim it belongs to:
        nothing unlinks it on the success path, so a key republished after an
        earlier run still carries that run's marker until the next claimant
        overwrites it.  A marker older than the record's own publication
        describes a different generation and names the wrong box, so it is
        refused rather than guessed with.
        """

        marker = _read_json(self.item_path(INTENT, action_key))
        if marker is None:
            return None
        host = marker.get("host")
        declared = marker.get("intent_unix")
        published = record.get("published_unix")
        if not isinstance(host, str) or not host:
            return None
        if not isinstance(declared, (int, float)) or isinstance(declared, bool):
            return None
        if isinstance(published, (int, float)) and not isinstance(published, bool):
            if float(declared) < float(published):
                return None
        return host

    def claim_reservation_hosts(self, action_key: str) -> list[str]:
        """Every box whose ledger holds committed tokens for this action.

        The ledger is the *effect* of the rename that decides ownership, not a
        report of it: ``begin_acquire`` files a claimant's tokens under a
        private ``held/<handle>`` precisely because the owner is undecided
        while they are taken, and ``commit_acquire`` -- "called by the winner
        of the ready-to-claimed rename, and by nobody else" -- is what moves
        them to ``held/<action_key>``.  So this directory exists on the winner
        and can exist nowhere else, and a loser cannot appear here however the
        race went.  ``release`` rmdirs the holder, so an emptied one does not
        linger as a false answer.

        A list rather than a host, because more than one is a contradiction
        the ledger's own invariant forbids and a caller must be able to refuse
        rather than pick.
        """

        return sorted(
            directory.name
            for directory in _scan(self.root / RESERVATIONS)
            if (directory / "held" / action_key).is_dir()
        )

    def resolve_claim_holder(
        self, action_key: str, record: Mapping[str, object]
    ) -> str | None:
        """The box holding this claim, by the best evidence that exists.

        Read in this order, and the order is the point:

        1. **The ledger.**  Exact, and derived from the rename itself, so it
           cannot name a loser (#272).  A nonempty host in the mutable claim or
           lease must agree with it; disagreement is contradictory evidence,
           not permission to debit the recorded box.
        2. **The record.**  The claimed host, or the lease host for a widowed
           lease, remains the legacy answer when no reservation exists.  That
           is the zero-demand case, where naming a box costs the ledger nothing.
        3. **The intent marker.**  A proxy: written *before* the rename, so it
           names a claimant rather than the winner.  ``_write_claim_intent``
           writes by rename, so a loser that wrote after the winner replaced
           the winner's marker, and both pass the generation check.  A losing
           claimant now removes its own marker, which shrinks that window
           without closing it -- the removal is a read-then-unlink on a shared
           mount and can only ever degrade to no marker at all, which is the
           honest unknown this method already returns.

        ``AmbiguousClaimHolder`` means multiple ledgers name different boxes;
        callers must retain the claim and reservations rather than conclude it.
        ``None`` means nothing named a box.  Callers must not substitute the
        local hostname for it: the box asking is almost never the holder, and
        releasing against its ledger moves nothing while reporting a number
        that looks like it did.
        """

        recorded = [
            (field, value)
            for field in ("claimed_host", "host")
            if isinstance((value := record.get(field)), str) and value
        ]
        hosts = self.claim_reservation_hosts(action_key)
        if len(hosts) == 1:
            conflicts = [
                f"{field}={value!r}" for field, value in recorded
                if value != hosts[0]
            ]
            if conflicts:
                raise AmbiguousClaimHolder(
                    f"contradictory claim holder for {action_key}: "
                    f"{', '.join(conflicts)}, committed reservation on "
                    f"{hosts[0]!r}; claim and reservations retained"
                )
            return hosts[0]
        if hosts:
            # Two ledgers holding one action contradicts ``commit_acquire``'s
            # single-winner rule.  Refusing is the only answer that cannot
            # make it worse by choosing.
            raise AmbiguousClaimHolder(
                f"ambiguous claim holder for {action_key}: committed reservations "
                f"on {', '.join(hosts)}; claim and reservations retained"
            )
        if recorded:
            # Preserve the established no-ledger fallback order. A claim's own
            # field is stronger than the lease-shaped compatibility field.
            return recorded[0][1]
        return self.claim_intent_host(action_key, record)

    def claim_holder_pids(self, host: str | None = None) -> set[int]:
        """The pids on ``host`` that hold a claim of this queue right now.

        ``claim`` writes the lease before it returns and ``finish`` unlinks it,
        so this is exactly the set of loops between those two points --
        including one that has claimed an action and has not yet started
        anything to run it.  Nothing about that loop's process tree says so,
        which is why the question is asked here: the lease carries the
        claiming loop's own pid, and has since it was written.

        Host-qualified, because the queue is shared and a pid is a name only
        one box can resolve.  A lease naming another box is another box's
        business.

        Missing or unreadable ownership is unknown, not idle. A claim is
        renamed before its first lease is written, so inspect claimed items
        and refuse to authorize a signal while any ownership is unresolved.
        """

        host = socket.gethostname() if host is None else host
        pids: set[int] = set()
        for claim in _glob(self.dir(CLAIMED), "*.json"):
            lease = claim.with_suffix(".lease")
            record = _read_json(lease)
            if record is None:
                if not claim.exists():
                    continue  # Finished while the directory was being read.
                raise PoolContractError(f"claim ownership is unknown: {claim}")
            pid = record.get("pid")
            if (not isinstance(record.get("host"), str) or not record["host"]
                    or not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0):
                raise PoolContractError(f"claim ownership is invalid: {lease}")
            if record["host"] == host:
                pids.add(pid)
        return pids

    def _sweep_due(self, *, interval_s: float = HEARTBEAT_S) -> bool:
        """Claim this box's turn to run the reaper, or decline it.

        ``serve_once`` used to reap on every poll of every loop, and the
        reaper reads every claimed record *and its lease*.  A box runs many
        loops (one per class, grown by ``supervise``), and each of them polls
        about once a second while the queue is non-empty, so the box as a
        whole opened every ``claimed/<key>.lease`` in the pool tens of times a
        second -- files that a *different* box is writing to once per
        heartbeat.  Measured on ``dl380g10`` 2026-09-06 at load 0.44 across 80
        CPUs with no process in ``D``: three of twenty ``pb-queue`` file reads
        took 34.2 s, 40.3 s and 11.9 s (the rest under a millisecond), fifteen
        of eighteen worker loops sat in ``__break_lease`` simultaneously, and
        the box's offer aged past ``OFFER_TIMEOUT_S`` -- an idle 80-CPU box
        invisible to placement because its poll could not get back to
        ``announce``.

        The per-read cost differs by box and the throttle does not depend on
        which one applies.  ``dl380g10`` exports the pool, so its local open of
        a remotely-written file recalls that writer's NFSv4 delegation and
        blocks on the remote client; that is why it pays most.  ``sparklina``
        is an ordinary client and was caught with all three of its loops in
        ``D`` on ``rpc_wait_bit_killable`` / ``do_renameat2`` /
        ``open_last_lookups`` at box load 3.5, and ``sparky`` took its own turn
        at an expired offer while ``dl380g10`` was live.  The stale role
        rotates, so the thing to reduce is the multiplier the boxes share --
        loops times polls times records -- not one box's filesystem role.

        The interval is derived, for the conclusions the reaper draws from
        leases.  Every one of those is a statement about a lease, and a
        lease's own writer refreshes it every ``HEARTBEAT_S``; the grace
        ``reap_stale`` applies to a claim with no lease at all is
        ``HEARTBEAT_S`` too.  So no lease input to the sweep can change more
        often than that, and a second sweep inside one heartbeat re-reads
        bytes that cannot have moved.  Detection is unaffected in kind: a
        lease expires at ``LEASE_TIMEOUT_S`` and is noticed within one
        heartbeat of expiring, by this box or by any other box polling the
        same pool.

        One branch of ``reap_stale`` is not derived and is accepted instead.
        ``finish_pending`` retries the saved outcome of a payload that has
        already returned but whose kernel scope has not drained; its input is
        cgroup state, which moves on its own clock, and its capacity return is
        therefore delayed by up to ``HEARTBEAT_S``.  That is a bounded delay
        in giving tokens back, weighed against a poll cycle that measured
        longer than ``OFFER_TIMEOUT_S`` and cost the box all of its capacity.
        Retrying it off this schedule needs a host-local record of which keys
        are pending, which is the enumeration this method exists to avoid.

        The marker is host-local and unlocked on purpose.  Taking the
        admission lock to decide whether to sweep would put this decision
        behind the very NFS waits it exists to prevent, and losing the race
        costs one extra sweep, which is exactly what the code did before.
        """

        try:
            directory, digest = cpu_admission.box_state(self.ledger().base)
        except Exception:                                        # noqa: BLE001
            # No host-local rendezvous (a read-only or absent ``/tmp``, an
            # unresolvable ledger): sweep, as this method's caller always did.
            return True
        marker = directory / f"{digest}.sweep"
        now = _now()
        try:
            if 0 <= now - marker.stat().st_mtime < interval_s:
                return False
        except (FileNotFoundError, NotADirectoryError):
            pass
        except OSError:
            return True
        try:
            descriptor = os.open(marker, os.O_CREAT | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        except OSError:
            return True
        try:
            os.utime(descriptor, (now, now))
        except OSError:
            pass
        finally:
            os.close(descriptor)
        return True

    def reap_stale(self, *, timeout_s: float = LEASE_TIMEOUT_S) -> list[str]:
        """Return claims whose lease has expired to ``ready``.

        Completed payloads awaiting cleanup carry ``finish_pending``. Their
        claiming host retries the saved outcome on every poll, even with a
        fresh lease: asynchronous scope termination is not a lost lease or a
        new attempt. Capacity remains held until cleanup is proved complete.

        A missing lease file also counts as stale: it means the claimant died
        between the rename and the first heartbeat.  A stale claim is returned
        only while its generation has no filed outcome.  Once ``done`` or
        ``failed`` carries the same ``published_unix``, that terminal record is
        authoritative and the stranded claim is concluded instead; a CAS hit
        does not license putting already-terminal work back in the queue.

        **The claim is not atomic with its lease.**  ``claim()`` renames the
        item, then rewrites the record with ``claimed_unix``, then writes the
        lease; a reaper running inside that window sees a claimed item with no
        lease and would requeue a worker that is alive and about to start.  So
        a missing lease is only stale once the claim itself has aged past
        ``grace_s``.  The clock for that is ``claimed_unix`` once the record
        carries it -- and before it does, the claim-intent marker, which
        ``claim()`` writes *before* the rename.  Between the rename and the
        record rewrite the claimed file is still the ready record, with no
        ``claimed_unix`` at all; on NFS that stretch spans two directory scans
        and is hundreds of milliseconds wide, and reading it as "no clock, so
        stale" requeued a live seven-second action within a second of its
        claim and let a retry's refusal stand as its outcome (issue #36).  A
        genuinely dead claimant still gets reaped, one grace period later.
        The default grace is the heartbeat interval, far shorter than the
        lease timeout that governs the normal case.

        It is **not** longer than the window actually spans, which is what this
        paragraph used to say (#222).  Measured on ``dl380g10`` on 2026-09-06,
        read-only, at load 0.44 across 80 CPUs with nothing in ``D``: one
        pool-record operation took 45.001 s against a 30 s grace, on NFSv4
        delegation recalls of the directories another box rewrites.  The window
        has no upper bound here, so no grace is safe and a larger one is only a
        larger guess.  The grace still decides *when* a leaseless claim is
        taken; what the taking costs is decided below, by asking whether
        anything ever ran under it rather than how long the claimant took to
        say so.

        **This loop reads its records loudly, and the sibling sweeps do not.**
        ``ready_items`` and ``quarantine_orphans`` treat an entry that goes
        away under the ``glob`` as ordinary, including when it arrives as
        ``ESTALE`` through a cached directory handle (#212).  Here ``None`` is
        not "skip an entry", it is a verdict about whether a claim concluded,
        and ``ESTALE`` can carry a state ``ENOENT`` cannot: ``finish()``
        publishes ``finish_pending`` by atomically *replacing* this file, so a
        stale handle can answer "absent" for a record that is present and
        newer.  The ``finish_pending`` guard above deliberately precedes the
        lease check, because a payload awaiting container cleanup is no longer
        heartbeating -- ``execute`` refreshes the lease only while the child
        runs -- so an expired lease is that state's steady condition, not
        evidence against it.  A tolerated ``ESTALE`` on the first read would
        walk past the guard and reap a completed action as ``lease_lost``.
        Tolerating only the first read and skipping the entry would be sound,
        but it is an asymmetry inside the reaper bought for a race nobody has
        observed on this path, and it invites the next reader to "finish" it
        on the read below, where it is not sound.  So the reaper stays loud
        and says why.

        The guard is also re-asked on the read this loop acts from (#215).
        The first read decides whether to guard; the later read is what
        container cleanup, the superseded filing, the attempt archive and the
        requeue all hang off, and an atomic replace publishing
        ``finish_pending`` between the two was an ordinary claim to the guard
        and a pending finish to nothing.  Re-asking there costs one comparison
        on a record already in hand, and makes the guard describe the bytes
        this loop is about to act on rather than the bytes that sent it here.
        """

        grace_s = HEARTBEAT_S
        requeued: list[str] = []
        claimed = self.dir(CLAIMED)
        if not claimed.is_dir():
            return requeued
        terminal_keys = self.terminal_keys()
        for path in sorted(claimed.glob("*.json")):
            key = path.stem
            with self._transition_locked(key, blocking=False) as acquired:
                if not acquired:
                    continue
                record = _read_json(path)
                pending_finish = (record or {}).get("finish_pending")
                if pending_finish is not None:
                    # Only the owner host can prove its kernel scope is empty.
                    # A payload has already returned here, so lease freshness is
                    # irrelevant; preserve that result rather than file lease_lost.
                    if record.get("claimed_host") != socket.gethostname():
                        continue
                    if (not isinstance(pending_finish, dict)
                            or not isinstance(pending_finish.get("status"), str)
                            or not isinstance(pending_finish.get("detail"), dict)):
                        raise PoolContractError(f"invalid pending finish for {key}")
                    try:
                        result = self.finish(
                            key, status=pending_finish["status"],
                            detail=pending_finish["detail"], claim_snapshot=record,
                        )
                    except AmbiguousClaimHolder as exc:
                        print(f"pool reaper: {exc}", file=sys.stderr)
                        continue
                    if result == self.item_path(READY, key):
                        requeued.append(key)
                    continue
                age = self.lease_age(key)
                if age is not None and age <= timeout_s:
                    continue
                if age is None:
                    record = _read_json(path) or {}
                    claimed_unix = record.get("claimed_unix")
                    if isinstance(claimed_unix, (int, float)):
                        if _now() - float(claimed_unix) <= grace_s:
                            continue          # claimed moments ago; lease imminent
                    else:
                        intent_age = self.claim_intent_age(key)
                        if intent_age is not None and intent_age <= grace_s:
                            # Renamed moments ago; the claimant's rewrite, lease and
                            # its own terminal/withdrawal checks are imminent.  Nothing
                            # below may touch this record: a conclusion here would
                            # release tokens the claimant is about to hold.
                            continue
                record = _read_json(path)
                # The claim exactly as it was read, before the archiving below
                # rewrites its attempt fields.  ``_entomb_claim`` compares against
                # this so the loop can only move aside the claim it judged.
                read_claim = dict(record) if record is not None else None
                if record is None:
                    # The claim concluded under us.  Both ``finish()`` and this
                    # loop write the item's next home and only then unlink the
                    # claimed file, so a reaper that globbed before that unlink
                    # reads nothing back here -- and two reapers on two boxes race
                    # each other for exactly this window.  Treating the absence as
                    # an empty record and requeueing it writes a stub with no
                    # ``action_key`` over whatever the winner just filed, and
                    # ``claim()`` skips a keyless item forever: the item never
                    # runs, never fails, and sits at the head of ``ready`` denying
                    # every worker that polls past it.  There is nothing to reap --
                    # the winner filed the item and released its capacity -- so the
                    # loser's only correct move is to leave it alone.
                    continue
                if record.get("finish_pending") is not None:
                    # The claim became a pending finish under us.  ``finish``
                    # publishes that state by atomically *replacing* this file, so
                    # it can land after the guard above read an ordinary claim --
                    # and the guard is where the owner-host rule lives.  Acting on
                    # this read without re-asking would run a foreign box's
                    # container cleanup and file ``lease_lost`` over a payload that
                    # has already returned.  Leave it: the next cycle's first read
                    # is the guard's read, and it decides on the owner's box under
                    # the rule that belongs to it.
                    continue
                # One holder, resolved once, before any branch below concludes
                # this claim -- because every one of them releases the claim's
                # tokens, and tokens are filed under the ledger of the box that
                # committed them.  ``claim`` commits them the moment it wins the
                # rename and rewrites the record with ``claimed_host`` only after
                # that, so a claim lost in between is holding real capacity on a
                # box this record does not name.  Releasing that against the
                # default ledger names the reaper instead, whose ``held/<key>``
                # does not exist: the release returns 0 and moves nothing, the
                # holder keeps its tokens, and a reservation outlives its holder
                # -- the starvation shape, reached by an accounting error rather
                # than by a missed call (#261).
                #
                # The intent marker precedes the rename and does name the box, so
                # the recovery is the one #227 added; what changes is that its
                # answer now reaches the release as well as the record.  Both, so
                # the ledger this loop debits and the hostname its terminal record
                # carries are the same box.
                #
                # Read here for the second reason too: the requeue branch below
                # strips ``claimed_host`` on its way to ``ready``, so this is the
                # last point at which every path can still ask.
                #
                # Contradictory ledger evidence must stop even cleanup from making
                # a choice. Refuse just this claim so healthy work can still recover.
                try:
                    # A host's token holder names the key, not its attempt. An
                    # expired lease can still contradict a stale claim read;
                    # refuse before payload cleanup or any ownership mutation.
                    _check_claim_lease_identity(key, record, _read_json(self.lease_path(key)))
                    holder = self.resolve_claim_holder(key, record)
                except AmbiguousClaimHolder as exc:
                    print(f"pool reaper: {exc}", file=sys.stderr)
                    continue
                if holder is not None:
                    record["claimed_host"] = holder
                container_cleanup = self.cleanup_action_containers(record, reason="lease_lost")
                if not container_cleanup["complete"]:
                    pending = dict(record)
                    self._note_cleanup_attempt(pending, record, container_cleanup)
                    _write_json_atomic(path, pending)
                    continue
                terminal = self.terminal_outcome_covers(
                    record, action_key=key, terminal=terminal_keys
                )
                if terminal is not None:
                    # The worker already filed this exact generation.  A stale
                    # directory view or a cycle racing the final unlink may still
                    # expose its old claim, but that copy is cleanup, not a retry.
                    state, outcome = terminal
                    self._file_superseded(
                        record, key=key, kind="terminal-claim", status="dropped",
                        dropped_unix=_now(), dropped_host=socket.gethostname(),
                        reason="stale claim belongs to a generation already filed "
                               f"under {state}",
                        terminal_status=outcome.get("status"),
                    )
                    self._release_reservation(
                        key, host=holder if isinstance(holder, str) else socket.gethostname(),
                        # Judged on the record that was *filed*, not on this
                        # stale copy: the ending already concluded, and if it
                        # kept its tier tokens the bytes are on the stage.
                        # Releasing them here would leave occupancy nothing can
                        # attribute -- every reclaim path (an egress, the orphan
                        # sweep) walks the tier's held keys, so a key released
                        # while its files remain is capacity no mechanism can
                        # ever take back.
                        keep_tier=self.pin_holds_tier_tokens(outcome, key))
                    path.unlink(missing_ok=True)
                    self.lease_path(key).unlink(missing_ok=True)
                    continue
                if self.withdrawal_covers(record, action_key=key) is not None:
                    # A withdrawal that could not finish its own cleanup -- the
                    # operator's box died mid-verb, say -- leaves a claimed record
                    # whose lease nobody refreshes.  Requeueing that is the one
                    # thing withdrawal exists to prevent, so conclude it here
                    # instead: capacity back, records gone, nothing counted as
                    # reaped because nothing was returned to the pool.
                    tombstone, mine = self._entomb_claim(key, expect=read_claim)
                    if not mine or tombstone is None:
                        continue
                    self.lease_path(key).unlink(missing_ok=True)
                    self._release_reservation(
                        key, host=holder if isinstance(holder, str) else socket.gethostname())
                    tombstone.unlink(missing_ok=True)
                    continue
                # The filename is the identity; a record that disagrees with it, or
                # has lost it, must not be written back to a queue directory where
                # every consumer addresses items by key.
                record["action_key"] = key
                prior_attempts = int(record.get("attempts", 0))
                if (age is None
                        and record.get("withdrawn_unix") is None
                        and not self.attempt_path(
                            record, prior_attempts + 1).exists()):
                    # Nothing ever ran under this claim, so nothing failed under
                    # it.  ``claim`` writes the lease before it returns and
                    # ``execute`` writes the child pid into it before the payload
                    # is launched, so a claim with no lease at all never reached a
                    # launch; and no attempt is published under the number this
                    # claim would take, so no other writer recorded one either.
                    #
                    # Both halves are needed.  A lease is also absent from a claim
                    # a *finisher* archived and then died holding:
                    # ``sweep_finish_tombstones`` restores exactly that record, and
                    # relies on this path archiving at the same attempt number so
                    # first-writer-wins hands the item the finisher's real outcome.
                    # ``finish`` publishes the attempt before it entombs the claim,
                    # so the attempt on disk is what tells the two apart.
                    #
                    # Charging an attempt here is what turned a stalled claimant
                    # into lost work: measured on this fleet, a single pool-record
                    # operation took 45 s against a 30 s grace, and
                    # ``0a44f2e0f62c`` came back ``lease_lost_max_attempts`` with
                    # empty stdout and stderr -- an action that never started,
                    # unrunnable, out of a queue another box could have taken it
                    # from.  Widening the grace only moves the guess; the window
                    # has no upper bound on this filesystem.  Releasing does not
                    # need one, because it asks what happened rather than how long
                    # it took.
                    #
                    # A withdrawn claim is the one thing a release must not
                    # touch. ``withdraw`` closes the retry with an immutable,
                    # generation-scoped decision, and ``withdrawal_covers`` reads
                    # that decision even after a later publication retires the
                    # visible summary. A release does not charge an attempt, but
                    # it would put work the operator cancelled straight back in
                    # the queue, so this branch explicitly excludes any covered
                    # generation.
                    #
                    # This does not stop a reaper taking a live-but-blocked
                    # claimant's claim -- that is not knowable across boxes.  It
                    # stops the taking from destroying the work. A resumed
                    # claimant must pass the exact-claim launch/heartbeat guards.
                    # Its late report cannot use the uncharged attempt number:
                    # that slot now belongs to the successor's first execution.
                    self._file_superseded(
                        record, key=key, kind="unstarted-claim", status="released",
                        released_unix=_now(), released_host=socket.gethostname(),
                        reason="claim released without an attempt: no lease was "
                               "written and no attempt was published",
                    )
                    # Counted, not bounded.  A bound would be the constant this
                    # issue exists to avoid, and a release costs an execution
                    # nothing; a key whose count climbs is a box that cannot start
                    # work, which is the thing to go and look at.
                    record["unstarted_releases"] = int(
                        record.get("unstarted_releases", 0) or 0) + 1
                    destination = self._shape_as_ready_item(record, action_key=key)
                    tombstone, mine = self._entomb_claim(key, expect=read_claim)
                    if not mine:
                        # A retry is live under this key and owns everything the
                        # branch below would have taken.  Nothing has been written
                        # outside the superseded filing, which is evidence rather
                        # than state.
                        continue
                    self.lease_path(key).unlink(missing_ok=True)
                    self._release_reservation(
                        key, host=holder if isinstance(holder, str) else socket.gethostname())
                    try:
                        _write_json_atomic(destination, record)
                    except OSError:
                        if tombstone is not None:
                            try:
                                os.link(tombstone, path)
                            except OSError:
                                pass
                            else:
                                tombstone.unlink(missing_ok=True)
                        continue
                    if tombstone is None:
                        path.unlink(missing_ok=True)
                    else:
                        tombstone.unlink(missing_ok=True)
                    requeued.append(key)
                    continue
                attempts = prior_attempts + 1
                limit = int(record.get("max_attempts", DEFAULT_MAX_ATTEMPTS))
                if (
                    prior_attempts
                    and "attempt_history" not in record
                    and "attempt_history_missing_before" not in record
                ):
                    # Runtime rollout can meet a record already requeued by older
                    # bytes.  State the irrecoverable prefix honestly and archive
                    # from this attempt onward; inventing links would be worse,
                    # while refusing the record would strand live work.
                    record["attempt_history_missing_before"] = prior_attempts
                terminal = attempts >= limit
                finished_unix = _now()
                finished_host = socket.gethostname()
                attempt_record = dict(record)
                attempt_record.update(
                    {
                        "finished_unix": finished_unix,
                        "finished_host": finished_host,
                    }
                )
                telemetry = (container_cleanup.get("resource_scope") or {}).get("telemetry") or {}
                resource_failure = self._resource_failure(telemetry)
                recovery_detail = {
                    "reason": "claim lease expired before an outcome was filed",
                    "lease_age_s": age,
                }
                if telemetry:
                    recovery_detail["resource_telemetry"] = telemetry
                if resource_failure:
                    recovery_detail.update(termination_reason=resource_failure,
                                           termination_evidence=telemetry.get("termination_evidence"),
                                           returncode=137)
                record["attempt_history"] = self.archive_attempt(
                    attempt_record,
                    attempt=attempts,
                    status="failed" if resource_failure else (
                        "lease_lost_max_attempts" if terminal else "lease_lost"),
                    disposition=FAILED if terminal else "requeued",
                    detail=recovery_detail,
                )
                record["attempts"] = attempts
                adopted = self.adopted_attempt_summary(record)
                record.update(
                    {
                        "status": adopted["status"],
                        "finished_unix": adopted["finished_unix"],
                        "finished_host": adopted["finished_host"],
                        "detail": adopted["detail"],
                    }
                )
                disposition = adopted["disposition"]
                if disposition in {DONE, FAILED}:
                    # The immutable winner may be the finisher, not this reaper.
                    # File the exact transition it proved rather than the local
                    # lease observation that lost the first-writer race.
                    record["schema"] = POOL_OUTCOME_SCHEMA_V1
                    destination = self.item_path(str(disposition), key)
                else:
                    destination = self._shape_as_ready_item(record, action_key=key)
                # Same ordering as ``finish``, and for the same reason: this loop
                # published the requeue and only then unlinked the claim and lease,
                # so a worker that claimed the requeue inside that window had its
                # claim and lease deleted by this reaper.
                tombstone, mine = self._entomb_claim(key, expect=read_claim)
                if not mine:
                    # A retry is live under this key: its claim, lease and
                    # reservation are its own.  This loop has written nothing
                    # outside the attempt archive, which is immutable and
                    # first-writer-wins, so leaving now costs the key nothing.
                    continue
                self.lease_path(key).unlink(missing_ok=True)
                # Whatever the outcome, the dead claimant's capacity goes back.  A
                # reservation outliving its holder is the starvation bug's shape.
                self._release_reservation(
                    key, host=holder if isinstance(holder, str) else socket.gethostname())
                try:
                    _write_json_atomic(destination, record)
                except OSError:
                    # Put the claim back rather than leave the key with no record
                    # anywhere.  Link first: the tombstone must not replace a claim
                    # that appeared while this was in flight.
                    if tombstone is not None:
                        try:
                            os.link(tombstone, path)
                        except OSError:
                            pass
                        else:
                            tombstone.unlink(missing_ok=True)
                    continue
                if tombstone is None:
                    path.unlink(missing_ok=True)
                else:
                    tombstone.unlink(missing_ok=True)
                requeued.append(key)
        self.sweep_widowed_leases(timeout_s=timeout_s)
        self.sweep_stale_acquisitions()
        self.sweep_finish_tombstones()
        self.sweep_ready_transitions()
        self.quarantine_orphans()
        return requeued

    def sweep_finish_tombstones(
        self, *, grace_s: float = LEASE_TIMEOUT_S
    ) -> list[str]:
        """Recover a claim whose finisher died with it moved out of the way.

        ``finish`` and ``reap_stale`` move a claim to a tombstone, publish the
        item's next home, then delete the tombstone.  A process killed inside
        that window leaves a record that no consumer addresses: the key is in
        neither ``ready`` nor ``claimed``, so nothing claims it, nothing reaps
        it, and its waiter never sees an outcome.  This is the only thing that
        looks.

        Three dispositions, and which one applies is decided by what else the
        key has, never by what the tombstone says about itself:

        *   A record in ``ready`` or ``claimed``, **of any generation**, means
            the key has moved on.  Re-injecting these bytes could only start a
            fight with a live record, so the tombstone is filed as evidence and
            removed.  Any generation, not just this one: a crash in this window
            leaves the key addressable nowhere, so a submitter re-publishes it,
            and restoring the old generation over that would have the reaper
            requeue it straight over the new one.
        *   A terminal record of the *same* generation means the publish landed
            and the tombstone is redundant cleanup.  Filed and removed.
        *   Otherwise the publish did not land: link the record back to
            ``claimed/<key>.json`` and let the ordinary reaper conclude it.
            Its lease is already gone, so the missing-lease path applies one
            grace later, and it charges the same attempt number the finisher
            archived -- ``archive_attempt`` is first-writer-wins, so the
            finisher's real outcome is what the record adopts, not this
            reaper's lease observation.

        A record whose attempt links no longer verify is filed rather than
        restored.  Restoring it would hand ``reap_stale`` a record that raises
        from ``archive_attempt``, and that exception stops reaping on every box
        for as long as the record exists.  Verification therefore has to cover
        every way a link can fail to resolve, not only the ones the queue
        itself judges: a missing or tampered immutable outcome raises out of
        ``core``, not out of the pool's contract error, and an escape here
        causes the exact stall this paragraph is about -- ``reap_stale`` calls
        this sweep unguarded, and ``serve_once`` calls ``reap_stale`` before it
        claims.  Being *unable to look* is a third answer and takes neither
        disposition: the tombstone is left for the next sweep, because filing
        it would hide it in ``withdrawn/superseded/`` on the strength of a
        stale directory handle.

        The grace is the lease timeout: nothing is blocked behind this except
        the action's own visibility, and a sweep that fires while a finisher is
        mid-publish would put a claim beside a ready record of the same
        generation.

        Exact-scope late finishes use a separate suffix. They are already
        awaiting cleanup, never restored as claims, and retried immediately
        on their owning host under the same key lock.
        """

        swept: list[str] = []
        claimed = self.dir(CLAIMED)
        if not claimed.is_dir():
            return swept
        now = _now()
        for tombstone in sorted([*claimed.glob(f"*{TOMBSTONE_SUFFIX}"),
                                 *claimed.glob(f"*{LATE_FINISH_SUFFIX}")]):
            parts = tombstone.name.split(".", 2)
            key = parts[0]
            if len(key) != 64 or any(ch not in "0123456789abcdef" for ch in key):
                continue
            with self._transition_locked(key, blocking=False) as acquired:
                if not acquired:
                    continue
                if tombstone.name.endswith(LATE_FINISH_SUFFIX):
                    # This record never owns the action's current claim or
                    # tokens. Retry only its saved scope, even beside a live
                    # successor, without the ordinary tombstone dispositions.
                    try:
                        record = _read_json(tombstone)
                        if record is None or record.get("action_key") != key:
                            raise PoolContractError("invalid late-finish recovery record")
                        if record.get("claimed_host") != socket.gethostname():
                            continue
                        result = self._retry_late_finish(tombstone, record)
                    except Exception as exc:                         # noqa: BLE001
                        print(f"pool: late finish {key} retained: {type(exc).__name__}: {exc}",
                              file=sys.stderr)
                        continue
                    if result != tombstone:
                        swept.append(key)
                    continue
                when: float | None = None
                if len(parts) >= 2:
                    try:
                        when = int(parts[1]) / 1_000_000.0
                    except ValueError:
                        when = None
                if when is None:
                    try:
                        when = tombstone.stat().st_mtime
                    except OSError:
                        continue
                if now - when <= grace_s:
                    continue
                record = _read_json(tombstone)
                live = any(
                    self.item_path(state, key).exists()
                    for state in (READY, CLAIMED)
                )
                covered = (
                    self.terminal_outcome_covers(record, action_key=key) is not None
                    or self.withdrawal_covers(record, action_key=key) is not None
                )
                restorable = record is not None and not live and not covered
                if restorable and "attempt_history" in record:
                    try:
                        self.attempt_outcomes(record)
                    except (PoolContractError, FileNotFoundError, pb.CASTamperError):
                        # ``attempt_outcomes`` decides most of this by reading the
                        # record and raises ``PoolContractError``, but the link it
                        # checks last is a *file*: ``_open_regular_nofollow``
                        # raises a bare ``FileNotFoundError`` when the immutable
                        # outcome is absent and ``CASTamperError`` when the entry
                        # is not a readonly regular file, and neither derives from
                        # ``PoolContractError``.  Both are positive evidence that
                        # the link does not verify, which is this branch's whole
                        # question, so both file the record rather than restore it.
                        restorable = False
                    except pb.CASUnavailableError:
                        # "Could not look" is not evidence, and it must not be
                        # answered either way.  Restoring risks the reaping stall
                        # this guard exists to prevent; filing moves the record
                        # into ``withdrawn/superseded/``, which every reader is
                        # documented to ignore, so a stale handle would lose the
                        # action permanently.  Leave the tombstone for the next
                        # sweep -- the only disposition that keeps the evidence.
                        continue
                if restorable:
                    try:
                        os.link(tombstone, self.item_path(CLAIMED, key))
                    except OSError:
                        pass
                    else:
                        tombstone.unlink(missing_ok=True)
                        swept.append(key)
                        continue
                # A cancellation can be durable before its finisher returns
                # tokens. If that finisher dies after removing the lease,
                # this tombstone is the only remaining cleanup authority.
                # A queued successor has not acquired anything yet: retain its
                # READY bytes while completing the predecessor's cleanup.
                # A claimed successor, however, owns the key now; none of its
                # resources or lease belongs to this old tombstone.
                if f"{key}.json" not in os.listdir(claimed):
                    try:
                        holders = self.claim_reservation_hosts(key)
                        if len(holders) > 1 or (record is None and holders):
                            continue
                        if record is not None:
                            holder = record.get("claimed_host")
                            if holders:
                                if holder and holder != holders[0]:
                                    continue
                                record["claimed_host"] = holders[0]
                            cleanup = self.cleanup_action_containers(
                                record, reason="interrupted finish cleanup")
                            if not cleanup["complete"]:
                                continue
                        if holders:
                            # The ending is already filed -- that is what made
                            # this tombstone unrestorable -- so if it pinned a
                            # staged range, its tier tokens are not this
                            # cleanup's to return.  Releasing them would leave
                            # the files on the stage with nothing holding them.
                            self._release_reservation(
                                key, host=holders[0],
                                keep_tier=self._filed_pin_holds(key))
                            if self.claim_reservation_hosts(key):
                                continue  # a partial return still needs this owner
                        lease = _read_json(self.lease_path(key))
                        if (record is not None and lease is not None
                                and lease.get("owner") == record.get("claimed_by")):
                            self.lease_path(key).unlink(missing_ok=True)
                    except (OSError, PoolContractError, pb.CASUnavailableError):
                        continue
                self._file_superseded(
                    record, key=key, kind="finish-tombstone", status="dropped",
                    dropped_unix=_now(), dropped_host=socket.gethostname(),
                    reason="a finisher was interrupted between moving its claim "
                           "aside and publishing the item's next home; the key "
                           "already has a live or terminal record, so these bytes "
                           "are evidence rather than work",
                )
                tombstone.unlink(missing_ok=True)
                swept.append(key)
        return swept

    def sweep_stale_acquisitions(
        self, *, grace_s: float = LEASE_TIMEOUT_S
    ) -> list[str]:
        """Free tokens a claimant on any box took and never committed.

        Every host's ledger, not just this one: a claimant that died between
        ``begin_acquire`` and ``commit_acquire`` left its tokens under its own
        box's reservations, and the box that notices may not be that box.
        ``reap_stale`` already releases a dead claimant's tokens from whatever
        host held them, so a foreign write here is the established shape and
        not a new one.
        """

        swept: list[str] = []
        for directory in _scan(self.root / RESERVATIONS):
            if not directory.is_dir():
                continue
            swept.extend(
                f"{directory.name}/{name}"
                for name in self.ledger(directory.name).sweep_stale_acquisitions(
                    grace_s=grace_s
                )
            )
        # Tier ledgers too: a claimant that died between taking a tier's
        # tokens and committing them left a private directory that no key
        # names, on a ledger no box's reaper walks.
        for tier_id in self.tier_ids():
            try:
                names = self.tier_ledger(tier_id).sweep_stale_acquisitions(grace_s=grace_s)
            except (OSError, PoolContractError):
                # Same containment as the release path: an unreadable tier
                # costs this cycle that tier's sweep, never the rest of the
                # reaper's work on every other key.
                continue
            swept.extend(f"{TIER_RESERVATIONS}/{tier_id}/{name}" for name in names)
        return swept

    def sweep_widowed_leases(self, *, timeout_s: float = LEASE_TIMEOUT_S) -> list[str]:
        """Remove leases in ``claimed/`` whose item record is gone.

        Both cleanup paths unlink the lease beside the record they conclude --
        ``finish`` on every branch, ``reap_stale`` on every outcome -- so a
        lease with no record should not exist.  One did: ``daf08495c8bb`` sat
        in the live queue for seven and a half hours, its pid long dead, with
        no ``.json`` beside it and no mechanism that would ever look at it
        again.  How it was widowed is not established, and this sweep is not a
        theory about that; it is the observation that nothing swept it.

        It reads as live work to anything counting ``claimed/``, which is what
        an operator reads when asking whether the fleet is busy, and it is the
        one shape ``quarantine_orphans`` does not cover -- that sweep is over
        ``ready``, this one is its mirror.

        Aged past ``timeout_s`` before removal, for the same reason
        ``reap_stale`` waits: ``claim()`` writes the lease *after* the rename,
        so a lease that briefly has no record beside it may simply be a claim
        mid-flight in the other direction.  Host tokens still held under the
        key go back, because a reservation outliving its holder is the
        starvation bug's shape -- but tier tokens do not, when the key's filed
        ending pins a staged range: those are meant to outlive the claim, and
        releasing them leaves occupancy nothing can attribute.
        """

        swept: list[str] = []
        claimed = self.dir(CLAIMED)
        if not claimed.is_dir():
            return swept
        now = _now()
        for lease in sorted(claimed.glob("*.lease")):
            key = lease.name[: -len(".lease")]
            with self._transition_locked(key, blocking=False) as acquired:
                if not acquired:
                    continue
                if self.item_path(CLAIMED, key).exists():
                    continue
                record = _read_json(lease) or {}
                beat = record.get("heartbeat_unix")
                try:
                    age = now - float(beat)
                except (TypeError, ValueError):
                    try:
                        age = now - lease.stat().st_mtime
                    except OSError:
                        continue
                if age <= timeout_s:
                    continue
                try:
                    host = self.resolve_claim_holder(key, record)
                except AmbiguousClaimHolder as exc:
                    print(f"pool lease sweep: {exc}", file=sys.stderr)
                    continue
                if host is not None:
                    record["host"] = host
                container_cleanup = self.cleanup_action_containers(record)
                if not container_cleanup["complete"]:
                    continue
                # "Any tokens still held under the key go back" is right for a
                # host ledger, where a reservation outliving its holder is the
                # starvation bug.  It is wrong for a tier: a mover's tier
                # tokens are *meant* to outlive its claim, from ``finish``
                # until an egress deletes the bytes, so a widowed lease beside
                # a filed ending that pins must leave them alone.
                self._release_reservation(key, host=host,
                                         keep_tier=self._filed_pin_holds(key))
                lease.unlink(missing_ok=True)
                swept.append(key)
        return swept

    def _file_unreadable(self, path: Path, *, reason: str) -> str | None:
        """Take one unparseable queue record out of the live queue, loudly.

        The original bytes and a bounded diagnostic go to ``superseded/``
        so whoever has to find the writer still can; a
        record with the file's own name goes to ``failed/`` because that is
        what ``pbstatus`` and ``pbwait`` read, and a defect nobody counts is
        the silence this sweep exists to end.

        Never over a terminal record.  A corrupt ready file says nothing about
        an ending already filed for that key, and a key with two terminals is
        a worse defect than the one being cleaned up.
        """

        key = path.stem
        # Take the bytes out of the live namespace before diagnosing or filing
        # them. A later publish must not be unlinked by this sweep. Preserve the
        # complete original alongside the bounded inline diagnostic.
        evidence = self.superseded_dir() / f"{key}.{uuid.uuid4().hex}.unreadable.raw"
        evidence.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.rename(path, evidence)
        except FileNotFoundError:
            return None
        try:
            repaired = _read_json(evidence)
        except PoolContractError:
            repaired = None
        else:
            if repaired is not None:
                # The producer repaired/replaced the record after the scan.
                # Restore without overwriting another concurrent publication.
                try:
                    os.link(evidence, path)
                except FileExistsError:
                    pass  # the replacement remains available as evidence
                else:
                    evidence.unlink()
                return None
        raw = evidence.read_bytes()
        self._file_superseded(
            None, key=key, kind="unreadable", state=READY,
            status="unreadable_record",
            filed_unix=_now(), filed_host=socket.gethostname(),
            reason=reason, raw_bytes=len(raw),
            raw_path=str(evidence.relative_to(self.root)),
            raw_head=raw[:UNREADABLE_HEAD_BYTES].decode("utf-8", "replace"),
        )
        if not any(self.item_path(state, key).exists()
                   for state in (DONE, FAILED)):
            pb._atomic_publish(
                self.item_path(FAILED, key),
                pb._canonical_bytes({
                    "schema": POOL_OUTCOME_SCHEMA_V1,
                    "action_key": key,
                    "status": "unreadable_record",
                    "finished_unix": _now(),
                    "finished_host": socket.gethostname(),
                    "detail": {
                        "reason": "the ready record could not be parsed, so no "
                                  "worker could ever claim it; its bytes are "
                                  "kept under withdrawn/superseded/",
                        "parse_error": reason,
                        "bytes": len(raw),
                    },
                }),
            )
        return key

    @staticmethod
    def _ready_record_usable(record: Mapping[str, object], key: str) -> bool:
        # The ordering fields are here for the same reason the addressing ones
        # are: ``ready_items`` skips a record it cannot place in the queue's
        # order, so such a record is never listed, never claimed, never runs
        # and never leaves ``ready`` -- a permanent resident of the state that
        # reports work the fleet will not do (#612).  Filing it is the point.
        return bool(
            record.get("action_key") == key
            and record.get("worker_script") and record.get("cas_root")
            and (record.get("checkout_root") or record.get("checkout_snapshot"))
            and PoolQueue._unorderable_queue_field(record) is None
        )

    def _capture_ready_transition(self, path: Path, *, kind: str) -> Path | None:
        """Move READY bytes to an address the reaper can recover after a crash.

        The caller holds the key transition lock through its final disposition.
        The source name is unique and immutable once captured. Superseded is
        only a final evidence destination, never an in-flight recovery address.
        """
        captured = self.root / "ready-transitions" / (
            f"{path.stem}.{int(_now() * 1_000_000)}.{uuid.uuid4().hex}.{kind}.json"
        )
        captured.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.rename(path, captured)
        except FileNotFoundError:
            return None
        return captured

    def _restore_ready_transition(self, captured: Path, key: str) -> bool:
        """Restore without replacing a publication; retain any failed restore."""
        try:
            os.link(captured, self.item_path(READY, key))
        except OSError:
            return False
        captured.unlink(missing_ok=True)
        return True

    def _finish_ready_transition(self, captured: Path) -> None:
        """Retain original bytes after a durable ending or successor is known."""
        evidence = self.superseded_dir() / (captured.name + ".ready-source")
        evidence.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.rename(captured, evidence)
        except FileNotFoundError:
            pass

    def sweep_ready_transitions(self, *, grace_s: float = LEASE_TIMEOUT_S) -> list[str]:
        """Recover interrupted READY examinations under the same key lock.

        No reservation belongs to this transition. A live successor is never
        overwritten, and an ending suppresses a usable source only when its
        generation is covered. Unknown reads retain the source for another
        sweep. An unusable orphan source is restored so quarantine can finish
        publishing its observable ending through the ordinary path.
        """
        recovered: list[str] = []
        for captured in sorted(_scan(self.root / "ready-transitions")):
            parts = captured.name.split(".")
            if (len(parts) != 5 or parts[-1] != "json"
                    or parts[3] not in {"orphan", "withdraw-ready"}
                    or len(parts[0]) != 64
                    or any(c not in "0123456789abcdef" for c in parts[0])):
                continue
            key = parts[0]
            try:
                age = _now() - int(parts[1]) / 1_000_000
            except ValueError:
                continue
            if age <= grace_s:
                continue
            with self._transition_locked(key, blocking=False) as acquired:
                if not acquired:
                    continue
                try:
                    record = _read_json(captured)
                    # A prior helper or sweep already concluded this capture.
                    if record is None and not captured.exists():
                        continue
                    # List first: a negatively cached missing name must not
                    # hide a successor or a terminal publication on NFS.
                    present = {
                        state: f"{key}.json" in os.listdir(self.dir(state))
                        for state in (READY, CLAIMED, DONE, FAILED)
                    }
                    superseded = present[READY] or present[CLAIMED]
                    if not superseded:
                        # A listed but unreadable/empty ending is unknown,
                        # including for a stub without generation metadata.
                        for state in (DONE, FAILED):
                            if present[state] and _read_json(self.item_path(state, key)) is None:
                                raise PoolContractError(f"unreadable terminal record: {state}/{key}")
                        terminal = frozenset({key}) if present[DONE] or present[FAILED] else frozenset()
                        superseded = (
                            self.terminal_outcome_covers(record, action_key=key,
                                                        terminal=terminal) is not None
                            or self.withdrawal_covers(record, action_key=key) is not None
                            or (record is not None
                                and not self._ready_record_usable(record, key)
                                and bool(terminal))
                        )
                    if superseded:
                        self._finish_ready_transition(captured)
                    elif not self._restore_ready_transition(captured, key):
                        continue
                except (OSError, PoolContractError, pb.CASUnavailableError):
                    continue
                recovered.append(key)
        return recovered

    def _take_orphan_stub(self, path: Path) -> tuple[dict[str, object], Path] | None:
        """Own and re-read a stub; caller acknowledges its durable ending."""
        captured = self._capture_ready_transition(path, kind="orphan")
        if captured is None:
            return None
        try:
            record = _read_json(captured)
            if record is not None and not self._ready_record_usable(record, path.stem):
                return record, captured
        except (OSError, PoolContractError):
            pass
        self._restore_ready_transition(captured, path.stem)
        return None

    def quarantine_orphans(self) -> list[str]:
        """File ready records that no consumer can address.

        ``claim()`` addresses an item by ``action_key`` and skips a record that
        has none, so such a record never runs, never fails, and never leaves
        ``ready``: the queue reports work it will not do, and the work it
        stands for is lost in silence.  The reaper race above is one way to
        make one, and a worker still running the pre-fix code is another, so
        the sweep stays whether or not that race can still fire.  Filing them
        is the point -- a countable ``orphaned_stub`` in ``failed`` is a
        defect someone can see; a permanent resident of ``ready`` is not.

        A record whose bytes will not parse is the same defect one step
        earlier, so it takes the same route.  It used to take the whole fleet
        instead: ``_read_json`` refuses a malformed record, and that refusal
        reached ``claim``, ``ready_items``, ``reap_stale`` and this sweep, so
        one foreign writer's truncated file stopped every consumer on every
        box until somebody deleted it by hand.
        """

        filed: list[str] = []
        ready = self.dir(READY)
        if not ready.is_dir():
            return filed
        for path in sorted(ready.glob("*.json")):
            with self._transition_locked(path.stem, blocking=False) as acquired:
                if not acquired:
                    continue
                try:
                    record = _read_json(path)
                except PoolContractError as exc:
                    key = self._file_unreadable(path, reason=str(exc))
                    if key is not None:
                        filed.append(key)
                    continue
                except OSError as exc:
                    if exc.errno != errno.ESTALE:
                        raise
                    # The same race the branch below calls ordinary, arriving
                    # through a directory handle the client had cached (#208).
                    # It is caught here rather than through ``_read_json``'s
                    # ``tolerate_stale`` because this sweep *discriminates*
                    # ``None``, and the flag would throw away the errno that tells
                    # the two cases apart: a record still on disk answers
                    # ``path.exists()`` with ``True`` and would be filed as a torn
                    # write and unlinked -- a live queue item destroyed on the
                    # evidence of a read that never reached it.  (``Path.exists``
                    # would not even survive the attempt: ``pathlib._ignore_error``
                    # covers ``ENOENT/ENOTDIR/EBADF/ELOOP``, so ``ESTALE`` comes
                    # straight back out of it.)  Filing is this sweep's only
                    # verb, and it may not be exercised on bytes it has not read.
                    continue
                if record is None:
                    # ``None`` covers two different things.  The file vanishing
                    # under the glob is an ordinary race with a concurrent claim
                    # and is not this sweep's business.  A file that is still
                    # there and holds zero bytes is a torn write no consumer will
                    # ever address, which is exactly what this sweep is for.
                    if path.exists():
                        key = self._file_unreadable(path, reason="queue record is empty")
                        if key is not None:
                            filed.append(key)
                    continue
                # Two ways to be unaddressable, and both belong here.  A record
                # with the wrong (or no) ``action_key`` is skipped by ``claim()``
                # and never runs.  A record that *has* the key but lacks the
                # fields a worker executes with -- ``worker_script``, ``cas_root``,
                # checkout addressing -- is worse: it is claimed, it kills the
                # worker process before execution, and retries before it is filed.
                if self._ready_record_usable(record, path.stem):
                    continue
                taken = self._take_orphan_stub(path)
                if taken is None:
                    continue
                record, captured = taken
                detail: dict[str, object] = {
                    "reason": "ready record is not executable: it lacks a "
                    "matching action_key, the worker_script/cas_root/"
                    "checkout addressing a worker runs from, or a place in "
                    "the queue's own order; see the reap_stale and finish() "
                    "requeue races",
                }
                unorderable = self._unorderable_queue_field(record)
                if unorderable is not None:
                    # Named, not merely counted: whoever has to find the
                    # writer needs the field and the value it stated, and the
                    # original bytes are already beside this in superseded/.
                    field, value = unorderable
                    detail["unorderable_field"] = field
                    detail["unorderable_value"] = repr(value)
                record.update(
                    {
                        "schema": POOL_OUTCOME_SCHEMA_V1,
                        "action_key": path.stem,
                        "status": "orphaned_stub",
                        "finished_unix": _now(),
                        "finished_host": socket.gethostname(),
                        "detail": detail,
                    }
                )
                # The moved bytes remain evidence. Neither a terminal ending nor
                # a new live record under the same key belongs to this stub.
                if not any(self.item_path(state, path.stem).exists()
                           for state in (READY, CLAIMED, DONE, FAILED)):
                    pb._atomic_publish(self.item_path(FAILED, path.stem),
                                       pb._canonical_bytes(record))
                # A ready stub supplies no authority over committed reservations.
                self._finish_ready_transition(captured)
                filed.append(path.stem)
        return filed

    # -- attempt evidence and terminal states ----------------------------

    def archive_attempt(
        self,
        record: Mapping[str, object],
        *,
        attempt: int,
        status: str,
        disposition: str,
        detail: Mapping[str, object] | None,
    ) -> list[dict[str, object]]:
        """Publish one immutable outcome plus stdout/stderr, then link it.

        The mutable queue item is the state machine's current pointer.  It is
        necessarily rewritten on retry and therefore cannot also be the audit
        history.  Each attempt is published first-writer-wins under the action
        generation and 1-based attempt number; the ready/terminal record then
        carries the ordered relative links returned here.
        """

        path = self.attempt_path(record, attempt)
        link: dict[str, object] = {
            "attempt": attempt,
            "outcome": str(path.relative_to(self.root)),
        }
        raw_history = (
            record["attempt_history"] if "attempt_history" in record else []
        )
        if not isinstance(raw_history, list) or any(
            not isinstance(entry, Mapping) for entry in raw_history
        ):
            raise PoolContractError("attempt_history must be a list of links")
        history = [dict(entry) for entry in raw_history]
        missing = record.get("attempt_history_missing_before", 0)
        if type(missing) is not int or missing < 0:
            raise PoolContractError(
                "attempt_history_missing_before must be a non-negative integer"
            )
        if history:
            # Refuse a corrupt prefix before extending it.  This also verifies
            # every immutable log rather than trusting links copied through a
            # mutable ready record.  ``finish`` has already advanced the mutable
            # attempt count, so validate the prefix at its own exact length.
            self.attempt_outcomes(
                {
                    **dict(record),
                    "attempts": missing + len(history),
                }
            )
        expected_attempt = missing + len(history) + 1
        if attempt < expected_attempt:
            index = attempt - missing - 1
            if index < 0 or history[index] != link:
                raise PoolContractError(
                    f"attempt {attempt} has conflicting history links")
            return history
        if attempt != expected_attempt:
            raise PoolContractError(
                f"attempt {attempt} does not follow {missing} unrecorded and "
                f"{len(history)} archived attempts"
            )

        if not isinstance(status, str) or not status:
            raise PoolContractError("pool attempt status must be nonempty text")
        if not isinstance(disposition, str) or not disposition:
            raise PoolContractError(
                "pool attempt disposition must be nonempty text"
            )
        retry_safe = record.get("retry_safe")
        if retry_safe is not None and type(retry_safe) is not bool:
            raise PoolContractError("retry_safe must be boolean or null")
        max_attempts = record.get("max_attempts", DEFAULT_MAX_ATTEMPTS)
        if type(max_attempts) is not int or max_attempts < 1:
            raise PoolContractError("max_attempts must be a positive integer")

        details = dict(detail or {})
        logs: dict[str, dict[str, object]] = {}
        for stream in ("stdout", "stderr"):
            value = details.pop(stream, "")
            if value is None:
                value = ""
            if not isinstance(value, str):
                raise PoolContractError(
                    f"pool attempt {stream} must be text or null")
            raw = value.encode("utf-8")
            digest = hashlib.sha256(raw).hexdigest()
            log_path = self.attempt_log_path(
                record, attempt, stream, digest)
            _publish_immutable(
                log_path, raw, where=f"pool attempt {stream}")
            logs[stream] = {
                "path": str(log_path.relative_to(self.root)),
                "bytes": len(raw),
                "sha256": digest,
            }

        outcome = {
            "schema": POOL_ATTEMPT_SCHEMA_V1,
            "action_key": str(record.get("action_key") or ""),
            "published_unix": record.get("published_unix"),
            "published_by": record.get("published_by"),
            "attempt": attempt,
            "max_attempts": max_attempts,
            # ``None`` is honest legacy evidence: lower-level pool producers
            # predate the explicit pbrun retry contract.  Never infer safety
            # merely from a bound greater than one.
            "retry_safe": retry_safe,
            "status": str(status),
            "disposition": str(disposition),
            "claimed_by": record.get("claimed_by"),
            "claimed_unix": record.get("claimed_unix"),
            "claimed_host": record.get("claimed_host"),
            "finished_unix": record.get("finished_unix"),
            "finished_host": record.get("finished_host"),
            "detail": details,
            "logs": logs,
        }
        if (record.get("preempted_by") is not None
                and type(record.get("attempt_history_missing_before")) is int
                and record["attempt_history_missing_before"] > 0):
            # Preserve the generation handoff in the existing immutable attempt
            # evidence. Mutable done/failed rows are one slot per action key
            # and can later be replaced by an unrelated generation.
            outcome["preemption_context"] = {
                field: record.get(field) for field in (
                    "preempted_by", "supersedes_withdrawal",
                    "attempt_history_missing_before")
            }
        # Outcome publication is first-writer-wins.  A finisher and a stale
        # reaper can legitimately race on the same numbered attempt; their
        # logs have content-addressed names, and whichever complete outcome
        # links first is the causal record the mutable queue must adopt.  This
        # also repairs a crash after immutable publication but before the
        # ready/terminal summary kept its link.
        pb._atomic_publish(path, pb._canonical_bytes(outcome))
        history.append(link)
        self.attempt_outcomes(
            {
                **dict(record),
                "attempts": attempt,
                "attempt_history": history,
            }
        )
        return history

    def attempt_outcomes(
        self, record: Mapping[str, object]
    ) -> list[dict[str, object]]:
        """Read and verify the immutable attempts linked by a queue outcome."""

        raw_history = (
            record["attempt_history"] if "attempt_history" in record else []
        )
        if not isinstance(raw_history, list):
            raise PoolContractError("attempt_history must be a list")
        outcomes: list[dict[str, object]] = []
        missing = record.get("attempt_history_missing_before", 0)
        if type(missing) is not int or missing < 0:
            raise PoolContractError(
                "attempt_history_missing_before must be a non-negative integer"
            )
        recorded_attempts = record.get("attempts")
        if type(recorded_attempts) is not int or recorded_attempts < 0:
            raise PoolContractError(
                "pool attempt count must be a non-negative integer"
            )
        if recorded_attempts != missing + len(raw_history):
            raise PoolContractError(
                "pool attempt count does not match its missing prefix and "
                "history links"
            )
        for expected_attempt, raw_link in enumerate(
            raw_history, start=missing + 1
        ):
            if not isinstance(raw_link, Mapping):
                raise PoolContractError("attempt_history link must be an object")
            attempt = raw_link.get("attempt")
            if type(attempt) is not int or attempt != expected_attempt:
                raise PoolContractError(
                    "attempt_history numbers must be contiguous and ordered"
                )
            expected = self.attempt_path(record, attempt)
            if raw_link.get("outcome") != str(expected.relative_to(self.root)):
                raise PoolContractError(
                    f"attempt {attempt} outcome link is not its canonical path"
                )
            raw = pb._read_regular_file_nofollow(
                expected,
                where="pool attempt outcome",
                require_readonly=True,
            )
            try:
                value = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise PoolContractError(
                    f"pool attempt outcome is not valid JSON: {expected}"
                ) from exc
            if not isinstance(value, dict):
                raise PoolContractError(
                    f"pool attempt outcome is not an object: {expected}"
                )
            if (
                value.get("schema") != POOL_ATTEMPT_SCHEMA_V1
                or value.get("action_key") != record.get("action_key")
                or value.get("published_unix") != record.get("published_unix")
                or value.get("attempt") != attempt
                or value.get("max_attempts")
                != record.get("max_attempts", DEFAULT_MAX_ATTEMPTS)
                or value.get("retry_safe") != record.get("retry_safe")
            ):
                raise PoolContractError(
                    f"pool attempt outcome differs from its history link: {expected}"
                )
            if "preemption_context" in value:
                expected_context = {
                    field: record.get(field) for field in (
                        "preempted_by", "supersedes_withdrawal",
                        "attempt_history_missing_before")
                }
                if value["preemption_context"] != expected_context:
                    raise PoolContractError("immutable attempt preemption context differs")
            raw_logs = value.get("logs")
            if not isinstance(raw_logs, Mapping):
                raise PoolContractError(f"pool attempt logs are missing: {expected}")
            expanded = dict(value)
            for stream in ("stdout", "stderr"):
                metadata = raw_logs.get(stream)
                if not isinstance(metadata, Mapping):
                    raise PoolContractError(
                        f"pool attempt {stream} metadata is missing: {expected}"
                    )
                digest = metadata.get("sha256")
                byte_count = metadata.get("bytes")
                if type(byte_count) is not int or byte_count < 0:
                    raise PoolContractError(
                        f"pool attempt {stream} byte count is invalid: {expected}"
                    )
                log_path = self.attempt_log_path(
                    record, attempt, stream, str(digest))
                if metadata.get("path") != str(log_path.relative_to(self.root)):
                    raise PoolContractError(
                        f"pool attempt {stream} link is not its canonical path"
                    )
                log = pb._read_regular_file_nofollow(
                    log_path,
                    where=f"pool attempt {stream}",
                    require_readonly=True,
                )
                if (
                    byte_count != len(log)
                    or metadata.get("sha256") != hashlib.sha256(log).hexdigest()
                ):
                    raise PoolContractError(
                        f"pool attempt {stream} differs from its recorded address"
                    )
                expanded[stream] = log.decode("utf-8")
            outcomes.append(expanded)
        return outcomes

    def archived_preemption_outcomes(
        self, action_key: str, *, generation: float | None = None
    ) -> list[tuple[Path, dict[str, object]]]:
        """Recover ended preemption successors from existing attempt evidence.

        No queue pointer is written. Each returned record is reconstructed from
        its immutable terminal attempt and verified canonical history/logs. The
        path names that actual immutable source, not an overwritten summary.
        Older attempts without handoff context supply no inferred successor.
        """
        identity = {"action_key": action_key,
                    "published_unix": generation if generation is not None else 0.0}
        generation_name = self.attempt_generation(identity)  # validates key and timestamp
        base = self.root / ATTEMPTS / action_key
        if generation is not None:
            base /= generation_name
        pattern = "*.json" if generation is not None else "*/*.json"
        found = []
        for path in _glob(base, pattern):
            value = _read_json(path)
            if value is None or "preemption_context" not in value:
                continue
            if value.get("disposition") not in {DONE, FAILED}:
                continue  # An intermediate failed attempt is not an ending.
            context = value["preemption_context"]
            if not isinstance(context, Mapping):
                raise PoolContractError("immutable attempt preemption context must be an object")
            record = {**value, **context, "schema": POOL_OUTCOME_SCHEMA_V1}
            attempt = value.get("attempt")
            missing = context.get("attempt_history_missing_before")
            limit = value.get("max_attempts")
            if (value.get("action_key") != action_key
                    or type(attempt) is not int or type(limit) is not int
                    or type(missing) is not int or not 0 < missing < attempt <= limit
                    or value.get("retry_safe") is not True
                    or path != self.attempt_path(record, attempt)
                    or not self._preemption_prefix_valid(record, missing, limit)):
                raise PoolContractError("invalid archived preemption outcome identity")
            record["attempts"] = attempt
            record["attempt_history"] = [
                {"attempt": number,
                 "outcome": str(self.attempt_path(record, number).relative_to(self.root))}
                for number in range(missing + 1, attempt + 1)
            ]
            adopted = self.adopted_attempt_summary(record)
            if adopted["disposition"] != value["disposition"]:
                raise PoolContractError("archived preemption outcome has conflicting disposition")
            for field in ("status", "finished_unix", "finished_host", "detail"):
                record[field] = adopted[field]
            found.append((path, record))
        return found

    def adopted_attempt_summary(
        self, record: Mapping[str, object]
    ) -> dict[str, object]:
        """Return the one mutable transition the immutable winner permits.

        A finisher and a stale reaper can both observe the same claim and race
        to publish one attempt number.  ``archive_attempt`` makes that evidence
        first-writer-wins; this method makes its status, disposition, detail,
        and provenance first-writer-wins too.  Both queue writers and readers
        use this rule so a mutable summary cannot route one cause while
        reporting or returning another.
        """

        attempts = self.attempt_outcomes(record)
        if not attempts:
            raise PoolContractError(
                "an attempt-backed queue record has no immutable outcome"
            )
        adopted = attempts[-1]
        status = adopted.get("status")
        disposition = adopted.get("disposition")
        if not isinstance(status, str) or not status:
            raise PoolContractError("pool attempt status must be nonempty text")
        if not isinstance(disposition, str) or not disposition:
            raise PoolContractError(
                "pool attempt disposition must be nonempty text"
            )
        attempt = adopted.get("attempt")
        max_attempts = adopted.get("max_attempts")
        if type(attempt) is not int or type(max_attempts) is not int:
            raise PoolContractError("pool attempt transition has invalid bounds")
        succeeded = status in {"executed", "cache_hit"}
        expected = (
            DONE if succeeded else FAILED if attempt >= max_attempts else "requeued"
        )
        if disposition != expected:
            raise PoolContractError(
                f"pool attempt {attempt} status {status!r} requires "
                f"disposition {expected!r}, not {disposition!r}"
            )
        raw_detail = adopted.get("detail")
        if not isinstance(raw_detail, Mapping):
            raise PoolContractError("pool attempt detail must be an object")
        finished_unix = adopted.get("finished_unix")
        if (
            isinstance(finished_unix, bool)
            or not isinstance(finished_unix, (int, float))
            or not math.isfinite(float(finished_unix))
        ):
            raise PoolContractError("pool attempt finished_unix must be finite")
        finished_host = adopted.get("finished_host")
        if not isinstance(finished_host, str) or not finished_host:
            raise PoolContractError("pool attempt finished_host must be nonempty text")
        detail = dict(raw_detail)
        detail["stdout"] = str(adopted.get("stdout") or "")
        detail["stderr"] = str(adopted.get("stderr") or "")
        return {
            "attempt": attempt,
            "status": status,
            "disposition": disposition,
            "finished_unix": finished_unix,
            "finished_host": finished_host,
            "detail": detail,
        }

    def _late_finish_path(self, record: Mapping[str, object]) -> Path:
        identity = pb.canonical_sha256({field: record.get(field) for field in _CLAIM_IDENTITY})
        return self.dir(CLAIMED) / f"{record['action_key']}.{identity}{LATE_FINISH_SUFFIX}"

    def _finish_late(
        self,
        action_key: str,
        *,
        status: str,
        detail: Mapping[str, object] | None,
        snapshot: Mapping[str, object],
        live: Mapping[str, object],
    ) -> Path:
        if snapshot.get("resource_scope") is None and snapshot.get("resource_scope_intent") is None:
            return self._archive_late_finish(
                action_key, status=status, detail=detail, snapshot=snapshot, live=live)
        record = {**dict(snapshot), "action_key": action_key}
        path = self._late_finish_path(record)
        saved = _read_json(path)
        if saved is None:
            record["finish_pending"] = {"status": status, "detail": dict(detail or {})}
            record["late_finish"] = {field: live.get(field) for field in _CLAIM_IDENTITY}
            # Persist authority and the completed payload's result before any
            # broker operation. A crash can then be retried by the same sweep
            # that already recovers interrupted finish tombstones.
            _write_json_atomic(path, record)
        else:
            record = saved
        return self._retry_late_finish(path, record)

    def _retry_late_finish(self, path: Path, record: dict[str, object]) -> Path:
        pending = record.get("finish_pending")
        live = record.get("late_finish")
        if (path != self._late_finish_path(record)
                or not isinstance(pending, dict) or not isinstance(pending.get("status"), str)
                or not isinstance(pending.get("detail"), dict) or not isinstance(live, dict)
                or (record.get("resource_scope") is None
                    and record.get("resource_scope_intent") is None)):
            raise PoolContractError("invalid late-finish recovery authority or result")
        status, detail = pending["status"], dict(pending["detail"])
        cleanup = self.cleanup_action_containers(
            record, reason=str(detail.get("termination_reason") or status), scope_only=True)
        if not cleanup["complete"]:
            self._note_cleanup_attempt(record, record, cleanup)
            _write_json_atomic(path, record)
            return path
        scope_cleanup = cleanup.get("resource_scope") or {}
        detail["resource_scope_cleanup"] = scope_cleanup
        telemetry = scope_cleanup.get("telemetry") or {}
        resource_failure = self._resource_failure(telemetry)
        if resource_failure:
            status = "failed"
            detail.update(status="failed", returncode=137,
                          termination_reason=resource_failure, resource_telemetry=telemetry,
                          termination_evidence=telemetry.get("termination_evidence"))
        # Keep completed cleanup durable until both the immutable attempt and
        # the recovery evidence are filed. Neither can overwrite a successor.
        _write_json_atomic(path, record)
        result = self._archive_late_finish(
            str(record["action_key"]), status=status, detail=detail, snapshot=record, live=live)
        evidence = dict(record)
        evidence.pop("finish_pending", None)
        evidence.pop("container_cleanup_pending", None)
        self._file_superseded(
            evidence, key=str(record["action_key"]), kind="late-finish",
            status=status, detail=detail, finished_unix=_now(),
            finished_host=socket.gethostname(), outcome=str(result.relative_to(self.root)))
        path.unlink(missing_ok=True)
        return result

    def _archive_late_finish(
        self,
        action_key: str,
        *,
        status: str,
        detail: Mapping[str, object] | None,
        snapshot: Mapping[str, object],
        live: Mapping[str, object],
    ) -> Path:
        """File the result of an attempt that a newer one has already replaced.

        ``finish`` used to read whichever record occupied
        ``claimed/<key>.json`` and prefer it over ``claim_snapshot`` without
        comparing identity.  When a lease expired while its launcher was still
        alive, the reaper requeued the action and a second worker claimed the
        retry, the first worker's ``finish`` then advanced *that* record's
        attempt counter, archived its own result under the second worker's
        identity, filed the generation terminal, released the second worker's
        tokens and removed its claim.  A ``done`` record and an immutable
        attempt both described a result the named attempt never produced, and
        the running retry lost its reservation.

        A worker may conclude only the attempt it executed.  The live claim,
        its lease and its reservation are left exactly as they are, and this
        attempt's result goes where it belongs: its own numbered attempt under
        its own generation, first-writer-wins like every other immutable
        outcome, so a reaper that already filed a lease loss for this attempt
        keeps that record and this one does not overwrite it.

        An unstarted release has not charged that number. If the replacement
        is still at the same number, retain this report as superseded evidence
        instead; the numbered slot belongs to the replacement's execution.

        The caller cleans up any exact broker scope before this archive.
        Action-wide Docker ownership cannot distinguish attempts and is never
        used by this path. A legacy attempt without scope authority can only
        archive its result here.
        """

        attempt = int(snapshot.get("attempts", 0)) + 1
        if (self.attempt_generation({**snapshot, "action_key": action_key})
                == self.attempt_generation({**live, "action_key": action_key})
                and int(live.get("attempts", 0)) < attempt):
            # An unstarted release keeps the publication and attempt count.
            # First-writer-wins is unsafe here: filing the predecessor in that
            # slot makes the successor adopt an outcome it never produced.
            # Keep the report attributable without charging an execution or
            # creating a terminal that a waiter could mistake for completion.
            return self._file_superseded(
                snapshot, key=action_key, kind="uncharged-late-finish",
                status=status, detail=dict(detail or {}),
                finished_unix=_now(), finished_host=socket.gethostname(),
                reason="replaced claim has no charged attempt; its execution "
                       "slot belongs to the same-generation successor",
            )
        archived = dict(snapshot)
        archived["action_key"] = action_key
        archived["finished_unix"] = _now()
        archived["finished_host"] = socket.gethostname()
        succeeded = status in {"executed", "cache_hit"}
        limit = int(snapshot.get("max_attempts", DEFAULT_MAX_ATTEMPTS))
        # The same rule ``adopted_attempt_summary`` applies, so this outcome can
        # never be the one that makes a reader refuse the record.
        disposition = (
            DONE if succeeded else FAILED if attempt >= limit else "requeued"
        )
        self.archive_attempt(
            archived,
            attempt=attempt,
            status=status,
            disposition=disposition,
            detail={
                **dict(detail or {}),
                "late_finisher": {
                    "reason": "this claim was requeued and re-claimed while "
                              "this worker was still running it, so its "
                              "result is filed under its own attempt and the "
                              "live claim, lease and reservation were left "
                              "untouched",
                    "live_claimed_by": live.get("claimed_by"),
                    "live_claimed_unix": live.get("claimed_unix"),
                    "live_attempts": live.get("attempts"),
                    "live_published_unix": live.get("published_unix"),
                },
            },
        )
        return self.attempt_path(archived, attempt)

    @_serialized_key
    def finish(
        self,
        action_key: str,
        *,
        status: str,
        detail: Mapping[str, object] | None = None,
        claim_snapshot: Mapping[str, object] | None = None,
    ) -> Path:
        """File an outcome and return the claim's capacity.

        ``claim_snapshot`` is the record this worker actually executed.  The
        live claimed path can disappear under a finishing worker when a reaper
        wins the terminal-file race; the snapshot keeps the reservation's host
        available even then.  It is not used to reconstruct the queue item --
        the lost-race outcome remains deliberately terminal.
        """

        succeeded = status in {"executed", "cache_hit"}
        src = self.item_path(CLAIMED, action_key)
        record = _read_json(src)
        if record is None and claim_snapshot is not None:
            ready = _read_json(self.item_path(READY, action_key))
            if ready is not None:
                # A queued successor is as live as a claimed successor. This
                # includes charged retries and replacement publications, not
                # only unstarted releases. The late archive distinguishes their
                # numbered history from an uncharged slot; the missing-claim
                # terminal path would end work still waiting in READY (#234).
                return self._finish_late(
                    action_key, status=status, detail=detail,
                    snapshot=claim_snapshot, live=ready,
                )
        if (record is not None and claim_snapshot is not None
                and not _same_claim(record, claim_snapshot)):
            # Whatever is at ``claimed/<key>.json`` now is not the claim this
            # worker executed, so none of the code below may touch it.
            return self._finish_late(
                action_key, status=status, detail=detail,
                snapshot=claim_snapshot, live=record,
            )
        if record is None and claim_snapshot is not None and (
                claim_snapshot.get("resource_scope") is not None
                or claim_snapshot.get("resource_scope_intent") is not None):
            late_path = self._late_finish_path({**claim_snapshot, "action_key": action_key})
            pending_late = _read_json(late_path)
            if pending_late is not None:
                # Its successor may have finished since the first refusal.
                # The saved exact-scope transition still owns this retry.
                return self._retry_late_finish(late_path, pending_late)
        if record is not None:
            # Host-level token ownership cannot distinguish successive attempts
            # on one box. A stale claim read can agree with this caller while
            # the lease names its successor. Refuse before action-wide Docker
            # cleanup or any claim/telemetry write; the later entomb comparison
            # cannot undo payload removal. Missing legacy fields add no proof.
            _check_claim_lease_identity(action_key, record, _read_json(self.lease_path(action_key)))
        read_claim = dict(record) if record is not None else None
        # Cleanup annotates this mapping with the completed scope evidence.
        # Keep the live record itself so the terminal retains that annotation;
        # only a caller-owned fallback snapshot needs a private copy.
        effective_record = record if record is not None else dict(claim_snapshot or {})
        holder = self.resolve_claim_holder(action_key, effective_record)
        if holder is not None:
            effective_record["claimed_host"] = holder
        container_cleanup = self.cleanup_action_containers(
            effective_record, reason=str((detail or {}).get("termination_reason") or status))
        if not container_cleanup["complete"]:
            # A detached container is still the action even after its launcher
            # has returned.  Keep the claim as the durable owner of both the
            # work and its tokens; a local reaper retries cleanup, while a
            # remote one sees the claimed host and leaves it alone.
            pending = dict(effective_record)
            pending["action_key"] = action_key
            self._note_cleanup_attempt(pending, effective_record, container_cleanup)
            pending["finish_pending"] = {
                "status": status, "detail": dict(detail or {}),
            }
            live = _read_json(src)
            if live is not None and _same_claim(live, effective_record):
                _write_json_atomic(src, pending)
            return src
        scope_cleanup = container_cleanup.get("resource_scope") or {}
        telemetry = scope_cleanup.get("telemetry") or {}
        resource_failure = self._resource_failure(telemetry)
        if resource_failure:
            status, succeeded = "failed", False
            detail = {**dict(detail or {}), "status": "failed", "returncode": 137,
                      "termination_reason": resource_failure, "resource_telemetry": telemetry,
                      "termination_evidence": telemetry.get("termination_evidence")}
        if self.withdrawal_covers(record, action_key=action_key) is not None:
            # An operator cancelled this while it was running.  Filing it under
            # ``done`` or ``failed`` would put the pool's opinion of the work on
            # top of a decision about it, and routing it back to ``ready`` --
            # the retry branch below -- would restart exactly what was
            # cancelled.  That restart is the race a hand-edited
            # ``max_attempts`` was trying to lose.  The withdrawal record is
            # already filed; all that is left here is the cleanup ``finish``
            # would otherwise do on its way past.
            #
            # Read AFTER the record, not before it: a withdrawal that lands
            # between the read and the write must still be seen, and this is
            # the last moment at which it can be.
            #
            # Generation-scoped like every other guard: a marker left over from
            # a cancellation the operator has since re-submitted past must not
            # swallow the NEW run's outcome, which would file it nowhere at
            # all.  ``record is None`` is the one case with no generation to
            # compare, and is treated as covered -- the claim was concluded by
            # somebody else, so there is nothing here to file either way.
            if read_claim is None:
                return self.item_path(WITHDRAWN, action_key)
            tombstone, mine = self._entomb_claim(action_key, expect=read_claim)
            if not mine or tombstone is None:
                return self.item_path(WITHDRAWN, action_key)
            self.lease_path(action_key).unlink(missing_ok=True)
            self._release_reservation(action_key, host=holder)
            tombstone.unlink(missing_ok=True)
            return self.item_path(WITHDRAWN, action_key)
        if record is None:
            # A reaper concluded this claim while the work was still running,
            # so the claim file is gone and the item has already been filed
            # somewhere by the winner.  Synthesising ``{"action_key": key}``
            # here and letting the code below requeue it publishes a record
            # that has the key and nothing else -- no ``worker_script``, no
            # ``cas_root``, no ``checkout_root`` -- *over* the full record the
            # reaper just wrote.  The action can then never run again: every
            # subsequent claim dies on ``KeyError('worker_script')``, and the
            # only copy of where the work lived is gone.  Six actions in the
            # live queue are unrecoverable for exactly this reason.
            #
            # This is the twin of the ``reap_stale`` race fixed in 8b32569 --
            # the same missing-read-treated-as-empty-record on the other side
            # of the same window; that fix's own comment names ``finish()``
            # and only the loop was repaired.  File the outcome terminally so
            # it is countable, and never route it back to ``ready``.
            #
            # Ask what this generation has already been filed as before
            # choosing a directory.  The winner's conclusion is the terminal:
            # a reaper that filed ``failed/`` and a launcher that then
            # succeeded are one attempt with one ending, and writing the
            # launcher's opinion into ``done/`` beside it gives one key two
            # terminals.  ``pbrun`` then answers with whichever record scores
            # higher, ``pool_reset`` offers to re-run work whose receipt is in
            # the CAS, and ``reclaim_terminal_reservation`` refuses the key as
            # ambiguous.  The snapshot carries ``published_unix``, which is
            # what makes the question askable here at all.
            snapshot = dict(effective_record)
            self._release_reservation(action_key, host=holder)
            self.lease_path(action_key).unlink(missing_ok=True)
            try:
                covered = self.terminal_outcome_covers(
                    snapshot, action_key=action_key)
            except PoolContractError:
                # A terminal for this key exists and cannot be read.  Raising
                # here ends the whole ``serve_once`` call over one bad file,
                # and PR #52 introduced that on a branch which used to write
                # unconditionally, so ask what is actually left to do.
                #
                # Nothing, is the answer.  This branch is reached only because
                # a reaper already concluded the claim, so the key HAS an
                # ending; the unreadable record is it.  Writing a second one
                # beside it is the two-terminals defect PR #52 removed, and a
                # generation this read cannot supply is no basis for deciding
                # that this is a different run.  So report the terminal that
                # is there and write nothing: the submitter's own reader
                # reports an unreadable record at once (PR #50), which is
                # where a corrupted queue record has to surface, and repairing
                # it from here would be inventing an ending for an attempt
                # this worker did not archive.
                for state in (DONE, FAILED):
                    unreadable = self.item_path(state, action_key)
                    if unreadable.exists():
                        return unreadable
                raise
            if covered is not None:
                return self.item_path(str(covered[0]), action_key)
            lost = self.item_path(
                DONE if succeeded else FAILED, action_key)
            if not lost.exists():
                filed = {
                    "schema": POOL_OUTCOME_SCHEMA_V1,
                    "action_key": action_key,
                    "status": status if succeeded else "finish_lost_race",
                    "finished_unix": _now(),
                    "finished_host": socket.gethostname(),
                    "detail": {
                        "reason": "the claim was concluded by a reaper while "
                                  "this worker was still running it; the "
                                  "item's own record was not available to "
                                  "carry forward",
                        "worker_detail": dict(detail or {}),
                    },
                }
                # Generation-scoped like every other terminal, from the only
                # copy of the item this branch has.  Identity fields only: the
                # snapshot's ``attempts`` predates this attempt, and its
                # ``attempt_history`` links an attempt somebody else archived,
                # which a reader would adopt against the wrong disposition.
                for field in ("published_unix", "published_by", "claimed_by",
                              "claimed_unix", "claimed_host", "max_attempts",
                              "retry_safe"):
                    if field in snapshot:
                        filed[field] = snapshot[field]
                _write_json_atomic(lost, filed)
            return lost
        record.pop("finish_pending", None)
        record.pop("container_cleanup_pending", None)
        prior_attempts = int(record.get("attempts", 0))
        attempts = prior_attempts + 1
        limit = int(record.get("max_attempts", DEFAULT_MAX_ATTEMPTS))
        if (
            prior_attempts
            and "attempt_history" not in record
            and "attempt_history_missing_before" not in record
        ):
            record["attempt_history_missing_before"] = prior_attempts
        # The prewarm receipt was referenced from the claim; resolve it into
        # the terminal record's ``detail`` so the done row answers "was this
        # row's data resident when it ran" without a reader having to join
        # against a sidecar the next campaign may have pruned.
        # ``setdefault``: a worker that measured its own residency outranks
        # the loop's prediction.  A reference that no longer resolves -- the
        # sidecar pruned early, or rewritten for a same-key successor whose
        # digest differs -- resolves to nothing rather than to a dangling
        # pointer: an immutable record must not cite a receipt it cannot
        # produce.  A legacy claim that still carries the full receipt (filed
        # before the reference) is copied as before.
        finished_detail = dict(detail or {})
        warmed = record.get("prewarm")
        if isinstance(warmed, Mapping):
            resolved = self.resolve_prewarm_reference(warmed)
            if resolved is not None:
                finished_detail.setdefault("prewarm", resolved)
        record.update(
            {
                "schema": POOL_OUTCOME_SCHEMA_V1,
                "status": status,
                "attempts": attempts,
                "finished_unix": _now(),
                "finished_host": socket.gethostname(),
                "detail": finished_detail,
            }
        )
        terminal = succeeded or attempts >= limit
        disposition = (
            DONE if succeeded else FAILED if terminal else "requeued"
        )
        # Publish the evidence before the mutable queue pointer moves.  A
        # retry rewrites ``detail`` with its own result, so the history link is
        # the only place the causal attempt can survive that transition.
        record["attempt_history"] = self.archive_attempt(
            record,
            attempt=attempts,
            status=status,
            disposition=disposition,
            # The merged detail, not the caller's: the attempt archive is the
            # immutable evidence and ``adopted_attempt_summary`` reads the
            # terminal's ``detail`` back out of it, so a receipt added only to
            # the record is overwritten two statements below.
            detail=finished_detail,
        )
        adopted = self.adopted_attempt_summary(record)
        record.update(
            {
                "status": adopted["status"],
                "finished_unix": adopted["finished_unix"],
                "finished_host": adopted["finished_host"],
                "detail": adopted["detail"],
            }
        )
        disposition = adopted["disposition"]
        if disposition in {DONE, FAILED}:
            dst = self.item_path(str(disposition), action_key)
        else:
            # Reaching this branch is the producer's explicit retry contract,
            # not an inference from deterministic bytes: an argv may mutate
            # external state before failing even when its CAS result would be
            # reproducible.  ``fleet/pbrun`` reaches it only with
            # ``--retry-safe`` and a bound above one.
            dst = self._shape_as_ready_item(record, action_key=action_key)
        # Everything this worker owns goes before the item's next home becomes
        # visible: the claim to a tombstone, then its own lease.  A retry
        # published while either still stood was claimed by the next poll, and
        # the unlinks below then deleted that new claim and its lease.
        # Another cleanup retry can finish and re-claim this key during the
        # archive write. Compare the original identity at the atomic move,
        # not just at entry, before touching the lease or reservation.
        tombstone, mine = self._entomb_claim(action_key, expect=read_claim)
        if not mine or tombstone is None:
            return self.attempt_path(record, attempts)
        self.lease_path(action_key).unlink(missing_ok=True)
        # Capacity is released before the item is filed, so the next worker to
        # look sees the tokens free rather than racing this rename.  A mover
        # that staged what it declared keeps its *tier* tokens: they stand for
        # bytes that are still on the stage, and only an egress that deletes
        # them may give them back.
        self._release_reservation(
            action_key, host=holder,
            keep_tier=self.pin_holds_tier_tokens(record, action_key))
        _write_json_atomic(dst, record)
        if tombstone is None:
            src.unlink(missing_ok=True)
        else:
            tombstone.unlink(missing_ok=True)
        return dst

    @_serialized_key
    def reclaim_terminal_reservation(self, action_key: str, *,
                                     unpin: bool = False) -> dict[str, object]:
        """Return an orphaned reservation only when terminal state proves it.

        This is the bounded repair for a worker that finished on bytes which
        predate ``claim_snapshot``.  It refuses a live or queued action, a
        missing/failed terminal result, a surviving lease, multiple holders,
        a holder that differs from the host which filed the successful
        outcome, and a container that still lives under the terminal record's
        owner -- that last one on the tier-only path too, where no host holds
        the key but a container still ran.  Those are ambiguous generations,
        not cleanup opportunities.
        """

        key = str(action_key)
        if len(key) != 64 or any(ch not in "0123456789abcdef" for ch in key):
            raise PoolContractError("action_key must be a 64-character hex digest")
        for state in (READY, CLAIMED):
            if self.item_path(state, key).exists():
                raise PoolContractError(
                    f"refusing to reclaim {key}: action is still {state}")
        if self.lease_path(key).exists():
            raise PoolContractError(
                f"refusing to reclaim {key}: action still has a lease")

        terminals = [
            (state, record)
            for state in (DONE, FAILED, WITHDRAWN)
            if (record := _read_json(self.item_path(state, key))) is not None
        ]
        if len(terminals) != 1:
            raise PoolContractError(
                f"refusing to reclaim {key}: expected exactly one terminal "
                f"record, found {len(terminals)}")
        state, terminal = terminals[0]
        if state != DONE or terminal.get("status") not in {"executed", "cache_hit"}:
            raise PoolContractError(
                f"refusing to reclaim {key}: terminal status is "
                f"{state}/{terminal.get('status')}")

        if not unpin and self.pin_holds_tier_tokens(terminal, key):
            # A concluded mover whose bytes are still on the stage holds its
            # tier tokens on purpose: they are the stage's occupancy, and
            # returning them here would let the ledger admit a mover onto bytes
            # this one has not released.  Repairing an orphan is the egress
            # node's job, or ``--unpin`` when an operator knows the files are
            # gone.
            raise PoolContractError(
                f"refusing to reclaim {key}: it still holds tier tokens for "
                f"{terminal['residency']['range_end_bytes'] - terminal['residency']['range_start_bytes']}"
                " bytes it staged; run its egress, or pass unpin=True if the "
                "files are known to be gone")

        hosts = self.claim_reservation_hosts(key)
        if hosts:
            if len(hosts) != 1:
                raise PoolContractError(
                    f"refusing to reclaim {key}: reservation is held on {hosts}")
            finished_host = terminal.get("finished_host")
            if finished_host != hosts[0]:
                raise PoolContractError(
                    f"refusing to reclaim {key}: successful outcome was filed on "
                    f"{finished_host!r}, reservation is held on {hosts[0]!r}")
        # The container lifecycle is verified on both paths, not only when a
        # host holds the key.  A tier-only item -- no host demand, so
        # ``claimed["reserved_on"]`` is ``None`` and no ledger names a box --
        # still ran in a container, and returning its tier tokens while that
        # container lives is the same unverified release the check below
        # refuses.  The holder check above is vacuous with no holder, which
        # is why it stays scoped to one; this one is not.
        terminal_owner = terminal.get("container_owner")
        if terminal_owner and self.container_marker(str(terminal_owner)).exists():
            raise PoolContractError(
                f"refusing to reclaim {key}: container lifecycle verification "
                "is required")
        if not hosts:
            # No box holds it; a tier still may (a mover's stage tokens), and
            # an operator asking for a terminal key's reservation back means
            # those too.
            return {"action_key": key, "released": self.release_tier_reservations(key),
                    "hosts": []}

        released = self._release_reservation(key, host=hosts[0])
        return {"action_key": key, "released": released, "hosts": hosts}

    # -- operator decisions ---------------------------------------------

    def terminal_keys(self) -> frozenset[str]:
        """Every action key with a worker-filed outcome.

        List rather than ``stat`` for the same NFS reason as
        :meth:`withdrawn_keys`: a negatively cached absence must not make a
        worker execute a ready copy after another box has filed its outcome.
        The record's generation is checked separately, so an old outcome does
        not blacklist this content-addressed name.

        **Loud on anything but absence.**  This set is the evidence
        ``terminal_outcome_covers`` decides on, and every ``OSError`` used to
        answer it the same way an empty directory does.  ``ESTALE`` on a
        cached directory handle is the ordinary way a listing fails on this
        mount -- ``_read_json`` treats it as a first-class event (#208) and
        ``quarantine_orphans`` re-raises every errno that is not ``ESTALE``
        rather than swallowing the class -- so one stale handle reported "no
        outcomes have been filed" for a queue full of them.  ``reap_stale``
        then found no filed outcome for a generation that had one and put it
        back in ``ready``, which is the one thing this method's own caller
        says a CAS hit does not license.  ``_read_json`` states the rule:
        answering "absent" without the evidence that the directory is live
        "would turn a broken mount into a confident wrong verdict".
        """

        keys: set[str] = set()
        for state in (DONE, FAILED):
            try:
                names = os.listdir(self.dir(state))
            except FileNotFoundError:
                # Absence only.  A queue whose layout has not been created yet
                # legitimately has no ``done`` and no ``failed``, and that is
                # the one reading of "no names" this method may make.
                continue
            keys.update(
                name[: -len(".json")] for name in names if name.endswith(".json")
            )
        return frozenset(keys)

    def terminal_outcome_covers(
        self,
        record: Mapping[str, object] | None,
        *,
        action_key: str | None = None,
        terminal: frozenset[str] | None = None,
    ) -> tuple[str, dict[str, object]] | None:
        """The filed outcome for this record's generation, or ``None``.

        An action key identifies work, not one request to perform it.  The
        equality of ``published_unix`` is already the queue's generation rule
        for withdrawal and is deliberately independent of clock ordering.
        Missing generation evidence cannot suppress a later submission.
        """

        key = str(action_key or (record or {}).get("action_key") or "")
        if not key or record is None:
            return None
        known = self.terminal_keys() if terminal is None else terminal
        if key not in known:
            return None
        mine = record.get("published_unix")
        if not isinstance(mine, (int, float)):
            return None
        for state in (DONE, FAILED):
            outcome = _read_json(self.item_path(state, key))
            if outcome is None:
                continue
            theirs = outcome.get("published_unix")
            if isinstance(theirs, (int, float)) and float(mine) == float(theirs):
                return state, outcome
        return None

    def withdrawn_keys(self) -> frozenset[str]:
        """Every action an operator has withdrawn.

        Listed rather than stat-ed, one call per decision point.  This queue
        lives on NFS, where a stat of a path that did not exist yet is
        negatively cached and keeps answering ``False`` after the file lands --
        the same reason pbrun's wait loop polls by ``readdir``.  A withdrawal
        that a guard could not see is not a withdrawal.

        Which is why an unreadable directory is not an empty one.  Every
        ``OSError`` used to answer ``frozenset()`` here, so an ``ESTALE`` on a
        cached handle said "nothing has been withdrawn" and defeated the
        sentence above: ``_claim`` reads this set as the load-bearing half of
        ``withdraw``, and with it empty the cancelled work is claimed and run
        again -- the race the operator used to have to win by hand.  Loud on
        anything but absence, for the reason :meth:`terminal_keys` records.
        """

        try:
            names = os.listdir(self.dir(WITHDRAWN))
        except FileNotFoundError:
            # Absence only, for the reason ``terminal_keys`` gives: a queue
            # whose layout has not been created yet has no ``withdrawn``.
            return frozenset()
        return frozenset(
            name[: -len(".json")] for name in names if name.endswith(".json")
        )

    def live_withdrawal(self, action_key: str) -> dict[str, object] | None:
        """The visible cancellation marker filed for this key, if there is one.

        Listed then read, for ``withdrawn_keys``' own NFS reason.  A marker
        that is present but unreadable still answers with a record (carrying
        no reason): an automatic publication refuses on it rather than
        guessing that a damaged cancellation was never filed.
        """

        key = str(action_key)
        if key not in self.withdrawn_keys():
            return None
        try:
            marker = _read_json(self.item_path(WITHDRAWN, key))
        except (OSError, PoolContractError):
            marker = None
        if isinstance(marker, dict):
            return marker
        return {"action_key": key, "reason": "",
                "unreadable": True}

    def superseded_dir(self) -> Path:
        """Where records go once a generation decision makes them non-live.

        A subdirectory rather than a timestamped sibling, because every reader
        of ``withdrawn/`` addresses it by ``<key>.json``: ``withdrawn_keys``
        lists it, ``find_key`` globs it, ``item_path`` builds the name,
        ``pbrun``'s wait loop lists it and ``tessera_status`` counts ``*.json``
        in it.  A sibling named ``<key>.<unix>.json`` would look to all five
        like an action whose key is nonsense; a subdirectory is invisible to
        every one of them.  It keeps both retired withdrawals and ready/claimed
        copies dropped because a terminal record already owns their generation,
        so no queue record disappears without evidence.
        """

        return self.dir(WITHDRAWN) / "superseded"

    def _file_superseded(
        self,
        record: Mapping[str, object] | None,
        *,
        key: str,
        kind: str,
        **stamps: object,
    ) -> Path:
        """Keep a record that is no longer live, under a name of its own."""

        when = _now()
        payload = dict(record or {})
        payload["action_key"] = key
        payload.update(stamps)
        path = self.superseded_dir() / f"{key}.{when:.6f}.{kind}.json"
        _write_json_atomic(path, payload)
        return path

    def _supersede_withdrawal(self, action_key: str) -> dict[str, object] | None:
        """Retire the live withdrawal for ``action_key``; return what it said."""

        live = self.item_path(WITHDRAWN, action_key)
        captured = self.superseded_dir() / (
            f"{action_key}.{uuid.uuid4().hex}.withdrawal-source"
        )
        captured.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.rename(live, captured)
        except FileNotFoundError:
            return None
        try:
            record = _read_json(captured)
            if record is None:
                raise PoolContractError(f"empty withdrawal marker: {captured}")
            self._persist_withdrawal_decision(record)
            self._file_superseded(
                record, key=action_key, kind="withdrawal",
                superseded_unix=_now(), superseded_host=socket.gethostname(),
            )
        except BaseException:
            # Failure must not erase the decision, and restoration must not
            # replace another cancellation written while this one was aside.
            try:
                os.link(captured, live)
            except FileExistsError:
                pass
            else:
                captured.unlink()
            raise
        captured.unlink()
        return record

    def withdrawal_decision_path(self, record: Mapping[str, object]) -> Path | None:
        """The durable cancellation address for records with known generation."""
        try:
            generation = self.attempt_generation(record)
        except PoolContractError:
            return None  # legacy marker without a publication identity
        return self.dir(WITHDRAWN) / "decisions" / str(record["action_key"]) / f"{generation}.json"

    def _persist_withdrawal_decision(self, record: Mapping[str, object]) -> dict[str, object]:
        path = self.withdrawal_decision_path(record)
        if path is None:
            return dict(record)
        pb._atomic_publish(path, pb._canonical_bytes(record))
        return self._read_withdrawal_decision(path)

    def _read_withdrawal_decision(self, path: Path) -> dict[str, object]:
        try:
            raw = pb._read_regular_file_nofollow(
                path, where="withdrawal decision", require_readonly=True)
            decision = json.loads(raw)
            if (not isinstance(decision, dict) or decision.get("status") != "withdrawn"
                    or self.withdrawal_decision_path(decision) != path):
                raise ValueError("decision identity disagrees with its address")
        except Exception as exc:
            raise PoolContractError(f"invalid withdrawal decision: {path}: {exc}") from exc
        return decision

    def withdrawal_decisions(
        self, key: str, *, generation: float | None = None,
    ) -> list[tuple[Path, dict[str, object]]]:
        """Immutable cancellations for this key, optionally one generation."""

        directory = self.dir(WITHDRAWN) / "decisions" / key
        if generation is None:
            paths = _glob(directory, "*.json")
        else:
            path = self.withdrawal_decision_path({
                "action_key": key, "published_unix": generation,
            })
            paths = [] if path is None else _glob(directory, path.name)
        return [(path, self._read_withdrawal_decision(path)) for path in paths]

    def _withdraw_ready(self, key: str) -> dict[str, object] | None:
        """Examine captured READY bytes without losing an uncancelled successor."""
        with self._transition_locked(key, blocking=False) as acquired:
            if not acquired:
                return None
            captured = self._capture_ready_transition(self.item_path(READY, key),
                                                      kind="withdraw-ready")
            if captured is None:
                return None
            try:
                moved = _read_json(captured)
                marker = self.withdrawal_covers(moved, action_key=key) if moved is not None else None
                if marker is not None:
                    self._persist_withdrawal_decision(marker)
                    self._finish_ready_transition(captured)
                    return moved
            except (OSError, PoolContractError, pb.CASUnavailableError):
                pass
            self._restore_ready_transition(captured, key)
            return None

    def withdrawal_covers(
        self,
        record: Mapping[str, object] | None,
        *,
        action_key: str | None = None,
        withdrawn: frozenset[str] | None = None,
    ) -> dict[str, object] | None:
        """The withdrawal that cancelled THIS record, or ``None``.

        One predicate for all seven guard sites, because the alternative was
        the rule half-applied: ``claim`` scoping the check while ``finish``
        and ``execute`` still matched on the bare key would discard a
        legitimate later run's outcome and kill the run outright.

        **The generation, not the key.**  An action key is a content hash, so
        a withdrawal has to name the *run* it cancelled, not the name of the
        work for all time.  ``published_unix`` is that name: ``publish``
        stamps a fresh one and every requeue -- ``finish``'s retry branch and
        ``reap_stale``'s -- carries the original forward, so the losing half
        of a withdrawal race and a fresh submission are distinguishable
        without asking either of them to declare which it is.  The test is
        equality, not "newer than": only two writers ever put a record in
        ``ready`` -- ``publish``, which stamps a fresh ``published_unix``, and
        the two requeue branches, which copy the original through unchanged --
        so a record whose stamp DIFFERS from the withdrawal's is a different
        request whichever way the difference runs.  Ordering would have made
        the guard depend on the clock never stepping backwards between two
        submissions, which is a promise nothing here needs to make.

        A record with no generation to compare is treated as covered, which is
        the safe direction: the cancelled work does not run.  Every caller
        that then removes such a record files it first, so "covered" never
        means "vanished".

        Immutable generation decisions survive retirement of the visible
        marker on re-submission. Both paths enumerate before reading so NFS's
        negative cache does not hide a decision just written by another host.
        Legacy markers remain readable when no durable decision exists.
        """

        key = str(action_key or (record or {}).get("action_key") or "")
        if not key:
            return None
        if record is not None:
            decision = self.withdrawal_decision_path({**record, "action_key": key})
            if decision is not None:
                # Enumerate before reading to avoid NFS's negatively cached
                # absence hiding a cancellation written by another host.
                for path in _glob(decision.parent, decision.name):
                    return self._read_withdrawal_decision(path)
        known = self.withdrawn_keys() if withdrawn is None else withdrawn
        if key not in known:
            return None
        marker = _read_json(self.item_path(WITHDRAWN, key))
        if marker is None:
            return None
        if record is None:
            return marker
        mine = record.get("published_unix")
        theirs = marker.get("published_unix")
        if isinstance(mine, (int, float)) and isinstance(theirs, (int, float)):
            if float(mine) != float(theirs):
                return None
        return marker

    def runtime_of(self, host: str | None) -> str | None:
        """Which published bytes the worker on ``host`` is answering with.

        ``None`` when no live offer names the host at all.  The empty string
        when the offer predates ``runtime_commit`` -- both mean "cannot tell",
        and a withdrawal that cannot tell has to say so rather than imply the
        worker will honour it.
        """

        if not host:
            return None
        for offer in self.offers():
            if str(offer.get("host")) == str(host):
                return str(offer.get("runtime_commit") or "")
        return None

    def find_key(self, prefix: str) -> str:
        """Resolve a key prefix to the one action it names.

        Everything an operator has on screen is a prefix: ``pbrun`` prints
        ``queued 8fc86da0e13f`` and the worker loop logs the same twelve
        characters.  Requiring the full digest to cancel would mean going and
        finding it in the queue directory first, at the moment the box is
        already on fire.  Ambiguity is refused rather than guessed at, because
        the wrong guess here kills someone else's work.
        """

        wanted = str(prefix)
        if not wanted:
            raise PoolContractError("an action key prefix must not be empty")
        # An action key is a hex digest, so anything else is a typo -- and the
        # match below is a glob, where a stray ``*`` would silently name every
        # action in the queue and a stray ``[`` would raise from pathlib.
        if any(character not in "0123456789abcdef" for character in wanted.lower()):
            raise PoolContractError(
                f"an action key is a hex digest; {wanted!r} is not a prefix of one")
        seen: set[str] = set()
        for state in (READY, CLAIMED, DONE, FAILED, WITHDRAWN):
            for path in _glob(self.dir(state), f"{wanted}*.json"):
                seen.add(path.stem)
        for directory in _glob(self.dir(WITHDRAWN) / "decisions", f"{wanted}*"):
            if directory.is_dir() and _glob(directory, "*.json"):
                seen.add(directory.name)
        if not seen:
            raise PoolContractError(f"no action in the queue starts with {wanted!r}")
        if len(seen) > 1:
            listed = ", ".join(sorted(key[:16] for key in seen))
            raise PoolContractError(
                f"{wanted!r} names {len(seen)} actions ({listed}); "
                "say more of the key")
        return seen.pop()

    def mark_residency_plan_superseded(self, consumer_action_key: str, *,
                                       reason: str = "") -> bool:
        """Mark the frozen window filed under this action key superseded (#708).

        A withdrawal is the one decision that makes a frozen plan dead: the
        plan that minted the withdrawn action cannot be published again
        without overriding the decision, and it cannot be repriced in place
        because a mover's key hashes the resources and argv it was sealed
        with (#710, #708).  The marker stops publication and lets the
        ordinary planner seal a fresh plan at the current price once the old
        window's work has ended; the body itself stays filed, so the dead
        consumer's queued children are still attributable for withdrawal and
        a running consumer's resident ranges stay named for the sweep.

        Best effort by design: a mount that refuses the marker leaves the
        plan live, and the window's own withdrawal guard still refuses to
        publish a cancelled key.  A failure is said out loud rather than
        swallowed, because what it leaves behind is a window that could be
        republished at the price it was cancelled for.
        """

        try:
            # Local, because ``residency_plan`` imports this module: the queue
            # owns the plan directory, but its naming and body are the plan
            # module's contract.  Without a plan argument the *current* filing
            # is marked, which is what withdrawing an action by its own key
            # means; mark_superseded re-reads it under this key's lock.
            from . import residency_plan
            return residency_plan.mark_superseded(
                self, consumer_action_key,
                reason=reason, by="withdraw") is not None
        except (OSError, ValueError) as exc:
            print(
                f"prismabuild: could not mark the residency plan for "
                f"{str(consumer_action_key)[:12]} superseded: {exc}",
                file=sys.stderr, flush=True,
            )
            return False

    @_serialized_key
    def withdraw(
        self,
        action_key: str,
        *,
        reason: str = "",
        by: str = "",
        preempted_by: str | None = None,
        expected_claim: Mapping[str, object] | None = None,
        membership_handoff: Mapping[str, object] | None = None,
        signal_child: bool = True,
    ) -> dict[str, object]:
        """Cancel one generation; its owner concludes any claimed attempt.

        ``preempted_by`` names the action this cancellation was made for, when
        it was made by admission rather than by an operator (#364).  It is
        stamped on the filed record and on the immutable decision, so the cost
        of a preemption is readable where the ending is, and by a reader that
        does not have to parse ``reason``.

        ``expected_claim`` confines an admission withdrawal to the exact
        attempt it selected. A successor changes nothing and returns
        ``claim_changed``; ordinary operator withdrawals omit this guard.

        ``membership_handoff`` carries the exact claimed snapshot a
        membership resigner built its requeue plan from.  It is proven
        here -- live claim still that attempt, generation uncovered,
        restart permission with remaining budget and existing lineage --
        and only then persisted as the decision's explicit
        ``membership_handoff`` identity, which is what preserves the
        sealed plan and what the tier-loop window classification reads.
        A supervisor-shaped ``by`` with no (or a failing) proof files an
        ordinary cancellation: the owner's shape alone authorizes
        nothing, and stale, replaced, covered, exhausted or foreign rows
        refuse BEFORE anything is stopped or filed.

        The immutable generation decision survives a new publication retiring
        the visible withdrawn record. Claimed records, leases and reservations
        remain with their worker or reaper. A local broker can accelerate the
        stop using the saved exact scope authority, but an operator never
        signals a process identified only by its content-addressed action key.

        Workers must support withdrawal markers. The former compatibility
        write of max_attempts=1 raced successor claims and is retired.
        """

        key = str(action_key)
        self.ensure_layout()
        withdrawn_path = self.item_path(WITHDRAWN, key)
        claimed_path = self.item_path(CLAIMED, key)
        ready_path = self.item_path(READY, key)

        existing = _read_json(withdrawn_path)
        record = _read_json(claimed_path)
        if expected_claim is not None and (
                record is None or not _same_claim(record, expected_claim)):
            # Admission selected one exact attempt before taking this key's
            # transition lock. A replacement must retain its own priority and
            # cancellation authority, even when it has the same action key.
            return {"action_key": key, "status": "claim_changed",
                    "state": CLAIMED if record is not None else None,
                    "released": 0, "signalled": None}
        origin: str | None = CLAIMED if record is not None else None
        handoff_proof: dict[str, object] | None = None
        if membership_handoff is not None:
            # A membership handoff is authorized from the LIVE record,
            # never from the caller's snapshot alone: the caller passes
            # the exact claimed snapshot its requeue plan was built from,
            # and this call re-verifies identity, bindings and permission
            # against the live claim under this method's key lock.  The
            # membership caller kind is one required condition, never
            # sufficient alone.  A snapshot copied from the same claim
            # but with flipped retry permission, inflated budget, another
            # action's key, or another attempt's scope cannot authorize
            # stopping a live job it does not describe: the durable proof
            # below is derived from authoritative live fields only.
            # Stale reads, replaced claims, covered generations,
            # exhausted budgets and foreign rows all refuse BEFORE
            # anything is stopped or filed.
            if not isinstance(membership_handoff, Mapping):
                raise PoolContractError(
                    f"membership handoff for {key[:12]} is not a claimed record")
            if not _membership_withdrawal_owner(by):
                raise PoolContractError(
                    f"membership handoff for {key[:12]} needs the membership "
                    "caller kind")
            snap = dict(membership_handoff)
            if (not isinstance(snap.get("action_key"), str)
                    or snap["action_key"] != key):
                raise PoolContractError(
                    f"membership handoff for {key[:12]} names another action")
            if record is None or not _same_claim(record, snap):
                raise PoolContractError(
                    f"membership handoff claim changed for {key[:12]}: "
                    "the live claim is not the planned attempt")
            if self.withdrawal_covers(record, action_key=key) is not None:
                if (isinstance(existing, dict)
                        and existing.get("published_unix")
                        == record.get("published_unix")
                        and membership_handoff_authorized(existing)):
                    # Covered by a proven handoff for this same generation:
                    # a crashed predecessor already filed this decision, so
                    # adopt it instead of stacking another one.  No new
                    # stamp is filed; the mark gate below reads the
                    # adopted proof, and the live marker keeps working.
                    handoff_proof = dict(
                        existing["membership_handoff"])  # type: ignore[index]
                else:
                    raise PoolContractError(
                        f"membership handoff for {key[:12]} is already "
                        "covered by an ordinary withdrawal decision")
            for binding in ("retry_safe", "max_attempts"):
                if snap.get(binding) != record.get(binding):
                    raise PoolContractError(
                        f"membership handoff for {key[:12]} differs from "
                        f"the live claim on {binding}")
            live_control = record.get("resource_scope")
            snap_control = snap.get("resource_scope")
            if live_control is not None or snap_control is not None:
                # Exact broker scope identity when the attempt holds one:
                # the live block is validated through the existing
                # recovery-identity derivation (no second hand-written
                # unit convention), and the snapshot must name the same
                # scope.  A malformed block on either side, or an
                # absent-on-one-side mismatch, refuses: malformed is
                # never read as "no scope".
                try:
                    self._scope_from_record(record)
                except PoolContractError as exc:
                    raise PoolContractError(
                        f"membership handoff for {key[:12]} has an invalid "
                        f"live scope identity: {exc}") from exc
                if not isinstance(snap_control, Mapping):
                    raise PoolContractError(
                        f"membership handoff for {key[:12]} misses the live "
                        "attempt's scope identity")
                live_pair = (live_control.get("scope_id"),
                             live_control.get("nonce"))
                snap_pair = (snap_control.get("scope_id"),
                             snap_control.get("nonce"))
                if live_pair != snap_pair:
                    raise PoolContractError(
                        f"membership handoff for {key[:12]} mismatches the "
                        "live attempt's scope identity")
            live_intent = record.get("resource_scope_intent")
            snap_intent = snap.get("resource_scope_intent")
            if live_intent is not None or snap_intent is not None:
                # The broker prelaunch identity carrier: intent-only rows
                # (created but not yet scope-bound) match on action and
                # nonce exactly, and an intent beside a control must name
                # its nonce.  Malformed blocks and stale nonces refuse.
                for side in (live_intent, snap_intent):
                    if (not isinstance(side, Mapping)
                            or side.get("action_key") != key
                            or not isinstance(side.get("nonce"), str)
                            or not side.get("nonce")):
                        raise PoolContractError(
                            f"membership handoff for {key[:12]} has an "
                            "invalid scope-intent identity")
                if live_intent.get("nonce") != snap_intent.get("nonce"):
                    raise PoolContractError(
                        f"membership handoff for {key[:12]} carries a stale "
                        "scope-intent nonce")
                if (isinstance(live_control, Mapping)
                        and live_intent.get("nonce")
                        != live_control.get("nonce")):
                    raise PoolContractError(
                        f"membership handoff for {key[:12]} disagrees "
                        "between scope intent and scope")
            live = dict(record)
            if not self._preemption_eligible(live):
                raise PoolContractError(
                    f"membership handoff for {key[:12]} carries no restart "
                    "permission, remaining budget, or lineage on the live "
                    "attempt")
            published = live.get("published_unix")
            if (type(published) not in (int, float)
                    or isinstance(published, bool)
                    or not math.isfinite(float(published))):
                raise PoolContractError(
                    f"membership handoff for {key[:12]} names no generation")
            handoff_proof = {
                "owner": str(by),
                "attempts": live.get("attempts"),
                "max_attempts": live.get("max_attempts"),
                "published_unix": published,
            }
        if (handoff_proof is None and record is not None
                and self.withdrawal_covers(record, action_key=key) is not None):
            # Ordinary operator cancel semantics: a repeated request may
            # target a newer submission queued behind an original attempt
            # that is still stopping.  A proven membership handoff never
            # retargets: the request stays bound to its exact authorized
            # claimed generation through all mutations and signalling, so
            # an old claim's authorization can never cancel the new READY
            # generation waiting behind it.
            waiting = _read_json(ready_path)
            if waiting is not None and self.withdrawal_covers(waiting, action_key=key) is None:
                # A repeated operator request can cancel a later submission
                # queued behind an original attempt that is still stopping.
                record, origin = waiting, READY
        if record is None:
            record = _read_json(ready_path)
            origin = READY if record is not None else None
        if record is None and existing is None:
            for state in (DONE, FAILED):
                finished = _read_json(self.item_path(state, key))
                if finished is not None:
                    # Nothing to stop and nothing to file.  Reporting this
                    # rather than raising matters: an operator who withdraws an
                    # action that finished a second earlier got what they asked
                    # for, and should be told so, not told they mistyped.
                    return {
                        "action_key": key,
                        "status": "already_finished",
                        "state": state,
                        "host": finished.get("finished_host"),
                        "released": 0,
                        "signalled": None,
                        "path": str(self.item_path(state, key)),
                        "reason": "",
                    }
            decisions = self.withdrawal_decisions(key)
            if decisions:
                existing = max(decisions, key=lambda entry: float(entry[1]["published_unix"]))[1]
            else:
                raise PoolContractError(f"no such action in the queue: {key}")

        lease = _read_json(self.lease_path(key)) or {}
        host: str | None = None
        if origin == CLAIMED and isinstance(record, Mapping):
            # Resolve ambiguity before retiring or publishing any decision.
            # This is also the holder named in the deferred-stop result.
            evidence = dict(record)
            if isinstance(lease.get("host"), str):
                evidence["host"] = lease["host"]
            host = self.resolve_claim_holder(key, evidence)
            if host is not None and isinstance(record, dict):
                # Named on the withdrawn record too, for the reason #227 gives:
                # a claim must not be able to be lost more anonymously than it
                # was taken.
                record["claimed_host"] = host
        else:
            if isinstance(record, Mapping):
                claimed_host = record.get("claimed_host")
                host = claimed_host if isinstance(claimed_host, str) else None
            if host is None and isinstance(lease.get("host"), str):
                host = str(lease["host"])

        if (existing is not None and record is not None
                and self.withdrawal_covers(record, action_key=key) is None):
            self._supersede_withdrawal(key)
            existing = None

        if existing is None:
            filed = dict(record or {})
            # A withdrawal is an operator's verb, not an attempt, and the
            # copied record can carry links a requeue wrote.  Every reader of a
            # terminal record adopts the immutable attempt whenever
            # ``attempt_history`` is present, and the attempt it adopts says
            # ``requeued`` while the directory says ``withdrawn``, so
            # ``outcome_summary`` refused the record and the operator's
            # decision reached nobody.  Keep the evidence under a name of its
            # own: the links still resolve, and no reader mistakes them for
            # this record's own ending.
            #
            # ``detail`` is the same fact one field over.  A record a requeue
            # has touched carries the returncode, stdout and stderr of the
            # attempt that failed, and under ``status: withdrawn`` that
            # describes an ending this record does not have: ``pbrun`` wrote
            # the failed attempt's stderr to the operator's terminal and only
            # then said who withdrew the action, and ``pbstatus`` showed its
            # returncode on the withdrawn row.  A cancellation has no detail of
            # its own -- ``withdrawn_by`` and ``reason`` are what it has to say
            # -- so the field is kept as evidence rather than left where every
            # reader takes it for this record's ending.
            for field, kept in (
                ("attempt_history", "attempt_history_before_withdrawal"),
                ("attempt_history_missing_before",
                 "attempt_history_missing_before_withdrawal"),
                ("detail", "detail_before_withdrawal"),
            ):
                if field in filed:
                    filed[kept] = filed.pop(field)
            filed.update(
                {
                    "schema": POOL_OUTCOME_SCHEMA_V1,
                    "action_key": key,
                    "status": "withdrawn",
                    "withdrawn_from": origin or "unknown",
                    "withdrawn_unix": _now(),
                    "withdrawn_host": socket.gethostname(),
                    "withdrawn_by": str(by),
                    "reason": str(reason),
                }
            )
            if preempted_by is not None:
                filed["preempted_by"] = str(preempted_by)
            if handoff_proof is not None:
                # Explicit durable handoff identity: persisted only after
                # the proof above, read back by the tier-loop window
                # classification.  An ordinary cancellation -- however
                # shaped its `by` string -- never carries this.
                filed["membership_handoff"] = handoff_proof
            filed = self._persist_withdrawal_decision(filed)
            _write_json_atomic(withdrawn_path, filed)
        else:
            filed = self._persist_withdrawal_decision(existing)

        # A ready record is ours only after its atomic move and generation
        # check. Publication can replace it between the initial read and now.
        self._withdraw_ready(key)

        signalled = None
        stop_pending = None
        container_cleanup = {"complete": True, "used": False, "deferred": False}
        if origin == CLAIMED:
            # The operator has a snapshot, never cleanup ownership. Its marker
            # may already have been observed by the first worker, which can
            # finish and admit a successor before this call resumes.
            container_cleanup = {"complete": None, "used": None, "deferred": True}
            stop_pending = {
                "holder_host": host,
                "checked_unix": _now(),
                "checked_host": socket.gethostname(),
                "reason": "the claiming worker or reaper retains cleanup ownership",
            }
            if signal_child and host == socket.gethostname() and record.get("resource_scope") is not None:
                try:
                    scope = self._scope_from_record(record)
                    stopped = scope.terminate_owned("withdrawn")
                    signalled = {"scope_id": scope.unit, "nonce": scope.nonce,
                                 "signals": ["broker exact-attempt stop"],
                                 "broker": stopped}
                except Exception as exc:  # the durable decision remains effective
                    stop_pending["stop_error"] = f"{type(exc).__name__}: {exc}"

        # The plan that minted a withdrawn *consumer* is marked superseded as
        # it goes -- unless the withdrawal carries a proven membership
        # handoff.  The proof (live claim match, uncovered generation,
        # restart permission with remaining budget and lineage, all
        # re-verified above) means the same work continues under a new
        # generation that revives this exact decision: retiring the filing
        # here would strand the successor's later phases.  An operator's
        # decision has no successor and retires the window it was made
        # against -- including a supervisor-shaped `by` with no (or a
        # failed) handoff proof, which files an ordinary cancellation.  A
        # mover's key names no plan -- the plan lives under the consumer's
        # key -- so this is a no-op for one, and a mover's plan is marked
        # by the window that would otherwise republish it (#708).
        # Admission's own preemption is excluded: it requeues its holder in
        # the same breath, and marking the plan would pause the window it just
        # put back.
        plan_superseded = False
        if preempted_by is None and handoff_proof is None:
            plan_superseded = self.mark_residency_plan_superseded(
                key, reason=reason or "withdrawn")

        return {
            "action_key": key,
            "status": "already_withdrawn" if existing is not None else "withdrawn",
            "state": origin,
            "host": host,
            "residency_plan_superseded": plan_superseded,
            # What the holder's worker is running, so the caller can be told
            # whether this withdrawal is one it can see.  ``None`` means no
            # live offer names the host; ``""`` means the offer predates the
            # field.  Both are "cannot tell", and the CLI says so.
            "holder_runtime": self.runtime_of(host),
            "released": 0,
            "signalled": signalled,
            "container_cleanup": container_cleanup,
            # ``None`` when the action is known to have stopped.  Otherwise
            # why the reservation is still held, and on which box.
            "stop_pending": stop_pending,
            "path": str(withdrawn_path),
            "reason": str(filed.get("reason") or ""),
        }

    # -- execution ------------------------------------------------------

    def execute(
        self,
        item: Mapping[str, object],
        *,
        python: str | Path = sys.executable,
        timeout_s: float | None = None,
        heartbeat_s: float = HEARTBEAT_S,
        timeout_grace_s: float = TIMEOUT_GRACE_S,
        containment: bool = False,
    ) -> dict[str, object]:
        """Materialize a sealed checkout and optionally contain the worker."""
        if not isinstance(item, dict):
            item = dict(item)
        # Priced here rather than only inside, so every ending carries it --
        # a timeout, a clean exit and a withdrawal all leave a receipt that
        # says which deadline was in force and whether the box's ceiling, not
        # the submitter, chose it (#293).  ``_execute_in_checkout`` re-derives
        # the same number from the same sealed request, which is idempotent
        # under the clamp; passing the effective value keeps the two in step
        # without giving either one a second source of truth.
        # An action admitted under the progress contract is not bounded in
        # total duration by this box's ceiling -- that ceiling is what killed
        # two demonstrably advancing GLM rows (#480), and a limit nobody
        # submitted and no receipt explained is exactly what the contract
        # replaces.  The ceiling is not waived, it is *re-aimed*: it clamps
        # every declared phase's quiet instead, so a stuck action still ends on
        # this box's terms.  A deadline the submitter asked for explicitly
        # still governs, progress or no progress.
        policy = progress_policy(item, timeout_s)
        budget = execution_budget(item, None if policy is not None else timeout_s)
        with _execution_checkout(item) as checkout_root:
            outcome = self._execute_in_checkout(
                item, checkout_root=checkout_root, python=python,
                timeout_s=budget.effective, heartbeat_s=heartbeat_s,
                timeout_grace_s=timeout_grace_s, containment=containment,
                progress=policy,
            )
        outcome.update(budget.as_record())
        # ``execution_timeout_ceiling_s`` is what bounded the *deadline*, and
        # under the progress contract nothing did.  This says what the box's
        # ceiling actually is regardless of what it governs, so a reader is
        # never left inferring "unbounded" from a null (#293's lesson, one
        # field over): with a policy in force it is the stall ceiling, and
        # ``progress_no_progress_bound_s`` is the total quiet it permits.
        outcome["worker_timeout_ceiling_s"] = timeout_s
        outcome["execution_governed_by"] = "progress" if policy is not None else "deadline"
        if policy is not None:
            outcome.update(policy.as_record())
        if item.get("resource_scope") is not None:
            telemetry = self._sample_resource_scope(self._scope_from_record(item))
            outcome["resource_telemetry"] = telemetry
            resource_failure = self._resource_failure(telemetry)
            if resource_failure:
                outcome.update(status="failed", returncode=137,
                               termination_reason=resource_failure,
                               termination_evidence=telemetry.get("termination_evidence"))
                outcome["stderr"] = str(outcome.get("stderr") or "") + (
                    f"\nPrismaBuild: action resource containment stopped this attempt: {resource_failure}.\n")
        # Last, because it folds what every step above measured -- and inside a
        # guard, because an action that ran must not be filed as a failure by
        # the instrumentation that was only describing it.
        try:
            outcome["resource_profile"] = self._resource_profile(item, outcome)
        except Exception as exc:                                 # noqa: BLE001
            outcome["resource_profile"] = {
                "schema": RESOURCE_PROFILE_SCHEMA_V1,
                "error": f"{type(exc).__name__}: {exc}",
            }
        return outcome

    def _resource_profile(self, item: Mapping[str, object],
                          outcome: dict[str, object]) -> dict[str, object]:
        """What this run cost, and what the box was doing while it ran.

        Three sources, each named where it lands, because they do not measure
        the same thing and a reader has to be able to tell which one a number
        came from.  ``reaped_children`` is this parent's kernel accounting for
        what it launched; ``scope`` and ``process_io`` are the exact attempt's
        cgroup and the processes inside it, which is where a contained run's
        payload actually is; ``box_window`` is the machine around it.

        A group whose source said nothing is absent, never zero: "not measured"
        and "measured as idle" are the two answers this record exists to keep
        apart.  None of it is sealed into the action -- it is metadata about
        one run of an action, and two runs of the same action stay the same
        action.
        """

        finished_unix = _now()
        profile: dict[str, object] = {
            "schema": RESOURCE_PROFILE_SCHEMA_V1,
            "host": socket.gethostname(),
            "finished_unix": finished_unix,
        }
        claimed = item.get("claimed_unix")
        if (type(claimed) in (int, float) and math.isfinite(float(claimed))
                and 0 < float(claimed) <= finished_unix):
            start, start_source = float(claimed), "claimed_unix"
        else:
            # A claim with no usable stamp still ran for a measured time, and
            # the window has to start somewhere a reader can name.
            elapsed = outcome.get("elapsed_s")
            span = float(elapsed) if type(elapsed) in (int, float) else 0.0
            start, start_source = finished_unix - span, "elapsed_s"
        profile["start_unix"] = start
        profile["start_source"] = start_source
        profile["wall_seconds"] = finished_unix - start

        reaped = outcome.pop("child_rusage", None)
        if isinstance(reaped, dict):
            profile["reaped_children"] = reaped

        telemetry = outcome.get("resource_telemetry")
        if isinstance(telemetry, Mapping):
            scope = {"source": "cgroup"}
            for field in ("cpu_seconds", "cpu_user_seconds", "cpu_system_seconds",
                          "memory_current_bytes", "memory_peak_bytes"):
                if telemetry.get(field) is not None:
                    scope[field] = telemetry[field]
            if len(scope) > 1:
                profile["scope"] = scope
            measured_io = telemetry.get("process_io")
            if isinstance(measured_io, Mapping):
                # Totals only.  ``live``, ``retired`` and ``members`` are the
                # sampler's working state for the next tick, not something a
                # receipt read years later has any use for.
                profile["process_io"] = {
                    key: value for key, value in measured_io.items()
                    if key not in ("live", "retired", "members")
                }

        window = self._box_window(start, finished_unix)
        framebuffer = outcome.pop("gpu_framebuffer_window", None)
        if isinstance(framebuffer, Mapping):
            # A memory-only discrete device has no power, clock or unified
            # memory series.  Its broker-derived VRAM reading is therefore the
            # GPU group for this action's box window, never a fabricated zero
            # in the pqteld shape.
            window["gpu"] = dict(framebuffer)
            sources = {str(group.get("source")) for group in window.values()
                       if isinstance(group, Mapping) and group.get("source")}
            window["source"] = "+".join(sorted(sources))
            reason = window.pop("reason", None)
            if isinstance(reason, str) and reason:
                window["errors"] = [*window.get("errors", []), reason]
        profile["box_window"] = window
        return profile

    @staticmethod
    def _box_window(start_unix: float, finished_unix: float) -> dict[str, object]:
        """The box's own view of these seconds, or why there isn't one.

        Bounded and total: the finish path may not fail on telemetry, so every
        way this can go wrong ends in ``unavailable`` with the reason on the
        record.  An action lost to its own instrumentation would be a worse
        defect than the blindness this is fixing.
        """

        try:
            return box_window.read_window(
                start_unix, finished_unix, host=socket.gethostname(),
                csv_dir=box_window.default_csv_dir(),
                gpu_reference=box_window.gpu_power_reference)
        except Exception as exc:                                 # noqa: BLE001
            return {"schema": box_window.BOX_WINDOW_SCHEMA_V1,
                    "host": socket.gethostname(),
                    "start_unix": start_unix, "end_unix": finished_unix,
                    "source": "unavailable",
                    "reason": f"{type(exc).__name__}: {exc}"}

    def _execute_in_checkout(
        self,
        item: Mapping[str, object],
        *,
        checkout_root: str | Path,
        python: str | Path = sys.executable,
        timeout_s: float | None = None,
        heartbeat_s: float = HEARTBEAT_S,
        timeout_grace_s: float = TIMEOUT_GRACE_S,
        containment: bool = False,
        progress: ProgressPolicy | None = None,
    ) -> dict[str, object]:
        """Run one claimed item through the canonical worker argv.

        Executes as a subprocess rather than in-process on purpose: it is the
        same launch SLURM would have made, so the executed contract does not
        depend on which transport delivered the action.

        ``timeout_s`` bounds the *action*, not just this launcher.  What is
        launched here is a worker that runs the action as a further child, so
        the timeout signals the launcher's whole process group and the launcher
        relays that into the action's own session; the timeout path itself is
        bounded end to end, because a timeout that can hang is not a timeout.
        """

        key = str(item["action_key"])
        timeout_s = _execution_timeout(item, timeout_s)
        argv = [str(python)] + worker_argv(
            worker_script=item["worker_script"],
            action_key=key,
            cas_root=item["cas_root"],
            checkout_root=checkout_root,
            recompute=item.get("recompute") is True,
        )
        allocation = item.get("cpu_allocation")
        if allocation is not None:
            host = str(item.get("reserved_on") or "")
            if host != socket.gethostname():
                raise PoolContractError("CPU allocation belongs to another host")
            ledger = self.ledger(host)
            tiers = _read_json(ledger.base / "cpu-map.json")
            if tiers is None or ledger.cpu_allocation(key, tiers) != allocation:
                raise PoolContractError("CPU allocation differs from held reservation")
            cpus = list(allocation["preferred"]) + list(allocation["fallback"])
            if len(cpus) != self.demand_of(item).get("cpu", 0):
                raise PoolContractError("CPU allocation does not cover demand")
            if cpus:
                if not set(cpus) <= os.sched_getaffinity(0):
                    raise PoolContractError("CPU allocation exceeds current affinity")
                # taskset applies affinity before exec, without preexec_fn in
                # this multithread-capable parent. Descendants inherit it.
                argv = ["/usr/bin/taskset", "--cpu-list", cpu_topology.as_range(cpus),
                        *argv]
        owner = str(item.get("claimed_by") or "")
        started = _now()
        # Withdrawal checkpoint one of three: before the launch.  A cancellation
        # that landed in the microseconds between ``claim``'s rename and this
        # call would otherwise start the work anyway, and then have to stop it.
        if self.withdrawal_covers(item) is not None:
            return {
                "status": "withdrawn",
                "returncode": None,
                "stdout": "",
                "stderr": "",
                "elapsed_s": 0.0,
                "argv": argv,
                "cpu_allocation": allocation,
            }
        scope = self._start_resource_scope(item) if containment else None
        if scope is not None:
            # The broker launches taskset inside the aggregate slice. The
            # stdio proxy itself is not an attributed action process. The
            # sealed worker_script names the runtime being launched --
            # retained or current -- so the proxy comes from that same
            # proven runtime however the argv is prefixed.
            argv = scope.wrap_argv(argv, worker_script=item["worker_script"])
        # Read immediately before the launch and again at every way out, so
        # the difference is this child's and not the worker loop's history.
        rusage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
        # Name the file before the launch, and clear anything a previous
        # attempt on this key left behind, so what is read afterwards is this
        # attempt's or nothing.
        status_path = self.action_status_path(key)
        with suppress(OSError):
            status_path.unlink()
        # Same rule for the progress file, and one more on top of it: the token
        # is minted here, per launch.  Clearing the path bounds a *tidy*
        # previous attempt; a token this launch invented is what bounds an
        # untidy one that outlived SIGKILL and still holds the path open.
        progress_path = self.action_progress_path(key)
        progress_token = uuid.uuid4().hex
        with suppress(OSError):
            progress_path.unlink()
        progress_environment = (
            {} if progress is None else {
                pb.ACTION_PROGRESS_PATH_ENV: str(progress_path),
                pb.ACTION_PROGRESS_TOKEN_ENV: progress_token,
                # The two conveniences (#488).  Both come from this box rather
                # than from the request: the phase list is the policy this
                # watchdog will actually enforce, and the helper is the file
                # this generation will read the record with, so an action that
                # cannot import PrismaBuild still writes the bytes its own
                # worker accepts instead of a second copy of the schema.
                pb.ACTION_PROGRESS_PHASES_ENV: json.dumps(
                    [phase.name for phase in progress.phases]),
                pb.ACTION_PROGRESS_HELPER_ENV: str(
                    Path(pb_progress.__file__).resolve()),
            }
        )
        # No payload exists during withdrawal, scope preparation or status-file
        # cleanup. Shared I/O there must not spend its execution budget.
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        process = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            # The action's own ending, and a killed run's partial profile,
            # travel in a file because this process's exit status cannot
            # carry them.
            env={**os.environ, pb.ACTION_STATUS_PATH_ENV: str(status_path),
                 **progress_environment, **self.residency_map_environment(item)},
            # The launcher leads its own group so the timeout can signal the
            # group rather than the single pid.  ``kill()`` on the pid reaches
            # the launcher only, and leaves the action holding the GPU.
            start_new_session=True,
        )
        # Say which process is the launcher, so a withdrawal ON THIS BOX can
        # signal the action's group at once instead of waiting a heartbeat for
        # the loop below to notice.  The lease's own ``pid`` is this loop, which
        # is not the same process and must never be the signal's target.
        # Inside the try, not before it: this write is the first thing after
        # the Popen, and an unwind here -- a Ctrl-C landing on it, or the lease
        # write itself failing -- would otherwise leave the action running in
        # its own session with nothing left to reap it.  Everything after the
        # Popen belongs under the same guard.
        try:
            checkpoint_started = time.monotonic()
            observation = _observe_execution(process, scope=scope)
            watch = (None if progress is None else ProgressWatch(
                progress_path, progress_token, progress,
                started=checkpoint_started))

            def ending(outcome: dict[str, object]) -> dict[str, object]:
                """Every way out files the same evidence and clears the same files.

                Five endings now leave this loop -- containment, withdrawal,
                the requested deadline, the stall allowance and the action's
                own exit -- and the one thing worse than any of them is four of
                them agreeing about what a record carries and one not.
                """

                if watch is not None:
                    outcome["progress_observation"] = watch.as_record(
                        now=time.monotonic())
                framebuffer = getattr(scope, "_framebuffer_window", None)
                if isinstance(framebuffer, box_window.DiscreteFramebufferWindow):
                    group = framebuffer.group()
                    if group is not None:
                        outcome["gpu_framebuffer_window"] = group
                with suppress(OSError):
                    progress_path.unlink()
                return self._merge_action_status(outcome, status_path)
            self.write_lease(
                key,
                owner=owner, claim_snapshot=item,
                child_pid=process.pid,
                execution_observation=observation,
                container_owner=(str(item["container_owner"])
                                 if item.get("container_owner") else None),
            )
            # Shared checkpoint I/O can stall independently of the payload.
            # Pause only that measured interval, retaining all budget already
            # spent in spawn/communicate. This is not a fresh timeout grant.
            if deadline is not None:
                deadline += time.monotonic() - checkpoint_started
            if watch is not None:
                watch.shift(time.monotonic() - checkpoint_started)
            # Refresh the lease while the child runs; a long action must not be
            # reaped out from under itself.
            next_heartbeat = time.monotonic() + heartbeat_s
            next_progress_poll = time.monotonic() + heartbeat_s
            while True:
                try:
                    interval = min(heartbeat_s, 2.0) if scope is not None else heartbeat_s
                    if deadline is not None:
                        interval = min(interval, max(0.0, deadline - time.monotonic()))
                    if watch is not None:
                        interval = min(
                            interval,
                            max(0.0, watch.stall_deadline() - time.monotonic()))
                    out, err = process.communicate(timeout=interval)
                    break
                except subprocess.TimeoutExpired as exc:
                    checkpoint_started = time.monotonic()
                    observation = _observe_execution(
                        process, observation, stdout=exc.output,
                        stderr=exc.stderr, scope=scope)
                    if scope is not None:
                        telemetry = self._sample_resource_scope(scope)
                        resource_failure = self._resource_failure(telemetry)
                        if resource_failure:
                            scope.terminate_owned(resource_failure)
                            pb._terminate_process_group(process, grace_s=timeout_grace_s)
                            out, err, survived = _drain(process, timeout_s=timeout_grace_s)
                            return ending({
                                "status": "failed", "returncode": 137,
                                "execution_observation": observation,
                                "termination_reason": resource_failure,
                                "termination_evidence": telemetry.get("termination_evidence"),
                                "resource_telemetry": telemetry,
                                "stdout": out, "stderr": err,
                                "action_survived_kill": survived,
                                "elapsed_s": _now() - started,
                                "child_rusage": _reaped_children(
                                    rusage_before,
                                    resource.getrusage(resource.RUSAGE_CHILDREN)),
                                "argv": argv, "cpu_allocation": allocation,
                            })
                    # Checkpoint two: the cross-box path.  A withdrawal from another
                    # box cannot signal anything on this one, so this poll is what
                    # makes the verb correct from anywhere -- at a cost of at most
                    # one heartbeat, and none at all when the operator is here.
                    if self.withdrawal_covers(item) is not None:
                        if scope is not None:
                            scope.terminate_owned("withdrawn")
                        out, err = self._stop_action(process)
                        return ending({
                            "status": "withdrawn",
                            "execution_observation": observation,
                            "returncode": process.returncode,
                            "stdout": out,
                            "stderr": err,
                            "elapsed_s": _now() - started,
                            "child_rusage": _reaped_children(
                                rusage_before,
                                resource.getrusage(resource.RUSAGE_CHILDREN)),
                            "argv": argv,
                            "cpu_allocation": allocation,
                        })
                    if watch is not None and time.monotonic() >= next_progress_poll:
                        # On the heartbeat's cadence, in the directory the
                        # heartbeat already writes to: one small read per
                        # running action per ``heartbeat_s``, which is the
                        # whole of what this contract costs a box.
                        # Credit this whole checkpoint once below. Anchoring
                        # an accepted sample after earlier checkpoint I/O and
                        # then refunding it again would grant extra quiet.
                        watch.sample(now=checkpoint_started)
                        next_progress_poll = time.monotonic() + heartbeat_s
                    if time.monotonic() >= next_heartbeat:
                        self.write_lease(
                            key, owner=owner, child_pid=process.pid, claim_snapshot=item,
                            execution_observation=observation,
                            progress_observation=(
                                None if watch is None
                                else watch.as_record(now=time.monotonic())),
                            container_owner=(str(item["container_owner"])
                                             if item.get("container_owner") else None),
                        )
                        next_heartbeat = time.monotonic() + heartbeat_s
                    if deadline is not None:
                        deadline += time.monotonic() - checkpoint_started
                    if watch is not None:
                        watch.shift(time.monotonic() - checkpoint_started)
                    if deadline is not None and time.monotonic() >= deadline:
                        # Worst case this branch spends three grace budgets
                        # -- TERM wait, KILL wait, drain (~45 s) -- without
                        # refreshing the lease, against a 300 s expiry.
                        if scope is not None:
                            scope.terminate_owned("timeout")
                        pb._terminate_process_group(
                            process, grace_s=timeout_grace_s
                        )
                        out, err, survived = _drain(
                            process, timeout_s=timeout_grace_s
                        )
                        return ending({
                            "status": "timeout",
                            "termination_reason": "execution_deadline",
                            "execution_observation": observation,
                            # Stays None: ``pbrun`` returns any integer
                            # ``returncode`` as its own exit status, and an
                            # action that finished inside the tick that
                            # crossed the deadline would hand it a 0 for a
                            # record filed as a timeout.  ``status`` is the
                            # authority here; the launcher's exit goes in a
                            # field of its own below.
                            "returncode": None,
                            # Which path the launcher took: 143 is its own
                            # unwind on the relayed TERM, -15/-9 mean it never
                            # handled the signal at all.
                            "launcher_returncode": process.returncode,
                            "stdout": out,
                            "stderr": err,
                            # True when the pipes never reached EOF, so
                            # something in the action's tree outlived SIGKILL
                            # (a D-state GPU wedge does).  This branch still
                            # returns and the ledger token is still released,
                            # so the flag is the only notice that it was
                            # released for a GPU somebody still holds.
                            "action_survived_kill": survived,
                            "elapsed_s": _now() - started,
                            "child_rusage": _reaped_children(
                                rusage_before,
                                resource.getrusage(resource.RUSAGE_CHILDREN)),
                            "argv": argv,
                            "cpu_allocation": allocation,
                        })
                    if (watch is not None
                            and time.monotonic() >= watch.stall_deadline()):
                        # One more read before ending it.  The boundary is
                        # exactly where a reporter that publishes every few
                        # seconds lands, and a record already on disk is
                        # advancement whether or not a poll had reached it.
                        stall_checkpoint = time.monotonic()
                        advanced = watch.sample(now=stall_checkpoint)
                        spent = time.monotonic() - stall_checkpoint
                        watch.shift(spent)
                        next_progress_poll = time.monotonic() + heartbeat_s
                        if deadline is not None:
                            deadline += spent
                        if (not advanced
                                and time.monotonic() >= watch.stall_deadline()):
                            # Same three grace budgets, same precedence: this
                            # rung is reached only when containment,
                            # withdrawal and the requested deadline all had
                            # nothing to say.
                            if scope is not None:
                                scope.terminate_owned("timeout")
                            pb._terminate_process_group(
                                process, grace_s=timeout_grace_s
                            )
                            out, err, survived = _drain(
                                process, timeout_s=timeout_grace_s
                            )
                            return ending({
                                # A stall IS an execution timeout: every reader
                                # of this lane already knows the word, and
                                # inventing a sixth status would make a policy
                                # change look like a schema change.  Which
                                # policy ended it is in the reason.
                                "status": "timeout",
                                "termination_reason": "no_progress",
                                "execution_observation": observation,
                                "returncode": None,
                                "launcher_returncode": process.returncode,
                                "stdout": out,
                                "stderr": err,
                                "action_survived_kill": survived,
                                "elapsed_s": _now() - started,
                                "child_rusage": _reaped_children(
                                    rusage_before,
                                    resource.getrusage(resource.RUSAGE_CHILDREN)),
                                "argv": argv,
                                "cpu_allocation": allocation,
                            })
        except BaseException:
            # The launcher leads its own session now, so a Ctrl-C or any other
            # signal reaching this loop no longer reaches it -- before the new
            # session it did, and the launcher's own unwind reaped the action.
            # Unwinding from here without reaping would leave exactly the
            # orphan that session was introduced to bound.
            pb._terminate_process_group(process, grace_s=timeout_grace_s)
            _drain(process, timeout_s=timeout_grace_s)
            with suppress(OSError):
                status_path.unlink()
            with suppress(OSError):
                progress_path.unlink()
            raise
        status = "executed" if process.returncode == 0 else "failed"
        if watch is not None:
            # A short action may finish before the first heartbeat, or publish
            # its last committed counter after the most recent poll. Retain
            # that evidence before ending() removes the channel. This read is
            # observational: it cannot change the completed action's verdict.
            watch.sample(now=time.monotonic())
        # Checkpoint three: on the way out.  When the operator's own signal
        # reached the action group first, the launcher reports the SIGTERM that
        # stopped it and this worker would otherwise log a defect for a
        # decision.  ``finish`` files the outcome correctly either way; this is
        # about the line the worker prints and the record's ``status``.
        if status == "failed" and self.withdrawal_covers(item) is not None:
            status = "withdrawn"
        outcome = {
            "status": status,
            "execution_observation": observation,
            "returncode": process.returncode,
            "stdout": out,
            "stderr": err,
            "elapsed_s": _now() - started,
            "child_rusage": _reaped_children(
                rusage_before, resource.getrusage(resource.RUSAGE_CHILDREN)),
            "argv": argv,
            "cpu_allocation": allocation,
        }
        # One key, lifted by one function, so #372's Tier 0 and Tier 1 touch
        # this path without touching each other.
        profile = profile_from_launcher_stdout(out)
        if profile is not None:
            outcome["profile"] = profile
        return ending(outcome)

    def _stop_action(self, process: subprocess.Popen) -> tuple[str, str]:
        """Stop a withdrawn action and collect whatever it managed to say.

        The read is bounded on purpose.  The action inherits the launcher's
        stdout and stderr pipes, so an unbounded ``communicate()`` after a kill
        returns only when the *action* exits -- the very process this is trying
        to stop.  A stop path that can itself hang is not a stop path.
        """

        terminate_action(process.pid)
        try:
            return process.communicate(timeout=WITHDRAW_GRACE_S)
        except subprocess.TimeoutExpired:
            process.kill()
        try:
            return process.communicate(timeout=WITHDRAW_GRACE_S)
        except subprocess.TimeoutExpired:
            return "", ""

    @_serialized_key
    def _defer_unstarted_claim(self, item: Mapping[str, object]) -> None:
        """Return a broker-refused launch without recording an execution attempt."""
        key = str(item["action_key"])
        record = _read_json(self.item_path(CLAIMED, key))
        if record is None or not _same_claim(record, item):
            return
        if record.get("resource_scope") is not None:
            raise PoolContractError("cannot defer an attempt that already owns a resource scope")
        read_claim = dict(record)
        host = self.resolve_claim_holder(key, record)
        if host is not None:
            record["claimed_host"] = host
        tombstone, mine = self._entomb_claim(key, expect=read_claim)
        if tombstone is None or not mine:
            return
        self.lease_path(key).unlink(missing_ok=True)
        self._release_reservation(key, host=host)
        destination = self._shape_as_ready_item(record, action_key=key)
        record["maintenance_deferred_unix"] = _now()
        _write_json_atomic(tombstone, record)
        try:
            os.link(tombstone, destination)
        except FileExistsError:
            self._file_superseded(
                record, key=key, kind="maintenance-deferred", status="dropped",
                reason="a newer publication already owns ready after maintenance deferral",
            )
        tombstone.unlink(missing_ok=True)

    def serve_once(
        self,
        *,
        tags: Iterable[str] = (),
        has_gpu: bool = False,
        python: str | Path = sys.executable,
        timeout_s: float | None = None,
        capacity: Mapping[str, int] | None = None,
        cpu_tiers: Mapping[str, Sequence[int]] | None = None,
        adaptive_cpu: bool = False,
        containment: bool = False,
        ready: list[dict[str, object]] | None = None,
        observed_images: Container[str] | None = None,
        admission_open: Callable[[], bool] | None = None,
    ) -> dict[str, object] | None:
        """Reap, claim, run, record.  ``None`` when the queue had nothing.

        ``None`` also means "nothing this box may admit right now" once
        ``capacity`` is in play -- including the deliberate case where a starved
        item is withholding the host.  A caller that loops should treat it as
        back-pressure and poll again, not as an empty queue.

        ``ready`` is a prefetched ``ready_items`` snapshot, read in an
        abandonable child by a caller that must not park in the scan (#16).
        ``None`` scans here, in-process, as before.

        ``observed_images`` is the claiming box's bounded local container
        inventory, passed through to the claim check for items that declare
        one; absent means unknown, and unknown refuses (#714).

        ``admission_open`` is an optional caller-owned fence re-checked under
        the per-key transition lock just before the claim rename.
        """

        if self._sweep_due():
            self.reap_stale()
        item = self.claim(tags=tags, has_gpu=has_gpu, capacity=capacity,
                          cpu_tiers=cpu_tiers, adaptive_cpu=adaptive_cpu,
                          ready=ready, observed_images=observed_images,
                          admission_open=admission_open)
        if item is None:
            return None
        key = str(item["action_key"])
        try:
            outcome = self.execute(item, python=python, timeout_s=timeout_s,
                                   **({"containment": True} if containment else {}))
        except resource_scope.ResourceUnavailable:
            # Only create emits this typed maintenance refusal: no payload or
            # scope has been created, so capacity returns without an attempt.
            self._defer_unstarted_claim(item)
            return None
        except BaseException as exc:                      # noqa: BLE001
            # Never leave a claim dangling: an unexpected failure is recorded as
            # a terminal state, not left for the reaper 300 s later.
            self.finish(
                key,
                status="failed",
                detail={"exception": repr(exc)},
                claim_snapshot=item,
            )
            raise
        self.finish(
            key,
            status=str(outcome["status"]),
            detail=outcome,
            claim_snapshot=item,
        )
        return outcome


def profile_from_launcher_stdout(stdout: str) -> dict[str, object] | None:
    """The launcher's ``profile`` record, out of the JSON it prints when it ends.

    ``core.main`` prints one result object as its last line; a profiled run
    carries the CAS reference to its profile under ``profile`` there.  This
    lifts that one key into the outcome so ``finish`` files it in the ending
    and a reader tests a field rather than parsing a launcher's stdout.

    Anything else -- an empty stdout, a non-JSON last line, a run with no
    profile -- is ``None``.  A stdout that cannot be parsed is not a defect
    here: an unprofiled action's last line is still a result object, and a
    failing one may print nothing at all.
    """

    for line in reversed(str(stdout or "").splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            value = json.loads(line)
        except ValueError:
            return None
        if not isinstance(value, dict):
            return None
        profile = value.get("profile")
        return profile if isinstance(profile, dict) else None
    return None


def describe_placement_census(census: Mapping[str, object]) -> str:
    """One line of the census, in the words every reader should use for it.

    Kept beside the measurement rather than at each call site so the metric
    has one name wherever it is printed.  A number two tools describe
    differently is a number nobody can grep for.
    """

    if not census.get("known"):
        return f"ready {int(census.get('ready', 0))} (fleet width unknown: no worker has announced)"
    parts = [f"ready {int(census.get('ready', 0))}"]
    one_box = int(census.get("one_box", 0))
    pinned = census.get("pinned_to") or {}
    where = ""
    if isinstance(pinned, Mapping) and pinned:
        where = " (" + ", ".join(f"{host} {n}" for host, n in pinned.items()) + ")"
    parts.append(f"{one_box} on exactly one box{where}")
    # The migration number.  ``one_box`` falls for two very different reasons
    # -- submitters moving to a checkout every box can see, or a box simply
    # going away -- and only the first is the fix working, so the half that
    # a path caused is named separately.
    by_path = int(census.get("one_box_by_path", 0))
    if by_path:
        parts.append(f"{by_path} by a box-local checkout")
    parts.append(f"{int(census.get('wide', 0))} on more than one")
    parts.append(f"{int(census.get('unplaceable', 0))} on none")
    # Printed only when there are any: a zero here would teach readers to skip
    # the clause, which is the one thing it must not be.
    unreadable = int(census.get("unreadable", 0))
    if unreadable:
        parts.append(f"{unreadable} unreadable")
    return ", ".join(parts)

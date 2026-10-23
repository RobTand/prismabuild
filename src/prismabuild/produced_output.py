"""PB-produced-output staging scope: deterministic PB-owned path (R2).

An admitted GPU action writes new exact activation/cotangent/checkpoint
artifacts and reads them again during THE SAME action. Existing PB manifests
only stage pre-existing immutable external inputs. This module owns the
staging contract for produced outputs; it does not copy bytes itself
(existing movers do), creates no parallel cache/ledger/dispatcher, and never
rewrites a sealed input manifest or action key.

WRITE-ONLY (#912): a template marked ``write_only`` declares outputs its own
action never reads again. It reserves no stage window (every tier's minimum
and window are zero, so `owner_demand_terms` is empty and admission charges
no stage token), and its batches commit at their origin through
`BatchLedger.commit_origin`: no mover, no stage copy, no funding. A later action
declares such a batch in its data manifest (`origin_batch_manifest`) and
stages it through the ordinary input path. The durable charge and the path
ownership of an origin-only batch last until `BatchLedger.reclaim` proves its
origin gone, as for a staged batch. A template without ``write_only`` hashes
exactly as before.

ORIGIN LIFETIME (#914): an origin-only batch is committed with a lifetime.
``retain``, the default, is #912's batch unchanged: PB never deletes it. A
``consumed`` batch is a handoff. Each consumer that declares it files a
consumer declaration at submission (`declare_origin_consumer`), and
`origin_retirement_tick`, run once per tier-loop cycle, deletes its origin
files and frees its durable charge once every declared consumer has
succeeded. It also sweeps a consumed batch that no consumer declared and
whose producer attempt is dead.

READ-BACK ORIGIN COMMIT (#1034): a read-back owner that reads a group back
from its own local spool publishes no stage copy, so it commits the group at
its origin too, through `ProducedSpool.commit_origin_group` once the export is
acknowledged (`BatchLedger.commit_origin` with ``landed``). That ends the prewrite
and charges the actual bytes. No consumer can declare such a batch, so a
``consumed`` one is swept once its owner attempt has ended, success included. Every retirement checks each file against
the identity its commit recorded, and every unknown keeps the batch.

R2 split (root review, defects 1-7):

  * TEMPLATE (sealed pre-submit): authorized prefix/slots/classes, durable
    byte maxima, per-tier minimum/window demand, allowed tiers. Carries NO
    action key and NO broker nonce (the client cannot know either).
  * INSTANCE (runtime, bound post-admission): template reference +
    owner (action/nonce/scope) derived from the PROTECTED live claim, never
    from caller arguments. Foreign/stale attempt context refuses.
  * BATCH (immutable committed window): exact manifest + class bytes + mover
    ownership on one tier. One scope holds many batches; each batch composes
    under its own namespace with the existing one-manifest validator.

OWNER vs NAMESPACE: OWNER = (owner_action_key, owner_attempt) names the
running action for holder binding and for the authoritative terminal/
containment proof. NAMESPACE (instance/batch derived keys) names material,
maps, pins and ledger holders and has NO terminal record. A certificate
naming a namespace is invalid. Writer digests are reused from the streaming
receipt, never recomputed by rereading HDD payloads.

IDENTITY (R3, no fallback): binding requires BOTH halves complete and
equal: launch env (`PRISMABUILD_ACTION_KEY/NONCE/SCOPE`, set by the
resource_exec proxy from the exact launch identity) AND the live claim
row's broker-issued `resource_scope` control (32-hex nonce + broker-formula
scope_id + matching action_key). Either half missing, or any mismatch,
refuses (`no-launch-context` / `no-control-context` / `attempt-superseded`,
same names as the PB730 `injected_context` contract). No intent fallback,
no claim-derived nonce, no live-claim-only binding. Fixtures file a
realistic broker control through the existing `ResourceScope` verification
(`_adopt_created_scope` over a canned broker response); production accepts
only broker-issued controls.

CREDIT vs TOKENS (liveness R2 direction, binding here): the admission
record (`admit_instance`) is binding metadata, NOT funding and NOT
capacity. Physical tokens are held ONLY by batch movers, exact per range,
acquired under the batch holder and TRANSFERRED whole to mover ownership
with no free interval (existing `TierLedger.handover`). No standing
ledger pool is held beside movers. The funded-claim primitive family
(`FundingLedger.reserve_window` / `ResourceLedger.transfer_tokens` / `FundingLedger.cover_window`) is
delivered by the prepaid-output lane, so `admit_funded_window` reports the
delivered `prepaid-per-batch` binding (declaration-only: the owner action's
own tier demand, reserved once at its claim; per-batch funding is the
authority). The `funding-primitive-pending` refusal remains only for a
pool missing one of those primitives. No second ledger here, never
subtracts unrelated holders' tokens.

MATERIALIZATION (bounded-window restage): a committed BATCH is immutable and
carries ONE durable origin charge; the stage copy under it is a window the
fleet takes back at retirement. `ensure_batch_materialized` is the ONE
transition this adds -- make an already committed batch resident again over
the SAME origin files, under the same owner action and attempt, the same
logical batch, manifest, descriptors, namespace and output prefix, and the
same durable charge. It adds no v2 record, no parallel
cache and no second dispatcher (an origin-only batch has nothing to
re-materialize, and refuses): it reuses the published first-publisher
sealing (`_seal_output_mover`), `movement_actions`, exact prepaid funding, the
strict SDK, the existing egress and the existing recovery. The caller chooses
neither origins (they come from the immutable record), nor tokens (ordinary
prepaid transfer), nor the successor id (the content-addressed key of a
request PB seals over the filed materialization GENERATION). Safety for a DEV
null-digest batch rests on the origin identity tuple captured at FIRST commit
(`origin_identity`, via `reader_lease.portable_identity`) and rechecked before
every re-materialization -- never an lstat size, and never a new payload hash.

PB730 owns: corrected `pin_id_for` (canonical object set), the additive
owner/material-namespace SDK contract, the containment writer, and the
immutable helper-env injection. This lane does not edit `reader_lease.py`,
does not invent pin serialization (ours is the manifest object set only),
and returns the exact SDK dependency instead of a stub.
"""

from __future__ import annotations

from prismabuild.legacy_funding import LegacyFunding

from prismabuild import funding

from . import output_contract as oc
from . import batch_ledger as bl

# Documented module-level reader contract (#955), shared with the real owners.
batch_record = bl.BatchLedger.record
batch_records = bl.BatchLedger.records
BATCH_STATE_COMMITTED = oc.BATCH_STATE_COMMITTED
BATCH_STATE_RETIRING = oc.BATCH_STATE_RETIRING
BATCH_STATE_RECLAIMED = oc.BATCH_STATE_RECLAIMED
ProducedOutputError = oc.ProducedOutputError
from . import pool
_RENAMEAT2: list[object] = []
_KEPT_PREWRITES: dict[str, tuple[tuple[object, ...], str]] = {}
from . import core, digest_primitives

from . import claim_identity

from prismabuild import immutable_files

from prismabuild import kept_reads
from prismabuild.record_store import (
    Absent, KeyStanding, Moving, Present, QueueRow, RecordStore, Unknown,
)
from collections.abc import Collection, Mapping, Sequence
import contextlib
import errno
import hashlib
import json
import os
from pathlib import Path
import socket
import stat
import sys
import tempfile
import time
import uuid
from prismabuild import tier_holdings
from prismabuild import staged_path
from .output_contract import (
    PRODUCED_OUTPUT_REF_SCHEMA_V1, OUTPUT_TEMPLATES_SUBDIR,
    OUTPUT_SCOPES_SUBDIR, OUTPUT_BATCHES_SUBDIR, OUTPUT_FRAGMENTS_SUBDIR,
)

#: The queue item's projection of the sealed declaration: which immutable
#: template this action's tier demand was derived from. The runtime binds
#: from this ref (filed body + live claim), never from a caller-supplied
#: template object.
#: The sealed params record that makes a RE-materialization's action key its
#: own: the batch stays identical, so the PB-filed materialization generation
#: is what distinguishes the successor's content-addressed mover/funding key
#: from the spent one. Never a caller nonce; see `ensure_batch_materialized`.
#: What a consumer declares to read one origin-only batch (#912): the batch's
#: filing coordinates plus its manifest digest. See `origin_batch_ref`.
#: The data-manifest annotation that carries a consumer's declared batches.
#: Where a data_manifest.v2 read plan reads its declared batches (#946): one
#: ``{"phase", "refs"}`` per read phase that reads committed batches, in plan
#: order. A deferred consumer's static plan names the ``--after`` edge that
#: fills each slot instead (``{"phase", "after"}``). See
#: `place_origin_batches`.
#: An origin-only batch's lifetime (#914). ``retain`` is the default and is
#: never written into a record, so a retained batch is filed byte for byte as
#: #912 filed it; ``consumed`` is retired once its declared consumers succeed.
#: One consumer's declaration that it reads one consumed batch (#914), filed
#: under the batch's instance at ``consumers/<batch_id>/<consumer_key>.json``.
#: An operator's release of one declaration (#926), filed beside the
#: declarations at ``released-consumers/<batch_id>/<consumer_key>.json``.
#: The declaration itself is never removed.
#: The queue-wide index of those releases (#954): one file per released
#: consumer and batch, ``<consumer_key>.<ref sha256>.json`` under
#: `pool.record_store.RecordStore.released_origin_consumers_dir`, so a claim finds a
#: released key by listing one directory. It is written before the release
#: record, and only the record makes the release real.
#: The mover's typed refusal when a declared window's origin directories are
#: not on the tier host (`stage_move.origin_reachability_diagnosis`). Spelled
#: as data: this package imports no fleet tool.

#: One pointer per attempt of each template, ``<template_id>/<owner>.<nonce>``
#: (#1053): how the attempts that may own an origin path are found without
#: listing every owner's scopes. `declare_instance` files it before the
#: instance; the tier loop's origin-retirement tick files it for any scope
#: that predates the index. It lives inside `OUTPUT_TEMPLATES_SUBDIR`, which
#: every residency enumerator skips as a record directory, so an egress or a
#: census running older code never reads it as an unknown fragment
#: namespace (#798); a template is filed as ``<template_id>.json``, and a
#: dotted directory is never one.
#: Filed in that directory once a tick has indexed every scope it listed.
#: Until then the attempts are found by listing the scopes themselves.

#: Sealed helper-root env name (spelling only; PB730 owns injection).
#: PQ resolves the published runtime helper from this value and verifies
#: `reader_lease.__file__` under it. Never a mutable `/repo` checkout.

#: Sealed producer env that opts a restage mover into reserving pool fill
#: (#747).  "" or "0" is off (the default), "1" is on, anything else refuses
#: the restage.  Read from the producer's SEALED request, never from the
#: calling process: the switch is part of what the producer was admitted as.

#: Delivered SDK + funding dependencies this lane binds to (not stubs).


# --------------------------------------------------------------------------
# Templates (sealed pre-submit; no action key, no nonce)
# --------------------------------------------------------------------------


def template_path(queue_root: str | Path, template: Mapping[str, object]) -> Path:
    checked = oc.validate_template(template)
    return (Path(queue_root) / "residency" / oc.OUTPUT_TEMPLATES_SUBDIR
            / f"{checked['template_id']}.json")


def declare_template(queue_root: str | Path, template: Mapping[str, object]) -> Path:
    """File one template immutably; a conflicting body refuses."""

    from prismabuild import pool as pool_mod

    checked = oc.validate_template(template)
    path = template_path(queue_root, checked)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = core._compact_ascii_lf_bytes(checked)[:-1].decode("ascii").encode() + b"\n"
    try:
        immutable_files.publish_exact(
            path,
            raw,
            where="produced-output template",
            conflict_error=pool_mod.PoolContractError,
        )
    except pool_mod.PoolContractError as exc:
        raise oc.ProducedOutputError(
            f"a different template is already filed for {checked['template_id']}: {exc}"
        ) from None
    return path


def build_declaration(
    template: Mapping[str, object], template_input: Mapping[str, object]
) -> dict[str, object]:
    """The sealed action-params declaration for one validated template.

    Pure constructor owned here so the submitter (``pbrun``) and the core
    validator cannot drift: the template body is validated by
    :func:`validate_template`, the input row by the core input contract, and
    the digest is the canonical template identity. The file bytes stay in the
    CAS as an ordinary declared input; this declaration is the params half
    that makes the action key cover them.
    """

    from prismabuild.core import (
        PRODUCED_OUTPUT_DECLARATION_SCHEMA_V1,
        PRODUCED_OUTPUT_TEMPLATE_INPUT_ID,
        validate_input_contract,
    )

    checked = oc.validate_template(template)
    inch = validate_input_contract(template_input)
    if inch["id"] != PRODUCED_OUTPUT_TEMPLATE_INPUT_ID:
        raise oc.ProducedOutputError(
            "template input id must be "
            f"{PRODUCED_OUTPUT_TEMPLATE_INPUT_ID!r}")
    return {
        "schema": PRODUCED_OUTPUT_DECLARATION_SCHEMA_V1,
        "template_id": str(checked["template_id"]),
        "template_sha256": oc.template_sha256(checked),
        "input": inch,
    }


def declared_template(queue, action_key: str) -> dict[str, object]:
    """Load the template this sealed action actually declared.

    Reads the queue item's ``produced_output`` ref (projected by
    ``PoolQueue.publish`` from the validated template whose working window
    the tier demand covers), then the filed immutable body, and verifies the
    digest. A caller-supplied template object is never trusted: an arbitrary
    template plus a real claim binds nothing. Looks in ``claimed/`` first
    (runtime) then ``ready/`` (submitter verification); anything else
    refuses.
    """

    owner = oc.output_key(action_key, where="declared template action_key")
    observed = RecordStore(queue.root).queued(owner)
    if isinstance(observed, Unknown):
        assert observed.error is not None
        raise observed.error
    item = None if isinstance(observed, Absent) else observed.value
    if not isinstance(item, Mapping):
        raise oc.ProducedOutputError("no queued item for this action key")
    ref = item.get("produced_output")
    if not isinstance(ref, Mapping):
        raise oc.ProducedOutputError("this action declares no produced-output template")
    if ref.get("schema") != oc.PRODUCED_OUTPUT_REF_SCHEMA_V1:
        raise oc.ProducedOutputError(
            f"produced-output ref schema must be {oc.PRODUCED_OUTPUT_REF_SCHEMA_V1!r}")
    template_id = ref.get("template_id")
    digest = ref.get("template_sha256")
    if (not isinstance(template_id, str) or not template_id
            or "/" in template_id):
        raise oc.ProducedOutputError("produced-output ref names no template")
    oc.output_key(digest, where="produced-output ref template_sha256")
    assert isinstance(digest, str)
    path = (Path(queue.root) / "residency" / oc.OUTPUT_TEMPLATES_SUBDIR
            / f"{template_id}.json")
    # Bounded intake: filed bodies from this path are tiny envelopes; a
    # larger file at this name is foreign, never the declared template.
    try:
        from prismabuild.core import PRODUCED_OUTPUT_TEMPLATE_MAX_BYTES as _MAX

        with open(path, "rb") as handle:
            raw = handle.read(_MAX + 1)
    except OSError as exc:
        raise oc.ProducedOutputError(f"undeclared-template: {exc}") from None
    if len(raw) > _MAX:
        raise oc.ProducedOutputError(
            "tampered-template: filed body exceeds the template envelope")
    try:
        body = json.loads(raw.decode())
    except (UnicodeDecodeError, ValueError) as exc:
        raise oc.ProducedOutputError(
            f"undeclared-template: filed body unreadable: {exc}") from None
    checked = oc.validate_template(body)
    if (str(checked["template_id"]) != template_id
            or oc.template_sha256(checked) != digest):
        raise oc.ProducedOutputError(
            "tampered-template: filed body differs from the declared ref")
    return checked


def bind_declared_instance(
    queue, *, owner_action_key: str,
    claim_snapshot: Mapping[str, object],
    env: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Bind the runtime instance from the sealed declaration, not arguments.

    Loads :func:`declared_template` from the actual sealed queue item, then
    :func:`bind_instance` against the protected live claim + launch halves.
    There is no template argument to substitute, invent, or mismatch: a
    foreign, missing, or tampered declaration refuses before any byte is
    written.
    """

    template = declared_template(queue, owner_action_key)
    return bind_instance(
        queue, template, owner_action_key=owner_action_key,
        claim_snapshot=claim_snapshot, env=env)


# --------------------------------------------------------------------------
# Instances (runtime; bound to the live claim, never caller-supplied)
# --------------------------------------------------------------------------

def _broker_scope_id(action_key: str, nonce: str) -> str:
    """The scope id the broker issues for an action+nonce (existing rule).

    Same formula `ResourceScope._adopt_created_scope` enforces on every
    broker response: `prismabuild-job` + sha256(action+nonce)[:32] + `.slice`.
    Reused here as the control-identity check, never redefined.  This is the
    package-side owner: `resource_scope` calls it rather than restating the
    formula.  The broker itself keeps its own spelling -- a stdlib-only root
    daemon cannot import the package -- and the issuer/mirror agreement is
    pinned by `tests/test_broker_scope_contract_1301.py`.
    """

    return ("prismabuild-job"
            + digest_primitives.raw_sha256((action_key + nonce).encode())[:32]
            + ".slice")


def _launch_env(env: Mapping[str, str] | None) -> dict[str, str]:
    """Launch-bound identity halves (same names as PB730 `injected_context`).

    Read from the execution environment by default (the resource_exec proxy
    sets them from the exact launch identity); tests pass an explicit env
    mapping. Names resolve exactly as the candidate does: `core` constants
    where they exist, literal `PRISMABUILD_ACTION_*` otherwise (the NONCE /
    SCOPE names exist only in the PB730 lane on main).
    """

    import os as _os

    try:
        from prismabuild.core import ACTION_KEY_ENV as _KEY_ENV
    except ImportError:
        _KEY_ENV = "PRISMABUILD_ACTION_KEY"
    try:
        from prismabuild.core import ACTION_NONCE_ENV as _NONCE_ENV
    except ImportError:
        _NONCE_ENV = "PRISMABUILD_ACTION_NONCE"
    try:
        from prismabuild.core import ACTION_SCOPE_ENV as _SCOPE_ENV
    except ImportError:
        _SCOPE_ENV = "PRISMABUILD_ACTION_SCOPE"
    source = dict(_os.environ) if env is None else dict(env)
    return {"action_key": source.get(_KEY_ENV) or "",
            "nonce": source.get(_NONCE_ENV) or "",
            "scope_id": source.get(_SCOPE_ENV) or ""}


def bind_instance(queue, template: Mapping[str, object], *,
                  owner_action_key: str,
                  claim_snapshot: Mapping[str, object],
                  env: Mapping[str, str] | None = None) -> dict[str, object]:
    """Bind a runtime instance to launch + control identity; refuse all else.

    Requires BOTH halves complete and equal (the PB730 both-halves rule):
    launch env (`PRISMABUILD_ACTION_KEY/NONCE/SCOPE`) AND the live claim
    row's broker-issued `resource_scope` control (matching action_key,
    32-hex nonce, broker-formula scope_id). Refusals name the missing half
    (`no-launch-context` / `no-control-context`) or the mismatch
    (`attempt-superseded`); a missing/moved claim refuses
    (foreign/stale). The template must already be declared (byte-identical
    filed body) — an arbitrary caller template plus a real claim binds
    nothing. There is no intent fallback, no derived nonce, and no
    live-claim-only binding in production or fixture.
    """

    from prismabuild import pool as pool_mod

    checked = oc.validate_template(template)
    owner = oc.output_key(owner_action_key, where="instance owner_action_key")
    if not isinstance(claim_snapshot, Mapping):
        raise oc.ProducedOutputError("instance binding needs the claim snapshot")
    if claim_snapshot.get("action_key") != owner:
        raise oc.ProducedOutputError("foreign claim snapshot: action key mismatch")
    observed = RecordStore(queue.root).claimed(owner)
    if isinstance(observed, Unknown):
        assert observed.error is not None
        raise observed.error
    live = None if isinstance(observed, Absent) else observed.value
    if live is None:
        raise oc.ProducedOutputError("stale claim snapshot: no live claimed record")
    try:
        same = pool_mod.claim_identity.same_claim(live, claim_snapshot)
    except Exception as exc:
        raise oc.ProducedOutputError(f"stale claim snapshot: {exc}") from None
    if not same:
        raise oc.ProducedOutputError("stale claim snapshot: live claim moved on")
    # The template must be declared (filed, byte-identical) — an arbitrary
    # caller template plus a real claim binds nothing. (Attribution of the
    # declaration to this sealed owning action travels in the owner item
    # annotation; see the submission hook proposal.)
    canonical = core._compact_ascii_lf_bytes(checked)[:-1].decode("ascii").encode() + b"\n"
    try:
        filed = template_path(queue.root, checked).read_bytes()
    except OSError as exc:
        raise oc.ProducedOutputError(
            f"undeclared-template: {exc}") from None
    if filed != canonical:
        raise oc.ProducedOutputError("undeclared-template: filed body differs")
    control = live.get("resource_scope")
    claim_nonce = claim_scope = ""
    if isinstance(control, Mapping):
        if control.get("action_key") == owner:
            candidate = control.get("nonce")
            if (isinstance(candidate, str) and len(candidate) == 32
                    and all(c in oc.OUTPUT_KEY_HEX for c in candidate)):
                claim_nonce = candidate
            unit = control.get("scope_id")
            if isinstance(unit, str) and unit:
                claim_scope = unit
    if not claim_nonce or not claim_scope:
        raise oc.ProducedOutputError("no-control-context: live claim names no "
                                  "complete broker-issued nonce/scope")
    if claim_scope != _broker_scope_id(owner, claim_nonce):
        raise oc.ProducedOutputError("no-control-context: scope_id is not the "
                                  "broker-issued identity for this nonce")
    launch = _launch_env(env)
    if not launch["nonce"] or not launch["scope_id"]:
        raise oc.ProducedOutputError("no-launch-context: launch env names no nonce/scope")
    if launch["action_key"] != owner:
        raise oc.ProducedOutputError("foreign launch env: action key mismatch")
    if launch["nonce"] != claim_nonce or launch["scope_id"] != claim_scope:
        raise oc.ProducedOutputError("attempt-superseded: launch and control disagree")
    return {
        "schema": oc.INSTANCE_SCHEMA_V1,
        "version": 1,
        "template_id": str(checked["template_id"]),
        "template_sha256": oc.template_sha256(checked),
        "owner_action_key": owner,
        "owner_attempt": {"nonce": claim_nonce, "scope_id": claim_scope},
        "attempt_source": "broker-launch",
        "output_prefix": str(checked["output_prefix"]),
        "bound_unix": time.time(),
    }


# --------------------------------------------------------------------------
# Origin-path owners across attempts and actions (#1053)
# --------------------------------------------------------------------------
#
# A path under a template's output prefix can be named by any attempt of any
# template whose prefix contains it or is contained by it: a retry of the
# same key, or another action entirely (R13's relaunch was a new key on the
# same template).  These helpers find those attempts through the attempt
# index, bounded by the templates whose prefixes overlap, and read what each
# one names.  They never list every owner's scopes, except before the index
# is complete.

#: Each filed template's id and output prefix by ``(file path, inode)``, or
#: None for a file that names no prefix (`_filed_templates`).  A filed
#: template is immutable (`declare_template`), so a file is read once per
#: process.
#: ``os.path.realpath`` of each output prefix, once per process.


#: The event a hold by an attempt whose state is unknown files (#1065), once
#: per change. A live attempt's hold stays quiet: it will commit or end.
#: What frees a hold by an orphaned attempt (#1065). PB never ends an
#: attempt it cannot read, so an operator does.


# --------------------------------------------------------------------------
# Descriptors (class-enforcing, prefix-realpath-checked)
# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
# Manifest object set (ours) vs pin serialization (PB730's)
# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
# Admission credit (bound record, NOT ledger tokens) + per-batch prewrite
# --------------------------------------------------------------------------











def refill_window(queue, instance: Mapping[str, object],
                   template: Mapping[str, object], *, tier: str
                   ) -> dict[str, object]:
    """Bounded lifecycle refill of a long-lived producer window.

    Retirement returns spent credits to FREE (the egress releases by mover
    key; the fleet never restores producer holdings), so a producer whose
    batches completed must re-acquire its OWN admitted window capacity
    before the next batch can spend it. This acquires from free, under the
    owner key, exactly up to the aggregate window bound -- holdings plus
    outstanding intent names never exceed the template's window_gib on the
    tier, before or after. It is a lifecycle return of the producer's own
    admitted window, never a batch's fresh acquisition: batches still fund
    only by exact transfer. Requires the live owner and a provable census;
    an unavailable top-up is deferred when positive holdings already meet
    the declared minimum. That is not batch funding: the subsequent exact
    transfer must still prove the actual batch fits. Otherwise the typed
    tier-reservation-unavailable refuses rather than exceeding the bound. The
    lane's sequential-writer contract applies (one producer action per
    instance, as everywhere in this lane).
    """

    from prismabuild import pool as pool_mod
    from prismabuild import storage_tiers as tiers_mod

    try:
        checked_template, checked_instance = oc.bound_output_contract(
            template, instance)
    except oc.ProducedOutputError:
        return {"ok": False, "refusal": "template-mismatch"}
    if checked_template.get("write_only"):
        # A write-only template holds no window to refill (#912).
        return {"ok": False, "refusal": "template-is-write-only"}
    if tier not in checked_template["permitted_tiers"]:
        return {"ok": False, "refusal": "tier-not-permitted"}
    window = int(checked_template["working_demands"][tier]["window_gib"])
    minimum = max(1, int(
        checked_template["working_demands"][tier]["minimum_gib"]))
    owner = str(checked_instance["owner_action_key"])
    kind = tiers_mod.capacity_kind_of(tier)
    gated = bl.require_live_owner(queue, checked_instance)
    if gated is not None:
        return gated
    # Aggregate window accounting: owner-held names PLUS everything this
    # owner has live elsewhere -- transferred names and the fences of
    # claimed-but-unretired (consumed) batches, right up to the retirement
    # that releases them. Fail-retain on any unprovable record.
    outstanding, census_unknown = (
        queue.funding._output_outstanding_window_tokens(owner, tier, kind))
    if census_unknown:
        return {"ok": False, "refusal": "unknown-retain: funding-census"}
    with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
        ledger = queue.tier(tier)
        held = ledger.holdings(owner).get(kind, 0)
        room = window - held - outstanding
        if room <= 0:
            return {"ok": True, "tier": tier, "acquired": 0, "held": held,
                    "outstanding": outstanding, "window_gib": window}
        available = ledger.free_supply().get(kind, 0)
        take = min(room, available)
        if take <= 0 or not ledger.take(owner, {kind: take}):
            # Replenishment is advisory when existing credit remains usable.
            # In particular a contended mint guard can decline acquisition
            # even beside free capacity. Do not make exact prepaid funding
            # wait for that optional acquisition. Re-read after the attempt:
            # only current holdings, never the earlier snapshot, qualify.
            held = ledger.holdings(owner).get(kind, 0)
            if held >= minimum:
                return {"ok": True, "tier": tier, "acquired": 0,
                        "held": held, "outstanding": outstanding,
                        "window_gib": window, "kind": kind,
                        "refill_deferred": "tier-reservation-unavailable"}
            return {'ok': False, 'refusal': 'tier-reservation-unavailable', 'available': ledger.free_supply(), 'window_gib': window, 'held': held, 'outstanding': outstanding}
        held = ledger.holdings(owner).get(kind, 0)
    return {"ok": True, "tier": tier, "acquired": take, "held": held,
            "outstanding": outstanding, "window_gib": window,
            "kind": kind}


def unheld_window_gib(queue, tier_id: str) -> dict[str, object]:
    """The live producers' admitted windows that nobody holds, on one tier.

    A producer reserves its template's ``window_gib`` on the tier at claim
    (``owner_demand_terms``), and its batches spend that window by exact
    transfer. Retirement returns the spent credits to FREE, not to the
    owner, and the owner takes them back with `refill_window`. Between a
    retirement and the refill the window is owed but held by nobody, so a
    tier gate that counts only held tokens and queued demand counts it as
    zero, and a consumer's window can take the room the refill needs.

    Per live owner this is ``window - held - outstanding``, floored at zero,
    with the same terms `refill_window` bounds itself by: ``held`` is the
    owner's own holdings and ``outstanding`` is what its batches still hold
    elsewhere (`PoolQueue.funding._output_outstanding_window_tokens`). Both are held
    tokens a gate already counts, so the result never counts a token twice.
    A queued owner's window is still in its ready demand and a finished or
    superseded owner owes nothing, so only an owner whose live CLAIMED row
    names the instance's own attempt contributes.

    Liveness is read first, from the claimed row alone, so a dead owner's
    records are never opened: a torn record under a finished owner costs
    nothing. For a live owner, an unreadable instance, claim, filed template
    or holding makes the obligation unknown: ``gib`` is ``None`` and
    ``unknown`` names the owner. An unreadable batch census counts no
    outstanding tokens, which can only raise the result, and ``bounded``
    names the owner. Returns ``{"gib", "owners", "unknown", "bounded"}``.
    Read-only: it takes no lock and changes nothing.
    """

    from prismabuild import pool as pool_mod
    from prismabuild import storage_tiers as tiers_mod

    tier = str(tier_id)
    kind = tiers_mod.capacity_kind_of(tier)
    owed: dict[str, int] = {}
    unknown: list[dict[str, str]] = []
    bounded: list[str] = []
    scopes_root = bl.scopes_root(queue.root)
    try:
        owners = bl.scope_owners(scopes_root)
    except FileNotFoundError:
        owners = []
    except OSError as exc:
        return {"gib": None, "owners": {},
                "unknown": [{"owner": "", "error": f"scopes unreadable: {exc}"}],
                "bounded": []}
    templates_root = Path(queue.root) / "residency" / oc.OUTPUT_TEMPLATES_SUBDIR
    for owner in owners:
        try:
            if not pool_mod.record_store.value_or_raise(
                queue.records.item_presence(pool_mod.record_store.CLAIMED, owner),
            ):
                continue
            paths = bl.owner_instance_paths(scopes_root / owner)
        except OSError as exc:
            unknown.append({"owner": owner, "error": f"unreadable: {exc}"})
            continue
        window: int | None = None
        error = ""
        for path in paths:
            try:
                instance = oc.validate_instance(json.loads(path.read_text()))
            except (OSError, ValueError) as exc:
                error = f"instance {path.name} unreadable: {exc}"
                break
            if str(instance["owner_action_key"]) != owner:
                error = f"instance {path.name} names another owner"
                break
            gated = bl.require_live_owner(queue, instance)
            if gated is not None:
                refusal = str(gated.get("refusal") or "")
                if refusal in ("owner-not-running", "stale-superseded-owner"):
                    continue
                error = f"claim unreadable: {refusal}"
                break
            try:
                template = oc.validate_template(json.loads(
                    (templates_root
                     / f"{instance['template_id']}.json").read_text()))
            except (OSError, ValueError) as exc:
                error = f"template {instance['template_id']} unreadable: {exc}"
                break
            if oc.template_sha256(template) != instance["template_sha256"]:
                error = (f"template {instance['template_id']} is not the one "
                         "the instance is bound to")
                break
            if tier not in template["permitted_tiers"]:
                continue
            bound = int(template["working_demands"][tier]["window_gib"])
            window = bound if window is None else max(window, bound)
        if error:
            unknown.append({"owner": owner, "error": error})
            continue
        if window is None:
            continue
        try:
            held = int(queue.tier(tier).holdings(owner).get(kind, 0))
        except (OSError, pool_mod.PoolContractError, ValueError) as exc:
            unknown.append({"owner": owner, "error": f"holdings unreadable: {exc}"})
            continue
        outstanding, census_unknown = (
            queue.funding._output_outstanding_window_tokens(owner, tier, kind))
        if census_unknown:
            outstanding = 0
            bounded.append(owner)
        owed[owner] = max(0, window - held - int(outstanding))
    return {"gib": None if unknown else sum(owed.values()),
            "owners": owed, "unknown": unknown, "bounded": bounded}


# --------------------------------------------------------------------------
# Repeat materialization over one committed batch (bounded-window restage)
# --------------------------------------------------------------------------
#
# A committed batch is an IMMUTABLE logical unit with ONE durable charge. The
# stage copy under it is not: the bounded-window path stages a batch, lets a
# reader consume it, retires the copy to give the window credit back, and --
# when the same bytes are needed again -- stages the SAME batch a second time
# over the SAME origin files. The batch, its manifest, its descriptors, its
# namespace and its durable charge never move; only the materialization does.
#
# Everything below is that one transition. There is no second cache, no second
# dispatcher and no v2 record: a materialization reuses the published
# first-publisher helpers, the pool's prepaid funding, the existing strict SDK,
# the existing egress and the existing recovery. A write-only template's
# origin-only batch (`BatchLedger.commit_origin`, #912) is not re-materialized here;
# a consumer stages it as an input instead.


#: Why an origin identity was re-pinned; the only reason there is.
#: Content checks that refused, by ``(path, identity, sha256)``.  A file
#: whose identity has not moved since its content was refused is refused
#: again without a read, so a retirement tick does not re-read a changed
#: origin every cycle.  Only refusals are kept: a changed origin is an
#: anomaly an operator settles, so this grows with those, not with the work.


# --------------------------------------------------------------------------
# Admission credit (bound record, NOT ledger tokens) + per-batch prewrite
# --------------------------------------------------------------------------




def admit_funded_window(queue, instance: Mapping[str, object],
                        template: Mapping[str, object], *,
                        need_gib_per_tier: Mapping[str, int]) -> dict[str, object]:
    """General funded-window admission gate (liveness-owned primitive).

    Fully validates the window request (bound instance + template match,
    permitted tiers, positive-integer needs, need within window demand and
    within minted tier capacity). With the funded-claim primitive family
    delivered (the prepaid-output lane: ``FundingLedger.reserve_window``,
    ``ResourceLedger.transfer_tokens`` and ``FundingLedger.cover_window``), this is the
    supported window binding: the producer action reserves its window ONCE
    as its own tier demand (``owner_demand_terms`` at submit), and every
    batch funds from those holdings by exact transfer -- never a second
    acquisition from free. Returns the binding the producer driver and the
    PQ integrator seal against; a pool missing any primitive still answers
    the pending refusal.
    """

    from prismabuild import pool as pool_mod
    from prismabuild import storage_tiers as tiers_mod

    checked_template = oc.validate_template(template)
    try:
        checked_instance = oc.validate_instance(instance)
    except oc.ProducedOutputError as exc:
        return {"ok": False, "refusal": f"bad-instance: {exc}"}
    if checked_instance["template_sha256"] != oc.template_sha256(checked_template):
        return {"ok": False, "refusal": "template-mismatch"}
    if checked_template.get("write_only"):
        # A write-only template has no window to fund (#912).
        return {"ok": False, "refusal": "template-is-write-only"}
    if not isinstance(need_gib_per_tier, Mapping) or not need_gib_per_tier:
        return {"ok": False, "refusal": "bad-window-need"}
    for tier, need in need_gib_per_tier.items():
        if tier not in checked_template["permitted_tiers"]:
            return {"ok": False, "refusal": "tier-not-permitted", "tier_id": tier}
        if type(need) is not int or need <= 0:
            return {"ok": False, "refusal": "bad-window-need", "tier_id": tier}
        window = int(checked_template["working_demands"][tier]["window_gib"])
        if need > window:
            return {"ok": False, "refusal": "window-need-exceeds-demand",
                    "tier_id": tier}
        kind = tiers_mod.capacity_kind_of(tier)
        try:
            capacity = queue.tier(tier).mint_supply().get(kind, 0)
        except Exception as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if need > capacity:
            return {"ok": False, "refusal": "never-fits-tier-capacity",
                    "tier_id": tier}
    from .funding import FundingLedger

    delivered = [label for label, method in (
        ("reserve_fence", FundingLedger.reserve_window),
        ("funded_cover", FundingLedger.cover_window)) if callable(method)]
    if callable(getattr(pool_mod.ResourceLedger, "transfer_tokens", None)):
        delivered.append("transfer_tokens")
    if len(delivered) == 3:
        # CAPABILITY/WINDOW DECLARATION ONLY (prepaid-output lane): this
        # reports that the funded-claim primitive family exists and what
        # the producer must declare -- the owner action's own tier demand
        # (reserved once at its claim). It does NOT verify that any owner
        # physically holds window tokens right now; the authority is the
        # per-batch funding itself (FundingLedger.stage_output ->
        # publish_prepaid_batch/FundingLedger.fund_output -> commit_batch), which
        # moves exact names owner->mover and never re-acquires from free.
        return {"ok": True, "mode": "prepaid-per-batch",
                "declaration_only": True,
                "authority": "per-batch-funding",
                "owner_action_key": str(checked_instance["owner_action_key"]),
                "owner_demand_terms": oc.owner_demand_terms(checked_template),
                "window_gib": {tier: int(
                    checked_template["working_demands"][tier]["window_gib"])
                    for tier in checked_template["permitted_tiers"]},
                "batch_ref_schema": pool_mod.PRODUCED_OUTPUT_BATCH_REF_SCHEMA_V1,
                "sequence": ["require_prewrite", "publish_prepaid_batch",
                             "fleet-claims-the-mover", "retire_batch",
                             "reclaim_origin", "safe_release_instance"]}
    return {"ok": False, "refusal": "funding-primitive-pending",
            "liveness_draft_present": delivered,
            "dependency": ("liveness funded-claim primitive: funding record "
                           "binding credit to exact tier/plan-window/mover/"
                           "range/generation + eligible-token verification + "
                           "serialized transfer + current+next window "
                           "admission")}


def commit_batch(queue, instance: Mapping[str, object],
                 template: Mapping[str, object],
                 descriptors: list[Mapping[str, object]], *, batch_id: str,
                 tier: str, mover_key: str) -> dict[str, object]:
    """Commit one immutable batch: check, prewrite-match, acquire exact under
    the batch holder, TRANSFER whole to mover ownership (no free interval),
    file the batch record. All-or-nothing with typed refusals."""

    from prismabuild import pool as pool_mod
    from prismabuild import storage_tiers as tiers_mod

    try:
        checked_template, checked_instance = oc.bound_output_contract(
            template, instance)
    except oc.ProducedOutputError:
        return {"ok": False, "refusal": "template-mismatch"}
    oc.output_name(batch_id, where="batch_id")
    if checked_template.get("write_only"):
        # A write-only template's batches commit at origin and are never
        # staged by their owner (#912); this staged path is not theirs.
        return {"ok": False, "refusal": "template-is-write-only"}
    mover = oc.output_key(mover_key, where="batch mover_key")
    if tier not in checked_template["permitted_tiers"]:
        return {"ok": False, "refusal": "tier-not-permitted"}
    sealed = [oc.validate_descriptor(d, checked_template, checked_instance)
              for d in descriptors]
    class_bytes = {"payload": 0, "checkpoint": 0, "temp": 0}
    for desc in sealed:
        class_bytes[str(desc["artifact_class"])] += int(desc["bytes"])
    manifest_digest = oc.output_manifest_sha256(sealed)
    batch_ns = oc.batch_namespace(checked_instance, batch_id, manifest_digest)
    kind = tiers_mod.capacity_kind_of(tier)
    batch_total = sum(class_bytes.values())
    batch_gib = tiers_mod.stage_tokens_for_bytes(batch_total)
    window = int(checked_template["working_demands"][tier]["window_gib"])
    if batch_gib > window:
        return {"ok": False, "refusal": "batch-exceeds-window",
                "batch_gib": batch_gib, "window_gib": window}
    try:
        # This prefix's lock and every overlapping template's (#1063).
        locks = bl.path_locks(queue, checked_instance)
    except oc.ProducedOutputError as exc:
        return {"ok": False, "refusal": str(exc)}
    with locks:
        # One lstat per origin, answering both questions at the same instant:
        # the size the descriptor claims, and the exact file identity this
        # commit is over. No payload reread -- digests ride the writer receipt
        # and the mover verifies on copy. The identity tuple is what lets the
        # SAME batch be materialized again later over provably the same bytes
        # (see `ensure_batch_materialized`): for a DEV null-digest descriptor
        # it is the only such proof, and a size check alone would bless a
        # rewritten file of equal length. Taken under these locks (#1064): a
        # retirement's delete, which holds one of them whatever its
        # template's prefix (#1063), can move an origin aside and link it
        # back (`_unlink_if_committed`), which moves its ctime, so an
        # identity taken before the locks could be stale before it was filed.
        origin_identity, identity_refusal = bl.origin_identity_at_commit(sealed)
        if identity_refusal is not None:
            return identity_refusal
        assert origin_identity is not None
        try:
            commitments = bl.BatchLedger.for_instance(queue.root, checked_instance).entries()
        except oc.ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        batches = commitments
        assert isinstance(batches, bl.BatchCapture)
        if batch_id in batches:
            existing = batches[batch_id]
            if (isinstance(existing, bl.BatchFacets) and existing.manifest == manifest_digest):
                return {"ok": True, "batch_id": batch_id, "duplicate": True,
                        "batch_namespace": batch_ns,
                        "manifest_digest": manifest_digest}
            return {"ok": False, "refusal": "batch-id-in-use"}
        gated = bl.require_live_owner(queue, checked_instance)
        if gated is not None:
            return gated
        try:
            prewrite = bl.BatchLedger.for_instance(queue.root, checked_instance).prewrite(batch_id)
        except oc.ProducedOutputError as exc:
            return {"ok": False, "refusal": f"prewrite-unreadable: {exc}"}
        if prewrite is None:
            return {"ok": False, "refusal": "prewrite-reservation-missing"}
        # Ceiling reconciliation (conservative prewrite -> actual): same
        # binding fields; actual paths subset of the planned superset;
        # actual per-class bytes at or under the admitted ceiling. The
        # prewrite is consumed here either way, and the class maxima were
        # charged against the (larger) ceiling while it lived.
        if not prewrite.matches_commit(checked_instance, tier, sealed):
            return {"ok": False, "refusal": "prewrite-mismatch"}
        if not prewrite.omitted_absent(sealed):
            return {"ok": False, "refusal": "planned-path-present-retain"}
        try:
            sums = batches.charges
        except oc.ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        maxima = oc.checked_instance_maxima(checked_template)
        for cls in sums:
            if sums[cls] + class_bytes[cls] > maxima[f"{cls}_max_bytes"]:
                return {"ok": False, "refusal": f"commit-exceeds-{cls}-maxima"}
        # Prepaid lane (R7 integration): one authoritative pool funding
        # record at (mover, tier). A batch that staged its intent funds by
        # exact transfer from the producer's already-reserved window
        # (stage_output_intent -> publish -> fund_output_batch); commit
        # reconciles that record and files the batch with NO second
        # acquisition from free. The SDK6 direct path also preserves first
        # acquisition under the batch holder; an existing per-batch intent
        # repairs allocated tokens without a second take. Unknown pool
        # funding never downgrades to that path (that would double-fund).
        funding = queue.funding.commit_stage(
            checked_instance, checked_template, batch_id=batch_id,
            manifest_digest=manifest_digest, batch_ns=batch_ns,
            tier=tier, mover=mover, kind=kind,
            batch_total=batch_total, batch_gib=batch_gib)
        if funding.refusal is not None:
            return funding.refusal
        funding_path, prepaid = funding.path, funding.prepaid
        refused = batches.publish_stage(
            queue, checked_template, checked_instance, sealed,
            batch_id=batch_id, batch_ns=batch_ns,
            manifest_digest=manifest_digest, tier=tier, mover=mover,
            class_bytes=class_bytes, batch_total=batch_total,
            origin_identity=origin_identity,
            funding_path=funding_path)
        if refused is not None:
            return refused
    return {"ok": True, "batch_id": batch_id, "batch_namespace": batch_ns,
            "manifest_digest": manifest_digest, "class_bytes": class_bytes,
            "mover_key": mover, "tier": tier, "entries": sealed,
            "funding": "prepaid" if prepaid is not None else "legacy"}


def _producer_launch_context(queue, producer: str) -> dict[str, object]:
    """The producer's FILED launch context, for a mover row of its own.

    The mover row must be launched exactly like the producer action is:
    `worker_script` is the PB worker launcher (whose run-local verb takes the
    request/cas/checkout arguments Pool.execute builds), NEVER the stage tool
    itself (stage_move is the action PAYLOAD that launcher executes), and the
    row's checkout addressing is reused so the launcher materializes the same
    tree the producer ran from. An agent's validation priority stays with the
    row. Returns `{"ok": True, "worker_script", "addressing", "priority"}` or a
    typed refusal; one spelling, shared by first publication and restage, so
    the two can never drift.
    """

    from prismabuild import pool as pool_mod

    try:
        observed = RecordStore(queue.root).claimed(producer)
        if isinstance(observed, Unknown):
            assert observed.error is not None
            raise observed.error
        producer_row = None if isinstance(observed, Absent) else observed.value
    except (OSError, pool_mod.PoolContractError) as exc:
        return {"ok": False, "step": "launch-context",
                "refusal": f"producer-claim-unreadable: {exc}"}
    if not isinstance(producer_row, Mapping):
        return {"ok": False, "step": "launch-context",
                "refusal": "producer-not-claimed: the mover must be "
                           "published from inside the admitted producer"}
    worker_script = str(producer_row.get("worker_script") or "")
    if not worker_script.startswith("/"):
        return {"ok": False, "step": "launch-context",
                "refusal": "producer-launch-context-required: the producer "
                           "row names no absolute worker script"}
    addressing: dict[str, object] = {}
    if isinstance(producer_row.get("checkout_snapshot"), Mapping):
        addressing["checkout_snapshot"] = producer_row["checkout_snapshot"]
    elif str(producer_row.get("checkout_root") or "").startswith("/"):
        addressing["checkout_root"] = str(producer_row["checkout_root"])
    else:
        return {"ok": False, "step": "launch-context",
                "refusal": "producer-launch-context-required: the producer "
                           "row names no checkout addressing"}
    # The row's own CAS root, then the queue-sibling default: the same
    # precedence `stage_release` resolves a claim's CAS root with.
    row_cas = producer_row.get("cas_root")
    cas_root = (str(row_cas) if isinstance(row_cas, str) and row_cas
                else str(Path(queue.root).parent / "cas"))
    return {"ok": True, "worker_script": worker_script,
            "addressing": addressing,
            "priority": int(producer_row.get("priority") or 0),
            "cas_root": cas_root}


def _read_producer_request(cas_root, producer: str):
    """The producer's own sealed request, through the existing CAS interface.

    Returns `(cas, request)`, or a typed `{"ok": False, "step", ...}` refusal.
    The runtime parent context every node of this lane inherits: the output
    mover and the tier-host egress both seal off it.
    """

    from prismabuild import core as core_mod

    if len(producer) != 64:
        return {"ok": False, "step": "parent-request",
                "refusal": "producer-request-required: pass "
                           "producer_action_key or run under a launcher "
                           "that sets PRISMABUILD_ACTION_KEY"}
    request_path = None
    try:
        cas = core_mod.PrismaBuildCAS(cas_root)
        request_path = (Path(cas.root) / "requests" / producer[:2]
                        / f"{producer}.json")
        with open(request_path, "rb") as handle:
            raw = handle.read(8 * 1024 * 1024 + 1)
        if len(raw) > 8 * 1024 * 1024:
            raise oc.ProducedOutputError("producer request oversize")
        request = core_mod.validate_action(
            core_mod.digest_primitives.decode_strict_json(raw, where="producer action request"),
        )
        if str(request.get("action_key")) != producer:
            raise oc.ProducedOutputError("producer request key mismatch")
    except FileNotFoundError:
        return {"ok": False, "step": "parent-request",
                "refusal": f"producer-request-missing: {request_path}"}
    except Exception as exc:
        return {"ok": False, "step": "parent-request",
                "refusal": f"producer-request-unreadable: {exc}"}
    return cas, request


def _producer_movement_template(queue, request: Mapping[str, object],
                                producer: str, *,
                                extra_inputs: Sequence[Mapping[str, object]] = ()
                                ) -> dict[str, object]:
    """The movement template a node of this lane seals off its producer.

    The producer's task, code closure, environment and execution scope, its
    inputs minus its own data manifest, and its OWN sealed checkout addressing
    carried into the child. `extra_inputs` follow the inherited ones: the
    output mover appends the batch's data manifest, an egress appends nothing.
    Returns `{"ok": True, "template"}` or a typed refusal.
    """

    from prismabuild import core as core_mod
    from . import data_manifest as manifest_codec
    from prismabuild import pool as pool_mod

    parent_inputs = [dict(entry) for entry in request.get("inputs") or ()
                     if isinstance(entry, Mapping)]
    child_inputs = [entry for entry in parent_inputs
                    if str(entry.get("id"))
                    != manifest_codec.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID]
    # The producer's OWN sealed checkout addressing, carried into the child.
    # The row already materializes the producer's snapshot; a sealed request
    # that does not say so is proved against the producer's pre-snapshot
    # closure stamp, which the materialized tree can never satisfy. Absent on
    # a legitimate non-snapshot producer: the legacy stamp proof and the
    # historical ownership digest are unchanged there.
    request_params = request.get("params")
    if not isinstance(request_params, Mapping):
        request_params = {}
    params: dict[str, object] = {
        "cwd": str(request_params.get("cwd") or ".")}
    raw_snapshot = request_params.get("checkout_snapshot")
    snapshot_sha256 = ""
    if raw_snapshot is not None:
        try:
            snapshot = core_mod.validate_pbrun_checkout_snapshot(raw_snapshot)
        except core_mod.ActionContractError as exc:
            return {"ok": False, "step": "parent-request",
                    "refusal": f"producer-checkout-snapshot-invalid: {exc}"}
        snapshot_input = snapshot["input"]
        assert isinstance(snapshot_input, Mapping)
        if snapshot_input not in child_inputs:
            return {"ok": False, "step": "parent-request",
                    "refusal": "producer-checkout-snapshot-input-missing: the "
                               "producer's sealed snapshot input is not among "
                               "the inputs the mover inherits"}
        params["checkout_snapshot"] = snapshot
        snapshot_sha256 = str(snapshot_input["sha256"])
    if not snapshot_sha256:
        # The ownership namespace keeps the historical digest over the first
        # inherited input, else the producer's own key.
        snapshot_sha256 = next((str(entry.get("sha256"))
                                for entry in child_inputs
                                if entry.get("sha256")), producer)
    return {"ok": True, "template": {
        "task": dict(request["task"]),
        "inputs": child_inputs + [dict(entry) for entry in extra_inputs],
        "code_closure": request["code_closure"],
        "environment": request["environment"],
        "execution_scope": request["execution_scope"],
        "params": params,
        "marker_root": Path(queue.root) / pool_mod.CONTAINER_OWNERS,
        "checkout_identity": {"checkout_snapshot": snapshot_sha256},
    }}


def _announced_tier_record(queue, tier: str) -> Mapping[str, object] | None:
    """The tier's announced record (`tier_loop.py` writes it), or None."""

    for candidate in queue.tiers():
        if isinstance(candidate, Mapping) and str(
                candidate.get("tier_id")) == tier:
            return candidate
    return None


def _sealed_restage_fill_setting(request: Mapping[str, object]) -> bool | None:
    """The producer's sealed `RESTAGE_FILL_ENV`: False, True, or None if bad.

    Absent, "" and "0" are off; "1" is on. Any other value is a sealing
    mistake that no restage should guess at, so it answers None and the
    caller refuses before anything is filed.
    """

    environment = request.get("environment")
    variables = (environment.get("variables")
                 if isinstance(environment, Mapping) else None)
    value = (variables.get(oc.RESTAGE_FILL_ENV, "")
             if isinstance(variables, Mapping) else "")
    if value in ("", "0"):
        return False
    if value == "1":
        return True
    return None


def _seal_output_mover(queue, checked_instance: Mapping[str, object],
                       checked_template: Mapping[str, object],
                       descriptors: list[Mapping[str, object]], *,
                       batch_id: str, tier: str, cas_root,
                       producer_action_key: str | None,
                       command_extra: Sequence[str] = (),
                       retry_policy: Mapping[str, object] | None = None,
                       generation: int = 0,
                       restage_fill: bool | None = None) -> dict[str, object]:
    """Seal and file ONE output mover for this batch, and answer its facts.

    The single sealing path for the produced-output lane: first publication
    (`generation == 0`) and every later re-materialization
    (`generation >= 1`, `ensure_batch_materialized`) construct the mover the
    SAME way -- movement template recovered from the producer's own sealed
    request through the existing CAS request interface, batch data manifest
    sealed as that request's input, interpreter/tools/stage-root/placement off
    the ANNOUNCED TIER RECORD, the ordinary paced `stage_move` command behind
    `movement_actions.seal_movement_action`. Nothing here is a second sealing
    scheme and nothing is copied from `publish`.

    A re-materialization differs from first publication in exactly two sealed
    fields, both derived from the PB-sealed materialization sequence and
    neither chosen by a caller: the log name carries the generation, and the
    params carry the `produced_output_materialization` record. That is what
    makes the successor's content-addressed action key -- which IS its funding
    key -- unique per generation, deterministic on replay, and impossible to
    supply as a nonce. Every generation seals the same physical batch namespace
    under the registered tier root. The child's
    `params.checkout_snapshot` is the producer's validated sealed record when
    the producer has one, and absent when it does not.

    The producer's sealed checkout snapshot rides the child exactly as
    ``movement_actions.seal_movement_action`` already carries it for pbrun's
    own movement nodes: the row the mover is published under materializes that
    snapshot (``_producer_launch_context``), so the sealed request has to say
    which tree that is. A child that inherited the materialized snapshot
    without inheriting the record was proved against the producer's
    pre-snapshot closure stamp instead, which no materialized snapshot can
    satisfy; the worker refused the row in ~0.8 s before any byte moved
    (2026-09-21 Stage A live cycle). A malformed record, or one whose declared
    input the child does not inherit, is refused here rather than sealed for a
    worker to reject.

    A re-materialization may also reserve pool fill (#747), and only when the
    producer opted in: `restage_fill` when the caller passes it, else the
    producer's sealed `RESTAGE_FILL_ENV`. A restage copies a retired batch
    back off the pool, cold, the way a consumer's input mover does, so it is
    priced the way pbrun prices one: `storage_tiers.current_fill_offer` over
    the movement receipts and the tier's current offer. The demand gains the
    `fill_mb_s_pool_side@<tier>` term and the command the matching
    `--fill-mb-s-pool-side`, and the price is returned as `fill` so the
    materialization row can carry it for a resume. A first publication never
    reserves fill: it reads what the producer has just written. A tier that
    offers no fill and has no usable receipt prices nothing, and the mover is
    sealed unreserved exactly as before.

    Returns the sealed facts (`mover_key`, `action`, `host`, `kind`, `gib`,
    `fill`, `total`, `manifest_digest`, `batch_namespace`, `retry_policy`,
    `cas`) with the request already filed in CAS, or a typed
    `{"ok": False, "step", ...}`.
    """

    from prismabuild import core as core_mod
    from . import data_manifest as manifest_codec
    from prismabuild import movement_actions
    from prismabuild import pool as pool_mod
    from prismabuild import storage_tiers as tiers_mod

    try:
        ref = pool_mod.PoolQueue.build_produced_output_batch_ref(
            instance=checked_instance, template=checked_template,
            batch_id=batch_id, descriptors=list(descriptors), tier_id=tier)
    except pool_mod.PoolContractError as exc:
        return {"ok": False, "step": "build-ref", "refusal": str(exc)}
    manifest_digest = str(ref["manifest_digest"])
    total = int(ref["range_end_bytes"])
    batch_ns = str(ref["batch_namespace"])
    # The producer's own sealed request, through the existing request
    # interface: this is the runtime parent context the mover inherits.
    producer = str(producer_action_key or
                   os.environ.get(core_mod.ACTION_KEY_ENV) or "")
    parent = _read_producer_request(cas_root, producer)
    if isinstance(parent, Mapping):
        return dict(parent)
    cas, request = parent
    reserve_fill = False
    if int(generation) > 0:
        setting = _sealed_restage_fill_setting(request)
        if setting is None:
            return {"ok": False, "step": "seal",
                    "refusal": (f"{oc.RESTAGE_FILL_ENV} in the producer's sealed "
                                f"environment must be 0 or 1")}
        reserve_fill = setting if restage_fill is None else bool(restage_fill)
    # The batch's stage data manifest, sealed as the request's data-manifest
    # input exactly as a consumer submission's is: the mover finds it in its
    # own sealed request, never on a caller's filesystem. The parent's own
    # data-manifest input is dropped from the child's inputs so the child
    # carries exactly one -- the batch's.
    batch_view = {"entries": [dict(d) for d in descriptors],
                  "batch_id": batch_id, "manifest_digest": manifest_digest}
    mount_prefix = str(os.path.commonpath(
        [os.path.dirname(str(d["path"])) for d in descriptors]))
    try:
        manifest_body = build_stage_manifest(batch_view, mount_prefix)
        handle, manifest_tmp = tempfile.mkstemp(
            prefix="produced-output-manifest-", suffix=".json")
        with os.fdopen(handle, "w") as stream:
            json.dump(manifest_body, stream, sort_keys=True)
        try:
            manifest_codec.load_data_manifest(manifest_tmp)
            manifest_input, _ = cas.ingest_input(
                manifest_tmp,
                input_id=manifest_codec.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
        finally:
            os.unlink(manifest_tmp)
    except Exception as exc:
        return {"ok": False, "step": "manifest", "refusal": str(exc)}
    templated = _producer_movement_template(
        queue, request, producer, extra_inputs=[manifest_input])
    if not templated.get("ok"):
        return templated
    mover_template = templated["template"]
    # Movement-node resolution off the TIER RECORD (the ordinary path):
    # interpreter/tools/mountpoint/host are facts about the box that runs
    # the movers, announced beside the tier by tier_loop.py.
    record = _announced_tier_record(queue, tier)
    if record is None:
        return {"ok": False, "step": "resolve",
                "refusal": f"tier-not-announced: {tier}"}
    try:
        mover_python, mover_tool, _egress_tool = (
            movement_actions.movement_tools(record))
    except SystemExit as exc:
        return {"ok": False, "step": "resolve", "refusal": str(exc)}
    stage_root = str(record.get("mountpoint") or "")
    if not stage_root.startswith("/"):
        return {"ok": False, "step": "resolve",
                "refusal": (f"stage tier {tier} announces no mountpoint "
                            f"to write into")}
    host = str(record.get("host") or "")
    kind = tiers_mod.capacity_kind_of(tier)
    gib = tiers_mod.stage_tokens_for_bytes(total)
    demand: dict[str, int] = {"cpu": 1, "mem_gb": 1, f"{kind}@{tier}": gib}
    fill: int | None = None
    fill_basis: str | None = None
    if reserve_fill:
        # pbrun's mover rule, unchanged: one copy's measured rate -- this
        # batch's slowest landing in its latest window, else the median
        # single-reader share (#909) -- capped by what the tier offers on this
        # cycle (#708). Receipts from another pool behind the same tier id
        # price nothing (#611).
        identity = record.get("pool_identity")
        measured = tiers_mod.mover_fill_demand_from_receipts(
            queue.move_records(), tier_id=tier,
            pool_identity=identity if isinstance(identity, Mapping) else None,
            manifest_sha256=manifest_digest)
        priced, _offer, fill_basis = tiers_mod.current_fill_offer(
            record, measured)
        if priced is not None and int(priced) > 0:
            fill = int(priced)
            demand[f"{tiers_mod.FILL_KIND}"
                   f"{tiers_mod.TIER_DEMAND_SEPARATOR}{tier}"] = fill
    command = [mover_python, mover_tool,
               "--pool-root", str(queue.root),
               "--cas-root", str(cas.root),
               "--consumer-action-key", batch_ns,
               "--produced-output-namespace", batch_ns,
               "--tier-id", tier,
               "--stage-root", stage_root,
               "--manifest-sha256", manifest_digest,
               "--range-start-bytes", "0",
               "--range-end-bytes", str(total),
               # Output-batch fragments file under the produced-output
               # fragment root, not the tier's default residency root.
               "--residency-root", str(bl.output_fragment_root(
                   Path(queue.root) / pool_mod.record_store.RESIDENCY)),
               "--readers", "2"]
    if fill is not None:
        # Receipt only: the mover records what its claim reserved, so a
        # later tier cycle can ask whether the pool delivered it.
        command += ["--fill-mb-s-pool-side", str(fill)]
    command += [str(flag) for flag in command_extra]
    mover_retry_policy = (dict(retry_policy) if retry_policy is not None
                          else {"max_attempts": 3, "retry_safe": True})
    extra_params: dict[str, object] = {
        "produced_output_batch": dict(ref),
        "data_manifest": {"input": manifest_input},
    }
    log_name = f"produced-output-mover-{batch_id}.log"
    if int(generation) > 0:
        log_name = (f"produced-output-mover-{batch_id}"
                    f"-m{int(generation)}.log")
        extra_params["produced_output_materialization"] = {
            "schema": oc.MATERIALIZATION_SCHEMA_V1,
            "batch_id": batch_id,
            "manifest_digest": manifest_digest,
            "batch_namespace": batch_ns,
            "tier_id": tier,
            "generation": int(generation),
        }
    try:
        action = movement_actions.seal_movement_action(
            mover_template,
            command=command,
            demand=demand,
            tags=[host] if host else [],
            log_name=log_name,
            retry_policy=mover_retry_policy,
            extra_params=extra_params)
        cas.publish_action_request(action)
    except SystemExit as exc:
        return {"ok": False, "step": "seal", "refusal": str(exc)}
    except Exception as exc:
        return {"ok": False, "step": "seal", "refusal": str(exc)}
    return {"ok": True, "action": action,
            "mover_key": str(action["action_key"]),
            "cas": cas, "cas_root": str(cas.root), "host": host,
            "kind": kind, "gib": gib, "fill": fill, "fill_basis": fill_basis,
            "total": total,
            "manifest_digest": manifest_digest, "batch_namespace": batch_ns,
            "retry_policy": mover_retry_policy, "ref": dict(ref)}


def publish_prepaid_batch(queue, instance: Mapping[str, object],
                          template: Mapping[str, object],
                          descriptors: list[Mapping[str, object]], *,
                          batch_id: str, tier: str, cas_root,
                          producer_action_key: str | None = None,
                          command_extra: Sequence[str] = (),
                          retry_policy: Mapping[str, object] | None = None,
                          ) -> dict[str, object]:
    """The operational prepaid writer path for one finished batch (R7).

    Callable INSIDE the admitted producer action. The movement action is
    sealed by `_seal_output_mover` (the shared first-publisher construction:
    the movement template is RECOVERED from the producer's own sealed request
    through the existing CAS request interface, never from submitter-local
    state), and the launch context comes from the producer's FILED claimed row
    through `_producer_launch_context`.

    Sequence, all existing mechanisms: seal + file the mover request ->
    stage the funding intent (reserved, no tokens moved) -> publish the mover
    READY row -> fund by exact transfer of the producer's existing window
    (``fund_output_batch``) -> ``commit_batch`` (files the immutable batch
    against the pool record; no second acquisition from free). The fleet's
    ordinary claim then admits the mover through the prepaid cover, and the
    worker executes the sealed argv on the storage owner.

    ``command_extra`` exists ONLY so a fixture can append explicit flags such
    as ``--unpaced`` (no ZFS pacer in a sandbox) -- production passes nothing.
    ``retry_policy`` defaults to the ordinary mover policy (bounded attempts,
    retry-safe copy).

    The mover key is the content-addressed action key of the sealed request:
    retrying with identical inputs re-derives the same key and every step is
    idempotent (stage/publish/fund/commit duplicates are typed successes; a
    fully committed batch short-circuits to the duplicate), so a restart re-
    calls this method. Refusals return the failing step's typed result under
    ``step``/``refusal``. Re-STAGING an already retired batch is a different
    transition and is NOT this function: see `ensure_batch_materialized`.
    """

    try:
        checked_template = oc.validate_template(template)
        checked_instance = oc.validate_instance(instance)
    except oc.ProducedOutputError as exc:
        return {"ok": False, "step": "validate", "refusal": str(exc)}
    oc.output_name(batch_id, where="batch_id")
    if checked_template.get("write_only"):
        # A write-only template's batches commit at origin and are never
        # staged by their owner (#912); this staged path is not theirs.
        return {"ok": False, "step": "validate",
                "refusal": "template-is-write-only"}
    if tier not in checked_template["permitted_tiers"]:
        return {"ok": False, "step": "validate", "refusal": "tier-not-permitted"}
    try:
        manifest_digest = oc.output_manifest_sha256(
            [oc.validate_descriptor(dict(d), checked_template, checked_instance)
             for d in descriptors])
    except oc.ProducedOutputError as exc:
        return {"ok": False, "step": "build-ref", "refusal": str(exc)}
    # Retry after FULL success: the prewrite and the pool-side staging are
    # consumed by design, so creation steps would refuse exactly what they
    # finished. A filed commitments entry with the same manifest means the
    # batch is complete -- replay the idempotent commit and answer the
    # duplicate.
    try:
        commitments = bl.BatchLedger.for_instance(queue.root, checked_instance).entries()
    except oc.ProducedOutputError as exc:
        return {"ok": False, "step": "validate",
                "refusal": f"unknown-retain: {exc}"}
    existing = commitments.lookup(batch_id)
    if isinstance(existing, bl.BatchFacets):
        if existing.kind == 'origin':
            # A read-back batch its owner committed at its origin (#1034) has
            # no mover to replay; nothing stages it.
            return {"ok": False, "step": "validate",
                    "refusal": "batch-committed-at-origin"}
        if str(existing.manifest) != manifest_digest:
            return {"ok": False, "step": "validate",
                    "refusal": "batch-id-in-use"}
        mover = str(existing.first_mover)
        committed = commit_batch(queue, checked_instance, checked_template,
                                 descriptors, batch_id=batch_id, tier=tier,
                                 mover_key=mover)
        if not committed.get("ok"):
            committed["step"] = "commit"
            return committed
        committed["mover_key"] = mover
        record = queue.funding.read_output(mover, tier)
        committed["generation"] = (str(record.get("generation"))
                                   if record is not None else "")
        committed["tokens"] = ([str(name) for name in record.get("tokens") or []]
                               if record is not None else [])
        committed["funding"] = "prepaid"
        committed["duplicate"] = True
        return committed
    sealed_mover = _seal_output_mover(
        queue, checked_instance, checked_template, list(descriptors),
        batch_id=batch_id, tier=tier, cas_root=cas_root,
        producer_action_key=producer_action_key,
        command_extra=command_extra, retry_policy=retry_policy,
        generation=0)
    if not sealed_mover.get("ok"):
        return sealed_mover
    launch = _producer_launch_context(
        queue, str(producer_action_key or
                   os.environ.get(_ACTION_KEY_ENV_NAME()) or ""))
    if not launch.get("ok"):
        return launch
    mover = str(sealed_mover["mover_key"])
    owner = str(checked_instance["owner_action_key"])
    kind = str(sealed_mover["kind"])
    gib = int(sealed_mover["gib"])
    total = int(sealed_mover["total"])
    host = str(sealed_mover["host"])
    mover_retry_policy = dict(sealed_mover["retry_policy"])
    staged = queue.funding.stage_output(
        tier_id=tier, owner_key=owner, mover_key=mover,
        instance=checked_instance, template=checked_template,
        batch_id=batch_id, descriptors=descriptors)
    if not staged.get("ok"):
        staged["step"] = "stage"
        return staged
    published = _publish_output_mover_row(
        queue, mover_key=mover, cas_root=str(sealed_mover["cas_root"]),
        launch=launch, host=host, tier=tier, kind=kind, gib=gib,
        manifest_digest=manifest_digest, total=total,
        retry_policy=mover_retry_policy)
    if not published.get("ok"):
        return published
    funded = queue.funding.fund_output(
        tier_id=tier, owner_key=owner, mover_key=mover,
        instance=checked_instance, template=checked_template,
        batch_id=batch_id, descriptors=descriptors)
    if not funded.get("ok"):
        funded["step"] = "fund"
        return funded
    committed = commit_batch(queue, checked_instance, checked_template,
                             descriptors, batch_id=batch_id, tier=tier,
                             mover_key=mover)
    if not committed.get("ok"):
        committed["step"] = "commit"
        return committed
    committed["mover_key"] = mover
    committed["generation"] = str(funded.get("generation"))
    committed["tokens"] = list(funded.get("tokens") or [])
    committed["funding"] = "prepaid"
    return committed


def _ACTION_KEY_ENV_NAME() -> str:
    from prismabuild import core as core_mod

    return core_mod.ACTION_KEY_ENV


def _publish_output_mover_row(queue, *, mover_key: str, cas_root: str,
                              launch: Mapping[str, object], host: str,
                              tier: str, kind: str, gib: int,
                              manifest_digest: str, total: int,
                              retry_policy: Mapping[str, object],
                              fill: int | None = None
                              ) -> dict[str, object]:
    """Publish one output mover's READY row through the EXISTING channel.

    Ordinary `queue.publish` semantics carried from the parent: the producer's
    worker script and checkout addressing, its validation priority, the
    effective sealed mover retry policy, the tier's placement tag, qualified
    tier demand, and the mover-variant residency block. One spelling for first
    publication and restage. A row already published for this content-
    addressed key is a typed duplicate, not a conflict: the sealed key IS the
    identity, so republishing the same key is the resume path, never a second
    unit of work. `fill` is the pool fill the sealed request reserved, if
    any: the row's resources must equal its sealed demand, or the launch
    refuses it.
    """

    from prismabuild import pool as pool_mod
    from prismabuild import storage_tiers as tiers_mod

    resources: dict[str, int] = {"cpu": 1, "mem_gb": 1, f"{kind}@{tier}": gib}
    if fill is not None:
        resources[f"{tiers_mod.FILL_KIND}"
                  f"{tiers_mod.TIER_DEMAND_SEPARATOR}{tier}"] = int(fill)
    mover = RecordStore(queue.root).mover(mover_key)
    if isinstance(mover, Unknown):
        return {"ok": False, "step": "publish",
                "refusal": "unknown-retain: mover-row-unreadable"}
    if isinstance(mover, Present):
        return {"ok": True, "published": False, "state": mover.value.state}
    try:
        queue.publish(
            action_key=mover_key, cas_root=str(cas_root),
            worker_script=str(launch["worker_script"]),
            tags=[host] if host else (),
            priority=int(launch["priority"]),
            max_attempts=int(retry_policy["max_attempts"]),
            retry_safe=bool(retry_policy.get("retry_safe", True)),
            **dict(launch["addressing"]),
            resources=resources,
            residency={"schema": pool_mod.RESIDENCY_SCHEMA_V1,
                       "tier_id": tier,
                       "manifest_sha256": manifest_digest,
                       "manifest_bytes": total,
                       "range_start_bytes": 0, "range_end_bytes": total},
            # The same question the state read above asked, asked again under
            # the queue's own transition lock.  The read can be stale on NFS
            # -- that is how the 2026-09-21 cycle republished live rows -- and
            # only the lock makes look-then-publish one decision (#810).
            refuse_if_live=True)
    except pool_mod.ActionAlreadyLiveError as exc:
        # The answer the state read would have given: the row is there.
        return {"ok": True, "published": False, "state": exc.state}
    except pool_mod.PoolContractError as exc:
        return {"ok": False, "step": "publish", "refusal": str(exc)}
    return {"ok": True, "published": True, "state": "ready"}


#: A committed batch's lifecycle as :func:`batch_record` reports it (#955).


def _reconcile_materialization_funding(queue, *, mover: str, tier: str,
                                       kind: str, gib: int
                                       ) -> dict[str, object]:
    """Is this materialization's prepaid funding actually in hand?

    The restage analogue of the reconciliation `commit_batch` performs against
    the pool record: the filed funding must read `transferring` (driving a
    publish-before-fund prefix through the SAME recovery API a restart would
    use, never a second reservation), and the mover must actually hold the
    batch's exact window price. A short transfer leaves the split intact and
    retains -- the drive retries resume it; nothing here re-funds.
    """

    record = queue.funding.read_output(mover, tier)
    if record is None or str(record.get("state")) != "transferring":
        driven = queue.funding.drive_output(mover, tier)
        if not driven.get("ok"):
            return {"ok": False, "step": "reconcile",
                    "refusal": f"prepaid-drive: {driven.get('refusal')}",
                    "drive": driven}
        record = queue.funding.read_output(mover, tier)
    if record is None or str(record.get("state")) != "transferring":
        return {"ok": False, "step": "reconcile",
                "refusal": "prepaid-funding-terminal",
                "state": (str(record.get("state"))
                          if record is not None else "unknown")}
    held = queue.tier(tier).holdings(mover).get(kind, 0)
    if held < gib:
        return {"ok": False, "step": "reconcile", "refusal": "transfer-short",
                "moved": held, "expected": gib}
    return {"ok": True, "record": dict(record), "held": held}


def ensure_batch_materialized(queue, instance: Mapping[str, object],
                              template: Mapping[str, object], *,
                              batch_id: str, cas_root,
                              producer_action_key: str | None = None,
                              command_extra: Sequence[str] = (),
                              retry_policy: Mapping[str, object] | None = None,
                              restage_fill: bool | None = None,
                              ) -> dict[str, object]:
    """Make one ALREADY COMMITTED batch resident on its tier again.

    The bounded-window transition, and the only one this lane adds. A produced
    batch is an immutable logical unit with ONE durable origin charge; the
    stage copy under it is a window the fleet gives back at retirement. When a
    later read needs those bytes again, this re-materializes the SAME batch --
    same owner action and attempt, same logical batch, manifest, descriptors,
    namespace and output prefix, same durable charge -- by publishing a fresh
    mover over the sealed origin files and funding it by exact transfer from
    the producer's existing window, exactly as the first publication did.

    There is no second schema, no parallel cache and no second dispatcher: the
    caller supplies neither origins, nor tokens, nor a successor id. A
    write-only template refuses `template-is-write-only`: its origin-only
    batches (#912) have no window to fund and are staged by the consumer that
    declares them. The descriptors come from the immutable batch record, the
    successor's mover/funding key is the content-addressed key of a request PB
    seals over the filed materialization generation, and the credit is the
    ordinary prepaid transfer.

    States it answers:

    * a live first materialization, or a live successor mid-flight, is
      reported (`state: "live"`) and nothing is republished;
    * a stage-retired batch with its origins intact and provably unchanged
      gets a new materialization (`state: "materializing"`);
    * an origin whose identity no longer matches what the first commit
      recorded refuses `restage-origin-changed` BEFORE any funding, and a
      batch with no such proof refuses `restage-origin-proof-missing` rather
      than blessing whatever bytes are there now. The one exception is an
      origin whose timestamps alone moved and whose descriptor carries its
      sha256 (an NFS delegation recall, #1111): it is hashed with no lock
      held, and when the digest is the committed one the re-pin is filed on
      the entry under the lock, after a re-stat, before the intent;
    * a batch whose origins were reclaimed refuses: its durable charge is
      gone and so are the only bytes that could be copied.

    Crash-safe at every prefix, without a fresh epoch or a duplicated credit:
    the materialization intent is filed under the output-prefix ownership lock
    BEFORE any funding or movement side effect reaches the pool, and a restart
    re-drives that exact row's sealed mover key through the same idempotent
    stage/publish/fund steps. Two concurrent callers collapse onto one
    generation (the lock serializes the append; the loser resumes the winner's
    intent).

    Window credit is NOT invented here: a producer whose window is spent must
    call `refill_window` first, and an unfunded successor reports the ordinary
    `tier-reservation-unavailable` with its intent standing, resumable.

    Pool fill is off by default (#747). With `restage_fill=True`, or with no
    argument and the producer's sealed `RESTAGE_FILL_ENV` set to "1", the
    successor's mover also reserves `fill_mb_s_pool_side@<tier>` at the price
    `_seal_output_mover` derives. That term is not prepaid: the claim takes it
    from the tier's free fill like any mover's, and the stop returns it. A
    resume republishes the price the intent was sealed at, whatever the switch
    or the tier's offer says by then.
    """

    kwargs = dict(batch_id=batch_id, cas_root=cas_root,
                  producer_action_key=producer_action_key,
                  command_extra=command_extra, retry_policy=retry_policy,
                  restage_fill=restage_fill)
    answer = _ensure_batch_materialized(queue, instance, template, **kwargs)
    if answer.get("refusal") != "restage-origin-unverified":
        return answer
    # Only the timestamps of a digest-carrying origin moved: read it here,
    # with no lock held, and let the locked pass re-stat and file the re-pins.
    verdict = bl.verify_origins_unlocked(queue.root, instance, template,
                                       batch_id, where="restage")
    if not verdict.get("ok"):
        return {"ok": False, "step": "origin",
                **{k: v for k, v in verdict.items() if k != "ok"}}
    return _ensure_batch_materialized(queue, instance, template, **kwargs,
                                      verified=verdict["repins"])


def _ensure_batch_materialized(queue, instance: Mapping[str, object],
                               template: Mapping[str, object], *,
                               batch_id: str, cas_root,
                               producer_action_key: str | None = None,
                               command_extra: Sequence[str] = (),
                               retry_policy: Mapping[str, object] | None = None,
                               restage_fill: bool | None = None,
                               verified: Sequence[Mapping[str, object]] | None = None,
                               ) -> dict[str, object]:
    """One pass of `ensure_batch_materialized`; ``verified`` are hashed re-pins."""

    try:
        checked_template, checked_instance = oc.bound_output_contract(
            template, instance)
    except oc.ProducedOutputError:
        return {"ok": False, "step": "validate", "refusal": "template-mismatch"}
    oc.output_name(batch_id, where="batch_id")
    if checked_template.get("write_only"):
        # A write-only template's batches commit at origin and are never
        # staged by their owner (#912); this staged path is not theirs.
        return {"ok": False, "step": "validate",
                "refusal": "template-is-write-only"}
    owner = str(checked_instance["owner_action_key"])
    prefix = str(checked_instance["output_prefix"])

    # ---- Phase 1: INTENT. Holds the output-prefix ownership lock ONLY. No
    # ledger token, no funding record, no queue row is touched here; the one
    # durable effect is the immutable CAS request (inert until a row names it)
    # and the materialization row that makes every later step resumable.
    with queue.stage_ownership_lock(prefix):
        gated = bl.require_live_owner(queue, checked_instance)
        if gated is not None:
            return {**gated, "step": "owner"}
        try:
            commitments = bl.BatchLedger.for_instance(queue.root, checked_instance).entries()
        except oc.ProducedOutputError as exc:
            return {"ok": False, "step": "validate",
                    "refusal": f"unknown-retain: {exc}"}
        batches = commitments
        assert isinstance(batches, bl.BatchCapture)
        entry = batches.lookup(batch_id)
        if not isinstance(entry, bl.BatchFacets):
            return {"ok": False, "step": "validate", "refusal": "unknown-batch"}
        if entry.kind == 'origin':
            # A read-back batch its owner committed at its origin (#1034) is
            # read from the owner's local spool; nothing stages it.
            return {"ok": False, "step": "validate",
                    "refusal": "batch-committed-at-origin"}
        try:
            filed, sealed = entry.open(queue.root, checked_instance, checked_template, batch_id)
            active = entry.active
            mats = entry.history
        except oc.ProducedOutputError as exc:
            return {"ok": False, "step": "validate", "refusal": str(exc)}
        tier = str(filed.get("tier") or "")
        manifest_digest = str(filed.get("manifest_digest") or "")
        total = int(filed.get("total_bytes") or 0)
        if tier not in checked_template["permitted_tiers"]:
            return {"ok": False, "step": "validate",
                    "refusal": "tier-not-permitted"}
        try:
            canonical_ns = oc.batch_namespace(checked_instance, batch_id,
                                           manifest_digest)
        except oc.ProducedOutputError as exc:
            return {"ok": False, "step": "validate",
                    "refusal": f"unknown-retain: {exc}"}
        if (str(entry.namespace or "") != canonical_ns
                or str(filed.get("batch_namespace") or "") != canonical_ns
                or str(entry.first_mover or "") != str(filed.get("mover_key") or "")
                or str(entry.tier or "") != tier):
            return {"ok": False, "step": "validate",
                    "refusal": "unknown-retain: batch-target-mismatch"}
        if not active.retired:
            # Something already owns this batch's material: the first
            # publication, or a successor this call (or another) started.
            # An ACTIVE copy is never replaced -- a pin on it blocks its
            # retirement, and retirement is what makes a successor legal.
            if str(active.source) == 'batch':
                return {
                    "ok": True, "state": "live", "step": "none",
                    "batch_id": batch_id, "tier": tier,
                    "mover_key": str(active.mover_key or ""), "generation": 0,
                    "batch_namespace": canonical_ns,
                    "manifest_digest": manifest_digest,
                    "total_bytes": total, "duplicate": True,
                }
            resume = active
            if str(resume.state) == 'funded':
                # A replay of a materialization that is already funded and
                # published: its credit is transferred and its row is the
                # fleet's to run. Re-driving the pool primitives would be
                # idempotent but pointless, and "live" is the same answer the
                # first publication's live copy gets. `materialization_state`
                # is where a reader asks whether the bytes have landed.
                return {
                    "ok": True, "state": "live", "step": "none",
                    "batch_id": batch_id, "tier": tier,
                    "mover_key": str(resume.mover_key or ""),
                    "generation": int(resume.generation or 0),
                    "batch_namespace": canonical_ns,
                    "manifest_digest": manifest_digest,
                    "total_bytes": total, "duplicate": True,
                }
        elif entry.origin_reclaimed:
            # The durable charge was released on proven absence: there are no
            # origin bytes left to copy, and nothing here re-creates them.
            return {"ok": False, "step": "authority",
                    "refusal": "origin-reclaimed-no-restage"}
        else:
            resume = None
        if resume is None:
            # A NEW materialization. Prove the origins are the committed ones
            # BEFORE anything is funded or published.
            gate = commitments.origin_gate(
                batch_id, entry, filed, sealed, verified)
            if not gate["ok"]:
                return {"ok": False, "step": "origin",
                        **{k: v for k, v in gate.items() if k != "ok"}}
            generation = len(mats) + 1
            sealed_mover = _seal_output_mover(
                queue, checked_instance, checked_template, sealed,
                batch_id=batch_id, tier=tier, cas_root=cas_root,
                producer_action_key=producer_action_key,
                command_extra=command_extra, retry_policy=retry_policy,
                generation=generation, restage_fill=restage_fill)
            if not sealed_mover.get("ok"):
                return sealed_mover
            mover = str(sealed_mover["mover_key"])
            host = str(sealed_mover["host"])
            kind = str(sealed_mover["kind"])
            gib = int(sealed_mover["gib"])
            fill = sealed_mover.get("fill")
            assert fill is None or isinstance(fill, int)
            mover_retry_policy = dict(sealed_mover["retry_policy"])
            mover_cas_root = str(sealed_mover["cas_root"])
            try:
                bl.BatchLedger.append_materialization(
                    queue, checked_instance, checked_template, batch_id,
                    mover_key=mover, tier=tier, generation=generation,
                    host=host, fill=fill)
            except oc.ProducedOutputError as exc:
                return {"ok": False, "step": "intent", "refusal": str(exc)}
        else:
            # RESUME the filed intent. Deliberately no re-seal: the sealed key
            # on the row is the funding key, and re-deriving it from a tier
            # record that drifted since the crash would mint a second
            # generation for work already funded under the first.
            mover = str(resume.mover_key or '')
            generation = int(resume.generation or 0)
            host = str(resume.host or '')
            # The price the intent was sealed at (#747), absent when it
            # reserved none. Anything else on the row is not a price.
            fill = resume.fill_mb_s_pool_side
            if fill is not None and (type(fill) is not int or fill <= 0):
                return {"ok": False, "step": "resume",
                        "refusal": "unknown-retain: materialization-fill"}
            if str(resume.state) != 'funded':
                gate = commitments.origin_gate(batch_id, entry, filed, sealed, verified)
                if not gate["ok"]:
                    return {"ok": False, "step": "origin",
                            **{k: v for k, v in gate.items() if k != "ok"}}
            from prismabuild import core as _core_mod
            from prismabuild import storage_tiers as _tiers_mod

            kind = _tiers_mod.capacity_kind_of(tier)
            gib = _tiers_mod.stage_tokens_for_bytes(total)
            mover_retry_policy = (dict(retry_policy)
                                  if retry_policy is not None
                                  else {"max_attempts": 3, "retry_safe": True})
            try:
                mover_cas_root = str(_core_mod.PrismaBuildCAS(cas_root).root)
            except Exception as exc:
                return {"ok": False, "step": "resume",
                        "refusal": f"unknown-retain: {exc}"}

    # ---- Phase 2: SIDE EFFECTS. No ownership lock is held. Each step is the
    # existing prepaid primitive and each is idempotent, so a crash between
    # any two of them resumes from the filed intent rather than restarting.
    launch = _producer_launch_context(
        queue, str(producer_action_key
                   or os.environ.get(_ACTION_KEY_ENV_NAME()) or ""))
    if not launch.get("ok"):
        return launch
    staged = queue.funding.stage_output(
        tier_id=tier, owner_key=owner, mover_key=mover,
        instance=checked_instance, template=checked_template,
        batch_id=batch_id, descriptors=sealed)
    if not staged.get("ok"):
        staged["step"] = "stage"
        return staged
    published = _publish_output_mover_row(
        queue, mover_key=mover, cas_root=mover_cas_root, launch=launch,
        host=host, tier=tier, kind=kind, gib=gib,
        manifest_digest=manifest_digest, total=total,
        retry_policy=mover_retry_policy, fill=fill)
    if not published.get("ok"):
        return published
    funded = queue.funding.fund_output(
        tier_id=tier, owner_key=owner, mover_key=mover,
        instance=checked_instance, template=checked_template,
        batch_id=batch_id, descriptors=sealed)
    if not funded.get("ok"):
        funded["step"] = "fund"
        return funded

    # ---- Phase 3: RECONCILE. The verification takes no ownership lock, and
    # the ownership lock it then takes to file the flag holds only plain
    # file/ledger reads underneath it -- no transition lock is ever taken
    # under it, so this adds no lock order to the fleet.
    reconciled = _reconcile_materialization_funding(
        queue, mover=mover, tier=tier, kind=kind, gib=gib)
    if not reconciled.get("ok"):
        return reconciled
    record = reconciled["record"]
    assert isinstance(record, Mapping)
    with queue.stage_ownership_lock(prefix):
        try:
            bl.BatchLedger.fund_materialization(
                queue, checked_instance, batch_id, mover_key=mover)
        except oc.ProducedOutputError as exc:
            return {"ok": False, "step": "reconcile", "refusal": str(exc)}
    return {"ok": True, "state": "materializing", "step": "done",
            "batch_id": batch_id, "tier": tier, "mover_key": mover,
            "generation": generation, "batch_namespace": canonical_ns,
            "manifest_digest": manifest_digest, "total_bytes": total,
            "funding": "prepaid",
            "funding_generation": str(record.get("generation")),
            "tokens": [str(name) for name in record.get("tokens") or []],
            "resumed": resume is not None}


def build_stage_manifest(batch: Mapping[str, object],
                         mount_prefix: str) -> dict[str, object]:
    """Synthetic data manifest for `stage_move.move` (one mover per batch)."""

    entries = batch.get("entries")
    if not isinstance(entries, list) or not entries:
        raise oc.ProducedOutputError("batch carries no entries to stage")
    manifest_entries = [
        {"path": str(e["path"]), "offset": 0, "bytes": int(e["bytes"]),
         # DEV null digests stay JSON null through the manifest (core's
         # data-manifest validator and the mover both accept null); never
         # stringified.
         "sha256": (e["sha256"] if e["sha256"] is not None else None)}
        for e in entries]
    total = sum(int(e["bytes"]) for e in manifest_entries)
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {"tool": "produced-output-batch"},
        "mount_prefix": mount_prefix,
        "entries": manifest_entries,
        "entry_count": len(manifest_entries),
        "total_bytes": total,
        "annotations": {
            "coordinate_space": "output-manifest-batch",
            "batch_id": str(batch.get("batch_id")),
            "manifest_digest": str(batch.get("manifest_digest")),
        },
    }


def load_origin_batch(queue_root: str | Path, ref: Mapping[str, object]
                      ) -> dict[str, object]:
    """Resolve one declared origin-only batch to its committed files (#912).

    Everything comes from what PB filed, never from the reference beyond its
    coordinates: the instance at ``<owner>/<template>.<nonce>``, the template
    it is bound to, the commitments entry, and the immutable batch record
    through the one batch loader. The batch must be origin-only, committed
    over the digest the reference names, and not reclaimed, and every origin
    must still carry the identity its commit recorded, and a consumed batch
    that `origin_retirement_tick` has started to retire refuses. That recheck
    is taken here, at submission; the consumer's mover later verifies each entry's
    sha256 as it copies, which is what binds the staged bytes to the
    committed ones. Raises `ProducedOutputError` naming what failed.

    An origin whose timestamps alone moved (an NFS delegation recall, #1111)
    is read and hashed, with no lock held; when its digest is the committed
    one the batch resolves, and the re-pin is filed on the commitments entry
    (`load_origin_batches`), so the next check compares against it and reads
    nothing.

    Returns ``{"ref", "instance", "template", "record", "entries"}`` with the
    sealed descriptors in the batch's own order.
    """

    return load_origin_batches(queue_root, [ref])[0]


def load_origin_batches(queue_root: str | Path,
                        refs: Sequence[Mapping[str, object]]
                        ) -> list[dict[str, object]]:
    """`load_origin_batch` for each ref, with one commitments write per instance.

    Each batch is checked with no lock held. The re-pins its content checks
    made are then filed per instance: one output-prefix lock, one re-read of
    the commitments document, a re-stat of each re-pinned origin, and one
    write (`_file_origin_repins`), whatever the number of batches.
    """

    loaded = [bl.resolve_origin_batch(queue_root, ref) for ref in refs]
    _file_origin_repins(queue_root, loaded)
    return [item for item, _repins in loaded]


def _file_origin_repins(queue_root: str | Path,
                        loaded: Sequence[tuple[Mapping[str, object],
                                               Sequence[Mapping[str, object]]]]
                        ) -> None:
    """File the re-pins lockless checks made, under each instance's prefix lock (#1111).

    Per instance: the output-prefix lock `BatchLedger.commit_origin` and the
    retirement take, the commitments document read again, each origin
    stat'ed again and required to still be the identity its content check
    hashed (`bl.apply_origin_repins`), and one write. The lock holds no read of
    an origin's bytes. A batch that was reclaimed or re-committed meanwhile
    is left alone: whatever reads it next reads its state then. Raises
    `ProducedOutputError` when an origin changed after its content was read.
    """

    from prismabuild import pool as pool_mod

    groups: dict[str, tuple[Mapping[str, object], list]] = {}
    for item, repins in loaded:
        if repins:
            instance = item["instance"]
            key = str(bl.BatchLedger.for_instance(queue_root, instance).path)
            groups.setdefault(key, (instance, []))[1].append((item, repins))
    if not groups:
        return
    queue = pool_mod.PoolQueue(queue_root)
    for key, (instance, items) in groups.items():
        bl.BatchLedger.repin_scope(queue, key, instance, items)


def origin_batch_manifest(queue_root: str | Path,
                          refs: Sequence[Mapping[str, object]]
                          ) -> dict[str, object]:
    """The data manifest a consumer declares to read origin-only batches (#912).

    One v1 manifest over every declared batch, in the order given: each
    batch's entries in its own order, one read phase per batch, and the
    references themselves under the ``produced_output_batches`` annotation.
    The consumer submits it with ``pbrun --data-manifest`` and stages it with
    ``--residency stage`` like any other input; ``pbrun`` derives it again
    from the queue at submission and refuses a manifest that says anything
    else. The mount prefix is the common directory of the batches' output
    prefixes. Every batch is resolved through `load_origin_batch`, so an
    uncommitted, reclaimed or changed batch refuses here.
    """

    if (not isinstance(refs, Sequence) or isinstance(refs, (str, bytes))
            or not refs):
        raise oc.ProducedOutputError("origin batch manifest needs at least one ref")
    loaded = load_origin_batches(queue_root, refs)
    keys = [tuple(item["ref"].values()) for item in loaded]
    if len(set(keys)) != len(keys):
        raise oc.ProducedOutputError("origin batch manifest names a batch twice")
    prefixes = [str(item["instance"]["output_prefix"]) for item in loaded]
    mount_prefix = os.path.commonpath(prefixes)
    if mount_prefix == "/":
        raise oc.ProducedOutputError(
            "origin batches share no directory below / to mount")
    entries: list[dict[str, object]] = []
    phases: list[dict[str, object]] = []
    running = 0
    seen: set[str] = set()
    for index, item in enumerate(loaded):
        size = 0
        for desc in item["entries"]:
            if str(desc["path"]) in seen:
                # Two committed batches over one origin path: a retried owner
                # attempt can file one (ownership is per attempt), and at most
                # one of them still names the file that is there.
                raise oc.ProducedOutputError(
                    f"origin batch manifest names {desc['path']} twice")
            seen.add(str(desc["path"]))
            entries.append({"path": str(desc["path"]), "offset": 0,
                            "bytes": int(desc["bytes"]),
                            "sha256": desc["sha256"]})
            size += int(desc["bytes"])
        running += size
        phases.append({"name": f"{index:04d}-{item['ref']['batch_id']}",
                       "bytes": size, "cumulative_bytes": running})
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {"tool": "prismabuild.produced_output.origin_batch_manifest"},
        "mount_prefix": mount_prefix,
        "entries": entries,
        "entry_count": len(entries),
        "total_bytes": running,
        "annotations": {
            "phases": phases,
            oc.ORIGIN_BATCHES_ANNOTATION: [dict(item["ref"]) for item in loaded],
        },
    }


def _slot_phases(checked: Mapping[str, object], slots: object, *,
                 where: str) -> dict[str, dict[str, object]]:
    """``slots`` by phase name, checked against a validated v2 manifest."""

    if (not isinstance(slots, Sequence) or isinstance(slots, (str, bytes))
            or not slots):
        raise oc.ProducedOutputError(f"{where} must be a non-empty array")
    plan = checked["read_plan"]
    assert isinstance(plan, Mapping)
    names = {str(phase["name"]) for phase in plan["phases"]}
    out: dict[str, dict[str, object]] = {}
    for slot in slots:
        if not isinstance(slot, Mapping) or "phase" not in slot:
            raise oc.ProducedOutputError(f"{where} entries must name a phase")
        name = slot["phase"]
        if not isinstance(name, str) or name not in names:
            raise oc.ProducedOutputError(
                f"{where} names {name!r}, which is not a read phase")
        if name in out:
            raise oc.ProducedOutputError(f"{where} names phase {name!r} twice")
        out[name] = dict(slot)
    return out


def place_origin_batches(queue_root: str | Path, static: Mapping[str, object],
                         slots: Sequence[Mapping[str, object]]
                         ) -> dict[str, object]:
    """A data_manifest.v2 that reads committed batches where its plan says (#946).

    ``static`` is the consumer's own v2 manifest: its entries, and a read
    plan in which each phase that will read batches reads nothing yet
    (``entry_indices: []``). ``slots`` is ``[{"phase", "refs"}]``: which
    committed origin-only batches each such phase reads. Every slot's batches
    are resolved through `origin_batch_manifest`, so an uncommitted,
    reclaimed, retiring or changed batch refuses here.

    The result is canonical, so ``pbrun`` can derive it again and compare:
    the static entries keep their indices, each slot's batch entries follow
    them in plan order, and each slot phase reads exactly its batch's entries
    in the batch's own order. Every other phase is unchanged, so its
    ``entry_indices`` keep their v2 meaning, re-reads included. Phase sizes,
    running sums, ``read_bytes`` and the totals are recomputed; the mount
    prefix is the common directory of the static prefix and the batches'.
    ``annotations.produced_output_batches`` lists every ref in plan order and
    ``annotations.produced_output_slots`` the ``{"phase", "refs"}`` placed.
    A batch is read in its slot phase only.
    """

    from prismabuild import core as core_mod
    from . import data_manifest as manifest_codec

    checked = manifest_codec.validate_data_manifest(static)
    if checked["schema"] != manifest_codec.DATA_MANIFEST_SCHEMA_V2:
        raise oc.ProducedOutputError(
            "placing batches in a read plan needs a data_manifest.v2")
    notes = dict(checked["annotations"])
    if oc.ORIGIN_BATCHES_ANNOTATION in notes:
        raise oc.ProducedOutputError(
            f"a v2 read plan declares its batches through "
            f"{oc.ORIGIN_SLOTS_ANNOTATION}, not {oc.ORIGIN_BATCHES_ANNOTATION}")
    wanted = _slot_phases(checked, slots, where="slots")
    plan = checked["read_plan"]
    assert isinstance(plan, Mapping)
    entries = [dict(entry) for entry in checked["entries"]]
    prefixes = [str(checked["mount_prefix"])]
    placed_slots: list[dict[str, object]] = []
    all_refs: list[dict[str, object]] = []
    orders: list[tuple[str, list[int]]] = []
    for phase in plan["phases"]:
        name = str(phase["name"])
        slot = wanted.get(name)
        if slot is None:
            orders.append((name, list(phase["entry_indices"])))
            continue
        if set(slot) != {"phase", "refs"}:
            raise oc.ProducedOutputError(
                f"slot {name!r} must carry exactly phase and refs")
        if phase["entry_indices"]:
            raise oc.ProducedOutputError(
                f"slot phase {name!r} already reads static entries")
        batch = origin_batch_manifest(queue_root, slot["refs"])
        start = len(entries)
        entries.extend(dict(entry) for entry in batch["entries"])
        orders.append((name, list(range(start, len(entries)))))
        refs = list(batch["annotations"][oc.ORIGIN_BATCHES_ANNOTATION])
        placed_slots.append({"phase": name, "refs": refs})
        all_refs.extend(refs)
        prefixes.append(str(batch["mount_prefix"]))
    keys = [tuple(sorted(ref.items())) for ref in all_refs]
    if len(set(keys)) != len(keys):
        raise oc.ProducedOutputError("slots name one batch twice")
    phases: list[dict[str, object]] = []
    running = 0
    for name, indices in orders:
        size = sum(int(entries[index]["bytes"]) for index in indices)
        running += size
        phases.append({"name": name, "entry_indices": indices,
                       "bytes": size, "cumulative_bytes": running})
    mount_prefix = os.path.commonpath(prefixes)
    if mount_prefix == "/":
        raise oc.ProducedOutputError(
            "the static entries and the batches share no directory below / "
            "to mount")
    notes.pop(oc.ORIGIN_SLOTS_ANNOTATION, None)
    notes[oc.ORIGIN_BATCHES_ANNOTATION] = all_refs
    notes[oc.ORIGIN_SLOTS_ANNOTATION] = placed_slots
    try:
        return manifest_codec.validate_data_manifest({
            "schema": manifest_codec.DATA_MANIFEST_SCHEMA_V2,
            "produced_by": dict(checked["produced_by"]),
            "mount_prefix": mount_prefix,
            "entries": entries,
            "entry_count": len(entries),
            "total_bytes": sum(int(entry["bytes"]) for entry in entries),
            "annotations": notes,
            "read_plan": {"phases": phases, "read_bytes": running},
        })
    except core_mod.PrismaBuildError as exc:
        raise oc.ProducedOutputError(f"placed manifest refused: {exc}") from None


def verify_placed_origin_batches(queue_root: str | Path,
                                 manifest: Mapping[str, object]) -> None:
    """Refuse a v2 manifest whose batches are not placed as its slots say (#946).

    The v2 counterpart of deriving `origin_batch_manifest` again: the
    manifest's static part is recovered by taking off the trailing batch
    entries and emptying the slot phases, `place_origin_batches` places the
    declared slots into it again from the queue's own records, and the
    result must be the manifest, byte for byte in its normalized form. So a
    submitted plan can read static entries in any order and re-read them, but
    each batch it declares is exactly the committed one, read once, at its
    slot. Raises `ProducedOutputError` naming what does not match.
    """

    from prismabuild import core as core_mod
    from . import data_manifest as manifest_codec

    checked = manifest_codec.validate_data_manifest(manifest)
    notes = dict(checked["annotations"])
    slots = notes.get(oc.ORIGIN_SLOTS_ANNOTATION)
    wanted = _slot_phases(checked, slots, where=oc.ORIGIN_SLOTS_ANNOTATION)
    for name, slot in wanted.items():
        if set(slot) != {"phase", "refs"}:
            raise oc.ProducedOutputError(
                f"slot {name!r} must carry exactly phase and refs; an "
                "--after slot is placed by the release, not submitted")
    counts = {}
    for name, slot in wanted.items():
        counts[name] = int(origin_batch_manifest(
            queue_root, slot["refs"])["entry_count"])
    batch_entries = sum(counts.values())
    entries = list(checked["entries"])
    if batch_entries >= len(entries):
        raise oc.ProducedOutputError(
            "a v2 read plan with slots needs static entries of its own")
    static_count = len(entries) - batch_entries
    plan = checked["read_plan"]
    assert isinstance(plan, Mapping)
    phases: list[dict[str, object]] = []
    running = 0
    for phase in plan["phases"]:
        indices = list(phase["entry_indices"])
        if str(phase["name"]) in wanted:
            indices = []
        elif any(index >= static_count for index in indices):
            raise oc.ProducedOutputError(
                f"phase {phase['name']!r} reads a batch entry outside its slot")
        size = sum(int(entries[index]["bytes"]) for index in indices)
        running += size
        phases.append({"name": phase["name"], "entry_indices": indices,
                       "bytes": size, "cumulative_bytes": running})
    static_entries = entries[:static_count]
    skeleton_notes = {key: value for key, value in notes.items()
                      if key not in (oc.ORIGIN_BATCHES_ANNOTATION,
                                     oc.ORIGIN_SLOTS_ANNOTATION)}
    try:
        skeleton = manifest_codec.validate_data_manifest({
            "schema": manifest_codec.DATA_MANIFEST_SCHEMA_V2,
            "produced_by": dict(checked["produced_by"]),
            "mount_prefix": checked["mount_prefix"],
            "entries": static_entries,
            "entry_count": static_count,
            "total_bytes": sum(int(entry["bytes"]) for entry in static_entries),
            "annotations": skeleton_notes,
            "read_plan": {"phases": phases, "read_bytes": running},
        })
    except core_mod.PrismaBuildError as exc:
        raise oc.ProducedOutputError(
            f"the manifest's static part is not a read plan: {exc}") from None
    placed = place_origin_batches(
        queue_root, skeleton,
        [{"phase": name, "refs": wanted[name]["refs"]}
         for name in [str(phase["name"]) for phase in plan["phases"]]
         if name in wanted])
    if placed != checked:
        raise oc.ProducedOutputError(
            "the manifest does not read its declared batches where its slots "
            "say; build it with produced_output.place_origin_batches")


#: The deferral `retire_batch` reports while the retirement's OWN egress action
#: is queued or running on the tier host. It rides the egress receipt's
#: `deferred_own` list beside `own-copy-in-flight` because it is the same kind
#: of answer: bytes, proof and full credit are kept, and an ordinary retry
#: completes the retirement once that action has ended.


def _egress_runs_in_process(record: Mapping[str, object] | None) -> bool:
    """Does THIS process sit on the box that owns the stage?

    The egress unlinks staged files, and only the tier host mounts the stage
    read-write: the GPU hosts mount it read-only, so an egress run in a
    producer there cannot delete anything (#801). The tier record's `host` is
    the fact `tier_loop.py` announces for exactly this purpose, and it is the
    same fact the movers are placed by.

    A wrong "elsewhere" is always safe -- the tier-host route works from any
    box, including the tier host itself -- so nothing here tries to be clever.
    Inside a container `socket.gethostname()` is the container's name, never
    the box's, and the answer is "elsewhere": that is the safe direction, and
    it must not be "fixed" by resolving `record["host"]` some other way. A
    tier with no announced record or host keeps the in-process egress. No
    mover can be sealed without the record, so its absence is a fixture or a
    tier that has gone away, and the in-process egress fails safe there: an
    unlink the mount refuses is an `errors` entry that keeps the bytes, the
    fragment and the tokens. `retire_batch` adds one condition: the
    in-process egress also needs the fleet tool importable, and an owner that
    cannot import it takes the tier-host route wherever it runs.
    """

    import socket

    host = str(record.get("host") or "") if isinstance(record, Mapping) else ""
    return not host or host == socket.gethostname()


def _seal_output_egress(queue, *, record: Mapping[str, object],
                        producer: str, cas_root, batch_id: str,
                        generation: int, target_mover: str, consumer: str
                        ) -> dict[str, object]:
    """Seal (never file) the egress action for one materialization.

    The egress node `pbrun` seals for a consumer's staged range, sealed the
    same way for a produced batch: `stage_release.py` off the ANNOUNCED TIER
    RECORD, placed on the box that owns the stage, one CPU and one GiB, and no
    tier demand -- an egress returns capacity, and one that had to reserve
    some before giving any back would deadlock exactly when the stage is full.
    It carries no data manifest, so nothing prewarms for it.

    Both roots are the ones the MOVER was sealed with -- the tier's announced
    mountpoint and the produced-output fragment root under this pool -- and
    never the calling process's own spelling of them: the action runs on
    another box, where a wrong stage root reads as an unregistered stage and
    a wrong fragment root reads as "nothing staged", which is a complete
    receipt that deleted nothing. The answer carries both so the caller can
    refuse a retirement whose own arguments name different ones.

    The key is content-addressed over the whole sealed action: this command,
    the tier's announced interpreter and tools, the placement tag and the
    producer's request. It is unique per materialization, and every call
    re-derives it instead of recording it; it moves only when one of those
    facts does, and an egress under the earlier key is then an idempotent
    no-op beside this one.
    """

    from prismabuild import movement_actions
    from prismabuild import pool as pool_mod

    parent = _read_producer_request(cas_root, producer)
    if isinstance(parent, Mapping):
        return dict(parent)
    cas, request = parent
    templated = _producer_movement_template(queue, request, producer)
    if not templated.get("ok"):
        return templated
    try:
        mover_python, _mover_tool, egress_tool = (
            movement_actions.movement_tools(record))
    except SystemExit as exc:
        return {"ok": False, "step": "resolve", "refusal": str(exc)}
    host = str(record.get("host") or "")
    stage_root = str(record.get("mountpoint") or "")
    if not stage_root.startswith("/"):
        return {"ok": False, "step": "resolve",
                "refusal": "the stage tier announces no mountpoint to "
                           "release from"}
    residency_root = str(bl.output_fragment_root(
        Path(queue.root) / pool_mod.record_store.RESIDENCY))
    command = [mover_python, egress_tool,
               "--pool-root", str(queue.root),
               "--mover-action-key", target_mover,
               "--consumer-action-key", consumer,
               "--stage-root", stage_root,
               "--residency-root", residency_root]
    retry_policy = {"max_attempts": 3, "retry_safe": True}
    log_name = f"produced-output-egress-{batch_id}.log"
    if int(generation) > 0:
        log_name = (f"produced-output-egress-{batch_id}"
                    f"-m{int(generation)}.log")
    try:
        action = movement_actions.seal_movement_action(
            templated["template"], command=command,
            demand={"cpu": 1, "mem_gb": 1},
            tags=[host] if host else [],
            log_name=log_name, retry_policy=retry_policy)
    except SystemExit as exc:
        return {"ok": False, "step": "seal", "refusal": str(exc)}
    except Exception as exc:
        return {"ok": False, "step": "seal", "refusal": str(exc)}
    return {"ok": True, "action": action, "cas": cas,
            "egress_key": str(action["action_key"]), "host": host,
            "stage_root": stage_root, "residency_root": residency_root,
            "retry_policy": retry_policy}


def _tier_host_egress(queue, *, record: Mapping[str, object], producer: str,
                      cas_root, batch_id: str, generation: int,
                      target_mover: str, consumer: str, stage_root: str,
                      residency_root: str) -> dict[str, object]:
    """Run one materialization's egress ON THE TIER HOST, across calls.

    Returns `{"receipt"}` -- a complete, error-free egress receipt, ready for
    the caller's filing phase -- or `{"answer"}`, the caller's whole answer.

    One `retire_batch` call cannot wait for another box, so the egress is an
    ordinary queue action and the retirement is a re-driven call: the first
    call publishes the action and answers the typed `egress-incomplete`
    deferral `own-egress-in-flight`; a later call finds the action ended,
    reads the receipt it filed, and files the retirement. Nothing is decided
    from silence: only a filed receipt naming this egress key and this batch's
    namespace, complete and error-free, retires anything.

    `stage_release.evict` is idempotent, so an egress that runs twice -- a
    queue retry, or a key that moved because the tier announced other tools --
    is a no-op receipt, never a second delete.

    `stage_root` and `residency_root` are the CALLER's; the action is sealed
    with the tier's (`_seal_output_egress`), and a caller naming different
    ones is refused before anything is published.
    """

    from prismabuild import pool as pool_mod

    def incomplete(receipt: Mapping[str, object], key: str | None
                   ) -> dict[str, object]:
        answer: dict[str, object] = {
            "ok": False, "refusal": "egress-incomplete",
            "receipt": dict(receipt)}
        if key:
            answer["egress_action_key"] = key
        return {"answer": answer}

    def deferred(reason: str, key: str | None = None,
                 state: str | None = None) -> dict[str, object]:
        receipt: dict[str, object] = {
            "schema": pool_mod.POOL_EGRESS_SCHEMA_V1,
            "action_key": target_mover,
            "consumer_action_key": consumer,
            "stage_root": str(stage_root),
            "complete": False, "errors": [],
            "live_pins": [], "deferred_handoffs": [],
            "deferred_own": [reason]}
        if key:
            receipt["egress_action_key"] = key
        if state:
            receipt["egress_state"] = state
        return incomplete(receipt, key)

    # The mover's OWN copy first, exactly as the egress itself would answer
    # (#795): a copy still queued or running is never raced by its egress.
    mover = RecordStore(queue.root).mover(target_mover)
    if isinstance(mover, Unknown):
        return {"answer": {"ok": False,
                           "refusal": "unknown-retain: mover-row-unreadable"}}
    if (isinstance(mover, Present)
            and mover.value.state in (pool_mod.record_store.READY, pool_mod.record_store.CLAIMED)):
        return deferred("own-copy-in-flight")
    sealed = _seal_output_egress(
        queue, record=record, producer=producer, cas_root=cas_root,
        batch_id=batch_id, generation=generation, target_mover=target_mover,
        consumer=consumer)
    if not sealed.get("ok"):
        return {"answer": {"ok": False, "step": sealed.get("step"),
                           "refusal": "unknown-retain: egress-seal: "
                                      f"{sealed.get('refusal')}"}}
    for name, mine, sealed_root in (
            ("stage-root", stage_root, sealed["stage_root"]),
            ("residency-root", residency_root, sealed["residency_root"])):
        if os.path.normpath(str(mine)) != os.path.normpath(str(sealed_root)):
            return {"answer": {
                "ok": False,
                "refusal": f"unknown-retain: {name}-mismatch: the tier's is "
                           f"{sealed_root}, this retirement named {mine}"}}
    egress_key = str(sealed["egress_key"])
    egress = RecordStore(queue.root).mover(egress_key)
    if isinstance(egress, Unknown):
        return {"answer": {"ok": False, "egress_action_key": egress_key,
                           "refusal": "unknown-retain: egress-row-unreadable"}}
    if (isinstance(egress, Present)
            and egress.value.state in (pool_mod.record_store.READY, pool_mod.record_store.CLAIMED)):
        return deferred(oc.OWN_EGRESS_IN_FLIGHT, egress_key, egress.value.state)
    # `Pool._file_move` files a node's receipt under the node's OWN key, so an
    # egress receipt is read by the egress key and names it; the mover it
    # retired is bound by that key, which hashes a command naming it. The
    # namespace must still be this batch's. Read whatever the row's state:
    # the tool files its receipt before it exits, and `finish` takes the
    # claim away before it writes the terminal, so a finished egress is
    # briefly in NO state directory with its complete receipt already filed.
    filed: dict[str, object] | None = None
    try:
        candidate = queue.move_record(egress_key)
    except Exception:
        candidate = None
    if (isinstance(candidate, Mapping)
            and str(candidate.get("action_key") or "") == egress_key
            and str(candidate.get("consumer_action_key") or "") == consumer):
        filed = dict(candidate)
        filed["schema"] = pool_mod.POOL_EGRESS_SCHEMA_V1
        filed["action_key"] = target_mover
        filed["egress_action_key"] = egress_key
    if (filed is not None and filed.get("complete") is True
            and not filed.get("errors")):
        return {"receipt": filed}
    # Nothing has completed this egress: publish it, or publish it AGAIN --
    # `recompute` is what makes a republished movement key run instead of
    # being answered from its old receipt.
    launch = _producer_launch_context(queue, producer)
    if not launch.get("ok"):
        return {"answer": {"ok": False, "step": launch.get("step"),
                           "egress_action_key": egress_key,
                           "refusal": "unknown-retain: egress-launch: "
                                      f"{launch.get('refusal')}"}}
    retry_policy = sealed["retry_policy"]
    assert isinstance(retry_policy, Mapping)
    host = str(sealed["host"])
    try:
        sealed["cas"].publish_action_request(sealed["action"])
        queue.publish(
            action_key=egress_key,
            cas_root=str(sealed["cas"].root),
            worker_script=str(launch["worker_script"]),
            tags=[host] if host else (),
            priority=int(launch["priority"]),
            max_attempts=int(retry_policy["max_attempts"]),
            retry_safe=bool(retry_policy["retry_safe"]),
            **dict(launch["addressing"]),
            # No tier demand, no residency block and no batch reference: the
            # row `pbrun` publishes for a consumer's egress, for its reasons.
            resources={"cpu": 1, "mem_gb": 1},
            recompute=True,
            # A re-driven retirement is an AUTOMATIC republication: it never
            # retires an operator's withdrawal of this egress by writing over
            # it.
            refuse_withdrawn=True,
            # And it never queues a second copy of an egress this queue is
            # already carrying.  The state read at the top of this function
            # asks the same question; on NFS its answer can be stale, and in
            # the 2026-09-21 cycle it was -- each egress was republished three
            # times and ran four. Only the queue's own transition lock makes
            # look-then-publish one decision (#810).
            refuse_if_live=True)
    except pool_mod.ActionAlreadyLiveError as exc:
        # Exactly what the state read above answers when it sees the row: this
        # egress is in flight, so the caller polls rather than publishes.
        return deferred(oc.OWN_EGRESS_IN_FLIGHT, egress_key, exc.state)
    except pool_mod.WithdrawnActionError as exc:
        # No `deferred_own`: a caller polling a deferral must stop here.
        return {"answer": {"ok": False, "step": "egress-publish",
                           "egress_action_key": egress_key,
                           "refusal": f"egress-withdrawn: {exc}"}}
    except Exception as exc:
        return {"answer": {"ok": False, "step": "egress-publish",
                           "egress_action_key": egress_key,
                           "refusal": f"unknown-retain: egress-publish: {exc}"}}
    if filed is not None:
        # An attempt ended without completing, and its receipt is the cause:
        # a live pin, a handoff, an error. Answered as the in-process egress
        # answers it; the attempt just published is the next re-drive's.
        return incomplete(filed, egress_key)
    return deferred(oc.OWN_EGRESS_IN_FLIGHT, egress_key, "published")


def _select_retirement_locked(queue, checked_instance: Mapping[str, object],
                              checked_template: Mapping[str, object],
                              batches: bl.BatchCapture,
                              batch_id: str) -> dict[str, object]:
    """Phase one of a retirement: validate and select, under the ownership lock.

    ``batches`` is the commitments document the caller read under the
    instance's output-prefix ownership lock, which it still holds.  Returns
    ``{"answer": ...}`` when the retirement ends here (a refusal, an
    origin-only batch, a duplicate), else ``{"selection": ...}``: the copy to
    retire and everything phase three revalidates against.

    Every check comes BEFORE any destructive call: the immutable record
    validates (schema/binding/entries/manifest), and the mutable
    commitments entry must agree with it on mover, tier, and the
    canonically derived namespace. A changed mover/tier in commitments
    never selects the egress target. Bad or foreign metadata refuses before
    fragments are read, files deleted, or tier ownership released.
    """

    from prismabuild import pool as pool_mod

    entry = batches.lookup(batch_id)
    if not isinstance(entry, bl.BatchFacets):
        return {"answer": {"ok": False, "refusal": "unknown-batch"}}
    try:
        filed, _sealed = entry.open(queue.root, checked_instance, checked_template, batch_id)
    except oc.ProducedOutputError as exc:
        return {"answer": {"ok": False, "refusal": str(exc)}}
    if entry.kind == 'origin' and filed.get('origin_only') is True:
        # Nothing was staged, so there is no copy to evict and no window
        # to return (#912). The origin is `BatchLedger.reclaim`'s.
        return {"answer": {"ok": True, "batch_id": batch_id,
                           "origin_only": True, "staged": False}}
    mover = str(filed.get("mover_key") or "")
    tier = str(filed.get("tier") or "")
    try:
        canonical_ns = oc.batch_namespace(
            checked_instance, batch_id,
            str(filed.get("manifest_digest") or ""))
    except oc.ProducedOutputError as exc:
        return {"answer": {"ok": False, "refusal": f"unknown-retain: {exc}"}}
    if (len(mover) != 64 or str(entry.first_mover or "") != mover
            or str(entry.tier or "") != tier
            or tier not in checked_template["permitted_tiers"]
            or str(entry.namespace or "") != canonical_ns
            or str(filed.get("batch_namespace") or "") != canonical_ns):
        return {"answer": {"ok": False,
                           "refusal": "unknown-retain: batch-target-mismatch"}}
    # WHICH copy is being retired: the batch's first materialization, or
    # a restaged successor that now owns the material under the same
    # canonical namespace. `entry["retired"]` answers only for the first,
    # so the active materialization is what selects the egress target --
    # otherwise a restaged batch reports a duplicate retirement and
    # orphans a live stage copy plus its window credit.
    try:
        active = entry.active
    except oc.ProducedOutputError as exc:
        return {"answer": {"ok": False, "refusal": str(exc)}}
    if active.retired:
        return {"answer": {"ok": True, "batch_id": batch_id,
                           "duplicate": True}}
    target_mover = str(active.mover_key or '')
    if len(target_mover) != 64 or str(active.tier or '') != tier:
        return {"answer": {"ok": False,
                           "refusal": "unknown-retain: batch-target-mismatch"}}
    consumer = canonical_ns
    # Capture the staged paths the egress is about to vouch while the
    # fragments still exist; after the delete only this record names
    # them for later live-path attribution. A fragment read that
    # fails is unknown attribution, never an empty set: retirement
    # refuses rather than recording known-empty paths.
    staged_paths: list[str] = []
    try:
        from prismabuild import residency_map as map_mod

        out_base = bl.output_fragment_root(queue.root / pool_mod.record_store.RESIDENCY)
        fragments = map_mod.read_fragments(out_base, consumer)
        if fragments:
            composed = map_mod.compose(fragments)
            entries = composed.get("entries")
            if not isinstance(entries, Mapping):
                return {"answer": {"ok": False,
                                   "refusal": "unknown-retain: fragment-entries"}}
            for record in entries.values():
                if isinstance(record, Mapping) and record.get("stage_path"):
                    staged_paths.append(os.path.normpath(
                        str(record["stage_path"])))
    except oc.ProducedOutputError as exc:
        return {"answer": {"ok": False, "refusal": f"unknown-retain: {exc}"}}
    except (OSError, ValueError) as exc:
        return {"answer": {"ok": False, "refusal": f"unknown-retain: {exc}"}}
    # Paths an earlier call of this retirement filed for this same copy:
    # a tier-host egress drops the fragment between two calls, so the
    # call that files the retirement may find nothing left to read.
    recorded = active.staged_paths
    recorded_paths = {os.path.normpath(path) for path in recorded
                      if isinstance(path, str) and path} if isinstance(
                          recorded, list) else set()
    return {"selection": {
        "entry": entry, "tier": tier, "canonical_ns": canonical_ns,
        "target_mover": target_mover, "consumer": consumer,
        "manifest": str(filed.get("manifest_digest") or ""),
        "source": str(active.source or ""),
        "generation": int(active.generation or 0),
        "recorded_paths": recorded_paths,
        "staged_paths": sorted(set(staged_paths) | recorded_paths),
    }}


def _answer(result: Mapping[str, object]) -> dict[str, object]:
    """A phase-three answer as `retire_batch` returns it."""

    return {key: value for key, value in result.items() if key != "marked"}


def retire_batch(queue, instance: Mapping[str, object],
                 template: Mapping[str, object], batch_id: str, *,
                 stage_root: str, residency_root: str | Path,
                 cas_root=None) -> dict[str, object]:
    """Evict one batch's staged files, then retire its stage window.

    WHERE the egress runs follows the tier record. On the box that owns the
    stage it runs in this process, as it always has. Anywhere else it runs as
    a queue action placed on that box (`_tier_host_egress`), because only the
    tier host mounts the stage read-write (#801): the first call publishes
    the action and answers `egress-incomplete` with the receipt's
    `deferred_own` naming `own-egress-in-flight`, and the caller re-drives
    this call -- exactly as it already does for `own-copy-in-flight` -- until
    the action has ended and the retirement files. `cas_root` is where that
    action's request is filed; it defaults to the producer row's own root.

    Provenance is validated BEFORE anything destructive: the immutable
    batch record loads through the single loader (schema/binding/
    entries/manifest), the mutable commitments entry must agree with it
    on mover, tier, and the canonically derived namespace, and the
    egress target (mover/namespace) comes from that validated result --
    never from unchecked entry fields. WHICH copy is retired is the batch's
    ACTIVE materialization: its first publication, or the successor a
    `ensure_batch_materialized` restage put on the tier.

    Three phases, and the ownership lock is held for only two of them:
    select and capture under the output-prefix ownership lock
    (`_select_retirement_locked`); run the existing egress with NO lock of
    this lane held; reacquire, revalidate the exact same
    manifest/mover/generation, and file `retired` for a complete error-free
    receipt naming that mover and namespace (`BatchCapture.file_retirement`). The
    egress takes the mover transition lock and then the stage root's
    ownership lock, so running it underneath this instance's ownership lock
    would nest two locks of one family with a blocking transition wait
    between them; nothing needs that, because the materialization stays
    unretired for the whole window and therefore keeps refusing both a
    second writer over its origins and any successor materialization.
    `retire_staged_batches` runs the same three phases for many batches of
    one instance, with one read and one write of the document for all of
    them.

    Durable-origin quota is NOT freed here -- origin files still exist; see
    `BatchLedger.reclaim`. Charge (durable) and window (tier) accounting
    stay distinct at every step, and a failed or partial egress files
    nothing and leaves the old materialization holding its own credit.
    """

    from prismabuild import core as core_mod

    try:
        checked_template, checked_instance = oc.bound_output_contract(
            template, instance)
    except oc.ProducedOutputError as exc:
        return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    oc.output_name(batch_id, where="batch_id")
    with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
        try:
            commitments = bl.BatchLedger.for_instance(queue.root, checked_instance).entries()
        except oc.ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        batches = commitments
        assert isinstance(batches, bl.BatchCapture)
        chosen = _select_retirement_locked(
            queue, checked_instance, checked_template, batches, batch_id)
        if "answer" in chosen:
            return dict(chosen["answer"])  # type: ignore[arg-type]
        selection = chosen["selection"]
        assert isinstance(selection, dict)
        entry = selection["entry"]
        tier = str(selection["tier"])
        target_mover = str(selection["target_mover"])
        consumer = str(selection["consumer"])
        staged_paths = list(selection["staged_paths"])
        recorded_paths = set(selection["recorded_paths"])
        try:
            tier_record = _announced_tier_record(queue, tier)
        except Exception as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        in_process = _egress_runs_in_process(tier_record)
        if not in_process and tier_record is None:
            return {"ok": False,
                    "refusal": f"unknown-retain: tier-not-announced: {tier}"}
        if not in_process and set(staged_paths) - recorded_paths:
            try:
                batches.record_staged_paths(
                    queue, checked_instance, batch_id,
                    str(selection["source"]), target_mover, staged_paths)
            except (oc.ProducedOutputError, OSError) as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    # --- The ownership lock is RELEASED here, before the egress runs. ---
    # `stage_release.evict` takes the mover transition lock (blocking) and
    # then the STAGE ROOT's ownership lock, and its containment reclamation
    # reaches further locks below that. Holding this instance's output-prefix
    # ownership lock across all of it nests two locks of the same family and
    # parks a blocking transition wait underneath an ownership lock -- the
    # shape the egress cross-root cycle came from. Nothing this lane needs is
    # protected by holding it here: the materialization is still unretired
    # for the whole window, so `bl.own_write_owner` keeps refusing a second
    # writer over these origins and `ensure_batch_materialized` keeps
    # refusing a successor, and a failed or partial egress files nothing and
    # leaves the old materialization holding its own credit.
    if in_process:
        assert staged_path is not None
        receipt = staged_path.evict(
            queue, target_mover, consumer_action_key=consumer,
            stage_root=str(stage_root), residency_root=str(residency_root))
        if not receipt.get("complete"):
            return {"ok": False, "refusal": "egress-incomplete",
                    "receipt": receipt}
    else:
        # The same egress, on the box that can delete: see `_tier_host_egress`.
        assert tier_record is not None
        producer = str(checked_instance.get("owner_action_key")
                       or os.environ.get(core_mod.ACTION_KEY_ENV) or "")
        if cas_root is None:
            launch = _producer_launch_context(queue, producer)
            cas_root = (str(launch["cas_root"]) if launch.get("ok")
                        else str(Path(queue.root).parent / "cas"))
        routed = _tier_host_egress(
            queue, record=tier_record, producer=producer, cas_root=cas_root,
            batch_id=batch_id, generation=int(selection["generation"]),
            target_mover=target_mover, consumer=consumer,
            stage_root=str(stage_root), residency_root=str(residency_root))
        if "answer" in routed:
            answer = routed["answer"]
            assert isinstance(answer, dict)
            return answer
        receipt = routed["receipt"]
        assert isinstance(receipt, dict)
    with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
        path = bl.BatchLedger.for_instance(queue.root, checked_instance).path
        try:
            commitments = bl.BatchLedger(path).entries()
        except oc.ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        batches = commitments
        assert isinstance(batches, bl.BatchCapture)
        result = batches.file_retirement(
            queue, checked_instance, checked_template, batch_id,
            selection, receipt)
        if result.get("marked"):
            try:
                commitments.file(admission=True)
            except oc.ProducedOutputError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    return _answer(result)


#: The unit `retire_staged_batches` asks a budget for: one batch's egress.


def retire_staged_batches(queue, instance: Mapping[str, object],
                          template: Mapping[str, object],
                          batch_ids: Sequence[str], *, stage_root: str,
                          residency_root: str | Path,
                          budget=None) -> dict[str, dict[str, object]]:
    """Retire many staged batches of one instance, with one commitments write (#1072).

    `retire_batch`, run for each batch, reads the instance's commitments
    document four times and writes it once, fsync included.  R13's document
    holds 436 batches in 9.2 MB, so a backlog of K batches of one instance
    cost K whole-document writes: the live tier loop's first cycle after
    the 09-24 publish spent 129.7 s retiring 137 batches of fourteen dead
    instances.  Here the three phases of `retire_batch` run for every batch
    at once: phase one selects all of them under one ownership lock and one
    read, phase two runs each egress with no lock of this lane held, and
    phase three revalidates each batch under one lock and one read and
    writes the document once for every batch it marked.

    Only the in-process egress: the caller is the tier loop, on the tier
    host.  A batch whose egress would take the tier-host route is answered
    ``not-on-the-tier-host`` and nothing of it is touched.

    **What a crash can leave.**  The two states that must never exist are a
    batch recorded retired while its copy is still on the stage, and a copy
    gone with nothing that will ever record it.

    * Recorded but not retired cannot happen.  Phase three marks a batch
      only for a complete, error-free egress receipt of this batch's exact
      selection (the same `BatchCapture.file_retirement` checks as
      `retire_batch`), and the one write that records it is made after
      every such receipt is in hand.  A batch whose egress was incomplete,
      was deferred, or whose selection moved is left out of the write, not
      the transaction: the others are still recorded.
    * Retired but unrecorded is the state `retire_batch` already has for
      one batch, between its egress and its write, and it recovers: the
      unretired materialization keeps refusing a second writer and any
      successor, and the next tick finds the batch again (its mover ended,
      no tokens held) and runs the egress again, which finds nothing to
      delete and returns a complete receipt, and files it.  Batching widens
      that window to every batch egressed before the one write; it opens no
      new state.
    * The write itself is one atomic rename of the whole document, so a
      crash during it leaves either every mark or none.

    ``budget``, when given, is asked before each egress whether another
    fits this cycle (`budget.start`) and told when it ends (`budget.done`),
    which is where the tier loop re-announces its tiers between units.  A
    batch the budget defers is answered ``deferred`` and waits, untouched,
    for a later cycle.

    Returns ``{batch_id: answer}``, each answer shaped as `retire_batch`'s.
    """

    try:
        checked_template, checked_instance = oc.bound_output_contract(
            template, instance)
    except oc.ProducedOutputError as exc:
        refusal = {"ok": False, "refusal": f"unknown-retain: {exc}"}
        return {batch_id: dict(refusal) for batch_id in batch_ids}
    for batch_id in batch_ids:
        oc.output_name(batch_id, where="batch_id")
    answers: dict[str, dict[str, object]] = {}
    selections: dict[str, dict[str, object]] = {}
    lock_name = str(checked_instance["output_prefix"])
    path = bl.BatchLedger.for_instance(queue.root, checked_instance).path
    with queue.stage_ownership_lock(lock_name):
        try:
            commitments = bl.BatchLedger(path).entries()
        except oc.ProducedOutputError as exc:
            refusal = {"ok": False, "refusal": f"unknown-retain: {exc}"}
            return {batch_id: dict(refusal) for batch_id in batch_ids}
        batches = commitments
        assert isinstance(batches, bl.BatchCapture)
        tier_records: dict[str, object] = {}
        for batch_id in batch_ids:
            chosen = _select_retirement_locked(
                queue, checked_instance, checked_template, batches, batch_id)
            if "answer" in chosen:
                answers[batch_id] = dict(chosen["answer"])  # type: ignore[arg-type]
                continue
            selection = chosen["selection"]
            assert isinstance(selection, dict)
            tier = str(selection["tier"])
            if tier not in tier_records:
                try:
                    tier_records[tier] = _announced_tier_record(queue, tier)
                except Exception as exc:
                    tier_records[tier] = exc
            tier_record = tier_records[tier]
            if isinstance(tier_record, Exception):
                answers[batch_id] = {"ok": False,
                                     "refusal": f"unknown-retain: {tier_record}"}
                continue
            if not _egress_runs_in_process(tier_record):
                answers[batch_id] = {"ok": False,
                                     "refusal": "not-on-the-tier-host"}
                continue
            selections[batch_id] = selection
    if not selections:
        return answers
    # --- No lock of this lane is held while the egresses run; see
    # `retire_batch` for why nothing needs it.
    receipts: dict[str, dict[str, object]] = {}
    # An egress that raises ends the egresses, not the retirement: the
    # receipts already in hand are still filed below, as `retire_batch` would
    # have filed each of them before the next one ran, and the exception is
    # raised once they are.  An interrupt is not caught: it leaves those
    # batches egressed and unrecorded, the recoverable state described above.
    raised: Exception | None = None
    for batch_id, selection in selections.items():
        if raised is not None or (budget is not None and not budget.start(
                oc.BATCH_RETIREMENT_UNIT, batch_id)):
            answers[batch_id] = {"ok": False, "refusal": "deferred",
                                 "deferred": True}
            continue
        try:
            receipt = staged_path.evict(
                queue, str(selection["target_mover"]),
                consumer_action_key=str(selection["consumer"]),
                stage_root=str(stage_root), residency_root=str(residency_root))
        except Exception as exc:  # noqa: BLE001 -- re-raised below
            raised = exc
            answers[batch_id] = {"ok": False, "refusal": "egress-raised"}
            continue
        finally:
            if budget is not None:
                budget.done(oc.BATCH_RETIREMENT_UNIT)
        if not receipt.get("complete"):
            answers[batch_id] = {"ok": False, "refusal": "egress-incomplete",
                                 "receipt": receipt}
            continue
        receipts[batch_id] = receipt
    if not receipts:
        if raised is not None:
            raise raised
        return answers
    with queue.stage_ownership_lock(lock_name):
        try:
            commitments = bl.BatchLedger(path).entries()
        except oc.ProducedOutputError as exc:
            for batch_id in receipts:
                answers[batch_id] = {"ok": False,
                                     "refusal": f"unknown-retain: {exc}"}
            if raised is not None:
                raise raised
            return answers
        batches = commitments
        assert isinstance(batches, bl.BatchCapture)
        results = {batch_id: batches.file_retirement(
            queue, checked_instance, checked_template, batch_id,
            selections[batch_id], receipt)
            for batch_id, receipt in receipts.items()}
        if any(result.get("marked") for result in results.values()):
            try:
                commitments.file(admission=True)
            except oc.ProducedOutputError as exc:
                for batch_id in receipts:
                    answers[batch_id] = {"ok": False,
                                         "refusal": f"unknown-retain: {exc}"}
                results = {}
    for batch_id, result in results.items():
        answers[batch_id] = _answer(result)
    if raised is not None:
        raise raised
    return answers


# --------------------------------------------------------------------------
# Consumed origin batches: consumer declarations and retirement (#914)
# --------------------------------------------------------------------------
#
# A ``consumed`` origin-only batch is a handoff: PB deletes its origin files
# and frees its durable charge once the actions that read it have succeeded.
# Nothing here copies, stages or moves a token. It reads the queue's own
# records to learn how each action ended, and it deletes only files whose
# identity is the one the batch's commit recorded.

#: The events `origin_retirement_tick` returns, one per batch.
#: The events it returns for a write-only prewrite whose attempt ended
#: before committing (#949): the reservation was dropped, or its files
#: belong to no batch and wait for an operator.
#: The one remedy an orphaned prewrite's event and listing name (#949,
#: #1053). PB never deletes a file whose identity no commit recorded, so an
#: operator does, unless a successor is about to commit the same paths.
#: The event for a dead producer's staged batch whose stage retirement the
#: tick filed (#1053). Its origin files are kept: ``origin_kept`` is always
#: true, and ``superseded`` names the paths another attempt now claims.

#: Attempt states after which an attempt can never commit again: its owner
#: gate (`bl.require_live_owner`) refuses a claim it no longer holds.

#: Consumer states that hold a consumed batch and are reported as a stall:
#: the consumer ended without succeeding, or was declared and never
#: published, or its records could not be read.

#: Consumer states that no longer hold a consumed batch (#926): it
#: succeeded, an operator released its declaration, or a supersession
#: names a successor declared against the same batch, which holds it in
#: its place.

#: The states that mean a consumer has ended without succeeding, and so can
#: be superseded (`action_edges.file_supersession`) or released.

#: Reports the tick could not file on the batch itself (its records were
#: unreadable, or the step raised), keyed by batch coordinates. They are
#: logged once per change for the life of the process instead of on every
#: cycle; a restarted tier loop logs each one again once.
_UNFILED_REPORTS: dict[str, str] = {}


def _release_index_body(consumer: str, checked_ref: Mapping[str, str]) -> bytes:
    # Deterministic, so a second release of the same pair publishes the same
    # bytes and `_publish_immutable` accepts them.
    return core._compact_ascii_lf_bytes({"schema": oc.ORIGIN_CONSUMER_RELEASE_INDEX_SCHEMA_V1,
         "consumer_action_key": consumer, "ref": dict(checked_ref)})[:-1].decode("ascii").encode() + b"\n"


def _index_release(queue, consumer: str, checked_ref: Mapping[str, str]) -> None:
    """File the queue-wide index entry for one release (#954).

    Written before the release record, never after it: a release whose
    record is filed is then always visible to `pool.PoolQueue._claim`'s one
    listing. A crash between the two leaves an entry with no record, which
    `origin_consumer_release` reads as no release.
    """

    from prismabuild import pool as pool_mod

    path = queue.records.released_origin_consumer_path(consumer, checked_ref)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        queue.records.file_released_origin_index_at(
            path,
            _release_index_body(consumer, checked_ref),
        )
    except pool_mod.PoolContractError as exc:
        raise oc.ProducedOutputError(
            f"origin-consumer-release-conflict: {exc}") from None


def origin_consumer_release(queue, key: str) -> dict[str, object] | None:
    """The release of any batch ``key`` declared, or ``None`` (#954).

    What a claim asks before it runs a row for ``key``, and what the tier
    loop asks before it stages one. An action key seals its data manifest, so
    every row of a key reads the batches its declarations name; one release
    among them is enough. The index entries name the refs; each is confirmed
    by its release record, read through the same `bl.read_consumer_release` the
    retirement tick reads. An entry without a record is a release that
    stopped before its record, and does not count.

    Raises `ProducedOutputError` when an entry, its batch's scope or its
    record cannot be read: unknown is neither a release nor its absence.
    """

    from prismabuild import pool as pool_mod

    consumer = oc.output_key(key, where="consumer action key")
    directory = queue.records.released_origin_consumers_dir()
    try:
        names = queue.records.released_origin_index_names(consumer)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise oc.ProducedOutputError(
            f"unknown-retain: release index unreadable: {exc}") from None
    for name in names:
        try:
            body = pool_mod.record_store.read_document(directory / name)
        except (OSError, pool_mod.PoolContractError) as exc:
            raise oc.ProducedOutputError(
                f"unknown-retain: release index {name} unreadable: {exc}"
            ) from None
        if (not isinstance(body, Mapping)
                or body.get("schema") != oc.ORIGIN_CONSUMER_RELEASE_INDEX_SCHEMA_V1
                or body.get("consumer_action_key") != consumer):
            raise oc.ProducedOutputError(
                f"unknown-retain: release index {name} does not name "
                f"{consumer[:12]}")
        checked_ref = oc.checked_origin_ref(body.get("ref"))
        if queue.records.released_origin_consumer_path(consumer, checked_ref).name != name:
            raise oc.ProducedOutputError(
                f"unknown-retain: release index {name} names another batch")
        instance, _template = bl.origin_scope(queue.root, checked_ref)
        release = bl.read_consumer_release(queue.root, instance,
                                    checked_ref["batch_id"], consumer,
                                    checked_ref)
        if release is not None:
            return release
    return None


def release_origin_consumer(queue, ref: Mapping[str, object], *,
                            consumer_action_key: str, by: str,
                            reason: str = "",
                            live_runtime: str | Path | None = None,
                            cas_root: str | Path | None = None
                            ) -> dict[str, object]:
    """Release one consumer's declaration on one consumed batch (#926, #945).

    The operator's remedy for a declaration that can never succeed: a
    consumer that failed or was withdrawn and will not be run again under
    that key, or one that was declared and never published (#945). It files
    ``released-consumers/<batch_id>/<key>.json`` beside the declarations; the
    retirement tick then counts that consumer as resolved, and retires the
    batch once every other declared consumer has succeeded, with its usual
    identity checks. Nothing is deleted here. From then on the key cannot
    declare the batch again (`declare_origin_consumer`), so no later
    submission of it can reach the queue as a reader of the batch. A row an
    older ``pbrun`` publishes anyway, one that declared before the release,
    is refused where live code runs (#954): the release first files an entry
    in the queue-wide index (`origin_consumer_release`), the claim fails any
    ready row of the key with ``origin-consumer-released``, and the tier loop
    stages nothing for it.

    The consumer's transition lock is taken first, without waiting: a
    submitter holds it from its declarations through its row
    (`pbrun.submission_window`), so a consumer being submitted is refused
    (``origin-consumer-submitting``). Then, under the batch's output-prefix
    lock, the lock declarations and retirement take: the batch must be
    committed over the ref's digest, ``consumed``, and neither retiring nor
    reclaimed; the consumer must be declared against it; and its latest
    generation must be ``failed``, ``withdrawn`` or ``unpublished``. A
    consumer that is queued, claimed or being moved is refused
    (``origin-consumer-live``), and so is one whose records cannot be read.

    An ``unpublished`` consumer needs two more facts, read from ``cas_root``
    and the queue: the generation that sealed its request is not
    ``live_runtime`` (a submitter on the live generation clears the hold by
    submitting the key again), and no pinned, unpublished deferred release
    names it (`action_edges.pending_release_of`), because that release still
    publishes it. Without ``live_runtime`` and ``cas_root`` it is refused
    (``origin-consumer-unpublished-unknown``). Releasing the same consumer
    again finds its own record.

    Returns ``{"ok": True, "released": bool, "state": ...}``; raises
    `ProducedOutputError` naming the refusal.
    """

    checked_ref = oc.checked_origin_ref(ref)
    consumer = oc.output_key(consumer_action_key, where="consumer_action_key")
    instance, template = bl.origin_scope(queue.root, checked_ref)
    if not template.get("write_only"):
        # A read-back owner's origin batch (#1034) is its own; no other
        # action reads it, so its retirement need not wait for one.
        raise oc.ProducedOutputError(
            "origin-batch-not-write-only: its template reads its batches back")
    with queue.action_transition_lock(consumer, blocking=False) as owned:
        if not owned:
            raise oc.ProducedOutputError(
                f"origin-consumer-submitting: {consumer[:12]} is being "
                "submitted or moved; release it once that has finished")
        return _release_origin_consumer_locked(
            queue, instance, checked_ref, consumer, by=by, reason=reason,
            live_runtime=live_runtime, cas_root=cas_root)


def _release_origin_consumer_locked(
        queue, instance: Mapping[str, object], checked_ref: Mapping[str, str],
        consumer: str, *, by: str, reason: str,
        live_runtime: str | Path | None,
        cas_root: str | Path | None) -> dict[str, object]:
    """`release_origin_consumer` under the consumer's transition lock."""

    from prismabuild import pool as pool_mod

    batch_id = checked_ref["batch_id"]
    with queue.stage_ownership_lock(str(instance["output_prefix"])):
        commitments = bl.BatchLedger.for_instance(queue.root, instance).entries()
        entry = commitments.lookup(batch_id)
        if not isinstance(entry, bl.BatchFacets):
            raise oc.ProducedOutputError(f"origin-batch-uncommitted: {batch_id}")
        if (entry.kind != 'origin' or entry.manifest != checked_ref['manifest_digest']):
            raise oc.ProducedOutputError(
                "origin-batch-mismatch: the committed batch is not origin-only "
                "over this manifest digest")
        if entry.lifetime != oc.ORIGIN_LIFETIME_CONSUMED:
            raise oc.ProducedOutputError(
                f"origin-batch-retained: {batch_id} is never retired, so it has "
                "no declarations to release")
        if entry.origin_reclaimed:
            raise oc.ProducedOutputError(f"origin-batch-reclaimed: {batch_id}")
        if entry.is_retiring:
            raise oc.ProducedOutputError(f"origin-batch-retiring: {batch_id}")
        if consumer not in bl.declared_consumers(
                queue.root, instance, batch_id, checked_ref):
            raise oc.ProducedOutputError(
                f"origin-consumer-undeclared: {consumer[:12]} did not declare "
                f"{batch_id}")
        if bl.read_consumer_release(queue.root, instance, batch_id, consumer,
                             checked_ref) is not None:
            # A record filed before the index existed (#945's releases) gets
            # its entry here, so running the release again makes the claim
            # see it (#954).
            _index_release(queue, consumer, checked_ref)
            return {"ok": True, "released": False, "state": "released"}
        state = bl.consumer_state(queue, consumer)
        if state == "live":
            raise oc.ProducedOutputError(
                f"origin-consumer-live: {consumer[:12]} is queued, claimed or "
                "being moved; release it once it has failed or been withdrawn")
        sealed: dict[str, str] = {}
        if state == "unpublished":
            if live_runtime is None or cas_root is None:
                raise oc.ProducedOutputError(
                    f"origin-consumer-unpublished-unknown: {consumer[:12]} is "
                    "unpublished, and without the live runtime generation and "
                    "the CAS nothing shows that no submitter can publish it")
            sealed = _unpublished_release_evidence(
                queue, consumer, live_runtime=live_runtime, cas_root=cas_root)
        elif state not in oc.TERMINAL_CONSUMER_STATES:
            raise oc.ProducedOutputError(
                f"origin-consumer-not-terminal: {consumer[:12]} is {state}; "
                "only a failed, withdrawn or unpublished consumer can be "
                "released")
        publication = bl.ConsumerReleaseFile.prepare(
            queue.root, instance, batch_id, consumer, checked_ref,
            state=state, by=by, reason=reason, sealed=sealed)
        # The index first (#954). The record is what makes the release real
        # to the retirement tick; a record the claim's listing could not see
        # would let a row for this key run while the tick deletes its batch.
        _index_release(queue, consumer, checked_ref)
        publication.file()
    return {"ok": True, "released": True, "state": state}


def _unpublished_release_evidence(queue, consumer: str, *,
                                  live_runtime: str | Path,
                                  cas_root: str | Path) -> dict[str, str]:
    """Why an unpublished consumer's key cannot reach the queue (#945).

    A declaration with no row is a submission that stopped between its
    declaration and its row, or one still between them. The caller holds the
    consumer's transition lock, which every submitter holds across that
    window, so none is between them now. What could still publish the key is
    a submitter on the live generation, which clears the hold by submitting
    it again, or a pinned deferred release that resumes. This refuses both,
    and anything it cannot read, and returns what the release records: the
    wrapper that sealed the key.
    """

    from . import action_edges

    live_wrapper = str(Path(live_runtime) / "tools")
    try:
        wrapper = action_edges.request_wrapper(
            cas_root, consumer, where=f"sealed request {consumer[:12]}")
    except Exception as exc:                                  # noqa: BLE001
        raise oc.ProducedOutputError(
            f"origin-consumer-unpublished-unknown: {consumer[:12]} has no "
            f"readable sealed request: {exc}") from None
    if wrapper == live_wrapper:
        raise oc.ProducedOutputError(
            f"origin-consumer-unpublished-live: {consumer[:12]} was sealed by "
            "the live runtime generation, whose pbrun can still publish it; "
            "submit the same key again to clear the hold")
    try:
        pending = action_edges.pending_release_of(queue.root, consumer)
    except (OSError, action_edges.ActionEdgeError) as exc:
        raise oc.ProducedOutputError(
            f"origin-consumer-unpublished-unknown: deferred releases "
            f"unreadable: {exc}") from None
    if pending is not None:
        raise oc.ProducedOutputError(
            f"origin-consumer-release-pending: deferred release "
            f"{pending[:12]} pinned {consumer[:12]} and will publish it")
    return {"sealed_wrapper": wrapper}


def _resolved_consumers(queue, instance: Mapping[str, object], batch_id: str,
                        checked_ref: Mapping[str, str],
                        declared: Sequence[str]) -> list[dict[str, object]]:
    """Each declared consumer's state for retirement (#914, #926).

    A consumer's own state (`bl.consumer_state`) answers first while it is
    queued, running or has succeeded. A consumer that ended without
    succeeding is ``released`` when an operator released its declaration,
    and ``superseded`` when its supersessions (`pbrun --supersedes`, #913)
    lead to a key that is also declared against this batch: that successor
    holds the batch in its place, and must succeed. A supersession whose
    successor is not declared here yet, or not released yet, is named as
    ``superseded_by`` and changes nothing. Raises `ProducedOutputError` when
    a release record cannot be read.
    """

    from . import action_edges

    declared_set = set(declared)
    consumers: list[dict[str, object]] = []
    for key in declared:
        state = bl.consumer_state(queue, key)
        item: dict[str, object] = {"action_key": key, "state": state}
        if state == "unpublished":
            # Released only through `release_origin_consumer`'s proof that
            # nothing can publish the key (#945); nothing supersedes it.
            if bl.read_consumer_release(queue.root, instance, batch_id, key,
                                 checked_ref) is not None:
                item["state"] = "released"
        elif state in oc.TERMINAL_CONSUMER_STATES:
            if bl.read_consumer_release(queue.root, instance, batch_id, key,
                                 checked_ref) is not None:
                item["state"] = "released"
            else:
                try:
                    successor = action_edges.successor_of(queue.root, key)
                except action_edges.ActionEdgeError as exc:
                    item["supersession"] = f"unreadable: {exc}"
                    successor = None
                if successor is not None:
                    item["superseded_by"] = successor["id"]
                    if (successor["kind"] == action_edges.PRODUCER_KEY
                            and successor["id"] in declared_set):
                        item["state"] = "superseded"
        consumers.append(item)
    return consumers


def declare_origin_consumer(queue, ref: Mapping[str, object], *,
                            consumer_action_key: str) -> dict[str, object]:
    """File one consumer's declaration that it reads one origin batch (#914).

    ``pbrun`` calls this for each batch a consumer's data manifest declares,
    after the consumer's action key is sealed and before its row is
    published, so that no consumer can be queued that the retirement tick
    does not know about. Under the batch's output-prefix lock -- the lock
    retirement takes -- the batch must be committed over the ref's digest and
    neither retiring nor reclaimed. A ``consumed`` batch gets the file
    ``consumers/<batch_id>/<consumer_action_key>.json`` under its instance;
    the same consumer declaring again finds its own file and succeeds, unless
    an operator released its declaration (``origin-consumer-released``,
    #945). A ``retain`` batch is never retired, so nothing is filed for it.

    Returns ``{"ok": True, "declared": bool}``; raises `ProducedOutputError`
    naming the refusal.
    """

    from prismabuild import pool as pool_mod

    checked_ref = oc.checked_origin_ref(ref)
    consumer = oc.output_key(consumer_action_key, where="consumer_action_key")
    instance, template = bl.origin_scope(queue.root, checked_ref)
    if not template.get("write_only"):
        # A read-back owner's origin batch (#1034) is its own; no other
        # action reads it, so its retirement need not wait for one.
        raise oc.ProducedOutputError(
            "origin-batch-not-write-only: its template reads its batches back")
    batch_id = checked_ref["batch_id"]
    with queue.stage_ownership_lock(str(instance["output_prefix"])):
        commitments = bl.BatchLedger.for_instance(queue.root, instance).entries()
        entry = commitments.lookup(batch_id)
        if not isinstance(entry, bl.BatchFacets):
            raise oc.ProducedOutputError(f"origin-batch-uncommitted: {batch_id}")
        if (entry.kind != 'origin' or entry.manifest != checked_ref['manifest_digest']):
            raise oc.ProducedOutputError(
                "origin-batch-mismatch: the committed batch is not origin-only "
                "over this manifest digest")
        if entry.origin_reclaimed:
            raise oc.ProducedOutputError(f"origin-batch-reclaimed: {batch_id}")
        if entry.is_retiring:
            raise oc.ProducedOutputError(f"origin-batch-retiring: {batch_id}")
        if entry.lifetime != oc.ORIGIN_LIFETIME_CONSUMED:
            return {"ok": True, "declared": False}
        if bl.read_consumer_release(queue.root, instance, batch_id, consumer,
                             checked_ref) is not None:
            # A released key no longer holds the batch, so a row for it
            # could outlive the delete (#945).
            raise oc.ProducedOutputError(
                f"origin-consumer-released: {consumer[:12]}'s declaration of "
                f"{batch_id} was released; this key cannot read the batch "
                "again")
        bl.file_consumer_declaration(
            queue.root, instance, batch_id, consumer, checked_ref)
    return {"ok": True, "declared": True}


def dead_input_hint(queue, item: Mapping[str, object], hints) -> bool:
    """The pass's memoized gate: does this mover's owner attempt read dead?

    #1202 review N2: the gate the caller consults BEFORE taking the mover
    key's transition lock, so a live producer's movers pay no lock round
    trip at all. One ``RecordStore.standing`` read per distinct owner per pass
    (the caller's memo); the answer is only a gate -- a ``dead`` hint still
    pays for the full proof, which re-reads every fact fresh under the
    lock, never trusting the memo.  Anything unprovable answers False.
    """

    ref = item.get("produced_output_batch")
    if not isinstance(ref, Mapping):
        return False
    try:
        owner = oc.output_key(str(ref.get("owner_action_key") or ""),
                       where="reference owner_action_key")
        nonce = oc.output_nonce(str(ref.get("owner_nonce") or ""),
                       where="reference nonce")
    except oc.ProducedOutputError:
        return False
    generation = hints.get(owner)
    if generation is None:
        generation = hints[owner] = RecordStore(queue.root).standing(owner)
    return bl.attempt_state(generation, nonce) == "dead"


def dead_input_dependency(
        queue, item: Mapping[str, object]
) -> dict[str, object] | None:
    """The proof that a ready mover's producer-owned input is gone (#1184).

    A mover whose sealed row carries a ``produced_output_batch`` reference
    reads origin files only that reference's producer attempt can write.
    This answers the combined proof the terminalization needs: the instance
    the reference names (read from its immutable filed record, with every
    identity field cross-checked against the reference so a foreign or
    forged reference never authorizes reading another owner's state), the
    producer attempt provably dead (:func:`bl.producer_attempt_state` -- the
    only authority that makes a missing origin permanent), the batch's
    committed entry bound back to the reference by manifest digest with its
    sealed descriptors (``bl.BatchLedger.open`` -- every entry re-validated
    against the bound template and instance, never the commitments' path
    list read unvalidated), and one of those descriptors' origin paths
    under the producer's output prefix that is missing: ``lstat`` answered
    ``ENOENT`` (#1202 review: only FileNotFoundError is missing; EACCES,
    ESTALE and EIO are unreadable, and unreadable is unknown, never gone).
    The answer names the owner nonce and the missing path.

    ``hints`` is the caller's per-pass memo of owner generations (#1202
    review finding 5): one ``RecordStore.standing`` read per distinct owner per
    pass, so a fleet of movers of one live producer pays one ``claimed/``
    census instead of two per mover. The hint is only a gate -- a
    non-``dead`` hint skips the mover entirely this pass (a producer that
    dies mid-pass is caught on the next one), and a ``dead`` hint still pays
    for the full proof, which re-reads the state fresh under the caller's
    lock, never trusting the memo.

    Anything unprovable -- a malformed or foreign reference, an unreadable
    instance, commitments or batch record, a live, succeeded or unknown
    producer, a batch committed at origin, an unreadable origin path, or a
    batch whose origin paths all still exist -- answers ``None``: ENOENT,
    poll counts and unknown state alone invent no authority (#1184), and
    the row stays ready.  Never raises for queue-state reasons.
    """

    from prismabuild import pool as pool_mod

    ref = item.get("produced_output_batch")
    if not isinstance(ref, Mapping):
        return None
    if ref.get("schema") != pool_mod.PRODUCED_OUTPUT_BATCH_REF_SCHEMA_V1:
        return None
    try:
        owner = oc.output_key(str(ref.get("owner_action_key") or ""),
                       where="reference owner_action_key")
        template_id = oc.output_name(str(ref.get("template_id") or ""),
                            where="reference template_id")
        nonce = oc.output_nonce(str(ref.get("owner_nonce") or ""),
                       where="reference nonce")
        scope_id = oc.output_name(str(ref.get("owner_scope_id") or ""),
                         where="reference scope_id")
        manifest_digest = oc.output_key(str(ref.get("manifest_digest") or ""),
                                 where="reference manifest_digest")
        batch_id = oc.output_name(str(ref.get("batch_id") or ""),
                         where="reference batch_id")
    except oc.ProducedOutputError:
        return None
    try:
        raw = json.loads(
            bl.BatchLedger.at_scope(bl.attempt_scope(
                queue.root, owner, template_id, nonce)).instance_path.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(raw, Mapping):
        return None
    try:
        instance = oc.validate_instance(dict(raw))
        template = oc.validate_template(json.loads(
            (Path(queue.root) / "residency" / oc.OUTPUT_TEMPLATES_SUBDIR
             / f"{instance['template_id']}.json").read_text()))
    except (OSError, ValueError, oc.ProducedOutputError):
        return None
    attempt = instance["owner_attempt"]
    assert isinstance(attempt, dict)
    if (str(instance["owner_action_key"]) != owner
            or str(instance["template_id"]) != template_id
            or str(attempt["nonce"]) != nonce
            or str(attempt["scope_id"]) != scope_id
            or oc.template_sha256(template) != str(instance["template_sha256"])):
        return None          # the reference does not name this instance
    state = bl.producer_attempt_state(queue, instance)
    if state != "dead":
        return None
    try:
        commitments = bl.BatchLedger.for_instance(queue.root, instance).entries()
        entry = commitments.lookup(batch_id)
        if (not isinstance(entry, bl.BatchFacets) or str(entry.manifest) != manifest_digest or entry.kind == 'origin'):
            return None
        _filed, sealed = entry.open(queue.root, instance, template, batch_id)
    except (oc.ProducedOutputError, OSError, ValueError):
        return None
    prefix = str(instance["output_prefix"]).rstrip(os.sep) + os.sep
    for path in sorted(str(each["path"]) for each in sealed):
        if not path.startswith(prefix):
            continue         # only the producer's namespace is its to lose
        try:
            os.lstat(path)
        except FileNotFoundError:
            return {
                "owner_action_key": owner,
                "owner_nonce": nonce,
                "owner_scope_id": scope_id,
                "template_id": template_id,
                "template_sha256": str(instance["template_sha256"]),
                "batch_id": batch_id,
                "manifest_digest": manifest_digest,
                "tier_id": str(ref.get("tier_id") or ""),
                "path": path,
                "producer_state": state,
            }
        except OSError:
            # Unreadable is unknown, never missing (#1202 review finding 2):
            # EACCES, ESTALE and EIO all mean the answer cannot be read, not
            # that the file is gone.
            return None
    return None


def _fsync_directories(paths: Sequence[str]) -> None:
    """Make the unlinks of these paths durable before the charge is freed."""

    for directory in sorted({os.path.dirname(path) for path in paths}):
        try:
            handle = os.open(directory, os.O_RDONLY)
        except OSError:
            continue
        try:
            os.fsync(handle)
        except OSError:
            pass
        finally:
            os.close(handle)


def _retire_consumed_batch(queue, instance: Mapping[str, object],
                           template: Mapping[str, object],
                           batch_id: str, *,
                           deferred_holds: Collection[tuple[str, str]] = (),
                           reads: bl.LedgerReads | None = None
                           ) -> dict[str, object] | None:
    """One retirement step for one consumed batch; the event to log, or None.

    Every decision and every delete happens under the batch's output-prefix
    lock, the lock `declare_origin_consumer` takes, so no consumer can be
    declared between the decision and the delete, and under the lock of
    every filed template whose prefix overlaps this one's
    (`bl.path_locks`, #1063).  The one thing done outside them is
    reading an origin whose timestamps alone moved (below).

    The decision is taken from the tick's own read of this scope, under the
    locks (`bl.LedgerReads.batches`, #992): it is re-read there whenever the
    file's trusted version moved, so a write made before the locks were
    taken answers, and an unchanged document is parsed once per tick rather
    than once per due batch.  Every write of the document first drops the
    memo (`bl.LedgerReads.forget`), so a decision a failed write did not file
    can never answer the next batch.

    The decision, unless the batch is already ``retiring``:

    * With declared consumers: every one must have ``succeeded``. A live one
      holds the batch quietly. A failed, withdrawn, unpublished or unknown
      one holds it and is reported as a stall, once per change of the
      consumers' states, because a retry of that consumer needs the batch.
    * With none: the batch is an orphan once its producer attempt is dead
      (`bl.attempt_state` over the tick's one read of the owner key,
      `bl.LedgerReads.generation`, #977). Otherwise it waits, quietly, for a
      consumer. A read-back template's batch (#1034) can have no consumer,
      so it is an orphan once its owner attempt has ended, dead or
      succeeded.
    * Either way, a consumer filed with ``pbrun --after`` and not yet
      released (#913) holds it, quietly: ``deferred_holds`` is
      `action_edges.held_producer_batches`, and a batch it names waits for
      that consumer to be released and declared.

    The delete checks the output prefix first: a prefix that is not a
    directory here means the file system is not mounted on this host, and
    an absent file would prove nothing. Each origin is then compared with
    the identity its commit recorded:

    * the same file is deleted;
    * an absent file is already gone;
    * a file with another inode is not this batch's any more (a retried
      producer attempt wrote the path again, #912's per-attempt ownership),
      so it is left alone and the batch stops charging for it;
    * the same inode changed in place, or any unreadable stat, refuses and
      keeps the batch -- except when only its timestamps moved (an NFS
      delegation recall, #1111): the file is read and hashed outside the
      lock, and when its digest is the committed one the retirement takes
      the lock again, re-stats it, files the re-pin on the entry
      (``origin_repins``) and deletes it as the committed file.

    Another attempt's claim is read under the same locks first (#1053),
    over every attempt of every template whose prefix overlaps this one's
    (`bl.LedgerReads.path_owners`, with the templates listed again under the
    locks): a path another attempt has committed is that batch's, and is
    left alone as ``superseded`` even when its identity is still the
    recorded one -- a direct commit adopts the file it finds, inode and
    all -- and a path a prewrite of an attempt that can still commit names
    holds the batch until that attempt commits or ends: quietly while it is
    live, and while any holder's state is unknown with
    `ORIGIN_HELD_BY_UNKNOWN_EVENT`, once per change, naming each such
    holder, why it is unknown and whether it is orphaned (#1065).  Every
    prewrite and commit of those templates takes one of these locks, so
    none can claim a path between this read and the delete (#1063).

    Before the first unlink the entry is marked ``retiring``: consumers can
    no longer declare it, and a crash resumes the delete rather than
    deciding again. Each delete is `_unlink_if_committed`, which moves the
    name aside and deletes only the file it moved if that is the committed
    one, so a writer's ``rename`` onto the name that lands at any point is
    never deleted. Afterwards ``origin_reclaimed`` frees its durable class
    bytes and its paths, as `BatchLedger.reclaim` does.
    """

    verified: dict[str, dict[str, object]] = {}
    for _attempt in (1, 2):
        outcome = _retire_consumed_batch_locked(
            queue, instance, template, batch_id,
            deferred_holds=deferred_holds, reads=reads, verified=verified)
        if not isinstance(outcome, _OriginsToVerify):
            return outcome
        # With no lock held: one read of each origin whose timestamps alone
        # moved.  A refusal is kept as one, and the locked pass reports it.
        for path, (published, observed, sha256) in outcome.pending.items():
            verified[path] = bl.retirement_origin_proof(
                path, published, observed, sha256, where="retire")
    # An origin moved again between its read and the lock: the next tick
    # decides, from what is there then.
    return None


class _OriginsToVerify:
    """Origins a retirement must read, outside its lock, before it decides (#1111)."""

    def __init__(self, pending: dict[str, tuple[Mapping[str, object],
                                                 Mapping[str, object], str]]):
        self.pending = pending


def _retire_consumed_batch_locked(
        queue, instance: Mapping[str, object], template: Mapping[str, object],
        batch_id: str, *, deferred_holds: Collection[tuple[str, str]] = (),
        reads: bl.LedgerReads | None = None,
        verified: Mapping[str, Mapping[str, object]]
        ) -> dict[str, object] | None | _OriginsToVerify:
    """One pass of `_retire_consumed_batch` under the output-prefix locks."""

    from prismabuild import reader_lease as lease_mod

    tick = reads if reads is not None else bl.LedgerReads(queue, lease_timeout_s=pool.LEASE_TIMEOUT_S)
    commitments_path = bl.BatchLedger.for_instance(queue.root, instance).path
    try:
        # This prefix's lock and every overlapping template's (#1063), from
        # the tick's listing: every scope the tick visits was declared, and
        # so its template filed, before the tick listed the templates.
        locks = bl.path_locks(
            queue, instance,
            templates=tick.templates() if reads is not None else None,
            reads=tick if reads is not None else None)
    except oc.ProducedOutputError as exc:
        return _unfiled_report(instance, batch_id, {
            "event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT, "reason": str(exc)})
    with locks:
        try:
            # The tick's own parse while the file's trusted version holds
            # (#992): unchanged commitments are parsed once per tick, not
            # once per due batch.  The stat and the read are under these
            # locks, so a writer they exclude cannot land between them; a
            # writer that landed before them moved the version and is read.
            batches = tick.entries(commitments_path.parent)
        except oc.ProducedOutputError as exc:
            return _unfiled_report(instance, batch_id, {
                "event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT,
                "reason": str(exc)})

        entry = batches.lookup(batch_id)
        if (not isinstance(entry, bl.BatchFacets) or entry.kind != "origin"
                or entry.lifetime != oc.ORIGIN_LIFETIME_CONSUMED
                or entry.origin_reclaimed):
            return None
        entry = batches.retirement(batch_id, reads=tick)
        ref = oc.origin_batch_ref(instance, batch_id=batch_id, manifest_digest=str(entry.required_manifest))
        try:
            total = entry.origin_charge(batch_id)
        except oc.ProducedOutputError as exc:
            return _unfiled_report(instance, batch_id, {
                "event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT, "ref": ref,
                "reason": str(exc)})
        base = {"ref": ref, "bytes": total}

        def report(event: dict[str, object]) -> dict[str, object] | None:
            # Once per change: the entry remembers the last report it made.
            if not entry.retirement_report(event):
                return None
            entry.file()
            return event

        def quiet() -> None:
            # Nothing to report now; a later stall reports again.
            if entry.quiet_retirement():
                entry.file()

        try:
            retiring = entry.delete_intent
        except oc.ProducedOutputError as exc:
            return report({"event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                           "reason": str(exc)})
        if retiring is not None:
            reason, consumers = retiring.reason, retiring.consumers
        elif _held_for_deferred(instance, deferred_holds):
            quiet()
            return None
        else:
            checked_ref = oc.checked_origin_ref(ref)
            try:
                declared = bl.declared_consumers(
                    queue.root, instance, batch_id, checked_ref)
            except oc.ProducedOutputError as exc:
                return report({"event": oc.ORIGIN_RETIREMENT_STALLED_EVENT, **base,
                               "consumers": [], "reason": str(exc)})
            if declared:
                try:
                    consumers = _resolved_consumers(
                        queue, instance, batch_id, checked_ref, declared)
                except oc.ProducedOutputError as exc:
                    return report({"event": oc.ORIGIN_RETIREMENT_STALLED_EVENT,
                                   **base, "consumers": [], "reason": str(exc)})
                if not all(item["state"] in oc.RESOLVED_CONSUMER_STATES
                           for item in consumers):
                    if any(item["state"] in oc.STALLED_CONSUMER_STATES
                           for item in consumers):
                        return report({"event": oc.ORIGIN_RETIREMENT_STALLED_EVENT,
                                       **base, "consumers": consumers})
                    quiet()
                    return None
                reason = "consumed"
            else:
                # A write-only batch waits for a consumer while its producer
                # lives or has succeeded. A read-back one (#1034) can have no
                # consumer (`declare_origin_consumer` refuses it), so its
                # owner attempt ending, by success too, ends it.
                # The owner key's one read this tick (#977): every due
                # batch of the instance asks, and a running producer that
                # commits ahead of its consumers has many.  Read before the
                # lock, which is safe because an attempt that ended, dead or
                # succeeded, never runs again under its nonce; a stale
                # ``live`` only waits for the next cycle.
                ended = ({"dead"} if template.get("write_only")
                         else oc.ENDED_ATTEMPT_STATES)
                attempt = instance["owner_attempt"]
                assert isinstance(attempt, dict)
                if bl.attempt_state(tick.generation(
                        str(instance["owner_action_key"])),
                        str(attempt["nonce"])) not in ended:
                    quiet()
                    return None
                reason, consumers = "orphan", []

        # The delete.  First prove this host sees the output prefix.
        try:
            prefix_mode = os.stat(str(instance["output_prefix"])).st_mode
        except OSError as exc:
            return report({"event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                           "reason": "output-prefix-unreachable",
                           "detail": str(exc)})
        if not stat.S_ISDIR(prefix_mode):
            return report({"event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                           "reason": "output-prefix-unreachable",
                           "detail": "not a directory on this host"})
        try:
            filed, sealed = entry.open(queue.root, instance, template, batch_id)
        except oc.ProducedOutputError as exc:
            return report({"event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                           "reason": str(exc)})
        recorded = filed.get("origin_identity")
        if (not isinstance(recorded, Mapping)
                or set(recorded) != {str(desc["path"]) for desc in sealed}):
            return report({"event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                           "reason": "origin-proof-missing"})
        # The committed identity with the entry's re-pins applied (#1111).
        try:
            recorded = entry.effective_origins(filed, sealed)
        except oc.ProducedOutputError as exc:
            return report({"event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                           "reason": str(exc)})
        assert recorded is not None
        digests = {str(desc["path"]): desc.get("sha256") for desc in sealed}
        observed: dict[str, dict[str, object]] = {}
        repinned: list[dict[str, object]] = []

        def classify(path: str) -> tuple[str, str]:
            try:
                live = bl.portable_origin_identity(os.lstat(path))
            except FileNotFoundError:
                return ("absent", "")
            except OSError as exc:
                return ("refuse", f"origin-unstatable: {exc}")
            observed[path] = live
            if lease_mod.file_id_matches(recorded[path], live):
                return ("unlink", "")
            if (isinstance(digests.get(path), str)
                    and lease_mod.timestamp_only_mismatch(recorded[path], live)):
                # A delegation recall: settled by the content, which is read
                # outside this lock (`_retire_consumed_batch`).
                got = verified.get(path)
                if got is None:
                    return ("verify", "")
                if "refused" in got:
                    return ("refuse", str(got["reason"]))
                if (got.get("from") == recorded[path]
                        and lease_mod.file_id_matches(got.get("to"), live)):
                    recorded[path] = dict(got["to"])
                    repinned.append(dict(got))
                    return ("unlink", "")
                return ("verify", "")
            if live.get("ino") != recorded[path].get("ino"):
                return ("superseded", "")
            return ("refuse", "origin-changed")

        def to_verify(paths_: Collection[str]) -> _OriginsToVerify:
            return _OriginsToVerify({
                path: (dict(recorded[path]), dict(observed[path]),
                       str(digests[path])) for path in paths_})

        def file_repins() -> None:
            # On the entry, in the same write as the decision it supports.
            entry.retirement_repins(repinned)

        paths = sorted(str(desc["path"]) for desc in sealed)
        # Another attempt's claim on these paths, read under these locks,
        # over the templates as they are filed now (#1063).
        try:
            owners = tick.path_owners(instance, template, relist=True)
        except (oc.ProducedOutputError, OSError, ValueError) as exc:
            return report({"event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                           "reason": f"unknown-retain: path owners: {exc}"})
        claimed = {path: note for path in paths
                   if (note := owners.committed(path)) is not None}
        held = [path for path in paths
                if path not in claimed and owners.pending(path) is not None]
        if held:
            # A live writer's hold is quiet; one whose state is unknown is
            # named, once per change, and so is an orphaned one (#1065).
            unknown = [holder for path in held
                       for holder in owners.unknown_holders(path)]
            if unknown:
                return report({"event": oc.ORIGIN_HELD_BY_UNKNOWN_EVENT, **base,
                               **bl.unknown_holder_report(unknown, queue.root)})
            quiet()
            return None
        pending: list[str] = []
        for path in paths:
            verdict, why = classify(path)
            if verdict == "refuse" and path not in claimed:
                return report({"event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                               "reason": why, "path": path})
            if verdict == "verify" and path not in claimed:
                pending.append(path)
        if pending:
            # Not a refusal and nothing is reported: the caller reads them
            # without this lock and comes back.
            return to_verify(pending)
        if not entry.is_retiring:
            file_repins()
            entry.begin_origin_retirement(
                reason=reason, consumers=consumers)
            entry.file()
        elif repinned:
            file_repins()
            entry.file()
        unlinked: list[str] = []
        superseded: list[str] = []
        absent: list[str] = []
        tag = hashlib.sha256(_batch_report_key(
            instance, batch_id).encode()).hexdigest()[:16]
        for path in paths:
            # A private name an interrupted delete left is settled first,
            # whoever owns the path now: it may hold another writer's file.
            outcome, why = _settle_retiring_leftover(path, recorded[path], tag)
            if outcome == "refuse":
                return report({"event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT,
                               **base, "reason": why, "path": path})
            if outcome == "unlinked":
                unlinked.append(path)
                continue
            if path in claimed:
                # Another attempt committed it: that batch's file, whatever
                # its identity (#1053).
                superseded.append(path)
                continue
            # Checked again at the unlink, not only above: the name is
            # removed only while it is still the committed file.
            verdict, why = classify(path)
            if verdict == "refuse":
                return report({"event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                               "reason": why, "path": path})
            if verdict == "verify":
                # Its timestamps moved again since the check above.  The
                # entry is ``retiring``, so the next pass resumes the delete.
                return to_verify([path])
            if repinned:
                file_repins()
                entry.file()
            if verdict == "unlink":
                outcome, why = _unlink_if_committed(path, recorded[path], tag)
                if outcome == "refuse":
                    return report({"event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT,
                                   **base, "reason": why, "path": path})
                if outcome == "unlinked":
                    unlinked.append(path)
                elif outcome == "superseded":
                    superseded.append(path)
                else:
                    absent.append(path)
            elif verdict == "superseded":
                superseded.append(path)
            else:
                absent.append(path)
        _fsync_directories(unlinked)
        entry.complete_origin_retirement()
        entry.file()
    _UNFILED_REPORTS.pop(_batch_report_key(instance, batch_id), None)
    event = {"event": oc.ORIGIN_RETIRED_EVENT, **base, "reason": reason,
             "consumers": consumers, "origin_identity": dict(recorded),
             "unlinked": unlinked, "superseded": superseded, "absent": absent}
    if claimed:
        event["superseded_by"] = claimed
    return event


def _retiring_name(path: str, tag: str) -> str:
    """The private name `_unlink_if_committed` moves ``path`` to."""

    directory, name = os.path.split(path)
    return os.path.join(directory, f".{name}.pb-retiring-{tag}")


def _is_committed_file(info: os.stat_result,
                       recorded: Mapping[str, object]) -> bool:
    """Whether ``info`` is the file a commit recorded, at any name.

    Inode, size and mtime: a rename changes only ctime. A recorded identity
    without all three is never a match.
    """

    live = bl.portable_origin_identity(info)
    fields = ("ino", "size", "mtime_ns")
    return (all(recorded.get(field) is not None for field in fields)
            and all(live.get(field) == recorded.get(field) for field in fields))


def _settle_retiring_leftover(path: str, recorded: Mapping[str, object],
                              tag: str, *, dir_fd: int | None = None
                              ) -> tuple[str, str]:
    """Finish what an interrupted `_unlink_if_committed` left at its private name.

    Returns ``("none", "")`` when there is no private name. Otherwise the
    private file is the committed one, which is deleted (``unlinked``), or a
    writer's file the interrupted call moved aside:

    * if ``path`` is that same file again (the call linked it back and
      stopped before removing the private name), the private name goes;
    * if ``path`` is free, the file is linked back and the private name
      goes;
    * if ``path`` holds a later file, the private one is kept and refused
      as ``origin-displaced``, naming it for an operator.

    The last two return ``superseded``; any unreadable step refuses.
    ``dir_fd`` is `_unlink_if_committed`'s.
    """

    private = _retiring_name(path, tag)
    try:
        try:
            left = os.stat(private, dir_fd=dir_fd, follow_symlinks=False)
        except FileNotFoundError:
            return ("none", "")
        if _is_committed_file(left, recorded):
            os.unlink(private, dir_fd=dir_fd)
            return ("unlinked", "")
        try:
            there = os.stat(path, dir_fd=dir_fd, follow_symlinks=False)
        except FileNotFoundError:
            there = None
        if there is not None and there.st_ino == left.st_ino:
            os.unlink(private, dir_fd=dir_fd)
            return ("superseded", "")
        if there is not None:
            return ("refuse", f"origin-displaced: another writer's file is "
                              f"kept at {private}")
        return _link_back(path, private, dir_fd=dir_fd)
    except OSError as exc:
        return ("refuse", f"origin-unlink: {private}: {exc}")


#: `renameat2`'s flag: refuse with ``EEXIST`` instead of replacing the target.
_RENAME_NOREPLACE = 1
#: `renameat2`'s directory for a path that is not resolved through a descriptor.
_AT_FDCWD = -100
#: libc's ``renameat2``, looked up on first use; ``[None]`` when there is none.


def _rename_noreplace(source: str, target: str, *,
                      dir_fd: int | None = None) -> None:
    """``rename(source, target)`` that never replaces a file at ``target`` (#1064).

    Linux ``renameat2`` with ``RENAME_NOREPLACE``: one atomic step, which
    fails with ``EEXIST`` when ``target`` exists. Unlike a hard link it needs
    no ownership of the file, only write access to the directory, which the
    move aside already used. With ``dir_fd`` both names resolve through that
    descriptor. Raises ``FileExistsError`` when ``target`` exists, and
    ``OSError`` when the file system does not offer it (``EINVAL``: NFS
    rejects every ``renameat2`` flag) or libc has no ``renameat2``
    (``ENOSYS``).
    """

    import ctypes

    if not _RENAMEAT2:
        try:
            function = ctypes.CDLL(None, use_errno=True).renameat2
        except (AttributeError, OSError):
            function = None
        else:
            function.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int,
                                 ctypes.c_char_p, ctypes.c_uint]
            function.restype = ctypes.c_int
        _RENAMEAT2.append(function)
    function = _RENAMEAT2[0]
    if function is None:
        raise OSError(errno.ENOSYS, "libc has no renameat2", source)
    directory = _AT_FDCWD if dir_fd is None else dir_fd
    if function(directory, os.fsencode(source), directory, os.fsencode(target),
                _RENAME_NOREPLACE) != 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code), source, None, target)


def _link_back(path: str, private: str, *, dir_fd: int | None = None
               ) -> tuple[str, str]:
    """Put a writer's file moved to ``private`` back at ``path``, never over one.

    A hard link, then the private name is removed. The kernel refuses the
    link with ``EPERM`` when ``fs.protected_hardlinks`` is 1 and the caller
    neither owns the file nor can write it -- dl380g10 sets it, and its tier
    loop runs as ``rob``, which need not own a producer's file -- and on a
    file system without hard links. The file is then renamed back with
    `_rename_noreplace`, which needs no ownership (#1064). Either way a file
    already at ``path`` is never replaced: the writer's file is kept at
    ``private`` and refused as ``origin-displaced``, and so is one that
    neither can put back, with both errors named.
    """

    displaced = f"origin-displaced: another writer's file is kept at {private}"
    try:
        os.link(private, path, src_dir_fd=dir_fd, dst_dir_fd=dir_fd,
                follow_symlinks=False)
    except FileExistsError:
        return ("refuse", displaced)
    except PermissionError as link_refused:
        try:
            _rename_noreplace(private, path, dir_fd=dir_fd)
        except FileExistsError:
            return ("refuse", displaced)
        except OSError as rename_refused:
            return ("refuse", f"{displaced}: neither a link ({link_refused}) "
                              f"nor a rename that never replaces "
                              f"({rename_refused}) can put it back")
        return ("superseded", "")
    os.unlink(private, dir_fd=dir_fd)
    return ("superseded", "")


def _unlink_if_committed(path: str, recorded: Mapping[str, object],
                         tag: str, *, dir_fd: int | None = None
                         ) -> tuple[str, str]:
    """Delete the file at ``path`` only if it is the one a commit recorded.

    The only delete of an origin file PB makes (#1053). A plain ``unlink``
    after an identity check removes whatever is at the name when it runs,
    and a producer writes by ``rename(tmp, path)`` under no PB lock, so a
    rename landing between the check and the unlink would lose its file.
    Instead:

    1. ``rename(path, private)``, where ``private`` is ``.<name>.pb-retiring-
       <tag>`` in the same directory (`_retiring_name`). The name is now
       free, and a writer's rename that lands from here on creates a new
       file at ``path``;
    2. ``lstat(private)``: if its inode, size and mtime are the recorded
       ones (a rename changes only ctime), it is the committed file, and it
       is unlinked -- under the private name, which no writer uses;
    3. otherwise a writer's rename landed before step 1, and this moved its
       file aside: it is put back at ``path`` (`_link_back`: a hard link and
       the private name removed, or, when the link is refused, a rename that
       never replaces; the same inode either way, so its bytes never moved).
       If ``path`` is taken again by then, the private file is kept and the
       refusal ``origin-displaced`` names it for an operator.

    Steps 1 and 3 move that writer's file's ctime, which
    `reader_lease.file_id_matches` compares. `origin_retirement_tick` holds
    the output-prefix locks across them, and a commit takes its identity
    under locks that share one with them (#1064, #1063), so a commit of that
    file under any template whose prefix contains it records the identity
    it has after the put-back.

    So a writer's file is never deleted, wherever its rename lands. A call
    interrupted between the steps leaves the private name, and the caller
    settles it before anything else (`_settle_retiring_leftover`). What
    this does not cover, and says so: a writer that rewrites the committed
    inode in place (``open`` and ``write`` with no rename) is outside the
    produced-output contract, which writes every origin file by rename;
    between steps 1 and 3 a reader of ``path`` can find it absent; and a
    file this process may not link, on a file system without
    ``RENAME_NOREPLACE`` (NFS), refuses at step 3 and keeps the file at the
    private name, named in the refusal.

    Returns ``(outcome, reason)``: ``unlinked``, ``absent`` (nothing was at
    the name), ``superseded`` (another file was, and is again) or
    ``refuse``.  With ``dir_fd``, ``path`` is a name in that pinned
    directory, and every step resolves through the descriptor: the produced
    spool's export retires a failed group's file that way (#1097).
    """

    private = _retiring_name(path, tag)
    try:
        try:
            os.rename(path, private, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        except FileNotFoundError:
            return ("absent", "")
        if _is_committed_file(os.stat(private, dir_fd=dir_fd, follow_symlinks=False),
                              recorded):
            os.unlink(private, dir_fd=dir_fd)
            return ("unlinked", "")
        return _link_back(path, private, dir_fd=dir_fd)
    except OSError as exc:
        return ("refuse", f"origin-unlink: {private}: {exc}")


def _held_for_deferred(instance: Mapping[str, object],
                       holds: Collection[tuple[str, str]]) -> bool:
    """Whether an unreleased deferred consumer may read this batch (#913)."""

    return (str(instance["owner_action_key"]),
            str(instance["template_id"])) in holds


def _batch_report_key(instance: Mapping[str, object], batch_id: str) -> str:
    attempt = instance["owner_attempt"]
    assert isinstance(attempt, dict)
    return (f"{instance['owner_action_key']}/{instance['template_id']}."
            f"{attempt['nonce']}/{batch_id}")


def _unfiled_report(instance: Mapping[str, object], batch_id: str,
                    event: dict[str, object]) -> dict[str, object] | None:
    """Report once per change what cannot be remembered on the batch itself."""

    event = {**event, "batch": _batch_report_key(instance, batch_id)}
    signature = hashlib.sha256(digest_primitives.sorted_json(event).encode()).hexdigest()
    key = _batch_report_key(instance, batch_id)
    if _UNFILED_REPORTS.get(key) == signature:
        return None
    _UNFILED_REPORTS[key] = signature
    return event




def _ended_prewrite_dispositions(
        queue, instance: Mapping[str, object], template: Mapping[str, object],
        records: Mapping[str, bl.PrewriteFacts],
        reads: bl.LedgerReads,
) -> dict[str, dict[str, object]]:
    """What each outstanding prewrite of an ended attempt is now (#949, #1053).

    ``records`` maps batch ids to their prewrite records. The caller has read
    that the attempt ended (`bl.attempt_state` is ``dead`` or ``succeeded``):
    its owner gate refuses a claim it no longer holds, so nothing can commit
    these prewrites. Their planned paths decide:

    * all absent: ``reclaim``, reason ``absent``. Nothing durable is left,
      the rule `abort_prewrite` applies.
    * each present one is committed by another attempt, of this key or of
      any other action whose template's prefix overlaps this one's
      (`bl.LedgerReads.path_owners`): ``reclaim``, reason ``superseded``. A
      retry or a relaunch wrote those files again and committed them, so
      they are that batch's, charged once, there. A prewrite of an attempt
      that succeeded counts as its commit.
    * any present one is still planned by another attempt that can commit,
      and none belongs to nobody: ``hold``, until that attempt commits or
      ends. Its ``unknown`` lists the holders whose state is unknown
      (`bl.RetirementOwners.unknown_holders`), which the sweep reports (#1065).
    * otherwise ``orphaned``: files that belong to no batch. PB never
      deletes a file whose identity no commit recorded, so an operator
      removes them, and then the reservation is reclaimed as ``absent``.

    The output prefix must be a directory on this host first, or an absent
    file proves nothing: ``refused``, reason ``output-prefix-unreachable``.
    State that cannot be read is ``refused``, reason ``unknown-retain``.
    The prefix is statted once and the other attempts' paths are read at
    most once, for all the records.
    """

    try:
        reachable = stat.S_ISDIR(os.stat(str(instance["output_prefix"])).st_mode)
        detail = "not a directory on this host"
    except OSError as exc:
        reachable, detail = False, str(exc)
    if not reachable:
        return {batch_id: {"action": "refused",
                           "reason": "output-prefix-unreachable",
                           "detail": detail}
                for batch_id in records}
    owners: bl.RetirementOwners | None = None
    owners_error: str | None = None
    decided: dict[str, dict[str, object]] = {}
    for batch_id, record in records.items():
        present: list[str] = []
        try:
            for path in record.paths:
                try:
                    os.lstat(str(path))
                except FileNotFoundError:
                    continue
                present.append(str(path))
        except OSError as exc:
            decided[batch_id] = {"action": "refused", "reason": "unknown-retain",
                                 "detail": str(exc)}
            continue
        if not present:
            decided[batch_id] = {"action": "reclaim", "reason": "absent",
                                 "superseded": []}
            continue
        if owners is None and owners_error is None:
            try:
                owners = reads.path_owners(instance, template)
            except (oc.ProducedOutputError, OSError, ValueError) as exc:
                owners_error = str(exc)
        if owners is None:
            decided[batch_id] = {"action": "refused", "reason": "unknown-retain",
                                 "detail": str(owners_error)}
            continue
        superseded: list[dict[str, str]] = []
        held: list[str] = []
        orphaned: list[str] = []
        unknown: list[dict[str, object]] = []
        for path in present:
            note = owners.committed(path)
            if note is not None:
                superseded.append({"path": path, **note})
            elif owners.pending(path) is not None:
                held.append(path)
                unknown.extend(owners.unknown_holders(path))
            else:
                orphaned.append(path)
        if orphaned:
            decided[batch_id] = {"action": "orphaned", "paths": orphaned,
                                 "superseded": superseded, "held": held,
                                 "owners": owners.fingerprint}
        elif held:
            # ``unknown``: the holders whose state could not be read, which
            # the sweep names (#1065); a live holder's hold is quiet.
            decided[batch_id] = {"action": "hold", "owners": owners.fingerprint,
                                 "unknown": unknown}
        else:
            decided[batch_id] = {"action": "reclaim", "reason": "superseded",
                                 "superseded": superseded}
    return decided


def _prepaid_intent_names(reads: bl.LedgerReads, instance: Mapping[str, object],
                          template: Mapping[str, object], batch_id: str,
                          tier: str) -> bool | None:
    """Whether a pool funding intent still names this prewrite's batch.

    The prewrite is that intent's precommit recovery authority
    (`abort_prewrite` refuses while one exists), so it is not reclaimed
    under it. ``None`` when the owner's funding census cannot be read.
    """

    intents, unknown = reads.census(str(instance["owner_action_key"]))
    if unknown:
        return None
    attempt = instance["owner_attempt"]
    assert isinstance(attempt, dict)
    digest = oc.template_sha256(template)
    return any(str(record.get("batch_id")) == batch_id
               and str(record.get("tier_id")) == tier
               and str(record.get("template_sha256")) == digest
               and str(record.get("owner_nonce")) == str(attempt["nonce"])
               and str(record.get("owner_scope_id")) == str(attempt["scope_id"])
               for record in intents)


#: What the tick last left in place for an ended attempt's prewrite (#1053),
#: by the prewrite's report key: ``(inputs, action)``. ``action`` is
#: ``orphaned``, or ``hold`` for a path another attempt still plans;
#: ``inputs`` is what it was decided on -- the record's version, the
#: version of each directory its paths are in (a file created, removed or
#: renamed there changes it), and the other attempts' claims
#: (`bl.RetirementOwners.fingerprint`). While all of those are unchanged the
#: decision is too, and the tick neither takes the lock nor ``lstat``s one
#: path: R13's 28 orphaned prewrites name 3,584 of them, every 5 s. A hold
#: for a pool funding intent is decided again each cycle.




def _sweep_ended_prewrites(queue, instance: Mapping[str, object],
                           template: Mapping[str, object],
                           batch_ids: Sequence[str],
                           reads: bl.LedgerReads) -> list[dict[str, object]]:
    """The sweep of one instance's outstanding prewrites (#949, #1053).

    Nothing while the attempt can still commit them: the owner key's one
    read this tick (`bl.LedgerReads.generation`) says whether it ended.
    Otherwise under the output-prefix lock, which `require_prewrite` and
    `commit_batch` also take, so no attempt of this template can prewrite
    or commit between the decision and its effect
    (`_ended_prewrite_dispositions`). A ``reclaim`` removes the record,
    which frees the reservation the instance's accounting charged for it
    (`_outstanding_sums`), and returns ``output-prewrite-reclaimed``. An
    ``orphaned`` or ``refused`` prewrite is reported once per change
    (``output-prewrite-orphaned``, with its ``remedy``, and
    ``output-origin-retirement-refused``), and so is a ``hold`` in which an
    attempt whose state is unknown takes part (`ORIGIN_HELD_BY_UNKNOWN_EVENT`,
    #1065); a hold only live attempts take stays quiet.

    A staged template's prewrite (#1053) is also the precommit authority of
    any pool funding intent filed for its batch: while one names it, the
    record is held, as `abort_prewrite` holds it, and a census that cannot
    be read refuses. A write-only template stages nothing and has none.

    The generation is read before the lock. That is safe because ``dead``
    and ``succeeded`` are final for a nonce. A sibling that ends or starts
    after the read can change only a report or a hold, never a removal,
    which depends only on the files present and the batches committed, both
    read under the lock.
    """

    keys = {batch_id: f"{_batch_report_key(instance, batch_id)}.prewrite"
            for batch_id in batch_ids}
    attempt = instance["owner_attempt"]
    assert isinstance(attempt, dict)
    generation = reads.generation(str(instance["owner_action_key"]))
    if bl.attempt_state(generation, str(attempt["nonce"])) not in oc.ENDED_ATTEMPT_STATES:
        for key in keys.values():
            _UNFILED_REPORTS.pop(key, None)
        return []
    ledger = bl.BatchLedger.for_instance(queue.root, instance)
    # First what is unchanged since the decision the tick last left in
    # place: no lock, and no path read (`_KEPT_PREWRITES`).
    owners_now: list[object] = []
    seen: dict[str, object] = {}

    def unchanged(batch_id: str) -> bool:
        kept = _KEPT_PREWRITES.get(keys[batch_id])
        if kept is None:
            return False
        (version, dirs, dir_versions, owners_then), _action = kept
        inputs = bl.prewrite_inputs(ledger.prewrite_path(batch_id),
                                  None, kept_dirs=dirs, seen=seen)
        if inputs is None or inputs != (version, dirs, dir_versions):
            return False
        if not owners_now:
            try:
                owners_now.append(
                    reads.path_owners(instance, template).fingerprint)
            except (oc.ProducedOutputError, OSError, ValueError):
                owners_now.append(None)
        return owners_now[0] is not None and owners_now[0] == owners_then

    batch_ids = [batch_id for batch_id in batch_ids if not unchanged(batch_id)]
    if not batch_ids:
        return []
    staged = not oc.is_write_only(template)
    reports: list[tuple[str, dict[str, object]]] = []
    reclaimed: list[dict[str, object]] = []
    unlinked: list[str] = []
    with queue.stage_ownership_lock(str(instance["output_prefix"])):
        records: dict[str, bl.PrewriteFacts] = {}
        inputs: dict[str, tuple[object, ...] | None] = {}
        locked_seen: dict[str, object] = {}
        for batch_id in batch_ids:
            base = {"prewrite": _batch_report_key(instance, batch_id)}
            record_path = ledger.prewrite_path(batch_id)
            version = bl.prewrite_inputs(record_path, None)
            try:
                record = ledger.prewrite(batch_id)
            except oc.ProducedOutputError as exc:
                _KEPT_PREWRITES.pop(keys[batch_id], None)
                reports.append((batch_id, {
                    "event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                    "reason": "unknown-retain", "detail": str(exc)}))
                continue
            if record is None:
                _KEPT_PREWRITES.pop(keys[batch_id], None)
                _UNFILED_REPORTS.pop(keys[batch_id], None)
                continue
            records[batch_id] = record
            # The record's version from before it was read; its directories'
            # from before any of its paths is.
            dirs = bl.prewrite_inputs(record_path, record, seen=locked_seen)
            inputs[batch_id] = (None if version is None or dirs is None
                                else (version[0], dirs[1], dirs[2]))
        dispositions = _ended_prewrite_dispositions(
            queue, instance, template, records, reads)
        for batch_id, disposition in dispositions.items():
            base = {"prewrite": _batch_report_key(instance, batch_id),
                    "class_bytes": dict(records[batch_id].class_bytes)}
            action = disposition["action"]
            kept = inputs.get(batch_id)
            if action in ("orphaned", "hold") and kept is not None:
                _KEPT_PREWRITES[keys[batch_id]] = (
                    (*kept, disposition["owners"]), action)
            else:
                _KEPT_PREWRITES.pop(keys[batch_id], None)
            if action == "reclaim" and staged:
                named = _prepaid_intent_names(
                    reads, instance, template, batch_id,
                    str(records[batch_id].tier))
                if named is None:
                    disposition = {"action": "refused",
                                   "reason": "unknown-retain",
                                   "detail": "funding-census"}
                    action = "refused"
                elif named:
                    action = "hold"
            if action == "hold" and disposition.get("unknown"):
                # Held by an attempt whose state is unknown (#1065): named,
                # once per change, not held silently.
                reports.append((batch_id, {
                    "event": oc.ORIGIN_HELD_BY_UNKNOWN_EVENT, **base,
                    **bl.unknown_holder_report(disposition["unknown"],
                                             queue.root)}))
            elif action == "hold":
                _UNFILED_REPORTS.pop(keys[batch_id], None)
            elif action == "reclaim":
                path = ledger.prewrite_path(batch_id)
                try:
                    path.unlink()
                except FileNotFoundError:
                    continue
                unlinked.append(str(path))
                _UNFILED_REPORTS.pop(keys[batch_id], None)
                reclaimed.append({"event": oc.ORIGIN_PREWRITE_RECLAIMED_EVENT, **base,
                                  "reason": disposition["reason"],
                                  "superseded": disposition["superseded"]})
            elif action == "orphaned":
                reports.append((batch_id, {
                    "event": oc.ORIGIN_PREWRITE_ORPHANED_EVENT, **base,
                    "paths": disposition["paths"],
                    "superseded": disposition["superseded"],
                    "held": disposition["held"],
                    "remedy": oc.ORPHANED_PREWRITE_REMEDY}))
            else:
                reports.append((batch_id, {
                    "event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                    "reason": disposition["reason"],
                    "detail": disposition["detail"]}))
        if unlinked:
            _fsync_directories(unlinked)
    events = list(reclaimed)
    for batch_id, event in sorted(reports, key=lambda item: item[0]):
        signature = hashlib.sha256(digest_primitives.sorted_json(event).encode()).hexdigest()
        if _UNFILED_REPORTS.get(keys[batch_id]) == signature:
            continue
        _UNFILED_REPORTS[keys[batch_id]] = signature
        events.append(event)
    return events


def _orphaned_prewrites(queue, instance: Mapping[str, object],
                        template: Mapping[str, object],
                        batch_ids: Sequence[str],
                        reads: bl.LedgerReads, *,
                        unreadable: list[str], where: str,
                        held: list[dict[str, object]] | None = None
                        ) -> list[dict[str, object]]:
    """The listing's side of the sweep: an ended attempt's orphaned prewrites.

    Read-only and lock-free, over the same `_ended_prewrite_dispositions` the
    tick applies. A record or state it cannot read goes to ``unreadable``.
    A prewrite held by an attempt whose state is unknown (#1065) goes to
    ``held``, as the tick reports it.
    """

    attempt = instance["owner_attempt"]
    assert isinstance(attempt, dict)
    generation = reads.generation(str(instance["owner_action_key"]))
    if bl.attempt_state(generation, str(attempt["nonce"])) not in oc.ENDED_ATTEMPT_STATES:
        return []
    ledger = bl.BatchLedger.for_instance(queue.root, instance)
    records: dict[str, bl.PrewriteFacts] = {}
    for batch_id in batch_ids:
        try:
            record = ledger.prewrite(batch_id)
        except oc.ProducedOutputError as exc:
            unreadable.append(f"{where}/{batch_id}.prewrite: {exc}")
            continue
        if record is not None:
            records[batch_id] = record
    listed: list[dict[str, object]] = []
    for batch_id, disposition in _ended_prewrite_dispositions(
            queue, instance, template, records, reads).items():
        if disposition["action"] == "orphaned":
            listed.append({
                "prewrite": _batch_report_key(instance, batch_id),
                "class_bytes": dict(records[batch_id].class_bytes),
                "paths": disposition["paths"],
                "superseded": disposition["superseded"],
                "held": disposition["held"],
                "remedy": oc.ORPHANED_PREWRITE_REMEDY})
        elif (disposition["action"] == "hold" and held is not None
              and disposition.get("unknown")):
            held.append({
                "prewrite": _batch_report_key(instance, batch_id),
                "class_bytes": dict(records[batch_id].class_bytes),
                **bl.unknown_holder_report(disposition["unknown"], queue.root)})
        elif (disposition["action"] == "refused"
              and disposition["reason"] == "unknown-retain"):
            unreadable.append(
                f"{where}/{batch_id}.prewrite: {disposition['detail']}")
    return listed


def blocked_origin_batches(queue) -> dict[str, list]:
    """The consumed batches held only by consumers that ended (#926), read-only.

    A batch is *blocked* when every declared consumer still holding it is
    ``failed`` or ``withdrawn``: nothing that is queued or running will
    release it, and it waits for a superseding resubmission or an operator
    release (`release_origin_consumer`). The tick reports that once per
    change; this reads the same records again, for a listing that does not
    scroll away.

    It scans the produced-output scopes as `origin_retirement_tick` does,
    skipping the scopes it skips, takes no lock and writes nothing. A batch
    that is retiring, reclaimed, or has no declared consumer is not listed;
    neither is one held for a deferred consumer's release (#913), which is
    waiting, not blocked. If those holds cannot be read, nothing is listed.
    Returns ``{"blocked": [...], "orphaned_prewrites": [...],
    "held_by_unknown": [...], "unreadable": [...]}``. Each blocked entry
    carries the batch's ``ref`` and ``bytes``, every declared consumer's
    state as the tick resolves it (`_resolved_consumers`), the ``holding``
    ones, and ``reported``: whether the entry carries the tick's report memo.

    ``orphaned_prewrites`` lists each prewrite, of any template (#1053),
    whose attempt ended before committing and whose files belong to no batch
    (#949, `_ended_prewrite_dispositions`): its ``prewrite`` coordinates
    (owner/template.nonce/batch), ``class_bytes``, the orphaned ``paths``,
    the ``superseded`` and ``held`` ones, and the ``remedy``.

    ``held_by_unknown`` lists what the tick reports as
    `ORIGIN_HELD_BY_UNKNOWN_EVENT` (#1065): a consumed batch the tick would
    retire now (every declared consumer resolved, or none declared and its
    producer attempt dead) and an ended attempt's prewrite it would reclaim,
    each held only because another attempt whose state is unknown has
    prewritten one of its paths. Each carries its ``ref`` and ``bytes`` (a
    batch) or ``prewrite`` and ``class_bytes``, the ``holders`` with ``why``
    each is unknown and whether it is ``orphaned``, the ``reason`` and, for
    an orphaned holder, the ``remedy``.
    """

    from . import action_edges

    blocked: list[dict[str, object]] = []
    orphaned: list[dict[str, object]] = []
    held_by_unknown: list[dict[str, object]] = []
    unreadable: list[str] = []

    def found() -> dict[str, list]:
        return {"blocked": blocked, "orphaned_prewrites": orphaned,
                "held_by_unknown": held_by_unknown,
                "unreadable": unreadable}

    scopes_root = bl.scopes_root(queue.root)
    templates_root = Path(queue.root) / "residency" / oc.OUTPUT_TEMPLATES_SUBDIR
    try:
        owners = bl.scope_owners(scopes_root)
    except FileNotFoundError:
        return found()
    except OSError as exc:
        unreadable.append(f"output scopes: {exc}")
        return found()
    holds: set[tuple[str, str]] | None = None
    reads = bl.LedgerReads(queue, lease_timeout_s=pool.LEASE_TIMEOUT_S)
    for owner in owners:
        try:
            scopes = sorted(child for child in (scopes_root / owner).iterdir()
                            if child.is_dir())
        except OSError as exc:
            unreadable.append(f"{owner[:12]}: {exc}")
            continue
        for scope in scopes:
            where = f"{owner[:12]}/{scope.name}"
            try:
                batches = reads.entries(scope)
            except oc.ProducedOutputError as exc:
                unreadable.append(f"{where}: {exc}")
                continue
            due = [
                (batch_id, entry) for batch_id, entry in sorted(batches.items())
                if isinstance(entry, bl.BatchFacets) and entry.kind == "origin"
                and entry.lifetime == oc.ORIGIN_LIFETIME_CONSUMED
                and not entry.origin_reclaimed and not entry.is_retiring
            ]
            try:
                outstanding = batches.outstanding_prewrite_ids()
            except OSError as exc:
                unreadable.append(f"{where}/prewrites: {exc}")
                outstanding = []
            if not due and not outstanding:
                continue
            if not due:
                # As the tick: a producer that can still commit is skipped
                # before its instance is read.
                if (bl.attempt_state(reads.generation(owner),
                                   scope.name.rpartition(".")[2])
                        not in oc.ENDED_ATTEMPT_STATES):
                    continue
            try:
                instance = oc.validate_instance(json.loads(
                    bl.BatchLedger.at_scope(scope).instance_path.read_text()))
                template = oc.validate_template(json.loads(
                    (templates_root / f"{instance['template_id']}.json"
                     ).read_text()))
            except (OSError, ValueError) as exc:
                unreadable.append(f"{where}: {exc}")
                continue
            # The tick skips a scope filed elsewhere or bound to another
            # template; so does the listing.
            if (bl.scope_dir(queue.root, instance) != scope
                    or oc.template_sha256(template) != instance["template_sha256"]):
                continue
            if outstanding:
                # Any template's, as the tick sweeps them (#1053).
                orphaned.extend(_orphaned_prewrites(
                    queue, instance, template, outstanding, reads,
                    unreadable=unreadable, where=where, held=held_by_unknown))
            if not due:
                continue
            if holds is None:
                try:
                    holds = action_edges.held_producer_batches(queue)
                except (OSError, ValueError) as exc:
                    # Without the holds no batch can be told apart from one
                    # waiting for a deferred release; list none rather than
                    # all of them.
                    unreadable.append(f"deferred holds: {exc}")
                    blocked.clear()
                    held_by_unknown[:] = [item for item in held_by_unknown
                                          if "ref" not in item]
                    return found()
            if _held_for_deferred(instance, holds):
                continue
            scope_owners: bl.RetirementOwners | None = None
            for batch_id, entry in due:
                try:
                    ref = oc.origin_batch_ref(instance, batch_id=batch_id, manifest_digest=str(entry.required_manifest))
                    checked_ref = oc.checked_origin_ref(ref)
                    declared = bl.declared_consumers(
                        queue.root, instance, batch_id, checked_ref)
                    consumers = (_resolved_consumers(
                        queue, instance, batch_id, checked_ref, declared)
                        if declared else [])
                    total = entry.origin_charge(batch_id)
                except (oc.ProducedOutputError, KeyError, ValueError) as exc:
                    unreadable.append(f"{where}/{batch_id}: {exc}")
                    continue
                # What the tick would retire now, but for a hold by an
                # attempt whose state is unknown (#1065).
                retirable = (all(item["state"] in oc.RESOLVED_CONSUMER_STATES
                                 for item in consumers) if declared
                             else bl.attempt_state(
                                 reads.generation(owner),
                                 str(instance["owner_attempt"]["nonce"]))
                             == "dead")
                if retirable:
                    try:
                        if scope_owners is None:
                            scope_owners = reads.path_owners(instance, template)
                        holders = bl.unknown_path_holders(scope_owners, entry.paths or [])
                    except (oc.ProducedOutputError, OSError, ValueError) as exc:
                        unreadable.append(f"{where}/{batch_id}: owners: {exc}")
                        holders = []
                    if holders:
                        held_by_unknown.append({
                            "ref": ref, "bytes": total,
                            **bl.unknown_holder_report(holders, queue.root)})
                if not declared:
                    continue
                holding = [item for item in consumers
                           if item["state"] not in oc.RESOLVED_CONSUMER_STATES]
                if holding and all(item["state"] in oc.TERMINAL_CONSUMER_STATES
                                   for item in holding):
                    blocked.append({
                        "ref": ref, "bytes": total, "consumers": consumers,
                        "holding": [str(item["action_key"]) for item in holding],
                        "reported": entry.has_retirement_report,
                    })
    return found()


#: Attempts this process has filed or found in the attempt index, by queue
#: root, owner and scope name: the tick's backfill stats each scope once
#: per process, not once per cycle.
#: Queue roots whose attempt index this process has marked complete.




class _DeadStagedReports:
    """Once-per-change reports for one dead producer's staged batch (#1053)."""

    def __init__(self, instance: Mapping[str, object], batch_id: str) -> None:
        self.coordinates = _batch_report_key(instance, batch_id)
        self.memo = f"{self.coordinates}.stage"

    def refused(self, reason: str, **detail: object) -> dict[str, object] | None:
        event = {"event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT,
                 "batch": self.coordinates, "reason": reason, **detail}
        signature = hashlib.sha256(json.dumps(
            event, sort_keys=True, default=str).encode()).hexdigest()
        if _UNFILED_REPORTS.get(self.memo) == signature:
            return None
        _UNFILED_REPORTS[self.memo] = signature
        return event

    def quiet(self) -> None:
        _UNFILED_REPORTS.pop(self.memo, None)
        return None


def _dead_staged_batch_ready(queue, instance: Mapping[str, object],
                             batch_id: str, batches: bl.BatchCapture, *,
                             reads: bl.LedgerReads) -> dict[str, object]:
    """Whether a dead producer's staged batch is the tick's to retire (#1053).

    Returns ``{"event": ...}`` (the report, or None) when it is not, else
    ``{"ready": ...}``: the mover, the tier and the tier's stage root.  The
    conditions are `_retire_dead_staged_batches`'s.
    """

    from prismabuild import pool as pool_mod

    report = _DeadStagedReports(instance, batch_id)
    entry = batches.lookup(batch_id)
    if not isinstance(entry, bl.BatchFacets):
        return {"event": report.quiet()}
    try:
        active = entry.active
    except oc.ProducedOutputError as exc:
        return {"event": report.refused(f"unknown-retain: {exc}")}
    if active.retired:
        return {"event": report.quiet()}
    mover = str(active.mover_key or '')
    tier = str(active.tier or entry.tier or '')
    if len(mover) != 64 or not tier:
        return {"event": report.refused("unknown-retain: batch-target-mismatch")}
    match reads.generation(mover):
        case Present(QueueRow(mover_state, _row)):
            pass
        case Present(Moving()):
            mover_state = "moving"
        case Absent():
            mover_state = "absent"
        case Unknown():
            mover_state = "unknown"
    if mover_state in (pool_mod.record_store.READY, pool_mod.record_store.CLAIMED, "moving"):
        return {"event": report.quiet()}
    if mover_state not in (pool_mod.record_store.DONE, pool_mod.record_store.FAILED, pool_mod.record_store.WITHDRAWN):
        return {"event": report.refused(
            f"unknown-retain: its mover {mover[:12]} is {mover_state}",
            mover_key=mover)}
    try:
        held = queue.tier(tier).holdings(mover)
    except Exception as exc:
        return {"event": report.refused(f"unknown-retain: tier ledger: {exc}",
                                        mover_key=mover)}
    if held:
        return {"event": report.quiet()}
    try:
        record, file_state = queue.funding.output_evidence(mover, tier)
    except Exception as exc:
        return {"event": report.refused(f"unknown-retain: funding: {exc}",
                                        mover_key=mover)}
    if file_state not in ("absent", "ok"):
        return {"event": report.refused(
            f"unknown-retain: funding record is {file_state}", mover_key=mover)}
    if file_state == "ok" and str((record or {}).get("state")) not in (
            "consumed", "released"):
        return {"event": report.refused(f"funding-{(record or {}).get('state')}",
                                        mover_key=mover)}
    try:
        tier_record = _announced_tier_record(queue, tier)
    except Exception as exc:
        return {"event": report.refused(f"unknown-retain: tier record: {exc}",
                                        mover_key=mover)}
    if tier_record is None or not tier_record.get("mountpoint"):
        return {"event": report.refused(f"tier-not-announced: {tier}",
                                        mover_key=mover)}
    if not _egress_runs_in_process(tier_record):
        # Elsewhere `retire_batch` would publish an egress action; the tick
        # publishes nothing.  The tier loop runs on the tier host.
        return {"event": report.refused(
            "not-on-the-tier-host", mover_key=mover,
            tier_host=str(tier_record.get("host") or ""))}
    return {"ready": {"mover": mover, "tier": tier,
                      "stage_root": str(tier_record["mountpoint"])}}


def _dead_staged_batch_event(queue, instance: Mapping[str, object],
                             template: Mapping[str, object], batch_id: str,
                             batches: bl.BatchCapture,
                             result: Mapping[str, object], *, mover: str,
                             tier: str, producer_state: str,
                             reads: bl.LedgerReads) -> dict[str, object] | None:
    """The event for one dead producer's batch retirement, from its answer."""

    report = _DeadStagedReports(instance, batch_id)
    if result.get("deferred"):
        # The cycle's budget ran out before this batch's egress: nothing of
        # it was touched, and the next cycle takes it up.
        return None
    if result.get("ok") is not True:
        receipt = result.get("receipt")
        errors = (list(receipt.get("errors") or [])
                  if isinstance(receipt, Mapping) else [])
        return report.refused(str(result.get("refusal") or "retirement refused"),
                              mover_key=mover, errors=errors)
    if result.get("duplicate"):
        return report.quiet()
    receipt = result.get("receipt")
    receipt = receipt if isinstance(receipt, Mapping) else {}
    event: dict[str, object] = {
        "event": oc.DEAD_PRODUCER_BATCH_RETIRED_EVENT,
        "batch": report.coordinates,
        "mover_key": mover, "tier": tier, "producer_state": producer_state,
        "origin_kept": True,
        "entries_deleted": int(receipt.get("entries_deleted") or 0),
        "bytes_deleted": int(receipt.get("bytes_deleted") or 0),
        "tokens_released": int(receipt.get("tokens_released") or 0),
    }
    # Which of its paths another attempt now claims: for the record only.
    entry = batches.lookup(batch_id)
    entry = entry if isinstance(entry, bl.BatchFacets) else bl.BatchFacets({})
    try:
        owners = reads.path_owners(instance, template)
        superseded: list[dict[str, str]] = []
        for path in sorted((str(item) for item in entry.paths or [])):
            note = owners.committed(path)
            kind = "committed"
            if note is None:
                note, kind = owners.pending(path), "prewritten"
            if note is not None:
                superseded.append({"path": path, "kind": kind, **note})
        event["superseded"] = superseded
    except (oc.ProducedOutputError, OSError, ValueError) as exc:
        event["superseded"] = None
        event["superseded_unreadable"] = str(exc)
    _UNFILED_REPORTS.pop(report.memo, None)
    return event


def _retire_dead_staged_batches(queue, instance: Mapping[str, object],
                                template: Mapping[str, object],
                                staged: Sequence[str],
                                batches: bl.BatchCapture, *,
                                producer_state: str, reads: bl.LedgerReads,
                                budget=None) -> list[dict[str, object]]:
    """File the stage retirements of a dead producer's batches (#1053, #1072).

    R13 left ten staged batches that read ``retired: false`` for ever: its
    movers had finished and held no tier tokens, their funding was
    ``consumed``, and their stage copies were gone, but the producer died
    before its own `retire_batch` filed. The #929 sweep reaches only a
    mover that still holds tokens, so nothing retired them, and the
    unretired batches kept their paths from every later writer.

    The caller has read that the producer attempt ended (``dead`` or
    ``succeeded``). A batch is retired here when its active
    materialization's mover has ended (``done``, ``failed`` or
    ``withdrawn``), holds no tier tokens (a holder is the #929 sweep's), and
    its funding record is absent, ``consumed`` or ``released``
    (`_dead_staged_batch_ready`). Then the producer's own retirement runs, on
    the tier host only: every such batch of the instance on one stage root
    goes through `retire_staged_batches`, which validates each batch record,
    runs the ordinary egress on the batch's fragment root (which finds
    nothing to delete once the copy is gone, and deletes a copy that remains
    only against its fragment's identity), and files ``retired`` for all of
    them with one write of the instance's commitments (#1072).

    Retirement closes the stage records. It never reclaims, and never
    touches, the batch's origin files: a relaunch reads them (R13's reads
    392 of its predecessor's 436 batches), and their durable charge stays
    until a successor supersedes them or an operator reclaims them.

    A mover still queued or running, or a token holder, waits quietly, and
    so does a batch whose egress ``budget`` deferred to a later cycle.
    Anything else that stops the retirement is reported once per change as
    ``output-origin-retirement-refused``. Returns the events to log.
    """

    events: list[dict[str, object]] = []
    groups: dict[str, list[str]] = {}
    ready: dict[str, dict[str, object]] = {}
    for batch_id in staged:
        try:
            found = _dead_staged_batch_ready(queue, instance, batch_id, batches,
                                             reads=reads)
        except (oc.ProducedOutputError, OSError, ValueError) as exc:
            found = {"event": _unfiled_report(instance, f"{batch_id}.stage", {
                "event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT,
                "reason": f"unknown-retain: {type(exc).__name__}: {exc}"})}
        if "ready" not in found:
            if found.get("event") is not None:
                events.append(found["event"])  # type: ignore[arg-type]
            continue
        ready[batch_id] = found["ready"]  # type: ignore[assignment]
        groups.setdefault(str(ready[batch_id]["stage_root"]), []).append(batch_id)
    from prismabuild import pool as pool_mod

    residency_root = bl.output_fragment_root(Path(queue.root) / pool_mod.record_store.RESIDENCY)
    for stage_root, batch_ids in groups.items():
        try:
            results = retire_staged_batches(
                queue, instance, template, batch_ids, stage_root=stage_root,
                residency_root=residency_root, budget=budget)
        except (oc.ProducedOutputError, OSError, ValueError) as exc:
            for batch_id in batch_ids:
                event = _unfiled_report(instance, f"{batch_id}.stage", {
                    "event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT,
                    "reason": f"unknown-retain: {type(exc).__name__}: {exc}"})
                if event is not None:
                    events.append(event)
            continue
        for batch_id in batch_ids:
            try:
                event = _dead_staged_batch_event(
                    queue, instance, template, batch_id, batches,
                    results.get(batch_id) or {"ok": False,
                                              "refusal": "no answer"},
                    mover=str(ready[batch_id]["mover"]),
                    tier=str(ready[batch_id]["tier"]),
                    producer_state=producer_state, reads=reads)
            except (oc.ProducedOutputError, OSError, ValueError) as exc:
                event = _unfiled_report(instance, f"{batch_id}.stage", {
                    "event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT,
                    "reason": f"unknown-retain: {type(exc).__name__}: {exc}"})
            if event is not None:
                events.append(event)
    return events


#: The units of work `origin_retirement_tick` asks a budget for (#1072): one
#: instance's ended prewrites, one staged batch's egress
#: (`BATCH_RETIREMENT_UNIT`), and one consumed batch's origin retirement.


class _ScopeBudget:
    """A cycle budget that sees one scope's units under the scope's own key.

    `retire_staged_batches` asks for each batch by its id; the tick's
    rotation is by scope, so the budget records a deferral under the scope.
    """

    def __init__(self, budget, key: str) -> None:
        self._budget = budget
        self._key = key

    def start(self, kind: str, _key: str) -> bool:
        return bool(self._budget.start(kind, self._key))

    def done(self, kind: str) -> None:
        self._budget.done(kind)


def origin_retirement_tick(queue, *, budget=None, trust=None) -> list[dict[str, object]]:
    """Retire what the produced-output lane's ended attempts left (#914, #1053).

    Called once per `tier_loop.cycle` on the tier host. It scans the same
    produced-output scopes `unheld_window_gib` and `output_scope_tick` scan
    and reads each instance's commitments once (`bl.LedgerReads`); each due
    batch's retirement under the output-prefix locks reuses that parse while
    the file's trusted version holds, so an unchanged document is parsed
    once per tick, not once per batch (#992). Then:

    * every origin-only batch committed with the ``consumed`` lifetime and
      not yet reclaimed goes to `_retire_consumed_batch`. A ``retain``
      batch is never touched, whatever became of its producer;
    * the outstanding prewrites of an attempt that ended before committing
      are swept (`_sweep_ended_prewrites`, #949), whatever the template:
      a staged template's too (#1053);
    * a staged batch of an ended attempt whose stage copy was never
      retired has its stage retirement filed (`_retire_dead_staged_batches`,
      #1053), one commitments write per instance (#1072). Its origin files
      are kept.

    It also files the attempt index pointer of any scope that has none, and
    marks the index complete once one full pass has indexed every scope it
    listed (`bl.backfill_attempt`).

    ``budget``, when the tier loop passes one (#1072), is asked before each
    unit of work -- an instance's ended prewrites, a staged batch's egress, a
    consumed batch's retirement -- whether it still fits this cycle
    (``budget.start(kind, key)``) and told when it ends
    (``budget.done(kind)``).  A unit it refuses is left exactly as it was
    for a later cycle, and the scopes are visited from the one whose unit
    was refused first (``budget.order``), so a backlog is worked through
    rather than retried from the top.  The scan itself still visits every
    scope: the attempt index is filed and marked complete as before.

    Returns the events to log: one ``output-origin-retired`` per retired
    batch (its ref, bytes, consumers and the origin identity each deleted
    file was checked against), ``output-dead-producer-batch-retired`` per
    stage retirement filed, and ``output-origin-retirement-stalled`` or
    ``-refused`` once per change of what holds a batch. A swept prewrite
    adds ``output-prewrite-reclaimed``, or ``output-prewrite-orphaned`` once
    per change. A tick with nothing to do and nothing new to report returns
    ``[]``. An instance whose commitments cannot be read is skipped, as the
    other scans skip it: its batches stay charged and on disk.
    """

    events: list[dict[str, object]] = []
    scopes_root = bl.scopes_root(queue.root)
    try:
        owners = bl.scope_owners(scopes_root)
    except OSError:
        return events
    templates_root = Path(queue.root) / "residency" / oc.OUTPUT_TEMPLATES_SUBDIR
    reads = bl.LedgerReads(queue, lease_timeout_s=pool.LEASE_TIMEOUT_S, trust=trust)
    indexed = True
    # What unreleased deferred consumers will read (#913), read once and only
    # when some batch is due.  A queue that cannot say keeps every batch for
    # this tick.
    holds: set[tuple[str, str]] | None = None
    visits: list[tuple[str, Path]] = []
    for owner in owners:
        try:
            visits.extend((owner, child) for child in sorted(
                child for child in (scopes_root / owner).iterdir()
                if child.is_dir()))
        except OSError:
            indexed = False
    if budget is not None:
        # From the scope whose unit the budget refused first last cycle.
        by_key = {str(scope): (owner, scope) for owner, scope in visits}
        visits = [by_key[key] for key in budget.order(
            oc.ORIGIN_RETIREMENT_UNITS, list(by_key))]
    for owner, scope in visits:
        indexed = bl.backfill_attempt(queue.root, owner, scope.name) and indexed
        nonce = scope.name.rpartition(".")[2]
        try:
            batches = reads.entries(scope)
        except oc.ProducedOutputError:
            continue
        due = [
            batch_id for batch_id, entry in sorted(batches.items())
            if isinstance(entry, bl.BatchFacets) and entry.kind == "origin"
            and entry.lifetime == oc.ORIGIN_LIFETIME_CONSUMED
            and not entry.origin_reclaimed
        ]
        try:
            outstanding = batches.outstanding_prewrite_ids()
        except OSError:
            outstanding = []
        staged = batches.unretired_staged
        if not due and not outstanding and not staged:
            continue
        ended = (bl.attempt_state(reads.generation(owner), nonce)
                 if staged or not due else "")
        if not due and ended not in oc.ENDED_ATTEMPT_STATES:
            # Only an ended attempt's prewrites and stage copies are the
            # tick's (#949, #1053).  The scope is named for the attempt's
            # nonce, so a producer that can still commit costs no
            # instance or template read.
            continue
        try:
            instance = oc.validate_instance(json.loads(
                bl.BatchLedger.at_scope(scope).instance_path.read_text()))
            template = oc.validate_template(json.loads(
                (templates_root / f"{instance['template_id']}.json"
                 ).read_text()))
        except (OSError, ValueError) as exc:
            event = {"event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT,
                     "batch": f"{owner}/{scope.name}",
                     "reason": f"unknown-retain: scope unreadable: {exc}"}
            signature = hashlib.sha256(digest_primitives.sorted_json(event).encode()).hexdigest()
            if _UNFILED_REPORTS.get(event["batch"]) != signature:
                _UNFILED_REPORTS[event["batch"]] = signature
                events.append(event)
            continue
        if (bl.scope_dir(queue.root, instance) != scope
                or oc.template_sha256(template) != instance["template_sha256"]):
            continue
        scope_budget = (None if budget is None
                        else _ScopeBudget(budget, str(scope)))
        if outstanding and (scope_budget is None or scope_budget.start(
                oc.ENDED_PREWRITES_UNIT, str(scope))):
            # An attempt that ended before committing (#949), of any
            # template (#1053).
            try:
                events.extend(_sweep_ended_prewrites(
                    queue, instance, template, outstanding, reads))
            except (oc.ProducedOutputError, OSError, ValueError) as exc:
                event = _unfiled_report(instance, "prewrites", {
                    "event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT,
                    "reason": f"unknown-retain: {type(exc).__name__}: {exc}"})
                if event is not None:
                    events.append(event)
            finally:
                if scope_budget is not None:
                    scope_budget.done(oc.ENDED_PREWRITES_UNIT)
        if staged and ended in oc.ENDED_ATTEMPT_STATES:
            events.extend(_retire_dead_staged_batches(
                queue, instance, template, staged, batches,
                producer_state=ended, reads=reads, budget=scope_budget))
        if not due:
            continue
        if holds is None:
            from . import action_edges

            try:
                holds = action_edges.held_producer_batches(queue)
            except (OSError, oc.ProducedOutputError, ValueError):
                return events
        for batch_id in due:
            if scope_budget is not None and not scope_budget.start(
                    oc.CONSUMED_RETIREMENT_UNIT, batch_id):
                continue
            try:
                event = _retire_consumed_batch(
                    queue, instance, template, batch_id,
                    deferred_holds=holds, reads=reads)
            except (oc.ProducedOutputError, OSError, ValueError) as exc:
                event = _unfiled_report(instance, batch_id, {
                    "event": oc.ORIGIN_RETIREMENT_REFUSED_EVENT,
                    "reason": f"unknown-retain: {type(exc).__name__}: {exc}"})
            finally:
                if scope_budget is not None:
                    scope_budget.done(oc.CONSUMED_RETIREMENT_UNIT)
            if event is not None:
                events.append(event)
    bl.finish_attempt_index(queue.root, complete=indexed)
    return events


def _require_coherent_lease_sdk(lease_sdk: object):
    """The accepted SDK object, or the reason it cannot vouch.

    Production release never runs without the coherent package: ``None``,
    a stub, or a foreign copy retains. Coherence is the accepted
    ``prismabuild.reader_lease`` package itself -- same tag, same file the
    fleet imports (no bare ``reader_lease`` import, no vendored copy) --
    exposing the census and containment predicates this path calls.
    Returns ``(sdk, None)`` or ``(None, refusal)``.
    """

    if lease_sdk is None:
        return None, "unknown-retain: lease-sdk-missing"
    try:
        from prismabuild import reader_lease as installed
    except ImportError as exc:
        return None, f"unknown-retain: lease-sdk-unimportable: {exc}"
    tag = getattr(lease_sdk, "READER_LEASE_TAG", None)
    if tag != getattr(installed, "READER_LEASE_TAG", "reader-lease-v1"):
        return None, "unknown-retain: lease-sdk-incoherent-tag"
    for name in ("live_for", "containment_certificate_ok", "leases_root"):
        if not callable(getattr(lease_sdk, name, None)):
            return None, f"unknown-retain: lease-sdk-missing-{name}"
    own_file = getattr(lease_sdk, "__file__", None)
    installed_file = getattr(installed, "__file__", None)
    try:
        same = (isinstance(own_file, str) and isinstance(installed_file, str)
                and os.path.realpath(own_file) == os.path.realpath(installed_file))
    except OSError:
        same = False
    if not same:
        return None, "unknown-retain: lease-sdk-foreign-package"
    return lease_sdk, None


def _dir_has_entries(path: Path) -> bool:
    """Whether a directory holds any entry; missing reads as empty.

    A stat/permission failure is unknown state that retains, never silent
    absence: `Path.is_dir/is_file` answer False on errors, which would
    prove nothing about the bytes the accounting vouches.
    """

    try:
        with os.scandir(path) as iterator:
            for _ in iterator:
                return True
            return False
    except FileNotFoundError:
        return False
    except (OSError, ValueError) as exc:
        raise oc.ProducedOutputError(f"unknown-retain: {exc}") from None


def _lease_pin_files(lease_sdk: object, queue, consumer: str,
                     residency_root: str | Path | None = None
                     ) -> list[str] | None:
    """Pin file names under one consumer namespace, or None when unreadable."""

    try:
        root = lease_sdk.leases_root(queue, residency_root=residency_root)
    except Exception:
        return None
    directory = Path(root) / consumer
    try:
        with os.scandir(directory) as iterator:
            return sorted(entry.name for entry in iterator
                          if entry.is_file() and entry.name.endswith(".lease.json"))
    except FileNotFoundError:
        return []
    except OSError:
        return None


def _live_output_paths(queue, lease_sdk: object,
                       batches: bl.BatchCapture,
                       residency_root: str | Path
                       ) -> tuple[list[str], str | None]:
    """Live pinned paths attributable to these batches, or unknown reason.

    Traces the staged paths each batch vouches -- live fragments while
    staged, plus `staged_paths` recorded at retire time after the delete
    -- and intersects the SDK pin census over the output fragment root.
    Returns `(paths, None)`, or `([], reason)` when attribution is
    impossible: a tainted/unreadable census, unreadable fragments beside
    live paths, or live paths beside batch records predating staged-path
    recording. Never ignores an attributable live path; never blocks on
    unrelated consumers' pins.
    """

    from prismabuild import residency_map as map_mod

    wanted: set[str] = set()
    unrecorded: list[str] = []
    for batch_id, entry in batches.items():
        if not isinstance(entry, bl.BatchFacets):
            continue
        # Every materialization this batch has had vouches staged paths: the
        # first publication records them on the entry at retirement, each
        # restage records its own on its materialization row. All of them are
        # attributable to this instance, so all of them are wanted.
        try:
            active = entry.active
            sources: list[bl.BatchFacets | bl.StageCopy] = [entry]
            sources += entry.history
        except oc.ProducedOutputError:
            unrecorded.append(str(batch_id))
            continue
        for source in sources:
            recorded = source.staged_paths
            if isinstance(recorded, list):
                for path in recorded:
                    if isinstance(path, str) and path:
                        wanted.add(os.path.normpath(path))
        if active.retired:
            # The copy is gone; only what its egress vouched can be pinned.
            # A record that retired before staged paths were recorded leaves
            # its paths unknowable, so live paths cannot be ruled out.
            if not isinstance(active.staged_paths, list):
                unrecorded.append(str(batch_id))
            continue
        ns = entry.namespace
        if not isinstance(ns, str) or not ns:
            unrecorded.append(str(batch_id))
            continue
        try:
            fragments = map_mod.read_fragments(str(residency_root), ns)
        except Exception:
            unrecorded.append(str(batch_id))
            continue
        if not fragments:
            # A live materialization that has vouched no bytes yet has
            # nothing pinnable.
            continue
        try:
            composed = map_mod.compose(fragments)
        except Exception:
            unrecorded.append(str(batch_id))
            continue
        entries = composed.get("entries")
        if not isinstance(entries, Mapping):
            unrecorded.append(str(batch_id))
            continue
        for record in entries.values():
            if isinstance(record, Mapping) and record.get("stage_path"):
                wanted.add(os.path.normpath(str(record["stage_path"])))
    try:
        census = lease_sdk.live_for(queue, None, residency_root=str(residency_root))
    except Exception as exc:
        return [], f"unknown-retain: {exc}"
    owners: Mapping[str, object] = {}
    tainted: list[object] = []
    if (isinstance(census, tuple) and len(census) == 2
            and isinstance(census[0], Mapping)):
        owners, tainted = census[0], list(census[1] or [])
    elif isinstance(census, Mapping):
        owners = census
    else:
        return [], "unknown-retain: lease-census-shape"
    if tainted:
        return [], "unknown-retain: pin-census-tainted"
    live = sorted({os.path.normpath(str(path)) for path in owners} & wanted)
    if live:
        return live, None
    if unrecorded and owners:
        return [], "unknown-retain: unattributable-live-paths"
    return [], None


def safe_release_instance(queue, instance: Mapping[str, object],
                          template: Mapping[str, object],
                          lease_sdk: object = None) -> dict[str, object]:
    """Release leftover holder tokens ONLY when retirement is proven safe.

    Movers release their exact tokens through egress (`retire_batch`); this
    reclaims remainders after a fresh census under the prefix lock. EVERY
    check is exact:

    - instance + bound template readable and matching, else refusal;
    - coherent accepted SDK required (same package the fleet imports; a
      missing, foreign, or unreadable SDK retains -- never
      `sdk-absent-structural`);
    - every committed batch retired AND its mover holder empty AND its
      fragments gone, else active-batches/movers-retain (directory stats
      that fail read as unknown, never as empty);
    - funding-intent-only movers ALWAYS retain with
      `funding-intent-reconcile-retain`: a mover that never published may
      still have copied partial bytes, and metadata absence never proves
      physical absence. The funding/reconciliation lane owns these intents;
      this path never releases them;
    - funding-intent-only movers ALWAYS retain with
      `funding-intent-reconcile-retain`: a mover that never published may
      still have copied partial bytes, and metadata absence never proves
      physical absence. The funding/reconciliation lane owns these intents;
      this path never releases them;
    - live pins: pin files under the owner and every batch namespace
      retain; live census paths attributable to this instance's recorded
      staged paths retain by name; a tainted or unattributable census
      retains as unknown (unrelated consumers' pins never block);
    - owner containment: a live claim in any form retains (same attempt =
      owner-active, other attempt = owner-superseded); a corrupt or
      unreadable live claim retains as unknown and never falls through to
      an old terminal. With no live claim, the accepted SDK
      `containment_certificate_ok` over the exact owner nonce/scope must
      authorize (broker attestation + matching terminal telemetry); ANY
      bare DONE/FAILED/WITHDRAWN record by key alone is insufficient;
    - every ledger release is attempted individually: the FIRST exception
      retains (ok False), never ok True after a release failure;
    - physical files are never deleted here (no unlink of staged bytes).

    Idempotent: the orderly path releases 0 (egress already released
    exactly once); leftovers release once.
    """

    from prismabuild import pool as pool_mod

    try:
        checked_template, checked = oc.bound_output_contract(template, instance)
    except oc.ProducedOutputError as exc:
        if "template-mismatch" in str(exc):
            return {"ok": False, "refusal": "template-mismatch"}
        return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    sdk, sdk_refusal = _require_coherent_lease_sdk(lease_sdk)
    if sdk is None:
        return {"ok": False, "refusal": str(sdk_refusal)}
    with queue.stage_ownership_lock(str(checked["output_prefix"])):
        try:
            commitments = bl.BatchLedger.for_instance(queue.root, checked).entries()
        except oc.ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        batches = commitments
        assert isinstance(batches, bl.BatchCapture)
        out_base = bl.output_fragment_root(queue.root / pool_mod.record_store.RESIDENCY)
        for batch_id, entry in batches.items():
            if not isinstance(entry, bl.BatchFacets):
                return {"ok": False, "refusal": "unknown-retain: bad-batch-entry"}
            # The ACTIVE materialization decides, not the entry flag: a batch
            # that was restaged has a live successor copy even though its own
            # `retired` says the first one is gone.
            try:
                if entry.stage_retired:
                    continue
            except oc.ProducedOutputError as exc:
                return {"ok": False, "refusal": str(exc)}
            return {"ok": False, "refusal": "active-batches-retain",
                    "batch_id": batch_id}
        for batch_id, entry in batches.items():
            assert isinstance(entry, bl.BatchFacets)
            tier = str(entry.tier or '')
            try:
                movers = entry.movers
            except oc.ProducedOutputError as exc:
                return {"ok": False, "refusal": str(exc)}
            for mover in movers:
                if mover and tier:
                    try:
                        if queue.tier(tier).holdings(mover):
                            return {"ok": False,
                                    "refusal": "active-movers-retain",
                                    "batch_id": batch_id}
                    except Exception as exc:
                        return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            ns = str(entry.namespace or '')
            if ns:
                try:
                    if _dir_has_entries(out_base / ns):
                        return {"ok": False, "refusal": "active-movers-retain",
                                "batch_id": batch_id}
                except oc.ProducedOutputError as exc:
                    return {"ok": False, "refusal": str(exc)}
        # Funding-intent-only movers: physical disposition is unknown and
        # metadata absence never proves physical absence, so every such
        # intent retains for the funding/reconciliation lane with a
        # specific recoverable refusal. No mover state (absent, withdrawn,
        # failed, or done) authorizes release here. Directory enumeration
        # names files by suffix without stat-gating: a stat failure must
        # surface at the intent read, never silently drop an intent.
        try:
            with os.scandir(LegacyFunding.directory(bl.scope_dir(queue.root, checked))) as iterator:
                intent_names = sorted(entry.name for entry in iterator
                                      if entry.name.endswith(".funding.json"))
        except FileNotFoundError:
            intent_names = []
        except OSError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        for name in intent_names:
            batch_id = name[:-len(".funding.json")]
            if batch_id in batches:
                continue
            try:
                funding = LegacyFunding(LegacyFunding.directory(bl.scope_dir(queue.root, checked)) / name).read()
            except oc.ProducedOutputError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            if funding is None:
                continue
            return {"ok": False,
                    "refusal": "funding-intent-reconcile-retain",
                    "batch_id": batch_id}
        # Live pins: structural per-namespace scan (owner + every batch
        # namespace, under the output fragment root where produced pins
        # live) around the SDK census over the same root. Either scan
        # finding pins retains; an unreadable scan retains as unknown.
        # Census paths attributable to this instance's recorded staged
        # paths retain by name; unattributable live paths retain as
        # unknown; unrelated consumers' pins never block this instance.
        owner = str(checked["owner_action_key"])
        attempt = checked["owner_attempt"]
        assert isinstance(attempt, dict)
        namespaces = [owner] + [
            str(entry.namespace or "") for entry in batches.values()
            if isinstance(entry, bl.BatchFacets) and entry.namespace
        ]
        for consumer in namespaces:
            if not consumer:
                continue
            pins = _lease_pin_files(sdk, queue, consumer,
                                    residency_root=str(out_base))
            if pins is None:
                return {"ok": False, "refusal": "unknown-retain: pin-scan"}
            if pins:
                return {"ok": False, "refusal": "live-refs-retain",
                        "pins": pins[:8]}
        live_paths, census_refusal = _live_output_paths(
            queue, sdk, batches, str(out_base))
        if census_refusal is not None:
            return {"ok": False, "refusal": census_refusal}
        if live_paths:
            return {"ok": False, "refusal": "live-refs-retain",
                    "paths": live_paths[:8]}
        for consumer in namespaces:
            if not consumer:
                continue
            pins = _lease_pin_files(sdk, queue, consumer,
                                    residency_root=str(out_base))
            if pins is None:
                return {"ok": False, "refusal": "unknown-retain: pin-scan"}
            if pins:
                return {"ok": False, "refusal": "live-refs-retain",
                        "pins": pins[:8]}
        lease_proof = "sdk-census-clean"
        # Owner containment: any live claim retains; an unreadable claim
        # retains as unknown and never falls through to an old terminal.
        # With no live claim, only the accepted SDK containment predicate
        # over the exact nonce/scope authorizes.
        try:
            observed = RecordStore(queue.root).claimed(owner)
            if isinstance(observed, Unknown):
                assert observed.error is not None
                raise observed.error
            live_claim = None if isinstance(observed, Absent) else observed.value
        except Exception as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if isinstance(live_claim, Mapping):
            control = live_claim.get("resource_scope")
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
            if (live_nonce == attempt["nonce"]
                    and live_scope == attempt["scope_id"]):
                return {"ok": False, "refusal": "owner-active-retain"}
            # A live claim for another attempt (successor or stranger) is
            # never freed by this attempt's terminal: exact containment or
            # nothing.
            return {"ok": False, "refusal": "owner-superseded-retain"}
        elif live_claim is not None:
            return {"ok": False, "refusal": "unknown-retain: claim-shape"}
        try:
            contained, reason = sdk.containment_certificate_ok(queue, {
                "action_key": owner,
                "nonce": attempt["nonce"],
                "scope_id": attempt["scope_id"],
            })
        except Exception as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if not contained:
            return {"ok": False, "refusal": str(reason)}
        released = 0
        holders: set[str] = set()
        for batch_id, entry in batches.items():
            assert isinstance(entry, bl.BatchFacets)
            # Holder identity comes from the validated immutable record
            # plus the canonically derived namespace -- never from mutable
            # commitments fields alone. A changed mover/namespace in
            # commitments releases nothing here.
            try:
                filed, _sealed = entry.open(queue.root, checked, checked_template, batch_id)
            except oc.ProducedOutputError as exc:
                return {"ok": False, "refusal": str(exc)}
            try:
                canonical_ns = oc.batch_namespace(
                    checked, batch_id, str(filed.get("manifest_digest") or ""))
            except oc.ProducedOutputError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            if (str(entry.namespace or '') != canonical_ns or str(filed.get('batch_namespace') or '') != canonical_ns):
                return {"ok": False,
                        "refusal": "unknown-retain: batch-target-mismatch"}
            holders.add(canonical_ns)
            mover = str(filed.get("mover_key") or "")
            if len(mover) == 64:
                holders.add(mover)
            # Every restage materialization held the same window under its own
            # sealed key; a leftover there is this instance's too.
            try:
                for extra in entry.history:
                    key = str(extra.mover_key or '')
                    if len(key) == 64:
                        holders.add(key)
            except oc.ProducedOutputError as exc:
                return {"ok": False, "refusal": str(exc)}
        for tier in checked_template["permitted_tiers"]:
            for holder in sorted(holders):
                try:
                    released += queue.tier(tier).egress(holder, certificate=queue.tier(tier).certify_release(holder))
                except Exception as exc:
                    return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        # Holders are empty post-transfer by construction; leftovers
        # release here, once, idempotently. Any release failure above
        # already retained with ok False -- this line is unreachable
        # after a release exception.
        return {"ok": True, "released": released, "lease_proof": lease_proof}


def output_scope_tick(queue, tiers: Mapping[str, object]) -> list[dict[str, object]]:
    """Deterministic read-only reconciliation for the tier-loop tick.

    NEW method owned by this lane. Staged means fragments compose AND the
    mover's receipt says complete; fragments whose completeness cannot be
    read are `output-recovery-unknown`, exactly as in `recover_batches`.
    For each bound instance: compose each
    unretired batch's fragments (existing validator, one manifest each),
    report staged/retire-needed events. No publishing (liveness), no deletion
    (lease), no placement. Proposed hook: call once per `tier_loop.cycle`
    after `residency_window` and extend its events (exact diff through root).
    """

    from prismabuild import pool as pool_mod
    from prismabuild import residency_map as map_mod

    events: list[dict[str, object]] = []
    scopes_root = bl.scopes_root(queue.root)
    try:
        owners = bl.scope_owners(scopes_root)
    except OSError:
        return events
    out_base = bl.output_fragment_root(queue.root / pool_mod.record_store.RESIDENCY)
    for owner in owners:
        try:
            candidates = bl.owner_instance_paths(scopes_root / owner)
        except OSError:
            continue
        for path in candidates:
            try:
                instance = oc.validate_instance(json.loads(path.read_text()))
            except (OSError, ValueError):
                continue
            try:
                commitments = bl.BatchLedger.for_instance(queue.root, instance).entries()
            except oc.ProducedOutputError:
                continue
            batches = commitments
            assert isinstance(batches, bl.BatchCapture)
            for batch_id, entry in batches.items():
                if not isinstance(entry, bl.BatchFacets):
                    continue
                # The ACTIVE materialization owns the material: after a
                # restage the entry flag describes a copy that is already
                # gone, and reading it here would report a live successor as
                # nothing to reconcile.
                try:
                    active = entry.active
                except oc.ProducedOutputError:
                    events.append({"event": "output-recovery-unknown",
                                   "batch_id": batch_id})
                    continue
                if active.retired:
                    continue
                ns = str(entry.namespace or '')
                try:
                    fragments = map_mod.read_fragments(out_base, ns)
                except Exception:
                    continue
                if not fragments:
                    events.append({"event": "output-batch-unstaged",
                                   "batch_id": batch_id, "namespace": ns})
                    continue
                try:
                    composed = map_mod.compose(fragments)
                except ValueError as exc:
                    events.append({"event": "output-batch-invalid",
                                   "batch_id": batch_id, "error": repr(exc)})
                    continue
                # Same question, same answer as `recover_batches`: composing
                # fragments prove bytes landed, not that the batch did, and
                # two censuses of one lane may not disagree about which.
                complete = bl.mover_receipt_complete(queue, str(active.mover_key or ''))
                if complete is not True:
                    events.append({"event": "output-recovery-unknown",
                                   "batch_id": batch_id, "namespace": ns})
                    continue
                entries = composed.get("entries")
                events.append({
                    "event": "output-batch-staged", "batch_id": batch_id,
                    "namespace": ns, "manifest_digest": str(entry.manifest),
                    "generation": int(active.generation or 0),
                    "entries": len(entries) if isinstance(entries, Mapping) else 0,
                })
    return events




def recover_batches(queue, instance: Mapping[str, object],
                    template: Mapping[str, object]) -> list[dict[str, object]]:
    """Classify every batch for deterministic recovery (read-only).

    NEW method owned by this lane. Uses existing receipts/ledgers/fragments
    only: staged (fragments compose AND the mover's receipt says complete),
    unstaged (no fragments, mover absent → due), mover-failed (FAILED with
    a fence still fundable → retry), mover-live (claimed, or ready with a
    fundable fence → wait), unfundable-retire (ready OR failed with a spent
    fence → the terminal route), unknown (unreadable scan, unreadable
    fence, or fragments whose completeness cannot be read → defer).
    Returns events in batch_id order; callers act through existing
    publish/evict paths, never here.

    A mover whose fence is spent is NOT work anyone can wait for or retry.
    `output_funded_cover` covers only a record in `transferring` and
    nothing re-funds a spent one, so once the claim has consumed the
    funding, that mover key can never be claimed again -- whether the retry
    ladder requeued the row READY or gave up on it and left it FAILED.
    BOTH branches therefore report `output-mover-unfundable-retire` with
    the terminal route the batch does have: retire it, reclaim the origin,
    and re-plan the work as a new batch. A `failed-retry` event pointing at
    a row no claim can cover is the same wait-forever defect as a
    `live-wait` one, and the two branches are decided by one shared
    question (`bl.funding_verdict`) so they cannot drift apart again.

    The verdict rests on ATTRIBUTABLE evidence and nothing weaker: the
    filed funding record, read through `output_funding_file_state` because
    this is a census path and `read_output_funding` cannot tell absent from
    corrupt (its own docstring says so). `consumed` is spent; `corrupt` is
    unknown; `absent` means no prepaid intent was ever filed, so the
    ordinary claim/retry path still applies; `released` beside an unretired
    batch is evidence disagreeing with itself. Holdings are NOT an input:
    an absent holding read is a moment, not a proof, and a mover that
    retains tokens for the bytes it staged is just as unclaimable as one
    that holds none. A CLAIMED row is never told to retire -- an executor
    owns it and the lease reaper is its recovery.
    """

    from prismabuild import pool as pool_mod
    from prismabuild import residency_map as map_mod

    checked_template = oc.validate_template(template)
    checked_instance = oc.validate_instance(instance)
    try:
        commitments = bl.BatchLedger.for_instance(queue.root, checked_instance).entries()
    except oc.ProducedOutputError as exc:
        return [{"event": "output-recovery-unknown", "error": repr(exc)}]
    batches = commitments
    assert isinstance(batches, bl.BatchCapture)
    out_base = bl.output_fragment_root(queue.root / pool_mod.record_store.RESIDENCY)
    events: list[dict[str, object]] = []
    try:
        funding_names = sorted(
            p.name for p in LegacyFunding.directory(bl.scope_dir(queue.root, checked_instance)).iterdir()
            if p.is_file() and p.name.endswith(".funding.json"))
    except OSError:
        funding_names = []
    for name in funding_names:
        batch_id = name[:-len(".funding.json")]
        if batch_id in batches:
            continue
        events.append({"event": "output-funding-intent-pending",
                       "batch_id": batch_id})
    for batch_id in sorted(batches):
        entry = batches[batch_id]
        if not isinstance(entry, bl.BatchFacets):
            events.append({"event": "output-recovery-unknown",
                           "batch_id": batch_id})
            continue
        # Which materialization is current. A restaged batch's own `retired`
        # flag describes a copy that is already gone; reporting it as retired
        # would orphan the live successor and its window credit, and a
        # malformed materialization list is unknown, never "nothing here".
        try:
            active = entry.active
        except oc.ProducedOutputError as exc:
            events.append({"event": "output-recovery-unknown",
                           "batch_id": batch_id, "error": repr(exc)})
            continue
        if str(active.source) == 'origin':
            # Committed at origin, never staged (#912): nothing to recover.
            events.append({"event": "output-batch-origin-only",
                           "batch_id": batch_id})
            continue
        if active.retired:
            events.append({
                "event": "output-batch-retired", "batch_id": batch_id,
                "generation": int(active.generation or 0),
            })
            continue
        ns = str(entry.namespace or '')
        mover = str(active.mover_key or '')
        if (str(active.source) == "materialization"
                and str(active.state) == "intent"
                and isinstance(RecordStore(queue.root).mover(mover), Absent)):
            # A filed restage intent whose mover row was never published:
            # the durable resumption point `ensure_batch_materialized` left
            # behind. It is neither staged nor lost; re-calling ensure
            # re-drives this exact sealed key.
            events.append({
                "event": "output-materialization-intent-pending",
                "batch_id": batch_id, "mover": mover,
                "generation": int(active.generation or 0),
                "action": "ensure-batch-materialized",
            })
            continue
        try:
            fragments = map_mod.read_fragments(out_base, ns) if ns else []
            composed = bool(fragments) and bool(map_mod.compose(fragments))
        except Exception as exc:
            events.append({"event": "output-recovery-unknown",
                           "batch_id": batch_id, "error": repr(exc)})
            continue
        # Fragments prove bytes landed; they do NOT prove the batch landed.
        # A mover that staged 700 of 1200 declared bytes composes exactly
        # like one that staged all of them, and reporting that as staged
        # tells the caller this batch needs nothing -- while its row is
        # unclaimable and its origin files are still the only copy of the
        # 500 bytes that never arrived. The mover's own receipt is what
        # says which it was, and an unreadable one leaves it unknown
        # rather than letting the fragments answer a question they cannot.
        complete = bl.mover_receipt_complete(queue, mover) if composed else None
        match RecordStore(queue.root).mover(mover) if mover else Absent():
            case Present(QueueRow(state, _row)):
                pass
            case Absent():
                state = "absent"
            case Unknown():
                state = "unknown"
        if composed and complete is True:
            events.append({"event": "output-batch-staged",
                           "batch_id": batch_id, "namespace": ns})
        elif composed and complete is None:
            events.append({"event": "output-recovery-unknown",
                           "batch_id": batch_id, "namespace": ns})
        elif state in ("failed", "ready"):
            verdict, reason = bl.funding_verdict(queue, mover, str(active.tier or ''))
            if verdict == "unknown":
                event = {"event": "output-recovery-unknown",
                         "batch_id": batch_id, "mover": mover}
                if reason is not None:
                    event["error"] = reason
                events.append(event)
            elif verdict == "spent":
                # Symmetric across both branches: a spent fence is spent
                # whether the ladder requeued the row or gave up on it, and
                # a retry event pointing at work no claim can ever cover is
                # the same wait-forever defect wearing the other state.
                events.append({"event": "output-mover-unfundable-retire",
                               "batch_id": batch_id, "mover": mover,
                               "action": "retire-reclaim-replan"})
            elif state == "failed":
                events.append({"event": "output-mover-failed-retry",
                               "batch_id": batch_id, "mover": mover})
            else:
                events.append({"event": "output-mover-live-wait",
                               "batch_id": batch_id, "mover": mover})
        elif state == "claimed":
            # An executor owns this row. Whatever its ledger reads say in
            # the instant this census runs, the recovery for a claim is the
            # lease reaper's, never a retire this function names.
            events.append({"event": "output-mover-live-wait",
                           "batch_id": batch_id, "mover": mover})
        elif state == "unknown":
            events.append({"event": "output-recovery-unknown",
                           "batch_id": batch_id})
        else:
            events.append({"event": "output-batch-unstaged",
                           "batch_id": batch_id, "namespace": ns})
    return events


__all__ = [
    "PRODUCED_OUTPUT_REF_SCHEMA_V1",
    "OUTPUT_TEMPLATES_SUBDIR",
    "OUTPUT_SCOPES_SUBDIR",
    "OUTPUT_BATCHES_SUBDIR",
    "OUTPUT_FRAGMENTS_SUBDIR",
    "batch_record",
    "batch_records",
    "BATCH_STATE_COMMITTED",
    "BATCH_STATE_RETIRING",
    "BATCH_STATE_RECLAIMED",
    "ProducedOutputError",
    "template_path",
    "declare_template",
    "bind_instance",
    "admit_funded_window",
    "commit_batch",
    "load_origin_batch",
    "load_origin_batches",
    "origin_batch_manifest",
    "place_origin_batches",
    "verify_placed_origin_batches",
    "publish_prepaid_batch",
    "refill_window",
    "unheld_window_gib",
    "build_stage_manifest",
    "retire_batch",
    "declare_origin_consumer",
    "release_origin_consumer",
    "origin_consumer_release",
    "blocked_origin_batches",
    "origin_retirement_tick",
    "safe_release_instance",
    "output_scope_tick",
    "recover_batches",
    "build_declaration",
    "declared_template",
    "bind_declared_instance",
]

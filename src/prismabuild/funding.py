"""Funding generations, named handoff repair and credit for window/output bindings."""
from __future__ import annotations

import json
import math
import os
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from . import batch_ledger as bl, output_contract as oc, record_store, storage_tiers, tier_holdings
from .record_store import PoolContractError, READY, CLAIMED, DONE, FAILED, WITHDRAWN, TIER_FUNDING, TIER_FUNDING_RETIRED

PRODUCED_OUTPUT_BATCH_REF_SCHEMA_V1 = "prismabuild.produced_output_batch_ref.v1"

TIER_FUNDING_SCHEMA_V1 = "prismabuild.tier_funding.v1"

TIER_FUNDING_STATES = frozenset({
    "reserved", "transferring", "consumed", "released",
})

_FUNDING_TRANSITIONS = {
    "reserved": frozenset({"transferring", "released"}),
    "transferring": frozenset({"consumed", "released"}),
}

_FUNDING_BINDING_FIELDS = frozenset({
    "schema", "tier_id", "consumer_action_key", "plan_sha256",
    "mover_action_key", "range_start_bytes", "range_end_bytes",
    "kind", "tokens", "generation", "published_unix",
})

TIER_FUNDING_OUTPUT_SCHEMA_V1 = "prismabuild.tier_funding.output.v1"

_FUNDING_OUTPUT_BINDING_FIELDS = frozenset({
    "schema", "tier_id", "kind", "mover_action_key", "tokens",
    "generation", "published_unix", "owner_action_key", "owner_nonce",
    "owner_scope_id", "owner_published_unix", "template_id",
    "template_sha256", "batch_id", "manifest_digest",
    "range_start_bytes", "range_end_bytes",
})

_FUNDING_KIND_ROLES = {
    "stage_gib": "mover_row",
    "ram_gib": "ram_mover_row",
}


@dataclass
class Handoff:
    """One chunk handoff: status, events, generation and moved count."""

    status: str = "deferred"
    events: list[str] = field(default_factory=list)
    generation: str | None = None
    moved: int = 0



@dataclass(frozen=True)
class CommitFunding:
    path: Path
    prepaid: Mapping[str, object] | None
    refusal: dict[str, object] | None = None


@dataclass
class AdvanceProtection:
    """Captured stage or RAM facts from one pass; absence grants no permission."""
    gated: dict[tuple[str, str], dict[str, object]] = field(default_factory=dict)
    protected: dict[tuple[str, str], dict[str, object]] = field(default_factory=dict)
    grants: dict[tuple[str, str], str] = field(default_factory=dict)
    permitted: dict[tuple[str, str], dict[str, object]] = field(default_factory=dict)
    prelaunch_open: set[tuple[str, str]] = field(default_factory=set)
    unknown_ready: bool = False
    unknown_tiers: set[str] = field(default_factory=set)
    unknown_consumers: set[tuple[str, str]] = field(default_factory=set)
    events: list[dict[str, object]] = field(default_factory=list)
    census: dict[str, dict[str, object]] | None = None
    census_consumers: list = field(default_factory=list)
    census_unknown: list = field(default_factory=list)

    def permits(self, key: str, tier_id: str) -> bool:
        return (not self.unknown_ready and tier_id not in self.unknown_tiers
                and (key, tier_id) not in self.unknown_consumers
                and (key, tier_id) not in self.gated
                and (key, tier_id) in self.permitted)


class FundingLedger:
    """One lazy domain owner; the queue retains claim and lifecycle policy."""

    def __init__(self, queue):
        self.queue = queue

    @staticmethod
    def validate_window(value: object) -> dict[str, object]:
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

    def read_window(self, mover_action_key: str,
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

        status, record, _reason = self.window_evidence(
            mover_action_key, tier_id)
        return record if status == "record" else None

    def window_evidence(self, mover_action_key: str,
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

        path = self.queue.records.funding_path(mover_action_key, tier_id)
        try:
            raw = record_store.read_document(path)
        except OSError as exc:
            return ("unknown", None, f"funding record unreadable: {exc!r}")
        except PoolContractError as exc:
            return ("unknown", None, f"funding record unparsable: {exc}")
        if raw is None:
            try:
                present = bool(record_store.value_or_raise(
                    self.queue.records.funding_presence(mover_action_key, tier_id)))
            except OSError as exc:
                return ("unknown", None,
                        f"funding record census unreadable: {exc!r}")
            if present:
                return ("unknown", None,
                        "funding record present but empty")
            return ("absent", None, None)
        try:
            return ("record", self.validate_window(raw), None)
        except (PoolContractError, ValueError) as exc:
            return ("unknown", None, f"funding record invalid: {exc}")

    def write_window(self, record: Mapping[str, object], *,
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
        See :meth:`_write_funding_record_locked` for the body.
        """

        checked = self.validate_window(record)
        mover = str(checked["mover_action_key"])
        with self.queue.mover_transition_lock(mover, blocking=False) as acquired:
            if not acquired:
                raise PoolContractError(
                    "funding writer lost the transition race")
            return self._write_locked(
                checked, expect_generation=expect_generation, binding="window")

    def _write_locked(
            self, record: Mapping[str, object], *,
            binding: Literal["window", "output"],
            expect_generation: str | None = None,
            rotate: bool = False) -> Path:
        """Persist one binding under its mover lock, keeping its wire variant.

        Ordinary writes preserve the binding and advance only legal states.
        The reserve path alone rotates to a fresh reserved generation.
        """
        if binding == "output":
            validate = self.validate_output
            binding_fields = _FUNDING_OUTPUT_BINDING_FIELDS
            label = "output funding"
        else:
            validate = self.validate_window
            binding_fields = _FUNDING_BINDING_FIELDS
            label = "funding"
        checked = validate(record)
        if rotate and checked.get("state") != "reserved":
            raise PoolContractError(f"rotated {label} generations are born reserved")
        mover, tier = str(checked["mover_action_key"]), str(checked["tier_id"])
        path = (self.queue.records.funding_output_path(mover, tier) if binding == "output"
                else self.queue.records.funding_path(mover, tier))
        path.parent.mkdir(parents=True, exist_ok=True)
        if binding == "output":
            self._refuse_retired_output_funding(checked)
        try:
            raw = record_store.read_document(path)
        except (OSError, PoolContractError):
            raise PoolContractError(f"{label} record unreadable for CAS")
        if raw is None:
            if expect_generation is not None:
                raise PoolContractError(f"{label} record vanished underneath this write")
            if not rotate and checked.get("state") != "reserved":
                raise PoolContractError(f"{label} records are born reserved")
        else:
            try:
                current = validate(raw)
            except (PoolContractError, ValueError):
                raise PoolContractError(f"{label} record unreadable for CAS")
            if (expect_generation is None
                    or str(current.get("generation")) != str(expect_generation)
                    or (not rotate and str(checked.get("generation")) != str(
                        current.get("generation")))):
                raise PoolContractError(f"{label} generation rotated underneath this write")
            if rotate:
                if str(checked.get("generation")) == str(current.get("generation")):
                    raise PoolContractError("rotation must mint a fresh generation")
            else:
                for field in binding_fields:
                    if current.get(field) != checked.get(field):
                        raise PoolContractError(
                            f"{label} {field} is immutable within one generation")
                if (str(checked.get("state")) != str(current.get("state"))
                        and str(checked.get("state")) not in _FUNDING_TRANSITIONS.get(
                            str(current.get("state")), frozenset())):
                    raise PoolContractError(
                        f"{label} state step "
                        f"{current.get('state')!r}->{checked.get('state')!r} "
                        "is not a legal advance")
        self.queue.records.replace_record_at(path, checked)
        return path

    def advance_window(self, mover_action_key: str, tier_id: str, *,
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

        with self.queue.mover_transition_lock(str(mover_action_key),
                                        blocking=False) as acquired:
            if not acquired:
                return False
            return self.advance_locked(
                mover_action_key, tier_id, expect=expect,
                advance_to=advance_to, generation=generation)

    def advance_locked(self, mover_action_key: str, tier_id: str, *,
                                      expect: str, advance_to: str,
                                      generation: str | None = None,
                                      binding: Literal["window", "output"] = "window") -> bool:
        """Move either funding binding one legal step, or refuse with ``False``.

        Caller holds the mover's transition lock (see
        :meth:`advance_funding_state`).

        Legal steps are :data:`_FUNDING_TRANSITIONS` -- the same table
        :meth:`_write_funding_record_locked` enforces, so the two can never
        disagree about what a step is.  When ``generation`` is given it must
        match the filed record: the check turns a stale writer into a loud
        ``False``, while the mover lock (held by the caller) is what keeps
        two writers from interleaving the read and the replace.  A fresh
        reservation never edits a live binding: it mints a new generation
        through the reserve path instead.
        """

        current = (self.read_output(mover_action_key, tier_id)
                   if binding == "output"
                   else self.read_window(mover_action_key, tier_id))
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
            write = (self.write_output if binding == "output"
                     else self.write_window)
            write(
                updated,
                expect_generation=(str(current.get("generation"))
                                   if isinstance(current.get("generation"),
                                                str) else None))
        except (OSError, PoolContractError, ValueError):
            return False
        return True

    def cover_window(self, tier_id: str, item: Mapping[str, object],
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
        record = self.read_window(key, tier_id)
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
                self.queue, str(consumer_key))
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
            held_names = self.queue.tier(tier_id).token_names(key)
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

    def reserve_window(self, tier_id: str, grant: str, fields: Mapping[str, object],
                      demand_gib: int) -> bool:
        """Hold one next-need under the grant key and bind it, idempotently.

        Serialized on the mover's transition lock (non-blocking: a claim
        holding it makes this defer rather than queue behind execution).
        See :meth:`_reserve_fence_locked` for the body.
        """

        mover = str(fields["mover_action_key"])
        with self.queue.mover_transition_lock(mover, blocking=False) as acquired:
            if not acquired:
                return False
            return self._reserve_window_locked(tier_id, grant, fields, demand_gib)

    def _reserve_window_locked(self, tier_id: str, grant: str,
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
        status, current, funding_reason = self.window_evidence(
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
                ledger = self.queue.tier(tier_id)
                kind = str(fields["kind"])
                mover_holds = int(ledger.holdings(mover).get(kind, 0))
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
                ledger = self.queue.tier(str(tier_id))
                ledger_names = ledger.holder_state(mover)
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
                        moved = int(self.queue.tier(str(tier_id)).handover(mover, grant))
                    except (OSError, PoolContractError, ValueError):
                        return False
                    # transfer moves the whole holder; keep is empty so the
                    # count must equal the bound size -- anything else is a
                    # partial move (crash/race) to finish next cycle.
                    if moved != len(bound_set):
                        # Complete the remainder if split, else retain.
                        try:
                            rest = ledger.holder_state(mover)
                        except (OSError, PoolContractError, ValueError):
                            return False
                        if rest:
                            return False
                        # Nothing left under the mover but the count is
                        # short: names collided under the grant (another
                        # incarnation's tokens) -- retain, do not double.
                        return False
                    if not self.advance_locked(
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
                    _status, current, _reason = self.window_evidence(
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
                        grant_names = self.queue.tier(str(tier_id)).holder_state(grant)
                    except (OSError, PoolContractError, ValueError):
                        return False
                    bound_set = {str(name) for name in bound}
                    if (bound_set <= (set(ledger_names) | grant_names)
                            and not (set(ledger_names) - bound_set)):
                        # All bound names accounted for, mover holds no
                        # extras: finish moving the remainder grant-ward,
                        # then close (both checked).
                        try:
                            self.queue.tier(str(tier_id)).handover(mover, grant)
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
                            ledger_after = self.queue.tier(str(tier_id))
                            mover_left = ledger_after.holder_state(mover)
                            grant_after = ledger_after.holder_state(grant)
                        except (OSError, PoolContractError, ValueError):
                            return False
                        if mover_left or not (bound_set <= grant_after):
                            return False
                        if not self.advance_locked(
                                mover, str(tier_id), expect="transferring",
                                advance_to="released",
                                generation=(str(current.get("generation"))
                                            if isinstance(
                                                current.get("generation"),
                                                str) else None)):
                            return False
                        _status, current, _reason = (
                            self.window_evidence(
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
                grant_names = self.queue.tier(str(tier_id)).holder_state(grant)
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
            ledger = self.queue.tier(tier_id)
            kind = str(fields["kind"])
            have = int(ledger.holdings(grant).get(kind, 0))
        except (OSError, PoolContractError, ValueError, KeyError, TypeError):
            return False
        deficit = int(demand_gib) - have
        if deficit > 0:
            try:
                if not ledger.take(grant, {kind: int(deficit)}):
                    # ``acquire`` is all-or-nothing: a ``False`` leaves
                    # nothing half-held, so there is nothing to unwind.
                    return False
            except (OSError, PoolContractError, ValueError):
                return False
        try:
            names = sorted(ledger.holder_state(grant))
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
                if bool(
                    record_store.value_or_raise(self.queue.records.funding_presence(mover, str(tier_id))),
                ):
                    # Appeared or unreadable since the read above (the
                    # mover lock makes the funding writer race impossible,
                    # so this is unknown evidence): retain, defer.
                    return False
                self.write_window(record)
            else:
                self._write_locked(
                    record, expect_generation=current_generation, binding="window", rotate=True)
        except (OSError, PoolContractError, ValueError):
            return False
        return True

    def _transfer_window(self, tier_id: str, grant: str, mover: str, *,
                       faults: list | None = None) -> int:
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
            if not (bool(record_store.value_or_raise(self.queue.records.item_presence(READY, mover)))
                    or bool(
                        record_store.value_or_raise(self.queue.records.item_presence(CLAIMED, mover)),
                    )):
                return 0
            return int(self.queue.tier(tier_id).handover(grant, mover, faults=faults))
        except (OSError, PoolContractError, ValueError):
            return 0

    @staticmethod
    def validate_output(value: object) -> dict[str, object]:
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

    def read_output(self, mover_action_key: str,
                            tier_id: str) -> dict[str, object] | None:
        """One output mover's funding record, or None when absent/unparsable.

        Note: None conflates absent with malformed/unreadable. Claim and
        census paths MUST use `output_funding_file_state` (absent vs unknown)
        instead of treating this None as legacy/no-funding; only the cover
        path (which requires a parsed transferring record) may use this.
        The retired record is read only when opening the live path reports
        ENOENT; present empty, invalid or unreadable data never falls back.
        """

        try:
            raw_bytes = self.queue.records.output_funding_bytes(
                mover_action_key, tier_id, max_bytes=None)
            if raw_bytes is None:
                return None
            raw = json.loads(raw_bytes)
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return None
        try:
            return self.validate_output(raw)
        except (PoolContractError, ValueError):
            return None

    def output_evidence(
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

        try:
            raw_bytes = self.queue.records.output_funding_bytes(
                mover_action_key, tier_id, max_bytes=1024 * 1024)
        except OSError:
            return (None, "corrupt")
        if raw_bytes is None:
            # Proven ENOENT only is absent. Any other failure (including
            # NotADirectoryError: a path component is a file, i.e. corrupt
            # namespace, never proof an intent never existed) is UNKNOWN.
            return (None, "absent")
        if len(raw_bytes) > 1024 * 1024:
            return (None, "corrupt")
        try:
            import json as _json
            raw = _json.loads(raw_bytes.decode())
        except (ValueError, UnicodeDecodeError):
            return (None, "corrupt")
        try:
            return (self.validate_output(raw), "ok")
        except (PoolContractError, ValueError):
            return (None, "corrupt")

    def write_output(self, record: Mapping[str, object], *,
                             expect_generation: str | None = None) -> Path:
        """File one output generation's binding, atomically.

        Same CAS + binding-immutability + table rules as V1, under the
        mover's transition lock (non-blocking; refuse when a claim holds
        it).  See _write_funding_record_locked for the body.
        """

        checked = self.validate_output(record)
        mover = str(checked["mover_action_key"])
        with self.queue.mover_transition_lock(mover, blocking=False) as acquired:
            if not acquired:
                raise PoolContractError(
                    "output funding writer lost the transition race")
            return self._write_locked(
                checked, expect_generation=expect_generation, binding="output")

    def _refuse_retired_output_funding(self, checked: Mapping[str, object]) -> None:
        """A retired record is terminal: no write may land beside it.

        Retirement requires a terminal mover holding nothing on the tier, and
        the produced-output lane mints a new mover key for every
        materialization, so no legal writer ever addresses a retired record.
        A write that did would file a second record for one mover and tier,
        which is exactly what the per-mover address forbids.
        """

        retired = self.queue.records.funding_output_retired_path(
            str(checked["mover_action_key"]), str(checked["tier_id"]))
        try:
            os.stat(retired)
        except FileNotFoundError:
            return
        except OSError:
            raise PoolContractError("retired output funding unreadable for CAS")
        raise PoolContractError(
            "output funding record is retired; a terminal record admits no write")

    def retire_output(self) -> dict[str, object]:
        """Move terminal output funding records out of the census directory.

        ``consumed`` is a terminal funding state and nothing advances it, so
        without this every produced batch left one record that every census
        read for ever: 973 of them on 2026-09-22, read three times per finish
        (once per tier), 0.8 s of NFS reads to conclude an action that held
        nothing.  A record is retired only when every fact that could still
        make it count is proven:

        * its state is ``consumed`` or ``released``;
        * its mover has a filed ``done``, ``failed`` or ``withdrawn`` record
          and no ``ready`` or ``claimed`` row;
        * its mover has no lease; and
        * its mover holds no token on the record's tier.

        The facts are re-read under the mover's transition lock before the
        rename, and any read failure keeps the record where it is.  Retired
        records stay readable at their per-mover address through
        :meth:`read_output_funding` and :meth:`output_funding_file_state`, so
        every decision about one mover reads what it read before.  Only the
        directory scans stop reading them: :meth:`output_census_for_owner`
        never counted a ``consumed`` record, and
        :meth:`_output_outstanding_window_tokens` counts one only while its
        mover holds tokens, which a retired mover proved it does not.

        Idempotent and safe to run beside the census.  Returns counts.
        """

        funding_dir = self.queue.root / TIER_FUNDING
        suffix = ".output-funding.json"
        counts = {"scanned": 0, "retired": 0, "kept": 0, "busy": 0,
                  "unreadable": 0}
        try:
            names = sorted(entry.name for entry in os.scandir(funding_dir)
                           if entry.name.endswith(suffix) and entry.is_file(
                               follow_symlinks=False))
        except FileNotFoundError:
            return counts
        except OSError:
            counts["unreadable"] += 1
            return counts
        retired_dir = funding_dir / TIER_FUNDING_RETIRED
        for name in names:
            counts["scanned"] += 1
            mover, dot, tier_id = name[: -len(suffix)].partition(".")
            if not dot or len(mover) != 64 or not tier_id:
                counts["kept"] += 1
                continue
            try:
                ledger = self.queue.tier(tier_id)
            except (OSError, PoolContractError, ValueError):
                counts["kept"] += 1
                continue
            if not self._output_funding_terminal(mover, tier_id, ledger):
                counts["kept"] += 1
                continue
            with self.queue.mover_transition_lock(mover, blocking=False) as acquired:
                if not acquired:
                    counts["busy"] += 1
                    continue
                if not self._output_funding_terminal(mover, tier_id, ledger):
                    counts["kept"] += 1
                    continue
                try:
                    retired_dir.mkdir(exist_ok=True)
                    target = retired_dir / name
                    if os.path.lexists(target):
                        counts["kept"] += 1
                        continue
                    os.rename(funding_dir / name, target)
                except OSError:
                    counts["unreadable"] += 1
                    continue
            counts["retired"] += 1
        return counts

    def _output_funding_terminal(self, mover: str, tier_id: str,
                                 ledger: "tier_holdings.TierLedger") -> bool:
        """Every retirement fact for one record, fail-closed (see above)."""

        record, file_state = self.output_evidence(mover, tier_id)
        if file_state != "ok" or not isinstance(record, Mapping):
            return False
        if str(record.get("state")) not in ("consumed", "released"):
            return False
        try:
            for state in (READY, CLAIMED):
                if os.path.lexists(self.queue.records.item_path(state, mover)):
                    return False
            if not any(os.path.lexists(self.queue.records.item_path(state, mover))
                       for state in (DONE, FAILED, WITHDRAWN)):
                return False
            if os.path.lexists(self.queue.records.lease_path(mover)):
                return False
            if ledger.holder_has_entries(mover):
                return False
        except FileNotFoundError:
            # The holder directory is absent: the mover holds nothing here.
            return True
        except (OSError, PoolContractError, ValueError):
            return False
        return True

    def _output_live_owner(self, owner_key: str) -> tuple[dict | None, str | None]:
        """Live owner CLAIMED row + its published_unix, or (None, refusal)."""

        try:
            live = record_store.value_or_raise(self.queue.records.item(CLAIMED, owner_key))
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
                row = record_store.value_or_raise(self.queue.records.item(state, mover_key))
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
            live = record_store.value_or_raise(self.queue.records.item(CLAIMED, owner_key))
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
                terminal = record_store.value_or_raise(self.queue.records.item(state, owner_key))
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
            inst_path = (bl.attempt_scope(self.queue.root, owner_key, template_id,
                                          owner_nonce) / "instance.json")
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
                instance = oc.validate_instance(
                    _json.loads(raw_inst.decode()))
            except (ValueError, UnicodeDecodeError,
                    oc.ProducedOutputError):
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
            tmpl_path = (Path(self.queue.root) / "residency"
                         / oc.OUTPUT_TEMPLATES_SUBDIR
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
                checked_template = oc.validate_template(
                    _json.loads(raw_tmpl.decode()))
            except (ValueError, UnicodeDecodeError,
                    oc.ProducedOutputError):
                return False
        except (OSError, PoolContractError, ValueError):
            return False
        if oc.template_sha256(checked_template) != template_sha:
            return False
        # Commitments entry (existing helper; small file per instance).
        try:
            commitments = bl.BatchLedger.for_instance(self.queue.root, instance).capture()
        except oc.ProducedOutputError:
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
                live = bl.BatchFacets(entry).pending_copy
            except oc.ProducedOutputError:
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
            filed, _ = bl.BatchLedger.open(
                self.queue.root, instance, checked_template, entry, batch_id)
        except oc.ProducedOutputError:
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
            inst_path = (bl.attempt_scope(self.queue.root, owner_key, template_id,
                                          owner_nonce) / "instance.json")
            try:
                with open(inst_path, "rb") as handle:
                    raw_inst = handle.read(64 * 1024 + 1)
            except (FileNotFoundError, OSError):
                return False
            if len(raw_inst) > 64 * 1024:
                return False
            try:
                instance = oc.validate_instance(
                    _json.loads(raw_inst.decode()))
            except (ValueError, UnicodeDecodeError,
                    oc.ProducedOutputError):
                return False
        except (OSError, PoolContractError, ValueError):
            return False
        if (str(instance.get("template_sha256")) != template_sha):
            return False
        try:
            prewrite = bl.read_prewrite(
                bl.prewrites_dir(self.queue.root, instance)
                / f"{batch_id}.prewrite.json")
        except oc.ProducedOutputError:
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

    def stage_output(self, *, tier_id: str, owner_key: str,
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
            tier = self.queue.records.check_tier_id(str(tier_id))
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
            binding = bl.BatchLedger.precommit(
                self.queue, instance, template, batch_id, descriptors,
                tier, str(mover_key))
        except oc.ProducedOutputError as exc:
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
            checked_template = oc.validate_template(template)
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
            ledger = self.queue.tier(tier)
            held_names = sorted(name for name in ledger.token_names(str(owner_key)) if name.startswith(kind + '-'))
        except (OSError, PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if self.read_output(str(mover_key), tier) is not None:
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
                    return {'ok': False, 'refusal': 'tier-reservation-unavailable', 'available': ledger.free_supply()}
                selected = unspoken_names[:batch_gib]
        with self.queue.action_transition_lock(str(owner_key),
                                     blocking=False) as owner_acquired:
            if not owner_acquired:
                return {"ok": False, "refusal": "funding-race-deferred"}
            with self.queue.mover_transition_lock(str(mover_key),
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
                current = self.read_output(str(mover_key), tier)
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
                    self._write_locked(
                        record, expect_generation=None, binding="output")
                except (OSError, PoolContractError, ValueError) as exc:
                    return {"ok": False, "refusal": f"unknown-retain: {exc}"}
                return {"ok": True, "generation": generation,
                        "tokens": sorted(selected), "moved": 0,
                        "staged": True}

    def fund_output(self, *, tier_id: str, owner_key: str,
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
            tier = self.queue.records.check_tier_id(str(tier_id))
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
            binding = bl.BatchLedger.precommit(
                self.queue, instance, template, batch_id, descriptors,
                tier, str(mover_key))
        except oc.ProducedOutputError as exc:
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
            checked_template = oc.validate_template(template)
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
            ledger = self.queue.tier(tier)
        except (OSError, PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        try:
            held_names = sorted(name for name in ledger.token_names(str(owner_key)) if name.startswith(kind + '-'))
        except (OSError, PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        # Same disjointness rule as stage_output_intent (R7 liveness): a
        # name already promised to one of this owner's outstanding intents
        # is spoken for. This block only selects for a FRESH intent: when
        # a record already exists for this mover/tier, its own filed set
        # is authoritative (and is itself spoken, by itself), so selection
        # is skipped and the in-lock reconciliation drives the filed set.
        if self.read_output(str(mover_key), tier) is not None:
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
                    return {'ok': False, 'refusal': 'tier-reservation-unavailable', 'available': ledger.free_supply()}
                selected = unspoken_names[:batch_gib]
        # Owner outer, mover inner (R1): serializes against owner finish.
        with self.queue.action_transition_lock(str(owner_key),
                                     blocking=False) as owner_acquired:
            if not owner_acquired:
                return {"ok": False, "refusal": "funding-race-deferred"}
            with self.queue.mover_transition_lock(str(mover_key),
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
            ledger = self.queue.tier(tier)
            held_now = ledger.token_names(owner_key)
        except (OSError, PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        # Existing record for this mover: idempotent re-drive, never a
        # second authoritative intent.
        current = self.read_output(mover_key, tier)
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
                    self._write_locked(
                        _rotated, expect_generation=str(
                            current.get("generation")), binding="output", rotate=True)
                except (OSError, PoolContractError, ValueError) as exc:
                    return {"ok": False, "refusal": f"unknown-retain: {exc}"}
                current = self.read_output(mover_key, tier)
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
                self._write_locked(record, expect_generation=None, binding="output")
            except (OSError, PoolContractError, ValueError) as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            current = self.read_output(mover_key, tier)
            if current is None:
                return {"ok": False, "refusal": "unknown-retain: funding-unreadable"}
            generation = str(current.get("generation"))
        # Transfer the exact filed set (re-drive safe: already-moved counts).
        filed = self.read_output(mover_key, tier)
        if filed is None:
            return {"ok": False, "refusal": "unknown-retain: funding-unreadable"}
        names = [str(n) for n in filed.get("tokens", [])]  # type: ignore[union-attr]
        try:
            moved = int(ledger.handover_named(owner_key, mover_key, names))
        except (OSError, PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if moved < len(names):
            return {"ok": False, "refusal": "transfer-short",
                    "moved": moved, "expected": len(names),
                    "generation": generation}
        if not self.advance_locked(
                mover_key, tier, expect="reserved",
                advance_to="transferring", generation=generation, binding="output"):
            # Already transferring with same generation is success (retry
            # after crash between transfer and advance).
            check = self.read_output(mover_key, tier)
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
        record = self.read_output(key, str(tier_id))
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
            held_names = self.queue.tier(str(tier_id)).token_names(key)
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

    def drive_output(self, mover_action_key: str,
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
            tier = self.queue.records.check_tier_id(str(tier_id))
        except (PoolContractError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        mover = str(mover_action_key)
        # Read owner without locks to learn which owner lock to take.
        probe = self.read_output(mover, tier)
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
        with self.queue.action_transition_lock(owner_key, blocking=False) as owner_acquired:
            if not owner_acquired:
                return {"ok": False, "refusal": "funding-race-deferred"}
            with self.queue.mover_transition_lock(mover, blocking=False) as mover_acquired:
                if not mover_acquired:
                    return {"ok": False, "refusal": "funding-race-deferred"}
                record = self.read_output(mover, tier)
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
                        self._write_locked(
                            rotated,
                            expect_generation=str(record.get("generation")), binding="output", rotate=True)
                    except (OSError, PoolContractError, ValueError) as exc:
                        return {"ok": False, "refusal": f"unknown-retain: {exc}"}
                    record = self.read_output(mover, tier)
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
                    _live_row = record_store.value_or_raise(
                        self.queue.records.item(CLAIMED, str(record.get("owner_action_key"))),
                    )
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
                    ledger = self.queue.tier(tier)
                    moved = int(ledger.handover_named(owner_key, mover, names))
                except (OSError, PoolContractError, ValueError) as exc:
                    return {"ok": False, "refusal": f"unknown-retain: {exc}"}
                if moved < len(names):
                    return {"ok": False, "refusal": "transfer-short",
                            "moved": moved, "expected": len(names),
                            "generation": str(record.get("generation"))}
                if record.get("state") == "reserved":
                    if not self.advance_locked(
                            mover, tier, expect="reserved",
                            advance_to="transferring",
                            generation=str(record.get("generation")), binding="output"):
                        check = self.read_output(mover, tier)
                        if not (isinstance(check, dict)
                                and check.get("state") == "transferring"
                                and str(check.get("generation")) == str(
                                    record.get("generation"))):
                            return {"ok": False, "refusal": "funding-race-deferred"}
                return {"ok": True, "generation": str(record.get("generation")),
                        "tokens": names, "moved": moved}

    def release_output(self, mover_action_key: str, tier_id: str, *,
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
        with self.queue.mover_transition_lock(mover, blocking=False) as acquired:
            if not acquired:
                return False
            # The public API answers a bool: an undecided read fault is a
            # non-release for it, never a strand (#1202 review N3 makes the
            # distinction only for the dead-input ending, which retries).
            released = self._release_never_started_funding_locked(
                mover, str(tier_id), generation=generation)
            return released is True

    def _release_never_started_funding_locked(
            self, mover_action_key: str, tier_id: str, *,
            generation: str | None = None,
            dead_input: Mapping[str, object] | None = None) -> bool | None:
        """Retire an output intent only when mover nonexecution is proven (R2).

        The lock-free body of :meth:`release_output_funding`; the caller
        holds the mover's transition lock (#1202's dead-input ending calls
        this before it files its terminal, while no terminal row exists,
        which is the one context in which the FAILED check below can never
        fire).  Refuses (retain, never free) unless the mover provably never
        started: no CLAIMED row, no DONE row, no FAILED row, no move receipt
        with staged bytes, no lease, and no claimed terminal carrying this
        funding generation. Missing marker/status is NOT proof a copy never
        ran (failed/uncertain prefixes retain both names and accounting);
        only the true not-started cancellation (published READY, never
        claimed, no receipt/lease/terminal) retires credit once via
        reserved|transferring -> released. Tokens stay where they are;
        ordinary owner/mover release then frees them.

        The answer is a tri-state (#1202 review N3): ``True`` released,
        ``False`` refused on proof (the mover started, the record is
        foreign, absent, or already settled), and ``None`` undecided -- a
        read fault anywhere inside.  A caller that files a terminal on the
        strength of this answer must treat ``None`` as "retry", never as a
        refusal, because a terminal row makes the public release refuse
        forever and strands the token.
        """

        mover = str(mover_action_key)
        try:
            current, file_state = self.output_evidence(
                mover, str(tier_id))
        except (OSError, ValueError, PoolContractError):
            return None                    # a read fault is undecided
        if file_state == "absent":
            return False                   # nothing to release is a settled answer
        if file_state != "ok" or not isinstance(current, Mapping):
            return None                    # an unreadable record is undecided
        if (generation is not None
                and str(current.get("generation")) != str(generation)):
            return False
        state = str(current.get("state"))
        if state == "released":
            return True
        if state not in ("reserved", "transferring"):
            return False
        proven = self._output_funding_nonexecution_locked(
            current, dead_input=dead_input)
        if proven is not True:
            return proven
        if self.advance_locked(
                mover, str(tier_id), expect=state, advance_to="released",
                generation=str(current.get("generation")), binding="output"):
            return True
        # Every proven refusal answered above; a False here is a read or
        # write fault under the advance's own idempotence, which the next
        # pass retries (#1202 review N3).
        return None

    def _output_funding_nonexecution_locked(
            self, current: Mapping[str, object], *,
            dead_input: Mapping[str, object] | None = None) -> bool | None:
        """Prove nonexecution without a state change; hold the mover lock.

        Return True for proof, False for refusal, or None for a read fault.
        A released marker alone does not prove that the mover never started.
        """

        mover = str(current["mover_action_key"])
        if dead_input is not None:
            # #1202's dead-input ending: the caller proved this funding's
            # producer attempt dead and its bound origin gone, so the batch's
            # claim can never come and the committed-batch refusal below
            # would strand the credit.  The funding must still name the same
            # batch the proof proved dead; a foreign record is retained.
            for field in ("batch_id", "manifest_digest", "tier_id",
                          "owner_action_key", "owner_nonce"):
                if str(current.get(field)) != str(dead_input.get(field)):
                    return False
        # Committed batches are recovery, not cancellation (R7 liveness):
        # once the immutable batch record + commitments entry exist, the
        # credit belongs to that batch's claim (or committed recovery
        # after producer finish), and retiring it here would leave a
        # filed batch whose sealed mover key can never claim or re-fund.
        try:
            if (dead_input is None
                    and self._output_batch_authority(current)):
                return False
        except (OSError, PoolContractError, ValueError):
            return None
        # Durable claim: CLAIMED row of any shape means the mover may hold
        # the fence while the consumed marker failed.
        try:
            claimed = record_store.value_or_raise(self.queue.records.item(CLAIMED, mover))
        except (OSError, PoolContractError):
            return None
        if isinstance(claimed, Mapping):
            return False
        # Terminals: DONE or FAILED of any status means the mover started
        # (or a successor did); a funded generation carried into the
        # terminal proves it took this fence. Uncertain/missing terminal
        # reads fail closed (retain).
        for terminal_state in (DONE, FAILED):
            try:
                terminal = record_store.value_or_raise(self.queue.records.item(terminal_state, mover))
            except (OSError, PoolContractError):
                return None
            if not isinstance(terminal, Mapping):
                continue
            # Any terminal for this key is proof of execution start.
            return False
        # Physical lifetime: move receipt with any staged bytes (complete
        # or partial, refused or not) means bytes may be on the stage.
        try:
            receipt = self.queue.move_record(mover)
        except (OSError, PoolContractError, ValueError):
            return None
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
            lease = record_store.read_document(self.queue.records.lease_path(mover))
        except (OSError, PoolContractError):
            return None
        if isinstance(lease, Mapping):
            return False

        return True

    def output_census(self, owner_key: str) -> tuple[list[dict], bool]:
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

        A released marker with held mover tokens remains outstanding until
        token release completes. Retain its prewrite recovery authority.
        """

        intents: list[dict] = []
        funding_dir = self.queue.root / TIER_FUNDING
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
                raw = record_store.read_document(path)
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
                record = self.validate_output(raw)
            except (PoolContractError, ValueError):
                # Corrupt binding that names this owner: may own
                # source-held names => unknown, retain.
                unknown = True
                continue
            state = str(record.get("state"))
            if state in ("reserved", "transferring"):
                intents.append(record)
            elif state == "released":
                try:
                    if self.queue.tier(str(record['tier_id'])).holder_state(str(record['mover_action_key'])):
                        intents.append(record)
                except (OSError, PoolContractError, ValueError):
                    unknown = True
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

        intents, unknown = self.output_census(str(owner_key))
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
            ledger = self.queue.tier(str(tier_id))
            held_names = ledger.token_names(str(owner_key))
        except (OSError, PoolContractError, ValueError):
            return (0, True)
        spoken, unknown = self.output_census(str(owner_key))
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
        funding_dir = self.queue.root / TIER_FUNDING
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
                record, file_state = self.output_evidence(
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
                    mover_held = int(ledger.holdings(str(record.get('mover_action_key'))).get(kind, 0))
                except (OSError, PoolContractError, ValueError):
                    return (outstanding, True)
                live = min(len(tokens), mover_held)
                if live > 0:
                    outstanding += live
        return (outstanding, False)

    def output_keep_names_for_owner(
            self, owner_key: str, tier_id: str, *,
            census: Callable[[], tuple[list[dict], bool]] | None = None,
    ) -> tuple[set[str], bool]:
        """(keep_set, unknown) for owner finish (R2 fail-retain + R3 scandir).

        Holdings enumerated with explicit `os.scandir` classification: proven
        ENOENT/NotADirectory (no holder dir) is empty; any other read failure
        is UNKNOWN (retain all). Census UNKNOWN likewise retains all.

        The census is read only when the owner holds something on this tier.
        The keep set is a subset of the holdings and UNKNOWN retains only
        holdings, so an owner holding nothing gets the same answer from no
        census as from a clean one. That is the case of almost every
        finishing action, and the census reads every output funding record
        in the pool. A caller concluding one key across several tiers passes
        ``census``, a callable returning one shared census, because the
        census names every tier and one read answers them all.
        """

        try:
            ledger = self.queue.tier(str(tier_id))
            try:
                held = ledger.holder_entries(str(owner_key))
            except FileNotFoundError:
                held = set()
            except NotADirectoryError:
                return (set(), True)
            except OSError:
                return (set(), True)
        except (OSError, PoolContractError, ValueError):
            return (set(), True)
        if not held:
            return (set(), False)
        intents, unknown = (census() if census is not None
                            else self.output_census(str(owner_key)))
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

    def pending_window(self, ledger: "tier_holdings.TierLedger", tier_id: str,
                            action_key: str,
                            sealed: Mapping[str, object] | None) -> str | None:
        """``"pending"``, ``"unknown"`` or ``None`` for this row's ``reserved`` fence.

        A publication or a recovery files the generation first and moves the
        tokens after.  A claim in that gap would pay its full demand and then
        hold the fence on top of it (#1637).  ``"pending"``: the record names
        this row's publication and every bound token is held by some holder.
        ``"unknown"``: the funding record is unreadable or malformed, or a
        holder directory could not be read.  The record exists and may still
        bind tokens, so none of these proves the fence absent (see
        :meth:`read_funding_evidence` and :func:`held_names_visible`), and the
        claim defers and asks again.  ``None``: proven absent or stale -- no
        record, another state or publication, or bound tokens that are gone --
        and the claim pays as before, so nothing waits on a fence that cannot
        arrive.
        """
        if not isinstance(sealed, Mapping):
            return None
        try:
            status, record, _why = self.window_evidence(
                action_key, tier_id)
        except (OSError, PoolContractError, ValueError):
            return "unknown"
        if status == "unknown":
            return "unknown"
        if status != "record" or record is None:
            return None
        if record.get("state") != "reserved":
            return None
        try:
            if float(record["published_unix"]) != float(sealed["published_unix"]):
                return None
        except (KeyError, TypeError, ValueError):
            return None
        tokens = record.get("tokens")
        if not isinstance(tokens, list) or not tokens:
            return None
        held: set[str] = set()
        try:
            for holder in ledger.holders():
                held |= ledger.holder_state(holder)
        except (OSError, PoolContractError, ValueError):
            return "unknown"
        return "pending" if {str(name) for name in tokens} <= held else None


    @staticmethod
    def _chunk_leg_mover(leg: Mapping[str, object]) -> str:
        """The mover key a leg hands its tokens to, or ValueError."""
        if not isinstance(leg, Mapping):
            raise ValueError("a chunk leg must be an object")
        mover = leg.get("mover_action_key")
        if not isinstance(mover, str) or not mover:
            raise ValueError("a chunk leg names its mover")
        return mover


    @staticmethod
    def _chunk_leg_size(leg: Mapping[str, object]) -> tuple[int, int, int]:
        """A leg's (start, end, gib), or ValueError for junk."""
        try:
            start = int(leg["start_bytes"])  # type: ignore[index]
            end = int(leg["end_bytes"])  # type: ignore[index]
            gib = int(leg["stage_gib"])  # type: ignore[index]
        except (KeyError, TypeError, ValueError):
            raise ValueError("a chunk leg carries whole start, end and gib")
        if (isinstance(leg["stage_gib"], bool) or end <= start or gib <= 0):  # type: ignore[index]
            raise ValueError("a chunk leg spans a positive range and demand")
        return (start, end, gib)


    @staticmethod
    def _chunk_funding_matches(record: Mapping[str, object], tier_id: str, mover: str,
                         kind: str, consumer: str, digest: str,
                         start: int, end: int, published: float) -> bool:
        """True when one record binds this chunk's exact publication."""
        try:
            row_published = float(record["published_unix"])  # type: ignore[index]
        except (KeyError, TypeError, ValueError):
            return False
        return (str(record.get("tier_id")) == str(tier_id)
                and str(record.get("mover_action_key")) == mover
                and str(record.get("kind")) == kind
                and str(record.get("consumer_action_key")) == consumer
                and str(record.get("plan_sha256")) == digest
                and str(record.get("range_start_bytes")) == str(start)
                and str(record.get("range_end_bytes")) == str(end)
                and row_published == published)


    @staticmethod
    def _chunk_record_tokens(record: Mapping[str, object]) -> list[str] | None:
        """The bound token names, or None when the field misbehaves."""
        tokens = record.get("tokens")
        if (not isinstance(tokens, list) or not tokens
                or any(not isinstance(name, str) or not name for name in tokens)):
            return None
        return [str(name) for name in tokens]


    def _chunk_birth_record(self, tier_id: str, mover: str, kind: str,
                      consumer: str, digest: str, start: int, end: int,
                      published: float, tokens: list[str]) -> str | None:
        """File one reserved generation; None when the write loses its race."""
        payload = {"schema": TIER_FUNDING_SCHEMA_V1, "tier_id": str(tier_id),
                   "consumer_action_key": consumer, "plan_sha256": digest,
                   "mover_action_key": mover, "range_start_bytes": start,
                   "range_end_bytes": end, "kind": kind, "tokens": sorted(tokens),
                   "generation": uuid.uuid4().hex, "state": "reserved",
                   "unix": time.time(), "published_unix": published}
        try:
            self.write_window(payload)
        except (OSError, PoolContractError, ValueError):
            return None
        try:
            filed = self.read_window(mover, tier_id)
        except (OSError, PoolContractError, ValueError):
            return None
        if filed is None or str(filed.get("state")) != "reserved":
            return None
        generation = filed.get("generation")
        return str(generation) if isinstance(generation, str) else None


    def _chunk_row_is_ready(self, mover: str) -> bool:
        """True while the mover's row waits to be claimed, never once claimed."""
        return (bool(
            record_store.value_or_raise(
                self.queue.records.item_presence(record_store.READY, mover),
            ),
        )
                and not bool(
                    record_store.value_or_raise(
                        self.queue.records.item_presence(record_store.CLAIMED, mover),
                    ),
                ))


    def _chunk_rotate_record(self, mover: str, tier_id: str,
                       expect_generation: str, fresh: Mapping[str, object],
                       *, expect_state: str | None = None) -> str | None:
        """Replace one generation under the mover lock; None on any loss.

        ``expect_state`` also requires, under the lock a claim holds from its
        tier acquire to its consumed-marking, that the record is in that state
        and the row is still READY: a recovery never rotates a live claim.
        """
        with self.queue.mover_transition_lock(mover, blocking=False) as acquired:
            if not acquired:
                return None
            try:
                status, current, _ = self.window_evidence(mover, tier_id)
            except (OSError, PoolContractError, ValueError):
                return None
            if (status != "record" or current is None
                    or str(current.get("generation")) != expect_generation):
                return None
            if expect_state is not None and (
                    str(current.get("state")) != expect_state
                    or not self._chunk_row_is_ready(mover)):
                return None
            try:
                self._write_locked(
                    dict(fresh), expect_generation=expect_generation,
                    binding="window", rotate=True)
            except (OSError, PoolContractError, ValueError):
                return None
        return str(fresh["generation"])


    def _chunk_advance_record(self, mover: str, tier_id: str,
                        generation: str) -> bool:
        """Close reserved into transferring; False defers to the next pass."""
        try:
            return bool(self.advance_window(
                mover, tier_id, expect="reserved", advance_to="transferring",
                generation=generation))
        except (OSError, PoolContractError, ValueError):
            return False


    @staticmethod
    def _chunk_fresh_binding(tier_id: str, mover: str, kind: str, consumer: str,
                       digest: str, start: int, end: int, published: float,
                       tokens: list[str]) -> dict[str, object]:
        """One reserved generation binding an exact token set."""
        return {"schema": TIER_FUNDING_SCHEMA_V1, "tier_id": str(tier_id),
                "consumer_action_key": consumer, "plan_sha256": digest,
                "mover_action_key": mover, "range_start_bytes": start,
                "range_end_bytes": end, "kind": kind, "tokens": sorted(tokens),
                "generation": uuid.uuid4().hex, "state": "reserved",
                "unix": time.time(), "published_unix": published}


    def _chunk_rebind_retained(self, ledger: tier_holdings.TierLedger,
                         tier_id: str, mover: str, kind: str, consumer: str,
                         digest: str, start: int, end: int, published: float,
                         gib: int, holder: str, tokens: list[str],
                         held: set[str], rotate_from: str,
                         expect_state: str | None = None,
                         ) -> tuple[str, set[str], set[str]] | Handoff:
        """Bind a mover's retained tokens and the group's exact remainder.

        Returns the new generation with the retained and the bound token sets,
        or the outcome that ends this pass.  Nothing moves before the rename.
        """
        if held - set(tokens):
            return Handoff("refused", ["prelaunch-mover-occupied"],
                                  None, 0)
        prior = set(tokens) & held
        try:
            names = ledger.holder_state(str(holder))
        except (OSError, PoolContractError, ValueError):
            return Handoff("deferred", ["prelaunch-unknown-evidence"],
                                  None, 0)
        need = gib - len(prior)
        kind_names = sorted(n for n in names if n.startswith(kind + "-"))
        if need < 0 or len(kind_names) < need:
            return Handoff("short", ["prelaunch-funding-short"], None, 0)
        bound = prior | set(kind_names[:need])
        fresh = self._chunk_fresh_binding(tier_id, mover, kind, consumer, digest,
                               start, end, published, sorted(bound))
        generation = self._chunk_rotate_record(mover, tier_id, rotate_from, fresh,
                                    expect_state=expect_state)
        if generation is None:
            return Handoff("deferred", ["prelaunch-publish-deferred"],
                                  None, 0)
        return (generation, prior, bound)


    def bind_chunk(self, tier_id: str, unit: str, holder: str,
                      plan: Mapping[str, object], leg: Mapping[str, object],
                      mover_row_published_unix: float) -> Handoff:
        """Hand one leg's tokens to its mover and bind them as its fence.

        The mover lock is held from the holdings read to the ``transferring``
        update.  A claim takes the same lock from its tier acquire to its
        consumed-marking, so it can neither pay a second charge in the gap between
        the rotation and the transfer nor spend a fence that is half moved (#1637).
        A busy lock defers to the next pass.
        """
        mover = self._chunk_leg_mover(leg)
        with self.queue.mover_transition_lock(mover, blocking=False) as acquired:
            if not acquired:
                return Handoff("deferred", ["prelaunch-publish-deferred"],
                                      None, 0)
            return self._bind_chunk_locked(tier_id, unit, holder, plan, leg,
                                  mover_row_published_unix)


    def _bind_chunk_locked(self, tier_id: str, unit: str, holder: str,
                       plan: Mapping[str, object], leg: Mapping[str, object],
                       mover_row_published_unix: float) -> Handoff:
        """The publication itself, under the mover lock its caller holds."""
        from . import residency_plan as plans
        mover = self._chunk_leg_mover(leg)
        start, end, gib = self._chunk_leg_size(leg)
        try:
            published = float(mover_row_published_unix)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            raise ValueError("the mover publication time must be a timestamp")
        if published < 0:
            raise ValueError("the mover publication time never goes negative")
        try:
            consumer = str(plan["consumer_action_key"])  # type: ignore[index]
            digest = plans.plan_sha256(plan)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"the chunk needs its live plan: {exc!r}")
        kind = storage_tiers.capacity_kind_of(tier_id)
        ledger = self.queue.tier(tier_id)
        if not (bool(
            record_store.value_or_raise(
                self.queue.records.item_presence(record_store.READY, mover),
            ),
        )
                or bool(
                    record_store.value_or_raise(
                        self.queue.records.item_presence(record_store.CLAIMED, mover),
                    ),
                )):
            return Handoff("refused", ["prelaunch-mover-unpublished"],
                                  None, 0)
        try:
            status, record, _ = self.window_evidence(mover, tier_id)
        except (OSError, PoolContractError, ValueError):
            return Handoff("deferred", ["prelaunch-unknown-evidence"],
                                  None, 0)
        if status == "unknown":
            return Handoff("deferred", ["prelaunch-unknown-evidence"],
                                  None, 0)
        generation: str | None = None
        prior: set[str] = set()
        rotate_from: str | None = None
        bound: set[str] | None = None
        if record is not None and self._chunk_funding_matches(
                record, tier_id, mover, kind, consumer, digest,
                start, end, published):
            tokens = self._chunk_record_tokens(record)
            generation = (str(record["generation"])
                          if isinstance(record.get("generation"), str) else None)
            if tokens is None or generation is None:
                return Handoff("deferred", ["prelaunch-unknown-evidence"],
                                      None, 0)
            held = self._chunk_holdings(ledger, mover)
            if held is None:
                return Handoff("deferred", ["prelaunch-unknown-evidence"],
                                      None, 0)
            state = str(record.get("state"))
            if state in ("transferring", "consumed") and set(tokens) <= held:
                return Handoff("already", [], generation, 0)
            if state == "transferring" and self._chunk_row_is_ready(mover):
                # A READY row whose bound tokens left (a recovered group): bind
                # what it still holds and the group's exact remainder, and only
                # while the record is still ``transferring`` under the lock (#1637).
                rebound = self._chunk_rebind_retained(ledger, tier_id, mover, kind, consumer, digest,
                    start, end, published, gib, holder, tokens, held,
                    generation, expect_state="transferring")
                if isinstance(rebound, Handoff):
                    return rebound
                generation, prior, bound = rebound
            else:
                if state == "transferring" or state == "consumed":
                    return Handoff("short", ["prelaunch-funding-short"],
                                          generation, 0)
                if state != "reserved":
                    return Handoff("deferred",
                                          ["prelaunch-unknown-evidence"], None, 0)
                prior = set(tokens) & held
                bound = set(tokens)
        elif record is not None:
            old = record.get("generation")
            rotate_from = str(old) if isinstance(old, str) else None
            tokens = self._chunk_record_tokens(record)
            republished = (tokens is not None
                           and self._chunk_republished(record, tier_id, mover, kind,
                                            consumer, digest, start, end))
            # Movers are keyed by manifest range, so a second capture of one
            # manifest names the first capture's movers.  What the first left is
            # a spent record bound to another consumer and plan.  It covers
            # nothing (only ``transferring`` funds a claim) and no state leaves
            # it, so it is rotated like an older publication of this chunk -- but
            # only when the mover holds no tokens (#1628).  A live record of
            # another consumer is still occupied.
            spent = str(record.get("state")) in ("consumed", "released")
            if rotate_from is None or not (republished or spent):
                return Handoff("refused", ["prelaunch-mover-occupied"],
                                      None, 0)
            held = self._chunk_holdings(ledger, mover)
            if held is None:
                return Handoff("deferred", ["prelaunch-unknown-evidence"],
                                      None, 0)
            if republished:
                rebound = self._chunk_rebind_retained(ledger, tier_id, mover, kind, consumer, digest,
                    start, end, published, gib, holder, tokens, held,
                    rotate_from)
                if isinstance(rebound, Handoff):
                    return rebound
                generation, prior, bound = rebound
                rotate_from = None
            else:
                if held:
                    return Handoff("refused", ["prelaunch-mover-occupied"],
                                          None, 0)
                # Establish the new reserved generation BEFORE any token moves,
                # naming tokens still in the group's holder, the way the
                # no-record path below does.  Rotating after the transfer would
                # leave tokens under the mover beside the old spent record when
                # the rotation is deferred or the process stops mid-transfer, and
                # the retry would find a held mover and refuse it for good.  With
                # the binding first, a retry finds a matching record and resumes.
                try:
                    names = ledger.holder_state(str(holder))
                except (OSError, PoolContractError, ValueError):
                    return Handoff("deferred",
                                          ["prelaunch-unknown-evidence"], None, 0)
                kind_names = sorted(n for n in names if n.startswith(kind + "-"))
                if len(kind_names) < gib:
                    return Handoff("short", ["prelaunch-funding-short"],
                                          None, 0)
                bound = set(kind_names[:gib])
                fresh = self._chunk_fresh_binding(tier_id, mover, kind, consumer, digest,
                                       start, end, published, kind_names[:gib])
                generation = self._chunk_rotate_record(mover, tier_id, rotate_from,
                                            fresh)
                if generation is None:
                    return Handoff("deferred",
                                          ["prelaunch-publish-deferred"], None, 0)
                rotate_from = None
                prior = set()
        else:
            held = self._chunk_holdings(ledger, mover)
            if held is None:
                return Handoff("deferred", ["prelaunch-unknown-evidence"],
                                      None, 0)
            if held:
                return Handoff("refused", ["prelaunch-mover-occupied"],
                                      None, 0)
            if generation is None:
                try:
                    names = ledger.holder_state(str(holder))
                except (OSError, PoolContractError, ValueError):
                    return Handoff("deferred",
                                          ["prelaunch-unknown-evidence"], None, 0)
                kind_names = sorted(n for n in names if n.startswith(kind + "-"))
                if len(kind_names) < gib:
                    return Handoff("short", ["prelaunch-funding-short"],
                                          None, 0)
                bound = set(kind_names[:gib])
                generation = self._chunk_birth_record(tier_id, mover, kind, consumer,
                                           digest, start, end, published,
                                           kind_names[:gib])
                if generation is None:
                    return Handoff("deferred",
                                          ["prelaunch-publish-deferred"], None, 0)
                try:
                    reread = self.read_window(mover, tier_id)
                except (OSError, PoolContractError, ValueError):
                    reread = None
                tokens = self._chunk_record_tokens(reread) if reread is not None else None
                if tokens is None or set(tokens) != bound:
                    return Handoff("deferred",
                                          ["prelaunch-unknown-evidence"], None, 0)
                prior = set()
        need = gib - len(prior)
        if need > 0:
            try:
                before = set(ledger.holder_state(mover))
            except (OSError, PoolContractError, ValueError):
                return Handoff("deferred", ["prelaunch-unknown-evidence"],
                                      generation, 0)
            try:
                moved = int(self.queue.tier(tier_id).handover_count(str(holder), mover, need))
            except (OSError, PoolContractError, ValueError):
                return Handoff("deferred", ["prelaunch-unknown-evidence"],
                                      generation, 0)
            if moved < need:
                return Handoff("short", ["prelaunch-funding-short"],
                                      generation, moved)
            try:
                after = set(ledger.holder_state(mover))
            except (OSError, PoolContractError, ValueError):
                return Handoff("deferred", ["prelaunch-unknown-evidence"],
                                      generation, 0)
            fresh_names = sorted(n for n in (prior | (after - before))
                                 if n.startswith(kind + "-"))
        else:
            moved = 0
            try:
                current = set(ledger.holder_state(mover))
            except (OSError, PoolContractError, ValueError):
                return Handoff("deferred", ["prelaunch-unknown-evidence"],
                                      generation, 0)
            fresh_names = sorted(n for n in current if n.startswith(kind + "-"))
        if generation is None:
            return Handoff("deferred", ["prelaunch-unknown-evidence"],
                                  None, moved)
        if bound is not None and not bound <= set(fresh_names):
            if len(fresh_names) != gib:
                return Handoff("short", ["prelaunch-funding-short"],
                                      generation, moved)
            fresh = self._chunk_fresh_binding(tier_id, mover, kind, consumer, digest,
                                   start, end, published, fresh_names)
            generation = self._chunk_rotate_record(mover, tier_id, generation, fresh)
            if generation is None:
                return Handoff("deferred", ["prelaunch-publish-deferred"],
                                      None, moved)
        if self._chunk_advance_record(mover, tier_id, generation):
            return Handoff("published", ["prelaunch-chunk-published"],
                                  generation, moved)
        return Handoff("deferred", ["prelaunch-publish-deferred"],
                              generation, moved)


    @staticmethod
    def _chunk_republished(record: Mapping[str, object], tier_id: str, mover: str,
                     kind: str, consumer: str, digest: str,
                     start: int, end: int) -> bool:
        """True when one record binds this chunk under an older publication."""
        return (str(record.get("tier_id")) == str(tier_id)
                and str(record.get("mover_action_key")) == mover
                and str(record.get("kind")) == kind
                and str(record.get("consumer_action_key")) == consumer
                and str(record.get("plan_sha256")) == digest
                and str(record.get("range_start_bytes")) == str(start)
                and str(record.get("range_end_bytes")) == str(end))

    @staticmethod
    def _chunk_holdings(ledger, mover):
        try:
            return set(ledger.holder_state(mover))
        except (OSError, PoolContractError, ValueError):
            return None


    def settle_live_row(self,
                         mover: str) -> dict[str, object] | None:
        """One live ready or claimed row, or None when the key has none."""
        row = record_store.value_or_raise(self.queue.records.ready_first_live_row(mover))
        return dict(row) if isinstance(row, Mapping) else None


    def settle_terminal_is_stale(self, mover: str,
                                  live_row: Mapping[str, object]) -> bool | None:
        """Whether every terminal record predates one live row: stale, live, unknown.

        True when each readable done/failed record finished before the live
        row published: the terminal belongs to an earlier generation and the
        live republication keeps its fence (#1690). False when any record
        proves the live attempt itself ended. None when nothing proves the
        order, and the caller settles as before.
        """
        try:
            row_published = float(live_row.get("published_unix"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None
        seen = False
        for state in (record_store.DONE, record_store.FAILED):
            try:
                ended = record_store.value_or_raise(self.queue.records.item(state, mover))
            except (OSError, PoolContractError):
                return None
            if ended is None:
                continue
            if not isinstance(ended, Mapping):
                return None
            seen = True
            try:
                finished = float(ended.get("finished_unix"))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                return None
            if finished >= row_published:
                return False
        return True if seen else None


    def settle_terminal_fence(self, ledger, *, tier_id: str,
                               mover_role: str, consumer: str | None,
                               mover: str) -> list[dict[str, object]]:
        """Release one terminal mover's exact unspent fence, never its physical.

        Shared by the protected loop (mover terminal with a live entry) and the
        funding scan below (mover terminal with no live entry -- a withdrawn or
        failed consumer's stranded fence, which no protected entry visits).
        ``consumed`` records are landed bytes: finish kept them on purpose and
        only an egress may return them.  A ``transferring`` record whose
        generation and token set match the terminal attempt's own
        ``tier_funding`` proof is that attempt's fence-or-bytes, never free
        credit.  Anything else releases only on an exact name match between the
        bound tokens and what the mover holds; the record always closes.
        A terminal record filed before the live row's own publication is an
        earlier generation: the live republication keeps its fence (#1690).
        Unreadable terminal, proof, or holder evidence defers with the record
        named and leaves recoverable authority intact -- absence of proof is
        never proof of absence.  Takes the mover's transition lock
        non-blocking (a live claim wins, this defers), so both call sites
        are safe locked or not.
        """

        events: list[dict[str, object]] = []
        try:
            with self.queue.action_transition_lock(mover, blocking=False) as acquired:
                if not acquired:
                    return events
                status, record, funding_reason = self.window_evidence(
                    mover, str(tier_id))
                if status == "unknown":
                    # A present-but-unreadable record may still bind held
                    # tokens: name it and retain, never settle beside it.
                    events.append({"event": "advance-deferred-unknown-evidence",
                                   "consumer": consumer, "tier_id": str(tier_id),
                                   "leg": str(mover_role),
                                   "error": funding_reason
                                   or "funding record unreadable"})
                    return events
                if status == "absent":
                    return events
                state = str(record.get("state"))
                if state in ("consumed", "released"):
                    return events
                try:
                    live_row = self.settle_live_row(mover)
                    terminal = (bool(
                        record_store.value_or_raise(
                            self.queue.records.item_presence(record_store.DONE, mover),
                        ),
                    )
                                or bool(
                                    record_store.value_or_raise(
                                        self.queue.records.item_presence(record_store.FAILED, mover),
                                    ),
                                )
                                or bool(
                                    record_store.value_or_raise(
                                        self.queue.records.item_presence(record_store.WITHDRAWN, mover),
                                    ),
                                ))
                except (OSError, PoolContractError) as exc:
                    events.append({"event": "advance-deferred-unknown-evidence",
                                   "consumer": consumer, "tier_id": str(tier_id),
                                   "leg": str(mover_role),
                                   "error": f"terminal census unreadable: {exc!r}"})
                    return events
                if not terminal:
                    return events
                if live_row is not None:
                    try:
                        stale = self.settle_terminal_is_stale(mover, live_row)
                    except (OSError, PoolContractError, ValueError):
                        stale = None
                    if stale is True:
                        return events
                bound = record.get("tokens")
                bound_names = (set(str(name) for name in bound)
                               if isinstance(bound, list) and bound else set())
                bound_generation = (str(record.get("generation"))
                                    if isinstance(record.get("generation"), str)
                                    else None)
                if not bound_names or bound_generation is None:
                    return events
                if state == "transferring":
                    bound_to_attempt = False
                    proof_unknown: Exception | None = None
                    proof_reason: str | None = None
                    for _state in (record_store.DONE, record_store.FAILED):
                        try:
                            ended = record_store.value_or_raise(self.queue.records.item(_state, mover))
                        except (OSError, PoolContractError) as exc:
                            # A terminal file that exists but cannot be read
                            # may bind this fence: defer, do not free.
                            try:
                                if bool(
                                    record_store.value_or_raise(
                                        self.queue.records.item_presence(_state, mover),
                                    ),
                                ):
                                    proof_unknown = exc
                                    break
                            except (OSError, PoolContractError) as exc2:
                                proof_unknown = exc2
                                break
                            continue
                        if ended is None:
                            # ``_read_json`` answers ``None`` for ENOENT and
                            # for a present-but-empty file.  Only ENOENT is
                            # absence: a zero-byte proof is present and
                            # unproved -- it may be this attempt's own torn
                            # binding, so it defers like any unreadable one.
                            try:
                                if bool(
                                    record_store.value_or_raise(
                                        self.queue.records.item_presence(_state, mover),
                                    ),
                                ):
                                    proof_reason = (
                                        f"terminal proof present but empty: "
                                        f"{self.queue.records.item_path(_state, mover)}")
                                    break
                            except (OSError, PoolContractError) as exc2:
                                proof_unknown = exc2
                                break
                            continue
                        if not isinstance(ended, Mapping):
                            continue
                        proof = ended.get("tier_funding")
                        if not isinstance(proof, Mapping):
                            continue
                        tier_proof = proof.get(str(tier_id))
                        if not isinstance(tier_proof, Mapping):
                            continue
                        try:
                            proof_names = {
                                str(name) for name in
                                tier_proof.get("tokens") or []}
                        except (TypeError, ValueError) as exc:
                            # A malformed present proof is unknown evidence,
                            # not positive absence of the binding.
                            proof_unknown = exc
                            break
                        if (str(tier_proof.get("generation")) == bound_generation
                                and proof_names == bound_names
                                and proof_names):
                            bound_to_attempt = True
                            break
                    if bound_to_attempt:
                        return events
                    if proof_unknown is not None or proof_reason is not None:
                        events.append({"event": "advance-deferred-unknown-evidence",
                                       "consumer": consumer,
                                       "tier_id": str(tier_id),
                                       "leg": str(mover_role),
                                       "error": (
                                           f"terminal proof unreadable: "
                                           f"{proof_unknown!r}"
                                           if proof_unknown is not None
                                           else proof_reason)})
                        return events
                try:
                    # Error-visible census (the accepted #742 semantics):
                    # ``Path.glob`` hides an ``EACCES`` holder directory as an
                    # empty listing, which would silently strand a held fence
                    # while the record closes.  True absence (no holder
                    # directory) reads empty; unreadable defers below.
                    mover_names = ledger.holder_state(mover)
                except (OSError, PoolContractError, ValueError) as exc:
                    events.append({"event": "advance-deferred-unknown-evidence",
                                   "consumer": consumer, "tier_id": str(tier_id),
                                   "leg": str(mover_role),
                                   "error": f"holder census unreadable: {exc!r}"})
                    return events
                if mover_names == bound_names:
                    released = self.queue.tier(tier_id).cancel_fence(mover)['released']
                    if released:
                        events.append({"event": "advance-released",
                                       "consumer": consumer,
                                       "tier_id": str(tier_id),
                                       "leg": str(mover_role),
                                       "reason": "mover-terminal-fused",
                                       "released_gib": released})
                self.advance_window(
                    mover, str(tier_id), expect=state,
                    advance_to="released", generation=bound_generation)
        except (OSError, PoolContractError, ValueError):
            pass
        return events


    def stranded_producer_dead(self,
                                record: Mapping[str, object]
                                ) -> tuple[str, str]:
        """The bound producer attempt's state, read by the lane's rule.

        Returns ``(state, owner)``: ``dead``, ``succeeded``, ``live`` or
        ``unknown`` over the instance the funding record names, through
        ``_producer_attempt_state``. Any unreadable or disagreeing step
        answers ``unknown`` and retains; a live or unreadable verdict
        changes nothing.
        """

        try:
            owner = str(record.get("owner_action_key") or "")
            nonce = str(record.get("owner_nonce") or "")
            template_id = str(record.get("template_id") or "")
            batch_id = str(record.get("batch_id") or "")
            owner_scope = str(record.get("owner_scope_id") or "")
        except (AttributeError, TypeError, ValueError):
            return ("unknown", "")
        if (not record_store.key_shaped(owner) or not nonce or not template_id
                or not batch_id or not owner_scope):
            return ("unknown", owner)
        try:
            residency = self.queue.root / record_store.RESIDENCY
            scope = bl.attempt_scope(self.queue.root, owner, template_id, nonce)
            instance = oc.validate_instance(
                json.loads((scope / "instance.json").read_text()))
            template = oc.validate_template(json.loads(
                (residency / oc.OUTPUT_TEMPLATES_SUBDIR
                 / f"{template_id}.json").read_text()))
        except (OSError, ValueError):
            return ("unknown", owner)
        attempt = instance.get("owner_attempt")
        if (not isinstance(attempt, Mapping)
                or bl.scope_dir(self.queue.root, instance) != scope
                or str(attempt.get("nonce")) != nonce
                or str(attempt.get("scope_id")) != owner_scope
                or oc.template_sha256(template)
                != instance.get("template_sha256")
                or str(record.get("template_sha256"))
                != instance.get("template_sha256")):
            return ("unknown", owner)
        try:
            return (str(bl.producer_attempt_state(self.queue, instance)), owner)
        except (OSError, ValueError, PoolContractError):
            return ("unknown", owner)


    def stranded_prewrite_paths_absent(self,
                                        record: Mapping[str, object]
                                        ) -> tuple[bool, str]:
        """Whether every prewrite path of the bound batch is proven absent.

        Returns ``(absent, reason)``. A present or unreadable path (any
        ``lstat`` fault but ``ENOENT``), a missing or corrupt prewrite
        record, or a record that fails validation answers absent False:
        present or unprovable retains, only ``ENOENT`` on every named
        path proves the mover never laid bytes.
        """

        try:
            owner = str(record.get("owner_action_key") or "")
            nonce = str(record.get("owner_nonce") or "")
            template_id = str(record.get("template_id") or "")
            batch_id = str(record.get("batch_id") or "")
            tier_id = str(record.get("tier_id") or "")
            owner_scope = str(record.get("owner_scope_id") or "")
        except (AttributeError, TypeError, ValueError):
            return (False, "funding binding unreadable")
        if (not record_store.key_shaped(owner) or not nonce or not template_id
                or not batch_id or not tier_id or not owner_scope):
            return (False, "funding binding unreadable")
        try:
            residency = self.queue.root / record_store.RESIDENCY
            scope = bl.attempt_scope(self.queue.root, owner, template_id, nonce)
            instance = oc.validate_instance(
                json.loads((scope / "instance.json").read_text()))
        except (OSError, ValueError):
            return (False, "producer instance unreadable")
        attempt = instance.get("owner_attempt")
        if (not isinstance(attempt, Mapping)
                or bl.scope_dir(self.queue.root, instance) != scope
                or str(attempt.get("nonce")) != nonce
                or str(attempt.get("scope_id")) != owner_scope):
            return (False, "funding, instance and attempt disagree")
        try:
            prewrite = bl.read_prewrite(
                bl.prewrites_dir(self.queue.root, instance)
                / f"{batch_id}.prewrite.json")
        except oc.ProducedOutputError as exc:
            return (False, f"prewrite unreadable: {exc}")
        if prewrite is None:
            return (False, "prewrite record absent")
        if str(prewrite.get("tier")) != tier_id:
            return (False, "prewrite names another tier")
        if (str(prewrite.get("owner_action_key")) != owner
                or dict(prewrite.get("owner_attempt", {}))
                != dict(instance.get("owner_attempt", {}))):
            return (False, "prewrite names another attempt")
        for planned in (prewrite.get("paths", []) or []):
            try:
                os.lstat(str(planned))
            except FileNotFoundError:
                continue
            except OSError as exc:
                return (False, f"prewrite path unreadable: {planned}: {exc!r}")
            return (False, f"prewrite path present: {planned}")
        return (True, "")


    def reconcile_stranded_output_funding(self,
                                             path) -> list[dict[str, object]]:
        """Roll back one never-started output intent through the pool (#1555).

        Holds the mover's transition lock from the first re-read to the
        token release, so a publication or claim between the proof and
        the release cannot meet freed tokens: ``publish`` and ``claim``
        take the same per-key lock, so a republished row either lands
        before this lock (and the fresh census refuses) or after it
        (and the new claim takes tokens this already returned). Acts
        only when all hold under that lock: the producer attempt ended
        in failure (``dead`` by ``_producer_attempt_state``; ``live``,
        ``succeeded`` and ``unknown`` refuse), the mover is neither
        ready nor claimed, the record is ``transferring`` or ``released``,
        and every prewrite path of the bound batch is proven absent.
        Present or unreadable paths retain the reservation.
        The pool's nonexecution proof refuses committed batches, terminal
        or claimed movers, staged bytes, and live leases.
        Release tokens through ``TierLedger.release_unspent_best_effort`` first.
        Verify that no tokens remain before the ordinary funding rollback.
        A fault can leave ``transferring`` with no tokens; the next pass
        completes the rollback under the same lock and fresh checks.
        An old ``released`` record with held tokens follows the same proof.
        A live or unreadable verdict changes nothing.
        """
        events: list[dict[str, object]] = []
        stem = Path(str(path)).name
        if not stem.endswith(".output-funding.json"):
            return events
        stem = stem[: -len(".output-funding.json")]
        mover_name, dot, scan_tier = stem.rpartition(".")
        if not dot or len(mover_name) != 64 or not scan_tier:
            return events
        try:
            with self.queue.action_transition_lock(mover_name,
                                          blocking=False) as acquired:
                if not acquired:
                    return events
                events.extend(self.reconcile_stranded_output_funding_locked(mover_name, scan_tier))
        except (OSError, PoolContractError, ValueError):
            pass
        return events


    def reconcile_stranded_output_funding_locked(self,
                                                  mover_name: str, scan_tier: str
                                                  ) -> list[dict[str, object]]:
        """Locked body of the stranded repair; the caller holds the lock."""

        events: list[dict[str, object]] = []

        def defer(reason: str) -> list[dict[str, object]]:
            events.append({"event": "output-funding-reconcile-deferred",
                           "tier_id": str(scan_tier), "mover": mover_name,
                           "error": reason})
            return events

        try:
            for state in (record_store.READY, record_store.CLAIMED):
                if record_store.value_or_raise(
                    self.queue.records.item(state, mover_name),
                ) is not None:
                    return events
            withdrawn = record_store.value_or_raise(
                self.queue.records.item(record_store.WITHDRAWN, mover_name),
            )
            if withdrawn is None:
                return events
        except (OSError, PoolContractError, ValueError) as exc:
            return defer(f"mover census unreadable: {exc!r}")
        try:
            record, file_state = self.output_evidence(
                mover_name, scan_tier)
        except (OSError, PoolContractError, ValueError) as exc:
            return defer(repr(exc))
        if file_state == "absent":
            return events
        if file_state != "ok" or not isinstance(record, Mapping):
            return defer(f"funding file state: {file_state}")
        state = str(record.get("state"))
        if state not in ("transferring", "released"):
            return events
        if (str(record.get("mover_action_key") or "") != mover_name
                or str(record.get("tier_id") or "") != scan_tier):
            return defer("funding record names another mover or tier")
        producer, _owner = self.stranded_producer_dead(record)
        if producer != "dead":
            if producer == "unknown":
                return defer("producer verdict unreadable")
            return events
        absent, why = self.stranded_prewrite_paths_absent(record)
        if not absent:
            if why.startswith("prewrite path present"):
                return events
            return defer(why)
        try:
            outcome = self._output_funding_nonexecution_locked(record)
        except (OSError, PoolContractError, ValueError) as exc:
            return defer(repr(exc))
        if outcome is None:
            return defer("nonexecution proof undecided")
        if outcome is not True:
            return events
        try:
            freed = int(self.queue.tier(scan_tier).release_unspent_best_effort(mover_name, authority=tier_holdings.Unspent.NEVER_STARTED))
            # The ordinary release can swallow an I/O error or return after
            # only some token moves. Its count is not proof of an empty holder.
            if self.queue.tier(scan_tier).holder_state(mover_name):
                return defer("token release incomplete")
        except (OSError, PoolContractError, ValueError) as exc:
            return defer(repr(exc))
        if freed:
            events.append({"event": "output-funding-tokens-released",
                           "tier_id": str(scan_tier), "mover": mover_name,
                           "released_gib": freed})
        if state == "transferring":
            try:
                outcome = self._release_never_started_funding_locked(
                    mover_name, scan_tier,
                    generation=str(record.get("generation")))
            except (OSError, PoolContractError, ValueError) as exc:
                return defer(repr(exc))
            if outcome is None:
                return defer("funding rollback undecided")
            if outcome is not True:
                return events
            events.append({"event": "output-funding-reconcile-released",
                           "tier_id": str(scan_tier), "mover": mover_name})
        return events


    def settle_windows(self,
                            protection: AdvanceProtection) -> list[dict[str, object]]:
        """Move held fences onto their published movers; release the moot ones.

        Transfer runs under the mover's transition lock (never blocking: a busy
        mover belongs to a live claim, which wins), so a ready claim racing the
        coordinator cannot interleave mid-move.  ``transfer`` never lets a token
        touch free, and a crash part-way leaves the sum split across the two
        holders -- the next cycle completes the remainder by the same rule, which
        is the whole partial-transfer recovery proof.  The mover's claim then
        counts the fused fence toward its demand (funded-claim bookkeeping), so
        nothing is ever charged twice.

        Exact-set discipline throughout: a terminal mover releases only what
        matches its funding record's names (anything fused beyond that belongs to
        an owner path -- finish, withdrawal, egress -- which serves it, and
        ``consumed`` stays owned for that reason); an unpublished, nonterminal
        mover keeps its recoverable ``transferring`` record for a later
        publication or the terminal scan -- the accepted state machine has no
        transferring-to-reserved step.
        """

        events: list[dict[str, object]] = []
        protected = protection.protected
        assert isinstance(protected, dict)
        seen: set[tuple[str, str]] = set()
        for (key, tier_id), entry in sorted(protected.items()):
            assert isinstance(entry, dict)
            if (key, tier_id) in seen:
                continue
            seen.add((key, tier_id))
            ledger = self.queue.tier(str(tier_id))
            grant = str(entry["grant"])
            mover = str(entry["mover"])
            kind = str(entry["kind"])
            try:
                locked = self.queue.action_transition_lock(mover, blocking=False)
            except (OSError, PoolContractError, ValueError):
                continue
            try:
                with locked as acquired:
                    if not acquired:
                        continue
                    record = self.read_window(mover, str(tier_id))
                    try:
                        held_grant = int(ledger.holdings(grant).get(kind, 0))
                        held_mover = int(ledger.holdings(mover).get(kind, 0))
                    except (OSError, PoolContractError, ValueError):
                        continue
                    try:
                        terminal = (bool(
                            record_store.value_or_raise(
                                self.queue.records.item_presence(record_store.DONE, mover),
                            ),
                        )
                                    or bool(
                                        record_store.value_or_raise(
                                            self.queue.records.item_presence(record_store.FAILED, mover),
                                        ),
                                    )
                                    or bool(
                                        record_store.value_or_raise(
                                            self.queue.records.item_presence(record_store.WITHDRAWN, mover),
                                        ),
                                    ))
                        rowed = (bool(
                            record_store.value_or_raise(
                                self.queue.records.item_presence(record_store.READY, mover),
                            ),
                        )
                                 or bool(
                                     record_store.value_or_raise(
                                         self.queue.records.item_presence(record_store.CLAIMED, mover),
                                     ),
                                 ))
                    except (OSError, PoolContractError):
                        continue
                    bound = (record.get("tokens") if isinstance(record, dict)
                             else None)
                    bound_names = (set(str(name) for name in bound)
                                   if isinstance(bound, list) and bound else set())
                    bound_generation = (str(record.get("generation"))
                                        if isinstance(record, dict)
                                        and isinstance(record.get("generation"),
                                                       str) else None)
                    if terminal:
                        # Owner paths serve whatever a terminal mover holds beyond
                        # the fence; the exact unspent fence is decided in
                        # _settle_terminal_fence (unknown evidence defers there).
                        if held_grant > 0:
                            released = self.queue.tier(tier_id).cancel_fence(grant)['released']
                            if released:
                                events.append({"event": "advance-released",
                                               "consumer": key,
                                               "tier_id": str(tier_id),
                                               "leg": str(entry["leg"]),
                                               "reason": "mover-terminal",
                                               "released_gib": released})
                        events.extend(self.settle_terminal_fence(ledger, tier_id=str(tier_id),
                            mover_role=str(entry["leg"]), consumer=key,
                            mover=mover))
                        continue
                    if not rowed:
                        # No queue row will ever claim this, yet the fence sits
                        # transferred under its key: leave the record
                        # ``transferring`` exactly as is.  That state is itself
                        # the recoverable one -- a later publication claims
                        # against it through the normal cover, and a terminal
                        # mover is reaped by the terminal scan above -- because
                        # the accepted state machine has no transferring-to-
                        # reserved step, and inventing one here would strand a
                        # permanently transferring record beside returned tokens.
                        continue
                    if held_grant > 0:
                        moved = self._transfer_window(str(tier_id), grant, mover)
                        if moved:
                            self.advance_window(
                                mover, str(tier_id), expect="reserved",
                                advance_to="transferring",
                                generation=bound_generation)
                            events.append({"event": "advance-handed-off",
                                           "consumer": key, "tier_id": str(tier_id),
                                           "leg": str(entry["leg"]), "mover": mover,
                                           "moved_gib": moved})
                        continue
                    # No fence under the grant: either never reserved (nothing to
                    # do) or the transfer landed without its mark (a crash between
                    # the move and the record write).  The latter is exactly
                    # recoverable: the bound names are all under the mover, so
                    # complete the marking instead of moving anything twice.
                    if (bound_names and record is not None
                            and record.get("state") == "reserved"):
                        try:
                            mover_names = ledger.token_names(mover)
                        except (OSError, PoolContractError, ValueError):
                            mover_names = set()
                        if all(name in mover_names for name in bound_names):
                            self.advance_window(
                                mover, str(tier_id), expect="reserved",
                                advance_to="transferring",
                                generation=bound_generation)
            except (OSError, PoolContractError, ValueError):
                continue
        # Terminal movers with no live protected entry: a withdrawn or failed
        # consumer's stranded fence is visited by nobody above, so its exact
        # unspent tokens would leak until a sweep that spares plan members.
        # Scan the funding records themselves; live and handled movers are
        # skipped, everything else goes through the same terminal discipline.
        handled: set[str] = set()
        for (_key, _tier), _entry in sorted(protected.items()):
            if isinstance(_entry, dict) and isinstance(_entry.get("mover"), str):
                handled.add(str(_entry["mover"]))
        try:
            funding_dir = self.queue.root / record_store.TIER_FUNDING
            funding_files = sorted(funding_dir.glob("*.funding.json"))
            funding_files += sorted(funding_dir.glob("*.output-funding.json"))
        except (OSError, PoolContractError, ValueError):
            funding_files = []
        for funding_path in funding_files:
            stem = funding_path.name
            if stem.endswith(".output-funding.json"):
                events.extend(self.reconcile_stranded_output_funding(funding_path))
                continue
            if not stem.endswith(".funding.json"):
                continue
            stem = stem[: -len(".funding.json")]
            mover_name, dot, scan_tier = stem.rpartition(".")
            if not dot or len(mover_name) != 64 or not scan_tier:
                continue
            if mover_name in handled:
                continue
            try:
                scan_ledger = self.queue.tier(scan_tier)
            except (OSError, PoolContractError, ValueError):
                continue
            scan_status, scan_record, scan_reason = self.window_evidence(
                mover_name, scan_tier)
            if scan_status == "unknown":
                # Unreadable record: nothing may settle against it.  Name it
                # and keep the scan moving; the fence and record both retain.
                events.append({"event": "advance-deferred-unknown-evidence",
                               "consumer": None, "tier_id": str(scan_tier),
                               "leg": "mover_row",
                               "error": scan_reason
                               or "funding record unreadable"})
                continue
            if (scan_status == "absent"
                    or scan_record.get("state") in ("consumed", "released")):
                continue
            scan_leg: str | None = None
            for (_key, _tier), _entry in sorted(protected.items()):
                if (str(_tier) == scan_tier and isinstance(_entry, dict)
                        and isinstance(_entry.get("leg"), str)):
                    scan_leg = str(_entry["leg"])
                    break
            events.extend(self.settle_terminal_fence(scan_ledger, tier_id=scan_tier,
                mover_role=scan_leg or "mover_row", consumer=None,
                mover=mover_name))
        return events

    def commit_stage(self, checked_instance, checked_template, *, batch_id,
                     manifest_digest, batch_ns, tier, mover, kind,
                     batch_total, batch_gib) -> CommitFunding:
        """Complete funding under the caller's unchanged output-prefix locks."""
        prepaid = None
        funding_path = (bl._funding_dir(self.queue.root, checked_instance)
                        / f"{batch_id}.funding.json")
        ledger = self.queue.tier(tier)
        try:
            _frec, _fstate = self.output_evidence(mover, tier)
        except Exception as exc:
            return CommitFunding(funding_path, prepaid, {"ok": False, "refusal": f"unknown-retain: {exc}"})
        if _fstate == "corrupt":
            return CommitFunding(funding_path, prepaid, {"ok": False,
                    "refusal": "unknown-retain: pool funding unreadable"})
        prepaid = _frec
        if prepaid is not None:
            if (str(prepaid.get("batch_id")) != batch_id
                    or str(prepaid.get("manifest_digest")) != manifest_digest
                    or str(prepaid.get("tier_id")) != tier
                    or str(prepaid.get("owner_action_key")) != str(
                        checked_instance["owner_action_key"])
                    or str(prepaid.get("template_sha256")) != oc.template_sha256(
                        checked_template)
                    or int(prepaid.get("range_start_bytes")) != 0
                    or int(prepaid.get("range_end_bytes")) != batch_total):
                return CommitFunding(funding_path, prepaid, {"ok": False, "refusal": "batch-id-in-use"})
            if str(prepaid.get("state")) == "reserved":
                # Publish-before-fund crash prefix or an unwound drive:
                # finish the transfer through the same recovery API a
                # restart would use, never a second reservation.
                driven = self.drive_output(mover, tier)
                if not driven.get("ok"):
                    return CommitFunding(funding_path, prepaid, {"ok": False,
                            "refusal": f"prepaid-drive: "
                                       f"{driven.get('refusal')}",
                            "drive": driven})
            live_funding = self.read_output(mover, tier)
            if (live_funding is None
                    or str(live_funding.get("state")) != "transferring"):
                return CommitFunding(funding_path, prepaid, {"ok": False, "refusal": "prepaid-funding-terminal",
                        "state": (str(live_funding.get("state"))
                                  if live_funding is not None
                                  else "unknown")})
            mover_now = ledger.holdings(mover).get(kind, 0)
            if mover_now < batch_gib:
                # Split tokens stay split; drive retries resume. Unknown
                # loss retains, never re-funds.
                return CommitFunding(funding_path, prepaid, {"ok": False, "refusal": "transfer-short",
                        "moved": mover_now, "expected": batch_gib})
        else:
            # Legacy in-flight recovery only: the per-batch intent names
            # tokens acquired from free under the batch namespace.
            try:
                funding = bl._read_funding(funding_path)
            except oc.ProducedOutputError as exc:
                return CommitFunding(funding_path, prepaid, {"ok": False, "refusal": f"unknown-retain: {exc}"})
            if funding is not None and (
                    funding.get("mover_key") != mover
                    or funding.get("tier") != tier
                    or int(funding.get("batch_gib", -1)) != batch_gib
                    or funding.get("manifest_digest") != manifest_digest):
                return CommitFunding(funding_path, prepaid, {"ok": False, "refusal": "batch-id-in-use"})
            mover_now = ledger.holdings(mover).get(kind, 0)
            holder_now = ledger.holdings(batch_ns).get(kind, 0)
            if funding is None and (holder_now > 0 or mover_now > 0):
                # Tokens without intent: unknown provenance (no record names
                # this funding). Never top up blindly around them.
                return CommitFunding(funding_path, prepaid, {"ok": False, "refusal": "unknown-retain: unfunded holdings"})
            if holder_now == 0 and mover_now < batch_gib:
                if funding is not None:
                    # A previous attempt moved some tokens and crashed before
                    # filing the batch; the remainder is gone to unknown hands.
                    return CommitFunding(funding_path, prepaid, {"ok": False, "refusal": "unknown-retain: funded tokens lost"})
                if not ledger.take(batch_ns, {kind: batch_gib}):
                    return CommitFunding(funding_path, prepaid, {'ok': False, 'refusal': 'tier-reservation-unavailable', 'available': ledger.free_supply()})
                # Intent names allocated tokens only: nothing is recorded
                # before the ledger holds it, so a crash before this line
                # retries clean and a crash after it resumes from the record.
                bl._write_funding(funding_path, {
                    "batch_id": batch_id, "tier": tier,
                    "mover_key": mover, "batch_gib": batch_gib,
                    "manifest_digest": manifest_digest})
                holder_now = batch_gib
            if holder_now > 0:
                self.queue.tier(tier).handover(batch_ns, mover)
                mover_now = ledger.holdings(mover).get(kind, 0)
            if mover_now < batch_gib:
                # Split tokens stay split (sum intact, nothing released);
                # retry resumes from live holdings + intent. Unknown loss
                # retains, never re-funds.
                return CommitFunding(funding_path, prepaid, {"ok": False, "refusal": "transfer-short",
                        "moved": mover_now, "expected": batch_gib})
        return CommitFunding(funding_path, prepaid)

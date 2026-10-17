"""Captured action facts and the owner of durable conclusion transitions.

Facts describe observations, never an atomic key-wide state. Existing records
are the recovery protocol. No lifecycle journal or on-disk schema is added.
"""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum
import errno
import os
from pathlib import Path
import re
import socket
import stat
import sys
import uuid

from . import claim_identity, core as pb, endings, immutable_files, local_scratch, record_store, scratch_lifetime
from .record_store import (
    Absent, AmbiguousClaimHolder, CLAIMED, DONE, FAILED, LATE_FINISH_SUFFIX,
    PoolContractError, Present, READY, TOMBSTONE_SUFFIX, Unknown, WITHDRAWN,
)


class NotCaptured(Enum):
    INPUT = "not_captured"


class ClaimRelation(Enum):
    UNOBSERVED = "unobserved"
    UNKNOWN = "unknown"
    ABSENT = "absent"
    HELD = "held"
    EXACT = "exact"
    SUCCESSOR = "successor"


class ReservationPhase(Enum):
    ACQUIRING = "acquiring"
    COMMITTED = "committed"


@dataclass(frozen=True)
class ReservationFact:
    host: str
    phase: ReservationPhase
    claim: Mapping[str, object] | None = None


@dataclass(frozen=True)
class RecordFact:
    kind: str
    record: Mapping[str, object]
    path: Path | None = None

    @property
    def generation(self) -> object:
        return self.record.get("published_unix")


@dataclass(frozen=True)
class ActionCapture:
    publication: Present | Absent | Unknown | NotCaptured = NotCaptured.INPUT
    claim: Present | Absent | Unknown | NotCaptured = NotCaptured.INPUT
    lease: Present | Absent | Unknown | NotCaptured = NotCaptured.INPUT
    reservations: tuple[ReservationFact, ...] | NotCaptured = NotCaptured.INPUT
    terminals: record_store.TerminalCapture | NotCaptured = NotCaptured.INPUT
    artifacts: tuple[RecordFact, ...] = ()
    expected_claim: Mapping[str, object] | None = None


@dataclass(frozen=True)
class ActionFacts:
    records: tuple[RecordFact, ...]
    claim: ClaimRelation
    reservations: tuple[ReservationFact, ...] | NotCaptured
    terminal: Mapping[str, object] | NotCaptured
    unknown: tuple[str, ...]
    uncaptured: tuple[str, ...]

    def for_generation(self, generation: object) -> tuple[RecordFact, ...]:
        return tuple(fact for fact in self.records if fact.generation == generation)


def classify(capture: ActionCapture) -> ActionFacts:
    """Classify supplied facts without reading or deciding a terminal twice."""
    records = list(capture.artifacts)
    unknown, uncaptured = [], []
    relation = ClaimRelation.UNOBSERVED
    for kind in ("publication", "claim", "lease"):
        observed = getattr(capture, kind)
        if isinstance(observed, NotCaptured):
            uncaptured.append(kind)
        elif isinstance(observed, Unknown):
            unknown.append(kind)
            if kind == "claim":
                relation = ClaimRelation.UNKNOWN
        elif isinstance(observed, Present):
            records.append(RecordFact(kind, observed.value))
            if kind == "claim":
                relation = (ClaimRelation.HELD if capture.expected_claim is None else
                            ClaimRelation.EXACT if claim_identity.same_claim(
                                observed.value, capture.expected_claim) else ClaimRelation.SUCCESSOR)
        elif kind == "claim":
            relation = ClaimRelation.ABSENT
    terminal = NotCaptured.INPUT
    if isinstance(capture.terminals, NotCaptured):
        uncaptured.append("terminals")
    else:
        terminal = endings.resolve_current(capture.terminals)
        for candidate in capture.terminals.candidates:
            if isinstance(candidate.observed, Present):
                records.append(RecordFact(candidate.state, candidate.observed.value))
            elif isinstance(candidate.observed, Unknown):
                unknown.append(candidate.state)
    if isinstance(capture.reservations, NotCaptured):
        uncaptured.append("reservations")
    return ActionFacts(tuple(records), relation, capture.reservations, terminal,
                       tuple(unknown), tuple(uncaptured))


class Transition(Enum):
    FINISH = "finish"
    EXPIRE = "expire"
    RELEASE_UNSTARTED = "release_unstarted"
    CANCEL = "cancel"
    LATE = "late"
    ADOPT_TERMINAL = "adopt_terminal"
    RESTORE_TOMBSTONE = "restore_tombstone"
    RETIRE_TOMBSTONE = "retire_tombstone"
    QUARANTINE = "quarantine"
    DEFER = "defer"


LEGAL_DESTINATIONS = {
    Transition.FINISH: frozenset({DONE, FAILED, READY}),
    Transition.EXPIRE: frozenset({DONE, FAILED, READY}),
    Transition.RELEASE_UNSTARTED: frozenset({READY}),
    Transition.CANCEL: frozenset({WITHDRAWN}),
    Transition.LATE: frozenset({"attempt", "evidence"}),
    Transition.ADOPT_TERMINAL: frozenset({"existing"}),
    Transition.RESTORE_TOMBSTONE: frozenset({CLAIMED}),
    Transition.RETIRE_TOMBSTONE: frozenset({"evidence"}),
    Transition.QUARANTINE: frozenset({FAILED, "existing"}),
    Transition.DEFER: frozenset({READY}),
}


@dataclass(frozen=True)
class WorkerFinish:
    action_key: str
    status: str
    detail: Mapping[str, object] | None = None
    claim_snapshot: Mapping[str, object] | None = None


@dataclass(frozen=True)
class ExpiredClaim:
    action_key: str
    record: dict[str, object]
    original: Mapping[str, object]
    holder: str | None
    lease_age: float | None
    terminal_keys: frozenset[str]


@dataclass(frozen=True)
class ClaimCommit:
    transition: Transition
    action_key: str
    record: dict[str, object]
    original: Mapping[str, object]
    holder: str | None
    destination: Path
    disposition: str
    attempt: int = 0
    adopted: Mapping[str, object] | None = None


def _legal_destination(transition: Transition, destination: str) -> None:
    if destination not in LEGAL_DESTINATIONS[transition]:
        raise PoolContractError(f"illegal {transition.value} destination: {destination}")

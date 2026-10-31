"""Per-observation wake dependencies, without changing claim authority or waits."""
from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum
import logging
import time
from types import MappingProxyType

from . import candidate_policy


class SourceKind(Enum):
    QUEUE = "queue"
    CONTROL = "control"
    HOST_LEDGER = "host_ledger"
    ACTION = "action"
    PEER = "peer"
    TIER = "tier"
    GROUP = "group"
    RESOURCE = "resource"
    CAPABILITY = "capability"
    RESIDENCY = "residency"
    RECEIPT = "receipt"
    OWNERSHIP = "ownership"
    PROGRESS = "progress"


class EvidenceState(Enum):
    PRESENT = "present"
    ABSENT = "absent"
    UNKNOWN = "unknown"
    UNREADABLE = "unreadable"


@dataclass(frozen=True)
class WakeInput:
    kind: SourceKind
    identity: str
    state: EvidenceState = EvidenceState.PRESENT


@dataclass(frozen=True)
class RetryDeadline:
    reason: str
    monotonic_at: float


@dataclass(frozen=True)
class Coverage:
    gaps: tuple[str, ...] = ()

    @property
    def complete(self) -> bool:
        return not self.gaps


@dataclass(frozen=True)
class ClaimWait:
    inputs: frozenset[WakeInput]
    deadlines: tuple[RetryDeadline, ...]
    reasons: frozenset[str]
    coverage: Coverage


@dataclass(frozen=True)
class ClaimObservation:
    value: dict[str, object] | None
    blocked_on: ClaimWait


# These are finite policy domains, not prefix matches. New reasons fail the audit.
_FAMILIES = (
    ((SourceKind.ACTION,), """
        already_claimed placement_mismatch publication_canary_authority_invalid
        malformed_demand malformed_tier_demand empty_demand admission_capacity_required
        admission_demand_required claimed_record_changed claim_rename_lost_race
    """),
    ((SourceKind.CONTROL, SourceKind.CAPABILITY), """
        operator_held resign_fenced gpu_container_runtime_unready
    """),
    ((SourceKind.PEER, SourceKind.HOST_LEDGER, SourceKind.RESOURCE), """
        deferred_for_preferred_cpu deferred_for_cross_resource_placement
        deferred_for_idle_gpu_host deferred_for_cpu_only_host deferred_for_cpu_load guest_retry_budget_exhausted
        deferred_for_local_scratch_io
    """),
    ((SourceKind.HOST_LEDGER, SourceKind.OWNERSHIP), """
        transition_busy admission_busy host_admission_unavailable committed_reservation_incomplete
        claim_persistence_failed measurement_census_unavailable
        deferred_behind_withheld_row deferred_behind_withholding deferred_for_ready_gpu
        deferred_for_ready_gpu_row deferred_for_measurement_reservation
        reservation_unavailable
    """),
    ((SourceKind.RESOURCE, SourceKind.HOST_LEDGER), """
        never_fits_capacity adaptive_cpu_refused adaptive_gpu_refused adaptive_refused
        unified_gpu_memory_unknown unified_gpu_memory_budget
    """),
    ((SourceKind.CAPABILITY,), """
        interpreter_requirement_missing interpreter_not_present local_dependencies_unavailable
        local_dependency_not_present dependency_requirement_missing dependency_not_present
        dependency_unreadable dependency_digest_mismatch container_image_requirement_missing
        container_image_presence_unknown container_image_absent container_class_unavailable
        malformed_interpreter malformed_container_images malformed_requires_files
        container_class_inventory_unknown container_class_class_requirements_missing
        container_class_image_store_mismatch container_class_required_image_missing
        container_class_image_content_mismatch local_scratch_io_intent_invalid
        local_scratch_io_profile_invalid local_scratch_io_generation_changed
        local_scratch_io_profile_expired
    """),
    ((SourceKind.GROUP, SourceKind.ACTION, SourceKind.HOST_LEDGER), """
        gang_group_incomplete gang_contract_invalid gang_torn_down gang_waiting_for_backfill_release
        deferred_for_gang_reservation deferred_for_gang_prelaunch gang_member_elected_elsewhere
        gang_waiting_for_peers
    """),
    ((SourceKind.RESIDENCY, SourceKind.TIER, SourceKind.OWNERSHIP, SourceKind.RECEIPT), """
        residency_lead_record_unreadable residency_lead_terminal deferred_for_resident_copy
        origin_consumer_release_unreadable dead_input_dependency_unreadable
        dead_input_funding_unreadable
    """),
    ((SourceKind.TIER, SourceKind.RECEIPT, SourceKind.PROGRESS, SourceKind.OWNERSHIP), """
        deferred_for_declared_wait deferred_for_pool_readers tier_unknown tier_census_unreadable
        never_fits_tier_capacity tier_reservation_unavailable output_funding_unknown
        output_funding_pending output_funding_required_absent window_funding_pending
        window_funding_unknown funding_not_durable
    """),
)
_refusals = {reason: kinds for kinds, names in _FAMILIES for reason in names.split()}
for _state in candidate_policy.RESIDENCY_REFUSAL_STATES:
    _refusals[f"residency_{_state}"] = (
        SourceKind.RESIDENCY, SourceKind.TIER, SourceKind.OWNERSHIP, SourceKind.RECEIPT)
for _base in ("adaptive_cpu_refused", "adaptive_gpu_refused", "adaptive_refused", "reservation_unavailable"):
    for _suffix in ("_withholding", "_past_ceiling", "_starved"):
        _refusals[_base + _suffix] = _refusals[_base]
REFUSAL_SOURCES = MappingProxyType(_refusals)

DECISION_REASONS = frozenset("""
    host_pressure host_pressure_unproven measurement_sampler_unknown measurement_foreign_ambient
    measurement_host_not_idle max_actions unbounded_cpu_not_exclusive projected_cpu_cost
    measurement_holder holder_reservation_unknown pressure_override_no_borrow borrow_evidence_unavailable
    exclusive_holder sample_invalid_or_stale measurement_device_not_idle host_or_device_congested
    sharing_probe_not_authorized gpu_memory_sample_invalid gpu_memory_budget holder_telemetry_unavailable
    unified_gpu_memory_unknown unified_gpu_memory_budget
""".split())
_RECOVERY_REASONS = frozenset("""
    host_admission_unavailable committed_reservation_incomplete claim_persistence_failed
    measurement_census_unavailable unified_gpu_memory_unknown local_dependencies_unavailable
    dependency_unreadable container_image_presence_unknown container_class_unavailable
    container_class_inventory_unknown local_scratch_io_profile_invalid
    residency_lead_record_unreadable residency_map_unreadable residency_plan_unreadable
    origin_consumer_release_unreadable dead_input_dependency_unreadable dead_input_funding_unreadable
    tier_unknown tier_census_unreadable output_funding_unknown window_funding_unknown funding_not_durable
    measurement_sampler_unknown host_pressure_unproven holder_reservation_unknown
    borrow_evidence_unavailable sample_invalid_or_stale gpu_memory_sample_invalid
    holder_telemetry_unavailable
""".split())
_ABSENT_REASONS = frozenset({"interpreter_not_present", "local_dependency_not_present",
                             "dependency_not_present", "container_image_absent",
                             "gang_group_incomplete", "output_funding_required_absent"})


def known_refusal(reason: str) -> bool:
    return reason in REFUSAL_SOURCES


class Collector:
    def __init__(self, queue: object, *, wall_clock: Callable[[], float],
                 monotonic_clock: Callable[[], float]) -> None:
        self.wall_clock = wall_clock
        self.monotonic_clock = monotonic_clock
        self.wall_at_capture = wall_clock()
        self.monotonic_at_capture = monotonic_clock()
        self.inputs: set[WakeInput] = set()
        self.deadlines: set[RetryDeadline] = set()
        self.reasons: set[str] = set()
        self.gaps: set[str] = set()
        identity = str(getattr(queue, "root", "queue"))
        for kind in (SourceKind.QUEUE, SourceKind.CONTROL, SourceKind.HOST_LEDGER):
            self.observe(kind, identity)

    def observe(self, kind: SourceKind, identity: str,
                state: EvidenceState = EvidenceState.PRESENT) -> None:
        self.inputs.add(WakeInput(kind, identity, state))
        if state in (EvidenceState.UNKNOWN, EvidenceState.UNREADABLE):
            self.recovery(f"{kind.value}:{identity}:{state.value}")

    def deadline(self, reason: str, monotonic_at: float) -> None:
        self.deadlines.add(RetryDeadline(reason, monotonic_at))

    def unix_deadline(self, reason: str, unix_at: float) -> None:
        self.deadline(reason, self.monotonic_at_capture + unix_at - self.wall_at_capture)

    def recovery(self, reason: str, delay_s: float = 5.0) -> None:
        self.gaps.add(reason)
        self.deadline("source_recovery", self.monotonic_clock() + min(5.0, max(0.0, delay_s)))

    def observation(self, value: dict[str, object] | None = None) -> ClaimObservation:
        return ClaimObservation(value, ClaimWait(
            frozenset(self.inputs),
            tuple(sorted(self.deadlines, key=lambda due: (due.monotonic_at, due.reason))),
            frozenset(self.reasons), Coverage(tuple(sorted(self.gaps)))))

    finish = observation


_CURRENT: ContextVar[tuple[object, Collector] | None] = ContextVar("claim_wait", default=None)


@contextmanager
def capture(queue: object, *, wall_clock: Callable[[], float] = time.time,
            monotonic_clock: Callable[[], float] = time.monotonic) -> Iterator[Collector]:
    collector = Collector(queue, wall_clock=wall_clock, monotonic_clock=monotonic_clock)
    token = _CURRENT.set((queue, collector))
    try:
        yield collector
    finally:
        _CURRENT.reset(token)


def _collector(queue: object) -> Collector | None:
    current = _CURRENT.get()
    return current[1] if current is not None and current[0] is queue else None


def is_observing(queue: object) -> bool:
    return _collector(queue) is not None


def on_unix_deadline(queue: object) -> Callable[[str, float], None] | None:
    collector = _collector(queue)
    return collector.unix_deadline if collector is not None else None


@contextmanager
def reading(queue: object, kind: SourceKind, identity: str) -> Iterator[None]:
    observe(queue, kind, identity)
    try:
        yield
    except (OSError, ValueError, TypeError, KeyError) as exc:
        observe(queue, kind, identity,
                EvidenceState.UNREADABLE if isinstance(exc, OSError) else EvidenceState.UNKNOWN)
        raise


def action_identity(item: Mapping[str, object]) -> str:
    return f"{item.get('action_key', '')}:{item.get('published_unix')!r}:{item.get('attempts', 0)!r}"


def observe(queue: object, kind: SourceKind, identity: str,
            state: EvidenceState = EvidenceState.PRESENT) -> None:
    if (collector := _collector(queue)) is not None:
        collector.observe(kind, identity, state)


def deadline(queue: object, reason: str, monotonic_at: float) -> None:
    if (collector := _collector(queue)) is not None:
        collector.deadline(reason, monotonic_at)


def unix_deadline(queue: object, reason: str, unix_at: float) -> None:
    if (collector := _collector(queue)) is not None:
        collector.unix_deadline(reason, unix_at)


def recovery(queue: object, reason: str, delay_s: float = 5.0) -> None:
    if (collector := _collector(queue)) is not None:
        collector.recovery(reason, delay_s)


def refusal(queue: object, item: Mapping[str, object], reason: str,
            evidence: Mapping[str, object] | None = None) -> None:
    collector = _collector(queue)
    if collector is None:
        return
    collector.reasons.add(reason)
    identity = action_identity(item)
    collector.observe(SourceKind.ACTION, identity)
    kinds = REFUSAL_SOURCES.get(reason)
    if kinds is None:
        if reason not in collector.gaps:
            logging.getLogger(__name__).warning("Unmapped claim refusal: %s", reason)
        collector.recovery(reason)
        return
    state = (EvidenceState.ABSENT if reason in _ABSENT_REASONS else
             EvidenceState.UNREADABLE if reason in _RECOVERY_REASONS and "unreadable" in reason else
             EvidenceState.UNKNOWN if reason in _RECOVERY_REASONS else EvidenceState.PRESENT)
    for kind in kinds:
        collector.observe(kind, identity if kind is SourceKind.ACTION else "*", state)
    if reason in ("transition_busy", "admission_busy"):
        collector.deadline("lock_retry", collector.monotonic_clock() + 1.0)
    if reason in _RECOVERY_REASONS:
        collector.recovery(reason)
    for field in ("decision", "cpu_decision", "gpu_decision"):
        decision = evidence.get(field) if isinstance(evidence, Mapping) else None
        nested = decision.get("reason") if isinstance(decision, Mapping) else None
        if not isinstance(nested, str):
            continue
        collector.reasons.add(nested)
        if nested not in DECISION_REASONS and nested not in REFUSAL_SOURCES:
            if nested not in collector.gaps:
                logging.getLogger(__name__).warning("Unmapped claim decision: %s", nested)
            collector.recovery(nested)
        elif nested in _RECOVERY_REASONS:
            collector.recovery(nested)

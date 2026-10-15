"""Staged fairness policy over observations; no capacity or election authority."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
import math
from types import MappingProxyType

from . import adaptive_cpu as cpu_admission
from . import adaptive_gpu as gpu_admission
from .digest_primitives import PrismaBuildError
from .gpu_sample_facts import ForeignProcesses, GpuSampleFacts

STARVATION_FLOOR = 3
WITHHOLD_CEILING_S = 900.0
MEASUREMENT_UNMEASURED_CEILING_S = 180.0
DRAIN_HELD_CPUS = "held_cpus"
DRAIN_NOTE_FIELDS = ("blocked_unix", "foreign_unix", "epoch_unix", "expired_unix")
MEASUREMENT_DRAIN_NOTE_FIELDS = (*DRAIN_NOTE_FIELDS, "measurement_until_unix",
                               "unmeasured_until_unix")


@dataclass(frozen=True)
class DrainQuestion:
    key: str
    need: Mapping[str, int]
    mode: str
    adaptive: bool = False
    foreign: bool = False
    held_cpus: Sequence[int] | None = None
    cpu_tiers_supplied: bool = False
    measurement: bool = False
    measurement_generation: str | None = None
    measurement_foreign_clear: bool = False
    measurement_unmeasured: bool = False


@dataclass(frozen=True)
class OrdinaryNotes:
    changes: Mapping[str, float | None]

    def __post_init__(self):
        object.__setattr__(self, "changes", MappingProxyType(dict(self.changes)))


@dataclass(frozen=True)
class MeasurementNotes:
    host: str
    generation: str | None
    changes: Mapping[str, float | None]

    def __post_init__(self):
        object.__setattr__(self, "changes", MappingProxyType(dict(self.changes)))


DrainNotes = OrdinaryNotes | MeasurementNotes


@dataclass(frozen=True)
class DrainAssessment:
    verdict: dict[str, object]
    notes: DrainNotes | None


@dataclass(frozen=True)
class DrainFailure:
    error: BaseException
    notes: DrainNotes | None


@dataclass(frozen=True)
class PreparedDrain:
    question: DrainQuestion
    now: float
    host: str
    measurement_wait: bool
    clocks: Mapping[str, object]
    passes: int
    age: float

    def stamp(self, name: str) -> float | None:
        value = self.clocks.get(name)
        return float(value) if type(value) in (int, float) and math.isfinite(value) else None


@dataclass
class PendingDrain:
    prepared: PreparedDrain
    verdict: dict[str, object]
    notes: dict[str, float | None] = field(default_factory=dict)
    unmeasured_until: float | None = None
    until: float | None = None
    epoch: float | None = None
    short: dict[str, int] = field(default_factory=dict)
    in_way: list[dict[str, object]] = field(default_factory=list)
    bounds: list[Mapping[str, object]] = field(default_factory=list)
    draining: list[Mapping[str, object]] = field(default_factory=list)
    covered: dict[str, int] = field(default_factory=dict)
    wanted_cpus: set[int] = field(default_factory=set)

    def copy(self) -> PendingDrain:
        return replace(self, verdict=dict(self.verdict), notes=dict(self.notes),
                       short=dict(self.short), in_way=list(self.in_way), bounds=list(self.bounds),
                       draining=list(self.draining), covered=dict(self.covered),
                       wanted_cpus=set(self.wanted_cpus))


def prepare_drain(question: DrainQuestion, prior: Mapping[str, object], *,
                  now: float, host: str) -> PreparedDrain:
    measurement_wait = question.measurement and question.mode in ("exclusive", "tokens")
    clocks = prior
    if measurement_wait:
        episodes = prior.get("measurement_drains")
        episode = episodes.get(host) if isinstance(episodes, Mapping) else None
        clocks = (episode if isinstance(episode, Mapping)
                  and episode.get("generation") == question.measurement_generation else {})
    passes = prior.get("passes", 0)
    passes = int(passes) if type(passes) in (int, float) else 0
    first = prior.get("first_unix")
    age = max(0.0, now - float(first)) if type(first) in (int, float) else 0.0
    return PreparedDrain(question, now, host, measurement_wait, clocks, passes, age)


def _initial(prepared: PreparedDrain) -> PendingDrain:
    return PendingDrain(prepared, {"eligible": False, "withhold": False, "why": None,
                                   "mode": prepared.question.mode,
                                   "withhold_age_s": prepared.age})


def _complete(p: PendingDrain, error: BaseException | None = None) -> DrainAssessment | DrainFailure:
    prepared, verdict = p.prepared, p.verdict
    try:
        if prepared.measurement_wait and prepared.question.measurement_unmeasured:
            verdict["measurement_unmeasured"] = True
            if p.unmeasured_until is not None:
                verdict["unmeasured_until_unix"] = p.unmeasured_until
                original = verdict.get("drain_until_unix")
                if type(original) in (int, float):
                    verdict["measurement_drain_until_unix"] = original
                    verdict["drain_until_unix"] = min(float(original), p.unmeasured_until)
            if p.unmeasured_until is None or prepared.now >= p.unmeasured_until:
                verdict.update(withhold=False, drain_resolves=False)
                if verdict.get("why") not in ("foreign_load", "verdict_unreadable"):
                    verdict["why"] = "measurement_baseline_unknown"
    except BaseException as exc:
        return DrainFailure(exc, None)
    notes = None
    if p.notes:
        notes = (MeasurementNotes(prepared.host, prepared.question.measurement_generation, p.notes)
                 if prepared.measurement_wait else OrdinaryNotes(p.notes))
    return DrainFailure(error, notes) if error is not None else DrainAssessment(dict(verdict), notes)


def failed_drain(progress: PreparedDrain | PendingDrain,
                 error: BaseException) -> DrainAssessment | DrainFailure:
    p = _initial(progress) if isinstance(progress, PreparedDrain) else progress.copy()
    if isinstance(error, (OSError, PrismaBuildError, ValueError, TypeError)):
        try:
            p.verdict.update(withhold=False, why="verdict_unreadable", error=str(error))
        except BaseException as exc:
            return _complete(p, exc)
        return _complete(p)
    return _complete(p, error)


def begin_drain(prepared: PreparedDrain, available: Mapping[str, int],
                gpu_sample: Mapping[str, object] | None) -> PendingDrain | DrainAssessment | DrainFailure:
    p = _initial(prepared)
    q = p.prepared.question
    now, host = p.prepared.now, p.prepared.host
    measurement_wait, passes, age = p.prepared.measurement_wait, p.prepared.passes, p.prepared.age
    key, need, mode, adaptive, foreign = q.key, q.need, q.mode, q.adaptive, q.foreign
    measurement_generation = q.measurement_generation
    measurement_foreign_clear, measurement_unmeasured = q.measurement_foreign_clear, q.measurement_unmeasured
    held_cpus, cpu_tiers = q.held_cpus, q.cpu_tiers_supplied
    clocks, stamp, verdict, notes = p.prepared.clocks, p.prepared.stamp, p.verdict, p.notes
    try:
        gpu_need = int(need.get("gpu", 0) or 0)
        gpu_clear = None
        gpu_facts = None
        if gpu_need and gpu_sample is not None:
            gpu_facts = GpuSampleFacts(gpu_sample)
            foreign_state = gpu_facts.foreign
            sample_timestamp = gpu_facts.timestamp
            gpu_clear = (foreign_state is ForeignProcesses.CLEAR and sample_timestamp is not None
                         and sample_timestamp.exact_type
                         and sample_timestamp.within(now, gpu_admission.MAX_SAMPLE_AGE_S))
            if foreign_state is ForeignProcesses.PRESENT:
                foreign = True
        gpu_first = (mode != "gpu" and bool(gpu_need)
                     and available.get("gpu", 0) >= gpu_need
                     and gpu_clear is not False)
        p.short = {}
        if mode == "tokens":
            p.short = {kind: int(amount) - available.get(kind, 0)
                     for kind, amount in need.items()
                     if int(amount) > available.get(kind, 0)}
            verdict["short"] = dict(p.short)
            if adaptive and not p.short:
                verdict["why"] = "not_a_token_shortage"
                return _complete(p)
        eligible = measurement_wait or passes + 1 >= STARVATION_FLOOR or gpu_first
        verdict.update(eligible=eligible, gpu_first=gpu_first)
        if measurement_wait:
            if (not isinstance(measurement_generation, str)
                    or len(measurement_generation) != 64
                    or any(c not in "0123456789abcdef" for c in measurement_generation)):
                raise ValueError("measurement drain requires a submission generation")
            verdict.update(drain_host=host, drain_generation=measurement_generation)
            if measurement_unmeasured:
                if "unmeasured_until_unix" not in clocks:
                    p.unmeasured_until = now + MEASUREMENT_UNMEASURED_CEILING_S
                    notes["unmeasured_until_unix"] = p.unmeasured_until
                else:
                    try:
                        p.unmeasured_until = stamp("unmeasured_until_unix")
                    except OverflowError:
                        p.unmeasured_until = None
                    if p.unmeasured_until is None:
                        notes["unmeasured_until_unix"] = 0.0
                        raise ValueError("unreadable unmeasured measurement deadline")
        if foreign:
            notes["foreign_unix"] = now
        if not eligible:
            return _complete(p)
        gpu_foreign_clear = (not gpu_need or (gpu_clear is True
            and gpu_facts.schema_attribution_declared))
        clear_foreign = (measurement_wait and measurement_foreign_clear
                         and gpu_foreign_clear and not foreign)
        if clear_foreign:
            notes["foreign_unix"] = None
        foreign_unix = (now if foreign else None if clear_foreign
                        else stamp("foreign_unix"))
        if foreign_unix is not None and now - foreign_unix <= WITHHOLD_CEILING_S:
            if not measurement_wait:
                notes.update(epoch_unix=None, expired_unix=None)
            verdict.update(why="foreign_load", foreign_age_s=now - foreign_unix)
            return _complete(p)
        p.until, p.epoch = stamp("measurement_until_unix"), stamp("epoch_unix")
        if measurement_wait and p.until is not None:
            verdict.update(measurement_key=key, drain_until_unix=p.until,
                           episode_age_s=now - (p.epoch if p.epoch is not None else now))
            if now >= p.until:
                verdict["why"] = "measurement_drain_expired"
                return _complete(p)

        p.in_way = []
        p.bounds = []
        p.draining = []
        p.covered = {kind: 0 for kind in p.short}
        if mode == DRAIN_HELD_CPUS:
            if not held_cpus or not cpu_tiers:
                raise ValueError("a held_cpus drain names no CPUs or no CPU tiers")
            p.wanted_cpus = {int(cpu) for cpu in held_cpus}
            verdict["held_cpus"] = sorted(p.wanted_cpus)
        return p
    except BaseException as exc:
        return failed_drain(p, exc)


def add_holder(pending: PendingDrain, *, holder: str, bound: Mapping[str, object],
               counts: Mapping[str, int]) -> PendingDrain | DrainAssessment | DrainFailure:
    p = pending.copy()
    age = p.prepared.age
    try:
        p.bounds.append(bound)
        if (bound["bound"] == "transient"
                or (bound["bound"] == "unknown" and age <= WITHHOLD_CEILING_S)):
            p.draining.append(bound)
            for kind, count in counts.items():
                p.covered[kind] += count
        else:
            p.in_way.append({"action_key": holder[:12], **{
                field: bound.get(field) for field in (
                    "bound", "age_s", "governed_by", "requested_timeout_s")}})
        return p
    except BaseException as exc:
        return failed_drain(p, exc)


def finish_drain(pending: PendingDrain) -> DrainAssessment | DrainFailure:
    p = pending.copy()
    now, age = p.prepared.now, p.prepared.age
    key, mode = p.prepared.question.key, p.prepared.question.mode
    measurement_wait, stamp = p.prepared.measurement_wait, p.prepared.stamp
    verdict, notes = p.verdict, p.notes
    try:
        unknown_drains = age <= WITHHOLD_CEILING_S
        verdict["holders"] = p.in_way
        if p.draining or p.in_way:
            notes["blocked_unix"] = now
        if measurement_wait and (p.bounds or mode == "tokens"):
            if p.until is None:
                p.epoch = now
                ends = [float(bound["claimed_unix"]) + float(bound["requested_timeout_s"])
                        for bound in p.bounds
                        if type(bound.get("claimed_unix")) in (int, float)
                        and type(bound.get("requested_timeout_s")) in (int, float)
                        and math.isfinite(float(bound["claimed_unix"]))
                        and math.isfinite(float(bound["requested_timeout_s"]))]
                p.until = max([now + WITHHOLD_CEILING_S,
                             *(end for end in ends if math.isfinite(end))])
            notes.update(epoch_unix=p.epoch, measurement_until_unix=p.until)
            verdict.update(withhold=now < p.until,
                           why=("draining_for_measurement" if now < p.until
                                else "measurement_drain_expired"),
                           measurement_key=key, drain_until_unix=p.until,
                           episode_age_s=now - (p.epoch if p.epoch is not None else now),
                           holders=[dict(bound) for bound in p.bounds])
            return _complete(p)
        if not p.draining and not p.in_way:
            if mode == "exclusive":
                blocked = stamp("blocked_unix")
                window = cpu_admission.MAX_INTERVAL_S + cpu_admission.MAX_SAMPLE_AGE_S
                if blocked is not None and now - blocked <= window:
                    verdict.update(withhold=True, why="holder_tail")
                    return _complete(p)
                notes["foreign_unix"] = now
                if not measurement_wait:
                    notes.update(epoch_unix=None, expired_unix=None)
                verdict["why"] = "foreign_load"
                return _complete(p)
            verdict.update(withhold=unknown_drains,
                           why="in_flight" if unknown_drains else "unknown_past_ceiling")
            return _complete(p)
        fits = (not p.in_way if mode in ("exclusive", "gpu", DRAIN_HELD_CPUS) else
                all(p.covered[kind] >= amount for kind, amount in p.short.items()))
        if not fits:
            notes.update(epoch_unix=None, expired_unix=None)
            verdict["why"] = (
                "holders_cannot_cover" if not p.in_way else
                "unknown_past_ceiling"
                if all(entry["bound"] == "unknown" for entry in p.in_way)
                else "holder_does_not_drain_soon")
            return _complete(p)
        p.epoch, expired = stamp("epoch_unix"), stamp("expired_unix")
        if p.epoch is None:
            p.epoch = now
        refills = [bound for bound in p.draining
                   if type(bound.get("claimed_unix")) in (int, float)
                   and p.epoch < float(bound["claimed_unix"])
                   and (expired is None or float(bound["claimed_unix"]) < expired)]
        if expired is not None and not refills:
            p.epoch, expired = now, None
        elif expired is None and refills and now - p.epoch > WITHHOLD_CEILING_S:
            expired = now
        notes.update(epoch_unix=p.epoch, expired_unix=expired)
        verdict.update(episode_age_s=now - p.epoch,
                       refills=[str(bound["action_key"])[:12] for bound in refills])
        if expired is not None:
            verdict["why"] = "refilled_past_ceiling"
            return _complete(p)
        verdict.update(withhold=True, why="drains_soon")
        return _complete(p)
    except BaseException as exc:
        return failed_drain(p, exc)


def ordinary_note_fields(prior: Mapping[str, object], notes: OrdinaryNotes | None) -> dict[str, object]:
    record: dict[str, object] = {}
    notes = notes.changes if notes is not None else {}
    for field in DRAIN_NOTE_FIELDS:
        value = notes[field] if field in notes else prior.get(field)
        if type(value) in (int, float) and math.isfinite(value):
            record[field] = float(value)
    return record


def measurement_episodes(prior: Mapping[str, object]) -> dict[str, object]:
    episodes = prior.get("measurement_drains")
    return dict(episodes) if isinstance(episodes, Mapping) else {}


def measurement_note_fields(episodes: Mapping[str, object],
                            notes: MeasurementNotes | None) -> dict[str, object]:
    record: dict[str, object] = {}
    episodes = dict(episodes)
    if notes is not None:
        host, generation, updates = notes.host, notes.generation, notes.changes
        previous = episodes.get(host)
        if not isinstance(previous, Mapping) or previous.get("generation") != generation:
            previous = {}
        episode: dict[str, object] = {"generation": generation}
        for field in MEASUREMENT_DRAIN_NOTE_FIELDS:
            value = updates[field] if field in updates else previous.get(field)
            if field == "unmeasured_until_unix" and (field in updates or field in previous):
                try:
                    valid = type(value) in (int, float) and math.isfinite(value)
                except OverflowError:
                    valid = False
                episode[field] = float(value) if valid else 0.0
            elif type(value) in (int, float) and math.isfinite(value):
                episode[field] = float(value)
        episodes[host] = episode
    if episodes:
        record["measurement_drains"] = episodes
    return record

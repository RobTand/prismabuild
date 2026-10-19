"""Pure observations from the public GPU sample, without admission policy."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum, auto
import math

from .gpu_capacity import SCHEMA

GPU_MEMORY_DOMAINS = frozenset(('shared_system', 'discrete'))


def is_nonnegative_number(value: object) -> bool:
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


class ForeignProcesses(Enum):
    UNKNOWN = auto()
    CLEAR = auto()
    PRESENT = auto()


@dataclass(frozen=True)
class SampleTimestamp:
    value: int | float

    @classmethod
    def from_value(cls, value: object) -> SampleTimestamp | None:
        return (cls(value)
                if isinstance(value, (int, float)) and not isinstance(value, bool) else None)

    @property
    def exact_type(self) -> bool:
        return type(self.value) in (int, float)

    @property
    def finite_nonnegative(self) -> bool:
        return is_nonnegative_number(self.value)

    @property
    def finite(self) -> bool:
        return math.isfinite(self.value)

    def within(self, now: float, max_age_s: float) -> bool:
        return 0 <= now - self.value <= max_age_s


@dataclass(frozen=True)
class SingleDeviceInventory:
    device: dict[str, object]
    sample_id: str
    jobs: list[object]
    foreign_processes: list[object]


@dataclass(frozen=True)
class ControllerHostFacts:
    total_bytes: int | float
    available_bytes: int | float
    memory_some: int | float
    memory_full: int | float
    cpu_some: int | float


@dataclass(frozen=True)
class MemoryCounters:
    total: int | float
    free: int | float
    used: int | float


class GpuSampleFacts:
    def __init__(self, sample: Mapping[str, object]):
        self._sample = sample

    @property
    def schema_attribution_declared(self) -> bool:
        sample = self._sample
        return (sample.get('schema') == SCHEMA
                and sample.get('complete') is True and sample.get('attributed') is True)

    @property
    def timestamp(self) -> SampleTimestamp | None:
        return SampleTimestamp.from_value(self._sample.get('sampled_unix'))

    @property
    def foreign(self) -> ForeignProcesses:
        processes = self._sample.get('foreign_processes')
        if not isinstance(processes, list):
            return ForeignProcesses.UNKNOWN
        return ForeignProcesses.PRESENT if processes else ForeignProcesses.CLEAR

    def single_device_inventory(self, *, fresh: bool) -> SingleDeviceInventory | None:
        sample = self._sample
        devices = sample.get('devices', [])
        if not (fresh and self.schema_attribution_declared
                and isinstance(sample.get('sample_id'), str) and bool(sample['sample_id'])
                and isinstance(devices, list) and len(devices) == 1
                and isinstance(devices[0], dict)
                and isinstance(sample.get('foreign_processes'), list)
                and isinstance(sample.get('jobs'), list)):
            return None
        return SingleDeviceInventory(devices[0], sample['sample_id'], sample['jobs'],
                                     sample['foreign_processes'])

    def controller_host(self) -> ControllerHostFacts | None:
        sample = self._sample
        fields = ('host_total_bytes', 'host_available_bytes', 'memory_pressure_some',
                  'memory_pressure_full', 'cpu_pressure_some')
        if not (all(is_nonnegative_number(sample.get(k)) for k in fields)
                and 0 < sample['host_total_bytes'] >= sample['host_available_bytes']
                and all(sample[k] <= 100 for k in fields if 'pressure' in k)):
            return None
        return ControllerHostFacts(*(sample[k] for k in fields))


def device_identity(device: Mapping[str, object]) -> tuple[str, str] | None:
    if (not isinstance(device.get('uuid'), str) or not device['uuid']
            or device.get('memory_domain') not in ('shared_system', 'discrete')):
        return None
    return device['uuid'], device['memory_domain']


def memory_counters(device: Mapping[str, object]) -> MemoryCounters | None:
    fields = ('memory_total_bytes', 'memory_free_bytes', 'memory_used_bytes')
    if not all(is_nonnegative_number(device.get(k)) for k in fields):
        return None
    return MemoryCounters(*(device[k] for k in fields))


def nonnegative_float(value: object, *, positive: bool = False) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or number < 0.0 or positive and number <= 0.0:
        return None
    return number


def nonnegative_int(value: object) -> int | None:
    if type(value) is not int or value < 0:
        return None
    return value


@dataclass(frozen=True)
class ObservationFailure:
    reason: str


@dataclass(frozen=True)
class OfferHeader:
    sample: Mapping[str, object]
    sampled_unix: float | None


@dataclass(frozen=True)
class OfferInventory:
    devices: list[Mapping[str, object]]
    foreign_processes: list[object]
    jobs: list[object]


def offer_header(sample: object) -> OfferHeader | ObservationFailure:
    if not isinstance(sample, Mapping):
        return ObservationFailure('missing trusted GPU capacity snapshot')
    if not GpuSampleFacts(sample).schema_attribution_declared:
        return ObservationFailure('incomplete or unattributed GPU capacity snapshot')
    return OfferHeader(sample, nonnegative_float(sample.get('sampled_unix'), positive=True))


def offer_inventory(header: OfferHeader, *, now: float,
                    max_age_s: float) -> OfferInventory | ObservationFailure:
    sampled = header.sampled_unix
    if sampled is None or not 0.0 <= now - sampled <= max_age_s:
        return ObservationFailure('stale or future GPU capacity snapshot')
    sample = header.sample
    devices = sample.get('devices')
    foreign = sample.get('foreign_processes')
    jobs = sample.get('jobs')
    if (not isinstance(devices, list) or not devices
            or not isinstance(foreign, list) or not isinstance(jobs, list)):
        return ObservationFailure('malformed GPU device or attribution inventory')
    host_total = nonnegative_float(sample.get('host_total_bytes'), positive=True)
    host_available = nonnegative_float(sample.get('host_available_bytes'))
    if host_total is None or host_available is None or host_available > host_total:
        return ObservationFailure('invalid host memory bounds in GPU snapshot')
    for key in ('memory_pressure_some', 'memory_pressure_full',
                'cpu_pressure_some', 'cpu_pressure_full'):
        pressure = nonnegative_float(sample.get(key))
        if pressure is None or pressure > 100.0:
            return ObservationFailure(f'invalid {key} in GPU snapshot')
    identities: set[str] = set()
    typed_devices: list[Mapping[str, object]] = []
    for device in devices:
        if not isinstance(device, Mapping):
            return ObservationFailure('malformed GPU device record')
        identity = device.get('uuid')
        domain = device.get('memory_domain')
        total = nonnegative_float(device.get('memory_total_bytes'), positive=True)
        free = nonnegative_float(device.get('memory_free_bytes'))
        used = nonnegative_float(device.get('memory_used_bytes'))
        if (not isinstance(identity, str) or not identity.startswith('GPU-')
                or identity in identities or domain not in GPU_MEMORY_DOMAINS):
            return ObservationFailure('unknown GPU identity, memory domain or bounds')
        raw_memory = tuple(device.get(f'memory_{kind}_bytes')
                           for kind in ('total', 'free', 'used'))
        if domain == 'discrete':
            if (total is None or free is None or used is None
                    or free > total or used > total or free + used > total):
                return ObservationFailure('unknown GPU identity, memory domain or bounds')
        elif any(value is not None for value in raw_memory):
            if (total is None or free is None or used is None
                    or free > total or used > total or free + used > total):
                return ObservationFailure('unknown GPU identity, memory domain or bounds')
        identities.add(identity)
        typed_devices.append(device)
    return OfferInventory(typed_devices, foreign, jobs)


@dataclass(frozen=True)
class FramebufferHeader:
    sample: dict[str, object]
    sample_id: str
    sampled_unix: int | float


@dataclass(frozen=True)
class FramebufferInventory:
    device: dict[str, object]
    jobs: list[object]


@dataclass(frozen=True)
class FramebufferDevice:
    identity: str
    name: str
    total: int
    free: int
    used: int


def framebuffer_header(sample: object, *, start_unix: float, now: float,
                       max_age_s: float) -> FramebufferHeader | None:
    if not isinstance(sample, dict) or sample.get('schema') != SCHEMA:
        return None
    sample_id = sample.get('sample_id')
    stamp = GpuSampleFacts(sample).timestamp
    if (not isinstance(sample_id, str) or not sample_id
            or stamp is None or not stamp.exact_type or not stamp.finite
            or stamp.value < start_unix or not stamp.within(now, max_age_s)):
        return None
    return FramebufferHeader(sample, sample_id, stamp.value)


def framebuffer_inventory(header: FramebufferHeader) -> FramebufferInventory | None:
    devices = header.sample.get('devices')
    jobs = header.sample.get('jobs')
    if not isinstance(devices, list) or len(devices) != 1 or not isinstance(devices[0], dict):
        return None
    if not isinstance(jobs, list):
        return None
    return FramebufferInventory(devices[0], jobs)


def framebuffer_device(device: Mapping[str, object], *, start_unix: float,
                       now: float, max_age_s: float) -> FramebufferDevice | None:
    identity = device.get('uuid')
    name = device.get('name')
    stamp = SampleTimestamp.from_value(device.get('sampled_unix'))
    total = nonnegative_int(device.get('memory_total_bytes'))
    free = nonnegative_int(device.get('memory_free_bytes'))
    used = nonnegative_int(device.get('memory_used_bytes'))
    if (not isinstance(identity, str) or not identity or not isinstance(name, str) or not name
            or stamp is None or not stamp.exact_type or not stamp.finite
            or stamp.value < start_unix or not stamp.within(now, max_age_s)
            or total is None or total <= 0 or free is None or used is None
            or free > total or used > total or free + used != total):
        return None
    return FramebufferDevice(identity, name, total, free, used)

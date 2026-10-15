"""Pure observations from the public GPU sample, without admission policy."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum, auto
import math

from .gpu_capacity import SCHEMA


def is_nonnegative_number(value: object) -> bool:
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


class ForeignProcesses(Enum):
    UNKNOWN = auto()
    CLEAR = auto()
    PRESENT = auto()


@dataclass(frozen=True)
class SampleTimestamp:
    value: int | float

    @property
    def exact_type(self) -> bool:
        return type(self.value) in (int, float)

    @property
    def finite_nonnegative(self) -> bool:
        return is_nonnegative_number(self.value)

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
        value = self._sample.get('sampled_unix')
        return (SampleTimestamp(value)
                if isinstance(value, (int, float)) and not isinstance(value, bool) else None)

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

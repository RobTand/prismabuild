"""Holder evidence without assuming a ledger name proves execution."""
from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from types import MappingProxyType

from .storage_tiers import RAM_HOST_MEMORY_PREFIX

CPU_METADATA = '.adaptive.json'
GPU_METADATA = '.gpu.json'


class HolderKind(Enum):
    OTHER_HOLDER = 'other_holder'
    HOST_MEMORY_MIRROR = 'host_memory_mirror'


@dataclass(frozen=True)
class HolderIdentity:
    name: str
    kind: HolderKind


@dataclass(frozen=True)
class CpuReservation:
    holder: HolderIdentity
    metadata: Mapping[str, object]
    _directory: Path = field(repr=False)

    @property
    def physical_cpu_count(self) -> int:
        return len(list(self._directory.glob('cpu-*')))

    @property
    def has_contents(self) -> bool:
        return any(self._directory.iterdir())


@dataclass(frozen=True)
class GpuReservation:
    holder: HolderIdentity
    metadata: Mapping[str, object]
    owns_device: bool


class HolderCensus:
    """A decision's lazy reads, using its existing metadata error policy.

    Holder names and metadata are read when requested, never retained across
    decisions. Consumers choose whether to materialize the holder iterator.
    """

    def __init__(self, directory: Path,
                 read_metadata: Callable[[Path], dict[str, object]]):
        self._directory = directory
        self._read_metadata = read_metadata

    @staticmethod
    def _identity(name: str) -> HolderIdentity:
        kind = (HolderKind.HOST_MEMORY_MIRROR
                if name.startswith(RAM_HOST_MEMORY_PREFIX)
                else HolderKind.OTHER_HOLDER)
        return HolderIdentity(name, kind)

    @property
    def non_mirror_holders(self) -> Iterator[HolderIdentity]:
        for directory in self._directory.iterdir():
            if directory.is_dir():
                holder = self._identity(directory.name)
                if holder.kind is not HolderKind.HOST_MEMORY_MIRROR:
                    yield holder

    def cpu(self, name: str) -> CpuReservation:
        directory = self._directory / name
        metadata = self._read_metadata(directory / CPU_METADATA)
        return CpuReservation(self._identity(name), MappingProxyType(metadata), directory)

    def gpu(self, name: str) -> GpuReservation:
        directory = self._directory / name
        metadata = self._read_metadata(directory / GPU_METADATA)
        owns_device = bool(metadata) or any(directory.glob('gpu-*'))
        return GpuReservation(self._identity(name), MappingProxyType(metadata), owns_device)

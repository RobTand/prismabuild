"""Holder evidence without assuming a ledger name proves execution."""
from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from types import MappingProxyType

from .storage_tiers import RAM_HOST_MEMORY_PREFIX

CPU_METADATA = '.adaptive.json'
GPU_METADATA = '.gpu.json'
ACQUIRING_PREFIX = 'claiming.'


def is_private_holder(name: str) -> bool:
    return name.startswith(ACQUIRING_PREFIX)


class HolderKind(Enum):
    OTHER_HOLDER = 'other_holder'
    PRIVATE_HOLDER = 'private_holder'
    HOST_MEMORY_MIRROR = 'host_memory_mirror'


@dataclass(frozen=True)
class HolderIdentity:
    name: str
    kind: HolderKind


@dataclass(frozen=True)
class HolderRecord:
    value: Mapping[str, object] | None

    @property
    def present(self) -> bool:
        return self.value is not None

    @property
    def fields(self) -> Mapping[str, object]:
        return self.value if self.value is not None else MappingProxyType({})

    def as_record(self) -> dict[str, object] | None:
        return dict(self.value) if self.value is not None else None


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
                 read_metadata: Callable[[Path], dict[str, object] | None], *,
                 scan: Callable[[Path], Iterable[Path]] | None = None,
                 token_paths: Callable[[Path, str], Iterable[Path]] | None = None):
        self._directory = directory
        self._read_metadata = read_metadata
        self._scan = scan
        self._token_paths = token_paths

    @staticmethod
    def _identity(name: str) -> HolderIdentity:
        if name.startswith(RAM_HOST_MEMORY_PREFIX):
            kind = HolderKind.HOST_MEMORY_MIRROR
        elif is_private_holder(name):
            kind = HolderKind.PRIVATE_HOLDER
        else:
            kind = HolderKind.OTHER_HOLDER
        return HolderIdentity(name, kind)

    @property
    def holders(self) -> Iterator[HolderIdentity]:
        directories = (self._directory.iterdir() if self._scan is None
                       else self._scan(self._directory))
        for directory in directories:
            if directory.is_dir():
                yield self._identity(directory.name)

    @property
    def non_mirror_holders(self) -> Iterator[HolderIdentity]:
        return (holder for holder in self.holders
                if holder.kind is not HolderKind.HOST_MEMORY_MIRROR)

    @property
    def non_private_holders(self) -> tuple[HolderIdentity, ...]:
        return tuple(sorted(
            (holder for holder in self.holders
             if holder.kind is not HolderKind.PRIVATE_HOLDER),
            key=lambda holder: holder.name))

    def _tokens(self, directory: Path, pattern: str) -> Iterable[Path]:
        return (directory.glob(pattern) if self._token_paths is None
                else self._token_paths(directory, pattern))

    def token_counts(self, name: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        for path in self._tokens(self._directory / name, '*-*'):
            kind = path.name.rsplit('-', 1)[0]
            counts[kind] = counts.get(kind, 0) + 1
        return counts

    def token_totals(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for holder in self.holders:
            for kind, count in self.token_counts(holder.name).items():
                counts[kind] = counts.get(kind, 0) + count
        return counts

    def _record(self, name: str, marker: str) -> HolderRecord:
        value = self._read_metadata(self._directory / name / marker)
        return HolderRecord(MappingProxyType(value) if value is not None else None)

    def cpu_record(self, name: str) -> HolderRecord:
        return self._record(name, CPU_METADATA)

    def gpu_record(self, name: str) -> HolderRecord:
        return self._record(name, GPU_METADATA)

    def has_gpu_reservation(self, name: str) -> bool:
        return (bool(list(self._tokens(self._directory / name, 'gpu-*')))
                or bool(self.gpu_record(name).fields))

    def cpu(self, name: str) -> CpuReservation:
        directory = self._directory / name
        return CpuReservation(self._identity(name), self.cpu_record(name).fields, directory)

    def gpu(self, name: str) -> GpuReservation:
        directory = self._directory / name
        metadata = self.gpu_record(name).fields
        owns_device = bool(metadata) or any(self._tokens(directory, 'gpu-*'))
        return GpuReservation(self._identity(name), metadata, owns_device)

"""Typed requests, explicit fleet roots and submission diagnostics."""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, NoReturn


@dataclass(frozen=True)
class Display:
    text: str
    channel: Literal["stdout", "stderr"] = "stderr"
    end: str = "\n"
    flush: bool = True


type DisplaySink = Callable[[Display], None]


def discard_display(event: Display) -> None:
    pass


def notice(display: DisplaySink, *values: object, channel: Literal["stdout", "stderr"] = "stdout",
           sep: str = " ", end: str = "\n", flush: bool = False) -> None:
    display(Display(sep.join(str(value) for value in values), channel, end, flush))


class SubmissionRefused(Exception):
    """A checked refusal with a stable client status and display text."""

    def __init__(self, reason: str | int | None, *, returncode: int | None = None) -> None:
        self.returncode = (reason if isinstance(reason, int) else 0 if reason is None else 1) if returncode is None else returncode
        self.reason = reason if isinstance(reason, str) else ""
        super().__init__(self.reason)


class OfferDiscoveryTimedOut(SubmissionRefused):
    """An unpublished offer timeout whose reader was reaped."""


@dataclass(frozen=True)
class FleetStore:
    root: Path
    runtime_root: Path
    roster_path: Path
    queue_root: Path | None = None
    cas_root: Path | None = None
    wrapper_dir: Path | None = None
    runtime_version: Path | None = None
    d38_config_path: Path | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", Path(self.root))
        object.__setattr__(self, "runtime_root", Path(self.runtime_root))
        object.__setattr__(self, "roster_path", Path(self.roster_path))
        object.__setattr__(self, "d38_config_path", Path(self.d38_config_path) if self.d38_config_path is not None else self.roster_path)
        object.__setattr__(self, "queue_root", Path(self.queue_root) if self.queue_root is not None else self.root / "pb-queue")
        object.__setattr__(self, "cas_root", Path(self.cas_root) if self.cas_root is not None else self.root / "cas")
        object.__setattr__(self, "wrapper_dir", Path(self.wrapper_dir) if self.wrapper_dir is not None else self.runtime_root / "tools")
        object.__setattr__(self, "runtime_version", Path(self.runtime_version) if self.runtime_version is not None else self.runtime_root / "RUNTIME_VERSION.json")


@dataclass(frozen=True, kw_only=True)
class SubmissionRequest:
    demand: str = ''
    gpu: bool = False
    cpus: int = 1
    exclusive: bool = False
    gpu_memory_gb: float | None = None
    gpu_capacity: int = 0
    tag: Sequence[str] = ()
    requires_files: str | None = None
    d38_receipt: str | None = None
    d38_exception: str | None = None
    d38_namespace: str | None = None
    container_image: Sequence[str] = ()
    measurement: bool = False
    host_class: str | None = None
    target_evidence: str | None = None
    anywhere: bool = False
    here: bool = False
    resident_set: str | None = None
    data_manifest: str | None = None
    after: Sequence[str] = ()
    supersedes: str | None = None
    produced_output_template: str | None = None
    residency: str = 'none'
    residency_tier: str | None = None
    residency_ram: str = 'auto'
    residency_share: str = 'auto'
    residency_mover_mem_gb: int = 1
    residency_mover_readers: int = 4
    residency_prefetch_depth_gib: int | None = None
    residency_read_mb_s: int | None = None
    residency_mover_max_attempts: int = 3
    checkout_snapshot_max_bytes: int = 512 * 1024 * 1024
    snapshot_ref: Sequence[str] = ()
    cwd: str = field(default_factory=lambda: str(Path.cwd()))
    as_sealed_by: str | None = None
    deterministic: bool = False
    gang_group: str | None = None
    gang_size: int | None = None
    gang_index: int | None = None
    retry_safe: bool = False
    max_attempts: int = 1
    timeout_s: float | None = None
    awaited_batch: Mapping[str, object] | None = None
    wait_s: float = 86400.0
    filesystem_growth_gib: int = 1
    detach: bool = False
    priority: int = 0
    priority_reason: str | None = None
    profile: str | None = None
    env: Sequence[str] = ()
    no_default_env: bool = False
    withdraw: Sequence[str] = ()
    reason: str = ''
    release_origin_consumer: tuple[str, str] | None = None
    transport: Literal["pool", "slurm"] = "pool"
    command: Sequence[str] = ()
    progress_policy: Mapping[str, object] | None = None

    def refuse_argument(self, reason: str) -> NoReturn:
        raise SubmissionRefused(reason, returncode=2)


@dataclass(frozen=True, kw_only=True)
class ProducerPublication:
    worker_script: Path
    checkout_root: str | Path | None
    tags: Sequence[str]
    needs_gpu: bool
    resources: Mapping[str, int]
    retry_safe: bool | None
    lane_root: str | Path | None
    job_entry: Path
    sbatch: str = "sbatch"

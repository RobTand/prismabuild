"""Code contracts that fence mixed worker generations through placement tags.

The boundary adapters supply normalized declarations and worker enablement.
Availability, resource capacity and host placement facts belong to their
existing owners. These rows perform no observation or I/O.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass


PROGRESS_TAG = "progress-v1"
PROGRESS_HELPER_TAG = "progress-helper-v1"
PROGRESS_CYCLE_TAG = "progress-cycle-v1"
POOL_CONTENTION_TAG = "progress-pool-contention-v1"
EGRESS_PROGRESS_TAG = "progress-egress-v1"
QUEUED_CHILD_TAG = "progress-queued-child-v1"
CONTAINER_IMAGE_TAG = "container-image-v1"
INTERPRETER_TAG = "interpreter-path-v1"
LOCAL_DEPENDENCY_TAG = "local-dependency-v1"
DEPENDENCY_DIGEST_TAG = "dependency-digest-v1"
SCRATCH_IO_TAG = "local-scratch-io-v1"
SCRATCH_LIFETIME_TAG = "scratch-lifetime-v1"
GANG_TAG = "gang-v1"
PUBLICATION_CANARY_TAG = "publication-canary-slot-v1"


@dataclass(frozen=True)
class Publication:
    """Validated declaration facts, independent of their publisher's storage."""

    progress: Mapping[str, object] | None = None
    pool_contention: bool = False
    egress: bool = False
    awaited_batch: bool = False
    container_images: bool = False
    interpreter: bool = False
    local_dependencies: bool = False
    requires_files: bool = False
    scratch_io: bool = False
    scratch_lifetime: bool = False
    gang: bool = False
    canary: bool = False


@dataclass(frozen=True)
class WorkerFacts:
    """Enablement of code loaded by a worker, never current availability."""

    gang_admission: bool = False
    loaded_generation: str = ""


def _always(_worker: WorkerFacts) -> bool:
    return True


def _none(_worker: WorkerFacts) -> tuple[str, ...]:
    return ()


@dataclass(frozen=True)
class Contract:
    tag: str
    declared: Callable[[Publication], bool]
    offered: Callable[[WorkerFacts], bool] = _always
    companions: Callable[[WorkerFacts], tuple[str, ...]] = _none
    remedy: str = "Update the published runtime and wait for worker adoption."


CONTRACTS = (
    Contract(PROGRESS_TAG, lambda p: p.progress is not None),
    Contract(PROGRESS_HELPER_TAG, lambda p: p.progress is not None),
    Contract(PROGRESS_CYCLE_TAG,
             lambda p: p.progress is not None and bool(p.progress.get("cycle"))),
    Contract(POOL_CONTENTION_TAG, lambda p: p.pool_contention),
    Contract(EGRESS_PROGRESS_TAG, lambda p: p.egress),
    Contract(QUEUED_CHILD_TAG, lambda p: p.awaited_batch),
    Contract(CONTAINER_IMAGE_TAG, lambda p: p.container_images,
             remedy="Update the published runtime, then load the declared image on an eligible worker."),
    Contract(INTERPRETER_TAG, lambda p: p.interpreter,
             remedy="Update the published runtime, then install the declared interpreter on an eligible worker."),
    Contract(LOCAL_DEPENDENCY_TAG, lambda p: p.local_dependencies,
             remedy="Update the published runtime, then provide the declared local dependencies on an eligible worker."),
    Contract(DEPENDENCY_DIGEST_TAG, lambda p: p.requires_files,
             remedy="Update the published runtime, then provide the exact declared dependency bytes on an eligible worker."),
    Contract(SCRATCH_IO_TAG, lambda p: p.scratch_io),
    Contract(SCRATCH_LIFETIME_TAG, lambda p: p.scratch_lifetime),
    Contract(GANG_TAG, lambda p: p.gang,
             offered=lambda w: w.gang_admission,
             remedy="Enable gang admission on an eligible worker."),
    Contract(PUBLICATION_CANARY_TAG, lambda p: p.canary,
             offered=lambda w: bool(w.loaded_generation),
             companions=lambda w: (f"runtime-generation:{w.loaded_generation}",),
             remedy="Wait for an eligible worker to load the canary's published generation."),
)


def declared_contract_tags(publication: Publication) -> tuple[str, ...]:
    return tuple(row.tag for row in CONTRACTS if row.declared(publication))


def offered_contract_tags(worker: WorkerFacts) -> tuple[str, ...]:
    tags = []
    for row in CONTRACTS:
        if row.offered(worker):
            tags.extend((row.tag, *row.companions(worker)))
    return tuple(tags)


def contract_remedies(tags: Iterable[str]) -> tuple[str, ...]:
    required = set(tags)
    return tuple(dict.fromkeys(row.remedy for row in CONTRACTS
                               if row.tag in required and row.remedy))

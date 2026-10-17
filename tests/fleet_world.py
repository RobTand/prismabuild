"""Explicit private fleet setup using production record and queue writers."""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import tempfile
import time

from prismabuild import pool

from admitted_queue_fixture import AdmittedQueueFixture
from residency_publication import vouch_landed


@dataclass
class Clock:
    now: float = 1000.0

    def advance(self, seconds: float) -> float:
        self.now += seconds
        return self.now

    def install(self, monkeypatch) -> None:
        monkeypatch.setattr(time, "time", lambda: self.now)


@dataclass(frozen=True)
class Admission:
    capacity: Mapping[str, int]
    default_demand: Mapping[str, int]

    def bind(self, queue: pool.PoolQueue) -> AdmittedQueueFixture:
        return AdmittedQueueFixture(queue, capacity=self.capacity,
                                    default_demand=self.default_demand)


class ActionRows:
    def __init__(self, queue: pool.PoolQueue) -> None:
        self.queue = queue

    def publish(self, key: str, **kw: object) -> None:
        self.queue.publish(
            action_key=key,
            cas_root=kw.pop("cas_root", "/cas"),
            checkout_root=kw.pop("checkout_root", "/co"),
            worker_script=kw.pop("worker_script", "/w.py"),
            **kw,
        )


class FleetWorld:
    def __init__(self, root: Path, *, queue_name: str = "pb-queue",
                 admission: Admission | None = None, clock: Clock | None = None) -> None:
        self.root = Path(root)
        raw_queue = pool.PoolQueue(self.root / queue_name)
        self.queue = raw_queue if admission is None else admission.bind(raw_queue)
        self.queue.records.ensure_layout()
        self.clock = Clock() if clock is None else clock
        self.actions = ActionRows(self.queue)

    def host(self, name: str, *, capacity: Mapping[str, int], tags=(),
             has_gpu: bool = False, **observations: object) -> None:
        self.queue.announce(host=name, capacity=capacity, tags=tags,
                            has_gpu=has_gpu, **observations)

    @staticmethod
    def use_host(monkeypatch, name: str) -> None:
        monkeypatch.setattr(pool.socket, "gethostname", lambda: name)

    def stage_root(self, name: str) -> Path:
        path = self.root / "stage" / name
        path.mkdir(parents=True, exist_ok=True)
        return path

    def tier(self, record: Mapping[str, object], *,
             supply: Mapping[str, int]) -> None:
        self.queue.announce_tier(record, now=self.clock.now)
        self.queue.mint_tier_capacity(str(record["tier_id"]), supply)

    def landed(self, **evidence: object) -> None:
        vouch_landed(self.queue, **evidence)

    @contextmanager
    def broker_directory(self):
        with tempfile.TemporaryDirectory(prefix="pbw-", dir="/tmp") as directory:
            yield Path(directory)

    def corrupt(self, relative_path: str, raw: bytes) -> Path:
        relative = Path(relative_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("corruption must name a private world path")
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        return path

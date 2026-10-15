"""Independent queue, host, clock and registered-stage roles for test fleets."""
from __future__ import annotations

import os
import uuid

from prismabuild import pool, stage_marks
import stage_move
import stage_release


class Clock:
    def __init__(self, monkeypatch):
        self.patch = monkeypatch

    def set(self, *, monotonic=None, unix=None):
        if monotonic is not None:
            self.patch.setattr(pool.time, "monotonic", lambda: monotonic)
        if unix is not None:
            self.patch.setattr(pool.time, "time", lambda: unix)


class Hosts:
    def __init__(self, queue, monkeypatch):
        self.queue, self.patch = queue, monkeypatch

    def announce(self, name, *, capacity, preferred, fallback=(), tags=(), gpu=False):
        tiers = {"preferred": list(preferred), "fallback": list(fallback)}
        ledger = self.queue.ledger(name)
        ledger.configure_cpu_tiers(tiers)
        ledger.ensure_capacity(capacity)
        self.queue.announce(host=name, tags=tags, has_gpu=gpu,
                            capacity=capacity, cpu_tiers=tiers)

    def preferred_reads(self, name):
        remote = self.queue.ledger(name)
        reads = []
        original = pool.ResourceLedger.free_preferred

        def free_preferred(ledger, cpu_tiers):
            if ledger.base == remote.base:
                reads.append(ledger.base)
            return original(ledger, cpu_tiers)

        self.patch.setattr(pool.ResourceLedger, "free_preferred", free_preferred)
        return reads


class Actions:
    def __init__(self, queue, root):
        self.queue, self.root = queue, root

    @staticmethod
    def key():
        return uuid.uuid4().hex + uuid.uuid4().hex

    def publish(self, index, *, cpus=1, tags=()):
        worker = self.root / "affinity.py"
        worker.write_text(
            'import os, subprocess, sys\n'
            'child = subprocess.check_output([sys.executable, "-c", '
            '"import json,os; print(json.dumps(sorted(os.sched_getaffinity(0))))"])\n'
            'print(child.decode().strip())\n')
        key = f"{index:064x}"
        self.queue.publish(action_key=key, cas_root=str(self.root / "cas"),
                           checkout_root=str(self.root), worker_script=str(worker),
                           resources={"cpu": cpus}, tags=tags)
        return key

    def claim_new(self, key):
        self.queue.publish(action_key=key, cas_root="/cas", checkout_root="/co",
                           worker_script="/w.py", resources={"cpu": 1}, max_attempts=1)
        claimed = self.queue.claim(capacity={"cpu": 4})
        assert claimed is not None and claimed["action_key"] == key
        return claimed

    def failed_consumer(self):
        key = self.key()
        self.claim_new(key)
        self.queue.finish(key, status="failed", detail={"returncode": 1})
        assert self.queue.item_path(pool.FAILED, key).exists()
        return key

    def withdrawn_mover(self):
        key = self.key()
        holder = self.claim_new(key)
        self.queue.withdraw(key, reason="stale test owner", by="test")
        self.queue.finish(key, status="withdrawn", detail={"returncode": -15},
                          claim_snapshot=holder)
        assert self.queue.item_path(pool.WITHDRAWN, key).exists()
        return key


class Stage:
    def __init__(self, queue, root, cas_root, tier, actions, damage):
        self.queue, self.root, self.cas_root = queue, root, cas_root
        self.tier, self.actions, self.damage = tier, actions, damage
        root.mkdir(parents=True)
        cas_root.mkdir(exist_ok=True)
        assert stage_release.register_stage_root(
            queue, tier_id=tier, stage_root=root) == "registered"

    def dead_owner_fragment(self, *, names, size):
        consumer = self.actions.failed_consumer()
        mover = self.actions.withdrawn_mover()
        for name in names:
            path = self.root / name
            path.write_bytes(b"\0" * size)
            os.setxattr(path, stage_marks.STAGE_SOURCE_XATTR,
                        f"/originals/{name}@0".encode())
        self.damage.fragment_without_move_receipt(
            tier=self.tier, stage=self.root, consumer=consumer, mover=mover,
            names=names, size=size)
        assert self.queue.move_record(mover) is None, "dead mover must file no receipt"
        assert mover not in self.queue.tier_ledger(self.tier).held_keys()
        assert not self.queue.item_path(pool.CLAIMED, mover).exists()
        return consumer, mover

    def publisher(self, *, mover, consumer):
        return stage_move._StagedPublisher(
            queue=self.queue, stage_root=self.root,
            residency_root=self.queue.root / pool.RESIDENCY,
            mover_action_key=mover, manifest_sha256="a" * 64,
            tier_id=self.tier, cas_root=self.cas_root, consumer_action_key=consumer)


class Stages:
    def __init__(self, queue, root, actions, damage):
        self.queue, self.root = queue, root
        self.actions, self.damage = actions, damage
        self.registered = {}

    def register(self, tier):
        stage = Stage(self.queue, self.root / f"stage-{len(self.registered)}",
                      self.root / "cas", tier, self.actions, self.damage)
        self.registered[tier] = stage
        return stage

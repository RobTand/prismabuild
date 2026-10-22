"""Scratch-only real queue writer and bounded archive/gate fixtures for #2023."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import time

from prismabuild import attempt_archive as archive, attempt_retention as retention
from prismabuild import digest_primitives as digests, immutable_files as native, pool
from prismabuild.generation_receipt import RUNTIME_SCHEMA
from admitted_queue_fixture import AdmittedQueueFixture


class AttemptArchiveFixture:
    def __init__(self, tmp_path, monkeypatch):
        self.now = time.time()
        self.old = self.now - 30 * 86400
        self.cas = tmp_path / "cas"
        self.checkout = tmp_path / "checkout"
        self.worker_script = tmp_path / "worker.py"
        self.cas.mkdir()
        self.checkout.mkdir()
        self.worker_script.write_text("# scratch-only writer fixture; never executed\n")
        self.queue = pool.PoolQueue(tmp_path / "queue")
        self.writer = AdmittedQueueFixture(self.queue, capacity={"cpu": 8, "mem_gb": 16},
                                          default_demand={"cpu": 1, "mem_gb": 1})
        self.queue.records.ensure_layout()
        self.monkeypatch = monkeypatch
        self.config = dict(schema=archive.RETENTION_CONFIG_SCHEMA, queue_id="a" * 64,
                           store_id="b" * 64, archive_root=str(tmp_path / "archive"),
                           dataset="scratch/pb-evidence", dataset_guid="12345", retention_days=14,
                           reader_gate="retention-gates/gate.json")
        self.root = Path(self.config["archive_root"])
        native.publish_immutable(self.root / "store.json", digests._canonical_bytes(
            dict(schema=archive.ARCHIVE_STORE_SCHEMA, queue_root=str(self.queue.root),
                 **{field: self.config[field] for field in ("queue_id", "store_id", "dataset", "dataset_guid")})))
        self.write_config()
        original_run = subprocess.run
        def observe_backing(argv, **kwargs):
            if argv[0] == "zfs":
                return subprocess.CompletedProcess(argv, 0,
                    f"guid\t12345\nmountpoint\t{self.root}\nsync\tstandard\ncompression\tzstd\n".encode(), b"")
            if argv[0] == "findmnt":
                return subprocess.CompletedProcess(argv, 0, b"scratch/pb-evidence zfs\n", b"")
            return original_run(argv, **kwargs)
        monkeypatch.setattr(archive.subprocess, "run", observe_backing)
        self.generation = "qualified-reader-generation"
        self.directory = tmp_path / "generations" / self.generation
        source = Path(archive.__file__).read_bytes()
        target = self.directory / "src/prismabuild/attempt_archive.py"
        native.publish_immutable(target, source)
        files = {"src/prismabuild/attempt_archive.py": digests.raw_sha256(source)}
        raw = digests._canonical_bytes(dict(schema=RUNTIME_SCHEMA, generation=self.generation,
                                           commit="c" * 40, files=files))
        native.publish_immutable(self.directory / "RUNTIME_VERSION.json", raw)
        self.generation_row = dict(generation=self.generation, directory=str(self.directory),
                                   commit="c" * 40, receipt_sha256=digests.raw_sha256(raw),
                                   content_sha256=digests.canonical_sha256(files))
        self.gate = self.make_gate()
        self.write_gate()
        self.offer()
        self.counter = 0

    def write_config(self):
        native.write_bytes_atomic(self.queue.root / archive.RETENTION_CONFIG, digests._canonical_bytes(self.config))

    def make_gate(self):
        issued = self.now - 60
        members = []
        roles = [("worker:test-host", "active_generation"), ("rollback:designated", "rollback_generation"),
                 ("detached:qualified", "detached_client"), ("mcp:qualified", "mcp"),
                 ("tl:computation", "tl_computation_runner"), ("tl:test", "tl_test_runner")]
        for index, (identity, role) in enumerate(roles):
            proof = dict(schema=retention.READER_PROOF_SCHEMA, id=identity, role=role,
                         generation=self.generation, capabilities=[archive.ATTEMPT_LOG_RESOLVER_CAPABILITY],
                         verified_unix=issued)
            raw = digests._canonical_bytes(proof)
            relative = f"retention-gates/proof-{index}.json"
            native.publish_immutable(self.queue.root / relative, raw)
            members.append(dict(id=identity, role=role, generation=self.generation,
                                evidence=dict(path=relative, bytes=len(raw), sha256=digests.raw_sha256(raw))))
        return dict(schema=retention.READER_GATE_SCHEMA, queue_id=self.config["queue_id"],
                    store_id=self.config["store_id"], queue_root=str(self.queue.root),
                    config_sha256=digests.canonical_sha256(self.config), issued_unix=issued,
                    expires_unix=self.now + 3600, inventory_complete=True,
                    inventory_attested_by="scratch fixture operator", deferred_inventory_complete=True,
                    approvals={role: dict(by=role, reason="scratch-only qualification", unix=issued)
                               for role in ("pb_lead", "tl_lead")}, members=members,
                    generations=[self.generation_row])

    def write_gate(self):
        inventory_sha = digests.canonical_sha256({field: self.gate[field] for field in (
            "members", "generations", "inventory_complete", "deferred_inventory_complete", "inventory_attested_by")})
        for approval in self.gate["approvals"].values():
            approval.update(config_sha256=self.gate["config_sha256"], inventory_sha256=inventory_sha)
        path = self.queue.root / self.config["reader_gate"]
        if path.exists():
            path.unlink()
        native.publish_immutable(path, digests._canonical_bytes(self.gate))

    def offer(self, generation=None, *, state=None, announced=None):
        value = dict(schema=pool.POOL_OFFER_SCHEMA_V1, host="test-host", runtime_commit="c" * 40,
                     tags=[f"runtime-generation:{generation or self.generation}"],
                     announced_unix=self.now if announced is None else announced)
        if state:
            value["state"] = state
        native.write_bytes_atomic(self.queue.root / "workers/test-host.json", digests._canonical_bytes(value))

    def finish(self, key="e" * 64, *, stdout="stdout evidence\n", stderr="stderr evidence\n", status="executed"):
        self.counter += 1
        with self.monkeypatch.context() as clock:
            clock.setattr(pool, "_now", lambda: self.old + self.counter)
            self.writer.publish(action_key=key, cas_root=str(self.cas), checkout_root=str(self.checkout),
                                worker_script=str(self.worker_script), max_attempts=1)
            assert self.writer.claim() is not None
            path = self.writer.finish(key, status=status, detail=dict(
                returncode=0 if status == "executed" else 23, stdout=stdout, stderr=stderr))
        terminal = json.loads(path.read_bytes())
        for candidate in (path, self.queue.records.item_path("intent", key),
                          *self.queue.records.attempt_path(terminal, 1).parent.iterdir()):
            os.utime(candidate, (self.old, self.old))
        return terminal

    def descriptors(self, terminal):
        attempt = self.queue.records.attempt_document(self.queue.records.attempt_path(terminal, 1))
        return attempt["logs"]

    def plan(self, **kwargs):
        return retention.survey_attempt_log_retention(self.queue, now=self.now, **kwargs)

    def retire(self, **kwargs):
        return retention.apply_attempt_log_retention(self.queue, self.plan(**kwargs))

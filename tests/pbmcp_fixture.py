"""One queue with a ready, a claimed and an ended action, and a CAS to match.

Shared by the ``pbmcp`` tests because the interesting properties are about
one populated queue seen five ways -- over the protocol, in process, against
a read-only root, past a deadline, across a generation change -- and building
it five times would let the five drift.

Everything here is built with the producers' own code.  ``publish``,
``claim`` and ``finish`` make the queue records, the attempt outcomes and
their immutable logs, so the tests read what the fleet actually writes rather
than a hand-typed imitation of it.  The CAS side is written by hand, because
publishing a real receipt means running a real action, but the digests are
computed with ``core.canonical_sha256`` over the producers' own key sets --
so a change to either would break these fixtures rather than let a stale
expectation pass.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
import time

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402

import residency_publication  # noqa: E402

READY_KEY = "a" * 64
CLAIMED_KEY = "c" * 64
DONE_KEY = "d" * 64
#: Two keys sharing a prefix, so ambiguity has something to be ambiguous about.
TWIN_KEY = "d" * 63 + "e"

#: The staged consumer the starvation census reads: claimed, mid-read on its
#: first phase, quiet past half its grace with a silent payload scope.
CONSUMER_KEY = "e" * 64
CONSUMER_MANIFEST = "9" * 64
STAGE_TIER = "prismabuild-stage:fixture-box"
RAM_TIER = "ram:fixture-box"
STAGE_KIND = f"stage_gib@{STAGE_TIER}"
RAM_KIND = f"ram_gib@{RAM_TIER}"
RAM_EPOCH = "1695052800-1a2b3c4d5e6f7a8b"
STARVED_GIB = storage_tiers.GIB

STDOUT = "first line\nsecond line\nthird line\n"
STDERR = "a warning\n"
PAYLOAD = b"the result payload\n"

GENERATION_A = "aaaaaaaaaaaa-1700000000-aaaaaaaaaaaa"
GENERATION_B = "bbbbbbbbbbbb-1700000001-bbbbbbbbbbbb"


class Fleet:
    """The paths one fixture built, named rather than returned as a tuple."""

    def __init__(self, base: Path) -> None:
        self.base = base
        self.queue_root = base / "queue"
        self.cas_root = base / "cas"
        self.checkout = base / "checkout"
        self.repo_link = base / "repo"
        self.generations = base / "runtime-generations"

    @property
    def queue(self) -> pool.PoolQueue:
        return pool.PoolQueue(self.queue_root)

    def record(self, state: str, key: str) -> dict:
        return json.loads(
            self.queue.item_path(state, key).read_text(encoding="utf-8"))

    def attempt_log(self, key: str, attempt: int = 1, stream: str = "stdout") -> Path:
        record = self.record(pool.DONE, key)
        outcome = json.loads(
            self.queue.attempt_path(record, attempt).read_text(encoding="utf-8"))
        return self.queue_root / str(outcome["logs"][stream]["path"])


def build(base: Path, *, host: str = "fixture-box") -> Fleet:
    """A queue holding one of each state, and a CAS holding one result."""

    fleet = Fleet(base)
    fleet.checkout.mkdir(parents=True, exist_ok=True)
    queue = pool.PoolQueue(fleet.queue_root)
    queue.ensure_layout()
    common = {
        "cas_root": fleet.cas_root,
        "checkout_root": str(fleet.checkout),
        "worker_script": str(fleet.base / "worker.py"),
        "needs_gpu": False,
    }
    # Published, claimed and finished one at a time, because ``claim`` takes
    # whatever the queue holds and the point of the fixture is one action in
    # each state.
    queue.publish(action_key=DONE_KEY, tags=["x86"], priority=-10,
                  resources={"cpu": 2, "mem_gb": 4}, max_attempts=2, **common)
    queue.claim(tags=["x86"], capacity={"cpu": 8, "mem_gb": 16})
    queue.finish(DONE_KEY, status="executed",
                 detail={"returncode": 0, "elapsed_s": 1.5, "status": "executed",
                         "stdout": STDOUT, "stderr": STDERR})
    queue.publish(action_key=CLAIMED_KEY, tags=["x86"], priority=0,
                  resources={"cpu": 1, "mem_gb": 2}, **common)
    queue.claim(tags=["x86"], capacity={"cpu": 8, "mem_gb": 16})
    queue.publish(action_key=READY_KEY, tags=["gb10"], priority=5,
                  resources={"cpu": 3, "mem_gb": 6}, **common)
    _write_manifest_and_receipt(fleet)
    _write_generations(fleet)
    return fleet


def action_manifest(fleet: Fleet, key: str = DONE_KEY) -> dict:
    """The request manifest the CAS holds for the ended action.

    Only the fields ``pbmcp`` derives the local-result claim from have to be
    real here: the claim body is the manifest digest, the resolved checkout
    root, and the working directory and result path the task declares.
    """

    return {
        "schema": pb.ACTION_SCHEMA_V2,
        "action_key": key,
        "task": {"working_directory": ".", "result_path": "result.json"},
    }


def claim_digest(fleet: Fleet, key: str = DONE_KEY) -> str:
    body = claim_body(fleet, key)
    return pb.canonical_sha256(body)


def claim_body(fleet: Fleet, key: str = DONE_KEY) -> dict:
    manifest = action_manifest(fleet, key)
    return {
        "schema": pb.LOCAL_RESULT_CLAIM_SCHEMA_V1,
        "action_key": key,
        "action_manifest_sha256": pb.canonical_sha256(manifest),
        "checkout_root": str(fleet.checkout),
        "working_directory": manifest["task"]["working_directory"],
        "result_path": manifest["task"]["result_path"],
    }


def _write(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    path.chmod(0o444)


def _write_manifest_and_receipt(fleet: Fleet) -> None:
    key = DONE_KEY
    manifest = action_manifest(fleet, key)
    _write(fleet.cas_root / "requests" / key[:2] / f"{key}.json",
           json.dumps(manifest).encode("utf-8"))

    body = claim_body(fleet, key)
    digest = pb.canonical_sha256(body)
    _write(fleet.cas_root / "local-results" / "v1" / digest[:2] / f"{digest}.json",
           json.dumps({**body, "claim_sha256": digest}).encode("utf-8"))

    payload_digest = hashlib.sha256(PAYLOAD).hexdigest()
    _write(fleet.cas_root / "blobs" / payload_digest[:2] / payload_digest, PAYLOAD)

    receipt_body = {
        "schema": pb.CAS_RECEIPT_SCHEMA_V3,
        "action_key": key,
        "action_manifest_sha256": pb.canonical_sha256(manifest),
        "result": {"sha256": payload_digest, "bytes": len(PAYLOAD)},
        "producer": {"schema": "fixture", "action_key": key},
    }
    receipt = {**receipt_body, "receipt_sha256": pb.canonical_sha256(receipt_body)}
    namespace = pb.CAS_RECEIPT_SCHEMA_V3.rsplit(".", 1)[-1]
    _write(fleet.cas_root / "actions" / namespace / key[:2] / f"{key}.json",
           json.dumps(receipt).encode("utf-8"))


def _write_generations(fleet: Fleet) -> None:
    for name in (GENERATION_A, GENERATION_B):
        directory = fleet.generations / name
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "RUNTIME_VERSION.json").write_text(json.dumps({
            "schema": "prismaquant.prismabuild.runtime_version.v1",
            "commit": name.split("-")[0] * 3,
            "dirty": False,
            "generation": name,
            "published_unix": 1700000000.0,
            "published_by": "fixture-box",
            "files": {"tools/fleet/pbmcp.py": {"sha256": "0" * 64, "bytes": 1}},
        }), encoding="utf-8")
    point_at(fleet, GENERATION_A)


def point_at(fleet: Fleet, generation: str) -> None:
    """Move ``repo`` to a generation the way the publisher does: atomically."""

    target = fleet.generations / generation
    staging = fleet.base / ".repo.next"
    if staging.is_symlink() or staging.exists():
        staging.unlink()
    os.symlink(target, staging)
    os.replace(staging, fleet.repo_link)


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _tier_row(key: str, resources: dict, queue: pool.PoolQueue) -> dict:
    """A mover/egress row shaped the way a submitter seals it."""

    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["fixture-box"], "resources": resources}


def _two_phase_plan(queue: pool.PoolQueue, *, stage_root: str) -> dict:
    """Two phases of two GiB; only the first carries a ram promotion leg."""

    phases = []
    start = 0
    for ordinal in range(2):
        end = start + 2 * STARVED_GIB
        entry: dict = {
            "name": f"phase-{ordinal:04d}",
            "start_bytes": start, "end_bytes": end, "stage_gib": 2,
            "mover_row": {
                **_tier_row(_hexkey(f"mover{ordinal}"),
                            {STAGE_KIND: 2, "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
                    "manifest_sha256": CONSUMER_MANIFEST, "manifest_bytes": end,
                    "range_start_bytes": start, "range_end_bytes": end}},
            "egress_row": _tier_row(_hexkey(f"egress{ordinal}"),
                                    {"mem_gb": 1}, queue),
        }
        if ordinal == 0:
            entry["ram_mover_row"] = {
                **_tier_row(_hexkey("rampromote0"),
                            {RAM_KIND: 2, "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
                    "manifest_sha256": CONSUMER_MANIFEST,
                    "manifest_bytes": end,
                    "range_start_bytes": start, "range_end_bytes": end}}
            entry["ram_egress_row"] = _tier_row(_hexkey("ramrelease0"),
                                                {"mem_gb": 1}, queue)
        phases.append(entry)
        start = end
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER_KEY, tier_id=STAGE_TIER,
        stage_root=stage_root, manifest_sha256=CONSUMER_MANIFEST,
        manifest_bytes=start, phases=phases, ram_tier_id=RAM_TIER)


def build_starved(base: Path, *, host: str = "fixture-box") -> Fleet:
    """``build``'s fleet, plus one staged consumer mid-read on phase one.

    Everything the starvation census and the cursor census read, filed the
    way the fleet files it: a frozen residency plan, a claimed consumer whose
    lease carries a progress observation (so the accepted phase and its
    timestamp are real records rather than assertions), tier announcements
    beside minted ledgers, a ram promotion fragment, a published-but-unstaged
    mover for phase two, and adaptive claim denials for the host and the
    mover.  The clock is read at build time rather than frozen, so the ages
    the tools derive are asserted approximately in the tests.
    """

    fleet = build(base, host=host)
    queue = fleet.queue
    now = time.time()
    plan = _two_phase_plan(queue, stage_root=str(base / "stage"))
    residency_plan.freeze(queue, plan)
    # The consumer, claimed and quiet on its first phase.  Filed directly
    # rather than through claim(): the claim gate admits a consumer only once
    # its leads are resident, and the census reads the claim, not the gate.
    queue.publish(**_tier_row(CONSUMER_KEY, {"mem_gb": 1}, queue), residency={
        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
        "manifest_sha256": CONSUMER_MANIFEST,
        "manifest_bytes": 4 * STARVED_GIB,
        "leads": residency_plan.leads_for(plan)})
    ready_path = queue.item_path(pool.READY, CONSUMER_KEY)
    record = json.loads(ready_path.read_text())
    record.update({"claimed_unix": now - 100, "claimed_host": host,
                   "claimed_by": f"worker-{host}"})
    queue.item_path(pool.CLAIMED, CONSUMER_KEY).write_text(json.dumps(record))
    ready_path.unlink()
    queue.lease_path(CONSUMER_KEY).write_text(json.dumps({
        "action_key": CONSUMER_KEY, "owner": record["claimed_by"], "host": host,
        "claimed_unix": record["claimed_unix"],
        "published_unix": record["published_unix"],
        "heartbeat_unix": now - 1,
        "progress_observation": {
            "phase": "encode", "quiet_s": 800.0, "grace_s": 900.0,
            "last_accepted": {"phase": "phase-0000",
                              "reported_unix": now - 800,
                              "units_completed": 3}},
        "execution_observation": {
            "child": {"alive": True, "pid_count": 3, "cpu_seconds": 12.0,
                      "silent_s": 700.0}}}))
    fleet.consumer_reported_unix = now - 800  # type: ignore[attr-defined]
    # Phase 0 staged on both tiers, with a ram fragment; phase 1 published
    # as a ready mover and staged nowhere.
    queue.mint_tier_capacity(STAGE_TIER, {"stage_gib": 64})
    queue.mint_tier_capacity(RAM_TIER, {"ram_gib": 8})
    assert queue.tier_ledger(STAGE_TIER).acquire(
        _hexkey("mover0"), {"stage_gib": 2})
    assert queue.tier_ledger(RAM_TIER).acquire(
        _hexkey("rampromote0"), {"ram_gib": 2})
    # Landed means landed (#759): the booking above is the room, and these
    # are the records a finished copy leaves.  The promotion's fragment
    # names the ram root it landed on, under the announced epoch, which is
    # what ``ram_promote`` writes and what the census now reports.
    residency_publication.vouch_landed(
        queue, consumer_action_key=CONSUMER_KEY,
        mover_action_key=_hexkey("mover0"), tier_id=STAGE_TIER,
        stage_root=base / "stage", manifest_sha256=CONSUMER_MANIFEST,
        range_start_bytes=0, range_end_bytes=2 * STARVED_GIB)
    residency_publication.vouch_landed(
        queue, consumer_action_key=CONSUMER_KEY,
        mover_action_key=_hexkey("rampromote0"), tier_id=RAM_TIER,
        stage_root=base / "ram", manifest_sha256=CONSUMER_MANIFEST,
        range_start_bytes=0, range_end_bytes=2 * STARVED_GIB,
        epoch=RAM_EPOCH)
    queue.publish(**_tier_row(_hexkey("mover1"),
                              {STAGE_KIND: 2, "mem_gb": 1}, queue),
                   residency={
                       "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
                       "manifest_sha256": CONSUMER_MANIFEST,
                       "manifest_bytes": 4 * STARVED_GIB,
                       "range_start_bytes": 2 * STARVED_GIB,
                       "range_end_bytes": 4 * STARVED_GIB})
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
        "tier_id": STAGE_TIER, "tier": "stage", "host": host,
        "mountpoint": str(base / "stage"),
        "capacity_bytes": 600 * STARVED_GIB, "sampled_unix": now - 5,
        "fill_source": "measured",
        storage_tiers.FILL_RECORD_FIELD: 310.0,
        "fill_supply": {"best_mb_s": 300.0, "ceiling_mb_s": None,
                        "may_grow": True}})
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
        "tier_id": RAM_TIER, "tier": "ram", "host": host,
        "mountpoint": str(base / "ram"),
        "capacity_bytes": 8 * STARVED_GIB, "sampled_unix": now - 5,
        "epoch": RAM_EPOCH, "window_gib": 4})
    published = json.loads(
        queue.item_path(pool.READY, _hexkey("mover1")).read_text())
    denials = {
        "schema": pool.CLAIM_DENIALS_SCHEMA_V1, "records": {
            "mover": {"action_key": _hexkey("mover1"),
                      "published_unix": published["published_unix"],
                      "host": host, "reason": "tier_reservation_unavailable",
                      "evidence": {"decision": {"reason": "tier_busy"}},
                      "denied_unix": now - 10},
            "consumer": {"action_key": CONSUMER_KEY,
                         "published_unix": record["published_unix"],
                         "host": host, "reason": "adaptive_cpu_refused",
                         "evidence": {}, "denied_unix": now - 20}}}
    adaptive = queue.ledger(host).base / "adaptive"
    adaptive.mkdir(parents=True, exist_ok=True)
    (adaptive / pool.CLAIM_DENIALS).write_text(json.dumps(denials))
    return fleet

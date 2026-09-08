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

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402

READY_KEY = "a" * 64
CLAIMED_KEY = "c" * 64
DONE_KEY = "d" * 64
#: Two keys sharing a prefix, so ambiguity has something to be ambiguous about.
TWIN_KEY = "d" * 63 + "e"

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

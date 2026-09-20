"""Produced-output lifecycle R2/R3: batch retirement + durable quota.

R2 proved bound writes, durable quota to reclaim, proof-tied retirement,
and attributed census on real batch interactions. R3 adds: write-gated
prewrite, loader-validated reclaim, the single egress-driven retire path,
and verified (not shape-only) producer holds in egress attribution.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import socket
from pathlib import Path
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, produced_output as po  # noqa: E402
from prismabuild import core as pb  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
from __future__ import annotations

import hashlib
import json
import os
import secrets
import socket
from pathlib import Path
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, produced_output as po  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402

try:
    from prismabuild import reader_lease as rlc
    HAS_SDK = True
except ImportError:
    rlc = None  # type: ignore[assignment]
    HAS_SDK = False

STAGE_TIER = "prismabuild-stage:dl380g10"
STAGE_BARE = "stage_gib"
OWNER = "c" * 64
DOWNSTREAM = "d" * 64
MOVER0 = "e" * 64
CAP = 1 << 20
BIG = 768 << 10


def _template(output_prefix: str, template_id: str = "r2-v1",
              payload_cap: int = CAP) -> dict:
    return po.validate_template({
        "schema": po.TEMPLATE_SCHEMA_V1,
        "version": 1,
        "template_id": template_id,
        "output_prefix": output_prefix,
        "slots": {
            "boundary-0": {"class": "payload"},
            "checkpoint-0": {"class": "checkpoint"},
        },
        "durable_maxima": {
            "payload_max_bytes": payload_cap,
            "checkpoint_max_bytes": 1 << 20,
            "temp_max_bytes": 1 << 20,
        },
        "working_demands": {
            STAGE_TIER: {"minimum_gib": 1, "window_gib": 2},
        },
        "permitted_tiers": [STAGE_TIER],
    })


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(STAGE_TIER, {STAGE_BARE: 8})
    return queue


def _publish_claim(queue: pool.PoolQueue, owner: str = OWNER) -> dict:
    queue.publish(action_key=owner, cas_root="/cas", worker_script="/w.py",
                  checkout_root="/co", resources={"cpu": 1, "mem_gb": 1})
    claimed = queue.claim(owner="fixture-worker")
    assert claimed is not None and claimed["action_key"] == owner
    return claimed


def _file_broker_control(queue: pool.PoolQueue, owner: str) -> dict:
    from prismabuild import resource_scope

    nonce = secrets.token_hex(16)
    scope = resource_scope.ResourceScope(
        owner, nonce, 1 * 1024 ** 3,
        queue.root / "telemetry" / f"{owner}.json")
    token = secrets.token_hex(32)
    unit = ("prismabuild-job"
            + hashlib.sha256((owner + nonce).encode()).hexdigest()[:32]
            + ".slice")
    scope._adopt_created_scope({
        "scope_id": unit, "token": token,
        "cgroup_path": str(Path("/sys/fs/cgroup/prismabuild.slice") / unit),
    })
    control = scope.control_record()
    path = queue.item_path(pool.CLAIMED, owner)
    live = pool._read_json(path)
    assert live is not None
    live["resource_scope"] = control
    pool._write_json_atomic(path, live)
    return control


def _bind(queue: pool.PoolQueue, template: dict, owner: str = OWNER):
    claimed = _publish_claim(queue, owner)
    control = _file_broker_control(queue, owner)
    env = {"PRISMABUILD_ACTION_KEY": owner,
           "PRISMABUILD_ACTION_NONCE": control["nonce"],
           "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
    po.declare_template(queue.root, template)
    instance = po.bind_instance(queue, template, owner_action_key=owner,
                                claim_snapshot=claimed, env=env)
    po.declare_instance(queue.root, instance)
    return {"instance": instance, "claimed": claimed, "control": control,
            "env": env}


def _producer_request(tmp_path: Path, owner: str, template: dict) -> Path:
    """File a sealed request binding the produced declaration (scaffolding).

    The params carry the produced declaration validated against the
    request's own inputs plus an ordinary command with no movement
    range flags -- exactly what egress attribution verifies. Every
    admission/egress decision stays real.
    """

    checked = po.validate_template(template)
    raw_template = json.dumps(
        checked, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    decl_input = {"id": pb.PRODUCED_OUTPUT_TEMPLATE_INPUT_ID,
                  "sha256": hashlib.sha256(raw_template).hexdigest(),
                  "bytes": len(raw_template)}
    declaration = {
        "schema": pb.PRODUCED_OUTPUT_DECLARATION_SCHEMA_V1,
        "template_id": str(checked["template_id"]),
        "template_sha256": po.template_sha256(checked),
        "input": dict(decl_input),
    }
    casdir = tmp_path / "cas"
    (casdir / "requests" / owner[:2]).mkdir(parents=True, exist_ok=True)
    (casdir / "requests" / owner[:2] / f"{owner}.json").write_text(
        json.dumps({"params": {"command": ["sh", "run.sh"],
                               pb.PRODUCED_OUTPUT_TEMPLATE_PARAM: declaration},
                    "inputs": [decl_input]}))
    return casdir


def _bind_live(queue: pool.PoolQueue, tmp_path: Path, template: dict,
               owner: str):
    """Bind through the real admission path with a verifiable sealed request."""

    terms = po.owner_demand_terms(template)
    casdir = _producer_request(tmp_path, owner, template)
    queue.publish(action_key=owner, cas_root=str(casdir),
                  worker_script="/w.py", checkout_root="/co",
                  resources={"cpu": 1, "mem_gb": 1, **terms},
                  produced_output_template=template)
    claimed = queue.claim(owner="fixture-worker")
    assert claimed is not None and claimed["action_key"] == owner
    control = _file_broker_control(queue, owner)
    env = {"PRISMABUILD_ACTION_KEY": owner,
           "PRISMABUILD_ACTION_NONCE": control["nonce"],
           "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
    po.declare_template(queue.root, template)
    instance = po.bind_instance(queue, template, owner_action_key=owner,
                                claim_snapshot=claimed, env=env)
    po.declare_instance(queue.root, instance)
    return {"instance": instance, "claimed": claimed, "control": control,
            "env": env}


def _write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _payload_desc(origin: Path, template: dict, instance: dict, name: str,
                  payload: bytes) -> dict:
    _write(origin / name, payload)
    return po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2,
        "slot": "boundary-0",
        "artifact_class": "payload", "path": str(origin / name),
        "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest(),
        "producer_generation": po.mint_generation(),
        "owner_action_key": instance["owner_action_key"],
        "owner_attempt": dict(instance["owner_attempt"]),
    }, template, instance)


def _move(queue: pool.PoolQueue, batch: dict, origin: Path,
          stage: Path, out_base: Path, tmp_path: Path, tag: str) -> dict:
    import stage_move

    manifest = po.build_stage_manifest(batch, str(origin))
    total = int(manifest["total_bytes"])
    manifest_path = tmp_path / f"manifest-{tag}.json"
    manifest_path.write_text(json.dumps(manifest))
    args = stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", batch["mover_key"],
        "--consumer-action-key", batch["batch_namespace"],
        "--tier-id", batch["tier"],
        "--stage-root", str(stage),
        "--manifest-sha256", batch["manifest_digest"],
        "--range-start-bytes", "0",
        "--range-end-bytes", str(total),
        "--manifest", str(manifest_path),
        "--residency-root", str(out_base),
        "--block", str(1 << 16),
        "--readers", "2",
        "--max-readers", "2",
        "--unpaced",
    ])
    receipt = stage_move.move(args)
    assert receipt["complete"] is True, receipt
    return receipt


def test_substituted_template_prewrite_refused(tmp_path: Path) -> None:
    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    # Same prefix/slots, larger durable cap the owner never bought.
    bigger = _template(str(origin), template_id="r2-v1", payload_cap=2 * CAP)
    assert po.template_sha256(bigger) != po.template_sha256(template)
    refused = po.require_prewrite(
        queue, instance, bigger, batch_id="sub", tier=STAGE_TIER,
        class_bytes={"payload": CAP + 1, "checkpoint": 0, "temp": 0},
        paths=[str(origin / "sub.pt")])
    assert refused == {"ok": False, "refusal": "template-mismatch"}


def test_stale_owner_commit_refused(tmp_path: Path) -> None:
    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    assert po.require_prewrite(
        queue, instance, template, batch_id="stale", tier=STAGE_TIER,
        class_bytes={"payload": 64, "checkpoint": 0, "temp": 0},
        paths=[str(origin / "stale.pt")])["ok"] is True
    payload = b"S" * 64
    _write(origin / "stale.pt", payload)
    desc = po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "boundary-0",
        "artifact_class": "payload", "path": str(origin / "stale.pt"),
        "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest(),
        "producer_generation": po.mint_generation(),
        "owner_action_key": instance["owner_action_key"],
        "owner_attempt": dict(instance["owner_attempt"]),
    }, template, instance)
    # A retry mints a new attempt on the same key: the old instance is
    # superseded and its commit -- quota plus token movement -- refuses.
    # The duplicate replay path still answers (it mutates nothing).
    control2 = _file_broker_control(queue, OWNER)
    assert control2["nonce"] != bound["control"]["nonce"]
    refused = po.commit_batch(queue, instance, template, [desc],
                              batch_id="stale", tier=STAGE_TIER,
                              mover_key=MOVER0)
    assert refused == {"ok": False, "refusal": "stale-superseded-owner"}


def test_bare_retire_refused(tmp_path: Path) -> None:
    # The raw-receipt mutation entrypoint is gone: retirement has exactly
    # one public path (`retire_batch`), which always drives egress.
    assert not hasattr(po, "mark_batch_retired")
    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    # ...and that path refuses unknown batches rather than filing flags.
    refused = po.retire_batch(queue, instance, template, "nope",
                              stage_root=str(tmp_path),
                              residency_root=str(tmp_path))
    assert refused == {"ok": False, "refusal": "unknown-batch"}


def test_durable_quota_survives_stage_egress(tmp_path: Path) -> None:
    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    owner = "b" * 64
    bound = _bind_live(queue, tmp_path, template, owner)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    stage = tmp_path / "stage"
    stage.mkdir()
    import stage_release

    assert stage_release.register_stage_root(
        queue, tier_id=STAGE_TIER, stage_root=str(stage)) == "registered"
    out_base = po.output_fragment_root(queue.root / pool.RESIDENCY)
    assert po.require_prewrite(
        queue, instance, template, batch_id="w0", tier=STAGE_TIER,
        class_bytes={"payload": BIG, "checkpoint": 0, "temp": 0},
        paths=[str(origin / "w0.pt")])["ok"] is True
    desc = _payload_desc(origin, template, instance, "w0.pt", b"A" * BIG)
    batch = po.commit_batch(queue, instance, template, [desc],
                            batch_id="w0", tier=STAGE_TIER, mover_key=MOVER0)
    assert batch["ok"] is True
    _move(queue, batch, origin, stage, out_base, tmp_path, "w0")
    # Egress runs while the producer is still live: the verified
    # producer hold is skipped by claimed-copy attribution, not tainted.
    retired = po.retire_batch(queue, instance, template, "w0",
                              stage_root=str(stage),
                              residency_root=str(out_base))
    assert retired["ok"] is True, retired
    assert retired["receipt"]["complete"] is True
    assert retired["staged_paths"] != []
    # Origin files remain: the retired window still charges durable quota,
    # so a second 768KiB write past the 1MiB cap refuses.
    refused = po.require_prewrite(
        queue, instance, template, batch_id="w1", tier=STAGE_TIER,
        class_bytes={"payload": BIG, "checkpoint": 0, "temp": 0},
        paths=[str(origin / "w1.pt")])
    assert refused["refusal"] == "prewrite-exceeds-payload-maxima"
    # Actual origin reclamation frees exactly once.
    assert (origin / "w0.pt").is_file()
    (origin / "w0.pt").unlink()
    claimed = po.reclaim_origin(queue, instance, template, batch_id="w0")
    assert claimed == {"ok": True, "batch_id": "w0", "reclaimed": True}
    assert po.reclaim_origin(queue, instance, template,
                             batch_id="w0") == {
        "ok": True, "batch_id": "w0", "reclaimed": False}
    assert po.require_prewrite(
        queue, instance, template, batch_id="w1", tier=STAGE_TIER,
        class_bytes={"payload": BIG, "checkpoint": 0, "temp": 0},
        paths=[str(origin / "w1.pt")])["ok"] is True


def test_damaged_batch_record_retains_quota(tmp_path: Path) -> None:
    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    bound = _bind(queue, template)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    assert po.require_prewrite(
        queue, instance, template, batch_id="dmg", tier=STAGE_TIER,
        class_bytes={"payload": 64, "checkpoint": 0, "temp": 0},
        paths=[str(origin / "dmg.pt")])["ok"] is True
    desc = _payload_desc(origin, template, instance, "dmg.pt", b"D" * 64)
    assert po.commit_batch(queue, instance, template, [desc],
                           batch_id="dmg", tier=STAGE_TIER,
                           mover_key=MOVER0)["ok"] is True
    # Corruption simulation (white-box queue-file edit, documented as
    # such): the origin file stays, only the metadata entries are
    # emptied. Quota must retain, never prove a vacuous absence. (The
    # filed record is read-only; chmod simulates disk-level damage.)
    batch_file = (queue.root / "residency" / po.OUTPUT_BATCHES_SUBDIR
                  / po.instance_namespace(instance) / "dmg.json")
    os.chmod(batch_file, 0o644)
    record = json.loads(batch_file.read_text())
    record["entries"] = []
    batch_file.write_text(json.dumps(record, sort_keys=True) + "\n")
    assert (origin / "dmg.pt").is_file()
    refused = po.reclaim_origin(queue, instance, template, batch_id="dmg")
    assert refused["ok"] is False
    assert "retain" in str(refused["refusal"])


def _commit_staged(queue: pool.PoolQueue, tmp_path: Path, template: dict,
                   instance: dict, owner: str, tag: str, size: int = 1 << 16):
    """One committed+staged batch with registered stage; returns batch/stage/base."""

    import stage_release

    stage = tmp_path / f"stage-{tag}"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=STAGE_TIER, stage_root=str(stage)) == "registered"
    out_base = po.output_fragment_root(queue.root / pool.RESIDENCY)
    origin = Path(str(po.validate_template(template)["output_prefix"]))
    name = f"{tag}.pt"
    assert po.require_prewrite(
        queue, instance, template, batch_id=tag, tier=STAGE_TIER,
        class_bytes={"payload": size, "checkpoint": 0, "temp": 0},
        paths=[str(origin / name)])["ok"] is True
    payload = bytes([len(tag) % 251 + 1]) * size
    _write(origin / name, payload)
    desc = po.validate_descriptor({
        "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "boundary-0",
        "artifact_class": "payload", "path": str(origin / name),
        "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest(),
        "producer_generation": po.mint_generation(),
        "owner_action_key": instance["owner_action_key"],
        "owner_attempt": dict(instance["owner_attempt"]),
    }, template, instance)
    batch = po.commit_batch(queue, instance, template, [desc],
                            batch_id=tag, tier=STAGE_TIER,
                            mover_key="1" * 64)
    assert batch["ok"] is True
    _move(queue, batch, origin, stage, out_base, tmp_path, tag)
    return batch, stage, out_base


def test_substituted_claim_ref_taints_egress(tmp_path: Path) -> None:
    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    owner = "b" * 64
    bound = _bind_live(queue, tmp_path, template, owner)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    batch, stage, out_base = _commit_staged(
        queue, tmp_path, template, instance, owner, "sub")
    # White-box queue-row edit (documented as tampering): the live claim's
    # projected ref names a foreign digest while the sealed request still
    # declares the admitted template. Egress must taint, not skip.
    claim_path = queue.item_path(pool.CLAIMED, owner)
    row = pool._read_json(claim_path)
    assert isinstance(row, dict)
    row["produced_output"] = dict(row["produced_output"])
    row["produced_output"]["template_sha256"] = "0" * 64
    pool._write_json_atomic(claim_path, row)
    refused = po.retire_batch(queue, instance, template, "sub",
                              stage_root=str(stage),
                              residency_root=str(out_base))
    assert refused["ok"] is False
    assert refused["refusal"] == "egress-incomplete"
    assert any("no range" in str(err) or "unreadable" in str(err)
               or "invalid range" in str(err)
               for err in refused["receipt"]["errors"])
    # Nothing destructive ran: staged bytes, fragment, and tokens stand.
    assert queue.tier_ledger(STAGE_TIER).holder_tokens(
        batch["mover_key"]) == {STAGE_BARE: 1}
    assert any((out_base / batch["batch_namespace"]).iterdir())


def test_declaration_less_request_taints_egress(tmp_path: Path) -> None:
    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    owner = "c" * 64
    bound = _bind_live(queue, tmp_path, template, owner)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True
    batch, stage, out_base = _commit_staged(
        queue, tmp_path, template, instance, owner, "nodecl")
    # The sealed request loses its declaration (legacy/handwritten row):
    # a shape-valid claim ref alone proves nothing.
    casdir = tmp_path / "cas"
    req_path = casdir / "requests" / owner[:2] / f"{owner}.json"
    req_path.write_text(json.dumps({"params": {"command": ["sh", "run.sh"]},
                                    "inputs": []}))
    refused = po.retire_batch(queue, instance, template, "nodecl",
                              stage_root=str(stage),
                              residency_root=str(out_base))
    assert refused["ok"] is False
    assert refused["refusal"] == "egress-incomplete"


def test_strict_loader_mutations_refuse_without_side_effects(
        tmp_path: Path, monkeypatch) -> None:
    import stage_release

    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)
    owner = "d" * 64
    bound = _bind_live(queue, tmp_path, template, owner)
    instance = bound["instance"]
    assert po.admit_instance(queue, instance, template)["ok"] is True

    def _fresh(tag: str, mover: str):
        assert po.require_prewrite(
            queue, instance, template, batch_id=tag, tier=STAGE_TIER,
            class_bytes={"payload": 64, "checkpoint": 0, "temp": 0},
            paths=[str(origin / f"{tag}.pt")])["ok"] is True
        desc = _payload_desc(origin, template, instance, f"{tag}.pt",
                             b"Q" * 64)
        batch = po.commit_batch(queue, instance, template, [desc],
                                batch_id=tag, tier=STAGE_TIER, mover_key=mover)
        assert batch["ok"] is True
        return batch

    def _state(batch):
        ns_dir = (po.output_fragment_root(queue.root / pool.RESIDENCY)
                  / batch["batch_namespace"])
        return (
            (origin / f"{batch['batch_id']}.pt").is_file(),
            _dir_nonempty(ns_dir),
            dict(queue.tier_ledger(STAGE_TIER).holder_tokens(
                batch["mover_key"])),
        )

    def _dir_nonempty(path: Path) -> bool:
        try:
            with os.scandir(path) as it:
                for _ in it:
                    return True
                return False
        except FileNotFoundError:
            return False

    calls: list[str] = []
    real_evict = stage_release.evict

    def _spy(q, mover, **kw):
        calls.append(mover)
        return real_evict(q, mover, **kw)

    monkeypatch.setattr(stage_release, "evict", _spy)
    monkeypatch.setattr("produced_output.stage_release.evict", _spy,
                        raising=False)

    # (a) non-mapping entry in the filed record.
    batch_a = _fresh("mut-a", "2" * 64)
    _ = _state(batch_a)
    _damage_filed(queue, instance, "mut-a",
                  lambda record: record["entries"].__setitem__(0, "scalar"))
    before = _state(batch_a)
    assert po.retire_batch(
        queue, instance, template, "mut-a",
        stage_root=str(tmp_path / "stage-mut-a"),
        residency_root=str(po.output_fragment_root(
            queue.root / pool.RESIDENCY)))["ok"] is False
    assert po.reclaim_origin(
        queue, instance, template, batch_id="mut-a")["ok"] is False
    assert _state(batch_a) == before
    assert calls == []

    # (b) boolean entry_count coerced by int() on the parent.
    batch_b = _fresh("mut-b", "3" * 64)
    _damage_filed(queue, instance, "mut-b",
                  lambda record: record.__setitem__("entry_count", True))
    before = _state(batch_b)
    assert po.retire_batch(
        queue, instance, template, "mut-b",
        stage_root=str(tmp_path / "stage-mut-b"),
        residency_root=str(po.output_fragment_root(
            queue.root / pool.RESIDENCY)))["ok"] is False
    assert po.reclaim_origin(
        queue, instance, template, batch_id="mut-b")["ok"] is False
    assert _state(batch_b) == before
    assert calls == []

    # (c) changed mover in mutable commitments: egress target must come
    # from the immutable record, so retirement refuses pre-egress.
    batch_c = _fresh("mut-c", "4" * 64)
    _damage_commitments(queue, instance, "mut-c", "mover_key", "5" * 64)
    before = _state(batch_c)
    refused = po.retire_batch(
        queue, instance, template, "mut-c",
        stage_root=str(tmp_path / "stage-mut-c"),
        residency_root=str(po.output_fragment_root(
            queue.root / pool.RESIDENCY)))
    assert refused["ok"] is False
    assert _state(batch_c) == before
    assert calls == []


def _damage_filed(queue: pool.PoolQueue, instance: dict, batch_id: str,
                  mutate) -> None:
    path = (queue.root / "residency" / po.OUTPUT_BATCHES_SUBDIR
            / po.instance_namespace(instance) / f"{batch_id}.json")
    os.chmod(path, 0o644)
    record = json.loads(path.read_text())
    mutate(record)
    path.write_text(json.dumps(record, sort_keys=True) + "\n")


def _damage_commitments(queue: pool.PoolQueue, instance: dict, batch_id: str,
                        field: str, value: object) -> None:
    path = po._commitments_path(queue.root, instance)
    record = json.loads(path.read_text())
    record["batches"][batch_id][field] = value
    path.write_text(json.dumps(record, sort_keys=True) + "\n")


@pytest.mark.skipif(not HAS_SDK, reason="needs accepted reader_lease")
def test_full_nonempty_lifecycle(tmp_path: Path, monkeypatch) -> None:
    """Sealed admission -> bound owner -> prewrite -> commit -> move ->
    downstream SDK pin (retire refuses, attribution names the path) ->
    release -> egress+retire -> durable hold -> reclaim -> contained
    finish -> safe release. All real primitives, tiny bytes."""

    import resource_broker
    from prismabuild import resource_scope

    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    queue = _queue(tmp_path)

    class _Kernel:
        def __init__(self):
            self.groups = {}

        def create(self, scope, budget):
            self.groups[scope] = {"populated": False}
            return {"cgroup_path": f"/sys/fs/cgroup/prismabuild.slice/{scope}"}

        def stop(self, scope):
            self.groups[scope]["populated"] = False

        def empty(self, scope):
            return scope not in self.groups or not self.groups[scope]["populated"]

        def exists(self, scope):
            return scope in self.groups

        def release(self, scope):
            if self.groups[scope]["populated"]:
                raise ValueError("scope still populated")
            self.groups.pop(scope)

    authority = resource_broker.Authority(
        tmp_path / "broker-state", os.getuid(), _Kernel(),
        max_memory_bytes=1024 ** 3)
    endpoint = tmp_path / "broker.sock"
    server = resource_broker.Server(str(endpoint), resource_broker.Handler)
    server.authority = authority
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01},
        daemon=True)
    thread.start()
    try:
        monkeypatch.setattr(resource_scope, "BROKER_SOCKET", endpoint)
        owner = "f" * 64
        # Admission through the real publish path with a verifiable
        # sealed request (ordinary command, no movement range): the
        # egress claimed-copy attribution verifies this live hold as a
        # producer reservation instead of tainting it as a mover copy.
        terms = po.owner_demand_terms(template)
        casdir = _producer_request(tmp_path, owner, template)
        queue.publish(action_key=owner, cas_root=str(casdir),
                      worker_script="/w.py", checkout_root="/co",
                      resources={"cpu": 1, "mem_gb": 1, **terms},
                      produced_output_template=template)
        claimed = queue.claim(owner="r2-worker")
        assert claimed is not None and claimed["action_key"] == owner
        nonce = secrets.token_hex(16)
        scope = resource_scope.ResourceScope(
            owner, nonce, 64 * 1024 ** 2, tmp_path / "telemetry.json",
            socket_path=endpoint)
        control = scope.create()
        live = pool._read_json(queue.item_path(pool.CLAIMED, owner))
        assert isinstance(live, dict)
        live["resource_scope"] = control
        pool._write_json_atomic(queue.item_path(pool.CLAIMED, owner), live)
        env = {"PRISMABUILD_ACTION_KEY": owner,
               "PRISMABUILD_ACTION_NONCE": control["nonce"],
               "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
        po.declare_template(queue.root, template)
        instance = po.bind_instance(
            queue, template, owner_action_key=owner,
            claim_snapshot=claimed, env=env)
        po.declare_instance(queue.root, instance)
        assert po.admit_instance(queue, instance, template)["ok"] is True

        stage = tmp_path / "stage"
        stage.mkdir()
        import stage_release

        assert stage_release.register_stage_root(
            queue, tier_id=STAGE_TIER, stage_root=str(stage)) == "registered"
        out_base = po.output_fragment_root(queue.root / pool.RESIDENCY)
        payload = b"W" * BIG
        assert po.require_prewrite(
            queue, instance, template, batch_id="full", tier=STAGE_TIER,
            class_bytes={"payload": BIG, "checkpoint": 0, "temp": 0},
            paths=[str(origin / "full.pt")])["ok"] is True
        desc = _payload_desc(origin, template, instance, "full.pt", payload)
        batch = po.commit_batch(queue, instance, template, [desc],
                                batch_id="full", tier=STAGE_TIER,
                                mover_key=MOVER0)
        assert batch["ok"] is True
        _move(queue, batch, origin, stage, out_base, tmp_path, "full")
        total = BIG

        # A distinct downstream consumer pins the staged window while the
        # producer is still live: retirement must refuse while the read
        # is live, and the producer hold itself must not taint the
        # egress.
        host = socket.gethostname()
        acquired = rlc.acquire(
            queue, consumer_action_key=batch["batch_namespace"],
            attempt={"nonce": secrets.token_hex(16),
                     "scope_id": "downstream-scope"},
            tier_id=STAGE_TIER, epoch="",
            span={"start_bytes": 0, "end_bytes": total},
            holder={"host": host, "worker": "downstream-1", "pid": 4242},
            acquire_token=secrets.token_hex(16),
            covers=[{"mover_action_key": MOVER0,
                     "manifest_sha256": batch["manifest_digest"]}],
            expected=None, owner_action_key=DOWNSTREAM,
            residency_root=str(out_base))
        assert acquired["ok"], acquired
        blocked = po.retire_batch(queue, instance, template, "full",
                                  stage_root=str(stage),
                                  residency_root=str(out_base))
        assert blocked == {"ok": False, "refusal": "egress-incomplete",
                           "receipt": blocked["receipt"]}
        # Attribution traces the recorded staged paths to the live census.
        live_paths, unknown = po._live_output_paths(
            queue, rlc, _recorded(queue, instance, "full"), str(out_base))
        assert unknown is None
        assert live_paths != []
        assert rlc.release(queue, acquired["pin_id"], acquired["ref_id"],
                           consumer_action_key=DOWNSTREAM,
                           residency_root=str(out_base)) is True

        retired = po.retire_batch(queue, instance, template, "full",
                                  stage_root=str(stage),
                                  residency_root=str(out_base))
        assert retired["ok"] is True, json.dumps(
            retired.get("receipt", retired), indent=1, sort_keys=True,
            default=str)
        assert retired["receipt"]["complete"] is True
        # Durable quota still held with the origin file present.
        assert po.require_prewrite(
            queue, instance, template, batch_id="full2", tier=STAGE_TIER,
            class_bytes={"payload": BIG, "checkpoint": 0, "temp": 0},
            paths=[str(origin / "full2.pt")])["refusal"] == \
            "prewrite-exceeds-payload-maxima"
        (origin / "full.pt").unlink()
        assert po.reclaim_origin(queue, instance, template,
                                 batch_id="full") == {
            "ok": True, "batch_id": "full", "reclaimed": True}
        assert po.require_prewrite(
            queue, instance, template, batch_id="full2", tier=STAGE_TIER,
            class_bytes={"payload": BIG, "checkpoint": 0, "temp": 0},
            paths=[str(origin / "full2.pt")])["ok"] is True

        # Ordinary next-prewrite work proceeds while live; the contained
        # finish then authorizes the final release.
        terminal = queue.finish(owner, status="executed", detail={})
        assert terminal == queue.item_path(pool.DONE, owner)
        proof = rlc.read_scope_attestation(queue, owner, nonce)
        assert isinstance(proof, dict) and proof["scope_empty"] is True
        released = po.safe_release_instance(
            queue, instance, template, lease_sdk=rlc)
        assert released["ok"] is True, released
        assert released["lease_proof"] == "sdk-census-clean"
    finally:
        try:
            server.shutdown()
        finally:
            thread.join(timeout=10)
            server.server_close()


def _recorded(queue: pool.PoolQueue, instance: dict, batch_id: str) -> dict:
    path = po._commitments_path(queue.root, instance)
    batches = json.loads(path.read_text())["batches"]
    return {batch_id: batches[batch_id]}

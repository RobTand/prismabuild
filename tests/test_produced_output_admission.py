"""Real produced-output template submission and tier admission (issue #734).

Drives the production hooks only: core declaration validation, pool publish
precondition + item projection, claim-time host+tier admission through the
existing ledger channel, and runtime binding from the sealed queue item.
No prewrite/commit/retire/release/funding bodies, no mover copy, no GPU.
"""
from __future__ import annotations

import hashlib
import json
import secrets
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, produced_output as po  # noqa: E402
from prismabuild import core as pb  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402

STAGE_TIER = "prismabuild-stage:dl380g10"
STAGE_BARE = "stage_gib"
OWNER = "c" * 64
OTHER = "b" * 64


def _template(output_prefix: str, template_id: str = "admit-v1") -> dict:
    body = {
        "schema": po.TEMPLATE_SCHEMA_V1,
        "version": 1,
        "template_id": template_id,
        "output_prefix": output_prefix,
        "slots": {
            "boundary-0": {"class": "payload"},
            "checkpoint-0": {"class": "checkpoint"},
        },
        "durable_maxima": {
            "payload_max_bytes": 1 << 20,
            "checkpoint_max_bytes": 1 << 20,
            "temp_max_bytes": 1 << 20,
        },
        "working_demands": {
            STAGE_TIER: {"minimum_gib": 1, "window_gib": 2},
        },
        "permitted_tiers": [STAGE_TIER],
    }
    return po.validate_template(body)


def _queue(tmp_path: Path, stage_gib: int = 8) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(STAGE_TIER, {STAGE_BARE: stage_gib})
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


def _write_template(path: Path, template: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(template, sort_keys=True,
                               separators=(",", ":")) + "\n")


def test_publish_admits_with_capacity_and_denies_without(tmp_path: Path) -> None:
    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    terms = po.owner_demand_terms(template)
    assert terms == {f"{STAGE_BARE}@{STAGE_TIER}": 2}

    # Enough capacity: publish projects the ref, claim holds host+tier.
    queue = _queue(tmp_path, stage_gib=8)
    key = "a" * 64
    row = queue.publish(action_key=key, cas_root="/cas", worker_script="/w.py",
                        checkout_root="/co",
                        resources={"cpu": 1, "mem_gb": 1, **terms},
                        produced_output_template=template)
    assert row.is_file()
    item = pool._read_json(queue.item_path(pool.READY, key))
    assert isinstance(item, dict)
    ref = item["produced_output"]
    assert ref["schema"] == po.PRODUCED_OUTPUT_REF_SCHEMA_V1
    assert ref["template_id"] == "admit-v1"
    assert ref["template_sha256"] == po.template_sha256(template)
    # Filed immutably beside the queue.
    filed = json.loads((queue.root / "residency" / po.OUTPUT_TEMPLATES_SUBDIR
                        / "admit-v1.json").read_text())
    assert po.template_sha256(filed) == ref["template_sha256"]

    claimed = queue.claim(owner="w1")
    assert claimed is not None and claimed["action_key"] == key
    assert claimed["tier_reservations"] == {STAGE_TIER: {STAGE_BARE: 2}}

    # Insufficient tier capacity denies even with ample host capacity.
    small = tmp_path / "small"
    small.mkdir()
    q2 = pool.PoolQueue(small / "pb-queue")
    q2.ensure_layout()
    q2.mint_tier_capacity(STAGE_TIER, {STAGE_BARE: 1})
    key2 = "9" * 64
    q2.publish(action_key=key2, cas_root="/cas", worker_script="/w.py",
               checkout_root="/co",
               resources={"cpu": 1, "mem_gb": 1, **terms},
               produced_output_template=_template(str(small / "out")))
    assert q2.claim(owner="w1") is None
    assert pool._read_json(q2.item_path(pool.READY, key2)) is not None


def test_input_and_output_demand_coexist(tmp_path: Path) -> None:
    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    terms = po.owner_demand_terms(template)
    queue = _queue(tmp_path, stage_gib=8)
    key = "d" * 64
    # Input residency (leads) carries no tier demand; output window does.
    # Combined publish must carry exactly the output window.
    queue.publish(
        action_key=key, cas_root="/cas", worker_script="/w.py",
        checkout_root="/co", resources={"cpu": 1, "mem_gb": 1, **terms},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                   "manifest_sha256": "e" * 64, "manifest_bytes": 10,
                   "leads": ["f" * 64]},
        produced_output_template=template)
    item = pool._read_json(queue.item_path(pool.READY, key))
    assert item["residency"]["leads"] == ["f" * 64]
    assert item["produced_output"]["template_id"] == "admit-v1"
    # Leads are absent, so the claim waits on inputs (input semantics
    # preserved) rather than admitting on tier capacity alone.
    assert queue.claim(owner="w1") is None
    assert pool._read_json(queue.item_path(pool.READY, key)) is not None


def test_refusals_and_legacy(tmp_path: Path) -> None:
    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin))
    terms = po.owner_demand_terms(template)
    queue = _queue(tmp_path)

    # Underdeclared tier demand refuses.
    with pytest.raises(pool.PoolContractError):
        queue.publish(action_key="1" * 64, cas_root="/cas",
                      worker_script="/w.py", checkout_root="/co",
                      resources={"cpu": 1, "mem_gb": 1,
                                 f"{STAGE_BARE}@{STAGE_TIER}": 1},
                      produced_output_template=template)
    # Extra qualified tier refuses.
    with pytest.raises(pool.PoolContractError):
        queue.publish(action_key="2" * 64, cas_root="/cas",
                      worker_script="/w.py", checkout_root="/co",
                      resources={"cpu": 1, "mem_gb": 1, **terms,
                                 f"{STAGE_BARE}@prismabuild-stage:other": 1},
                      produced_output_template=template)
    # Unknown template fields refuse (no side effects: no row, no filing).
    bad = dict(template)
    bad["extra"] = 1
    with pytest.raises(pool.PoolContractError):
        queue.publish(action_key="3" * 64, cas_root="/cas",
                      worker_script="/w.py", checkout_root="/co",
                      resources={"cpu": 1, "mem_gb": 1, **terms},
                      produced_output_template=bad)
    assert pool._read_json(queue.item_path(pool.READY, "3" * 64)) is None
    # Conflicting filed body refuses as foreign/tampered.
    other = _template(str(origin), template_id="admit-v1")
    other = dict(other)
    other["durable_maxima"] = dict(other["durable_maxima"])
    other["durable_maxima"]["payload_max_bytes"] = 2 << 20
    other_validated = po.validate_template(other)
    other_terms = po.owner_demand_terms(other_validated)
    with pytest.raises(pool.PoolContractError):
        queue.publish(action_key="4" * 64, cas_root="/cas",
                      worker_script="/w.py", checkout_root="/co",
                      resources={"cpu": 1, "mem_gb": 1, **other_terms},
                      produced_output_template=other_validated)
    assert pool._read_json(queue.item_path(pool.READY, "4" * 64)) is None

    # Legacy input-only mover still publishes without any produced template.
    mover = "5" * 64
    queue.publish(action_key=mover, cas_root="/cas", worker_script="/w.py",
                  checkout_root="/co",
                  resources={"cpu": 1, "mem_gb": 1,
                             f"{STAGE_BARE}@{STAGE_TIER}": 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "manifest_sha256": "a" * 64,
                             "manifest_bytes": 1 << 30,
                             "tier_id": STAGE_TIER,
                             "range_start_bytes": 0,
                             "range_end_bytes": 1 << 30})
    assert pool._read_json(queue.item_path(pool.READY, mover)) is not None
    # And the old #595 gate still refuses unbacked tier demand.
    with pytest.raises(pool.PoolContractError, match="residency block"):
        queue.publish(action_key="6" * 64, cas_root="/cas",
                      worker_script="/w.py", checkout_root="/co",
                      resources={"cpu": 1, "mem_gb": 1,
                                 f"{STAGE_BARE}@{STAGE_TIER}": 1})


def test_core_declaration_binds_input_row(tmp_path: Path) -> None:
    template = _template(str(tmp_path / "out"))
    digest = po.template_sha256(template)
    good_input = {"id": pb.PRODUCED_OUTPUT_TEMPLATE_INPUT_ID,
                  "sha256": "a" * 64, "bytes": 100}
    declaration = {"schema": pb.PRODUCED_OUTPUT_DECLARATION_SCHEMA_V1,
                   "template_id": "admit-v1", "template_sha256": digest,
                   "input": dict(good_input)}
    checked = pb.validate_produced_output_declaration(
        declaration, [dict(good_input)])
    assert checked["template_sha256"] == digest
    # Missing input row refuses (tampered seal).
    with pytest.raises(pb.ActionContractError):
        pb.validate_produced_output_declaration(declaration, [])
    # Wrong input id refuses.
    tampered = dict(declaration)
    tampered["input"] = dict(good_input)
    tampered["input"]["id"] = "pbcampaign.data-manifest"
    with pytest.raises(pb.ActionContractError):
        pb.validate_produced_output_declaration(
            tampered, [dict(tampered["input"])])


def test_sealed_key_covers_template_and_postseal_edit_changes_nothing(
        tmp_path: Path) -> None:
    from prismabuild import produced_output as produced_mod

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    tpath = tmp_path / "template.json"
    _write_template(tpath, _template(str(tmp_path / "out"), "key-a"))
    inp_a, _ = cas.ingest_input(
        str(tpath), input_id=pb.PRODUCED_OUTPUT_TEMPLATE_INPUT_ID)
    decl_a = produced_mod.build_declaration(
        _template(str(tmp_path / "out"), "key-a"), inp_a)
    _write_template(tpath, _template(str(tmp_path / "out"), "key-b"))
    inp_b, _ = cas.ingest_input(
        str(tpath), input_id=pb.PRODUCED_OUTPUT_TEMPLATE_INPUT_ID)
    decl_b = produced_mod.build_declaration(
        _template(str(tmp_path / "out"), "key-b"), inp_b)
    assert decl_a["template_sha256"] != decl_b["template_sha256"]
    assert inp_a["sha256"] != inp_b["sha256"]
    # The declaration is what the action key covers: different templates
    # seal different keys, and editing the file after seal leaves the
    # sealed input (CAS bytes) unchanged.
    assert decl_a["input"]["sha256"] == inp_a["sha256"]
    assert cas.input_path(inp_a).read_bytes() != cas.input_path(inp_b).read_bytes()


def test_runtime_binds_declared_template_not_caller_template(
        tmp_path: Path) -> None:
    origin = tmp_path / "outputs"
    origin.mkdir(parents=True)
    template = _template(str(origin), "bind-a")
    terms = po.owner_demand_terms(template)
    queue = _queue(tmp_path)
    key = "c" * 64
    queue.publish(action_key=key, cas_root="/cas", worker_script="/w.py",
                  checkout_root="/co",
                  resources={"cpu": 1, "mem_gb": 1, **terms},
                  produced_output_template=template)
    claimed = queue.claim(owner="w1")
    assert claimed is not None and claimed["action_key"] == key
    control = _file_broker_control(queue, key)
    env = {"PRISMABUILD_ACTION_KEY": key,
           "PRISMABUILD_ACTION_NONCE": control["nonce"],
           "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
    # The runtime binds from the sealed item, never from arguments.
    declared = po.declared_template(queue, key)
    assert declared["template_id"] == "bind-a"
    instance = po.bind_declared_instance(
        queue, owner_action_key=key, claim_snapshot=claimed, env=env)
    assert instance["template_id"] == "bind-a"
    assert instance["owner_action_key"] == key
    # A caller-supplied foreign template (even a filed one) is not what the
    # item declared: binding it directly would answer for the wrong scope.
    foreign = _template(str(origin), "bind-b")
    po.declare_template(queue.root, foreign)
    assert po.template_sha256(foreign) != po.template_sha256(declared)


def test_pbcampaign_row_forwards_template_option() -> None:
    import pbcampaign

    row = {"argv": ["true"], "produced_output_template": "/tmp/t.json"}
    flags = pbcampaign.pbrun_argv(row)
    assert "--produced-output-template" in flags
    assert "/tmp/t.json" in flags

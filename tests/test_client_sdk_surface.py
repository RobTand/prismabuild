"""Pin the public client SDK, ``prismabuild.client`` (#1254).

A client imports ``prismabuild.client`` and nothing else under ``prismabuild``.
This file is the contract that makes that safe: an internal refactor that
renames, re-signs or re-values anything the SDK exports fails here, and so does
an addition that does not bump ``SDK_VERSION``.

What is pinned:

- the exact set of exported names, and that ``__all__`` is that set;
- ``SDK_VERSION``;
- every callable's parameter shape (name, kind, default), not its annotations;
- every constant's value;
- that each re-export is the internal object itself, so a client gets the
  internal behaviour unchanged;
- the behaviour of the functions the SDK defines itself;
- that each capability tag the SDK advertises is backed by this tree.

Changing any of these is a contract change: bump ``SDK_VERSION`` and update
the "Client SDK" section of ``docs/design.md`` in the same change.
"""
from __future__ import annotations

import ast
import inspect
import json
import sys
from pathlib import Path

import pytest

import prismabuild.client as client
from prismabuild import (
    local_scratch,
    pool,
    produced_output,
    reader_lease,
    residency_map,
    storage_tiers,
)
from prismabuild import core as pb

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _no_outer_launch_identity(monkeypatch):
    """A test that seals its own action never inherits an outer attempt's."""

    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT"):
        monkeypatch.delenv(name, raising=False)

SDK_VERSION = 3

#: Each callable's parameters as ``[kind]name[=default]``: ``*`` keyword-only,
#: no prefix positional-or-keyword.
SIGNATURES = {
    "injected_context": "queue=None, *env=None, *residency_root=None",
    "acquire_for": "ctx, *tier_id, *epoch, *covers, *expected=None, *span, *acquire_token, *ram=None, *context=None, *residency_root=None, *material_namespace=None, *file_pin=True",
    "open_pinned": "queue, pin, ref_id, key, *residency_root=None",
    "release": "queue, pin_id, ref_id, *consumer_action_key=None, *residency_root=None, *stage_root=None",
    "covers_for_keys": "root, consumer_action_key, keys, *tier_id, *manifest_sha256, *epoch, *context=None",
    "leases_root": "queue, residency_root=None",
    "live_for": "queue, wanted, *residency_root=None, *memo=None",
    "containment_certificate_ok": "queue, certificate",
    "read_data_manifest": "path",
    "manifest_read_entries": "manifest",
    "read_claimed_record": "queue, action_key",
    "declared_template": "queue, action_key",
    "validate_template": "value",
    "bind_declared_instance": "queue, *owner_action_key, *claim_snapshot, *env=None",
    "declare_instance": "queue_root, instance",
    "admit_instance": "queue, instance, template",
    "validate_instance": "value",
    "instance_dir": "queue_root, instance",
    "checked_instance_maxima": "template",
    "owner_demand_terms": "template",
    "admit_funded_window": "queue, instance, template, *need_gib_per_tier",
    "refill_window": "queue, instance, template, *tier",
    "require_prewrite": "queue, instance, template, *batch_id, *tier, *class_bytes, *paths",
    "abort_prewrite": "queue, instance, template, *batch_id",
    "validate_descriptor": "value, template, instance",
    "output_manifest_sha256": "descriptors",
    "batch_namespace": "instance, batch_id, manifest_digest",
    "output_fragment_root": "residency_root",
    "publish_prepaid_batch": "queue, instance, template, descriptors, *batch_id, *tier, *cas_root, *producer_action_key=None, *command_extra=(), *retry_policy=None",
    "commit_batch": "queue, instance, template, descriptors, *batch_id, *tier, *mover_key",
    "commit_origin_batch": "queue, instance, template, descriptors, *batch_id, *landed=None, *lifetime='retain'",
    "retire_batch": "queue, instance, template, batch_id, *stage_root, *residency_root, *cas_root=None",
    "reclaim_origin": "queue, instance, template, *batch_id",
    "recover_batches": "queue, instance, template",
    "due_mover_rows": "queue, instance, template",
    "materialization_state": "queue, instance, template, *batch_id",
    "ensure_batch_materialized": "queue, instance, template, *batch_id, *cas_root, *producer_action_key=None, *command_extra=(), *retry_policy=None, *restage_fill=None",
    "safe_release_instance": "queue, instance, template, lease_sdk=None",
    "release_produced_instance": "queue, instance, template",
    "residency_map_key": "path, offset=0",
    "validate_residency_map": "value",
    "read_residency_map": "path",
    "read_residency_fragments": "root, consumer_action_key",
    "compose_residency_map": "fragments",
    "write_residency_map": "path, mapping",
    "cas_receipt_self_check": "receipt",
    "canonical_sha256": "value",
    "bind_ephemeral_scratch": "queue, *root_env, *name, *claim_snapshot, *env=None",
    "ephemeral_scratch_path": "declaration",
    "record_ephemeral_scratch_declarations": "queue, *claim_snapshot, *env=None",
}

CONSTANTS = {
    "SDK_VERSION": 3,
    "EPHEMERAL_SCRATCH_SCHEMA_V1": "prismabuild.ephemeral_scratch.v1",
    "SCRATCH_DECLARATION_RECORD_SCHEMA_V1": "prismabuild.scratch_declaration_record.v1",
    "READER_LEASE_TAG": "reader-lease-v1",
    "DATA_MANIFEST_MAX_BYTES": 64 * 1024 * 1024,
    "CLAIMED": "claimed",
    "RESIDENCY": "residency",
    "POOL_OUTCOME_SCHEMA_V1": "prismaquant.prismabuild.pool_outcome.v1",
    "TEMPLATE_SCHEMA_V1": "prismaquant.prismabuild.produced_output_template.v1",
    "DESCRIPTOR_SCHEMA_V2": "prismaquant.prismabuild.produced_output_descriptor.v2",
    "RESIDENCY_MAP_ENV": "PRISMABUILD_RESIDENCY_MAP",
    "RESIDENCY_MAP_SCHEMA_V1": "prismaquant.prismabuild.residency_map.v1",
    "RESIDENCY_MAP_FRAGMENT_SCHEMA_V1": "prismaquant.prismabuild.residency_map_fragment.v1",
    "RESIDENCY_LANDING_SCHEMA_V1": "prismaquant.prismabuild.residency_landing.v1",
    "LANDING_STATES": ("ready", "claimed", "unpublished", "evicted",
                       "done-not-resident", "terminal-no-receipt"),
    "CAS_RECEIPT_SCHEMA_V3": "prismaquant.prismabuild.cas_receipt.v3",
    "WORKER_ATTESTATION_SCHEMA_V2": "prismaquant.prismabuild.worker_attestation.v2",
    "RECEIPT_REFUSALS": ("cas-receipt-shape", "cas-receipt-digest",
                         "worker-attestation-digest"),
    "TIER_LOOP_LIVENESS_S": 120.0,
    "TIER_RECORD_SCHEMA": "prismabuild.storage_tier.v1",
    "DECOMPOSITION_TAG": "decomposition-v1",
    "CAPABILITIES": frozenset({"reader-lease-v1", "progress-v1", "decomposition-v1"}),
}

PATTERNS = {
    "ID_PATTERN": r"[a-z0-9][a-z0-9._/-]{0,255}\Z",
    "ENV_NAME_PATTERN": r"[A-Za-z_][A-Za-z0-9_]*\Z",
}

TYPES = {"PoolQueue": pool.PoolQueue, "ResidencyMapError": residency_map.ResidencyMapError,
         "LocalScratchError": local_scratch.LocalScratchError}

#: Names the SDK re-exports unchanged, with the internal object each must be.
REEXPORTS = {
    "EPHEMERAL_SCRATCH_SCHEMA_V1": local_scratch.EPHEMERAL_SCRATCH_SCHEMA_V1,
    "LocalScratchError": local_scratch.LocalScratchError,
    "bind_ephemeral_scratch": local_scratch.bind_ephemeral_scratch,
    "ephemeral_scratch_path": local_scratch.ephemeral_scratch_path,
    "SCRATCH_DECLARATION_RECORD_SCHEMA_V1": local_scratch.SCRATCH_DECLARATION_RECORD_SCHEMA_V1,
    "record_ephemeral_scratch_declarations": local_scratch.record_ephemeral_scratch_declarations,
    "READER_LEASE_TAG": reader_lease.READER_LEASE_TAG,
    "injected_context": reader_lease.injected_context,
    "acquire_for": reader_lease.acquire_for,
    "open_pinned": reader_lease.open_pinned,
    "release": reader_lease.release,
    "covers_for_keys": reader_lease.covers_for_keys,
    "leases_root": reader_lease.leases_root,
    "live_for": reader_lease.live_for,
    "containment_certificate_ok": reader_lease.containment_certificate_ok,
    "read_data_manifest": pb.read_data_manifest,
    "manifest_read_entries": storage_tiers.manifest_read_entries,
    "PoolQueue": pool.PoolQueue,
    "CLAIMED": pool.CLAIMED,
    "RESIDENCY": pool.RESIDENCY,
    "POOL_OUTCOME_SCHEMA_V1": pool.POOL_OUTCOME_SCHEMA_V1,
    "validate_residency_map": residency_map.validate_map,
    "read_residency_map": residency_map.read_map,
    "read_residency_fragments": residency_map.read_fragments,
    "compose_residency_map": residency_map.compose,
    "write_residency_map": residency_map.write_map,
    "residency_map_key": residency_map.residency_map_key,
    "ResidencyMapError": residency_map.ResidencyMapError,
    "ID_PATTERN": pb._ID_RE,
    "ENV_NAME_PATTERN": pb._ENV_RE,
    "canonical_sha256": pb.canonical_sha256,
    **{name: getattr(produced_output, name) for name in (
        "TEMPLATE_SCHEMA_V1", "DESCRIPTOR_SCHEMA_V2", "declared_template",
        "validate_template", "bind_declared_instance", "declare_instance",
        "admit_instance", "validate_instance", "instance_dir",
        "checked_instance_maxima", "owner_demand_terms", "admit_funded_window",
        "refill_window", "require_prewrite", "abort_prewrite",
        "validate_descriptor", "output_manifest_sha256", "batch_namespace",
        "output_fragment_root", "publish_prepaid_batch", "commit_batch",
        "commit_origin_batch", "retire_batch", "reclaim_origin",
        "recover_batches", "due_mover_rows", "materialization_state",
        "ensure_batch_materialized", "safe_release_instance")},
}


def _shape(function) -> str:
    kinds = {"POSITIONAL_OR_KEYWORD": "", "KEYWORD_ONLY": "*",
             "VAR_POSITIONAL": "**args", "VAR_KEYWORD": "**kw",
             "POSITIONAL_ONLY": "/"}
    parts = []
    for parameter in inspect.signature(function).parameters.values():
        default = ("" if parameter.default is inspect.Parameter.empty
                   else "=" + repr(parameter.default))
        parts.append(kinds[parameter.kind.name] + parameter.name + default)
    return ", ".join(parts)


def _public(module) -> set[str]:
    return {name for name, value in vars(module).items()
            if not name.startswith("_") and not inspect.ismodule(value)
            and name != "annotations" and name != "Mapping"}


def test_the_exported_names_are_exactly_the_pinned_surface():
    pinned = set(SIGNATURES) | set(CONSTANTS) | set(PATTERNS) | set(TYPES)
    assert len(client.__all__) == len(set(client.__all__)), "__all__ repeats a name"
    assert set(client.__all__) == pinned
    assert _public(client) == pinned, (
        "a public name outside __all__ is surface a client can reach unpinned")


def test_sdk_version():
    assert client.SDK_VERSION == SDK_VERSION


@pytest.mark.parametrize("name", sorted(SIGNATURES))
def test_each_callable_keeps_its_parameter_shape(name):
    assert _shape(getattr(client, name)) == SIGNATURES[name]


@pytest.mark.parametrize("name", sorted(CONSTANTS))
def test_each_constant_keeps_its_value(name):
    value = getattr(client, name)
    assert value == CONSTANTS[name] and type(value) is type(CONSTANTS[name])


def test_the_patterns_keep_their_grammar():
    for name, text in PATTERNS.items():
        assert getattr(client, name).pattern == text
    assert client.ID_PATTERN.fullmatch("tests/build-result.v1")
    assert client.ID_PATTERN.fullmatch("Upper") is None
    assert client.ID_PATTERN.fullmatch("a" * 257) is None
    assert client.ENV_NAME_PATTERN.fullmatch("PRISMABUILD_X")
    assert client.ENV_NAME_PATTERN.fullmatch("1X") is None


@pytest.mark.parametrize("name", sorted(REEXPORTS))
def test_a_reexport_is_the_internal_object_itself(name):
    assert getattr(client, name) is REEXPORTS[name]


def test_every_non_reexport_is_defined_by_the_sdk():
    """What is not a re-export is one of the few functions the SDK defines."""

    own = {"read_claimed_record", "release_produced_instance", "cas_receipt_self_check"}
    functions = set(SIGNATURES) - set(REEXPORTS)
    assert functions == own
    for name in own:
        assert getattr(client, name).__module__ == "prismabuild.client"


def test_the_liveness_bound_is_the_one_the_pool_applies():
    assert client.TIER_LOOP_LIVENESS_S == pool.OFFER_TIMEOUT_S
    assert client.TIER_RECORD_SCHEMA == storage_tiers.TIER_RECORD_SCHEMA_V1


def test_each_advertised_capability_is_backed_by_this_tree():
    assert client.READER_LEASE_TAG in client.CAPABILITIES
    assert pb.PROGRESS_TAG in client.CAPABILITIES
    assert client.DECOMPOSITION_TAG in client.CAPABILITIES
    tool = ast.parse((ROOT / "tools" / "fleet" / "pbcampaign.py").read_text())
    assert any(isinstance(node, ast.FunctionDef) and node.name == "decompose"
               for node in tool.body), "decomposition-v1 without pbcampaign.decompose"
    import prismabuild.decomposition as decomposition  # noqa: PLC0415
    assert callable(decomposition.validate_logical_request)


def test_the_sdk_imports_no_client_and_no_tool():
    tree = ast.parse((ROOT / "src" / "prismabuild" / "client.py").read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert all(alias.name.split(".")[0] not in {"tessera", "prismaquant"}
                       for alias in node.names)
        if isinstance(node, ast.ImportFrom):
            assert node.level == 1 or node.module in {"__future__", "collections.abc"}


# -- read_claimed_record -----------------------------------------------------

def test_read_claimed_record_reads_the_claimed_row(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "a" * 64
    path = queue.item_path(pool.CLAIMED, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {"action_key": key, "cas_root": "/cas", "residency": {"manifest_sha256": "b" * 64}}
    path.write_text(json.dumps(row), encoding="utf-8")
    assert client.read_claimed_record(queue, key) == row
    assert client.read_claimed_record(queue, key) == pool._read_json(path)


def test_read_claimed_record_is_none_when_not_claimed(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "queue")
    assert client.read_claimed_record(queue, "c" * 64) is None


def test_read_claimed_record_refuses_a_row_that_is_not_an_object(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "d" * 64
    path = queue.item_path(pool.CLAIMED, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[1, 2]", encoding="utf-8")
    with pytest.raises(pool.PoolContractError):
        client.read_claimed_record(queue, key)
    path.write_text("{torn", encoding="utf-8")
    with pytest.raises(pool.PoolContractError):
        client.read_claimed_record(queue, key)


# -- release_produced_instance -----------------------------------------------

def test_release_produced_instance_passes_this_trees_reader_lease(monkeypatch):
    seen = {}

    def fake(queue, instance, template, lease_sdk=None):
        seen.update(queue=queue, instance=instance, template=template, sdk=lease_sdk)
        return {"ok": True}

    monkeypatch.setattr(produced_output, "safe_release_instance", fake)
    assert client.release_produced_instance("q", {"i": 1}, {"t": 2}) == {"ok": True}
    assert seen == {"queue": "q", "instance": {"i": 1}, "template": {"t": 2},
                    "sdk": reader_lease}
    # The module is the one safe_release_instance accepts as coherent.
    sdk, refusal = produced_output._require_coherent_lease_sdk(seen["sdk"])
    assert refusal is None and sdk is reader_lease


# -- cas_receipt_self_check --------------------------------------------------

def _real_receipt(tmp_path: Path) -> dict:
    """A receipt the CAS itself published and serves."""

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task_code.py").write_text("# closure member\n", encoding="utf-8")
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "tests/client-sdk", "definition_version": "v1",
            "task_class": "generation", "determinism": "deterministic",
            "artifact_family": "generic", "artifact_kind": "generic",
            "argv": [sys.executable, "-c", "pass"], "working_directory": ".",
            "result_path": "result.bin",
        },
        "inputs": [{"id": "model/config", "sha256": "2" * 64, "bytes": 20}],
        "code_closure": pb.build_code_closure(checkout, ["task_code.py"]),
        "params": {"alpha": 1},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    output = tmp_path / "output"
    output.write_bytes(b"trusted")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    attestation = pb.preflight_action(action, cas_root=tmp_path / "cas",
                                      checkout_root=checkout)
    cas.publish_result(action, output, attestation=attestation)
    served = cas.lookup(action)
    assert served is not None
    return json.loads(cas._receipt_path(str(action["action_key"])).read_text())


def test_a_receipt_the_cas_published_passes(tmp_path: Path):
    receipt = _real_receipt(tmp_path)
    assert receipt["schema"] == client.CAS_RECEIPT_SCHEMA_V3
    assert client.cas_receipt_self_check(receipt) is None


def _redigest(receipt: dict) -> dict:
    body = {k: v for k, v in receipt.items() if k != "receipt_sha256"}
    return {**body, "receipt_sha256": pb.canonical_sha256(body)}


def test_each_refusal_is_named_in_check_order(tmp_path: Path):
    good = _real_receipt(tmp_path)
    shape, digest, attestation = client.RECEIPT_REFUSALS

    missing = {k: v for k, v in good.items() if k != "result"}
    assert client.cas_receipt_self_check(missing) == shape
    assert client.cas_receipt_self_check({**good, "extra": 1}) == shape
    assert client.cas_receipt_self_check({**good, "schema": "x.v2"}) == shape

    assert client.cas_receipt_self_check({**good, "receipt_sha256": "0" * 64}) == digest
    moved = {**good, "result": {**good["result"], "bytes": good["result"]["bytes"] + 1}}
    assert client.cas_receipt_self_check(moved) == digest

    producer = dict(good["producer"])
    for broken in ({**producer, "schema": "other.v1"},
                   {**producer, "action_key": "f" * 64},
                   {**producer, "attestation_sha256": "0" * 64},
                   {**producer, "worker_id": "someone-else"}):
        assert client.cas_receipt_self_check(_redigest({**good, "producer": broken})) \
            == attestation
    # A self-consistent producer of another action is still refused.
    other = {k: v for k, v in producer.items() if k != "attestation_sha256"}
    other["action_key"] = "f" * 64
    other["attestation_sha256"] = pb.canonical_sha256(other)
    assert client.cas_receipt_self_check(_redigest({**good, "producer": other})) \
        == attestation
    # Shape is checked before digests: a wrong schema with a bad digest says shape.
    assert client.cas_receipt_self_check(
        {**good, "schema": "x.v2", "receipt_sha256": "0" * 64}) == shape


# -- the residency-map validator is PB's own ---------------------------------

def test_validate_residency_map_accepts_and_refuses_as_pb_does(tmp_path: Path):
    stage = str(tmp_path / "stage")
    entry = {"stage_path": stage + "/a", "bytes": 4, "offset": 0, "sha256": "e" * 64}
    good = {"schema": client.RESIDENCY_MAP_SCHEMA_V1, "tier_id": "ssd",
            "stage_root": stage, "manifest_sha256": "a" * 64, "leads": ["b" * 64],
            "generation": 1, "entries": {"0:/pool/a": entry}}
    checked = client.validate_residency_map(good)
    entries = checked["entries"]
    assert isinstance(entries, dict)
    entry = entries["0:/pool/a"]
    assert isinstance(entry, dict)
    assert entry["bytes"] == 4
    with pytest.raises(client.ResidencyMapError):
        client.validate_residency_map({**good, "unknown": 1})
    with pytest.raises(client.ResidencyMapError):
        client.validate_residency_map(
            {**good, "entries": {"0:/pool/a": {**entry, "stage_path": "/elsewhere/a"}}})

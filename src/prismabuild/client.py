"""PrismaBuild's public client SDK: the one module a client imports (#1254).

A client of PrismaBuild -- any program that runs as a PrismaBuild action, or
that prepares work for one -- reaches PrismaBuild through the fleet tools
(``pbrun``, ``pbtest``, ``pbcampaign``), the progress helper
(:mod:`prismabuild.progress`), and this module.  Everything else under
``prismabuild`` is internal and may change in any release without notice.

This module is versioned.  :data:`SDK_VERSION` names the contract, and
``tests/test_client_sdk_surface.py`` pins every name below: the set of names,
each callable's signature, and each constant's value.  An internal refactor
that would change any of them fails that test; a change to this contract bumps
:data:`SDK_VERSION` and updates ``docs/design.md`` ("Client SDK") in the same
change.  An additive change (a new name) bumps the version too, so a client
can ask for what it needs by version rather than by probing attributes.

Most names are the internal objects themselves, re-exported unchanged: a
client holding ``client.acquire_for`` holds ``reader_lease.acquire_for``, with
its signature and behaviour.  The few functions defined here exist because the
behaviour a client needed had no public name (the claimed-record read, the
receipt self-check) or took an internal module as an argument
(:func:`release_produced_instance`).

The surface, by area:

* **Reader leases** (``reader-lease-v1``): :func:`injected_context`,
  :func:`acquire_for`, :func:`open_pinned`, :func:`release`,
  :func:`covers_for_keys`, :func:`leases_root`, :func:`live_for`,
  :func:`containment_certificate_ok`.
* **The sealed data manifest**: :func:`read_data_manifest`,
  :data:`DATA_MANIFEST_MAX_BYTES`, :func:`manifest_read_entries`.
* **The queue**: :class:`PoolQueue`, :data:`CLAIMED`, :data:`RESIDENCY`,
  :data:`POOL_OUTCOME_SCHEMA_V1`, and :func:`read_claimed_record`, which reads
  one action's claimed record.
* **Produced output**: the template, instance, prewrite, batch and
  materialization calls of the produced-output lane, and
  :func:`release_produced_instance`.
* **Residency maps**: :func:`validate_residency_map`,
  :func:`read_residency_map`, :func:`read_residency_fragments`,
  :func:`compose_residency_map`, :func:`write_residency_map`,
  :func:`residency_map_key`, and the schema names.
* **Ephemeral scratch naming** (SDK v2, no lifetime capability):
  :func:`bind_ephemeral_scratch`, :func:`ephemeral_scratch_path`,
  :data:`EPHEMERAL_SCRATCH_SCHEMA_V1`, :class:`LocalScratchError`.
* **Durable scratch declaration evidence** (SDK v3, not cleanup registration):
  :func:`record_ephemeral_scratch_declarations`,
  :data:`SCRATCH_DECLARATION_RECORD_SCHEMA_V1`.
* **Receipts**: :func:`cas_receipt_self_check`.
* **Identifiers and digests**: :data:`ID_PATTERN`, :data:`ENV_NAME_PATTERN`,
  :func:`canonical_sha256`.
* **Liveness**: :data:`TIER_LOOP_LIVENESS_S`, :data:`TIER_RECORD_SCHEMA`.
* **Capabilities**: :data:`CAPABILITIES`, which names
  :data:`DECOMPOSITION_TAG` when this tree's ``pbcampaign`` can decompose a
  logical request.

The module imports nothing from any client, and no client's name appears in
it.
"""

from __future__ import annotations

from collections.abc import Mapping

from . import core as _core
from . import local_scratch as _local_scratch
from . import pool as _pool
from . import produced_output as _produced_output
from . import reader_lease as _reader_lease
from . import residency_map as _residency_map
from . import storage_tiers as _storage_tiers

#: The contract version.  Bumped on any change to the names below, their
#: signatures, or the values of the constants.
SDK_VERSION = 3

# -- reader leases (capability ``reader-lease-v1``) --------------------------

READER_LEASE_TAG = _reader_lease.READER_LEASE_TAG
injected_context = _reader_lease.injected_context
acquire_for = _reader_lease.acquire_for
open_pinned = _reader_lease.open_pinned
release = _reader_lease.release
covers_for_keys = _reader_lease.covers_for_keys
leases_root = _reader_lease.leases_root
live_for = _reader_lease.live_for
containment_certificate_ok = _reader_lease.containment_certificate_ok

# -- the sealed data manifest ------------------------------------------------

DATA_MANIFEST_MAX_BYTES = _core.DATA_MANIFEST_MAX_BYTES
read_data_manifest = _core.read_data_manifest
manifest_read_entries = _storage_tiers.manifest_read_entries

# -- the queue ---------------------------------------------------------------

PoolQueue = _pool.PoolQueue
#: The queue state of an action a worker has claimed and is running.
CLAIMED = _pool.CLAIMED
#: The queue subdirectory residency maps, fragments and landing records live
#: under: ``Path(queue.root) / RESIDENCY``.
RESIDENCY = _pool.RESIDENCY
#: The schema of the terminal record the pool files when an action ends.
POOL_OUTCOME_SCHEMA_V1 = _pool.POOL_OUTCOME_SCHEMA_V1


def read_claimed_record(queue, action_key: str) -> dict[str, object] | None:
    """The claimed-queue record of ``action_key``, or ``None`` if it is not claimed.

    The record is the one PrismaBuild files when a worker claims the action.
    It carries, among other fields, the action's ``cas_root`` and its
    ``residency`` block.  An absent record is ``None``; a record that exists
    but is empty or is not a JSON object raises, so a torn write or a broken
    mount is never read as "not claimed" (#1267).
    """

    path = queue.item_path(_pool.CLAIMED, action_key)
    try:
        present = path.exists()
    except OSError:
        present = True          # a stat that cannot answer is not "absent"
    if not present:
        return None
    record = _pool._read_json(path)
    if record is None:
        try:
            if not path.exists():
                # Completed between the stat and the read: the row moved to
                # done/, which is "not claimed", not a torn write (#1271).
                return None
        except OSError:
            pass
        # ``_read_json`` answers ``None`` for absent and for empty; only the
        # absent is "not claimed".  An empty row is a torn write or a broken
        # mount holding an opinion, and the reader that asked by key holds no
        # evidence the directory is live -- the same reason ``_read_json``
        # keeps single-key callers loud.
        raise _pool.PoolContractError(
            f"claimed record is empty (torn write or broken mount?): {path}")
    return record


# -- produced output ---------------------------------------------------------

TEMPLATE_SCHEMA_V1 = _produced_output.TEMPLATE_SCHEMA_V1
DESCRIPTOR_SCHEMA_V2 = _produced_output.DESCRIPTOR_SCHEMA_V2
declared_template = _produced_output.declared_template
validate_template = _produced_output.validate_template
bind_declared_instance = _produced_output.bind_declared_instance
declare_instance = _produced_output.declare_instance
admit_instance = _produced_output.admit_instance
validate_instance = _produced_output.validate_instance
instance_dir = _produced_output.instance_dir
checked_instance_maxima = _produced_output.checked_instance_maxima
owner_demand_terms = _produced_output.owner_demand_terms
admit_funded_window = _produced_output.admit_funded_window
refill_window = _produced_output.refill_window
require_prewrite = _produced_output.require_prewrite
abort_prewrite = _produced_output.abort_prewrite
validate_descriptor = _produced_output.validate_descriptor
output_manifest_sha256 = _produced_output.output_manifest_sha256
batch_namespace = _produced_output.batch_namespace
output_fragment_root = _produced_output.output_fragment_root
publish_prepaid_batch = _produced_output.publish_prepaid_batch
commit_batch = _produced_output.commit_batch
commit_origin_batch = _produced_output.commit_origin_batch
retire_batch = _produced_output.retire_batch
reclaim_origin = _produced_output.reclaim_origin
recover_batches = _produced_output.recover_batches
due_mover_rows = _produced_output.due_mover_rows
materialization_state = _produced_output.materialization_state
ensure_batch_materialized = _produced_output.ensure_batch_materialized
safe_release_instance = _produced_output.safe_release_instance


def release_produced_instance(queue, instance, template) -> dict[str, object]:
    """Close a produced-output instance and reclaim what it still holds.

    ``safe_release_instance`` with this tree's own reader-lease module as its
    ``lease_sdk``.  That argument must be the exact module the fleet imports
    (``safe_release_instance`` refuses any other), which a client cannot name
    without importing an internal module.
    """

    return _produced_output.safe_release_instance(
        queue, instance, template, lease_sdk=_reader_lease)


# -- residency maps ----------------------------------------------------------

RESIDENCY_MAP_ENV = _residency_map.RESIDENCY_MAP_ENV
RESIDENCY_MAP_SCHEMA_V1 = _residency_map.RESIDENCY_MAP_SCHEMA_V1
RESIDENCY_MAP_FRAGMENT_SCHEMA_V1 = _residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1
RESIDENCY_LANDING_SCHEMA_V1 = _residency_map.RESIDENCY_LANDING_SCHEMA_V1
LANDING_STATES = _residency_map.LANDING_STATES
ResidencyMapError = _residency_map.ResidencyMapError
residency_map_key = _residency_map.residency_map_key
validate_residency_map = _residency_map.validate_map
read_residency_map = _residency_map.read_map
read_residency_fragments = _residency_map.read_fragments
compose_residency_map = _residency_map.compose
write_residency_map = _residency_map.write_map

# -- ephemeral scratch naming (not cleanup authority, Refs #1360) ------------

EPHEMERAL_SCRATCH_SCHEMA_V1 = _local_scratch.EPHEMERAL_SCRATCH_SCHEMA_V1
LocalScratchError = _local_scratch.LocalScratchError
bind_ephemeral_scratch = _local_scratch.bind_ephemeral_scratch
ephemeral_scratch_path = _local_scratch.ephemeral_scratch_path
SCRATCH_DECLARATION_RECORD_SCHEMA_V1 = _local_scratch.SCRATCH_DECLARATION_RECORD_SCHEMA_V1
record_ephemeral_scratch_declarations = _local_scratch.record_ephemeral_scratch_declarations

# -- receipts ----------------------------------------------------------------

CAS_RECEIPT_SCHEMA_V3 = _core.CAS_RECEIPT_SCHEMA_V3
WORKER_ATTESTATION_SCHEMA_V2 = _core.WORKER_ATTESTATION_SCHEMA_V2
_CAS_RECEIPT_KEYS = frozenset({
    "schema", "action_key", "action_manifest_sha256", "producer", "result",
    "receipt_sha256"})

#: :func:`cas_receipt_self_check` refusals, in the order they are checked.
RECEIPT_REFUSALS = ("cas-receipt-shape", "cas-receipt-digest",
                    "worker-attestation-digest")


def cas_receipt_self_check(receipt: Mapping[str, object]) -> str | None:
    """Whether a ``cas_receipt.v3`` agrees with its own digests.

    ``None`` when it does, else the first refusal of
    :data:`RECEIPT_REFUSALS`:

    1. ``cas-receipt-shape``: the receipt is not exactly the six v3 fields, or
       its schema is not :data:`CAS_RECEIPT_SCHEMA_V3`.
    2. ``cas-receipt-digest``: ``receipt_sha256`` is not the canonical digest
       of the other five fields.
    3. ``worker-attestation-digest``: the producer is not a
       :data:`WORKER_ATTESTATION_SCHEMA_V2` attestation of the same action
       whose ``attestation_sha256`` is the canonical digest of its other
       fields.

    This is the check a reader holding only the receipt can make.  It does not
    re-derive the attestation from the sealed action, as the CAS does when it
    serves a receipt, so it proves the receipt is the one its digests name,
    not that the action it names is the one the reader expects: a reader
    binds that itself (the action key, the inputs, the result digest).
    """

    if set(receipt) != _CAS_RECEIPT_KEYS or receipt["schema"] != CAS_RECEIPT_SCHEMA_V3:
        return RECEIPT_REFUSALS[0]
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    if _core.canonical_sha256(body) != receipt["receipt_sha256"]:
        return RECEIPT_REFUSALS[1]
    producer = receipt["producer"]
    # A producer that is not an object (null, a list, a string) is the
    # attestation refusal, not an AttributeError: the receipt's digests may
    # be intact over a body whose producer field is corrupt, and a reader
    # holding only the receipt gets the named refusal either way (#1267).
    if not isinstance(producer, dict) or not (
            producer.get("schema") == WORKER_ATTESTATION_SCHEMA_V2
            and producer.get("action_key") == receipt["action_key"]
            and _core.canonical_sha256(
                {key: value for key, value in producer.items()
                 if key != "attestation_sha256"})
            == producer.get("attestation_sha256")):
        return RECEIPT_REFUSALS[2]
    return None


# -- identifiers and digests -------------------------------------------------

#: The grammar of every identifier that reaches an action key (task ids,
#: demand keys, tags): ``[a-z0-9][a-z0-9._/-]{0,255}``, matched whole.
ID_PATTERN = _core._ID_RE
#: The grammar of an environment variable name an action may set.
ENV_NAME_PATTERN = _core._ENV_RE
#: The digest of a value's canonical JSON (sorted keys, compact separators,
#: UTF-8, no NaN), without a trailing newline.  The identity digest every
#: PrismaBuild record uses.
canonical_sha256 = _core.canonical_sha256

# -- liveness ----------------------------------------------------------------

#: How long a tier loop may go without re-announcing a tier before the pool
#: reads the loop as dead.  A landing record carries the same bound as its
#: ``tier_loop_liveness_s`` field; this is the value the pool applies.
TIER_LOOP_LIVENESS_S = _pool.OFFER_TIMEOUT_S
#: The schema of the per-tier record the tier loop announces.
TIER_RECORD_SCHEMA = _storage_tiers.TIER_RECORD_SCHEMA_V1

# -- capabilities ------------------------------------------------------------

#: This tree can decompose a logical request into sealed child actions
#: (``pbcampaign`` ``decompose``, #517/#518).
DECOMPOSITION_TAG = "decomposition-v1"
#: What this tree supports, by tag.  A client asks for a capability by tag
#: rather than by probing files or attributes.
CAPABILITIES = frozenset({
    READER_LEASE_TAG,
    _core.PROGRESS_TAG,
    DECOMPOSITION_TAG,
})

__all__ = [
    "SDK_VERSION",
    # reader leases
    "READER_LEASE_TAG", "injected_context", "acquire_for", "open_pinned",
    "release", "covers_for_keys", "leases_root", "live_for",
    "containment_certificate_ok",
    # the sealed data manifest
    "DATA_MANIFEST_MAX_BYTES", "read_data_manifest", "manifest_read_entries",
    # the queue
    "PoolQueue", "CLAIMED", "RESIDENCY", "POOL_OUTCOME_SCHEMA_V1",
    "read_claimed_record",
    # produced output
    "TEMPLATE_SCHEMA_V1", "DESCRIPTOR_SCHEMA_V2", "declared_template",
    "validate_template", "bind_declared_instance", "declare_instance",
    "admit_instance", "validate_instance", "instance_dir",
    "checked_instance_maxima", "owner_demand_terms", "admit_funded_window",
    "refill_window", "require_prewrite", "abort_prewrite",
    "validate_descriptor", "output_manifest_sha256", "batch_namespace",
    "output_fragment_root", "publish_prepaid_batch", "commit_batch",
    "commit_origin_batch", "retire_batch", "reclaim_origin",
    "recover_batches", "due_mover_rows", "materialization_state",
    "ensure_batch_materialized", "safe_release_instance",
    "release_produced_instance",
    # residency maps
    "RESIDENCY_MAP_ENV", "RESIDENCY_MAP_SCHEMA_V1",
    "RESIDENCY_MAP_FRAGMENT_SCHEMA_V1", "RESIDENCY_LANDING_SCHEMA_V1",
    "LANDING_STATES", "ResidencyMapError", "residency_map_key",
    "validate_residency_map", "read_residency_map",
    "read_residency_fragments", "compose_residency_map",
    "write_residency_map",
    # ephemeral scratch naming (no lifetime capability)
    "EPHEMERAL_SCRATCH_SCHEMA_V1", "LocalScratchError",
    "bind_ephemeral_scratch", "ephemeral_scratch_path",
    "SCRATCH_DECLARATION_RECORD_SCHEMA_V1", "record_ephemeral_scratch_declarations",
    # receipts
    "CAS_RECEIPT_SCHEMA_V3", "WORKER_ATTESTATION_SCHEMA_V2",
    "RECEIPT_REFUSALS", "cas_receipt_self_check",
    # identifiers and digests
    "ID_PATTERN", "ENV_NAME_PATTERN", "canonical_sha256",
    # liveness
    "TIER_LOOP_LIVENESS_S", "TIER_RECORD_SCHEMA",
    # capabilities
    "DECOMPOSITION_TAG", "CAPABILITIES",
]

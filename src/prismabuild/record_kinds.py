"""Wire ownership metadata. Production adapters are selected only by qualification."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RecordKind:
    name: str
    wire_id: str
    location: str
    field_policy: str
    reader: str
    writers: tuple[str, ...]
    variants: tuple[str, ...]
    legacy: tuple[str, ...] = ()
    closed_sets: tuple[str, ...] = ()
    negative_field: str = "schema"
    legacy_writers: tuple[str, ...] = ()


KINDS = (
    RecordKind("fragment", "prismaquant.prismabuild.residency_map_fragment.v1",
               "residency/<consumer>/<mover>.json", "closed",
               "prismabuild.residency_map:read_fragments",
               ("prismabuild.residency_map:write_fragment",), ("ssd", "ram"),
               ("SSD fragments omit epoch",),
               ("prismabuild.residency_map:_FRAGMENT_KEYS", "prismabuild.residency_map:_ENTRY_KEYS")),
    RecordKind("map", "prismaquant.prismabuild.residency_map.v1",
               "residency/<consumer>.map.json", "closed",
               "prismabuild.residency_map:read_map", ("prismabuild.residency_map:write_map",),
               ("ssd", "ram"), ("SSD maps omit the RAM overlay header",),
               ("prismabuild.residency_map:_MAP_KEYS", "prismabuild.residency_map:_ENTRY_KEYS")),
    RecordKind("landing", "prismaquant.prismabuild.residency_landing.v1",
               "residency/<consumer>.landing.json", "closed",
               "prismabuild.residency_map:read_landing", ("prismabuild.residency_map:write_landing",),
               ("ready", "claimed", "unpublished", "evicted", "done-not-resident",
                "terminal-no-receipt", "horizon", "null-horizon", "reported", "claim", "queue", "held"),
               ("horizon may be absent or null", "pre-1018 readers are history controls only"),
               ("prismabuild.residency_map:_LANDING_KEYS", "prismabuild.residency_map:_LANDING_RANGE_KEYS",
                "prismabuild.residency_map:_LANDING_HORIZON_KEYS")),
    RecordKind("material", "prismaquant.prismabuild.reader_material.v1",
               "residency/<consumer>/<mover>.material.json", "closed",
               "prismabuild.reader_lease:read_material", ("prismabuild.reader_lease:write_material",),
               ("ssd", "empty-epoch", "ram"), ("SSD material permits absent or empty epoch",)),
    RecordKind("retiring", "prismaquant.prismabuild.reader_retiring.v2",
               "residency/<consumer>/<mover>.retiring.json", "closed",
               "prismabuild.reader_lease:retiring_for", ("prismabuild.reader_lease:write_retiring",),
               ("generation",), ("v1 never establishes generation-bound retirement",)),
    RecordKind("move", "prismaquant.prismabuild.pool_move.v1", "movers/<key>.json", "additive",
               "prismabuild.pool:PoolQueue.move_record", ("prismabuild.pool:PoolQueue.record_move",),
               ("implicit", "explicit"), ("missing schema at write time becomes pool_move.v1",)),
    RecordKind("egress", "prismaquant.prismabuild.pool_egress.v1", "movers/<key>.json", "additive",
               "prismabuild.pool:PoolQueue.move_record", ("prismabuild.pool:PoolQueue.record_move",),
               ("complete",), ("misstamped pre-1158 egress is never egress pricing evidence",)),
    RecordKind("prewarm", "prismaquant.prismabuild.pool_prewarm.v1", "prewarm/<key>.json", "additive",
               "prismabuild.pool:PoolQueue.prewarm", ("prismabuild.pool:PoolQueue.record_prewarm",),
               ("plain", "stage"), ("legacy claims may retain the inline receipt",)),
    RecordKind("export", "prismaquant.prismabuild.pool_export.v1", "exports/<key>.json", "additive",
               "prismabuild.pool:PoolQueue.export_records", ("prismabuild.pool:PoolQueue.record_export",),
               ("measured",)),
    RecordKind("staged-wait", "prismabuild.staged_wait.v1", "claimed/<key>.progress.staged-wait", "additive",
               "prismabuild.pool:read_staged_wait", ("prismabuild.progress:declare_staged_wait",),
               ("explicit-time",)),
    RecordKind("export-wait", "prismabuild.export_wait.v1", "claimed/<key>.progress.export-wait", "additive",
               "prismabuild.pool:read_export_wait", ("prismabuild.progress:declare_export_wait",),
               ("explicit-time",)),
    RecordKind("progress", "prismabuild.action_progress.v1", "claimed/<key>.progress", "additive",
               "prismabuild.pool:PoolQueue.mover_report",
               ("prismabuild.progress:commit", "prismabuild.core:report_action_progress"),
               ("helper", "helper-unit", "core", "core-unit")),
    RecordKind("dc-roster", "prismabuild.logical_task_roster.v1", "logical request roster", "closed",
               "prismabuild.decomposition:validate_roster", ("prismabuild.decomposition:write_document",), ("ordered",)),
    RecordKind("dc-policy", "prismabuild.roster_batch_policy.v1", "logical request batch_policy", "closed",
               "prismabuild.decomposition:validate_batch_policy", ("prismabuild.decomposition:write_document",), ("priced",)),
    RecordKind("dc-request", "prismabuild.logical_request.v1", "campaign request.json", "closed",
               "prismabuild.decomposition:validate_logical_request", ("prismabuild.decomposition:write_document",), ("plain",)),
    RecordKind("dc-frozen", "prismabuild.frozen_common.v1", "logical parent identity frozen_common", "closed",
               "prismabuild.decomposition:validate_frozen_common", ("prismabuild.decomposition:freeze_common",), ("snapshot",)),
    RecordKind("dc-plan", "prismabuild.decomposition_plan.v1", "cas/decompositions/<xx>/<parent>/plan.json", "closed",
               "prismabuild.decomposition:_validate_plan", ("prismabuild.decomposition:write_document",), ("partitioned",),
               ("retained reader is named validate_plan",)),
    RecordKind("dc-publication", "prismabuild.decomposition_publication.v1", "cas/decompositions/<xx>/<parent>/publication.json", "closed",
               "prismabuild.decomposition:_validate_publication_index", ("prismabuild.decomposition:publish_children",), ("children",),
               ("retained reader is named validate_publication_index",), legacy_writers=("pbcampaign:publish_index",)),
    RecordKind("dc-child-result", "prismabuild.child_result_manifest.v1", "child declared result manifest", "closed",
               "prismabuild.decomposition:_validate_child_result_manifest", ("prismabuild.decomposition:write_document",), ("cover",),
               ("retained reader is named validate_child_result_manifest",)),
    RecordKind("dc-task-data", "prismabuild.task_data_manifest.v1", "logical request task_data_manifest", "closed",
               "prismabuild.decomposition:validate_task_data_policy", ("prismabuild.decomposition:write_document",), ("plain", "reader")),
    RecordKind("q-funding", "prismabuild.tier_funding.v1", "tier-funding/<mover>.<tier>.funding.json", "closed",
               "prismabuild.pool:PoolQueue.read_funding", ("prismabuild.pool:PoolQueue.write_funding",),
               ("reserved", "transferring", "consumed", "released")),
    RecordKind("q-output-funding", "prismabuild.tier_funding.output.v1", "tier-funding output binding", "closed",
               "prismabuild.pool:PoolQueue.read_output_funding", ("prismabuild.pool:PoolQueue.write_output_funding",),
               ("reserved", "transferring", "consumed", "released")),
    RecordKind("q-denials", "prismabuild.claim_denials.v1", "host adaptive/claim-denials.json", "additive",
               "prismabuild.pool:PoolQueue.latest_denials", ("prismabuild.pool:PoolQueue.record_denial",),
               ("first", "repeat"), ("reader treats wrapper schema as diagnostic, not authority",), negative_field="records"),
    RecordKind("q-denial-transitions", "prismabuild.denial_transitions.v1", "denial-transitions/<key>.json", "additive",
               "prismabuild.pool:PoolQueue.denial_transitions", ("prismabuild.pool:PoolQueue.record_denial",),
               ("first", "repeat"), ("reader treats wrapper schema as diagnostic, not authority",), negative_field="transitions"),
    RecordKind("core-action", "prismaquant.prismabuild.action.v2", "cas/requests/<xx>/<key>.json", "closed",
               "prismabuild.core:PrismaBuildCAS.read_action_request", ("prismabuild.core:PrismaBuildCAS.publish_action_request",),
               ("portable",), ("action.v1 is retired and rejected",)),
    RecordKind("core-closure", "prismaquant.prismabuild.code_closure.v1", "action.code_closure", "closed",
               "prismabuild.core:verify_code_closure", ("prismabuild.core:build_code_closure",), ("single", "multiple")),
    RecordKind("core-manifest-v1", "prismaquant.prismabuild.data_manifest.v1", "declared CAS data-manifest input", "closed",
               "prismabuild.core:validate_data_manifest", ("prismabuild.core:PrismaBuildCAS.ingest_bytes",), ("unphased", "phased"),
               ("v1 phase annotations may be absent",)),
    RecordKind("core-manifest-v2", "prismaquant.prismabuild.data_manifest.v2", "declared CAS data-manifest input", "closed",
               "prismabuild.core:validate_data_manifest", ("prismabuild.core:PrismaBuildCAS.ingest_bytes",), ("read-plan",)),
    RecordKind("edge-deferred", "prismabuild.deferred_submission.v1", "deferred/<pending>.json", "closed",
               "prismabuild.action_edges:read_deferred", ("prismabuild.action_edges:file_deferred",), ("plain", "optional"),
               ("optional publication options may be absent",)),
    RecordKind("edge-release", "prismabuild.deferred_release.v1", "deferred-releases/<pending>.json", "additive",
               "prismabuild.action_edges:read_release", ("prismabuild.action_edges:file_release",), ("pinned",)),
    RecordKind("edge-published", "prismabuild.deferred_release_published.v1", "deferred-releases/<pending>.published.json", "additive",
               "prismabuild.action_edges:read_published", ("prismabuild.action_edges:file_published",), ("published",)),
    RecordKind("edge-supersession", "prismabuild.supersession.v1", "supersessions/<old>.json", "additive",
               "prismabuild.action_edges:read_supersession", ("prismabuild.action_edges:file_supersession",), ("key", "pending")),
    RecordKind("out-template", "prismaquant.prismabuild.produced_output_template.v1", "produced-output/templates/<id>.json", "closed",
               "prismabuild.produced_output:validate_template", ("prismabuild.produced_output:declare_template",),
               ("read-back", "read-back-rate", "write-only", "write-only-rate"),
               ("write_only absent means false; export_rate_family may be absent",)),
    RecordKind("out-instance", "prismaquant.prismabuild.produced_output_instance.v1", "produced-output/<owner>/<template>/<nonce>/instance.json", "closed",
               "prismabuild.produced_output:validate_instance", ("prismabuild.produced_output:declare_instance",), ("bound",)),
    RecordKind("out-declaration", "prismabuild.produced_output_declaration.v1", "action.params.produced_output_template", "closed",
               "prismabuild.core:validate_produced_output_declaration", ("prismabuild.produced_output:build_declaration",), ("sealed",)),
)


def cases():
    return tuple(f"{kind.name}/{variant}" for kind in KINDS for variant in kind.variants)


def wire_id(name):
    return next(kind.wire_id for kind in KINDS if kind.name == name)

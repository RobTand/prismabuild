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
    RecordKind("move", "prismaquant.prismabuild.pool_move.v1", "moves/<key>.json", "additive",
               "prismabuild.pool:PoolQueue.move_record", ("prismabuild.pool:PoolQueue.record_move",),
               ("implicit", "explicit"), ("missing schema at write time becomes pool_move.v1",)),
    RecordKind("egress", "prismaquant.prismabuild.pool_egress.v1", "moves/<key>.json", "additive",
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
)


def cases():
    return tuple(f"{kind.name}/{variant}" for kind in KINDS for variant in kind.variants)

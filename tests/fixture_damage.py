"""Named damaged relationships and records, kept apart from valid setup writers."""
from prismabuild import pool, residency_map
import pytest


class Damage:
    def __init__(self, queue):
        self.queue = queue

    def malformed_output_funding(self, mover, tier):
        """Violate JSON syntax on an existing production-funded mover record."""
        path = self.queue.records.funding_output_path(mover, tier)
        assert pool.read_queue_record(path) is not None
        path.write_text("{corrupt", encoding="utf-8")
        with pytest.raises(pool.PoolContractError):
            pool.read_queue_record(path)

    def fragment_without_move_receipt(self, *, tier, stage, consumer, mover, names, size):
        """Schema-valid fragment names files without its paired mover receipt.

        The publication and reconciliation consumers must refuse replacement and
        retain this evidence. Schema validation alone cannot detect this defect.
        """
        assert self.queue.move_record(mover) is None, "damage requires a missing receipt"
        return residency_map.write_fragment(self.queue.root / pool.record_store.RESIDENCY, {
            "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
            "consumer_action_key": consumer, "mover_action_key": mover,
            "tier_id": tier, "stage_root": str(stage), "manifest_sha256": "a" * 64,
            "entries": {
                residency_map.residency_map_key(str(stage / name), 0): {
                    "stage_path": str(stage / name), "bytes": size,
                    "sha256": "b" * 64, "offset": 0,
                } for name in names
            },
        })

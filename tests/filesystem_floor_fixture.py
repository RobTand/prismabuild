"""Synthetic authority for isolated test queues, never fleet activation proof."""
import time

from prismabuild import filesystem_floor as floor


def closed_coverage(binding, **changes):
    coverage = dict(version="test-coverage-v1", mapping_version="test-mapping-v1",
        population_version="test-population-v1", bounds_version="test-bounds-v1",
        configuration_version="test-config-v1", writer_fence="test-owner-protocol",
        authority="isolated test fixture", evidence=["synthetic test evidence"],
        scope_sha256=floor.coverage_identity(binding),
        valid_from_unix=0, valid_until_unix=time.time() + 3600,
        bytes_per_token={floor.member_id(m["ledger_root"], m["ledger"], m["kind"]):
                         floor.BYTE_KINDS[m["kind"]] for m in binding["members"]},
        additional_bytes=0)
    return {**coverage, **changes}

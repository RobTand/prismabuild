"""Malformed manifests remain incomplete report evidence."""
import json

from test_stage_ownership_bounds import setup, manifest_move
from prismabuild import core as pb, data_manifest
from tools.fleet import stage_reclaim


def test_invalid_read_plan_keeps_observed_payload_unknown(tmp_path):
    queue, stage = setup(tmp_path)
    record, relative = manifest_move(queue, stage)
    target = stage / relative
    target.parent.mkdir(parents=True)
    target.write_bytes(b'x' * 11)

    movement = json.loads(record.read_text())
    cas = pb.PrismaBuildCAS(queue.root.parent / 'cas')
    document = json.loads(cas.blob_path(movement['manifest_sha256']).read_bytes())
    document['schema'] = data_manifest.DATA_MANIFEST_SCHEMA_V2
    document['read_plan'] = dict(
        phases=[dict(name='load', entry_indices=[1], bytes=11, cumulative_bytes=11)],
        read_bytes=11)
    raw = json.dumps(document, sort_keys=True).encode()
    digest = pb.new_sha256(raw).hexdigest()
    blob = cas.blob_path(digest)
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.write_bytes(raw)
    movement['manifest_sha256'] = digest
    record.write_text(json.dumps(movement))

    got = stage_reclaim.ownership_report(queue, tier_id='test-tier', stage_root=stage)

    assert not got['complete']
    assert got['totals']['observed']['apparent_bytes'] == 11
    assert got['totals']['physical_bytes'] == dict(
        owned=0, source_only=0, unattributed=0, unknown=11)
    refusal = next(item for item in got['unresolved_manifests']
                   if item['record_path'] == str(record)
                   and item['manifest_sha256'] == digest)
    assert 'references an absent entry' in refusal['reason']

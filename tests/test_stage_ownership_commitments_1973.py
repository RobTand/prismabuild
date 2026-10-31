"""The bounded report shares reclaim's batch-commitment validation."""
import json

import pytest

from test_stage_ownership_bounds import setup
from tools.fleet import stage_reclaim


@pytest.mark.parametrize('batches, complete', [({}, True), ([], False)])
def test_report_validates_schema_less_commitments(tmp_path, batches, complete):
    queue, stage = setup(tmp_path)
    path = (queue.root / 'residency' / 'produced-output-scopes' / ('a' * 64)
            / ('template.with.dots.' + 'b' * 32) / 'commitments.json')
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({'batches': batches}))

    got = stage_reclaim.ownership_report(queue, tier_id='test-tier', stage_root=stage)

    assert got['complete'] is complete
    assert any(item['record_path'] == str(path) for item in got['errors']) is not complete

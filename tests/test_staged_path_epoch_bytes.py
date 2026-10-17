"""Preserve a8 material optional epoch bytes across real trim/shrink operations."""
from pathlib import Path
import json
import os
import subprocess
import sys
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools/maintenance'))
from staged_path_epoch_replay import epoch_cases


@pytest.fixture(scope='module')
def prune_epoch_wire_report(tmp_path_factory):
    output = tmp_path_factory.mktemp('epoch-wire') / 'report.json'
    subprocess.run([sys.executable, str(ROOT / 'tools/maintenance/staged_path_epoch_replay.py'),
                    '--output', str(output), '--observe'], cwd=ROOT, env=dict(os.environ), check=True)
    return json.loads(output.read_bytes())


@pytest.mark.parametrize('case', epoch_cases())
def test_the_original_prune_epoch_bytes_and_effect_order(case, prune_epoch_wire_report):
    row = next(r for r in prune_epoch_wire_report['cases'] if r['case']==list(case))
    assert row['old']['before'] == row['current']['before']
    assert row['old']['after'] == row['current']['after'], row
    assert row['old']['effects'] == row['current']['effects'], row
    assert row['old']['receipt'] == row['current']['receipt'], row

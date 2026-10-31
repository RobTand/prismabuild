"""Replay reports incomplete estimates without converting missing evidence to zero."""
import importlib.util
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools/ci'))
SPEC = importlib.util.spec_from_file_location('gate_replay', ROOT / 'tools/ci/gate_replay.py')
replay = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(replay)


def test_replay_uses_nearest_rank_and_keeps_unknown_cost_unknown():
    rows = [{'number': 1, 'selected_files': 24, 'wall_seconds': 10},
            {'number': 2, 'selected_files': 100, 'wall_seconds': 70},
            {'number': 3, 'selected_files': 200, 'wall_seconds': 120},
            {'number': 4, 'selected_files': 900, 'wall_seconds': None}]
    summary = replay.summarize(rows)
    assert summary['selected_files'] == {'p50': 100, 'p90': 900}
    assert summary['wall_seconds'] == {'p50': None, 'p90': None}
    assert summary['unknown_cost_prs'] == [4]
    rows[-1]['wall_seconds'] = 300
    assert replay.summarize(rows)['wall_seconds'] == {'p50': 70, 'p90': 300}

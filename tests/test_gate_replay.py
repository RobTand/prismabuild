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


def test_replay_reports_large_selection_distribution():
    rows = [{'number': n, 'selected_files': count, 'wall_seconds': 1}
            for n, count in enumerate([24, 300, 301, 600, 601, 900])]
    assert replay.summarize(rows)['distribution'] == {'over_300_files': 4, 'over_600_files': 2}


def test_function_diff_ignores_formatting_and_counts_added_removed_and_methods():
    before = 'def unchanged(): return 1\ndef gone(): return 0\nclass Pool:\n def claim(self): return 1\n'
    after = 'def unchanged():\n return 1\nclass Pool:\n def claim(self): return 2\nasync def added(): return 3\n'
    result = replay.changed_functions(before, after)
    assert result['changed'] == ['Pool.claim']
    assert result['added'] == ['added'] and result['removed'] == ['gone']
    assert result['count'] == 3

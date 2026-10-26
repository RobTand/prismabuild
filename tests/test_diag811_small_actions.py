"""Check diagnostic egress guards against the mover's byte-range layout."""
import importlib.util
import json
from pathlib import Path
import sys

import pytest


@pytest.fixture
def egress_case(tmp_path, monkeypatch):
    source = (Path(__file__).resolve().parents[1] / 'tools' / 'maintenance'
              / 'diag_811_small_actions.py')
    spec = importlib.util.spec_from_file_location('diag811_small_actions_test', source)
    harness = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(harness)
    root = tmp_path / 'fixture'
    runtime = tmp_path / 'runtime'
    harness.prepare(root, runtime)
    landed = []
    for index in range(4):
        path = root / 'stage' / f'part-{index}.bin.pbrange' / '0-2089'
        path.parent.mkdir()
        path.write_bytes(bytes([index + 1]) * 2089)
        landed.append(path)
    monkeypatch.setattr(sys, 'argv', [])
    return harness, root, landed


def test_egress_accepts_all_landed_ranges(egress_case, monkeypatch, capsys):
    harness, root, landed = egress_case

    def release(path, *, run_name):
        for entry in landed:
            entry.unlink()
        harness.write_json(root / 'egress-receipt.json',
                           {'complete': True, 'bytes_deleted': 8356})

    monkeypatch.setattr(harness.runpy, 'run_path', release)
    harness.run_payload('egress', root, None)
    assert json.loads(capsys.readouterr().out)['verified'] is True
    assert not any(path.exists() for path in landed)


def test_egress_rejects_a_missing_range_before_launch(egress_case, monkeypatch):
    harness, root, landed = egress_case
    landed[0].unlink()

    def unexpected_launch(*args, **kwargs):
        pytest.fail('egress launched without the complete fixture')

    monkeypatch.setattr(harness.runpy, 'run_path', unexpected_launch)
    with pytest.raises(SystemExit, match='egress needs all four landed fixture entries'):
        harness.run_payload('egress', root, None)


def test_egress_rejects_a_retained_range_despite_a_success_receipt(egress_case, monkeypatch):
    harness, root, landed = egress_case

    def incomplete_release(path, *, run_name):
        for entry in landed[1:]:
            entry.unlink()
        harness.write_json(root / 'egress-receipt.json',
                           {'complete': True, 'bytes_deleted': 8356})

    monkeypatch.setattr(harness.runpy, 'run_path', incomplete_release)
    with pytest.raises(SystemExit, match='egress left fixture bytes behind'):
        harness.run_payload('egress', root, None)

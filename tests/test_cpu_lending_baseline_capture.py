"""The main-era observer preserves claims and durably identifies their loans."""
import importlib.util
import json
from pathlib import Path

from prismabuild import adaptive_cpu, pool

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('cpu_baseline_observer',
    ROOT / 'tools/maintenance/cpu_lending_baseline_worker.py')
OBSERVER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(OBSERVER)


def test_main_baseline_records_exact_owned_and_borrowed_ids(tmp_path):
    tiers = {'preferred': [5, 7, 9, 11], 'fallback': [13, 15]}
    ledger = pool.ResourceLedger(tmp_path / 'reservations', 'baseline-host')
    ledger.ensure_capacity({'cpu': 6})
    ledger.configure_cpu_tiers(tiers)
    key = 'b' * 64
    handle = ledger.begin_acquire(key, {'cpu': 2})
    assert handle and ledger.commit_acquire(key, handle)
    allocation = {'preferred': [5, 7, 9], 'fallback': []}
    adaptive_cpu.write_json(ledger.held_dir / key / adaptive_cpu.METADATA,
                           {'allocation': allocation, 'borrowed_cpu': 1})
    row = {'action_key': key, 'published_unix': 100., 'claimed_by': 'baseline-owner',
           'claimed_unix': 101., 'claimed_host': 'baseline-host', 'attempts': 0,
           'cpu_allocation': allocation}
    before = dict(row)
    output = OBSERVER.capture_main_cpu_attempt(row, ledger, tiers, tmp_path / 'baseline')
    assert row == before
    value = json.loads(output.read_bytes())
    assert value['assigned_cpus'] == [5, 7, 9]
    assert value['physical_cpus'] == [5, 7] and value['borrowed_cpus'] == [9]
    assert value['attempt'] == 1 and value['source_commit'] == '43234d0fbd42d6ed76da38658cb9f447ed19a606'
    raw = output.read_bytes()
    assert OBSERVER.capture_main_cpu_attempt(row, ledger, tiers, tmp_path / 'baseline') == output
    assert output.read_bytes() == raw
    ledger.release(key)
    assert json.loads(output.read_bytes()) == value


def test_main_baseline_excludes_funded_allowance_ids(tmp_path):
    tiers = {'preferred': [0, 1, 2], 'fallback': []}
    ledger = pool.ResourceLedger(tmp_path / 'reservations', 'baseline-host')
    ledger.ensure_capacity({'cpu': 3})
    ledger.configure_cpu_tiers(tiers)
    key = 'b' * 64
    handle = ledger.begin_acquire(key, {'cpu': 1})
    assert handle and ledger.commit_acquire(key, handle)
    allocation = {'preferred': [0, 1], 'fallback': []}
    adaptive_cpu.write_json(ledger.held_dir / key / adaptive_cpu.METADATA,
                           {'allocation': allocation, 'funded_by': 'a' * 64})
    row = {'action_key': key, 'attempts': 2, 'cpu_allocation': allocation}
    output = OBSERVER.capture_main_cpu_attempt(row, ledger, tiers, tmp_path / 'baseline')
    assert json.loads(output.read_bytes())['borrowed_cpus'] == []

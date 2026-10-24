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


def test_main_baseline_records_unknown_affinity_instead_of_silently_omitting_attempt(tmp_path):
    tiers = {'preferred': [0], 'fallback': []}
    ledger = pool.ResourceLedger(tmp_path / 'reservations', 'baseline-host')
    ledger.ensure_capacity({'cpu': 1, 'mem_gb': 1})
    ledger.configure_cpu_tiers(tiers)
    key = 'b' * 64
    handle = ledger.begin_acquire(key, {'mem_gb': 1})
    assert handle and ledger.commit_acquire(key, handle)
    output = OBSERVER.capture_main_cpu_attempt({'action_key': key}, ledger, tiers, tmp_path / 'baseline')
    value = json.loads(output.read_bytes())
    assert value['assigned_cpus'] is None and value['borrowed_cpus'] is None
    assert value['physical_cpus'] == []


def test_installed_observer_reads_the_positional_scan_context(tmp_path, monkeypatch, capsys):
    from types import SimpleNamespace
    from prismabuild import digest_primitives
    expected = {'pool.py': 'ac9f3a70e4d2fe9b6135be18cd9c3224b8da5a9c0bdedd25f5c6ea5f774762f7',
                'adaptive_cpu.py': 'dadf5d5c4575bdd18e5cc8723da7d3e5c35f50a0bd8d64a2726c99c719fa10e6'}
    monkeypatch.setattr(digest_primitives, 'stream_sha256', lambda path: expected[path.name])
    row = {'action_key': 'a' * 64}
    monkeypatch.setattr(pool.PoolQueue, '_build_claim_record', lambda *args: row)
    original = pool.PoolQueue._build_claim_record
    observed = []
    monkeypatch.setattr(OBSERVER, 'capture_main_cpu_attempt', lambda *args: observed.append(args))
    ledger, tiers = object(), {'preferred': [0], 'fallback': []}
    context = SimpleNamespace(ledger=ledger, cpu_tiers=tiers)
    OBSERVER.install_main_cpu_baseline_observer(tmp_path)
    assert pool.PoolQueue._build_claim_record(object(), object(), context) is row
    assert observed == [(row, ledger, tiers, tmp_path)]
    def failed(*args):
        raise OSError('injected unavailable capture store')
    monkeypatch.setattr(OBSERVER, 'capture_main_cpu_attempt', failed)
    assert pool.PoolQueue._build_claim_record(object(), object(), context) is row
    assert 'CPU_BASELINE_CAPTURE_FAILED' in capsys.readouterr().err
    monkeypatch.setattr(pool.PoolQueue, '_build_claim_record', original)

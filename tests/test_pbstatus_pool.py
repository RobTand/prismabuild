"""Pool status uses the active queue even without SLURM executables."""
import json
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools/fleet"))
from prismabuild import pool
import pbstatus


@pytest.fixture
def live_pool(tmp_path, monkeypatch):
    monkeypatch.setenv("PRISMABUILD_TRANSPORT", "pool")
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    for host, key, demand in [
        ("cpu-box", "a" * 64, {"cpu": 4, "mem_gb": 8}),
        ("gpu-box", "b" * 64, {"cpu": 2, "gpu": 1, "mem_gb": 16}),
    ]:
        monkeypatch.setattr(pool.socket, "gethostname", lambda host=host: host)
        queue.announce(host=host, tags=[host], has_gpu="gpu" in demand,
                       capacity=demand, observed_capacity=demand)
        queue.publish(action_key=key, cas_root=tmp_path / "cas",
                      checkout_root=tmp_path, worker_script=ROOT / "tools/prismabuild_worker.py",
                      resources=demand, tags=[host], needs_gpu="gpu" in demand)
        assert queue.claim(tags=[host], has_gpu="gpu" in demand, owner=host)
    return queue


def test_default_pool_status_lists_claimed_cpu_and_gpu_without_slurm(live_pool, monkeypatch, capsys):
    def absent(*args, **kwargs):
        raise pbstatus.SchedulerUnavailable("SLURM must not be queried for pool status")
    monkeypatch.setattr(pbstatus, "_scheduler_output", absent)
    assert pbstatus.main(["--json", "--recent", "0", "--queue-root", str(live_pool.root)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert {job["action_key"] for job in result["jobs"]} == {"a" * 64, "b" * 64}
    assert {node["node"] for node in result["nodes"]} == {"cpu-box", "gpu-box"}
    assert result["transport"] == "pool"
    assert result["pool"]["ready"] == 0 and result["pool"]["claimed"] == 2
    assert not any("SLURM" in note for note in result["scheduler"])


def test_ready_pool_jobs_report_placement_and_admission_passes(live_pool, tmp_path):
    key = "c" * 64
    live_pool.publish(action_key=key, cas_root=tmp_path / "cas", checkout_root="/mnt/shared/status-fixture",
                      worker_script=ROOT / "tools/prismabuild_worker.py",
                      resources={"cpu": 2, "mem_gb": 4}, tags=["cpu-box"])
    live_pool.record_pass(key)
    result = pbstatus.read_pool(live_pool.root)
    row = next(j for j in result['jobs'] if j['action_key'] == key)
    assert row['state'] == 'READY' and row['placeable_hosts'] == ['cpu-box']
    assert row['admission_passes'] == 1 and row['admission_wait_s'] >= 0
    assert 'awaiting admission' in row['reason']


def test_stale_offer_and_lease_are_retained_as_uncertain(live_pool):
    offer = live_pool.root / pool.WORKERS / 'gpu-box.json'
    record = json.loads(offer.read_text())
    record['announced_unix'] = time.time() - pool.OFFER_TIMEOUT_S - 1
    offer.write_text(json.dumps(record))
    lease = live_pool.lease_path('b' * 64)
    record = json.loads(lease.read_text())
    record['heartbeat_unix'] = time.time() - pool.LEASE_TIMEOUT_S - 1
    lease.write_text(json.dumps(record))
    result = pbstatus.read_pool(live_pool.root)
    node = next(n for n in result['nodes'] if n['node'] == 'gpu-box')
    job = next(j for j in result['jobs'] if j['action_key'] == 'b' * 64)
    assert node['state'] == 'stale' and not node['healthy']
    assert job['stale'] and 'liveness unknown' in job['reason']
    assert result['queue']['claimed'] == 2 and result['queue']['live_workers'] == 1


def test_empty_pool_is_distinct_from_missing_or_unreadable_state(tmp_path, monkeypatch):
    root = tmp_path / 'empty'
    queue = pool.PoolQueue(root)
    queue.ensure_layout()
    assert pbstatus.read_pool(root)['queue']['empty'] is True
    missing = tmp_path / 'missing'
    result = pbstatus.read_pool(missing)
    assert result['queue']['empty'] is None and result['queue']['ready'] is None
    assert not missing.exists()
    original = pbstatus.os.scandir
    def denied(path):
        if Path(path) == queue.dir(pool.CLAIMED):
            raise PermissionError('permission denied')
        return original(path)
    monkeypatch.setattr(pbstatus.os, 'scandir', denied)
    result = pbstatus.read_pool(root)
    assert result['queue']['empty'] is None and result['queue']['claimed'] is None
    assert any('permission denied' in note for note in result['notes'])


def test_malformed_active_records_remain_visible_without_hiding_valid_jobs(live_pool):
    live_pool.item_path(pool.READY, 'c' * 64).write_text('{')
    result = pbstatus.read_pool(live_pool.root)
    assert result['queue']['ready'] is None and result['queue']['empty'] is None
    assert len(result['jobs']) == 3
    assert next(j for j in result['jobs'] if j['action_key'] == 'c' * 64)['state'] == 'UNREADABLE'


def test_pool_status_reads_shared_admission_evidence_without_writing(live_pool):
    base = live_pool.ledger('gpu-box').base / 'adaptive'
    base.mkdir(parents=True)
    sample = {'sampled_unix': time.time() - 30, 'low_samples': 2}
    (base / 'gpu-state.json').write_text(json.dumps(sample))
    paths = [p for p in live_pool.root.rglob('*') if p.is_file()]
    before = {str(p): (p.read_bytes(), p.stat().st_mtime_ns) for p in paths}
    result = pbstatus.read_pool(live_pool.root)
    node = next(n for n in result['nodes'] if n['node'] == 'gpu-box')
    assert node['admission']['gpu']['state'] == 'stale'
    assert node['admission']['gpu']['record']['low_samples'] == 2
    after = {str(p): (p.read_bytes(), p.stat().st_mtime_ns)
             for p in live_pool.root.rglob('*') if p.is_file()}
    assert after == before


def test_explicit_transport_overrides_environment(live_pool, monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(pbstatus, 'read_nodes', lambda **kw: (calls.append('nodes') or [], None))
    monkeypatch.setattr(pbstatus, 'read_jobs', lambda **kw: calls.append('jobs') or [])
    pbstatus.main(['--transport', 'slurm', '--json', '--queue-root', str(live_pool.root)])
    result = json.loads(capsys.readouterr().out)
    assert calls == ['nodes', 'jobs'] and result['transport'] == 'slurm'
    monkeypatch.setenv('PRISMABUILD_TRANSPORT', 'slurm')
    pbstatus.main(['--transport', 'pool', '--json', '--queue-root', str(live_pool.root)])
    assert json.loads(capsys.readouterr().out)['transport'] == 'pool'
    assert calls == ['nodes', 'jobs']


def test_deployed_transport_default_is_used(tmp_path, monkeypatch, capsys):
    import fleet_submit
    monkeypatch.delenv('PRISMABUILD_TRANSPORT', raising=False)
    (tmp_path / 'RUNTIME_VERSION.json').write_text(json.dumps({'default_transport': 'slurm'}))
    monkeypatch.setattr(fleet_submit, 'RUNTIME_ROOT', tmp_path)
    monkeypatch.setattr(pbstatus, 'read_nodes', lambda **kw: ([], None))
    monkeypatch.setattr(pbstatus, 'read_jobs', lambda **kw: [])
    pbstatus.main(['--json', '--queue-root', str(tmp_path / 'queue')])
    assert json.loads(capsys.readouterr().out)['transport'] == 'slurm'


def test_pool_text_names_jobs_and_unavailable_state(live_pool, capsys, tmp_path):
    pbstatus.main(['--transport', 'pool', '--queue-root', str(live_pool.root), '--recent', '0'])
    output = capsys.readouterr().out
    assert 'cpu-box' in output and 'gpu-box' in output
    assert 'aaaaaaaaaaaa' in output and 'bbbbbbbbbbbb' in output
    assert 'CLAIMED' in output and 'SLURM' not in output
    pbstatus.main(['--transport', 'pool', '--queue-root', str(tmp_path / 'missing'), '--recent', '0'])
    output = capsys.readouterr().out
    assert 'pool job state unavailable' in output
    assert 'no jobs ready or claimed' not in output


def test_bad_worker_offer_does_not_hide_other_workers_or_jobs(live_pool):
    path = live_pool.root / pool.WORKERS / 'cpu-box.json'
    record = json.loads(path.read_text())
    record['capacity']['cpu'] = 'unknown'
    path.write_text(json.dumps(record))
    result = pbstatus.read_pool(live_pool.root)
    assert next(n for n in result['nodes'] if n['node'] == 'cpu-box')['state'] == 'unreadable'
    assert next(n for n in result['nodes'] if n['node'] == 'gpu-box')['healthy']
    assert len(result['jobs']) == 2


def test_nonregular_queue_entry_is_unreadable_without_opening(live_pool):
    import os
    os.mkfifo(live_pool.item_path(pool.READY, 'c' * 64))
    result = pbstatus.read_pool(live_pool.root)
    assert result['queue']['ready'] is None
    assert any('not a regular' in note for note in result['notes'])

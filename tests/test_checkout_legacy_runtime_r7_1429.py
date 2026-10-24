"""Regenerate actual e9 records in an admitted child and reclaim absent holds."""
import io
import json
import socket
import subprocess
import sys
import tarfile
import types
from pathlib import Path

from prismabuild import legacy_custody, pool, storage_tiers
from prismabuild.legacy_custody import LegacyCustodyMigration
from test_checkout_review_r7_1429 import E9, _stopped_fleet, _host_roots
from test_lifetime_contract_1429 import Skew

ROOT = Path(__file__).resolve().parents[1]


def test_real_e9_terminal_host_tier_and_mirror_have_a_certified_return(tmp_path, monkeypatch):
    archive = subprocess.run(['git', 'archive', E9, 'src', 'tests'], cwd=ROOT,
                             check=True, capture_output=True).stdout
    legacy = tmp_path / 'e9'
    legacy.mkdir()
    with tarfile.open(fileobj=io.BytesIO(archive)) as source:
        source.extractall(legacy, filter='data')
    world = tmp_path / 'world'
    world.mkdir()
    run = subprocess.run([sys.executable, '-I', str(ROOT / 'tests/legacy_runtime_fixture_1429.py'),
                          str(legacy), str(world)], capture_output=True, text=True, timeout=900)
    assert run.returncode == 0, run.stderr[-1000:]
    report = json.loads(run.stdout.splitlines()[-1])
    assert not report['errors'], {key: value[:160] for key, value in report['errors'].items()}
    assert report['done']['done'] and report['done']['detail_returncode'] == 0
    assert not report['done']['top_returncode'] and not report['done']['top_container_cleanup']
    queue = pool.PoolQueue(Path(report['queue_root']))
    host = socket.gethostname()
    _stopped_fleet(monkeypatch, [host], local_aliases={'storage': host})
    root, covered = _host_roots(tmp_path, host)
    assert not (world / 'materialized/cleanup-failures').exists(), 'actual e9 lazily creates failures'
    covered['roots'] = [{'path': str(world / 'materialized'), 'kind': 'checkout'},
                        {'path': str(world / 'materialized/cleanup-failures'), 'kind': 'cleanup-failures'}]
    migration = LegacyCustodyMigration(queue, roots={'runtime': E9, 'hosts': {host: covered}})
    inventory = migration.inventory()
    for row in inventory['holds']:
        if row['kind'] == 'tier' or row['holder'].startswith(storage_tiers.RAM_HOST_MEMORY_PREFIX):
            empty = tmp_path / 'occupancy' / row['kind'] / row['ledger'] / row['holder']
            empty.mkdir(parents=True)
            covered['occupancy_roots'][row['kind'] + ':' + row['ledger'] + ':' + row['holder']] = [str(empty)]
    clock = Skew(monkeypatch)
    clock.to(pool._now() + 3 * pool.LEASE_TIMEOUT_S)
    monkeypatch.setattr(legacy_custody, "time", types.SimpleNamespace(time=clock.now))
    rows = migration.apply(inventory, writers_stopped=True)
    done_rows = [row for row in rows if row['holder'].removeprefix(storage_tiers.RAM_HOST_MEMORY_PREFIX) == 'd' * 64]
    assert len(done_rows) == 3 and all(row.get('certified_absent') for row in done_rows), [(row['kind'], row['ledger'], row['status'], row['reason'][:120]) for row in done_rows]
    assert all(row['status'] == 'retained' for row in rows
               if row['holder'].removeprefix(storage_tiers.RAM_HOST_MEMORY_PREFIX) == 'b' * 64)
    result = queue.reclaim_terminal_reservation('d' * 64, unpin=True)
    assert result['released'] == 2  # Host plus tier; mirror repair is not counted twice.
    assert not queue.ledger().holder_tokens('d' * 64)
    assert not queue.tier('ram:storage').holdings('d' * 64)
    assert not queue.ledger('storage').holder_tokens(storage_tiers.RAM_HOST_MEMORY_PREFIX + 'd' * 64)

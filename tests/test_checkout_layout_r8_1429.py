"""Lazy e9 roots and the broker-first deployment boundary."""
from pathlib import Path
import pytest
from prismabuild import legacy_custody_host


def test_code_defined_cleanup_failures_can_be_absent(tmp_path):
    root = tmp_path / 'checkouts'
    root.mkdir()
    proof = legacy_custody_host.legacy_namespace_scan('a' * 64, [
        {'path': str(root), 'kind': 'checkout'},
        {'path': str(root / 'cleanup-failures'), 'kind': 'cleanup-failures'}])
    assert not proof['matches']
    lazy = proof['roots'][1]
    assert lazy['absent'] is True and lazy['parent_inode'] == root.stat().st_ino


@pytest.mark.parametrize('kind,name', [('checkout', 'missing'), ('quarantine', 'missing'),
    ('occupancy', 'missing'), ('cleanup-failures', 'cleanup-faliures')])
def test_every_other_missing_root_refuses(tmp_path, kind, name):
    with pytest.raises((OSError, ValueError)):
        legacy_custody_host.legacy_namespace_scan('a' * 64, [{'path': str(tmp_path / name), 'kind': kind}])


def test_deploy_publishes_broker_generation_before_custody_generation():
    guide = (Path(__file__).resolve().parents[1] / 'docs/operating_prismabuild.md').read_text().split(
        '## Deploying the checkout custody contract (#1429)', 1)[1]
    assert 'broker-only generation' in guide
    assert 'updater reverts' in guide
    assert 'held' in guide and 'maintenance_query' in guide
    assert guide.index('broker-only generation') < guide.index('1. Pause producers')


def test_all_thirty_dl380_census_holds_adopt_without_lazy_child(tmp_path, monkeypatch):
    import types
    from prismabuild import legacy_custody, legacy_custody_host
    from prismabuild.legacy_custody import LegacyCustodyMigration
    from test_checkout_review_r7_1429 import _census_world, _apply_on_owner
    queue, _, roots, _, _ = _census_world(tmp_path, monkeypatch, mixed=False)
    failure = Path(roots['hosts']['dl380g10']['roots'][1]['path'])
    failure.rmdir()
    monkeypatch.setattr(legacy_custody_host.socket, 'gethostname', lambda: 'dl380g10')
    migration = LegacyCustodyMigration(queue, roots=roots)
    inventory = migration.inventory()
    inventory['holds'] = [r for r in inventory['holds'] if r['ledger'] == 'prismabuild-stage:dl380g10']
    assert len(inventory['holds']) == 30
    rows = _apply_on_owner(migration, inventory, monkeypatch)
    assert all(r.get('certified_absent') for r in rows), [r['reason'] for r in rows if not r.get('certified_absent')]
    proofs = migration.status_rows()
    absent = [r for r in proofs if r['status'] == 'certified absent']
    assert len(absent) == 30
    for row in absent:
        lazy = next(r for r in row['proof']['scan']['roots'] if r['kind'] == 'cleanup-failures')
        assert lazy['absent'] and lazy['parent_inode'] == failure.parent.stat().st_ino
    print('dl380 layout fixture: 30 certified absent, 0 retained; lazy child absent')


@pytest.mark.live_store(reason='PB lead r8 requires a read-only copy of dl380 checkout layout and its 30 legacy holds')
def test_copy_dl380_actual_layout_and_certify_thirty_fixture_holds(tmp_path, monkeypatch):
    import json
    import os
    import re
    import shutil
    import socket
    import stat
    from prismabuild import materialize, pool, legacy_custody
    from prismabuild.legacy_custody import LegacyCustodyMigration
    from test_checkout_review_r7_1429 import _e9_done, _stopped_fleet, _pre_contract, _apply_on_owner, E9
    host = socket.gethostname()
    if host != 'dl380g10':
        pytest.skip('requires dl380g10 owner filesystem; covered by targeted x86 lane')
    actual = Path(materialize.DEFAULT_LOCAL_CHECKOUT_ROOT)
    copied = tmp_path / 'copied-checkouts'
    copied.mkdir()
    with materialize._custody_open(actual, directory=True) as parent:
        names = os.listdir(parent)
        # Copy only entry names and types, never checkout contents or credentials.
        for name in names:
            info = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                (copied / name).mkdir()
            elif stat.S_ISREG(info.st_mode):
                (copied / name).touch()
            else:
                raise AssertionError('real checkout root has an unexpected entry type')
        assert 'cleanup-failures' not in names, 'real-layout assumption changed; review coverage before adopting'
    live = Path('/mnt/shared/prismabuild-fleet/pb-queue/tier-reservations/prismabuild-stage:dl380g10')
    with materialize._custody_open(live / 'held', directory=True) as parent:
        keys = [name for name in os.listdir(parent) if re.fullmatch('[0-9a-f]{64}', name)
                and not (live / 'release-owners' / (name + '.json')).exists()]
    assert len(keys) == 30, 'real pre-contract custody census changed'
    queue = pool.PoolQueue(tmp_path / 'fixture-queue')
    queue.records.ensure_layout()
    _stopped_fleet(monkeypatch, [host])
    tier_name = 'prismabuild-stage:' + host
    tier = queue.tier(tier_name)
    tier.grow_supply({'stage_gib': len(keys)})
    occupancy = {}
    for key in keys:
        _e9_done(queue, key, host, tier=True)
        assert tier.take(key, {'stage_gib': 1})
        _pre_contract(tier._ledger, key)
        empty = tmp_path / 'fixture-occupancy' / key
        empty.mkdir(parents=True)
        occupancy['tier:' + tier_name + ':' + key] = [str(empty)]
    roots = {'runtime': E9, 'hosts': {host: {'coverage_evidence': 'descriptor copy of dl380g10 actual checkout layout; isolated occupancy fixture',
        'all_historical_roots': True, 'roots': [
            {'path': str(copied), 'kind': 'checkout'},
            {'path': str(copied / 'cleanup-failures'), 'kind': 'cleanup-failures'}], 'occupancy_roots': occupancy}}}
    migration = LegacyCustodyMigration(queue, roots=roots)
    rows = _apply_on_owner(migration, migration.inventory(), monkeypatch)
    assert len(rows) == 30 and all(r.get('certified_absent') for r in rows), [r['reason'] for r in rows]
    assert all(tier.holdings(key) for key in keys), 'fixture adoption returns no tokens'
    print('read-only dl380 projection: 30 real hold names; copied namespace; 30 certified, 0 retained; live custody untouched')

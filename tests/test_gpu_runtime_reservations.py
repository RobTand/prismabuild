"""A fenced host suspends pending GPU elections while keeping running authority."""
from pathlib import Path
from types import SimpleNamespace

import pytest

from prismabuild import _gang, _measurement_reservation as reservation
from prismabuild import contracts, core, gpu_capacity, gpu_runtime_gate, movement_actions, pool
from test_gang_reservation_1517 import CAPACITY, HOSTS, TIERS, _busy_both, gang_fleet
from test_measurement_drains_gpu_backfill import cpu_sample, fleet, gpu_sample

__all__ = ['fleet', 'gang_fleet']


def _publish_readiness(queue, tmp_path):
    token = 'e' * 64
    params = gpu_capacity.runtime_probe_request(token)
    env = params.pop('environment')
    params['placement'] = {'required_tags': [HOSTS[0]]}
    prefix = str(Path(__file__).resolve().parents[1] / 'tools/fleet')
    env['PATH'] = prefix + ':/usr/local/bin:/usr/bin:/bin'
    action = core.seal_action({
        'schema': core.ACTION_SCHEMA_V2,
        'task': {'definition_id': 'fleet/pbrun', 'definition_version': 'v1',
                 'task_class': 'generation', 'determinism': 'deterministic',
                 'artifact_family': 'generic', 'artifact_kind': 'generic',
                 'argv': movement_actions.standard_capture_argv(
                     params['command'], 'readiness.log', path_prefix=prefix),
                 'working_directory': '.', 'result_path': 'readiness.log'},
        'inputs': [], 'code_closure': core.build_code_closure(tmp_path / 'checkout', ['task.py']),
        'params': params, 'environment': {'variables': env, 'toolchain': {}},
        'execution_scope': {'portability': 'portable', 'platform_key': None, 'host_class': None}})
    cas = core.PrismaBuildCAS(tmp_path / 'cas')
    cas.publish_action_request(action)
    key = action['action_key']
    queue.publish(action_key=key, cas_root=str(cas.root), checkout_root=str(tmp_path / 'checkout'),
                  worker_script='worker.py', resources=params['demand'], needs_gpu=True,
                  tags=params['placement']['required_tags'], priority=0,
                  container_images=params['container_images'])
    row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, key))
    assert gpu_runtime_gate.runtime_probe_identity(row) == token
    return key


def _pending_election(gang_fleet, kind, priority):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    if kind == 'measurement':
        holder = publish('incumbent', timeout_s=1200)
        assert gclaim(HOSTS[0]) == holder
        key = publish('measurement', measurement=True, priority=priority, cpu=8, gpu=1, mem_gb=48)
        assert gclaim(HOSTS[0]) is None
        selected = reservation.selection(reservation._read(queue.records.passes_path(key)))
        assert selected['host'] == HOSTS[0]
        finish(holder, HOSTS[0])
    elif kind == 'gang':
        holders = _busy_both(publish, gclaim)
        _, (key, _) = members('gang', priority=priority)
        assert gclaim(HOSTS[0]) is None
        for host, holder in holders.items():
            finish(holder, host)
    else:
        key = None
    assert queue.ledger(HOSTS[0]).held_keys() == []
    return key


@pytest.mark.parametrize('kind,priority', [('measurement', 5), ('gang', 5), ('gang', 0), ('none', 0)])
@pytest.mark.parametrize('age', [0, pool.WITHHOLD_CEILING_S + 1, 1000])
@pytest.mark.parametrize('work', ['readiness', 'cpu'])
def test_fenced_pending_elections_allow_recovery_and_cpu(
        gang_fleet, fleet, tmp_path, monkeypatch, kind, priority, age, work):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    elected = _pending_election(gang_fleet, kind, priority)
    _, _, readings, sample, _, tick, _, _ = fleet
    gate = {'latched': True, 'reason': gpu_capacity.RUNTIME_REASON, 'subtype': 'cdi_mount_missing'}
    monkeypatch.setattr(gpu_runtime_gate, 'runtime_gate_state', lambda: gate if gate['latched'] else None)
    monkeypatch.setattr(pool.socket, 'gethostname', lambda: HOSTS[0])
    tick(age)
    target = (_publish_readiness(queue, tmp_path) if work == 'readiness' else
              publish('cpu-continuation', priority=0, cpu=1, gpu=0, mem_gb=1, tags=[HOSTS[0]]))

    def claim_target():
        tick(0.01)
        row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, target))
        return queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
                           has_gpu=True, tags=['gb10', HOSTS[0], _gang.TAG, contracts.CONTAINER_IMAGE_TAG],
                           ready=[row],
                           observed_images=[gpu_capacity.RUNTIME_PROBE_IMAGE],
                           cpu_sampler=lambda: cpu_sample(clock, readings),
                           gpu_sampler=lambda: gpu_sample(sample, readings))

    claimed = claim_target()
    assert claimed is not None, denial(target)
    assert claimed['action_key'] == target
    if elected is not None and (priority == 5 or (
            kind == 'gang' and age > reservation.GANG_RESERVE_AFTER_S and work == 'readiness')):
        finish(target, HOSTS[0])
        queue.publish(**{name: claimed[name] for name in (
            'action_key', 'cas_root', 'checkout_root', 'worker_script', 'resources',
            'needs_gpu', 'tags', 'priority')}, recompute=True,
            container_images=claimed.get('container_images'))
        gate['latched'] = False
        assert claim_target() is None
        assert denial(target)['reason'] == 'deferred_for_' + kind + '_reservation'


@pytest.mark.parametrize('kind', ['measurement', 'gang'])
@pytest.mark.parametrize('evidence', ['pending', 'held', 'claimed', 'missing', 'cpu', 'malformed', 'other_host', 'read_error'])
def test_runtime_election_view_preserves_running_and_ambiguous_authority(monkeypatch, kind, evidence):
    key = 'a' * 64
    chosen = {'action_key': key, 'host': HOSTS[0], 'published_unix': 1.0, 'priority': 5,
              'member_state': 'ready', 'demand': {'cpu': 1, 'mem_gb': 1, 'gpu': 1}}
    row = {'action_key': key, 'published_unix': 1.0, 'resources': dict(chosen['demand'])}
    field = 'elections' if kind == 'measurement' else 'gang_elections'
    census = {'elections': {}, 'gang_elections': {}, 'measurements': {key: [row]}, 'opportunities': {}}
    census[field][key] = chosen
    held = []
    if evidence == 'held': held.append(key)
    if evidence == 'claimed':
        chosen['member_state'] = 'claimed'
        row['claimed_host'] = HOSTS[0]
    if evidence == 'missing':
        chosen.pop('member_state')
        census['measurements'].clear()
    if evidence == 'cpu':
        chosen['demand'].pop('gpu')
        row['resources'].pop('gpu')
    if evidence == 'malformed':
        chosen['demand'] = row['resources'] = {'gpu': '1'}
    if evidence == 'other_host': chosen['host'] = HOSTS[1]

    def held_keys():
        if evidence == 'read_error': raise OSError('unavailable ledger')
        return held

    ledger = SimpleNamespace(base=Path(HOSTS[0]), held_keys=held_keys)
    monkeypatch.setattr(gpu_runtime_gate, 'runtime_gate_state', lambda: {'latched': True})
    view = reservation.runtime_reservation_census(census, ledger)
    assert bool(view[field]) is (evidence != 'pending')
    assert census[field][key] == chosen
    monkeypatch.setattr(gpu_runtime_gate, 'runtime_gate_state', lambda: None)
    assert reservation.runtime_reservation_census(census, ledger) is census


@pytest.mark.parametrize('kind', ['measurement', 'gang'])
def test_runtime_fence_keeps_real_running_election_and_tokens(gang_fleet, monkeypatch, kind):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    elected = _pending_election(gang_fleet, kind, 5)
    if kind == 'gang':
        assert gclaim(HOSTS[0]) is None
        assert gclaim(HOSTS[1]) is not None
    assert gclaim(HOSTS[0]) == elected
    held = queue.ledger(HOSTS[0]).holder_tokens(elected)
    monkeypatch.setattr(gpu_runtime_gate, 'runtime_gate_state', lambda: {'latched': True})
    target = publish('cpu-behind-running-election', priority=0, cpu=1, gpu=0, mem_gb=1, tags=[HOSTS[0]])
    assert gclaim(HOSTS[0]) is None
    assert denial(target)['reason'] == 'deferred_for_' + kind + '_reservation'
    assert queue.ledger(HOSTS[0]).held_keys() == [elected]
    assert queue.ledger(HOSTS[0]).holder_tokens(elected) == held

"""Refused opted rows cannot change the state or verdict of legacy admission."""
from pathlib import Path

import pytest

from prismabuild import adaptive_cpu, pool


def host_sample(at, busy=0.):
    return {'sampled_unix': at, 'cpu_count': 8, 'interval_s': 1.,
            'busy_cpus': busy, 'psi_some': 0., 'psi_avg10': 0., 'psi_avg60': 0.,
            'per_cpu_busy': {str(cpu): busy / 8 for cpu in range(8)}}


def test_refused_opted_row_cannot_replace_legacy_pass_sample(tmp_path, monkeypatch):
    clock = [100.]
    monkeypatch.setattr(adaptive_cpu.time, 'time', lambda: clock[0])
    monkeypatch.setattr(adaptive_cpu, '_sealed_request',
                        lambda item: {'task': {'task_class': 'generation'}, 'params': {}})
    outcomes = []
    for interleave in (False, True):
        clock[0] = 100.
        tiers = {'preferred': list(range(4)), 'fallback': list(range(4, 8))}
        ledger = pool.ResourceLedger(tmp_path / str(interleave), 'sample-host')
        ledger.ensure_capacity({'cpu': 8})
        ledger.configure_cpu_tiers(tiers)
        controller = adaptive_cpu.Controller(ledger, tiers, ramp_cap=2,
            sampler=lambda: host_sample(clock[0], 8. if clock[0] == 100. else 0.))
        legacy = {'action_key': 'a' * 64, 'resources': {'cpu': 1}}
        with controller.locked():
            assert controller.decision(legacy, legacy['resources'], identity=('legacy', False), owner=None) is None
            clock[0] = 101.2
            if interleave:
                opted = {'action_key': 'b' * 64, 'resources': {'cpu': 1},
                         'cpu_qos': 'opportunistic', 'priority': 0}
                assert controller.decision(opted, opted['resources'], identity=('opted', False), owner=None) is None
            result = controller.decision(legacy, legacy['resources'], identity=('legacy', False), owner=None)
            outcomes.append((result is None, controller.last_decision['reason'],
                             controller.last_decision['sample']))
    assert outcomes[0] == outcomes[1]


def test_refused_opted_rows_do_not_learn_legacy_telemetry_twice(tmp_path, monkeypatch):
    clock = [100.]
    monkeypatch.setattr(adaptive_cpu.time, 'time', lambda: clock[0])
    monkeypatch.setattr(adaptive_cpu, '_sealed_request',
                        lambda item: {'task': {'task_class': 'generation'}, 'params': {}})
    snapshots = []
    for interleave in (False, True):
        tiers = {'preferred': list(range(4)), 'fallback': list(range(4, 8))}
        ledger = pool.ResourceLedger(tmp_path / str(interleave), 'learning-host')
        ledger.ensure_capacity({'cpu': 8})
        ledger.configure_cpu_tiers(tiers)
        donor = 'd' * 64
        handle = ledger.begin_acquire(donor, {'cpu': 4}, cpu_tiers=tiers,
            adaptive={'declared_cpu': 4, 'shape': 'donor', 'cost': 4., 'measurement': False})
        assert handle and ledger.commit_acquire(donor, handle)
        for tick in range(4):
            clock[0] = 100. + tick
            controller = adaptive_cpu.Controller(ledger, tiers, ramp_cap=2,
                sampler=lambda: host_sample(clock[0], .1))
            adaptive_cpu.write_json(controller.base / 'telemetry' / (donor + '.json'), {
                'action_key': donor, 'complete': True, 'nonce': 'e' * 32,
                'sampled_unix': clock[0], 'wall_seconds': float(tick),
                'cpu_seconds': tick * .1, 'memory_peak_bytes': 0})
            with controller.locked():
                legacy = {'action_key': 'a' * 64, 'resources': {'cpu': 1}}
                controller.decision(legacy, legacy['resources'], identity=('legacy', False), owner=None)
                if interleave:
                    opted = {'action_key': 'b' * 64, 'resources': {'cpu': 7},
                             'cpu_qos': 'opportunistic', 'priority': -10}
                    assert controller.decision(opted, opted['resources'], identity=('opted', False), owner=None) is None
                controller.decision(legacy, legacy['resources'], identity=('legacy', False), owner=None)
        snapshots.append({name: (controller.base / name).read_bytes()
                          if (controller.base / name).exists() else None
                          for name in ('profiles.json', 'jobs.json', 'idle-baseline.json', 'last-borrow.json')})
    assert snapshots[0] == snapshots[1]


def test_opted_idle_and_counter_sampling_never_write_legacy_history(tmp_path, monkeypatch):
    clock = [100.]
    monkeypatch.setattr(adaptive_cpu.time, 'time', lambda: clock[0])
    monkeypatch.setattr(adaptive_cpu, 'control_plane_counters', lambda cpus: {})
    monkeypatch.setattr(adaptive_cpu, 'boot_ticks', lambda: 1)
    monkeypatch.setattr(adaptive_cpu, 'counters', lambda cpus: {
        'sampled_unix': clock[0], 'cpus': {str(cpu): [int(clock[0]), int(clock[0] * 100)] for cpu in cpus},
        'psi_total': 0, 'psi_avg10': 0., 'psi_avg60': 0.})
    monkeypatch.setattr(adaptive_cpu, '_sealed_request',
                        lambda item: {'task': {'task_class': 'generation'}, 'params': {}})
    tiers = {'preferred': list(range(4)), 'fallback': list(range(4, 8))}
    ledger = pool.ResourceLedger(tmp_path / 'reservations', 'counter-host')
    ledger.ensure_capacity({'cpu': 8})
    ledger.configure_cpu_tiers(tiers)
    controller = adaptive_cpu.Controller(ledger, tiers, ramp_cap=2)
    with controller.locked():
        controller.sample()
        clock[0] = 101.2
        sample = controller.sample()
        controller.idle(sample, [])
        names = ('cpu-sample.json', 'idle-baseline.json')
        before = {name: (controller.base / name).read_bytes() for name in names}
        clock[0] = 102.4
        opted = {'action_key': 'b' * 64, 'resources': {'cpu': 1},
                 'cpu_qos': 'opportunistic', 'priority': 0}
        assert controller.decision(opted, opted['resources'], identity=('opted', False), owner=None) is None
        assert before == {name: (controller.base / name).read_bytes() for name in names}

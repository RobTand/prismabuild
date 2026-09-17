"""Independent acceptance tests for the issue #569 pressure corroboration.

These tests belong to review, not to the implementation: the PR author owns
``src/``.  Every telemetry fixture is complete and internally consistent --
``busy_cpus`` is exactly the sum of the per-CPU fractions -- so a refusal or an
admission here is the behavior under test, never a missing-telemetry accident.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import pool  # noqa: E402


def host_sample(cpus, busy_by_cpu, psi_some, *, interval=10.):
    """A fresh sample as the real sampler files it: some PSI plus per-CPU busy."""
    busy = sum(busy_by_cpu.get(cpu, 0.) for cpu in cpus)
    return {'sampled_unix': time.time(), 'cpu_count': len(cpus), 'interval_s': interval,
            'busy_cpus': busy, 'psi_some': psi_some,
            'per_cpu_busy': {str(cpu): busy_by_cpu.get(cpu, 0.) for cpu in cpus}}


def uniform(cpus, value):
    return {cpu: value for cpu in cpus}


def box(tmp_path, tiers, *, gpu=False):
    """A configured box whose ledger directories exist before any decision."""
    queue = pool.PoolQueue(tmp_path / 'queue')
    ledger = queue.ledger()
    ledger.configure_cpu_tiers(tiers)
    capacity = {'cpu': len(tiers['preferred']) + len(tiers['fallback']), 'mem_gb': 2}
    if gpu:
        capacity['gpu'] = 1
    ledger.ensure_capacity(capacity)
    return queue, capacity


def publish(queue, tmp_path, *, key='a' * 64, resources=None):
    queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources=resources or {})


def seed_cheap_profile(queue, cpu=.25, shape='shape'):
    """A learned profile for the action shape, as repeated completions build."""
    from prismabuild import adaptive_cpu
    adaptive_cpu.write_json(
        adaptive_cpu.local_state_base(queue.ledger().base) / 'profiles.json',
        {shape: {'samples': 5, 'cpu': cpu, 'sampled_unix': time.time()}})


def seed_light_holder(queue, holder='h' * 64, *, ordinal=0, declared=1, cpu_rate=.1):
    """One held token whose telemetry proves light use, so it may be lent."""
    from prismabuild import adaptive_cpu
    ledger = queue.ledger()
    target = ledger.held_dir / holder
    target.mkdir(parents=True, exist_ok=True)
    token = next(path for path in sorted(ledger.free_dir.glob('cpu-*'))
                 if int(path.name.split('-')[-1]) == ordinal)
    token.rename(target / token.name)
    (target / adaptive_cpu.METADATA).write_text(json.dumps(
        {'declared_cpu': declared, 'admitted_unix': 0, 'shape': 'shape'}))
    now = time.time()
    adaptive_cpu.write_json(adaptive_cpu.local_telemetry_path(ledger.base, holder), {
        'action_key': holder, 'complete': True, 'sampled_unix': now,
        'cpu_seconds': 2., 'wall_seconds': 20., 'memory_peak_bytes': 1000})
    adaptive_cpu.write_json(adaptive_cpu.local_state_base(ledger.base) / 'jobs.json', {
        holder: {'sampled_unix': now - 1., 'cpu_seconds': 2. - cpu_rate,
                 'wall_seconds': 19.}})


@pytest.mark.parametrize('psi_some,expect_admission', [(0., True), (.11, False)])
def test_pressure_refuses_measurement_with_complete_healthy_telemetry(
        tmp_path, monkeypatch, psi_some, expect_admission):
    """A measurement is isolation-grade: host pressure refuses it outright.

    The per-CPU fractions are all low and complete, so the corroboration has
    nothing to object to; the refusal, if the contract holds, has to come from
    the pressure itself rather than from bad evidence.
    """
    from prismabuild import adaptive_cpu
    cpus = [0, 1]
    tiers = {'preferred': [0], 'fallback': [1]}
    queue, capacity = box(tmp_path, tiers)
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', True))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample',
                        lambda self: host_sample(cpus, uniform(cpus, .02), psi_some))
    publish(queue, tmp_path, resources={'cpu': 1, 'mem_gb': 1})
    item = queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True)
    assert (item is not None) is expect_admission


@pytest.mark.parametrize('psi_some,expect_admission', [(0., True), (.11, False)])
def test_pressure_refuses_unbounded_demand_with_complete_healthy_telemetry(
        tmp_path, monkeypatch, psi_some, expect_admission):
    """Legacy demand that declares no CPU cannot be checked against per-CPU
    evidence at all, so host pressure must refuse it rather than skip the
    question for lack of a declared CPU count."""
    from prismabuild import adaptive_cpu
    cpus = [0, 1]
    tiers = {'preferred': [0], 'fallback': [1]}
    queue, capacity = box(tmp_path, tiers, gpu=True)
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample',
                        lambda self: host_sample(cpus, uniform(cpus, .02), psi_some))
    publish(queue, tmp_path, resources={'gpu': 1, 'mem_gb': 1})
    item = queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True)
    assert (item is not None) is expect_admission


@pytest.mark.parametrize('psi_some,per_cpu,expect_admission', [
    (0., .03, True), (0., 0., True), (.11, .03, False), (.11, 0., False)])
def test_pressure_refuses_full_width_even_with_a_learned_cheap_profile(
        tmp_path, monkeypatch, psi_some, per_cpu, expect_admission):
    """An exclusive full-width reservation needs an exclusive host.

    A learned cheap profile shrinks the projected cost, and a low zero busy
    reading satisfies the incidental-activity tolerance, but neither is
    evidence that the host is this action's to take while "some" is high.
    """
    from prismabuild import adaptive_cpu
    cpus = [0, 1]
    tiers = {'preferred': [0], 'fallback': [1]}
    queue, capacity = box(tmp_path, tiers)
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample',
                        lambda self: host_sample(cpus, uniform(cpus, per_cpu), psi_some))
    seed_cheap_profile(queue)
    publish(queue, tmp_path, resources={'cpu': 2, 'mem_gb': 1})
    item = queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True)
    assert (item is not None) is expect_admission


def light_preferred_box(tmp_path, monkeypatch, psi_some):
    """Preferred CPU 0 is held by a proven-light action; fallback CPU 1 is free."""
    from prismabuild import adaptive_cpu
    cpus = [0, 1]
    tiers = {'preferred': [0], 'fallback': [1]}
    queue, capacity = box(tmp_path, tiers)
    seed_light_holder(queue)
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample',
                        lambda self: host_sample(cpus, {0: .3, 1: .01}, psi_some))
    publish(queue, tmp_path, resources={'cpu': 1, 'mem_gb': 1})
    return queue, capacity, tiers


def test_low_some_control_still_borrows_the_proven_idle_preferred_cpu(tmp_path, monkeypatch):
    """With pressure low, the proven-idle preferred CPU is shared first, as
    designed: this is the unchanged preferred-before-fallback behavior."""
    from prismabuild import adaptive_cpu
    queue, capacity, tiers = light_preferred_box(tmp_path, monkeypatch, 0.)
    item = queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True)
    assert item is not None
    assert item['cpu_allocation'] == {'preferred': [0], 'fallback': []}
    meta = adaptive_cpu.read_json(
        queue.ledger().held_dir / item['action_key'] / adaptive_cpu.METADATA)
    assert meta['borrowing'] is True
    assert meta['borrowed_cpu'] == 1


def test_high_some_never_shares_the_held_preferred_cpu(tmp_path, monkeypatch):
    """The claim may take the free fallback CPU or be refused, but sharing the
    held preferred CPU under high pressure is exactly what borrowing must not
    be allowed to do: the evidence that would authorize sharing is the same
    evidence pressure has made untrustworthy."""
    from prismabuild import adaptive_cpu
    queue, capacity, tiers = light_preferred_box(tmp_path, monkeypatch, .11)
    item = queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True)
    if item is not None:
        allocation = item['cpu_allocation']
        assert 0 not in allocation['preferred'] + allocation['fallback'], (
            'high pressure shared a held preferred CPU')
        assert allocation == {'preferred': [], 'fallback': [1]}
        meta = adaptive_cpu.read_json(
            queue.ledger().held_dir / item['action_key'] / adaptive_cpu.METADATA)
        assert meta.get('borrowed_cpu', 0) == 0
        assert meta.get('borrowing') is not True


def test_hot_cpus_this_claim_would_receive_refuse_even_with_idle_cores_later(
        tmp_path, monkeypatch):
    """The proof is about the CPUs the ledger would hand over, not about how
    many idle cores exist somewhere on the box."""
    from prismabuild import adaptive_cpu
    cpus = list(range(8))
    tiers = {'preferred': cpus, 'fallback': []}
    queue, capacity = box(tmp_path, tiers)
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    busy = {cpu: (1. if cpu < 4 else 0.) for cpu in cpus}
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample',
                        lambda self: host_sample(cpus, busy, .633))
    publish(queue, tmp_path, resources={'cpu': 4, 'mem_gb': 1})
    assert queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True) is None


def test_pinned_neighbour_outside_the_claim_still_admits(tmp_path, monkeypatch):
    """The reported measured shape: four of eighty cores pinned, the claim's
    own CPUs idle, so the admission stands despite some .633."""
    from prismabuild import adaptive_cpu
    cpus = list(range(80))
    tiers = {'preferred': cpus, 'fallback': []}
    queue, capacity = box(tmp_path, tiers)
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    busy = {cpu: (1. if cpu in (8, 9, 10, 11) else 0.) for cpu in cpus}
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample',
                        lambda self: host_sample(cpus, busy, .633))
    publish(queue, tmp_path, resources={'cpu': 4, 'mem_gb': 1})
    item = queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True)
    assert item is not None
    assert item['cpu_allocation'] == {'preferred': [0, 1, 2, 3], 'fallback': []}


def test_full_host_saturation_refuses_on_occupancy_alone(tmp_path, monkeypatch):
    from prismabuild import adaptive_cpu
    cpus = list(range(8))
    tiers = {'preferred': cpus, 'fallback': []}
    queue, capacity = box(tmp_path, tiers)
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample',
                        lambda self: host_sample(cpus, uniform(cpus, 1.), .633))
    publish(queue, tmp_path, resources={'cpu': 4, 'mem_gb': 1})
    assert queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True) is None


def test_projected_cost_refuses_despite_pressure(tmp_path, monkeypatch):
    """Below the occupancy gate, a claim the host cannot actually carry keeps
    its projected-cost refusal even when its own CPUs look idle."""
    from prismabuild import adaptive_cpu
    cpus = list(range(12))
    tiers = {'preferred': cpus, 'fallback': []}
    queue, capacity = box(tmp_path, tiers)
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    busy = {cpu: 0. for cpu in cpus}
    busy[5] = .05
    for cpu in range(6, 12):
        busy[cpu] = 1.
    assert sum(busy.values()) == pytest.approx(6.05)
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample',
                        lambda self: host_sample(cpus, busy, .633))
    publish(queue, tmp_path, resources={'cpu': 6, 'mem_gb': 1})
    assert queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True) is None


@pytest.mark.parametrize('mutation', ['missing', 'nan'])
def test_unknown_per_cpu_evidence_refuses_instead_of_admitting(
        tmp_path, monkeypatch, mutation):
    """Incomplete or impossible per-CPU evidence is unknown, and unknown
    refuses; it is never a CPU that is quietly idle."""
    from prismabuild import adaptive_cpu
    cpus = [0, 1]
    tiers = {'preferred': [0], 'fallback': [1]}
    queue, capacity = box(tmp_path, tiers)
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))

    def mutated(self):
        value = host_sample(cpus, uniform(cpus, .0), .633)
        if mutation == 'missing':
            del value['per_cpu_busy']
        else:
            value['per_cpu_busy']['1'] = float('nan')
        return value

    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', mutated)
    publish(queue, tmp_path, resources={'cpu': 1, 'mem_gb': 1})
    assert queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True) is None

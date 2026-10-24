"""PB #2156 reviewer r1 probes. Synthetic fixtures under tmp_path; nothing live.

The same file runs on the head and on the task base. Head-only tests skip on
the base. Characterization metrics print as one ``PROBE-METRICS`` line each.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import warnings

import pytest

from prismabuild import adaptive_cpu, pool

try:
    from prismabuild import cpu_lending
except ImportError:  # the task base has no opt-in lending module
    cpu_lending = None

head_only = pytest.mark.skipif(cpu_lending is None, reason='task base has no cpu_lending')
TIERS = {'preferred': list(range(4)), 'fallback': list(range(4, 8))}
PROTECTED = {'action_key': 'b' * 64, 'priority': 0, 'resources': {'cpu': 1}}
CPUS, PREFERRED = list(range(8)), list(range(4))


def observation(at, **changes):
    value = dict(sampled_unix=float(at), cpu_count=8, interval_s=1., busy_cpus=.4,
                 per_cpu_busy={str(c): .1 if c < 4 else 0. for c in range(8)},
                 psi_some=0., psi_avg10=0., psi_avg60=0.)
    value.update(changes)
    return value


@pytest.fixture
def rig(tmp_path, monkeypatch):
    clock = [100.]
    monkeypatch.setattr(adaptive_cpu.time, 'time', lambda: clock[0])
    monkeypatch.setattr(adaptive_cpu, '_sealed_request',
                        lambda item: {'task': {'task_class': 'generation'}, 'params': {}})
    ledger = pool.ResourceLedger(tmp_path / 'reservations', 'test-host')
    ledger.ensure_capacity({'cpu': 8, 'mem_gb': 8})
    ledger.configure_cpu_tiers(TIERS)

    def controller(sample, cap=2):
        extra = {'ramp_cap': cap} if cpu_lending is not None else {}
        return adaptive_cpu.Controller(ledger, TIERS, sampler=lambda: sample, **extra)
    return clock, ledger, controller


def decide(controller, item=PROTECTED):
    controller.decision(item, item['resources'], identity=('probe-shape', False), owner=None)
    return adaptive_cpu.read_json(controller.base / 'cpu-lending.json')


# --- Control loop: shared host-local history under the production loop shape ---

@head_only
def test_control_one_admission_loop_opens_after_30_healthy_seconds(rig):
    clock, _, make = rig
    opened = []
    for at in range(100, 161):
        clock[0] = float(at)
        if decide(make(observation(at))).get('open'):
            opened.append(at)
    assert opened and opened[0] <= 131


@head_only
def test_two_admission_loops_sharing_cpu_lending_json_open_lending(rig):
    # pool.claim builds one Controller per pass and caches one sample for it;
    # AdmissionGate is taken per candidate. A sibling loop whose pass sampled
    # one second earlier therefore decides between this loop's samples.
    clock, _, make = rig
    opened, sibling = [], None
    for at in range(100, 221):
        clock[0] = float(at)
        current = make(observation(at))
        states = [decide(current)]
        if sibling is not None:
            states.append(decide(sibling))
        sibling = current
        opened += [at for state in states if state.get('open')]
    assert opened, 'two overlapping loops never opened lending in 120 healthy seconds'


@head_only
def test_one_slow_pass_does_not_erase_shared_open_history(rig):
    clock, _, make = rig
    for at in range(100, 141):
        clock[0] = float(at)
        state = decide(make(observation(at)))
    assert state['open'] and state['budget'] == 2
    decide(make(observation(134)))  # a sibling pass still deciding on its 6 s old sample
    clock[0] = 141.
    state = decide(make(observation(141)))
    assert state['open'] and state['budget'] == 2, state


@head_only
def test_seventeen_overlapping_loops_advance_each_sample_once(rig):
    clock, _, make = rig
    siblings, first_open = [], None
    for at in range(100, 151):
        clock[0] = float(at)
        current = make(observation(at))
        state = decide(current)
        if state['open'] and first_open is None:
            first_open = at
        for sibling in [current, *siblings[-16:]]:
            assert decide(sibling) == state
        assert state['generation'] == at - 99
        siblings.append(current)
    assert first_open == 130
    assert state['open'] and state['budget'] == 2


@head_only
def test_history_generation_check_does_not_overwrite_a_winner(rig, monkeypatch):
    clock, _, make = rig
    for at in range(100, 141):
        clock[0] = float(at)
        controller = make(observation(at))
        state = decide(controller)
    path = controller.base / 'cpu-lending.json'
    winner = {**state, 'generation': state['generation'] + 1, 'sampled_unix': 141.}
    original, reads = adaptive_cpu.read_json, []

    def interleave(target):
        if target == path:
            reads.append(target)
            if len(reads) == 2:
                adaptive_cpu.write_json(path, winner)
        return original(target)

    monkeypatch.setattr(adaptive_cpu, 'read_json', interleave)
    clock[0] = 141.
    assert decide(make(observation(141))) == winner
    assert len(reads) >= 2


@head_only
def test_late_decision_never_rewrites_an_open_generation(rig):
    clock, _, make = rig
    for at in range(100, 141):
        clock[0] = float(at)
        controller = make(observation(at))
        state = decide(controller)
    before = (controller.base / 'cpu-lending.json').read_bytes()
    decide(make(observation(134)))
    assert (controller.base / 'cpu-lending.json').read_bytes() == before
    assert state['open'] and state['budget'] == 2


# --- Existing adaptive lending for default (protected) work ---

def test_default_generation_work_still_shares_idle_preferred_cpus(rig):
    clock, ledger, make = rig
    donor = {'action_key': 'a' * 64, 'priority': 0, 'resources': {'cpu': 4}}
    controller = make(observation(100), cap=0)
    with controller.locked():
        metadata = controller.decision(
            donor, donor['resources'], identity=('donor', False), owner=None)
        handle = ledger.begin_acquire(donor['action_key'], donor['resources'],
                                      adaptive=metadata, cpu_tiers=TIERS)
        assert handle and ledger.commit_acquire(donor['action_key'], handle)
    telemetry = adaptive_cpu.local_telemetry_path(ledger.base, donor['action_key'])
    for at in range(101, 132):
        clock[0] = float(at)
        adaptive_cpu.write_json(telemetry, {
            'action_key': donor['action_key'], 'complete': True, 'nonce': 'c' * 32,
            'sampled_unix': clock[0], 'wall_seconds': clock[0] - 100,
            'cpu_seconds': (clock[0] - 100) * .1})
        controller = make(observation(at), cap=0)
        with controller.locked():
            metadata = controller.decision(
                PROTECTED, PROTECTED['resources'], identity=('candidate', False), owner=None)
    warnings.warn('PROBE-METRICS ' + json.dumps({'probe': 'default_lending',
                                       'borrowing': (metadata or {}).get('borrowing'),
                                       'preferred_borrow': (metadata or {}).get('preferred_borrow')}))
    assert metadata and metadata['borrowing'] and metadata['preferred_borrow'] == 1




@head_only
def test_nonfinite_passive_observation_preserves_main_verdict_and_history(rig):
    clock, _, make = rig
    for at in range(100, 141):
        clock[0] = float(at)
        controller = make(observation(at))
        state = decide(controller)
    path = controller.base / 'cpu-lending.json'
    before = path.read_bytes()
    clock[0] = 141.
    for changes in ({'interval_s': float('nan')}, {'psi_avg10': float('inf')},
                    {'per_cpu_busy': {'0': float('nan')}}):
        assert decide(make(observation(141, **changes))) == state
        assert path.read_bytes() == before

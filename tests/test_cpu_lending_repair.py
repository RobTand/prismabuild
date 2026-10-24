"""PB #2156 reviewer r1 probes. Synthetic fixtures under tmp_path; nothing live.

The same file runs on the head and on the task base. Head-only tests skip on
the base. Characterization metrics print as one ``PROBE-METRICS`` line each.
"""
import json
import io
import os
from pathlib import Path
import subprocess
import sys
import tarfile
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


# --- Existing adaptive lending for default (protected) work ---

def test_default_generation_work_still_shares_idle_preferred_cpus(rig):
    clock, ledger, make = rig
    donor = {'action_key': 'a' * 64, 'priority': 0, 'resources': {'cpu': 4}}
    metadata = make(observation(100), cap=0).decision(
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
        metadata = make(observation(at), cap=0).decision(
            PROTECTED, PROTECTED['resources'], identity=('candidate', False), owner=None)
    warnings.warn('PROBE-METRICS ' + json.dumps({'probe': 'default_lending',
                                       'borrowing': (metadata or {}).get('borrowing'),
                                       'preferred_borrow': (metadata or {}).get('preferred_borrow')}))
    assert metadata and metadata['borrowing'] and metadata['preferred_borrow'] == 1


@pytest.fixture(scope='module')
def task_base_source(tmp_path_factory):
    root = Path(__file__).resolve().parents[1]
    target = tmp_path_factory.mktemp('cpu-lending-task-base')
    # pb-history-ok: Compare admission against the immutable task-base source.
    source = subprocess.check_output([
        'git', '-C', str(root), 'archive',
        '43234d0fbd42d6ed76da38658cb9f447ed19a606', 'src'])
    with tarfile.open(fileobj=io.BytesIO(source)) as archive:
        archive.extractall(target, filter='data')
    return target


@pytest.mark.parametrize('variant', ['ordinary', 'wide', 'busy', 'unknown', 'measurement'])
def test_cap_zero_matches_task_base_on_the_same_admission_trace(task_base_source, tmp_path, variant):
    root = Path(__file__).resolve().parents[1]
    fixture = root / 'tests/compat/support/cpu_lending_cap0_fixture.py'
    observed = []
    for label, checkout in [('base', task_base_source), ('head', root)]:
        scratch = tmp_path / label
        scratch.mkdir()
        result = subprocess.run(
            [sys.executable, str(fixture), str(scratch), variant], cwd=checkout,
            env=dict(os.environ, PYTHONPATH=str(checkout / 'src'),
                     PRISMABUILD_BOX_STATE_ROOT=str(scratch / 'box-state')),
            text=True, capture_output=True, check=True)
        observed.append(json.loads(result.stdout))
    assert observed[0] == observed[1]
    if variant == 'ordinary':
        assert observed[0]['trace'][-1]['preferred_borrow'] == 1
        assert observed[0]['trace'][-1]['borrowing'] is True



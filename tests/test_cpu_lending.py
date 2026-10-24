"""Bounded loans require current, unforgeable decisions at token acquisition."""
import ast
import copy
import importlib.util
import math
import os
import subprocess
import sys
from pathlib import Path
import pickle

import pytest

from prismabuild import adaptive_cpu, cpu_lending, pool

if os.environ.get('PB_CPU_LENDING_MUTANT'):
    spec = importlib.util.spec_from_file_location('mutated_lending', os.environ['PB_CPU_LENDING_MUTANT'])
    mutant = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mutant)
    adaptive_cpu.cpu_lending = mutant
    cpu_lending = mutant


def observation(at, **changes):
    return dict(sampled_unix=at, cpu_count=8, interval_s=1., busy_cpus=.4,
                per_cpu_busy={str(c): .1 if c < 4 else 0. for c in range(8)},
                psi_some=0., psi_avg10=0., psi_avg60=0., **changes)


def history(at=131):
    state = {}
    for stamp in range(100, at + 1):
        state = cpu_lending.advance_lending_history(
            state, observation(stamp), identity='boot/map', cpus=list(range(8)),
            preferred=list(range(4)), now=stamp, cap=2)
    return state


def advance(state, sample, *, identity='boot/map', now=None):
    return cpu_lending.advance_lending_history(
        state, sample, identity=identity, cpus=list(range(8)), preferred=list(range(4)),
        now=sample['sampled_unix'] if now is None else now, cap=2)


def test_ema_uses_elapsed_time_and_raw_spikes_close_immediately():
    state = history(141)
    raw = {str(c): 1. if c < 4 else 0. for c in range(8)}
    sample = observation(142)
    sample['per_cpu_busy'] = raw
    changed = advance(state, sample)
    assert changed['ema']['0'] == pytest.approx(.1 + .9 * -math.expm1(-1 / 30))
    assert not changed['open'] and changed['budget'] == 1
    assert advance(changed, sample)['budget'] == 1


@pytest.mark.parametrize('values', [{'psi_some': .10}, {'psi_avg10': 10.}, {'psi_avg60': 10.},
                                    {'psi_avg60': None}, {'psi_avg10': float('nan')}])
def test_pressure_and_unknowns_close_lending(values):
    sample = observation(132)
    sample.update(values)
    assert not advance(history(), sample)['open']


def test_hysteresis_requires_30_continuous_seconds_and_growth_is_bounded():
    state = history(141)
    sample = observation(142)
    sample['psi_some'] = .1
    state = advance(state, sample)
    assert state['budget'] == 1
    for at in range(143, 173):
        state = advance(state, observation(at))
        assert not state['open']
    assert advance(state, observation(173))['open']
    state = advance({}, observation(100))
    assert advance(state, observation(101))['budget'] == 0
    for at in range(102, 111):
        state = advance(state, observation(at))
    assert state['budget'] == 0
    for at in range(111, 131):
        state = advance(state, observation(at))
    assert state['budget'] == 1


@pytest.mark.parametrize('fault', ['boot', 'gap', 'stale', 'nonfinite', 'retier'])
def test_history_resets_on_lost_identity_or_coverage(fault):
    sample = observation(132)
    identity = 'new-boot' if fault == 'boot' else 'new-map' if fault == 'retier' else 'boot/map'
    if fault == 'gap':
        sample['sampled_unix'] = 140
    if fault == 'nonfinite':
        sample['per_cpu_busy']['0'] = float('inf')
    result = advance(history(), sample, identity=identity,
                     now=140 if fault == 'stale' else None)
    assert not result['open'] and result['budget'] == 0


@pytest.fixture
def loan_rig(tmp_path, monkeypatch, request):
    clock = [100.]
    monkeypatch.setattr(adaptive_cpu.time, 'time', lambda: clock[0])
    monkeypatch.setattr(adaptive_cpu, '_sealed_request', lambda item:
                        {'task': {'task_class': 'generation'}, 'params': {}})
    tiers = {'preferred': list(range(4)), 'fallback': list(range(4, 8))}
    ledger = pool.ResourceLedger(tmp_path / 'reservations', 'test-host')
    ledger.ensure_capacity({'cpu': 8, 'mem_gb': 8})
    ledger.configure_cpu_tiers(tiers)
    controller = adaptive_cpu.Controller(ledger, tiers, sampler=lambda: observation(clock[0]), ramp_cap=2)
    def item(key, cpu):
        return {'action_key': key * 64, 'cpu_qos': 'opportunistic', 'priority': -10,
                'resources': {'cpu': cpu}}
    donor_width, candidate_width = getattr(request, 'param', (4, 1))
    with cpu_lending.lending_admission_epoch():
        donor, candidate = item('a', donor_width), item('b', candidate_width)
        metadata = controller.decision(donor, donor['resources'], identity=('donor', False), owner=None)
        handle = ledger.begin_acquire(donor['action_key'], donor['resources'],
                                      adaptive=metadata, cpu_tiers=tiers)
        assert handle and ledger.commit_acquire(donor['action_key'], handle)
        for at in range(101, 132):
            clock[0] = float(at)
            adaptive_cpu.write_json(controller.base / 'telemetry' / (donor['action_key'] + '.json'),
                {'action_key': donor['action_key'], 'complete': True, 'nonce': 'c' * 32,
                 'sampled_unix': clock[0], 'wall_seconds': clock[0] - 100,
                 'cpu_seconds': (clock[0] - 100) * .1})
            controller._host_sample = None
            metadata = controller.decision(candidate, candidate['resources'],
                                           identity=('candidate', False), owner=None)
        assert metadata and metadata['borrowing']
        yield ledger, tiers, controller, donor, candidate, metadata


def test_real_decision_borrows_and_protects_orphaned_ids(loan_rig):
    ledger, tiers, controller, donor, candidate, metadata = loan_rig
    handle = ledger.begin_acquire(candidate['action_key'], candidate['resources'],
                                  adaptive=metadata, cpu_tiers=tiers)
    assert handle and ledger.commit_acquire(candidate['action_key'], handle)
    controller.admitted(metadata)
    stored = adaptive_cpu.read_json(ledger.held_dir / candidate['action_key'] / adaptive_cpu.METADATA)
    assert stored['cpu_weight'] == 10 and stored['borrowed_cpu'] == 1
    assert not {'_loan_grant', '_eligibility'} & stored.keys()
    ledger.release(donor['action_key'])
    candidate.update(action_key='d' * 64, priority=0, cpu_qos='protected')
    controller._host_sample = None
    protected = controller.decision(candidate, candidate['resources'], identity=('protected', False), owner=None)
    assert protected and protected['cpu_weight'] == 100 and not protected['borrowing']
    borrowed = stored['allocation']['preferred'][0]
    assert borrowed not in protected['eligible_cpus']
    for acquire in (ledger.begin_acquire, ledger._begin_acquire_locked):
        handle = acquire('f' * 64, {'cpu': 1}, cpu_tiers=tiers)
        assert handle
        assert borrowed not in ledger.cpu_ids(list((ledger.held_dir / handle).glob('cpu-*')), tiers)
        ledger.abandon_acquire(handle)


@pytest.mark.parametrize('private', [False, True])
def test_loan_cannot_leave_its_admission_lock_hold(loan_rig, private):
    ledger, tiers, controller, donor, item, _ = loan_rig
    with controller.locked():
        metadata = controller.decision(item, item['resources'], identity=('candidate', False), owner=None)
        assert metadata and metadata['borrowing']
    acquire = ledger._begin_acquire_locked if private else ledger.begin_acquire
    assert acquire(item['action_key'], item['resources'], adaptive=metadata, cpu_tiers=tiers) is None


@pytest.mark.parametrize('private', [False, True])
@pytest.mark.parametrize('fault', ['priority', 'declaration', 'plain', 'new', 'subclass', 'changed_ids',
                                   'stale', 'closed', 'cap', 'donor_exit', 'competing', 'spent',
                                   'eligibility_new', 'eligibility_subclass', 'measurement', 'funding',
                                   'donor_stale', 'donor_greedy', 'donor_nonce'])
def test_valid_to_invalid_is_refused_at_final_boundary(loan_rig, private, fault, monkeypatch):
    ledger, tiers, controller, donor, item, metadata = loan_rig
    if fault == 'priority':
        item['priority'] = 0
    elif fault == 'declaration':
        item['cpu_qos'] = 'protected'
    elif fault == 'measurement':
        metadata['measurement'] = True
    elif fault == 'funding':
        metadata['funded'] = {'cpus': [0], 'mem_gb': 0}
    elif fault == 'changed_ids':
        metadata['borrowable_cpus'] = [3]
    elif fault == 'stale':
        monkeypatch.setattr(cpu_lending.time, 'time', lambda: 138.)
    elif fault == 'closed':
        state = adaptive_cpu.read_json(controller.base / 'cpu-lending.json')
        state['open'] = False
        adaptive_cpu.write_json(controller.base / 'cpu-lending.json', state)
    elif fault == 'cap':
        state = adaptive_cpu.read_json(controller.base / 'cpu-lending.json')
        state['budget'] = 0
        adaptive_cpu.write_json(controller.base / 'cpu-lending.json', state)
    elif fault == 'donor_exit':
        ledger.release(donor['action_key'])
    elif fault.startswith('donor_'):
        path = controller.base / 'telemetry' / (donor['action_key'] + '.json')
        value = adaptive_cpu.read_json(path)
        field, changed = {'donor_stale': ('sampled_unix', 100.),
                          'donor_greedy': ('cpu_seconds', 100.),
                          'donor_nonce': ('nonce', 'd' * 32)}[fault]
        value[field] = changed
        adaptive_cpu.write_json(path, value)
    elif fault == 'competing':
        handle = ledger.begin_acquire('e' * 64, {'mem_gb': 1})
        assert handle and ledger.commit_acquire('e' * 64, handle)
    elif fault == 'spent':
        handle = ledger.begin_acquire(item['action_key'], item['resources'],
                                      adaptive=metadata, cpu_tiers=tiers)
        assert handle
        controller.admitted(metadata)
    elif fault.startswith('eligibility_'):
        kind = cpu_lending.OpportunisticEligibility
        if fault == 'eligibility_subclass':
            kind = type('ForgedEligibility', (kind,), {})
        metadata['_eligibility'] = object.__new__(kind)
    else:
        kind = cpu_lending.CpuLoanGrant
        if fault == 'subclass':
            kind = type('ForgedGrant', (kind,), {})
        metadata['_loan_grant'] = object.__new__(kind) if fault != 'plain' else {'borrowing': True}
    acquire = ledger._begin_acquire_locked if private else ledger.begin_acquire
    before = ledger.available()
    if fault in ('stale', 'closed', 'cap', 'donor_exit', 'competing',
                 'donor_stale', 'donor_greedy', 'donor_nonce'):
        assert acquire(item['action_key'], item['resources'], adaptive=metadata, cpu_tiers=tiers) is None
    else:
        with pytest.raises(ValueError, match='eligibility|grant'):
            acquire(item['action_key'], item['resources'], adaptive=metadata, cpu_tiers=tiers)
    assert ledger.available() == before


@pytest.mark.parametrize('name', ['_eligibility', '_loan_grant'])
@pytest.mark.parametrize('operation', [copy.copy, copy.deepcopy, pickle.dumps])
def test_capabilities_cannot_be_transferred(loan_rig, name, operation):
    with pytest.raises(TypeError):
        operation(loan_rig[-1][name])


def test_capability_construction_has_one_ast_owner():
    root = Path(__file__).resolve().parents[1]
    expected = {'OpportunisticEligibility': 'opportunistic_eligibility',
                'LegacyEligibility': 'legacy_eligibility',
                'CpuLoanGrant': 'decide_cpu_loan'}
    owners = {name: [] for name in expected}
    class Construction(ast.NodeVisitor):
        owner = '<module>'
        def visit_FunctionDef(self, node):
            old, self.owner = self.owner, node.name
            self.generic_visit(node)
            self.owner = old
        def visit_Call(self, node):
            name = node.func.id if isinstance(node.func, ast.Name) else (
                node.func.attr if isinstance(node.func, ast.Attribute) else None)
            if name in expected:
                owners[name].append((relative, self.owner))
            if name == '__new__' and node.args:
                argument = node.args[0]
                name = argument.id if isinstance(argument, ast.Name) else None
                if name in expected:
                    owners[name].append((relative, self.owner))
            self.generic_visit(node)
    for directory in ('src', 'tools'):
        for path in sorted((root / directory).rglob('*.py')):
            relative = str(path.relative_to(root))
            Construction().visit(ast.parse(path.read_text()))
    assert owners == {name: [('src/prismabuild/cpu_lending.py', owner)]
                      for name, owner in expected.items()}


@pytest.mark.parametrize('decision,kind', [('opportunistic_eligibility', 'OpportunisticEligibility'),
                                         ('decide_cpu_loan', 'CpuLoanGrant'),
                                         ('opportunistic_eligibility', 'policy'),
                                         ('decide_cpu_loan', 'policy')])
def test_mutation_bypassing_decision_fails_the_valid_loan_suite(tmp_path, monkeypatch, decision, kind):
    tree = ast.parse(Path(cpu_lending.__file__).read_text())
    target = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == decision)
    if kind == 'policy':
        guards = [node for node in target.body if isinstance(node, ast.If)
                  and len(node.body) == 1 and isinstance(node.body[0], ast.Return)
                  and isinstance(node.body[0].value, ast.Constant) and node.body[0].value.value is None
                  and (decision != 'decide_cpu_loan' or any(
                      isinstance(value, ast.Name) and value.id == 'needed' for value in ast.walk(node.test)))]
        assert len(guards) == 1
        target.body.remove(guards[0])
    else:
        target.body = ast.parse(f'return object.__new__({kind})').body
    mutated = tmp_path / 'mutated_lending.py'
    mutated.write_text(ast.unparse(ast.fix_missing_locations(tree)))
    result = subprocess.run(
        [sys.executable, '-m', 'pytest', '-q', str(Path(__file__).resolve()),
         '-k', 'test_real_decision_borrows_and_protects_orphaned_ids or test_eligibility_requires_explicit_policy or test_closed_policy_cannot_issue_a_loan'],
        env={**os.environ, 'PB_CPU_LENDING_MUTANT': str(mutated)},
        capture_output=True, text=True, timeout=90)
    assert result.returncode == 1, result.stdout + result.stderr
    assert ('FAILED' in result.stdout or 'ERROR' in result.stdout), result.stdout + result.stderr


def test_rollback_uses_frozen_grant_after_priority_changes(loan_rig):
    ledger, tiers, controller, donor, item, metadata = loan_rig
    handle = ledger.begin_acquire(item['action_key'], item['resources'], adaptive=metadata, cpu_tiers=tiers)
    assert handle
    previous = controller.admitted(metadata)
    item['priority'] = 0
    ledger.abandon_acquire(handle)
    controller.withdrew(metadata, previous)
    assert adaptive_cpu.read_json(controller.base / 'last-borrow.json') == (previous or {})


@pytest.mark.parametrize('private', [False, True])
@pytest.mark.parametrize('loan_rig', [(4, 1), (3, 2)], indirect=True)
@pytest.mark.parametrize('fault', ['missing', 'wrong_type', 'wrong_tier'])
def test_missing_borrower_affinity_never_makes_returned_tokens_free(loan_rig, private, fault):
    ledger, tiers, controller, donor, item, metadata = loan_rig
    handle = ledger.begin_acquire(item['action_key'], item['resources'],
                                 adaptive=metadata, cpu_tiers=tiers)
    assert handle and ledger.commit_acquire(item['action_key'], handle)
    ledger.release(donor['action_key'])
    path = ledger.held_dir / item['action_key'] / adaptive_cpu.METADATA
    if fault == 'missing':
        path.unlink()
    else:
        saved = adaptive_cpu.read_json(path)
        saved['allocation']['preferred'] = ['0'] if fault == 'wrong_type' else [4]
        adaptive_cpu.write_json(path, saved)
    acquire = ledger._begin_acquire_locked if private else ledger.begin_acquire
    assert acquire('e' * 64, {'cpu': 1}, cpu_tiers=tiers) is None


def test_static_cpu_less_holder_records_proven_empty_affinity(loan_rig):
    ledger, tiers, controller, donor, item, metadata = loan_rig
    handle = ledger.begin_acquire('e' * 64, {'mem_gb': 1}, cpu_tiers=tiers)
    assert handle and ledger.commit_acquire('e' * 64, handle)
    saved = adaptive_cpu.read_json(ledger.held_dir / ('e' * 64) / adaptive_cpu.METADATA)
    assert saved['allocation'] == {'preferred': [], 'fallback': []}
    assert ledger.begin_acquire('f' * 64, {'cpu': 1}, cpu_tiers=tiers)


def test_running_donor_policy_must_name_its_actual_holder(loan_rig):
    ledger, tiers, controller, donor, item, metadata = loan_rig
    saved = adaptive_cpu.read_json(ledger.held_dir / donor['action_key'] / adaptive_cpu.METADATA)
    action = {'task': {'task_class': 'generation'}, 'params': {}}
    options = dict(measurement=False, owner=None, allowance=None,
                   running=True, holder_key=donor['action_key'])
    assert cpu_lending.opportunistic_eligibility(saved, action, **options)
    saved['opportunistic_policy']['action_key'] = 'f' * 64
    assert cpu_lending.opportunistic_eligibility(saved, action, **options) is None


@pytest.mark.parametrize('counted', [False, True])
@pytest.mark.parametrize('nfs_remnant', [False, True])
def test_completed_release_remnant_cannot_block_host_cpu_admission(loan_rig, monkeypatch, counted, nfs_remnant):
    ledger, tiers, controller, donor, item, metadata = loan_rig
    holder = ledger.held_dir / donor['action_key']
    original = Path.rmdir
    def cannot_remove(path):
        if path == holder:
            if nfs_remnant:
                (path / '.nfs-test-reader').touch()
            raise OSError('injected delayed directory removal')
        return original(path)
    monkeypatch.setattr(Path, 'rmdir', cannot_remove)
    if counted:
        assert ledger.release_count(donor['action_key'], {'cpu': 4}) == {'cpu': 4}
    else:
        assert ledger.release(donor['action_key']) == 4
    assert not holder.exists()
    assert len(list((ledger.base / 'released-holders').iterdir())) == 1
    assert ledger.begin_acquire('e' * 64, {'cpu': 1}, cpu_tiers=tiers)
    assert ledger.capacity()['cpu'] == 8


def test_directory_retirement_cannot_hide_a_surviving_resource(loan_rig):
    ledger, tiers, controller, donor, item, metadata = loan_rig
    holder = ledger.held_dir / donor['action_key']
    with pytest.raises(OSError):
        ledger._finish_returned_holder(holder)
    assert holder.is_dir() and len(list(holder.glob('cpu-*'))) == 4


def test_bad_physical_token_refuses_pressure_attribution(loan_rig, monkeypatch):
    ledger, tiers, controller, donor, item, metadata = loan_rig
    (ledger.free_dir / 'cpu-999').touch()
    sample = observation(131)
    sample['psi_some'] = .2
    controller._host_sample = None
    monkeypatch.setattr(controller, '_sampler', lambda: sample)
    assert controller.decision(item, item['resources'], identity=('candidate', False), owner=None) is None
    assert controller.last_decision['reason'] == 'host_pressure_unproven'


@pytest.mark.parametrize('fault', ['default', 'priority', 'measurement', 'exclusive', 'funded', 'legacy_donor'])
def test_eligibility_requires_explicit_policy(fault):
    item = {'action_key': 'a' * 64, 'cpu_qos': 'opportunistic', 'priority': -10, 'resources': {'cpu': 1}}
    action = {'task': {'task_class': 'generation'}, 'params': {}}
    if fault == 'default':
        item.pop('cpu_qos')
    elif fault == 'priority':
        item['priority'] = -1
    elif fault == 'exclusive':
        action['params']['gpu_exclusive'] = True
    elif fault == 'legacy_donor':
        item.update(cpu_weight=10, shape='shape')
    assert cpu_lending.opportunistic_eligibility(
        item, action, measurement=fault == 'measurement', owner='owner' if fault == 'funded' else None,
        allowance=None, running=fault == 'legacy_donor') is None


@pytest.mark.parametrize('fault', ['pressure', 'per_core', 'ema', 'budget', 'claimants', 'nested', 'fallback', 'preferred_cost'])
def test_closed_policy_cannot_issue_a_loan(loan_rig, fault):
    ledger, tiers, controller, donor, item, issued = loan_rig
    metadata = dict(issued)
    state = adaptive_cpu.read_json(controller.base / 'cpu-lending.json')
    sample = copy.deepcopy(controller._host_sample)
    assignments = {cpu: 1 for cpu in tiers['preferred']}
    cost, borrowers = issued['cost'], 0
    if fault == 'pressure':
        state['open'] = False
    elif fault == 'per_core':
        sample['per_cpu_busy']['0'] = .5
    elif fault == 'ema':
        state['ema']['0'] = .5
    elif fault == 'budget':
        state['budget'] = 0
    elif fault == 'claimants':
        borrowers = 2
    elif fault == 'nested':
        assignments[0] = 2
    elif fault == 'fallback':
        metadata['borrowable_cpus'] = [4]
    elif fault == 'preferred_cost':
        cost = 4.
    assert cpu_lending.decide_cpu_loan(
        metadata['_eligibility'], metadata, ledger=ledger, demand=item['resources'], tiers=tiers,
        history=state, sample=sample, assignments=assignments, outstanding=0,
        borrowers=borrowers, pending=0., cost=cost, state_path=controller.base / 'cpu-lending.json') is None


@pytest.mark.parametrize('private', [False, True])
def test_none_epoch_cannot_mint_or_acquire_a_loan(loan_rig, private):
    ledger, tiers, controller, donor, item, metadata = loan_rig
    token = cpu_lending._admission_epoch.set(None)
    try:
        acquire = ledger._begin_acquire_locked if private else ledger.begin_acquire
        assert acquire(item['action_key'], item['resources'], adaptive=metadata, cpu_tiers=tiers) is None
        assert controller.decision(item, item['resources'], identity=('candidate', False), owner=None).get('borrowing') is False
    finally:
        cpu_lending._admission_epoch.reset(token)

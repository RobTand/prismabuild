"""Main's legacy statements remain identical after named policy identity guards."""
import ast
import copy
import hashlib
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / 'tests/fixtures/cpu_lending_main_methods.json'
PIN = 'd6ce1339b26318acb3939cda57e3cd31ed664849b7c5b866f1bc86eb40e3a325'
METHODS = json.loads(REFERENCE.read_bytes())['methods']


class LegacyIdentityGuards(ast.NodeTransformer):
    def visit_BoolOp(self, node):
        node = self.generic_visit(node)
        node.values = [value for value in node.values if not (
            isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute)
            and isinstance(value.func.value, ast.Name) and value.func.value.id == 'cpu_lending'
            and value.func.attr in ('legacy_donor_allowed', 'legacy_borrower_allowed'))]
        return node.values[0] if len(node.values) == 1 else node

    def visit_AugAssign(self, node):
        if (isinstance(node.target, ast.Name) and node.target.id == 'borrowable'
                and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute)
                and node.value.func.attr == 'opportunistic_borrowed_cpu_ids'):
            return None
        return self.generic_visit(node)

    def visit_Attribute(self, node):
        if node.attr == 'admission_available':
            node.attr = 'available'
        return self.generic_visit(node)


@pytest.mark.parametrize('identity', sorted(METHODS))
def test_legacy_statements_match_adopted_main(identity):
    assert hashlib.sha256(REFERENCE.read_bytes()).hexdigest() == PIN
    filename, qualified = identity.split(':')
    owner, name = qualified.split('.')
    current_name = name if name == 'cpu_allocation' else '_legacy_' + name.lstrip('_')
    tree = ast.parse((ROOT / 'src/prismabuild' / filename).read_bytes())
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == owner)
    actual = copy.deepcopy(next(node for node in cls.body if isinstance(node, ast.FunctionDef)
                                and node.name == current_name))
    expected = ast.parse('class Reference:\n' + METHODS[identity]).body[0].body[0]
    actual.name = expected.name
    if name == 'decision':
        actual = LegacyIdentityGuards().visit(actual)
    assert ast.dump(actual, include_attributes=False) == ast.dump(expected, include_attributes=False)


def test_explicit_policy_guards_are_identity_for_undeclared_rows():
    from prismabuild import cpu_lending
    for value in ({}, {'shape': 'known'}, {'cpu_qos': 'legacy'}, {'cpu_qos': 'opportunistic'}):
        assert cpu_lending.legacy_donor_allowed(value)
        assert cpu_lending.legacy_borrower_allowed(value)
    assert not cpu_lending.legacy_donor_allowed({'cpu_qos': 'protected'})
    assert not cpu_lending.legacy_borrower_allowed({'cpu_qos': 'protected'})


@pytest.mark.parametrize('ramp_cap', [0, 2])
def test_legacy_acquisition_never_calls_opted_capability_checks(tmp_path, monkeypatch, ramp_cap):
    from prismabuild import adaptive_cpu, cpu_lending, pool
    def forbidden(*args, **kwargs):
        raise AssertionError('legacy admission entered opted validation')
    monkeypatch.setattr(cpu_lending, 'require_cpu_acquisition', forbidden)
    tiers = {'preferred': [0, 1], 'fallback': [2, 3]}
    ledger = pool.ResourceLedger(tmp_path / 'reservations', 'legacy-host')
    ledger.ensure_capacity({'cpu': 4})
    ledger.configure_cpu_tiers(tiers)
    controller = adaptive_cpu.Controller(ledger, tiers, ramp_cap=ramp_cap, sampler=lambda: {
        'sampled_unix': adaptive_cpu.time.time(), 'busy_cpus': 0., 'psi_some': 0.,
        'cpu_count': 4, 'interval_s': 1.})
    monkeypatch.setattr(controller, '_opportunistic_decision', forbidden)
    with controller.locked():
        item = {'action_key': 'a' * 64, 'resources': {'cpu': 1}}
        metadata = controller.decision(item, item['resources'], identity=('known', False), owner=None)
        handle = ledger.begin_acquire(item['action_key'], item['resources'], adaptive=metadata, cpu_tiers=tiers)
        assert handle and ledger.commit_acquire(item['action_key'], handle)


@pytest.mark.parametrize('need,values', [
    (1, {}), (1, {'sampled_unix': 90.}), (1, {'busy_cpus': 8.}),
    (1, {'psi_some': .2, 'per_cpu_busy': {str(cpu): .95 for cpu in range(8)}}),
    (8, {}), (0, {}), (1, {'interval_s': float('nan')}),
])
def test_shared_nonloan_gates_have_the_same_decision_and_allocation(tmp_path, monkeypatch, need, values):
    from prismabuild import adaptive_cpu, pool
    monkeypatch.setattr(adaptive_cpu.time, 'time', lambda: 100.)
    monkeypatch.setattr(adaptive_cpu, '_sealed_request',
                        lambda item: {'task': {'task_class': 'generation'}, 'params': {}})
    tiers = {'preferred': list(range(4)), 'fallback': list(range(4, 8))}
    sample = {'sampled_unix': 100., 'cpu_count': 8, 'interval_s': 1., 'busy_cpus': .4,
              'per_cpu_busy': {str(cpu): .05 for cpu in range(8)},
              'psi_some': 0., 'psi_avg10': 0., 'psi_avg60': 0., **values}
    outcomes = []
    for policy in ('protected', 'opportunistic'):
        ledger = pool.ResourceLedger(tmp_path / policy / 'reservations', 'parity-host')
        ledger.ensure_capacity({'cpu': 8})
        ledger.configure_cpu_tiers(tiers)
        controller = adaptive_cpu.Controller(ledger, tiers, sampler=lambda: sample, ramp_cap=2)
        item = {'action_key': 'a' * 64, 'resources': {'cpu': need},
                'cpu_qos': policy, 'priority': -10}
        allocation = None
        with controller.locked():
            metadata = controller.decision(item, item['resources'], identity=('known', False), owner=None)
            if metadata:
                assert not metadata['borrowing'] and '_loan_grant' not in metadata
                handle = ledger.begin_acquire(item['action_key'], item['resources'],
                                              adaptive=metadata, cpu_tiers=tiers)
                if handle:
                    allocation = ledger.cpu_allocation(handle, tiers)
                    ledger.abandon_acquire(handle)
        outcomes.append((metadata is not None, controller.last_decision['reason'],
                         (metadata or {}).get('cost'), allocation))
    assert outcomes[0] == outcomes[1]


@pytest.mark.parametrize('ramp_cap', [0, 2])
def test_optional_lending_boot_identity_cannot_break_undeclared_admission(tmp_path, monkeypatch, ramp_cap):
    from prismabuild import adaptive_cpu, pool
    original = Path.read_text
    def absent_boot(path, *args, **kwargs):
        if path == Path('/proc/sys/kernel/random/boot_id'):
            if ramp_cap == 0:
                raise AssertionError('cap zero read opportunistic boot identity')
            raise OSError('injected unavailable opportunistic boot identity')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'read_text', absent_boot)
    tiers = {'preferred': [0, 1], 'fallback': [2, 3]}
    ledger = pool.ResourceLedger(tmp_path / 'reservations', 'legacy-host')
    ledger.ensure_capacity({'cpu': 4})
    ledger.configure_cpu_tiers(tiers)
    controller = adaptive_cpu.Controller(ledger, tiers, ramp_cap=ramp_cap, sampler=lambda: {
        'sampled_unix': adaptive_cpu.time.time(), 'cpu_count': 4, 'interval_s': 1.,
        'busy_cpus': 0., 'psi_some': 0.})
    with controller.locked():
        result = controller.decision({'action_key': 'a' * 64}, {'cpu': 1},
                                     identity=('known', False), owner=None)
    assert result and not result['borrowing']
    assert not (controller.base / 'cpu-lending.json').exists()


def test_failed_passive_history_write_cannot_refuse_undeclared_work(tmp_path, monkeypatch):
    from prismabuild import adaptive_cpu, pool
    tiers = {'preferred': [0, 1], 'fallback': [2, 3]}
    ledger = pool.ResourceLedger(tmp_path / 'reservations', 'legacy-host')
    ledger.ensure_capacity({'cpu': 4})
    ledger.configure_cpu_tiers(tiers)
    controller = adaptive_cpu.Controller(ledger, tiers, ramp_cap=2, sampler=lambda: {
        'sampled_unix': adaptive_cpu.time.time(), 'cpu_count': 4, 'interval_s': 1.,
        'busy_cpus': 0., 'psi_some': 0.})
    def unavailable(*args):
        raise OSError('injected failed lending history write')
    monkeypatch.setattr(controller, '_observe_lending', unavailable)
    with controller.locked():
        result = controller.decision({'action_key': 'a' * 64}, {'cpu': 1},
                                     identity=('known', False), owner=None)
    assert result and not result['borrowing']
    assert controller.last_decision['reason'] == 'admitted'

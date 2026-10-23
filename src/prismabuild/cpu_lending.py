"""Opt-in CPU lending history and process-local decision capabilities.

Only host-local observations authorize lending. Durable metadata describes a
past decision; deserializing it cannot reconstruct permission to borrow.
"""
from __future__ import annotations

import copy
import math
import json
import time
import os
from contextlib import contextmanager
from contextvars import ContextVar
from weakref import WeakKeyDictionary


def lending_ramp_cap(preferred_count, configured=0):
    """Default off; worker configuration bounds lending independently of capacity."""
    maximum = 2 if preferred_count <= 10 else 4
    return configured if type(configured) is int and 0 <= configured <= maximum else 0


class CpuLendingChanged(ValueError):
    """A valid loan lost its current observation or holder evidence."""


def advance_lending_history(previous, sample, *, identity, cpus, preferred, now, cap):
    """One EMA/hysteresis/AIMD transition per fresh host observation."""
    at = sample.get('sampled_unix')
    raw = sample.get('per_cpu_busy')
    valid = (type(at) in (int, float) and math.isfinite(at) and 0 <= now - at <= 5
             and sample.get('cpu_count') == len(cpus)
             and type(sample.get('interval_s')) in (int, float)
             and math.isfinite(sample['interval_s']) and 0 < sample['interval_s'] <= 5
             and isinstance(raw, dict) and set(raw) == {str(c) for c in cpus}
             and all(type(v) in (int, float) and math.isfinite(v) and 0 <= v <= 1
                     for v in raw.values())
             and all(type(sample.get(k)) in (int, float) and math.isfinite(sample[k])
                     and 0 <= sample[k] <= ceiling for k, ceiling in
                     (('psi_some', 1), ('psi_avg10', 100), ('psi_avg60', 100))))
    if not valid:
        return {'identity': identity, 'open': False, 'budget': 0, 'reason': 'unknown_or_stale'}
    last = previous.get('sampled_unix', 0)
    previous_valid = (type(last) in (int, float) and math.isfinite(last)
                      and type(previous.get('budget')) is int and previous['budget'] >= 0
                      and type(previous.get('open')) is bool
                      and (previous.get('healthy_since') is None
                           or type(previous['healthy_since']) in (int, float)
                           and math.isfinite(previous['healthy_since'])
                           and previous['healthy_since'] <= last)
                      and all(type(previous.get(k)) in (int, float) and math.isfinite(previous[k])
                              and previous[k] <= last for k in ('history_since', 'last_growth')))
    if not previous_valid:
        previous, last = {}, 0
    same_identity = previous.get('identity') == identity
    if (same_identity and at == last and isinstance(previous.get('ema'), dict)
            and set(previous['ema']) == set(raw)
            and all(type(v) in (int, float) and math.isfinite(v) and 0 <= v <= 1
                    for v in previous['ema'].values())):
        return {**previous, 'budget': min(cap, previous.get('budget', 0)),
                'open': bool(cap and previous.get('open'))}
    elapsed = at - last
    continuous = (same_identity and 0 < elapsed <= 5
                  and isinstance(previous.get('ema'), dict)
                  and set(previous['ema']) == set(raw)
                  and all(type(v) in (int, float) and math.isfinite(v) and 0 <= v <= 1
                          for v in previous['ema'].values()))
    alpha = -math.expm1(-elapsed / 30) if continuous else 1.
    ema = {key: value * alpha + previous['ema'][key] * (1 - alpha)
           if continuous else value for key, value in raw.items()}
    start = previous['history_since'] if continuous else at
    budget = min(cap, previous.get('budget', 0)) if continuous else 0
    breach = (sample['psi_some'] >= .10 or sample['psi_avg10'] >= 10
              or sum(raw[str(c)] for c in preferred) > .85 * len(preferred))
    healthy = sample['psi_avg10'] < 5 and sample['psi_avg60'] < 5 and not breach
    healthy_since = previous.get('healthy_since') if continuous and healthy else None
    if healthy and healthy_since is None:
        healthy_since = at
    opened = (continuous and previous.get('open', False) and not breach
              or healthy_since is not None and at - healthy_since >= 30)
    opened = bool(opened and at - start >= 30 and cap)
    growth = previous.get('last_growth', at) if continuous else at
    if breach:
        budget //= 2
        growth = at
    elif opened and healthy and at - growth >= 10:
        budget = min(cap, budget + 1)
        growth = at
    elif not healthy:
        growth = at
    return {'identity': identity, 'sampled_unix': at, 'ema': ema,
            'history_since': start, 'healthy_since': healthy_since,
            'last_growth': growth, 'budget': budget, 'open': opened,
            'reason': 'open' if opened else 'pressure' if breach else 'warming'}


_ELIGIBILITY_MINT = object()
_LOAN_MINT = object()
_eligible = WeakKeyDictionary()
_loans = WeakKeyDictionary()
_admission_epoch = ContextVar('cpu_lending_admission_epoch', default=None)


@contextmanager
def lending_admission_epoch():
    token = _admission_epoch.set(object())
    try:
        yield
    finally:
        _admission_epoch.reset(token)


class OpportunisticEligibility:
    __slots__ = ('__weakref__',)

    def __new__(cls, authority):
        if cls is not OpportunisticEligibility or authority is not _ELIGIBILITY_MINT:
            raise TypeError('eligibility requires the CPU QoS decision')
        return super().__new__(cls)

    def __reduce_ex__(self, protocol):
        raise TypeError('CPU eligibility cannot be copied or serialized')


class CpuLoanGrant:
    __slots__ = ('__weakref__',)

    def __new__(cls, authority):
        if cls is not CpuLoanGrant or authority is not _LOAN_MINT:
            raise TypeError('loan requires the CPU lending decision')
        return super().__new__(cls)

    def __reduce_ex__(self, protocol):
        raise TypeError('CPU loan grants cannot be copied or serialized')


def _qos_inputs(item):
    return copy.deepcopy({key: item.get(key) for key in
                          ('action_key', 'priority', 'cpu_qos', 'resources',
                           'dependent_of', 'needs_gpu', 'opportunistic_policy',
                           'published_unix', 'attempts', 'cas_root')})


def opportunistic_eligibility(item, action, *, measurement, owner, allowance, running=False, holder_key=None):
    """The sole eligibility decision. Priority alone never opts work in."""
    original = item
    if running:
        policy = item.get('opportunistic_policy')
        if (not isinstance(policy, dict) or item.get('cpu_qos') != 'opportunistic'
                or item.get('cpu_weight') != 10 or not item.get('shape')
                or policy.get('action_key') != holder_key):
            return None
        item = policy
    if (item.get('cpu_qos') != 'opportunistic' or item.get('priority') != -10
            or measurement or owner or item.get('dependent_of') or allowance
            or not isinstance(action, dict)
            or action.get('task', {}).get('task_class') != 'generation'
            or action.get('params', {}).get('gpu_exclusive')
            or action.get('params', {}).get('exclusive')
            or not item.get('resources', {}).get('cpu')):
        return None
    result = OpportunisticEligibility(_ELIGIBILITY_MINT)
    _eligible[result] = (os.getpid(), original, _qos_inputs(original))
    return result


def require_opportunistic(value):
    if type(value) is not OpportunisticEligibility or value not in _eligible:
        raise ValueError('unissued opportunistic eligibility')
    pid, item, selected = _eligible[value]
    if pid != os.getpid() or _qos_inputs(item) != selected:
        raise ValueError('opportunistic eligibility changed before acquisition')
    return selected


_LOAN_FIELDS = ('declared_cpu', 'cpu_qos', 'cpu_weight', 'borrowing',
                'preferred_borrow', 'borrowable_cpus', 'eligible_cpus', 'sampled_unix',
                'measurement', 'funded_by', 'funded', 'dependent_allowance',
                'unbounded_cpu', 'serves_measurement', 'shape', 'opportunistic_policy')


def decide_cpu_loan(eligibility, metadata, *, ledger, demand, tiers, history,
                    sample, assignments, outstanding, borrowers, pending, cost, state_path):
    """Authorize a bounded preferred-ID loan after the ordinary CPU gates."""
    require_opportunistic(eligibility)
    preferred = set(tiers['preferred'])
    raw = sample.get('per_cpu_busy', {})
    ema = history.get('ema', {})
    candidates = [cpu for cpu in metadata['borrowable_cpus']
                  if cpu in preferred and assignments.get(cpu, 0) == 1
                  and raw.get(str(cpu), 1) < .5 and ema.get(str(cpu), 1) < .5]
    needed = max(metadata['preferred_borrow'],
                 metadata['declared_cpu'] - len(ledger.free_cpu_tokens(
                     tiers, eligible_cpus=metadata.get('eligible_cpus'))))
    projected = max(sum(raw.get(str(c), 1) for c in preferred),
                    sum(ema.get(str(c), 1) for c in preferred)) + pending + cost
    if (not history.get('open') or needed <= 0 or needed > len(candidates)
            or outstanding + needed > history.get('budget', 0) or borrowers >= 2
            or projected > .85 * len(preferred)):
        return None
    metadata['borrowable_cpus'] = candidates[:needed]
    metadata['preferred_borrow'] = needed
    holders = _lending_holder_binding(ledger)
    try:
        telemetry = _donor_binding(holders, metadata['borrowable_cpus'], state_path.parent)
    except CpuLendingChanged:
        return None
    result = CpuLoanGrant(_LOAN_MINT)
    _loans[result] = {
        'pid': os.getpid(), 'eligibility': eligibility, 'ledger_base': ledger.base.resolve(),
        'epoch': _admission_epoch.get(),
        'demand': {k: v for k, v in demand.items() if v},
        'tiers': copy.deepcopy(tiers),
        'fields': copy.deepcopy({k: metadata.get(k) for k in _LOAN_FIELDS}),
        'history': copy.deepcopy(history), 'state_path': state_path,
        'holders': holders, 'telemetry': telemetry, 'phase': 'decided'}
    return result


def require_cpu_acquisition(metadata, *, ledger, key, demand, tiers, begin=False):
    """Used by both public and private reservation entrypoints before mutation."""
    if metadata is None:
        return
    eligible = metadata.get('_eligibility')
    if metadata.get('cpu_qos') == 'opportunistic' or metadata.get('cpu_weight', 100) != 100:
        selected = require_opportunistic(eligible)
        if selected['action_key'] != key or metadata.get('cpu_weight') != 10:
            raise ValueError('CPU eligibility differs from the reservation')
    borrowing = (metadata.get('borrowing') or metadata.get('preferred_borrow')
                 or metadata.get('borrowable_cpus'))
    if not borrowing:
        return
    record = require_cpu_loan(metadata)
    selected = require_opportunistic(record['eligibility'])
    if (eligible is not record['eligibility'] or record['ledger_base'] != ledger.base.resolve()
            or record['demand'] != {k: v for k, v in demand.items() if v} or record['tiers'] != tiers
            or selected['action_key'] != key or record['phase'] != 'decided'):
        raise ValueError('CPU loan grant differs from the reservation')
    current = _read_lending_evidence(record['state_path'])
    last_borrow = _read_lending_evidence(record['state_path'].parent / 'last-borrow.json', optional=True)
    if (record['epoch'] is not _admission_epoch.get()
            or not 0 <= time.time() - metadata['sampled_unix'] <= 5
            or current != record['history'] or not current.get('open')
            or last_borrow.get('sampled_unix', 0) >= metadata['sampled_unix']
            or _lending_holder_binding(ledger) != record['holders']
            or _donor_binding(record['holders'], metadata['borrowable_cpus'],
                              record['state_path'].parent) != record['telemetry']):
        raise CpuLendingChanged('CPU loan grant evidence changed before acquisition')
    if begin:
        record['phase'] = 'acquiring'


def _read_lending_evidence(path, *, optional=False):
    try:
        record = json.loads(path.read_text())
    except FileNotFoundError as exc:
        if optional:
            return {}
        raise CpuLendingChanged('CPU loan grant evidence unavailable') from exc
    except (OSError, ValueError) as exc:
        raise CpuLendingChanged('CPU loan grant evidence unavailable') from exc
    if not isinstance(record, dict):
        raise CpuLendingChanged('CPU loan grant evidence is not an object')
    return record


def _lending_holder_binding(ledger):
    census = ledger.holder_census()
    return {holder.name: census.cpu_record(holder.name).as_record()
            for holder in census.non_mirror_holders}


def _donor_binding(holders, borrowed, base):
    selected = {}
    for key, holder in holders.items():
        if not isinstance(holder, dict):
            continue
        allocation = holder.get('allocation', {})
        if not set(borrowed).intersection(allocation.get('preferred', [])):
            continue
        value = _read_lending_evidence(base / 'telemetry' / (key + '.json'))
        stamp = value.get('sampled_unix')
        if (value.get('complete') is not True or type(stamp) not in (int, float)
                or not math.isfinite(stamp) or not 0 <= time.time() - stamp <= 5):
            raise CpuLendingChanged('CPU loan donor observation changed before acquisition')
        selected[key] = value
    if not selected:
        raise CpuLendingChanged('CPU loan donor disappeared before acquisition')
    return selected


def finish_cpu_acquisition(metadata):
    if metadata and metadata.get('borrowing'):
        record = require_cpu_loan(metadata)
        if record['phase'] != 'acquiring':
            raise ValueError('CPU loan grant was not acquired')
        record['phase'] = 'reserved'


def spend_cpu_loan(metadata):
    record = require_cpu_loan(metadata, current=False)
    if record['phase'] != 'reserved':
        raise ValueError('CPU loan grant was not reserved or was already spent')
    record['phase'] = 'spent'
    return record


def require_cpu_loan(metadata, *, current=True):
    grant = metadata.get('_loan_grant')
    if type(grant) is not CpuLoanGrant or grant not in _loans:
        raise ValueError('unissued CPU loan grant')
    record = _loans[grant]
    if record['pid'] != os.getpid():
        raise ValueError('CPU loan grant belongs to another process')
    if current:
        require_opportunistic(record['eligibility'])
    if current and record['fields'] != {k: metadata.get(k) for k in _LOAN_FIELDS}:
        raise ValueError('CPU loan grant differs from the reservation')
    return record


def selected_cpu_policy_matches(metadata, record):
    if metadata is not None and metadata.get('cpu_qos') == 'opportunistic':
        try:
            return require_opportunistic(metadata.get('_eligibility')) == _qos_inputs(record)
        except ValueError:
            return False
    return True


def require_selected_cpu_policy(metadata, record):
    if not selected_cpu_policy_matches(metadata, record):
        raise ValueError('CPU eligibility differs from winning publication')


def cpu_admission_record(metadata):
    """Live capabilities never enter queue records, holder files, or JSON."""
    return {key: value for key, value in metadata.items()
            if key not in ('_eligibility', '_loan_grant')}

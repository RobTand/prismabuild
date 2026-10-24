"""One synthetic legacy admission trace, shared by task-base and current readers."""
from __future__ import annotations

import inspect
import json
from pathlib import Path
import sys

from prismabuild import adaptive_cpu, pool


def legacy_admission_trace(root, variant):
    clock = [100.]
    adaptive_cpu.time.time = lambda: clock[0]
    adaptive_cpu._sealed_request = lambda item: {
        'task': {'task_class': 'generation'}, 'params': {}}
    tiers = {'preferred': list(range(4)), 'fallback': list(range(4, 8))}
    ledger = pool.ResourceLedger(root / 'reservations', 'fixture-host')
    ledger.ensure_capacity({'cpu': 8, 'mem_gb': 8})
    ledger.configure_cpu_tiers(tiers)
    sample = lambda: dict(
        sampled_unix=clock[0], cpu_count=8, interval_s=1., busy_cpus=.4,
        per_cpu_busy={str(cpu): .1 if cpu < 4 else 0. for cpu in range(8)},
        psi_some=0., psi_avg10=0., psi_avg60=0.)
    options = {'ramp_cap': 0} if 'ramp_cap' in inspect.signature(adaptive_cpu.Controller).parameters else {}
    make = lambda: adaptive_cpu.Controller(ledger, tiers, sampler=sample, **options)
    donor = {'action_key': 'a' * 64, 'priority': 0, 'resources': {'cpu': 4}}
    candidate = {'action_key': 'b' * 64, 'priority': 0,
                 'resources': {'cpu': 5 if variant == 'wide' else 1}}
    controller = make()
    with controller.locked():
        metadata = controller.decision(donor, donor['resources'], identity=('donor', False), owner=None)
        handle = ledger.begin_acquire(donor['action_key'], donor['resources'],
                                      adaptive=metadata, cpu_tiers=tiers)
        assert handle and ledger.commit_acquire(donor['action_key'], handle)
    trace = []
    for at in range(101, 132):
        clock[0] = float(at)
        controller = make()
        adaptive_cpu.write_json(adaptive_cpu.local_telemetry_path(ledger.base, donor['action_key']), {
            'action_key': donor['action_key'], 'complete': True, 'nonce': 'c' * 32,
            'sampled_unix': clock[0], 'wall_seconds': clock[0] - 100,
            'cpu_seconds': (clock[0] - 100) * (4. if variant == 'busy' else .1)})
        identity = (None if variant == 'unknown' else 'candidate', variant == 'measurement')
        with controller.locked():
            metadata = controller.decision(candidate, candidate['resources'], identity=identity, owner=None)
            trace.append({'admitted': metadata is not None,
                          'reason': controller.last_decision['reason'],
                          **{name: (metadata or {}).get(name) for name in
                             ('preferred_borrow', 'borrowing', 'cost')}})
            if at == 131 and metadata is not None:
                handle = ledger.begin_acquire(candidate['action_key'], candidate['resources'],
                                              adaptive=metadata, cpu_tiers=tiers)
                assert handle and ledger.commit_acquire(candidate['action_key'], handle)
                controller.admitted(metadata)
    admitted = trace[-1]['admitted']
    return {'trace': trace, 'allocation': ledger.cpu_allocation(candidate['action_key'], tiers)
            if admitted else None, 'tokens': ledger.holder_tokens(candidate['action_key'])
            if admitted else {}, 'free': ledger.available()}


if __name__ == '__main__':
    print(json.dumps(legacy_admission_trace(Path(sys.argv[1]), sys.argv[2]), sort_keys=True))

"""PB #2156 review r2: paired cap-0 fixtures, run once per source tree.

Usage: python review_r2_cap0_fixture.py SCRATCH SCENARIO
PYTHONPATH selects the task-base or head source. Prints one JSON document.
Nothing live: every ledger, queue and state root is under SCRATCH.
"""
from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path

from prismabuild import adaptive_cpu, pool

REAL_START = adaptive_cpu.time.time()
CLOCK = [100.0]
adaptive_cpu.time.time = lambda: CLOCK[0]  # the shared ``time`` module
HEAD = hasattr(pool.ResourceLedger, 'cpu_token_selection')

X8 = {'preferred': [0, 1, 2, 3], 'fallback': [4, 5, 6, 7]}
X16 = {'preferred': list(range(8)), 'fallback': list(range(8, 16))}
SPARK = {'preferred': [5, 6, 7, 8, 9, 15, 16, 17, 18, 19], 'fallback': []}


def key(ch):
    return ch * 64


class Host:
    def __init__(self, root, tiers, capacity):
        self.tiers = tiers
        self.cpus = list(tiers['preferred']) + list(tiers['fallback'])
        self.ledger = pool.ResourceLedger(root / 'reservations', 'fixture-host')
        self.ledger.ensure_capacity(capacity)
        self.ledger.configure_cpu_tiers(tiers)
        # Distinct idle readings make every busy-sorted order deterministic.
        self.busy = {cpu: .01 + .001 * index for index, cpu in enumerate(self.cpus)}
        self.psi = 0.
        self.lag = 0.
        self.foreign = None
        self.running = {}

    def sample(self):
        per = {str(cpu): self.busy[cpu] for cpu in self.cpus}
        value = dict(sampled_unix=CLOCK[0] - self.lag, cpu_count=len(self.cpus), interval_s=1.,
                     busy_cpus=sum(per.values()), per_cpu_busy=per, psi_some=self.psi,
                     psi_avg10=0., psi_avg60=0.)
        if self.foreign is not None:
            value['foreign_per_cpu_busy'] = {str(cpu): self.foreign.get(cpu, per[str(cpu)])
                                             for cpu in self.cpus}
        return value

    def controller(self):
        return adaptive_cpu.Controller(self.ledger, self.tiers, sampler=self.sample)

    def telemetry(self, action_key):
        rate, since = self.running[action_key]
        adaptive_cpu.write_json(adaptive_cpu.local_telemetry_path(self.ledger.base, action_key), {
            'action_key': action_key, 'complete': True, 'nonce': action_key[:32],
            'sampled_unix': CLOCK[0], 'wall_seconds': CLOCK[0] - since,
            'cpu_seconds': (CLOCK[0] - since) * rate})

    def tick(self, seconds=1.):
        CLOCK[0] += seconds
        for action_key in self.running:
            self.telemetry(action_key)

    def set_busy(self, cpus, value):
        for cpu in cpus:
            self.busy[cpu] = value

    def counts(self):
        free = self.ledger.available().get('cpu', 0)
        projected = (self.ledger.admission_available(self.tiers).get('cpu', 0)
                     if HEAD else free)
        return {'free_tokens': free, 'admission_cpu': projected}

    def release(self, action_key):
        self.running.pop(action_key, None)
        self.ledger.release(action_key)


def allocation_of(host, action_key):
    allocation = host.ledger.cpu_allocation(action_key, host.tiers)
    physical = set(host.ledger.cpu_ids(
        sorted((host.ledger.held_dir / action_key).glob('cpu-*')), host.tiers))
    assigned = set(allocation['preferred'] + allocation['fallback'])
    return {'allocation': {tier: list(allocation[tier]) for tier in ('preferred', 'fallback')},
            'borrowed': sorted(assigned - physical)}


def decide(host, action_key, cpu, *, acquire=True, measurement=False, owner=None,
           allowance=None, between=None, rate=.1, mem=0, busy=None):
    resources = {'cpu': cpu, **({'mem_gb': mem} if mem else {})}
    item = {'action_key': action_key, 'priority': 0, 'resources': dict(resources)}
    controller = host.controller()
    out = {}
    with controller.locked():
        metadata = controller.decision(item, dict(resources),
                                       identity=('shape-' + action_key[0], measurement),
                                       owner=owner, allowance=allowance)
        decision = controller.last_decision
        out['admitted'] = metadata is not None
        out['reason'] = decision.get('reason')
        for field in ('cpus', 'held_cpus', 'foreign_cpus', 'holder'):
            if field in decision:
                out['refusal_' + field] = decision[field]
        if metadata is not None:
            out['borrowing'] = bool(metadata.get('borrowing'))
            out['preferred_borrow'] = metadata.get('preferred_borrow')
            out['funded_by'] = metadata.get('funded_by')
        if metadata is None or not acquire:
            return out
        if between is not None:
            between()
        handle = host.ledger.begin_acquire(action_key, dict(resources), adaptive=metadata,
                                           cpu_tiers=host.tiers)
        out['acquired'] = handle is not None
        if handle is None:
            out['shortage_resource'] = (host.ledger.last_token_shortage or {}).get('resource')
            return out
        host.ledger.commit_acquire(action_key, handle)
        controller.admitted(metadata)
    out.update(allocation_of(host, action_key))
    host.running[action_key] = (rate, CLOCK[0])
    if busy is not None:
        assigned = out['allocation']['preferred'] + out['allocation']['fallback']
        host.set_busy(assigned, busy)
    return out


def static_holder(host, action_key, cpu):
    handle = host.ledger.begin_acquire(action_key, {'cpu': cpu})
    assert handle and host.ledger.commit_acquire(action_key, handle)
    return allocation_of(host, action_key)


def warm(host, steps, seconds=3):
    """Feed every holder's telemetry through ordinary non-acquiring decisions."""
    for _ in range(seconds):
        host.tick()
        steps.append(('warm', decide(host, key('9'), 1, acquire=False)))


def s01_x86_free_preferred(root):
    host, steps = Host(root, X8, {'cpu': 8, 'mem_gb': 16}), []
    steps.append(('donor', decide(host, key('a'), 2, busy=.05)))
    warm(host, steps)
    steps.append(('candidate', decide(host, key('b'), 1)))
    return host, steps


def s02_x86_preferred_exhausted(root):
    host, steps = Host(root, X8, {'cpu': 8, 'mem_gb': 16}), []
    steps.append(('donor', decide(host, key('a'), 4, busy=.03)))
    warm(host, steps)
    steps.append(('candidate', decide(host, key('b'), 2)))
    return host, steps


def s03_x86_fallback_only_donor(root):
    host, steps = Host(root, X8, {'cpu': 8, 'mem_gb': 16}), []
    steps.append(('busy_preferred', decide(host, key('a'), 4, rate=3.6, busy=.9)))
    warm(host, steps)  # the busy holder's cost is learned, so pending cost clears
    steps.append(('smt_donor', decide(host, key('d'), 3, rate=.1, busy=.03)))
    warm(host, steps)
    steps.append(('counts', host.counts()))
    steps.append(('candidate', decide(host, key('b'), 3)))
    return host, steps


def s18_spark_near_full_two_donors(root):
    host, steps = Host(root, SPARK, {'cpu': 10, 'mem_gb': 16}), []
    steps.append(('busy', decide(host, key('a'), 4, rate=3.6, busy=.9)))
    warm(host, steps)
    steps.append(('donor', decide(host, key('d'), 5, rate=.2, busy=.03)))
    warm(host, steps)
    steps.append(('counts', host.counts()))
    steps.append(('candidate', decide(host, key('b'), 3)))
    host.tick()
    steps.append(('second', decide(host, key('c'), 2)))
    return host, steps


def s19_x86_smt_donor_beside_preferred_donor(root):
    host, steps = Host(root, X8, {'cpu': 8, 'mem_gb': 16}), []
    steps.append(('preferred_donor', decide(host, key('a'), 4, rate=.1, busy=.03)))
    warm(host, steps)
    steps.append(('smt_donor', decide(host, key('d'), 3, rate=.1, busy=.03)))
    warm(host, steps)
    steps.append(('counts', host.counts()))
    steps.append(('candidate', decide(host, key('b'), 4)))
    return host, steps


def s04_x86_mixed_tier_donor(root):
    host, steps = Host(root, X8, {'cpu': 8, 'mem_gb': 16}), []
    steps.append(('busy_preferred', decide(host, key('a'), 2, rate=1.8, busy=.9)))
    steps.append(('mixed_donor', decide(host, key('d'), 4, rate=.1, busy=.03)))
    warm(host, steps)
    steps.append(('candidate', decide(host, key('b'), 3)))
    return host, steps


def s05_spark_pcore_borrow(root):
    host, steps = Host(root, SPARK, {'cpu': 10, 'mem_gb': 16}), []
    steps.append(('donor', decide(host, key('a'), 8, rate=.2, busy=.03)))
    warm(host, steps)
    steps.append(('candidate', decide(host, key('b'), 4)))
    return host, steps


def _spark_orphan(root):
    host, steps = s05_spark_pcore_borrow(root)
    warm(host, steps, 2)
    host.release(key('a'))
    host.tick()
    steps.append(('counts_after_donor_exit', host.counts()))
    warm(host, steps, 2)
    return host, steps


def s06_spark_orphan_one_cpu(root):
    host, steps = _spark_orphan(root)
    steps.append(('next_one_cpu', decide(host, key('e'), 1)))
    return host, steps


def s07_spark_orphan_eight_cpu(root):
    host, steps = _spark_orphan(root)
    steps.append(('next_eight_cpu', decide(host, key('e'), 8)))
    return host, steps


def s08_x86_second_borrower_same_cpu(root):
    host, steps = Host(root, X8, {'cpu': 8, 'mem_gb': 16}), []
    steps.append(('donor', decide(host, key('a'), 4, rate=.1, busy=.03)))
    host.set_busy([0], .001)
    warm(host, steps)
    steps.append(('borrower_one', decide(host, key('b'), 1, rate=.05)))
    warm(host, steps)
    steps.append(('borrower_two', decide(host, key('c'), 1)))
    return host, steps


def s09_x86_telemetry_tick_before_acquire(root):
    host, steps = Host(root, X8, {'cpu': 8, 'mem_gb': 16}), []
    steps.append(('donor', decide(host, key('a'), 4, busy=.03)))
    warm(host, steps)

    def donor_sampler_ticks():
        # The donor's attempt rewrites its host-local telemetry every <= 2 s.
        CLOCK[0] += .4
        host.telemetry(key('a'))
    steps.append(('candidate', decide(host, key('b'), 2, between=donor_sampler_ticks)))
    return host, steps


def s10_x86_unrelated_release_before_acquire(root):
    host, steps = Host(root, X8, {'cpu': 8, 'mem_gb': 16}), []
    steps.append(('donor', decide(host, key('a'), 4, busy=.03)))
    steps.append(('unrelated', decide(host, key('x'), 1, rate=.9, busy=.9)))
    warm(host, steps)

    def sibling_loop_finishes():
        host.release(key('x'))
    steps.append(('candidate', decide(host, key('b'), 2, between=sibling_loop_finishes)))
    return host, steps


def s11_x16_near_full(root):
    host, steps = Host(root, X16, {'cpu': 16, 'mem_gb': 32}), []
    steps.append(('busy', decide(host, key('a'), 6, rate=5.4, busy=.9)))
    steps.append(('donor', decide(host, key('d'), 6, rate=.2, busy=.03)))
    warm(host, steps)
    steps.append(('counts', host.counts()))
    steps.append(('candidate', decide(host, key('b'), 5)))
    host.tick()
    steps.append(('second', decide(host, key('c'), 2)))
    return host, steps


def s12_mixed_static_and_adaptive(root):
    host, steps = Host(root, X8, {'cpu': 8, 'mem_gb': 16}), []
    steps.append(('static', static_holder(host, key('s'), 2)))
    host.running[key('s')] = (.1, CLOCK[0])
    steps.append(('donor', decide(host, key('a'), 4, busy=.03)))
    warm(host, steps)
    steps.append(('candidate', decide(host, key('b'), 3)))
    host.tick()
    steps.append(('second', decide(host, key('c'), 1)))
    return host, steps


def s13_mixed_producer_and_dependent(root):
    host, steps = Host(root, X8, {'cpu': 8, 'mem_gb': 16}), []
    allowance = {'cpu': 1, 'mem_gb': 1, 'slots': 1, 'basis': 'default'}
    steps.append(('producer', decide(host, key('p'), 4, mem=2, allowance=allowance, busy=.03)))
    warm(host, steps)
    steps.append(('dependent', decide(host, key('q'), 1, mem=1, owner=key('p'))))
    steps.append(('donor', decide(host, key('a'), 3, busy=.03)))
    warm(host, steps)
    steps.append(('candidate', decide(host, key('b'), 2)))
    return host, steps


def s14_x86_pressure_attribution(root):
    host, steps = Host(root, X8, {'cpu': 8, 'mem_gb': 16}), []
    steps.append(('busy_preferred', decide(host, key('a'), 4, rate=3.6, busy=.9)))
    warm(host, steps)
    host.psi = .2
    host.foreign = {cpu: 0. for cpu in host.cpus}
    host.foreign.update({6: .5})
    steps.append(('fits_idle_smt', decide(host, key('b'), 2)))
    host.tick()
    steps.append(('needs_busy_smt', decide(host, key('c'), 2)))
    return host, steps


def s15_spark_orphan_under_pressure(root):
    host, steps = _spark_orphan(root)
    host.psi = .2
    host.foreign = {cpu: 0. for cpu in host.cpus}
    steps.append(('pressure_four', decide(host, key('e'), 4)))
    host.tick()
    steps.append(('pressure_seven', decide(host, key('f'), 7)))
    return host, steps


def s16_x86_sample_ages_before_acquire(root):
    host, steps = Host(root, X8, {'cpu': 8, 'mem_gb': 16}), []
    steps.append(('donor', decide(host, key('a'), 4, busy=.03)))
    warm(host, steps)
    host.lag = 4.7  # a cached pass sample, still within MAX_SAMPLE_AGE_S

    def gpu_decision_takes_half_a_second():
        CLOCK[0] += .5
    steps.append(('candidate', decide(host, key('b'), 2, between=gpu_decision_takes_half_a_second)))
    return host, steps


def s17_preupgrade_holder_without_metadata(root):
    host, steps = Host(root, X8, {'cpu': 8, 'mem_gb': 16}), []
    steps.append(('donor', decide(host, key('a'), 4, busy=.03)))
    # A main-era static holder: physical tokens, no CPU metadata file.
    old = host.ledger.held_dir / key('o')
    old.mkdir(parents=True)
    for token in sorted(host.ledger.free_dir.glob('cpu-*'))[:1]:
        token.rename(old / token.name)
    warm(host, steps)
    steps.append(('counts', host.counts()))
    steps.append(('candidate', decide(host, key('b'), 1)))
    return host, steps


def s20_multislot_gpu_row_borrows_cpu(root):
    host, steps = Host(root, X8, {'cpu': 8, 'mem_gb': 16, 'gpu': 1}), []
    steps.append(('donor', decide(host, key('a'), 4, busy=.03)))
    warm(host, steps)
    item = {'action_key': key('g'), 'priority': 0, 'resources': {'cpu': 2, 'gpu': 2}}
    controller = host.controller()
    out = {}
    with controller.locked():
        metadata = controller.decision(item, {'cpu': 2, 'gpu': 2}, identity=('shape-g', False), owner=None)
        out.update(admitted=metadata is not None, reason=controller.last_decision.get('reason'),
                   borrowing=bool((metadata or {}).get('borrowing')))
        if metadata is not None:
            # The claim reserves one physical GPU for a historical slot count
            # (``PoolQueue._reservation_demand``) and passes that to begin_acquire.
            try:
                handle = host.ledger.begin_acquire(key('g'), {'cpu': 2, 'gpu': 1}, adaptive=metadata,
                                                   cpu_tiers=host.tiers)
                out['acquired'] = handle is not None
                if handle is not None:
                    host.ledger.commit_acquire(key('g'), handle)
                    controller.admitted(metadata)
            except Exception as exc:
                out['acquire_error'] = type(exc).__name__ + ': ' + str(exc)[:80]
    if out.get('acquired'):
        out.update(allocation_of(host, key('g')))
    steps.append(('multislot_gpu_row', out))
    return host, steps


# --- Queue level: #1929 guest, legacy loan, donor exit, then a GPU row -------

def _queue_gpu_row_after_loan(root, adaptive_gpu_claim):
    from prismabuild import adaptive_gpu
    CLOCK[0] = float(int(REAL_START))  # queue records and offers use wall time
    host_name = 'sparky'
    current = [host_name]
    pool.socket.gethostname = lambda: current[0]
    adaptive_cpu.action_identity = lambda row: ('shape', False)
    adaptive_gpu.action_contract = lambda item, demand: (
        'shape', False, False, int(demand.get('mem_gb', 1)) * adaptive_gpu.GIB)
    tiers = {'preferred': list(range(8)), 'fallback': []}
    capacity = {'cpu': 8, 'mem_gb': 32, 'gpu': 1}
    queue = pool.PoolQueue(root / 'queue')

    def detail():
        return {'observed_unix': CLOCK[0], 'load1': 0., 'gpu_power_fraction': .05,
                'gpu_power_sampled_unix': CLOCK[0], 'gpu_capacity_trusted': True,
                'gpu_sampled_unix': CLOCK[0], 'physical_gpus': 1, 'foreign_gpu_processes': 0}

    def announce():
        for name, gpu in ((host_name, True), ('cpuhost', False)):
            cap = dict(capacity) if gpu else {'cpu': 8, 'mem_gb': 32}
            current[0] = name
            queue.announce(host=name, tags=[name, 'gb10' if gpu else 'x86'], has_gpu=gpu,
                           capacity=cap, observed_capacity=cap, cpu_tiers=tiers,
                           observed_detail=detail())
        current[0] = host_name
    for name, gpu in ((host_name, True), ('cpuhost', False)):
        ledger_n = queue.ledger(name)
        ledger_n.configure_cpu_tiers(tiers)
        ledger_n.ensure_capacity(dict(capacity) if gpu else {'cpu': 8, 'mem_gb': 32})
    announce()
    # As in the #1929 fixture: the CPU host is full, so CPU rows become guests.
    assert queue.ledger('cpuhost').acquire('f' * 64, {'cpu': 8})
    ledger = queue.ledger(host_name)
    busy = {cpu: .01 + .001 * cpu for cpu in range(8)}
    running = {}

    def cpu_sample():
        per = {str(cpu): busy[cpu] for cpu in range(8)}
        return dict(sampled_unix=CLOCK[0], cpu_count=8, interval_s=1., busy_cpus=sum(per.values()),
                    per_cpu_busy=per, psi_some=0., psi_avg10=0., psi_avg60=0.)

    def gpu_sample():
        return {'schema': 'prismabuild.gpu_capacity.v1', 'sample_id': str(CLOCK[0]),
                'sampled_unix': CLOCK[0], 'complete': True, 'attributed': True,
                'devices': [{'uuid': 'GPU-1', 'name': 'NVIDIA GB10', 'power_w': 15.,
                             'power_limit_w': None, 'power_reference_w': 140.,
                             'power_reference_scope': 'soc_tdp',
                             'memory_domain': 'shared_system', 'limited': False}],
                'host_total_bytes': 128 * adaptive_gpu.GIB,
                'host_available_bytes': 100 * adaptive_gpu.GIB,
                'memory_pressure_some': 0., 'memory_pressure_full': 0.,
                'cpu_pressure_some': 0., 'foreign_processes': [],
                'jobs': [{'action_key': k, 'nonce': k[:32], 'scope_id': k[:8] + '-scope',
                          'complete': True} for k in sorted(running)]}

    def tick():
        CLOCK[0] += 1.
        for k, since in running.items():
            adaptive_cpu.write_json(adaptive_cpu.local_telemetry_path(ledger.base, k), {
                'action_key': k, 'complete': True, 'nonce': k[:32], 'scope_unit': k[:8] + '-scope',
                'sampled_unix': CLOCK[0], 'wall_seconds': CLOCK[0] - since,
                'cpu_seconds': (CLOCK[0] - since) * .1})
        announce()

    def publish(ch, cpu, gpu=False, priority=0):
        queue.publish(action_key=key(ch), cas_root='/cas', checkout_root='/co',
                      worker_script='/w.py',
                      resources={'cpu': cpu, 'mem_gb': cpu, **({'gpu': 1} if gpu else {})},
                      retry_safe=True, max_attempts=3, priority=priority)

    def claim(adaptive=True):
        extra = (dict(adaptive_cpu=True, cpu_sampler=cpu_sample, gpu_sampler=gpu_sample)
                 if adaptive else {})
        return queue.claim(tags=[host_name, 'gb10'], has_gpu=True, capacity=capacity,
                           cpu_tiers=tiers, **extra)

    def claimed_view(row):
        if row is None:
            return None
        return {'key': row['action_key'][0], 'cpu_guest': bool(row.get('cpu_guest')),
                'cpu_allocation': row.get('cpu_allocation')}

    steps = []
    tick()
    publish('a', 4, priority=20)
    donor = claim()
    steps.append(('donor', claimed_view(donor)))
    if donor is None:
        return None, steps
    running[key('a')] = CLOCK[0]
    publish('b', 6, priority=20)
    borrower = None
    for attempt in range(6):
        tick()
        borrower = claim()
        steps.append((f'borrower_pass_{attempt}', claimed_view(borrower)))
        if borrower is not None:
            break
    if borrower is None:
        return None, steps
    running[key('b')] = CLOCK[0]
    view = type('View', (), {'ledger': ledger, 'tiers': tiers})()
    steps.append(('borrower_holder', allocation_of(view, key('b'))))
    tick()
    queue.finish(key('a'), status='executed', detail={'returncode': 0}, claim_snapshot=donor)
    running.pop(key('a'))
    tick()
    steps.append(('counts_after_donor_exit', {
        'free_tokens': ledger.available().get('cpu', 0),
        'admission_cpu': (ledger.admission_available(tiers).get('cpu', 0) if HEAD
                          else ledger.available().get('cpu', 0))}))
    publish('g', 4, gpu=True, priority=-10)
    for attempt in range(3):
        tick()
        row = claim(adaptive=adaptive_gpu_claim)
        stopped = [str(r.get('preempted_by', ''))[:1] for _, r in
                   queue.records.withdrawal_decisions(key('b'))]
        denials = queue.latest_denials({key('g')}).get(key('g'), [])
        reasons = sorted({str(d.get('reason')) + ':' + str((d.get('evidence') or {}).get('decision', {}).get('reason'))
                          for d in denials if d.get('host') == host_name})
        result = {'claimed': claimed_view(row), 'borrower_stopped_by': stopped,
                  'gpu_denial_reasons': reasons}
        if row is not None:
            result['gpu_row_holder'] = allocation_of(view, key('g'))
        steps.append((f'gpu_pass_{attempt}', result))
        if row is not None:
            break
    return None, steps


def q01_spark_loan_then_adaptive_gpu_row(root):
    return _queue_gpu_row_after_loan(root, True)


def q02_spark_guest_loan_then_static_gpu_row(root):
    return _queue_gpu_row_after_loan(root, False)


SCENARIOS = {name: fn for name, fn in globals().items()
             if callable(fn) and (name.startswith('s') or name.startswith('q'))
             and name[1:3].isdigit()}


def run(root, name):
    steps = []
    try:
        host, steps = SCENARIOS[name](root)
        if host is not None:
            steps.append(('final_counts', host.counts()))
    except Exception as exc:  # report the same way on both trees
        steps.append(('error', {'type': type(exc).__name__, 'message': str(exc)[:160],
                                'where': traceback.format_exc().strip().splitlines()[-3:-1]}))
    return [[label, value] for label, value in steps]


if __name__ == '__main__':
    scratch = Path(sys.argv[1])
    scratch.mkdir(parents=True, exist_ok=True)
    print(json.dumps({'head': HEAD, 'source': str(Path(pool.__file__).parents[1]),
                      'steps': run(scratch, sys.argv[2])}, sort_keys=True, default=str))

"""Host CPU admission from measured headroom and attributed action consumption.

CPU affinity tokens remain physical. A reservation may borrow an occupied CPU
only after fresh, complete attribution proves idle demand; memory/GPU tokens
are never discounted. The local lock serializes budget decisions, not queue
ownership (which still uses NFS rename).
"""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import socket
import stat
import time

MAX_SAMPLE_AGE_S = 5.0
MIN_INTERVAL_S = 1.0
MAX_INTERVAL_S = 60.0
MAX_ACTIONS = 256
METADATA = '.adaptive.json'


def read_json(path):
    try:
        value = json.loads(Path(path).read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def write_json(path, value):
    from .materialize import _write_json_atomic
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(Path(path), value)


def action_identity(item):
    """Exact executable shape; variable inputs still distinguish unlike work.

    Action keys themselves include result names and therefore do not generalize
    over repetitions. Code, argv, environment, input dimensions and demand do.
    Unknown/custom launchers have no shape and may never borrow.
    """
    from . import core
    key = str(item['action_key'])
    raw = read_json(Path(str(item['cas_root'])) / 'requests' / key[:2] / f'{key}.json')
    if not raw:
        return None, False
    try:
        action = core.validate_action(raw)
    except (ValueError, TypeError, KeyError):
        return None, False
    if action['action_key'] != key:
        return None, False
    # Exclude only destination/provenance, never normalize arbitrary command
    # arguments or caller parameters into an unrelated workload.
    shape = {name: action.get(name) for name in ('task', 'code_closure', 'inputs', 'environment')}
    task = dict(shape['task'])
    task.pop('result_path', None)
    shape['task'] = task
    shape['resources'] = item.get('resources')
    shape['params'] = action.get('params')
    if task.get('definition_id') == 'fleet/pbrun':
        params = action.get('params', {})
        snapshot = params.get('checkout_snapshot', {})
        files = action.get('code_closure', {}).get('files', [])
        # pbrun's stamp CONTENT is the full checkout identity (including dirty
        # bytes). Its generated filename and bundle/result names include the
        # command's bookkeeping fingerprint, which is not workload shape.
        if (isinstance(params.get('command'), list) and files
                and snapshot.get('schema') == 'prismaquant.prismabuild.pbrun_checkout_snapshot.v2'
                and all(str(f.get('path', '')).startswith('.pbrun-closure.') for f in files)):
            environment = dict(action['environment'])
            environment['variables'] = {k: v for k, v in environment['variables'].items()
                                        if k not in ('PRISMABUILD_CONTAINER_OWNER',
                                                     'PRISMABUILD_CONTAINER_MARKER')}
            shape = {'task': {k: v for k, v in task.items() if k != 'argv'},
                     'command': params['command'], 'cwd': params.get('cwd'),
                     'resources': item.get('resources'), 'environment': environment,
                     'code': [{k: f[k] for k in ('sha256', 'bytes')} for f in files],
                     'inputs': [i for i in action['inputs'] if i['id'] != 'pbrun.checkout-snapshot'],
                     'params': {k: v for k, v in params.items()
                                if k not in ('command', 'cwd', 'checkout_snapshot')}}
    digest = hashlib.sha256(json.dumps(shape, sort_keys=True).encode()).hexdigest()
    return digest, task.get('task_class') == 'measurement'


def shape_key(item):
    return action_identity(item)[0]


def counters(cpus):
    """Per-CPU busy/total jiffies: guest time is already included in user."""
    values = {}
    try:
        for line in Path('/proc/stat').read_text().splitlines():
            fields = line.split()
            if not fields or not fields[0].startswith('cpu') or not fields[0][3:].isdigit():
                continue
            cpu = int(fields[0][3:])
            if cpu not in cpus:
                continue
            ticks = [int(x) for x in fields[1:9]]
            if len(ticks) != 8:
                return None
            total = sum(ticks)
            values[str(cpu)] = [total - ticks[3] - ticks[4], total]
        psi = next(line for line in Path('/proc/pressure/cpu').read_text().splitlines()
                   if line.startswith('some '))
        pressure = int(dict(part.split('=') for part in psi.split()[1:])['total'])
    except (OSError, ValueError, StopIteration):
        return None
    if len(values) != len(cpus):
        return None
    return {'cpus': values, 'psi_total': pressure, 'sampled_unix': time.time()}


class Controller:
    def __init__(self, ledger, tiers):
        self.ledger = ledger
        self.tiers = tiers
        self.cpus = list(tiers['preferred']) + list(tiers['fallback'])
        self.base = ledger.base / 'adaptive'
        self._host_sample = None

    @contextmanager
    def locked(self):
        # Never unlink: two generations must not lock different inodes. The
        # private directory and O_NOFOLLOW prevent another uid redirecting it.
        directory = Path('/tmp') / f'prismabuild-admission-{os.getuid()}'
        directory.mkdir(mode=0o700, exist_ok=True)
        info = directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise RuntimeError('unsafe PrismaBuild admission lock directory')
        identity = f'{self.ledger.base.resolve()}:{socket.gethostname()}'
        name = hashlib.sha256(identity.encode()).hexdigest() + '.lock'
        descriptor = os.open(directory / name, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
                raise RuntimeError('unsafe PrismaBuild admission lock file')
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            os.close(descriptor)

    def sample(self):
        current = counters(set(self.cpus))
        if current is None:
            return {}
        path = self.base / 'cpu-sample.json'
        previous = read_json(path)
        elapsed = current['sampled_unix'] - previous.get('sampled_unix', 0)
        if 0 <= elapsed < MIN_INTERVAL_S:
            return previous.get('observation', {})
        observation = {}
        if MIN_INTERVAL_S <= elapsed <= MAX_INTERVAL_S and previous.get('cpus', {}).keys() == current['cpus'].keys():
            deltas = [(value[0] - previous['cpus'][key][0],
                       value[1] - previous['cpus'][key][1]) for key, value in current['cpus'].items()]
            psi_delta = current['psi_total'] - previous.get('psi_total', current['psi_total'])
            if all(0 <= busy <= total and total > 0 for busy, total in deltas) and psi_delta >= 0:
                observation = {'sampled_unix': current['sampled_unix'],
                               'busy_cpus': sum(busy / total for busy, total in deltas),
                               'psi_some': min(1., psi_delta / (elapsed * 1e6)),
                               'cpu_count': len(self.cpus), 'interval_s': elapsed,
                               'per_cpu_busy': {key: busy / total for key, (busy, total)
                                                in zip(current['cpus'], deltas)}}
        current['observation'] = observation
        write_json(path, current)
        return observation

    def decision(self, item, demand):
        """Return reservation metadata, or None when admission must wait."""
        if self._host_sample is None:
            self._host_sample = self.sample()
        sample = self._host_sample
        now = time.time()
        fresh = (0 <= now - sample.get('sampled_unix', 0) <= MAX_SAMPLE_AGE_S
                 and sample.get('cpu_count') == len(self.cpus)
                 and all(isinstance(sample.get(key), (float, int)) and math.isfinite(sample[key])
                         for key in ('busy_cpus', 'psi_some', 'interval_s')))
        if fresh and (sample['psi_some'] >= .10 or sample['busy_cpus'] >= .95 * len(self.cpus)):
            return None
        shape, measurement = action_identity(item)
        unbounded_cpu = not int(demand.get('cpu', 0))
        if measurement and (not fresh or sample['busy_cpus'] > .05 * len(self.cpus)):
            return None
        holders = [p for p in self.ledger.held_dir.iterdir() if p.is_dir()]
        if len(holders) >= MAX_ACTIONS:
            return None
        # Legacy producers sometimes reserved only GPU/memory. Their children
        # inherit the whole worker affinity, so zero tokens are unknown CPU
        # use, never evidence of zero use. Keep that historical demand intact
        # but serialize it on a freshly idle host until the producer declares
        # an enforceable CPU allocation.
        if unbounded_cpu and (holders or not fresh or sample['busy_cpus'] > .05 * len(self.cpus)):
            return None
        profiles = read_json(self.base / 'profiles.json')
        recent = read_json(self.base / 'jobs.json')
        next_recent = {}
        pending = 0.
        lendable = False
        lending_cpus = set()
        protected_cpus = set()
        active_cost = 0.
        for holder in holders:
            meta = read_json(holder / METADATA)
            physical = len(list(holder.glob('cpu-*')))
            reserved = meta.get('declared_cpu', physical)
            if not reserved:
                if meta or any(holder.iterdir()):
                    return None
                continue
            if measurement or meta.get('measurement'):
                return None
            record = read_json(self.ledger.base / 'telemetry' / f'{holder.name}.json')
            valid = (record.get('complete') is True
                     and 0 <= now - record.get('sampled_unix', 0) <= MAX_SAMPLE_AGE_S
                     and record.get('sampled_unix', 0) >= meta.get('admitted_unix', now)
                     and record.get('action_key', holder.name) == holder.name
                     and all(type(record.get(k)) in (int, float) and math.isfinite(record[k]) and record[k] >= 0
                             for k in ('cpu_seconds', 'wall_seconds')))
            previous = recent.get(holder.name, {})
            if (previous.get('nonce') != record.get('nonce')
                    or previous.get('sampled_unix', 0) < meta.get('admitted_unix', now)):
                previous = {}
            cpu = None
            if valid:
                wall_delta = record['wall_seconds'] - previous.get('wall_seconds', record['wall_seconds'])
                cpu_delta = record['cpu_seconds'] - previous.get('cpu_seconds', record['cpu_seconds'])
                if MIN_INTERVAL_S <= wall_delta <= MAX_INTERVAL_S and cpu_delta >= 0:
                    cpu = cpu_delta / wall_delta
                    record['_cpu'] = cpu
                    next_recent[holder.name] = record
                    if meta.get('shape'):
                        old = profiles.get(meta['shape'], {})
                        # Decay slowly; increases apply immediately. A changed
                        # phase must promptly undo a previous cheap estimate.
                        profiles[meta['shape']] = {**old, 'cpu': max(cpu * 1.25, old.get('cpu', cpu) * .98),
                                                   'sampled_unix': now,
                                                   'samples': min(1000, old.get('samples', 0) + 1),
                                                   'memory_peak_bytes': max(record.get('memory_peak_bytes', 0),
                                                                            old.get('memory_peak_bytes', 0))}
                elif 0 <= wall_delta < MIN_INTERVAL_S and previous:
                    next_recent[holder.name] = previous
                    if 0 <= now - previous.get('sampled_unix', 0) <= MAX_SAMPLE_AGE_S:
                        cpu = previous.get('_cpu')
                else:
                    next_recent[holder.name] = record
            cost = float(reserved) if cpu is None else max(.05, cpu * 1.25)
            allocation = self.ledger.cpu_allocation(holder.name, self.tiers)
            assigned = set(allocation['preferred'] + allocation['fallback'])
            if cpu is not None and meta.get('shape') and cost < reserved:
                lending_cpus.update(assigned)
            else:
                protected_cpus.update(assigned)
            active_cost += cost
            # Host busy already includes samples ending before this reading.
            # Charge startup/unknown attribution in full to avoid spending the
            # same headroom in concurrent admissions.
            if cpu is None:
                pending += cost
            elif meta.get('shape') and not meta.get('measurement') and cost < reserved:
                lendable = True
        write_json(self.base / 'jobs.json', next_recent)
        profiles = dict(sorted(profiles.items(), key=lambda x: x[1].get('sampled_unix', 0))[-512:])
        write_json(self.base / 'profiles.json', profiles)
        declared = int(demand.get('cpu', 0))
        learned = profiles.get(shape, {}) if shape else {}
        learned_valid = (learned.get('samples', 0) >= 3
                         and 0 <= now - learned.get('sampled_unix', 0) < 86400)
        cost = (float(len(self.cpus)) if unbounded_cpu else
                max(.05, min(float(declared), learned['cpu'])) if learned_valid else float(declared))
        # A solitary full-width reservation must not wait forever for every
        # background daemon to consume exactly zero CPU. The fresh idle-host
        # test permits only incidental activity, never real foreign load or a
        # competing reservation; PSI pressure was refused before this point.
        full_width_idle = (fresh and not holders and declared == len(self.cpus)
                           and sample['busy_cpus'] <= .05 * len(self.cpus))
        # Unbounded legacy work already proved the same exclusive idle host.
        if (fresh and not unbounded_cpu and not full_width_idle
                and max(sample['busy_cpus'] + pending, active_cost) + cost > len(self.cpus) + .01):
            return None
        available = self.ledger.available().get('cpu', 0)
        borrowable = lending_cpus - protected_cpus
        can_borrow = fresh and shape and not measurement and lendable
        preferred_borrow = (min(max(0, declared - self.ledger.free_preferred(self.tiers)),
                                len(borrowable & set(self.tiers['preferred'])))
                            if can_borrow else 0)
        borrowing = available < declared or preferred_borrow > 0
        if borrowing:
            last = read_json(self.base / 'last-borrow.json').get('sampled_unix', 0)
            if (not fresh or not shape or measurement or not lendable
                    or len(borrowable) < declared - available
                    or sample['sampled_unix'] <= last):
                return None
        return {'declared_cpu': declared, 'cost': cost, 'shape': shape,
                'unbounded_cpu': unbounded_cpu,
                'measurement': measurement, 'admitted_unix': now,
                'preferred_borrow': preferred_borrow,
                'sampled_unix': sample.get('sampled_unix', 0), 'borrowing': borrowing,
                'host_busy_cpus': sample.get('busy_cpus'), 'host_psi_some': sample.get('psi_some'),
                'active_cpu_cost': active_cost, 'pending_cpu_cost': pending,
                'borrowable_cpus': sorted(borrowable, key=lambda c: sample.get('per_cpu_busy', {}).get(str(c), 0.))}

    def admitted(self, metadata):
        if metadata.get('borrowing'):
            write_json(self.base / 'last-borrow.json', {'sampled_unix': metadata['sampled_unix']})


def record_completion(ledger, item, telemetry):
    """Learn short actions from final attributed totals before they disappear.

    The scope owner calls this with its final complete sample. This path never
    grants a reservation; live admission still requires fresh host headroom and
    a fully attributed donor. Failure to attribute produces no learned credit.
    """
    now = time.time()
    if (telemetry.get('complete') is not True
            or telemetry.get('action_key') != item.get('action_key')
            or not 0 <= now - telemetry.get('sampled_unix', 0) <= MAX_SAMPLE_AGE_S
            or telemetry.get('sampled_unix', 0) < item.get('claimed_unix', 0)
            or not all(type(telemetry.get(k)) in (int, float)
                       and math.isfinite(telemetry[k]) and telemetry[k] >= 0
                       for k in ('cpu_seconds', 'wall_seconds', 'memory_peak_bytes'))
            or telemetry['wall_seconds'] <= 0):
        return False
    shape, measurement = action_identity(item)
    tiers = read_json(ledger.base / 'cpu-map.json')
    if not shape or measurement or not tiers:
        return False
    controller = Controller(ledger, tiers)
    with controller.locked():
        profiles = read_json(controller.base / 'profiles.json')
        previous = profiles.get(shape, {})
        completion_id = hashlib.sha256(json.dumps([item['action_key'], telemetry.get('nonce', item.get('claimed_unix'))]).encode()).hexdigest()
        completed = previous.get('completions', [])
        if completion_id in completed:
            return False
        cpu = telemetry['cpu_seconds'] / telemetry['wall_seconds']
        profiles[shape] = {'completions': (completed + [completion_id])[-32:], 'cpu': max(cpu * 1.25, previous.get('cpu', cpu) * .98),
                           'sampled_unix': now,
                           'samples': min(1000, previous.get('samples', 0) + 1),
                           'memory_peak_bytes': max(telemetry['memory_peak_bytes'],
                                                    previous.get('memory_peak_bytes', 0))}
        profiles = dict(sorted(profiles.items(), key=lambda x: x[1].get('sampled_unix', 0))[-512:])
        write_json(controller.base / 'profiles.json', profiles)
    return True

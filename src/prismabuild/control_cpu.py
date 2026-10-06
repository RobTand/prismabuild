"""Conservative per-CPU attribution of control and kernel threads.

No descendant exemption and no last-CPU guess for a migrating thread. Unknown
work stays in the raw host reading. Control accounting is cooperative, not a
security boundary against impersonation. Kernel identity uses Linux flags,
never a process name, an empty cmdline or a low PID (#1399).
"""
import os
from pathlib import Path
import socket

# The same three ownership facts as supervise._is_fleet_loop (#87), narrowed
# to this reader's loaded tree. Older generations are conservatively unknown.
# Do not enumerate the shared generation store while holding host admission.
RUNTIME_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = frozenset(('worker_loop.py', 'tier_loop.py', 'prewarm_loop.py', 'pbmetrics.py'))
OWNERSHIP_ENV = 'PRISMABUILD_SUPERVISED_WORKER'
PF_KTHREAD = 0x00200000  # Linux include/linux/sched.h, stat field 9.


def _stat(path):
    raw = path.read_text()
    close = raw.rfind(')')
    if close < 0:
        raise ValueError('missing process comm')
    fields = raw[close + 1:].split()
    return {'start': int(fields[19]), 'cpu': int(fields[36]),
            'ticks': int(fields[11]) + int(fields[12]), 'flags': int(fields[6])}


def _identity(process, runtime_root, hostname):
    argv = [p.decode('utf-8', 'strict') for p in (process / 'cmdline').read_bytes().split(b'\0') if p]
    if len(argv) < 2 or 'python' not in Path(argv[0]).name:
        return None
    path = Path(argv[1])
    if path.name not in SCRIPTS:
        return None
    if not path.is_absolute():
        path = (process / 'cwd').resolve(strict=True) / path
    path = path.resolve(strict=True)
    if path.name not in SCRIPTS or not path.is_relative_to(runtime_root):
        return None
    mark = f'{OWNERSHIP_ENV}={hostname}'.encode()
    marks = [entry for entry in (process / 'environ').read_bytes().split(b'\0')
             if entry.startswith(f'{OWNERSHIP_ENV}='.encode())]
    if marks != [mark]:
        return None
    return str(path), _stat(process / 'stat')['start']


def _process_identity(process, runtime_root, hostname):
    """Mutually exclusive, positively established attribution classes."""
    status = _stat(process / 'stat')
    if status['flags'] >= 0 and status['flags'] & PF_KTHREAD:
        return 'kernel', ('kernel-thread', status['start'])
    identity = _identity(process, runtime_root, hostname)
    return None if identity is None else ('control', identity)


def _thread_counters(process, identity, kind, cpus):
    """Read stable inner tick bounds for one established process identity."""
    threads = {}
    for thread in (process / 'task').iterdir():
        try:
            before = _stat(thread / 'stat')
            migrations = int(next(line.split(':', 1)[1].strip()
                                  for line in (thread / 'sched').read_text().splitlines()
                                  if line.split(':', 1)[0].strip() == 'se.nr_migrations'))
            after = _stat(thread / 'stat')
            if (before['start'] != after['start'] or before['cpu'] != after['cpu']
                    or before['cpu'] not in cpus or migrations < 0
                    or before['ticks'] < 0 or after['ticks'] < before['ticks']
                    or before['flags'] < 0 or after['flags'] < 0
                    or bool(before['flags'] & PF_KTHREAD) != (kind == 'kernel')
                    or bool(after['flags'] & PF_KTHREAD) != (kind == 'kernel')):
                continue
            # Start at the later tick; end at the earlier tick. The entire
            # credited interval lies inside the host-counter interval.
            threads[f'{process.name}/{thread.name}'] = {
                **before, 'end_ticks': after['ticks'], 'migrations': migrations,
                'process_identity': list(identity), 'attribution_kind': kind}
        except (OSError, ValueError, IndexError, StopIteration):
            continue
    return threads


def control_plane_counters(cpus, *, proc_root=Path('/proc'), runtime_root=RUNTIME_ROOT,
                           hostname=None):
    """Verified control/kernel jiffies with stable thread/CPU identity.

    One existing host-local scan, not an extra kernel census. Control scripts
    need supervisor ownership; kernel threads need PF_KTHREAD at both inner
    bounds. Exec, PID reuse, migration or unavailable evidence grants no credit.
    Attribution kinds keep control-only reporting distinct from system work.
    """
    result = {}
    hostname = socket.gethostname() if hostname is None else hostname
    try:
        processes = list(proc_root.iterdir())
    except OSError:
        return result
    for process in processes:
        if not process.name.isdigit():
            continue
        try:
            classified = _process_identity(process, runtime_root, hostname)
            if classified is None:
                continue
            kind, identity = classified
            threads = _thread_counters(process, identity, kind, cpus)
            if _process_identity(process, runtime_root, hostname) == classified:
                result.update(threads)
        except (OSError, ValueError, IndexError, RuntimeError):
            continue
    return result


def boot_ticks(*, proc_root=Path('/proc')):
    """Boot-relative clock ticks now, from ``/proc/uptime`` (CLOCK_BOOTTIME).

    The same clock a process's ``start`` (``/proc/<pid>/stat`` field 22) counts
    in.  Unlike wall-clock time minus ``btime`` it does not move when the realtime
    clock is stepped, so a boundary taken from it stays true across a step.
    ``None`` when it or the clock rate cannot be read.
    """
    try:
        seconds = float((proc_root / 'uptime').read_text().split()[0])
        rate = os.sysconf('SC_CLK_TCK')
    except (OSError, ValueError, IndexError):
        return None
    if not seconds >= 0 or rate <= 0:
        return None
    return int(seconds * rate)


def attributed_ticks(previous, current, busy, *, kind=None, born_after=None):
    """Credit only stable, non-migrating work within the host interval.

    ``born_after`` (the boot-relative clock ticks :func:`boot_ticks` read just
    after the previous host sample's counters, persisted with that sample) also credits a control thread that is not in
    ``previous`` because it was born after that sample: with zero migrations it
    ran on one CPU for its whole life, all of it inside the interval, so its
    ticks are control-plane work.  Without it a worker loop spawned between two
    samples had every start-up tick counted as foreign (#1581).  Kernel threads
    and anything else unproven stay uncredited.

    ``previous`` follows previous host counters; ``current`` precedes current
    host counters. Missing kind in historical records means control, the only
    class the old collector could prove. Kernel credit requires explicit flags
    at both ends. Inconsistent per-CPU totals are unknown, never clamped.
    """
    excluded = dict.fromkeys(busy, 0)
    if not isinstance(previous, dict) or not isinstance(current, dict):
        return excluded
    for tid, now in current.items():
        old = previous.get(tid)
        if not isinstance(now, dict):
            continue
        if not isinstance(old, dict):
            if (born_after is not None and kind in (None, 'control')
                    and now.get('attribution_kind', 'control') == 'control'
                    and all(type(now.get(k)) is int and now[k] >= 0
                            for k in ('start', 'cpu', 'migrations', 'ticks'))
                    and now['start'] > born_after and now['migrations'] == 0
                    and str(now['cpu']) in excluded):
                excluded[str(now['cpu'])] += now['ticks']
            continue
        category = now.get('attribution_kind', 'control')
        if (category not in ('control', 'kernel')
                or old.get('attribution_kind', 'control') != category
                or (kind is not None and category != kind)):
            continue
        if category == 'kernel' and any(
                type(record.get('flags')) is not int or record['flags'] < 0
                or not record['flags'] & PF_KTHREAD for record in (old, now)):
            continue
        if any(old.get(k) != now.get(k) for k in ('start', 'cpu', 'migrations', 'process_identity')):
            continue
        if not all(type(now.get(k)) is int and now[k] >= 0 for k in ('start', 'cpu', 'migrations', 'ticks')):
            continue
        start = old.get('end_ticks', old.get('ticks'))
        if type(start) is not int or start < 0:
            continue
        delta = now['ticks'] - start
        cpu = str(now['cpu'])
        if cpu in excluded and delta >= 0:
            excluded[cpu] += delta
    return {cpu: ticks if ticks <= busy[cpu] else 0 for cpu, ticks in excluded.items()}

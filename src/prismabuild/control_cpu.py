"""Conservative per-CPU attribution of supervised control threads (#1210).

No descendant exemption and no last-CPU guess for a migrating thread. Unknown
work stays in the raw host reading. This is cooperative accounting, not a
security boundary against a process deliberately impersonating the supervisor.
"""
from pathlib import Path
import socket

# The same three ownership facts as supervise._is_fleet_loop (#87), narrowed
# to this reader's loaded tree. Older generations are conservatively unknown.
# Do not enumerate the shared generation store while holding host admission.
RUNTIME_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = frozenset(('worker_loop.py', 'tier_loop.py', 'prewarm_loop.py', 'pbmetrics.py'))
OWNERSHIP_ENV = 'PRISMABUILD_SUPERVISED_WORKER'


def _stat(path):
    raw = path.read_text()
    close = raw.rfind(')')
    if close < 0:
        raise ValueError('missing process comm')
    fields = raw[close + 1:].split()
    return {'start': int(fields[19]), 'cpu': int(fields[36]),
            'ticks': int(fields[11]) + int(fields[12])}


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


def control_plane_counters(cpus, *, proc_root=Path('/proc'), runtime_root=RUNTIME_ROOT,
                           hostname=None):
    """Verified thread jiffies with stable pid/start/CPU/migration identity.

    Exact scripts plus supervisor ownership, not an inherited environment flag
    alone. Read every thread, never child processes. A disappearing process,
    exec, PID reuse or unavailable scheduler migration counter grants no credit.
    All process reads are host-local; no subprocess or shared census is used.
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
            identity = _identity(process, runtime_root, hostname)
            if identity is None:
                continue
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
                            or before['ticks'] < 0 or after['ticks'] < before['ticks']):
                        continue
                    # The two bounds are used differently at interval ends:
                    # start at the later tick, end at the earlier tick.
                    threads[f'{process.name}/{thread.name}'] = {
                        **before, 'end_ticks': after['ticks'], 'migrations': migrations,
                        'process_identity': list(identity)}
                except (OSError, ValueError, IndexError, StopIteration):
                    continue
            if _identity(process, runtime_root, hostname) == identity:
                result.update(threads)
        except (OSError, ValueError, IndexError, RuntimeError):
            continue
    return result


def attributed_ticks(previous, current, busy):
    """Only same-thread, non-migrating ticks within the host interval count.

    ``previous`` was collected AFTER the previous host counters; ``current``
    BEFORE the current counters. An inconsistent per-CPU sum is unknown, not
    a request to clamp away arbitrary foreign work.
    """
    excluded = dict.fromkeys(busy, 0)
    if not isinstance(previous, dict) or not isinstance(current, dict):
        return excluded
    for tid, now in current.items():
        old = previous.get(tid)
        if not isinstance(old, dict) or not isinstance(now, dict):
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

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

from . import adaptive_snapshot

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


class AdmissionBusy(RuntimeError):
    """Another loop on this box is inside the admission decision right now.

    Raised instead of waiting.  A caller that sees this has learned something
    true about the box -- admission is occupied -- and its correct response is
    to come back on its own schedule, not to sit in the kernel until the
    holder is done.  ``holder`` is the pid still in there when that could be
    read from ``/proc/locks``, and ``None`` when it could not; a missing
    holder is "unknown", never "nobody".
    """

    def __init__(self, holder=None):
        self.holder = holder
        super().__init__(
            f'PrismaBuild admission is held by pid {holder} on this box'
            if holder is not None else
            'PrismaBuild admission is held by another loop on this box')


def _device_and_inode(field):
    """``(major, minor, inode)`` from a ``/proc/locks`` device field, or None.

    Parsed rather than formatted-and-compared.  The kernel prints the device as
    ``%02x:%02x``, so a key built by formatting has to reproduce that padding
    exactly, and getting it wrong yields a silent false negative that reads as
    good news: #264 matched nothing at all on dl380g10, whose ``/tmp`` is tmpfs
    with major 0, while looking correct on sparky, whose major 259 prints the
    same either way.  Reading the numbers back out cannot have that bug, for
    any padding the kernel might choose.
    """

    parts = field.split(':')
    if len(parts) != 3:
        return None
    try:
        return (int(parts[0], 16), int(parts[1], 16), int(parts[2]))
    except ValueError:
        return None


def _holder_of(descriptor):
    """The pid holding the flock on ``descriptor``, or ``None`` if unreadable.

    Best effort and local: ``/proc/locks`` is procfs, so asking costs no I/O
    on the shared mount -- which matters, because the reason this is being
    asked at all is that something on the shared mount is slow.  Every failure
    answers ``None`` rather than raising: a diagnostic must never be able to
    turn a refusal into a crash.

    The INODE identifies the lock and the device only breaks a tie, which is
    not the obvious way round.  Requiring both to match makes this blind on
    btrfs, where a subvolume carries its own anonymous device: ``fstat``
    returns the subvolume's, while ``/proc/locks`` reports the superblock's,
    and the two differ.  Measured on dl380g10 on 2026-09-06, one lock, one
    row::

        /home/rob/tmp  btrfs  fstat 00:1f:14164446
                              /proc/locks 00:1e:14164446

    Same inode, minor off by one, and #276 reported no holder at all for a
    lock it was itself holding.  Matching on the inode recovers it, and costs
    nothing: this is only ever asked after a non-blocking acquisition failed,
    so a holder exists and its row is there to be found.  A second row on the
    same inode number means some other filesystem also has one -- that is the
    collision the device exists to resolve, and where it cannot resolve it
    this answers ``None`` rather than naming a pid from another mount.
    """

    try:
        info = os.fstat(descriptor)
    except OSError:
        return None
    wanted = (os.major(info.st_dev), os.minor(info.st_dev), info.st_ino)
    candidates = []
    try:
        with open('/proc/locks') as handle:
            for line in handle:
                fields = line.split()
                # "<n>: FLOCK ADVISORY WRITE <pid> <maj>:<min>:<ino> 0 EOF",
                # where the device numbers are hex and the inode is decimal.
                # A waiter's line begins "<n>: -> FLOCK", which fails the test
                # below and is skipped, so only the holder is ever named.
                if len(fields) < 6 or fields[1] != 'FLOCK':
                    continue
                found = _device_and_inode(fields[5])
                if found is None or found[2] != info.st_ino:
                    continue
                candidates.append((found, int(fields[4])))
    except (OSError, ValueError):
        return None
    if not candidates:
        return None
    if len(candidates) > 1:
        # More than one filesystem has a lock on this inode NUMBER, so the
        # number alone no longer names a lock and the device has to break the
        # tie.  If it cannot -- neither row carries this descriptor's device,
        # or both do -- the honest answer is that we do not know which.
        exact = [row for row in candidates if row[0] == wanted]
        if len(exact) != 1:
            return None
        candidates = exact
    held_by = candidates[0][1]
    # -1 is the kernel's "no owning process" (an OFD lock); it is not a pid
    # and must not be reported as one.
    return held_by if held_by > 0 else None


#: Where a box keeps the host-local state its loops share.  An attribute
#: rather than a literal inside ``box_state`` so the test guard can repoint it
#: at the test's own ``tmp_path``: the identity below is keyed on the queue
#: root, every test builds its own queue under a fresh ``tmp_path``, and the
#: lock file for each is created and never unlinked -- so the suite minted a
#: permanent file per test into the directory the *fleet* uses.  dl380g10 had
#: accumulated 3780 of them against sparky's 2, 874 of those in one 20-minute
#: run.  Nothing read them and nothing broke, but a box's own admission state
#: lived in a directory the suite was filling.
#:
#: Unlinking is not the alternative.  A file here can be held by a live loop
#: that this process cannot see, and there is no way to test "unheld" and
#: unlink it without a window in which a sibling opens the path and ends up
#: holding an inode nobody else can reach -- which is a lapse of the mutual
#: exclusion the file exists for.  Not writing them is the fix; the ones
#: already on a box are cleared by the reboot that clears ``/tmp``.
#: Env-backed as well as an attribute, because the attribute alone reaches
#: only the modules already imported in *this* process.  The suite runs real
#: workers and real ``pbrun`` invocations as child processes, and each of those
#: imports this module fresh, past any ``monkeypatch``: with the attribute
#: repoint alone, one suite run still left three ``.sweep`` markers in the
#: fleet's directory (measured on sparky, 2026-09-06, against 132 files for the
#: same suite with neither).  ``LOCAL_CHECKOUT_ROOT`` is env-backed for exactly
#: this reason and says so.
BOX_STATE_ROOT = Path(os.environ.get('PRISMABUILD_BOX_STATE_ROOT')
                      or Path('/tmp') / f'prismabuild-admission-{os.getuid()}')


def box_state(base):
    """Return ``(directory, digest)`` naming host-local state for ONE box.

    The loops of a single box have to agree about a few things -- who holds
    admission, when the queue was last swept -- and the pool they share is on
    NFS.  Coordinating through the pool would put an NFS round trip in front
    of every answer, which is the cost this identity exists to avoid, so the
    rendezvous is a host-local directory keyed by the ledger the loops share.
    Two boxes serving the same pool hash differently and never meet here;
    two generations on one box hash identically and do.

    ``/tmp`` is cleared on some of these hosts.  Every user of this directory
    must therefore treat a missing file as "no information", never as a fact:
    the lock re-creates its inode (a cleared directory can only lapse mutual
    exclusion between a live loop and a new one, which is the pre-existing
    behaviour of this path), and the sweep marker below simply sweeps once
    more than it had to.
    """

    directory = Path(BOX_STATE_ROOT)
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    info = directory.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        # Not a directory, or not ours.  Nothing this process can do makes it
        # safe, so this is the honest refusal and it stays one.
        raise RuntimeError('unsafe PrismaBuild admission lock directory')
    if info.st_mode & 0o077:
        # Ours, and too permissive: set it right rather than refuse.  What the
        # check wants is that the directory be private to this uid; we own it,
        # so `chmod` is an answer and refusing is a way of not giving one.
        #
        # Refusing took a box out of service for as long as the mode survived.
        # The raise leaves `claim` into `serve_once`, where the worker loop
        # counts it among the consecutive failures it tolerates for a bad
        # ITEM -- but every poll re-reads the same directory and gets the same
        # answer, so the count only runs up.  Announcing happens earlier in the
        # same poll, so the box kept publishing a fresh offer at full capacity
        # while claiming nothing: live to everything watching, and useless.
        # Worse, the same raise reaches `pool.cleanup_action_containers`, which
        # is the gate that returns tokens; `RuntimeError` is in neither of its
        # `except` tuples, so it escapes after `scope.release()` and before the
        # claim is finished, and an action that had already run lost its lease.
        #
        # Measured on sparky, 2026-09-06: mode 0770, thirteen minutes, three
        # actions pinned to it sitting in `ready` with no passes recorded;
        # `chmod 700` by hand and the queue drained in eighteen seconds.  It
        # happened again on dl380g10 within the hour.
        #
        # The origin was ours -- a test that chmodded the very path
        # `box_state` handed it -- and that is fixed at source in #265, so this
        # is not the only thing standing between the fleet and that outage.  It
        # is here because the mode arrives from outside this process and a box
        # must not be removable from the fleet by a permission bit it is
        # entitled to set.
        directory.chmod(0o700)
        info = directory.lstat()
        if info.st_mode & 0o077:
            # The chmod did not take.  Now there is nothing left to try.
            raise RuntimeError('unsafe PrismaBuild admission lock directory')
    identity = f'{Path(base).resolve()}:{socket.gethostname()}'
    return directory, hashlib.sha256(identity.encode()).hexdigest()


def local_state_base(base):
    """CPU learning is local authority, never restored from diagnostic copies."""
    directory, digest = box_state(base)
    state = directory / (digest + '.adaptive-cpu-v1')
    state.mkdir(mode=0o700, exist_ok=True)
    info = state.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise RuntimeError('unsafe PrismaBuild adaptive CPU state directory')
    if info.st_mode & 0o077:
        # As with the owning box-state directory, repair our own permissions
        # instead of taking admission down for an owner-correctable mode.
        state.chmod(0o700)
        if state.lstat().st_mode & 0o077:
            raise RuntimeError('unsafe PrismaBuild adaptive CPU state directory')
    return state


def local_telemetry_path(base, action_key):
    """Where the executing box records a holder's live telemetry; admission reads only this.

    The record is written by this host's own sampler for this host's own
    action, and read under this host's own admission lock, so it is host-local
    in meaning. The copy under ``reservations/<host>/telemetry/`` on the shared
    mount is the same record written for remote readers (``pbmetrics``) and is
    never consulted for credit: a stale or edited copy there changes nothing.

    ``local_state_base`` resolves ``base`` (stats on the mount), so the
    controllers read from their own cached ``base`` inside the lock and only
    the scope owner, once per scope, derives the path through this function.
    """
    return local_state_base(base) / 'telemetry' / f'{action_key}.json'


class Controller:
    def __init__(self, ledger, tiers):
        self.ledger = ledger
        self.tiers = tiers
        self.cpus = list(tiers['preferred']) + list(tiers['fallback'])
        self.base = local_state_base(ledger.base)
        self._host_sample = None

    def write_state(self, name, value):
        write_json(self.base / name, value)

    @contextmanager
    def locked(self):
        """Hold box admission for the block, or raise ``AdmissionBusy`` at once.

        The acquisition is non-blocking, and that is the whole point.  What
        this lock guards is the box's headroom decision, not the claim that
        follows it.  ``_claim`` takes it for its capacity prelude, and then
        per candidate for the adaptive and GPU decisions through
        ``begin_acquire`` -- the point at which the tokens leave ``free/``
        and every sibling's ``decision`` and ``available`` can see them
        reserved.  The record rename, the lease write and the token renames
        that follow run outside it, and the only thing that comes back under
        it is ``admitted``'s host-local borrow record.

        Even so the holder's time inside is not purely local: CPU samples,
        profiles, interval/borrowing state, holder telemetry and GPU probe
        state are host-local, but ``decision`` reads every holder's token
        metadata on the shared mount and ``begin_acquire`` renames there.  So
        the holder's time inside is still bounded by a filesystem another
        machine controls, and a blocking ``LOCK_EX`` made every other loop on
        the box wait for it.

        That is not a worst case, it is a measurement.  On 2026-09-06 one
        client was slow to return an NFS read delegation; the holder sat in
        ``__break_lease`` against a 45-second ``lease-break-time``, and 15 of
        dl380g10's 16 loops were in ``locks_lock_inode_wait`` behind it.  The
        box announced nothing for the duration -- offer age climbed past
        ``OFFER_TIMEOUT_S`` -- so a box that was merely waiting was
        indistinguishable, to everything watching, from a box that was gone.

        Refusing instead of waiting keeps every loser on its own poll cadence,
        which is where announcing lives, so the box keeps saying what it is
        while one loop is slow.  There is no timeout here and no deadline
        constant: the poll interval already is the retry.

        The cost is fairness.  A refused loop loses its place -- the kernel's
        FIFO wait queue was the only ordering these loops had -- so under
        steady contention the same fast poller can win repeatedly.  Per-box
        admission throughput is unchanged either way, since it is one critical
        section wide in both designs; what changes is that nobody is ever
        parked in the kernel for a remote filesystem's lease timer.

        Refusing fast is not the same as being narrow, and #351 is the
        difference: while this lock still enclosed the whole of ``_claim``,
        one loop stalled on the mount refused every sibling for the length of
        the stall, and the box claimed nothing at all with ready work waiting
        and its tokens free.  A refusal that arrives instantly is still a
        refusal.  Keep the block short and host-local; anything on the mount
        that does not have to be exclusive between this box's loops belongs
        outside it.
        """

        # Never unlink: two generations must not lock different inodes. The
        # private directory and O_NOFOLLOW prevent another uid redirecting it.
        directory, digest = box_state(self.ledger.base)
        name = digest + '.lock'
        descriptor = os.open(directory / name, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        acquired = False
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
                raise RuntimeError('unsafe PrismaBuild admission lock file')
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                # Read the holder before releasing anything, so the pid named
                # is the one that was actually in the way.
                raise AdmissionBusy(holder=_holder_of(descriptor)) from None
            acquired = True
            yield
        finally:
            os.close(descriptor)
            # No shared operation or inherited admission descriptor in the
            # publisher. A blocked diagnostic copy occupies only its own
            # host-local publication lock, never this admission lock.
            if acquired:
                adaptive_snapshot.publish(self.base, self.ledger.base / 'adaptive')

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
        self.write_state('cpu-sample.json', current)
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
            # ``self.base`` rather than ``local_telemetry_path``: that helper
            # resolves the ledger path, which is three stats on the mount per
            # call, and this loop runs once per holder under the lock.
            record = read_json(self.base / 'telemetry' / f'{holder.name}.json')
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
            # Pass the metadata already read; an absent or unreadable file keeps
            # the ledger's own read and its own refusal on a torn record.
            allocation = self.ledger.cpu_allocation(holder.name, self.tiers, metadata=meta or None)
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
        self.write_state('jobs.json', next_recent)
        profiles = dict(sorted(profiles.items(), key=lambda x: x[1].get('sampled_unix', 0))[-512:])
        self.write_state('profiles.json', profiles)
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
            self.write_state('last-borrow.json', {'sampled_unix': metadata['sampled_unix']})


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
    # Learning a shape is worth having and never worth waiting for.  This runs
    # on a scope owner that has just finished its action, so blocking here
    # would hold that process open on another loop's NFS latency -- and this
    # function's own contract already covers the outcome: failure to attribute
    # produces no learned credit.  A busy box learns this shape the next time
    # an action of it completes.
    #
    # The whole read-modify-write stays inside the lock; nothing within it
    # locks again, so the handler can only be catching this box's own refusal
    # to wait.
    try:
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
            controller.write_state('profiles.json', profiles)
    except AdmissionBusy:
        return False
    return True

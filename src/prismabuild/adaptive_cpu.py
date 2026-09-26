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
import statistics
import time
import uuid

from . import adaptive_snapshot

#: A CPU is treated as idle for admission corroboration when it was busy for
#: at most this fraction of the fresh sampling interval.  Small on purpose: a
#: pinned neighbour sits at ~1.0, so this separates "nobody ran here" from
#: "someone ran here", not "lightly used" from "heavily used".
IDLE_BUSY_FRACTION = .05

MAX_SAMPLE_AGE_S = 5.0
MIN_INTERVAL_S = 1.0
MAX_INTERVAL_S = 60.0
MAX_ACTIONS = 256
METADATA = '.adaptive.json'
#: ``decision`` has not been told the item's :func:`dependent_owner`.
_UNREAD = object()


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
    path = Path(str(item['cas_root'])) / 'requests' / key[:2] / f'{key}.json'
    raw = read_json(path)
    # Remembered whatever it held, an unreadable request included, so the
    # #985 lookups that follow never read the same path a second time.
    _LAST_REQUEST[0] = (str(path), None)
    if not raw:
        return None, False
    try:
        action = core.validate_action(raw)
    except (ValueError, TypeError, KeyError):
        return None, False
    if action['action_key'] != key:
        return None, False
    _LAST_REQUEST[0] = (str(path), action)
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


#: A produced-output producer's export allowance (#985).  Every producer
#: whose sealed environment configures a local spool reserves, at claim and
#: with its own reservation, room for ``slots`` concurrent spool exports of
#: :data:`EXPORT_DEMAND` each.  Its exports are admitted against that
#: allowance, never the free pool: a foreign action cannot take the room, and
#: host pressure the producer creates on its own CPUs cannot refuse them.
#: Spelled here, the lowest layer, because ``produced_spool`` imports the pool.
SPOOL_ROOT_ENV = 'PRISMABUILD_PRODUCED_SPOOL_ROOT'
EXPORT_SLOTS_ENV = 'PRISMABUILD_PRODUCED_SPOOL_EXPORT_SLOTS'
#: What ``ProducedSpool.submit_group`` seals for one export.
EXPORT_DEMAND = {'cpu': 1, 'mem_gb': 1}
#: The slot count when nothing is measured yet (#999): one export at a time,
#: labelled ``unmeasured`` in the allowance.  Measured, the count is derived
#: by :func:`export_slots`; declared, ``EXPORT_SLOTS_ENV`` wins and
#: ``EXPORT_SLOTS_ENV=0`` opts out.
DEFAULT_EXPORT_SLOTS = 1
#: Measured export landings and group spacing per produced-output template
#: (#999), or per the family a template declares (#1126), host-local beside
#: ``profiles.json`` and learned the same way.
EXPORT_RATES = 'export-rates.json'
#: How many landings and gaps a key keeps: the memory ``profiles`` keeps of
#: completions.
EXPORT_RATE_MEMORY = 32
#: The prefix of a declared family's key in :data:`EXPORT_RATES` (#1126).
#: A template's key is its 64-hex digest, which cannot carry it, so no
#: template's entry and no family's are ever the same entry.
EXPORT_RATE_FAMILY_PREFIX = 'family:'


def export_rate_key(template_sha256, family=None):
    """The :data:`EXPORT_RATES` key a producer's exports learn and read under.

    ``family`` is the ``export_rate_family`` the producer's produced-output
    template declares (#1126): every template of a family shares its entry,
    ``family:<name>``, so a Stage B row -- its own template -- inherits what
    the family's earlier rows measured.  ``None`` means none is declared, and
    the key is the template's digest, as #999 keyed every producer.  A family
    that is not an identifier (``produced_output.validate_export_rate_family``)
    or, undeclared, a digest that is not a key has no key: nothing is learned
    or read under it, so no malformed value lands on another producer's entry.
    """
    if family is None:
        return template_sha256 if _is_key(template_sha256) else None
    from . import produced_output
    try:
        return EXPORT_RATE_FAMILY_PREFIX + produced_output.validate_export_rate_family(family)
    except produced_output.ProducedOutputError:
        return None


def export_rate_names(ref):
    """``(template_sha256, family)`` a producer's ``produced_output`` reference
    names, or ``None`` when it declares a family that is not an identifier.

    ``PoolQueue.publish`` projects the reference from the validated template,
    and carries ``export_rate_family`` only when the template declares it
    (#1126); ``family`` is then ``None``.  A declared family the reader cannot
    use is refused rather than read as undeclared: falling back to the
    template would learn and size under a key the producer did not name.
    """
    if not isinstance(ref, dict):
        return None, None
    template = ref.get('template_sha256')
    if 'export_rate_family' not in ref:
        return template, None
    family = ref['export_rate_family']
    if not isinstance(family, str) or export_rate_key(template, family) is None:
        return None
    return template, family


def export_slots(rates, template_sha256, family=None):
    """``(slots, basis)`` for a producer of ``template_sha256`` (#999), or of
    the ``family`` its template declares (#1126).

    Little's law with both sides taken at their worst: exports in flight are
    at most the longest landing over the shortest group spacing, so
    ``slots = ceil(max landing_s / min spacing_s)``, never below one.  Both
    come from this host's own completed exports under the producer's
    :func:`export_rate_key` (:func:`learn_export`): the family's, whichever of
    its templates they were, or the template's own.  With either side
    unmeasured the count is :data:`DEFAULT_EXPORT_SLOTS` and the basis says
    ``unmeasured``; it names ``export_rate_family`` when one is declared.
    """
    key = export_rate_key(template_sha256, family)
    entry = rates.get(key) if isinstance(rates, dict) and key else None
    entry = entry if isinstance(entry, dict) else {}
    landings = [v for v in entry.get('landing_s', []) if type(v) in (int, float) and v > 0]
    gaps = [v for v in entry.get('spacing_s', []) if type(v) in (int, float) and v > 0]
    names = {'template_sha256': template_sha256}
    if family is not None:
        names['export_rate_family'] = family
    if not landings or not gaps:
        return DEFAULT_EXPORT_SLOTS, {'basis': 'unmeasured', **names,
                                      'landings': len(landings), 'spacings': len(gaps)}
    landing, spacing = max(landings), min(gaps)
    return max(1, math.ceil(landing / spacing)), {
        'basis': 'measured', 'rule': 'ceil(max landing_s / min spacing_s)',
        **names, 'landing_s': round(landing, 3),
        'spacing_s': round(spacing, 3), 'landings': len(landings), 'spacings': len(gaps)}


#: The last sealed request :func:`action_identity` read, as ``(path, action)``
#: (``action`` is ``None`` when it was unreadable).  The claim path reads each candidate's request once, for
#: its identity, and the #985 facts about the same candidate -- the producer an
#: export serves, a producer's allowance -- come from those bytes rather than a
#: second read.  A request is content-addressed, so a hit is never stale.
_LAST_REQUEST = [None]


def _sealed_request(item):
    """The item's validated sealed request, or ``None`` if unreadable."""
    from . import core
    try:
        key = str(item['action_key'])
        path = Path(str(item['cas_root'])) / 'requests' / key[:2] / f'{key}.json'
    except (KeyError, TypeError):
        return None
    last = _LAST_REQUEST[0]
    if last is not None and last[0] == str(path):
        return last[1]
    raw = read_json(path)
    if not raw:
        return None
    try:
        action = core.validate_action(raw)
    except (ValueError, TypeError, KeyError):
        return None
    return action if action['action_key'] == key else None


def _is_key(value):
    return (isinstance(value, str) and len(value) == 64
            and all(c in '0123456789abcdef' for c in value))


def dependent_owner(item):
    """The producer this item's sealed request says it serves, or ``None``.

    A producer's spool exports run on its host, and its progress is counted
    when they land (#982, #985).  ``produced_spool.ProducedSpool.submit_group``
    seals each export with ``params.produced_spool.owner``, the action key of
    the producer that owns the spool.  The key is content-addressed over the
    whole request, so the link is as trustworthy as the request itself.  Only
    a ``generation`` action can be a dependent: a measurement never runs beside
    another holder.  Anything unreadable is no link.
    """
    action = _sealed_request(item)
    if action is None or action['task'].get('task_class') != 'generation':
        return None
    link = action.get('params', {}).get('produced_spool')
    owner = link.get('owner') if isinstance(link, dict) else None
    return owner if _is_key(owner) else None


def producer_allowance(item, rates=None):
    """The export allowance this item's sealed environment declares, or ``None``.

    ``{'slots': k, 'cpu': k, 'mem_gb': k, 'basis': {...}}`` for a producer
    whose sealed environment names a spool root (#985), ``None`` for
    everything else, for ``EXPORT_SLOTS_ENV=0`` and for anything unreadable or
    malformed, a malformed ``export_rate_family`` included (#1126).  ``k`` is
    the sealed ``EXPORT_SLOTS_ENV`` when declared, else :func:`export_slots`
    over ``rates`` (the host's ``EXPORT_RATES``) for the row's produced-output
    template, or the family it declares; ``basis`` says which (#999).
    """
    action = _sealed_request(item)
    if action is None:
        return None
    variables = action.get('environment', {}).get('variables', {})
    if not isinstance(variables, dict) or not variables.get(SPOOL_ROOT_ENV):
        return None
    raw = variables.get(EXPORT_SLOTS_ENV)
    if raw is None:
        names = export_rate_names(item.get('produced_output') if isinstance(item, dict) else None)
        if names is None:
            return None
        template, family = names
        slots, basis = export_slots(rates, template if isinstance(template, str) else None,
                                    family)
    else:
        if not isinstance(raw, str) or not raw.isascii() or not raw.isdigit() or int(raw) <= 0:
            return None
        slots, basis = int(raw), {'basis': 'declared', 'env': EXPORT_SLOTS_ENV}
    return {'slots': slots, **{kind: need * slots for kind, need in EXPORT_DEMAND.items()},
            'basis': basis}


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
        # Only "some" exists for the CPU resource at the system level: the
        # kernel records FULL for CPU under a cgroup, never for psi_system
        # (kernel/sched/psi.c -- "the FULL state doesn't exist for the CPU
        # resource at the system level", and the state mask at the system
        # root omits PSI_CPU_FULL).  Reading it here would compare a value the
        # kernel never advances, so the pressure gate is corroborated with
        # occupancy instead.
        try:
            psi = next(line for line in Path('/proc/pressure/cpu').read_text().splitlines()
                       if line.startswith('some '))
        except StopIteration:
            return None
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
    return directory, box_identity(base)


def box_identity(base):
    """The digest that names ONE box's host-local state for the ledger at ``base``.

    Every loop of a box names the same ledger and so the same digest; two boxes
    serving one pool hash differently.  Host-local state kept outside
    ``box_state``'s directory is keyed by it too (``local_scratch``'s spool
    offer), so the two cannot disagree about which box they describe.
    """

    identity = f'{Path(base).resolve()}:{socket.gethostname()}'
    return hashlib.sha256(identity.encode()).hexdigest()


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


#: The host's own idle history (#997), host-local beside ``cpu-sample.json``.
IDLE_BASELINE = 'idle-baseline.json'
IDLE_BASELINE_SCHEMA = 'prismabuild.idle_baseline.v1'
#: What an idle sample carries, and what a measurement is judged on.
IDLE_FIELDS = ('busy_cpus', 'psi_some')
#: How many idle samples a host keeps.  A storage bound, not a decision
#: threshold: the rule below compares a sample with the largest idle sample
#: in the window, so on a box whose idle load is stationary a truly idle
#: sample is refused with probability ``1 / (IDLE_WINDOW + 1)`` (the chance
#: that it is the largest of ``IDLE_WINDOW + 1`` exchangeable draws), 0.4%
#: here, and a refused measurement is judged again on the next pass.  The
#: file is rewritten under the admission lock once per new idle sample, so
#: the bound also keeps that write to a few kilobytes.
IDLE_WINDOW = 256


def _idle_statistics(samples, field):
    values = [float(s[field]) for s in samples]
    mean = sum(values) / len(values)
    top = max(values)
    return {'mean': round(mean, 6), 'max': round(top, 6), 'margin': round(top - mean, 6),
            'stdev': round(statistics.pstdev(values), 6) if len(values) > 1 else 0.}


def _judge_idle(verdict, reference, current, fields, prior_rule):
    """Whether ``current`` is above ``reference``'s maximum on any field.

    With no reference there is no measurement of this host's idle state, and
    ``prior_rule`` -- the caller's pre-#997 fixed line -- decides, labelled
    ``basis: unmeasured`` (#997 ruling, as for the unmeasured export slots of
    #999).  Without a prior rule an unmeasured sample exceeds.
    """
    verdict['samples'] = len(reference)
    verdict['current'] = current
    if reference:
        verdict['basis'] = 'measured'
        verdict['span_s'] = round(reference[-1]['sampled_unix'] - reference[0]['sampled_unix'], 3)
        for field in fields:
            verdict[field] = _idle_statistics(reference, field)
        return any(current[field] > verdict[field]['max'] for field in fields)
    verdict['basis'] = 'unmeasured'
    if prior_rule is None:
        return True
    verdict['prior'] = prior_rule.__doc__ or getattr(prior_rule, '__name__', 'prior')
    return bool(prior_rule(current))


def idle_judgement(state, sample, *, holders, identity, fields=IDLE_FIELDS, interval_s=None,
                   prior_rule=None):
    """Judge one host sample against the host's own idle history (#997).

    Returns ``(verdict, state)``: ``verdict['exceeds']`` is whether the sample
    is outside what this host has been observed doing while PrismaBuild ran
    nothing, with the evidence a refusal records, and ``state`` is the history
    to persist (unchanged when ``state is`` the returned one).

    * An **idle sample** is a fresh one taken with no holder on the host and
      no holder seen since its interval began: a sample whose interval
      overlaps a holder's tail measures that holder, not the host.
    * The **baseline** is every idle sample kept before this one, less an
      ongoing excursion: a run of consecutive idle samples each above the
      baseline's maximum.  The run is judged against the samples before it
      began, so a sustained foreign load is refused for as long as it runs,
      not only on its first pass.  Its span is measured from the *oldest*
      remembered sample to the run's own start, not between the remembered
      samples themselves -- that span is zero with only one of them, which
      would let any run "outlive" the window at once (#1014).  A run that has
      lasted as long as that span has outlived what the window remembers of
      the host's earlier state, and the host's idle state is taken to have
      changed: the whole window becomes its baseline.
    * A sample **exceeds** when any of ``fields`` (default :data:`IDLE_FIELDS`)
      is above the
      baseline's maximum.  No multiplier: the maximum is the largest load
      this host has shown while idle, and ``margin`` (maximum less mean) is
      recorded beside the standard deviation so a reader sees it in units of
      the host's own variation.
    * With no idle history nothing about this host is measured, and
      ``prior_rule`` (the caller's pre-#997 fixed line) judges the sample,
      labelled ``basis: unmeasured``.  A sample the rule refuses is foreign
      load, not idle evidence, and does not seed the window (#1014): the host
      stays unmeasured, refused by the same fixed line, for as long as the
      load runs, rather than becoming its own baseline maximum and being
      admitted on the very next pass.  A sample the rule does not refuse
      seeds the window, so the next idle pass is judged against it.  Without a
      prior rule there is no rule to have refused the sample, so it exceeds
      and still seeds -- the window has to start somewhere, and nothing here
      can tell that sample apart from a genuinely idle one.
    * With holders present the sample is judged against the whole window
      (``state: holders_present``) but never joins it: it measures the
      holders too, so exceeding says the host is not idle, and not exceeding
      leaves the holders to refuse an exclusive claim themselves.
    * Every sample joins the window except one an empty baseline's
      ``prior_rule`` refuses, bounded by :data:`IDLE_WINDOW`.  An excursion
      sample joins even though it exceeds an established baseline, so it can
      later become the baseline itself; only the seed of an unmeasured window
      is held to the stricter rule, since nothing yet distinguishes it from
      foreign load.

    ``interval_s`` is how far back the sample's reading reaches, when the
    sample does not say (the GPU broker's PSI fields are the kernel's 10 s
    averages).
    """
    state = dict(state) if isinstance(state, dict) else {}
    if state.get('schema') != IDLE_BASELINE_SCHEMA or state.get('identity') != identity:
        # A changed CPU topology or device is a different host for this purpose.
        state = {'schema': IDLE_BASELINE_SCHEMA, 'identity': identity, 'samples': []}
    samples = [s for s in state.get('samples', []) if isinstance(s, dict)
               and all(type(s.get(k)) in (int, float) and math.isfinite(s[k])
                       for k in ('sampled_unix',) + tuple(fields))]
    now = sample.get('sampled_unix')
    seen = state.get('holders_seen_unix')
    seen = float(seen) if type(seen) in (int, float) and math.isfinite(seen) else None
    verdict = {'window_bound': IDLE_WINDOW}
    current = {field: float(sample[field]) for field in fields}
    if holders:
        verdict['state'] = 'holders_present'
        verdict['exceeds'] = _judge_idle(verdict, samples, current, fields, prior_rule)
        if type(now) in (int, float) and (seen is None or now > seen):
            state['holders_seen_unix'] = now
        return verdict, state
    started = now - (sample['interval_s'] if interval_s is None else interval_s)
    if seen is not None and started <= seen:
        verdict.update(state='holder_tail', exceeds=True, holders_seen_unix=seen,
                       interval_start_unix=started)
        return verdict, state
    prior = [s for s in samples if s['sampled_unix'] < now]
    run = state.get('excursion_unix')
    run = float(run) if type(run) in (int, float) and math.isfinite(run) else None
    reference = prior
    if run is not None:
        before = [s for s in prior if s['sampled_unix'] < run]
        # From the oldest remembered sample to when the run started, not
        # between the remembered samples themselves: that is zero with a
        # single one of them, which read every run as having already
        # outlived the window on its very first pass (#1014).
        span = (run - before[0]['sampled_unix']) if before else 0.
        if before and now - run < span:
            reference = before
        else:
            run = None
    verdict['state'] = 'idle'
    exceeds = verdict['exceeds'] = _judge_idle(verdict, reference, current, fields, prior_rule)
    if run is not None and exceeds:
        verdict['excursion_s'] = round(now - run, 3)
    # A sample an empty baseline's prior_rule refuses is foreign load, not
    # idle evidence, and must not seed the window: seeding it becomes the
    # window's only (and therefore maximum) sample, so the same load reads as
    # its own baseline and is admitted on the very next pass (#1014). Without
    # a prior_rule there was no rule to have refused it -- the unmeasured
    # default always exceeds, and still seeds, since the window has to start
    # somewhere.
    refused_seed = verdict['basis'] == 'unmeasured' and exceeds and prior_rule is not None
    if not refused_seed and all(s['sampled_unix'] != now for s in samples):
        samples.append({'sampled_unix': now, **current})
        state['samples'] = samples[-IDLE_WINDOW:]
        if exceeds and reference:
            state['excursion_unix'] = run if run is not None else now
        else:
            state.pop('excursion_unix', None)
    return verdict, state


class Controller:
    def __init__(self, ledger, tiers):
        self.ledger = ledger
        self.tiers = tiers
        self.cpus = list(tiers['preferred']) + list(tiers['fallback'])
        self.base = local_state_base(ledger.base)
        self._host_sample = None
        self._idle = None

    def idle(self, sample, holders):
        """This pass's :func:`idle_judgement`, persisted host-local (#997).

        Under the admission lock, like every other file in ``self.base``.  One
        judgement per sample and holder state: a pass that decides several
        candidates reads the history once and writes it at most once.
        """
        key = (sample.get('sampled_unix'), bool(holders))
        if self._idle is not None and self._idle[0] == key:
            return self._idle[1]
        state = read_json(self.base / IDLE_BASELINE)
        cpus = len(self.cpus)

        def prior_rule(current):
            # The pre-#997 lines, applied only while this host has no idle
            # history to judge against.
            return current['busy_cpus'] > .05 * cpus or current['psi_some'] >= .10
        prior_rule.__doc__ = f'busy_cpus > {.05 * cpus:g} (.05 x {cpus} CPUs) or psi_some >= .10'
        verdict, updated = idle_judgement(state, sample, holders=bool(holders),
                                          identity=cpus, prior_rule=prior_rule)
        if updated != state:
            self.write_state(IDLE_BASELINE, updated)
        self._idle = (key, verdict)
        return verdict

    def write_state(self, name, value):
        write_json(self.base / name, value)

    def _predicted_cpus(self, need: int) -> list[int] | None:
        """The CPUs this claim's tokens would represent, by the ledger's rule.

        ``ResourceLedger.begin_acquire`` takes the first ``need`` free
        ``cpu-*`` tokens in sorted-name order and maps each token ordinal to
        ``(preferred + fallback)[ordinal]``.  The ordinal is a token index,
        never a CPU ID, so this asks the ledger for that same answer instead of
        keeping a second copy of the selection rule here.  ``None`` means the
        question cannot be answered -- fewer free tokens than the demand, or an
        ordinal outside the configured topology -- and the caller treats that
        as unknown rather than as idle.
        """
        return self.ledger.free_cpu_allocation(need, self.tiers)

    @contextmanager
    def locked(self):
        """Hold box admission for the block, or raise ``AdmissionBusy`` at once.

        The acquisition is non-blocking, and that is the whole point.  What
        this lock guards is the box's headroom decision, not the claim that
        follows it.  ``_claim`` takes it for its capacity prelude, and then
        per candidate for the adaptive and GPU decisions through
        ``begin_acquire`` -- the point at which the tokens leave ``free/``
        and every sibling's ``decision`` and ``available`` can see them
        reserved, and at which ``admitted`` records the borrow that decision
        spent.  The record rename, the lease write and the token renames that
        follow run outside it, and nothing reacquires it after them.

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

    def _funding(self, owner, holders, demand):
        """Whether ``owner``'s export allowance covers this dependent (#985).

        Returns ``(funded, None)`` -- the allowance CPUs and memory this
        dependent runs on -- or ``(None, why)``.  Read under the admission
        lock from the holders' metadata, which the holder loop reads anyway;
        a slot is in use while any holder, committed or still acquiring, names
        ``owner`` in ``funded_by``.
        """
        if set(demand) - set(EXPORT_DEMAND) or not int(demand.get('cpu', 0)):
            return None, 'demand_outside_allowance'
        meta_of_owner = read_json(self.ledger.held_dir / owner / METADATA)
        allowance = meta_of_owner.get('dependent_allowance')
        if not isinstance(allowance, dict):
            return None, 'owner_holds_no_allowance'
        cpus, mem, slots = allowance.get('cpus'), allowance.get('mem_gb'), allowance.get('slots')
        if (not isinstance(cpus, list) or not all(type(c) is int for c in cpus)
                or type(mem) is not int or type(slots) is not int or slots <= 0):
            return None, 'owner_allowance_malformed'
        used, in_use = set(), 0
        for holder in holders:
            if holder.name == owner:
                continue
            meta = read_json(holder / METADATA)
            if meta.get('measurement'):
                return None, 'measurement_holder'
            if meta.get('funded_by') == owner:
                in_use += 1
                used.update((meta.get('funded') or {}).get('cpus') or [])
        if in_use >= slots:
            return None, 'allowance_in_use'
        need_cpu, need_mem = int(demand.get('cpu', 0)), int(demand.get('mem_gb', 0))
        spare = [cpu for cpu in cpus if cpu not in used]
        if need_cpu > len(spare) or need_cpu > len(cpus) // slots or need_mem > mem // slots:
            return None, 'demand_exceeds_allowance'
        return {'cpus': spare[:need_cpu], 'mem_gb': need_mem,
                'owner_measurement': bool(meta_of_owner.get('measurement'))}, None

    def decision(self, item, demand, *, identity=None, owner=_UNREAD, allowance=None):
        """Decide under admission; callers may pre-read sealed action identity.

        ``owner`` is the producer a dependent serves, when the caller knows it
        (the pool reads it from the sealed request, outside this lock); left unread it is
        read only if a measurement holds the host (#982).  ``allowance`` is the
        export allowance a producer's claim reserves with itself (#985).
        """
        funding_refusal = None
        self.last_decision = {"reason": "not_evaluated"}
        if self._host_sample is None:
            self._host_sample = self.sample()
        sample = self._host_sample
        now = time.time()
        def refuse(reason, **values):
            # The claimant publishes this exact decision after it releases
            # admission.  Do not sample or reread state for diagnostics.
            # A dependent's refusal names its producer, and why its allowance
            # did not cover it, so a starved producer's evidence reads in one
            # place (#985).
            extra = {}
            if owner is not _UNREAD:
                extra['dependent_of'] = owner
            if funding_refusal is not None:
                extra['allowance'] = funding_refusal
            self.last_decision = {"reason": reason, "sample": sample, **extra, **values}
            return None
        fresh = (0 <= now - sample.get('sampled_unix', 0) <= MAX_SAMPLE_AGE_S
                 and sample.get('cpu_count') == len(self.cpus)
                 and all(isinstance(sample.get(key), (float, int)) and math.isfinite(sample[key])
                         for key in ('busy_cpus', 'psi_some', 'interval_s')))
        # There is no system-wide CPU FULL to corroborate "some" with: the
        # kernel records FULL for CPU only under a cgroup, never for
        # psi_system (kernel/sched/psi.c: "the FULL state doesn't exist for the
        # CPU resource at the system level"; the system root's state mask omits
        # PSI_CPU_FULL).  So the corroboration is the occupancy the sampler
        # already keeps per CPU.  The host-wide saturation gate is unchanged and
        # stands alone; what follows only decides whether a high "some" reading
        # is this action's problem or a pinned neighbour's local contention.
        holders = [p for p in self.ledger.held_dir.iterdir() if p.is_dir()]
        # Resolved once, before the pressure decision reads the demand.  The
        # caller may have pre-read the sealed identity, and the measurement and
        # ownership paths below all need the same answer rather than a second
        # read of the same CAS record.
        shape, measurement = action_identity(item) if identity is None else identity
        declared = int(demand.get('cpu', 0))
        unbounded_cpu = not declared
        # Measurements, unbounded demand and full-width reservations need the
        # host idle.  "Idle" is judged against the host's own idle history
        # (#997), which every fresh decision feeds, not a fixed fraction of
        # the CPUs that housekeeping alone can cross.
        idle = (self.idle(sample, holders) if fresh else
                {'state': 'sample_not_fresh', 'exceeds': True})
        exclusive_need = measurement or unbounded_cpu or declared == len(self.cpus)
        if isinstance(owner, str) and not measurement:
            # A dependent runs on the room its producer reserved (#985).  Host
            # pressure is not its gate: its CPUs are reserved, so nothing the
            # pool admits runs there, and the pressure its producer makes on
            # its own CPUs is the producer's.  No free token is taken.
            funded, funding_refusal = self._funding(owner, holders, demand)
            if funded is not None:
                serves_measurement = funded.pop('owner_measurement')
                self.last_decision = {"reason": "admitted_on_allowance", "sample": sample,
                                      "dependent_of": owner}
                return {'declared_cpu': int(demand.get('cpu', 0)),
                        'cost': float(int(demand.get('cpu', 0))), 'shape': shape,
                        'unbounded_cpu': False, 'measurement': False,
                        'serves_measurement': owner if serves_measurement else None,
                        'funded_by': owner, 'funded': funded, 'admitted_unix': now,
                        'preferred_borrow': 0, 'sampled_unix': sample.get('sampled_unix', 0),
                        'borrowing': False, 'host_busy_cpus': sample.get('busy_cpus'),
                        'host_psi_some': sample.get('psi_some'),
                        'active_cpu_cost': 0., 'pending_cpu_cost': 0., 'borrowable_cpus': []}
        if fresh and sample['busy_cpus'] >= .95 * len(self.cpus):
            return refuse("host_pressure", fresh=fresh)
        # True only when a high "some" was believed because the CPUs this
        # claim would actually be given are idle.  It keeps the proof and the
        # claim's real selection in step: the lending path below can hand a
        # claim a *held* CPU, which that proof never saw.
        pressure_override = False
        # The exclusive needs are judged on PSI by the idle baseline below,
        # which carries ``psi_some``: a pinned neighbour's local contention, or
        # the host's own housekeeping, is not a reason to refuse them unless it
        # is above what this host shows while idle (#997).
        full_width = declared == len(self.cpus) and not measurement and not unbounded_cpu
        if fresh and sample['psi_some'] >= .10 and (
                not exclusive_need or (full_width and holders)):
            # System-wide CPU PSI "some" counts any task anywhere waiting for a
            # CPU, so one job pinned to a few cores with more runnable threads
            # than cores holds it high while most of the box is idle (measured:
            # 0.63 with 6.7 of 80 cores busy).  It is believed only when the
            # CPUs this claim would actually be given are not idle.
            #
            # "Would actually be given" is the ledger's own rule, not a count:
            # ``begin_acquire`` takes the first ``need`` free ``cpu-*`` tokens
            # in ``_glob`` (sorted) order, and ``cpu_allocation`` maps a token
            # ordinal through ``preferred + fallback`` -- the ordinal is NOT a
            # CPU ID, so tiers such as preferred [8, 10] / fallback [2, 4] make
            # token 0 CPU 8.  Held CPUs come from ``cpu_allocation`` as well,
            # which is what carries a borrowed allocation recorded in the
            # holder's metadata.  Anything unreadable or out of range is
            # unknown, and unknown refuses rather than being read as idle.
            per_cpu = sample.get('per_cpu_busy')
            if (not isinstance(per_cpu, dict)
                    or set(per_cpu) != {str(cpu) for cpu in self.cpus}
                    or any(type(busy) not in (int, float) or not math.isfinite(busy)
                           or busy < 0 or busy > 1 for busy in per_cpu.values())):
                return refuse("host_pressure_unproven", fresh=fresh)
            if full_width:
                # Beside holders the baseline cannot say what is foreign, so
                # a full-width reservation keeps the pre-#997 refusal under
                # pressure; a learned cheap cost and an all-zero reading do
                # not reopen it.
                return refuse("host_pressure", fresh=fresh)
            held = set()
            for holder in holders:
                allocation = self.ledger.cpu_allocation(holder.name, self.tiers)
                held.update(allocation['preferred'] + allocation['fallback'])
            predicted = self._predicted_cpus(declared)
            if predicted is None:
                return refuse("host_pressure_unproven", fresh=fresh)
            # Held and foreign are recorded apart (#1160): a CPU a pool holder
            # holds is cleared by that holder draining, which the claim path
            # may withhold the box for; a busy CPU no holder holds is load the
            # pool does not own, which no drain clears.
            held_busy = sorted(cpu for cpu in predicted if cpu in held)
            foreign_busy = sorted(cpu for cpu in predicted if cpu not in held
                                  and per_cpu[str(cpu)] > IDLE_BUSY_FRACTION)
            busy = sorted(held_busy + foreign_busy)
            if busy:
                return refuse("host_pressure", fresh=fresh, cpus=busy[:8],
                              held_cpus=held_busy, foreign_cpus=foreign_busy)
            pressure_override = True
        # With holders present the sample measures them too: above the
        # host's idle history it refuses here, as the fixed line did, and
        # otherwise the holder loop below refuses the measurement
        # ``measurement_holder`` (naming ``isolated_by``, #982).
        if measurement and (not fresh or idle['exceeds']):
            return refuse("measurement_host_not_idle", fresh=fresh, baseline=idle)
        if full_width and fresh and not holders and idle['exceeds']:
            # A reservation of every CPU needs the host idle too.
            return refuse("host_pressure", fresh=fresh, baseline=idle)
        if len(holders) >= MAX_ACTIONS:
            return refuse("max_actions", holders=len(holders))
        # Legacy producers sometimes reserved only GPU/memory. Their children
        # inherit the whole worker affinity, so zero tokens are unknown CPU
        # use, never evidence of zero use. Keep that historical demand intact
        # but serialize it on a freshly idle host until the producer declares
        # an enforceable CPU allocation.
        if unbounded_cpu and (holders or not fresh or idle['exceeds']):
            return refuse("unbounded_cpu_not_exclusive", holders=len(holders), fresh=fresh,
                          baseline=idle)
        profiles = read_json(self.base / 'profiles.json')
        recent = read_json(self.base / 'jobs.json')
        next_recent = {}
        serves = None
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
                    return refuse("holder_reservation_unknown", holder=holder.name)
                continue
            if measurement:
                # ``isolated_by`` names a measurement already holding the host,
                # if any: the claim path must not withhold the box against
                # that measurement's own dependents (#982).
                isolated = holder.name if meta.get('measurement') else next(
                    (other.name for other in holders
                     if read_json(other / METADATA).get('measurement')), None)
                return refuse("measurement_holder", holder=holder.name,
                              isolated_by=isolated)
            if meta.get('measurement'):
                # A measurement's own spool exports run under its isolation;
                # everything else waits (#982).  Read only here, where a
                # measurement holds the host: this path used to refuse every
                # sibling, so the one CAS read it adds under the lock is paid
                # only while nothing else could be admitted anyway.
                if owner is _UNREAD:
                    owner = dependent_owner(item)
                if owner != holder.name:
                    return refuse("measurement_holder", holder=holder.name,
                                  isolated_by=holder.name)
                serves = holder.name
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
                    # Measurements are never learned from (``record_completion``);
                    # one is read here only beside its own dependents (#982).
                    if meta.get('shape') and not meta.get('measurement'):
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
            # A measurement's CPUs are never lent, not even to its own
            # dependents (#982): they are admitted beside it, on free tokens.
            # Nor are CPUs a dependent runs on from its producer's allowance
            # (#985): they are the producer's reservation, not idle credit.
            if (cpu is not None and meta.get('shape') and not meta.get('measurement')
                    and not meta.get('funded_by') and cost < reserved):
                lending_cpus.update(assigned)
            else:
                protected_cpus.update(assigned)
            active_cost += cost
            # Host busy already includes samples ending before this reading.
            # Charge startup/unknown attribution in full to avoid spending the
            # same headroom in concurrent admissions.
            if cpu is None:
                pending += cost
            elif (meta.get('shape') and not meta.get('measurement')
                  and not meta.get('funded_by') and cost < reserved):
                lendable = True
        self.write_state('jobs.json', next_recent)
        profiles = dict(sorted(profiles.items(), key=lambda x: x[1].get('sampled_unix', 0))[-512:])
        self.write_state('profiles.json', profiles)
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
                           and not idle['exceeds'])
        # Unbounded legacy work already proved the same exclusive idle host.
        if (fresh and not unbounded_cpu and not full_width_idle
                and max(sample['busy_cpus'] + pending, active_cost) + cost > len(self.cpus) + .01):
            return refuse("projected_cpu_cost", pending_cpu_cost=pending,
                          active_cpu_cost=active_cost, requested_cpu_cost=cost)
        available = self.ledger.available().get('cpu', 0)
        borrowable = lending_cpus - protected_cpus
        can_borrow = fresh and shape and not measurement and lendable
        preferred_borrow = (min(max(0, declared - self.ledger.free_preferred(self.tiers)),
                                len(borrowable & set(self.tiers['preferred'])))
                            if can_borrow else 0)
        borrowing = available < declared or preferred_borrow > 0
        if pressure_override:
            # The override above was earned by the CPUs this claim's own free
            # tokens map to.  A borrowed CPU is different evidence: it is a
            # *held* reservation judged idle from its holder's telemetry, not
            # from the fresh sample that proof read, so the proof does not
            # cover it -- and a borrowed CPU can be busy right now while its
            # holder's average still reads cheap.  Rather than extend the proof
            # to a CPU it never saw, the lending path is closed for this one
            # decision: the claim is still admitted when ordinary free tokens
            # cover its demand.  Ordinary borrowing, taken when no pressure
            # override is in play, is unchanged.
            if available < declared:
                return refuse("pressure_override_no_borrow",
                              available_cpu=available, declared_cpu=declared)
            can_borrow, preferred_borrow = False, 0
            borrowable = set()
            borrowing = False
        if borrowing:
            last = read_json(self.base / 'last-borrow.json').get('sampled_unix', 0)
            authorized = (fresh and shape and not measurement and lendable
                          and len(borrowable) >= declared - available
                          and sample['sampled_unix'] > last)
            if not authorized and available >= declared:
                # A preferred borrow is a placement preference, not a need:
                # free tokens already cover the demand.  When the borrow cannot
                # be authorized -- most often because this sample's one borrow
                # was already spent -- the claim takes its free tokens instead
                # of being refused.  Where those land on fallback CPUs, the
                # claim path's bounded ``deferred_for_preferred_cpu`` decides
                # whether to wait for a preferred one; a refusal here cost one
                # sample interval per item for nothing (#924).
                preferred_borrow, borrowing = 0, False
            elif not authorized:
                return refuse("borrow_evidence_unavailable", fresh=fresh, shape=shape,
                              lendable=lendable, available_cpu=available,
                              declared_cpu=declared, borrowable_cpus=len(borrowable),
                              last_borrow_sampled_unix=last)
        self.last_decision = {"reason": "admitted", "sample": sample}
        return {'declared_cpu': declared, 'cost': cost, 'shape': shape,
                'unbounded_cpu': unbounded_cpu,
                'measurement': measurement, 'serves_measurement': serves,
                'dependent_allowance': dict(allowance) if allowance else None,
                # What an exclusive admission was judged idle against, so
                # measurements can be compared on the baselines they ran on.
                'idle_baseline': idle if exclusive_need and fresh else None,
                'admitted_unix': now,
                'preferred_borrow': preferred_borrow,
                'sampled_unix': sample.get('sampled_unix', 0), 'borrowing': borrowing,
                'host_busy_cpus': sample.get('busy_cpus'), 'host_psi_some': sample.get('psi_some'),
                'active_cpu_cost': active_cost, 'pending_cpu_cost': pending,
                'borrowable_cpus': sorted(borrowable, key=lambda c: sample.get('per_cpu_busy', {}).get(str(c), 0.))}

    def admitted(self, metadata):
        """Spend the sample's borrow freshness; answer what it replaced."""
        if not metadata.get('borrowing'):
            return None
        previous = read_json(self.base / 'last-borrow.json')
        metadata['borrow_id'] = uuid.uuid4().hex
        self.write_state('last-borrow.json', {
            'sampled_unix': metadata['sampled_unix'], 'borrow_id': metadata['borrow_id']})
        return previous

    def withdrew(self, metadata, previous):
        """Give the freshness back when the claim it was spent on never happened.

        A borrowing decision is spent at the decision, not at the rename, so a
        claimant that loses the rename has already consumed the sample.  It
        never used it: its tokens went back and no borrowed CPU was ever
        occupied, so keeping the record would refuse the retry that the lost
        race is supposed to allow.

        Only while the record is still this decision's own.  A newer borrow
        that landed in between owns it now, and writing an older sample over
        that one would let the newer sample authorize a second borrow -- the
        one thing ``admitted`` exists to prevent.  So this is a
        compare-and-set, and it belongs under the same lock as ``admitted``.
        """
        if not metadata.get('borrowing'):
            return
        # A host sample can be returned and borrowed again. Its timestamp is
        # not the owner of that later borrow. Retire this caller's authority
        # before I/O too: a write followed by an exception is still a return.
        borrow_id = metadata.pop('borrow_id', None)
        if not borrow_id:
            return
        current = read_json(self.base / 'last-borrow.json')
        if (current.get('borrow_id') != borrow_id
                or current.get('sampled_unix') != metadata.get('sampled_unix')):
            return
        self.write_state('last-borrow.json', previous or {})


def learn_export(ledger, template_sha256, owner, published_unix, landing_s, family=None):
    """Remember one landed export of ``template_sha256`` (#999), under the
    ``family`` its template declares when it declares one (#1126).

    ``landing_s`` is the export's own wall time; ``published_unix`` its row's
    publication, whose gap from the same producer's previous export is one
    group spacing.  Kept per :func:`export_rate_key`, ``EXPORT_RATE_MEMORY``
    of each, under the admission lock and never waited for: a busy box learns
    it next time.  Nothing is learned without a key.
    """
    key = export_rate_key(template_sha256, family)
    if (key is None or not _is_key(owner)
            or not all(type(v) in (int, float) and math.isfinite(v) and v > 0
                       for v in (published_unix, landing_s))):
        return False
    tiers = read_json(ledger.base / 'cpu-map.json')
    if not tiers:
        return False
    controller = Controller(ledger, tiers)
    try:
        with controller.locked():
            rates = read_json(controller.base / EXPORT_RATES)
            entry = dict(rates.get(key) or {})
            last = dict(entry.get('last_published') or {})
            previous = last.get(owner)
            if type(previous) in (int, float) and published_unix <= previous:
                return False
            if type(previous) in (int, float):
                entry['spacing_s'] = (list(entry.get('spacing_s', []))
                                      + [published_unix - previous])[-EXPORT_RATE_MEMORY:]
            entry['landing_s'] = (list(entry.get('landing_s', []))
                                  + [landing_s])[-EXPORT_RATE_MEMORY:]
            last[owner] = published_unix
            entry['last_published'] = dict(sorted(last.items(), key=lambda x: x[1])[-EXPORT_RATE_MEMORY:])
            entry['learned_unix'] = time.time()
            rates[key] = entry
            rates = dict(sorted(rates.items(), key=lambda x: x[1].get('learned_unix', 0))[-512:])
            controller.write_state(EXPORT_RATES, rates)
    except AdmissionBusy:
        return False
    return True


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

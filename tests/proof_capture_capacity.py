"""Observe capture storage and use the admitting generation's floor owner."""
from contextlib import contextmanager
import importlib.util
import json
import os
from pathlib import Path
import socket
import stat
import sys
import time

from proof_container_runtime import CAPTURE_BOUNDS, CAPTURE_PUBLISHED_SOURCES, canonical, digest, inventory


CAPACITY_SCHEMA = 'prismabuild.proof_capture_capacity.v1'
PRIVATE_FREE_BYTES = 27 * 1024**3
PUBLICATION_GROWTH_GIB = 10
PUBLICATION_LEASE_S = 600
STATVFS_FIELDS = ('f_bsize', 'f_frsize', 'f_blocks', 'f_bfree', 'f_bavail',
                 'f_files', 'f_ffree', 'f_favail', 'f_flag', 'f_namemax')
OWNER_FIELDS = {'schema', 'holder', 'label', 'host', 'machine_id', 'boot_id',
                'pid', 'pid_start', 'lease_until'}


def bounded_capture_json(value, *, prefix=''):
    raw = canonical(value) + b'\n'
    if len(prefix.encode()) + len(raw) > CAPTURE_BOUNDS['metadata_bytes']:
        raise ValueError('capture metadata exceeds its sealed byte limit')
    return raw


def published_capture_sources(published):
    descriptors = {}
    total = 0
    for name in CAPTURE_PUBLISHED_SOURCES:
        path = published / name
        info = path.lstat()
        total += info.st_size
        if not stat.S_ISREG(info.st_mode) or total > CAPTURE_BOUNDS['supplemental_bytes']:
            raise ValueError('published capture sources exceed the supplemental byte contract')
        entry = inventory(path)['.']
        descriptors[name] = dict(path=str(path), resolved_path=str(path.resolve(strict=True)),
                                 bytes=entry['bytes'], sha256=entry['sha256'])
    return descriptors


def check_published_capture_modules(published, sources):
    expected = {str(published / name) for name in sources if name.startswith('src/prismabuild/')}
    for name, module in tuple(sys.modules.items()):
        if name == 'prismabuild' or name.startswith('prismabuild.'):
            path = getattr(module, '__file__', None)
            spec = getattr(module, '__spec__', None)
            if path not in expected or spec is None or spec.origin != path:
                raise ValueError('capture imported an unsealed product module: ' + name)
    if published_capture_sources(published) != sources:
        raise ValueError('published capture owner sources changed during execution')


def load_published_capture_floor(published, sources):
    check_published_capture_modules(published, sources)
    if 'prismabuild' not in sys.modules:
        path = published / 'src/prismabuild/__init__.py'
        spec = importlib.util.spec_from_file_location('prismabuild', path,
            submodule_search_locations=[str(path.parent)])
        package = importlib.util.module_from_spec(spec)
        sys.modules['prismabuild'] = package
        spec.loader.exec_module(package)
    from prismabuild import filesystem_floor
    check_published_capture_modules(published, sources)
    return filesystem_floor


def observed_operation_owners(floor, queue, cas, action):
    directory = floor._growth_ledger(queue, cas).base / 'operations'
    owners = []
    process_start = floor._process_start(os.getpid())
    if not isinstance(process_start, str) or not process_start.isdigit():
        raise ValueError('capture process start identity is unknown')
    with os.scandir(directory) as paths:
        for index, item in enumerate(paths):
            if index >= CAPTURE_BOUNDS['members']:
                raise ValueError('operation owner observation exceeds its member limit')
            path = Path(item.path)
            if not item.name.startswith(floor.OPERATION_PREFIX) or not item.name.endswith('.json'):
                continue
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_size > CAPTURE_BOUNDS['metadata_bytes']:
                raise ValueError('operation owner is not bounded regular JSON')
            with path.open('rb') as handle:
                raw = handle.read(CAPTURE_BOUNDS['metadata_bytes'] + 1)
            if len(raw) > CAPTURE_BOUNDS['metadata_bytes']:
                raise ValueError('operation owner exceeded its metadata bound while reading')
            value = json.loads(raw)
            if value.get('pid') != os.getpid() or value.get('label') != action:
                continue
            if (set(value) != OWNER_FIELDS or value['schema'] != floor.OWNER_SCHEMA
                    or value['host'] != socket.gethostname()
                    or value['machine_id'] != floor._machine_id()
                    or value['boot_id'] != floor._floor_boot_id()
                    or value['pid_start'] != process_start
                    or type(value['pid']) is not int or type(value['lease_until']) not in (int, float)
                    or not time.time() < value['lease_until'] <= time.time() + PUBLICATION_LEASE_S
                    or path.name != value['holder'] + '.json'):
                raise ValueError('actual operation owner identity differs from the capture process')
            after = path.lstat()
            if any(getattr(after, name) != getattr(info, name) for name in
                   ('st_dev', 'st_ino', 'st_mode', 'st_size', 'st_mtime_ns', 'st_ctime_ns')):
                raise ValueError('operation owner changed during observation')
            owners.append(dict(path=str(path), bytes=len(raw), sha256=digest(raw), raw_json=raw.decode()))
    if len(owners) != 1:
        raise ValueError('capture needs exactly one actual matching growth operation owner')
    return owners


@contextmanager
def capture_publication_capacity(private, cas, published):
    queue_value = os.environ.get('PRISMABUILD_QUEUE_ROOT', '')
    action = os.environ.get('PRISMABUILD_ACTION_KEY', '')
    queue = Path(queue_value)
    if (not queue.is_absolute() or not queue.is_dir() or len(action) != 64
            or any(char not in '0123456789abcdef' for char in action)):
        raise ValueError('capture needs actual admitted queue and action identities')
    private, cas, queue = private.resolve(strict=True), cas.resolve(strict=True), queue.resolve(strict=True)
    sample = os.statvfs(private)
    available = sample.f_bavail * sample.f_frsize
    if available < PRIVATE_FREE_BYTES:
        raise ValueError('capture private filesystem is below its current 27GiB capacity threshold')
    sources = published_capture_sources(published)
    floor = load_published_capture_floor(published, sources)
    publication = dict(queue_root=str(queue), action_key=action, cas_root=str(cas),
        mode_before=floor.mode(queue, max_age_s=0.0), mode_inside=None, mode_after=None,
        growth_gib=PUBLICATION_GROWTH_GIB, lease_s=PUBLICATION_LEASE_S, verdicts=None,
        operation_owners=[], owner_observation_error=None, held_allowance_observed=False,
        owner_paths_absent_after=[])
    record = dict(schema=CAPACITY_SCHEMA,
        private=dict(path=str(private), minimum_free_bytes=PRIVATE_FREE_BYTES,
            statvfs={name:getattr(sample, name) for name in STATVFS_FIELDS},
            available_bytes=available, observation_only=True),
        publication=publication, published_sources=sources)
    with floor.operation(queue, used_paths=[private, cas], growth_gib={cas:PUBLICATION_GROWTH_GIB},
                         label=action, lease_s=PUBLICATION_LEASE_S) as verdicts:
        publication['verdicts'] = verdicts
        publication['mode_inside'] = floor.mode(queue, max_age_s=0.0)
        if publication['mode_before'] != publication['mode_inside']:
            raise ValueError('filesystem floor mode changed during capture admission')
        if publication['mode_inside'] != 'off':
            try:
                publication['operation_owners'] = observed_operation_owners(floor, queue, cas, action)
            except (ValueError, OSError, floor.FloorError) as exc:
                publication['owner_observation_error'] = str(exc)
                if publication['mode_inside'] == 'enforce':
                    raise
        publication['held_allowance_observed'] = (
            publication['mode_inside'] == 'enforce' and publication['owner_observation_error'] is None
            and len(publication['operation_owners']) == 1
            and all(value['allowed'] is True for value in verdicts))
        if publication['mode_inside'] == 'enforce' and not publication['held_allowance_observed']:
            raise ValueError('enforced capture publication lacks observed acquisition evidence')
        bounded_capture_json(record)
        yield record
    publication['mode_after'] = floor.mode(queue, max_age_s=0.0)
    if publication['mode_after'] != publication['mode_before']:
        raise ValueError('filesystem floor mode changed during capture publication')
    for owner in publication['operation_owners']:
        path = Path(owner['path'])
        try:
            path.lstat()
        except FileNotFoundError:
            publication['owner_paths_absent_after'].append(str(path))
        else:
            raise ValueError('capture operation owner still exists after publication: ' + str(path))
    check_published_capture_modules(published, sources)
    bounded_capture_json(record)

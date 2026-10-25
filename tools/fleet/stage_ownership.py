"""Bounded one-shot evidence collection for stage_reclaim's read-only report.

No lifecycle decisions belong here. Budgets are cooperative between filesystem
operations, not deadlines capable of interrupting a blocked filesystem syscall.
"""
from __future__ import annotations

from contextlib import contextmanager
import math
import os
from pathlib import Path
import stat
import time

from prismabuild import data_manifest, digest_primitives, manifest_layout, reader_lease, staged_path


class _Budget(ValueError):
    pass


class _IO:
    def __init__(self, metadata, seconds):
        self.started = time.monotonic()
        self.deadline = self.started + seconds
        self.limit = metadata
        self.bytes = 0
        self.errors = []
        self.exhausted = set()
        self.documents = {}
        self.raw = {}

    def check(self):
        if time.monotonic() >= self.deadline:
            self.exhausted.add('max_seconds')
            raise _Budget('max_seconds exhausted')

    def no_links(self, path):
        path = Path(path).absolute()
        for component in [*reversed(path.parents), path]:
            self.check()
            if stat.S_ISLNK(component.lstat().st_mode):
                raise ValueError(f'{component}: symlink excluded')

    @contextmanager
    def scandir(self, path):
        self.check()
        try:
            self.no_links(path)
            entries = os.scandir(path)
        except FileNotFoundError:
            yield iter(())
            return
        with entries:
            def checked():
                for entry in entries:
                    self.check()
                    yield entry
            yield checked()

    def read_document(self, path, *, max_bytes):
        self.check()
        path = Path(path)
        key = str(path)
        if key in self.documents:
            return self.documents[key]
        remaining = min(max_bytes, self.limit - self.bytes)
        if remaining <= 0:
            self.exhausted.add('max_metadata_bytes')
            raise _Budget('max_metadata_bytes exhausted')
        # No symlink records, and no unbounded reads of even one document.
        self.no_links(path)
        size = path.lstat()
        if not stat.S_ISREG(size.st_mode):
            raise ValueError(f'{path}: metadata is not a regular file')
        if size.st_size > remaining:
            self.exhausted.add('max_metadata_bytes')
            raise _Budget(f'{path}: max_metadata_bytes exhausted')
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        with os.fdopen(fd, 'rb') as stream:
            raw = stream.read(remaining)
        self.bytes += len(raw)
        self.check()
        value = digest_primitives.decode_strict_json(raw, where=str(path))
        self.raw[key] = raw
        self.documents[key] = value
        return value

    def manifest_layout(self, cas, digest):
        from prismabuild import core as pb
        path = pb.PrismaBuildCAS(Path(cas)).blob_path(digest)
        document = self.read_document(path, max_bytes=128 << 20)
        if pb.new_sha256(self.raw[str(path)]).hexdigest() != digest:
            raise ValueError(f'{path}: manifest digest mismatch')
        document = data_manifest.validate_data_manifest(document)
        entries = manifest_layout.read_entries(document)
        return document['mount_prefix'], [dict(entry) for entry in entries]

    def validated_request(self, cas, key):
        from prismabuild import core as pb
        try:
            path = Path(cas) / 'requests' / key[:2] / f'{key}.json'
            self.read_document(path, max_bytes=16 << 20)
            request = pb.validate_action(pb.digest_primitives.decode_strict_json(
                self.raw[str(path)], where='ownership report sealed request'))
            if str(request.get('action_key') or '') != key:
                return None, 'sealed request identity mismatch'
            return request, ''
        except (OSError, ValueError, TypeError) as exc:
            return None, str(exc)


def collect_stage_ownership(queue, *, tier_id, stage_root, cas_root, residency_root,
                            max_files, max_metadata_bytes, max_seconds,
                            reference_census, source_mark):
    for name, value in [('max_files', max_files),
                        ('max_metadata_bytes', max_metadata_bytes),
                        ('max_seconds', max_seconds)]:
        if value < 0 or not math.isfinite(value):
            raise ValueError(f'{name} must be finite and nonnegative')
        if name != 'max_seconds' and not isinstance(value, int):
            raise ValueError(f'{name} must be an integer')
    io = _IO(max_metadata_bytes, max_seconds)
    stage = Path(stage_root).absolute()
    root = Path(os.path.normpath(stage))
    excluded = []
    rows = {}
    observations = []
    keys = ('observed_apparent_bytes', 'observed_allocated_bytes',
            'owned_bytes', 'source_only_bytes', 'unattributed_bytes',
            'unknown_bytes', 'fragment_declared_bytes', 'source_mark_bytes',
            'source_mark_fragment_overlap_bytes')

    def row(name):
        return rows.setdefault(name, dict(name=name, directory=name, path=str(root / name),
            **{k: 0 for k in keys}, references=[], complete=True))

    def error(path, exc):
        io.errors.append({'record_path': str(path), 'reason': str(exc)})

    def work():
        try:
            io.check()
            return True
        except _Budget:
            return False

    def inside(value):
        """Lexical containment plus component lstat; never import via links."""
        try:
            path = Path(os.path.normpath(value))
            relative = path.relative_to(root)
            if not relative.parts:
                return None
            current = root
            for component in relative.parts:
                io.check()
                current /= component
                try:
                    if stat.S_ISLNK(current.lstat().st_mode):
                        return None
                except FileNotFoundError:
                    # Missing declarations are still evidence, not observations.
                    continue
            return relative
        except (OSError, ValueError):
            return None

    def row_name(relative):
        return relative.parts[0] if len(relative.parts) > 1 else '.'

    def walk(directory, name):
        try:
            directory.lstat()  # A discovered directory disappearing is not empty.
            with io.scandir(directory) as entries:
                for entry in entries:
                    path = Path(entry.path)
                    mode = entry.stat(follow_symlinks=False).st_mode
                    if stat.S_ISLNK(mode):
                        excluded.append({'path': str(path), 'reason': 'symlink'})
                    elif stat.S_ISDIR(mode):
                        walk(path, name)
                    elif stat.S_ISREG(mode):
                        if len(observations) >= max_files:
                            io.exhausted.add('max_files')
                            row(name)['complete'] = False
                            return
                        info = path.lstat()
                        observations.append((path, name, info, source_mark(path)))
                    else:
                        excluded.append({'path': str(path), 'reason': 'not regular file'})
        except (OSError, ValueError) as exc:
            error(directory, exc)
            row(name)['complete'] = False

    # Discover all top-level rows before spending the payload file budget.
    try:
        io.check()
        if stat.S_ISLNK(stage.lstat().st_mode):
            raise ValueError('stage root is a symlink')
        with io.scandir(root) as entries:
            for entry in entries:
                mode = entry.stat(follow_symlinks=False).st_mode
                if entry.name == staged_path.STAGE_ROOT_MARKER:
                    excluded.append({'path': entry.path, 'reason': 'stage-root control metadata'})
                    continue
                if stat.S_ISDIR(mode):
                    row(entry.name)
                elif stat.S_ISLNK(mode):
                    excluded.append({'path': entry.path, 'reason': 'symlink'})
                elif stat.S_ISREG(mode):
                    # Root-level files have an explicit synthetic directory row.
                    row('.')
                else:
                    excluded.append({'path': entry.path, 'reason': 'not regular file'})
        for name in list(rows):
            if name != '.':
                walk(root / name, name)
        if '.' in rows:
            with io.scandir(root) as entries:
                for entry in entries:
                    if entry.name != staged_path.STAGE_ROOT_MARKER and entry.is_file(follow_symlinks=False):
                        if len(observations) >= max_files:
                            io.exhausted.add('max_files')
                            rows['.']['complete'] = False
                            break
                        path = Path(entry.path)
                        observations.append((path, '.', path.lstat(), source_mark(path)))
    except (OSError, ValueError) as exc:
        error(root, exc)

    references = []
    unknown_scopes = []
    fragments_by_path = {}
    positive_paths = set()
    census = None
    metadata_error_start = len(io.errors)
    try:
        io.check()
        census = reference_census(queue, stage=root, tier_id=tier_id,
                                  cas_root=cas_root, residency_root=residency_root, io=io)
    except (OSError, ValueError) as exc:
        error(queue.root, exc)
    if census is not None:
        for record in census.records:
            path = record['record_path']
            document = record['document']
            ref = {'kind': 'record', **record}
            references.append(ref)
            for value in record['paths']:
                if not work():
                    break
                relative = inside(value)
                if relative is None:
                    excluded.append({'path': value, 'record_path': path,
                                     'reason': 'reference outside stage or symlink'})
                elif row_name(relative) in rows:
                    rows[row_name(relative)]['references'].append(ref)
                    positive_paths.add(value)
        references.extend(census.references)
        unknown_scopes.extend(census.refusals)

    residence = Path(residency_root or queue.root / 'residency')
    fragments = []
    tainted = []
    try:
        fragments, tainted = staged_path.fragment_census(residence, io=io)
    except (OSError, ValueError) as exc:
        error(residence, exc)
    for message in tainted:
        error(residence, message)
    from prismabuild import stage_release
    wanted_movers, live = stage_release.live_claims(
        queue, evidence=census.records if census is not None else [])
    live_actions = set(live)
    for namespace, mover, document, direct in fragments:
        if not work():
            break
        ref = {'kind': 'fragment', 'namespace': namespace, 'mover': mover,
               **document, 'potentially_stale': document.get('consumer_action_key') not in live_actions}
        ref['record_path'] = io.fragment_records[id(document)]
        ref['live_consumer_observed'] = document.get('consumer_action_key') in live_actions
        ref['live_mover_wanted'] = mover in wanted_movers
        references.append(ref)
        attached = set()
        for file in document.get('entries', {}).values():
            if not work():
                break
            value = file.get('stage_path')
            relative = inside(value) if isinstance(value, str) else None
            if relative is None or row_name(relative) not in rows:
                if value:
                    excluded.append({'path': value, 'reason': 'fragment outside stage or symlink'})
                continue
            name = row_name(relative)
            fragments_by_path.setdefault(value, []).append(ref)
            positive_paths.add(value)
            rows[name]['fragment_declared_bytes'] += int(file.get('bytes', 0))
            attached.add(name)
        for name in attached:
            rows[name]['references'].append(ref)

    try:
        pins, taint = reader_lease._bounded_live_for(
            reader_lease.leases_root(queue, residence), None, io)
        for message in taint:
            error(residence, message)
        for value, ids in pins.items():
            relative = inside(value)
            if relative is None or row_name(relative) not in rows:
                continue
            ref = {'kind': 'reader_lease', 'path': value, 'pins': ids,
                   'protection_only': True,
                   'record_paths': [path for path, doc in io.documents.items()
                                    if isinstance(doc, dict) and doc.get('pin_id') in ids]}
            for record_path in ref['record_paths']:
                lease = {'kind': 'reader_lease', **io.documents[record_path],
                         'record_path': record_path, 'protection_only': True}
                lease['refs'] = list(lease.get('refs', {}).values())
                references.append(lease)
                rows[row_name(relative)]['references'].append(lease)
            positive_paths.add(value)
    except (OSError, ValueError) as exc:
        error(residence, exc)

    # Explicit identity joins; holder names are not substring searches of paths.
    def identities(value):
        found = set()
        if not work():
            return found
        if isinstance(value, dict):
            for key, child in value.items():
                if key in ('action_key', 'consumer_action_key', 'mover_action_key') and isinstance(child, str):
                    found.add(child)
                elif key == 'leads' and isinstance(child, list):
                    found.update(str(lead) for lead in child)
                elif isinstance(child, (dict, list)):
                    found.update(identities(child))
        elif isinstance(value, list):
            for child in value:
                found.update(identities(child))
        return found
    directory_identities = {name: identities(directory['references'])
                            for name, directory in rows.items()}
    # Token files themselves are holder evidence, not content documents.
    try:
        held = queue.tier(tier_id).paths.held
        with io.scandir(held) as holders:
            for holder in holders:
                if not holder.is_dir(follow_symlinks=False):
                    continue
                tokens = {}
                paths = []
                with io.scandir(Path(holder.path)) as entries:
                    for entry in entries:
                        if '-' in entry.name and entry.is_file(follow_symlinks=False):
                            kind = entry.name.rsplit('-', 1)[0]
                            tokens[kind] = tokens.get(kind, 0) + 1
                            paths.append(entry.path)
                ref = {'kind': 'holder', 'holder': holder.name,
                       'record_path': holder.path, 'token_paths': paths, 'holdings': tokens}
                references.append(ref)
                attached = False
                for name, directory in rows.items():
                    if holder.name in directory_identities[name]:
                        directory['references'].append(ref)
                        attached = True
                if tokens.get('stage_gib') and not attached:
                    error(holder.path, 'held stage scope has no resolved path evidence')
    except (OSError, ValueError) as exc:
        error(queue.root, exc)

    # Any unreadable attribution has global uncertainty unless W1 supplied
    # an explicit unresolved-manifest destination bound for that diagnostic.
    scoped_errors = {(u['record_path'], u['reason']) for u in unknown_scopes}
    global_unknown = any((e.get('record_path'), e.get('reason')) not in scoped_errors
                         for e in io.errors[metadata_error_start:])
    if tainted or io.exhausted.intersection({'max_seconds', 'max_metadata_bytes'}):
        global_unknown = True
    for path, name, info, mark in observations:
        directory = rows[name]
        size = info.st_size
        directory['observed_apparent_bytes'] += size
        if not hasattr(info, 'st_blocks'):
            directory['observed_allocated_bytes'] = None
        elif directory['observed_allocated_bytes'] is not None:
            directory['observed_allocated_bytes'] += info.st_blocks * 512
        marked = mark is not None
        if marked:
            directory['source_mark_bytes'] += size
        fragment = str(path) in fragments_by_path
        if fragment and marked:
            directory['source_mark_fragment_overlap_bytes'] += size
        uncertain = global_unknown or any(
            str(path) == scope or Path(scope) in path.parents
            for refusal in unknown_scopes for scope in refusal['scope'])
        category = ('owned' if str(path) in positive_paths else
                    'unknown' if uncertain else 'source_only' if marked else 'unattributed')
        directory[category + '_bytes'] += size
    for budget in sorted(io.exhausted):
        error(root, f'budget exhausted: {budget}')
    complete = not io.errors and not io.exhausted and not unknown_scopes
    for directory in rows.values():
        directory['complete'] = directory['complete'] and complete
        directory['deletion_authorized'] = False
    totals = {k: (None if any(directory[k] is None for directory in rows.values())
                  else sum(directory[k] for directory in rows.values())) for k in keys}
    def accounting(target):
        target['observed'] = {'basis': 'regular_file_lstat',
                              'apparent_bytes': target['observed_apparent_bytes'],
                              'allocated_bytes': target['observed_allocated_bytes']}
        target['physical_bytes'] = {key: target[key + '_bytes'] for key in
            ('owned', 'source_only', 'unattributed', 'unknown')}
        target['declarations'] = dict(fragment_bytes=target['fragment_declared_bytes'],
            source_mark_bytes=target['source_mark_bytes'],
            source_mark_fragment_overlap_bytes=target['source_mark_fragment_overlap_bytes'])
    for directory in rows.values():
        accounting(directory)
        directory['potentially_stale'] = any(r.get('potentially_stale') for r in directory['references'])
        directory['fragments'] = [r for r in directory['references'] if r.get('kind') == 'fragment']
        directory['readers'] = [r for r in directory['references'] if r.get('kind') == 'reader_lease']
        directory['holders'] = [r for r in directory['references'] if r.get('kind') == 'holder']
        directory['movements'] = []
        directory['plans'] = []
        for ref in directory['references']:
            if ref.get('kind') != 'record':
                continue
            path = Path(ref['record_path'])
            item = {**ref['document'], 'record_path': str(path)}
            if 'movers' in path.parts or 'movers-retired' in path.parts:
                item['state'] = 'retired' if 'movers-retired' in path.parts else 'active'
                directory['movements'].append(item)
            elif 'plans' in path.parts or 'residency-plans' in path.parts:
                directory['plans'].append(item)
    accounting(totals)
    for ref in references:
        ref.setdefault('record_path', None)
        ref.setdefault('manifest_sha256', None)
        ref.setdefault('paths', [])
    return {'directories': list(rows.values()), 'totals': totals,
            'references': references, 'unresolved_manifests': unknown_scopes,
            'errors': io.errors, 'excluded': excluded,
            'scan': {'max_files': max_files, 'max_metadata_bytes': max_metadata_bytes,
                     'max_seconds': max_seconds, 'files_observed': len(observations),
                     'metadata_bytes_read': io.bytes,
                     'elapsed_seconds': time.monotonic() - io.started,
                     'exhausted': sorted(io.exhausted),
                     'unscanned_size_bytes': None if not complete else 0},
            'accounting_basis': 'Regular-file lstat apparent bytes; allocated bytes are st_blocks * 512 where available. No symlinks followed. Hardlinks, reflinks, compression and snapshots prevent any claim of filesystem-wide unique consumption. Concurrent non-atomic scan.',
            'complete': complete, 'read_only': True, 'deletion_authorized': False}

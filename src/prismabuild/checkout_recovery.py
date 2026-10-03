"""Plan-first, explicitly authorized recovery of banked execution checkouts.

This is not a reaper. It never discovers deletion candidates, releases a hold,
repairs a queue, extracts an archive, or deletes anything except through the
materializer's existing cleanup owner. The caller keeps the host quiescent and
obtains RootGO for the exact Core-canonical plan digest before applying it.
"""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import ExitStack
import hashlib
import math
import os
from pathlib import Path, PurePosixPath
import re
import socket
import stat
import subprocess
import tarfile
import time

from . import core as pb, materialize, pool

PLAN_SCHEMA = "prismabuild.checkout_recovery_plan.v1"
RESULT_SCHEMA = "prismabuild.checkout_recovery_result.v1"
MAX_ENTRIES = 32
# Legacy terminal records can retain captured logs (observed 1,721,943 bytes).
# Keep evidence bounded while permitting that actual retained protocol.
MAX_JSON_BYTES = 2 * 1024 * 1024
MAX_MEMBERS = 100_000
MAX_PROCESS_MAPS = 100_000
MAX_MANIFEST_BYTES = 32 * 1024 * 1024
CHUNK_BYTES = 1024 * 1024
_HEX = re.compile(r"[0-9a-f]{64}\Z")
_MAP_RECORD = re.compile(
    r'([0-9a-f]{1,16})-([0-9a-f]{1,16}) +([r-][w-][x-][ps]) +'
    r'([0-9a-f]{1,16}) +([0-9a-f]{1,8}):([0-9a-f]{1,8}) +'
    r'([0-9]{1,20})(?: +(.*))?\Z')
_MAP_RANGE = re.compile(r'[0-9a-f]{1,16}-[0-9a-f]{1,16}\Z')


class CheckoutRecoveryRefusal(ValueError):
    """Missing, changed, or unprovable deletion authority."""


def _refuse(message: str) -> None:
    raise CheckoutRecoveryRefusal(message)


def _gate_uid() -> int:
    """Private fixture seam; the production maintenance authority is root."""
    return 0


def _digest(value: object) -> str:
    return pb.canonical_sha256(value)


def _identity(info: os.stat_result) -> dict[str, int]:
    return dict(dev=info.st_dev, ino=info.st_ino, uid=info.st_uid,
                gid=info.st_gid, mode=info.st_mode, size=info.st_size,
                mtime_ns=info.st_mtime_ns, ctime_ns=info.st_ctime_ns,
                nlink=info.st_nlink)


def _path(value: object, where: str) -> Path:
    if not isinstance(value, (str, Path)):
        _refuse(f"{where} must be an absolute exact path")
    text = str(value)
    path = Path(text)
    if (not path.is_absolute() or path == Path('/') or '..' in path.parts
            or str(path) != text or '\x00' in text):
        _refuse(f"{where} must be an absolute exact non-root path")
    return path


def _directory(path: Path, where: str) -> dict[str, int]:
    fd = pb._open_directory_nofollow(path, where=where)
    try:
        info = os.fstat(fd)
        pb._assert_directory_identity(fd, path, where=where)
        return _identity(info)
    finally:
        os.close(fd)


def _json(path: Path, where: str, *, readonly: bool = False) -> tuple[dict, str]:
    raw = pb._read_regular_file_nofollow(path, where=where,
                                        max_bytes=MAX_JSON_BYTES, require_readonly=readonly)
    value = pb._decode_strict_json(raw, where=where)
    if not isinstance(value, dict):
        _refuse(f"{where} must be a JSON object")
    return value, hashlib.sha256(raw).hexdigest()


def _finite(value: object) -> bool:
    try:
        return type(value) in (float, int) and math.isfinite(float(value))
    except OverflowError:
        return False


def _gate(path: Path, owner: str) -> dict:
    before = _identity(path.lstat())
    if (not stat.S_ISREG(before['mode']) or before['uid'] != _gate_uid()
            or before['mode'] & 0o022 or before['nlink'] != 1):
        _refuse('unsafe maintenance gate ownership, type, or mode')
    value, sha = _json(path, 'maintenance gate')
    if (value.get('schema') != 'prismabuild.resource-maintenance.v1'
            or value.get('draining') is not True or value.get('owner') != owner
            or not _finite(value.get('changed_unix'))):
        _refuse('maintenance gate is not closed under the exact stated owner/epoch')
    if _identity(path.lstat()) != before:
        _refuse('maintenance gate changed during read')
    return dict(path=str(path), identity=before, sha256=sha,
                changed_unix=value['changed_unix'])


def _selections(entries: object, local: Path, bank: Path) -> list[dict]:
    if not isinstance(entries, list) or not 1 <= len(entries) <= MAX_ENTRIES:
        _refuse(f'entries must be an explicit nonempty list of at most {MAX_ENTRIES}')
    normalized = []
    paths = set()
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {
                'action_key', 'path', 'archive_path', 'archive_sha256'}:
            _refuse('each selection must carry exactly key, path, archive path and SHA256')
        key, sha = entry['action_key'], entry['archive_sha256']
        if not isinstance(key, str) or not _HEX.fullmatch(key):
            _refuse('action_key must be full lowercase SHA256')
        if not isinstance(sha, str) or not _HEX.fullmatch(sha):
            _refuse('archive_sha256 must be full lowercase SHA256')
        root = _path(entry['path'], 'checkout path')
        archive = _path(entry['archive_path'], 'archive path')
        if root.parent != local or not re.fullmatch(
                re.escape(key[:12]) + r'\.[a-z0-9_]{8}', root.name):
            _refuse('checkout path is not the exact generated materializer root')
        if root in paths:
            _refuse('duplicate checkout selection')
        paths.add(root)
        if not archive.is_relative_to(bank) or archive == bank:
            _refuse('archive is outside the stated bank root')
        if not str(archive).endswith(('.tar', '.tar.zst')):
            _refuse('archive must be .tar or .tar.zst')
        normalized.append(dict(entry))
    for entry in normalized:
        root = Path(entry['path'])
        for protected in (bank, Path(entry['archive_path'])):
            if protected == root or protected.is_relative_to(root):
                _refuse('bank or archive is under a deletion root')
    return normalized


def _terminal(queue: pool.PoolQueue, key: str, proc: Path) -> tuple[dict, dict]:
    for state in (pool.READY, pool.CLAIMED):
        path = queue.item_path(state, key)
        try:
            path.lstat()
        except FileNotFoundError:
            pass
        else:
            _refuse(f'{key} is present in {state}')
    readable, unreadable = queue.read_terminal_candidates(key, max_bytes=MAX_JSON_BYTES)
    ending = queue.resolve_ending(readable, unreadable)
    if unreadable:
        reasons = "; ".join(str(entry["reason"]) for entry in unreadable)
        _refuse(f"{key} terminal evidence is unreadable: {reasons}")
    if (ending['ambiguous'] or ending['state'] not in
            (pool.DONE, pool.FAILED, pool.WITHDRAWN) or ending['generation'] is None):
        _refuse(f'{key} has no complete unambiguous terminal generation')
    record = ending['record']
    if (record.get('schema') != pool.POOL_OUTCOME_SCHEMA_V1
            or record.get('action_key') != key or not isinstance(record.get('status'), str)
            or not record['status'] or type(record.get('attempts')) is not int
            or record['attempts'] < 0 or not _finite(record.get(
                'withdrawn_unix' if ending['state'] == pool.WITHDRAWN else 'finished_unix'))):
        _refuse(f'{key} has incomplete or malformed terminal metadata')
    for other, (_, candidate) in readable.items():
        if candidate.get('action_key') != key or not _finite(candidate.get('published_unix')):
            _refuse(f'{key} has malformed {other} terminal authority')
    snapshot = pb.validate_pbrun_checkout_snapshot(record.get('checkout_snapshot'))
    cas_root = _path(record.get('cas_root'), 'terminal CAS root')
    request_path = cas_root / 'requests' / key[:2] / f'{key}.json'
    request, request_sha = _json(request_path, 'sealed action request', readonly=True)
    action = pb.validate_action(request)
    if action['action_key'] != key:
        _refuse('sealed request action key differs from selection')
    sealed_snapshot = pb.validate_pbrun_checkout_snapshot(
        action['params'].get('checkout_snapshot'))
    if snapshot != sealed_snapshot or snapshot['input'] not in action['inputs']:
        _refuse('terminal snapshot differs from the sealed action input')
    bundle = pb.PrismaBuildCAS(cas_root).input_path(snapshot['input'])
    lifetime = dict(cas_root=str(cas_root), request_sha256=request_sha, bundle_path=str(bundle),
                    bundle_identity=_identity(bundle.lstat()), lease=None, pids=[])
    try:
        lease, lease_sha = _json(queue.lease_path(key), 'claim lease')
    except FileNotFoundError:
        lease = None
    if lease is not None:
        heartbeat = lease.get('heartbeat_unix')
        if (lease.get('schema') != pool.POOL_LEASE_SCHEMA_V1
                or lease.get('action_key') != key or not _finite(heartbeat)
                or float(heartbeat) > time.time() - pool.LEASE_TIMEOUT_S):
            _refuse('present lease is malformed or live')
        lifetime['lease'] = dict(sha256=lease_sha,
                                identity=_identity(queue.lease_path(key).lstat()))
    pids = set()
    for source in (record, lease or {}):
        for field in ('pid', 'child_pid'):
            pid = source.get(field)
            if pid is not None:
                if type(pid) is not int or pid <= 0:
                    _refuse('malformed execution PID evidence')
                pids.add(pid)
    for pid in sorted(pids):
        try:
            (proc / str(pid)).lstat()
        except FileNotFoundError:
            pass
        else:
            _refuse(f'execution PID {pid} still exists')
    lifetime['pids'] = sorted(pids)
    terminal = dict(state=ending['state'], path=str(ending['path']),
                    generation=ending['generation'], attempt=record['attempts'],
                    snapshot=snapshot, record_sha256=_digest(record),
                    queue_records={state: _digest(value[1])
                                   for state, value in sorted(readable.items())})
    return terminal, lifetime


def _hash_stream(stream) -> tuple[str, int]:
    sha = hashlib.sha256()
    size = 0
    while chunk := stream.read(CHUNK_BYTES):
        sha.update(chunk)
        size += len(chunk)
    return sha.hexdigest(), size


def _manifest_digest(manifest: dict) -> str:
    sha = hashlib.sha256()
    for name, entry in sorted(manifest.items()):
        sha.update(pb._canonical_file_bytes([name, entry]))
    return sha.hexdigest()


def _add(manifest: dict, name: str, entry: dict, budget: list[int]) -> None:
    if name in manifest:
        _refuse(f'duplicate archive/tree member: {name}')
    budget[0] += len(pb._canonical_bytes([name, entry]))
    if len(manifest) >= MAX_MEMBERS or budget[0] > MAX_MANIFEST_BYTES:
        _refuse('tree/archive manifest exceeds metadata/count bound')
    manifest[name] = entry


def _tree(root: Path) -> tuple[dict, dict, dict]:
    manifest, identities, budget = {}, {}, [0]
    rootfd = pb._open_directory_nofollow(root, where='execution checkout')
    device = os.fstat(rootfd).st_dev

    def visit(fd: int, relative: str) -> None:
        before = _identity(os.fstat(fd))
        identities[relative] = before
        _add(manifest, relative, dict(type='directory', mode=stat.S_IMODE(before['mode']),
                                     uid=before['uid'], gid=before['gid']), budget)
        with os.scandir(fd) as listing:
            for member in listing:
                name = member.name if relative == '.' else f'{relative}/{member.name}'
                path = root / name
                info = os.stat(member.name, dir_fd=fd, follow_symlinks=False)
                identity = _identity(info)
                if info.st_dev != device:
                    _refuse('checkout contains a foreign filesystem/mount')
                if stat.S_ISDIR(info.st_mode):
                    child = os.open(member.name, os.O_RDONLY | os.O_DIRECTORY |
                                    os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=fd)
                    try:
                        if _identity(os.fstat(child)) != identity:
                            _refuse('directory changed during tree scan')
                        visit(child, name)
                    finally:
                        os.close(child)
                elif stat.S_ISREG(info.st_mode):
                    child, parent = pb._open_regular_nofollow(path, where='checkout member')
                    try:
                        if _identity(os.fstat(child)) != identity:
                            _refuse('file changed before hashing')
                        with os.fdopen(os.dup(child), 'rb') as stream:
                            sha, size = _hash_stream(stream)
                        pb._assert_regular_identity(child, parent, path, where='checkout member')
                        if _identity(os.fstat(child)) != identity or size != info.st_size:
                            _refuse('file changed during hashing')
                    finally:
                        os.close(child)
                        os.close(parent)
                    identities[name] = identity
                    _add(manifest, name, dict(type='file', mode=stat.S_IMODE(info.st_mode),
                         uid=info.st_uid, gid=info.st_gid, bytes=size, sha256=sha), budget)
                elif stat.S_ISLNK(info.st_mode):
                    target = os.readlink(member.name, dir_fd=fd)
                    identities[name] = identity
                    _add(manifest, name, dict(type='symlink', mode=stat.S_IMODE(info.st_mode),
                         uid=info.st_uid, gid=info.st_gid, target=target), budget)
                else:
                    _refuse(f'unrecognized checkout member type: {name}')
                if _identity(os.stat(member.name, dir_fd=fd, follow_symlinks=False)) != identity:
                    _refuse('checkout member changed during scan')
        if _identity(os.fstat(fd)) != before:
            _refuse('checkout directory changed during scan')

    try:
        visit(rootfd, '.')
        pb._assert_directory_identity(rootfd, root, where='execution checkout')
    finally:
        os.close(rootfd)
    summary = dict(manifest_sha256=_manifest_digest(manifest),
                   identities_sha256=_digest(identities), member_count=len(manifest),
                   total_bytes=sum(entry.get('bytes', 0) for entry in manifest.values()))
    return manifest, identities, summary


class _BoundedTarReader:
    """Bound long/PAX metadata reads as well as streamed payload reads."""
    def __init__(self, stream, limit):
        self.stream = stream
        self.limit = limit
        self.count = 0
        self.tail = b''

    def read(self, size=-1):
        if size < 0 or size > MAX_JSON_BYTES:
            _refuse('archive metadata read exceeds bound')
        raw = self.stream.read(size)
        self.count += len(raw)
        if self.count > self.limit:
            _refuse('archive expansion exceeds full-tree plus bounded metadata allowance')
        self.tail = (self.tail + raw)[-1024:]
        return raw


class _BoundedTarInfo(tarfile.TarInfo):
    def _proc_member(self, archive):
        if self.size < 0:
            _refuse('archive member has a negative size')
        # Bound extended bodies before tarfile's buffering stream allocates them.
        if self.type in (tarfile.XHDTYPE, tarfile.XGLTYPE,
                         tarfile.GNUTYPE_LONGNAME, tarfile.GNUTYPE_LONGLINK):
            count = getattr(archive, '_recovery_metadata_bytes', 0) + self.size + 512
            if self.size > MAX_JSON_BYTES or count > MAX_MANIFEST_BYTES:
                _refuse('archive extended metadata exceeds bound')
            archive._recovery_metadata_bytes = count
        return super()._proc_member(archive)


def _archive_name(raw: str, root_name: str, layout: str | None) -> tuple[str, str]:
    parts = PurePosixPath(raw).parts
    if raw.startswith('/') or '..' in parts or '\x00' in raw:
        _refuse('unsafe archive member pathname')
    if layout is None:
        layout = 'outer' if parts and parts[0] == root_name else 'dot'
    if layout == 'outer':
        if not parts or parts[0] != root_name:
            _refuse('archive contains an unrelated root')
        parts = parts[1:]
    elif parts and parts[0] == root_name:
        _refuse('archive mixes generated-root and dot layouts')
    return '/'.join(parts) or '.', layout


def _archive(path: Path, bank: Path, bank_uid: int, root: Path,
             expected_sha: str, tree: dict) -> dict:
    parents = {}
    for parent in (bank, *reversed(path.parent.relative_to(bank).parents), path.parent):
        # Only descendants of bank need bank policy; absolute ancestors have
        # already been checked no-follow by Core (including private /tmp fixtures).
        candidate = parent if parent.is_absolute() else bank / parent
        if candidate.is_relative_to(bank):
            identity = _directory(candidate, 'archive bank directory')
            if identity['uid'] not in (0, bank_uid) or identity['mode'] & 0o022:
                _refuse('untrusted writable or foreign archive bank directory')
            parents[str(candidate)] = identity
    fd, parentfd = pb._open_regular_nofollow(path, where='bank archive')
    child = None
    try:
        before = _identity(os.fstat(fd))
        if before['mode'] & 0o222 or before['uid'] not in (0, bank_uid) or before['nlink'] != 1:
            _refuse('bank archive must be trusted, single-link and read-only')
        with os.fdopen(os.dup(fd), 'rb') as stream:
            sha, size = _hash_stream(stream)
        if sha != expected_sha or size != before['size']:
            _refuse('archive SHA256 differs from approved selection')
        os.lseek(fd, 0, os.SEEK_SET)
        stream = os.fdopen(os.dup(fd), 'rb')
        if str(path).endswith('.tar.zst'):
            # No pathname is reopened; zstd reads the held no-follow inode.
            try:
                child = subprocess.Popen(['zstd', '-q', '-d', '-c'], stdin=stream,
                                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            finally:
                stream.close()
            stream = child.stdout
        manifest, budget, layout = {}, [0], None
        payload_bound = sum((entry.get('bytes', 0) + 511) // 512 * 512
                            for entry in tree.values())
        reader = _BoundedTarReader(stream, payload_bound + len(tree) * 512 +
                                   MAX_MANIFEST_BYTES + MAX_JSON_BYTES)
        try:
            with tarfile.open(fileobj=reader, mode='r|', bufsize=65536,
                              tarinfo=_BoundedTarInfo) as archive:
                for member in archive:
                    name, layout = _archive_name(member.name, root.name, layout)
                    common = dict(mode=member.mode, uid=member.uid, gid=member.gid)
                    if member.isdir():
                        entry = dict(type='directory', **common)
                    elif member.issym():
                        entry = dict(type='symlink', target=member.linkname, **common)
                    elif member.isfile():
                        if tree.get(name, {}).get('bytes') != member.size:
                            _refuse('archive file size differs from original member')
                        payload = archive.extractfile(member)
                        if payload is None:
                            _refuse('archive regular member has no payload')
                        with payload:
                            digest, count = _hash_stream(payload)
                        if count != member.size:
                            _refuse('incomplete archive regular-file payload')
                        entry = dict(type='file', bytes=count, sha256=digest, **common)
                    elif member.islnk():
                        target, _ = _archive_name(member.linkname, root.name, layout)
                        original = manifest.get(target)
                        if not original or original['type'] != 'file':
                            _refuse('archive hardlink has no earlier regular-file target')
                        entry = dict(original, **common)
                    else:
                        _refuse('archive contains a special or unsupported member')
                    _add(manifest, name, entry, budget)
                    if tree.get(name) != entry:
                        _refuse(f'archive does not preserve original member: {name}')
                    archive.members.clear()
                # tarfile stops at its first zero header. Require all remaining
                # bytes to be zero and at least two complete terminating blocks.
                end_offset = archive.offset
                while raw := archive.fileobj.read(CHUNK_BYTES):
                    if any(raw):
                        _refuse('archive contains bytes after its end marker')
            if (reader.count % 512 or reader.count < end_offset + 1024
                    or len(reader.tail) != 1024 or any(reader.tail)):
                _refuse('archive is incomplete: missing full tar end marker')
            if manifest != tree:
                _refuse('archive is an incomplete full-tree bank copy')
        finally:
            stream.close()
        if child is not None and child.wait(timeout=30) != 0:
            _refuse('zstd refused or did not finish the bank archive')
        pb._assert_regular_identity(fd, parentfd, path, where='bank archive')
        if _identity(os.fstat(fd)) != before or _identity(path.lstat()) != before:
            _refuse('bank archive changed during verification')
        return dict(identity=before, parents=parents, manifest_sha256=_manifest_digest(manifest),
                    member_count=len(manifest), total_bytes=sum(
                        entry.get('bytes', 0) for entry in manifest.values()))
    finally:
        if child is not None and child.poll() is None:
            child.kill()
            child.wait()
        os.close(fd)
        os.close(parentfd)


def _process_fields(path: Path) -> list[str]:
    raw = pb._read_regular_file_nofollow(path, where='process stat', max_bytes=65536)
    text = raw.decode('utf-8', errors='strict')
    closing = text.rfind(')')
    prefix = path.parent.name + ' ('
    if (not text.startswith(prefix) or closing < len(prefix)
            or text[closing:closing + 2] != ') '):
        _refuse('malformed process lifetime census')
    fields = text[closing + 2:].split()
    if (len(fields) < 20 or fields[0] not in ('R', 'S', 'D', 'Z', 'T', 't', 'X', 'x', 'K', 'W', 'P', 'I')
            or not re.fullmatch(r'[0-9]{1,20}', fields[6])
            or not re.fullmatch(r'[0-9]{1,20}', fields[19])
            or int(fields[6]) >= 1 << 64 or int(fields[19]) >= 1 << 64):
        _refuse('malformed process lifetime census')
    return fields


def _process_stat(path: Path) -> tuple[str, str]:
    fields = _process_fields(path)
    return fields[0], fields[19]


def _mapped_path_reference(target: str, roots: list[str]) -> bool:
    # maps escapes newlines, not arbitrary backslashes; either spelling can
    # name a selected path. A deleted suffix never hides the original name.
    for spelling in (target, target.replace('\\012', '\n')):
        clean = spelling.removesuffix(' (deleted)')
        if any(clean == root or clean.startswith(root + '/') for root in roots):
            return True
    return False


def _process_maps(pidroot: Path, roots: list[str], inodes: set[tuple[int, int]],
                  *, kernel_thread: bool = False) -> tuple[bytes, dict[str, str]]:
    """Prove complete mapping metadata, never stat a possibly deleted target.

    maps supplies the kernel device/inode, including external hardlink and
    bind aliases. map_files independently accounts for file-backed VMA ranges
    and names; following its symlinks would need additional Linux capabilities
    and would wrongly depend on a pathname still existing.
    """
    raw = pb._read_regular_file_nofollow(pidroot / 'maps', where='process maps',
                                        max_bytes=MAX_JSON_BYTES)
    if not raw and not kernel_thread:
        _refuse(f'process maps empty/incomplete for live PID {pidroot.name}')
    if (raw and kernel_thread) or (raw and not raw.endswith(b'\n')):
        _refuse(f'process maps truncated/inconsistent for PID {pidroot.name}')
    file_maps = {}
    previous_end = 0
    if raw.count(b'\n') > MAX_PROCESS_MAPS:
        _refuse('process maps count exceeds bound')
    lines = raw.decode('utf-8', errors='strict').split('\n')[:-1]
    for line in lines:
        match = _MAP_RECORD.fullmatch(line)
        if match is None or '\x00' in line or '\r' in line:
            _refuse(f'malformed process maps for PID {pidroot.name}')
        start, end = int(match[1], 16), int(match[2], 16)
        major, minor, inode = int(match[5], 16), int(match[6], 16), int(match[7])
        if start < previous_end or start >= end or inode >= 1 << 64:
            _refuse(f'malformed process map identity/range for PID {pidroot.name}')
        previous_end = end
        device = os.makedev(major, minor)
        if os.major(device) != major or os.minor(device) != minor:
            _refuse(f'malformed process map device for PID {pidroot.name}')
        target = match[8] or ''
        if _mapped_path_reference(target, roots):
            _refuse(f'live PID {pidroot.name} maps a selected checkout path')
        if inode and (device, inode) in inodes:
            _refuse(f'live PID {pidroot.name} maps a selected checkout inode')
        pseudo = target.startswith('[') and target.endswith(']')
        if (target and not target.startswith('/') and not pseudo
                or inode and not target
                or not inode and (device or target.startswith('/'))):
            _refuse(f'malformed process map identity/path for PID {pidroot.name}')
        if inode:
            # map_files uses unpadded lowercase %lx ranges even when maps pads
            # a low address. Duplicate/overlapping ranges were refused above.
            file_maps[f'{start:x}-{end:x}'] = target
    mapdir = pidroot / 'map_files'
    fd = pb._open_directory_nofollow(mapdir, where='process map_files')
    targets = {}
    text_bytes = 0
    try:
        with os.scandir(fd) as listing:
            for member in listing:
                name = member.name
                if len(targets) >= MAX_PROCESS_MAPS:
                    _refuse('process map_files count exceeds bound')
                if (not _MAP_RANGE.fullmatch(name) or name not in file_maps
                        or name in targets or not member.is_symlink()):
                    _refuse(f'malformed/incomplete process map_files for PID {pidroot.name}')
                target = os.readlink(name, dir_fd=fd)
                text_bytes += len(os.fsencode(name)) + len(os.fsencode(target))
                if text_bytes > MAX_JSON_BYTES:
                    _refuse('process map_files metadata exceeds byte bound')
                if not target or '\x00' in target:
                    _refuse(f'empty/malformed process map_files target for PID {pidroot.name}')
                if _mapped_path_reference(target, roots):
                    _refuse(f'live PID {pidroot.name} maps a selected checkout via map_files')
                mapped = file_maps[name]
                if (mapped.startswith('/')
                        and target not in (mapped, mapped.replace('\\012', '\n'))):
                    _refuse(f'process maps/map_files identity changed for PID {pidroot.name}')
                targets[name] = target
        if targets.keys() != file_maps.keys():
            _refuse(f'process maps/map_files incomplete for PID {pidroot.name}')
        pb._assert_directory_identity(fd, mapdir, where='process map_files')
    finally:
        os.close(fd)
    return raw, targets


def _census(proc: Path, entries: list[dict], identities: list[dict]) -> None:
    _directory(proc, 'process census root')
    roots = [entry['path'] for entry in entries]
    keys = [entry['action_key'].encode() for entry in entries]
    inodes = {(entry['dev'], entry['ino'])
              for tree in identities for entry in tree.values()}
    listing = {name for name in os.listdir(proc) if name.isdigit()}
    lifetimes = {}
    for name in sorted(listing, key=int):
        pidroot = proc / name
        pidfd = None
        try:
            pidfd = pb._open_directory_nofollow(pidroot, where='process lifetime directory')
            state, started = _process_stat(pidroot / 'stat')
            pidinfo = os.fstat(pidfd)
            lifetimes[name] = (started, pidinfo.st_dev, pidinfo.st_ino)
            if state in ('Z', 'X', 'x'):
                after_state, after_started = _process_stat(pidroot / 'stat')
                if after_started != started or after_state not in ('Z', 'X', 'x'):
                    _refuse('process lifetime changed during census; replan')
                pb._assert_directory_identity(pidfd, pidroot, where='process lifetime directory')
                continue
            raw = pb._read_regular_file_nofollow(pidroot / 'cmdline',
                    where='process cmdline', max_bytes=MAX_JSON_BYTES)
            if any(key in raw for key in keys) or any(root.encode() in raw for root in roots):
                _refuse(f'live PID {name} command references selected checkout/action')
            descriptors = os.listdir(pidroot / 'fd')
            links = [pidroot / 'cwd', pidroot / 'exe']
            links += [pidroot / 'fd' / fd for fd in descriptors]
            kernel_thread = False
            for link in links:
                try:
                    target = os.readlink(link)
                except FileNotFoundError:
                    if link.name == 'exe' and not raw and not descriptors:
                        fields = _process_fields(pidroot / 'stat')
                        if fields[19] != started or not int(fields[6]) & 0x00200000:
                            _refuse(f'process executable census incomplete for live PID {name}')
                        kernel_thread = True  # PF_KTHREAD proves no userspace mm.
                        continue
                    if link.parent.name == 'fd':
                        continue  # an FD closed during the census
                    raise
                clean = target.removesuffix(' (deleted)')
                if any(clean == root or clean.startswith(root + '/') for root in roots):
                    _refuse(f'live PID {name} references selected checkout via {link.name}')
                try:
                    info = link.stat()
                except FileNotFoundError:
                    if link.parent.name == 'fd':
                        continue
                    raise
                if (info.st_dev, info.st_ino) in inodes:
                    _refuse(f'live PID {name} holds a selected checkout inode')
            maps = _process_maps(pidroot, roots, inodes, kernel_thread=kernel_thread)
            if _process_maps(pidroot, roots, inodes, kernel_thread=kernel_thread) != maps:
                _refuse(f'process maps/map_files changed during census for PID {name}; replan')
            after_state, after_started = _process_stat(pidroot / 'stat')
            if after_started != started or after_state in ('Z', 'X', 'x'):
                _refuse('process lifetime changed during census; replan')
            pb._assert_directory_identity(pidfd, pidroot, where='process lifetime directory')
        except (FileNotFoundError, ProcessLookupError):
            if pidroot.exists():
                _refuse(f'process census incomplete for live PID {name}')
        except (OSError, UnicodeError, pb.PrismaBuildError) as exc:
            _refuse(f'process census unreadable for PID {name}: {exc}')
        finally:
            if pidfd is not None:
                os.close(pidfd)
    remaining = {name for name in os.listdir(proc) if name.isdigit()}
    if remaining - listing:
        _refuse('process census changed during scan; retry while quiescent')
    for name in sorted(remaining, key=int):
        pidroot = proc / name
        try:
            _, started = _process_stat(pidroot / 'stat')
            info = _directory(pidroot, 'process lifetime directory')
            if lifetimes.get(name) != (started, info['dev'], info['ino']):
                _refuse('process lifetime changed during census; replan')
        except (OSError, UnicodeError, pb.PrismaBuildError) as exc:
            _refuse(f'process census incomplete during lifetime recheck for PID {name}: {exc}')


def _assert_no_mounts(entries: list[dict]) -> None:
    # A bind mount can share st_dev with its parent: never cross into a bank/CAS.
    path = Path('/proc') / str(os.getpid()) / 'mountinfo'
    raw = pb._read_regular_file_nofollow(path, where='checkout mount census',
                                        max_bytes=MAX_JSON_BYTES)
    roots = [Path(entry['path']) for entry in entries]
    lines = raw.decode('utf-8', errors='strict').splitlines()
    if not lines:
        _refuse('checkout mount census is empty/incomplete')
    for line in lines:
        fields = line.split()
        if len(fields) < 10 or '-' not in fields:
            _refuse('malformed checkout mount census')
        separator = fields.index('-')
        options = ','.join((fields[5], fields[separator + 3]))
        if any(option.startswith('hidepid=') and option != 'hidepid=0'
               for option in options.split(',')):
            _refuse('hidden process census cannot prove checkout abandonment')
        target = fields[4]
        for code in ('040', '011', '012', '134'):
            target = target.replace(chr(92) + code, chr(int(code, 8)))
        mounted = Path(target)
        if not mounted.is_absolute():
            _refuse('malformed checkout mount target')
        if any(mounted == root or mounted.is_relative_to(root) for root in roots):
            _refuse('selected checkout contains a filesystem/bind mount')



def _assert_tree_identity(root: Path, tree: dict) -> None:
    for name, expected in tree.items():
        path = root if name == '.' else root / name
        if stat.S_ISDIR(expected['mode']):
            observed = _directory(path, 'checkout directory identity')
        else:
            parent = pb._open_directory_nofollow(path.parent, where='checkout member parent')
            try:
                observed = _identity(os.stat(path.name, dir_fd=parent, follow_symlinks=False))
            finally:
                os.close(parent)
        if observed != expected:
            _refuse('checkout lifetime/file identity changed; replan')


def _recheck(queue, planned, identities, proc) -> None:
    for entry, tree in zip(planned, identities):
        _assert_tree_identity(Path(entry['path']), tree)
        terminal, lifetime = _terminal(queue, entry['action_key'], proc)
        if terminal != entry['terminal'] or lifetime != entry['lifetime']:
            _refuse('terminal/lease/request/snapshot identity changed; replan')
        for path, identity in entry['archive']['parents'].items():
            if _directory(Path(path), 'archive bank directory') != identity:
                _refuse('archive bank directory changed; replan')
        path = Path(entry['archive_path'])
        fd, parent = pb._open_regular_nofollow(path, where='bank archive identity')
        try:
            pb._assert_regular_identity(fd, parent, path, where='bank archive identity')
            if _identity(os.fstat(fd)) != entry['archive']['identity']:
                _refuse('bank archive changed after verification; replan')
        finally:
            os.close(fd)
            os.close(parent)


def _capture(queue, entries, local, bank, gate, owner, proc) -> tuple[dict, list[dict]]:
    if not isinstance(owner, str) or not owner or len(owner) > 256:
        _refuse('maintenance_owner must be explicit and bounded')
    queue_root = _path(queue.root, 'queue root')
    selections = _selections(entries, local, bank)
    roots = [Path(entry['path']) for entry in selections]
    if any(protected == root or protected.is_relative_to(root)
           for root in roots for protected in (queue_root, gate, proc)):
        _refuse('protected queue/gate/process authority is under a deletion root')
    local_identity = _directory(local, 'materializer local root')
    queue_identity = _directory(queue_root, 'existing queue root')
    bank_identity = _directory(bank, 'bank root')
    if bank_identity['mode'] & 0o022:
        _refuse('bank root is group/world writable')
    maintenance = _gate(gate, owner)
    planned, tree_identities = [], []
    for selection in selections:
        root = Path(selection['path'])
        identity = _directory(root, 'generated execution checkout')
        if (identity['uid'] not in (0, local_identity['uid'])
                or bank_identity['uid'] not in (0, identity['uid'])
                or identity['mode'] & 0o022):
            _refuse('foreign or writable execution checkout/bank owner')
        terminal, lifetime = _terminal(queue, selection['action_key'], proc)
        if any(Path(lifetime['cas_root']).is_relative_to(selected) for selected in roots):
            _refuse('protected CAS authority is under a deletion root')
        repository = root / 'checkout'
        _directory(repository, 'materialized repository')
        _directory(repository / '.git', 'materialized Git directory')
        env = {key: value for key, value in os.environ.items()
               if not key.startswith('GIT_')}
        env.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL='/dev/null',
                   GIT_OPTIONAL_LOCKS='0')
        head = pb._git(repository, '-c', 'safe.directory=' + str(repository),
                       'rev-parse', '--verify', 'HEAD', env=env).strip()
        if head != terminal['snapshot']['commit']:
            _refuse('materialized HEAD differs from the selected sealed snapshot')
        manifest, identities, summary = _tree(root)
        archive = _archive(Path(selection['archive_path']), bank, bank_identity['uid'],
                           root, selection['archive_sha256'], manifest)
        if _directory(root, 'generated execution checkout') != identity:
            _refuse('execution checkout changed during archive verification')
        planned.append(dict(selection, directory_identity=identity, terminal=terminal,
                            archive=archive, tree=summary, lifetime=lifetime))
        tree_identities.append(identities)
    _assert_no_mounts(selections)
    _census(proc, selections, tree_identities)
    _recheck(queue, planned, tree_identities, proc)
    if (_directory(local, 'materializer local root') != local_identity
            or _directory(queue_root, 'existing queue root') != queue_identity
            or _directory(bank, 'bank root') != bank_identity):
        _refuse('configured root changed during capture')
    if _gate(gate, owner) != maintenance:
        _refuse('maintenance epoch changed during plan capture')
    plan = dict(schema=PLAN_SCHEMA, complete=True, status='planned',
                host=socket.gethostname(), queue_root=str(queue_root), local_root=str(local),
                bank_root=str(bank), maintenance_owner=owner, maintenance=maintenance,
                roots=dict(local=local_identity, queue=queue_identity, bank=bank_identity),
                entries=planned)
    return plan, tree_identities


def _failure(exc: Exception, removed: list[str] | None = None, *, started=False) -> dict:
    removed = [] if removed is None else removed
    return dict(schema=RESULT_SCHEMA, complete=False,
                status='incomplete' if removed or started else 'refused',
                errors=[str(exc) or type(exc).__name__], removed=removed)


def prepare_checkout_recovery(queue, entries, *, local_root=materialize.LOCAL_CHECKOUT_ROOT,
                              bank_root, maintenance_gate=Path('/run/prismabuild/maintenance.json'),
                              maintenance_owner, proc_root=Path('/proc')) -> dict:
    """Return a complete bounded plan, or a JSON-serializable refusal; never delete."""
    try:
        return _capture(queue, entries, _path(local_root, 'local root'),
                        _path(bank_root, 'bank root'), _path(maintenance_gate, 'maintenance gate'),
                        maintenance_owner, _path(proc_root, 'process root'))[0]
    except Exception as exc:  # Public boundary reports all refusals, never fake success.
        return _failure(exc)


def apply_checkout_recovery(queue, plan, *, expected_plan_sha256,
                            local_root=materialize.LOCAL_CHECKOUT_ROOT, bank_root,
                            maintenance_gate=Path('/run/prismabuild/maintenance.json'),
                            maintenance_owner, proc_root=Path('/proc')) -> dict:
    """Re-prove every entry under all key locks, then invoke the sole cleanup owner.

    Applying is not atomic across trees. A later helper failure leaves an explicit
    incomplete result, keeps the maintenance hold, and requires operator review.
    """
    removed = []
    started = False
    try:
        if os.geteuid() != 0:
            _refuse('apply requires root/euid 0')
        if (not isinstance(expected_plan_sha256, str)
                or not _HEX.fullmatch(expected_plan_sha256)
                or not isinstance(plan, dict) or _digest(plan) != expected_plan_sha256):
            _refuse('exact canonical plan SHA256 does not match RootGO')
        if plan.get('schema') != PLAN_SCHEMA or plan.get('complete') is not True:
            _refuse('apply requires a complete checkout recovery plan')
        local, bank = _path(local_root, 'local root'), _path(bank_root, 'bank root')
        gate, proc = _path(maintenance_gate, 'maintenance gate'), _path(proc_root, 'process root')
        selections = [{key: entry[key] for key in
                       ('action_key', 'path', 'archive_path', 'archive_sha256')}
                      for entry in plan.get('entries', [])]
        selections = _selections(selections, local, bank)
        queue_root = _path(queue.root, 'queue root')
        _directory(queue_root, 'existing queue root')
        if (plan.get('queue_root') != str(queue_root)
                or plan.get('local_root') != str(local)
                or plan.get('bank_root') != str(bank)
                or plan.get('maintenance_owner') != maintenance_owner
                or plan.get('host') != socket.gethostname()):
            _refuse('plan belongs to different roots, maintenance owner or host')
        with ExitStack() as held:
            for key in sorted({entry['action_key'] for entry in selections}):
                if not held.enter_context(queue._transition_locked(key, blocking=False)):
                    _refuse(f'transition lock busy for {key}')
            current, identities = _capture(queue, selections, local, bank, gate,
                                            maintenance_owner, proc)
            if current != plan:
                _refuse('plan identities/queue/gate/archive/tree drifted; replan')
            # All candidates have passed before the first delete. The final
            # no-follow identity pass closes the archive/process scan interval.
            _recheck(queue, current['entries'], identities, proc)
            _assert_no_mounts(selections)
            _census(proc, selections, identities)
            if _gate(gate, maintenance_owner) != current['maintenance']:
                _refuse('maintenance gate changed before deletion')
            for entry, tree in zip(current['entries'], identities):
                root = Path(entry['path'])
                if (_directory(root, 'selected deletion root') != entry['directory_identity']
                        or _gate(gate, maintenance_owner) != current['maintenance']):
                    _refuse('checkout or maintenance identity changed before cleanup')
                _assert_tree_identity(root, tree)
                started = True
                materialize._cleanup_execution_checkout(local, root,
                                                       {'action_key': entry['action_key']})
                try:
                    root.lstat()
                except FileNotFoundError:
                    removed.append(str(root))
                else:
                    _refuse(f'existing cleanup owner did not remove {root}; recovery incomplete')
        return dict(schema=RESULT_SCHEMA, complete=True, status='applied', errors=[],
                    removed=removed, plan_sha256=expected_plan_sha256)
    except Exception as exc:  # Includes unexpected cleanup failure after partial removal.
        return _failure(exc, removed, started=started)

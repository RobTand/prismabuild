"""Best-effort admission diagnostic copies, with one publisher per host/ledger.

Admission never waits for this process. The permanent local publication flock
is handed to the child across exec; even if the parent exits or the child
blocks in NFS, another worker cannot start a competing publisher. Only kernel
descriptor closure releases that slot. The child inherits no admission lock,
pipe to an action's captured output, or shared working directory.
"""
from __future__ import annotations

import errno
import fcntl
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time
import uuid

FILES = ('cpu-sample.json', 'jobs.json', 'profiles.json', 'last-borrow.json', 'gpu-state.json',
         'claim-denials.json')
#: Records whose copy is stamped as a copy; their readers show the stamp.
STAMPED = ('cpu-sample.json', 'gpu-state.json')
MIN_PUBLISH_INTERVAL_S = 1.0
_ENTRY = Path(__file__).resolve()
_children: list[subprocess.Popen] = []


def _read(path):
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _write(path, value):
    from prismabuild import immutable_files
    immutable_files.write_json_atomic(path, value)


def _start_ticks(pid):
    # The command name may contain spaces or parentheses.
    try:
        return int(Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19])
    except (OSError, ValueError, IndexError):
        return None


def _source_directory(local: Path):
    info = local.lstat()
    if (not stat.S_ISDIR(info.st_mode) or not info.st_mode & 0o444
            or not info.st_mode & 0o111):
        raise OSError(errno.EINVAL, 'unsafe adaptive source directory')
    return info.st_dev, info.st_ino


def _source_signature(local: Path):
    """Identify local atomic replacements, without reading the shared copy.

    This is a diagnostic retry hint, never admission authority. Capture it
    before copying: a write during the copy must still request a later pass.
    """
    directory = _source_directory(local)
    signature = {}
    for name in FILES:
        try:
            info = (local / name).lstat()
        except FileNotFoundError:
            if _source_directory(local) != directory:
                raise OSError(errno.ESTALE, 'adaptive source directory replaced')
            continue
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or not info.st_mode & 0o444):
            raise OSError(errno.EINVAL, f'unsafe adaptive source: {name}')
        signature[name] = [info.st_dev, info.st_ino, info.st_size,
                           info.st_mtime_ns, info.st_ctime_ns]
    return signature


def next_publication(local: Path, *, now: float) -> float | None:
    """Return a dirty copy's earliest start using only host-local records."""
    signature = _source_signature(local)
    result = _read(local / 'publisher-result.json')
    initially_empty = not signature and not result
    published_current = (result.get('status') == 'published'
                         and result.get('source_signature') == signature)
    if initially_empty or published_current:
        return None
    owner = _read(local / 'publisher-owner.json')
    elapsed = now - owner.get('started_monotonic', -MIN_PUBLISH_INTERVAL_S)
    if 0 <= elapsed < MIN_PUBLISH_INTERVAL_S:
        return now + MIN_PUBLISH_INTERVAL_S - elapsed
    return now


def publish(local: Path, shared: Path):
    """Try to start a copy; return immediately if another publisher owns it.

    The caller has already released admission. All filesystem operations here
    are host-local; the remote destination is passed as text to the child.
    Failure to publish never changes a claim result or grants admission credit.
    """
    _children[:] = [child for child in _children if child.poll() is None]
    descriptor = None
    try:
        descriptor = os.open(local / 'publish.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_nlink != 1):
            raise OSError('unsafe adaptive CPU publication lock')
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return None
        now = time.monotonic()
        deadline = next_publication(local, now=now)
        if deadline is None or deadline > now:
            return None
        nonce = uuid.uuid4().hex
        child = subprocess.Popen(
            [sys.executable, str(_ENTRY), str(local), str(shared), str(descriptor), nonce],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            cwd='/', close_fds=True, pass_fds=(descriptor,), start_new_session=True)
        _children.append(child)
        _write(local / 'publisher-owner.json', {
            'pid': child.pid, 'start_ticks': _start_ticks(child.pid), 'nonce': nonce,
            'started_monotonic': now, 'started_unix': time.time(),
            'source': str(local), 'destination': str(shared)})
        return child
    except (OSError, ValueError, TypeError) as exc:
        # A full local disk, missing interpreter or malformed diagnostic marker
        # must not override an already acquired claim or its original failure.
        try:
            print(f'pool: adaptive CPU snapshot not published: {exc}', file=sys.stderr, flush=True)
        except (OSError, ValueError):
            pass
        return None
    finally:
        if descriptor is not None:
            # Do not LOCK_UN: the child owns this same open-file description.
            os.close(descriptor)


def _previous_snapshot(path: Path, value: dict, *, stamped: bool):
    from prismabuild import immutable_files
    from prismabuild.digest_primitives import _canonical_bytes
    # The writer adds only source and a finite copied_unix timestamp. Its
    # canonical stamp fits 128 bytes; source profiles retain their own size.
    limit = len(_canonical_bytes(value))
    if stamped:
        limit += 128
    try:
        info = path.lstat()
        if stat.S_ISREG(info.st_mode) and info.st_size > limit:
            return None  # A larger old profile is unequal, without reading it.
        return json.loads(immutable_files.read_replaced_record(
            path, where='previous adaptive mirror', max_bytes=limit))
    except (FileNotFoundError, ValueError):
        return None


def copy_snapshot(local: Path, shared: Path, *, changed: list[str] | None = None):
    """Publish each local record independently; source timestamps stay intact."""
    directory = _source_directory(local)
    copied = []
    for name in FILES:
        path = local / name
        try:
            from prismabuild import immutable_files
            value = json.loads(immutable_files.read_replaced_record(path, where='adaptive mirror source'))
        except FileNotFoundError:
            if _source_directory(local) != directory:
                raise OSError(errno.ESTALE, 'adaptive source directory replaced')
            try:
                (shared / name).unlink()
            except FileNotFoundError:
                pass
            else:
                if changed is not None:
                    changed.append(name)
            continue
        if not isinstance(value, dict):
            raise ValueError(f'invalid local CPU state: {name}')
        previous = _previous_snapshot(shared / name, value, stamped=name in STAMPED)
        if isinstance(previous, dict) and name in STAMPED:
            previous.pop('_snapshot', None)
        if previous != value:
            if name in STAMPED:
                value['_snapshot'] = {'source': 'host-local', 'copied_unix': time.time()}
            _write(shared / name, value)
            if changed is not None:
                changed.append(name)
        copied.append(name)
    return copied


def main(argv):
    local, shared = Path(argv[0]), Path(argv[1])
    descriptor, nonce = int(argv[2]), argv[3]
    result = {'nonce': nonce, 'pid': os.getpid(), 'start_ticks': _start_ticks(os.getpid())}
    changed = []
    previous = _read(local / 'publisher-result.json')
    try:
        result['source_signature'] = _source_signature(local)
        result.update(status='published', files=copy_snapshot(local, shared, changed=changed))
        returncode = 0
    except Exception as exc:
        result.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        returncode = 1
    try:
        result['finished_unix'] = time.time()
        result['changed_files'] = changed
        if changed or returncode or previous.get('status') != 'published':
            try:
                _write(shared / 'publisher-result.json', {
                    key: result[key] for key in ('nonce', 'status', 'changed_files', 'finished_unix')})
            except Exception as exc:
                result.update(status='failed', error=f'{type(exc).__name__}: {exc}')
                returncode = 1
        _write(local / 'publisher-result.json', result)
    finally:
        os.close(descriptor)
    return returncode


if __name__ == '__main__':
    # This file is an exec entry point as well as an importable module. Imports
    # may themselves wait on the published runtime; that wait is in the child.
    sys.path.insert(0, str(Path(__file__).parent.parent))
    raise SystemExit(main(sys.argv[1:]))

"""Best-effort CPU diagnostic copies, with one publisher per host/ledger.

Admission never waits for this process. The permanent local publication flock
is handed to the child across exec; even if the parent exits or the child
blocks in NFS, another worker cannot start a competing publisher. Only kernel
descriptor closure releases that slot. The child inherits no admission lock,
pipe to an action's captured output, or shared working directory.
"""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time
import uuid

FILES = ('cpu-sample.json', 'jobs.json', 'profiles.json', 'last-borrow.json')
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
    from prismabuild.materialize import _write_json_atomic
    _write_json_atomic(path, value)


def _start_ticks(pid):
    # The command name may contain spaces or parentheses.
    try:
        return int(Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19])
    except (OSError, ValueError, IndexError):
        return None


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
        owner = _read(local / 'publisher-owner.json')
        elapsed = now - owner.get('started_monotonic', -MIN_PUBLISH_INTERVAL_S)
        if 0 <= elapsed < MIN_PUBLISH_INTERVAL_S:
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


def copy_snapshot(local: Path, shared: Path):
    """Publish each local record independently; source timestamps stay intact."""
    copied = []
    for name in FILES:
        path = local / name
        try:
            value = json.loads(path.read_text())
        except FileNotFoundError:
            continue
        if not isinstance(value, dict):
            raise ValueError(f'invalid local CPU state: {name}')
        if name == 'cpu-sample.json':
            value['_snapshot'] = {'source': 'host-local', 'copied_unix': time.time()}
        _write(shared / name, value)
        copied.append(name)
    return copied


def main(argv):
    local, shared = Path(argv[0]), Path(argv[1])
    descriptor, nonce = int(argv[2]), argv[3]
    result = {'nonce': nonce, 'pid': os.getpid(), 'start_ticks': _start_ticks(os.getpid())}
    try:
        result.update(status='published', files=copy_snapshot(local, shared))
        returncode = 0
    except Exception as exc:
        result.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        returncode = 1
    try:
        result['finished_unix'] = time.time()
        _write(local / 'publisher-result.json', result)
    finally:
        os.close(descriptor)
    return returncode


if __name__ == '__main__':
    # This file is an exec entry point as well as an importable module. Imports
    # may themselves wait on the published runtime; that wait is in the child.
    sys.path.insert(0, str(Path(__file__).parent.parent))
    raise SystemExit(main(sys.argv[1:]))

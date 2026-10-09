"""Worker-local pbtest scratch custody and expiry (#1710).

The sealer embeds this stdlib-only owner in each default shard. The worker
supervisor uses the same owner. Root locks serialize startup, completion and
expiry. An attempt lock stays held until the shard interpreter exits.
"""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import pwd
import re
import shutil
import stat
import tempfile
import time

MAX_AGE_S = 24 * 3600
SWEEP_INTERVAL_S = 3600
SCHEMA = "prismabuild.pbtest_scratch.v1"
OWNER = ".pbtest-owner.json"
LOCK = ".pbtest-lock"
CGROUP_ROOT = Path("/sys/fs/cgroup/prismabuild.slice")
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_FILE_FLAGS = os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK


def registry_root():
    # The action may seal a different HOME. All payloads and the supervisor
    # must still use this uid's one host-local registry.
    return Path(pwd.getpwuid(os.geteuid()).pw_dir) / ".local/state/prismabuild/pbtest-scratch"


def _identity(info):
    return info.st_dev, info.st_ino


def _root_record(root, info):
    return {"root": str(root), "device": info.st_dev, "inode": info.st_ino}


def _read(fd, name):
    descriptor = os.open(name, os.O_RDONLY | _FILE_FLAGS, dir_fd=fd)
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_nlink != 1 or info.st_size > 16384):
            raise ValueError("unsafe scratch record")
        return json.loads(os.read(descriptor, 16385))
    finally:
        os.close(descriptor)


def _write(fd, name, record):
    temporary = name + "." + os.urandom(8).hex()
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _FILE_FLAGS,
                         0o600, dir_fd=fd)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(record, stream, allow_nan=False)
        os.replace(temporary, name, src_dir_fd=fd, dst_dir_fd=fd)
    finally:
        try:
            os.unlink(temporary, dir_fd=fd)
        except FileNotFoundError:
            pass


@contextmanager
def _guard(root, *, blocking=True):
    registry = registry_root()
    registry.mkdir(mode=0o700, parents=True, exist_ok=True)
    state = os.open(registry, _DIRECTORY_FLAGS)
    lock = -1
    try:
        info = os.fstat(state)
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError("scratch registry is not private")
        digest = hashlib.sha256(os.fsencode(str(root))).hexdigest()
        lock = os.open(digest + ".lock", os.O_RDWR | os.O_CREAT | _FILE_FLAGS,
                       0o600, dir_fd=state)
        info = os.fstat(lock)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_nlink != 1):
            raise ValueError("unsafe scratch root lock")
        fcntl.flock(lock, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        yield state, digest + ".json"
    finally:
        if lock >= 0:
            os.close(lock)
        os.close(state)


def _open_root(root):
    root = Path(os.path.abspath(root))
    if root == Path("/") or root.resolve(strict=True) != root:
        raise ValueError("scratch root must be a real absolute directory")
    fd = os.open(root, _DIRECTORY_FLAGS)
    info = os.fstat(fd)
    if info.st_uid != os.geteuid() and not (
            info.st_uid == 0 and info.st_mode & stat.S_ISVTX):
        os.close(fd)
        raise ValueError("scratch root has an unknown owner")
    return root, fd


class Attempt:
    def __init__(self, root, root_fd, directory, directory_fd, lock_fd, record):
        self.root = root
        self.root_fd = root_fd
        self.directory = directory
        self.directory_fd = directory_fd
        self.lock_fd = lock_fd
        self.record = record

    def finish(self, code):
        """Keep failures with a completion time; remove successful scratch."""
        with _guard(self.root):
            if _identity(os.stat(self.record["name"], dir_fd=self.root_fd,
                                 follow_symlinks=False)) != _identity(os.fstat(self.directory_fd)):
                raise ValueError("scratch attempt identity changed")
            if int(code) == 0:
                _no_mounts(self.directory)
                shutil.rmtree(self.record["name"], dir_fd=self.root_fd)
                self.close()
            else:
                self.record["kept_unix"] = time.time()
                _write(self.directory_fd, OWNER, self.record)
        return code

    def close(self):
        for name in ("lock_fd", "directory_fd", "root_fd"):
            descriptor = getattr(self, name)
            if descriptor >= 0:
                os.close(descriptor)
                setattr(self, name, -1)


def create(root, action_key, nonce):
    """Acquire custody before the attempt namespace becomes visible."""
    if not re.fullmatch("[0-9a-f]{64}", action_key):
        raise ValueError("invalid scratch action key")
    if nonce and not re.fullmatch("[0-9a-f]{32}", nonce):
        raise ValueError("invalid scratch attempt nonce")
    root, root_fd = _open_root(root)
    directory_fd = lock_fd = -1
    try:
        with _guard(root) as (state, entry):
            root_record = _root_record(root, os.fstat(root_fd))
            try:
                prior = _read(state, entry)
            except FileNotFoundError:
                prior = {}
            _write(state, entry, {**prior, **root_record})
            directory = Path(tempfile.mkdtemp(prefix="pb-", dir=root))
            directory_fd = os.open(directory.name, _DIRECTORY_FLAGS, dir_fd=root_fd)
            lock_fd = os.open(LOCK, os.O_RDWR | os.O_CREAT | os.O_EXCL | _FILE_FLAGS,
                              0o600, dir_fd=directory_fd)
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            info = os.fstat(directory_fd)
            record = {"schema": SCHEMA, **root_record, "name": directory.name,
                      "attempt_device": info.st_dev, "attempt_inode": info.st_ino,
                      "action_key": action_key, "nonce": nonce,
                      "scope": os.environ.get("PRISMABUILD_ACTION_SCOPE", ""),
                      "kept_unix": None}
            _write(directory_fd, OWNER, record)
            os.mkdir("pytest", dir_fd=directory_fd)
            return Attempt(root, root_fd, directory, directory_fd, lock_fd, record)
    except BaseException:
        for descriptor in (lock_fd, directory_fd, root_fd):
            if descriptor >= 0:
                os.close(descriptor)
        raise


def _ended(record):
    key, nonce = record.get("action_key"), record.get("nonce")
    if (not isinstance(key, str) or not re.fullmatch("[0-9a-f]{64}", key)
            or not isinstance(nonce, str) or not re.fullmatch("[0-9a-f]{32}", nonce)):
        return False
    expected = "prismabuild-job" + hashlib.sha256((key + nonce).encode()).hexdigest()[:32] + ".slice"
    if record.get("scope") != expected:
        return False
    # The launcher created this unique scope before it started the shard.
    # Its later absence proves departure only when the host cgroup root exists.
    CGROUP_ROOT.stat()
    try:
        events = (CGROUP_ROOT / expected / "cgroup.events").read_text()
    except FileNotFoundError:
        return not (CGROUP_ROOT / expected).exists()
    return dict(line.split() for line in events.splitlines()).get("populated") == "0"


def _allocated(fd, device):
    total = os.fstat(fd).st_blocks * 512
    for name in os.listdir(fd):
        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
        if info.st_dev != device:
            raise ValueError("scratch contains another filesystem")
        if stat.S_ISDIR(info.st_mode):
            child = os.open(name, _DIRECTORY_FLAGS, dir_fd=fd)
            try:
                if _identity(os.fstat(child)) != _identity(info):
                    raise ValueError("scratch tree changed")
                total += _allocated(child, device)
            finally:
                os.close(child)
        elif info.st_nlink == 1:
            total += info.st_blocks * 512
    return total


def _no_mounts(directory):
    # Same-device bind mounts also lie outside the attempt's owned bytes.
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        mount = re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), line.split()[4])
        if mount == str(directory) or mount.startswith(str(directory) + "/"):
            raise ValueError("scratch contains a mount")


def _attempt_names(root_fd):
    with os.scandir(root_fd) as entries:
        for entry in entries:
            if (entry.name.startswith(("pb-", "pbtest-"))
                    and entry.is_dir(follow_symlinks=False)):
                yield entry.name


def sweep_root(root, *, now=None, expected=None):
    """Expire proven ended attempts under the pinned root, or preserve them."""
    now = time.time() if now is None else now
    report = {"root": str(root), "removed": [], "skipped": [], "allocated_bytes": 0}
    root, root_fd = _open_root(root)
    try:
        with _guard(root, blocking=False) as (state, entry):
            identity = _root_record(root, os.fstat(root_fd))
            if expected is not None and any(expected.get(key) != value for key, value in identity.items()):
                raise ValueError("registered scratch root identity changed")
            for name in _attempt_names(root_fd):
                directory_fd = lock_fd = -1
                try:
                    directory_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=root_fd)
                    info = os.fstat(directory_fd)
                    if info.st_uid != os.geteuid():
                        raise ValueError("attempt has another owner")
                    record = _read(directory_fd, OWNER)
                    if (record.get("schema") != SCHEMA or record.get("name") != name
                            or any(record.get(key) != value for key, value in identity.items())
                            or (record.get("attempt_device"), record.get("attempt_inode")) != _identity(info)):
                        raise ValueError("unknown attempt ownership")
                    kept = record.get("kept_unix")
                    if (type(kept) not in (int, float) or not math.isfinite(kept)
                            or kept <= 0 or now - kept < MAX_AGE_S):
                        raise ValueError("active or unexpired attempt")
                    lock_fd = os.open(LOCK, os.O_RDWR | _FILE_FLAGS, dir_fd=directory_fd)
                    lock_info = os.fstat(lock_fd)
                    if (not stat.S_ISREG(lock_info.st_mode) or lock_info.st_uid != os.geteuid()
                            or lock_info.st_nlink != 1):
                        raise ValueError("unsafe attempt lock")
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    if not _ended(record):
                        raise ValueError("live or unknown attempt scope")
                    _no_mounts(root / name)
                    allocated = _allocated(directory_fd, info.st_dev)
                    if _identity(os.stat(name, dir_fd=root_fd, follow_symlinks=False)) != _identity(info):
                        raise ValueError("attempt identity changed")
                    shutil.rmtree(name, dir_fd=root_fd)
                    report["removed"].append({"name": name, "allocated_bytes": allocated})
                    report["allocated_bytes"] += allocated
                except (OSError, ValueError, TypeError, AttributeError) as exc:
                    report["skipped"].append({"name": name, "reason": str(exc)})
                finally:
                    for descriptor in (lock_fd, directory_fd):
                        if descriptor >= 0:
                            os.close(descriptor)
            _write(state, entry, {**identity, "last_sweep_unix": now})
            return report
    finally:
        os.close(root_fd)


def sweep_registered():
    """Sweep each registered root; the supervisor owns the hourly schedule."""
    registry = registry_root()
    if not registry.exists():
        return []
    reports = []
    state = os.open(registry, _DIRECTORY_FLAGS)
    try:
        for name in os.listdir(state):
            if not re.fullmatch("[0-9a-f]{64}\\.json", name):
                continue
            try:
                record = _read(state, name)
                root = record["root"]
                if hashlib.sha256(os.fsencode(root)).hexdigest() + ".json" != name:
                    raise ValueError("scratch registry path mismatch")
                reports.append(sweep_root(root, expected=record))
            except (OSError, ValueError, KeyError, TypeError) as exc:
                reports.append({"registry_entry": name, "error": str(exc)})
    finally:
        os.close(state)
    return reports


def main():
    """Report one operator pass through the same expiry path as the supervisor."""
    import argparse
    import socket

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", action="append", required=True)
    args = parser.parse_args()
    reports = [sweep_root(root) for root in args.root]
    print(json.dumps({"schema": "prismabuild.pbtest_scratch_expiry.v1",
                      "host": socket.gethostname(), "finished_unix": time.time(),
                      "max_age_s": MAX_AGE_S, "sweep_interval_s": SWEEP_INTERVAL_S,
                      "allocated_bytes": sum(report["allocated_bytes"] for report in reports),
                      "roots": reports}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

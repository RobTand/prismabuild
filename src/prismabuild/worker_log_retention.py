"""Bounded pruning of inactive supervisor worker logs."""
from __future__ import annotations

from collections import deque
from collections.abc import Collection
from pathlib import Path
import os
import re
import stat
import time

from . import role_log_identity

#: A worker loop's log is pruned once nothing has written to it for this
#: long, it is not the highest index, and no live loop holds it.
WORKER_LOG_RETENTION_S = 7 * 24 * 3600
#: The most worker logs one regular cycle inspects for pruning, so a
#: directory of tens of thousands never holds up a cycle or a spawn.
WORKER_LOG_PRUNES_PER_TICK = 256
#: The only names ``_spawn`` gives worker logs.
WORKER_LOG_NAME = re.compile(r"pb-worker-(0|[1-9][0-9]*)\.log")


def worker_log_census(log_dir: Path) -> list[int]:
    """Every worker log index in ``LOG_DIR``, lowest first.

    Listed once per supervisor start.  The next slot is one above the
    highest, so no running or earlier loop can already be appending to it;
    the rest are what ``_prune_worker_logs`` works through.
    """

    try:
        names = os.listdir(log_dir)
    except OSError:
        return []
    return sorted(int(match.group(1)) for name in names
                  if (match := WORKER_LOG_NAME.fullmatch(name)))


def _log_writers(pids: Collection[int], proc: Path) -> set[tuple[int, int]] | None:
    """The files these loops hold as stdout and stderr; ``None`` if unreadable.

    A loop that has exited (or closed the descriptor) holds nothing; any
    other failure to read one is unknown, and unknown keeps every log.
    """

    held = set()
    for pid in pids:
        for descriptor in (1, 2):
            try:
                info = os.stat(proc / str(pid) / "fd" / str(descriptor))
            except FileNotFoundError:
                continue
            except OSError:
                return None
            held.add((info.st_dev, info.st_ino))
    return held


def prune_worker_logs(backlog: deque[int], pids: Collection[int],
                      newest: int, *, log_dir: Path, proc: Path,
                      limit: int = WORKER_LOG_PRUNES_PER_TICK) -> int:
    """Remove worker logs no loop will append to again; return the count.

    A log goes when it is a single-link regular file this user owns, last
    written more than ``WORKER_LOG_RETENTION_S`` ago, below ``newest`` (the
    highest index, which a restarted supervisor counts from), and held as
    stdout or stderr by none of ``pids``, the cycle's live loops.  At most
    ``WORKER_LOG_PRUNES_PER_TICK`` are inspected; a kept log returns to the
    back of the backlog and anything else is forgotten untouched.  A loop
    whose descriptors cannot be read might hold any log, so that cycle
    prunes nothing.
    """

    now = time.time()
    held = None
    pruned = 0
    try:
        directory = os.open(log_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
                            | os.O_NOFOLLOW)
    except OSError:
        return 0
    try:
        role_log_identity.check_directory(directory, log_dir, os.getuid())
        for _ in range(min(limit, len(backlog))):
            index = backlog.popleft()
            name = f"pb-worker-{index}.log"
            try:
                info = os.stat(name, dir_fd=directory, follow_symlinks=False)
            except FileNotFoundError:
                continue
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or info.st_uid != os.getuid()):
                continue
            if index >= newest or now - info.st_mtime < WORKER_LOG_RETENTION_S:
                backlog.append(index)
                continue
            if held is None and (held := _log_writers(pids, proc)) is None:
                backlog.appendleft(index)
                break
            again = os.stat(name, dir_fd=directory, follow_symlinks=False)
            if ((info.st_dev, info.st_ino) in held
                    or (again.st_dev, again.st_ino) != (info.st_dev, info.st_ino)):
                backlog.append(index)
                continue
            os.unlink(name, dir_fd=directory)
            pruned += 1
    except OSError as exc:
        print(f"worker log pruning stopped: {str(exc)[:200]!r}", flush=True)
    finally:
        os.close(directory)
    return pruned

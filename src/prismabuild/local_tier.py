"""Host-local occupancy and dynamic capacity, sharing the ordinary tier ledger."""
from contextlib import contextmanager
import fcntl
import math
import os
from pathlib import Path

from . import pool, resident_sets

GIB = 1 << 30
KIND = "local_gib"


def local_resident_tier_id(host):
    return "local:" + resident_sets._resident_name(host, "host")


@contextmanager
def host_lock(root):
    """Local flock shared by copy transitions and eventual container injection."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    fd = os.open(root / ".resident.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def capacity_gib(spec, sample, *, held_bytes):
    available = sample.f_bavail * sample.f_frsize
    floor = math.ceil(sample.f_blocks * sample.f_frsize * spec["floor_fraction"])
    budget = available - floor - math.ceil(spec["docker_allowance_gib"] * GIB) + held_bytes
    return max(0, min(math.floor(spec["maximum_gib"]), budget // GIB))


def require_write_space(spec, sample, bytes_to_write):
    available = sample.f_bavail * sample.f_frsize - math.ceil(spec["docker_allowance_gib"] * GIB)
    floor = math.ceil(sample.f_blocks * sample.f_frsize * spec["floor_fraction"])
    if available - bytes_to_write < floor or available < math.ceil(1.5 * bytes_to_write) + 20 * GIB:
        raise ValueError("local capacity: write violates D1 floor or 1.5N + 20 GiB, including Docker allowance")


def occupied_bytes(root):
    total = 0
    for path in Path(root).iterdir():
        if path.name == ".resident.lock":
            continue
        if path.is_symlink() or not path.is_dir():
            raise ValueError(f"unsafe local tier entry: {path}")
        total += sum(resident_sets.directory_files(path).values())
    return total


def mint_local_tier_capacity(queue, host, spec):
    root = Path(spec["root"])
    root.mkdir(parents=True, exist_ok=True)
    with host_lock(root):
        held = occupied_bytes(root)
        sampled = os.statvfs(root)
        wanted = capacity_gib(spec, sampled, held_bytes=held)
        result = queue.mint_tier_capacity(local_resident_tier_id(host), {KIND: wanted})
        result.update({"root": str(root), "available_bytes": sampled.f_bavail * sampled.f_frsize,
                       "occupied_bytes": held, "wanted_gib": wanted, "host": host})
        resident_sets.write_record(queue.root / "resident-capacity" / (host + ".json"), result)
        return result


def reserve(queue, set_id, hosts, size):
    """Reserve all hosts before publication. Roll back only our new, empty holds."""
    acquired = []
    count = math.ceil(size / GIB)
    try:
        for host in hosts:
            ledger = queue.tier_ledger(local_resident_tier_id(host))
            with queue.tier_mint_lock(local_resident_tier_id(host)):
                prior = pool.held_names_visible(ledger, set_id)
                if prior:
                    if len(prior) != count or any(not name.startswith(KIND + "-") for name in prior):
                        raise ValueError(f"local capacity reservation disagrees for {host}")
                    continue
                if not ledger.acquire(set_id, {KIND: count}):
                    raise ValueError(f"local capacity unavailable on {host}")
                acquired.append(ledger)
    except BaseException:
        for ledger in reversed(acquired):
            ledger.release(set_id)
        raise
    return acquired

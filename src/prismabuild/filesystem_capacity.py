"""Filesystem room shared by pool admission, output reservations and pbtest."""
import os

INODE_FLOOR_PERCENT = 5


def local_disk_room(path, floor_percent, *, statvfs=None):
    """Available bytes above the requested floor and the five percent inode floor.

    Both available counters exclude any privileged reserve. A filesystem with
    zero total inodes reports no fixed inode limit, so it has no inode floor.
    Bind statvfs at call time so worker preflight reads the current filesystem.
    """
    sampled = (os.statvfs if statvfs is None else statvfs)(path)
    size = int(sampled.f_blocks) * int(sampled.f_frsize)
    free = int(sampled.f_bavail) * int(sampled.f_frsize)
    floor = -(-size * floor_percent // 100)
    total_inodes = int(sampled.f_files)
    free_inodes = int(sampled.f_favail)
    floor_inodes = -(-total_inodes * INODE_FLOOR_PERCENT // 100)
    return {"size_bytes": size, "free_bytes": free, "floor_bytes": floor,
            "room_bytes": free - floor, "size_inodes": total_inodes,
            "free_inodes": free_inodes, "floor_inodes": floor_inodes,
            "inode_refusal": (
                f"free inodes {free_inodes} below {INODE_FLOOR_PERCENT}% floor "
                f"{floor_inodes} on {path}" if free_inodes < floor_inodes else None)}


def queue_space_diagnostic(path):
    """Cheap quota-aware statvfs observation, for reporting rather than admission.

    f_bavail includes dataset/quota limits exposed by the filesystem. It is an
    observation, not a reservation or a forecast. Remote quota implementations
    that do not expose their limit through statvfs need operator monitoring.
    """
    try:
        room = local_disk_room(path, 0)
    except OSError as exc:
        return {"state": "unknown", "available_bytes": None, "available_inodes": None,
                "note": f"queue space unavailable: {type(exc).__name__}"}
    available = room["free_bytes"]
    state = "critical" if available < 4 * 1024 ** 3 else "warning" if available < 16 * 1024 ** 3 else "ok"
    if room["inode_refusal"]:
        state = "critical"
    return {"state": state, "available_bytes": available,
            "available_inodes": room["free_inodes"], "total_inodes": room["size_inodes"],
            "inode_pressure": bool(room["inode_refusal"]),
            "warning_bytes": 16 * 1024 ** 3, "critical_bytes": 4 * 1024 ** 3,
            "note": f"queue space {state}: {available} available bytes; "
                    f"{room['free_inodes']} available inodes"}

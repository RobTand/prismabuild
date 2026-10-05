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

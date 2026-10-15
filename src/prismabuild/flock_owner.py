"""Name a contended local flock's holder from its descriptor and procfs.

Call after a nonblocking flock acquisition is refused. None means the holder
is unknown; it does not mean the lock is free.
"""
import os


def _parse_device_inode(field: str) -> tuple[int, int, int] | None:
    """``(major, minor, inode)`` from a ``/proc/locks`` device field, or None.

    Parsed rather than formatted-and-compared.  The kernel prints the device as
    ``%02x:%02x``, so a key built by formatting has to reproduce that padding
    exactly, and getting it wrong yields a silent false negative that reads as
    good news: #264 matched nothing at all on dl380g10, whose ``/tmp`` is tmpfs
    with major 0, while looking correct on sparky, whose major 259 prints the
    same either way.  Reading the numbers back out cannot have that bug, for
    any padding the kernel might choose.
    """

    parts = field.split(':')
    if len(parts) != 3:
        return None
    try:
        return (int(parts[0], 16), int(parts[1], 16), int(parts[2]))
    except ValueError:
        return None


def holder_pid(descriptor: int) -> int | None:
    """The pid holding the flock on ``descriptor``, or ``None`` if unreadable.

    Best effort and local: ``/proc/locks`` is procfs, so asking costs no I/O
    on the shared mount -- which matters, because the reason this is being
    asked at all is that something on the shared mount is slow.  Every failure
    answers ``None`` rather than raising: a diagnostic must never be able to
    turn a refusal into a crash.

    The INODE identifies the lock and the device only breaks a tie, which is
    not the obvious way round.  Requiring both to match makes this blind on
    btrfs, where a subvolume carries its own anonymous device: ``fstat``
    returns the subvolume's, while ``/proc/locks`` reports the superblock's,
    and the two differ.  Measured on dl380g10 on 2026-09-06, one lock, one
    row::

        /home/rob/tmp  btrfs  fstat 00:1f:14164446
                              /proc/locks 00:1e:14164446

    Same inode, minor off by one, and #276 reported no holder at all for a
    lock it was itself holding.  Matching on the inode recovers it, and costs
    nothing: this is only ever asked after a non-blocking acquisition failed,
    so a holder exists and its row is there to be found.  A second row on the
    same inode number means some other filesystem also has one -- that is the
    collision the device exists to resolve, and where it cannot resolve it
    this answers ``None`` rather than naming a pid from another mount.
    """

    try:
        info = os.fstat(descriptor)
    except OSError:
        return None
    wanted = (os.major(info.st_dev), os.minor(info.st_dev), info.st_ino)
    candidates = []
    try:
        with open('/proc/locks') as handle:
            for line in handle:
                fields = line.split()
                # "<n>: FLOCK ADVISORY WRITE <pid> <maj>:<min>:<ino> 0 EOF",
                # where the device numbers are hex and the inode is decimal.
                # A waiter's line begins "<n>: -> FLOCK", which fails the test
                # below and is skipped, so only the holder is ever named.
                if len(fields) < 6 or fields[1] != 'FLOCK':
                    continue
                found = _parse_device_inode(fields[5])
                if found is None or found[2] != info.st_ino:
                    continue
                candidates.append((found, int(fields[4])))
    except (OSError, ValueError):
        return None
    if not candidates:
        return None
    if len(candidates) > 1:
        # More than one filesystem has a lock on this inode NUMBER, so the
        # number alone no longer names a lock and the device has to break the
        # tie.  If it cannot -- neither row carries this descriptor's device,
        # or both do -- the honest answer is that we do not know which.
        exact = [row for row in candidates if row[0] == wanted]
        if len(exact) != 1:
            return None
        candidates = exact
    held_by = candidates[0][1]
    # -1 is the kernel's "no owning process" (an OFD lock); it is not a pid
    # and must not be reported as one.
    return held_by if held_by > 0 else None

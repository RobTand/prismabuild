"""Best-effort names for cooperating holders of dedicated flock files.

None means unknown, never free. Exclusion always comes from flock itself.
"""
import json
import os
import stat


_SCHEMA = "prismabuild.flock_holder.v1"
_MAX_RECORD_BYTES = 1024


def _process_starttime(pid: int) -> int:
    with open(f"/proc/{pid}/stat", "rb") as stream:
        payload = stream.read(4097)
    if len(payload) > 4096:
        raise ValueError("oversized process identity")
    # comm can contain spaces and closing parentheses. Fields after its final
    # parenthesis start at field 3; starttime is field 22.
    fields = payload.rpartition(b")")[2].split()
    if fields[0] in (b"Z", b"X", b"x"):
        raise ValueError("holder is no longer alive")
    return int(fields[19])


def _boot_id() -> str:
    with open("/proc/sys/kernel/random/boot_id", "r", encoding="ascii") as stream:
        value = stream.read(65).strip()
    if len(value) != 36:
        raise ValueError("unreadable boot identity")
    return value


def record_holder(descriptor: int) -> None:
    """Record this process after taking an exclusive lock on a dedicated file.

    Only callers owning a lock-only file may use this writer. Never pass a
    descriptor whose contents are application data. Publication is diagnostic;
    failures must not break acquisition. No extra file or durable write is needed.
    """
    try:
        os.ftruncate(descriptor, 0)
        info = os.fstat(descriptor)
        pid = os.getpid()
        record = {
            "schema": _SCHEMA,
            "pid": pid,
            "starttime": _process_starttime(pid),
            "boot_id": _boot_id(),
            "pid_namespace": os.readlink("/proc/self/ns/pid"),
            "device": info.st_dev,
            "inode": info.st_ino,
            "fd": descriptor,
        }
        payload = json.dumps(record, separators=(",", ":")).encode("ascii")
        if len(payload) > _MAX_RECORD_BYTES:
            return
        if os.pwrite(descriptor, payload, 0) != len(payload):
            os.ftruncate(descriptor, 0)
    except Exception:
        # This metadata grants no authority and must not turn a lock into an
        # availability dependency on procfs or a writable diagnostic record.
        return


def holder_pid(descriptor: int) -> int | None:
    """Name a recorded live holder with this file open, or answer unknown.

    Read only this file and one recorded process. Verify its boot, PID namespace,
    process start time and exact descriptor's device/inode, then recheck the
    process and record. This is a racing diagnostic, never evidence of freedom
    or authority to signal a process. Unrecorded locks remain unknown.
    """
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            return None
        payload = os.pread(descriptor, _MAX_RECORD_BYTES + 1, 0)
        if len(payload) > _MAX_RECORD_BYTES:
            return None
        record = json.loads(payload)
        if not isinstance(record, dict) or record.get("schema") != _SCHEMA:
            return None
        for name in ("pid", "starttime", "device", "inode", "fd"):
            minimum = 0 if name in ("device", "fd") else 1
            if type(record[name]) is not int or record[name] < minimum:
                return None
        pid = record["pid"]
        wanted = (info.st_dev, info.st_ino)
        if (record["device"], record["inode"]) != wanted:
            return None
        if record["boot_id"] != _boot_id():
            return None
        namespace = os.readlink("/proc/self/ns/pid")
        if (record["pid_namespace"] != namespace
                or os.readlink(f"/proc/{pid}/ns/pid") != namespace):
            return None
        if _process_starttime(pid) != record["starttime"]:
            return None
        target = f"/proc/{pid}/fd/{record['fd']}"
        opened = os.stat(target)
        if (opened.st_dev, opened.st_ino) != wanted:
            return None
        if _process_starttime(pid) != record["starttime"]:
            return None
        if os.pread(descriptor, _MAX_RECORD_BYTES + 1, 0) != payload:
            return None
        opened = os.stat(target)
        if (opened.st_dev, opened.st_ino) != wanted:
            return None
        return pid
    except Exception:
        return None

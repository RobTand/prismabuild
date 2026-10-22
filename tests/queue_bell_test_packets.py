"""Select the mutation a deterministic bell installation barrier controls."""
import os

from prismabuild.queue_bell_events import EVENT_HEADER, IN_CREATE, IN_MOVED_TO


def contains_installation(watch, data, parent, name):
    offset = 0
    while offset < len(data):
        wd, mask, _, length = EVENT_HEADER.unpack_from(data, offset)
        start = offset + EVENT_HEADER.size
        observed = os.fsdecode(data[start:start + length].split(b"\0", 1)[0])
        offset = start + length
        if (watch.directories.get(wd) == parent and observed == name
                and mask & (IN_CREATE | IN_MOVED_TO)):
            return True
    return False

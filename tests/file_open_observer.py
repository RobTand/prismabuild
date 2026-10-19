"""Observe real opens of one inode without replacing its contents or reader."""
import builtins
import io
import os


def observe_file_opens(monkeypatch, path):
    target = path.stat()
    identity = target.st_dev, target.st_ino
    observed = []
    native_os_open = os.open
    native_builtin_open = builtins.open
    native_io_open = io.open

    def record(descriptor):
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) == identity:
            observed.append(descriptor)

    def os_open(*args, **kwargs):
        descriptor = native_os_open(*args, **kwargs)
        record(descriptor)
        return descriptor

    def builtin_open(*args, **kwargs):
        handle = native_builtin_open(*args, **kwargs)
        record(handle.fileno())
        return handle

    def io_open(*args, **kwargs):
        handle = native_io_open(*args, **kwargs)
        record(handle.fileno())
        return handle

    monkeypatch.setattr(os, "open", os_open)
    monkeypatch.setattr(builtins, "open", builtin_open)
    monkeypatch.setattr(io, "open", io_open)
    return observed

"""Declared test evidence over real files, supplied through the public protocol.

This controls whether evidence is offered. Native Linux proof semantics are
exercised separately by the retained mount/fd/namespace consumer controls.
"""
from pathlib import Path
import os
import stat as statmod
from prismabuild import kept_reads


class ObservedTrust(kept_reads.LinuxFilesystemTrust):
    def __init__(self):
        self.available = True
        self.directory_available = True
        self.events = []
        self.directory_versions = {}
        self.phase_error = None
        self.reject_commit = False
        self.fence = 2**63 - 1

    def directory_current(self, path):
        self.events.append(('current', os.fspath(path)))
        if self.phase_error == 'hit':
            self.phase_error = None
            raise RuntimeError('injected hit proof interruption')
        if not self.available or not self.directory_available or self.reject_commit:
            return None
        key = os.fspath(path)
        if key in self.directory_versions:
            return self.directory_versions[key]
        try:
            info = os.lstat(path)
        except OSError:
            return None
        if not statmod.S_ISDIR(info.st_mode):
            return None
        return info.st_dev, info.st_ino, info.st_mtime_ns, info.st_ctime_ns

    def directory_before(self, path):
        self.events.append(('before', os.fspath(path)))
        if self.phase_error == 'acquire':
            self.phase_error = None
            raise RuntimeError('injected acquisition proof interruption')
        proof = self.directory_current(path)
        fence = self.file_fence()
        if proof is None or fence is None or max(proof[2:]) >= fence:
            return None
        return proof

    def file_fence(self):
        self.events.append(('fence', None))
        return (self.fence() if callable(self.fence) else self.fence) if self.available else None

    def file_version(self, info, fence, *, path=None, descriptor=None,
                     follow_symlinks=True):
        self.events.append(('file', path if path is not None else descriptor))
        if not self.available or fence is None or info.st_ctime_ns >= fence:
            return None
        return kept_reads.metadata_version(info)

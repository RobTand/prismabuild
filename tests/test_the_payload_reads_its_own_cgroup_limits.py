"""A bare-host payload can read its own cgroup limits (#916).

The broker creates each job's ``payload`` leaf with ``mkdir`` under its unit's
``UMask=0077``, which left it ``drwx------ root root``.  The payload runs as
the execution user, so it could not open its own ``memory.max``; PrismaQuant's
``CaptureMemoryGuard`` walks the process's cgroup and its ancestors reading
``memory.max`` and died with ``PermissionError`` on the leaf.  A containerized
action's ``docker-<id>.scope`` is ``drwxr-xr-x``, and the job slice systemd
creates above the leaf already is.

These tests run the real ``SystemdBackend.create`` against a fake cgroup tree
under the broker's umask.  A test that is not root cannot become another user,
so "the payload can read it" is checked the way the kernel checks it for a
uid that neither owns the files nor is in their group: the "other" execute
bit on every directory from the slice down to the leaf, and the "other" read
bit on the control file.  The kernel creates control files such as
``memory.max`` as ``-rw-r--r--`` root; the fake does the same.
"""
from __future__ import annotations

import os
from pathlib import Path
import stat

import pytest

from test_resource_broker import module

SCOPE = 'prismabuild-job' + 'b' * 32 + '.slice'
BUDGET = 64 * 1024 ** 2
#: The broker unit's umask (``install_resource_broker.sh``).
BROKER_UMASK = 0o077


def _kernel_file(path: Path, text: str) -> None:
    path.write_text(text)
    path.chmod(0o644)


def _backend(root: Path, monkeypatch):
    """A ``SystemdBackend`` whose ``systemctl`` builds the tree systemd would."""

    broker = module()
    backend = broker.SystemdBackend(root=root)

    def command(*argv):
        if argv[0] == 'start':
            group = root / 'prismabuild.slice' / argv[1]
            for directory in (root / 'prismabuild.slice', group):
                directory.mkdir(exist_ok=True)
                directory.chmod(0o755)          # systemd's slices are 0755
            if not (root / 'prismabuild.slice' / 'memory.max').exists():
                _kernel_file(root / 'prismabuild.slice' / 'memory.max', 'max\n')
            for name, text in {
                    'memory.max': 'max\n', 'memory.high': 'max\n',
                    'memory.oom.group': '0\n', 'cgroup.controllers': 'cpu memory\n',
                    'cgroup.subtree_control': '', 'cgroup.events': 'populated 0\n',
                    'memory.events': 'oom_kill 0\n', 'memory.events.local': 'oom 0\n',
            }.items():
                _kernel_file(group / name, text)
        elif argv[0] == 'set-property':
            group = root / 'prismabuild.slice' / argv[2]
            for item in argv[3:]:
                if item.startswith('MemoryMax='):
                    (group / 'memory.max').write_text(item.split('=', 1)[1] + '\n')

    monkeypatch.setattr(backend, 'command', command)
    return broker, backend


@pytest.fixture
def broker_umask():
    previous = os.umask(BROKER_UMASK)
    try:
        yield
    finally:
        os.umask(previous)


def _other_may_traverse(directory: Path) -> bool:
    return bool(stat.S_IMODE(directory.stat().st_mode) & stat.S_IXOTH)


def _other_may_read(path: Path) -> bool:
    return bool(stat.S_IMODE(path.stat().st_mode) & stat.S_IROTH)


def test_a_non_root_payload_reads_its_own_memory_max(tmp_path, monkeypatch, broker_umask):
    _, backend = _backend(tmp_path, monkeypatch)
    record = backend.create(SCOPE, BUDGET)
    leaf = Path(record['leaf_path'])
    # The kernel populates a new cgroup directory with its control files.
    _kernel_file(leaf / 'memory.max', 'max\n')

    chain = [tmp_path / 'prismabuild.slice', Path(record['cgroup_path']), leaf]
    assert [_other_may_traverse(d) for d in chain] == [True, True, True]
    for directory in chain:                     # the guard reads each level
        assert _other_may_read(directory / 'memory.max'), directory
    assert (Path(record['cgroup_path']) / 'memory.max').read_text() == f'{BUDGET}\n'


def test_the_leaf_is_readable_and_never_writable_by_anyone_but_root(
        tmp_path, monkeypatch, broker_umask):
    _, backend = _backend(tmp_path, monkeypatch)
    leaf = Path(backend.create(SCOPE, BUDGET)['leaf_path'])
    assert stat.S_IMODE(leaf.stat().st_mode) == 0o755


def test_an_existing_private_leaf_is_opened_too(tmp_path, monkeypatch, broker_umask):
    """An empty scope the broker re-creates keeps its leaf; its mode is set, not assumed."""

    _, backend = _backend(tmp_path, monkeypatch)
    group = tmp_path / 'prismabuild.slice' / SCOPE
    group.mkdir(parents=True)
    _kernel_file(group / 'cgroup.events', 'populated 0\n')
    (group / 'payload').mkdir(mode=0o700)
    leaf = Path(backend.create(SCOPE, BUDGET)['leaf_path'])
    assert stat.S_IMODE(leaf.stat().st_mode) == 0o755


def test_a_leaf_whose_mode_does_not_take_is_refused(tmp_path, monkeypatch, broker_umask):
    """Verified like the memory envelope: no process enters an unreadable leaf."""

    broker, backend = _backend(tmp_path, monkeypatch)
    monkeypatch.setattr(broker.os, 'chmod', lambda *args, **kwargs: None)
    with pytest.raises(ValueError, match='payload cgroup'):
        backend.create(SCOPE, BUDGET)

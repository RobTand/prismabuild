"""Exercise the opt-in's symlinked ancestor, with root metadata simulated."""
import os
from pathlib import Path

from prismabuild import gpu_capacity


def test_parent_symlink_into_caller_controlled_ancestry_is_not_trusted(tmp_path, monkeypatch):
    etc = tmp_path / 'host' / 'etc'
    etc.mkdir(parents=True)
    caller = tmp_path / 'caller'
    target = caller / 'root-owned-policy'
    target.mkdir(parents=True)
    link = etc / 'prismabuild'
    link.symlink_to(target, target_is_directory=True)
    flag = link / 'gpu-container-runtime.enabled'
    flag.write_bytes(b'enabled\n')
    flag.chmod(0o644)
    monkeypatch.setattr(gpu_capacity, 'RUNTIME_ENABLE', flag)
    # Root owns all lexical ancestors and the final policy directory/file.
    # The symlink target's caller-owned ancestor is deliberately not trusted.
    root_owned = set(flag.parents) | {target}
    original_stat = Path.stat
    def stat(path, *args, **kwargs):
        actual = original_stat(path, *args, **kwargs)
        if path not in root_owned:
            return actual
        fields = list(actual)
        fields[0] &= ~0o022
        fields[4] = 0
        return os.stat_result(fields)
    monkeypatch.setattr(Path, 'stat', stat)
    original_fstat = os.fstat
    def fstat(fd):
        fields = list(original_fstat(fd))
        fields[4] = 0
        fields[0] &= ~0o022
        return os.stat_result(fields)
    monkeypatch.setattr(os, 'fstat', fstat)
    assert gpu_capacity.runtime_fence_enabled() is False


def test_optin_file_open_is_relative_to_verified_parent_fd(tmp_path, monkeypatch):
    import stat
    flag = tmp_path / 'policy' / 'gpu-container-runtime.enabled'
    flag.parent.mkdir()
    flag.write_bytes(b'enabled\n')
    monkeypatch.setattr(gpu_capacity, 'RUNTIME_ENABLE', flag)
    original_fstat = os.fstat
    def root_metadata(fd):
        fields = list(original_fstat(fd))
        fields[4] = 0
        fields[0] &= ~0o022
        return os.stat_result(fields)
    monkeypatch.setattr(os, 'fstat', root_metadata)
    original_open = os.open
    opened = []
    def anchored_open(name, flags, *args, **kwargs):
        fd = original_open(name, flags, *args, **kwargs)
        opened.append((name, flags, kwargs.get('dir_fd'), fd, original_fstat(fd).st_mode))
        return fd
    monkeypatch.setattr(os, 'open', anchored_open)
    assert gpu_capacity.runtime_fence_enabled()
    assert opened[0][0] == '/'
    assert opened[-1][0] == flag.name
    assert opened[-1][2] == opened[-2][3]
    assert all(flags & os.O_NOFOLLOW for _, flags, _, _, _ in opened)
    assert all(flags & os.O_DIRECTORY for _, flags, _, _, mode in opened if stat.S_ISDIR(mode))

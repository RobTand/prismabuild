"""Owned Docker containers inherit their actual PB scope's memory boundary."""
from test_docker_shim_global_options import _shim


def _environment(tmp_path):
    root = tmp_path / 'cgroups'
    group = root / 'user.slice/prismabuild-job0123456789abcdef0123456789abcdef.slice'
    group.mkdir(parents=True)
    (group / 'memory.max').write_text(str(64 * 1024**2))
    return {'PRISMABUILD_CGROUP_ROOT': str(root)}, '0::/' + str(group.relative_to(root)) + '\n'


def test_memory_and_scope_label_reach_daemon(tmp_path):
    environment, cgroup = _environment(tmp_path)
    result, _, forwarded = _shim(tmp_path, ['run', 'image'], cgroup=cgroup, docker_env=environment)
    assert result.returncode == 0, result.stderr
    assert '--memory' in forwarded
    assert forwarded[forwarded.index('--memory')+1] == str(64 * 1024**2)
    assert forwarded[forwarded.index('--memory-swap')+1] == str(64 * 1024**2)
    assert any(value.startswith('prismabuild.scope=') for value in forwarded)


def test_explicit_memory_cannot_widen_scope(tmp_path):
    environment, cgroup = _environment(tmp_path)
    result, _, forwarded = _shim(tmp_path, ['run', '--memory=8g', '--memory-swap=-1', 'image'], cgroup=cgroup, docker_env=environment)
    assert result.returncode == 0, result.stderr
    assert forwarded[forwarded.index('--memory')+1] == str(64 * 1024**2)
    assert '--memory=8g' not in forwarded
    assert '--memory-swap=-1' not in forwarded


def test_caller_cannot_forge_scope_label(tmp_path):
    result, _, _ = _shim(tmp_path, ['run', '--label', 'prismabuild.scope=other.scope', 'image'])
    assert result.returncode == 125


def test_container_parent_is_the_aggregate_job_slice(tmp_path):
    environment, cgroup = _environment(tmp_path)
    result, _, forwarded = _shim(tmp_path, ['run', 'image'], cgroup=cgroup, docker_env=environment)
    assert result.returncode == 0, result.stderr
    assert forwarded[forwarded.index('--cgroup-parent')+1] == 'prismabuild-job0123456789abcdef0123456789abcdef.slice'


def test_explicit_parent_cannot_escape_aggregate(tmp_path):
    environment, cgroup = _environment(tmp_path)
    result, _, _ = _shim(tmp_path, ['run', '--cgroup-parent=elsewhere.slice', 'image'], cgroup=cgroup, docker_env=environment)
    assert result.returncode == 125


def test_stricter_memory_preserved_and_image_argv_opaque(tmp_path):
    environment, cgroup = _environment(tmp_path)
    tail = ['image', '--memory=8g', '--cgroup-parent=other.slice']
    result, _, forwarded = _shim(tmp_path, ['run', '-itm32m', *tail], cgroup=cgroup, docker_env=environment)
    assert result.returncode == 0, result.stderr
    assert forwarded[forwarded.index('--memory')+1] == str(32 * 1024**2)
    assert '-it' in forwarded
    assert forwarded[-len(tail):] == tail

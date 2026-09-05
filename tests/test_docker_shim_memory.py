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


def test_restart_policy_cannot_resurrect_oom_killed_job(tmp_path):
    environment, cgroup = _environment(tmp_path)
    result, _, _ = _shim(tmp_path, ['run', '--restart=always', 'image'], cgroup=cgroup, docker_env=environment)
    assert result.returncode == 125


import pytest


@pytest.mark.parametrize('command', [
    ['build', '.'], ['buildx', 'build', '.'], ['buildx', 'inspect', '--bootstrap'],
    ['exec', 'another-container', 'sh'], ['container', 'restart', 'another-container'],
    ['compose', '-f', 'compose.yml', 'build'], ['service', 'create', 'image'],
    ['stack', 'deploy', 'stack'], ['update', '--restart=always', 'container'],
])
def test_daemon_work_cannot_escape_accounted_creation(tmp_path, command):
    environment, cgroup = _environment(tmp_path)
    result, _, _ = _shim(tmp_path, command, cgroup=cgroup, docker_env=environment)
    assert result.returncode == 125
    assert 'docker run/create' in result.stderr


@pytest.mark.parametrize('command', [['buildx', 'inspect'], ['buildx', 'ls'], ['images'], ['inspect', 'container']])
def test_scope_preserves_read_only_docker_commands(tmp_path, command):
    environment, cgroup = _environment(tmp_path)
    result, _, forwarded = _shim(tmp_path, command, cgroup=cgroup, docker_env=environment)
    assert result.returncode == 0, result.stderr
    assert forwarded == command


@pytest.mark.parametrize('value', ['-1000', '-01000'])
def test_container_cannot_opt_out_of_group_oom(tmp_path, value):
    environment, cgroup = _environment(tmp_path)
    result, _, _ = _shim(tmp_path, ['run', '--oom-score-adj=' + value, 'image'], cgroup=cgroup, docker_env=environment)
    assert result.returncode == 125


def test_container_intent_brackets_the_daemon_request(tmp_path):
    import json
    environment, cgroup = _environment(tmp_path)
    result, _, forwarded = _shim(tmp_path, ['run', 'image'], cgroup=cgroup, docker_env=environment)
    assert result.returncode == 0, result.stderr
    scope = forwarded[forwarded.index('--cgroup-parent')+1]
    assert json.loads((tmp_path / 'broker.json').read_text()) == [
        {'op': 'container_begin', 'scope_id': scope},
        {'op': 'container_end', 'scope_id': scope, 'ticket': 'b'*64},
    ]


def test_broker_refusal_prevents_container_request(tmp_path):
    environment, cgroup = _environment(tmp_path)
    environment['PRISMABUILD_DOCKER_TEST_BROKER_FAIL'] = '1'
    result, _, forwarded = _shim(tmp_path, ['run', 'image'], cgroup=cgroup, docker_env=environment)
    assert result.returncode == 125
    assert forwarded is None
    assert 'resource broker' in result.stderr


def test_killed_shim_preserves_pending_creation_intent(tmp_path):
    import json
    environment, cgroup = _environment(tmp_path)
    environment['PRISMABUILD_DOCKER_TEST_KILL_SHIM'] = '1'
    result, _, forwarded = _shim(tmp_path, ['run', 'image'], cgroup=cgroup, docker_env=environment)
    assert result.returncode == -9
    assert forwarded is not None
    calls = json.loads((tmp_path / 'broker.json').read_text())
    assert len(calls) == 1 and calls[0]['op'] == 'container_begin'


@pytest.mark.parametrize('status', ['1', '125'])
def test_nonzero_cli_result_retains_ambiguous_intent(tmp_path, status):
    import json
    environment, cgroup = _environment(tmp_path)
    environment['PRISMABUILD_DOCKER_TEST_RETURN'] = status
    result, _, forwarded = _shim(tmp_path, ['run', 'image'], cgroup=cgroup, docker_env=environment)
    assert result.returncode == int(status), result.stderr
    assert forwarded is not None
    assert [call['op'] for call in json.loads((tmp_path / 'broker.json').read_text())] == ['container_begin']

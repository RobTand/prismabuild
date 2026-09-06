"""The executed shell and native threads honor the admitted environment."""
import json
import subprocess

import pytest
from test_pbrun_detach import _checkout, _queue, _run_pbrun


@pytest.mark.parametrize('options,expected', [
    ((), '1'), (('--cpus', '3'), '3'),
    (('--demand', 'cpu=2'), '2'),
    (('--cpus', '3', '--env', 'OMP_NUM_THREADS=1'), '1'),
])
def test_native_threads_follow_reserved_cpus(tmp_path, monkeypatch, capsys, options, expected):
    work = _checkout(tmp_path)
    _queue(tmp_path)
    assert _run_pbrun(tmp_path, monkeypatch, work, '--detach', *options) == 0
    key = json.loads(capsys.readouterr().out)['action_key']
    action = json.loads((tmp_path / 'cas' / 'requests' / key[:2] / f'{key}.json').read_text())
    variables = action['environment']['variables']
    assert variables['OMP_NUM_THREADS'] == expected
    for name in ('MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS'):
        assert variables[name] == str(action['params']['demand']['cpu'])


def test_wrapper_does_not_reopen_login_profiles(tmp_path, monkeypatch, capsys):
    work = _checkout(tmp_path)
    _queue(tmp_path)
    command = ('/bin/bash', '-c', 'shopt -q login_shell; printf "%s" "$PATH"')
    assert _run_pbrun(tmp_path, monkeypatch, work, '--detach', command=command) == 0
    key = json.loads(capsys.readouterr().out)['action_key']
    action = json.loads((tmp_path / 'cas' / 'requests' / key[:2] / f'{key}.json').read_text())
    argv = action['task']['argv']
    # Exercise the shell options with an observable login-shell property;
    # the pipeline payload is immaterial to whether profiles were opened.
    result = subprocess.run([*argv[:-1], 'shopt -q login_shell'],
                            env=action['environment']['variables'], cwd=work)
    assert result.returncode == 1

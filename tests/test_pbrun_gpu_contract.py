"""GPU sharing and VRAM intent are sealed, validated submission contracts."""
from submission_support import patch_store
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools' / 'fleet'))
import pbrun
from prismabuild import submission as submit_core


def params(argv, tmp_path, monkeypatch):
    subprocess.run(['git','init','-q',str(tmp_path)], check=True)
    subprocess.run(['git','-C',str(tmp_path),'-c','user.name=PB test',
                    '-c','user.email=test@example.invalid','commit','--allow-empty','-qm','fixture'],check=True)
    captured=[]
    class Stop(Exception): pass
    def seal(body):
        captured.append(body['params'])
        raise Stop()
    monkeypatch.setattr(submit_core.pb,'seal_action',seal)
    patch_store(monkeypatch, client=pbrun, root=tmp_path/'fleet')
    monkeypatch.setattr(submit_core,'git_repository_root',lambda cwd, **_context: tmp_path)
    monkeypatch.setattr(submit_core,'snapshot_checkout',lambda *a,**k:{'input':{'id':'test'}})
    monkeypatch.setattr(sys,'argv',['pbrun.py','--cwd',str(tmp_path),*argv,'--','true'])
    try: pbrun.main()
    except Stop: pass
    return captured[0]


def test_shared_and_exclusive_intent_are_explicit(tmp_path,monkeypatch):
    shared=params(['--gpu'],tmp_path,monkeypatch)
    exclusive=params(['--gpu','--exclusive','--gpu-capacity','1'],tmp_path,monkeypatch)
    assert shared['gpu_exclusive'] is False
    assert exclusive['gpu_exclusive'] is True


def test_discrete_gpu_budget_does_not_rewrite_host_ram(tmp_path,monkeypatch):
    value=params(['--gpu','--gpu-memory-gb','.5','--demand','mem_gb=32'],tmp_path,monkeypatch)
    assert value['gpu_memory_gb'] == .5
    assert value['demand']['mem_gb'] == 32


@pytest.mark.parametrize('selector', [
    ['--tag', 'gb10'], ['--measurement', '--host-class', 'gb10'],
])
def test_explicit_gb10_subset_is_checked_before_sealing(tmp_path, monkeypatch, capsys, selector):
    from test_pbrun_target_evidence_1598 import _packet

    # The test runtime is box-local; placement validation here concerns the
    # declared class/tag, not availability of a published worker launcher.
    monkeypatch.setattr(submit_core, 'require_reachable_runtime', lambda *args, **kw: None)
    monkeypatch.setattr(submit_core.pb, '_collect_worker_evidence',
                        lambda **kw: _packet(host='fixture-worker'))
    # Valid subsets and effective defaults must still seal before the refusal.
    for index, (host, cap, exclusive, effective) in enumerate([
        (104, 104, False, 104), (104, 100, False, 104),
        (80, 64, False, 80), (104, None, False, 104),
        (None, 1, False, 4), (4, 16, True, 16),
    ]):
        argv = (['--gpu', *selector] if host is not None else
                ['--demand', 'gpu=1', *selector])
        if host is not None:
            argv += ['--demand', f'mem_gb={host}']
        if cap is not None:
            argv += ['--gpu-memory-gb', str(cap)]
        if exclusive:
            argv += ['--exclusive', '--gpu-capacity', '1']
        value = params(argv, tmp_path / f'valid-{index}', monkeypatch)
        assert value['demand']['mem_gb'] == effective
        assert value.get('gpu_memory_gb') == cap

    discrete = params(['--gpu', '--tag', 'discrete', '--demand', 'mem_gb=64',
                       '--gpu-memory-gb', '104'], tmp_path / 'discrete', monkeypatch)
    assert discrete['gpu_memory_gb'] == 104
    # params intercepts seal_action: reaching it would return, not refuse.
    capsys.readouterr()
    with pytest.raises(SystemExit) as error:
        params(['--gpu', *selector, '--demand', 'mem_gb=104',
                '--gpu-memory-gb', '105'], tmp_path / 'invalid', monkeypatch)
    assert error.value.code == 2
    diagnostic = capsys.readouterr().err.lower()
    assert all(word in diagnostic for word in ('105', '104', 'subset', 'mem_gb'))


@pytest.mark.parametrize('argv', [['--gpu','--gpu-memory-gb','0'],
                                 ['--gpu','--gpu-memory-gb','nan'],
                                 ['--gpu','--gpu-memory-gb','1e300'],
                                 ['--gpu','--gpu-memory-gb','1e-12'],
                                 ['--gpu','--gpu-memory-gb','8589934592'],
                                 ['--gpu-memory-gb','1'],
                                 ['--gpu','--gpu-memory-gb','1','--transport','slurm']])
def test_invalid_or_unsupported_gpu_budget_is_rejected(tmp_path,monkeypatch,argv):
    with pytest.raises(SystemExit) as error:
        params(argv,tmp_path,monkeypatch)
    assert error.value.code == 2

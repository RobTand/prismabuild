"""GPU sharing and VRAM intent are sealed, validated submission contracts."""
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools' / 'fleet'))
import pbrun


def params(argv, tmp_path, monkeypatch):
    subprocess.run(['git','init','-q',str(tmp_path)], check=True)
    subprocess.run(['git','-C',str(tmp_path),'-c','user.name=PB test',
                    '-c','user.email=test@example.invalid','commit','--allow-empty','-qm','fixture'],check=True)
    captured=[]
    class Stop(Exception): pass
    def seal(body):
        captured.append(body['params'])
        raise Stop()
    monkeypatch.setattr(pbrun.pb,'seal_action',seal)
    monkeypatch.setattr(pbrun,'SH',tmp_path/'fleet')
    monkeypatch.setattr(pbrun,'git_repository_root',lambda cwd:tmp_path)
    monkeypatch.setattr(pbrun,'build_git_checkout_snapshot',lambda *a,**k:{'input':{'id':'test'}})
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


@pytest.mark.parametrize('argv', [['--gpu','--gpu-memory-gb','0'],
                                 ['--gpu','--gpu-memory-gb','nan'],
                                 ['--gpu-memory-gb','1'],
                                 ['--gpu','--gpu-memory-gb','1','--transport','slurm']])
def test_invalid_or_unsupported_gpu_budget_is_rejected(tmp_path,monkeypatch,argv):
    with pytest.raises(SystemExit) as error:
        params(argv,tmp_path,monkeypatch)
    assert error.value.code == 2

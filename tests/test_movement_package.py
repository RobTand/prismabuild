"""A src-only production owner retires real staged bytes in either layout."""
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from test_produced_output_restage import (
    REPO, _World, _descriptors, _prewrite,
    _isolated_synthetic_launch_context,
)


@pytest.mark.parametrize('layout', ['source', 'tools', 'tools/fleet'])
def test_local_retirement_uses_the_packaged_owner(tmp_path, layout):
    world = _World(tmp_path / 'world', maxima=4096)
    descs = _descriptors(world.template, world.inst, 'p1', b'package-egress')
    _prewrite(world.q, world.inst, world.template, 'b1', descs)
    first = world.first_publish('b1', descs)
    world.run_mover(str(first['mover_key']), 'package-source-proof')
    staged = world.stage_path('p1.bin')
    assert staged.read_bytes() == b'package-egress'
    before = list(world.q.ready())
    root = REPO
    if layout != 'source':
        import publish_runtime
        root = tmp_path / 'generation'
        for name in publish_runtime._publication_manifest():
            target = root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(publish_runtime._source_for(name), target)
    data = tmp_path / 'retirement.json'
    data.write_text(json.dumps({
        'instance': world.inst, 'template': world.template,
        'queue': str(world.q.root), 'stage': str(world.stage_root),
        'residency': str(world.out_base),
    }))
    program = r'''
import json, pathlib, runpy, sys
root, data, layout = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
sys.path.insert(0, str(root / 'src'))
from prismabuild import produced_output, pool
assert 'prismabuild.stage_release' not in sys.modules
if layout != 'source':
    script = str(root / layout / 'stage_release.py')
    sys.argv = [script, '--help']
    try:
        runpy.run_path(script, run_name='__main__')
    except SystemExit as ended:
        assert ended.code == 0
    sys.path[:] = [p for p in sys.path if not pathlib.Path(p).is_relative_to(root / 'tools')]
config = json.loads(data.read_text())
queue = pool.PoolQueue(pathlib.Path(config['queue']))
answer = produced_output.retire_batch(
    queue, config['instance'], config['template'], 'b1',
    stage_root=config['stage'], residency_root=config['residency'])
assert answer.get('ok') is True, answer
from prismabuild import prewarm_loop, stage_move, stage_release, ram_promote, stage_rounding
modules = (prewarm_loop, stage_move, stage_release, ram_promote, stage_rounding)
origins = {m.__name__: str(pathlib.Path(m.__file__).resolve()) for m in modules}
for name, path in origins.items():
    assert pathlib.Path(path) == root / 'src' / 'prismabuild' / (name.rsplit('.', 1)[1] + '.py')
    assert name.rsplit('.', 1)[1] not in sys.modules
assert all(not pathlib.Path(p).is_relative_to(root / 'tools') for p in sys.path)
print('package-proof: ' + json.dumps({'answer': answer, 'origins': origins, 'sys_path': sys.path}))
'''
    result = subprocess.run(
        [sys.executable, '-I', '-S', '-c', program, str(root), str(data), layout],
        cwd=tmp_path, text=True, capture_output=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr
    proof = json.loads(next(line.removeprefix('package-proof: ') for line in result.stdout.splitlines() if line.startswith('package-proof: ')))
    assert proof['answer']['ok'] is True
    assert not staged.exists()
    assert list(world.q.ready()) == before
    assert world.entry('b1')['retired'] is True
    print('package-proof: ' + json.dumps(proof))

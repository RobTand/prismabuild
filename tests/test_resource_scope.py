"""Per-attempt telemetry and selective memory containment."""
from pathlib import Path
import pytest

from prismabuild.resource_scope import ResourceScope, read_cgroup


def _group(path, cpu=2000000, memory=1234):
    path.mkdir(parents=True)
    (path / 'cpu.stat').write_text(f'usage_usec {cpu}\n')
    (path / 'memory.current').write_text(str(memory))
    (path / 'memory.peak').write_text(str(memory + 1))
    (path / 'memory.events').write_text('oom 0\noom_kill 0\n')
    return path


def test_scope_identity_separates_attempts(tmp_path, monkeypatch):
    calls = []
    def request(self, op, **extra):
        calls.append((self.nonce, op, extra))
        unit = 'prismabuild-job' + self.nonce + '.slice'
        return {'ok': True, 'scope_id': unit, 'token': 'b' * 64,
                'cgroup_path': '/sys/fs/cgroup/prismabuild.slice/' + unit}
    monkeypatch.setattr(ResourceScope, '_request', request)
    one = ResourceScope('a'*64, '1'*32, 1024**3, tmp_path / 'one.json')
    two = ResourceScope('a'*64, '2'*32, 1024**3, tmp_path / 'two.json')
    one.create()
    two.create()
    assert one.unit != two.unit
    assert calls[0] == ('1'*32, 'create', {'memory_max_bytes': 1024**3})
    argv = one.wrap_argv(['/bin/true'])
    assert argv[-2:] == ['--', '/bin/true']
    assert argv[argv.index('--token')+1] == 'b'*64


def test_cgroup_reads_whole_tree_counters(tmp_path):
    result = read_cgroup(_group(tmp_path / 'cg'))
    assert result['cpu_seconds'] == 2
    assert result['memory_current_bytes'] == 1234
    assert result['memory_peak_bytes'] == 1235


def test_missing_scope_is_incomplete_not_idle(tmp_path):
    scope = ResourceScope('a'*64, '1'*32, 1024**3, tmp_path / 'one.json')
    scope.cgroup_path = tmp_path / 'missing'
    sample = scope.sample()
    assert not sample['complete']


def test_scope_rejects_invalid_identity(tmp_path):
    with pytest.raises(ValueError):
        ResourceScope('../elsewhere', '1'*32, 1024, tmp_path / 'x')

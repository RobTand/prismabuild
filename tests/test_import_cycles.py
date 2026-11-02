"""Import cycles among PrismaBuild's own modules may shrink and never grow (PB #2582).

``tools/ci/import_cycles.py`` finds the strongly connected components of the
import graph over ``src/prismabuild/`` and ``tools/``, counting function-local
and ``TYPE_CHECKING`` imports. ``tests/fixtures/import_cycle_baseline.json``
records each component's members and internal edges. A new member, a new
internal edge, or a component larger than the largest recorded fails. So does
a baseline entry that is no longer in a cycle: each cut shrinks the baseline
in the same change, by running ``python tools/ci/import_cycles.py --write-baseline``.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('import_cycles', ROOT / 'tools/ci/import_cycles.py')
cycles = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cycles)


def plant(root: Path, sources: dict[str, str]) -> Path:
    for name, content in sources.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding='utf-8')
    return root


def test_a_function_local_import_closes_a_cycle(tmp_path):
    root = plant(tmp_path, {
        'src/prismabuild/__init__.py': '',
        'src/prismabuild/low.py': 'def later():\n    from . import high\n    return high.X\n',
        'src/prismabuild/high.py': 'from prismabuild import low\nX = 1\n',
        'src/prismabuild/alone.py': 'from . import low\n',
    })
    state = cycles.cycle_state(root)
    assert state['components'] == [{
        'members': ['src/prismabuild/high.py', 'src/prismabuild/low.py'],
        'edges': ['src/prismabuild/high.py -> src/prismabuild/low.py',
                  'src/prismabuild/low.py -> src/prismabuild/high.py'],
    }]


def test_type_checking_and_literal_loader_imports_are_edges(tmp_path):
    root = plant(tmp_path, {
        'src/prismabuild/__init__.py': '',
        'src/prismabuild/a.py': ('from typing import TYPE_CHECKING\n'
                                 'if TYPE_CHECKING:\n    from .b import Thing\n'),
        'src/prismabuild/b.py': 'import importlib\nc = importlib.import_module("prismabuild.c")\n',
        'src/prismabuild/c.py': 'def f():\n    return __import__("prismabuild.a")\n',
    })
    [component] = cycles.cycle_state(root)['components']
    assert component['members'] == ['src/prismabuild/a.py', 'src/prismabuild/b.py', 'src/prismabuild/c.py']


def test_a_relative_import_resolves_inside_its_package_and_loads_the_package(tmp_path):
    root = plant(tmp_path, {
        'src/prismabuild/__init__.py': 'from . import core\n',
        'src/prismabuild/core.py': 'from prismabuild.sub import leaf\n',
        'src/prismabuild/sub/__init__.py': '',
        'src/prismabuild/sub/leaf.py': 'from .. import core\n',
    })
    graph = cycles.import_graph(root)
    assert graph['src/prismabuild/sub/leaf.py'] == {'src/prismabuild/__init__.py', 'src/prismabuild/core.py'}
    assert 'src/prismabuild/sub/__init__.py' in graph['src/prismabuild/core.py']
    members = {m for c in cycles.cycle_state(root)['components'] for m in c['members']}
    assert {'src/prismabuild/core.py', 'src/prismabuild/sub/leaf.py'} <= members


def test_tools_modules_resolve_by_their_short_names(tmp_path):
    root = plant(tmp_path, {
        'tools/fleet/one.py': 'import two\n',
        'tools/fleet/two.py': 'def f():\n    import one\n',
        'tools/other.py': 'import one\n',
    })
    [component] = cycles.cycle_state(root)['components']
    assert component['members'] == ['tools/fleet/one.py', 'tools/fleet/two.py']


def test_a_new_member_a_new_edge_or_a_bigger_cycle_is_a_regression():
    baseline = {'components': [{'members': ['a', 'b'], 'edges': ['a -> b', 'b -> a']}]}
    assert cycles.cycle_regressions(baseline, baseline) == []
    grown = {'components': [{'members': ['a', 'b', 'c'], 'edges': ['a -> b', 'b -> a', 'b -> c', 'c -> a']}]}
    found = cycles.cycle_regressions(grown, baseline)
    assert 'new module joined an import cycle: c' in found
    assert 'new import edge inside a cycle: c -> a' in found
    assert any('exceeds the largest recorded, 2' in line for line in found)
    returned = {'components': [{'members': ['a', 'b'], 'edges': ['a -> b', 'b -> a', 'a -> a2']}]}
    assert cycles.cycle_regressions(returned, baseline) == ['new import edge inside a cycle: a -> a2']


def test_a_cut_the_baseline_still_records_is_an_unrecorded_gain():
    baseline = {'components': [{'members': ['a', 'b', 'c'], 'edges': ['a -> b', 'b -> a', 'b -> c', 'c -> a']}]}
    cut = {'components': [{'members': ['a', 'b'], 'edges': ['a -> b', 'b -> a']}]}
    assert cycles.cycle_regressions(cut, baseline) == []
    assert cycles.unrecorded_gains(cut, baseline) == [
        'module no longer in a cycle: c',
        'edge no longer inside a cycle: b -> c',
        'edge no longer inside a cycle: c -> a',
    ]


def test_writing_the_baseline_refuses_a_regression(tmp_path, monkeypatch):
    root = plant(tmp_path, {
        'src/prismabuild/__init__.py': '',
        'src/prismabuild/x.py': 'from . import y\n',
        'src/prismabuild/y.py': 'def f():\n    from . import x\n',
        'tests/fixtures/import_cycle_baseline.json': json.dumps({'components': []}),
    })
    assert cycles.main(['--root', str(root), '--write-baseline']) == 1
    assert json.loads((root / 'tests/fixtures/import_cycle_baseline.json').read_text()) == {'components': []}
    (root / 'src/prismabuild/y.py').write_text('X = 1\n')
    assert cycles.main(['--root', str(root), '--write-baseline']) == 0
    assert json.loads((root / 'tests/fixtures/import_cycle_baseline.json').read_text()) == {'components': []}


@pytest.fixture(scope='module')
def live_cycles():
    return cycles.cycle_state()


def _baseline():
    return json.loads(cycles.BASELINE.read_text(encoding='utf-8'))


def test_no_import_cycle_grows(live_cycles):
    worse = cycles.cycle_regressions(live_cycles, _baseline())
    assert not worse, (
        'an import cycle grew (PB #2582). A module in a cycle depends on every other member, so '
        'Bazel reruns the tests of all of them when any one changes. Import the lower module '
        'directly, move the shared name down, or pass the dependency in:\n  ' + '\n  '.join(worse))


def test_the_baseline_records_every_cut(live_cycles):
    gained = cycles.unrecorded_gains(live_cycles, _baseline())
    assert not gained, (
        'the import cycle baseline records edges that are no longer in a cycle; run '
        '`python tools/ci/import_cycles.py --write-baseline` to record the cut:\n  ' + '\n  '.join(gained))

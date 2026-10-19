"""Actual current prelaunch records and retained reader controls for the C10 port."""
from __future__ import annotations

import base64
import json
from pathlib import Path
import shutil
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
sys.path.insert(0, str(ROOT / 'tests/compat/support'))
from maintenance.compose_compat_tier import REPLACEMENTS, port, transformed
from record_compat import accepted, invoke, retained
from record_compat_contract import WriterEvidence
from prismabuild.record_kinds import KINDS

DATA = ROOT / 'tests/compat'
GOLDENS = {row['case']: row for path in DATA.glob('goldens*.jsonl')
           for line in path.read_text().splitlines() for row in [json.loads(line)]}
HISTORY = {(row['generation'], row['case']): row for path in DATA.glob('historical*.jsonl')
           for line in path.read_text().splitlines() for row in [json.loads(line)]}
CASES = [kind.name + '/' + variant for kind in KINDS if kind.name.startswith('pg-')
         for variant in kind.variants]


@pytest.fixture(scope='module')
def current_driver(tmp_path_factory):
    path = tmp_path_factory.mktemp('current-tier-driver') / 'support'
    report = port(ROOT, destination=path)
    assert report['current_only'] and not report['historical_driver_changed']
    return path / 'compat_adapters.py'


@pytest.fixture(scope='module')
def writer_evidence(pytestconfig):
    plugin = pytestconfig.pluginmanager.get_plugin('compat-writer-evidence')
    if plugin is None:
        plugin = WriterEvidence()
        pytestconfig.pluginmanager.register(plugin, 'compat-writer-evidence')
    return plugin


@pytest.fixture(scope='module')
def reader_controls(tmp_path_factory):
    directory = tmp_path_factory.mktemp('retained-tier-controls')
    original = DATA / 'generations'
    index = json.loads((original / 'window.json').read_bytes())['generations']
    index += json.loads((original / 'prepared.json').read_bytes())
    (directory / 'prepared.json').write_text(json.dumps(index))
    for row in index:
        for suffix in ('.json', '.tar.gz'):
            (directory / (row['generation'] + suffix)).symlink_to(original / (row['generation'] + suffix))
    with retained(directory, history_control=True) as sources:
        yield sources


@pytest.mark.parametrize('case', CASES)
def test_current_port_preserves_actual_bytes_and_both_retained_read_directions(
        case, current_driver, reader_controls, writer_evidence, tmp_path):
    def current(store, raw=None):
        return accepted(invoke(ROOT, case, store, raw=raw, adapter_path=current_driver))

    first_result = invoke(ROOT, case, tmp_path / 'first', adapter_path=current_driver)
    first = base64.b64decode(accepted(first_result)['raw'])
    writer_evidence.observe(first_result, first)
    writer_evidence.records[case].update({key: first_result[key] for key in
                                         ('adapters_sha256', 'python', 'machine', 'affinity')})
    second = base64.b64decode(current(tmp_path / 'second')['raw'])
    assert first == second == base64.b64decode(GOLDENS[case]['raw'])
    for generation, source, manifest in reader_controls:
        prior = base64.b64decode(accepted(invoke(source, case, tmp_path / generation / 'writer',
                                               manifest=manifest))['raw'])
        assert prior == base64.b64decode(HISTORY[generation, case]['raw'])
        current(tmp_path / generation / 'forward', prior)
        accepted(invoke(source, case, tmp_path / generation / 'reverse', raw=first, manifest=manifest))


def test_ambiguous_preimage_refuses_before_staging_any_driver(tmp_path):
    support = tmp_path / 'root/tests/compat/support'
    shutil.copytree(DATA / 'support', support)
    rule = REPLACEMENTS[0]
    selected = support / rule.file
    selected.write_text(selected.read_text().replace(rule.after, 'unexpected_current_owner'))
    before = {p.name: p.read_bytes() for p in support.glob('*.py')}
    destination = tmp_path / 'driver'
    with pytest.raises(ValueError, match='ambiguous preimage'):
        port(tmp_path / 'root', destination=destination)
    assert not destination.exists()
    assert before == {p.name: p.read_bytes() for p in support.glob('*.py')}


def test_shared_driver_port_refuses_without_retained_selector(tmp_path):
    support = tmp_path / 'tests/compat/support'
    shutil.copytree(DATA / 'support', support)
    (support / 'record_compat.py').write_text('def invoke(): pass\n')
    before = {p.name: p.read_bytes() for p in support.glob('*.py')}
    with pytest.raises(ValueError, match='retained/current driver selector first'):
        port(tmp_path)
    assert before == {p.name: p.read_bytes() for p in support.glob('*.py')}


def test_port_is_idempotent_and_preserves_the_generic_floor_ledger(current_driver):
    for name in {rule.file for rule in REPLACEMENTS}:
        text = (current_driver.parent / name).read_text()
        assert transformed(text, [rule for rule in REPLACEMENTS if rule.file == name]) == text
    floor = (current_driver.parent / 'compat_floor_adapters.py').read_text()
    assert 'ledger = pool.ResourceLedger(queue.root / ff.FILESYSTEM_RESERVATIONS' in floor
    assert 'assert ledger.acquire(holder, {"filesystem_gib": amount})' in floor



def test_shared_driver_port_preserves_the_complete_private_copy_on_repeat(tmp_path):
    root = tmp_path / 'source'
    support = root / 'tests/compat/support'
    shutil.copytree(DATA / 'support', support,
                    ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    shutil.copyfile(DATA / 'drivers-before-c13.tar.gz',
                    root / 'tests/compat/drivers-before-c13.tar.gz')
    before = {str(path.relative_to(root)): path.read_bytes()
              for path in root.rglob('*') if path.is_file()}
    first = port(root)
    second = port(root)
    assert first['current_only'] and not first['historical_driver_changed']
    assert second['current_only'] and not second['historical_driver_changed']
    assert first['files'] == second['files']
    assert all(not row['changed'] for row in first['files'])
    assert before == {str(path.relative_to(root)): path.read_bytes()
                      for path in root.rglob('*') if path.is_file()}

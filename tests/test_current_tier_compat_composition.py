"""The C10 driver copy preserves the canonical compatibility population."""
from __future__ import annotations

from pathlib import Path
import shutil
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
sys.path.insert(0, str(ROOT / 'tests/compat/support'))
from maintenance.compose_compat_tier import REPLACEMENTS, port, transformed

DATA = ROOT / 'tests/compat'


@pytest.fixture(scope='module')
def current_driver(tmp_path_factory):
    path = tmp_path_factory.mktemp('current-tier-driver') / 'support'
    report = port(ROOT, destination=path)
    assert report['current_only'] and not report['historical_driver_changed']
    return path / 'compat_adapters.py'


def test_current_port_preserves_the_qualified_driver(current_driver):
    original = {path.name: path.read_bytes() for path in (DATA / 'support').glob('*.py')}
    copied = {path.name: path.read_bytes() for path in current_driver.parent.glob('*.py')}
    assert copied == original


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

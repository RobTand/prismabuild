"""Run main's byte-pinned CPU tests unchanged, beside the opted regressions."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET

import pytest

ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / 'tests/fixtures/cpu_legacy_main_43234d0f'
MANIFEST_PIN = 'b5d5919010b58a4cd38892063f984242c9b3f87f1d4f72e8a37c7dec2d0eeddc'
MANIFEST = json.loads((REFERENCE / 'manifest.json').read_bytes())
HOST_READS_PIN = 'b1554fe17e7b179e0e7d6012b0d51cf2948fe5eeed921d618699157dc1a11d67'


def _prepare_legacy_checkout(source, checkout):
    assert hashlib.sha256((REFERENCE / 'manifest.json').read_bytes()).hexdigest() == MANIFEST_PIN
    assert MANIFEST['main_commit'] == '43234d0fbd42d6ed76da38658cb9f447ed19a606'
    tests = checkout / 'tests'
    shutil.copytree(source / 'tests', tests,
                    ignore=shutil.ignore_patterns('__pycache__', '.pytest_cache'))
    for name, pin in MANIFEST['files'].items():
        raw = (REFERENCE / (name + '.fixture')).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == pin['sha256']
        (tests / name).write_bytes(raw)
    for name in ('src', 'tools'):
        shutil.copytree(source / name, checkout / name)
    # The archived test needs its original support after the production relocation.
    support = (REFERENCE / 'queue_bell_host_reads.py.fixture').read_bytes()
    assert hashlib.sha256(support).hexdigest() == HOST_READS_PIN
    (checkout / 'src/prismabuild/queue_bell_host_reads.py').write_bytes(support)
    for name in ('pyproject.toml', 'pytest.ini'):
        if (source / name).is_file():
            shutil.copyfile(source / name, checkout / name)


def test_archived_host_review_collects_without_production_host_reads(tmp_path):
    source = tmp_path / 'relocated'
    shutil.copytree(ROOT / 'src', source / 'src',
                    ignore=shutil.ignore_patterns('queue_bell_host_reads.py', '__pycache__'))
    for name in ('tests', 'tools'):
        (source / name).symlink_to(ROOT / name, target_is_directory=True)
    checkout = tmp_path / 'snapshot'
    _prepare_legacy_checkout(source, checkout)
    result = subprocess.run([sys.executable, '-m', 'pytest', '--collect-only', '-q',
        'tests/test_queue_bell_host_review_edges.py'], cwd=checkout,
        env=dict(os.environ, PYTHONPATH=str(checkout / 'src') + os.pathsep + str(checkout / 'tools/fleet'),
                 PRISMABUILD_BOX_STATE_ROOT=str(tmp_path / 'box-state')),
        text=True, capture_output=True, timeout=1200)
    assert result.returncode == 0, result.stdout[-10000:] + result.stderr[-4000:]


@pytest.mark.parametrize('filename', MANIFEST['selected'])
def test_main_cpu_tests_run_unchanged_at_cap_zero(tmp_path, filename):
    checkout = tmp_path / 'snapshot'
    _prepare_legacy_checkout(ROOT, checkout)
    tests = checkout / 'tests'
    report = tmp_path / 'results.xml'
    result = subprocess.run([sys.executable, '-m', 'pytest', '-q', str(tests / filename),
        '--junitxml', str(report)], cwd=checkout,
        env=dict(os.environ, PYTHONPATH=str(checkout / 'src') + os.pathsep + str(checkout / 'tools/fleet'),
                 PRISMABUILD_BOX_STATE_ROOT=str(tmp_path / 'box-state')),
        text=True, capture_output=True, timeout=1200)
    assert result.returncode == 0, result.stdout[-10000:] + result.stderr[-4000:]
    cases = list(ET.parse(report).getroot().iter('testcase'))
    assert cases and not any(list(case.iter('failure')) or list(case.iter('error')) for case in cases)
    skipped = [case.get('name') for case in cases if list(case.iter('skipped'))]
    print('LEGACY_MAIN_FILE ' + json.dumps({'file': filename, 'tests': len(cases),
        'passed': len(cases) - len(skipped), 'skipped': skipped, 'cap': 0}, sort_keys=True))

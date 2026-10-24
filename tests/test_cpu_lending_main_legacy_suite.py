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


@pytest.mark.parametrize('filename', MANIFEST['selected'])
def test_main_cpu_tests_run_unchanged_at_cap_zero(tmp_path, filename):
    assert hashlib.sha256((REFERENCE / 'manifest.json').read_bytes()).hexdigest() == MANIFEST_PIN
    assert MANIFEST['main_commit'] == '43234d0fbd42d6ed76da38658cb9f447ed19a606'
    checkout = tmp_path / 'snapshot'
    tests = checkout / 'tests'
    tests.mkdir(parents=True)
    for name, pin in MANIFEST['files'].items():
        raw = (REFERENCE / (name + '.fixture')).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == pin['sha256']
        (tests / name).write_bytes(raw)
    shutil.copyfile(ROOT / 'tests/conftest.py', tests / 'conftest.py')
    for name in ('src', 'tools'):
        shutil.copytree(ROOT / name, checkout / name)
    for name in ('pyproject.toml', 'pytest.ini'):
        if (ROOT / name).is_file():
            shutil.copyfile(ROOT / name, checkout / name)
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

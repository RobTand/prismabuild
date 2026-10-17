"""Runtime adoption checks the helper behind Core's unchanged closed wire body."""
from pathlib import Path
import runpy

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('mutation', ['helper', 'core-pin', 'duplicate-pin', 'missing-pin'])
def test_runtime_capture_refuses_a_stale_or_ambiguous_helper_binding(tmp_path, mutation):
    check = runpy.run_path(str(ROOT / 'tools/maintenance/refresh_compat_runtime.py'))['runtime_source_pin']
    package = tmp_path / 'src/prismabuild'
    package.mkdir(parents=True)
    for name in ('core.py', 'immutable_files.py'):
        (package / name).write_bytes((ROOT / 'src/prismabuild' / name).read_bytes())
    original = check(tmp_path)
    core = package / 'core.py'
    if mutation == 'helper':
        helper = package / 'immutable_files.py'
        helper.write_bytes(helper.read_bytes() + b'\n')
    elif mutation == 'core-pin':
        core.write_text(core.read_text().replace(original['sha256'], '0' * 64, 1))
    elif mutation == 'duplicate-pin':
        core.write_text(core.read_text() + '\n_IMMUTABLE_FILES_SOURCE = {}\n')
    else:
        core.write_text(core.read_text().replace('_IMMUTABLE_FILES_SOURCE = ', '_RETIRED_SOURCE_PIN = '))
    with pytest.raises(ValueError, match='exact immutable helper pin'):
        check(tmp_path)


def test_runtime_capture_binds_the_actual_helper_without_extending_the_runtime_body():
    check = runpy.run_path(str(ROOT / 'tools/maintenance/refresh_compat_runtime.py'))['runtime_source_pin']
    identity = check(ROOT)
    assert identity['bytes'] == (ROOT / 'src/prismabuild/immutable_files.py').stat().st_size
    assert set(identity) == {'bytes', 'sha256'}

"""Verify retained remote ancestry and unchanged helper, guards and goldens; no execution."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

REMOTE = 'a7b0b8def49fa992e7676304753eced414b29b78'
REPAIR = 'affe5b27eedd8e2ac55e9b07e52ad7b64e0e1db8'
INTEGRATED = '85fa57bb7194afe9562193a7368d2e3a403a9293'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()

    def git(*argv):
        return subprocess.check_output(['git', '-C', str(args.repo), *argv])

    def source(head, path):
        return git('show', head + ':' + path)

    for parent in (REMOTE, REPAIR):
        git('merge-base', '--is-ancestor', parent, INTEGRATED)
    changed = git('diff', '--name-only', REMOTE, INTEGRATED).decode().splitlines()
    assert set(changed) == {
        'docs/design.md', 'src/prismabuild/core.py', 'src/prismabuild/numeric_fields.py',
        'src/prismabuild/_gang.py', 'src/prismabuild/residency_plan.py',
        'tests/test_core_numbers_1521.py',
    }
    for path in ('src/prismabuild/core.py', 'src/prismabuild/numeric_fields.py',
                 'src/prismabuild/residency_plan.py', 'tests/test_core_numbers_1521.py'):
        assert source(INTEGRATED, path) == source(REPAIR, path), path
    gang = source(REMOTE, 'src/prismabuild/_gang.py').decode()
    expected = gang.replace('from . import contracts, core',
                            'from . import contracts, core, numeric_fields')
    expected = expected.replace('core.is_finite_number', 'numeric_fields.is_finite_number')
    assert source(INTEGRATED, 'src/prismabuild/_gang.py').decode() == expected
    guards = ('tests/compat/field_sets.json', 'tests/test_record_compatibility_core.py',
              'tests/test_record_compatibility.py', 'tests/test_wire_inventory.py',
              'tests/test_duplication_baseline.py', 'tests/test_runtime_capture_source_binding.py',
              'tests/test_runtime_source_pin.py')
    for path in guards:
        assert source(INTEGRATED, path) == source(REMOTE, path), path
    assert git('diff', '--name-only', REMOTE, INTEGRATED, '--', 'tests/compat') == b''
    answer = dict(head=INTEGRATED, parents=git('show', '-s', '--format=%P', INTEGRATED).decode().split(),
                  preserved_remote=REMOTE, repair=REPAIR, changed_paths_from_remote=changed,
                  core_sha256=hashlib.sha256(source(INTEGRATED, 'src/prismabuild/core.py')).hexdigest(),
                  checks_passed=True,
                  limits='Static ancestry and exact source readback only; integrated behavioral proof is separate.')
    args.out.write_text(json.dumps(answer, indent=2) + '\n')
    print(json.dumps(dict(head=INTEGRATED, checks_passed=True)))


if __name__ == '__main__':
    main()

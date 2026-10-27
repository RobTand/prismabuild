"""Read Git objects to verify the helper move and unchanged attested inputs."""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import subprocess

OLD = '8b2835da9254e515e11aa5ce1fc774067f9f14a6'
BASE = '87bf80ee5ed9f494ded1ab06ed858308135b4d26'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', required=True, type=Path)
    parser.add_argument('--head', required=True)
    parser.add_argument('--out', required=True, type=Path)
    args = parser.parse_args()

    def git(*argv):
        return subprocess.check_output(['git', '-C', str(args.repo), *argv])

    def source(head, path):
        return git('show', head + ':' + path)

    core = 'src/prismabuild/core.py'
    assert source(args.head, core) == source(BASE, core)
    assert source(args.head, core) == source('0d7c43c9424b8acd3f36ee74e076a14871e2ab12', core)

    def predicate(raw):
        return next(n for n in ast.parse(raw).body
                    if isinstance(n, ast.FunctionDef) and n.name == 'is_finite_number')

    owner = 'src/prismabuild/numeric_fields.py'
    assert ast.dump(predicate(source(OLD, core))) == ast.dump(predicate(source(args.head, owner)))
    assert b'is_finite_number' not in source(args.head, core)
    for path in ['src/prismabuild/_gang.py', 'src/prismabuild/residency_plan.py']:
        current = source(args.head, path)
        assert b'core.is_finite_number' not in current and b'pb.is_finite_number' not in current
        assert b'numeric_fields.is_finite_number' in current

    test = 'tests/test_core_numbers_1521.py'
    before = source(OLD, test).decode().replace('from prismabuild import core', 'from prismabuild import numeric_fields').replace('core.is_finite_number', 'numeric_fields.is_finite_number').replace('share the core predicate', 'share a predicate')
    assert before.encode() == source(args.head, test)
    guards = ['tests/test_record_compatibility_core.py', 'tests/test_record_compatibility.py',
              'tests/test_duplication_baseline.py', 'tests/test_wire_inventory.py',
              'tests/test_runtime_capture_source_binding.py', 'tests/test_runtime_source_pin.py',
              'tests/compat/field_sets.json', 'src/prismabuild/digest_primitives.py',
              'src/prismabuild/immutable_files.py']
    for path in guards:
        assert source(OLD, path) == source(args.head, path), path
    assert git('diff', '--name-only', OLD, args.head, '--', 'tests/compat') == b''
    paths = [core, owner, test, *guards, 'src/prismabuild/_gang.py', 'src/prismabuild/residency_plan.py']
    hashes = {path: hashlib.sha256(source(args.head, path)).hexdigest() for path in paths}
    args.out.write_text(json.dumps({'head': args.head, 'prior_head': OLD,
                                   'core_matches_pre2344': True, 'predicate_ast_unchanged': True,
                                   'numeric_assertions_unchanged': True, 'source_sha256': hashes,
                                   'limits': 'Static source identity audit. Behavioral evidence comes from PB.'}, indent=2) + '\n')
    print(json.dumps({'head': args.head, 'core_sha256': hashes[core], 'checks_passed': True}))


if __name__ == '__main__':
    main()

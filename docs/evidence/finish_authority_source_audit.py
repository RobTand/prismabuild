"""Read-only source proof for the restored-finisher publication repair.

Run with Python's standard library, from any checkout containing the commits:
python3 docs/evidence/finish_authority_source_audit.py --checkout .
"""
import argparse
import ast
from collections import Counter
import copy
import json
from pathlib import Path
import subprocess

options = argparse.ArgumentParser(description=__doc__)
options.add_argument('--checkout', type=Path, default=Path(__file__).resolve().parents[2])
options.add_argument('--base', default='fb1f43e10625d4897877e323528ea207c6451c0d')
options.add_argument('--source', default='e2e9b1697b7fdd612d61767d328d789206762d21')
args = options.parse_args()


def _authority_git(*words):
    return subprocess.check_output(['git', '-C', str(args.checkout), *words])


def _authority_ast(pin, name):
    return ast.parse(_authority_git('show', pin + ':' + name))


def _authority_dump(node):
    return ast.dump(node, include_attributes=False)


def _authority_paths(pin):
    result = {}
    for entry in _authority_git('ls-tree', '-rz', pin).split(b'\0'):
        if entry:
            identity, path = entry.split(b'\t', 1)
            result[path.decode()] = identity
    return result


before, current = _authority_paths(args.base), _authority_paths(args.source)
assert set(before) <= set(current)
assert {name for name in before if before[name] != current[name]} == {
    'docs/design.md', 'src/prismabuild/lifecycle.py',
    'src/prismabuild/record_store.py', 'src/prismabuild/pool.py'}
assert set(current) - set(before) == {
    'tests/test_resumed_finish_authority.py', 'tests/test_resumed_retry_authority.py'}
old = _authority_ast(args.base, 'src/prismabuild/lifecycle.py')
new = _authority_ast(args.source, 'src/prismabuild/lifecycle.py')
assert Counter(_authority_dump(n) for n in ast.walk(old) if isinstance(n, ast.Assert)) == Counter(
    _authority_dump(n) for n in ast.walk(new) if isinstance(n, ast.Assert))
commit = next(n for n in new.body if isinstance(n, ast.FunctionDef) and n.name == '_commit_claim')
guards = [n for n in commit.body if (
    isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'guarded_publication' for t in n.targets)
    or isinstance(n, ast.If) and isinstance(n.test, ast.Name) and n.test.id == 'guarded_publication')]
assert len(guards) == 3
predicate = next(n.value for n in guards if isinstance(n, ast.Assign))
assert _authority_dump(predicate) == _authority_dump(ast.parse(
    'transition is Transition.FINISH or (transition is Transition.EXPIRE and disposition in {DONE, FAILED})',
    mode='eval').body)
commit.body = [n for n in commit.body if n not in guards]
helper = next(n for n in new.body if isinstance(n, ast.FunctionDef) and n.name == '_publish_claim_destination')
assert ast.unparse(commit.body[-1]) == 'return _publish_claim_destination(queue, plan, tombstone)'
commit.body[-1:] = copy.deepcopy(helper.body[3:])
new.body.remove(helper)
archive = next(n for n in new.body if isinstance(n, ast.FunctionDef) and n.name == '_archive_earlier_terminal')
old_archive = next(n for n in old.body if isinstance(n, ast.FunctionDef) and n.name == archive.name)
assert isinstance(archive.body[0], ast.Expr) and isinstance(archive.body[0].value, ast.Constant)
archive.body[0] = copy.deepcopy(old_archive.body[0])
assert _authority_dump(new) == _authority_dump(old)

old_store = _authority_ast(args.base, 'src/prismabuild/record_store.py')
new_store = _authority_ast(args.source, 'src/prismabuild/record_store.py')
store = next(n for n in new_store.body if isinstance(n, ast.ClassDef) and n.name == 'RecordStore')
questions = [n for n in store.body if isinstance(n, ast.FunctionDef) and n.name == 'finishing_mark_identity']
assert len(questions) == 1
store.body.remove(questions[0])
assert _authority_dump(new_store) == _authority_dump(old_store)
assert _authority_dump(_authority_ast(args.base, 'src/prismabuild/pool.py')) == _authority_dump(
    _authority_ast(args.source, 'src/prismabuild/pool.py'))
subprocess.run(['git', '-C', str(args.checkout), 'diff', '--check', args.base + '..' + args.source], check=True)
print(json.dumps(dict(verified=True, base=args.base, source=args.source,
                     tree=_authority_git('rev-parse', args.source + '^{tree}').decode().strip(),
                     original_tracked_paths=len(before), all_original_test_bytes_preserved=True,
                     original_lifecycle_AST_restored_after_guard_removal_and_helper_inlining=True,
                     pool_AST_unchanged=True, Store_delta='one strict regular-file identity question')))

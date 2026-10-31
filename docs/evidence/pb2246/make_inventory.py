"""Compare the reviewed profile tests with their original claims."""
import ast
import json
from pathlib import Path
import subprocess

FILES = ['test_sample_profile.py', 'test_profile_primary_checkpoint.py',
         'test_sample_docker_coverage.py', 'test_contained_deadline_profile.py',
         'test_profile_window_deadline.py']


def inventory():
    rows = []
    for file in FILES:
        path = 'tests/' + file
        old = ast.parse(subprocess.check_output(
            ['git', 'show', '322addff9d^:' + path], text=True))
        new = ast.parse(Path(path).read_text())
        def tests(tree):
            return {n.name: n for n in tree.body
                    if isinstance(n, ast.FunctionDef) and n.name.startswith('test_')}
        before, after = tests(old), tests(new)
        for name in sorted(before.keys() | after.keys()):
            a, b = before.get(name), after.get(name)
            if a and b and ast.dump(a) == ast.dump(b):
                continue
            node = b or a
            claim = (ast.get_docstring(node) or name[5:].replace('_', ' ')).split('\n')[0]
            rows.append({
                'file': path, 'test': name,
                'status': 'rewritten' if a and b else 'added' if b else 'deleted',
                'claim': claim, 'current_proof': name if b else 'none',
                'before_assertions': [ast.unparse(n) for n in ast.walk(a)
                                      if isinstance(n, ast.Assert)] if a else [],
                'after_assertions': [ast.unparse(n) for n in ast.walk(b)
                                     if isinstance(n, ast.Assert)] if b else [],
            })
    return rows


if __name__ == '__main__':
    print(json.dumps(inventory(), indent=2))

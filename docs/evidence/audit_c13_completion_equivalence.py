"""Audit the bounded C13 composition corrections without importing product code."""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import subprocess

unused_import_functions = {
    '_output_batch_authority', '_output_precommit_authority',
    'stage_output_intent', 'fund_output_batch', '_output_published_material',
    'validate_produced_output', 'build_produced_output_batch_ref',
    'validate_produced_output_batch',
}

class Normalize(ast.NodeTransformer):
    def __init__(self):
        self.prewrite_function = False
        self.current_function = None

    def visit_ImportFrom(self, node):
        if self.current_function in unused_import_functions and node.level == 1 and node.module is None and len(node.names) == 1:
            name = node.names[0]
            if name.name == 'produced_output' and name.asname == 'produced_mod':
                return None
        return node

    def visit_Try(self, node):
        result = self.generic_visit(node)
        if not result.body:
            assert self.current_function in unused_import_functions
            assert not result.orelse and not result.finalbody
            assert len(result.handlers) == 1
            assert ast.unparse(result.handlers[0].type) == 'ImportError'
            return None
        return result

    def visit_Module(self, node):
        if ast.get_docstring(node, clean=False) is not None:
            node.body.pop(0)
        return self.generic_visit(node)

    def visit_ClassDef(self, node):
        if ast.get_docstring(node, clean=False) is not None:
            node.body.pop(0)
        return self.generic_visit(node)

    def visit_FunctionDef(self, node):
        prior = self.prewrite_function
        prior_function = self.current_function
        self.current_function = node.name
        self.prewrite_function = node.name in ('_sweep_ended_prewrites', '_orphaned_prewrites') or prior
        if ast.get_docstring(node, clean=False) is not None:
            node.body.pop(0)
        result = self.generic_visit(node)
        self.prewrite_function = prior
        self.current_function = prior_function
        return result

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Assign(self, node):
        if self.prewrite_function and ast.unparse(node) == 'ledger = bl.BatchLedger.for_instance(queue.root, instance)':
            return ast.parse('directory = bl.prewrites_dir(queue.root, instance)').body[0]
        return self.generic_visit(node)

    def visit_Attribute(self, node):
        if self.prewrite_function and isinstance(node.value, ast.Name) and node.value.id == 'ledger':
            node.value = ast.parse('bl.BatchLedger.at_scope(directory.parent)', mode='eval').body
        return self.generic_visit(node)

    def visit_Call(self, node):
        name = ast.unparse(node.func)
        if name in ('bl.scopes_root', 'scopes_root'):
            assert len(node.args) == 1 and not node.keywords
            return ast.parse('Path(' + ast.unparse(node.args[0]) + ') / "residency" / oc.OUTPUT_SCOPES_SUBDIR', mode='eval').body
        return self.generic_visit(node)


def git(root, *args):
    return subprocess.check_output(['git', '-C', str(root), *args])


def tree(root, pin, prefix):
    return {row.split(b'\t', 1)[1].decode(): row.split(b'\t', 1)[0]
            for row in git(root, 'ls-tree', '-r', '-z', pin, '--', prefix).split(b'\0') if row}


def audit(checkout):
    pins = {name: git(checkout, 'rev-parse', pin).decode().strip() for name, pin in {
        'combined': '7439ed043867a532c8ea120e3e133c3eb40a574a',
        'formatted': '44676419a', 'guard': '6384103e9', 'final': '1840aa0f0',
    }.items()}
    changes = {}
    for old, new in (('combined', 'formatted'), ('formatted', 'guard'), ('guard', 'final')):
        changed = git(checkout, 'diff', '--name-only', pins[old], pins[new]).decode().splitlines()
        changes[new] = changed
        for path in changed:
            if path.startswith('tests/'):
                assert new == 'guard' and path == 'tests/test_empty_retirement_intent_is_filed_before_unlink.py'
                control = git(checkout, 'show', pins[new] + ':' + path)
                assert hashlib.sha256(control).hexdigest() == '87a1ffb8390651a752f23f7b769ae0b59df0bc7ab3956df1fa57f9288fdac624'
                continue
            before = ast.parse(git(checkout, 'show', pins[old] + ':' + path))
            after = ast.parse(git(checkout, 'show', pins[new] + ':' + path))
            if new == 'guard':
                function = next(n for n in after.body if isinstance(n, ast.FunctionDef) and n.name == '_retire_consumed_batch_locked')
                guards = [n for n in ast.walk(function) if isinstance(n, ast.If)
                          and ast.dump(n.test) == ast.dump(ast.parse('not entry.is_retiring', mode='eval').body)]
                assert len(guards) == 1
                guards[0].test = ast.parse('not retiring', mode='eval').body
            elif new == 'final':
                before, after = Normalize().visit(before), Normalize().visit(after)
            assert ast.dump(before, include_attributes=False) == ast.dump(after, include_attributes=False), (old, new, path)
    assert set(changes['formatted']) == {'src/prismabuild/action_edges.py', 'src/prismabuild/batch_ledger.py', 'src/prismabuild/produced_output.py'}
    assert set(changes['guard']) == {'src/prismabuild/produced_output.py', 'tests/test_empty_retirement_intent_is_filed_before_unlink.py'}
    assert set(changes['final']) == {'src/prismabuild/action_edges.py', 'src/prismabuild/batch_ledger.py', 'src/prismabuild/produced_output.py', 'src/prismabuild/pool.py', 'src/prismabuild/produced_spool.py', 'tools/fleet/tier_loop.py'}
    ledger = ast.parse(git(checkout, 'show', pins['final'] + ':src/prismabuild/batch_ledger.py'))
    facets = next(n for n in ledger.body if isinstance(n, ast.ClassDef) and n.name == 'BatchFacets')
    predicate = next(n for n in facets.body if isinstance(n, ast.FunctionDef) and n.name == 'is_retiring')
    assert len(predicate.body) == 1 and ast.unparse(predicate.body[0]) == "return bool(self._entry.get('retiring'))"
    historical = tree(checkout, pins['combined'], 'tests/compat')
    assert historical == tree(checkout, pins['final'], 'tests/compat')
    sdk = ast.parse(git(checkout, 'show', pins['final'] + ':src/prismabuild/client.py'))
    constants = {target.id: ast.literal_eval(node.value) for node in sdk.body if isinstance(node, ast.Assign)
                 for target in node.targets if isinstance(target, ast.Name) and target.id in ('SDK_VERSION', '__all__')}
    assert constants['SDK_VERSION'] == 6 and len(constants['__all__']) == 90
    return {'verified': True, 'pins': pins, 'exact_changed_paths': changes,
            'formatting_whole_module_AST_equal': True,
            'behavioral_guard_delta': 'not retiring -> not entry.is_retiring',
            'predicate': ast.unparse(predicate),
            'tail_normalization': ['eight unused produced_output imports and three import-only error adapters removed',
                                   'same-point pure scope/instance addressing', 'comments/docstrings'],
            'all_other_production_syntax_equal': True,
            'whole_compatibility_tree_entries_unchanged': len(historical),
            'SDK_VERSION': 6, 'SDK_exports': 90,
            'runtime_or_deployment_credit': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--checkout', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    report = audit(args.checkout)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'verified': True, 'final': report['pins']['final'], 'runtime_or_deployment_credit': False}))

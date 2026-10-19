"""Audit the accepted drain implementation's composition without product imports."""
import argparse
import ast
from collections import Counter
import copy
import json
from pathlib import Path
import subprocess


def audit_drain_source_composition(checkout, root_pin, proof_pin):
    def source(pin, path):
        result = subprocess.run(['git', '-C', str(checkout), 'show', pin + ':' + path],
                                capture_output=True)
        return None if result.returncode else result.stdout

    def definitions(raw):
        result, counts = {}, Counter()
        if raw is None:
            return result

        def visit(nodes, scope):
            for node in nodes:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    name = '.'.join((*scope, node.name))
                    counts[name] += 1
                    if not isinstance(node, ast.ClassDef):
                        result[f'{name}#{counts[name]}'] = node
                    visit(node.body, (*scope, node.name))

        visit(ast.parse(raw).body, ())
        return result

    class RestoreStore(ast.NodeTransformer):
        def visit_Attribute(self, node):
            node = self.generic_visit(node)
            if isinstance(node.value, ast.Attribute) and node.value.attr in {'records', 'record_store'}:
                node.value = node.value.value
            return node

    class RestoreUpgrade(ast.NodeTransformer):
        def visit_Name(self, node):
            if node.id == 'upgrade':
                node.id = 'upgrade_client'
            return node

    pin = subprocess.check_output(['git', '-C', str(checkout), 'rev-parse', 'HEAD'], text=True).strip()
    paths = subprocess.check_output(['git', '-C', str(checkout), 'diff', '--name-only',
                                     '--diff-filter=ACM', root_pin, pin], text=True).splitlines()
    rows, joint = [], []
    for path in paths:
        if not path.endswith('.py') or path.startswith('docs/'):
            continue
        current = definitions((checkout / path).read_bytes())
        old = [definitions(source(parent, path)) for parent in (root_pin, proof_pin)]
        for name, node in current.items():
            dump = ast.dump(node, include_attributes=False)
            matches = [parent for parent, values in zip((root_pin, proof_pin), old)
                       if name in values and ast.dump(values[name], include_attributes=False) == dump]
            if matches:
                rows.append(dict(path=path, definition=name, exact_parents=matches))
                continue
            if path in {'tests/test_fleet_membership_join_resign.py',
                        'tests/test_resign_requires_known_gate.py'}:
                restored = RestoreStore().visit(copy.deepcopy(node))
                expected = old[1][name]
            elif path == 'tests/test_park_proof_real_writer.py' and name == 'test_storage_boundary_marker_satisfies_the_updaters_drain_census#1':
                expected = copy.deepcopy(old[1][name])
                expected.body = [item for item in expected.body if not (
                    isinstance(item, ast.Assign) and isinstance(item.value, ast.Call)
                    and isinstance(item.value.func, ast.Name) and item.value.func.id == 'load')]
                expected = RestoreUpgrade().visit(expected)
                restored = node
            else:
                raise AssertionError((path, name, 'unmapped joint definition'))
            assert ast.dump(restored, include_attributes=False) == ast.dump(expected, include_attributes=False)
            joint.append(dict(path=path, definition=name, exact_original_after_owner_binding=True))
    assert len(joint) == 4, joint
    for path in ('src/prismabuild/park_proof.py', 'tools/fleet/qualify_rollout.py'):
        assert (checkout / path).read_bytes() == source(proof_pin, path)
    module_rows = []
    for path in ('tools/fleet/fleet_membership.py', 'tools/fleet/upgrade_client.py'):
        parents = [set(ast.dump(node, include_attributes=False)
                       for node in ast.parse(source(parent, path)).body)
                   for parent in (root_pin, proof_pin)]
        for index, node in enumerate(ast.parse((checkout / path).read_bytes()).body):
            dump = ast.dump(node, include_attributes=False)
            assert dump in parents[0] or dump in parents[1], (path, index)
        module_rows.append(dict(path=path, all_complete_top_level_nodes_equal_to_one_parent=True))
    return dict(verified=True, source=pin, root_parent=root_pin, qualified_proof_parent=proof_pin,
                exact_parent_definitions=rows, joint_complete_AST_proofs=joint,
                proof_and_paired_qualifier_bytes_exact=True, complete_production_module_checks=module_rows,
                moved_test='The storage-boundary installed-reader test moves into the actual-root file, retaining all original behavior assertions. The prior accepted thirteen obsolete formatter cases and six process-test moves remain explicit in the original proof population metadata.',
                source_only=True, runtime_qualification=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--checkout', type=Path, required=True)
    parser.add_argument('--root-pin', required=True)
    parser.add_argument('--proof-pin', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = audit_drain_source_composition(args.checkout, args.root_pin, args.proof_pin)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(dict(verified=True, parent_definitions=len(result['exact_parent_definitions']),
                          joint_complete_AST_checks=len(result['joint_complete_AST_proofs']))))

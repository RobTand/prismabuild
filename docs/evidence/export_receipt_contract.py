#!/usr/bin/env python3
"""Export C15 source bindings, preserved assertions and transport rules; stdlib only."""
import argparse
import ast
from collections import Counter
import hashlib
from io import BytesIO
import json
from pathlib import Path
import subprocess
import tarfile


BASE = 'a962ed3fb2767ce6805110a18124f3cf01a166a3'
LEAVES = {'digest_primitives.py': 'src/prismabuild/digest_primitives.py',
          'generation_receipt.py': 'src/prismabuild/generation_receipt.py'}
POLICIES = {
    'generation_receipt.observe': {
        'order': ['explicit UTF8 receipt read', 'ordinary JSON capture', 'nonempty string commit/generation'],
        'question': 'identity only; no schema, commit format or manifest requirement',
        'failure': 'typed missing/unreadable/malformed/incomplete; compare preserves Unknown'},
    'supervise._published_generation': {
        'order': ['resolve authorized store/direct child', 'explicit UTF8 receipt read/capture',
                  'schema/name/files mapping', '40hex string commit', 'resolve selected supervisor',
                  'allowed supervisor spelling', 'selected digest syntax', 'selected stream hash'],
        'failure': 'None retains old supervisor; no all-member scan'},
    'publish_runtime._sealed_generation': {
        'order': ['name bound', 'authorized canonical sealed root', 'default-encoding receipt read/capture',
                  'schema/name/files mapping', 'string-form40hex commit',
                  'explicit every-member safe path/seal/file/hash'],
        'failure': 'existing SystemExit prefix; empty manifest remains accepted'},
    'publish_runtime._barrier_generation': {
        'order': ['sealed generation', 'selected updater membership', 'read selected source bytes',
                  'bind second read to captured member digest', 'unique literal rollout protocol1'],
        'correction': 'source-bound AST capability correction, separately retained old-source REDs'},
    'upgrade_client.desired': {
        'order': ['resolve enrolled runtime', 'resolve delegated store', 'direct-child/nonstaging authority',
                  'bounded1MiB receipt read', 'lazy local reader closure capture/compile/execute',
                  'ordinary JSON capture', 'schema/name/string-form40hex identity',
                  'select required and present optional members from original files value',
                  'selected literal path', 'bounded4MiB member bytes', 'selected exact hash', 'export hashes'],
        'question': 'does not eagerly require a files dict; original missing/type errors remain',
        'failure': 'original published receipt/member error vocabulary; no staging/drain yet'},
    'upgrade_client.receipt_owner': {
        'order': ['explicit tools/fleet or tools source layout; otherwise trusted installed directory',
                  'root-owned source layout additionally requires trusted source and directory',
                  'both bounded nofollow regular source snapshots with installed uid0/no022',
                  'compile both before executing either', 'execute exact buffers in fresh private package',
                  'remove private module-cache entries'],
        'failure': 'missing/untrusted/invalid source refuses; no package/pyc/legacy fallback'},
    'upgrade_client.Agent._run_once': {
        'order': ['parked-root custody', 'attest actual running updater', 'persist/read rollout hold',
                  'recover existing journal before desired/reader', 'desired export and installed snapshots',
                  'existing current/health path', 'literal durable/rollout/optional/proof/receipt fences',
                  'stage previous/target bytes', 'drain and proof', 'durable journal',
                  'present dependencies', 'present updater', 'absent optionals', 'health/close drain'],
        'authority': 'existing broker scope/token and installed C18 proof capture unchanged'},
    'runtime_publication': {
        'order': ['old regular receipt read point', 'capture schema/name/files + string-form40hex commit',
                  'nonempty canonical nonreserved manifest and digest fields',
                  'old root custody/HMAC checks and store lock', 'explicit complete protected copy'],
        'caches': 'receipt/root/member stamp caches retained at old observations; HMAC outside reader'},
    'resource_scope._sealed_generation_proxy': {
        'order': ['authorized retained store', 'literal direct-child sealed root',
                  'default-encoding receipt read/capture', 'schema/name/files + string-form40hex commit',
                  'selected worker path/member', 'existing package dependencies',
                  'receipt-listed digest/immutable dependencies', 'first present proxy and its closure'],
        'failure': 'present invalid refuses immediately; absent candidate alone permits next spelling',
        'authority': 'actual resource scope and broker token authority unchanged'},
    'submission.verify_retained_wrapper': {
        'order': ['old immutable bounded receipt intake', 'existing strict JSON decoder',
                  'construct capture from decoded value', 'schema/name/files mapping without commit',
                  'selected wrapper member/bytes/hash'],
        'question': 'legacy wrapper receipts omitting commit remain valid'},
    'scalar_adapters': {
        'callers': ['submission.published_commit', 'submission.transport_default',
                    'supervise scalar reporters (exact enclosing symbols in capture_sites)',
                    'fleet_membership', 'pbcanary_watch', 'publish_runtime rollback metadata'],
        'policy': 'capture at existing default/explicit UTF8 read; original defaults/coercions/catches',
        'read_plan': 'submission read-plan gate asks for files only; original source and selected hashes'},
}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def git(root, *words):
    return subprocess.check_output(['git', '-C', str(root), *words])


def dump(node):
    return ast.dump(node, include_attributes=False)


def functions(raw):
    tree = ast.parse(raw)
    return {node.name: {'arguments': dump(node.args), 'returns': dump(node.returns) if node.returns else None,
                        'decorators': [dump(d) for d in node.decorator_list]}
            for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}


class ApprovedAssertionBindings(ast.NodeTransformer):
    def __init__(self, path):
        self.path = path

    def visit_Call(self, node):
        if (self.path == 'tests/test_client_proof_member_bridge.py'
                and dump(node) == dump(ast.parse(
                    '(ROOT / "tools/fleet/upgrade_client.py").read_bytes()', mode='eval').body)):
            return ast.parse('prepared_receipt_bridge()', mode='eval').body
        return self.generic_visit(node)

    def visit_Set(self, node):
        if self.path == 'tests/test_client_proof_current_trust.py':
            node.elts = [item for item in node.elts if not (
                isinstance(item, ast.Constant) and item.value in LEAVES)]
        return self.generic_visit(node)


def preserved_tests(root):
    files = git(root, 'ls-tree', '-r', '--name-only', BASE, 'tests').decode().splitlines()
    reports = []
    for name in files:
        if not name.endswith('.py'):
            continue
        old = git(root, 'show', BASE + ':' + name)
        new = (root / name).read_bytes()
        before, after = ast.parse(old), ast.parse(new)
        normalize = ApprovedAssertionBindings(name)
        before, after = normalize.visit(before), normalize.visit(after)
        original = Counter(dump(n) for n in ast.walk(before) if isinstance(n, ast.Assert))
        current = Counter(dump(n) for n in ast.walk(after) if isinstance(n, ast.Assert))
        assert not original - current, ('original assertion missing', name, original - current)
        old_functions, new_functions = functions(old), functions(new)
        for symbol, signature in old_functions.items():
            if symbol.startswith('test_'):
                assert signature == new_functions.get(symbol), ('native ID/decorator/signature changed', name, symbol)
        reports.append({'path': name, 'original_assertions': sum(original.values()),
                        'current_assertions': sum(current.values()), 'original_functions': old_functions,
                        'all_original_assertion_operations_present': True,
                        'exact_bytes_equal': old == new})
    return reports


def export(root, destination):
    destination.mkdir(parents=True, exist_ok=True)
    bindings, capture_sites = {}, []

    def retain(label, raw):
        identity = sha(raw)
        path = destination / 'source-bytes' / (identity + '.py')
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(raw)
        bindings[label] = {'sha256': identity, 'bytes': len(raw), 'artifact': str(path)}

    changed = git(root, 'diff', '--name-only', BASE).decode().splitlines()
    for name in sorted(set(changed) | set(LEAVES.values())):
        if name.startswith(('src/', 'tools/')) and (root / name).is_file():
            retain('current/' + name, (root / name).read_bytes())
            result = subprocess.run(['git', '-C', str(root), 'show', BASE + ':' + name], capture_output=True)
            if result.returncode == 0:
                retain('base/' + name, result.stdout)
                if name.endswith('.py'):
                    before, after = functions(result.stdout), functions((root / name).read_bytes())
                    assert all(after.get(n) == sig for n, sig in before.items() if not n.startswith('_')), name
            if name.endswith('.py'):
                tree = ast.parse((root / name).read_bytes())
                parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
                for node in ast.walk(tree):
                    if isinstance(node, ast.Call) and ('GenerationReceipt' in ast.unparse(node.func)
                                                       or ast.unparse(node.func) == 'generation_receipt.observe'):
                        owners, parent = [], node
                        while parent in parents:
                            parent = parents[parent]
                            if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                                owners.insert(0, parent.name)
                        capture_sites.append({'path': name, 'line': node.lineno,
                                              'symbol': '.'.join(owners), 'call': ast.unparse(node)})
    metadata = json.loads((root / 'tests/fixtures/receipt-readers-r2.tar.json').read_bytes())
    with tarfile.open(fileobj=BytesIO((root / 'tests/fixtures/receipt-readers-r2.tar.gz').read_bytes()), mode='r:gz') as archive:
        for member in archive.getmembers():
            retain('prepared/' + member.name, archive.extractfile(member).read())
    for name in ('tests/proof_execution.json', 'tests/fixtures/proof-readers.tar.gz'):
        raw = (root / name).read_bytes()
        assert raw == git(root, 'show', BASE + ':' + name), 'frozen C18 source corpus changed'
    assert (root / 'src/prismabuild/core.py').read_bytes() == git(root, 'show', BASE + ':src/prismabuild/core.py')
    report = {
        'schema': 'c15.receipt-contract.v1', 'base': BASE,
        'head': git(root, 'rev-parse', 'HEAD').decode().strip(),
        'branch': git(root, 'branch', '--show-current').decode().strip(), 'checkout': str(root),
        'capture_order_and_exceptions': POLICIES, 'capture_sites': capture_sites, 'source_bindings': bindings,
        'bridge': metadata, 'shipping': {
            name: {'source': path, 'package': path, 'standalone': 'tools/' + name,
                   'installed_client': '/opt/prismabuild-resource-broker/' + name,
                   'installed_movement_publisher': '/opt/prismabuild/' + name,
                   'sha256': sha((root / path).read_bytes())} for name, path in LEAVES.items()},
        'cold_imports': {'generation_receipt': ['stdlib', 'digest_primitives'],
                         'installed_updater': 'stdlib only until lazy receipt_owner after recovery',
                         'prepared_bridge': 'stdlib; never imports either new leaf',
                         'core': 'unchanged attested single-file contract'},
        'direction_predicates': {
            'candidate_closure': 'target intersect {digest_primitives.py,generation_receipt.py} is empty or the complete pair',
            'reader_requirement': 'candidate reader protocol1 implies complete target pair',
            'transaction_requirement': 'union(previous,target) intersects pair implies member protocol1 in both exact updater sources',
            'declaration': 'one unique module-level positive integer literal; ambiguous/invalid source refuses',
            'order': ['present dependencies', 'present updater', 'absent optionals'],
            'required_forward': ['old -> standalone bridge without pair', 'bridge -> bridge with pair', 'bridge+pair -> final+pair'],
            'required_reverse': ['final+pair -> standalone bridge+pair', 'bridge+pair -> standalone bridge without pair'],
            'unsafe_immutable_old': 'old -> final can copy updater while omitting unknown pair; actual negative retained',
            'root_c31_consumers': ['compatibility_window', 'publish_runtime fresh preparation/stage',
                                  'publish_runtime._activate_existing_locked', 'publish_runtime rollout/resume/rollback',
                                  'client enrollment/deployment activation'],
            'root_c31_status': 'Root-owned source transport gate; record parsing alone grants no direction support'},
        'historical_fixture_substitution': {
            'artifact': metadata['bridge_sha256'], 'role': 'standalone proof-only bridge',
            'paths': ['tests/client_proof_bridge_fixture.py', 'tests/test_client_proof_member_bridge.py',
                      'tests/test_client_upgrade.py'],
            'authority': 'explicit Root source/fixture approval in task messages',
            'current_root_roles': 'bind_current_receipt_reader selects current updater and both current leaves',
            'unchanged': 'frozen C18 corpus/goldens, original IDs/decorators/assertion operations'},
        'tests': preserved_tests(root), 'no_core_change': True, 'no_publication': True,
        'live_and_designated_rollback': 'a17477b2b206adf79d89e65c7b9d09584f0bfa34',
    }
    (destination / 'receipt-contract.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'export': str(destination / 'receipt-contract.json'), 'bindings': len(bindings),
                      'original_test_files': len(report['tests']), 'all_original_assertions_present': True}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--checkout', type=Path, default=Path.cwd())
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    export(args.checkout.resolve(), args.output.resolve())

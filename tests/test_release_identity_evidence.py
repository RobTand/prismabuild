"""Issue 1828: computed release evidence, not an operator's commit assertions.

The coordinator API is exposed by installed_bootstrap; implementations may
re-export small domain owners there. Collection is injected at the admitted-I/O
boundary, like CAS I/O in test_bootstrap_qualification. No release code, Git
comparison, source pinning or index publication is mocked.
"""
from __future__ import annotations

from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import subprocess

import pytest

from prismabuild import core, installed_bootstrap as bootstrap, submission
from prismabuild.compatibility_window import WindowError
from prismabuild.digest_primitives import raw_sha256
from prismabuild.generation_receipt import RUNTIME_SCHEMA

REPO = Path(__file__).resolve().parents[1]


def entry(name):
    function = getattr(bootstrap, name, None)
    assert callable(function), (
        'release evidence is still manual: installed_bootstrap must expose ' + name)
    return function


def git(checkout, *args, input=None):
    return subprocess.run(['git', '-C', str(checkout), *args], input=input,
        text=True, capture_output=True, check=True).stdout.strip()


def put(checkout, name, content):
    path = checkout / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def commit(checkout, name, content):
    put(checkout, name, content)
    git(checkout, 'add', '--', name)
    git(checkout, 'commit', '-qm', name)
    return git(checkout, 'rev-parse', 'HEAD')


@pytest.fixture
def release(tmp_path, monkeypatch):
    checkout = tmp_path / 'checkout'
    checkout.mkdir()
    git(checkout, 'init', '-q', '-b', 'main')
    git(checkout, 'config', 'user.email', 'test@example.invalid')
    git(checkout, 'config', 'user.name', 'Release evidence test')
    # Files beyond an incidental src/tools shortlist also ship in a generation.
    for name in (*bootstrap.QUALIFICATION_SOURCES, 'src/prismabuild/core.py',
                 'src/prismabuild/generation_receipt.py', 'src/prismabuild/digest_primitives.py',
                 'tools/fleet/pbrun.py', 'docs/runtime-contract.json',
                 '.githooks/pre-push', 'fleet/observability/pbmetrics.service'):
        put(checkout, name, '# fixture ' + name + '\n' if name.endswith('.py') else '{}\n')
    profiles = {}
    for machine, elf in [('aarch64', 183), ('x86_64', 62)]:
        path = 'profiles/' + machine + '.json'
        primary = {'version': [3, 12, 3], 'elf_machine': elf,
            'executable': {'path': '/configured/python', 'sha256': 'a' * 64}}
        put(checkout, path, json.dumps({'primary': primary,
            'installed_reader': primary, 'image': 'sha256:' + 'b' * 64}))
        profiles[machine] = path
    git(checkout, 'add', '.')
    git(checkout, 'commit', '-qm', 'requested release M')
    requested = git(checkout, 'rev-parse', 'HEAD')
    spec = importlib.util.spec_from_file_location('_release_test_publisher',
        REPO / 'tools/fleet/publish_runtime.py')
    publisher = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(publisher)
    monkeypatch.setattr(publisher, 'CHECKOUT', checkout)
    yield checkout, requested, publisher, profiles
    # Sealed fixture generations must not leave unwritable pytest temp trees.
    for directory in (p for p in tmp_path.rglob('*') if p.is_dir()):
        directory.chmod(0o755)


def merge_change(checkout, name, content):
    """A genuine later --no-ff merge, not a fabricated ancestry answer."""
    git(checkout, 'checkout', '-qb', 'later')
    changed = commit(checkout, name, content)
    git(checkout, 'checkout', '-q', 'main')
    git(checkout, 'merge', '-q', '--no-ff', 'later', '-m', 'later merge')
    return changed, git(checkout, 'rev-parse', 'HEAD')


def snapshot(release, tmp_path):
    checkout, requested, _, _ = release
    cas = core.PrismaBuildCAS(tmp_path / 'cas')
    payload = json.dumps({'schema': core.CODE_CLOSURE_SCHEMA_V1, 'files': []})
    sealed = submission.snapshot_checkout(checkout, cas=cas,
        stamp_name='.pbrun-closure.0123456789abcdef.json', stamp_payload=payload,
        max_bytes=16 * 1024 * 1024)
    git(checkout, 'bundle', 'unbundle', str(cas.input_path(sealed['input'])))
    assert sealed['parent'] == requested
    assert git(checkout, 'rev-parse', sealed['commit'] + '^') == requested
    return sealed['commit']


def generation(release, tmp_path, snapshot_commit, *, name='staged'):
    checkout, _, publisher, _ = release
    root = tmp_path / name
    root.mkdir()
    manifest = publisher._publication_manifest()
    for member in manifest:
        source = publisher._source_for(member)
        target = root / member
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
        target.chmod(0o444)
    raw = json.dumps({'schema': RUNTIME_SCHEMA, 'generation': name,
        'commit': snapshot_commit, 'dirty': False, 'files': manifest}).encode()
    (root / 'RUNTIME_VERSION.json').write_bytes(raw)
    (root / 'RUNTIME_VERSION.json').chmod(0o444)
    # Match publication custody, including read-only directories.
    for directory in sorted((p for p in root.rglob('*') if p.is_dir()), reverse=True):
        directory.chmod(0o555)
    root.chmod(0o555)
    return root


def test_release_source_accepts_later_ci_only_first_parent_merge(release):
    checkout, requested, _, _ = release
    _, main = merge_change(checkout, '.github/workflows/ci.yml', 'name: later CI\n')
    evidence = entry('release_source_identity')(checkout=checkout,
        requested_commit=requested, main_ref='refs/heads/main')
    assert evidence['requested_commit'] == requested
    assert evidence['main_commit'] == main


def test_release_source_rejects_merged_side_parent_even_with_identical_runtime(release):
    checkout, _, _, _ = release
    side, _ = merge_change(checkout, '.github/workflows/ci.yml', 'name: CI\n')
    require = entry('release_source_identity')
    with pytest.raises(WindowError, match='first.parent'):
        require(checkout=checkout, requested_commit=side, main_ref='refs/heads/main')


@pytest.mark.parametrize('name', [
    'src/prismabuild/core.py', 'src/prismabuild/new_runtime.py',
    'tools/fleet/pbrun.py', 'docs/runtime-contract.json', '.githooks/pre-push',
    'tests/test_client_bootstrap_root.py',
])
def test_later_runtime_merge_is_classified_by_publication_contract(release, name):
    checkout, requested, publisher, _ = release
    merge_change(checkout, name, '# changed publication member\n')
    # Establish why this path is runtime-relevant using the real publisher.
    sources = {publisher._source_for(member).relative_to(checkout).as_posix()
               for member in publisher._publication_manifest()}
    assert name in sources
    require = entry('release_source_identity')
    with pytest.raises(WindowError, match='runtime|publication'):
        require(checkout=checkout, requested_commit=requested, main_ref='refs/heads/main')


@pytest.mark.parametrize('damage', ['delete', 'mode', 'rename'])
def test_release_source_cannot_hide_old_published_paths(release, damage):
    checkout, requested, _, _ = release
    name = 'docs/runtime-contract.json'
    git(checkout, 'checkout', '-qb', 'later')
    if damage == 'delete':
        git(checkout, 'rm', name)
    elif damage == 'mode':
        (checkout / name).chmod(0o755)
        git(checkout, 'update-index', '--chmod=+x', name)
    else:
        git(checkout, 'mv', name, 'not-published.txt')
    git(checkout, 'commit', '-qm', damage)
    git(checkout, 'checkout', '-q', 'main')
    git(checkout, 'merge', '-q', '--no-ff', 'later', '-m', damage)
    require = entry('release_source_identity')
    with pytest.raises(WindowError, match='runtime|publication'):
        require(checkout=checkout, requested_commit=requested, main_ref='refs/heads/main')


def test_snapshot_identity_uses_recorded_snapshot_not_executing_head(release, tmp_path):
    checkout, requested, _, _ = release
    sealed = snapshot(release, tmp_path)
    staged = generation(release, tmp_path, sealed)
    # The inspecting checkout is on a later CI commit; neither HEAD nor M is S.
    commit(checkout, '.github/workflows/ci.yml', 'name: newer CI\n')
    evidence = entry('release_snapshot_identity')(checkout=checkout,
        requested_commit=requested, generation=staged)
    assert evidence['requested_commit'] == requested
    assert evidence['snapshot_commit'] == sealed
    assert sealed != requested
    assert evidence['receipt_sha256'] == raw_sha256((staged / 'RUNTIME_VERSION.json').read_bytes())


@pytest.mark.parametrize('damage', ['wrong-parent', 'merge-parent', 'runtime',
    'nonruntime', 'nested-stamp', 'mode', 'receipt-member'])
def test_snapshot_identity_refuses_wrong_ancestry_or_nonclosure_content(release, tmp_path, damage):
    checkout, requested, _, _ = release
    sealed = snapshot(release, tmp_path)
    tree = git(checkout, 'rev-parse', sealed + '^{tree}')
    if damage == 'wrong-parent':
        other = git(checkout, 'commit-tree', requested + '^{tree}', '-p', requested,
                    input='other parent\n')
        sealed = git(checkout, 'commit-tree', tree, '-p', other, input='wrong parent\n')
    elif damage == 'merge-parent':
        other = git(checkout, 'commit-tree', requested + '^{tree}', '-p', requested,
                    input='second parent\n')
        sealed = git(checkout, 'commit-tree', tree, '-p', requested, '-p', other,
                    input='multiple parents\n')
    elif damage != 'receipt-member':
        git(checkout, 'checkout', '-q', '--detach', sealed)
        if damage == 'mode':
            git(checkout, 'update-index', '--chmod=+x', 'src/prismabuild/core.py')
        else:
            path = {'runtime': 'src/prismabuild/core.py',
                    'nonruntime': '.github/new-ci.yml',
                    'nested-stamp': 'nested/.pbrun-closure.abc.json'}[damage]
            put(checkout, path, '# not the permitted closure addition\n')
            git(checkout, 'add', '--', path)
        tree = git(checkout, 'write-tree')
        sealed = git(checkout, 'commit-tree', tree, '-p', requested, input='changed snapshot\n')
        git(checkout, 'checkout', '-qf', 'main')
    staged = generation(release, tmp_path, sealed)
    if damage == 'receipt-member':
        member = staged / 'src/prismabuild/core.py'
        member.chmod(0o644)
        member.write_text('# tampered after receipt\n')
        member.chmod(0o444)
    require = entry('release_snapshot_identity')
    with pytest.raises(WindowError):
        require(checkout=checkout, requested_commit=requested, generation=staged)


def make_index(release, tmp_path):
    checkout, requested, _, profiles = release
    calls = []
    def collect(**request):
        # This stands for independently admitted collection, NOT execution logs.
        calls.append(deepcopy(request))
        return [name + '::test_case[' + case + ']'
                for name in request['files'] for case in ('one', 'two')]
    routes = {'x86_64': {'commands': [
        ['/configured/python', '-m', 'pytest', *sorted(bootstrap.CORPUS_FILES)]]}}
    path = tmp_path / 'before-execution.json'
    entry('generate_qualification_index')(checkout=checkout, target_commit=requested,
        root_profiles=profiles, corpus_routes=routes, collect=collect, out=path)
    return path, calls, routes


def test_index_is_generated_before_execution_from_complete_collection_and_source_pins(release, tmp_path):
    checkout, requested, _, profiles = release
    path, calls, routes = make_index(release, tmp_path)
    index = json.loads(path.read_bytes())
    assert index['schema'] == 'prismabuild.bootstrap_qualification_index.v1'
    assert index['target_commit'] == requested
    assert not path.stat().st_mode & 0o222
    assert {(c['kind'], c['machine']) for c in calls} == {
        ('root', 'aarch64'), ('root', 'x86_64'), ('corpus', 'x86_64')}
    for kind, files in [('root', bootstrap.ROOT_FILES), ('corpus', bootstrap.CORPUS_FILES)]:
        expected = {name + '::test_case[' + case + ']'
                    for name in files for case in ('one', 'two')}
        assert set(index['populations'][kind]['files']) == set(files)
        assert set(index['populations'][kind]['nodes']) == expected
    assert index['corpus_routes'] == routes
    for machine, profile in profiles.items():
        digest = raw_sha256((checkout / profile).read_bytes())
        assert index['root_routes'][machine] == {'profile': profile, 'profile_sha256': digest}
    for name in (*bootstrap.QUALIFICATION_SOURCES, *profiles.values()):
        assert index['source_sha256'][name] == raw_sha256((checkout / name).read_bytes())
    # The generated evidence is consumable by the EXISTING strict verifier.
    pin = {'path': str(path), 'sha256': raw_sha256(path.read_bytes())}
    assessment = {'qualification_index': pin, 'plan': {'target': {'commit': requested},
        'qualification_index_sha256': pin['sha256']}}
    assert bootstrap._qualification_index(assessment, checkout) == index


def test_rebind_creates_new_sealed_index_preserving_preexecution_provenance(release, tmp_path):
    checkout, requested, _, _ = release
    original, _, _ = make_index(release, tmp_path)
    before = original.read_bytes()
    sealed = snapshot(release, tmp_path)
    staged = generation(release, tmp_path, sealed)
    output = tmp_path / 'snapshot-index.json'
    entry('rebind_qualification_index')(checkout=checkout, requested_commit=requested,
        generation=staged, index_path=original, out=output)
    rebound = json.loads(output.read_bytes())
    initial = json.loads(before)
    assert rebound['target_commit'] == sealed
    assert rebound['rebound_from'] == {'target_commit': requested, 'sha256': raw_sha256(before)}
    for field in ('populations', 'root_routes', 'corpus_routes', 'source_sha256'):
        assert rebound[field] == initial[field]
    assert original.read_bytes() == before
    assert not output.stat().st_mode & 0o222
    pin = {'path': str(output), 'sha256': raw_sha256(output.read_bytes())}
    assessment = {'qualification_index': pin, 'plan': {'target': {'commit': sealed},
        'qualification_index_sha256': pin['sha256']}}
    assert bootstrap._qualification_index(assessment, checkout) == rebound
    # Never "fix" installed_bootstrap by accepting an index bound merely to M.
    assessment['qualification_index'] = {'path': str(original), 'sha256': raw_sha256(before)}
    assessment['plan']['qualification_index_sha256'] = raw_sha256(before)
    with pytest.raises(WindowError, match='source/routes'):
        bootstrap._qualification_index(assessment, checkout)


@pytest.mark.parametrize('damage', ['source-change', 'overwrite'])
def test_rebind_refuses_nonclosure_change_and_existing_destination(release, tmp_path, damage):
    checkout, requested, _, _ = release
    original, _, _ = make_index(release, tmp_path)
    before = original.read_bytes()
    if damage == 'source-change':
        put(checkout, '.github/unrelated.yml', 'name: still a nonclosure source change\n')
    sealed = snapshot(release, tmp_path)
    staged = generation(release, tmp_path, sealed)
    output = tmp_path / 'snapshot-index.json'
    if damage == 'overwrite':
        output.write_bytes(b'conflicting immutable evidence\n')
        output.chmod(0o444)
    rebind = entry('rebind_qualification_index')
    with pytest.raises(WindowError):
        rebind(checkout=checkout, requested_commit=requested,
            generation=staged, index_path=original, out=output)
    assert original.read_bytes() == before
    if damage == 'overwrite':
        assert output.read_bytes() == b'conflicting immutable evidence\n'
    else:
        assert not output.exists()


def test_constructed_release_argv_is_portable_without_relaxing_pbrun(release, tmp_path):
    checkout, _, _, _ = release
    external = tmp_path / 'external-evidence'
    argv = ['/configured/python', 'tests/compat/support/compat_sources.py', 'capture',
        '--git-root', str(checkout), '--out', str(external),
        '--designation', str(checkout / 'docs/designation.json')]
    with pytest.raises(submission.SubmissionRefused, match='submitter repository path'):
        submission.require_relocatable_checkout(argv, {}, checkout)
    portable = entry('portable_release_command')(checkout=checkout, command=argv)
    assert portable == ['/configured/python', 'tests/compat/support/compat_sources.py', 'capture',
        '--git-root', '.', '--out', str(external), '--designation', 'docs/designation.json']
    submission.require_relocatable_checkout(portable, {}, checkout)
    # Building a release command must not globally turn off the old refusal.
    with pytest.raises(submission.SubmissionRefused, match='submitter repository path'):
        submission.require_relocatable_checkout(argv, {}, checkout)

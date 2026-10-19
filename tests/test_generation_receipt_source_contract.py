"""Fixed old-source controls distinguish extraction from capability corrections."""
import importlib.util
import json
import os
from pathlib import Path
import sys

import pytest

from prismabuild.digest_primitives import raw_sha256
from receipt_reader_fixture import ROOT, receipt_source_artifact, prepared_receipt_bridge


def program(relative, historical):
    source = receipt_source_artifact('base/' + relative) if historical else (ROOT / relative).read_bytes()
    name = '_receipt_contract_' + raw_sha256(source)
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        exec(compile(source, str(ROOT / relative), 'exec'), module.__dict__)
    finally:
        sys.modules.pop(name, None)
    return module


def sealed_updater(tmp_path, source):
    root = tmp_path / 'runtime-generations' / 'g'
    path = root / 'tools/upgrade_client.py'
    path.parent.mkdir(parents=True)
    path.write_bytes(source)
    (root / 'RUNTIME_VERSION.json').write_text(json.dumps({
        'schema': 'prismaquant.prismabuild.runtime_version.v1',
        'generation': 'g', 'commit': 'a' * 40,
        'files': {'tools/upgrade_client.py': raw_sha256(source)}}))
    for member in root.rglob('*'):
        member.chmod(0o555 if member.is_dir() else 0o444)
    root.chmod(0o555)
    return root


@pytest.mark.parametrize('source', [
    b'CLIENT_UPGRADE_ROLLOUT_PROTOCOL = 1\nCLIENT_UPGRADE_ROLLOUT_PROTOCOL = 2\n',
    b'TEXT = """\nCLIENT_UPGRADE_ROLLOUT_PROTOCOL = 1\n"""\n',
    b'CLIENT_UPGRADE_ROLLOUT_PROTOCOL = 1\ndef CLIENT_UPGRADE_ROLLOUT_PROTOCOL(): pass\n',
    b'CLIENT_UPGRADE_ROLLOUT_PROTOCOL = 1\ndef f():\n CLIENT_UPGRADE_ROLLOUT_PROTOCOL = 2\n',
])
def test_selected_updater_cannot_establish_support_from_ambiguous_source(tmp_path, monkeypatch, source):
    historical = os.environ.get('C15_OLD_SOURCE_NEGATIVE') == '1'
    publisher = program('tools/fleet/publish_runtime.py', historical)
    sealed_updater(tmp_path, source)
    monkeypatch.setattr(publisher, 'MIRROR', tmp_path / 'repo')
    with pytest.raises(SystemExit):
        publisher._barrier_generation('g')


def test_an_inline_comment_keeps_a_unique_literal_declaration(tmp_path, monkeypatch):
    source = b'CLIENT_UPGRADE_ROLLOUT_PROTOCOL = 1 # reviewed declaration\n'
    root = sealed_updater(tmp_path, source)
    old = program('tools/fleet/publish_runtime.py', True)
    monkeypatch.setattr(old, 'MIRROR', tmp_path / 'repo')
    with pytest.raises(SystemExit, match='rolling bridge'):
        old._barrier_generation('g')
    current = program('tools/fleet/publish_runtime.py', False)
    monkeypatch.setattr(current, 'MIRROR', tmp_path / 'repo')
    assert current._barrier_generation('g')[0] == root


def test_declaration_bytes_are_bound_to_the_selected_receipt(tmp_path, monkeypatch):
    source = b'CLIENT_UPGRADE_ROLLOUT_PROTOCOL = 1\n'
    root = sealed_updater(tmp_path, source)
    historical = os.environ.get('C15_OLD_SOURCE_NEGATIVE') == '1'
    publisher = program('tools/fleet/publish_runtime.py', historical)
    monkeypatch.setattr(publisher, 'MIRROR', tmp_path / 'repo')
    selected = publisher._sealed_generation

    def changed(name):
        captured = selected(name)
        path = root / 'tools/upgrade_client.py'
        path.chmod(0o644)
        path.write_bytes(source + b'# changed after selection\n')
        path.chmod(0o444)
        return captured

    monkeypatch.setattr(publisher, '_sealed_generation', changed)
    with pytest.raises(SystemExit, match='source differs from its receipt'):
        publisher._barrier_generation('g')


@pytest.mark.parametrize('change', [
    'valid', 'numeric-commit', 'missing-files', 'files-list', 'files-null',
    'wrong-schema', 'wrong-generation', 'bad-commit', 'null', 'list',
    'duplicate-commit', 'noncanonical-unused-member', 'invalid-unused-digest',
])
def test_updater_extraction_keeps_original_acceptance_values_and_errors(tmp_path, change):
    root = tmp_path / 'generations' / 'g'
    old = program('tools/fleet/upgrade_client.py', True)
    current = program('tools/fleet/upgrade_client.py', False)
    files = {}
    for member in old.MEMBERS.values():
        path = root / member
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'# selected bytes\n')
        files[member] = raw_sha256(path.read_bytes())
    value = {'schema': 'prismaquant.prismabuild.runtime_version.v1',
             'generation': 'g', 'commit': 'a' * 40, 'files': files}
    if change == 'numeric-commit': value['commit'] = int('1' * 40)
    if change == 'missing-files': value.pop('files')
    if change == 'files-list': value['files'] = []
    if change == 'files-null': value['files'] = None
    if change == 'wrong-schema': value['schema'] = 'wrong'
    if change == 'wrong-generation': value['generation'] = 'wrong'
    if change == 'bad-commit': value['commit'] = 'short'
    if change == 'null': value = None
    if change == 'list': value = []
    if change == 'noncanonical-unused-member': files['../unused'] = 'b' * 64
    if change == 'invalid-unused-digest': files['unused'] = False
    raw = json.dumps(value)
    if change == 'duplicate-commit':
        raw = raw.replace('"commit":', '"commit":"overridden", "commit":', 1)
    (root / 'RUNTIME_VERSION.json').write_text(raw)
    config = dict(runtime=str(root), generation_store=str(root.parent))

    def observe(module):
        try:
            return ('accepted', module.desired(config))
        except Exception as error:
            return (type(error).__name__, str(error))

    assert observe(current) == observe(old)


@pytest.mark.parametrize('source', [b'P = 1\nP = 2\n', b'P = True\n', b'P = None\n',
    b'P = 1\ndef P(): pass\n', b'if True:\n P = 1\n', b'P = 1\nimport other as P\n'])
def test_standalone_bridge_uses_its_own_unique_literal_reader(source):
    raw = prepared_receipt_bridge()
    name = '_receipt_standalone_bridge_control'
    spec = importlib.util.spec_from_file_location(name, ROOT / 'tools/fleet/upgrade_client.py')
    module = importlib.util.module_from_spec(spec)
    exec(compile(raw, str(ROOT / 'tools/fleet/upgrade_client.py'), 'exec'), module.__dict__)
    with pytest.raises((ValueError, SyntaxError)):
        module.bridge_protocol(source, 'P')

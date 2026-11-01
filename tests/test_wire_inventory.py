"""Pin actual closed boundaries and find new writers outside the catalogue."""
from __future__ import annotations

import json
import hashlib
import io
from pathlib import Path
import subprocess
import sys
import tarfile

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
from wire_inventory import audit, field_policies, inventory_metadata, source_paths, validated_archive
from rebind_source_reviews import reviewed_sites


def test_repository_catalogue_and_dispositions_are_consistent():
    inventory_metadata(ROOT)


@pytest.mark.parametrize('archive', sorted((ROOT / 'tests/compat/generations').glob('*.tar.gz')),
                         ids=lambda path: path.name)
def test_retained_archive_is_intact(archive):
    validated_archive(archive)


@pytest.mark.parametrize('corruption', ['duplicate', 'overlap', 'empty_reason', 'unknown_disposition', 'catalogue_alias'])
def test_repository_metadata_guard_rejects_corruption(tmp_path, monkeypatch, corruption):
    package = tmp_path / 'src/prismabuild'
    package.mkdir(parents=True)
    catalogue_source = 'RecordKind(name="qualified", wire_id="example.qualified.v1")\n'
    (package / 'record_kinds.py').write_text(catalogue_source)
    metadata = tmp_path / 'tests/compat/dispositions.jsonl'
    metadata.parent.mkdir(parents=True)
    row = dict(wire_id='example.control.v1', disposition='operator_control', reason='stdout only',
               witnesses={key: [] for key in ('occurrences', 'schema_builders', 'reader_checks')})
    metadata.write_text(json.dumps(row) + '\n')
    monkeypatch.setitem(globals(), 'ROOT', tmp_path)
    test_repository_catalogue_and_dispositions_are_consistent()
    if corruption == 'duplicate':
        metadata.write_text(metadata.read_text() * 2)
    elif corruption == 'catalogue_alias':
        (package / 'record_kinds.py').write_text(catalogue_source * 2)
    else:
        row[{'overlap': 'wire_id', 'empty_reason': 'reason', 'unknown_disposition': 'disposition'}[corruption]] = {
            'overlap': 'example.qualified.v1', 'empty_reason': '', 'unknown_disposition': 'unreviewed'}[corruption]
        metadata.write_text(json.dumps(row) + '\n')
    with pytest.raises((AssertionError, ValueError)):
        test_repository_catalogue_and_dispositions_are_consistent()


@pytest.mark.parametrize('corruption', ['archive_hash', 'member_hash', 'receipt_hash', 'missing_member',
                                      'unexpected_member', 'duplicate_member', 'unsafe_path', 'non_file'])
def test_archive_guard_rejects_corruption(tmp_path, corruption):
    archive = tmp_path / 'generation.tar.gz'
    receipt = b'{"generation": "fixture"}'
    payload = b'VALUE = 1\n'
    members = [('src/prismabuild/example.py', payload), ('RUNTIME_VERSION.json', receipt)]
    manifest = dict(generation='fixture', archive_sha256='',
                    receipt_sha256=hashlib.sha256(receipt).hexdigest(),
                    files={'src/prismabuild/example.py': hashlib.sha256(payload).hexdigest()})

    def write_archive():
        with tarfile.open(archive, 'w:gz') as stream:
            for name, raw in members:
                entry = tarfile.TarInfo(name)
                entry.size = len(raw)
                if corruption == 'non_file' and name == 'link':
                    entry.type = tarfile.SYMTYPE
                    entry.linkname = 'src/prismabuild/example.py'
                stream.addfile(entry, io.BytesIO(raw))
        manifest['archive_sha256'] = hashlib.sha256(archive.read_bytes()).hexdigest()
        archive.with_suffix('').with_suffix('.json').write_text(json.dumps(manifest))

    write_archive()
    test_retained_archive_is_intact(archive)
    if corruption in ('archive_hash', 'member_hash', 'receipt_hash'):
        if corruption == 'archive_hash':
            manifest['archive_sha256'] = '0' * 64
        elif corruption == 'member_hash':
            manifest['files']['src/prismabuild/example.py'] = '0' * 64
        else:
            manifest['receipt_sha256'] = '0' * 64
        archive.with_suffix('').with_suffix('.json').write_text(json.dumps(manifest))
    else:
        if corruption == 'missing_member':
            members.pop(0)
        else:
            name, raw = {'unexpected_member': ('extra', b'new'), 'duplicate_member': members[0],
                         'unsafe_path': ('../escape', b'bad'), 'non_file': ('link', payload)}[corruption]
            members.append((name, raw))
            if corruption in ('unsafe_path', 'non_file'):
                manifest['files'][name] = hashlib.sha256(raw).hexdigest()
        write_archive()
    with pytest.raises(AssertionError):
        test_retained_archive_is_intact(archive)


def _rejected(check):
    try:
        check()
    except (AssertionError, ValueError, KeyError, StopIteration, TypeError):
        return True
    return False


@pytest.mark.parametrize('corruption', ['generation', 'generation_list', 'capture_json', 'current_case',
                                      'historical_generation', 'unknown_absence', 'ineligible_absence',
                                      'witness_occurrences', 'witness_schema_builders', 'witness_reader_checks'])
def test_metadata_guard_covers_audit_documents(tmp_path, monkeypatch, corruption):
    package = tmp_path / 'src/prismabuild'
    package.mkdir(parents=True)
    declaration = 'RecordKind(name="qualified", wire_id="example.qualified.v1", older_participant_absence=True)\n'
    catalogue = package / 'record_kinds.py'
    catalogue.write_text(declaration)
    compat = tmp_path / 'tests/compat'
    generations = compat / 'generations'
    generations.mkdir(parents=True)
    descriptor = generations / 'window.json'
    descriptor.write_text('{"generations": [{"generation": "fixture"}]}')
    current = compat / 'goldens.jsonl'
    current.write_text('{"case": "qualified/fixture"}\n')
    historical = compat / 'historical.jsonl'
    historical.write_text('{"case": "qualified/fixture", "generation": "fixture"}\n')
    absent = compat / 'absent-inputs.jsonl'
    absent.write_text('{"case": "qualified/fixture", "generation": "fixture"}\n')
    disposition = dict(wire_id='example.control.v1', reason='stdout only', disposition='operator_control',
                       witnesses={key: [] for key in ('occurrences', 'schema_builders', 'reader_checks')})
    reviewed = compat / 'dispositions.jsonl'
    reviewed.write_text(json.dumps(disposition) + '\n')
    monkeypatch.setitem(globals(), 'ROOT', tmp_path)
    test_repository_catalogue_and_dispositions_are_consistent()
    if corruption == 'generation':
        descriptor.write_text('{"generations": [{}]}')
    elif corruption == 'generation_list':
        descriptor.write_text('{}')
    elif corruption == 'capture_json':
        current.write_text('{broken\n')
    elif corruption == 'current_case':
        current.write_text('{}\n')
    elif corruption == 'historical_generation':
        historical.write_text('{"case": "qualified/fixture"}\n')
    elif corruption == 'unknown_absence':
        absent.write_text('{"case": "unknown/fixture", "generation": "fixture"}\n')
    elif corruption == 'ineligible_absence':
        catalogue.write_text(declaration.replace('True', 'False'))
    else:
        del disposition['witnesses'][corruption.removeprefix('witness_')]
        reviewed.write_text(json.dumps(disposition) + '\n')
    assert _rejected(test_repository_catalogue_and_dispositions_are_consistent), corruption


def _source_audit(root):
    # Red stub for the bounded source-index validator.
    return None


@pytest.fixture(scope='module')
def repository_source_index():
    return _source_audit(ROOT)


@pytest.mark.parametrize('path', list(source_paths(ROOT)))
def test_source_file_preserves_audit_assertions(path, repository_source_index):
    # Red stub: source assertions currently run only for production pin cases.
    pass


@pytest.mark.parametrize('wire', ['prismaquant.prismabuild.action.v1', 'buffered-sequential-write-fdatasync-read.v1',
                                'contiguous-residency-exact-cover.v1', 'repeated-shake256-block.v1'])
@pytest.mark.parametrize('form', ['literal', 'imported', 'embedded'])
def test_source_guard_rejects_forbidden_constructors(tmp_path, monkeypatch, wire, form):
    package = tmp_path / 'src/prismabuild'
    package.mkdir(parents=True)
    (package / 'owner.py').write_text('SCHEMA = ' + repr(wire) + '\n')
    source = 'def write():\n    return {"schema": ' + repr(wire) + '}\n'
    if form == 'imported':
        source = 'from .owner import SCHEMA\ndef write():\n    return dict(schema=SCHEMA)\n'
    elif form == 'embedded':
        source = 'SCRIPT = ' + repr(source) + '\n'
    (package / 'builder.py').write_text(source)
    monkeypatch.setitem(globals(), 'ROOT', tmp_path)
    assert _rejected(lambda: test_source_file_preserves_audit_assertions(
        'src/prismabuild/builder.py', _source_audit(tmp_path))), (wire, form)


def test_source_guard_rejects_changed_test_source(tmp_path, monkeypatch):
    source = tmp_path / 'tests/test_fixture.py'
    source.parent.mkdir()
    source.write_text('VALUE = 1\n')
    index = _source_audit(tmp_path)
    monkeypatch.setitem(globals(), 'ROOT', tmp_path)
    test_source_file_preserves_audit_assertions('tests/test_fixture.py', index)
    source.write_text('VALUE = 2\n')
    assert _rejected(lambda: test_source_file_preserves_audit_assertions('tests/test_fixture.py', index))


def _write_source_archive(root, files):
    archive = root / 'fixture.tar.gz'
    receipt = b'{}'
    with tarfile.open(archive, 'w:gz') as contents:
        for name, raw in {**files, 'RUNTIME_VERSION.json': receipt}.items():
            member = tarfile.TarInfo(name)
            member.size = len(raw)
            contents.addfile(member, io.BytesIO(raw))
    manifest = dict(generation='fixture', archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
                    receipt_sha256=hashlib.sha256(receipt).hexdigest(),
                    files={name: hashlib.sha256(raw).hexdigest() for name, raw in files.items()})
    archive.with_suffix('').with_suffix('.json').write_text(json.dumps(manifest))
    return archive


@pytest.fixture(scope='module')
def retained_source_indexes(tmp_path_factory):
    # Red stub: retained contents are currently checked only for byte integrity.
    return lambda archive: None


def _retained_source_paths():
    for archive in sorted((ROOT / 'tests/compat/generations').glob('*.tar.gz')):
        manifest = json.loads(archive.with_suffix('').with_suffix('.json').read_bytes())
        for path in sorted(manifest['files']):
            if path.split('/')[0] in ('src', 'tools', 'tests', 'fleet') and Path(path).suffix in ('.py', '', '.sh'):
                yield archive, path


@pytest.mark.parametrize('archive,path', list(_retained_source_paths()),
                         ids=lambda value: value.name if isinstance(value, Path) else value)
def test_retained_source_preserves_audit_assertions(archive, path, retained_source_indexes):
    pass


@pytest.mark.parametrize('wire', ['prismaquant.prismabuild.action.v1', 'buffered-sequential-write-fdatasync-read.v1',
                                'contiguous-residency-exact-cover.v1', 'repeated-shake256-block.v1'])
def test_retained_source_guard_rejects_forbidden_constructors(tmp_path, retained_source_indexes, wire):
    path = 'src/prismabuild/builder.py'
    archive = _write_source_archive(tmp_path, {path: ('def write():\n    return {"schema": ' + repr(wire) + '}\n').encode()})
    test_retained_archive_is_intact(archive)
    assert _rejected(lambda: test_retained_source_preserves_audit_assertions(archive, path, retained_source_indexes))


def test_retained_source_guard_rejects_changes_after_indexing(tmp_path, retained_source_indexes):
    path = 'src/prismabuild/builder.py'
    archive = _write_source_archive(tmp_path, {path: b'VALUE = 1\n'})
    loaded = retained_source_indexes(archive)
    if loaded is not None:
        root, _ = loaded
        (root / path).write_text('VALUE = 2\n')
    assert _rejected(lambda: test_retained_source_preserves_audit_assertions(archive, path, retained_source_indexes))


def _field_policy_paths(root):
    pinned = json.loads((root / "tests/compat/field_sets.json").read_bytes())
    return sorted(set(source_paths(root, include_tests=False)) | {row['path'] for row in pinned})


@pytest.mark.parametrize('path', _field_policy_paths(ROOT))
def test_closed_field_sets_remain_pinned(path):
    policies = field_policies(ROOT, paths=[path])
    expected = json.loads((ROOT / "tests/compat/field_sets.json").read_bytes())
    if expected and path == expected[0]['path']:
        assert expected == sorted(expected, key=lambda row: (row['path'], row['owner'], json.dumps(row, sort_keys=True)))
    assert policies == [row for row in expected if row['path'] == path]


def test_symbolic_fstring_policies_retain_selected_source_bytes(tmp_path, monkeypatch):
    import ast
    original = ast.unparse

    def version_specific_unparse(node):
        if any(isinstance(part, ast.JoinedStr) for part in ast.walk(node)):
            raise AssertionError('interpolated source must not depend on interpreter rendering')
        return original(node)

    monkeypatch.setattr(ast, 'unparse', version_specific_unparse)
    expression = '''{'schema', value['host'], f"runtime-generation:{value['generation']}"}'''
    source = 'def check(value):\n    required = ' + expression + '\n    if set(value) != required: raise ValueError("fields")\n'
    for name in ('direct', 'embedded'):
        root = tmp_path / name
        package = root / 'src/prismabuild'
        package.mkdir(parents=True)
        (package / 'reader.py').write_text(source if name == 'direct' else 'SCRIPT = ' + repr(source) + '\n')
        _, policies = audit(root)
        selected = [row for row in policies if row.get('binding') == 'required']
        assert len(selected) == 1
        assert selected[0]['unresolved'] == expression


def test_inventory_finds_unregistered_imported_embedded_and_data_wires(tmp_path):
    package = tmp_path / "src/prismabuild"
    package.mkdir(parents=True)
    (package / "record_kinds.py").write_text("KINDS = ()\n")
    (package / "owner.py").write_text('SCHEMA = "example.actual_record.v7"\n')
    (package / "caller.py").write_text('from . import owner\ndef write():\n    return dict(schema=owner.SCHEMA, payload=1)\ndef read(row):\n    allowed = frozenset({"schema", "payload"})\n    if set(row) - allowed: raise ValueError("unknown")\n    if row.get("schema") != owner.SCHEMA: raise ValueError("schema")\n    return row["payload"]\n')
    (package / "lazy.py").write_text('def publish():\n    from . import owner as selected\n    return {"schema": selected.SCHEMA, "payload": 2}\n')
    tools = tmp_path / "tools"
    tools.mkdir()
    (tools / "embedded.py").write_text('SCRIPT = \'schema = {"schema": "example.embedded.v3"}\\n\'\n')
    (tools / "config.json").write_text('{"schema": "example.configuration.v2"}')
    (tools / "once.py").write_text('SCRIPT = \'{"schema": "example.one_line.v4"}\'\n')
    (tools / "embedded.sh").write_text('python3 -c \'print({"schema": "example.shell.v2"})\'\n')
    (tools / "record.sh").write_text("python3 <<'PY'\ndef publish():\n    return {'schema': 'example.heredoc.v5'}\nPY\n")
    (tools / "marker.sh").write_text('cat <<EOF\n{"schema": "example.marker.v2", "unix": $(date +%s), "host": "$HOST"}\nEOF\n')
    rows, policies = audit(tmp_path)
    found = {row["wire_id"]: row for row in rows}
    assert set(found) == {"example.actual_record.v7", "example.embedded.v3", "example.configuration.v2",
                          "example.one_line.v4", "example.shell.v2", "example.heredoc.v5", "example.marker.v2"}
    writer = found["example.actual_record.v7"]["schema_builders"]
    assert writer[0]["owner"] == "write" and writer[0]["fields"] == ["payload", "schema"]
    assert any(row["owner"] == "publish" and row["path"].endswith("lazy.py") for row in writer)
    assert found["example.embedded.v3"]["occurrences"][0]["embedded"]
    assert found["example.heredoc.v5"]["schema_builders"][0]["owner"].endswith(".publish")
    assert found["example.marker.v2"]["schema_builders"][0]["fields"] == ["host", "schema", "unix"]
    assert found["example.actual_record.v7"]["reader_checks"][0]["owner"] == "read"
    assert any(row["owner"] == "read" and row.get("fields") == ["payload", "schema"] for row in policies)


def test_control_classification_cannot_hide_a_new_durable_reader(tmp_path):
    package = tmp_path / "src/prismabuild"
    package.mkdir(parents=True)
    source = package / "report.py"
    source.write_text('def report():\n    return {"schema": "example.status.v1", "rows": []}\n')
    rows, _ = audit(tmp_path)
    row = rows[0]
    captured = {key: sorted({item["path"] + ":" + item["owner"] for item in row[key]})
                for key in ("occurrences", "schema_builders", "reader_checks")}
    destination = tmp_path / "tests/compat/dispositions.jsonl"
    destination.parent.mkdir(parents=True)
    destination.write_text(json.dumps({"wire_id": row["wire_id"], "disposition": "operator_control",
                                      "reason": "stdout report with no durable reader", "witnesses": captured}) + "\n")
    assert audit(tmp_path)[0][0]["disposition"] == "operator_control"
    with source.open("a") as stream:
        stream.write('def recover(row):\n    if row.get("schema") != "example.status.v1": raise ValueError("wrong generation")\n    return row["rows"]\n')
    changed = audit(tmp_path)[0][0]
    assert changed["disposition"] == "classification_requires_review"
    assert changed["classification_changed"]["reader_checks"] == ["src/prismabuild/report.py:recover"]


def test_inventory_follows_transitive_module_owners_without_importing_them(tmp_path):
    package = tmp_path / "src/prismabuild"
    package.mkdir(parents=True)
    (package / "owner.py").write_text('SCHEMA = "example.transitive.v1"\nraise RuntimeError("never import source")\n')
    (package / "facade.py").write_text('from . import owner as selected\nfrom .owner import SCHEMA as REEXPORTED\n')
    (package / "cycle_a.py").write_text('from .cycle_b import SCHEMA\n')
    (package / "cycle_b.py").write_text('from .cycle_a import SCHEMA\n')
    (package / "caller.py").write_text('from . import facade, cycle_a\n'
        'def read(row):\n    if row.get("schema") != facade.selected.SCHEMA: raise ValueError("identity")\n'
        '    return row\n'
        'def read_export(row):\n    if row.get("schema") != facade.REEXPORTED: raise ValueError("identity")\n'
        '    return row\n'
        'def unrelated(row):\n    return row.get("schema") == cycle_a.SCHEMA\n')
    rows, _ = audit(tmp_path)
    record = next(row for row in rows if row["wire_id"] == "example.transitive.v1")
    assert [(row["path"], row["owner"]) for row in record["reader_checks"]] == [
        ("src/prismabuild/caller.py", "read"), ("src/prismabuild/caller.py", "read_export")]


def test_source_review_rebinding_compares_every_non_schema_expression():
    old = b"def read():\n    return {'schema': old_owner.SCHEMA, 'body': record}\n"
    moved = old.replace(b"old_owner", b"new_owner")
    changed = moved.replace(b"'body': record", b"'body': other")
    members = ["body", "schema"]
    assert reviewed_sites(old, "read", members, "resolved") == reviewed_sites(moved, "read", members, "resolved")
    assert reviewed_sites(old, "read", members, "resolved") != reviewed_sites(changed, "read", members, "resolved")


@pytest.mark.parametrize("stale", ["source", "line"])
def test_stale_source_review_retains_discovery_and_still_refuses(tmp_path, stale):
    import hashlib
    package = tmp_path / "src/prismabuild"
    package.mkdir(parents=True)
    source = package / "owner.py"
    source.write_text("def normalize(schema):\n    return {'schema': schema, 'body': True}\n")
    review = dict(path="src/prismabuild/owner.py", owner="normalize", line=2,
                  schema="schema", fields=["body", "schema"], reason="Private source control",
                  source_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
    if stale == "source":
        review["source_sha256"] = "0" * 64
    else:
        review["line"] = 99
    metadata = tmp_path / "tests/compat/dynamic_schema_dispositions.jsonl"
    metadata.parent.mkdir(parents=True)
    metadata.write_text(json.dumps(review) + "\n")
    original = metadata.read_bytes()
    output = tmp_path / "audit"
    result = subprocess.run([sys.executable, str(ROOT / "tests/compat/support/wire_inventory.py"),
                             "--root", str(tmp_path), "--out", str(output), "--require-complete"],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 2, result.stderr
    report = json.loads((output / "source-audit.json").read_bytes())
    assert len(report["source_review_problems"]) == 1
    assert len(report["unreviewed_dynamic_schema_builders"]) == 1
    assert (output / "inventory.jsonl").is_file()
    assert (output / "field_sets.json").is_file()
    assert metadata.read_bytes() == original

def _field_policy_tree(root):
    sources = {
        'src/prismabuild/plain.py': "ALLOWED_FIELDS = {'schema', 'value'}\ndef check(row):\n    required = {'schema'}\n    if set(row) != required: raise ValueError('fields')\n",
        'tools/embedded.py': "SCRIPT = \"def check(row):\\n    fields = {'embedded'}\\n    if set(row) != fields: raise ValueError('fields')\\n# schema\\n\"\n",
        'tools/extensionless': "def check(row):\n    optional = {'extra'}\n    return row\n",
        'tools/status.py': "def check(row):\n    return row.get('schema') == 'example.status.v1'\n",
        'fleet/check.sh': "python3 <<'PYTHON'\ndef check(row):\n    allowed = {'shell'}\n    return row\nPYTHON\n",
        'tests/ignored.py': "TEST_FIELDS = {'control'}\n",
    }
    for name, source in sources.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
    return [
        {'path': 'fleet/check.sh', 'owner': '<shell-python@2>.check', 'fields': ['shell'], 'binding': 'allowed'},
        {'path': 'src/prismabuild/plain.py', 'owner': '<module>', 'fields': ['schema', 'value'], 'binding': 'ALLOWED_FIELDS'},
        {'path': 'src/prismabuild/plain.py', 'owner': 'check', 'fields': ['schema'], 'binding': 'required'},
        {'path': 'src/prismabuild/plain.py', 'owner': 'check', 'fields': ['schema']},
        {'path': 'tools/embedded.py', 'owner': '<module>:embedded.check', 'fields': ['embedded'], 'binding': 'fields'},
        {'path': 'tools/embedded.py', 'owner': '<module>:embedded.check', 'fields': ['embedded']},
        {'path': 'tools/extensionless', 'owner': 'check', 'fields': ['extra'], 'binding': 'optional'},
    ]


def test_field_policy_findings_are_characterized(tmp_path):
    expected = _field_policy_tree(tmp_path)
    _, policies = audit(tmp_path)
    assert policies == expected
    assert field_policies(tmp_path) == expected
    assert [row for path in sorted(source_paths(tmp_path, include_tests=False))
            for row in field_policies(tmp_path, paths=[path])] == expected


def test_pinned_field_guard_does_not_scan_corpus_or_retained_records(tmp_path, monkeypatch):
    import wire_inventory

    expected = _field_policy_tree(tmp_path)
    pin = tmp_path / 'tests/compat/field_sets.json'
    pin.parent.mkdir(parents=True)
    pin.write_text(json.dumps(expected))
    original = wire_inventory.syntax_tree

    def policy_source(path, snippet):
        assert b'TEST_FIELDS' not in snippet, "field guard must not parse test-only source"
        return original(path, snippet)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("field guard must not audit retained record generations")

    monkeypatch.setattr(wire_inventory, 'syntax_tree', policy_source)
    monkeypatch.setattr(wire_inventory, 'retained_inventory', forbidden)
    monkeypatch.setattr(wire_inventory, 'source_expression', forbidden)
    monkeypatch.setitem(globals(), 'ROOT', tmp_path)
    for path in _field_policy_paths(tmp_path):
        test_closed_field_sets_remain_pinned(path)


def test_pinned_field_guard_scans_only_requested_source(tmp_path, monkeypatch):
    import wire_inventory

    expected = _field_policy_tree(tmp_path)
    pin = tmp_path / 'tests/compat/field_sets.json'
    pin.parent.mkdir(parents=True)
    pin.write_text(json.dumps(expected))
    original = wire_inventory.syntax_tree

    def selected_source(path, snippet):
        assert (path if path.is_absolute() else tmp_path / path) == tmp_path / 'tools/extensionless', "pin case must scan only its source"
        return original(path, snippet)

    monkeypatch.setattr(wire_inventory, 'syntax_tree', selected_source)
    monkeypatch.setitem(globals(), 'ROOT', tmp_path)
    test_closed_field_sets_remain_pinned(path='tools/extensionless')


@pytest.mark.parametrize('change', ['added', 'removed', 'reordered'])
def test_pin_cases_detect_added_and_removed_sources(tmp_path, monkeypatch, change):
    expected = _field_policy_tree(tmp_path)
    pin = tmp_path / 'tests/compat/field_sets.json'
    pin.parent.mkdir(parents=True)
    pin.write_text(json.dumps(expected))
    path = 'tools/new.py' if change == 'added' else 'tools/extensionless'
    if change == 'reordered':
        pin.write_text(json.dumps(expected[::-1]))
        path = expected[-1]['path']
    elif change == 'added':
        (tmp_path / path).write_text("NEW_FIELDS = {'new'}\n")
    else:
        (tmp_path / path).unlink()
    monkeypatch.setitem(globals(), 'ROOT', tmp_path)
    assert path in _field_policy_paths(tmp_path)
    with pytest.raises(AssertionError):
        test_closed_field_sets_remain_pinned(path)

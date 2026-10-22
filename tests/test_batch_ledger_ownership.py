"""C13's record boundary preserves real old-source bytes, effects and legacy paths."""
from __future__ import annotations

import ast
from collections.abc import Mapping
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import pytest

from prismabuild import batch_ledger as bl, output_contract as oc, digest_primitives
from prismabuild import pool, produced_output as po
import test_prepaid_writer_integration as output_fixture
from test_write_only_produced_output import _descriptor, _prewrite, _queue, _template

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tests/compat/support'))
from record_compat import retained

# These three functions are verbatim-identical to the former d2f92f5ff533
# baseline. Reuse its already-retained source, not the checkout's Git history.
ORIGINAL_GENERATION = 'a17477b2b206-1791745175-65dffa700dfb'


@pytest.fixture(scope='module')
def original_records():
    with retained() as generations:
        _, root, _ = next(row for row in generations if row[0] == ORIGINAL_GENERATION)
        source = (root / 'src/prismabuild/produced_output.py').read_text()
    tree = ast.parse(source)
    names = {'_read_commitments', '_write_commitments', '_class_sums'}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert {n.name for n in nodes} == names
    namespace = dict(json=json, os=os, Path=Path, tempfile=tempfile, Mapping=Mapping,
                     ProducedOutputError=oc.ProducedOutputError,
                     _check_class_bytes=oc.output_class_bytes)
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])),
                 ORIGINAL_GENERATION + '/produced_output.py', 'exec'), namespace)
    return namespace


def test_original_records_load_without_git_history(monkeypatch):
    def refuse_subprocess(*args, **kwargs):
        raise AssertionError('original records must load from committed fixtures')
    monkeypatch.setattr(subprocess, 'Popen', refuse_subprocess)
    records = original_records.__wrapped__()
    for name in ('_read_commitments', '_write_commitments', '_class_sums'):
        assert records[name].__code__.co_filename == ORIGINAL_GENERATION + '/produced_output.py'


def test_construction_preserves_the_path_and_performs_no_read():
    class UnreadablePath:
        def read_text(self):
            raise AssertionError('construction read the commitments')
    path = UnreadablePath()
    ledger = bl.BatchLedger(path)
    assert ledger.path is path


@pytest.mark.parametrize('admission', [None, {'label': 'ü/名字', 'bound_unix': float('nan')}],
                         ids=['absent-admission', 'original-nonfinite-and-ascii-recipe'])
def test_real_commitments_bytes_and_read_modify_write_match_original(
        tmp_path, original_records, admission):
    path = tmp_path / 'scope' / 'commitments.json'
    record = {'batches': {'b1': {'class_bytes': {'payload': 7, 'checkpoint': 0, 'temp': 0},
                                  'retired': True, 'label': 'ü/名字'}},
              'admission': admission}
    original_records['_write_commitments'](path, record)
    old = path.read_bytes()
    bl.BatchLedger(path).file(record)
    assert path.read_bytes() == old
    # A legacy row has no paths. Neither serialization invents one or a phase.
    assert b'"paths"' not in old and b'"phase"' not in old
    original_records['_write_commitments'](path, {'batches': record['batches']})
    modified = path.read_bytes()
    bl.BatchLedger(path).file({'batches': record['batches']})
    assert path.read_bytes() == modified
    assert bl._class_sums(record['batches']) == original_records['_class_sums'](record['batches'])


@pytest.mark.parametrize('raw', ['', '[]', '{"batches": []}', '{"batches":{},"admission":1}'])
def test_real_record_refusals_keep_the_original_type_and_message(tmp_path, original_records, raw):
    path = tmp_path / 'commitments.json'
    path.write_text(raw)
    with pytest.raises(oc.ProducedOutputError) as old:
        original_records['_read_commitments'](path)
    with pytest.raises(oc.ProducedOutputError) as current:
        bl.BatchLedger(path).capture()
    assert type(current.value) is type(old.value)
    assert str(current.value) == str(old.value)


def test_read_replace_fsync_order_matches_the_original_on_real_files(
        tmp_path, monkeypatch, original_records):
    path = tmp_path / 'scope' / 'commitments.json'
    path.parent.mkdir()
    initial = b'{"admission":{"minimum_gib":{}},"batches":{}}\n'
    record = {'batches': {'b1': {'origin_reclaimed': False}}}
    read_text, fsync, replace = Path.read_text, os.fsync, os.replace
    events = []
    def read(subject, *args, **kwargs):
        if subject == path:
            events.append('read-admission')
        return read_text(subject, *args, **kwargs)
    def sync(fd):
        events.append('fsync')
        return fsync(fd)
    def rename(source, destination, *args, **kwargs):
        if destination == path:
            events.append('replace-commitments')
        return replace(source, destination, *args, **kwargs)
    monkeypatch.setattr(Path, 'read_text', read)
    monkeypatch.setattr(os, 'fsync', sync)
    monkeypatch.setattr(os, 'replace', rename)
    path.write_bytes(initial)
    original_records['_write_commitments'](path, record)
    old_events, old_bytes = list(events), path.read_bytes()
    events.clear()
    path.write_bytes(initial)
    bl.BatchLedger(path).file(record)
    assert events == old_events == ['read-admission', 'fsync', 'replace-commitments']
    assert path.read_bytes() == old_bytes


def test_legacy_missing_paths_use_the_real_immutable_batch(tmp_path):
    template = _template(tmp_path / 'canonical')
    queue = _queue(tmp_path)
    # This reader control needs a bound owner and real immutable commit. File
    # the claim input explicitly, as the C31 writer does; queue admission and
    # its pidfd-backed census are exercised by their own integration controls.
    owner = output_fixture._hexkey('legacy-paths-owner')
    ready = queue.publish(
        action_key=owner, cas_root='/cas',
        worker_script=str(output_fixture.REPO / 'tools' / 'prismabuild_worker.py'),
        checkout_root=str(tmp_path / 'mover-checkout'), resources={'cpu': 1, 'mem_gb': 1},
        max_attempts=1, produced_output_template=template)
    claimed = pool.record_store.read_document(ready)
    assert isinstance(claimed, dict)
    claimed.update(claimed_by='w-owner', claimed_host=__import__('socket').gethostname(),
                   claimed_unix=pool._now(), attempts=1)
    ready.unlink()
    queue.records.replace_record_at(
        queue.records.item_path(pool.record_store.CLAIMED, owner), claimed)
    control = output_fixture._broker_control(queue, owner)
    po.declare_template(queue.root, template)
    instance = po.bind_instance(queue, template, owner_action_key=owner,
        claim_snapshot=claimed, env={'PRISMABUILD_ACTION_KEY': owner,
            'PRISMABUILD_ACTION_NONCE': control['nonce'],
            'PRISMABUILD_ACTION_SCOPE': control['scope_id']})
    bl.declare_instance(queue.root, instance)
    assert bl.BatchLedger.admit(queue, instance, template)['ok'] is True
    path = Path(template['output_prefix']) / 'legacy-paths-owner-b1.bin'
    payload = b'band handoff bytes'
    assert _prewrite(queue, instance, template, 'b1', [path], len(payload))['ok']
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    descriptor = _descriptor(instance, template, path, payload)
    _committed = bl.BatchLedger.commit_origin(
        queue, instance, template, [descriptor], batch_id='b1',
        lifetime=oc.ORIGIN_LIFETIME_CONSUMED)
    assert _committed['ok'], _committed
    ledger = bl.BatchLedger.for_instance(queue.root, instance)
    record = ledger.capture()
    record['batches']['b1'].pop('paths')
    ledger.file(record)
    committed, prewritten = bl._capture_attempt_paths(
        queue.root, ledger.path.parent, record['batches'])
    assert committed == {str(path): 'b1'}
    assert prewritten == {}
    assert bl.own_write_owner(queue.root, instance, template, [str(path)], 'b2') == (
        str(path), 'batch', 'b1')
    assert bl.BatchFacets(record['batches']['b1']).kind == 'origin'
    assert bl.BatchFacets(record['batches']['b1']).stage_retired is True
    assert bl.BatchFacets(record['batches']['b1']).origin_reclaimed is False


def test_the_admitted_interpreter_reports_its_actual_identity(capsys):
    executable = Path(sys.executable).resolve()
    payload = Path('/proc/self/exe').read_bytes()
    identity = {"version": sys.version, "version_info": list(sys.version_info[:3]),
                "executable": sys.executable, "resolved": str(executable),
                "mapped_executable": os.readlink('/proc/self/exe'),
                "bytes": len(payload), "sha256": digest_primitives.raw_sha256(payload),
                "host": __import__("socket").gethostname(),
                "cpus": sorted(os.sched_getaffinity(0)),
                "tmpdir": tempfile.gettempdir()}
    with capsys.disabled():
        print("c13-interpreter: " + json.dumps(identity), flush=True)
    assert tuple(identity["version_info"][:2]) in ((3, 12), (3, 14))

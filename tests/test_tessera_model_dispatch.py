"""PB owns whole-model subdivision, exact inputs, and the receipt barrier."""
import importlib.util
import json
from pathlib import Path
import re
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import tessera_model as model
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools/fleet'))
import dispatch_tessera_model as dispatcher


PRODUCER = SimpleNamespace(BODY_LAYER=re.compile(r'^model\.layers\.(\d+)\.'),
    partition_owner=lambda name, count: int(name.split('.')[2]) % count if '.layers.' in name else 0)


def test_single_source_file_still_yields_24_layer_actions():
    tensors = {f'model.layers.{i}.weight': 'model.safetensors' for i in range(24)}
    tensors['model.embed_tokens.weight'] = 'model.safetensors'
    assert model.partitions(tensors, PRODUCER) == 24
    spec = dict(cpus=1, mem_gb=16, assembly_mem_gb=4, tags=['gb10'])
    rows = [dispatcher.campaign_row('/checkout', spec, 'encode', index=i) for i in range(24)]
    assert len({tuple(row['argv']) for row in rows}) == 24
    assert all(row['tags'] == ['gb10'] and row['demand']['gpu'] == 1 for row in rows)
    assert not any('here' in row for row in rows)


def test_sparse_layers_never_generate_empty_partitions():
    names = ['model.layers.0.weight', 'model.layers.2.weight']
    count = model.partitions(names, PRODUCER)
    assert {PRODUCER.partition_owner(n, count) for n in names} == set(range(count))


def source_fixture(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'config.json').write_text('{}')
    (source / 'weights').write_bytes(b'original')
    identity = {'auxiliary_sha256': {}, 'config_sha256': model.digest_file(source / 'config.json'),
                'files': {'weights': model.digest_file(source / 'weights')}}
    return source, identity


def test_input_hash_is_reused_only_for_unchanged_verified_files(tmp_path, monkeypatch):
    source, identity = source_fixture(tmp_path)
    cache = tmp_path / 'cache'
    model.verified_inputs(source, identity, cache)
    original = model.digest_file
    calls = []
    def digest(path):
        calls.append(path)
        return original(path)
    monkeypatch.setattr(model, 'digest_file', digest)
    model.verified_inputs(source, identity, cache)
    assert calls == []
    (source / 'weights').write_bytes(b'modified')
    with pytest.raises(ValueError, match='source changed'):
        model.verified_inputs(source, identity, cache)


def test_mid_export_source_mutation_is_detected(tmp_path):
    source, identity = source_fixture(tmp_path)
    stamps = model.verified_inputs(source, identity, tmp_path / 'cache')
    (source / 'weights').write_bytes(b'changed')
    with pytest.raises(ValueError, match='during export'):
        model.check_stamps(source, stamps)


def test_output_must_match_receipt_and_actual_payload(tmp_path):
    (tmp_path / 'wire').write_bytes(b'wire')
    record = {'files': {'wire': model.digest_file(tmp_path / 'wire')}, 'contract': 'a'}
    model.atomic_json(tmp_path / 'pb-result.json', record)
    assert model.verify_record(tmp_path, record) == record
    with pytest.raises(ValueError, match='CAS receipt'):
        model.verify_record(tmp_path, dict(record, contract='b'))
    (tmp_path / 'wire').write_bytes(b'corrupt')
    with pytest.raises(ValueError, match='output changed'):
        model.verify_record(tmp_path, record)


def test_failed_partition_cannot_pass_assembly_barrier(tmp_path, monkeypatch):
    monkeypatch.setattr(dispatcher.pbcampaign, 'submit', lambda *a, **k: [
        {'action_key': 'a' * 64, 'status': 'submitted', 'published_unix': 100.}])
    monkeypatch.setattr(dispatcher.pbwait, 'wait_for_keys', lambda *a, **k: [
        {'action_key': 'a' * 64, 'status': 'failed', 'succeeded': False, 'host': 'worker',
         'returncode': 1, 'elapsed_s': 1, 'transport': 'pool'}])
    with pytest.raises(ValueError, match='assembly remains blocked'):
        dispatcher.run_stage([{}], tmp_path, 'encode', 1)
    assert not (tmp_path / 'barrier.json').exists()


def test_resume_refuses_an_input_override(tmp_path):
    with pytest.raises(SystemExit) as exc:
        dispatcher.main(['--workspace', str(tmp_path), '--resume', '--plan', 'changed.json'])
    assert exc.value.code == 2


def test_unlisted_output_cannot_be_reused(tmp_path):
    (tmp_path / 'wire').write_bytes(b'wire')
    record = {'files': {'wire': model.digest_file(tmp_path / 'wire')}}
    model.atomic_json(tmp_path / 'pb-result.json', record)
    (tmp_path / 'unexpected.safetensors').write_bytes(b'stale checkpoint')
    with pytest.raises(ValueError, match='population changed'):
        model.verify_record(tmp_path, record)


def test_mutated_plan_refuses_before_any_encode(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'plan.json').write_text('{}')
    model.atomic_json('job.json', {'plan_sha256': '0' * 64})
    with pytest.raises(ValueError, match='plan changed'):
        model.main(['encode'])

"""Configured class image drift gates container claims, not native work (#807)."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import adaptive_cpu, container_images as ci, core as pb, pool

CONTENT = 'content:sha256:' + 'a' * 64
OTHER = 'content:sha256:' + 'b' * 64
KEY = 'a' * 64
CAPACITY = {'cpu': 2, 'mem_gb': 2}
DECLARATION = {'schema': ci.CLASS_REQUIREMENTS_SCHEMA, 'classes': {
    'gb10': {'store_root': '/var/lib/docker', 'images': {'pb-canary:test': CONTENT}}}}


def snapshot(now=1000., **changes):
    return {'schema': ci.INVENTORY_SCHEMA, 'observed_unix': now,
            'entries': [CONTENT, OTHER], 'store_root': '/var/lib/docker',
            'image_contents': {'pb-canary:test': CONTENT}, **changes}


def policy():
    return ci.ClassImagePolicy(DECLARATION, klass='gb10')


def publish(queue, *, container=True):
    queue.publish(action_key=KEY, cas_root=str(queue.root / 'cas'), checkout_root='/co', worker_script='/w.py', resources={'cpu': 1, 'mem_gb': 1},
                  container_images=[CONTENT] if container else None)


def denials(queue):
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    return list(adaptive_cpu.read_json(path).get('records', {}).values())


@pytest.mark.parametrize('change,reason', [
    ({'image_contents': {}}, 'required_image_missing'),
    ({'image_contents': {'pb-canary:test': OTHER}}, 'image_content_mismatch'),
    ({'store_root': '/other/docker'}, 'image_store_mismatch'),
    ({'observed_unix': 994.}, 'inventory_unknown'),
    ({'observed_unix': 1001.}, 'inventory_unknown'),
    ({'entries': None}, 'inventory_unknown'),
    ({'image_contents': None}, 'inventory_unknown'),
])
def test_drift_refuses_before_consuming_claim_attempt_or_resources(tmp_path, monkeypatch, change, reason):
    monkeypatch.setattr(pool, '_now', lambda: 1000.)
    q = pool.PoolQueue(tmp_path / 'queue')
    q.ledger().ensure_capacity(CAPACITY)
    publish(q)
    before = q.item_path(pool.READY, KEY).read_bytes()
    claimed = q.claim(tags=[pb.CONTAINER_IMAGE_TAG], capacity=CAPACITY,
                      observed_images=[CONTENT], container_class_policy=policy(),
                      container_inventory=snapshot(**change))
    assert claimed is None
    assert q.item_path(pool.READY, KEY).read_bytes() == before
    assert not q.item_path(pool.CLAIMED, KEY).exists()
    assert q.ledger().available() == CAPACITY
    assert denials(q)[-1]['reason'] == 'container_class_' + reason


def test_unknown_class_inventory_refuses_even_with_positive_action_reference(tmp_path, monkeypatch):
    monkeypatch.setattr(pool, '_now', lambda: 1000.)
    q = pool.PoolQueue(tmp_path / 'queue'); publish(q)
    assert q.claim(tags=[pb.CONTAINER_IMAGE_TAG], observed_images=[CONTENT],
                   container_class_policy=policy(), container_inventory=None) is None
    assert denials(q)[-1]['reason'] == 'container_class_inventory_unknown'


def test_native_claim_survives_class_drift(tmp_path, monkeypatch):
    monkeypatch.setattr(pool, '_now', lambda: 1000.)
    q = pool.PoolQueue(tmp_path / 'queue'); publish(q, container=False)
    assert q.claim(capacity=CAPACITY, container_class_policy=policy(), container_inventory=None) is not None


def test_configured_claim_uses_current_snapshot_not_an_old_satisfied_offer(tmp_path, monkeypatch):
    monkeypatch.setattr(pool, '_now', lambda: 1000.)
    q = pool.PoolQueue(tmp_path / 'queue'); publish(q)
    q.announce(host='seen', tags=[pb.CONTAINER_IMAGE_TAG], has_gpu=False,
               observed_images=[CONTENT], container_class_verdict=policy().evaluate(snapshot(), now=1000.))
    assert q.placeable(q.ready_items()[0])
    assert q.claim(tags=[pb.CONTAINER_IMAGE_TAG], observed_images=[CONTENT],
                   container_class_policy=policy(), container_inventory=snapshot(image_contents={})) is None


def test_satisfied_claim_retains_configuration_and_observation_evidence(tmp_path, monkeypatch):
    monkeypatch.setattr(pool, '_now', lambda: 1000.)
    q = pool.PoolQueue(tmp_path / 'queue'); publish(q)
    claimed = q.claim(tags=[pb.CONTAINER_IMAGE_TAG], capacity=CAPACITY, observed_images=[CONTENT],
                      container_class_policy=policy(), container_inventory=snapshot())
    evidence = claimed['container_class_verdict']
    assert evidence['status'] == 'satisfied'
    assert evidence['observed_unix'] == 1000.
    assert len(evidence['requirements_sha256']) == 64
    assert evidence == json.loads(q.item_path(pool.CLAIMED, KEY).read_text())['container_class_verdict']


@pytest.mark.parametrize('container', [True, False])
def test_offer_reports_refusal_and_only_excludes_declared_container_rows(tmp_path, monkeypatch, container):
    monkeypatch.setattr(pool, '_now', lambda: 1000.)
    q = pool.PoolQueue(tmp_path / 'queue'); publish(q, container=container)
    evidence = policy().evaluate(snapshot(image_contents={}), now=1000.)
    q.announce(host='drifted', tags=[pb.CONTAINER_IMAGE_TAG], has_gpu=False,
               observed_images=[CONTENT], container_class_verdict=evidence)
    assert q.offers()[0]['container_class_verdict'] == evidence
    assert q.placeable(q.ready_items()[0]) is (not container)


def test_offer_class_observation_cannot_borrow_fresh_offer_time(tmp_path, monkeypatch):
    now = [1000.]
    monkeypatch.setattr(pool, '_now', lambda: now[0])
    q = pool.PoolQueue(tmp_path / 'queue'); publish(q)
    evidence = policy().evaluate(snapshot(), now=1000.)
    now[0] = 1031.
    q.announce(host='old-observation', tags=[pb.CONTAINER_IMAGE_TAG], has_gpu=False,
               observed_images=[CONTENT], container_class_verdict=evidence)
    assert not q.placeable(q.ready_items()[0])


def test_expiry_during_reservation_abandons_only_this_handle(tmp_path, monkeypatch):
    now = [1000.]
    monkeypatch.setattr(pool, '_now', lambda: now[0])
    q = pool.PoolQueue(tmp_path / 'queue'); q.ledger().ensure_capacity(CAPACITY); publish(q)
    real_begin = pool.ResourceLedger.begin_acquire
    def delayed_begin(self, *args, **kwargs):
        result = real_begin(self, *args, **kwargs)
        now[0] += 6.
        return result
    monkeypatch.setattr(pool.ResourceLedger, 'begin_acquire', delayed_begin)
    assert q.claim(tags=[pb.CONTAINER_IMAGE_TAG], capacity=CAPACITY, observed_images=[CONTENT],
                   container_class_policy=policy(), container_inventory=snapshot()) is None
    assert q.ledger().available() == CAPACITY
    assert q.item_path(pool.READY, KEY).exists()
    assert denials(q)[-1]['reason'] == 'container_class_inventory_unknown'


def test_snapshot_shares_cache_refresh_and_cannot_mutate_stored_evidence(tmp_path):
    root = tmp_path / 'cache'; root.mkdir(mode=0o700)
    (root / 'inventory.json').write_text(json.dumps(snapshot())); (root / 'inventory.json').chmod(0o600)
    cache = ci.InventoryCache(root=root, clock=lambda:1000., probe=lambda *a, **k: pytest.fail('duplicate probe'))
    observed = cache.snapshot(max_age_s=5.)
    assert observed == snapshot()
    observed['image_contents'].clear()
    assert cache.snapshot()['image_contents'] == {'pb-canary:test': CONTENT}
    assert cache.get() == frozenset([CONTENT, OTHER])


def test_policy_detaches_from_mutable_config_and_pins_canonical_identity():
    declaration = copy.deepcopy(DECLARATION)
    p = ci.ClassImagePolicy(declaration, klass='gb10')
    declaration['classes']['gb10']['images'].clear()
    assert p.evaluate(snapshot(image_contents={}), now=1000.)['reason'] == 'required_image_missing'
    assert p.requirements_sha256 == policy().requirements_sha256


@pytest.mark.parametrize('path', ['../foreign.json', '/absolute.json'])
def test_worker_config_cannot_escape_generation(tmp_path, path):
    with pytest.raises(ValueError, match='contained|relative'):
        ci.ClassImagePolicy.from_file(path, source_root=tmp_path, klass='gb10')


def test_worker_config_load_is_bounded_and_rejects_duplicate_keys(tmp_path):
    config = tmp_path / 'class-images.json'
    config.write_text('{"schema":"x","schema":"y","classes":{}}')
    with pytest.raises(ValueError, match='duplicate'):
        ci.ClassImagePolicy.from_file('class-images.json', source_root=tmp_path, klass='gb10')
    config.write_text(json.dumps(DECLARATION))
    p = ci.ClassImagePolicy.from_file('class-images.json', source_root=tmp_path, klass='gb10')
    assert p.requirements_sha256 == policy().requirements_sha256


def test_expiry_during_intent_write_restores_moved_item_and_returns_own_reservation(tmp_path, monkeypatch):
    now = [1000.]
    monkeypatch.setattr(pool, '_now', lambda: now[0])
    q = pool.PoolQueue(tmp_path / 'queue'); publish(q)
    original = q._write_claim_intent
    def delay(*args, **kwargs):
        original(*args, **kwargs)
        now[0] += 6.
    monkeypatch.setattr(q, '_write_claim_intent', delay)
    assert q.claim(tags=[pb.CONTAINER_IMAGE_TAG], capacity=CAPACITY, observed_images=[CONTENT],
                   container_class_policy=policy(), container_inventory=snapshot()) is None
    assert q.ledger().available() == CAPACITY
    assert q.item_path(pool.READY, KEY).exists()
    assert not q.item_path(pool.CLAIMED, KEY).exists()
    assert denials(q)[-1]['reason'] == 'container_class_inventory_unknown'


def test_serve_once_forwards_the_actual_class_snapshot_to_claim(tmp_path, monkeypatch):
    q = pool.PoolQueue(tmp_path / 'queue'); publish(q)
    selected = policy(); observed = snapshot(); seen = {}
    def capture(**kwargs):
        seen.update(kwargs)
        return None
    monkeypatch.setattr(q, 'claim', capture)
    monkeypatch.setattr(q, '_sweep_due', lambda: False)
    q.serve_once(capacity=CAPACITY, container_class_policy=selected, container_inventory=observed)
    assert seen['container_class_policy'] is selected
    assert seen['container_inventory'] is observed


def test_config_symlink_cannot_point_outside_the_immutable_source(tmp_path):
    source = tmp_path / 'source'; source.mkdir()
    outside = tmp_path / 'external.json'; outside.write_text(json.dumps(DECLARATION))
    (source / 'escape.json').symlink_to(outside)
    with pytest.raises(ValueError, match='contained'):
        ci.ClassImagePolicy.from_file('escape.json', source_root=source, klass='gb10')


def test_worker_reads_one_cache_snapshot_for_class_and_reference_evidence(tmp_path, monkeypatch):
    import importlib.util
    source = Path(__file__).resolve().parents[1] / 'tools/fleet/worker_loop.py'
    spec = importlib.util.spec_from_file_location('class_image_loop', source)
    loop = importlib.util.module_from_spec(spec); spec.loader.exec_module(loop)
    seen = []
    class Cache:
        def snapshot(self, **kwargs):
            seen.append(kwargs)
            return snapshot()
        def get(self, **kwargs):
            pytest.fail('class path must not duplicate the inventory read')
    monkeypatch.setattr(loop.time, 'time', lambda: 1000.)
    refs, observed, verdict = loop.image_observation(Cache(), policy(), max_age_s=5.)
    assert refs == frozenset([CONTENT, OTHER]) and observed == snapshot()
    assert verdict['status'] == 'satisfied' and seen == [{'max_age_s': 5.}]
    assert loop.build_parser().parse_args(['--class-image-config', 'images.json']).class_image_config == 'images.json'


@pytest.mark.parametrize('draining', [False, True])
def test_real_worker_poll_publishes_class_refusal_and_preserves_native_polling(tmp_path, monkeypatch, draining):
    from test_a_drained_box_finishes_its_pending_claim_1403 import _load_loop
    import socket
    selected = copy.deepcopy(DECLARATION)
    selected['classes']['x86'] = selected['classes'].pop('gb10')
    (tmp_path / 'images.json').write_text(json.dumps(selected))
    loop = _load_loop(tmp_path, monkeypatch, {'draining': draining, 'changed_unix': 1.})
    monkeypatch.setattr(loop, 'RUNTIME_ROOT', tmp_path)
    sys.argv.extend(['--class-image-config', 'images.json'])
    monkeypatch.setattr(loop.container_images.InventoryCache, 'snapshot', lambda *a, **k: snapshot(image_contents={}))
    monkeypatch.setattr(loop.time, 'time', lambda: 1000.)
    seen = []
    monkeypatch.setattr(loop.pool.PoolQueue, 'serve_once', lambda *a, **k: seen.append(k))
    assert loop._run_loop(lambda: False) == 0
    q = pool.PoolQueue(tmp_path / 'pb-queue')
    offer = json.loads((q.root / pool.WORKERS / (socket.gethostname()+'.json')).read_text())
    assert offer['container_class_verdict']['reason'] == 'required_image_missing'
    assert offer['container_images'] == [CONTENT, OTHER]
    if draining:
        assert not seen and offer['state'] == 'draining'
    else:
        assert len(seen) == 1 and seen[0]['container_class_policy'].klass == 'x86'

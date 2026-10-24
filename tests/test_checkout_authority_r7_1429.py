"""Identity-only certification boundaries and lease-proof forgery refusals."""
import ast
import copy
from pathlib import Path

import pytest

from prismabuild import claim_identity, materialize, pool, record_store
from test_checkout_review_r7_1429 import _uncertified, _owner, _forged_unlink_proof
from test_lifetime_contract_1429 import broker, Skew


def test_certificate_entrypoints_accept_identity_only():
    root = Path(__file__).resolve().parents[1] / 'src/prismabuild'
    signatures = {}
    for source in root.glob('*.py'):
        tree = ast.parse(source.read_text())
        for node in tree.body:
            assert not (isinstance(node, ast.FunctionDef) and node.name in {
                'certify_release', 'certify_checkout_cleanup'}), source
        for call in ast.walk(tree):
            if isinstance(call, ast.Call) and getattr(call.func, 'attr', '') in {
                    'certify_release', 'certify_checkout_cleanup'}:
                assert len(call.args) == 1 and not any(isinstance(arg, ast.Starred) for arg in call.args)
                assert all(keyword.arg in {'attempt', 'claim_nonce', 'nonce'} for keyword in call.keywords)
        for cls in (n for n in tree.body if isinstance(n, ast.ClassDef)):
            for method in cls.body:
                if isinstance(method, ast.FunctionDef) and method.name in {'certify_release', 'certify_checkout_cleanup'}:
                    args = method.args
                    assert not args.vararg and not args.kwarg and not args.posonlyargs
                    parameters = tuple(a.arg for a in args.args + args.kwonlyargs)
                    signatures[(cls.name, method.name)] = parameters
    assert signatures == {
        ('ResourceLedger', 'certify_release'): ('self', 'action_key', 'attempt', 'claim_nonce', 'nonce'),
        ('TierLedger', 'certify_release'): ('self', 'holder', 'attempt', 'claim_nonce', 'nonce'),
        ('PoolQueue', 'certify_checkout_cleanup'): ('self', 'action_key', 'attempt', 'claim_nonce'),
        ('CheckoutCertifier', 'certify_checkout_cleanup'): ('self', 'action_key', 'attempt', 'claim_nonce', 'nonce'),
    }


@pytest.mark.parametrize('field', ['claim_snapshot', 'proof', 'owner_path', 'queue', 'local_checkout_root', 'release_ledger', 'record'])
@pytest.mark.parametrize('form', ['keyword', 'positional', 'splat', 'reflection'])
def test_evidence_cannot_enter_either_certificate_api(tmp_path, broker, monkeypatch, field, form):
    queue, key, item, _, checkout = _uncertified(tmp_path, monkeypatch, lose='claim')
    owner = _owner(queue, key)
    forged = dict(owner, checkout_cleanup=_forged_unlink_proof(owner))
    for authority, method in ((queue.ledger(), 'certify_release'), (queue.queue, 'certify_checkout_cleanup'),
                              (materialize.CheckoutCertifier(queue.root), 'certify_checkout_cleanup')):
        function = getattr(authority, method)
        with pytest.raises(TypeError):
            if form == 'positional':
                function(key, forged)
            elif form == 'reflection':
                getattr(type(authority), method)(authority, key, **{field: forged})
            else:
                function(key, **{field: forged})
    assert checkout.is_dir() and queue.ledger().holder_tokens(key)


def test_planted_lease_proof_never_becomes_an_unlink_journal(tmp_path, broker, monkeypatch):
    queue, key, item, _, checkout = _uncertified(tmp_path, monkeypatch, lose='claim')
    owner = _owner(queue, key)
    proof = _forged_unlink_proof(owner)
    owner['checkout_cleanup'] = proof
    owner['checkout_registrations'][0]['cleanup'] = copy.deepcopy(proof)
    queue.records.replace_record_at(queue.records.lease_path(key), owner)
    assert not queue.queue.certify_checkout_cleanup(key)['complete']
    Skew(monkeypatch).to(item['lifetime_deadline_unix'] + 3 * pool.LEASE_TIMEOUT_S)
    queue.sweep_widowed_leases(timeout_s=pool.LEASE_TIMEOUT_S)
    assert checkout.is_dir() and queue.ledger().holder_tokens(key)
    assert not (queue.root / 'checkout-unlinks').exists()


def test_foreign_queue_cannot_be_bound_to_a_real_ledger(tmp_path, broker, monkeypatch):
    queue, key, item, _, checkout = _uncertified(tmp_path, monkeypatch, lose='lease')
    foreign = pool.PoolQueue(tmp_path / 'foreign')
    with pytest.raises(pool.pb.PrismaBuildError, match='queue differ'):
        materialize.CheckoutCertifier(foreign.root, ledger=queue.ledger())
    assert checkout.is_dir() and queue.ledger().holder_tokens(key)


def test_identity_cannot_select_another_incarnation(tmp_path, broker, monkeypatch):
    queue, key, item, _, checkout = _uncertified(tmp_path, monkeypatch, lose='claim')
    with pytest.raises(pool.PoolContractError):
        queue.ledger().certify_release(key, attempt=item['attempts'] + 1,
                                     claim_nonce=claim_identity.claim_nonce(item))
    with pytest.raises(pool.PoolContractError):
        queue.ledger().certify_release(key, attempt=item['attempts'], claim_nonce='foreign')
    assert checkout.is_dir() and queue.ledger().holder_tokens(key)

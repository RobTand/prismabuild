"""Opus review r4 probes: can anything but gpu_execution_judgment mint an accepted GpuUseJudgment?

A failing test is a finding. CPU-only fixtures; no GPU. Controls (expected to
pass) confirm the builder's claims; probes named ``*_leak*``/``*_guard*``
test the module-private authority and the AST guard's spelling coverage.
"""
import copy
import os
import pickle
import site
import sys
import warnings
from pathlib import Path

import pytest

from gpu_evidence_fixtures import frozen_gpu
from prismabuild import core as pb
from test_gpu_frozen_record_1744 import _gpu_constructor_calls

USED = {'gpu_uuids': ['GPU-real'], 'gpu_evidence': {'sample_failed': False, 'live_sample': True,
                                                    'final_succeeded': True}}
FORGED = {'gpu_use': 'zero', 'gpu_use_reason': 'successful_payload_sample', 'gpu_devices_used': 0,
          'gpu_uuids': [], 'nonce': None, 'scope_id': None}


def _issued():
    return pb.gpu_execution_judgment(frozen_gpu(USED))


def _verdict(value):
    try:
        pb.GpuUseJudgment.require(value)
    except pb.ActionContractError:
        return 'refused'
    return 'accepted:' + str(value['gpu_use'])


# -- Controls: the builder's claims ------------------------------------------

def test_control_plain_mapping_bare_object_new_and_missing_authority_are_refused():
    bare = object.__new__(pb.GpuUseJudgment)
    encoded = object.__new__(pb.GpuUseJudgment)
    object.__setattr__(encoded, '_encoded', pb._canonical_bytes(FORGED))
    with pytest.raises(pb.ActionContractError):
        pb.GpuUseJudgment(FORGED)
    assert [_verdict(v) for v in (FORGED, bare, encoded)] == ['refused'] * 3


def test_control_copy_cannot_mint_a_changed_judgment():
    judgment = _issued()
    outcomes = {}
    for name, fn in (('copy', copy.copy), ('deepcopy', copy.deepcopy)):
        try:
            clone = fn(judgment)
        except Exception as exc:                                  # noqa: BLE001
            outcomes[name] = 'raises ' + type(exc).__name__
            continue
        outcomes[name] = _verdict(clone)
        assert outcomes[name] == 'refused' or clone.as_dict() == judgment.as_dict(), outcomes
    replace = getattr(copy, 'replace', None)
    if replace is not None:
        with pytest.raises(Exception):
            replace(judgment, _encoded=pb._canonical_bytes(FORGED))
    warnings.warn(f'R4-CAPABILITY copy: {outcomes}')


@pytest.mark.parametrize('protocol', range(2, pickle.HIGHEST_PROTOCOL + 1))
def test_control_pickle_round_trip_cannot_mint(protocol):
    judgment = _issued()
    try:
        blob = pickle.dumps(judgment, protocol=protocol)
        clone = pickle.loads(blob)
    except Exception as exc:                                      # noqa: BLE001
        warnings.warn(f'R4-CAPABILITY pickle protocol {protocol}: raises {type(exc).__name__}')
        return
    assert _verdict(clone) == 'refused' or clone.as_dict() == judgment.as_dict()


def test_control_subclass_is_refused_even_with_the_authority():
    judgment = _issued()

    class Sub(pb.GpuUseJudgment):
        __slots__ = ()

    with pytest.raises(pb.ActionContractError):
        Sub(FORGED)
    with pytest.raises(pb.ActionContractError):
        Sub(FORGED, _authority=getattr(judgment, '_authority', None))


# -- Probes: the authority is published on every issued instance -------------

def test_leak_type_of_any_issued_judgment_mints_an_accepted_forgery():
    judgment = _issued()
    forged = _try_mint(lambda: type(judgment)(FORGED, _authority=getattr(judgment, '_authority', None)))
    assert _verdict(forged) == 'refused', (
        'any holder of an issued judgment mints another: type(j)(data, _authority=j._authority) '
        f'is {_verdict(forged)} by GpuUseJudgment.require')


def test_leak_object_new_plus_instance_authority_mints_an_accepted_forgery():
    judgment = _issued()
    forged = object.__new__(type(judgment))
    object.__setattr__(forged, '_encoded', pb._canonical_bytes(FORGED))
    with pytest.raises((AttributeError, pb.ActionContractError)):
        object.__setattr__(forged, '_authority', getattr(judgment, '_authority', None))
    assert _verdict(forged) == 'refused', (
        f'object.__new__ + object.__setattr__ with j._authority is {_verdict(forged)}')


SPELLINGS = {
    'type_of_instance': 'type(j)(x, _authority=j._authority)',
    'dunder_class': 'j.__class__(x, _authority=j._authority)',
    'object_new': ('(lambda f: (object.__setattr__(f, "_encoded", pb._canonical_bytes(x)), '
                   'object.__setattr__(f, "_authority", j._authority), f)[-1])(object.__new__(type(j)))'),
    'computed_getattr': 'getattr(pb, "GpuUse" + "Judgment")(x, _authority=pb._GPU_JUDGMENT_AUTHORITY)',
    'module_vars': 'vars(pb)["GpuUseJudgment"](x, _authority=pb._GPU_JUDGMENT_AUTHORITY)',
}


@pytest.mark.parametrize('spelling', sorted(SPELLINGS))
def test_guard_sees_every_accepted_construction_spelling(spelling):
    source = f'def illicit(j, x):\n    return {SPELLINGS[spelling]}\n'
    namespace = {'pb': pb}
    exec(compile(source, 'mutant', 'exec'), namespace)
    verdict = _verdict(_try_mint(lambda: namespace['illicit'](_issued(), FORGED)))
    sites = _gpu_constructor_calls(source)
    assert verdict == 'refused' or sites, (
        f'{spelling}: {SPELLINGS[spelling]!r} is {verdict} at runtime and invisible to the AST guard '
        f'(_gpu_constructor_calls -> {sites})')


def test_guard_control_named_constructor_is_seen():
    assert _gpu_constructor_calls('def illicit(x):\n    return pb.GpuUseJudgment(x)\n') == [('illicit', 2)]


# -- Payload-reachable code paths (environment fact, no writes) ---------------

@pytest.mark.xfail(reason="PB lead N-6: runtime/import-path code execution is outside this PR", strict=False)
def test_payload_uid_cannot_rewrite_pb_import_paths():
    uid = os.geteuid()
    candidates = [Path(p) for p in site.getsitepackages() if Path(p).is_dir()]
    candidates.append(Path('/mnt/shared/prismabuild-fleet/repo/src/prismabuild'))
    owned = {str(p): {'owner_is_payload_uid': p.stat().st_uid == uid, 'w_ok': os.access(p, os.W_OK)}
             for p in candidates if p.exists()}
    warnings.warn(f'R4-CAPABILITY import paths: {sys.executable} {owned}')
    assert not any(v['owner_is_payload_uid'] or v['w_ok'] for v in owned.values()), (
        f'a same-UID payload can rewrite PB/runtime import paths (owner chmod or write): {owned}')


def _try_mint(factory):
    try:
        return factory()
    except (AttributeError, pb.ActionContractError):
        return None

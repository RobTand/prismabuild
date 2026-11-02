import importlib.machinery
import importlib.util
from pathlib import Path

import pytest


def vendor_test_shim():
    path = Path(__file__).resolve().parents[1] / 'tools/fleet/docker'
    loader = importlib.machinery.SourceFileLoader('gpu_options_shim', str(path))
    shim = importlib.util.module_from_spec(importlib.util.spec_from_loader(loader.name, loader))
    loader.exec_module(shim)
    return shim


def intent_for(*options):
    shim = vendor_test_shim()
    intent = {}
    shim._container_options([*options, 'image'], runtime_intent=intent)
    return shim._nvidia_injection_intent(intent)


MALFORMED = [
    'driver=cdi,device=nvidia.com/gpu=all,count=bogus',
    'driver=cdi,device=nvidia.com/gpu=all,bogus=1',
    'driver=cdi,device=nvidia.com/gpu=all,count=1,count=2',
    'driver=cdi,driver=cdi,device=nvidia.com/gpu=all',
    'driver=cdi,device=nvidia.com/gpu=all,device=nvidia.com/gpu=1',
    'driver=cdi,device=nvidia.com/g"pu=all',
    'driver=cdi,device=nvidia.com/gpu=0,nvidia.com/gpu=1',
    'driver=cdi,"device=nvidia.com/gpu=all',
]


@pytest.mark.parametrize('value', MALFORMED)
def test_malformed_gpus_value_has_no_nvidia_intent(value):
    assert intent_for('--gpus', value) is False


@pytest.mark.parametrize('value', MALFORMED)
@pytest.mark.parametrize('sibling', [
    ('--gpus', 'all'),
    ('--device', 'nvidia.com/gpu=all'),
])
def test_malformed_gpus_value_voids_valid_sibling_intent(value, sibling):
    assert intent_for(*sibling) is True
    assert intent_for('--gpus', value, *sibling) is False
    assert intent_for(*sibling, '--gpus', value) is False


@pytest.mark.parametrize('value', ['', 'driver=cdi,', ',', 'driver=nvidia,,count=all'])
def test_empty_gpus_value_or_field_has_no_nvidia_intent(value):
    assert intent_for('--gpus', value) is False
    assert intent_for('--gpus', 'all', '--gpus', value) is False
    assert intent_for('--gpus=' + value) is False


@pytest.mark.parametrize('count,expected', [
    ('all', True), ('1', True), ('0', True), ('bogus', False), ('1x', False),
])
def test_gpus_count_accepts_all_and_non_negative_integers_only(count, expected):
    assert intent_for('--gpus', f'driver=nvidia,count={count}') is expected


@pytest.mark.parametrize('options,expected', [
    (('--gpus', 'all'), True),
    (('--gpus', 'driver=nvidia,count=all'), True),
    (('--gpus=driver=nvidia,count=all'), True),
    (('--gpus', 'driver=nvidia,count=1'), True),
    (('--gpus', '"device=0,1",capabilities=utility'), True),
    (('--gpus', 'driver=cdi,device=nvidia.com/gpu=all'), True),
    (('--gpus', 'driver=cdi,"device=nvidia.com/gpu=0,nvidia.com/gpu=1"'), True),
    (('--gpus', 'driver=cdi,"device=amd.com/gpu=0,nvidia.com/gpu=1"'), True),
    (('--gpus', 'driver=cdi,device=amd.com/gpu=all'), False),
    (('--gpus', 'driver=other,count=all'), False),
    (('--gpus', 'driver=other', '--gpus', 'all'), True),
    (('--entrypoint', '--gpus=all'), False),
])
def test_valid_gpus_forms_keep_their_intent(options, expected):
    if isinstance(options, str):
        options = (options,)
    assert intent_for(*options) is expected

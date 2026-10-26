"""Real NVIDIA execution, enabled only by the GPU qualification tier."""
from __future__ import annotations

import json
import os
import platform
import sys

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get('PRISMABUILD_GPU_QUALIFICATION') != '1',
    reason='run python3 tools/verify.py gpu for admitted NVIDIA qualification',
)


def test_admitted_cuda_executes_and_returns_correct_results(capsys):
    import torch

    assert torch.version.cuda, 'qualification requires NVIDIA CUDA PyTorch'
    assert torch.cuda.is_available(), 'the admitted GPU must be visible'
    torch.set_num_threads(1)
    values = (torch.arange(256 * 256, dtype=torch.float32) % 7).reshape(256, 256)
    expected = values.double() @ values.double().T
    device_values = values.to('cuda')
    for _ in range(64):
        actual = device_values @ device_values.T
    torch.cuda.synchronize()
    assert actual.is_cuda
    assert torch.cuda.memory_allocated() > 0
    torch.testing.assert_close(actual.cpu().double(), expected, rtol=0, atol=0)
    with capsys.disabled():
        print('prismabuild-gpu-qualification: ' + json.dumps({
            'host': platform.node(), 'python': sys.executable,
            'torch': torch.__version__, 'cuda': torch.version.cuda,
            'device': torch.cuda.get_device_name(), 'iterations': 64,
            'allocated_bytes': torch.cuda.memory_allocated(),
            'checksum': actual.double().sum().item(),
            'expected_checksum': expected.sum().item(),
        }), flush=True)

"""Agent test execution and GPU containers use the shared scheduler."""
import pytest
from test_require_pool import _armed, _verdict


@pytest.mark.parametrize('command', [
    'python3 -m pytest tests', 'CUDA_VISIBLE_DEVICES= python3 -m pytest tests',
    'pytest -q', 'python3 -m unittest discover', 'ctest --parallel 8',
    'cargo test', 'go test ./...', 'npm test', 'npm run test:unit',
    'uv run pytest tests', 'docker run --gpus all image train.py',
    'echo ok && pytest tests', 'pbrun.py -- true && pytest tests',
])
def test_off_pool_test_and_gpu_work_is_refused(tmp_path, command):
    assert _verdict(_armed(tmp_path, None), command) == 2


@pytest.mark.parametrize('command', [
    'python3 tools/fleet/pbrun.py --cpus 8 -- python3 -m pytest -n 8',
    'python3 tools/fleet/pbtest.py --checkout . --python /usr/bin/python3',
    'python3 tools/fleet/pbcampaign.py tests.json',
    'git commit -m "pytest requires PrismaBuild"',
    'rg pytest tests', 'nvidia-smi', 'python3 -m pytest --help',
])
def test_submission_and_inspection_are_allowed(tmp_path, command):
    assert _verdict(_armed(tmp_path, None), command) == 0

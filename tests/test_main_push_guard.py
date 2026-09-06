from pathlib import Path
import subprocess

import pytest

HOOK = Path(__file__).resolve().parents[1] / '.githooks/pre-push'

@pytest.mark.parametrize('rows,allowed', [
    ('refs/heads/fix a refs/heads/fix b\n', True),
    ('refs/heads/fix a refs/heads/main b\n', False),
    ('(delete) 0 refs/heads/main b\n', False),
    ('refs/heads/fix a refs/heads/fix b\nrefs/heads/main a refs/heads/main b\n', False),
    ('refs/tags/main a refs/tags/main b\n', True),
])
def test_main_push_guard(rows, allowed):
    result = subprocess.run(['sh', str(HOOK)], input=rows, text=True, capture_output=True)
    assert (result.returncode == 0) is allowed
    if not allowed:
        assert 'linked issue and pull request' in result.stderr

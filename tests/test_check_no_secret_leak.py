"""Pin the shared scanner and verify the artifact scan used by the gate."""
import gzip
import hashlib
import importlib.util
from compression import zstd
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCANNER = ROOT / 'tools/ci/check_no_secret_leak.py'
# RobTand/prismaquant 71c36dc8.
SCANNER_SHA256 = 'decd9ce671299ece1b245811af459349ed9932ee9b81b2c4c7a13cf96b94f609'


def test_vendored_scanner_is_byte_identical_to_prismaquant():
    assert hashlib.sha256(SCANNER.read_bytes()).hexdigest() == SCANNER_SHA256


@pytest.mark.parametrize('kind', [
    'clean', 'raw', 'gzip', 'zstd', 'zstd-stray-gzip-clean', 'zstd-stray-gzip-leak',
])
def test_workflow_scan_accepts_clean_artifacts_and_blocks_leaks(kind, tmp_path, monkeypatch, capsys):
    spec = importlib.util.spec_from_file_location('check_no_secret_leak', SCANNER)
    scanner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(scanner)
    secret = 'fake-secret-value'
    names = ('BUILDBUDDY_API_KEY', 'MERGIFY_TOKEN', 'GITHUB_TOKEN')
    monkeypatch.setattr(scanner.os, 'environ', {name: secret for name in names})
    clean = kind in ('clean', 'zstd-stray-gzip-clean')
    data = b'clean artifact' if clean else secret.encode()
    if kind == 'gzip':
        data = gzip.compress(data, mtime=0)
    elif kind == 'zstd':
        data = zstd.compress(data)
    elif kind.startswith('zstd-stray-gzip-'):
        data = zstd.compress(b'profile record\x1f\x8bnot-a-gzip-member\n' + data)
    artifact = 'execution_log.binpb.zst' if kind.startswith('zstd') else 'artifact.log'
    (tmp_path / artifact).write_bytes(data)
    code = scanner.main([str(tmp_path), *(arg for name in names for arg in ('--env', name))])
    assert code == (0 if clean else 1)
    output = capsys.readouterr().out
    assert secret not in output
    if not clean:
        for name in names:
            assert name + ': secret detected' in output


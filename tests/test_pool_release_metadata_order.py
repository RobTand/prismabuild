"""Metadata cleanup must not strand physical tokens during rollback (#318)."""
from pathlib import Path
import errno
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool


@pytest.mark.parametrize("metadata", [pool.cpu_admission.METADATA, pool.gpu_admission.METADATA])
@pytest.mark.parametrize("error", [errno.EACCES, errno.ESTALE])
@pytest.mark.parametrize("operation", ["release", "abandon_acquire"])
def test_metadata_unlink_failure_does_not_strand_tokens(tmp_path, monkeypatch, metadata, error, operation):
    ledger = pool.PoolQueue(tmp_path / "queue").ledger()
    ledger.ensure_capacity({"cpu": 2, "mem_gb": 1})
    key = "a" * 64
    handle = ledger.begin_acquire(key, {"cpu": 2, "mem_gb": 1})
    if operation == "release":
        assert ledger.commit_acquire(key, handle) == 3
        handle = key
    holder = ledger.held_dir / handle
    marker = holder / metadata
    marker.write_text('{}')
    original = Path.unlink

    def refuse_metadata(path, *args, **kwargs):
        if path == marker:
            raise OSError(error, "injected metadata unlink refusal", str(path))
        return original(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "unlink", refuse_metadata)
        with pytest.raises(OSError) as raised:
            getattr(ledger, operation)(handle)
        assert raised.value.errno == error
        assert ledger.held() == {}, "cleanup error stranded physical capacity"
        assert marker.exists(), "failed metadata cleanup must remain retryable"

    assert getattr(ledger, operation)(handle) == 0
    assert not holder.exists()
    assert ledger.acquire("b" * 64, {"cpu": 2, "mem_gb": 1})


def test_partial_token_return_retains_metadata_until_retry(tmp_path, monkeypatch):
    ledger = pool.PoolQueue(tmp_path / "queue").ledger()
    ledger.ensure_capacity({"cpu": 2})
    key = "a" * 64
    assert ledger.acquire(key, {"cpu": 2})
    holder = ledger.held_dir / key
    marker = holder / pool.cpu_admission.METADATA
    marker.write_text('{"borrowed_cpu": 1}')
    blocked = sorted(holder.glob("cpu-*"))[0]
    original = pool.os.rename

    def refuse_one_token(source, destination):
        if source == blocked:
            raise OSError(errno.ESTALE, "injected token rename refusal")
        return original(source, destination)

    with monkeypatch.context() as patch:
        patch.setattr(pool.os, "rename", refuse_one_token)
        assert ledger.release(key) == 1
        assert blocked.exists()
        assert marker.exists(), "partial release must retain adaptive accounting"

    assert ledger.release(key) == 1
    assert not holder.exists()

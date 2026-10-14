from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

from prismabuild import kept_reads, produced_output
from kept_read_clock import after_paths


def test_package_owner_imports_without_fleet_tools():
    source = Path(__file__).resolve().parents[1] / "src"
    command = (
        f"import sys; sys.path.insert(0, {str(source)!r}); "
        "from prismabuild import kept_reads; "
        "assert 'stage_move' not in sys.modules; "
        "assert 'stage_release' not in sys.modules; "
        "assert 'prewarm_loop' not in sys.modules; "
        "kept_reads.DirectoryRecords(); kept_reads.CensusMemo(); kept_reads.CensusIndex()"
    )
    subprocess.run([sys.executable, "-I", "-c", command], check=True)


def test_output_memo_defaults_to_fresh_reads_and_scope_resets(tmp_path, monkeypatch):
    scope = tmp_path / "scope"
    scope.mkdir()
    commitments = scope / "commitments.json"
    commitments.write_text(json.dumps({"batches": {"b": {"state": "filed"}}}))
    after_paths(monkeypatch, commitments, scope)
    monkeypatch.setattr(kept_reads, "_object_filesystem_type", lambda *a, **kw: "tmpfs")
    memo = produced_output._TickReads(None)
    memo.batches(scope)
    memo.batches(scope)
    assert (memo.commitments_read, memo.commitments_reused) == (2, 0)
    with pytest.raises(RuntimeError, match="interrupted"):
        with kept_reads.file_version_scope():
            memo.batches(scope)
            memo.batches(scope)
            assert (memo.commitments_read, memo.commitments_reused) == (3, 1)
            raise RuntimeError("interrupted")
    assert not kept_reads.file_versions_enabled()
    memo.batches(scope)
    assert (memo.commitments_read, memo.commitments_reused) == (4, 1)

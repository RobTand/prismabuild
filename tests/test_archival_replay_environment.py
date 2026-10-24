"""Archival replay refuses invalid scratch paths before launching a writer."""
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
from record_compat import archival_runtime, invoke


@pytest.mark.parametrize("tmpdir", [None, "", "relative/scratch"], ids=["unset", "empty", "relative"])
def test_archival_replay_requires_an_absolute_tmpdir(tmpdir, tmp_path, monkeypatch):
    if tmpdir is None:
        monkeypatch.delenv("TMPDIR", raising=False)
    else:
        monkeypatch.setenv("TMPDIR", tmpdir)
    store = tmp_path / "writer"
    with pytest.raises(ValueError, match="archival replay requires an absolute TMPDIR in the sealed environment"):
        invoke(tmp_path / "historical", "shape-table/unhashed", store,
               manifest=tmp_path / "manifest.json", adapter_path=tmp_path / "compat_adapters.py",
               archival_runtime=archival_runtime())
    assert not store.exists()

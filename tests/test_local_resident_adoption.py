import json
from pathlib import Path

import pytest

from prismabuild import pool
from test_local_resident_mover import world


def test_adoption_verifies_every_file_and_moves_without_copying(tmp_path):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    source = tmp_path / "manual"
    source.mkdir()
    (source / "weights").write_bytes(b"weights")
    inode = (source / "weights").stat().st_ino
    result = local_resident.adopt(store, record["set_id"], "test-host", spec, source)
    assert result["state"] == "resident"
    assert not source.exists()
    target = Path(result["local_root"]) / "weights"
    assert target.stat().st_ino == inode
    assert result["verification"][0]["sha256"] == record["manifest"]["entries"][0]["sha256"]
    assert pool.PoolQueue(store.queue_root).tier_ledger("local:test-host").held()["local_gib"] == 1


@pytest.mark.parametrize("defect", ["digest", "extra", "symlink"])
def test_adoption_refuses_wrong_bytes_or_incomplete_coverage_without_moving(tmp_path, defect):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    source = tmp_path / "manual"
    source.mkdir()
    (source / "weights").write_bytes(b"corrupt" if defect == "digest" else b"weights")
    if defect == "extra":
        (source / "extra").write_bytes(b"x")
    if defect == "symlink":
        (source / "weights").unlink()
        (source / "weights").symlink_to(Path(record["canonical_root"]) / "weights")
    with pytest.raises(ValueError):
        local_resident.adopt(store, record["set_id"], "test-host", spec, source)
    assert source.exists()
    assert store.read_copy(record["set_id"], "test-host")["state"] == "absent"


def test_adoption_refuses_cross_filesystem_before_rename(tmp_path, monkeypatch):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    source = tmp_path / "manual"
    source.mkdir()
    (source / "weights").write_bytes(b"weights")
    monkeypatch.setattr(local_resident, "same_filesystem", lambda *_: False, raising=False)
    with pytest.raises(ValueError, match="same filesystem"):
        local_resident.adopt(store, record["set_id"], "test-host", spec, source)
    assert source.exists()


def test_adoption_cli_queues_the_owner_action_and_never_hashes_on_coordinator(tmp_path, monkeypatch, capsys):
    import pbresident
    store, record, spec = world(tmp_path)
    observed = []
    def submit(store, set_id, **kwargs):
        observed.append(kwargs)
        return {"action_key": "a" * 64}
    monkeypatch.setattr(pbresident, "submit_adoption", submit, raising=False)
    assert pbresident.main(["--pool-root", str(store.queue_root), "adopt", record["set_id"],
        "--host", "test-host", "--source", str(tmp_path / "manual")]) == 0
    assert observed[0]["host"] == "test-host"
    assert observed[0]["source"] == str(tmp_path / "manual")
    assert json.loads(capsys.readouterr().out)["action_key"] == "a" * 64

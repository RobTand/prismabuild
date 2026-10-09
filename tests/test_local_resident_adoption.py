import json
from pathlib import Path

import pytest

from prismabuild import pool
from test_local_resident_mover import world

from local_resident_space import roomy_disk  # noqa: F401

pytestmark = pytest.mark.usefixtures("roomy_disk")


def test_adoption_verifies_every_file_and_moves_without_copying(tmp_path):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    source = tmp_path / "manual"
    source.mkdir()
    (source / "weights").write_bytes(b"weights")
    inode = (source / "weights").stat().st_ino
    result = local_resident.adopt_resident_copy(store, record["set_id"], "test-host", spec, source, now=120)
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
        local_resident.adopt_resident_copy(store, record["set_id"], "test-host", spec, source, now=120)
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
        local_resident.adopt_resident_copy(store, record["set_id"], "test-host", spec, source, now=120)
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


def _refused_adoption_keeps_its_reservation(tmp_path, monkeypatch, defect, *, release_on_error=False):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    set_id = record["set_id"]
    ledger = pool.PoolQueue(store.queue_root).tier_ledger("local:test-host")
    ledger.release(set_id)  # This fixture has neither local bytes nor an existing hold.
    source = tmp_path / "manual"
    source.mkdir()
    payload = b"corrupt" if defect == "wrong_bytes" else b"weights"
    (source / "weights").write_bytes(payload)
    inode = (source / "weights").stat().st_ino
    expected = {"wrong_bytes": "sha256", "cross_filesystem": "same filesystem", "mounted_source": "global bind mount"}
    if defect == "cross_filesystem":
        monkeypatch.setattr(local_resident, "same_filesystem", lambda *_: False)
    elif defect == "mounted_source":
        original_read = Path.read_text

        def mountinfo(path, *args, **kwargs):
            if str(path) == "/proc/self/mountinfo":
                return f"1 0 0:1 / {source} rw - ext4 /dev/test rw\n"
            return original_read(path, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", mountinfo)
    function = local_resident.adopt_resident_copy
    if release_on_error:
        original = function

        def release_mutant(*args, **kwargs):
            try:
                return original(*args, **kwargs)
            except ValueError:
                ledger.release(set_id)
                raise

        function = release_mutant
    with pytest.raises(ValueError, match=expected[defect]):
        function(store, set_id, "test-host", spec, source, now=120)
    assert store.read_copy(set_id, "test-host")["state"] == "absent"
    assert not Path(spec["root"]).joinpath(set_id).exists()
    assert (source / "weights").read_bytes() == payload
    assert (source / "weights").stat().st_ino == inode
    assert ledger.holder_tokens(set_id).get("local_gib", 0) == 1, "refused adoption must retain its reservation"


@pytest.mark.parametrize("defect", ["wrong_bytes", "cross_filesystem", "mounted_source"])
def test_refused_adoption_retains_its_lease_bounded_reservation(tmp_path, monkeypatch, defect):
    _refused_adoption_keeps_its_reservation(tmp_path, monkeypatch, defect)


@pytest.mark.parametrize("defect", ["wrong_bytes", "cross_filesystem", "mounted_source"])
def test_releasing_on_adoption_failure_mutant_fails_retention(tmp_path, monkeypatch, defect):
    with pytest.raises(AssertionError, match="refused adoption must retain its reservation"):
        _refused_adoption_keeps_its_reservation(tmp_path, monkeypatch, defect, release_on_error=True)


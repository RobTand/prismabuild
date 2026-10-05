import hashlib
import json

import pytest

from prismabuild import core


def source(tmp_path):
    root = tmp_path / "canonical"
    root.mkdir()
    (root / "weights").write_bytes(b"weights")
    manifest = {"schema": core.DATA_MANIFEST_SCHEMA_V1,
                "produced_by": {}, "annotations": {}, "mount_prefix": str(root),
                "entries": [{"path": str(root / "weights"), "offset": 0,
                             "bytes": 7, "sha256": hashlib.sha256(b"weights").hexdigest()}],
                "entry_count": 1, "total_bytes": 7}
    return root, manifest


def publish(tmp_path, **changes):
    from prismabuild import resident_sets as rs
    from prismabuild import pool
    pool.PoolQueue(tmp_path / "queue").mint_tier_capacity("local:test-host", {"local_gib": 1})
    root, manifest = source(tmp_path)
    args = dict(manifest=manifest, canonical_root=str(root), hosts=["test-host"],
                lease={"until": 150, "hard_max": 200}, created_by="test", now=100)
    args.update(changes)
    store = rs.ResidentSets(tmp_path / "queue")
    return store, store.publish(**args)


def test_set_body_is_immutable_and_release_only_appends_lease(tmp_path):
    store, record = publish(tmp_path)
    body = store.set_path(record["set_id"]).read_bytes()
    store.release(record["set_id"], by="operator", now=110)
    assert store.set_path(record["set_id"]).read_bytes() == body
    status = store.status(record["set_id"])
    assert [row["event"] for row in status["lease_log"]] == ["published", "released"]
    assert all(row["manifest_sha256"] == record["set_id"] for row in status["lease_log"])
    assert status["copies"]["test-host"]["state"] == "absent"


@pytest.mark.parametrize("defect", ["digest", "coverage", "range", "maximum", "symlink"])
def test_publish_refuses_invalid_set_before_filing(tmp_path, defect):
    from prismabuild import resident_sets as rs
    root, manifest = source(tmp_path)
    lease = {"until": 150, "hard_max": 200}
    if defect == "digest":
        manifest["entries"][0]["sha256"] = None
    elif defect == "coverage":
        (root / "extra").write_bytes(b"x")
    elif defect == "range":
        manifest["entries"][0]["offset"] = 1
    elif defect == "maximum":
        lease.pop("hard_max")
    else:
        (root / "weights").unlink()
        (root / "weights").symlink_to(tmp_path / "elsewhere")
    store = rs.ResidentSets(tmp_path / "queue")
    with pytest.raises((ValueError, core.ActionContractError)):
        store.publish(manifest=manifest, canonical_root=str(root), hosts=["test-host"],
                      lease=lease, created_by="test", now=100)
    assert not list((tmp_path / "queue").rglob("body.json"))


def test_cli_publish_status_release_and_policy_publication(tmp_path, capsys):
    import pbresident
    import publish_runtime
    from prismabuild import pool
    pool.PoolQueue(tmp_path / "queue").mint_tier_capacity("local:test-host", {"local_gib": 1})
    root, manifest = source(tmp_path)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    common = ["--pool-root", str(tmp_path / "queue")]
    assert pbresident.main(common + ["publish", "--manifest", str(path),
        "--canonical-root", str(root), "--hosts", "test-host",
        "--lease-until", "2099-01-01T00:00:00Z",
        "--hard-max", "2099-01-02T00:00:00Z"]) == 0
    record = json.loads(capsys.readouterr().out)
    assert pbresident.main(common + ["status", record["set_id"]]) == 0
    assert json.loads(capsys.readouterr().out)["copies"]["test-host"]["state"] == "absent"
    assert pbresident.main(common + ["release", record["set_id"]]) == 0
    assert "local_tier_policy.json" in publish_runtime.FLEET_DATA
    assert "pbresident.py" in publish_runtime.FLEET_SCRIPTS

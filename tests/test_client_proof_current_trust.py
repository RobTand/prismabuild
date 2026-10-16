"""Equal client digests must not prevent repair of an untrusted proof file."""
import json
from pathlib import Path
import runpy

import pytest

from client_proof_bridge_fixture import ROOT, _fresh, _sha, publication
from test_client_proof_prior_trust import (
    PRIOR_FILES, _file_facts, _retain_proof, root_publication, tmp_path,
)


def _member_metadata(install, names):
    result = {}
    for name in names:
        info = (install / name).stat()
        result[name] = (info.st_dev, info.st_ino, info.st_uid, info.st_gid,
                        info.st_mode, info.st_mtime_ns, info.st_ctime_ns)
    return result


@pytest.mark.parametrize("kind", ["worker-owned", "worker-writable", "trusted"])
def test_equal_client_bytes_repair_untrusted_proof_and_preserve_trusted_current(root_publication, kind):
    generation, install, state, config_path, backend_path, _ = root_publication
    config = json.loads(config_path.read_text())
    program = runpy.run_path(str(install / "upgrade_client.py"), run_name="current_proof_reader")
    desired, blobs = program["desired"](config)
    assert set(blobs) == {
        "resource_broker.py", "resource_payload.py", "gpu_memory.py", "gpu_capacity.py",
        "upgrade_client.py", "park_proof.py",
    }
    assert blobs["upgrade_client.py"] == (ROOT / "tools/fleet/upgrade_client.py").read_bytes()
    assert blobs["park_proof.py"] == (ROOT / "src/prismabuild/park_proof.py").read_bytes()
    for name, data in blobs.items():
        target = install / name
        target.write_bytes(data)
        target.chmod(0o644)
    leaf, _ = _retain_proof(root_publication, kind)
    retained = blobs["park_proof.py"]
    leaf.write_bytes(retained)
    assert leaf.read_bytes() == retained and _file_facts(leaf) == PRIOR_FILES[kind]
    # These independently owned broker hashes come from the actual six-member
    # installation. The updater and proof are deliberately absent from the reply.
    backend = json.loads(backend_path.read_text())
    backend["loaded"] = {name: _sha((install / name).read_bytes()) for name in (
        "resource_broker.py", "resource_payload.py", "gpu_memory.py", "gpu_capacity.py")}
    assert backend["holder"] is None and backend["active"] == 0 and backend["running"] is True
    backend_path.write_text(json.dumps(backend))
    actual_hashes = {name: _sha((install / name).read_bytes()) for name in blobs}
    assert actual_hashes == desired["files"]
    before_metadata = _member_metadata(install, blobs)
    operations_before = list(backend["operations"])
    publication_receipt = (generation / "RUNTIME_VERSION.json").read_bytes()
    parked = Path(config["maintenance_gate"]).parent / "rollout/parked"
    assert not parked.exists()
    assert not (install / "RUNTIME_VERSION.json").exists()

    observed = _fresh(root_publication)
    assert observed["exception"] is None, observed
    expected = "current" if kind == "trusted" else "updated"
    assert observed["result"]["state"] == expected, (
        "equal digests must not certify worker-controlled proof metadata as current", observed)
    assert {name: (install / name).read_bytes() for name in blobs} == blobs
    assert (generation / "RUNTIME_VERSION.json").read_bytes() == publication_receipt
    assert not (state / "transaction.json").exists()
    assert list(parked.iterdir()) == []
    backend = json.loads(backend_path.read_text())
    operations = backend["operations"][len(operations_before):]
    assert backend["holder"] is None and backend["active"] == 0 and backend["running"] is True
    if kind == "trusted":
        assert operations == ["maintenance_status"]
        assert _member_metadata(install, blobs) == before_metadata
        assert not (state / "staged").exists() and not (state / "previous").exists()
    else:
        assert all(operations.count(name) == 1 for name in (
            "maintenance_begin", "stop", "start", "maintenance_end"))
        assert (state / "staged/park_proof.py").read_bytes() == retained
        assert (state / "previous/park_proof.py").read_bytes() == retained
        assert _file_facts(leaf) == (0, 0, 0o644)
    assert _fresh(root_publication, "trust")["result"]["trusted"] is True

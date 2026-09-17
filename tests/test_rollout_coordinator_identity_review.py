"""Review regression: an epoch must bind the coordinator revision that drives it."""
from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path

import pytest

from test_rollout_coordinator import arm, fleet, post_host, publisher


ROOT = Path(__file__).resolve().parents[1]


def _load_revision_b(tmp_path: Path):
    """Load a valid coordinator with observably different transition semantics."""
    source = (ROOT / "tools/fleet/publish_runtime.py").read_text()
    old = '"state": "resume_authorized", "complete": False'
    new = '"state": "resume_authorized_by_revision_b", "complete": False'
    assert old in source
    revised = source.replace(old, new, 1)
    path = tmp_path / "revision-b" / "tools/fleet/publish_runtime.py"
    path.parent.mkdir(parents=True)
    path.write_text(revised)
    spec = importlib.util.spec_from_file_location("rollout_coordinator_revision_b", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, path


def test_import_does_not_read_coordinator_source(tmp_path, monkeypatch):
    original = Path.read_bytes

    def refuse_source_read(path):
        if path.name == "publish_runtime.py":
            raise AssertionError("coordinator source read during import")
        return original(path)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "read_bytes", refuse_source_read)
        module, path = _load_revision_b(tmp_path)
    assert module.COORDINATOR_SHA256 is None
    assert module._coordinator_sha256() == hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize("capture_first", [False, True])
def test_coordinator_refuses_source_mutation_since_import(tmp_path, capture_first):
    module, path = _load_revision_b(tmp_path)
    if capture_first:
        module._coordinator_sha256()
    path.write_bytes((ROOT / "tools/fleet/publish_runtime.py").read_bytes())
    with pytest.raises(SystemExit, match="coordinator source changed"):
        module._coordinator_sha256()


def test_resume_refuses_a_coordinator_revision_not_bound_by_the_intent(
    fleet, tmp_path, monkeypatch
):
    """A later checkout must not take over a retained epoch's decision chain."""
    epoch = arm(fleet)
    for host in ("one", "two"):
        post_host(epoch, host, "drained")
    assert publisher._barrier_step(epoch)["state"] == "activated"
    for host in ("one", "two"):
        post_host(epoch, host, "rotated")

    revision_b, revision_b_path = _load_revision_b(tmp_path)
    revision_b.MIRROR = publisher.MIRROR
    revision_b.CHECKOUT = ROOT
    revision_b.FINAL_BARRIER_QUALIFICATION_GUARD = False
    revision_a_hash = hashlib.sha256(
        (ROOT / "tools/fleet/publish_runtime.py").read_bytes()
    ).hexdigest()
    revision_b_hash = hashlib.sha256(revision_b_path.read_bytes()).hexdigest()
    assert revision_a_hash != revision_b_hash

    with pytest.raises(SystemExit, match="coordinator.*(identity|hash|revision)"):
        revision_b._barrier_step(epoch)
    assert "resume.json" not in publisher._rollout_view(epoch)["markers"]


def test_identity_check_binds_the_exact_helper_bytes_it_executes(
    fleet, tmp_path, monkeypatch
):
    """A path restored to revision A must not bless revision B's loaded code."""
    epoch = arm(fleet)
    view = publisher._rollout_view(epoch)
    checkout = tmp_path / "helper-race"
    helper = checkout / "tools/fleet/upgrade_client.py"
    helper.parent.mkdir(parents=True)
    revision_a = (ROOT / "tools/fleet/upgrade_client.py").read_bytes()
    helper.write_bytes(revision_a)
    revision_b = revision_a.replace(
        b"CLIENT_UPGRADE_ROLLOUT_PROTOCOL = 1",
        b"CLIENT_UPGRADE_ROLLOUT_PROTOCOL = 2",
        1,
    )
    assert revision_b != revision_a
    monkeypatch.setattr(publisher, "CHECKOUT", checkout)

    original_read_bytes = Path.read_bytes

    def raced_helper_read(path):
        # `_agent_definitions` observes and executes B. The pathname remains A,
        # exactly as after an atomic B -> A replacement before the later hashes.
        if path == helper:
            return revision_b
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", raced_helper_read)
    with pytest.raises(SystemExit, match="marker semantics.*(identity|hash|differ)"):
        publisher._assert_coordinator_identity(view)

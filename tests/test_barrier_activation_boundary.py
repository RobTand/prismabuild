"""Historical attestations cannot authorize a runtime swap (#458, #469).

Exercise both publisher entry paths against a private generation store. The
rollback case extends the retained #470 maintenance regression; the publication
case also proves that an unsupported barrier cannot stage a generation.
"""
import hashlib

import pytest

from test_the_coordinator_proves_the_fleet_before_a_barrier import (
    OTHER, ROOT, _generation, fleet, post, publish_runtime, upgrade,
)


@pytest.mark.parametrize("operation", ["publish", "activate"])
def test_default_barrier_refuses_mutation_before_history_io(fleet, monkeypatch, operation):
    """The safe default cannot stage, swap, or enumerate attestations."""

    target = _generation(fleet, "a" * 64)
    previous = target.parent / "previous"
    previous.mkdir()
    publish_runtime.MIRROR.rmdir()
    publish_runtime.MIRROR.symlink_to(previous, target_is_directory=True)
    before = set(target.parent.iterdir())
    argv = ["publish_runtime.py"]
    if operation == "activate":
        argv += ["--activate-generation", target.name]
    monkeypatch.setattr(publish_runtime.sys, "argv", argv)
    monkeypatch.setattr(
        publish_runtime, "_require_attested_fleet",
        lambda *_args: pytest.fail("mutating default read historical attestations"),
    )

    with pytest.raises(SystemExit, match="barrier activation is not implemented"):
        publish_runtime.main()
    assert publish_runtime.MIRROR.resolve() == previous
    assert set(target.parent.iterdir()) == before


@pytest.mark.parametrize("operation", ["publish", "activate"])
@pytest.mark.parametrize("later_version", [False, True])
def test_barrier_refuses_mutation_even_with_matching_history(
    fleet, monkeypatch, operation, later_version,
):
    source = ROOT / "tools/fleet/upgrade_client.py"
    sha = hashlib.sha256(source.read_bytes()).hexdigest()
    target = _generation(fleet, sha)
    for host in ("sparky", "sparklina"):
        post(fleet, host, sha)
        if later_version:
            post(fleet, host, OTHER)
    previous = target.parent / "previous"
    previous.mkdir()
    publish_runtime.MIRROR.rmdir()
    publish_runtime.MIRROR.symlink_to(previous, target_is_directory=True)
    before = set(target.parent.iterdir())
    argv = ["publish_runtime.py", "--rollout", "barrier"]
    if operation == "activate":
        argv += ["--activate-generation", target.name]
    else:
        # Source identity/probing are not this regression's subject. Staging,
        # receipt writing, sealing and activation remain the real operations.
        member = upgrade.MEMBERS["upgrade_client.py"]
        monkeypatch.setattr(publish_runtime, "_commit_identity", lambda: "c" * 40)
        monkeypatch.setattr(publish_runtime, "_working_tree_dirty", lambda: False)
        monkeypatch.setattr(publish_runtime, "_publication_manifest", lambda: {member: sha})
        monkeypatch.setattr(publish_runtime, "_git_index_modes", lambda: {})
        monkeypatch.setattr(publish_runtime, "_source_for", lambda _: source)
        monkeypatch.setattr(publish_runtime, "_probe", lambda _: None)
    monkeypatch.setattr(publish_runtime.sys, "argv", argv)

    with pytest.raises(SystemExit, match="barrier activation is not implemented"):
        publish_runtime.main()
    assert publish_runtime.MIRROR.resolve() == previous
    assert set(target.parent.iterdir()) == before


@pytest.mark.parametrize("operation", ["publish", "activate"])
def test_barrier_dry_run_reports_history_without_proving_participation(
    fleet, monkeypatch, capsys, operation,
):
    # A normal publisher need not inherit the worker's bytecode-disable flag.
    monkeypatch.setattr(publish_runtime.sys, "dont_write_bytecode", False)
    monkeypatch.setattr(publish_runtime.sys, "pycache_prefix", None)
    source_files = set(publish_runtime.CHECKOUT.rglob("*"))
    sha = "a" * 64
    target = _generation(fleet, sha)
    for host in ("sparky", "sparklina"):
        post(fleet, host, sha)
        post(fleet, host, OTHER)
    before = set(target.parent.iterdir())
    argv = ["publish_runtime.py", "--rollout", "barrier", "--dry-run"]
    if operation == "activate":
        argv += ["--activate-generation", target.name]
    else:
        monkeypatch.setattr(publish_runtime, "_commit_identity", lambda: "c" * 40)
        monkeypatch.setattr(publish_runtime, "_working_tree_dirty", lambda: False)
        monkeypatch.setattr(publish_runtime, "_publication_manifest", lambda: {
            upgrade.MEMBERS["upgrade_client.py"]: sha})
        monkeypatch.setattr(publish_runtime, "_git_index_modes", lambda: {})
    monkeypatch.setattr(publish_runtime.sys, "argv", argv)

    assert publish_runtime.main() == 0
    output = capsys.readouterr().out
    assert "historical attestations" in output
    assert "current participation" in output
    assert publish_runtime.MIRROR.is_dir() and not publish_runtime.MIRROR.is_symlink()
    assert set(target.parent.iterdir()) == before
    assert set(publish_runtime.CHECKOUT.rglob("*")) == source_files

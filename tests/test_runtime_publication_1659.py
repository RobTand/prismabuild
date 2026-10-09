"""Publication authority for movement roles, and its fallback (#1579, #1659).

A host that holds the root-owned copy of the generation it runs can tell
PrismaBuild's own movement tools from other work; a host that does not keeps the
behaviour it had before roles existed.  These tests cover both halves of
``runtime_publication``: the root unit's publication and convergence, and the
fleet side's questions (``published_member``, ``live_authority``,
``protected_counterpart``, ``protected_tools_dir``).

``publication_store`` models root custody with the unprivileged test user; the
Docker smoke of the root program uses a real root UID.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path

import pytest

from movement_publication_support import (
    GENERATION, approve, retained_generation, seal, unseal)
from prismabuild import runtime_publication as publication


@contextmanager
def as_root(monkeypatch):
    with monkeypatch.context() as authority:
        authority.setattr(publication.os, "geteuid", lambda: 0)
        yield


def source_generation(tmp_path, name=GENERATION):
    return retained_generation(tmp_path / "runtime-generations", name)


def digest(source):
    return hashlib.sha256((source / "RUNTIME_VERSION.json").read_bytes()).hexdigest()


def tool(root, script="stage_release.py"):
    return root / "tools" / "fleet" / script


def test_a_submitter_cannot_publish_an_authority_record(tmp_path, monkeypatch, publication_store):
    source = source_generation(tmp_path)
    monkeypatch.setattr(publication.os, "geteuid", lambda: 1000)
    with pytest.raises(PermissionError, match="root authority"):
        publication.publish_generation(source, receipt_sha256=digest(source))
    assert not publication_store.exists()


@pytest.mark.parametrize("umask", [0o002, 0o077])
def test_publication_preserves_custody_under_the_callers_umask(
        tmp_path, monkeypatch, publication_store, umask):
    previous = os.umask(umask)
    try:
        protected = approve(source_generation(tmp_path), monkeypatch)
    finally:
        os.umask(previous)
    assert publication.published_member(tool(protected)) == tool(protected)


def test_publication_copies_imports_and_binds_an_independent_record(
        tmp_path, monkeypatch, publication_store):
    source = source_generation(tmp_path)
    protected = approve(source, monkeypatch)
    member = tool(protected)
    assert publication.published_member(member) == member
    # A store owner can alter both source code and its own receipt, but not the copy.
    original = member.read_bytes()
    unseal(source.parent)
    tool(source).chmod(0o644)
    tool(source).write_text("raise SystemExit(73)\n")
    (source / "src" / "prismabuild" / "helper.py").chmod(0o644)
    (source / "src" / "prismabuild" / "helper.py").write_text("VALUE = 73\n")
    assert member.read_bytes() == original
    assert (protected / "src" / "prismabuild" / "helper.py").read_text() == "VALUE = 1\n"
    assert publication.published_member(tool(source)) is None
    with pytest.raises(FileExistsError, match="append-only"):
        approve(source, monkeypatch)


@pytest.mark.parametrize("fault", ["missing-record", "changed-receipt", "changed-member",
                                  "writable-parent", "symlink-record"])
def test_unknown_or_changed_publication_cannot_grant_a_role(
        tmp_path, monkeypatch, publication_store, fault):
    protected = approve(source_generation(tmp_path), monkeypatch)
    member = tool(protected)
    assert publication.published_member(member) == member  # Populate the verification cache.
    if fault == "missing-record":
        protected.chmod(0o755)
        (protected / publication.PUBLICATION_RECORD).unlink()
    elif fault == "symlink-record":
        protected.chmod(0o755)
        record = protected / publication.PUBLICATION_RECORD
        copied = tmp_path / "forged.json"
        copied.write_bytes(record.read_bytes())
        record.unlink()
        record.symlink_to(copied)
    elif fault == "writable-parent":
        publication_store.chmod(0o777)
    else:
        target = member if fault == "changed-member" else protected / "RUNTIME_VERSION.json"
        before = target.stat()
        raw = target.read_bytes()
        target.chmod(0o644)
        # Preserve length and mtime. Ctime and receipt binding must defeat a stale verdict.
        target.write_bytes(raw.replace(b"s", b"z", 1) if fault == "changed-member"
                           else raw.replace(b"c", b"d", 1))
        target.chmod(0o444)
        os.utime(target, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert publication.published_member(member) is None


@pytest.mark.parametrize("fault", ["receipt-digest", "member-digest", "member-symlink", "traversal"])
def test_publication_refuses_changed_or_escaping_source(
        tmp_path, monkeypatch, publication_store, fault):
    source = source_generation(tmp_path)
    unseal(source.parent)
    receipt_path = source / "RUNTIME_VERSION.json"
    selected = digest(source)
    if fault == "receipt-digest":
        selected = "f" * 64
    elif fault == "member-digest":
        tool(source).chmod(0o644)
        tool(source).write_text("raise SystemExit(73)\n")
    elif fault == "member-symlink":
        member = tool(source)
        alias = tmp_path / "payload.py"
        alias.write_bytes(member.read_bytes())
        member.unlink()
        member.symlink_to(alias)
    else:
        receipt = json.loads(receipt_path.read_bytes())
        receipt["files"]["../payload.py"] = "f" * 64
        receipt_path.chmod(0o644)
        receipt_path.write_text(json.dumps(receipt))
        selected = digest(source)
    with as_root(monkeypatch):
        with pytest.raises(ValueError):
            publication.publish_generation(source, receipt_sha256=selected)
    assert not (publication_store / source.name).exists()
    assert not list(publication_store.glob(".publication-*")), "a refused copy leaves no staging"


def test_actual_custody_rejects_a_submitter_path(tmp_path):
    # A non-root owner or the writable /tmp ancestor defeats custody.
    assert publication._protected_path(tmp_path) is False


def test_a_production_shaped_receipt_publishes(tmp_path, monkeypatch, publication_store):
    """The live receipts carry rollout and gate keys, dotted directories and executables."""
    source = retained_generation(tmp_path / "runtime-generations")
    unseal(source.parent)
    hook = source / ".githooks" / "pre-push"
    hook.parent.mkdir()
    hook.write_text("#!/bin/sh\n")
    hook.chmod(0o755)
    receipt = json.loads((source / "RUNTIME_VERSION.json").read_text())
    receipt["files"][".githooks/pre-push"] = hashlib.sha256(hook.read_bytes()).hexdigest()
    receipt.update(rollout="rolling", rollout_reason="why", dirty=False, published_by="celestia",
                   published_unix=1.0, shape_gate={"verdict": "passed"})
    (source / "RUNTIME_VERSION.json").chmod(0o644)
    (source / "RUNTIME_VERSION.json").write_text(json.dumps(receipt))
    seal(source.parent)
    protected = approve(source, monkeypatch)
    assert (protected / ".githooks" / "pre-push").stat().st_mode & 0o777 == 0o555
    assert publication.published_member(tool(protected)) == tool(protected)


# --- spelling, the question a publishing box can answer ---------------------------

def test_a_tool_is_spelled_as_a_member_of_the_protected_store_or_it_is_not(publication_store):
    base = str(publication_store / GENERATION)
    assert publication.spelled_member(f"{base}/tools/stage_release.py")
    assert publication.spelled_member(f"{base}/tools/fleet/stage_release.py")
    for refused in (
            f"{base}/tools/fleet/deeper/stage_release.py",       # not a tool path
            f"{base}/src/prismabuild/core.py",                   # not a tool
            f"{base}/tools/../tools/stage_release.py",           # not normalized
            f"{publication_store}//{GENERATION}/tools/stage_release.py",
            f"{publication_store.parent}/other/{GENERATION}/tools/stage_release.py",
            str(publication_store / ".hidden" / "tools" / "stage_release.py"),
            "tools/stage_release.py", "", None, 7):
        assert not publication.spelled_member(refused), refused


# --- what a host knows about itself ----------------------------------------------

def test_a_host_with_the_copy_of_its_generation_has_authority(
        tmp_path, monkeypatch, publication_store):
    source = source_generation(tmp_path)
    monkeypatch.setattr(publication, "executing_generation", lambda: source)
    assert publication.live_authority() is False, "no copy yet"
    approve(source, monkeypatch)
    assert publication.live_authority() is True


def test_a_checkout_has_no_authority(monkeypatch, publication_store):
    assert publication.executing_generation() is None
    assert publication.live_authority() is False


def test_a_copy_of_another_generation_gives_no_authority(
        tmp_path, monkeypatch, publication_store):
    """Stale: the copy of the last generation does not cover the one this process runs."""
    old = source_generation(tmp_path)
    approve(old, monkeypatch)
    live = retained_generation(tmp_path / "runtime-generations", "d" * 12 + "-1791400000-" + "e" * 12)
    monkeypatch.setattr(publication, "executing_generation", lambda: live)
    assert publication.live_authority() is False
    approve(live, monkeypatch)
    assert publication.live_authority() is True


def test_a_copy_made_from_other_bytes_gives_no_authority(
        tmp_path, monkeypatch, publication_store):
    """The copy must come from this generation's receipt, not from one that shares its name."""
    source = source_generation(tmp_path)
    approve(source, monkeypatch)
    monkeypatch.setattr(publication, "executing_generation", lambda: source)
    assert publication.live_authority() is True
    unseal(source.parent)
    receipt = source / "RUNTIME_VERSION.json"
    receipt.chmod(0o644)
    receipt.write_text(receipt.read_text().replace("c" * 40, "d" * 40))
    receipt.chmod(0o444)
    assert publication.live_authority() is False


@pytest.mark.parametrize("fault", ["record", "receipt", "writable-store"])
def test_authority_follows_the_copy_as_it_changes(tmp_path, monkeypatch, publication_store, fault):
    source = source_generation(tmp_path)
    protected = approve(source, monkeypatch)
    monkeypatch.setattr(publication, "executing_generation", lambda: source)
    assert publication.live_authority() is True
    if fault == "writable-store":
        publication_store.chmod(0o777)
    else:
        protected.chmod(0o755)
        (protected / (publication.PUBLICATION_RECORD if fault == "record"
                      else "RUNTIME_VERSION.json")).unlink()
    assert publication.live_authority() is False


# --- what a box announces and what a sealer takes ----------------------------------

def test_the_protected_counterpart_needs_the_same_receipt(tmp_path, monkeypatch, publication_store):
    source = source_generation(tmp_path)
    protected = approve(source, monkeypatch)
    copied = tool(protected)
    assert publication.protected_counterpart(
        tool(source), retained_store=source.parent) == copied
    assert publication.protected_counterpart(copied, retained_store=source.parent) == copied
    unseal(source.parent)
    receipt = source / "RUNTIME_VERSION.json"
    receipt.chmod(0o644)
    receipt.write_bytes(receipt.read_bytes().replace(b"c", b"d", 1))
    assert publication.protected_counterpart(tool(source), retained_store=source.parent) is None


def test_a_box_announces_its_protected_tool_root_only_when_it_holds_the_copy(
        tmp_path, monkeypatch, publication_store):
    source = source_generation(tmp_path)
    own = source / "tools" / "fleet"
    assert publication.protected_tools_dir(own, retained_store=source.parent) is None
    protected = approve(source, monkeypatch)
    assert publication.protected_tools_dir(
        own, retained_store=source.parent) == protected / "tools" / "fleet"
    assert publication.protected_tools_dir(
        source / "tools", retained_store=source.parent) == protected / "tools"
    # A directory that is not a retained generation's has no twin.
    elsewhere = tmp_path / "checkout" / "tools" / "fleet"
    elsewhere.mkdir(parents=True)
    (elsewhere / "stage_release.py").write_text("#\n")
    assert publication.protected_tools_dir(elsewhere, retained_store=source.parent) is None


# --- the root unit: publication without a person -----------------------------------

@pytest.fixture
def enrolled(tmp_path, monkeypatch, publication_store):
    """The enrolled store, its live pointer and the root unit's settings."""
    source = source_generation(tmp_path)
    pointer = tmp_path / "repo"
    pointer.symlink_to(source)
    config = {"runtime": str(pointer), "generation_store": str(source.parent),
              "status": str(tmp_path / "state" / "status.json")}
    return source, pointer, config


def status(config):
    return json.loads(Path(config["status"]).read_text())


def test_the_root_unit_publishes_the_live_generation_and_then_finds_it_current(
        enrolled, monkeypatch, publication_store):
    source, pointer, config = enrolled
    with as_root(monkeypatch):
        first = publication.converge(config)
        second = publication.converge(config)
    assert (first["state"], first["generation"]) == ("published", source.name)
    assert (second["state"], second["generation"]) == ("current", source.name)
    assert status(config)["state"] == "current"
    assert publication.published_member(tool(publication_store / source.name)) is not None


def test_a_new_live_generation_is_published_without_a_person_and_the_old_copy_stays(
        enrolled, tmp_path, monkeypatch, publication_store):
    source, pointer, config = enrolled
    newer = retained_generation(source.parent, "d" * 12 + "-1791400000-" + "e" * 12,
                                commit="d" * 40)
    with as_root(monkeypatch):
        assert publication.converge(config)["generation"] == source.name
        pointer.unlink()
        pointer.symlink_to(newer)
        result = publication.converge(config)
    assert (result["state"], result["generation"]) == ("published", newer.name)
    assert sorted(entry.name for entry in publication_store.iterdir() if not entry.name.startswith(".")) == \
        sorted([source.name, newer.name])


@pytest.mark.parametrize("fault", ["outside-the-store", "not-a-generation", "no-pointer", "differing-copy"])
def test_the_root_unit_reports_a_refusal_and_never_raises(
        enrolled, tmp_path, monkeypatch, publication_store, fault):
    source, pointer, config = enrolled
    if fault == "outside-the-store":
        stray = retained_generation(tmp_path / "elsewhere", "f" * 12 + "-1791300000-" + "a" * 12)
        pointer.unlink()
        pointer.symlink_to(stray)
    elif fault == "not-a-generation":
        unseal(source.parent)
        (source / "RUNTIME_VERSION.json").unlink()
    elif fault == "no-pointer":
        pointer.unlink()
    else:
        approve(source, monkeypatch)
        unseal(publication_store)
        shared = publication_store / source.name
        shared.chmod(0o755)
        (shared / "RUNTIME_VERSION.json").chmod(0o644)
        (shared / "RUNTIME_VERSION.json").write_text(
            (shared / "RUNTIME_VERSION.json").read_text().replace("c" * 40, "9" * 40))
    with as_root(monkeypatch):
        result = publication.converge(config)
    assert result["state"] == "error" and result["error"], result
    assert status(config)["state"] == "error"
    if fault != "differing-copy":
        assert not (publication_store / source.name).exists()


def test_a_nearly_full_filesystem_is_an_error_and_publishes_nothing(
        enrolled, monkeypatch, publication_store):
    """The unit never fills the root filesystem; a host that cannot publish falls back."""
    source, pointer, config = enrolled
    real = os.statvfs

    def nearly_full(path, *args, **kwargs):
        result = real(path, *args, **kwargs)
        return os.statvfs_result((result.f_bsize, result.f_frsize, result.f_blocks,
                                  result.f_bfree, 1, result.f_files, result.f_ffree,
                                  result.f_favail, result.f_flag, result.f_namemax))

    with monkeypatch.context() as full, as_root(monkeypatch):
        full.setattr(os, "statvfs", nearly_full)
        result = publication.converge(config)
    assert result["state"] == "error" and "free" in result["error"], result
    assert not (publication_store / source.name).exists()
    with as_root(monkeypatch):
        assert publication.converge(config)["state"] == "published"


def test_an_interrupted_publication_leaves_no_staging_behind_the_next_one(
        enrolled, monkeypatch, publication_store):
    source, pointer, config = enrolled
    for directory in (publication_store.parent, publication_store):
        directory.mkdir(exist_ok=True)
        directory.chmod(0o755)
    debris = publication_store / ".publication-crashed"
    (debris / "tools").mkdir(parents=True)
    (debris / "tools" / "half.py").write_text("partial\n")
    debris.chmod(0o555)
    with as_root(monkeypatch):
        assert publication.converge(config)["state"] == "published"
    assert not debris.exists()


def test_the_main_entry_converges_once_under_root_custody(
        enrolled, tmp_path, monkeypatch, publication_store, capsys):
    source, pointer, config = enrolled
    path = tmp_path / "etc" / "movement-publish.json"
    path.parent.mkdir()
    path.parent.chmod(0o755)
    path.write_text(json.dumps(config))
    path.chmod(0o644)
    monkeypatch.setattr(publication.os, "geteuid", lambda: 1000)
    with pytest.raises(SystemExit) as refused:
        publication.main(["--config", str(path)])
    assert refused.value.code == 2, "a submitter cannot run the root program"
    with as_root(monkeypatch):
        assert publication.main(["--config", str(path)]) == 0
        printed = json.loads(capsys.readouterr().out)
        assert (printed["state"], printed["generation"]) == ("published", source.name)
        path.write_text("not json")
        assert publication.main(["--config", str(path)]) == 1
        assert json.loads(capsys.readouterr().out)["state"] == "error"
        path.write_text(json.dumps({"runtime": "x"}))
        assert publication.main(["--config", str(path)]) == 1


# --- the installed program and its enrollment ---------------------------------------

INSTALLER = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "install_movement_publisher.sh"
PROGRAM = Path(publication.__file__).resolve()


def test_the_installed_program_runs_standalone_and_refuses_a_submitter():
    """It is installed alone under a root-owned directory and run with ``-I``: no package imports."""
    import subprocess
    import sys
    if os.geteuid() == 0:
        pytest.skip("the refusal is for a submitter")
    done = subprocess.run([sys.executable, "-I", str(PROGRAM), "--config", "/nonexistent"],
                          capture_output=True, text=True, timeout=60)
    assert done.returncode == 2, (done.stdout, done.stderr)
    assert "requires root authority" in done.stderr
    # No import of the rest of the package: the file stands alone.
    import ast
    imported = {alias.name.split(".")[0] for node in ast.walk(ast.parse(PROGRAM.read_text()))
                if isinstance(node, ast.Import) for alias in node.names}
    imported |= {node.module.split(".")[0] for node in ast.walk(ast.parse(PROGRAM.read_text()))
                 if isinstance(node, ast.ImportFrom) and node.module and node.level == 0}
    assert "prismabuild" not in imported
    assert not [node for node in ast.walk(ast.parse(PROGRAM.read_text()))
                if isinstance(node, ast.ImportFrom) and node.level > 0]


def test_the_installer_enrolls_what_the_program_reads():
    """One enrollment: the settings it writes are the settings the program requires."""
    import re
    import subprocess
    text = INSTALLER.read_text()
    assert subprocess.run(["bash", "-n", str(INSTALLER)], capture_output=True).returncode == 0
    config = json.loads(re.search(r"<<'CONFIG'\n(.*?)\nCONFIG\n", text, re.S).group(1))
    assert set(config) == {"runtime", "generation_store", "status"}
    assert str(publication.DEFAULT_CONFIG) in text
    assert str(publication.PROTECTED_GENERATION_STORE) in text
    # The unit runs the file the installer installs, isolated, and nothing from the shared mount.
    assert "ExecStart=/usr/bin/python3 -I /opt/prismabuild/runtime_publication.py" in text
    assert 'install -o root -g root -m 0644 "$module" /opt/prismabuild/runtime_publication.py' in text
    assert "OnUnitInactiveSec=60s" in text and "OnBootSec=" in text
    # The status file's directory exists before the first run, and the store's parent is root's.
    assert os.path.dirname(config["status"]) in text
    assert "/opt/prismabuild" in text and "movement-generations" in text
    # Custody is checked before anything is written, and the installer needs root.
    assert text.index("has no root custody") < text.index("install -d")
    assert text.index("must run as root") < text.index("install -d")


def test_the_settings_the_installer_writes_pass_the_programs_validation(
        tmp_path, monkeypatch, publication_store):
    import re
    config = json.loads(re.search(r"<<'CONFIG'\n(.*?)\nCONFIG\n", INSTALLER.read_text(), re.S).group(1))
    path = tmp_path / "etc" / "movement-publish.json"
    path.parent.mkdir()
    path.parent.chmod(0o755)
    path.write_text(json.dumps(config))
    path.chmod(0o644)
    assert publication._read_config(path) == config


def test_the_installer_and_the_program_travel_with_every_generation():
    """No checkout exists on three of the boxes: an administrator copies both out of the live generation."""
    import importlib.util
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "publish_runtime_for_1659", root / "tools" / "fleet" / "publish_runtime.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    manifest = module._publication_manifest()
    for member in ("tools/fleet/install_movement_publisher.sh", "tools/install_movement_publisher.sh",
                   "src/prismabuild/runtime_publication.py", "tests/movement_publication_support.py"):
        assert member in manifest, member

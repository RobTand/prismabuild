"""NEWFEATURE #1032 controls; never an OLD RED for an absent provisioner.

Run only inside an admitted PB action. Synthetic root metadata is serialization
and refusal coverage, not real privilege, kernel mount or reboot qualification.
"""
from __future__ import annotations

import configparser
import copy
import errno
import hashlib
import importlib.util
import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import pbstatus
import tier_loop
from prismabuild import pool, storage_tiers

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = "install_ram_mount_unit.py"
SIZE = 123456789
UID = 2345
GID = 3456
EXPECTED = (
    "[Unit]\nDescription=PrismaBuild RAM tier tmpfs\n\n"
    "[Mount]\nWhat=tmpfs\nWhere=/ram/prewarm\nType=tmpfs\n"
    "Options=size=123456789,noswap,mpol=interleave,uid=2345,gid=3456,mode=0755\n\n"
    "[Install]\nWantedBy=local-fs.target\n"
).encode("ascii")


@pytest.fixture
def generation(tmp_path):
    """Materialize actual publisher members, including the real validator."""
    import publish_runtime

    root = tmp_path / "generation"
    manifest = publish_runtime._publication_manifest()
    assert SCRIPT in publish_runtime.FLEET_SCRIPTS
    required = [f"{base}/{name}" for base in ("tools", "tools/fleet")
                for name in (SCRIPT, "runtime_paths.py", "ram_tier_policy.json")]
    assert all(name in manifest for name in required)
    members = required + [name for name in manifest if name.startswith("src/prismabuild/")]
    assert "src/prismabuild/storage_tiers.py" in members
    for name in members:
        source = publish_runtime._source_for(name)
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        assert hashlib.sha256(target.read_bytes()).hexdigest() == manifest[name]
    return root


def _arguments(generation, mode="--render"):
    return ["--policy", str(generation / "tools/ram_tier_policy.json"),
            "--size-bytes", str(SIZE), "--owner-uid", str(UID),
            "--owner-gid", str(GID), mode]


def _cli(generation, arguments=None, *, spelling="tools", isolated=True):
    return subprocess.run(
        [sys.executable, *(["-I", "-B"] if isolated else ["-B"]),
         str(generation / spelling / SCRIPT),
         *(_arguments(generation) if arguments is None else arguments)],
        cwd=generation.parent, capture_output=True, timeout=20,
        env={**os.environ, "PYTHONPATH": str(generation.parent / "poison"),
             "PYTHONPYCACHEPREFIX": str(generation.parent / "unwanted-bytecode")},
    )


def _snapshot(root):
    return {str(path.relative_to(root)): (path.lstat().st_mode,
                                        path.lstat().st_mtime_ns,
                                        path.read_bytes() if path.is_file() else None)
            for path in root.rglob("*")}


@pytest.mark.parametrize("spelling", ["tools", "tools/fleet"])
def test_published_actual_cli_renders_independently_expected_ini_without_writes(generation, spelling):
    poison = generation.parent / "poison"
    poison.mkdir()
    (poison / "runtime_paths.py").write_text("raise RuntimeError('inherited Python path')\n")
    before = _snapshot(generation.parent)
    result = _cli(generation, spelling=spelling)
    assert result.returncode == 0, result.stderr
    assert result.stderr == b""
    assert result.stdout == EXPECTED
    parsed = configparser.ConfigParser(interpolation=None)
    parsed.read_string(result.stdout.decode("ascii"))
    assert parsed.sections() == ["Unit", "Mount", "Install"]
    assert dict(parsed["Mount"]) == {
        "what": "tmpfs", "where": "/ram/prewarm", "type": "tmpfs",
        "options": "size=123456789,noswap,mpol=interleave,uid=2345,gid=3456,mode=0755"}
    assert dict(parsed["Install"]) == {"wantedby": "local-fs.target"}
    assert _snapshot(generation.parent) == before
    assert not (generation.parent / "unwanted-bytecode").exists()
    assert not list(generation.rglob("__pycache__"))


@pytest.mark.parametrize("flag", ["--size-bytes", "--owner-uid", "--owner-gid"])
@pytest.mark.parametrize("value", ["0", "-1", "+1", "01", "1.0", " 1", "1\n", "1,noswap", "1;echo x", "١"])
def test_actual_cli_rejects_noncanonical_numeric_inputs_before_effects(generation, flag, value):
    arguments = _arguments(generation)
    arguments[arguments.index(flag) + 1] = value
    before = _snapshot(generation.parent)
    result = _cli(generation, arguments)
    assert result.returncode != 0
    assert result.stdout == b""
    assert _snapshot(generation.parent) == before


@pytest.mark.parametrize("flag", ["--owner-uid", "--owner-gid"])
@pytest.mark.parametrize("value", ["4294967295", "4294967296", "999999999999999999999999"])
def test_actual_cli_refuses_unusable_owner_ids(generation, flag, value):
    arguments = _arguments(generation)
    arguments[arguments.index(flag) + 1] = value
    assert _cli(generation, arguments).returncode != 0


@pytest.mark.parametrize("flag", ["--policy", "--size-bytes", "--owner-uid", "--owner-gid", "--render"])
def test_actual_cli_requires_each_explicit_input_and_mode(generation, flag):
    arguments = _arguments(generation)
    index = arguments.index(flag)
    del arguments[index:index + (1 if flag == "--render" else 2)]
    assert _cli(generation, arguments).returncode != 0


@pytest.mark.parametrize("extra", [["--render"], ["--install"], ["--size-bytes", "1"],
                                   ["--owner-uid", "1"], ["--owner-gid", "1"],
                                   ["--install-root", "/"], ["--size", "1"], ["unexpected"]])
def test_actual_cli_refuses_duplicates_conflicting_modes_and_extra_options(generation, extra):
    assert _cli(generation, [*_arguments(generation), *extra]).returncode != 0


def test_actual_cli_refuses_duplicate_policy_option(generation):
    assert _cli(generation, [*_arguments(generation), "--policy",
                            str(generation / "tools/ram_tier_policy.json")]).returncode != 0


@pytest.mark.parametrize("value,accepted", [(256 * (1 << 30), True), (256 * (1 << 30) + 1, False)])
def test_actual_cli_policy_ceiling_is_a_bound_not_a_size_default(generation, value, accepted):
    arguments = _arguments(generation)
    arguments[arguments.index("--size-bytes") + 1] = str(value)
    result = _cli(generation, arguments)
    assert (result.returncode == 0) == accepted
    if accepted:
        assert f"size={value},".encode() in result.stdout


def test_actual_cli_accepts_last_usable_owner_ids(generation):
    arguments = _arguments(generation)
    for flag in ("--owner-uid", "--owner-gid"):
        arguments[arguments.index(flag) + 1] = "4294967294"
    result = _cli(generation, arguments)
    assert result.returncode == 0, result.stderr
    assert b"uid=4294967294,gid=4294967294" in result.stdout


@pytest.mark.parametrize("change", [{"extra": 1}, {"ceiling_gib_max": True},
                                    {"ceiling_gib_max": 0}, {"schema": "wrong"},
                                    {"mountpoint": "/other"}, {"mountpoint": "/ram/prewarm\n[Install]"}])
def test_actual_cli_refuses_invalid_or_unsupported_policy_before_effects(generation, change):
    path = generation / "custom.json"
    value = json.loads((generation / "tools/ram_tier_policy.json").read_text())
    value.update(change)
    path.write_text(json.dumps(value))
    arguments = _arguments(generation)
    arguments[1] = str(path)
    before = _snapshot(generation.parent)
    result = _cli(generation, arguments)
    assert result.returncode != 0
    assert result.stdout == b""
    assert _snapshot(generation.parent) == before


@pytest.mark.parametrize("text", ["{", "[]", '{"schema":"wrong","schema":"also wrong"}'])
def test_actual_cli_refuses_malformed_and_ambiguous_policy(generation, text):
    path = generation / "bad.json"
    path.write_text(text)
    arguments = _arguments(generation)
    arguments[1] = str(path)
    assert _cli(generation, arguments).returncode != 0


@pytest.mark.parametrize("symlink", [False, True])
def test_actual_cli_refuses_policy_outside_pinned_generation(generation, symlink):
    outside = generation.parent / "outside.json"
    shutil.copyfile(generation / "tools/ram_tier_policy.json", outside)
    path = outside
    if symlink:
        path = generation / "escape.json"
        path.symlink_to(outside)
    arguments = _arguments(generation)
    arguments[1] = str(path)
    result = _cli(generation, arguments)
    assert result.returncode != 0
    assert b"pinned entrypoint generation" in result.stderr


def test_actual_cli_requires_isolated_interpreter(generation):
    result = _cli(generation, isolated=False)
    assert result.returncode != 0
    assert b"python3 -I -B" in result.stderr


@pytest.fixture
def installer(generation, tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("private_ram_installer", generation / "tools" / SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root = tmp_path / "installation"
    directory = root / "etc/systemd/system"
    directory.mkdir(parents=True)
    # The admitted worker's umask may be 002. These private fixture directories
    # model trusted root configuration; production's writable-dir refusal stays.
    for ancestor in (root, root / "etc", root / "etc/systemd", directory):
        ancestor.chmod(0o755)
        assert stat.S_IMODE(ancestor.stat().st_mode) == 0o755
    calls = []
    proxy = SimpleNamespace(**vars(os))
    real_lstat = Path.lstat

    def owned(metadata):
        values = list(metadata)
        values[4] = values[5] = 0
        return os.stat_result(values)

    def lstat(path, *args, **kwargs):
        metadata = real_lstat(path, *args, **kwargs)
        return owned(metadata) if path.is_relative_to(root) else metadata

    def fstat(fd):
        assert Path(os.readlink(f"/proc/self/fd/{fd}")).is_relative_to(root)
        return owned(os.fstat(fd))

    def descriptor_stat(path, *, dir_fd, follow_symlinks):
        assert Path(os.readlink(f"/proc/self/fd/{dir_fd}")).is_relative_to(root)
        return owned(os.stat(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks))

    def fchown(fd, uid, gid):
        assert Path(os.readlink(f"/proc/self/fd/{fd}")).is_relative_to(root)
        calls.append(("fchown", uid, gid))

    def systemctl(private_root, arguments):
        assert private_root == root and private_root != Path("/")
        calls.append(("/usr/bin/systemctl", *arguments))

    proxy.geteuid = lambda: 0
    proxy.fstat = fstat
    proxy.stat = descriptor_stat
    proxy.fchown = fchown
    monkeypatch.setattr(Path, "lstat", lstat)
    monkeypatch.setattr(module, "os", proxy)
    monkeypatch.setattr(module, "_INSTALL_ROOT", root)
    production_systemctl = module._systemctl
    monkeypatch.setattr(module, "_systemctl", systemctl)
    return SimpleNamespace(module=module, root=root, directory=directory,
                           unit=directory / "ram-prewarm.mount", calls=calls,
                           production_systemctl=production_systemctl)


def test_private_real_install_branch_commits_exact_bytes_and_repeats_without_rewrite(installer):
    obj = installer
    obj.module.persist_unit(EXPECTED)
    assert obj.unit.read_bytes() == EXPECTED
    assert stat.S_IMODE(obj.unit.stat().st_mode) == 0o644
    assert obj.calls == [("fchown", 0, 0), ("/usr/bin/systemctl", "daemon-reload"),
                         ("/usr/bin/systemctl", "enable", "ram-prewarm.mount")]
    before = obj.unit.stat()
    obj.calls.clear()
    obj.module.os.fchmod = lambda *args: pytest.fail("chmod of existing unit")
    obj.module.os.fchown = lambda *args: pytest.fail("chown of existing unit")
    obj.module.persist_unit(EXPECTED)
    after = obj.unit.stat()
    assert (before.st_ino, before.st_mtime_ns) == (after.st_ino, after.st_mtime_ns)
    assert obj.calls == [("/usr/bin/systemctl", "daemon-reload"),
                         ("/usr/bin/systemctl", "enable", "ram-prewarm.mount")]
    assert sorted(path.name for path in obj.directory.iterdir()) == ["ram-prewarm.mount"]


def test_private_actual_main_install_composes_real_policy_plan_and_persistence(installer):
    obj = installer
    assert obj.module.main(_arguments(obj.module._GENERATION, "--install")) == 0
    assert obj.unit.read_bytes() == EXPECTED
    assert obj.calls[-2:] == [("/usr/bin/systemctl", "daemon-reload"),
                             ("/usr/bin/systemctl", "enable", "ram-prewarm.mount")]


def test_private_main_render_never_queries_privilege_or_opens_install_files(installer, capsys):
    obj = installer
    obj.module.os.geteuid = lambda: pytest.fail("render queried installation privilege")
    obj.module.os.open = lambda *a, **kw: pytest.fail("render opened install descriptor")
    assert obj.module.main(_arguments(obj.module._GENERATION)) == 0
    assert capsys.readouterr().out.encode("ascii") == EXPECTED
    assert obj.calls == []
    assert not obj.unit.exists()


@pytest.mark.parametrize("failure", ["ceiling", "invalid-policy", "outside-policy"])
def test_private_main_install_validates_before_privileged_effects(installer, failure, capsys):
    obj = installer
    arguments = _arguments(obj.module._GENERATION, "--install")
    if failure == "ceiling":
        arguments[arguments.index("--size-bytes") + 1] = str(256 * (1 << 30) + 1)
    else:
        policy = (obj.module._GENERATION if failure == "invalid-policy" else obj.root) / "bad.json"
        policy.write_text("{}")
        arguments[1] = str(policy)
    obj.module.os.geteuid = lambda: pytest.fail("invalid input reached privilege query")
    obj.module.os.open = lambda *a, **kw: pytest.fail("invalid input opened install descriptor")
    assert obj.module.main(arguments) == 1
    assert capsys.readouterr().out == ""
    assert not obj.unit.exists()
    assert obj.calls == []


def test_private_production_runner_cannot_contact_live_manager(installer, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("private root contacted external systemctl")
    monkeypatch.setattr(installer.module.subprocess, "run", forbidden)
    with pytest.raises(ValueError, match="private installation root"):
        installer.production_systemctl(installer.root, ["daemon-reload"])


def test_install_nonroot_refuses_before_any_write_or_systemctl(installer):
    installer.module.os.geteuid = lambda: 2345
    installer.module.os.open = lambda *a, **kw: pytest.fail("opened install descriptor without root")
    with pytest.raises(PermissionError, match="effective UID 0"):
        installer.module.persist_unit(EXPECTED)
    assert not installer.unit.exists()
    assert installer.calls == []


@pytest.mark.parametrize("kind", ["different", "symlink", "directory", "fifo", "mode", "hardlink", "owner"])
def test_private_install_refuses_existing_unit_without_migration(installer, kind):
    obj = installer
    if kind == "symlink":
        obj.unit.symlink_to("/dev/null")
    elif kind == "directory":
        obj.unit.mkdir()
    elif kind == "fifo":
        os.mkfifo(obj.unit)
    else:
        obj.unit.write_bytes(b"different\n" if kind == "different" else EXPECTED)
        obj.unit.chmod(0o600 if kind == "mode" else 0o644)
        if kind == "hardlink":
            os.link(obj.unit, obj.directory / "alias")
        if kind == "owner":
            original = obj.module.os.stat
            def foreign(*args, **kwargs):
                values = list(original(*args, **kwargs))
                values[4] = 2345
                return os.stat_result(values)
            obj.module.os.stat = foreign
    with pytest.raises(ValueError):
        obj.module.persist_unit(EXPECTED)
    assert obj.calls == []
    assert not list(obj.directory.glob(".*.tmp"))


@pytest.mark.parametrize("relative", ["etc/systemd/system/ram-prewarm.mount.d", "etc/systemd/system/ram-.mount.d",
                                      "etc/systemd/system/mount.d", "run/systemd/system/ram-prewarm.mount",
                                      "usr/local/lib/systemd/system/ram-prewarm.mount", "usr/lib/systemd/system/ram-prewarm.mount",
                                      "lib/systemd/system/ram-prewarm.mount", "run/systemd/generator/ram-prewarm.mount",
                                      "run/systemd/generator.early/ram-prewarm.mount", "run/systemd/generator.late/ram-prewarm.mount"])
def test_private_install_refuses_dropins_vendor_masks_and_generated_conflicts(installer, relative):
    path = installer.root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.symlink_to("/dev/null")
    with pytest.raises(ValueError, match="unit conflict"):
        installer.module.persist_unit(EXPECTED)
    assert not installer.unit.exists()
    assert installer.calls == []


@pytest.mark.parametrize("relative", ["etc/fstab", "run/systemd/system/mount.d",
                                      "run/systemd/generator/ram-prewarm.mount"])
def test_private_install_refuses_unreadable_conflict_observations(installer, monkeypatch, relative):
    """An I/O error is unknown configuration, never evidence of no conflict."""
    obj = installer
    target = obj.root / relative
    real_stat, real_lstat = os.stat, os.lstat
    reached = []

    def is_target(path, kwargs):
        return (isinstance(path, (str, bytes, os.PathLike))
                and os.fsdecode(path) == str(target)
                and kwargs.get("dir_fd") is None)

    def unreadable_stat(path, *args, **kwargs):
        if is_target(path, kwargs):
            reached.append("stat")
            raise OSError(errno.EIO, "injected conflict metadata failure")
        return real_stat(path, *args, **kwargs)

    def unreadable_lstat(path, *args, **kwargs):
        if is_target(path, kwargs):
            reached.append("lstat")
            raise OSError(errno.EIO, "injected conflict metadata failure")
        return real_lstat(path, *args, **kwargs)

    # Python 3.14's exists/is_symlink predicates call os directly. Inject at
    # actual metadata operations, also covering a future explicit lstat reader.
    monkeypatch.setattr(os, "stat", unreadable_stat)
    monkeypatch.setattr(os, "lstat", unreadable_lstat)
    failure = None
    try:
        obj.module.persist_unit(EXPECTED)
    except OSError as exc:
        failure = exc
    assert reached, "fixture metadata fault never reached production"
    assert failure is not None, f"unknown conflict was accepted after {reached}"
    assert failure.errno == errno.EIO
    assert not obj.unit.exists()
    assert obj.calls == []


@pytest.mark.parametrize("mountpoint", ["/ram/prewarm", r"\057ram\057prewarm",
                                       "/ram/prewarm/", "/ram//prewarm", "//ram/prewarm",
                                       "/ram/./prewarm", "/ram/other/../prewarm"])
def test_private_install_refuses_fstab_mountpoint(installer, mountpoint):
    (installer.root / "etc/fstab").write_text(f"tmpfs {mountpoint} tmpfs defaults 0 0\n")
    with pytest.raises(ValueError, match="fstab mountpoint conflict"):
        installer.module.persist_unit(EXPECTED)
    assert not installer.unit.exists()
    assert installer.calls == []


@pytest.mark.parametrize("directory", ["", "etc", "etc/systemd", "etc/systemd/system"])
def test_private_install_refuses_writable_ancestors(installer, directory):
    (installer.root / directory).chmod(0o777)
    with pytest.raises(ValueError, match="untrusted system directory"):
        installer.module.persist_unit(EXPECTED)
    assert not installer.unit.exists()
    assert installer.calls == []


def test_private_install_requires_existing_system_directory(installer):
    installer.directory.rmdir()
    with pytest.raises(FileNotFoundError):
        installer.module.persist_unit(EXPECTED)
    assert not installer.directory.exists()
    assert installer.calls == []


def test_private_install_does_not_change_simulated_current_mount_contents(installer):
    mountpoint = installer.root / "ram/prewarm"
    mountpoint.mkdir(parents=True)
    (mountpoint / ".prismabuild-ram-epoch.json").write_text('{"epoch":"unchanged"}')
    (mountpoint / "resident-range").write_bytes(b"existing pages")
    before = _snapshot(mountpoint)
    installer.module.persist_unit(EXPECTED)
    assert _snapshot(mountpoint) == before


def test_private_install_refuses_foreign_directory_owner(installer, monkeypatch):
    original = Path.lstat
    def foreign(path, *args, **kwargs):
        metadata = original(path, *args, **kwargs)
        if path == installer.directory:
            values = list(metadata)
            values[4] = 2345
            return os.stat_result(values)
        return metadata
    monkeypatch.setattr(Path, "lstat", foreign)
    with pytest.raises(ValueError, match="untrusted system directory"):
        installer.module.persist_unit(EXPECTED)
    assert installer.calls == []


def test_private_install_refuses_changed_directory_descriptor(installer):
    original = installer.module.os.fstat
    def changed(fd):
        values = list(original(fd))
        values[1] += 1
        return os.stat_result(values)
    installer.module.os.fstat = changed
    with pytest.raises(ValueError, match="system directory identity changed"):
        installer.module.persist_unit(EXPECTED)
    assert installer.calls == []


def test_private_install_refuses_symlink_system_directory(installer):
    obj = installer
    other = obj.root / "other"
    obj.directory.rename(other)
    obj.directory.symlink_to(other, target_is_directory=True)
    with pytest.raises(ValueError, match="untrusted system directory"):
        obj.module.persist_unit(EXPECTED)
    assert not (other / "ram-prewarm.mount").exists()
    assert obj.calls == []


@pytest.mark.parametrize("failure", ["fchown", "write", "fsync", "commit", "reload", "enable"])
def test_private_install_cleans_own_temporary_and_does_not_fake_rollback(installer, failure):
    obj = installer
    def fail(*args, **kwargs):
        raise OSError(f"injected {failure}")
    if failure in ("fchown", "write", "fsync"):
        setattr(obj.module.os, failure, fail)
    elif failure == "commit":
        obj.module.os.link = fail
    else:
        original = obj.module._systemctl
        def failed_command(root, arguments):
            original(root, arguments)
            if arguments[0] == ("daemon-reload" if failure == "reload" else "enable"):
                fail()
        obj.module._systemctl = failed_command
    with pytest.raises(OSError, match=f"injected {failure}"):
        obj.module.persist_unit(EXPECTED)
    assert not list(obj.directory.glob(".*.tmp"))
    assert obj.unit.exists() == (failure in ("reload", "enable"))
    if obj.unit.exists():
        assert obj.unit.read_bytes() == EXPECTED
    else:
        assert not any(call[0] == "/usr/bin/systemctl" for call in obj.calls)


def test_private_install_postcommit_directory_fsync_failure_keeps_committed_file(installer):
    original = installer.module.os.fsync
    def failed_directory(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError("injected directory fsync failure")
        return original(fd)
    installer.module.os.fsync = failed_directory
    with pytest.raises(OSError, match="directory fsync failure"):
        installer.module.persist_unit(EXPECTED)
    assert installer.unit.read_bytes() == EXPECTED
    assert not list(installer.directory.glob(".*.tmp"))
    assert not any(call[0] == "/usr/bin/systemctl" for call in installer.calls)


def test_private_install_fullwrite_handles_short_writes(installer):
    real_write = os.write
    installer.module.os.write = lambda fd, data: real_write(fd, data[:7])
    installer.module.persist_unit(EXPECTED)
    assert installer.unit.read_bytes() == EXPECTED


def test_private_install_zero_write_refuses_and_cleans_temporary(installer):
    installer.module.os.write = lambda *args: 0
    with pytest.raises(OSError, match="write made no progress"):
        installer.module.persist_unit(EXPECTED)
    assert not installer.unit.exists()
    assert not list(installer.directory.glob(".*.tmp"))
    assert not any(call[0] == "/usr/bin/systemctl" for call in installer.calls)


def test_private_install_atomic_commit_cannot_overwrite_a_competing_file(installer):
    obj = installer
    def occupied(source, destination, **kwargs):
        obj.unit.write_bytes(b"operator's competing file\n")
        return os.link(source, destination, **kwargs)
    obj.module.os.link = occupied
    with pytest.raises(FileExistsError):
        obj.module.persist_unit(EXPECTED)
    assert obj.unit.read_bytes() == b"operator's competing file\n"
    assert not list(obj.directory.glob(".*.tmp"))
    assert not any(call[0] == "/usr/bin/systemctl" for call in obj.calls)


def test_actual_rendered_unit_verifies_in_private_systemd_graph(generation, tmp_path):
    analyzer = shutil.which("systemd-analyze")
    assert analyzer, "missing systemd-analyze: unit graph is NOT qualified"
    result = _cli(generation)
    assert result.returncode == 0, result.stderr
    private = tmp_path / "systemd"
    directory = private / "etc/systemd/system"
    directory.mkdir(parents=True)
    unit = directory / "ram-prewarm.mount"
    unit.write_bytes(result.stdout)
    wants = directory / "local-fs.target.wants"
    wants.mkdir()
    (wants / unit.name).symlink_to("../" + unit.name)
    for target in ("local-fs", "local-fs-pre", "sysinit", "basic", "shutdown", "umount", "swap"):
        (directory / f"{target}.target").write_text("[Unit]\nDescription=Fixture target\n")
    for executable in ("bin/mount", "bin/umount"):
        path = private / executable
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("#!/bin/sh\nexit 0\n")
        path.chmod(0o755)
    verified = subprocess.run([analyzer, "verify", "--man=no", f"--root={private}",
                               unit.name, "local-fs.target"], capture_output=True, text=True, timeout=20)
    assert verified.returncode == 0, verified.stdout + verified.stderr


RAM_GIB = 1_073_741_824
RAM_HOST = "dl380g10"
RAM_ID = f"ram:{RAM_HOST}"
RAM_NOW = 2_000_000.0
RAM_SAMPLED = RAM_NOW - 5
RAM_FREE = {"0": 13 * 1024, "1": 987 * 1024}
RAM_SHMEM = {"0": 765 * 1024, "1": 21 * 1024}


@pytest.fixture
def rendered_ram_options(generation):
    def render(spelling):
        arguments = _arguments(generation)
        arguments[arguments.index("--size-bytes") + 1] = str(256 * RAM_GIB)
        before = _snapshot(generation.parent)
        result = _cli(generation, arguments, spelling=spelling)
        assert result.returncode == 0, result.stderr
        assert result.stderr == b""
        parsed = configparser.ConfigParser(interpolation=None)
        parsed.read_string(result.stdout.decode("ascii"))
        assert parsed.sections() == ["Unit", "Mount", "Install"]
        assert parsed["Mount"]["What"] == "tmpfs"
        assert parsed["Mount"]["Where"] == "/ram/prewarm"
        assert parsed["Mount"]["Type"] == "tmpfs"
        assert parsed["Install"]["WantedBy"] == "local-fs.target"
        options = parsed["Mount"]["Options"]
        assert options.split(",") == [
            f"size={256 * RAM_GIB}", "noswap", "mpol=interleave",
            f"uid={UID}", f"gid={GID}", "mode=0755",
        ]
        assert _snapshot(generation.parent) == before
        return options

    return render


class _RenderedRamEvidence:
    def __init__(self, root, options):
        self.root = root
        self.mount = root / "ram"
        self.proc = root / "proc"
        self.nodes = root / "sys/devices/system/node"
        self.block = root / "sys/block"
        self.by_id = root / "dev/disk/by-id"
        for directory in (self.mount, self.proc, self.nodes, self.block, self.by_id):
            directory.mkdir(parents=True)
        (self.nodes / "has_memory").write_text("0-1\n")
        (self.nodes / "online").write_text("0-1\n")
        for node in (0, 1):
            directory = self.nodes / f"node{node}"
            directory.mkdir()
            (directory / "cpulist").write_text("0\n" if node == 0 else "\n")
        self.set_node_totals((147, 147))
        self.set_options(options)
        (self.proc / "meminfo").write_text(
            f"MemTotal: {294 * RAM_GIB // 1024} kB\n")
        (self.proc / "arcstats").write_text(
            f"c_max 4 {22 * RAM_GIB}\nsize 4 {11 * RAM_GIB}\n"
            f"arc_meta_used 4 {5 * RAM_GIB}\n")
        (self.proc / "pressure").mkdir()
        (self.proc / "pressure/io").write_text(
            "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
            "full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n")
        self.policy = {
            "schema": storage_tiers.RAM_TIER_POLICY_SCHEMA_V1,
            "mountpoint": str(self.mount), "ceiling_gib_max": 256,
            "window_gib_default": 112, "arc_floor_gib": 20,
            "system_reserve_gib": 16, "prefill_depth": None,
        }
        self.policy_path = root / "ram-policy.json"
        self.policy_path.write_text(json.dumps(self.policy))
        self.discovered = None

    def node_file(self, node):
        return self.nodes / f"node{node}/meminfo"

    def set_node_totals(self, totals):
        assert sum(totals) == 294
        for node, total in enumerate(totals):
            self.node_file(node).write_text(
                f"Node {node} MemTotal: {total * RAM_GIB // 1024} kB\n"
                f"Node {node} MemFree: {13 if node == 0 else 987} kB\n"
                f"Node {node} Shmem: {765 if node == 0 else 21} kB\n")

    def set_options(self, options):
        self.options = options
        (self.proc / "mounts").write_text(
            f"tmpfs {self.mount} tmpfs {options} 0 0\n")

    def statvfs(self, path):
        assert str(path) == str(self.mount)
        return os.statvfs_result(
            (1024, 1024, 256 * RAM_GIB // 1024, 200 * RAM_GIB // 1024,
             200 * RAM_GIB // 1024, 1_000_000, 900_000, 900_000, 0, 255))

    def discover(self, **kwargs):
        def no_zfs(argv):
            raise OSError(f"no {argv[0]} on this fixture box")

        tiers = storage_tiers.discover_tiers(
            **kwargs, runner=no_zfs, statvfs=self.statvfs,
            arcstats_path=str(self.proc / "arcstats"),
            sysfs=str(self.block), by_id=str(self.by_id),
            proc_pressure=str(self.proc / "pressure/io"),
            proc_mounts=str(self.proc / "mounts"),
            meminfo_path=str(self.proc / "meminfo"),
            memory_numa_root=str(self.nodes))
        self.discovered = copy.deepcopy(tiers[RAM_ID])
        return tiers


@pytest.fixture
def ram_integration(tmp_path, monkeypatch, rendered_ram_options):
    clock = [RAM_NOW]
    monkeypatch.setattr(pbstatus.time, "time", lambda: clock[0])
    monkeypatch.setattr(pool, "_now", lambda: clock[0])
    for name in ("LAST_CYCLE", "_VERDICT_CYCLE", "_VERDICT_SINCE", "_EVENT_LINES",
                 "_LOCK_BUSY_COUNT", "_STAGED_WAITS", "_SHARED_MOVERS",
                 "_CYCLE_COMMITMENTS", "_CENSUS_COST", "_JOINT_COMMITTED",
                 "_FASTEST_CONSUMPTION", "_MANIFEST_SUMMARY_LAST"):
        monkeypatch.setattr(tier_loop, name, copy.deepcopy(getattr(tier_loop, name)))
    monkeypatch.setattr(tier_loop.residency_plan, "_SHARE_NAMESPACE_MEMO",
                        copy.deepcopy(tier_loop.residency_plan._SHARE_NAMESPACE_MEMO))
    real_statvfs = os.statvfs

    def create(spelling="tools"):
        options = rendered_ram_options(spelling)
        evidence = _RenderedRamEvidence(tmp_path / "evidence", options)
        monkeypatch.setattr(storage_tiers, "RAM_POLICY_FILE", str(evidence.policy_path))
        assert tier_loop.load_ram_policy() == evidence.policy

        def private_statvfs(path):
            if str(path) == str(evidence.mount):
                return evidence.statvfs(path)
            return real_statvfs(path)

        monkeypatch.setattr(tier_loop.os, "statvfs", private_statvfs)
        queue = pool.PoolQueue(tmp_path / "pb-queue")
        queue.records.ensure_layout()
        queue.ledger(RAM_HOST).ensure_capacity({"mem_gb": 294})
        assert queue.ledger(RAM_HOST).acquire("fixture-worker", {"mem_gb": 88})
        assert queue.rows_host_memory_held(RAM_HOST) == 88
        return SimpleNamespace(evidence=evidence, queue=queue, clock=clock,
                               receipts=tier_loop.ReceiptCache())

    return create


def _rendered_ram_cycle(obj, capsys, *, reason=None):
    announced = tier_loop.cycle(
        obj.queue, host=RAM_HOST, source_pool="fixture-source", receipts=obj.receipts,
        now=RAM_SAMPLED, discover=obj.evidence.discover)
    assert tier_loop.LAST_CYCLE["completed"] is True
    record = next(row for row in announced if row["tier_id"] == RAM_ID)
    persisted = json.loads(obj.queue.records.tier_record_path(RAM_ID).read_text())
    assert persisted == dict(record, announced_unix=obj.clock[0])
    assert next(row for row in obj.queue.tiers() if row["tier_id"] == RAM_ID) == persisted
    expected_tokens = {"ram_gib": 112} if reason is None else {}
    for carrier in (obj.evidence.discovered, record, persisted):
        assert carrier["mount_options"] == obj.evidence.options.split(",")
        assert carrier["mountpoint"] == str(obj.evidence.mount)
        assert carrier["ceiling_bytes"] == carrier["size_bytes"] == 256 * RAM_GIB
        assert carrier["capacity_bytes"] == 200 * RAM_GIB
        assert carrier["window_gib"] == 112
        assert carrier["sampled_unix"] == RAM_SAMPLED
        assert isinstance(carrier["epoch"], str) and carrier["epoch"]
        admission = carrier["ram_admission"]
        assert admission["admissible"] is (reason is None), admission
        assert admission["reason"] == reason
        for field, gib in {
            "ceiling_bytes": 256, "window_bytes": 112, "mem_total_bytes": 294,
            "rows_held_bytes": 88, "arc_c_max": 22, "arc_size": 11,
            "arc_floor_bytes": 20, "arc_meta_used": 5, "system_reserve_bytes": 16,
            "allowed_ceiling_bytes": 256, "allowed_window_bytes": 168,
            "node_required_numerator_bytes": 150,
        }.items():
            assert admission[field] == gib * RAM_GIB, (field, admission)
        assert storage_tiers.tier_tokens(carrier) == expected_tokens
    expected_capacity = 112 if reason is None else 0
    ledger = obj.queue.tier_ledger(RAM_ID)
    assert ledger.capacity().get("ram_gib", 0) == expected_capacity
    assert ledger.available().get("ram_gib", 0) == expected_capacity
    capsys.readouterr()
    return obj.evidence.discovered, record, persisted


def _rendered_ram_status(obj, capsys, monkeypatch):
    before = _snapshot(obj.evidence.root.parent)
    open_path = Path.open

    def no_sampling(path, *args, **kwargs):
        for root in (obj.evidence.root / "sys", obj.evidence.proc, Path("/sys"), Path("/proc")):
            assert path != root and root not in path.parents, "status resampled kernel evidence"
        return open_path(path, *args, **kwargs)

    with monkeypatch.context() as guard:
        guard.setattr(Path, "open", no_sampling)
        assert pbstatus.main(["--starvation", "--queue-root", str(obj.queue.root),
                              "--timeout-s", "0"]) == 0
    blob = json.loads(capsys.readouterr().out)
    assert blob["schema"] == pbstatus.STARVATION_SCHEMA_V1
    assert blob["complete"] is True
    assert _snapshot(obj.evidence.root.parent) == before
    operator = next(row for row in blob["tiers"] if row["tier_id"] == RAM_ID)
    assert operator["announced"] is True
    assert operator["sampled_age_s"] == obj.clock[0] - RAM_SAMPLED
    return operator


def _assert_rendered_ram_observations(carriers, *, free=RAM_FREE, shmem=RAM_SHMEM,
                                      nodes=(0, 1)):
    for carrier in carriers:
        assert carrier["memory_nodes"] == (None if nodes is None else list(nodes))
        for field, expected in (("node_memfree_bytes", free), ("node_shmem_bytes", shmem)):
            assert field in carrier
            assert carrier[field] == expected
            if expected is not None:
                assert list(carrier[field]) == ["0", "1"]
                assert all(type(key) is str for key in carrier[field])
                assert all(value is None or type(value) is int
                           for value in carrier[field].values())


@pytest.mark.parametrize("spelling", ["tools", "tools/fleet"])
@pytest.mark.parametrize("placement", ["mpol=interleave", "mpol=interleave:0-1",
                                        "mpol=interleave:0,1"])
def test_rendered_ram_unit_admits_through_tier_cycle_and_status(
        ram_integration, capsys, monkeypatch, spelling, placement):
    obj = ram_integration(spelling)
    options = obj.evidence.options
    assert options.count("mpol=") == 1
    if placement != "mpol=interleave":
        obj.evidence.set_options(options.replace("mpol=interleave", placement))
    carriers = _rendered_ram_cycle(obj, capsys)
    assert carriers[1]["stage_root_owner"] == "registered"
    admission = carriers[0]["ram_admission"]
    assert admission["node_memtotal_bytes"] == {"0": 147 * RAM_GIB, "1": 147 * RAM_GIB}
    assert admission["memory_node_count"] == 2
    assert admission["failing_memory_node"] is None
    operator = _rendered_ram_status(obj, capsys, monkeypatch)
    assert operator["ledger_capacity"]["ram_gib"] == 112
    _assert_rendered_ram_observations((*carriers, operator))


@pytest.mark.parametrize("placement,noswap,reason", [
    (None, True, "ram_mount_not_interleaved"),
    ("mpol=bind:0-1", True, "ram_mount_not_interleaved"),
    ("mpol=interleave:0", True, "ram_mount_not_interleaved"),
    ("mpol=interleave", False, "ram_mount_not_noswap"),
    (None, False, "ram_mount_not_noswap"),
])
def test_rendered_ram_unit_placement_damage_refuses_without_rotating_epoch(
        ram_integration, capsys, monkeypatch, placement, noswap, reason):
    obj = ram_integration()
    baseline = _rendered_ram_cycle(obj, capsys)
    epoch = baseline[0]["epoch"]
    marker = obj.evidence.mount / storage_tiers.RAM_EPOCH_MARKER
    marker_before = marker.read_bytes(), marker.stat().st_mtime_ns
    options = obj.evidence.options
    options = (options.replace(",mpol=interleave", "") if placement is None
               else options.replace("mpol=interleave", placement))
    if not noswap:
        options = options.replace(",noswap", "")
    obj.evidence.set_options(options)
    carriers = _rendered_ram_cycle(obj, capsys, reason=reason)
    operator = _rendered_ram_status(obj, capsys, monkeypatch)
    assert operator["ledger_capacity"].get("ram_gib", 0) == 0
    assert all(carrier["epoch"] == epoch for carrier in (*carriers, operator))
    assert (marker.read_bytes(), marker.stat().st_mtime_ns) == marker_before
    _assert_rendered_ram_observations((*carriers, operator))


@pytest.mark.parametrize("totals,reason", [
    ((60, 234), "ram_window_exceeds_node_memtotal_floor"),
    ((75, 219), None),
])
def test_rendered_ram_unit_checks_every_node_and_admits_exact_equality(
        ram_integration, capsys, monkeypatch, totals, reason):
    obj = ram_integration()
    obj.evidence.set_node_totals(totals)
    carriers = _rendered_ram_cycle(obj, capsys, reason=reason)
    for carrier in carriers:
        admission = carrier["ram_admission"]
        assert admission["node_memtotal_bytes"] == {
            str(node): total * RAM_GIB for node, total in enumerate(totals)}
        assert admission["memory_node_count"] == 2
        assert admission["node_required_numerator_bytes"] == 150 * RAM_GIB
        assert admission["failing_memory_node"] == (0 if reason else None)
        if reason:
            assert type(admission["failing_memory_node"]) is int
            assert 150 * RAM_GIB > totals[0] * RAM_GIB * 2
        else:
            assert 150 * RAM_GIB == totals[0] * RAM_GIB * 2
            assert all(150 * RAM_GIB <= total * RAM_GIB * 2 for total in totals)
    operator = _rendered_ram_status(obj, capsys, monkeypatch)
    assert operator["ledger_capacity"].get("ram_gib", 0) == (0 if reason else 112)
    _assert_rendered_ram_observations((*carriers, operator))


@pytest.mark.parametrize("metric,lines,kib,damage", [
    pytest.param(metric, lines, kib, None, id=f"{metric}-{name}")
    for metric in ("MemFree", "Shmem")
    for name, lines, kib in [
        ("valid", "Node 0 {metric}: 123 kB\n", 123),
        ("zero", "Node 0 {metric}: 0 kB\n", 0),
        ("missing", "", None),
        ("tabs", "Node 0\t{metric}:\t123\tkB\t\n", 123),
        ("unicode-whitespace", "Node 0\u2003{metric}:\u00a0123\u2009kB\u001f\n", 123),
        ("splitlines", "Node 0 {metric}: 123 kB\u2028Node 0 Dirty: 17 kB\n", 123),
        ("leading-zeros", "Node 0 {metric}: 00000000000000000123 kB\n", 123),
        ("twenty-digits", "Node 0 {metric}: 12345678901234567890 kB\n", 12345678901234567890),
        ("too-many-digits", "Node 0 {metric}: 123456789012345678901 kB\n", None),
        ("unicode-digits", "Node 0 {metric}: ١٢٣ kB\n", None),
        ("negative", "Node 0 {metric}: -1 kB\n", None),
        ("signed", "Node 0 {metric}: +1 kB\n", None),
        ("wrong-unit", "Node 0 {metric}: 123 MB\n", None),
        ("literal-node-space", "Node\t0 {metric}: 123 kB\n", None),
        ("leading-whitespace", " Node 0 {metric}: 123 kB\n", None),
        ("trailing-junk", "Node 0 {metric}: 123 kB junk\n", None),
        ("duplicate", "Node 0 {metric}: 123 kB\nNode 0 {metric}: 123 kB\n", None),
        ("valid-plus-malformed", "Node 0 {metric}: 123 kB\nNode 0 {metric}: bad kB\n", None),
        ("valid-plus-wrong-node", "Node 0 {metric}: 123 kB\nNode 1 {metric}: 1 kB\n", None),
        ("wrong-node", "Node 1 {metric}: 123 kB\n", None),
        ("bare-candidate", "Node 0 {metric}: 123 kB\n{metric}\n", None),
        ("whitespace-candidate", "Node 0 {metric}: 123 kB\nother\u00a0{metric} garbage\n", None),
        ("field-prefix-not-candidate", "Node 0 {metric}: 123 kB\nOther{metric}: bad\n", 123),
        ("field-suffix-not-candidate", "Node 0 {metric}: 123 kB\n{metric}Other: bad\n", 123),
    ]
] + [
    pytest.param(None, None, None, damage, id=damage)
    for damage in ("missing-counters", "missing-has-memory", "missing-total", "zero-total")
])
def test_rendered_ram_unit_observations_preserve_unknowns_and_sample_age(
        ram_integration, capsys, monkeypatch, metric, lines, kib, damage):
    obj = ram_integration()
    total = f"Node 0 MemTotal: {147 * RAM_GIB // 1024} kB\n"
    free, shmem = dict(RAM_FREE), dict(RAM_SHMEM)
    if metric is not None:
        other = ("Node 0 Shmem: 765 kB\n" if metric == "MemFree"
                 else "Node 0 MemFree: 13 kB\n")
        obj.evidence.node_file(0).write_text(total + other + lines.format(metric=metric))
        (free if metric == "MemFree" else shmem)["0"] = None if kib is None else kib * 1024
    elif damage == "missing-counters":
        for node in (0, 1):
            obj.evidence.node_file(node).write_text(
                f"Node {node} MemTotal: {147 * RAM_GIB // 1024} kB\n")
        free = shmem = {"0": None, "1": None}
    elif damage == "missing-has-memory":
        (obj.evidence.nodes / "has_memory").unlink()
    else:
        obj.evidence.node_file(0).write_text(
            ("" if damage == "missing-total" else "Node 0 MemTotal: 0 kB\n")
            + "Node 0 MemFree: 13 kB\nNode 0 Shmem: 765 kB\n")
    topology_unknown = damage in ("missing-has-memory", "missing-total", "zero-total")
    reason = "ram_numa_topology_unreadable" if topology_unknown else None
    carriers = _rendered_ram_cycle(obj, capsys, reason=reason)
    if topology_unknown:
        free = shmem = None
    for node in (0, 1):
        obj.evidence.node_file(node).write_text(
            f"Node {node} MemTotal: {147 * RAM_GIB // 1024} kB\n"
            f"Node {node} MemFree: 999 kB\nNode {node} Shmem: 888 kB\n")
    (obj.evidence.nodes / "has_memory").write_text("0-1\n")
    obj.clock[0] += 37.5
    operator = _rendered_ram_status(obj, capsys, monkeypatch)
    assert operator["sampled_age_s"] == 42.5
    assert operator["ledger_capacity"].get("ram_gib", 0) == (0 if topology_unknown else 112)
    _assert_rendered_ram_observations((*carriers, operator), free=free, shmem=shmem,
                                      nodes=None if topology_unknown else (0, 1))

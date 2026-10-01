"""NEW FEATURE private CLI/consumer controls; no artificial old-source RED.

Run ONLY in a parent-admitted PrismaBuild CPU action. Real finite Popen writers,
real sealed receipts/members and real fchmod. Substitutions are exclusively raw
filesystem/kernel/runtime observations, never qualifier/manager/verdict helpers.
"""
from __future__ import annotations

import errno
import hashlib
import io
import json
import os
import stat
import subprocess
import sys
import time
from contextlib import ExitStack
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools/fleet"))
import migrate_role_logs as migration  # noqa: E402

HOST = "private-exact-role-box"
ROLES = ("storage", "tiers", "metrics")
MARKER = b"later stdout\nlater stderr\n"
AUTH_SENTINEL = "private-authorization-value-must-not-appear-in-report"
NONASCII_ARG = "private-α-雪-authorization"
CHILD = """import json, os, sys, time
from pathlib import Path
control = Path(sys.argv[1])
if len(sys.argv) > 2:
    os.write(1, b'private payload stdout\\n')
    os.write(2, b'private payload stderr\\n')
(control / 'ready').touch()
deadline = time.monotonic() + 45
while not (control / 'finish').exists():
    if time.monotonic() >= deadline:
        raise SystemExit(3)
    if (control / 'append').exists() and not (control / 'appended').exists():
        os.write(1, b'later stdout\\n')
        os.write(2, b'later stderr\\n')
        (control / 'appended').touch()
    time.sleep(.01)
"""


def wait_for(predicate):
    deadline = time.monotonic() + 10
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("private child deadline")
        time.sleep(.01)


def fingerprint(path):
    info = path.stat()
    return (info.st_dev, info.st_ino, info.st_uid, info.st_gid, info.st_nlink,
            hashlib.sha256(path.read_bytes()).hexdigest())


@pytest.fixture
def box(tmp_path, monkeypatch):
    if os.getuid() != 1000 or os.geteuid() != 1000:
        pytest.skip("exact production acting-owner UID1000 required")
    mirror = tmp_path / "fleet"
    generation = mirror / "runtime-generations/private-generation"
    scripts = generation / "tools"
    (scripts / "fleet").mkdir(parents=True)
    controls = {r: tmp_path / f"control-{r}" for r in (*ROLES, "supervisor")}
    for control in controls.values():
        control.mkdir()
    declarations = {r: [str(controls[r]), "writer", AUTH_SENTINEL,
                        NONASCII_ARG if r == "tiers" else "ascii-authorization"]
                    for r in ROLES}
    (scripts / "fleet/fleet_boxes.json").write_text(json.dumps({"boxes": {
        HOST: {"roles": declarations, "loops": 1, "args": []}}}))
    for name in ("supervise.py", *migration.supervise.ROLE_SCRIPTS.values()):
        (scripts / name).write_text(CHILD)
    (mirror / "repo").symlink_to(generation, target_is_directory=True)
    logs = tmp_path / "logs"
    logs.mkdir()
    logs.chmod(0o775)
    paths = {r: logs / f"pb-role-{r}.log" for r in ROLES}
    for path in paths.values():
        path.write_bytes(b"seed\n")
        path.chmod(0o664)
    children = {}
    original_iterdir = Path.iterdir

    def private_process_listing(path):
        if path == Path("/proc"):
            return iter(Path("/proc") / str(child.pid) for child in children.values())
        return original_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", private_process_listing)
    monkeypatch.setattr(migration, "_LOG_DIRECTORY", logs)
    monkeypatch.setattr(migration.supervise, "MIRROR", mirror)
    monkeypatch.setattr(migration.publish_runtime, "MIRROR", mirror / "repo")
    monkeypatch.setattr(migration.socket, "gethostname", lambda: HOST)

    def unseal():
        for path in [generation, *generation.rglob("*")]:
            if path.is_dir():
                path.chmod(0o755)
            elif path.is_file():
                path.chmod(0o644)

    def seal():
        files = {path.relative_to(generation).as_posix():
                 hashlib.sha256(path.read_bytes()).hexdigest()
                 for path in generation.rglob("*")
                 if path.is_file() and path.name != "RUNTIME_VERSION.json"}
        (generation / "RUNTIME_VERSION.json").write_text(json.dumps({
            "schema": migration.supervise.RUNTIME_VERSION_SCHEMA,
            "generation": generation.name, "commit": "a" * 40, "files": files}))
        for path in generation.rglob("*"):
            path.chmod(0o555 if path.is_dir() else 0o444)
        generation.chmod(0o555)

    seal()
    with ExitStack() as stack:
        for role in ROLES:
            output = stack.enter_context(paths[role].open("ab", buffering=0))
            children[role] = subprocess.Popen(
                [sys.executable, str(scripts / migration.supervise.ROLE_SCRIPTS[role]),
                 *declarations[role]], stdout=output, stderr=output,
                start_new_session=True,
                env={**os.environ, migration.supervise.OWNERSHIP_ENV: HOST})
        # Valid cron/manual style: no role mark, no session leadership contract.
        environment = dict(os.environ)
        environment.pop(migration.supervise.OWNERSHIP_ENV, None)
        children["supervisor"] = subprocess.Popen(
            [sys.executable, str(scripts / "supervise.py"), str(controls["supervisor"]),
             AUTH_SENTINEL, NONASCII_ARG],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=environment)
        try:
            wait_for(lambda: all((c / "ready").exists() for c in controls.values()))
            yield {"paths": paths, "logs": logs, "children": children,
                   "controls": controls, "generation": generation,
                   "unseal": unseal, "seal": seal, "declarations": declarations}
        finally:
            for control in controls.values():
                (control / "finish").touch()
            for child in children.values():
                # Exact finite private children only, not role manager/signals.
                child.wait(timeout=12)
                assert child.returncode == 0
            unseal()


def cli(capsys, *, apply=False):
    code = migration.main(["--apply"] if apply else [])
    output = capsys.readouterr().out
    result = json.loads(output)
    # Independent old print-spelling oracle: default JSON spacing/ASCII and LF.
    assert output == json.dumps(result, sort_keys=True) + "\n"
    return code, result


def observe_bytes(monkeypatch, path, transform):
    """Substitute a raw proc-file observation, not any verification verdict."""
    original = Path.open

    def opened(self, *args, **kwargs):
        if self == path:
            with original(self, "rb") as stream:
                raw = stream.read()
            return io.BytesIO(transform(raw))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", opened)


def unchanged(box, before):
    assert {r: fingerprint(p) for r, p in box["paths"].items()} == before


def test_closed_cli_plan_then_real_apply_all_three_and_later_append(box, capsys):
    before = {r: fingerprint(p) for r, p in box["paths"].items()}
    modes = {r: stat.S_IMODE(p.stat().st_mode) for r, p in box["paths"].items()}
    code, planned = cli(capsys)
    assert code == 0 and planned["status"] == "planned"
    assert not planned["effect_attempted"]
    unchanged(box, before)
    assert modes == {r: stat.S_IMODE(p.stat().st_mode) for r, p in box["paths"].items()}
    code, applied = cli(capsys, apply=True)
    assert code == 0 and applied["status"] == "applied"
    assert len(applied["leaves"]) == 3
    assert applied["qualification"]["loaded_import_bytes"] == "UNKNOWN"
    assert applied["qualification"]["owning_claim"] == "UNKNOWN"
    assert AUTH_SENTINEL not in json.dumps(planned)
    assert AUTH_SENTINEL not in json.dumps(applied)
    assert NONASCII_ARG not in json.dumps(applied, ensure_ascii=False)
    observed = applied["qualification"]
    proofs = {"supervisor": observed["observed_supervisor"], **observed["observed_roles"]}
    for name, proof in proofs.items():
        assert "argv" not in proof["process"]
        # Independent oracle, including ASCII and nonASCII argv, not a
        # self-comparison to the Core helper called by the implementation.
        expected = hashlib.sha256(json.dumps(
            box["children"][name].args, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False
        ).encode("utf-8")).hexdigest()
        assert proof["process"]["argv_sha256"] == expected
    unchanged(box, before)  # FULL private payload digests, not point sizes.
    for leaf in applied["leaves"]:
        assert leaf["before"]["mode"] == 0o664 and leaf["after"]["mode"] == 0o600
        assert leaf["writer_fds_before"] == leaf["writer_fds_after"]
        assert {item["fd"] for item in leaf["writer_fds_after"]} == {1, 2}
        assert all(item["flags"] & os.O_APPEND for item in leaf["writer_fds_after"])
    for role in ROLES:
        path = box["paths"][role]
        payload = path.read_bytes()
        (box["controls"][role] / "append").touch()
        wait_for(lambda role=role: (box["controls"][role] / "appended").exists())
        assert path.read_bytes() == payload + MARKER
        assert fingerprint(path)[:5] == before[role][:5]
    code, noop = cli(capsys, apply=True)
    assert code == 0 and noop["status"] == "applied"
    assert not noop["effect_attempted"]
    assert all(leaf["disposition"] == "qualified-noop" for leaf in noop["leaves"])


@pytest.mark.parametrize("argument", ["--path", "--mode", "--uid", "--directory", "--host"])
def test_cli_has_no_policy_override(argument):
    with pytest.raises(SystemExit) as exc:
        migration.main([argument, "anything"])
    assert exc.value.code == 2


@pytest.mark.parametrize("kind", ["absent", "symlink", "fifo", "directory", "hardlink",
                                  "mode0620", "mode0602", "mode0644", "mode1664"])
def test_unsafe_leaf_stops_full_plan_before_any_effect(box, kind, capsys):
    path = box["paths"]["metrics"]  # last leaf: all-three validation before effect
    if kind == "hardlink":
        os.link(path, path.with_name("other-link"))
    elif kind.startswith("mode"):
        path.chmod(int(kind[4:], 8))
    else:
        path.rename(path.with_name("held-original"))
        if kind == "symlink":
            path.symlink_to("held-original")
        elif kind == "fifo":
            os.mkfifo(path)
        elif kind == "directory":
            path.mkdir()
    modes = {r: stat.S_IMODE(p.stat().st_mode) for r, p in box["paths"].items()
             if p.is_file()}
    code, result = cli(capsys, apply=True)
    assert code == 2 and not result["effect_attempted"]
    assert result["status"] == ("absent-no-effect" if kind == "absent" else "refused")
    for role, mode in modes.items():
        assert stat.S_IMODE(box["paths"][role].stat().st_mode) == mode


@pytest.mark.parametrize("kind", ["directory-link", "directory-foreign", "leaf-foreign"])
def test_namespace_owner_and_directory_link_refuse(box, kind, monkeypatch, capsys):
    if kind == "directory-link":
        renamed = box["logs"].with_name("real-logs")
        box["logs"].rename(renamed)
        box["logs"].symlink_to(renamed, target_is_directory=True)
    else:
        original = os.fstat

        def foreign(fd):
            info = original(fd)
            if ((kind == "directory-foreign" and stat.S_ISDIR(info.st_mode))
                    or (kind == "leaf-foreign" and stat.S_ISREG(info.st_mode))):
                values = list(info)
                values[4] = 1001
                return os.stat_result(values)
            return info

        monkeypatch.setattr(os, "fstat", foreign)
    code, result = cli(capsys, apply=True)
    assert code == 2 and not result["effect_attempted"]


@pytest.mark.parametrize("kind", ["undeclared", "custom", "invalid-receipt", "changed-member",
                                  "writable-member", "member-link", "generation-link",
                                  "supervisor-roster-mismatch"])
def test_real_generation_and_declared_member_refusals(box, kind, capsys):
    box["unseal"]()
    generation = box["generation"]
    if kind in ("undeclared", "custom"):
        roles = dict(box["declarations"])
        if kind == "undeclared":
            roles.pop("metrics")
        else:
            roles["storage"] = [*roles["storage"], "--log", "/custom"]
        (generation / "tools/fleet/fleet_boxes.json").write_text(json.dumps({
            "boxes": {HOST: {"roles": roles}}}))
        box["seal"]()
    else:
        box["seal"]()
        if kind == "generation-link":
            generation.chmod(0o755)
            moved = generation.with_name("moved-generation")
            generation.rename(moved)
            generation.symlink_to(moved, target_is_directory=True)
        elif kind == "invalid-receipt":
            receipt = generation / "RUNTIME_VERSION.json"
            receipt.chmod(0o644)
            receipt.write_text("{}")
            receipt.chmod(0o444)
        elif kind == "supervisor-roster-mismatch":
            # A different sealed published generation leaves the observed
            # supervisor in its original sealed source; no proof verdict mocks.
            other = generation.with_name("other-generation")
            import shutil
            shutil.copytree(generation, other)
            for path in [other, *other.rglob("*")]:
                path.chmod(0o755 if path.is_dir() else 0o644)
            roster = other / "tools/fleet/fleet_boxes.json"
            value = json.loads(roster.read_text())
            value["boxes"][HOST]["loops"] = 2
            roster.write_text(json.dumps(value))
            receipt = json.loads((other / "RUNTIME_VERSION.json").read_text())
            receipt["generation"] = other.name
            receipt["files"]["tools/fleet/fleet_boxes.json"] = hashlib.sha256(roster.read_bytes()).hexdigest()
            (other / "RUNTIME_VERSION.json").write_text(json.dumps(receipt))
            for path in other.rglob("*"):
                path.chmod(0o555 if path.is_dir() else 0o444)
            other.chmod(0o555)
            repo = generation.parent.parent / "repo"
            repo.unlink()
            repo.symlink_to(other, target_is_directory=True)
        else:
            member = generation / "tools/tier_loop.py"
            member.chmod(0o644)
            if kind == "changed-member":
                member.write_text("changed bytes")
                member.chmod(0o444)
            elif kind == "member-link":
                generation.chmod(0o755)
                member.parent.chmod(0o755)
                member.rename(member.with_name("old-tier"))
                member.symlink_to("old-tier")
    before = {r: fingerprint(p) for r, p in box["paths"].items()}
    code, result = cli(capsys, apply=True)
    assert code == 2 and not result["effect_attempted"]
    unchanged(box, before)


@pytest.mark.parametrize("kind", ["argv", "interpreter", "env", "duplicate-env", "leader",
                                  "stopped", "zombie", "uid", "malformed-stat",
                                  "wrong-script", "bad-start"])
def test_raw_process_proof_refusals(box, kind, monkeypatch, capsys):
    pid = box["children"]["storage"].pid
    proc = Path("/proc") / str(pid)
    field = "cmdline" if kind in ("argv", "interpreter", "wrong-script") else (
        "environ" if kind in ("env", "duplicate-env") else "status" if kind == "uid" else "stat")

    def transform(raw):
        if kind == "argv":
            return raw[:-1] + b"\0undeclared\0"
        if kind == "interpreter":
            return b"not-python\0" + raw.split(b"\0", 1)[1]
        if kind == "wrong-script":
            return raw.replace(b"/tools/prewarm_loop.py", b"/other/prewarm_loop.py")
        if kind == "env":
            return raw.replace(HOST.encode(), b"foreign-host")
        if kind == "duplicate-env":
            return raw + migration.supervise.OWNERSHIP_ENV.encode() + b"=" + HOST.encode() + b"\0"
        if kind == "uid":
            return b"\n".join(b"Uid:\t1001\t1001\t1001\t1001" if line.startswith(b"Uid:")
                               else line for line in raw.splitlines())
        if kind == "malformed-stat":
            return b"not a kernel stat"
        closing = raw.rfind(b")")
        fields = raw[closing + 1:].split()
        fields[{"leader": 2, "stopped": 0, "zombie": 0, "bad-start": 19}[kind]] = {
            "leader": b"1", "stopped": b"T", "zombie": b"Z", "bad-start": b"0"}[kind]
        return raw[:closing + 1] + b" " + b" ".join(fields)

    observe_bytes(monkeypatch, proc / field, transform)
    before = {r: fingerprint(p) for r, p in box["paths"].items()}
    code, result = cli(capsys, apply=True)
    assert code == 2 and not result["effect_attempted"]
    unchanged(box, before)


@pytest.mark.parametrize("descriptor", [1, 2])
@pytest.mark.parametrize("kind", ["wrong-inode", "nonappend", "readonly", "malformed", "oversized", "duplicate-flags"])
def test_both_append_descriptors_required(box, descriptor, kind, monkeypatch, capsys):
    pid = box["children"]["tiers"].pid
    proc = Path("/proc") / str(pid)
    if kind == "wrong-inode":
        original = os.stat

        def changed(path, *args, **kwargs):
            if path == proc / "fd" / str(descriptor):
                path = box["paths"]["storage"]
            return original(path, *args, **kwargs)

        monkeypatch.setattr(os, "stat", changed)
    else:
        flags = {"nonappend": b"1", "readonly": b"2000", "malformed": b"+2001",
                 "oversized": b"1" * 5000, "duplicate-flags": b"2001"}[kind]
        observe_bytes(monkeypatch, proc / "fdinfo" / str(descriptor),
                      lambda raw: b"flags:\t" + flags + b"\n" + (
                          b"flags:\t2001\n" if kind == "duplicate-flags" else b""))
    before = {r: fingerprint(p) for r, p in box["paths"].items()}
    code, result = cli(capsys, apply=True)
    assert code == 2 and not result["effect_attempted"]
    unchanged(box, before)


@pytest.mark.parametrize("which", ["storage", "supervisor"])
@pytest.mark.parametrize("kind", ["absent", "duplicate", "exited"])
def test_unambiguous_current_process_census(box, which, kind, monkeypatch, capsys):
    original = Path.iterdir
    pid = box["children"][which].pid
    if kind == "exited":
        (box["controls"][which] / "finish").touch()
        box["children"][which].wait(timeout=10)
    elif kind == "duplicate":
        original_child = box["children"][which]
        environment = dict(os.environ)
        if which != "supervisor":
            environment[migration.supervise.OWNERSHIP_ENV] = HOST
            with box["paths"][which].open("ab", buffering=0) as output:
                duplicate = subprocess.Popen(original_child.args, stdout=output, stderr=output,
                                             start_new_session=True, env=environment)
        else:
            environment.pop(migration.supervise.OWNERSHIP_ENV, None)
            duplicate = subprocess.Popen(original_child.args, stdout=subprocess.DEVNULL,
                                         stderr=subprocess.DEVNULL, env=environment)
        box["children"]["duplicate"] = duplicate

    def listed(path):
        result = list(original(path))
        if path == Path("/proc"):
            candidate = path / str(pid)
            if kind == "absent":
                result = [p for p in result if p != candidate]
        return iter(result)

    monkeypatch.setattr(Path, "iterdir", listed)
    code, result = cli(capsys, apply=True)
    assert code == 2 and not result["effect_attempted"]


@pytest.mark.parametrize("kind", ["start-change", "mode-drift", "rename-before", "fchmod-failure",
                                  "second-failure", "late-rename"])
def test_immediate_rechecks_partial_stop_and_late_rename_uncertainty(box, kind, monkeypatch, capsys):
    original_fchmod = os.fchmod
    effects = []
    first = box["paths"]["storage"]
    second = box["paths"]["tiers"]
    originals = {r: fingerprint(p) for r, p in box["paths"].items()}
    if kind == "start-change":
        pid = box["children"]["storage"].pid
        reads = 0

        def changing_stat(raw):
            nonlocal reads
            reads += 1
            if reads < 4:
                return raw
            closing = raw.rfind(b")")
            fields = raw[closing + 1:].split()
            fields[19] = str(int(fields[19]) + 1).encode()
            return raw[:closing + 1] + b" " + b" ".join(fields)

        observe_bytes(monkeypatch, Path("/proc") / str(pid) / "stat", changing_stat)
    elif kind in ("mode-drift", "rename-before"):
        # Raw named stat observation triggers drift before first effect.
        original_stat = os.stat
        reads = 0

        def drifting(path, *args, **kwargs):
            nonlocal reads
            if path == "pb-role-storage.log" and kwargs.get("dir_fd") is not None:
                reads += 1
                if reads == 2:
                    if kind == "mode-drift":
                        first.chmod(0o620)
                    else:
                        first.rename(first.with_name("old-storage"))
                        first.write_bytes(b"replacement")
                        first.chmod(0o664)
            return original_stat(path, *args, **kwargs)

        monkeypatch.setattr(os, "stat", drifting)

    def fchmod(fd, mode):
        role = next(r for r, fp in originals.items()
                    if (os.fstat(fd).st_dev, os.fstat(fd).st_ino) == fp[:2])
        effects.append(role)
        if kind == "fchmod-failure" or (kind == "second-failure" and role == "tiers"):
            raise OSError(errno.EPERM, "private fchmod refusal")
        if kind == "late-rename" and role == "storage":
            first.rename(first.with_name("old-storage"))
            first.write_bytes(b"replacement")
            first.chmod(0o664)
        original_fchmod(fd, mode)

    monkeypatch.setattr(os, "fchmod", fchmod)
    code, result = cli(capsys, apply=True)
    assert code == 2
    if kind in ("start-change", "mode-drift", "rename-before"):
        assert effects == [] and not result["effect_attempted"]
    else:
        assert result["status"] == "partial-uncertain"
        assert effects == (["storage", "tiers"] if kind == "second-failure" else ["storage"])
        if kind == "second-failure":
            assert stat.S_IMODE(first.stat().st_mode) == 0o600
            assert stat.S_IMODE(second.stat().st_mode) == 0o664
        elif kind == "late-rename":
            old = first.with_name("old-storage")
            assert stat.S_IMODE(old.stat().st_mode) == 0o600
            assert stat.S_IMODE(first.stat().st_mode) == 0o664
            assert fingerprint(old) == originals["storage"]
            assert result["leaves"][0]["disposition"] == "effect-uncertain"
        else:
            assert stat.S_IMODE(first.stat().st_mode) == 0o664
    assert stat.S_IMODE(box["paths"]["metrics"].stat().st_mode) == 0o664


def test_operator_syscall_surface_no_content_mutation_signals_or_manager(box, monkeypatch, capsys):
    # Guards forbid dangerous OS operations. They are not mocked proof verdicts.
    def forbidden(*_args, **_kwargs):
        raise AssertionError("operator attempted forbidden effect")

    original_open = os.open
    original_path_open = Path.open

    def read_only_open(path, flags, *args, **kwargs):
        assert not flags & (os.O_CREAT | os.O_TRUNC)
        assert flags & os.O_ACCMODE == os.O_RDONLY
        return original_open(path, flags, *args, **kwargs)

    def read_only_stream(path, mode="r", *args, **kwargs):
        assert not any(flag in mode for flag in "wax+")
        return original_path_open(path, mode, *args, **kwargs)

    with monkeypatch.context() as guard:
        for name in ("chmod", "chown", "fchown", "truncate", "ftruncate", "pwrite", "write",
                     "rename", "replace", "kill", "killpg"):
            guard.setattr(os, name, forbidden)
        guard.setattr(os, "open", read_only_open)
        guard.setattr(Path, "open", read_only_stream)
        code, result = cli(capsys, apply=True)
    assert code == 0 and result["status"] == "applied"

#!/usr/bin/env python3
"""Plan the exact three owned legacy role-log modes; --apply changes only metadata.

Source observations are NOT loaded-import memory attestation, owning-claim proof,
role adoption or retention evidence. Namespace checks are best effort, not atomic.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import socket
import stat
import sys
from contextlib import ExitStack
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
import fleet_roster  # noqa: E402
import publish_runtime  # noqa: E402
import role_log_identity  # noqa: E402
import supervise  # noqa: E402

sys.path.insert(0, str(supervise.RUNTIME_ROOT / "src"))
from prismabuild import core  # noqa: E402

# Closed production scope. Private controls substitute filesystem/runtime facts,
# never these verification functions or their verdicts. No CLI policy overrides.
_LOG_DIRECTORY = Path("/home/rob/tmp")
_OWNER_UID = 1000
_ROLES = ("storage", "tiers", "metrics")
_LIMIT = 1024 * 1024


class _AbsentRoleLog(FileNotFoundError):
    """A fixed diagnostic leaf was absent at its initial descriptor open."""


def _read(path: Path, limit: int = _LIMIT) -> bytes:
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise OSError("oversized runtime observation")
    return raw


def _process(pid: int) -> dict:
    base = supervise.PROC / str(pid)
    raw = _read(base / "stat", 4096)
    opening, closing = raw.find(b"("), raw.rfind(b")")
    fields = raw[closing + 1:].split()
    try:
        if (opening <= 0 or closing < opening or len(fields) < 20
                or int(raw[:opening].strip()) != pid):
            raise ValueError("malformed stat")
        state = fields[0].decode("ascii")
        start, group, session = int(fields[19]), int(fields[2]), int(fields[3])
        if start <= 0 or state not in ("R", "S", "D", "I"):
            raise ValueError("not a current serving process")
        status = _read(base / "status", 65536)
        uid_lines = [line.split()[1:] for line in status.splitlines()
                     if line.startswith(b"Uid:")]
        if (len(uid_lines) != 1 or len(uid_lines[0]) != 4
                or any(int(uid) != _OWNER_UID for uid in uid_lines[0])
                or base.stat().st_uid != _OWNER_UID):
            raise ValueError("foreign process UID")
        cmdline = _read(base / "cmdline")
        if not cmdline.endswith(b"\0"):
            raise ValueError("unterminated cmdline")
        argv = [part.decode("utf-8", "strict") for part in cmdline[:-1].split(b"\0")]
        if (len(argv) < 2 or not argv[1] or argv[1].startswith("-")
                or re.fullmatch(r"python(?:[0-9]+(?:\.[0-9]+)*)?",
                                Path(argv[0]).name) is None):
            raise ValueError("unsupported Python script invocation")
        script = supervise._script_of(pid, argv, supervise.PROC)
        if script is None:
            raise ValueError("unresolved process script")
        return {"pid": pid, "start_ticks": start, "uid": _OWNER_UID,
                "pgrp": group, "sid": session, "argv": argv,
                "script": str(script)}
    except (ValueError, UnicodeError, IndexError) as exc:
        raise OSError(f"invalid process observation for pid {pid}") from exc


def _generation(root: Path) -> tuple[dict, dict]:
    # The publisher remains the ONLY owner of generation/member integrity rules.
    try:
        qualified, receipt = publish_runtime._sealed_generation(root.name)
    except SystemExit as exc:
        raise OSError(str(exc)) from exc
    if qualified != root:
        raise OSError("process source is not a direct sealed generation")
    return ({"generation": root.name, "commit": receipt["commit"],
             "root": str(root), "receipt_sha256": publish_runtime._sha256(
                 root / "RUNTIME_VERSION.json")}, receipt)


def _source(process: dict, allowed: tuple[str, ...]) -> tuple[dict, dict]:
    script = Path(process["script"])
    if script.parent.name == "tools":
        root = script.parent.parent
    elif script.parent.name == "fleet" and script.parent.parent.name == "tools":
        root = script.parents[2]
    else:
        raise OSError("unsupported script member path")
    identity, receipt = _generation(root)
    member = script.relative_to(root).as_posix()
    if member not in allowed or member not in receipt["files"]:
        raise OSError("undeclared or wrong generation script member")
    identity.update(member=member, member_sha256=receipt["files"][member])
    return identity, receipt


def _roster(root: Path, receipt: dict, host: str) -> dict[str, list[str]]:
    member = "tools/fleet/fleet_boxes.json"
    if member not in receipt["files"]:
        raise OSError("roster is not a qualified generation member")
    try:
        value = json.loads(_read(root / member))
        box = value["boxes"][host]
        if (not isinstance(box, dict)
                or fleet_roster.box_status(host, box)[0] != fleet_roster.ACTIVE):
            raise ValueError("absent host")
        roles = box.get("roles")
        if not isinstance(roles, dict) or set(roles) != set(_ROLES):
            raise ValueError("exact three roles must be declared")
        for args in roles.values():
            if not isinstance(args, list) or any(not isinstance(a, str) for a in args):
                raise ValueError("unsupported declared role argv")
        if any(a == "--log" or a.startswith("--log=")
               for args in roles.values() for a in args):
            raise ValueError("custom diagnostic path declaration")
        return roles
    except (KeyError, TypeError, ValueError) as exc:
        raise OSError("invalid qualified host declaration") from exc


def _candidates() -> dict[str, list[int]]:
    scripts = {"supervise.py": "supervisor",
               **{supervise.ROLE_SCRIPTS[r]: r for r in _ROLES}}
    result: dict[str, list[int]] = {r: [] for r in ("supervisor", *_ROLES)}
    entries = list(supervise.PROC.iterdir())
    if len(entries) > 65536:
        raise OSError("oversized process census")
    for entry in entries:
        if not entry.name.isdecimal() or int(entry.name) <= 0:
            continue
        try:
            # UID is a qualification requirement, not a candidate filter: a
            # foreign-UID role-shaped process must not disappear from ambiguity.
            # Unreadable census evidence refuses rather than implying absence.
            raw = _read(entry / "cmdline")
        except FileNotFoundError:
            continue  # no process remains at this candidate observation
        argv = raw.split(b"\0")
        if len(argv) > 1:
            name = Path(os.fsdecode(argv[1])).name
            if name in scripts:
                result[scripts[name]].append(int(entry.name))
    if any(len(pids) != 1 for pids in result.values()):
        raise OSError("absent or ambiguous supervisor/role writer census")
    return result


def _observe() -> dict:
    if os.getuid() != _OWNER_UID or os.geteuid() != _OWNER_UID:
        raise OSError("acting owner must be UID 1000")
    host = socket.gethostname()
    boot = _read(supervise.PROC / "sys/kernel/random/boot_id", 128).decode("ascii").strip()
    if re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", boot) is None:
        raise OSError("invalid boot identity")
    published_root = supervise._current_root().resolve(strict=True)
    published, published_receipt = _generation(published_root)
    declaration = _roster(published_root, published_receipt, host)
    candidates = _candidates()
    supervisor = _process(candidates["supervisor"][0])
    supervisor_source, supervisor_receipt = _source(
        supervisor, ("tools/supervise.py", "tools/fleet/supervise.py"))
    supervisor_root = Path(supervisor_source["root"])
    if (supervisor_receipt["files"].get("tools/fleet/fleet_boxes.json") !=
            published_receipt["files"]["tools/fleet/fleet_boxes.json"]
            or _roster(supervisor_root, supervisor_receipt, host) != declaration):
        raise OSError("published and resolved supervisor roster bytes differ")
    roles = {}
    for role in _ROLES:
        pid = candidates[role][0]
        process = _process(pid)
        source, receipt = _source(process, (f"tools/{supervise.ROLE_SCRIPTS[role]}",))
        root = Path(source["root"])
        if (receipt["files"].get("tools/fleet/fleet_boxes.json") !=
                published_receipt["files"]["tools/fleet/fleet_boxes.json"]
                or _roster(root, receipt, host) != declaration):
            raise OSError("resolved role and published roster bytes differ")
        if (process["argv"][2:] != declaration[role]
                or process["pgrp"] != pid or process["sid"] != pid
                or not supervise._is_fleet_loop(
                    pid, [root], supervise.PROC, supervise.ROLE_SCRIPTS[role])):
            raise OSError("writer argv/environment/leadership not proven")
        environ = _read(supervise.PROC / str(pid) / "environ")
        marks = [part.partition(b"=")[2] for part in environ.split(b"\0")
                 if part.partition(b"=")[0] == supervise.OWNERSHIP_ENV.encode()]
        if marks != [host.encode()]:
            raise OSError("ambiguous writer ownership environment")
        roles[role] = {"process": process, "resolved_source": source}
    candidate_paths = (__file__, role_log_identity.__file__, fleet_roster.__file__,
                       publish_runtime.__file__, supervise.__file__, core.__file__)
    if any(not isinstance(path, str) for path in candidate_paths):
        raise OSError("candidate source path unknown")
    candidate_source = {str(Path(path).resolve(strict=True)):
                        publish_runtime._sha256(Path(path))
                        for path in candidate_paths if isinstance(path, str)}
    # Full argv remains ephemeral internal proof, never the public projection.
    return {"host": host, "boot_id": boot, "observed_published": published,
            "observed_supervisor": {"process": supervisor,
                                    "resolved_source": supervisor_source},
            "observed_roles": roles,
            "candidate_source": candidate_source,
            "loaded_import_bytes": "UNKNOWN", "owning_claim": "UNKNOWN"}


def _report_process(process: dict) -> dict:
    """Expose necessary kernel/source observations, not arbitrary argv values."""
    report = {key: process[key] for key in
              ("pid", "start_ticks", "uid", "pgrp", "sid", "script")}
    report["argv_sha256"] = core.canonical_sha256(process["argv"])
    return report


def _report_context(context: dict) -> dict:
    report = {key: context[key] for key in
              ("host", "boot_id", "observed_published", "candidate_source",
               "loaded_import_bytes", "owning_claim")}
    supervisor = context["observed_supervisor"]
    report["observed_supervisor"] = {
        "process": _report_process(supervisor["process"]),
        "resolved_source": supervisor["resolved_source"]}
    report["observed_roles"] = {
        role: {"process": _report_process(proof["process"]),
               "resolved_source": proof["resolved_source"]}
        for role, proof in context["observed_roles"].items()}
    return report


def _metadata(info: os.stat_result) -> dict:
    return {"dev": info.st_dev, "ino": info.st_ino, "uid": info.st_uid,
            "gid": info.st_gid, "nlink": info.st_nlink,
            "mode": stat.S_IMODE(info.st_mode), "size": info.st_size,
            "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns}


def _check_leaf(fd: int, directory: int, name: str, original: dict | None,
                mode: int | None = None) -> dict:
    role_log_identity.check_directory(directory, _LOG_DIRECTORY, _OWNER_UID)
    held = os.fstat(fd)
    named = os.stat(name, dir_fd=directory, follow_symlinks=False)
    for info in (held, named):
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != _OWNER_UID
                or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) not in (0o664, 0o600)):
            raise OSError("unsafe legacy diagnostic leaf")
    value = _metadata(held)
    if (held.st_dev, held.st_ino) != (named.st_dev, named.st_ino):
        raise OSError("renamed diagnostic leaf")
    for key in ("dev", "ino", "uid", "gid", "nlink", "mode"):
        expected = mode if key == "mode" and mode is not None else (
            original[key] if original is not None else value[key])
        if value[key] != expected or _metadata(named)[key] != expected:
            raise OSError("diagnostic identity or mode drift")
    return value


def _writers(context: dict, role: str, fd: int) -> list[dict]:
    process = context["observed_roles"][role]["process"]
    observations = role_log_identity.check_append_descriptors(
        process["pid"], os.fstat(fd), supervise.PROC)
    if any(re.fullmatch(r"[0-7]{1,24}", item["flags_text"]) is None
           for item in observations):
        raise OSError("malformed append flags")
    if _process(process["pid"]) != process:
        raise OSError("writer incarnation changed during FD observation")
    return observations


def run(*, apply: bool = False) -> dict:
    """Consume one ephemeral full plan; no retries, stored-plan replay or rollback."""
    result = {"schema": "prismabuild.exact_role_log_metadata.v1",
              "operation": "apply" if apply else "plan", "status": "refused",
              "leaves": [], "effect_attempted": False,
              "limitations": ["loaded Python imports UNKNOWN; resolved sealed source only",
                              "no owning-claim, adoption, retention or live byte-equality proof",
                              "best-effort namespace checks; late rename may change held old inode"]}
    changed = False
    stage = "qualification"
    try:
        with ExitStack() as stack:
            context = _observe()
            result["qualification"] = _report_context(context)
            directory = os.open(_LOG_DIRECTORY, os.O_RDONLY | os.O_DIRECTORY |
                                os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
            stack.callback(os.close, directory)
            role_log_identity.check_directory(directory, _LOG_DIRECTORY, _OWNER_UID)
            result["directory"] = _metadata(os.fstat(directory))
            held = []
            for role in _ROLES:
                name = supervise._role_log_name(role)
                stage = f"{role}:preflight"
                try:
                    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC |
                                 os.O_NONBLOCK, dir_fd=directory)
                except FileNotFoundError as exc:
                    raise _AbsentRoleLog(f"fixed role log absent: {role}") from exc
                stack.callback(os.close, fd)
                before = _check_leaf(fd, directory, name, None)
                writer_fds = _writers(context, role, fd)
                leaf = {"role": role, "name": name, "before": before,
                        "writer_fds_before": writer_fds, "disposition": "qualified"}
                result["leaves"].append(leaf)
                held.append((role, name, fd, leaf))
            if _observe() != context:
                raise OSError("qualification changed during full plan")
            for role, name, fd, leaf in held:
                stage = f"{role}:precheck"
                if _observe() != context:
                    raise OSError("current source/process proof changed")
                _check_leaf(fd, directory, name, leaf["before"])
                _writers(context, role, fd)
                leaf["directory_before"] = _metadata(os.fstat(directory))
                leaf["qualification_rechecked_pre"] = True
                if not apply:
                    leaf["disposition"] = "would-change" if leaf["before"]["mode"] == 0o664 else "qualified-noop"
                    continue
                if leaf["before"]["mode"] == 0o664:
                    stage = f"{role}:fchmod"
                    result["effect_attempted"] = True
                    leaf["disposition"] = "effect-uncertain"
                    # The ONLY metadata effect, against the original qualified FD.
                    os.fchmod(fd, 0o600)
                    changed = True
                    leaf["held_after_effect"] = _metadata(os.fstat(fd))
                stage = f"{role}:postcheck"
                leaf["after"] = _check_leaf(fd, directory, name, leaf["before"], 0o600)
                leaf["writer_fds_after"] = _writers(context, role, fd)
                if _observe() != context:
                    raise OSError("post-effect source/process proof changed")
                leaf["directory_after"] = _metadata(os.fstat(directory))
                leaf["qualification_rechecked_post"] = True
                leaf["disposition"] = "changed" if leaf["before"]["mode"] == 0o664 else "qualified-noop"
            result["status"] = "applied" if apply else "planned"
    except (OSError, ValueError, UnicodeError) as exc:
        for leaf in result["leaves"]:
            if leaf["disposition"] == "qualified":
                leaf["disposition"] = "not-applied"
        result["stage"] = stage
        result["error"] = str(exc)[:240]
        result["status"] = ("partial-uncertain" if changed or result["effect_attempted"]
                            else "absent-no-effect" if isinstance(exc, _AbsentRoleLog)
                            else "refused")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Exact three-role metadata plan; source observation, not adoption")
    parser.add_argument("--apply", action="store_true", help="apply held-FD 0600 metadata only")
    args = parser.parse_args(argv)
    result = run(apply=args.apply)
    sys.stdout.write(core._sorted_lf_bytes(result).decode("utf-8"))
    return 0 if result["status"] in ("planned", "applied") else 2


if __name__ == "__main__":
    raise SystemExit(main())

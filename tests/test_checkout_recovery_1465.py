"""PB #1465: an exact banked orphan is the only checkout recovery may delete.

These are private filesystem fixtures, not a maintenance operation. Git bundles,
CAS requests, queue transitions and the materializer/cleanup owner are real. Only
host admission, root identity and an injected initial cleanup failure are seams.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tarfile
from typing import Callable

import pytest

from admitted_queue_fixture import AdmittedQueueFixture
from test_pool import _materialization_item
from prismabuild import checkout_recovery as recovery
from prismabuild import core as pb
from prismabuild import materialize, pool


OWNER = "test-pb1465-exact-orphan-recovery"
PLAN_SCHEMA = "prismabuild.checkout_recovery_plan.v1"
RESULT_SCHEMA = "prismabuild.checkout_recovery_result.v1"
_OMITTED = object()


def _digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(128 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def _snapshot(root: Path, *, ignored: str | None = None) -> dict[str, object]:
    """Capture bytes, links and identities without following links or large reads."""
    result: dict[str, object] = {}
    pending = [root]
    while pending:
        path = pending.pop()
        relative = str(path.relative_to(root))
        try:
            info = path.lstat()
        except FileNotFoundError:
            result[relative] = ("absent",)
            continue
        identity = (info.st_dev, info.st_ino, info.st_uid, info.st_mode)
        if stat.S_ISLNK(info.st_mode):
            result[relative] = ("link", identity, os.readlink(path))
        elif stat.S_ISDIR(info.st_mode):
            result[relative] = ("directory", identity)
            with os.scandir(path) as children:
                pending.extend(Path(child.path) for child in children
                               if not (path == root and child.name == ignored))
        elif stat.S_ISREG(info.st_mode):
            result[relative] = ("file", identity, info.st_size, _digest(path))
        else:
            result[relative] = ("special", identity)
    return result


def _json(path: Path, value: object) -> None:
    if path.exists():
        path.chmod(0o644)
    path.write_bytes(pb._canonical_bytes(value))


def _git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *arguments], check=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env={**os.environ, "GIT_CONFIG_GLOBAL": os.devnull,
             "GIT_CONFIG_NOSYSTEM": "1"},
    ).stdout.strip()


@dataclass
class RecoveryCase:
    workspace: Path
    queue: AdmittedQueueFixture
    cas: pb.PrismaBuildCAS
    request: Path
    action: dict[str, object]
    claim: dict[str, object]
    directory: Path
    checkout: Path
    terminal_path: Path
    entry: dict[str, object]
    local_root: Path
    bank_root: Path
    gate: Path
    proc_root: Path
    protected_root: Path
    cleanup_calls: list[Path]

    @property
    def key(self) -> str:
        return str(self.action["action_key"])

    @property
    def archive(self) -> Path:
        return Path(str(self.entry["archive_path"]))

    def options(self, **changes: object) -> dict[str, object]:
        return {"local_root": self.local_root, "bank_root": self.bank_root,
                "maintenance_gate": self.gate, "maintenance_owner": OWNER,
                "proc_root": self.proc_root, **changes}

    def prepare(self, entries: object = _OMITTED, **changes: object) -> dict[str, object]:
        return recovery.prepare_checkout_recovery(
            self.queue, [self.entry] if entries is _OMITTED else entries,
            **self.options(**changes),
        )

    def apply(self, plan: dict[str, object], *, sha256: str | None = None,
              **changes: object) -> dict[str, object]:
        return recovery.apply_checkout_recovery(
            self.queue, plan,
            expected_plan_sha256=pb.canonical_sha256(plan) if sha256 is None else sha256,
            **self.options(**changes),
        )

    def preserved(self) -> dict[str, object]:
        paths = [
            self.directory, self.local_root, self.bank_root, self.cas.root,
            self.queue.root, self.gate, self.proc_root,
            self.workspace / "materialization-source", self.protected_root,
        ]
        # Fault fixtures can move the original outside its old spelling.
        for field in ("path", "archive_path"):
            path = Path(str(self.entry[field]))
            if path.is_relative_to(self.workspace.parent):
                paths.append(path)
        return {str(path): _snapshot(path, ignored="transition-locks"
                                     if path == self.queue.root else None) for path in paths}

    def bank(self, *, layout: str = "root", omit: str | None = None) -> None:
        archive = self.archive
        if archive.exists():
            archive.chmod(0o644)
        prefix = self.directory.name if layout == "root" else "."

        def select(member: tarfile.TarInfo) -> tarfile.TarInfo | None:
            relative = member.name.removeprefix(prefix + "/")
            return None if omit is not None and relative == omit else member

        with tarfile.open(archive, "w", dereference=False) as target:
            target.add(self.directory, arcname=prefix, filter=select)
        archive.chmod(0o444)
        self.entry["archive_sha256"] = _digest(archive)


@pytest.fixture
def make_case(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Reuse the materializer's Git fixture and real Pool publication/finish."""
    local_root = tmp_path / "checkouts"
    bank_root = tmp_path / "operator-bank"
    proc_root = tmp_path / "proc"
    protected_root = tmp_path / "not-recovery-owned"
    for path in (local_root, bank_root, proc_root, protected_root):
        path.mkdir(mode=0o700)
    for name in ("user-primary", "native-banks", "images", "historical-checkout"):
        path = protected_root / name
        path.mkdir()
        (path / "keep.bin").write_bytes(b"not deletion authority\0" + name.encode())
    (local_root / "unselected-historical-tree").mkdir()
    (local_root / "unselected-historical-tree" / "keep").write_text("keep original")
    gate = tmp_path / "maintenance.json"
    _json(gate, {"schema": "prismabuild.resource-maintenance.v1", "draining": True,
                 "changed_unix": 1465.0, "owner": OWNER, "reason": "fixture only"})
    gate.chmod(0o644)
    monkeypatch.setattr(recovery, "_gate_uid", lambda: os.getuid())
    monkeypatch.setattr(recovery.os, "geteuid", lambda: 0)
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "queue"), capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    queue.ensure_layout()
    worker = Path(__file__).resolve().parents[1] / "tools" / "prismabuild_worker.py"
    unrelated = hashlib.sha256(str(tmp_path).encode()).hexdigest()
    queue.publish(action_key=unrelated, cas_root=protected_root / "active-cas",
                  checkout_root=protected_root / "user-primary", worker_script=worker)
    active = queue.claim(owner="unrelated-live-claim")
    assert active is not None and active["action_key"] == unrelated
    cleanup_calls: list[Path] = []
    actual_cleanup = materialize._cleanup_execution_checkout

    def observed_cleanup(base, temporary, item):
        cleanup_calls.append(Path(temporary))
        return actual_cleanup(base, temporary, item)

    monkeypatch.setattr(materialize, "_cleanup_execution_checkout", observed_cleanup)
    counter = 0

    def create(*, ending: str = "failed", layout: str = "root") -> RecoveryCase:
        nonlocal counter
        counter += 1
        workspace = tmp_path / f"case-{counter}"
        workspace.mkdir()
        item = _materialization_item(workspace)
        source = workspace / "materialization-source"
        snapshot = item["checkout_snapshot"]
        stamp = next(source.glob(f"{pb.PBRUN_STAMP_PREFIX}*"))
        command = [sys.executable, "-c", "pass"]
        action = pb.seal_action({
            "schema": pb.ACTION_SCHEMA_V2,
            "task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                     "task_class": "generation", "determinism": "stochastic",
                     "artifact_family": "generic", "artifact_kind": "generic",
                     "argv": command, "working_directory": ".",
                     "result_path": "result.bin"},
            "inputs": [snapshot["input"]],
            "code_closure": pb.build_code_closure(source, [stamp.name]),
            "params": {"command": command, "cwd": ".",
                       "demand": {"cpu": 1, "mem_gb": 1},
                       "checkout_snapshot": snapshot},
            "environment": {"variables": {}, "toolchain": {}},
            "execution_scope": {"portability": "portable", "platform_key": None,
                                "host_class": None},
        })
        cas = pb.PrismaBuildCAS(str(item["cas_root"]))
        request = cas.publish_action_request(action)
        queue.publish(action_key=action["action_key"], cas_root=cas.root,
                      checkout_snapshot=snapshot, worker_script=worker, max_attempts=1)
        claim = queue.claim(owner=f"recovery-fixture-{counter}")
        assert claim is not None and claim["action_key"] == action["action_key"]
        held: list[Path] = []
        actual_rmtree = materialize.shutil.rmtree

        def retained_cleanup(path, *args, **kwargs):
            if Path(path) in held:
                raise PermissionError("injected root-owned native cleanup failure")
            return actual_rmtree(path, *args, **kwargs)

        with monkeypatch.context() as retain:
            retain.setattr(materialize.shutil, "rmtree", retained_cleanup)
            with materialize._execution_checkout(
                    claim, local_checkout_root=local_root, on_temporary=held.append) as checkout:
                (checkout / "payload.txt").write_bytes(b"dirty tracked bytes\0not in Git\n")
                native = checkout / "native"
                native.mkdir()
                (native / "libkernel.so").write_bytes(b"\x7fELF\0native untracked bytes" * 4096)
                (checkout / "native-link").symlink_to("native/libkernel.so")
                (checkout / "native-directory-link").symlink_to("native", target_is_directory=True)
                (checkout / "empty-native-directory").mkdir()
        directory = held[0]
        assert directory.exists()
        if ending == "withdrawn":
            queue.withdraw(action["action_key"], reason="fixture cancellation",
                           by=OWNER, signal_child=False)
            terminal_path = queue.finish(action["action_key"], status="withdrawn",
                                         claim_snapshot=claim)
        else:
            terminal_path = queue.finish(action["action_key"], status=ending,
                                         detail={"termination_reason": ending},
                                         claim_snapshot=claim)
        cleanup_calls.clear()
        entry = {"action_key": action["action_key"], "path": str(directory),
                 "archive_path": str(bank_root / f"{directory.name}.tar"),
                 "archive_sha256": "0" * 64}
        case = RecoveryCase(workspace, queue, cas, request, action, claim,
                            directory, checkout, terminal_path, entry,
                            local_root, bank_root, gate, proc_root, protected_root,
                            cleanup_calls)
        case.bank(layout=layout)
        return case

    return create


@pytest.fixture
def case(make_case) -> RecoveryCase:
    return make_case()


def _planned(case: RecoveryCase, **kwargs: object) -> dict[str, object]:
    before = case.preserved()
    plan = case.prepare(**kwargs)
    assert plan.get("complete") is True, json.dumps(plan, sort_keys=True)
    assert plan["schema"] == PLAN_SCHEMA, json.dumps(plan, sort_keys=True)
    assert plan["complete"] is True and plan["status"] == "planned", plan
    assert case.cleanup_calls == []
    assert case.preserved() == before
    return plan


def _refused(case: RecoveryCase, operation: Callable[[], dict[str, object]]) -> dict[str, object]:
    before = case.preserved()
    case.cleanup_calls.clear()
    result = operation()
    assert result["schema"] == RESULT_SCHEMA, result
    assert result["complete"] is False and result["status"] == "refused", result
    assert result["errors"] and all(isinstance(error, str) for error in result["errors"])
    assert result["removed"] == []
    assert case.cleanup_calls == [], "a refusal must precede the one deletion owner"
    assert case.preserved() == before, "refusal changed original or protected bytes"
    return result


def _terminal(case: RecoveryCase, change: Callable[[dict], None]) -> None:
    value = json.loads(case.terminal_path.read_text())
    change(value)
    _json(case.terminal_path, value)


def _append_member(case: RecoveryCase, member: tarfile.TarInfo, data: bytes = b"") -> None:
    case.archive.chmod(0o644)
    with tarfile.open(case.archive, "a") as archive:
        archive.addfile(member, io.BytesIO(data) if member.isfile() else None)
    case.archive.chmod(0o444)
    case.entry["archive_sha256"] = _digest(case.archive)


def _rewrite_member(case: RecoveryCase, relative: str, *, data: bytes | None = None,
                    link_target: str | None = None) -> None:
    replacement = case.archive.with_suffix(".replacement.tar")
    selected = f"{case.directory.name}/{relative}"
    changed = False
    with tarfile.open(case.archive) as source, tarfile.open(replacement, "w") as target:
        for original in source:
            member = copy.copy(original)
            if member.name == selected:
                changed = True
                if link_target is not None:
                    member.linkname = link_target
                if data is not None:
                    member.size = len(data)
                    target.addfile(member, io.BytesIO(data))
                    continue
            if member.isfile():
                with source.extractfile(original) as payload:
                    target.addfile(member, payload)
            else:
                target.addfile(member)
    assert changed, selected
    replacement.chmod(0o444)
    os.replace(replacement, case.archive)
    case.entry["archive_sha256"] = _digest(case.archive)


@pytest.mark.parametrize("ending", ["executed", "failed", "lease_lost", "withdrawn"])
@pytest.mark.parametrize("layout", ["root", "dot"])
def test_authentic_banked_ending_is_planned_then_removed_only_by_cleanup_owner(
        make_case, ending: str, layout: str):
    case = make_case(ending=ending, layout=layout)
    plan = _planned(case)
    entry = plan["entries"][0]
    for key, value in case.entry.items():
        assert entry[key] == value
    identity = case.directory.stat()
    assert entry["directory_identity"]["dev"] == identity.st_dev
    assert entry["directory_identity"]["ino"] == identity.st_ino
    assert entry["directory_identity"]["uid"] == identity.st_uid
    terminal = json.loads(case.terminal_path.read_text())
    assert entry["terminal"]["state"] == case.terminal_path.parent.name
    assert entry["terminal"]["generation"] == terminal["published_unix"]
    assert entry["terminal"]["attempt"] == terminal["attempts"]
    assert entry["terminal"]["snapshot"] == terminal["checkout_snapshot"]
    assert entry["archive"]["manifest_sha256"] == entry["tree"]["manifest_sha256"]
    assert entry["archive"]["member_count"] == entry["tree"]["member_count"]
    assert entry["archive"]["total_bytes"] == entry["tree"]["total_bytes"]
    assert "receipt" not in entry and "profile" not in entry
    assert "receipt" not in entry["terminal"] and "profile" not in entry["terminal"]
    protected = {str(path): _snapshot(path) for path in (
        case.bank_root, case.cas.root, case.queue.root, case.protected_root,
        case.local_root / "unselected-historical-tree", case.gate,
    )}
    native_digest = _digest(case.checkout / "native" / "libkernel.so")
    with tarfile.open(case.archive) as bank:
        prefix = case.directory.name if layout == "root" else "."
        assert bank.getmember(f"{prefix}/checkout/empty-native-directory").isdir()
        assert bank.getmember(f"{prefix}/checkout/native-link").linkname == "native/libkernel.so"
        with bank.extractfile(f"{prefix}/checkout/payload.txt") as dirty:
            assert dirty.read() == b"dirty tracked bytes\0not in Git\n"
        with bank.extractfile(f"{prefix}/checkout/native/libkernel.so") as native:
            assert hashlib.sha256(native.read()).hexdigest() == native_digest
    result = case.apply(plan)
    assert result["schema"] == RESULT_SCHEMA and result["complete"] is True, result
    assert result["status"] == "applied" and result["removed"] == [str(case.directory)]
    assert result["plan_sha256"] == pb.canonical_sha256(plan)
    assert case.cleanup_calls == [case.directory]
    assert not case.directory.exists() and not case.directory.is_symlink()
    assert {str(path): _snapshot(path) for path in map(Path, protected)} == protected


@pytest.mark.parametrize("fault", [
    "not-list", "null", "empty", "not-object", "extra-field", "missing-key", "missing-path",
    "missing-archive", "missing-digest", "short-key", "uppercase-key",
    "short-digest", "uppercase-digest", "relative-path", "relative-archive",
    "duplicate", "oversized",
])
def test_candidate_input_is_explicit_exact_and_bounded(case, fault):
    entry = dict(case.entry)
    entries: object = [entry]
    if fault == "not-list":
        entries = {"entries": [entry]}
    elif fault == "null":
        entries = None
    elif fault == "empty":
        entries = []
    elif fault == "not-object":
        entries = [str(case.directory)]
    elif fault == "extra-field":
        entry["recursive"] = True
    elif fault.startswith("missing-"):
        key = {"key": "action_key", "path": "path", "archive": "archive_path",
               "digest": "archive_sha256"}[fault.removeprefix("missing-")]
        entry.pop(key)
    elif fault in ("short-key", "uppercase-key"):
        entry["action_key"] = "a" * 12 if fault == "short-key" else "A" * 64
    elif fault in ("short-digest", "uppercase-digest"):
        entry["archive_sha256"] = "a" * 12 if fault == "short-digest" else "A" * 64
    elif fault == "relative-path":
        entry["path"] = case.directory.name
    elif fault == "relative-archive":
        entry["archive_path"] = case.archive.name
    elif fault == "duplicate":
        entries = [entry, dict(entry)]
    else:
        entries = [dict(entry) for _ in range(33)]
    _refused(case, lambda: case.prepare(entries=entries))


@pytest.mark.parametrize("fault", [
    "missing-terminal", "malformed-terminal", "wrong-key", "missing-key",
    "missing-generation", "boolean-generation", "nonfinite-generation",
    "missing-attempt", "negative-attempt", "boolean-attempt", "missing-timestamp",
    "missing-snapshot", "malformed-snapshot", "wrong-snapshot", "missing-request",
    "malformed-request", "wrong-request", "missing-bundle", "missing-head",
    "missing-git", "wrong-head", "tied-endings", "unreadable-sibling",
])
def test_missing_or_conflicting_action_snapshot_and_ending_proof_keeps_original(case, fault):
    if fault == "missing-terminal":
        case.terminal_path.unlink()
    elif fault == "malformed-terminal":
        case.terminal_path.write_text("{not complete json")
    elif fault == "wrong-key":
        _terminal(case, lambda value: value.update(action_key="f" * 64))
    elif fault == "missing-key":
        _terminal(case, lambda value: value.pop("action_key"))
    elif fault == "missing-generation":
        _terminal(case, lambda value: value.pop("published_unix"))
    elif fault == "boolean-generation":
        _terminal(case, lambda value: value.update(published_unix=True))
    elif fault == "nonfinite-generation":
        value = json.loads(case.terminal_path.read_text())
        value["published_unix"] = float("inf")
        case.terminal_path.write_text(json.dumps(value))
    elif fault == "missing-attempt":
        _terminal(case, lambda value: value.pop("attempts"))
    elif fault == "negative-attempt":
        _terminal(case, lambda value: value.update(attempts=-1))
    elif fault == "boolean-attempt":
        _terminal(case, lambda value: value.update(attempts=True))
    elif fault == "missing-timestamp":
        _terminal(case, lambda value: value.pop("finished_unix"))
    elif fault == "missing-snapshot":
        _terminal(case, lambda value: value.pop("checkout_snapshot"))
    elif fault == "malformed-snapshot":
        _terminal(case, lambda value: value.update(checkout_snapshot={"commit": "a" * 40}))
    elif fault == "wrong-snapshot":
        _terminal(case, lambda value: value["checkout_snapshot"].update(commit="f" * 40))
    elif fault == "missing-request":
        case.request.unlink()
    elif fault == "malformed-request":
        case.request.chmod(0o644)
        case.request.write_text("{partial immutable request")
        case.request.chmod(0o444)
    elif fault == "wrong-request":
        value = copy.deepcopy(case.action)
        value["params"]["cwd"] = "different"
        _json(case.request, value)
        case.request.chmod(0o444)
    elif fault == "missing-bundle":
        case.cas.input_path(case.claim["checkout_snapshot"]["input"]).unlink()
    elif fault == "missing-head":
        (case.checkout / ".git" / "HEAD").unlink()
        case.bank()
    elif fault == "missing-git":
        shutil.rmtree(case.checkout / ".git")
        case.bank()
    elif fault == "wrong-head":
        _git(case.checkout, "-c", "user.name=PB recovery test",
             "-c", "user.email=test@example.invalid", "commit", "--allow-empty",
             "-qm", "not the sealed commit")
        case.bank()
    else:
        sibling = case.queue.item_path(pool.DONE, case.key)
        if fault == "tied-endings":
            _json(sibling, json.loads(case.terminal_path.read_text()))
        else:
            sibling.write_text("{unreadable sibling")
    _refused(case, case.prepare)


@pytest.mark.parametrize("state", [pool.READY, pool.CLAIMED, "lease"])
def test_live_queue_owner_or_lease_refuses_even_beside_complete_terminal(case, state):
    if state == "lease":
        _json(case.queue.lease_path(case.key), {
            "schema": pool.POOL_LEASE_SCHEMA_V1, "action_key": case.key,
            "owner": "still-live", "host": pool.socket.gethostname(),
            "heartbeat_unix": pool._now(), "pid": os.getpid(),
            "published_unix": case.claim["published_unix"],
        })
    else:
        _json(case.queue.item_path(state, case.key), case.claim)
    _refused(case, case.prepare)


@pytest.mark.parametrize("fault", [
    "digest", "corrupt-tar", "missing-native", "missing-empty-directory",
    "extra-file", "duplicate-member", "parent-traversal", "absolute-member",
    "escaping-symlink", "escaping-hardlink", "wrong-symlink-target", "wrong-native-bytes",
    "incomplete-tar", "trailing-payload", "writable", "multiple-links",
    "symlink", "bank-symlink", "writable-bank", "outside-bank", "bank-under-checkout",
])
def test_archive_is_a_complete_stable_nofollow_copy_not_merely_a_digest(case, fault):
    if fault == "digest":
        case.entry["archive_sha256"] = "0" * 64
    elif fault == "corrupt-tar":
        case.archive.chmod(0o644)
        case.archive.write_bytes(b"not a tar archive")
        case.archive.chmod(0o444)
        case.entry["archive_sha256"] = _digest(case.archive)
    elif fault.startswith("missing-"):
        omitted = "checkout/native/libkernel.so" if fault == "missing-native" else "checkout/empty-native-directory"
        case.bank(omit=omitted)
    elif fault in ("extra-file", "duplicate-member", "parent-traversal", "absolute-member"):
        name = {"extra-file": f"{case.directory.name}/checkout/unbanked-extra",
                "duplicate-member": f"{case.directory.name}/checkout/payload.txt",
                "parent-traversal": "../escape", "absolute-member": "/escape"}[fault]
        member = tarfile.TarInfo(name)
        data = b"not original bytes"
        member.size = len(data)
        _append_member(case, member, data)
    elif fault in ("escaping-symlink", "escaping-hardlink"):
        member = tarfile.TarInfo(f"{case.directory.name}/checkout/extra-link")
        member.type = tarfile.LNKTYPE if fault == "escaping-hardlink" else tarfile.SYMTYPE
        member.linkname = "../../../outside"
        _append_member(case, member)
    elif fault == "wrong-symlink-target":
        _rewrite_member(case, "checkout/native-link", link_target="payload.txt")
    elif fault == "wrong-native-bytes":
        _rewrite_member(case, "checkout/native/libkernel.so", data=b"different native build")
    elif fault in ("incomplete-tar", "trailing-payload"):
        case.archive.chmod(0o644)
        if fault == "incomplete-tar":
            with case.archive.open("rb") as source:
                raw = source.read().rstrip(b"\0")
            with case.archive.open("wb") as target:
                target.write(raw)
                target.write(b"\0" * ((-len(raw)) % 512 + 512))
        else:
            with case.archive.open("ab") as target:
                target.write(b"not part of the complete bank")
        case.archive.chmod(0o444)
        case.entry["archive_sha256"] = _digest(case.archive)
    elif fault == "writable":
        case.archive.chmod(0o644)
    elif fault == "multiple-links":
        os.link(case.archive, case.bank_root / "untrusted-second-link.tar")
    elif fault == "symlink":
        original = case.archive.with_suffix(".original.tar")
        case.archive.rename(original)
        case.archive.symlink_to(original)
    elif fault == "bank-symlink":
        actual = case.bank_root.with_name("actual-bank")
        case.bank_root.rename(actual)
        case.bank_root.symlink_to(actual, target_is_directory=True)
    elif fault == "writable-bank":
        case.bank_root.chmod(0o777)
    elif fault == "outside-bank":
        moved = case.workspace / case.archive.name
        case.archive.rename(moved)
        case.entry["archive_path"] = str(moved)
    else:
        inside = case.directory / "operator-bank"
        inside.mkdir()
        archive = inside / case.archive.name
        case.archive.rename(archive)
        case.entry["archive_path"] = str(archive)
        _refused(case, lambda: case.prepare(bank_root=inside))
        return
    _refused(case, case.prepare)


@pytest.mark.parametrize("fault", [
    "root-symlink", "parent-symlink", "outside-root", "wrong-key-prefix",
    "short-token", "long-token", "bad-token", "namespace-itself", "nested-root",
    "terminal-symlink", "request-symlink", "git-symlink",
])
def test_only_an_exact_generated_root_and_nofollow_metadata_can_be_selected(case, fault):
    if fault == "root-symlink":
        actual = case.local_root / "kept-original"
        case.directory.rename(actual)
        case.directory.symlink_to(actual, target_is_directory=True)
    elif fault == "parent-symlink":
        actual = case.local_root.with_name("actual-local-root")
        case.local_root.rename(actual)
        case.local_root.symlink_to(actual, target_is_directory=True)
    elif fault == "outside-root":
        actual = case.workspace / case.directory.name
        case.directory.rename(actual)
        case.entry["path"] = str(actual)
    elif fault in ("wrong-key-prefix", "short-token", "long-token", "bad-token"):
        name = {"wrong-key-prefix": "f" * 12 + ".abcdefgh",
                "short-token": case.key[:12] + ".abc",
                "long-token": case.key[:12] + ".abcdefghi",
                "bad-token": case.key[:12] + ".abcd!fgh"}[fault]
        actual = case.local_root / name
        case.directory.rename(actual)
        case.entry["path"] = str(actual)
    elif fault == "namespace-itself":
        case.entry["path"] = str(case.local_root)
    elif fault == "nested-root":
        case.entry["path"] = str(case.checkout)
    elif fault in ("terminal-symlink", "request-symlink"):
        path = case.terminal_path if fault == "terminal-symlink" else case.request
        actual = path.with_name(path.name + ".original")
        path.rename(actual)
        path.symlink_to(actual)
    else:
        path = case.checkout / ".git"
        actual = case.workspace / "original-git"
        path.rename(actual)
        path.symlink_to(actual, target_is_directory=True)
        case.bank()
    _refused(case, case.prepare)


@pytest.mark.parametrize("fault", ["missing", "malformed", "foreign-owner", "open",
                                  "missing-epoch", "boolean-epoch", "symlink", "writable"])
def test_closed_named_maintenance_gate_is_required_without_changing_it(case, fault):
    if fault == "missing":
        case.gate.unlink()
    elif fault == "malformed":
        case.gate.write_text("{partial gate")
    elif fault == "symlink":
        original = case.gate.with_suffix(".original")
        case.gate.rename(original)
        case.gate.symlink_to(original)
    elif fault == "writable":
        case.gate.chmod(0o666)
    else:
        value = json.loads(case.gate.read_text())
        if fault == "foreign-owner":
            value["owner"] = "another-root-approved-owner"
        elif fault == "open":
            value["draining"] = False
        elif fault == "missing-epoch":
            value.pop("changed_unix")
        else:
            value["changed_unix"] = True
        _json(case.gate, value)
    _refused(case, case.prepare)


def _process(case: RecoveryCase, *, argv: bytes = b"unrelated\0", start: int = 900,
             pid: int = 101) -> Path:
    process = case.proc_root / str(pid)
    process.mkdir()
    (process / "stat").write_text(f"{pid} (worker (recovery)) S " + "0 " * 18 + f"{start} 0")
    (process / "status").write_text("Name:\tworker\nState:\tS (sleeping)\n")
    (process / "cmdline").write_bytes(argv)
    (process / "cwd").symlink_to(case.protected_root, target_is_directory=True)
    (process / "exe").symlink_to(sys.executable)
    (process / "fd").mkdir()
    return process


@pytest.mark.parametrize("reference", ["cwd", "exe", "fd", "cmdline-path", "cmdline-key", "pid"])
def test_live_process_reference_prevents_recovery(case, reference):
    process = _process(case)
    if reference in ("cwd", "exe"):
        (process / reference).unlink()
        (process / reference).symlink_to(case.checkout, target_is_directory=True)
    elif reference == "fd":
        (process / "fd" / "7").symlink_to(case.checkout / "native" / "libkernel.so")
    elif reference == "cmdline-path":
        (process / "cmdline").write_bytes(b"python3\0" + os.fsencode(case.checkout) + b"\0")
    elif reference == "cmdline-key":
        (process / "cmdline").write_bytes(b"worker\0--action-key\0" + case.key.encode() + b"\0")
    else:
        _terminal(case, lambda value: value.update(child_pid=101))
    _refused(case, case.prepare)


@pytest.mark.parametrize("missing", ["stat", "cmdline", "cwd", "exe", "fd"])
def test_incomplete_process_census_is_not_proof_of_death(case, missing):
    process = _process(case)
    path = process / missing
    path.rmdir() if missing == "fd" else path.unlink()
    _refused(case, case.prepare)


def test_an_unrelated_readable_process_does_not_block_the_exact_orphan(case):
    _process(case)
    _planned(case)


@pytest.mark.parametrize("fault", [
    "tree-bytes", "tree-added", "tree-removed", "tree-mode", "directory-replaced",
    "archive-replaced", "archive-writable", "gate-epoch", "gate-owner", "gate-open",
    "gate-replaced", "ending-generation", "ending-attempt", "ending-snapshot",
    "ending-status", "ready", "claimed", "lease", "fd",
])
def test_apply_revalidates_every_plan_identity_and_lifetime_before_deletion(case, fault):
    plan = _planned(case)
    if fault == "tree-bytes":
        with (case.checkout / "native" / "libkernel.so").open("ab") as native:
            native.write(b"later dirty native bytes")
    elif fault == "tree-added":
        (case.checkout / "untracked-after-plan").write_text("keep")
    elif fault == "tree-removed":
        (case.checkout / "native-link").unlink()
    elif fault == "tree-mode":
        (case.checkout / "payload.txt").chmod(0o600)
    elif fault == "directory-replaced":
        original = case.local_root / "preserved-old-inode"
        case.directory.rename(original)
        shutil.copytree(original, case.directory, symlinks=True)
    elif fault == "archive-replaced":
        original = case.archive.with_suffix(".old.tar")
        case.archive.rename(original)
        shutil.copyfile(original, case.archive)
        case.archive.chmod(0o444)
    elif fault == "archive-writable":
        case.archive.chmod(0o644)
    elif fault.startswith("gate-"):
        value = json.loads(case.gate.read_text())
        if fault == "gate-epoch":
            value["changed_unix"] += 1
        elif fault == "gate-owner":
            value["owner"] = "replacement-owner"
        elif fault == "gate-open":
            value["draining"] = False
        else:
            original = case.gate.with_suffix(".old")
            case.gate.rename(original)
        _json(case.gate, value)
    elif fault.startswith("ending-"):
        if fault == "ending-generation":
            _terminal(case, lambda value: value.update(published_unix=value["published_unix"] + 1))
        elif fault == "ending-attempt":
            _terminal(case, lambda value: value.update(attempts=value["attempts"] + 1))
        elif fault == "ending-snapshot":
            _terminal(case, lambda value: value["checkout_snapshot"].update(commit="f" * 40))
        else:
            _terminal(case, lambda value: value.update(status="changed-after-root-go"))
    elif fault in ("ready", "claimed"):
        _json(case.queue.item_path(fault, case.key), case.claim)
    elif fault == "lease":
        _json(case.queue.lease_path(case.key), {"schema": pool.POOL_LEASE_SCHEMA_V1,
              "action_key": case.key, "heartbeat_unix": pool._now(), "pid": 101})
    else:
        process = _process(case)
        (process / "fd" / "9").symlink_to(case.checkout / "payload.txt")
    _refused(case, lambda: case.apply(plan))


@pytest.mark.parametrize("sha256", ["0" * 64, "abc", "A" * 64])
def test_apply_requires_rootgo_for_the_exact_canonical_plan_sha(case, sha256):
    plan = _planned(case)
    _refused(case, lambda: case.apply(plan, sha256=sha256))


@pytest.mark.parametrize("field", ["path", "archive_path", "directory_identity", "terminal", "schema"])
def test_even_a_rehashed_tampered_plan_is_not_a_new_selection(case, field):
    plan = copy.deepcopy(_planned(case))
    if field == "schema":
        plan[field] = "not-the-recovery-contract"
    elif field == "path":
        plan["entries"][0][field] = str(case.local_root)
    elif field == "archive_path":
        plan["entries"][0][field] = str(case.request)
    elif field == "directory_identity":
        plan["entries"][0][field]["ino"] += 1
    else:
        plan["entries"][0][field]["generation"] += 1
    _refused(case, lambda: case.apply(plan))


def test_plan_does_not_require_root_but_apply_does(case, monkeypatch):
    monkeypatch.setattr(recovery.os, "geteuid", lambda: 1000)
    plan = _planned(case)
    _refused(case, lambda: case.apply(plan))


def test_all_entries_are_verified_before_any_cleanup_owner_is_called(make_case):
    first, second = make_case(), make_case()
    before = first.preserved(), second.preserved()
    plan = first.prepare(entries=[first.entry, second.entry])
    assert plan["schema"] == PLAN_SCHEMA and plan["complete"] is True, plan
    assert (first.preserved(), second.preserved()) == before
    with (second.checkout / "native" / "libkernel.so").open("ab") as native:
        native.write(b"second entry drift must protect the first too")
    second_before = second.preserved()
    _refused(first, lambda: first.apply(plan))
    assert second.preserved() == second_before
    assert first.directory.exists() and second.directory.exists()


def test_cleanup_failure_is_incomplete_and_never_a_fabricated_removal(case, monkeypatch):
    plan = _planned(case)
    original = _snapshot(case.directory)
    actual_rmtree = materialize.shutil.rmtree

    def fail_selected(path, *args, **kwargs):
        if Path(path) == case.directory:
            raise PermissionError("still held by native root owner")
        return actual_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(materialize.shutil, "rmtree", fail_selected)
    result = case.apply(plan)
    assert result["schema"] == RESULT_SCHEMA and result["complete"] is False, result
    assert result["status"] != "applied" and result["removed"] == []
    assert result["errors"]
    assert case.cleanup_calls == [case.directory]
    assert _snapshot(case.directory) == original
    failures = list((case.local_root / "cleanup-failures").glob(f"{case.key}.*.json"))
    assert len(failures) == 1
    assert "still held by native root owner" in json.loads(failures[0].read_text())["error"]


def test_real_operator_open_fd_is_live_even_when_it_belongs_to_this_pid(case):
    with (case.checkout / "native" / "libkernel.so").open("rb") as native:
        process = _process(case, pid=os.getpid())
        (process / "fd" / str(native.fileno())).symlink_to(case.checkout / "native" / "libkernel.so")
        result = _refused(case, case.prepare)
        assert any("PID" in error or "inode" in error for error in result["errors"])


def test_a_descriptor_alias_outside_the_namespace_still_holds_the_original_inode(case):
    alias = case.protected_root / "live-native-alias"
    os.link(case.checkout / "native" / "libkernel.so", alias)
    process = _process(case)
    (process / "fd" / "7").symlink_to(alias)
    _refused(case, case.prepare)


def test_unreadable_process_fd_census_is_not_an_empty_census(case, monkeypatch):
    process = _process(case)
    original = recovery.os.listdir

    def denied(path):
        if Path(path) == process / "fd":
            raise PermissionError("private process fd census denied")
        return original(path)

    monkeypatch.setattr(recovery.os, "listdir", denied)
    result = _refused(case, case.prepare)
    assert any("unreadable" in error or "denied" in error for error in result["errors"])


def test_pid_reuse_during_census_is_unstable_not_proof_of_death(case, monkeypatch):
    process = _process(case)
    original_tree = _snapshot(case.directory)
    actual = recovery._process_stat
    reads = 0

    def replaced(path):
        nonlocal reads
        result = actual(path)
        if path == process / "stat":
            reads += 1
            if reads == 1:
                (process / "stat").write_text("101 (reused) S " + "0 " * 18 + "901 0")
        return result

    monkeypatch.setattr(recovery, "_process_stat", replaced)
    result = case.prepare()
    assert reads >= 2 and result["complete"] is False and result["removed"] == [], result
    assert case.cleanup_calls == [] and _snapshot(case.directory) == original_tree


@pytest.mark.parametrize("metadata", ["request", "terminal", "gate"])
def test_small_json_authorities_are_bounded_before_consumption(case, metadata):
    path = {"request": case.request, "terminal": case.terminal_path, "gate": case.gate}[metadata]
    value = json.loads(path.read_text())
    value["oversized_metadata"] = "x" * (recovery.MAX_JSON_BYTES + 1)
    _json(path, value)
    if metadata == "request":
        path.chmod(0o444)
    result = _refused(case, case.prepare)
    assert any("bound" in error or "bytes" in error or "large" in error for error in result["errors"])


@pytest.mark.parametrize("limit", ["MAX_MEMBERS", "MAX_MANIFEST_BYTES"])
def test_full_tree_manifest_and_member_count_are_bounded(case, monkeypatch, limit):
    monkeypatch.setattr(recovery, limit, 1)
    _refused(case, case.prepare)


def test_archive_extended_metadata_is_bounded_not_an_unlimited_tarfile_read(case):
    member = tarfile.TarInfo(case.directory.name + "/" + "x" * (recovery.MAX_JSON_BYTES + 1024))
    _append_member(case, member)
    _refused(case, case.prepare)


def test_special_original_member_is_retained_instead_of_silently_omitted(case):
    os.mkfifo(case.checkout / "native-owner-fifo")
    _refused(case, case.prepare)


def test_foreign_gate_file_uid_and_writable_generated_root_refuse(case, monkeypatch):
    monkeypatch.setattr(recovery, "_gate_uid", lambda: os.getuid() + 1)
    _refused(case, case.prepare)
    monkeypatch.setattr(recovery, "_gate_uid", lambda: os.getuid())
    case.directory.chmod(0o777)
    _refused(case, case.prepare)


def test_hashing_dirty_native_bytes_is_streamed_and_no_archive_is_extracted(case, monkeypatch):
    native = case.checkout / "native" / "libkernel.so"
    with native.open("ab") as output:
        output.write(b"large native payload\0" * 131072)
    case.bank()
    actual_read = Path.read_bytes

    def no_unbounded_payload_read(path):
        if path == native or path == case.archive:
            raise AssertionError("full native/archive payload must use streaming hashing")
        return actual_read(path)

    def no_extract(*args, **kwargs):
        raise AssertionError("archive proof may not extract arbitrary tar paths")

    monkeypatch.setattr(Path, "read_bytes", no_unbounded_payload_read)
    monkeypatch.setattr(tarfile.TarFile, "extract", no_extract)
    monkeypatch.setattr(tarfile.TarFile, "extractall", no_extract)
    plan = _planned(case)
    result = case.apply(plan)
    assert result["complete"] is True and result["removed"] == [str(case.directory)], result


@pytest.mark.parametrize("truncated", [False, True])
def test_real_zstd_bank_is_verified_to_completion_not_just_tar_end(case, truncated):
    zstd = shutil.which("zstd")
    if zstd is None:
        pytest.skip("real .tar.zst qualification requires the documented zstd binary")
    archive = case.archive.with_suffix(".tar.zst")
    subprocess.run([zstd, "-q", "-f", "-o", str(archive), str(case.archive)], check=True)
    if truncated:
        archive.chmod(0o644)
        with archive.open("r+b") as output:
            output.truncate(max(0, archive.stat().st_size - 5))
    archive.chmod(0o444)
    case.entry["archive_path"] = str(archive)
    case.entry["archive_sha256"] = _digest(archive)
    if truncated:
        _refused(case, case.prepare)
    else:
        plan = _planned(case)
        result = case.apply(plan)
        assert result["complete"] is True and result["removed"] == [str(case.directory)], result


def test_apply_holds_every_selected_transition_lock_through_the_cleanup_owner(make_case, monkeypatch):
    first, second = make_case(), make_case()
    plan = first.prepare(entries=[first.entry, second.entry])
    assert plan["complete"] is True, plan
    active: set[str] = set()
    actual_lock = first.queue.queue._transition_locked
    actual_cleanup = materialize._cleanup_execution_checkout

    @contextmanager
    def tracked_lock(key, **kwargs):
        with actual_lock(key, **kwargs) as acquired:
            if acquired:
                active.add(key)
            try:
                yield acquired
            finally:
                active.discard(key)

    def locked_cleanup(base, temporary, item):
        assert active == {first.key, second.key}
        return actual_cleanup(base, temporary, item)

    monkeypatch.setattr(first.queue.queue, "_transition_locked", tracked_lock)
    monkeypatch.setattr(materialize, "_cleanup_execution_checkout", locked_cleanup)
    result = first.apply(plan)
    assert result["complete"] is True, result
    assert result["removed"] == [str(first.directory), str(second.directory)]
    assert active == set()


def test_batch_helper_failure_reports_actual_partial_progress_and_keeps_the_hold(make_case, monkeypatch):
    first, second = make_case(), make_case()
    plan = first.prepare(entries=[first.entry, second.entry])
    assert plan["complete"] is True, plan
    second_original = _snapshot(second.directory)
    gate_original = _snapshot(first.gate)
    actual = materialize.shutil.rmtree

    def fail_second(path, *args, **kwargs):
        if Path(path) == second.directory:
            raise PermissionError("second checkout still owned")
        return actual(path, *args, **kwargs)

    monkeypatch.setattr(materialize.shutil, "rmtree", fail_second)
    result = first.apply(plan)
    assert result["complete"] is False and result["status"] == "incomplete", result
    assert result["removed"] == [str(first.directory)]
    assert first.cleanup_calls == [first.directory, second.directory]
    assert not first.directory.exists() and _snapshot(second.directory) == second_original
    assert _snapshot(first.gate) == gate_original


@pytest.fixture
def cli(case, monkeypatch):
    path = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "pbrecover_checkout.py"
    spec = importlib.util.spec_from_file_location("pb1465_private_recovery_cli", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    actual_prepare = recovery.prepare_checkout_recovery
    actual_apply = recovery.apply_checkout_recovery

    def private_prepare(queue, entries, **options):
        return actual_prepare(queue, entries, **{**case.options(), **options})

    def private_apply(queue, plan, **options):
        return actual_apply(queue, plan, **{**case.options(), **options})

    monkeypatch.setattr(recovery, "prepare_checkout_recovery", private_prepare)
    monkeypatch.setattr(recovery, "apply_checkout_recovery", private_apply)
    return module


def _cli(cli, case, monkeypatch, capsys, *flags):
    monkeypatch.setattr(sys, "argv", ["pbrecover_checkout.py", "--queue-root", str(case.queue.root),
        "--bank-root", str(case.bank_root), "--maintenance-owner", OWNER, *flags])
    try:
        status = cli.main()
    except SystemExit as exc:
        status = exc.code
    report = json.loads(capsys.readouterr().out)
    assert report["schema"] == RESULT_SCHEMA
    return status, report


def test_source_cli_emits_complete_plan_json_sha_then_requires_explicit_apply(cli, case, monkeypatch, capsys):
    candidates = case.workspace / "candidates.json"
    _json(candidates, [case.entry])
    before = case.preserved()
    status, report = _cli(cli, case, monkeypatch, capsys, "--candidates", str(candidates))
    assert status == 0 and report["complete"] is True and report["status"] == "planned", report
    assert report["plan_sha256"] == pb.canonical_sha256(report["plan"])
    assert case.cleanup_calls == [] and case.preserved() == before
    plan_path = case.workspace / "rootgo-plan.json"
    _json(plan_path, report["plan"])
    status, applied = _cli(cli, case, monkeypatch, capsys, "--apply", "--plan", str(plan_path),
                           "--plan-sha256", report["plan_sha256"])
    assert status == 0 and applied["complete"] is True and applied["status"] == "applied", applied
    assert applied["removed"] == [str(case.directory)]
    assert case.cleanup_calls == [case.directory]


@pytest.mark.parametrize("flags", [
    (), ("--apply",), ("--apply", "--plan", "plan.json"),
    ("--apply", "--plan-sha256", "0" * 64), ("--plan", "plan.json"),
    ("--candidates", "candidates.json", "--apply"),
    ("--candidates", "candidates.json", "--local-root", "/tmp/unsafe"),
    ("--candidates", "candidates.json", "--proc-root", "/tmp/unsafe"),
    ("--candidates", "candidates.json", "--maintenance-gate", "/tmp/unsafe"),
])
def test_cli_refuses_missing_rootgo_or_operator_namespace_override_as_json(
        cli, case, monkeypatch, capsys, flags):
    before = case.preserved()
    status, report = _cli(cli, case, monkeypatch, capsys, *flags)
    assert status != 0 and report["complete"] is False and report["status"] == "refused", report
    assert report["errors"] and report["removed"] == []
    assert case.cleanup_calls == [] and case.preserved() == before


@pytest.mark.parametrize("fault", ["malformed", "symlink", "oversized", "missing-queue"])
def test_cli_json_is_nofollow_bounded_and_existing_queue_is_not_created(
        cli, case, monkeypatch, capsys, fault):
    candidates = case.workspace / "candidates.json"
    _json(candidates, [case.entry])
    flags = ["--candidates", str(candidates)]
    if fault == "malformed":
        candidates.write_text("{partial candidates")
    elif fault == "symlink":
        original = candidates.with_suffix(".original")
        candidates.rename(original)
        candidates.symlink_to(original)
    elif fault == "oversized":
        monkeypatch.setattr(recovery, "MAX_MANIFEST_BYTES", 64)
    else:
        missing = case.workspace / "queue-must-not-be-created"
        flags += ["--queue-root", str(missing)]
    before = case.preserved()
    status, report = _cli(cli, case, monkeypatch, capsys, *flags)
    assert status != 0 and report["complete"] is False and report["removed"] == [], report
    assert case.cleanup_calls == [] and case.preserved() == before
    if fault == "missing-queue":
        assert not missing.exists()


def test_archive_replacement_during_streaming_verification_is_not_a_stable_bank(case, monkeypatch):
    archive_identity = case.archive.stat()
    actual = recovery._hash_stream
    swapped = False
    original_tree = _snapshot(case.directory)

    def replaced(stream):
        nonlocal swapped
        try:
            info = os.fstat(stream.fileno())
        except (AttributeError, io.UnsupportedOperation):
            info = None
        result = actual(stream)
        if (not swapped and info is not None and
                (info.st_dev, info.st_ino) == (archive_identity.st_dev, archive_identity.st_ino)):
            swapped = True
            previous = case.archive.with_suffix(".held-original.tar")
            case.archive.rename(previous)
            shutil.copyfile(previous, case.archive)
            case.archive.chmod(0o444)
        return result

    monkeypatch.setattr(recovery, "_hash_stream", replaced)
    result = case.prepare()
    assert swapped and result["complete"] is False and result["removed"] == [], result
    assert case.cleanup_calls == [] and _snapshot(case.directory) == original_tree
    assert _digest(case.archive) == case.entry["archive_sha256"]


def test_ending_changed_after_archive_proof_is_re_read_before_the_plan_is_authoritative(case, monkeypatch):
    actual = recovery._archive
    before_tree, before_cas, before_bank = (
        _snapshot(case.directory), _snapshot(case.cas.root), _snapshot(case.bank_root))
    changed = False

    def next_generation(*args, **kwargs):
        nonlocal changed
        result = actual(*args, **kwargs)
        if not changed:
            changed = True
            _terminal(case, lambda value: value.update(published_unix=value["published_unix"] + 1))
        return result

    monkeypatch.setattr(recovery, "_archive", next_generation)
    result = case.prepare()
    assert changed and result["complete"] is False and result["removed"] == [], result
    assert case.cleanup_calls == []
    assert (_snapshot(case.directory), _snapshot(case.cas.root), _snapshot(case.bank_root)) == (
        before_tree, before_cas, before_bank)


def test_gate_epoch_changing_during_apply_census_keeps_every_original(case, monkeypatch):
    plan = _planned(case)
    original_tree = _snapshot(case.directory)
    actual = recovery._census
    changed = False

    def reheld(*args, **kwargs):
        nonlocal changed
        result = actual(*args, **kwargs)
        if not changed:
            changed = True
            value = json.loads(case.gate.read_text())
            value["changed_unix"] += 1
            _json(case.gate, value)
        return result

    monkeypatch.setattr(recovery, "_census", reheld)
    result = case.apply(plan)
    assert changed and result["complete"] is False and result["removed"] == [], result
    assert case.cleanup_calls == [] and _snapshot(case.directory) == original_tree
    assert json.loads(case.gate.read_text())["draining"] is True


def test_busy_real_transition_lock_refuses_without_waiting_or_cleanup(case):
    plan = _planned(case)
    source_root = Path(__file__).resolve().parents[1] / "src"
    holder = subprocess.Popen([
        sys.executable, "-c",
        "import sys; from prismabuild.pool import PoolQueue; "
        "q=PoolQueue(sys.argv[1]); "
        "hold=q._transition_locked(sys.argv[2]); hold.__enter__(); "
        "print('held', flush=True); sys.stdin.readline(); hold.__exit__(None,None,None)",
        str(case.queue.root), case.key,
    ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
       text=True, env={**os.environ, "PYTHONPATH": str(source_root)})
    try:
        assert holder.stdout.readline().strip() == "held"
        result = _refused(case, lambda: case.apply(plan))
        assert any("lock" in error and "busy" in error for error in result["errors"])
    finally:
        try:
            holder.communicate(input="release\n", timeout=10)
        except subprocess.TimeoutExpired:
            holder.kill()
            holder.communicate()


@pytest.mark.parametrize("fault", ["selected-root", "same-device-native-bank", "escaped-space",
                                  "hidepid", "unreadable", "malformed", "empty"])
def test_real_kernel_mount_census_cannot_hide_external_banks_or_processes(case, monkeypatch, fault):
    path = Path("/proc") / str(os.getpid()) / "mountinfo"
    actual_reader = pb._read_regular_file_nofollow
    selected = case.directory
    if fault == "same-device-native-bank":
        selected = case.checkout / "native"
    elif fault == "escaped-space":
        selected = case.checkout / "external native bank"
    device = case.directory.stat().st_dev
    encoded = str(selected).replace(" ", r"\040")
    payload = (f"36 25 {os.major(device)}:{os.minor(device)} / {encoded} rw "
               "- ext4 fixture rw\n").encode()
    if fault == "hidepid":
        payload = b"36 25 0:1 / /proc rw - proc proc rw,hidepid=2\n"
    elif fault == "malformed":
        payload = b"incomplete mount identity\n"
    elif fault == "empty":
        payload = b""

    def mount_census(candidate, **options):
        if candidate == path:
            if fault == "unreadable":
                raise PermissionError("real mount census denied")
            return payload
        return actual_reader(candidate, **options)

    monkeypatch.setattr(pb, "_read_regular_file_nofollow", mount_census)
    _refused(case, case.prepare)


@pytest.mark.parametrize("authority", ["queue", "gate", "proc", "cas"])
def test_recovery_authority_itself_must_never_be_inside_a_selected_deletion_root(case, authority):
    nested = case.directory / f"protected-{authority}-authority"
    options = case.options()
    queue = case.queue
    if authority == "queue":
        shutil.copytree(case.queue.root, nested, symlinks=True)
        queue = pool.PoolQueue(nested)
    elif authority == "gate":
        shutil.copyfile(case.gate, nested)
        nested.chmod(0o644)
        options["maintenance_gate"] = nested
    elif authority == "proc":
        nested.mkdir()
        options["proc_root"] = nested
    else:
        shutil.copytree(case.cas.root, nested, symlinks=True)
        _terminal(case, lambda value: value.update(cas_root=str(nested)))
    # The archive remains a complete copy even of these added dirty bytes;
    # refusal must come from bounded deletion ownership, not an omitted bank.
    case.bank()
    _refused(case, lambda: recovery.prepare_checkout_recovery(queue, [case.entry], **options))





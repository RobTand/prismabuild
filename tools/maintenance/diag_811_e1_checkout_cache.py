#!/usr/bin/python3
"""E1: paired before/after experiment for a verified checkout object cache.

Standalone experimental harness for issue #811.  It imports the *sealed runtime
generation's* ``prismabuild.materialize`` / ``prismabuild.core`` primitives and
changes no production file.  Both arms run in this one process: no queue, no
broker scope, no tier write.

- Arm A is today's path: ``materialize._execution_checkout`` on the retained
  ``a3`` sealed snapshot (bundle ``97bb30f2…``), with every git command timed.
- Arm B is the proposed cache path: the bundle's pack section is extracted
  verbatim and indexed with ``git index-pack -o``; once per process the entry
  pack is bound to the verified CAS bundle (sha256 of the bundle's pack
  section) and the index is proven with ``git index-pack --verify``; each rep
  copies the entry into a fresh private repo (reflink when supported, byte copy
  otherwise), hashes the copies against the verified digests, recreates the
  recorded refs and FETCH_HEAD with the same git commands, and checks out.
- Negative controls prove the tampered-pack and tampered-index rejections
  before any timing is eligible.
- A dedicated parity phase materializes both arms side by side and compares the
  worktree bytes and the repository state.

Everything is written under this action's own temporary root and removed at the
end; the only reads outside it are the CAS request and bundle.  Admission and
measurement isolation come from the sealed PB action this runs under; this
report supplies raw paired timings, phase costs, parity and controls, and makes
no speedup claim by itself.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import resource
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import cast

FICLONE = 0x40049409
WORK_PREFIX = "e1-811."
REPORT_BEGIN = "E1-REPORT-BEGIN"
REPORT_END = "E1-REPORT-END"


class E1Error(RuntimeError):
    """The harness cannot produce trustworthy data."""


def log(message: str) -> None:
    print(f"[e1 {time.strftime('%H:%M:%S')}] {message}", flush=True)


def sha256_stream(path: Path, *, offset: int = 0, length: int | None = None) -> str:
    digest = hashlib.sha256()
    remaining = length
    with open(path, "rb") as handle:
        if offset:
            handle.seek(offset)
        while True:
            want = 1 << 20 if remaining is None else min(1 << 20, remaining)
            if want <= 0:
                break
            chunk = handle.read(want)
            if not chunk:
                break
            digest.update(chunk)
            if remaining is not None:
                remaining -= len(chunk)
    return digest.hexdigest()


def file_bytes(path: Path) -> int:
    return os.stat(path).st_size


def rusage_snapshot() -> dict[str, float]:
    """CPU is split: this Python process vs the Git children it reaped."""

    own = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return {
        "self_user_seconds": own.ru_utime,
        "self_system_seconds": own.ru_stime,
        "children_user_seconds": children.ru_utime,
        "children_system_seconds": children.ru_stime,
        "max_rss_watermark_bytes": own.ru_maxrss * 1024,
    }


def rusage_delta(before: dict[str, float]) -> dict[str, float]:
    """Arm deltas; max RSS is reported as an absolute watermark, not a delta."""

    after = rusage_snapshot()
    return {
        "self_user_seconds": round(float(after["self_user_seconds"])
                                   - float(before["self_user_seconds"]), 4),
        "self_system_seconds": round(float(after["self_system_seconds"])
                                     - float(before["self_system_seconds"]), 4),
        "children_user_seconds": round(float(after["children_user_seconds"])
                                       - float(before["children_user_seconds"]), 4),
        "children_system_seconds": round(float(after["children_system_seconds"])
                                         - float(before["children_system_seconds"]), 4),
        "max_rss_watermark_bytes": after["max_rss_watermark_bytes"],
    }


def statvfs(path: Path) -> dict[str, object]:
    value = os.statvfs(path)
    return {"path": str(path), "device": os.stat(path).st_dev,
            "bsize": value.f_bsize, "free_bytes": value.f_bavail * value.f_frsize,
            "total_bytes": value.f_blocks * value.f_frsize}


class Phase:
    """Monotonic per-rep phase stopwatch."""

    def __init__(self) -> None:
        self._at = time.monotonic()
        self.seconds: dict[str, float] = {}

    def done(self, name: str) -> None:
        now = time.monotonic()
        self.seconds[name] = round(now - self._at, 4)
        self._at = now


class Git:
    """Every git call goes through the production subprocess wrapper."""

    def __init__(self, materialize) -> None:
        self._materialize = materialize
        self._original = materialize._run_materializer_git
        # The refusal the poisoned-pack/index controls expect is the
        # materializer's own exception, never core's (it does not export one).
        self.error = materialize.MaterializationError
        self.spans: list[dict[str, object]] = []
        self.arm = "setup"

    def run(self, argv, *, where: str, environment=None) -> str:
        started = time.monotonic()
        try:
            return self._original(argv, where=where, environment=environment)
        finally:
            self.spans.append({"arm": self.arm, "where": where,
                               "seconds": round(time.monotonic() - started, 4)})

    def install(self) -> None:
        harness = self

        def wrapper(argv, *, where, environment=None):
            return harness.run(argv, where=where, environment=environment)

        self._materialize._run_materializer_git = wrapper

    def restore(self) -> None:
        self._materialize._run_materializer_git = self._original


def load_generation():
    root = os.environ.get("PRISMABUILD_READER_HELPER_ROOT")
    if not root:
        raise E1Error("PRISMABUILD_READER_HELPER_ROOT is unset; run under an "
                      "admitted PB action")
    root = Path(root).resolve()
    source = root / "src"
    if not (source / "prismabuild" / "materialize.py").is_file():
        raise E1Error(f"no sealed prismabuild source under {root}")
    sys.path.insert(0, str(source))
    import prismabuild.core as core
    import prismabuild.materialize as materialize
    import prismabuild.reader_lease as reader_lease

    for module in (core, materialize, reader_lease):
        if not Path(cast(str, module.__file__)).resolve().is_relative_to(root):
            raise E1Error(f"imported {module.__name__} from outside the generation")
    return root, core, materialize, reader_lease


def entry_paths(entry: Path, framing: dict) -> tuple[Path, Path]:
    """Conventional ``pack-<trailer>.pack`` / ``.idx`` names Git itself uses."""

    stem = f"pack-{framing['pack_trailer_hex']}"
    return entry / f"{stem}.pack", entry / f"{stem}.idx"


def parse_bundle(bundle: Path) -> dict[str, object]:
    """The bundle's own framing: header lines, blank line, then the pack."""

    raw = bundle.read_bytes()
    if not raw.startswith(b"# v2 git bundle\n") and not raw.startswith(b"# v3 git bundle\n"):
        raise E1Error("bundle does not start with a Git bundle header")
    terminator = raw.find(b"\n\n", 1)
    if terminator < 0:
        raise E1Error("bundle has no header terminator")
    lines = raw[: terminator + 1].decode("utf-8").splitlines()
    object_format = "sha1"
    advertised: dict[str, str] = {}
    for line in lines:
        if line.startswith("#"):
            continue
        if line.startswith("@object-format="):
            object_format = line.split("=", 1)[1].strip()
            continue
        fields = line.split(maxsplit=1)
        if len(fields) != 2:
            raise E1Error(f"unparsable bundle header line: {line!r}")
        advertised[fields[1]] = fields[0]
    if not advertised:
        raise E1Error("bundle advertises no refs")
    trailer_len = 32 if object_format == "sha256" else 20
    pack_offset = terminator + 2
    pack_len = len(raw) - pack_offset
    if pack_len <= trailer_len:
        raise E1Error("bundle carries no pack")
    return {
        "header_lines": lines,
        "advertised": advertised,
        "object_format": object_format,
        "trailer_len": trailer_len,
        "pack_offset": pack_offset,
        "pack_len": pack_len,
        "pack_sha256": sha256_stream(bundle, offset=pack_offset, length=pack_len),
        "pack_trailer_hex": raw[-trailer_len:].hex(),
        "bytes": len(raw),
        "sha256": sha256_stream(bundle),
    }


def entry_identity(entry: Path, framing: dict, reader_lease) -> dict[str, object]:
    identity: dict[str, object] = {}
    pack, idx = entry_paths(entry, framing)
    for label, path in (("pack", pack), ("idx", idx),
                        ("manifest", entry / "manifest.json")):
        info = os.lstat(path)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise E1Error(f"cache {label} is not a regular file: {path}")
        identity[label] = {**reader_lease.portable_identity(info),
                           "device": info.st_dev}
    return identity


def copy_or_reflink(source: Path, destination: Path) -> str:
    destination.unlink(missing_ok=True)
    try:
        with open(source, "rb") as src, open(destination, "wb") as dst:
            fcntl.ioctl(dst.fileno(), FICLONE, src.fileno())
        return "reflink"
    except OSError:
        destination.unlink(missing_ok=True)
        shutil.copyfile(source, destination)
        return "copy"


def entry_manifest_binding(*, framing: dict, generation: str,
                           pack: Path, idx: Path, pack_sha: str,
                           idx_sha: str) -> dict:
    """The publication binding shared by the experimental writer and reader."""

    return {
        "schema": "prismabuild.diag811.e1_entry.v1",
        "generation": generation,
        "bundle": {key: framing[key] for key in
                   ("sha256", "bytes", "pack_offset", "pack_len")},
        "pack": {"name": pack.name, "sha256": pack_sha,
                 "bytes": file_bytes(pack)},
        "idx": {"name": idx.name, "sha256": idx_sha,
                "bytes": file_bytes(idx)},
        "advertised": framing["advertised"],
        "object_format": framing["object_format"],
    }


def same_manifest_binding(actual, expected) -> bool:
    """Compare typed records; bools and floats cannot stand in for integers."""

    if type(actual) is not type(expected):
        return False
    if isinstance(expected, dict):
        return (actual.keys() == expected.keys()
                and all(same_manifest_binding(actual[key], value)
                        for key, value in expected.items()))
    if isinstance(expected, list):
        return (len(actual) == len(expected)
                and all(same_manifest_binding(a, b)
                        for a, b in zip(actual, expected)))
    return actual == expected


def build_entry(
    *, bundle_path: Path, framing: dict, entry: Path, staging_root: Path,
    lock_root: Path, git: Git, core, digest: str, generation: str,
) -> dict[str, object]:
    """Extract the pack verbatim and build its index; publish under flock."""

    lock_root.mkdir(parents=True, exist_ok=True)
    pack_path, idx_path = entry_paths(entry, framing)
    with open(lock_root / f"{digest}.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if pack_path.is_file() and idx_path.is_file():
            return {"published": False, "reason": "entry already present",
                    "verbatim_index": True, "pack_unchanged_by_index_pack": True,
                    "seconds": 0.0}
        staging_root.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=f"{digest[:12]}.", dir=str(staging_root)))
        started = time.monotonic()
        try:
            pack = entry_paths(staging, framing)[0]
            with open(bundle_path, "rb") as src, open(pack, "wb") as dst:
                src.seek(int(framing["pack_offset"]))
                remaining = int(framing["pack_len"])
                while remaining:
                    chunk = src.read(min(1 << 20, remaining))
                    if not chunk:
                        raise E1Error("bundle pack section ended early")
                    dst.write(chunk)
                    remaining -= len(chunk)
                dst.flush()
                os.fsync(dst.fileno())
            pack_sha = sha256_stream(pack)
            if pack_sha != framing["pack_sha256"]:
                raise E1Error("extracted pack differs from the bundle pack section")
            idx = entry_paths(staging, framing)[1]
            try:
                git.run(["git", "index-pack", "-o", str(idx), str(pack)],
                        where="index cached pack verbatim")
                verbatim, error = True, None
            except git.error as exc:
                verbatim, error = False, str(exc)
            result = {
                "published": False,
                "verbatim_index": verbatim,
                "verbatim_error": error,
                "pack_unchanged_by_index_pack": sha256_stream(pack) == pack_sha,
                "seconds": round(time.monotonic() - started, 4),
                "pack_sha256": pack_sha,
                "pack_bytes": file_bytes(pack),
            }
            if not verbatim or not result["pack_unchanged_by_index_pack"]:
                return result
            git.run(["git", "index-pack", "--verify", str(pack)],
                    where="verify cached pack and index")
            manifest = entry_manifest_binding(
                framing=framing, generation=generation, pack=pack, idx=idx,
                pack_sha=pack_sha, idx_sha=sha256_stream(idx))
            manifest["created_unix"] = time.time()
            (staging / "manifest.json").write_text(
                json.dumps(manifest, indent=1, sort_keys=True) + "\n")
            os.chmod(pack, 0o444)
            os.chmod(idx, 0o444)
            entry.parent.mkdir(parents=True, exist_ok=True)
            if entry.exists():
                shutil.rmtree(entry)
            os.replace(staging, entry)
            result.update({"published": True, "manifest": manifest})
            return result
        finally:
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)


def verify_entry(
    *, entry: Path, bundle_path: Path, framing: dict, git: Git, core,
    reader_lease, memo: dict,
) -> dict[str, object]:
    """Verify publication, generation and bytes; memoize their stable identity."""

    started = time.monotonic()
    generation = framing.get("runtime_generation")
    try:
        if not isinstance(generation, str) or not generation:
            raise E1Error("entry manifest expected runtime generation is missing")
        identity = entry_identity(entry, framing, reader_lease)
        cached = memo.get(str(entry))
        if (cached is not None and cached["identity"] == identity
                and cached["bundle_sha256"] == framing["sha256"]
                and cached.get("runtime_generation") == generation):
            return {"ok": True, "cached": True, "seconds": 0.0,
                    "manifest_verified": True,
                    "pack_sha256": cached["pack_sha256"],
                    "idx_sha256": cached["idx_sha256"]}
        memo.pop(str(entry), None)
        manifest = core._decode_strict_json(
            core._read_regular_file_nofollow(
                entry / "manifest.json", where="entry publication manifest"),
            where="entry publication manifest")
        if not isinstance(manifest, dict):
            raise E1Error("entry publication manifest is not an object")
        created = manifest.get("created_unix")
        try:
            created_is_finite = (
                not isinstance(created, bool)
                and isinstance(created, (int, float))
                and math.isfinite(created))
        except OverflowError:
            created_is_finite = False
        if not created_is_finite:
            raise E1Error("entry publication manifest created_unix is not finite")
        pack, idx = entry_paths(entry, framing)
        idx_sha = sha256_stream(idx)
        expected = entry_manifest_binding(
            framing=framing, generation=generation, pack=pack, idx=idx,
            pack_sha=framing["pack_sha256"], idx_sha=idx_sha)
        binding = {key: value for key, value in manifest.items()
                   if key != "created_unix"}
        if not same_manifest_binding(binding, expected):
            raise E1Error("entry publication manifest binding differs from the "
                          "verified bundle, runtime generation or pack/index")
    except (OSError, E1Error, core.ActionContractError) as exc:
        memo.pop(str(entry), None)
        return {"ok": False, "cached": False,
                "reason": f"entry publication manifest refused: {exc}",
                "seconds": round(time.monotonic() - started, 4)}
    pack_sha = sha256_stream(pack)
    bundle_pack_sha = sha256_stream(bundle_path, offset=int(framing["pack_offset"]),
                                    length=int(framing["pack_len"]))
    if pack_sha != bundle_pack_sha:
        return {"ok": False, "cached": False,
                "reason": "entry pack digest differs from the bundle pack section",
                "entry_pack_sha256": pack_sha,
                "bundle_pack_sha256": bundle_pack_sha,
                "seconds": round(time.monotonic() - started, 4)}
    try:
        git.run(["git", "index-pack", "--verify", str(pack)],
                where="verify cached pack and index")
    except git.error as exc:
        return {"ok": False, "cached": False,
                "reason": f"index verification refused: {exc}",
                "seconds": round(time.monotonic() - started, 4)}
    if entry_identity(entry, framing, reader_lease) != identity:
        return {"ok": False, "cached": False,
                "reason": "entry publication manifest or pack/index changed during verification",
                "seconds": round(time.monotonic() - started, 4)}
    memo[str(entry)] = {"identity": identity, "bundle_sha256": framing["sha256"],
                        "runtime_generation": generation,
                        "pack_sha256": pack_sha, "idx_sha256": idx_sha}
    return {"ok": True, "cached": False, "manifest_verified": True,
            "seconds": round(time.monotonic() - started, 4),
            "pack_sha256": pack_sha, "idx_sha256": idx_sha}


def repo_state(root: Path, core) -> dict[str, object]:
    """The repository facts the existing preflight reads, for parity."""

    def git_output(*args: str) -> str:
        completed = subprocess.run(["git", "-C", str(root), *args],
                                   capture_output=True, text=True, timeout=60)
        if completed.returncode != 0:
            raise E1Error(f"git {' '.join(args)} failed: {completed.stderr.strip()}")
        return completed.stdout

    git_dir = Path(git_output("rev-parse", "--absolute-git-dir").strip())
    fetch_head = git_dir / "FETCH_HEAD"
    return {
        "head": git_output("rev-parse", "HEAD").strip(),
        "refs": git_output("for-each-ref", "--format=%(refname) %(objectname)"),
        "index": hashlib.sha256(git_output("ls-files", "-s").encode()).hexdigest(),
        "status": git_output("status", "--porcelain"),
        "fetch_head": fetch_head.read_bytes().hex() if fetch_head.exists() else None,
        "identity": core.git_checkout_identity(root),
    }


def preflight(request: dict, tree: Path, core) -> dict[str, object]:
    """The worker's own checkout proof, recorded rather than fatal."""

    try:
        core._verify_pbrun_checkout_identity(request, tree)
        return {"ok": True}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def tree_manifest(root: Path) -> dict[str, object]:
    """Every path below the tree with its mode and content digest."""

    manifest: dict[str, object] = {}
    pending = [root]
    while pending:
        directory = pending.pop()
        for entry in os.scandir(directory):
            path = Path(entry.path)
            relative = path.relative_to(root).as_posix()
            if relative == ".git":
                continue
            info = entry.stat(follow_symlinks=False)
            if entry.is_symlink():
                manifest[relative] = {"kind": "symlink",
                                      "target": os.readlink(path)}
            elif entry.is_dir(follow_symlinks=False):
                manifest[relative] = {"kind": "dir"}
                pending.append(path)
            elif stat.S_ISREG(info.st_mode):
                manifest[relative] = {"kind": "file",
                                      "mode": stat.S_IMODE(info.st_mode),
                                      "sha256": sha256_stream(path),
                                      "bytes": info.st_size}
            else:
                manifest[relative] = {"kind": "special",
                                      "mode": stat.S_IMODE(info.st_mode)}
    return manifest


def negative_controls(
    *, entry: Path, framing: dict, git: Git, core, scratch: Path,
) -> dict[str, object]:
    """Prove the pack and index checks reject tampering before any timing."""

    scratch.mkdir(parents=True, exist_ok=True)
    pack, idx = entry_paths(entry, framing)
    stem = pack.name[: -len(".pack")]
    controls: dict[str, object] = {}

    tampered_pack = scratch / pack.name
    shutil.copyfile(pack, tampered_pack)
    shutil.copyfile(idx, scratch / f"{stem}.idx")
    with open(tampered_pack, "r+b") as handle:
        middle = max(1, file_bytes(tampered_pack) // 2)
        handle.seek(middle)
        original = handle.read(1)
        handle.seek(middle)
        handle.write(bytes([original[0] ^ 0xFF]))
    controls["tampered_pack_hash_rejected"] = (
        sha256_stream(tampered_pack) != framing["pack_sha256"])
    try:
        git.run(["git", "index-pack", "--verify", str(tampered_pack)],
                where="control: verify tampered pack")
        controls["tampered_pack_verify_rejected"] = False
    except git.error:
        controls["tampered_pack_verify_rejected"] = True

    verify_dir = scratch / "verify"
    verify_dir.mkdir()
    shutil.copyfile(pack, verify_dir / pack.name)
    shutil.copyfile(idx, verify_dir / f"{stem}.idx")
    with open(verify_dir / f"{stem}.idx", "r+b") as handle:
        middle = max(1, file_bytes(verify_dir / f"{stem}.idx") // 2)
        handle.seek(middle)
        original = handle.read(1)
        handle.seek(middle)
        handle.write(bytes([original[0] ^ 0xFF]))
    try:
        git.run(["git", "index-pack", "--verify", str(verify_dir / pack.name)],
                where="control: verify tampered index")
        controls["tampered_index_verify_rejected"] = False
    except git.error:
        controls["tampered_index_verify_rejected"] = True

    controls["ok"] = bool(controls["tampered_pack_hash_rejected"]
                          and controls["tampered_pack_verify_rejected"]
                          and controls["tampered_index_verify_rejected"])
    return controls


def timed_materialization_seconds(phases: dict[str, float]) -> float:
    """Count materialization and verification identically in both arms.

    Cleanup is reported separately. Population, cold verification, negative
    controls and parity are outside each repetition, not hidden warm costs.
    """
    return round(sum(value for name, value in phases.items()
                     if name != "cleanup"), 4)


def arm_a_rep(*, item, checkout_root: Path, materialize, git: Git, core,
              pair: int, order: str) -> dict:
    """Materialize-and-verify time; cleanup is reported separately."""

    phase = Phase()
    before = len(git.spans)
    before_usage = rusage_snapshot()
    started_unix = time.time()
    git.arm = "A"
    with materialize._execution_checkout(
        item, local_checkout_root=checkout_root
    ) as tree:
        phase.done("materialize")
        state = repo_state(tree, core)
        proof = preflight(item["request"], tree, core)
        phase.done("verify")
    phase.done("cleanup")
    materialize_seconds = timed_materialization_seconds(phase.seconds)
    return {
        "arm": "A",
        "pair": pair,
        "order": order,
        "started_unix": started_unix,
        "finished_unix": time.time(),
        "seconds": materialize_seconds,
        "cleanup_seconds": phase.seconds["cleanup"],
        "total_with_cleanup_seconds": round(
            materialize_seconds + phase.seconds["cleanup"], 4),
        "phases": phase.seconds,
        "git_spans": git.spans[before:],
        "repo_state": state,
        "preflight": proof,
        "rusage": rusage_delta(before_usage),
    }


def arm_b_rep(
    *, item, snapshot, framing, bundle_path: Path, entry: Path, checkout_root: Path,
    core, materialize, git: Git, reader_lease, memo: dict,
    pair: int, order: str, keep: bool = False,
) -> dict:
    """Materialize-and-verify time; cleanup is reported separately.

    ``keep`` is for the parity phase only, which needs the tree alive to
    compare against arm A; timed repetitions always clean up here.
    """

    phase = Phase()
    before = len(git.spans)
    before_usage = rusage_snapshot()
    started_unix = time.time()
    git.arm = "B"
    cas = core.PrismaBuildCAS(str(item["cas_root"]))
    cas.input_path(snapshot["input"])
    phase.done("bundle_verify")
    identity = entry_identity(entry, framing, reader_lease)
    cached = memo.get(str(entry))
    if cached is None or cached["identity"] != identity:
        raise E1Error("entry changed since verification")
    phase.done("entry_identity")

    key = str(item["action_key"])
    temporary = Path(tempfile.mkdtemp(prefix=f"{key[:12]}.", dir=str(checkout_root)))
    repository = temporary / "checkout"
    try:
        git.run(["git", "init", "-q", str(repository)],
                where="initialize materialized checkout")
        phase.done("git_init")
        objects = repository / ".git" / "objects" / "pack"
        entry_pack, entry_idx = entry_paths(entry, framing)
        methods = []
        for source in (entry_pack, entry_idx):
            destination = objects / source.name
            methods.append(copy_or_reflink(source, destination))
            os.chmod(destination, 0o444)
        phase.done("copy_objects")
        copy_pack_sha = sha256_stream(objects / entry_pack.name)
        copy_idx_sha = sha256_stream(objects / entry_idx.name)
        if copy_pack_sha != cached["pack_sha256"] or copy_idx_sha != cached["idx_sha256"]:
            raise E1Error("copied objects do not match the verified entry")
        phase.done("copy_verify")

        commit = str(snapshot["commit"])
        heads = git.run(["git", "-C", str(repository), "bundle", "list-heads",
                         str(bundle_path)], where="read checkout snapshot bundle")
        advertised = {
            fields[1]: fields[0]
            for line in heads.splitlines()
            if len(fields := line.split(maxsplit=1)) == 2
        }
        sealed = [name for name, oid in advertised.items() if oid == commit]
        if not sealed:
            raise E1Error("bundle does not advertise the sealed commit")
        refspecs = [sealed[0]]
        for name, sealed_id in sorted(dict(snapshot.get("refs") or {}).items()):
            qualified = f"refs/heads/{name}"
            if advertised.get(qualified) != sealed_id:
                raise E1Error(f"bundle contradicts sealed ref {name!r}")
            refspecs.append(f"{qualified}:{qualified}")
        if len(refspecs) > 1:
            git.run(["git", "-C", str(repository), "symbolic-ref", "HEAD",
                     f"refs/heads/{core.PBRUN_CHECKOUT_SNAPSHOT_REF_NAME}.materializing"],
                    where="detach materialized HEAD from a fetched branch")
        for name, sealed_id in sorted(dict(snapshot.get("refs") or {}).items()):
            git.run(["git", "-C", str(repository), "update-ref",
                     f"refs/heads/{name}", str(sealed_id)],
                    where="recreate sealed branch")
        temporary_ref = f"refs/e1-parity/{uuid.uuid4().hex}"
        git.run(["git", "-C", str(repository), "update-ref", temporary_ref, commit],
                where="seed parity-fetch negotiation")
        phase.done("refs")

        packs_before = sorted(p.name for p in objects.iterdir())
        git.run(["git", "-C", str(repository), "fetch", "-q", "--no-tags",
                 str(bundle_path), *refspecs], where="parity fetch from bundle")
        packs_after = sorted(p.name for p in objects.iterdir())
        object_free = packs_before == packs_after
        git.run(["git", "-C", str(repository), "update-ref", "-d", temporary_ref],
                where="remove parity-fetch negotiation ref")
        phase.done("parity_fetch")

        git.run(["git", "-c", "core.autocrlf=false",
                 "-c", "core.attributesFile=/dev/null",
                 "-C", str(repository), "checkout", "-q", "--detach", commit],
                where="check out sealed commit",
                environment={**os.environ, "GIT_ATTR_NOSYSTEM": "1"})
        phase.done("checkout")
        materialize._require_contained_materialized_links(repository)
        phase.done("symlink_scan")

        proof = preflight(item["request"], repository, core)
        state = repo_state(repository, core)
        phase.done("verify")
        materialize_seconds = timed_materialization_seconds(phase.seconds)
        record = {
            "arm": "B",
            "pair": pair,
            "order": order,
            "started_unix": started_unix,
            "finished_unix": None,
            "seconds": materialize_seconds,
            "cleanup_seconds": None,
            "total_with_cleanup_seconds": None,
            "phases": phase.seconds,
            "git_spans": git.spans[before:],
            "repo_state": state,
            "preflight": proof,
            "rusage": rusage_delta(before_usage),
            "copy_methods": methods,
            "parity_fetch_object_free": object_free,
        }
        if keep:
            record["tree"] = str(repository)
            record["temporary"] = str(temporary)
            record["finished_unix"] = time.time()
            return record
        materialize._cleanup_execution_checkout(checkout_root, temporary, item)
        phase.done("cleanup")
        record["cleanup_seconds"] = phase.seconds["cleanup"]
        record["total_with_cleanup_seconds"] = round(
            materialize_seconds + phase.seconds["cleanup"], 4)
        record["finished_unix"] = time.time()
        return record
    except BaseException:
        materialize._cleanup_execution_checkout(checkout_root, temporary, item)
        raise


def parity(
    *, item, snapshot, framing, bundle_path, entry, checkout_root, core,
    materialize, git, reader_lease, memo,
) -> dict[str, object]:
    """Materialize both arms side by side and compare bytes and Git state."""

    with materialize._execution_checkout(
        item, local_checkout_root=checkout_root
    ) as a_tree:
        b = arm_b_rep(item=item, snapshot=snapshot, framing=framing,
                      bundle_path=bundle_path, entry=entry,
                      checkout_root=checkout_root, core=core,
                      materialize=materialize, git=git, reader_lease=reader_lease,
                      memo=memo, pair=-1, order="parity", keep=True)
        b_tree = Path(b["tree"])
        try:
            a_manifest = tree_manifest(a_tree)
            b_manifest = tree_manifest(b_tree)
            a_state = repo_state(a_tree, core)
            b_state = repo_state(b_tree, core)
            a_proof = preflight(item["request"], a_tree, core)
            b_proof = b["preflight"]
            results = {
                "tree_bytes_equal": a_manifest == b_manifest,
                "tree_paths": {"a": len(a_manifest), "b": len(b_manifest)},
                "head_equal": a_state["head"] == b_state["head"],
                "refs_equal": a_state["refs"] == b_state["refs"],
                "index_equal": a_state["index"] == b_state["index"],
                "status_equal": a_state["status"] == b_state["status"],
                "fetch_head_equal": a_state["fetch_head"] == b_state["fetch_head"],
                "identity_equal": a_state["identity"] == b_state["identity"],
                "preflight_a_ok": a_proof["ok"],
                "preflight_b_ok": b_proof["ok"],
                "a_identity": a_state["identity"],
                "b_identity": b_state["identity"],
                "b_parity_fetch_object_free": b["parity_fetch_object_free"],
                "b_copy_methods": b["copy_methods"],
            }
            results["ok"] = bool(
                results["tree_bytes_equal"] and results["head_equal"]
                and results["refs_equal"] and results["index_equal"]
                and results["status_equal"] and results["fetch_head_equal"]
                and results["identity_equal"] and results["preflight_a_ok"]
                and results["preflight_b_ok"]
                and results["b_parity_fetch_object_free"])
            return results
        finally:
            materialize._cleanup_execution_checkout(
                checkout_root, Path(b["temporary"]), item)


def summarise(reps: list[dict]) -> dict[str, object]:
    def median(values: list[float]) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        middle = len(ordered) // 2
        if len(ordered) % 2:
            return ordered[middle]
        return round((ordered[middle - 1] + ordered[middle]) / 2, 4)

    a = [float(r["seconds"]) for r in reps if r["arm"] == "A"]
    b = [float(r["seconds"]) for r in reps if r["arm"] == "B"]
    pairs = []
    for pair in sorted({int(r["pair"]) for r in reps if int(r["pair"]) >= 0}):
        rows = {r["arm"]: r for r in reps if int(r["pair"]) == pair}
        if "A" in rows and "B" in rows:
            pairs.append({
                "pair": pair,
                "order": rows["A"]["order"],
                "a_seconds": rows["A"]["seconds"],
                "b_seconds": rows["B"]["seconds"],
                "a_started_unix": rows["A"]["started_unix"],
                "b_started_unix": rows["B"]["started_unix"],
                "a_cleanup_seconds": rows["A"]["cleanup_seconds"],
                "b_cleanup_seconds": rows["B"]["cleanup_seconds"],
            })
    phases: dict[str, dict[str, float | None]] = {}
    for arm in ("A", "B"):
        rows = [r for r in reps if r["arm"] == arm]
        names = sorted({name for row in rows for name in row["phases"]})
        phases[arm] = {name: median([float(row["phases"].get(name, 0.0))
                                     for row in rows]) for name in names}
    return {
        "a_median_seconds": median(a),
        "b_median_seconds": median(b),
        "a_median_cleanup_seconds": median(
            [float(r["cleanup_seconds"]) for r in reps
             if r["arm"] == "A" and r["cleanup_seconds"] is not None]),
        "b_median_cleanup_seconds": median(
            [float(r["cleanup_seconds"]) for r in reps
             if r["arm"] == "B" and r["cleanup_seconds"] is not None]),
        "pairs": pairs,
        "phases_median": phases,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--production-request", required=True)
    parser.add_argument("--cas-root", default="/mnt/shared/prismabuild-fleet/cas")
    parser.add_argument("--pairs", type=int, default=4)
    parser.add_argument(
        "--expect-bundle-sha256",
        default="97bb30f295dfe4701e2ed1e64f2ca662fff25bcf322d8b67b43859ada78e983f")
    parser.add_argument("--work-root", default=None)
    parser.add_argument("--report", default=None)
    args = parser.parse_args()

    generation_root, core, materialize, reader_lease = load_generation()
    request = json.loads(Path(args.production_request).read_text())
    snapshot = core.validate_pbrun_checkout_snapshot(
        request["params"]["checkout_snapshot"])
    digest = str(snapshot["input"]["sha256"])
    if digest != args.expect_bundle_sha256:
        raise E1Error(f"snapshot bundle {digest} is not the retained a3 bundle")
    item = {"action_key": uuid.uuid4().hex + uuid.uuid4().hex,
            "cas_root": args.cas_root, "checkout_snapshot": snapshot,
            "request": request}

    if args.pairs < 1:
        raise E1Error("--pairs must be at least 1")
    work_parent = Path(args.work_root or os.environ.get("TMPDIR") or "/tmp")
    work_parent.mkdir(parents=True, exist_ok=True)
    invocation = f"{os.getpid()}.{int(time.time())}.{uuid.uuid4().hex[:8]}"
    work = work_parent / f"{WORK_PREFIX}{invocation}"
    os.mkdir(work, 0o700)  # exclusive: this invocation owns exactly this path
    (work / "OWNED-BY-E1").write_text(
        json.dumps({"invocation": invocation, "action_key": item["action_key"],
                    "created_unix": time.time()}, sort_keys=True) + "\n")
    checkout_root = work / "checkouts"
    checkout_root.mkdir(parents=True, exist_ok=True)
    cache_root = work / "cache"
    cache_root.mkdir(parents=True, exist_ok=True)
    scratch = work / "scratch"
    git = Git(materialize)
    git.install()
    report: dict[str, object] = {
        "schema": "prismabuild.diag811.e1_checkout_cache.v2",
        "timing_boundary": {
            "includes": ["materialization", "verification"],
            "excludes": ["cleanup", "population", "cold_entry_verification",
                         "negative_controls", "parity"],
            "v1_correction": "arm B seconds previously omitted verification",
        },
    }
    reasons: list[str] = []
    try:
        cas = core.PrismaBuildCAS(args.cas_root)
        bundle_path = cas.input_path(snapshot["input"])
        framing = {**parse_bundle(bundle_path),
                   "runtime_generation": generation_root.name}
        if framing["sha256"] != digest:
            raise E1Error("bundle digest does not match the sealed input")
        git_version = git.run(["git", "--version"], where="read git version").strip()
        report.update({
            "generation": generation_root.name,
            "generation_materialize_sha256": sha256_stream(Path(cast(str, materialize.__file__))),
            "generation_core_sha256": sha256_stream(Path(cast(str, core.__file__))),
            "host": os.uname().nodename,
            "platform": f"{os.uname().sysname}-{os.uname().machine}",
            "python": sys.version.split()[0],
            "git_version": git_version,
            "production_request": request.get("action_key"),
            "production_request_path": str(Path(args.production_request).resolve()),
            "production_request_sha256": sha256_stream(Path(args.production_request)),
            "source_harness": str(Path(__file__).resolve()),
            "source_harness_sha256": sha256_stream(Path(__file__)),
            "snapshot": snapshot,
            "bundle_framing": framing,
            "invocation": invocation,
            "tmpdir_env": os.environ.get("TMPDIR"),
            "work_parent": str(work_parent),
            "work_root": str(work),
            "statvfs": {name: statvfs(path) for name, path in
                        (("work", work), ("checkouts", checkout_root),
                         ("cache", cache_root))},
        })
        log(f"generation={generation_root.name} bundle={digest[:12]} "
            f"pack={cast(str, framing['pack_sha256'])[:12]} offset={framing['pack_offset']} "
            f"len={framing['pack_len']}")

        entry = cache_root / "objects" / digest[:2] / digest
        git.arm = "population"
        population = build_entry(
            bundle_path=bundle_path, framing=framing, entry=entry,
            staging_root=cache_root / ".staging", lock_root=cache_root / "locks",
            git=git, core=core, digest=digest, generation=generation_root.name)
        report["population"] = {**population, "entry": str(entry)}
        log(f"population: published={population.get('published')} "
            f"verbatim={population.get('verbatim_index')} "
            f"seconds={population.get('seconds')}")

        memo: dict = {}
        pack_ok = bool(population.get("verbatim_index")
                       and (population.get("published")
                            or population.get("reason") == "entry already present"))
        viable = pack_ok
        if not pack_ok:
            reasons.append("the retained bundle's pack is not directly indexable; "
                           "the cache path is not viable")
        else:
            git.arm = "verify"
            cold = verify_entry(entry=entry, bundle_path=bundle_path, framing=framing,
                                git=git, core=core, reader_lease=reader_lease, memo=memo)
            report["cold_entry_verification"] = cold
            if not cold.get("ok"):
                viable = False
                reasons.append(f"cold entry verification failed: {cold.get('reason')}")
        if viable:
            controls = negative_controls(entry=entry, framing=framing, git=git,
                                         core=core, scratch=scratch)
            report["negative_controls"] = controls
            log(f"controls: ok={controls['ok']}")
            if not controls["ok"]:
                viable = False
                reasons.append("tamper controls did not reject")

        reps: list[dict] = []

        def run_b(pair: int, order: str):
            nonlocal viable
            if not viable:
                return None
            try:
                return arm_b_rep(item=item, snapshot=snapshot, framing=framing,
                                 bundle_path=bundle_path, entry=entry,
                                 checkout_root=checkout_root, core=core,
                                 materialize=materialize, git=git,
                                 reader_lease=reader_lease, memo=memo,
                                 pair=pair, order=order)
            except Exception as exc:  # noqa: BLE001
                reasons.append(f"arm B pair {pair} failed: {type(exc).__name__}: {exc}")
                viable = False
                return None

        for pair in range(args.pairs):
            order = "AB" if pair % 2 == 0 else "BA"
            a_record = None
            b_record = None
            if order == "AB":
                a_record = arm_a_rep(item=item, checkout_root=checkout_root,
                                     materialize=materialize, git=git, core=core,
                                     pair=pair, order=order)
                reps.append(a_record)
                b_record = run_b(pair, order)
            else:
                b_record = run_b(pair, order)
                a_record = arm_a_rep(item=item, checkout_root=checkout_root,
                                     materialize=materialize, git=git, core=core,
                                     pair=pair, order=order)
                reps.append(a_record)
            if b_record is not None:
                reps.append(b_record)
            log(f"pair {pair} ({order}): "
                f"A={None if a_record is None else a_record['seconds']}s "
                f"B={None if b_record is None else b_record['seconds']}s")
        report["reps"] = reps
        report["summary"] = summarise(reps)

        if not all(bool(row.get("preflight", {}).get("ok")) for row in reps):
            reasons.append("a timed arm's preflight failed")
        if viable and len([row for row in reps if row["arm"] == "B"]) != args.pairs:
            reasons.append("a timed arm B repetition is missing")

        if viable:
            parity_result = parity(item=item, snapshot=snapshot, framing=framing,
                                   bundle_path=bundle_path, entry=entry,
                                   checkout_root=checkout_root, core=core,
                                   materialize=materialize, git=git,
                                   reader_lease=reader_lease, memo=memo)
            report["parity"] = parity_result
            log(f"parity: ok={parity_result['ok']}")
            if not parity_result["ok"]:
                reasons.append("parity mismatch")
        if any(not row.get("parity_fetch_object_free", True) for row in reps
               if row["arm"] == "B"):
            reasons.append("parity fetch moved objects")
        report["cache_viable"] = viable
        report["timing_claim_eligible"] = viable and not reasons
        report["eligibility_reasons"] = reasons
        report["isolation_note"] = (
            "Admission and measurement isolation are established by the sealed "
            "PB action this ran under (task_class=measurement, pinned host, "
            "resource_profile); this harness reports raw paired timings, phase "
            "costs, controls and parity, and claims no speedup by itself.")
    finally:
        git.restore()
        # Ownership is the exclusive mkdir plus the marker this invocation
        # wrote, never the name prefix alone.
        marker = work / "OWNED-BY-E1"
        if marker.is_file():
            shutil.rmtree(work, ignore_errors=True)

    text = json.dumps(report, indent=1, sort_keys=True)
    if args.report:
        Path(args.report).write_text(text + "\n")
    print(REPORT_BEGIN)
    print(text)
    print(REPORT_END)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

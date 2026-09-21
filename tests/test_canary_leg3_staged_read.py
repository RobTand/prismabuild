"""Canary leg 3 must read its staged copies through the strict reader (PB #784).

Leg 3 reported staged-read verification while hashing its original input
paths with an ordinary ``open`` and re-reading them for the combined digest.
``--residency stage`` only plans and stages bytes; it never redirects
Python's opens.  These tests exercise the real
``tools/fleet/pbcanary_legs/leg3.py --run-action`` entry point against a tiny
staged fixture, with an OS-level forbidden-origin tripwire (an ``LD_PRELOAD``
interposer compiled from C) that denies every ``open`` of the declared
originals while leaving them readable in the parent and to ordinary
processes.  A passing envelope under that tripwire proves the action served
its bytes from the reader-lease pin, not from the pool path.

The fixture publishes real residency-map fragments, publish-time material
sidecars, and a composed map through the existing interfaces; each chunk is a
reader lease of its own so a phase not yet staged is waited for within the
declared allowance instead of demanding the whole working set up front.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
FLEET_DIR = REPO / "tools" / "fleet"
SRC = REPO / "src"
for _path in (str(SRC), str(FLEET_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from prismabuild import pool, reader_lease, residency_map  # noqa: E402
from pbcanary_legs import leg3  # noqa: E402
from pbcanary_legs.common import deterministic_bytes, sha256_hex  # noqa: E402
import pbcanary  # noqa: E402

CONSUMER = "a" * 64
NONCE = "c" * 32
SCOPE = "unit-leg3"
TIER = "prismabuild-stage:testbox"
MANIFEST_SHA = "d" * 64
PROGRESS_TOKEN = "e" * 32
CHUNK_SIZES = (4096, 6144, 8192)  # Tiny routing fixture (canonical sizes below).


def _mover(index: int) -> str:
    return f"{index + 1:064x}"


_TRIPWIRE_SOURCE = r"""
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

static int tripwire_forbidden(const char *path) {
    const char *prefix;
    size_t length;
    if (path == NULL) return 0;
    prefix = getenv("PRISMABUILD_TRIPWIRE_FORBID");
    if (prefix == NULL || *prefix == '\0') return 0;
    length = strlen(prefix);
    if (strncmp(path, prefix, length) != 0) return 0;
    return path[length] == '\0' || path[length] == '/';
}

static void tripwire_note(const char *path) {
    const char *log = getenv("PRISMABUILD_TRIPWIRE_LOG");
    int fd;
    char line[4200];
    int length;
    if (log == NULL || *log == '\0' || path == NULL) return;
    fd = (int)syscall(SYS_openat, AT_FDCWD, log,
                      O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0) return;
    length = snprintf(line, sizeof(line), "deny %s\n", path);
    if (length > 0) {
        ssize_t written = write(fd, line, (size_t)length);
        (void)written;
    }
    (void)close(fd);
}

static int tripwire_deny(const char *path) {
    tripwire_note(path);
    errno = EACCES;
    return -1;
}

static mode_t tripwire_mode(int flags, va_list ap) {
    mode_t mode = 0;
    if (flags & (O_CREAT | O_TMPFILE)) mode = (mode_t)va_arg(ap, int);
    return mode;
}

int open(const char *path, int flags, ...) {
    mode_t mode;
    va_list ap;
    if (tripwire_forbidden(path)) return tripwire_deny(path);
    va_start(ap, flags);
    mode = tripwire_mode(flags, ap);
    va_end(ap);
    return (int)syscall(SYS_openat, AT_FDCWD, path, flags, mode);
}

int open64(const char *path, int flags, ...) {
    mode_t mode;
    va_list ap;
    if (tripwire_forbidden(path)) return tripwire_deny(path);
    va_start(ap, flags);
    mode = tripwire_mode(flags, ap);
    va_end(ap);
    return (int)syscall(SYS_openat, AT_FDCWD, path, flags, mode);
}

int openat(int dirfd, const char *path, int flags, ...) {
    mode_t mode;
    va_list ap;
    if (tripwire_forbidden(path)) return tripwire_deny(path);
    va_start(ap, flags);
    mode = tripwire_mode(flags, ap);
    va_end(ap);
    return (int)syscall(SYS_openat, dirfd, path, flags, mode);
}

int openat64(int dirfd, const char *path, int flags, ...) {
    mode_t mode;
    va_list ap;
    if (tripwire_forbidden(path)) return tripwire_deny(path);
    va_start(ap, flags);
    mode = tripwire_mode(flags, ap);
    va_end(ap);
    return (int)syscall(SYS_openat, dirfd, path, flags, mode);
}
"""


def _compile_tripwire(compiler: str, directory: Path) -> Path:
    source = directory / "forbidden_origin.c"
    source.write_text(_TRIPWIRE_SOURCE, encoding="utf-8")
    library = directory / "forbidden_origin.so"
    completed = subprocess.run(
        [compiler, "-shared", "-fPIC", "-O2", "-o", str(library), str(source)],
        capture_output=True, text=True)
    if completed.returncode != 0:
        pytest.fail(
            "cannot build the OS-level forbidden-origin tripwire "
            f"({compiler}): {completed.stderr.strip()}")
    return library


@pytest.fixture(scope="session")
def tripwire(tmp_path_factory) -> Path:
    """A compiled libc interposer that denies the declared origin tree.

    OS-level, not a Python mock: the denial happens in the child's libc
    ``open``/``openat`` before any byte is read, and every denial is logged
    with the exact path for the test to inspect.  A missing compiler is a
    concrete failure of the acceptance, never a silent skip.
    """
    compiler = next(
        (found for candidate in ("cc", "gcc", "clang")
         if (found := shutil.which(candidate))), None)
    if compiler is None:
        pytest.fail(
            "no C compiler on this box: the forbidden-origin tripwire is an "
            "OS-level denial and this acceptance is not reduced to a Python "
            "mock; install cc/gcc/clang on the executing worker")
    return _compile_tripwire(
        compiler, tmp_path_factory.mktemp("leg3-tripwire"))


class _Fleet:
    """Tiny staged fleet: queue, stage root, origins, fragments, manifest."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp = tmp_path
        self.queue = pool.PoolQueue(tmp_path / "pb-queue")
        self.queue.ensure_layout()
        self.stage = tmp_path / "stage"
        self.stage.mkdir()
        self.origins = tmp_path / "origins"
        self.origins.mkdir()
        self.root = self.queue.root / pool.RESIDENCY
        self.keys: list[str] = []
        self.fragments: list[dict] = []
        self.materials: list[dict] = []
        self.entries: list[dict] = []
        self.contents: list[bytes] = []

    def add_chunk(self, index: int, data: bytes) -> None:
        origin = self.origins / leg3.LEG3_CHUNK_NAMES[index]
        origin.write_bytes(data)
        staged = self.stage / f"chunk-{index}.staged"
        staged.write_bytes(data)
        digest = sha256_hex(data)
        key = residency_map.residency_map_key(str(origin), 0)
        identity = reader_lease.stat_identity(str(staged))
        assert identity is not None
        self.keys.append(key)
        self.contents.append(data)
        self.fragments.append({
            "stage_path": str(staged), "bytes": len(data),
            "sha256": digest, "offset": 0})
        self.materials.append({
            "stage_path": str(staged), "bytes": len(data),
            "sha256": digest, "file_id": identity})
        self.entries.append({
            "path": str(origin), "offset": 0, "bytes": len(data),
            "sha256": digest})

    def publish(self, indices) -> None:
        """Publish the named chunks as that phase's mover, then recompose."""
        for index in indices:
            residency_map.write_fragment(self.root, {
                "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
                "consumer_action_key": CONSUMER,
                "mover_action_key": _mover(index),
                "tier_id": TIER, "stage_root": str(self.stage),
                "manifest_sha256": MANIFEST_SHA,
                "entries": {self.keys[index]: self.fragments[index]}})
            reader_lease.write_material(
                self.root, consumer_action_key=CONSUMER,
                mover_action_key=_mover(index), tier_id=TIER,
                stage_root=str(self.stage), manifest_sha256=MANIFEST_SHA,
                generation=reader_lease.mint_generation(),
                entries={self.keys[index]: self.materials[index]})
        mapping = residency_map.compose(
            residency_map.read_fragments(self.root, CONSUMER))
        residency_map.write_map(
            self.queue.residency_map_path(CONSUMER), mapping)

    def file_claim(self) -> None:
        claimed = self.queue.dir(pool.CLAIMED)
        claimed.mkdir(parents=True, exist_ok=True)
        (claimed / f"{CONSUMER}.json").write_text(json.dumps({
            "action_key": CONSUMER, "claimed_by": "worker-leg3",
            "claimed_host": "test-host", "published_unix": 1.0,
            "resources": {"cpu": 1, "mem_gb": 1},
            "resource_scope": {"action_key": CONSUMER, "nonce": NONCE,
                               "scope_id": SCOPE}}))

    def write_manifest(self) -> Path:
        path = self.tmp / "leg3.manifest.json"
        path.write_text(json.dumps({
            "schema": leg3.LEG3_MANIFEST_SCHEMA,
            "produced_by": {"tool": "pbcanary", "leg": 3},
            "annotations": {"producer": "pbcanary", "leg": 3},
            "mount_prefix": str(self.tmp),
            "entries": self.entries,
            "entry_count": len(self.entries),
            "total_bytes": sum(entry["bytes"] for entry in self.entries),
        }), encoding="utf-8")
        return path

    @property
    def trips(self) -> Path:
        return self.tmp / "trips.log"

    def tripwire_env(self, library: Path) -> dict:
        return {"LD_PRELOAD": str(library),
                "PRISMABUILD_TRIPWIRE_FORBID": str(self.origins),
                "PRISMABUILD_TRIPWIRE_LOG": str(self.trips)}

    def tripwire_denials(self) -> list[str]:
        if not self.trips.exists():
            return []
        return [line for line in self.trips.read_text().splitlines() if line]


def _build_fleet(tmp_path: Path, sizes=CHUNK_SIZES) -> _Fleet:
    fleet = _Fleet(tmp_path)
    for index, size in enumerate(sizes):
        fleet.add_chunk(index, deterministic_bytes(
            b"/leg3/chunk%d" % index, size))
    return fleet


def _child_env(fleet: _Fleet, manifest: Path, *, library: Path | None = None,
               identity: bool = True, progress: bool = True,
               phases=None, helper: bool = True,
               wait_s: float | None = None) -> dict:
    env = {name: value for name, value in os.environ.items()
           if not name.startswith("PRISMABUILD_") and name != "LD_PRELOAD"}
    env["PBCANARY_LEG3_MANIFEST"] = str(manifest)
    env["PRISMABUILD_READER_HELPER_ROOT"] = str(REPO)
    if identity:
        env["PRISMABUILD_ACTION_KEY"] = CONSUMER
        env["PRISMABUILD_RESIDENCY_MAP"] = str(
            fleet.queue.residency_map_path(CONSUMER))
        env["PRISMABUILD_ACTION_NONCE"] = NONCE
        env["PRISMABUILD_ACTION_SCOPE"] = SCOPE
    if progress:
        env["PRISMABUILD_ACTION_PROGRESS_PATH"] = str(
            fleet.tmp / "progress.json")
        env["PRISMABUILD_ACTION_PROGRESS_TOKEN"] = PROGRESS_TOKEN
        env["PRISMABUILD_ACTION_PROGRESS_PHASES"] = json.dumps(
            list(phases if phases is not None else leg3.LEG3_PHASES))
        if helper:
            env["PRISMABUILD_ACTION_PROGRESS_HELPER"] = str(
                SRC / "prismabuild" / "progress.py")
    if wait_s is not None:
        env[leg3.LEG3_STAGE_WAIT_ENV] = str(wait_s)
    if library is not None:
        env.update(fleet.tripwire_env(library))
    return env


def _run_action(env: dict, *, timeout: float = 300.0) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(FLEET_DIR / "pbcanary_legs" / "leg3.py"),
         "--run-action"],
        cwd=str(REPO), env=env, capture_output=True, text=True,
        timeout=timeout)


def _envelope(completed: subprocess.CompletedProcess) -> dict | None:
    for line in reversed(completed.stdout.splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict) and value.get("schema") == leg3.LEG3_SCHEMA:
            return value
    return None


def _watch(fleet: _Fleet):
    return pool.ProgressWatch(
        fleet.tmp / "progress.json", PROGRESS_TOKEN,
        pool.ProgressPolicy(tuple(
            pool.ProgressPhase(name, 600.0, None)
            for name in leg3.LEG3_PHASES), None), started=0.0)


# --- the tripwire is a real OS denial, and originals stay readable ----------


def test_tripwire_denies_an_actual_origin_open_before_bytes(
        tripwire: Path, tmp_path: Path) -> None:
    fleet = _build_fleet(tmp_path)
    origin = Path(fleet.entries[0]["path"])
    control = subprocess.run(
        ["/bin/cat", str(origin)], env=fleet.tripwire_env(tripwire),
        capture_output=True, text=True)
    assert control.returncode != 0
    assert fleet.tripwire_denials() == [f"deny {origin}"]
    # The originals remain readable: the tripwire is process-scoped, not a
    # deletion or a chmod, and an ordinary process reads them normally.
    assert origin.read_bytes() == fleet.contents[0]
    plain = subprocess.run(["/bin/cat", str(origin)], capture_output=True)
    assert plain.returncode == 0 and plain.stdout == fleet.contents[0]


# --- staged reads route through the reader lease, origins forbidden --------


def test_leg3_reads_every_chunk_through_the_staged_reader(
        tripwire: Path, tmp_path: Path) -> None:
    fleet = _build_fleet(tmp_path)
    fleet.file_claim()
    fleet.publish(range(3))
    manifest = fleet.write_manifest()
    env = _child_env(fleet, manifest, library=tripwire)

    completed = _run_action(env)
    envelope = _envelope(completed)
    assert completed.returncode == 0, (completed.returncode, completed.stderr,
                                       envelope)
    assert envelope is not None and envelope.get("ok") is True, envelope
    assert [chunk["sha256"] for chunk in envelope["chunks"]] == [
        entry["sha256"] for entry in fleet.entries]
    assert envelope["combined"] == sha256_hex(b"".join(fleet.contents))
    for chunk, entry in zip(envelope["chunks"], fleet.entries):
        serving = chunk["serving"]
        assert serving["tier_id"] == TIER
        assert serving["pin_id"]
        assert serving["range_ref"] == f"0:{entry['path']}"
        assert serving["path"] != entry["path"]
        assert serving["path"].startswith(str(fleet.stage))
    assert fleet.tripwire_denials() == []


def test_leg3_waits_for_a_later_phase_instead_of_failing(
        tripwire: Path, tmp_path: Path) -> None:
    """The moving window: chunk 1 arrives mid-read, not before the claim."""
    fleet = _build_fleet(tmp_path)
    fleet.file_claim()
    fleet.publish([0])
    manifest = fleet.write_manifest()
    env = _child_env(fleet, manifest, library=tripwire, wait_s=120.0)

    process = subprocess.Popen(
        [sys.executable, str(FLEET_DIR / "pbcanary_legs" / "leg3.py"),
         "--run-action"],
        cwd=str(REPO), env=env, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True)
    try:
        time.sleep(1.5)
        fleet.publish([0, 1])
        time.sleep(1.5)
        fleet.publish([0, 1, 2])
        stdout, stderr = process.communicate(timeout=300.0)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
    completed = subprocess.CompletedProcess(
        process.args, process.returncode, stdout, stderr)
    envelope = _envelope(completed)
    assert completed.returncode == 0, (completed.returncode, stderr, envelope)
    assert envelope is not None and envelope.get("ok") is True, envelope
    assert envelope["combined"] == sha256_hex(b"".join(fleet.contents))
    assert fleet.tripwire_denials() == []


def test_leg3_refuses_without_any_staged_coverage(
        tripwire: Path, tmp_path: Path) -> None:
    fleet = _build_fleet(tmp_path)
    fleet.file_claim()
    manifest = fleet.write_manifest()
    env = _child_env(fleet, manifest, library=tripwire, wait_s=2.0)

    completed = _run_action(env)
    envelope = _envelope(completed)
    assert completed.returncode != 0
    assert envelope is not None and envelope.get("ok") is False, envelope
    error = str(envelope.get("error") or "")
    assert "staged-read refusal" in error, error
    for entry in fleet.entries:
        assert entry["path"] not in error
    assert fleet.tripwire_denials() == []


def test_leg3_refuses_an_unpublished_range_without_touching_origins(
        tripwire: Path, tmp_path: Path) -> None:
    fleet = _build_fleet(tmp_path)
    fleet.file_claim()
    fleet.publish([0, 1])  # Chunk 2 never arrives.
    manifest = fleet.write_manifest()
    env = _child_env(fleet, manifest, library=tripwire, wait_s=2.0)

    completed = _run_action(env)
    envelope = _envelope(completed)
    assert completed.returncode != 0
    assert envelope is not None and envelope.get("ok") is False, envelope
    error = str(envelope.get("error") or "")
    assert "staged-read refusal" in error, error
    # The refusal names the undeclared range but is not an OS denial of the
    # origin: no origin open was attempted (the tripwire log stays empty).
    assert "Permission denied" not in error, error
    assert fleet.tripwire_denials() == []


def test_leg3_refuses_without_admitted_launch_identity(
        tripwire: Path, tmp_path: Path) -> None:
    fleet = _build_fleet(tmp_path)
    fleet.file_claim()
    fleet.publish(range(3))
    manifest = fleet.write_manifest()
    env = _child_env(fleet, manifest, library=tripwire, identity=False,
                     wait_s=2.0)

    completed = _run_action(env)
    envelope = _envelope(completed)
    assert completed.returncode != 0
    assert envelope is not None and envelope.get("ok") is False, envelope
    error = str(envelope.get("error") or "")
    assert "staged-read refusal" in error, error
    for entry in fleet.entries:
        assert entry["path"] not in error
    assert fleet.tripwire_denials() == []


def test_leg3_refuses_when_no_progress_channel_is_declared(
        tripwire: Path, tmp_path: Path) -> None:
    fleet = _build_fleet(tmp_path)
    fleet.file_claim()
    fleet.publish(range(3))
    manifest = fleet.write_manifest()
    env = _child_env(fleet, manifest, library=tripwire, progress=False,
                     wait_s=2.0)

    completed = _run_action(env)
    envelope = _envelope(completed)
    assert completed.returncode != 0
    assert envelope is not None and envelope.get("ok") is False, envelope
    assert "staged-read refusal" in str(envelope.get("error") or "")
    assert fleet.tripwire_denials() == []


# --- progress: committed only from durable work, accepted by the watchdog ---


def test_leg3_refuses_when_a_progress_commit_is_refused(tmp_path: Path) -> None:
    """A declared phase list that excludes a later phase rejects its commit."""
    fleet = _build_fleet(tmp_path)
    fleet.file_claim()
    fleet.publish(range(3))
    manifest = fleet.write_manifest()
    env = _child_env(fleet, manifest, phases=[leg3.LEG3_PHASES[0]])

    completed = _run_action(env)
    envelope = _envelope(completed)
    assert completed.returncode != 0
    assert envelope is not None and envelope.get("ok") is False, envelope
    error = str(envelope.get("error") or "")
    assert "progress" in error and leg3.LEG3_PHASES[1] in error, error


def test_leg3_reports_accepted_cumulative_progress(tmp_path: Path) -> None:
    fleet = _build_fleet(tmp_path)
    fleet.file_claim()
    fleet.publish(range(3))
    manifest = fleet.write_manifest()
    env = _child_env(fleet, manifest)

    completed = _run_action(env)
    envelope = _envelope(completed)
    assert completed.returncode == 0, (completed.returncode, envelope)
    assert envelope is not None and envelope.get("ok") is True, envelope
    watch = _watch(fleet)
    assert watch.sample(now=1.0) is True, watch.last_rejection
    assert watch.accepted == 1
    assert watch.last_accepted["units_completed"] == len(leg3.LEG3_PHASES)
    assert watch.last_accepted["phase"] == leg3.LEG3_PHASES[-1]
    assert watch.phase_index == len(leg3.LEG3_PHASES) - 1


# --- canonical 6/8/10 MiB integrity still holds through the reader ----------


def test_canonical_chunks_hash_through_the_staged_reader(
        tripwire: Path, tmp_path: Path) -> None:
    fleet = _build_fleet(tmp_path, sizes=leg3.LEG3_CHUNK_SIZES)
    fleet.file_claim()
    fleet.publish(range(3))
    manifest = fleet.write_manifest()
    env = _child_env(fleet, manifest, library=tripwire)

    completed = _run_action(env, timeout=600.0)
    envelope = _envelope(completed)
    assert completed.returncode == 0, (completed.returncode, envelope)
    assert envelope is not None and envelope.get("ok") is True, envelope
    assert [chunk["sha256"] for chunk in envelope["chunks"]] == list(
        leg3.LEG3_EXPECTED_CHUNK_SHA256)
    assert envelope["combined"] == leg3.LEG3_EXPECTED_COMBINED
    assert fleet.tripwire_denials() == []


# --- receipt-level verification gates staged qualification ------------------


def _good_receipt(fleet: _Fleet) -> dict:
    envelope = {
        "schema": leg3.LEG3_SCHEMA, "leg": 3, "ok": True,
        "chunks": [
            {"path": entry["path"], "offset": 0, "bytes": entry["bytes"],
             "sha256": entry["sha256"],
             "serving": {"tier_id": TIER, "epoch": "",
                         "pin_id": "f" * 32,
                         "range_ref": f"0:{entry['path']}",
                         "path": str(fleet.stage / f"chunk-{index}.staged")}}
            for index, entry in enumerate(fleet.entries)],
        "combined": sha256_hex(b"".join(fleet.contents)),
        "progress": [
            {"phase": phase, "units_completed": index + 1, "committed": True}
            for index, phase in enumerate(leg3.LEG3_PHASES)],
    }
    raw = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
    return {"stdout": raw + "\n", "returncode": 0, "artifact": raw + "\n"}


def _observation(units: int = 3) -> dict:
    return {
        "source": "action-progress", "accepted_count": 1, "rejected_count": 0,
        "phases_entered": units, "last_rejection": None,
        "last_accepted": {"phase": leg3.LEG3_PHASES[-1],
                          "units_completed": units, "unit": None,
                          "reported_unix": 1.0},
    }


def test_verify_requires_accepted_cumulative_progress(tmp_path: Path) -> None:
    fleet = _build_fleet(tmp_path)
    expected = {"chunks": [
        {"name": leg3.LEG3_CHUNK_NAMES[index], "size": entry["bytes"],
         "sha256": entry["sha256"]} for index, entry in enumerate(fleet.entries)],
        "combined": sha256_hex(b"".join(fleet.contents))}

    receipt = _good_receipt(fleet)
    assert leg3.verify(receipt, expected)[0] is False  # no observation at all
    assert leg3.verify({**receipt, "progress_observation": {}}, expected)[0] is False
    bool_count = {**_observation(), "accepted_count": True}
    assert leg3.verify(
        {**receipt, "progress_observation": bool_count}, expected)[0] is False
    short = _observation(units=2)
    assert leg3.verify({**receipt, "progress_observation": short}, expected)[0] is False
    early_phase = _observation()
    early_phase["last_accepted"]["phase"] = leg3.LEG3_PHASES[0]
    assert leg3.verify(
        {**receipt, "progress_observation": early_phase}, expected)[0] is False
    fewer_phases = {**_observation(), "phases_entered": 2}
    assert leg3.verify(
        {**receipt, "progress_observation": fewer_phases}, expected)[0] is False
    good = {**receipt, "progress_observation": _observation()}
    ok, reason = leg3.verify(good, expected)
    assert ok, reason


def test_verify_refuses_malformed_serving_evidence(tmp_path: Path) -> None:
    fleet = _build_fleet(tmp_path)
    expected = {"chunks": [
        {"name": leg3.LEG3_CHUNK_NAMES[index], "size": entry["bytes"],
         "sha256": entry["sha256"]} for index, entry in enumerate(fleet.entries)],
        "combined": sha256_hex(b"".join(fleet.contents))}
    base = {**_good_receipt(fleet), "progress_observation": _observation()}

    def mutated(mutator) -> dict:
        raw = json.loads(base["stdout"])
        mutator(raw)
        text = json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n"
        return {**base, "stdout": text, "artifact": text}

    assert leg3.verify(base, expected)[0] is True

    def drop_serving(raw):
        del raw["chunks"][0]["serving"]
    assert leg3.verify(mutated(drop_serving), expected)[0] is False

    def drop_range_ref(raw):
        del raw["chunks"][0]["serving"]["range_ref"]
    assert leg3.verify(mutated(drop_range_ref), expected)[0] is False

    def origin_path(raw):
        origin = fleet.entries[0]["path"]
        raw["chunks"][0]["serving"]["path"] = origin
    assert leg3.verify(mutated(origin_path), expected)[0] is False

    def pool_tier(raw):
        raw["chunks"][0]["serving"]["tier_id"] = "arc:testbox"
    assert leg3.verify(mutated(pool_tier), expected)[0] is False

    bool_count = {**base, "progress_observation": {
        **_observation(), "accepted_count": True}}
    assert leg3.verify(bool_count, expected)[0] is False


# --- the driver binds accepted progress to the receipt-producing attempt ----


def _attempt_outcome(*, key: str, published_unix: float, attempt: int,
                     stdout: str, observation: dict | None) -> dict:
    return {
        "schema": pool.POOL_ATTEMPT_SCHEMA_V1,
        "action_key": key, "published_unix": published_unix,
        "attempt": attempt, "max_attempts": 1, "retry_safe": True,
        "status": "executed", "disposition": "done",
        "claimed_by": "worker", "claimed_unix": published_unix,
        "claimed_host": "test-host", "finished_unix": published_unix,
        "finished_host": "test-host",
        "detail": {"status": "executed", "returncode": 0,
                   "progress_observation": observation},
        "logs": {},
    }


def _write_readonly(path: Path, data: bytes) -> None:
    """Publish one immutable attempt file the way the pool does (mode 0444)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(data)
    os.chmod(temporary, 0o444)
    os.replace(temporary, path)


def _write_terminal(queue, key: str, *, attempts: list[dict],
                    status: str = "executed") -> None:
    published_unix = 100.0
    record = {
        "schema": "prismaquant.prismabuild.pool_action.v1",
        "action_key": key, "published_unix": published_unix,
        "status": status, "attempts": len(attempts), "max_attempts": 1,
        "retry_safe": True, "attempt_history": [],
    }
    for number, stdout in enumerate(attempts, start=1):
        outcome = _attempt_outcome(
            key=key, published_unix=published_unix, attempt=number,
            stdout=stdout, observation=_observation())
        path = queue.attempt_path(record, number)
        logs = {}
        for stream, text in (("stdout", stdout), ("stderr", "")):
            data = text.encode("utf-8")
            digest = hashlib.sha256(data).hexdigest()
            log_path = queue.attempt_log_path(record, number, stream, digest)
            _write_readonly(log_path, data)
            logs[stream] = {"bytes": len(data), "sha256": digest,
                            "path": str(log_path.relative_to(queue.root))}
        outcome["logs"] = logs
        _write_readonly(path, json.dumps(outcome).encode("utf-8"))
        record["attempt_history"].append(
            {"attempt": number, "outcome": str(path.relative_to(queue.root))})
    terminal = queue.dir(pool.DONE)
    terminal.mkdir(parents=True, exist_ok=True)
    (terminal / f"{key}.json").write_text(json.dumps(record),
                                          encoding="utf-8")


def _paths(queue, tmp_path: Path) -> dict:
    return {"queue_root": str(queue.root), "published_src": str(SRC),
            "cas_root": str(tmp_path / "cas")}


def test_driver_binds_progress_to_the_receipt_attempt(tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    artifact = '{"schema":"prismabuild.pbcanary.leg3.v1","ok":true}\n'
    _write_terminal(queue, CONSUMER, attempts=[artifact])
    evidence = pbcanary.terminal_progress_observation(
        _paths(queue, tmp_path), CONSUMER, artifact)
    assert evidence is not None, "executed attempt carrying the artifact"
    assert evidence["attempt"] == 1
    assert evidence["observation"]["last_accepted"]["units_completed"] == 3

    # A superseding terminal attempt must not lend its observation to an
    # artifact another attempt produced: the binding follows the artifact to
    # its own attempt, and an artifact no attempt's stdout carries proves
    # nothing.
    superseding = '{"schema":"prismabuild.pbcanary.leg3.v1","ok":false}\n'
    _write_terminal(queue, CONSUMER, attempts=[artifact, superseding])
    bound = pbcanary.terminal_progress_observation(
        _paths(queue, tmp_path), CONSUMER, artifact)
    assert bound is not None and bound["attempt"] == 1
    latest = pbcanary.terminal_progress_observation(
        _paths(queue, tmp_path), CONSUMER, superseding)
    assert latest is not None and latest["attempt"] == 2
    assert pbcanary.terminal_progress_observation(
        _paths(queue, tmp_path), CONSUMER, "not in any attempt\n") is None


def test_driver_attaches_the_observation_to_the_verify_envelope(
        tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(pbcanary, "submit_leg",
                        lambda *a, **k: ("key-1", {"action_key": "key-1"}))
    monkeypatch.setattr(pbcanary, "wait_leg", lambda *a, **k: {
        "returncode": 0, "stdout": "", "stderr": "", "record": {}})
    monkeypatch.setattr(pbcanary, "load_verified_receipt",
                        lambda *a, **k: ({}, tmp_path / "receipt.json",
                                         b"artifact\n"))
    monkeypatch.setattr(pbcanary, "terminal_progress_observation",
                        lambda *a, **k: {"observation": _observation(),
                                         "attempt": 1})
    leg_dir = tmp_path / "leg-3"
    leg_dir.mkdir()
    paths = {"queue_root": str(tmp_path / "pb-queue"),
             "published_src": str(SRC), "cas_root": str(tmp_path / "cas")}
    envelope, _ref, _blob, action_key = pbcanary._execute_side(
        paths, leg="leg-3", spec={"name": "leg-3"}, argv=["true"],
        checkout=tmp_path, run_id="run-1", generation=None, priority=-10,
        fleet_root=tmp_path, leg_dir=leg_dir, side=None, extra_flags=[],
        extra_env={}, manifest=None, wait_s=60)
    assert action_key == "key-1"
    assert envelope["progress_observation"]["last_accepted"][
        "units_completed"] == 3
    assert envelope["progress_attempt"] == 1

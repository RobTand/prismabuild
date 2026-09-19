#!/usr/bin/env python3
"""Publish this checkout as the bytes the fleet executes, and say which commit.

Workers do not run this repository.  They run ``/mnt/shared/prismabuild-fleet/
repo`` -- a copy on the one filesystem every box mounts -- and until now that
copy was maintained by hand.  The failure that follows from a hand-copied
runtime is not subtle: a fix lands in the checkout, the fleet keeps executing
the old bytes, and the two disagree silently because nothing anywhere records
which commit the mirror is.  Thirty-two resubmissions died instantly on
``AttributeError: 'PoolQueue' object has no attribute 'placeable'`` -- a
method committed twenty minutes earlier, in a file the mirror did not have.

So publication becomes a complete immutable generation with a receipt.
``RUNTIME_VERSION.json`` records the commit, whether the tree was dirty, and
a sha256 per published file next to the bytes themselves; the generation is
copied, revalidated and import-probed off-line, then ``repo`` moves to it in
one namespace operation.  A caller therefore sees one complete generation,
never the member-by-member interval a receipt could not attest.  Worker
attestation already records the sha256 of what it loaded, so the two can be
compared after the fact and a mismatch is a fact rather than a suspicion.

The mirror keeps its scripts at the top of ``tools/`` while the checkout has
them under ``tools/fleet/``, because the hook and older command lines address
the flat path.  Both are written, from the same source, rather than left to
drift into two versions of one file -- which is exactly what happened to
``worker_loop.py``, where the flat copy was current and the nested one was
three days stale.

After activation, ``--canary`` submits the fleet canary (issue #688) resolving
against the new generation and records ``verified`` or ``failed`` in the
rollout record ``runtime-generations/<generation>.canary.json``; ``--no-canary``
records ``not_run``. The gate is default-OFF (phase 1: ``CANARY_DEFAULT_ENABLED``).
A failed canary marks and exits nonzero; it never rolls back and never touches
admission. See the canary runbook.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import shutil
import socket
import stat
import subprocess
import sys
import threading
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
import fleet_roster  # noqa: E402

_SCRIPT = Path(__file__).resolve()
CHECKOUT = _SCRIPT.parents[1] if _SCRIPT.parent.name == "tools" else _SCRIPT.parents[2]
MIRROR = Path("/mnt/shared/prismabuild-fleet/repo")
_PUBLISH_MUTEX = threading.RLock()
_PUBLISH_LOCK_DEPTH = 0
#: Published as ``tools/<name>`` *and* ``tools/fleet/<name>``.
FLEET_SCRIPTS = (
    "dispatch_tessera_model.py",
    "docker", "pbrun.py", "pbtest.py", "pbtest_pins.py", "require_pool.py", "worker_loop.py", "worker.py",
    "render_identity.py", "seal_and_publish.py", "tessera_status.py",
    "dispatch_tessera_shards.py", "dispatch_tessera_ladder.py",
    "publish_runtime.py", "pool_reset.py", "runtime_paths.py", "supervise.py",
    # The supervisor spawns prewarm_loop.py on a box declaring the storage
    # role, by published path like every other child. A generation without it
    # makes that role a log line saying the script is not there (#487).
    "prewarm_loop.py",
    # ...and tier_loop.py on a box declaring the tiers role (#583): it
    # mints and announces the box's discovered storage tiers, and a role
    # script that does not travel cannot be spawned.
    "tier_loop.py",
    # ...and stage_move.py, which is not a role script at all: it is the
    # movement node itself, exec'd by an admitted action on the storage box
    # (#583).  A generation without it publishes movers nothing can run.
    "stage_move.py",
    # ...and stage_release.py beside it, the egress node that deletes a staged
    # range and returns its tier tokens.  The tier loop also imports it for the
    # orphan sweep, so a generation without it leaves a withdrawn consumer's
    # movers holding the stage with nothing able to take it back.
    "stage_release.py",
    # ...and ram_promote.py, the ram tier's movement node (#640): it copies a
    # landed stage range into the tmpfs and files the fragment that carries
    # the epoch.  A generation without it leaves the ram window publishing
    # promotions nothing can run.
    "ram_promote.py",
    # The SLURM lane's two halves: the shared submit every producer routes
    # through, and the job entry it names.  A runtime published without them
    # has producers importing a module that is not there and a batch script
    # execing a path that does not exist.
    "fleet_submit.py", "slurm_job.py",
    # The commands an operator runs on a box that has no checkout.  The
    # published pool_reset.py:582 tells them to run ``pbwait.py <key>``, and
    # neither dl380g10 nor sparklina has a checkout to run it from.
    # runtime_process_census.py is here for the same reason and is only
    # useful there: it reads /proc on the box it runs on.
    "pbstatus.py", "pbmetrics.py", "pbwait.py", "pbcampaign.py", "runtime_process_census.py",
    # pbmcp.py is the same case as pbstatus.py and then some: an agent
    # registers it by absolute published path so that every session starts on
    # the current generation, and the boxes it is registered from are exactly
    # the ones with no checkout.
    "pbmcp.py",
    # retire_worker.py belongs to the same set for a sharper reason: the name
    # it retires is a name whose box stopped answering, and the operator who
    # notices is on whichever box is holding the mount.
    "retire_worker.py",
    # The exporter's installer travels with it for the same reason: the box
    # best placed to run it is a box with no checkout.
    "install_pbmetrics.sh",
    # mount_latency.py measures the medium from the box it runs on, so it has
    # to be present on a box with no checkout -- and most of all on the box
    # whose mount is the one in question, which is the box least able to fetch
    # it at the time somebody wants it.
    "mount_latency.py",
    # pb_gc.py is the same case: it sweeps a store from whichever box is
    # holding the mount, and it reads /proc on the box it runs on to find out
    # what is still live there.
    "pb_gc.py",
    # The reconcile, for the same reason: the endings it files are the ones
    # no waiter asked for, and the operator who notices they are missing is
    # on whichever box has the shared mount rather than the checkout.
    "pbsweep.py",
    # fleet_roster.py holds the roster presence contract (#606): which boxes
    # count as declared absent and what provenance that needs.  The
    # supervisor and the barrier preflight import it on boxes with no
    # checkout, so a generation without it cannot enforce a retirement.
    "fleet_roster.py",
    # Per-job containment clients and root-installed authority sources travel
    # with the generation; installation copies privileged code to root-owned
    # storage rather than executing it from this shared runtime.
    "resource_exec.py", "resource_broker.py", "resource_payload.py",
    "qualify_resource_scope.py", "install_resource_broker.sh",
    "upgrade_client.py", "install_client_upgrader.sh", "install_supervisor_unit.sh",
)
#: Fleet tools deliberately left out of the generation, each with the reason.
#: Runtime tools travel; qualification harnesses use submitted checkouts.
#: The tuple exists so that leaving one out is a decision
#: somebody wrote down rather than an omission nobody noticed.
EXCLUDED: tuple[tuple[str, str], ...] = (
    ("qualify_rollout.py",
     "paired rollout qualification actors use submitted checkouts and a fresh "
     "private shared root; their simulated host services are not an operator "
     "command or permission to activate the production barrier"),
    ("qualify_claim_recovery.py",
     "paired queue-recovery qualification actors run from an isolated "
     "checkout through pbcampaign against a fresh private queue root; "
     "not an operator command for a box without a checkout"),
    ("admission_shared_io.py",
     "a measurement harness for the admission critical section, run from a "
     "checkout through pbrun against a private queue root; a box with no "
     "checkout has no reason to run it and it must never be pointed at the "
     "live queue"),
    ("bench_prewarm_stage_release.py",
     "a standalone timing of the prewarm stage's release path, run from a "
     "checkout through pbrun over a named data manifest; it carries the "
     "pre-#589 arithmetic as its before arm, so it is a record of one "
     "measurement rather than an operator command"),
)


#: Rollout canary gate (issue #688, crew D). Two-phase adoption: this landing
#: is phase 1 with the gate default-OFF, so publication behaves exactly as
#: before unless ``--canary`` is passed. Phase 2 flips this constant to True
#: in a follow-up after the first green canary on main; ``--no-canary`` is
#: then the escape hatch. The constant (not an environment variable) is the
#: switch so the default is versioned, reviewable, and published with the
#: generation that implements it.
CANARY_DEFAULT_ENABLED = False
#: Rollout-record statuses. ``pending`` is written before the driver is
#: invoked and rewritten to ``verified`` or ``failed`` on its verdict;
#: ``not_run`` is the skip record. There is no separate error state: a canary
#: that could not verify the generation (missing driver, refusal, crash) is
#: ``failed`` with the reason in ``detail``, because "did not test" is never
#: "passed".
CANARY_STATUSES = ("pending", "verified", "failed", "not_run")
CANARY_RECORD_SCHEMA = "prismaquant.prismabuild.canary_status.v1"

#: Not code, but read by published code.  The supervisor on each box reads
#: the fleet's declared shape from ``fleet_boxes.json``, so a runtime
#: published without it starts no workers at all.  The tier loop reads the
#: ram tier's declared sizing from ``ram_tier_policy.json`` every cycle
#: (#640), so a change to it is a publish rather than an ssh -- and a
#: generation published without it discovers no ram tier, which is the
#: honest answer for a runtime that predates the tier.
FLEET_DATA = ("fleet_boxes.json", "ram_tier_policy.json")

# These originate at tools/, without a second tools/fleet/ spelling. The
# torch helper is copied by consumers following the published profile guide.
TOP_LEVEL_SCRIPTS = ("prismabuild_worker.py", "profile_torch.py")

# The dashboard deploy tests travel with the runtime, so their implementation
# and maintained dashboard must travel too. This does not start monitoring or
# modify an existing Grafana deployment during worker publication.
OBSERVABILITY_FILES = (
    "README.md", "build_dashboard.py", "deploy_dashboard.py", "prismabuild.json",
    "prismabuild-metrics.service", "prometheus.scrape.yml", "qualify_dashboard.py",
)

# ``pbstatus`` reads the /mnt/shared NFS readahead window through this helper
# and reports it; without the helper in the generation the reading is
# unavailable on every box that has no checkout, which is every worker. The
# helper is stdlib-only and read-only unless given ``--apply``, and publishing
# it installs nothing: the host unit beside it is still installed by an
# operator, as docs/fleet_storage.md says.
STORAGE_FILES = ("nfs_readahead.py",)

#: The modes a published generation's members carry, chosen rather than
#: inherited.  Until #316 the mode came from ``shutil.copy2`` preserving
#: whatever the *publishing checkout* happened to have, which is a box-local
#: umask artifact: a worktree created under umask 077 published every file
#: ``0500``/``0400``, and the same commit published from a umask-002 worktree
#: would have published ``0555``/``0444``.  A generation's permissions must be
#: a property of the generation, not of the shell that made it.
#:
#: World-readable, because a published generation has a reader that is not
#: ``rob``.  ``docs/mount_measurement.md`` installs Netdata's mount collector
#: as a symlink *into the live generation* on purpose, so that a later
#: publication re-points it and no re-link is ever owed -- which makes a
#: non-owner UID a first-class reader of published bytes.  The group bit alone
#: would not do it: Netdata is uid 983 in group ``rob`` on sparky only because
#: a ``usermod`` was run there, while on dl380g10 it is uid 984 with
#: ``groups=984(netdata),110(docker)`` and in no group of Rob's at all.  A
#: group-bit fix would make plugin adoption depend on a per-box ``usermod`` on
#: every current and future box; the ``other`` bits need no provisioning.
#: Nothing in a generation is secret -- it is PrismaBuild's own source, and
#: credentials and queue state live elsewhere -- and the directories on the
#: way already permit traversal by anyone.
#:
#: Write stays denied to everyone including the owner, so the property that
#: makes a generation quotable -- it is immutable and append-only history --
#: is exactly preserved.
PUBLISHED_EXECUTABLE_MODE = 0o555
PUBLISHED_FILE_MODE = 0o444

# Source and private qualification may exercise the epoch state machine, but a
# live publication stays disabled until the coordinator has reviewed the
# cross-host qualification evidence.  This guard is intentionally adjacent to
# the publication entrypoint rather than an operator flag: no command spelling
# can accidentally turn a source-only protocol into a fleet transition.
FINAL_BARRIER_QUALIFICATION_GUARD = True
PUBLISHED_DIRECTORY_MODE = 0o555

# Retain the loaded code without reading source at import. At first use, bind
# source bytes only if they compile to this code; a mutable checkout must not
# let a later pathname revision impersonate the coordinator already loaded.
COORDINATOR_SOURCE = Path(__file__).resolve()
COORDINATOR_CODE = sys._getframe().f_code
COORDINATOR_SHA256 = None


def _coordinator_sha256() -> str:
    global COORDINATOR_SHA256
    try:
        raw = COORDINATOR_SOURCE.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        if COORDINATOR_SHA256 is None:
            code = compile(raw, COORDINATOR_CODE.co_filename, "exec", dont_inherit=True)
            if code != COORDINATOR_CODE:
                raise SystemExit("rollout coordinator source changed after it was loaded")
            COORDINATOR_SHA256 = digest
        elif digest != COORDINATOR_SHA256:
            raise SystemExit("rollout coordinator source changed after it was loaded")
    except (OSError, SyntaxError, ValueError) as exc:
        raise SystemExit(f"rollout coordinator identity cannot be read: {exc}") from exc
    return COORDINATOR_SHA256


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_result(*argv: str):
    return subprocess.run(
        ["git", "-C", str(CHECKOUT), *argv],
        capture_output=True, text=True, check=False,
    )


def _commit_identity() -> str:
    """The exact Git commit the published bytes claim, or refuse.

    A linked worktree under the shared mount can still point its ``.git`` file
    at a box-local control directory.  On another box ``git rev-parse`` then
    exits 128; treating its empty stdout as a commit published real bytes under
    ``commit: ""``.  A receipt with no identity is not an approximate receipt.
    """

    result = _git_result("rev-parse", "--verify", "HEAD")
    commit = result.stdout.strip()
    if result.returncode != 0 or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        detail = (result.stderr or result.stdout or "no output").strip()
        raise SystemExit(
            "cannot prove a 40-hex Git commit for runtime publication: "
            f"git rev-parse --verify HEAD exited {result.returncode}: {detail}"
        )
    return commit


def _working_tree_dirty() -> bool:
    # Every file under the published source/script/test roots is eligible for
    # the generation below, including a newly-created one Git does not yet
    # track.  Excluding untracked paths here could therefore put bytes absent
    # from ``commit`` into a receipt that claimed ``dirty: false``.
    result = _git_result("status", "--porcelain")
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "no output").strip()
        raise SystemExit(
            "cannot prove whether the runtime checkout is clean: "
            f"git status exited {result.returncode}: {detail}"
        )
    return bool(result.stdout.strip())


def _git_index_modes() -> dict[str, int]:
    """The mode Git records for every tracked file, keyed by checkout path.

    The executable bit a published file carries is a fact the repository
    already states -- ``100755`` or ``100644`` in the index -- so publication
    reads it there rather than guessing from a filename, an extension, or a
    hardcoded list of "the plugin files".  The alternative that was in place
    read it from the publishing worktree's filesystem, which is why the same
    commit published different modes from different worktrees.

    A failure here is a refusal, in the shape ``_commit_identity`` uses: a
    publisher that cannot read the index cannot say which of its members are
    programs, and guessing would put an unexecutable collector on the fleet
    exactly as before.
    """

    result = _git_result("ls-files", "--stage", "-z")
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "no output").strip()
        raise SystemExit(
            "cannot read the Git index modes the published files derive from: "
            f"git ls-files --stage exited {result.returncode}: {detail}"
        )
    modes: dict[str, int] = {}
    for entry in result.stdout.split("\0"):
        if not entry:
            continue
        head, separator, path = entry.partition("\t")
        fields = head.split()
        if not separator or len(fields) != 3 or not re.fullmatch(r"[0-7]{6}", fields[0]):
            raise SystemExit(
                f"cannot parse a Git index entry for runtime publication: {entry!r}"
            )
        modes[path] = int(fields[0], 8)
    return modes


def _published_mode(source: Path, index_modes: dict[str, int]) -> int:
    """The mode ``source`` gets in the generation: 0555 if it is a program.

    Tracked files take the answer from the index.  An untracked one -- which
    only a ``--allow-dirty`` publication has -- has no index entry to read, so
    its own owner-execute bit is the only statement available and is used.
    Either way the group and other read bits are set, which is the whole point.
    """

    try:
        relative = source.resolve().relative_to(CHECKOUT.resolve()).as_posix()
    except ValueError:
        relative = None
    recorded = index_modes.get(relative) if relative is not None else None
    if recorded is not None:
        executable = bool(recorded & 0o111)
    else:
        executable = bool(source.stat().st_mode & stat.S_IXUSR)
    return PUBLISHED_EXECUTABLE_MODE if executable else PUBLISHED_FILE_MODE


def _source_for(name: str) -> Path:
    if name.startswith("tools/fleet/"):
        base = name.rsplit("/", 1)[1]
        source = CHECKOUT / "tools" / "fleet" / base
        return source if source.is_file() else CHECKOUT / "tools" / base
    if name in {f"tools/{script}" for script in TOP_LEVEL_SCRIPTS}:
        return CHECKOUT / name
    if name.startswith("tools/"):
        base = name.rsplit("/", 1)[1]
        source = CHECKOUT / "tools" / "fleet" / base
        return source if source.is_file() else CHECKOUT / "tools" / base
    return CHECKOUT / name


def _publication_manifest() -> dict[str, str]:
    published: dict[str, str] = {}
    for source in sorted((CHECKOUT / "src" / "prismabuild").glob("*.py")):
        published[f"src/prismabuild/{source.name}"] = _sha256(source)
    for name in FLEET_SCRIPTS:
        source = CHECKOUT / "tools" / "fleet" / name
        if not source.is_file():
            source = CHECKOUT / "tools" / name
        if not source.is_file():
            continue
        published[f"tools/{name}"] = _sha256(source)
        published[f"tools/fleet/{name}"] = published[f"tools/{name}"]
    for name in FLEET_DATA:
        source = CHECKOUT / "tools" / "fleet" / name
        if source.is_file():
            published[f"tools/{name}"] = _sha256(source)
            published[f"tools/fleet/{name}"] = published[f"tools/{name}"]
    skill = CHECKOUT / "skills" / "prismabuild" / "SKILL.md"
    if skill.is_file():
        published["skills/prismabuild/SKILL.md"] = _sha256(skill)
    # The installed skill requires the policy and operating guide. Keep the
    # small documentation tree and root guides together so their relative
    # references resolve within this same sealed generation, without a checkout.
    guides = [CHECKOUT / "README.md", CHECKOUT / "AGENTS.md",
              *sorted((CHECKOUT / "docs").rglob("*.md"))]
    for source in guides:
        if source.is_file():
            published[source.relative_to(CHECKOUT).as_posix()] = _sha256(source)
    # The published push-guard test depends on its repository-owned hook.
    hook = CHECKOUT / ".githooks" / "pre-push"
    if hook.is_file():
        published[".githooks/pre-push"] = _sha256(hook)
    for name in TOP_LEVEL_SCRIPTS:
        source = CHECKOUT / "tools" / name
        if source.is_file():
            published[f"tools/{name}"] = _sha256(source)
    for source in sorted((CHECKOUT / "tests").glob("*.py")):
        published[f"tests/{source.name}"] = _sha256(source)
    for name in OBSERVABILITY_FILES:
        source = CHECKOUT / "fleet" / "observability" / name
        if source.is_file():
            published[f"fleet/observability/{name}"] = _sha256(source)
    for name in STORAGE_FILES:
        source = CHECKOUT / "fleet" / "storage" / name
        if source.is_file():
            published[f"fleet/storage/{name}"] = _sha256(source)
    return published


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        try:
            os.fsync(descriptor)
        except OSError:
            # Some NFS servers decline directory fsync.  Every regular member
            # is still flushed before the namespace operation below.
            pass
    finally:
        os.close(descriptor)


def _write_receipt(path: Path, receipt: dict[str, object]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(receipt, handle, indent=1)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _probe(root: Path) -> None:
    probe = subprocess.run(
        [sys.executable, "-B", "-c",
         "import sys; sys.path.insert(0, r'%s'); "
         "from prismabuild import pool, core; "
         "assert hasattr(pool.PoolQueue, 'claim'); print('import ok')"
         % (root / "src")],
        capture_output=True, text=True, check=False,
    )
    print((probe.stdout or probe.stderr).strip())
    if probe.returncode != 0:
        raise SystemExit(
            f"refusing to activate a runtime that failed its import probe "
            f"with status {probe.returncode}"
        )


def _seal_generation(root: Path) -> None:
    """Deny write to everyone, and settle the read and execute bits.

    Generations are append-only history, so write is denied to the owner too.
    The rest of the mode is set here rather than inherited: a directory came
    from ``mkdir`` under the publisher's umask and the receipt came from
    ``open("w")`` under the same umask, so a umask-077 publisher would close
    the generation to its non-owner readers by accident -- which is the defect
    in #316, one level up from the files.  Every member therefore lands on
    ``PUBLISHED_*_MODE``; the copy loop has already set each file's execute
    bit from the Git index, and this preserves that distinction rather than
    re-deciding it.
    """

    for path in sorted(root.rglob("*"), reverse=True):
        if path.is_symlink():
            continue
        if path.is_dir():
            path.chmod(PUBLISHED_DIRECTORY_MODE)
        elif path.stat().st_mode & stat.S_IXUSR:
            path.chmod(PUBLISHED_EXECUTABLE_MODE)
        else:
            path.chmod(PUBLISHED_FILE_MODE)
    root.chmod(PUBLISHED_DIRECTORY_MODE)


def _unseal_tree(root: Path) -> None:
    """Undo ``_seal_generation`` on a tree that is still private to us."""

    for path in [root, *root.rglob("*")]:
        if path.is_symlink():
            continue
        path.chmod(path.stat().st_mode | 0o200)


def _remove_staging_tree(stage: Path) -> None:
    """Remove a private ``.staging`` tree even after it was sealed.

    ``_seal_generation`` runs before the rename, so a failure between the two
    leaves a read-only tree that plain ``rmtree`` cannot delete, and the leak
    sits under ``runtime-generations`` forever (issue #34).  Only the private
    staging target is ever unsealed here; a published generation is never
    touched.  This runs from a ``finally``: a cleanup failure is reported and
    swallowed so the publication error that caused it stays the exception the
    caller sees.
    """

    if stage.suffix != ".staging":
        raise ValueError(f"refusing to remove a non-staging tree: {stage}")
    try:
        _unseal_tree(stage)
        shutil.rmtree(stage)
    except OSError as exc:
        print(
            f"warning: staging tree survived failed publication: {stage}: {exc}",
            file=sys.stderr,
        )


def _activate(generation: Path, *, migrate_directory: bool) -> Path | None:
    """Expose ``generation`` at MIRROR in one namespace operation.

    Once MIRROR is a symlink, replacing a prepared sibling symlink is one
    atomic operation.  Linux ``RENAME_EXCHANGE`` would make the one-time move
    from the historical non-empty directory atomic too, but the fleet's NFS
    mount rejects that operation with ``EINVAL``.  That migration therefore
    requires an explicit maintenance flag: first retain the old directory,
    then install the complete symlink, rolling the old name back if install
    fails.  A caller can be refused in that narrow interval; it can never read
    a mixed generation.  Publication never deletes a prior generation.
    """

    MIRROR.parent.mkdir(parents=True, exist_ok=True)
    candidate = MIRROR.parent / f".{MIRROR.name}.activate-{uuid.uuid4().hex}"
    target = os.path.relpath(generation, MIRROR.parent)
    os.symlink(target, candidate)
    _fsync_directory(MIRROR.parent)
    legacy: Path | None = None
    try:
        if MIRROR.is_symlink() or not MIRROR.exists():
            os.replace(candidate, MIRROR)
        elif MIRROR.is_dir():
            if not migrate_directory:
                raise SystemExit(
                    f"{MIRROR} is the legacy directory runtime; refusing its "
                    "one-time maintenance handoff without --migrate-directory"
                )
            legacy = generation.parent / (
                f"legacy-{int(time.time())}-{uuid.uuid4().hex[:8]}"
            )
            os.replace(MIRROR, legacy)
            try:
                os.replace(candidate, MIRROR)
            except BaseException:
                if not MIRROR.exists() and not MIRROR.is_symlink():
                    os.replace(legacy, MIRROR)
                raise
        else:
            raise SystemExit(
                f"refusing to replace live runtime {MIRROR}: expected a "
                "directory or symlink"
            )
        _fsync_directory(MIRROR.parent)
    finally:
        if candidate.is_symlink():
            candidate.unlink()
    return legacy


class _CanaryPrecondition(Exception):
    """The canary produced no verdict on the generation.

    Missing driver, unimportable driver, a driver that raises, or a driver
    whose return shape drifted from the assumed contract below: in every case
    there is nothing to verify against, and "did not test" is never "passed",
    so the rollout record is marked ``failed`` with the reason in ``detail``.
    """


def _canary_driver_path() -> Path:
    """Where crew A's driver (issue #688 deliverable 1) is expected."""

    return CHECKOUT / "tools" / "fleet" / "pbcanary.py"


def _load_canary_driver():
    """Import the driver; never duplicate its submit-and-verify logic.

    ASSUMED crew-A entry shape (adapt here if the integrator reports drift):
    ``run_canary(generation=<name>) -> int``, where the int is the issue's
    verdict exit code (0 every leg verified, 1 a leg failed its contract,
    2 precondition refused). Anything else -- no ``run_canary`` attribute, a
    non-int return, an exception -- is a ``_CanaryPrecondition``: the
    generation is marked ``failed`` and publication exits nonzero, rather
    than inventing a verdict. ``SystemExit`` raised by the driver is honored
    as its verdict code (``None`` counts as 0, a string message as 1).
    """

    driver = _canary_driver_path()
    if not driver.is_file():
        raise _CanaryPrecondition(
            f"no canary driver at {driver}: crew A has not landed "
            "tools/fleet/pbcanary.py in this checkout"
        )
    spec = importlib.util.spec_from_file_location("pbcanary", driver)
    if spec is None or spec.loader is None:
        raise _CanaryPrecondition(
            f"cannot load canary driver at {driver}: no import spec"
        )
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except SystemExit:
        raise
    except Exception as exc:
        raise _CanaryPrecondition(
            f"canary driver at {driver} failed to import: {exc!r}"
        ) from exc
    entry = getattr(module, "run_canary", None)
    if entry is None:
        raise _CanaryPrecondition(
            f"canary driver at {driver} has no run_canary(generation=...) "
            "entry: assumed crew-A shape drifted; refusing to invent a verdict"
        )
    return entry


def _invoke_canary_driver(generation_name: str) -> tuple[int, str]:
    """Run the driver against the just-activated generation; return (code, detail)."""

    entry = _load_canary_driver()
    try:
        result = entry(generation=generation_name)
    except SystemExit as exc:
        code = exc.code
        if code is None:
            return 0, "driver exited without a code; counted as verified"
        if isinstance(code, int):
            return code, f"driver raised SystemExit({code})"
        return 1, f"driver refused: {code}"
    except _CanaryPrecondition:
        raise
    except Exception as exc:
        raise _CanaryPrecondition(
            f"canary driver raised instead of returning a verdict: {exc!r}"
        ) from exc
    if isinstance(result, bool) or not isinstance(result, int):
        raise _CanaryPrecondition(
            f"canary driver returned {result!r}: assumed contract is "
            "run_canary(generation=...) -> int exit code (0/1/2 per #688)"
        )
    detail = {
        0: "every leg executed and every receipt verified",
        1: "a leg failed its contract; the driver names the leg and the refusing check",
        2: "precondition refused: the canary did not test this generation",
    }.get(result, f"unrecognized driver exit code: {result}")
    return result, detail


def _canary_record_path(store: Path, generation_name: str) -> Path:
    """The rollout record: a sidecar beside the sealed generation.

    The generation directory itself is sealed read-only at publication, so
    the post-activation verdict cannot live inside it without breaking the
    append-only history. This sibling file is the only mutable rollout
    state, and ``supervise`` ignores store children that carry no
    ``RUNTIME_VERSION.json``, so it is never mistaken for a generation.
    """

    return store / f"{generation_name}.canary.json"


def _write_canary_record(
    path: Path, *, generation: str, commit: str, status: str,
    exit_code: int | None, detail: str,
) -> None:
    assert status in CANARY_STATUSES, status
    _write_receipt(path, {
        "schema": CANARY_RECORD_SCHEMA,
        "generation": generation,
        "commit": commit,
        "canary_status": status,
        "canary_exit": exit_code,
        "detail": detail,
        "recorded_unix": time.time(),
        "recorded_by": socket.gethostname(),
    })


def _run_rollout_canary(
    *, store: Path, generation_name: str, commit: str, enabled: bool,
) -> int:
    """Record the canary outcome for an activated generation; return the process exit.

    Exit 0 means the rollout record says ``verified`` (canary ran green) or
    ``not_run`` (gate off or skipped). Exit 1 means it says ``failed``. A
    failure marks the record and exits nonzero; it never rolls anything back
    and never touches admission -- campaign-primacy (issue #688 non-goals).
    """

    record = _canary_record_path(store, generation_name)
    if not enabled:
        reason = ("--no-canary" if CANARY_DEFAULT_ENABLED
                  else "gate default-OFF (phase 1); pass --canary to verify")
        _write_canary_record(record, generation=generation_name, commit=commit,
                             status="not_run", exit_code=None,
                             detail=f"canary skipped: {reason}")
        print(f"canary not_run for {generation_name}: {reason}")
        return 0
    _write_canary_record(record, generation=generation_name, commit=commit,
                         status="pending", exit_code=None,
                         detail="canary submitted against the activated generation")
    print(f"canary pending for {generation_name}; invoking the driver", flush=True)
    try:
        code, detail = _invoke_canary_driver(generation_name)
    except _CanaryPrecondition as exc:
        _write_canary_record(record, generation=generation_name, commit=commit,
                             status="failed", exit_code=None, detail=str(exc))
        print(f"canary failed for {generation_name}: {exc}", file=sys.stderr)
        return 1
    if code == 0:
        _write_canary_record(record, generation=generation_name, commit=commit,
                             status="verified", exit_code=code, detail=detail)
        print(f"canary verified for {generation_name}: {detail}")
        return 0
    _write_canary_record(record, generation=generation_name, commit=commit,
                         status="failed", exit_code=code, detail=detail)
    print(f"canary failed for {generation_name} (exit {code}): {detail}; "
          "activation stands, nothing was rolled back", file=sys.stderr)
    return 1


def _rollout_reason(rollout: str, reason: str | None) -> str | None:
    """Validate the declaration that decides whether generations may mix."""

    if rollout not in {"barrier", "rolling"}:
        raise SystemExit(f"unknown rollout mode: {rollout!r}")
    if rollout == "rolling":
        if not isinstance(reason, str) or not reason.strip():
            raise SystemExit(
                "--rollout rolling requires a nonblank --rollout-reason stating "
                "why this generation is safe to run beside the previous one"
            )
        return reason.strip()
    if reason is not None:
        raise SystemExit("--rollout-reason is only valid with --rollout rolling")
    return None


@contextmanager
def _publication_lock():
    """One publisher or recovery coordinator, across NFS clients and threads.

    This permanent POSIX lock is never removed or broken on elapsed time.
    Closing another descriptor for this inode would release a process's lock,
    so nested entry reuses the outer descriptor instead of opening it again.
    """
    global _PUBLISH_LOCK_DEPTH
    with _PUBLISH_MUTEX:
        if _PUBLISH_LOCK_DEPTH:
            _PUBLISH_LOCK_DEPTH += 1
            try:
                yield
            finally:
                _PUBLISH_LOCK_DEPTH -= 1
            return
        path = MIRROR.parent / ".publication.lock"
        try:
            MIRROR.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o644)
        except OSError as exc:
            raise SystemExit(f"cannot open publication lock {path}: {exc}; nothing activated") from exc
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise SystemExit(f"publication lock is not a regular file: {path}")
            try:
                fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise SystemExit("another publication or barrier coordinator holds the publication lock") from exc
            _PUBLISH_LOCK_DEPTH = 1
            try:
                yield
            finally:
                _PUBLISH_LOCK_DEPTH = 0
        finally:
            os.close(fd)


def _rollout_config():
    return {"runtime": str(MIRROR),
            "generation_store": str(MIRROR.parent / "runtime-generations"),
            "rollout_root": str(MIRROR.parent / "rollout")}


def _require_external_coordinator():
    # A production barrier waits for every admitted scope, including its own
    # caller. Stage/probe through PB, then drive only the control-plane
    # transition externally. Private qualification fleets remain admitted.
    if (MIRROR.parent.resolve() == Path("/mnt/shared/prismabuild-fleet").resolve()
            and re.search(r"prismabuild-job[0-9a-f]{32}\.slice",
                          Path("/proc/self/cgroup").read_text())):
        raise SystemExit("a live barrier cannot coordinate from its own admitted scope; "
                         "use --stage-only through PB, then activate or resume externally")


def _require_barrier_qualification() -> None:
    """Keep every public barrier mutation disabled until qualification is reviewed.

    Private source fixtures deliberately set the module constant false around
    their state-machine exercise.  No pathname is an authority boundary here:
    public publication, resume, and rollback all refuse by default everywhere.
    """
    if FINAL_BARRIER_QUALIFICATION_GUARD:
        raise SystemExit(
            "barrier activation remains qualification-guarded: source protocol "
            "is present, but no runtime generation was staged or activated"
        )


def _rollout_view(epoch=None):
    _coordinator_sha256()
    try:
        agent = _agent_definitions()
        view = agent.read_rollout(_rollout_config(), epoch)
        if view is not None:
            _assert_coordinator_identity(view, agent=agent)
        return view
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise SystemExit(f"rollout epoch evidence unavailable: {exc}; admission must remain held") from exc


def _assert_coordinator_identity(view, *, agent=None):
    """Bind retained decisions to the exact coordinator and marker semantics."""
    expected = view["intent"].get("coordinator_sha256")
    if expected != _coordinator_sha256():
        raise SystemExit("rollout coordinator identity differs from the armed epoch")
    try:
        expected_agent = view["intent"]["agent_sha256"]
        if agent is None:
            agent = _agent_definitions(expected_sha=expected_agent)
        if agent._pb_source_sha256 != expected_agent:
            raise SystemExit("rollout marker semantics identity differs from the armed updater hash")
        source = CHECKOUT / "tools" / "fleet" / "upgrade_client.py"
        if _sha256(source) != expected_agent:
            raise SystemExit("rollout marker semantics differ from the armed updater hash")
        if not hasattr(agent, "validate_marker"):
            raise SystemExit("rollout marker helper lacks validation semantics")
    except OSError as exc:
        raise SystemExit(f"rollout coordinator identity cannot be read: {exc}") from exc
    return agent


def _require_no_epoch():
    # Retain rolling/bootstrap use from minimal source trees before epochs
    # existed. An unavailable directory is never the same as an absent one.
    directory = MIRROR.parent / "rollout" / "epochs"
    try:
        directory.stat()
    except FileNotFoundError:
        return
    view = _rollout_view()
    if view is not None:
        raise SystemExit(f"rollout epoch {view['intent']['epoch']} remains active; "
                         "use --resume-barrier, not another publication")


def _barrier_generation(name):
    """Revalidate the immutable generation before trusting its rollout inputs."""
    if not isinstance(name, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", name) is None:
        raise SystemExit(f"invalid barrier generation: {name!r}")
    store = (MIRROR.parent / "runtime-generations").resolve(strict=True)
    root = store / name
    if root.resolve(strict=True) != root or root.stat().st_mode & 0o222:
        raise SystemExit(f"barrier generation is not sealed: {name}")
    try:
        receipt = json.loads((root / "RUNTIME_VERSION.json").read_text())
        if (receipt.get("schema") != "prismaquant.prismabuild.runtime_version.v1"
                or receipt.get("generation") != name
                or re.fullmatch(r"[0-9a-f]{40}", str(receipt.get("commit", ""))) is None
                or not isinstance(receipt.get("files"), dict)):
            raise ValueError("invalid generation receipt")
        for member, expected in receipt["files"].items():
            parts = Path(member).parts
            if not parts or Path(member).is_absolute() or any(p in (".", "..") for p in parts):
                raise ValueError("unsafe generation member path")
            path = root / member
            if (path.resolve(strict=True) != path or not path.is_file()
                    or path.stat().st_mode & 0o222 or _sha256(path) != expected):
                raise ValueError(f"unsealed or changed generation member: {member}")
        agent_path = "tools/upgrade_client.py"
        if agent_path not in receipt["files"]:
            raise ValueError("no updater in generation")
        if not re.search(rb"(?m)^CLIENT_UPGRADE_ROLLOUT_PROTOCOL[ \t]*=[ \t]*1[ \t]*$",
                         (root / agent_path).read_bytes()):
            raise ValueError("generation has no rollout-aware updater; a rolling bridge is required")
        return root, receipt
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise SystemExit(f"cannot qualify barrier generation {name}: {exc}") from exc


def _barrier_roster(generations):
    """Freeze the union of old/new boxes; a roster edit cannot drop a member.

    A box the *target* generation declares absent (``retired``/``offline``,
    #606) is excluded from the quorum instead: the union still freezes it, so
    a silent file edit cannot drop a member, but an explicit declared absence
    with its provenance is exactly how a member leaves.  An absent box that
    is still announcing is refused rather than skipped -- a live participant
    the quorum ignores is a split fleet, and the operator either stops it or
    un-declares the absence.  A group mixing absent and active names is the
    same ambiguity and refuses too.
    """
    groups = []
    boxes_by_generation = []
    for root, receipt in generations:
        member = "tools/fleet/fleet_boxes.json"
        if member not in receipt["files"]:
            raise SystemExit(f"barrier roster is absent from {root.name}")
        try:
            boxes = json.loads((root / member).read_text())["boxes"]
            if not isinstance(boxes, dict) or not boxes:
                raise ValueError("empty roster")
            for key, value in boxes.items():
                names = {key}
                if isinstance(value, dict) and value.get("_alias"):
                    names.add(value["_alias"])
                if any(not isinstance(n, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", n) is None for n in names):
                    raise ValueError("invalid roster name")
                try:
                    status, _ = fleet_roster.box_status(key, value)
                except fleet_roster.RosterPresenceError as exc:
                    raise ValueError(str(exc)) from exc
                overlap = [group for group in groups if group & names]
                for group in overlap:
                    names |= group
                    groups.remove(group)
                groups.append(names)
            boxes_by_generation.append(boxes)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise SystemExit(f"invalid barrier roster in {root.name}: {exc}") from exc
    # Offers only resolve spelling and reject undeclared live participants.
    # Their age is evaluated after the complete read, never used as quorum.
    offers = []
    directory = MIRROR.parent / "pb-queue/workers"
    try:
        for path in directory.iterdir():
            if path.name.startswith(".") or path.suffix != ".json":
                continue
            value = json.loads(path.read_text())
            if (not isinstance(value, dict) or value.get("schema") != "prismaquant.prismabuild.pool_offer.v1"
                    or path.name != str(value.get("host")) + ".json"):
                raise ValueError(f"invalid worker offer {path.name}")
            stamp = value.get("announced_unix")
            if type(stamp) not in (int, float) or not math.isfinite(stamp):
                raise ValueError(f"invalid worker offer time {path.name}")
            offers.append(value)
    except (OSError, ValueError, TypeError) as exc:
        raise SystemExit(f"cannot establish live barrier roster: {exc}") from exc
    now = time.time()
    live = {o["host"] for o in offers if -60 <= now - o["announced_unix"] <= 120}
    target_boxes = boxes_by_generation[-1]
    absent_names: set[str] = set()
    absent_detail: dict[str, str] = {}
    for key, value in target_boxes.items():
        status, detail = fleet_roster.box_status(key, value)
        if status in fleet_roster.ABSENT:
            names = {key}
            if isinstance(value, dict) and value.get("_alias"):
                names.add(value["_alias"])
            absent_names |= names
            absent_detail[key] = fleet_roster.describe_absent(key, detail)
    announced_absent = sorted(absent_names & live)
    if announced_absent:
        raise SystemExit(
            "barrier roster refuses: boxes declared absent are announcing: "
            f"{announced_absent}. Stop their loops or un-declare the absence: "
            + "; ".join(sorted(absent_detail.values()))
        )
    kept = []
    for group in groups:
        if group <= absent_names:
            continue
        if group & absent_names:
            raise SystemExit(
                f"barrier roster mixes absent and active names in one box: "
                f"{sorted(group)}; fix the roster's aliases and statuses")
        kept.append(group)
    groups = kept
    if not groups:
        raise SystemExit(
            "barrier roster is empty: every declared box is absent: "
            + "; ".join(sorted(absent_detail.values())))
    declared = set().union(*groups)
    if live - declared - absent_names:
        raise SystemExit(f"undeclared live barrier hosts: {sorted(live - declared - absent_names)}")
    canonical = []
    for group in groups:
        matches = live & group
        if len(matches) != 1:
            raise SystemExit(f"barrier roster requires exactly one live identity for {sorted(group)}; found {sorted(matches)}")
        canonical.append(matches.pop())
    return sorted(canonical)


def _quorum(view, phase):
    agent = _assert_coordinator_identity(view)
    observed, missing = {}, []
    for host in view["intent"]["roster"]:
        name = agent.marker_name(host, phase)
        value = view["markers"].get(name)
        if value is None:
            missing.append(host)
        else:
            agent.validate_marker(name, value, view["intent"], view["intent_sha256"])
            observed[host] = agent.digest(agent.canonical_json(value))
    return observed, missing


def _decision(view, phase, *, direction, observed, **extra):
    agent = _assert_coordinator_identity(view)
    generation = view["intent"]["to_generation" if direction == "forward" else "from_generation"]
    value = agent.make_marker(view["intent"], phase, generation=generation,
                              direction=direction, observed=observed,
                              decided_unix=time.time(),
                              decided_by=f"{socket.gethostname()}:{os.getuid()}", **extra)
    name = phase + ".json"
    agent.validate_marker(name, value, view["intent"], view["intent_sha256"])
    existing = view["markers"].get(name)
    if existing is not None:
        if any(existing.get(k) != v for k, v in value.items() if k not in ("decided_unix", "decided_by")):
            raise SystemExit(f"conflicting rollout decision: {name}")
        return
    _assert_coordinator_identity(view, agent=agent)
    if not agent.post_marker(MIRROR.parent / "rollout", f"epochs/{view['intent']['epoch']}/{name}",
                             agent.canonical_json(value)):
        raise SystemExit(f"rollout decision raced: {name}; re-read with --resume-barrier")


def _barrier_step(epoch, *, rollback_reason=None):
    """One recoverable step. No observation of elapsed time grants a swap."""
    view = _rollout_view(epoch)
    if view is None:
        raise SystemExit(f"rollout epoch disappeared: {epoch}")
    agent = _assert_coordinator_identity(view)
    intent, markers = view["intent"], view["markers"]
    source, target = intent["from_generation"], intent["to_generation"]
    live = view["live_generation"]
    if "terminal.json" in markers:
        return {"epoch": epoch, "state": markers["terminal.json"]["outcome"], "complete": True}
    drained, missing = _quorum(view, "drained")
    if rollback_reason is not None and "resume.json" in markers:
        raise SystemExit("rollback is refused after the fleet resume decision; use a new barrier")
    failures = [h for h in intent["roster"] if agent.marker_name(h, "failed") in markers]
    if rollback_reason is not None or (failures and "resume.json" not in markers):
        if missing:
            return {"epoch": epoch, "state": "draining", "missing": missing, "failed": failures, "complete": False}
        if "rollback.json" not in markers:
            _decision(view, "rollback", direction="rollback", observed=drained,
                      reason=rollback_reason or f"member rotation failed on {', '.join(failures)}")
            return {"epoch": epoch, "state": "rollback_declared", "complete": False}
    rollback = "rollback.json" in markers
    direction = "rollback" if rollback else "forward"
    expected = source if rollback else target
    if missing:
        if live != source:
            raise SystemExit("rollout symlink moved before the fleet drain quorum")
        return {"epoch": epoch, "state": "draining", "missing": missing, "complete": False}
    activation = "reverted" if rollback else "activated"
    if activation + ".json" not in markers:
        if live not in (source, target):
            raise SystemExit("unexpected third generation during rollout; all hosts remain held")
        root, _ = _barrier_generation(expected)
        _assert_coordinator_identity(_rollout_view(epoch))
        # The caller owns the publication lock. Re-read after potentially slow
        # member hashing so an external symlink change cannot become our swap.
        actual = MIRROR.resolve(strict=True).name
        if actual != live:
            raise SystemExit("runtime moved during rollout verification; admission remains held")
        if actual != expected:
            _activate(root, migrate_directory=False)
        if MIRROR.resolve(strict=True) != root:
            raise SystemExit("rollout activation readback mismatch; admission remains held")
        _decision(view, activation, direction=direction, observed=drained)
        return {"epoch": epoch, "state": activation, "complete": False}
    if live != expected:
        raise SystemExit("runtime differs from recorded rollout activation; admission remains held")
    rotated, missing = _quorum(view, "rolled-back" if rollback else "rotated")
    if "resume.json" not in markers:
        if missing:
            return {"epoch": epoch, "state": "rollback_rotating" if rollback else "rotating",
                    "missing": missing, "complete": False}
        _decision(view, "resume", direction=direction, observed=rotated)
        return {"epoch": epoch, "state": "resume_authorized", "complete": False}
    resumed, missing = _quorum(view, "resumed")
    if missing:
        return {"epoch": epoch, "state": "resuming", "missing": missing, "complete": False}
    outcome = "rolled_back" if rollback else "completed"
    _decision(view, "terminal", direction=direction, observed=resumed, outcome=outcome)
    return {"epoch": epoch, "state": outcome, "complete": True}


def _wait_barrier(epoch, *, wait_s=300, rollback_reason=None):
    _require_barrier_qualification()
    _require_external_coordinator()
    if not math.isfinite(wait_s) or wait_s < 0:
        raise SystemExit("--barrier-wait-s must be finite and nonnegative")
    deadline, previous = time.monotonic() + wait_s, None
    while True:
        result = _barrier_step(epoch, rollback_reason=rollback_reason)
        rendered = json.dumps(result, sort_keys=True)
        if rendered != previous:
            print(rendered, flush=True)
            previous = rendered
        if result["complete"]:
            return 0 if result["state"] == "completed" else 1
        if time.monotonic() >= deadline:
            print(f"barrier {epoch} remains pending; --resume-barrier {epoch} continues it. "
                  "No timeout releases admission.", flush=True)
            return 75
        time.sleep(min(2, max(0, deadline - time.monotonic())))


def _arm_barrier(name, *, wait_s=300):
    _require_barrier_qualification()
    _require_external_coordinator()
    _require_no_epoch()
    if not MIRROR.is_symlink():
        raise SystemExit("barrier requires an existing sealed runtime symlink")
    source = _barrier_generation(MIRROR.resolve(strict=True).name)
    target = _barrier_generation(name)
    if source[0] == target[0]:
        print(f"runtime already activates {name}; no rollout required")
        return 0
    member = "tools/upgrade_client.py"
    sha = target[1]["files"][member]
    coordinator_member = "tools/fleet/publish_runtime.py"
    coordinator_sha = target[1]["files"].get(coordinator_member)
    if (not isinstance(coordinator_sha, str)
            or source[1]["files"].get(coordinator_member) != coordinator_sha
            or coordinator_sha != _coordinator_sha256()):
        raise SystemExit("barrier requires the same executing coordinator in source and target; "
                         "converge a reviewed rolling bridge first")
    if source[1]["files"][member] != sha:
        raise SystemExit("barrier requires the same rollout-aware updater in both generations; "
                         "converge a reviewed rolling bridge first")
    roster = _barrier_roster((source, target))
    # Historical presence is only the inexpensive bootstrap check. Fresh
    # participant markers, durable holds and quorums grant actual movement.
    agent = _agent_definitions(expected_sha=sha)
    attested = _attested_agents(agent)
    missing = [host for host in roster if sha not in attested.get(host, set())]
    if missing:
        raise SystemExit(f"barrier updater bootstrap attestations missing on {missing}")
    epoch = uuid.uuid4().hex
    intent = {"schema": "prismabuild.rollout_barrier.intent.v1", "epoch": epoch,
              "from_generation": source[0].name, "to_generation": name,
              "roster": roster, "agent_sha256": sha,
              "coordinator_sha256": coordinator_sha, "drain_policy": "wait",
              "armed_unix": time.time(), "armed_by": f"{socket.gethostname()}:{os.getuid()}"}
    agent.validate_intent(intent, epoch=epoch)
    if MIRROR.resolve(strict=True) != source[0]:
        raise SystemExit("runtime moved while arming the barrier")
    _assert_coordinator_identity({"intent": intent}, agent=agent)
    if not agent.post_marker(MIRROR.parent / "rollout", f"epochs/{epoch}/intent.json",
                             agent.canonical_json(intent)):
        raise SystemExit("rollout epoch identity collided; nothing activated")
    print(f"armed barrier {epoch}: {source[0].name} -> {name}; hosts={roster}", flush=True)
    return _wait_barrier(epoch, wait_s=wait_s)


def _activate_existing(
    name: str, *, dry_run: bool, rollout: str = "barrier",
    rollout_reason: str | None = None, wait_s: float = 300,
) -> int:
    if dry_run:
        return _activate_existing_locked(name, dry_run=True, rollout=rollout,
                                         rollout_reason=rollout_reason, wait_s=wait_s)
    with _publication_lock():
        return _activate_existing_locked(name, dry_run=False, rollout=rollout,
                                         rollout_reason=rollout_reason, wait_s=wait_s)


def _activate_existing_locked(
    name: str, *, dry_run: bool, rollout: str = "barrier",
    rollout_reason: str | None = None, wait_s: float = 300,
) -> int:
    """Point the live runtime at a generation that already exists.

    Rollback's whole job.  A generation is immutable and already carries a
    receipt proving its bytes, so restoring one is a namespace operation and
    nothing else -- no copy, no re-hash, no dependence on what the checkout
    happens to contain now.  Publication never deletes a generation, which is
    what makes this possible at all.

    The name is validated rather than trusted: it must be a direct child of the
    generation store, it must not be a dot-name, and it must carry a receipt
    that reads.  A path that escapes the store, a staging tree, or a directory
    that is not a published generation is refused before ``repo`` is touched.
    """

    rollout_reason = _rollout_reason(rollout, rollout_reason)
    _require_no_epoch()
    if rollout == "barrier" and not dry_run:
        return _arm_barrier(name, wait_s=wait_s)
    store = MIRROR.parent / "runtime-generations"
    # A dot-name is never a generation, and one shape of it is dangerous.  A
    # publish stages at ``.<generation>.staging`` in this same store, writes
    # the receipt into it, and removes it on failure -- but that removal
    # reports an OSError and continues, so an interrupted publish can leave a
    # survivor that carries a receipt and passes every other check here.  Its
    # bytes were never sealed or probed.
    if "/" in name or name in ("", ".", "..") or name.startswith("."):
        raise SystemExit(
            f"not a generation name: {name!r}. A staging tree left behind by "
            "an interrupted publish is a dot-name and carries a receipt; it "
            "is not a generation."
        )
    generation = store / name
    if not (generation / "RUNTIME_VERSION.json").is_file():
        raise SystemExit(
            f"{generation} is not a published generation: no RUNTIME_VERSION.json"
        )
    # Rollback is the command that runs when everything else has failed, so a
    # short or damaged receipt has to be a sentence rather than a traceback.
    try:
        receipt = json.loads((generation / "RUNTIME_VERSION.json").read_text())
    except (OSError, ValueError) as exc:
        raise SystemExit(
            f"{generation}: receipt is not readable ({exc}); this is not a "
            "generation to point the live runtime at."
        ) from exc
    if not isinstance(receipt, dict):
        raise SystemExit(
            f"{generation}: receipt is not readable (not a JSON object); this "
            "is not a generation to point the live runtime at."
        )
    if rollout == "barrier":
        # A rollback publishes no manifest, so the value to prove the fleet
        # against is the one already recorded in the generation being restored.
        files = receipt.get("files")
        member = _agent_definitions().MEMBERS["upgrade_client.py"]
        agent_sha = files.get(member) if isinstance(files, dict) else None
        if not isinstance(agent_sha, str) or not agent_sha:
            raise SystemExit(
                f"{generation}: receipt records no sha256 for {member}, so a "
                "barrier activation cannot be proved against it."
            )
        _barrier_preflight(agent_sha, dry_run=dry_run)
    print(
        f"activating {name}: commit {str(receipt.get('commit', ''))[:12]}, "
        f"default transport {receipt.get('default_transport') or 'pool'}"
    )
    if rollout == "rolling":
        print(f"rollout rolling: {rollout_reason}")
    if dry_run:
        return 0
    _activate(generation, migrate_directory=False)
    print(f"activated {MIRROR} -> {generation}")
    return 0


def _agent_definitions(*, expected_sha=None):
    """The agent's own module, loaded from the checkout being published.

    The coordinator has to spell a marker name exactly the way the agent
    spelled it, and read the member key exactly as the agent reads it.  Two
    copies of either spelling agree right up until somebody edits one, so
    there is one copy and this reads it.  Loading under a private name keeps
    the checkout's module out of anybody else's import table.
    """

    source = CHECKOUT / "tools" / "fleet" / "upgrade_client.py"
    spec = importlib.util.spec_from_file_location(
        "_publish_runtime_upgrade_client", source
    )
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot read the client agent at {source}")
    module = importlib.util.module_from_spec(spec)
    # A preflight must not create __pycache__ in the checkout. Compile the
    # source bytes directly so the check neither writes nor trusts a cached
    # version of the definitions it is supposed to read from this source.
    try:
        raw = source.read_bytes()
        actual_sha = hashlib.sha256(raw).hexdigest()
        if expected_sha is not None and actual_sha != expected_sha:
            raise SystemExit("rollout marker semantics identity differs from the armed updater hash")
        code = compile(raw, str(source), "exec")
    except (OSError, SyntaxError) as exc:
        raise SystemExit(f"cannot read the client agent at {source}: {exc}") from exc
    exec(code, module.__dict__)
    # Bind the bytes actually executed, independently of later pathname reads.
    module._pb_source_sha256 = actual_sha
    return module


def _roster_entries() -> list[tuple[str, frozenset[str], object]]:
    """Every box the fleet declares, with its raw roster entry.

    ``_roster_boxes`` is the name-and-alias view the history check needs;
    this is the same walk with the entry attached, so presence can be
    validated from the same bytes rather than re-reading the file.
    """

    try:
        roster = json.loads((CHECKOUT / "tools" / "fleet" / "fleet_boxes.json").read_text())
        boxes = roster["boxes"]
        if not isinstance(boxes, dict) or not boxes:
            raise ValueError("no boxes declared")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise SystemExit(f"cannot read the fleet roster: {exc}") from exc
    declared = []
    for key in sorted(boxes):
        entry = boxes[key]
        names = {key}
        alias = entry.get("_alias") if isinstance(entry, dict) else None
        if isinstance(alias, str) and alias:
            names.add(alias)
        declared.append((key, frozenset(names), entry))
    return declared


def _roster_boxes() -> list[tuple[str, frozenset[str]]]:
    """Every box the fleet declares, with the names it may report itself as.

    A box is keyed here by the name its submissions use as a placement tag,
    and that is not always what ``gethostname`` returns on it: ``gx10-6b77``
    answers ``sparklina``.  The file records the second name as ``_alias``
    precisely because the two are the same box, so both are accepted.
    """

    return [(key, names) for key, names, _entry in _roster_entries()]


def _active_roster_boxes() -> tuple[list[tuple[str, frozenset[str]]], list[str]]:
    """The roster boxes a barrier must hear from, and the absent ones it skips.

    A box declared ``retired`` or ``offline`` in ``fleet_boxes.json`` (#606)
    is excluded from the attestation preflight and the barrier quorum: an
    absent box must not veto a publish for the boxes that are live.  The
    declaration needs its provenance (``status_reason``/``status_by``/
    ``status_unix``); an unknown status or a missing provenance refuses here,
    because an ambiguous presence is not an active box.
    """

    active = []
    absent = []
    for key, names, entry in _roster_entries():
        try:
            status, detail = fleet_roster.box_status(key, entry)
        except fleet_roster.RosterPresenceError as exc:
            raise SystemExit(f"cannot establish the fleet roster: {exc}") from exc
        if status in fleet_roster.ABSENT:
            absent.append(fleet_roster.describe_absent(key, detail))
        else:
            active.append((key, names))
    return active, absent


def _attested_agents(agent) -> dict[str, set[str]]:
    """Which agent versions each host has recorded in the rollout tree.

    A marker is counted only when it proves itself: its body has to parse,
    carry the agent's schema, and reproduce its own file name through the
    agent's own name function.  A name alone is a claim about a file; a name
    that matches the body it sits on is a claim the writer had to mean.
    Write-once records survive upgrades and downgrades; they do not establish
    the currently installed version or participation in a particular rollout.
    """

    directory = MIRROR.parent / agent.ROLLOUT_DIRNAME / agent.AGENTS_DIRNAME
    try:
        entries = sorted(entry.name for entry in directory.iterdir())
    except FileNotFoundError:
        return {}
    except OSError as exc:
        raise SystemExit(f"cannot read {directory}: {exc}") from exc
    attested: dict[str, set[str]] = {}
    for name in entries:
        parts = name.rsplit(".", 2)
        if len(parts) != 3 or parts[2] != "json":
            continue
        try:
            with (directory / name).open("rb") as stream:
                raw = stream.read(agent.MAX_MARKER + 1)
        except OSError:
            continue
        if len(raw) > agent.MAX_MARKER:
            continue
        try:
            body = json.loads(raw)
        except ValueError:
            continue
        if not isinstance(body, dict) or body.get("schema") != agent.ATTESTATION_SCHEMA:
            continue
        host = body.get("host")
        client = body.get("client_sha256")
        if not isinstance(host, str) or not isinstance(client, str):
            continue
        if agent.attestation_name(host, client) != name:
            continue
        attested.setdefault(host, set()).add(client)
    return attested


def _require_attested_fleet(agent_sha: str) -> None:
    """Require historical attestations for the target agent on every live box.

    This is only a bootstrap prerequisite. Matching records can remain after
    every host has moved to another version. A barrier must additionally prove
    current participation in its own epoch, then its drain and rotation quorums.

    Boxes declared absent (``retired``/``offline``) in the roster are not
    required to have posted: an offline box must not veto a publish for the
    boxes that are live (#606).  Their exclusion is said out loud on success
    so a stale retirement cannot pass silently.
    """

    agent = _agent_definitions()
    attested = _attested_agents(agent)
    active, absent = _active_roster_boxes()
    missing = []
    for key, names in active:
        held = set().union(*(attested.get(name, set()) for name in names))
        if agent_sha in held:
            continue
        spelling = key if len(names) == 1 else f"{key} ({'/'.join(sorted(names))})"
        if not held:
            # A box that has posted nothing at all is more often a box with no
            # agent installed than a box one tick behind, and rolling does not
            # install one.  Say so here rather than let the advice below be
            # read as covering a case it does not.
            missing.append(
                f"  {spelling}: has posted no attestation. A box with no "
                "client upgrade agent installed cannot hold a barrier at all; "
                "see docs/client_upgrade.md."
            )
        else:
            posted = ", ".join(sorted(short[:12] for short in held))
            missing.append(f"  {spelling}: posted versions {posted}")
    if missing:
        raise SystemExit(
            "refusing barrier attestation preflight: not every box has posted "
            f"the agent version this generation requires ({agent_sha[:12]}).\n"
            + "\n".join(missing)
            + "\nIf a reviewed compatibility assessment permits a normal rolling "
            "publication, pass --rollout rolling with its nonblank "
            "--rollout-reason, let each box converge and post its attestation, then "
            "repeat --rollout barrier --dry-run. Historical attestations do not prove "
            "current participation."
        )
    for line in absent:
        print(f"barrier preflight: skipping absent box: {line}", flush=True)


def _barrier_preflight(agent_sha: str, *, dry_run: bool) -> None:
    """Check bootstrap compatibility; epoch quorums grant activation later."""

    if not dry_run:
        if not MIRROR.is_symlink():
            raise SystemExit("barrier requires an existing sealed runtime symlink")
        _, receipt = _barrier_generation(MIRROR.resolve(strict=True).name)
        if receipt["files"]["tools/upgrade_client.py"] != agent_sha:
            raise SystemExit("barrier updater differs from the live generation; "
                             "converge a reviewed rolling bridge first")
    _require_attested_fleet(agent_sha)
    print(
        "barrier preflight: historical attestations match; current participation, "
        "drain and rotation have not been proved. Only epoch quorums authorize activation."
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--allow-dirty", action="store_true",
                    help="publish a tree with uncommitted changes")
    ap.add_argument(
        "--migrate-directory", action="store_true",
        help="perform the one-time fail-closed handoff from the legacy live directory",
    )
    ap.add_argument(
        "--default-transport", choices=("pool", "slurm"), default=None,
        help="the transport every pbrun and producer running THIS generation "
             "uses when nothing says otherwise; recorded in the receipt and "
             "read by fleet_submit.default_transport.  Omitted means the pull "
             "queue, which is what every generation published before the "
             "SLURM cutover means too.",
    )
    ap.add_argument(
        "--activate-generation", metavar="NAME", default=None,
        help="point the live runtime at an existing generation instead of "
             "publishing a new one; this is what rollback does, and it is "
             "deliberately not a re-publish -- the previous generation's bytes "
             "and receipt are already proved and a rebuild from a moved "
             "checkout would not be the same thing.",
    )
    ap.add_argument(
        "--rollout", choices=("rolling", "barrier"), default="barrier",
        help="barrier drains and rotates the fleet before admission resumes; "
             "rolling requires a stated mixed-generation-safety reason. "
             "Barrier --dry-run checks bootstrap history only.",
    )
    ap.add_argument("--stage-only", action="store_true",
                    help="seal and import-probe a generation without arming or activating it; "
                         "use inside PB, then activate with the external control-plane coordinator")
    ap.add_argument("--resume-barrier", metavar="EPOCH",
                    help="continue a retained epoch without creating a new publication")
    ap.add_argument("--rollback-barrier", metavar="EPOCH",
                    help="declare rollback to this epoch's source before its resume decision")
    ap.add_argument("--rollback-reason", metavar="TEXT",
                    help="required reason for --rollback-barrier")
    ap.add_argument("--barrier-wait-s", type=float, default=300,
                    help="how long the coordinator waits; expiry returns 75 and never releases admission")
    ap.add_argument(
        "--rollout-reason", default=None,
        help="required nonblank mixed-generation-safety reason for --rollout rolling; "
             "recorded in a newly published generation receipt",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="report what would happen and write nothing.  A publish still "
             "resolves the commit and still refuses a dirty tree, then lists "
             "the files it would publish and stops before the generation "
             "store is created.  With --activate-generation the name and its "
             "receipt are validated and the target is printed, but the live "
             "runtime is not repointed.",
    )
    ap.add_argument(
        "--canary", action="store_true",
        help="after activation, submit the fleet canary (issue #688) resolving "
             "against the new generation and record verified|failed in its "
             "rollout record; a failed canary marks and exits nonzero without "
             "rolling back. Opt-in while the gate is default-OFF (phase 1).",
    )
    ap.add_argument(
        "--no-canary", action="store_true",
        help="skip the fleet canary and record not_run in the rollout record; "
             "the escape hatch once the gate flips default-ON (phase 2).",
    )
    args = ap.parse_args()
    if not math.isfinite(args.barrier_wait_s) or args.barrier_wait_s < 0:
        ap.error("--barrier-wait-s must be finite and nonnegative")
    recovery = args.resume_barrier or args.rollback_barrier
    if args.resume_barrier and args.rollback_barrier:
        ap.error("choose one of --resume-barrier and --rollback-barrier")
    if recovery and (args.activate_generation or args.stage_only or args.allow_dirty
                     or args.migrate_directory or args.default_transport
                     or args.rollout != "barrier" or args.rollout_reason
                     or args.canary or args.no_canary):
        ap.error("barrier recovery cannot be combined with publication options")
    if args.rollback_barrier and not (args.rollback_reason and args.rollback_reason.strip()):
        ap.error("--rollback-barrier requires a nonblank --rollback-reason")
    if args.rollback_reason is not None and not args.rollback_barrier:
        ap.error("--rollback-reason requires --rollback-barrier")
    if args.canary and args.no_canary:
        ap.error("--canary and --no-canary conflict")
    if args.stage_only and (args.canary or args.no_canary):
        ap.error("the canary verifies a live generation; --stage-only activates nothing")
    if args.stage_only and args.activate_generation:
        ap.error("--stage-only cannot activate an existing generation")
    if args.dry_run:
        return _run_publication(args)
    with _publication_lock():
        return _run_publication(args)


def _run_publication(args) -> int:
    rollout_reason = _rollout_reason(args.rollout, args.rollout_reason)
    recovery = args.resume_barrier or args.rollback_barrier
    # Refuse before reading/staging a candidate generation.  The recovery
    # branch would otherwise reach `_wait_barrier`, which can call the pointer
    # mutation through `_barrier_step`.
    if args.rollout == "barrier" and not args.dry_run and not args.stage_only:
        _require_barrier_qualification()
    if recovery:
        if args.dry_run:
            view = _rollout_view(recovery)
            print(json.dumps(view, sort_keys=True))
            return 0
        return _wait_barrier(recovery, wait_s=args.barrier_wait_s,
                             rollback_reason=args.rollback_reason)

    _require_no_epoch()

    if args.activate_generation is not None:
        if args.canary or args.no_canary:
            raise SystemExit(
                "the rollout canary runs only on fresh publication; rollback "
                "(--activate-generation) leaves existing rollout records untouched"
            )
        return _activate_existing(
            args.activate_generation, dry_run=args.dry_run, rollout=args.rollout,
            rollout_reason=rollout_reason, wait_s=args.barrier_wait_s,
        )

    # Identity is established before MIRROR is even enumerated, much less
    # touched.  Failure here is a refusal, never an empty field in a receipt.
    commit = _commit_identity()
    dirty = _working_tree_dirty()
    if dirty and not args.allow_dirty:
        raise SystemExit(
            "refusing to publish a dirty tree: the receipt would name a commit "
            "whose bytes are not the bytes published.  Commit, or pass "
            "--allow-dirty and accept that the version is only approximate."
        )

    published = _publication_manifest()
    # --canary forces the gate on, --no-canary forces it off (the two together
    # are refused in main()); otherwise the versioned default decides.
    canary_enabled = bool(args.canary or (CANARY_DEFAULT_ENABLED and not args.no_canary))
    # Read before a single byte is staged: a publisher that cannot say
    # which of its members are programs refuses here, not halfway
    # through a tree it then has to clean up.
    index_modes = _git_index_modes()
    if args.rollout == "barrier" and not args.stage_only:
        member = _agent_definitions().MEMBERS["upgrade_client.py"]
        agent_sha = published.get(member)
        if not isinstance(agent_sha, str) or not agent_sha:
            raise SystemExit(
                f"this publication carries no {member}, so a barrier rollout "
                "has nothing to prove the fleet against."
            )
        _barrier_preflight(agent_sha, dry_run=args.dry_run)

    if canary_enabled and not _canary_driver_path().is_file():
        raise SystemExit(
            f"canary requested but no driver at {_canary_driver_path()}: "
            "nothing was published and the live runtime still points where it did."
        )
    if args.rollout == "rolling":
        print(f"rollout rolling: {rollout_reason}")
    print(f"publishing {len(published)} files from {commit[:12]}"
          f"{' (dirty)' if dirty else ''} to {MIRROR}")
    if args.dry_run:
        print(f"canary: {'enabled' if canary_enabled else 'disabled'} "
              f"(gate default-{'ON' if CANARY_DEFAULT_ENABLED else 'OFF'})")
        for name in sorted(published):
            print(f"  {name}")
        return 0

    store = MIRROR.parent / "runtime-generations"
    nonce = uuid.uuid4().hex[:12]
    generation_name = f"{commit[:12]}-{int(time.time())}-{nonce}"
    stage = store / f".{generation_name}.staging"
    generation = store / generation_name
    # The same refusal shape as the dirty-tree refusal above, and for the same
    # reason: these two directories are the first bytes written, so a store
    # this user cannot write is a fact to state before "publishing N files"
    # rather than a PermissionError traceback after it.  The live runtime is
    # untouched either way.
    try:
        store.mkdir(parents=True, exist_ok=True)
        stage.mkdir()
    except OSError as exc:
        raise SystemExit(
            f"cannot write the generation store {store}: {exc}.  Nothing was "
            "published and the live runtime still points where it did."
        ) from exc
    activated = False
    try:
        for name, expected in sorted(published.items()):
            source = _source_for(name)
            target = stage / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
            # copy2 preserves the source's mode, which is the publishing
            # worktree's umask rather than anything about the file.  Say what
            # the generation carries instead of inheriting an accident.
            os.chmod(target, _published_mode(source, index_modes))
            _fsync_file(target)
            actual = _sha256(target)
            if actual != expected:
                raise SystemExit(
                    f"copied runtime member changed at {name}: expected "
                    f"{expected}, staged {actual}"
                )

        # A checkout can advance while a slow NFS copy is in progress.  Hash
        # the complete source set again and re-prove both Git facts before a
        # receipt names the staged bytes.
        if _commit_identity() != commit or _working_tree_dirty() != dirty:
            raise SystemExit(
                "runtime checkout identity moved while publication was staged"
            )
        if _publication_manifest() != published:
            raise SystemExit(
                "runtime source bytes moved while publication was staged"
            )

        receipt: dict[str, object] = {
            "schema": "prismaquant.prismabuild.runtime_version.v1",
            "commit": commit,
            "rollout": args.rollout,
            **({"rollout_reason": rollout_reason}
               if args.rollout == "rolling" else {}),
            # Optional, and absent means the pull queue: every generation
            # published before the SLURM cutover has no such field and must
            # keep behaving as it did.
            **({"default_transport": args.default_transport}
               if args.default_transport else {}),
            "dirty": dirty,
            "generation": generation_name,
            "published_unix": time.time(),
            "published_by": socket.gethostname(),
            "files": published,
        }
        _write_receipt(stage / "RUNTIME_VERSION.json", receipt)
        _probe(stage)
        _seal_generation(stage)
        _fsync_directory(stage)
        os.replace(stage, generation)
        _fsync_directory(store)
        if args.stage_only:
            print(json.dumps({"state": "staged", "generation": generation_name,
                              "path": str(generation), "activated": False}, sort_keys=True))
            return 0
        if args.rollout == "barrier":
            barrier_exit = _arm_barrier(generation_name, wait_s=args.barrier_wait_s)
            if barrier_exit != 0:
                # Rolled back or still pending: the rollout has no ending yet,
                # so there is no rollout record to write. The resume path owns it.
                return barrier_exit
            return _run_rollout_canary(store=store, generation_name=generation_name,
                                       commit=commit, enabled=canary_enabled)
        legacy = _activate(
            generation, migrate_directory=args.migrate_directory
        )
        activated = True
        print(f"activated {MIRROR} -> {generation}")
        if legacy is not None:
            print(f"retained previous directory runtime at {legacy}")
        return _run_rollout_canary(store=store, generation_name=generation_name,
                                   commit=commit, enabled=canary_enabled)
    finally:
        if not activated and stage.exists():
            _remove_staging_tree(stage)


if __name__ == "__main__":
    raise SystemExit(main())

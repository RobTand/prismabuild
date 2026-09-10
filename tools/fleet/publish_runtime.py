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
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import socket
import stat
import subprocess
import sys
import time
import uuid

CHECKOUT = Path(__file__).resolve().parents[2]
MIRROR = Path("/mnt/shared/prismabuild-fleet/repo")
#: Published as ``tools/<name>`` *and* ``tools/fleet/<name>``.
FLEET_SCRIPTS = (
    "dispatch_tessera_model.py",
    "docker", "pbrun.py", "pbtest.py", "require_pool.py", "worker_loop.py", "worker.py",
    "render_identity.py", "seal_and_publish.py", "tessera_status.py",
    "dispatch_tessera_shards.py", "dispatch_tessera_ladder.py",
    "publish_runtime.py", "pool_reset.py", "runtime_paths.py", "supervise.py",
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
    # Per-job containment clients and root-installed authority sources travel
    # with the generation; installation copies privileged code to root-owned
    # storage rather than executing it from this shared runtime.
    "resource_exec.py", "resource_broker.py", "resource_payload.py",
    "qualify_resource_scope.py", "install_resource_broker.sh",
    "upgrade_client.py", "install_client_upgrader.sh",
)
#: Fleet tools deliberately left out of the generation, each with the reason.
#: Runtime tools travel; qualification harnesses use submitted checkouts.
#: The tuple exists so that leaving one out is a decision
#: somebody wrote down rather than an omission nobody noticed.
EXCLUDED: tuple[tuple[str, str], ...] = (
    ("qualify_claim_recovery.py",
     "paired queue-recovery qualification actors run from an isolated "
     "checkout through pbcampaign against a fresh private queue root; "
     "not an operator command for a box without a checkout"),
    ("admission_shared_io.py",
     "a measurement harness for the admission critical section, run from a "
     "checkout through pbrun against a private queue root; a box with no "
     "checkout has no reason to run it and it must never be pointed at the "
     "live queue"),
)

#: Not code, but read by published code: the supervisor on each box reads the
#: fleet's declared shape from here, so a runtime published without it starts
#: no workers at all.
FLEET_DATA = ("fleet_boxes.json",)

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
PUBLISHED_DIRECTORY_MODE = 0o555


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


def _activate_existing(name: str, *, dry_run: bool, rollout: str = "rolling") -> int:
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
    if dry_run:
        return 0
    _activate(generation, migrate_directory=False)
    print(f"activated {MIRROR} -> {generation}")
    return 0


def _agent_definitions():
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
    spec.loader.exec_module(module)
    return module


def _roster_boxes() -> list[tuple[str, frozenset[str]]]:
    """Every box the fleet declares, with the names it may report itself as.

    A box is keyed here by the name its submissions use as a placement tag,
    and that is not always what ``gethostname`` returns on it: ``gx10-6b77``
    answers ``sparklina``.  The file records the second name as ``_alias``
    precisely because the two are the same box, so both are accepted.
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
        names = {key}
        alias = boxes[key].get("_alias") if isinstance(boxes[key], dict) else None
        if isinstance(alias, str) and alias:
            names.add(alias)
        declared.append((key, frozenset(names)))
    return declared


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
    """Require historical attestations for the target agent on every box.

    This is only a bootstrap prerequisite. Matching records can remain after
    every host has moved to another version. A barrier must additionally prove
    current participation in its own epoch, then its drain and rotation quorums.
    """

    agent = _agent_definitions()
    attested = _attested_agents(agent)
    missing = []
    for key, names in _roster_boxes():
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
    if not missing:
        return
    raise SystemExit(
        "refusing barrier attestation preflight: not every box has posted "
        f"the agent version this generation requires ({agent_sha[:12]}).\n"
        + "\n".join(missing)
        + "\nPublish this generation with --rollout rolling, let each box "
        "converge and post its attestation, then repeat --rollout barrier "
        "--dry-run. Historical attestations do not prove current participation."
    )


def _barrier_preflight(agent_sha: str, *, dry_run: bool) -> None:
    """Expose the history check without granting it activation authority."""

    _require_attested_fleet(agent_sha)
    if not dry_run:
        raise SystemExit(
            "barrier activation is not implemented: historical attestations "
            "cannot prove current participation, a fleet drain or rotation. "
            "Use --rollout barrier --dry-run for the attestation preflight; "
            "issue #458 tracks the required epoch protocol. Nothing was staged "
            "or activated."
        )
    print(
        "barrier preflight: historical attestations match; current participation, "
        "drain and rotation have not been proved. Barrier activation is unavailable."
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
        "--rollout", choices=("rolling", "barrier"), default="rolling",
        help="rolling uses independent host convergence (the default). "
             "barrier requires --dry-run and checks historical agent "
             "attestations only; it does not prove current participation. "
             "Actual barrier publication and activation are refused until "
             "the fleet epoch protocol is implemented.",
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
    args = ap.parse_args()

    if args.activate_generation is not None:
        return _activate_existing(
            args.activate_generation, dry_run=args.dry_run, rollout=args.rollout
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
    # Read before a single byte is staged: a publisher that cannot say
    # which of its members are programs refuses here, not halfway
    # through a tree it then has to clean up.
    index_modes = _git_index_modes()
    if args.rollout == "barrier":
        member = _agent_definitions().MEMBERS["upgrade_client.py"]
        agent_sha = published.get(member)
        if not isinstance(agent_sha, str) or not agent_sha:
            raise SystemExit(
                f"this publication carries no {member}, so a barrier rollout "
                "has nothing to prove the fleet against."
            )
        _barrier_preflight(agent_sha, dry_run=args.dry_run)

    print(f"publishing {len(published)} files from {commit[:12]}"
          f"{' (dirty)' if dirty else ''} to {MIRROR}")
    if args.dry_run:
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
        legacy = _activate(
            generation, migrate_directory=args.migrate_directory
        )
        activated = True
        print(f"activated {MIRROR} -> {generation}")
        if legacy is not None:
            print(f"retained previous directory runtime at {legacy}")
        return 0
    finally:
        if not activated and stage.exists():
            _remove_staging_tree(stage)


if __name__ == "__main__":
    raise SystemExit(main())

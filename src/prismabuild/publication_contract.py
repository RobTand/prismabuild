"""Authoritative generation membership and member-to-source mapping.

Both publication and release identity consume this contract, including historical
Git inventories (not only files that happen to survive in a checkout).
"""
from pathlib import Path, PurePosixPath
from .digest_primitives import raw_sha256

FLEET_SCRIPTS = (
    "docker", "pbrun.py", "pbtest.py", "pbtest_outcomes.py", "pbtest_pins.py", "require_pool.py", "worker_loop.py", "worker.py",
    "seal_and_publish.py",
    "publish_runtime.py", "pool_reset.py", "supervise.py",
    "migrate_role_logs.py",
    # The supervisor spawns prewarm_loop.py on a box declaring the storage
    # role, by published path like every other child. A generation without it
    # makes that role a log line saying the script is not there (#487).
    "prewarm_loop.py",
    # ...and tier_loop.py on a box declaring the tiers role (#583): it
    # mints and announces the box's discovered storage tiers, and a role
    # script that does not travel cannot be spawned.
    "tier_loop.py", "pbresident.py", "local_tier_loop.py", "local_resident.py",
    # ...and stage_move.py, which is not a role script at all: it is the
    # movement node itself, exec'd by an admitted action on the storage box
    # (#583).  A generation without it publishes movers nothing can run.
    "stage_move.py",
    # Source-host PRECOMMIT local-spool exports are ordinary admitted actions.
    # Their payload must travel with the package that seals their command.
    "produced_export.py",
    # ...and stage_release.py beside it, the egress node that deletes a staged
    # range and returns its tier tokens.  The tier loop also imports it for the
    # orphan sweep, so a generation without it leaves a withdrawn consumer's
    # movers holding the stage with nothing able to take it back.
    "stage_release.py",
    # ...and stage_reclaim.py, the operator command that moves digest-proven
    # source-mark-only stage copies into quarantine and restores them (#1636).
    # The stage lives on dl380g10, which has no checkout, so a generation
    # without it leaves that command with no place to run.
    "stage_reclaim.py",
    # ...and ram_promote.py, the ram tier's movement node (#640): it copies a
    # landed stage range into the tmpfs and files the fragment that carries
    # the epoch.  A generation without it leaves the ram window publishing
    # promotions nothing can run.
    "ram_promote.py",
    # Root operator provisioning is separate from publication and discovery.
    "install_ram_mount_unit.py",
    # Qualified scratch profiles bind this producer's bytes in the generation.
    "local_scratch_profile.py",
    # The retained SLURM lane job entry. Submission is a package owner.
    "slurm_job.py",
    # The commands an operator runs on a box that has no checkout.  The
    # published pool_reset.py:582 tells them to run ``pbwait.py <key>``, and
    # neither dl380g10 nor sparklina has a checkout to run it from.
    # runtime_process_census.py is here for the same reason and is only
    # useful there: it reads /proc on the box it runs on.
    "pbstatus.py", "pbmetrics.py", "pbwait.py", "pbcampaign.py", "runtime_process_census.py",
    # pbsnapshot.py runs inside an action: a client that hashes its own
    # source asks it which files in the pbrun checkout PrismaBuild generated.
    # It has to be the executing generation's copy, found by published path.
    "pbsnapshot.py",
    # pbmergeq.py is a client a repository's merge flow runs from a box with
    # no PB checkout; it imports pbtest.py and pbtest_outcomes.py from its own
    # directory, so it must be the same generation's copy (#1417).
    "pbmergeq.py",
    # pbmcp.py is the same case as pbstatus.py and then some: an agent
    # registers it by absolute published path so that every session starts on
    # the current generation, and the boxes it is registered from are exactly
    # the ones with no checkout.
    "pbmcp.py",
    # retire_worker.py belongs to the same set for a sharper reason: the name
    # it retires is a name whose box stopped answering, and the operator who
    # notices is on whichever box is holding the mount.
    "retire_worker.py",
    # The residency-plan backfill (#1041, published by #1381) is the same
    # operator's second cleanup: it reaps plans whose consumer concluded
    # before PoolQueue.finish started retiring them at the terminal write,
    # and the queue it names lives on the shared mount rather than in any
    # checkout.  It lists without --apply and archives only when no child of
    # the plan is still ready, claimed or live.
    "retire_residency_plans.py",
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
    # pbgang.py submits a gang (#1517) from a box that has no checkout; a tool
    # missing from this list is a tool no box can run, so the gang canary could
    # not start from a published generation.
    "pbgang.py",
    # pbrun imports pbevidence unconditionally (#1598).  A published script that
    # imports a tools module the publication lacks makes the publisher refuse
    # every generation, so the tool ships even for commands that never use
    # --target-evidence.  It also runs as a normal action on a worker of the
    # class, where it prints the evidence packet.
    "pbevidence.py",
    # Explicit plan-first banked-checkout recovery runs on the owning host.
    "pbrecover_checkout.py",
    # The reconcile, for the same reason: the endings it files are the ones
    # no waiter asked for, and the operator who notices they are missing is
    # on whichever box has the shared mount rather than the checkout.
    "pbsweep.py",
    # Local JOIN/RESIGN and the worker-loop reconciliation hook must work
    # on registered hosts with only the published generation available.
    "fleet_membership.py",
    # Per-job containment clients and root-installed authority sources travel
    # with the generation; installation copies privileged code to root-owned
    # storage rather than executing it from this shared runtime.
    "resource_exec.py", "resource_broker.py", "resource_payload.py",
    "qualify_resource_scope.py", "install_resource_broker.sh",
    # The host-local check an operator runs after provisioning the broker: it
    # reads the installed needrestart main configuration alone to prove the
    # exact-service deferral survived installation (#1378), and the box that
    # needs that proof is the box whose running broker cannot be disturbed by
    # materializing a checkout.
    "qualify_needrestart_broker_deferral.py",
    "upgrade_client.py", "bootstrap_client.py", "install_client_upgrader.sh", "install_supervisor_unit.sh",
    # The one-time root enrollment of a host in automatic movement publication
    # (#1659).  An administrator copies it out of the live generation, as the
    # client upgrader's installer; a generation without it leaves no host able
    # to enroll.  ``runtime_publication.py`` it installs is a package module.
    "install_movement_publisher.sh",
)
# The retained proxy verifies the same stdlib layout helper beside itself.
STANDALONE_PACKAGE_SOURCES = {
    "runtime_paths.py": "src/prismabuild/runtime_paths.py",
}


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


FLEET_DATA = ("fleet_boxes.json", "ram_tier_policy.json", "local_tier_policy.json",
              "50-prismabuild-resource-broker.conf")

def publication_sources(paths, *, fleet_scripts=FLEET_SCRIPTS, fleet_data=FLEET_DATA):
    paths = set(paths)
    members = {}
    def add(member, source):
        if source in paths:
            members[member] = source
    for source in sorted(paths):
        path = PurePosixPath(source)
        if path.parent == PurePosixPath('src/prismabuild') and path.suffix == '.py':
            add(source, source)
        if (source in ('README.md', 'AGENTS.md', 'worker-expansion-design.md',
                       'skills/prismabuild/SKILL.md', '.githooks/pre-push')
                or (source.startswith('docs/') and path.suffix in ('.md', '.json'))
                or (path.parent == PurePosixPath('tests') and path.suffix == '.py')):
            add(source, source)
    for name in ('generation_receipt.py', 'digest_primitives.py'):
        add('tools/' + name, 'src/prismabuild/' + name)
    for name, source in STANDALONE_PACKAGE_SOURCES.items():
        for prefix in ('tools/', 'tools/fleet/'):
            add(prefix + name, source)
    for name in fleet_scripts:
        source = 'tools/fleet/' + name
        if source not in paths:
            source = 'tools/' + name
        for prefix in ('tools/', 'tools/fleet/'):
            add(prefix + name, source)
    for name in fleet_data:
        for prefix in ('tools/', 'tools/fleet/'):
            add(prefix + name, 'tools/fleet/' + name)
    for name in TOP_LEVEL_SCRIPTS:
        add('tools/' + name, 'tools/' + name)
    for directory, names in (('observability', OBSERVABILITY_FILES), ('storage', STORAGE_FILES)):
        for name in names:
            source = 'fleet/' + directory + '/' + name
            add(source, source)
    return members


def checkout_sources(checkout, **contract):
    checkout = Path(checkout)
    return publication_sources((p.relative_to(checkout).as_posix()
        for base in ('src/prismabuild', 'tools', 'docs', 'tests', 'skills', '.githooks', 'fleet')
        for p in (checkout / base).rglob('*') if p.is_file()), **contract) | {
            name: name for name in ('README.md', 'AGENTS.md', 'worker-expansion-design.md')
            if (checkout / name).is_file()}


def publication_source(checkout, member):
    """Resolve one member without recursively rescanning the checkout per file."""
    leaf = PurePosixPath(member).name
    candidates = {member, 'tools/' + leaf, 'tools/fleet/' + leaf,
                  'src/prismabuild/' + leaf}
    mapping = publication_sources(name for name in candidates
                                  if (Path(checkout) / name).is_file())
    return Path(checkout) / mapping.get(member, member)


def publication_manifest(checkout, **contract):
    return {member: raw_sha256((Path(checkout) / source).read_bytes())
            for member, source in checkout_sources(checkout, **contract).items()}

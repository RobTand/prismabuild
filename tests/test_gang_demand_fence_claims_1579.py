"""The demand-based gang fence on real claims (#1579).

The two-host fixture of ``test_gang_reservation_1517``: real queue, ledgers,
census and controllers; only the clock and the sampler are controlled.  A gang
member is a whole-box row (cpu 2 or 20, gpu 1, mem 100 on a 20/1/120 host).
The roles ``returns_capacity`` and ``serves_residency`` are assigned by
``publish`` from the sealed definition of a movement node and refused in any
submitted action, so these tests publish real sealed movement nodes.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest
from test_gang_reservation_1517 import HOSTS, _busy_both, gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401

from prismabuild import _gang, _measurement_reservation as reservation, core as pb, pool

REASONS = ("deferred_for_gang_reservation",)
from test_gang_residency_members import GIB, MANIFEST, STAGE_KIND, TIER  # noqa: E402

RANGE = {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER, "manifest_sha256": MANIFEST,
         "manifest_bytes": 6 * GIB, "range_start_bytes": 0, "range_end_bytes": 2 * GIB}
TOOLS = "/mnt/shared/prismabuild-fleet/repo/tools"


SCRIPTS = ("stage_move.py", "ram_promote.py", "stage_release.py", "produced_export.py", "local_resident.py")


@pytest.fixture()
def store(tmp_path, monkeypatch):
    """A retained-generation store holding one published generation of the movement scripts.

    Sealed like a real one: a direct child of the store, no write bits, a receipt naming it with a
    40-hex commit and the sha256 of every file under ``tools/`` and ``tools/fleet/``.
    """
    from prismabuild import resource_scope
    root = tmp_path / "runtime-generations"
    generation = root / ("a" * 12 + "-1791311474-" + "b" * 12)
    files = {}
    for sub in ("tools", "tools/fleet"):
        (generation / sub).mkdir(parents=True)
        for name in SCRIPTS:
            path = generation / sub / name
            path.write_text(f"# {name}\n")
            files[f"{sub}/{name}"] = hashlib.sha256(path.read_bytes()).hexdigest()
            path.chmod(0o444)
    (generation / "RUNTIME_VERSION.json").write_text(json.dumps({
        "schema": resource_scope.RUNTIME_RECEIPT_SCHEMA, "generation": generation.name,
        "commit": "c" * 40, "files": files}))
    (generation / "RUNTIME_VERSION.json").chmod(0o444)
    for sub in ("tools/fleet", "tools"):
        (generation / sub).chmod(0o555)
    generation.chmod(0o555)
    monkeypatch.setattr(resource_scope, "RETAINED_GENERATION_STORE", root)
    resource_scope._RECEIPT_CACHE.clear()
    resource_scope._MEMBER_CACHE.clear()
    yield generation
    for sub in ("tools/fleet", "tools"):
        (generation / sub).chmod(0o755)
    generation.chmod(0o755)


def _tool(generation, script):
    return str(generation / "tools" / "fleet" / script)


def _seal(queue, tmp_path, name, *, script=None, extra_params=None, extra_command=(), python=None,
          argv=None, scope=None, task_over=None, tool=None, generation=None, variables=None):
    """Seal one action; a genuine movement node when ``script`` and ``tool`` are given.

    The shape is exactly ``movement_actions.seal_movement_action``'s: the bash capture wrapper as
    ``task.argv``, the movement task fields, the movement execution scope, the fleet python and
    the movement environment.  Each keyword spoils one part of it, for the look-alike cases.
    """
    from prismabuild import movement_actions as ma
    checkout = tmp_path / "checkout"
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    sealed_variables = {"variables": {}, "toolchain": {}}
    if script is None:
        command, task_argv, task_fields, execution_scope = (
            [sys.executable, "task.py"], [sys.executable, "task.py"],
            {"task_class": "generation", "determinism": "deterministic",
             "artifact_family": "generic", "artifact_kind": "generic"},
            {"portability": "portable", "platform_key": None, "host_class": None})
    else:
        tool = tool or _tool(generation, script)
        command = [python or ma.MOVEMENT_PYTHON, tool, "--pool-root", str(queue.root), *extra_command]
        task_argv = [ma.SEALED_ARGV0, "--noprofile", "--norc", "-c", ma.captured_command(command, name)]
        task_fields, execution_scope = dict(ma.MOVEMENT_TASK), dict(ma.MOVEMENT_EXECUTION_SCOPE)
        sealed_variables = {"variables": dict(ma.movement_environment(command)), "toolchain": {}}
    if variables is not None:
        sealed_variables = {"variables": dict(variables), "toolchain": {}}
    params = {"gpu_exclusive": False, "execution_timeout_s": 600, "command": command, **(extra_params or {})}
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/demand-fence", "definition_version": "v1", **task_fields,
                 "argv": argv if argv is not None else task_argv, "working_directory": ".",
                 "result_path": name, **(task_over or {})},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": params, "environment": sealed_variables,
        "execution_scope": scope if scope is not None else execution_scope}
    action = pb.seal_action(body)
    cas.publish_action_request(action)
    return action["action_key"], cas, checkout


def _enqueue(queue, clock, key, cas, checkout, *, resources, recompute=False, residency=None,
             priority=-10, tags=("sparky",)):
    clock[0] += 0.001
    queue.publish(action_key=key, cas_root=str(cas.root), checkout_root=str(checkout),
                  worker_script="worker.py", resources=dict(resources),
                  needs_gpu=bool(resources.get("gpu")), tags=list(tags), priority=priority,
                  max_attempts=1, retry_safe=True, recompute=recompute,
                  **({} if residency is None else {"residency": residency}))


def _publish_sealed(queue, tmp_path, clock, name, *, script, resources, residency=None, priority=-10,
                    tags=("sparky",), extra_params=None, extra_command=(), recompute=False, **spoil):
    key, cas, checkout = _seal(queue, tmp_path, name, script=script, extra_params=extra_params,
                               extra_command=extra_command, **spoil)
    _enqueue(queue, clock, key, cas, checkout, resources=resources, recompute=recompute,
             residency=residency, priority=priority, tags=tags)
    return key


def _row(queue, key):
    return pool._read_json(queue.item_path(pool.READY, key))


@pytest.mark.parametrize("field", ["returns_capacity", "serves_residency"])
@pytest.mark.parametrize("value", [True, False, "yes", 1])
def test_publish_refuses_a_role_declared_by_a_submitted_action(gang_fleet, store, tmp_path, field, value):
    """A forged exemption: the roles are PrismaBuild's to assign, never an action's to claim."""
    queue, clock, *_ = gang_fleet
    with pytest.raises(pool.PoolContractError, match="assigned by PrismaBuild"):
        _publish_sealed(queue, tmp_path, clock, f"forged-{field}-{value!r}", script="stage_release.py",
                        resources={"cpu": 1, "mem_gb": 1}, extra_params={field: value}, generation=store)


def _roles(queue, key):
    row = _row(queue, key)
    return [field for field in ("returns_capacity", "serves_residency") if row.get(field) is True]


def test_publish_assigns_the_roles_to_genuine_published_movement_nodes_only(gang_fleet, store, tmp_path):
    queue, clock, *_ = gang_fleet
    small = {"cpu": 1, "mem_gb": 1}
    mover = {"cpu": 4, "mem_gb": 8, STAGE_KIND: 2}
    resident = ("--pool-root", "q", "--set-id", "s", "--host", "h", "--policy", "p")
    cases = [
        # (name, script, resources, residency, extra_command, expected role)
        ("release", "stage_release.py", small, None, (), "returns_capacity"),
        ("export", "produced_export.py", small, None, (), "returns_capacity"),
        ("evict", "local_resident.py", small, None, (*resident, "--operation", "evict"),
         "returns_capacity"),
        ("mover", "stage_move.py", mover, RANGE, (), "serves_residency"),
        ("promotion", "ram_promote.py", mover, RANGE, (), "serves_residency"),
        # genuine tools that are not exempt: each missing condition leaves an ordinary consumer
        ("big-release", "stage_release.py", {"cpu": 8, "mem_gb": 1}, None, (), None),
        ("big-memory", "stage_release.py", {"cpu": 1, "mem_gb": 64}, None, (), None),
        ("gpu-release", "stage_release.py", {**small, "gpu": 1}, None, (), None),
        ("foreign-kind", "stage_release.py", {**small, "scratch_gib": 1}, None, (), None),
        ("copy", "local_resident.py", small, None, (*resident, "--operation", "copy"), None),
        ("mover-without-range", "stage_move.py", {"cpu": 4, "mem_gb": 8}, None, (), None),
        ("gpu-mover", "stage_move.py", {"cpu": 4, "mem_gb": 8, "gpu": 1, STAGE_KIND: 2}, RANGE, (), None),
    ]
    for name, script, resources, residency, extra, role in cases:
        key = _publish_sealed(queue, tmp_path, clock, name, script=script, resources=resources,
                              residency=residency, extra_command=extra, generation=store)
        assert _roles(queue, key) == ([role] if role else []), (name, _row(queue, key))
    # An ordinary action has no role whatever it demands.
    plain = _publish_sealed(queue, tmp_path, clock, "plain", script=None, resources=small)
    assert _roles(queue, plain) == []


def test_a_genuine_spool_export_gets_its_role_without_recompute(gang_fleet, store, tmp_path):
    """Review 2: ``ProducedSpool._publish`` publishes exports without ``recompute``; the role does not need it."""
    queue, clock, *_ = gang_fleet
    for recompute in (False, True):
        key = _publish_sealed(queue, tmp_path, clock, f"export-{recompute}", script="produced_export.py",
                              resources={"cpu": 1, "mem_gb": 1}, recompute=recompute, generation=store)
        assert _roles(queue, key) == ["returns_capacity"], recompute


def test_a_look_alike_is_refused_a_role_part_by_part(gang_fleet, store, tmp_path, monkeypatch):
    """Every way a submitted action could imitate a movement node leaves it an ordinary row (review 2)."""
    from prismabuild import movement_actions as ma
    queue, clock, *_ = gang_fleet
    small = {"cpu": 1, "mem_gb": 1}
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    lookalike = outside / "stage_release.py"            # right name, wrong place
    lookalike.write_text("# not a published tool\n")
    # a real generation directory that is not published: no receipt naming it
    unknown = Path(str(store.parent)) / ("d" * 12 + "-1791311474-" + "e" * 12)
    (unknown / "tools" / "fleet").mkdir(parents=True)
    (unknown / "tools" / "fleet" / "stage_release.py").write_text("# unpublished\n")
    # a published file whose bytes no longer match the manifest
    tampered = store / "tools" / "fleet" / "produced_export.py"
    tampered.chmod(0o644)
    tampered.write_text("# tampered after publication\n")
    tampered.chmod(0o444)
    hook = {"BASH_ENV": str(tmp_path / "payload.sh")}
    startup = {"PYTHONPATH": str(tmp_path), "PYTHONSTARTUP": str(tmp_path / "start.py")}
    cases = {
        "look-alike script in /tmp": dict(tool=str(lookalike)),
        "unknown generation": dict(tool=str(unknown / "tools" / "fleet" / "stage_release.py")),
        "tampered published file": dict(script="produced_export.py"),
        "relative script path": dict(tool="tools/fleet/stage_release.py"),
        "wrong interpreter": dict(python="/bin/sh"),
        "relative interpreter": dict(python="python3"),
        "submitter python": dict(python=str(outside / "python3")),
        "wrong wrapper executable": dict(argv=["/bin/sh", "--noprofile", "--norc", "-c", "true"]),
        "argv is not the capture wrapper": dict(argv=[sys.executable, "task.py"]),
        "wrapper runs other code": dict(argv=[ma.SEALED_ARGV0, "--noprofile", "--norc", "-c", "echo anything"]),
        "wrong movement task": dict(task_over={"determinism": "deterministic"}),
        "bash hook in environment": dict(variables={**ma.movement_environment(["x"]), **hook}),
        "python startup hook in environment": dict(variables={**ma.movement_environment(["x"]), **startup}),
        "empty environment": dict(variables={}),
    }
    for name, spoil in cases.items():
        spoil = {"script": "stage_release.py", **spoil}
        if "variables" in spoil and len(spoil["variables"]) > 3:
            command = [ma.MOVEMENT_PYTHON, _tool(store, "stage_release.py"),
                       "--pool-root", str(queue.root)]
            spoil["variables"] = {**ma.movement_environment(command),
                                  **{k: v for k, v in spoil["variables"].items()
                                     if k not in ma.movement_environment(command)}}
        key = _publish_sealed(queue, tmp_path, clock, name.replace(" ", "-"), resources=small,
                              generation=store, **spoil)
        assert _roles(queue, key) == [], (name, _row(queue, key))


def test_a_submitter_alias_of_a_published_tool_gets_no_role(gang_fleet, store, tmp_path):
    """Review 3: the role is read off the resolved member, and the sealed path spells it exactly."""
    from prismabuild import movement_actions as ma
    from prismabuild import resource_scope
    queue, clock, *_ = gang_fleet
    small = {"cpu": 1, "mem_gb": 1}
    # An alias named stage_release.py pointing at the published stage_move.py:
    # the anchor verifies the mover bytes, but the sealed name is an alias.
    alias = tmp_path / "stage_release.py"
    alias.symlink_to(store / "tools" / "fleet" / "stage_move.py")
    key = _publish_sealed(queue, tmp_path, clock, "alias-basename", script="stage_release.py",
                          resources=small, tool=str(alias), generation=store)
    assert _roles(queue, key) == [], _row(queue, key)
    # The same alias retargeted after publication still names the alias.
    alias.unlink()
    alias.symlink_to(store / "tools" / "fleet" / "stage_release.py")
    resource_scope._MEMBER_CACHE.clear()
    key = _publish_sealed(queue, tmp_path, clock, "alias-retargeted", script="stage_release.py",
                          resources=small, tool=str(alias), generation=store)
    assert _roles(queue, key) == [], _row(queue, key)
    # A published tool reached through an unrelated published name is ordinary too.
    honest = tmp_path / "honest-link.py"
    honest.symlink_to(store / "tools" / "fleet" / "stage_release.py")
    key = _publish_sealed(queue, tmp_path, clock, "honest-alias", script="stage_release.py",
                          resources=small, tool=str(honest), generation=store)
    assert _roles(queue, key) == [], _row(queue, key)
    # The sealer canonicalizes the live repo link, so a sealed member path counts.
    link = tmp_path / "repo"
    if not link.exists():
        link.symlink_to(store)
    canonical = ma.movement_tools({"tier_id": "t", "mover_python": ma.MOVEMENT_PYTHON,
                                   "mover_tools_root": str(link / "tools" / "fleet")})[2]
    assert canonical == str(store / "tools" / "fleet" / "stage_release.py"), canonical
    key = _publish_sealed(queue, tmp_path, clock, "canonical-member", script="stage_release.py",
                          resources=small, tool=canonical, generation=store)
    assert _roles(queue, key) == ["returns_capacity"]


def test_local_resident_operation_parsing_matches_the_tool(gang_fleet, store, tmp_path):
    """Review 3: the role's operation test parses exactly as local_resident does."""
    import sys
    sys.path.insert(0, str(Path("tools/fleet").resolve()))
    from local_resident import effective_operation as tool_parses
    from prismabuild import movement_actions as ma
    queue, clock, *_ = gang_fleet
    small = {"cpu": 1, "mem_gb": 1}
    base = ["--pool-root", str(queue.root), "--set-id", "s", "--host", "h", "--policy", "/p"]
    shapes = [
        [*base, "--operation", "evict"],
        [*base, "--operation", "evict", "--operation", "copy"],
        [*base, "--operation=copy", "--operation", "evict"],
        [*base, "--oper", "evict"],
        [*base, "--operation=evict"],
        [*base, "--operation", "copy"],
        [*base, "--operation"],
        [*base],
    ]
    for argv in shapes:
        assert ma.effective_local_resident_operation(argv) == tool_parses(argv), argv
    # Only one literal --operation evict gets the role; every other shape is ordinary.
    key = _publish_sealed(queue, tmp_path, clock, "evict", script="local_resident.py",
                          resources=small, extra_command=(*base, "--operation", "evict"),
                          generation=store)
    assert _roles(queue, key) == ["returns_capacity"]
    for name, extra in {
            "duplicate-evict-copy": (*base, "--operation", "evict", "--operation", "copy"),
            "duplicate-copy-evict": (*base, "--operation", "copy", "--operation", "evict"),
            "equals-evict": (*base, "--operation=evict"),
            "abbreviated-evict": (*base, "--oper", "evict"),
            "truncated": (*base, "--operation"),
    }.items():
        key = _publish_sealed(queue, tmp_path, clock, name, script="local_resident.py",
                              resources=small, extra_command=extra, generation=store)
        assert _roles(queue, key) == [], (name, _row(queue, key))


def test_without_a_retained_store_nothing_is_a_movement_node(gang_fleet, store, tmp_path, monkeypatch):
    """No published generations to anchor to is an ordinary row, never a guess."""
    from prismabuild import resource_scope
    queue, clock, *_ = gang_fleet
    monkeypatch.setattr(resource_scope, "RETAINED_GENERATION_STORE", tmp_path / "no-such-store")
    key = _publish_sealed(queue, tmp_path, clock, "no-store", script="stage_release.py",
                          resources={"cpu": 1, "mem_gb": 1}, generation=store)
    assert _roles(queue, key) == []


def _wait_gang(publish, gclaim, members, clock, name, **kw):
    incumbents = _busy_both(publish, gclaim)
    group, keys = members(name, priority=-10, **kw)
    for host in HOSTS:
        assert gclaim(host) is None
    return incumbents, group, keys


def test_a_waiting_gang_reserves_its_member_demand_and_admits_what_returns_capacity(gang_fleet, store, tmp_path):
    """Real claims: held by demand, never by type.

    Past the bound sparky admits PrismaBuild's own release node while the GPU single and a small
    ordinary row are held, because the incumbent still holds what the member needs.  When the
    incumbent ends, the small row fits beside the member and is admitted; the GPU single never does.
    """
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents, group, (first, second) = _wait_gang(publish, gclaim, members, clock, "res")
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    gpu = publish("late-gpu", priority=-10, timeout_s=None, cpu=1, gpu=1, mem_gb=8, tags=["sparky"])
    small = _publish_sealed(queue, tmp_path, clock, "small", script=None, resources={"cpu": 1, "mem_gb": 1})
    release = _publish_sealed(queue, tmp_path, clock, "release", script="stage_release.py",
                              resources={"cpu": 1, "mem_gb": 1}, generation=store)
    assert gclaim("sparky") == release, (denial(release, "sparky"), denial(small, "sparky"))
    for key in (gpu, small):
        assert denial(key, "sparky")["reason"] in REASONS, denial(key, "sparky")
    finish(incumbents["sparky"], "sparky")
    assert gclaim("sparky") == small, denial(small, "sparky")
    assert denial(gpu, "sparky")["reason"] in REASONS


def test_a_young_gang_does_not_hold_equal_priority_work(gang_fleet, tmp_path):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    _wait_gang(publish, gclaim, members, clock, "young")
    clock[0] += reservation.GANG_RESERVE_AFTER_S - 60
    small = _publish_sealed(queue, tmp_path, clock, "young-small", script=None, resources={"cpu": 1, "mem_gb": 1})
    assert gclaim("sparky") == small


def test_a_gang_member_that_takes_every_cpu_still_progresses_through_its_own_movers(
        gang_fleet, store, monkeypatch, tmp_path):
    """The review's hole, end to end.

    Member 1 takes all 20 CPUs on sparky and waits there, its gang past the bound.  Member 0's
    residency lead is a stage mover (4 CPUs, 8 GiB, tier tokens) that runs on sparky.  The
    reservation leaves no CPU slack, so an ordinary row of that demand is held; the sealed mover
    PrismaBuild publishes (recompute, its own script, its range) is admitted anyway, runs, and
    the gang starts whole.
    """
    from test_gang_residency_members import _compose_map, _consumer_block
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    stage = tmp_path / "stage"
    stage.mkdir()
    need = {"cpu": 4, "mem_gb": 8, STAGE_KIND: 2}
    mover, cas, checkout = _seal(queue, tmp_path, "mover", script="stage_move.py", generation=store)
    incumbents = _busy_both(publish, gclaim)
    group, (first, second) = members("whole-cpu", priority=-10, member_cpu=20,
                                     residency=_consumer_block([mover]))
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    for host in HOSTS:
        assert gclaim(host) is None
    assert _gang.elections(queue, group, 2)[1]["host"] == "sparky"
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    for host in HOSTS:
        finish(incumbents[host], host)

    # An ordinary row with the mover's demand is held: 4 CPUs beside the member's 20 do not fit.
    control = _publish_sealed(queue, tmp_path, clock, "ordinary-same-demand", script=None,
                              resources=need, residency=RANGE)
    assert gclaim("sparky") is None
    held = denial(control, "sparky")
    assert held["reason"] in REASONS, held
    assert held["evidence"]["gang_election"]["reservation"]["cpu"] == 4, held
    queue.withdraw(control, reason="control done", by="test")

    # The mover PrismaBuild publishes is admitted, runs, and releases the gang.
    _enqueue(queue, clock, mover, cas, checkout, resources=need, residency=RANGE)
    assert _row(queue, mover)["serves_residency"] is True
    assert gclaim("sparky") == mover, denial(mover, "sparky")
    queue.record_move(mover, {
        "consumer_action_key": first, "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": MANIFEST, "range_start_bytes": 0, "range_end_bytes": 2 * GIB,
        "bytes_staged": 2 * GIB, "complete": True})
    queue.finish(mover, status="executed")
    _compose_map(monkeypatch, queue, first, [mover])
    started = set()
    for _ in range(3):
        for host in ("sparklina", "sparky"):
            claimed = gclaim(host)
            if claimed is not None:
                started.add(claimed)
    assert started == {first, second}, (started, denial(first, "sparklina"), denial(second, "sparky"))


def test_a_carried_measurement_withhold_does_not_hold_back_an_aged_gang_member(
        gang_fleet, monkeypatch, tmp_path):
    """The review's carried-withhold path: a measurement's carried episode is a measurement withhold.

    The older measurement single cannot be evaluated this pass (its residency lead record is
    unreadable) and carries its withhold.  Past the bound the gang member behind it still elects.
    """
    from test_gang_residency_members import _consumer_block, _hexkey
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    measurement = publish("carried-measurement", measurement=True, priority=-10, timeout_s=None,
                          cpu=2, gpu=1, mem_gb=8, tags=["sparky"])
    group, (first, second) = members("behind-carried", priority=-10)
    real = queue.residency_verdict

    def verdict(item):
        if item.get("action_key") == measurement:
            raise OSError("ESTALE")
        return real(item)

    monkeypatch.setattr(queue, "residency_verdict", verdict)
    real_carry = queue._carried_withhold
    monkeypatch.setattr(pool.PoolQueue, "_carried_withhold", staticmethod(
        lambda records, item, *, host, now: {
            "reason": "adaptive_cpu_refused_withholding", "mode": "exclusive",
            "epoch_unix": now, "drain_until_unix": now + 3600.0}
        if item.get("action_key") == measurement else real_carry(records, item, host=host, now=now)))
    assert gclaim("sparklina") is None
    assert gclaim("sparky") is None
    assert denial(second, "sparky")["reason"] == "deferred_behind_withheld_row", denial(second, "sparky")
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    assert gclaim("sparky") is None
    assert denial(second, "sparky")["reason"] != "deferred_behind_withheld_row", denial(second, "sparky")
    assert _gang.elections(queue, group, 2).get(1) is not None
    assert queue.item_path(pool.READY, measurement).exists()


def test_the_reservation_wins_over_an_older_measurement_withhold_so_the_gang_can_elect(gang_fleet, tmp_path):
    """The morning's starvation: a waiting measurement single held the box."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    measurement = publish("old-measurement", measurement=True, priority=-10, timeout_s=None,
                          cpu=2, gpu=1, mem_gb=8, tags=["sparky"])
    group, (first, second) = members("behind-measurement", priority=-10)
    assert gclaim("sparklina") is None
    assert gclaim("sparky") is None
    assert denial(second, "sparky")["reason"] == "deferred_behind_withheld_row", denial(second, "sparky")
    assert _gang.elections(queue, group, 2).get(1) is None
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    assert gclaim("sparky") is None
    assert denial(second, "sparky")["reason"] != "deferred_behind_withheld_row", denial(second, "sparky")
    assert _gang.elections(queue, group, 2).get(1) is not None
    assert queue.item_path(pool.READY, measurement).exists()


def test_a_strictly_higher_priority_measurement_keeps_its_place_ahead_of_an_aged_gang(gang_fleet, tmp_path):
    """Priority order, not a reservation exception: a priority-10 measurement goes first."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    group, (first, second) = members("below-ship", priority=-10)
    for host in HOSTS:
        assert gclaim(host) is None
    elected = _gang.elections(queue, group, 2)
    assert {e["host"] for e in elected.values()} == set(HOSTS)
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    ship = publish("ship-window-measurement", measurement=True, priority=10, timeout_s=None,
                   cpu=2, gpu=1, mem_gb=8, tags=["sparky"])
    for _ in range(3):
        assert gclaim("sparky") is None
    census = reservation.CensusReader(queue, queue.ledger("sparky")).capture()
    assert ship in census["elections"], "the higher-priority measurement still elects on a reserved host"
    held = denial(second, "sparky")
    assert held["reason"] in ("deferred_for_measurement_reservation", "deferred_behind_withheld_row"), held
    assert held["evidence"]["withheld_for"] == ship, held  # held for the higher-priority measurement


def test_a_measurement_single_of_the_gangs_priority_does_not_elect_on_a_reserved_host(gang_fleet, tmp_path):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents, group, keys = _wait_gang(publish, gclaim, members, clock, "no-elect")
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    measurement = publish("late-measurement", measurement=True, priority=-10, timeout_s=None,
                          cpu=2, gpu=1, mem_gb=8, tags=["sparky"])
    assert gclaim("sparky") is None
    census = reservation.CensusReader(queue, queue.ledger("sparky")).capture()
    assert measurement not in census["elections"], "a host must not be reserved and withheld"
    assert queue.item_path(pool.READY, measurement).exists()


def test_a_measurement_class_gang_member_is_not_blocked_by_its_own_reservation(gang_fleet, tmp_path):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    group, (first, second) = members("measured-gang", priority=-10, measurement_member=1)
    for host in HOSTS:
        assert gclaim(host) is None
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    other = publish("other-measurement", measurement=True, priority=-10, timeout_s=None,
                    cpu=2, gpu=1, mem_gb=8, tags=["sparky"])
    for host in HOSTS:
        finish(incumbents[host], host)
    started = set()
    for _ in range(3):
        for host in HOSTS:
            claimed = gclaim(host)
            if claimed is not None:
                started.add(claimed)
    assert started == {first, second}, (started, denial(second, "sparky"), denial(other, "sparky"))
    assert queue.item_path(pool.READY, other).exists()


def test_a_changed_scope_or_task_on_a_genuine_node_loses_the_role(gang_fleet, store, tmp_path):
    """Checked on a mutated copy of a genuine sealed node, since the sealer refuses some scopes."""
    import copy
    from prismabuild import movement_actions as ma
    queue, clock, *_ = gang_fleet
    key, cas, checkout = _seal(queue, tmp_path, "genuine", script="stage_release.py", generation=store)
    genuine = pool._sealed_action_request(str(cas.root), key)
    small = {"cpu": 1, "mem_gb": 1}
    assert ma.capacity_role(genuine, small, residency=None) == "returns_capacity"
    for mutate in (
            lambda a: a["execution_scope"].update(portability="host_class_keyed", host_class="gb10"),
            lambda a: a["execution_scope"].update(platform_key="linux-x86_64"),
            lambda a: a["task"].update(determinism="deterministic"),
            lambda a: a["task"].update(artifact_kind="measurement"),
            lambda a: a["task"].update(result_path="another.log"),
            lambda a: a["task"]["argv"].__setitem__(4, a["task"]["argv"][4] + " "),
            lambda a: a["params"]["command"].append("--extra"),
            lambda a: a["environment"]["variables"].update(BASH_ENV="/tmp/payload.sh"),
            lambda a: a["environment"]["variables"].update(PYTHONPATH="/tmp/evil"),
            lambda a: a["environment"]["variables"].__delitem__("LANG"),
            lambda a: a["params"]["command"].__setitem__(0, "/tmp/python3")):
        changed = copy.deepcopy(dict(genuine))
        mutate(changed)
        assert ma.capacity_role(changed, small, residency=None) is None

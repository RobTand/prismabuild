"""The demand-based gang fence on real claims (#1579, #1659).

The two-host fixture of ``test_gang_reservation_1517``: real queue, ledgers,
census and controllers; only the clock and the sampler are controlled.  A gang
member is a whole-box row (cpu 2 or 20, gpu 1, mem 100 on a 20/1/120 host).

``gang_fleet`` runs on a host that holds the protected copy of the runtime it
runs, so the reservation applies; the fallback tests take the copy away.  The
roles ``returns_capacity`` and ``serves_residency`` are derived by ``publish``
from the sealed definition of a movement node and refused in any submitted
action; the host that enforces the reservation honours a mark only when the
tool the row names is a member of its own protected copy.  These tests publish
real sealed movement nodes.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from movement_publication_support import retained_generation, unseal
from test_gang_reservation_1517 import HOSTS, _busy_both, gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401

from prismabuild import _gang, _measurement_reservation as reservation, core as pb, pool
from prismabuild import movement_actions as ma, resource_scope, runtime_publication

REASONS = ("deferred_for_gang_reservation",)
from test_gang_residency_members import GIB, MANIFEST, STAGE_KIND, TIER  # noqa: E402

RANGE = {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER, "manifest_sha256": MANIFEST,
         "manifest_bytes": 6 * GIB, "range_start_bytes": 0, "range_end_bytes": 2 * GIB}
SMALL = {"cpu": 1, "mem_gb": 1}


@pytest.fixture()
def store(movement_authority):
    """The protected copy of the generation this host runs, with its tools."""
    return movement_authority


@pytest.fixture()
def retained(store):
    """The same generation as an ordinary retained-store directory, which no role anchors."""
    return resource_scope.RETAINED_GENERATION_STORE / store.name


def _tool(generation, script):
    return str(generation / "tools" / "fleet" / script)


def _seal(queue, tmp_path, name, *, script=None, extra_params=None, extra_command=(), python=None,
          argv=None, scope=None, task_over=None, tool=None, generation=None, variables=None,
          isolated=True):
    """Seal one action; a genuine movement node when ``script`` and ``tool`` are given.

    The shape is exactly ``movement_actions.seal_movement_action``'s: the bash capture wrapper as
    ``task.argv``, the movement task fields, the movement execution scope, the fleet python and
    the movement environment.  Each keyword spoils one part of it, for the look-alike cases.
    ``isolated=False`` seals the previous sealer's shape without ``-I`` (review 305cadf).
    """
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
        isolated_args = list(ma.MOVEMENT_PYTHON_ARGS) if isolated else []
        command = [python or ma.MOVEMENT_PYTHON, *isolated_args, tool,
                   "--pool-root", str(queue.root), *extra_command]
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


def _roles(queue, key):
    row = _row(queue, key)
    return [field for field in ma.CAPACITY_ROLE_FIELDS if row.get(field) is True]


# --- what publish derives, from the sealed definition alone --------------------------

@pytest.mark.parametrize("field", ["returns_capacity", "serves_residency"])
@pytest.mark.parametrize("value", [True, False, "yes", 1])
def test_publish_refuses_a_role_declared_by_a_submitted_action(gang_fleet, store, tmp_path, field, value):
    """A forged exemption: the roles are PrismaBuild's to assign, never an action's to claim."""
    queue, clock, *_ = gang_fleet
    with pytest.raises(pool.PoolContractError, match="assigned by PrismaBuild"):
        _publish_sealed(queue, tmp_path, clock, f"forged-{field}-{value!r}", script="stage_release.py",
                        resources=SMALL, extra_params={field: value}, generation=store)


def test_publish_assigns_the_roles_to_movement_nodes_of_a_protected_copy_only(gang_fleet, store, tmp_path):
    queue, clock, *_ = gang_fleet
    small = SMALL
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
        if role:
            assert _row(queue, key)[ma.ROLE_SCRIPT_FIELD] == _tool(store, script), name
        else:
            assert ma.ROLE_SCRIPT_FIELD not in _row(queue, key), name
    # An ordinary action has no role whatever it demands.
    plain = _publish_sealed(queue, tmp_path, clock, "plain", script=None, resources=small)
    assert _roles(queue, plain) == []


def test_a_genuine_spool_export_gets_its_role_without_recompute(gang_fleet, store, tmp_path):
    """Review 2: ``ProducedSpool._publish`` publishes exports without ``recompute``; the role does not need it."""
    queue, clock, *_ = gang_fleet
    for recompute in (False, True):
        key = _publish_sealed(queue, tmp_path, clock, f"export-{recompute}", script="produced_export.py",
                              resources=SMALL, recompute=recompute, generation=store)
        assert _roles(queue, key) == ["returns_capacity"], recompute


# --- the sealer names a tool root the executing host announced -----------------------

def _tier(root):
    return {"tier_id": TIER, "host": "sparky", "mover_python": ma.MOVEMENT_PYTHON,
            "mover_tools_root": str(root)}


def _template(queue, tmp_path):
    key, cas, checkout = _seal(queue, tmp_path, "consumer")
    consumer = pool._sealed_action_request(str(cas.root), key)
    return {"task": consumer["task"], "params": {"cwd": str(checkout)}, "inputs": consumer["inputs"],
            "code_closure": consumer["code_closure"], "environment": consumer["environment"],
            "marker_root": tmp_path / "markers", "checkout_identity": {"commit": "a" * 40}}, cas, checkout


@pytest.mark.parametrize("layout", ["tools", "tools/fleet"])
def test_a_mover_sealed_from_a_protected_tool_root_gets_its_role(gang_fleet, store, tmp_path, layout):
    queue, clock, *_ = gang_fleet
    template, cas, checkout = _template(queue, tmp_path)
    python, mover, egress = ma.movement_tools(_tier(store / layout))
    action = ma.seal_movement_action(
        template, command=[python, egress, "--pool-root", str(queue.root)],
        demand=SMALL, tags=["sparky"], log_name=f"release-{layout}.log")
    assert action["params"]["command"][1:1 + len(ma.MOVEMENT_PYTHON_ARGS)] == list(ma.MOVEMENT_PYTHON_ARGS)
    assert action["params"]["command"][1 + len(ma.MOVEMENT_PYTHON_ARGS)] == str(store / layout / "stage_release.py")


def test_the_sealing_host_having_the_copy_does_not_change_the_path_the_executing_host_runs(
        gang_fleet, store, retained, tmp_path):
    """Review of 14fb82c6, finding 3: a partial deployment must not break a mover launch.

    This process (the sealing host) holds the protected copy.  The executing host's tier announces
    only its ordinary tool root, so it has no copy.  The sealed command names the path that
    host announced, a file it has; the sealer never swaps in a path from its own box.
    """
    queue, clock, *_ = gang_fleet
    template, cas, checkout = _template(queue, tmp_path)
    announced = retained / "tools" / "fleet"
    python, mover, egress = ma.movement_tools(_tier(announced))
    for tool, script in ((mover, "stage_move.py"), (egress, "stage_release.py")):
        assert tool == str(announced / script)
    action = ma.seal_movement_action(
        template, command=[python, egress, "--pool-root", str(queue.root)],
        demand=SMALL, tags=["sparky"], log_name="partial.log")
    assert action["params"]["command"][1 + len(ma.MOVEMENT_PYTHON_ARGS)] == str(announced / "stage_release.py")
    assert action["task"]["argv"][-1].count(str(store)) == 0
    cas.publish_action_request(action)
    _enqueue(queue, clock, action["action_key"], cas, checkout, resources=SMALL)
    assert _roles(queue, action["action_key"]) == [], "an ordinary retained path derives no role"


def test_a_retained_store_path_is_no_protected_tool(gang_fleet, store, retained, tmp_path):
    """A store owner cannot authorize its own movement code: its path is not the protected copy's."""
    queue, clock, *_ = gang_fleet
    key = _publish_sealed(queue, tmp_path, clock, "self-certified-release",
                          script="stage_release.py", resources=SMALL, generation=retained)
    assert _roles(queue, key) == []


def test_a_look_alike_is_refused_a_role_part_by_part(gang_fleet, store, retained, tmp_path):
    """Every way a submitted action could imitate a movement node leaves it an ordinary row (review 2)."""
    queue, clock, *_ = gang_fleet
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    lookalike = outside / "stage_release.py"            # right name, wrong place
    lookalike.write_text("# not a published tool\n")
    unknown = retained.parent / ("d" * 12 + "-1791311474-" + "e" * 12)   # a store directory, not protected
    (unknown / "tools" / "fleet").mkdir(parents=True)
    (unknown / "tools" / "fleet" / "stage_release.py").write_text("# unpublished\n")
    hook = {"BASH_ENV": str(tmp_path / "payload.sh")}
    startup = {"PYTHONPATH": str(tmp_path), "PYTHONSTARTUP": str(tmp_path / "start.py")}
    cases = {
        "look-alike script in /tmp": dict(tool=str(lookalike)),
        "unknown generation": dict(tool=str(unknown / "tools" / "fleet" / "stage_release.py")),
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
        key = _publish_sealed(queue, tmp_path, clock, name.replace(" ", "-"), resources=SMALL,
                              generation=store, **spoil)
        assert _roles(queue, key) == [], (name, _row(queue, key))


@pytest.mark.parametrize("missing", ["PATH", "LANG", "LC_ALL"])
def test_missing_movement_environment_keys_with_docker_ownership_publish_without_a_role(
        gang_fleet, store, tmp_path, missing):
    """An incomplete movement environment produces an ordinary row."""
    queue, clock, *_ = gang_fleet
    command = [ma.MOVEMENT_PYTHON, _tool(store, "stage_release.py"),
               "--pool-root", str(queue.root)]
    owner = "f" * 64
    variables = {
        **ma.movement_environment(command),
        pool.CONTAINER_OWNER_ENV: owner,
        pool.CONTAINER_MARKER_ENV: str(tmp_path / f"{owner}.used"),
    }
    variables.pop(missing)
    key = _publish_sealed(queue, tmp_path, clock, f"missing-{missing}",
                          script="stage_release.py", resources=SMALL,
                          generation=store, variables=variables)
    assert _roles(queue, key) == []
    assert _row(queue, key)["resources"] == SMALL


def test_a_submitter_alias_of_a_published_tool_gets_no_role(gang_fleet, store, tmp_path):
    """Review 3: the sealed path spells a protected member, never a mutable alias."""
    queue, clock, *_ = gang_fleet
    # An alias named stage_release.py pointing at the published stage_move.py.
    alias = tmp_path / "stage_release.py"
    alias.symlink_to(store / "tools" / "fleet" / "stage_move.py")
    key = _publish_sealed(queue, tmp_path, clock, "alias-basename", script="stage_release.py",
                          resources=SMALL, tool=str(alias), generation=store)
    assert _roles(queue, key) == [], _row(queue, key)
    # The same alias retargeted after publication still names the alias.
    alias.unlink()
    alias.symlink_to(store / "tools" / "fleet" / "stage_release.py")
    key = _publish_sealed(queue, tmp_path, clock, "alias-retargeted", script="stage_release.py",
                          resources=SMALL, tool=str(alias), generation=store)
    assert _roles(queue, key) == [], _row(queue, key)


def test_local_resident_operation_parsing_matches_the_tool(gang_fleet, store, tmp_path):
    """Review 3: the role's operation test parses exactly as local_resident does."""
    sys.path.insert(0, str(Path("tools/fleet").resolve()))
    from local_resident import effective_operation as tool_parses
    queue, clock, *_ = gang_fleet
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
                          resources=SMALL, extra_command=(*base, "--operation", "evict"),
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
                              resources=SMALL, extra_command=extra, generation=store)
        assert _roles(queue, key) == [], (name, _row(queue, key))


def test_a_changed_scope_or_task_on_a_genuine_node_loses_the_role(gang_fleet, store, tmp_path):
    """Checked on a mutated copy of a genuine sealed node, since the sealer refuses some scopes."""
    import copy
    queue, clock, *_ = gang_fleet
    key, cas, checkout = _seal(queue, tmp_path, "genuine", script="stage_release.py", generation=store)
    genuine = pool._sealed_action_request(str(cas.root), key)
    assert ma.capacity_role(genuine, SMALL, residency=None) == ma.CapacityRole(
        "returns_capacity", _tool(store, "stage_release.py"))
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
        assert ma.capacity_role(changed, SMALL, residency=None) is None


# --- the host that enforces the reservation checks the tool against its own copy ------

def _mark(path, role="returns_capacity", **over):
    return {role: True, ma.ROLE_SCRIPT_FIELD: str(path), **over}


def test_a_mark_stands_where_the_named_tool_is_a_member_of_this_hosts_protected_copy(store):
    assert ma.authorized_role(_mark(store / "tools" / "fleet" / "stage_release.py"))
    assert ma.authorized_role(_mark(store / "tools" / "stage_release.py"))
    assert ma.authorized_role(_mark(store / "tools" / "fleet" / "stage_move.py", "serves_residency"))
    assert ma.authorized_role(_mark(store / "tools" / "fleet" / "ram_promote.py", "serves_residency"))
    assert not ma.authorized_role({})        # a row with no mark costs nothing to ask about


def test_a_mark_without_such_a_tool_exempts_nothing(store, retained, tmp_path):
    genuine = store / "tools" / "fleet" / "stage_release.py"
    other_generation = store.parent / ("d" * 12 + "-1791311474-" + "e" * 12)     # no copy here
    cases = {
        "a hand-written mark with no tool": {"returns_capacity": True},
        "a tool outside the protected store": _mark(retained / "tools" / "fleet" / "stage_release.py"),
        "a tool of a generation this host has no copy of":
            _mark(other_generation / "tools" / "fleet" / "stage_release.py"),
        "a role the tool cannot have": _mark(store / "tools" / "fleet" / "stage_move.py"),
        "the other role the tool cannot have": _mark(genuine, "serves_residency"),
        "a tool that is no movement tool": _mark(store / "src" / "prismabuild" / "helper.py"),
        "two roles": _mark(genuine, serves_residency=True),
        "a mark that is not literally true": {"returns_capacity": "true", ma.ROLE_SCRIPT_FIELD: str(genuine)},
        "a relative path": _mark("tools/fleet/stage_release.py"),
        "a path that is not normalized": _mark(f"{store}/tools/../tools/fleet/stage_release.py"),
        "no path": {"returns_capacity": True, ma.ROLE_SCRIPT_FIELD: None},
    }
    for name, row in cases.items():
        assert not ma.authorized_role(row), name


def test_a_tool_altered_after_publication_loses_its_mark(store):
    member = store / "tools" / "fleet" / "stage_release.py"
    assert ma.authorized_role(_mark(member))
    member.chmod(0o644)
    member.write_text("# altered\n")
    member.chmod(0o444)
    assert not ma.authorized_role(_mark(member))


def test_an_alias_inside_the_protected_namespace_is_no_member(store):
    """The spelled path must be the member itself, whatever it resolves to."""
    alias = store / "tools" / "fleet" / "alias.py"
    store.chmod(0o755)
    (store / "tools" / "fleet").chmod(0o755)
    alias.symlink_to(store / "tools" / "fleet" / "stage_release.py")
    (store / "tools" / "fleet").chmod(0o555)
    store.chmod(0o555)
    assert not ma.authorized_role(_mark(alias))


# --- real claims ----------------------------------------------------------------------

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
    small = _publish_sealed(queue, tmp_path, clock, "small", script=None, resources=SMALL)
    release = _publish_sealed(queue, tmp_path, clock, "release", script="stage_release.py",
                              resources=SMALL, generation=store)
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
    small = _publish_sealed(queue, tmp_path, clock, "young-small", script=None, resources=SMALL)
    assert gclaim("sparky") == small


def _whole_cpu_gang(gang_fleet, monkeypatch, tmp_path, mover_tool, *, isolated=True):
    """A gang whose first member takes every CPU, aged past the bound, and its stage mover.

    Member 1 takes all 20 CPUs on sparky and waits there.  Member 0's residency lead is a stage
    mover (4 CPUs, 8 GiB, tier tokens) that runs on sparky, sealed with ``mover_tool``.
    ``isolated=False`` seals the mover as the previous sealer did, without ``-I``.
    """
    from test_gang_residency_members import _consumer_block
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    need = {"cpu": 4, "mem_gb": 8, STAGE_KIND: 2}
    mover, cas, checkout = _seal(queue, tmp_path, "mover", script="stage_move.py", tool=mover_tool,
                                 isolated=isolated)
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
    return mover, cas, checkout, need, first, second


def _run_the_mover_and_start_the_gang(gang_fleet, monkeypatch, tmp_path, mover, cas, checkout,
                                      need, first, second, *, role, published=False):
    from test_gang_residency_members import _compose_map
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    stage = tmp_path / "stage"
    stage.mkdir()
    if not published:
        _enqueue(queue, clock, mover, cas, checkout, resources=need, residency=RANGE)
    assert _roles(queue, mover) == ([role] if role else [])
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


def test_a_gang_member_that_takes_every_cpu_still_progresses_through_its_own_movers(
        gang_fleet, store, monkeypatch, tmp_path):
    """The review's hole, end to end, on a host that holds the protected copy.

    The reservation leaves no CPU slack, so an ordinary row of the mover's demand is held; the
    sealed mover PrismaBuild publishes (its own protected tool, its range) is admitted anyway,
    runs, and the gang starts whole.
    """
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    mover, cas, checkout, need, first, second = _whole_cpu_gang(
        gang_fleet, monkeypatch, tmp_path, _tool(store, "stage_move.py"))
    # An ordinary row with the mover's demand is held: 4 CPUs beside the member's 20 do not fit.
    control = _publish_sealed(queue, tmp_path, clock, "ordinary-same-demand", script=None,
                              resources=need, residency=RANGE)
    assert gclaim("sparky") is None
    held = denial(control, "sparky")
    assert held["reason"] in REASONS, held
    assert held["evidence"]["gang_election"]["reservation"]["cpu"] == 4, held
    queue.withdraw(control, reason="control done", by="test")
    _run_the_mover_and_start_the_gang(gang_fleet, monkeypatch, tmp_path, mover, cas, checkout,
                                      need, first, second, role="serves_residency")


def _lose_the_copy(kind, retained, monkeypatch, tmp_path):
    """Take this host's protected copy of the generation it runs away, in one of three ways."""
    if kind == "absent":                      # the host is not enrolled, or nothing was published yet
        monkeypatch.setattr(runtime_publication, "PROTECTED_GENERATION_STORE", tmp_path / "no-such-store")
    elif kind == "stale":                     # a roll: the copy of the last generation, not of this one
        live = retained_generation(retained.parent, "d" * 12 + "-1791400000-" + "e" * 12,
                                   commit="d" * 40)
        monkeypatch.setattr(runtime_publication, "executing_generation", lambda: live)
    else:                                     # a copy made from other bytes than this generation's receipt
        unseal(retained.parent)
        receipt = retained / "RUNTIME_VERSION.json"
        receipt.chmod(0o644)
        receipt.write_text(receipt.read_text().replace("c" * 40, "9" * 40))
        receipt.chmod(0o444)


def test_a_mark_derived_on_a_box_without_the_copy_is_honoured_on_the_box_that_has_it(
        gang_fleet, store, monkeypatch, tmp_path):
    """The control seat seals and publishes without being enrolled; the claiming host decides.

    The mover is published while the protected copy is away from this filesystem (the publishing
    box has none).  Its mark comes from the sealed definition alone.  The copy is back where the
    claiming host looks, so the aged whole-CPU gang starts through that mover.
    """
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    mover, cas, checkout, need, first, second = _whole_cpu_gang(
        gang_fleet, monkeypatch, tmp_path, _tool(store, "stage_move.py"))
    away = store.with_name(store.name + ".away")
    store.rename(away)
    try:
        assert runtime_publication.published_member(store / "tools" / "fleet" / "stage_move.py") is None
        _enqueue(queue, clock, mover, cas, checkout, resources=need, residency=RANGE)
    finally:
        away.rename(store)
    assert _roles(queue, mover) == ["serves_residency"]
    _run_the_mover_and_start_the_gang(gang_fleet, monkeypatch, tmp_path, mover, cas, checkout,
                                      need, first, second, role="serves_residency", published=True)


@pytest.mark.parametrize("missing", ["absent", "stale", "other-receipt"])
def test_a_host_without_the_copy_of_its_generation_has_no_authority(
        store, retained, monkeypatch, tmp_path, missing):
    import time as _time
    assert runtime_publication.live_authority() is False, "a fresh copy is not mature"
    assert runtime_publication.live_authority(now=_time.time() + 700) is True
    _lose_the_copy(missing, retained, monkeypatch, tmp_path)
    assert runtime_publication.live_authority() is False
    assert runtime_publication.live_authority(now=_time.time() + 700) is False


@pytest.mark.parametrize("missing", ["absent", "stale", "other-receipt"])
def test_without_a_protected_copy_of_its_generation_the_gang_starts_as_on_main(
        gang_fleet, store, retained, monkeypatch, tmp_path, missing):
    """Review of 14fb82c6, finding 1, and the CEO decision of 2026-10-09.

    A copy that has not arrived must never deadlock a gang.  The host has no protected copy of
    the generation it runs (none at all; only the copy of an older generation; a copy made from
    other bytes), so its movers are sealed from the ordinary tool root and are ordinary rows.  A
    whole-CPU member, aged past the bound, still starts: equal-priority work is not held, which
    is what main does.
    """
    _lose_the_copy(missing, retained, monkeypatch, tmp_path)
    mover, cas, checkout, need, first, second = _whole_cpu_gang(
        gang_fleet, monkeypatch, tmp_path, _tool(retained, "stage_move.py"))
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    # The mover is an ordinary row here, and so is any row of its demand: neither is held.
    ordinary = _publish_sealed(queue, tmp_path, clock, "ordinary-same-cpu", script=None,
                               resources={"cpu": need["cpu"], "mem_gb": need["mem_gb"]})
    assert gclaim("sparky") == ordinary, denial(ordinary, "sparky")
    finish(ordinary, "sparky")
    _run_the_mover_and_start_the_gang(gang_fleet, monkeypatch, tmp_path, mover, cas, checkout,
                                      need, first, second, role=None)


def test_without_the_copy_equal_priority_work_is_admitted_but_strictly_lower_is_still_fenced(
        gang_fleet, store, monkeypatch, tmp_path):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    monkeypatch.setattr(runtime_publication, "PROTECTED_GENERATION_STORE", tmp_path / "no-such-store")
    incumbents, group, (first, second) = _wait_gang(publish, gclaim, members, clock, "no-copy")
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    lower = publish("lower", priority=-20, timeout_s=None, cpu=1, mem_gb=1, tags=["sparky"])
    gpu = publish("equal-gpu", priority=-10, timeout_s=None, cpu=1, gpu=1, mem_gb=8, tags=["sparky"])
    assert gclaim("sparky") is None            # the incumbent still holds the box
    assert denial(lower, "sparky")["reason"] in REASONS, "the strictly lower fence of #1517 stays"
    assert "reservation" not in denial(lower, "sparky")["evidence"]["gang_election"]
    finish(incumbents["sparky"], "sparky")
    assert gclaim("sparky") == gpu, denial(gpu, "sparky")


def test_a_mark_judged_by_another_box_does_not_exempt_here(gang_fleet, store, monkeypatch, tmp_path):
    """The host that enforces the reservation decides, with its own copy.

    The row is marked from a sealed definition naming the tool of a generation whose protected copy
    this host lacks (the host holds the copy of the generation it runs, not of that one).  The mark
    exempts nothing, so the row is held by its demand.
    """
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents, group, (first, second) = _wait_gang(publish, gclaim, members, clock, "judged")
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    foreign = store.parent / ("d" * 12 + "-1791400000-" + "e" * 12)
    release = _publish_sealed(queue, tmp_path, clock, "release-of-another-generation",
                              script="stage_release.py", resources=SMALL, generation=foreign)
    assert _roles(queue, release) == ["returns_capacity"]
    assert gclaim("sparky") is None
    held = denial(release, "sparky")
    assert held["reason"] in REASONS and held["evidence"]["gang_election"]["reservation"], held


# --- measurement precedence -------------------------------------------------------------

def test_a_carried_measurement_withhold_does_not_hold_back_an_aged_gang_member(
        gang_fleet, monkeypatch, tmp_path):
    """The review's carried-withhold path: a measurement's carried episode is a measurement withhold.

    The older measurement single cannot be evaluated this pass (its residency lead record is
    unreadable) and carries its withhold.  Past the bound the gang member behind it still elects.
    """
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


def test_without_the_copy_a_waiting_measurement_keeps_its_place_as_on_main(
        gang_fleet, monkeypatch, tmp_path):
    """No reservation, so no precedence: the measurement withhold is not suspended either."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    monkeypatch.setattr(runtime_publication, "PROTECTED_GENERATION_STORE", tmp_path / "no-such-store")
    incumbents = _busy_both(publish, gclaim)
    measurement = publish("old-measurement", measurement=True, priority=-10, timeout_s=None,
                          cpu=2, gpu=1, mem_gb=8, tags=["sparky"])
    group, (first, second) = members("behind-measurement", priority=-10)
    assert gclaim("sparklina") is None
    assert gclaim("sparky") is None
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    assert gclaim("sparky") is None
    assert denial(second, "sparky")["reason"] == "deferred_behind_withheld_row", denial(second, "sparky")
    assert _gang.elections(queue, group, 2).get(1) is None
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


def test_without_the_copy_a_measurement_single_of_the_gangs_priority_elects_as_on_main(
        gang_fleet, monkeypatch, tmp_path):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    monkeypatch.setattr(runtime_publication, "PROTECTED_GENERATION_STORE", tmp_path / "no-such-store")
    incumbents, group, keys = _wait_gang(publish, gclaim, members, clock, "no-copy-elect")
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    measurement = publish("late-measurement", measurement=True, priority=-10, timeout_s=None,
                          cpu=2, gpu=1, mem_gb=8, tags=["sparky"])
    assert gclaim("sparky") is None
    census = reservation.CensusReader(queue, queue.ledger("sparky")).capture()
    assert measurement in census["elections"], "no reservation, so the host is withheld for it as before"


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


def test_a_running_member_reserves_nothing_more_than_its_ledger_hold(gang_fleet, tmp_path):
    """Review fd78197 finding 1: a running member already holds its demand."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents, group, (first, second) = _wait_gang(publish, gclaim, members, clock, "running")
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    for host in HOSTS:
        finish(incumbents[host], host)
    started = set()
    for _ in range(3):
        for host in HOSTS:
            claimed = gclaim(host)
            if claimed is not None:
                started.add(claimed)
    assert started == {first, second}, (started, denial(first, "sparklina"), denial(second, "sparky"))
    small = _publish_sealed(queue, tmp_path, clock, "small-beside-running", script=None, resources=SMALL)
    assert gclaim("sparky") == small, denial(small, "sparky")


def test_a_finished_member_with_a_live_sibling_reserves_nothing(gang_fleet, tmp_path):
    """Review fd78197 finding 1: a terminal member holds nothing, not the whole host."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents, group, (first, second) = _wait_gang(publish, gclaim, members, clock, "finished")
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    for host in HOSTS:
        finish(incumbents[host], host)
    started = set()
    for _ in range(3):
        for host in HOSTS:
            claimed = gclaim(host)
            if claimed is not None:
                started.add(claimed)
    assert started == {first, second}, (started, denial(first, "sparklina"), denial(second, "sparky"))
    finish(first, "sparklina")
    assert _gang.elections(queue, group, 2)[0]["host"] == "sparklina"
    idle = _publish_sealed(queue, tmp_path, clock, "idle-host-row", script=None, resources=SMALL,
                           tags=("sparklina",))
    assert gclaim("sparklina") == idle, denial(idle, "sparklina")

def test_a_retained_mover_queued_before_the_copy_runs_while_the_copy_is_fresh(
        gang_fleet, store, retained, monkeypatch, tmp_path):
    """Review 305cadf finding 2: a fresh copy grants no authority, so the gang starts."""
    from movement_publication_support import approve as _approve, unseal as _unseal
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    away = store.with_name(store.name + ".away")
    store.rename(away)
    mover, cas, checkout, need, first, second = _whole_cpu_gang(
        gang_fleet, monkeypatch, tmp_path, _tool(retained, "stage_move.py"))
    _enqueue(queue, clock, mover, cas, checkout, resources=need, residency=RANGE)
    assert _roles(queue, mover) == []
    assert queue.item_path(pool.READY, mover).exists()
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    _unseal(retained.parent)
    _approve(retained, monkeypatch)
    assert runtime_publication.live_authority() is False, "a fresh copy is not mature"
    _run_the_mover_and_start_the_gang(gang_fleet, monkeypatch, tmp_path, mover, cas, checkout,
                                      need, first, second, role=None, published=True)

def test_a_mutated_retained_tool_and_import_without_a_new_receipt_stays_ordinary(
        gang_fleet, store, retained, monkeypatch, tmp_path):
    """Review 305cadf finding 1: mutable retained bytes never exempt, even with a mature copy."""
    from movement_publication_support import unseal as _unseal
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    _unseal(retained.parent)
    tool = retained / "tools" / "fleet" / "stage_move.py"
    tool.chmod(0o644)
    tool.write_text("# tampered mover\n")
    tool.chmod(0o444)
    helper = retained / "src" / "prismabuild" / "helper.py"
    helper.chmod(0o644)
    helper.write_text("VALUE = 2\n")
    helper.chmod(0o444)
    mover, cas, checkout, need, first, second = _whole_cpu_gang(
        gang_fleet, monkeypatch, tmp_path, str(tool))
    _enqueue(queue, clock, mover, cas, checkout, resources=need, residency=RANGE)
    assert _roles(queue, mover) == [], "a retained path derives no role"
    assert gclaim("sparky") is None
    held = denial(mover, "sparky")
    assert held["reason"] in REASONS, held
    assert "reservation" in held["evidence"]["gang_election"], held


def test_a_mover_sealed_by_the_previous_sealer_runs_while_the_copy_is_fresh(
        gang_fleet, store, retained, monkeypatch, tmp_path):
    """Review 305cadf finding 2: the upgrade must not deadlock existing prerequisite movers."""
    from movement_publication_support import approve as _approve, unseal as _unseal
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    away = store.with_name(store.name + ".away")
    store.rename(away)
    try:
        mover, cas, checkout, need, first, second = _whole_cpu_gang(
            gang_fleet, monkeypatch, tmp_path, _tool(retained, "stage_move.py"), isolated=False)
    finally:
        away.rename(store)
    _enqueue(queue, clock, mover, cas, checkout, resources=need, residency=RANGE)
    assert _roles(queue, mover) == [], "the previous sealer lacks isolated Python"
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    _unseal(retained.parent)
    _approve(retained, monkeypatch)
    assert runtime_publication.live_authority() is False, "a fresh copy is not mature"
    _run_the_mover_and_start_the_gang(gang_fleet, monkeypatch, tmp_path, mover, cas, checkout,
                                      need, first, second, role=None, published=True)



def test_a_protected_tool_without_isolated_python_gets_no_role(gang_fleet, store, tmp_path):
    """Review fd78197 finding 4: user-site startup code must not reach the tool."""
    queue, clock, *_ = gang_fleet
    template, cas, checkout = _template(queue, tmp_path)
    python, mover, egress = ma.movement_tools(_tier(store / "tools" / "fleet"))
    action = ma.seal_movement_action(
        template, command=[python, egress, "--pool-root", str(queue.root)],
        demand=SMALL, tags=["sparky"], log_name="isolated.log")
    assert action["params"]["command"][1] == "-I"
    assert ma.capacity_role(action, SMALL, residency=None) is not None
    bare = dict(action)
    bare_command = [python, egress, "--pool-root", str(queue.root)]
    bare_params = dict(action["params"])
    bare_params["command"] = bare_command
    bare["params"] = bare_params
    bare_task = dict(action["task"])
    bare_task["argv"] = [ma.SEALED_ARGV0, "--noprofile", "--norc", "-c",
                         ma.captured_command(bare_command, "isolated.log")]
    bare["task"] = bare_task
    assert ma.capacity_role(bare, SMALL, residency=None) is None

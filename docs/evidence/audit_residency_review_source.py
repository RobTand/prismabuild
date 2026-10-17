#!/usr/bin/env python3
"""Audit the bounded C21 review corrections from Git blobs, without product imports.

This is source evidence only. It executes no tests, collection, or product code.
Only the enumerated substitutions below are accepted; unexplained drift fails.
"""
import argparse
import ast
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys


TESTS = [
    "test_a_chunk_never_cuts_a_manifest_entry.py",
    "test_a_measurement_consumers_movers_are_ordinary_movement_work.py",
    "test_a_mover_is_sealed_at_one_copys_measured_rate.py",
    "test_a_mover_seals_a_movement_environment.py",
    "test_a_stage_mover_declares_the_cpu_and_retries_it_owns.py",
    "test_a_stalled_mover_ends_no_progress.py",
    "test_an_egress_declares_the_cpu_it_owns.py",
    "test_n_consumers_of_one_range_share_one_copy.py",
    "test_the_sealed_ram_leg_is_chunked_per_phase.py",
    "test_the_sealed_stage_leg_is_chunked_per_phase.py",
    "test_the_submitted_window_pins_the_bytes_it_stages.py",
]
OVERRIDE_FIXTURES = {"test_a_stage_mover_declares_the_cpu_and_retries_it_owns.py",
                     "test_an_egress_declares_the_cpu_it_owns.py"}
RP = "src/prismabuild/residency_plan.py"
STORE = "src/prismabuild/record_store.py"
SUB = "src/prismabuild/submission.py"
TIER = "tools/fleet/tier_loop.py"
MAN = "tools/fleet/manifest_promotion.py"
DEAD = "src/prismabuild/window_credit.py"
TOOL = "tools/maintenance/finish_residency_callers.py"
DOC = "docs/design.md"
WHITELIST = {RP, STORE, SUB, TIER, MAN, DEAD, TOOL, DOC,
             *("tests/" + name for name in TESTS)}
LAYOUT = {"RESIDENCY_SUPERSEDED": "superseded", "RESIDENCY_SHARED": "shared",
          "RESIDENCY_SHARED_MOVERS": "movers"}
DOC_APPEND = '''

The cycle-local residency question requires an explicit `WindowBudget.free_gib`
for `ConsumerWindow.decision`. An absent budget raises `TypeError`; it grants
no zero-capacity observation. `ConsumerWindow.advance` and the geometric
`pre_progress_stage_reach` need no free-capacity probe. The stage and RAM,
publication and pressure captures remain independent. Manifest promotion
constructs `FilingOptions` and `RowPublication` directly. Its separate standard
submission request serves only the unchanged C14 filesystem-floor context.
Physical residency directory names belong to RecordStore; RP keeps the domain
registry questions and interpretation above its captures.
'''


def require(condition, message):
    if not condition:
        raise ValueError(message)


def dump(node):
    return ast.dump(node, include_attributes=False)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def replace_exact(text, old, new, count=1):
    require(text.count(old) == count, f"substitution count {old!r}: {text.count(old)} != {count}")
    return text.replace(old, new)


def definition(tree, *names):
    node = tree
    for name in names:
        matches = [n for n in node.body if isinstance(n, (ast.FunctionDef,
                   ast.AsyncFunctionDef, ast.ClassDef)) and n.name == name]
        require(len(matches) == 1, f"expected exactly one definition: {names}")
        node = matches[0]
    return node


def remove_definition(tree, name):
    node = definition(tree, name)
    tree.body.remove(node)
    return node


def effect_body(node):
    body = copy.deepcopy(node.body)
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
        body.pop(0)
    return ast.Module(body=body, type_ignores=[])


def inventory(tree):
    """Ordered occurrences, never a name-keyed map, preserve setters/duplicates."""
    result = []
    def visit(node, scope):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            scope = scope + [node.name]
            result.append({"kind": type(node).__name__, "path": ".".join(scope),
                           "line": node.lineno,
                           "decorators": [dump(d) for d in node.decorator_list]})
        for child in ast.iter_child_nodes(node):
            visit(child, scope)
    visit(tree, [])
    assertions = [dump(n) for n in ast.walk(tree) if isinstance(n, ast.Assert)]
    return {"definitions": result, "assertion_count": len(assertions),
            "ordered_assertions_sha256": sha(json.dumps(assertions).encode())}


def defaults(tree, name):
    return {n.target.id: n.value for n in definition(tree, name).body
            if isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name)}


class Substitute(ast.NodeTransformer):
    def __init__(self, mapping):
        self.mapping = {dump(ast.parse(k, mode="eval").body): ast.parse(v, mode="eval").body
                        for k, v in mapping.items()}
        self.counts = {k: 0 for k in self.mapping}

    def visit(self, node):
        key = dump(node)
        if key in self.mapping:
            self.counts[key] += 1
            return copy.deepcopy(self.mapping[key])
        return super().visit(node)


def substitute(tree, mapping, counts):
    transform = Substitute(mapping)
    tree = transform.visit(tree)
    require(list(transform.counts.values()) == counts,
            f"AST substitution counts: {list(transform.counts.values())} != {counts}")
    return tree


def audit(checkout, base, head):
    def git(*args):
        return subprocess.check_output(["git", "-C", str(checkout), *args])
    base = git("rev-parse", "--verify", base + "^{commit}").decode().strip()
    head = git("rev-parse", "--verify", head + "^{commit}").decode().strip()
    def tree(ref):
        rows = git("ls-tree", "-rz", "--full-tree", ref).split(b"\0")
        return {name.decode(): (meta.split()[0].decode(), meta.split()[2].decode())
                for row in rows if row for meta, name in [row.split(b"\t", 1)]}
    bt, ht = tree(base), tree(head)
    changed = {p for p in bt.keys() | ht.keys() if bt.get(p) != ht.get(p)}
    require(changed == WHITELIST, f"tracked diff differs: missing={sorted(WHITELIST-changed)}, extra={sorted(changed-WHITELIST)}")
    require(set(ht)-set(bt) == {TOOL} and not set(bt)-set(ht), "unexpected added/deleted paths")
    require(all(bt[p][0] == ht[p][0] for p in bt.keys() & ht.keys()), "tracked mode changed")
    require(ht[TOOL] == ("100644", "006cf305fbcbda3e28fce94f7977c3d91f241ae8"), "unreviewed replay-tool bytes/mode")
    def blobs(entries):
        # Read the full tree in one Git process; never inspect or import a worktree module.
        ids = sorted({entry[1] for entry in entries.values()})
        output = subprocess.check_output(["git", "-C", str(checkout), "cat-file", "--batch"],
                                         input=("\n".join(ids)+"\n").encode())
        data, offset = {}, 0
        for oid in ids:
            end = output.index(b"\n", offset)
            header = output[offset:end].split()
            require(header[:2] == [oid.encode(), b"blob"], f"not a blob: {oid}")
            size = int(header[2]); offset = end + 1
            data[oid] = output[offset:offset+size]; offset += size+1
        return {p: data[entry[1]] for p, entry in entries.items()}
    before, after = blobs(bt), blobs(ht)
    parsed = {}
    for ref, files in ((base, before), (head, after)):
        parsed[ref] = {p: ast.parse(data, filename=f"{ref}:{p}")
                       for p, data in files.items() if p.endswith(".py")}
    b = {p: data.decode() for p, data in before.items() if p in changed}
    h = {p: data.decode() for p, data in after.items() if p in changed}
    checks = []
    proofs = []
    def equal(label, left, right):
        ld, rd = dump(left), dump(right)
        require(ld == rd, label + ": unexplained AST drift")
        proofs.append({"claim": label, "base_normalized_ast_sha256": sha(ld.encode()),
                       "head_normalized_ast_sha256": sha(rd.encode())})
    def module_equal(path, left, right):
        equal(path + " whole module", left, right)
    def parse(source):
        return ast.parse(source)

    # Exact directory literals are relocated, with no other Store AST changes.
    store = parse(h[STORE])
    for name, value in LAYOUT.items():
        matches = [n for n in store.body if isinstance(n, ast.Assign) and
                   len(n.targets) == 1 and isinstance(n.targets[0], ast.Name) and n.targets[0].id == name]
        require(len(matches) == 1 and dump(matches[0].value) == dump(ast.Constant(value)), "layout binding changed: " + name)
        store.body.remove(matches[0])
    store = substitute(store, {k: repr(v) for k, v in LAYOUT.items()}, [3, 2, 1])
    module_equal(STORE, parse(b[STORE]), store)

    # The move's complete effect body is compared separately, not inferred by name.
    tier_base = parse(b[TIER]); tier_head = parse(h[TIER])
    old_reach = remove_definition(tier_base, "_pre_progress_reach")
    rp_head = parse(h[RP]); consumer = definition(rp_head, "ConsumerWindow")
    new_reach = definition(rp_head, "ConsumerWindow", "pre_progress_stage_reach")
    expected_signature = parse("@cached_property\ndef pre_progress_stage_reach(self) -> set[str]:\n    pass").body[0]
    for attribute in ("args", "returns", "decorator_list", "type_comment", "type_params"):
        left, right = getattr(new_reach, attribute, None), getattr(expected_signature, attribute, None)
        require(ast.dump(left) == ast.dump(right) if isinstance(left, ast.AST) else
                [dump(x) for x in left] == [dump(x) for x in right] if isinstance(left, list) else left == right,
                "new reach signature/decorator differs: " + attribute)
    moved = substitute(effect_body(new_reach), {
        "self.plan": "plan", "self.accepted_phase": "accepted_phase",
        "remaining": "residency_plan.remaining", "legs_over": "residency_plan.legs_over",
        "_budget_from_step": "residency_plan._budget_from_step"}, [2, 1, 1, 1, 1])
    equal("complete moved reach effect body", effect_body(old_reach), moved)
    consumer.body.remove(new_reach)
    tier_head = substitute(tier_head, {
        "residency_plan.ConsumerWindow(plan, accepted_phase, residency_plan.LegObservation()).pre_progress_stage_reach":
        "_pre_progress_reach(plan, accepted_phase)"}, [1])
    tier_text = b[TIER]
    for old, new, count in [
        (":func:`residency_plan.advance_needs`", ":func:`residency_plan.ConsumerWindow.advance`", 1),
        (":func:`residency_plan.window`", ":func:`residency_plan.ConsumerWindow.decision`", 2),
        (":func:`_pre_progress_reach`", ":attr:`residency_plan.ConsumerWindow.pre_progress_stage_reach`", 1)]:
        tier_text = replace_exact(tier_text, old, new, count)
    tier_base = parse(tier_text); remove_definition(tier_base, "_pre_progress_reach")
    module_equal(TIER, tier_base, tier_head)
    require("ConsumerWindow.decision_owned" not in h[TIER], "nonexistent decision_owned reference")
    require(b[TIER].count("residency_plan.window_owned(") == h[TIER].count("residency_plan.window_owned(") == 2,
            "window_owned callers changed")

    # R7 is the one behavioral change. Accept exactly this first executable guard.
    decision = definition(rp_head, "ConsumerWindow", "decision")
    guard = parse('if self.budget is None:\n    raise TypeError("window decision requires a captured free_gib budget")').body[0]
    require(dump(decision.body[1]) == dump(guard), "R7 guard missing/changed/not first")
    decision.body.pop(1)
    rp_text = b[RP]
    for old, new in [("free_gib: int = 0", "free_gib: int"),
                     ("budget: WindowBudget = WindowBudget()", "budget: WindowBudget | None = None"),
                     (":func:`window`", ":attr:`ConsumerWindow.decision`")]:
        rp_text = replace_exact(rp_text, old, new, 3 if old == ":func:`window`" else 1)
    rp_base = parse(rp_text)
    rp_head = substitute(rp_head, {"_pool.record_store."+k: repr(v) for k, v in LAYOUT.items()}, [1, 1, 1])
    module_equal(RP, rp_base, rp_head)
    checks.append("R7 changes only the required free_gib field, optional budget, and exact leading TypeError guard; advance is unchanged")

    # Dead wrapper deletion is exact; no module normalization is needed.
    dead = parse(b[DEAD]); removed = remove_definition(dead, "decision_needs")
    require(dump(effect_body(removed)) == dump(effect_body(parse('''def decision_needs():
    from prismabuild import residency_plan as plan_mod
    return plan_mod.advance_needs(plan, accepted_phase, published=published, mover_role=mover_role)
''').body[0])), "unexpected deleted wrapper effect")
    module_equal(DEAD, dead, parse(h[DEAD]))
    uses = []
    for path, tree_ in parsed[base].items():
        if path.startswith(("src/", "tools/")):
            uses.extend((path, n.lineno) for n in ast.walk(tree_) if
                        isinstance(n, ast.Call) and ((isinstance(n.func, ast.Name) and n.func.id == "decision_needs") or
                        (isinstance(n.func, ast.Attribute) and n.func.attr == "decision_needs")))
    require(not uses, "deleted wrapper has callers: " + repr(uses))

    # Typed field reads replace exactly four fallback reads.
    sub = b[SUB]
    fallbacks = {"residency_ram": '"auto"', "residency_share": '"auto"',
                 "residency_prefetch_depth_gib": "None", "residency_read_mb_s": "None"}
    for field, default in fallbacks.items():
        sub = replace_exact(sub, f'getattr(args, "{field}", {default})', f"args.{field}")
    module_equal(SUB, parse(sub), parse(h[SUB]))

    # Prove the constructor bindings against actual declarations, without importing them.
    request = defaults(parsed[head]["src/prismabuild/submission_types.py"], "SubmissionRequest")
    options = defaults(parsed[head][RP], "FilingOptions")
    publication = defaults(parsed[head]["src/prismabuild/queue_publication.py"], "RowPublication")
    expected_options = {"ram": "'auto'", "share": "'auto'", "mover_readers": "4", "mover_mem_gb": "1",
                        "mover_max_attempts": "3", "prefetch_depth_gib": "None", "read_mb_s": "None"}
    for field, expected in expected_options.items():
        equal("FilingOptions default " + field, options[field], ast.parse(expected, mode="eval").body)
        equal("SubmissionRequest residency default " + field, request["residency_"+field], options[field])
    for field, expected in {**fallbacks, "priority_reason": "None", "transport": "'pool'", "filesystem_growth_gib": "1"}.items():
        equal("SubmissionRequest adapter/floor default " + field, request[field], ast.parse(expected, mode="eval").body)
    equal("RowPublication priority_reason default", publication["priority_reason"], request["priority_reason"])
    floor = definition(parsed[head][SUB], "write_window")
    equal("C14 whole write_window including decorators", definition(parsed[base][SUB], "write_window"), floor)
    floor_reads = sorted({n.attr for n in ast.walk(floor) if isinstance(n, ast.Attribute) and
                          isinstance(n.value, ast.Name) and n.value.id == "request"})
    require(floor_reads == ["cwd", "filesystem_growth_gib", "transport"], "C14 floor request reads changed")
    equal("unchanged cwd default factory", request["cwd"], defaults(parsed[base]["src/prismabuild/submission_types.py"], "SubmissionRequest")["cwd"])

    # Exact planner binding substitutions; complete enclosing function/module still compared.
    planner = definition(parse(b[MAN]), "planner_args")
    expected_planner = parse('''def planner_args(**overrides: object) -> SubmissionRequest:
    args = SubmissionRequest(residency_ram="auto", residency_share="auto",
        residency_mover_max_attempts=3, residency_mover_mem_gb=1, residency_mover_readers=4,
        priority=-10, max_attempts=3, retry_safe=True)
    return replace(args, **overrides)
''').body[0]
    equal("deleted planner complete executable body", effect_body(planner), effect_body(expected_planner))
    planner_request = next(n.value for n in effect_body(planner).body if isinstance(n, ast.Assign))
    planner_values = dict(request)
    planner_values.update({k.arg: k.value for k in planner_request.keywords})
    planner_values["priority"] = ast.Name(id="priority", ctx=ast.Load())
    def adapter_projection(function_name, values):
        function = definition(parsed[base][SUB], function_name)
        body = effect_body(function).body
        require(len(body) == 1 and isinstance(body[0], ast.Return), "adapter is not one return: " + function_name)
        class BindFields(ast.NodeTransformer):
            def visit_Call(self, node):
                if isinstance(node.func, ast.Name) and node.func.id == "getattr" and len(node.args) == 3 and dump(node.args[0]) == dump(ast.Name(id="args", ctx=ast.Load())):
                    require(isinstance(node.args[1], ast.Constant) and isinstance(node.args[1].value, str), "nonliteral adapter field")
                    return copy.deepcopy(values.get(node.args[1].value, node.args[2]))
                return self.generic_visit(node)
            def visit_Attribute(self, node):
                if isinstance(node.value, ast.Name) and node.value.id == "args":
                    require(node.attr in values, "missing old explicit adapter field: " + node.attr)
                    return copy.deepcopy(values[node.attr])
                return self.generic_visit(node)
        return BindFields().visit(copy.deepcopy(body[0].value))
    def manifest_call(name):
        calls = [n for n in ast.walk(definition(parsed[head][MAN], "promote_ready_manifest_rows"))
                 if isinstance(n, ast.Call) and ast.unparse(n.func) == name]
        require(len(calls) == 1, "expected single direct planner constructor: " + name)
        return copy.deepcopy(calls[0])
    direct_options = manifest_call("residency_plan.FilingOptions")
    require(not direct_options.args and not direct_options.keywords, "planner options no longer default-only")
    direct_options.keywords = [ast.keyword(arg=k, value=copy.deepcopy(v)) for k, v in options.items()]
    equal("planner complete FilingOptions projection from actual old adapter and defaults",
          adapter_projection("filing_options", planner_values), direct_options)
    direct_publication = manifest_call("RowPublication")
    require(not direct_publication.args and "priority_reason" not in [k.arg for k in direct_publication.keywords], "planner publication default changed")
    direct_publication.keywords.append(ast.keyword(arg="priority_reason", value=copy.deepcopy(publication["priority_reason"])))
    equal("planner complete RowPublication projection from actual old adapter and defaults",
          adapter_projection("row_publication", planner_values), direct_publication)
    planner_floor_overrides = set(k.arg for k in planner_request.keywords) & set(floor_reads)
    require(not planner_floor_overrides, "old planner overrides C14 floor fields")
    expected = b[MAN]
    expected = replace_exact(expected, "from prismabuild.submission_types import SubmissionRequest, SubmissionRefused",
                             "from prismabuild.submission_types import SubmissionRequest, SubmissionRefused\nfrom prismabuild.queue_publication import RowPublication")
    expected = replace_exact(expected, "from prismabuild import submission\nfrom prismabuild import submission", "from prismabuild import submission")
    expected = replace_exact(expected, "args = planner_args(\n                priority=-10 if raw_priority is None else int(raw_priority))",
                             "priority = -10 if raw_priority is None else int(raw_priority)")
    expected = replace_exact(expected, "submission.write_window(args, store=store)", "submission.write_window(SubmissionRequest(), store=store)")
    expected = replace_exact(expected, "options=submission.filing_options(args)", "options=residency_plan.FilingOptions()")
    expected = replace_exact(expected, "publication=submission.row_publication(args, store)", '''publication=RowPublication(
                        queue_root=store.queue_root, cas_root=store.cas_root,
                        worker_script=store.runtime_root / "tools" / "prismabuild_worker.py",
                        priority=priority, max_attempts=3, retry_safe=True)''')
    man_base = parse(expected); remove_definition(man_base, "planner_args")
    module_equal(MAN, man_base, parse(h[MAN]))
    # RowPublication's projection and fallback remain the old exact function.
    equal("row_publication whole function", definition(parsed[base][SUB], "row_publication"), definition(parsed[head][SUB], "row_publication"))

    fixture_proofs = []
    for name in TESTS:
        path = "tests/" + name
        restored = replace_exact(h[path], "args = submit_core.SubmissionRequest(", "args = types.SimpleNamespace(")
        if name in OVERRIDE_FIXTURES:
            restored = replace_exact(restored, "import types\nfrom dataclasses import replace\n", "import types\n")
            restored = replace_exact(restored, "    args = replace(args, **overrides)",
                                     "    for name, value in overrides.items():\n        setattr(args, name, value)")
        require(restored == b[path], "fixture contains unexplained byte drift: " + path)
        fixture_tree = parse(h[path])
        candidates = [n.value for n in ast.walk(fixture_tree) if isinstance(n, ast.Assign) and
                      any(isinstance(t, ast.Name) and t.id == "args" for t in n.targets) and
                      isinstance(n.value, ast.Call) and ast.unparse(n.value.func) == "submit_core.SubmissionRequest"]
        require(len(candidates) == 1 and not candidates[0].args, "fixture constructor shape: " + path)
        fields = [k.arg for k in candidates[0].keywords]
        require(len(set(fields)) == len(fields) and set(fields) <= request.keys(), "invalid fixture field: " + path)
        owners = [n for n in ast.walk(fixture_tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                  and any(child is candidates[0] for child in ast.walk(n))]
        require(len(owners) == 1, "ambiguous fixture request scope: " + path)
        owner = owners[0]
        parents = {child: parent for parent in ast.walk(owner) for child in ast.iter_child_nodes(parent)}
        request_uses = []
        for node in ast.walk(owner):
            if not isinstance(node, ast.Name) or node.id != "args":
                continue
            parent = parents[node]
            if isinstance(node.ctx, ast.Store):
                require(isinstance(parent, ast.Assign) and len(parent.targets) == 1,
                        "unexpected fixture request store: " + path)
                require(parent.value is candidates[0] or
                        name in OVERRIDE_FIXTURES and dump(parent.value) == dump(ast.parse("replace(args, **overrides)", mode="eval").body),
                        "unexpected fixture request reassignment: " + path)
            else:
                require(isinstance(parent, ast.Call) and parent.args and parent.args[0] is node and
                        ast.unparse(parent.func) in {"submit_core.filing_options", "submit_core.row_publication", "replace"},
                        "frozen fixture request has an unsupported use or mutation: " + path)
                request_uses.append(ast.unparse(parent))
        override_fields = set()
        if name in OVERRIDE_FIXTURES:
            require(owner.args.kwarg and owner.args.kwarg.arg == "overrides", "fixture override binding changed")
            declared = {a.arg for a in owner.args.args + owner.args.kwonlyargs}
            consumed = {"monkeypatch"} if name == "test_an_egress_declares_the_cpu_it_owns.py" else set()
            for node in ast.walk(fixture_tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == owner.name:
                    require(all(k.arg is not None for k in node.keywords), "dynamic fixture overrides are not source-proven")
                    override_fields.update(k.arg for k in node.keywords if k.arg not in declared | consumed)
            require(override_fields <= request.keys(), "override contains non-request fields: " + path)
            # SubmissionRequest has only field initialization and refuse_argument; no init hook can alter replacement values.
            request_methods = [n.name for n in definition(parsed[head]["src/prismabuild/submission_types.py"], "SubmissionRequest").body
                               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
            require(request_methods == ["refuse_argument"], "request initialization hook invalidates pure field replacement proof")
        require(any(isinstance(n, ast.Call) and ast.unparse(n.func) == "submit_core.filing_options" and
                    n.args and isinstance(n.args[0], ast.Name) and n.args[0].id == "args"
                    for n in ast.walk(parse(h[path]))), "fixture lacks filing_options(args): " + path)
        explicit = {k.arg: k.value for k in candidates[0].keywords}
        typed_values = {**request, **explicit}
        equal("fixture FilingOptions default completion " + path,
              adapter_projection("filing_options", explicit), adapter_projection("filing_options", typed_values))
        equal("fixture RowPublication default completion " + path,
              adapter_projection("row_publication", explicit), adapter_projection("row_publication", typed_values))
        fixture_proofs.append({"path": path, "whole_file_bytes_equal_after_constructor_reverse": name not in OVERRIDE_FIXTURES,
                               "whole_file_bytes_equal_after_exact_allowed_reversals": True,
                               "override_loop_replaced": name in OVERRIDE_FIXTURES,
                               "override_fields_at_existing_call_sites": sorted(override_fields),
                               "scoped_request_load_uses": request_uses, "explicit_fields": fields})
    require(h[DOC] == b[DOC] + DOC_APPEND, "design doc changed beyond exact appended contract")
    require(before["src/prismabuild/client.py"] == after["src/prismabuild/client.py"], "SDK file changed")
    sdk = [n for n in parsed[head]["src/prismabuild/client.py"].body if isinstance(n, ast.Assign) and
           any(isinstance(t, ast.Name) and t.id == "SDK_VERSION" for t in n.targets)]
    require(len(sdk) == 1 and dump(sdk[0].value) == dump(ast.Constant(6)), "SDK_VERSION != 6")
    all_tests = [p for p in bt if p.startswith("tests/")]
    unchanged = [p for p in bt if p in ht and bt[p] == ht[p]]
    return {"status": "PASS", "axis": "source only; no test/runtime/deployment credit",
            "audit_script_sha256": sha(Path(__file__).read_bytes()), "audit_python": sys.version,
            "base": base, "head": head, "base_tree": git("rev-parse", base+"^{tree}").decode().strip(),
            "head_tree": git("rev-parse", head+"^{tree}").decode().strip(),
            "checkout_head": git("rev-parse", "HEAD").decode().strip(),
            "tracked_paths_base": len(bt), "tracked_paths_head": len(ht),
            "byte_identical_paths": len(unchanged), "all_python_parsed_base": len(parsed[base]),
            "all_python_parsed_head": len(parsed[head]), "original_test_paths": len(all_tests),
            "byte_identical_original_test_paths": sum(p in unchanged for p in all_tests),
            "changed_files": [{"path": p, "base_sha256": sha(before[p]) if p in before else None,
                               "head_sha256": sha(after[p])} for p in sorted(changed)],
            "exact_ast_proofs": proofs, "fixture_proofs": fixture_proofs,
            "occurrences": {p: {"base": inventory(parsed[base][p]), "head": inventory(parsed[head][p])}
                            for p in sorted(changed-{DOC, TOOL})},
            "checks": checks + ["SDK_VERSION 6 and complete client bytes unchanged",
                "every path outside the exact whitelist is byte-identical, including identity/runtime/schema/digest/hash implementations",
                "all original test paths are byte-identical or differ only in eleven exact constructor tokens and two exact immutable override repairs",
                "whole AST comparisons include ordered duplicate definitions, getter/setter occurrences, assertions and decorators"],
            "limits": ["R7 original-source RED and runtime outcomes are separate evidence; this audit does not verify them",
                       "source strings in the added pinned maintenance tool are not production calls",
                       "no claim of full refactor completion or deployment qualification"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = audit(args.checkout, args.base, args.head)
    except Exception as exc:
        result = {"status": "FAIL", "axis": "source only", "error": str(exc),
                  "base": args.base, "head": args.head}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"status": result["status"], "output": str(args.output),
                      **({"error": result["error"]} if "error" in result else {})}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())

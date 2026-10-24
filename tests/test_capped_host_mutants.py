"""Review r2's sixteen mutations, ported to the required final proof boundary.

Each isolated copy proves import origins before running the #2141 regressions.
Already-admitted child processes execute directly and inherit one native thread.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import warnings

import pytest

ROOT = Path(__file__).resolve().parents[1]
CORE = [
    "tests/test_capped_capacity.py", "tests/test_capped_claim_proof.py",
    "tests/test_capped_host_bench_fences.py", "tests/test_capped_host_offers.py",
    "tests/test_capped_host_readers.py", "tests/test_capped_host_stale_admission.py",
    "tests/test_capped_host_enrollment_cycle.py", "tests/test_capped_host_fences.py",
    "tests/test_capped_host_scope_identity.py", "tests/test_capped_host_units.py",
    "tests/test_capped_partial_host.py", "tests/test_enrolling_worker.py",
    "tests/test_private_claim_admission_compat_1419.py", "tests/test_r13_private_box_state_1542.py",
]
WL = "tools/fleet/worker_loop.py"
HC = "src/prismabuild/host_cap.py"
POOL = "src/prismabuild/pool.py"
BC = "tools/fleet/bench_claim_pass.py"
BR = "tools/fleet/bench_tier_cycle_r13.py"
PRIVATE = '"admission_open": claim_capacity_fence(queue, capacity)}'
SIMULATION = 'return {"capacity": value, "admission_open": claim_capacity_fence(queue, value)}'
LOOP = 'admission_open=claim_capacity_fence(queue, capacity),'
CLAIM = '**private_claim_parameters(queue))'
R13 = 'claimed = queue.claim(owner="bench-write-only", **private_claim_parameters(queue))'
REMINT = ('type(host_cap.enforce_host_cap({}, None, {}))'
          '(capacity, _authority=host_cap._CAPACITY_MINT)')
BENCH_REMINT = (
    '**(lambda p, m: {**p, "capacity": (lambda cls: cls({**dict(p["capacity"]), "mem_gb": 214}, '
    '_authority=m._CAPACITY_MINT))(m.CappedCapacity)})(private_claim_parameters(queue), '
    '__import__("prismabuild.host_cap", fromlist=["x"])))')
# Names and effects match review-r2. Moved sites follow the same check into
# host_cap and the new proof-requiring rename primitive; none is silently skipped.
MUTANTS = {
    "r1_fence_dict": [(HC, 'fresh = recheck_host_cap(capacity, detail=detail)', 'fresh = dict(capacity)')],
    "fence_box_none": [(HC, 'return enforce_host_cap(capacity, box, held, detail=detail)',
                        'return enforce_host_cap(capacity, None, held, detail=detail)')],
    "producer_box_none": [(WL, 'capacity = host_cap.enforce_host_cap(capacity, box, held, detail=args.host_cap_detail)',
                           'capacity = host_cap.enforce_host_cap(capacity, None, held, detail=args.host_cap_detail)')],
    "private_no_fence": [(WL, PRIVATE, '"admission_open": None}')],
    "simulation_no_fence": [(WL, SIMULATION, 'return {"capacity": value, "admission_open": None}')],
    "loop_maintenance_only": [(WL, LOOP, 'admission_open=lambda: role_session.read_maintenance_gate() is None,')],
    "simulation_remint": [(WL, 'value = host_cap.enforce_host_cap(capacity, box, queue.ledger().held, detail=detail)',
                           'value = ' + REMINT)],
    "bench_claim_pass_no_fence": [(BC, CLAIM, '**{k: v for k, v in private_claim_parameters(queue).items() if k != "admission_open"})')],
    "bench_r13_no_fence": [(BR, R13, R13.replace(CLAIM, '**{k: v for k, v in private_claim_parameters(queue).items() if k != "admission_open"})'))],
    "bench_claim_pass_remint": [(BC, CLAIM, BENCH_REMINT)],
    "offer_declared_uncapped": [(WL, 'if box is not None and "cap" in box.entry:\n        cap = host_cap.HostCap.parse',
                                 'if False:\n        cap = host_cap.HostCap.parse')],
    "commit_fence_dropped": [(POOL, 'proof = host_cap.check_claim_fence(scan_pass.admission_open, scan_pass.capacity, self, scan_pass, key)',
                              'proof = None')],
    "type_check_disabled": [(HC, 'if type(value) is not kind or registered is None or registered[0]() is not value:', 'if False:')],
    "scope_parent_from_broker": [("src/prismabuild/resource_scope.py", 'parent = declared_scope_parent(host)', 'parent = path.parent.name')],
    "control_unbound": [("src/prismabuild/resource_scope.py", 'or not scope_path_matches(control["cgroup_path"], unit, host=host)\n', 'or False\n')],
    "reconcile_local_host": [("src/prismabuild/pool_reconcile.py", 'scope_path_matches(scope["cgroup_path"], unit, host=host)',
                              'scope_path_matches(scope["cgroup_path"], unit)')],
    # New B1 mutations replace the required capability at each entrypoint
    # with a truthy value. A truthiness-only or optional guard accepts these.
    "b1_private_truthy": [(WL, PRIVATE, '"admission_open": True}')],
    "b1_simulation_truthy": [(WL, SIMULATION, 'return {"capacity": value, "admission_open": True}')],
    "b1_ordinary_truthy": [(WL, LOOP, 'admission_open=True,')],
    "b1_bench_claim_truthy": [(BC, CLAIM, '**{**private_claim_parameters(queue), "admission_open": True})')],
    "b1_bench_r13_truthy": [(BR, R13, R13.replace(CLAIM, '**{**private_claim_parameters(queue), "admission_open": True})'))],
    "n1_alias_constructor": [(BC, '    queue = pool.PoolQueue(work / "pb-queue")',
                               '    from prismabuild import host_cap\n'
                               '    def remint(capacity):\n        cls = host_cap.CappedCapacity\n        return cls(capacity)\n'
                               '    queue = pool.PoolQueue(work / "pb-queue")')],
    "b1_proof_not_consumed": [(POOL, 'host_cap.consume_claim_fence(proof, scan_pass.capacity, self, scan_pass, tiers.host.candidate.key)', 'pass')],
    "n2_reader_local_host": [("src/prismabuild/action_result.py", '_resource_scope._checked_control_identity(key, scope, host=host)',
                             '_resource_scope._checked_control_identity(key, scope)')],
}

ORIGIN = '''from pathlib import Path
import importlib.util
import prismabuild.host_cap, prismabuild.pool, prismabuild.pool_reconcile, prismabuild.resource_scope
import prismabuild.action_result, worker_loop
ROOT = Path(__file__).resolve().parents[1]
def test_mutant_import_origins():
    for module in (prismabuild.host_cap, prismabuild.pool, prismabuild.pool_reconcile,
                   prismabuild.resource_scope, prismabuild.action_result, worker_loop):
        assert Path(module.__file__).resolve().is_relative_to(ROOT), module.__file__
    for name in ("bench_claim_pass", "bench_tier_cycle_r13"):
        assert Path(importlib.util.find_spec(name).origin).resolve().is_relative_to(ROOT)
'''


def _run_mutant_suite(target, files, tmp_path, label):
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-o", "addopts=", "-p", "no:cacheprovider", "--tb=line",
         "--basetemp", str(tmp_path / label), *files,
         "-k", "not test_builder_tests_kill_a_final_claim_cap_bypass"],
        cwd=target, capture_output=True, text=True, timeout=900,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(str(target / p) for p in ("src", "tools/fleet", "tests"))})
    (tmp_path / (label + ".stdout")).write_text(result.stdout)
    (tmp_path / (label + ".stderr")).write_text(result.stderr)
    failed = [line for line in result.stdout.splitlines() if line.startswith(("FAILED ", "ERROR "))]
    return result.returncode, failed, result.stdout.splitlines()[-1:]


@pytest.mark.parametrize("mutant", ["baseline", *sorted(MUTANTS)])
def test_review_mutant_is_killed(tmp_path, mutant):
    target = tmp_path / "source"
    shutil.copytree(ROOT, target, ignore=shutil.ignore_patterns(
        ".git", "__pycache__", ".pytest_cache", ".pbrun-closure.*", "qualification_deps"))
    try:
        for relative, old, new in MUTANTS.get(mutant, []):
            path = target / relative
            source = path.read_text()
            assert source.count(old) == 1, (mutant, relative, "mutation site moved")
            path.write_text(source.replace(old, new))
        origin_file = "tests/test_zz_mutant_origin.py"
        (target / origin_file).write_text(ORIGIN)
        origin = _run_mutant_suite(target, [origin_file], tmp_path, "origin")
        assert origin[0] == 0, ("mutant origin check failed", origin)
        code, failures, summary = _run_mutant_suite(target, CORE, tmp_path, "regressions")
        record = {"mutant": mutant, "origin_verified": True, "returncode": code,
                  "failures": failures, "summary": summary}
        warnings.warn("CAP MUTANT " + json.dumps(record), UserWarning)
        if mutant == "baseline":
            assert code == 0, record
        else:
            assert code == 1 and failures, record
    finally:
        shutil.rmtree(target, ignore_errors=True)

#!/usr/bin/env python3
"""Run deterministic scratch guards against wrong implementations inside PB."""
from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]
TEST = "tests/test_scratch_dedicated_capacity.py"


@dataclass(frozen=True)
class Mutant:
    name: str
    path: str
    before: str
    after: str
    test: str


MUTANTS = (
    Mutant("ordinary-charge", "src/prismabuild/local_scratch.py",
           "dedicated = capacity_roots(variables, pairs=pairs)", "dedicated = frozenset()",
           "test_o6_reserves_dedicated_capacity_without_charging_ordinary_spool"),
    Mutant("unreserved-zfs", "src/prismabuild/scratch_capacity.py",
           'or int(properties["reservation"]) != self.backing["reservation_bytes"]', "or False",
           "test_unreserved_zfs_capacity_never_becomes_an_offer"),
    Mutant("missing-quota", "src/prismabuild/scratch_capacity.py",
           'or int(properties["quota"]) != self.backing["quota_bytes"]', "or False",
           "test_allocation_drift_refuses_both_offer_and_launch[quota-0]"),
    Mutant("holder-credit", "src/prismabuild/scratch_capacity.py",
           'offered = (min(budget.max_gib, total.get(budget.kind, 0), held) if held\n'
           '                           else found["measured_gib"])',
           'offered = found["measured_gib"]',
           "test_a_holder_never_creates_credit_for_unwritten_bytes"),
    Mutant("ordinary-alias", "src/prismabuild/scratch_capacity.py",
           'if any(found["mount"][k] == ordinary[k] for k in ("major_minor", "source")):', "if False:",
           "test_unknown_or_aliasing_backing_never_grants_capacity[ordinary]"),
    Mutant("launch-recheck", "src/prismabuild/pool.py",
           "if local_scratch.capacity_roots(variables):", "if False:",
           "test_launch_rechecks_backing_before_payload_and_retains_the_claim"),
    Mutant("claim-authority", "src/prismabuild/pool.py",
           'if dedicated_scratch:\n'
           '                intent_refusal = "scratch_capacity_unavailable"\n'
           '                if self.scratch_capacity_inputs is None:\n'
           '                    raise PoolContractError("dedicated scratch requires configured root authority")\n'
           '                self.scratch_capacity_inputs.require(request["environment"]["variables"])',
           "if dedicated_scratch:\n                pass",
           "test_publish_and_claim_cannot_spend_unconfigured_root_capacity"),
    Mutant("claim-binding", "src/prismabuild/local_scratch.py",
           "resources is None or isinstance(resources, Mapping)", "True or isinstance(resources, Mapping)",
           "test_dedicated_claim_envelope_does_not_require_ordinary_spool"),
    Mutant("btrfs-independence", "src/prismabuild/scratch_capacity.py",
           'if not isinstance(identity, dict) or identity.get("filesystem_type") != "zfs":',
           "if not isinstance(identity, dict):",
           "test_btrfs_subvolumes_do_not_supply_independent_capacity"),
    Mutant("root-sharing", "src/prismabuild/pool.py",
           'self.last_token_shortage = scratch_capacity.claim_shortage(self, demand)',
           'self.last_token_shortage = None',
           "test_high_index_holder_blocks_new_root_claim_after_remint[committed]"),
)


def main():
    if not re.fullmatch(r"[0-9a-f]{64}", os.environ.get("PRISMABUILD_ACTION_KEY", "")):
        raise SystemExit("submit this check through the published PrismaBuild client")
    report = {"schema": "prismabuild.scratch_capacity_mutants.v1", "cases": [],
              "scope": "component mutation checks only; no native root or hostile qualification"}
    report_path = ROOT / "scratch-capacity-mutants.json"
    with tempfile.TemporaryDirectory(prefix="pb-scratch-mutants-") as temporary:
        for mutant in MUTANTS:
            path = ROOT / mutant.path
            original = path.read_text()
            if original.count(mutant.before) != 1:
                raise RuntimeError(f"mutation location is not unique: {mutant.name}")
            trial = Path(temporary) / mutant.name
            trial.mkdir()
            junit = trial / "outcomes.xml"
            try:
                path.write_text(original.replace(mutant.before, mutant.after))
                env = {**os.environ, "PYTHONPYCACHEPREFIX": str(trial / "pycache"),
                       "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"}
                result = subprocess.run(
                    [sys.executable, "-m", "pytest", f"{TEST}::{mutant.test}",
                     "--junitxml", str(junit)], cwd=ROOT, env=env,
                    capture_output=True, text=True, timeout=180, check=False)
            finally:
                path.write_text(original)
            cases = list(ET.parse(junit).iter("testcase")) if junit.exists() else []
            failed = [case.get("name") for case in cases if case.find("failure") is not None]
            errors = [case.get("name") for case in cases if case.find("error") is not None]
            skipped = [case.get("name") for case in cases if case.find("skipped") is not None]
            killed = (result.returncode == 1 and len(cases) == 1
                      and failed == [mutant.test] and not errors and not skipped)
            report["cases"].append({"name": mutant.name, "selected": mutant.test,
                                    "returncode": result.returncode, "failed": failed,
                                    "errors": errors, "skipped": skipped, "killed": killed})
            report_path.write_text(json.dumps(report, indent=2) + "\n")
            print(f"{mutant.name}: {'killed' if killed else 'FAILED'}", flush=True)
            if not killed:
                return 1
    report["all_killed"] = True
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

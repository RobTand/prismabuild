#!/usr/bin/env python3
"""Plant known breaks and prove the gate's selection catches each one.

For every seed: commit the break on top of BASE, select tests with the same
code the gate uses (bazel-diff and bazel_gate.gate_decision), run the selection
on remote execution, and require a failing target inside it that the seed is
expected to break. Runs on a Remote Bazel runner (docs/bazel.md); it never
pushes the seed commits.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bazel_gate  # noqa: E402

RAISE = '\nraise RuntimeError("seeded break")\n'
FAILING_TEST = '\n\ndef test_seeded_break():\n    assert False, "seeded break"\n'

# (place, path, mutation, labels one of which must fail inside the selection)
SEEDS = [
    ('shared src module', 'src/prismabuild/core.py', ('append', RAISE), ['//tests:test_core']),
    ('leaf src module', 'src/prismabuild/drain_policy.py', ('append', RAISE), ['//tests:test_drain_policy']),
    ('fleet tool', 'tools/fleet/pbrun.py', ('append', RAISE), ['//tests:test_pbrun_']),
    ('maintenance tool', 'tools/duplication_inventory.py', ('append', RAISE), ['//tests:test_duplication_baseline']),
    ('gate tooling (fallback)', 'tools/ci/gate_select.py', ('append', RAISE), ['//tests:test_gate_select']),
    ('test helper', 'tests/admitted_queue_fixture.py', ('append', RAISE), ['//tests:test_']),
    ('test file', 'tests/test_drain_policy.py', ('append', FAILING_TEST), ['//tests:test_drain_policy']),
    ('JSON fixture', 'tests/fixtures/measurement_kernel_denial_1399.json', ('replace', '{"seeded": break'),
     ['//tests:test_measurement_kernel_noise_1399']),
    ('compat golden', 'tests/compat/field_sets.json', ('replace', '[]\n'), ['//tests:test_wire_inventory']),
    ('policy document', 'AGENTS.md', ('replace', 'seeded break\n'), ['//tests:test_agent_compute_routing_policy']),
    ('fleet configuration', 'tools/fleet/fleet_boxes.json', ('replace', '{"seeded": break'), ['//tests:test_']),
]


def mutate(root, path, mutation):
    kind, text = mutation
    target = Path(root, path)
    if not target.is_file():
        raise bazel_gate.GateError(f'seed target {path} is missing; update SEEDS')
    target.write_text(target.read_text() + text if kind == 'append' else text)


def seed_verdict(selected, statuses, expected):
    failing = sorted(label for label in selected if statuses.get(label) != 'PASSED')
    caught = [label for label in failing if any(label.startswith(prefix) for prefix in expected)]
    return {'impacted_tests': len(selected), 'failing_selected': failing, 'caught': bool(caught),
            'example': caught[0] if caught else None}


def table(rows):
    lines = ['| Place | Changed file | Selected tests | Failing selected | Expected failure caught | Example |',
             '|---|---|---:|---:|---|---|']
    for row in rows:
        lines.append(f"| {row['place']} | `{row['path']}` | {row['impacted_tests']} | "
                     f"{len(row['failing_selected'])} | {'yes' if row['caught'] else 'NO'} | "
                     f"`{row['example'] or '-'}` |")
    return '\n'.join(lines)


def run_seed(workspace, base, place, path, mutation, expected):
    workspace.checkout(base)
    mutate(workspace.root, path, mutation)
    subprocess.run(['git', '-c', 'user.name=seed', '-c', 'user.email=seed@invalid', 'commit',
                    '--quiet', '--no-verify', '-am', f'seeded break: {place}'],
                   cwd=workspace.root, check=True)
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=workspace.root, text=True).strip()
    changed, deleted = workspace.changes(base, head)
    forced = bazel_gate.fallback_files(changed | deleted)
    impacted = set() if forced else workspace.bazel_diff(base, head)
    workspace.checkout(head)
    decision = bazel_gate.gate_decision(changed, deleted, impacted, workspace.used_sources(),
                                        workspace.tests(), workspace.hosted_tests())
    statuses = workspace.test(decision['selected'], 'remote', {'ROLE': 'SEEDED_BREAK'}, 'seed')
    return {'place': place, 'path': path, 'selection': decision['selection'],
            **seed_verdict(decision['selected'], statuses, expected)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', default='HEAD')
    args = parser.parse_args()
    workspace = bazel_gate.Workspace()
    base = subprocess.check_output(['git', 'rev-parse', args.base], cwd=workspace.root, text=True).strip()
    rows = [run_seed(workspace, base, *seed) for seed in SEEDS]
    workspace.checkout(base)
    report = table(rows)
    print(report)
    output = Path(os.environ.get('BUILDBUDDY_ARTIFACTS_DIRECTORY', workspace.root / 'gate-artifacts'))
    output.mkdir(parents=True, exist_ok=True)
    (output / 'seeded-breaks.json').write_text(json.dumps({'base': base, 'rows': rows}, indent=2))
    (output / 'seeded-breaks.md').write_text(report + '\n')
    return 0 if all(row['caught'] for row in rows) else 1


if __name__ == '__main__':
    sys.exit(main())

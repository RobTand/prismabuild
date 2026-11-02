#!/usr/bin/env python3
"""History-free impact policy for a dependency graph. Vendored as one stdlib file."""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import statistics
import sys

PERCENT_TOLERANCE = 1.0
FIGURES = {'u_pct': PERCENT_TOLERANCE, 'loc_u_pct': PERCENT_TOLERANCE, 'scc_max': 0}


def canonical(label):
    if label.startswith('@//'):
        return label[1:]
    return label.removeprefix('@@') if label.startswith('@@//') else label


def internal(label):
    return not label.startswith('@')


class Snapshot:
    """Edges point from a consumer to its inputs; loc keys define source files.

    roots maps each test to its entry nodes. contents contains test fingerprints,
    not transitive fingerprints. Adapters own discovery and bazel-diff blame.
    """

    def __init__(self, edges, roots, loc, contents=None):
        self.edges = {node: frozenset(deps) for node, deps in edges.items()}
        self.roots = {test: frozenset(nodes) for test, nodes in roots.items()}
        self.loc = dict(loc)
        self.contents = dict(contents or {})
        self._measurement = None


def read_deps(path, tests, loc, contents=None):
    raw = json.loads(Path(path).read_text())
    if not isinstance(raw, dict):
        raise ValueError('dependencies must be an object of label lists')
    edges = {}
    for node, deps in raw.items():
        if not isinstance(deps, list) or any(not isinstance(dep, str) for dep in deps):
            raise ValueError(f'dependencies for {node} must be a list of labels')
        node = canonical(node)
        if internal(node):
            edges[node] = {canonical(dep) for dep in deps if internal(canonical(dep))}
    return Snapshot(edges, {canonical(t): {canonical(t)} for t in tests},
                    {canonical(k): v for k, v in loc.items() if internal(canonical(k))}, contents)


def closure(edges, roots):
    seen, pending = set(), list(roots)
    while pending:
        node = pending.pop()
        if node not in seen:
            seen.add(node)
            pending.extend(edges.get(node, ()))
    return seen


def components(edges):
    """Iterative Kosaraju, including singleton leaves and disconnected nodes."""
    nodes = set(edges).union(*(set(deps) for deps in edges.values()))
    seen, order = set(), []
    for start in sorted(nodes):
        if start in seen:
            continue
        seen.add(start)
        pending = [(start, iter(edges.get(start, ())))]
        while pending:
            node, children = pending[-1]
            child = next(children, None)
            if child is None:
                order.append(node)
                pending.pop()
            elif child not in seen:
                seen.add(child)
                pending.append((child, iter(edges.get(child, ()))))
    reverse = {node: set() for node in nodes}
    for node, deps in edges.items():
        for dep in deps:
            reverse[dep].add(node)
    seen, groups = set(), []
    for node in reversed(order):
        if node in seen:
            continue
        group, pending = set(), [node]
        seen.add(node)
        while pending:
            member = pending.pop()
            group.add(member)
            for parent in reverse[member]:
                if parent not in seen:
                    seen.add(parent)
                    pending.append(parent)
        groups.append(sorted(group))
    return sorted(groups, key=lambda group: (-len(group), group))


def measure(snapshot):
    if snapshot._measurement is not None:
        return snapshot._measurement
    sources = set(snapshot.loc)
    reach = {test: sorted(closure(snapshot.edges, roots) & sources)
             for test, roots in sorted(snapshot.roots.items())}
    dependents = dict.fromkeys(sorted(sources), 0)
    for files in reach.values():
        for file in files:
            dependents[file] += 1
    count, total_loc = len(reach), sum(snapshot.loc.values())
    sizes = sorted(map(len, reach.values()))
    groups = components(snapshot.edges)
    snapshot._measurement = {
        'tests': count, 'source_files': len(sources), 'reach': reach, 'dependents': dependents,
        'u_pct': 100 * sum(dependents.values()) / (len(sources) * count) if sources and count else 0,
        'loc_u_pct': 100 * sum(snapshot.loc[m] * n for m, n in dependents.items()) / (total_loc * count)
                     if total_loc and count else 0,
        'scc_max': len(groups[0]) if groups else 0, 'scc_members': groups[0] if groups else [],
        'sccs': groups,
        'reach_median': statistics.median(sizes) if sizes else 0,
        'reach_p90': sizes[math.ceil(.9 * len(sizes)) - 1] if sizes else 0,
        'reach_max': max(sizes, default=0),
    }
    return snapshot._measurement


def baseline_record(live, baseline=None):
    record = {key: live[key] for key in FIGURES}
    record['reach_budget'] = math.ceil(live['reach_p90'])
    if baseline:
        record = {key: min(value, baseline.get(key, value)) for key, value in record.items()}
    return record


def regressions(live, baseline):
    return [f'{key} rose from {baseline[key]:g} to {live[key]:g} (tolerance {tolerance:g})'
            for key, tolerance in FIGURES.items() if live[key] > baseline[key] + tolerance + 1e-9]


def stale_entries(live, baseline):
    found = [f'{key} fell from {baseline[key]:g} to {live[key]:g}'
             for key, tolerance in FIGURES.items() if live[key] < baseline[key] - tolerance - 1e-9]
    budget = math.ceil(live['reach_p90'])
    if budget < baseline['reach_budget']:
        found.append(f'reach_budget fell from {baseline["reach_budget"]} to {budget}')
    return found


def allowlist_errors(current, previous):
    errors = []
    for key, reason in sorted(current.items()):
        if not isinstance(reason, str) or not reason.strip():
            errors.append(f'allowlist {key} needs a reason')
        if key not in previous or reason != previous[key]:
            errors.append(f'allowlist may only shrink: {key}')
    return errors


def edge_set(snapshot):
    return {(node, dep) for node, deps in snapshot.edges.items() for dep in deps}


def changes(before, after):
    old, new = measure(before) if before else None, measure(after)
    added = edge_set(after) - (edge_set(before) if before else set())
    growth = {file: count - (old['dependents'].get(file, 0) if old else 0)
              for file, count in new['dependents'].items()
              if count > (old['dependents'].get(file, 0) if old else 0)}
    return added, dict(sorted(growth.items(), key=lambda item: (-item[1], item[0])))


def attribution(after, added, growth):
    reverse = {}
    for node, deps in after.edges.items():
        for dep in deps:
            reverse.setdefault(dep, set()).add(node)
    ancestors = closure(reverse, growth)
    relevant = [(a, b) for a, b in sorted(added) if b in ancestors]
    return ('added edges: ' + (', '.join(f'{a} -> {b}' for a, b in relevant) or 'none')
            + '; fanout grew: ' + (', '.join(f'{m} +{n}' for m, n in list(growth.items())[:5]) or 'none')
            + (f'; {len(growth) - 5} more in graph_impact.fanout_growth' if len(growth) > 5 else ''))


def compare(before, after, baseline, allowlist=None, explain=None, *, record_gains=True):
    """Policy failures, with blame supplied by the graph producer's native tool.

    record_gains=False is only for the baseline writer, which must be allowed to
    record improvements. It still refuses every regression and budget loss.
    """
    allowed = allowlist or {}
    old, live = measure(before) if before else None, measure(after)
    added, growth = changes(before, after)
    errors = []
    default_target = next((test for test, files in live['reach'].items() if growth.keys() & set(files)),
                          next(iter(after.roots), next(iter(after.edges), '')))
    explanations = {}

    def fail(key, message, target=default_target):
        if key in allowed:
            return
        if explain and target:
            if target not in explanations:
                explanations[target] = explain(target)
            message += '\n' + explanations[target]
        errors.append(message)

    for test, reach in live['reach'].items():
        changed = (old is None or test not in old['reach']
                   or after.contents.get(test) != before.contents.get(test)
                   or after.roots[test] != before.roots[test]
                   or any(after.edges.get(root) != before.edges.get(root) for root in after.roots[test]))
        if changed and len(reach) > baseline['reach_budget']:
            fail(f'reach:{test}', f'{test} reach {len(reach)} exceeds budget {baseline["reach_budget"]}', test)
    detail = attribution(after, added, growth)
    for reference in [baseline, *([old] if old else [])]:
        for error in regressions(live, reference):
            key = error.split()[0]
            if not any(e.startswith(error) for e in errors):
                fail(key, error + '; ' + detail)
    old_groups = [set(group) for group in old['sccs']] if old else []
    all_edges = edge_set(after)
    for members in live['sccs']:
        group = set(members)
        cyclic = len(group) > 1 or any((node, node) in all_edges for node in group)
        if old is not None and cyclic and not any(group <= old_group for old_group in old_groups):
            causes = sorted((a, b) for a, b in added if a in group and b in group)
            fail('scc:' + ','.join(members), 'SCC grew or merged: ' + ', '.join(members)
                 + '; added edges: ' + ', '.join(f'{a} -> {b}' for a, b in causes), members[0])
    if record_gains:
        for error in stale_entries(live, baseline):
            fail('baseline:' + error.split()[0], 'record gain with --write-baseline: ' + error)
    return errors


def summary(before, after, budget):
    old, live = measure(before) if before else None, measure(after)
    added, growth = changes(before, after)
    new = sorted(set(after.roots) - (set(before.roots) if before else set()))
    sizes = ', '.join(str(len(live['reach'][t])) for t in new) or 'none'
    u = f'{old["u_pct"]:.1f}%' if old else 'no base available'
    scc = str(old['scc_max']) if old else '?'
    hubs = ', '.join(f'{m} +{n}' for m, n in list(growth.items())[:3]) if growth else 'none to a hub'
    return (f'U {u} → {live["u_pct"]:.1f}%, SCC max {scc} → {live["scc_max"]}, '
            f'new tests {len(new)} (reach {sizes} ≤ budget {budget}), new edges {len(added)}, {hubs}')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--head-deps', required=True, type=Path)
    parser.add_argument('--head-metadata', required=True, type=Path)
    parser.add_argument('--base-deps', type=Path)
    parser.add_argument('--base-metadata', type=Path)
    parser.add_argument('--baseline', required=True, type=Path)
    parser.add_argument('--allowlist', type=Path)
    parser.add_argument('--base-allowlist', type=Path)
    parser.add_argument('--write-baseline', action='store_true')
    args = parser.parse_args(argv)

    def load(deps, metadata):
        meta = json.loads(metadata.read_text())
        return read_deps(deps, meta['tests'], meta['loc'], meta.get('contents'))

    head = load(args.head_deps, args.head_metadata)
    base = load(args.base_deps, args.base_metadata) if args.base_deps else None
    recorded = json.loads(args.baseline.read_text())
    baseline = recorded['graph_impact']
    allowed = json.loads(args.allowlist.read_text()) if args.allowlist else {}
    previous = json.loads(args.base_allowlist.read_text()) if args.base_allowlist else {}
    print(summary(base, head, baseline['reach_budget']))
    errors = allowlist_errors(allowed, previous)
    errors += compare(base, head, baseline, allowed, record_gains=not args.write_baseline)
    if errors:
        prefix = 'refusing to record a regression:\n' if args.write_baseline else ''
        print(prefix + '\n'.join(errors), file=sys.stderr)
        return 1
    if args.write_baseline:
        recorded['graph_impact'] = baseline_record(measure(head), baseline)
        args.baseline.write_text(json.dumps(recorded, indent=2) + '\n')
    return 0


if __name__ == '__main__':
    sys.exit(main())

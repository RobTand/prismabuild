"""PB #2789's pinned, recorded-import exception to structural attribution.

The shared graph core, actual snapshots and all budgets stay unchanged. Only
the exact measured added edges lose structural attribution, after every bound
is checked. Missing, edited or outgrown policy never grants an exception.
"""
from __future__ import annotations

import os
import stat

ADOPTION_SHA256 = '7a04ed0fe284f0284974f66c56ce55d61e9ef744138252c9891857f09073e8ce'
MAX_ADOPTION_BYTES = 1024 * 1024


def read_first_adoption_manifest(path, digests):
    """Verify a bounded regular policy file using PB's existing digest owner.

    sha256 covers canonical finite JSON excluding its own sha256 field. The
    code pin prevents a manifest-only change from expanding the grant.
    """
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, 'rb') as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError('graph adoption manifest must be a regular file')
        raw = stream.read(MAX_ADOPTION_BYTES + 1)
    if len(raw) > MAX_ADOPTION_BYTES:
        raise ValueError('graph adoption manifest exceeds its byte bound')
    manifest = digests.decode_strict_json(raw, where='graph adoption manifest')
    if type(manifest) is not dict or manifest.get('sha256') != ADOPTION_SHA256:
        raise ValueError('graph adoption manifest hash is not the reviewed pin')
    payload = {key: value for key, value in manifest.items() if key != 'sha256'}
    if digests.canonical_sha256(payload) != ADOPTION_SHA256:
        raise ValueError('graph adoption manifest differs from the reviewed payload')
    return manifest


def adoption_structural_inputs(before, after, inputs, manifest):
    """Return comparison inputs and an auditable grant decision; never mutate.

    Every other policy still receives the real graph. Admit recorded import subsets;
    reject unrecorded typed edges, removals, unrecorded consumers, excessive source or
    test deltas, and new/merged SCCs before excluding attribution edges.
    """
    decision = {'manifest_sha256': manifest['sha256'], 'applied': False, 'admitted_edges': 0}
    changes = inputs['edge_diff']
    imports = {tuple(edge) for edge in manifest['imports']}
    added_rules = {tuple(edge) for edge in changes['added']['rule_to_rule']}
    targets = {target for _, target in imports}
    if not any(target in targets for _, target in added_rules):
        return inputs, decision, []
    errors = []
    recorded_edges = {kind: set(map(tuple, edges)) for kind, edges in manifest['added_edges'].items()}
    for kind in ('rule_to_rule', 'rule_to_source'):
        unrecorded = set(map(tuple, changes['added'][kind])) - recorded_edges[kind]
        if unrecorded:
            first = min(unrecorded)
            errors.append(f'graph adoption unrecorded {kind} edge: {first[0]} -> {first[1]}')
        if changes['removed'][kind]:
            errors.append('graph adoption does not admit simultaneous edge removals')

    old_tests = before['reach'].keys() & after['reach'].keys()
    existing_sources = before['loc'].keys() & after['loc'].keys()
    new_consumers = {}
    for test in sorted(old_tests):
        gained = set(after['reach'][test]) - set(before['reach'][test])
        for source in gained & existing_sources:
            new_consumers.setdefault(source, set()).add(test)
        if not gained:
            continue
        record = manifest['test_deltas'].get(test)
        if record is None or len(gained) > record['max_added_sources']:
            errors.append(f'graph adoption exceeds recorded reach delta: {test} +{len(gained)}')
        elif not gained <= set(record['sources']):
            errors.append(f'graph adoption has unrecorded reach sources: {test}')
    for source, gained in sorted(new_consumers.items()):
        record = manifest['source_deltas'].get(source)
        if record is None or len(gained) > record['max_added_consumers']:
            errors.append(f'graph adoption exceeds recorded consumer delta: {source} +{len(gained)}')
        elif not gained <= set(record['consumers']):
            errors.append(f'graph adoption has unrecorded consumers: {source}')
    old_components = {node: set(component) for component in before['components'] for node in component}
    if any(len(component) > 1 and not set(component) <= old_components.get(component[0], set())
           for component in after['components']):
        errors.append('graph adoption does not admit SCC growth or merging')
    if errors:
        return inputs, decision, errors

    remaining = {kind: [edge for edge in changes['added'][kind] if tuple(edge) not in recorded_edges[kind]]
                 for kind in ('rule_to_rule', 'rule_to_source')}
    admitted_count = sum(len(changes['added'][kind]) - len(remaining[kind]) for kind in remaining)
    decision.update(applied=True, admitted_edges=admitted_count)
    return {**inputs, 'edge_diff': {**changes, 'added': remaining}}, decision, []

#!/usr/bin/env python3
"""Merge process records into a reproducible, gzip JSON function impact map."""
from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path

SCHEMA = 'prismabuild.tia_map.v1'

try:
    from .tia_bytes import raw_sha256, compact_ascii_json_bytes, sorted_json
except ImportError:
    from tia_bytes import raw_sha256, compact_ascii_json_bytes, sorted_json


def write_impact_map(records: list[Path], destination: Path, *, unmapped: dict | None = None, duration_updates: dict | None = None) -> dict:
    if not records:
        raise ValueError('no recorder output')
    tests: dict[str, set[str]] = {}
    hashes, versions, commits, unattributed = {}, set(), set(), set()
    for path in sorted(records):
        record = json.loads(path.read_text())
        if record['schema'] != 'prismabuild.tia_record.v1':
            raise ValueError(f'unsupported record: {path}')
        unattributed.update(record['unattributed'])
        commits.add(record['source_commit'])
        versions.add(record['python'])
        for name, content_hash in record['files'].items():
            if name in hashes and hashes[name] != content_hash:
                raise ValueError(f'inconsistent source hash: {name}')
            hashes[name] = content_hash
        for node, functions in record['tests'].items():
            tests.setdefault(node, set()).update(functions)
    if len(commits) != 1:
        raise ValueError('records have different source commits')
    functions = sorted({function for values in tests.values() for function in values})
    ids = {name: index for index, name in enumerate(functions)}
    result = {'schema': SCHEMA, 'source_commit': commits.pop(), 'python': sorted(versions),
              'unmapped': dict(sorted((unmapped or {}).items())), 'duration_updates': dict(sorted((duration_updates or {}).items())), 'files': dict(sorted(hashes.items())), 'functions': functions, 'unattributed': sorted(unattributed),
              'tests': {node: sorted(ids[name] for name in values) for node, values in sorted(tests.items())}}
    encoded = (compact_ascii_json_bytes(result).decode() + '\n').encode()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(gzip.compress(encoded, mtime=0))
    return result


def read_impact_map(path: Path) -> dict:
    result = json.loads(gzip.decompress(path.read_bytes()))
    if result['schema'] != SCHEMA:
        raise ValueError('unsupported TIA map schema')
    functions = result['functions']
    for node, ids in result['tests'].items():
        if not node.startswith('tests/') or any(type(index) is not int or not 0 <= index < len(functions) for index in ids):
            raise ValueError(f'invalid function indices or test node: {node}')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('records', type=Path, nargs='+', help='record files or directories')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--unmapped', type=Path, help='JSON test-file reasons and execution evidence')
    args = parser.parse_args()
    paths = [record for path in args.records for record in (sorted(path.glob('*.json')) if path.is_dir() else [path])]
    result = write_impact_map(paths, args.output, unmapped=json.loads(args.unmapped.read_text()) if args.unmapped else None)
    print(sorted_json({'functions': len(result['functions']), 'tests': len(result['tests']),
                      'bytes': args.output.stat().st_size, 'source_commit': result['source_commit']}))


if __name__ == '__main__':
    main()

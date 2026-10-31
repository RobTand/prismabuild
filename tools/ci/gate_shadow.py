#!/usr/bin/env python3
"""Compare shadow selection with complete full-population queue evidence."""
from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import xml.etree.ElementTree as ET


def audit(plan: dict, records: list[dict], junit_paths: list[Path]) -> dict:
    full = {row['path'] for row in plan['selected']}
    predicted = {row['path'] for row in plan.get('shadow_selected', [])}
    problems = []
    if plan.get('mode') != 'full':
        problems.append('accuracy requires a full-population queue plan')
    assigned = Counter(path for record in records for path in record.get('files', []))
    if set(assigned) != full or any(count != 1 for count in assigned.values()):
        problems.append('full-population assignments are missing, unexpected, or duplicated')
    if not records or any(record.get('verdict') not in {'pass', 'fail'} for record in records):
        problems.append('a CI assignment has no test verdict')
    if any(record.get('population_version') != plan['population']['version'] for record in records):
        problems.append('CI results name a different population version')
    if len(junit_paths) != len(records):
        problems.append('each final CI assignment needs its JUnit file')
    failures, observed = set(), set()
    for path in junit_paths:
        try:
            cases = list(ET.parse(path).iter('testcase'))
        except (OSError, ET.ParseError):
            problems.append('missing or invalid JUnit: ' + str(path))
            continue
        if not cases:
            problems.append('JUnit has no test cases: ' + str(path))
        for case in cases:
            filename = case.get('file', '').removeprefix('./')
            if not filename or filename not in full:
                problems.append('JUnit test case has missing or unexpected file identity')
            observed.add(filename)
            if case.find('failure') is not None or case.find('error') is not None:
                if filename:
                    failures.add(filename)
    if observed != full:
        problems.append('JUnit does not cover the full population')
    missed = sorted(failures - predicted)
    verdict = 'inconclusive' if problems else 'missed_failure' if missed else 'no_miss_observed'
    return {'schema': 'prismabuild.gate_shadow_accuracy.v1', 'verdict': verdict,
            'exit_code': 75 if problems else 1 if missed else 0,
            'population_version': plan['population']['version'],
            'full_files': len(full), 'shadow_files': len(predicted),
            'failure_files': len(failures), 'caught_files': sorted(failures & predicted),
            'missed_files': missed, 'problems': sorted(set(problems))}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--results', type=Path, nargs='+', required=True)
    parser.add_argument('--junitxml', type=Path, nargs='+', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        report = audit(json.loads(args.plan.read_text()),
                       [json.loads(path.read_text()) for path in args.results], args.junitxml)
    except (OSError, ValueError, KeyError, TypeError) as error:
        report = {'verdict': 'inconclusive', 'exit_code': 75, 'problems': [str(error)]}
    args.output.write_text(json.dumps(report, sort_keys=True, indent=2) + '\n')
    return report['exit_code']


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
"""Reduce #1917 read-only mountstats and bounded strace captures.

Counts are process syscalls and client-wide RPCs, never per-process RPCs.
Use the original -ttt -yy trace, with clone/fork events and data buffers raw.
"""
from __future__ import annotations

import argparse
import gzip
from collections import Counter
import json
from pathlib import Path
import posixpath
import re
import sys
from typing import NamedTuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fleet.mount_latency import read_mountstats

LINE = re.compile(r"^(\d+)\s+(\d+\.\d+)\s+(.*)$")
START = re.compile(r"^(\w+)\(")
RESUME = re.compile(r"^<\.\.\. (\w+) resumed>(.*)$")
RESULT = re.compile(r"\)\s+=\s+(-?\d+)")


class CompletedCall(NamedTuple):
    pid: str
    entered_at: float
    completed_at: float
    name: str
    result: int
    body: str


def completed_calls(lines):
    pending = {}
    for line in lines:
        match = LINE.match(line)
        if match is None:
            continue
        pid, stamp, body = match.groups()
        entered_at = stamp
        resumed = RESUME.match(body)
        if resumed:
            previous = pending.pop((pid, resumed[1]), None)
            if previous is None:
                continue
            entered_at, prefix = previous
            body = prefix + resumed[2]
        elif '<unfinished ...>' in body:
            call = START.match(body)
            if call:
                pending[pid, call[1]] = (stamp, body.split('<unfinished ...>')[0])
            continue
        call = START.match(body)
        result = RESULT.search(body)
        if call and result:
            yield CompletedCall(pid, float(entered_at), float(stamp),
                                call[1], int(result[1]), body)


def shared_path(body):
    quoted = re.findall(r'"([^"\\]*(?:\\.[^"\\]*)*)"', body)
    if quoted and quoted[0].startswith('/'):
        path = quoted[0]
    elif quoted and quoted[0]:
        base = re.search(r'(?:AT_FDCWD|\b\d+)<([^>]+)>', body)
        if base is None:
            return None
        path = posixpath.join(base[1], quoted[0])
    else:
        descriptor = re.search(r'\b\d+<([^>]+)>', body)
        if descriptor is None:
            return None
        path = descriptor[1]
    path = posixpath.normpath(path)
    return path if path == '/mnt/shared' or path.startswith('/mnt/shared/') else None


def summarize_trace(lines, roles, start, end):
    parents = {}
    counts = Counter()
    for completed in completed_calls(lines):
        pid, _, _, call, result, body = completed
        if call in ('clone', 'clone3', 'fork', 'vfork') and result > 0:
            parents[str(result)] = pid
        if not start <= completed.entered_at < end:
            continue
        path = shared_path(body)
        if path is None:
            continue
        outcome = 'success' if result >= 0 else 'failure'
        counts[pid, call, path, outcome] += 1
    def owner(pid):
        seen = set()
        while pid not in roles and pid in parents and pid not in seen:
            seen.add(pid)
            pid = parents[pid]
        return pid
    rows = []
    for (pid, call, path, outcome), count in sorted(counts.items()):
        root = owner(pid)
        rows.append({'pid': int(pid), 'root_pid': int(root),
                     'role': roles.get(root, 'unidentified'), 'syscall': call,
                     'path': path, 'outcome': outcome, 'count': count,
                     'calls_per_s': count / (end - start)})
    return rows


def rpc_window(samples, mount):
    first, last = samples[0], samples[-1]
    before = read_mountstats(Path(mount), first['mountstats'])
    after = read_mountstats(Path(mount), last['mountstats'])
    if before is None or after is None or not before.is_nfs or not after.is_nfs:
        raise ValueError(f'unavailable NFS counters for {mount}')
    if (before.device, before.mount_point) != (after.device, after.mount_point):
        raise ValueError('mount identity changed')
    seconds = last['monotonic'] - first['monotonic']
    if seconds <= 0 or last['unix'] <= first['unix']:
        raise ValueError('nonadvancing capture clock')
    deltas = {name: after.ops[name]['ops'] - value['ops']
              for name, value in before.ops.items()}
    if any(value < 0 for value in deltas.values()):
        raise ValueError('counter reset')
    return {'mount': before.mount_point, 'device': before.device,
            'started_unix': first['unix'], 'ended_unix': last['unix'],
            'seconds': seconds, 'operations': deltas,
            'rates_per_s': {key: value / seconds for key, value in deltas.items()},
            'total_per_s': sum(deltas.values()) / seconds}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture', type=Path, required=True)
    parser.add_argument('--trace', type=Path)
    parser.add_argument('--roles', type=Path)
    parser.add_argument('--first-sample', type=int, default=0)
    parser.add_argument('--last-sample', type=int, default=-1)
    args = parser.parse_args()
    capture = json.loads(args.capture.read_text())
    all_samples = capture['samples']
    last = args.last_sample if args.last_sample >= 0 else len(all_samples) - 1
    samples = all_samples[args.first_sample:last + 1]
    if len(samples) < 2:
        parser.error('at least two mountstats samples required')
    mounts = [rpc_window(samples, path) for path in
              ('/mnt/shared', '/mnt/shared/prismabuild-fleet/pb-queue')]
    trace = None
    if args.trace:
        if args.roles is None:
            parser.error('--trace requires --roles with initial PID identities')
        roles = json.loads(args.roles.read_text())
        opener = gzip.open if args.trace.suffix == ".gz" else open
        with opener(args.trace, "rt") as stream:
            trace = summarize_trace(stream, roles, samples[0]['unix'], samples[-1]['unix'])
    print(json.dumps({'host': capture['host'], 'mounts': mounts, 'syscalls': trace,
                      'limits': 'Syscalls are not per-process RPCs. Missing attachments, unpaired or nonnumeric returns and boundary-crossing calls are not counted.'}))


if __name__ == '__main__':
    main()

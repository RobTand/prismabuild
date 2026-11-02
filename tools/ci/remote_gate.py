#!/usr/bin/env python3
"""Start the Bazel gate on a BuildBuddy Remote Bazel runner and report its result.

Runs in the GitHub job. The runner executes tools/ci/bazel_gate.py and leaves
gate-result.json as a BuildBuddy artifact; this script fetches it. A failed,
timed-out or disconnected run, or a missing or malformed result, fails here.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import sys
import time
import urllib.error
import urllib.request

API = 'https://app.buildbuddy.io/api/v1/'
IMAGE = ('docker://gcr.io/flame-public/rbe-ubuntu24-04@sha256:'
         '5f0c01e5f6d79e58d7a996330de33aa38ce599bf8431eaf27521e7602daadf50')
RESULT = 'gate-result.json'
# The runner image has Python 3.12 and no uv. The gate parses PrismaBuild's
# sources, which target 3.14, so it runs on a uv-installed 3.14.
UV_VERSION = '0.10.10'
UV_SHA256 = '3e1027f26ce8c7e4c32e2277a7fed2cb410f2f1f9320d3df97653d40e21f415b'
PYTHON = '3.14'
INSTALL = f'''set -euo pipefail
cd "$(mktemp -d)"
curl -fsSLo uv.tar.gz https://github.com/astral-sh/uv/releases/download/{UV_VERSION}/uv-x86_64-unknown-linux-gnu.tar.gz
echo "{UV_SHA256}  uv.tar.gz" | sha256sum -c -
tar -xzf uv.tar.gz
install -D -m 755 uv-x86_64-unknown-linux-gnu/uv "$HOME/.local/bin/uv"
"$HOME/.local/bin/uv" python install {PYTHON}'''
SCHEMA = 'prismabuild.bazel-gate.v1'
# Diagnostic logs, by suffix: each test or build run names its own.
DIAGNOSTICS = ('bazel-remote-grpc.log', 'execution_log.binpb.zst')
DONE = 'COMPLETE_INVOCATION_STATUS'
LOST = 'DISCONNECTED_INVOCATION_STATUS'
# bazel_gate.py's ARM64_EXCLUDED: lanes main mode leaves out of its arm64 pass,
# whose files the arm64 post-merge hosted lane runs instead.
ARM64_HOSTED = ('runner', 'hosted-arm64')


class RemoteError(RuntimeError):
    pass


class BuildBuddy:
    def __init__(self, key, api=API):
        if not key:
            raise RemoteError('BUILDBUDDY_API_KEY is empty')
        self.key, self.api = key, api

    def call(self, method, body, timeout=120, raw=False):
        """JSON in; JSON out, or the raw body for GetFile."""
        request = urllib.request.Request(
            self.api + method, data=json.dumps(body).encode(),
            headers={'x-buildbuddy-api-key': self.key, 'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read() if raw else json.load(response)
        except (urllib.error.URLError, TimeoutError, ValueError) as error:
            raise RemoteError(f'BuildBuddy {method} failed: {error}') from error


def run_request(args):
    if not args.base or not args.head:
        raise RemoteError('empty base or head: the queue-aware refs were unavailable')
    command = ['tools/ci/bazel_gate.py', args.mode, '--base', args.base,
               '--head', args.head, '--branch', args.branch]
    fail_fast = 'off' if args.mode == 'main' else 'batch' if os.environ.get('BATCH_DRAFT') == 'true' else 'pr'
    command.append(f'--fail-fast={fail_fast}')
    return {
        'repo': args.repo,
        'branch': args.branch,
        'commit_sha': args.checkout or args.head,
        'steps': [{'run': INSTALL},
                  {'run': f'"$("$HOME/.local/bin/uv" python find {PYTHON})" ' + shlex.join(command)}],
        'env': {'GIT_REPO_DEFAULT_BRANCH': 'main'},
        'platform_properties': {
            # The runner hosts only the Bazel client; tests go to remote execution.
            'EstimatedCPU': str(args.cpu),
            'EstimatedMemory': args.memory,
            # 1,281 test actions' input trees; see .bazelrc's --nobuild_runfile_links.
            'EstimatedFreeDiskBytes': '50GB',
            'container-image': IMAGE,
            'workload-isolation-type': 'firecracker',
            'recycle-runner': 'true',
        },
        'timeout': f'{args.timeout}m',
        'wait_until': 'STARTED',
    }


def await_invocation(client, invocation_id, deadline, interval=15, clock=time.monotonic, sleep=time.sleep,
         retries=3):
    """Wait for the run's own terminal status. Returns the invocation record."""
    errors = 0
    while True:
        try:
            found = client.call('GetInvocation', {'selector': {'invocation_id': invocation_id},
                                                  'include_artifacts': True}).get('invocation', [])
            errors = 0
        except RemoteError:
            # One API blip must not fail a whole merge-queue batch.
            errors += 1
            if errors > retries or clock() > deadline:
                raise
            sleep(interval)
            continue
        invocation = found[0] if found else {}
        status = invocation.get('invocationStatus')
        if status == DONE:
            return invocation
        if status == LOST:
            raise RemoteError(f'remote run {invocation_id} disconnected')
        if clock() > deadline:
            raise RemoteError(f'remote run {invocation_id} did not finish in time')
        sleep(interval)


def diagnostics(client, invocation):
    for artifact in invocation.get('artifacts', []):
        name = artifact.get('name', '')
        if name.endswith(DIAGNOSTICS):
            yield Path(name).name, client.call('GetFile', {'uri': artifact['uri']}, timeout=300, raw=True)


def fetch(client, invocation, name):
    for artifact in invocation.get('artifacts', []):
        if artifact.get('name') == name:
            return client.call('GetFile', {'uri': artifact['uri']}, timeout=300, raw=True)
    return None


def parse_result(raw, head):
    if raw is None:
        raise RemoteError(f'the remote run left no {RESULT}')
    try:
        result = json.loads(raw)
    except json.JSONDecodeError as error:
        raise RemoteError(f'{RESULT} is not JSON: {error}') from error
    if not isinstance(result, dict) or result.get('schema') != SCHEMA or result.get('head') != head:
        raise RemoteError(f'{RESULT} has the wrong schema or head')
    if result.get('error') is None and result.get('mode') == 'pr' and result.get('verdict') not in (
            'pass', 'fail', 'skipped'):
        raise RemoteError(f'{RESULT} has no verdict')
    return result


def outputs(result):
    """GitHub step outputs. Scopes are ready only when detection succeeded."""
    ready = result.get('error') is None and result.get('mode') == 'pr'
    values = {
        # The hosted lane's files, as tools/bazel/build_files.py names test targets.
        'hosted': json.dumps([label.removeprefix('//').replace(':', '/') + '.py'
                              for label in result.get('hosted', [])] if result.get('error') is None else []),
        'scopes_ready': str(ready).lower(),
        'scopes': ','.join(result.get('scopes', [])) if ready else '',
        'all_scopes': str(bool(result.get('all_scopes'))).lower(),
    }
    if result.get('mode') == 'pr':
        ok = not result.get('error') and not result.get('inputs') and not result.get('aborted_early')
        values['tests'] = 'success' if ok and result.get('verdict') in ('pass', 'skipped') else 'failure'
    return values


def post_merge_record(result, prs, seconds):
    """One machine-readable line per post-merge run."""
    architectures = result.get('architectures', {})
    failures = {arch: info['failed'] for arch, info in architectures.items()}
    missed = {arch: info['missed'] for arch, info in architectures.items()}
    errors = {arch: info['error'] for arch, info in architectures.items() if info.get('error')}
    if result.get('error') or result.get('inputs') or errors:
        verdict = 'gate-error'
    elif missed.get('amd64'):
        verdict = 'detection-miss'
    elif failures.get('amd64'):
        verdict = 'selected-but-failed'
    elif failures.get('arm64'):
        verdict = 'arm64-only'
    else:
        verdict = 'pass'
    return {'merge_sha': result['head'], 'base_sha': result['base'], 'prs': prs,
            'verdict': verdict, 'selection': result.get('selection'),
            'selected': len(result.get('selected', [])), 'tests': result.get('tests'),
            'failures': failures, 'missed': missed, 'errors': errors,
            'excluded': {arch: info.get('excluded', {}) for arch, info in architectures.items()},
            'seconds': round(seconds, 1), 'error': result.get('error'), 'inputs': result.get('inputs')}


def explain(result, prs):
    lines = [f"Bazel gate ({result['mode']}) {result['base'][:12]}..{result['head'][:12]}"]
    if result.get('error'):
        lines.append(f"Detection failed: {result['error']}")
    if result.get('inputs'):
        lines.append('Stale generated inputs, parity drift or growing budgets; fix with '
                     'tools/bazel/generate.sh: ' + '; '.join(result['inputs']))
    if result['mode'] == 'pr':
        lines.append(f"Selection: {result.get('selection')}, {len(result.get('selected', []))} tests")
        lines.extend(f'FAILED: {label}' for label in result.get('failed', []))
        if result.get('aborted_early'):
            failure = result['aborted_early']
            lines.append(f"Aborted early: {failure['label']} ({failure['reason']})")
            lines.extend(f'NOT RUN: {label}' for label in result.get('not_run', []))
    else:
        lines.append(f"Merged PRs: {', '.join(f'#{pr}' for pr in prs) or 'unknown'}")
        for arch, info in result.get('architectures', {}).items():
            if info.get('error'):
                lines.append(f"{arch}: {info['error']}")
            if info.get('excluded'):
                lines.append(f"{arch} left out, by lane: "
                             + ', '.join(f'{tag} {count}' for tag, count in info['excluded'].items()))
            for label in info['failed']:
                caught = 'not selected: detection miss' if label in info['missed'] else 'was selected'
                if arch == 'arm64':
                    caught = 'arm64 runs only after merge'
                lines.append(f'FAILED on {arch}: {label} ({caught})')
    return '\n'.join(lines)


def hosted_files(root=Path(__file__).resolve().parents[2], arch='amd64'):
    """Test files tools/bazel/nonhermetic.bzl sends to the hosted lane. The lane
    runs them on every gate, beside the Bazel job, so it never waits for it.
    On arm64 (post-merge only) it also runs the ARM64_HOSTED lanes' files."""
    table = (root / 'tools/bazel/nonhermetic.bzl').read_text()
    lanes = {'hosted', *(ARM64_HOSTED if arch == 'arm64' else ())}
    return sorted(f'{package}/{name}.py' for package, name, tags in
                  re.findall(r'"//([\w/]+):(\w+)": struct\(\n        tags = \[([^\]]*)\]', table)
                  if lanes & set(re.findall(r'"([^"]+)"', tags)))


def aggregate(needs, hosted):
    """A required check: the Bazel job finished detection, uploaded scopes and
    passed, and the hosted lane passed its files."""
    gate = needs.get('bazel', {})
    values = gate.get('outputs', {})
    if gate.get('result') not in ('success', 'failure') or values.get('scopes_uploaded') != 'true':
        raise RuntimeError(f'The Bazel gate did not finish detection and scope upload: {gate}')
    if values.get('tests') != 'success':
        raise RuntimeError('The Bazel gate failed: see the Bazel job')
    lane = needs.get('hosted', {}).get('result')
    if hosted and lane != 'success' or not hosted and lane not in ('skipped', 'success'):
        raise RuntimeError(f'The hosted lane did not pass its {len(hosted)} files: {lane}')


def write_github(path_variable, text):
    path = os.environ.get(path_variable)
    if path:
        with open(path, 'a') as handle:
            handle.write(text)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ['hosted-files']:
        lane = argparse.ArgumentParser(prog='remote_gate.py hosted-files')
        lane.add_argument('--arch', choices=['amd64', 'arm64'], default='amd64')
        print(json.dumps(hosted_files(arch=lane.parse_args(argv[1:]).arch)))
        return 0
    if argv[:1] == ['aggregate']:
        try:
            aggregate(json.loads(os.environ['NEEDS_JSON']), hosted_files())
        except (RuntimeError, KeyError, ValueError) as error:
            sys.exit(str(error))
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['pr', 'main'])
    parser.add_argument('--base', default='')
    parser.add_argument('--head', required=True)
    parser.add_argument('--checkout', default='', help='commit the runner clones before the gate fetches HEAD')
    parser.add_argument('--branch', required=True)
    parser.add_argument('--repo', default='https://github.com/RobTand/prismabuild')
    parser.add_argument('--prs', default='', help='comma-separated merged PR numbers')
    parser.add_argument('--cpu', type=int, default=4)
    parser.add_argument('--memory', default='8GB')
    parser.add_argument('--timeout', type=int, default=45, help='minutes')
    parser.add_argument('--output-dir', type=Path, default=Path('gate-output'))
    args = parser.parse_args(argv)
    prs = [int(pr) for pr in args.prs.split(',') if pr]
    started = time.monotonic()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    try:
        client = BuildBuddy(os.environ.get('BUILDBUDDY_API_KEY', ''))
        invocation_id = client.call('Run', run_request(args), timeout=600).get('invocationId')
        if not invocation_id:
            raise RemoteError('BuildBuddy Run returned no invocation id')
        print(f'Remote Bazel run: https://app.buildbuddy.io/invocation/{invocation_id}', flush=True)
        invocation = await_invocation(client, invocation_id, started + (args.timeout + 10) * 60)
        result = parse_result(fetch(client, invocation, RESULT), args.head)
    except RemoteError as error:
        print(f'::error::{error}', flush=True)
        write_github('GITHUB_OUTPUT', 'scopes_ready=false\ntests=failure\n')
        return 1
    (args.output_dir / RESULT).write_text(json.dumps(result, indent=2))
    summary = explain(result, prs)
    print(summary, flush=True)
    write_github('GITHUB_STEP_SUMMARY', '```\n' + summary + '\n```\n')
    write_github('GITHUB_OUTPUT', ''.join(f'{k}={v}\n' for k, v in outputs(result).items()))
    ok = invocation.get('success') and not result.get('error') and not result.get('inputs')
    if args.mode == 'main':
        record = post_merge_record(result, prs, time.monotonic() - started)
        line = json.dumps(record)
        (args.output_dir / 'post-merge-record.jsonl').write_text(line + '\n')
        print('POST_MERGE_RECORD ' + line, flush=True)
        write_github('GITHUB_STEP_SUMMARY', f'\n`POST_MERGE_RECORD {line}`\n')
        ok = ok and record['verdict'] == 'pass'
    else:
        ok = ok and outputs(result)['tests'] == 'success'
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())

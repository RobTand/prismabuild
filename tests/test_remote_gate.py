"""The GitHub side of the Bazel gate fails closed on every missing or malformed piece."""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('remote_gate', ROOT / 'tools/ci/remote_gate.py')
remote_gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(remote_gate)

HEAD = 'b' * 40


def request(**overrides):
    values = dict(mode='pr', base='a' * 40, head=HEAD, checkout='c' * 40, branch='feature',
                  repo='https://github.com/RobTand/prismabuild', cpu=4, memory='8GB', timeout=45)
    values.update(overrides)
    return remote_gate.run_request(argparse.Namespace(**values))


def test_the_runner_checks_out_the_pull_request_and_installs_pinned_tools():
    body = request()
    assert body['commit_sha'] == 'c' * 40 and body['branch'] == 'feature'
    install, gate, final = (step['run'] for step in body['steps'])
    assert f'echo "{remote_gate.UV_SHA256}  uv.tar.gz" | sha256sum -c -' in install
    assert 'python install 3.14' in install
    assert 'tools/ci/bazel_gate.py' in gate and HEAD in gate
    assert 'run_private_gate' in gate and 'scan_artifacts' in final
    assert gate.startswith('\"$HOME/.local/bin/gate-python\" ')
    assert 'python find 3.14)' in install
    assert body['platform_properties']['container-image'] == remote_gate.IMAGE
    assert '@sha256:' in remote_gate.IMAGE


@pytest.mark.parametrize('missing', ['base', 'head'])
def test_empty_queue_aware_refs_fail(missing):
    with pytest.raises(remote_gate.RemoteError, match='empty base or head'):
        request(**{missing: ''})


def test_an_empty_api_key_fails():
    with pytest.raises(remote_gate.RemoteError, match='BUILDBUDDY_API_KEY is empty'):
        remote_gate.BuildBuddy('')


def result(**fields):
    return json.dumps({'schema': remote_gate.SCHEMA, 'mode': 'pr', 'head': HEAD, 'base': 'a' * 40,
                       'error': None, 'inputs': [], 'verdict': 'pass', 'scopes': ['//tests'],
                       'hosted': [], **fields}).encode()


@pytest.mark.parametrize('raw, message', [
    (None, 'left no gate-result.json'),
    (b'{not json', 'not JSON'),
    (result(schema='other'), 'wrong schema or head'),
    (result(head='c' * 40), 'wrong schema or head'),
    (result(verdict=None), 'has no verdict'),
])
def test_a_missing_or_malformed_result_fails(raw, message):
    with pytest.raises(remote_gate.RemoteError, match=message):
        remote_gate.parse_result(raw, HEAD)


def test_outputs_name_the_hosted_files_and_fail_on_stale_inputs():
    values = remote_gate.outputs(json.loads(result(hosted=['//tests:test_pbrun_placement'])))
    assert values['tests'] == 'success' and values['scopes_ready'] == 'true'
    assert json.loads(values['hosted']) == ['tests/test_pbrun_placement.py']
    assert remote_gate.outputs(json.loads(result(inputs=['parity'])))['tests'] == 'failure'
    failed = remote_gate.outputs(json.loads(result(error='bazel-diff exited 1')))
    assert failed['scopes_ready'] == 'false' and failed['tests'] == 'failure' and failed['hosted'] == '[]'


def needs(tests='success', uploaded='true', lane='success', result_='success'):
    return {'bazel': {'result': result_, 'outputs': {'tests': tests, 'scopes_uploaded': uploaded}},
            'hosted': {'result': lane}}


HOSTED = ['tests/test_pbrun_placement.py']


@pytest.mark.parametrize('case, hosted, message', [
    (needs(uploaded=''), HOSTED, 'did not finish detection and scope upload'),
    (needs(result_='cancelled'), HOSTED, 'did not finish detection and scope upload'),
    (needs(tests='failure'), HOSTED, 'The Bazel gate failed'),
    (needs(lane='failure'), HOSTED, 'hosted lane did not pass its 1 files'),
    (needs(lane='skipped'), HOSTED, 'hosted lane did not pass its 1 files'),
    (needs(lane='failure'), [], 'hosted lane did not pass its 0 files'),
])
def test_the_required_checks_fail_closed(case, hosted, message):
    with pytest.raises(RuntimeError, match=message):
        remote_gate.aggregate(case, hosted)


def test_the_required_checks_pass_with_or_without_hosted_files():
    remote_gate.aggregate(needs(), HOSTED)
    remote_gate.aggregate(needs(lane='skipped'), [])


def test_the_hosted_lane_runs_what_nonhermetic_tags_hosted():
    files = remote_gate.hosted_files(ROOT)
    assert files and all(path.startswith('tests/test_') and (ROOT / path).is_file() for path in files)
    table = (ROOT / 'tools/bazel/nonhermetic.bzl').read_text()
    assert table.count('tags = ["hosted"]') == len(files)


def test_the_arm64_hosted_lane_also_runs_what_arm64_executors_cannot_host():
    hosted = remote_gate.hosted_files(ROOT)
    arm64 = remote_gate.hosted_files(ROOT, arch='arm64')
    table = (ROOT / 'tools/bazel/nonhermetic.bzl').read_text()
    extra = sum(table.count(f'tags = ["{tag}"]') for tag in remote_gate.ARM64_HOSTED)
    assert set(hosted) < set(arm64) and len(arm64) == len(hosted) + extra
    assert 'tests/test_git_runners_1307.py' in arm64 and 'tests/test_produced_spool.py' in arm64
    assert remote_gate.hosted_files(ROOT, arch='amd64') == hosted
    assert remote_gate.ARM64_HOSTED == ('runner', 'hosted-arm64')


@pytest.mark.parametrize('architecture', ['amd64', 'arm64'])
def test_root_owned_interpreter_observation_runs_on_hosted_runners(architecture):
    assert 'tests/test_proof_runtime_observation.py' in remote_gate.hosted_files(ROOT, arch=architecture)


class Client:
    def __init__(self, answers):
        self.answers = list(answers)

    def call(self, method, body, **kwargs):
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return {'invocation': [answer]}


def test_waiting_survives_one_api_blip_and_fails_on_disconnect_or_deadline():
    blip = remote_gate.RemoteError('BuildBuddy GetInvocation failed: timeout')
    done = {'invocationStatus': remote_gate.DONE}
    assert remote_gate.await_invocation(Client([blip, done]), 'id', deadline=10, clock=lambda: 0,
                                        sleep=lambda _: None) == done
    with pytest.raises(remote_gate.RemoteError, match='disconnected'):
        remote_gate.await_invocation(Client([{'invocationStatus': remote_gate.LOST}]), 'id', deadline=10,
                                     clock=lambda: 0, sleep=lambda _: None)
    with pytest.raises(remote_gate.RemoteError, match='did not finish in time'):
        remote_gate.await_invocation(Client([{'invocationStatus': 'PARTIAL'}]), 'id', deadline=10,
                                     clock=lambda: 11, sleep=lambda _: None)


def post_merge(amd64=(), missed=(), arm64=(), arm64_error=None, **fields):
    return {'head': HEAD, 'base': 'a' * 40, 'selection': 'impacted', 'selected': ['//tests:test_a'],
            'tests': 3, 'error': None, 'inputs': [], **fields,
            'architectures': {'amd64': {'failed': list(amd64), 'missed': list(missed), 'excluded': {}},
                              'arm64': {'failed': list(arm64), 'missed': [], 'error': arm64_error,
                                        'excluded': {'runner': 49}}}}


@pytest.mark.parametrize('record, verdict', [
    (post_merge(), 'pass'),
    (post_merge(amd64=['//tests:test_b'], missed=['//tests:test_b']), 'detection-miss'),
    (post_merge(amd64=['//tests:test_a']), 'selected-but-failed'),
    (post_merge(arm64=['//tests:test_a']), 'arm64-only'),
    (post_merge(error='bazel-diff exited 1'), 'gate-error'),
    (post_merge(arm64_error='invocation x ran no tests: ERROR: no toolchain'), 'gate-error'),
])
def test_post_merge_records_classify_detection(record, verdict):
    assert remote_gate.post_merge_record(record, [2593], 12.0)['verdict'] == verdict


def test_a_post_merge_record_names_invocation_errors_and_exclusions():
    error = 'invocation x ran no tests: ERROR: no toolchain'
    record = remote_gate.post_merge_record(post_merge(arm64_error=error), [2593], 12.0)
    assert record['errors'] == {'arm64': error}
    assert record['excluded'] == {'amd64': {}, 'arm64': {'runner': 49}}
    summary = remote_gate.explain({**post_merge(arm64_error=error), 'mode': 'main'}, [2593])
    assert f'arm64: {error}' in summary and 'FAILED' not in summary


@pytest.mark.parametrize('mode', ['pr', 'main'])
def test_budget_warnings_are_visible_in_the_summary_without_failing_outputs(mode):
    warning = {'label': '//tests:test_a', 'nodeid': 'tests/test_a.py::test_cost',
               'seconds': 138.0, 'budget_seconds': 20.0}
    if mode == 'main':
        warning['architecture'] = 'arm64'
    record = json.loads(result()) if mode == 'pr' else post_merge()
    record.update(mode=mode, budget_warnings=[warning])
    summary = remote_gate.explain(record, [])
    assert 'BUDGET_WARNING' in summary
    assert '//tests:test_a' in summary and 'tests/test_a.py::test_cost' in summary
    assert '138.000s > 20.000s' in summary
    if mode == 'main':
        assert 'arm64' in summary
        assert remote_gate.post_merge_record(record, [], 0)['verdict'] == 'pass'
    else:
        assert remote_gate.outputs(record)['tests'] == 'success'


@pytest.mark.parametrize('case, expected', [('clean', 0), ('failed-command', 7),
                                         ('leak', 1), ('missing-key', 75), ('zstd-unavailable', 75)])
def test_runner_final_scan_owns_the_actual_upload_directory(case, expected, tmp_path):
    import os
    import shlex
    import subprocess
    import sys
    initial, final = tmp_path / 'initial', tmp_path / 'final'
    initial.mkdir()
    gate = tmp_path / 'fake_gate.py'
    gate.write_text("""import os
from pathlib import Path
assert 'MERGIFY_TOKEN' not in os.environ and 'GITHUB_TOKEN' not in os.environ
assert os.environ['BBRUN_FINAL_SCAN'] == '1'
assert 'BUILDBUDDY_API_KEY' in os.environ
out = Path(os.environ['BUILDBUDDY_ARTIFACTS_DIRECTORY'])
if os.environ['CASE'] == 'zstd-unavailable':
    from compression import zstd
    (out / 'unexpected.binpb.zst').write_bytes(zstd.compress(b'synthetic compressed payload'))
else:
    (out / 'result.json').write_text(os.environ['BUILDBUDDY_API_KEY'] if os.environ['CASE'] == 'leak' else 'safe')
raise SystemExit(7 if os.environ['CASE'] == 'failed-command' else 0)
""")
    steps = remote_gate.secured_steps([str(gate)], str(tmp_path / 'status'))
    environment = {'PATH': os.environ['PATH'], 'HOME': str(tmp_path), 'CASE': case,
                   'BUILDBUDDY_API_KEY': 'synthetic-bb-key', 'MERGIFY_TOKEN': 'synthetic-merge-key',
                   'GITHUB_TOKEN': 'synthetic-gh-key', 'BUILDBUDDY_ARTIFACTS_DIRECTORY': str(initial)}
    # CASE is test input passed in the sealed script, not a new allowlist entry.
    gate.write_text(gate.read_text().replace("os.environ['CASE']", repr(case)))
    first_source = shlex.split(steps[0]['run'])[-1]
    result = subprocess.run([sys.executable, '-I', '-S', '-c', first_source],
                            env=environment, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, 'handoff must reach the required-key final step'
    assert list(initial.iterdir()) == [], 'early step would upload unscanned artifacts'
    environment['BUILDBUDDY_ARTIFACTS_DIRECTORY'] = str(final)
    if case == 'missing-key':
        environment.pop('BUILDBUDDY_API_KEY')
    final_source = shlex.split(steps[1]['run'])[-1]
    if case == 'zstd-unavailable':
        final_source = "import sys; sys.modules['compression'] = None\n" + final_source
    result = subprocess.run([sys.executable, '-I', '-S', '-c', final_source],
                            env=environment, capture_output=True, text=True, timeout=60)
    assert result.returncode == expected
    assert 'synthetic-bb-key' not in result.stdout + result.stderr
    if case in ('clean', 'failed-command'):
        assert (final / 'result.json').read_text() == 'safe'
    else:
        assert list(final.iterdir()) == []


def test_private_diagnostics_are_not_fetched_or_staged_for_github(tmp_path, monkeypatch):
    calls = []
    output = tmp_path / 'github-output'
    class ArtifactsClient:
        def __init__(self, key):
            pass

        def call(self, method, body, **kwargs):
            calls.append((method, body))
            if method == 'Run':
                return {'invocationId': 'synthetic-invocation'}
            if method == 'GetInvocation':
                return {'invocation': [{'invocationStatus': remote_gate.DONE, 'success': True,
                        'artifacts': [
                            {'name': 'pr-execution_log.binpb.zst', 'uri': 'private-zstd'},
                            {'name': 'pr-bazel-remote-grpc.log', 'uri': 'private-grpc'},
                            {'name': remote_gate.RESULT, 'uri': 'safe-result'}]}]}
            assert method == 'GetFile' and body == {'uri': 'safe-result'}, 'raw diagnostic fetch attempted'
            return result()

    monkeypatch.setattr(remote_gate, 'BuildBuddy', ArtifactsClient)
    monkeypatch.setenv('BUILDBUDDY_API_KEY', 'synthetic-key-only')
    monkeypatch.delenv('GITHUB_OUTPUT', raising=False)
    monkeypatch.delenv('GITHUB_STEP_SUMMARY', raising=False)
    assert remote_gate.main(['pr', '--base', 'a' * 40, '--head', HEAD, '--branch', 'feature',
                             '--output-dir', str(output)]) == 0
    assert {path.name for path in output.iterdir()} == {'gate-result.json'}
    assert [body['uri'] for method, body in calls if method == 'GetFile'] == ['safe-result']


@pytest.mark.parametrize('phase', ['gate', 'scan'])
def test_abrupt_exit_never_leaves_unscanned_artifacts_for_upload(phase, tmp_path):
    import os
    import shlex
    import subprocess
    import sys

    initial, final = tmp_path / 'initial', tmp_path / 'final'
    initial.mkdir()
    final.mkdir()
    gate = tmp_path / 'fake_gate.py'
    gate.write_text("import os\nfrom pathlib import Path\n"
                    "out = Path(os.environ['BUILDBUDDY_ARTIFACTS_DIRECTORY'])\n"
                    "(out / 'result.json').write_text('unscanned synthetic result')\n"
                    + ('os._exit(23)\n' if phase == 'gate' else ''))
    status = tmp_path / 'status'
    steps = remote_gate.secured_steps([str(gate)], str(status))
    environment = {'PATH': os.environ['PATH'], 'HOME': str(tmp_path),
                   'BUILDBUDDY_API_KEY': 'synthetic-bb-key',
                   'BUILDBUDDY_ARTIFACTS_DIRECTORY': str(initial)}
    first = subprocess.run([sys.executable, '-I', '-S', '-c', shlex.split(steps[0]['run'])[-1]],
                           env=environment, capture_output=True, text=True, timeout=60)
    assert first.returncode == (23 if phase == 'gate' else 0)
    assert list(initial.iterdir()) == [], 'hard gate exit exposed unscanned artifacts'
    if phase == 'scan':
        environment['BUILDBUDDY_ARTIFACTS_DIRECTORY'] = str(final)
        interrupt = """import os, sys
def stop_scanner(frame, event, arg):
    if frame.f_code.co_filename == '<pinned-artifact-scanner>':
        os._exit(23)
sys.settrace(stop_scanner)
"""
        last = subprocess.run([sys.executable, '-I', '-S', '-c',
                               interrupt + shlex.split(steps[1]['run'])[-1]],
                              env=environment, capture_output=True, text=True, timeout=60)
        assert last.returncode == 23
        assert list(final.iterdir()) == [], 'hard scan exit exposed unscanned artifacts'
    assert (Path(str(status) + '.artifacts') / 'result.json').read_text() == 'unscanned synthetic result'

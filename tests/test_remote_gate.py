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
    install, gate = (step['run'] for step in body['steps'])
    assert f'echo "{remote_gate.UV_SHA256}  uv.tar.gz" | sha256sum -c -' in install
    assert 'python install 3.14' in install
    assert 'tools/ci/bazel_gate.py pr --base' in gate and HEAD in gate
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


def post_merge(amd64=(), missed=(), arm64=(), **fields):
    return {'head': HEAD, 'base': 'a' * 40, 'selection': 'impacted', 'selected': ['//tests:test_a'],
            'tests': 3, 'error': None, 'inputs': [], **fields,
            'architectures': {'amd64': {'failed': list(amd64), 'missed': list(missed)},
                              'arm64': {'failed': list(arm64), 'missed': []}}}


@pytest.mark.parametrize('record, verdict', [
    (post_merge(), 'pass'),
    (post_merge(amd64=['//tests:test_b'], missed=['//tests:test_b']), 'detection-miss'),
    (post_merge(amd64=['//tests:test_a']), 'selected-but-failed'),
    (post_merge(arm64=['//tests:test_a']), 'arm64-only'),
    (post_merge(error='bazel-diff exited 1'), 'gate-error'),
])
def test_post_merge_records_classify_detection(record, verdict):
    assert remote_gate.post_merge_record(record, [2593], 12.0)['verdict'] == verdict

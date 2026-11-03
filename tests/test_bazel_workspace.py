"""Bazel workspace contracts: pinned toolchains and images, BuildBuddy flags, no secrets."""
from __future__ import annotations

import ast
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
CONFIG = (ROOT / '.bazelrc').read_text().splitlines()
MODULE = (ROOT / 'MODULE.bazel').read_text()
PLATFORMS = (ROOT / 'tools/bazel/BUILD.bazel').read_text()


def test_bzlmod_python_and_the_gate_requirements_drive_the_build():
    for required in ['bazel_dep(name = "rules_python"', 'python_version = "3.14"', 'pip.parse(',
                     '"//tools/bazel:requirements.txt": "linux_x86_64,linux_aarch64"',
                     'explicit_init_py(default = True)']:
        assert required in MODULE
    assert 'rules_rust' not in MODULE
    assert re.fullmatch(r'\d+\.\d+\.\d+\n', (ROOT / '.bazelversion').read_text())
    assert (ROOT / 'MODULE.bazel.lock').is_file()


def test_both_executor_platforms_pin_an_ubuntu24_image_by_digest_without_network():
    images = re.findall(r'"container-image": "docker://gcr.io/flame-public/rbe-ubuntu24-04@sha256:[0-9a-f]{64}"',
                        PLATFORMS)
    assert len(images) == 2
    assert PLATFORMS.count('"dockerNetwork": "off"') == 2
    assert PLATFORMS.count('"dockerUser": "1000:1000"') == 2


def test_both_executor_platforms_resolve_a_cc_toolchain():
    # Without one, every target fails analysis on linux_arm64, so the post-merge
    # arm64 run finds no tests (the first run after #2614 did exactly that).
    assert 'bazel_dep(name = "toolchains_buildbuddy"' in MODULE
    for toolchain in ['ubuntu_cc_toolchain', 'ubuntu_cc_toolchain_arm64']:
        assert f'"@buildbuddy_toolchain//:{toolchain}"' in MODULE
    assert PLATFORMS.count('"@bazel_tools//tools/cpp:gcc"') == 2


def test_buildbuddy_suggested_flags_apply_to_every_command():
    for flag in ['--remote_cache_compression', '--experimental_remote_cache_chunking',
                 '--remote_grpc_log=bazel-remote-grpc.log',
                 '--execution_log_compact_file=execution_log.binpb.zst']:
        assert f'common {flag}' in CONFIG
    for flag in ['--noslim_profile', '--experimental_profile_include_target_label',
                 '--experimental_profile_include_primary_output']:
        assert f'build {flag}' in CONFIG


def test_remote_configs_execute_everything_remotely_and_leave_room_for_tl3():
    for mode in ['remote', 'remote-arm64']:
        for flag in ['--remote_download_minimal', '--remote_cache=grpcs://remote.buildbuddy.io',
                     '--remote_executor=grpcs://remote.buildbuddy.io',
                     '--bes_backend=grpcs://remote.buildbuddy.io', '--remote_local_fallback=false',
                     '--nobuild_runfile_links']:
            assert f'build:{mode} {flag}' in CONFIG
        # BuildBuddy's free plan is shared with TL3's gate.
        assert f'build:{mode} --jobs=40' in CONFIG
    assert 'build:remote --platforms=//tools/bazel:linux_amd64' in CONFIG
    for flag in ['--platforms', '--extra_execution_platforms', '--host_platform']:
        assert f'build:remote-arm64 {flag}=//tools/bazel:linux_arm64' in CONFIG
    assert not [line for line in CONFIG if line.startswith('build --jobs')]


def test_both_remote_platforms_route_to_their_self_hosted_pool_without_fallback():
    platforms = {}
    for statement in ast.parse(PLATFORMS).body:
        if not isinstance(statement, ast.Expr) or not isinstance(statement.value, ast.Call):
            continue
        call = statement.value
        if not isinstance(call.func, ast.Name) or call.func.id != 'platform':
            continue
        fields = {keyword.arg: keyword.value for keyword in call.keywords}
        platforms[ast.literal_eval(fields['name'])] = ast.literal_eval(fields['exec_properties'])
    for architecture, config in [('amd64', 'remote'), ('arm64', 'remote-arm64')]:
        assert platforms[f'linux_{architecture}'].get('Pool') == f'selfhosted-{architecture}'
        assert f'build:{config} --remote_local_fallback=false' in CONFIG


def test_ci_refuses_a_stale_module_lockfile_and_keeps_receipts():
    assert 'common:ci --lockfile_mode=error' in CONFIG
    assert 'build:ci --remote_download_regex=.*/test\\.outputs/.*' in CONFIG
    assert 'build:ci --build_metadata=REPO_URL=https://github.com/RobTand/prismabuild' in CONFIG


def test_no_key_is_committed_and_outputs_are_ignored():
    assert not [line for line in CONFIG if 'x-buildbuddy-api-key' in line]
    ignored = (ROOT / '.gitignore').read_text().splitlines()
    for pattern in ['/bazel-*', '/*bazel-remote-grpc.log', '/*execution_log.binpb.zst']:
        assert pattern in ignored


def test_tests_run_with_native_threads_bounded_like_the_hosted_gate():
    for name in ['OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'OPENBLAS_NUM_THREADS']:
        assert f'test --test_env={name}=1' in CONFIG


def test_non_hermetic_tests_carry_a_reason_and_a_known_lane():
    table = (ROOT / 'tools/bazel/nonhermetic.bzl').read_text()
    entries = re.findall(r'"(//[^"]+)": struct\(\n        tags = \[([^\]]*)\],\n        reason = "([^"]+)",', table)
    assert entries and len(entries) == table.count('struct(')
    for label, tags, reason in entries:
        assert set(re.findall(r'"([^"]+)"', tags)) <= {'runner', 'hosted', 'hosted-arm64', 'requires-network'}, label
        assert len(reason) > 20 and 'TBD' not in reason, label

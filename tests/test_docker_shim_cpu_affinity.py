"""A Docker daemon must receive the CPU IDs reserved for its action client."""
from __future__ import annotations

import json
import os

import pytest

from test_docker_shim_global_options import _shim


def _mask(forwarded):
    at = forwarded.index("--cpuset-cpus")
    return {int(value) for value in forwarded[at + 1].split(",")}


@pytest.mark.parametrize("verb", [["run"], ["create"], ["container", "run"],
                                   ["container", "create"]])
def test_inherited_affinity_reaches_container_creation(tmp_path, verb):
    allowed = {min(os.sched_getaffinity(0))}
    result, marked, forwarded = _shim(tmp_path, [*verb, "--rm", "image"],
                                      affinity=allowed)
    assert result.returncode == 0, result.stderr
    assert marked
    assert _mask(forwarded) == allowed


@pytest.mark.parametrize("equals", [False, True])
def test_explicit_cpuset_is_intersected_with_reserved_ids(tmp_path, equals):
    allowed = {min(os.sched_getaffinity(0))}
    outside = max(os.sched_getaffinity(0)) + 1
    wanted = ",".join(map(str, [*allowed, outside]))
    option = ["--cpuset-cpus=" + wanted] if equals else ["--cpuset-cpus", wanted]
    result, _, forwarded = _shim(tmp_path, ["run", *option, "image"], affinity=allowed)
    assert result.returncode == 0, result.stderr
    assert _mask(forwarded) == allowed
    assert sum(token.startswith("--cpuset-cpus") for token in forwarded) == 1


def test_disjoint_cpuset_is_refused_before_creating_a_container(tmp_path):
    outside = max(os.sched_getaffinity(0)) + 1
    result, marked, forwarded = _shim(tmp_path, ["run", "--cpuset-cpus", str(outside), "image"])
    assert result.returncode == 125
    assert "no reserved CPUs" in result.stderr
    assert not marked and forwarded is None


@pytest.mark.parametrize("options", [[], ["-it", "-eVALUE=ok"],
    ["--env", "--cpuset-cpus"], ["--entrypoint", "--cpuset-cpus"], ["--entrypoint", "-lprismabuild.job=999"], ["--"]])
def test_image_command_arguments_are_not_rewritten(tmp_path, options):
    command = ["image", "sh", "-c", "echo --cpuset-cpus=999", "--cpuset-cpus", "999"]
    result, _, forwarded = _shim(tmp_path, ["run", *options, *command])
    assert result.returncode == 0, result.stderr
    assert forwarded[-len(command):] == command
    assert _mask(forwarded) == os.sched_getaffinity(0)


@pytest.mark.parametrize("endpoint,globals,env", [
    ("ssh://worker", ["--context", "remote"], {}),
    ("tcp://worker:2376", ["-Htcp://worker:2376"], {}),
    ("ssh://worker", [], {"DOCKER_CONTEXT": "remote"}),
    ("tcp://worker:2376", [], {"DOCKER_HOST": "tcp://worker:2376"}),
])
def test_remote_daemon_cpu_ids_are_never_guessed(tmp_path, endpoint, globals, env):
    result, marked, forwarded = _shim(tmp_path, [*globals, "run", "image"],
                                      endpoint=endpoint, docker_env=env)
    assert result.returncode == 125
    assert "local Unix" in result.stderr
    assert not marked and forwarded is None


def test_resolved_context_is_pinned_before_creation(tmp_path):
    endpoint = "unix:///run/user/1000/docker.sock"
    result, _, forwarded = _shim(tmp_path, ["--context", "local-rootless", "run", "image"],
                                 endpoint=endpoint)
    assert result.returncode == 0, result.stderr
    assert forwarded[:2] == ["--host", endpoint]
    assert "--context" not in forwarded
    inspected = json.loads((tmp_path / "inspected.json").read_text())
    assert inspected[:2] == ["--context", "local-rootless"]


@pytest.mark.parametrize("requested", ["", "0-99999999999999999999999"])
def test_unrestricted_or_broad_requests_still_use_only_reserved_cpus(tmp_path, requested):
    allowed = {min(os.sched_getaffinity(0))}
    result, _, forwarded = _shim(tmp_path, ["run", "--cpuset-cpus=" + requested, "image"],
                                 affinity=allowed)
    assert result.returncode == 0, result.stderr
    assert _mask(forwarded) == allowed


@pytest.mark.parametrize("requested", ["a", "3-1", "1,", "-1", "1-"])
def test_invalid_cpuset_refuses_before_docker(tmp_path, requested):
    result, marked, forwarded = _shim(tmp_path, ["run", "--cpuset-cpus=" + requested, "image"])
    assert result.returncode == 125
    assert "invalid --cpuset-cpus" in result.stderr
    assert not marked and forwarded is None


def test_repeated_cpuset_uses_docker_last_value_semantics(tmp_path):
    allowed = {min(os.sched_getaffinity(0))}
    result, _, forwarded = _shim(tmp_path, ["run", "--cpuset-cpus=999999",
        "--cpuset-cpus", str(min(allowed)), "image"], affinity=allowed)
    assert result.returncode == 0, result.stderr
    assert _mask(forwarded) == allowed
    assert forwarded.count("--cpuset-cpus") == 1


def test_unknown_container_option_cannot_hide_a_later_cpuset(tmp_path):
    result, marked, forwarded = _shim(tmp_path, ["run", "--future-value-option", "value",
        "--cpuset-cpus=999999", "image"])
    assert result.returncode == 125
    assert "unknown container option" in result.stderr
    assert not marked and forwarded is None


def test_grouped_short_option_cannot_hide_a_reserved_label(tmp_path):
    result, marked, forwarded = _shim(tmp_path, ["run", "-itlprismabuild.job=999", "image"])
    assert result.returncode == 125
    assert "reserved" in result.stderr
    assert not marked and forwarded is None


@pytest.mark.parametrize("option,value", [
    ("--device-read-iops", "/dev/example:100"),
    ("--mac-address", "02:42:ac:11:00:02"),
    ("--stop-timeout", "10"),
    ("--use-api-socket", None),
])
def test_hyphenated_container_options_keep_their_arity(tmp_path, option, value):
    options = [option] if value is None else [option, value]
    result, _, forwarded = _shim(tmp_path, ["run", *options, "image"])
    assert result.returncode == 0, result.stderr
    assert forwarded[-len(options) - 1:] == [*options, "image"]
    assert _mask(forwarded) == os.sched_getaffinity(0)

"""Independent fake-clock probes of the default activity signal boundary."""
import os
from types import SimpleNamespace

import pytest

from test_progress_keeps_a_working_action_alive import _virtual_execution
from test_docker_shim_global_options import _shim
from test_docker_shim_memory import _environment
from prismabuild import pool, resource_scope, core as pb


def _fake_scope(tmp_path):
    root = tmp_path / "aggregate.slice"
    shim = root / "payload.scope"
    container = root / "docker-container.scope"
    for path in (root, shim, container):
        path.mkdir(exist_ok=True)
        (path / "cgroup.procs").write_text(str(os.getpid()))
        (path / "cpu.stat").write_text("usage_usec 10000000\n")
        (path / "memory.events").write_text("oom 0\noom_kill 0\n")
        (path / "memory.events.local").write_text("oom 0\n")
        (path / "memory.current").write_text("1024\n")
        (path / "memory.peak").write_text("2048\n")
    stopped = []
    scope = SimpleNamespace(cgroup_path=root,
                            wrap_argv=lambda argv, **kwargs: argv,
                            terminate_owned=stopped.append)
    return scope, shim, container, stopped


def _execute_contained(child, scope, monkeypatch, telemetry=None):
    monkeypatch.setattr(child.queue, "_start_resource_scope", lambda item: scope)
    monkeypatch.setattr(child.queue, "_sample_resource_scope",
                        lambda scope: telemetry or {"oom_local": 0})
    return child.queue.execute(child.item, timeout_s=7200.0,
                               heartbeat_s=300.0, containment=True,
                               timeout_grace_s=0.2)


@pytest.mark.parametrize("command", [["run", "image"], ["pull", "--quiet", "image"],
                                     ["load", "--input", "archive.tar"]])
def test_real_shim_routes_container_but_permits_daemon_image_work(tmp_path, command):
    environment, cgroup = _environment(tmp_path)
    result, _, forwarded = _shim(tmp_path, command, cgroup=cgroup,
                                  docker_env=environment)
    assert result.returncode == 0, result.stderr
    if command[0] == "run":
        assert forwarded[forwarded.index("--cgroup-parent") + 1] == (
            "prismabuild-job0123456789abcdef0123456789abcdef.slice")
    else:
        assert forwarded == command


def test_container_cpu_in_aggregate_keeps_silent_shim_alive(tmp_path, monkeypatch):
    scope, shim, container, stopped = _fake_scope(tmp_path)
    sampled_paths = []
    read_cgroup = resource_scope.read_cgroup

    def trace(path):
        sampled_paths.append(path)
        return read_cgroup(path)

    monkeypatch.setattr(resource_scope, "read_cgroup", trace)

    def script(child):
        for tick in range(1, 29):
            usec = 10000000 + tick * 1000000
            (container / "cpu.stat").write_text(f"usage_usec {usec}\n")
            # Model Linux's hierarchical counter, including the container.
            (scope.cgroup_path / "cpu.stat").write_text(f"usage_usec {usec}\n")
            yield tick * 300.0

    with _virtual_execution(tmp_path, script, policy=None, ceiling=7200.0,
                            heartbeat_s=300.0) as child:
        outcome = _execute_contained(child, scope, monkeypatch)
        assert child.now == 8400.0 and child.stopped_at is None
    assert outcome["status"] == "executed" and not stopped
    assert outcome["progress_observation"]["cpu_seconds"] == 38.0
    assert int((shim / "cpu.stat").read_text().split()[1]) == 10000000
    assert sampled_paths and set(sampled_paths) == {scope.cgroup_path}


def _advance_unobserved_work(child, scope, progress_file, signal, ticks):
    for tick in range(1, ticks + 1):
        progress_file.write_text(f"{signal}:{tick}")
        (scope.cgroup_path / "io.stat").write_text(
            f"8:0 rbytes={tick * 1000000000} wbytes={tick * 1000000000}\n")
        yield tick * 300.0


@pytest.mark.parametrize("signal", ["gpu", "io", "file", "image-load"])
def test_quiet_external_work_survives_the_former_1800_second_stall(
        tmp_path, monkeypatch, signal):
    """Daemon-side loading can advance while the contained client stays quiet."""
    scope, _, _, stopped = _fake_scope(tmp_path)
    progress_file = tmp_path / "committed-file-progress"
    telemetry = {"oom_local": 0, "gpu_utilization": 100,
                 "gpu_active_seconds": 3000, "io_bytes": 1 << 40}

    def script(child):
        yield from _advance_unobserved_work(child, scope, progress_file, signal, 10)

    with _virtual_execution(tmp_path, script, policy=None, ceiling=7200.0,
                            heartbeat_s=300.0) as child:
        outcome = _execute_contained(child, scope, monkeypatch, telemetry)
        assert child.stopped_at is None and child.now == 3000.0
    assert outcome["status"] == "executed" and not stopped
    assert outcome["stall_allowance_s"] == 7200.0
    assert outcome["progress_observation"]["cpu_seconds"] == 10.0
    assert outcome["progress_observation"]["quiet_s"] == 3000.0
    assert progress_file.read_text() == f"{signal}:10"


@pytest.mark.parametrize("signal", ["gpu", "io", "file"])
def test_external_signals_do_not_renew_the_default_quiet_allowance(
        tmp_path, monkeypatch, signal):
    """These are fake observations; this probe never runs GPU work."""
    scope, _, _, stopped = _fake_scope(tmp_path)
    progress_file = tmp_path / "committed-file-progress"
    telemetry = {"oom_local": 0, "gpu_utilization": 100,
                 "gpu_active_seconds": 8400, "io_bytes": 1 << 40}

    def script(child):
        yield from _advance_unobserved_work(child, scope, progress_file, signal, 28)

    with _virtual_execution(tmp_path, script, policy=None, ceiling=7200.0,
                            heartbeat_s=300.0) as child:
        outcome = _execute_contained(child, scope, monkeypatch, telemetry)
        assert child.stopped_at == 7200.0
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    assert outcome["progress_observation"]["cpu_seconds"] == 10.0
    assert outcome["progress_observation"]["quiet_s"] == 7200.0
    assert progress_file.read_text() == f"{signal}:24"
    assert stopped == ["timeout"]



def test_one_microsecond_of_cpu_per_sample_saves_quiet_gpu_io_and_file_work(
        tmp_path, monkeypatch):
    scope, _, _, stopped = _fake_scope(tmp_path)

    def script(child):
        for tick in range(1, 29):
            (scope.cgroup_path / "cpu.stat").write_text(
                f"usage_usec {10000000 + tick}\n")
            (tmp_path / "result-progress").write_text(str(tick))
            yield tick * 300.0

    with _virtual_execution(tmp_path, script, policy=None, ceiling=7200.0,
                            heartbeat_s=300.0) as child:
        outcome = _execute_contained(child, scope, monkeypatch)
        assert child.stopped_at is None and child.now == 8400.0
    assert outcome["status"] == "executed" and not stopped


def test_periodic_output_saves_daemon_image_load(tmp_path, monkeypatch):
    scope, _, _, stopped = _fake_scope(tmp_path)

    def script(child):
        for tick in range(1, 29):
            child.stderr += f"loaded durable layer {tick}\n"
            yield tick * 300.0

    with _virtual_execution(tmp_path, script, policy=None, ceiling=7200.0,
                            heartbeat_s=300.0) as child:
        outcome = _execute_contained(child, scope, monkeypatch)
        assert child.stopped_at is None and child.now == 8400.0
    assert outcome["status"] == "executed" and not stopped


@pytest.mark.parametrize(("stall_s", "ceiling", "ticks"), [
    (3600.0, 7200.0, 10), (9000.0, 10000.0, 28),
])
def test_sealed_stall_override_allows_shorter_or_longer_quiet_work(
        tmp_path, monkeypatch, stall_s, ceiling, ticks):
    import test_progress_keeps_a_working_action_alive as helpers
    from test_1707_progress_supervision_by_stall import _claimed
    scope, _, _, stopped = _fake_scope(tmp_path)

    def claimed(path, **kwargs):
        return _claimed(path, stall_s=stall_s)

    monkeypatch.setattr(helpers, "_claimed", claimed)

    def script(child):
        for tick in range(1, ticks + 1):
            yield tick * 300.0

    with _virtual_execution(tmp_path, script, policy=None, ceiling=ceiling,
                            heartbeat_s=300.0) as child:
        monkeypatch.setattr(child.queue, "_start_resource_scope", lambda item: scope)
        monkeypatch.setattr(child.queue, "_sample_resource_scope",
                            lambda scope: {"oom_local": 0})
        outcome = child.queue.execute(child.item, timeout_s=ceiling,
                                      heartbeat_s=300.0, containment=True,
                                      timeout_grace_s=0.2)
        assert child.stopped_at is None and child.now == ticks * 300.0
    assert outcome["status"] == "executed" and not stopped
    assert outcome["stall_allowance_requested_s"] == stall_s
    assert outcome["stall_allowance_s"] == stall_s


def test_declared_startup_phase_saves_3000_second_quiet_load(tmp_path, monkeypatch):
    scope, _, _, stopped = _fake_scope(tmp_path)

    def script(child):
        for tick in range(1, 11):
            yield tick * 300.0

    policy = {"schema": pb.PROGRESS_POLICY_SCHEMA_V1,
              "phases": [{"name": "startup", "grace_s": 3600.0}]}
    with _virtual_execution(tmp_path, script, policy=policy, ceiling=7200.0,
                            heartbeat_s=300.0) as child:
        outcome = _execute_contained(child, scope, monkeypatch)
        assert child.stopped_at is None and child.now == 3000.0
    assert outcome["status"] == "executed" and not stopped
    assert outcome["execution_governed_by"] == "progress"


def test_deadline_and_d30_precede_a_simultaneous_default_stall(tmp_path, monkeypatch):
    scope, _, _, stopped = _fake_scope(tmp_path)

    def script(child):
        yield 7200.0
        raise AssertionError("survived coincident deadline, D30 and stall")

    with _virtual_execution(tmp_path, script, policy=None, ceiling=7200.0,
                            timeout_s=7200.0, heartbeat_s=7200.0) as child:
        monkeypatch.setattr(child.queue, "_start_resource_scope", lambda item: scope)
        monkeypatch.setattr(child.queue, "_sample_resource_scope", lambda scope: {
            "oom_local": 1 if child.now >= 7200 else 0})
        outcome = child.queue.execute(child.item, containment=True,
                                      timeout_s=7200.0, heartbeat_s=7200.0,
                                      timeout_grace_s=0.2)
    assert outcome["termination_reason"] == "memory_limit_oom"
    assert stopped == ["memory_limit_oom"]


def test_explicit_deadline_precedes_simultaneous_default_stall(tmp_path):
    def script(child):
        yield 7200.0
        raise AssertionError("survived coincident deadline and stall")

    with _virtual_execution(tmp_path, script, policy=None, ceiling=7200.0,
                            timeout_s=7200.0, heartbeat_s=7200.0) as child:
        outcome = child.execute()
    assert outcome["termination_reason"] == "execution_deadline"


def test_stall_s_is_forwarded_by_the_typed_campaign_owner():
    import pbcampaign
    assert "stall_s" in pbcampaign.KNOWN_FIELDS
    command = pbcampaign.pbrun_argv({"argv": ["true"], "stall_s": 3600.0})
    assert command[command.index("--stall-s") + 1] == "3600.0"

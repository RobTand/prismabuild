"""Every action's receipt carries what the run cost and how loaded the box was.

Tier 0 of issue #372.  Three sources, each named in the record it lands in:

*   ``getrusage(RUSAGE_CHILDREN)``, bracketed around the child this parent
    launched and reaped.  On a contained fleet run that child is the stdio
    proxy, so the block says what it covers rather than claiming to be the
    action.
*   the exact attempt's cgroup and the ``/proc/<pid>/io`` counters of the
    processes inside it, sampled while they live because ``/proc`` is gone
    after the reap.
*   a box window over the seconds the action ran, from the ``pqteld`` flight
    recorder and from Netdata, summarised per field with the source that
    produced it.

None of it is allowed to enter the action key, and none of it may fail the
finish path: a window nobody can read is ``unavailable``, not an exception.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools/fleet"))

from prismabuild import box_window, core as pb, pool, resource_scope  # noqa: E402
import pbstatus  # noqa: E402

MIB = 1024 ** 2


# -- harness ------------------------------------------------------------------

def _claimed(tmp_path: Path, body: str):
    """One claimed item whose action runs ``body`` in its own checkout."""

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text(body)
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/resource-profile",
                 "definition_version": "v1", "task_class": "generation",
                 "determinism": "deterministic", "artifact_family": "generic",
                 "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"],
                 "working_directory": ".", "result_path": "result"},
        "inputs": [],
        "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
                  checkout_root=checkout,
                  worker_script=ROOT / "tools" / "prismabuild_worker.py")
    return action, queue, queue.claim()


#: Allocates and touches 96 MiB so the peak is unmistakably above the noise of
#: an interpreter, then writes a result.
ALLOCATE = (
    "buf = bytearray(96 * 1024 * 1024)\n"
    "for offset in range(0, len(buf), 4096):\n"
    "    buf[offset] = 1\n"
    "open('result', 'w').write('ok')\n"
)


def _own_cgroup() -> Path:
    """The cgroup this test process is already in; nothing is created."""

    for line in Path("/proc/self/cgroup").read_text().splitlines():
        parts = line.split(":", 2)
        if len(parts) == 3 and parts[0] == "0":
            return Path("/sys/fs/cgroup") / parts[2].lstrip("/")
    pytest.skip("no cgroup v2 membership for this process")


def _fake_cgroup(path: Path, *, usage_usec: int, user_usec: int,
                 system_usec: int, pids: list[int]) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    (path / "cpu.stat").write_text(
        f"usage_usec {usage_usec}\nuser_usec {user_usec}\n"
        f"system_usec {system_usec}\nnr_periods 0\n")
    (path / "memory.current").write_text("2048\n")
    (path / "memory.peak").write_text("409600\n")
    (path / "memory.events").write_text("low 0\noom_kill 0\n")
    (path / "memory.events.local").write_text("low 0\noom 0\n")
    (path / "cgroup.procs").write_text("".join(f"{pid}\n" for pid in pids))
    return path


# -- the parent's own view of what it reaped ----------------------------------

def test_the_outcome_carries_what_the_parent_reaped(tmp_path):
    """rusage deltas bracket the child, and say what they cover."""

    _, queue, item = _claimed(tmp_path, ALLOCATE)
    outcome = queue.execute(item)
    assert outcome["status"] == "executed", outcome.get("stderr")

    profile = outcome["resource_profile"]
    assert profile["schema"] == pool.RESOURCE_PROFILE_SCHEMA_V1
    reaped = profile["reaped_children"]
    assert reaped["source"] == "getrusage_children"
    # Deltas, so both are this child's own and not the process's history.
    assert reaped["user_seconds"] > 0.0
    assert reaped["system_seconds"] >= 0.0
    assert isinstance(reaped["voluntary_context_switches"], int)
    assert isinstance(reaped["involuntary_context_switches"], int)
    # ru_maxrss under RUSAGE_CHILDREN is a high-water mark over every child
    # this process has ever reaped, so it can only be raised, never lowered,
    # by a child that touched 96 MiB.
    assert reaped["max_rss_watermark_bytes"] >= 64 * MIB
    # The attributable figure is present only when this child raised the mark;
    # when it did not, the field is absent rather than a number that is not
    # about this action.
    if reaped.get("max_rss_bytes") is not None:
        assert reaped["max_rss_bytes"] >= 64 * MIB
    assert profile["wall_seconds"] > 0.0


def test_a_withdrawn_action_that_never_launched_reports_no_child(tmp_path):
    """Nothing was reaped, so nothing is claimed about a child."""

    _, queue, item = _claimed(tmp_path, ALLOCATE)
    queue.withdraw(item["action_key"], by="operator", reason="test")
    outcome = queue.execute(item)
    assert outcome["status"] == "withdrawn"
    assert "reaped_children" not in (outcome.get("resource_profile") or {})


# -- the sampler's view of the processes inside the scope ---------------------

def test_the_sampler_reads_process_io_while_the_processes_live(tmp_path):
    """``/proc`` is gone after the reap, so the sampler is what reads it."""

    marker = tmp_path / "written"
    child = subprocess.Popen(
        [sys.executable, "-c",
         "import os, sys, time\n"
         "data = b'x' * (1024 * 1024)\n"
         "with open(sys.argv[1], 'wb') as handle:\n"
         "    for _ in range(24):\n"
         "        handle.write(data)\n"
         "    handle.flush()\n"
         "    os.fsync(handle.fileno())\n"
         "sys.stdout.write('ready\\n')\n"
         "sys.stdout.flush()\n"
         "time.sleep(30)\n",
         str(marker)],
        stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == "ready"
        group = _fake_cgroup(tmp_path / "cgroup", usage_usec=1_500_000,
                             user_usec=1_000_000, system_usec=500_000,
                             pids=[child.pid])
        scope = resource_scope.ResourceScope(
            "a" * 64, "b" * 32, 1 << 30, tmp_path / "telemetry.json")
        scope.unit = "prismabuild-job" + "c" * 32 + ".slice"
        scope.cgroup_path = group
        record = scope.sample()
    finally:
        child.terminate()
        child.wait(timeout=30)
        child.stdout.close()

    # The split the cgroup already publishes and nothing read.
    assert record["cpu_user_seconds"] == pytest.approx(1.0)
    assert record["cpu_system_seconds"] == pytest.approx(0.5)

    io = record["process_io"]
    assert io["source"] == "proc_io"
    assert io["processes_observed"] == 1
    # Character counts are exact wherever the file lives; block counts are not
    # comparable on a tmpfs, so only their presence is asserted here and the
    # bytes are checked against ``/usr/bin/time -v`` on the fleet.
    assert io["wchar"] >= 24 * MIB
    assert isinstance(io["write_bytes"], int)
    assert isinstance(io["read_bytes"], int)
    assert io["rchar"] >= 0


def test_process_io_survives_the_process_that_earned_it(tmp_path):
    """A pid that has gone keeps contributing what it was last seen using."""

    child = subprocess.Popen(
        [sys.executable, "-c",
         "import sys, time\n"
         "sys.stdout.write('x' * (3 * 1024 * 1024))\n"
         "sys.stdout.flush()\n"
         "sys.stderr.write('ready\\n')\n"
         "sys.stderr.flush()\n"
         "time.sleep(30)\n"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    group = _fake_cgroup(tmp_path / "cgroup", usage_usec=1, user_usec=1,
                         system_usec=0, pids=[child.pid])
    scope = resource_scope.ResourceScope(
        "a" * 64, "b" * 32, 1 << 30, tmp_path / "telemetry.json")
    scope.unit = "prismabuild-job" + "c" * 32 + ".slice"
    scope.cgroup_path = group
    try:
        assert child.stderr.readline().strip() == "ready"
        first = scope.sample()
    finally:
        child.terminate()
        child.wait(timeout=30)
        child.stderr.close()
    assert first["process_io"]["wchar"] >= 3 * MIB

    # The scope is empty now, and the reading it earned is still in the total.
    (group / "cgroup.procs").write_text("")
    second = scope.sample()
    assert second["process_io"]["wchar"] >= first["process_io"]["wchar"]
    assert second["process_io"]["processes_live"] == 0
    assert second["process_io"]["processes_observed"] == 1


def test_a_second_sampler_continues_the_first_ones_accounting(tmp_path):
    """The telemetry file is the state, so a rebuilt scope does not restart.

    The pool builds one scope to launch an attempt and rebuilds another from
    the claim record to sample it after the child has gone.  A total that
    started again between the two would report the last tick as the whole run.
    """

    child = subprocess.Popen(
        [sys.executable, "-c",
         "import sys, time\n"
         "sys.stdout.write('x' * (5 * 1024 * 1024))\n"
         "sys.stdout.flush()\n"
         "sys.stderr.write('ready\\n')\n"
         "sys.stderr.flush()\n"
         "time.sleep(30)\n"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    group = _fake_cgroup(tmp_path / "cgroup", usage_usec=1, user_usec=1,
                         system_usec=0, pids=[child.pid])
    telemetry = tmp_path / "telemetry.json"
    first = resource_scope.ResourceScope("a" * 64, "b" * 32, 1 << 30, telemetry)
    first.unit = "prismabuild-job" + "c" * 32 + ".slice"
    first.cgroup_path = group
    try:
        assert child.stderr.readline().strip() == "ready"
        seen = first.sample()["process_io"]
    finally:
        child.terminate()
        child.wait(timeout=30)
        child.stderr.close()
    assert seen["wchar"] >= 5 * MIB

    (group / "cgroup.procs").write_text("")
    second = resource_scope.ResourceScope("a" * 64, "b" * 32, 1 << 30, telemetry)
    second.unit = first.unit
    second.cgroup_path = group
    carried = second.sample()["process_io"]
    assert carried["wchar"] >= seen["wchar"]
    assert carried["processes_observed"] == 1
    assert carried["processes_live"] == 0


def test_rebuilt_sampler_keeps_local_io_after_shared_copy_failure(tmp_path, monkeypatch):
    """A failed diagnostic copy must not discard the last local accounting."""
    shared = tmp_path / "shared.json"
    local = tmp_path / "local.json"
    group = _fake_cgroup(tmp_path / "cgroup", usage_usec=1, user_usec=1,
                         system_usec=0, pids=[4242])
    monkeypatch.setattr(resource_scope, "scope_pids", lambda _, **kw: [4242])
    monkeypatch.setattr(resource_scope, "read_process_io", lambda _: (
        "4242:1", 1, {name: 9 * MIB for name in resource_scope.IO_COUNTERS}))
    first = resource_scope.ResourceScope(
        "a" * 64, "b" * 32, 1 << 30, shared, authority_path=local)
    first.cgroup_path = group
    # Leave an older, valid shared sample, then fail its next publication.
    shared.write_text(json.dumps({"nonce": first.nonce, "process_io": {}}))
    write = resource_scope._atomic_json

    def fail_shared(path, record):
        if path == shared:
            raise OSError("shared copy unavailable")
        write(path, record)

    with monkeypatch.context() as patch:
        patch.setattr(resource_scope, "_atomic_json", fail_shared)
        with pytest.raises(OSError, match="shared copy unavailable"):
            first.sample()
    assert json.loads(local.read_text())["process_io"]["wchar"] == 9 * MIB

    # The process has gone by the final sample. Its bytes survive in local
    # authority, while the shared copy never learned about them.
    monkeypatch.setattr(resource_scope, "scope_pids", lambda _, **kw: [])
    second = resource_scope.ResourceScope(
        "a" * 64, "b" * 32, 1 << 30, shared, authority_path=local)
    second.cgroup_path = group
    carried = second.sample()["process_io"]
    assert carried["wchar"] == 9 * MIB
    assert carried["processes_observed"] == 1
    assert carried["processes_live"] == 0
    assert json.loads(shared.read_text())["process_io"] == carried


@pytest.mark.parametrize("local_state", ["missing", "malformed", "other_attempt", "unreadable"])
def test_local_io_authority_never_falls_back_to_shared(tmp_path, monkeypatch, local_state):
    shared = tmp_path / "shared.json"
    local = tmp_path / "local.json"
    shared.write_text(json.dumps({
        "nonce": "b" * 32,
        "process_io": {"retired": {name: 9 * MIB for name in resource_scope.IO_COUNTERS}},
    }))
    if local_state == "malformed":
        local.write_text("{")
    elif local_state == "other_attempt":
        local.write_text(json.dumps({"nonce": "c" * 32, "process_io": {"retired": {"wchar": 5}}}))
    read = Path.read_text

    def read_local(path, *args, **kwargs):
        assert path != shared, "shared diagnostic copy is not accounting authority"
        if path == local and local_state == "unreadable":
            raise PermissionError("local telemetry unavailable")
        return read(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read_local)
    scope = resource_scope.ResourceScope(
        "a" * 64, "b" * 32, 1 << 30, shared, authority_path=local)
    assert scope._prior_process_io() == {}


def test_a_retry_does_not_inherit_the_previous_attempts_readings(tmp_path):
    """The file is named for the action; only the nonce says whose attempt.

    A retry on the same host finds its predecessor's telemetry file still
    sitting there, because nothing unlinks it. Adopting it would open attempt
    two holding attempt one's retired bytes and retire attempt one's dead
    roots all over again.
    """

    telemetry = tmp_path / "telemetry.json"
    group = _fake_cgroup(tmp_path / "cgroup", usage_usec=1, user_usec=1,
                         system_usec=0, pids=[])
    telemetry.write_text(json.dumps({
        "action_key": "a" * 64, "nonce": "b" * 32,
        "process_io": {"source": "proc_io", "wchar": 9 * MIB,
                       "write_bytes": 9 * MIB, "processes_observed": 4,
                       "retired": {name: 9 * MIB
                                   for name in resource_scope.IO_COUNTERS},
                       "live": {}, "members": [4242]},
    }))

    # The same nonce still continues, or the sampler could not sample twice.
    # It goes first because sampling rewrites the file with its own record.
    same = resource_scope.ResourceScope("a" * 64, "b" * 32, 1 << 30, telemetry)
    same.unit = "prismabuild-job" + "c" * 32 + ".slice"
    same.cgroup_path = group
    assert same.sample()["process_io"]["wchar"] == 9 * MIB

    retry = resource_scope.ResourceScope("a" * 64, "d" * 32, 1 << 30, telemetry)
    retry.unit = same.unit
    retry.cgroup_path = group
    fresh = retry.sample()["process_io"]
    assert fresh["wchar"] == 0, fresh
    assert fresh["write_bytes"] == 0, fresh
    assert fresh["processes_observed"] == 0, fresh


def test_collecting_a_measurement_never_decides_the_verdict(tmp_path):
    """The pool samples outside a guard, so the guard has to be here.

    A prior record shaped wrongly raises something that is neither OSError nor
    ValueError. Reaching the worker loop with it would file a green action as
    failed, because the action's own outcome is not what raised.
    """

    telemetry = tmp_path / "telemetry.json"
    group = _fake_cgroup(tmp_path / "cgroup", usage_usec=1, user_usec=1,
                         system_usec=0, pids=[])
    telemetry.write_text(json.dumps({
        "action_key": "a" * 64, "nonce": "b" * 32,
        "process_io": {"source": "proc_io", "retired": "not a mapping"},
    }))
    scope = resource_scope.ResourceScope("a" * 64, "b" * 32, 1 << 30, telemetry)
    scope.unit = "prismabuild-job" + "c" * 32 + ".slice"
    scope.cgroup_path = group

    record = scope.sample()
    assert record["process_io"]["source"] == "proc_io"
    assert record["process_io"]["errors"], record["process_io"]
    assert "AttributeError" in record["process_io"]["errors"][0]
    # The cgroup half is untouched: one broken group does not lose the others.
    assert record["memory_peak_bytes"] is not None


# -- the box window -----------------------------------------------------------

PQTELD_HEADER = ("epoch_ms,MemTotal,MemAvailable,psi_mem_full_avg10,"
                 "psi_io_full_avg10,uvm_residual_kb,gpu_util,power_draw_w")


def _pqteld_csv(csv_dir: Path, host: str, when: float, rows) -> Path:
    csv_dir.mkdir(parents=True, exist_ok=True)
    day = time.strftime("%Y%m%d", time.localtime(when))
    path = csv_dir / f"pqteld-{host}-{day}.s2.csv"
    path.write_text(PQTELD_HEADER + "\n" + "".join(row + "\n" for row in rows))
    return path


def _reference(*, timeout_s=1.0):
    return {"power_reference_w": 140.0, "power_reference_scope": "soc_tdp",
            "power_reference_source": "https://example.invalid/gb10"}


def test_the_window_summarises_only_the_seconds_the_action_ran(tmp_path):
    start, end = 1_700_000_100.0, 1_700_000_110.0
    _pqteld_csv(tmp_path / "csv", "testbox", start, [
        # Before the window: a peak that must not be reported as this action's.
        f"{int((start - 30) * 1000)},127535272,118841008,0,0,3276936,99,500.0",
        f"{int((start + 1) * 1000)},127535272,118841008,0.5,0,3276936,10,10.0",
        f"{int((start + 3) * 1000)},127535272,110000000,1.5,0.25,4000000,20,20.0",
        f"{int((start + 5) * 1000)},127535272,118841008,0,0,3276936,30,60.0",
        # After the window.
        f"{int((end + 30) * 1000)},127535272,118841008,0,0,3276936,99,400.0",
    ])
    window = box_window.read_window(
        start, end, host="testbox", csv_dir=tmp_path / "csv",
        netdata_url=None, gpu_reference=_reference)

    assert window["source"] == "pqteld"
    gpu = window["gpu"]
    assert gpu["source"] == "pqteld"
    assert gpu["samples"] == 3
    assert gpu["power_w_mean"] == pytest.approx(30.0)
    assert gpu["power_w_peak"] == pytest.approx(60.0)
    assert gpu["power_reference_w"] == pytest.approx(140.0)
    assert gpu["power_reference_scope"] == "soc_tdp"
    assert gpu["power_peak_fraction_of_reference"] == pytest.approx(60.0 / 140.0)

    memory = window["memory"]
    assert memory["source"] == "pqteld"
    assert memory["unified_available_bytes_min"] == 110000000 * 1024
    assert memory["unified_used_bytes_peak"] == (127535272 - 110000000) * 1024
    assert memory["psi_mem_full_avg10_max"] == pytest.approx(1.5)
    assert memory["psi_io_full_avg10_max"] == pytest.approx(0.25)


def test_an_empty_cell_is_not_measured_and_is_never_zero(tmp_path):
    start, end = 1_700_000_100.0, 1_700_000_110.0
    _pqteld_csv(tmp_path / "csv", "testbox", start, [
        f"{int((start + 1) * 1000)},127535272,118841008,0,0,3276936,10,10.0",
        f"{int((start + 2) * 1000)},127535272,118841008,0,0,3276936,,",
        f"{int((start + 3) * 1000)},127535272,118841008,0,0,3276936,30,20.0",
    ])
    gpu = box_window.read_window(
        start, end, host="testbox", csv_dir=tmp_path / "csv",
        netdata_url=None, gpu_reference=_reference)["gpu"]
    assert gpu["samples"] == 2
    assert gpu["power_w_mean"] == pytest.approx(15.0)


def test_a_recorder_nobody_can_reach_is_unavailable_with_a_reason(tmp_path):
    (tmp_path / "csv").mkdir()
    window = box_window.read_window(
        1_700_000_100.0, 1_700_000_110.0, host="testbox",
        csv_dir=tmp_path / "csv",
        # Port 1 is reserved and nothing on this box listens there.
        netdata_url="http://127.0.0.1:1", gpu_reference=_reference)
    assert window["source"] == "unavailable"
    assert window["reason"]
    assert "gpu" not in window and "cpu" not in window


def test_the_window_is_bounded(tmp_path):
    """A slow recorder costs the deadline, not the action."""

    (tmp_path / "csv").mkdir()
    started = time.monotonic()
    window = box_window.read_window(
        1_700_000_100.0, 1_700_000_110.0, host="testbox",
        csv_dir=tmp_path / "csv", netdata_url="http://127.0.0.1:1",
        gpu_reference=_reference, deadline_s=1.0)
    assert time.monotonic() - started < 8.0
    assert window["source"] == "unavailable"


@pytest.mark.parametrize("csv_elapsed", [0.0, 1.99, 2.0, 3.0])
def test_netdata_cpu_read_respects_remaining_budget(monkeypatch, csv_elapsed):
    clock = [100.0]
    monkeypatch.setattr(box_window.time, "monotonic", lambda: clock[0])

    def csv(*args, **kwargs):
        clock[0] += csv_elapsed
        return {}, []

    calls = []

    def chart(url, name, after, before, timeout):
        calls.append((name, timeout))
        return None

    monkeypatch.setattr(box_window, "_pqteld_series", csv)
    window = box_window.read_window(
        10, 20, host="testbox", chart_reader=chart, deadline_s=2)
    assert window["source"] == "unavailable"
    if csv_elapsed >= 2:
        assert calls == [], "an expired budget must not start a Netdata request"
        assert "deadline" in window["reason"]
    else:
        assert len(calls) == 1
        assert calls[0][1] == pytest.approx(min(1, 2 - csv_elapsed))


@pytest.mark.parametrize("spent", [0.0, 0.99, 1.0, 1.1])
def test_netdata_pressure_reads_share_budget_and_retain_cpu(monkeypatch, spent):
    clock = [100.0]
    monkeypatch.setattr(box_window.time, "monotonic", lambda: clock[0])
    calls = []

    def chart(url, name, after, before, timeout):
        calls.append((name, timeout))
        if name == "system.cpu":
            clock[0] += spent
            return {"labels": ["time", "user"], "data": [[10, 25.0]]}
        # Consume the rest of the budget: full pressure must then be skipped.
        clock[0] = 101.0
        return {"labels": ["time", "some 10"], "data": [[10, 3.0]]}

    cpu, errors = box_window._netdata_group(
        "http://test.invalid", 10, 20, expires=101.0, chart_reader=chart)
    assert cpu["busy_percent_mean"] == 25.0
    assert cpu["busy_percent_peak"] == 25.0
    assert any("deadline" in error for error in errors)
    if spent >= 1:
        assert len(calls) == 1
        assert "psi_some_avg10_max" not in cpu
    else:
        assert len(calls) == 2
        assert calls[1][1] == pytest.approx(1 - spent)
        assert cpu["psi_some_avg10_max"] == 3.0
    assert "psi_full_avg10_max" not in cpu


@pytest.mark.parametrize("csv_elapsed", [0.0, 1.75, 1.999, 2.0, 3.0])
def test_gpu_reference_uses_only_the_remaining_window_budget(monkeypatch, csv_elapsed):
    from prismabuild import gpu_capacity

    clock = [100.0]
    monkeypatch.setattr(box_window.time, "monotonic", lambda: clock[0])
    power = box_window._Series()
    power.add(42.0)

    def csv(*args, **kwargs):
        clock[0] += csv_elapsed
        return {"power_draw_w": power}, []

    budgets = []

    def devices(*, timeout_s):
        budgets.append(timeout_s)
        return [{"power_reference_w": 140.0}], []

    monkeypatch.setattr(box_window, "_pqteld_series", csv)
    monkeypatch.setattr(gpu_capacity, "devices", devices)
    window = box_window.read_window(
        10.0, 20.0, host="testbox", netdata_url=None,
        gpu_reference=box_window.gpu_power_reference, deadline_s=2.0)
    # A missing reference never discards power already measured by the recorder.
    assert window["gpu"]["power_w_peak"] == 42.0
    if csv_elapsed >= 2.0:
        assert budgets == [], "a spent finish budget must not launch nvidia-smi"
        assert "power_reference_w" not in window["gpu"]
        assert any("deadline" in error for error in window["errors"])
    else:
        assert budgets == pytest.approx([min(1.0, 2.0 - csv_elapsed)])
        assert window["gpu"]["power_peak_fraction_of_reference"] == 0.3


def test_netdata_supplies_the_cpu_fields_pqteld_does_not_record(tmp_path, monkeypatch):
    start, end = 1_700_000_100.0, 1_700_000_110.0
    charts = {
        "system.cpu": {"labels": ["time", "user", "system", "iowait"],
                       "data": [[int(start + 2), 10.0, 5.0, 1.0],
                                [int(start + 4), 20.0, 4.0, 0.0],
                                [int(start + 6), None, None, None]]},
        "system.cpu_some_pressure": {
            "labels": ["time", "some 10", "some 60", "some 300"],
            "data": [[int(start + 2), 3.0, 1.0, 0.5],
                     [int(start + 4), 7.0, 2.0, 0.5]]},
    }
    monkeypatch.setattr(box_window, "_netdata_chart",
                        lambda url, chart, after, before, timeout_s: charts.get(chart))
    (tmp_path / "csv").mkdir()
    window = box_window.read_window(
        start, end, host="testbox", csv_dir=tmp_path / "csv",
        netdata_url="http://127.0.0.1:19999", gpu_reference=_reference)
    assert window["source"] == "netdata"
    cpu = window["cpu"]
    assert cpu["source"] == "netdata"
    assert cpu["samples"] == 2
    assert cpu["busy_percent_mean"] == pytest.approx((16.0 + 24.0) / 2)
    assert cpu["psi_some_avg10_max"] == pytest.approx(7.0)


# -- the window never fails the finish path ----------------------------------

def test_a_window_that_raises_does_not_fail_the_action(tmp_path, monkeypatch):
    def explode(*args, **kwargs):
        raise RuntimeError("recorder is on fire")

    monkeypatch.setattr(pool.box_window, "read_window", explode)
    _, queue, item = _claimed(tmp_path, ALLOCATE)
    outcome = queue.execute(item)
    assert outcome["status"] == "executed", outcome.get("stderr")
    window = outcome["resource_profile"]["box_window"]
    assert window["source"] == "unavailable"
    assert "recorder is on fire" in window["reason"]


# -- identity -----------------------------------------------------------------

def test_the_profile_is_not_part_of_the_action_key(tmp_path):
    """Metadata about a run cannot change which action the run was."""

    action, queue, item = _claimed(tmp_path, ALLOCATE)
    key = item["action_key"]
    request = Path(item["cas_root"]) / "requests" / key[:2] / f"{key}.json"
    before = request.read_bytes()

    outcome = queue.execute(item)
    assert outcome["status"] == "executed", outcome.get("stderr")
    assert outcome["resource_profile"]

    assert request.read_bytes() == before
    resealed = pb.seal_action({k: v for k, v in action.items()
                               if k != "action_key"})
    assert resealed["action_key"] == key
    assert "resource_profile" not in json.dumps(action["params"])


# -- readers ------------------------------------------------------------------

def _profile_ending(queue_root: Path, key: str, *, profile) -> None:
    for name in ("ready", "claimed", "done", "failed", "withdrawn", "workers"):
        (queue_root / name).mkdir(parents=True, exist_ok=True)
    detail = {"elapsed_s": 12.0, "returncode": 0}
    if profile is not None:
        detail["resource_profile"] = profile
    (queue_root / "done" / f"{key}.json").write_text(json.dumps({
        "schema": pool.POOL_OUTCOME_SCHEMA_V1, "action_key": key,
        "status": "executed", "claimed_host": "sparky",
        "finished_host": "sparky", "published_unix": 1.0,
        "claimed_unix": 2.0, "finished_unix": 14.0, "detail": detail,
    }))


PROFILE = {
    "schema": "prismabuild.resource_profile.v1",
    "scope": {"source": "cgroup", "memory_peak_bytes": 3 * 1024 ** 3},
    "process_io": {"source": "proc_io", "read_bytes": 1024,
                   "write_bytes": 64 * MIB},
    "box_window": {"source": "pqteld", "gpu": {
        "source": "pqteld", "power_w_peak": 42.0, "power_reference_w": 140.0,
        "power_peak_fraction_of_reference": 0.3}},
}


def test_pbstatus_endings_report_peak_memory_io_and_gpu_power(tmp_path):
    _profile_ending(tmp_path / "queue", "a" * 64, profile=PROFILE)
    row = pbstatus.read_endings(tmp_path / "queue")[0]
    assert row["memory_peak_bytes"] == 3 * 1024 ** 3
    assert row["io_write_bytes"] == 64 * MIB
    assert row["io_read_bytes"] == 1024
    assert row["gpu_power_peak_w"] == pytest.approx(42.0)
    assert row["gpu_power_reference_w"] == pytest.approx(140.0)
    assert row["gpu_power_peak_fraction"] == pytest.approx(0.3)
    rendered = "\n".join(pbstatus.ending_lines([row]))
    assert "RESOURCE" in rendered
    assert "rss=3.0G" in rendered
    assert "gpu=42.0W/140.0W(30%)" in rendered


def test_an_ending_from_before_the_profile_renders_absent_not_zero(tmp_path):
    _profile_ending(tmp_path / "queue", "b" * 64, profile=None)
    row = pbstatus.read_endings(tmp_path / "queue")[0]
    assert row["memory_peak_bytes"] is None
    assert row["io_write_bytes"] is None
    assert row["gpu_power_peak_w"] is None
    rendered = "\n".join(pbstatus.ending_lines([row]))
    # Not measured is not measured as zero, and does not print as a number.
    assert "rss=" not in rendered
    assert "gpu=" not in rendered
    assert pbstatus.ABSENT in rendered


# -- the payload leaf the worker may not open ---------------------------------

def test_membership_is_read_from_the_processes_not_the_directory(tmp_path):
    """``/proc/<pid>/cgroup`` answers what a closed leaf directory will not."""

    membership = resource_scope.cgroup_membership(_own_cgroup())
    assert membership.startswith("/")
    assert os.getpid() in resource_scope.procs_in_cgroup(membership)
    # A scope nothing belongs to is empty, and a path outside the cgroup root
    # is not a scope at all.
    assert resource_scope.procs_in_cgroup("/prismabuild.slice/nobody.slice") == []
    assert resource_scope.cgroup_membership(tmp_path) == ""


def test_a_leaf_this_uid_cannot_open_is_not_an_empty_one(tmp_path, monkeypatch):
    """The broker keeps the payload leaf root-only, and that is where the work is.

    Walking the tree and taking the refusal as "no processes here" is how the
    whole I/O total came back zero on the fleet while the action was writing
    64 MiB.  A refused directory has to send the reader to the other source,
    not end the search.
    """

    group = tmp_path / "cgroup"
    group.mkdir()
    (group / "cgroup.procs").write_text("")
    leaf = group / "payload"
    leaf.mkdir()
    (leaf / "cgroup.procs").write_text("4242\n")
    leaf.chmod(0o000)
    try:
        monkeypatch.setattr(resource_scope, "procs_in_cgroup",
                            lambda membership, **kw: [909090])
        assert 909090 in resource_scope.scope_pids(group)
    finally:
        leaf.chmod(0o700)


def test_a_tree_that_reads_completely_does_not_scan_proc(tmp_path, monkeypatch):
    """The scan is what a refusal costs, not what every sample costs."""

    group = tmp_path / "cgroup"
    group.mkdir()
    (group / "cgroup.procs").write_text("17\n18\n")

    def refuse(membership, **kw):
        raise AssertionError("a readable tree must not scan /proc")

    monkeypatch.setattr(resource_scope, "procs_in_cgroup", refuse)
    assert sorted(resource_scope.scope_pids(group)) == [17, 18]


def test_the_sampler_finds_a_child_through_this_processes_own_scope(tmp_path):
    """End to end against a real cgroup: a real child, its real counters."""

    marker = tmp_path / "written"
    child = subprocess.Popen(
        [sys.executable, "-c",
         "import os, sys, time\n"
         "with open(sys.argv[1], 'wb') as handle:\n"
         "    for _ in range(12):\n"
         "        handle.write(b'x' * (1024 * 1024))\n"
         "    handle.flush()\n"
         "    os.fsync(handle.fileno())\n"
         "sys.stderr.write('ready\\n')\n"
         "sys.stderr.flush()\n"
         "time.sleep(30)\n",
         str(marker)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    scope = resource_scope.ResourceScope(
        "a" * 64, "b" * 32, 1 << 30, tmp_path / "telemetry.json")
    scope.unit = "prismabuild-job" + "c" * 32 + ".slice"
    scope.cgroup_path = _own_cgroup()
    try:
        assert child.stderr.readline().strip() == "ready"
        io = scope.sample()["process_io"]
    finally:
        child.terminate()
        child.wait(timeout=30)
        child.stderr.close()
    # This process and its child are both in this cgroup, so the total covers
    # at least the child's own writes.
    assert io["processes_observed"] >= 2
    assert io["wchar"] >= 12 * MIB


def test_a_reaped_child_is_counted_once_not_twice(tmp_path):
    """``/proc/<pid>/io`` folds a reaped child into its parent, like rusage.

    Measured on the fleet: an action that wrote 64 MiB was reported as
    129 MiB, because the exited writer's last reading was retired *and* the
    same bytes had already migrated into the parent that reaped it. A process
    whose parent is in the scope keeps being counted through that parent; only
    the scope's roots are retired.

    The writer says when it has written and then holds itself open, because a
    process that has exited but not been reaped is a zombie, and a zombie's
    ``/proc/<pid>/io`` is EACCES: its bytes are readable nowhere until its
    parent absorbs them. Without that handshake this test is a race, and on a
    box fast enough to win it, the race reads as a hang.
    """

    target = tmp_path / "written"
    writer_script = tmp_path / "writer.py"
    writer_script.write_text(
        "import os, sys\n"
        "handle = open(sys.argv[1], 'wb')\n"
        "[handle.write(b'x' * (1024 * 1024)) for _ in range(16)]\n"
        "handle.flush(); os.fsync(handle.fileno()); handle.close()\n"
        "sys.stderr.write('written\\n'); sys.stderr.flush()\n"
        "sys.stdin.readline()\n")
    helper_script = tmp_path / "helper.py"
    helper_script.write_text(
        "import subprocess, sys, time\n"
        "child = subprocess.Popen([sys.executable, sys.argv[1], sys.argv[2]],\n"
        "                         stdin=subprocess.PIPE, text=True)\n"
        "sys.stderr.write('spawned %d\\n' % child.pid)\n"
        "sys.stderr.flush()\n"
        "sys.stdin.readline()\n"
        "child.stdin.close()\n"
        "child.wait()\n"
        "sys.stderr.write('reaped\\n')\n"
        "sys.stderr.flush()\n"
        "time.sleep(30)\n")
    parent = subprocess.Popen(
        [sys.executable, str(helper_script), str(writer_script), str(target)],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE, text=True)
    group = _fake_cgroup(tmp_path / "cgroup", usage_usec=1, user_usec=1,
                         system_usec=0, pids=[])
    scope = resource_scope.ResourceScope(
        "a" * 64, "b" * 32, 1 << 30, tmp_path / "telemetry.json")
    scope.unit = "prismabuild-job" + "c" * 32 + ".slice"
    scope.cgroup_path = group
    try:
        # Both processes write to the same inherited pipe, so read until each
        # has said its line rather than assuming which lands first.
        writer = None
        written = False
        for _ in range(4):
            if writer is not None and written:
                break
            line = parent.stderr.readline().strip()
            if line.startswith("spawned "):
                writer = int(line.split()[1])
            elif line == "written":
                written = True
        assert writer is not None and written, "the writer never reported"
        (group / "cgroup.procs").write_text(f"{parent.pid}\n{writer}\n")
        seen = scope.sample()["process_io"]
        assert seen["processes_live"] == 2, seen
        assert seen["wchar"] >= 16 * MIB, seen
        # Now the parent reaps it, and the kernel moves the bytes.
        parent.stdin.write("go\n")
        parent.stdin.flush()
        assert parent.stderr.readline().strip() == "reaped"
        (group / "cgroup.procs").write_text(f"{parent.pid}\n")
        after = scope.sample()["process_io"]
    finally:
        parent.terminate()
        parent.wait(timeout=30)
        parent.stderr.close()
        parent.stdin.close()

    assert after["processes_live"] == 1
    assert after["wchar"] >= 16 * MIB
    assert after["wchar"] < 32 * MIB, (
        "the reaped writer was counted twice: once retired, once inside the "
        "parent that absorbed it")

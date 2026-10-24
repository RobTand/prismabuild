"""The measurement of the medium, checked on the shapes it exists for.

Every case here is one the live fleet produced on 2026-09-06: a client in a
state-recovery storm, the same client healthy afterwards on a different
transport, the box that is the server and has no NFS statistics at all, a
remount that resets every counter, and a mount that does not answer.
"""

from __future__ import annotations

import io
import os
import json
from pathlib import Path
import subprocess
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import mount_latency  # noqa: E402


def test_probe_failure_reaches_netdata_and_human_output():
    reason = "PermissionError: [Errno 13] Permission denied: /probe/claim.tmp"
    record = {"probe": {"status": "error", "error": reason}}
    sink = io.StringIO()
    mount_latency._emit(record, sink)
    assert reason in sink.getvalue()
    assert reason in mount_latency.one_line(record)


def test_recording_failure_reaches_protocol_without_losing_probe(tmp_path,
                                                               capsys):
    # A regular file in place of the log directory is a real I/O failure,
    # independent of the worker UID's permission privileges.
    blocked = tmp_path / "records"
    blocked.write_text("not a directory")
    assert mount_latency.main([
        "--once", "--netdata", "--mount", str(tmp_path / "probe"),
        "--record-dir", str(blocked), "--lock-dir", str(tmp_path / "locks"),
    ]) == 0
    output = capsys.readouterr().out
    assert "SET ok = 1" in output
    assert "FileExistsError" in output
    assert str(blocked) in output


def test_diagnostic_labels_cannot_inject_commands_and_recovery_clears_them():
    reason = "bad 'path'\\\"\nDISABLE\r\x00" + "x" * 900
    sink = io.StringIO()
    mount_latency._emit({"probe": {"status": "error", "error": reason}},
                        sink, update_every=15)
    lines = sink.getvalue().splitlines()
    error_line = next(line for line in lines if line.startswith("CLABEL probe_error"))
    assert len(error_line) <= 512 + len("CLABEL probe_error '' 1")
    assert error_line.count("'") == 2
    assert "\\" not in error_line and "\x00" not in error_line
    assert "DISABLE" not in lines
    assert any(line.startswith("CHART ") and line.endswith(" 15") for line in lines)

    sink = io.StringIO()
    mount_latency._emit({"probe": {"status": "ok"},
                         "recording": {"status": "ok", "path": "/logs/box.jsonl"}},
                        sink)
    assert "CLABEL probe_error 'none' 1" in sink.getvalue()
    assert "CLABEL record_error 'none' 1" in sink.getvalue()
    assert "CLABEL record_status 'ok' 1" in sink.getvalue()


def test_forked_probe_error_is_retained_verbatim(tmp_path):
    reason = "denied 'probe'\\path\nsecond line"

    def denied(*args, **kwargs):
        raise PermissionError(reason)

    sampler = mount_latency.MountSampler(tmp_path, probe=denied,
                                        lock_dir=tmp_path / "locks")
    record = sampler.sample()
    assert record["probe"]["status"] == "error"
    assert record["probe"]["error"] == f"PermissionError: {reason}"
    path = mount_latency.append_record(record, tmp_path / "records")
    saved = json.loads(path.read_text())
    assert saved["probe"]["error"] == record["probe"]["error"]
    assert saved["recording"] == {"status": "ok", "path": str(path)}


@pytest.mark.parametrize("direct_exec", [False, True])
def test_external_plugin_interval_emits_charts_and_samples(tmp_path, direct_exec):
    """Exercise the installed symlink and Netdata's positional interval argv.

    The Python arm isolates protocol routing from the executable-mode failure.
    All probes and retained records stay in this host's temporary directory.
    """
    plugin = tmp_path / "mount_latency.plugin"
    plugin.symlink_to(Path(mount_latency.__file__).resolve())
    lock_dir = tmp_path / "worker-locks"
    lock_dir.mkdir(mode=0o700)
    (lock_dir / "admission.lock").touch()
    command = [str(plugin), "15", "--mount", str(tmp_path / "probe"),
               "--record-dir", str(tmp_path / "records"),
               "--lock-dir", str(lock_dir)]
    if not direct_exec:
        command.insert(0, sys.executable)
    with subprocess.Popen(command, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True) as process:
        try:
            stdout, stderr = process.communicate(timeout=3)
        except subprocess.TimeoutExpired:
            process.terminate()
            stdout, stderr = process.communicate(timeout=5)
    assert not stderr
    lines = stdout.splitlines()
    assert lines and lines[0].startswith("CHART "), stdout
    assert "BEGIN prismabuild.mount_probe_state" in lines
    assert "SET ok = 1" in lines
    assert "END" in lines
    assert any(line.startswith("CHART prismabuild.admission_gate ") for line in lines)
    assert any(line.startswith("CHART prismabuild.admission_hold ") for line in lines)
    assert "BEGIN prismabuild.admission_gate" not in lines
    assert "BEGIN prismabuild.admission_hold" not in lines
    records = list((tmp_path / "records").glob("*.jsonl"))
    assert len(records) == 1
    record = json.loads(records[0].read_text().splitlines()[0])
    assert record["probe"]["status"] == "ok"
    assert record["locks"]["present"] is True
    assert record["locks"]["identity_complete"] is False


@pytest.mark.parametrize("json_mode", [False, True])
def test_once_cli_preserves_human_and_json_output(tmp_path, json_mode):
    command = [sys.executable, mount_latency.__file__, "--once",
               "--mount", str(tmp_path / "probe"),
               "--record-dir", str(tmp_path / "records"),
               "--lock-dir", str(tmp_path / "locks")]
    if json_mode:
        command.append("--json")
    result = subprocess.run(command, text=True, capture_output=True, timeout=5)
    assert result.returncode == 0, result.stderr
    assert not result.stderr
    if json_mode:
        assert json.loads(result.stdout)["probe"]["status"] == "ok"
    else:
        assert result.stdout.startswith("[")
        assert "probe=ok" in result.stdout
    assert "CHART " not in result.stdout


@pytest.mark.parametrize("netdata", [False, True])
def test_default_record_directory_matches_invocation_mode(monkeypatch, netdata):
    """Netdata must not silently try to append in Rob's private directory."""
    recorded = []
    monkeypatch.setattr(mount_latency.MountSampler, "sample", lambda self: {})
    monkeypatch.setattr(mount_latency, "append_record",
                        lambda record, directory: recorded.append(directory))
    arguments = ["--once"] + (["--netdata"] if netdata else [])
    assert mount_latency.main(arguments) == 0
    assert recorded == [Path("/var/lib/netdata/prismabuild") if netdata
                        else mount_latency.DEFAULT_RECORD_DIR]


#: sparky at 21:30 UTC, after the storm and after the remount onto RDMA.
HEALTHY = """\
device systemd-1 mounted on /mnt/shared with fstype autofs
device 10.100.98.3:/storage_pool/shared mounted on /mnt/shared with fstype nfs4 statvers=1.1
\topts:\trw,vers=4.2,rsize=1048576,wsize=1048576,hard,proto=rdma,port=20049,timeo=600,retrans=2
\tage:\t2415
\timpl_id:\tname='Linux 7.0.0-31-generic',domain='kernel.org',date='0,0'
\tcaps:\tcaps=0xdffbf6b7,wtmult=512,dtsize=1048576,bsize=0,namlen=255
\tevents:\t35129 807358 1255 2564 31871 2968 862945 3533 0 23853 0 3996 51947 2079 20041 2186 0 19756 0 5 3533 57 0 0 96 0 0
\tbytes:\t249283724959 503403322 587202560 0 23922732683 503403322 5704528 3520
\txprt:\trdma 0 0 3 0 0 113031 111921 0
\tper-op statistics
\t        READ: 100 100 0 1000 2000 5 300 310 0
\t     GETATTR: 1000 1000 0 2000 3000 20 110 140 0
\t      RENAME: 50 50 0 500 400 1 20 22 0
"""

#: sparky at 16:37 UTC, mid-storm: TEST_STATEID saturating the slot table, so
#: GETATTR waits 142 ms to be sent while the server answers in 0.32 ms.
STORM = """\
device 192.168.1.107:/storage_pool/shared mounted on /mnt/shared with fstype nfs4 statvers=1.1
\topts:\trw,vers=4.2,hard,proto=tcp,nconnect=16,timeo=600,retrans=2
\tage:\t11772
\tevents:\t35129 807358 1255 2564 31871 2968 862945 3533 0 23853 0 3996 51947 2079 20041 2186 0 19756 0 5 3533 57 0 0 96 0 0
\tper-op statistics
\tTEST_STATEID: 70300 70300 0 100 100 30 14060 14100 0
\t     GETATTR: 1024 1024 0 2000 3000 145889 327 146220 0
\t      RENAME: 33 33 0 500 400 5452 201 5655 0
"""

#: dl380g10 exports this filesystem and reaches it as a local pool.
SERVER = """\
device storage_pool/shared mounted on /mnt/shared with fstype zfs
"""


def _mount(text, target="/mnt/shared/prismabuild-fleet"):
    return mount_latency.read_mountstats(Path(target), text)


def test_the_nfs_record_wins_over_the_autofs_trigger_on_one_mount_point():
    """An autofs trigger names the same mount point and carries no statistics.

    Taking the first match would report a mount with no operations, which
    reads as a silent healthy mount rather than as a parse that missed.
    """

    mount = _mount(HEALTHY)

    assert mount is not None
    assert mount.fstype == "nfs4"
    assert mount.transport == "rdma"
    assert mount.options["vers"] == "4.2"
    assert mount.ops["GETATTR"]["ops"] == 1000
    # ``events:`` carries twenty-seven integers and would pass any shape test
    # for an operation row.  Counted in, its sixth column landed in the
    # round-trip mean and reported 266 seconds per RPC on a healthy mount.
    assert set(mount.ops) == {"READ", "GETATTR", "RENAME"}


def test_the_server_box_reports_no_rpc_rather_than_zero_rpc(monkeypatch):
    """dl380g10 has no NFS statistics because it is the NFS server.

    Zero is the wrong answer twice over: it reads as a perfectly healthy
    network mount, and it would let a fleet-relative comparison rank the three
    boxes as if they crossed the same medium.
    """

    sampler = mount_latency.MountSampler(
        Path("/mnt/shared/prismabuild-fleet"), host="dl380g10",
        probe=lambda *_a, **_k: {"status": "ok", "stat_ms": 0.01,
                                 "worst_ms": 0.01})
    server = _mount(SERVER)          # parsed before the reader is replaced
    monkeypatch.setattr(mount_latency, "read_mountstats",
                        lambda *_a, **_k: server)
    record = sampler.sample()

    assert record["mount"]["transport"] == "local"
    assert record["mount"]["is_nfs"] is False
    assert record["rpc"] is None


def test_queue_time_and_round_trip_time_are_reported_apart():
    """The split is the diagnosis, and averaging it away destroys it.

    Mid-storm, GETATTR spent 142.5 ms queued against 0.32 ms at the server.
    One combined "latency" number would have said 142.8 ms and pointed at the
    server, which was answering other clients in 0.01 ms the whole time.
    """

    delta = mount_latency._op_delta({}, _mount(STORM).ops)
    view = mount_latency._rpc_view(delta, window_s=15.0)

    assert view["by_op"]["GETATTR"]["queue_ms"] == pytest.approx(142.47, abs=0.1)
    assert view["by_op"]["GETATTR"]["rtt_ms"] == pytest.approx(0.32, abs=0.01)
    # 97.7% of the traffic is TEST_STATEID, which is individually cheap; the
    # verdict has to survive that dilution.
    assert view["queue_share"] > 0.9
    assert view["by_op"]["TEST_STATEID"]["ops_per_s"] == pytest.approx(
        4686.7, abs=1.0)


def test_a_healthy_mount_puts_its_time_at_the_server_not_in_the_queue():
    """The same arithmetic, on the same box four hours later.

    `queue_share` is what separates the two, and it separates them by two
    orders of magnitude rather than by a margin somebody has to pick.
    """

    view = mount_latency._rpc_view(
        mount_latency._op_delta({}, _mount(HEALTHY).ops), window_s=15.0)

    assert view["queue_share"] < 0.1
    assert view["by_op"]["GETATTR"]["rtt_ms"] > view["by_op"]["GETATTR"][
        "queue_ms"]


def test_a_rate_is_never_reported_without_the_window_it_was_measured_over(
    monkeypatch,
):
    """The counters are cumulative since the mount came up.

    A first sample that divided them by anything would fold a four-hour-old
    storm into a reading of the last fifteen seconds -- and would keep doing
    it, quietly, for as long as the mount stayed up.
    """

    sampler = mount_latency.MountSampler(
        probe=lambda *_a, **_k: {"status": "ok", "worst_ms": 0.01})
    healthy = _mount(HEALTHY)        # parsed before the reader is replaced
    monkeypatch.setattr(mount_latency, "read_mountstats",
                        lambda *_a, **_k: healthy)
    first = sampler.sample()
    time.sleep(0.01)
    second = sampler.sample()

    assert first["rpc"] is None
    assert first["window_s"] == 0.0
    assert "first sample" in first["note"]
    assert second["window_s"] > 0


@pytest.mark.parametrize("operation_data", [
    "",  # missing per-op section
    "\tper-op statistics\n",  # empty per-op section
    "\tper-op statistics\n\tOPEN: invalid\n\tCLOSE: invalid\n",
    "\tper-op statistics\n\tOPEN: 1 1 0 0 0 0 0 0 invalid\n",
    "\tper-op statistics\n\tOPEN: 1 1 0 0 0 0 0 0 0\n\tCLOSE: invalid\n",
    "\tper-op statistics\n\tOPEN: 1 1 0 0 0 0 0 0 0\n\tCLOSE invalid\n",
    "\tper-op statistics\n\tOPEN: 1 1 0 0 0 0 0 0 -1\n",
])
@pytest.mark.parametrize("passive", [False, True])
def test_unusable_nfs_operations_clear_window_and_recover(
    monkeypatch, operation_data, passive,
):
    """Bad counters are unavailable evidence, not a quiet NFS mount.

    Exercise the real binary procfs reader with complete newline-terminated
    inputs, including partially valid operation tables.
    """
    header = STORM.partition("\tper-op statistics\n")[0]
    unusable = header + operation_data
    increased = STORM.replace("GETATTR: 1024 1024", "GETATTR: 1044 1044")
    samples = [unusable, unusable, STORM, STORM, unusable, STORM, increased]
    reads = iter(text.encode("utf-8") for text in samples
                 for _ in range(1 if passive else 2))
    clock = iter(range(1000, 1070, 10))
    actual_open = Path.open

    def proc_open(path, mode="r", *args, **kwargs):
        if str(path) != "/proc/self/mountstats":
            return actual_open(path, mode, *args, **kwargs)
        assert mode == "rb"
        return io.BytesIO(next(reads))

    monkeypatch.setattr(Path, "open", proc_open)
    monkeypatch.setattr(mount_latency.time, "time", lambda: next(clock))
    monkeypatch.setattr(mount_latency.MountSampler, "_run_probe",
                        lambda self: {"status": "ok"})
    monkeypatch.setattr(mount_latency, "lock_contention", lambda *a, **k: {})
    sampler = mount_latency.MountSampler(
        Path("/mnt/shared/prismabuild-fleet"), passive=passive)

    records = [sampler.sample() for _ in samples]
    for index in (0, 1, 4):
        unavailable = records[index]
        assert unavailable["rpc"] is None, "unusable counters are not zero RPCs"
        assert unavailable["window_s"] == 0
        assert unavailable["mount"]["transport"] == "unknown"
        assert "unusable NFS operation data" in unavailable["note"]
        assert not unavailable.get("attribution")
    for index in (2, 5):
        rebaseline = records[index]
        assert rebaseline["rpc"] is None, "never bridge a gap in counter coverage"
        assert rebaseline["window_s"] == 0
    idle = records[3]
    assert idle["window_s"] == 10
    assert idle["rpc"]["ops"] == 0, "valid unchanged counters really are idle"
    resumed = records[6]
    assert resumed["window_s"] == 10
    assert resumed["rpc"]["ops"] == 20
    assert resumed["rpc"]["by_op"]["GETATTR"]["ops_per_s"] == 2.0


@pytest.mark.parametrize("namespace", ["autofs", "submount", "unrelated"])
def test_unusable_nfs_operation_rejection_respects_mount_selection(namespace):
    bad = (HEALTHY.partition("\tper-op statistics\n")[0]
           + "\tper-op statistics\n\tOPEN: invalid\n")
    target = "/mnt/shared/jobs"
    if namespace == "submount":
        text = HEALTHY + bad.replace("/mnt/shared", "/mnt/shared/jobs")
    elif namespace == "unrelated":
        text = HEALTHY + bad.replace("/mnt/shared", "/elsewhere")
    else:
        text = bad  # includes an autofs trigger at the same mount point
    result = _mount(text, target)
    if namespace == "unrelated":
        assert result is not None and result.ops["GETATTR"]["ops"] == 1000
    else:
        assert result is None, "do not fall back to a parent or autofs trigger"


def test_a_remount_restarts_the_counters_and_the_window_says_so(monkeypatch):
    """Every counter goes back to zero and the mount age drops.

    Differencing across that boundary produces large negative numbers, and a
    negative count read as a rate is a fabricated measurement. sparky was
    remounted onto RDMA at about 20:50 UTC while this was being written.
    """

    before = _mount(STORM)
    after = _mount(HEALTHY)
    sampler = mount_latency.MountSampler(
        probe=lambda *_a, **_k: {"status": "ok", "worst_ms": 0.01})
    readings = iter([before, before, after, after])
    monkeypatch.setattr(mount_latency, "read_mountstats",
                        lambda *_a, **_k: next(readings, after))
    sampler.sample()
    time.sleep(0.01)
    second = sampler.sample()

    assert second["rpc"] is None
    assert "remount" in second["note"]


def test_counters_that_went_backwards_are_dropped_not_negated():
    delta = mount_latency._op_delta(_mount(STORM).ops, _mount(HEALTHY).ops)

    assert "GETATTR" not in delta, "1000 - 1024 is not a rate"
    assert "READ" in delta, "an op absent from the earlier snapshot still counts"


def test_a_mount_that_does_not_answer_is_recorded_not_raised():
    """A hard NFS mount waits rather than failing, and no signal reaches it.

    So the deadline has to be enforced by a process the parent can walk away
    from. What the caller gets back is a reading that says the mount did not
    answer -- which is the finding -- rather than an exception, which on a
    scrape interval is just a gap in the series where the incident was.
    """

    def never_returns(*_args, **_kwargs):
        time.sleep(30)
        return {"status": "ok"}

    sampler = mount_latency.MountSampler(
        deadline_s=0.2, probe=never_returns)
    started = time.time()
    record = sampler.sample()
    elapsed = time.time() - started

    assert record["probe"]["status"] == "timed_out"
    assert record["probe"]["deadline_s"] == 0.2
    assert elapsed < 5.0, "the deadline bounds the caller, not just the child"
    # The child was merely slow, not wedged, so SIGKILL reached it and the
    # next sample is free to probe again.  Only a child the kernel will not
    # kill is allowed to suppress the following one.
    assert sampler._outstanding_pid is None


def test_a_wedged_probe_suppresses_the_next_one_instead_of_doubling_it(
    monkeypatch,
):
    """One blocked process, however long the mount stays wedged.

    A D-state child does not die on SIGKILL, so it is still there at the next
    scrape. Probing anyway would add a second blocked process every fifteen
    seconds -- 240 an hour, all of them waiting on the thing being measured.
    Skipping is not a degraded reading: `wedged` is the strongest statement
    this module makes.
    """

    calls = []
    sampler = mount_latency.MountSampler(
        probe=lambda *_a, **_k: calls.append(1) or {"status": "ok"})
    sampler._outstanding_pid = 4242
    sampler._outstanding_since = time.time() - 30.0
    monkeypatch.setattr(mount_latency.os, "waitpid",
                        lambda _pid, _flags=0: (0, 0))    # still blocked
    record = sampler.sample()

    assert record["probe"]["status"] == "wedged"
    assert record["probe"]["outstanding_pid"] == 4242
    assert record["probe"]["wedged_for_s"] == pytest.approx(30.0, abs=2.0)
    assert calls == [], "a wedged mount must not be given a second waiter"
    # The leg that costs the mount nothing keeps reporting throughout.
    assert record["mount"]["is_nfs"] in (True, False)


def test_the_syscall_leg_really_runs_and_times_the_claim_path(tmp_path):
    """RENAME is how the pool takes a claim, and it was the 165 ms operation.

    A read-only probe would have timed everything except the operation the
    queue depends on, so the write cycle is timed and its scratch is left in
    place between samples.
    """

    result = mount_latency.timed_probe(tmp_path / "probe", repeats=3)

    assert result["status"] == "ok"
    for key in ("stat_ms", "open_ms", "listdir_ms", "claim_ms", "worst_ms"):
        assert isinstance(result[key], float)
    assert (tmp_path / "probe" / "anchor").is_file()
    # The claim cycle cleans up after itself; a probe that accumulated files
    # would make its own listdir slower every sample.
    leftovers = [p.name for p in (tmp_path / "probe").iterdir()]
    assert leftovers == ["anchor"]


def test_setup_is_paid_once_so_the_steady_state_cost_is_the_measurement(
    tmp_path,
):
    """Left on at a 15 s scrape, the per-sample footprint is what is timed.

    Re-creating the directory and re-checking the anchor every cycle is
    several RPCs per box per scrape against the mount under observation.
    """

    def record_setup(probe_dir, repeats, setup=True):
        # The probe runs in a forked child, so the flag has to travel back in
        # the payload; a list appended to here would be the child's list.
        result = mount_latency.timed_probe(probe_dir, repeats, setup)
        result["setup"] = setup
        return result

    sampler = mount_latency.MountSampler(
        tmp_path, probe=record_setup, repeats=2)
    first = sampler.sample()
    second = sampler.sample()

    assert first["probe"]["setup"] is True
    assert second["probe"]["setup"] is False, (
        "setup is paid once, not on every scrape")


def test_netdata_values_are_integers_in_declared_units():
    """The plugin protocol takes integers, so latencies travel in microseconds.

    A float here is silently dropped by netdata, which loses the chart rather
    than reporting a malformed one.
    """

    class Sink:
        def __init__(self):
            self.lines = []

        def write(self, text):
            self.lines.extend(text.splitlines())

        def flush(self):
            pass

    sink = Sink()
    mount_latency._emit({
        "probe": {"status": "ok", "stat_ms": 0.0026, "open_ms": 0.02,
                  "listdir_ms": 0.061, "claim_ms": 2.721, "worst_ms": 6.6},
        "rpc": {"ops_per_s": 85.6, "queue_ms": 0.051, "rtt_ms": 5.646,
                "queue_share": 0.0089},
    }, sink)

    values = [line.split("=")[1].strip() for line in sink.lines
              if line.startswith("SET ")]
    assert values, "nothing was emitted"
    for value in values:
        assert value.lstrip("-").isdigit(), f"{value!r} is not an integer"
    assert "SET claim = 2721" in sink.lines
    assert "SET rtt = 5646" in sink.lines


def test_a_wedged_probe_still_emits_its_state_so_the_gap_is_labelled():
    """The series must carry the incident, not a hole where it was."""

    class Sink:
        def __init__(self):
            self.lines = []

        def write(self, text):
            self.lines.extend(text.splitlines())

        def flush(self):
            pass

    sink = Sink()
    mount_latency._emit({"probe": {"status": "wedged"}, "rpc": None}, sink)

    assert "SET wedged = 1" in sink.lines
    assert "SET ok = 0" in sink.lines


def test_records_land_off_the_filesystem_being_measured(tmp_path):
    """Writing the measurement onto the mount loses the samples worth having."""

    record = {"host": "boxa", "probe": {"status": "wedged"}}
    path = mount_latency.append_record(record, tmp_path)

    assert path is not None
    assert path.parent == tmp_path
    assert "boxa" in path.name


def test_the_local_log_is_bounded_so_leaving_it_on_is_safe(tmp_path,
                                                           monkeypatch):
    """A measurement left on forever must not become the disk problem.

    At a 15 s scrape this file grows all day; netdata holds the long history,
    so the local copy only has to outlive the delay between an incident and
    somebody coming to look at it.
    """

    cap = 400
    record_dir = tmp_path / "records"
    record = {"host": "boxa", "probe": {"status": "ok"}}
    monkeypatch.setattr(mount_latency, "RECORD_MAX_BYTES", cap)
    for _ in range(40):
        mount_latency.append_record(record, record_dir)

    current = record_dir / "pb-mount-latency-boxa.jsonl"
    rotated = record_dir / "pb-mount-latency-boxa.jsonl.1"
    # The size is checked before the append, so one record may cross the cap.
    # The guarantee is a bound, and the test states the bound the code makes
    # rather than the tidier one it does not.
    one_record = len(json.dumps(record, sort_keys=True)) + 1
    assert current.stat().st_size <= cap + one_record
    assert rotated.is_file(), "one previous generation is kept"
    assert len(list(record_dir.iterdir())) == 2, "and only one"
    assert sum(f.stat().st_size for f in record_dir.iterdir()) <= 2 * (
        cap + one_record)


def test_a_closed_stdout_is_a_stop_signal_not_a_crash(tmp_path, monkeypatch):
    """netdata stops a plugin by closing its pipe.

    The plugin must treat that as the stop it is.  A traceback in the agent's
    error log reports the same event less clearly and looks like a defect in
    the collector every time netdata restarts it.
    """

    monkeypatch.setattr(
        mount_latency.MountSampler, "sample",
        lambda self: {"schema": mount_latency.SCHEMA, "probe": {}, "rpc": {}},
    )

    class ClosedPipe(io.StringIO):
        def write(self, _data):
            raise BrokenPipeError(32, "Broken pipe")

    monkeypatch.setattr(sys, "stdout", ClosedPipe())
    assert mount_latency.main(
        ["--once", "--record-dir", str(tmp_path)]) == 0


# --- the admission gate ------------------------------------------------------
#
# The leg that exists because the other two would have called dl380g10 healthy
# on 2026-09-06: 15 of 16 loops blocked on a local flock, nothing served, load
# average 1.13.

def _gate(tmp_path):
    lock = tmp_path / "a9de.lock"
    lock.touch()
    return lock, mount_latency.lock_key(lock)


def test_hold_age_grows_across_samples_and_resets_when_the_pid_lets_go(
        tmp_path, monkeypatch):
    lock, key = _gate(tmp_path)
    observed = [4242]
    monkeypatch.setattr(mount_latency, "read_lock_holders",
                        lambda keys: {k: {"holders": list(observed), "waiters": [],
                                          "identity_unverified": True} for k in keys})
    monkeypatch.setattr(mount_latency, "process_status",
                        lambda pid, want_wchan=True: ("D", "__break_lease"))
    memory = {}
    first = mount_latency.lock_contention(tmp_path, held_since=memory, now=1000.0)
    later = mount_latency.lock_contention(tmp_path, held_since=memory, now=1240.0)
    assert first["max_hold_s"] == 0.0
    assert later["max_hold_s"] == 240.0
    observed.clear()
    mount_latency.lock_contention(tmp_path, held_since=memory, now=1300.0)
    assert memory == {}
    observed.append(4242)
    again = mount_latency.lock_contention(tmp_path, held_since=memory, now=1400.0)
    assert again["max_hold_s"] == 0.0


def test_a_box_with_no_admission_lock_reports_absence_not_health(tmp_path):
    assert mount_latency.lock_contention(tmp_path / "absent")["present"] is False


def test_the_gate_is_measured_on_the_box_that_exports_the_filesystem(
        tmp_path, monkeypatch):
    _gate(tmp_path)
    server = mount_latency.read_mountstats(mount_latency.DEFAULT_MOUNT, text=SERVER)
    monkeypatch.setattr(mount_latency, "read_mountstats", lambda target, text=None: server)
    monkeypatch.setattr(mount_latency, "process_status",
                        lambda pid, want_wchan=True: ("S", "locks_lock_inode_wait"))
    monkeypatch.setattr(mount_latency, "read_lock_holders",
                        lambda keys: {k: {"holders": [4242], "waiters": [],
                                          "identity_unverified": True} for k in keys})
    sampler = mount_latency.MountSampler(
        mount_latency.DEFAULT_MOUNT, lock_dir=tmp_path,
        probe=lambda *a, **k: {"status": "ok"})
    record = sampler.sample()
    assert record["rpc"] is None
    assert record["locks"]["holders"] == 1
    assert record["locks"]["identity_complete"] is False


def test_a_lock_key_retains_its_zero_padded_report_format(monkeypatch):
    monkeypatch.setattr(
        mount_latency.os, "stat",
        lambda _path: os.stat_result(
            (0o100600, 104, os.makedev(0, 30), 1, 0, 0, 0, 0, 0, 0)))
    assert mount_latency.lock_key(Path("/dev/shm/anything")) == "00:1e:104"


def test_a_high_major_number_is_not_truncated_by_the_padding(monkeypatch):
    """The width is a minimum, not a field size.

    sparky's ``/tmp`` is major 259, which the kernel writes ``103``.  A fix for
    the major-0 case that clamped to two digits would break the box the tool
    was developed on.
    """

    monkeypatch.setattr(
        mount_latency.os, "stat",
        lambda _path: os.stat_result(
            (0o100600, 1441858, os.makedev(259, 2), 1, 0, 0, 0, 0, 0, 0)))

    assert mount_latency.lock_key(Path("/tmp/x")) == "103:02:1441858"

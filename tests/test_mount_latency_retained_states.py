"""Issue #2066: passive NFS evidence must not create the states it measures.

Synthetic mountstats, not production NFS qualification.  The active probe's
claim-<pid>.tmp/.done population is distinct from done/<action-key>.json.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import mount_latency  # noqa: E402


def retained_state_snapshot(*, age=100, opened=100, closed=90, errors=2,
                            device="storage.example:/export", point="/queue",
                            target="/queue/jobs"):
    """Counter columns are ops/trans/timeouts/bytes/bytes/queue/rtt/exec/errors."""
    return mount_latency.read_mountstats(Path(target), f"""\
device {device} mounted on {point} with fstype nfs4 statvers=1.1
\topts:\trw,vers=4.2,hard,proto=tcp,nconnect=4,timeo=600,retrans=2
\tage:\t{age}
\tper-op statistics
\tOPEN: {opened} {opened + 4} 1 1000 2000 10 20 30 {errors}
\tCLOSE: {closed} {closed + 3} 2 1000 2000 10 20 30 {errors}
""")


def test_retained_state_rpc_view_keeps_close_and_error_counter_deltas():
    before = retained_state_snapshot()
    after = retained_state_snapshot(opened=120, closed=105, errors=5)
    view = mount_latency._rpc_view(
        mount_latency._op_delta(before.ops, after.ops), window_s=10.0)

    assert "CLOSE" in view["by_op"], "OPEN without CLOSE cannot diagnose retention"
    for name, count in (("OPEN", 20), ("CLOSE", 15)):
        row = view["by_op"][name]
        assert row["ops_per_s"] == pytest.approx(count / 10)
        # Retain the deltas, not just means: errors are RPC errors, not proof
        # that a particular application close failed or was skipped.
        assert row["ops"] == count
        assert row["trans"] == count
        assert row["errors"] == 3
        assert row["timeouts"] == 0


def test_retained_state_passive_cli_never_runs_probe_or_lock_scan(
    tmp_path, monkeypatch, capsys,
):
    observations = []
    # Build through the real parser BEFORE replacing it.  Calling the helper
    # inside observed_mountstats recursed in the previous acceptance file.
    snapshot = retained_state_snapshot()

    def observed_mountstats(*args, **kwargs):
        observations.append(1)
        return snapshot

    def forbidden_active_leg(*args, **kwargs):
        pytest.fail("passive observation invoked an active probe or lock scan")

    monkeypatch.setattr(mount_latency, "read_mountstats", observed_mountstats)
    monkeypatch.setattr(mount_latency.MountSampler, "_run_probe", forbidden_active_leg)
    monkeypatch.setattr(mount_latency, "lock_contention", forbidden_active_leg)
    monkeypatch.setattr(mount_latency.os, "fork", forbidden_active_leg)
    measured_path = tmp_path / "uncreated-mount"
    assert mount_latency.main([
        "--passive", "--once", "--json", "--mount", str(measured_path),
        "--record-dir", str(tmp_path / "records"),
        "--lock-dir", str(tmp_path / "uncreated-locks"),
    ]) == 0
    record = json.loads(capsys.readouterr().out)
    assert record["mode"] == "passive"
    assert record["probe"]["status"] == "skipped"
    assert not record.get("attribution"), "no probe footprint was measured"
    assert not record.get("locks"), "no lock observation was performed"
    assert record["rpc"] is None, "one snapshot has no rate window"
    assert len(observations) == 1
    assert not measured_path.exists()
    assert not (tmp_path / "uncreated-locks").exists()
    saved = list((tmp_path / "records").glob("*.jsonl"))
    assert len(saved) == 1
    assert json.loads(saved[0].read_text())["mode"] == "passive"


def test_retained_state_default_sample_is_explicitly_active(monkeypatch):
    probes = []
    # As above, the fake must return a precomputed snapshot, not invoke itself.
    snapshot = retained_state_snapshot()
    monkeypatch.setattr(mount_latency, "read_mountstats",
                        lambda *a, **k: snapshot)
    monkeypatch.setattr(mount_latency, "lock_contention", lambda *a, **k: {})
    monkeypatch.setattr(mount_latency.MountSampler, "_run_probe",
                        lambda self: probes.append(1) or {"status": "ok"})
    record = mount_latency.MountSampler(Path("/queue/jobs")).sample()
    assert probes == [1], "default collection must retain its existing active leg"
    assert record["mode"] == "active", "mixed logs must identify probe-generated traffic"
    assert record["probe"]["status"] == "ok"


def test_retained_state_passive_windows_keep_selected_mount_identity(monkeypatch):
    snapshots = iter([retained_state_snapshot(), retained_state_snapshot(
        age=110, opened=120, closed=105, errors=5)])
    clock = iter([1000.0, 1010.0])
    monkeypatch.setattr(mount_latency, "read_mountstats",
                        lambda *args, **kwargs: next(snapshots))
    monkeypatch.setattr(mount_latency.time, "time", lambda: next(clock))
    sampler = mount_latency.MountSampler(Path("/queue/jobs"), passive=True)
    first, second = sampler.sample(), sampler.sample()

    assert first["rpc"] is None
    assert second["unix"] == 1010.0
    assert second["window_s"] == 10.0
    assert second["mode"] == "passive"
    assert second["mount_point"] == "/queue/jobs"  # requested observation path
    identity = second["mount"]
    assert identity["mount_point"] == "/queue"  # actual selected mount
    assert identity["device"] == "storage.example:/export"
    assert identity["fstype"] == "nfs4"
    assert identity["options"]["hard"] == "yes"
    assert identity["options"]["retrans"] == "2"
    assert identity["options"]["nconnect"] == "4"
    assert second["rpc"]["by_op"]["CLOSE"]["ops_per_s"] == 1.5
    assert not second.get("attribution")


@pytest.mark.parametrize("boundary", ["source", "mount-point", "age", "counters"])
def test_retained_state_passive_windows_do_not_cross_identity_or_counter_reset(
    monkeypatch, boundary,
):
    changed = dict(age=110, opened=120, closed=105)
    if boundary == "source":
        changed["device"] = "other.example:/export"
    elif boundary == "mount-point":
        changed["point"] = "/queue/jobs"
    elif boundary == "age":
        changed["age"] = 1
    else:
        changed["opened"] = 1  # age can increase despite reset/namespace changes
    final = dict(changed, age=changed["age"] + 10,
                 opened=changed["opened"] + 20, closed=changed["closed"] + 10)
    snapshots = iter([retained_state_snapshot(), retained_state_snapshot(**changed),
                      retained_state_snapshot(**final)])
    clock = iter([1000.0, 1010.0, 1020.0])
    monkeypatch.setattr(mount_latency, "read_mountstats",
                        lambda *args, **kwargs: next(snapshots))
    monkeypatch.setattr(mount_latency.time, "time", lambda: next(clock))
    sampler = mount_latency.MountSampler(Path("/queue/jobs"), passive=True)
    sampler.sample()
    reset = sampler.sample()
    assert reset["rpc"] is None, "different counter populations are not a window"
    assert reset["window_s"] == 0
    assert reset.get("note"), "the discontinuity must be visible in retained JSON"
    resumed = sampler.sample()
    assert resumed["window_s"] == 10.0
    assert resumed["rpc"]["by_op"]["OPEN"]["ops_per_s"] == 2.0


@pytest.mark.parametrize("oversized", [False, True])
def test_retained_state_mountstats_proc_read_is_bounded(monkeypatch, oversized):
    """A proc read may consume at most 4 MiB plus one overflow sentinel.

    A valid-looking prefix followed by an over-limit tail is incomplete, not
    a usable counter snapshot. Both read-all and streaming implementations can
    satisfy this bound; explicit text fixtures retain the existing parser API.
    """
    cap = 4 * 1024 * 1024
    text = ("device storage.example:/export mounted on /queue with fstype nfs4 "
            "statvers=1.1\n\topts:\trw,vers=4.2,hard,proto=tcp\n\tage:\t100\n"
            "\tper-op statistics\n\tOPEN: 100 100 0 0 0 0 0 0 0\n")
    if oversized:
        text += "x" * (cap + 2)

    class AccountedMountstats:
        def __init__(self, binary):
            self.source = (io.BytesIO(text.encode("utf-8")) if binary
                           else io.StringIO(text))
            self.consumed = 0

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.source.close()

        def read(self, size=-1):
            assert 0 <= size <= cap + 1, "unbounded mountstats read"
            result = self.source.read(size)
            self.consumed += len(result)
            assert self.consumed <= cap + 1, "mountstats total read bound exceeded"
            return result

        def readline(self, size=-1):
            assert 0 <= size <= cap + 1, "unbounded mountstats line read"
            result = self.source.readline(size)
            self.consumed += len(result)
            assert self.consumed <= cap + 1, "mountstats total read bound exceeded"
            return result

        def __iter__(self):
            return self

        def __next__(self):
            result = self.readline(cap + 1 - self.consumed)
            if not result:
                raise StopIteration
            return result

    actual_open = Path.open
    reads = []

    def mountstats_open(path, mode="r", *args, **kwargs):
        if str(path) != "/proc/self/mountstats":
            return actual_open(path, mode, *args, **kwargs)
        stream = AccountedMountstats(binary="b" in mode)
        reads.append(stream)
        return stream

    monkeypatch.setattr(Path, "open", mountstats_open)
    result = mount_latency.read_mountstats(Path("/queue/jobs"))
    assert reads, "the real procfs read path must be exercised"
    if oversized:
        assert result is None, "truncated mountstats must not yield counters"
    else:
        assert result is not None and result.ops["OPEN"]["ops"] == 100


def test_retained_state_missing_mountstats_is_not_reported_as_local(monkeypatch):
    monkeypatch.setattr(mount_latency, "read_mountstats", lambda *a, **k: None)
    record = mount_latency.MountSampler(Path("/queue/jobs"), passive=True).sample()
    assert record["rpc"] is None
    assert record["mount"]["transport"] == "unknown", "missing is not local ZFS"
    assert record.get("note"), "unavailable observations must be explicit"

"""Proposed opt-in measured scratch placement (Refs #1182), NOT deployed support.

The literal v1 schemas began as a reviewed proposal and now have in-progress
source wiring, not qualification/deployment proof. Profile fixture bytes,
elapsed times, filesystem/device observations and GPU samples are labelled
synthetic observations, never production measurements or Stage-B traffic.
199,051,640,832 is the issue's occupancy ceiling (186 GiB); explicitly using
that number as traffic here is fixture input, not an inference from occupancy.

The placement inner action never runs or spills. Separate admitted tiny
recorder controls execute real 1-KiB filesystem semantics, not qualification.
Real prepare -> freeze -> seal -> CAS request -> publication_row -> publish -> adaptive admission -> tokens/rename
-> finish/immutable attempt are exercised. Only kernel/clock/hostname inputs
and owned filesystem locations are substituted. The primary old-source RED
must be a SUCCESSFUL slower-host claim, not an absent API or schema failure.
Run only inside the parent's admitted CPU-only PB test action; its real CAS
receipt is separate from the synthetic inner queue's durable claim evidence.
"""
from __future__ import annotations

import json
import shlex
import socket
import subprocess
import sys
import threading
import time
from copy import deepcopy
from pathlib import Path
from typing import Any, cast

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "tools" / "fleet"))
import pbrun  # noqa: E402  # type: ignore[import-not-found]
import worker_loop  # noqa: E402  # type: ignore[import-not-found]
from prismabuild import adaptive_cpu, adaptive_gpu, materialize, pool  # noqa: E402
from prismabuild import core as pb  # noqa: E402
from prismabuild import local_scratch as ls  # noqa: E402

IO_ENV = "PRISMABUILD_LOCAL_SCRATCH_IO"
IO_SCHEMA = "prismabuild.local_scratch_io.v1"
PROFILE_SCHEMA = "prismabuild.local_scratch_io_profile.v1"
PLACEMENT_SCHEMA = "prismabuild.local_scratch_io_placement.v1"
CAPABILITY = "local-scratch-io-v1"
PROFILES = "local_scratch_io_profiles"
DEVICES = "local_scratch_devices"
PLACEMENT = "local_scratch_io_placement"
# Supported protocol labels only; these observations remain synthetic.
CONTRACT = "prismabuild.local_scratch_io.buffered_seq_sync.v1"
METHOD = "buffered-sequential-write-fdatasync-read.v1"
SLOW, FAST = "fixture-slow", "fixture-fast"
ISSUE_BYTES = 199_051_640_832
GIB = 1 << 30
CAPACITY = {"cpu": 4, "gpu": 1, "mem_gb": 32, "spool_gb": 186}
TIERS = {"preferred": [0, 1, 2, 3], "fallback": []}
SHARED_RUNTIME = Path("/mnt/shared/prismabuild-fleet/repo")


def _canonical(body):
    # The digest covers the profile body WITHOUT artifact_sha256, not itself.
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()


def _record(path) -> dict[str, Any]:
    record = pool._read_json(path)
    assert isinstance(record, dict), f"missing durable queue record: {path}"
    return cast(dict[str, Any], record)


def _checkout(root):
    """Git mutation is confined to this admitted test's private checkout."""
    work = root / "work"
    work.mkdir()
    (work / "seed.txt").write_text("sealed fixture\n", encoding="utf-8")
    for args in (("init", "-q"), ("config", "user.email", "fixture@example.invalid"),
                 ("config", "user.name", "PB scratch fixture"),
                 ("add", "seed.txt"), ("commit", "-qm", "fixture")):
        done = subprocess.run(["git", "-C", str(work), *args],
                              capture_output=True, text=True)
        assert done.returncode == 0, done.stderr
    return work


class ScratchFleet:
    def __init__(self, root, monkeypatch, *, cpu_tiers=None):
        # A genuine shared runtime, not an imaginary path to bypass the gate.
        for relative in ("tools/prismabuild_worker.py", "tools/docker"):
            assert (SHARED_RUNTIME / relative).is_file(), (
                f"fixture dependency unavailable (not behavioral RED): {relative}")
        self.root, self.monkeypatch = root, monkeypatch
        self.tiers = deepcopy(TIERS if cpu_tiers is None else cpu_tiers)
        self.actual_host = socket.gethostname()
        self.actual_root_identity = getattr(ls, "root_identity", None)
        self.clock = [time.time()]
        monkeypatch.setattr(time, "time", lambda: self.clock[0])
        monkeypatch.setattr(pbrun, "SH", root)
        monkeypatch.setattr(pbrun, "RUNTIME_ROOT", SHARED_RUNTIME)
        monkeypatch.setattr(pbrun, "CONTAINER_WRAPPER_DIR", SHARED_RUNTIME / "tools")
        monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", root / "box-state")
        monkeypatch.setenv("PRISMABUILD_BOX_STATE_ROOT", str(root / "box-state"))
        self.host = SLOW
        monkeypatch.setattr(socket, "gethostname", lambda: self.host)
        self.cpu_fault = self.gpu_fault = None
        monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda _: self.cpu_sample())
        monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda _: self.gpu_sample())
        self.work = _checkout(root)
        self.scratch = str(root / "scratch")
        Path(self.scratch).mkdir()
        self.queue = pool.PoolQueue(root / "pb-queue")
        self.queue.ensure_layout()
        self.cas = pb.PrismaBuildCAS(root / "cas")
        # Independent current-device observations; profile mutation never updates these.
        self.devices = {host: {"filesystem": f"fixture-fs-{host}",
                               "device": f"fixture-device-{host}",
                               "root_inode": "101" if host == SLOW else "102"}
                        for host in (SLOW, FAST)}
        if self.actual_root_identity is not None:
            # Low-level independent filesystem observations, never a placement
            # verdict or a production qualification claim. Old-source primary
            # remains callable when this new observation seam does not exist.
            monkeypatch.setattr(ls, "root_identity", lambda root: deepcopy(self.devices[self.host]))
        self.profiles = {host: [self.profile(host, write_elapsed_s=elapsed)]
                         for host, elapsed in ((SLOW, 20.), (FAST, 2.))}
        for host in (SLOW, FAST):
            ledger = self.queue.ledger(host)
            ledger.configure_cpu_tiers(self.tiers)
            ledger.ensure_capacity(CAPACITY)
            self.announce(host)

    def cpu_sample(self):
        return {"sampled_unix": self.clock[0] - (10 if self.cpu_fault == "stale" else 0),
                "busy_cpus": 0., "psi_some": 0., "cpu_count": 4, "interval_s": 1.}

    def gpu_sample(self):
        return {"schema": "prismabuild.gpu_capacity.v1", "sample_id": str(self.clock[0]),
                "sampled_unix": self.clock[0] - (10 if self.gpu_fault == "stale" else 0),
                "complete": True, "attributed": True,
                "devices": [{"uuid": f"fixture-GPU-{self.host}", "power_w": 5.,
                             "power_limit_w": 140., "memory_domain": "shared_system",
                             "limited": False}],
                "host_total_bytes": 128 * GIB, "host_available_bytes": 100 * GIB,
                "memory_pressure_some": 0., "memory_pressure_full": 0.,
                "cpu_pressure_some": 0., "jobs": [],
                "foreign_processes": ([{"pid": 9000}] if self.gpu_fault == "foreign" else [])}

    def artifact(self, body):
        body = deepcopy(body)
        body.pop("artifact_sha256", None)
        entry, _ = self.cas.ingest_bytes(_canonical(body), input_id="fixture.scratch-profile")
        assert self.cas.input_path(entry).read_bytes() == _canonical(body)
        return {**body, "artifact_sha256": entry["sha256"]}

    def profile(self, host, **overrides):
        # 1 GiB measured over labelled elapsed observations; never configured bps.
        body = {"schema": PROFILE_SCHEMA, "host": host, "root": self.scratch,
                **self.devices[host], "profile_contract": CONTRACT, "method": METHOD,
                "envelope": {"root": self.scratch, "bytes": GIB,
                             "block_bytes": 1024 * 1024, "repetitions": 1},
                "pattern": "repeated-shake256-block.v1",
                "measured_unix": self.clock[0], "write_bytes": GIB, "read_bytes": 0,
                "write_elapsed_s": 2., "read_elapsed_s": None,
                "completed": True, "errors": [],
                "provenance": "synthetic test observation; no benchmark executed"}
        return self.artifact({**body, **overrides})

    def announce(self, host, *, tags=None, state=None):
        detail = {"observed_unix": self.clock[0], "load1": 0.,
                  "gpu_power_fraction": .05, "gpu_power_sampled_unix": self.clock[0],
                  PROFILES: deepcopy(self.profiles[host]),
                  DEVICES: {self.scratch: deepcopy(self.devices[host])}}
        self.queue.announce(host=host, tags=(["gb10", CAPABILITY] if tags is None else tags),
                            has_gpu=True, capacity=CAPACITY, cpu_tiers=self.tiers,
                            observed_capacity=(dict.fromkeys(CAPACITY, 0)
                                               if state == "draining" else CAPACITY),
                            observed_detail=detail, state=state)

    def build(self, *, opted_in=True, read_bytes=0, write_bytes=ISSUE_BYTES,
              max_age=60., retry=False):
        declaration = {"schema": IO_SCHEMA, "write_bytes": write_bytes,
                       "read_bytes": read_bytes, "profile_contract": CONTRACT,
                       "max_profile_age_s": max_age}
        variables = {"PQ_SPILL_ROOT": self.scratch, "PQ_SPILL_MAX_BYTES": str(ISSUE_BYTES),
                     "PRISMABUILD_LOCAL_SCRATCH_PAIRS": "PQ_SPILL_ROOT:PQ_SPILL_MAX_BYTES"}
        tags = ["--tag", "gb10"]
        if opted_in:
            variables[IO_ENV] = json.dumps(declaration, sort_keys=True)
            # Generic old API: new source must derive the same capability automatically.
            tags += ["--tag", CAPABILITY]
        extra = [part for name, value in variables.items() for part in ("--env", f"{name}={value}")]
        args = pbrun.parse_args([
            "--cwd", str(self.work), "--transport", "pool", "--no-default-env",
            "--gpu", "--cpus", "1", "--demand", "mem_gb=4", *tags, *extra,
            *(["--max-attempts", "2", "--retry-safe"] if retry else []),
            "--", "/bin/bash", "-c", "true"])
        # prepare_submission calls the real freeze_action_template.
        template = pbrun.prepare_submission(args)["template"]
        action = pbrun.seal_action_from_template(template)
        assert action["params"]["demand"] == {"cpu": 1, "mem_gb": 4, "gpu": 1, "spool_gb": 186}
        assert -(-ISSUE_BYTES // GIB) == 186
        required = action["params"]["placement"]["required_tags"]
        assert SLOW not in required and FAST not in required
        return args, action, declaration

    def publish(self, **options):
        args, self.action, self.declaration = self.build(**options)
        self.cas.publish_action_request(self.action)
        row = pbrun.publication_row(self.action, args=args, queue=self.queue)
        self.publication = deepcopy(row)
        self.queue.publish(**row)
        self.key = self.action["action_key"]
        return self.key

    def claim(self, host):
        self.host = host
        return self.queue.claim(owner=f"worker-{host}", capacity=CAPACITY, cpu_tiers=self.tiers,
                                adaptive_cpu=True, has_gpu=True, tags=["gb10", CAPABILITY])

    def ready_unspent(self):
        item = _record(self.queue.item_path(pool.READY, self.key))
        assert item["attempts"] == 0
        assert not self.queue.item_path(pool.CLAIMED, self.key).exists()
        assert not self.queue.lease_path(self.key).exists()
        assert not self.queue.passes_path(self.key).exists()
        for host in (SLOW, FAST):
            assert not self.queue.ledger(host).held_keys()
            assert self.queue.ledger(host).available() == CAPACITY

    def denial(self, host):
        return self.denial_for(self.key, host)

    def denial_for(self, key, host):
        self.host = host
        path = adaptive_cpu.local_state_base(self.queue.ledger(host).base) / pool.CLAIM_DENIALS
        records = adaptive_cpu.read_json(path).get("records", {}).values()
        return next(record for record in records if record["action_key"] == key)

    def mirror_real_denial(self, host, expected_reason):
        # Make the existing asynchronous observation copy deterministic, AFTER
        # genuine claim evaluation; no fabricated or edited verdict is published.
        denial = self.denial(host)
        assert denial["host"] == host and denial["action_key"] == self.key
        assert denial["reason"].startswith(expected_reason)
        ledger = self.queue.ledger(host)
        adaptive_cpu.adaptive_snapshot.copy_snapshot(
            adaptive_cpu.local_state_base(ledger.base), ledger.base / "adaptive")

    def receipt(self, claimed, host):
        # Called only AFTER the actual behavioral assertion/claim. No injected receipt.
        assert claimed is not None and claimed["claimed_host"] == host
        persisted = _record(self.queue.item_path(pool.CLAIMED, self.key))
        evidence = persisted[PLACEMENT]
        assert claimed[PLACEMENT] == evidence
        assert evidence["schema"] == PLACEMENT_SCHEMA
        assert evidence["selected_host"] == host
        assert evidence["declaration"] == self.declaration
        candidates = {row["host"]: row for row in evidence["candidates"]}
        chosen = candidates[host]
        profile = self.profiles[host][0]
        assert chosen["artifact_sha256"] == profile["artifact_sha256"]
        assert chosen["measured_unix"] == profile["measured_unix"]
        assert chosen["root_inode"] == profile["root_inode"]
        assert chosen["envelope"] == profile["envelope"]
        assert chosen["pattern"] == profile["pattern"]
        cost = 0.
        for direction in ("write", "read"):
            traffic = self.declaration[f"{direction}_bytes"]
            if traffic:
                rate = profile[f"{direction}_bytes"] / profile[f"{direction}_elapsed_s"]
                assert chosen[f"{direction}_bytes_per_s"] == pytest.approx(rate)
                cost += traffic / rate
        assert chosen["io_seconds"] == pytest.approx(cost)
        assert chosen["fit"] is True and chosen["exclusion"] is None
        assert self.queue.ledger(host).held()["spool_gb"] == 186
        assert self.queue.ledger(host).held()["gpu"] == 1
        return evidence, candidates


@pytest.fixture
def fleet(tmp_path, monkeypatch):
    return ScratchFleet(tmp_path, monkeypatch)


def test_199gb_scratch_yields_slower_host_and_claims_faster_with_measured_receipt(fleet):
    fleet.publish()
    claimed = fleet.claim(SLOW)                    # slower worker evaluates FIRST
    if claimed is not None:
        # Positive old-source witness before desired None, with real tokens/row.
        assert claimed["action_key"] == fleet.key
        assert claimed["claimed_host"] == SLOW
        assert fleet.queue.ledger(SLOW).held()["spool_gb"] == 186
        assert fleet.queue.ledger(SLOW).held()["gpu"] == 1
        assert fleet.queue.item_path(pool.CLAIMED, fleet.key).is_file()
    assert claimed is None, "capacity-only placement actually CLAIMED the slower host"
    fleet.ready_unspent()
    denial = fleet.denial(SLOW)
    assert "scratch" in denial["reason"] and "deferred" in denial["reason"]
    winner = fleet.claim(FAST)
    evidence, candidates = fleet.receipt(winner, FAST)
    assert set(candidates) == {SLOW, FAST}
    assert candidates[SLOW]["fit"] is True
    for host in (SLOW, FAST):
        profile = fleet.profiles[host][0]
        candidate = candidates[host]
        rate = profile["write_bytes"] / profile["write_elapsed_s"]
        assert candidate["artifact_sha256"] == profile["artifact_sha256"]
        assert candidate["measured_unix"] == profile["measured_unix"]
        assert candidate["write_bytes_per_s"] == pytest.approx(rate)
        assert candidate["io_seconds"] == pytest.approx(ISSUE_BYTES / rate)
    assert candidates[SLOW]["io_seconds"] > candidates[FAST]["io_seconds"]
    terminal = _record(fleet.queue.finish(fleet.key, status="executed", detail={}))
    assert terminal["detail"][PLACEMENT] == evidence
    attempts = cast(list[dict[str, Any]], fleet.queue.attempt_outcomes(terminal))
    assert len(attempts) == 1 and attempts[0]["claimed_host"] == FAST
    assert attempts[0]["detail"][PLACEMENT] == evidence
    assert not fleet.queue.ledger(FAST).held_keys()


def test_opted_out_profiles_cannot_change_identity_or_capacity_placement(fleet):
    _, first, _ = fleet.build(opted_in=False)
    fleet.profiles[SLOW] = []
    fleet.profiles[FAST] = ["corrupt observation"]
    for host in (SLOW, FAST):
        fleet.announce(host)
    _, second, _ = fleet.build(opted_in=False)
    assert first["action_key"] == second["action_key"]
    assert IO_ENV not in second["environment"]["variables"]
    assert CAPABILITY not in second["params"]["placement"]["required_tags"]
    fleet.publish(opted_in=False)
    claimed = fleet.claim(SLOW)
    assert claimed is not None and claimed["claimed_host"] == SLOW
    assert PLACEMENT not in claimed
    assert fleet.queue.ledger(SLOW).held()["spool_gb"] == 186


def test_same_filesystem_root_replacement_invalidates_profile_without_spending(fleet):
    fleet.publish()
    # Device/FSID unchanged; independent current directory inode changed.
    fleet.devices[FAST]["root_inode"] = "999"
    fleet.announce(FAST)
    assert fleet.claim(FAST) is None
    fleet.ready_unspent()


def test_opted_in_profile_rates_never_enter_action_identity(fleet):
    _, first, _ = fleet.build()
    fleet.profiles[FAST] = [fleet.profile(FAST, write_elapsed_s=4.)]
    fleet.announce(FAST)
    _, second, _ = fleet.build()
    assert first["action_key"] == second["action_key"]


@pytest.mark.parametrize("fault", ["missing", "envelope-missing", "pattern-missing",
                                   "stale", "corrupt", "nonfinite", "zero",
                                   "zero-elapsed", "method", "device", "filesystem",
                                   "incomplete", "errors"])
def test_unusable_required_local_profile_fails_closed_without_spending(fleet, fault):
    fleet.publish()
    body = deepcopy(fleet.profiles[FAST][0])
    if fault == "missing":
        fleet.profiles[FAST] = []
    elif fault in {"envelope-missing", "pattern-missing"}:
        body.pop(fault.removesuffix("-missing"))
        fleet.profiles[FAST] = [fleet.artifact(body)]
    elif fault == "corrupt":
        body["artifact_sha256"] = "0" * 64
        fleet.profiles[FAST] = [body]
    else:
        updates = {"stale": {"measured_unix": fleet.clock[0] - 61.},
                   # Finite canonical inputs; their derived rate overflows.
                   # A synthetic numeric boundary, not measured throughput.
                   "nonfinite": {"write_elapsed_s": 1e-320},
                   "zero": {"write_bytes": 0}, "zero-elapsed": {"write_elapsed_s": 0},
                   "method": {"method": "fixture.other-method.v1"},
                   "device": {"device": "fixture-replaced-device"},
                   "filesystem": {"filesystem": "fixture-other-filesystem"},
                   "incomplete": {"completed": False}, "errors": {"errors": ["fixture error"]}}
        fleet.profiles[FAST] = [fleet.artifact({**body, **updates[fault]})]
    fleet.announce(FAST)
    assert fleet.claim(FAST) is None
    fleet.ready_unspent()
    assert "scratch" in fleet.denial(FAST)["reason"]


def test_original_profile_age_is_not_refreshed_by_an_offer(fleet):
    fleet.publish(max_age=1.)
    original = fleet.profiles[FAST][0]["measured_unix"]
    fleet.clock[0] += 2.
    fleet.announce(FAST)                           # fresh TTL, ORIGINAL profile old
    assert fleet.profiles[FAST][0]["measured_unix"] == original
    assert fleet.claim(FAST) is None
    fleet.ready_unspent()


@pytest.mark.parametrize("read_required", [False, True])
def test_only_a_used_traffic_direction_requires_a_positive_rate(fleet, read_required):
    fleet.publish(read_bytes=GIB if read_required else 0)
    # Default profile has NO read observation, not a read speed of zero.
    claimed = fleet.claim(FAST)
    if read_required:
        assert claimed is None
        fleet.ready_unspent()
    else:
        fleet.receipt(claimed, FAST)


def test_unused_write_rate_is_not_required_for_read_only_traffic(fleet):
    fleet.profiles[FAST] = [fleet.profile(FAST, write_bytes=0, write_elapsed_s=None,
                                         read_bytes=GIB, read_elapsed_s=2.)]
    fleet.announce(FAST)
    fleet.publish(write_bytes=0, read_bytes=ISSUE_BYTES)
    fleet.receipt(fleet.claim(FAST), FAST)


def test_newer_unusable_profile_cannot_be_hidden_by_an_older_good_profile(fleet):
    fleet.publish()
    older = fleet.profiles[FAST][0]
    fleet.clock[0] += 1.
    newer = fleet.profile(FAST, completed=False, errors=["fixture interrupted measurement"])
    fleet.profiles[FAST] = [older, newer]
    fleet.announce(FAST)
    assert fleet.claim(FAST) is None
    fleet.ready_unspent()


def test_measured_read_and_write_service_times_are_added(fleet):
    for host in (SLOW, FAST):
        fleet.profiles[host] = [fleet.profile(host, write_elapsed_s=20. if host == SLOW else 2.,
                                             read_bytes=GIB, read_elapsed_s=4.)]
        fleet.announce(host)
    fleet.publish(read_bytes=3 * GIB)
    assert fleet.claim(SLOW) is None
    fleet.ready_unspent()
    fleet.receipt(fleet.claim(FAST), FAST)


@pytest.mark.parametrize("why", ["full", "draining", "incompatible", "gone"])
def test_unavailable_faster_host_ends_yield_but_preserves_real_admission(fleet, why):
    fleet.publish()
    if why == "full":
        assert fleet.queue.ledger(FAST).acquire("b" * 64, {"mem_gb": 32})
    elif why == "draining":
        fleet.announce(FAST, state="draining")
    elif why == "incompatible":
        fleet.announce(FAST, tags=["x86", CAPABILITY])
    else:
        (fleet.queue.root / pool.WORKERS / f"{FAST}.json").unlink()
    _, candidates = fleet.receipt(fleet.claim(SLOW), SLOW)
    if why in {"full", "draining"}:
        assert candidates[FAST]["fit"] is False
        assert isinstance(candidates[FAST]["exclusion"], str) and candidates[FAST]["exclusion"]


def test_equal_measured_cost_never_yields(fleet):
    fleet.profiles[SLOW] = [fleet.profile(SLOW, write_elapsed_s=2.)]
    fleet.announce(SLOW)
    fleet.publish()
    fleet.receipt(fleet.claim(SLOW), SLOW)


def test_real_faster_host_gpu_refusal_ends_the_independent_yield(fleet):
    fleet.publish()
    assert fleet.claim(SLOW) is None
    fleet.ready_unspent()
    fleet.gpu_fault = "foreign"                  # real GPU decision, not a fake denial
    assert fleet.claim(FAST) is None
    fleet.mirror_real_denial(FAST, "adaptive_gpu_refused")
    assert not fleet.queue.ledger(FAST).held_keys()
    fleet.gpu_fault = None
    fleet.receipt(fleet.claim(SLOW), SLOW)


def test_transition_busy_on_faster_host_is_not_an_admission_refusal(fleet):
    fleet.publish()
    acquired, release = threading.Event(), threading.Event()
    errors = []

    def independent_lock_holder():
        try:
            # Different thread: posix_lock's same-thread nesting is reentrant.
            with fleet.queue._transition_locked(fleet.key) as locked:
                assert locked
                acquired.set()
                assert release.wait(10.), "test lock release signal timed out"
        except BaseException as exc:
            errors.append(exc)
            acquired.set()

    holder = threading.Thread(target=independent_lock_holder, name="real-transition-lock-holder")
    holder.start()
    try:
        assert acquired.wait(5.), "different thread did not acquire the real lock"
        assert not errors and holder.is_alive()
        assert fleet.claim(FAST) is None
        assert fleet.denial(FAST)["reason"] == "transition_busy"
    finally:
        release.set()
        holder.join(timeout=5.)
        assert not holder.is_alive(), "test transition-lock holder did not finish"
        assert not errors, f"real lock holder failed: {errors!r}"
    fleet.mirror_real_denial(FAST, "transition_busy")
    assert fleet.claim(SLOW) is None
    fleet.ready_unspent()
    fleet.receipt(fleet.claim(FAST), FAST)


def test_profile_expiry_during_slow_offer_read_is_rechecked_before_tokens(fleet, monkeypatch):
    fleet.publish(max_age=1.)
    read_json = pool._read_json
    peer = fleet.queue.root / pool.WORKERS / f"{FAST}.json"
    delayed = []

    def delayed_observation(path, *args, **kwargs):
        value = read_json(path, *args, **kwargs)
        if Path(path) == peer and not delayed:
            delayed.append(True)
            fleet.clock[0] += 2.                 # shared read delay, not a decision patch
        return value

    monkeypatch.setattr(pool, "_read_json", delayed_observation)
    assert fleet.claim(SLOW) is None
    assert delayed, "fixture did not exercise the actual offer read"
    fleet.ready_unspent()
    assert "scratch" in fleet.denial(SLOW)["reason"]


def test_retry_reobserves_profiles_strips_claim_evidence_and_keeps_immutable_history(fleet):
    fleet.publish(retry=True)
    assert fleet.claim(SLOW) is None
    first, _ = fleet.receipt(fleet.claim(FAST), FAST)
    ready = _record(fleet.queue.finish(fleet.key, status="failed", detail={}))
    assert PLACEMENT not in ready
    assert ready["attempts"] == 1
    first_link = fleet.queue.root / ready["attempt_history"][0]["outcome"]
    immutable = first_link.read_bytes()
    assert json.loads(immutable)["detail"][PLACEMENT] == first
    fleet.clock[0] += 2.
    for host, elapsed in ((SLOW, 1.), (FAST, 10.)):
        fleet.profiles[host] = [fleet.profile(host, write_elapsed_s=elapsed)]
        fleet.announce(host)
    assert fleet.claim(FAST) is None              # now SLOW is actually cheaper
    second, _ = fleet.receipt(fleet.claim(SLOW), SLOW)
    assert second != first
    terminal = _record(fleet.queue.finish(fleet.key, status="executed", detail={}))
    outcomes = cast(list[dict[str, Any]], fleet.queue.attempt_outcomes(terminal))
    assert [row["claimed_host"] for row in outcomes] == [FAST, SLOW]
    assert outcomes[0]["detail"][PLACEMENT] == first
    assert outcomes[1]["detail"][PLACEMENT] == second
    assert first_link.read_bytes() == immutable


@pytest.mark.parametrize("opted_in", [False, True], ids=["capacity-only", "measured"])
@pytest.mark.parametrize("fault", ["gpu-stale", "gpu-foreign"])
def test_measured_preference_cannot_override_existing_freshness_or_gpu_safety(
        fleet, fault, opted_in):
    fleet.publish(opted_in=opted_in)              # FAST is cheaper, but still GPU-unsafe
    fleet.gpu_fault = fault.removeprefix("gpu-")
    assert fleet.claim(FAST) is None
    assert not fleet.queue.ledger(FAST).held_keys()
    assert fleet.queue.item_path(pool.READY, fleet.key).exists()
    assert fleet.denial(FAST)["reason"].startswith("adaptive_gpu_refused")


@pytest.mark.parametrize("opted_in", [False, True], ids=["capacity-only", "measured"])
def test_stale_cpu_sample_keeps_normal_reserved_preferred_admission(fleet, opted_in):
    # Existing CPU policy: stale samples forbid measurement/unbounded/borrow
    # proof paths, not a normal declared CPU covered by free preferred tokens.
    # Do not invent a stricter CPU freshness policy for scratch placement.
    fleet.publish(opted_in=opted_in)
    fleet.cpu_fault = "stale"
    claimed = fleet.claim(FAST)
    assert claimed is not None and claimed["action_key"] == fleet.key
    assert claimed["cpu_allocation"] == {"preferred": [0], "fallback": []}
    ledger = fleet.queue.ledger(FAST)
    metadata = adaptive_cpu.read_json(ledger.held_dir / fleet.key / adaptive_cpu.METADATA)
    assert fleet.clock[0] - metadata["sampled_unix"] > adaptive_cpu.MAX_SAMPLE_AGE_S
    assert metadata["borrowing"] is False and metadata["preferred_borrow"] == 0
    assert ledger.held()["cpu"] == 1 and ledger.held()["spool_gb"] == 186
    if opted_in:
        fleet.receipt(claimed, FAST)
    fleet.queue.finish(fleet.key, status="executed", detail={})
    assert ledger.available() == CAPACITY


@pytest.mark.parametrize("fact", ["envelope", "pattern"])
def test_apparently_faster_incomparable_workload_cannot_induce_yield(fleet, fact):
    body = deepcopy(fleet.profiles[FAST][0])
    if fact == "envelope":
        body["envelope"]["bytes"] *= 2
        body["write_bytes"] *= 2
    else:
        body["pattern"] = "fixture.other-pattern.v1"
    body["write_elapsed_s"] = 1.
    fleet.profiles[FAST] = [fleet.artifact(body)]
    fleet.announce(FAST)
    fleet.publish()
    _, candidates = fleet.receipt(fleet.claim(SLOW), SLOW)
    assert candidates[FAST]["fit"] is False
    assert candidates[FAST]["exclusion"] == "incomparable_workload"


@pytest.mark.parametrize("options", [
    {"write_bytes": True}, {"read_bytes": -1}, {"write_bytes": 0, "read_bytes": 0},
    {"max_age": 0}, {"max_age": float("inf")}, {"max_age": True},
])
def test_real_prepare_refuses_invalid_traffic_before_publication(fleet, options):
    with pytest.raises(SystemExit, match=IO_ENV):
        fleet.build(**options)
    assert fleet.queue.ready_items() == []


def _variables(fleet):
    return {"PATH": "/usr/bin:/bin", "PQ_SPILL_ROOT": fleet.scratch,
            "PQ_SPILL_MAX_BYTES": str(ISSUE_BYTES),
            "PRISMABUILD_LOCAL_SCRATCH_PAIRS": "PQ_SPILL_ROOT:PQ_SPILL_MAX_BYTES",
            IO_ENV: json.dumps({"schema": IO_SCHEMA, "write_bytes": ISSUE_BYTES,
                                "read_bytes": 0, "profile_contract": CONTRACT,
                                "max_profile_age_s": 60.})}


@pytest.mark.parametrize("fault", ["pairs-missing", "multiple-pairs", "slurm", "schema"])
def test_traffic_requires_pool_one_pair_and_exact_version_at_prepare(fleet, fault):
    variables = _variables(fleet)
    if fault == "pairs-missing":
        variables.pop("PRISMABUILD_LOCAL_SCRATCH_PAIRS")
    elif fault == "multiple-pairs":
        variables.update(OTHER_ROOT=str(fleet.root / "other"), OTHER_MAX="1")
        variables["PRISMABUILD_LOCAL_SCRATCH_PAIRS"] += ",OTHER_ROOT:OTHER_MAX"
    elif fault == "schema":
        variables[IO_ENV] = variables[IO_ENV].replace(IO_SCHEMA, "unknown.v9")
    extra = [part for k, v in variables.items() for part in ("--env", f"{k}={v}")]
    args = pbrun.parse_args(["--cwd", str(fleet.work), "--no-default-env", "--tag", "gb10",
                             "--transport", "slurm" if fault == "slurm" else "pool", *extra,
                             "--", "/bin/bash", "-c", "true"])
    with pytest.raises(SystemExit, match=IO_ENV):
        pbrun.prepare_submission(args)
    assert fleet.queue.ready_items() == []


def test_freeze_cannot_omit_opted_in_worker_code_fence(fleet):
    with pytest.raises(SystemExit, match=CAPABILITY):
        pbrun.freeze_action_template(
            command=["/bin/bash", "-c", "true"], cwd=fleet.work, logical_cwd=".",
            demand={"cpu": 1, "mem_gb": 4, "spool_gb": 186},
            placement={"required_tags": ["gb10"]}, variables=_variables(fleet),
            determinism="stochastic", retry_policy={"max_attempts": 1, "retry_safe": False},
            host_class=None, measurement=False, transport="pool", pool_measurement_class=False,
            data_manifest_path=None, checkout_snapshot_max_bytes=512 * 1024 * 1024,
            snapshot_refs=[], exclusive=False, gpu_memory_gb=None, execution_timeout_s=None,
            progress=None, profile=None, wrapper_dir=SHARED_RUNTIME / "tools")


def test_direct_unsealed_publication_cannot_forge_the_capability(fleet):
    with pytest.raises(pool.PoolContractError, match="scratch"):
        fleet.queue.publish(action_key="e" * 64, cas_root=fleet.cas.root,
                            worker_script=SHARED_RUNTIME / "tools/prismabuild_worker.py",
                            checkout_root=fleet.work, tags=["gb10", CAPABILITY], needs_gpu=True,
                            resources={"cpu": 1, "gpu": 1, "mem_gb": 4, "spool_gb": 186})
    assert fleet.queue.ready_items() == []


@pytest.mark.parametrize("fault", ["erase", "erase-and-untag", "traffic", "resources"])
def test_forged_ready_projection_cannot_bypass_the_actual_sealed_request(fleet, fault):
    fleet.publish()
    path = fleet.queue.item_path(pool.READY, fleet.key)
    item = _record(path)
    if fault.startswith("erase"):
        item.pop(IO_ENV)
        if fault == "erase-and-untag":
            item["tags"].remove(CAPABILITY)
    elif fault == "traffic":
        item[IO_ENV]["declaration"]["write_bytes"] += 1
    else:
        item["resources"]["spool_gb"] = 1
    pool._write_json_atomic(path, item)             # owned corrupt-row observation
    assert fleet.claim(FAST) is None
    fleet.ready_unspent()
    assert fleet.denial(FAST)["reason"] == "local_scratch_io_intent_invalid"


def test_no_capacity_legacy_claim_cannot_bypass_opted_in_admission(fleet):
    fleet.publish()
    fleet.host = FAST
    assert fleet.queue.claim(tags=["gb10", CAPABILITY], has_gpu=True) is None
    fleet.ready_unspent()


def test_actual_worker_offer_path_keeps_opt_out_and_merges_configured_inputs(fleet):
    assert worker_loop.build_parser().parse_args([]).local_scratch_profile_config is None
    assert worker_loop.scratch_observed_detail(None, None) == {}
    config = fleet.root / "profile-inputs.json"
    config.write_text(json.dumps({"schema": ls.CONFIG_SCHEMA, "profiles": []}))
    reader = ls.ProfileInputs(config, source_root=REPO, checkout_root=fleet.root / "verification",
                              producer_python=sys.executable)
    assert worker_loop.scratch_observed_detail(None, reader)[PROFILES] == []
    # Structural closure control supplements the real loader/CAS controls;
    # it is not the primary behavioral RED or a deployed worker run.
    import inspect
    source = inspect.getsource(worker_loop._run_loop)
    assert "scratch_observed_detail(observer, profile_inputs)" in source
    assert "observed_detail=observed_detail" in source
    assert "tags.append(local_scratch.IO_CAPABILITY)" in source


def _recorder_action(fleet, *, changed_source=False, extra_env=None):
    # Real source files in the sealed fixture snapshot, not guessed hashes.
    for name in ls.PRODUCER_FILES:
        target = fleet.work / name
        target.parent.mkdir(parents=True, exist_ok=True)
        raw = (REPO / name).read_bytes()
        if changed_source and name == ls.RECORDER:
            raw += b"\n# harmless source-identity mismatch fixture\n"
        target.write_bytes(raw)
    variables = {"PQ_PROFILE_ROOT": fleet.scratch, "PQ_PROFILE_MAX_BYTES": "1024",
                 "PRISMABUILD_LOCAL_SCRATCH_PAIRS": "PQ_PROFILE_ROOT:PQ_PROFILE_MAX_BYTES",
                 "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"}
    variables.update(extra_env or {})
    extra = [part for k, v in variables.items() for part in ("--env", f"{k}={v}")]
    args = pbrun.parse_args([
        "--cwd", str(fleet.work), "--transport", "pool", "--no-default-env", "--tag", "gb10",
        "--cpus", "1", "--demand", "mem_gb=4", "--timeout-s", "30", *extra,
        "--", sys.executable, "-I", "-S", ls.RECORDER, "--root", fleet.scratch,
        "--bytes", "1024", "--block-bytes", "256", "--repetitions", "1",
        "--result", ls.PROFILE_RESULT])
    template = pbrun.prepare_submission(args)["template"]
    action = pbrun.seal_action_from_template(template)
    fleet.cas.publish_action_request(action)
    return action


@pytest.mark.parametrize("hook", ["PATH", "BASH_ENV", "LD_PRELOAD", "LD_LIBRARY_PATH", "PYTHONPATH"])
def test_recorder_freeze_rejects_forged_path_tee_startup_and_loader_hooks(fleet, hook):
    with pytest.raises(SystemExit, match="scratch recorder"):
        _recorder_action(fleet, extra_env={hook: str(fleet.root / "untrusted")})
    assert not list(Path(fleet.scratch).iterdir())
    assert not (fleet.work / ls.PROFILE_RESULT).exists()


def _standalone_inner_reader_context(monkeypatch):
    # Only the inherited outer reader tuple is isolated. Kernel containment,
    # PB affinity/resources/native bounds and the real core gates are untouched.
    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE", "PRISMABUILD_READER_HELPER_ROOT"):
        monkeypatch.delenv(name, raising=False)


def test_recorder_file_result_and_toolchain_cannot_be_forged_into_execution(fleet, monkeypatch):
    _standalone_inner_reader_context(monkeypatch)
    fleet.host = fleet.actual_host
    monkeypatch.setattr(pb, "_probe_nvidia_accelerators", lambda **kw: [])
    action = _recorder_action(fleet)
    assert action["task"]["argv"] == action["params"]["command"]
    assert action["task"]["argv"][1:3] == ["-I", "-S"]
    assert action["task"]["result_path"] == ls.PROFILE_RESULT
    forged = deepcopy(action)
    forged.pop("action_key")
    forged["environment"]["toolchain"]["argv0.sha256"] = "0" * 64
    forged = cast(dict[str, Any], pb.seal_action(forged))
    fleet.cas.publish_action_request(forged)
    with materialize._execution_checkout(
            {"action_key": forged["action_key"], "cas_root": str(fleet.cas.root),
             "checkout_snapshot": forged["params"]["checkout_snapshot"]},
            local_checkout_root=fleet.root / "execution") as checkout:
        with pytest.raises(pb.ActionContractError, match="toolchain"):
            pb.run_local_action(forged, cas_root=fleet.cas.root, checkout_root=checkout,
                                timeout_seconds=30)
        assert not (checkout / ls.PROFILE_RESULT).exists()
    assert fleet.cas.lookup(forged) is None
    assert not list(Path(fleet.scratch).iterdir())


def _profile_reader(fleet, action, digest):
    config = fleet.root / "profile-inputs.json"
    ref = {"cas_root": str(fleet.cas.root), "action_key": action["action_key"],
           "artifact_sha256": digest, "root": fleet.scratch, "profile_contract": ls.PROFILE_CONTRACT}
    config.write_text(json.dumps({"schema": ls.CONFIG_SCHEMA, "profiles": [ref]}))
    return ls.ProfileInputs(config, source_root=REPO, checkout_root=fleet.root / "verification",
                            producer_python=sys.executable)


def test_worker_loader_refuses_hash_only_input_without_successful_execution_receipt(fleet):
    action = _recorder_action(fleet)
    reader = _profile_reader(fleet, action, fleet.profiles[FAST][0]["artifact_sha256"])
    assert fleet.cas.lookup(action) is None
    assert worker_loop.scratch_observed_detail(None, reader)[PROFILES] == []
    assert not reader.verified


@pytest.mark.parametrize("changed_source", [False, True], ids=["real-source", "wrong-source"])
def test_real_admitted_tiny_recorder_cas_result_is_bound_into_worker_observation(
        fleet, monkeypatch, changed_source):
    """Actual 1-KiB CPU filesystem semantics, NOT representative profile qualification.

    Parent selects/runs this only in its admitted PB action. No fabricated
    producer attestation or execution receipt; core executes and publishes it.
    """
    _standalone_inner_reader_context(monkeypatch)
    fleet.host = fleet.actual_host
    assert fleet.actual_root_identity is not None
    monkeypatch.setattr(ls, "root_identity", fleet.actual_root_identity)
    monkeypatch.setattr(pb, "_probe_nvidia_accelerators", lambda **kw: [])
    action = _recorder_action(fleet, changed_source=changed_source)
    with materialize._execution_checkout(
            {"action_key": action["action_key"], "cas_root": str(fleet.cas.root),
             "checkout_snapshot": action["params"]["checkout_snapshot"]},
            local_checkout_root=fleet.root / "execution") as checkout:
        result = pb.run_local_action(action, cas_root=fleet.cas.root, checkout_root=checkout,
                                     timeout_seconds=30)
    assert action["task"]["argv"] == action["params"]["command"]
    assert action["task"]["result_path"] == ls.PROFILE_RESULT
    assert result["status"] == "published"
    receipt = fleet.cas.lookup(action)
    assert receipt is not None
    producer = cast(dict[str, Any], receipt)["producer"]
    for fact in ("python", "argv0.sha256", "argv0.bytes"):
        assert producer["toolchain"]["verified"][fact] == action["environment"]["toolchain"][fact]
    assert producer["executable"]["path"] == sys.executable
    body = json.loads(fleet.cas.result_path(receipt, action).read_bytes())
    assert body["completed"] is True and body["errors"] == []
    assert body["write_bytes"] == body["read_bytes"] == 1024
    assert body["start_identity"] == body["end_identity"]
    assert body["measured_unix"] == body["started_unix"] <= body["ended_unix"]
    assert not list(Path(fleet.scratch).iterdir())
    digest = pb.canonical_sha256(body)
    assert digest != receipt["result"]["sha256"]   # BODY versus BODY+LF CAS result
    reader = _profile_reader(fleet, action, digest)
    detail = worker_loop.scratch_observed_detail(None, reader)
    if changed_source:
        assert detail[PROFILES] == [] and not reader.verified
    else:
        assert detail[PROFILES] == [{**body, "artifact_sha256": digest}]
        assert detail[DEVICES][fleet.scratch] == fleet.actual_root_identity(fleet.scratch)
        assert len(reader.verified) == 1
        assert worker_loop.scratch_observed_detail(None, reader) == detail
        assert len(reader.verified) == 1
        assert not list((fleet.root / "verification").iterdir())
        # Independent runtime configuration, never copied from the artifact.
        reader.producer_python = "/unapproved/python"
        assert worker_loop.scratch_observed_detail(None, reader)[PROFILES] == []


@pytest.mark.parametrize("filesystem_type", ["ext4", "tmpfs", "nfs4", "unknown"])
def test_descriptor_mount_id_not_path_prefix_defines_allowed_filesystem(
        fleet, monkeypatch, filesystem_type):
    from types import SimpleNamespace
    real_read = Path.read_text

    def observed_kernel_text(path, *args, **kwargs):
        if str(path).startswith("/proc/self/fdinfo/"):
            return "mnt_id:\t77\n"
        if path == Path("/proc/self/mountinfo"):
            return (f"78 1 0:1 / {fleet.scratch} rw - tmpfs none rw\n"
                    f"77 1 0:2 / /unrelated rw - {filesystem_type} fixture rw\n")
        return real_read(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", observed_kernel_text)
    monkeypatch.setattr(ls.os, "fstatvfs", lambda fd: SimpleNamespace(f_fsid=42))
    if filesystem_type == "ext4":
        observed = fleet.actual_root_identity(fleet.scratch)
        assert observed["filesystem"] == "42" and observed["filesystem_type"] == "ext4"
    else:
        with pytest.raises(ls.LocalScratchError, match="filesystem type"):
            fleet.actual_root_identity(fleet.scratch)


# Independent-review regressions: tests-only pre-fix witnesses, not source fixes.
def _successful_profile_execution(fleet, monkeypatch, action):
    _standalone_inner_reader_context(monkeypatch)
    fleet.host = fleet.actual_host
    assert fleet.actual_root_identity is not None
    monkeypatch.setattr(ls, "root_identity", fleet.actual_root_identity)
    monkeypatch.setattr(pb, "_probe_nvidia_accelerators", lambda **kw: [])
    fleet.cas.publish_action_request(action)
    with materialize._execution_checkout(
            {"action_key": action["action_key"], "cas_root": str(fleet.cas.root),
             "checkout_snapshot": action["params"]["checkout_snapshot"]},
            local_checkout_root=fleet.root / "review-execution") as checkout:
        result = pb.run_local_action(action, cas_root=fleet.cas.root, checkout_root=checkout,
                                     timeout_seconds=30)
    assert result["status"] == "published", "negative control did not really execute"
    receipt = fleet.cas.lookup(action)
    assert receipt is not None, "negative control lacks a real successful CAS receipt"
    raw = fleet.cas.result_path(receipt, action).read_bytes()
    assert not list(Path(fleet.scratch).iterdir())
    return cast(dict[str, Any], receipt), raw


@pytest.mark.parametrize("field", ["command", "demand"])
def test_successfully_receipted_malformed_recorder_params_invalidate_root_not_worker(
        fleet, monkeypatch, field):
    base = _recorder_action(fleet)
    # First establish a real usable input for the SAME root; the broken
    # reference must invalidate that root, not expose the older good record.
    _, good_raw = _successful_profile_execution(fleet, monkeypatch, base)
    good_body = json.loads(good_raw)
    assert good_body["completed"] is True and good_body["write_bytes"] == 1024
    reader = _profile_reader(fleet, base, pb.canonical_sha256(good_body))
    good = worker_loop.scratch_observed_detail(None, reader)
    assert good[PROFILES] and fleet.scratch in good[DEVICES]
    config = json.loads(reader.config_path.read_bytes())
    good_ref = deepcopy(config["profiles"][0])

    body = deepcopy(base)
    body.pop("action_key")
    body["params"][field] = []
    broken = cast(dict[str, Any], pb.seal_action(body))
    assert broken["task"]["argv"] == base["task"]["argv"]  # actual recorder still executes
    assert broken["params"][field] == []
    _, broken_raw = _successful_profile_execution(fleet, monkeypatch, broken)
    observed = json.loads(broken_raw)
    assert observed["completed"] is True and observed["errors"] == []
    assert observed["write_bytes"] == observed["read_bytes"] == 1024
    assert observed["producer_action_key"] == broken["action_key"]
    broken_reader = _profile_reader(fleet, broken, pb.canonical_sha256(observed))
    config = json.loads(broken_reader.config_path.read_bytes())
    config["profiles"].insert(0, good_ref)
    broken_reader.config_path.write_text(json.dumps(config))
    # Pre-fix attributable RED: IndexError(command) / AttributeError(demand)
    # escapes this REAL loader call, after all successful-execution assertions.
    detail = worker_loop.scratch_observed_detail(None, broken_reader)
    assert detail[PROFILES] == []
    assert fleet.scratch not in detail[DEVICES]


@pytest.mark.parametrize("variant", ["legacy-stdout-result", "shell-wrapper", "malformed-result"])
def test_real_successful_cas_negative_producers_are_not_qualified(fleet, monkeypatch, variant):
    base = _recorder_action(fleet)
    body = deepcopy(base)
    body.pop("action_key")
    command = base["params"]["command"]
    if variant == "shell-wrapper":
        body["task"]["argv"] = ["/bin/bash", "--noprofile", "--norc", "-c", shlex.join(command)]
        # REAL executable facts for the actual wrapper, no claimed Python proof.
        body["environment"]["toolchain"] = pb.executable_toolchain_contract("/bin/bash")
    else:
        # Execute the copied real recorder through stdlib run_path, including
        # its actual 1-KiB write/read and dedicated result. Deliberate negative
        # request argv/results are NOT a qualified producer or fake receipt.
        driver = ("import runpy,sys; from pathlib import Path; "
                  f"m=runpy.run_path({ls.RECORDER!r}); "
                  f"assert m['main']({command[4:]!r}) == 0; ")
        if variant == "legacy-stdout-result":
            driver += f"sys.stdout.buffer.write(Path({ls.PROFILE_RESULT!r}).read_bytes())"
            legacy = "legacy-profile-stdout.json"
            emitter = [sys.executable, "-I", "-S", "-c", driver]
            body["task"]["argv"] = [
                "/bin/bash", "--noprofile", "--norc", "-c",
                f"{shlex.join(emitter)} | /usr/bin/tee {legacy}; exit ${{PIPESTATUS[0]}}"]
            body["task"]["result_path"] = legacy
            body["environment"]["toolchain"] = pb.executable_toolchain_contract("/bin/bash")
        else:
            driver += f"Path({ls.PROFILE_RESULT!r}).write_bytes(b'not canonical JSON\\n')"
            body["task"]["argv"] = [sys.executable, "-I", "-S", "-c", driver]
    action = cast(dict[str, Any], pb.seal_action(body))
    receipt, raw = _successful_profile_execution(fleet, monkeypatch, action)
    assert receipt["action_key"] == action["action_key"]
    if variant == "malformed-result":
        assert raw == b"not canonical JSON\n"
        # Correct digest for the actual malformed bytes, not a made-up result.
        digest = receipt["result"]["sha256"]
    else:
        observed = json.loads(raw)
        assert observed["completed"] is True and observed["write_bytes"] == 1024
        assert observed["producer_action_key"] == action["action_key"]
        assert receipt["producer"]["executable"]["path"] == "/bin/bash"
        digest = pb.canonical_sha256(observed)
    detail = worker_loop.scratch_observed_detail(None, _profile_reader(fleet, action, digest))
    assert detail[PROFILES] == [] and fleet.scratch not in detail[DEVICES]


def _normal_cpu_publication(fleet, *, cpu=1, memory=1, priority=0, retry=False, label="holder"):
    args = pbrun.parse_args([
        "--cwd", str(fleet.work), "--transport", "pool", "--no-default-env", "--tag", "gb10",
        "--cpus", str(cpu), "--demand", f"mem_gb={memory}", "--priority", str(priority),
        "--timeout-s", "30", *(["--max-attempts", "2", "--retry-safe"] if retry else []),
        "--", "/bin/bash", "-c", f"true # {label}"])
    action = pbrun.seal_action_from_template(pbrun.prepare_submission(args)["template"])
    fleet.cas.publish_action_request(action)
    fleet.queue.publish(**pbrun.publication_row(action, args=args, queue=fleet.queue))
    return str(action["action_key"])


@pytest.mark.parametrize("opted_in", [False, True], ids=["old-normal-path", "measured-path"])
def test_local_fallback_without_preferred_or_alternative_reaches_real_admission(
        tmp_path, monkeypatch, opted_in):
    fleet = ScratchFleet(tmp_path, monkeypatch, cpu_tiers={"preferred": [], "fallback": [0, 1, 2, 3]})
    (fleet.queue.root / pool.WORKERS / f"{FAST}.json").unlink()
    fleet.publish(opted_in=opted_in)
    assert fleet.queue.ledger(SLOW).free_preferred(fleet.tiers) == 0
    assert fleet.queue.ledger(SLOW).available() == CAPACITY
    claimed = fleet.claim(SLOW)
    assert claimed is not None, "scratch local prefit blocked sufficient real fallback admission"
    assert claimed["cpu_allocation"] == {"preferred": [], "fallback": [0]}
    assert fleet.queue.ledger(SLOW).held()["spool_gb"] == 186
    fleet.queue.finish(fleet.key, status="executed", detail={})
    assert fleet.queue.ledger(SLOW).available() == CAPACITY


@pytest.mark.parametrize("opted_in", [False, True], ids=["old-normal-borrow", "measured-borrow"])
def test_local_preferred_borrow_still_uses_actual_holder_telemetry(
        tmp_path, monkeypatch, opted_in):
    fleet = ScratchFleet(tmp_path, monkeypatch, cpu_tiers={"preferred": [0], "fallback": [1, 2, 3]})
    (fleet.queue.root / pool.WORKERS / f"{FAST}.json").unlink()
    holder = _normal_cpu_publication(fleet)
    first = fleet.claim(SLOW)
    assert first is not None and first["action_key"] == holder
    assert first["cpu_allocation"] == {"preferred": [0], "fallback": []}
    calibration = _normal_cpu_publication(fleet, cpu=4, label="telemetry-observer")
    fleet.clock[0] += 1.
    telemetry_path = adaptive_cpu.local_telemetry_path(fleet.queue.ledger(SLOW).base, holder)
    telemetry = {"action_key": holder, "complete": True, "sampled_unix": fleet.clock[0],
                 "cpu_seconds": .1, "wall_seconds": 1., "memory_peak_bytes": 0}
    adaptive_cpu.write_json(telemetry_path, telemetry)  # low-level cumulative observation
    assert fleet.claim(SLOW) is None                # real projected-cost refusal records first point
    assert fleet.denial_for(calibration, SLOW)["reason"].startswith("adaptive_cpu_refused")
    fleet.queue.withdraw(calibration, reason="owned telemetry calibration control concluded")
    fleet.clock[0] += 1.
    adaptive_cpu.write_json(telemetry_path, {**telemetry, "sampled_unix": fleet.clock[0],
                                           "cpu_seconds": .2, "wall_seconds": 2.})
    fleet.announce(SLOW)
    fleet.publish(opted_in=opted_in)
    assert fleet.queue.ledger(SLOW).free_preferred(fleet.tiers) == 0
    borrowed = fleet.claim(SLOW)
    assert borrowed is not None, "scratch local prefit blocked normal preferred borrowing"
    assert borrowed["cpu_allocation"] == {"preferred": [0], "fallback": []}
    meta = adaptive_cpu.read_json(fleet.queue.ledger(SLOW).held_dir / fleet.key / adaptive_cpu.METADATA)
    assert meta["borrowing"] is True and meta["borrowed_cpu"] == 1
    assert fleet.queue.ledger(SLOW).capacity() == CAPACITY
    fleet.queue.finish(fleet.key, status="executed", detail={})
    fleet.queue.finish(holder, status="executed", detail={})
    assert fleet.queue.ledger(SLOW).available() == CAPACITY


@pytest.mark.parametrize("opted_in", [False, True], ids=["old-priority-gate", "measured-priority-gate"])
def test_local_token_shortage_still_reaches_real_background_preemption(fleet, opted_in):
    (fleet.queue.root / pool.WORKERS / f"{FAST}.json").unlink()
    holder = _normal_cpu_publication(fleet, memory=29, priority=-10, retry=True)
    first = fleet.claim(SLOW)
    assert first is not None and first["action_key"] == holder
    assert first["priority"] == -10 and first["retry_safe"] is True
    fleet.clock[0] += 1.
    fleet.announce(SLOW)
    fleet.publish(opted_in=opted_in)
    assert fleet.queue.ledger(SLOW).available()["mem_gb"] == 3
    assert fleet.claim(SLOW) is None               # preemption never releases the holder's tokens
    decisions = [d for _, d in fleet.queue.withdrawal_decisions(holder)]
    assert decisions, "scratch local prefit bypassed the real priority/token-shortage gate"
    assert decisions[-1]["status"] == "withdrawn" and decisions[-1]["preempted_by"] == fleet.key
    assert fleet.queue.ledger(SLOW).held()["mem_gb"] == 29
    fleet.queue.finish(holder, status="withdrawn", detail={}, claim_snapshot=first)
    assert fleet.queue.ledger(SLOW).available() == CAPACITY
    taken = fleet.claim(SLOW)
    assert taken is not None and taken["action_key"] == fleet.key
    fleet.queue.finish(fleet.key, status="executed", detail={})


def test_generation_replacement_at_supported_gate_unwinds_earlier_refusal_verdict(fleet):
    fleet.publish()
    a = _record(fleet.queue.item_path(pool.READY, fleet.key))
    fleet.gpu_fault = "foreign"
    assert fleet.claim(FAST) is None
    fleet.mirror_real_denial(FAST, "adaptive_gpu_refused")
    assert fleet.denial(FAST)["published_unix"] == a["published_unix"]
    fleet.gpu_fault = None
    fleet.host = SLOW
    replacements = []

    def reentrant_publication_gate():
        # Supported same-thread nested transition lock, NOT a cross-process
        # lock bypass. Ordinary worker callbacks currently do not publish.
        assert not replacements
        ledger = fleet.queue.ledger(SLOW)
        assert ledger.available().get("spool_gb", 0) == 0  # zero kinds are omitted
        # Actual private pre-rename token reservation, not a vacuous zero.
        assert ledger.capacity()["spool_gb"] == 186
        assert ledger.held()["spool_gb"] == 186
        assert not fleet.queue.item_path(pool.CLAIMED, fleet.key).exists()
        fleet.clock[0] += 1.
        fleet.queue.publish(**fleet.publication)   # real publication B, same sealed action/demand
        b = _record(fleet.queue.item_path(pool.READY, fleet.key))
        assert b["published_unix"] > a["published_unix"]
        assert b["resources"] == a["resources"] and b["action_key"] == a["action_key"]
        replacements.append(b)
        return True

    claimed = fleet.queue.claim(owner="generation-race-control", capacity=CAPACITY,
                                cpu_tiers=fleet.tiers, adaptive_cpu=True, has_gpu=True,
                                tags=["gb10", CAPABILITY], admission_open=reentrant_publication_gate)
    assert replacements, "fixture did not reach the actual pre-rename gate"
    if claimed is not None:
        assert claimed["published_unix"] == replacements[0]["published_unix"]
        assert claimed[PLACEMENT]["published_unix"] == claimed["published_unix"]
    assert claimed is None, "publication B inherited A's evaluated peer-refusal placement"
    b = _record(fleet.queue.item_path(pool.READY, fleet.key))
    assert b["published_unix"] == replacements[0]["published_unix"]
    assert not fleet.queue.ledger(SLOW).held_keys()
    assert fleet.queue.ledger(SLOW).available() == CAPACITY
    assert not fleet.queue.lease_path(fleet.key).exists()
    assert not fleet.queue.item_path(pool.CLAIMED, fleet.key).exists()
    assert not fleet.queue.item_path(pool.DONE, fleet.key).exists()
    assert b.get("attempt_history", []) == [] and PLACEMENT not in b


def test_refusal_sample_total_age_cannot_authorize_measured_pass_on(fleet, monkeypatch):
    fleet.publish()
    actual_sample = fleet.gpu_sample

    def aging_gpu_observation():
        sample = actual_sample()
        if fleet.host == FAST:
            sample["sampled_unix"] -= 4.9
        return sample

    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda _: aging_gpu_observation())
    fleet.gpu_fault = "foreign"
    assert fleet.claim(FAST) is None
    fleet.mirror_real_denial(FAST, "adaptive_gpu_refused")
    denial = fleet.denial(FAST)
    sampled = denial["evidence"]["decision"]["sample"]["sampled_unix"]
    assert 0 < denial["denied_unix"] - sampled < adaptive_gpu.MAX_SAMPLE_AGE_S
    fleet.gpu_fault = None
    fleet.clock[0] += 4.9
    # Do NOT reannounce FAST: that would independently invalidate its denial.
    offer = _record(fleet.queue.root / pool.WORKERS / f"{FAST}.json")
    assert offer["announced_unix"] <= denial["denied_unix"]
    assert fleet.clock[0] - offer["observed_detail"]["observed_unix"] < pool.box_capacity.GPU_SAMPLE_MAX_AGE_S
    assert fleet.queue.ledger(FAST).available() == CAPACITY
    assert 0 < fleet.clock[0] - denial["denied_unix"] < adaptive_gpu.MAX_SAMPLE_AGE_S
    assert fleet.clock[0] - sampled > adaptive_gpu.MAX_SAMPLE_AGE_S
    claimed = fleet.claim(SLOW)
    assert claimed is None, "individually fresh ages improperly admitted a currently stale refusal sample"
    assert not fleet.queue.ledger(SLOW).held_keys()

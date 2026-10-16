"""An owner blocked on its own produced-output export is not stuck (#1035).

Since PrismaQuant #1118 a Stage A owner waits on its own exports at ordering
barriers and when its local window is full.  The wait has no clock of its
own and commits nothing, so before #1035 the owner's ``no_progress`` rung
ended it whenever the export queue was slower than one phase grace: its
allowance depended on another action's admission.  ``declare_staged_wait``
could not help, because an export is not a mover in any residency plan.

Now the owner writes an export-wait record beside its progress report
(``progress.declare_export_wait``), and the rung asks
``PoolQueue.export_wait_verdict``, under the staged wait's evidence rules
(#1016): exempt only while a named export of the owner's own shows progress;
a withheld, refused, failed, stalled or foreign export is not exempt, and
the verdict names the export and what it went on.

A test-local process adapter writes the owner's records under the actual
launch token and lands export bytes in finite steps on an injected clock.
The real execution loop, watch and export verdict decide each outcome.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import sys
import subprocess
import time
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import contracts, core as pb, pool, produced_spool, progress  # noqa: E402
from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402

ENTRY_BYTES = 1 << 20
PHASE_ALLOWANCE = 0.4
FIXED_UNIX = 1000.0
CLOCK_ORIGINS = [(FIXED_UNIX, 0.0), (10_000.0, 7000.0),
                 (1_700_000_000.0, 1_234_567.0)]


class _Clock:
    def __init__(self, *, unix_origin: float = FIXED_UNIX,
                 monotonic_origin: float = 0.0) -> None:
        self.unix_origin = unix_origin
        self.monotonic_origin = monotonic_origin
        self.elapsed = 0.0

    def advance(self, seconds: float) -> None:
        assert seconds >= 0
        self.elapsed += seconds

    def monotonic(self) -> float:
        return self.monotonic_origin + self.elapsed

    def time(self) -> float:
        return self.unix_origin + self.elapsed

    def perf_counter(self) -> float:
        return self.monotonic()

    def sleep(self, seconds: float) -> None:
        raise AssertionError("the owner path must not sleep")


def _real_process_launch(*args, **kwargs):
    raise AssertionError("the owner must use the scripted process adapter")


@pytest.fixture(autouse=True)
def clock(monkeypatch, request):
    unix_origin, monotonic_origin = getattr(request, "param", (FIXED_UNIX, 0.0))
    clock = _Clock(unix_origin=unix_origin, monotonic_origin=monotonic_origin)
    monkeypatch.setattr(pool, "time", clock)
    monkeypatch.setattr(pool, "_now", clock.time)
    monkeypatch.setattr(progress, "time", clock)
    monkeypatch.setattr(pb, "time", clock)
    monkeypatch.setattr(pool.materialize, "time", clock)
    return clock


def _owner(tmp_path: Path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text(
        "raise AssertionError('the owner must use the scripted process adapter')\n")
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/export-wait", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"],
                 "working_directory": ".", "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {pb.PROGRESS_PARAM: {
            "schema": pb.PROGRESS_POLICY_SCHEMA_V1,
            "phases": [{"name": name, "grace_s": PHASE_ALLOWANCE}
                       for name in ("startup", "run", "publish")]}},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue = AdmittedQueueFixture(pool.PoolQueue(tmp_path / "queue"),
                                 capacity={"cpu": 1, "mem_gb": 4},
                                 default_demand={"cpu": 1, "mem_gb": 1})
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
                  checkout_root=checkout,
                  worker_script=Path(__file__).resolve().parents[1]
                  / "tools" / "prismabuild_worker.py")
    item = queue.claim(tags=[contracts.PROGRESS_TAG, contracts.PROGRESS_HELPER_TAG])
    assert item is not None, "the owner was not admitted"
    return queue, item


@dataclass(frozen=True)
class _Step:
    seconds: float
    landed_bytes: int | None = None
    completes: bool = False


class _OwnerProcess:
    """Supply finite process observations and records to the real execution loop."""

    pid = 99999999

    def __init__(self, queue, item, export, destination, clock, monkeypatch,
                 *, steps: list[_Step], clock_reads: int = 0) -> None:
        self.path = queue.records.action_progress_path(str(item["action_key"]))
        self.export = export
        self.destination = Path(str(destination) + ".tmp")
        self.clock = clock
        self.steps = tuple(steps)
        self.next_step = 0
        self.clock_reads = clock_reads
        self.returncode = None
        self.token = None
        self.argv = None
        self.terminated = False
        monkeypatch.setattr(time, "sleep", clock.sleep)
        monkeypatch.setattr(subprocess, "Popen", _real_process_launch)
        monkeypatch.setattr(pool, "subprocess", SimpleNamespace(
            **{**vars(subprocess), "Popen": self.launch}))
        monkeypatch.setattr(pb, "_terminate_process_group", self.terminate)
        started_unix = clock.time()
        monkeypatch.setattr(queue, "_box_window", lambda *args: {
            "source": "test-local", "start_unix": started_unix,
            "end_unix": clock.time()})

    def launch(self, argv, *, env, **kwargs):
        assert self.argv is None, "the owner launched more than once"
        assert env[pb.ACTION_PROGRESS_PATH_ENV] == str(self.path)
        self.argv = argv
        self.token = env[pb.ACTION_PROGRESS_TOKEN_ENV]
        assert self.token
        self.path.parent.mkdir(parents=True, exist_ok=True)
        Path(progress.export_wait_path(str(self.path))).write_text(json.dumps({
            "schema": progress.EXPORT_WAIT_SCHEMA_V1, "token": self.token,
            "since_unix": self.clock.time(), "exports": [self.export]}))
        return self

    def poll(self):
        return self.returncode

    def communicate(self, *, timeout):
        assert self.argv is not None, "the owner was never launched"
        if self.terminated:
            return "", ""
        assert self.returncode is None, "the owner already completed its observations"
        before = (self.clock.monotonic(), self.clock.time(), self.clock.perf_counter())
        for _ in range(self.clock_reads):
            assert (self.clock.monotonic(), self.clock.time(),
                    self.clock.perf_counter()) == before, "clock reads advanced time"
        assert self.next_step < len(self.steps), (
            "the owner exhausted its process observations")
        step = self.steps[self.next_step]
        self.next_step += 1
        self.clock.advance(step.seconds)
        if step.landed_bytes is not None:
            self.destination.write_bytes(b"x" * step.landed_bytes)
        if step.completes:
            assert self.next_step == len(self.steps), (
                "the owner completed before consuming its process observations")
            Path(progress.export_wait_path(str(self.path))).unlink()
            self.path.write_text(json.dumps({
                "schema": pb.PROGRESS_RECORD_SCHEMA_V1, "token": self.token,
                "phase": "run", "units_completed": 1,
                "reported_unix": self.clock.time()}))
            self.returncode = 0
            return "", ""
        raise subprocess.TimeoutExpired(self.argv, timeout)

    def terminate(self, process, *, grace_s):
        assert process is self
        assert self.returncode is None, "the owner was already stopped"
        self.terminated = True
        self.returncode = -15

    def assert_finished(self):
        assert self.argv is not None
        assert self.next_step == len(self.steps), (
            "the owner ended before consuming its observations")
        assert self.returncode is not None, "the owner is still running"


def _seal_export(tmp_path: Path, item, *, seed: str, owner: str | None = None
                 ) -> tuple[str, Path]:
    """One sealed spool export of ``item`` (or of ``owner``) in its CAS.

    Its manifest names one destination; the export's key and that
    destination are returned.
    """

    destination = tmp_path / "canonical" / f"{seed}.bin"
    destination.parent.mkdir(parents=True, exist_ok=True)
    manifest_file = tmp_path / f"manifest-{seed}.json"
    manifest_file.write_text(json.dumps({
        "schema": produced_spool.SCHEMA, "owner": str(item["action_key"]),
        "batch_id": seed, "group": str(tmp_path / "group" / seed),
        "entries": [{"destination_path": str(destination),
                     "bytes": ENTRY_BYTES, "artifact_class": "payload"}]}))
    cas = pb.PrismaBuildCAS(item["cas_root"])
    manifest_input, _ = cas.ingest_input(manifest_file,
                                         input_id="produced-spool-manifest")
    checkout = tmp_path / f"export-{seed}"
    checkout.mkdir()
    (checkout / "export.py").write_text("")
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/export", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "export.py", seed],
                 "working_directory": ".", "result_path": "result"},
        "inputs": [manifest_input],
        "code_closure": pb.build_code_closure(checkout, ["export.py"]),
        "params": {"produced_spool": {
            "manifest_sha256": manifest_input["sha256"],
            "owner": owner or str(item["action_key"]), "batch_id": seed}},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas.publish_action_request(action)
    return str(action["action_key"]), destination


def _publish(queue: pool.PoolQueue, item, export: str) -> None:
    queue.publish(action_key=export, cas_root=item["cas_root"],
                  checkout_root=item["checkout_root"],
                  worker_script=item["worker_script"],
                  dependent_of=str(item["action_key"]))


def _claim(queue: pool.PoolQueue, item, export: str, *,
           claimed_unix: float = 1.0) -> None:
    """Hand ``export`` to ``claimed/``, as a claim on another box would."""

    _publish(queue, item, export)
    source = queue.records.item_path(pool.record_store.READY, export)
    record = json.loads(source.read_text())
    source.unlink()
    record.update({"claimed_unix": claimed_unix, "claimed_by": "export-fixture",
                   "claimed_host": "sparky"})
    queue.records.item_path(pool.record_store.CLAIMED, export).write_text(json.dumps(record))


def _execute(queue, item):
    return queue.execute(item, timeout_s=None, heartbeat_s=0.05,
                         timeout_grace_s=0.2)


def _live_export_outcome(
        tmp_path: Path, monkeypatch, clock, *, clock_reads: int = 0):
    queue, item = _owner(tmp_path)
    export, destination = _seal_export(tmp_path, item, seed="live")
    _claim(queue, item, export)
    owner = _OwnerProcess(
        queue, item, export, destination, clock, monkeypatch,
        clock_reads=clock_reads,
        steps=[_Step(0.5, 1024), _Step(0.5, 2048), _Step(0.5, 3072),
               _Step(0.0, completes=True)])

    outcome = _execute(queue, item)
    owner.assert_finished()

    assert outcome["status"] == "executed", repr(outcome.get("termination_reason"))
    assert outcome["returncode"] == 0
    assert owner.terminated is False
    assert "termination_reason" not in outcome
    observed = outcome["progress_observation"]
    assert observed["accepted_count"] == 1
    assert observed["last_accepted"]["units_completed"] == 1
    assert observed["export_wait_exempt_s"] > PHASE_ALLOWANCE
    assert observed["export_wait_exempt_s"] == pytest.approx(clock.elapsed)
    wait = observed["export_wait"]
    assert wait["exempt"] is True, wait
    (entry,) = wait["exports"]
    assert (entry["key"], entry["state"]) == (export, "claimed")
    assert entry["evidence"] == "progress-grew", entry
    assert entry["landed_bytes"] == 3072
    assert 0 < entry["landed_bytes"] <= ENTRY_BYTES
    assert entry["export_bytes"] == ENTRY_BYTES
    return outcome


def _stalled_export_outcome(
        tmp_path: Path, monkeypatch, clock, *, clock_reads: int = 0):
    queue, item = _owner(tmp_path)
    export, destination = _seal_export(tmp_path, item, seed="hung")
    _claim(queue, item, export)
    owner = _OwnerProcess(
        queue, item, export, destination, clock, monkeypatch,
        steps=[_Step(0.5), _Step(2.0)], clock_reads=clock_reads)

    outcome = _execute(queue, item)
    owner.assert_finished()

    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    assert owner.terminated is True
    observed = outcome["progress_observation"]
    assert observed["accepted_count"] == 0
    assert observed["export_wait_exempt_s"] == pytest.approx(0.5)
    wait = observed["export_wait"]
    assert wait["exempt"] is False
    assert wait["reason"] == "no named export shows progress"
    assert wait["checked_unix"] - wait["exports"][0]["evidence_unix"] > (
        wait["evidence_window_s"])
    (entry,) = wait["exports"]
    assert (entry["key"], entry["state"], entry["evidence"]) == (
        export, "claimed", "none")
    assert entry["landed_bytes"] == 0
    assert outcome["stall"]["quiet_s"] > outcome["progress_no_progress_bound_s"]
    assert outcome["stall"]["credited_s"]["export_wait"] == pytest.approx(
        observed["export_wait_exempt_s"])
    return outcome


def test_an_owner_waiting_on_its_live_export_is_not_killed_no_progress(
        tmp_path: Path, monkeypatch, clock) -> None:
    _live_export_outcome(tmp_path, monkeypatch, clock)


def test_an_owner_waiting_on_an_export_that_writes_nothing_still_ends(
        tmp_path: Path, monkeypatch, clock) -> None:
    _stalled_export_outcome(tmp_path, monkeypatch, clock)


@pytest.mark.parametrize("clock", CLOCK_ORIGINS, indirect=True)
@pytest.mark.parametrize("clock_reads", [0, 17])
def test_live_export_outcome_uses_only_scripted_time(
        tmp_path: Path, monkeypatch, clock, clock_reads: int) -> None:
    outcome = _live_export_outcome(tmp_path, monkeypatch, clock,
                                   clock_reads=clock_reads)
    assert outcome["elapsed_s"] == clock.elapsed == 1.5
    observed = outcome["progress_observation"]
    assert observed["last_accepted"]["reported_unix"] == clock.time()
    wait = observed["export_wait"]
    assert wait["since_unix"] == clock.unix_origin
    assert wait["checked_unix"] == clock.time()
    assert wait["exports"][0]["evidence_unix"] == clock.time()


@pytest.mark.parametrize("clock", CLOCK_ORIGINS, indirect=True)
@pytest.mark.parametrize("clock_reads", [0, 17])
def test_stalled_export_outcome_uses_only_scripted_time(
        tmp_path: Path, monkeypatch, clock, clock_reads: int) -> None:
    outcome = _stalled_export_outcome(tmp_path, monkeypatch, clock,
                                      clock_reads=clock_reads)
    assert outcome["elapsed_s"] == clock.elapsed == 2.5
    wait = outcome["progress_observation"]["export_wait"]
    assert wait["since_unix"] == clock.unix_origin
    assert wait["checked_unix"] == clock.time()
    assert wait["exports"][0]["evidence_unix"] == clock.unix_origin + 0.5


# -- the verdict, read directly ----------------------------------------------

def _declare(queue: pool.PoolQueue, item, exports: list[str], *,
             token: str = "t" * 32, since_unix: float = FIXED_UNIX) -> Path:
    path = queue.records.action_progress_path(str(item["action_key"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    Path(progress.export_wait_path(str(path))).write_text(json.dumps({
        "schema": progress.EXPORT_WAIT_SCHEMA_V1, "token": token,
        "since_unix": since_unix,
        "exports": exports}))
    return path


def _verdict(queue, item, path, **kwargs):
    kwargs.setdefault("window_s", 600.0)
    kwargs.setdefault("now", FIXED_UNIX)
    return queue.export_wait_verdict(
        str(item["action_key"]), path, token="t" * 32,
        cas_root=item["cas_root"], **kwargs)


def test_no_record_is_no_verdict(tmp_path: Path) -> None:
    queue, item = _owner(tmp_path)
    path = queue.records.action_progress_path(str(item["action_key"]))
    assert _verdict(queue, item, path) is None


def test_a_claimed_export_is_waited_on_while_its_bytes_grow(tmp_path: Path) -> None:
    queue, item = _owner(tmp_path)
    export, destination = _seal_export(tmp_path, item, seed="grow")
    _claim(queue, item, export)
    path = _declare(queue, item, [export], since_unix=100.0)
    temporary = Path(str(destination) + ".tmp")
    temporary.write_bytes(b"x" * 10)

    first = _verdict(queue, item, path, now=1000.0)
    assert first["exempt"] is True
    assert first["exports"][0]["evidence"] == "baseline"
    assert first["exports"][0]["landed_bytes"] == 10

    temporary.write_bytes(b"x" * 20)
    grew = _verdict(queue, item, path, now=2000.0, prior=first)
    assert grew["exempt"] is True
    assert grew["exports"][0]["evidence"] == "progress-grew"

    # Still within the window of the growth, then past it.
    carried = _verdict(queue, item, path, now=2300.0, prior=grew)
    assert carried["exports"][0]["evidence"] == "carried"
    assert carried["exempt"] is True
    stalled = _verdict(queue, item, path, now=2700.0, prior=carried)
    assert stalled["exempt"] is False
    assert stalled["exports"][0]["evidence"] == "none"

    # The landed destination counts, capped at the entry's bytes.
    temporary.unlink()
    destination.write_bytes(b"x" * (ENTRY_BYTES + 5))
    landed = _verdict(queue, item, path, now=2800.0, prior=stalled)
    assert landed["exports"][0]["landed_bytes"] == ENTRY_BYTES
    assert landed["exports"][0]["evidence"] == "progress-grew"


def test_a_fresh_claim_is_evidence_and_a_new_claim_takes_a_new_baseline(
        tmp_path: Path) -> None:
    queue, item = _owner(tmp_path)
    export, _destination = _seal_export(tmp_path, item, seed="fresh")
    now = FIXED_UNIX
    _claim(queue, item, export, claimed_unix=now - 1.0)
    path = _declare(queue, item, [export], since_unix=now - 5.0)

    verdict = _verdict(queue, item, path, now=now)
    assert verdict["exempt"] is True
    assert verdict["exports"][0]["evidence"] == "claimed"

    # A requeued and reclaimed export: the previous claim's reading does
    # not carry, the new claim is judged from its own baseline.
    stale = {**verdict, "exports": [{**verdict["exports"][0], "claimed_unix": 1.0,
                                     "landed_bytes": 0, "evidence": "none"}]}
    again = _verdict(queue, item, path, now=now + 1000.0, prior=stale)
    assert again["exports"][0]["evidence"] == "baseline"


@pytest.mark.parametrize("reason,evidence", [
    ("never_fits_capacity", "refused"),
    ("malformed_demand", "refused"),
    ("deferred_behind_withholding", "withheld"),
    ("reservation_unavailable_withholding", "withheld"),
])
def test_a_refused_or_withheld_export_is_not_waited_on(
        tmp_path: Path, reason: str, evidence: str) -> None:
    """#1035's ruling: the exemption holds only while the export shows
    progress, and a withheld or refused export shows none."""

    queue, item = _owner(tmp_path)
    export, _destination = _seal_export(tmp_path, item, seed="denied")
    _publish(queue, item, export)
    ready = json.loads(queue.records.item_path(pool.record_store.READY, export).read_text())
    queue._record_denial_transition(ready, host="sparky", reason=reason,
                                    decision_reason=None)
    path = _declare(queue, item, [export])

    verdict = _verdict(queue, item, path)

    assert verdict["exempt"] is False, verdict
    (entry,) = verdict["exports"]
    assert (entry["key"], entry["state"], entry["evidence"]) == (
        export, "ready", evidence)
    assert reason in (entry.get("refusal"), entry.get("withhold")), entry
    assert entry["denied_by"] == "sparky"


def test_an_export_the_spool_refused_is_not_waited_on(tmp_path: Path) -> None:
    """A filed spool identity refusal (#1098) for the export: its retry will
    meet the same group, so it is not coming."""

    queue, item = _owner(tmp_path)
    export, _destination = _seal_export(tmp_path, item, seed="spoolrefused")
    _publish(queue, item, export)
    filed = (queue.root / produced_spool.REFUSALS_SUBDIR / str(item["action_key"])
             / f"spoolrefused.{export[:16]}.export.local-source-changed.json")
    filed.parent.mkdir(parents=True)
    filed.write_text("{}")
    path = _declare(queue, item, [export])

    verdict = _verdict(queue, item, path)

    assert verdict["exempt"] is False
    entry = verdict["exports"][0]
    assert entry["evidence"] == "refused"
    assert entry["spool_refusal"] == filed.name


def test_a_queued_export_is_waited_on_for_one_window(tmp_path: Path) -> None:
    """Ready, and neither refused nor withheld: the first look is the
    baseline, and a whole window with the export still queued ends it."""

    queue, item = _owner(tmp_path)
    export, _destination = _seal_export(tmp_path, item, seed="queued")
    _publish(queue, item, export)
    path = _declare(queue, item, [export], since_unix=100.0)

    first = _verdict(queue, item, path, now=1000.0)
    assert (first["exempt"], first["exports"][0]["evidence"]) == (True, "baseline")
    carried = _verdict(queue, item, path, now=1500.0, prior=first)
    assert (carried["exempt"], carried["exports"][0]["evidence"]) == (True, "carried")
    ended = _verdict(queue, item, path, now=1700.0, prior=carried)
    assert (ended["exempt"], ended["exports"][0]["evidence"]) == (False, "none")
    # And it stays ended: a none is not a new baseline.
    later = _verdict(queue, item, path, now=1800.0, prior=ended)
    assert later["exempt"] is False


def test_a_failed_withdrawn_or_unpublished_export_is_not_waited_on(
        tmp_path: Path) -> None:
    queue, item = _owner(tmp_path)
    failed, _ = _seal_export(tmp_path, item, seed="failed")
    withdrawn, _ = _seal_export(tmp_path, item, seed="withdrawn")
    unpublished, _ = _seal_export(tmp_path, item, seed="unpublished")
    for key, state in ((failed, pool.record_store.FAILED), (withdrawn, pool.record_store.WITHDRAWN)):
        target = queue.records.item_path(state, key)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps({"action_key": key}))
    path = _declare(queue, item, [failed, withdrawn, unpublished])

    verdict = _verdict(queue, item, path)

    assert verdict["exempt"] is False
    assert [(entry["key"], entry["state"], entry["evidence"])
            for entry in verdict["exports"]] == [
        (failed, "failed", "none"), (withdrawn, "withdrawn", "none"),
        (unpublished, "unpublished", "none")]


def test_a_done_export_is_waited_on_only_for_the_report_latency(
        tmp_path: Path) -> None:
    queue, item = _owner(tmp_path)
    export, _destination = _seal_export(tmp_path, item, seed="done")
    target = queue.records.item_path(pool.record_store.DONE, export)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({"action_key": export, "finished_unix": 1000.0}))
    path = _declare(queue, item, [export])

    just = _verdict(queue, item, path, now=1001.0)
    assert (just["exempt"], just["exports"][0]["evidence"]) == (True, "landed")
    late = _verdict(queue, item, path, now=1000.0 + 3 * pool.HEARTBEAT_S)
    assert (late["exempt"], late["exports"][0]["evidence"]) == (False, "none")


def test_someone_elses_export_or_a_forged_record_is_not_waited_on(
        tmp_path: Path) -> None:
    """An export whose sealed request names another owner, however its row
    is labelled, and a record under a token this launch never minted."""

    queue, item = _owner(tmp_path)
    stranger, destination = _seal_export(tmp_path, item, seed="stranger",
                                         owner="e" * 64)
    _claim(queue, item, stranger)
    Path(str(destination) + ".tmp").write_bytes(b"x")
    path = _declare(queue, item, [stranger])

    verdict = _verdict(queue, item, path)
    assert verdict["exempt"] is False
    assert verdict["exports"][0]["state"] == "not-own-export"

    forged = _declare(queue, item, [stranger], token="0" * 32)
    refused = _verdict(queue, item, forged)
    assert refused == {"exempt": False, "reason": "foreign token", "exports": [],
                       "checked_unix": refused["checked_unix"]}


def test_the_helper_writes_the_record_the_worker_reads(tmp_path: Path,
                                                       monkeypatch) -> None:
    path = tmp_path / "x.progress"
    monkeypatch.setenv(progress.ACTION_PROGRESS_PATH_ENV, str(path))
    monkeypatch.setenv(progress.ACTION_PROGRESS_TOKEN_ENV, "t" * 32)
    assert progress.declare_export_wait(["a" * 64], since_unix=5.0) is True
    record_path = Path(progress.export_wait_path(str(path)))
    assert json.loads(record_path.read_text()) == {
        "schema": progress.EXPORT_WAIT_SCHEMA_V1, "token": "t" * 32,
        "since_unix": 5.0, "exports": ["a" * 64]}
    assert pool.read_export_wait(record_path, token="t" * 32)[0] == {
        "exports": ["a" * 64], "since_unix": 5.0}
    # The staged-wait reader does not take it, nor this reader a staged wait.
    assert pool.read_staged_wait(record_path, token="t" * 32) == (
        None, "wrong schema")
    with pytest.raises(ValueError):
        progress.declare_export_wait([])
    assert progress.clear_export_wait() is True
    assert not record_path.exists()
    monkeypatch.delenv(progress.ACTION_PROGRESS_TOKEN_ENV)
    assert progress.declare_export_wait(["a" * 64]) is False

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
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import threading
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import core as pb, pool, produced_spool, progress  # noqa: E402
from test_progress_keeps_a_working_action_alive import _claimed, _policy  # noqa: E402

#: Declares an export wait on the keys in ``exports.json`` beside the
#: checkout, stays quiet for ``seconds``, then commits one unit and exits.
#: Written without importing PrismaBuild, the way PrismaQuant's owner would
#: from a container.  The keys come from a file because the export names its
#: owner, so it is sealed after the owner's key is known.
WAITER = '''
import json, os, sys, time
path = os.environ["PRISMABUILD_ACTION_PROGRESS_PATH"]
token = os.environ["PRISMABUILD_ACTION_PROGRESS_TOKEN"]
seconds = float(sys.argv[2])
exports = json.loads(open(EXPORTS).read())
record = {"schema": "prismabuild.export_wait.v1", "token": token,
          "since_unix": time.time(), "exports": exports}
tmp = path + ".export-wait.tmp"
with open(tmp, "w") as handle:
    json.dump(record, handle)
os.replace(tmp, path + ".export-wait")
time.sleep(seconds)
os.unlink(path + ".export-wait")
report = {"schema": "prismabuild.action_progress.v1", "token": token,
          "phase": "run", "units_completed": 1, "reported_unix": time.time()}
with open(path + ".tmp", "w") as handle:
    json.dump(report, handle)
os.replace(path + ".tmp", path)
open("result", "w").write("ok")
'''

ENTRY_BYTES = 1 << 20


def _owner(tmp_path: Path, *, seconds: float):
    exports = tmp_path / "exports.json"
    source = WAITER.replace("EXPORTS", repr(str(exports)))
    queue, item = _claimed(tmp_path, mode="waiter", seconds=seconds,
                           policy=_policy(0.4, 0.4, 0.4), source=source)
    return queue, item, exports


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
    source = queue.item_path(pool.READY, export)
    record = json.loads(source.read_text())
    source.unlink()
    record.update({"claimed_unix": claimed_unix, "claimed_by": "export-fixture",
                   "claimed_host": "sparky"})
    queue.item_path(pool.CLAIMED, export).write_text(json.dumps(record))


def _execute(queue, item):
    return queue.execute(item, timeout_s=None, heartbeat_s=0.05,
                         timeout_grace_s=0.2)


class _Writer:
    """An export that writes its destination's temporary while it runs."""

    def __init__(self, destination: Path) -> None:
        self.path = Path(str(destination) + ".tmp")
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        with open(self.path, "wb") as handle:
            while not self.stop.wait(0.02):
                handle.write(b"x" * 1024)
                handle.flush()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop.set()
        self.thread.join()


# -- red first: the owner the rung killed ------------------------------------

def test_an_owner_waiting_on_its_live_export_is_not_killed_no_progress(
        tmp_path: Path) -> None:
    """Quiet for 1.5 s against a 0.4 s grace, all of it waiting on its own
    export, which is claimed and writing.  On main the rung kills the owner
    at 0.4 s, because nothing reads the export wait."""

    queue, item, exports = _owner(tmp_path, seconds=1.5)
    export, destination = _seal_export(tmp_path, item, seed="live")
    exports.write_text(json.dumps([export]))
    _claim(queue, item, export)

    with _Writer(destination):
        outcome = _execute(queue, item)

    assert outcome["status"] == "executed", repr(outcome.get("termination_reason"))
    observed = outcome["progress_observation"]
    assert observed["export_wait_exempt_s"] > 0.4
    wait = observed["export_wait"]
    assert wait["exempt"] is True, wait
    (entry,) = wait["exports"]
    assert (entry["key"], entry["state"]) == (export, "claimed")
    assert entry["evidence"] in ("progress-grew", "baseline"), entry
    assert 0 < entry["landed_bytes"] <= ENTRY_BYTES
    assert entry["export_bytes"] == ENTRY_BYTES


def test_an_owner_waiting_on_an_export_that_writes_nothing_still_ends(
        tmp_path: Path) -> None:
    """Claimed, but its landed bytes never grow: the first look is the
    baseline, the next one finds nothing, and the rung ends the owner with
    the export named on the record."""

    queue, item, exports = _owner(tmp_path, seconds=4.0)
    export, _destination = _seal_export(tmp_path, item, seed="hung")
    exports.write_text(json.dumps([export]))
    _claim(queue, item, export)

    outcome = _execute(queue, item)

    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    wait = outcome["progress_observation"]["export_wait"]
    assert wait["exempt"] is False
    assert wait["reason"] == "no named export shows progress"
    (entry,) = wait["exports"]
    assert (entry["key"], entry["state"], entry["evidence"]) == (
        export, "claimed", "none")
    assert outcome["stall"]["credited_s"]["export_wait"] == pytest.approx(
        outcome["progress_observation"]["export_wait_exempt_s"])


# -- the verdict, read directly ----------------------------------------------

def _declare(queue: pool.PoolQueue, item, exports: list[str], *,
             token: str = "t" * 32, since_unix: float | None = None) -> Path:
    path = queue.action_progress_path(str(item["action_key"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    Path(progress.export_wait_path(str(path))).write_text(json.dumps({
        "schema": progress.EXPORT_WAIT_SCHEMA_V1, "token": token,
        "since_unix": time.time() if since_unix is None else since_unix,
        "exports": exports}))
    return path


def _verdict(queue, item, path, **kwargs):
    kwargs.setdefault("window_s", 600.0)
    return queue.export_wait_verdict(
        str(item["action_key"]), path, token="t" * 32,
        cas_root=item["cas_root"], **kwargs)


def test_no_record_is_no_verdict(tmp_path: Path) -> None:
    queue, item, _exports = _owner(tmp_path, seconds=0)
    path = queue.action_progress_path(str(item["action_key"]))
    assert _verdict(queue, item, path) is None


def test_a_claimed_export_is_waited_on_while_its_bytes_grow(tmp_path: Path) -> None:
    queue, item, _exports = _owner(tmp_path, seconds=0)
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
    queue, item, _exports = _owner(tmp_path, seconds=0)
    export, _destination = _seal_export(tmp_path, item, seed="fresh")
    now = time.time()
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

    queue, item, _exports = _owner(tmp_path, seconds=0)
    export, _destination = _seal_export(tmp_path, item, seed="denied")
    _publish(queue, item, export)
    ready = json.loads(queue.item_path(pool.READY, export).read_text())
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

    queue, item, _exports = _owner(tmp_path, seconds=0)
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

    queue, item, _exports = _owner(tmp_path, seconds=0)
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
    queue, item, _exports = _owner(tmp_path, seconds=0)
    failed, _ = _seal_export(tmp_path, item, seed="failed")
    withdrawn, _ = _seal_export(tmp_path, item, seed="withdrawn")
    unpublished, _ = _seal_export(tmp_path, item, seed="unpublished")
    for key, state in ((failed, pool.FAILED), (withdrawn, pool.WITHDRAWN)):
        target = queue.item_path(state, key)
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
    queue, item, _exports = _owner(tmp_path, seconds=0)
    export, _destination = _seal_export(tmp_path, item, seed="done")
    target = queue.item_path(pool.DONE, export)
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

    queue, item, _exports = _owner(tmp_path, seconds=0)
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

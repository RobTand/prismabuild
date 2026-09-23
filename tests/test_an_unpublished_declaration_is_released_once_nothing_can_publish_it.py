"""An unpublished declaration can be released once nothing can publish it (#945).

``pbrun`` declares a consumer against each consumed batch it reads, then
publishes its row (#914). A submitter that dies in between leaves a
declaration with no queue record, ``unpublished``, which holds the batch.
#926's release refused it, because it may be a submission still in progress.

The release now accepts it when the state shows that no submission of that
key can reach the queue:

*   **No submitter is between its declaration and its row.** Every submitter
    holds the key's transition lock from its first declaration through its
    row (``pbrun.submission_window``); the release takes that lock without
    waiting and refuses while it is held.
*   **The live generation did not seal it.** A ``pbrun`` on the live
    generation clears the hold by submitting the same key again, so that is
    the remedy there, not a release.
*   **No pinned deferred release names it.** A release that stopped before
    its row resumes and publishes exactly its pinned key, sealed into the
    retained generation its template froze (#913).
*   **Released keys cannot declare again.** ``declare_origin_consumer``
    refuses a released key, so a later ``--as-sealed-by`` or identical
    resubmission dies at its declaration, before any row.

Fixture concessions: as in #914's and #926's tests, owners and consumers are
published, claimed and finished through the real ``PoolQueue``; submissions,
deferred releases and operator releases go through the real ``pbrun.main``
and ``deferred_release.release_tick``. A submitter's death between its
declaration and its row is a ``publish_consumer_row`` that raises, and a
publish is the fleet's ``repo`` link moving to another generation. The
operator who races a submitter is a separate Python process, so the lock it
meets is the submitter's ``fcntl`` lock, not this process's thread mutex.
"""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_prepaid_writer_integration as fx  # noqa: E402
from prismabuild import action_edges as ae  # noqa: E402
from prismabuild import pool, produced_output as po  # noqa: E402
import deferred_release as dr  # noqa: E402
import pbrun  # noqa: E402
from test_consumed_origin_retirement import (  # noqa: E402
    _charged, _commit, _publish_consumer, _queue, _run_consumer, _template,
)
import test_deferred_action_edges as edges  # noqa: E402
from test_superseded_origin_consumers import (  # noqa: E402
    _events, _manifest, _pbrun, _pbrun_env, _refused,
)

_isolated_synthetic_launch_context = fx._isolated_synthetic_launch_context

STALLED = po.ORIGIN_RETIREMENT_STALLED_EVENT
RETIRED = po.ORIGIN_RETIRED_EVENT


@pytest.fixture(autouse=True)
def _fresh_reports(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dr, "_REPORTED", {})


def _publish(tmp_path: Path, name: str) -> Path:
    """Make generation ``name`` the live one, as a publish does."""

    root = edges._retained_generation(tmp_path / "runtime-generations", name).parent
    link = tmp_path / "repo"
    if link.is_symlink():
        link.unlink()
    link.symlink_to(root)
    return root


def _consumer_argv(work: Path, manifest: Path, word: str) -> tuple[str, ...]:
    return ("--cwd", str(work), "--wait-s", "0.01", "--detach",
            "--data-manifest", str(manifest), "--", "/bin/bash", "-lc", word)


def _dies_after_declaring(tmp_path: Path, queue, monkeypatch, *argv: str) -> str:
    """Run a real submission whose submitter dies before its row: its key."""

    seen: list[str] = []

    def died(q, action, template, *, key, **_kwargs):
        seen.append(key)
        raise SystemExit("submitter died between its declaration and its row")

    with monkeypatch.context() as patch:
        patch.setattr(pbrun, "publish_consumer_row", died)
        assert "submitter died" in _refused(monkeypatch, *argv)
    [key] = seen
    assert po._key_generation(queue, key)[0] == "absent"
    return key


#: An operator's release in its own process: prints the answer or the refusal.
_OPERATOR = """
import json, sys
sys.path.insert(0, sys.argv[1])
from prismabuild import pool, produced_output as po
root, ref, key, live, cas = sys.argv[2:7]
try:
    answer = po.release_origin_consumer(
        pool.PoolQueue(root), json.loads(ref), consumer_action_key=key,
        by="op", live_runtime=live, cas_root=cas)
except po.ProducedOutputError as exc:
    answer = {"refusal": str(exc)}
print(json.dumps(answer))
"""


def _release_elsewhere(queue, committed: dict, key: str, *, live: Path,
                       cas: Path) -> dict:
    """Release from another process, as an operator's ``pbrun`` would."""

    src = Path(__file__).resolve().parents[1] / "src"
    done = subprocess.run(
        [sys.executable, "-c", _OPERATOR, str(src), str(queue.root),
         json.dumps(committed["ref"]), key, str(live), str(cas)],
        capture_output=True, text=True, timeout=120, check=False)
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])


def _release(monkeypatch, capsys, committed: dict, key: str) -> dict:
    return _pbrun(monkeypatch, capsys, "--release-origin-consumer",
                  json.dumps(committed["ref"]), key, "--reason", "died")


def _release_argv(committed: dict, key: str) -> tuple[str, ...]:
    return ("--release-origin-consumer", json.dumps(committed["ref"]), key,
            "--reason", "died")


def _unpublished_consumer(tmp_path: Path, monkeypatch, *, seed: str = "band-l"):
    """A committed batch, and a consumer whose submitter died after declaring."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, seed)
    queue.finish(instance["owner_action_key"], status="executed")
    work = _pbrun_env(tmp_path, queue, monkeypatch)
    (tmp_path / "repo").symlink_to(pbrun.RUNTIME_ROOT)
    manifest = _manifest(tmp_path, queue, committed)
    argv = _consumer_argv(work, manifest, "true")
    key = _dies_after_declaring(tmp_path, queue, monkeypatch, *argv)
    assert (po._consumers_dir(queue.root, instance, "b1")
            / f"{key}.json").exists(), "declared before it died"
    return queue, instance, path, committed, key, argv


# -- acceptance ----------------------------------------------------------------


def test_a_dead_submitters_declaration_is_released_once_its_generation_retires(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Refused on the live generation; released after a publish; the batch retires."""

    queue, instance, path, committed, key, _argv = _unpublished_consumer(
        tmp_path, monkeypatch)
    [stall] = _events(queue, STALLED)
    assert stall["consumers"] == [{"action_key": key, "state": "unpublished"}]

    # Sealed by the live generation: submitting it again is the remedy.
    assert "origin-consumer-unpublished-live" in _refused(
        monkeypatch, *_release_argv(committed, key))
    assert not po._released_consumers_dir(queue.root, instance, "b1").exists()
    assert path.exists()

    _publish(tmp_path, "g-next")
    answer = _release(monkeypatch, capsys, committed, key)
    assert (answer["released"], answer["state"]) == (True, "unpublished")
    record = json.loads((po._released_consumers_dir(queue.root, instance, "b1")
                         / f"{key}.json").read_text())
    assert record["state"] == "unpublished"
    assert record["sealed_wrapper"] == str(pbrun.RUNTIME_ROOT / "tools")
    assert _release(monkeypatch, capsys, committed, key)["released"] is False, (
        "a second release finds the first")

    [retired] = _events(queue, RETIRED)
    assert retired["consumers"] == [{"action_key": key, "state": "released"}]
    assert not path.exists() and _charged(queue, instance) == 0


def test_a_pinned_deferred_release_still_lands_so_its_key_is_refused(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """A release sealed into a retained generation resumes; nothing releases it first."""

    queue, work = edges._env(tmp_path, monkeypatch)
    template = edges._template(tmp_path / "canonical")
    producer = edges._producer_key(tmp_path, template, "pinned")
    edges._publish_producer(queue, template, producer)
    old = edges._retained_generation(tmp_path / "runtime-generations", "g-old")
    edge = f"{producer}:{template['template_id']}"
    current = pbrun.CONTAINER_WRAPPER_DIR
    monkeypatch.setattr(pbrun, "CONTAINER_WRAPPER_DIR", old)
    pending = edges._submit(work, monkeypatch, capsys, "--after", edge)["pending_id"]
    monkeypatch.setattr(pbrun, "CONTAINER_WRAPPER_DIR", current)
    instance = edges._start(queue, template, producer)
    _path, committed = edges._commit(queue, template, instance, "b1", b"handoff")
    queue.finish(producer, status="executed")

    def died(q, action, template, *, key, **_kwargs):
        raise SystemExit("the tier loop died between the declaration and the row")

    with monkeypatch.context() as patch:
        patch.setattr(pbrun, "publish_consumer_row", died)
        events = edges._without_summary(dr.release_tick(queue))
    assert [event["event"] for event in events] == [dr.REFUSED_EVENT]
    key = str(ae.read_release(queue.root, pending)["action_key"])
    assert po._key_generation(queue, key)[0] == "absent"
    assert ae.request_wrapper(tmp_path / "cas", key, where="test") == str(old)
    assert po._consumer_state(queue, key) == "unpublished"

    assert "origin-consumer-release-pending" in _refused(
        monkeypatch, *_release_argv(committed, key))
    assert not po._released_consumers_dir(queue.root, instance, "b1").exists()

    [resumed] = edges._released(dr.release_tick(queue))
    assert (resumed["action_key"], resumed["resumed"]) == (key, True)
    assert po._consumer_state(queue, key) == "live"
    assert "origin-consumer-live" in _refused(
        monkeypatch, *_release_argv(committed, key))


# -- the mechanisms ------------------------------------------------------------


def test_a_submitter_between_its_declaration_and_its_row_holds_off_the_release(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """The submitter holds the key's transition lock from declaration to row."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "window")
    queue.finish(instance["owner_action_key"], status="executed")
    work = _pbrun_env(tmp_path, queue, monkeypatch)
    live = _publish(tmp_path, "g-next")
    manifest = _manifest(tmp_path, queue, committed)
    real = pbrun.publish_consumer_row
    seen: dict[str, object] = {}

    def racing(q, action, template, *, key, **kwargs):
        # Declared, row not yet published: an operator releases it now.
        assert po._consumer_state(queue, key) == "unpublished"
        seen["key"] = key
        seen["operator"] = _release_elsewhere(
            queue, committed, key, live=live, cas=tmp_path / "cas")
        return real(q, action, template, key=key, **kwargs)

    monkeypatch.setattr(pbrun, "publish_consumer_row", racing)
    _pbrun(monkeypatch, capsys, *_consumer_argv(work, manifest, "true"))
    key = str(seen["key"])
    operator = seen["operator"]
    assert "refusal" in operator, (
        f"the release landed while the row was still to come: {operator}")
    assert "origin-consumer-submitting" in str(operator["refusal"])
    assert po._consumer_state(queue, key) == "live"
    assert not po._released_consumers_dir(queue.root, instance, "b1").exists()
    after = _release_elsewhere(queue, committed, key, live=live,
                               cas=tmp_path / "cas")
    assert "origin-consumer-live" in str(after.get("refusal")), after
    _run_consumer(queue, key, "executed", tags=("sparky", "gb10"))
    assert [event["event"] for event in po.origin_retirement_tick(queue)] == [RETIRED]


def test_a_released_key_cannot_declare_the_batch_again(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """The same key submitted again dies at its declaration, before any row."""

    queue, instance, path, committed, key, argv = _unpublished_consumer(
        tmp_path, monkeypatch, seed="again")
    other = fx._hexkey("still-reading")
    po.declare_origin_consumer(queue, committed["ref"], consumer_action_key=other)
    _publish_consumer(queue, other)
    _publish(tmp_path, "g-next")
    assert _release(monkeypatch, capsys, committed, key)["released"] is True

    refusal = _refused(monkeypatch, *argv)
    assert "origin-consumer-released" in refusal and key[:12] in refusal
    assert po._key_generation(queue, key)[0] == "absent", "no row was published"
    with pytest.raises(po.ProducedOutputError, match="origin-consumer-released"):
        po.declare_origin_consumer(queue, committed["ref"], consumer_action_key=key)

    assert po.origin_retirement_tick(queue) == [], "the other reader still holds it"
    assert path.exists()
    _run_consumer(queue, other, "executed")
    [retired] = _events(queue, RETIRED)
    assert {item["action_key"]: item["state"] for item in retired["consumers"]} == {
        key: "released", other: "succeeded"}
    assert not path.exists()


def test_an_unpublished_release_without_its_evidence_is_refused(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """No live generation or CAS to compare against: nothing is shown, so refuse."""

    queue, instance, _path, committed, key, _argv = _unpublished_consumer(
        tmp_path, monkeypatch, seed="blind")
    with pytest.raises(po.ProducedOutputError,
                       match="origin-consumer-unpublished-unknown"):
        po.release_origin_consumer(queue, committed["ref"],
                                   consumer_action_key=key, by="op")
    live = _publish(tmp_path, "g-next")
    # A CAS without its sealed request: which generation sealed it is unknown.
    with pytest.raises(po.ProducedOutputError,
                       match="origin-consumer-unpublished-unknown"):
        po.release_origin_consumer(queue, committed["ref"],
                                   consumer_action_key=key, by="op",
                                   live_runtime=live,
                                   cas_root=tmp_path / "another-cas")
    assert not po._released_consumers_dir(queue.root, instance, "b1").exists()

"""A consumed batch's failed consumer can be superseded or released (#926).

#914 retires a ``consumed`` origin batch once every consumer that declared it
has succeeded.  A consumer that failed and was resubmitted under another key
(every publish moves every key) left its first declaration behind, and the
batch was held for ever.  Two ways out:

*   **Supersession.**  ``pbrun --supersedes OLD`` (#913) files that a new
    submission replaces a failed or withdrawn key.  When that chain leads to a
    key declared against the same batch, the old declaration is resolved and
    the successor holds the batch in its place.
*   **Operator release.**  ``pbrun --release-origin-consumer BATCH_REF KEY``
    files a release for one failed or withdrawn declaration.  It refuses a
    consumer that is still queued or running.

A declaration that neither resolves stays a stall, reported once per change
with the consumer's terminal state and any supersession that did not apply.

Fixture concessions: as in #914's and #913's tests, owners and consumers are
published, claimed and finished through the real ``PoolQueue``; submissions,
supersessions and releases go through the real ``pbrun.main``.
"""
from __future__ import annotations

import json
from pathlib import Path
import shlex
import socket
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_prepaid_writer_integration as fx  # noqa: E402
from prismabuild import action_edges as ae  # noqa: E402
from prismabuild import pool, produced_output as po  # noqa: E402
import deferred_release as dr  # noqa: E402
import pbmcp  # noqa: E402
import pbrun  # noqa: E402
import pbstatus  # noqa: E402
from test_consumed_origin_retirement import (  # noqa: E402
    _charged, _commit, _entry, _publish_consumer, _queue, _run_consumer,
    _template,
)
from test_pbrun_detach import _checkout  # noqa: E402
import test_deferred_action_edges as edges  # noqa: E402

_isolated_synthetic_launch_context = fx._isolated_synthetic_launch_context

TAGS = ("sparky", "gb10")
STALLED = po.ORIGIN_RETIREMENT_STALLED_EVENT
RETIRED = po.ORIGIN_RETIRED_EVENT


@pytest.fixture(autouse=True)
def _fresh_reports(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dr, "_REPORTED", {})


def _pbrun_env(tmp_path: Path, queue, monkeypatch) -> Path:
    queue.announce(host="sparky", tags=list(TAGS), has_gpu=True,
                   capacity={"cpu": 4, "mem_gb": 16, "gpu": 1})
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    return _checkout(tmp_path)


def _pbrun(monkeypatch, capsys, *argv: str) -> dict:
    monkeypatch.setattr(sys, "argv", ["pbrun.py", *argv])
    capsys.readouterr()
    assert pbrun.main() == 0
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def _refused(monkeypatch, *argv: str) -> str:
    monkeypatch.setattr(sys, "argv", ["pbrun.py", *argv])
    with pytest.raises(SystemExit) as caught:
        pbrun.main()
    return str(caught.value)


def _submit_consumer(work: Path, manifest: Path, monkeypatch, capsys,
                     *options: str, word: str) -> str:
    return str(_pbrun(monkeypatch, capsys, "--cwd", str(work), "--wait-s",
                      "0.01", "--detach", "--data-manifest", str(manifest),
                      *options, "--", "/bin/bash", "-lc", word)["action_key"])


def _manifest(tmp_path: Path, queue, committed: dict) -> Path:
    path = tmp_path / "consumer-manifest.json"
    path.write_text(json.dumps(po.origin_batch_manifest(
        queue.root, [committed["ref"]])))
    return path


def _events(queue, name: str) -> list[dict]:
    return [event for event in po.origin_retirement_tick(queue)
            if event["event"] == name]


# -- the supersede path ----------------------------------------------------------


def test_a_superseding_resubmission_takes_over_the_failed_declaration(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """A fails; A' is submitted with --supersedes A; the batch retires after A'."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "band-l")
    queue.finish(instance["owner_action_key"], status="executed")
    work = _pbrun_env(tmp_path, queue, monkeypatch)
    manifest = _manifest(tmp_path, queue, committed)

    first = _submit_consumer(work, manifest, monkeypatch, capsys, word="true")
    _run_consumer(queue, first, "failed", tags=TAGS)
    [stall] = _events(queue, STALLED)
    assert stall["consumers"] == [{"action_key": first, "state": "failed"}]

    second = _submit_consumer(work, manifest, monkeypatch, capsys,
                              "--supersedes", first, word="true # retry")
    assert second != first
    assert ae.read_supersession(queue.root, first)["new"] == second
    assert (po._consumers_dir(queue.root, instance, "b1")
            / f"{second}.json").exists(), "the successor declared the batch"

    # Queued successor: the old declaration is resolved, the new one holds.
    assert po.origin_retirement_tick(queue) == []
    assert "retirement_report" not in _entry(queue, instance)
    assert path.exists()

    _run_consumer(queue, second, "executed", tags=TAGS)
    [retired] = _events(queue, RETIRED)
    assert sorted(retired["consumers"], key=lambda item: item["state"]) == sorted([
        {"action_key": first, "state": "superseded", "superseded_by": second},
        {"action_key": second, "state": "succeeded"}],
        key=lambda item: item["state"])
    assert not path.exists() and _charged(queue, instance) == 0
    assert queue.item_path(pool.FAILED, first).exists(), "history is kept"


def test_a_failed_declaration_nobody_supersedes_is_held_and_reported(
        tmp_path: Path) -> None:
    """Without the supersede path the batch is held, and says what holds it."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "blocked")
    queue.finish(instance["owner_action_key"], status="executed")
    consumer = fx._hexkey("blocked-reader")
    po.declare_origin_consumer(queue, committed["ref"], consumer_action_key=consumer)
    _publish_consumer(queue, consumer)
    queue.withdraw(consumer, reason="gave up", signal_child=False)
    assert queue.item_path(pool.WITHDRAWN, consumer).exists()

    blocked = {"event": STALLED, "ref": committed["ref"],
               "bytes": path.stat().st_size,
               "consumers": [{"action_key": consumer, "state": "withdrawn"}]}
    assert po.origin_retirement_tick(queue) == [blocked]
    assert po.origin_retirement_tick(queue) == [], "once per change"

    # A successor that never declared this batch changes nothing, and the
    # report names it.
    elsewhere = fx._hexkey("reads-another-batch")
    ae.file_supersession(queue, consumer, new=elsewhere, new_kind=ae.PRODUCER_KEY)
    assert po.origin_retirement_tick(queue) == [{
        **blocked, "consumers": [{"action_key": consumer, "state": "withdrawn",
                                  "superseded_by": elsewhere}]}]
    assert po.origin_retirement_tick(queue) == []
    assert path.exists() and _charged(queue, instance) == path.stat().st_size


def test_a_chain_of_supersessions_resolves_to_the_declared_successor(
        tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "chain")
    queue.finish(instance["owner_action_key"], status="executed")
    keys = [fx._hexkey(f"attempt-{n}") for n in range(3)]
    for index, key in enumerate(keys):
        po.declare_origin_consumer(queue, committed["ref"], consumer_action_key=key)
        _publish_consumer(queue, key)
        if index < 2:
            _run_consumer(queue, key, "failed")
            ae.file_supersession(queue, key, new=keys[index + 1],
                                 new_kind=ae.PRODUCER_KEY)

    assert po.origin_retirement_tick(queue) == [], "the last one is queued"
    _run_consumer(queue, keys[2], "executed")
    [retired] = _events(queue, RETIRED)
    assert retired["consumers"] == sorted([
        {"action_key": keys[0], "state": "superseded", "superseded_by": keys[2]},
        {"action_key": keys[1], "state": "superseded", "superseded_by": keys[2]},
        {"action_key": keys[2], "state": "succeeded"}],
        key=lambda item: str(item["action_key"]))
    assert not path.exists()


def test_a_deferred_resubmission_takes_over_once_it_is_released(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """The Stage B shape: a released consumer fails; its --after retry wins."""

    queue, work = edges._env(tmp_path, monkeypatch)
    template = edges._template(tmp_path / "canonical")
    producer = edges._producer_key(tmp_path, template, "band-l")
    edges._publish_producer(queue, template, producer)
    edge = f"{producer}:{template['template_id']}"
    edges._submit(work, monkeypatch, capsys, "--after", edge)
    instance = edges._start(queue, template, producer)
    path, _committed = edges._commit(queue, template, instance, "b1", b"handoff")
    queue.finish(producer, status="executed")
    [first] = edges._released(dr.release_tick(queue))
    _run_consumer(queue, first["action_key"], "failed", tags=TAGS)

    retry = edges._submit(work, monkeypatch, capsys, "--after", edge,
                          "--supersedes", first["action_key"],
                          command=("/bin/cat", edges.PLACEHOLDER, "retry"))
    assert ae.read_supersession(queue.root, first["action_key"])["new"] == \
        retry["pending_id"]
    # Unreleased: #913 holds the producer's batches quietly.
    assert po.origin_retirement_tick(queue) == []

    [second] = edges._released(dr.release_tick(queue))
    assert second["pending_id"] == retry["pending_id"]
    assert po.origin_retirement_tick(queue) == [], "the successor is queued"
    _run_consumer(queue, second["action_key"], "executed", tags=TAGS)
    [retired] = _events(queue, RETIRED)
    assert {item["action_key"]: item["state"] for item in retired["consumers"]} == {
        first["action_key"]: "superseded", second["action_key"]: "succeeded"}
    assert not path.exists()


# -- the operator release ------------------------------------------------------


def test_the_operator_release_refuses_a_live_consumer_and_releases_a_failed_one(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "operator")
    queue.finish(instance["owner_action_key"], status="executed")
    _pbrun_env(tmp_path, queue, monkeypatch)
    consumer = fx._hexkey("dead-reader")
    po.declare_origin_consumer(queue, committed["ref"], consumer_action_key=consumer)
    _publish_consumer(queue, consumer)
    ref = json.dumps(committed["ref"])
    release = ("--release-origin-consumer", ref, consumer, "--reason", "dead")

    claimed = queue.claim(owner="w-consumer")
    assert claimed["action_key"] == consumer
    assert "origin-consumer-live" in _refused(monkeypatch, *release)
    assert "origin-consumer-undeclared" in _refused(
        monkeypatch, "--release-origin-consumer", ref, fx._hexkey("stranger"))
    assert "takes no command" in _refused(monkeypatch, *release, "--", "true")
    assert not po._released_consumers_dir(queue.root, instance, "b1").exists()

    queue.finish(consumer, status="failed")
    [stall] = _events(queue, STALLED)
    assert stall["consumers"] == [{"action_key": consumer, "state": "failed"}]

    ref_file = tmp_path / "ref.json"
    ref_file.write_text(ref)
    answer = _pbrun(monkeypatch, capsys, "--release-origin-consumer",
                    str(ref_file), consumer, "--reason", "dead")
    assert (answer["released"], answer["state"]) == (True, "failed")
    record = json.loads((po._released_consumers_dir(queue.root, instance, "b1")
                         / f"{consumer}.json").read_text())
    assert record["reason"] == "dead" and record["by"].endswith("@sparky")
    assert record["state"] == "failed" and record["ref"] == committed["ref"]
    assert _pbrun(monkeypatch, capsys, *release)["released"] is False, (
        "a second release finds the first")
    assert (po._consumers_dir(queue.root, instance, "b1")
            / f"{consumer}.json").exists(), "the declaration itself stays"

    [retired] = _events(queue, RETIRED)
    assert retired["consumers"] == [{"action_key": consumer, "state": "released"}]
    assert not path.exists() and _charged(queue, instance) == 0
    assert "origin-batch-reclaimed" in _refused(monkeypatch, *release)


def test_a_release_leaves_the_other_consumers_holding_the_batch(
        tmp_path: Path) -> None:
    """The release resolves one declaration; the delete still waits for the rest."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "two")
    queue.finish(instance["owner_action_key"], status="executed")
    dead, live = fx._hexkey("dead"), fx._hexkey("live")
    for key in (dead, live):
        po.declare_origin_consumer(queue, committed["ref"], consumer_action_key=key)
    _publish_consumer(queue, dead)
    _run_consumer(queue, dead, "failed")
    _publish_consumer(queue, live)
    with pytest.raises(po.ProducedOutputError, match="origin-consumer-live"):
        po.release_origin_consumer(queue, committed["ref"],
                                   consumer_action_key=live, by="op")
    assert po.release_origin_consumer(queue, committed["ref"],
                                      consumer_action_key=dead,
                                      by="op")["released"] is True

    assert po.origin_retirement_tick(queue) == [], "live: held, quietly"
    assert path.exists()
    _run_consumer(queue, live, "executed")
    assert [event["event"] for event in po.origin_retirement_tick(queue)] == [RETIRED]
    assert not path.exists()


def test_a_retained_batch_has_nothing_to_release(tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(
        queue, template, "kept", lifetime=po.ORIGIN_LIFETIME_RETAIN)
    with pytest.raises(po.ProducedOutputError, match="origin-batch-retained"):
        po.release_origin_consumer(queue, committed["ref"],
                                   consumer_action_key=fx._hexkey("any"), by="op")
    assert path.exists()


# -- reporting blocked batches -------------------------------------------------


def test_the_blocked_listing_names_each_dead_holder_and_its_exact_remedy(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """``pbstatus --blocked-origins`` lists only the batches no live consumer
    will free, and its release command frees them as printed.

    Three batches: one held by a withdrawn consumer alone (blocked); one held
    by a failed consumer and a queued one (waiting, not blocked); one whose
    failed consumer was superseded by a queued declared successor (waiting).
    """

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    _pbrun_env(tmp_path, queue, monkeypatch)
    batches = {}
    for seed in ("blocked", "mixed", "handed"):
        instance, path, committed = _commit(queue, template, seed)
        queue.finish(instance["owner_action_key"], status="executed")
        batches[seed] = (instance, path, committed["ref"])

    def declare(seed: str, key: str) -> None:
        po.declare_origin_consumer(queue, batches[seed][2], consumer_action_key=key)

    gone, dead, live = fx._hexkey("gone"), fx._hexkey("dead"), fx._hexkey("live")
    first, second = fx._hexkey("first"), fx._hexkey("second")
    declare("blocked", gone)
    _publish_consumer(queue, gone)
    queue.withdraw(gone, reason="gave up", signal_child=False)
    for seed, key in (("mixed", dead), ("handed", first)):
        declare(seed, key)
        _publish_consumer(queue, key)
        _run_consumer(queue, key, "failed")
    declare("mixed", live)
    _publish_consumer(queue, live)
    declare("handed", second)
    _publish_consumer(queue, second)
    ae.file_supersession(queue, first, new=second, new_kind=ae.PRODUCER_KEY)

    def listing() -> tuple[int, dict]:
        capsys.readouterr()
        code = pbstatus.main(["--blocked-origins", "--queue-root", str(queue.root)])
        return code, json.loads(capsys.readouterr().out)

    code, before = listing()
    assert code == 0 and before["complete"] is True and before["unreadable"] == []
    assert before["schema"] == pbstatus.BLOCKED_ORIGINS_SCHEMA_V1
    [row] = before["blocked"]
    _instance, path, ref = batches["blocked"]
    assert (row["ref"], row["bytes"], row["holding"]) == (ref, path.stat().st_size, [gone])
    assert row["consumers"] == [{"action_key": gone, "state": "withdrawn"}]
    assert row["reported"] is False, "the tick has not run"
    [remedy] = row["remedies"]
    assert (remedy["action_key"], remedy["state"]) == (gone, "withdrawn")
    assert remedy["resubmit"].startswith(f"pbrun.py --priority -10 --supersedes {gone} ")

    stalls = {event["ref"]["owner_action_key"] for event in _events(queue, STALLED)}
    assert batches["blocked"][2]["owner_action_key"] in stalls
    assert batches["mixed"][2]["owner_action_key"] in stalls, (
        "the tick reports a failed holder beside a live one; the listing does not")
    assert listing()[1]["blocked"][0]["reported"] is True

    session = pbmcp.Session(queue_root=queue.root, cas_root=tmp_path / "cas",
                            repo_link=tmp_path / "repo")
    body = session.call("pb_blocked_origins")
    assert body["census_complete"] is True
    assert json.loads(json.dumps(body["blocked"])) == listing()[1]["blocked"]

    # The release command runs as printed, save the reason.
    argv = shlex.split(remedy["release"])
    assert argv[0] == "pbrun.py" and argv[-1] == "<why>"
    answer = _pbrun(monkeypatch, capsys, *argv[1:-1], "abandoned band")
    assert (answer["released"], answer["state"]) == (True, "withdrawn")
    assert listing()[1]["blocked"] == []
    [retired] = _events(queue, RETIRED)
    assert retired["ref"] == ref and not path.exists()

    # A declaration it cannot read is named, and the listing says it is partial.
    broken = po._consumers_dir(queue.root, batches["mixed"][0], "b1") / f"{live}.json"
    broken.write_text("{")
    code, partial = listing()
    assert code == pbstatus.EXIT_INCOMPLETE and partial["complete"] is False
    assert partial["blocked"] == [] and len(partial["unreadable"]) == 1

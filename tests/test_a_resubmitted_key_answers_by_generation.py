"""A resubmitted key answers by generation, wherever it is read (#1178).

``done/`` and ``failed/`` are one slot each per action key, and the key is a
content hash: resubmitting the same work publishes a new generation.  The GLM
Stage B layer 037 incident (and the pre-#1117 queues) left two generations'
terminal records standing, each with ``attempts: 1``: ``pbwait`` read
``done/`` first and said ``executed``, ``pbmcp`` and the tier plan preferred
``done/`` by directory order, and ``pbstatus --json`` listed both rows with
nothing to say which answered ``how did this key end?``.  A coordinator could
re-run a finished row, or hold a consumer, on the wrong answer.

The fix is one generation-resolution helper used by every reader, with the
double-record state reported rather than hidden: the later generation's
terminal record is the key's answer, the earlier one stays readable history,
and an unorderable or unreadable pair is never read as success.

Everything here runs on ``tmp_path`` queues.  The terminal pair is produced by
``publish`` / ``claim`` / ``finish``; #1117 retires the earlier generation at
conclusion, so :func:`_coexisting_pair` restores it from the archive the
queue itself wrote -- the exact bytes at the exact path the live queue still
holds them -- and then asks each reader the same question.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(REPOSITORY / "tests"))

from prismabuild import pool, progress, residency_plan  # noqa: E402
import pbstatus  # noqa: E402
import pbmcp  # noqa: E402
import pbrun  # noqa: E402

from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    STAGE_KIND, TIER, _hexkey, _row)

KEY = "b" * 64
CONSUMER_KEY = "c" * 64
MANIFEST = "d" * 64
TOKEN = "t" * 32

#: first status, second status, first directory, second directory,
#: the status word each record carries.
ORDERS = [
    ("failed", "executed", pool.FAILED, pool.DONE, "failed", "executed"),
    ("executed", "failed", pool.DONE, pool.FAILED, "executed", "failed"),
]


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    value = pool.PoolQueue(tmp_path / "queue")
    value.ensure_layout()
    return value


def _end(queue: pool.PoolQueue, status: str):
    queue.publish(action_key=KEY, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", max_attempts=1)
    claimed = queue.claim()
    assert claimed is not None and claimed["action_key"] == KEY
    path = queue.finish(KEY, status=status, detail={"returncode": 0})
    return path, claimed


def _coexisting_pair(queue: pool.PoolQueue, *, first: str, second: str):
    """Run fail/resubmit/succeed (either order) and restore the retired row.

    #1117 archives the earlier generation's record at the later conclusion
    and unlinks it.  The incident state -- and the state of records filed
    before that fix -- is both records present, so this copies the archived
    record back from the queue's own ``superseded_path``.
    """

    first_path, first_claim = _end(queue, first)
    second_path, second_claim = _end(queue, second)
    terminal = json.loads(second_path.read_text())
    archived = Path(terminal["supersedes_terminal"]["superseded_path"])
    assert archived.name.startswith(f"{KEY}.")
    assert json.loads(archived.read_text())["published_unix"] == (
        first_claim["published_unix"])
    first_path.write_bytes(archived.read_bytes())
    assert first_path.exists() and second_path.exists()
    return {
        "first_path": first_path, "first": first_claim,
        "second_path": second_path, "second": second_claim,
        "terminal": terminal,
    }


def _answer(queue: pool.PoolQueue) -> dict:
    resolver = getattr(queue, "current_ending", None)
    assert resolver is not None, (
        "PoolQueue has no generation-resolution helper; readers each order "
        "done/ and failed/ by directory")
    return resolver(KEY)


@pytest.mark.parametrize("first,second,first_state,second_state,first_word,second_word",
                         ORDERS)
def test_the_current_ending_is_the_later_generation(
        tmp_path: Path, first, second, first_state, second_state,
        first_word, second_word) -> None:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    pair = _coexisting_pair(queue, first=first, second=second)

    answer = _answer(queue)

    assert answer["state"] == second_state, (
        f"the key answered {answer['state']!r}, not the later {second_state!r}")
    assert answer["path"] == pair["second_path"]
    assert answer["generation"] == pytest.approx(
        float(pair["second"]["published_unix"]))
    assert answer["ambiguous"] is False
    assert sorted(answer["coexisting"]) == sorted([pool.DONE, pool.FAILED])
    assert [entry["state"] for entry in answer["history"]] == [first_state]
    assert [entry["generation"] for entry in answer["history"]] == [
        pytest.approx(float(pair["first"]["published_unix"]))]

    # No deletion of historical evidence: both records stay where they are,
    # and the earlier generation's ending is readable from its immutable
    # attempt archive.
    assert pair["first_path"].exists(), "the earlier record was deleted"
    assert pair["second_path"].exists()
    archived = queue.archived_generation_outcomes(
        KEY, generation=float(pair["first"]["published_unix"]))
    assert archived, "the earlier generation has no readable ending"
    assert archived[-1][1]["disposition"] == first_state


@pytest.mark.parametrize("first,second,first_state,second_state,first_word,second_word",
                         ORDERS)
def test_pbstatus_endings_name_the_current_generation(
        tmp_path: Path, first, second, first_state, second_state,
        first_word, second_word) -> None:
    """``pbstatus --json``'s ending rows resolve by generation and report the
    double-record state instead of leaving two rows with no answer."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    pair = _coexisting_pair(queue, first=first, second=second)

    rows = [row for row in pbstatus.read_endings(queue.root, limit=10)
            if row["action_key"] == KEY]
    assert len(rows) == 2, "the retained records must both stay in the table"
    current = [row for row in rows if row.get("current") is True]
    older = [row for row in rows if row.get("current") is False]
    assert len(current) == 1 and len(older) == 1, (
        "exactly one row answers how the key ended")
    assert current[0]["status"] == second_word
    assert current[0]["published_unix"] == pytest.approx(
        float(pair["second"]["published_unix"]))
    assert older[0]["status"] == first_word
    superseded = older[0]["superseded_by"]
    assert superseded["state"] == second_state
    assert superseded["path"] == str(pair["second_path"])
    assert superseded["generation"] == pytest.approx(
        float(pair["second"]["published_unix"]))
    assert all(row.get("terminal_conflict") is True for row in rows), (
        "the key carried two terminal generations and that is reported")


@pytest.mark.parametrize("first,second,first_state,second_state,first_word,second_word",
                         ORDERS)
def test_mcp_action_reports_the_current_generation(
        tmp_path: Path, first, second, first_state, second_state,
        first_word, second_word) -> None:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    pair = _coexisting_pair(queue, first=first, second=second)

    session = pbmcp.Session(queue_root=queue.root, cas_root=tmp_path / "cas",
                            repo_link=tmp_path / "repo")
    body = session.call("pb_action", {"key_prefix": KEY[:12]})

    assert body["state"] == second_state, (
        f"pb_action answered {body['state']!r}, not {second_state!r}")
    assert body["outcome"]["status"] == second_word
    superseded = body.get("superseded")
    assert isinstance(superseded, list) and superseded, (
        "the older terminal generation is not reported")
    assert any(entry["state"] == first_state for entry in superseded)
    reported = next(entry for entry in superseded
                    if entry["state"] == first_state)
    assert reported["published_unix"] == pytest.approx(
        float(pair["first"]["published_unix"]))


@pytest.mark.parametrize("first,second,first_state,second_state,first_word,second_word",
                         ORDERS)
def test_pbwait_still_binds_the_generation(
        tmp_path: Path, first, second, first_state, second_state,
        first_word, second_word) -> None:
    """``pbwait``'s reader already resolves exactly; it must stay agreeing."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    pair = _coexisting_pair(queue, first=first, second=second)

    landed, generation = pbrun.outcome_poll(
        queue, KEY, float(pair["second"]["published_unix"]))
    assert landed is not None and generation == float(
        pair["second"]["published_unix"])
    assert landed[1]["status"] == second_word

    landed, _generation = pbrun.outcome_poll(queue, KEY, None)
    assert landed is not None
    assert landed[1]["status"] == second_word, (
        "a key-only wait must answer with the newest generation")


def _plan_fixture(tmp_path: Path):
    """A consumer's frozen plan naming one mover, and a staged-wait record."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    mover = _hexkey("generation")
    size = 22 * 10 ** 9
    row = {**_row(queue, mover, {STAGE_KIND: 21, "cpu": 2, "mem_gb": 1}),
           "residency": {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                         "manifest_sha256": MANIFEST, "manifest_bytes": size,
                         "range_start_bytes": 0, "range_end_bytes": size}}
    plan = residency_plan.build_plan(
        consumer_action_key=CONSUMER_KEY, tier_id=TIER,
        stage_root=str(tmp_path / "stage"), manifest_sha256=MANIFEST,
        manifest_bytes=size, phases=[{
            "name": "chain-043", "start_bytes": 0, "end_bytes": size,
            "stage_gib": 21, "mover_row": row,
            "egress_row": _row(queue, _hexkey("generationegress"),
                               {"mem_gb": 1})}])
    residency_plan.freeze(queue, plan)
    queue.mint_tier_capacity(TIER, {STAGE_KIND: 21})
    progress_path = tmp_path / "consumer.progress"
    Path(progress.staged_wait_path(str(progress_path))).write_text(json.dumps({
        "schema": progress.STAGED_WAIT_SCHEMA_V1, "token": TOKEN,
        "since_unix": 1.0, "movers": [mover]}))
    return queue, mover, progress_path


@pytest.mark.parametrize("first,second,first_state,second_state,first_word,second_word",
                         ORDERS)
def test_the_tier_plan_resolves_the_current_generation(
        tmp_path: Path, first, second, first_state, second_state,
        first_word, second_word) -> None:
    """The tier plan's mover state must come from the key's current ending.

    A consumer still waiting on a mover whose newest generation failed is not
    waiting on a dependency; one whose newest generation is done/evicted is.
    """

    queue, mover, progress_path = _plan_fixture(tmp_path)
    now = time.time()
    first_generation, second_generation = now - 100.0, now - 50.0

    def _file(state: str, generation: float, word: str) -> None:
        path = queue.item_path(state, mover)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "action_key": mover, "status": word,
            "published_unix": generation, "attempts": 1,
            "finished_unix": generation + 1.0}))

    _file(first_state, first_generation, first_word)
    _file(second_state, second_generation, second_word)

    verdict = queue.staged_wait_verdict(CONSUMER_KEY, progress_path,
                                        token=TOKEN)

    assert verdict is not None
    entry = next(one for one in verdict["movers"]
                 if one["key"] == mover)
    if second_state == pool.FAILED:
        assert entry["state"] == "failed", (
            f"a failed newest generation read as {entry['state']!r}")
    else:
        assert entry["state"] in {"done", "evicted"}, (
            f"a done newest generation read as {entry['state']!r}")


def test_a_lone_legacy_record_is_still_the_keys_ending(tmp_path: Path) -> None:
    """A generation-less record with no competitor keeps the pbrun rule: it
    stands, because staleness cannot be proved and refusing it would hang a
    reader on the only account of what happened."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.item_path(pool.FAILED, KEY).write_text(json.dumps(
        {"action_key": KEY, "status": "failed"}))

    answer = _answer(queue)

    assert answer["state"] == pool.FAILED
    assert answer["generation"] is None
    assert answer["ambiguous"] is False


def test_an_unorderable_failure_beside_a_success_is_never_success(
        tmp_path: Path) -> None:
    """No generation on the failure, one on the success: the pair cannot be
    ordered, and an unknown generation is never read as success."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.item_path(pool.DONE, KEY).write_text(json.dumps(
        {"action_key": KEY, "status": "executed", "published_unix": 5.0}))
    queue.item_path(pool.FAILED, KEY).write_text(json.dumps(
        {"action_key": KEY, "status": "failed"}))

    answer = _answer(queue)

    assert answer["state"] == pool.FAILED, (
        "an unorderable failure beside a success answered success")
    assert answer["ambiguous"] is True


def test_an_unreadable_failure_beside_a_success_is_never_success(
        tmp_path: Path) -> None:
    """A record that cannot be read is reported, not guessed at, and it never
    turns a success claim into a confident one."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.item_path(pool.DONE, KEY).write_text(json.dumps(
        {"action_key": KEY, "status": "executed", "published_unix": 5.0}))
    queue.item_path(pool.FAILED, KEY).write_text("{not json")

    answer = _answer(queue)

    assert answer["state"] is None, (
        "an unreadable terminal was read around and reported as success")
    assert answer["ambiguous"] is True
    assert [entry["state"] for entry in answer["unreadable"]] == [pool.FAILED]


def test_a_stale_link_never_claims_over_an_unreadable_slot(
        tmp_path: Path) -> None:
    """Review correction 1: a link to failed@4 beside an UNREADABLE failed
    slot may describe an already-replaced generation.  That slot is unknown,
    not success -- the link is historical evidence, never current authority.
    """

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.item_path(pool.FAILED, KEY).write_text("{not json")
    queue.item_path(pool.DONE, KEY).write_text(json.dumps({
        "action_key": KEY, "status": "executed", "published_unix": 9.0,
        "supersedes_terminal": {"state": pool.FAILED, "status": "failed",
                                "published_unix": 4.0}}))

    answer = _answer(queue)

    assert answer["state"] is None, (
        "a stale link claimed over an unreadable terminal slot")
    assert answer["ambiguous"] is True
    assert answer["unreadable"], "the unreadable record is not reported"


def test_a_link_does_not_outrank_a_newer_readable_generation(
        tmp_path: Path) -> None:
    """Review correction 1: the slot the link names now holds a strictly
    newer generation; ordering wins over the historical link."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.item_path(pool.DONE, KEY).write_text(json.dumps({
        "action_key": KEY, "status": "executed", "published_unix": 9.0,
        "supersedes_terminal": {"state": pool.FAILED, "status": "failed",
                                "published_unix": 5.0}}))
    queue.item_path(pool.FAILED, KEY).write_text(json.dumps(
        {"action_key": KEY, "status": "failed", "published_unix": 10.0}))

    answer = _answer(queue)

    assert answer["state"] == pool.FAILED
    assert answer["generation"] == pytest.approx(10.0)


def test_a_carrier_with_no_generation_is_not_answered_by_its_link(
        tmp_path: Path) -> None:
    """Review correction 1: a carrier whose own generation is missing has
    nothing to order by, so the link it carries cannot make it current."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.item_path(pool.DONE, KEY).write_text(json.dumps({
        "action_key": KEY, "status": "executed",
        "supersedes_terminal": {"state": pool.FAILED, "status": "failed",
                                "published_unix": 5.0}}))
    queue.item_path(pool.FAILED, KEY).write_text(json.dumps(
        {"action_key": KEY, "status": "failed", "published_unix": 5.0}))

    answer = _answer(queue)

    assert answer["state"] != pool.DONE, (
        "a generation-less carrier was answered by its historical link")
    assert answer["ambiguous"] is True


@pytest.mark.parametrize("first_state,second_state", [
    (pool.DONE, pool.FAILED), (pool.FAILED, pool.DONE)])
def test_an_unlinked_pair_resolves_by_generation(
        tmp_path: Path, first_state, second_state) -> None:
    """Basic fallback coverage the linked producer pairs masked: with no
    ``supersedes_terminal`` anywhere, the newer generation still answers."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    for state, generation in ((first_state, 4.0), (second_state, 9.0)):
        queue.item_path(state, KEY).write_text(json.dumps(
            {"action_key": KEY, "status": state, "published_unix": generation}))

    answer = _answer(queue)

    assert answer["state"] == second_state
    assert answer["generation"] == pytest.approx(9.0)
    assert [entry["state"] for entry in answer["history"]] == [first_state]
    assert answer["ambiguous"] is False


def test_a_tie_between_generations_prefers_the_failure(
        tmp_path: Path) -> None:
    """Equal generations cannot order a winner; the failure stands and the
    ambiguity is reported, in either directory-insertion order."""

    for order in ((pool.DONE, pool.FAILED), (pool.FAILED, pool.DONE)):
        root = tmp_path / f"queue-{order[0]}"
        queue = pool.PoolQueue(root)
        queue.ensure_layout()
        for state in order:
            queue.item_path(state, KEY).write_text(json.dumps(
                {"action_key": KEY, "status": state, "published_unix": 7.0}))

        answer = queue.current_ending(KEY)

        assert answer["state"] == pool.FAILED
        assert answer["ambiguous"] is True


@pytest.mark.parametrize("malformed", ["9", None, True, float("nan")])
def test_a_malformed_generation_never_answers_success(
        tmp_path: Path, malformed) -> None:
    """A malformed generation is not a generation: a success carrying one
    never outranks a readable failure."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.item_path(pool.DONE, KEY).write_text(json.dumps(
        {"action_key": KEY, "status": "executed", "published_unix": malformed}))
    queue.item_path(pool.FAILED, KEY).write_text(json.dumps(
        {"action_key": KEY, "status": "failed", "published_unix": 5.0}))

    answer = _answer(queue)

    assert answer["state"] == pool.FAILED, (
        f"a malformed generation ({malformed!r}) was read as authoritative")
    assert answer["ambiguous"] is True


def test_the_resolution_is_pure_over_one_captured_record_set(
        tmp_path: Path) -> None:
    """Review correction 2: the capture returns the records and the errors,
    and the resolution is pure over them, so nothing can be replaced between
    the census and the ordered answer."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.item_path(pool.DONE, KEY).write_text(json.dumps(
        {"action_key": KEY, "status": "executed", "published_unix": 4.0}))
    queue.item_path(pool.FAILED, KEY).write_text(json.dumps(
        {"action_key": KEY, "status": "failed", "published_unix": 9.0}))
    queue.item_path(pool.WITHDRAWN, KEY).write_text("{not json")

    readable, unreadable = queue.read_terminal_candidates(KEY)
    assert set(readable) == {pool.DONE, pool.FAILED}
    assert [entry["state"] for entry in unreadable] == [pool.WITHDRAWN]

    answer = queue.resolve_ending(readable, unreadable)

    assert answer["state"] == pool.FAILED
    assert answer["record"] is readable[pool.FAILED][1]
    assert answer["path"] == readable[pool.FAILED][0]


def test_resolve_state_keeps_the_ordered_record_over_the_census(
        tmp_path: Path) -> None:
    """Review correction 2: if a census and the resolution ever disagree,
    the tool reports the resolution's record, never a second read's."""

    ordered = {"action_key": KEY, "status": "failed", "published_unix": 9.0}
    census = {pool.FAILED: {"action_key": KEY, "status": "failed",
                            "published_unix": 3.0}}
    view = {"state": pool.FAILED, "record": ordered}

    state, record, _answer = pbmcp._resolve_state(census, view)

    assert state == pool.FAILED
    assert record == ordered, "the census record replaced the ordered one"


def test_an_unknown_mcp_answer_never_falls_back_to_directory_order(
        tmp_path: Path) -> None:
    """Review correction 2: an unknown or failed resolution stays unknown;
    it is not re-read as the old directory-order success."""

    census = {pool.DONE: {"action_key": KEY, "status": "executed",
                          "published_unix": 9.0},
              pool.FAILED: {"action_key": KEY, "status": "failed",
                            "published_unix": 10.0}}
    view = {"state": None, "record": None, "ambiguous": True}

    state, record, _answer = pbmcp._resolve_state(census, view)

    assert state is None and record is None, (
        "an unknown resolution fell back to a directory-order success")


def _session(queue: pool.PoolQueue, tmp_path: Path) -> pbmcp.Session:
    return pbmcp.Session(queue_root=queue.root, cas_root=tmp_path / "cas",
                         repo_link=tmp_path / "repo")


def test_pbstatus_marks_current_only_when_path_and_generation_match(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Astra review item 1: the endings projection and the resolution are two
    reads, so the slot can be replaced between them.  Path equality alone
    would mark the OLD row current for a newer record; the generation must
    match too, and a mismatch is unknown with a reason."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    path = queue.item_path(pool.DONE, KEY)
    path.write_text(json.dumps({"action_key": KEY, "status": "executed",
                                "published_unix": 5.0, "finished_unix": 5.0}))
    replaced = []

    real = pbstatus.ending_row

    def racing(entry, queue_root):
        row = real(entry, queue_root)
        if row.get("action_key") == KEY and not replaced:
            replaced.append(True)
            path.write_text(json.dumps({
                "action_key": KEY, "status": "executed",
                "published_unix": 10.0, "finished_unix": 10.0}))
        return row

    monkeypatch.setattr(pbstatus, "ending_row", racing)

    rows = pbstatus.read_endings(queue.root, limit=10)
    row = next(one for one in rows if one["action_key"] == KEY)

    assert replaced, "the fixture did not replace the slot"
    assert row["published_unix"] == pytest.approx(5.0)
    assert row["current"] is None, (
        "the replaced slot's old row was marked current")
    assert "replaced" in row["current_unresolved"]


def test_public_tools_report_unknown_not_absent_for_an_unreadable_terminal(
        tmp_path: Path) -> None:
    """Astra review item 2: a readable done beside a malformed failed record
    holds known terminal records but no chosen one.  The tools must answer
    unknown with the unreadable path and reason, not ``found: false``."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.item_path(pool.DONE, KEY).write_text(json.dumps({
        "action_key": KEY, "status": "executed", "published_unix": 5.0,
        "finished_unix": 5.0, "attempts": 1}))
    queue.item_path(pool.FAILED, KEY).write_text("{not json")
    failed_path = str(queue.item_path(pool.FAILED, KEY))

    session = _session(queue, tmp_path)
    action = session.call("pb_action", {"key_prefix": KEY[:12]})
    assert action["state"] is None
    assert action["found"] is None, (
        "known terminal records answered as an absent key")
    unresolved = action["unresolved_terminals"]
    assert [entry["state"] for entry in unresolved["unreadable"]] == [
        pool.FAILED]
    assert unresolved["unreadable"][0]["path"] == failed_path
    assert unresolved["unreadable"][0]["reason"]

    receipts = session.call("pb_receipts", {"keys": [KEY[:12]]})
    row = next(one for one in receipts["receipts"] if one["action_key"] == KEY)
    assert row["found"] is None, "the receipt row answered absent"
    assert row["unresolved_terminals"]["unreadable"][0]["path"] == failed_path
    assert receipts["summary"]["unknown"] == 1
    assert receipts["summary"]["errors"] == 0

    log = session.call("pb_log", {"key_prefix": KEY[:12]})
    assert log["found"] is None, "the log row answered absent"
    assert log["unresolved_terminals"]["unreadable"][0]["path"] == failed_path


def test_a_failed_terminal_capture_is_unavailable_not_an_absent_key(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Astra review item 2: a capture that cannot read the slots at all must
    reach ``call.read``'s unavailable accounting, never be swallowed as an
    empty census."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    # A decision makes the prefix resolvable without a terminal marker, so
    # the call reaches the failing capture rather than an unresolved prefix.
    decisions = queue.root / pool.WITHDRAWN / "decisions" / KEY
    decisions.mkdir(parents=True)
    (decisions / "5.json").write_text(json.dumps(
        {"action_key": KEY, "published_unix": 5.0}))

    def refuse(self, action_key):
        raise OSError("the queue root did not answer")

    # Both the capture and the composed convenience are refused, so the test
    # reproduces the swallowed failure on a candidate that reads terminals
    # through either name.
    monkeypatch.setattr(pool.PoolQueue, "read_terminal_candidates", refuse,
                        raising=False)
    monkeypatch.setattr(pool.PoolQueue, "current_ending", refuse,
                        raising=False)

    session = _session(queue, tmp_path)
    body = session.call("pb_action", {"key_prefix": KEY[:12]})

    assert body["found"] is None
    assert body["states"] is None
    assert body["complete"] is False
    assert any(entry["section"] == "records" for entry in body["unavailable"]), (
        "the failed capture was not recorded as unavailable")


def test_an_absent_key_is_still_absent_for_public_tools(tmp_path: Path) -> None:
    """A key with no terminal marker (only an immutable decision, which makes
    its prefix resolvable) is still absent, with no unresolved terminals."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    decisions = queue.root / pool.WITHDRAWN / "decisions" / KEY
    decisions.mkdir(parents=True)
    (decisions / "5.json").write_text(json.dumps(
        {"action_key": KEY, "published_unix": 5.0}))

    session = _session(queue, tmp_path)
    action = session.call("pb_action", {"key_prefix": KEY[:12]})
    assert action["found"] is False
    assert action["state"] is None
    assert "unresolved_terminals" not in action

    log = session.call("pb_log", {"key_prefix": KEY[:12]})
    assert log["found"] is False
    assert "unresolved_terminals" not in log

    receipts = session.call("pb_receipts", {"keys": [KEY[:12]]})
    row = next(one for one in receipts["receipts"] if one["action_key"] == KEY)
    assert row["found"] is False
    assert receipts["summary"]["errors"] == 1
    assert receipts["summary"]["unknown"] == 0

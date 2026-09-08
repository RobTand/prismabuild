"""Each ``pbmcp`` tool, against a queue holding one action in each state.

In process rather than over the transport: the protocol is
``test_pbmcp_protocol``'s subject, and what is under test here is what the
tools say about a queue.  The envelope is checked on every one of them
anyway, because a tool that answers without saying whether its answer is
complete is the failure mode this server exists to remove.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(REPOSITORY / "tests"))

from prismabuild import pool  # noqa: E402
import pbmcp  # noqa: E402
import pbmcp_fixture as fx  # noqa: E402


@pytest.fixture()
def fleet(tmp_path: Path) -> fx.Fleet:
    return fx.build(tmp_path)


@pytest.fixture()
def session(fleet: fx.Fleet) -> pbmcp.Session:
    return pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                         repo_link=fleet.repo_link)


def test_status_shows_the_queue_the_fleet_actually_holds(
    session: pbmcp.Session,
) -> None:
    body = session.call("pb_status")
    assert body["complete"] is True and body["timed_out"] == []
    states = {row["action_key"]: row["state"] for row in body["jobs"]}
    assert states[fx.READY_KEY] == "READY"
    assert states[fx.CLAIMED_KEY] == "CLAIMED"
    endings = {row["action_key"]: row["status"] for row in body["endings"]}
    assert endings[fx.DONE_KEY] == "executed"
    assert body["queue"]["ready"] == 1 and body["queue"]["claimed"] == 1


def test_status_counts_reservation_tokens_as_files_not_as_a_verdict(
    session: pbmcp.Session, fleet: fx.Fleet,
) -> None:
    ledger = fleet.queue_root / pool.RESERVATIONS / "fixture-box"
    (ledger / "free").mkdir(parents=True)
    for index in range(3):
        (ledger / "free" / f"cpu-{index:04d}").write_text("")
    (ledger / "held" / "holder-a").mkdir(parents=True)
    (ledger / "held" / "holder-a" / "cpu-0009").write_text("")

    body = session.call("pb_status")
    host = body["reservations"]["fixture-box"]
    assert host["free_tokens"] == {"cpu": 3}
    assert host["held_tokens"] == {"cpu": 1}
    assert host["holders"] == 1


def test_action_reports_the_sealed_submission(session: pbmcp.Session,
                                              fleet: fx.Fleet) -> None:
    body = session.call("pb_action", {"key_prefix": fx.READY_KEY[:12]})
    assert body["found"] is True and body["state"] == "ready"
    sealed = body["sealed"]
    assert sealed["tags"] == ["gb10"]
    assert sealed["priority"] == 5
    assert sealed["resources"] == {"cpu": 3, "mem_gb": 6}
    assert sealed["checkout_root"] == str(fleet.checkout)
    assert sealed["published_by"]
    assert body["attempts_detail"] == []
    assert "log_tail" not in body, "a ready action has published no log"


def test_action_reports_the_ending_its_attempts_and_a_log_tail(
    session: pbmcp.Session,
) -> None:
    body = session.call("pb_action", {"key_prefix": fx.DONE_KEY[:12],
                                      "tail_lines": 2})
    assert body["complete"] is True
    assert body["state"] == "done"
    assert body["outcome"]["status"] == "executed"
    assert body["outcome"]["returncode"] == 0
    assert body["outcome"]["elapsed_s"] == 1.5
    assert body["adopted_attempt"] == 1

    attempt = body["attempts_detail"][0]
    assert attempt["attempt"] == 1 and attempt["status"] == "executed"
    assert attempt["logs"]["stdout"]["bytes"] == len(fx.STDOUT.encode())
    assert "stdout" not in attempt, (
        "an attempt record must arrive with its log metadata and not its log; "
        "expanding it is what makes a status call cost a gigabyte")

    tail = body["log_tail"]
    assert tail["lines"] == ["second line", "third line"]
    assert tail["truncated_head"] is False
    assert tail["bytes_match"] is True
    assert tail["total_bytes"] == len(fx.STDOUT.encode())


def test_action_derives_the_local_result_claim_and_finds_the_receipt(
    session: pbmcp.Session, fleet: fx.Fleet,
) -> None:
    """The digest is not on the queue record, and it is still recoverable.

    ``run_action_locally`` returns it and nothing carries it into the item,
    so an agent holding a key could not reach its own result.  It is a hash
    of the manifest, the resolved checkout and the declared paths, all of
    which are reachable, so it is derived with the producer's own function
    rather than reported as unavailable.
    """

    body = session.call("pb_action", {"key_prefix": fx.DONE_KEY[:12]})
    claim = body["local_result_claim"]
    assert claim["sha256"] == fx.claim_digest(fleet)
    assert claim["present"] is True
    receipt = body["receipt"]
    assert receipt["present"] is True
    assert receipt["result"]["bytes"] == len(fx.PAYLOAD)


def test_a_snapshot_submission_says_why_the_claim_cannot_be_derived(
    session: pbmcp.Session, fleet: fx.Fleet,
) -> None:
    """A refusal that names its reason, rather than a silent ``null``."""

    path = fleet.queue.item_path(pool.READY, fx.READY_KEY)
    record = json.loads(path.read_text(encoding="utf-8"))
    record.pop("checkout_root")
    record["checkout_snapshot"] = {"commit": "0" * 40}
    path.write_text(json.dumps(record), encoding="utf-8")

    claim = session.call("pb_action", {"key_prefix": fx.READY_KEY[:12]})[
        "local_result_claim"]
    assert claim["sha256"] is None
    assert "checkout_snapshot" in claim["reason"]


def test_an_ambiguous_prefix_answers_with_its_candidates(
    session: pbmcp.Session, fleet: fx.Fleet,
) -> None:
    fleet.queue.publish(action_key=fx.TWIN_KEY, cas_root=fleet.cas_root,
                        checkout_root=str(fleet.checkout),
                        worker_script=str(fleet.base / "worker.py"))
    with pytest.raises(pbmcp.ToolError) as raised:
        session.call("pb_action", {"key_prefix": fx.DONE_KEY[:12]})
    assert set(raised.value.detail["candidates"]) == {fx.DONE_KEY, fx.TWIN_KEY}


def test_a_prefix_that_cannot_name_a_key_says_so_before_reading_the_mount(
    session: pbmcp.Session,
) -> None:
    with pytest.raises(pbmcp.ToolError) as raised:
        session.call("pb_action", {"key_prefix": "not-hex"})
    assert "hexadecimal" in str(raised.value)


def test_actions_filters_and_says_what_it_could_not_filter_on(
    session: pbmcp.Session, fleet: fx.Fleet,
) -> None:
    body = session.call("pb_actions", {"limit": 20})
    found = {row["action_key"]: row for row in body["actions"]}
    assert set(found) == {fx.READY_KEY, fx.CLAIMED_KEY, fx.DONE_KEY}
    assert found[fx.READY_KEY]["priority"] == 5
    assert body["identity"]["submitter_field"] is None
    assert "no submitter identity" in body["identity"]["note"]

    only_ready = session.call("pb_actions", {"states": ["ready"], "limit": 20})
    assert [row["action_key"] for row in only_ready["actions"]] == [fx.READY_KEY]

    by_tag = session.call("pb_actions", {"tags": ["x86"], "limit": 20})
    assert set(row["action_key"] for row in by_tag["actions"]) == {
        fx.CLAIMED_KEY, fx.DONE_KEY}

    by_checkout = session.call("pb_actions", {"checkout_root": "/elsewhere",
                                              "limit": 20})
    assert by_checkout["actions"] == []
    mine = session.call("pb_actions", {"checkout_root": str(fleet.checkout),
                                       "limit": 20})
    assert len(mine["actions"]) == 3

    band = session.call("pb_actions", {"priority_max": -1, "limit": 20})
    assert [row["action_key"] for row in band["actions"]] == [fx.DONE_KEY]


def test_actions_refuses_a_state_that_is_not_one(session: pbmcp.Session) -> None:
    with pytest.raises(pbmcp.ToolError):
        session.call("pb_actions", {"states": ["nearly-done"]})


def test_actions_by_key_bypasses_the_newest_first_window(
    session: pbmcp.Session,
) -> None:
    """A named key must not be missable because the window was small."""

    windowed = session.call("pb_actions", {"states": ["done"], "limit": 1})
    assert windowed["truncated"] is True
    named = session.call("pb_actions", {"keys": [fx.DONE_KEY[:12]], "limit": 1})
    assert [row["action_key"] for row in named["actions"]] == [fx.DONE_KEY]
    assert named["truncated"] is False


def test_verify_claim_reports_each_check_by_name(session: pbmcp.Session,
                                                 fleet: fx.Fleet) -> None:
    body = session.call("pb_verify_claim", {"sha256": fx.claim_digest(fleet)})
    assert body["checks_passed"] is True
    assert "verified" not in body, (
        "the verdict this tool can reach is 'every check it ran passed'; a "
        "field called verified would claim the one it cannot reach")
    assert body["attestation_verified"] is None, (
        "the check that is still owed says so as a value, not only in prose")
    checks = body["checks"]
    assert checks["claim_present"] is True
    assert checks["claim_addressed_correctly"] is True
    assert checks["claim_digest_matches_body"] is True
    assert checks["receipt_present"] is True
    assert checks["receipt_self_consistent"] is True
    assert checks["receipt_binds_the_claims_manifest"] is True
    assert checks["payload_present"] is True
    assert checks["payload_bytes_match"] is True
    assert checks["payload_sha256_match"] is None, (
        "a result blob is a rendered model often enough that hashing one by "
        "default would spend an hour of NFS bandwidth by accident")
    assert body["not_checked"], "say what was not checked, every time"


def test_verify_claim_hashes_the_payload_when_asked(session: pbmcp.Session,
                                                    fleet: fx.Fleet) -> None:
    body = session.call("pb_verify_claim", {"sha256": fx.claim_digest(fleet),
                                            "hash_payload": True})
    assert body["checks"]["payload_sha256_match"] is True
    assert body["checks_passed"] is True
    assert body["attestation_verified"] is None


def test_verify_claim_fails_a_payload_that_is_not_what_the_receipt_says(
    session: pbmcp.Session, fleet: fx.Fleet,
) -> None:
    import hashlib

    digest = hashlib.sha256(fx.PAYLOAD).hexdigest()
    blob = fleet.cas_root / "blobs" / digest[:2] / digest
    blob.chmod(0o644)
    blob.write_bytes(fx.PAYLOAD + b"tampered\n")

    body = session.call("pb_verify_claim", {"sha256": fx.claim_digest(fleet)})
    assert body["checks"]["payload_bytes_match"] is False
    assert body["checks_passed"] is False


def test_verify_claim_says_a_missing_claim_is_missing(
    session: pbmcp.Session,
) -> None:
    body = session.call("pb_verify_claim", {"sha256": "0" * 64})
    assert body["checks"]["claim_present"] is False
    assert body["checks_passed"] is False
    assert body["attestation_verified"] is None


def test_verify_claim_refuses_something_that_is_not_a_digest(
    session: pbmcp.Session,
) -> None:
    with pytest.raises(pbmcp.ToolError):
        session.call("pb_verify_claim", {"sha256": "nope"})


def test_log_returns_a_bounded_tail_of_the_stream_asked_for(
    session: pbmcp.Session,
) -> None:
    body = session.call("pb_log", {"key_prefix": fx.DONE_KEY[:12],
                                   "tail_lines": 1})
    assert body["log"]["lines"] == ["third line"]
    errors = session.call("pb_log", {"key_prefix": fx.DONE_KEY[:12],
                                     "stream": "stderr"})
    assert errors["log"]["lines"] == ["a warning"]


def test_log_never_reads_more_than_its_cap_and_says_it_skipped_a_head(
    fleet: fx.Fleet,
) -> None:
    small = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                          repo_link=fleet.repo_link, log_tail_bytes=12)
    body = small.call("pb_log", {"key_prefix": fx.DONE_KEY[:12],
                                 "tail_lines": 5})
    log = body["log"]
    assert log["bytes_read"] <= 12
    assert log["truncated_head"] is True
    assert log["total_bytes"] == len(fx.STDOUT.encode())
    assert "first line" not in log["lines"]


def test_log_says_a_running_action_has_not_published_one(
    session: pbmcp.Session,
) -> None:
    body = session.call("pb_log", {"key_prefix": fx.CLAIMED_KEY[:12]})
    assert body["log"] is None
    assert "finishes" in body["reason"]


def test_log_refuses_a_stream_that_is_not_one(session: pbmcp.Session) -> None:
    with pytest.raises(pbmcp.ToolError):
        session.call("pb_log", {"key_prefix": fx.DONE_KEY[:12],
                                "stream": "syslog"})


def test_runtime_reports_the_generation_and_who_is_on_it(
    session: pbmcp.Session,
) -> None:
    body = session.call("pb_runtime")
    assert body["generation"] == fx.GENERATION_A
    assert body["manifest"]["generation"] == fx.GENERATION_A
    assert body["manifest"]["files"] == 1
    assert body["generation_stale"] is False
    assert body["started_from_generation"] is False, (
        "this server was started from a checkout, not from the generation "
        "the fleet publishes, and it must say so rather than imply agreement")
    assert body["loops"] == []

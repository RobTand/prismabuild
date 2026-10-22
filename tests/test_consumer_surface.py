"""Versioned consumer CLI contracts use private queues and CAS objects (#1892)."""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools/fleet")]

import pbrun
import pbwait
from prismabuild import action_client, action_result, core as pb, pool
from fleet_world import Admission, FleetWorld
from submission_support import patch_store
from test_verified_action_result import _standard_action, _publish_result
from test_pool_resource_scope import scoped
from pbmcp_fixture import settle_publishers
from test_slurm_withdraw_siblings import controller, _lane_record, KEY as SLURM_KEY

ROW_FIELDS = {"action_key", "status", "succeeded", "exit_code", "signal", "host",
              "attempt", "publication", "started_unix", "ended_unix", "reason",
              "result", "diagnostics", "phases"}
RESULT_FIELDS = {"verified", "receipt_sha256", "stdout", "stderr", "outputs"}


@pytest.fixture
def consumer_world(tmp_path, monkeypatch):
    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT"):
        monkeypatch.delenv(name, raising=False)
    world = FleetWorld(tmp_path, admission=Admission(
        capacity={"cpu": 8, "mem_gb": 16}, default_demand={"cpu": 1, "mem_gb": 1}))
    patch_store(monkeypatch, root=tmp_path)
    monkeypatch.setenv(pbwait.slurm_lane.LANE_ROOT_ENV, str(tmp_path / "slurm"))
    return world


def consumer_action(world, name, status="executed", action_exit=None, *, with_receipt=False):
    base = world.root / name
    checkout = world.root / "checkout"
    checkout.mkdir(parents=True, exist_ok=True)
    action = _standard_action(checkout, command=["/bin/echo", name])
    cas = pb.PrismaBuildCAS(world.root / "cas")
    cas.publish_action_request(action)
    receipt = None
    stdout = "failed command output\n"
    if status in {"executed", "cache_hit"} or with_receipt:
        # Build the receipt in the same CAS used by the queue's sealed request.
        scratch_cas, receipt = _publish_result(world.root, action, b"result\n")
        assert scratch_cas.root == cas.root
        stdout = json.dumps({"status": "published", "receipt": receipt}) + "\n"
    key = action["action_key"]
    world.actions.publish(key, cas_root=cas.root, checkout_root=checkout, tags=["x86"],
                          resources={"cpu": 1, "mem_gb": 1}, max_attempts=1)
    if status != "waiting":
        world.queue.claim(tags=["x86"], capacity={"cpu": 8, "mem_gb": 16})
        world.queue.finish(key, status=status, detail={
            "status": status, "returncode": 0 if receipt else 1,
            "action_returncode": action_exit if action_exit is not None else 0 if receipt else 7,
            "stdout": stdout, "stderr": "diagnostic\n"})
        settle_publishers()
    return key, receipt


def test_json_endings_are_ordered_and_verify_success_and_failure(consumer_world, capsys):
    success, receipt = consumer_action(consumer_world, "success")
    failed, _ = consumer_action(consumer_world, "failed", "failed", 7)
    assert pbwait.main(["--endings", "--wait-s", "0", failed, success, failed]) == 1
    body = json.loads(capsys.readouterr().out)
    assert set(body) == {"schema", "complete", "endings"}
    assert body["schema"] == "prismabuild.endings.v1"
    assert body["complete"] is True
    assert [row["action_key"] for row in body["endings"]] == [failed, success, failed]
    for row in body["endings"]:
        assert set(row) == ROW_FIELDS
        assert set(row["publication"]) == {"published_unix", "generation"}
        assert row["attempt"] == 1
        assert row["publication"]["generation"]
        result = row["result"]
        assert set(result) == RESULT_FIELDS
        assert result["verified"] is True
        assert result["outputs"] == []
        for stream in ("stdout", "stderr"):
            log = result[stream]
            assert set(log) == {"path", "bytes", "sha256"}
            raw = Path(log["path"]).read_bytes()
            assert len(raw) == log["bytes"]
            assert pb.raw_sha256(raw) == log["sha256"]
        assert "resource_scope_cleanup" in row["diagnostics"]
        assert "cleanup" in row["diagnostics"]
    assert body["endings"][0]["exit_code"] == 7
    assert body["endings"][0]["reason"]
    assert body["endings"][0]["result"]["receipt_sha256"] is None
    assert body["endings"][1]["result"]["receipt_sha256"] == receipt["receipt_sha256"]


@pytest.mark.parametrize("status,exit_code", [("executed", 0), ("cache_hit", 0),
                                               ("failed", 1), ("refused", 1), ("waiting", 75)])
def test_json_exit_codes_from_real_records(consumer_world, capsys, status, exit_code):
    key, _ = consumer_action(consumer_world, status, status)
    assert pbwait.main(["--endings", "--wait-s", "0", key]) == exit_code
    body = json.loads(capsys.readouterr().out)
    assert body["complete"] == (status != "waiting")
    assert body["endings"][0]["status"] == status
    if status != "waiting":
        assert body["endings"][0]["result"]["verified"] is True


def test_withdraw_json_and_human_output_match(consumer_world, capsys):
    key, _ = consumer_action(consumer_world, "withdraw", "waiting")
    assert pbrun.main(["--withdraw", key, "--reason", "stop this fixture", "--json"]) == 0
    captured = capsys.readouterr()
    body = json.loads(captured.out)
    assert set(body) == {"schema", "action_key", "state_before", "withdrawn", "reason"}
    assert body == {"schema": "prismabuild.withdrawal.v1", "action_key": key,
                    "state_before": "ready", "withdrawn": True, "reason": "stop this fixture"}
    assert pbwait.main(["--endings", "--wait-s", "0", key]) == 1
    row = json.loads(capsys.readouterr().out)["endings"][0]
    assert row["status"] == "withdrawn"
    assert row["result"] is None
    assert row["diagnostics"]["cleanup"]["complete"] is True
    other, _ = consumer_action(consumer_world, "human-withdraw", "waiting")
    assert pbrun.main(["--withdraw", other, "--reason", "stop this fixture"]) == 0
    human = capsys.readouterr()
    assert human.out == ""
    assert human.err.replace(other[:12], "KEY") == captured.err.replace(key[:12], "KEY")


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_tampered_failed_log_is_unreadable(consumer_world, capsys, stream):
    key, _ = consumer_action(consumer_world, "tamper", "failed")
    record = json.loads(consumer_world.queue.records.item_path("failed", key).read_bytes())
    attempt = consumer_world.queue.records.attempt_path(record, 1)
    metadata = json.loads(attempt.read_bytes())["logs"][stream]
    path = consumer_world.queue.root / metadata["path"]
    path.chmod(0o644)
    path.write_bytes(b"forged\n")
    path.chmod(0o444)
    assert pbwait.main(["--endings", "--wait-s", "0", key]) == 74
    body = json.loads(capsys.readouterr().out)
    assert body["complete"] is False
    row = body["endings"][0]
    assert row["status"] == "unreadable"
    assert row["succeeded"] is False
    assert row["result"] is None


def test_ending_reader_preserves_success_only_sdk_and_bounds_logs(consumer_world):
    key, _ = consumer_action(consumer_world, "bounded", "failed")
    record = json.loads(consumer_world.queue.records.item_path("failed", key).read_bytes())
    identity = {"published_unix": record["published_unix"], "attempt": 1}
    with pytest.raises(action_result.ActionResultError):
        action_result.read_verified_action_result(consumer_world.queue, key,
                                                  max_result_bytes=1024, **identity)
    with pytest.raises(action_result.ActionResultError, match="cap"):
        action_result.read_verified_ending_result(consumer_world.queue, key,
                                                  max_log_bytes=1, **identity)


@pytest.mark.parametrize("status,expected", [("executed", 0), ("failed", 1),
    ("withdrawn", 1), ("refused", 1), ("waiting", 75), ("superseded", 76), ("unreadable", 74)])
def test_consumer_exit_constants(status, expected):
    assert pbwait.consumer_endings_verdict([
        {"status": status, "succeeded": status == "executed"}]) == expected
    assert action_client.GAVE_UP_EXIT == 75
    assert action_client.PUBLICATION_SUPERSEDED_EXIT == 76
    assert action_client.RECORD_WRITE_FAILED_EXIT == 74
    assert action_client.WITHDRAWN_EXIT == 143


@pytest.mark.parametrize("arguments", [["--endings", "--wait-s", "0", "a" * 64],
                                      ["--endings", "--wait-s", "-1", "a" * 64],
                                      ["--endings", "--wait-s", "nan", "a" * 64]])
def test_usage_and_unknown_keys_are_exit_two(consumer_world, arguments):
    with pytest.raises(SystemExit) as exc:
        pbwait.main(arguments)
    assert exc.value.code == 2


def test_human_table_unchanged(consumer_world, capsys):
    key, _ = consumer_action(consumer_world, "human", "failed")
    rows = pbwait.wait_for_keys(consumer_world.queue, [key],
                              cas=pb.PrismaBuildCAS(consumer_world.root / "cas"), wait_s=0)
    assert pbwait.main(["--wait-s", "0", key]) == 1
    assert capsys.readouterr().out == pbwait.render(rows) + "\n"


def test_withdraw_parser_contract(consumer_world):
    flags = pbrun.argument_parser()._option_string_actions
    assert {"--json", "--withdraw", "--reason"} <= set(flags)
    assert flags["--json"].nargs == 0
    args = pbrun.parse_args(["--withdraw", "a" * 64, "--reason", "because", "--json"])
    assert args.json is True and args.withdraw == ["a" * 64] and args.reason == "because"


def test_a_running_withdrawal_stays_withdrawn_until_cleanup(consumer_world, capsys):
    key, _ = consumer_action(consumer_world, "running-withdraw", "waiting")
    consumer_world.queue.claim(tags=["x86"], capacity={"cpu": 8, "mem_gb": 16})
    assert pbrun.main(["--withdraw", key, "--reason", "stop", "--json"]) == 0
    capsys.readouterr()
    assert pbwait.main(["--endings", "--wait-s", "0", key]) == 1
    row = json.loads(capsys.readouterr().out)["endings"][0]
    assert row["status"] == "withdrawn"
    assert row["result"] is None
    assert row["diagnostics"]["cleanup"]["complete"] is False


def test_failed_result_refuses_foreign_generation_and_replacement(consumer_world, monkeypatch):
    key, _ = consumer_action(consumer_world, "replace", "failed")
    path = consumer_world.queue.records.item_path("failed", key)
    record = json.loads(path.read_bytes())
    with pytest.raises(action_result.ActionResultError):
        action_result.read_verified_ending_result(consumer_world.queue, key,
            published_unix=record["published_unix"] + 1, attempt=1)
    original = action_result._read_bounded
    def replace_after_log(log_path, **kwargs):
        raw = original(log_path, **kwargs)
        if str(log_path).endswith(".stdout." + pb.raw_sha256(b"failed command output\n") + ".log"):
            path.write_bytes(pb._canonical_file_bytes({**record, "published_unix": record["published_unix"] + 1}))
        return raw
    monkeypatch.setattr(action_result, "_read_bounded", replace_after_log)
    with pytest.raises(action_result.ActionResultError):
        action_result.read_verified_ending_result(consumer_world.queue, key,
            published_unix=record["published_unix"], attempt=1)


def test_failed_ending_can_carry_its_exact_published_receipt(consumer_world, capsys):
    key, receipt = consumer_action(consumer_world, "failed-after-publish", "failed", 0,
                                   with_receipt=True)
    assert pbwait.main(["--endings", "--wait-s", "0", key]) == 1
    row = json.loads(capsys.readouterr().out)["endings"][0]
    assert row["status"] == "failed" and row["succeeded"] is False
    assert row["exit_code"] == 0
    assert row["result"]["verified"] is True
    assert row["result"]["receipt_sha256"] == receipt["receipt_sha256"]


def test_concluded_withdrawal_verifies_its_attempt_logs(consumer_world, capsys):
    key, _ = consumer_action(consumer_world, "concluded-withdraw", "waiting")
    consumer_world.queue.claim(tags=["x86"], capacity={"cpu": 8, "mem_gb": 16})
    consumer_world.queue.withdraw(key, reason="stop", signal_child=False)
    consumer_world.queue.finish(key, status="withdrawn", detail={
        "returncode": 143, "stdout": "partial output\n", "stderr": "stopped\n"})
    settle_publishers()
    assert pbwait.main(["--endings", "--wait-s", "0", key]) == 1
    row = json.loads(capsys.readouterr().out)["endings"][0]
    assert row["status"] == "withdrawn"
    assert row["result"]["verified"] is True
    assert Path(row["result"]["stdout"]["path"]).read_text() == "partial output\n"


def test_slurm_withdraw_json_reports_the_filed_decision_when_scancel_refuses(
        consumer_world, controller, monkeypatch, capsys):
    _lane_record(consumer_world.root / "slurm")
    monkeypatch.setenv("FAKE_SCANCEL_REFUSES", "1001")
    assert pbrun.main(["--withdraw", SLURM_KEY, "--reason", "stop", "--json"]) == 2
    row = json.loads(capsys.readouterr().out)
    assert row == {"schema": "prismabuild.withdrawal.v1", "action_key": SLURM_KEY,
                   "state_before": None, "withdrawn": True, "reason": "stop"}
    assert consumer_world.queue.records.item_path("withdrawn", SLURM_KEY).exists()


def test_json_publication_identity_for_a_waiting_action(consumer_world, capsys):
    key, _ = consumer_action(consumer_world, "publication", "waiting")
    assert pbwait.main(["--endings", "--wait-s", "0", key]) == 75
    row = json.loads(capsys.readouterr().out)["endings"][0]
    assert row["publication"]["generation"] == consumer_world.queue.records.attempt_generation({
        "action_key": key, "published_unix": row["publication"]["published_unix"]})


@pytest.mark.parametrize("status,expected", [("executed", 0), ("failed", 1), ("waiting", 75)])
def test_legacy_json_supports_trustladder_parse(consumer_world, capsys, status, expected):
    key, _ = consumer_action(consumer_world, "legacy-" + status, status)
    assert pbwait.main(["--json", "--wait-s", "0", key]) == expected
    out = capsys.readouterr().out
    (row,) = json.loads(out)
    assert row["status"] == status
    expected = {"action_key", "status", "transport", "job", "host", "elapsed_s",
                "returncode", "action_returncode", "action_signal", "receipt_published",
                "note", "succeeded", "phases"}
    if status != "waiting":
        expected |= {"withdrawn_attempt", "resource_scope_cleanup", "container_cleanup"}
    assert set(row) == expected


@pytest.mark.parametrize("arguments", [
    ["--json", "--endings"], ["--until", "cleaned"],
    ["--json", "--until", "cleaned"], ["--until", "ended"],
    ["--endings", "--until", "unknown"],
    ["--endings", "--reconcile-pool", "--generation", "b" * 64, "--attempt", "1"],
])
def test_endings_option_combinations_are_usage_errors(consumer_world, arguments):
    with pytest.raises(SystemExit) as exc:
        pbwait.main([*arguments, "a" * 64])
    assert exc.value.code == 2


@pytest.mark.parametrize("status", ["cache_hit", "withdrawn", "superseded", "refused", "failed"])
def test_no_attempt_has_no_cleanup_debt(consumer_world, status):
    row = pbwait.consumer_ending_row(consumer_world.queue, pbwait._row("a" * 64, status),
        cas=pb.PrismaBuildCAS(consumer_world.root / "cas"),
        context={"attempt": 0, "claimed": False})
    assert row["diagnostics"]["cleanup"]["complete"] is True


@pytest.mark.parametrize("scope,containers,expected", [
    (None, None, False), (False, True, False), (True, False, False),
    (False, False, False), (True, True, True), (True, None, True),
])
def test_claimed_attempt_requires_scope_and_container_cleanup(
        consumer_world, scope, containers, expected):
    row = pbwait.consumer_ending_row(consumer_world.queue,
        pbwait._row("a" * 64, "withdrawn"),
        cas=pb.PrismaBuildCAS(consumer_world.root / "cas"),
        context={"claimed": True, "result_ended": True,
                 "resource_scope_cleanup": None if scope is None else {"complete": scope},
                 "container_cleanup": None if containers is None else {"complete": containers}})
    assert row["diagnostics"]["cleanup"]["complete"] is expected


def test_until_cleaned_keeps_the_ending_at_the_deadline(consumer_world, capsys):
    key, _ = consumer_action(consumer_world, "cleanup-pending", "waiting")
    consumer_world.queue.claim(tags=["x86"], capacity={"cpu": 8, "mem_gb": 16})
    consumer_world.queue.withdraw(key, reason="stop", signal_child=False)
    assert pbwait.main(["--endings", "--until", "cleaned", "--wait-s", "0", key]) == 75
    body = json.loads(capsys.readouterr().out)
    assert body["complete"] is False
    row, = body["endings"]
    assert row["status"] == "withdrawn"
    assert row["diagnostics"]["cleanup"]["complete"] is False
    assert pbwait.main(["--endings", "--until", "ended", "--wait-s", "0", key]) == 1
    assert json.loads(capsys.readouterr().out)["complete"] is True


def test_until_cleaned_returns_for_a_ready_withdrawal(consumer_world, capsys):
    key, _ = consumer_action(consumer_world, "never-started", "waiting")
    consumer_world.queue.withdraw(key, reason="stop", signal_child=False)
    assert pbwait.main(["--endings", "--until", "cleaned", "--wait-s", "0", key]) == 1
    body = json.loads(capsys.readouterr().out)
    assert body["complete"] is True
    assert body["endings"][0]["diagnostics"]["cleanup"]["complete"] is True


def test_until_cleaned_rechecks_through_the_existing_waiter(consumer_world, monkeypatch, capsys):
    key, _ = consumer_action(consumer_world, "cleanup-arrives", "waiting")
    consumer_world.queue.claim(tags=["x86"], capacity={"cpu": 8, "mem_gb": 16})
    consumer_world.queue.withdraw(key, reason="stop", signal_child=False)
    original = pbwait._look_once_checked
    observed = []
    def finish_after_observation(*args, **kwargs):
        row = original(*args, **kwargs)
        observed.append(row)
        if len(observed) == 1:
            consumer_world.queue.finish(key, status="withdrawn", detail={
                "returncode": 143, "stdout": "stopped\n", "stderr": ""})
            settle_publishers()
        return row
    monkeypatch.setattr(pbwait, "_look_once_checked", finish_after_observation)
    assert pbwait.main(["--endings", "--until", "cleaned", "--wait-s", "30", key]) == 1
    body = json.loads(capsys.readouterr().out)
    assert body["complete"] is True
    row, = body["endings"]
    assert row["status"] == "withdrawn"
    assert row["diagnostics"]["cleanup"]["complete"] is True
    assert row["result"]["verified"] is True
    assert len(observed) >= 2
    assert observed[0]["withdrawn_attempt"] is None
    assert observed[-1]["withdrawn_attempt"] is not None



def test_no_scope_or_container_owner_has_no_cleanup_debt(consumer_world):
    row = pbwait.consumer_ending_row(consumer_world.queue,
        pbwait._row("a" * 64, "withdrawn"), cas=None,
        context={"claimed": True, "result_ended": True,
                 "scope_required": False, "containers_required": False})
    assert row["diagnostics"]["cleanup"]["complete"] is True



@pytest.mark.parametrize("container_record", [{}, {"complete": False},
    {"complete": None}, {"complete": 1}, {"complete": True}])
@pytest.mark.parametrize("location", ["context", "row"])
@pytest.mark.parametrize("claimed", [False, True])
def test_explicit_container_cleanup_always_requires_true(
        consumer_world, container_record, location, claimed):
    row = pbwait._row("a" * 64, "withdrawn")
    context = {"claimed": claimed, "result_ended": True,
               "resource_scope_cleanup": {"complete": True}}
    (context if location == "context" else row)["container_cleanup"] = container_record
    projected = pbwait.consumer_ending_row(consumer_world.queue, row, cas=None, context=context)
    assert projected["diagnostics"]["cleanup"]["containers"] == container_record
    assert projected["diagnostics"]["cleanup"]["complete"] is (
        container_record.get("complete") is True)


@pytest.mark.parametrize("removal", ["removed", "remaining", "error"])
def test_scope_cleanup_is_persisted_only_after_containers_are_removed(scoped, monkeypatch, removal):
    queue, item, calls = scoped
    queue._start_resource_scope(item)
    key = item["action_key"]
    owner = "c" * 64
    item.update(container_owner=owner, max_attempts=1)
    claim_path = queue.records.item_path("claimed", key)
    queue.records.replace_record_at(claim_path, item)
    marker = queue.container_marker(owner)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()
    containers = ["container-1892"]
    events = []
    published = []
    replace = queue.records.replace_record_at

    def assert_scope_cleanup_absent():
        assert "resource_scope_cleanup" not in item
        assert "resource_scope_cleanup" not in json.loads(claim_path.read_bytes())

    def list_containers(label, value):
        if not published:
            assert_scope_cleanup_absent()
        events.append("list-empty" if not containers else "list-populated")
        return list(containers)

    def remove_containers(identities):
        assert_scope_cleanup_absent()
        assert identities == ["container-1892"]
        events.append("remove")
        if removal == "error":
            raise OSError("container daemon refused removal")
        if removal == "removed":
            containers.clear()
        return list(identities)

    def persist_after_removal(path, value):
        if value.get("resource_scope_cleanup", {}).get("complete") is True:
            assert not containers
            assert not marker.exists()
            assert events.index("remove") < events.index("list-empty")
            published.append(str(path))
        return replace(path, value)

    monkeypatch.setattr(pool, "_docker_containers_with_label", list_containers)
    monkeypatch.setattr(pool, "_docker_remove_containers", remove_containers)
    monkeypatch.setattr(queue.records, "replace_record_at", persist_after_removal)
    result = queue.cleanup_action_containers(item)
    if removal != "removed":
        assert result["complete"] is False
        assert_scope_cleanup_absent()
        assert not published
        assert "release" not in calls
        assert marker.exists()
        return

    assert result["complete"] is True
    assert published
    assert item["resource_scope_cleanup"]["complete"] is True
    assert json.loads(claim_path.read_bytes())["resource_scope_cleanup"]["complete"] is True
    assert "container_cleanup" not in item
    # The real finish consumes the persisted proof without a second removal.
    queue.finish(key, status="failed", detail={"returncode": 7, "stdout": "stopped\n"},
                 claim_snapshot=item)
    terminal = json.loads(queue.records.item_path("failed", key).read_bytes())
    assert "container_cleanup" not in terminal
    assert events.count("remove") == 1
    context = {}
    row = pbwait._from_ending(queue, (queue.records.item_path("failed", key), terminal),
                              context=context)
    projected = pbwait.consumer_ending_row(queue, row, cas=None, context=context)
    assert projected["diagnostics"]["cleanup"]["complete"] is True

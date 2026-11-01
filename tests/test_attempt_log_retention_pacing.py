"""Scratch-only rollout budgets and continuation for immutable attempt logs."""
import json
import shutil

import pytest

import pb_gc
from prismabuild import attempt_archive as archive, attempt_retention as retention
from prismabuild import immutable_files as native
from attempt_archive_fixture import AttemptArchiveFixture


@pytest.fixture
def fleet(tmp_path, monkeypatch):
    assert shutil.which("zstd"), "zstd is required to qualify log retirement"
    return AttemptArchiveFixture(tmp_path, monkeypatch)


def run_paced_cli(fleet, *flags):
    return pb_gc.main(["--queue-root", str(fleet.queue.root), "--summary", "--apply",
                       "--all-lock-takers-verify", "--retire-attempt-logs", *flags])


def test_cli_key_limit_bounds_each_survey_and_continues(fleet):
    records = [fleet.finish(f"{number:064x}") for number in range(1, 4)]
    for expected in range(1, 3):
        assert run_paced_cli(fleet, "--attempt-log-max-keys", "1") == 0
        for index, record in enumerate(records):
            for fact in fleet.descriptors(record).values():
                assert (fleet.queue.root / fact["path"]).exists() == (index >= expected)
            assert fleet.queue.records.item_path("done", record["action_key"]).exists()


@pytest.mark.parametrize("first_key", ["0" * 64, "e" * 64])
@pytest.mark.parametrize("wrapped", [False, True])
def test_byte_budget_has_no_overshoot_and_next_run_resurveys_partial_key(
        fleet, capsys, first_key, wrapped):
    records = [fleet.finish(f"{int(first_key, 16) + offset:064x}", stdout="aaaa", stderr="bbbb")
               for offset in range(2)]
    if wrapped:
        prior = fleet.plan()
        assert retention.checkpoint_attempt_log_survey(
            fleet.queue.root, {**prior, "next_cursor": "f" * 64})
    plan = fleet.plan()
    assert plan["wrapped"] == wrapped
    logs = [fact for candidate in plan["candidates"] for fact in candidate["logs"]]
    owners = {fact["path"]: candidate["key"]
              for candidate in plan["candidates"] for fact in candidate["logs"]}
    before = {fact["path"]: archive.resolve_attempt_log(fleet.queue.root, fact) for fact in logs}
    for index in range(4):
        assert run_paced_cli(fleet, "--attempt-log-max-bytes", "5") == 0
        output = capsys.readouterr().out
        assert "bytes_retired=4" in output
        assert f"stopped_on_budget={index < 3}" in output
        for position, fact in enumerate(logs):
            assert (fleet.queue.root / fact["path"]).exists() == (position > index)
            assert archive.resolve_attempt_log(fleet.queue.root, fact) == before[fact["path"]]
        remaining = fleet.plan()["candidates"]
        if index < 3:
            assert remaining[0]["key"] == owners[logs[index + 1]["path"]]
    for record in records:
        assert fleet.queue.records.item_path("done", record["action_key"]).exists()
        assert fleet.queue.records.attempt_path(record, 1).exists()
    receipts = [json.loads(path.read_bytes())
                for path in (fleet.queue.root / "gc-receipts").glob("attempt-logs-*.json")]
    assert len(receipts) == 4 and all(row["bytes_retired"] == 4 for row in receipts)


@pytest.mark.parametrize("budget,removed,stopped", [(0, 0, True), (3, 0, True), (4, 1, True), (8, 2, False)])
def test_descriptor_byte_budget_stops_before_the_next_unlink(fleet, monkeypatch, budget, removed, stopped):
    fleet.finish(stdout="aaaa", stderr="bbbb")
    original = native.unlink_nofollow
    attempted = []

    def bounded_unlink(path, **kwargs):
        attempted.append(path.stat().st_size)
        assert sum(attempted) <= budget
        return original(path, **kwargs)

    monkeypatch.setattr(native, "unlink_nofollow", bounded_unlink)
    result = retention.apply_attempt_log_retention(fleet.queue, fleet.plan(), max_bytes=budget)
    assert len(result["removed"]) == removed
    assert len(attempted) == removed
    assert result["bytes_retired"] == 4 * removed <= budget
    assert result["stopped_on_budget"] is stopped


def test_empty_payloads_fit_a_zero_byte_budget(fleet):
    fleet.finish(stdout="", stderr="")
    result = retention.apply_attempt_log_retention(fleet.queue, fleet.plan(), max_bytes=0)
    assert len(result["removed"]) == 2
    assert result["bytes_retired"] == 0 and not result["stopped_on_budget"]


def test_skipped_descriptor_does_not_consume_retirement_budget(fleet, monkeypatch):
    fleet.finish(stdout="aaaa", stderr="bbbb")
    plan = fleet.plan()
    first, second = plan["candidates"][0]["logs"]
    original = native.unlink_nofollow

    def refuse_first(path, **kwargs):
        if path == fleet.queue.root / first["path"]:
            raise native.ReplacedRecordError("injected source replacement")
        return original(path, **kwargs)

    monkeypatch.setattr(native, "unlink_nofollow", refuse_first)
    result = retention.apply_attempt_log_retention(fleet.queue, plan, max_bytes=4)
    assert result["removed"] == [{"path": second["path"], "bytes": 4}]
    assert result["bytes_retired"] == 4 and not result["stopped_on_budget"]
    assert (fleet.queue.root / first["path"]).exists()


def test_uncertain_post_unlink_error_keeps_its_byte_reservation(fleet, monkeypatch):
    record = fleet.finish(stdout="aaaa", stderr="bbbb")
    plan = fleet.plan()
    first, second = plan["candidates"][0]["logs"]
    original = native.unlink_nofollow

    def unlink_then_lose_custody(path, **kwargs):
        original(path, **kwargs)
        raise native.CASTamperError("injected parent custody error after unlink")

    monkeypatch.setattr(native, "unlink_nofollow", unlink_then_lose_custody)
    result = retention.apply_attempt_log_retention(fleet.queue, plan, max_bytes=4)
    assert not (fleet.queue.root / first["path"]).exists()
    assert (fleet.queue.root / second["path"]).exists()
    assert result["stopped_on_budget"] and not result["removed"]
    assert result["bytes_retired"] == 0
    for fact in fleet.descriptors(record).values():
        assert len(archive.resolve_attempt_log(fleet.queue.root, fact)) == 4


@pytest.mark.parametrize("budget", [-1, True, 1.5])
def test_invalid_python_byte_budget_refuses_before_apply(fleet, budget):
    fleet.finish()
    with pytest.raises(retention.AttemptRetentionRefused, match="byte.*nonnegative integer"):
        retention.apply_attempt_log_retention(fleet.queue, fleet.plan(), max_bytes=budget)


@pytest.mark.parametrize("flag,value", [("--attempt-log-max-keys", "0"),
                                         ("--attempt-log-max-keys", "257"),
                                         ("--attempt-log-max-bytes", "-1")])
def test_invalid_cli_budget_refuses_before_survey(tmp_path, monkeypatch, flag, value):
    def forbidden(*args, **kwargs):
        raise AssertionError("invalid rollout bounds must refuse before the queue survey")

    monkeypatch.setattr(pb_gc, "survey_queue", forbidden)
    queue = tmp_path / "absent-queue"
    assert pb_gc.main(["--queue-root", str(queue), flag, value]) == 2
    assert not queue.exists()


def test_rollout_flag_help_and_unchanged_defaults(capsys):
    parser = pb_gc.build_parser()
    args = parser.parse_args(["--queue-root", "scratch"])
    assert args.attempt_log_max_keys == retention.MAX_RETENTION_KEYS == 256
    assert args.attempt_log_max_bytes is None
    with pytest.raises(SystemExit) as ended:
        pb_gc.main(["--help"])
    assert ended.value.code == 0
    help_text = capsys.readouterr().out
    assert "--attempt-log-max-keys" in help_text and "--attempt-log-max-bytes" in help_text
    for action in parser._actions:
        if action.dest in {"attempt_log_max_keys", "attempt_log_max_bytes"}:
            assert action.help and action.help != "==SUPPRESS=="

"""Default supervision follows activity and silence on the execution clock."""

from test_progress_keeps_a_working_action_alive import _virtual_execution, _running_until
import pytest


def test_default_output_outlives_the_former_worker_budget(tmp_path):
    def script(child):
        for tick in range(1, 10):
            child.stdout += "committed a unit\n"
            yield tick * 1000.0

    with _virtual_execution(tmp_path, script, policy=None, ceiling=7200.0,
                            heartbeat_s=1000.0) as child:
        outcome = child.execute()
        assert child.stopped_at is None
    assert outcome["status"] == "executed"


def test_default_silence_ends_with_a_stall_verdict(tmp_path):
    with _virtual_execution(tmp_path, _running_until(9000.0), policy=None,
                            ceiling=7200.0, heartbeat_s=1000.0) as child:
        outcome = child.execute()
        assert child.stopped_at == 7200.0
    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"


@pytest.mark.parametrize("semantic", [False, True])
def test_running_supervision_is_logged_before_the_payload_finishes(tmp_path, semantic):
    from test_progress_keeps_a_working_action_alive import _policy

    def script(child):
        if semantic:
            child.publish(1, phase="startup")
        else:
            child.stdout += "committed a unit\n"
        yield 1.0
        assert child.poll() is None
        events = child.queue.consumer_events(child.item["action_key"])
        logged = [row for row in events if row.get("event") == "payload-supervision"]
        assert logged
        event = logged[-1]
        assert event["action_key"] == child.item["action_key"]
        assert event["claimed_by"] == child.item["claimed_by"]
        assert event["published_unix"] == child.item["published_unix"]
        assert event["age_s"] == 1.0
        progress = event["progress_observation"]
        assert progress["phase"] == ("startup" if semantic else "activity")
        assert progress["last_accepted"]["units_completed"] == 1
        assert progress["quiet_s"] == 0.0

    with _virtual_execution(tmp_path, script, policy=_policy(10.0) if semantic else None,
                            ceiling=10.0, heartbeat_s=1.0) as child:
        assert child.execute()["status"] == "executed"


def test_supervision_logs_are_bounded_and_separate_claim_writers(tmp_path):
    from prismabuild import pool

    queue = pool.PoolQueue(tmp_path / "queue")
    claim = {"action_key": "a" * 64, "published_unix": 0.0,
             "claimed_unix": 1.0, "claimed_by": "first", "attempts": 0}
    for number in range(5):
        queue.records.append_supervision_event(claim, {"number": number}, limit=2)
    successor = {**claim, "claimed_by": "second"}
    queue.records.append_supervision_event(successor, {"number": 99}, limit=2)
    assert sorted(row["number"] for row in queue.consumer_events(claim["action_key"])) == [3, 4, 99]


@pytest.mark.parametrize("missing", ["claimed_unix", "published_unix", "attempts"])
def test_legacy_heartbeat_keeps_observations_without_inventing_an_exact_event(tmp_path, missing):
    import json
    from prismabuild import pool

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    key = "a" * 64
    claim = {"action_key": key, "claimed_by": "legacy", "claimed_host": "compat-host",
             "claimed_unix": 10.0, "published_unix": 1.0, "attempts": 0}
    del claim[missing]
    queue.records.replace_record_at(queue.records.item_path(pool.CLAIMED, key), claim)
    observation = {"phase": "activity", "quiet_s": 0.0}
    queue.write_lease(key, owner="legacy", progress_observation=observation)
    lease = json.loads(queue.records.lease_path(key).read_bytes())
    assert lease["progress_observation"] == observation
    assert missing not in lease
    assert queue.consumer_events(key) == []

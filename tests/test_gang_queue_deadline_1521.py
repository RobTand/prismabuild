"""Only an explicitly opted-in, still-unclaimed gang has a queue-wait deadline."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from prismabuild import _gang, pool

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools/fleet"))
import pbgang  # noqa: E402
import pbrun  # noqa: E402


def test_the_explicit_queue_wait_option_reaches_both_submission_parsers(tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"queue_wait_timeout_s": 45,
                               "members": [{"tag": host, "argv": ["/bin/true"]}
                                           for host in ("sparklina", "sparky")]}))
    manifest = pbgang.load(path)
    command = pbgang.member_command(SimpleNamespace(cwd=tmp_path, queue_wait_timeout_s=None),
                                    manifest, manifest["members"][0], group="a" * 32, index=0)
    parsed = pbrun.parse_args(command[2:])
    assert parsed.gang_queue_wait_timeout_s == 45
    assert pbrun.gang_declaration(parsed) == {"group": "a" * 32, "size": 2, "index": 0}


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf"])
def test_a_bad_queue_wait_duration_is_refused_by_name(value):
    args = pbrun.parse_args(["--gang-group", "a" * 32, "--gang-size", "2", "--gang-index", "0",
                            "--gang-queue-wait-timeout-s", value, "--", "/bin/true"])
    with pytest.raises(SystemExit, match="queue.*positive finite"):
        pbrun.gang_declaration(args)


def test_the_queue_wait_option_cannot_apply_to_non_gang_work():
    args = pbrun.parse_args(["--gang-queue-wait-timeout-s", "45", "--", "/bin/true"])
    with pytest.raises(SystemExit, match="requires.*gang"):
        pbrun.gang_declaration(args)


def test_expiry_cancels_only_the_opted_in_unclaimed_gang(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    own, own_keys = members("own-deadline", priority=10)
    other, other_keys = members("other-no-deadline", priority=10)
    ordinary = publish("unrelated-row", priority=0, gpu=0, mem_gb=1)
    record = _gang.read_group(queue, own)
    record["queue_wait_deadline_unix"] = clock[0] + 45
    pool._write_json_atomic(_gang.group_path(queue, own), record)
    clock[0] += 46
    queue.sweep_gangs()
    assert all(queue.item_path(pool.WITHDRAWN, key).exists() for key in own_keys), (
        "an expired opt-in queue deadline did not cancel its own gang")
    assert all(queue.item_path(pool.READY, key).exists() for key in other_keys)
    assert queue.item_path(pool.READY, ordinary).exists()
    assert _gang.teardown(queue, other) is None


def test_a_first_claim_ends_queue_wait_and_is_not_canceled(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members("already-admitted", priority=10)
    assert claim("sparklina") is None
    assert claim("sparky") == keys[1]
    record = _gang.read_group(queue, group)
    record["queue_wait_deadline_unix"] = clock[0] - 1
    pool._write_json_atomic(_gang.group_path(queue, group), record)
    queue.sweep_gangs()
    assert queue.item_path(pool.CLAIMED, keys[1]).exists()
    assert _gang.teardown(queue, group) is None
    assert claim("sparklina") == keys[0]



def test_the_opt_in_budget_is_preserved_from_request_to_group(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members("declared-deadline", queue_wait_timeout_s=45)
    rows = [pool._read_json(queue.item_path(pool.READY, key)) for key in keys]
    assert _gang.read_group(queue, group)["queue_wait_deadline_unix"] == (
        min(row["published_unix"] for row in rows) + 45)
    for row in rows:
        assert row["gang_queue_wait_timeout_s"] == 45
        request = pool._sealed_action_request(row["cas_root"], row["action_key"])
        assert request["params"]["gang_queue_wait_timeout_s"] == 45
    clock[0] += 46
    assert claim("sparklina") is None
    assert all(queue.item_path(pool.WITHDRAWN, key).exists() for key in keys)


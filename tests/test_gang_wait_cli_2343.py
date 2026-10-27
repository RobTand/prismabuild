"""The submitter's CLI allowance controls the registered gang's cancellation."""
from __future__ import annotations

import json
from pathlib import Path
import sys
from types import SimpleNamespace

from prismabuild import _gang, pool
from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbgang


def test_cli_deadline_overrides_manifest_and_cancels_registered_members(
        gang_fleet, tmp_path, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members("cli-wait", file_group=False)
    originals = [pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, key))
                 for key in keys]
    path = tmp_path / "gang.json"
    path.write_text(json.dumps({"queue_wait_timeout_s": 120, "members": [
        {"tag": host, "argv": ["/bin/true"]} for host in ("sparklina", "sparky")]}))
    replies = iter(keys)

    def submitted(command, **kwargs):
        return SimpleNamespace(returncode=0, stderr="", stdout=json.dumps(
            {"status": "submitted", "action_key": next(replies)}))

    monkeypatch.setattr(pbgang, "subprocess", SimpleNamespace(run=submitted))
    monkeypatch.setattr(pbgang, "secrets", SimpleNamespace(token_hex=lambda size: group))
    assert pbgang.main(["--manifest", str(path), "--cwd", str(tmp_path),
                        "--queue", str(queue.root), "--queue-wait-timeout-s", "60"]) == 0
    assert _gang.read_group(queue, group)["queue_wait_timeout_s"] == 60
    clock.now += 61
    queue.sweep_gangs()
    assert all(queue.withdrawal_covers(row) is not None for row in originals)

"""Read-only barrier progress is visible through status without a live fleet."""
from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "src"))

import pbmcp  # noqa: E402
import pbstatus  # noqa: E402
import upgrade_client as upgrade  # noqa: E402
from prismabuild import pool  # noqa: E402


def _runtime(tmp_path: Path):
    store = tmp_path / "runtime-generations"
    old, new = store / "old", store / "new"
    old.mkdir(parents=True)
    new.mkdir()
    repo = tmp_path / "repo"
    repo.symlink_to(old)
    return repo, old, new


def _intent(repo: Path, old: Path, new: Path):
    epoch = "a" * 32
    intent = {"schema": upgrade.ROLLOUT_INTENT_SCHEMA, "epoch": epoch,
              "from_generation": old.name, "to_generation": new.name,
              "roster": ["alpha", "beta"], "agent_sha256": "b" * 64,
              "coordinator_sha256": "c" * 64,
              "drain_policy": "wait", "armed_unix": 458.0,
              "armed_by": "status-test"}
    root = repo.parent / "rollout" / "epochs" / epoch
    root.mkdir(parents=True)
    (root / "intent.json").write_bytes(upgrade.canonical_json(intent))
    marker = upgrade.make_marker(intent, "drained", host="alpha",
                                 generation=old.name, posted_unix=459.0,
                                 active_scopes=0, rollout_protocol=1,
                                 drain_changed_unix=458.0)
    (root / "alpha.drained.json").write_bytes(upgrade.canonical_json(marker))


def test_summary_names_the_epoch_phase_and_only_missing_hosts(tmp_path):
    repo, old, new = _runtime(tmp_path)
    _intent(repo, old, new)

    summary = pbstatus.read_rollout_summary(repo)

    assert summary["state"] == "active"
    assert summary["epoch"] == "a" * 32
    assert summary["pending_phase"] == "drained"
    assert summary["outstanding_hosts"] == ["beta"]
    assert summary["live_generation"] == "old"


def test_no_epoch_is_normal_idle_and_mcp_reuses_the_summary(tmp_path):
    repo, _old, _new = _runtime(tmp_path)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()

    assert pbstatus.read_rollout_summary(repo) == {
        "state": "idle", "epoch": None, "pending_phase": None,
        "outstanding_hosts": [], "failed_hosts": []}
    runtime = pbmcp.Session(queue_root=queue.root, cas_root=tmp_path / "cas",
                            repo_link=repo).call("pb_runtime")
    assert runtime["rollout"]["state"] == "idle"

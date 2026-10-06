"""Every native pbtest action names the repository it tested (#1565).

Daily test-cost accounting cannot say which repository a native ``pbtest``
action tested, so the submission tags the row with the repository root it
seals, and the worker side retains that tag in the attempt, end and resource
metadata its consumers read.  The tag rides the queue row -- the carrier that
is not part of the sealed identity, the way ``priority_reason`` rides beside
``priority`` -- so the action key never moves with it.  A checkout no name
can be read from, and a row filed before the tag existed, both read
``unknown``: explicit, never blank, never guessed.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import subprocess
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet")]
from prismabuild import pool  # noqa: E402
import pbrun  # noqa: E402
import pbstatus  # noqa: E402

from test_pbrun_detach import (  # noqa: E402
    _checkout, _queue, _run_pbrun, _one_json_line)
from test_action_resource_profile import _claimed  # noqa: E402
from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402

UNKNOWN = "unknown"


def _row(queue: pool.PoolQueue, key: str) -> dict:
    return json.loads(queue.item_path(pool.READY, key).read_text(
        encoding="utf-8"))


def _admitted(tmp_path: Path) -> AdmittedQueueFixture:
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "pb-queue"),
        capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    queue.ensure_layout()
    return queue


def test_detached_submission_tags_the_row_with_the_sealed_repository(
        tmp_path, monkeypatch, capsys):
    work, queue = _checkout(tmp_path), _queue(tmp_path)
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach") == 0
    key = _one_json_line(capsys.readouterr())["action_key"]
    assert _row(queue, key)["tested_repository"] == "work"


def test_the_tag_stays_out_of_the_sealed_body(tmp_path, monkeypatch, capsys):
    work = _checkout(tmp_path)
    _queue(tmp_path)
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach") == 0
    key = _one_json_line(capsys.readouterr())["action_key"]
    requests = sorted((tmp_path / "cas" / "requests").rglob("*.json"))
    assert len(requests) == 1, f"one submission sealed {len(requests)} requests"
    sealed = json.loads(requests[0].read_text(encoding="utf-8"))
    assert sealed["action_key"] == key
    assert "tested_repository" not in json.dumps(sealed)


def test_same_inputs_seal_one_key_whatever_the_tag_says(tmp_path, monkeypatch):
    """Tagged, renamed and untagged publications of one freeze seal one action.

    One frozen template, sealed three times independently; only the queue
    row's tag varies (a name, omitted, another name).  The action keys match
    and the CAS request bytes filed for each match: the tag never enters the
    sealed body or its key.
    """

    work = _checkout(tmp_path)
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    template = pbrun.freeze_action_template(
        command=("/bin/bash", "-lc", "printf ok"), cwd=work, logical_cwd=".",
        demand={"cpu": 1, "mem_gb": 1}, placement={"required_tags": []},
        variables={"PATH": "/usr/bin:/bin"}, determinism="deterministic",
        retry_policy={"max_attempts": 3, "retry_safe": True},
        host_class=None, measurement=False, transport="pool",
        pool_measurement_class=False, data_manifest_path=None,
        checkout_snapshot_max_bytes=pbrun.CHECKOUT_SNAPSHOT_MAX_BYTES,
        snapshot_refs=(), exclusive=False, gpu_memory_gb=None,
        execution_timeout_s=None, progress=None, profile=None)
    cas = template["cas"]
    args = pbrun.parse_args(["--max-attempts", "1", "--", "/bin/true"])
    queue = _queue(tmp_path)
    seen = {}
    for tag in ("atlas", None, "boreal"):
        sealed = pbrun.seal_action_from_template(template)
        cas.publish_action_request(sealed)
        bodies = {
            path.read_bytes()
            for path in (tmp_path / "cas" / "requests").rglob("*.json")
            if json.loads(path.read_text(encoding="utf-8"))["action_key"]
            == sealed["action_key"]}
        assert len(bodies) == 1, "one seal files one request body"
        row = pbrun.publication_row(
            sealed, args=args, queue=queue, tested_repository=tag)
        filed = json.loads(queue.publish(**row).read_text(encoding="utf-8"))
        seen[tag] = (sealed["action_key"], next(iter(bodies)), filed)
    assert len({entry[0] for entry in seen.values()}) == 1
    assert len({entry[1] for entry in seen.values()}) == 1
    assert seen["atlas"][2]["tested_repository"] == "atlas"
    assert "tested_repository" not in seen[None][2]
    assert seen["boreal"][2]["tested_repository"] == "boreal"
    assert seen["atlas"][2]["action_key"] == seen[None][2]["action_key"]


def test_repository_naming_uses_the_sealed_root_or_unknown(tmp_path):
    assert pbrun.tested_repository_name(_checkout(tmp_path)) == "work"
    plain = tmp_path / "plain"
    plain.mkdir()
    assert pbrun.tested_repository_name(plain) == UNKNOWN


def test_an_explicit_unknown_is_filed_not_dropped(tmp_path):
    queue = _queue(tmp_path)
    path = queue.publish(
        action_key="b" * 64, cas_root=tmp_path / "cas",
        worker_script="/worker.py", checkout_root=str(tmp_path),
        tested_repository=UNKNOWN)
    assert json.loads(path.read_text(encoding="utf-8"))[
        "tested_repository"] == UNKNOWN


@pytest.mark.parametrize(
    "value",
    [17, "", "  ", "bad\x00name", "line\nline", "x" * 257],
    ids=["integer", "empty", "blank", "nul", "multiline", "too-long"])
def test_bad_repository_names_are_refused_before_queue_evidence_changes(
        tmp_path, value):
    queue = _queue(tmp_path)
    before = {path for path in queue.root.rglob("*.json")}
    with pytest.raises(pool.PoolContractError):
        queue.publish(
            action_key="c" * 64, cas_root=tmp_path / "cas",
            worker_script="/worker.py", checkout_root=str(tmp_path),
            tested_repository=value)
    assert {path for path in queue.root.rglob("*.json")} == before


def test_attempt_and_end_carry_the_repository(tmp_path):
    queue = _admitted(tmp_path)
    key = "d" * 64
    queue.publish(action_key=key, cas_root=tmp_path / "cas",
                  worker_script="/worker.py", checkout_root="/co",
                  tested_repository="atlas")
    claimed = queue.claim()
    assert claimed is not None
    assert claimed["tested_repository"] == "atlas"
    assert pool.tested_repository_of(claimed) == "atlas"
    queue.finish(key, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)
    done = pool._read_json(queue.item_path(pool.DONE, key))
    assert done["tested_repository"] == "atlas"
    attempt = pool._read_json(queue.attempt_path(done, 1))
    assert attempt["schema"] == pool.POOL_ATTEMPT_SCHEMA_V1
    assert attempt["tested_repository"] == "atlas"


def test_a_row_without_the_field_reads_unknown_on_attempt_and_end(
        tmp_path):
    queue = _admitted(tmp_path)
    key = "e" * 64
    queue.publish(action_key=key, cas_root=tmp_path / "cas",
                  worker_script="/worker.py", checkout_root="/co")
    assert pool.tested_repository_of(
        json.loads(queue.item_path(pool.READY, key).read_text(
            encoding="utf-8"))) == UNKNOWN
    claimed = queue.claim()
    assert claimed is not None
    queue.finish(key, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)
    done = pool._read_json(queue.item_path(pool.DONE, key))
    assert "tested_repository" not in done
    attempt = pool._read_json(queue.attempt_path(done, 1))
    assert attempt["tested_repository"] == UNKNOWN


def test_requeue_arguments_carry_the_repository_forward(tmp_path):
    queue = _queue(tmp_path)
    record = {"action_key": "f" * 64, "cas_root": "/cas",
              "worker_script": "/w.py", "checkout_root": "/co",
              "tested_repository": "atlas"}
    carried = queue._requeue_arguments(record, action_key="f" * 64)
    assert carried is not None
    assert carried["tested_repository"] == "atlas"
    dropped = queue._requeue_arguments(
        {name: value for name, value in record.items()
         if name != "tested_repository"}, action_key="f" * 64)
    assert dropped is not None
    assert "tested_repository" not in dropped


def test_resource_profile_names_the_repository_it_measured(tmp_path):
    action, _, _ = _claimed(tmp_path, "open('result', 'w').write('ok')\n")
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "pb-queue2"),
        capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    queue.publish(action_key=action["action_key"], cas_root=tmp_path / "cas",
                  checkout_root=tmp_path / "checkout",
                  worker_script=ROOT / "tools" / "prismabuild_worker.py",
                  tested_repository="atlas")
    outcome = queue.execute(queue.claim())
    assert outcome["status"] == "executed", outcome.get("stderr")
    assert outcome["resource_profile"]["tested_repository"] == "atlas"


def test_resource_profile_without_a_named_checkout_says_unknown(tmp_path):
    _, queue, item = _claimed(tmp_path, "open('result', 'w').write('ok')\n")
    outcome = queue.execute(item)
    assert outcome["status"] == "executed", outcome.get("stderr")
    assert outcome["resource_profile"]["tested_repository"] == UNKNOWN


def test_repository_visible_in_status_reads(tmp_path):
    queue = _admitted(tmp_path)
    key = "9" * 64
    queue.publish(action_key=key, cas_root=tmp_path / "cas",
                  worker_script="/worker.py", checkout_root="/co",
                  tested_repository="atlas")
    assert queue.claim() is not None
    live = pbstatus.read_pool(queue.root)
    assert live["jobs"][0]["tested_repository"] == "atlas"
    claimed = json.loads(queue.item_path(pool.CLAIMED, key).read_text(
        encoding="utf-8"))
    queue.finish(key, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)
    entries = [entry for entry in os.scandir(queue.dir(pool.DONE))
               if entry.name == f"{key}.json"]
    assert len(entries) == 1
    assert pbstatus.ending_row(entries[0], queue.root)[
        "tested_repository"] == "atlas"


def test_linked_worktree_reports_its_main_repository(tmp_path):
    main = tmp_path / "main"
    main.mkdir()
    (main / "seed.txt").write_text("sealed\n", encoding="utf-8")
    for args in (
        ("init", "-q", "-b", "main"),
        ("config", "user.email", "test@example.invalid"),
        ("config", "user.name", "PrismaBuild test"),
        ("add", "seed.txt"),
        ("commit", "-qm", "sealed tree"),
    ):
        done = subprocess.run(
            ["git", "-C", str(main), *args],
            capture_output=True, text=True)
        assert done.returncode == 0, done.stderr
    linked = tmp_path / "linked"
    done = subprocess.run(
        ["git", "-C", str(main), "worktree", "add", str(linked)],
        capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    assert pbrun.tested_repository_name(linked) == "main"
    assert pbrun.tested_repository_name(main) == "main"


def test_worktree_of_a_bare_origin_names_the_bare_stem(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "seed.txt").write_text("sealed\n", encoding="utf-8")
    for args in (
        ("init", "-q", "-b", "main"),
        ("config", "user.email", "test@example.invalid"),
        ("config", "user.name", "PrismaBuild test"),
        ("add", "seed.txt"),
        ("commit", "-qm", "sealed tree"),
    ):
        done = subprocess.run(
            ["git", "-C", str(src), *args],
            capture_output=True, text=True)
        assert done.returncode == 0, done.stderr
    done = subprocess.run(
        ["git", "clone", "-q", "--bare", str(src), str(tmp_path / "blue.git")],
        capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    wt = tmp_path / "wt-blue"
    done = subprocess.run(
        ["git", "--git-dir", str(tmp_path / "blue.git"),
         "worktree", "add", str(wt), "main"],
        capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    assert pbrun.tested_repository_name(wt) == "blue"

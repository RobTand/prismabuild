"""Review-round regressions for vehicle admission, recovery and shadow cleanup (#2295)."""

from __future__ import annotations

import json
import subprocess

import pytest

from test_pbmergeq import FakeGitHub, config, entries, git, make_queue, mq


@pytest.fixture
def issue_api(monkeypatch):
    original = mq.call_tool
    answers = {}

    def request(command, **kwargs):
        if command[:2] == ["gh", "api"] and "/issues/" in command[2]:
            number = int(command[2].rsplit("/", 1)[1])
            status, payload = answers.get(number, (0, {"number": number}))
            result = subprocess.CompletedProcess(command, status, json.dumps(payload), "")
            if status and kwargs.get("check", True):
                raise subprocess.CalledProcessError(status, command, result.stdout, "not found")
            return result
        return original(command, **kwargs)

    monkeypatch.setattr(mq, "call_tool", request)
    monkeypatch.setattr(FakeGitHub, "issue", mq.GitHub.issue)
    return answers


@pytest.mark.parametrize("reference", ["absent", "nonexistent", "pull-request"])
def test_member_without_a_real_issue_is_refused_before_testing_or_push(
        tmp_path, monkeypatch, issue_api, reference):
    queue, origin, github, runner = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    view = github.pr
    metadata = {"body": "", "closingIssuesReferences": []} if reference == "absent" else {}
    monkeypatch.setattr(github, "pr", lambda number: {**view(number), **metadata})
    if reference == "nonexistent":
        issue_api[101] = (1, {"message": "Not Found"})
    elif reference == "pull-request":
        issue_api[101] = (0, {"number": 101, "pull_request": {"url": "a pull request"}})
    pushes = []
    monkeypatch.setattr(queue.mirror, "push_candidate", lambda *args: pushes.append(args))

    batch = queue.run_batch(entries(github, 1))

    assert batch["verdict"] == "empty", batch
    assert runner.runs == [] and pushes == []
    assert origin.vehicle_creations == []
    assert "issue" in batch["skipped"][0]["why"].lower()
    assert queue.store.state["batch"] is None


def test_failed_vehicle_issue_link_ends_the_batch_and_charges_members(tmp_path, issue_api):
    queue, origin, github, _ = make_queue(tmp_path, mode="merge")
    origin.pr(1, {"one.txt": "1\n"})
    github.issue_link_state = "failure"
    approved = [{**entry, "attempts": 1} for entry in entries(github, 1)]

    batch = queue.run_batch(approved)

    assert batch["verdict"] != "waiting", batch
    assert "issue-link" in batch["summary"]
    assert github.pr(batch["vehicle"]["number"])["state"] == "CLOSED"
    assert queue.store.state["batch"] is None
    assert [(p["pr"], p["sha"], p["attempts"]) for p in queue.store.state["queue"]] == [
        (1, approved[0]["sha"], 2)]
    assert github.merged == []


@pytest.mark.parametrize("cause", [
    "pending-check", "candidate-status", "member-status", "merge-refused",
    "member-unobserved", "comment-refused", "close-refused", "exception",
])
def test_every_green_wait_releases_the_queue_after_a_persisted_allowance(
        tmp_path, monkeypatch, issue_api, cause):
    clock = [1000.0]
    monkeypatch.setattr(mq.time, "time", lambda: clock[0])
    queue, origin, github, runner = make_queue(tmp_path, mode="merge")
    origin.pr(1, {"one.txt": "1\n"})
    approved = entries(github, 1)
    if cause == "pending-check":
        github.issue_link_state = "pending"
    elif cause in {"candidate-status", "member-status"}:
        post = github.post_status

        def refuse(batch, sha, state, description, *, context=None):
            if (context is None) == (cause == "candidate-status"):
                return False
            return post(batch, sha, state, description, context=context)

        monkeypatch.setattr(github, "post_status", refuse)
    elif cause == "merge-refused":
        monkeypatch.setattr(github, "merge", lambda *args: False)
    elif cause == "member-unobserved":
        view = github.pr
        monkeypatch.setattr(github, "pr", lambda number: {
            **view(number), **({"state": "OPEN"} if number == 1 else {})})
    elif cause in {"comment-refused", "close-refused"}:
        run = runner.run_many

        def change_after_test(*args):
            results = run(*args)
            if cause == "close-refused":
                origin.advance_main()
            else:
                view = github.pr
                monkeypatch.setattr(github, "pr", lambda number: {
                    **view(number), **({"headRefOid": "a" * 40} if number == 1 else {})})
            return results

        monkeypatch.setattr(runner, "run_many", change_after_test)
        monkeypatch.setattr(github, "head_moved", lambda *args: False)
        monkeypatch.setattr(github, "close_vehicle", lambda *args: False)
    else:
        def unavailable(*args):
            raise RuntimeError("GitHub unavailable after push")
        monkeypatch.setattr(github, "find_vehicle", unavailable)

    pending = queue.run_batch(approved)
    assert pending["verdict"] == "waiting", pending
    completed = list(runner.runs)
    # Reconstruct the durable state, then advance only the injected clock.
    store = mq.Store(queue.cfg.state_dir)
    github.store = store
    resumed = mq.Queue(queue.cfg, store, github, queue.mirror, runner, "merge")
    resumed.resume()
    clock[0] += 86400

    ended = resumed.tick()

    assert ended["verdict"] != "waiting", ended
    assert store.state["batch"] is None
    assert runner.runs == completed
    if cause == "member-unobserved":
        assert git("rev-parse", "main", cwd=origin.work) != pending["base"]
        assert store.state["queue"] == [], "already-integrated members must not be retried as failures"


def test_once_wait_allowance_survives_repeated_cli_recovery(
        tmp_path, monkeypatch, issue_api):
    clock = [1000.0]
    monkeypatch.setattr(mq.time, "time", lambda: clock[0])
    queue, origin, github, runner = make_queue(tmp_path)
    queue.once = True
    origin.pr(1, {"one.txt": "1\n"})
    monkeypatch.setattr(github, "post_status", lambda *args, **kwargs: False)
    monkeypatch.setattr(mq, "open_queue", lambda *args, **kwargs: queue)
    command = ["--config", str(tmp_path / "config.json"), "once", "--mode", "status", "1"]

    assert mq.main(command) == 0
    assert queue.store.state["batch"]["verdict"] == "waiting"
    completed = list(runner.runs)
    clock[0] += 86400
    assert mq.main(command) == 0

    assert queue.store.state["batch"] is None
    assert queue.store.state["queue"] == []
    assert runner.runs == completed
    assert len(origin.vehicle_creations) == 1


class ShadowGitHub(FakeGitHub):
    def create_vehicle(self, branch, title, body, draft=False):
        vehicle = super().create_vehicle(branch, title, body, draft=draft)
        self.origin.vehicles[branch]["isDraft"] = draft
        return {**vehicle, "isDraft": draft}

    def close_vehicle(self, batch, number):
        # Exercise the production adapter, including its branch deletion flag.
        return mq.GitHub.close_vehicle(self, batch, number)

    def _write(self, what, command, stdin=None):
        accepted = super()._write(what, command, stdin)
        if accepted and command[:3] == ["gh", "pr", "close"]:
            vehicle = next(v for v in self.origin.vehicles.values() if v["number"] == int(command[3]))
            assert vehicle["isDraft"], "a shadow vehicle became mergeable before closure"
            vehicle["state"] = "CLOSED"
            if "--delete-branch" in command:
                git("branch", "-D", vehicle["headRefName"], cwd=self.origin.work)
        return accepted


def test_status_vehicle_is_draft_then_closed_with_its_branch_deleted(tmp_path, issue_api):
    queue, origin, _, _ = make_queue(tmp_path)
    github = ShadowGitHub(queue.cfg, queue.store, origin)
    queue.github = github
    origin.pr(1, {"one.txt": "1\n"})
    queue.once = True

    batch = queue.run_batch([{"pr": 1, "sha": None, "at": "now"}])

    assert batch["verdict"] == "green", batch
    vehicle = github.pr(batch["vehicle"]["number"])
    assert vehicle["isDraft"] is True
    assert vehicle["state"] == "CLOSED"
    branches = git("for-each-ref", "--format=%(refname)", "refs/heads/release-mq/", cwd=origin.work)
    assert branches == ""
    assert queue.store.state["batch"] is None
    assert github.merged == []


def test_unpinned_once_member_movement_does_not_claim_review_approval(
        tmp_path, monkeypatch, issue_api):
    queue, origin, github, runner = make_queue(tmp_path)
    queue.once = True
    origin.pr(1, {"one.txt": "1\n"})
    run = runner.run_many

    def change_after_test(*args):
        results = run(*args)
        view = github.pr
        monkeypatch.setattr(github, "pr", lambda number: {
            **view(number), **({"headRefOid": "a" * 40} if number == 1 else {})})
        return results

    monkeypatch.setattr(runner, "run_many", change_after_test)

    batch = queue.run_batch([{"pr": 1, "sha": None, "at": "now"}])

    assert batch["verdict"] != "merged"
    assert github.pr(batch["vehicle"]["number"])["state"] == "CLOSED"
    assert not any("after approval" in body for _, body in github.comments)


@pytest.mark.parametrize("allowance", [0, -1, float("inf"), float("nan")])
def test_green_wait_allowance_must_be_positive_and_finite(tmp_path, allowance):
    with pytest.raises(ValueError, match="green_wait_s"):
        config(tmp_path, green_wait_s=allowance)


def test_terminal_retry_is_saved_before_close_and_recovers_branch_deletion(
        tmp_path, monkeypatch, issue_api):
    clock = [1000.0]
    monkeypatch.setattr(mq.time, "time", lambda: clock[0])
    queue, origin, github, runner = make_queue(tmp_path, mode="merge")
    origin.pr(1, {"one.txt": "1\n"})
    github.issue_link_state = "failure"
    close = github.close_vehicle

    def close_then_crash(*args):
        close(*args)
        raise SystemExit("crash after remote close")

    monkeypatch.setattr(github, "close_vehicle", close_then_crash)
    with pytest.raises(SystemExit, match="crash after remote close"):
        queue.run_batch(entries(github, 1))

    store = mq.Store(queue.cfg.state_dir)
    assert store.state["batch"] is None
    assert [p["attempts"] for p in store.state["queue"]] == [1]
    assert len(store.state["vehicle_cleanup"]) == 1
    completed = list(runner.runs)
    clock[0] += queue.cfg.stuck_backoff_s
    github = FakeGitHub(queue.cfg, store, origin)
    resumed = mq.Queue(queue.cfg, store, github, queue.mirror, runner, "merge")
    def already_closed(*args):
        raise AssertionError("a closed vehicle needs branch deletion, not another close")
    monkeypatch.setattr(github, "close_vehicle", already_closed)
    resumed.cleanup_vehicles()

    assert store.state["vehicle_cleanup"] == {}
    assert [p["attempts"] for p in store.state["queue"]] == [1]
    assert git("for-each-ref", "--format=%(refname)", "refs/heads/release-mq/", cwd=origin.work) == ""
    assert runner.runs == completed


def test_exhausted_cleanup_does_not_block_another_batch(tmp_path, monkeypatch, issue_api):
    clock = [1000.0]
    monkeypatch.setattr(mq.time, "time", lambda: clock[0])
    queue, origin, github, _ = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    monkeypatch.setattr(queue.mirror, "delete_candidate", lambda *args: False)
    first = queue.run_batch(entries(github, 1))
    for _ in range(queue.cfg.max_attempts + 1):
        clock[0] += queue.cfg.stuck_backoff_s
        queue.cleanup_vehicles()

    pending = queue.store.state["vehicle_cleanup"][first["id"]]
    assert pending["attempts"] == queue.cfg.max_attempts
    assert "branch deletion refused" in queue.store.status_text()
    origin.pr(2, {"two.txt": "2\n"})
    queue.store.add(2, github.pr(2)["headRefOid"], front=False)

    second = queue.tick()

    assert second["verdict"] == "green" and second["id"] != first["id"]
    assert queue.store.state["batch"] is None
    assert pending["attempts"] == queue.cfg.max_attempts


def test_resume_cannot_elevate_a_status_batch_to_merge_mode(tmp_path, monkeypatch, issue_api):
    queue, origin, github, runner = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    monkeypatch.setattr(github, "post_status", lambda *args, **kwargs: False)
    batch = queue.run_batch(entries(github, 1))
    resumed = mq.Queue(queue.cfg, queue.store, github, queue.mirror, runner, "merge")

    with pytest.raises(SystemExit, match="resume it in its original mode"):
        resumed.resume()

    assert github.pr(batch["vehicle"]["number"])["isDraft"]
    assert github.merged == []


def test_restart_converts_an_existing_status_vehicle_to_draft_before_success(
        tmp_path, monkeypatch, issue_api):
    queue, origin, github, runner = make_queue(tmp_path)
    origin.pr(1, {"one.txt": "1\n"})
    create = github.create_vehicle

    def old_ready_vehicle(*args, **kwargs):
        created = create(*args, **kwargs)
        origin.vehicles[created["headRefName"]]["isDraft"] = False
        raise SystemExit("old status vehicle needs recovery")

    monkeypatch.setattr(github, "create_vehicle", old_ready_vehicle)
    with pytest.raises(SystemExit, match="old status vehicle"):
        queue.run_batch(entries(github, 1))
    completed = list(runner.runs)
    write = github._write

    def require_draft_before_success(what, command, stdin=None):
        if command[:2] == ["gh", "api"]:
            assert next(iter(origin.vehicles.values()))["isDraft"]
        return write(what, command, stdin)

    monkeypatch.setattr(github, "_write", require_draft_before_success)
    queue.resume()
    ended = queue.tick()

    assert ended["verdict"] == "green"
    assert next(iter(origin.vehicles.values()))["state"] == "CLOSED"
    assert runner.runs == completed
    assert len(origin.vehicle_creations) == 1


def test_expired_green_allowance_takes_precedence_over_exception_backoff(
        tmp_path, monkeypatch, issue_api):
    clock = [1000.0]
    monkeypatch.setattr(mq.time, "time", lambda: clock[0])
    queue, origin, github, runner = make_queue(tmp_path, mode="merge")
    queue.cfg = mq.dataclasses.replace(queue.cfg, green_wait_s=1, stuck_backoff_s=600)
    origin.pr(1, {"one.txt": "1\n"})

    def unavailable(*args):
        raise RuntimeError("GitHub unavailable after push")

    monkeypatch.setattr(github, "find_vehicle", unavailable)
    pending = queue.run_batch(entries(github, 1))
    assert pending["verdict"] == "waiting"
    completed = list(runner.runs)
    clock[0] += 2

    ended = queue.tick()

    assert ended is not None and ended["verdict"] == "green-timeout"
    assert queue.store.state["batch"] is None
    assert runner.runs == completed

"""Consumer seal/publish verbs use fixture stores, never the live fleet."""
from __future__ import annotations

import json
import shutil
import socket

import pytest
import pbrun
from prismabuild import core as pb, digest_primitives, sealed_submission, submission
from prismabuild.submission_types import SubmissionRefused
from submission_support import patch_store
from test_pbrun_detach import _checkout, _queue, _one_json_line
from test_slurm_lane import fleet

__all__ = ["fleet"]


def test_consumer_seal_and_publish_flags_exist():
    options = pbrun.argument_parser()._option_string_actions
    assert "--seal-only" in options
    assert "--publish-sealed" in options


@pytest.fixture
def sealed_fleet(tmp_path, monkeypatch):
    work, queue = _checkout(tmp_path), _queue(tmp_path)
    store = patch_store(monkeypatch, root=tmp_path)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    return work, queue, store


def _consumer_argv(work, *options):
    return ["--cwd", str(work), *options, "--", "/bin/bash", "-c", "printf sealed-proof"]


def _seal_fixture(sealed_fleet, capsys, *options):
    work, queue, store = sealed_fleet
    assert pbrun.main(_consumer_argv(work, "--seal-only", *options)) == 0
    record = _one_json_line(capsys.readouterr())
    assert set(record) == {"schema", "action_key", "request_sha256", "transport"}
    assert record["schema"] == "prismabuild.sealed.v1"
    assert queue.ready_items() == []
    return record


def test_seal_then_publish_matches_direct_publication(sealed_fleet, monkeypatch, capsys):
    work, queue, store = sealed_fleet
    sealed = _seal_fixture(sealed_fleet, capsys, "--priority", "-7",
        "--priority-reason", "consumer proof", "--env", "EXAMPLE=sealed")
    action = pb.PrismaBuildCAS(store.cas_root).read_action_request(sealed["action_key"])
    assert sealed["request_sha256"] == digest_primitives.raw_sha256(pb._canonical_file_bytes(action))
    publications = []
    publish = submission.pool.PoolQueue.publish

    def observe_publication(self, **row):
        publications.append(row)
        return publish(self, **row)

    monkeypatch.setattr(submission.pool.PoolQueue, "publish", observe_publication)
    assert pbrun.main(_consumer_argv(work, "--detach", "--priority", "-7",
        "--priority-reason", "consumer proof", "--env", "EXAMPLE=sealed")) == 0
    direct = _one_json_line(capsys.readouterr())
    assert direct["schema"] == "prismabuild.submission.v1"
    assert direct["action_key"] == sealed["action_key"]
    assert direct["request_sha256"] == sealed["request_sha256"]
    queue.records.item_path("ready", sealed["action_key"]).unlink()
    assert pbrun.main(["--publish-sealed", sealed["action_key"], "--detach"]) == 0
    recovered = _one_json_line(capsys.readouterr())
    assert recovered["action_key"] == direct["action_key"]
    assert recovered["status"] == direct["status"] == "published"
    assert recovered["request_sha256"] == direct["request_sha256"]
    assert recovered["publication"] == dict(published_unix=recovered["published_unix"],
        generation=recovered["published_unix"])
    assert len(publications) == 2
    assert publications[0] == publications[1]


def test_publish_never_reprepares_and_attaches_idempotently(sealed_fleet, monkeypatch, capsys):
    work, queue, store = sealed_fleet
    sealed = _seal_fixture(sealed_fleet, capsys)
    shutil.rmtree(work)

    def checkout_work_forbidden(*args, **kwargs):
        pytest.fail("publish-sealed attempted checkout preparation")

    for name in ("prepare", "seal", "freeze_template", "snapshot_checkout", "write_deterministic_bundle"):
        monkeypatch.setattr(submission, name, checkout_work_forbidden)
    monkeypatch.setattr(submission.movement_actions, "checkout_identity", checkout_work_forbidden)
    assert pbrun.main(["--publish-sealed", sealed["action_key"], "--detach"]) == 0
    first = _one_json_line(capsys.readouterr())
    assert pbrun.main(["--publish-sealed", sealed["action_key"], "--detach"]) == 0
    second = _one_json_line(capsys.readouterr())
    assert second["status"] == "attached"
    assert second["publication"] == first["publication"]
    assert second["request_sha256"] == sealed["request_sha256"]
    assert len(queue.ready_items()) == 1


def test_a_verified_result_is_a_cache_hit(sealed_fleet, capsys):
    work, queue, store = sealed_fleet
    sealed = _seal_fixture(sealed_fleet, capsys)
    assert pbrun.main(["--publish-sealed", sealed["action_key"], "--detach"]) == 0
    _one_json_line(capsys.readouterr())
    outcome = queue.serve_once(tags=["sparky", "gb10"],
        python=submission.sys.executable, capacity={"cpu": 4, "mem_gb": 16, "gpu": 1}, timeout_s=60)
    assert outcome["status"] == "executed", outcome
    capsys.readouterr()
    assert pbrun.main(["--publish-sealed", sealed["action_key"], "--detach"]) == 0
    record = _one_json_line(capsys.readouterr())
    assert record["status"] == "cache_hit"
    assert record["request_sha256"] == sealed["request_sha256"]
    assert queue.ready_items() == []


@pytest.mark.parametrize("damage,code", [
    ("missing_request", "missing_sealed_request"),
    ("changed_request", "invalid_sealed_request"),
    ("missing_bundle", "sealed_input_unavailable"),
    ("changed_bundle", "sealed_input_unavailable"),
    ("context", "invalid_sealed_publication"),
])
def test_sealed_failures_are_named(sealed_fleet, capsys, damage, code):
    work, queue, store = sealed_fleet
    sealed = _seal_fixture(sealed_fleet, capsys)
    key = sealed["action_key"]
    request = store.cas_root / "requests" / key[:2] / f"{key}.json"
    if damage == "missing_request":
        request.unlink()
    elif damage == "changed_request":
        request.chmod(0o644)
        value = json.loads(request.read_text())
        value["params"]["command"] = ["changed"]
        request.write_text(json.dumps(value))
        request.chmod(0o444)
    elif damage == "context":
        path = sealed_submission.sealed_context_path(store, key)
        path.chmod(0o644)
        value = json.loads(path.read_text())
        value["request_sha256"] = "f" * 64
        path.write_text(json.dumps(value))
        path.chmod(0o444)
    else:
        action = pb.PrismaBuildCAS(store.cas_root).read_action_request(key)
        bundle = pb.PrismaBuildCAS(store.cas_root).input_path(action["inputs"][0])
        if damage == "missing_bundle":
            bundle.unlink()
        else:
            bundle.chmod(0o644)
            bundle.write_bytes(b"wrong bundle")
            bundle.chmod(0o444)
    assert pbrun.main(["--publish-sealed", key, "--detach"]) == 1
    record = _one_json_line(capsys.readouterr())
    assert record["schema"] == "prismabuild.submission.v1"
    assert record["refusal"]["code"] == code
    assert record["refusal"]["reason"]
    if "bundle" in damage:
        assert pb.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID in record["refusal"]["reason"]
    assert queue.ready_items() == []


def test_publication_rechecks_ordinary_gates(sealed_fleet, monkeypatch, capsys):
    work, queue, store = sealed_fleet
    sealed = _seal_fixture(sealed_fleet, capsys)
    calls = []

    def refuse_publication(args, action, **kwargs):
        calls.append(action["action_key"])
        raise SubmissionRefused("controlled publication gate")

    monkeypatch.setattr(submission.d38_gate, "judge_publication", refuse_publication)
    assert pbrun.main(["--publish-sealed", sealed["action_key"], "--detach"]) == 1
    assert calls == [sealed["action_key"]]
    assert _one_json_line(capsys.readouterr())["refusal"]["reason"] == "controlled publication gate"
    assert queue.ready_items() == []


def test_publication_rechecks_worker_offers(sealed_fleet, capsys):
    work, queue, store = sealed_fleet
    sealed = _seal_fixture(sealed_fleet, capsys)
    queue.announce(host="sparky", tags=["sparky", "gb10"], has_gpu=True,
        capacity={"cpu": 1, "mem_gb": 1, "gpu": 1})
    assert pbrun.main(["--publish-sealed", sealed["action_key"], "--detach"]) == 1
    record = _one_json_line(capsys.readouterr())
    assert "no recorded worker can run this action" in record["refusal"]["reason"]
    assert queue.ready_items() == []


def test_seal_refusal_has_schema_and_code(sealed_fleet, capsys):
    work, queue, store = sealed_fleet
    assert pbrun.main(_consumer_argv(work, "--seal-only", "--after", "a" * 64)) == 1
    record = _one_json_line(capsys.readouterr())
    assert record["schema"] == "prismabuild.sealed.v1"
    assert record["refusal"]["code"] == "deferred_seal_unsupported"
    assert queue.ready_items() == []


@pytest.mark.parametrize("options", [
    ["--publish-sealed", "bad"],
    ["--publish-sealed", "a" * 64, "--", "/bin/true"],
    ["--publish-sealed", "a" * 64, "--cpus", "2"],
    ["--seal-only", "--detach", "--", "/bin/true"],
])
def test_consumer_usage_errors_exit_two(options):
    with pytest.raises(SystemExit) as exc:
        pbrun.main(options)
    assert exc.value.code == 2


def test_staged_seal_files_no_window_until_publication(tmp_path, monkeypatch, capsys):
    from test_pbrun_residency_stage_submission import _prepare
    prepared = _prepare(tmp_path, monkeypatch)
    argv = submission.sys.argv[1:]
    argv[argv.index("--detach")] = "--seal-only"
    assert pbrun.main(argv) == 0
    sealed = _one_json_line(capsys.readouterr())
    queue = prepared["queue"]
    assert queue.ready_items() == []
    assert pbrun.main(["--publish-sealed", sealed["action_key"], "--detach"]) == 0
    published = _one_json_line(capsys.readouterr())
    row = json.loads(submission.Path(published["submission"]).read_text())
    assert row["residency"]["leads"]


def test_attached_publish_passes_wait_budget(sealed_fleet, monkeypatch, capsys):
    sealed = _seal_fixture(sealed_fleet, capsys)
    budgets = []

    def wait_for_publication(queue, key, *, wait_s, generation, report):
        budgets.append(wait_s)
        return report.finish(submission.action_client.ClientPatienceExpired(key, generation))

    monkeypatch.setattr(submission.action_client, "report_pool_outcome", wait_for_publication)
    assert pbrun.main(["--publish-sealed", sealed["action_key"], "--wait-s", "0.25"]) == 75
    assert budgets == [0.25]


def test_existing_cas_requests_do_not_require_new_context(sealed_fleet, capsys):
    sealed = _seal_fixture(sealed_fleet, capsys)
    store = sealed_fleet[2]
    sealed_submission.sealed_context_path(store, sealed["action_key"]).unlink()
    assert pbrun.main(["--publish-sealed", sealed["action_key"], "--detach"]) == 0
    assert _one_json_line(capsys.readouterr())["status"] == "published"


def test_seal_context_cannot_silently_change_publication_choices(sealed_fleet, capsys):
    work = sealed_fleet[0]
    _seal_fixture(sealed_fleet, capsys, "--priority", "1")
    assert pbrun.main(_consumer_argv(work, "--seal-only", "--priority", "2")) == 1
    assert _one_json_line(capsys.readouterr())["refusal"]["code"] == "sealed_publication_conflict"


def test_publication_keeps_the_interpreter_verdict(sealed_fleet, capsys):
    from prismabuild import capability_eligibility, contracts
    work, queue, store = sealed_fleet
    interpreter = submission.sys.executable
    argv = _consumer_argv(work, "--seal-only")
    argv[-3:] = [interpreter, "-c", "print('sealed interpreter')"]
    assert pbrun.main(argv) == 0
    sealed = _one_json_line(capsys.readouterr())
    queue.announce(host="sparky", tags=["sparky", "gb10", contracts.INTERPRETER_TAG],
        has_gpu=True, capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
        capabilities=capability_eligibility.OfferCapabilities(
            interpreters=capability_eligibility.InterpreterAnswers.from_observed([], [interpreter])))
    assert pbrun.main(["--publish-sealed", sealed["action_key"], "--detach"]) == 1
    reason = _one_json_line(capsys.readouterr())["refusal"]["reason"]
    assert interpreter in reason
    assert "every recorded eligible worker names it absent" in reason
    assert queue.ready_items() == []


def test_sealed_transport_survives_publication_defaults(sealed_fleet, monkeypatch, capsys, fleet):
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "PENDING")
    sealed = _seal_fixture(sealed_fleet, capsys, "--transport", "slurm")
    assert pbrun.main(["--publish-sealed", sealed["action_key"], "--detach"]) == 0
    record = _one_json_line(capsys.readouterr())
    assert record["transport"] == "slurm"
    assert record["status"] == "published"
    assert record["job_id"]
    assert record["request_sha256"] == sealed["request_sha256"]

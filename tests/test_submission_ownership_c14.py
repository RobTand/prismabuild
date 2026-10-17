"""The package owns every publication gate and each client's resource lifetime."""
from contextlib import contextmanager
from dataclasses import replace
import json
from pathlib import Path

import pbrun
import pbcampaign
from prismabuild import action_client as client, core, filesystem_floor, pool, submission
from prismabuild.submission_types import SubmissionRequest, SubmissionRefused
from submission_support import patch_store
from test_pbrun_detach import _checkout


def _request(tmp_path, monkeypatch, **changes):
    work = _checkout(tmp_path)
    store = patch_store(monkeypatch, root=tmp_path)
    request = SubmissionRequest(cwd=str(work), command=("/bin/bash", "-c", "printf ok"),
                                no_default_env=True, here=True, detach=True)
    return store, replace(request, **changes)


def test_pool_wait_begins_after_growth_release(tmp_path, monkeypatch):
    store, request = _request(tmp_path, monkeypatch, detach=False)
    events = []

    @contextmanager
    def operation(*args, **kwargs):
        events.append("acquired")
        try:
            yield
        finally:
            events.append("released")

    def wait(pool_queue, key, *, wait_s, generation, report):
        queue = pool_queue.records
        assert events == ["acquired", "released"]
        assert queue.item_path(pool.READY, key).exists()
        events.append("wait")
        return report.finish(client.ClientPatienceExpired(key, generation))

    monkeypatch.setattr(filesystem_floor, "operation", operation)
    monkeypatch.setattr(client, "report_pool_outcome", wait)
    result = submission.submit_request(request, store=store)
    assert isinstance(result, client.ClientReport)
    assert isinstance(result.ending.verdict, client.ClientPatienceExpired)
    assert result.ending.readers.complete
    assert events == ["acquired", "released", "wait"]


def test_cache_hit_has_identity_and_never_publishes_a_row(tmp_path, monkeypatch):
    store, request = _request(tmp_path, monkeypatch)
    monkeypatch.setattr(core.PrismaBuildCAS, "lookup", lambda self, action: {
        "result": {"sha256": "a" * 64}})
    result = submission.submit_request(request, store=store)
    assert isinstance(result, client.ClientReport)
    assert isinstance(result.identity, client.CachedClientIdentity)
    assert isinstance(result.ending.verdict, client.ClientSubmitted)
    assert not list((store.queue_root / pool.READY).glob("*.json"))


def test_live_attachment_keeps_the_same_action_and_generation(tmp_path, monkeypatch):
    store, request = _request(tmp_path, monkeypatch)
    first = submission.submit_request(request, store=store)
    second = submission.submit_request(request, store=store)
    assert isinstance(first, client.ClientReport) and isinstance(second, client.ClientReport)
    assert first.identity.action_key == second.identity.action_key
    assert first.identity.published_unix == second.identity.published_unix
    assert second.identity.arrival is client.ClientArrival.ATTACHED
    assert len(list((store.queue_root / pool.READY).glob("*.json"))) == 1


def test_floor_refuses_before_sealing_any_checkout(tmp_path, monkeypatch):
    store, request = _request(tmp_path, monkeypatch)

    @contextmanager
    def refused(*args, **kwargs):
        raise filesystem_floor.FloorRefused([])
        yield

    monkeypatch.setattr(filesystem_floor, "operation", refused)
    result = submission.submit_request(request, store=store)
    assert isinstance(result, SubmissionRefused)
    assert "below its floor" in result.reason
    assert not store.cas_root.exists()
    assert not store.queue_root.exists()


def test_decomposed_children_reach_the_gate_and_keep_repository_identity(tmp_path, monkeypatch):
    from test_a_decomposed_child_is_an_ordinary_sealed_action import _checkout as checkout, _request as logical
    work = checkout(tmp_path)
    store = patch_store(monkeypatch, root=tmp_path)
    request = logical()
    request["common"]["cwd"] = str(work)
    request["common"]["here"] = True
    calls = []
    judge = submission.d38_gate.judge_publication

    def observed(options, action, **kwargs):
        calls.append((action["action_key"], kwargs["store"].queue_root))
        return judge(options, action, **kwargs)

    monkeypatch.setattr(submission.d38_gate, "judge_publication", observed)
    records, frozen = pbcampaign.decompose(request, transport="pool", priority=-10)
    assert {row["status"] for row in records} == {"submitted"}
    assert [key for key, root in calls] == [child["action_key"] for child in frozen["children"]]
    assert all(root == store.queue_root for key, root in calls)
    for record in records:
        row = json.loads((store.queue_root / pool.READY / (record["action_key"] + ".json")).read_text())
        assert row["tested_repository"] == submission.tested_repository_name(work)


def test_gpu_children_cannot_skip_enforced_d38(tmp_path, monkeypatch, d38_config):
    from test_a_decomposed_child_is_an_ordinary_sealed_action import _checkout as checkout, _request as logical
    work = checkout(tmp_path)
    store = patch_store(monkeypatch, root=tmp_path)
    d38_config.write_text('{"d38_enforce": true}')
    request = logical()
    request["common"].update(cwd=str(work), demand={"cpu": 1, "mem_gb": 1, "gpu": 1}, tags=["gb10"], here=True)
    records, frozen = pbcampaign.decompose(request, transport="pool", priority=-10)
    assert len(records) == len(frozen["children"])
    assert {row["status"] for row in records} == {"refused"}
    assert not list((store.queue_root / pool.READY).glob("*.json"))
    from prismabuild.submission_types import ProducerPublication
    native = ProducerPublication(worker_script=store.wrapper_dir / "prismabuild_worker.py",
        checkout_root=None, tags=(), needs_gpu=True, resources={"cpu": 1, "mem_gb": 1},
        retry_safe=None, lane_root=None, job_entry=store.wrapper_dir / "slurm_job.py")
    body = {key: value for key, value in frozen["children"][0].items() if key != "action_key"}
    body["params"] = {**body["params"], "demand": {"cpu": 1, "mem_gb": 1},
                      "placement": {"required_tags": []}}
    action = core.seal_action(body)
    cas = core.PrismaBuildCAS(store.cas_root)
    result = submission.submit_producer(action, cas=cas,
        request_path=cas.publish_action_request(action), producer=native,
        request=SubmissionRequest(cwd=str(work), detach=True), store=store)
    assert isinstance(result, SubmissionRefused)
    assert not list((store.queue_root / pool.READY).glob("*.json"))


def test_staged_publication_reaches_the_same_gate_and_ownership_transaction(tmp_path, monkeypatch):
    import test_pbrun_residency_stage_submission as staging
    from prismabuild import residency_plan
    state = staging._prepare(tmp_path, monkeypatch)
    seen = []
    judge = submission.d38_gate.judge_publication

    def observed(options, action, **kwargs):
        seen.append(action["action_key"])
        return judge(options, action, **kwargs)

    monkeypatch.setattr(submission.d38_gate, "judge_publication", observed)
    assert pbrun.main() == 0
    rows = list(state["queue"].records.dir(pool.READY).glob("*.json"))
    assert len(rows) == 1
    key = rows[0].stem
    assert seen == [key]
    row = json.loads(rows[0].read_text())
    assert row["residency"]["schema"] == pool.RESIDENCY_SCHEMA_V1
    assert residency_plan.read(state["queue"], key) is not None


def test_deferred_release_enters_the_common_gate_before_publishing(tmp_path, monkeypatch, capsys):
    import test_deferred_action_edges as deferred
    from prismabuild import action_edges
    pool_queue, work = deferred._env(tmp_path, monkeypatch)
    queue = pool_queue.records
    template = deferred._template(tmp_path / "canonical")
    producer = deferred._producer_key(tmp_path, template, "c14")
    deferred._publish_producer(pool_queue, template, producer)
    record = deferred._submit(work, monkeypatch, capsys,
        "--after", f"{producer}:{template['template_id']}")
    instance = deferred._start(pool_queue, template, producer)
    deferred._commit(pool_queue, template, instance, "b1", b"C14 handoff")
    pool_queue.finish(producer, status="executed")
    seen = []
    judge = submission.d38_gate.judge_publication

    def observed(options, action, **kwargs):
        key = action["action_key"]
        assert not queue.item_path(pool.READY, key).exists()
        seen.append(key)
        return judge(options, action, **kwargs)

    monkeypatch.setattr(submission.d38_gate, "judge_publication", observed)
    events = deferred.dr.release_tick(pool_queue)
    released, = deferred._released(events)
    assert seen == [released["action_key"]]
    assert action_edges.read_published(queue.root, record["pending_id"])["action_key"] == seen[0]

"""A consumer filed before its producer runs (#913).

``pbrun --after PRODUCER:TEMPLATE_ID`` files a consumer whose producer has
not run yet.  The tiers loop releases it once the producer succeeds: it
resolves the origin-only batches the producer's successful attempt committed
(#912), builds the consumer's data manifest over exactly those, seals it into
the generation its template froze, and publishes it as ``pbrun`` would have.

The issue's acceptance:

*   a consumer published before its producer runs only after the producer
    commits, and reads exactly the committed bytes;
*   a failed producer never releases its consumer;
*   an edge to an unknown key is refused at submission.

And the coordinator's conditions: the tick is bounded per cycle and stops at
the cycle's deadline; a malformed record is reported once and kept; the loop
releases nothing unless it is the published generation, and seals into the
template's retained generation or holds; a released-pending producer
resolves through its release, and a superseded one through its successor.

Fixture concessions: producers are sealed CAS requests published and claimed
through the real ``PoolQueue``, and commit through ``commit_origin_batch``
without running a worker; consumers go through the real ``pbrun.main``.
"""
from __future__ import annotations

import contextlib
import hashlib
import io
import json
from pathlib import Path
import socket
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_prepaid_writer_integration as fx  # noqa: E402
from prismabuild import action_edges as ae  # noqa: E402
from prismabuild import core as pb, pool, produced_output as po  # noqa: E402
from prismabuild import residency_map as rm, residency_plan, storage_tiers  # noqa: E402
import deferred_release as dr  # noqa: E402
import pbrun  # noqa: E402
import pbstatus  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402
from test_pbrun_detach import _checkout  # noqa: E402
from test_pbrun_residency_stage_submission import (  # noqa: E402
    _announce_tier, _tier_cycle,
)
from test_write_only_produced_output import (  # noqa: E402
    KIND, TIER, _descriptor, _prewrite, _queue, _template,
)

_isolated_synthetic_launch_context = fx._isolated_synthetic_launch_context

WORKER = str(fx.REPO / "tools" / "prismabuild_worker.py")
CONSUMED = po.ORIGIN_LIFETIME_CONSUMED
PLACEHOLDER = ae.DATA_MANIFEST_PLACEHOLDER
TAGS = ("sparky", "gb10")


@pytest.fixture(autouse=True)
def _fresh_reports(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each test starts with the tick's once-per-change memo empty."""

    monkeypatch.setattr(dr, "_REPORTED", {})


# -- fixtures ------------------------------------------------------------------


def _env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A queue, a checkout, a worker, and ``repo`` naming this runtime."""

    queue = _queue(tmp_path)
    queue.announce(host="sparky", tags=list(TAGS), has_gpu=True,
                   capacity={"cpu": 4, "mem_gb": 16, "gpu": 1})
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    (tmp_path / "repo").symlink_to(pbrun.RUNTIME_ROOT)
    return queue, _checkout(tmp_path)


def _producer_key(tmp_path: Path, template: dict, seed: str) -> str:
    """Seal and file a producer's request that declares ``template``."""

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    envelope = tmp_path / f"{seed}-template.json"
    envelope.write_text(json.dumps(template, sort_keys=True))
    template_input, _ = cas.ingest_input(
        envelope, input_id=pb.PRODUCED_OUTPUT_TEMPLATE_INPUT_ID)
    checkout = tmp_path / "producer-checkout"
    checkout.mkdir(exist_ok=True)
    (checkout / "seed.txt").write_text("producer\n")
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/deferred-producer",
                 "definition_version": "v1", "task_class": "generation",
                 "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": ["/bin/true", seed], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [template_input],
        "code_closure": pb.build_code_closure(checkout, ["seed.txt"]),
        "params": {"cwd": ".", "command": ["/bin/true", seed],
                   "produced_output_template":
                       po.build_declaration(template, template_input)},
        "environment": {"variables": {"PATH": "/usr/bin:/bin"},
                        "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas.publish_action_request(action)
    return str(action["action_key"])


def _publish_producer(queue: pool.PoolQueue, template: dict, key: str) -> None:
    queue.publish(action_key=key, cas_root="/cas", worker_script=WORKER,
                  checkout_root=str(Path(queue.root).parent / "producer-checkout"),
                  resources={"cpu": 1, "mem_gb": 1}, max_attempts=1,
                  produced_output_template=template)


def _start(queue: pool.PoolQueue, template: dict, key: str, *,
           tags: tuple[str, ...] = ()) -> dict:
    """Claim a producer and bind its attempt's instance, as its run would."""

    claimed = queue.claim(owner="w-producer", tags=list(tags))
    assert claimed is not None and claimed["action_key"] == key
    control = fx._broker_control(queue, key)
    env = {"PRISMABUILD_ACTION_KEY": key,
           "PRISMABUILD_ACTION_NONCE": control["nonce"],
           "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
    instance = po.bind_instance(queue, template, owner_action_key=key,
                                claim_snapshot=claimed, env=env)
    po.declare_instance(queue.root, instance)
    assert po.admit_instance(queue, instance, template)["ok"] is True
    return instance


def _commit(queue, template, instance, batch_id: str, payload: bytes, *,
            lifetime: str = CONSUMED) -> tuple[Path, dict]:
    attempt = instance["owner_attempt"]
    path = (Path(template["output_prefix"])
            / f"{instance['owner_action_key'][:8]}-{attempt['nonce'][:8]}-{batch_id}.bin")
    assert _prewrite(queue, instance, template, batch_id, [path],
                     len(payload))["ok"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    committed = po.commit_origin_batch(
        queue, instance, template, [_descriptor(instance, template, path, payload)],
        batch_id=batch_id, lifetime=lifetime)
    assert committed["ok"], committed
    return path, committed


def _submit(work: Path, monkeypatch, capsys, *options: str,
            command: tuple[str, ...] = ("/bin/cat", PLACEHOLDER)) -> dict:
    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--cwd", str(work), "--wait-s", "0.01", "--detach",
        *options, "--", *command])
    capsys.readouterr()
    assert pbrun.main() == 0
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def _released(events: list[dict]) -> list[dict]:
    return [event for event in events if event["event"] == dr.RELEASED_EVENT]


def _without_summary(events: list[dict]) -> list[dict]:
    return [event for event in events if event["event"] != dr.TICK_EVENT]


def _request(tmp_path: Path, key: str) -> dict:
    return json.loads((tmp_path / "cas" / "requests" / key[:2]
                       / f"{key}.json").read_text())


# -- acceptance ----------------------------------------------------------------


def test_a_consumer_filed_before_its_producer_runs_reads_the_committed_bytes(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Filed first, released after the commit, staged from the committed bytes."""

    queue, work = _env(tmp_path, monkeypatch)
    template = _template(tmp_path / "canonical")
    producer = _producer_key(tmp_path, template, "band-l")
    _publish_producer(queue, template, producer)
    _announce_tier(queue, mountpoint=tmp_path / "stage")

    detach = _submit(work, monkeypatch, capsys,
                     "--after", f"{producer}:{template['template_id']}",
                     "--residency", "stage")
    pending = detach["pending_id"]
    assert detach["status"] == "deferred" and detach["action_key"] is None
    assert ae.read_deferred(queue.root, pending) is not None
    assert {row.stem for row in queue.dir(pool.READY).glob("*.json")} == {
        producer}, "nothing is queued for the consumer yet"

    # The producer is queued, then running: the consumer waits, quietly.
    assert _without_summary(dr.release_tick(queue)) == []
    instance = _start(queue, template, producer)
    assert _without_summary(dr.release_tick(queue)) == []
    payload = b"band L handoff bytes"
    path, committed = _commit(queue, template, instance, "b1", payload)
    assert _without_summary(dr.release_tick(queue)) == [], (
        "a commit is not a success: the producer may still fail")
    queue.finish(producer, status="executed")

    events = dr.release_tick(queue)
    [released] = _released(events)
    key = released["action_key"]
    assert released["pending_id"] == pending
    assert released["refs"] == [committed["ref"]]
    assert released["runtime"] == {
        "root": str(pbrun.RUNTIME_ROOT),
        "generation": ae.generation_name(pbrun.RUNTIME_ROOT)}
    summary = [event for event in events if event["event"] == dr.TICK_EVENT]
    assert summary[0]["released"] == 1 and summary[0]["elapsed_s"] >= 0
    assert ae.read_published(queue.root, pending)["action_key"] == key
    assert queue.item_path(pool.READY, key).exists()
    # Declared against the batch (#914), so its retirement waits for it.
    assert (po._consumers_dir(queue.root, instance, "b1")
            / f"{key}.json").exists()

    # The sealed command names the manifest, and the manifest names exactly
    # the committed file by its committed digest.
    request = _request(tmp_path, key)
    manifest_path = Path(request["params"]["command"][1])
    assert request["params"]["command"][0] == "/bin/cat"
    manifest, _ = pb.read_data_manifest(manifest_path)
    assert [(entry["path"], entry["sha256"]) for entry in manifest["entries"]] == [
        (str(path), hashlib.sha256(payload).hexdigest())]
    assert manifest["annotations"][po.ORIGIN_BATCHES_ANNOTATION] == [
        committed["ref"]]
    assert request["params"]["data_manifest"]["input"]["sha256"] == \
        released["manifest_sha256"]

    # The window stages those bytes, through the real mover.
    _tier_cycle(queue, tmp_path / "stage")
    plan = residency_plan.read(queue, key)
    assert plan is not None and len(plan["phases"]) == 1
    lead = str(plan["phases"][0]["mover_row"]["action_key"])
    mover = _request(tmp_path, lead)["params"]["command"]
    assert stage_move.main([*mover[2:], "--action-key", lead, "--unpaced"]) == 0
    composed = rm.compose(rm.read_fragments(queue.root / pool.RESIDENCY, key))
    staged = composed["entries"][rm.residency_map_key(str(path), 0)]
    assert Path(staged["stage_path"]).read_bytes() == payload

    # Released once: later ticks find nothing unreleased.
    assert dr.release_tick(queue) == []


def test_a_failed_producer_never_releases_its_consumer(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    queue, work = _env(tmp_path, monkeypatch)
    template = _template(tmp_path / "canonical")
    producer = _producer_key(tmp_path, template, "fails")
    _publish_producer(queue, template, producer)
    pending = _submit(work, monkeypatch, capsys, "--after",
                      f"{producer}:{template['template_id']}")["pending_id"]
    instance = _start(queue, template, producer)
    _commit(queue, template, instance, "b1", b"never read")
    queue.finish(producer, status="failed")

    events = _without_summary(dr.release_tick(queue))
    assert events == [{
        "event": dr.HELD_EVENT, "pending_id": pending,
        "reason": "producer-will-not-succeed",
        "edges": [{"producer": producer,
                   "template_id": template["template_id"], "state": "failed",
                   "key": producer, "path": [producer]}]}]
    assert _without_summary(dr.release_tick(queue)) == [], "once per change"
    assert ae.read_release(queue.root, pending) is None
    assert ae.read_published(queue.root, pending) is None
    assert pbrun.await_release(queue, pending, wait_s=0.01) == pbrun.GAVE_UP_EXIT
    # The failed producer's batch is an orphan: nothing unreleased will read
    # a failed key, so #914 sweeps it.
    assert ae.held_producer_batches(queue) == set()


def test_an_edge_to_an_unknown_key_is_refused_at_submission(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    queue, work = _env(tmp_path, monkeypatch)
    template = _template(tmp_path / "canonical")
    producer = _producer_key(tmp_path, template, "known")
    _publish_producer(queue, template, producer)

    def refused(*options: str, command=("/bin/cat", PLACEHOLDER)) -> str:
        monkeypatch.setattr(sys, "argv", [
            "pbrun.py", "--cwd", str(work), "--wait-s", "0.01", "--detach",
            *options, "--", *command])
        with pytest.raises(SystemExit) as caught:
            pbrun.main()
        return str(caught.value)

    unknown = fx._hexkey("nobody-filed-this")
    assert "no action or deferred submission with this key" in refused(
        "--after", f"{unknown}:{template['template_id']}")
    assert "declares template" in refused("--after", f"{producer}:another")
    assert "at most once" in refused(
        "--after", f"{producer}:{template['template_id']}",
        command=("/bin/cat", PLACEHOLDER, PLACEHOLDER))
    # A pbrun outside the generation store (a development checkout) freezes
    # a template no release could seal: refused now, not held for ever.
    current = pbrun.CONTAINER_WRAPPER_DIR
    monkeypatch.setattr(pbrun, "CONTAINER_WRAPPER_DIR",
                        _retained_generation(tmp_path / "dev", "g-dev"))
    assert "no release could seal" in refused(
        "--after", f"{producer}:{template['template_id']}")
    monkeypatch.setattr(pbrun, "CONTAINER_WRAPPER_DIR", current)
    assert not (queue.root / ae.DEFERRED_SUBDIR).exists(), (
        "a refused submission files nothing")
    with pytest.raises(SystemExit):
        pbrun.parse_args(["--after", f"{producer}:t", "--transport", "slurm",
                          "--", "true"])
    with pytest.raises(SystemExit):
        pbrun.parse_args(["--after", f"{producer}:t", "--as-sealed-by",
                          producer, "--", "true"])


# -- chains and supersession ---------------------------------------------------


def test_a_chain_resolves_a_deferred_producer_through_its_release(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """B waits on A's pending id; A waits on P.  P, then A, then B."""

    queue, work = _env(tmp_path, monkeypatch)
    template = _template(tmp_path / "canonical")
    template_path = tmp_path / "band-template.json"
    template_path.write_text(json.dumps(template, sort_keys=True))
    producer = _producer_key(tmp_path, template, "band-45")
    _publish_producer(queue, template, producer)
    edge = template["template_id"]
    a = _submit(work, monkeypatch, capsys, "--after", f"{producer}:{edge}",
                "--produced-output-template", str(template_path),
                command=("/bin/cat", PLACEHOLDER, "band-44"))["pending_id"]
    b = _submit(work, monkeypatch, capsys, "--after", f"{a}:{edge}",
                command=("/bin/cat", PLACEHOLDER, "band-43"))["pending_id"]
    # A pending producer must declare the template the edge names.
    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--cwd", str(work), "--detach", "--after", f"{b}:{edge}",
        "--", "/bin/cat", PLACEHOLDER])
    with pytest.raises(SystemExit, match="does not declare template"):
        pbrun.main()

    instance = _start(queue, template, producer)
    _commit(queue, template, instance, "b1", b"band 45")
    queue.finish(producer, status="executed")
    [released_a] = _released(dr.release_tick(queue))
    assert released_a["pending_id"] == a
    assert ae.read_published(queue.root, b) is None, "B waits on A running"

    a_key = released_a["action_key"]
    a_instance = _start(queue, template, a_key, tags=TAGS)
    a_path, a_batch = _commit(queue, template, a_instance, "b1", b"band 44")
    queue.finish(a_key, status="executed")
    [released_b] = _released(dr.release_tick(queue))
    assert released_b["pending_id"] == b
    assert released_b["refs"] == [a_batch["ref"]]
    assert released_b["producers"][0]["path"] == [a, a_key]

    b_key = released_b["action_key"]
    claimed = queue.claim(owner="w-consumer", tags=list(TAGS))
    assert claimed["action_key"] == b_key
    queue.finish(b_key, status="executed")
    assert pbrun.await_release(queue, b, wait_s=1.0) == 0


def test_a_superseded_producer_releases_its_consumer_under_the_new_key(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Followed once filed, whatever the old key does later."""

    queue, work = _env(tmp_path, monkeypatch)
    template = _template(tmp_path / "canonical")
    old = _producer_key(tmp_path, template, "attempt-one")
    new = _producer_key(tmp_path, template, "attempt-two")
    _publish_producer(queue, template, old)
    pending = _submit(work, monkeypatch, capsys, "--after",
                      f"{old}:{template['template_id']}")["pending_id"]
    with pytest.raises(ae.ActionEdgeError, match="only a failed or withdrawn"):
        ae.file_supersession(queue, old, new=new, new_kind=ae.PRODUCER_KEY)
    instance = _start(queue, template, old)
    _commit(queue, template, instance, "b1", b"old bytes")
    queue.finish(old, status="failed")
    ae.file_supersession(queue, old, new=new, new_kind=ae.PRODUCER_KEY)

    # The old key runs again and succeeds; edges still read the new one,
    # which is queued.
    _publish_producer(queue, template, old)
    rerun = _start(queue, template, old)
    _publish_producer(queue, template, new)
    _commit(queue, template, rerun, "b1", b"old again")
    queue.finish(old, status="executed")
    assert _without_summary(dr.release_tick(queue)) == []
    assert ae.read_release(queue.root, pending) is None

    successor = _start(queue, template, new)
    new_path, new_batch = _commit(queue, template, successor, "b1", b"new bytes")
    queue.finish(new, status="executed")
    [released] = _released(dr.release_tick(queue))
    assert released["refs"] == [new_batch["ref"]]
    assert released["producers"][0]["path"] == [old, new]


def test_a_superseded_pending_id_is_never_released_and_a_loop_refuses(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    queue, work = _env(tmp_path, monkeypatch)
    template = _template(tmp_path / "canonical")
    producer = _producer_key(tmp_path, template, "resubmitted")
    _publish_producer(queue, template, producer)
    edge = f"{producer}:{template['template_id']}"
    first = _submit(work, monkeypatch, capsys, "--after", edge)["pending_id"]
    second = _submit(work, monkeypatch, capsys, "--after", edge,
                     "--supersedes", first,
                     command=("/bin/cat", PLACEHOLDER, "fixed"))["pending_id"]
    assert ae.read_supersession(queue.root, first)["new"] == second
    with pytest.raises(ae.ActionEdgeError, match="close a loop"):
        ae.file_supersession(queue, second, new=first,
                             new_kind=ae.PRODUCER_PENDING)

    instance = _start(queue, template, producer)
    _commit(queue, template, instance, "b1", b"read once")
    queue.finish(producer, status="executed")
    events = dr.release_tick(queue)
    assert [event["pending_id"] for event in _released(events)] == [second]
    [summary] = [event for event in events if event["event"] == dr.TICK_EVENT]
    assert summary["superseded"] == 1
    assert ae.read_release(queue.root, first) is None


# -- the #914 interaction ------------------------------------------------------


def test_an_unreleased_consumer_holds_the_batch_its_producer_committed(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Fan-out: one consumer succeeded, the deferred one is not released yet."""

    queue, work = _env(tmp_path, monkeypatch)
    template = _template(tmp_path / "canonical")
    first = _producer_key(tmp_path, template, "first")
    second = _producer_key(tmp_path, template, "second")
    _publish_producer(queue, template, first)
    _publish_producer(queue, template, second)
    edge = template["template_id"]
    pending = _submit(work, monkeypatch, capsys,
                      "--after", f"{first}:{edge}",
                      "--after", f"{second}:{edge}")["pending_id"]
    instance = _start(queue, template, first)
    other = _start(queue, template, second)
    path, committed = _commit(queue, template, instance, "b1", b"shared handoff")
    queue.finish(first, status="executed")
    reader = fx._hexkey("ordinary-reader")
    po.declare_origin_consumer(queue, committed["ref"], consumer_action_key=reader)
    queue.publish(action_key=reader, cas_root="/cas", worker_script=WORKER,
                  checkout_root=str(tmp_path / "reader-checkout"),
                  resources={"cpu": 1, "mem_gb": 1}, max_attempts=1)
    claimed = queue.claim(owner="w-reader")
    assert claimed["action_key"] == reader
    queue.finish(reader, status="executed")

    assert ae.held_producer_batches(queue) == {(first, edge), (second, edge)}
    assert po.origin_retirement_tick(queue) == []
    assert path.exists(), "the deferred consumer will read it"

    _commit(queue, template, other, "b1", b"second handoff")
    queue.finish(second, status="executed")
    [released] = _released(dr.release_tick(queue))
    key = released["action_key"]
    assert ae.held_producer_batches(queue) == set()
    assert po.origin_retirement_tick(queue) == [], "declared and queued: held"
    claimed = queue.claim(owner="w-consumer", tags=list(TAGS))
    assert claimed["action_key"] == key
    queue.finish(key, status="executed")
    retired = [event for event in po.origin_retirement_tick(queue)
               if event["event"] == po.ORIGIN_RETIRED_EVENT]
    assert {event["ref"]["owner_action_key"] for event in retired} == {
        first, second}
    assert not path.exists()
    assert ae.read_published(queue.root, pending)["action_key"] == key


def test_a_cache_hit_producer_is_read_from_the_attempt_that_committed(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Attempt N1 committed and died; N2 found the receipt and ran nothing."""

    queue, work = _env(tmp_path, monkeypatch)
    template = _template(tmp_path / "canonical")
    producer = _producer_key(tmp_path, template, "cache-hit")
    _publish_producer(queue, template, producer)
    instance = _start(queue, template, producer)
    path, committed = _commit(queue, template, instance, "b1", b"n1 bytes")
    fx._broker_control(queue, producer)       # a retry's attempt holds the claim
    queue.finish(producer, status="cache_hit")

    assert po._producer_attempt_state(queue, instance) == "unknown"
    assert po.origin_retirement_tick(queue) == []
    assert path.exists(), "the cache hit ran nothing: these are the only bytes"

    _submit(work, monkeypatch, capsys, "--after",
            f"{producer}:{template['template_id']}")
    [released] = _released(dr.release_tick(queue))
    assert released["refs"] == [committed["ref"]]
    assert released["producers"][0]["nonce"] == instance["owner_attempt"]["nonce"]


# -- crash safety --------------------------------------------------------------


def test_a_pinned_release_resumes_as_pinned_after_a_crash(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    queue, work = _env(tmp_path, monkeypatch)
    template = _template(tmp_path / "canonical")
    producer = _producer_key(tmp_path, template, "crash")
    _publish_producer(queue, template, producer)
    pending = _submit(work, monkeypatch, capsys, "--after",
                      f"{producer}:{template['template_id']}")["pending_id"]
    instance = _start(queue, template, producer)
    _path, committed = _commit(queue, template, instance, "b1", b"pinned")
    queue.finish(producer, status="executed")

    publish = pbrun.publish_consumer_row

    def crash(*_args, **_kwargs):
        raise OSError("the tier host lost power")

    monkeypatch.setattr(pbrun, "publish_consumer_row", crash)
    [refusal] = _without_summary(dr.release_tick(queue))
    assert refusal["event"] == dr.REFUSED_EVENT and "lost power" in refusal["reason"]
    pinned = ae.read_release(queue.root, pending)
    assert pinned is not None and ae.read_published(queue.root, pending) is None
    assert ae.held_producer_batches(queue) == {(producer, template["template_id"])}

    # The producer's key runs again and fails: re-resolving would hold.
    _publish_producer(queue, template, producer)
    queue.claim(owner="w-producer")
    queue.finish(producer, status="failed")
    monkeypatch.setattr(pbrun, "publish_consumer_row", publish)
    [resumed] = _released(dr.release_tick(queue))
    assert resumed["resumed"] is True
    assert resumed["action_key"] == pinned["action_key"]
    assert resumed["refs"] == [committed["ref"]]
    assert queue.item_path(pool.READY, pinned["action_key"]).exists()


def test_a_release_published_before_a_crash_records_that_generation(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    queue, work = _env(tmp_path, monkeypatch)
    template = _template(tmp_path / "canonical")
    producer = _producer_key(tmp_path, template, "late-record")
    _publish_producer(queue, template, producer)
    pending = _submit(work, monkeypatch, capsys, "--after",
                      f"{producer}:{template['template_id']}")["pending_id"]
    instance = _start(queue, template, producer)
    _commit(queue, template, instance, "b1", b"published once")
    queue.finish(producer, status="executed")

    record = ae.file_published

    def crash(*_args, **_kwargs):
        raise OSError("crashed before the record")

    monkeypatch.setattr(ae, "file_published", crash)
    dr.release_tick(queue)
    key = ae.read_release(queue.root, pending)["action_key"]
    row = json.loads(queue.item_path(pool.READY, key).read_text())
    monkeypatch.setattr(ae, "file_published", record)
    [resumed] = _released(dr.release_tick(queue))
    assert resumed["published_unix"] == row["published_unix"]
    assert json.loads(queue.item_path(pool.READY, key).read_text()) == row, (
        "the row was not published a second time")


# -- generations ---------------------------------------------------------------


def _retained_generation(store: Path, name: str) -> Path:
    """A retained generation: a Docker wrapper beside a receipt naming it."""

    tools = store / name / "tools"
    tools.mkdir(parents=True)
    shim = tools / "docker"
    shim.write_bytes(b"#!/bin/sh\nexec /usr/bin/docker \"$@\"\n")
    receipt = store / name / "RUNTIME_VERSION.json"
    receipt.write_text(json.dumps({
        "schema": "prismaquant.prismabuild.runtime_version.v1",
        "generation": name,
        "files": {"tools/docker": hashlib.sha256(shim.read_bytes()).hexdigest()}}))
    for path in (shim, receipt):
        path.chmod(0o444)
    return tools


def test_a_consumer_is_sealed_into_the_retained_generation_that_froze_it(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """A publish between submission and release does not strand it."""

    queue, work = _env(tmp_path, monkeypatch)
    template = _template(tmp_path / "canonical")
    producer = _producer_key(tmp_path, template, "retained")
    _publish_producer(queue, template, producer)
    store = tmp_path / "runtime-generations"
    old = _retained_generation(store, "g-old")
    bad = _retained_generation(store, "g-bad")
    edge = f"{producer}:{template['template_id']}"
    current = pbrun.CONTAINER_WRAPPER_DIR
    monkeypatch.setattr(pbrun, "CONTAINER_WRAPPER_DIR", old)
    kept = _submit(work, monkeypatch, capsys, "--after", edge)["pending_id"]
    monkeypatch.setattr(pbrun, "CONTAINER_WRAPPER_DIR", bad)
    tampered = _submit(work, monkeypatch, capsys, "--after", edge,
                       command=("/bin/cat", PLACEHOLDER, "b"))["pending_id"]
    monkeypatch.setattr(pbrun, "CONTAINER_WRAPPER_DIR", current)
    # Tampered after its submission: its wrapper no longer matches its receipt.
    receipt = bad.parent / "RUNTIME_VERSION.json"
    receipt.chmod(0o644)
    receipt.write_text(json.dumps({**json.loads(receipt.read_text()),
                                   "files": {"tools/docker": "0" * 64}}))
    receipt.chmod(0o444)

    capsys.readouterr()
    assert pbstatus.main(["--deferred", "--queue-root", str(queue.root),
                          "--repo-link", str(tmp_path / "repo")]) == 0
    listing = json.loads(capsys.readouterr().out)
    assert {(item["pending_id"], item["runtime"]["generation"],
             item["off_published"]) for item in listing["consumers"]} == {
        (kept, "g-old", True), (tampered, "g-bad", True)}

    instance = _start(queue, template, producer)
    _commit(queue, template, instance, "b1", b"handoff")
    queue.finish(producer, status="executed")
    events = _without_summary(dr.release_tick(queue))
    [released] = _released(events)
    assert released["pending_id"] == kept
    assert released["runtime"] == {"root": str(old.parent), "generation": "g-old"}
    assert ae.read_release(queue.root, kept)["runtime"]["generation"] == "g-old"
    request = _request(tmp_path, released["action_key"])
    assert request["environment"]["variables"]["PATH"].startswith(f"{old}:")
    [held] = [event for event in events if event["event"] == dr.HELD_EVENT]
    assert held["pending_id"] == tampered
    assert held["reason"] == "runtime-generation-unavailable"
    assert held["remedy"] == f"resubmit with --supersedes {tampered}"
    assert ae.read_release(queue.root, tampered) is None, "never sealed elsewhere"
    assert _without_summary(dr.release_tick(queue)) == [], "once per change"


def test_a_publish_keeps_the_generation_an_unreleased_consumer_froze(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Two real publications: the first stays, and the consumer seals into it.

    Retention is not a reference count.  ``publish_runtime`` never deletes a
    generation (it removes only its own failed ``.staging`` trees), and
    ``pb_gc`` surveys the CAS, not the generation store, so a generation an
    unreleased consumer froze is still there, receipt and wrapper intact,
    after any number of later publications.
    """

    from test_publish_runtime import _fake_git_and_probe, publish_runtime

    queue = _queue(tmp_path)
    queue.announce(host="sparky", tags=list(TAGS), has_gpu=True,
                   capacity={"cpu": 4, "mem_gb": 16, "gpu": 1})
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    work = _checkout(tmp_path)
    mirror = tmp_path / "repo"
    monkeypatch.setattr(publish_runtime, "CHECKOUT", fx.REPO)
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)

    real_run = publish_runtime.subprocess.run

    def publish(commit: str) -> Path:
        # ``subprocess`` is one module: the fake answers publication's Git
        # questions only while it publishes, never pbrun's.
        monkeypatch.setattr(publish_runtime.subprocess, "run",
                            _fake_git_and_probe(commit))
        monkeypatch.setattr(sys, "argv", [
            "publish_runtime.py", "--rollout", "rolling", "--no-canary",
            "--rollout-reason", "fixture publication"])
        try:
            assert publish_runtime.main() == 0
        finally:
            monkeypatch.setattr(publish_runtime.subprocess, "run", real_run)
        return mirror.resolve()

    first = publish("a" * 40)
    template = _template(tmp_path / "canonical")
    producer = _producer_key(tmp_path, template, "across-a-publish")
    _publish_producer(queue, template, producer)
    monkeypatch.setattr(pbrun, "CONTAINER_WRAPPER_DIR", first / "tools")
    pending = _submit(work, monkeypatch, capsys, "--after",
                      f"{producer}:{template['template_id']}")["pending_id"]

    second = publish("b" * 40)
    assert second != first and second.parent == first.parent
    assert (first / "RUNTIME_VERSION.json").is_file()
    # The loop restarted on the new generation, as the supervisor does.
    monkeypatch.setattr(pbrun, "RUNTIME_ROOT", second)
    monkeypatch.setattr(pbrun, "CONTAINER_WRAPPER_DIR", second / "tools")
    instance = _start(queue, template, producer)
    _commit(queue, template, instance, "b1", b"across a publish")
    queue.finish(producer, status="executed")
    [released] = _released(dr.release_tick(queue))
    assert released["pending_id"] == pending
    assert released["runtime"] == {"root": str(first), "generation": first.name}


def test_a_loop_that_is_not_the_published_generation_releases_nothing(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    queue, work = _env(tmp_path, monkeypatch)
    template = _template(tmp_path / "canonical")
    producer = _producer_key(tmp_path, template, "loop-behind")
    _publish_producer(queue, template, producer)
    pending = _submit(work, monkeypatch, capsys, "--after",
                      f"{producer}:{template['template_id']}")["pending_id"]
    instance = _start(queue, template, producer)
    _commit(queue, template, instance, "b1", b"bytes")
    queue.finish(producer, status="executed")
    newer = tmp_path / "runtime-generations" / "g-new"
    newer.mkdir(parents=True)
    (tmp_path / "repo").unlink()
    (tmp_path / "repo").symlink_to(newer)

    events = dr.release_tick(queue)
    assert [event["reason"] for event in events] == ["loop-runtime-is-not-published"]
    assert dr.release_tick(queue) == [], "once per change"
    assert ae.read_release(queue.root, pending) is None


# -- bounds and containment ----------------------------------------------------


def test_a_burst_of_releases_is_bounded_per_cycle_and_after_the_windows(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    queue, work = _env(tmp_path, monkeypatch)
    template = _template(tmp_path / "canonical")
    producer = _producer_key(tmp_path, template, "burst")
    _publish_producer(queue, template, producer)
    for index in range(5):
        _submit(work, monkeypatch, capsys, "--after",
                f"{producer}:{template['template_id']}",
                command=("/bin/cat", PLACEHOLDER, f"reader-{index}"))
    instance = _start(queue, template, producer)
    _commit(queue, template, instance, "b1", b"burst")
    queue.finish(producer, status="executed")

    order: list[str] = []
    window = tier_loop.residency_window
    tick = dr.release_tick

    def windows(*args, **kwargs):
        order.append("windows")
        return window(*args, **kwargs)

    def releases(*args, **kwargs):
        order.append("releases")
        return tick(*args, **kwargs)

    monkeypatch.setattr(tier_loop, "residency_window", windows)
    monkeypatch.setattr(dr, "release_tick", releases)

    def cycle() -> list[dict]:
        out = io.StringIO()
        queue.mint_tier_capacity(TIER, {KIND: 8})
        stage = tmp_path / "stage"
        stage.mkdir(exist_ok=True)
        stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
        with contextlib.redirect_stdout(out):
            tier_loop.cycle(queue, host="sparky", source_pool="storage_pool",
                            receipts=tier_loop.ReceiptCache(),
                            discover=lambda **_kwargs: {TIER: {
                                "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                                "tier_id": TIER, "host": "sparky",
                                "tier": "stage", "mountpoint": str(stage),
                                "capacity_bytes": 8 * storage_tiers.GIB}})
        return [json.loads(line) for line in out.getvalue().splitlines()
                if line.startswith("{")]

    # A cycle whose interval has already run out starts no release.
    monkeypatch.setattr(tier_loop, "CYCLE_INTERVAL_S", 0.0)
    [summary] = [line for line in cycle() if line.get("event") == dr.TICK_EVENT]
    assert (summary["released"], summary["carried"]) == (0, 5)
    assert order == ["windows", "releases"]

    monkeypatch.setattr(tier_loop, "CYCLE_INTERVAL_S", 60.0)
    monkeypatch.setattr(dr, "MAX_RELEASES_PER_CYCLE", 2)
    counts = []
    for _ in range(3):
        lines = cycle()
        counts.append(sum(line.get("event") == dr.RELEASED_EVENT for line in lines))
    assert counts == [2, 2, 1]
    assert ae.unreleased_ids(queue.root) == []
    assert [line for line in cycle()
            if str(line.get("event", "")).startswith("deferred")] == [], (
        "nothing unreleased: the tick is silent")


def test_a_malformed_record_is_reported_once_and_kept(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    queue, work = _env(tmp_path, monkeypatch)
    template = _template(tmp_path / "canonical")
    producer = _producer_key(tmp_path, template, "malformed")
    _publish_producer(queue, template, producer)
    good = _submit(work, monkeypatch, capsys, "--after",
                   f"{producer}:{template['template_id']}")["pending_id"]
    broken = "f" * 64
    path = queue.root / ae.DEFERRED_SUBDIR / f"{broken}.json"
    path.write_text('{"schema": "not-a-deferred-record"}')

    events = _without_summary(dr.release_tick(queue))
    assert [(event["event"], event["pending_id"]) for event in events] == [
        (dr.REFUSED_EVENT, broken)]
    assert _without_summary(dr.release_tick(queue)) == []
    assert path.exists(), "kept for an operator"
    # It can never be released, so it holds no batch (#914).
    assert ae.held_producer_batches(queue) == {(producer, template["template_id"])}

    instance = _start(queue, template, producer)
    _commit(queue, template, instance, "b1", b"still released")
    queue.finish(producer, status="executed")
    assert [event["pending_id"] for event in _released(dr.release_tick(queue))] == [good]


# -- off by default ------------------------------------------------------------


def test_a_submission_without_after_files_nothing_new(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    queue, work = _env(tmp_path, monkeypatch)
    detach = _submit(work, monkeypatch, capsys, command=("/bin/true",))
    assert detach["action_key"] and "pending_id" not in detach
    for subdir in (ae.DEFERRED_SUBDIR, ae.RELEASES_SUBDIR, ae.SUPERSESSIONS_SUBDIR):
        assert not (queue.root / subdir).exists(), subdir
    assert dr.release_tick(queue) == []
    assert ae.held_producer_batches(queue) == set()

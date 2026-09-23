"""A v2 read plan reads produced-output batches where it names them (#946).

A consumer that reads a producer's committed handoff in the middle of its
readset, and re-reads its own entries after it, declares one data_manifest.v2:
its static entries and read plan, with each phase that reads batches left
empty and named under ``annotations.produced_output_slots``.

*   **Deferred** (``--after``): each slot names the edge that fills it
    (``{"phase", "after": "PRODUCER:TEMPLATE_ID"}``). The release places
    the batches that producer committed at that phase.
*   **Ordinary**: the submitter places committed batches itself with
    ``produced_output.place_origin_batches`` (``{"phase", "refs"}``), and
    ``pbrun`` places them again from the queue's records and compares.

Both build the manifest with the same function, so the two paths yield the
same bytes, and residency stages the plan's own order: the batch second and
the re-read entries again. A v1 manifest is released and checked as before.

Fixture concessions, as in #913's tests: producers are published, claimed
and finished through the real ``PoolQueue``; submissions go through the real
``pbrun.main``; the release is the real tick and the movers the real
``stage_move``. The published-storage gate for v2 compares hashed files of a
published generation this checkout does not have, so it is replaced by a
recorder; its own tests are ``test_pbrun_read_plan_storage_gate.py``.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_prepaid_writer_integration as fx  # noqa: E402
from prismabuild import action_edges as ae  # noqa: E402
from prismabuild import core as pb, pool, produced_output as po  # noqa: E402
from prismabuild import residency_map as rm, residency_plan  # noqa: E402
import deferred_release as dr  # noqa: E402
import pbrun  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_move  # noqa: E402
from test_deferred_action_edges import (  # noqa: E402
    TAGS, _commit, _env, _producer_key, _publish_producer, _released,
    _request, _start, _submit,
)
from test_pbrun_residency_stage_submission import (  # noqa: E402
    _announce_tier, _tier_cycle,
)
from test_superseded_origin_consumers import _refused  # noqa: E402
from test_write_only_produced_output import _template  # noqa: E402

_isolated_synthetic_launch_context = fx._isolated_synthetic_launch_context

V2 = pb.DATA_MANIFEST_SCHEMA_V2
HEAD = (b"head entry zero", b"head entry one, a little longer")
PAYLOAD = b"band L handoff bytes"
#: The consumer's read timeline: its head, the handoff, then a replay that
#: reads the head's entries again, one window each.
PLAN = (("head", [0, 1]), ("handoff", []), ("replay-0", [0]), ("replay-1", [1]))
PROGRESS = tuple(arg for name, _ in PLAN
                 for arg in ("--progress-phase", f"{name}=600"))


@pytest.fixture(autouse=True)
def _fresh_reports(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dr, "_REPORTED", {})


def _progress_worker(queue: pool.PoolQueue, monkeypatch) -> list[str]:
    """A worker that runs progress-reporting actions, and the storage gate.

    Returns the list the gate appends to each time it is consulted.
    """

    queue.announce(host="sparky",
                   tags=[*TAGS, pb.PROGRESS_TAG, pb.PROGRESS_HELPER_TAG],
                   has_gpu=True, capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
                   progress_contracts=[pb.PROGRESS_RECORD_SCHEMA_V1])
    consulted: list[str] = []
    monkeypatch.setattr(pbrun, "require_deployed_read_plan_storage",
                        lambda: consulted.append("v2"))
    return consulted


def _head_paths(tmp_path: Path) -> list[str]:
    return [str(tmp_path / "static-src" / f"h{index}.bin")
            for index in range(len(HEAD))]


def _plan(entries: list[dict], layout) -> dict:
    phases, running = [], 0
    for name, indices in layout:
        size = sum(int(entries[index]["bytes"]) for index in indices)
        running += size
        phases.append({"name": name, "entry_indices": list(indices),
                       "bytes": size, "cumulative_bytes": running})
    return {"phases": phases, "read_bytes": running}


def _static(tmp_path: Path, *slots: dict, layout=PLAN,
            annotations: dict | None = None) -> dict:
    """The consumer's own v2 manifest: its head files and the plan above."""

    root = tmp_path / "static-src"
    root.mkdir(exist_ok=True)
    entries = []
    for path, data in zip(_head_paths(tmp_path), HEAD):
        Path(path).write_bytes(data)
        entries.append({"path": path, "offset": 0, "bytes": len(data),
                        "sha256": None})
    notes = dict(annotations or {})
    if slots:
        notes[po.ORIGIN_SLOTS_ANNOTATION] = list(slots)
    return pb.validate_data_manifest({
        "schema": V2, "produced_by": {"tool": "tests/946"},
        "mount_prefix": str(root), "entries": entries,
        "entry_count": len(entries),
        "total_bytes": sum(len(data) for data in HEAD),
        "annotations": notes, "read_plan": _plan(entries, layout)})


def _write(path: Path, manifest: dict) -> Path:
    path.write_text(json.dumps(manifest, sort_keys=True))
    return path


def _committed(tmp_path: Path, queue: pool.PoolQueue, seed: str):
    """A producer that ran, committed one batch and succeeded."""

    template = _template(tmp_path / "canonical")
    producer = _producer_key(tmp_path, template, seed)
    _publish_producer(queue, template, producer)
    instance = _start(queue, template, producer)
    path, committed = _commit(queue, template, instance, "b1", PAYLOAD)
    queue.finish(producer, status="executed")
    return template, producer, instance, path, committed


def _placed(tmp_path: Path, queue: pool.PoolQueue, ref: dict) -> dict:
    return po.place_origin_batches(
        queue.root, _static(tmp_path), [{"phase": "handoff", "refs": [ref]}])


def _sealed_manifest(tmp_path: Path, key: str) -> dict:
    summary = _request(tmp_path, key)["params"]["data_manifest"]
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    manifest, _ = pb.read_data_manifest(cas.input_path(summary["input"]))
    assert summary["schema"] == V2
    assert summary["read_bytes"] == manifest["read_plan"]["read_bytes"]
    return manifest


def _reads(manifest: dict) -> list[tuple[str, list[str]]]:
    return [(phase["name"],
             [manifest["entries"][index]["path"] for index in phase["entry_indices"]])
            for phase in manifest["read_plan"]["phases"]]


def _staged_windows(queue: pool.PoolQueue, key: str,
                    manifest: dict) -> list[tuple[str, list[str]]]:
    """What each phase of the frozen plan stages, as its mover resolves it."""

    plan = residency_plan.read(queue, key)
    assert plan is not None
    timeline = prewarm_loop.manifest_read_entries(manifest)
    return [(str(phase["name"]),
             [str(entry["path"]) for entry in prewarm_loop.entries_between(
                 timeline, int(phase["start_bytes"]), int(phase["end_bytes"]))])
            for phase in plan["phases"]]


def _expected_reads(tmp_path: Path, batch: Path) -> list[tuple[str, list[str]]]:
    h0, h1 = _head_paths(tmp_path)
    return [("head", [h0, h1]), ("handoff", [str(batch)]),
            ("replay-0", [h0]), ("replay-1", [h1])]


def _run_mover(tmp_path: Path, queue: pool.PoolQueue, key: str,
               name: str) -> dict:
    """Run one phase's mover and return the consumer's composed map."""

    plan = residency_plan.read(queue, key)
    [phase] = [phase for phase in plan["phases"] if phase["name"] == name]
    mover = str(phase["mover_row"]["action_key"])
    command = _request(tmp_path, mover)["params"]["command"]
    assert stage_move.main([*command[2:], "--action-key", mover, "--unpaced"]) == 0
    return rm.compose(rm.read_fragments(queue.root / pool.RESIDENCY, key))


def _staged_bytes(composed: dict, path: str) -> bytes:
    staged = composed["entries"][rm.residency_map_key(path, 0)]
    return Path(staged["stage_path"]).read_bytes()


# -- acceptance ----------------------------------------------------------------


def test_a_deferred_plan_reads_the_batch_at_the_phase_it_names(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """--after: head, then the handoff, then a replay of the head, staged so."""

    queue, work = _env(tmp_path, monkeypatch)
    consulted = _progress_worker(queue, monkeypatch)
    template = _template(tmp_path / "canonical")
    producer = _producer_key(tmp_path, template, "band-l")
    _publish_producer(queue, template, producer)
    _announce_tier(queue, mountpoint=tmp_path / "stage")
    edge = f"{producer}:{template['template_id']}"
    static = _write(tmp_path / "static.json",
                    _static(tmp_path, {"phase": "handoff", "after": edge}))

    detach = _submit(work, monkeypatch, capsys, "--after", edge,
                     "--data-manifest", str(static), "--residency", "stage",
                     *PROGRESS)
    assert detach["status"] == "deferred"
    assert consulted == ["v2"], "the published storage generation must read v2"

    instance = _start(queue, template, producer)
    path, committed = _commit(queue, template, instance, "b1", PAYLOAD)
    queue.finish(producer, status="executed")
    [released] = _released(dr.release_tick(queue))
    key = released["action_key"]
    assert released["refs"] == [committed["ref"]]

    # The batch is read where the plan named it, and the replay re-reads the
    # head's entries by index, as v2 means.
    manifest = _sealed_manifest(tmp_path, key)
    assert _request(tmp_path, key)["params"]["command"][1] == str(
        pb.PrismaBuildCAS(tmp_path / "cas").input_path(
            _request(tmp_path, key)["params"]["data_manifest"]["input"]))
    assert _reads(manifest) == _expected_reads(tmp_path, path)
    assert manifest["annotations"][po.ORIGIN_BATCHES_ANNOTATION] == [committed["ref"]]
    assert manifest["annotations"][po.ORIGIN_SLOTS_ANNOTATION] == [
        {"phase": "handoff", "refs": [committed["ref"]]}]
    # One mechanism: the release built what an ordinary submitter builds, and
    # what pbrun accepts from one.
    assert manifest == _placed(tmp_path, queue, committed["ref"])
    pbrun.require_declared_origin_batches(
        manifest, transport="pool", queue_root=queue.root)
    assert (po._consumers_dir(queue.root, instance, "b1")
            / f"{key}.json").exists(), "declared against the batch (#914)"

    # Residency follows the plan: the batch second, the head again after it.
    _tier_cycle(queue, tmp_path / "stage")
    assert _staged_windows(queue, key, manifest) == _expected_reads(tmp_path, path)
    h0, h1 = _head_paths(tmp_path)
    composed = _run_mover(tmp_path, queue, key, "head")
    assert [_staged_bytes(composed, item) for item in (h0, h1)] == list(HEAD)
    composed = _run_mover(tmp_path, queue, key, "handoff")
    assert _staged_bytes(composed, str(path)) == PAYLOAD
    assert hashlib.sha256(_staged_bytes(composed, str(path))).hexdigest() == \
        manifest["entries"][2]["sha256"]
    # EXPERIMENT: the replay's movers stage the head's entries again.
    composed = _run_mover(tmp_path, queue, key, "replay-0")
    assert _staged_bytes(composed, h0) == HEAD[0]
    composed = _run_mover(tmp_path, queue, key, "replay-1")
    assert _staged_bytes(composed, h1) == HEAD[1]


def test_an_ordinary_submission_declares_the_same_plan(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Placed by the submitter, the same plan is accepted and staged alike."""

    queue, work = _env(tmp_path, monkeypatch)
    consulted = _progress_worker(queue, monkeypatch)
    _announce_tier(queue, mountpoint=tmp_path / "stage")
    _template_, _producer, instance, path, committed = _committed(
        tmp_path, queue, "band-l")
    placed = _placed(tmp_path, queue, committed["ref"])
    source = _write(tmp_path / "placed.json", placed)

    key = str(_submit(work, monkeypatch, capsys, "--data-manifest", str(source),
                      "--residency", "stage", *PROGRESS,
                      command=("/bin/true",))["action_key"])
    assert consulted == ["v2"]
    manifest = _sealed_manifest(tmp_path, key)
    assert manifest == placed
    assert _reads(manifest) == _expected_reads(tmp_path, path)
    assert (po._consumers_dir(queue.root, instance, "b1")
            / f"{key}.json").exists(), "declared against the batch (#914)"

    _tier_cycle(queue, tmp_path / "stage")
    assert _staged_windows(queue, key, manifest) == _expected_reads(tmp_path, path)


def _rephased(manifest: dict, layout) -> dict:
    return {**manifest, "read_plan": _plan(manifest["entries"], layout)}


def test_a_submitted_plan_must_read_its_batches_exactly_at_their_slots(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    queue, work = _env(tmp_path, monkeypatch)
    _progress_worker(queue, monkeypatch)
    _t, producer, _instance, path, committed = _committed(tmp_path, queue, "band-l")
    ref = committed["ref"]
    placed = _placed(tmp_path, queue, ref)
    notes = placed["annotations"]

    def refused(manifest: dict, *progress: str) -> str:
        source = _write(tmp_path / "wrong.json", manifest)
        return _refused(monkeypatch, "--cwd", str(work), "--wait-s", "0.01",
                        "--detach", "--data-manifest", str(source),
                        *(progress or PROGRESS), "--", "/bin/true")

    reread = _rephased(placed, (("head", [0, 1]), ("handoff", [2]),
                                ("replay-0", [0]), ("replay-1", [1, 2])))
    assert "outside its slot" in refused(reread)

    elsewhere = {**placed, "annotations": {
        **notes, po.ORIGIN_SLOTS_ANNOTATION: [{"phase": "replay-0", "refs": [ref]}]}}
    assert "outside its slot" in refused(elsewhere)

    moved = _rephased(placed, (("head", [0, 1]), ("handoff", []),
                               ("replay-0", [0, 2]), ("replay-1", [1])))
    assert "outside its slot" in refused(moved)

    mixed = _rephased(placed, (("head", [0, 1]), ("handoff", [0, 2]),
                               ("replay-0", [0]), ("replay-1", [1])))
    assert "read its declared batches where its slots say" in refused(mixed)

    changed = json.loads(json.dumps(placed))
    changed["entries"][2]["sha256"] = "0" * 64
    assert "read its declared batches where its slots say" in refused(changed)

    unplaced = {**placed, "annotations": {
        key: value for key, value in notes.items()
        if key != po.ORIGIN_SLOTS_ANNOTATION}}
    assert po.ORIGIN_SLOTS_ANNOTATION in refused(unplaced)

    edge = f"{producer}:{_t['template_id']}"
    deferred_slot = {**placed, "annotations": {
        **notes, po.ORIGIN_SLOTS_ANNOTATION: [{"phase": "handoff", "after": edge}]}}
    assert "placed by the release" in refused(deferred_slot)

    assert not queue.dir(pool.READY).exists() or not [
        row for row in queue.dir(pool.READY).glob("*.json")
        if row.stem != producer], "nothing refused was queued"
    # Control: the placed manifest itself is accepted.
    assert _submit(work, monkeypatch, capsys, "--data-manifest",
                   str(_write(tmp_path / "placed.json", placed)), *PROGRESS,
                   command=("/bin/true",))["action_key"]


def test_a_deferred_plan_names_one_slot_per_edge(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    queue, work = _env(tmp_path, monkeypatch)
    consulted = _progress_worker(queue, monkeypatch)
    template = _template(tmp_path / "canonical")
    producer = _producer_key(tmp_path, template, "band-l")
    _publish_producer(queue, template, producer)
    _announce_tier(queue, mountpoint=tmp_path / "stage")
    edge = f"{producer}:{template['template_id']}"
    other = f"{fx._hexkey('another-producer')}:{template['template_id']}"

    def refused(manifest: dict, *options: str) -> str:
        source = _write(tmp_path / "static.json", manifest)
        return _refused(monkeypatch, "--cwd", str(work), "--wait-s", "0.01",
                        "--detach", "--after", edge, "--data-manifest",
                        str(source), *options, "--", "/bin/cat",
                        ae.DATA_MANIFEST_PLACEHOLDER)

    assert "must name the phase each --after edge fills" in refused(
        _static(tmp_path), *PROGRESS)
    assert "not an --after edge" in refused(
        _static(tmp_path, {"phase": "handoff", "after": other}), *PROGRESS)
    assert "already reads static entries" in refused(
        _static(tmp_path, {"phase": "head", "after": edge}), *PROGRESS)
    assert "exactly phase and after" in refused(
        _static(tmp_path, {"phase": "handoff", "after": edge, "refs": []}),
        *PROGRESS)
    assert po.ORIGIN_BATCHES_ANNOTATION in refused(
        _static(tmp_path, {"phase": "handoff", "after": edge},
                annotations={po.ORIGIN_BATCHES_ANNOTATION: []}), *PROGRESS)
    assert "linear progress reporting" in refused(
        _static(tmp_path, {"phase": "handoff", "after": edge}),
        *PROGRESS[:-2])
    # A plan that opens with a phase reading nothing has no first boundary to
    # stage up to; one that opens with its slot does.
    opens_idle = (("warmup", []), *PLAN)
    assert "opens the plan and reads nothing" in refused(
        _static(tmp_path, {"phase": "handoff", "after": edge}, layout=opens_idle),
        "--residency", "stage", "--progress-phase", "warmup=60", *PROGRESS)
    assert not list((queue.root / ae.DEFERRED_SUBDIR).glob("*.json")), (
        "nothing refused was filed")

    opens_with_slot = (("handoff", []), ("head", [0, 1]), ("replay-0", [0]),
                       ("replay-1", [1]))
    detach = _submit(
        work, monkeypatch, capsys, "--after", edge, "--data-manifest",
        str(_write(tmp_path / "static.json", _static(
            tmp_path, {"phase": "handoff", "after": edge},
            layout=opens_with_slot))),
        "--residency", "stage",
        *(arg for name, _ in opens_with_slot
          for arg in ("--progress-phase", f"{name}=600")))
    assert detach["status"] == "deferred" and "v2" in consulted


def test_a_v1_manifest_is_released_and_checked_as_before(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """The v1 path: batches after every static phase; slots are v2's only."""

    queue, work = _env(tmp_path, monkeypatch)
    template = _template(tmp_path / "canonical")
    producer = _producer_key(tmp_path, template, "band-l")
    _publish_producer(queue, template, producer)
    root = tmp_path / "static-src"
    root.mkdir()
    head = root / "h0.bin"
    head.write_bytes(HEAD[0])
    static = pb.validate_data_manifest({
        "schema": pb.DATA_MANIFEST_SCHEMA_V1, "produced_by": {"tool": "tests/946"},
        "mount_prefix": str(root),
        "entries": [{"path": str(head), "offset": 0, "bytes": len(HEAD[0]),
                     "sha256": None}],
        "entry_count": 1, "total_bytes": len(HEAD[0]),
        "annotations": {"phases": [{"name": "head", "bytes": len(HEAD[0]),
                                    "cumulative_bytes": len(HEAD[0])}]}})
    edge = f"{producer}:{template['template_id']}"
    detach = _submit(work, monkeypatch, capsys, "--after", edge,
                     "--data-manifest", str(_write(tmp_path / "v1.json", static)))
    assert detach["status"] == "deferred"

    instance = _start(queue, template, producer)
    path, committed = _commit(queue, template, instance, "b1", PAYLOAD)
    queue.finish(producer, status="executed")
    [released] = _released(dr.release_tick(queue))
    request = _request(tmp_path, released["action_key"])
    summary = request["params"]["data_manifest"]
    assert set(summary) == {"input", "mount_prefix", "entry_count", "total_bytes"}
    manifest, _ = pb.read_data_manifest(
        pb.PrismaBuildCAS(tmp_path / "cas").input_path(summary["input"]))
    assert manifest == ae.merged_manifest(
        static, po.origin_batch_manifest(queue.root, [committed["ref"]]))
    assert [entry["path"] for entry in manifest["entries"]] == [str(head), str(path)]

    # A v1 manifest has no read plan to place a batch in.
    slotted = {**static, "annotations": {
        **static["annotations"],
        po.ORIGIN_SLOTS_ANNOTATION: [{"phase": "head", "refs": [committed["ref"]]}]}}
    with pytest.raises(SystemExit, match="only a data_manifest.v2 has"):
        pbrun.require_declared_origin_batches(
            slotted, transport="pool", queue_root=queue.root)

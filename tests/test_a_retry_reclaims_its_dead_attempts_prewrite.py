"""A producer that died between its prewrite and its commit (#949).

A write-only producer (PQ's Stage B prep) prewrites its origin paths, writes
them, then commits them as an origin batch (#912).  If the attempt dies in
between, its prewrite record stays in its own instance directory and the
files it wrote belong to no batch.  A retry of the same key gets a new
nonce, so a new instance, and reuses the same batch id.

*   The retry prewrites the same paths and commits.  Path ownership is per
    attempt (#912), so nothing of the dead attempt's refuses it; its late
    commit is refused instead.  The batch holds the retry's bytes and
    sha256.  This part is #912's behaviour, tested here as a regression.
*   New: `origin_retirement_tick` sweeps the ended attempt's prewrite.  It
    drops the record when the files are absent, or when a sibling attempt's
    committed batch owns every one still present.  It keeps it while the
    attempt, or a sibling that plans the same paths, can still commit.
*   Otherwise the files belong to no batch.  PB never deletes a file whose
    identity no commit recorded, so it reports them once per change and
    lists them under ``orphaned_prewrites`` in ``pbstatus
    --blocked-origins`` until an operator removes them.

What the prewrite record costs: it is the attempt's reservation against its
own instance's durable maxima (`_outstanding_sums`).  No gate of another
attempt reads it, so the dead record blocked nothing.  The cost was a record
PB never released and files on the output prefix that nothing reported.

Fixture concessions, as in `test_consumed_origin_retirement`: owners are
published, claimed and finished through the real ``PoolQueue``, and a retry
is simulated by filing a fresh broker control on the owner's claim
(`fx._broker_control`), which is what a new attempt's claim carries.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_prepaid_writer_integration as fx  # noqa: E402
from prismabuild import pool, produced_output as po  # noqa: E402
import pbmcp  # noqa: E402
import pbstatus  # noqa: E402
from test_consumed_origin_retirement import (  # noqa: E402
    WORKER, _charged, _cycle_lines, _entry,
)
from test_write_only_produced_output import (  # noqa: E402
    TIER, _descriptor, _prewrite, _queue, _template,
)

RECLAIMED = po.ORIGIN_PREWRITE_RECLAIMED_EVENT
ORPHANED = po.ORIGIN_PREWRITE_ORPHANED_EVENT
PREWRITE_EVENTS = {RECLAIMED, ORPHANED, po.ORIGIN_RETIREMENT_REFUSED_EVENT}


def _first_attempt(queue: pool.PoolQueue, template: dict,
                   owner: str) -> tuple[dict, dict]:
    """Publish, claim and bind one owner with one attempt; return the claim too."""

    queue.publish(action_key=owner, cas_root="/cas", worker_script=WORKER,
                  checkout_root=str(Path(queue.root).parent / "mover-checkout"),
                  resources={"cpu": 1, "mem_gb": 1,
                             **po.owner_demand_terms(template)},
                  max_attempts=1, produced_output_template=template)
    claimed = queue.claim(owner="w-owner")
    assert claimed is not None and claimed["action_key"] == owner
    po.declare_template(queue.root, template)
    return _attempt(queue, template, owner, claimed), claimed


def _attempt(queue: pool.PoolQueue, template: dict, owner: str,
             claimed: dict) -> dict:
    """Bind a new attempt of ``owner``: a fresh broker control, a new nonce."""

    control = fx._broker_control(queue, owner)
    env = {"PRISMABUILD_ACTION_KEY": owner,
           "PRISMABUILD_ACTION_NONCE": control["nonce"],
           "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
    instance = po.bind_instance(queue, template, owner_action_key=owner,
                                claim_snapshot=claimed, env=env)
    po.declare_instance(queue.root, instance)
    assert po.admit_instance(queue, instance, template)["ok"] is True
    return instance


def _reserved(queue: pool.PoolQueue, instance: dict) -> int:
    """Committed plus outstanding payload bytes the instance's accounting holds."""

    return po._outstanding_sums(queue.root, instance, "")["payload"]


def _record(queue: pool.PoolQueue, instance: dict, batch_id: str = "b1") -> Path:
    return po._prewrites_dir(queue.root, instance) / f"{batch_id}.prewrite.json"


def _write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".partial")
    staged.write_bytes(payload)
    staged.replace(path)


def _class_bytes(size: int) -> dict[str, int]:
    return {"payload": size, "checkpoint": 0, "temp": 0}


def _nonce(instance: dict) -> str:
    return str(instance["owner_attempt"]["nonce"])


def _listed(queue: pool.PoolQueue) -> list[dict]:
    blob = pbstatus.read_blocked_origins(queue.root)
    assert blob["complete"] is True and blob["blocked"] == []
    return blob["orphaned_prewrites"]


# -- acceptance 1: the retry ---------------------------------------------------


def test_a_retry_commits_over_its_dead_attempts_prewrite(tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    owner = fx._hexkey("stage-b-prep")
    first, claimed = _first_attempt(queue, template, owner)
    path = Path(template["output_prefix"]) / "band-07.meta.json"
    v1 = b'{"attempt": 1, "partial": true}'
    assert _prewrite(queue, first, template, "b1", [path], len(v1))["ok"]
    _write(path, v1)
    # Attempt 1 dies here, before its commit.  The key is retried.

    second = _attempt(queue, template, owner, claimed)
    assert _nonce(second) != _nonce(first)
    assert po._producer_attempt_state(queue, first) == "dead"
    late = po.commit_origin_batch(
        queue, first, template, [_descriptor(first, template, path, v1)],
        batch_id="b1")
    assert late == {"ok": False, "refusal": "stale-superseded-owner"}

    v2 = b'{"attempt": 2, "rows": [1, 2, 3], "complete": true}'
    assert _prewrite(queue, second, template, "b1", [path], len(v2))["ok"], (
        "path ownership is per attempt: the dead prewrite does not refuse it")
    assert po.origin_retirement_tick(queue) == [], (
        "the retry plans the path and can still commit it, so the dead "
        "prewrite is held, silently")
    assert _record(queue, first).exists()
    assert _reserved(queue, first) == len(v1)

    _write(path, v2)
    committed = po.commit_origin_batch(
        queue, second, template, [_descriptor(second, template, path, v2)],
        batch_id="b1")
    assert committed["ok"], committed
    _filed, sealed = po._load_batch_record(
        queue.root, second, template, _entry(queue, second), "b1")
    assert [(d["path"], d["bytes"], d["sha256"]) for d in sealed] == [
        (str(path), len(v2), hashlib.sha256(v2).hexdigest())]

    events = po.origin_retirement_tick(queue)

    assert events == [{
        "event": RECLAIMED, "prewrite": f"{owner}/{template['template_id']}."
                                        f"{_nonce(first)}/b1",
        "class_bytes": _class_bytes(len(v1)), "reason": "superseded",
        "superseded": [{"path": str(path), "nonce": _nonce(second),
                        "batch_id": "b1"}]}]
    assert not _record(queue, first).exists()
    assert path.read_bytes() == v2
    assert (_reserved(queue, first), _charged(queue, first)) == (0, 0)
    assert _reserved(queue, second) == _charged(queue, second) == len(v2), (
        "the batch is charged once, to the attempt that committed it")
    assert po.origin_retirement_tick(queue) == []
    assert _listed(queue) == []


# -- acceptance 2: no retry ever commits ---------------------------------------


def test_files_no_attempt_committed_are_reported_until_removed(
        tmp_path: Path) -> None:
    """Reported by the tier cycle and listed, never deleted by PB."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    owner = fx._hexkey("never-retried")
    first, _claimed = _first_attempt(queue, template, owner)
    path = Path(template["output_prefix"]) / "band-08.meta.json"
    v1 = b'{"attempt": 1}'
    assert _prewrite(queue, first, template, "b1", [path], len(v1))["ok"]
    _write(path, v1)
    stage = tmp_path / "stage"

    def cycle() -> list[dict]:
        return [line for line in _cycle_lines(queue, stage)
                if line.get("event") in PREWRITE_EVENTS]

    assert cycle() == [], "a live attempt's prewrite is its own"
    queue.finish(owner, status="failed")
    assert po._producer_attempt_state(queue, first) == "dead"

    lines = cycle()
    key = f"{owner}/{template['template_id']}.{_nonce(first)}/b1"
    assert len(lines) == 1 and isinstance(lines.pop()["unix"], float)
    assert cycle() == [], "reported once per change"
    orphan = {"prewrite": key, "class_bytes": _class_bytes(len(v1)),
              "paths": [str(path)], "superseded": [], "held": []}
    assert _listed(queue) == [{**orphan,
                               "remedy": pbstatus.ORPHANED_PREWRITE_REMEDY}]
    session = pbmcp.Session(queue_root=queue.root, cas_root=tmp_path / "cas",
                            repo_link=tmp_path / "repo")
    body = session.call("pb_blocked_origins")
    assert body["census_complete"] is True
    assert body["orphaned_prewrites"] == _listed(queue)
    assert path.read_bytes() == v1, "PB does not delete it"
    assert _reserved(queue, first) == len(v1)

    path.unlink()
    lines = cycle()

    assert [{k: v for k, v in line.items() if k != "unix"} for line in lines] == [{
        "event": RECLAIMED, "prewrite": key,
        "class_bytes": _class_bytes(len(v1)), "reason": "absent",
        "superseded": []}]
    assert _reserved(queue, first) == 0
    assert not _record(queue, first).exists()
    assert _listed(queue) == [] and cycle() == []


def test_the_orphan_event_names_what_the_tick_found(tmp_path: Path) -> None:
    """The tick's own event, and a new report when what it found changes."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    owner = fx._hexkey("two-files")
    first, _claimed = _first_attempt(queue, template, owner)
    prefix = Path(template["output_prefix"])
    a, b = prefix / "a.bin", prefix / "b.bin"
    assert _prewrite(queue, first, template, "b1", [a, b], 8)["ok"]
    _write(a, b"aaaa")
    _write(b, b"bbbb")
    queue.finish(owner, status="failed")
    key = f"{owner}/{template['template_id']}.{_nonce(first)}/b1"

    assert po.origin_retirement_tick(queue) == [{
        "event": ORPHANED, "prewrite": key, "class_bytes": _class_bytes(8),
        "paths": [str(a), str(b)], "superseded": [], "held": []}]
    assert po.origin_retirement_tick(queue) == []
    a.unlink()
    assert po.origin_retirement_tick(queue) == [{
        "event": ORPHANED, "prewrite": key, "class_bytes": _class_bytes(8),
        "paths": [str(b)], "superseded": [], "held": []}]
    b.unlink()
    assert [e["reason"] for e in po.origin_retirement_tick(queue)] == ["absent"]


# -- what decides a disposition ------------------------------------------------


def test_a_path_the_retry_did_not_write_again_is_orphaned(tmp_path: Path) -> None:
    """A retry that commits fewer paths vouches only for those."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    owner = fx._hexkey("fewer-paths")
    first, claimed = _first_attempt(queue, template, owner)
    prefix = Path(template["output_prefix"])
    kept, dropped = prefix / "kept.bin", prefix / "dropped.bin"
    assert _prewrite(queue, first, template, "b1", [kept, dropped], 8)["ok"]
    _write(kept, b"one1")
    _write(dropped, b"one2")
    second = _attempt(queue, template, owner, claimed)
    assert _prewrite(queue, second, template, "b1", [kept], 4)["ok"]
    _write(kept, b"two1")
    assert po.commit_origin_batch(
        queue, second, template,
        [_descriptor(second, template, kept, b"two1")], batch_id="b1")["ok"]
    queue.finish(owner, status="executed")

    events = po.origin_retirement_tick(queue)

    by_kept = {"path": str(kept), "nonce": _nonce(second), "batch_id": "b1"}
    assert [(e["event"], e["paths"], e["superseded"], e["held"])
            for e in events] == [(ORPHANED, [str(dropped)], [by_kept], [])]
    assert _record(queue, first).exists()
    dropped.unlink()
    assert [(e["event"], e["reason"], e["superseded"])
            for e in po.origin_retirement_tick(queue)] == [
        (RECLAIMED, "superseded", [by_kept])]
    assert _reserved(queue, first) == 0
    assert _reserved(queue, second) == 4


def test_a_retry_that_also_died_vouches_for_nothing(tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    owner = fx._hexkey("twice-dead")
    first, claimed = _first_attempt(queue, template, owner)
    path = Path(template["output_prefix"]) / "band.bin"
    assert _prewrite(queue, first, template, "b1", [path], 4)["ok"]
    _write(path, b"one!")
    second = _attempt(queue, template, owner, claimed)
    assert _prewrite(queue, second, template, "b1", [path], 4)["ok"]
    _write(path, b"two!")
    third = _attempt(queue, template, owner, claimed)
    assert po._producer_attempt_state(queue, second) == "dead"

    events = po.origin_retirement_tick(queue)

    assert sorted((e["event"], e["prewrite"].split("/")[1], e["paths"], e["held"])
                  for e in events) == sorted(
        (ORPHANED, f"{template['template_id']}.{_nonce(dead)}", [str(path)], [])
        for dead in (first, second))
    assert _prewrite(queue, third, template, "b1", [path], 4)["ok"]
    assert po.origin_retirement_tick(queue) == [], (
        "held while the live retry plans the path, and the change is silent")
    _write(path, b"3rd!")
    assert po.commit_origin_batch(
        queue, third, template,
        [_descriptor(third, template, path, b"3rd!")], batch_id="b1")["ok"]
    assert sorted((e["event"], e["reason"])
                  for e in po.origin_retirement_tick(queue)) == [
        (RECLAIMED, "superseded"), (RECLAIMED, "superseded")]
    assert _reserved(queue, first) == _reserved(queue, second) == 0


def test_a_prewrite_is_kept_while_its_attempt_can_still_commit(
        tmp_path: Path) -> None:
    """Live and unknown keep it; an attempt that succeeded has ended."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    owner = fx._hexkey("still-running")
    first, _claimed = _first_attempt(queue, template, owner)
    Path(template["output_prefix"]).mkdir(parents=True)
    path = Path(template["output_prefix"]) / "never-written.bin"
    assert _prewrite(queue, first, template, "b1", [path], 4)["ok"]

    assert po._producer_attempt_state(queue, first) == "live"
    assert po.origin_retirement_tick(queue) == []
    assert _record(queue, first).exists()

    claim = queue.item_path(pool.CLAIMED, owner)
    live = pool._read_json(claim)
    assert live is not None
    control = dict(live["resource_scope"])
    live["resource_scope"] = {**control, "nonce": ""}
    pool._write_json_atomic(claim, live)
    assert po._producer_attempt_state(queue, first) == "unknown"
    assert po.origin_retirement_tick(queue) == []
    assert _record(queue, first).exists()

    live["resource_scope"] = control
    pool._write_json_atomic(claim, live)
    queue.finish(owner, status="executed")
    assert po._producer_attempt_state(queue, first) == "succeeded"
    assert [(e["event"], e["reason"]) for e in po.origin_retirement_tick(queue)
            ] == [(RECLAIMED, "absent")]
    assert _reserved(queue, first) == 0


def test_an_unreachable_output_prefix_is_never_read_as_absent(
        tmp_path: Path) -> None:
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    owner = fx._hexkey("unmounted-prewrite")
    first, _claimed = _first_attempt(queue, template, owner)
    path = Path(template["output_prefix"]) / "band.bin"
    assert _prewrite(queue, first, template, "b1", [path], 4)["ok"]
    _write(path, b"one!")
    queue.finish(owner, status="failed")
    prefix = Path(template["output_prefix"])
    parked = tmp_path / "parked"
    prefix.rename(parked)

    events = po.origin_retirement_tick(queue)

    assert [(e["event"], e["reason"]) for e in events] == [
        (po.ORIGIN_RETIREMENT_REFUSED_EVENT, "output-prefix-unreachable")]
    events = po.origin_retirement_tick(queue)
    assert [(event["event"], event["reason"]) for event in events] == [
        (po.ORIGIN_PREWRITE_RECLAIMED_EVENT, "absent")]
    assert not _record(queue, first).exists()
    assert _listed(queue) == []
    parked.rename(prefix)
    assert [e["event"] for e in po.origin_retirement_tick(queue)] == [ORPHANED]


# -- what one pass reads ------------------------------------------------------


def test_one_pass_reads_each_owner_key_and_sibling_set_once(
        tmp_path: Path, monkeypatch) -> None:
    """The coordinator's review rule: no state is read twice in one pass."""

    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    owner = fx._hexkey("one-read")
    first, claimed = _first_attempt(queue, template, owner)
    prefix = Path(template["output_prefix"])
    for batch_id in ("b1", "b2"):
        path = prefix / f"{batch_id}.bin"
        assert _prewrite(queue, first, template, batch_id, [path], 4)["ok"]
        _write(path, b"dead")
    second = _attempt(queue, template, owner, claimed)
    assert _prewrite(queue, second, template, "b3", [prefix / "b3.bin"], 4)["ok"]
    running = fx._hexkey("still-writing")
    other, _other_claim = _first_attempt(queue, template, running)
    assert _prewrite(queue, other, template, "b1", [prefix / "other.bin"], 4)["ok"]
    # A running producer's instance is never read: a read would report it.
    filed = po.instance_dir(queue.root, other) / "instance.json"
    filed.unlink()
    filed.write_text("{")

    reads: list[str] = []
    scans: list[str] = []
    key_generation = po._key_generation
    # The other attempts' paths are read through one tick's reads (#1053),
    # once per instance whose prewrites are decided.
    path_owners = po._TickReads.path_owners
    monkeypatch.setattr(po, "_key_generation", lambda q, key: (
        reads.append(key), key_generation(q, key))[1])
    monkeypatch.setattr(po._TickReads, "path_owners", lambda *args: (
        scans.append(_nonce(args[1])), path_owners(*args))[1])

    events = po.origin_retirement_tick(queue)

    assert sorted((e["event"], e["prewrite"].rsplit("/", 1)[1]) for e in events) == [
        (ORPHANED, "b1"), (ORPHANED, "b2")]
    assert (sorted(reads), scans) == (sorted([owner, running]), [_nonce(first)])
    reads.clear()
    scans.clear()
    listed = pbstatus.read_blocked_origins(queue.root)
    assert listed["complete"] is True
    assert sorted(item["prewrite"].rsplit("/", 1)[1]
                  for item in listed["orphaned_prewrites"]) == ["b1", "b2"]
    assert (sorted(reads), scans) == (sorted([owner, running]), [_nonce(first)])


# -- off the write-only path ---------------------------------------------------


def test_a_read_back_templates_prewrite_is_swept_too(tmp_path: Path) -> None:
    """A staged template's ended prewrite is swept like a write-only one (#1053).

    #949 swept only write-only templates, and R13's 28 staged prewrites
    stayed outstanding for ever. A staged template differs in one way: a
    pool funding intent may name the batch, and then the prewrite is its
    precommit authority and is held (`abort_prewrite` holds it the same
    way). With none, as here, an absent file frees the reservation.
    """

    template = fx._template(str(tmp_path / "canonical"))
    assert not po.is_write_only(template)
    queue = fx._queue(tmp_path)
    owner = fx._hexkey("read-back")
    first, claimed = _first_attempt(queue, template, owner)
    path = Path(template["output_prefix"]) / "p0.bin"
    pre = po.require_prewrite(queue, first, template, batch_id="b1",
                              tier=fx.TIER, class_bytes=_class_bytes(4),
                              paths=[str(path)])
    assert pre["ok"], pre
    _attempt(queue, template, owner, claimed)
    assert po._producer_attempt_state(queue, first) == "dead"
    path.parent.mkdir(parents=True, exist_ok=True)

    events = po.origin_retirement_tick(queue)
    assert [(event["event"], event["reason"]) for event in events] == [
        (po.ORIGIN_PREWRITE_RECLAIMED_EVENT, "absent")]
    assert not _record(queue, first).exists()
    assert _listed(queue) == []

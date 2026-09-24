"""A dead producer's staged batches and prewrites give their paths back (#1053).

Live shape, 2026-09-23: R13 Stage A (``03f50d8e390b``) died at chain-042.
Its instance holds 436 staged batches.  Ten read ``retired: false`` and
``origin_reclaimed: false``: their funding is ``consumed``, their movers are
done and hold no tier tokens, the map has no fragment for them, and their
staged copies are gone.  Its 28 outstanding prewrites name the at-43 plane
(1,792 ``.pt`` files, all present, and their ``.tmp`` names).  The relaunch is
a new action key on the same template and the same origin paths.

Three defects, each red here on main ``0e5a669e8f04``:

1.  Nothing retires such a batch.  The #929 sweep walks only token-holding
    movers, and the origin-retirement tick only ``origin_only`` batches.
2.  An ended attempt's prewrites are swept only for a write-only template
    (#949), so a staged template's stay outstanding for ever.
3.  Path ownership is checked within one owner key only, so a second action
    prewrites and commits a path another action's batch owns, with no
    refusal and no record.

Everything runs on a synthetic stage and origin prefix under ``tmp_path``,
registered to a queue under ``tmp_path``.  Nothing reads or writes a real
stage, origin or queue.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, residency_map  # noqa: E402
import prismabuild.produced_output as po  # noqa: E402
import stage_release  # noqa: E402

from test_prepaid_writer_integration import (  # noqa: E402
    KIND, REPO, TIER, _announce_tier, _broker_control, _claim_mover,
    _queue, _template,
)

#: The tick's events, spelled out: they are what an operator greps for.
DEAD_BATCH_RETIRED = "output-dead-producer-batch-retired"
PREWRITE_RECLAIMED = "output-prewrite-reclaimed"
PREWRITE_ORPHANED = "output-prewrite-orphaned"
#: A tier nothing waits on: nothing is evicted for pressure.
IDLE = {TIER: 0}


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    """No outer launch identity leaks in, and no report cache leaks across."""

    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT", "PRISMABUILD_ACTION_KEY"):
        monkeypatch.delenv(name, raising=False)
    stage_release.reset_holder_reports()
    po._UNFILED_REPORTS.clear()
    yield
    stage_release.reset_holder_reports()
    po._UNFILED_REPORTS.clear()


def _request(tmp_path: Path, cas_root: Path, template: dict, label: str) -> str:
    """Seal one producer request on ``template``; its key differs by ``label``.

    `test_prepaid_writer_integration._producer_request`, with the label in the
    command, so two producers of one template are two action keys: the R13
    relaunch is a new key on the same template and the same origin paths.
    """

    checkout = tmp_path / "mover-checkout"
    tools = checkout / "tools" / "fleet"
    tools.mkdir(parents=True, exist_ok=True)
    for name in ("stage_move.py", "prewarm_loop.py", "stage_release.py"):
        (tools / name).write_bytes((REPO / "tools" / "fleet" / name)
                                   .read_bytes())
    cas = pb.PrismaBuildCAS(cas_root)
    envelope = tmp_path / "produced-template.json"
    envelope.write_text(json.dumps(template, sort_keys=True))
    template_input, _ = cas.ingest_input(
        envelope, input_id=pb.PRODUCED_OUTPUT_TEMPLATE_INPUT_ID)
    declaration = po.build_declaration(template, template_input)
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/produced-1053-producer",
                 "definition_version": "v1", "task_class": "generation",
                 "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": ["/bin/true", label], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [template_input],
        "code_closure": pb.build_code_closure(
            checkout, ["tools/fleet/stage_move.py",
                       "tools/fleet/prewarm_loop.py",
                       "tools/fleet/stage_release.py"]),
        "params": {"cwd": ".", "command": ["/bin/true", label],
                   "produced_output_template": declaration},
        "environment": {"variables": {"PATH": "/usr/bin:/bin"},
                        "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    }
    action = pb.seal_action(body)
    cas.publish_action_request(action)
    return str(action["action_key"])


class _Fleet:
    """One queue, one stage, one staged template, and producers on it."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp = tmp_path
        self.cas_root = tmp_path / "cas"
        self.template = _template(str(tmp_path / "outputs"))
        self.prefix = Path(self.template["output_prefix"])
        self.prefix.mkdir(parents=True, exist_ok=True)
        self.q = _queue(tmp_path, gib=8)
        self.ledger = self.q.tier_ledger(TIER)
        self.stage = tmp_path / "stage"
        _announce_tier(self.q, self.stage)
        po.declare_template(self.q.root, self.template)

    def producer(self, label: str) -> "_Producer":
        return _Producer(self, label)

    def tick(self) -> list[dict]:
        return po.origin_retirement_tick(self.q)

    def sweep(self) -> list[dict]:
        return stage_release.sweep(
            self.q, stage_roots={TIER: str(self.stage)}, pressure=IDLE)


class _Producer:
    """One action key with one attempt: its failure is terminal."""

    def __init__(self, fleet: _Fleet, label: str) -> None:
        self.fleet = fleet
        q = fleet.q
        self.owner = _request(fleet.tmp, fleet.cas_root, fleet.template, label)
        q.publish(action_key=self.owner, cas_root=str(fleet.cas_root),
                  worker_script=str(REPO / "tools" / "prismabuild_worker.py"),
                  checkout_root=str(fleet.tmp / "mover-checkout"),
                  resources={"cpu": 1, "mem_gb": 1,
                             **po.owner_demand_terms(fleet.template)},
                  produced_output_template=fleet.template,
                  max_attempts=1, retry_safe=False)
        claimed = q.claim(owner=f"w-{label}")
        assert claimed is not None and claimed["action_key"] == self.owner
        control = _broker_control(q, self.owner)
        env = {"PRISMABUILD_ACTION_KEY": self.owner,
               "PRISMABUILD_ACTION_NONCE": control["nonce"],
               "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
        self.inst = po.bind_instance(q, fleet.template,
                                     owner_action_key=self.owner,
                                     claim_snapshot=claimed, env=env)
        po.declare_instance(q.root, self.inst)
        assert po.admit_instance(q, self.inst, fleet.template)["ok"] is True

    # -- writes ---------------------------------------------------------------

    def descriptor(self, path: Path, payload: bytes) -> dict:
        return po.validate_descriptor({
            "schema": po.DESCRIPTOR_SCHEMA_V2, "slot": "s0",
            "artifact_class": "payload", "path": str(path),
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "producer_generation": po.mint_generation(),
            "owner_action_key": self.inst["owner_action_key"],
            "owner_attempt": dict(self.inst["owner_attempt"]),
        }, self.fleet.template, self.inst)

    def prewrite(self, batch_id: str, paths: list[Path],
                 size: int) -> dict:
        return po.require_prewrite(
            self.fleet.q, self.inst, self.fleet.template, batch_id=batch_id,
            tier=TIER, class_bytes={"payload": size, "checkpoint": 0,
                                    "temp": 0},
            paths=[str(path) for path in paths])

    @staticmethod
    def write(path: Path, payload: bytes) -> None:
        """The producer's own write: a temporary, then ``rename`` onto the name."""

        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_bytes(payload)
        temporary.replace(path)

    def publish(self, batch_id: str, path: Path, payload: bytes) -> dict:
        """Prewrite, write and commit one prepaid staged batch of one file."""

        pre = self.prewrite(batch_id, [path], len(payload))
        assert pre.get("ok") is True, pre
        self.write(path, payload)
        res = po.publish_prepaid_batch(
            self.fleet.q, self.inst, self.fleet.template,
            [self.descriptor(path, payload)], batch_id=batch_id, tier=TIER,
            cas_root=self.fleet.cas_root, producer_action_key=self.owner,
            command_extra=["--unpaced"])
        assert res.get("ok") is True, res
        return res

    # -- the R13 state --------------------------------------------------------

    def strand_like_r13(self, batch_id: str, res: dict, path: Path,
                        payload: bytes) -> None:
        """Mover done, tokens and fragment gone, copy gone, batch unretired.

        The mover copies, publishes its fragment and receipt, and finishes.
        Then an eviction that is not the producer's own ``retire_batch``
        takes the copy back: it releases the tokens and drops the fragment,
        and the batch record is never told.  That is the state R13's ten
        batches were left in.
        """

        q = self.fleet.q
        mover = str(res["mover_key"])
        namespace = str(res["batch_namespace"])
        claimed = _claim_mover(q, f"w-mover-{batch_id}")
        assert claimed["action_key"] == mover
        staged = self.fleet.stage / "produced-output" / namespace / path.name
        staged.parent.mkdir(parents=True, exist_ok=True)
        staged.write_bytes(payload)
        residency_map.write_fragment(
            po.output_fragment_root(q.root / pool.RESIDENCY), {
                "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
                "consumer_action_key": namespace,
                "mover_action_key": mover, "tier_id": TIER,
                "stage_root": str(self.fleet.stage),
                "manifest_sha256": "a" * 64,
                "entries": {residency_map.residency_map_key(str(path), 0): {
                    "stage_path": str(staged), "bytes": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "offset": 0}}})
        q.record_move(mover, {
            "consumer_action_key": namespace, "tier_id": TIER,
            "stage_root": str(self.fleet.stage), "manifest_sha256": "a" * 64,
            "range_start_bytes": 0, "range_end_bytes": len(payload),
            "bytes_staged": len(payload), "complete": True})
        q.finish(mover, status="executed")
        receipt = stage_release.evict(
            q, mover, consumer_action_key=namespace,
            stage_root=str(self.fleet.stage),
            residency_root=po.output_fragment_root(q.root / pool.RESIDENCY))
        assert receipt.get("complete") is True, receipt
        assert self.fleet.ledger.holder_tokens(mover) == {}
        assert not staged.exists()
        funding = q.read_output_funding(mover, TIER)
        assert funding is not None and funding["state"] == "consumed"
        entry = self.entry(batch_id)
        assert entry.get("retired") is False
        assert entry.get("origin_reclaimed") is False
        assert path.read_bytes() == payload

    def fail(self) -> None:
        self.fleet.q.finish(self.owner, status="failed",
                            detail={"returncode": 1})
        assert po._producer_attempt_state(self.fleet.q, self.inst) == "dead"

    # -- reads ----------------------------------------------------------------

    def entry(self, batch_id: str) -> dict:
        return po._read_commitments(
            po._commitments_path(self.fleet.q.root, self.inst)
        )["batches"][batch_id]

    def prewrite_record(self, batch_id: str) -> Path:
        return (po._prewrites_dir(self.fleet.q.root, self.inst)
                / f"{batch_id}.prewrite.json")

    def coordinates(self, batch_id: str) -> str:
        return (f"{self.owner}/{self.fleet.template['template_id']}."
                f"{self.inst['owner_attempt']['nonce']}/{batch_id}")


def _events(events: list[dict], name: str) -> list[dict]:
    return [event for event in events if event.get("event") == name]


# -- defect 1: a dead producer's no-token batch ---------------------------------


def test_a_dead_producers_no_token_batch_is_retired_and_keeps_its_origin(
        tmp_path: Path) -> None:
    """The R13 shape is retired by the tier cycle; its origin file stays."""

    fleet = _Fleet(tmp_path)
    dead = fleet.producer("r13")
    path = fleet.prefix / "entries" / "cotangent-0-448-at-44.pt"
    payload = b"cotangent at 44, written by the dead attempt"
    res = dead.publish("b44p0-g7", path, payload)
    dead.strand_like_r13("b44p0-g7", res, path, payload)
    dead.fail()

    events = fleet.sweep() + fleet.tick()

    assert dead.entry("b44p0-g7").get("retired") is True, (
        "a dead producer's batch whose mover ended and holds no tokens is "
        "never retired: neither the #929 sweep nor the tick reaches it")
    retired = _events(events, DEAD_BATCH_RETIRED)
    assert [event.get("batch") for event in retired] == [
        dead.coordinates("b44p0-g7")], events
    assert retired[0].get("origin_kept") is True
    assert dead.entry("b44p0-g7").get("origin_reclaimed") is False
    assert path.read_bytes() == payload, "retirement never deletes an origin"
    # Settled: a second pass has nothing left to do.
    assert not _events(fleet.sweep() + fleet.tick(), DEAD_BATCH_RETIRED)


# -- defect 2: an ended attempt of a staged template ----------------------------


def test_an_ended_staged_attempts_prewrites_are_swept(tmp_path: Path) -> None:
    """Absent files free the reservation; present ones are reported."""

    fleet = _Fleet(tmp_path)
    dead = fleet.producer("r13")
    absent = fleet.prefix / "entries" / "cotangent-0-0-at-43.pt"
    present = fleet.prefix / "entries" / "cotangent-0-64-at-43.pt"
    assert dead.prewrite("b43p0-g0", [absent, Path(f"{absent}.tmp")],
                         64)["ok"] is True
    assert dead.prewrite("b43p0-g1", [present, Path(f"{present}.tmp")],
                         64)["ok"] is True
    dead.write(present, b"at-43, never committed")
    assert not po.is_write_only(fleet.template)
    dead.fail()

    events = fleet.tick()

    reclaimed = _events(events, PREWRITE_RECLAIMED)
    assert [(event.get("prewrite"), event.get("reason"))
            for event in reclaimed] == [
        (dead.coordinates("b43p0-g0"), "absent")], (
        "an ended attempt of a staged template keeps its prewrite for ever")
    assert not dead.prewrite_record("b43p0-g0").exists()
    orphaned = _events(events, PREWRITE_ORPHANED)
    assert [(event.get("prewrite"), event.get("paths"))
            for event in orphaned] == [
        (dead.coordinates("b43p0-g1"), [str(present)])], events
    assert orphaned[0].get("remedy"), "an orphaned prewrite names its remedy"
    assert dead.prewrite_record("b43p0-g1").exists()
    assert present.read_bytes() == b"at-43, never committed"


# -- defect 3: ownership across two actions -------------------------------------


def test_a_live_actions_batch_path_refuses_another_actions_prewrite(
        tmp_path: Path) -> None:
    """One origin path, one live writer, whichever action asks."""

    fleet = _Fleet(tmp_path)
    first = fleet.producer("first")
    second = fleet.producer("second")
    path = fleet.prefix / "entries" / "cotangent-0-448-at-44.pt"
    first.publish("b44p0-g7", path, b"the first action's committed bytes")
    assert first.entry("b44p0-g7").get("retired") is False

    refused = second.prewrite("b44p0-g7", [path, Path(f"{path}.tmp")], 64)

    assert refused.get("ok") is False, (
        "a second action prewrites a path the first action's unretired "
        f"batch owns, with no refusal: {refused}")
    assert refused.get("refusal") == "prewrite-path-owned-by-live-action"
    assert refused.get("path") == str(path)
    assert refused.get("owner_action_key") == first.owner
    assert refused.get("owner_batch_id") == "b44p0-g7"
    assert not second.prewrite_record("b44p0-g7").exists()


def test_a_successor_supersedes_a_dead_actions_batch_on_the_record(
        tmp_path: Path) -> None:
    """The relaunch writes the dead batch's path; the record says so."""

    fleet = _Fleet(tmp_path)
    dead = fleet.producer("r13")
    path = fleet.prefix / "entries" / "cotangent-0-448-at-44.pt"
    first = b"cotangent at 44, written by the dead attempt"
    res = dead.publish("b44p0-g7", path, first)
    dead.strand_like_r13("b44p0-g7", res, path, first)
    dead.fail()

    successor = fleet.producer("r13-resume")
    regenerated = b"cotangent at 44, regenerated by the relaunch"
    successor.publish("b44p0-g7", path, regenerated)
    assert path.read_bytes() == regenerated

    events = fleet.sweep() + fleet.tick()

    assert dead.entry("b44p0-g7").get("retired") is True, (
        "a second action committed a path a dead action's unretired batch "
        "owns, and nothing records it")
    retired = _events(events, DEAD_BATCH_RETIRED)
    assert len(retired) == 1, events
    superseded = retired[0].get("superseded")
    assert [(item.get("path"), item.get("owner_action_key"),
             item.get("batch_id")) for item in superseded or []] == [
        (str(path), successor.owner, "b44p0-g7")], retired
    assert path.read_bytes() == regenerated, "the successor's file stays"

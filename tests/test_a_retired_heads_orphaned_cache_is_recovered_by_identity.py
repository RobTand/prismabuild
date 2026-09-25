"""Bounded recovery of a retired head's unprovable cache copies.

Routine ``reconcile`` cannot reach these.  It decides ownership from the
``user.pbstage.source`` mark, and ``stage_move`` sets that same mark on every
file it publishes, so a stage copy always reads as prewarm-owned and lands in
``unowned_left`` forever.  A head whose fragment and material are gone leaves
bytes no document proves and no sweep may touch, and every later head then pays
the 30 s publisher grace once per entry for them.

``recover_orphaned_range`` asks by identity instead, and takes no scope from
its caller: the consumer, tier, stage root, manifest and exact range are read
off the head's own filed move receipt, cross-checked against the sealed CAS
request -- read as an action and validated under its own key, never as a bare
JSON load -- and corroborated by the egress receipt that retired it.

These pin the asymmetry the repair is built on -- **over-retaining is a pass
and over-removing is a failure** -- so every case that is not provably eligible
survives, and every refusal fails *before* anything is unlinked, with the
qualified prefix and the originals asserted intact.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, residency_map  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
CONSUMER = "2" * 64
LIVE_MOVER = "3" * 64
LIVE_CONSUMER = "4" * 64
NAMES = ["alpha.bin", "beta.bin", "gamma.bin", "delta.bin"]
SIZE = 1024
SPAN = SIZE * len(NAMES)


def _staged(stage: Path, name: str) -> Path:
    """Where the stage mover writes source ``name``: its range name.

    Derived through ``stage_move.stage_relative`` rather than spelled here, so
    this file follows the naming rule instead of restating it.
    """

    return stage / stage_move.stage_relative(
        f"/originals/{name}", 0, SIZE, mount_prefix="/originals")


def _manifest_blob(cas_root: Path, mount: Path) -> str:
    """A real v1 manifest in the CAS, addressed by its own digest."""

    manifest = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {}, "annotations": {},
        "mount_prefix": str(mount),
        "entry_count": len(NAMES), "total_bytes": SPAN,
        "entries": [{"path": f"{mount}/{name}", "offset": 0,
                     "bytes": SIZE, "sha256": None} for name in NAMES],
    }
    raw = json.dumps(manifest, sort_keys=True).encode()
    digest = hashlib.sha256(raw).hexdigest()
    blob = pb.PrismaBuildCAS(cas_root).blob_path(digest)
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.write_bytes(raw)
    return digest


def _head_command(stage: Path, *, start: int = 0, end: int = SPAN,
                  consumer: str = CONSUMER,
                  tier: str = TIER) -> list[str]:
    """The head request's real flag set: it alone carries tier and range."""

    return ["pbrun.py", "--tool", "prewarm-head",
            "--consumer-action-key", consumer, "--tier-id", tier,
            "--stage-root", str(stage),
            "--range-start-bytes", str(start), "--range-end-bytes", str(end)]


def _egress_command(stage: Path, *, mover: str,
                    consumer: str = CONSUMER) -> list[str]:
    """The egress names the head as its mover and carries no scope of its own."""

    return ["pbrun.py", "--tool", "prewarm-egress",
            "--mover-action-key", mover,
            "--consumer-action-key", consumer, "--stage-root", str(stage)]


def _seal_action(cas: pb.PrismaBuildCAS, checkout: Path, *, command: object,
                 digest: str, mount: Path) -> str:
    """One real sealed v2 movement action, published under its own key.

    ``pb.seal_action`` and ``PrismaBuildCAS.publish_action_request`` are the
    same helpers every other test seals small actions with.  A hand-written
    request body wearing a chosen key is not a sealed request -- it is the
    forgery the recovery has to refuse, so only the real path gives the
    fixture a key its bytes actually hash to, which is what lets the variant
    tests below change what a request *seals* instead of pretending to.
    """

    manifest_input = {"id": pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID,
                      "sha256": digest, "bytes": 1024}
    params: dict[str, object] = {
        "cwd": ".",
        "demand": {"mem_gb": 1, f"stage_gib@{TIER}": 1},
        "placement": {"required_tags": ["dl380g10"]},
        "data_manifest": {"input": manifest_input,
                          "mount_prefix": str(mount),
                          "entry_count": len(NAMES), "total_bytes": SPAN},
    }
    if command is not None:
        params["command"] = command
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            "definition_id": "fleet/pbrun", "definition_version": "v1",
            "task_class": "generation", "determinism": "deterministic",
            "artifact_family": "generic", "artifact_kind": "generic",
            "argv": ["/usr/bin/env", "python3", "tools/fleet/stage_move.py"],
            "working_directory": ".", "result_path": "stage.log",
        },
        "inputs": [manifest_input],
        "code_closure": pb.build_code_closure(checkout, ["task_code.py"]),
        "params": params,
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {
            "portability": "portable", "platform_key": None, "host_class": None,
        },
    })
    cas.publish_action_request(action)
    return str(action["action_key"])


def _file_receipts(queue: pool.PoolQueue, stage: Path, digest: str, *,
                   head: str, egress: str, entries: int = len(NAMES),
                   end: int = SPAN) -> None:
    # The real field shapes, read off pb-queue/movers/ for an adopted head
    # and its egress.  The asymmetry is the point: the head carries tier,
    # manifest and range; the egress carries none of them, so nothing here
    # may require them of it.
    queue.record_move(head, {
        "schema": pool.POOL_MOVE_SCHEMA_V1, "action_key": head,
        "consumer_action_key": CONSUMER, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": digest,
        "range_start_bytes": 0, "range_end_bytes": end, "range_bytes": end,
        "entries_declared": entries, "entries_staged": entries,
        "bytes_staged": SPAN, "bytes_copied": 0, "phase": "head",
        "adopted_from": "d" * 64, "adopted_from_consumer": "e" * 64,
        "host": "dl380g10", "unix": 1.0, "complete": True,
    })
    queue.record_move(egress, {
        "schema": pool.POOL_MOVE_SCHEMA_V1, "action_key": egress,
        "consumer_action_key": CONSUMER, "stage_root": str(stage),
        "reason": "egress", "retiring": False,
        "entries_shared": entries, "entries_deleted": 0,
        "entries_already_gone": 0, "entries_deferred": 0,
        "bytes_shared": SPAN, "bytes_deleted": 0,
        "shared_with": [], "live_pins": [], "auto_reclaimed": [],
        "auto_retained": {}, "tokens_released": 0, "tokens_decharged": 0,
        "errors": [], "host": "dl380g10", "unix": 2.0, "complete": True,
    })


@pytest.fixture()
def fleet(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    cas_root = tmp_path / "cas"
    cas_root.mkdir()
    cas = pb.PrismaBuildCAS(cas_root)
    originals = tmp_path / "originals"
    originals.mkdir()
    for name in NAMES:
        (originals / name).write_bytes(b"\1" * SIZE)
    checkout = tmp_path / "movement-checkout"
    checkout.mkdir()
    (checkout / "task_code.py").write_text("# movement closure member\n")
    digest = _manifest_blob(cas_root, originals)
    head = _seal_action(cas, checkout, command=_head_command(stage),
                        digest=digest, mount=originals)
    egress = _seal_action(cas, checkout,
                          command=_egress_command(stage, mover=head),
                          digest=digest, mount=originals)
    _file_receipts(queue, stage, digest, head=head, egress=egress)
    stage_release._manifest_layout_cache.clear()
    return queue, stage, cas_root, digest, originals, checkout, head, egress


def _recover(fleet, *, apply: bool = False, **kwargs):
    queue, stage, cas, _digest, _originals, _co, head, egress = fleet
    params = {"stage_root": str(stage), "head_action_key": head,
              "egress_action_key": egress, "cas_root": str(cas),
              "apply": apply}
    params.update(kwargs)
    got = stage_release.recover_orphaned_range(queue, **params)
    print(json.dumps(got, indent=1, sort_keys=True, default=str))
    return got


#: "No override" for :func:`_reseal` -- distinct from ``None``, which is a
#: real variant here: it seals a request whose params carry no command at all.
_UNSET = object()


def _reseal(fleet, *, seal_digest: str | None = None,
            receipt_digest: str | None = None, mount: Path | None = None,
            head_command: object = _UNSET, egress_command: object = _UNSET,
            entries: int = len(NAMES), end: int = SPAN) -> tuple[str, str]:
    """Reseal the pair with variant authority and refile both receipts.

    A scope-crossing test changes what a request *seals*, never the filed
    receipt that cross-checks it, so the variant action is sealed for real
    under its own derived key and the receipts are refiled under that key
    exactly as history would have filed them.  ``egress_command`` may be a
    callable taking the sealed head key, so an egress variant can still name
    the head it retires.
    """

    queue, stage, cas_root, base, originals, checkout, _head, _egress = fleet
    cas = pb.PrismaBuildCAS(cas_root)
    seal_digest = base if seal_digest is None else seal_digest
    receipt_digest = base if receipt_digest is None else receipt_digest
    mount = originals if mount is None else mount
    head_cmd = (_head_command(stage) if head_command is _UNSET
                else head_command)
    head_key = _seal_action(cas, checkout, command=head_cmd,
                            digest=seal_digest, mount=mount)
    egress_cmd = (_egress_command(stage, mover=head_key)
                  if egress_command is _UNSET
                  else (egress_command(head_key)
                        if callable(egress_command) else egress_command))
    egress_key = _seal_action(cas, checkout, command=egress_cmd,
                              digest=seal_digest, mount=mount)
    _file_receipts(queue, stage, receipt_digest, head=head_key,
                   egress=egress_key, entries=entries, end=end)
    stage_release._manifest_layout_cache.clear()
    return head_key, egress_key


def _populate(stage: Path) -> None:
    for name in NAMES:
        _stage_copy(stage, name)


def _intact(fleet) -> bool:
    """Nothing staged was unlinked and no original was touched."""

    _queue, stage, _cas, _digest, originals, *_rest = fleet
    return (all(_staged(stage, name).exists() for name in NAMES)
            and all((originals / name).exists() for name in NAMES))


def _stage_copy(stage: Path, name: str, *, marked: bool = True) -> Path:
    path = _staged(stage, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\0" * SIZE)
    if marked:
        os.setxattr(path, prewarm_loop.STAGE_SOURCE_XATTR,
                    f"/originals/{name}@0".encode())
    return path


def _live_fragment(queue: pool.PoolQueue, stage: Path, names: list[str],
                   digest: str) -> None:
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": LIVE_CONSUMER, "mover_action_key": LIVE_MOVER,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": digest,
        "entries": {
            residency_map.residency_map_key(str(_staged(stage, name)), 0): {
                "stage_path": str(_staged(stage, name)), "bytes": SIZE,
                "sha256": "b" * 64, "offset": 0,
            } for name in names
        },
    })


# --- the positive case ------------------------------------------------------

def test_a_retired_heads_unprovable_copies_are_retired(fleet) -> None:
    _queue, stage, _cas, _digest, originals, *_rest = fleet
    _populate(stage)

    dry = _recover(fleet)
    assert dry["complete"] and dry["applied"] is False
    assert dry["entries_eligible"] == len(NAMES)
    assert dry["bytes_eligible"] == SPAN
    assert dry["entries_retired"] == 0 and dry["bytes_retired"] == 0, (
        "a report must not read as bytes already deleted")
    assert _intact(fleet), "a report-only pass unlinks nothing"

    got = _recover(fleet, apply=True)
    assert got["complete"] and got["applied"] is True
    assert got["entries_retired"] == len(NAMES)
    assert got["bytes_retired"] == SPAN
    assert not any(_staged(stage, name).exists() for name in NAMES)
    assert got["originals_checked"] == got["originals_present"] == len(NAMES)
    assert all((originals / name).exists() for name in NAMES)


def test_an_egress_receipt_filed_with_its_own_schema_is_evidence(
        fleet) -> None:
    """``stage_release`` files its receipt as ``POOL_EGRESS_SCHEMA_V1`` and
    ``record_move`` keeps it (#1158); the fixture's ``pool_move.v1`` egress
    is the shape filed before that.  Both are the egress's evidence."""

    queue, stage, *_rest = fleet
    _populate(stage)
    key = fleet[7]
    record = json.loads(queue.move_path(key).read_text())
    queue.record_move(key, {**record, "schema": pool.POOL_EGRESS_SCHEMA_V1})
    assert queue.move_record(key)["schema"] == pool.POOL_EGRESS_SCHEMA_V1

    got = _recover(fleet, apply=True)
    assert got["complete"] and got["entries_retired"] == len(NAMES)


def test_the_recovery_is_idempotent(fleet) -> None:
    _queue, stage, *_rest = fleet
    _populate(stage)

    first = _recover(fleet, apply=True)
    second = _recover(fleet, apply=True)
    assert first["entries_retired"] == len(NAMES)
    assert second["entries_retired"] == 0
    assert second["entries_already_gone"] == len(NAMES)
    assert second["bytes_retired"] == 0 and second["complete"]


# --- retention --------------------------------------------------------------

def test_a_qualified_prefix_a_fragment_names_is_preserved(fleet) -> None:
    """Every fragment retains, wanted or not -- withdrawn movers' too."""

    queue, stage, _cas, digest, _originals, *_rest = fleet
    _populate(stage)
    kept = NAMES[:2]
    _live_fragment(queue, stage, kept, digest)

    got = _recover(fleet, apply=True)
    assert got["entries_retired"] == len(NAMES) - len(kept)
    assert got["retained_reasons"] == {
        "attributed_pinned_claimed_or_handed_off": 2}
    assert all(_staged(stage, name).exists() for name in kept)


@pytest.mark.parametrize("census", [
    "reader_lease.live_for", "_claimed_paths", "_claimed_source_paths"])
def test_every_ownership_census_retains_what_it_names(
        fleet, monkeypatch, census: str) -> None:
    """A pin, a claim in flight and a promotion handoff each retain."""

    _queue, stage, _cas, _digest, _originals, *_rest = fleet
    _populate(stage)
    pinned = os.path.normpath(str(_staged(stage, NAMES[0])))
    held: object = ({pinned: [LIVE_CONSUMER]}
                    if census.endswith("live_for")
                    else {str(_staged(stage, NAMES[0]).relative_to(stage))}
                    if census == "_claimed_paths"
                    else {pinned})
    if "." in census:
        module, name = census.split(".")
        monkeypatch.setattr(getattr(stage_release, module), name,
                            lambda *a, **k: (held, []))
    else:
        monkeypatch.setattr(stage_release, census, lambda *a, **k: (held, []))

    got = _recover(fleet, apply=True)
    assert _staged(stage, NAMES[0]).exists()
    assert got["entries_retired"] == len(NAMES) - 1


def test_a_file_the_stage_did_not_write_is_retained(fleet) -> None:
    """The source mark is a necessary condition, never permission."""

    _queue, stage, *_rest = fleet
    _stage_copy(stage, NAMES[0], marked=False)
    for name in NAMES[1:]:
        _stage_copy(stage, name)

    got = _recover(fleet, apply=True)
    assert _staged(stage, NAMES[0]).exists()
    assert got["retained_reasons"]["not_marked_by_the_stage"] == 1


def test_a_pre_range_bare_name_is_retained_and_named(fleet) -> None:
    """A copy under the bare name a pre-range head wrote is never retired.

    Before range-only naming every read of a path from offset zero shared the
    bare relative name, so a bare copy is not this head's identity even when
    the stage marked it.  It is retained with its own reason rather than
    counted as already gone, and nothing else in the scope is held back.
    """

    _queue, stage, *_rest = fleet
    legacy = stage / NAMES[0]
    legacy.write_bytes(b"\0" * SIZE)
    os.setxattr(legacy, prewarm_loop.STAGE_SOURCE_XATTR,
                f"/originals/{NAMES[0]}@0".encode())
    for name in NAMES[1:]:
        _stage_copy(stage, name)

    got = _recover(fleet, apply=True)
    assert legacy.exists()
    assert got["retained_reasons"] == {
        "pre_range_name_not_this_heads_identity": 1}
    assert got["entries_retired"] == len(NAMES) - 1
    assert got["entries_already_gone"] == 0


def test_an_unanswerable_mark_retains_rather_than_deletes(
        fleet, monkeypatch) -> None:
    _queue, _stage, _cas, _digest, _originals, *_rest = fleet
    _populate(fleet[1])

    def unanswerable(*_args, **_kwargs):
        raise OSError(95, "not supported")

    monkeypatch.setattr(os, "getxattr", unanswerable)
    got = _recover(fleet, apply=True)
    assert got["entries_retired"] == 0 and _intact(fleet)


# --- the fragment census is evidence, not noise ------------------------------

def test_a_fragment_that_cannot_be_validated_refuses_the_whole_pass(
        fleet) -> None:
    """A bad fragment taints the census; it never reads as unowned.

    A real corrupt file under the real residency root, not a monkeypatch of
    a high-level wrapper: the map-composition reader deliberately skips a
    fragment that cannot be read or validated, and a recovery that reused
    that tolerance would delete exactly the bytes the unreadable fragment
    may have been vouching for.
    """

    queue, stage, *_rest = fleet
    _populate(stage)
    corrupt = (queue.root / pool.RESIDENCY / LIVE_CONSUMER
               / f"{LIVE_MOVER}.json")
    corrupt.parent.mkdir(parents=True)
    corrupt.write_text('{"schema": "prismaquant.prismabuild.residency_map')

    got = _recover(fleet, apply=True)
    assert got["complete"] is False
    assert "fragment census unreadable" in str(got["skipped"])
    assert _intact(fleet)


def test_a_residency_root_that_cannot_be_listed_refuses(
        fleet, tmp_path: Path) -> None:
    """A residency root that cannot be scanned is unknown, not empty."""

    _queue, stage, *_rest = fleet
    _populate(stage)
    occupied = tmp_path / "residency-occupied-by-a-file"
    occupied.write_text("not a residency root")

    got = _recover(fleet, apply=True, residency_root=occupied)
    assert got["complete"] is False
    assert "fragment census unreadable" in str(got["skipped"])
    assert _intact(fleet)


# --- refusals: each must fail BEFORE anything is unlinked -------------------

@pytest.mark.parametrize("census", [
    "reader_lease.live_for", "_claimed_paths", "_claimed_source_paths"])
def test_an_unreadable_census_refuses_before_mutating(
        fleet, monkeypatch, census: str) -> None:
    """Fail closed: an unreadable census is not an absence of ownership."""

    _queue, _stage, _cas, _digest, _originals, *_rest = fleet
    _populate(fleet[1])
    empty: object = {} if census.endswith("live_for") else set()
    if "." in census:
        module, name = census.split(".")
        monkeypatch.setattr(getattr(stage_release, module), name,
                            lambda *a, **k: (empty, ["unreadable"]))
    else:
        monkeypatch.setattr(stage_release, census,
                            lambda *a, **k: (empty, ["unreadable"]))

    got = _recover(fleet, apply=True)
    assert got["complete"] is False and got["entries_retired"] == 0
    assert _intact(fleet)


def test_a_missing_original_refuses_before_mutating(fleet) -> None:
    """The staged copy must never be the last surviving input."""

    _queue, stage, _cas, _digest, originals, *_rest = fleet
    _populate(stage)
    (originals / NAMES[0]).unlink()

    got = _recover(fleet, apply=True)
    assert got["complete"] is False
    assert "last surviving input" in str(got["skipped"])
    assert got["entries_retired"] == 0
    assert all(_staged(stage, name).exists() for name in NAMES)


def test_a_directory_original_is_refused(fleet) -> None:
    """``exists`` alone would pass a directory off as a surviving input."""

    _queue, stage, _cas, _digest, originals, *_rest = fleet
    _populate(stage)
    (originals / NAMES[0]).unlink()
    (originals / NAMES[0]).mkdir()

    got = _recover(fleet, apply=True)
    assert got["complete"] is False
    assert "not a regular source file" in str(got["skipped"])
    assert all(_staged(stage, name).exists() for name in NAMES)
    assert (originals / NAMES[0]).is_dir(), "the refusal touched nothing"


def test_a_truncated_original_is_refused(fleet) -> None:
    """A source shrunken below its manifest extent is no surviving input."""

    _queue, stage, _cas, _digest, originals, *_rest = fleet
    _populate(stage)
    short = originals / NAMES[1]
    short.write_bytes(b"\1" * (SIZE - 1))

    got = _recover(fleet, apply=True)
    assert got["complete"] is False
    assert "short of" in str(got["skipped"])
    assert all(_staged(stage, name).exists() for name in NAMES)
    assert short.stat().st_size == SIZE - 1, "the refusal touched nothing"


def test_an_original_that_resolves_into_the_stage_is_refused(
        fleet, tmp_path: Path) -> None:
    """An 'original' aliased into the stage is not a separate input."""

    queue, stage, cas, _digest, _originals, *_rest = fleet
    _populate(stage)
    aliased = tmp_path / "aliased"
    aliased.mkdir()
    for name in NAMES:
        (aliased / name).symlink_to(_staged(stage, name))
    digest = _manifest_blob(cas, aliased)
    head, egress = _reseal(fleet, seal_digest=digest,
                           receipt_digest=digest, mount=aliased)

    got = _recover(fleet, apply=True, head_action_key=head,
                   egress_action_key=egress)
    assert got["complete"] is False
    assert "inside the stage root" in str(got["skipped"])
    assert all(_staged(stage, name).exists() for name in NAMES)


def test_nothing_happens_while_a_mover_could_be_writing(fleet) -> None:
    queue, stage, *_rest = fleet
    _populate(stage)
    queue.publish(action_key=LIVE_MOVER, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root), worker_script="w.py",
                  tags=["dl380g10"],
                  resources={"cpu": 1, "mem_gb": 1, f"stage_gib@{TIER}": 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "manifest_sha256": "a" * 64,
                             "manifest_bytes": SIZE, "tier_id": TIER,
                             "range_start_bytes": 0, "range_end_bytes": SIZE})
    assert stage_release.movers_in_flight(queue, tier_id=TIER) == {LIVE_MOVER}

    got = _recover(fleet, apply=True)
    assert got["skipped"] == "movers_in_flight"
    assert got["entries_retired"] == 0 and _intact(fleet)


def test_a_head_that_is_not_finished_is_refused(fleet) -> None:
    queue, stage, *_rest = fleet
    _populate(stage)
    queue.publish(action_key=fleet[6], cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root), worker_script="w.py",
                  tags=["dl380g10"], resources={"cpu": 1, "mem_gb": 1})

    got = _recover(fleet, apply=True)
    assert got["complete"] is False and "still" in str(got["skipped"])
    assert got["entries_retired"] == 0 and _intact(fleet)


@pytest.mark.parametrize("which", ["head", "egress"])
def test_an_absent_receipt_is_no_evidence_at_all(fleet, which: str) -> None:
    """Absence is not terminal: it is no answer, and no answer refuses."""

    queue, stage, *_rest = fleet
    _populate(stage)
    queue.move_path(fleet[6] if which == "head" else fleet[7]).unlink()

    got = _recover(fleet, apply=True)
    assert got["complete"] is False and got["entries_retired"] == 0
    assert _intact(fleet)


@pytest.mark.parametrize("which", ["head", "egress"])
def test_a_receipt_filed_under_another_actions_key_is_refused(
        fleet, which: str) -> None:
    """The record's own key must be the key it is read under.

    ``record_move`` stamps the filing key into the body, so a record whose
    embedded ``action_key`` names another action is a misfile, and evidence
    for that other action's history -- never this one's.  Note the egress
    record's own key is its egress key, not the head's: each receipt answers
    for the action that filed it.
    """

    queue, stage, _cas, _digest, _originals, *_rest = fleet
    _populate(stage)
    key = fleet[6] if which == "head" else fleet[7]
    record = json.loads(queue.move_path(key).read_text())
    record["action_key"] = "7" * 64
    queue.move_path(key).write_text(json.dumps(record))

    got = _recover(fleet, apply=True)
    assert got["complete"] is False
    assert f"{which} evidence refused" in str(got["skipped"])
    assert "another action's key" in str(got["skipped"])
    assert _intact(fleet)


def test_an_incomplete_receipt_is_refused(fleet) -> None:
    queue, stage, _cas, digest, *_rest = fleet
    _populate(stage)
    queue.record_move(fleet[6], {
        "schema": pool.POOL_MOVE_SCHEMA_V1, "action_key": fleet[6],
        "consumer_action_key": CONSUMER, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": digest,
        "range_start_bytes": 0, "range_end_bytes": SPAN,
        "entries_staged": len(NAMES), "complete": False})

    got = _recover(fleet, apply=True)
    assert got["complete"] is False and _intact(fleet)


def test_a_sealed_request_naming_another_manifest_is_refused(fleet) -> None:
    """The receipt alone is not authority; the seal must agree with it."""

    _queue, stage, _cas, _digest, _originals, *_rest = fleet
    _populate(stage)
    head, egress = _reseal(fleet, seal_digest="f" * 64)

    got = _recover(fleet, apply=True, head_action_key=head,
                   egress_action_key=egress)
    assert got["complete"] is False
    assert "sealed head request" in str(got["skipped"])
    assert _intact(fleet)


def test_an_unsealed_request_is_refused(fleet) -> None:
    _queue, _stage, cas, _digest, _originals, _co, _head, egress = fleet
    _populate(fleet[1])
    (Path(cas) / "requests" / egress[:2] / f"{egress}.json").unlink()

    got = _recover(fleet, apply=True)
    assert got["complete"] is False and _intact(fleet)


def test_a_receipt_naming_another_stage_root_is_refused(
        fleet, tmp_path: Path) -> None:
    queue, stage, _cas, digest, *_rest = fleet
    _populate(stage)
    _file_receipts(queue, tmp_path / "elsewhere", digest,
                   head=fleet[6], egress=fleet[7])

    got = _recover(fleet, apply=True)
    assert got["complete"] is False and _intact(fleet)


@pytest.mark.parametrize("bad_end", [0, -1, True])
def test_an_unusable_recorded_range_is_refused(fleet, bad_end) -> None:
    """Bounds must be real, nonnegative, ordered integers -- not bools."""

    queue, stage, _cas, digest, *_rest = fleet
    _populate(stage)
    _file_receipts(queue, stage, digest, head=fleet[6], egress=fleet[7],
                   end=bad_end)

    got = _recover(fleet, apply=True)
    assert got["complete"] is False and _intact(fleet)


def test_a_window_that_is_not_the_recorded_one_is_refused(fleet) -> None:
    """A manifest-wide count is not the count one range covers."""

    queue, stage, _cas, digest, *_rest = fleet
    _populate(stage)
    _file_receipts(queue, stage, digest, head=fleet[6], egress=fleet[7],
                   entries=len(NAMES) + 1)

    got = _recover(fleet, apply=True)
    assert got["complete"] is False
    assert "entries" in str(got["skipped"])
    assert _intact(fleet)


def test_an_unreadable_manifest_refuses_rather_than_scoping_to_nothing(
        fleet) -> None:
    queue, stage, cas, _digest, _originals, *_rest = fleet
    _populate(stage)
    head, egress = _reseal(fleet, seal_digest="e" * 64,
                           receipt_digest="e" * 64)

    got = _recover(fleet, apply=True, head_action_key=head,
                   egress_action_key=egress)
    assert got["complete"] is False and _intact(fleet)


def test_a_foreign_stage_root_is_refused_before_anything_else(
        fleet, tmp_path: Path) -> None:
    """Whose stage this is comes before what is on it (#628)."""

    foreign = tmp_path / "foreign"
    foreign.mkdir()
    _populate(fleet[1])
    got = _recover(fleet, apply=True, stage_root=str(foreign))
    assert got["complete"] is False
    assert got["event"] == stage_release.STAGE_ROOT_REFUSED_EVENT
    assert _intact(fleet)


# ---------------------------------------------------------------------------
# The sealed argv is the scope authority.  Manifest equality alone would admit
# the whole 628 GB manifest against authority for one 10.9 GB phase, because
# both phases of a campaign seal the same digest.


def test_the_egress_must_name_the_head_as_its_mover(fleet) -> None:
    """The head-to-egress link is explicit in the argv, not inferred."""

    _queue, stage, _cas, _digest, _originals, *_rest = fleet
    _populate(stage)
    head, egress = _reseal(
        fleet, egress_command=lambda head: _egress_command(
            stage, mover="c" * 64))

    got = _recover(fleet, apply=True, head_action_key=head,
                   egress_action_key=egress)
    assert got["complete"] is False
    assert "not the head" in str(got["skipped"])
    assert _intact(fleet)


@pytest.mark.parametrize("label", ["head", "egress"])
def test_a_sealed_request_naming_another_consumer_is_refused(
        fleet, label: str) -> None:
    _queue, stage, _cas, _digest, _originals, *_rest = fleet
    _populate(stage)
    other = "9" * 64
    if label == "head":
        head, egress = _reseal(
            fleet, head_command=_head_command(stage, consumer=other))
    else:
        head, egress = _reseal(
            fleet, egress_command=lambda head: _egress_command(
                stage, mover=head, consumer=other))

    got = _recover(fleet, apply=True, head_action_key=head,
                   egress_action_key=egress)
    assert got["complete"] is False
    assert "names consumer" in str(got["skipped"])
    assert _intact(fleet)


@pytest.mark.parametrize("label", ["head", "egress"])
def test_a_sealed_request_naming_another_stage_root_is_refused(
        fleet, tmp_path: Path, label: str) -> None:
    _queue, stage, _cas, _digest, _originals, *_rest = fleet
    _populate(stage)
    elsewhere = tmp_path / "elsewhere"
    if label == "head":
        head, egress = _reseal(fleet, head_command=_head_command(elsewhere))
    else:
        head, egress = _reseal(
            fleet, egress_command=lambda head: _egress_command(
                elsewhere, mover=head))

    got = _recover(fleet, apply=True, head_action_key=head,
                   egress_action_key=egress)
    assert got["complete"] is False
    assert "names stage root" in str(got["skipped"])
    assert _intact(fleet)


def test_a_sealed_request_naming_another_tier_is_refused(fleet) -> None:
    _queue, stage, _cas, _digest, _originals, *_rest = fleet
    _populate(stage)
    head, egress = _reseal(
        fleet, head_command=_head_command(stage, tier="prismabuild:other"))

    got = _recover(fleet, apply=True, head_action_key=head,
                   egress_action_key=egress)
    assert got["complete"] is False
    assert "names tier" in str(got["skipped"])
    assert _intact(fleet)


@pytest.mark.parametrize("sealed_end", [SPAN // 2, SPAN * 57])
def test_a_receipt_may_not_widen_the_window_its_request_authorized(
        fleet, sealed_end: int) -> None:
    """The filed receipt is a report; the sealed request is the authority."""

    _queue, stage, _cas, _digest, _originals, *_rest = fleet
    _populate(stage)
    head, egress = _reseal(fleet, head_command=_head_command(
        stage, end=sealed_end))

    got = _recover(fleet, apply=True, head_action_key=head,
                   egress_action_key=egress)
    assert got["complete"] is False
    assert "authorizes only" in str(got["skipped"])
    assert _intact(fleet)


@pytest.mark.parametrize("flag", ["--tier-id", "--stage-root",
                                  "--range-end-bytes",
                                  "--consumer-action-key"])
def test_a_missing_head_flag_is_refused(fleet, flag: str) -> None:
    _queue, stage, _cas, _digest, _originals, *_rest = fleet
    _populate(stage)
    command = _head_command(stage)
    at = command.index(flag)
    head, egress = _reseal(
        fleet, head_command=command[:at] + command[at + 2:])

    got = _recover(fleet, apply=True, head_action_key=head,
                   egress_action_key=egress)
    assert got["complete"] is False
    assert f"names no {flag}" in str(got["skipped"])
    assert _intact(fleet)


@pytest.mark.parametrize("flag", ["--tier-id", "--range-end-bytes"])
def test_a_duplicated_head_flag_is_refused(fleet, flag: str) -> None:
    """A tool built these; a repeated flag is a real signal, not a quirk."""

    _queue, stage, _cas, _digest, _originals, *_rest = fleet
    _populate(stage)
    command = _head_command(stage)
    at = command.index(flag)
    head, egress = _reseal(
        fleet, head_command=command + command[at:at + 2])

    got = _recover(fleet, apply=True, head_action_key=head,
                   egress_action_key=egress)
    assert got["complete"] is False
    assert f"names {flag} 2 times" in str(got["skipped"])
    assert _intact(fleet)


def test_an_egress_carrying_scope_flags_is_refused(fleet) -> None:
    """The egress carries no window of its own; one appearing is unexplained."""

    _queue, stage, _cas, _digest, _originals, *_rest = fleet
    _populate(stage)
    head, egress = _reseal(
        fleet, egress_command=lambda head: _egress_command(stage, mover=head)
        + ["--range-end-bytes", str(SPAN)])

    got = _recover(fleet, apply=True, head_action_key=head,
                   egress_action_key=egress)
    assert got["complete"] is False
    assert "carries no authority here" in str(got["skipped"])
    assert _intact(fleet)


def test_a_head_carrying_a_mover_flag_is_refused(fleet) -> None:
    _queue, stage, _cas, _digest, _originals, *_rest = fleet
    _populate(stage)
    head, egress = _reseal(
        fleet, head_command=_head_command(stage)
        + ["--mover-action-key", "6" * 64])

    got = _recover(fleet, apply=True, head_action_key=head,
                   egress_action_key=egress)
    assert got["complete"] is False
    assert "carries no authority here" in str(got["skipped"])
    assert _intact(fleet)


@pytest.mark.parametrize("command", [
    None, "--tier-id prismabuild-stage:dl380g10", [], ["--tier-id", 7],
])
def test_an_unreadable_sealed_command_is_refused(fleet, command) -> None:
    """A shell string is not an argv; it is never re-parsed into one."""

    _queue, stage, _cas, _digest, _originals, *_rest = fleet
    _populate(stage)
    head, egress = _reseal(fleet, head_command=command)

    got = _recover(fleet, apply=True, head_action_key=head,
                   egress_action_key=egress)
    assert got["complete"] is False
    assert "sealed head request" in str(got["skipped"])
    assert _intact(fleet)


def test_a_trailing_flag_with_no_value_is_refused(fleet) -> None:
    _queue, stage, _cas, _digest, _originals, *_rest = fleet
    _populate(stage)
    command = _head_command(stage)
    at = command.index("--tier-id")
    head, egress = _reseal(
        fleet, head_command=command[:at] + command[at + 2:] + ["--tier-id"])

    got = _recover(fleet, apply=True, head_action_key=head,
                   egress_action_key=egress)
    assert got["complete"] is False
    assert "names no value" in str(got["skipped"])
    assert _intact(fleet)


# ---------------------------------------------------------------------------
# The sealed request is authority, so it must be the action it claims to be.
# A bare JSON load accepts any bytes at that path wearing the right key.


def test_a_request_that_does_not_validate_under_its_key_is_refused(
        fleet) -> None:
    """Hand-edited bytes at the request path are not the sealed request.

    The forged body below carries perfectly plausible flags and manifest --
    left in place, a bare JSON load would treat it as authority and retire
    the copies it names.  The request has to validate as the very action it
    is read under.
    """

    _queue, stage, cas, digest, _originals, _co, head, _egress = fleet
    _populate(stage)
    forged = Path(cas) / "requests" / head[:2] / f"{head}.json"
    forged.chmod(0o644)
    forged.write_text(json.dumps({
        "action_key": head,
        "params": {"command": _head_command(stage),
                   "data_manifest": {"input": {
                       "id": pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID,
                       "sha256": digest, "bytes": 1024},
                       "mount_prefix": "/unused",
                       "entry_count": len(NAMES)}}}))

    got = _recover(fleet, apply=True)
    assert got["complete"] is False
    assert "the head request" in str(got["skipped"])
    assert got["entries_retired"] == 0
    assert _intact(fleet)


def test_a_valid_request_published_under_the_wrong_key_is_refused(
        fleet) -> None:
    """Even a perfectly sealed action is no evidence at another key's path."""

    _queue, _stage, cas, _digest, _originals, _co, head, egress = fleet
    _populate(fleet[1])
    body = (Path(cas) / "requests" / egress[:2] / f"{egress}.json").read_bytes()
    at_head = Path(cas) / "requests" / head[:2] / f"{head}.json"
    at_head.chmod(0o644)
    at_head.write_bytes(body)

    got = _recover(fleet, apply=True)
    assert got["complete"] is False
    assert "sealed for another action" in str(got["skipped"])
    assert _intact(fleet)

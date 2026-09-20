"""Reader lifetime regressions (RNG-02/SM-02/SM-03/INV-07/PRG-01/SAFE-01).

Red-first against main + unwired egress: every test asserting egress,
reconcile, or promotion behavior FAILS before the wiring (bytes deleted
under a live pin, charge released early, source leg unprotected). Tests of
the standalone pin logic (identity fixture, coverage proof, certificate
refusals) pass once the module lands and guard the corrected behavior.

Each test asserts a transition, a refusal, or a byte equality -- never
prose. Run via published pbtest at -10; JSON evidence outside the checkout.
Fixtures are independent (tmp_path queue, no cross-worker state).
"""
from __future__ import annotations

import json
import os
import socket
import threading
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map, reader_lease  # noqa: E402
import stage_release  # noqa: E402

CONSUMER = "c" * 64
CONSUMER2 = "d" * 64
MOVER = "e" * 64
MOVER2 = "f" * 64
TIER = "prismabuild-stage:dl380g10"
ATTEMPT = {"nonce": "n1", "scope_id": "s1"}
HOLDER = {"host": "test-host", "pid": 4242}


def _fragment(root: Path, stage: Path, consumer: str, mover: str,
              source: str, staged: Path, size: int, digest: str = "b" * 64,
              ) -> None:
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {
            residency_map.residency_map_key(source, 0): {
                "stage_path": str(staged), "bytes": size,
                "sha256": digest, "offset": 0,
            },
        },
    })


def _material(root: Path, stage: Path, consumer: str, mover: str,
              source: str, staged: Path, size: int,
              digest: str = "b" * 64) -> str:
    """A publish event: fragment already filed, sidecar dates it. Returns gen."""

    generation = reader_lease.mint_generation()
    identity = reader_lease.stat_identity(str(staged))
    assert identity is not None
    reader_lease.write_material(
        root, consumer_action_key=consumer, mover_action_key=mover,
        tier_id=TIER, stage_root=str(stage), manifest_sha256="a" * 64,
        generation=generation,
        entries={residency_map.residency_map_key(source, 0): {
            "stage_path": str(staged), "bytes": size, "sha256": digest,
            "file_id": identity}})
    return generation


def _publish(root: Path, stage: Path, consumer: str, mover: str,
             source: str, staged: Path, size: int,
             digest: str = "b" * 64) -> str:
    _fragment(root, stage, consumer, mover, source, staged, size, digest)
    return _material(root, stage, consumer, mover, source, staged, size,
                     digest)


def _acquire(queue, mover: str, token: str,
             attempt: dict = ATTEMPT, holder: dict = HOLDER,
             consumer: str = CONSUMER, movers: list | None = None,
             expected=None, context: dict | None = None):
    covers = ([{"mover_action_key": mover, "manifest_sha256": "a" * 64}]
              if movers is None else
              [{"mover_action_key": m, "manifest_sha256": "a" * 64}
               for m in movers])
    return reader_lease.acquire(
        queue, consumer_action_key=consumer, attempt=attempt, tier_id=TIER,
        epoch="", span={"start_bytes": 0, "end_bytes": 4096},
        holder=holder, acquire_token=token, covers=covers, expected=expected,
        context=context)


@pytest.fixture()
def fleet(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    return queue, stage


def test_portable_identity_ignores_client_device(fleet) -> None:
    """Cross-client fixture: dev 64-vs-75 agrees, ino change refuses."""

    _, stage = fleet
    staged = stage / "model" / "id.bin"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x00" * 64)
    first = reader_lease.stat_identity(str(staged))
    second = reader_lease.stat_identity(str(staged))
    assert first is not None and first == second
    assert set(first) == {"ino", "size", "mtime_ns", "ctime_ns"}

    info = os.stat(staged)
    other_dev = 999999 if info.st_dev != 999999 else 888888
    # Another client statting the same NFS file: the server-side fields
    # agree, the client-local device does not (observed 64-vs-75).  A
    # namespace stands in for the foreign stat result -- portable_identity
    # reads attributes only, never the local stat call.
    twin = SimpleNamespace(st_dev=other_dev, st_ino=info.st_ino,
                           st_size=info.st_size,
                           st_mtime_ns=info.st_mtime_ns,
                           st_ctime_ns=info.st_ctime_ns)
    # Same file as another client sees it: different st_dev, same portable id.
    assert reader_lease.portable_identity(twin) == first
    changed = SimpleNamespace(st_dev=info.st_dev, st_ino=info.st_ino + 1,
                              st_size=info.st_size,
                              st_mtime_ns=info.st_mtime_ns,
                              st_ctime_ns=info.st_ctime_ns)
    assert reader_lease.portable_identity(changed) != first


def test_double_acquire_refcounts_window(fleet) -> None:
    """Two acquires, two refs: first release never unpins the second."""

    queue, stage = fleet
    staged = stage / "model" / "w.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x01" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/w.safetensors", staged, 4096)

    first = _acquire(queue, MOVER, "token-one")
    second = _acquire(queue, MOVER, "token-two",
                      holder={"host": "test-host", "pid": 9999})
    assert first["ok"] and second["ok"]
    assert first["pin_id"] == second["pin_id"]
    assert first["ref_id"] != second["ref_id"]

    blocked = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                  stage_root=str(stage))
    assert staged.exists()
    assert blocked["entries_deleted"] == 0
    assert blocked["complete"] is False
    assert blocked.get("tokens_released", 0) == 0

    assert reader_lease.release(queue, first["pin_id"], first["ref_id"],
                                consumer_action_key=CONSUMER) is True
    still = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                stage_root=str(stage))
    assert staged.exists(), "second ref still live; still no delete"
    assert still["entries_deleted"] == 0

    assert reader_lease.release(queue, second["pin_id"], second["ref_id"],
                                consumer_action_key=CONSUMER) is True
    done = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                               stage_root=str(stage))
    assert not staged.exists()
    assert done["complete"] is True
    assert done["entries_deleted"] == 1


def test_retry_token_is_idempotent(fleet) -> None:
    """Same acquire token twice: one ref, one release unpins."""

    queue, stage = fleet
    staged = stage / "model" / "r.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x07" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/r.safetensors", staged, 4096)

    first = _acquire(queue, MOVER, "retry-token")
    second = _acquire(queue, MOVER, "retry-token")
    assert first["ok"] and second["ok"]
    assert second.get("duplicate") is True
    assert first["ref_id"] == second["ref_id"]

    assert reader_lease.release(queue, first["pin_id"], first["ref_id"],
                                consumer_action_key=CONSUMER) is True
    done = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                               stage_root=str(stage))
    assert not staged.exists()
    assert done["entries_deleted"] == 1


def test_forked_child_reads_under_pin_after_parent_stops_releasing(fleet
                                                                    ) -> None:
    """Fork/async lifetime: the pin outlives the issuer; bytes stay readable."""

    queue, stage = fleet
    staged = stage / "model" / "k.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x0b" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/k.safetensors", staged, 4096)
    acquired = _acquire(queue, MOVER, "fork-token")
    assert acquired["ok"]
    key = residency_map.residency_map_key("/mnt/shared/model/k.safetensors", 0)

    pid = os.fork()
    if pid == 0:
        try:
            fd, serving = reader_lease.open_pinned(
                queue, acquired["pin"], acquired["ref_id"], key)
            try:
                assert os.read(fd, 4096) == b"\x0b" * 4096
                assert serving["tier_id"] == TIER
            finally:
                os.close(fd)
            # Supported handoff: the child registers its own ref before the
            # parent may release, so the parent's release cannot unpin it.
            inherited = reader_lease.register_inherited_ref(
                queue, acquired["pin_id"], acquired["ref_id"],
                child_holder={"host": "test-host", "pid": 7777},
                child_token="fork-child-token",
                consumer_action_key=CONSUMER)
            assert inherited["ok"], inherited
        except BaseException:
            os._exit(10)
        os._exit(0)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    child_ref = None
    owners, tainted = reader_lease.live_for(
        queue, {os.path.normpath(str(staged))})
    assert not tainted
    assert len(owners) == 1

    # The issuer releases its own ref (simulates parent exit): the inherited
    # ref keeps the pin live -- no timestamp expiry frees anything.
    assert reader_lease.release(queue, acquired["pin_id"],
                                acquired["ref_id"],
                                consumer_action_key=CONSUMER) is True
    kept = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                               stage_root=str(stage))
    assert staged.exists(), "child ref still live; still no delete"
    assert kept["entries_deleted"] == 0

    pins = reader_lease.refs_for_holder(queue, "test-host")
    child_ref = [entry for entry in pins
                 if entry["ref"]["acquire_token"] == "fork-child-token"]
    assert len(child_ref) == 1
    assert reader_lease.release(queue, acquired["pin_id"],
                                child_ref[0]["ref_id"],
                                consumer_action_key=CONSUMER) is True
    done = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                               stage_root=str(stage))
    assert not staged.exists()
    assert done["entries_deleted"] == 1


def test_aba_republish_refuses_old_pin_and_acquires_new(fleet) -> None:
    """Same path/length, new bytes: old pin refuses, new acquire binds new."""

    queue, stage = fleet
    staged = stage / "model" / "z.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x04" * 4096)
    root = queue.root / pool.RESIDENCY
    source = "/mnt/shared/model/z.safetensors"
    _publish(root, stage, CONSUMER, MOVER, source, staged, 4096, "b" * 64)
    old = _acquire(queue, MOVER, "aba-token")
    assert old["ok"]
    key = residency_map.residency_map_key(source, 0)

    staged.write_bytes(b"\x05" * 4096)  # same path/length, different bytes
    _publish(root, stage, CONSUMER, MOVER, source, staged, 4096, "c" * 64)

    with pytest.raises(reader_lease.ReaderLeaseError):
        reader_lease.open_pinned(queue, old["pin"], old["ref_id"], key)
    fresh = _acquire(queue, MOVER, "aba-token-2")
    assert fresh["ok"]
    assert fresh["pin"]["entries"][0]["generation"] != old["pin"]["entries"][0]["generation"]  # type: ignore[index]
    fd, _ = reader_lease.open_pinned(
        queue, fresh["pin"], fresh["ref_id"], key)
    try:
        assert os.read(fd, 4096) == b"\x05" * 4096
    finally:
        os.close(fd)


def test_retiring_binds_generation_not_path(fleet) -> None:
    """A retiring mark closes its generation; the next generation acquires."""

    queue, stage = fleet
    staged = stage / "model" / "y.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x03" * 4096)
    root = queue.root / pool.RESIDENCY
    source = "/mnt/shared/model/y.safetensors"
    _publish(root, stage, CONSUMER, MOVER, source, staged, 4096)
    held = _acquire(queue, MOVER, "retire-token")
    assert held["ok"]

    deferred = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                   stage_root=str(stage))
    assert staged.exists()
    assert deferred["complete"] is False

    refused = _acquire(queue, MOVER, "retire-token-2",
                       holder={"host": "test-host", "pid": 777})
    assert refused["ok"] is False
    assert refused["refusal"] == "retiring"

    staged.write_bytes(b"\x09" * 4096)  # republish: new generation, same path
    _publish(root, stage, CONSUMER, MOVER, source, staged, 4096, "c" * 64)
    nxt = _acquire(queue, MOVER, "retire-token-3")
    assert nxt["ok"], f"stale generation mark must not wedge the path: {nxt}"
    assert reader_lease.release(queue, held["pin_id"], held["ref_id"],
                                consumer_action_key=CONSUMER) is True
    assert reader_lease.release(queue, nxt["pin_id"], nxt["ref_id"],
                                consumer_action_key=CONSUMER) is True
    done = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                               stage_root=str(stage))
    assert not staged.exists()
    assert done["entries_deleted"] == 1


def test_cover_proves_full_window_across_movers(fleet) -> None:
    """Promotion shape: two stage movers cover one window, gaps refuse."""

    queue, stage = fleet
    first = stage / "m" / "00.bin"
    second = stage / "m" / "01.bin"
    first.parent.mkdir(parents=True)
    first.write_bytes(b"\x11" * 1024)
    second.write_bytes(b"\x22" * 1024)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/pkg/shard.bin", first, 1024, "b" * 64)
    # Second range of the same pool file lives under its staged name.
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER2,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {"1048576:/mnt/shared/pkg/big.bin": {
            "stage_path": str(second), "bytes": 1024,
            "sha256": "c" * 64, "offset": 1048576}}})
    identity = reader_lease.stat_identity(str(second))
    assert identity is not None
    reader_lease.write_material(
        root, consumer_action_key=CONSUMER, mover_action_key=MOVER2,
        tier_id=TIER, stage_root=str(stage), manifest_sha256="a" * 64,
        generation=reader_lease.mint_generation(),
        entries={"1048576:/mnt/shared/pkg/big.bin": {
            "stage_path": str(second), "bytes": 1024, "sha256": "c" * 64,
            "file_id": identity}})

    expected = {"0:/mnt/shared/pkg/shard.bin": {"bytes": 1024,
                                                "sha256": "b" * 64},
                "1048576:/mnt/shared/pkg/big.bin": {"bytes": 1024,
                                                    "sha256": "c" * 64}}
    whole = _acquire(queue, MOVER, "cover-token", movers=[MOVER, MOVER2],
                     expected=expected)
    assert whole["ok"], whole
    assert len(whole["pin"]["entries"]) == 2  # type: ignore[index]

    gapped = dict(expected)
    gapped["2097152:/mnt/shared/pkg/big.bin"] = {"bytes": 1024,
                                                 "sha256": "d" * 64}
    refused = _acquire(queue, MOVER, "cover-token-2", movers=[MOVER, MOVER2],
                       expected=gapped)
    assert refused == {"ok": False, "refusal": "source-coverage-gap"}

    mismatch = dict(expected)
    mismatch["0:/mnt/shared/pkg/shard.bin"] = {"bytes": 2048,
                                               "sha256": "b" * 64}
    refused2 = _acquire(queue, MOVER, "cover-token-3", movers=[MOVER, MOVER2],
                        expected=mismatch)
    assert refused2 == {"ok": False, "refusal": "source-coverage-gap"}


def test_corrupt_pin_taints_egress_closed(fleet) -> None:
    """An unreadable pin file fails closed: nothing deleted, nothing freed."""

    queue, stage = fleet
    staged = stage / "model" / "t.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x06" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/t.safetensors", staged, 4096)
    leases = reader_lease.leases_root(queue)
    (leases / CONSUMER).mkdir(parents=True, exist_ok=True)
    (leases / CONSUMER / ("0" * 32 + ".lease.json")).write_text("{not json\n")

    receipt = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                  stage_root=str(stage))
    assert staged.exists(), "ownership uncertain: egress must delete nothing"
    assert receipt["entries_deleted"] == 0
    assert receipt.get("tokens_released", 0) == 0
    assert receipt["complete"] is False


def test_malformed_retiring_fails_closed_then_recovers(fleet) -> None:
    """A malformed mark taints acquires; clearing it recovers the range."""

    queue, stage = fleet
    staged = stage / "model" / "m.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x08" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/m.safetensors", staged, 4096)
    leases = reader_lease.leases_root(queue)
    (leases / CONSUMER).mkdir(parents=True, exist_ok=True)
    (leases / CONSUMER / f"{MOVER}.retiring.json").write_text("{bad\n")

    refused = _acquire(queue, MOVER, "recover-token")
    assert refused["ok"] is False
    assert refused["refusal"].startswith("ownership-uncertain")

    reader_lease.clear_retiring(leases, consumer_action_key=CONSUMER,
                                mover_action_key=MOVER)
    recovered = _acquire(queue, MOVER, "recover-token")
    assert recovered["ok"], recovered




def _attest(queue, nonce="n1", scope_empty=True, host="test-host",
            worker="w1"):
    """Helper: fabricate a complete typed broker attestation (test input).

    Helper only: the file is the pool writer's input shape, fabricated
    the way terminal records are fabricated.  Carries the full typed
    export booleans (empty True, tickets_pending False) a real writer
    files; verification additionally requires terminal broker telemetry,
    so this file alone proves nothing.
    """

    path = reader_lease.attestation_path(queue, CONSUMER, nonce)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "schema": reader_lease.ATTESTATION_SCHEMA_V1,
        "action_key": CONSUMER, "nonce": nonce, "scope_id": "s1",
        "host": host, "worker": worker, "incarnation": "i1",
        "scope_empty": scope_empty, "released": True, "retired": False,
        "settled": False, "empty": True, "tickets_pending": False,
        "stopped_unix": 1789870000.0,
        "unix": 1789880000.0}) + "\n")

def test_containment_needs_terminal_and_attestation(fleet) -> None:
    """No attestation, live scope, or missing terminal retains; certified frees.

    Terminal records use the real broker-evidence shape PB writes
    (``detail.resource_telemetry`` with ``{action_key, nonce, scope_unit,
    host}``); the attestation body must bind the same IDs.
    """

    queue, stage = fleet
    staged = stage / "model" / "q.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x0d" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/q.safetensors", staged, 4096)
    acquired = _acquire(queue, MOVER, "contain-token")
    assert acquired["ok"]
    target = {"consumer_action_key": CONSUMER, "pin_id": acquired["pin_id"],
              "ref_id": acquired["ref_id"]}
    cert = {"action_key": CONSUMER, "nonce": "n1", "scope_id": "s1",
            "worker": "w1", "incarnation": "i1", "host": "test-host"}

    # No broker attestation at all: retain, whatever the caller claims.
    no_proof = reader_lease.release_refs(queue, [target], dict(cert))
    assert no_proof["ok"] is False
    assert no_proof["reason"] == "no-broker-attestation-retain"

    # The broker proved the scope still live: retain.
    _attest(queue, scope_empty=False)
    live_scope = reader_lease.release_refs(queue, [target], dict(cert))
    assert live_scope["ok"] is False
    assert live_scope["reason"] == "scope-not-empty-retain"
    assert staged.exists()

    # Attestation for a stopped scope, but no terminal evidence yet: retain.
    _attest(queue, scope_empty=True)
    no_terminal = reader_lease.release_refs(queue, [target], dict(cert))
    assert no_terminal["ok"] is False
    assert no_terminal["reason"] == "no-terminal-evidence-retain"

    # A terminal record for a DIFFERENT attempt (a retry is current) with no
    # history for this one: retain, not forever-proof.
    failed = queue.dir(pool.FAILED)
    failed.mkdir(parents=True, exist_ok=True)
    (failed / f"{CONSUMER}.json").write_text(json.dumps(
        {"action_key": CONSUMER, "status": "failed",
         "detail": {"resource_telemetry": {
             "action_key": CONSUMER, "nonce": "n9", "scope_unit": "s1",
             "host": "test-host"}}}))
    superseded = reader_lease.release_refs(queue, [target], dict(cert))
    assert superseded["ok"] is False
    assert superseded["reason"] == "no-terminal-evidence-retain"

    # The exact older history proves this attempt terminally closed with
    # matching broker telemetry: a newer terminal does not retain forever.
    history = queue.root / "attempts" / CONSUMER / "gen"
    history.mkdir(parents=True, exist_ok=True)
    (history / "00000001.json").write_text(json.dumps(
        {"action_key": CONSUMER, "status": "failed", "disposition": "failed",
         "resource_scope": {"nonce": "n1", "unit": "s1"},
         "detail": {"resource_telemetry": {
             "action_key": CONSUMER, "nonce": "n1", "scope_unit": "s1",
             "host": "test-host",
             "termination_reason": "memory_limit_oom",
             "termination_evidence": {"victim": "payload"}}}}))
    via_history = reader_lease.release_refs(queue, [target], dict(cert))
    assert via_history["ok"] is True, via_history
    assert via_history["released"] == [acquired["ref_id"]]
    done = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                               stage_root=str(stage))
    assert not staged.exists()
    assert done["entries_deleted"] == 1


def test_terminal_telemetry_match_releases_without_history(fleet) -> None:
    """Terminal telemetry naming the exact attempt is terminal evidence."""

    queue, stage = fleet
    staged = stage / "model" / "h.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x0e" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/h.safetensors", staged, 4096)
    acquired = _acquire(queue, MOVER, "telemetry-token")
    assert acquired["ok"]
    target = {"consumer_action_key": CONSUMER, "pin_id": acquired["pin_id"],
              "ref_id": acquired["ref_id"]}
    cert = {"action_key": CONSUMER, "nonce": "n1", "scope_id": "s1",
            "host": "test-host"}
    _attest(queue, scope_empty=True)
    done_dir = queue.dir(pool.DONE)
    done_dir.mkdir(parents=True, exist_ok=True)
    (done_dir / f"{CONSUMER}.json").write_text(json.dumps(
        {"action_key": CONSUMER, "status": "executed",
         "resource_telemetry": {
             "action_key": CONSUMER, "nonce": "n1", "scope_unit": "s1",
             "host": "test-host"}}))
    certified = reader_lease.release_refs(queue, [target], dict(cert))
    assert certified["ok"] is True, certified
    done = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                               stage_root=str(stage))
    assert not staged.exists()
    assert done["entries_deleted"] == 1


def test_attestation_body_must_bind_the_attempt(fleet) -> None:
    """An attestation file for another nonce/scope/action proves nothing."""

    queue, stage = fleet
    staged = stage / "model" / "b.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x0f" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/b.safetensors", staged, 4096)
    acquired = _acquire(queue, MOVER, "bind-token")
    assert acquired["ok"]
    target = {"consumer_action_key": CONSUMER, "pin_id": acquired["pin_id"],
              "ref_id": acquired["ref_id"]}
    cert = {"action_key": CONSUMER, "nonce": "n1", "scope_id": "s1",
            "host": "test-host"}
    # A file at the right path but for another attempt: body mismatch.
    other = reader_lease.attestation_path(queue, CONSUMER, "n1")
    other.parent.mkdir(parents=True, exist_ok=True)
    other.write_text(json.dumps({
        "schema": reader_lease.ATTESTATION_SCHEMA_V1,
        "action_key": CONSUMER, "nonce": "nX", "scope_id": "s1",
        "host": "test-host", "worker": "w", "incarnation": "i",
        "scope_empty": True, "unix": 1789880000.0}) + "\n")
    refused = reader_lease.release_refs(queue, [target], dict(cert))
    assert refused["ok"] is False
    assert refused["reason"] == "attestation-id-mismatch-retain"
    assert staged.exists()


def test_open_pinned_refuses_an_unknown_key(fleet) -> None:
    """Exact key or nothing: no lone-entry fallback serves a wrong range."""

    queue, stage = fleet
    staged = stage / "model" / "u.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x10" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/u.safetensors", staged, 4096)
    acquired = _acquire(queue, MOVER, "exact-key-token")
    assert acquired["ok"]
    with pytest.raises(reader_lease.ReaderLeaseError):
        reader_lease.open_pinned(
        queue, acquired["pin"], acquired["ref_id"], "0:/mnt/shared/nope.bin")


def test_context_miss_does_not_stick(fleet) -> None:
    """A cached absence must not blind later acquires to new material."""

    queue, stage = fleet
    staged = stage / "model" / "c.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x11" * 4096)
    root = queue.root / pool.RESIDENCY
    source = "/mnt/shared/model/c.safetensors"
    _fragment(root, stage, CONSUMER, MOVER, source, staged, 4096)
    context: dict = {}
    first = _acquire(queue, MOVER, "context-token", context=context)
    assert first == {"ok": False, "refusal": "no-file-identity"}
    # The publish lands after the miss, reusing the same context.
    _material(root, stage, CONSUMER, MOVER, source, staged, 4096)
    second = _acquire(queue, MOVER, "context-token-2", context=context)
    assert second["ok"], second


def test_concurrent_acquire_release_keeps_exact_refs(fleet) -> None:
    """Hammered acquire/release pairs lose no ref and resurrect none."""

    import threading

    queue, stage = fleet
    staged = stage / "model" / "v.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x12" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/v.safetensors", staged, 4096)

    for round in range(20):
        start = threading.Barrier(3)
        outcomes: dict[str, object] = {}

        def acquire_b() -> None:
            start.wait(timeout=30)
            outcomes["b"] = _acquire(
                queue, MOVER, f"hammer-b-{round}",
                holder={"host": "test-host", "pid": 2})

        def release_a() -> None:
            start.wait(timeout=30)
            outcomes["rel"] = reader_lease.release(
                queue, held["pin_id"], held["ref_id"],
                consumer_action_key=CONSUMER)

        held = _acquire(queue, MOVER, f"hammer-a-{round}")
        assert held["ok"], held
        first = threading.Thread(target=acquire_b)
        second = threading.Thread(target=release_a)
        first.start()
        second.start()
        start.wait(timeout=30)
        first.join(timeout=30)
        second.join(timeout=30)
        assert outcomes["b"]["ok"], outcomes  # type: ignore[index]
        assert outcomes["rel"] is True, outcomes
        # Exactly the new ref survives: the release neither took it nor
        # left the old one behind.
        owners, tainted = reader_lease.live_for(
            queue, {os.path.normpath(str(staged))})
        assert tainted == []
        assert len(owners) == 1
        pin = reader_lease._read_pin(
            reader_lease.leases_root(queue) / CONSUMER
            / f"{held['pin_id']}.lease.json")
        assert isinstance(pin, dict)
        refs = pin["refs"]
        assert list(refs) == [outcomes["b"]["ref_id"]], refs  # type: ignore[index]
        assert reader_lease.release(
            queue, held["pin_id"], outcomes["b"]["ref_id"],  # type: ignore[index]
            consumer_action_key=CONSUMER) is True
    assert staged.exists()
    done = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                               stage_root=str(stage))
    assert not staged.exists()
    assert done["entries_deleted"] == 1


def test_legacy_enumerators_ignore_lease_namespaces(fleet) -> None:
    """Sidecars, pins and retiring marks never parse as fragments.

    The fragment-owner scan, the fragment reader and the attribution walk
    must not taint (or count) the dedicated metadata namespaces -- that
    taint would wedge every egress fail-closed on a healthy tier.
    """

    queue, stage = fleet
    staged = stage / "model" / "g.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x0e" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/g.safetensors", staged, 4096)
    acquired = _acquire(queue, MOVER, "namespace-token")
    assert acquired["ok"]
    reader_lease.write_retiring(
        reader_lease.leases_root(queue), consumer_action_key=CONSUMER2,
        mover_action_key=MOVER2, generation="0" * 32)

    wanted = {os.path.normpath(str(staged))}
    owners, tainted = stage_release._fragment_owners(root, wanted)
    assert tainted == [], f"metadata namespaces taint the owner scan: {tainted}"
    assert owners == {os.path.normpath(str(staged)): {(CONSUMER, MOVER)}}

    fragments = residency_map.read_fragments(root, CONSUMER)
    assert len(fragments) == 1
    attributed = stage_release.attributed_stage_paths(
        queue, wanted={MOVER}, residency_root=root)
    assert str(staged) in attributed


def test_open_refuses_a_released_ref_dict(fleet) -> None:
    """A stale dict from a released ref pins nothing: open refuses."""

    queue, stage = fleet
    staged = stage / "model" / "e.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x13" * 4096)
    root = queue.root / pool.RESIDENCY
    source = "/mnt/shared/model/e.safetensors"
    _publish(root, stage, CONSUMER, MOVER, source, staged, 4096)
    acquired = _acquire(queue, MOVER, "stale-token")
    assert acquired["ok"]
    key = residency_map.residency_map_key(source, 0)
    assert reader_lease.release(queue, acquired["pin_id"],
                                acquired["ref_id"],
                                consumer_action_key=CONSUMER) is True
    with pytest.raises(reader_lease.ReaderLeaseError):
        reader_lease.open_pinned(queue, acquired["pin"], acquired["ref_id"],
                                 key)


def test_injected_context_comes_from_pb_sources(fleet,
                                                monkeypatch) -> None:
    """The SDK derives identity from env + claim row; gaps refuse, nothing guessed."""

    import socket

    queue, stage = fleet
    claimed = queue.dir(pool.CLAIMED)
    claimed.mkdir(parents=True, exist_ok=True)
    nonce = "n" * 32
    (claimed / f"{CONSUMER}.json").write_text(json.dumps({
        "action_key": CONSUMER,
        "claimed_by": "worker-7",
        "resources": {"cpu": 1, "mem_gb": 1},
        "claimed_host": "sparky",
        "resource_scope": {"action_key": CONSUMER, "nonce": nonce,
                           "scope_id": "unit-1"}}))
    env = {"PRISMABUILD_ACTION_KEY": CONSUMER,
           "PRISMABUILD_RESIDENCY_MAP": str(
               queue.root / pool.RESIDENCY / f"{CONSUMER}.map.json"),
           "PRISMABUILD_ACTION_NONCE": nonce,
           "PRISMABUILD_ACTION_SCOPE": "unit-1"}
    got = reader_lease.injected_context(queue, env=env)
    assert got["ok"], got
    ctx = got["ctx"]
    assert ctx["action_key"] == CONSUMER
    assert ctx["nonce"] == nonce
    assert ctx["scope_id"] == "unit-1"
    assert ctx["worker"] == "worker-7"
    # Fleet alias from the claim, never the container-local hostname.
    assert ctx["host"] == "sparky"
    assert ctx["attempt_source"] == "launch-env"
    assert ctx["helper_root"] == str(
        Path(reader_lease.__file__).resolve().parents[2])

    # No token is exposed through the context.
    assert "token" not in json.dumps(ctx)

    # A superseded process holding an old launch identity refuses instead
    # of adopting the live claim's newer attempt.
    (claimed / f"{CONSUMER}.json").write_text(json.dumps({
        "action_key": CONSUMER,
        "claimed_by": "worker-7",
        "resources": {"cpu": 1, "mem_gb": 1},
        "claimed_host": "sparky",
        "resource_scope": {"action_key": CONSUMER, "nonce": "m" * 32,
                           "scope_id": "unit-2"}}))
    assert reader_lease.injected_context(
        queue, env=env)["refusal"] == "attempt-superseded"

    # Strict: no live-claim fallback.  Launch env present but the claim
    # carries no control to check against: unbound, refuse.
    (claimed / f"{CONSUMER}.json").write_text(json.dumps(
        {"action_key": CONSUMER, "claimed_by": "worker-7",
        "resources": {"cpu": 1, "mem_gb": 1},
         "claimed_host": "sparky",
         "resource_scope": {"action_key": CONSUMER, "nonce": "",
                            "scope_id": ""}}))
    bare = {"PRISMABUILD_ACTION_KEY": CONSUMER,
            "PRISMABUILD_RESIDENCY_MAP": str(
                queue.root / pool.RESIDENCY / f"{CONSUMER}.map.json")}
    assert reader_lease.injected_context(
        queue, env={**bare,
                    "PRISMABUILD_ACTION_NONCE": nonce,
                    "PRISMABUILD_ACTION_SCOPE": "unit-1"},
    )["refusal"] == "no-control-context"
    # No launch env at all: refuse even with a complete claim.  There is
    # no strict pin from a live claim alone.
    (claimed / f"{CONSUMER}.json").write_text(json.dumps(
        {"action_key": CONSUMER, "claimed_by": "worker-7",
        "resources": {"cpu": 1, "mem_gb": 1},
         "claimed_host": "sparky",
         "resource_scope": {"action_key": CONSUMER, "nonce": nonce,
                            "scope_id": "unit-1"}}))
    assert reader_lease.injected_context(
        queue, env=bare)["refusal"] == "no-launch-context"
    # Nothing names a box: no local hostname substitution, ever.
    (claimed / f"{CONSUMER}.json").write_text(json.dumps(
        {"action_key": CONSUMER, "claimed_by": "worker-7",
        "resources": {"cpu": 1, "mem_gb": 1},
         "resource_scope": {"action_key": CONSUMER, "nonce": nonce,
                            "scope_id": "unit-1"}}))
    assert reader_lease.injected_context(
        queue, env={**bare,
                    "PRISMABUILD_ACTION_NONCE": nonce,
                    "PRISMABUILD_ACTION_SCOPE": "unit-1"},
    )["refusal"] == "no-host-context"


def test_legacy_inspection_never_acquires(fleet) -> None:
    """inspect_claim_context reports the claim unqualified; acquire refuses it."""

    queue, stage = fleet
    claimed = queue.dir(pool.CLAIMED)
    claimed.mkdir(parents=True, exist_ok=True)
    (claimed / f"{CONSUMER}.json").write_text(json.dumps({
        "action_key": CONSUMER, "claimed_by": "worker-7",
        "resources": {"cpu": 1, "mem_gb": 1},
        "claimed_host": "sparky",
        "resource_scope": {"action_key": CONSUMER, "nonce": "n" * 32,
                           "scope_id": "unit-1"}}))
    seen = reader_lease.inspect_claim_context(queue, CONSUMER)
    assert seen["ok"] is True
    assert seen["inspection"]["qualified"] is False  # type: ignore[index]
    assert seen["inspection"]["nonce"] == "n" * 32  # type: ignore[index]
    # The inspection carries no launch binding, so strict context refuses.
    env = {"PRISMABUILD_ACTION_KEY": CONSUMER,
           "PRISMABUILD_RESIDENCY_MAP": str(
               queue.root / pool.RESIDENCY / f"{CONSUMER}.map.json")}
    assert reader_lease.injected_context(
        queue, env=env)["refusal"] == "no-launch-context"


def test_acquire_for_carries_fleet_identity_into_refs(fleet) -> None:
    """Container readers are found under the fleet alias, not localhost."""

    import socket

    queue, stage = fleet
    staged = stage / "model" / "f.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x15" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/f.safetensors", staged, 4096)
    ctx = {"queue_root": str(queue.root), "action_key": CONSUMER,
           "nonce": "n1", "scope_id": "s1", "worker": "worker-7",
           "host": "sparky", "incarnation": None,
           "attempt_source": "launch-env", "map_path": "x",
           "helper_root": "y"}
    got = reader_lease.acquire_for(
        ctx, tier_id=TIER, epoch="",
        covers=[{"mover_action_key": MOVER, "manifest_sha256": "a" * 64}],
        expected=None, span={"start_bytes": 0, "end_bytes": 4096},
        acquire_token="fleet-token", residency_root=root)
    assert got["ok"], got
    pin = reader_lease._read_pin(
        root / "leases" / CONSUMER / f"{got['pin_id']}.lease.json")
    assert isinstance(pin, dict)
    holder = pin["refs"][got["ref_id"]]["holder"]  # type: ignore[index]
    assert holder["host"] == "sparky"
    assert holder["worker"] == "worker-7"
    # Found under the fleet alias even though this process runs elsewhere.
    found = reader_lease.refs_for_holder(queue, "sparky",
                                         residency_root=root)
    assert [entry["ref_id"] for entry in found] == [got["ref_id"]]


def test_open_revalidates_ram_epoch(fleet) -> None:
    """An epoch that moves between acquire and open refuses the open."""

    queue, stage = fleet
    ram_tier = "ram:testhost"
    staged = stage / "model" / "p.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x16" * 4096)
    root = queue.root / pool.RESIDENCY
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": ram_tier, "stage_root": str(stage),
        "manifest_sha256": "a" * 64, "epoch": "epoch-1",
        "entries": {residency_map.residency_map_key(
            "/mnt/shared/model/p.safetensors", 0): {
                "stage_path": str(staged), "bytes": 4096,
                "sha256": "b" * 64, "offset": 0}}})
    identity = reader_lease.stat_identity(str(staged))
    assert identity is not None
    reader_lease.write_material(
        root, consumer_action_key=CONSUMER, mover_action_key=MOVER,
        tier_id=ram_tier, stage_root=str(stage),
        manifest_sha256="a" * 64,
        generation=reader_lease.mint_generation(),
        entries={residency_map.residency_map_key(
            "/mnt/shared/model/p.safetensors", 0): {
                "stage_path": str(staged), "bytes": 4096,
                "sha256": "b" * 64, "file_id": identity}},
        epoch="epoch-1")
    tiers = queue.root / "tiers"
    tiers.mkdir(parents=True, exist_ok=True)
    (tiers / f"{ram_tier}.json").write_text(json.dumps({"epoch": "epoch-1"}))
    acquired = reader_lease.acquire(
        queue, consumer_action_key=CONSUMER, attempt=ATTEMPT,
        tier_id=ram_tier, epoch="epoch-1",
        span={"start_bytes": 0, "end_bytes": 4096},
        holder=HOLDER, acquire_token="epoch-token",
        covers=[{"mover_action_key": MOVER, "manifest_sha256": "a" * 64}],
        residency_root=root)
    assert acquired["ok"], acquired
    key = residency_map.residency_map_key("/mnt/shared/model/p.safetensors",
                                          0)
    # The tier re-announces mid-hold: the old header epoch cannot make a
    # stale generation current.
    (tiers / f"{ram_tier}.json").write_text(json.dumps({"epoch": "epoch-2"}))
    with pytest.raises(reader_lease.ReaderLeaseError):
        reader_lease.open_pinned(queue, acquired["pin"],
                                 acquired["ref_id"], key,
                                 residency_root=root)


def test_adopted_generation_is_stable_reuse(fleet) -> None:
    """Same bytes without replacement keep their generation; gaps raise."""

    queue, stage = fleet
    staged = stage / "model" / "s.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x14" * 4096)
    root = queue.root / pool.RESIDENCY
    generation = _publish(root, stage, CONSUMER, MOVER,
                          "/mnt/shared/model/s.safetensors", staged, 4096)
    material = reader_lease.read_material(root, CONSUMER, MOVER)
    assert isinstance(material, dict)
    assert reader_lease.adopted_generation(material) == generation
    with pytest.raises(reader_lease.ReaderLeaseError):
        reader_lease.adopted_generation({"generation": "0" * 32})
    with pytest.raises(reader_lease.ReaderLeaseError):
        reader_lease.adopted_generation(
            {"generation": generation, "entries": {}})


class _FakeBrokerBackend:
    """Kernel seam fake (precedent: test_resource_broker.Backend)."""

    def __init__(self):
        self.groups = {}

    def create(self, scope, budget):
        self.groups[scope] = {"populated": False}
        return {}

    def stop(self, scope):
        self.groups[scope]["populated"] = False

    def empty(self, scope):
        return scope not in self.groups or not self.groups[scope]["populated"]

    def exists(self, scope):
        return scope in self.groups

    def release(self, scope):
        if self.groups[scope]["populated"]:
            raise ValueError("scope still populated")
        self.groups.pop(scope)


def _broker(authority_path, backend=None):
    import resource_broker

    backend = backend if backend is not None else _FakeBrokerBackend()
    return resource_broker.Authority(
        authority_path, os.getuid(), backend,
        max_memory_bytes=1024 ** 3)


def _broker_create(authority, nonce="b" * 32):
    req = {"op": "create", "action_key": CONSUMER, "nonce": nonce,
           "memory_max_bytes": 64 * 1024 ** 2}
    return req, authority.handle(
        os.getuid(), os.getpid(), req)


def _broker_token_call(authority, req, record, op):
    return authority.handle(os.getuid(), os.getpid(),
                            {**req, "op": op, "token": record["token"]})


class _KernelBackend:
    """Controlled kernel seam for connected tests: real Authority op
    logic, fake cgroup path strings (no kernel touch)."""

    def __init__(self):
        self.groups = {}

    def create(self, scope, budget):
        self.groups[scope] = {"populated": False}
        return {"cgroup_path": f"/sys/fs/cgroup/prismabuild.slice/{scope}"}

    def stop(self, scope):
        self.groups[scope]["populated"] = False

    def empty(self, scope):
        return scope not in self.groups or not self.groups[scope]["populated"]

    def exists(self, scope):
        return scope in self.groups

    def release(self, scope):
        if self.groups[scope]["populated"]:
            raise ValueError("scope still populated")
        self.groups.pop(scope)


class _ConnectedBroker:
    """A real Authority behind a real Unix socket (context manager).

    Production RPC framing, production op logic, controlled kernel seam.
    No stubbed proof booleans anywhere: every verdict below comes out of
    ``Authority.handle``.
    """

    def __init__(self, tmp_path):
        import resource_broker

        self.module = resource_broker
        self.backend = _KernelBackend()
        self.authority = resource_broker.Authority(
            tmp_path / "broker-state", os.getuid(), self.backend,
            max_memory_bytes=1024 ** 3)
        self.endpoint = tmp_path / "broker.sock"
        self.server = resource_broker.Server(
            str(self.endpoint), resource_broker.Handler)
        self.server.authority = self.authority
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.01}, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        try:
            self.server.shutdown()
        finally:
            self.thread.join(timeout=10)
            self.server.server_close()
        return False


def _connected_setup(queue, stage, root, tmp_path, broker, nonce, token,
                     source="/mnt/shared/cx.bin", name="cx.bin", size=2048):
    """Publish, create a real scope, file the real claim, acquire a ref.

    Returns ``(unit, host, worker, staged, acquired)``.  The CLAIMED row
    carries the broker-minted control record; the host is the real local
    hostname everywhere (claim, holder, telemetry) via real derivation,
    never handwritten to match.
    """

    from prismabuild import resource_scope

    staged = stage / "model" / name
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x67" * size)
    _publish(root, stage, CONSUMER, MOVER, source, staged, size)
    host = socket.gethostname()
    worker = f"{host}:4242:9d001122"
    scope = resource_scope.ResourceScope(
        CONSUMER, nonce, 64 * 1024 ** 2, tmp_path / "telemetry.json",
        socket_path=broker.endpoint)
    control = scope.create()
    assert control["scope_id"] == scope.unit
    unit = scope.unit
    claimed = queue.dir(pool.CLAIMED)
    claimed.mkdir(parents=True, exist_ok=True)
    (claimed / f"{CONSUMER}.json").write_text(json.dumps({
        "action_key": CONSUMER, "claimed_by": worker,
        "claimed_host": host, "published_unix": 1789880000.0,
        # A sealed demand shape without this tier: production claims
        # always seal resources, and a consumer is not a copy (the
        # egress taints a claim that cannot establish non-mover).
        "resources": {"cpu": 1, "mem_gb": 1},
        "resource_scope": control}))
    key = residency_map.residency_map_key(source, 0)
    acquired = reader_lease.acquire(
        queue, consumer_action_key=CONSUMER,
        attempt={"nonce": nonce, "scope_id": unit}, tier_id=TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": size},
        holder={"host": host, "worker": worker, "pid": 4242},
        acquire_token=token,
        covers=[{"mover_action_key": MOVER, "manifest_sha256": "a" * 64}],
        expected={key: {"bytes": size, "sha256": "b" * 64}},
        residency_root=root)
    assert acquired["ok"], acquired
    return unit, host, worker, staged, acquired


def test_broker_export_verdict_needs_stop_and_token(tmp_path) -> None:
    """The export op extends broker authority: token-gated, read-only."""

    authority = _broker(tmp_path / "broker-state")
    req, record = _broker_create(authority)
    scope = record["scope_id"]

    with pytest.raises(ValueError, match="scope not stopped"):
        _broker_token_call(authority, req, record, "export_stopped")
    with pytest.raises(PermissionError):
        authority.handle(os.getuid(), os.getpid(),
                         {**req, "op": "export_stopped", "token": "0" * 64})

    _broker_token_call(authority, req, record, "stop")
    verdict = _broker_token_call(authority, req, record, "export_stopped")
    assert verdict["stopped"] is True
    assert verdict["empty"] is True
    assert verdict["released"] is False
    assert verdict["scope_id"] == scope
    # Read-only: nothing mutated by the export.
    assert "released_unix" not in verdict


def test_broker_export_reports_tickets_and_live_scope(tmp_path) -> None:
    """Unresolved Docker tickets / live cgroup export proof-negative."""

    backend = _FakeBrokerBackend()
    authority = _broker(tmp_path / "broker-state", backend)
    req, record = _broker_create(authority)
    scope = record["scope_id"]
    _broker_token_call(authority, req, record, "stop")
    # A late container lands after the stop: populated with an unresolved
    # ticket.  The export reports proof-negative (empty False, tickets
    # pending), which authorizes nothing downstream.
    backend.groups[scope]["populated"] = True
    authority.records[scope]["container_tickets"] = ["ticket-1"]
    verdict = _broker_token_call(authority, req, record, "export_stopped")
    assert verdict["stopped"] is True
    assert verdict["empty"] is False
    assert verdict["tickets_pending"] is True


def test_ordinary_completion_reclaims_after_broker_containment(fleet,
                                                              tmp_path) -> None:
    """Helper: end to end with membership-shaped inputs (fabricated file).

    Helper only: the attestation file below is fabricated in the pool
    writer's shape from a real broker verdict above (labeled as such);
    the connected tests below drive the real writer through real
    cleanup/finish instead.  Every proof field comes out of the real
    broker verdict; verification additionally needs terminal telemetry."""

    queue, stage = fleet
    staged = stage / "model" / "j.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x17" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/j.safetensors", staged, 4096)
    nonce = "b" * 32
    holder = {"host": "sparky", "worker": "worker-7", "pid": 31337}
    acquired = reader_lease.acquire(
        queue, consumer_action_key=CONSUMER,
        attempt={"nonce": nonce, "scope_id": "s1"}, tier_id=TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": 4096}, holder=holder,
        acquire_token="e2e-token",
        covers=[{"mover_action_key": MOVER, "manifest_sha256": "a" * 64}],
        residency_root=root)
    assert acquired["ok"], acquired

    # Real broker lifecycle for this attempt (fake kernel seam, real op
    # logic): create, run to populated, stop, release.
    authority = _broker(tmp_path / "broker-state")
    req, record = _broker_create(authority, nonce=nonce)
    authority.backend.groups[record["scope_id"]]["populated"] = True
    _broker_token_call(authority, req, record, "stop")
    _broker_token_call(authority, req, record, "release")
    verdict = _broker_token_call(authority, req, record, "export_stopped")
    assert verdict["released"] is True

    # Membership files the attestation FROM the export verdict (their
    # writer; fabricated here as their input, labeled as such): every
    # proof field below comes out of the real broker verdict above.
    assert verdict["scope_id"] == record["scope_id"]
    attest = reader_lease.attestation_path(queue, CONSUMER, nonce)
    attest.parent.mkdir(parents=True, exist_ok=True)
    attest.write_text(json.dumps({
        "schema": reader_lease.ATTESTATION_SCHEMA_V1,
        "action_key": CONSUMER, "nonce": nonce, "scope_id": "s1",
        "host": "sparky", "worker": "worker-7",
        "scope_empty": True,
        "released": bool(verdict.get("released")),
        "retired": bool(verdict.get("retired")),
        "settled": bool(verdict.get("settled")),
        "empty": bool(verdict.get("empty")),
        "tickets_pending": bool(verdict.get("tickets_pending")),
        "stopped_unix": verdict.get("stopped_unix"),
        "unix": 1789880000.0}) + "\n")

    # Terminal broker telemetry from the execution path (their record).
    done_dir = queue.dir(pool.DONE)
    done_dir.mkdir(parents=True, exist_ok=True)
    (done_dir / f"{CONSUMER}.json").write_text(json.dumps(
        {"action_key": CONSUMER, "status": "executed",
         "resource_telemetry": {
             "action_key": CONSUMER, "nonce": nonce, "scope_unit": "s1",
             "host": "sparky"}}))
    cert = {"action_key": CONSUMER, "nonce": nonce, "scope_id": "s1",
            "worker": "worker-7", "host": "sparky"}
    freed = reader_lease.release_refs(
        queue, [{"consumer_action_key": CONSUMER,
                 "pin_id": acquired["pin_id"],
                 "ref_id": acquired["ref_id"]}], dict(cert))
    assert freed["ok"] is True, freed
    done = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                               stage_root=str(stage))
    assert not staged.exists()
    assert done["entries_deleted"] == 1


def test_release_refs_keeps_worker_correspondence(fleet) -> None:
    """A certificate for one worker incarnation never frees another's ref."""

    queue, stage = fleet
    staged = stage / "model" / "w2.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x18" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/w2.safetensors", staged, 4096)
    acquired = _acquire(queue, MOVER, "worker-token",
                        holder={"host": "test-host", "worker": "worker-7",
                                "pid": 11})
    assert acquired["ok"]
    target = {"consumer_action_key": CONSUMER, "pin_id": acquired["pin_id"],
              "ref_id": acquired["ref_id"]}
    done_dir = queue.dir(pool.DONE)
    done_dir.mkdir(parents=True, exist_ok=True)
    (done_dir / f"{CONSUMER}.json").write_text(json.dumps(
        {"action_key": CONSUMER, "status": "executed",
         "resource_telemetry": {
             "action_key": CONSUMER, "nonce": "n1", "scope_unit": "s1",
             "host": "test-host"}}))
    other_worker = {"action_key": CONSUMER, "nonce": "n1", "scope_id": "s1",
                    "worker": "worker-9", "host": "test-host"}
    _attest(queue, scope_empty=True, worker="worker-9")
    refused = reader_lease.release_refs(queue, [target], dict(other_worker))
    assert refused["ok"] is False, refused
    assert refused["skipped"] == ["%s: worker mismatch" % acquired["ref_id"]]
    assert staged.exists()
    same_worker = {"action_key": CONSUMER, "nonce": "n1", "scope_id": "s1",
                   "worker": "worker-7", "host": "test-host"}
    _attest(queue, scope_empty=True, worker="worker-7")
    freed = reader_lease.release_refs(queue, [target], dict(same_worker))
    assert freed["ok"] is True, freed


def test_export_after_release_keeps_full_proof(tmp_path) -> None:
    """Helper: repeated export after release keeps retired/empty/evidence."""

    authority = _broker(tmp_path / "broker-state")
    req, record = _broker_create(authority)
    _broker_token_call(authority, req, record, "stop")
    _broker_token_call(authority, req, record, "release")
    first = _broker_token_call(authority, req, record, "export_stopped")
    second = _broker_token_call(authority, req, record, "export_stopped")
    assert first["released"] is True
    assert second["released"] is True
    assert second["empty"] is True
    assert first == second


def test_first_release_reply_carries_no_released_flag(tmp_path) -> None:
    """Helper: the R8 protocol shape on the real Authority, both paths.

    The first successful ``release`` with no container tickets returns
    ``{ok, scope_id, **stop details}`` -- no ``released`` flag -- and a
    ticket retirement returns only ``{ok, scope_id, retired}``.  Only
    ``export_stopped`` carries the full proof.  Real op logic,
    controlled kernel seam.
    """

    authority = _broker(tmp_path / "broker-state")
    req, record = _broker_create(authority)
    _broker_token_call(authority, req, record, "stop")
    first = _broker_token_call(authority, req, record, "release")
    assert first["ok"] is True
    assert first["scope_id"] == record["scope_id"]
    assert "released" not in first
    assert "retired" not in first
    assert "settled" not in first

    ticketed = _broker(tmp_path / "broker-state-2")
    treq, trecord = _broker_create(ticketed)
    _broker_token_call(ticketed, treq, trecord, "stop")
    ticketed.records[trecord["scope_id"]]["container_tickets"] = ["t-1"]
    retired = _broker_token_call(ticketed, treq, trecord, "release")
    assert retired == {"ok": True, "scope_id": trecord["scope_id"],
                       "retired": True}


def test_egress_auto_reclaims_contained_pins(fleet) -> None:
    """No manual release_refs: terminal + proof present, egress frees itself."""

    queue, stage = fleet
    staged = stage / "model" / "auto.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x19" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/auto.safetensors", staged, 4096)
    acquired = _acquire(queue, MOVER, "auto-token")
    assert acquired["ok"]
    _attest(queue, scope_empty=True)
    done_dir = queue.dir(pool.DONE)
    done_dir.mkdir(parents=True, exist_ok=True)
    (done_dir / f"{CONSUMER}.json").write_text(json.dumps(
        {"action_key": CONSUMER, "status": "executed",
         "resource_telemetry": {
             "action_key": CONSUMER, "nonce": "n1", "scope_unit": "s1",
             "host": "test-host"}}))
    receipt = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                  stage_root=str(stage))
    assert receipt["auto_reclaimed"] == [acquired["ref_id"]], receipt
    assert not staged.exists()
    assert receipt["complete"] is True
    assert receipt["entries_deleted"] == 1


def test_bare_withdrawn_record_frees_nothing(fleet) -> None:
    """A withdrawal note saying withdrawn, without attempt telemetry, retains."""

    queue, stage = fleet
    staged = stage / "model" / "wd.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x1a" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/wd.safetensors", staged, 4096)
    acquired = _acquire(queue, MOVER, "withdrawn-token")
    assert acquired["ok"]
    target = {"consumer_action_key": CONSUMER, "pin_id": acquired["pin_id"],
              "ref_id": acquired["ref_id"]}
    _attest(queue, scope_empty=True)
    withdrawn = queue.dir(pool.WITHDRAWN)
    withdrawn.mkdir(parents=True, exist_ok=True)
    (withdrawn / f"{CONSUMER}.json").write_text(json.dumps(
        {"action_key": CONSUMER, "reason": "superseded"}))
    cert = {"action_key": CONSUMER, "nonce": "n1", "scope_id": "s1",
            "host": "test-host"}
    refused = reader_lease.release_refs(queue, [target], dict(cert))
    assert refused["ok"] is False
    assert refused["reason"] == "no-terminal-evidence-retain"
    kept = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                               stage_root=str(stage))
    assert staged.exists()
    assert kept["entries_deleted"] == 0


def test_resolve_window_covers_from_published_material(fleet) -> None:
    """PQ cover lookup: keys in, covers+expected out; gaps refuse; nothing invented."""

    queue, stage = fleet
    first = stage / "m" / "00.bin"
    second = stage / "m" / "01.bin"
    first.parent.mkdir(parents=True)
    first.write_bytes(b"\x21" * 1024)
    second.write_bytes(b"\x22" * 1024)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/pkg/shard.bin", first, 1024, "b" * 64)
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER2,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {"1048576:/mnt/shared/pkg/big.bin": {
            "stage_path": str(second), "bytes": 1024,
            "sha256": "c" * 64, "offset": 1048576}}})
    identity = reader_lease.stat_identity(str(second))
    assert identity is not None
    reader_lease.write_material(
        root, consumer_action_key=CONSUMER, mover_action_key=MOVER2,
        tier_id=TIER, stage_root=str(stage), manifest_sha256="a" * 64,
        generation=reader_lease.mint_generation(),
        entries={"1048576:/mnt/shared/pkg/big.bin": {
            "stage_path": str(second), "bytes": 1024, "sha256": "c" * 64,
            "file_id": identity}})
    resolved = reader_lease.resolve_window_covers(
        queue, consumer_action_key=CONSUMER, tier_id=TIER, epoch="",
        keys=["0:/mnt/shared/pkg/shard.bin",
              "1048576:/mnt/shared/pkg/big.bin"],
        manifest_sha256="a" * 64, residency_root=root)
    assert resolved["ok"], resolved
    assert sorted(cover["mover_action_key"]  # type: ignore[index]
                  for cover in resolved["covers"]) == sorted([MOVER, MOVER2])
    assert sorted(resolved["expected"]) == [  # type: ignore[index]
        "0:/mnt/shared/pkg/shard.bin", "1048576:/mnt/shared/pkg/big.bin"]
    gapped = reader_lease.resolve_window_covers(
        queue, consumer_action_key=CONSUMER, tier_id=TIER, epoch="",
        keys=["0:/mnt/shared/pkg/shard.bin", "9:/mnt/shared/pkg/nope.bin"],
        manifest_sha256="a" * 64, residency_root=root)
    assert gapped == {"ok": False, "refusal": "source-coverage-gap"}
    whole = reader_lease.resolve_window_covers(
        queue, consumer_action_key=CONSUMER, tier_id=TIER, epoch="",
        keys=None, manifest_sha256="a" * 64, residency_root=root)
    assert whole["ok"] and len(whole["expected"]) == 2  # type: ignore[index]
    # The lookup feeds acquire directly: selection proves under the lock.
    acquired = reader_lease.acquire(
        queue, consumer_action_key=CONSUMER, attempt=ATTEMPT, tier_id=TIER,
        epoch="", span={"start_bytes": 0, "end_bytes": 2048},
        holder=HOLDER, acquire_token="lookup-token",
        covers=resolved["covers"], expected=resolved["expected"],  # type: ignore[index]
        residency_root=root)
    assert acquired["ok"], acquired


def test_distinct_same_shape_files_never_share_a_pin(fleet) -> None:
    """Pin identity binds the object keyset: equal size/offset files split.

    Two equal-sized distinct files under one mover (both source offset 0)
    must file two pins.  Before the keyset joined the pin identity, the
    second acquire appended a ref to entries naming the first file's
    bytes, and opening the second key failed or served the wrong window.
    """

    import threading

    queue, stage = fleet
    first = stage / "model" / "a.bin"
    second = stage / "model" / "b.bin"
    first.parent.mkdir(parents=True)
    first.write_bytes(b"\x31" * 4096)
    second.write_bytes(b"\x32" * 4096)
    root = queue.root / pool.RESIDENCY
    key_a = residency_map.residency_map_key("/mnt/shared/model/a.bin", 0)
    key_b = residency_map.residency_map_key("/mnt/shared/model/b.bin", 0)
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {
            key_a: {"stage_path": str(first), "bytes": 4096,
                    "sha256": "b" * 64, "offset": 0},
            key_b: {"stage_path": str(second), "bytes": 4096,
                    "sha256": "c" * 64, "offset": 0},
        }})
    generation = reader_lease.mint_generation()
    reader_lease.write_material(
        root, consumer_action_key=CONSUMER, mover_action_key=MOVER,
        tier_id=TIER, stage_root=str(stage), manifest_sha256="a" * 64,
        generation=generation,
        entries={
            key_a: {"stage_path": str(first), "bytes": 4096,
                    "sha256": "b" * 64,
                    "file_id": reader_lease.stat_identity(str(first))},
            key_b: {"stage_path": str(second), "bytes": 4096,
                    "sha256": "c" * 64,
                    "file_id": reader_lease.stat_identity(str(second))}})

    def acquire_key(key, digest, token):
        return reader_lease.acquire(
            queue, consumer_action_key=CONSUMER, attempt=ATTEMPT,
            tier_id=TIER, epoch="",
            span={"start_bytes": 0, "end_bytes": 4096},
            holder=HOLDER, acquire_token=token,
            covers=[{"mover_action_key": MOVER,
                     "manifest_sha256": "a" * 64}],
            expected={key: {"bytes": 4096, "sha256": digest}},
            residency_root=root)

    pin_a = acquire_key(key_a, "b" * 64, "token-a")
    pin_b = acquire_key(key_b, "c" * 64, "token-b")
    assert pin_a["ok"] and pin_b["ok"]
    assert pin_a["pin_id"] != pin_b["pin_id"]
    fd, _ = reader_lease.open_pinned(queue, pin_a["pin"],
                                     pin_a["ref_id"], key_a,
                                     residency_root=root)
    try:
        assert os.read(fd, 4096) == b"\x31" * 4096
    finally:
        os.close(fd)
    with pytest.raises(reader_lease.ReaderLeaseError):
        reader_lease.open_pinned(queue, pin_a["pin"], pin_a["ref_id"],
                                 key_b, residency_root=root)

    # Concurrent distinct keysets keep independent lifetimes, repeatedly.
    for round in range(10):
        start = threading.Barrier(3)
        outcomes: dict[str, object] = {}

        def take_a() -> None:
            start.wait(timeout=30)
            outcomes["a"] = acquire_key(key_a, "b" * 64, f"race-a-{round}")

        def take_b() -> None:
            start.wait(timeout=30)
            outcomes["b"] = acquire_key(key_b, "c" * 64, f"race-b-{round}")

        first_t = threading.Thread(target=take_a)
        second_t = threading.Thread(target=take_b)
        first_t.start()
        second_t.start()
        start.wait(timeout=30)
        first_t.join(timeout=30)
        second_t.join(timeout=30)
        assert outcomes["a"]["ok"] and outcomes["b"]["ok"]  # type: ignore[index]
        assert outcomes["a"]["pin_id"] != outcomes["b"]["pin_id"]  # type: ignore[index]
        assert reader_lease.release(
            queue, outcomes["a"]["pin_id"], outcomes["a"]["ref_id"],  # type: ignore[index]
            consumer_action_key=CONSUMER) is True
        assert reader_lease.release(
            queue, outcomes["b"]["pin_id"], outcomes["b"]["ref_id"],  # type: ignore[index]
            consumer_action_key=CONSUMER) is True

    # Independent release: freeing A deletes only A (per-keyset pins mean
    # per-object lifetimes); B stays pinned behind its own pin.
    assert reader_lease.release(queue, pin_a["pin_id"], pin_a["ref_id"],
                                consumer_action_key=CONSUMER) is True
    kept = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                               stage_root=str(stage))
    assert not first.exists() and second.exists()
    assert kept["entries_deleted"] == 1
    assert reader_lease.release(queue, pin_b["pin_id"], pin_b["ref_id"],
                                consumer_action_key=CONSUMER) is True
    done = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                               stage_root=str(stage))
    assert not first.exists() and not second.exists()
    assert done["entries_deleted"] == 1


def test_covers_for_keys_both_tiers_minimal(fleet) -> None:
    """covers_for_keys: exact manifest+epoch filter, minimal movers, gaps."""

    queue, stage = fleet
    ram_tier = "ram:testhost"
    stage_file = stage / "model" / "s.bin"
    ram_file = stage / "model" / "r.bin"
    stage_file.parent.mkdir(parents=True)
    stage_file.write_bytes(b"\x41" * 1024)
    ram_file.write_bytes(b"\x42" * 1024)
    root = queue.root / pool.RESIDENCY
    stage_key = residency_map.residency_map_key("/mnt/shared/s.bin", 0)
    ram_key = residency_map.residency_map_key("/mnt/shared/r.bin", 0)
    _publish(root, stage, CONSUMER, MOVER, "/mnt/shared/s.bin",
             stage_file, 1024, "b" * 64)
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER2,
        "tier_id": ram_tier, "stage_root": str(stage),
        "manifest_sha256": "a" * 64, "epoch": "epoch-1",
        "entries": {ram_key: {"stage_path": str(ram_file), "bytes": 1024,
                              "sha256": "c" * 64, "offset": 0}}})
    identity = reader_lease.stat_identity(str(ram_file))
    assert identity is not None
    reader_lease.write_material(
        root, consumer_action_key=CONSUMER, mover_action_key=MOVER2,
        tier_id=ram_tier, stage_root=str(stage), manifest_sha256="a" * 64,
        generation=reader_lease.mint_generation(),
        entries={ram_key: {"stage_path": str(ram_file), "bytes": 1024,
                           "sha256": "c" * 64, "file_id": identity}},
        epoch="epoch-1")

    stage_only = reader_lease.covers_for_keys(
        root, CONSUMER, [stage_key], tier_id=TIER,
        manifest_sha256="a" * 64, epoch="")
    assert stage_only["ok"], stage_only
    # Minimal: the ram mover does not cover the stage key.
    assert stage_only["covers"] == [  # type: ignore[index]
        {"mover_action_key": MOVER, "manifest_sha256": "a" * 64}]

    ram_only = reader_lease.covers_for_keys(
        root, CONSUMER, [ram_key], tier_id=ram_tier,
        manifest_sha256="a" * 64, epoch="epoch-1")
    assert ram_only["ok"], ram_only

    wrong_epoch = reader_lease.covers_for_keys(
        root, CONSUMER, [ram_key], tier_id=ram_tier,
        manifest_sha256="a" * 64, epoch="epoch-2")
    assert wrong_epoch == {"ok": False, "refusal": "unpublished"}

    wrong_manifest = reader_lease.covers_for_keys(
        root, CONSUMER, [stage_key], tier_id=TIER,
        manifest_sha256="d" * 64, epoch="")
    assert wrong_manifest == {"ok": False, "refusal": "unpublished"}

    both = reader_lease.covers_for_keys(
        root, CONSUMER, [stage_key], tier_id=TIER,
        manifest_sha256="a" * 64, epoch="")
    missing = reader_lease.covers_for_keys(
        root, CONSUMER, [stage_key, "0:/mnt/shared/nope.bin"], tier_id=TIER,
        manifest_sha256="a" * 64, epoch="")
    assert missing == {"ok": False, "refusal": "source-coverage-gap"}
    assert both["ok"]


def test_staged_sidecar_with_epoch_refuses_at_write(fleet) -> None:
    """Q4: a staged epoch is corrupt at write time, never at open."""

    queue, stage = fleet
    staged = stage / "model" / "e2.bin"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x43" * 64)
    root = queue.root / pool.RESIDENCY
    identity = reader_lease.stat_identity(str(staged))
    assert identity is not None
    with pytest.raises(reader_lease.ReaderLeaseError):
        reader_lease.write_material(
            root, consumer_action_key=CONSUMER, mover_action_key=MOVER,
            tier_id=TIER, stage_root=str(stage), manifest_sha256="a" * 64,
            generation=reader_lease.mint_generation(),
            entries={"0:/mnt/shared/e2.bin": {
                "stage_path": str(staged), "bytes": 64, "sha256": "b" * 64,
                "file_id": identity}},
            epoch="epoch-1")


def test_payload_identity_env_is_assignment_not_leak() -> None:
    """resource_exec stamps exact launch identity; outer values never leak."""

    import resource_exec

    outer = {"PRISMABUILD_ACTION_NONCE": "o" * 32,
             "PRISMABUILD_ACTION_SCOPE": "outer-scope",
             "OTHER": "kept"}
    stamped = resource_exec.payload_identity_env(
        outer, action_key="a" * 64, nonce="b" * 32)
    assert stamped["PRISMABUILD_ACTION_NONCE"] == "b" * 32
    assert stamped["OTHER"] == "kept"
    assert outer["PRISMABUILD_ACTION_NONCE"] == "o" * 32
    scope = stamped["PRISMABUILD_ACTION_SCOPE"]
    assert scope.startswith("prismabuild-job") and scope.endswith(".slice")
    helper = stamped["PRISMABUILD_READER_HELPER_ROOT"]
    assert helper and not helper.endswith("/src")


def test_owner_and_material_namespace_stay_split(fleet) -> None:
    """Produced-output shape: pin owned by the reader, proof in the
    producer namespace; the output namespace is never the running action."""

    PRODUCER = "e" * 64
    READER = "f" * 64
    queue, stage = fleet
    staged = stage / "model" / "o.bin"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x51" * 2048)
    root = queue.root / pool.RESIDENCY
    key = residency_map.residency_map_key("/mnt/shared/model/o.bin", 0)
    _publish(root, stage, PRODUCER, MOVER, "/mnt/shared/model/o.bin",
             staged, 2048, "b" * 64)
    got = reader_lease.acquire(
        queue, consumer_action_key=PRODUCER,
        attempt={"nonce": "n1", "scope_id": "s1"}, tier_id=TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": 2048},
        holder={"host": "test-host", "worker": "worker-7", "pid": 5},
        acquire_token="owner-split-token",
        covers=[{"mover_action_key": MOVER, "manifest_sha256": "a" * 64}],
        expected={key: {"bytes": 2048, "sha256": "b" * 64}},
        residency_root=root, owner_action_key=READER)
    assert got["ok"], got
    assert got["pin"]["consumer_action_key"] == PRODUCER  # type: ignore[index]
    assert got["pin"]["owner_action_key"] == READER  # type: ignore[index]
    assert (root / "leases" / READER
            / f"{got['pin_id']}.lease.json").exists()
    assert not (root / "leases" / PRODUCER
                / f"{got['pin_id']}.lease.json").exists()
    fd, serving = reader_lease.open_pinned(
        queue, got["pin"], got["ref_id"], key, residency_root=root)
    try:
        assert os.read(fd, 2048) == b"\x51" * 2048
        assert serving["tier_id"] == TIER
    finally:
        os.close(fd)
    # The producer's egress defers to the reader-owned pin...
    blocked = stage_release.evict(queue, MOVER,
                                  consumer_action_key=PRODUCER,
                                  stage_root=str(stage))
    assert staged.exists()
    assert blocked["entries_deleted"] == 0
    # ...and the READER's terminal (never the producer's) reclaims it.
    _attest_for(queue, READER, "n1", worker="worker-7")
    done_dir = queue.dir(pool.DONE)
    done_dir.mkdir(parents=True, exist_ok=True)
    (done_dir / f"{READER}.json").write_text(json.dumps(
        {"action_key": READER, "status": "executed",
         "resource_telemetry": {
             "action_key": READER, "nonce": "n1", "scope_unit": "s1",
             "host": "test-host"}}))
    freed = stage_release.evict(queue, MOVER,
                                consumer_action_key=PRODUCER,
                                stage_root=str(stage))
    assert freed["auto_reclaimed"] == [got["ref_id"]]
    assert not staged.exists()
    assert freed["entries_deleted"] == 1


def _attest_for(queue, action, nonce, scope_empty=True, host="test-host",
                worker="w1"):
    """Helper: fabricated complete typed attestation for another action."""

    path = reader_lease.attestation_path(queue, action, nonce)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "schema": reader_lease.ATTESTATION_SCHEMA_V1,
        "action_key": action, "nonce": nonce, "scope_id": "s1",
        "host": host, "worker": worker, "incarnation": "i1",
        "scope_empty": scope_empty, "released": True, "retired": False,
        "settled": False, "empty": True, "tickets_pending": False,
        "stopped_unix": 1789870000.0,
        "unix": 1789880000.0}) + "\n")


def test_pin_identity_is_canonical_over_paths(fleet) -> None:
    """Delimiter characters in paths cannot merge or split pin identity."""

    first = reader_lease.pin_id_for(
        consumer_action_key=CONSUMER, tier_id=TIER, epoch="", stage_root="/s",
        start=0, end=8, movers=[MOVER], generations={MOVER: "0" * 32},
        keys={"0:/a|b,c=d": {"bytes": 8, "sha256": "b" * 64,
                             "generation": "0" * 32}})
    second = reader_lease.pin_id_for(
        consumer_action_key=CONSUMER, tier_id=TIER, epoch="", stage_root="/s",
        start=0, end=8, movers=[MOVER], generations={MOVER: "0" * 32},
        keys={"0:/a|b,c=d": {"bytes": 8, "sha256": "b" * 64,
                             "generation": "0" * 32}})
    other = reader_lease.pin_id_for(
        consumer_action_key=CONSUMER, tier_id=TIER, epoch="", stage_root="/s",
        start=0, end=8, movers=[MOVER], generations={MOVER: "0" * 32},
        keys={"0:/a": {"bytes": 8, "sha256": "b" * 64,
                       "generation": "0" * 32}})
    assert first == second
    assert first != other
    body = reader_lease._canonical_pin_body(
        consumer_action_key=CONSUMER, tier_id=TIER, epoch="",
        stage_root="/s", start=0, end=8, movers=[MOVER],
        generations={MOVER: "0" * 32},
        keys={"0:/a|b,c=d": {"bytes": 8, "sha256": "b" * 64,
                             "generation": "0" * 32}})
    import json as _json
    assert _json.loads(body)["objects"] == [
        {"key": "0:/a|b,c=d", "bytes": 8, "sha256": "b" * 64,
         "generation": "0" * 32}]


def test_contradictory_covers_refuse(fleet) -> None:
    """Two movers vouching one key with different bytes is no cover."""

    queue, stage = fleet
    staged = stage / "model" / "cc.bin"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x61" * 512)
    root = queue.root / pool.RESIDENCY
    key = residency_map.residency_map_key("/mnt/shared/cc.bin", 0)
    _publish(root, stage, CONSUMER, MOVER, "/mnt/shared/cc.bin",
             staged, 512, "b" * 64)
    other = stage / "model" / "cc2.bin"
    other.write_bytes(b"\x62" * 1024)
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER2,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {key: {"stage_path": str(other), "bytes": 1024,
                          "sha256": "c" * 64, "offset": 0}}})
    identity = reader_lease.stat_identity(str(other))
    assert identity is not None
    reader_lease.write_material(
        root, consumer_action_key=CONSUMER, mover_action_key=MOVER2,
        tier_id=TIER, stage_root=str(stage), manifest_sha256="a" * 64,
        generation=reader_lease.mint_generation(),
        entries={key: {"stage_path": str(other), "bytes": 1024,
                       "sha256": "c" * 64, "file_id": identity}})
    refused = reader_lease.covers_for_keys(
        root, CONSUMER, [key], tier_id=TIER, manifest_sha256="a" * 64,
        epoch="")
    assert refused == {
        "ok": False, "refusal": "ownership-uncertain: contradictory covers"}


def test_cover_lookup_caches_valid_reuses_valid_only(fleet) -> None:
    """Generation-keyed cache: repeats reuse, new material is seen, misses stick never."""

    queue, stage = fleet
    staged = stage / "model" / "v2.bin"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x63" * 256)
    root = queue.root / pool.RESIDENCY
    key = residency_map.residency_map_key("/mnt/shared/v2.bin", 0)
    context: dict = {}
    assert reader_lease.covers_for_keys(
        root, CONSUMER, [key], tier_id=TIER, manifest_sha256="a" * 64,
        epoch="", context=context) == {"ok": False, "refusal": "unpublished"}
    assert context == {}, "absence must never populate the cache"
    _publish(root, stage, CONSUMER, MOVER, "/mnt/shared/v2.bin",
             staged, 256, "b" * 64)
    first = reader_lease.covers_for_keys(
        root, CONSUMER, [key], tier_id=TIER, manifest_sha256="a" * 64,
        epoch="", context=context)
    assert first["ok"], first
    assert any(entry.startswith("cover:") for entry in context)
    second = reader_lease.covers_for_keys(
        root, CONSUMER, [key], tier_id=TIER, manifest_sha256="a" * 64,
        epoch="", context=context)
    assert second == first


def test_cleanup_persists_broker_proof_exactly(fleet) -> None:
    """Helper: the pool cleanup hook files the broker verdict, exact IDs."""

    from types import SimpleNamespace

    queue, stage = fleet
    record = {"action_key": CONSUMER, "claimed_by": "worker-7",
        "resources": {"cpu": 1, "mem_gb": 1},
              "claimed_host": "sparky"}
    export = {"scope_id": "unit-9", "released": True, "retired": False,
              "settled": False, "empty": True, "tickets_pending": False,
              "stopped_unix": 1789870000.0,
              "termination_evidence": {"stop": "done"}}
    queue._persist_reader_scope_proof(
        record, "n" * 32, "unit-9", dict(export))
    attestation = reader_lease.read_scope_attestation(queue, CONSUMER,
                                                      "n" * 32)
    assert isinstance(attestation, dict)
    assert attestation["action_key"] == CONSUMER
    assert attestation["scope_id"] == "unit-9"
    assert attestation["scope_empty"] is True
    assert attestation["worker"] == "worker-7"
    assert attestation["released"] is True
    assert attestation["termination_evidence"] == {"stop": "done"}


def test_withdrawn_with_telemetry_reclaims_automatically(fleet) -> None:
    """Withdraw terminal WITH broker telemetry frees via the egress itself."""

    queue, stage = fleet
    staged = stage / "model" / "aw.bin"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x64" * 4096)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER,
             "/mnt/shared/model/aw.bin", staged, 4096)
    acquired = _acquire(queue, MOVER, "auto-withdraw-token")
    assert acquired["ok"]
    _attest(queue, scope_empty=True)
    withdrawn = queue.dir(pool.WITHDRAWN)
    withdrawn.mkdir(parents=True, exist_ok=True)
    (withdrawn / f"{CONSUMER}.json").write_text(json.dumps(
        {"action_key": CONSUMER, "status": "withdrawn",
         "resource_telemetry": {
             "action_key": CONSUMER, "nonce": "n1", "scope_unit": "s1",
             "host": "test-host"}}))
    receipt = stage_release.evict(queue, MOVER, consumer_action_key=CONSUMER,
                                  stage_root=str(stage))
    assert receipt["auto_reclaimed"] == [acquired["ref_id"]], receipt
    assert not staged.exists()
    assert receipt["entries_deleted"] == 1


def test_mixed_half_control_refuses_without_intent_synthesis(fleet) -> None:
    """Strict: control scope without control nonce refuses; intent never fills it."""

    queue, stage = fleet
    claimed = queue.dir(pool.CLAIMED)
    claimed.mkdir(parents=True, exist_ok=True)
    nonce = "n" * 32
    (claimed / f"{CONSUMER}.json").write_text(json.dumps({
        "action_key": CONSUMER,
        "claimed_by": "worker-7",
        "resources": {"cpu": 1, "mem_gb": 1},
        "claimed_host": "sparky",
        "resource_scope": {"action_key": CONSUMER, "scope_id": "unit-1"},
        "resource_scope_intent": {"action_key": CONSUMER, "nonce": nonce}}))
    env = {"PRISMABUILD_ACTION_KEY": CONSUMER,
           "PRISMABUILD_RESIDENCY_MAP": str(
               queue.root / pool.RESIDENCY / f"{CONSUMER}.map.json"),
           "PRISMABUILD_ACTION_NONCE": nonce,
           "PRISMABUILD_ACTION_SCOPE": "unit-1"}
    refused = reader_lease.injected_context(queue, env=env)
    assert refused == {"ok": False, "refusal": "no-control-context"}
    # The legacy inspector still reports what exists, unqualified.
    seen = reader_lease.inspect_claim_context(queue, CONSUMER)
    assert seen["ok"] is True
    assert seen["inspection"]["nonce"] == ""  # type: ignore[index]
    assert seen["inspection"]["scope_id"] == "unit-1"  # type: ignore[index]


def test_split_namespace_full_lifecycle_direct_calls(fleet) -> None:
    """Owner/material split through direct release/inherit/open, no egress."""

    PRODUCER = "e" * 64
    READER = "f" * 64
    queue, stage = fleet
    staged = stage / "model" / "ox.bin"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x52" * 2048)
    root = queue.root / pool.RESIDENCY
    key = residency_map.residency_map_key("/mnt/shared/model/ox.bin", 0)
    _publish(root, stage, PRODUCER, MOVER, "/mnt/shared/model/ox.bin",
             staged, 2048, "b" * 64)
    got = reader_lease.acquire(
        queue, consumer_action_key=PRODUCER,
        attempt={"nonce": "n1", "scope_id": "s1"}, tier_id=TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": 2048},
        holder={"host": "test-host", "worker": "worker-7", "pid": 5},
        acquire_token="split-direct-token",
        covers=[{"mover_action_key": MOVER, "manifest_sha256": "a" * 64}],
        expected={key: {"bytes": 2048, "sha256": "b" * 64}},
        residency_root=root, owner_action_key=READER)
    assert got["ok"], got
    # Fork handoff without naming the owner dir: found by scan.
    inherited = reader_lease.register_inherited_ref(
        queue, got["pin_id"], got["ref_id"],
        child_holder={"host": "test-host", "worker": "worker-7", "pid": 6},
        child_token="split-child-token")
    assert inherited["ok"], inherited
    fd, _ = reader_lease.open_pinned(queue, got["pin"],
                                     inherited["ref_id"], key,
                                     residency_root=root)
    try:
        assert os.read(fd, 2048) == b"\x52" * 2048
    finally:
        os.close(fd)
    # Direct release without naming the owner dir: exact ref dropped.
    assert reader_lease.release(
        queue, got["pin_id"], got["ref_id"]) is True
    owners, tainted = reader_lease.live_for(
        queue, {os.path.normpath(str(staged))})
    assert not tainted and len(owners) == 1
    assert reader_lease.release(
        queue, got["pin_id"], inherited["ref_id"]) is True
    owners, tainted = reader_lease.live_for(
        queue, {os.path.normpath(str(staged))})
    assert not tainted and owners == {}


def test_launch_rpc_carries_identity_env_to_broker(tmp_path) -> None:
    """resource_exec transmits the stamped env in the actual run RPC.

    A fake broker socket captures the request: the nonce/scope/helper
    vars are present with exact values, outer stale values are
    overwritten (never leaked through), and the attempt token rides the
    request identity only -- never the payload env.
    """

    import socket as _socket
    import threading

    import resource_exec

    sock_path = tmp_path / "broker.sock"
    captured: dict = {}
    ready = threading.Event()

    def serve() -> None:
        server = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
        server.bind(str(sock_path))
        server.listen(1)
        ready.set()
        conn, _ = server.accept()
        with conn:
            data = bytearray()
            while b"\n" not in data:
                chunk = conn.recv(65536)
                if not chunk:
                    break
                data.extend(chunk)
            captured.update(
                json.loads(data.split(b"\n", 1)[0].decode()))
            conn.sendall(b'{"ok": true, "returncode": 0}\n')
        server.close()

    thread = threading.Thread(target=serve)
    thread.start()
    assert ready.wait(timeout=30)
    key, nonce = "a" * 64, "b" * 32
    argv = ["resource_exec", "--socket", str(sock_path),
            "--action-key", key, "--nonce", nonce,
            "--token", "c" * 64, "--", "/usr/bin/true"]
    import sys as _sys
    old_argv = _sys.argv
    old_env = dict(os.environ)
    os.environ["PRISMABUILD_ACTION_NONCE"] = "o" * 32
    os.environ["PRISMABUILD_ACTION_SCOPE"] = "outer-scope"
    try:
        _sys.argv = argv
        assert resource_exec.main() == 0
    finally:
        _sys.argv = old_argv
        os.environ.clear()
        os.environ.update(old_env)
    thread.join(timeout=30)
    assert captured["op"] == "run"
    assert captured["nonce"] == nonce
    env = captured["env"]
    assert env["PRISMABUILD_ACTION_NONCE"] == nonce
    assert env["PRISMABUILD_ACTION_SCOPE"].startswith("prismabuild-job")
    assert env["PRISMABUILD_ACTION_SCOPE"].endswith(".slice")
    assert "PRISMABUILD_READER_HELPER_ROOT" in env
    assert not env["PRISMABUILD_READER_HELPER_ROOT"].endswith("/src")
    assert "token" not in env and "TOKEN" not in " ".join(env)


def test_identity_derivation_failure_clears_stale_outer(
        tmp_path, monkeypatch) -> None:
    """Derivation failure removes keys; stale outer values never survive."""

    import sys as _sys

    import resource_exec

    outer = {"PRISMABUILD_ACTION_NONCE": "o" * 32,
             "PRISMABUILD_ACTION_SCOPE": "outer-scope",
             "PRISMABUILD_READER_HELPER_ROOT": "/outer/root",
             "OTHER": "kept"}
    monkeypatch.setitem(_sys.modules, "resource_broker", None)
    monkeypatch.setitem(_sys.modules, "runtime_paths", None)
    stamped = resource_exec.payload_identity_env(
        outer, action_key="a" * 64, nonce="b" * 32)
    assert "PRISMABUILD_ACTION_NONCE" not in stamped
    assert "PRISMABUILD_ACTION_SCOPE" not in stamped
    assert "PRISMABUILD_READER_HELPER_ROOT" not in stamped
    assert stamped["OTHER"] == "kept"
    assert outer["PRISMABUILD_ACTION_NONCE"] == "o" * 32


def test_foreign_owner_certificate_never_releases(fleet) -> None:
    """Same nonce/scope/host/worker under another action frees nothing."""

    FOREIGN = "f" * 64
    queue, stage = fleet
    staged = stage / "model" / "fo.bin"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x65" * 1024)
    root = queue.root / pool.RESIDENCY
    key = residency_map.residency_map_key("/mnt/shared/fo.bin", 0)
    _publish(root, stage, CONSUMER, MOVER, "/mnt/shared/fo.bin",
             staged, 1024, "b" * 64)
    acquired = _acquire(queue, MOVER, "foreign-token")
    assert acquired["ok"]
    target = {"consumer_action_key": CONSUMER, "pin_id": acquired["pin_id"],
              "ref_id": acquired["ref_id"]}
    # Helper: valid proof, but for ANOTHER action reusing the same
    # attempt strings.
    foreign = reader_lease.attestation_path(queue, FOREIGN, "n1")
    foreign.parent.mkdir(parents=True, exist_ok=True)
    foreign.write_text(json.dumps({
        "schema": reader_lease.ATTESTATION_SCHEMA_V1,
        "action_key": FOREIGN, "nonce": "n1", "scope_id": "s1",
        "host": "test-host", "worker": "w1", "incarnation": "w1",
        "scope_empty": True, "released": True, "retired": False,
        "settled": False, "empty": True, "tickets_pending": False,
        "stopped_unix": 1789870000.0,
        "unix": 1789880000.0}) + "\n")
    failed = queue.dir(pool.FAILED)
    failed.mkdir(parents=True, exist_ok=True)
    (failed / f"{FOREIGN}.json").write_text(json.dumps(
        {"action_key": FOREIGN, "status": "failed",
         "resource_telemetry": {
             "action_key": FOREIGN, "nonce": "n1", "scope_unit": "s1",
             "host": "test-host"}}))
    cert = {"action_key": FOREIGN, "nonce": "n1", "scope_id": "s1",
            "worker": "w1", "host": "test-host"}
    refused = reader_lease.release_refs(queue, [target], dict(cert))
    assert refused["ok"] is False, refused
    assert refused["skipped"] == ["%s: owner mismatch" % acquired["ref_id"]]
    assert staged.exists()


def test_tombstone_without_settlement_retains(fleet) -> None:
    """Retired, unsettled proof is not containment even when marked empty."""

    queue, stage = fleet
    staged = stage / "model" / "ts.bin"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x66" * 1024)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER, "/mnt/shared/ts.bin",
             staged, 1024, "b" * 64)
    acquired = _acquire(queue, MOVER, "tomb-token")
    assert acquired["ok"]
    target = {"consumer_action_key": CONSUMER, "pin_id": acquired["pin_id"],
              "ref_id": acquired["ref_id"]}
    tomb = reader_lease.attestation_path(queue, CONSUMER, "n1")
    tomb.parent.mkdir(parents=True, exist_ok=True)
    tomb.write_text(json.dumps({
        "schema": reader_lease.ATTESTATION_SCHEMA_V1,
        "action_key": CONSUMER, "nonce": "n1", "scope_id": "s1",
        "host": "test-host", "worker": "w1", "incarnation": "w1",
        "scope_empty": True, "released": False, "retired": True,
        "settled": False, "empty": True, "tickets_pending": False,
        "stopped_unix": 1789870000.0,
        "unix": 1789880000.0}) + "\n")
    done_dir = queue.dir(pool.DONE)
    done_dir.mkdir(parents=True, exist_ok=True)
    (done_dir / f"{CONSUMER}.json").write_text(json.dumps(
        {"action_key": CONSUMER, "status": "executed",
         "resource_telemetry": {
             "action_key": CONSUMER, "nonce": "n1", "scope_unit": "s1",
             "host": "test-host"}}))
    cert = {"action_key": CONSUMER, "nonce": "n1", "scope_id": "s1",
            "host": "test-host"}
    refused = reader_lease.release_refs(queue, [target], dict(cert))
    assert refused["ok"] is False
    assert refused["reason"] == "tombstone-unsettled-retain"
    assert staged.exists()


def test_missing_tickets_pending_is_unknown_never_proof(fleet) -> None:
    """A scope_empty True file without tickets_pending retains (unknown)."""

    queue, stage = fleet
    staged = stage / "model" / "ut.bin"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x69" * 1024)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER, "/mnt/shared/ut.bin",
             staged, 1024, "b" * 64)
    acquired = _acquire(queue, MOVER, "unknown-tickets-token")
    assert acquired["ok"]
    target = {"consumer_action_key": CONSUMER, "pin_id": acquired["pin_id"],
              "ref_id": acquired["ref_id"]}
    # Helper: scope_empty True with the ticket field absent -- unknown
    # absence, never proof of none.
    path = reader_lease.attestation_path(queue, CONSUMER, "n1")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "schema": reader_lease.ATTESTATION_SCHEMA_V1,
        "action_key": CONSUMER, "nonce": "n1", "scope_id": "s1",
        "host": "test-host", "worker": "w1", "incarnation": "w1",
        "scope_empty": True, "released": True, "retired": False,
        "settled": False, "empty": True,
        "stopped_unix": 1789870000.0,
        "unix": 1789880000.0}) + "\n")
    done_dir = queue.dir(pool.DONE)
    done_dir.mkdir(parents=True, exist_ok=True)
    (done_dir / f"{CONSUMER}.json").write_text(json.dumps(
        {"action_key": CONSUMER, "status": "executed",
         "resource_telemetry": {
             "action_key": CONSUMER, "nonce": "n1", "scope_unit": "s1",
             "host": "test-host"}}))
    cert = {"action_key": CONSUMER, "nonce": "n1", "scope_id": "s1",
            "host": "test-host"}
    refused = reader_lease.release_refs(queue, [target], dict(cert))
    assert refused["ok"] is False
    assert refused["reason"] == "tickets-unknown-retain"
    assert staged.exists()


def test_export_verdict_validation_is_typed(fleet) -> None:
    """Helper: the export validator refuses untyped/incomplete verdicts."""

    queue, _stage = fleet
    scope = "unit-9"
    clean = {"scope_id": scope, "released": True, "retired": False,
             "settled": False, "empty": True, "tickets_pending": False,
             "stopped_unix": 1789870000.0}
    assert reader_lease.export_verdict_proves_empty(
        dict(clean), scope_id=scope) == (True, "proven")
    cases = [
        ({"scope_id": "other", **{k: v for k, v in clean.items()
                                  if k != "scope_id"}},
         "export-scope-mismatch-retain"),
        ({k: v for k, v in clean.items() if k != "tickets_pending"},
         "export-tickets-unknown-retain"),
        ({**clean, "tickets_pending": True}, "export-tickets-pending-retain"),
        ({k: v for k, v in clean.items() if k != "empty"},
         "export-not-empty-retain"),
        ({**clean, "empty": False}, "export-not-empty-retain"),
        ({k: v for k, v in clean.items() if k != "stopped_unix"},
         "export-unstopped-retain"),
        ({**clean, "stopped_unix": 0}, "export-unstopped-retain"),
        ({**clean, "stopped_unix": float("nan")}, "export-unstopped-retain"),
        ({**clean, "stopped_unix": float("inf")}, "export-unstopped-retain"),
        ({**clean, "stopped_unix": "1789870000.0"},
         "export-unstopped-retain"),
        ({**clean, "stopped_unix": True}, "export-unstopped-retain"),
        ({**clean, "released": 1}, "export-verdict-untyped-retain"),
        ({**clean, "released": False}, "export-proof-incomplete-retain"),
        ("not-a-verdict", "export-unreadable-retain"),
        (None, "export-unreadable-retain"),
    ]
    for verdict, reason in cases:
        assert reader_lease.export_verdict_proves_empty(
            verdict, scope_id=scope) == (False, reason), verdict
    # The attestation shortcut validates too: scope_empty True alone,
    # without the exact export booleans, proves nothing.
    ok, _att = reader_lease.attestation_proves_empty(
        queue, CONSUMER, "n1", "s1")
    assert ok is False


def test_writer_maps_verdicts_to_empty_honestly(fleet) -> None:
    """Helper: _persist files True only for fully typed broker exports."""

    queue, stage = fleet
    record = {"action_key": CONSUMER, "claimed_by": "sparklina:99:abc12345",
        "resources": {"cpu": 1, "mem_gb": 1},
              "claimed_host": "sparklina"}
    clean = {"scope_id": "unit-1", "released": True, "retired": False,
             "settled": False, "empty": True, "tickets_pending": False,
             "stopped_unix": 1789870000.0,
             "termination_evidence": {"stop": "done"}}
    assert queue._persist_reader_scope_proof(
        record, "n" * 32, "unit-1", dict(clean)) is True
    attestation = reader_lease.read_scope_attestation(queue, CONSUMER,
                                                      "n" * 32)
    assert attestation["scope_empty"] is True
    assert attestation["host"] == "sparklina"
    assert attestation["worker"] == "sparklina:99:abc12345"
    assert attestation["incarnation"] == "sparklina:99:abc12345"
    assert attestation["empty"] is True
    assert attestation["tickets_pending"] is False
    assert attestation["termination_evidence"] == {"stop": "done"}
    # A verdict for another scope files False: identity pairing first.
    assert queue._persist_reader_scope_proof(
        record, "n" * 32, "unit-9", dict(clean)) is True
    assert reader_lease.read_scope_attestation(
        queue, CONSUMER, "n" * 32)["scope_empty"] is False
    tombstone = {"scope_id": "unit-1", "released": False, "retired": True,
                 "settled": False, "empty": True, "tickets_pending": False,
                 "stopped_unix": 1789870000.0}
    assert queue._persist_reader_scope_proof(
        record, "m" * 32, "unit-1", dict(tombstone)) is True
    tomb = reader_lease.read_scope_attestation(queue, CONSUMER, "m" * 32)
    assert tomb["scope_empty"] is False
    settled = {"scope_id": "unit-1", "released": False, "retired": True,
               "settled": True, "empty": True, "tickets_pending": False,
               "stopped_unix": 1789870000.0}
    assert queue._persist_reader_scope_proof(
        record, "k" * 32, "unit-1", dict(settled)) is True
    assert reader_lease.read_scope_attestation(
        queue, CONSUMER, "k" * 32)["scope_empty"] is True
    # Missing tickets_pending is unknown absence, never proof of none.
    no_tickets = {"scope_id": "unit-1", "released": True, "retired": False,
                  "settled": False, "empty": True,
                  "stopped_unix": 1789870000.0}
    assert queue._persist_reader_scope_proof(
        record, "t" * 32, "unit-1", dict(no_tickets)) is True
    assert reader_lease.read_scope_attestation(
        queue, CONSUMER, "t" * 32)["scope_empty"] is False
    reboot = {"released": True}
    assert queue._persist_reader_scope_proof(
        record, "j" * 32, "unit-1", dict(reboot)) is True
    assert reader_lease.read_scope_attestation(
        queue, CONSUMER, "j" * 32)["scope_empty"] is False


def test_shortcut_recovery_republishes_missing_proof(
        fleet, tmp_path, monkeypatch) -> None:
    """A blipped proof heals on the next production cleanup shortcut.

    Real socket broker, real broker-minted control, production
    ``cleanup_action_containers`` only (never the private recovery):
    the first cleanup files the proof, the file is lost, and the second
    cleanup's shortcut republishes from a fresh token-gated export for
    the exact attempt.  The first release reply carries no ``released``
    flag -- the R8 protocol shape -- while the export does.
    """

    from prismabuild import resource_scope

    queue, stage = fleet
    root = queue.root / pool.RESIDENCY
    nonce = "b" * 32
    with _ConnectedBroker(tmp_path) as broker:
        monkeypatch.setattr(resource_scope, "BROKER_SOCKET", broker.endpoint)
        unit, host, worker, staged, acquired = _connected_setup(
            queue, stage, root, tmp_path, broker, nonce, "shortcut-token")
        record = json.loads(
            (queue.dir(pool.CLAIMED) / f"{CONSUMER}.json").read_text())
        first = queue.cleanup_action_containers(record)
        assert first["complete"] is True
        assert "released" not in first["resource_scope"]["released"]
        assert first["resource_scope"]["export"]["released"] is True
        assert first["resource_scope"]["export"]["scope_id"] == unit
        attestation = reader_lease.read_scope_attestation(
            queue, CONSUMER, nonce)
        assert isinstance(attestation, dict)
        assert attestation["scope_empty"] is True
        assert attestation["scope_id"] == unit
        # The shared mount loses the file; the next production cleanup
        # heals it through the shortcut, not through a manual call.
        reader_lease.attestation_path(queue, CONSUMER, nonce).unlink()
        assert reader_lease.read_scope_attestation(
            queue, CONSUMER, nonce) is None
        second = queue.cleanup_action_containers(record)
        assert second["complete"] is True
        healed = reader_lease.read_scope_attestation(queue, CONSUMER, nonce)
        assert isinstance(healed, dict)
        assert healed["scope_empty"] is True
        assert healed["scope_id"] == unit
        assert healed["host"] == host
        assert healed["worker"] == worker


def test_connected_sdk_ref_reclaims_through_egress(
        fleet, tmp_path, monkeypatch) -> None:
    """REAL connected finish: socket broker -> cleanup -> DONE -> egress.

    No handcrafted proof anywhere: the scope is created through the real
    RPC, the claim carries the broker-minted control, the SDK ref is
    acquired under the real attempt, and ``queue.finish`` drives the
    production terminate/release/export/persist path.  Asserts the first
    release reply, the saved proof, and the terminal are the actual
    production values; ordinary completion auto-reclaims exactly once.
    The host everywhere is the real local hostname via real derivation
    (claim holder, SDK holder, sample telemetry), never handwritten.
    """

    from prismabuild import resource_scope

    queue, stage = fleet
    root = queue.root / pool.RESIDENCY
    nonce = "b" * 32
    with _ConnectedBroker(tmp_path) as broker:
        monkeypatch.setattr(resource_scope, "BROKER_SOCKET", broker.endpoint)
        unit, host, worker, staged, acquired = _connected_setup(
            queue, stage, root, tmp_path, broker, nonce, "connected-token")
        terminal = queue.finish(CONSUMER, status="executed", detail={})
        assert terminal == queue.item_path(pool.DONE, CONSUMER)
        done = json.loads(terminal.read_text())
        cleanup = done["resource_scope_cleanup"]
        first = cleanup["released"]
        assert first["ok"] is True
        assert first["scope_id"] == unit
        assert "released" not in first
        export = cleanup["export"]
        assert export["scope_id"] == unit
        assert export["released"] is True
        assert export["empty"] is True
        assert export["tickets_pending"] is False
        telemetry = cleanup["telemetry"]
        assert telemetry["action_key"] == CONSUMER
        assert telemetry["nonce"] == nonce
        assert telemetry["scope_unit"] == unit
        assert telemetry["host"] == host
        proof = reader_lease.read_scope_attestation(queue, CONSUMER, nonce)
        assert isinstance(proof, dict)
        assert proof["scope_empty"] is True
        assert proof["scope_id"] == unit
        assert proof["host"] == host
        assert proof["worker"] == worker
        assert proof["incarnation"] == worker
        assert proof["empty"] is True
        assert proof["tickets_pending"] is False
        receipt = stage_release.evict(queue, MOVER,
                                      consumer_action_key=CONSUMER,
                                      stage_root=str(stage))
        assert receipt["auto_reclaimed"] == [acquired["ref_id"]], receipt
        assert not staged.exists()
        assert receipt["complete"] is True
        assert receipt["entries_deleted"] == 1
        again = stage_release.evict(queue, MOVER,
                                    consumer_action_key=CONSUMER,
                                    stage_root=str(stage))
        assert again["auto_reclaimed"] == []


def test_finish_write_failure_heals_on_egress_tick(
        fleet, tmp_path, monkeypatch) -> None:
    """A proof lost after finish heals on the normal egress tick.

    The attestation write fails exactly once during the real finish (a
    shared-mount blip: unwritable directory, or one injected OSError as
    root), so DONE publishes with the broker export but no proof file.
    No private recovery is called: the next ordinary
    ``stage_release.evict`` -- the deployed reconciliation tick --
    replays the terminal's validated export and reclaims.  Missing
    publication before the tick retains.
    """

    from prismabuild import resource_scope

    queue, stage = fleet
    root = queue.root / pool.RESIDENCY
    nonce = "c" * 32
    with _ConnectedBroker(tmp_path) as broker:
        monkeypatch.setattr(resource_scope, "BROKER_SOCKET", broker.endpoint)
        unit, host, worker, staged, acquired = _connected_setup(
            queue, stage, root, tmp_path, broker, nonce, "blip-token",
            source="/mnt/shared/bl.bin", name="bl.bin", size=1024)
        attest_dir = reader_lease.attestation_path(
            queue, CONSUMER, nonce).parent
        attest_dir.mkdir(parents=True, exist_ok=True)
        if os.geteuid() == 0:
            calls = {"n": 0}
            real = queue._persist_reader_scope_proof

            def flaky(record, nonce_, scope_id, export):
                calls["n"] += 1
                if calls["n"] == 1:
                    raise OSError("simulated shared-mount blip")
                return real(record, nonce_, scope_id, export)

            monkeypatch.setattr(
                queue, "_persist_reader_scope_proof", flaky)
            terminal = queue.finish(CONSUMER, status="executed", detail={})
        else:
            os.chmod(attest_dir, 0o555)
            try:
                terminal = queue.finish(
                    CONSUMER, status="executed", detail={})
            finally:
                os.chmod(attest_dir, 0o755)
        assert terminal == queue.item_path(pool.DONE, CONSUMER)
        assert reader_lease.read_scope_attestation(
            queue, CONSUMER, nonce) is None
        done = json.loads(terminal.read_text())
        assert done["resource_scope_cleanup"]["export"]["released"] is True
        assert "proof_persistence_error" in (
            done["resource_scope_cleanup"]["telemetry"])
        assert staged.exists()
        receipt = stage_release.evict(queue, MOVER,
                                      consumer_action_key=CONSUMER,
                                      stage_root=str(stage))
        assert receipt["auto_reclaimed"] == [acquired["ref_id"]], receipt
        assert not staged.exists()
        proof = reader_lease.read_scope_attestation(queue, CONSUMER, nonce)
        assert isinstance(proof, dict)
        assert proof["scope_empty"] is True
        assert proof["scope_id"] == unit
        assert proof["host"] == host


def test_unsettled_ticket_retains_then_settles_through_cleanup(
        fleet, tmp_path, monkeypatch) -> None:
    """Empty-but-unsettled retains; settlement releases, production only.

    A container ticket left unresolved at cleanup retires the scope
    without settlement: the pool files scope_empty False and the egress
    retains.  After the holder settles through the real broker settle
    op, the next production cleanup (shortcut recovery reading a fresh
    export) republishes True and the egress reclaims.  No invented
    proof at any step; an incomplete export never replays.
    """

    from prismabuild import resource_scope

    queue, stage = fleet
    root = queue.root / pool.RESIDENCY
    nonce = "d" * 32
    with _ConnectedBroker(tmp_path) as broker:
        monkeypatch.setattr(resource_scope, "BROKER_SOCKET", broker.endpoint)
        unit, host, worker, staged, acquired = _connected_setup(
            queue, stage, root, tmp_path, broker, nonce, "ticket-token",
            source="/mnt/shared/tk.bin", name="tk.bin", size=1024)
        record = json.loads(
            (queue.dir(pool.CLAIMED) / f"{CONSUMER}.json").read_text())
        control = record["resource_scope"]
        # A late container lands with an unresolved ticket (broker state).
        broker.authority.records[unit]["container_tickets"] = ["ticket-1"]
        first = queue.cleanup_action_containers(record)
        assert first["complete"] is True
        assert first["resource_scope"]["released"]["retired"] is True
        proof = reader_lease.read_scope_attestation(queue, CONSUMER, nonce)
        assert isinstance(proof, dict)
        assert proof["scope_empty"] is False
        assert proof["retired"] is True
        assert proof["settled"] is False
        kept = stage_release.evict(queue, MOVER,
                                   consumer_action_key=CONSUMER,
                                   stage_root=str(stage))
        assert staged.exists()
        assert kept["auto_reclaimed"] == []
        assert kept["auto_retained"] == {
            acquired["ref_id"]: "scope-not-empty-retain"}
        # The holder settles through the real broker authority; the next
        # production cleanup reads a fresh export and republishes True.
        evidence = {"schema": broker.module.SETTLEMENT_SCHEMA,
                    "marker_absent": True, "owner_container_ids": [],
                    "scope_container_ids": [], "checked_unix": 1789870000.0}
        settled = broker.authority.handle(
            os.getuid(), os.getpid(),
            {"op": "settle", "action_key": CONSUMER, "nonce": nonce,
             "token": control["token"], "evidence": evidence})
        assert settled["settled"] is True
        second = queue.cleanup_action_containers(record)
        assert second["complete"] is True
        proof2 = reader_lease.read_scope_attestation(queue, CONSUMER, nonce)
        assert isinstance(proof2, dict)
        assert proof2["scope_empty"] is True
        assert proof2["settled"] is True
        assert proof2["retired"] is True
        # Ordinary finish files the terminal; the egress reclaims.
        terminal = queue.finish(CONSUMER, status="executed", detail={})
        assert terminal == queue.item_path(pool.DONE, CONSUMER)
        receipt = stage_release.evict(queue, MOVER,
                                      consumer_action_key=CONSUMER,
                                      stage_root=str(stage))
        assert receipt["auto_reclaimed"] == [acquired["ref_id"]], receipt
        assert not staged.exists()


def test_unknown_scan_never_reports_released(fleet) -> None:
    """An unreadable census is unknown absence: retain/refuse, not released."""

    if os.geteuid() == 0:
        pytest.skip("permission-gated census needs a non-root reader")
    queue, stage = fleet
    staged = stage / "model" / "uc.bin"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x68" * 1024)
    root = queue.root / pool.RESIDENCY
    _publish(root, stage, CONSUMER, MOVER, "/mnt/shared/uc.bin",
             staged, 1024, "b" * 64)
    acquired = _acquire(queue, MOVER, "unknown-token")
    assert acquired["ok"]
    leases = reader_lease.leases_root(queue)
    os.chmod(leases, 0o000)
    try:
        # Full scan unreadable: no released report.
        assert reader_lease.release(
            queue, acquired["pin_id"], acquired["ref_id"]) is False
        owners, tainted = reader_lease.live_for(
            queue, {os.path.normpath(str(staged))})
        assert owners == {} and len(tainted) == 1
        # The egress fails closed on the same census.
        receipt = stage_release.evict(queue, MOVER,
                                      consumer_action_key=CONSUMER,
                                      stage_root=str(stage))
        assert staged.exists()
        assert receipt["complete"] is False
    finally:
        os.chmod(leases, 0o755)
    assert reader_lease.release(
        queue, acquired["pin_id"], acquired["ref_id"],
        consumer_action_key=CONSUMER) is True

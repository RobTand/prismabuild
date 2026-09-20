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
             expected=None):
    covers = ([{"mover_action_key": mover, "manifest_sha256": "a" * 64}]
              if movers is None else
              [{"mover_action_key": m, "manifest_sha256": "a" * 64}
               for m in movers])
    return reader_lease.acquire(
        queue, consumer_action_key=consumer, attempt=attempt, tier_id=TIER,
        epoch="", span={"start_bytes": 0, "end_bytes": 4096},
        holder=holder, acquire_token=token, covers=covers, expected=expected)


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
            fd, serving = reader_lease.open_pinned(acquired["pin"], key)
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
        reader_lease.open_pinned(old["pin"], key)
    fresh = _acquire(queue, MOVER, "aba-token-2")
    assert fresh["ok"]
    assert fresh["pin"]["entries"][0]["generation"] != old["pin"]["entries"][0]["generation"]  # type: ignore[index]
    fd, _ = reader_lease.open_pinned(fresh["pin"], key)
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


def test_containment_needs_terminal_and_attestation(fleet) -> None:
    """No terminal or a live scope retains; certified containment releases."""

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
    reader_lease.write_scope_attestation(
        queue, action_key=CONSUMER, nonce="n1", scope_id="s1",
        host="test-host", worker="w1", incarnation="i1", scope_empty=False)
    live_scope = reader_lease.release_refs(queue, [target], dict(cert))
    assert live_scope["ok"] is False
    assert live_scope["reason"] == "scope-not-empty-retain"
    assert staged.exists()

    # Attestation for a stopped scope, but no terminal evidence yet: retain.
    reader_lease.write_scope_attestation(
        queue, action_key=CONSUMER, nonce="n1", scope_id="s1",
        host="test-host", worker="w1", incarnation="i1", scope_empty=True)
    no_terminal = reader_lease.release_refs(queue, [target], dict(cert))
    assert no_terminal["ok"] is False
    assert no_terminal["reason"] == "no-terminal-evidence-retain"

    # A terminal record for a DIFFERENT attempt (a retry is current): the
    # certified attempt is superseded, not contained -- retain.
    failed = queue.dir(pool.FAILED)
    failed.mkdir(parents=True, exist_ok=True)
    (failed / f"{CONSUMER}.json").write_text(json.dumps(
        {"action_key": CONSUMER, "status": "failed",
         "resource_scope": {"nonce": "n9", "unit": "s1"}}))
    superseded = reader_lease.release_refs(queue, [target], dict(cert))
    assert superseded["ok"] is False
    assert superseded["reason"] == "terminal-names-newer-attempt-retain"

    # Exact attempt terminal plus stopped-scope attestation: release.
    (failed / f"{CONSUMER}.json").write_text(json.dumps(
        {"action_key": CONSUMER, "status": "failed",
         "resource_scope": {"nonce": "n1", "unit": "s1"}}))
    certified = reader_lease.release_refs(queue, [target], dict(cert))
    assert certified["ok"] is True, certified
    assert certified["released"] == [acquired["ref_id"]]
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

"""A rolling receipt activation needs every enrolled host's current installed-updater proof (#1850).

Each proof is an ordinary PB action: a real sealed request, a real CAS receipt
and a real queue ending, claimed under the host's own name. The publisher reads
it back through the verified action-result read and judges it with
``updater_proof.judge``. The refusal names the host that lacks the proof.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild.compatibility_window import (  # noqa: E402
    WindowError, client_protocol, require_client_transition, require_receipt_participants,
)
from receipt_reader_fixture import RECEIPT_LEAVES  # noqa: E402
from test_receipt_activation_direction import generation, private_store, source  # noqa: E402,F401
from test_rollout_coordinator import publisher  # noqa: E402
from test_verified_action_result import _finish, _publish_result, _standard_action  # noqa: E402

HOSTS = ("one", "two")
CAPABLE = hashlib.sha256(source("final")).hexdigest()


def proof_tool():
    """The tool under test, imported where it is used so the window tests run without it."""

    import updater_proof
    return updater_proof


@pytest.fixture(autouse=True)
def _no_outer_launch_identity(monkeypatch):
    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT"):
        monkeypatch.delenv(name, raising=False)


def receipt_direction(store):
    previous = generation(store, "previous", "final", tuple(RECEIPT_LEAVES))
    target = generation(store, "target", "final", tuple(RECEIPT_LEAVES))
    return require_client_transition(client_protocol(target), client_protocol(previous))


def test_the_refusal_names_the_participant_without_proof(private_store):
    direction = receipt_direction(private_store)
    with pytest.raises(WindowError, match=r"current installed updater proof.*\['two'\]"):
        require_receipt_participants(direction, {"one": CAPABLE}, HOSTS)


def test_the_refusal_names_the_participant_whose_updater_is_incapable(private_store):
    direction = receipt_direction(private_store)
    with pytest.raises(WindowError, match=r"receipt-member recovery protocol 1.*\['two'\]"):
        require_receipt_participants(direction, {"one": CAPABLE, "two": "0" * 64}, HOSTS)


def test_proof_for_every_participant_passes(private_store):
    direction = receipt_direction(private_store)
    assert require_receipt_participants(direction, {"one": CAPABLE, "two": CAPABLE}, HOSTS) is None


def test_a_proof_from_outside_the_roster_is_refused_by_name(private_store):
    direction = receipt_direction(private_store)
    with pytest.raises(WindowError, match=r"outside the enrolled roster.*\['three'\]"):
        require_receipt_participants(
            direction, {"one": CAPABLE, "two": CAPABLE, "three": CAPABLE}, HOSTS)


def enrolled_host(base, host, monkeypatch, *, updater=None, state="current", tick_age=10.0):
    """One host's root-owned updater files, as an ordinary user reads them."""

    install, status_dir = base / host / "opt", base / host / "state"
    install.mkdir(parents=True)
    status_dir.mkdir(parents=True)
    raw = source("final") if updater is None else updater
    (install / "upgrade_client.py").write_bytes(raw)
    (install / "upgrade_client.py").chmod(0o644)
    (status_dir / "status.json").write_text(json.dumps({
        "schema": "prismabuild.client_upgrade.v1", "state": state, "host": host,
        "checked_unix": time.time() - tick_age,
        "installed": {"upgrade_client.py": hashlib.sha256(raw).hexdigest()}}))
    enrollment = base / host / "client-upgrade.json"
    enrollment.write_text(json.dumps({"install_dir": str(install), "state_dir": str(status_dir)}))
    monkeypatch.setattr(socket, "gethostname", lambda: host)
    return proof_tool().observe_installed_updater(nonce=host, enrollment=enrollment)


def proof_action(fleet, host, proof, monkeypatch):
    """Run nothing; file the ending a proof action on ``host`` would file."""

    base = fleet / "proof-actions" / f"{host}-{proof['nonce']}"
    (base / "checkout").mkdir(parents=True)
    action = _standard_action(base / "checkout", command=[
        "python3", "tools/fleet/updater_proof.py", "--nonce", str(proof["nonce"])])
    payload = b"pbrun: log capture executable=/usr/bin/tee\n" + json.dumps(proof).encode() + b"\n"
    _cas, receipt = _publish_result(base, action, payload)
    monkeypatch.setattr(socket, "gethostname", lambda: host)
    _finish(base, action, receipt, queue_root=fleet / "pb-queue")
    return str(action["action_key"])


@pytest.fixture
def receipt_fleet(private_store, monkeypatch):
    """Two receipt-carrying generations with the same updater, two live offers."""

    previous = generation(private_store, "previous", "final", tuple(RECEIPT_LEAVES))
    target = generation(private_store, "target", "final", tuple(RECEIPT_LEAVES))
    for root in (previous, target):
        publisher._seal_generation(root)
    publisher.MIRROR.symlink_to(previous)
    fleet = private_store.parent
    offers = fleet / "pb-queue/workers"
    offers.mkdir(parents=True)
    for host in HOSTS:
        (offers / f"{host}.json").write_text(json.dumps({
            "schema": "prismaquant.prismabuild.pool_offer.v1", "host": host,
            "announced_unix": time.time()}))
    monkeypatch.setattr(proof_tool(), "TRUSTED_UID", os.getuid())
    return fleet, previous, target


def activate(name, proofs, *, dry_run):
    return publisher._activate_existing(name, dry_run=dry_run, rollout="rolling",
                                        rollout_reason="same updater; receipt leaves unchanged",
                                        proofs=proofs)


@pytest.mark.parametrize("dry_run", [True, False])
def test_rolling_activation_succeeds_with_every_participants_proof(receipt_fleet, monkeypatch, dry_run):
    fleet, previous, target = receipt_fleet
    keys = [proof_action(fleet, host, enrolled_host(fleet / "hosts", host, monkeypatch), monkeypatch)
            for host in HOSTS]
    assert activate(target.name, keys, dry_run=dry_run) == 0
    assert publisher.MIRROR.resolve() == (previous if dry_run else target)


def test_rolling_activation_is_refused_naming_the_participant_without_proof(receipt_fleet, monkeypatch):
    fleet, previous, target = receipt_fleet
    keys = [proof_action(fleet, "one", enrolled_host(fleet / "hosts", "one", monkeypatch), monkeypatch)]
    with pytest.raises(SystemExit, match=r"current installed updater proof.*\['two'\]"):
        activate(target.name, keys, dry_run=False)
    assert publisher.MIRROR.resolve() == previous


def test_rolling_activation_without_any_proof_names_every_participant(receipt_fleet):
    fleet, previous, target = receipt_fleet
    with pytest.raises(SystemExit, match=r"current installed updater proof.*\['one', 'two'\]"):
        activate(target.name, [], dry_run=True)
    assert publisher.MIRROR.resolve() == previous


@pytest.mark.parametrize("defect,match", [
    ({"updater": source("old")}, r"receipt-member recovery protocol 1.*\['two'\]"),
    ({"state": "draining"}, r"proof for two shows updater state 'draining'"),
    ({"tick_age": 3600.0}, r"proof for two shows no timer tick"),
])
def test_a_participant_whose_proof_does_not_hold_is_named(receipt_fleet, monkeypatch, defect, match):
    fleet, previous, target = receipt_fleet
    keys = [proof_action(fleet, "one", enrolled_host(fleet / "hosts", "one", monkeypatch), monkeypatch),
            proof_action(fleet, "two", enrolled_host(fleet / "hosts", "two", monkeypatch, **defect),
                         monkeypatch)]
    with pytest.raises(SystemExit, match=match):
        activate(target.name, keys, dry_run=False)
    assert publisher.MIRROR.resolve() == previous


def test_a_proof_that_ran_on_another_host_is_not_that_hosts_proof(receipt_fleet, monkeypatch):
    fleet, previous, target = receipt_fleet
    one = enrolled_host(fleet / "hosts", "one", monkeypatch)
    forged = dict(enrolled_host(fleet / "hosts", "two", monkeypatch))
    keys = [proof_action(fleet, "one", one, monkeypatch),
            proof_action(fleet, "one", {**forged, "nonce": "again"}, monkeypatch)]
    with pytest.raises(SystemExit, match=r"proof for one names host 'two'"):
        activate(target.name, keys, dry_run=False)
    assert publisher.MIRROR.resolve() == previous


def test_a_stale_proof_is_refused(receipt_fleet, monkeypatch):
    fleet, previous, target = receipt_fleet
    stale = {**enrolled_host(fleet / "hosts", "two", monkeypatch)}
    stale["observed_unix"] -= proof_tool().PROOF_MAX_AGE_S + 60
    stale["status"] = {**stale["status"], "checked_unix": stale["observed_unix"] - 10}
    keys = [proof_action(fleet, "one", enrolled_host(fleet / "hosts", "one", monkeypatch), monkeypatch),
            proof_action(fleet, "two", stale, monkeypatch)]
    with pytest.raises(SystemExit, match=r"proof for two is not current"):
        activate(target.name, keys, dry_run=False)
    assert publisher.MIRROR.resolve() == previous


def test_unsafe_custody_of_the_installed_updater_is_refused(receipt_fleet, monkeypatch):
    fleet, previous, target = receipt_fleet
    proof = enrolled_host(fleet / "hosts", "two", monkeypatch)
    proof["updater"] = {**proof["updater"], "mode": 0o666}
    keys = [proof_action(fleet, "one", enrolled_host(fleet / "hosts", "one", monkeypatch), monkeypatch),
            proof_action(fleet, "two", proof, monkeypatch)]
    with pytest.raises(SystemExit, match=r"proof for two shows unsafe custody"):
        activate(target.name, keys, dry_run=False)
    assert publisher.MIRROR.resolve() == previous


def test_the_proof_tool_prints_one_line_and_writes_nothing(tmp_path, monkeypatch, capsys):
    proof = enrolled_host(tmp_path, "one", monkeypatch)
    before = sorted(tmp_path.rglob("*"))
    assert proof_tool().main(["--nonce", "n", "--enrollment",
                               str(tmp_path / "one/client-upgrade.json")]) == 0
    printed = capsys.readouterr().out.splitlines()
    assert len(printed) == 1
    assert proof_tool().last_proof(printed[0].encode())["updater"] == proof["updater"]
    assert sorted(tmp_path.rglob("*")) == before

"""A class-scoped measurement seals the WORKER's argv[0] identity (#1598).

The first two-host canary of the fix failed four times with
``worker toolchain field 'argv0.bytes' differs: declared='1540520',
observed='1543048'``.  pbrun took the platform, ABI, driver and GPU facts from
the packet but still read /bin/bash from the submitting box.  An x86_64
submitter and an aarch64 worker have different /bin/bash files, and the worker
refuses a declared size that is not its own.  The packet now names the
worker's identity and pbrun seals that.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

import pbevidence
import pbrun
from prismabuild import core as pb
from test_pbrun_target_evidence_1598 import _packet, _write


def _scope(packet, tmp_path):
    loaded = pbrun.load_target_evidence(str(_write(tmp_path, packet)), host_class="gb10")
    return pbrun.host_class_scope("gb10", measurement=True, transport="pool",
                                  target_evidence=loaded)


def test_the_toolchain_declares_the_workers_bash_and_not_the_submitters(tmp_path) -> None:
    local = pb.executable_toolchain_contract(pbrun.SEALED_ARGV0)
    packet = _packet()
    packet["argv0"]["bytes"] = str(int(local["argv0.bytes"]) + 2528)
    _, toolchain = _scope(packet, tmp_path)
    assert toolchain["argv0.bytes"] == packet["argv0"]["bytes"]
    assert toolchain["argv0.bytes"] != local["argv0.bytes"]
    assert toolchain["argv0.sha256"] == packet["argv0"]["sha256"]


def test_the_worker_check_accepts_what_the_packet_declared_and_refuses_the_submitters(tmp_path) -> None:
    """The real preflight function, with a submitter whose bash differs."""
    packet = _packet()
    _, declared = _scope(packet, tmp_path)
    worker = {"sha256": packet["argv0"]["sha256"], "bytes": int(packet["argv0"]["bytes"])}
    evidence = {k: v for k, v in packet.items() if k != "argv0"}
    pb._verified_toolchain(declared, executable=worker, evidence=evidence)
    stale = dict(declared, **{"argv0.bytes": "1540520"})     # what celestia declared
    with pytest.raises(pb.ActionContractError, match="argv0.bytes"):
        pb._verified_toolchain(stale, executable=worker, evidence=evidence)


def test_without_a_packet_the_submitters_own_bash_is_still_sealed() -> None:
    local = pb.executable_toolchain_contract(pbrun.SEALED_ARGV0)
    _, toolchain = pbrun.host_class_scope(None, measurement=True, transport="pool")
    assert toolchain["argv0.bytes"] == local["argv0.bytes"]
    assert toolchain["argv0.sha256"] == local["argv0.sha256"]


@pytest.mark.parametrize("change", [
    lambda p: p.pop("argv0"),
    lambda p: p.__setitem__("argv0", "x"),
    lambda p: p["argv0"].__setitem__("path", "/bin/sh"),
    lambda p: p["argv0"].__setitem__("sha256", "zz" * 32),
    lambda p: p["argv0"].__setitem__("sha256", "ab" * 31),
    lambda p: p["argv0"].__setitem__("bytes", "0"),
    lambda p: p["argv0"].__setitem__("bytes", "01543048"),
    lambda p: p["argv0"].__setitem__("bytes", 1543048),
    lambda p: p["argv0"].__setitem__("bytes", "-5"),
])
def test_a_packet_without_a_usable_argv0_is_refused_before_anything_is_sealed(
        tmp_path: Path, change) -> None:
    packet = _packet()
    change(packet)
    path = _write(tmp_path, packet)
    with pytest.raises(SystemExit, match="argv0"):
        pbrun.load_target_evidence(str(path), host_class="gb10")


def test_a_good_packet_loads_with_the_workers_identity_beside_the_evidence(tmp_path: Path) -> None:
    path = _write(tmp_path, _packet())
    loaded = pbrun.load_target_evidence(str(path), host_class="gb10")
    assert loaded.argv0 == {"argv0.sha256": "ab" * 32, "argv0.bytes": "1543048"}
    assert "argv0" not in loaded        # the core evidence mapping is strict


def test_evidence_without_the_loader_cannot_seal_a_class(tmp_path: Path) -> None:
    plain = {k: v for k, v in _packet().items() if k != "argv0"}
    with pytest.raises(ValueError, match="argv0"):
        pbrun.host_class_scope("gb10", measurement=True, transport="pool",
                               target_evidence=plain)


def test_the_collector_names_this_boxs_own_bash(monkeypatch) -> None:
    base = {k: v for k, v in _packet().items() if k != "argv0"}
    monkeypatch.setattr(pb, "_collect_worker_evidence", lambda **kw: dict(base))
    monkeypatch.setattr(pb, "executable_toolchain_contract",
                        lambda path: {"argv0.sha256": "cd" * 32, "argv0.bytes": "99"})
    packet = pbevidence.collect_packet()
    assert packet["argv0"] == {"path": "/bin/bash", "sha256": "cd" * 32, "bytes": "99"}
    json.dumps(packet)


# -- the scratch recorder's interpreter (review of PR 1611) --------------------

RECORDER_PYTHON = "/usr/bin/python3"
RECORDER = {"path": RECORDER_PYTHON, "sha256": "cd" * 32, "bytes": "7845048",
            "python": "3.12.3"}


def _recorder_body(packet, tmp_path, monkeypatch):
    """The body sealed for a scratch recorder under a class measurement packet."""
    from prismabuild import local_scratch
    from test_pbrun_host_class import _sealed_body
    monkeypatch.setattr(local_scratch, "check_profile_request",
                        lambda *a, **k: {"root": "."})
    monkeypatch.setattr(pb, "executable_toolchain_contract",
                        lambda path: {"argv0.sha256": "ee" * 32, "argv0.bytes": "1"})
    monkeypatch.setattr(pb, "_probe_python_toolchain", lambda path, *a: {"python": "9.9.9"})
    path = _write(tmp_path, packet)
    return _sealed_body(
        ["--measurement", "--host-class", "gb10", "--transport", "pool",
         "--target-evidence", str(path), "--", RECORDER_PYTHON,
         "tools/fleet/local_scratch_profile.py"],
        monkeypatch, tmp_path)


def test_a_recorder_seals_the_workers_interpreter_and_not_the_submitters(
        tmp_path, monkeypatch) -> None:
    body = _recorder_body(dict(_packet(), recorder=RECORDER), tmp_path, monkeypatch)
    toolchain = body["environment"]["toolchain"]
    assert toolchain["argv0.sha256"] == RECORDER["sha256"]
    assert toolchain["argv0.bytes"] == RECORDER["bytes"]
    assert toolchain["python"] == RECORDER["python"]
    assert "ee" * 32 not in toolchain.values() and "9.9.9" not in toolchain.values()


def test_a_recorder_without_the_workers_interpreter_is_refused(tmp_path, monkeypatch) -> None:
    with pytest.raises(SystemExit, match="--recorder-python"):
        _recorder_body(_packet(), tmp_path, monkeypatch)


def test_a_recorder_packet_for_another_interpreter_is_refused(tmp_path, monkeypatch) -> None:
    other = dict(RECORDER, path="/opt/other/python3")
    with pytest.raises(SystemExit, match="--recorder-python"):
        _recorder_body(dict(_packet(), recorder=other), tmp_path, monkeypatch)


@pytest.mark.parametrize("change", [
    {"path": "python3"}, {"sha256": "AB" * 32}, {"bytes": "07"}, {"python": ""},
])
def test_a_malformed_recorder_declaration_is_refused(change) -> None:
    with pytest.raises(pbevidence.PacketError):
        pbevidence.recorder_contract(dict(_packet(), recorder=dict(RECORDER, **change)))
    with pytest.raises(pbevidence.PacketError):
        pbevidence.recorder_contract(dict(_packet(), recorder={"path": RECORDER_PYTHON}))


def test_the_collector_names_the_recorder_interpreter_it_was_asked_for(monkeypatch) -> None:
    monkeypatch.setattr(pb, "_collect_worker_evidence", lambda **_k: _packet_evidence())
    monkeypatch.setattr(pb, "executable_toolchain_contract",
                        lambda path: {"argv0.sha256": RECORDER["sha256"],
                                      "argv0.bytes": RECORDER["bytes"]})
    monkeypatch.setattr(pb, "_probe_python_toolchain", lambda path, *a: {"python": "3.12.3"})
    packet = pbevidence.collect_packet(RECORDER_PYTHON)
    assert pbevidence.recorder_contract(packet)["python"] == "3.12.3"
    assert "recorder" not in pbevidence.collect_packet()


def _packet_evidence():
    return {k: v for k, v in _packet().items() if k != "argv0"}

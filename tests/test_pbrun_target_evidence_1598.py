"""A class-scoped GPU measurement takes its class facts from a vetted packet (#1598).

``pbrun --measurement --host-class gb10`` read the platform and GPU facts from
the box that submits.  A box without an accelerator could not seal them, so it
could not submit.  ``--target-evidence PATH`` replaces the local probe with a
packet that a GB10 worker printed.  Each worker still checks the declared facts
against its own live facts before it runs, so a wrong packet fails closed.  The
option adds no seal and no identity field.
"""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import core as pb  # noqa: E402
import pbevidence  # noqa: E402
import pbgang  # noqa: E402
import pbrun  # noqa: E402

from test_pbrun_host_class import _sealed_body  # noqa: E402


def _packet(host="sparky", uuid="GPU-1111", *, driver="595.91.07"):
    return {
        "source": "local", "hostname": host, "system": "linux",
        "machine": "aarch64", "libc": "glibc-2.39", "slurm": None,
        "accelerators": [{"kind": "nvidia", "compute_capability": "12.1",
                          "driver_version": driver, "name": "NVIDIA GB10",
                          "uuid": uuid}],
    }


def _write(tmp_path: Path, packet, name="packet.json") -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(packet))
    return path


def _no_probe(monkeypatch):
    """The submitting box has no accelerator: any local probe is a failure."""

    def _probe(**_kw):
        raise AssertionError("the local box was probed for accelerator facts")

    monkeypatch.setattr(pb, "_collect_worker_evidence", _probe)


CLASS_ARGS = ["--measurement", "--host-class", "gb10", "--gpu"]


def test_the_packet_seals_the_class_facts_without_probing_this_box(
        monkeypatch, tmp_path):
    _no_probe(monkeypatch)
    packet = _write(tmp_path, _packet())
    body = _sealed_body(
        [*CLASS_ARGS, "--target-evidence", str(packet), "--", "true"],
        monkeypatch, tmp_path)
    assert body["execution_scope"]["portability"] == "platform_keyed"
    assert body["execution_scope"]["platform_key"] == "linux-aarch64-sm121"
    toolchain = body["environment"]["toolchain"]
    assert toolchain["cuda_compute_capability"] == "12.1"
    assert toolchain["nvidia_driver"] == "595.91.07"
    assert toolchain["accelerator_models.sha256"] == pb.accelerator_models_contract(
        _packet())


@pytest.mark.parametrize("host,uuid", [("sparky", "GPU-1111"),
                                       ("sparklina", "GPU-2222")])
def test_a_packet_from_either_worker_seals_the_same_scope_and_toolchain(
        monkeypatch, tmp_path, host, uuid):
    """The host name and the device UUID are provenance, never identity."""

    _no_probe(monkeypatch)
    reference = _sealed_body(
        [*CLASS_ARGS, "--target-evidence",
         str(_write(tmp_path, _packet(), "reference.json")), "--", "true"],
        monkeypatch, tmp_path)
    other = _sealed_body(
        [*CLASS_ARGS, "--target-evidence",
         str(_write(tmp_path, _packet(host, uuid), "other.json")), "--", "true"],
        monkeypatch, tmp_path)
    assert other["execution_scope"] == reference["execution_scope"]
    assert other["environment"]["toolchain"] == reference["environment"]["toolchain"]


def test_the_packet_equals_a_live_probe_of_the_same_facts(monkeypatch, tmp_path):
    packet = _write(tmp_path, _packet("sparklina", "GPU-2222"))
    _no_probe(monkeypatch)
    with_packet = _sealed_body(
        [*CLASS_ARGS, "--target-evidence", str(packet), "--", "true"],
        monkeypatch, tmp_path)
    monkeypatch.setattr(pb, "_collect_worker_evidence", lambda **kw: _packet())
    probed = _sealed_body([*CLASS_ARGS, "--", "true"], monkeypatch, tmp_path)
    assert with_packet["execution_scope"] == probed["execution_scope"]
    assert with_packet["environment"]["toolchain"] == probed["environment"]["toolchain"]


def test_without_the_option_a_box_without_an_accelerator_is_still_refused(
        monkeypatch, tmp_path):
    bare = {**_packet(), "accelerators": []}
    monkeypatch.setattr(pb, "_collect_worker_evidence", lambda **kw: bare)
    with pytest.raises(SystemExit) as exc:
        _sealed_body([*CLASS_ARGS, "--", "true"], monkeypatch, tmp_path)
    assert "requires live accelerator model, compute capability and driver" in str(
        exc.value)


@pytest.mark.parametrize("arguments,fragment", [
    (["--host-class", "gb10", "--gpu"], "--measurement"),
    (["--measurement", "--gpu"], "--host-class"),
    (["--measurement", "--host-class", "gb10", "--gpu", "--transport", "slurm"],
     "pool transport"),
])
def test_the_option_needs_a_pool_class_measurement(
        monkeypatch, tmp_path, arguments, fragment):
    _no_probe(monkeypatch)
    packet = _write(tmp_path, _packet())
    with pytest.raises(SystemExit) as exc:
        _sealed_body([*arguments, "--target-evidence", str(packet), "--", "true"],
                     monkeypatch, tmp_path)
    assert "--target-evidence" in str(exc.value)
    assert fragment in str(exc.value)


def _refusal(tmp_path, packet=None, *, raw: bytes | None = None,
             host_class="gb10") -> str:
    path = tmp_path / "packet.json"
    if raw is not None:
        path.write_bytes(raw)
    elif packet is not None:
        path.write_text(json.dumps(packet))
    with pytest.raises(SystemExit) as exc:
        pbrun.load_target_evidence(str(path), host_class=host_class)
    return str(exc.value)


def test_a_missing_file_is_refused(tmp_path):
    with pytest.raises(SystemExit) as exc:
        pbrun.load_target_evidence(str(tmp_path / "absent.json"), host_class="gb10")
    assert "cannot read --target-evidence" in str(exc.value)


def test_a_file_that_is_not_json_is_refused(tmp_path):
    assert "not JSON" in _refusal(tmp_path, raw=b"{not json")


def test_an_oversize_file_is_refused(tmp_path):
    raw = b" " * (pbrun.TARGET_EVIDENCE_MAX_BYTES + 1)
    assert "exceeds" in _refusal(tmp_path, raw=raw)


def test_a_slurm_packet_is_refused(tmp_path):
    packet = _packet()
    packet["source"] = "slurm"
    packet["slurm"] = {"job_id": "1", "node_name": "node", "partition": "p",
                       "constraints": [], "cgroup": "/x"}
    assert "must be local" in _refusal(tmp_path, packet)


def test_a_packet_with_no_accelerator_is_refused(tmp_path):
    assert "no accelerator" in _refusal(tmp_path, {**_packet(), "accelerators": []})


def test_a_packet_without_device_identity_is_refused(tmp_path):
    packet = _packet()
    packet["accelerators"][0].pop("name")
    packet["accelerators"][0].pop("uuid")
    assert "device identity" in _refusal(tmp_path, packet)


def test_a_packet_that_mixes_models_or_drivers_is_refused(tmp_path):
    packet = _packet()
    other = copy.deepcopy(packet["accelerators"][0])
    other["uuid"] = "GPU-2222"
    other["driver_version"] = "590.1"
    packet["accelerators"].append(other)
    assert "one compute capability and one driver" in _refusal(tmp_path, packet)


def test_a_packet_that_disagrees_with_the_class_is_refused(tmp_path):
    packet = _packet()
    packet["machine"] = "x86_64"
    assert "disagrees with class gb10" in _refusal(tmp_path, packet)


def test_a_class_without_a_rule_is_refused(tmp_path):
    assert "no rule for class" in _refusal(tmp_path, _packet(), host_class="h100")


def test_a_malformed_packet_is_refused_with_the_contract_error(tmp_path):
    packet = _packet()
    packet["accelerators"][0]["compute_capability"] = "twelve"
    assert "--target-evidence" in _refusal(tmp_path, packet)


# --- the collector ---------------------------------------------------------


def test_the_collector_prints_a_packet_the_loader_accepts(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(pb, "_collect_worker_evidence", lambda **kw: _packet())
    assert pbevidence.main([]) == 0
    printed = json.loads(capsys.readouterr().out)
    path = _write(tmp_path, printed)
    loaded = pbrun.load_target_evidence(str(path), host_class="gb10")
    assert loaded["accelerators"][0]["driver_version"] == "595.91.07"


def test_the_collector_asks_for_the_device_identity(monkeypatch):
    seen = {}

    def probe(**kw):
        seen.update(kw)
        return _packet()

    monkeypatch.setattr(pb, "_collect_worker_evidence", probe)
    pbevidence.collect()
    assert seen == {"attest_accelerator_identity": True}


def test_the_collector_writes_one_file_atomically(monkeypatch, tmp_path):
    monkeypatch.setattr(pb, "_collect_worker_evidence", lambda **kw: _packet())
    out = tmp_path / "out" / "packet.json"
    assert pbevidence.main(["--out", str(out)]) == 0
    assert json.loads(out.read_text())["accelerators"][0]["name"] == "NVIDIA GB10"
    assert [p.name for p in out.parent.iterdir()] == ["packet.json"]


@pytest.mark.parametrize("change", ["no_accelerator", "slurm"])
def test_the_collector_refuses_a_box_that_cannot_attest(
        monkeypatch, tmp_path, capsys, change):
    packet = _packet()
    if change == "no_accelerator":
        packet["accelerators"] = []
    else:
        packet["source"] = "slurm"
    monkeypatch.setattr(pb, "_collect_worker_evidence", lambda **kw: packet)
    out = tmp_path / "packet.json"
    assert pbevidence.main(["--out", str(out)]) == 1
    assert not out.exists()
    assert capsys.readouterr().err.startswith("pbevidence:")


# --- pbgang ----------------------------------------------------------------

GROUP = "0" * 32
MEMBER = {"measurement": True, "host_class": "gb10", "exclusive": True}


def _members(tmp_path, first=None, second=None):
    members = [{"tag": "sparklina", "argv": ["/bin/true"], **(first or MEMBER)},
               {"tag": "sparky", "argv": ["/bin/true"], **(second or MEMBER)}]
    path = tmp_path / "gang.json"
    path.write_text(json.dumps({"priority": 0, "timeout_s": 1800, "members": members}))
    return pbgang._pbgang_load_manifest(path)


def _command(tmp_path, evidence, index=0, manifest=None):
    manifest = manifest or _members(tmp_path)
    args = SimpleNamespace(cwd=tmp_path, target_evidence=evidence)
    return pbgang.member_command(args, manifest, manifest["members"][index],
                                 group=GROUP, index=index)


def test_pbgang_forwards_the_option_to_every_class_measurement_member(tmp_path):
    packet = tmp_path / "packet.json"
    for index in (0, 1):
        command = _command(tmp_path, packet, index)
        flags = command[:command.index("--")]
        assert flags[flags.index("--target-evidence") + 1] == str(packet)
        args = pbrun.parse_args([*flags[2:], "--", "/bin/true"])
        assert args.target_evidence == str(packet)


def test_pbgang_without_the_option_builds_the_same_command(tmp_path):
    with_none = _command(tmp_path, None)
    legacy = pbgang.member_command(
        SimpleNamespace(cwd=tmp_path), _members(tmp_path),
        _members(tmp_path)["members"][0], group=GROUP, index=0)
    assert with_none == legacy
    assert "--target-evidence" not in with_none


def test_pbgang_does_not_forward_the_option_to_a_member_without_a_class(tmp_path):
    manifest = _members(tmp_path, second={"measurement": False})
    command = _command(tmp_path, tmp_path / "packet.json", 1, manifest)
    assert "--target-evidence" not in command


def test_pbgang_refuses_the_option_when_no_member_can_use_it(tmp_path, capsys):
    manifest = tmp_path / "gang.json"
    manifest.write_text(json.dumps({"members": [
        {"tag": "sparky", "argv": ["/bin/true"]},
        {"tag": "sparklina", "argv": ["/bin/true"]}]}))
    packet = tmp_path / "packet.json"
    packet.write_text("{}")
    with pytest.raises(SystemExit) as exc:
        pbgang.main(["--manifest", str(manifest), "--cwd", str(tmp_path),
                     "--target-evidence", str(packet)])
    assert exc.value.code == 2
    assert "--target-evidence" in capsys.readouterr().err


def test_pbgang_refuses_a_relative_packet_path(tmp_path, capsys):
    manifest = tmp_path / "gang.json"
    manifest.write_text(json.dumps({"members": [
        {"tag": "sparky", "argv": ["/bin/true"], **MEMBER},
        {"tag": "sparklina", "argv": ["/bin/true"], **MEMBER}]}))
    with pytest.raises(SystemExit) as exc:
        pbgang.main(["--manifest", str(manifest), "--cwd", str(tmp_path),
                     "--target-evidence", "packet.json"])
    assert exc.value.code == 2
    assert "absolute" in capsys.readouterr().err

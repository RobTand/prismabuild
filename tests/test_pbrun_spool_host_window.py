"""pbrun derives a producer's spool window demand from its sealed environment (#747).

The producer's local spool window becomes a ``spool_gb`` host reservation
when its sealed environment sets ``PRISMABUILD_PRODUCED_SPOOL_HOST_WINDOW=1``.
The amount is never typed: pbrun derives it from the byte bound the producer
is sealed with, ``ceil(PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES / 2**30)``, the
same way the produced-output template derives its tier demand.  A typed
``--demand spool_gb`` is refused, the typed vocabulary stays closed, and the
SLURM lane, which cannot hold a host spool, refuses the opt-in.

Off -- no variable, ``""`` or ``"0"`` -- the sealed action carries no
``spool_gb`` anywhere, and its demand is exactly what it was before.
"""
from __future__ import annotations

import json
from pathlib import Path
import socket
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import produced_spool as ps  # noqa: E402
import pbrun  # noqa: E402
from test_pbrun_detach import _checkout  # noqa: E402

GIB = 1 << 30
KIND = "spool_gb"
SWITCH = "PRISMABUILD_PRODUCED_SPOOL_HOST_WINDOW"
MAX = "PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES"


def _build(tmp_path, monkeypatch, *extra: str, transport: str = "pool"):
    work = _checkout(tmp_path)
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    args = pbrun.parse_args([
        "--cwd", str(work), "--detach", "--transport", transport, *extra,
        "--", "/bin/bash", "-lc", "true",
    ])
    return pbrun.prepare_submission(args)["template"]


def _sealed_mentions(template) -> bool:
    sealed = pbrun.seal_action_from_template(template)
    body = json.dumps(sealed, default=str)
    return KIND in body


@pytest.mark.parametrize("env", [(), (f"{SWITCH}=",), (f"{SWITCH}=0",)])
def test_an_unopted_submission_seals_no_spool_demand(tmp_path, monkeypatch, env):
    extra = [item for value in env for item in ("--env", value)]
    extra += ["--env", f"{MAX}={5 * GIB}"]
    template = _build(tmp_path, monkeypatch, *extra)
    assert template["params"]["demand"] == {"cpu": 1, "mem_gb": 4}
    assert not _sealed_mentions(template)


@pytest.mark.parametrize("maximum, gib", [(256, 1), (GIB, 1), (3 * GIB + 1, 4)])
def test_an_opted_in_submission_derives_spool_gb_from_the_byte_bound(
        tmp_path, monkeypatch, maximum, gib):
    template = _build(tmp_path, monkeypatch,
                      "--env", f"{SWITCH}=1", "--env", f"{MAX}={maximum}")
    assert template["params"]["demand"] == {"cpu": 1, "mem_gb": 4, KIND: gib}
    sealed = pbrun.seal_action_from_template(template)
    assert sealed["params"]["demand"][KIND] == gib


def test_an_opted_in_submission_without_a_byte_bound_is_refused(tmp_path, monkeypatch):
    with pytest.raises(SystemExit, match=MAX):
        _build(tmp_path, monkeypatch, "--env", f"{SWITCH}=1")


def test_an_invalid_switch_is_refused(tmp_path, monkeypatch):
    with pytest.raises(SystemExit, match="must be 0 or 1"):
        _build(tmp_path, monkeypatch, "--env", f"{SWITCH}=yes", "--env", f"{MAX}=256")


@pytest.mark.parametrize("env", [(), ("--env", f"{SWITCH}=1", "--env", f"{MAX}=256")])
def test_a_typed_spool_demand_is_refused(tmp_path, monkeypatch, env):
    with pytest.raises(SystemExit, match=KIND):
        _build(tmp_path, monkeypatch, *env, "--demand", f"{KIND}=4")


def test_the_typed_demand_vocabulary_stays_closed():
    assert pbrun._FLEET_DEMAND_KINDS == frozenset({"cpu", "gpu", "mem_gb"})
    with pytest.raises(SystemExit, match="derived"):
        pbrun.validate_fleet_demand({KIND: 4})


def test_slurm_refuses_the_opt_in(tmp_path, monkeypatch):
    with pytest.raises(SystemExit, match="slurm|SLURM"):
        _build(tmp_path, monkeypatch, "--env", f"{SWITCH}=1", "--env", f"{MAX}=256",
               transport="slurm")


def _freeze(tmp_path, *, demand, variables, transport="pool"):
    repo = _checkout(tmp_path)
    return pbrun.freeze_action_template(
        command=["/bin/bash", "-lc", "true"], cwd=repo, logical_cwd=".",
        demand=demand, placement={"required_tags": []},
        variables={"PATH": "/usr/bin:/bin", **variables}, determinism="stochastic",
        retry_policy={"max_attempts": 1, "retry_safe": False},
        host_class=None, measurement=False, transport=transport,
        pool_measurement_class=False, data_manifest_path=None,
        checkout_snapshot_max_bytes=512 * 1024 * 1024, snapshot_refs=[],
        exclusive=False, gpu_memory_gb=None, execution_timeout_s=None,
        progress=None, profile=None, container_image_refs=(),
        wrapper_dir=tmp_path / "wrapper")


@pytest.mark.parametrize("demand, variables", [
    # A spool reservation with no opt-in behind it.
    ({"cpu": 1, "mem_gb": 1, KIND: 1}, {}),
    ({"cpu": 1, "mem_gb": 1, KIND: 1}, {SWITCH: "0", MAX: "256"}),
    # An opt-in whose reservation is missing or disagrees with the bound.
    ({"cpu": 1, "mem_gb": 1}, {SWITCH: "1", MAX: "256"}),
    ({"cpu": 1, "mem_gb": 1, KIND: 1}, {SWITCH: "1", MAX: str(GIB + 1)}),
])
def test_freeze_refuses_a_spool_demand_that_disagrees_with_the_environment(
        tmp_path, monkeypatch, demand, variables):
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    with pytest.raises(SystemExit, match=KIND):
        _freeze(tmp_path, demand=demand, variables=variables)


def test_freeze_refuses_the_opt_in_off_the_pool(tmp_path, monkeypatch):
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    with pytest.raises(SystemExit, match="slurm|SLURM|pool"):
        _freeze(tmp_path, demand={"cpu": 1, "mem_gb": 1, KIND: 1},
                variables={SWITCH: "1", MAX: "256"}, transport="slurm")


def test_freeze_accepts_the_derived_demand(tmp_path, monkeypatch):
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    frozen = _freeze(tmp_path, demand={"cpu": 1, "mem_gb": 1, KIND: 2},
                     variables={SWITCH: "1", MAX: str(GIB + 1)})
    assert frozen["params"]["demand"][KIND] == 2
    assert ps.host_window_terms(frozen["environment"]["variables"]) == {KIND: 2}

"""pbrun accounts a declared produced-spool bound by default (#747, #905).

A producer whose sealed environment declares
``PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES`` gets a ``spool_gb`` host
reservation at new submission: pbrun derives
``ceil(PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES / 2**30)``, the same way the
produced-output template derives its tier demand, and seals
``PRISMABUILD_PRODUCED_SPOOL_HOST_WINDOW=1`` with it before the action is
frozen.  The amount is never typed: a typed ``--demand spool_gb`` is refused,
the typed vocabulary stays closed, and the SLURM lane, which cannot hold a
host spool, refuses a declared bound.  A declared bound with an explicit
``HOST_WINDOW=0`` is a contradictory declaration and is refused by name.

No bound -- with or without a stale switch -- seals exactly the demand it did
before.  ``produced_spool.host_window_terms`` itself keeps its switch-gated
meaning, so an already-sealed request, explicit ``0`` included, is never
re-derived.
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


@pytest.mark.parametrize("switch", [None, "", "1"])
@pytest.mark.parametrize("maximum, gib", [(256, 1), (GIB, 1), (3 * GIB + 1, 4)])
def test_a_declared_bound_derives_spool_gb_at_submission(
        tmp_path, monkeypatch, switch, maximum, gib):
    extra = [] if switch is None else ["--env", f"{SWITCH}={switch}"]
    template = _build(tmp_path, monkeypatch, *extra, "--env", f"{MAX}={maximum}")
    assert template["params"]["demand"] == {"cpu": 1, "mem_gb": 4, KIND: gib}
    sealed = pbrun.seal_action_from_template(template)
    assert sealed["params"]["demand"][KIND] == gib
    assert sealed["environment"]["variables"][SWITCH] == "1"


def test_a_declared_bound_with_an_explicit_zero_is_refused(tmp_path, monkeypatch):
    with pytest.raises(SystemExit, match=MAX):
        _build(tmp_path, monkeypatch,
               "--env", f"{SWITCH}=0", "--env", f"{MAX}=256")


def test_a_submission_without_a_spool_bound_seals_no_spool_demand(tmp_path, monkeypatch):
    plain = tmp_path / "plain"
    plain.mkdir()
    template = _build(plain, monkeypatch)
    assert template["params"]["demand"] == {"cpu": 1, "mem_gb": 4}
    assert not _sealed_mentions(template)
    # A stale switch with no bound changes nothing either.
    stale = tmp_path / "stale"
    stale.mkdir()
    template = _build(stale, monkeypatch, "--env", f"{SWITCH}=0")
    assert template["params"]["demand"] == {"cpu": 1, "mem_gb": 4}
    assert not _sealed_mentions(template)


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
    assert pbrun._FLEET_DEMAND_KINDS == frozenset(
        {"cpu", "gpu", "mem_gb", "disk_metadata"})
    with pytest.raises(SystemExit, match="derived"):
        pbrun.validate_fleet_demand({KIND: 4})


def test_slurm_refuses_the_opt_in(tmp_path, monkeypatch):
    with pytest.raises(SystemExit, match="slurm|SLURM"):
        _build(tmp_path, monkeypatch, "--env", f"{SWITCH}=1", "--env", f"{MAX}=256",
               transport="slurm")


def test_slurm_refuses_a_declared_bound(tmp_path, monkeypatch):
    with pytest.raises(SystemExit, match="slurm|SLURM"):
        _build(tmp_path, monkeypatch, "--env", f"{MAX}=256", transport="slurm")


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
    # A spool reservation nothing in the environment explains.
    ({"cpu": 1, "mem_gb": 1, KIND: 1}, {}),
    # A declared bound whose explicit zero would not charge it.
    ({"cpu": 1, "mem_gb": 1, KIND: 1}, {SWITCH: "0", MAX: "256"}),
    # A switch whose reservation is missing or disagrees with the bound.
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


def test_freeze_accepts_a_declared_bound_and_normalizes_the_switch(tmp_path, monkeypatch):
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    frozen = _freeze(tmp_path, demand={"cpu": 1, "mem_gb": 1, KIND: 1},
                     variables={MAX: "256"})
    assert frozen["params"]["demand"][KIND] == 1
    variables = frozen["environment"]["variables"]
    assert variables[SWITCH] == "1"
    assert ps.host_window_terms(variables) == {KIND: 1}


def test_freeze_normalizes_before_the_ownership_and_stamp_fingerprints(tmp_path, monkeypatch):
    """A declared bound and its explicit switch seal one action, not two.

    The freeze fingerprints the environment for the result/stamp names and
    for container ownership, and the sealed action re-derives its owner from
    that environment.  Normalization therefore has to happen inside the
    freeze, before those fingerprints, or the same effective contract would
    take two action keys depending on how it was spelled.
    """

    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    repo = _checkout(tmp_path)

    def frozen(variables):
        return pbrun.freeze_action_template(
            command=["/bin/bash", "-lc", "true"], cwd=repo, logical_cwd=".",
            demand={"cpu": 1, "mem_gb": 1, KIND: 1},
            placement={"required_tags": []},
            variables={"PATH": "/usr/bin:/bin", **variables},
            determinism="stochastic",
            retry_policy={"max_attempts": 1, "retry_safe": False},
            host_class=None, measurement=False, transport="pool",
            pool_measurement_class=False, data_manifest_path=None,
            checkout_snapshot_max_bytes=512 * 1024 * 1024, snapshot_refs=[],
            exclusive=False, gpu_memory_gb=None, execution_timeout_s=None,
            progress=None, profile=None, container_image_refs=(),
            wrapper_dir=tmp_path / "wrapper")

    default = frozen({MAX: "256"})
    explicit = frozen({SWITCH: "1", MAX: "256"})
    assert default["log_name"] == explicit["log_name"]
    assert default["stamp_name"] == explicit["stamp_name"]
    assert (pbrun.seal_action_from_template(default)["action_key"]
            == pbrun.seal_action_from_template(explicit)["action_key"])


def test_freeze_accepts_the_derived_demand(tmp_path, monkeypatch):
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    frozen = _freeze(tmp_path, demand={"cpu": 1, "mem_gb": 1, KIND: 2},
                     variables={SWITCH: "1", MAX: str(GIB + 1)})
    assert frozen["params"]["demand"][KIND] == 2
    assert ps.host_window_terms(frozen["environment"]["variables"]) == {KIND: 2}

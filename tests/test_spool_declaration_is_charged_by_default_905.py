"""A declared produced-spool bound is charged to the host ledger by default (#905).

prof-1 (PQ #1348, action ``f3f4489d2106``) sealed a 34,359,738,368-byte
produced-spool bound but not ``PRISMABUILD_PRODUCED_SPOOL_HOST_WINDOW``, so
``pbrun.spool_window_terms`` returned ``{}`` and the claim charged zero
``spool_gb`` while the box's disk could still fill.  A new submission that
declares a bound is now normalized to an accounted host window **before the
action is sealed**: the switch is sealed as ``1``, the window joins the sealed
demand (and any declared local scratch) in one ``spool_gb`` term, and
``HOST_WINDOW=0`` beside a declared bound is a contradictory declaration
refused by name at submission.

Already-sealed shapes keep the old switch-gated meaning of
``produced_spool.host_window_terms``, including explicit ``0``: that function
is deliberately unchanged, and the normalization lives only at the ``pbrun``
submission boundary.  A request without a spool bound stays byte-for-byte
what it was.

These tests are the red/green artifact for the fix: run against main
(``0fcd2b2567f5``) the first three tests fail because the declared bound seals
no ``spool_gb`` and the explicit ``0`` is accepted silently.
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
PAIRS = "PRISMABUILD_LOCAL_SCRATCH_PAIRS"
SPILL = ("PQ_SPILL_ROOT", "PQ_SPILL_MAX_BYTES")
SPILL_BYTES = 178 * 10**9
SPILL_GIB = -(-SPILL_BYTES // GIB)          # 166


def _env(*items: str) -> list[str]:
    return [part for item in items for part in ("--env", item)]


def _build(work: Path, fleet: Path, monkeypatch, *extra: str, transport: str = "pool"):
    """The frozen template of one submission over an existing checkout."""

    monkeypatch.setattr(pbrun, "SH", fleet)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    args = pbrun.parse_args([
        "--cwd", str(work), "--detach", "--transport", transport, *extra,
        "--", "/bin/bash", "-lc", "true",
    ])
    return pbrun.prepare_submission(args)["template"]


def _fixture(tmp_path: Path):
    return _checkout(tmp_path), tmp_path / "fleet"


def test_a_declared_bound_is_charged_without_any_switch(tmp_path, monkeypatch):
    work, fleet = _fixture(tmp_path)
    template = _build(work, fleet, monkeypatch, "--env", f"{MAX}={5 * GIB}")
    assert template["params"]["demand"] == {"cpu": 1, "mem_gb": 4, KIND: 5}
    sealed = pbrun.seal_action_from_template(template)
    assert sealed["params"]["demand"][KIND] == 5
    variables = sealed["environment"]["variables"]
    assert variables[SWITCH] == "1"
    assert variables[MAX] == str(5 * GIB)


def test_an_explicit_opt_in_seals_the_same_effective_contract(tmp_path, monkeypatch):
    work, fleet = _fixture(tmp_path)
    default = pbrun.seal_action_from_template(_build(
        work, fleet, monkeypatch, "--env", f"{MAX}={5 * GIB}"))
    explicit = pbrun.seal_action_from_template(_build(
        work, fleet, monkeypatch, "--env", f"{SWITCH}=1", "--env", f"{MAX}={5 * GIB}"))
    assert default["params"]["demand"] == explicit["params"]["demand"]
    assert (default["environment"]["variables"][SWITCH]
            == explicit["environment"]["variables"][SWITCH] == "1")
    # The effective contract is in the action identity: the normalized default
    # and the explicit opt-in over one checkout are the same action.
    assert default["action_key"] == explicit["action_key"]


def test_an_explicit_zero_beside_a_declared_bound_is_refused_by_name(tmp_path, monkeypatch):
    work, fleet = _fixture(tmp_path)
    with pytest.raises(SystemExit, match=MAX):
        _build(work, fleet, monkeypatch,
               *_env(f"{SWITCH}=0", f"{MAX}={5 * GIB}"))


@pytest.mark.parametrize("maximum", ["0", "", "-1", "1.5", "x", " 8"])
def test_a_malformed_or_zero_bound_is_refused_by_name(tmp_path, monkeypatch, maximum):
    work, fleet = _fixture(tmp_path)
    with pytest.raises(SystemExit, match=MAX):
        _build(work, fleet, monkeypatch, *_env(f"{MAX}={maximum}"))


def test_a_declared_switch_that_is_not_zero_or_one_is_refused(tmp_path, monkeypatch):
    work, fleet = _fixture(tmp_path)
    with pytest.raises(SystemExit, match="must be 0 or 1"):
        _build(work, fleet, monkeypatch, *_env(f"{SWITCH}=yes", f"{MAX}=256"))


def test_the_window_and_declared_scratch_add_in_one_kind(tmp_path, monkeypatch):
    work, fleet = _fixture(tmp_path)
    declared = _env(
        f"{SPILL[0]}=/home/rob/pq-scratch/spill",
        f"{SPILL[1]}={SPILL_BYTES}",
        f"{PAIRS}={SPILL[0]}:{SPILL[1]}",
        f"{MAX}={32 * GIB}",
    )
    template = _build(work, fleet, monkeypatch, *declared)
    assert template["params"]["demand"] == {
        "cpu": 1, "mem_gb": 4, KIND: SPILL_GIB + 32}
    sealed = pbrun.seal_action_from_template(template)
    variables = sealed["environment"]["variables"]
    assert variables[SWITCH] == "1"
    assert ps.host_window_terms(variables) == {KIND: 32}


def test_slurm_refuses_a_declared_bound(tmp_path, monkeypatch):
    work, fleet = _fixture(tmp_path)
    with pytest.raises(SystemExit, match="pull queue"):
        _build(work, fleet, monkeypatch, *_env(f"{MAX}={5 * GIB}"),
               transport="slurm")


@pytest.mark.parametrize("extra", [(), (f"{SWITCH}=0",)])
def test_a_request_without_a_spool_bound_is_unchanged(tmp_path, monkeypatch, extra):
    work, fleet = _fixture(tmp_path)
    template = _build(work, fleet, monkeypatch, *_env(*extra))
    assert template["params"]["demand"] == {"cpu": 1, "mem_gb": 4}
    variables = template["environment"]["variables"]
    assert MAX not in variables
    # A stale switch the caller passed is carried through untouched.
    assert variables.get(SWITCH) == (extra[0].split("=", 1)[1] if extra else None)
    assert KIND not in json.dumps(
        pbrun.seal_action_from_template(template), default=str)


def test_the_sealed_helpers_legacy_semantics_are_unchanged(tmp_path):
    # Explicit 0 on an already-sealed shape still derives nothing, and a
    # bound with no switch still derives nothing to the helper itself: the
    # default-on and the contradiction refusal are pbrun's submission
    # boundary, not a retroactive change to sealed requests (#905 direction).
    assert ps.host_window_terms({MAX: str(5 * GIB)}) == {}
    assert ps.host_window_terms({SWITCH: "0", MAX: str(5 * GIB)}) == {}
    assert ps.host_window_terms({SWITCH: "", MAX: str(5 * GIB)}) == {}
    assert ps.host_window_terms({SWITCH: "1", MAX: str(5 * GIB)}) == {KIND: 5}

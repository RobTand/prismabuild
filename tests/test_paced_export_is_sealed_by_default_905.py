"""A declared produced-spool bound seals paced exports by default (#905 Phase 1).

``PRISMABUILD_PRODUCED_SPOOL_PACED_EXPORT`` was an opt-in each producer had to
remember, so a producer that declared a spool bound exported at line rate and
starved the movers sharing its tier (reads 175 -> 19 MB/s under >300 MB/s
writes, dl380g10, 2026-09-22).  ``pbrun.normalize_spool_declaration`` now seals
``PACED_EXPORT=1`` beside a declared bound, exactly parallel to the host
window: explicit ``1`` is the same contract, explicit ``0`` beside a bound is a
contradictory declaration refused by name with the same "drop the bound"
guidance, and a request without a bound seals byte-for-byte as before.

Already-sealed requests are immutable: ``ProducedSpool`` still reads whatever
the sealed environment says, ``0`` and absent included.  The per-group
``submit_group(..., paced=)`` override is untouched and stays the A/B hook.

RED on base (19c5beb40f0d): the first two tests fail because a new
spool-declaring submission seals no ``PACED_EXPORT``, and the explicit ``0`` is
accepted silently.
"""
from __future__ import annotations

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
PACED = "PRISMABUILD_PRODUCED_SPOOL_PACED_EXPORT"
SWITCH = "PRISMABUILD_PRODUCED_SPOOL_HOST_WINDOW"
MAX = "PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES"


def test_the_sealed_name_is_the_producers_own():
    # The submission boundary seals the variable ProducedSpool reads.
    assert ps.PACED_EXPORT_ENV == PACED


def _env(*items: str) -> list[str]:
    return [part for item in items for part in ("--env", item)]


def _build(tmp_path, monkeypatch, *extra: str, transport: str = "pool"):
    tmp_path = tmp_path / f"build{len(list(tmp_path.iterdir()))}"
    tmp_path.mkdir()
    work = _checkout(tmp_path)
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    args = pbrun.parse_args([
        "--cwd", str(work), "--detach", "--transport", transport, *extra,
        "--", "/bin/bash", "-lc", "true",
    ])
    return pbrun.prepare_submission(args)["template"]


def _sealed_variables(template) -> dict:
    return pbrun.seal_action_from_template(template)["environment"]["variables"]


def test_a_declared_bound_seals_paced_export_without_any_switch(tmp_path, monkeypatch):
    template = _build(tmp_path, monkeypatch, *_env(f"{MAX}={5 * GIB}"))
    variables = _sealed_variables(template)
    assert variables[PACED] == "1"
    assert variables[SWITCH] == "1"


def test_an_explicit_opt_in_is_the_same_declaration(tmp_path, monkeypatch):
    # Each build lives in its own checkout, so the action keys differ by path;
    # the sealed environment is what the declaration owns.
    default = _sealed_variables(
        _build(tmp_path, monkeypatch, *_env(f"{MAX}={5 * GIB}")))
    explicit = _sealed_variables(
        _build(tmp_path, monkeypatch, *_env(f"{PACED}=1", f"{MAX}={5 * GIB}")))
    assert explicit[PACED] == "1"
    ours = lambda v: {k: x for k, x in v.items() if k.startswith("PRISMABUILD_PRODUCED_SPOOL")}
    assert ours(default) == ours(explicit)


def test_an_explicit_zero_beside_a_declared_bound_is_refused_by_name(tmp_path, monkeypatch):
    with pytest.raises(SystemExit, match=PACED) as caught:
        _build(tmp_path, monkeypatch, *_env(f"{PACED}=0", f"{MAX}={5 * GIB}"))
    assert "Drop the bound to declare no spool" in str(caught.value)


def test_a_paced_value_that_is_not_zero_or_one_is_refused(tmp_path, monkeypatch):
    with pytest.raises(SystemExit, match="must be 0 or 1"):
        _build(tmp_path, monkeypatch, *_env(f"{PACED}=yes", f"{MAX}={5 * GIB}"))


@pytest.mark.parametrize("extra", [(), (f"{PACED}=0",), (f"{PACED}=1",)])
def test_a_request_without_a_spool_bound_is_unchanged(tmp_path, monkeypatch, extra):
    template = _build(tmp_path, monkeypatch, *_env(*extra))
    variables = _sealed_variables(template)
    # No bound declares no spool: nothing is added, a caller's own value is
    # carried through untouched.
    assert variables.get(PACED) == (extra[0].split("=", 1)[1] if extra else None)
    assert MAX not in variables


def test_the_normalization_is_idempotent_over_a_sealed_environment():
    variables = {MAX: str(5 * GIB), SWITCH: "1", PACED: "1"}
    pbrun.normalize_spool_declaration(variables)
    assert variables == {MAX: str(5 * GIB), SWITCH: "1", PACED: "1"}

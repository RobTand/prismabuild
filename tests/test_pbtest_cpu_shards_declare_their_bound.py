"""A CPU-only shard's declared end is not a GPU box's campaign ceiling (#1123).

Both Sparks announce a 86,400 s timeout ceiling -- the one they accept for
day-long GPU campaign work (#975, #939).  A CPU-only shard tagged ``gb10``
with no ``--timeout-s`` read that same announcement as its own ceiling and
sealed ``execution_timeout_s=86400``, even though it actually ran for a
minute or two.  Admission trusts the sealed request over the shard's real
runtime (``PoolQueue.holder_bound``): a holder is read ``transient`` --
draining soon, and so eligible to hold the box in reserve for a starved
action -- only while its age is inside ``pool.WITHHOLD_CEILING_S`` (900 s) of
its claim, or its declared end is inside that same ceiling of *now*.  A
holder sealed 86,400 s out reads ``long`` for the entire middle of its life,
so a starved GPU action lost its reservation to a same-band CPU shard on the
same box (the 2026-09-25 incident this closes).

A CPU-only shard never runs campaign work, so an announcement that size is
not its own bound to inherit.  ``shard_ceiling`` now caps an *unrequested*
inheritance at ``pbtest.NON_GPU_UNREQUESTED_CEILING_CAP_S`` --
``2 * pool.WITHHOLD_CEILING_S`` (1,800 s) -- the largest end for which
``holder_bound`` reads ``transient`` at *every* age of the holder's life:
the first half by age alone, the second half because the end is then "inside
the ceiling of now".  A wider cap, including the published loop default of
7,200 s tried first, still leaves a "long" window in the middle of a shard's
life (900 s to end-900 s) -- the same shape of starvation the original
86,400 s seal gave, only shorter -- so it does not fix the mechanism the
issue describes; only ``2 * WITHHOLD_CEILING_S`` does.  An explicit
``--timeout-s`` is honoured uncapped, same as an announced ceiling already
tighter than the cap.  A ``--gpu`` shard is unaffected: it keeps inheriting
the full announced ceiling, exactly as #975 left it.

See ``tests/test_a_ready_gpu_action_is_not_starved_by_cpu_shards.py`` for the
tie-in test that drives the real ``PoolQueue.holder_bound`` with the value
``pbtest`` derives here, at ages spanning a shard's whole declared life.
"""

from __future__ import annotations

import pytest

from test_pbtest_seals_its_shard_deadline import (  # noqa: E402
    _announce, _dispatch, _exported_bound, _sealed, pbtest,
)
from prismabuild import pool  # noqa: E402

SPARK_CEILING_S = 86400.0
#: The largest end a non-GPU shard may seal from an announcement it never
#: asked for.  Read off the module rather than restated as a literal, so this
#: file fails the moment the derivation drifts from ``2 * WITHHOLD_CEILING_S``.
CAP_S = pbtest.NON_GPU_UNREQUESTED_CEILING_CAP_S


def test_the_cap_is_twice_the_pools_withhold_ceiling() -> None:
    """The derivation, not a coincidence: see ``shard_ceiling``'s docstring.

    ``holder_bound`` reads ``transient`` while age <= ``WITHHOLD_CEILING_S``
    or the declared end is within ``WITHHOLD_CEILING_S`` of now.  Those two
    windows tile a holder's whole life with no gap exactly when its declared
    end is at most twice ``WITHHOLD_CEILING_S``.
    """

    assert CAP_S == 2 * pool.WITHHOLD_CEILING_S


def test_a_cpu_shard_tagged_gb10_caps_at_twice_the_withhold_ceiling_not_the_days_ceiling(
    tmp_path, monkeypatch, capsys,
) -> None:
    """main: sealed end is ``CAP_S`` (1,800 s), not 86,400 s.

    branch: before the fix this sealed 86400 s, read verbatim off the Sparks'
    announced ceiling, so a shard that runs for a minute declared an end a
    day out and admission read it as ``long`` -- not draining soon -- for the
    whole middle of that day (#1123).
    """

    _announce(sparky=(["gb10", "sparky"], SPARK_CEILING_S),
              sparklina=(["gb10", "sparklina"], SPARK_CEILING_S))

    calls = _dispatch(tmp_path, monkeypatch, ["--tag", "gb10"])

    for command in calls:
        assert _sealed(command) == CAP_S, (
            f"a CPU-only shard sealed execution_timeout_s={_sealed(command)!r} "
            f"instead of the cap {CAP_S!r} -- it inherited the Sparks' "
            "campaign ceiling")
        assert _exported_bound(command) == pytest.approx(CAP_S - pool.HEARTBEAT_S)
    out = capsys.readouterr().out
    assert f"execution_timeout_s={CAP_S:g}" in out
    assert "execution_timeout_s=86400" not in out


def test_a_wider_cap_would_not_have_fixed_the_mechanism(tmp_path, monkeypatch) -> None:
    """The published loop default (7,200 s) was tried and rejected (#1123).

    branch: ``DEFAULT_EXECUTION_CEILING_S`` (7,200 s) is more than twice
    ``WITHHOLD_CEILING_S``, so a shard sealed to it still reads ``long`` from
    900 s to 6,300 s of its life -- the same shape of starvation the original
    86,400 s seal gave, only shorter.  This asserts the sealed end is the
    tighter, correct cap rather than that wider, insufficient one.
    """

    from worker_loop import DEFAULT_EXECUTION_CEILING_S

    assert CAP_S < DEFAULT_EXECUTION_CEILING_S

    _announce(sparky=(["gb10", "sparky"], SPARK_CEILING_S))

    calls = _dispatch(tmp_path, monkeypatch, ["--tag", "gb10"])

    for command in calls:
        assert _sealed(command) != DEFAULT_EXECUTION_CEILING_S
        assert _sealed(command) == CAP_S


def test_an_explicit_timeout_s_past_the_cap_is_still_honoured_for_a_cpu_shard(
    tmp_path, monkeypatch,
) -> None:
    """A declared ``--timeout-s`` is not silently clipped to the cap.

    branch: the cap corrects an *unrequested* inheritance only; a submitter
    who explicitly asks for more still gets the smaller of what they asked
    for and the real announced ceiling, exactly as before #1123.
    """

    _announce(sparky=(["gb10", "sparky"], SPARK_CEILING_S))

    calls = _dispatch(tmp_path, monkeypatch,
                       ["--tag", "gb10", "--timeout-s", "10000"])

    for command in calls:
        assert _sealed(command) == 10000.0
        assert _exported_bound(command) == pytest.approx(10000.0 - pool.HEARTBEAT_S)


def test_an_announced_ceiling_already_tighter_than_the_cap_is_unaffected(
    tmp_path, monkeypatch,
) -> None:
    """The cap only ever lowers an oversized announcement, never raises one."""

    _announce(dl380g10=(["x86", "dl380g10"], 900.0))

    calls = _dispatch(tmp_path, monkeypatch, ["--tag", "x86"])

    for command in calls:
        assert _sealed(command) == 900.0
        assert _exported_bound(command) == pytest.approx(900.0 - pool.HEARTBEAT_S)


def test_a_gpu_shard_still_inherits_the_full_announced_ceiling(
    tmp_path, monkeypatch,
) -> None:
    """The cap is CPU-only: a ``--gpu`` shard's declared end is unchanged (#975).

    ``--test-timeout-s`` alone (no ``--timeout-s``) satisfies #975's refusal
    without giving ``shard_ceiling`` an explicit ``--timeout-s`` to honour, so
    this is exactly the case that must stay uncapped.
    """

    _announce(sparky=(["gb10", "sparky"], SPARK_CEILING_S))

    calls = _dispatch(tmp_path, monkeypatch,
                       ["--tag", "gb10", "--gpu", "--test-timeout-s", "900"])

    for command in calls:
        assert _sealed(command) == SPARK_CEILING_S


def test_shard_ceiling_caps_an_unrequested_announcement_only_for_non_gpu() -> None:
    """Unit-level: the ``gpu`` flag is the entire difference."""

    assert pbtest.shard_ceiling(
        timeout_s=None, gpu=False,
        ceilings={"sparky": SPARK_CEILING_S}) == CAP_S
    assert pbtest.shard_ceiling(
        timeout_s=None, gpu=True,
        ceilings={"sparky": SPARK_CEILING_S}) == SPARK_CEILING_S
    # An explicit ask is honoured uncapped for either.
    assert pbtest.shard_ceiling(
        timeout_s=50000.0, gpu=False,
        ceilings={"sparky": SPARK_CEILING_S}) == 50000.0

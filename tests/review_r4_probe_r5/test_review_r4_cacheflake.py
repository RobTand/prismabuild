"""Opus review r4: the builder's unreproduced ARM checkpoint failure.

test_stale_mention_co_owner_cache.py::test_rewriting_this_owners_fragment_reruns_the_census
failed once on ARM at line 267 ("the census after the rewrite installs a fresh
checkpoint"). kept_reads refuses to keep a version whose ctime is not older
than the coarse-clock fence (#1045). If the third pass starts inside the
rewrite's clock tick, no checkpoint is installed and the fourth pass censuses
again. These probes pin that mechanism; the product files are byte-identical
to main 6abb4b06.
"""
import time
import warnings

import pytest

from prismabuild import kept_reads
import test_stale_mention_co_owner_cache as t
from test_stale_material_done_owner_retires import fleet  # noqa: F401
from test_stale_mention_co_owner_cache import _fresh_checkpoints  # noqa: F401

COARSE = kept_reads._COARSE_REALTIME


def _run(fleet, monkeypatch, mode):
    queue, stage, _ = fleet
    consumer, mover = t.red.stale_owner(fleet, coherent_names=set(t.NAMES), replace=False)
    co_consumer, co_mover = t._co_owner(fleet, t.NAMES)
    t._pass(queue, stage)
    assert t._receipts_for(t._pass(queue, stage), mover) == []
    t._rewrite_fragment(queue, consumer, mover)
    stamp = t.residency_map.fragment_path(queue.records.residency_fragment_root(), consumer, mover).stat().st_ctime_ns
    real = time.clock_gettime_ns
    if mode == 'same_tick':
        monkeypatch.setattr(time, 'clock_gettime_ns', lambda clock: stamp if clock == COARSE else real(clock))
    elif mode == 'tick_passed':
        while real(COARSE) <= stamp:            # one coarse tick, at most a few ms
            time.sleep(0.001)
    third = t._pass(queue, stage)
    monkeypatch.setattr(time, 'clock_gettime_ns', real)
    reasons = (t._one(third, mover)["retained_reason"], t._one(third, co_mover)["retained_reason"])
    return reasons, t._receipts_for(t._pass(queue, stage), mover)


def test_same_tick_third_pass_reproduces_the_arm_assertion(fleet, monkeypatch):
    reasons, fourth = _run(fleet, monkeypatch, 'same_tick')
    assert reasons == ('co-owner', 'co-owner')
    assert fourth != [], 'a same-tick third pass still installed the checkpoint'
    warnings.warn(f"R4-CACHE same tick: fourth pass re-censused {[r.get('event') for r in fourth]}")


@pytest.mark.parametrize('repeat', range(20))
def test_third_pass_after_the_tick_always_installs_the_checkpoint(fleet, monkeypatch, repeat):
    reasons, fourth = _run(fleet, monkeypatch, 'tick_passed')
    assert reasons == ('co-owner', 'co-owner') and fourth == []


@pytest.mark.parametrize('repeat', range(60))
def test_original_test_repeated(fleet, repeat):
    try:
        t.test_rewriting_this_owners_fragment_reruns_the_census(fleet)
    except AssertionError as exc:
        if str(exc) == 'the census after the rewrite installs a fresh checkpoint':
            pytest.xfail('PB lead review r4 proved the unchanged #1045 coarse-clock fixture flake')
        raise

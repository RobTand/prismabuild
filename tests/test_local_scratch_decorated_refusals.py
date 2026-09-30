"""Actual #1182 fairness-decorated refusal boundary; TESTS ONLY, no source fix.

The sample/profile observations are the existing labelled synthetic fixture;
claims, policy refusals, fairness labels, ledgers and terminal/archive are real.
No physical GPU/profile/spill qualification or fabricated diagnostic/receipt.
Execute only in the parent's admitted PB test action, without resubmission.
"""
from __future__ import annotations

import json
from copy import deepcopy
from typing import Any, cast

from test_local_scratch_measured_placement import (
    CAPACITY,
    CONTRACT,
    DEVICES,
    FAST,
    METHOD,
    PLACEMENT,
    PROFILES,
    SLOW,
    ScratchFleet,
    _record,
    pool,
)


def test_actual_fairness_starved_gpu_refusal_ends_measured_yield(tmp_path, monkeypatch):
    fleet: Any = ScratchFleet(tmp_path, monkeypatch)
    fleet.publish()
    ready = _record(fleet.queue.item_path(pool.READY, fleet.key))
    offer = _record(fleet.queue.root / pool.WORKERS / f"{FAST}.json")
    profile = offer["observed_detail"][PROFILES][0]
    current = offer["observed_detail"][DEVICES][fleet.scratch]
    assert profile["profile_contract"] == CONTRACT and profile["method"] == METHOD
    assert profile["pattern"] == "repeated-shake256-block.v1"
    assert profile["root"] == fleet.scratch and profile["completed"] is True
    assert profile["errors"] == []
    assert all(profile[k] == current[k] for k in ("device", "filesystem", "root_inode"))
    assert fleet.queue.ledger(FAST).available() == CAPACITY
    assert fleet.claim(SLOW) is None               # actual faster-fit preference first
    fleet.ready_unspent()

    # Source-derived bound: _withhold_verdict checks passes+1 >= the existing
    # STARVATION_FLOOR before record_pass. Foreign GPU work makes gpu_first
    # false and eligible foreign-load refusal non-withholding, hence _starved.
    # No clock advance, profile/offer refresh, guessed timeout or seeded passes.
    fleet.gpu_fault = "foreign"
    observations = []
    for count in range(1, pool.STARVATION_FLOOR + 1):
        assert fleet.claim(FAST) is None
        actual = fleet.denial(FAST)
        observations.append(deepcopy(actual))
        assert actual["action_key"] == fleet.key and actual["host"] == FAST
        assert actual["published_unix"] == ready["published_unix"]
        assert actual["attempts"] == ready["attempts"] == 0
        decision, sample = actual["evidence"]["decision"], actual["evidence"]["decision"]["sample"]
        assert decision["reason"] == "host_or_device_congested"
        assert sample["complete"] is True and sample["attributed"] is True
        assert sample["schema"] == "prismabuild.gpu_capacity.v1"
        assert sample["foreign_processes"] and len(sample["devices"]) == 1
        assert sample["devices"][0]["memory_domain"] == "shared_system"
        assert 0 <= fleet.clock[0] - sample["sampled_unix"] <= pool.gpu_admission.MAX_SAMPLE_AGE_S
        assert fleet.queue.passes(fleet.key) == count
        assert not fleet.queue.ledger(FAST).held_keys()
        assert fleet.queue.ledger(FAST).available() == CAPACITY
        assert _record(fleet.queue.item_path(pool.READY, fleet.key))["attempts"] == 0
    retained = observations[-1]
    assert retained["reason"] == "adaptive_gpu_refused_starved", observations
    assert retained["evidence"]["withhold"]["eligible"] is True
    assert retained["evidence"]["withhold"]["withhold"] is False
    assert retained["evidence"]["withhold"]["why"] == "foreign_load"
    assert retained["evidence"]["starved"]["why"] == "foreign_load"
    assert retained["denied_unix"] >= offer["announced_unix"]
    assert 0 <= fleet.clock[0] - profile["measured_unix"] <= fleet.declaration["max_profile_age_s"]

    # Mirror ONLY this actual generated record through the existing fixture
    # observation boundary. Restore evidence without another FAST evaluation.
    fleet.mirror_real_denial(FAST, "adaptive_gpu_refused_starved")
    shared = _record(fleet.queue.ledger(FAST).base / "adaptive" / pool.CLAIM_DENIALS)
    identity = f"{fleet.key}:{repr(float(ready['published_unix']))}"
    assert shared["records"][identity] == retained
    fleet.gpu_fault = None
    claimed = fleet.claim(SLOW)
    if claimed is None:
        # Durable failure output includes the genuine record and the actual
        # candidate diagnostics, not a helper-computed placement substitute.
        print("decorated-refusal-witness: " + json.dumps({
            "retained_fast_refusals": observations,
            "actual_slow_denial": fleet.denial(SLOW),
        }, sort_keys=True))
    assert claimed is not None, "recognized adaptive_gpu_refused_starved did not end measured yield"
    assert claimed["action_key"] == fleet.key and claimed["claimed_host"] == SLOW
    assert claimed["published_unix"] == ready["published_unix"]
    assert claimed["cpu_allocation"] == {"preferred": [0], "fallback": []}
    assert fleet.queue.ledger(SLOW).held() == {"cpu": 1, "gpu": 1, "mem_gb": 4, "spool_gb": 186}
    evidence, candidates = fleet.receipt(claimed, SLOW)
    assert candidates[FAST]["fit"] is False
    assert candidates[FAST]["exclusion"] == "current_policy_refusal"
    assert fleet.denial(FAST) == retained
    fleet.host = SLOW
    terminal = _record(fleet.queue.finish(fleet.key, status="executed", detail={}))
    assert terminal["detail"][PLACEMENT] == evidence
    outcomes = cast(list[dict[str, Any]], fleet.queue.attempt_outcomes(terminal))
    assert len(outcomes) == 1 and outcomes[0]["claimed_host"] == SLOW
    assert outcomes[0]["detail"][PLACEMENT] == evidence
    assert fleet.queue.ledger(SLOW).available() == CAPACITY
    assert not fleet.queue.ledger(SLOW).held_keys()

"""Independent claim-boundary regression for PR #748's R2 candidate.

Uses the candidate's real queue/ledger fixture on a private temporary queue.
No batch is committed: a pending output mover must never acquire fresh credit.
"""
from __future__ import annotations

import json

import pytest

from test_prepaid_output_funding import (
    KIND, TIER, _bind, _descriptors, _hexkey, _prewrite, _publish_mover,
    _queue, _template, po, pool,
)


@pytest.mark.parametrize("funded", [False, True], ids=["staged", "transferred"])
@pytest.mark.parametrize("damage", ["none", "truncated", "invalid", "missing"])
def test_unknown_output_intent_never_acquires_fresh_credit(tmp_path, funded, damage):
    owner = _hexkey("review747-owner")
    mover = _hexkey("review747-mover")
    q = _queue(tmp_path)
    ledger = q.tier_ledger(TIER)
    template = _template(str(tmp_path / "outputs"))
    inst, _, _, _ = _bind(q, template, owner)
    descriptors = _descriptors(tmp_path, template, inst)
    _prewrite(q, inst, template, "b1", TIER, descriptors)
    staged = q.stage_output_intent(
        tier_id=TIER, owner_key=owner, mover_key=mover, instance=inst,
        template=template, batch_id="b1", descriptors=descriptors,
    )
    assert staged.get("ok") is True, staged
    _publish_mover(q, mover, po.output_manifest_sha256(descriptors),
                   sum(d["bytes"] for d in descriptors), gib=1)
    if funded:
        driven = q.drive_output_funding(mover, TIER)
        assert driven.get("ok") is True, driven

    before = {
        "free": ledger.available().get(KIND, 0),
        "owner": ledger.holder_tokens(owner).get(KIND, 0),
        "mover": ledger.holder_tokens(mover).get(KIND, 0),
    }
    intent_path = q.funding_output_path(mover, TIER)
    if damage == "truncated":
        intent_path.write_text("{truncated", encoding="utf-8")
    elif damage == "invalid":
        record = json.loads(intent_path.read_text(encoding="utf-8"))
        record["generation"] = "invalid"
        intent_path.write_text(json.dumps(record), encoding="utf-8")
    elif damage == "missing":
        intent_path.unlink()

    claimed = q.claim(owner="review747-claimant")
    after = {
        "free": ledger.available().get(KIND, 0),
        "owner": ledger.holder_tokens(owner).get(KIND, 0),
        "mover": ledger.holder_tokens(mover).get(KIND, 0),
    }
    observed = {
        "funded": funded, "damage": damage,
        "claimed": claimed.get("action_key") if claimed else None,
        "before": before, "after": after,
    }
    print(json.dumps(observed, sort_keys=True))
    assert claimed is None, observed
    assert q.item_path(pool.READY, mover).is_file()
    assert after == before

"""Origin reclamation and stage-copy lifetime are independent batch facts."""
from prismabuild import batch_ledger as bl
from prismabuild import output_contract as oc
from pathlib import Path

from prismabuild import produced_output as po
from test_produced_output_restage import _World, _descriptors, _prewrite


def test_origin_reclaimed_while_the_current_stage_copy_remains_live(tmp_path):
    world = _World(tmp_path)
    payload = b"independent batch facets"
    descriptors = _descriptors(world.template, world.inst, "p1", payload)
    _prewrite(world.q, world.inst, world.template, "b1", descriptors)
    published = world.first_publish("b1", descriptors)
    world.run_mover(str(published["mover_key"]), "facet-mover")
    staged = world.stage_path("p1.bin")
    assert staged.read_bytes() == payload
    before = bl.BatchLedger.materialization(
        world.q, world.inst, world.template, batch_id="b1")
    assert before["ok"] and before["mover_receipt_complete"]
    assert before["stage_retired"] is False
    assert before["origin_reclaimed"] is False

    Path(descriptors[0]["path"]).unlink()
    assert bl.BatchLedger.reclaim(world.q, world.inst, world.template, batch_id="b1") == {
        "ok": True, "batch_id": "b1", "reclaimed": True}

    after = bl.BatchLedger.materialization(
        world.q, world.inst, world.template, batch_id="b1")
    assert after["ok"] and after["mover_receipt_complete"]
    assert after["origin_reclaimed"] is True
    assert after["stage_retired"] is False
    assert after["mover_key"] == before["mover_key"]
    assert after["generation"] == before["generation"]
    assert staged.read_bytes() == payload
    record = bl.BatchLedger.record(world.q, world.inst, world.template, batch_id="b1")
    assert record["state"] == oc.BATCH_STATE_RECLAIMED
    assert record["commitment"]["origin_reclaimed"] is True
